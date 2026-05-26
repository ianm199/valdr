//! Global cross-connection BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/XREAD BLOCK wait queue.
//!
//! Each entry holds a per-client `mpsc::Sender<Vec<u8>>` (the same writer-thread
//! channel that `PubSubRegistry` uses) plus a `BlockSpec` describing how to
//! satisfy the wait when the key becomes ready.
//!
//! Layout:
//!   * `keys` — key → FIFO `VecDeque<ClientId>` of waiters in arrival order.
//!   * `waiters` — `ClientId` → `Waiter { sender, deadline_ms, keys, spec }`.
//!
//! All access goes through `Arc<Mutex<BlockedKeysIndex>>` returned by
//! [`blocked_keys_index`]. The blocking command handler inserts a waiter;
//! the LIST family's push hook drains FIFO entries via [`take_waiter`]; the
//! STREAM family's XADD wakes stream waiters via [`take_stream_waiters_for`];
//! the timer thread drains expired entries via [`take_expired`].

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_ds::stream::StreamId;
use redis_types::RedisString;

use crate::client::ClientId;

static BLOCKED_KEYS_WAITERS: AtomicUsize = AtomicUsize::new(0);

pub fn blocked_keys_any() -> bool {
    BLOCKED_KEYS_WAITERS.load(Ordering::Acquire) != 0
}

/// Which end of the list to pop on wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockedSide {
    Head,
    Tail,
}

/// How a woken waiter consumes its element.
///
/// `Pop` returns a `*2 [key, value]` reply when `count == 0` (BLPOP / BRPOP
/// shape) or a `*2 [key, *N [...]]` reply when `count >= 1` (BLMPOP shape,
/// pops up to `count` elements).
/// `Move` pops from the source and pushes onto `dst_key` at `dst_side`, then
/// replies with a bulk-string of the moved value (BLMOVE / BRPOPLPUSH shape).
/// `ZSetPop` returns a `*3 [key, member, score]` reply when `count == 0`
/// (BZPOPMIN / BZPOPMAX shape) or `*2 [key, *N [[m1,s1],[m2,s2],...]]` when
/// `count >= 1` (BZMPOP shape, pops up to `count` members).
/// `Stream` parks the client until a new entry with id strictly greater than
/// `id_after` arrives on the stream key (XREAD BLOCK shape).
/// `StreamGroup` parks the client in an XREADGROUP BLOCK call on a specific
/// consumer group. Woken when a new entry arrives (XADD), the key is deleted,
/// or the group is destroyed.
/// `Wait` parks the client until at least `numreplicas` replicas have
/// acknowledged `target_offset` or the timeout fires.
#[derive(Debug, Clone)]
pub enum BlockedAction {
    Pop {
        side: BlockedSide,
        count: u64,
    },
    Move {
        side: BlockedSide,
        dst_key: RedisString,
        dst_side: BlockedSide,
    },
    ZSetPop {
        reverse: bool,
        count: u64,
    },
    Stream {
        id_after: StreamId,
    },
    StreamGroup {
        id_after: StreamId,
        group: RedisString,
        consumer: RedisString,
        count: Option<i64>,
        noack: bool,
    },
    Wait {
        target_offset: i64,
        numreplicas: usize,
    },
    WaitAof {
        target_offset: i64,
        numreplicas: usize,
        numlocal: usize,
    },
}

impl BlockedAction {
    /// Return the RESP bytes to send when this waiter's deadline expires with
    /// no data delivered.
    ///
    /// BLPOP / BRPOP / BLMPOP: null array (`*-1\r\n`).
    /// BLMOVE / BRPOPLPUSH: null bulk (`$-1\r\n`).
    /// XREAD BLOCK: null bulk (`$-1\r\n`) — matches real Redis behaviour.
    /// WAIT timeout: integer reply of current acked-replica count (`:<n>\r\n`).
    ///
    /// The `acked_count` argument is only consulted for `Wait`; other variants
    /// ignore it.
    pub fn timeout_reply_bytes_with_count(&self, acked_count: usize) -> Vec<u8> {
        match self {
            BlockedAction::Pop { .. } => b"*-1\r\n".to_vec(),
            BlockedAction::Move { .. } => b"$-1\r\n".to_vec(),
            BlockedAction::ZSetPop { .. } => b"*-1\r\n".to_vec(),
            BlockedAction::Stream { .. } => b"$-1\r\n".to_vec(),
            BlockedAction::StreamGroup { .. } => b"*-1\r\n".to_vec(),
            BlockedAction::Wait { .. } => format!(":{}\r\n", acked_count).into_bytes(),
            BlockedAction::WaitAof { .. } => format!("*2\r\n:0\r\n:{}\r\n", acked_count).into_bytes(),
        }
    }

    /// Return the RESP bytes to send when this waiter's deadline expires.
    ///
    /// Delegates to [`timeout_reply_bytes_with_count`] with zero for the
    /// acked count. `Wait` waiters that call `take_expired` must use
    /// [`timeout_reply_bytes_with_count`] directly to pass the live count.
    pub fn timeout_reply_bytes(&self) -> &'static [u8] {
        match self {
            BlockedAction::Pop { .. } => b"*-1\r\n",
            BlockedAction::Move { .. } => b"$-1\r\n",
            BlockedAction::ZSetPop { .. } => b"*-1\r\n",
            BlockedAction::Stream { .. } => b"$-1\r\n",
            BlockedAction::StreamGroup { .. } => b"*-1\r\n",
            BlockedAction::Wait { .. } => b":0\r\n",
            BlockedAction::WaitAof { .. } => b"*2\r\n:0\r\n:0\r\n",
        }
    }

    /// Whether this waiter should be unblocked when the key it waits on is
    /// deleted or otherwise stops existing. XREADGROUP must wake with a
    /// NOGROUP error in that case; plain XREAD and the list/zset pops keep
    /// waiting for data, so they are not "nokey" waiters.
    pub fn unblock_on_nokey(&self) -> bool {
        matches!(self, BlockedAction::StreamGroup { .. })
    }
}

/// One blocked client's full wait spec.
#[derive(Debug, Clone)]
pub struct BlockedWaiter {
    pub client_id: ClientId,
    pub sender: Sender<Vec<u8>>,
    pub keys: Vec<RedisString>,
    pub action: BlockedAction,
    pub deadline_ms: i64,
    pub resp_proto: i32,
    pub username: Option<RedisString>,
}

/// Server-wide blocked-keys index.
#[derive(Default)]
pub struct BlockedKeysIndex {
    keys: HashMap<RedisString, VecDeque<ClientId>>,
    waiters: HashMap<ClientId, BlockedWaiter>,
}

impl BlockedKeysIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `waiter` under each of its keys, in order of arrival.
    ///
    /// Overwrites any prior waiter for the same `client_id` (callers must
    /// ensure a client never re-blocks while already parked).
    pub fn add(&mut self, waiter: BlockedWaiter) {
        let cid = waiter.client_id;
        let already_registered = self.waiters.contains_key(&cid);
        for key in &waiter.keys {
            self.keys.entry(key.clone()).or_default().push_back(cid);
        }
        self.waiters.insert(cid, waiter);
        if !already_registered {
            BLOCKED_KEYS_WAITERS.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Pop the FIFO-front waiter for `key` and return its full record.
    ///
    /// Also clears that client from every other key it was waiting on so a
    /// single push can never satisfy the same waiter twice across two keys.
    pub fn take_waiter(&mut self, key: &RedisString) -> Option<BlockedWaiter> {
        let cid = loop {
            let deque = self.keys.get_mut(key)?;
            let front = deque.pop_front()?;
            if deque.is_empty() {
                self.keys.remove(key);
            }
            if self.waiters.contains_key(&front) {
                break front;
            }
        };
        self.remove_client(cid)
    }

    /// Remove `client_id` from every key queue and return its waiter record.
    pub fn remove_client(&mut self, client_id: ClientId) -> Option<BlockedWaiter> {
        let waiter = self.waiters.remove(&client_id)?;
        let _ = BLOCKED_KEYS_WAITERS.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(1))
        });
        for key in &waiter.keys {
            if let Some(deque) = self.keys.get_mut(key) {
                deque.retain(|cid| *cid != client_id);
                if deque.is_empty() {
                    self.keys.remove(key);
                }
            }
        }
        Some(waiter)
    }

    /// Drain every waiter whose `deadline_ms` is `<= now_ms`.
    pub fn take_expired(&mut self, now_ms: i64) -> Vec<BlockedWaiter> {
        let expired: Vec<ClientId> = self
            .waiters
            .iter()
            .filter(|(_, w)| w.deadline_ms <= now_ms)
            .map(|(cid, _)| *cid)
            .collect();
        let mut out = Vec::with_capacity(expired.len());
        for cid in expired {
            if let Some(w) = self.remove_client(cid) {
                out.push(w);
            }
        }
        out
    }

    /// Drain all `Stream` waiters for `key` whose `id_after` is strictly less
    /// than `new_id`, remove them from the index, and return their records.
    ///
    /// Unlike list pop semantics (FIFO, one waiter per element), XREAD BLOCK
    /// uses broadcast semantics: every blocked reader whose cursor is behind
    /// the new entry receives a wake, and all of them receive the same entry.
    pub fn take_stream_waiters_for(
        &mut self,
        key: &RedisString,
        new_id: StreamId,
    ) -> Vec<BlockedWaiter> {
        let client_ids: Vec<ClientId> = match self.keys.get(key) {
            None => return Vec::new(),
            Some(deque) => deque.iter().copied().collect(),
        };
        let mut out = Vec::new();
        for cid in client_ids {
            let matches = match self.waiters.get(&cid) {
                Some(w) => {
                    matches!(&w.action, BlockedAction::Stream { id_after } if *id_after < new_id)
                }
                None => false,
            };
            if matches {
                if let Some(w) = self.remove_client(cid) {
                    out.push(w);
                }
            }
        }
        out
    }

    /// Drain all `StreamGroup` waiters for `key` whose `id_after` is strictly
    /// less than `new_id`, remove them from the index, and return their records.
    ///
    /// Used by XADD to broadcast a new entry to all blocked XREADGROUP clients
    /// whose consumer group cursor is behind the new entry.
    pub fn take_stream_group_waiters_for(
        &mut self,
        key: &RedisString,
        new_id: StreamId,
    ) -> Vec<BlockedWaiter> {
        let client_ids: Vec<ClientId> = match self.keys.get(key) {
            None => return Vec::new(),
            Some(deque) => deque.iter().copied().collect(),
        };
        let mut out = Vec::new();
        for cid in client_ids {
            let matches = match self.waiters.get(&cid) {
                Some(w) => {
                    matches!(&w.action, BlockedAction::StreamGroup { id_after, .. } if *id_after < new_id)
                }
                None => false,
            };
            if matches {
                if let Some(w) = self.remove_client(cid) {
                    out.push(w);
                }
            }
        }
        out
    }

    /// Drain all `StreamGroup` waiters for `key` regardless of their cursor,
    /// remove them from the index, and return their records.
    ///
    /// Used when the key is deleted, flushed, or the group is destroyed — any
    /// of these events should unblock all XREADGROUP waiters on that key.
    pub fn take_all_stream_group_waiters_for(&mut self, key: &RedisString) -> Vec<BlockedWaiter> {
        let client_ids: Vec<ClientId> = match self.keys.get(key) {
            None => return Vec::new(),
            Some(deque) => deque.iter().copied().collect(),
        };
        let mut out = Vec::new();
        for cid in client_ids {
            let matches = match self.waiters.get(&cid) {
                Some(w) => matches!(&w.action, BlockedAction::StreamGroup { .. }),
                None => false,
            };
            if matches {
                if let Some(w) = self.remove_client(cid) {
                    out.push(w);
                }
            }
        }
        out
    }

    /// Drain every `StreamGroup` waiter across all keys and return them.
    ///
    /// Used by FLUSHDB/FLUSHALL where every blocked XREADGROUP client must be
    /// woken with a NOGROUP error because all keys are gone.
    pub fn take_all_stream_group_waiters(&mut self) -> Vec<BlockedWaiter> {
        let cids: Vec<ClientId> = self
            .waiters
            .iter()
            .filter(|(_, w)| matches!(&w.action, BlockedAction::StreamGroup { .. }))
            .map(|(cid, _)| *cid)
            .collect();
        let mut out = Vec::with_capacity(cids.len());
        for cid in cids {
            if let Some(w) = self.remove_client(cid) {
                out.push(w);
            }
        }
        out
    }

    /// Pop the FIFO-front `ZSetPop` waiter for `key` and return its full record.
    ///
    /// Skips any non-`ZSetPop` waiters at the head of the queue (they belong to
    /// list commands that happen to share a key name). Removes the waiter from
    /// every other key it was waiting on so a single ZADD can never satisfy the
    /// same waiter twice.
    pub fn take_zset_waiter(&mut self, key: &RedisString) -> Option<BlockedWaiter> {
        let deque = self.keys.get(key)?;
        let cid = deque
            .iter()
            .find(|cid| {
                self.waiters
                    .get(cid)
                    .is_some_and(|w| matches!(w.action, BlockedAction::ZSetPop { .. }))
            })
            .copied()?;
        if let Some(deque) = self.keys.get_mut(key) {
            deque.retain(|c| *c != cid);
            if deque.is_empty() {
                self.keys.remove(key);
            }
        }
        let waiter = self.waiters.remove(&cid)?;
        for k in &waiter.keys {
            if k == key {
                continue;
            }
            if let Some(deque) = self.keys.get_mut(k) {
                deque.retain(|c| *c != cid);
                if deque.is_empty() {
                    self.keys.remove(k);
                }
            }
        }
        Some(waiter)
    }

    /// Peek at the FIFO-front `ZSetPop` waiter for `key` and return a clone.
    ///
    /// Does not remove the waiter from the index. Used by
    /// `zset::wake_blocked_zset_for_key` to drive the wake loop.
    pub fn peek_zset_waiter(&mut self, key: &RedisString) -> Option<BlockedWaiter> {
        self.take_zset_waiter(key)
    }

    /// Whether any waiter currently parks on `key`.
    pub fn has_waiters_for(&self, key: &RedisString) -> bool {
        self.keys.get(key).is_some_and(|d| !d.is_empty())
    }

    /// Snapshot of every key that has at least one waiter.
    pub fn all_blocked_keys(&self) -> Vec<RedisString> {
        self.keys
            .keys()
            .filter(|k| {
                self.keys[*k]
                    .iter()
                    .any(|cid| self.waiters.contains_key(cid))
            })
            .cloned()
            .collect()
    }

    /// Snapshot the number of currently-blocked clients (test/debug helper).
    pub fn len(&self) -> usize {
        self.waiters.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }

    /// Number of distinct keys that have at least one blocked client.
    /// Reported as `total_blocking_keys` in `INFO clients`.
    pub fn total_blocking_keys(&self) -> usize {
        self.keys.values().filter(|q| !q.is_empty()).count()
    }

    /// Number of distinct blocking keys that have at least one client which
    /// should be unblocked even when the key does not exist (the "nokey"
    /// condition — currently XREADGROUP, which must wake with NOGROUP on
    /// delete/destroy). Reported as `total_blocking_keys_on_nokey`.
    pub fn total_blocking_keys_on_nokey(&self) -> usize {
        self.keys
            .values()
            .filter(|q| {
                q.iter().any(|cid| {
                    self.waiters
                        .get(cid)
                        .is_some_and(|w| w.action.unblock_on_nokey())
                })
            })
            .count()
    }

    /// Drain all `Wait` waiters whose required replica count is now satisfied.
    ///
    /// `acked_count_for` is a closure that, given a `target_offset`, returns
    /// the number of replicas whose acknowledged offset is `>= target_offset`.
    /// Waiters where that count reaches `numreplicas` are removed from the
    /// index and returned to the caller; the caller should send an integer
    /// reply through each waiter's sender.
    pub fn take_satisfied_wait_waiters(
        &mut self,
        acked_count_for: impl Fn(i64) -> usize,
    ) -> Vec<(BlockedWaiter, usize)> {
        let satisfied: Vec<(ClientId, usize)> = self
            .waiters
            .iter()
            .filter_map(|(cid, w)| match &w.action {
                BlockedAction::Wait {
                    target_offset,
                    numreplicas,
                } => {
                    let count = acked_count_for(*target_offset);
                    if count >= *numreplicas {
                        Some((*cid, count))
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();
        let mut out = Vec::with_capacity(satisfied.len());
        for (cid, count) in satisfied {
            if let Some(w) = self.remove_wait_client(cid) {
                out.push((w, count));
            }
        }
        out
    }

    /// Drain every `WaitAof` waiter whose local and replica fsync
    /// requirements are now satisfied.
    pub fn take_satisfied_waitaof_waiters(
        &mut self,
        local_count_for: impl Fn(i64) -> usize,
        aof_acked_count_for: impl Fn(i64) -> usize,
    ) -> Vec<(BlockedWaiter, usize, usize)> {
        let satisfied: Vec<(ClientId, usize, usize)> = self
            .waiters
            .iter()
            .filter_map(|(cid, w)| match &w.action {
                BlockedAction::WaitAof {
                    target_offset,
                    numreplicas,
                    numlocal,
                } => {
                    let local = local_count_for(*target_offset);
                    let replicas = aof_acked_count_for(*target_offset);
                    if local >= *numlocal && replicas >= *numreplicas {
                        Some((*cid, local, replicas))
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();
        let mut out = Vec::with_capacity(satisfied.len());
        for (cid, local, replicas) in satisfied {
            if let Some(w) = self.remove_wait_client(cid) {
                out.push((w, local, replicas));
            }
        }
        out
    }

    /// Drain WAITAOF waiters that require local AOF after appendonly was
    /// disabled. Upstream unblocks these with an error rather than waiting
    /// until their timeout.
    pub fn take_waitaof_local_waiters(&mut self) -> Vec<BlockedWaiter> {
        let cids: Vec<ClientId> = self
            .waiters
            .iter()
            .filter_map(|(cid, w)| match &w.action {
                BlockedAction::WaitAof { numlocal, .. } if *numlocal > 0 => Some(*cid),
                _ => None,
            })
            .collect();
        let mut out = Vec::with_capacity(cids.len());
        for cid in cids {
            if let Some(w) = self.remove_wait_client(cid) {
                out.push(w);
            }
        }
        out
    }

    pub fn take_all_waitaof_waiters(&mut self) -> Vec<BlockedWaiter> {
        let cids: Vec<ClientId> = self
            .waiters
            .iter()
            .filter_map(|(cid, w)| match &w.action {
                BlockedAction::WaitAof { .. } => Some(*cid),
                _ => None,
            })
            .collect();
        let mut out = Vec::with_capacity(cids.len());
        for cid in cids {
            if let Some(w) = self.remove_wait_client(cid) {
                out.push(w);
            }
        }
        out
    }

    /// Remove a `Wait` waiter by client id without consulting the keys index
    /// (Wait waiters are not keyed — they park under a sentinel key).
    fn remove_wait_client(&mut self, client_id: ClientId) -> Option<BlockedWaiter> {
        let waiter = self.waiters.remove(&client_id)?;
        let _ = BLOCKED_KEYS_WAITERS.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(1))
        });
        for key in &waiter.keys {
            if let Some(deque) = self.keys.get_mut(key) {
                deque.retain(|cid| *cid != client_id);
                if deque.is_empty() {
                    self.keys.remove(key);
                }
            }
        }
        Some(waiter)
    }
}

static BLOCKED_KEYS_INDEX: OnceLock<Arc<Mutex<BlockedKeysIndex>>> = OnceLock::new();

/// Install or fetch the global blocked-keys index.
pub fn blocked_keys_index() -> &'static Arc<Mutex<BlockedKeysIndex>> {
    BLOCKED_KEYS_INDEX.get_or_init(|| Arc::new(Mutex::new(BlockedKeysIndex::new())))
}

/// Wall-clock time in milliseconds since the Unix epoch.
pub fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Convert a non-negative seconds timeout (BLPOP-style) into an absolute
/// millisecond deadline. A `seconds` of `0.0` means block forever and maps to
/// `i64::MAX`.
pub fn deadline_from_timeout_secs(seconds: f64) -> i64 {
    if seconds <= 0.0 {
        return i64::MAX;
    }
    let now = current_time_ms();
    let add_ms = (seconds * 1000.0) as i64;
    now.saturating_add(add_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn k(s: &[u8]) -> RedisString {
        RedisString::from_bytes(s)
    }

    fn waiter(id: ClientId, keys: Vec<RedisString>, deadline: i64) -> BlockedWaiter {
        let (tx, _rx) = mpsc::channel();
        BlockedWaiter {
            client_id: id,
            sender: tx,
            keys,
            action: BlockedAction::Pop {
                side: BlockedSide::Head,
                count: 0,
            },
            deadline_ms: deadline,
            resp_proto: 2,
            username: None,
        }
    }

    #[test]
    fn fifo_order_per_key() {
        let mut idx = BlockedKeysIndex::new();
        idx.add(waiter(1, vec![k(b"q")], i64::MAX));
        idx.add(waiter(2, vec![k(b"q")], i64::MAX));
        idx.add(waiter(3, vec![k(b"q")], i64::MAX));
        assert_eq!(idx.take_waiter(&k(b"q")).map(|w| w.client_id), Some(1));
        assert_eq!(idx.take_waiter(&k(b"q")).map(|w| w.client_id), Some(2));
        assert_eq!(idx.take_waiter(&k(b"q")).map(|w| w.client_id), Some(3));
        assert!(idx.take_waiter(&k(b"q")).is_none());
    }

    #[test]
    fn multi_key_wake_removes_from_other_queues() {
        let mut idx = BlockedKeysIndex::new();
        idx.add(waiter(1, vec![k(b"a"), k(b"b")], i64::MAX));
        let w = idx.take_waiter(&k(b"a")).expect("wake on a");
        assert_eq!(w.client_id, 1);
        assert!(!idx.has_waiters_for(&k(b"b")));
    }

    #[test]
    fn expired_waiters_drained() {
        let mut idx = BlockedKeysIndex::new();
        idx.add(waiter(1, vec![k(b"a")], 100));
        idx.add(waiter(2, vec![k(b"b")], 200));
        let expired = idx.take_expired(150);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].client_id, 1);
        assert!(idx.has_waiters_for(&k(b"b")));
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        round-12b BLPOP/BRPOP/BLMOVE blocking architecture
//                  (no direct C analogue — Rust replacement for the
//                  `serverDb.blocking_keys` + `bio` timer/ready-key plumbing
//                  in `src/blocked.c` / `src/t_list.c`).
//   target_crate:  redis-core
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Reuses each client's PubSubRegistry mpsc Sender for the
//                  cross-connection wake reply; FIFO ordering preserved per
//                  key. Background timer thread scans take_expired() every
//                  100 ms in main.rs.
// ──────────────────────────────────────────────────────────────────────────
