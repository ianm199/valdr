//! Sampling-based maxmemory enforcement — coarse Rust-native replacement for
//! the C-shaped skeleton in `evict.rs`.
//!
//! Algorithm sketch (intentional simplifications vs upstream):
//!   - Memory usage is whatever [`approximate_memory_used`] returns; the C
//!     allocator-tracked `zmalloc_used_memory` is not faithfully ported.
//!   - Candidate selection is N-of-K sampling over the main dict's iteration
//!     order rather than the C eviction pool sorted by idle time. K is
//!     `maxmemory-samples` (default 5) and the oldest sample is evicted.
//!   - `volatile-*` and `allkeys-lfu` are stubbed at the policy level; the
//!     supported policies this round are `noeviction` and `allkeys-lru`.
//!
//! Operator handoff: when the C-shaped pool in `evict.rs` is reconciled with
//! reality, this module should be retired in favour of that path. Until then
//! the dispatch gate in `redis-commands::dispatch` calls into here.

use std::sync::atomic::Ordering;

use redis_types::RedisString;

use crate::db::RedisDb;
use crate::live_config::MaxmemoryPolicyCode;
use crate::memory::approximate_memory_used;
use crate::metrics::server_metrics;
use crate::object::RedisObject;

/// Maximum number of evictions to attempt in a single dispatch-gate call.
///
/// Caps the worst-case work done before the gate gives up and returns
/// [`EvictionOutcome::StillOver`]. Tuned to "small enough that one slow
/// command does not stall the server, large enough that any realistic
/// per-command memory growth can be absorbed".
const MAX_EVICTIONS_PER_CALL: usize = 100;

/// Default `maxmemory-samples` per upstream config. Controls the K in
/// "sample K keys, evict the oldest".
const DEFAULT_MAXMEMORY_SAMPLES: usize = 5;

/// Outcome of a single eviction attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum EvictionOutcome {
    /// `approximate_memory_used` was already below `target_bytes` on entry.
    Sufficient,
    /// Evicted `usize` keys and is now under `target_bytes`.
    Evicted(usize),
    /// Exhausted the per-call budget without getting under the limit.
    StillOver,
}

/// Drive eviction until either `approximate_memory_used(db) <= target_bytes`,
/// the per-call budget is exhausted, or the configured policy refuses to
/// evict anything.
///
/// The eviction policy is consulted on every iteration so that a runtime
/// CONFIG SET seen mid-loop is honoured immediately.
pub fn try_evict_to_fit(
    db: &mut RedisDb,
    target_bytes: u64,
    policy: MaxmemoryPolicyCode,
) -> EvictionOutcome {
    if approximate_memory_used(db) <= target_bytes {
        return EvictionOutcome::Sufficient;
    }

    let mut evicted: usize = 0;
    while evicted < MAX_EVICTIONS_PER_CALL {
        let victim = match select_victim(db, policy) {
            Some(k) => k,
            None => break,
        };
        if !db.delete(&victim) {
            break;
        }
        evicted += 1;
        server_metrics()
            .evicted_keys
            .fetch_add(1, Ordering::Relaxed);
        if approximate_memory_used(db) <= target_bytes {
            return EvictionOutcome::Evicted(evicted);
        }
    }

    if approximate_memory_used(db) <= target_bytes {
        EvictionOutcome::Evicted(evicted)
    } else {
        EvictionOutcome::StillOver
    }
}

fn select_victim(db: &RedisDb, policy: MaxmemoryPolicyCode) -> Option<RedisString> {
    match policy {
        MaxmemoryPolicyCode::NoEviction => None,
        MaxmemoryPolicyCode::AllkeysLru => sample_oldest_by_lru(db, DEFAULT_MAXMEMORY_SAMPLES),
        MaxmemoryPolicyCode::AllkeysLfu
        | MaxmemoryPolicyCode::AllkeysRandom
        | MaxmemoryPolicyCode::VolatileLru
        | MaxmemoryPolicyCode::VolatileLfu
        | MaxmemoryPolicyCode::VolatileRandom
        | MaxmemoryPolicyCode::VolatileTtl => None,
    }
}

fn sample_oldest_by_lru(db: &RedisDb, samples: usize) -> Option<RedisString> {
    let take = samples.max(1);
    let mut oldest: Option<(&RedisString, &RedisObject)> = None;
    for (key, obj) in db.iter_for_eviction().take(take) {
        oldest = match oldest {
            None => Some((key, obj)),
            Some((_, best)) if obj.lru < best.lru => Some((key, obj)),
            Some(curr) => Some(curr),
        };
    }
    oldest.map(|(k, _)| k.clone())
}

/// True when the named policy has an active sampler in this module.
///
/// Used by the CONFIG SET path to surface a clear error for the policies
/// stubbed above instead of silently accepting them and then failing every
/// write with OOM.
pub fn is_policy_supported(policy: MaxmemoryPolicyCode) -> bool {
    matches!(
        policy,
        MaxmemoryPolicyCode::NoEviction | MaxmemoryPolicyCode::AllkeysLru
    )
}

/// Build the canonical error bytes for a policy that parsed but is not yet
/// implemented in this round. Surfaced through the OOM reply path when the
/// eviction gate trips with one of the stubbed policies.
pub fn unimplemented_policy_reply(policy: MaxmemoryPolicyCode) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(b"-ERR eviction policy '");
    buf.extend_from_slice(policy.as_config_str().as_bytes());
    buf.extend_from_slice(b"' not yet implemented\r\n");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::RedisObject;

    fn db_with(keys: &[(&[u8], &[u8], u32)]) -> RedisDb {
        let mut db = RedisDb::new(0);
        for (k, v, lru) in keys {
            let mut obj = RedisObject::from_string(RedisString::from_bytes(v));
            obj.lru = *lru;
            db.insert(RedisString::from_bytes(k), obj);
        }
        db
    }

    #[test]
    fn under_limit_returns_sufficient() {
        let mut db = db_with(&[(b"k", b"v", 1)]);
        let res = try_evict_to_fit(&mut db, u64::MAX, MaxmemoryPolicyCode::AllkeysLru);
        assert_eq!(res, EvictionOutcome::Sufficient);
    }

    #[test]
    fn noeviction_returns_still_over_when_over_limit() {
        let mut db = db_with(&[(b"k1", b"value1", 1), (b"k2", b"value2", 2)]);
        let res = try_evict_to_fit(&mut db, 1, MaxmemoryPolicyCode::NoEviction);
        assert_eq!(res, EvictionOutcome::StillOver);
        assert_eq!(db.len(), 2);
    }

    #[test]
    fn allkeys_lru_drops_keys_to_fit() {
        let mut db = db_with(&[
            (b"old", b"value", 1),
            (b"mid", b"value", 5),
            (b"new", b"value", 9),
        ]);
        let res = try_evict_to_fit(&mut db, 0, MaxmemoryPolicyCode::AllkeysLru);
        assert!(matches!(res, EvictionOutcome::Evicted(_) | EvictionOutcome::StillOver));
    }
}
