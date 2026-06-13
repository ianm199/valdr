//! Replication backbone: master-side state, backlog, replica registry.
//! Process-wide replication types and state management:
//! * [`ReplBacklog`] — circular byte buffer of recent write-command output,
//! consulted by PSYNC to decide between partial resync (`+CONTINUE`)
//! full resync (`+FULLRESYNC`).
//! * [`ReplicationState`] — process-wide replication state (run id, master
//! offset, backlog, connected replicas, optional replica-of target).
//! * [`ReplicaConn`] — per-replica metadata + outbound mpsc sender
//! for delivering bytes to replicas without the master re-acquiring the socket.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{
    AtomicBool, AtomicI32, AtomicI64, AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize,
    Ordering,
};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_types::RedisString;

use crate::client::ClientId;

/// Default size of the replication backlog in bytes (1 MiB).
pub const DEFAULT_REPL_BACKLOG_SIZE: usize = 1024 * 1024;

/// Replication role / connection state codes stored
/// [`ReplicationState::repl_state`].
pub mod repl_state_code {
    /// Server is operating as a master (default at startup). Replicas may
    /// attach. The server itself does not connect to any upstream.
    pub const MASTER: u8 = 0;
    /// Server has been told to `REPLICAOF host port` and is in the middle
    /// dialling the master; replica-side handshake has not finished. Wave C
    /// owns the transitions out of this state.
    pub const REPLICA_CONNECTING: u8 = 1;
    /// Replica-side handshake completed, applying streamed commands. Wave C
    /// owns this state. The struct fields are wired so Wave C can flip
    /// without changing the shape.
    pub const REPLICA_ONLINE: u8 = 2;
}

/// Fine-grained replica-side link state, published by the dialer purely for
/// observability (`ROLE` reply state field, upstream `replicaStateToString`).
/// The coarse [`repl_state_code`] still drives propagation/ACK logic; this is a
/// faithful mirror of the C `server.repl_state` handshake phases so `ROLE`
/// reports `connect`/`connecting`/`handshake`/`sync`/`connected` like Valkey.
pub mod replica_link_code {
    /// Not connected; the dialer is between reconnect attempts (`connect`).
    pub const CONNECT: u8 = 0;
    /// A TCP connection to the primary is being established (`connecting`).
    pub const CONNECTING: u8 = 1;
    /// Connected; PING/REPLCONF/PSYNC exchange in progress and the replica is
    /// awaiting the `+FULLRESYNC`/`+CONTINUE` reply (`handshake`).
    pub const HANDSHAKE: u8 = 2;
    /// `+FULLRESYNC` received; the RDB bulk payload is being received (`sync`).
    pub const TRANSFER: u8 = 3;
    /// RDB loaded; streaming live command deltas (`connected`).
    pub const CONNECTED: u8 = 4;

    /// Map a link-state code to the `ROLE` reply spelling used by upstream
    /// `replicaStateToString`.
    pub fn as_role_str(code: u8) -> &'static str {
        match code {
            CONNECT => "connect",
            CONNECTING => "connecting",
            HANDSHAKE => "handshake",
            TRANSFER => "sync",
            CONNECTED => "connected",
            _ => "unknown",
        }
    }
}

/// Per-replica connection state. Drives whether the master will stream
/// backlog to a given replica yet (it has to wait for the BGSAVE RDB transfer
/// to land first when full-syncing).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaState {
    /// Replica is waiting for the master's BGSAVE child to finish so the RDB
    /// snapshot can be shipped. Backlog deltas accumulated during the BGSAVE
    /// are held back until after the snapshot is delivered.
    WaitingBgsave = 0,
    /// RDB snapshot is being streamed to the replica.
    SendingRdb = 1,
    /// RDB delivered. Replica is consuming backlog deltas in real time.
    Online = 2,
    /// Replica disconnected. The entry stays in the registry until
    /// reader thread reaps it.
    Disconnected = 3,
}

impl ReplicaState {
    /// Reconstruct from the wire-stored discriminant. Unknown values map
    /// `Disconnected` rather than panicking; the registry is allowed to hold
    /// `Disconnected` entries that are later swept by the reaper.
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::WaitingBgsave,
            1 => Self::SendingRdb,
            2 => Self::Online,
            _ => Self::Disconnected,
        }
    }

    /// Canonical string spelling used by the `INFO replication` `state=` field
    /// (`wait_bgsave`, `send_bulk`, `online`, `disconnected`). Matches
    /// upstream `slave_state_str` output.
    pub fn as_info_str(self) -> &'static str {
        match self {
            Self::WaitingBgsave => "wait_bgsave",
            Self::SendingRdb => "send_bulk",
            Self::Online => "online",
            Self::Disconnected => "disconnected",
        }
    }
}

/// Replication backlog: a circular byte buffer holding the most recent
/// `size` bytes of write-command stream. The master keeps an absolute offset
/// counter (`offset`) of the next-write position; partial-resync decides
/// whether a replica's requested offset lies inside the live window.
pub struct ReplBacklog {
    /// Allocated capacity. Once non-zero this never changes; CONFIG SET
    /// `repl-backlog-size` allocates a fresh `ReplBacklog` rather than
    /// re-sizing the live buffer.
    pub size: usize,
    /// Raw buffer of length `size`. The circular index for absolute offset
    /// `off` is `(off % size as i64) as usize` once `histlen` reaches `size`.
    pub buffer: Vec<u8>,
    /// Absolute offset of the next byte the buffer will receive. Equals
    /// total number of bytes ever appended.
    pub offset: i64,
    /// Number of valid bytes currently in the buffer (saturates at `size`).
    pub histlen: usize,
}

impl ReplBacklog {
    /// Allocate a backlog of `size` bytes. The buffer is filled with zeros up
    /// front so the circular wrap-around does not need a separate
    /// "initialised up to" cursor.
    pub fn new(size: usize) -> Self {
        Self {
            size,
            buffer: vec![0u8; size],
            offset: 0,
            histlen: 0,
        }
    }

    /// Append `bytes` to the backlog and return the new absolute offset.
    /// When `bytes.len` exceeds `size`, only the trailing `size` bytes are
    /// retained (the older portion is effectively overwritten by the wrap).
    /// Callers should still pass the full slice.
    pub fn append(&mut self, bytes: &[u8]) -> i64 {
        if self.size == 0 {
            self.offset = self.offset.saturating_add(bytes.len() as i64);
            return self.offset;
        }
        for &b in bytes {
            let idx = (self.offset as usize) % self.size;
            self.buffer[idx] = b;
            self.offset = self.offset.saturating_add(1);
        }
        let new_hist = self.histlen.saturating_add(bytes.len());
        self.histlen = new_hist.min(self.size);
        self.offset
    }

    /// Lowest absolute offset still readable from the backlog. A replica that
    /// asks for an offset below this must full-resync.
    pub fn min_offset(&self) -> i64 {
        self.offset.saturating_sub(self.histlen as i64)
    }

    /// Read up to `max_len` bytes starting at absolute `offset`. Returns
    /// `None` when `offset` falls outside the live window (either below
    /// `min_offset` or above the current write head).
    pub fn read_at(&self, offset: i64, max_len: usize) -> Option<Vec<u8>> {
        if offset < self.min_offset() || offset > self.offset {
            return None;
        }
        let available = (self.offset - offset) as usize;
        let n = available.min(max_len);
        if n == 0 {
            return Some(Vec::new());
        }
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let abs = offset as usize + i;
            out.push(self.buffer[abs % self.size]);
        }
        Some(out)
    }
}

/// Per-replica record kept in [`ReplicationState::replicas`].
/// The outbound mpsc sender is the same writer-thread channel
/// `PubSubRegistry::register_sender` installs at connection accept time —
/// see [`steal_replica_sender`] for the lookup pattern.
pub struct ReplicaConn {
    /// Replica's master-assigned client id (same id the regular dispatch
    /// path issued for this socket).
    pub client_id: ClientId,
    /// Discriminant of [`ReplicaState`] — see [`ReplicaState::from_u8`].
    pub state: AtomicU8,
    /// Last replication offset the replica acknowledged via REPLCONF ACK.
    /// Wave B's REPLCONF ACK handler updates this; for now it just tracks
    /// the snapshot/partial-resync starting offset.
    pub offset: AtomicI64,
    /// Last replication offset the replica reported as fsynced to its AOF via
    /// `REPLCONF ACK <off> FACK <aof-off>`. `-1` means the replica has not
    /// advertised an AOF fsync offset, which matches upstream's "AOF disabled"
    /// sentinel.
    pub aof_offset: AtomicI64,
    /// Listening port the replica is exposing for client connections, set by
    /// `REPLCONF listening-port <port>` (Wave B). 0 until reported.
    pub listening_port: AtomicU16,
    /// Capability flags reported by the replica via `REPLCONF capa`. Wave B
    /// parses the symbolic names (`eof`, `psync2`, …) into bits.
    pub capa_flags: AtomicU32,
    /// Unix millisecond timestamp of the last REPLCONF ACK seen from
    /// replica. Drives the `lag` field in `INFO replication`.
    pub last_ack_time_ms: AtomicI64,
    /// Approximate bytes queued to the replica writer thread. This mirrors
    /// backlog that Valkey reports as slave/client output memory so INFO can
    /// exclude it from key-eviction pressure.
    pub pending_output_bytes: AtomicUsize,
    /// Outbound mpsc sender — the writer-thread channel the master pushes
    /// backlog deltas and the RDB blob through.
    pub outbound_sender: Sender<Vec<u8>>,
}

impl ReplicaConn {
    /// Construct a fresh replica record. The caller is responsible for
    /// inserting it into [`ReplicationState::replicas`].
    pub fn new(
        client_id: ClientId,
        state: ReplicaState,
        offset: i64,
        outbound_sender: Sender<Vec<u8>>,
    ) -> Self {
        Self {
            client_id,
            state: AtomicU8::new(state as u8),
            offset: AtomicI64::new(offset),
            aof_offset: AtomicI64::new(-1),
            listening_port: AtomicU16::new(0),
            capa_flags: AtomicU32::new(0),
            last_ack_time_ms: AtomicI64::new(0),
            pending_output_bytes: AtomicUsize::new(0),
            outbound_sender,
        }
    }

    /// Read the replica's current state.
    pub fn state(&self) -> ReplicaState {
        ReplicaState::from_u8(self.state.load(Ordering::Relaxed))
    }

    /// Update the replica's state. Caller is responsible for ordering this
    /// with any side-effects on the outbound writer.
    pub fn set_state(&self, state: ReplicaState) {
        self.state.store(state as u8, Ordering::Relaxed);
    }

    /// Read the last acknowledged offset.
    pub fn offset(&self) -> i64 {
        self.offset.load(Ordering::Relaxed)
    }

    /// Read the last acknowledged AOF-fsynced offset.
    pub fn aof_offset(&self) -> i64 {
        self.aof_offset.load(Ordering::Relaxed)
    }

    /// Listening port reported by the replica via REPLCONF.
    pub fn listening_port(&self) -> u16 {
        self.listening_port.load(Ordering::Relaxed)
    }

    /// Capability flag bitset reported by the replica via REPLCONF capa.
    pub fn capa_flags(&self) -> u32 {
        self.capa_flags.load(Ordering::Relaxed)
    }
}

/// Bookkeeping for an in-flight BGSAVE-for-replication job.
/// Disk-based full-sync forks a child that writes an RDB snapshot to a temp
/// file. While the child is alive, additional replicas may PSYNC ? -1
/// they join the same job's `waiting_replicas` list — every waiter gets
/// same RDB and the same catch-up window once the child exits successfully.
pub struct ReplBgsaveJob {
    /// PID of the forked child writing the RDB.
    pub child_pid: i32,
    /// Path of the temp RDB file the child is producing. Deleted after
    /// transfer completes (success or failure).
    pub temp_path: PathBuf,
    /// `client_id`s of replicas waiting for this RDB to land. New replicas
    /// joining mid-snapshot are appended to this list.
    pub waiting_replicas: Vec<ClientId>,
    /// Master replication offset at the moment the BGSAVE was forked. Catch-up
    /// backlog after the RDB send streams bytes from this offset to
    /// current master offset, so the replica receives every write that arrived
    /// during the snapshot window.
    pub snapshot_offset: i64,
    /// Replication bytes appended after `snapshot_offset` while the snapshot
    /// child is running. This is Valdr's shared full-sync catch-up buffer:
    /// it lets waiters receive the complete post-RDB stream even when the
    /// configured circular backlog has wrapped.
    pub catch_up_bytes: Vec<u8>,
    /// Whether this full-sync was armed while a WAIT/WAITAOF client was
    /// blocked. The reaper uses this to prompt replicas for an ACK after
    /// RDB transfer without emitting GETACK for ordinary replication streams.
    pub needs_getack_on_completion: bool,
}

/// Immutable replication bytes retained after a full-sync RDB has been queued
/// to one or more replicas. The bytes remain readable for PSYNC while at least
/// one dependent replica may still need them to finish consuming the stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedReplHistory {
    /// Absolute offset of the first retained byte.
    pub start_offset: i64,
    /// RESP command stream bytes beginning at `start_offset`.
    pub bytes: Vec<u8>,
    /// Replica client ids that still pin this segment.
    pub owners: Vec<ClientId>,
}

impl RetainedReplHistory {
    /// Absolute offset one byte past the retained segment.
    pub fn end_offset(&self) -> i64 {
        self.start_offset.saturating_add(self.bytes.len() as i64)
    }
}

/// Process-wide replication state.
/// One instance is installed via [`install_replication_state`] at startup
/// looked up everywhere through [`global_replication_state`].
pub struct ReplicationState {
    /// 40-byte lowercase hex run id generated once at startup. Identifies
    /// this server's replication history; replicas embed it in PSYNC so a
    /// partial resync only succeeds when the run id matches.
    pub runid: [u8; 40],
    /// Total bytes ever appended to the replication stream. Equals
    /// backlog's `offset` after every append.
    pub master_repl_offset: AtomicI64,
    /// The backlog circular buffer.
    pub backlog: Mutex<ReplBacklog>,
    /// Connected replicas (master-assigned `client_id` → metadata).
    pub replicas: Mutex<HashMap<ClientId, ReplicaConn>>,
    /// `Some((host, port))` when this server has been told `REPLICAOF host
    /// port`; `None` when it is operating as a primary.
    pub replica_of: Mutex<Option<(RedisString, u16)>>,
    /// Top-level role/state code; see [`repl_state_code`].
    pub repl_state: AtomicU8,
    /// Fine-grained replica-side link phase; see [`replica_link_code`]. Set by
    /// the dialer for `ROLE`-reply observability only. Meaningless on a primary.
    pub replica_link: AtomicU8,
    /// PID of the in-flight BGSAVE-for-replication child, or 0 when no such
    /// child is running. Tracked separately from `RedisServer::rdb_child_pid`
    /// so a user-issued `BGSAVE` does not interfere with replica full-sync.
    pub repl_child_pid: AtomicI32,
    /// Active full-sync job. `Some` from the fork until the reaper has
    /// finished shipping the RDB and catch-up bytes to every waiter; then
    /// reset to `None`.
    pub repl_bgsave_job: Mutex<Option<ReplBgsaveJob>>,
    /// Full-sync catch-up history retained after the BGSAVE job has been
    /// consumed. This models Valkey's shared replication buffer lifetime well
    /// enough for slow full-sync waiters and PSYNC decisions to keep seeing the
    /// same bytes after the child exits.
    pub retained_history: Mutex<Vec<RetainedReplHistory>>,
    /// Database id last emitted into the replication command stream.
    /// Upstream tracks this as `server.slaveseldb` so the first write after
    /// a replica attaches is prefixed with `SELECT <db>`, and later writes
    /// only pay the SELECT frame when the selected DB changes.
    pub selected_db: AtomicI32,
    /// Set to `true` by `REPLICAOF NO ONE` to signal the running dialer thread
    /// to exit its reconnection loop immediately.
    pub dialer_stop_flag: AtomicBool,
    /// Monotonic generation for replica-side dialer threads.
    /// `REPLICAOF <host> <port>` can retarget an already-running replica.
    /// A boolean stop flag is insufficient because the new dialer clears it
    /// while the old dialer may still be reading from the previous master.
    /// Dialers capture this epoch at spawn and must stop applying bytes or
    /// sending ACKs once it changes.
    pub dialer_epoch: AtomicU64,
    /// `INFO stats sync_full` — count of full resyncs served to replicas
    /// (mirrors C `server.stat_sync_full`).
    pub stat_sync_full: AtomicU64,
    /// `INFO stats sync_partial_ok` — count of successful partial resyncs
    /// (`+CONTINUE`), mirrors C `server.stat_sync_partial_ok`.
    pub stat_sync_partial_ok: AtomicU64,
    /// `INFO stats sync_partial_err` — count of partial-resync requests that
    /// could not be satisfied and fell back to full resync (mirrors C
    /// `server.stat_sync_partial_err`).
    pub stat_sync_partial_err: AtomicU64,
    /// The primary replid the replica adopted from the last `+FULLRESYNC`
    /// reply. `None` until the first full sync completes. On reconnect the
    /// dialer echoes this in `PSYNC <replid> <offset>` so the primary's
    /// run-id check can grant a `+CONTINUE` partial resync.
    pub cached_primary_replid: Mutex<Option<[u8; 40]>>,
}

impl ReplicationState {
    /// Allocate state for a fresh standalone primary. `backlog_size` is
    /// usually [`DEFAULT_REPL_BACKLOG_SIZE`]; CLI / CONFIG SET feeds an
    /// override at startup.
    pub fn new(runid: [u8; 40], backlog_size: usize) -> Self {
        Self {
            runid,
            master_repl_offset: AtomicI64::new(0),
            backlog: Mutex::new(ReplBacklog::new(backlog_size)),
            replicas: Mutex::new(HashMap::new()),
            replica_of: Mutex::new(None),
            repl_state: AtomicU8::new(repl_state_code::MASTER),
            replica_link: AtomicU8::new(replica_link_code::CONNECT),
            repl_child_pid: AtomicI32::new(0),
            repl_bgsave_job: Mutex::new(None),
            retained_history: Mutex::new(Vec::new()),
            selected_db: AtomicI32::new(-1),
            dialer_stop_flag: AtomicBool::new(false),
            dialer_epoch: AtomicU64::new(0),
            stat_sync_full: AtomicU64::new(0),
            stat_sync_partial_ok: AtomicU64::new(0),
            stat_sync_partial_err: AtomicU64::new(0),
            cached_primary_replid: Mutex::new(None),
        }
    }

    /// Increment the full-resync served counter (`INFO sync_full`).
    pub fn incr_sync_full(&self) {
        self.stat_sync_full.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the successful partial-resync counter (`INFO sync_partial_ok`).
    pub fn incr_sync_partial_ok(&self) {
        self.stat_sync_partial_ok.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the failed partial-resync counter (`INFO sync_partial_err`).
    pub fn incr_sync_partial_err(&self) {
        self.stat_sync_partial_err.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot of the three sync counters as
    /// `(sync_full, sync_partial_ok, sync_partial_err)` for `INFO stats`.
    pub fn sync_counters(&self) -> (u64, u64, u64) {
        (
            self.stat_sync_full.load(Ordering::Relaxed),
            self.stat_sync_partial_ok.load(Ordering::Relaxed),
            self.stat_sync_partial_err.load(Ordering::Relaxed),
        )
    }

    /// Adopt the primary's replid from a `+FULLRESYNC` reply (replica side).
    pub fn set_cached_primary_replid(&self, replid: [u8; 40]) {
        match self.cached_primary_replid.lock() {
            Ok(mut g) => *g = Some(replid),
            Err(p) => *p.into_inner() = Some(replid),
        }
    }

    /// The cached primary replid for a partial-resync `PSYNC`, if a full sync
    /// has completed at least once.
    pub fn cached_primary_replid(&self) -> Option<[u8; 40]> {
        match self.cached_primary_replid.lock() {
            Ok(g) => *g,
            Err(p) => *p.into_inner(),
        }
    }

    /// Return the run id as a `&[u8; 40]` for callers that want to embed it
    /// in a reply line.
    pub fn runid(&self) -> &[u8; 40] {
        &self.runid
    }

    /// Current master replication offset.
    pub fn master_offset(&self) -> i64 {
        self.master_repl_offset.load(Ordering::Relaxed)
    }

    /// Append `bytes` to the backlog and bump the master offset.
    pub fn append_to_backlog(&self, bytes: &[u8]) -> i64 {
        let new_offset = match self.backlog.lock() {
            Ok(mut g) => g.append(bytes),
            Err(p) => p.into_inner().append(bytes),
        };
        self.append_to_repl_bgsave_catchup(bytes);
        self.master_repl_offset.store(new_offset, Ordering::Relaxed);
        new_offset
    }

    fn append_to_repl_bgsave_catchup(&self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let mut guard = match self.repl_bgsave_job.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(job) = guard.as_mut() {
            job.catch_up_bytes.extend_from_slice(bytes);
        }
    }

    /// True when a successful write command needs replication-stream work.
    /// Upstream Valkey does not format and feed the replication stream when it
    /// is a standalone primary with AOF off, no backlog, and no connected
    /// replicas. The Rust port always has a `ReplBacklog` value allocated for
    /// simpler ownership, so "backlog exists" maps to "history is active".
    pub fn should_propagate_writes(&self) -> bool {
        if self.repl_state.load(Ordering::Relaxed) != repl_state_code::MASTER {
            return false;
        }

        let has_replicas = match self.replicas.lock() {
            Ok(g) => !g.is_empty(),
            Err(p) => !p.into_inner().is_empty(),
        };
        if has_replicas {
            return true;
        }

        let backlog_active = match self.backlog.lock() {
            Ok(g) => g.histlen > 0,
            Err(p) => p.into_inner().histlen > 0,
        };
        if backlog_active {
            return true;
        }

        match self.repl_bgsave_job.lock() {
            Ok(g) => g.is_some(),
            Err(p) => p.into_inner().is_some(),
        }
    }

    /// Snapshot `(min_offset, master_offset, backlog_histlen, backlog_size)`.
    /// During a full-sync BGSAVE, the readable history can extend past the
    /// configured circular backlog because the active job holds a shared
    /// catch-up buffer for waiting replicas.
    pub fn backlog_snapshot(&self) -> (i64, i64, usize, usize) {
        let master = self.master_repl_offset.load(Ordering::Relaxed);
        let (backlog_min, backlog_hist, size, backlog_offset) = match self.backlog.lock() {
            Ok(g) => (g.min_offset(), g.histlen, g.size, g.offset),
            Err(p) => {
                let g = p.into_inner();
                (g.min_offset(), g.histlen, g.size, g.offset)
            }
        };

        let mut intervals = Vec::new();
        if backlog_hist > 0 {
            intervals.push((backlog_min, backlog_offset));
        }
        match self.repl_bgsave_job.lock() {
            Ok(g) => {
                if let Some(job) = g.as_ref().filter(|job| !job.catch_up_bytes.is_empty()) {
                    intervals.push((
                        job.snapshot_offset,
                        job.snapshot_offset
                            .saturating_add(job.catch_up_bytes.len() as i64),
                    ));
                }
            }
            Err(p) => {
                if let Some(job) = p
                    .into_inner()
                    .as_ref()
                    .filter(|job| !job.catch_up_bytes.is_empty())
                {
                    intervals.push((
                        job.snapshot_offset,
                        job.snapshot_offset
                            .saturating_add(job.catch_up_bytes.len() as i64),
                    ));
                }
            }
        }
        match self.retained_history.lock() {
            Ok(g) => {
                for segment in g.iter().filter(|segment| !segment.bytes.is_empty()) {
                    intervals.push((segment.start_offset, segment.end_offset()));
                }
            }
            Err(p) => {
                for segment in p
                    .into_inner()
                    .iter()
                    .filter(|segment| !segment.bytes.is_empty())
                {
                    intervals.push((segment.start_offset, segment.end_offset()));
                }
            }
        }

        let min = contiguous_history_min(master, &intervals);
        let hist = master.saturating_sub(min).max(0) as usize;
        (min, master, hist, size)
    }

    /// Read replication history from either the configured circular backlog or
    /// shared full-sync catch-up buffers. Returns `None` when `offset` is older
    /// than every readable window, past the current master offset, or when the
    /// requested range crosses a gap between retained history and the backlog.
    pub fn read_history_at(&self, offset: i64, max_len: usize) -> Option<Vec<u8>> {
        let master = self.master_repl_offset.load(Ordering::Relaxed);
        if offset > master {
            return None;
        }
        if max_len == 0 {
            return Some(Vec::new());
        }

        let target_end = offset.saturating_add(max_len as i64).min(master);
        let mut cursor = offset;
        let mut out = Vec::with_capacity(target_end.saturating_sub(offset) as usize);
        while cursor < target_end {
            let remaining = target_end.saturating_sub(cursor) as usize;
            let mut best: Option<Vec<u8>> = None;
            let mut best_end = cursor;

            {
                let guard = match self.retained_history.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                for segment in guard.iter() {
                    if segment.bytes.is_empty() {
                        continue;
                    }
                    let segment_end = segment.end_offset();
                    if cursor < segment.start_offset || cursor >= segment_end {
                        continue;
                    }
                    let start = cursor.saturating_sub(segment.start_offset) as usize;
                    let end = start.saturating_add(remaining).min(segment.bytes.len());
                    let candidate_end = cursor.saturating_add((end - start) as i64);
                    if candidate_end > best_end {
                        best = Some(segment.bytes[start..end].to_vec());
                        best_end = candidate_end;
                    }
                }
            }

            {
                let guard = match self.repl_bgsave_job.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                if let Some(job) = guard.as_ref().filter(|job| !job.catch_up_bytes.is_empty()) {
                    let job_end = job
                        .snapshot_offset
                        .saturating_add(job.catch_up_bytes.len() as i64);
                    if cursor >= job.snapshot_offset && cursor < job_end {
                        let start = cursor.saturating_sub(job.snapshot_offset) as usize;
                        let end = start
                            .saturating_add(remaining)
                            .min(job.catch_up_bytes.len());
                        let candidate_end = cursor.saturating_add((end - start) as i64);
                        if candidate_end > best_end {
                            best = Some(job.catch_up_bytes[start..end].to_vec());
                            best_end = candidate_end;
                        }
                    }
                }
            }

            let from_backlog = {
                let guard = match self.backlog.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.read_at(cursor, remaining)
            };
            if let Some(bytes) = from_backlog.filter(|bytes| !bytes.is_empty()) {
                let candidate_end = cursor.saturating_add(bytes.len() as i64);
                if candidate_end > best_end {
                    best = Some(bytes);
                }
            }

            let bytes = best?;
            if bytes.is_empty() {
                return None;
            }
            cursor = cursor.saturating_add(bytes.len() as i64);
            out.extend_from_slice(&bytes);
        }

        Some(out)
    }

    /// True when the entire range `[offset, end_offset)` can be read from the
    /// current replication history, including any retained full-sync segments.
    pub fn can_read_history_range(&self, offset: i64, end_offset: i64) -> bool {
        if offset > end_offset {
            return false;
        }
        let master = self.master_repl_offset.load(Ordering::Relaxed);
        if end_offset > master {
            return false;
        }
        if offset == end_offset {
            return true;
        }

        let mut intervals = Vec::new();
        match self.backlog.lock() {
            Ok(g) => {
                if g.histlen > 0 {
                    intervals.push((g.min_offset(), g.offset));
                }
            }
            Err(p) => {
                let g = p.into_inner();
                if g.histlen > 0 {
                    intervals.push((g.min_offset(), g.offset));
                }
            }
        }
        match self.repl_bgsave_job.lock() {
            Ok(g) => {
                if let Some(job) = g.as_ref().filter(|job| !job.catch_up_bytes.is_empty()) {
                    intervals.push((
                        job.snapshot_offset,
                        job.snapshot_offset
                            .saturating_add(job.catch_up_bytes.len() as i64),
                    ));
                }
            }
            Err(p) => {
                if let Some(job) = p
                    .into_inner()
                    .as_ref()
                    .filter(|job| !job.catch_up_bytes.is_empty())
                {
                    intervals.push((
                        job.snapshot_offset,
                        job.snapshot_offset
                            .saturating_add(job.catch_up_bytes.len() as i64),
                    ));
                }
            }
        }
        match self.retained_history.lock() {
            Ok(g) => {
                for segment in g.iter().filter(|segment| !segment.bytes.is_empty()) {
                    intervals.push((segment.start_offset, segment.end_offset()));
                }
            }
            Err(p) => {
                for segment in p
                    .into_inner()
                    .iter()
                    .filter(|segment| !segment.bytes.is_empty())
                {
                    intervals.push((segment.start_offset, segment.end_offset()));
                }
            }
        }

        range_covered_by_intervals(offset, end_offset, &intervals)
    }

    /// Retain a completed full-sync catch-up segment while the replicas that
    /// received it may still be consuming those bytes from their sockets.
    pub fn retain_fullsync_history(&self, start_offset: i64, bytes: Vec<u8>, owners: &[ClientId]) {
        if bytes.is_empty() {
            return;
        }
        let mut owners = owners.to_vec();
        owners.sort_unstable();
        owners.dedup();
        if owners.is_empty() {
            return;
        }
        let mut guard = match self.retained_history.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.push(RetainedReplHistory {
            start_offset,
            bytes,
            owners,
        });
    }

    /// Release retained history pinned by a disconnecting replica.
    pub fn release_retained_history_for(&self, client_id: ClientId) {
        let mut guard = match self.retained_history.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for segment in guard.iter_mut() {
            segment.owners.retain(|owner| *owner != client_id);
        }
        guard.retain(|segment| !segment.owners.is_empty());
    }

    /// Release retained segments once a replica ACK proves it consumed through
    /// the end of that segment.
    pub fn release_retained_history_ack(&self, client_id: ClientId, offset: i64) {
        let mut guard = match self.retained_history.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for segment in guard.iter_mut() {
            if offset >= segment.end_offset() {
                segment.owners.retain(|owner| *owner != client_id);
            }
        }
        guard.retain(|segment| !segment.owners.is_empty());
    }

    /// Bytes retained after full-sync transfer completion.
    pub fn retained_repl_history_len(&self) -> usize {
        match self.retained_history.lock() {
            Ok(g) => g.iter().map(|segment| segment.bytes.len()).sum(),
            Err(p) => p
                .into_inner()
                .iter()
                .map(|segment| segment.bytes.len())
                .sum(),
        }
    }

    /// Bytes held outside the circular backlog for active and completed
    /// full-sync catch-up windows.
    pub fn replication_history_extra_len(&self) -> usize {
        self.repl_bgsave_catchup_len()
            .saturating_add(self.retained_repl_history_len())
    }

    /// Bytes currently held for in-flight full-sync catch-up.
    pub fn repl_bgsave_catchup_len(&self) -> usize {
        match self.repl_bgsave_job.lock() {
            Ok(g) => g.as_ref().map_or(0, |job| job.catch_up_bytes.len()),
            Err(p) => p
                .into_inner()
                .as_ref()
                .map_or(0, |job| job.catch_up_bytes.len()),
        }
    }

    /// True when this server is currently a replica of some master.
    pub fn is_replica(&self) -> bool {
        self.repl_state.load(Ordering::Relaxed) != repl_state_code::MASTER
    }

    /// Configured master address `(host, port)` when in replica mode.
    pub fn replica_of_target(&self) -> Option<(RedisString, u16)> {
        match self.replica_of.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Switch this server out of replica mode (`REPLICAOF NO ONE`). Resets
    /// `replica_of` to `None` and `repl_state` to MASTER. Signals the running
    /// dialer thread to exit via `dialer_stop_flag`.
    pub fn become_master(&self) {
        self.dialer_epoch.fetch_add(1, Ordering::SeqCst);
        self.dialer_stop_flag.store(true, Ordering::SeqCst);
        match self.replica_of.lock() {
            Ok(mut g) => *g = None,
            Err(p) => *p.into_inner() = None,
        }
        self.repl_state
            .store(repl_state_code::MASTER, Ordering::Relaxed);
        self.replica_link
            .store(replica_link_code::CONNECT, Ordering::Relaxed);
    }

    /// Publish the fine-grained replica link phase (see [`replica_link_code`]).
    /// Dialer-only; primaries never call this.
    pub fn set_replica_link(&self, code: u8) {
        self.replica_link.store(code, Ordering::Relaxed);
    }

    /// Current replica link phase rendered as the `ROLE`-reply state string.
    pub fn replica_link_str(&self) -> &'static str {
        replica_link_code::as_role_str(self.replica_link.load(Ordering::Relaxed))
    }

    /// Configure this server as a replica of `(host, port)` and move
    /// top-level state to `REPLICA_CONNECTING`. Clears the dialer stop flag so
    /// a freshly-spawned dialer thread is not immediately told to quit.
    pub fn become_replica_of(&self, host: RedisString, port: u16) -> u64 {
        let epoch = self
            .dialer_epoch
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dialer_stop_flag.store(false, Ordering::SeqCst);
        let target_changed = match self.replica_of.lock() {
            Ok(mut g) => {
                let changed = g
                    .as_ref()
                    .is_none_or(|(old_host, old_port)| old_host != &host || *old_port != port);
                *g = Some((host, port));
                changed
            }
            Err(p) => {
                let mut g = p.into_inner();
                let changed = g
                    .as_ref()
                    .is_none_or(|(old_host, old_port)| old_host != &host || *old_port != port);
                *g = Some((host, port));
                changed
            }
        };
        if target_changed {
            match self.cached_primary_replid.lock() {
                Ok(mut g) => *g = None,
                Err(p) => *p.into_inner() = None,
            }
            self.master_repl_offset.store(0, Ordering::Relaxed);
        }
        self.repl_state
            .store(repl_state_code::REPLICA_CONNECTING, Ordering::Relaxed);
        self.replica_link
            .store(replica_link_code::CONNECT, Ordering::Relaxed);
        epoch
    }

    /// True if a replica-side dialer captured the currently-active generation.
    pub fn dialer_epoch_is_current(&self, epoch: u64) -> bool {
        !self.dialer_stop_flag.load(Ordering::SeqCst)
            && self.dialer_epoch.load(Ordering::SeqCst) == epoch
    }

    /// Snapshot of the connected replicas in a stable, sorted order keyed by
    /// `client_id`. Each entry is rendered as a tuple of fields ready for
    /// `INFO replication`'s `slave0:` / `slave1:` lines.
    /// Returned tuple shape:
    /// `(client_id, state_str, listening_port, offset, last_ack_ms)`.
    pub fn replicas_snapshot(&self) -> Vec<(ClientId, &'static str, u16, i64, i64)> {
        let guard = match self.replicas.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut out: Vec<(ClientId, &'static str, u16, i64, i64)> = guard
            .iter()
            .map(|(cid, r)| {
                (
                    *cid,
                    r.state().as_info_str(),
                    r.listening_port(),
                    r.offset(),
                    r.last_ack_time_ms.load(Ordering::Relaxed),
                )
            })
            .collect();
        out.sort_by_key(|e| e.0);
        out
    }

    /// Count of currently-connected replicas. Reads the registry length under
    /// the mutex; cheap because the entries are small.
    pub fn connected_replicas(&self) -> usize {
        match self.replicas.lock() {
            Ok(g) => g.len(),
            Err(p) => p.into_inner().len(),
        }
    }

    /// Count online replicas whose last ACK lag is within `max_lag_secs`.
    pub fn good_replicas_count(&self, max_lag_secs: u64) -> usize {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let max_lag_ms = (max_lag_secs as i64).saturating_mul(1000);
        let guard = match self.replicas.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard
            .values()
            .filter(|replica| {
                if replica.state() != ReplicaState::Online {
                    return false;
                }
                let last_ack = replica.last_ack_time_ms.load(Ordering::Relaxed);
                last_ack > 0 && now_ms.saturating_sub(last_ack) <= max_lag_ms
            })
            .count()
    }

    /// Register `replica` under its `client_id`. Replaces any prior entry for
    /// the same id (clients can only PSYNC once per connection so this
    /// should not race in practice).
    pub fn add_replica(&self, replica: ReplicaConn) {
        let cid = replica.client_id;
        match self.replicas.lock() {
            Ok(mut g) => {
                g.insert(cid, replica);
            }
            Err(p) => {
                p.into_inner().insert(cid, replica);
            }
        }
    }

    /// Drop the replica record for `client_id`, if present. Called from
    /// per-connection cleanup path when a replica disconnects.
    pub fn remove_replica(&self, client_id: ClientId) {
        match self.replicas.lock() {
            Ok(mut g) => {
                g.remove(&client_id);
            }
            Err(p) => {
                p.into_inner().remove(&client_id);
            }
        }
        self.release_retained_history_for(client_id);
        if let Ok(mut guard) = crate::client_info::client_info_registry().lock() {
            guard.set_output_buffer_memory(client_id, 0);
        }
    }

    /// PID of the in-flight BGSAVE-for-replication child, or 0 when no such
    /// child is running.
    pub fn repl_child_pid(&self) -> i32 {
        self.repl_child_pid.load(Ordering::SeqCst)
    }

    /// Record the PID of the newly-forked BGSAVE-for-replication child.
    pub fn set_repl_child_pid(&self, pid: i32) {
        self.repl_child_pid.store(pid, Ordering::SeqCst);
    }

    /// Install a fresh `ReplBgsaveJob`. Called from `bgsave_for_replication`
    /// once the fork has returned a child PID.
    pub fn install_repl_bgsave_job(&self, job: ReplBgsaveJob) {
        match self.repl_bgsave_job.lock() {
            Ok(mut g) => *g = Some(job),
            Err(p) => *p.into_inner() = Some(job),
        }
    }

    /// Remove and return the current `ReplBgsaveJob`. Called by the reaper
    /// after the child exits so the temp file path and waiting-replica list
    /// can be consumed without holding the mutex through the I/O.
    pub fn take_repl_bgsave_job(&self) -> Option<ReplBgsaveJob> {
        match self.repl_bgsave_job.lock() {
            Ok(mut g) => g.take(),
            Err(p) => p.into_inner().take(),
        }
    }

    /// Cleanup side effects for a failed replication BGSAVE job whose waiters
    /// will not receive an RDB. The live connection cleanup path will close
    /// sockets separately; this removes the replica records so stale
    /// `wait_bgsave` entries do not poison later full syncs.
    pub fn cleanup_failed_repl_bgsave_job(&self, job: &ReplBgsaveJob) {
        for client_id in &job.waiting_replicas {
            self.remove_replica(*client_id);
        }
        let _ = std::fs::remove_file(&job.temp_path);
        let _ = std::fs::remove_file(job.temp_path.with_extension("rdb.tmp"));
    }

    /// Abort the currently-installed replication BGSAVE job, if any, and clear
    /// process-wide replication-child state. Returns the consumed job so tests
    /// and callers can inspect which waiters were dropped.
    pub fn abort_repl_bgsave_job(&self) -> Option<ReplBgsaveJob> {
        let job = self.take_repl_bgsave_job();
        if let Some(job) = job.as_ref() {
            self.cleanup_failed_repl_bgsave_job(job);
        }
        self.set_repl_child_pid(0);
        job
    }

    /// Append `client_id` to the current job's waiting-replica list when a
    /// fresh PSYNC arrives while a BGSAVE is already running. Returns `true`
    /// if a job exists (so the caller can skip starting a new one); `false`
    /// when no job is in flight.
    pub fn enqueue_repl_waiter(&self, client_id: ClientId) -> bool {
        let mut guard = match self.repl_bgsave_job.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        match guard.as_mut() {
            Some(job) => {
                job.waiting_replicas.push(client_id);
                job.needs_getack_on_completion |=
                    crate::blocked_keys::blocked_replication_wait_any();
                true
            }
            None => false,
        }
    }

    /// Snapshot of waiting-replica `client_id`s without taking the job.
    /// Used by the reaper when it wants to walk the waiters and ship bytes
    /// without removing the job mid-flight.
    pub fn repl_bgsave_job_snapshot(&self) -> Option<(PathBuf, Vec<ClientId>, i64)> {
        let guard = match self.repl_bgsave_job.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.as_ref().map(|j| {
            (
                j.temp_path.clone(),
                j.waiting_replicas.clone(),
                j.snapshot_offset,
            )
        })
    }

    /// Send `bytes` through the outbound sender of the replica identified by
    /// `client_id`, if it is still registered. Returns `true` on a successful
    /// send (the send may still be queued in the writer thread but the channel
    /// is alive). Returns `false` when the replica is gone or the channel is
    /// closed.
    pub fn send_to_replica(&self, client_id: ClientId, bytes: Vec<u8>) -> bool {
        let len = bytes.len();
        let guard = match self.replicas.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        match guard.get(&client_id) {
            Some(r) => {
                if r.outbound_sender.send(bytes).is_ok() {
                    let pending = r
                        .pending_output_bytes
                        .fetch_add(len, Ordering::Relaxed)
                        .saturating_add(len);
                    if let Ok(mut guard) = crate::client_info::client_info_registry().lock() {
                        guard.set_output_buffer_memory(client_id, pending);
                    }
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }

    /// Mark a replica's state, no-op when the entry is gone. Used by
    /// full-sync transfer path to step replicas through WaitingBgsave →
    /// SendingRdb → Online.
    pub fn set_replica_state(&self, client_id: ClientId, state: ReplicaState) {
        let guard = match self.replicas.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(r) = guard.get(&client_id) {
            r.set_state(state);
        }
    }
}

fn contiguous_history_min(master: i64, intervals: &[(i64, i64)]) -> i64 {
    let mut min = master;
    loop {
        let mut extended = false;
        for (start, end) in intervals {
            if *start < min && *end >= min {
                min = *start;
                extended = true;
            }
        }
        if !extended {
            return min;
        }
    }
}

fn range_covered_by_intervals(start: i64, end: i64, intervals: &[(i64, i64)]) -> bool {
    let mut cursor = start;
    while cursor < end {
        let mut next = cursor;
        for (segment_start, segment_end) in intervals {
            if *segment_start <= cursor && *segment_end > next {
                next = *segment_end;
            }
        }
        if next == cursor {
            return false;
        }
        cursor = next.min(end);
    }
    true
}

static GLOBAL_REPLICATION_STATE: OnceLock<Arc<ReplicationState>> = OnceLock::new();

/// Install the process-wide replication state. Idempotent: subsequent calls
/// after the first one are no-ops (OnceLock semantics).
pub fn install_replication_state(state: Arc<ReplicationState>) {
    let _ = GLOBAL_REPLICATION_STATE.set(state);
}

/// Return the process-wide replication state, allocating a default standalone
/// primary if none has been installed (unit-test fallback).
pub fn global_replication_state() -> Arc<ReplicationState> {
    GLOBAL_REPLICATION_STATE
        .get_or_init(|| {
            Arc::new(ReplicationState::new(
                generate_runid(),
                DEFAULT_REPL_BACKLOG_SIZE,
            ))
        })
        .clone()
}

/// Generate a 40-character lowercase hex run id from `SystemTime::now`
/// a small inline xorshift step seeded by the address of a stack variable.
/// No external `rand` dependency and no shared mutable global; the caller
/// invokes this once at startup.
pub fn generate_runid() -> [u8; 40] {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id() as u64;
    let stack_marker: u64 = 0;
    let addr_entropy = (&stack_marker as *const u64) as usize as u64;
    let mut seed = now_ns
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add(pid.wrapping_mul(0xBF58476D1CE4E5B9))
        .wrapping_add(addr_entropy.wrapping_mul(0x94D049BB133111EB));
    if seed == 0 {
        seed = 1;
    }
    let mut bytes = [0u8; 40];
    for chunk in bytes.chunks_mut(16) {
        let hi = xorshift64(&mut seed);
        let lo = xorshift64(&mut seed);
        let hex = format!("{:016x}{:016x}", hi, lo);
        let hex_bytes = hex.as_bytes();
        for (i, slot) in chunk.iter_mut().enumerate() {
            *slot = hex_bytes[i];
        }
    }
    bytes
}

/// Single-step xorshift64. Used only by [`generate_runid`].
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Build the canonical `+FULLRESYNC <runid> <offset>\r\n` reply line for
/// PSYNC handshake responses.
pub fn fullresync_reply(runid: &[u8; 40], offset: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(b"+FULLRESYNC ");
    buf.extend_from_slice(runid);
    buf.push(b' ');
    buf.extend_from_slice(offset.to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf
}

/// Build the canonical `+CONTINUE <runid>\r\n` reply line for partial
/// resync.
pub fn continue_reply(runid: &[u8; 40]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(48);
    buf.extend_from_slice(b"+CONTINUE ");
    buf.extend_from_slice(runid);
    buf.extend_from_slice(b"\r\n");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backlog_round_trip_within_window() {
        let mut b = ReplBacklog::new(16);
        let off = b.append(b"hello");
        assert_eq!(off, 5);
        assert_eq!(b.min_offset(), 0);
        assert_eq!(b.read_at(0, 5).as_deref(), Some(b"hello".as_slice()));
    }

    #[test]
    fn backlog_wraps_and_drops_old_offset() {
        let mut b = ReplBacklog::new(4);
        b.append(b"abcd");
        b.append(b"efgh");
        assert_eq!(b.offset, 8);
        assert_eq!(b.min_offset(), 4);
        assert_eq!(b.read_at(4, 4).as_deref(), Some(b"efgh".as_slice()));
        assert!(b.read_at(0, 4).is_none());
    }

    #[test]
    fn bgsave_catchup_extends_history_beyond_circular_backlog() {
        let st = ReplicationState::new(generate_runid(), 4);
        st.install_repl_bgsave_job(ReplBgsaveJob {
            child_pid: 1,
            temp_path: PathBuf::from("temp-repl-test.rdb"),
            waiting_replicas: vec![42],
            snapshot_offset: 0,
            catch_up_bytes: Vec::new(),
            needs_getack_on_completion: false,
        });

        st.append_to_backlog(b"abcdef");
        assert_eq!(st.backlog_snapshot(), (0, 6, 6, 4));
        assert_eq!(
            st.read_history_at(0, 6).as_deref(),
            Some(b"abcdef".as_slice())
        );

        let job = st.take_repl_bgsave_job().expect("job still installed");
        assert_eq!(job.catch_up_bytes, b"abcdef");
        assert_eq!(st.backlog_snapshot(), (2, 6, 4, 4));
        assert!(st.read_history_at(0, 1).is_none());
    }

    #[test]
    fn runid_is_40_lowercase_hex_chars() {
        let id = generate_runid();
        assert_eq!(id.len(), 40);
        for b in id.iter() {
            assert!(
                (b'0'..=b'9').contains(b) || (b'a'..=b'f').contains(b),
                "runid char out of range: {}",
                *b as char
            );
        }
    }

    #[test]
    fn two_runids_differ() {
        let a = generate_runid();
        let b = generate_runid();
        assert_ne!(a, b, "runid generator must produce unique values per call");
    }

    #[test]
    fn state_round_trip() {
        let st = ReplicationState::new(generate_runid(), 1024);
        assert!(!st.is_replica());
        assert_eq!(st.master_offset(), 0);
        assert_eq!(st.connected_replicas(), 0);
        let new_off = st.append_to_backlog(b"abc");
        assert_eq!(new_off, 3);
        assert_eq!(st.master_offset(), 3);
    }

    #[test]
    fn standalone_primary_does_not_need_replication_propagation() {
        let st = ReplicationState::new(generate_runid(), 1024);
        assert!(!st.should_propagate_writes());

        st.append_to_backlog(b"active");
        assert!(st.should_propagate_writes());

        st.repl_state
            .store(repl_state_code::REPLICA_ONLINE, Ordering::Relaxed);
        assert!(!st.should_propagate_writes());
    }

    #[test]
    fn target_change_resets_cached_partial_resync_state() {
        let st = ReplicationState::new(generate_runid(), 1024);
        st.become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 6379);

        let cached = [b'a'; 40];
        st.set_cached_primary_replid(cached);
        st.master_repl_offset.store(1234, Ordering::Relaxed);

        let same_epoch = st.become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 6379);
        assert_eq!(st.cached_primary_replid(), Some(cached));
        assert_eq!(st.master_offset(), 1234);
        assert!(st.dialer_epoch_is_current(same_epoch));

        let changed_epoch = st.become_replica_of(RedisString::from_bytes(b"127.0.0.2"), 6379);
        assert_eq!(st.cached_primary_replid(), None);
        assert_eq!(st.master_offset(), 0);
        assert!(!st.dialer_epoch_is_current(same_epoch));
        assert!(st.dialer_epoch_is_current(changed_epoch));
    }

    #[test]
    fn fullresync_reply_line_shape() {
        let runid = generate_runid();
        let line = fullresync_reply(&runid, 42);
        assert!(line.starts_with(b"+FULLRESYNC "));
        assert!(line.ends_with(b" 42\r\n"));
        assert_eq!(line.len(), b"+FULLRESYNC ".len() + 40 + b" 42\r\n".len());
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//                  plus the architect packet for Session 3A.
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         3
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Wave A foundation. Backlog + state types compile; PSYNC
//                  accept and INFO readback work end-to-end. Outstanding:
//                  (a) write-command propagation into append_to_backlog
//                  (Wave B), (b) RDB transfer to a replica after +FULLRESYNC
//                  (Wave B), (c) replica-side handshake spawn (Wave C),
//                  (d) REPLCONF subcommands + WAIT (Wave B).
// ──────────────────────────────────────────────────────────────────────────
