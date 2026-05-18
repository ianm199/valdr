//! Eviction logic — LRU/LFU approximation and `maxmemory` directive enforcement.
//!
//! This module implements key eviction when the server's memory usage exceeds
//! the configured `maxmemory` limit.  The primary entry point is
//! [`perform_evictions`], which must be called before executing any command
//! that may increase memory usage.
//!
//! Eviction uses an approximation pool ([`EvictionPool`]) that accumulates the
//! best candidates across multiple sampling rounds.  Available policies: LRU,
//! LFU, TTL-ordered, and uniform random — each applied to all keys or only to
//! volatile (expiry-carrying) keys.
//!
//! C source: evict.c (648 lines, 10 functions)

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::db::RedisDb;
use crate::object::RedisObject;
use crate::server::RedisServer;
use redis_types::RedisString;

// ──────────────────────────────────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────────────────────────────────

/// Number of candidate slots in the eviction pool.
/// C: `#define EVPOOL_SIZE 16`
pub const EVPOOL_SIZE: usize = 16;

/// Maximum key byte-length that can be stored inline in a pool entry's
/// pre-allocated cache buffer, avoiding an extra heap allocation per candidate.
/// C: `#define EVPOOL_CACHED_SDS_SIZE 255`
pub const EVPOOL_CACHED_SDS_SIZE: usize = 255;

/// Sentinel returned by kvstore slot-selection helpers when no non-empty slot
/// exists.
/// C: `KVSTORE_INDEX_NOT_FOUND` (−1)
const KVSTORE_INDEX_NOT_FOUND: i32 = -1;

/// AOF disabled state flag.
/// C: `AOF_OFF`
/// TODO(port): import from crates/redis-persist/src/aof.rs once that module lands.
const AOF_OFF: i32 = 0;

// ──────────────────────────────────────────────────────────────────────────
// Public types
// ──────────────────────────────────────────────────────────────────────────

/// Maxmemory eviction policy.
///
/// The C code represents policies as a bitflag / enum hybrid (constants like
/// `MAXMEMORY_FLAG_LRU`, `MAXMEMORY_FLAG_LFU`, `MAXMEMORY_FLAG_ALLKEYS` are
/// ORed with policy enum values).  This Rust enum expands the combinations
/// explicitly; the three helper methods below replace the bitflag tests.
///
/// C: `MAXMEMORY_*` constants in `server.h`
///
/// TODO(architect): move to `crates/redis-core/src/config.rs` (or `server.rs`)
///                  and declare it canonical.  Remove this local definition once
///                  the type has a registered owner in type-vocabulary.tsv.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MaxmemoryPolicy {
    /// Reject writes that exceed the limit; never evict.
    /// C: `MAXMEMORY_NO_EVICTION`
    #[default]
    NoEviction,
    /// Evict any key by LRU approximation.
    /// C: `MAXMEMORY_ALLKEYS_LRU`
    AllkeysLru,
    /// Evict any key by LFU (least-frequently-used).
    /// C: `MAXMEMORY_ALLKEYS_LFU`
    AllkeysLfu,
    /// Evict any key uniformly at random.
    /// C: `MAXMEMORY_ALLKEYS_RANDOM`
    AllkeysRandom,
    /// Evict a volatile (has-expiry) key by LRU.
    /// C: `MAXMEMORY_VOLATILE_LRU`
    VolatileLru,
    /// Evict a volatile key by LFU.
    /// C: `MAXMEMORY_VOLATILE_LFU`
    VolatileLfu,
    /// Evict the volatile key whose expiry arrives soonest.
    /// C: `MAXMEMORY_VOLATILE_TTL`
    VolatileTtl,
    /// Evict a volatile key uniformly at random.
    /// C: `MAXMEMORY_VOLATILE_RANDOM`
    VolatileRandom,
}

impl MaxmemoryPolicy {
    /// True when the policy scores candidates using LRU or LFU.
    /// C: `server.maxmemory_policy & (MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU)`
    pub fn uses_lru_or_lfu(self) -> bool {
        matches!(
            self,
            Self::AllkeysLru | Self::AllkeysLfu | Self::VolatileLru | Self::VolatileLfu
        )
    }

    /// True when the policy applies to every key, not only volatile ones.
    /// C: `server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS`
    pub fn is_allkeys(self) -> bool {
        matches!(self, Self::AllkeysLru | Self::AllkeysLfu | Self::AllkeysRandom)
    }

    /// True when the policy uses the candidate pool (LRU, LFU, or TTL order).
    /// C: `policy & (FLAG_LRU|FLAG_LFU) || policy == VOLATILE_TTL`
    pub fn uses_pool(self) -> bool {
        self.uses_lru_or_lfu() || matches!(self, Self::VolatileTtl)
    }
}

/// Return value of [`perform_evictions`].
/// C: `EVICT_OK` / `EVICT_RUNNING` / `EVICT_FAIL`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictResult {
    /// Memory is within limits, or eviction resolved the excess.
    /// C: `EVICT_OK`
    Ok,
    /// Memory is still over the limit; an async time-event continues evicting.
    /// C: `EVICT_RUNNING`
    Running,
    /// Nothing left to evict; memory cannot be freed.
    /// C: `EVICT_FAIL`
    Fail,
}

/// One candidate slot in the eviction pool.
///
/// Entries are sorted ascending by `idle` so that `entries[EVPOOL_SIZE - 1]`
/// holds the best eviction candidate (highest idle / inverse-frequency score).
/// An empty slot is represented by `key: None`.
///
/// C: `struct evictionPoolEntry` — evict.c:56–62
#[derive(Debug)]
pub struct EvictionPoolEntry {
    /// Idle time (LRU) or inverse-frequency (LFU) score.
    /// Higher value ↔ better eviction candidate.
    /// C: `unsigned long long idle`
    pub idle: u64,
    /// Key bytes.  `None` indicates an empty slot.
    /// C: `sds key`  (NULL == empty)
    pub key: Option<RedisString>,
    /// Pre-allocated byte buffer (`EVPOOL_CACHED_SDS_SIZE + 1` bytes) used to
    /// avoid a per-key heap allocation for short keys.
    /// C: `sds cached`
    pub cached: Vec<u8>,
    /// Database index that owns this key.
    /// C: `int dbid`
    pub dbid: i32,
    /// Hash-table slot within the database's kvstore.
    /// C: `int slot`
    pub slot: i32,
}

/// Fixed-size pool of eviction candidates, sorted by idle time ascending.
///
/// Also carries a round-robin counter used by the random eviction policies.
///
/// C: `static struct evictionPoolEntry *EvictionPoolLRU` — evict.c:64
#[derive(Debug)]
pub struct EvictionPool {
    /// The 16-entry candidate array.
    pub entries: [EvictionPoolEntry; EVPOOL_SIZE],
    /// Round-robin DB counter for `allkeys-random` / `volatile-random`.
    /// C: `static unsigned int next_db` (local static inside `performEvictions`)
    pub next_db: u32,
}

/// Snapshot of memory state relative to the `maxmemory` limit.
///
/// Returned by [`get_maxmemory_state`]; callers may ignore fields that are not
/// meaningful for their use case.
#[derive(Debug, Default)]
pub struct MaxmemoryState {
    /// Total allocator bytes in use.
    /// C: `mem_reported` / `*total`
    pub total: usize,
    /// Logical bytes (total minus AOF + replication-buffer overhead).
    /// C: `mem_used` / `*logical`
    pub logical: usize,
    /// Bytes to free to return below the limit.
    /// C: `mem_tofree` / `*tofree`
    pub tofree: usize,
    /// Utilization ratio: `logical / maxmemory`.  May exceed 1.0 when over limit.
    /// C: `*level`
    pub level: f32,
}

// ──────────────────────────────────────────────────────────────────────────
// Pool lifecycle
// ──────────────────────────────────────────────────────────────────────────

/// Allocate and initialise the eviction pool.
///
/// Each entry receives a pre-allocated `cached` buffer of
/// `EVPOOL_CACHED_SDS_SIZE + 1` bytes to avoid per-key allocation.
///
/// C: `evictionPoolAlloc()` — evict.c:91
pub fn eviction_pool_alloc() -> EvictionPool {
    // PERF(port): C allocates all entries in one contiguous slab; here each
    //             `cached` Vec is a separate heap allocation.  Profile in Phase B.
    let entries = std::array::from_fn(|_| EvictionPoolEntry {
        idle: 0,
        key: None,
        cached: vec![0u8; EVPOOL_CACHED_SDS_SIZE + 1],
        dbid: 0,
        slot: 0,
    });
    EvictionPool { entries, next_db: 0 }
}

// ──────────────────────────────────────────────────────────────────────────
// Pool population
// ──────────────────────────────────────────────────────────────────────────

/// Insert one eviction candidate into the pool at its correct sorted position.
///
/// PORT NOTE: Extracted from the C's `evictionPoolPopulate` loop body so that
/// the pool insertion logic can be tested independently of the kvstore.  In C
/// this logic is inlined at evict.c:141–186.
///
/// The pool is kept sorted ascending by `idle`; the best candidate lives at
/// `entries[EVPOOL_SIZE - 1]`.  When the pool is full, the entry with the
/// lowest idle score (worst candidate) is discarded.
fn eviction_pool_insert_candidate(
    pool: &mut EvictionPool,
    key_bytes: &[u8],
    idle: u64,
    db_id: i32,
    slot: i32,
) {
    // C: evict.c:142 — find first empty slot, or first slot with idle >= new idle.
    let mut k = 0usize;
    while k < EVPOOL_SIZE && pool.entries[k].key.is_some() && pool.entries[k].idle < idle {
        k += 1;
    }

    // C: evict.c:143 — candidate is worse than every existing entry AND pool is full: skip.
    if k == 0 && pool.entries[EVPOOL_SIZE - 1].key.is_some() {
        return;
    }

    if k < EVPOOL_SIZE && pool.entries[k].key.is_none() {
        // C: evict.c:147 — inserting into an empty slot; no shifting needed.
    } else {
        // C: evict.c:150 — inserting in the middle; shift to make room.
        if pool.entries[EVPOOL_SIZE - 1].key.is_none() {
            // C: evict.c:152 — free space at the right end: shift k..EVPOOL_SIZE-2 rightward.
            // Save the tail's cached buffer; it migrates to slot k after the shift.
            // C: `sds cached = pool[EVPOOL_SIZE-1].cached; memmove(...); pool[k].cached = cached`
            // PERF(port): C achieves this with a single memmove; here we do N field-by-field copies.
            let saved_cached = std::mem::take(&mut pool.entries[EVPOOL_SIZE - 1].cached);
            for i in (k..EVPOOL_SIZE - 1).rev() {
                // Move all fields — including the cached buffer — one slot to the right.
                let cached = std::mem::take(&mut pool.entries[i].cached);
                pool.entries[i + 1].idle = pool.entries[i].idle;
                pool.entries[i + 1].key = pool.entries[i].key.take();
                pool.entries[i + 1].cached = cached;
                pool.entries[i + 1].dbid = pool.entries[i].dbid;
                pool.entries[i + 1].slot = pool.entries[i].slot;
            }
            pool.entries[k].cached = saved_cached;
        } else {
            // C: evict.c:161 — pool is full: decrement k then shift 1..k leftward,
            // discarding the entry with the lowest idle score (slot 0).
            // C: `k--; sds cached = pool[0].cached; sdsfree(pool[0].key); memmove(pool, pool+1, k); pool[k].cached = cached`
            debug_assert!(k > 0, "k underflow guard: k==0 AND pool full is caught by the first if");
            k -= 1;
            let saved_cached = std::mem::take(&mut pool.entries[0].cached);
            pool.entries[0].key = None; // drop the key with lowest idle (worst candidate)
            for i in 0..k {
                // Move all fields one slot to the left.
                let cached = std::mem::take(&mut pool.entries[i + 1].cached);
                pool.entries[i].idle = pool.entries[i + 1].idle;
                pool.entries[i].key = pool.entries[i + 1].key.take();
                pool.entries[i].cached = cached;
                pool.entries[i].dbid = pool.entries[i + 1].dbid;
                pool.entries[i].slot = pool.entries[i + 1].slot;
            }
            pool.entries[k].cached = saved_cached;
        }
    }

    // C: evict.c:176 — store key: reuse cached buffer if it fits, else allocate.
    let klen = key_bytes.len();
    if klen > EVPOOL_CACHED_SDS_SIZE {
        // Key too long for the inline buffer: allocate a separate RedisString.
        // C: `pool[k].key = sdsdup(key)`
        pool.entries[k].key = Some(RedisString::from_bytes(key_bytes));
    } else {
        // Key fits in the 256-byte inline buffer.
        // C: `memcpy(pool[k].cached, key, klen+1); sdssetlen(pool[k].cached, klen);
        //     pool[k].key = pool[k].cached;`  — key points INTO cached (no extra alloc)
        // PORT NOTE: Rust forbids self-referential structs without `unsafe`, so we copy
        //            the bytes into `cached` (for inspection/debug) and also create a
        //            separate `RedisString` for the key field.  The extra allocation is
        //            small (≤255 bytes) and avoids `unsafe`.
        // PERF(port): C avoids the allocation entirely; Phase B should benchmark this.
        pool.entries[k].cached[..klen].copy_from_slice(key_bytes);
        if pool.entries[k].cached.len() > klen {
            pool.entries[k].cached[klen] = 0; // NUL-terminate for compat
        }
        pool.entries[k].key = Some(RedisString::from_bytes(key_bytes));
    }

    pool.entries[k].idle = idle;
    pool.entries[k].dbid = db_id;
    pool.entries[k].slot = slot;
}

/// Sample keys from a database's kvstore and insert the best candidates into
/// the eviction pool.
///
/// Returns the number of keys sampled (0 if the kvstore has no non-empty slots).
///
/// C: `evictionPoolPopulate()` — evict.c:113
///
/// TODO(port): The `samplekvs` kvstore parameter is absent in Phase A because
///             `KvStore` lives in `crates/redis-ds/src/kvstore.rs` (defer phase).
///             Phase B: add `samplekvs: &KvStore` (or a trait object) once the
///             dependency edge `redis-core → redis-ds` is established per
///             TODO(architect) below.  The pool insertion logic lives in
///             [`eviction_pool_insert_candidate`] and is fully implemented.
pub fn eviction_pool_populate(
    _db: &RedisDb,
    pool: &mut EvictionPool,
    policy: MaxmemoryPolicy,
    _maxmemory_samples: usize,
) -> usize {
    // TODO(port): slot = kvstoreGetFairRandomHashtableIndex(samplekvs)
    // TODO(port): if slot == KVSTORE_INDEX_NOT_FOUND { return 0; }
    // TODO(port): count = kvstoreHashtableSampleEntries(samplekvs, slot, samples, maxmemory_samples)
    // TODO(port): for each sampled robj *o:
    //   let key_bytes = object_get_key(o).as_bytes();
    //   let idle = if policy.uses_lru_or_lfu() {
    //       object_get_idleness(o)
    //   } else if matches!(policy, MaxmemoryPolicy::VolatileTtl) {
    //       u64::MAX - object_get_expire(o)
    //   } else {
    //       // TODO(architect): is panic correct for unknown policy, or return Err?
    //       panic!("unknown eviction policy in eviction_pool_populate");
    //   };
    //   eviction_pool_insert_candidate(pool, key_bytes, idle, db.id, slot);
    let _ = (pool, policy); // suppress unused-var lint in Phase A
    0 // Phase A stub; full body lands in Phase B when kvstore dep is wired
}

// ──────────────────────────────────────────────────────────────────────────
// Memory accounting
// ──────────────────────────────────────────────────────────────────────────

/// Return the bytes that must NOT be counted toward the `maxmemory` limit.
///
/// Counting AOF / replication buffers creates a feedback loop: eviction
/// generates DEL propagations, which grow those buffers, which trigger more
/// eviction, and so on.
///
/// Returns: excess replication buffer beyond the backlog size + AOF write
/// buffer + active cluster slot-export buffers.
///
/// C: `freeMemoryGetNotCountedMemory()` — evict.c:200
pub fn free_memory_get_not_counted_memory(server: &RedisServer) -> usize {
    let mut overhead: usize = 0;

    // C: evict.c:218 — excess replication buffer memory beyond the backlog.
    // The backlog caps its own growth; only the per-replica excess is "free-floating".
    // TODO(port): `server.repl_buffer_mem` and `server.repl_backlog_size` fields need
    //             to be added to `RedisServer` (crates/redis-core/src/server.rs).
    let repl_buffer_mem: usize = 0; // TODO(port): server.repl_buffer_mem
    let repl_backlog_size: usize = 0; // TODO(port): server.repl_backlog_size
    if repl_buffer_mem > repl_backlog_size {
        // C: `extra_approx_size = (backlog_size / PROTO_REPLY_CHUNK_BYTES + 1) * (sizeof(replBufBlock) + sizeof(listNode))`
        // TODO(port): PROTO_REPLY_CHUNK_BYTES and struct sizes must come from
        //             constants in the networking / replication modules.
        let proto_reply_chunk_bytes: usize = 16 * 1024; // placeholder
        let block_overhead: usize = 64; // placeholder: sizeof(replBufBlock) + sizeof(listNode)
        let extra_approx =
            (repl_backlog_size / proto_reply_chunk_bytes + 1) * block_overhead;
        let counted_mem = repl_backlog_size + extra_approx;
        if repl_buffer_mem > counted_mem {
            overhead += repl_buffer_mem - counted_mem;
        }
    }

    // C: evict.c:230 — AOF write buffer allocation.
    // TODO(port): `server.aof_state` and `server.aof_buf` fields needed on RedisServer.
    let aof_state: i32 = 0; // TODO(port): server.aof_state
    if aof_state != AOF_OFF {
        // C: `overhead += sdsAllocSize(server.aof_buf)`
        // sdsAllocSize returns the allocated capacity, not just the used length.
        // TODO(port): RedisString needs an alloc_size() / capacity() accessor.
        let aof_buf_capacity: usize = 0; // TODO(port): server.aof_buf.capacity()
        overhead += aof_buf_capacity;
    }

    // C: evict.c:234 — cluster slot-export buffers.
    // TODO(port): cluster_is_any_slot_exporting() and
    //             cluster_get_total_slot_export_buffer_memory() require
    //             crates/redis-cluster (defer phase).  Skipped in Phase A.
    let _ = server;

    overhead
}

/// Check memory usage against the `maxmemory` limit.
///
/// Returns `Ok(state)` when within limits and `Err(state)` when over.  Both
/// variants populate `state.total` and `state.level`; `Err` additionally
/// populates `state.logical` and `state.tofree`.
///
/// C: `getMaxmemoryState()` — evict.c:265
///
/// PORT NOTE: C passes optional output-pointer arguments and returns `C_OK` /
///            `C_ERR`.  Rust bundles the outputs in [`MaxmemoryState`] and
///            uses `Result` for the C_OK / C_ERR distinction.
pub fn get_maxmemory_state(server: &RedisServer) -> Result<MaxmemoryState, MaxmemoryState> {
    let mut state = MaxmemoryState::default();

    // C: `mem_reported = zmalloc_used_memory()`
    // TODO(port): zmalloc_used_memory() is in crates/redis-core/src/zmalloc.rs (defer).
    //             Phase A uses the config.max_memory as a proxy for presence-of-limit only.
    let mem_reported: usize = 0; // TODO(port): zmalloc_used_memory()
    state.total = mem_reported;

    let maxmemory = server.live_config.maxmemory() as usize;
    if maxmemory == 0 {
        state.level = 0.0;
        return Ok(state);
    }

    let overhead = free_memory_get_not_counted_memory(server);
    let mem_used = mem_reported.saturating_sub(overhead);

    state.level = if maxmemory > 0 {
        mem_used as f32 / maxmemory as f32
    } else {
        0.0
    };

    // C: evict.c:289 — first guard: raw reported usage is under limit.
    if mem_reported <= maxmemory {
        return Ok(state);
    }
    // C: evict.c:292 — second guard: logical usage (after removing overhead) is under limit.
    if mem_used <= maxmemory {
        return Ok(state);
    }

    state.logical = mem_used;
    state.tofree = mem_used - maxmemory;
    Err(state)
}

/// Returns `true` if adding `moremem` bytes would push total usage over the
/// `maxmemory` limit.
///
/// C: `overMaxmemoryAfterAlloc()` — evict.c:306
pub fn over_maxmemory_after_alloc(server: &RedisServer, moremem: usize) -> bool {
    let maxmemory = server.live_config.maxmemory() as usize;
    if maxmemory == 0 {
        return false;
    }
    // TODO(port): zmalloc_used_memory()
    let mem_used: usize = 0; // stub
    if mem_used + moremem <= maxmemory {
        return false;
    }
    let overhead = free_memory_get_not_counted_memory(server);
    let mem_used = mem_used.saturating_sub(overhead);
    mem_used + moremem > maxmemory
}

// ──────────────────────────────────────────────────────────────────────────
// Event-loop integration
// ──────────────────────────────────────────────────────────────────────────

/// Whether the eviction time-event callback is currently registered.
///
/// C: `static int isEvictionProcRunning` — evict.c:322
///
/// TODO(architect): In multi-threaded operation this flag belongs on
///                  `RedisServer` as an `AtomicBool`.  A module-level
///                  `AtomicBool` is fine for the single-threaded pilot.
static IS_EVICTION_PROC_RUNNING: AtomicBool = AtomicBool::new(false);

/// Event-loop time-event callback that drives incremental eviction.
///
/// Reschedules itself (returns `0`) while eviction is still needed; removes
/// itself (returns `AE_NOMORE = -1`) when done.
///
/// C: `evictionTimeProc()` — evict.c:323
///
/// TODO(port): The real callback signature must match the ae event-loop
///             interface (`aeEventLoop *`, `long long id`, `void *clientData`).
///             The event loop is in the defer phase; wire this once
///             `crates/redis-core/src/event_loop.rs` is available.
pub fn eviction_time_proc(server: &RedisServer) -> i64 {
    const AE_NOMORE: i64 = -1;
    if perform_evictions(server) == EvictResult::Running {
        return 0; // keep firing
    }
    IS_EVICTION_PROC_RUNNING.store(false, Ordering::Relaxed);
    AE_NOMORE
}

/// Schedule the eviction time-event if it is not already running.
///
/// C: `startEvictionTimeProc()` — evict.c:336
///
/// TODO(port): Call `aeCreateTimeEvent(server.el, 0, eviction_time_proc, ...)`
///             once `event_loop.rs` (defer phase) is available.
pub fn start_eviction_time_proc(server: &RedisServer) {
    let _ = server; // server.el needed for event registration — TODO(port)
    if !IS_EVICTION_PROC_RUNNING.load(Ordering::Relaxed) {
        IS_EVICTION_PROC_RUNNING.store(true, Ordering::Relaxed);
        // TODO(port): aeCreateTimeEvent(server.el, 0, eviction_time_proc, NULL, NULL)
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Safety and timing helpers
// ──────────────────────────────────────────────────────────────────────────

/// Returns `true` if it is currently safe to run the eviction loop.
///
/// Eviction must be skipped during: long-running script timeouts, dataset
/// loading, replica mode with `replica-ignore-maxmemory`, and when eviction
/// actions are explicitly paused.
///
/// C: `isSafeToPerformEvictions()` — evict.c:347
fn is_safe_to_perform_evictions(server: &RedisServer) -> bool {
    // C: `isInsideYieldingLongCommand() || server.loading`
    // TODO(port): is_inside_yielding_long_command() — scripting / command context
    // TODO(port): server.loading field
    let loading: bool = false; // TODO(port): server.loading
    let in_yielding_long_command: bool = false; // TODO(port): is_inside_yielding_long_command()
    if in_yielding_long_command || loading {
        return false;
    }

    // C: `server.primary_host && server.repl_replica_ignore_maxmemory`
    // TODO(port): server.primary_host and server.repl_replica_ignore_maxmemory fields
    let has_primary_host: bool = false; // TODO(port): server.primary_host.is_some()
    let repl_replica_ignore_maxmemory: bool = false; // TODO(port): server.repl_replica_ignore_maxmemory
    if has_primary_host && repl_replica_ignore_maxmemory {
        return false;
    }

    // C: `isPausedActionsWithUpdate(PAUSE_ACTION_EVICT)`
    // TODO(port): is_paused_actions_with_update(PAUSE_ACTION_EVICT) — blocked.c
    let pause_action_evict: bool = false; // TODO(port)
    if pause_action_evict {
        return false;
    }

    let _ = server;
    true
}

/// Compute the per-cycle eviction time budget in microseconds.
///
/// Converts `maxmemory-eviction-tenacity` (0–100) to a wall-clock limit using
/// a piecewise formula:
/// - ≤ 10: linear 0 … 500 µs
/// - 11–99: 15 % geometric growth, ~2 minutes at tenacity 99
/// - 100: unlimited
///
/// C: `evictionTimeLimitUs()` — evict.c:363
fn eviction_time_limit_us(server: &RedisServer) -> u64 {
    // TODO(port): server.maxmemory_eviction_tenacity field (i32, 0–100)
    let tenacity: i32 = 50; // TODO(port): server.maxmemory_eviction_tenacity
    debug_assert!(tenacity >= 0);
    debug_assert!(tenacity <= 100);

    let t = tenacity as u64;
    let _ = server;

    if t <= 10 {
        return 50 * t; // C: `50uL * server.maxmemory_eviction_tenacity`
    }
    if t < 100 {
        // C: `(unsigned long)(500.0 * pow(1.15, tenacity - 10.0))`
        return (500.0_f64 * (1.15_f64).powi((t - 10) as i32)) as u64;
    }
    u64::MAX // C: `ULONG_MAX` — no eviction time limit
}

// ──────────────────────────────────────────────────────────────────────────
// Main eviction entry point
// ──────────────────────────────────────────────────────────────────────────

/// Check memory usage and free keys if necessary.
///
/// Must be called before executing commands that may increase memory usage.
/// When the eviction time budget is exceeded before the limit is resolved, an
/// async time-event ([`start_eviction_time_proc`]) continues in the background.
///
/// Returns:
/// - [`EvictResult::Ok`]      — within limits, or eviction resolved the excess
/// - [`EvictResult::Running`] — over limit, async eviction in progress
/// - [`EvictResult::Fail`]    — over limit, nothing left to evict
///
/// C: `performEvictions()` — evict.c:404
///
/// PORT NOTE: C uses `goto cant_free` / `goto update_metrics`; translated here
///            as labeled-loop break and early-return respectively.
pub fn perform_evictions(server: &RedisServer) -> EvictResult {
    // C: evict.c:407 — skip even metric updates if eviction is unsafe ("fake EVICT_OK").
    if !is_safe_to_perform_evictions(server) {
        return EvictResult::Ok;
    }

    let mut mem_freed: i64 = 0;
    let mut keys_freed: u32 = 0;
    let mut result = EvictResult::Fail;

    // C: evict.c:414 — number of connected replicas.
    // TODO(port): server.replicas list length
    let replicas: usize = 0; // TODO(port): server.replicas.len()

    // C: evict.c:417 — check if already within limits.
    let mem_tofree = match get_maxmemory_state(server) {
        Ok(_) => {
            return update_eviction_metrics(server, EvictResult::Ok);
        }
        Err(state) => state.tofree as i64,
    };

    // C: evict.c:422 — no-eviction policy or import mode: nothing to do.
    // TODO(port): server.maxmemory_policy and server.import_mode fields
    let maxmemory_policy = MaxmemoryPolicy::NoEviction; // TODO(port): server.maxmemory_policy
    let is_primary_in_import_mode: bool = false; // TODO(port): iAmPrimary() && server.import_mode
    if maxmemory_policy == MaxmemoryPolicy::NoEviction || is_primary_in_import_mode {
        return update_eviction_metrics(server, EvictResult::Fail);
    }

    let eviction_time_limit = eviction_time_limit_us(server);
    let eviction_timer = Instant::now();

    // C: `serverAssert(server.also_propagate.numops == 0)`
    // TODO(port): server.also_propagate.numops field
    // debug_assert!(server.also_propagate_numops == 0);

    // C: evict.c:427 — `latencyStartMonitor(latency)` / `elapsedStart(&evictionTimer)`
    // TODO(port): latency monitoring hooks

    // ── Main eviction loop ─────────────────────────────────────────────────
    // C: evict.c:438 — `while (mem_freed < (long long)mem_tofree)`
    'main_loop: while mem_freed < mem_tofree {
        let mut best_key: Option<RedisString> = None;
        let mut best_dbid: i32 = 0;
        let mut best_slot: i32 = 0;

        if maxmemory_policy.uses_pool() {
            // ── LRU / LFU / volatile-TTL: use the candidate pool ──────────
            // C: evict.c:449–515
            //
            // TODO(port): The inner pool-fill and pool-scan below need:
            //   (a) `server.dbnum` field
            //   (b) `server.maxmemory_samples` field
            //   (c) `db.keys` / `db.expires` KvStore references
            //   (d) kvstoreSize, kvstoreNumNonEmptyHashtables
            //   (e) eviction_pool_populate with real kvstore reference
            //   (f) kvstoreHashtableFind for ghost-key detection
            // All of these are Phase B work tied to the kvstore dep edge.
            //
            // Phase A preserves the outer loop structure and pool-scan logic:

            'find_pool_key: loop {
                // C: evict.c:456 — sample from every database to fill the pool.
                let total_keys_sampled: u64 = 0; // TODO(port): real sampling loop over dbs

                if total_keys_sampled == 0 {
                    break 'find_pool_key; // no keys anywhere
                }

                // C: evict.c:485 — scan pool from best (highest idle) to worst.
                for k in (0..EVPOOL_SIZE).rev() {
                    let mut pool_guard = match server.eviction_pool.lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                    let pool_key = match pool_guard.entries[k].key.take() {
                        Some(k) => k,
                        None => continue,
                    };
                    let entry_dbid = pool_guard.entries[k].dbid;
                    let entry_slot = pool_guard.entries[k].slot;
                    pool_guard.entries[k].idle = 0;
                    drop(pool_guard);

                    // C: evict.c:497 — kvstoreHashtableFind: confirm key still exists.
                    // TODO(port): real kvstore lookup; for now all pool entries are treated as ghosts.
                    let found: bool = false; // TODO(port): kvstoreHashtableFind(...)

                    if found {
                        best_key = Some(pool_key);
                        best_dbid = entry_dbid;
                        best_slot = entry_slot;
                        break;
                    }
                    // Ghost key (already evicted/expired) — try next pool slot.
                }

                if best_key.is_some() {
                    break 'find_pool_key;
                }
                // All pool entries were ghosts; fall through to re-fill next iteration.
            }
        } else if matches!(
            maxmemory_policy,
            MaxmemoryPolicy::AllkeysRandom | MaxmemoryPolicy::VolatileRandom
        ) {
            // ── Random policy: round-robin through databases ───────────────
            // C: evict.c:519–543
            //
            // TODO(port): server.dbnum, db.keys / db.expires kvstore,
            //             kvstoreGetFairRandomHashtableIndex,
            //             kvstoreHashtableRandomEntry
            let db_count: usize = server.db_count();
            let is_allkeys = maxmemory_policy.is_allkeys();
            for _ in 0..db_count {
                let mut pool_guard = match server.eviction_pool.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                pool_guard.next_db = pool_guard.next_db.wrapping_add(1);
                let _j = (pool_guard.next_db as usize) % db_count.max(1);
                drop(pool_guard);
                // TODO(port): slot = kvstoreGetFairRandomHashtableIndex(db[j].keys or expires)
                // TODO(port): if slot == KVSTORE_INDEX_NOT_FOUND: continue
                // TODO(port): if kvstoreHashtableRandomEntry(db[j], slot, &entry):
                //     best_key = Some(objectGetKey(entry)); best_dbid = j; best_slot = slot; break
                let _ = is_allkeys;
            }
        }

        // ── Evict the chosen key ───────────────────────────────────────────
        // C: evict.c:547–609
        if let Some(ref bk) = best_key {
            // C: `robj *keyobj = createStringObject(bestkey, sdslen(bestkey))`
            // In Rust: `bk` IS already a RedisString (byte-string key).

            // C: `delta = zmalloc_used_memory(); dbGenericDelete(...); delta -= zmalloc_used_memory()`
            // TODO(port): latencyStartMonitor(eviction_latency)
            // TODO(port): enterExecutionUnit(1, 0)
            let before_mem: usize = 0; // TODO(port): zmalloc_used_memory()
            // TODO(port): server.db_generic_delete(best_dbid, bk, lazyfree_lazy_eviction)
            //             C: dbGenericDelete(db, keyobj, server.lazyfree_lazy_eviction, DB_FLAG_KEY_EVICTED)
            let after_mem: usize = 0; // TODO(port): zmalloc_used_memory() after delete
            let delta = before_mem as i64 - after_mem as i64;
            mem_freed += delta;
            // TODO(port): latencyEndMonitor(eviction_latency)
            // TODO(port): latencyAddSampleIfNeeded("eviction-del", eviction_latency)
            // TODO(port): exitExecutionUnit(); postExecutionUnitOperations()

            // C: `server.stat_evictedkeys++`
            // TODO(port): server.stat_evictedkeys field

            // C: `signalModifiedKey(NULL, db, keyobj)`
            // TODO(port): server.signal_modified_key(best_dbid, bk)

            // C: `notifyKeyspaceEvent(NOTIFY_EVICTED, "evicted", keyobj, db->id)`
            // TODO(port): server.notify_keyspace_event_evicted(best_dbid, bk)

            // C: `propagateDeletion(db, keyobj, server.lazyfree_lazy_eviction, bestslot)`
            // TODO(port): server.propagate_deletion(best_dbid, bk, lazyfree, best_slot)

            let _ = (best_dbid, best_slot);

            keys_freed += 1;

            if keys_freed % 16 == 0 {
                // C: evict.c:583 — periodically flush replica output buffers.
                if replicas > 0 {
                    // TODO(port): server.flush_replicas_output_buffers()
                }

                // C: evict.c:592 — if lazyfree is on, the real freed memory appears
                // asynchronously; check the allocator rather than the running delta.
                let lazyfree_lazy_eviction: bool = false; // TODO(port): server.lazyfree_lazy_eviction
                if lazyfree_lazy_eviction && get_maxmemory_state(server).is_ok() {
                    break 'main_loop;
                }

                // C: evict.c:601 — honour the eviction time budget.
                let elapsed_us = eviction_timer.elapsed().as_micros() as u64;
                if elapsed_us > eviction_time_limit {
                    start_eviction_time_proc(server);
                    break 'main_loop;
                }
            }
        } else {
            // C: `goto cant_free` — no candidate found; pool is exhausted.
            result = EvictResult::Fail;
            break 'main_loop;
        }
    }

    // C: evict.c:612 — normal loop exit: freed enough or hit time budget.
    if result != EvictResult::Fail {
        result = if IS_EVICTION_PROC_RUNNING.load(Ordering::Relaxed) {
            EvictResult::Running
        } else {
            EvictResult::Ok
        };
    }

    // ── cant_free: wait for pending lazyfree background jobs ──────────────
    // C: evict.c:615–631
    if result == EvictResult::Fail {
        // C: `while (bioPendingJobsOfType(BIO_LAZY_FREE) && elapsedUs(...) < limit)`
        // TODO(port): bio_pending_jobs_of_type(BIO_LAZY_FREE) — crates/redis-core/src/bio.rs
        // TODO(port): latencyStartMonitor(lazyfree_latency) / latencyAddSampleIfNeeded
        let bio_pending_lazy_free: bool = false; // TODO(port): bio_pending_jobs_of_type(BIO_LAZY_FREE)
        if bio_pending_lazy_free {
            loop {
                let elapsed_us = eviction_timer.elapsed().as_micros() as u64;
                if elapsed_us >= eviction_time_limit {
                    break;
                }
                if get_maxmemory_state(server).is_ok() {
                    result = EvictResult::Ok;
                    break;
                }
                // C: `usleep(eviction_time_limit_us < 1000 ? eviction_time_limit_us : 1000)`
                let sleep_us = eviction_time_limit.min(1_000);
                std::thread::sleep(Duration::from_micros(sleep_us));
            }
        }
    }

    // C: evict.c:633 — `latencyEndMonitor(latency); latencyAddSampleIfNeeded("eviction-cycle")`
    // TODO(port): latency accounting hooks

    // C: evict.c:637 — `update_metrics:`
    update_eviction_metrics(server, result)
}

/// Update the server's eviction-exceeded timing statistics and return `result`.
///
/// C: `update_metrics:` label in `performEvictions()` — evict.c:637
fn update_eviction_metrics(server: &RedisServer, result: EvictResult) -> EvictResult {
    // C: evict.c:638–645
    // TODO(port): server.stat_last_eviction_exceeded_time and
    //             server.stat_total_eviction_exceeded_time fields needed.
    // TODO(port): elapsedStart / elapsedUs for monotonic timer.
    if result == EvictResult::Running || result == EvictResult::Fail {
        // C: `if (server.stat_last_eviction_exceeded_time == 0) elapsedStart(...)`
        // TODO(port): record start of exceeded-time interval
    } else if result == EvictResult::Ok {
        // C: `server.stat_total_eviction_exceeded_time += elapsedUs(...);
        //     server.stat_last_eviction_exceeded_time = 0`
        // TODO(port): accumulate elapsed µs into total_eviction_exceeded_time
    }
    let _ = server;
    result
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/evict.c  (648 lines, 10 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         31
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:         Pool insertion logic (eviction_pool_insert_candidate) is
//                  fully translated; all kvstore/bio/cluster/event-loop
//                  cross-crate calls are explicit Phase A stubs with TODO(port).
//                  MaxmemoryPolicy defined locally pending TODO(architect)
//                  canonicalisation.  goto-based control flow restructured to
//                  labeled-loop break + early returns.
// ──────────────────────────────────────────────────────────────────────────
