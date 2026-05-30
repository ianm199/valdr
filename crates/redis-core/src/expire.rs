//! Incremental expiry of TTL keys and hash fields.
//!
//! Covers:
//! - Active expiration background cycle (`active_expire_cycle`).
//! - EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, TTL, PTTL, EXPIRETIME,
//!   PEXPIRETIME, TOUCH command implementations.
//! - Replica-local key expiry tracking for writable replicas.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use redis_types::{RedisError, RedisString};

use crate::command_context::CommandContext;
use crate::db::{RedisDb, LOOKUP_NOTOUCH};
use crate::metrics::{server_metrics, ServerMetrics};
use crate::notify::{NOTIFY_EXPIRED, NOTIFY_GENERIC};
use crate::object::RedisObject;
use crate::server::RedisServer;

// ── Public constants ────────────────────────────────────────────────────────

pub const EXPIRY_NONE: i64 = -1;

pub const EXPIRE_FORCE_DELETE_EXPIRED: i32 = 1;
pub const EXPIRE_AVOID_DELETE_EXPIRED: i32 = 2;

pub const ACTIVE_EXPIRE_CYCLE_SLOW: i32 = 0;
pub const ACTIVE_EXPIRE_CYCLE_FAST: i32 = 1;

pub const EXPIRE_NX: i32 = 1 << 0;
pub const EXPIRE_XX: i32 = 1 << 1;
pub const EXPIRE_GT: i32 = 1 << 2;
pub const EXPIRE_LT: i32 = 1 << 3;

// ── Internal constants ──────────────────────────────────────────────────────

const ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP: u64 = 20;
const ACTIVE_EXPIRE_CYCLE_FAST_DURATION: i64 = 1000;
const ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC: i64 = 25;
const ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE: i64 = 10;
const ACTIVE_EXPIRY_TYPE_COUNT: usize = 2;

// TODO(port): CRON_DBS_PER_CALL comes from server.h — import when stub expands.
const CRON_DBS_PER_CALL: usize = 16;

// TODO(port): UNIT_SECONDS / UNIT_MILLISECONDS come from server.h.
pub const UNIT_SECONDS: i32 = 0;
pub const UNIT_MILLISECONDS: i32 = 1;

// TODO(port): PAUSE_ACTION_EXPIRE is a server.h bitflag; placeholder value.
const PAUSE_ACTION_EXPIRE: u32 = 1 << 2;

// ── Type aliases ─────────────────────────────────────────────────────────

/// Millisecond Unix timestamp.
pub type MsTime = i64;

/// Microsecond duration.
pub type UsTime = i64;

/// Monotonic microsecond counter.
pub type MonoTime = u64;

// ── avg_ttl_factor table: pow(0.98, k) for k = 1..16 ────────────────────
// Used to compute running-average TTL with a closed-form geometric series.
static AVG_TTL_FACTOR: [f64; 16] = [
    0.98, 0.9604, 0.941192, 0.922368, 0.903921, 0.885842, 0.868126, 0.850763, 0.833748, 0.817073,
    0.800731, 0.784717, 0.769022, 0.753642, 0.738569, 0.723798,
];

// ── Public types ────────────────────────────────────────────────────────────

/// Return status from key-existence/expiry checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyStatus {
    /// Key exists and is not logically expired, or does not exist at all.
    Valid,
    /// Logically expired but not yet deleted (replica / loading mode).
    Expired,
    /// Deleted now.
    Deleted,
}

/// Policy returned by `get_expiration_policy_with_flags`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpirationPolicy {
    /// Treat items as valid regardless of their expiry time.
    IgnoreExpire,
    /// Do not delete expired items but expose them as logically absent.
    KeepExpired,
    /// Delete expired keys on access.
    DeleteExpired,
}

/// Selects which active expiry mechanism to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum ActiveExpiryType {
    /// Expire top-level keys via `db->expires`.
    Keys = 0,
    /// Expire hash fields with per-field TTLs stored in volatile sets.
    Fields = 1,
}

// ── Internal structs ─────────────────────────────────────────────────────

// Per-scan accounting passed to callbacks.
#[allow(dead_code)] // active-expire-cycle scan-callback port; fields wired when kvstoreScan is ported
struct ExpireScanData {
    db_id: u32,
    now: MsTime,
    sampled: u64,
    expired: u64,
    ttl_sum: MsTime,
    ttl_samples: i32,
    max_entries: u64,
    has_more_expired_entries: bool,
}

/// Iterator state for field-level active expiry.
pub struct ActiveExpireFieldIterator {
    pub current_db: i32,
    pub cursor: u64,
}

// Persistent per-job state across calls to active_expire_cycle_job.
// TODO(architect): move into RedisServer fields to avoid module-level global state.
#[derive(Clone, Copy)]
struct ExpireState {
    current_db: u32,
    timelimit_exit: bool,
}

impl ExpireState {
    const fn zeroed() -> Self {
        Self {
            current_db: 0,
            timelimit_exit: false,
        }
    }
}

// ── Module-level global state ─────────────────────────────────────────────

static ACTIVE_EXPIRE_STATE: Mutex<[ExpireState; ACTIVE_EXPIRY_TYPE_COUNT]> =
    Mutex::new([ExpireState::zeroed(); ACTIVE_EXPIRY_TYPE_COUNT]);

// Key → bitmask of db IDs.
// TODO(architect): move into RedisServer to avoid module-level global state.
static REPLICA_KEYS_WITH_EXPIRE: Mutex<Option<HashMap<RedisString, u64>>> = Mutex::new(None);

static LAST_FAST_CYCLE_START: Mutex<MonoTime> = Mutex::new(0);

static EXPIRE_CYCLE_START_WITH_FIELDS: Mutex<bool> = Mutex::new(false);

// ── Timing helpers ────────────────────────────────────────────────────────

// Monotonically increasing microsecond counter.
// TODO(port): Valkey uses CLOCK_MONOTONIC_RAW; Phase B should adopt the same source.
fn get_monotonic_us() -> MonoTime {
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    let epoch = EPOCH.get_or_init(std::time::Instant::now);
    epoch.elapsed().as_micros() as u64
}

// Microseconds since `start`.
fn elapsed_us(start: MonoTime) -> u64 {
    get_monotonic_us().saturating_sub(start)
}

// Current wall-clock time in milliseconds since Unix epoch.
fn ms_time_now() -> MsTime {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ── activeExpireEffort ──────────────────────────────────────────────────────
// Returns normalized 0-based effort level (0–9) from the server config (1–10).
fn active_expire_effort(server: &RedisServer) -> i64 {
    // TODO(port): server.active_expire_effort not yet on RedisServer stub.
    // Placeholder returns 0 (minimum effort).
    let _ = server;
    0
}

// ── activeExpireCycleTryExpire ──────────────────────────────────────────────
/// Attempts to expire `val` if its TTL has elapsed. Returns `true` and removes
/// the key from `db` when expired; returns `false` otherwise.
pub fn active_expire_cycle_try_expire(
    server: &mut RedisServer,
    db: &mut RedisDb,
    val: &RedisObject,
    now: MsTime,
    didx: i32,
) -> bool {
    // TODO(port): RedisObject::expire_ms() not yet on object stub.
    let t: MsTime = val.expire_ms().unwrap_or(EXPIRY_NONE);
    debug_assert!(
        t >= 0,
        "expire time passed to try_expire must be non-negative"
    );
    if now > t {
        // TODO(port): enterExecutionUnit / exitExecutionUnit not yet ported.
        // TODO(port): objectGetKey not yet on RedisObject stub.
        // TODO(port): deleteExpiredKeyAndPropagateWithDictIndex not yet on RedisDb stub.
        // TODO(port): server.stat_expiredkeys increment not on RedisServer stub.
        let _ = (server, db, didx);
        true
    } else {
        false
    }
}

// ── expireScanCallback ──────────────────────────────────────────────────────
// Callback passed to kvstoreScan for key-level TTL expiry.
// PORT NOTE: In C this is a `void (*)(void*, void*, int)` passed to kvstoreScan.
// Defined here as a typed Rust function; the caller adapts it when kvstore is ported.
#[allow(dead_code)] // active-expire-cycle scan-callback; wired when kvstoreScan (redis-ds) is ported
fn expire_scan_callback(
    data: &mut ExpireScanData,
    server: &mut RedisServer,
    db: &mut RedisDb,
    val: &RedisObject,
    didx: i32,
) {
    // TODO(port): RedisObject::expire_ms() not yet on object stub.
    let ttl = val.expire_ms().unwrap_or(EXPIRY_NONE) - data.now;
    if active_expire_cycle_try_expire(server, db, val, data.now, didx) {
        data.expired += 1;
        // TODO(port): postExecutionUnitOperations not yet ported.
    }
    if ttl > 0 {
        data.ttl_sum += ttl;
        data.ttl_samples += 1;
    }
    data.sampled += 1;
}

// ── fieldExpireScanCallback ─────────────────────────────────────────────────
// Callback passed to kvstoreScan for field-level TTL expiry inside hashes.
#[allow(dead_code)] // active-expire-cycle scan-callback; wired when kvstoreScan (redis-ds) is ported
fn field_expire_scan_callback(
    data: &mut ExpireScanData,
    _server: &RedisServer,
    db: &mut RedisDb,
    o: &RedisObject,
    didx: i32,
) {
    // TODO(port): hashTypeHasVolatileFields not yet ported.
    // TODO(port): dbReclaimExpiredFields not yet ported.
    // TODO(port): server.mstime not yet on RedisServer stub; using ms_time_now().
    let now = ms_time_now();
    let max_entries = data.max_entries;
    let _ = (db, o, didx, now, max_entries);
    data.sampled += 1;
}

// ── expireShouldSkipTableForSamplingCb ───────────────────────────────────────
// Returns true when the hash table fill ratio is below 1%, making random-key
// sampling too expensive relative to the number of hits found.
// PORT NOTE: In C this takes `hashtable *ht`; caller extracts size/buckets.
// Deferred until hashtable is ported; signature uses pre-extracted counts.
#[allow(dead_code)] // active-expire-cycle scan-callback; wired when hashtable (redis-ds) is ported
fn expire_should_skip_table_for_sampling(num_keys: u64, num_buckets: u64) -> bool {
    num_buckets > 0 && (num_keys * 100 / num_buckets) < 1
}

// ── activeExpireCycleJob ────────────────────────────────────────────────────
/// Runs one round of active expiration for `job_type` (KEYS or FIELDS).
///
/// `cycle_type` is `ACTIVE_EXPIRE_CYCLE_SLOW` or `ACTIVE_EXPIRE_CYCLE_FAST`.
/// `timelimit_us` is the CPU budget in microseconds.
/// Returns elapsed time in microseconds.
pub fn active_expire_cycle_job(
    server: &mut RedisServer,
    job_type: ActiveExpiryType,
    cycle_type: i32,
    timelimit_us: UsTime,
) -> UsTime {
    if timelimit_us <= 0 {
        return 0;
    }

    let effort = active_expire_effort(server);
    let config_cycle_acceptable_stale = ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE - effort;
    let keys_per_loop: u64 =
        ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP + ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP / 4 * effort as u64;

    let job_idx = job_type as usize;

    // TODO(port): server.stat_expired_keys_stale_perc and
    // server.stat_expired_keys_with_vola_stale_perc not on RedisServer stub.
    // Using 0.0 placeholder so the fast-cycle guard below is never suppressed.
    let expired_stale_perc_now: f64 = 0.0;

    // Fast cycle: skip if prior cycle didn't time out and stale percentage is acceptable.
    if cycle_type == ACTIVE_EXPIRE_CYCLE_FAST {
        let should_skip = {
            let guard = ACTIVE_EXPIRE_STATE
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            !guard[job_idx].timelimit_exit
                && expired_stale_perc_now < config_cycle_acceptable_stale as f64
        };
        if should_skip {
            return 0;
        }
    }

    // Scan all DBs if last call hit the time limit.
    let db_count = server.db_count();
    let dbs_per_call = {
        let guard = ACTIVE_EXPIRE_STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if CRON_DBS_PER_CALL > db_count || guard[job_idx].timelimit_exit {
            db_count
        } else {
            CRON_DBS_PER_CALL
        }
    };
    {
        let mut guard = ACTIVE_EXPIRE_STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard[job_idx].timelimit_exit = false;
    }

    let mut total_sampled: i64 = 0;
    let mut total_expired: i64 = 0;
    let start = get_monotonic_us();
    let mut iteration: i32 = 0;
    let mut dbs_performed: usize = 0;
    let mut last_db_id: Option<u32> = None;

    let time_check_mask: i32 = match job_type {
        // Check every 16 iterations for regular keys.
        ActiveExpiryType::Keys => 0xf,
        // Check every iteration for fields (more work per key).
        ActiveExpiryType::Fields => 0x0,
    };

    // Main loop over databases.
    let mut j: usize = 0;
    loop {
        let tl_exit = {
            ACTIVE_EXPIRE_STATE
                .lock()
                .unwrap_or_else(|e| e.into_inner())[job_idx]
                .timelimit_exit
        };
        if dbs_performed >= dbs_per_call || tl_exit || j >= db_count {
            break;
        }
        j += 1;

        let db_idx = {
            let mut guard = ACTIVE_EXPIRE_STATE
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let idx = guard[job_idx].current_db as usize % db_count;
            guard[job_idx].current_db = guard[job_idx].current_db.wrapping_add(1);
            idx
        };

        last_db_id = Some(db_idx as u32);

        let mut data = ExpireScanData {
            db_id: db_idx as u32,
            now: ms_time_now(),
            sampled: 0,
            expired: 0,
            ttl_sum: 0,
            ttl_samples: 0,
            max_entries: keys_per_loop * 4,
            has_more_expired_entries: false,
        };

        // TODO(port): db->expires (kvstore) and db->keys_with_volatile_items
        // not yet on RedisDb stub. Count here is a placeholder.
        dbs_performed += 1;

        let db_done = false;
        let mut update_avg_ttl_times: i32 = 0;

        loop {
            iteration += 1;

            // TODO(port): kvstoreSize(kvs) not yet available; placeholder 0 breaks immediately.
            let num: u64 = 0;
            if num == 0 {
                // TODO(port): db->expiry not yet on RedisDb stub.
                // PORT NOTE: db_done = true removed here — assignment was dead (break follows immediately).
                break;
            }

            data.now = ms_time_now();
            data.sampled = 0;
            data.expired = 0;

            let num = num.min(keys_per_loop);
            let max_buckets: u64 = num * 10;
            let checked_buckets: u64 = 0;
            let origin_ttl_samples = data.ttl_samples;

            // Scan buckets until enough keys sampled.
            while data.sampled < num && checked_buckets < max_buckets {
                // TODO(port): kvstoreScan(kvs, cursor, -1, -1, scan_cb, skip_cb, &data)
                // not yet ported. Cannot scan until kvstore lands in redis-ds.
                // PORT NOTE: checked_buckets += 1 removed — dead assignment before break.
                break; // placeholder: nothing to scan yet
            }

            total_expired += data.expired as i64;
            total_sampled += data.sampled as i64;

            if data.ttl_samples - origin_ttl_samples > 0 {
                update_avg_ttl_times += 1;
            }

            // Repeat if stale percentage is still too high.
            let repeat = if db_done {
                false
            } else if data.sampled == 0 {
                true
            } else {
                (data.expired * 100 / data.sampled) > config_cycle_acceptable_stale as u64
            };

            // Update avg_ttl every 16 iterations or on exit.
            if ((iteration & 0xf) == 0 || !repeat)
                && data.ttl_samples > 0
                && matches!(job_type, ActiveExpiryType::Keys)
            {
                let avg_ttl = data.ttl_sum / data.ttl_samples as i64;
                // closed-form geometric series avg using AVG_TTL_FACTOR.
                // TODO(port): db->expiry[jobType].avg_ttl not yet on RedisDb stub.
                // The formula: new_avg = avg_ttl + (old_avg - avg_ttl) * pow(0.98, n)
                // where n = update_avg_ttl_times (clamped to 1..16).
                let factor_idx = (update_avg_ttl_times as usize).saturating_sub(1).min(15);
                let _ = (avg_ttl, AVG_TTL_FACTOR[factor_idx]);
                update_avg_ttl_times = 0;
                data.ttl_sum = 0;
                data.ttl_samples = 0;
            }

            // Enforce time limit.
            if (iteration & time_check_mask) == 0 && elapsed_us(start) > timelimit_us as u64 {
                let mut guard = ACTIVE_EXPIRE_STATE
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                guard[job_idx].timelimit_exit = true;
                // TODO(port): server.stat_expired_time_cap_reached_count not on stub.
                break;
            }

            if !repeat {
                break;
            }
        }
    }

    let elapsed = elapsed_us(start) as UsTime;

    // TODO(port): latencyTraceIfNeeded(db, expire_cycle_keys/fields, elapsed) not yet ported.
    // PORT NOTE: In C the `db` variable passed to latencyTraceIfNeeded is from the inner
    // loop scope and is technically out of scope here — likely a C UB or intentional
    // "last db seen" idiom. Rust captures last_db_id instead.
    let _ = last_db_id;

    // Update stale-key percentage estimate (5% new, 95% old).
    // TODO(port): server.stat_expired_keys_stale_perc / stat_expired_keys_with_vola_stale_perc
    // not yet on RedisServer stub.
    let current_perc = if total_sampled > 0 {
        total_expired as f64 / total_sampled as f64
    } else {
        0.0
    };
    let _ = current_perc;

    elapsed
}

// ── activeExpireCycle ───────────────────────────────────────────────────────
/// Top-level active expiry entry point. Alternates KEYS/FIELDS priority each call.
/// Returns total microseconds spent.
pub fn active_expire_cycle(server: &mut RedisServer, cycle_type: i32) -> UsTime {
    // TODO(port): isPausedActionsWithUpdate(PAUSE_ACTION_EXPIRE) not yet ported.
    let _ = PAUSE_ACTION_EXPIRE;

    let effort = active_expire_effort(server);

    let timelimit_us: UsTime = if cycle_type == ACTIVE_EXPIRE_CYCLE_FAST {
        let config_cycle_fast_duration =
            ACTIVE_EXPIRE_CYCLE_FAST_DURATION + ACTIVE_EXPIRE_CYCLE_FAST_DURATION / 4 * effort;

        let start = get_monotonic_us();
        let last = *LAST_FAST_CYCLE_START
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        // never repeat a fast cycle within its own duration window.
        if (start as i64) < (last as i64 + config_cycle_fast_duration * 2) {
            return 0;
        }
        *LAST_FAST_CYCLE_START
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = start;
        config_cycle_fast_duration
    } else {
        let config_cycle_slow_time_perc = ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC + 2 * effort;
        // TODO(port): server.hz not yet on RedisServer stub; using 10 Hz default.
        let hz: i64 = 10;
        config_cycle_slow_time_perc * 1_000_000 / hz / 100
    };

    // TODO(port): serverAssert(server.also_propagate.numops == 0) — also_propagate not on stub.

    let mut elapsed: UsTime = 0;
    let start_with_fields = *EXPIRE_CYCLE_START_WITH_FIELDS
        .lock()
        .unwrap_or_else(|e| e.into_inner());

    // Alternate which expiry type gets priority.
    if start_with_fields {
        elapsed += active_expire_cycle_job(
            server,
            ActiveExpiryType::Fields,
            cycle_type,
            timelimit_us - elapsed,
        );
        elapsed += active_expire_cycle_job(
            server,
            ActiveExpiryType::Keys,
            cycle_type,
            timelimit_us - elapsed,
        );
    } else {
        elapsed += active_expire_cycle_job(
            server,
            ActiveExpiryType::Keys,
            cycle_type,
            timelimit_us - elapsed,
        );
        elapsed += active_expire_cycle_job(
            server,
            ActiveExpiryType::Fields,
            cycle_type,
            timelimit_us - elapsed,
        );
    }

    // TODO(port): server.stat_expire_cycle_time_used not yet on stub.
    // TODO(port): latencyAddSampleIfNeeded("expire-cycle", elapsed) not yet ported.
    // TODO(port): latencyTraceIfNeeded(db, expire_cycle, elapsed) — `db` N/A here per C code bug.

    *EXPIRE_CYCLE_START_WITH_FIELDS
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = !start_with_fields;
    elapsed
}

// ── expireReplicaKeys ───────────────────────────────────────────────────────
/// Scans `REPLICA_KEYS_WITH_EXPIRE` and expires keys whose TTL has passed.
/// Runs at most 64 iterations or 1 ms, whichever comes first.
pub fn expire_replica_keys(server: &mut RedisServer) {
    let has_keys = {
        let guard = REPLICA_KEYS_WITH_EXPIRE
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.as_ref().map(|h| !h.is_empty()).unwrap_or(false)
    };
    if !has_keys {
        return;
    }

    let mut cycles: i32 = 0;
    let mut noexpire: i32 = 0;
    let start = ms_time_now();

    loop {
        // Pick a random entry. Using first entry as placeholder for now.
        // PERF(port): C uses random selection to avoid hot-spot bias; first-entry is O(1) but biased.
        let entry = {
            let guard = REPLICA_KEYS_WITH_EXPIRE
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard
                .as_ref()
                .and_then(|h| h.iter().next().map(|(k, v)| (k.clone(), *v)))
        };
        let (keyname, dbids) = match entry {
            Some(pair) => pair,
            None => break,
        };

        let mut new_dbids: u64 = 0;
        let mut remaining = dbids;
        let mut dbid: u32 = 0;

        // Check each db whose bit is set in the bitmap.
        while remaining != 0 && (dbid as usize) < server.db_count() {
            if (remaining & 1) != 0 {
                // TODO(port): getKVStoreIndexForKey not yet ported.
                // TODO(port): dbFindExpiresWithDictIndex not yet on RedisDb stub.
                // TODO(port): db->expires (kvstore TTL index) not yet on RedisDb stub.
                let expired = false; // placeholder until db->expires is ported

                if !expired {
                    noexpire += 1;
                    new_dbids |= 1u64 << dbid;
                }
                // TODO(port): postExecutionUnitOperations after DEL propagation not ported.
            }
            dbid += 1;
            remaining >>= 1;
        }

        // Update or remove the bitmap entry.
        {
            let mut guard = REPLICA_KEYS_WITH_EXPIRE
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(h) = guard.as_mut() {
                if new_dbids != 0 {
                    h.insert(keyname.clone(), new_dbids);
                } else {
                    h.remove(&keyname);
                }
            }
        }

        cycles += 1;
        if noexpire > 3 {
            break;
        }
        if (cycles % 64) == 0 && ms_time_now() - start > 1 {
            break;
        }
        let is_empty = {
            let guard = REPLICA_KEYS_WITH_EXPIRE
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.as_ref().map(|h| h.is_empty()).unwrap_or(true)
        };
        if is_empty {
            break;
        }
    }
}

// ── rememberReplicaKeyWithExpire ────────────────────────────────────────────
/// Records that `key` in `db` may have a local expire set on this replica.
///
/// Skips databases with id > 63 (only 64 bits in the bitmask).
pub fn remember_replica_key_with_expire(db: &RedisDb, key: &RedisObject) {
    if db.id > 63 {
        return;
    }

    // TODO(port): objectGetVal not yet on RedisObject stub; extracting bytes from
    // String variant as a placeholder — should handle all types uniformly.
    let key_bytes: RedisString = match key.as_string() {
        Some(s) => s.clone(),
        None => {
            // TODO(port): other RedisObject variants need objectGetVal equivalent.
            return;
        }
    };

    let mut guard = REPLICA_KEYS_WITH_EXPIRE
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let h = guard.get_or_insert_with(HashMap::new);
    // If new entry, copy the SDS key and zero bitmap.
    let entry = h.entry(key_bytes).or_insert(0u64);
    *entry |= 1u64 << db.id;
}

// ── getReplicaKeyWithExpireCount ────────────────────────────────────────────
/// Returns the number of keys currently tracked in the replica expire dict.
pub fn get_replica_key_with_expire_count() -> usize {
    let guard = REPLICA_KEYS_WITH_EXPIRE
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    guard.as_ref().map(|h| h.len()).unwrap_or(0)
}

// ── flushReplicaKeysWithExpireList ──────────────────────────────────────────
/// Drops all replica expire tracking, optionally asynchronously.
pub fn flush_replica_keys_with_expire_list(_async_free: bool) {
    // TODO(port): freeReplicaKeysWithExpireAsync not yet ported; always drop synchronously.
    let mut guard = REPLICA_KEYS_WITH_EXPIRE
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *guard = None;
}

// ── checkAlreadyExpired ─────────────────────────────────────────────────────
/// Returns `true` if `when` is already in the past and the server should immediately
/// delete the key rather than storing it with the (past) expire time.
///
/// Returns `false` during AOF load, as a replica, in import mode, or during slot
/// migration — in those cases the key is stored anyway.
pub fn check_already_expired(server: &RedisServer, when: MsTime) -> bool {
    // TODO(port): server.current_client / slot_migration_job not yet on stub.

    // TODO(port): commandTimeSnapshot not yet ported; using ms_time_now() approximation.
    // TODO(port): server.loading / server.primary_host not on stub.
    // A primary in import-mode stores an already-expired key (with its past expire)
    // instead of deleting it immediately, and waits for the import source to propagate it.
    let now = ms_time_now();
    when <= now && !server.live_config.import_mode()
}

// ── parseExtendedExpireArgumentsOrReply ────────────────────────────────────
/// Parses optional NX / XX / GT / LT flags from command args starting at index 3.
///
/// Updates `flags` in place. Returns `Err` on invalid or conflicting options.
pub fn parse_extended_expire_arguments(
    ctx: &CommandContext,
    flags: &mut i32,
    max_args: usize,
) -> Result<(), RedisError> {
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;

    let mut j = 3usize;
    while j < max_args {
        let opt = ctx.arg(j)?;
        let opt_bytes = opt.as_bytes();
        if opt_bytes.eq_ignore_ascii_case(b"nx") {
            *flags |= EXPIRE_NX;
            nx = true;
        } else if opt_bytes.eq_ignore_ascii_case(b"xx") {
            *flags |= EXPIRE_XX;
            xx = true;
        } else if opt_bytes.eq_ignore_ascii_case(b"gt") {
            *flags |= EXPIRE_GT;
            gt = true;
        } else if opt_bytes.eq_ignore_ascii_case(b"lt") {
            *flags |= EXPIRE_LT;
            lt = true;
        } else {
            let mut msg: Vec<u8> = b"ERR Unsupported option ".to_vec();
            msg.extend_from_slice(opt_bytes);
            return Err(RedisError::runtime(msg));
        }
        j += 1;
    }

    if (nx && xx) || (nx && gt) || (nx && lt) {
        return Err(RedisError::runtime(
            b"ERR NX and XX, GT or LT options at the same time are not compatible".as_ref(),
        ));
    }
    if gt && lt {
        return Err(RedisError::runtime(
            b"ERR GT and LT options at the same time are not compatible".as_ref(),
        ));
    }

    Ok(())
}

// ── convertExpireArgumentToUnixTime ────────────────────────────────────────
/// Parses `arg` as an integer, applies `unit` conversion, adds `basetime`,
/// and returns the resulting absolute Unix millisecond timestamp.
///
/// `cmd_name` is the lowercase command name used to format the canonical
/// `ERR invalid expire time in '<cmd>' command` error when validation
/// fails.
pub fn convert_expire_argument_to_unix_time(
    arg: &RedisString,
    basetime: MsTime,
    unit: i32,
    cmd_name: &[u8],
) -> Result<MsTime, RedisError> {
    let when: i64 = parse_i64_from_redis_string(arg)?;

    if when < 0 {
        return Err(expire_time_error(cmd_name));
    }

    let when = if unit == UNIT_SECONDS {
        if when > i64::MAX / 1000 {
            return Err(expire_time_error(cmd_name));
        }
        when * 1000
    } else {
        when
    };

    if when > i64::MAX - basetime {
        return Err(expire_time_error(cmd_name));
    }

    Ok(when + basetime)
}

// ── expireGenericCommand ────────────────────────────────────────────────────
/// Generic implementation for EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT.
///
/// `basetime`: 0 for *AT variants; `commandTimeSnapshot()` for relative variants.
/// `unit`: `UNIT_SECONDS` or `UNIT_MILLISECONDS`.
pub fn expire_generic_command(
    ctx: &mut CommandContext,
    basetime: MsTime,
    unit: i32,
) -> Result<(), RedisError> {
    let mut flag: i32 = 0;
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    parse_extended_expire_arguments(ctx, &mut flag, argc)?;

    let cmd_name_lower = ascii_lower(ctx.command_name());
    let param = ctx.arg(2)?.clone();
    let mut when: MsTime = parse_i64_from_redis_string(&param)?;

    if unit == UNIT_SECONDS {
        if !(i64::MIN / 1000..=i64::MAX / 1000).contains(&when) {
            return Err(expire_time_error(&cmd_name_lower));
        }
        when *= 1000;
    }
    if basetime > 0 && when > i64::MAX - basetime {
        return Err(expire_time_error(&cmd_name_lower));
    }
    when += basetime;

    let key = ctx.arg(1)?.clone();

    let current_expire = ctx.db().get_expire(&key);
    let key_exists = ctx.db_mut().lookup_key_write(&key).is_some();
    if !key_exists {
        ctx.client_mut().set_prevent_propagation();
        return ctx.reply_integer(0);
    }

    let has_expire = current_expire != crate::object::EXPIRY_NONE;
    if (flag & EXPIRE_NX != 0 && has_expire)
        || (flag & EXPIRE_XX != 0 && !has_expire)
        || (flag & EXPIRE_GT != 0 && !has_expire)
        || (flag & EXPIRE_GT != 0 && has_expire && when <= current_expire)
        || (flag & EXPIRE_LT != 0 && has_expire && when >= current_expire)
    {
        ctx.client_mut().set_prevent_propagation();
        return ctx.reply_integer(0);
    }

    if check_already_expired(ctx.server(), when) {
        ctx.db_mut().sync_delete(&key);
        server_metrics()
            .expired_keys
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ctx.notify_keyspace_event(NOTIFY_EXPIRED, b"expired", &key);
        rewrite_expire_propagation_unlink(ctx, &key);
        return ctx.reply_integer(1);
    }

    ctx.db_mut().set_expire(&key, when);
    ctx.db().signal_modified(&key);
    ctx.notify_keyspace_event(NOTIFY_GENERIC, b"expire", &key);
    rewrite_expire_propagation_pexpireat(ctx, &key, when);
    ctx.reply_integer(1)
}

/// Rewrite the client's argv so the EXPIRE-family command propagates as the
/// canonical `PEXPIREAT key <absolute-ms>`.
///
/// Replicas and the AOF always receive an absolute millisecond timestamp so a
/// key never outlives the primary's intent due to replication lag, and so the
/// representation matches the RDB form.
fn rewrite_expire_propagation_pexpireat(ctx: &mut CommandContext, key: &RedisString, when: MsTime) {
    ctx.client_mut().set_args(vec![
        RedisString::from_bytes(b"PEXPIREAT"),
        key.clone(),
        RedisString::from_bytes(when.to_string().as_bytes()),
    ]);
}

/// Rewrite the client's argv so an EXPIRE-family command whose timestamp is
/// already in the past propagates as `UNLINK key` (the key is deleted rather
/// than given a TTL). Matches the default `lazyfree-lazy-expire yes` behavior.
fn rewrite_expire_propagation_unlink(ctx: &mut CommandContext, key: &RedisString) {
    ctx.client_mut()
        .set_args(vec![RedisString::from_bytes(b"UNLINK"), key.clone()]);
}

/// EXPIRE key seconds [ NX | XX | GT | LT ]
pub fn expire_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let now = ms_time_now();
    expire_generic_command(ctx, now, UNIT_SECONDS)
}

/// EXPIREAT key unix-time-seconds [ NX | XX | GT | LT ]
pub fn expireat_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    expire_generic_command(ctx, 0, UNIT_SECONDS)
}

/// PEXPIRE key milliseconds [ NX | XX | GT | LT ]
pub fn pexpire_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let now = ms_time_now();
    expire_generic_command(ctx, now, UNIT_MILLISECONDS)
}

/// PEXPIREAT key unix-time-milliseconds [ NX | XX | GT | LT ]
pub fn pexpireat_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    expire_generic_command(ctx, 0, UNIT_MILLISECONDS)
}

// ── ttlGenericCommand ────────────────────────────────────────────────────────
/// Implements TTL, PTTL, EXPIRETIME, PEXPIRETIME.
///
/// `output_ms`: reply in milliseconds when true, seconds when false.
/// `output_abs`: reply as absolute Unix timestamp when true, relative TTL when false.
pub fn ttl_generic_command(
    ctx: &mut CommandContext,
    output_ms: bool,
    output_abs: bool,
) -> Result<(), RedisError> {
    let key = ctx.arg(1)?.clone();
    let exists = ctx
        .db_mut()
        .lookup_key_read_with_flags(&key, LOOKUP_NOTOUCH)
        .is_some();
    if !exists {
        return ctx.reply_integer(-2);
    }
    let expire = ctx.db().get_expire(&key);
    if expire == crate::object::EXPIRY_NONE {
        return ctx.reply_integer(-1);
    }
    let raw_ttl: i64 = if output_abs {
        expire
    } else {
        expire - ms_time_now()
    };
    let ttl = raw_ttl.max(0);
    let out = if output_ms { ttl } else { (ttl + 500) / 1000 };
    ctx.reply_integer(out)
}

/// TTL key
pub fn ttl_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    ttl_generic_command(ctx, false, false)
}

/// PTTL key
pub fn pttl_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    ttl_generic_command(ctx, true, false)
}

/// EXPIRETIME key — absolute seconds, `-1` for no TTL, `-2` for missing key.
pub fn expiretime_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    ttl_generic_command(ctx, false, true)
}

/// PEXPIRETIME key — absolute milliseconds, `-1` for no TTL, `-2` for missing key.
pub fn pexpiretime_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    ttl_generic_command(ctx, true, true)
}

/// PERSIST key — remove the TTL from a key, making it persistent.
pub fn persist_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let key = ctx.arg(1)?.clone();
    let exists = ctx.db_mut().lookup_key_write(&key).is_some();
    if !exists {
        return ctx.reply_integer(0);
    }
    if ctx.db_mut().remove_expire(&key) {
        ctx.db().signal_modified(&key);
        ctx.notify_keyspace_event(NOTIFY_GENERIC, b"persist", &key);
        ctx.reply_integer(1)
    } else {
        ctx.reply_integer(0)
    }
}

// ── timestampIsExpired ──────────────────────────────────────────────────────
/// Returns `true` if `when` represents an already-elapsed Unix millisecond timestamp.
///
/// `now` should be `commandTimeSnapshot()` — passed explicitly to avoid
/// hidden dependencies on global state.
pub fn timestamp_is_expired(when: MsTime, now: MsTime) -> bool {
    if when < 0 {
        return false; // negative means no expire
    }
    now > when
}

// ── getExpirationPolicyWithFlags ────────────────────────────────────────────
/// Returns the expiration policy appropriate for the current server state and flags.
///
/// Used by key-lookup paths to decide whether to delete, keep, or ignore expired keys.
pub fn get_expiration_policy_with_flags(server: &RedisServer, flags: i32) -> ExpirationPolicy {
    // TODO(port): server.loading not yet on RedisServer stub.

    // TODO(port): server.primary_host / server.current_client / server.import_mode not on stub.

    // TODO(port): isPausedActionsWithUpdate not yet ported.

    let _ = (server, flags);
    ExpirationPolicy::DeleteExpired
}

// ── Shared private helpers ────────────────────────────────────────────────

/// Parse a Redis byte-string as a decimal `i64`.
fn parse_i64_from_redis_string(s: &RedisString) -> Result<i64, RedisError> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(RedisError::not_integer());
    }
    let mut i = 0usize;
    let negative = bytes[0] == b'-';
    if negative {
        i += 1;
    }
    if i >= bytes.len() {
        return Err(RedisError::not_integer());
    }
    let mut result: i64 = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if !(b'0'..=b'9').contains(&b) {
            return Err(RedisError::not_integer());
        }
        result = result
            .checked_mul(10)
            .and_then(|r| r.checked_add((b - b'0') as i64))
            .ok_or_else(RedisError::not_integer)?;
        i += 1;
    }
    Ok(if negative { -result } else { result })
}

/// Canonical "invalid expire time" error, matching the Redis wire format
/// `ERR invalid expire time in '<cmd>' command`.
fn expire_time_error(cmd_name: &[u8]) -> RedisError {
    let mut buf = Vec::with_capacity(
        b"ERR invalid expire time in '".len() + cmd_name.len() + b"' command".len(),
    );
    buf.extend_from_slice(b"ERR invalid expire time in '");
    buf.extend_from_slice(cmd_name);
    buf.extend_from_slice(b"' command");
    RedisError::runtime(buf)
}

/// Returns a lowercase copy of `bytes`, used to format the command-name
/// portion of `expire_time_error` since `ctx.command_name()` is the
/// uppercase wire token but Redis embeds the lowercase form.
fn ascii_lower(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(|b| b.to_ascii_lowercase()).collect()
}

// ──────────────────────────────────────────────────────────────────────────────
// Active-expiration background driver
//
// A minimal background thread that reaps TTL keys. Config is held in
// `ACTIVE_EXPIRE_CONFIG` as a pair of atomics so the CONFIG SET path can flip
// values mid-flight without taking a Mutex on the hot loop.
// ──────────────────────────────────────────────────────────────────────────────

/// Maximum wall-time budget for a single active-expire tick. The thread will
/// stop the aggressive (>25% expired) inner loop once this many milliseconds
/// have elapsed since the tick began, so that other connections can make
/// progress.
const ACTIVE_EXPIRE_TICK_BUDGET_MS: u128 = 25;

/// Number of keys sampled per pass per effort unit. effort=1 → 20 keys,
/// effort=10 → 200 keys.
const ACTIVE_EXPIRE_KEYS_PER_EFFORT: usize = 20;

/// Effort/hz pair driving the active-expiration thread.
///
/// `effort` of `0` disables the thread (it observes the value and idles); any
/// non-zero value enables sampling. `hz` controls how often the thread wakes
/// — `1000 / hz` ms between ticks, default `10` (100 ms tick interval).
pub struct ActiveExpireConfig {
    pub effort: AtomicU8,
    pub hz: AtomicU32,
}

impl ActiveExpireConfig {
    /// Default config: effort=1 (minimum aggressiveness), hz=10 (10 Hz wake).
    pub const fn default_const() -> Self {
        Self {
            effort: AtomicU8::new(1),
            hz: AtomicU32::new(10),
        }
    }

    pub fn snapshot(&self) -> (u8, u32) {
        (
            self.effort.load(Ordering::Relaxed),
            self.hz.load(Ordering::Relaxed),
        )
    }

    pub fn set_effort(&self, effort: u8) {
        let clamped = effort.min(10);
        self.effort.store(clamped, Ordering::Relaxed);
    }

    pub fn set_hz(&self, hz: u32) {
        let clamped = hz.clamp(1, 500);
        self.hz.store(clamped, Ordering::Relaxed);
    }
}

static ACTIVE_EXPIRE_CONFIG: OnceLock<Arc<ActiveExpireConfig>> = OnceLock::new();

/// Process-global active-expire config. First caller initialises with the
/// default; CONFIG SET and the spawn helper both go through here.
pub fn active_expire_config() -> &'static Arc<ActiveExpireConfig> {
    ACTIVE_EXPIRE_CONFIG.get_or_init(|| Arc::new(ActiveExpireConfig::default_const()))
}

/// Spawn the active-expire background thread. Returns the join handle so the
/// caller can `.join()` on shutdown if desired. The thread runs until the
/// process exits; there is intentionally no stop flag because the binary's
/// shutdown path tears everything down with the process.
pub fn spawn_active_expire_thread(
    db: Arc<Mutex<RedisDb>>,
    config: Arc<ActiveExpireConfig>,
    metrics: Option<Arc<ServerMetrics>>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("active-expire".to_string())
        .spawn(move || active_expire_loop(db, config, metrics))
        .unwrap_or_else(|e| {
            eprintln!("active-expire: thread spawn failed: {}", e);
            thread::spawn(|| {})
        })
}

fn active_expire_loop(
    db: Arc<Mutex<RedisDb>>,
    config: Arc<ActiveExpireConfig>,
    metrics: Option<Arc<ServerMetrics>>,
) {
    loop {
        let (effort, hz) = config.snapshot();
        let sleep_ms = if hz == 0 { 100 } else { (1000 / hz).max(1) };
        thread::sleep(Duration::from_millis(sleep_ms as u64));

        if effort == 0 {
            continue;
        }

        run_active_expire_tick(&db, effort, metrics.as_deref());
    }
}

/// One tick of the active-expire cycle. Locks the db, samples, deletes
/// expired, repeats while >25% of the sample was expired and the tick budget
/// has not been exhausted. Returns the number of keys deleted.
fn run_active_expire_tick(
    db: &Arc<Mutex<RedisDb>>,
    effort: u8,
    metrics: Option<&ServerMetrics>,
) -> u64 {
    let mut deleted_total = 0u64;
    let mut guard = match db.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    deleted_total += run_active_expire_tick_on_db(&mut guard, effort, metrics);
    deleted_total
}

/// One active-expire tick against a caller-owned DB.
///
/// RuntimeOwner calls this from its cron/expire step after the live keyspace
/// moves into the owner-owned DB vector. The legacy background-thread wrapper
/// above is retained for tests and non-owner callers, but the default server
/// path no longer spawns it after the owner-owned DB flip.
pub fn run_active_expire_tick_on_db(
    db: &mut RedisDb,
    effort: u8,
    metrics: Option<&ServerMetrics>,
) -> u64 {
    use crate::db::notify_keyspace_event_global;
    use crate::notify::NOTIFY_EXPIRED;

    let sample_size = ACTIVE_EXPIRE_KEYS_PER_EFFORT.saturating_mul(effort as usize);
    if sample_size == 0 {
        return 0;
    }

    let tick_start = std::time::Instant::now();
    let mut total_deleted: u64 = 0;

    loop {
        let now_ms = wall_clock_ms();
        let seed = pseudo_random_seed();

        let db_id = db.id;
        let sample = db.sample_expiring_keys(sample_size, seed);
        let mut deleted_keys: Vec<RedisString> = Vec::new();
        for (key, expire_at) in &sample {
            if *expire_at <= now_ms && db.sync_delete(key) {
                deleted_keys.push(key.clone());
            }
        }
        let deleted = deleted_keys.len() as u64;
        let sampled = sample.len();
        for key in &deleted_keys {
            notify_keyspace_event_global(NOTIFY_EXPIRED, b"expired", key, db_id);
        }

        total_deleted = total_deleted.saturating_add(deleted);
        if let Some(m) = metrics {
            if deleted > 0 {
                m.expired_keys.fetch_add(deleted, Ordering::Relaxed);
            }
        }

        if sampled == 0 {
            break;
        }
        let threshold = sampled / 4;
        let should_repeat = (deleted as usize) > threshold;
        if !should_repeat {
            break;
        }
        if tick_start.elapsed().as_millis() >= ACTIVE_EXPIRE_TICK_BUDGET_MS {
            break;
        }
    }

    total_deleted
}

fn wall_clock_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Cheap pseudo-random seed for sample-start offsets. Uses the system
/// monotonic clock so successive ticks pick different starting points
/// without pulling in the `rand` crate.
fn pseudo_random_seed() -> u64 {
    get_monotonic_us()
}

#[cfg(test)]
mod active_expire_tests {
    use super::*;
    use crate::object::{ObjectKind, RedisObject, StringEncoding, EXPIRY_NONE};

    fn make_str_obj_with_expire(value: &[u8], expire: i64) -> RedisObject {
        RedisObject {
            lru: Default::default(),
            expire,
            kind: ObjectKind::String(StringEncoding::Raw(RedisString::from_bytes(value))),
        }
    }

    #[test]
    fn tick_reaps_expired_keys() {
        let db = Arc::new(Mutex::new(RedisDb::new(0)));
        {
            let mut guard = db.lock().expect("lock");
            let past = 1i64;
            guard.add(
                RedisString::from_bytes(b"a"),
                make_str_obj_with_expire(b"v", past),
            );
            guard.add(
                RedisString::from_bytes(b"b"),
                make_str_obj_with_expire(b"v", past),
            );
            guard.add(
                RedisString::from_bytes(b"c"),
                make_str_obj_with_expire(b"v", past),
            );
            guard.add(
                RedisString::from_bytes(b"keep"),
                make_str_obj_with_expire(b"v", EXPIRY_NONE),
            );
        }
        let deleted = run_active_expire_tick(&db, 1, None);
        assert!(
            deleted >= 3,
            "expected to reap at least 3 expired keys, got {}",
            deleted
        );
        let guard = db.lock().expect("lock");
        assert!(guard.exists_raw(&RedisString::from_bytes(b"keep")));
    }

    #[test]
    fn tick_with_effort_zero_skipped_by_loop() {
        let config = Arc::new(ActiveExpireConfig::default_const());
        config.set_effort(0);
        let (effort, _) = config.snapshot();
        assert_eq!(effort, 0);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/expire.c (1032 lines, 28 functions)
//                  src/expire.h (76 lines)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         31
//   port_notes:    5
//   unsafe_blocks: 0
//   notes:         Logic faithful to C. Major blockers: RedisObject::expire_ms(),
//                  db->expires kvstore, db->expiry[jobType] cursor/avg_ttl fields,
//                  server stat fields, commandTimeSnapshot, kvstoreScan callbacks,
//                  CommandContext server/db access (Phase 3 architect packet).
//                  expire_scan_callback / field_expire_scan_callback are typed
//                  Rust fns pending kvstoreScan adaptation. parse_i64_from_redis_string
//                  should move to util module in Phase B.
// ──────────────────────────────────────────────────────────────────────────────
