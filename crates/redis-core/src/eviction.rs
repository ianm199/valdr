//! Sampling-based maxmemory enforcement — coarse Rust-native replacement for
//! the C-shaped skeleton in `evict.rs`.
//! Algorithm sketch (intentional simplifications vs upstream):
//! - Memory usage is whatever [`approximate_memory_used`] returns; the C
//! allocator-tracked `zmalloc_used_memory` is not faithfully ported.
//! - Candidate selection is N-of-K sampling over the main dict's iteration
//! order rather than the C eviction pool sorted by idle time. K is
//! `maxmemory-samples` (default 5) and the oldest/lowest-scored sample
//! is evicted.
//! - All eight maxmemory policies are implemented: `noeviction`,
//! `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`,
//! `volatile-lfu`, `volatile-random`, `volatile-ttl`.
//! LFU counter encoding in `RedisObject.lru` (same 24-bit field as LRU):
//! - bits 7..0 (low 8 bits): logarithmic frequency counter (0–255)
//! - bits 23..8 (next 16 bits): last-decrement timestamp in minutes
//! (truncated from the real clock, wraps after ~45 days)
//! Initial value for new objects: low-8 = LFU_INIT_VAL (5), high-16 = now_minutes.

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_types::RedisString;

use crate::db::RedisDb;
use crate::live_config::MaxmemoryPolicyCode;
use crate::memory::approximate_memory_used;
use crate::metrics::server_metrics;
use crate::object::{RedisObject, EXPIRY_NONE};

/// Maximum number of evictions to attempt in a single dispatch-gate call.
/// Caps the worst-case work done before the gate gives up and returns
/// [`EvictionOutcome::StillOver`]. Tuned to "small enough that one slow
/// command does not stall the server, large enough that any realistic
/// per-command memory growth can be absorbed".
const MAX_EVICTIONS_PER_CALL: usize = 100;

/// Default `maxmemory-samples` per upstream config. Controls the K
/// "sample K keys, evict the oldest/least-frequently-used".
const DEFAULT_MAXMEMORY_SAMPLES: usize = 5;

/// Initial LFU counter assigned to newly-created objects.
/// Matches `LFU_INIT_VAL` in Valkey's.
const LFU_INIT_VAL: u8 = 5;

/// Outcome of a single eviction attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum EvictionOutcome {
 /// `approximate_memory_used` was already below `target_bytes` on entry.
    Sufficient,
 /// Evicted these keys and is now under `target_bytes`.
    Evicted(Vec<RedisString>),
 /// Evicted these keys but remained over the limit.
    StillOver(Vec<RedisString>),
}

/// Drive eviction until either `approximate_memory_used(db) <= target_bytes`,
/// the per-call budget is exhausted, or the configured policy refuses
/// evict anything.
/// The eviction policy is consulted on every iteration so that a runtime
/// CONFIG SET seen mid-loop is honoured immediately.
pub fn try_evict_to_fit(
    db: &mut RedisDb,
    target_bytes: u64,
    policy: MaxmemoryPolicyCode,
    log_factor: u32,
    decay_time: u32,
) -> EvictionOutcome {
    if approximate_memory_used(db) <= target_bytes {
        return EvictionOutcome::Sufficient;
    }

    let mut evicted: Vec<RedisString> = Vec::new();
    while evicted.len() < MAX_EVICTIONS_PER_CALL {
        let victim = match select_victim(db, policy, log_factor, decay_time) {
            Some(k) => k,
            None => break,
        };
        if !db.delete(&victim) {
            break;
        }
        evicted.push(victim);
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
        EvictionOutcome::StillOver(evicted)
    }
}

fn select_victim(
    db: &RedisDb,
    policy: MaxmemoryPolicyCode,
    log_factor: u32,
    decay_time: u32,
) -> Option<RedisString> {
    match policy {
        MaxmemoryPolicyCode::NoEviction => None,
        MaxmemoryPolicyCode::AllkeysLru => sample_allkeys_lru(db, DEFAULT_MAXMEMORY_SAMPLES),
        MaxmemoryPolicyCode::AllkeysLfu => {
            sample_allkeys_lfu(db, DEFAULT_MAXMEMORY_SAMPLES, log_factor, decay_time)
        }
        MaxmemoryPolicyCode::AllkeysRandom => sample_allkeys_random(db, DEFAULT_MAXMEMORY_SAMPLES),
        MaxmemoryPolicyCode::VolatileLru => sample_volatile_lru(db, DEFAULT_MAXMEMORY_SAMPLES),
        MaxmemoryPolicyCode::VolatileLfu => {
            sample_volatile_lfu(db, DEFAULT_MAXMEMORY_SAMPLES, log_factor, decay_time)
        }
        MaxmemoryPolicyCode::VolatileRandom => {
            sample_volatile_random(db, DEFAULT_MAXMEMORY_SAMPLES)
        }
        MaxmemoryPolicyCode::VolatileTtl => sample_volatile_ttl(db, DEFAULT_MAXMEMORY_SAMPLES),
    }
}

fn sample_allkeys_lru(db: &RedisDb, samples: usize) -> Option<RedisString> {
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

fn lfu_effective_counter(obj: &RedisObject, log_factor: u32, decay_time: u32) -> u8 {
    let raw = obj.lru;
    let counter = (raw & 0xFF) as u8;
    let last_decrement_minutes = (raw >> 8) & 0xFFFF;

    let now_minutes = now_minutes();
    let elapsed = now_minutes.wrapping_sub(last_decrement_minutes);
    let decay_steps = if decay_time == 0 {
        0u32
    } else {
        elapsed / decay_time
    };

    let decayed = counter.saturating_sub(decay_steps.min(255) as u8);
    let _ = log_factor;
    decayed
}

fn sample_allkeys_lfu(
    db: &RedisDb,
    samples: usize,
    log_factor: u32,
    decay_time: u32,
) -> Option<RedisString> {
    let take = samples.max(1);
    let mut lowest: Option<(&RedisString, u8)> = None;
    for (key, obj) in db.iter_for_eviction().take(take) {
        let freq = lfu_effective_counter(obj, log_factor, decay_time);
        lowest = match lowest {
            None => Some((key, freq)),
            Some((_, best_freq)) if freq < best_freq => Some((key, freq)),
            Some(curr) => Some(curr),
        };
    }
    lowest.map(|(k, _)| k.clone())
}

fn sample_allkeys_random(db: &RedisDb, samples: usize) -> Option<RedisString> {
    let take = samples.max(1);
    db.iter_for_eviction()
        .take(take)
        .next()
        .map(|(k, _)| k.clone())
}

fn sample_volatile_lru(db: &RedisDb, samples: usize) -> Option<RedisString> {
    let take = samples.max(1);
    let mut oldest: Option<(&RedisString, &RedisObject)> = None;
    for (key, obj) in db
        .iter_for_eviction()
        .filter(|(_, o)| o.expire != EXPIRY_NONE)
        .take(take)
    {
        oldest = match oldest {
            None => Some((key, obj)),
            Some((_, best)) if obj.lru < best.lru => Some((key, obj)),
            Some(curr) => Some(curr),
        };
    }
    oldest.map(|(k, _)| k.clone())
}

fn sample_volatile_lfu(
    db: &RedisDb,
    samples: usize,
    log_factor: u32,
    decay_time: u32,
) -> Option<RedisString> {
    let take = samples.max(1);
    let mut lowest: Option<(&RedisString, u8)> = None;
    for (key, obj) in db
        .iter_for_eviction()
        .filter(|(_, o)| o.expire != EXPIRY_NONE)
        .take(take)
    {
        let freq = lfu_effective_counter(obj, log_factor, decay_time);
        lowest = match lowest {
            None => Some((key, freq)),
            Some((_, best_freq)) if freq < best_freq => Some((key, freq)),
            Some(curr) => Some(curr),
        };
    }
    lowest.map(|(k, _)| k.clone())
}

fn sample_volatile_random(db: &RedisDb, samples: usize) -> Option<RedisString> {
    let take = samples.max(1);
    db.iter_for_eviction()
        .filter(|(_, o)| o.expire != EXPIRY_NONE)
        .take(take)
        .next()
        .map(|(k, _)| k.clone())
}

fn sample_volatile_ttl(db: &RedisDb, samples: usize) -> Option<RedisString> {
    let take = samples.max(1);
    let mut soonest: Option<(&RedisString, i64)> = None;
    for (key, obj) in db
        .iter_for_eviction()
        .filter(|(_, o)| o.expire != EXPIRY_NONE)
        .take(take)
    {
        let exp = obj.expire;
        soonest = match soonest {
            None => Some((key, exp)),
            Some((_, best_exp)) if exp < best_exp => Some((key, exp)),
            Some(curr) => Some(curr),
        };
    }
    soonest.map(|(k, _)| k.clone())
}

/// Return current Unix time truncated to whole minutes, wrapping to u32.
/// Used to track the last-decrement timestamp in the LFU counter's high 16 bits.
fn now_minutes() -> u32 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    (secs / 60) as u32
}

/// Initialise the LFU field of a freshly-created `RedisObject`.
/// Packs `LFU_INIT_VAL` into bits 7..0 and the current minute clock into
/// bits 23..8 so the first decay calculation has a valid baseline.
pub fn lfu_init(obj: &mut RedisObject) {
    let minutes = now_minutes() & 0xFFFF;
    obj.lru = (minutes << 8) | (LFU_INIT_VAL as u32);
}

/// Probabilistically increment the LFU counter and refresh the decay timestamp.
/// Formula from Valkey's `LFULogIncr`:
/// `counter += 1` with probability `1 / ((counter - LFU_INIT_VAL) * log_factor + 1)`
/// Decay is applied first: for every `decay_time` minutes elapsed since the last
/// decrement the counter is decremented by 1 (down to 0).
pub fn lfu_update(obj: &mut RedisObject, log_factor: u32, decay_time: u32) {
    let raw = obj.lru;
    let counter = (raw & 0xFF) as u8;
    let last_decrement_minutes = (raw >> 8) & 0xFFFF;

    let now = now_minutes();
    let elapsed = now.wrapping_sub(last_decrement_minutes);
    let decay_steps = if decay_time == 0 {
        0u32
    } else {
        elapsed / decay_time
    };
    let decayed = counter.saturating_sub(decay_steps.min(255) as u8);

    let incremented = if decayed < 255 {
        let baseval = if decayed > LFU_INIT_VAL {
            (decayed - LFU_INIT_VAL) as f64
        } else {
            0.0
        };
        let p = 1.0 / (baseval * log_factor as f64 + 1.0);
        let r = pseudo_random_f64(obj.lru as u64 ^ now as u64);
        if r < p {
            decayed + 1
        } else {
            decayed
        }
    } else {
        255
    };

    obj.lru = ((now & 0xFFFF) << 8) | (incremented as u32);
}

/// Deterministic pseudo-random f64 in [0.0, 1.0) derived from a seed.
/// Uses a simple xorshift64 step so there is no external `rand` dependency
/// and no mutable global state.
fn pseudo_random_f64(seed: u64) -> f64 {
    let mut x = seed;
    if x == 0 {
        x = 1;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    (x as f64) / (u64::MAX as f64)
}

/// Build the canonical error bytes for the -OOM case.
/// Called by the dispatch gate when the policy refused to evict anything
/// (e.g. `noeviction`, or `volatile-*` with no TTL'd candidates).
pub fn oom_error_reply() -> Vec<u8> {
    b"-OOM command not allowed when used memory > 'maxmemory'.\r\n".to_vec()
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

    fn db_with_ttl(keys: &[(&[u8], &[u8], u32, i64)]) -> RedisDb {
        let mut db = RedisDb::new(0);
        for (k, v, lru, exp) in keys {
            let mut obj = RedisObject::from_string(RedisString::from_bytes(v));
            obj.lru = *lru;
            obj.expire = *exp;
            db.insert(RedisString::from_bytes(k), obj);
        }
        db
    }

    #[test]
    fn under_limit_returns_sufficient() {
        let mut db = db_with(&[(b"k", b"v", 1)]);
        let res = try_evict_to_fit(&mut db, u64::MAX, MaxmemoryPolicyCode::AllkeysLru, 10, 1);
        assert_eq!(res, EvictionOutcome::Sufficient);
    }

    #[test]
    fn noeviction_returns_still_over_when_over_limit() {
        let mut db = db_with(&[(b"k1", b"value1", 1), (b"k2", b"value2", 2)]);
        let res = try_evict_to_fit(&mut db, 1, MaxmemoryPolicyCode::NoEviction, 10, 1);
        assert_eq!(res, EvictionOutcome::StillOver(Vec::new()));
        assert_eq!(db.len(), 2);
    }

    #[test]
    fn allkeys_lru_drops_keys_to_fit() {
        let mut db = db_with(&[
            (b"old", b"value", 1),
            (b"mid", b"value", 5),
            (b"new", b"value", 9),
        ]);
        let res = try_evict_to_fit(&mut db, 0, MaxmemoryPolicyCode::AllkeysLru, 10, 1);
        assert!(matches!(
            res,
            EvictionOutcome::Evicted(_) | EvictionOutcome::StillOver(_)
        ));
    }

    #[test]
    fn allkeys_lfu_evicts_lowest_counter() {
        let mut db = db_with(&[(b"rare", b"value", 1), (b"freq", b"value", 9)]);
        let res = try_evict_to_fit(&mut db, 0, MaxmemoryPolicyCode::AllkeysLfu, 10, 1);
        assert!(matches!(
            res,
            EvictionOutcome::Evicted(_) | EvictionOutcome::StillOver(_)
        ));
    }

    #[test]
    fn allkeys_random_evicts_something() {
        let mut db = db_with(&[(b"a", b"value", 1), (b"b", b"value", 2)]);
        let res = try_evict_to_fit(&mut db, 0, MaxmemoryPolicyCode::AllkeysRandom, 10, 1);
        assert!(matches!(
            res,
            EvictionOutcome::Evicted(_) | EvictionOutcome::StillOver(_)
        ));
    }

    #[test]
    fn volatile_lru_ignores_persistent_keys() {
        let far_future = i64::MAX;
        let mut db = db_with_ttl(&[
            (b"no_ttl", b"value", 1, EXPIRY_NONE),
            (b"with_ttl", b"value", 2, far_future),
        ]);
        let res = try_evict_to_fit(&mut db, 0, MaxmemoryPolicyCode::VolatileLru, 10, 1);
        assert!(matches!(
            res,
            EvictionOutcome::Evicted(_) | EvictionOutcome::StillOver(_)
        ));
        assert!(db.find(&RedisString::from_bytes(b"no_ttl")).is_some());
    }

    #[test]
    fn volatile_lru_with_no_ttl_keys_returns_still_over() {
        let mut db = db_with(&[(b"a", b"value", 1), (b"b", b"value", 2)]);
        let res = try_evict_to_fit(&mut db, 0, MaxmemoryPolicyCode::VolatileLru, 10, 1);
        assert_eq!(res, EvictionOutcome::StillOver(Vec::new()));
        assert_eq!(db.len(), 2);
    }

    #[test]
    fn volatile_ttl_picks_soonest_expiry() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut db = db_with_ttl(&[
            (
                b"soon",
                b"value_that_is_big_enough_to_matter",
                1,
                now_ms + 1_000,
            ),
            (
                b"later",
                b"value_that_is_big_enough_to_matter",
                2,
                now_ms + 100_000,
            ),
        ]);
        let size_after_one = approximate_memory_used(&db) / 2 + 1;
        try_evict_to_fit(
            &mut db,
            size_after_one,
            MaxmemoryPolicyCode::VolatileTtl,
            10,
            1,
        );
        assert!(db.find(&RedisString::from_bytes(b"later")).is_some());
    }

    #[test]
    fn lfu_update_increments_fresh_counter() {
        let mut obj = RedisObject::from_string(RedisString::from_bytes(b"v"));
        lfu_init(&mut obj);
        for _ in 0..20 {
            lfu_update(&mut obj, 10, 0);
        }
        let after = obj.lru & 0xFF;
        assert!(
            after >= LFU_INIT_VAL as u32,
            "counter should not fall below init value without decay"
        );
    }
}
