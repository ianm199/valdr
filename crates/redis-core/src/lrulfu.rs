//! LRU (Least Recently Used) and LFU (Least Frequently Used) clock and eviction
//! logic for the Redis maxmemory eviction policies.
//! Implements a shared 24-bit value (`lrulfu`) that represents either an LRU
//! timestamp (seconds) or an LFU frequency counter, depending on the active
//! eviction policy.
//! **LRU layout (24 bits):** seconds since epoch truncated to 24 bits.
//! Rolls over every ~194 days.
//! **LFU layout (24 bits):**
//! `[16 bits: last-access minutes][8 bits: LOG_C frequency]`
//! `LOG_C` is a logarithmic approximation of access frequency.
//! Rolls over every ~45 days.
//! PORT NOTE: Global clock state in C (`lru_clock`, `lfu_clock_minutes`,
//! `is_using_lfu_policy`) is translated to module-level atomics. These should
//! eventually migrate into `RedisServer` to support per-instance state in tests.
// TODO(architect): migrate LRU/LFU clock atomics into RedisServer for proper encapsulation

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU16, AtomicU32, Ordering};

// ─── Constants ────────────────────────────────────────────────────────────────

/// Number of bits used by an LRU/LFU value (packed into the lower bits of `u32`).
pub const LRULFU_BITS: u32 = 24;

const LRULFU_MASK: u32 = (1u32 << LRULFU_BITS) - 1;

/// LRU clock resolution in milliseconds.
/// The default of 1000 ms is expected for normal operation.
/// Set to 1 when building to support legacy Ruby LRU behaviour tests.
const LRU_CLOCK_RESOLUTION: u32 = 1000;

/// Initial LFU frequency counter value for newly created keys.
/// Starting above zero ensures new keys survive long enough to accumulate
/// accesses before being evicted.
const LFU_INIT_VAL: u8 = 5;

// ─── Configuration globals ────────────────────────────────────────────────────

/// LFU logarithmic counter factor (`lfu-log-factor` config option).
/// Controls how quickly the frequency saturates: higher values require more
/// accesses to reach the maximum counter value of 255.
pub static LFU_CONFIG_LOG_FACTOR: AtomicI32 = AtomicI32::new(10);

/// LFU counter decay period in minutes (`lfu-decay-time` config option).
/// The frequency counter is decremented by one for each elapsed decay period.
/// A value of 0 disables decay entirely.
pub static LFU_CONFIG_DECAY_TIME: AtomicI32 = AtomicI32::new(1);

// ─── Clock state ──────────────────────────────────────────────────────────────

/// 24-bit LRU clock counter in seconds (rolls over every ~194 days).
/// Updated by [`update_clock_and_policy`] on each server tick.
static LRU_CLOCK: AtomicU32 = AtomicU32::new(0);

/// 16-bit LFU clock counter in minutes (rolls over every ~45 days).
/// Updated by [`update_clock_and_policy`] on each server tick.
static LFU_CLOCK_MINUTES: AtomicU16 = AtomicU16::new(0);

/// Whether the server is currently configured to use the LFU eviction policy.
static IS_USING_LFU_POLICY: AtomicBool = AtomicBool::new(false);

// ─── LRU ──────────────────────────────────────────────────────────────────────

/// Returns the current 24-bit LRU clock value (normally seconds; rolls over).
fn lru_get_clock_time() -> u32 {
    LRU_CLOCK.load(Ordering::Relaxed)
}

/// Convert an idle duration in seconds into an LRU timestamp relative to now.
/// The returned value is the 24-bit clock value at which the key was last
/// accessed, computed by subtracting `idle_secs` from the current clock.
/// Underflow / wrap-around is intentional and expected (the 24-bit range
/// is designed to roll over).
pub fn lru_import(idle_secs: u32) -> u32 {
    let now = lru_get_clock_time();
    let adjusted = if LRU_CLOCK_RESOLUTION != 1000 {
        ((idle_secs as i64 * 1000) / LRU_CLOCK_RESOLUTION as i64) as u32
    } else {
        idle_secs
    };
    let masked = adjusted & LRULFU_MASK;
 // Underflow is ok/expected: C comment "Underflow is ok/expected"
    now.wrapping_sub(masked) & LRULFU_MASK
}

/// Compute how many seconds have elapsed since the given LRU timestamp.
pub fn lru_get_idle_secs(lru: u32) -> u32 {
 // Underflow is ok/expected
    let seconds = lru_get_clock_time().wrapping_sub(lru) & LRULFU_MASK;
    if LRU_CLOCK_RESOLUTION != 1000 {
        ((seconds as i64 * LRU_CLOCK_RESOLUTION as i64) / 1000) as u32
    } else {
        seconds
    }
}

// ─── LFU ──────────────────────────────────────────────────────────────────────

/// Returns the current 16-bit LFU clock value in minutes (rolls over every ~45 days).
fn lfu_get_time_in_minutes() -> u16 {
    LFU_CLOCK_MINUTES.load(Ordering::Relaxed)
}

/// Pack a frequency byte into an LFU value stamped with the current time.
pub fn lfu_import(freq: u8) -> u32 {
    ((lfu_get_time_in_minutes() as u32) << 8) | freq as u32
}

/// Apply time-based decay to an LFU value without recording a new access.
/// Computes elapsed minutes since the stored timestamp, divides by
/// configured decay period, and decrements the frequency counter accordingly.
/// The timestamp in the returned value is updated to the current time.
/// PORT NOTE: In C, `lfu_config_decay_time` is checked as a bare truthy int,
/// meaning negative values would trigger division. Here we guard on `> 0`
/// to be defensive; negative values are treated as "no decay" rather than
/// undefined integer division behaviour.
fn lfu_decay(lfu: u32) -> u32 {
    let now: u16 = lfu_get_time_in_minutes();
    let prev_time = (lfu >> 8) as u16;
    let freq = lfu as u8;
 // Wrap-around subtraction is expected/valid (C comment)
    let elapsed: u16 = now.wrapping_sub(prev_time);
    let decay_time = LFU_CONFIG_DECAY_TIME.load(Ordering::Relaxed);
    let num_periods: u16 = if decay_time > 0 {
        elapsed / (decay_time as u16)
    } else {
        0
    };
    let decayed_freq: u8 = if num_periods as u32 >= freq as u32 {
        0
    } else {
        freq - num_periods as u8
    };
    ((now as u32) << 8) | decayed_freq as u32
}

/// Logarithmically increment an LFU frequency counter.
/// Keys near 0 have a high probability of incrementing; keys near 255 are
/// logarithmically less likely to increment. The counter saturates at 255.
/// The probability of increment is `1 / (baseval * lfu_log_factor + 1)` where
/// `baseval = max(0, freq - LFU_INIT_VAL)`.
fn lfu_log_incr(freq: u8) -> u8 {
    if freq == u8::MAX {
        return freq;
    }
    // TODO(port): replace `crate::rand::rand_float()` with the actual rand
 // abstraction once crate::rand is available.
    // TODO(architect): expose a `rand_float() -> f64` function from redis-core::rand
 // (or add the `rand` crate as a dependency) so this has a real implementation.
    let r: f64 = crate::rand::rand_float();
    let baseval: f64 = if (freq as i32) < LFU_INIT_VAL as i32 {
        0.0
    } else {
        (freq as i32 - LFU_INIT_VAL as i32) as f64
    };
    let log_factor = LFU_CONFIG_LOG_FACTOR.load(Ordering::Relaxed);
    let p = 1.0 / (baseval * log_factor as f64 + 1.0);
    if r < p {
        freq + 1
    } else {
        freq
    }
}

/// Apply decay then add a "touch" (logarithmic increment) to an LFU value.
/// Returns the updated 24-bit LFU value with the frequency counter incremented
/// and the timestamp refreshed.
pub fn lfu_touch(lfu: u32) -> u32 {
    let decayed = lfu_decay(lfu);
    let freq = lfu_log_incr(decayed as u8);
 // Replace the low 8 bits with the updated frequency
    (decayed & !(u8::MAX as u32)) | freq as u32
}

/// Apply decay and return the current frequency counter without adding a touch.
/// Returns `(updated_lfu, freq)` where `updated_lfu` has decay applied
/// the stored timestamp refreshed to the current minute.
/// PORT NOTE: C signature uses an out-parameter `uint8_t *freq`; translated to a
/// return tuple `(u32, u8)` which is idiomatic Rust.
pub fn lfu_get_frequency(lfu: u32) -> (u32, u8) {
    let updated = lfu_decay(lfu);
    let freq = updated as u8;
    (updated, freq)
}

// ─── Generic API ──────────────────────────────────────────────────────────────

/// Update the LRU and LFU clocks and record the current eviction policy.
/// Must be called periodically (on each server tick) with the current
/// monotonic millisecond timestamp. Both clocks are derived from `mstime`.
pub fn update_clock_and_policy(mstime: i64, is_policy_lfu: bool) {
    let lru_value = ((mstime / LRU_CLOCK_RESOLUTION as i64) as u32) & LRULFU_MASK;
    let lfu_minutes = (mstime / 60_000) as u16;
    LRU_CLOCK.store(lru_value, Ordering::Relaxed);
    LFU_CLOCK_MINUTES.store(lfu_minutes, Ordering::Relaxed);
    IS_USING_LFU_POLICY.store(is_policy_lfu, Ordering::Relaxed);
}

/// Returns `true` if the server is currently using the LFU eviction policy.
pub fn is_using_lfu() -> bool {
    IS_USING_LFU_POLICY.load(Ordering::Relaxed)
}

/// Create an initial LRU or LFU value for a newly created key.
/// For LFU, the key starts at [`LFU_INIT_VAL`] to avoid immediate eviction.
/// For LRU, the key starts as though it was just accessed (idle seconds = 0).
pub fn lrulfu_init() -> u32 {
    if is_using_lfu() {
        lfu_import(LFU_INIT_VAL)
    } else {
        lru_import(0)
    }
}

/// Compute a relative idleness metric suitable for comparing LRU or LFU values.
/// A larger return value means a greater degree of idleness (higher eviction
/// priority). Applies decay to LFU values as a side effect.
/// Returns `(updated_lrulfu, idleness)`. For LFU, `updated_lrulfu` has decay
/// applied; for LRU, it is returned unchanged.
/// PORT NOTE: C signature uses an out-parameter `uint32_t *idleness`; translated
/// to a return tuple `(u32, u32)`.
pub fn lrulfu_get_idleness(lrulfu: u32) -> (u32, u32) {
    if is_using_lfu() {
        let (updated, freq) = lfu_get_frequency(lrulfu);
        let idleness = u8::MAX as u32 - freq as u32;
        (updated, idleness)
    } else {
        let idleness = lru_get_idle_secs(lrulfu);
        (lrulfu, idleness)
    }
}

/// Add a touch to the LRU or LFU value, returning the updated value.
/// For LFU: applies decay then increments the frequency logarithmically.
/// For LRU: returns a fresh timestamp as though the key was accessed now.
pub fn lrulfu_touch(lrulfu: u32) -> u32 {
    if is_using_lfu() {
        lfu_touch(lrulfu)
    } else {
        lru_import(0)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         3
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:         Logic is a faithful translation. Two TODOs block compilation:
//                  (1) crate::rand::rand_float() does not exist yet (rand.c not
//                  yet translated); (2) clock statics should migrate into
//                  RedisServer. All C out-parameters converted to return tuples.
// ──────────────────────────────────────────────────────────────────────────────
