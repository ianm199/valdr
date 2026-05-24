//! Global LRU clock — coarse seconds-since-startup counter used to score
//! `RedisObject.lru` for the `allkeys-lru` eviction policy.
//!
//! Approximation vs upstream: real Valkey ticks `server.lruclock` at
//! `server.hz` from `serverCron` and stores it in the 24-bit `robj.lru`
//! field. This port ticks once per wall-clock second from
//! [`spawn_lru_clock_thread`] and exposes the value through
//! [`current_lru_clock`]. Wraparound handling is intentionally omitted —
//! a `u32` ticking once per second wraps after ~136 years.
//!
//! Round 16b introduces this module so the eviction sampler in
//! `eviction.rs` has a monotonically-increasing per-object age signal
//! without having to take a `SystemTime::now()` per touch.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use crate::object::LruClock;

static LRU_CLOCK: OnceLock<Arc<AtomicU32>> = OnceLock::new();

/// Install or fetch the global LRU clock. Initialised lazily on first read.
pub fn lru_clock_handle() -> &'static Arc<AtomicU32> {
    LRU_CLOCK.get_or_init(|| Arc::new(AtomicU32::new(0)))
}

/// Read the current LRU clock value.
///
/// Returns `0` before [`spawn_lru_clock_thread`] has run a single tick;
/// callers who use this for ordering should treat the value as opaque
/// other than "larger means more recent".
pub fn current_lru_clock() -> LruClock {
    lru_clock_handle().load(Ordering::Relaxed)
}

/// Atomically bump the LRU clock by one tick. Used by both the background
/// ticker thread and tests that need to advance the clock deterministically.
pub fn tick_lru_clock() -> LruClock {
    lru_clock_handle()
        .fetch_add(1, Ordering::Relaxed)
        .wrapping_add(1)
}

/// Spawn a 1Hz background thread that increments the LRU clock once per
/// second. Returns the join handle so the caller can store it for shutdown.
///
/// The thread runs until the process exits.
pub fn spawn_lru_clock_thread() -> thread::JoinHandle<()> {
    let clock = Arc::clone(lru_clock_handle());
    thread::Builder::new()
        .name("lru-clock".to_string())
        .spawn(move || loop {
            thread::sleep(Duration::from_secs(1));
            clock.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap_or_else(|e| {
            eprintln!("lru-clock: thread spawn failed: {}", e);
            thread::spawn(|| {})
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_advances_the_clock() {
        let before = current_lru_clock();
        tick_lru_clock();
        assert!(current_lru_clock() > before);
    }
}
