//! Keyspace copy-on-write telemetry.
//!
//! These counters are intentionally lightweight and approximate. They make
//! held-snapshot segment clone pressure visible without taking locks or walking
//! every object deeply in the command path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(test)]
use std::sync::{Mutex, MutexGuard};

static ACTIVE_SNAPSHOTS: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_STARTS: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_DROPS: AtomicU64 = AtomicU64::new(0);
static SEGMENT_CLONES: AtomicU64 = AtomicU64::new(0);
static SEGMENT_CLONE_KEYS: AtomicU64 = AtomicU64::new(0);
static SEGMENT_CLONE_ESTIMATED_BYTES: AtomicU64 = AtomicU64::new(0);
static SEGMENT_CLONE_MAX_KEYS: AtomicU64 = AtomicU64::new(0);
static SEGMENT_CLONE_MAX_ESTIMATED_BYTES: AtomicU64 = AtomicU64::new(0);
static SEGMENT_CLONE_MICROS: AtomicU64 = AtomicU64::new(0);
static SEGMENT_CLONE_MAX_MICROS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
static TEST_COUNTER_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeyspaceCowStats {
    pub active_snapshots: u64,
    pub snapshot_starts: u64,
    pub snapshot_drops: u64,
    pub segment_clones: u64,
    pub segment_clone_keys: u64,
    pub segment_clone_estimated_bytes: u64,
    pub segment_clone_max_keys: u64,
    pub segment_clone_max_estimated_bytes: u64,
    pub segment_clone_micros: u64,
    pub segment_clone_max_micros: u64,
}

pub fn stats_snapshot() -> KeyspaceCowStats {
    KeyspaceCowStats {
        active_snapshots: ACTIVE_SNAPSHOTS.load(Ordering::Relaxed),
        snapshot_starts: SNAPSHOT_STARTS.load(Ordering::Relaxed),
        snapshot_drops: SNAPSHOT_DROPS.load(Ordering::Relaxed),
        segment_clones: SEGMENT_CLONES.load(Ordering::Relaxed),
        segment_clone_keys: SEGMENT_CLONE_KEYS.load(Ordering::Relaxed),
        segment_clone_estimated_bytes: SEGMENT_CLONE_ESTIMATED_BYTES.load(Ordering::Relaxed),
        segment_clone_max_keys: SEGMENT_CLONE_MAX_KEYS.load(Ordering::Relaxed),
        segment_clone_max_estimated_bytes: SEGMENT_CLONE_MAX_ESTIMATED_BYTES
            .load(Ordering::Relaxed),
        segment_clone_micros: SEGMENT_CLONE_MICROS.load(Ordering::Relaxed),
        segment_clone_max_micros: SEGMENT_CLONE_MAX_MICROS.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
pub fn reset_for_test() {
    ACTIVE_SNAPSHOTS.store(0, Ordering::Relaxed);
    SNAPSHOT_STARTS.store(0, Ordering::Relaxed);
    SNAPSHOT_DROPS.store(0, Ordering::Relaxed);
    SEGMENT_CLONES.store(0, Ordering::Relaxed);
    SEGMENT_CLONE_KEYS.store(0, Ordering::Relaxed);
    SEGMENT_CLONE_ESTIMATED_BYTES.store(0, Ordering::Relaxed);
    SEGMENT_CLONE_MAX_KEYS.store(0, Ordering::Relaxed);
    SEGMENT_CLONE_MAX_ESTIMATED_BYTES.store(0, Ordering::Relaxed);
    SEGMENT_CLONE_MICROS.store(0, Ordering::Relaxed);
    SEGMENT_CLONE_MAX_MICROS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub fn test_counter_lock() -> MutexGuard<'static, ()> {
    match TEST_COUNTER_LOCK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[derive(Debug)]
pub(crate) struct KeyspaceCowSnapshotGuard;

impl Drop for KeyspaceCowSnapshotGuard {
    fn drop(&mut self) {
        ACTIVE_SNAPSHOTS.fetch_sub(1, Ordering::Relaxed);
        SNAPSHOT_DROPS.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn new_snapshot_guard() -> Arc<KeyspaceCowSnapshotGuard> {
    SNAPSHOT_STARTS.fetch_add(1, Ordering::Relaxed);
    ACTIVE_SNAPSHOTS.fetch_add(1, Ordering::Relaxed);
    Arc::new(KeyspaceCowSnapshotGuard)
}

pub(crate) fn record_segment_clone(keys: usize, estimated_bytes: usize, micros: u64) {
    let keys = keys as u64;
    let estimated_bytes = estimated_bytes as u64;
    SEGMENT_CLONES.fetch_add(1, Ordering::Relaxed);
    SEGMENT_CLONE_KEYS.fetch_add(keys, Ordering::Relaxed);
    SEGMENT_CLONE_ESTIMATED_BYTES.fetch_add(estimated_bytes, Ordering::Relaxed);
    SEGMENT_CLONE_MICROS.fetch_add(micros, Ordering::Relaxed);
    update_max(&SEGMENT_CLONE_MAX_KEYS, keys);
    update_max(&SEGMENT_CLONE_MAX_ESTIMATED_BYTES, estimated_bytes);
    update_max(&SEGMENT_CLONE_MAX_MICROS, micros);
}

fn update_max(slot: &AtomicU64, value: u64) {
    let mut current = slot.load(Ordering::Relaxed);
    while value > current {
        match slot.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}
