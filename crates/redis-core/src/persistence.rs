//! Persistence runtime state shared by INFO, command handlers, and reapers.
//! This module intentionally contains only state and small typed enums. RDB
//! codecs stay in `redis-core::rdb`; AOF replay/rewrite stays
//! `redis-commands`, because replay needs the command table.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};

/// C-compatible AOF state discriminant.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofState {
    Off = 0,
    On = 1,
    WaitRewrite = 2,
}

impl AofState {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::On,
            2 => Self::WaitRewrite,
            _ => Self::Off,
        }
    }
}

/// Coarse persistence operation status as rendered by INFO.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceStatus {
    Ok = 0,
    Err = 1,
}

impl PersistenceStatus {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Err,
            _ => Self::Ok,
        }
    }

    pub fn as_info_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Err => "err",
        }
    }
}

/// Runtime persistence stats. All fields are atomics so `INFO persistence`
/// can read them from a shared `Arc<RedisServer>` without acquiring locks.
#[derive(Debug)]
pub struct PersistenceState {
    loading: AtomicBool,
    aof_state: AtomicU8,
    aof_rewrite_in_progress: AtomicBool,
    aof_rewrite_scheduled: AtomicBool,
    rdb_last_bgsave_status: AtomicU8,
    aof_last_bgrewrite_status: AtomicU8,
    aof_last_write_status: AtomicU8,
    aof_current_size: AtomicU64,
    aof_base_size: AtomicU64,
    aof_last_rewrite_snapshot_keys: AtomicU64,
    aof_last_rewrite_snapshot_micros: AtomicU64,
}

impl Default for PersistenceState {
    fn default() -> Self {
        Self {
            loading: AtomicBool::new(false),
            aof_state: AtomicU8::new(AofState::Off as u8),
            aof_rewrite_in_progress: AtomicBool::new(false),
            aof_rewrite_scheduled: AtomicBool::new(false),
            rdb_last_bgsave_status: AtomicU8::new(PersistenceStatus::Ok as u8),
            aof_last_bgrewrite_status: AtomicU8::new(PersistenceStatus::Ok as u8),
            aof_last_write_status: AtomicU8::new(PersistenceStatus::Ok as u8),
            aof_current_size: AtomicU64::new(0),
            aof_base_size: AtomicU64::new(0),
            aof_last_rewrite_snapshot_keys: AtomicU64::new(0),
            aof_last_rewrite_snapshot_micros: AtomicU64::new(0),
        }
    }
}

impl PersistenceState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn loading(&self) -> bool {
        self.loading.load(Ordering::Relaxed)
    }

    pub fn set_loading(&self, value: bool) {
        self.loading.store(value, Ordering::Relaxed);
    }

    pub fn aof_state(&self) -> AofState {
        AofState::from_u8(self.aof_state.load(Ordering::Relaxed))
    }

    pub fn set_aof_state(&self, state: AofState) {
        self.aof_state.store(state as u8, Ordering::Relaxed);
    }

    pub fn aof_rewrite_in_progress(&self) -> bool {
        self.aof_rewrite_in_progress.load(Ordering::Relaxed)
    }

    pub fn set_aof_rewrite_in_progress(&self, value: bool) {
        self.aof_rewrite_in_progress.store(value, Ordering::Relaxed);
    }

    pub fn aof_rewrite_scheduled(&self) -> bool {
        self.aof_rewrite_scheduled.load(Ordering::Relaxed)
    }

    pub fn set_aof_rewrite_scheduled(&self, value: bool) {
        self.aof_rewrite_scheduled.store(value, Ordering::Relaxed);
    }

    pub fn rdb_last_bgsave_status(&self) -> PersistenceStatus {
        PersistenceStatus::from_u8(self.rdb_last_bgsave_status.load(Ordering::Relaxed))
    }

    pub fn set_rdb_last_bgsave_status(&self, status: PersistenceStatus) {
        self.rdb_last_bgsave_status
            .store(status as u8, Ordering::Relaxed);
    }

    pub fn aof_last_bgrewrite_status(&self) -> PersistenceStatus {
        PersistenceStatus::from_u8(self.aof_last_bgrewrite_status.load(Ordering::Relaxed))
    }

    pub fn set_aof_last_bgrewrite_status(&self, status: PersistenceStatus) {
        self.aof_last_bgrewrite_status
            .store(status as u8, Ordering::Relaxed);
    }

    pub fn aof_last_write_status(&self) -> PersistenceStatus {
        PersistenceStatus::from_u8(self.aof_last_write_status.load(Ordering::Relaxed))
    }

    pub fn set_aof_last_write_status(&self, status: PersistenceStatus) {
        self.aof_last_write_status
            .store(status as u8, Ordering::Relaxed);
    }

    pub fn aof_current_size(&self) -> u64 {
        self.aof_current_size.load(Ordering::Relaxed)
    }

    pub fn set_aof_current_size(&self, size: u64) {
        self.aof_current_size.store(size, Ordering::Relaxed);
    }

    pub fn aof_base_size(&self) -> u64 {
        self.aof_base_size.load(Ordering::Relaxed)
    }

    pub fn set_aof_base_size(&self, size: u64) {
        self.aof_base_size.store(size, Ordering::Relaxed);
    }

    pub fn aof_last_rewrite_snapshot_keys(&self) -> u64 {
        self.aof_last_rewrite_snapshot_keys.load(Ordering::Relaxed)
    }

    pub fn aof_last_rewrite_snapshot_micros(&self) -> u64 {
        self.aof_last_rewrite_snapshot_micros
            .load(Ordering::Relaxed)
    }

    pub fn set_aof_last_rewrite_snapshot_stats(&self, keys: u64, micros: u64) {
        self.aof_last_rewrite_snapshot_keys
            .store(keys, Ordering::Relaxed);
        self.aof_last_rewrite_snapshot_micros
            .store(micros, Ordering::Relaxed);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Typed persistence state spine only. AOF lifecycle remains
//                  in redis-commands/redis-server to avoid crate cycles.
// ──────────────────────────────────────────────────────────────────────────
