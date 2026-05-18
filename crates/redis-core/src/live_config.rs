//! `LiveConfig` — server-wide configuration state with per-field atomic reads.
//!
//! Single source of truth for every behavioural config knob that command
//! handlers and background threads need to consult at runtime. The struct
//! lives behind `Arc<LiveConfig>` on `RedisServer` so threads can read
//! lock-free atomics in hot paths and clone the `Arc` for owned snapshots.
//!
//! Adding a new live-config key:
//!   1. Add the field here (per-field atomic preferred; `Mutex<T>` only when
//!      the value is variable-length, e.g. `requirepass`).
//!   2. Wire the parser into `apply_config_set` in
//!      `redis-commands::connection::config`.
//!   3. Surface the readback in `config_pairs_with_dynamic`.
//!   4. Update the relevant hot-path reader (eviction, INFO, encoding
//!      heuristics, etc.) to read from `LiveConfig` rather than a shadow
//!      global.
//!
//! Round 15a established this as the spine that Round 15b+ (CONFIG SET
//! hooks, AUTH, keyspace-notifications wiring, maxmemory eviction) will
//! ride on.

use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Mutex;

use redis_types::RedisString;

/// Eviction policy discriminant matching the `MaxmemoryPolicy` enum in
/// `evict.rs`. Stored as `u8` inside `AtomicU8` so reads are lock-free.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxmemoryPolicyCode {
    NoEviction = 0,
    AllkeysLru = 1,
    AllkeysLfu = 2,
    AllkeysRandom = 3,
    VolatileLru = 4,
    VolatileLfu = 5,
    VolatileRandom = 6,
    VolatileTtl = 7,
}

impl MaxmemoryPolicyCode {
    /// Parse the canonical config-string form (`noeviction`, `allkeys-lru`, …)
    /// into a discriminant. Returns `None` for unknown spellings; callers should
    /// surface that as a config-syntax error rather than silently defaulting.
    pub fn parse(name: &[u8]) -> Option<Self> {
        match name {
            b"noeviction" => Some(Self::NoEviction),
            b"allkeys-lru" => Some(Self::AllkeysLru),
            b"allkeys-lfu" => Some(Self::AllkeysLfu),
            b"allkeys-random" => Some(Self::AllkeysRandom),
            b"volatile-lru" => Some(Self::VolatileLru),
            b"volatile-lfu" => Some(Self::VolatileLfu),
            b"volatile-random" => Some(Self::VolatileRandom),
            b"volatile-ttl" => Some(Self::VolatileTtl),
            _ => None,
        }
    }

    /// Inverse of `parse`: render the discriminant as the canonical
    /// config-string spelling for CONFIG GET readback.
    pub fn as_config_str(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllkeysLru => "allkeys-lru",
            Self::AllkeysLfu => "allkeys-lfu",
            Self::AllkeysRandom => "allkeys-random",
            Self::VolatileLru => "volatile-lru",
            Self::VolatileLfu => "volatile-lfu",
            Self::VolatileRandom => "volatile-random",
            Self::VolatileTtl => "volatile-ttl",
        }
    }

    /// Reconstruct from the wire-stored discriminant.
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::AllkeysLru,
            2 => Self::AllkeysLfu,
            3 => Self::AllkeysRandom,
            4 => Self::VolatileLru,
            5 => Self::VolatileLfu,
            6 => Self::VolatileRandom,
            7 => Self::VolatileTtl,
            _ => Self::NoEviction,
        }
    }
}

/// Server-wide live configuration.
///
/// All numeric fields are per-field atomics so reads are lock-free in hot
/// paths. The `requirepass` field is variable-length (a `RedisString`) so it
/// uses a `Mutex` for the rare CONFIG SET write path.
pub struct LiveConfig {
    pub maxmemory: AtomicU64,
    pub maxmemory_policy: AtomicU8,
    pub maxclients: AtomicU64,
    pub requirepass: Mutex<Option<RedisString>>,
    pub notify_keyspace_events_flags: AtomicU32,
    pub slowlog_threshold_micros: AtomicI64,
    pub slowlog_max_len: AtomicUsize,
    pub active_expire_effort: AtomicU8,
    pub hz: AtomicU32,
    pub hash_max_listpack_entries: AtomicUsize,
    pub hash_max_listpack_value: AtomicUsize,
    pub list_max_listpack_size: AtomicI64,
    pub set_max_intset_entries: AtomicUsize,
    pub set_max_listpack_entries: AtomicUsize,
    pub set_max_listpack_value: AtomicUsize,
    pub zset_max_listpack_entries: AtomicUsize,
    pub zset_max_listpack_value: AtomicUsize,
}

/// Default `maxclients` (matches upstream server.c).
pub const DEFAULT_MAX_CLIENTS: u64 = 10_000;

/// Default slowlog threshold in microseconds.
pub const DEFAULT_SLOWLOG_THRESHOLD_MICROS: i64 = 10_000;

/// Default slowlog ring-buffer capacity.
pub const DEFAULT_SLOWLOG_MAX_LEN: usize = 128;

/// Default value of `server.hz` (events per second).
pub const DEFAULT_HZ: u32 = 10;

/// Default `active-expire-effort` (minimum aggressiveness).
pub const DEFAULT_ACTIVE_EXPIRE_EFFORT: u8 = 1;

/// Default `hash-max-listpack-entries` per Valkey config.
pub const DEFAULT_HASH_MAX_LISTPACK_ENTRIES: usize = 128;

/// Default `hash-max-listpack-value`.
pub const DEFAULT_HASH_MAX_LISTPACK_VALUE: usize = 64;

/// Default `list-max-listpack-size` (-2 = 8 KiB per node in upstream).
pub const DEFAULT_LIST_MAX_LISTPACK_SIZE: i64 = -2;

/// Default `set-max-intset-entries`.
pub const DEFAULT_SET_MAX_INTSET_ENTRIES: usize = 512;

/// Default `set-max-listpack-entries`.
pub const DEFAULT_SET_MAX_LISTPACK_ENTRIES: usize = 128;

/// Default `set-max-listpack-value`.
pub const DEFAULT_SET_MAX_LISTPACK_VALUE: usize = 64;

/// Default `zset-max-listpack-entries`.
pub const DEFAULT_ZSET_MAX_LISTPACK_ENTRIES: usize = 128;

/// Default `zset-max-listpack-value`.
pub const DEFAULT_ZSET_MAX_LISTPACK_VALUE: usize = 64;

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            maxmemory: AtomicU64::new(0),
            maxmemory_policy: AtomicU8::new(MaxmemoryPolicyCode::NoEviction as u8),
            maxclients: AtomicU64::new(DEFAULT_MAX_CLIENTS),
            requirepass: Mutex::new(None),
            notify_keyspace_events_flags: AtomicU32::new(0),
            slowlog_threshold_micros: AtomicI64::new(DEFAULT_SLOWLOG_THRESHOLD_MICROS),
            slowlog_max_len: AtomicUsize::new(DEFAULT_SLOWLOG_MAX_LEN),
            active_expire_effort: AtomicU8::new(DEFAULT_ACTIVE_EXPIRE_EFFORT),
            hz: AtomicU32::new(DEFAULT_HZ),
            hash_max_listpack_entries: AtomicUsize::new(DEFAULT_HASH_MAX_LISTPACK_ENTRIES),
            hash_max_listpack_value: AtomicUsize::new(DEFAULT_HASH_MAX_LISTPACK_VALUE),
            list_max_listpack_size: AtomicI64::new(DEFAULT_LIST_MAX_LISTPACK_SIZE),
            set_max_intset_entries: AtomicUsize::new(DEFAULT_SET_MAX_INTSET_ENTRIES),
            set_max_listpack_entries: AtomicUsize::new(DEFAULT_SET_MAX_LISTPACK_ENTRIES),
            set_max_listpack_value: AtomicUsize::new(DEFAULT_SET_MAX_LISTPACK_VALUE),
            zset_max_listpack_entries: AtomicUsize::new(DEFAULT_HASH_MAX_LISTPACK_ENTRIES),
            zset_max_listpack_value: AtomicUsize::new(DEFAULT_ZSET_MAX_LISTPACK_VALUE),
        }
    }
}

impl LiveConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn maxmemory(&self) -> u64 {
        self.maxmemory.load(Ordering::Relaxed)
    }

    pub fn set_maxmemory(&self, bytes: u64) {
        self.maxmemory.store(bytes, Ordering::Relaxed);
    }

    pub fn maxmemory_policy(&self) -> MaxmemoryPolicyCode {
        MaxmemoryPolicyCode::from_u8(self.maxmemory_policy.load(Ordering::Relaxed))
    }

    pub fn set_maxmemory_policy(&self, policy: MaxmemoryPolicyCode) {
        self.maxmemory_policy.store(policy as u8, Ordering::Relaxed);
    }

    pub fn maxclients(&self) -> u64 {
        self.maxclients.load(Ordering::Relaxed)
    }

    pub fn set_maxclients(&self, n: u64) {
        self.maxclients.store(n, Ordering::Relaxed);
    }

    /// Snapshot the requirepass secret (cloned out so the lock is released).
    ///
    /// Returns `None` when no password is configured; otherwise the bytes set
    /// by the most recent `CONFIG SET requirepass ...`.
    pub fn requirepass(&self) -> Option<RedisString> {
        match self.requirepass.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Update the requirepass secret. Pass `None` (or an empty `RedisString`)
    /// to disable authentication.
    pub fn set_requirepass(&self, secret: Option<RedisString>) {
        let value = match secret {
            Some(s) if s.as_bytes().is_empty() => None,
            other => other,
        };
        match self.requirepass.lock() {
            Ok(mut g) => *g = value,
            Err(p) => *p.into_inner() = value,
        }
    }

    pub fn notify_keyspace_events_flags(&self) -> u32 {
        self.notify_keyspace_events_flags.load(Ordering::Relaxed)
    }

    pub fn set_notify_keyspace_events_flags(&self, flags: u32) {
        self.notify_keyspace_events_flags
            .store(flags, Ordering::Relaxed);
    }

    pub fn slowlog_threshold_micros(&self) -> i64 {
        self.slowlog_threshold_micros.load(Ordering::Relaxed)
    }

    pub fn set_slowlog_threshold_micros(&self, micros: i64) {
        self.slowlog_threshold_micros
            .store(micros, Ordering::Relaxed);
    }

    pub fn slowlog_max_len(&self) -> usize {
        self.slowlog_max_len.load(Ordering::Relaxed)
    }

    pub fn set_slowlog_max_len(&self, max_len: usize) {
        self.slowlog_max_len.store(max_len, Ordering::Relaxed);
    }

    pub fn active_expire_effort(&self) -> u8 {
        self.active_expire_effort.load(Ordering::Relaxed)
    }

    pub fn set_active_expire_effort(&self, effort: u8) {
        let clamped = effort.min(10);
        self.active_expire_effort
            .store(clamped, Ordering::Relaxed);
    }

    pub fn hz(&self) -> u32 {
        self.hz.load(Ordering::Relaxed)
    }

    pub fn set_hz(&self, hz: u32) {
        let clamped = hz.clamp(1, 500);
        self.hz.store(clamped, Ordering::Relaxed);
    }

    pub fn hash_max_listpack_entries(&self) -> usize {
        self.hash_max_listpack_entries.load(Ordering::Relaxed)
    }

    pub fn set_hash_max_listpack_entries(&self, n: usize) {
        self.hash_max_listpack_entries.store(n, Ordering::Relaxed);
    }

    pub fn hash_max_listpack_value(&self) -> usize {
        self.hash_max_listpack_value.load(Ordering::Relaxed)
    }

    pub fn set_hash_max_listpack_value(&self, n: usize) {
        self.hash_max_listpack_value.store(n, Ordering::Relaxed);
    }

    pub fn list_max_listpack_size(&self) -> i64 {
        self.list_max_listpack_size.load(Ordering::Relaxed)
    }

    pub fn set_list_max_listpack_size(&self, n: i64) {
        self.list_max_listpack_size.store(n, Ordering::Relaxed);
    }

    pub fn set_max_intset_entries(&self) -> usize {
        self.set_max_intset_entries.load(Ordering::Relaxed)
    }

    pub fn store_set_max_intset_entries(&self, n: usize) {
        self.set_max_intset_entries.store(n, Ordering::Relaxed);
    }

    pub fn set_max_listpack_entries(&self) -> usize {
        self.set_max_listpack_entries.load(Ordering::Relaxed)
    }

    pub fn store_set_max_listpack_entries(&self, n: usize) {
        self.set_max_listpack_entries.store(n, Ordering::Relaxed);
    }

    pub fn set_max_listpack_value(&self) -> usize {
        self.set_max_listpack_value.load(Ordering::Relaxed)
    }

    pub fn store_set_max_listpack_value(&self, n: usize) {
        self.set_max_listpack_value.store(n, Ordering::Relaxed);
    }

    pub fn zset_max_listpack_entries(&self) -> usize {
        self.zset_max_listpack_entries.load(Ordering::Relaxed)
    }

    pub fn set_zset_max_listpack_entries(&self, n: usize) {
        self.zset_max_listpack_entries.store(n, Ordering::Relaxed);
    }

    pub fn zset_max_listpack_value(&self) -> usize {
        self.zset_max_listpack_value.load(Ordering::Relaxed)
    }

    pub fn set_zset_max_listpack_value(&self, n: usize) {
        self.zset_max_listpack_value.store(n, Ordering::Relaxed);
    }

    /// Snapshot of encoding thresholds — convenience for the encoding
    /// heuristics in `object.rs` that historically held a single struct.
    pub fn encoding_thresholds(&self) -> EncodingThresholdsSnapshot {
        EncodingThresholdsSnapshot {
            hash_max_listpack_entries: self.hash_max_listpack_entries(),
            hash_max_listpack_value: self.hash_max_listpack_value(),
            list_max_listpack_size: self.list_max_listpack_size(),
            set_max_intset_entries: self.set_max_intset_entries(),
            set_max_listpack_entries: self.set_max_listpack_entries(),
            set_max_listpack_value: self.set_max_listpack_value(),
            zset_max_listpack_entries: self.zset_max_listpack_entries(),
            zset_max_listpack_value: self.zset_max_listpack_value(),
        }
    }
}

/// Owned snapshot of every encoding-threshold field at one instant.
///
/// Cheap-to-copy struct returned from [`LiveConfig::encoding_thresholds`] so
/// `object.rs` heuristics can do a single batched read rather than one atomic
/// load per check.
#[derive(Debug, Clone, Copy)]
pub struct EncodingThresholdsSnapshot {
    pub hash_max_listpack_entries: usize,
    pub hash_max_listpack_value: usize,
    pub list_max_listpack_size: i64,
    pub set_max_intset_entries: usize,
    pub set_max_listpack_entries: usize,
    pub set_max_listpack_value: usize,
    pub zset_max_listpack_entries: usize,
    pub zset_max_listpack_value: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_documented_values() {
        let cfg = LiveConfig::new();
        assert_eq!(cfg.maxmemory(), 0);
        assert_eq!(cfg.maxmemory_policy(), MaxmemoryPolicyCode::NoEviction);
        assert_eq!(cfg.maxclients(), DEFAULT_MAX_CLIENTS);
        assert_eq!(cfg.notify_keyspace_events_flags(), 0);
        assert!(cfg.requirepass().is_none());
        assert_eq!(
            cfg.hash_max_listpack_entries(),
            DEFAULT_HASH_MAX_LISTPACK_ENTRIES
        );
    }

    #[test]
    fn maxmemory_policy_roundtrips_through_atomic() {
        let cfg = LiveConfig::new();
        cfg.set_maxmemory_policy(MaxmemoryPolicyCode::AllkeysLru);
        assert_eq!(cfg.maxmemory_policy(), MaxmemoryPolicyCode::AllkeysLru);
    }

    #[test]
    fn requirepass_set_empty_string_clears_secret() {
        let cfg = LiveConfig::new();
        cfg.set_requirepass(Some(RedisString::from_bytes(b"foo")));
        assert_eq!(
            cfg.requirepass().map(|s| s.as_bytes().to_vec()),
            Some(b"foo".to_vec())
        );
        cfg.set_requirepass(Some(RedisString::new()));
        assert!(cfg.requirepass().is_none());
    }
}
