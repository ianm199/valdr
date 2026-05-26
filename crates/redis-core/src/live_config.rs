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

use std::path::PathBuf;
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::sync::Mutex;

use redis_types::RedisString;

/// Default `dir` for RDB/AOF files.
pub const DEFAULT_RDB_DIR: &str = "./";
/// Default `dbfilename` for RDB persistence.
pub const DEFAULT_RDB_FILENAME: &str = "dump.rdb";

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
    /// Client-memory eviction limit (`maxmemory-clients`). Positive values are
    /// absolute bytes; negative values are percentages of `maxmemory`.
    pub maxmemory_clients: AtomicI64,
    pub maxclients: AtomicU64,
    pub requirepass: Mutex<Option<RedisString>>,
    /// Password this instance uses when authenticating to its configured
    /// primary (`primaryauth` / legacy `masterauth`).
    pub primaryauth: Mutex<Option<RedisString>>,
    pub notify_keyspace_events_flags: AtomicU32,
    pub slowlog_threshold_micros: AtomicI64,
    pub slowlog_max_len: AtomicUsize,
    /// Cached threshold used by command dispatch to decide whether it needs a
    /// slowlog duration timer. `-1` means slowlog cannot currently record.
    slowlog_timing_threshold_micros: AtomicI64,
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
    /// Sparse HyperLogLog byte limit before promotion to dense.
    pub hll_sparse_max_bytes: AtomicUsize,
    /// Directory where the RDB file is written (`dir` config key).
    pub rdb_dir: Mutex<String>,
    /// Filename for the RDB dump (`dbfilename` config key).
    pub rdb_filename: Mutex<String>,
    /// Whether RDB save rules are configured (`save` config key).
    pub save_enabled: AtomicBool,
    /// Unix timestamp (seconds) of the last successful RDB save.
    pub last_save_unix: AtomicI64,
    /// LFU logarithmic counter growth factor (`lfu-log-factor` config key).
    /// Higher values make the counter saturate more slowly. Default 10.
    pub lfu_log_factor: AtomicU32,
    /// Minutes between LFU counter decay ticks (`lfu-decay-time` config key).
    /// Default 1.
    pub lfu_decay_time: AtomicU32,
    /// TLS listener port (`tls-port` config key). 0 = disabled.
    pub tls_port: AtomicU16,
    /// Path to the PEM certificate chain for the TLS listener.
    pub tls_cert_file: Mutex<Option<PathBuf>>,
    /// Path to the PEM private key for the TLS listener.
    pub tls_key_file: Mutex<Option<PathBuf>>,
    /// Path to the PEM CA bundle used for mTLS client verification.
    pub tls_ca_cert_file: Mutex<Option<PathBuf>>,
    /// mTLS client-auth policy: 0 = no, 1 = yes (require cert), 2 = optional.
    pub tls_auth_clients: AtomicU8,
    /// Whether AOF persistence is enabled (`appendonly` config key).
    pub appendonly: AtomicBool,
    /// AOF filename relative to `rdb_dir` (`appendfilename` config key).
    pub appendfilename: Mutex<String>,
    /// AOF multi-part directory name (`appenddirname` config key).
    pub appenddirname: Mutex<String>,
    /// fsync policy: 0=no, 1=everysec, 2=always (`appendfsync` config key).
    pub appendfsync: AtomicU8,
    /// Whether startup accepts an otherwise-valid AOF truncated at EOF.
    pub aof_load_truncated: AtomicBool,
    /// Whether AOF rewrite may emit an RDB preamble.
    pub aof_use_rdb_preamble: AtomicBool,
    /// Auto AOF rewrite threshold percentage.
    pub auto_aof_rewrite_percentage: AtomicU64,
    /// Auto AOF rewrite minimum size in bytes.
    pub auto_aof_rewrite_min_size: AtomicU64,
    /// Size of the replication backlog circular buffer
    /// (`repl-backlog-size` config key). Default 1 MiB. Reducing it does
    /// not shrink the live buffer until the next `ReplicationState` is
    /// rebuilt; see `replication.rs`.
    pub repl_backlog_size: AtomicU64,
    /// Replica idle-link timeout in seconds (`repl-timeout`). Default 60.
    /// Consumed by Wave B's replica-health watchdog; readback only here.
    pub repl_timeout: AtomicU64,
    /// Minimum number of good replicas required before accepting writes
    /// (`min-replicas-to-write` / `min-slaves-to-write`). Default 0 disables
    /// the check.
    pub repl_min_replicas_to_write: AtomicU64,
    /// Maximum acceptable replica lag in seconds for the min-replicas write
    /// gate (`min-replicas-max-lag` / `min-slaves-max-lag`). Default 10.
    pub repl_min_replicas_max_lag: AtomicU64,
    /// Disable per-write TCP_NODELAY on the replication link
    /// (`repl-disable-tcp-nodelay`). Default `false`. Wave B/C may consume
    /// this once the master-link socket is owned.
    pub repl_disable_tcp_nodelay: AtomicBool,
    /// Replicas may serve read commands (`slave-read-only`/`replica-read-only`).
    /// Default `true`; matches real Redis.
    pub slave_read_only: AtomicBool,
    /// Replicas may serve commands while the master link is down
    /// (`replica-serve-stale-data`/`slave-serve-stale-data`). Default `true`.
    pub replica_serve_stale_data: AtomicBool,
    /// Enable Lua 5.1 APIs that upstream marks insecure/deprecated
    /// (`lua-enable-insecure-api`). Default `false`.
    pub lua_enable_insecure_api: AtomicBool,
    /// Whether the primary is in import mode (`import-mode` config key).
    pub import_mode: AtomicBool,
    /// Optional availability-zone string surfaced by HELLO.
    pub availability_zone: Mutex<String>,
    /// Send the RDB snapshot diskless during full resync
    /// (`repl-diskless-sync`). Default `true`. No-op until Wave B wires the
    /// actual snapshot transfer.
    pub repl_diskless_sync: AtomicBool,
    /// Accept future DUMP/RESTORE payload RDB versions
    /// (`rdb-version-check relaxed`). Default strict.
    pub rdb_version_check_relaxed: AtomicBool,
}

/// Default `maxclients` (matches upstream server.c).
pub const DEFAULT_MAX_CLIENTS: u64 = 10_000;

/// Default slowlog threshold in microseconds.
pub const DEFAULT_SLOWLOG_THRESHOLD_MICROS: i64 = 10_000;

/// Default slowlog ring-buffer capacity.
pub const DEFAULT_SLOWLOG_MAX_LEN: usize = 128;

/// Slowlog timing decision snapshot for one command dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlowlogTimingGate {
    threshold_micros: i64,
}

impl SlowlogTimingGate {
    pub const fn new(threshold_micros: i64) -> Self {
        Self { threshold_micros }
    }

    /// Whether dispatch must capture a command duration for slowlog.
    pub const fn should_time(self) -> bool {
        self.threshold_micros >= 0
    }

    /// Whether a measured command duration should be recorded.
    pub const fn should_record(self, elapsed_micros: u64) -> bool {
        self.threshold_micros >= 0 && elapsed_micros >= self.threshold_micros as u64
    }

    pub const fn threshold_micros(self) -> i64 {
        self.threshold_micros
    }
}

/// Default value of `server.hz` (events per second).
pub const DEFAULT_HZ: u32 = 10;

/// Default `active-expire-effort` (minimum aggressiveness).
pub const DEFAULT_ACTIVE_EXPIRE_EFFORT: u8 = 1;

/// Default `lfu-log-factor` — controls how fast the LFU counter saturates.
pub const DEFAULT_LFU_LOG_FACTOR: u32 = 10;

/// Default `lfu-decay-time` in minutes — how often the LFU counter decays by 1.
pub const DEFAULT_LFU_DECAY_TIME: u32 = 1;

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

/// Default `hll-sparse-max-bytes`.
pub const DEFAULT_HLL_SPARSE_MAX_BYTES: usize = 3000;

/// Default AOF filename.
pub const DEFAULT_AOF_FILENAME: &str = "appendonly.aof";
/// Default AOF multi-part directory name.
pub const DEFAULT_AOF_DIRNAME: &str = "appendonlydir";
/// Default `aof-load-truncated` setting.
pub const DEFAULT_AOF_LOAD_TRUNCATED: bool = true;
/// Default `aof-use-rdb-preamble` setting.
pub const DEFAULT_AOF_USE_RDB_PREAMBLE: bool = true;
/// Default auto AOF rewrite percentage.
pub const DEFAULT_AUTO_AOF_REWRITE_PERCENTAGE: u64 = 100;
/// Default auto AOF rewrite minimum size (64 MiB).
pub const DEFAULT_AUTO_AOF_REWRITE_MIN_SIZE: u64 = 64 * 1024 * 1024;

/// Default `repl-backlog-size` (1 MiB).
pub const DEFAULT_REPL_BACKLOG_SIZE: u64 = 1024 * 1024;

/// Default `repl-timeout` (60 seconds).
pub const DEFAULT_REPL_TIMEOUT: u64 = 60;

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            maxmemory: AtomicU64::new(0),
            maxmemory_policy: AtomicU8::new(MaxmemoryPolicyCode::NoEviction as u8),
            maxmemory_clients: AtomicI64::new(0),
            maxclients: AtomicU64::new(DEFAULT_MAX_CLIENTS),
            requirepass: Mutex::new(None),
            primaryauth: Mutex::new(None),
            notify_keyspace_events_flags: AtomicU32::new(0),
            slowlog_threshold_micros: AtomicI64::new(DEFAULT_SLOWLOG_THRESHOLD_MICROS),
            slowlog_max_len: AtomicUsize::new(DEFAULT_SLOWLOG_MAX_LEN),
            slowlog_timing_threshold_micros: AtomicI64::new(DEFAULT_SLOWLOG_THRESHOLD_MICROS),
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
            hll_sparse_max_bytes: AtomicUsize::new(DEFAULT_HLL_SPARSE_MAX_BYTES),
            rdb_dir: Mutex::new(DEFAULT_RDB_DIR.to_string()),
            rdb_filename: Mutex::new(DEFAULT_RDB_FILENAME.to_string()),
            save_enabled: AtomicBool::new(true),
            last_save_unix: AtomicI64::new(0),
            lfu_log_factor: AtomicU32::new(DEFAULT_LFU_LOG_FACTOR),
            lfu_decay_time: AtomicU32::new(DEFAULT_LFU_DECAY_TIME),
            tls_port: AtomicU16::new(0),
            tls_cert_file: Mutex::new(None),
            tls_key_file: Mutex::new(None),
            tls_ca_cert_file: Mutex::new(None),
            tls_auth_clients: AtomicU8::new(0),
            appendonly: AtomicBool::new(false),
            appendfilename: Mutex::new(DEFAULT_AOF_FILENAME.to_string()),
            appenddirname: Mutex::new(DEFAULT_AOF_DIRNAME.to_string()),
            appendfsync: AtomicU8::new(1),
            aof_load_truncated: AtomicBool::new(DEFAULT_AOF_LOAD_TRUNCATED),
            aof_use_rdb_preamble: AtomicBool::new(DEFAULT_AOF_USE_RDB_PREAMBLE),
            auto_aof_rewrite_percentage: AtomicU64::new(DEFAULT_AUTO_AOF_REWRITE_PERCENTAGE),
            auto_aof_rewrite_min_size: AtomicU64::new(DEFAULT_AUTO_AOF_REWRITE_MIN_SIZE),
            repl_backlog_size: AtomicU64::new(DEFAULT_REPL_BACKLOG_SIZE),
            repl_timeout: AtomicU64::new(DEFAULT_REPL_TIMEOUT),
            repl_min_replicas_to_write: AtomicU64::new(0),
            repl_min_replicas_max_lag: AtomicU64::new(10),
            repl_disable_tcp_nodelay: AtomicBool::new(false),
            slave_read_only: AtomicBool::new(true),
            replica_serve_stale_data: AtomicBool::new(true),
            lua_enable_insecure_api: AtomicBool::new(false),
            import_mode: AtomicBool::new(false),
            availability_zone: Mutex::new(String::new()),
            repl_diskless_sync: AtomicBool::new(true),
            rdb_version_check_relaxed: AtomicBool::new(false),
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

    pub fn maxmemory_clients(&self) -> i64 {
        self.maxmemory_clients.load(Ordering::Relaxed)
    }

    pub fn set_maxmemory_clients(&self, bytes_or_negative_percent: i64) {
        self.maxmemory_clients
            .store(bytes_or_negative_percent, Ordering::Relaxed);
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

    /// Snapshot the password used for AUTH during replica handshakes.
    pub fn primaryauth(&self) -> Option<RedisString> {
        match self.primaryauth.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Update the primary authentication secret. Empty disables it.
    pub fn set_primaryauth(&self, secret: Option<RedisString>) {
        let value = match secret {
            Some(s) if s.as_bytes().is_empty() => None,
            other => other,
        };
        match self.primaryauth.lock() {
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
        self.refresh_slowlog_timing_gate();
    }

    pub fn slowlog_max_len(&self) -> usize {
        self.slowlog_max_len.load(Ordering::Relaxed)
    }

    pub fn set_slowlog_max_len(&self, max_len: usize) {
        self.slowlog_max_len.store(max_len, Ordering::Relaxed);
        self.refresh_slowlog_timing_gate();
    }

    pub fn slowlog_timing_gate(&self) -> SlowlogTimingGate {
        SlowlogTimingGate::new(self.slowlog_timing_threshold_micros.load(Ordering::Relaxed))
    }

    fn refresh_slowlog_timing_gate(&self) {
        let threshold = self.slowlog_threshold_micros.load(Ordering::Relaxed);
        let max_len = self.slowlog_max_len.load(Ordering::Relaxed);
        let timing_threshold = if threshold >= 0 && max_len > 0 {
            threshold
        } else {
            -1
        };
        self.slowlog_timing_threshold_micros
            .store(timing_threshold, Ordering::Relaxed);
    }

    pub fn active_expire_effort(&self) -> u8 {
        self.active_expire_effort.load(Ordering::Relaxed)
    }

    pub fn set_active_expire_effort(&self, effort: u8) {
        let clamped = effort.min(10);
        self.active_expire_effort.store(clamped, Ordering::Relaxed);
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

    pub fn hll_sparse_max_bytes(&self) -> usize {
        self.hll_sparse_max_bytes.load(Ordering::Relaxed)
    }

    pub fn set_hll_sparse_max_bytes(&self, n: usize) {
        self.hll_sparse_max_bytes.store(n, Ordering::Relaxed);
    }

    /// Return the current `dir` setting for RDB/AOF files.
    pub fn rdb_dir(&self) -> String {
        match self.rdb_dir.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Update the `dir` setting.
    pub fn set_rdb_dir(&self, dir: String) {
        match self.rdb_dir.lock() {
            Ok(mut g) => *g = dir,
            Err(p) => *p.into_inner() = dir,
        }
    }

    /// Return the current `dbfilename` setting.
    pub fn rdb_filename(&self) -> String {
        match self.rdb_filename.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Update the `dbfilename` setting.
    pub fn set_rdb_filename(&self, name: String) {
        match self.rdb_filename.lock() {
            Ok(mut g) => *g = name,
            Err(p) => *p.into_inner() = name,
        }
    }

    pub fn save_enabled(&self) -> bool {
        self.save_enabled.load(Ordering::Relaxed)
    }

    pub fn set_save_enabled(&self, enabled: bool) {
        self.save_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Return the Unix timestamp (seconds) of the last successful RDB save, or
    /// 0 if no save has occurred this session.
    pub fn last_save_unix(&self) -> i64 {
        self.last_save_unix.load(Ordering::Relaxed)
    }

    /// Record the timestamp of a successful RDB save.
    pub fn set_last_save_unix(&self, ts: i64) {
        self.last_save_unix.store(ts, Ordering::Relaxed);
    }

    pub fn lfu_log_factor(&self) -> u32 {
        self.lfu_log_factor.load(Ordering::Relaxed)
    }

    pub fn set_lfu_log_factor(&self, factor: u32) {
        self.lfu_log_factor.store(factor, Ordering::Relaxed);
    }

    pub fn lfu_decay_time(&self) -> u32 {
        self.lfu_decay_time.load(Ordering::Relaxed)
    }

    pub fn set_lfu_decay_time(&self, minutes: u32) {
        self.lfu_decay_time.store(minutes, Ordering::Relaxed);
    }

    /// Current TLS listener port. Returns 0 when TLS is disabled.
    pub fn tls_port(&self) -> u16 {
        self.tls_port.load(Ordering::Relaxed)
    }

    /// Update the TLS listener port. 0 disables TLS.
    pub fn set_tls_port(&self, port: u16) {
        self.tls_port.store(port, Ordering::Relaxed);
    }

    /// Snapshot the TLS certificate file path.
    pub fn tls_cert_file(&self) -> Option<PathBuf> {
        match self.tls_cert_file.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Update the TLS certificate file path.
    pub fn set_tls_cert_file(&self, path: Option<PathBuf>) {
        match self.tls_cert_file.lock() {
            Ok(mut g) => *g = path,
            Err(p) => *p.into_inner() = path,
        }
    }

    /// Snapshot the TLS private key file path.
    pub fn tls_key_file(&self) -> Option<PathBuf> {
        match self.tls_key_file.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Update the TLS private key file path.
    pub fn set_tls_key_file(&self, path: Option<PathBuf>) {
        match self.tls_key_file.lock() {
            Ok(mut g) => *g = path,
            Err(p) => *p.into_inner() = path,
        }
    }

    /// Snapshot the TLS CA certificate file path used for mTLS.
    pub fn tls_ca_cert_file(&self) -> Option<PathBuf> {
        match self.tls_ca_cert_file.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Update the TLS CA certificate file path.
    pub fn set_tls_ca_cert_file(&self, path: Option<PathBuf>) {
        match self.tls_ca_cert_file.lock() {
            Ok(mut g) => *g = path,
            Err(p) => *p.into_inner() = path,
        }
    }

    /// mTLS client-auth policy: 0 = no (default), 1 = yes (require cert),
    /// 2 = optional.
    pub fn tls_auth_clients(&self) -> u8 {
        self.tls_auth_clients.load(Ordering::Relaxed)
    }

    /// Update the mTLS client-auth policy.
    pub fn set_tls_auth_clients(&self, mode: u8) {
        self.tls_auth_clients.store(mode, Ordering::Relaxed);
    }

    /// Whether AOF persistence is currently enabled.
    pub fn appendonly(&self) -> bool {
        self.appendonly.load(Ordering::Relaxed)
    }

    /// Enable or disable AOF persistence.
    pub fn set_appendonly(&self, enabled: bool) {
        self.appendonly.store(enabled, Ordering::Relaxed);
    }

    /// Return the current AOF filename.
    pub fn appendfilename(&self) -> String {
        match self.appendfilename.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Update the AOF filename.
    pub fn set_appendfilename(&self, name: String) {
        match self.appendfilename.lock() {
            Ok(mut g) => *g = name,
            Err(p) => *p.into_inner() = name,
        }
    }

    /// Return the current AOF directory name.
    pub fn appenddirname(&self) -> String {
        match self.appenddirname.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Update the AOF directory name.
    pub fn set_appenddirname(&self, name: String) {
        match self.appenddirname.lock() {
            Ok(mut g) => *g = name,
            Err(p) => *p.into_inner() = name,
        }
    }

    /// Return the fsync policy code (0=no, 1=everysec, 2=always).
    pub fn appendfsync(&self) -> u8 {
        self.appendfsync.load(Ordering::Relaxed)
    }

    /// Update the fsync policy.
    pub fn set_appendfsync(&self, policy: u8) {
        self.appendfsync.store(policy, Ordering::Relaxed);
    }

    pub fn aof_load_truncated(&self) -> bool {
        self.aof_load_truncated.load(Ordering::Relaxed)
    }

    pub fn set_aof_load_truncated(&self, enabled: bool) {
        self.aof_load_truncated.store(enabled, Ordering::Relaxed);
    }

    pub fn aof_use_rdb_preamble(&self) -> bool {
        self.aof_use_rdb_preamble.load(Ordering::Relaxed)
    }

    pub fn set_aof_use_rdb_preamble(&self, enabled: bool) {
        self.aof_use_rdb_preamble.store(enabled, Ordering::Relaxed);
    }

    pub fn auto_aof_rewrite_percentage(&self) -> u64 {
        self.auto_aof_rewrite_percentage.load(Ordering::Relaxed)
    }

    pub fn set_auto_aof_rewrite_percentage(&self, value: u64) {
        self.auto_aof_rewrite_percentage
            .store(value, Ordering::Relaxed);
    }

    pub fn auto_aof_rewrite_min_size(&self) -> u64 {
        self.auto_aof_rewrite_min_size.load(Ordering::Relaxed)
    }

    pub fn set_auto_aof_rewrite_min_size(&self, value: u64) {
        self.auto_aof_rewrite_min_size
            .store(value, Ordering::Relaxed);
    }

    /// Configured replication backlog size in bytes (`repl-backlog-size`).
    pub fn repl_backlog_size(&self) -> u64 {
        self.repl_backlog_size.load(Ordering::Relaxed)
    }

    /// Update the configured replication backlog size. Note that the live
    /// backlog is not resized in place; consumers consult this for new
    /// allocations only.
    pub fn set_repl_backlog_size(&self, n: u64) {
        self.repl_backlog_size.store(n, Ordering::Relaxed);
    }

    /// Replica idle-link timeout in seconds (`repl-timeout`).
    pub fn repl_timeout(&self) -> u64 {
        self.repl_timeout.load(Ordering::Relaxed)
    }

    /// Update the replica idle-link timeout.
    pub fn set_repl_timeout(&self, n: u64) {
        self.repl_timeout.store(n, Ordering::Relaxed);
    }

    pub fn repl_min_replicas_to_write(&self) -> u64 {
        self.repl_min_replicas_to_write.load(Ordering::Relaxed)
    }

    pub fn set_repl_min_replicas_to_write(&self, n: u64) {
        self.repl_min_replicas_to_write.store(n, Ordering::Relaxed);
    }

    pub fn repl_min_replicas_max_lag(&self) -> u64 {
        self.repl_min_replicas_max_lag.load(Ordering::Relaxed)
    }

    pub fn set_repl_min_replicas_max_lag(&self, n: u64) {
        self.repl_min_replicas_max_lag.store(n, Ordering::Relaxed);
    }

    /// Whether the replication link disables per-write TCP_NODELAY
    /// (`repl-disable-tcp-nodelay`).
    pub fn repl_disable_tcp_nodelay(&self) -> bool {
        self.repl_disable_tcp_nodelay.load(Ordering::Relaxed)
    }

    /// Update the repl-disable-tcp-nodelay flag.
    pub fn set_repl_disable_tcp_nodelay(&self, v: bool) {
        self.repl_disable_tcp_nodelay.store(v, Ordering::Relaxed);
    }

    /// Whether replicas may serve read commands (`slave-read-only`/
    /// `replica-read-only`).
    pub fn slave_read_only(&self) -> bool {
        self.slave_read_only.load(Ordering::Relaxed)
    }

    /// Update the slave-read-only flag.
    pub fn set_slave_read_only(&self, v: bool) {
        self.slave_read_only.store(v, Ordering::Relaxed);
    }

    pub fn replica_serve_stale_data(&self) -> bool {
        self.replica_serve_stale_data.load(Ordering::Relaxed)
    }

    pub fn set_replica_serve_stale_data(&self, v: bool) {
        self.replica_serve_stale_data.store(v, Ordering::Relaxed);
    }

    pub fn lua_enable_insecure_api(&self) -> bool {
        self.lua_enable_insecure_api.load(Ordering::Relaxed)
    }

    pub fn set_lua_enable_insecure_api(&self, v: bool) {
        self.lua_enable_insecure_api.store(v, Ordering::Relaxed);
    }

    pub fn import_mode(&self) -> bool {
        self.import_mode.load(Ordering::Relaxed)
    }

    pub fn set_import_mode(&self, v: bool) {
        self.import_mode.store(v, Ordering::Relaxed);
    }

    pub fn availability_zone(&self) -> String {
        match self.availability_zone.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    pub fn set_availability_zone(&self, zone: String) {
        match self.availability_zone.lock() {
            Ok(mut g) => *g = zone,
            Err(p) => *p.into_inner() = zone,
        }
    }

    /// Whether full-resync RDB transfer uses the diskless path
    /// (`repl-diskless-sync`).
    pub fn repl_diskless_sync(&self) -> bool {
        self.repl_diskless_sync.load(Ordering::Relaxed)
    }

    /// Update the repl-diskless-sync flag.
    pub fn set_repl_diskless_sync(&self, v: bool) {
        self.repl_diskless_sync.store(v, Ordering::Relaxed);
    }

    /// Whether RESTORE accepts future DUMP payload RDB versions.
    pub fn rdb_version_check_relaxed(&self) -> bool {
        self.rdb_version_check_relaxed.load(Ordering::Relaxed)
    }

    /// Update `rdb-version-check`: false = strict, true = relaxed.
    pub fn set_rdb_version_check_relaxed(&self, relaxed: bool) {
        self.rdb_version_check_relaxed
            .store(relaxed, Ordering::Relaxed);
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
        assert_eq!(cfg.hll_sparse_max_bytes(), DEFAULT_HLL_SPARSE_MAX_BYTES);
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

    #[test]
    fn slowlog_timing_gate_tracks_threshold_and_capacity() {
        let cfg = LiveConfig::new();
        assert_eq!(
            cfg.slowlog_timing_gate().threshold_micros(),
            DEFAULT_SLOWLOG_THRESHOLD_MICROS
        );
        assert!(cfg.slowlog_timing_gate().should_time());
        assert!(!cfg.slowlog_timing_gate().should_record(1));

        cfg.set_slowlog_max_len(0);
        assert!(!cfg.slowlog_timing_gate().should_time());

        cfg.set_slowlog_max_len(64);
        cfg.set_slowlog_threshold_micros(-1);
        assert!(!cfg.slowlog_timing_gate().should_time());

        cfg.set_slowlog_threshold_micros(0);
        assert!(cfg.slowlog_timing_gate().should_time());
        assert!(cfg.slowlog_timing_gate().should_record(0));
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        runtime config spine (CONFIG SET live-state support)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         LiveConfig atomics and snapshots for command/background
//                  runtime config reads, including HLL sparse promotion.
// ──────────────────────────────────────────────────────────────────────────
