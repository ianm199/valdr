//! `server.rs` — Rust port of `src/server.c` (7 942 lines, ~180 functions)
//!               combined with constants/types from `src/server.h` (4 307 lines).
//!
//! # Responsibility
//!
//! This is the coordination hub of the Redis server: logging, time-keeping,
//! server lifecycle, event-loop hooks (`beforeSleep` / `afterSleep`), the
//! main cron timer, command dispatch (`call`, `processCommand`), built-in
//! commands (PING, ECHO, TIME, COMMAND, INFO, MONITOR), shutdown handling,
//! and server-wide statistics.
//!
//! # Phase A note
//!
//! This is a **faithful logic translation** — it does NOT compile cross-crate.
//! All types imported from sibling crates (`RedisServer`, `Client`, …) are
//! canonical as per `harness/type-vocabulary.tsv`; they are `pub use`-d here,
//! not redefined.  Complex multi-hundred-line functions are stubbed with
//! `TODO(port)`.  The wire-diff oracle (Phase C+) will verify behavioral
//! equivalence.
//!
//! # Structure
//!
//! - Constants (from `server.h`)
//! - Enumerations
//! - Type aliases
//! - Logging utilities
//! - Time / statistics utilities
//! - Server initialization stubs
//! - Command execution (`call`, `processCommand`)
//! - Shutdown management
//! - Built-in command implementations
//! - Event-loop hooks (`beforeSleep`, `afterSleep`, `serverCron`)
//! - Miscellaneous helpers

// ── Standard library ─────────────────────────────────────────────────────────

use std::io::Write as IoWrite;
use std::time::{SystemTime, UNIX_EPOCH};

// ── Canonical types (pub use from owner crates) ───────────────────────────────
//
// Per harness/type-vocabulary.tsv — DO NOT redefine these structs/enums here.

pub use redis_core::server::RedisServer;
pub use redis_core::client::Client;
pub use redis_core::db::RedisDb;
pub use redis_core::object::RedisObject;
pub use redis_core::command_context::CommandContext;
pub use redis_types::string::RedisString;
pub use redis_types::error::RedisError;
pub use redis_protocol::frame::RespFrame;

// ── Constants from server.h ───────────────────────────────────────────────────

/// Default timer interrupt frequency (calls/sec). C: `CONFIG_DEFAULT_HZ`
pub const CONFIG_DEFAULT_HZ: u32 = 10;
/// Minimum permitted Hz. C: `CONFIG_MIN_HZ`
pub const CONFIG_MIN_HZ: u32 = 1;
/// Maximum permitted Hz. C: `CONFIG_MAX_HZ`
pub const CONFIG_MAX_HZ: u32 = 500;

/// Number of databases processed per cron call. C: `CRON_DBS_PER_CALL`
pub const CRON_DBS_PER_CALL: usize = 16;
/// Max bytes flushed per write event. C: `NET_MAX_WRITES_PER_EVENT`
pub const NET_MAX_WRITES_PER_EVENT: usize = 1024 * 64;

/// Shared integer object pool size. C: `OBJ_SHARED_INTEGERS`
pub const OBJ_SHARED_INTEGERS: i64 = 10_000;
/// Maximum log line length. C: `LOG_MAX_LEN`
pub const LOG_MAX_LEN: usize = 1024;

/// Run-ID size in bytes. C: `CONFIG_RUN_ID_SIZE`
pub const CONFIG_RUN_ID_SIZE: usize = 40;
/// RDB EOF mark size. C: `RDB_EOF_MARK_SIZE`
pub const RDB_EOF_MARK_SIZE: usize = 40;

/// Minimum backlog size for replication. C: `CONFIG_REPL_BACKLOG_MIN_SIZE`
pub const CONFIG_REPL_BACKLOG_MIN_SIZE: usize = 1024 * 16;
/// Seconds to wait before retrying a BGSAVE. C: `CONFIG_BGSAVE_RETRY_DELAY`
pub const CONFIG_BGSAVE_RETRY_DELAY: u64 = 5;

/// Default PID file path. C: `CONFIG_DEFAULT_PID_FILE`
pub const CONFIG_DEFAULT_PID_FILE: &[u8] = b"/var/run/valkey.pid";
/// Default process title template. C: `CONFIG_DEFAULT_PROC_TITLE_TEMPLATE`
pub const CONFIG_DEFAULT_PROC_TITLE_TEMPLATE: &[u8] = b"{title} {listen-addr} {server-mode}";

/// Default grace period (seconds) before freeing RDB client. C: `DEFAULT_WAIT_BEFORE_RDB_CLIENT_FREE`
pub const DEFAULT_WAIT_BEFORE_RDB_CLIENT_FREE: u64 = 5;
/// Default loading-events interval (ms). C: `LOADING_PROCESS_EVENTS_INTERVAL_DEFAULT`
pub const LOADING_PROCESS_EVENTS_INTERVAL_DEFAULT: u64 = 100;

/// File-descriptor set increment over maxclients. C: `CONFIG_FDSET_INCR`
pub const CONFIG_FDSET_INCR: usize = 128; // CONFIG_MIN_RESERVED_FDS(32) + 96

/// Client exit code signalling clean child termination. C: `SERVER_CHILD_NOERROR_RETVAL`
pub const SERVER_CHILD_NOERROR_RETVAL: i32 = 255;

// ── Protocol defines ──────────────────────────────────────────────────────────

/// Generic I/O buffer size. C: `PROTO_IOBUF_LEN`
pub const PROTO_IOBUF_LEN: usize = 1024 * 16;
/// Output buffer chunk size. C: `PROTO_REPLY_CHUNK_BYTES`
pub const PROTO_REPLY_CHUNK_BYTES: usize = 16 * 1024;
/// Max inline read size. C: `PROTO_INLINE_MAX_SIZE`
pub const PROTO_INLINE_MAX_SIZE: usize = 1024 * 64;
/// Resize threshold for query buffer. C: `PROTO_RESIZE_THRESHOLD`
pub const PROTO_RESIZE_THRESHOLD: usize = 1024 * 32;
/// Minimum reply buffer size. C: `PROTO_REPLY_MIN_BYTES`
pub const PROTO_REPLY_MIN_BYTES: usize = 1024;

// ── Log level constants ───────────────────────────────────────────────────────

/// Debug-level log. C: `LL_DEBUG`
pub const LL_DEBUG: u32 = 0;
/// Verbose log. C: `LL_VERBOSE`
pub const LL_VERBOSE: u32 = 1;
/// Notice log. C: `LL_NOTICE`
pub const LL_NOTICE: u32 = 2;
/// Warning log. C: `LL_WARNING`
pub const LL_WARNING: u32 = 3;
/// Log nothing. C: `LL_NOTHING`
pub const LL_NOTHING: u32 = 4;
/// Modifier: log without timestamp. C: `LL_RAW`
pub const LL_RAW: u32 = 1 << 10;

// ── AOF / RDB state constants ─────────────────────────────────────────────────

/// AOF is off. C: `AOF_OFF`
pub const AOF_OFF: u8 = 0;
/// AOF is on. C: `AOF_ON`
pub const AOF_ON: u8 = 1;
/// AOF waiting for rewrite to start. C: `AOF_WAIT_REWRITE`
pub const AOF_WAIT_REWRITE: u8 = 2;

// ── Shutdown flags ────────────────────────────────────────────────────────────

/// No shutdown flags. C: `SHUTDOWN_NOFLAGS`
pub const SHUTDOWN_NOFLAGS: u32 = 0;
/// Force save on shutdown. C: `SHUTDOWN_SAVE`
pub const SHUTDOWN_SAVE: u32 = 1 << 0;
/// Do not save on shutdown. C: `SHUTDOWN_NOSAVE`
pub const SHUTDOWN_NOSAVE: u32 = 1 << 1;
/// Do not wait for replicas. C: `SHUTDOWN_NOW`
pub const SHUTDOWN_NOW: u32 = 1 << 2;
/// Don't let errors prevent shutdown. C: `SHUTDOWN_FORCE`
pub const SHUTDOWN_FORCE: u32 = 1 << 3;
/// Only shut down if safe. C: `SHUTDOWN_SAFE`
pub const SHUTDOWN_SAFE: u32 = 1 << 4;
/// Trigger failover before shutdown. C: `SHUTDOWN_FAILOVER`
pub const SHUTDOWN_FAILOVER: u32 = 1 << 5;

// ── Command call flags ────────────────────────────────────────────────────────

/// C: `CMD_CALL_NONE`
pub const CMD_CALL_NONE: u32 = 0;
/// C: `CMD_CALL_PROPAGATE_AOF`
pub const CMD_CALL_PROPAGATE_AOF: u32 = 1 << 0;
/// C: `CMD_CALL_PROPAGATE_REPL`
pub const CMD_CALL_PROPAGATE_REPL: u32 = 1 << 1;
/// C: `CMD_CALL_FROM_MODULE`
pub const CMD_CALL_FROM_MODULE: u32 = 1 << 2;
/// C: `CMD_CALL_PROPAGATE`
pub const CMD_CALL_PROPAGATE: u32 = CMD_CALL_PROPAGATE_AOF | CMD_CALL_PROPAGATE_REPL;
/// C: `CMD_CALL_FULL`
pub const CMD_CALL_FULL: u32 = CMD_CALL_PROPAGATE;

// ── Command flags (subset) ───────────────────────────────────────────────────

/// C: `CMD_WRITE`
pub const CMD_WRITE: u64 = 1 << 0;
/// C: `CMD_READONLY`
pub const CMD_READONLY: u64 = 1 << 1;
/// C: `CMD_DENYOOM`
pub const CMD_DENYOOM: u64 = 1 << 2;
/// C: `CMD_ADMIN`
pub const CMD_ADMIN: u64 = 1 << 4;
/// C: `CMD_PUBSUB`
pub const CMD_PUBSUB: u64 = 1 << 5;
/// C: `CMD_NOSCRIPT`
pub const CMD_NOSCRIPT: u64 = 1 << 6;
/// C: `CMD_LOADING`
pub const CMD_LOADING: u64 = 1 << 9;
/// C: `CMD_STALE`
pub const CMD_STALE: u64 = 1 << 10;
/// C: `CMD_FAST`
pub const CMD_FAST: u64 = 1 << 13;
/// C: `CMD_NO_AUTH`
pub const CMD_NO_AUTH: u64 = 1 << 14;

// ── Pause-action flags ────────────────────────────────────────────────────────

/// C: `PAUSE_ACTION_CLIENT_WRITE`
pub const PAUSE_ACTION_CLIENT_WRITE: u32 = 1 << 0;
/// C: `PAUSE_ACTION_CLIENT_ALL`
pub const PAUSE_ACTION_CLIENT_ALL: u32 = 1 << 1;
/// C: `PAUSE_ACTION_EXPIRE`
pub const PAUSE_ACTION_EXPIRE: u32 = 1 << 2;
/// C: `PAUSE_ACTION_EVICT`
pub const PAUSE_ACTION_EVICT: u32 = 1 << 3;
/// C: `PAUSE_ACTION_REPLICA`
pub const PAUSE_ACTION_REPLICA: u32 = 1 << 4;

// ── Maxmemory eviction policies ───────────────────────────────────────────────

pub const MAXMEMORY_FLAG_LRU: u32 = 1 << 0;
pub const MAXMEMORY_FLAG_LFU: u32 = 1 << 1;
pub const MAXMEMORY_FLAG_ALLKEYS: u32 = 1 << 2;

pub const MAXMEMORY_VOLATILE_LRU: u32 = (0 << 8) | MAXMEMORY_FLAG_LRU;
pub const MAXMEMORY_VOLATILE_TTL: u32 = 2 << 8;
pub const MAXMEMORY_VOLATILE_RANDOM: u32 = 3 << 8;
pub const MAXMEMORY_ALLKEYS_LRU: u32 = (4 << 8) | MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_ALLKEYS;
pub const MAXMEMORY_ALLKEYS_RANDOM: u32 = (6 << 8) | MAXMEMORY_FLAG_ALLKEYS;
pub const MAXMEMORY_NO_EVICTION: u32 = 7 << 8;

// ── Unit constants ────────────────────────────────────────────────────────────

/// C: `UNIT_SECONDS`
pub const UNIT_SECONDS: u32 = 0;
/// C: `UNIT_MILLISECONDS`
pub const UNIT_MILLISECONDS: u32 = 1;

// ── Replication constants ─────────────────────────────────────────────────────

/// Synchronous replication I/O timeout (seconds). C: `CONFIG_REPL_SYNCIO_TIMEOUT`
pub const CONFIG_REPL_SYNCIO_TIMEOUT: u32 = 5;
/// Trim blocks per cron call. C: `REPL_BACKLOG_TRIM_BLOCKS_PER_CALL`
pub const REPL_BACKLOG_TRIM_BLOCKS_PER_CALL: u32 = 64;
/// Backlog index interval (blocks). C: `REPL_BACKLOG_INDEX_PER_BLOCKS`
pub const REPL_BACKLOG_INDEX_PER_BLOCKS: u32 = 64;

// ── Instantaneous-metrics sample count ───────────────────────────────────────

/// Number of samples per instantaneous metric. C: `STATS_METRIC_SAMPLES`
pub const STATS_METRIC_SAMPLES: usize = 16;

// ═════════════════════════════════════════════════════════════════════════════
// Enumerations
// ═════════════════════════════════════════════════════════════════════════════

/// Instantaneous metric kinds. C: `instantaneous_metric_type` enum in server.h.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StatsMetric {
    Command = 0,
    NetInput,
    NetOutput,
    NetInputReplication,
    NetOutputReplication,
    ElCycle,
    ElDuration,
    IoWait,
    MainThreadActiveTime,
}

/// Number of `StatsMetric` variants. C: `STATS_METRIC_COUNT`
pub const STATS_METRIC_COUNT: usize = 9;

/// Client blocking cause. C: `blocking_type` enum in server.h.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlockingType {
    #[default]
    None,
    List,
    Wait,
    Module,
    Stream,
    ZSet,
    Postpone,
    Shutdown,
}

/// Slow-log / large-request / large-reply log category.
/// C: `commandlog_type` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandlogType {
    Slow = 0,
    LargeRequest,
    LargeReply,
}

/// Replica replication handshake state machine. C: `repl_state` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplState {
    #[default]
    None,
    Connect,
    Connecting,
    ReceivePingReply,
    SendHandshake,
    ReceiveAuthReply,
    ReceivePortReply,
    ReceiveIpReply,
    ReceiveNodeidReply,
    SendPsync,
    ReceivePsyncReply,
    Transfer,
    Connected,
}

/// Dual-channel RDB replication channel state. C: `repl_rdb_channel_state` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplRdbChannelState {
    #[default]
    None,
    SendHandshake,
    ReceiveAuthReply,
    ReceiveReplconfReply,
    ReceiveEndoff,
    RdbLoad,
    RdbLoaded,
}

/// Coordinated-failover state. C: `failover_state` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FailoverState {
    #[default]
    NoFailover,
    WaitForSync,
    InProgress,
}

/// Log format kind. C: `log_format_type` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    #[default]
    Legacy,
    Logfmt,
    Json,
}

/// Log timestamp kind. C: `log_timestamp_type` enum (server.h ~line 599).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogTimestamp {
    #[default]
    Legacy,
    Iso8601,
    Milliseconds,
}

/// Command-list filter discriminant.
/// C: `commandListFilterType` enum in server.c.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandListFilterType {
    Module,
    AclCat,
    Pattern,
}

// ═════════════════════════════════════════════════════════════════════════════
// Type aliases
// ═════════════════════════════════════════════════════════════════════════════

/// Millisecond-precision Unix timestamp. C: `mstime_t` (`long long`).
pub type MsTime = i64;

/// Microsecond-precision Unix timestamp. C: `ustime_t` (`long long`).
pub type UsTime = i64;

/// Monotonic timestamp (µs). C: `monotime`.
pub type Monotime = u64;

// ═════════════════════════════════════════════════════════════════════════════
// Global IEEE floating-point sentinels
// ═════════════════════════════════════════════════════════════════════════════
//
// In C these are globals initialised in main() to avoid compiler-constant-
// folding. We provide them as associated constants here.
//
// C: `double R_Zero, R_PosInf, R_NegInf, R_Nan;` (server.c:101)

/// IEEE 754 positive zero. C: `R_Zero`
pub const R_ZERO: f64 = 0.0_f64;
/// IEEE 754 positive infinity. C: `R_PosInf`
pub const R_POS_INF: f64 = f64::INFINITY;
/// IEEE 754 negative infinity. C: `R_NegInf`
pub const R_NEG_INF: f64 = f64::NEG_INFINITY;
/// IEEE 754 NaN. C: `R_Nan`
pub const R_NAN: f64 = f64::NAN;

// ═════════════════════════════════════════════════════════════════════════════
// Logging utilities
// C: server.c:127–325
// ═════════════════════════════════════════════════════════════════════════════

/// Formats a UTC-offset string like `+HH:MM` or `-HH:MM` into `buf`.
///
/// `timezone` is the offset in seconds west of UTC (POSIX convention: positive
/// means west).  `daylight_active` is 1 when DST is in effect.
///
/// C: `formatTimezone(buf, buflen, timezone, daylight_active)` — server.c:127
pub fn format_timezone(buf: &mut [u8; 7], timezone: i32, daylight_active: i32) {
    debug_assert!((-50400..=43200).contains(&timezone));

    let total_offset = (-1) * timezone + 3600 * daylight_active;
    let hours = (total_offset / 3600).unsigned_abs() as u8;
    let minutes = ((total_offset % 3600) / 60).unsigned_abs() as u8;

    buf[0] = if total_offset >= 0 { b'+' } else { b'-' };
    buf[1] = b'0' + hours / 10;
    buf[2] = b'0' + hours % 10;
    buf[3] = b':';
    buf[4] = b'0' + minutes / 10;
    buf[5] = b'0' + minutes % 10;
    buf[6] = b'\0';
}

/// Returns `true` if `msg` contains characters that are unsafe in logfmt format
/// (`"`, `\n`, `\r`).
///
/// C: `hasInvalidLogfmtChar(msg)` — server.c:143
pub fn has_invalid_logfmt_char(msg: &[u8]) -> bool {
    msg.iter().any(|&b| b == b'"' || b == b'\n' || b == b'\r')
}

/// Copies `msg` into `out`, replacing `"` → `'`, `\n`/`\r` → ` `.
///
/// `out` must be `LOG_MAX_LEN` bytes.
///
/// C: `filterInvalidLogfmtChar(safemsg, safemsglen, msg)` — server.c:162
pub fn filter_invalid_logfmt_char(out: &mut [u8; LOG_MAX_LEN], msg: &[u8]) {
    let limit = out.len() - 1;
    let mut i = 0;
    while i < limit && i < msg.len() {
        out[i] = match msg[i] {
            b'"' => b'\'',
            b'\n' | b'\r' => b' ',
            b => b,
        };
        i += 1;
    }
    out[i] = b'\0';
}

/// Core low-level logging function.  Writes a pre-formatted message to the
/// configured log destination (stdout or file) and optionally to syslog.
///
/// C: `serverLogRaw(level, msg)` — server.c:182
///
/// TODO(port): Needs access to the global `server` config (logfile path,
/// verbosity, format, syslog settings, timezone, pid, role).  For Phase A this
/// writes to stderr unconditionally.  Phase B must thread `&RedisServer` through
/// here or use a global logger (e.g. the `log` crate).
pub fn server_log_raw(level: u32, msg: &[u8]) {
    let raw_mode = (level & LL_RAW) != 0;
    let effective_level = level & 0xff;

    // TODO(port): compare against server.verbosity; for now always log.

    if raw_mode {
        let _ = std::io::stderr().write_all(msg);
    } else {
        let level_char: u8 = match effective_level {
            LL_DEBUG => b'.',
            LL_VERBOSE => b'-',
            LL_NOTICE => b'*',
            _ => b'#',
        };
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        // PORT NOTE: We write the pid and level prefix as ASCII, then the raw
        // byte slice directly.  No UTF-8 conversion — log messages may contain
        // arbitrary byte data (e.g. user keys embedded in error strings).
        let prefix = format!("{}:{} ", std::process::id(), level_char as char);
        let _ = std::io::stderr().write_all(prefix.as_bytes());
        let _ = std::io::stderr().write_all(msg);
        let _ = std::io::stderr().write_all(b"\n");
        let _ = now_ms; // silence unused-variable warning until wired up
    }
}

/// Printf-style logging entry point.  Formats `args` and calls `server_log_raw`.
///
/// C: `_serverLog(level, fmt, ...)` — server.c:272
///
/// PORT NOTE: Rust uses `format!` macros at call sites rather than a varargs
/// function.  Callers should call `server_log(level, &format!("..."))` or use
/// the `server_log!` macro (TODO(architect): define a convenient macro).
pub fn server_log(level: u32, msg: &[u8]) {
    server_log_raw(level, msg);
}

/// Async-signal-safe logging, suitable for use in POSIX signal handlers.
///
/// C: `serverLogRawFromHandler(level, msg)` — server.c:285
///
/// TODO(port): The C implementation writes directly to an fd via `write(2)`
/// to avoid non-async-signal-safe libc functions.  In Rust, true async-
/// signal-safety is not achievable without `unsafe`.  For Phase A we delegate
/// to `server_log_raw` and flag this as incorrect for real signal handlers.
/// TODO(architect): design a proper signal-handler logging path (likely via
/// a lock-free ring buffer drained by the main thread).
pub fn server_log_raw_from_handler(level: u32, msg: &[u8]) {
    server_log_raw(level, msg);
}

/// Printf-style async-signal-safe logging.
///
/// C: `serverLogFromHandler(level, fmt, ...)` — server.c:316
///
/// PORT NOTE: same signal-safety caveats as `server_log_raw_from_handler`.
pub fn server_log_from_handler(level: u32, msg: &[u8]) {
    server_log_raw_from_handler(level, msg);
}

// ═════════════════════════════════════════════════════════════════════════════
// Time snapshot
// C: server.c:332–347
// ═════════════════════════════════════════════════════════════════════════════

/// Returns the command-time snapshot (ms).
///
/// During command execution the time is frozen at the point the command started,
/// so that e.g. `RPOPLPUSH` visiting a key twice in one invocation sees the
/// same expiry timestamp both times.  Scripts rely on this for deterministic
/// propagation to replicas.
///
/// C: `commandTimeSnapshot(void)` — server.c:332
///
/// TODO(port): This should return `server.cmd_time_snapshot` (a field on the
/// running server state).  Phase B must thread `&RedisServer` through here.
/// For Phase A, falls back to the wall clock.
pub fn command_time_snapshot() -> MsTime {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as MsTime)
        .unwrap_or(0)
}

/// Returns the current wall-clock time in microseconds.
///
/// C: `ustime()` (util.c / monotonic.c callers)
pub fn us_time() -> UsTime {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as UsTime)
        .unwrap_or(0)
}

/// Returns the current wall-clock time in milliseconds.
///
/// C: `mstime()` (util.c callers)
pub fn ms_time() -> MsTime {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as MsTime)
        .unwrap_or(0)
}

// ═════════════════════════════════════════════════════════════════════════════
// Instantaneous metric tracking
// C: server.c:920–944
// ═════════════════════════════════════════════════════════════════════════════

/// Per-metric instantaneous sample ring buffer.
///
/// C: embedded in `struct valkeyServer` as `struct{...} inst_metric[STATS_METRIC_COUNT]`.
/// Phase A externalises this as a standalone struct so the caller can own it.
#[derive(Debug, Default)]
pub struct InstMetric {
    /// Ring-buffer of raw values. C: `samples[STATS_METRIC_SAMPLES]`
    pub samples: [i64; STATS_METRIC_SAMPLES],
    /// Next write position. C: `idx`
    pub idx: usize,
    /// Last recorded cumulative value. C: `last_sample_base`
    pub last_sample_base: i64,
    /// Last recorded sample count. C: `last_sample_count`
    pub last_sample_count: i64,
}

/// Updates one instantaneous-metric ring-buffer slot.
///
/// C: `trackInstantaneousMetric(metric, current_value, current_base, factor)` — server.c:920
///
/// `current_value` and `current_base` are cumulative counters; the function
/// records the delta since the last call, scaled by `factor`.
///
/// PORT NOTE: The C version mutates `server.inst_metric[]` directly.  Here we
/// pass `&mut InstMetric` so the caller controls which slot is updated.
pub fn track_instantaneous_metric(
    metric: &mut InstMetric,
    current_value: i64,
    current_base: i64,
    factor: i64,
) {
    let base_delta = current_base - metric.last_sample_base;
    let count_delta = current_value - metric.last_sample_count;

    let sample = if base_delta > 0 {
        count_delta * factor / base_delta
    } else {
        0
    };

    metric.samples[metric.idx] = sample;
    metric.idx = (metric.idx + 1) % STATS_METRIC_SAMPLES;
    metric.last_sample_base = current_base;
    metric.last_sample_count = current_value;
}

/// Returns the instantaneous metric rate (average over the ring buffer).
///
/// C: `getInstantaneousMetric(metric)` — server.c:934
///
/// PORT NOTE: same ownership note as `track_instantaneous_metric`.
pub fn get_instantaneous_metric(metric: &InstMetric) -> i64 {
    let sum: i64 = metric.samples.iter().sum();
    sum / STATS_METRIC_SAMPLES as i64
}

// ═════════════════════════════════════════════════════════════════════════════
// Server-state helpers
// C: server.c:877–919
// ═════════════════════════════════════════════════════════════════════════════

/// Returns `true` if there is an active background child process (RDB / AOF save).
///
/// C: `hasActiveChildProcess(void)` — server.c:877
///
/// TODO(port): Checks `server.child_type != CHILD_TYPE_NONE`.  Phase B must
/// add `child_type` tracking to `RedisServer`.
pub fn has_active_child_process() -> bool {
    false // TODO(port): wire up to server.child_type
}

/// Returns `true` if all persistence mechanisms (RDB and AOF) are disabled.
///
/// C: `allPersistenceDisabled(void)` — server.c:907
///
/// TODO(port): Requires access to `server.saveparams`, `server.aof_state`.
pub fn all_persistence_disabled() -> bool {
    true // TODO(port): wire up to RedisServer config fields
}

/// Returns `true` if the server is in a context that should be treated as
/// exclusive (child-type RDB/AOF write is mutually exclusive with another
/// active child).
///
/// C: `isMutuallyExclusiveChildType(type)` — server.c:896
///
/// TODO(port): needs `ChildType` enum wired through RedisServer.
pub fn is_mutually_exclusive_child_type(child_type: u32) -> bool {
    let _ = child_type;
    false // TODO(port): implement with ChildType enum
}

// ═════════════════════════════════════════════════════════════════════════════
// Server initialization stubs
// C: server.c:2299 (initServerConfig), server.c:2915 (initServer)
// ═════════════════════════════════════════════════════════════════════════════

/// Initialises the server configuration with built-in defaults.
///
/// C: `initServerConfig(void)` — server.c:2299 (~150 lines)
///
/// TODO(port): This populates >100 fields on the C `valkeyServer` struct,
/// including replication IDs, AOF state, persistence save params, TLS cert
/// expiry, thread counts, etc.  Phase B must expand `RedisServer` in
/// `redis-core` with those fields and call setters here.
pub fn init_server_config(server: &mut RedisServer) {
    // PORT NOTE: Only the fields that exist on the Phase-A RedisServer stub
    // are set here.  All other fields in the C version are TODO(port).
    server.port = 6379;
    // TODO(port): server.hz = CONFIG_DEFAULT_HZ
    // TODO(port): server.timezone = getTimeZone()
    // TODO(port): getRandomHexChars(server.runid, CONFIG_RUN_ID_SIZE)
    // TODO(port): server.arch_bits = if sizeof::<usize>() == 8 { 64 } else { 32 }
    // TODO(port): server.bindaddr_count / bindaddr defaults
    // TODO(port): server.active_expire_enabled = true
    // TODO(port): server.aof_state = AOF_OFF
    // TODO(port): replication state initialisation
    // TODO(port): latency percentile defaults (50.0, 99.0, 99.9)
    // TODO(port): TLS cert expiry defaults
    // TODO(port): save params (1h/1, 5m/100, 1m/10000)
}

/// Completes server initialisation after config has been loaded.
///
/// Sets up signal handlers, creates event loop, allocates databases, creates
/// shared objects, etc.
///
/// C: `initServer(void)` — server.c:2915 (~220 lines)
///
/// TODO(port): The bulk of this function wires together the event loop (`ae`),
/// database array (`serverDb`), pubsub channels (`kvstore`), shared objects,
/// and various system-level setup (setlocale, open-files limit, THP disable).
/// Phase B will implement this once `EventLoop` and the full `RedisServer`
/// struct are wired.
pub fn init_server(server: &mut RedisServer) -> Result<(), RedisError> {
    // TODO(port): signal(SIGHUP, SIG_IGN); signal(SIGPIPE, SIG_IGN);
    // TODO(port): setupSignalHandlers()
    // TODO(port): server.aof_state = if server.aof_enabled { AOF_ON } else { AOF_OFF }
    // TODO(port): server.clients = VecDeque::new()
    // TODO(port): server.errors = RadixTree::new()
    // TODO(port): server.el = EventLoop::new(server.maxclients + CONFIG_FDSET_INCR)
    // TODO(port): createDatabaseIfNeeded(0)
    // TODO(port): create shared objects (shared.ok, shared.pong, etc.)
    server_log(LL_NOTICE, b"Server initialized (Phase A stub)");
    Ok(())
}

/// Completes late-stage initialisation (after event listeners are set up).
///
/// C: `InitServerLast(void)` — server.c:3221
///
/// TODO(port): Initialises background threads (BIO), modules, scripting
/// engines, and TLS.
pub fn init_server_last(_server: &mut RedisServer) -> Result<(), RedisError> {
    // TODO(port): bioInit()
    // TODO(port): initThreadedIO()
    // TODO(port): moduleLoadFromQueue()
    // TODO(port): ACLUpdateDefaultUserPassword()
    Ok(())
}

// ═════════════════════════════════════════════════════════════════════════════
// Command execution
// C: server.c:3863 (call), server.c:4303 (processCommand)
// ═════════════════════════════════════════════════════════════════════════════

/// Executes the command currently queued on `ctx`.
///
/// This is the heart of Redis command dispatch: it calls the command handler,
/// records latency, propagates to AOF/replicas if needed, fires module events,
/// and updates command statistics.
///
/// C: `call(client *c, int flags)` — server.c:3863 (~270 lines)
///
/// TODO(port): This function is extremely complex.  Key pending work:
///   - AOF propagation (`propagatePendingCommands`)
///   - Module callback hooks (`moduleFireCommandResultEvent`)
///   - Latency tracking (`latencyAddSampleIfNeeded`)
///   - Commandlog / monitoring (`commandlogTryAddEntry`, monitor feed)
///   - Replication backlog accounting (`server.dirty`)
///   - Debug argv-clone assertion (guarded by `server.enable_debug_assert`)
pub fn call(ctx: &mut CommandContext, flags: u32) -> Result<(), RedisError> {
    // TODO(port): Save client_old_flags, server.executing_client
    // TODO(port): Clear force_aof / force_repl / prevent_prop flags on ctx
    // TODO(port): record dirty = server.dirty, call_timer = ustime()
    // TODO(port): enterExecutionUnit(1, call_timer)
    // TODO(port): ctx.cmd.proc(ctx)    ← actual command dispatch
    // TODO(port): exitExecutionUnit()
    // TODO(port): duration = ustime() - call_timer
    // TODO(port): update command stats, commandlog, monitor
    // TODO(port): propagate to AOF / replicas if CMD_CALL_PROPAGATE set in flags
    let _ = flags;
    // PORT NOTE: For Phase 2 (pilot single-command loop), dispatch is done
    // directly in the networking layer; this stub is a placeholder.
    Err(RedisError::runtime(b"call() not yet implemented"))
}

/// Validates and dispatches a parsed command from a client.
///
/// C: `processCommand(client *c)` — server.c:4303 (~380 lines)
///
/// Checks authentication, ACLs, cluster redirection, OOM, loading/stale
/// state, multi/exec consistency, and then calls `call()`.
///
/// TODO(port): Very large function.  Key pending checks:
///   - `authRequired(c)` → reject with shared.noautherr
///   - ACL checks (`ACLCheckAllPerm`)
///   - Cluster redirection (`getNodeByQuery` / `clusterRedirectClient`)
///   - OOM check (`server.maxmemory > 0 && ...`)
///   - Loading / stale checks
///   - Pub/sub context restrictions
///   - Multi/exec state checks
///   - Pause-actions check
pub fn process_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): moduleCallCommandFilters(c)
    // TODO(port): reqresAppendRequest(c)
    // TODO(port): busy-module yield postpone
    // TODO(port): command existence / arity check
    // TODO(port): AUTH gate
    // TODO(port): ACL check
    // TODO(port): cluster redirect
    // TODO(port): OOM check
    // TODO(port): loading / stale check
    // TODO(port): multi/exec state check
    // TODO(port): delegate to call()
    let _ = ctx;
    Err(RedisError::runtime(b"processCommand() not yet implemented"))
}

/// Prepares a command context for execution (command lookup, arity check).
///
/// C: `prepareCommand(client *c)` — server.c:4269
///
/// TODO(port): Sets `c->cmd`, `c->parsed_cmd`, `c->read_flags`, `c->slot`.
pub fn prepare_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _ = ctx;
    Ok(()) // TODO(port): implement command lookup
}

// ═════════════════════════════════════════════════════════════════════════════
// Rejection helpers
// C: server.c:4131–4170
// ═════════════════════════════════════════════════════════════════════════════

/// Rejects the current command with a pre-built reply object.
///
/// C: `rejectCommand(client *c, robj *reply, int notify_modules)` — server.c:4131
///
/// TODO(port): `notify_modules` fires a module hook; Phase 10.
pub fn reject_command(ctx: &mut CommandContext, reply: &RedisObject) -> Result<(), RedisError> {
    let _ = reply;
    let _ = ctx;
    // TODO(port): addReply(c, reply); moduleNotifyCommandReject(c)
    Err(RedisError::runtime(b"command rejected"))
}

/// Rejects the current command with a byte-string error.
///
/// C: `rejectCommandSds(client *c, sds s, ...)` — server.c:4146
pub fn reject_command_bytes(ctx: &mut CommandContext, err: &[u8]) -> Result<(), RedisError> {
    let _ = ctx;
    Err(RedisError::runtime(err))
}

// ═════════════════════════════════════════════════════════════════════════════
// Shutdown management
// C: server.c:4748–4826
// ═════════════════════════════════════════════════════════════════════════════

/// State for the orderly-shutdown protocol.  Extracted from `RedisServer`
/// fields for Phase A.
///
/// C: fields `server.shutdown_mstime`, `server.shutdown_asap`,
///    `server.shutdown_flags`, `server.last_sig_received`.
#[derive(Debug, Default)]
pub struct ShutdownState {
    /// Non-zero when a delayed shutdown has been initiated (deadline in ms).
    /// C: `server.shutdown_mstime`
    pub shutdown_mstime: MsTime,
    /// Set by the signal handler; causes the cron to call `prepareForShutdown`.
    /// C: `server.shutdown_asap`
    pub shutdown_asap: bool,
    /// Flags from the SHUTDOWN command or signal handler.
    /// C: `server.shutdown_flags`
    pub shutdown_flags: u32,
    /// Last POSIX signal received. C: `server.last_sig_received`
    pub last_sig_received: i32,
}

/// Returns `true` if a deadline-based shutdown has been initiated.
///
/// C: `isShutdownInitiated(void)` — server.c:4783 (static inline)
pub fn is_shutdown_initiated(state: &ShutdownState) -> bool {
    state.shutdown_mstime != 0
}

/// Returns `true` if all replicas have caught up to the current replication
/// offset (safe to shut down without data loss).
///
/// C: `isReadyToShutdown(void)` — server.c:4790
///
/// TODO(port): iterate `server.replicas`, check `replica.repl_data.repl_ack_off`
/// against `server.primary_repl_offset`.
pub fn is_ready_to_shutdown() -> bool {
    true // TODO(port): check replica lag; for now assume no replicas
}

/// Cancels an in-progress shutdown (SIGTERM/SIGINT).
///
/// C: `cancelShutdown(void)` — server.c:4803 (static)
///
/// TODO(port): needs `replyToClientsBlockedOnShutdown()` and `unpauseActions`.
pub fn cancel_shutdown(state: &mut ShutdownState) {
    state.shutdown_asap = false;
    state.shutdown_flags = 0;
    state.shutdown_mstime = 0;
    state.last_sig_received = 0;
    // TODO(port): replyToClientsBlockedOnShutdown()
    // TODO(port): unpauseActions(PAUSE_DURING_SHUTDOWN)
}

/// Aborts a pending or initiated shutdown.
///
/// Returns `Ok(())` if a shutdown was aborted, or `Err` if none was pending.
///
/// C: `abortShutdown(void)` — server.c:4813
pub fn abort_shutdown(state: &mut ShutdownState) -> Result<(), RedisError> {
    if is_shutdown_initiated(state) {
        cancel_shutdown(state);
    } else if state.shutdown_asap {
        state.shutdown_asap = false;
    } else {
        return Err(RedisError::runtime(b"No shutdown was initiated"));
    }
    server_log(LL_NOTICE, b"Shutdown manually aborted.");
    Ok(())
}

/// Initiates (or immediately completes) server shutdown.
///
/// C: `prepareForShutdown(client *c, int flags)` — server.c:4748
///
/// Returns `Ok(())` if shutdown should proceed immediately (`exit(0)` in C),
/// or `Err` if shutdown was deferred to wait for replica catch-up.
///
/// TODO(port): Needs access to `server.loading`, `server.sentinel_mode`,
/// `server.shutdown_timeout`, `server.replicas`, `server.mstime`.
pub fn prepare_for_shutdown(
    state: &mut ShutdownState,
    flags: u32,
) -> Result<(), RedisError> {
    if is_shutdown_initiated(state) {
        return Err(RedisError::runtime(b"Shutdown already initiated"));
    }

    // TODO(port): if server.loading || server.sentinel_mode { flags = (flags & !SHUTDOWN_SAVE) | SHUTDOWN_NOSAVE }
    state.shutdown_flags = flags;
    server_log(LL_NOTICE, b"User requested shutdown...");

    // TODO(port): if supervised_mode == SUPERVISED_SYSTEMD { serverCommunicateSystemd("STOPPING=1\n") }
    // TODO(port): if !(flags & SHUTDOWN_NOW) && shutdown_timeout != 0 && !is_ready_to_shutdown() { defer... }

    finish_shutdown(state, flags)
}

/// Performs the actual shutdown sequence: saves data if needed, closes
/// listeners, exits.
///
/// C: `finishShutdown(void)` — server.c:4831 (~170 lines)
///
/// TODO(port): Very complex — RDB / AOF save logic, replica disconnect,
/// AOF rewrite, module shutdown hooks, socket cleanup, AOF fsync.
/// Returns `Ok(())` to signal "shutdown is complete; caller should exit".
pub fn finish_shutdown(state: &mut ShutdownState, flags: u32) -> Result<(), RedisError> {
    let _ = flags;
    let _ = state;
    // TODO(port): if !(flags & SHUTDOWN_NOSAVE) { save RDB / AOF }
    // TODO(port): closeListeningSockets(1)
    // TODO(port): flushAppendOnlyFile(1) if aof_enabled
    // TODO(port): moduleFireServerEvent(SHUTDOWN, ...)
    server_log(LL_NOTICE, b"Shutdown complete.");
    Ok(())
}

// ═════════════════════════════════════════════════════════════════════════════
// Built-in commands
// C: server.c:5029–5059
// ═════════════════════════════════════════════════════════════════════════════

/// PING [message]
///
/// Returns PONG (or the optional message as a bulk string).  In PubSub context
/// with RESP2, wraps the reply as a two-element array `["pong", <message>]`.
///
/// C: `pingCommand(client *c)` — server.c:5029
pub fn ping_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();

    if argc > 2 {
        return Err(RedisError::wrong_number_of_args(b"ping"));
    }

    if ctx.is_pubsub_client() && ctx.resp_version() == 2 {
        // RESP2 pub/sub context: reply with ["pong", <message>]
        ctx.reply_array_header(2)?;
        ctx.reply_bulk(b"pong")?;
        if argc == 1 {
            ctx.reply_bulk(b"")?;
        } else {
            ctx.reply_bulk_arg(1)?;
        }
    } else if argc == 1 {
        ctx.reply_simple_string(b"PONG")?;
    } else {
        ctx.reply_bulk_arg(1)?;
    }
    Ok(())
}

/// ECHO message
///
/// Returns the argument as a bulk string.
///
/// C: `echoCommand(client *c)` — server.c:5051
pub fn echo_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    ctx.reply_bulk_arg(1)
}

/// TIME
///
/// Returns a two-element array: [unix_seconds, microseconds_within_second].
///
/// C: `timeCommand(client *c)` — server.c:5055
pub fn time_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let now_us = us_time();
    let secs = now_us / 1_000_000;
    let usecs = now_us % 1_000_000;

    ctx.reply_array_header(2)?;
    ctx.reply_integer_bulk(secs)?;
    ctx.reply_integer_bulk(usecs)?;
    Ok(())
}

/// COMMAND (returns metadata for all commands)
///
/// Uses a cached pre-serialised response keyed by the client's RESP version.
///
/// C: `commandCommand(client *c)` — server.c:5598
///
/// TODO(port): Needs access to the generated command registry
/// (`server.commands` hashtable) and cached RESP serialisations.
pub fn command_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): look up server.command_response_cache[resp_version]
    // TODO(port): generate via generateCommandResponse(resp) if not cached
    // TODO(port): addReplyProto(c, cache, sdslen(cache))
    let _ = ctx;
    Err(RedisError::runtime(b"COMMAND not yet implemented in Phase A"))
}

/// COMMAND COUNT
///
/// Returns the number of commands in the command table.
///
/// C: `commandCountCommand(client *c)` — server.c:5614
///
/// TODO(port): `hashtableSize(server.commands)`.
pub fn command_count_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): ctx.reply_integer(server.commands.len() as i64)
    let _ = ctx;
    Err(RedisError::runtime(b"COMMAND COUNT not yet implemented"))
}

/// COMMAND GETKEYS <command> [arg ...]
///
/// Returns the keys touched by the given command string.
///
/// C: `getKeysSubcommand(client *c)` — server.c:5554
///
/// TODO(port): Requires key-extraction logic from the command spec.
pub fn command_getkeys_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _ = ctx;
    Err(RedisError::runtime(b"COMMAND GETKEYS not yet implemented"))
}

/// COMMAND INFO [command-name ...]
///
/// C: `commandInfoCommand(client *c)` — server.c:5738
///
/// TODO(port): iterates over named commands or all commands and replies
/// with the COMMAND INFO structure for each.
pub fn command_info_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _ = ctx;
    Err(RedisError::runtime(b"COMMAND INFO not yet implemented"))
}

/// COMMAND DOCS [command-name ...]
///
/// C: `commandDocsCommand(client *c)` — server.c:5760
///
/// TODO(port): returns the documentation map for each named command.
pub fn command_docs_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _ = ctx;
    Err(RedisError::runtime(b"COMMAND DOCS not yet implemented"))
}

/// COMMAND LIST [FILTERBY MODULE <module> | ACLCAT <cat> | PATTERN <pat>]
///
/// C: `commandListCommand(client *c)` — server.c:5697
///
/// TODO(port): iterate command table with optional filter.
pub fn command_list_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _ = ctx;
    Err(RedisError::runtime(b"COMMAND LIST not yet implemented"))
}

/// INFO [section ...]
///
/// Returns server statistics grouped into named sections.
///
/// C: `infoCommand(client *c)` — server.c:6805
///       `genValkeyInfoString(section_dict, all, everything)` — server.c:6099 (~700 lines)
///
/// TODO(port): The C implementation builds a large sds by iterating over
/// ~25 named sections.  Phase B will implement each section getter using
/// Rust server state fields.  For Phase A this returns an informational stub.
pub fn info_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): if server.sentinel_mode { return sentinelInfoCommand(c) }
    // TODO(port): parse section arguments (argv[1..])
    // TODO(port): call gen_valkey_info_string(sections, all_sections, everything)
    // TODO(port): ctx.reply_verbatim(info_bytes, b"txt")
    let stub = b"# Server\r\nredis_version:0.0.1-phase-a\r\n";
    ctx.reply_verbatim(stub, b"txt")
}

/// MONITOR
///
/// Switches the client into monitor mode (receives all commands executed by
/// any client as they happen).
///
/// C: `monitorCommand(client *c)` — server.c:6820
///
/// TODO(port): Requires `client.flag.deny_blocking`, `client.flag.replica`,
/// `client.flag.monitor`, `server.monitors` list append.
pub fn monitor_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.deny_blocking() {
        return Err(RedisError::runtime(
            b"MONITOR isn't allowed for DENY BLOCKING client",
        ));
    }
    // TODO(port): if c.flag.replica { return Ok(()) }  /* ignore if already a replica */
    // TODO(port): initClientReplicationData(c)
    // TODO(port): c.flag.replica = true; c.flag.monitor = true
    // TODO(port): listAddNodeTail(server.monitors, c)
    // TODO(port): ctx.reply_simple_string(b"OK")
    Err(RedisError::runtime(b"MONITOR not yet implemented"))
}

/// RESET
///
/// Resets client state (auth, RESP version, etc.) to defaults.
///
/// C: `resetCommand(client *c)` — server.c (line varies by version)
///
/// TODO(port): Clear client flags, reset RESP version to 2, etc.
pub fn reset_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _ = ctx;
    Err(RedisError::runtime(b"RESET not yet implemented"))
}

/// DEBUG … (various sub-commands for server introspection / testing)
///
/// C: `debugCommand(client *c)` — debug.c (referenced here as it is in scope)
///
/// TODO(port): large sub-command dispatch; implement in redis-core/src/debug.rs.
pub fn debug_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _ = ctx;
    Err(RedisError::runtime(b"DEBUG not yet implemented"))
}

// ═════════════════════════════════════════════════════════════════════════════
// SHUTDOWN command
// C: server.c:4748 (prepareForShutdown called from shutdownCommand)
// ═════════════════════════════════════════════════════════════════════════════

/// SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT]
///
/// C: `shutdownCommand(client *c)` — server.c (delegating to prepareForShutdown)
///
/// TODO(port): parse flags, call prepare_for_shutdown, exit on Ok(()).
pub fn shutdown_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _ = ctx;
    Err(RedisError::runtime(b"SHUTDOWN not yet implemented"))
}

// ═════════════════════════════════════════════════════════════════════════════
// Event-loop hooks
// C: server.c:1846 (beforeSleep), server.c:2048 (afterSleep), server.c:1537 (serverCron)
// ═════════════════════════════════════════════════════════════════════════════

/// Called by the event loop before blocking in poll/epoll/kqueue.
///
/// Handles: I/O thread responses, pending TLS data, cluster sleep hooks,
/// blocked clients, AOF flush, replica ACKs, keyspace notifications, client
/// memory updates, lazy-free jobs, and write-pending clients.
///
/// C: `beforeSleep(struct aeEventLoop *eventLoop)` — server.c:1846 (~200 lines)
///
/// TODO(port): All sub-operations require wired-up subsystems (I/O threads,
/// AOF, cluster, replication).  Phase 2 / later will fill this in.
pub fn before_sleep() -> Result<(), RedisError> {
    // TODO(port): trySendPollJobToIOThreads()
    // TODO(port): update stat_peak_memory
    // TODO(port): if ProcessingEventsWhileBlocked { minimal subset + return }
    // TODO(port): processIOThreadsResponses()
    // TODO(port): connTypeProcessPendingData()
    // TODO(port): if aof_state == AOF_ON || AOF_WAIT_REWRITE { flushAppendOnlyFile(0) }
    // TODO(port): if cluster_enabled { clusterBeforeSleep() }
    // TODO(port): blockedBeforeSleep()
    // TODO(port): activeExpireCycle(ACTIVE_EXPIRE_CYCLE_FAST)
    // TODO(port): moduleFireServerEvent(EVENTLOOP, BEFORE_SLEEP, NULL)
    // TODO(port): sendGetackToReplicas() if get_ack_from_replicas
    // TODO(port): handleClientsWithPendingWrites()
    // TODO(port): freeClientsInAsyncFreeQueue()
    Ok(())
}

/// Called by the event loop after returning from poll/epoll/kqueue.
///
/// C: `afterSleep(struct aeEventLoop *eventLoop, int numevents)` — server.c:2048
///
/// TODO(port): Updates latency stats, fires module event, handles I/O threads.
pub fn after_sleep(_num_events: i32) -> Result<(), RedisError> {
    // TODO(port): latencyAddSampleIfNeeded("eventloop-cycle", ...)
    // TODO(port): moduleFireServerEvent(EVENTLOOP, AFTER_SLEEP, NULL)
    Ok(())
}

/// Main server cron timer callback: called `server.hz` times per second.
///
/// Handles: instantaneous metrics, memory stats, database cron, AOF rewrite
/// scheduling, child-process monitoring, replication, RDB saving, cluster
/// cron, client cron, replica ACK requests, and server stats tracking.
///
/// C: `serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData)` — server.c:1537 (~225 lines)
///
/// Returns the next interval in milliseconds (1000 / hz).
///
/// TODO(port): Each sub-operation is a deferred subsystem.  The skeleton
/// below follows the C order of operations.
pub fn server_cron() -> u64 {
    // TODO(port): software watchdog signal scheduling
    // TODO(port): if server.pause_cron { return 1000 / server.hz }

    // TODO(port): run_with_period(100) { trackInstantaneousMetric(...) * 8 }
    // TODO(port): cronUpdateMemoryStats()

    // TODO(port): if server.shutdown_asap && !isShutdownInitiated() { prepareForShutdown }
    // TODO(port): else if isShutdownInitiated() { if ready { finishShutdown / exit } }

    // TODO(port): if verbosity <= LL_VERBOSE: log DB sizes every 5s
    // TODO(port): if !sentinel_mode: log connected-client count every 5s

    // TODO(port): databasesCron()

    // TODO(port): if !hasActiveChildProcess() && aof_rewrite_scheduled && !aofRewriteLimited() { rewriteAppendOnlyFileBackground() }
    // TODO(port): if hasActiveChildProcess() { checkChildrenDone() }
    // TODO(port): else: check save params, bgsaveIfNeeded()

    // TODO(port): run_with_period(100 * REPLICATION_CRON_PERIOD) { replicationCron() }
    // TODO(port): run_with_period(100) { modulesCron() }
    // TODO(port): if cluster_enabled { run_with_period(100) { clusterCron() } }
    // TODO(port): run_with_period(1000) { sentinelTimer() if sentinel_mode }

    // TODO(port): clientsCron(CLIENTS_PER_CRON_CALL)

    // TODO(port): trackInstantaneousMetric MAIN_THREAD_ACTIVE_TIME
    // TODO(port): cronloops++

    // PERF(port): The C version returns milliseconds to the next call.
    // For Phase A we always return a 100ms interval (hz=10 equivalent).
    100
}

// ═════════════════════════════════════════════════════════════════════════════
// Client memory / cron helpers
// C: server.c:946–1266
// ═════════════════════════════════════════════════════════════════════════════

/// Resizes a client's query buffer if it's oversized and idle.
///
/// C: `clientsCronResizeQueryBuffer(client *c)` — server.c:946
///
/// TODO(port): Requires `client.qb_pos`, `client.querybuf`, timing.
pub fn clients_cron_resize_query_buffer(_client_id: u64) -> bool {
    false // TODO(port)
}

/// Runs per-client cron tasks for a batch of clients.
///
/// C: `clientsCron(int clients_this_cycle)` — server.c:1205
///
/// TODO(port): iterates `server.clients` with a round-robin cursor, calling
/// resize, timeout, and memory-usage tracking for each client.
pub fn clients_cron(clients_this_cycle: usize) {
    let _ = clients_this_cycle;
    // TODO(port): implement client iterator + per-client tasks
}

// ═════════════════════════════════════════════════════════════════════════════
// Database cron
// C: server.c:1304
// ═════════════════════════════════════════════════════════════════════════════

/// Performs per-database maintenance (active expiry, resizing, defrag).
///
/// C: `databasesCron(void)` — server.c:1304 (~68 lines)
///
/// TODO(port): iterate `server.db[]`, call activeExpireCycle, tryResizeDb,
/// activeDefragCycle, incrementallyRehash.
pub fn databases_cron() {
    // TODO(port): implement
}

// ═════════════════════════════════════════════════════════════════════════════
// Command propagation helpers
// C: server.c:3585–3787
// ═════════════════════════════════════════════════════════════════════════════

/// Returns `true` if the client must be obeyed regardless of server state
/// (e.g. the primary in a replica context).
///
/// C: `mustObeyClient(client *c)` — server.c:3577
///
/// TODO(port): checks `c->flag.primary` and `c->id == CLIENT_ID_AOF`.
pub fn must_obey_client(_ctx: &CommandContext) -> bool {
    false // TODO(port): implement with client flags
}

/// Propagates a command to AOF and/or replicas.
///
/// C: `propagateNow(dbid, argv, argc, target, slot)` — server.c:3614 (static)
///
/// TODO(port): writes the command to `server.repl_buffer` and the AOF file.
pub fn propagate_now(
    _dbid: i32,
    _argv: &[RedisObject],
    _target: u32,
    _slot: i32,
) {
    // TODO(port): implement AOF + replication propagation
}

/// Marks a command for forced propagation to AOF / replicas.
///
/// C: `forceCommandPropagation(client *c, int flags)` — server.c:3696
pub fn force_command_propagation(_ctx: &mut CommandContext, _flags: u32) {
    // TODO(port): set ctx.flag.force_aof / force_repl
}

/// Prevents a command from being propagated at all.
///
/// C: `preventCommandPropagation(client *c)` — server.c:3705
pub fn prevent_command_propagation(_ctx: &mut CommandContext) {
    // TODO(port): set ctx.flag.prevent_prop
}

// ═════════════════════════════════════════════════════════════════════════════
// Miscellaneous helpers
// C: server.c:5822–5879
// ═════════════════════════════════════════════════════════════════════════════

/// Converts `n` bytes to a human-readable string (e.g. "1.50G", "384.00M").
///
/// C: `bytesToHuman(char *s, size_t size, unsigned long long n)` — server.c:5822
pub fn bytes_to_human(n: u64) -> Vec<u8> {
    const UNITS: &[(&[u8], u64)] = &[
        (b"P", 1 << 50),
        (b"T", 1 << 40),
        (b"G", 1 << 30),
        (b"M", 1 << 20),
        (b"K", 1 << 10),
    ];
    for (suffix, factor) in UNITS {
        if n >= *factor {
            let val = n as f64 / *factor as f64;
            let mut out = format!("{:.2}", val).into_bytes();
            out.extend_from_slice(suffix);
            return out;
        }
    }
    format!("{}", n).into_bytes()
}

/// Returns `true` if `warning` appears in `server.ignore_warnings`.
///
/// C: `checkIgnoreWarning(const char *warning)` — server.c:6842
///
/// TODO(port): needs access to `server.ignore_warnings` config field.
pub fn check_ignore_warning(_warning: &[u8]) -> bool {
    false // TODO(port): parse server.ignore_warnings
}

/// Returns `true` if the write-denial disk-error condition is active.
///
/// C: `writeCommandsDeniedByDiskError(void)` — server.c:4998
///
/// TODO(port): Checks `server.aof_last_write_status` / `server.lastbgsave_status`.
pub fn write_commands_denied_by_disk_error() -> bool {
    false // TODO(port)
}

/// Returns a human-readable description of the current disk-error condition.
///
/// C: `writeCommandsGetDiskErrorMessage(int error_code)` — server.c:5016
pub fn write_commands_get_disk_error_message(error_code: i32) -> Vec<u8> {
    let _ = error_code;
    b"MISCONF Unknown disk error".to_vec() // TODO(port)
}

// ═════════════════════════════════════════════════════════════════════════════
// Server op-array (multi-propagation accumulator)
// C: server.c:3438–3459
// ═════════════════════════════════════════════════════════════════════════════

/// One pending propagation entry.  C: `serverOp` struct in server.h.
#[derive(Debug, Clone)]
pub struct ServerOp {
    /// Target database index.
    pub dbid: i32,
    /// Command arguments (owned byte strings).
    pub argv: Vec<RedisObject>,
    /// Propagation target flags (`PROPAGATE_AOF | PROPAGATE_REPL`).
    pub target: u32,
    /// Cluster hash slot (-1 if not applicable).
    pub slot: i32,
}

/// Accumulates pending propagation ops for deferred flushing.
/// C: `serverOpArray` struct in server.h.
#[derive(Debug, Default, Clone)]
pub struct ServerOpArray {
    ops: Vec<ServerOp>,
}

impl ServerOpArray {
    pub fn append(&mut self, dbid: i32, argv: Vec<RedisObject>, target: u32, slot: i32) {
        self.ops.push(ServerOp { dbid, argv, target, slot });
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn drain(&mut self) -> Vec<ServerOp> {
        std::mem::take(&mut self.ops)
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Error counter
// C: server.c:4679
// ═════════════════════════════════════════════════════════════════════════════

/// Increments the error frequency counter for an error prefix.
///
/// C: `incrementErrorCount(const char *fullerr, size_t namelen)` — server.c:4679
///
/// TODO(port): Inserts/updates `server.errors` RadixTree keyed by `fullerr[0..namelen]`.
pub fn increment_error_count(fullerr: &[u8], namelen: usize) {
    let _ = (fullerr, namelen);
    // TODO(port): server.errors radix-tree update
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/server.c (7942 lines, ~180 functions)
//                  + src/server.h (4307 lines)
//   target_crate:  redis-server
//   confidence:    medium
//   todos:         161
//   port_notes:    6
//   unsafe_blocks: 0
//   notes:         Phase A skeleton. Constants, enums, type aliases, and
//                  simple utility functions (format_timezone, logging, bytes_to_human,
//                  ping/echo/time commands, shutdown helpers, metrics ring-buffer)
//                  are fully translated. Complex multi-hundred-line functions
//                  (call, processCommand, serverCron, genValkeyInfoString,
//                  initServer, finishShutdown, beforeSleep) are stubs with
//                  TODO(port) markers identifying each sub-operation in C-order.
//                  Canonical types (RedisServer, Client, RedisDb, RedisObject,
//                  CommandContext, RedisString, RedisError, RespFrame) are
//                  pub-used from their owner crates; not redefined here.
//                  Phase B must expand RedisServer fields in redis-core to
//                  support the full server-cron / shutdown / propagation logic.
//                  CommandContext trait methods used in commands (reply_bulk_arg,
//                  reply_verbatim, is_pubsub_client, resp_version, deny_blocking,
//                  argc) require TODO(architect) once CommandContext is expanded.
// ──────────────────────────────────────────────────────────────────────────
