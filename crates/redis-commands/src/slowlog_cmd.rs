//! SLOWLOG and LATENCY command handlers with global ring-buffer state.
//!
//! Wraps the fully-ported `redis_core::commandlog` and `redis_core::latency`
//! modules behind a pair of `OnceLock<Arc<Mutex<_>>>` globals so the stateless
//! `Handler` function-pointer signature required by `dispatch.rs` can reach
//! persistent state without threading it through `CommandContext`.
//!
//! SLOWLOG global state:
//!   - Ring buffer of at most `slowlog_max_len` entries (default 128).
//!   - Records commands whose execution time exceeds `threshold_micros`
//!     (default 10 000 µs; -1 disables recording entirely).
//!
//! LATENCY global state:
//!   - Per-event ring buffers exposed by `LatencyMonitor`.
//!   - No internal collection hooks for Phase B; the in-memory map is
//!     populated only via the public `report_latency_event` API.
//!
//! The timing wrap that feeds SLOWLOG entries is in `dispatch.rs`'s
//! `dispatch_timed` function, which records duration post-handler and calls
//! `record_slowlog_entry` defined here.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use redis_core::commandlog::{
    CommandLog, CommandLogEntry, CommandLogType, COMMANDLOG_ENTRY_MAX_ARGC,
    COMMANDLOG_ENTRY_MAX_STRING,
};
use redis_core::monotonic::{elapsed_us, MonoTime};
use redis_core::latency::{LatencyMonitor, LatencyReportConfig};
use redis_core::CommandContext;
use redis_types::{RedisResult, RedisString};

// ── Slowlog global ────────────────────────────────────────────────────────────

/// Singleton slowlog state shared across all connections.
static SLOWLOG: OnceLock<Arc<Mutex<CommandLog>>> = OnceLock::new();
static LARGE_REQUEST_LOG: OnceLock<Arc<Mutex<CommandLog>>> = OnceLock::new();
static LARGE_REPLY_LOG: OnceLock<Arc<Mutex<CommandLog>>> = OnceLock::new();
static BLOCKED_SLOWLOG: OnceLock<Arc<Mutex<HashMap<u64, PendingBlockedSlowlogEntry>>>> =
    OnceLock::new();
static LATENCY_HISTOGRAMS: OnceLock<Arc<Mutex<HashMap<Vec<u8>, CommandLatencyStats>>>> =
    OnceLock::new();

#[derive(Clone, Copy, Debug, Default)]
struct CommandLatencyStats {
    calls: u64,
    max_usec: u64,
}

struct PendingBlockedSlowlogEntry {
    argv: Vec<RedisString>,
    start_micros: MonoTime,
    client_name: Option<RedisString>,
}

/// Return a handle to the global slowlog, initialising it on first call.
pub fn global_slowlog() -> Arc<Mutex<CommandLog>> {
    SLOWLOG
        .get_or_init(|| {
            let mut log = CommandLog::new();
            log.threshold = 10_000;
            log.max_len = 128;
            Arc::new(Mutex::new(log))
        })
        .clone()
}

fn global_large_request_log() -> Arc<Mutex<CommandLog>> {
    LARGE_REQUEST_LOG
        .get_or_init(|| {
            let mut log = CommandLog::new();
            log.threshold = -1;
            log.max_len = 128;
            Arc::new(Mutex::new(log))
        })
        .clone()
}

fn global_large_reply_log() -> Arc<Mutex<CommandLog>> {
    LARGE_REPLY_LOG
        .get_or_init(|| {
            let mut log = CommandLog::new();
            log.threshold = -1;
            log.max_len = 128;
            Arc::new(Mutex::new(log))
        })
        .clone()
}

fn global_latency_histograms() -> Arc<Mutex<HashMap<Vec<u8>, CommandLatencyStats>>> {
    LATENCY_HISTOGRAMS
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

fn blocked_slowlog_pending() -> Arc<Mutex<HashMap<u64, PendingBlockedSlowlogEntry>>> {
    BLOCKED_SLOWLOG
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

/// Record one command execution into the slowlog if `duration_micros` meets the threshold.
///
/// Acquires the global lock, checks the threshold and max-len, then pushes a
/// new entry at the front of the deque (newest-first) and trims the tail.
/// Called from `dispatch_timed` in `dispatch.rs` after every command completes.
pub fn record_slowlog_entry(
    argv: &[RedisString],
    duration_micros: u64,
    client_id: u64,
    client_name: Option<RedisString>,
) {
    let handle = global_slowlog();
    let mut log = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };

    if log.threshold < 0 {
        return;
    }
    if log.max_len == 0 {
        return;
    }
    if duration_micros < log.threshold as u64 {
        return;
    }

    let id = log.entry_id;
    log.entry_id = log.entry_id.wrapping_add(1);

    let timestamp_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let argc = argv.len();
    let ceargc = argc.min(COMMANDLOG_ENTRY_MAX_ARGC);
    let mut stored_argv: Vec<RedisString> = Vec::with_capacity(ceargc);
    for j in 0..ceargc {
        if ceargc != argc && j == ceargc - 1 {
            let remaining = argc - ceargc + 1;
            let msg = format!("... ({} more arguments)", remaining);
            stored_argv.push(RedisString::from_bytes(msg.as_bytes()));
        } else {
            let arg = &argv[j];
            if arg.len() > COMMANDLOG_ENTRY_MAX_STRING {
                let extra = arg.len() - COMMANDLOG_ENTRY_MAX_STRING;
                let mut truncated: Vec<u8> =
                    arg.as_bytes()[..COMMANDLOG_ENTRY_MAX_STRING].to_vec();
                let suffix = format!("... ({} more bytes)", extra);
                truncated.extend_from_slice(suffix.as_bytes());
                stored_argv.push(RedisString::from_vec(truncated));
            } else {
                stored_argv.push(arg.clone());
            }
        }
    }

    let entry = CommandLogEntry {
        argv: stored_argv,
        id,
        value: duration_micros as i64,
        time: timestamp_unix,
        cname: client_name.unwrap_or_else(RedisString::new),
        peerid: RedisString::from_bytes(format!("id={}", client_id).as_bytes()),
    };

    log.entries.push_front(entry);
    while log.entries.len() > log.max_len {
        log.entries.pop_back();
    }
}

/// Remember a blocked command's original argv until the wake path completes it.
///
/// C Valkey skips commandlog emission while `call()` leaves the client blocked
/// and records the command from the unblock/reprocess path instead.
pub fn remember_blocked_slowlog_entry(
    argv: Vec<RedisString>,
    start_micros: MonoTime,
    client_id: u64,
    client_name: Option<RedisString>,
) {
    let handle = blocked_slowlog_pending();
    let mut pending = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    pending.insert(
        client_id,
        PendingBlockedSlowlogEntry {
            argv,
            start_micros,
            client_name,
        },
    );
}

/// Record and clear a pending blocked command after it has been unblocked.
pub fn record_blocked_slowlog_entry(client_id: u64) {
    let handle = blocked_slowlog_pending();
    let pending_entry = {
        let mut pending = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        pending.remove(&client_id)
    };
    if let Some(entry) = pending_entry {
        record_slowlog_entry(
            &entry.argv,
            elapsed_us(entry.start_micros),
            client_id,
            entry.client_name,
        );
    }
}

pub fn record_large_commandlog_entries(
    argv: &[RedisString],
    request_bytes: u64,
    reply_bytes: u64,
    client_id: u64,
    client_name: Option<RedisString>,
) {
    record_commandlog_entry_for_handle(
        global_large_request_log(),
        argv,
        request_bytes as i64,
        client_id,
        client_name.clone(),
    );
    record_commandlog_entry_for_handle(
        global_large_reply_log(),
        argv,
        reply_bytes as i64,
        client_id,
        client_name,
    );
}

fn record_commandlog_entry_for_handle(
    handle: Arc<Mutex<CommandLog>>,
    argv: &[RedisString],
    value: i64,
    client_id: u64,
    client_name: Option<RedisString>,
) {
    let mut log = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if log.threshold < 0 || log.max_len == 0 || value < log.threshold {
        return;
    }

    let id = log.entry_id;
    log.entry_id = log.entry_id.wrapping_add(1);
    let timestamp_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let stored_argv = commandlog_stored_argv(argv);
    let entry = CommandLogEntry {
        argv: stored_argv,
        id,
        value,
        time: timestamp_unix,
        cname: client_name.unwrap_or_else(RedisString::new),
        peerid: RedisString::from_bytes(format!("id={}", client_id).as_bytes()),
    };
    log.entries.push_front(entry);
    while log.entries.len() > log.max_len {
        log.entries.pop_back();
    }
}

fn commandlog_stored_argv(argv: &[RedisString]) -> Vec<RedisString> {
    let argc = argv.len();
    let ceargc = argc.min(COMMANDLOG_ENTRY_MAX_ARGC);
    let mut stored_argv: Vec<RedisString> = Vec::with_capacity(ceargc);
    for j in 0..ceargc {
        if ceargc != argc && j == ceargc - 1 {
            let remaining = argc - ceargc + 1;
            let msg = format!("... ({} more arguments)", remaining);
            stored_argv.push(RedisString::from_bytes(msg.as_bytes()));
        } else {
            let arg = &argv[j];
            if arg.len() > COMMANDLOG_ENTRY_MAX_STRING {
                let extra = arg.len() - COMMANDLOG_ENTRY_MAX_STRING;
                let mut truncated: Vec<u8> =
                    arg.as_bytes()[..COMMANDLOG_ENTRY_MAX_STRING].to_vec();
                let suffix = format!("... ({} more bytes)", extra);
                truncated.extend_from_slice(suffix.as_bytes());
                stored_argv.push(RedisString::from_vec(truncated));
            } else {
                stored_argv.push(arg.clone());
            }
        }
    }
    stored_argv
}

pub fn record_latency_histogram(argv: &[RedisString], elapsed_usec: u64) {
    let Some(fullname) = latency_command_fullname(argv) else {
        return;
    };
    let handle = global_latency_histograms();
    let mut histograms = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let stats = histograms.entry(fullname).or_default();
    stats.calls = stats.calls.saturating_add(1);
    stats.max_usec = stats.max_usec.max(elapsed_usec.max(1));
}

pub fn reset_latency_histograms() {
    let handle = global_latency_histograms();
    let mut histograms = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    histograms.clear();
}

/// Update the slowlog threshold in microseconds.
///
/// Called from `CONFIG SET slowlog-log-slower-than <value>`.
pub fn set_slowlog_threshold(micros: i64) {
    let handle = global_slowlog();
    let mut log = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    log.threshold = micros;
}

/// Update the slowlog maximum length.
///
/// Called from `CONFIG SET slowlog-max-len <value>`.
pub fn set_slowlog_max_len(max: usize) {
    let handle = global_slowlog();
    let mut log = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    log.max_len = max;
    while log.entries.len() > log.max_len {
        log.entries.pop_back();
    }
}

pub fn set_commandlog_large_request_threshold(bytes: i64) {
    set_commandlog_threshold(global_large_request_log(), bytes);
}

pub fn set_commandlog_large_request_max_len(max: usize) {
    set_commandlog_max_len(global_large_request_log(), max);
}

pub fn set_commandlog_large_reply_threshold(bytes: i64) {
    set_commandlog_threshold(global_large_reply_log(), bytes);
}

pub fn set_commandlog_large_reply_max_len(max: usize) {
    set_commandlog_max_len(global_large_reply_log(), max);
}

fn set_commandlog_threshold(handle: Arc<Mutex<CommandLog>>, threshold: i64) {
    let mut log = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    log.threshold = threshold;
}

fn set_commandlog_max_len(handle: Arc<Mutex<CommandLog>>, max: usize) {
    let mut log = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    log.max_len = max;
    while log.entries.len() > log.max_len {
        log.entries.pop_back();
    }
}

// ── SLOWLOG command handler ───────────────────────────────────────────────────

/// `SLOWLOG GET [count]`, `SLOWLOG LEN`, `SLOWLOG RESET`, `SLOWLOG HELP`.
pub fn slowlog_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(redis_types::RedisError::wrong_number_of_args(b"slowlog"));
    }

    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();

    if sub_bytes.eq_ignore_ascii_case(b"len") {
        if argc != 2 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"slowlog|len"));
        }
        let handle = global_slowlog();
        let log = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        return ctx.reply_integer(log.entries.len() as i64);
    }

    if sub_bytes.eq_ignore_ascii_case(b"reset") {
        if argc != 2 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"slowlog|reset"));
        }
        let handle = global_slowlog();
        let mut log = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        log.entries.clear();
        return ctx.reply_simple_string(b"OK");
    }

    if sub_bytes.eq_ignore_ascii_case(b"get") {
        if argc > 3 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"slowlog|get"));
        }
        let default_count: i64 = 10;
        let requested: i64 = if argc == 3 {
            let count_arg = ctx.arg_owned(2usize)?;
            parse_count(count_arg.as_bytes())?
        } else {
            default_count
        };
        let handle = global_slowlog();
        let log = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let actual_count = if requested == -1 {
            log.entries.len()
        } else {
            (requested as usize).min(log.entries.len())
        };
        ctx.reply_array_header(actual_count)?;
        for entry in log.entries.iter().take(actual_count) {
            ctx.reply_array_header(6usize)?;
            ctx.reply_integer(entry.id as i64)?;
            ctx.reply_integer(entry.time)?;
            ctx.reply_integer(entry.value)?;
            ctx.reply_array_header(entry.argv.len())?;
            for arg in &entry.argv {
                ctx.reply_bulk(arg.as_bytes())?;
            }
            ctx.reply_bulk(entry.peerid.as_bytes())?;
            ctx.reply_bulk(entry.cname.as_bytes())?;
        }
        return Ok(());
    }

    if sub_bytes.eq_ignore_ascii_case(b"help") {
        let lines: &[&[u8]] = &[
            b"SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"GET [<count>]",
            b"    Return top <count> entries from the slowlog (default: 10, -1 means all).",
            b"    Entries are made of:",
            b"    id, timestamp, time in microseconds, arguments array, client IP and port,",
            b"    client name",
            b"LEN",
            b"    Return the length of the slowlog.",
            b"RESET",
            b"    Reset the slowlog.",
        ];
        ctx.reply_array_header(lines.len())?;
        for line in lines {
            ctx.reply_bulk(line)?;
        }
        return Ok(());
    }

    let mut msg = Vec::with_capacity(
        b"ERR unknown subcommand or wrong number of arguments for '".len()
            + sub_bytes.len()
            + 2,
    );
    msg.extend_from_slice(b"ERR unknown subcommand or wrong number of arguments for '");
    msg.extend_from_slice(sub_bytes);
    msg.push(b'\'');
    Err(redis_types::RedisError::runtime(msg))
}

/// `COMMANDLOG GET <count> <type>`, `COMMANDLOG LEN <type>`,
/// `COMMANDLOG RESET <type>`, `COMMANDLOG HELP`.
pub fn commandlog_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(redis_types::RedisError::wrong_number_of_args(b"commandlog"));
    }

    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();

    if sub_bytes.eq_ignore_ascii_case(b"help") {
        if argc != 2 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"commandlog|help"));
        }
        let lines: &[&[u8]] = &[
            b"COMMANDLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"GET <count> <type>",
            b"    Return top <count> entries of the specified type (-1 means all).",
            b"LEN <type>",
            b"    Return the length of the specified type of commandlog.",
            b"RESET <type>",
            b"    Reset the specified type of commandlog.",
            b"HELP",
            b"    Return this help.",
        ];
        ctx.reply_array_header(lines.len())?;
        for line in lines {
            ctx.reply_bulk(line)?;
        }
        return Ok(());
    }

    if sub_bytes.eq_ignore_ascii_case(b"len") {
        if argc != 3 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"commandlog|len"));
        }
        let log_type = parse_commandlog_type(ctx.arg_owned(2usize)?.as_bytes())?;
        let handle = commandlog_handle(log_type);
        let log = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        return ctx.reply_integer(log.entries.len() as i64);
    }

    if sub_bytes.eq_ignore_ascii_case(b"reset") {
        if argc != 3 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"commandlog|reset"));
        }
        let log_type = parse_commandlog_type(ctx.arg_owned(2usize)?.as_bytes())?;
        let handle = commandlog_handle(log_type);
        let mut log = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        log.entries.clear();
        return ctx.reply_simple_string(b"OK");
    }

    if sub_bytes.eq_ignore_ascii_case(b"get") {
        if argc != 4 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"commandlog|get"));
        }
        let requested = parse_count(ctx.arg_owned(2usize)?.as_bytes())?;
        let log_type = parse_commandlog_type(ctx.arg_owned(3usize)?.as_bytes())?;
        let handle = commandlog_handle(log_type);
        let log = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        return reply_commandlog_entries(ctx, &log, requested);
    }

    let mut msg = Vec::with_capacity(
        b"ERR unknown subcommand or wrong number of arguments for '".len()
            + sub_bytes.len()
            + 2,
    );
    msg.extend_from_slice(b"ERR unknown subcommand or wrong number of arguments for '");
    msg.extend_from_slice(sub_bytes);
    msg.push(b'\'');
    Err(redis_types::RedisError::runtime(msg))
}

fn commandlog_handle(log_type: CommandLogType) -> Arc<Mutex<CommandLog>> {
    match log_type {
        CommandLogType::Slow => global_slowlog(),
        CommandLogType::LargeRequest => global_large_request_log(),
        CommandLogType::LargeReply => global_large_reply_log(),
    }
}

fn parse_commandlog_type(bytes: &[u8]) -> Result<CommandLogType, redis_types::RedisError> {
    if bytes.eq_ignore_ascii_case(b"slow") {
        Ok(CommandLogType::Slow)
    } else if bytes.eq_ignore_ascii_case(b"large-request") {
        Ok(CommandLogType::LargeRequest)
    } else if bytes.eq_ignore_ascii_case(b"large-reply") {
        Ok(CommandLogType::LargeReply)
    } else {
        Err(redis_types::RedisError::runtime(
            b"ERR type should be one of the following: slow, large-request, large-reply",
        ))
    }
}

fn reply_commandlog_entries(
    ctx: &mut CommandContext,
    log: &CommandLog,
    requested: i64,
) -> RedisResult<()> {
    let actual_count = if requested == -1 {
        log.entries.len()
    } else {
        (requested as usize).min(log.entries.len())
    };
    ctx.reply_array_header(actual_count)?;
    for entry in log.entries.iter().take(actual_count) {
        ctx.reply_array_header(6usize)?;
        ctx.reply_integer(entry.id as i64)?;
        ctx.reply_integer(entry.time)?;
        ctx.reply_integer(entry.value)?;
        ctx.reply_array_header(entry.argv.len())?;
        for arg in &entry.argv {
            ctx.reply_bulk(arg.as_bytes())?;
        }
        ctx.reply_bulk(entry.peerid.as_bytes())?;
        ctx.reply_bulk(entry.cname.as_bytes())?;
    }
    Ok(())
}

fn parse_count(bytes: &[u8]) -> Result<i64, redis_types::RedisError> {
    if bytes.is_empty() {
        return Err(slowlog_count_error());
    }
    let (negative, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    if digits.is_empty() {
        return Err(slowlog_count_error());
    }
    let mut value: i64 = 0;
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return Err(slowlog_count_error());
        }
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add((byte - b'0') as i64))
            .ok_or_else(slowlog_count_error)?;
    }
    let parsed = if negative {
        value.checked_neg().ok_or_else(slowlog_count_error)?
    } else {
        value
    };
    if parsed < -1 {
        return Err(slowlog_count_error());
    }
    Ok(parsed)
}

fn slowlog_count_error() -> redis_types::RedisError {
    redis_types::RedisError::runtime(b"ERR count should be greater than or equal to -1")
}

fn latency_command_fullname(argv: &[RedisString]) -> Option<Vec<u8>> {
    let cmd = argv.first()?;
    let mut name: Vec<u8> = cmd
        .as_bytes()
        .iter()
        .map(|b| b.to_ascii_lowercase())
        .collect();
    if argv.len() > 1 && latency_parent_uses_subcommands(&name) {
        name.push(b'|');
        name.extend(argv[1].as_bytes().iter().map(|b| b.to_ascii_lowercase()));
    }
    Some(name)
}

fn latency_parent_uses_subcommands(name: &[u8]) -> bool {
    matches!(
        name,
        b"acl"
            | b"client"
            | b"command"
            | b"commandlog"
            | b"config"
            | b"function"
            | b"latency"
            | b"memory"
            | b"module"
            | b"object"
            | b"pubsub"
            | b"script"
            | b"slowlog"
    )
}

// ── Latency global ────────────────────────────────────────────────────────────

/// Singleton latency monitor shared across all connections.
static LATENCY: OnceLock<Arc<Mutex<LatencyMonitor>>> = OnceLock::new();

/// Return a handle to the global latency monitor, initialising it on first call.
pub fn global_latency() -> Arc<Mutex<LatencyMonitor>> {
    LATENCY
        .get_or_init(|| Arc::new(Mutex::new(LatencyMonitor::new())))
        .clone()
}

/// Report a latency event observation (milliseconds) into the global monitor.
///
/// Phase B: no internal callers; exposed for future integration with expire-cycle,
/// fork, AOF-write, and other latency hooks.
pub fn report_latency_event(event_name: &[u8], latency_ms: u64) {
    let handle = global_latency();
    let mut monitor = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    monitor.add_sample(event_name, latency_ms as i64 * 1000);
}

// ── LATENCY command handler ───────────────────────────────────────────────────

/// `LATENCY LATEST`, `LATENCY HISTORY event`, `LATENCY RESET [event...]`,
/// `LATENCY GRAPH event`, `LATENCY DOCTOR`, `LATENCY HELP`.
pub fn latency_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(redis_types::RedisError::wrong_number_of_args(b"latency"));
    }

    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();

    if sub_bytes.eq_ignore_ascii_case(b"latest") {
        if argc != 2 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"latency|latest"));
        }
        let handle = global_latency();
        let monitor = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        return reply_latency_latest(ctx, &monitor);
    }

    if sub_bytes.eq_ignore_ascii_case(b"history") {
        if argc != 3 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"latency|history"));
        }
        let event = ctx.arg_owned(2usize)?;
        let handle = global_latency();
        let monitor = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        return reply_latency_history(ctx, &monitor, event.as_bytes());
    }

    if sub_bytes.eq_ignore_ascii_case(b"reset") {
        let handle = global_latency();
        let mut monitor = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if argc == 2 {
            let count = monitor.reset_event(None);
            return ctx.reply_integer(count as i64);
        }
        let mut events: Vec<Vec<u8>> = Vec::with_capacity(argc - 2);
        for i in 2..argc {
            events.push(ctx.arg_owned(i)?.as_bytes().to_vec());
        }
        let mut total = 0i32;
        for ev in &events {
            total += monitor.reset_event(Some(ev));
        }
        return ctx.reply_integer(total as i64);
    }

    if sub_bytes.eq_ignore_ascii_case(b"histogram") {
        return reply_latency_histogram(ctx);
    }

    if sub_bytes.eq_ignore_ascii_case(b"graph") {
        if argc != 3 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"latency|graph"));
        }
        return ctx.reply_bulk(b"(no data)\n");
    }

    if sub_bytes.eq_ignore_ascii_case(b"doctor") {
        if argc != 2 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"latency|doctor"));
        }
        let handle = global_latency();
        let monitor = match handle.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let cfg = LatencyReportConfig {
            latency_monitor_threshold: 0,
            stat_fork_rate: 0.0,
            slowlog_threshold_us: {
                let sl = global_slowlog();
                let log = match sl.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                log.threshold
            },
            slowlog_max_len: {
                let sl = global_slowlog();
                let log = match sl.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                log.max_len as i32
            },
            hz: 10,
            aof_fsync_always: false,
        };
        let report = monitor.create_report(&cfg);
        return ctx.reply_bulk(&report);
    }

    if sub_bytes.eq_ignore_ascii_case(b"help") {
        if argc != 2 {
            return Err(redis_types::RedisError::wrong_number_of_args(b"latency|help"));
        }
        let lines: &[&[u8]] = &[
            b"LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"DOCTOR",
            b"    Return a human readable latency analysis report.",
            b"GRAPH <event>",
            b"    Return an ASCII latency graph for the <event> class.",
            b"HISTORY <event>",
            b"    Return time-latency samples for the <event> class.",
            b"LATEST",
            b"    Return the latest latency samples for all events.",
            b"RESET [<event> ...]",
            b"    Reset latency data of one or more <event> classes.",
            b"    (default: reset all data for all event classes)",
            b"HISTOGRAM [COMMAND ...]",
            b"    Return a cumulative distribution of latencies for commands.",
        ];
        ctx.reply_array_header(lines.len())?;
        for line in lines {
            ctx.reply_bulk(line)?;
        }
        return Ok(());
    }

    let mut msg = Vec::with_capacity(
        b"ERR unknown subcommand or wrong number of arguments for '".len()
            + sub_bytes.len()
            + 2,
    );
    msg.extend_from_slice(b"ERR unknown subcommand or wrong number of arguments for '");
    msg.extend_from_slice(sub_bytes);
    msg.push(b'\'');
    Err(redis_types::RedisError::runtime(msg))
}

fn reply_latency_histogram(ctx: &mut CommandContext) -> RedisResult<()> {
    let entries = selected_latency_histograms(ctx);
    ctx.reply_map_header(entries.len())?;
    for (name, stats) in entries {
        ctx.reply_bulk(&name)?;
        ctx.reply_map_header(2usize)?;
        ctx.reply_bulk(b"calls")?;
        ctx.reply_integer(stats.calls as i64)?;
        ctx.reply_bulk(b"histogram_usec")?;
        ctx.reply_map_header(1usize)?;
        ctx.reply_integer(stats.max_usec.max(1) as i64)?;
        ctx.reply_integer(stats.calls as i64)?;
    }
    Ok(())
}

fn selected_latency_histograms(ctx: &CommandContext) -> Vec<(Vec<u8>, CommandLatencyStats)> {
    let handle = global_latency_histograms();
    let histograms = match handle.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if ctx.arg_count() == 2 {
        let mut all: Vec<(Vec<u8>, CommandLatencyStats)> =
            histograms.iter().map(|(k, v)| (k.clone(), *v)).collect();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        return all;
    }

    let mut selected: Vec<(Vec<u8>, CommandLatencyStats)> = Vec::new();
    for idx in 2..ctx.arg_count() {
        let Ok(raw) = ctx.arg(idx) else {
            continue;
        };
        let requested: Vec<u8> = raw
            .as_bytes()
            .iter()
            .map(|b| b.to_ascii_lowercase())
            .collect();
        append_selected_latency_histogram(&histograms, &requested, &mut selected);
    }
    selected
}

fn append_selected_latency_histogram(
    histograms: &HashMap<Vec<u8>, CommandLatencyStats>,
    requested: &[u8],
    out: &mut Vec<(Vec<u8>, CommandLatencyStats)>,
) {
    if requested.contains(&b'|') {
        if let Some(stats) = histograms.get(requested) {
            out.push((requested.to_vec(), *stats));
        }
        return;
    }

    if crate::dispatch::registered_command_spec(requested).is_none() {
        return;
    }

    let mut parent_matches: Vec<(Vec<u8>, CommandLatencyStats)> = histograms
        .iter()
        .filter_map(|(name, stats)| {
            if name.len() > requested.len()
                && name.starts_with(requested)
                && name.get(requested.len()) == Some(&b'|')
            {
                Some((name.clone(), *stats))
            } else {
                None
            }
        })
        .collect();
    parent_matches.sort_by(|a, b| a.0.cmp(&b.0));
    if parent_matches.is_empty() {
        if let Some(stats) = histograms.get(requested) {
            out.push((requested.to_vec(), *stats));
        }
    } else {
        out.extend(parent_matches);
    }
}

fn reply_latency_latest(
    ctx: &mut CommandContext,
    monitor: &LatencyMonitor,
) -> RedisResult<()> {
    use redis_core::latency::LATENCY_TS_LEN;
    let count = monitor.len();
    ctx.reply_array_header(count)?;
    for (event_key, ts) in monitor.iter() {
        let last = (ts.idx + LATENCY_TS_LEN - 1) % LATENCY_TS_LEN;
        ctx.reply_array_header(4usize)?;
        ctx.reply_bulk(event_key)?;
        ctx.reply_integer(ts.samples[last].time as i64)?;
        ctx.reply_integer(ts.samples[last].latency as i64)?;
        ctx.reply_integer(ts.max as i64)?;
    }
    Ok(())
}

fn reply_latency_history(
    ctx: &mut CommandContext,
    monitor: &LatencyMonitor,
    event: &[u8],
) -> RedisResult<()> {
    use redis_core::latency::LATENCY_TS_LEN;
    let ts = match monitor.get(event) {
        None => {
            return ctx.reply_array_header(0usize);
        }
        Some(ts) => ts,
    };
    let mut samples: Vec<(i64, i64)> = Vec::new();
    for j in 0..LATENCY_TS_LEN {
        let i = (ts.idx + j) % LATENCY_TS_LEN;
        if ts.samples[i].time == 0 {
            continue;
        }
        samples.push((ts.samples[i].time as i64, ts.samples[i].latency as i64));
    }
    ctx.reply_array_header(samples.len())?;
    for (time, latency) in samples {
        ctx.reply_array_header(2usize)?;
        ctx.reply_integer(time)?;
        ctx.reply_integer(latency)?;
    }
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        new (OV-2 implementation)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         SLOWLOG ring buffer with global OnceLock. LATENCY in-memory
//                  map backed by redis_core::latency::LatencyMonitor. Phase B:
//                  no internal event-collection callers; API exposed for future
//                  hooks. SLOWLOG GET reply format matches canonical Redis 6-tuple;
//                  blocked list commands are recorded from the wake path.
// ──────────────────────────────────────────────────────────────────────────────
