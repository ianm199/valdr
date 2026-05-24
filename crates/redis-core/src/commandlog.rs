//! Command log — slow query log and large request/reply log.
//!
//! Merges `src/commandlog.c` (276 lines, 8 functions) and `src/commandlog.h`.
//!
//! Records recent commands that exceeded configurable thresholds: execution
//! time in microseconds (Slow), input payload size in bytes (LargeRequest),
//! output payload size in bytes (LargeReply). Three independent logs share
//! the same entry structure and differ only in the metric they track.
//!
//! Results are accessible via the `COMMANDLOG` command (all types) and the
//! legacy `SLOWLOG` alias (slow-type only).
//!
//! ## Integration note
//!
//! The C code stores `server.commandlog[COMMANDLOG_TYPE_NUM]` on the global
//! `redisServer` struct. In Rust, `RedisServer` is a canonical type owned by
//! `crates/redis-core/src/server.rs`. Adding the `commandlog` field there is
//! a Phase 3 architect decision (see `TODO(architect)` below). Until then,
//! the public functions accept `&mut [CommandLog; CommandLogType::NUM]` as an
//! explicit parameter.

use crate::command_context::CommandContext;
use redis_types::{RedisError, RedisResult, RedisString};
use std::collections::VecDeque;

// ── Constants ─────────────────────────────────────────────────────────────
// C: commandlog.h:35-36

/// Maximum number of arguments stored per entry; excess args are replaced with
/// a single truncation descriptor in the last slot.
pub const COMMANDLOG_ENTRY_MAX_ARGC: usize = 32;

/// Maximum argument byte length stored per entry; longer args are truncated
/// and suffixed with a byte-count descriptor.
pub const COMMANDLOG_ENTRY_MAX_STRING: usize = 128;

/// Value substituted for arguments that must be redacted (e.g., passwords).
/// C: `shared.redacted` — a shared `robj` containing `(redacted)`.
/// TODO(port): wire up `clientCommandArgShouldBeRedacted` to actually use this.
const REDACTED_MARKER: &[u8] = b"(redacted)";

// ── CommandLogType ────────────────────────────────────────────────────────

/// Category of a command log entry.
///
/// C: `COMMANDLOG_TYPE_SLOW` (0), `COMMANDLOG_TYPE_LARGE_REQUEST` (1),
///    `COMMANDLOG_TYPE_LARGE_REPLY` (2), `COMMANDLOG_TYPE_NUM` (3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum CommandLogType {
    Slow = 0,
    LargeRequest = 1,
    LargeReply = 2,
}

impl CommandLogType {
    /// Total number of log types; matches `COMMANDLOG_TYPE_NUM` in C.
    pub const NUM: usize = 3;

    /// Convert the enum variant to its array index.
    #[inline]
    pub fn as_index(self) -> usize {
        self as usize
    }

    /// Parse a log-type name from a byte string (case-insensitive).
    /// Returns `None` for unrecognised names.
    ///
    /// C: `commandlogGetTypeOrReply` (commandlog.c:218-224) — type-parsing half.
    fn from_bytes(s: &[u8]) -> Option<Self> {
        if s.eq_ignore_ascii_case(b"slow") {
            Some(Self::Slow)
        } else if s.eq_ignore_ascii_case(b"large-request") {
            Some(Self::LargeRequest)
        } else if s.eq_ignore_ascii_case(b"large-reply") {
            Some(Self::LargeReply)
        } else {
            None
        }
    }
}

// ── CommandLogEntry ───────────────────────────────────────────────────────

/// A single captured entry in the command log.
///
/// C: `commandlogEntry` (commandlog.h:39-47)
#[derive(Debug, Clone)]
pub struct CommandLogEntry {
    /// Recorded command arguments, possibly truncated.
    /// If truncated, `argv[COMMANDLOG_ENTRY_MAX_ARGC - 1]` is a descriptor
    /// of the form `"... (N more arguments)"`.
    pub argv: Vec<RedisString>,

    /// Unique monotonically increasing identifier within this log type.
    ///
    /// C: `ce->id` (`long long`)
    pub id: u64,

    /// Metric value:
    /// - `Slow`: microseconds of execution time.
    /// - `LargeRequest`: input bytes for the command.
    /// - `LargeReply`: output bytes for the command.
    ///
    /// C: `ce->value` (`long long`)
    pub value: i64,

    /// Unix timestamp (seconds since epoch) at which the command was recorded.
    ///
    /// C: `ce->time` (`time_t`)
    pub time: i64,

    /// Connection name of the client (empty if unset).
    ///
    /// C: `ce->cname` (`sds`)
    pub cname: RedisString,

    /// Peer address string, e.g. `"127.0.0.1:12345"`.
    ///
    /// C: `ce->peerid` (`sds`)
    pub peerid: RedisString,
}

// ── CommandLog ────────────────────────────────────────────────────────────

/// Per-type command log: a bounded ring of recent entries plus configuration.
///
/// Mirrors the anonymous struct used in the C `server.commandlog[]` array.
///
/// TODO(architect): add `commandlog: [CommandLog; CommandLogType::NUM]` to
///   `RedisServer` in `crates/redis-core/src/server.rs`. Until that is done,
///   callers must hold and pass this array explicitly.
#[derive(Debug)]
pub struct CommandLog {
    /// Entries stored newest-first (front = most recent).
    ///
    /// C: `server.commandlog[type].entries` (`list *`)
    pub entries: VecDeque<CommandLogEntry>,

    /// Monotonically increasing counter that provides each entry's unique ID.
    ///
    /// C: `server.commandlog[type].entry_id`
    pub entry_id: u64,

    /// Entries with `value >= threshold` are recorded. Negative means disabled.
    ///
    /// C: `server.commandlog[type].threshold` (`long long`; -1 = disabled)
    pub threshold: i64,

    /// Maximum number of entries retained (oldest are dropped). 0 means disabled.
    ///
    /// C: `server.commandlog[type].max_len`
    pub max_len: usize,
}

impl CommandLog {
    /// Construct a new, empty command log. Thresholds must be configured
    /// separately via the config layer before entries will be recorded.
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            entry_id: 0,
            threshold: -1,
            max_len: 128,
        }
    }
}

impl Default for CommandLog {
    fn default() -> Self {
        Self::new()
    }
}

// ── CommandLogClientInfo ──────────────────────────────────────────────────

/// Snapshot of per-client metrics and identifiers needed when pushing a
/// command log entry.
///
/// The C code reads directly from `client *c`. A snapshot struct decouples
/// `commandlog.rs` from the still-evolving `Client` type.
///
/// PORT NOTE: introduced to avoid a hard dependency on `Client` fields that
/// do not exist in the current Phase A stub (`duration`,
/// `net_input_bytes_curr_cmd`, `net_output_bytes_curr_cmd`, `name`, `peerid`).
///
/// TODO(port): once `Client` gains those fields (Phase 3 networking), replace
///   this struct with a direct `&Client` parameter and remove the snapshot.
pub struct CommandLogClientInfo {
    /// Command argument vector. If the client rewrote argv, this should be the
    /// original pre-rewrite vector.
    ///
    /// C: `c->original_argv ? c->original_argv : c->argv`
    pub argv: Vec<RedisString>,

    /// Command execution duration in microseconds.
    ///
    /// C: `c->duration` (`long`)
    pub duration: i64,

    /// Network bytes received for this command.
    ///
    /// C: `c->net_input_bytes_curr_cmd` (`unsigned long long`)
    pub net_input_bytes: u64,

    /// Network bytes sent for this command.
    ///
    /// C: `c->net_output_bytes_curr_cmd` (`unsigned long long`)
    pub net_output_bytes: u64,

    /// Client peer address (e.g., `"127.0.0.1:12345"`).
    ///
    /// C: `getClientPeerId(c)` → `sds`
    pub peerid: RedisString,

    /// Client connection name (empty if unset).
    ///
    /// C: `c->name ? objectGetVal(c->name) : ""`
    pub cname: RedisString,
}

// ── Public API ────────────────────────────────────────────────────────────

/// Construct the initial array of command logs with disabled-by-default config.
/// The caller stores this result on `RedisServer`.
///
/// C: `commandlogInit` (commandlog.c:94-100) — mutated `server.commandlog[]`
/// in-place. Rust returns the initialised array so the caller owns it.
pub fn commandlog_init() -> [CommandLog; CommandLogType::NUM] {
    [CommandLog::new(), CommandLog::new(), CommandLog::new()]
}

/// Push a command log entry for the most recently executed command into each
/// applicable log type.
///
/// Checks each log's threshold and only records entries that meet or exceed it.
/// Safe to call for every command; disabled logs (`threshold < 0` or
/// `max_len == 0`) return immediately.
///
/// C: `commandlogPushCurrentCommand` (commandlog.c:147-172)
///
/// TODO(port): `cmd->flags & CMD_SKIP_COMMANDLOG` is not checked — needs
///   `CommandSpec` with flag bits surfaced through `CommandContext` (Phase 3).
///
/// TODO(port): `scriptIsRunning()` / `scriptGetCaller()` is not implemented —
///   scripting is deferred to Phase 7. The script-caller substitution
///   (using the outer caller's client info) is omitted; `info` must already
///   reflect the correct client for script contexts.
pub fn commandlog_push_current_command(
    logs: &mut [CommandLog; CommandLogType::NUM],
    info: &CommandLogClientInfo,
) {
    commandlog_push_entry_if_needed(
        &mut logs[CommandLogType::Slow.as_index()],
        &info.argv,
        info.duration,
        info.peerid.clone(),
        info.cname.clone(),
    );
    // PERF(port): net_input_bytes / net_output_bytes are u64; cast to i64 matches
    // the C implicit truncation (unsigned long long → long long). Harmless in
    // practice since byte counts won't realistically exceed i64::MAX.
    commandlog_push_entry_if_needed(
        &mut logs[CommandLogType::LargeRequest.as_index()],
        &info.argv,
        info.net_input_bytes as i64,
        info.peerid.clone(),
        info.cname.clone(),
    );
    commandlog_push_entry_if_needed(
        &mut logs[CommandLogType::LargeReply.as_index()],
        &info.argv,
        info.net_output_bytes as i64,
        info.peerid.clone(),
        info.cname.clone(),
    );
}

/// Handle the `SLOWLOG` command — the legacy alias for the slow-execution log.
///
/// Accepted subcommands: `GET [count]`, `LEN`, `RESET`, `HELP`.
///
/// C: `slowlogCommand` (commandlog.c:176-216)
///
/// TODO(architect): the `logs` parameter must be threaded through
///   `CommandContext` once `RedisServer` is wired into `CommandContext` in
///   Phase 3. The current signature is a Phase A placeholder.
pub fn slowlog_command(
    ctx: &mut CommandContext,
    logs: &mut [CommandLog; CommandLogType::NUM],
) -> RedisResult<()> {
    let argc = ctx.arg_count();

    // Clone subcommand bytes before any mutable borrows on ctx.
    let subcmd = ctx.arg(1)?.clone();
    let subcmd_bytes = subcmd.as_bytes();

    if argc == 2 && subcmd_bytes.eq_ignore_ascii_case(b"help") {
        let help_lines: &[&[u8]] = &[
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
        reply_help(ctx, help_lines)?;
    } else if argc == 2 && subcmd_bytes.eq_ignore_ascii_case(b"reset") {
        commandlog_reset(&mut logs[CommandLogType::Slow.as_index()]);
        ctx.reply_simple_string(b"OK")?;
    } else if argc == 2 && subcmd_bytes.eq_ignore_ascii_case(b"len") {
        let len = logs[CommandLogType::Slow.as_index()].entries.len();
        ctx.reply_integer(len as i64)?;
    } else if (argc == 2 || argc == 3) && subcmd_bytes.eq_ignore_ascii_case(b"get") {
        let mut count: i64 = 10;
        if argc == 3 {
            // Clone arg(2) before the mutable ctx borrow in reply helpers.
            let count_arg = ctx.arg(2)?.clone();
            count = parse_range_long(count_arg.as_bytes(), -1, i64::MAX).ok_or_else(|| {
                RedisError::runtime(b"count should be greater than or equal to -1")
            })?;
            if count == -1 {
                count = logs[CommandLogType::Slow.as_index()].entries.len() as i64;
            }
        }
        commandlog_get_reply(ctx, &logs[CommandLogType::Slow.as_index()], count)?;
    } else {
        // C: addReplySubcommandSyntaxError(c)
        return Err(RedisError::syntax(
            b"unknown subcommand or wrong number of arguments",
        ));
    }
    Ok(())
}

/// Handle the `COMMANDLOG` command — general log with subtype selection.
///
/// Accepted subcommands:
/// - `GET <count> <type>` — return entries.
/// - `LEN <type>` — return entry count.
/// - `RESET <type>` — clear the log.
/// - `HELP` — print usage.
///
/// `<type>` must be one of: `slow`, `large-request`, `large-reply`.
///
/// C: `commandlogCommand` (commandlog.c:228-275)
///
/// TODO(architect): the `logs` parameter must be threaded through
///   `CommandContext` once `RedisServer` is wired into `CommandContext` in
///   Phase 3. The current signature is a Phase A placeholder.
pub fn commandlog_command(
    ctx: &mut CommandContext,
    logs: &mut [CommandLog; CommandLogType::NUM],
) -> RedisResult<()> {
    let argc = ctx.arg_count();

    // Clone subcommand bytes before any mutable borrows on ctx.
    let subcmd = ctx.arg(1)?.clone();
    let subcmd_bytes = subcmd.as_bytes();

    if argc == 2 && subcmd_bytes.eq_ignore_ascii_case(b"help") {
        let help_lines: &[&[u8]] = &[
            b"GET <count> <type>",
            b"    Return top <count> entries of the specified <type> from the commandlog (-1 means all).",
            b"    Entries are made of:",
            b"    id, timestamp,",
            b"        time in microseconds for type of slow,",
            b"        or size in bytes for type of large-request,",
            b"        or size in bytes for type of large-reply",
            b"    arguments array, client IP and port,",
            b"    client name",
            b"LEN <type>",
            b"    Return the length of the specified type of commandlog.",
            b"RESET <type>",
            b"    Reset the specified type of commandlog.",
        ];
        reply_help(ctx, help_lines)?;
    } else if argc == 3 && subcmd_bytes.eq_ignore_ascii_case(b"reset") {
        let type_arg = ctx.arg(2)?.clone();
        let log_type = commandlog_parse_type(type_arg.as_bytes())?;
        commandlog_reset(&mut logs[log_type.as_index()]);
        ctx.reply_simple_string(b"OK")?;
    } else if argc == 3 && subcmd_bytes.eq_ignore_ascii_case(b"len") {
        let type_arg = ctx.arg(2)?.clone();
        let log_type = commandlog_parse_type(type_arg.as_bytes())?;
        let len = logs[log_type.as_index()].entries.len();
        ctx.reply_integer(len as i64)?;
    } else if argc == 4 && subcmd_bytes.eq_ignore_ascii_case(b"get") {
        // Clone both args before any mutable ctx borrows.
        let count_arg = ctx.arg(2)?.clone();
        let type_arg = ctx.arg(3)?.clone();
        let mut count = parse_range_long(count_arg.as_bytes(), -1, i64::MAX)
            .ok_or_else(|| RedisError::runtime(b"count should be greater than or equal to -1"))?;
        let log_type = commandlog_parse_type(type_arg.as_bytes())?;
        if count == -1 {
            count = logs[log_type.as_index()].entries.len() as i64;
        }
        commandlog_get_reply(ctx, &logs[log_type.as_index()], count)?;
    } else {
        // C: addReplySubcommandSyntaxError(c)
        return Err(RedisError::syntax(
            b"unknown subcommand or wrong number of arguments",
        ));
    }
    Ok(())
}

// ── Private helpers ───────────────────────────────────────────────────────

/// Construct a single command log entry from a command's argument vector.
///
/// Applies two truncation policies:
/// 1. If the argument count exceeds `COMMANDLOG_ENTRY_MAX_ARGC`, the last
///    slot becomes `"... (N more arguments)"`.
/// 2. If any individual argument exceeds `COMMANDLOG_ENTRY_MAX_STRING` bytes,
///    it is truncated and suffixed with `"... (N more bytes)"`.
///
/// `next_id` is incremented to produce a monotonically unique entry ID.
///
/// C: `commandlogCreateEntry` (commandlog.c:31-75)
fn commandlog_create_entry(
    argv: &[RedisString],
    value: i64,
    next_id: &mut u64,
    peerid: RedisString,
    cname: RedisString,
) -> CommandLogEntry {
    let argc = argv.len();
    let ceargc = argc.min(COMMANDLOG_ENTRY_MAX_ARGC);
    let mut ce_argv: Vec<RedisString> = Vec::with_capacity(ceargc);

    for j in 0..ceargc {
        // C: if (ceargc != argc && j == ceargc - 1) → truncation descriptor
        if ceargc != argc && j == ceargc - 1 {
            let remaining = argc - ceargc + 1;
            let msg = format!("... ({} more arguments)", remaining);
            ce_argv.push(RedisString::from_bytes(msg.as_bytes()));
        } else {
            // TODO(port): clientCommandArgShouldBeRedacted(c, j) equivalent is
            //   not yet ported. When it is, check here and push REDACTED_MARKER
            //   for sensitive argument positions (e.g., passwords in AUTH).
            let _ = REDACTED_MARKER; // suppress unused-constant warning until wired up

            let arg = &argv[j];
            if arg.len() > COMMANDLOG_ENTRY_MAX_STRING {
                // C: sdsnewlen(ptr, COMMANDLOG_ENTRY_MAX_STRING)
                //    + sdscatprintf("... (%lu more bytes)", extra)
                let extra = arg.len() - COMMANDLOG_ENTRY_MAX_STRING;
                let mut truncated: Vec<u8> = arg.as_bytes()[..COMMANDLOG_ENTRY_MAX_STRING].to_vec();
                let suffix = format!("... ({} more bytes)", extra);
                truncated.extend_from_slice(suffix.as_bytes());
                ce_argv.push(RedisString::from_vec(truncated));
            } else {
                // C: argv[j]->refcount == OBJ_SHARED_REFCOUNT → reuse shared obj
                //    else dupStringObject(argv[j])
                // Rust: shared/refcount management is gone; clone owns the bytes.
                ce_argv.push(arg.clone());
            }
        }
    }

    let id = *next_id;
    *next_id = next_id.wrapping_add(1);

    // C: ce->time = time(NULL)
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    CommandLogEntry {
        argv: ce_argv,
        id,
        value,
        time,
        cname,
        peerid,
    }
}

/// Push a new entry into `log` if `value` meets the threshold; trim to max_len.
///
/// Returns immediately when the log is disabled (`threshold < 0` or
/// `max_len == 0`).
///
/// C: `commandlogPushEntryIfNeeded` (commandlog.c:105-112)
fn commandlog_push_entry_if_needed(
    log: &mut CommandLog,
    argv: &[RedisString],
    value: i64,
    peerid: RedisString,
    cname: RedisString,
) {
    if log.threshold < 0 || log.max_len == 0 {
        return;
    }
    if value >= log.threshold {
        // C: listAddNodeHead — newest entry goes to the front.
        let entry = commandlog_create_entry(argv, value, &mut log.entry_id, peerid, cname);
        log.entries.push_front(entry);
    }
    // C: while (listLength > max_len) listDelNode(listLast(...))
    while log.entries.len() > log.max_len {
        log.entries.pop_back();
    }
}

/// Clear all entries from a command log.
///
/// C: `commandlogReset` (commandlog.c:115-117)
fn commandlog_reset(log: &mut CommandLog) {
    // C: while (listLength > 0) listDelNode(listLast(...))
    // VecDeque::clear is O(n) but equivalent; Drop handles element cleanup.
    log.entries.clear();
}

/// Write at most `count` command log entries to the client as a RESP array.
///
/// Each entry is a 6-element array:
/// `[id, timestamp, value, [args...], peerid, cname]`
///
/// `count` must be >= 0; pass the log length to return all entries.
///
/// C: `commandlogGetReply` (commandlog.c:120-144)
fn commandlog_get_reply(ctx: &mut CommandContext, log: &CommandLog, count: i64) -> RedisResult<()> {
    let actual_count = if count <= 0 {
        0_usize
    } else {
        (count as usize).min(log.entries.len())
    };

    // C: addReplyArrayLen(c, count)
    ctx.reply_array_header(actual_count)?;

    // C: listRewind + while (count--) { ln = listNext(&li); ce = ln->value; ... }
    for ce in log.entries.iter().take(actual_count) {
        // C: addReplyArrayLen(c, 6)
        ctx.reply_array_header(6)?;
        // C: addReplyLongLong(c, ce->id)
        ctx.reply_integer(ce.id as i64)?;
        // C: addReplyLongLong(c, ce->time)
        ctx.reply_integer(ce.time)?;
        // C: addReplyLongLong(c, ce->value)
        ctx.reply_integer(ce.value)?;
        // C: addReplyArrayLen(c, ce->argc); for j: addReplyBulk(c, ce->argv[j])
        ctx.reply_array_header(ce.argv.len())?;
        for arg in &ce.argv {
            ctx.reply_bulk(arg.as_bytes())?;
        }
        // C: addReplyBulkCBuffer(c, ce->peerid, sdslen(ce->peerid))
        ctx.reply_bulk(ce.peerid.as_bytes())?;
        // C: addReplyBulkCBuffer(c, ce->cname, sdslen(ce->cname))
        ctx.reply_bulk(ce.cname.as_bytes())?;
    }
    Ok(())
}

/// Parse a log-type name from bytes, returning an error if unrecognised.
///
/// C: `commandlogGetTypeOrReply` (commandlog.c:218-224) — error-returning half.
/// The function is split: parsing is here; error emission is done by `?`.
fn commandlog_parse_type(type_bytes: &[u8]) -> Result<CommandLogType, RedisError> {
    CommandLogType::from_bytes(type_bytes).ok_or_else(|| {
        RedisError::runtime(
            b"type should be one of the following: slow, large-request, large-reply",
        )
    })
}

/// Emit a RESP array of bulk strings, one per help line.
///
/// Mirrors `addReplyHelp(c, help[])` from `server.c`.
/// TODO(port): move to `CommandContext::reply_help` once `addReplyHelp` is
///   fully ported (it also handles RESP2 vs RESP3 formatting).
fn reply_help(ctx: &mut CommandContext, lines: &[&[u8]]) -> RedisResult<()> {
    ctx.reply_array_header(lines.len())?;
    for line in lines {
        ctx.reply_bulk(line)?;
    }
    Ok(())
}

/// Parse a decimal integer from bytes and validate it lies within `[min, max]`.
/// Returns `None` if the bytes are not a valid integer or fall outside the range.
///
/// Approximates `getRangeLongFromObjectOrReply` from `util.c` (not yet ported).
///
/// TODO(port): replace with `redis_core::util::get_range_long` once `util.c`
///   is translated and available.
fn parse_range_long(bytes: &[u8], min: i64, max: i64) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (negative, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    if digits.is_empty() {
        return None;
    }
    let mut val: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    if negative {
        val = val.checked_neg()?;
    }
    if val < min || val > max {
        return None;
    }
    Some(val)
}

// ── Unit tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn rs(s: &[u8]) -> RedisString {
        RedisString::from_bytes(s)
    }

    #[test]
    fn commandlog_init_produces_three_disabled_logs() {
        let logs = commandlog_init();
        assert_eq!(logs.len(), CommandLogType::NUM);
        for log in &logs {
            assert_eq!(log.threshold, -1);
            assert!(log.entries.is_empty());
            assert_eq!(log.entry_id, 0);
        }
    }

    #[test]
    fn push_entry_skips_when_disabled() {
        let mut log = CommandLog::new();
        log.threshold = -1;
        let argv = vec![rs(b"SET"), rs(b"key"), rs(b"value")];
        commandlog_push_entry_if_needed(&mut log, &argv, 1000, rs(b"127.0.0.1:1"), rs(b""));
        assert!(log.entries.is_empty());
    }

    #[test]
    fn push_entry_skips_below_threshold() {
        let mut log = CommandLog::new();
        log.threshold = 500;
        log.max_len = 10;
        let argv = vec![rs(b"GET"), rs(b"key")];
        commandlog_push_entry_if_needed(&mut log, &argv, 100, rs(b"127.0.0.1:1"), rs(b""));
        assert!(log.entries.is_empty());
    }

    #[test]
    fn push_entry_records_when_at_threshold() {
        let mut log = CommandLog::new();
        log.threshold = 100;
        log.max_len = 10;
        let argv = vec![rs(b"GET"), rs(b"mykey")];
        commandlog_push_entry_if_needed(&mut log, &argv, 100, rs(b"127.0.0.1:2"), rs(b"myconn"));
        assert_eq!(log.entries.len(), 1);
        let entry = &log.entries[0];
        assert_eq!(entry.value, 100);
        assert_eq!(entry.argv[1].as_bytes(), b"mykey");
        assert_eq!(entry.peerid.as_bytes(), b"127.0.0.1:2");
        assert_eq!(entry.cname.as_bytes(), b"myconn");
    }

    #[test]
    fn push_entry_trims_to_max_len() {
        let mut log = CommandLog::new();
        log.threshold = 0;
        log.max_len = 3;
        let argv = vec![rs(b"PING")];
        for _ in 0..5 {
            commandlog_push_entry_if_needed(&mut log, &argv, 1, rs(b"peer"), rs(b""));
        }
        assert_eq!(log.entries.len(), 3);
    }

    #[test]
    fn push_entry_newest_at_front() {
        let mut log = CommandLog::new();
        log.threshold = 0;
        log.max_len = 10;
        let argv1 = vec![rs(b"CMD1")];
        let argv2 = vec![rs(b"CMD2")];
        commandlog_push_entry_if_needed(&mut log, &argv1, 1, rs(b"peer"), rs(b""));
        commandlog_push_entry_if_needed(&mut log, &argv2, 2, rs(b"peer"), rs(b""));
        assert_eq!(log.entries[0].argv[0].as_bytes(), b"CMD2");
        assert_eq!(log.entries[1].argv[0].as_bytes(), b"CMD1");
    }

    #[test]
    fn create_entry_truncates_long_arg() {
        let long_val = vec![b'x'; COMMANDLOG_ENTRY_MAX_STRING + 10];
        let argv = vec![rs(b"SET"), rs(b"key"), RedisString::from_vec(long_val)];
        let mut id = 0u64;
        let entry = commandlog_create_entry(&argv, 999, &mut id, rs(b"peer"), rs(b""));
        let truncated = entry.argv[2].as_bytes();
        assert!(truncated.len() > COMMANDLOG_ENTRY_MAX_STRING);
        assert!(truncated.windows(3).any(|w| w == b"..."));
    }

    #[test]
    fn create_entry_truncates_argc() {
        let argv: Vec<RedisString> = (0..COMMANDLOG_ENTRY_MAX_ARGC + 5)
            .map(|i| rs(format!("arg{}", i).as_bytes()))
            .collect();
        let mut id = 0u64;
        let entry = commandlog_create_entry(&argv, 0, &mut id, rs(b"peer"), rs(b""));
        assert_eq!(entry.argv.len(), COMMANDLOG_ENTRY_MAX_ARGC);
        let last = entry.argv.last().unwrap().as_bytes();
        assert!(last.windows(3).any(|w| w == b"..."));
        assert!(last
            .windows(b"more arguments".len())
            .any(|w| w == b"more arguments"));
    }

    #[test]
    fn commandlog_reset_clears_entries() {
        let mut log = CommandLog::new();
        log.threshold = 0;
        log.max_len = 10;
        let argv = vec![rs(b"PING")];
        commandlog_push_entry_if_needed(&mut log, &argv, 1, rs(b"peer"), rs(b""));
        assert!(!log.entries.is_empty());
        commandlog_reset(&mut log);
        assert!(log.entries.is_empty());
    }

    #[test]
    fn parse_range_long_valid() {
        assert_eq!(parse_range_long(b"10", -1, i64::MAX), Some(10));
        assert_eq!(parse_range_long(b"-1", -1, i64::MAX), Some(-1));
        assert_eq!(parse_range_long(b"0", 0, 100), Some(0));
    }

    #[test]
    fn parse_range_long_invalid() {
        assert_eq!(parse_range_long(b"abc", -1, i64::MAX), None);
        assert_eq!(parse_range_long(b"-2", -1, i64::MAX), None);
        assert_eq!(parse_range_long(b"", -1, i64::MAX), None);
        assert_eq!(parse_range_long(b"101", 0, 100), None);
    }

    #[test]
    fn commandlog_type_from_bytes() {
        assert_eq!(
            CommandLogType::from_bytes(b"slow"),
            Some(CommandLogType::Slow)
        );
        assert_eq!(
            CommandLogType::from_bytes(b"SLOW"),
            Some(CommandLogType::Slow)
        );
        assert_eq!(
            CommandLogType::from_bytes(b"large-request"),
            Some(CommandLogType::LargeRequest)
        );
        assert_eq!(
            CommandLogType::from_bytes(b"large-reply"),
            Some(CommandLogType::LargeReply)
        );
        assert_eq!(CommandLogType::from_bytes(b"unknown"), None);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/commandlog.c  (276 lines, 8 functions) + commandlog.h
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         7
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         Logic is faithful; 7 TODOs cover missing Client fields,
//                  script integration, CMD_SKIP_COMMANDLOG flag, and Phase 3
//                  CommandContext/RedisServer wiring. CommandLogClientInfo is
//                  a Phase A decoupling shim, not a permanent type.
// ──────────────────────────────────────────────────────────────────────────
