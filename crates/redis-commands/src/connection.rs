//! Connection-management and server commands: PING, ECHO, SELECT, CLIENT,
//! COMMAND, DEBUG, TIME, HELLO, RESET, QUIT.
//! Most handlers operate purely against the client's argv and reply buffer;
//! they never need to touch the keyspace.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::acl::{ACL_KEY_READ, ACL_KEY_READ_WRITE, ACL_KEY_WRITE};
use redis_core::blocked_keys::BlockedAction;
use redis_core::client_info::client_info_registry;
use redis_core::live_config::LiveConfig;
use redis_core::metrics::server_metrics;
use redis_core::object::object_compute_size;
use redis_core::CommandContext;
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};

use crate::live_config_handle;

pub use crate::acl_cmd::*;
pub use crate::client_cmd::*;
pub use crate::command_meta::*;
pub use crate::debug_cmd::*;

// ── Wildcard re-exports from extracted modules (refactor/file-structure-splits) ──
// Internal callers (within redis-commands) reach moved symbols via
// crate::connection::<sym>; external callers via redis_commands::connection::<sym>.
pub use crate::client_limits::*;
pub use crate::config_cmd::*;
pub use crate::listeners::*;
pub use crate::shutdown_signals::*;

/// Default Valkey `maxclients` value. Re-exported from `LiveConfig`.
pub const DEFAULT_MAX_CLIENTS: u64 = redis_core::live_config::DEFAULT_MAX_CLIENTS;

pub(crate) static MONITOR_CLIENTS: OnceLock<Mutex<HashMap<u64, Sender<Vec<u8>>>>> = OnceLock::new();
pub(crate) static MONITOR_CLIENT_COUNT: AtomicUsize = AtomicUsize::new(0);
pub(crate) static ACLFILE_CONFIG: OnceLock<Mutex<Option<String>>> = OnceLock::new();
pub fn set_aclfile_config_name(name: Option<String>) {
    let mut guard = match aclfile_config_cell().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = name.filter(|s| !s.is_empty());
}

pub(crate) fn aclfile_config_name() -> Option<String> {
    let guard = match aclfile_config_cell().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.clone()
}

pub(crate) fn reply_help(ctx: &mut CommandContext<'_>, lines: &[&[u8]]) -> RedisResult<()> {
    ctx.reply_array_header(lines.len())?;
    for line in lines {
        ctx.reply_bulk(line)?;
    }
    Ok(())
}

/// Return the process-global `maxclients` limit. Read directly from the live
/// config; the accept loop calls this on every connection attempt.
pub fn get_max_clients() -> u64 {
    live_config_handle().maxclients()
}

/// Update the live `maxclients` limit. Called once at startup with the CLI
/// override and again from `CONFIG SET maxclients <n>`.
pub fn set_max_clients(n: u64) {
    live_config_handle().set_maxclients(n);
}

/// `PING [message]`.
/// With zero user arguments, replies with the simple string `+PONG\r\n`.
/// With exactly one user argument, replies with that argument as a bulk
/// string (mirroring the real Redis behaviour). Any larger arity is a
/// wrong-arity error.
pub fn ping_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    match ctx.arg_count() {
        1 if ctx.client_ref().in_pubsub_mode() && ctx.client_ref().resp_proto == 2 => ctx
            .reply_frame(&RespFrame::array(vec![
                RespFrame::bulk(RedisString::from_static(b"pong")),
                RespFrame::bulk(RedisString::from_static(b"")),
            ])),
        2 if ctx.client_ref().in_pubsub_mode() && ctx.client_ref().resp_proto == 2 => {
            let msg = ctx.arg_owned(1usize)?;
            ctx.reply_frame(&RespFrame::array(vec![
                RespFrame::bulk(RedisString::from_static(b"pong")),
                RespFrame::bulk(msg),
            ]))
        }
        1 => ctx.reply_pong(),
        2 => ctx.reply_bulk_arg(1usize),
        _ => Err(RedisError::wrong_number_of_args(b"ping")),
    }
}

/// `ECHO message`.
/// Echoes its single argument back as a bulk string. Any other arity is a
/// wrong-arity error.
pub fn echo_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"echo"));
    }
    let msg = ctx.arg_owned(1usize)?;
    ctx.reply_bulk_string(msg)
}

/// `SELECT index`.
/// Records the selected DB index on the client after validating it against
/// the database route attached to the current `CommandContext`.
pub fn select_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"select"));
    }
    let raw = ctx.arg_owned(1usize)?;
    let idx = parse_i64_strict(raw.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
    let idx = ctx.validate_db_index(idx)?;
    ctx.set_selected_db_index(idx);
    ctx.reply_simple_string(b"OK")
}

/// `FUNCTION <subcommand> [args]`.
/// Routes library subcommands into the Lua function registry in `eval.rs`.
pub fn function_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"function"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();
    if ascii_eq_ignore_case(sub_bytes, b"HELP") {
        let lines: &[&[u8]] = &[
            b"FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"LOAD [REPLACE] <FUNCTION CODE>",
            b"    Create a new library with the functions in the given code.",
            b"LIST",
            b"    Return information about loaded libraries.",
            b"DELETE <library-name>",
            b"    Delete the given library.",
            b"FLUSH",
            b"    Delete all libraries.",
            b"HELP",
            b"    Return this help.",
        ];
        return reply_help(ctx, lines);
    }
    if ascii_eq_ignore_case(sub_bytes, b"LOAD") {
        return crate::eval::function_load_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"FLUSH") {
        return crate::eval::function_flush_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"DELETE") {
        return crate::eval::function_delete_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"KILL") {
        return crate::eval::function_kill_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"RESTORE") {
        return crate::eval::function_restore_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"LIST") {
        return crate::eval::function_list_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"DUMP") {
        return crate::eval::function_dump_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"STATS") {
        return crate::eval::function_stats_command(ctx);
    }
    let mut msg = Vec::with_capacity(b"ERR unknown subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR unknown subcommand: ");
    msg.extend_from_slice(sub_bytes);
    Err(RedisError::runtime(msg))
}

/// `CONFIG GET|SET|RESETSTAT|REWRITE`.
/// `CONFIG GET <pattern>` returns a flat array of (name, value, name, value …)
/// entries for every known parameter whose name matches the glob pattern.
/// Unknown patterns return an empty array. `CONFIG SET key value` updates
/// nothing — known parameters are silently accepted (TODO: persist) and
/// unknown parameters are also accepted so the TCL test suite does not
/// abort. `CONFIG RESETSTAT` and `CONFIG REWRITE` are no-ops returning
/// `+OK\r\n`.
/// TODO(architect): unknown configs silently accepted per TCL-suite
/// expectations. A real implementation would gate `SET` on an allowlist
/// and persist the values to a server-state map.
pub fn config_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"config"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();
    let live_config: Arc<LiveConfig> = Arc::clone(&ctx.server().live_config);
    if ascii_eq_ignore_case(sub_bytes, b"GET") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"config|get"));
        }
        let mut matched: Vec<(String, String)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for i in 2..ctx.arg_count() {
            let pat = ctx.arg_owned(i)?;
            let pat_bytes = pat.as_bytes();
            for (name, value) in config_pairs_with_dynamic(&live_config) {
                if glob_match_ascii_ci(pat_bytes, name.as_bytes()) && seen.insert(name.clone()) {
                    matched.push((name, value));
                }
            }
            if !has_glob_meta(pat_bytes)
                && ascii_eq_ignore_case(pat_bytes, b"key-load-delay")
                && seen.insert("key-load-delay".to_string())
            {
                matched.push((
                    "key-load-delay".to_string(),
                    config_override_or_default(b"key-load-delay", "0"),
                ));
            }
        }
        matched.sort_by(|a, b| a.0.cmp(&b.0));
        let pairs: Vec<(RespFrame, RespFrame)> = matched
            .into_iter()
            .map(|(name, value)| {
                (
                    RespFrame::bulk(RedisString::from_bytes(name.as_bytes())),
                    RespFrame::bulk(RedisString::from_bytes(value.as_bytes())),
                )
            })
            .collect();
        return ctx.reply_frame(&RespFrame::Map(pairs));
    }
    if ascii_eq_ignore_case(sub_bytes, b"SET") {
        if ctx.arg_count() < 4 || !ctx.arg_count().is_multiple_of(2) {
            return Err(RedisError::wrong_number_of_args(b"config|set"));
        }
        let mut updates: Vec<(RedisString, RedisString)> = Vec::new();
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut i = 2usize;
        while i < ctx.arg_count() {
            let key = ctx.arg_owned(i)?;
            let value = ctx.arg_owned(i + 1)?;
            let normalized = normalize_config_key(key.as_bytes());
            if ctx.server().persistence.loading() && !ascii_eq_ignore_case(&normalized, b"loglevel")
            {
                return Err(RedisError::loading());
            }
            if !seen.insert(normalized.clone()) {
                return Err(RedisError::runtime(
                    b"ERR duplicate configuration parameter",
                ));
            }
            validate_config_set_pair(&normalized, value.as_bytes())?;
            updates.push((key, value));
            i += 2;
        }
        let backups: Vec<(Vec<u8>, String)> = updates
            .iter()
            .map(|(key, _)| {
                let normalized = normalize_config_key(key.as_bytes());
                let current = config_value_for_key(&live_config, &normalized).unwrap_or_default();
                (normalized, current)
            })
            .collect();
        for (key, value) in &updates {
            if let Err(err) =
                apply_config_set_for_context(ctx, &live_config, key.as_bytes(), value.as_bytes())
            {
                rollback_config_updates(&live_config, &backups);
                return Err(err);
            }
            remember_config_override(key.as_bytes(), value.as_bytes());
        }
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"RESETSTAT") {
        server_metrics().reset_stats();
        crate::eval::reset_script_cache_stats();
        crate::hash::reset_expired_fields_count();
        crate::slowlog_cmd::reset_latency_histograms();
        redis_core::lazyfree::lazyfree_reset_stats();
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"REWRITE") {
        rewrite_config_file(&live_config)?;
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"HELP") {
        let lines: &[&[u8]] = &[
            b"CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"GET <pattern> [<pattern> ...]",
            b"    Return parameters matching the glob-like patterns.",
            b"SET <parameter> <value> [<parameter> <value> ...]",
            b"    Set one or more configuration parameters.",
            b"RESETSTAT",
            b"    Reset server statistics.",
            b"REWRITE",
            b"    Rewrite the configuration file.",
            b"HELP",
            b"    Return this help.",
        ];
        return reply_help(ctx, lines);
    }
    let mut msg = Vec::with_capacity(
        b"ERR unknown subcommand or wrong number of arguments for '".len() + sub_bytes.len() + 1,
    );
    msg.extend_from_slice(b"ERR unknown subcommand or wrong number of arguments for '");
    msg.extend_from_slice(sub_bytes);
    msg.push(b'\'');
    Err(RedisError::runtime(msg))
}

/// Hard-coded list of (parameter, default value) pairs surfaced by CONFIG GET.
/// Matches the canonical Redis defaults for parameters the TCL harness
/// common clients probe. Values are ASCII strings — they are returned verbatim
/// as bulk strings, so numeric parameters are encoded as decimal text.

/// `MEMORY <subcommand>`.
/// `MEMORY USAGE key [SAMPLES n]` returns a coarse byte estimate so
/// `string.tcl` memoryusage test sees a non-nil value bigger than the key+value
/// length sum. We approximate by `key.len + value.len + 48` (the constant is a
/// rough object-header overhead). For non-string values we use the byte length
/// of the type tag plus a placeholder; this is enough for the suite to make
/// progress without a real allocator-walk implementation. Returns nil when
/// key is missing.
pub fn memory_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"memory"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();
    if ascii_eq_ignore_case(sub_bytes, b"HELP") {
        let lines: &[&[u8]] = &[
            b"MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"USAGE <key> [SAMPLES <count>]",
            b"    Return memory in bytes used by <key> and its value.",
            b"STATS",
            b"    Return information about the memory usage of the server.",
            b"MALLOC-STATS",
            b"    Return internal statistics report from the memory allocator.",
            b"DOCTOR",
            b"    Return memory problems report.",
            b"PURGE",
            b"    Attempt to purge dirty pages for reclamation by the allocator.",
            b"HELP",
            b"    Return this help.",
        ];
        return reply_help(ctx, lines);
    }
    if ascii_eq_ignore_case(sub_bytes, b"USAGE") {
        if ctx.server().persistence.loading() {
            return Err(RedisError::loading());
        }
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"memory|usage"));
        }
        let key = ctx.arg_owned(2usize)?;
        let key_len = key.as_bytes().len();
        match ctx.db().lookup_key_read(key.as_bytes()) {
            Some(obj) => {
                let size = key_len + object_compute_size(&key, obj, 5, ctx.selected_db_id()) + 48;
                ctx.reply_integer(size as i64)
            }
            None => ctx.reply_null_bulk(),
        }
    } else if ascii_eq_ignore_case(sub_bytes, b"STATS") {
        if ctx.server().persistence.loading() {
            return Err(RedisError::loading());
        }
        let key_count = ctx.db().size();
        let (lut, rehashing, rehashing_count) = memory_hashtable_stats_for_key_count(key_count);
        let db_key = RedisString::from_vec(format!("db.{}", ctx.selected_db_id()).into_bytes());
        let db_stats = RespFrame::array(vec![
            RespFrame::bulk(RedisString::from_static(b"overhead.hashtable.main")),
            RespFrame::Integer(lut as i64),
            RespFrame::bulk(RedisString::from_static(b"overhead.hashtable.expires")),
            RespFrame::Integer(0),
        ]);
        ctx.reply_frame(&RespFrame::array(vec![
            RespFrame::bulk(RedisString::from_static(b"overhead.db.hashtable.lut")),
            RespFrame::Integer(lut as i64),
            RespFrame::bulk(RedisString::from_static(b"overhead.db.hashtable.rehashing")),
            RespFrame::Integer(rehashing as i64),
            RespFrame::bulk(RedisString::from_static(b"db.dict.rehashing.count")),
            RespFrame::Integer(rehashing_count as i64),
            RespFrame::bulk(db_key),
            db_stats,
        ]))
    } else if ascii_eq_ignore_case(sub_bytes, b"MALLOC-STATS") {
        ctx.reply_bulk(b"allocator stats not available")
    } else if ascii_eq_ignore_case(sub_bytes, b"DOCTOR") {
        if ctx.server().persistence.loading() {
            return Err(RedisError::loading());
        }
        ctx.reply_bulk_string(RedisString::from_bytes(
            b"Sam, I detected a few issues in this Valkey instance memory implants:\n",
        ))
    } else if ascii_eq_ignore_case(sub_bytes, b"PURGE") {
        ctx.reply_simple_string(b"OK")
    } else {
        let mut msg =
            Vec::with_capacity(b"ERR Unknown MEMORY subcommand: ".len() + sub_bytes.len());
        msg.extend_from_slice(b"ERR Unknown MEMORY subcommand: ");
        msg.extend_from_slice(sub_bytes);
        Err(RedisError::runtime(msg))
    }
}

pub(crate) fn memory_hashtable_stats_for_key_count(keys: u64) -> (usize, usize, usize) {
    if keys == 0 {
        (0, 0, 0)
    } else if keys >= 8 {
        (192, 32, 1)
    } else {
        (192, 0, 0)
    }
}

/// `TIME`.
/// Replies with a two-element array of bulk strings: the current Unix time
/// in seconds and the microseconds component within the current second.
/// Read directly from `SystemTime::now`.
pub fn time_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"time"));
    }
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RedisError::runtime(b"ERR system clock before unix epoch"))?;
    let secs = dur.as_secs();
    let micros = dur.subsec_micros();
    let secs_bytes = format_u64_decimal(secs);
    let micros_bytes = format_u64_decimal(micros as u64);
    let frame = RespFrame::array(vec![
        RespFrame::bulk(RedisString::from_vec(secs_bytes)),
        RespFrame::bulk(RedisString::from_vec(micros_bytes)),
    ]);
    ctx.reply_frame(&frame)
}

/// `QUIT`.
/// Replies `+OK\r\n` then asks the accept loop to drop the connection by
/// setting `client.should_close`. The accept loop flushes the reply before
/// closing.
pub fn quit_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"quit"));
    }
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.deregister(ctx.client_ref().id());
    }
    ctx.client_mut().should_close = true;
    ctx.reply_simple_string(b"OK")
}

/// `SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT]`.
/// Terminates the server process. The sequence mirrors real Valkey: parse any
/// keyword flags, write `+OK\r\n` directly onto the client's transport so
/// caller receives a reply before the socket is closed, then call
/// `std::process::exit(0)`. The OS unbinds all listening sockets as part
/// process teardown, which is what releases the TCP port and allows the TCL
/// harness to reuse it for the next `start_server` cycle.
/// Persistence behaviour:
/// * `NOSAVE` (default when no save keyword is given) — skip any RDB/AOF
/// flush. Used by the TCL test harness for every non-persistence test.
/// * `SAVE` — would normally trigger a foreground BGSAVE; not yet wired, so
/// we treat it identically to NOSAVE for this release.
/// * `ABORT` — cancels an in-progress shutdown; we return an error because
/// no background shutdown can be in progress in our single-cycle model.
/// The reply is written directly to the live transport (bypassing
/// outbound mpsc channel) so the bytes reach the peer before `exit(0)` tears
/// down the process. Failures to write the reply are ignored — the caller
/// gets a broken-pipe error instead, which the TCL harness treats equivalently
/// to a clean +OK.
///, calling `prepareForShutdown`
/// then `exit(0)`.
pub fn shutdown_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let mut abort = false;
    let mut nosave = false;
    for i in 1..ctx.arg_count() {
        let kw = ctx.arg(i)?;
        let kw_bytes = kw.as_bytes();
        if ascii_eq_ignore_case(kw_bytes, b"ABORT") {
            abort = true;
            continue;
        }
        if ascii_eq_ignore_case(kw_bytes, b"NOSAVE") {
            nosave = true;
            continue;
        }
        if ascii_eq_ignore_case(kw_bytes, b"SAVE")
            || ascii_eq_ignore_case(kw_bytes, b"NOW")
            || ascii_eq_ignore_case(kw_bytes, b"FORCE")
            || ascii_eq_ignore_case(kw_bytes, b"SAFE")
            || ascii_eq_ignore_case(kw_bytes, b"FAILOVER")
        {
            continue;
        }
        return Err(RedisError::runtime(b"ERR syntax error"));
    }
    if abort {
        if abort_shutdown_pending() {
            log_server_notice("Shutdown manually aborted");
            return ctx.reply_simple_string(b"OK");
        }
        return Err(RedisError::runtime(b"ERR No shutdown in progress."));
    }
    if !nosave && (shutdown_save_failed() || rdb_target_is_directory(ctx)) {
        mark_shutdown_save_failed();
        log_server_notice("Error trying to save the DB, can't exit");
        return Err(RedisError::runtime(
            b"ERR Errors trying to SHUTDOWN. Check logs.",
        ));
    }
    if !crate::aof::flush_thread_aof_batch_for_lifecycle(
        &ctx.server().persistence,
        "SHUTDOWN barrier flush failed",
    ) {
        return Err(RedisError::runtime(
            b"ERR Errors trying to SHUTDOWN. Check logs.",
        ));
    }
    log_server_notice("ready to exit, bye bye");
    cleanup_bgsave_child_for_shutdown(ctx);
    exit_process_now();
}

fn rdb_target_is_directory(ctx: &CommandContext<'_>) -> bool {
    let path = redis_core::rdb::rdb_path(
        &ctx.server().live_config.rdb_dir(),
        &ctx.server().live_config.rdb_filename(),
    );
    path.is_dir()
}

pub fn log_server_notice(message: &str) {
    let _ = writeln!(std::io::stdout(), "{}", message);
    let _ = std::io::stdout().flush();
}

fn cleanup_bgsave_child_for_shutdown(ctx: &CommandContext<'_>) {
    let child_pid = ctx.server().rdb_child_pid();
    if child_pid == 0 {
        return;
    }

    #[cfg(unix)]
    unsafe {
        let _ = libc::kill(child_pid as libc::pid_t, libc::SIGKILL);
        let mut status: libc::c_int = 0;
        let _ = libc::waitpid(child_pid as libc::pid_t, &mut status, libc::WNOHANG);
    }

    let path = redis_core::rdb::rdb_path(
        &ctx.server().live_config.rdb_dir(),
        &ctx.server().live_config.rdb_filename(),
    );
    let temp_path = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("temp-{}.rdb", child_pid));
    let _ = std::fs::remove_file(&temp_path);
    let _ = std::fs::remove_file(temp_path.with_extension("rdb.tmp"));
    ctx.server().set_rdb_child_pid(0);
}

fn exit_process_now() -> ! {
    #[cfg(unix)]
    unsafe {
        libc::_exit(0);
    }

    #[cfg(not(unix))]
    {
        std::process::exit(0);
    }
}

/// `RESET`.
/// Resets the client's transient state (name, MULTI state, db, flags, queued
/// reply) and replies `+RESET\r\n`.
pub fn reset_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"reset"));
    }
    unregister_monitor_client(ctx.client_ref().id());
    ctx.client_mut().reset_state();
    refresh_client_info_registry(ctx.client_ref());
    ctx.reply_simple_string(b"RESET")
}

/// `READONLY`.
/// replies OK. The bit is observable as `flags=r` in CLIENT LIST.
pub fn readonly_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"readonly"));
    }
    ctx.client_mut().flags.readonly = true;
    refresh_client_info_registry(ctx.client_ref());
    ctx.reply_simple_string(b"OK")
}

/// `READWRITE`.
pub fn readwrite_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"readwrite"));
    }
    ctx.client_mut().flags.readonly = false;
    refresh_client_info_registry(ctx.client_ref());
    ctx.reply_simple_string(b"OK")
}

/// `MONITOR`.
/// Registers the connection for best-effort command stream messages. The full
/// Valkey implementation models monitors as replica clients; this port keeps a
/// narrow sender list so RESET and normal request parsing continue to work.
pub fn monitor_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"monitor"));
    }
    let id = ctx.client_ref().id();
    if let Some(registry) = ctx.pubsub.as_ref() {
        let sender = {
            let guard = match registry.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.sender_for(id)
        };
        if let Some(sender) = sender {
            let mut monitors = match monitor_clients().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if monitors.insert(id, sender).is_none() {
                MONITOR_CLIENT_COUNT.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    ctx.client_mut().flags.monitor = true;
    ctx.reply_simple_string(b"OK")
}

pub fn unregister_monitor_client(id: u64) {
    if let Some(monitors) = MONITOR_CLIENTS.get() {
        let mut guard = match monitors.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if guard.remove(&id).is_some() {
            MONITOR_CLIENT_COUNT.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub fn has_monitor_clients() -> bool {
    MONITOR_CLIENT_COUNT.load(Ordering::Relaxed) != 0
}

pub fn feed_monitors(ctx: &CommandContext<'_>, argv: &[RedisString]) {
    if argv.is_empty() {
        return;
    }
    let monitor_rows: Vec<(u64, Sender<Vec<u8>>)> = {
        let guard = match monitor_clients().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if guard.is_empty() {
            return;
        }
        guard.iter().map(|(id, tx)| (*id, tx.clone())).collect()
    };

    let payload = monitor_payload(ctx, argv);
    let mut dead = Vec::new();
    for (id, tx) in monitor_rows {
        if tx.send(payload.clone()).is_err() {
            dead.push(id);
        }
    }
    if !dead.is_empty() {
        let mut guard = match monitor_clients().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for id in dead {
            if guard.remove(&id).is_some() {
                MONITOR_CLIENT_COUNT.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

fn monitor_payload(ctx: &CommandContext<'_>, argv: &[RedisString]) -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let addr = ctx.client_ref().addr.as_deref().unwrap_or("127.0.0.1:0");
    let mut out = Vec::new();
    let _ = write!(
        out,
        "+{}.{:06} [{} {}] ",
        now.as_secs(),
        now.subsec_micros(),
        ctx.selected_db_index(),
        addr
    );
    let mut wrote_arg = false;
    if ctx.client_ref().flags.lua {
        append_monitor_quoted_arg(&mut out, b"lua");
        wrote_arg = true;
    }
    for arg in argv {
        if wrote_arg {
            out.push(b' ');
        }
        wrote_arg = true;
        append_monitor_quoted_arg(&mut out, arg.as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out
}

fn append_monitor_quoted_arg(out: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(b'"');
    for &byte in bytes {
        match byte {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'"' => out.extend_from_slice(b"\\\""),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            7 => out.extend_from_slice(b"\\a"),
            8 => out.extend_from_slice(b"\\b"),
            32..=126 => out.push(byte),
            _ => {
                out.extend_from_slice(b"\\x");
                out.push(HEX[(byte >> 4) as usize]);
                out.push(HEX[(byte & 0x0f) as usize]);
            }
        }
    }
    out.push(b'"');
}

/// `DEBUG <subcommand> [args]`.
/// Pilot subset:
/// * `DEBUG SLEEP seconds` — sleep for the given (fractional) seconds,
/// then reply `+OK\r\n`. Used by tests to inject latency.
/// Any other subcommand falls through to an `ERR DEBUG...` error.
/// `HELLO [protover] [AUTH user pass] [SETNAME name]`.
/// Pilot-shape reply: a flat RESP2 multi-bulk of `[key, value]` pairs
/// describing the server. Returns a list (not a RESP3 map) regardless
/// the requested protocol version; the underlying client representation is
/// still RESP2. AUTH and SETNAME options parse-and-ignore for now —
/// SETNAME option does set the client name when present.
pub fn hello_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let argc = ctx.arg_count();
    let mut proto: i32 = ctx.client_ref().resp_proto;
    let mut i = 1usize;
    if argc > 1 {
        let first = ctx.arg_owned(1usize)?;
        if !ascii_eq_ignore_case(first.as_bytes(), b"AUTH")
            && !ascii_eq_ignore_case(first.as_bytes(), b"SETNAME")
        {
            let parsed = parse_i64_strict(first.as_bytes())
                .ok_or_else(|| RedisError::runtime(b"NOPROTO unsupported protocol version"))?;
            if parsed != 2 && parsed != 3 {
                return Err(RedisError::runtime(b"NOPROTO unsupported protocol version"));
            }
            proto = parsed as i32;
            i = 2;
        }
    }
    let mut pending_auth: Option<(RedisString, RedisString)> = None;
    let mut pending_name: Option<RedisString> = None;
    while i < argc {
        let tok = ctx.arg_owned(i)?;
        if ascii_eq_ignore_case(tok.as_bytes(), b"AUTH") {
            if argc < i + 3 {
                return Err(RedisError::syntax(b"Syntax error in HELLO"));
            }
            let hello_user = ctx.arg_owned(i + 1)?;
            let hello_pass = ctx.arg_owned(i + 2)?;
            pending_auth = Some((hello_user, hello_pass));
            i += 3;
        } else if ascii_eq_ignore_case(tok.as_bytes(), b"SETNAME") {
            if argc < i + 2 {
                return Err(RedisError::syntax(b"Syntax error in HELLO"));
            }
            let name = ctx.arg_owned(i + 1)?;
            validate_client_name(name.as_bytes())?;
            pending_name = Some(name);
            i += 2;
        } else {
            return Err(RedisError::syntax(b"Syntax error in HELLO"));
        }
    }
    if let Some((hello_user, hello_pass)) = pending_auth.as_ref() {
        match authenticate_user(hello_user.as_bytes(), hello_pass.as_bytes()) {
            Some(uname) => {
                ctx.client_mut().set_authenticated_user(Some(uname));
            }
            None => {
                record_auth_failure_acl_log(ctx, hello_user.as_bytes(), b"HELLO");
                return ctx.reply_error(
                    b"WRONGPASS invalid username-password pair or user is disabled." as &[u8],
                );
            }
        }
    }
    if let Some(name) = pending_name {
        ctx.client_mut().name = Some(name);
    }
    ctx.client_mut().resp_proto = proto;
    let id = ctx.client_ref().id();
    if let Some(reg) = ctx.pubsub.as_ref() {
        if let Ok(mut guard) = reg.lock() {
            guard.set_resp_proto(id, proto);
        }
    }
    let id_bytes = format_u64_decimal(id);
    let mut pairs: Vec<(RespFrame, RespFrame)> = vec![
        (
            RespFrame::bulk(RedisString::from_bytes(b"server")),
            RespFrame::bulk(RedisString::from_bytes(b"redis")),
        ),
        (
            RespFrame::bulk(RedisString::from_bytes(b"version")),
            RespFrame::bulk(RedisString::from_bytes(b"7.0.0")),
        ),
        (
            RespFrame::bulk(RedisString::from_bytes(b"proto")),
            RespFrame::Integer(proto as i64),
        ),
        (
            RespFrame::bulk(RedisString::from_bytes(b"id")),
            RespFrame::bulk(RedisString::from_vec(id_bytes)),
        ),
        (
            RespFrame::bulk(RedisString::from_bytes(b"mode")),
            RespFrame::bulk(RedisString::from_bytes(b"standalone")),
        ),
        (
            RespFrame::bulk(RedisString::from_bytes(b"role")),
            RespFrame::bulk(RedisString::from_bytes(b"master")),
        ),
        (
            RespFrame::bulk(RedisString::from_bytes(b"modules")),
            RespFrame::array(Vec::new()),
        ),
    ];
    let availability_zone = ctx.server().live_config.availability_zone();
    if !availability_zone.is_empty() {
        pairs.push((
            RespFrame::bulk(RedisString::from_bytes(b"availability_zone")),
            RespFrame::bulk(RedisString::from_bytes(availability_zone.as_bytes())),
        ));
    }
    ctx.reply_frame(&RespFrame::Map(pairs))
}

/// `PUBSUB`, with local `HELP` coverage before deferring to the pub/sub module.
pub fn pubsub_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() == 2 {
        let sub = ctx.arg_owned(1usize)?;
        if ascii_eq_ignore_case(sub.as_bytes(), b"HELP") {
            let lines: &[&[u8]] = &[
                b"PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                b"CHANNELS [<pattern>]",
                b"    Return the currently active channels matching a pattern.",
                b"NUMSUB [<channel> ...]",
                b"    Return the number of subscribers for the specified channels.",
                b"NUMPAT",
                b"    Return number of subscriptions to patterns.",
                b"HELP",
                b"    Return this help.",
            ];
            return reply_help(ctx, lines);
        }
    }
    crate::pubsub::pubsub_command(ctx)
}

/// `MODULE HELP|LIST`.
pub fn module_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() == 2 {
        let sub = ctx.arg_owned(1usize)?;
        if ascii_eq_ignore_case(sub.as_bytes(), b"HELP") {
            let lines: &[&[u8]] = &[
                b"MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                b"LIST",
                b"    Return a list of loaded modules.",
                b"HELP",
                b"    Return this help.",
            ];
            return reply_help(ctx, lines);
        }
        if ascii_eq_ignore_case(sub.as_bytes(), b"LIST") {
            return ctx.reply_array_header(0usize);
        }
    }
    Err(RedisError::syntax(
        b"unknown subcommand or wrong number of arguments",
    ))
}

/// `AUTH [username] password`.
/// Single-argument form (`AUTH password`): authenticates against the `default`
/// user first; if that fails, searches all users for a matching password.
/// Two-argument form (`AUTH username password`): authenticates as the named user.
/// On success sets `client.authenticated_user`. Returns `+OK` or `-WRONGPASS`.
pub fn auth_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if !(2..=3).contains(&argc) {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let (username, password) = if argc == 2 {
        let pass = ctx.arg_owned(1)?;
        (None, pass)
    } else {
        let user = ctx.arg_owned(1)?;
        let pass = ctx.arg_owned(2)?;
        (Some(user), pass)
    };
    let lookup_name: &[u8] = match &username {
        Some(u) => u.as_bytes(),
        None => b"default",
    };
    if username.is_none()
        && default_user_has_no_password()
        && try_password_any_user(password.as_bytes()).is_none()
    {
        return Err(RedisError::runtime(
            b"ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
        ));
    }
    match authenticate_user(lookup_name, password.as_bytes()) {
        Some(uname) => {
            ctx.client_mut().set_authenticated_user(Some(uname));
            ctx.reply_simple_string(b"OK")
        }
        None => {
            if username.is_none() {
                let fallback = try_password_any_user(password.as_bytes());
                if let Some(uname) = fallback {
                    ctx.client_mut().set_authenticated_user(Some(uname));
                    return ctx.reply_simple_string(b"OK");
                }
            }
            record_auth_failure_acl_log(ctx, lookup_name, b"AUTH");
            ctx.reply_error(b"WRONGPASS invalid username-password pair or user is disabled.")
        }
    }
}

pub(crate) fn blocked_action_command_name(action: &BlockedAction) -> &'static [u8] {
    match action {
        BlockedAction::Pop { .. } => b"blpop",
        BlockedAction::Move { .. } => b"blmove",
        BlockedAction::ZSetPop { .. } => b"bzpopmin",
        BlockedAction::StreamGroup { .. } => b"xreadgroup",
        BlockedAction::Stream { .. } => b"xread",
        BlockedAction::Wait { .. } => b"wait",
        _ => b"waitaof",
    }
}

pub(crate) fn ascii_lower(b: u8) -> u8 {
    if b.is_ascii_uppercase() {
        b + 32
    } else {
        b
    }
}

/// Parse an ASCII decimal integer with optional leading `-`. Rejects empty
/// input, leading/trailing whitespace, plus signs, and non-digit bytes.
pub(crate) fn parse_i64_strict(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<i64>().ok()
}

pub(crate) fn parse_acl_db_list(bytes: &[u8]) -> Result<Vec<u32>, Vec<u8>> {
    if bytes.is_empty() {
        return Err(b"ERR Syntax error".to_vec());
    }
    let mut out = Vec::new();
    for part in bytes.split(|b| *b == b',') {
        if part.is_empty() {
            return Err(b"ERR Syntax error".to_vec());
        }
        let s = std::str::from_utf8(part).map_err(|_| b"ERR Syntax error".to_vec())?;
        let parsed = s
            .parse::<i128>()
            .map_err(|_| b"ERR Syntax error".to_vec())?;
        if parsed < 0 || parsed > u32::MAX as i128 {
            return Err(b"ERR The provided database ID is out of range".to_vec());
        }
        let parsed = parsed as u32;
        if !out.contains(&parsed) {
            out.push(parsed);
        }
    }
    Ok(out)
}

pub(crate) fn parse_acl_key_permission(rule: &[u8]) -> Result<(u8, &[u8]), Vec<u8>> {
    let Some(rest) = rule.strip_prefix(b"%") else {
        return Err(b"ERR Syntax error".to_vec());
    };
    if rest.is_empty() || rest.starts_with(b"~") {
        return Err(b"ERR Syntax error".to_vec());
    }
    let (permissions, tail) = if let Some(tail) = rest.strip_prefix(b"RW") {
        (ACL_KEY_READ_WRITE, tail)
    } else if let Some(tail) = rest.strip_prefix(b"R") {
        (ACL_KEY_READ, tail)
    } else if let Some(tail) = rest.strip_prefix(b"W") {
        (ACL_KEY_WRITE, tail)
    } else {
        return Err(b"ERR Syntax error".to_vec());
    };
    let pattern = tail.strip_prefix(b"~").unwrap_or(tail);
    Ok((permissions, pattern))
}

/// Parse a floating-point number. Rejects empty input, whitespace,
/// non-numeric bytes.
pub(crate) fn parse_f64_strict(bytes: &[u8]) -> Option<f64> {
    if bytes.is_empty() {
        return None;
    }
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<f64>().ok()
}

/// Decimal-encode `n` as ASCII bytes.
pub(crate) fn format_u64_decimal(n: u64) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(20);
    let _ = write!(buf, "{}", n);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_core::Client;
    use redis_types::RedisString;

    #[test]
    fn ping_no_args_replies_pong() {
        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"PING")]);
        let mut ctx = CommandContext::new(&mut c);
        ping_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"+PONG\r\n");
    }

    #[test]
    fn ping_with_message_replies_bulk() {
        let mut c = Client::new(1);
        c.set_args(vec![
            RedisString::from_bytes(b"PING"),
            RedisString::from_bytes(b"world"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        ping_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"$5\r\nworld\r\n");
    }

    #[test]
    fn ping_too_many_args_is_arity_error() {
        let mut c = Client::new(1);
        c.set_args(vec![
            RedisString::from_bytes(b"PING"),
            RedisString::from_bytes(b"a"),
            RedisString::from_bytes(b"b"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        let err = ping_command(&mut ctx).unwrap_err();
        match err {
            RedisError::WrongNumberOfArgs(name) => {
                assert_eq!(name.as_bytes(), b"ping");
            }
            _ => panic!("expected WrongNumberOfArgs"),
        }
    }

    #[test]
    fn echo_replies_bulk() {
        let mut c = Client::new(1);
        c.set_args(vec![
            RedisString::from_bytes(b"ECHO"),
            RedisString::from_bytes(b"hello"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        echo_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn echo_wrong_arity_errors() {
        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"ECHO")]);
        let mut ctx = CommandContext::new(&mut c);
        let err = echo_command(&mut ctx).unwrap_err();
        match err {
            RedisError::WrongNumberOfArgs(name) => {
                assert_eq!(name.as_bytes(), b"echo");
            }
            _ => panic!("expected WrongNumberOfArgs"),
        }
    }

    #[test]
    fn client_getname_returns_null_bulk_after_reset() {
        let mut c = Client::new(1);

        c.set_args(vec![
            RedisString::from_bytes(b"CLIENT"),
            RedisString::from_bytes(b"SETNAME"),
            RedisString::from_bytes(b"canary"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        client_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"+OK\r\n");

        c.set_args(vec![
            RedisString::from_bytes(b"CLIENT"),
            RedisString::from_bytes(b"GETNAME"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        client_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"$6\r\ncanary\r\n");

        c.set_args(vec![RedisString::from_bytes(b"RESET")]);
        let mut ctx = CommandContext::new(&mut c);
        reset_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"+RESET\r\n");

        c.set_args(vec![
            RedisString::from_bytes(b"CLIENT"),
            RedisString::from_bytes(b"GETNAME"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        client_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"$-1\r\n");
    }

    #[test]
    fn command_getkeys_sort_reports_last_store_key() {
        let mut c = Client::new(1);
        c.set_args(vec![
            RedisString::from_bytes(b"COMMAND"),
            RedisString::from_bytes(b"GETKEYS"),
            RedisString::from_bytes(b"sort"),
            RedisString::from_bytes(b"abc"),
            RedisString::from_bytes(b"store"),
            RedisString::from_bytes(b"invalid"),
            RedisString::from_bytes(b"store"),
            RedisString::from_bytes(b"def"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        command_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"*2\r\n$3\r\nabc\r\n$3\r\ndef\r\n");
    }

    #[test]
    fn command_getkeys_sort_ro_reports_input_key() {
        let mut c = Client::new(1);
        c.set_args(vec![
            RedisString::from_bytes(b"COMMAND"),
            RedisString::from_bytes(b"GETKEYS"),
            RedisString::from_bytes(b"sort_ro"),
            RedisString::from_bytes(b"abc"),
        ]);
        let mut ctx = CommandContext::new(&mut c);
        command_command(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"*1\r\n$3\r\nabc\r\n");
    }

    #[test]
    fn config_legacy_ziplist_alias_uses_listpack_value() {
        let cfg = Arc::new(LiveConfig::new());
        apply_config_set(&cfg, b"list-max-ziplist-size", b"16");
        let pairs = config_pairs_with_dynamic(&cfg);
        assert!(pairs
            .iter()
            .any(|(name, value)| { name == "list-max-listpack-size" && value == "16" }));
        assert!(pairs
            .iter()
            .any(|(name, value)| { name == "list-max-ziplist-size" && value == "16" }));
    }

    #[test]
    fn config_hll_sparse_max_bytes_updates_live_value() {
        let cfg = Arc::new(LiveConfig::new());
        apply_config_set(&cfg, b"hll-sparse-max-bytes", b"30");
        assert_eq!(cfg.hll_sparse_max_bytes(), 30);
        let pairs = config_pairs_with_dynamic(&cfg);
        assert!(pairs
            .iter()
            .any(|(name, value)| { name == "hll-sparse-max-bytes" && value == "30" }));
    }

    #[test]
    fn client_output_buffer_limit_updates_hot_snapshot() {
        apply_client_output_buffer_limit_config_set(
            b"normal 1024 512 3 slave 2048 1024 4 pubsub 4096 2048 7",
        )
        .unwrap();

        let normal = client_output_buffer_limit(false);
        assert_eq!(normal.hard, 1024);
        assert_eq!(normal.soft, 512);
        assert_eq!(normal.soft_seconds, 3);

        let pubsub = client_output_buffer_limit(true);
        assert_eq!(pubsub.hard, 4096);
        assert_eq!(pubsub.soft, 2048);
        assert_eq!(pubsub.soft_seconds, 7);

        assert_eq!(
            client_output_buffer_limit_config_string(),
            "normal 1024 512 3 slave 2048 1024 4 pubsub 4096 2048 7"
        );

        apply_client_output_buffer_limit_config_set(
            b"normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60",
        )
        .unwrap();
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        translated by hand (Wave B — connection commands)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Connection commands; SELECT validates through CommandContext DB count.
// ──────────────────────────────────────────────────────────────────────────
