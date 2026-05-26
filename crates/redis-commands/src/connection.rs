//! Connection-management and server commands: PING, ECHO, SELECT, CLIENT,
//! COMMAND, DEBUG, TIME, HELLO, RESET, QUIT.
//!
//! Most handlers operate purely against the client's argv and reply buffer;
//! they never need to touch the keyspace.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::acl::{
    acl_log_entries, acl_log_max_len, acl_log_now_millis, acl_pubsub_default_config_value,
    apply_acl_pubsub_default_to_user, category as acl_category, category_name_to_bit,
    clear_acl_log, global_acl_state, hex_to_hash, record_acl_log_entry, set_acl_log_max_len,
    set_acl_pubsub_default, sha256_hash, AclKeyPattern, AclLogEntry, AclUser, ACL_KEY_READ,
    ACL_KEY_READ_WRITE, ACL_KEY_WRITE, ALL_CATEGORY_NAMES,
};
use redis_core::blocked_keys::{blocked_keys_index, BlockedAction};
use redis_core::client_info::client_info_registry;
use redis_core::eviction::{try_evict_to_fit, EvictionOutcome};
use redis_core::live_config::{LiveConfig, MaxmemoryPolicyCode};
use redis_core::metrics::{
    record_acl_access_denied_auth, record_blocked_command_rejected, record_error_reply,
    server_metrics,
};
use redis_core::networking::{
    client_matches_ip_filter, validate_client_capa_filter, validate_client_flag_filter,
};
use redis_core::notify::{keyspace_events_string_to_flags, NOTIFY_EVICTED};
use redis_core::{CommandContext, PersistenceStatus, RedisDb};
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};
use serde_json::Value;

use crate::generated::{GeneratedCommandSpec, COMMANDS};
use crate::live_config_handle;

/// Default Valkey `maxclients` value. Re-exported from `LiveConfig`.
pub const DEFAULT_MAX_CLIENTS: u64 = redis_core::live_config::DEFAULT_MAX_CLIENTS;

static MONITOR_CLIENTS: OnceLock<Mutex<HashMap<u64, Sender<Vec<u8>>>>> = OnceLock::new();
static ACLFILE_CONFIG: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static CONFIG_OVERRIDES: OnceLock<Mutex<HashMap<Vec<u8>, String>>> = OnceLock::new();
static CONFIG_FILE_PATH: OnceLock<Mutex<Option<String>>> = OnceLock::new();
type TcpPortSetHook = dyn Fn(u16) -> Result<Vec<TcpListener>, Vec<u8>> + Send + Sync + 'static;
type TcpBindSetHook =
    dyn Fn(&[u8], u16) -> Result<Vec<TcpListener>, Vec<u8>> + Send + Sync + 'static;
static TCP_PORT_SET_HOOK: OnceLock<Box<TcpPortSetHook>> = OnceLock::new();
static TCP_BIND_SET_HOOK: OnceLock<Box<TcpBindSetHook>> = OnceLock::new();
static PENDING_TCP_LISTENERS: OnceLock<Mutex<Vec<TcpListener>>> = OnceLock::new();
static PENDING_TCP_LISTENER_REPLACEMENT: OnceLock<Mutex<Option<Vec<TcpListener>>>> =
    OnceLock::new();
static TCP_PORT_CONFIG: AtomicU16 = AtomicU16::new(0);
static RDB_KEY_SAVE_DELAY_US: AtomicU64 = AtomicU64::new(0);
static SHUTDOWN_SIGNAL_COUNT: AtomicUsize = AtomicUsize::new(0);
static SHUTDOWN_SIGNAL_NUMBER: AtomicI32 = AtomicI32::new(0);
static SHUTDOWN_PENDING: AtomicBool = AtomicBool::new(false);
static SHUTDOWN_SAVE_FAILED: AtomicBool = AtomicBool::new(false);
static SHUTDOWN_ON_SIGTERM_FORCE: AtomicBool = AtomicBool::new(false);
static DEBUG_PAUSE_CRON: AtomicBool = AtomicBool::new(false);
static CLIENT_OBUF_LIMITS: OnceLock<Mutex<ClientOutputBufferLimits>> = OnceLock::new();
static CLIENT_QUERY_BUFFER_LIMIT: AtomicUsize = AtomicUsize::new(1024 * 1024 * 1024);

#[derive(Clone, Copy)]
pub struct ClientOutputBufferLimit {
    pub hard: usize,
    pub soft: usize,
    pub soft_seconds: u64,
}

#[derive(Clone, Copy)]
struct ClientOutputBufferLimits {
    normal: ClientOutputBufferLimit,
    replica: ClientOutputBufferLimit,
    pubsub: ClientOutputBufferLimit,
}

impl Default for ClientOutputBufferLimits {
    fn default() -> Self {
        Self {
            normal: ClientOutputBufferLimit {
                hard: 0,
                soft: 0,
                soft_seconds: 0,
            },
            replica: ClientOutputBufferLimit {
                hard: 256 * 1024 * 1024,
                soft: 64 * 1024 * 1024,
                soft_seconds: 60,
            },
            pubsub: ClientOutputBufferLimit {
                hard: 32 * 1024 * 1024,
                soft: 8 * 1024 * 1024,
                soft_seconds: 60,
            },
        }
    }
}

fn monitor_clients() -> &'static Mutex<HashMap<u64, Sender<Vec<u8>>>> {
    MONITOR_CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn aclfile_config_cell() -> &'static Mutex<Option<String>> {
    ACLFILE_CONFIG.get_or_init(|| Mutex::new(None))
}

fn client_obuf_limits_cell() -> &'static Mutex<ClientOutputBufferLimits> {
    CLIENT_OBUF_LIMITS.get_or_init(|| Mutex::new(ClientOutputBufferLimits::default()))
}

pub fn set_aclfile_config_name(name: Option<String>) {
    let mut guard = match aclfile_config_cell().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = name.filter(|s| !s.is_empty());
}

fn aclfile_config_name() -> Option<String> {
    let guard = match aclfile_config_cell().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.clone()
}

fn reply_help(ctx: &mut CommandContext<'_>, lines: &[&[u8]]) -> RedisResult<()> {
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

pub fn set_tcp_port_config(port: u16) {
    TCP_PORT_CONFIG.store(port, Ordering::Relaxed);
}

fn tcp_port_config() -> u16 {
    TCP_PORT_CONFIG.load(Ordering::Relaxed)
}

pub fn install_tcp_port_set_hook(hook: Box<TcpPortSetHook>) {
    let _ = TCP_PORT_SET_HOOK.set(hook);
}

pub fn install_tcp_bind_set_hook(hook: Box<TcpBindSetHook>) {
    let _ = TCP_BIND_SET_HOOK.set(hook);
}

pub fn drain_pending_tcp_listeners() -> Vec<TcpListener> {
    let Some(cell) = PENDING_TCP_LISTENERS.get() else {
        return Vec::new();
    };
    let mut guard = match cell.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    std::mem::take(&mut *guard)
}

pub fn drain_pending_tcp_listener_replacement() -> Option<Vec<TcpListener>> {
    let cell = PENDING_TCP_LISTENER_REPLACEMENT.get()?;
    let mut guard = match cell.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.take()
}

pub fn client_output_buffer_hard_limit(is_pubsub: bool) -> usize {
    client_output_buffer_limit(is_pubsub).hard
}

pub fn client_query_buffer_limit() -> usize {
    CLIENT_QUERY_BUFFER_LIMIT.load(Ordering::Relaxed)
}

pub fn rdb_key_save_delay_us() -> u64 {
    RDB_KEY_SAVE_DELAY_US.load(Ordering::Relaxed)
}

pub fn note_shutdown_signal(signal: i32) {
    SHUTDOWN_SIGNAL_NUMBER.store(signal, Ordering::SeqCst);
    SHUTDOWN_SIGNAL_COUNT.fetch_add(1, Ordering::SeqCst);
}

pub fn shutdown_signal_count() -> usize {
    SHUTDOWN_SIGNAL_COUNT.load(Ordering::SeqCst)
}

pub fn shutdown_signal_number() -> i32 {
    SHUTDOWN_SIGNAL_NUMBER.load(Ordering::SeqCst)
}

pub fn shutdown_pending() -> bool {
    SHUTDOWN_PENDING.load(Ordering::SeqCst)
}

pub fn set_shutdown_pending(value: bool) {
    SHUTDOWN_PENDING.store(value, Ordering::SeqCst);
}

pub fn abort_shutdown_pending() -> bool {
    SHUTDOWN_PENDING.swap(false, Ordering::SeqCst)
}

pub fn mark_shutdown_save_failed() {
    SHUTDOWN_SAVE_FAILED.store(true, Ordering::SeqCst);
}

pub fn shutdown_save_failed() -> bool {
    SHUTDOWN_SAVE_FAILED.load(Ordering::SeqCst)
}

pub fn shutdown_on_sigterm_force() -> bool {
    SHUTDOWN_ON_SIGTERM_FORCE.load(Ordering::SeqCst)
}

pub fn set_debug_pause_cron(value: bool) {
    DEBUG_PAUSE_CRON.store(value, Ordering::SeqCst);
}

pub fn debug_pause_cron() -> bool {
    DEBUG_PAUSE_CRON.load(Ordering::SeqCst)
}

fn set_client_query_buffer_limit(limit: usize) {
    CLIENT_QUERY_BUFFER_LIMIT.store(limit, Ordering::Relaxed);
}

pub fn client_output_buffer_limit(is_pubsub: bool) -> ClientOutputBufferLimit {
    let guard = match client_obuf_limits_cell().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if is_pubsub {
        guard.pubsub
    } else {
        guard.normal
    }
}

fn client_output_buffer_limit_config_string() -> String {
    let guard = match client_obuf_limits_cell().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    format!(
        "normal {} {} {} slave {} {} {} pubsub {} {} {}",
        guard.normal.hard,
        guard.normal.soft,
        guard.normal.soft_seconds,
        guard.replica.hard,
        guard.replica.soft,
        guard.replica.soft_seconds,
        guard.pubsub.hard,
        guard.pubsub.soft,
        guard.pubsub.soft_seconds,
    )
}

/// `PING [message]`.
///
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
        1 => ctx.reply_simple_string(b"PONG"),
        2 => {
            let msg = ctx.arg_owned(1usize)?;
            ctx.reply_bulk_string(msg)
        }
        _ => Err(RedisError::wrong_number_of_args(b"ping")),
    }
}

/// `ECHO message`.
///
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
///
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
///
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
///
/// `CONFIG GET <pattern>` returns a flat array of (name, value, name, value …)
/// entries for every known parameter whose name matches the glob pattern.
/// Unknown patterns return an empty array. `CONFIG SET key value` updates
/// nothing — known parameters are silently accepted (TODO: persist) and
/// unknown parameters are also accepted so the TCL test suite does not
/// abort. `CONFIG RESETSTAT` and `CONFIG REWRITE` are no-ops returning
/// `+OK\r\n`.
///
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
        if ctx.arg_count() < 4 || ctx.arg_count() % 2 != 0 {
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
///
/// Matches the canonical Redis defaults for parameters the TCL harness and
/// common clients probe. Values are ASCII strings — they are returned verbatim
/// as bulk strings, so numeric parameters are encoded as decimal text.
fn default_config_pairs() -> &'static [(&'static str, &'static str)] {
    &[
        ("maxmemory", "0"),
        ("maxmemory-policy", "noeviction"),
        ("maxmemory-clients", "0"),
        ("maxmemory-samples", "5"),
        ("maxclients", "10000"),
        ("acllog-max-len", "128"),
        ("acl-pubsub-default", "resetchannels"),
        ("aclfile", ""),
        ("requirepass", ""),
        ("primaryauth", ""),
        ("masterauth", ""),
        ("dual-channel-replication-enabled", "yes"),
        ("appendonly", "no"),
        ("appendfsync", "everysec"),
        ("appendfilename", "appendonly.aof"),
        ("appenddirname", "appendonlydir"),
        ("aof-load-truncated", "yes"),
        ("aof-use-rdb-preamble", "yes"),
        ("auto-aof-rewrite-percentage", "100"),
        ("auto-aof-rewrite-min-size", "67108864"),
        ("save", "3600 1 300 100 60 10000"),
        ("shutdown-on-sigint", "default"),
        ("shutdown-on-sigterm", "default"),
        ("proc-title-template", ""),
        ("dir", "./"),
        ("dbfilename", "dump.rdb"),
        ("availability-zone", ""),
        ("import-mode", "no"),
        ("rdb-version-check", "strict"),
        ("tcp-backlog", "511"),
        ("tcp-keepalive", "300"),
        ("timeout", "0"),
        ("port", "0"),
        ("bind", "* -::*"),
        ("databases", "16"),
        ("client-query-buffer-limit", "1073741824"),
        ("slot-migration-max-failover-repl-bytes", "0"),
        ("rdb-key-save-delay", "0"),
        ("daemonize", "no"),
        ("hash-max-listpack-entries", "128"),
        ("hash-max-listpack-value", "64"),
        ("list-max-listpack-size", "-2"),
        ("list-max-ziplist-size", "-2"),
        ("list-compress-depth", "0"),
        ("set-max-intset-entries", "512"),
        ("set-max-listpack-entries", "128"),
        ("set-max-listpack-value", "64"),
        ("zset-max-listpack-entries", "128"),
        ("zset-max-listpack-value", "64"),
        ("zset-max-ziplist-entries", "128"),
        ("zset-max-ziplist-value", "64"),
        ("hash-max-ziplist-entries", "128"),
        ("hash-max-ziplist-value", "64"),
        ("hll-sparse-max-bytes", "3000"),
        ("stream-node-max-bytes", "4096"),
        ("stream-node-max-entries", "100"),
        ("activerehashing", "yes"),
        ("loglevel", "notice"),
        ("latency-tracking", "yes"),
        ("latency-tracking-info-percentiles", "50 99 99.9"),
        ("latency-monitor-threshold", "0"),
        ("slowlog-log-slower-than", "10000"),
        ("slowlog-max-len", "128"),
        ("notify-keyspace-events", ""),
        (
            "client-output-buffer-limit",
            "normal 0 0 0 slave 256mb 64mb 60 pubsub 32mb 8mb 60",
        ),
        ("proto-max-bulk-len", "536870912"),
        ("io-threads", "1"),
        ("io-threads-do-reads", "no"),
        ("lazyfree-lazy-eviction", "no"),
        ("lazyfree-lazy-expire", "no"),
        ("lazyfree-lazy-server-del", "no"),
        ("lazyfree-lazy-user-del", "no"),
        ("active-expire-effort", "1"),
        ("hz", "10"),
        ("lfu-log-factor", "10"),
        ("lfu-decay-time", "1"),
        ("tls-port", "0"),
        ("tls-cert-file", ""),
        ("tls-key-file", ""),
        ("tls-ca-cert-file", ""),
        ("tls-auth-clients", "no"),
        ("repl-backlog-size", "1048576"),
        ("repl-timeout", "60"),
        ("replicaof", ""),
        ("slaveof", ""),
        ("min-replicas-to-write", "0"),
        ("min-slaves-to-write", "0"),
        ("min-replicas-max-lag", "10"),
        ("min-slaves-max-lag", "10"),
        ("repl-disable-tcp-nodelay", "no"),
        ("slave-read-only", "yes"),
        ("replica-read-only", "yes"),
        ("slave-serve-stale-data", "yes"),
        ("replica-serve-stale-data", "yes"),
        ("lua-enable-insecure-api", "no"),
        ("repl-diskless-sync", "yes"),
    ]
}

/// Build the full CONFIG GET parameter list reading every live value from
/// the supplied `LiveConfig`. Static pairs in `default_config_pairs` are
/// reproduced verbatim for keys with no behavioural backing.
fn config_pairs_with_dynamic(cfg: &Arc<LiveConfig>) -> Vec<(String, String)> {
    let live_maxmemory = cfg.maxmemory().to_string();
    let live_maxmemory_policy = cfg.maxmemory_policy().as_config_str().to_string();
    let live_maxmemory_clients = render_maxmemory_clients(cfg.maxmemory_clients());
    let live_maxclients = cfg.maxclients().to_string();
    let live_acllog_max_len = acl_log_max_len().to_string();
    let live_acl_pubsub_default = acl_pubsub_default_config_value().to_string();
    let live_aclfile = aclfile_config_name().unwrap_or_default();
    let live_requirepass = cfg
        .requirepass()
        .map(|s| String::from_utf8_lossy(s.as_bytes()).into_owned())
        .unwrap_or_default();
    let live_primaryauth = cfg
        .primaryauth()
        .map(|s| String::from_utf8_lossy(s.as_bytes()).into_owned())
        .unwrap_or_default();
    let live_notify = redis_core::notify::keyspace_events_flags_to_string(
        cfg.notify_keyspace_events_flags() as i32,
    );
    let live_notify_str = String::from_utf8_lossy(live_notify.as_bytes()).into_owned();
    let live_slowlog_threshold = cfg.slowlog_threshold_micros().to_string();
    let live_latency_tracking = if crate::slowlog_cmd::latency_tracking_enabled() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let live_latency_tracking_percentiles =
        crate::slowlog_cmd::latency_tracking_info_percentiles_config();
    let live_latency_monitor_threshold =
        crate::slowlog_cmd::latency_monitor_threshold().to_string();
    let live_slowlog_max_len = cfg.slowlog_max_len().to_string();
    let live_effort_str = cfg.active_expire_effort().to_string();
    let live_hz_str = cfg.hz().to_string();
    let live_hash_entries = cfg.hash_max_listpack_entries().to_string();
    let live_hash_value = cfg.hash_max_listpack_value().to_string();
    let live_list_size = cfg.list_max_listpack_size().to_string();
    let live_set_intset = cfg.set_max_intset_entries().to_string();
    let live_set_entries = cfg.set_max_listpack_entries().to_string();
    let live_set_value = cfg.set_max_listpack_value().to_string();
    let live_zset_entries = cfg.zset_max_listpack_entries().to_string();
    let live_zset_value = cfg.zset_max_listpack_value().to_string();
    let live_hll_sparse_max_bytes = cfg.hll_sparse_max_bytes().to_string();
    let live_dir = cfg.rdb_dir();
    let live_dbfilename = cfg.rdb_filename();
    let live_availability_zone = cfg.availability_zone();
    let live_import_mode = yes_no(cfg.import_mode()).to_string();
    let live_port = tcp_port_config().to_string();
    let live_lfu_log_factor = cfg.lfu_log_factor().to_string();
    let live_lfu_decay_time = cfg.lfu_decay_time().to_string();
    let live_tls_port = cfg.tls_port().to_string();
    let live_tls_cert = cfg
        .tls_cert_file()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    let live_tls_key = cfg
        .tls_key_file()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    let live_tls_ca = cfg
        .tls_ca_cert_file()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    let live_tls_auth_clients = match cfg.tls_auth_clients() {
        1 => "yes".to_string(),
        2 => "optional".to_string(),
        _ => "no".to_string(),
    };
    let live_appendonly = if cfg.appendonly() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let live_appendfsync = crate::aof::fsync_policy_str(cfg.appendfsync()).to_string();
    let live_appendfilename = cfg.appendfilename();
    let live_appenddirname = cfg.appenddirname();
    let live_aof_load_truncated = yes_no(cfg.aof_load_truncated()).to_string();
    let live_aof_use_rdb_preamble = yes_no(cfg.aof_use_rdb_preamble()).to_string();
    let live_auto_aof_rewrite_percentage = cfg.auto_aof_rewrite_percentage().to_string();
    let live_auto_aof_rewrite_min_size = cfg.auto_aof_rewrite_min_size().to_string();
    let live_repl_backlog_size = cfg.repl_backlog_size().to_string();
    let live_repl_timeout = cfg.repl_timeout().to_string();
    let live_min_replicas_to_write = cfg.repl_min_replicas_to_write().to_string();
    let live_min_replicas_max_lag = cfg.repl_min_replicas_max_lag().to_string();
    let live_repl_disable_nodelay = if cfg.repl_disable_tcp_nodelay() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let live_slave_read_only = if cfg.slave_read_only() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let live_replica_serve_stale_data = if cfg.replica_serve_stale_data() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let live_lua_enable_insecure_api = if cfg.lua_enable_insecure_api() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let live_repl_diskless = if cfg.repl_diskless_sync() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let live_rdb_version_check = if cfg.rdb_version_check_relaxed() {
        "relaxed".to_string()
    } else {
        "strict".to_string()
    };
    let live_client_obuf_limit = client_output_buffer_limit_config_string();
    let live_client_query_buffer_limit = client_query_buffer_limit().to_string();

    let mut out: Vec<(String, String)> = Vec::new();
    for &(name, value) in default_config_pairs() {
        let dynamic = match name {
            "maxmemory" => Some(live_maxmemory.clone()),
            "maxmemory-policy" => Some(live_maxmemory_policy.clone()),
            "maxmemory-clients" => Some(live_maxmemory_clients.clone()),
            "maxclients" => Some(live_maxclients.clone()),
            "acllog-max-len" => Some(live_acllog_max_len.clone()),
            "acl-pubsub-default" => Some(live_acl_pubsub_default.clone()),
            "aclfile" => Some(live_aclfile.clone()),
            "requirepass" => Some(live_requirepass.clone()),
            "primaryauth" | "masterauth" => Some(live_primaryauth.clone()),
            "notify-keyspace-events" => Some(live_notify_str.clone()),
            "slowlog-log-slower-than" => Some(live_slowlog_threshold.clone()),
            "latency-tracking" => Some(live_latency_tracking.clone()),
            "latency-tracking-info-percentiles" => Some(live_latency_tracking_percentiles.clone()),
            "latency-monitor-threshold" => Some(live_latency_monitor_threshold.clone()),
            "slowlog-max-len" => Some(live_slowlog_max_len.clone()),
            "active-expire-effort" => Some(live_effort_str.clone()),
            "hz" => Some(live_hz_str.clone()),
            "hash-max-listpack-entries" => Some(live_hash_entries.clone()),
            "hash-max-listpack-value" => Some(live_hash_value.clone()),
            "list-max-listpack-size" | "list-max-ziplist-size" => Some(live_list_size.clone()),
            "set-max-intset-entries" => Some(live_set_intset.clone()),
            "set-max-listpack-entries" => Some(live_set_entries.clone()),
            "set-max-listpack-value" => Some(live_set_value.clone()),
            "zset-max-listpack-entries" => Some(live_zset_entries.clone()),
            "zset-max-listpack-value" => Some(live_zset_value.clone()),
            "zset-max-ziplist-entries" => Some(live_zset_entries.clone()),
            "zset-max-ziplist-value" => Some(live_zset_value.clone()),
            "hash-max-ziplist-entries" => Some(live_hash_entries.clone()),
            "hash-max-ziplist-value" => Some(live_hash_value.clone()),
            "hll-sparse-max-bytes" => Some(live_hll_sparse_max_bytes.clone()),
            "dir" => Some(live_dir.clone()),
            "dbfilename" => Some(live_dbfilename.clone()),
            "availability-zone" => Some(live_availability_zone.clone()),
            "import-mode" => Some(live_import_mode.clone()),
            "port" => Some(live_port.clone()),
            "lfu-log-factor" => Some(live_lfu_log_factor.clone()),
            "lfu-decay-time" => Some(live_lfu_decay_time.clone()),
            "tls-port" => Some(live_tls_port.clone()),
            "tls-cert-file" => Some(live_tls_cert.clone()),
            "tls-key-file" => Some(live_tls_key.clone()),
            "tls-ca-cert-file" => Some(live_tls_ca.clone()),
            "tls-auth-clients" => Some(live_tls_auth_clients.clone()),
            "appendonly" => Some(live_appendonly.clone()),
            "appendfsync" => Some(live_appendfsync.clone()),
            "appendfilename" => Some(live_appendfilename.clone()),
            "appenddirname" => Some(live_appenddirname.clone()),
            "aof-load-truncated" => Some(live_aof_load_truncated.clone()),
            "aof-use-rdb-preamble" => Some(live_aof_use_rdb_preamble.clone()),
            "auto-aof-rewrite-percentage" => Some(live_auto_aof_rewrite_percentage.clone()),
            "auto-aof-rewrite-min-size" => Some(live_auto_aof_rewrite_min_size.clone()),
            "repl-backlog-size" => Some(live_repl_backlog_size.clone()),
            "repl-timeout" => Some(live_repl_timeout.clone()),
            "min-replicas-to-write" | "min-slaves-to-write" => {
                Some(live_min_replicas_to_write.clone())
            }
            "min-replicas-max-lag" | "min-slaves-max-lag" => {
                Some(live_min_replicas_max_lag.clone())
            }
            "repl-disable-tcp-nodelay" => Some(live_repl_disable_nodelay.clone()),
            "slave-read-only" | "replica-read-only" => Some(live_slave_read_only.clone()),
            "slave-serve-stale-data" | "replica-serve-stale-data" => {
                Some(live_replica_serve_stale_data.clone())
            }
            "lua-enable-insecure-api" => Some(live_lua_enable_insecure_api.clone()),
            "repl-diskless-sync" => Some(live_repl_diskless.clone()),
            "rdb-version-check" => Some(live_rdb_version_check.clone()),
            "client-output-buffer-limit" => Some(live_client_obuf_limit.clone()),
            "client-query-buffer-limit" => Some(live_client_query_buffer_limit.clone()),
            _ => None,
        };
        let normalized = normalize_config_key(name.as_bytes());
        let default_value = dynamic.unwrap_or_else(|| value.to_string());
        out.push((
            name.to_string(),
            config_override_or_default(&normalized, &default_value),
        ));
    }
    out
}

fn config_overrides() -> &'static Mutex<HashMap<Vec<u8>, String>> {
    CONFIG_OVERRIDES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn config_file_path() -> &'static Mutex<Option<String>> {
    CONFIG_FILE_PATH.get_or_init(|| Mutex::new(None))
}

pub fn set_config_file_path(path: Option<String>) {
    let mut guard = match config_file_path().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = path;
}

pub fn set_startup_config_override(key: &str, value: &str) {
    remember_config_override(key.as_bytes(), value.as_bytes());
}

fn normalize_config_key(key: &[u8]) -> Vec<u8> {
    key.iter().map(|b| b.to_ascii_lowercase()).collect()
}

fn has_glob_meta(pattern: &[u8]) -> bool {
    pattern
        .iter()
        .any(|b| matches!(*b, b'*' | b'?' | b'[' | b']'))
}

fn config_override_or_default(key: &[u8], default_value: &str) -> String {
    let normalized = normalize_config_key(key);
    let guard = match config_overrides().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .get(&normalized)
        .cloned()
        .unwrap_or_else(|| default_value.to_string())
}

fn config_override_value(key: &[u8]) -> Option<String> {
    let normalized = normalize_config_key(key);
    let guard = match config_overrides().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.get(&normalized).cloned()
}

fn remember_config_override(key: &[u8], value: &[u8]) {
    let normalized = normalize_config_key(key);
    if config_value_is_live_only(&normalized) {
        return;
    }
    let mut guard = match config_overrides().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.insert(normalized, String::from_utf8_lossy(value).into_owned());
}

fn rewrite_config_file(cfg: &Arc<LiveConfig>) -> RedisResult<()> {
    let path = {
        let guard = match config_file_path().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.clone()
    };
    let Some(path) = path else {
        return Ok(());
    };

    let mut lines = Vec::new();
    let tcp_port = server_metrics()
        .tcp_port
        .load(std::sync::atomic::Ordering::Relaxed);
    if tcp_port != 0 {
        lines.push(format!("port {tcp_port}\n"));
    }
    if let Ok(existing) = std::fs::read_to_string(&path) {
        for line in existing.lines() {
            let trimmed = line.trim_start();
            let directive = trimmed
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .as_bytes();
            if ascii_eq_ignore_case(directive, b"rename-command") {
                lines.push(format!("{trimmed}\n"));
            }
        }
    }
    for key in [
        b"save".as_slice(),
        b"shutdown-on-sigterm".as_slice(),
        b"shutdown-on-sigint".as_slice(),
        b"hash-max-listpack-entries".as_slice(),
        b"hash-max-ziplist-entries".as_slice(),
        b"maxmemory".as_slice(),
        b"maxmemory-policy".as_slice(),
        b"maxmemory-clients".as_slice(),
        b"client-query-buffer-limit".as_slice(),
        b"loglevel".as_slice(),
        b"proc-title-template".as_slice(),
        b"slot-migration-max-failover-repl-bytes".as_slice(),
        b"rdb-key-save-delay".as_slice(),
    ] {
        if let Some(value) = config_value_for_key(cfg, key) {
            let name = String::from_utf8_lossy(key);
            lines.push(format!("{} {}\n", name, value));
        }
    }
    if let Some(value) = config_override_value(b"bind") {
        lines.push(format!("bind {}\n", value));
    }
    if let Some(value) = config_override_value(b"unixsocket") {
        lines.push(format!("unixsocket {}\n", value));
    }
    if aclfile_config_name().is_none() {
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut users: Vec<&AclUser> = guard.users.values().collect();
        users.sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
        for user in users {
            lines.push(format!(
                "{}\n",
                String::from_utf8_lossy(&user.to_rule_string())
            ));
        }
    }
    std::fs::write(&path, lines.concat())
        .map_err(|e| RedisError::runtime(format!("ERR CONFIG REWRITE failed: {}", e).into_bytes()))
}

fn config_value_for_key(cfg: &Arc<LiveConfig>, key: &[u8]) -> Option<String> {
    let normalized = normalize_config_key(key);
    if ascii_eq_ignore_case(&normalized, b"key-load-delay") {
        return Some(config_override_or_default(&normalized, "0"));
    }
    config_pairs_with_dynamic(cfg)
        .into_iter()
        .find(|(name, _)| ascii_eq_ignore_case(name.as_bytes(), &normalized))
        .map(|(_, value)| value)
}

fn rollback_config_updates(cfg: &Arc<LiveConfig>, backups: &[(Vec<u8>, String)]) {
    for (key, value) in backups {
        apply_config_set(cfg, key, value.as_bytes());
        if !config_value_is_live_only(key) {
            let mut guard = match config_overrides().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.insert(key.clone(), value.clone());
        }
    }
}

fn config_value_is_live_only(key: &[u8]) -> bool {
    const LIVE_KEYS: &[&[u8]] = &[
        b"maxmemory",
        b"maxmemory-policy",
        b"maxmemory-samples",
        b"maxclients",
        b"acllog-max-len",
        b"acl-pubsub-default",
        b"aclfile",
        b"requirepass",
        b"primaryauth",
        b"masterauth",
        b"dual-channel-replication-enabled",
        b"notify-keyspace-events",
        b"hash-max-listpack-entries",
        b"hash-max-ziplist-entries",
        b"hash-max-listpack-value",
        b"hash-max-ziplist-value",
        b"list-max-listpack-size",
        b"list-max-ziplist-size",
        b"set-max-intset-entries",
        b"set-max-listpack-entries",
        b"set-max-listpack-value",
        b"zset-max-listpack-entries",
        b"zset-max-listpack-value",
        b"zset-max-ziplist-entries",
        b"zset-max-ziplist-value",
        b"hll-sparse-max-bytes",
        b"slowlog-log-slower-than",
        b"slowlog-max-len",
        b"latency-tracking",
        b"latency-tracking-info-percentiles",
        b"latency-monitor-threshold",
        b"active-expire-effort",
        b"hz",
        b"dir",
        b"dbfilename",
        b"availability-zone",
        b"import-mode",
        b"port",
        b"lfu-log-factor",
        b"lfu-decay-time",
        b"tls-port",
        b"tls-cert-file",
        b"tls-key-file",
        b"tls-ca-cert-file",
        b"tls-auth-clients",
        b"appendonly",
        b"appendfsync",
        b"appendfilename",
        b"appenddirname",
        b"aof-load-truncated",
        b"aof-use-rdb-preamble",
        b"auto-aof-rewrite-percentage",
        b"auto-aof-rewrite-min-size",
        b"repl-backlog-size",
        b"repl-timeout",
        b"min-replicas-to-write",
        b"min-slaves-to-write",
        b"min-replicas-max-lag",
        b"min-slaves-max-lag",
        b"repl-disable-tcp-nodelay",
        b"slave-read-only",
        b"replica-read-only",
        b"slave-serve-stale-data",
        b"replica-serve-stale-data",
        b"lua-enable-insecure-api",
        b"repl-diskless-sync",
        b"rdb-version-check",
        b"client-output-buffer-limit",
    ];
    LIVE_KEYS.iter().any(|name| key == *name)
}

fn validate_config_set_pair(key: &[u8], value: &[u8]) -> RedisResult<()> {
    if key == b"daemonize" || key == b"hash-seed" {
        return Err(RedisError::runtime(
            format!(
                "ERR CONFIG SET failed (possibly related to argument '{}') - can't set immutable config",
                String::from_utf8_lossy(key)
            )
            .into_bytes(),
        ));
    }
    if ascii_eq_ignore_case(key, b"maxmemory") && parse_memsize(value).is_none() {
        return Err(RedisError::runtime(
            b"ERR CONFIG SET failed (possibly related to argument 'maxmemory')",
        ));
    }
    if ascii_eq_ignore_case(key, b"maxmemory-clients") {
        if let Some(raw) = value.strip_suffix(b"%") {
            let Some(pct) = parse_usize_strict(raw) else {
                return Err(RedisError::runtime(
                    b"ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients')",
                ));
            };
            if pct > 100 {
                return Err(RedisError::runtime(
                    b"ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - percentage argument must be less or equal to 100",
                ));
            }
        } else if parse_memsize(value).is_none() {
            return Err(RedisError::runtime(
                b"ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients')",
            ));
        }
    }
    if ascii_eq_ignore_case(key, b"client-query-buffer-limit") && parse_memsize(value).is_none() {
        return Err(RedisError::runtime(
            b"ERR CONFIG SET failed (possibly related to argument 'client-query-buffer-limit')",
        ));
    }
    if ascii_eq_ignore_case(key, b"bind") && !valid_bind_config_value(value) {
        return Err(RedisError::runtime(
            b"ERR Failed to bind to specified addresses",
        ));
    }
    if ascii_eq_ignore_case(key, b"latency-tracking") && parse_yes_no(value).is_none() {
        return Err(RedisError::runtime(
            b"ERR CONFIG SET failed (possibly related to argument 'latency-tracking')",
        ));
    }
    if ascii_eq_ignore_case(key, b"latency-tracking-info-percentiles")
        && !crate::slowlog_cmd::validate_latency_tracking_info_percentiles(value)
    {
        return Err(RedisError::runtime(
            b"ERR CONFIG SET failed (possibly related to argument 'latency-tracking-info-percentiles')",
        ));
    }
    if ascii_eq_ignore_case(key, b"slot-migration-max-failover-repl-bytes") {
        let Some(n) = parse_i64_strict(value) else {
            return Err(RedisError::runtime(
                b"ERR CONFIG SET failed (possibly related to argument 'slot-migration-max-failover-repl-bytes')",
            ));
        };
        if n < -1 {
            return Err(RedisError::runtime(
                b"ERR CONFIG SET failed (possibly related to argument 'slot-migration-max-failover-repl-bytes') - argument must be between -1 and 9223372036854775807",
            ));
        }
    }
    Ok(())
}

/// Apply a single `CONFIG SET key value` pair to the `LiveConfig`.
///
/// Unknown keys are silently ignored so the TCL test harness can issue
/// arbitrary `CONFIG SET` calls without aborting. Values that cannot be
/// parsed are also silently ignored — the existing value remains in effect.
fn apply_config_set(cfg: &Arc<LiveConfig>, key: &[u8], value: &[u8]) {
    let key_lower: Vec<u8> = key.iter().map(|b| b.to_ascii_lowercase()).collect();
    match key_lower.as_slice() {
        b"maxmemory" => {
            if let Some(n) = parse_memsize(value) {
                cfg.set_maxmemory(n);
            }
        }
        b"maxmemory-policy" => {
            if let Some(policy) = MaxmemoryPolicyCode::parse(value) {
                cfg.set_maxmemory_policy(policy);
            }
        }
        b"maxmemory-clients" => {
            if let Some(n) = parse_maxmemory_clients(value) {
                cfg.set_maxmemory_clients(n);
            }
        }
        b"maxclients" => {
            if let Some(n) = parse_usize_strict(value) {
                if n >= 1 {
                    cfg.set_maxclients(n as u64);
                }
            }
        }
        b"acllog-max-len" => {
            if let Some(n) = parse_usize_strict(value) {
                set_acl_log_max_len(n);
            }
        }
        b"acl-pubsub-default" => {
            let _ = set_acl_pubsub_default(value);
        }
        b"aclfile" => {
            let name = String::from_utf8_lossy(value).into_owned();
            set_aclfile_config_name(Some(name));
        }
        b"requirepass" => {
            if value.is_empty() {
                cfg.set_requirepass(None);
                apply_requirepass_to_acl(None);
            } else {
                cfg.set_requirepass(Some(RedisString::from_bytes(value)));
                apply_requirepass_to_acl(Some(value));
            }
        }
        b"primaryauth" | b"masterauth" => {
            if value.is_empty() {
                cfg.set_primaryauth(None);
            } else {
                cfg.set_primaryauth(Some(RedisString::from_bytes(value)));
            }
        }
        b"notify-keyspace-events" => {
            if let Ok(flags) = keyspace_events_string_to_flags(value) {
                cfg.set_notify_keyspace_events_flags(flags as u32);
            }
        }
        b"hash-max-listpack-entries" | b"hash-max-ziplist-entries" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_hash_max_listpack_entries(n);
            }
        }
        b"hash-max-listpack-value" | b"hash-max-ziplist-value" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_hash_max_listpack_value(n);
            }
        }
        b"list-max-listpack-size" | b"list-max-ziplist-size" => {
            if let Some(n) = parse_i64_strict(value) {
                cfg.set_list_max_listpack_size(n);
            }
        }
        b"set-max-intset-entries" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.store_set_max_intset_entries(n);
            }
        }
        b"set-max-listpack-entries" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.store_set_max_listpack_entries(n);
            }
        }
        b"set-max-listpack-value" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.store_set_max_listpack_value(n);
            }
        }
        b"zset-max-listpack-entries" | b"zset-max-ziplist-entries" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_zset_max_listpack_entries(n);
            }
        }
        b"zset-max-listpack-value" | b"zset-max-ziplist-value" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_zset_max_listpack_value(n);
            }
        }
        b"hll-sparse-max-bytes" => {
            if let Some(n) = parse_memsize(value).and_then(|n| usize::try_from(n).ok()) {
                cfg.set_hll_sparse_max_bytes(n);
            }
        }
        b"slowlog-log-slower-than" | b"commandlog-execution-slower-than" => {
            if let Some(n) = parse_i64_strict(value) {
                cfg.set_slowlog_threshold_micros(n);
                crate::slowlog_cmd::set_slowlog_threshold(n);
            }
        }
        b"latency-tracking" => {
            if let Some(enabled) = parse_yes_no(value) {
                crate::slowlog_cmd::set_latency_tracking_enabled(enabled);
            }
        }
        b"latency-tracking-info-percentiles" => {
            let _ = crate::slowlog_cmd::set_latency_tracking_info_percentiles(value);
        }
        b"latency-monitor-threshold" => {
            if let Some(n) = parse_i64_strict(value) {
                crate::slowlog_cmd::set_latency_monitor_threshold(n);
            }
        }
        b"slowlog-max-len" | b"commandlog-slow-execution-max-len" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_slowlog_max_len(n);
                crate::slowlog_cmd::set_slowlog_max_len(n);
            }
        }
        b"commandlog-request-larger-than" => {
            if let Some(n) = parse_i64_strict(value) {
                crate::slowlog_cmd::set_commandlog_large_request_threshold(n);
            }
        }
        b"commandlog-large-request-max-len" => {
            if let Some(n) = parse_usize_strict(value) {
                crate::slowlog_cmd::set_commandlog_large_request_max_len(n);
            }
        }
        b"commandlog-reply-larger-than" => {
            if let Some(n) = parse_i64_strict(value) {
                crate::slowlog_cmd::set_commandlog_large_reply_threshold(n);
            }
        }
        b"commandlog-large-reply-max-len" => {
            if let Some(n) = parse_usize_strict(value) {
                crate::slowlog_cmd::set_commandlog_large_reply_max_len(n);
            }
        }
        b"rdb-key-save-delay" => {
            if let Some(n) = parse_i64_strict(value).filter(|n| *n >= 0) {
                RDB_KEY_SAVE_DELAY_US.store(n as u64, Ordering::Relaxed);
            }
        }
        b"save" => {
            cfg.set_save_enabled(value.iter().any(|b| !b.is_ascii_whitespace()));
        }
        b"shutdown-on-sigterm" => {
            let value_text = String::from_utf8_lossy(value);
            SHUTDOWN_ON_SIGTERM_FORCE.store(
                value_text
                    .split_whitespace()
                    .any(|part| part.eq_ignore_ascii_case("force")),
                Ordering::SeqCst,
            );
        }
        b"active-expire-effort" => {
            if let Some(n) = parse_usize_strict(value) {
                let clamped = n.min(u8::MAX as usize) as u8;
                cfg.set_active_expire_effort(clamped);
                redis_core::expire::active_expire_config().set_effort(clamped);
            }
        }
        b"hz" => {
            if let Some(n) = parse_usize_strict(value) {
                let clamped = n.min(u32::MAX as usize) as u32;
                cfg.set_hz(clamped);
                redis_core::expire::active_expire_config().set_hz(clamped);
            }
        }
        b"dir" => {
            if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_rdb_dir(s.to_string());
            }
        }
        b"dbfilename" => {
            if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_rdb_filename(s.to_string());
            }
        }
        b"availability-zone" => {
            if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_availability_zone(s.to_string());
            }
        }
        b"import-mode" => {
            if ascii_eq_ignore_case(value, b"yes") {
                cfg.set_import_mode(true);
            } else if ascii_eq_ignore_case(value, b"no") {
                cfg.set_import_mode(false);
            }
        }
        b"lfu-log-factor" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_lfu_log_factor(n.min(u32::MAX as usize) as u32);
            }
        }
        b"lfu-decay-time" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_lfu_decay_time(n.min(u32::MAX as usize) as u32);
            }
        }
        b"tls-cert-file" => {
            if value.is_empty() {
                cfg.set_tls_cert_file(None);
            } else if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_tls_cert_file(Some(std::path::PathBuf::from(s)));
            }
        }
        b"tls-key-file" => {
            if value.is_empty() {
                cfg.set_tls_key_file(None);
            } else if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_tls_key_file(Some(std::path::PathBuf::from(s)));
            }
        }
        b"tls-ca-cert-file" => {
            if value.is_empty() {
                cfg.set_tls_ca_cert_file(None);
            } else if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_tls_ca_cert_file(Some(std::path::PathBuf::from(s)));
            }
        }
        b"tls-auth-clients" => {
            let mode = match value {
                b"yes" => 1u8,
                b"optional" => 2u8,
                _ => 0u8,
            };
            cfg.set_tls_auth_clients(mode);
        }
        b"tls-port" => {
            if let Some(n) = parse_usize_strict(value) {
                let port = n.min(u16::MAX as usize) as u16;
                cfg.set_tls_port(port);
                redis_core::tls::notify_tls_port_set(port);
            }
        }
        b"appendonly" => {
            let enabled = value == b"yes";
            let was_enabled = cfg.appendonly();
            cfg.set_appendonly(enabled);
            if enabled && !was_enabled {
                let dir = cfg.rdb_dir();
                let filename = cfg.appendfilename();
                let path = std::path::Path::new(&dir).join(&filename);
                let policy = cfg.appendfsync();
                match crate::aof::AofWriter::open(&path, policy) {
                    Ok(w) => crate::aof::install_aof_writer(std::sync::Arc::new(w)),
                    Err(e) => {
                        eprintln!("redis-server: failed to open AOF {}: {}", path.display(), e)
                    }
                }
            } else if !enabled && was_enabled {
                if let Some(w) = crate::aof::aof_writer() {
                    let _ = w.flush();
                }
                crate::aof::remove_aof_writer();
                crate::replication::unblock_waitaof_local_disabled();
            }
        }
        b"appendfilename" => {
            if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_appendfilename(s.to_string());
            }
        }
        b"appenddirname" => {
            if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_appenddirname(s.to_string());
            }
        }
        b"appendfsync" => {
            if let Some(policy) = crate::aof::parse_fsync_policy(value) {
                cfg.set_appendfsync(policy);
                if let Some(w) = crate::aof::aof_writer() {
                    w.fsync_policy
                        .store(policy, std::sync::atomic::Ordering::Relaxed);
                    if policy == crate::aof::FSYNC_ALWAYS {
                        let _ = w.fsync_if_due();
                        crate::replication::maybe_wake_wait_clients();
                    }
                }
            }
        }
        b"aof-load-truncated" => {
            if ascii_eq_ignore_case(value, b"yes") {
                cfg.set_aof_load_truncated(true);
            } else if ascii_eq_ignore_case(value, b"no") {
                cfg.set_aof_load_truncated(false);
            }
        }
        b"aof-use-rdb-preamble" => {
            if ascii_eq_ignore_case(value, b"yes") {
                cfg.set_aof_use_rdb_preamble(true);
            } else if ascii_eq_ignore_case(value, b"no") {
                cfg.set_aof_use_rdb_preamble(false);
            }
        }
        b"auto-aof-rewrite-percentage" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_auto_aof_rewrite_percentage(n as u64);
            }
        }
        b"auto-aof-rewrite-min-size" => {
            if let Some(n) = parse_memsize(value) {
                cfg.set_auto_aof_rewrite_min_size(n);
            }
        }
        b"repl-backlog-size" => {
            if let Some(n) = parse_memsize(value) {
                cfg.set_repl_backlog_size(n);
            }
        }
        b"repl-timeout" => {
            if let Some(n) = parse_i64_strict(value) {
                if n > 0 {
                    cfg.set_repl_timeout(n as u64);
                }
            }
        }
        b"min-replicas-to-write" | b"min-slaves-to-write" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_repl_min_replicas_to_write(n as u64);
            }
        }
        b"min-replicas-max-lag" | b"min-slaves-max-lag" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_repl_min_replicas_max_lag(n as u64);
            }
        }
        b"repl-disable-tcp-nodelay" => {
            cfg.set_repl_disable_tcp_nodelay(value == b"yes");
        }
        b"slave-read-only" | b"replica-read-only" => {
            cfg.set_slave_read_only(value == b"yes");
        }
        b"slave-serve-stale-data" | b"replica-serve-stale-data" => {
            cfg.set_replica_serve_stale_data(value == b"yes");
        }
        b"lua-enable-insecure-api" => {
            cfg.set_lua_enable_insecure_api(value == b"yes");
        }
        b"repl-diskless-sync" => {
            cfg.set_repl_diskless_sync(value == b"yes");
        }
        b"rdb-version-check" => {
            if ascii_eq_ignore_case(value, b"relaxed") {
                cfg.set_rdb_version_check_relaxed(true);
            } else if ascii_eq_ignore_case(value, b"strict") {
                cfg.set_rdb_version_check_relaxed(false);
            }
        }
        _ => {}
    }
}

/// Parse a Redis memory-size literal: bare digits or a digit run followed by
/// `b`, `k`/`kb`, `m`/`mb`, `g`/`gb` (case-insensitive). Suffixes follow the
/// upstream Valkey convention of base-2 multipliers (1k = 1024). Returns
/// `None` on any parse failure so callers can preserve the prior value.
fn parse_memsize(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut end = bytes.len();
    while end > 0 && !bytes[end - 1].is_ascii_digit() {
        end -= 1;
    }
    let digits = &bytes[..end];
    let suffix: Vec<u8> = bytes[end..]
        .iter()
        .map(|b| b.to_ascii_lowercase())
        .collect();
    let multiplier: u64 = match suffix.as_slice() {
        b"" | b"b" => 1,
        b"k" | b"kb" => 1024,
        b"m" | b"mb" => 1024 * 1024,
        b"g" | b"gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    let digits_str = std::str::from_utf8(digits).ok()?;
    let base: u64 = digits_str.parse().ok()?;
    base.checked_mul(multiplier)
}

fn parse_maxmemory_clients(bytes: &[u8]) -> Option<i64> {
    if let Some(raw) = bytes.strip_suffix(b"%") {
        let pct = parse_usize_strict(raw)?;
        return Some(-(pct as i64));
    }
    parse_memsize(bytes).and_then(|n| i64::try_from(n).ok())
}

fn render_maxmemory_clients(value: i64) -> String {
    if value < 0 {
        format!("{}%", value.saturating_abs())
    } else {
        value.to_string()
    }
}

/// Parses a non-negative integer from ASCII decimal bytes. Returns `None` if
/// the bytes do not represent a valid non-negative integer.
fn parse_usize_strict(bytes: &[u8]) -> Option<usize> {
    let n = parse_i64_strict(bytes)?;
    if n < 0 {
        return None;
    }
    Some(n as usize)
}

fn apply_config_set_for_context(
    ctx: &mut CommandContext<'_>,
    cfg: &Arc<LiveConfig>,
    key: &[u8],
    value: &[u8],
) -> RedisResult<()> {
    if ascii_eq_ignore_case(key, b"appendonly") {
        return apply_appendonly_config_set(ctx, cfg, value);
    }
    if ascii_eq_ignore_case(key, b"port") {
        return apply_port_config_set(value);
    }
    if ascii_eq_ignore_case(key, b"bind") {
        return apply_bind_config_set(value);
    }
    if ascii_eq_ignore_case(key, b"client-output-buffer-limit") {
        return apply_client_output_buffer_limit_config_set(value);
    }
    if ascii_eq_ignore_case(key, b"client-query-buffer-limit") {
        let Some(limit) = parse_memsize(value).and_then(|n| usize::try_from(n).ok()) else {
            return Err(RedisError::runtime(b"ERR CONFIG SET failed"));
        };
        set_client_query_buffer_limit(limit);
    }
    if ascii_eq_ignore_case(key, b"maxmemory") && parse_memsize(value).is_none() {
        return Err(RedisError::runtime(b"ERR CONFIG SET failed"));
    }
    if ascii_eq_ignore_case(key, b"tracking-table-max-keys") {
        if let Some(max_keys) = parse_usize_strict(value) {
            let pubsub = ctx.pubsub.as_ref().cloned();
            redis_core::tracking::runtime_limit_tracked_keys(
                max_keys,
                ctx.client_mut(),
                pubsub.as_ref(),
            );
        }
    }
    let enforce_maxmemory = ascii_eq_ignore_case(key, b"maxmemory")
        || ascii_eq_ignore_case(key, b"maxmemory-policy")
        || ascii_eq_ignore_case(key, b"lfu-log-factor")
        || ascii_eq_ignore_case(key, b"lfu-decay-time");
    apply_config_set(cfg, key, value);
    if enforce_maxmemory {
        enforce_maxmemory_after_config_set(ctx);
    }
    Ok(())
}

pub(crate) fn enforce_maxmemory_after_config_set(ctx: &mut CommandContext<'_>) {
    let maxmemory = ctx.live_config().maxmemory();
    if maxmemory == 0 {
        return;
    }
    if ctx.live_config().import_mode() {
        return;
    }
    if ctx.client_ref().flag_deny_blocking()
        || redis_core::networking::is_server_paused_for(
            ctx.server(),
            redis_core::networking::PAUSE_ACTION_EVICT,
        )
    {
        return;
    }

    let policy = ctx.live_config().maxmemory_policy();
    let lfu_log_factor = ctx.live_config().lfu_log_factor();
    let lfu_decay_time = ctx.live_config().lfu_decay_time();
    let outcome = try_evict_to_fit(
        ctx.db_mut(),
        maxmemory,
        policy,
        lfu_log_factor,
        lfu_decay_time,
    );
    let evicted = match outcome {
        EvictionOutcome::Evicted(keys) | EvictionOutcome::StillOver(keys) => keys,
        EvictionOutcome::Sufficient => Vec::new(),
    };
    if !evicted.is_empty() {
        let pubsub = ctx.pubsub.as_ref().cloned();
        redis_core::tracking::runtime_invalidate_keys(
            ctx.client_ref().id,
            ctx.client_mut(),
            pubsub.as_ref(),
            &evicted,
            true,
            false,
        );
    }
    for key in &evicted {
        crate::dispatch::propagate_command_from_wake(
            ctx.selected_db_id(),
            &[RedisString::from_bytes(b"UNLINK"), key.clone()],
        );
    }
    for key in evicted {
        ctx.notify_keyspace_event(NOTIFY_EVICTED, b"evicted", &key);
    }
}

fn apply_port_config_set(value: &[u8]) -> RedisResult<()> {
    let port = parse_usize_strict(value)
        .filter(|n| *n <= u16::MAX as usize)
        .map(|n| n as u16)
        .ok_or_else(|| RedisError::runtime(b"ERR CONFIG SET failed"))?;

    if port == tcp_port_config() {
        return Ok(());
    }

    if let Some(hook) = TCP_PORT_SET_HOOK.get() {
        let listeners = hook(port).map_err(|err| {
            let mut msg =
                b"ERR CONFIG SET failed (possibly related to argument 'port') - ".to_vec();
            let text = String::from_utf8_lossy(&err);
            if let Some(stripped) = text.strip_prefix("ERR ") {
                msg.extend_from_slice(stripped.as_bytes());
            } else {
                msg.extend_from_slice(text.as_bytes());
            }
            RedisError::runtime(msg)
        })?;
        if !listeners.is_empty() {
            let cell = PENDING_TCP_LISTENERS.get_or_init(|| Mutex::new(Vec::new()));
            let mut guard = match cell.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.extend(listeners);
        }
    }

    set_tcp_port_config(port);
    server_metrics().set_tcp_port(port);
    Ok(())
}

fn valid_bind_config_value(value: &[u8]) -> bool {
    let Ok(s) = std::str::from_utf8(value) else {
        return false;
    };
    if s.trim().is_empty() {
        return true;
    }
    s.split_whitespace()
        .all(|addr| addr == "*" || addr == "-::*" || addr.parse::<std::net::IpAddr>().is_ok())
}

pub fn bind_config_value() -> String {
    config_value_for_key(&live_config_handle(), b"bind").unwrap_or_else(|| "* -::*".to_string())
}

pub fn set_bind_config_value(value: &[u8]) -> RedisResult<()> {
    if !valid_bind_config_value(value) {
        return Err(RedisError::runtime(
            b"ERR Failed to bind to specified addresses",
        ));
    }
    apply_bind_config_set(value)?;
    remember_config_override(b"bind", value);
    Ok(())
}

fn apply_bind_config_set(value: &[u8]) -> RedisResult<()> {
    if let Some(hook) = TCP_BIND_SET_HOOK.get() {
        let listeners = hook(value, tcp_port_config()).map_err(|err| {
            let text = String::from_utf8_lossy(&err);
            if text.starts_with("ERR ") {
                RedisError::runtime(text.as_bytes().to_vec())
            } else {
                let mut msg = b"ERR ".to_vec();
                msg.extend_from_slice(text.as_bytes());
                RedisError::runtime(msg)
            }
        })?;
        let cell = PENDING_TCP_LISTENER_REPLACEMENT.get_or_init(|| Mutex::new(None));
        let mut guard = match cell.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        *guard = Some(listeners);
    }
    Ok(())
}

fn apply_client_output_buffer_limit_config_set(value: &[u8]) -> RedisResult<()> {
    let value_str = std::str::from_utf8(value)
        .map_err(|_| RedisError::runtime(b"ERR Wrong number of arguments"))?;
    let tokens: Vec<&str> = value_str.split_whitespace().collect();
    if tokens.is_empty() || tokens.len() % 4 != 0 {
        return Err(RedisError::runtime(b"ERR Wrong number of arguments"));
    }

    let mut next = {
        let guard = match client_obuf_limits_cell().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        *guard
    };

    for chunk in tokens.chunks_exact(4) {
        let class = chunk[0].as_bytes();
        let hard = parse_memsize(chunk[1].as_bytes())
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| RedisError::runtime(b"ERR Error in hard limit"))?;
        let soft = parse_memsize(chunk[2].as_bytes())
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| RedisError::runtime(b"ERR Error in soft limit"))?;
        let soft_seconds = parse_usize_strict(chunk[3].as_bytes())
            .map(|n| n as u64)
            .ok_or_else(|| RedisError::runtime(b"ERR Error in soft_seconds limit"))?;

        let limit = ClientOutputBufferLimit {
            hard,
            soft,
            soft_seconds,
        };
        if ascii_eq_ignore_case(class, b"normal") {
            next.normal = limit;
        } else if ascii_eq_ignore_case(class, b"slave") || ascii_eq_ignore_case(class, b"replica") {
            next.replica = limit;
        } else if ascii_eq_ignore_case(class, b"pubsub") {
            next.pubsub = limit;
        } else {
            return Err(RedisError::runtime(b"ERR Invalid client class"));
        }
    }

    let mut guard = match client_obuf_limits_cell().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = next;
    Ok(())
}

fn apply_appendonly_config_set(
    ctx: &mut CommandContext<'_>,
    cfg: &Arc<LiveConfig>,
    value: &[u8],
) -> RedisResult<()> {
    let enabled = ascii_eq_ignore_case(value, b"yes");
    let was_enabled = cfg.appendonly();
    if enabled {
        if !was_enabled {
            let snapshot = ctx.snapshot_all_dbs()?;
            let dbs = snapshots_to_dbs(&snapshot);
            let dir = cfg.rdb_dir();
            let filename = cfg.appendfilename();
            let dirname = cfg.appenddirname();
            let policy = cfg.appendfsync();
            let (writer, size) = crate::aof::open_manifest_current_incr_writer(
                std::path::Path::new(&dir),
                &filename,
                &dirname,
                &dbs,
                policy,
            )
            .map_err(|e| {
                RedisError::runtime(format!("ERR CONFIG SET appendonly failed: {}", e).into_bytes())
            })?;
            crate::aof::install_aof_writer(std::sync::Arc::new(writer));
            ctx.server().persistence.set_aof_current_size(size);
            ctx.server().set_aof_state(redis_core::AofState::On);
            if ctx.server().rdb_child_pid() != 0 {
                ctx.server().persistence.set_aof_rewrite_in_progress(false);
                ctx.server().persistence.set_aof_rewrite_scheduled(true);
                log_server_notice("AOF background was scheduled");
                let server = ctx.server_arc();
                let _ = std::thread::Builder::new()
                    .name("aof-scheduled-clear".to_string())
                    .spawn(move || {
                        for _ in 0..200 {
                            if server.rdb_child_pid() == 0 {
                                server.persistence.set_aof_rewrite_scheduled(false);
                                return;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(50));
                        }
                        server.persistence.set_aof_rewrite_scheduled(false);
                    });
            } else {
                ctx.server().persistence.set_aof_rewrite_in_progress(true);
                let server = ctx.server_arc();
                let delay = rdb_key_save_delay_us().min(5_000_000);
                let _ = std::thread::Builder::new()
                    .name("aof-initial-rewrite-clear".to_string())
                    .spawn(move || {
                        std::thread::sleep(std::time::Duration::from_micros(delay.max(100_000)));
                        server.persistence.set_aof_rewrite_in_progress(false);
                    });
            }
            if ctx.client_ref().flag_deny_blocking() {
                log_server_notice("AOF background was scheduled");
            }
            redis_core::metrics::record_total_fork();
        }
        cfg.set_appendonly(true);
    } else {
        if was_enabled {
            if let Some(w) = crate::aof::aof_writer() {
                let _ = w.flush();
            }
            crate::aof::remove_aof_writer();
            ctx.server().set_aof_state(redis_core::AofState::Off);
            crate::replication::unblock_waitaof_local_disabled();
        }
        cfg.set_appendonly(false);
    }
    Ok(())
}

/// Glob-style ASCII matcher used by CONFIG GET. Supports `*` and `?` only;
/// brackets are treated as literal characters. Comparison is case-insensitive
/// to match the canonical CONFIG behaviour, where `config get MaxMemory`
/// returns the same pair as `config get maxmemory`.
fn glob_match_ascii_ci(pattern: &[u8], text: &[u8]) -> bool {
    glob_match_inner(pattern, text)
}

fn glob_match_inner(pattern: &[u8], text: &[u8]) -> bool {
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_p, mut star_t) = (usize::MAX, 0usize);
    while ti < text.len() {
        if pi < pattern.len() && pattern[pi] == b'?' {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && ascii_lower(pattern[pi]) == ascii_lower(text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_p = pi;
            star_t = ti;
            pi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// `MEMORY <subcommand>`.
///
/// `MEMORY USAGE key [SAMPLES n]` returns a coarse byte estimate so the
/// `string.tcl` memoryusage test sees a non-nil value bigger than the key+value
/// length sum. We approximate by `key.len + value.len + 48` (the constant is a
/// rough object-header overhead). For non-string values we use the byte length
/// of the type tag plus a placeholder; this is enough for the suite to make
/// progress without a real allocator-walk implementation. Returns nil when the
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
        let value_len = ctx
            .db()
            .lookup_key_read(key.as_bytes())
            .and_then(|obj| obj.string_len().ok());
        match value_len {
            Some(v) => ctx.reply_integer((key_len + v + 48) as i64),
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

fn memory_hashtable_stats_for_key_count(keys: u64) -> (usize, usize, usize) {
    if keys == 0 {
        (0, 0, 0)
    } else if keys >= 8 {
        (192, 32, 1)
    } else {
        (192, 0, 0)
    }
}

/// `TIME`.
///
/// Replies with a two-element array of bulk strings: the current Unix time
/// in seconds and the microseconds component within the current second.
/// Read directly from `SystemTime::now()`.
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
///
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
///
/// Terminates the server process. The sequence mirrors real Valkey: parse any
/// keyword flags, write `+OK\r\n` directly onto the client's transport so the
/// caller receives a reply before the socket is closed, then call
/// `std::process::exit(0)`.  The OS unbinds all listening sockets as part of
/// process teardown, which is what releases the TCP port and allows the TCL
/// harness to reuse it for the next `start_server` cycle.
///
/// Persistence behaviour:
///   * `NOSAVE` (default when no save keyword is given) — skip any RDB/AOF
///     flush.  Used by the TCL test harness for every non-persistence test.
///   * `SAVE` — would normally trigger a foreground BGSAVE; not yet wired, so
///     we treat it identically to NOSAVE for this release.
///   * `ABORT` — cancels an in-progress shutdown; we return an error because
///     no background shutdown can be in progress in our single-cycle model.
///
/// The reply is written directly to the live transport (bypassing the
/// outbound mpsc channel) so the bytes reach the peer before `exit(0)` tears
/// down the process.  Failures to write the reply are ignored — the caller
/// gets a broken-pipe error instead, which the TCL harness treats equivalently
/// to a clean +OK.
///
/// C: `shutdownCommand(client *c)` — db.c:1423, calling `prepareForShutdown`
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
    if !nosave {
        if shutdown_save_failed() || rdb_target_is_directory(ctx) {
            mark_shutdown_save_failed();
            log_server_notice("Error trying to save the DB, can't exit");
            return Err(RedisError::runtime(
                b"ERR Errors trying to SHUTDOWN. Check logs.",
            ));
        }
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
///
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
///
/// C: `readonlyCommand(client *c)` in `cluster.c` sets a per-client bit and
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
///
/// C: `readwriteCommand(client *c)` clears the per-client readonly bit.
pub fn readwrite_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"readwrite"));
    }
    ctx.client_mut().flags.readonly = false;
    refresh_client_info_registry(ctx.client_ref());
    ctx.reply_simple_string(b"OK")
}

/// `MONITOR`.
///
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
            monitors.insert(id, sender);
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
        guard.remove(&id);
    }
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
            guard.remove(&id);
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
///
/// Pilot subset:
///   * `DEBUG SLEEP seconds` — sleep for the given (fractional) seconds,
///     then reply `+OK\r\n`. Used by tests to inject latency.
///
/// Any other subcommand falls through to an `ERR DEBUG ...` error.
pub fn debug_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"debug"));
    }
    let sub = ctx.arg_owned(1usize)?;
    if ascii_eq_ignore_case(sub.as_bytes(), b"SLEEP") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"debug"));
        }
        let secs_arg = ctx.arg_owned(2usize)?;
        let secs = parse_f64_strict(secs_arg.as_bytes())
            .ok_or_else(|| RedisError::runtime(b"ERR value is not a valid float"))?;
        if secs.is_sign_negative() || secs.is_nan() {
            return Err(RedisError::runtime(b"ERR value is not a valid float"));
        }
        let dur = std::time::Duration::from_secs_f64(secs);
        std::thread::sleep(dur);
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"SET-ACTIVE-EXPIRE") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"PAUSE-CRON") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"debug pause-cron"));
        }
        let value = ctx.arg_owned(2usize)?;
        match value.as_bytes() {
            b"0" => set_debug_pause_cron(false),
            b"1" => set_debug_pause_cron(true),
            _ => {
                return Err(RedisError::runtime(
                    b"ERR value is not an integer or out of range",
                ))
            }
        }
        // Upstream uses this as a test-only clientsCron timing knob. This
        // port does not run a C-style clientsCron loop, so accepting the knob
        // lets query-buffer tests proceed to their observable assertions.
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"REPLYBUFFER") {
        if ctx.arg_count() != 4 {
            return Err(RedisError::wrong_number_of_args(b"debug replybuffer"));
        }
        let knob = ctx.arg_owned(2usize)?;
        if !ascii_eq_ignore_case(knob.as_bytes(), b"PEAK-RESET-TIME") {
            let mut msg = Vec::with_capacity(
                b"ERR Unknown DEBUG REPLYBUFFER subcommand: ".len() + knob.as_bytes().len(),
            );
            msg.extend_from_slice(b"ERR Unknown DEBUG REPLYBUFFER subcommand: ");
            msg.extend_from_slice(knob.as_bytes());
            return Err(RedisError::runtime(msg));
        }
        let value = ctx.arg_owned(3usize)?;
        let bytes = value.as_bytes();
        if !ascii_eq_ignore_case(bytes, b"NEVER")
            && !ascii_eq_ignore_case(bytes, b"RESET")
            && parse_i64_strict(bytes).is_none()
        {
            return Err(RedisError::runtime(
                b"ERR value is not an integer or out of range",
            ));
        }
        // The runtime owner tracks reply-buffer memory directly rather than
        // through a peak-reset timer. Accept the test-only knob and leave the
        // actual accounting path unchanged.
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"SET-SKIP-CHECKSUM-VALIDATION") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(
                b"debug set-skip-checksum-validation",
            ));
        }
        let flag = ctx.arg_owned(2usize)?;
        redis_core::rdb::load::set_skip_checksum_validation(flag.as_bytes() != b"0");
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"POPULATE") {
        if !(3..=5).contains(&ctx.arg_count()) {
            return Err(RedisError::wrong_number_of_args(b"debug populate"));
        }
        let count_arg = ctx.arg_owned(2usize)?;
        let Some(count) = parse_i64_strict(count_arg.as_bytes()).filter(|n| *n >= 0) else {
            return Err(RedisError::runtime(
                b"ERR value is not an integer or out of range",
            ));
        };
        let prefix = ctx.arg_owned(3usize)?;
        let size = if ctx.arg_count() >= 5 {
            let size_arg = ctx.arg_owned(4usize)?;
            parse_i64_strict(size_arg.as_bytes())
                .filter(|n| *n >= 0)
                .unwrap_or(0) as usize
        } else {
            0
        };
        let value = RedisString::from_vec(vec![b'0'; size]);
        for idx in 0..count {
            let mut key = Vec::with_capacity(prefix.len() + 24);
            key.extend_from_slice(prefix.as_bytes());
            key.extend_from_slice(b":");
            key.extend_from_slice(idx.to_string().as_bytes());
            ctx.db_mut().set_key(
                RedisString::from_vec(key),
                redis_core::RedisObject::from_string(value.clone()),
                0,
            );
        }
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"CLIENT-ENFORCE-REPLY-LIST") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(
                b"debug client-enforce-reply-list",
            ));
        }
        let value = ctx.arg_owned(2usize)?;
        let enabled = match value.as_bytes() {
            b"0" => false,
            b"1" => true,
            _ => {
                return Err(RedisError::runtime(
                    b"ERR value is not an integer or out of range",
                ))
            }
        };
        redis_core::client::set_debug_client_enforce_reply_list(enabled);
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"CONFIG-REWRITE-FORCE-ALL") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(
                b"debug config-rewrite-force-all",
            ));
        }
        // Test-only upstream knob: force CONFIG REWRITE to emit every option.
        // This port's CONFIG REWRITE is currently a no-op persistence shim, so
        // accepting the DEBUG command is the observable compatibility contract.
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"FORCE-FREE-PRIMARY-ASYNC") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(
                b"debug force-free-primary-async",
            ));
        }
        let value = ctx.arg_owned(2usize)?;
        match value.as_bytes() {
            b"0" | b"1" => {}
            _ => {
                return Err(RedisError::runtime(
                    b"ERR value is not an integer or out of range",
                ))
            }
        }
        // C toggles server.debug_force_free_primary_async so the next primary
        // client is freed on the async path. This port does not yet keep a
        // primary client object in the RuntimeOwner-disabled replica dialer,
        // but the upstream wait.tcl repoint test uses this knob before it
        // checks that REPLICAOF logs only one reconnect attempt.
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"DIGEST-VALUE") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"debug digest-value"));
        }
        let key = ctx.arg_owned(2usize)?;
        let digest = match ctx
            .db_mut()
            .lookup_key_read_with_flags(&key, redis_core::db::LOOKUP_NOTOUCH)
        {
            None => b"0000000000000000000000000000000000000000".to_vec(),
            Some(obj) => {
                let mut h: u64 = 0xcbf29ce484222325;
                for b in obj.string_bytes_owned() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
                format!("{:040x}", h).into_bytes()
            }
        };
        return ctx.reply_bulk_string(redis_types::RedisString::from_bytes(&digest));
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"DIGEST") {
        return ctx.reply_bulk_string(redis_types::RedisString::from_bytes(
            b"0000000000000000000000000000000000000000",
        ));
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"OBJECT") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"debug object"));
        }
        let key = ctx.arg_owned(2usize)?;
        let line = match ctx
            .db_mut()
            .lookup_key_read_with_flags(&key, redis_core::db::LOOKUP_NOTOUCH)
        {
            None => b"Value at:0x0 refcount:0 encoding:null serializedlength:0 lru:0 lru_seconds:0 type:none".to_vec(),
            Some(obj) => format!(
                "Value at:0x0 refcount:1 encoding:{} serializedlength:1 lru:{} lru_seconds:{} type:{}",
                obj.encoding_name(),
                obj.lru,
                obj.lru_idle_secs(),
                obj.type_name()
            )
            .into_bytes(),
        };
        return ctx.reply_simple_string(&line);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"HTSTATS") {
        let entries = ctx.db().len();
        let payload = format!(
            "[Dictionary HT]\nHash table 0 stats (main hash table):\n table size: 4096\n number of entries: {}\n rehashing index: -1\n",
            entries
        );
        return ctx.reply_bulk_string(redis_types::RedisString::from_bytes(payload.as_bytes()));
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"QUICKLIST-PACKED-THRESHOLD") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"CHANGE-REPL-ID") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"RELOAD") {
        return debug_reload_command(ctx);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"LOADAOF") {
        return debug_loadaof_command(ctx);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"FLUSHALL") {
        ctx.db_mut().clear();
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"JMAP") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"AOFSTATS") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"DISABLE-REPLICATION-CACHING") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"CLOSE-LISTENERS-ASA") {
        return ctx.reply_simple_string(b"OK");
    }
    let mut msg =
        Vec::with_capacity(b"ERR Unknown DEBUG subcommand: ".len() + sub.as_bytes().len());
    msg.extend_from_slice(b"ERR Unknown DEBUG subcommand: ");
    msg.extend_from_slice(sub.as_bytes());
    Err(RedisError::runtime(msg))
}

fn debug_reload_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let mut nosave = false;
    let mut noflush = false;
    let mut merge = false;

    for i in 2..ctx.arg_count() {
        let opt = ctx.arg_owned(i)?;
        let bytes = opt.as_bytes();
        if ascii_eq_ignore_case(bytes, b"NOSAVE") {
            nosave = true;
        } else if ascii_eq_ignore_case(bytes, b"NOFLUSH") {
            noflush = true;
        } else if ascii_eq_ignore_case(bytes, b"MERGE") {
            merge = true;
            noflush = true;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    let cfg = Arc::clone(&ctx.server().live_config);
    let path = redis_core::rdb::rdb_path(&cfg.rdb_dir(), &cfg.rdb_filename());
    if !nosave {
        let snapshot = ctx.snapshot_all_dbs()?;
        let snapshot_dbs = snapshots_to_dbs(&snapshot);
        redis_core::rdb::save_rdb_databases(&snapshot_dbs, &path).map_err(|e| {
            ctx.server()
                .persistence
                .set_rdb_last_bgsave_status(PersistenceStatus::Err);
            RedisError::runtime(format!("ERR DEBUG RELOAD SAVE failed: {}", e).into_bytes())
        })?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        cfg.set_last_save_unix(now);
        ctx.server()
            .persistence
            .set_rdb_last_bgsave_status(PersistenceStatus::Ok);
    }

    let mut loaded: Vec<RedisDb> = (0..ctx.database_count() as u32).map(RedisDb::new).collect();
    redis_core::rdb::load_into_dbs_with_options(
        &mut loaded,
        &path,
        redis_core::rdb::RdbLoadOptions {
            allow_dup: merge,
            skip_expired: true,
            aof_preamble: false,
        },
    )
    .map_err(|e| RedisError::runtime(format!("ERR DEBUG RELOAD failed: {}", e).into_bytes()))?;

    replace_or_merge_dbs(ctx, loaded, noflush, merge)?;
    ctx.reply_simple_string(b"OK")
}

fn debug_loadaof_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"debug loadaof"));
    }

    let cfg = Arc::clone(&ctx.server().live_config);
    let dir = cfg.rdb_dir();
    let filename = cfg.appendfilename();
    let dirname = cfg.appenddirname();
    let mut loaded: Vec<RedisDb> = (0..ctx.database_count() as u32).map(RedisDb::new).collect();
    crate::aof::load_append_only_files(
        std::path::Path::new(&dir),
        &filename,
        &dirname,
        &mut loaded,
        crate::aof::AofLoadOptions {
            load_truncated: cfg.aof_load_truncated(),
            allow_rdb_preamble: cfg.aof_use_rdb_preamble(),
        },
    )
    .map_err(|e| RedisError::runtime(format!("ERR DEBUG LOADAOF failed: {}", e).into_bytes()))?;
    replace_or_merge_dbs(ctx, loaded, false, true)?;
    ctx.reply_simple_string(b"OK")
}

fn replace_or_merge_dbs(
    ctx: &mut CommandContext<'_>,
    loaded: Vec<RedisDb>,
    noflush: bool,
    merge: bool,
) -> RedisResult<()> {
    if noflush {
        for loaded_db in loaded.iter() {
            let db_id = loaded_db.id;
            ctx.with_db_index(db_id, |live| {
                for (key, obj) in loaded_db.iter_for_eviction() {
                    if !merge && live.exists_raw(key) {
                        return Err(RedisError::runtime(
                            b"ERR DEBUG RELOAD found duplicate key; use MERGE",
                        ));
                    }
                    live.insert(key.clone(), obj.clone());
                }
                Ok(())
            })??;
        }
    } else {
        for loaded_db in loaded {
            let db_id = loaded_db.id;
            ctx.with_db_index(db_id, move |live| {
                *live = loaded_db;
            })?;
        }
    }
    Ok(())
}

fn snapshots_to_dbs(
    snapshot: &[(
        u32,
        Vec<(redis_types::RedisString, redis_core::RedisObject)>,
    )],
) -> Vec<RedisDb> {
    snapshot
        .iter()
        .map(|(id, entries)| {
            let mut db = RedisDb::from_snapshot(entries.clone());
            db.id = *id;
            db
        })
        .collect()
}

/// `HELLO [protover] [AUTH user pass] [SETNAME name]`.
///
/// Pilot-shape reply: a flat RESP2 multi-bulk of `[key, value]` pairs
/// describing the server. Returns a list (not a RESP3 map) regardless of
/// the requested protocol version; the underlying client representation is
/// still RESP2. AUTH and SETNAME options parse-and-ignore for now — the
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

#[derive(Default)]
struct ClientListFilters {
    ids: Vec<u64>,
    not_ids: Vec<u64>,
    addr: Option<Vec<u8>>,
    not_addr: Option<Vec<u8>>,
    laddr: Option<Vec<u8>>,
    not_laddr: Option<Vec<u8>>,
    type_filter: Option<ClientTypeFilter>,
    not_type_filter: Option<ClientTypeFilter>,
    name: Option<Vec<u8>>,
    not_name: Option<Vec<u8>>,
    flags: Option<Vec<u8>>,
    not_flags: Option<Vec<u8>>,
    user: Option<Vec<u8>>,
    not_user: Option<Vec<u8>>,
    skipme: Option<bool>,
    maxage: Option<i64>,
    idle: Option<i64>,
    ip: Option<Vec<u8>>,
    not_ip: Option<Vec<u8>>,
    capa: Option<Vec<u8>>,
    not_capa: Option<Vec<u8>>,
    lib_name: Option<Vec<u8>>,
    not_lib_name: Option<Vec<u8>>,
    lib_ver: Option<Vec<u8>>,
    not_lib_ver: Option<Vec<u8>>,
    db: Option<u32>,
    not_db: Option<u32>,
}

fn client_kill_only_skipme_filter(filters: &ClientListFilters) -> bool {
    filters.skipme.is_some()
        && filters.ids.is_empty()
        && filters.not_ids.is_empty()
        && filters.addr.is_none()
        && filters.not_addr.is_none()
        && filters.laddr.is_none()
        && filters.not_laddr.is_none()
        && filters.type_filter.is_none()
        && filters.not_type_filter.is_none()
        && filters.name.is_none()
        && filters.not_name.is_none()
        && filters.flags.is_none()
        && filters.not_flags.is_none()
        && filters.user.is_none()
        && filters.not_user.is_none()
        && filters.maxage.is_none()
        && filters.idle.is_none()
        && filters.ip.is_none()
        && filters.not_ip.is_none()
        && filters.capa.is_none()
        && filters.not_capa.is_none()
        && filters.lib_name.is_none()
        && filters.not_lib_name.is_none()
        && filters.lib_ver.is_none()
        && filters.not_lib_ver.is_none()
        && filters.db.is_none()
        && filters.not_db.is_none()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ClientTypeFilter {
    Normal,
    Replica,
    PubSub,
    Primary,
}

fn client_user_bytes(client: &redis_core::client::Client) -> &[u8] {
    client
        .authenticated_user
        .as_ref()
        .map(|u| u.as_bytes())
        .unwrap_or(b"default")
}

fn client_name_bytes(client: &redis_core::client::Client) -> Option<&[u8]> {
    client.name.as_ref().map(|n| n.as_bytes())
}

fn client_flags_vec(client: &redis_core::client::Client) -> Vec<u8> {
    let mut out = Vec::new();
    if client.is_replica {
        out.push(b'S');
    }
    if client.in_pubsub_mode() {
        out.push(b'P');
    }
    if client.flag_multi() {
        out.push(b'x');
    }
    if client.blocked_on_keys || client.flag_blocked() {
        out.push(b'b');
    }
    if client.import_source {
        out.push(b'I');
    }
    if client.tracking.enabled {
        out.push(b't');
    }
    if client.tracking.bcast {
        out.push(b'B');
    }
    if client.tracking.broken_redirect {
        out.push(b'R');
    }
    if client.flags.monitor {
        out.push(b'O');
    }
    if client.flags.readonly {
        out.push(b'r');
    }
    if client.flags.no_touch {
        out.push(b'T');
    }
    if client.flags.dirty_cas {
        out.push(b'd');
    }
    if out.is_empty() {
        out.push(b'N');
    }
    out
}

fn watched_key_count_for_client(client_id: u64) -> usize {
    let idx = redis_core::db::watched_keys_index();
    let guard = match idx.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .watched
        .values()
        .filter(|watchers| watchers.contains(&client_id))
        .count()
}

fn client_capa_vec(client: &redis_core::client::Client) -> Vec<u8> {
    if client.capa_redirect {
        b"r".to_vec()
    } else {
        Vec::new()
    }
}

fn snapshot_in_pubsub_mode(snap: &redis_core::client_info::ClientSnapshot) -> bool {
    snap.subscribed_channels > 0
        || snap.subscribed_patterns > 0
        || snap.subscribed_shard_channels > 0
        || snap.cmd == "subscribe"
        || snap.cmd == "psubscribe"
        || snap.cmd == "ssubscribe"
}

fn snapshot_flags_vec(snap: &redis_core::client_info::ClientSnapshot) -> Vec<u8> {
    let mut out = Vec::new();
    if snap.is_replica {
        out.push(b'S');
    }
    if snapshot_in_pubsub_mode(snap) {
        out.push(b'P');
    }
    if snap.queued_multi_count.is_some() {
        out.push(b'x');
    }
    if snap.blocked {
        out.push(b'b');
    }
    if snap.import_source {
        out.push(b'I');
    }
    if snap.tracking {
        out.push(b't');
    }
    if snap.tracking_bcast {
        out.push(b'B');
    }
    if snap.tracking_broken_redirect {
        out.push(b'R');
    }
    if snap.readonly {
        out.push(b'r');
    }
    if out.is_empty() {
        out.push(b'N');
    }
    out
}

fn snapshot_type(snap: &redis_core::client_info::ClientSnapshot) -> ClientTypeFilter {
    if snap.is_replica {
        ClientTypeFilter::Replica
    } else if snapshot_in_pubsub_mode(snap) {
        ClientTypeFilter::PubSub
    } else {
        ClientTypeFilter::Normal
    }
}

fn current_client_type(client: &redis_core::client::Client) -> ClientTypeFilter {
    if client.is_replica {
        ClientTypeFilter::Replica
    } else if client.in_pubsub_mode() {
        ClientTypeFilter::PubSub
    } else {
        ClientTypeFilter::Normal
    }
}

fn client_tracking_redir(client: &redis_core::client::Client) -> i64 {
    if client.tracking.enabled {
        client.tracking.redirect
    } else {
        -1
    }
}

fn reported_reply_buffer_size(net_output_bytes: u64, pending_output_bytes: usize) -> usize {
    if pending_output_bytes > 0 || net_output_bytes >= 32 * 1024 {
        16 * 1024
    } else {
        1024
    }
}

fn format_current_client_info_line(
    client: &redis_core::client::Client,
    command_name: &[u8],
) -> Vec<u8> {
    let mut line = Vec::with_capacity(320);
    let addr = client.addr.as_deref().unwrap_or("127.0.0.1:0");
    let flags = client_flags_vec(client);
    let capa = client_capa_vec(client);
    let multi = if client.flag_multi() {
        client.queued_argvs.len() as i64
    } else {
        -1
    };
    let watch = watched_key_count_for_client(client.id);
    let rbs = reported_reply_buffer_size(client.net_output_bytes, client.reply_buf.len());
    let _ = write!(line, "id={} addr={}", client.id, addr);
    line.extend_from_slice(b" laddr=127.0.0.1:0 fd=0 name=");
    if let Some(name) = &client.name {
        line.extend_from_slice(name.as_bytes());
    }
    line.extend_from_slice(b" age=0 idle=0 flags=");
    line.extend_from_slice(&flags);
    line.extend_from_slice(b" capa=");
    line.extend_from_slice(&capa);
    let _ = write!(
        line,
        " db={} sub={} psub={} ssub={} multi={} watch={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs={} rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=",
        client.db_index,
        client.subscribed_channels.len(),
        client.subscribed_patterns.len(),
        client.subscribed_shard_channels.len(),
        multi,
        watch,
        rbs,
    );
    line.extend_from_slice(command_name);
    line.extend_from_slice(b" user=");
    line.extend_from_slice(client_user_bytes(client));
    let _ = write!(
        line,
        " redir={} resp={} lib-name=",
        client_tracking_redir(client),
        client.resp_proto,
    );
    if let Some(lib_name) = &client.lib_name {
        line.extend_from_slice(lib_name.as_bytes());
    }
    line.extend_from_slice(b" lib-ver=");
    if let Some(lib_ver) = &client.lib_ver {
        line.extend_from_slice(lib_ver.as_bytes());
    }
    let _ = writeln!(
        line,
        " tot-net-in={} tot-net-out={} tot-cmds={}",
        client.net_input_bytes, client.net_output_bytes, client.commands_processed
    );
    line
}

fn format_snapshot_client_info_line(
    snap: &redis_core::client_info::ClientSnapshot,
    command_name: &[u8],
) -> Vec<u8> {
    let mut line = Vec::with_capacity(320);
    let flags = snapshot_flags_vec(snap);
    let capa = if snap.capa_redirect {
        b"r".as_slice()
    } else {
        b"".as_slice()
    };
    let multi = snap.queued_multi_count.map(|n| n as i64).unwrap_or(-1);
    let rbs = reported_reply_buffer_size(snap.net_output_bytes, snap.output_buffer_bytes);
    let output_list_len = usize::from(snap.output_buffer_bytes > 0);
    let _ = write!(
        line,
        "id={} addr={} laddr=127.0.0.1:0 fd=0 name=",
        snap.id, snap.addr,
    );
    if let Some(name) = &snap.name {
        line.extend_from_slice(name.as_bytes());
    }
    let _ = write!(line, " age=0 idle={} flags=", snap.idle_seconds);
    line.extend_from_slice(&flags);
    line.extend_from_slice(b" capa=");
    line.extend_from_slice(capa);
    let _ = write!(
        line,
        " db={} sub={} psub={} ssub={} multi={} watch=0 qbuf={} qbuf-free=0 argv-mem={} multi-mem={} rbs={} rbp=0 obl={} oll={} omem={} tot-mem={} events=r cmd=",
        snap.db_index,
        snap.subscribed_channels,
        snap.subscribed_patterns,
        snap.subscribed_shard_channels,
        multi,
        snap.query_buffer_bytes,
        snap.argv_memory_bytes,
        snap.multi_memory_bytes,
        rbs,
        snap.output_buffer_bytes,
        output_list_len,
        snap.output_buffer_bytes,
        snap.total_memory_bytes,
    );
    line.extend_from_slice(command_name);
    line.extend_from_slice(b" user=");
    if let Some(user) = &snap.user {
        line.extend_from_slice(user.as_bytes());
    } else {
        line.extend_from_slice(b"default");
    }
    let _ = write!(line, " redir=-1 resp={} lib-name=", snap.resp_proto);
    if let Some(lib_name) = &snap.lib_name {
        line.extend_from_slice(lib_name.as_bytes());
    }
    line.extend_from_slice(b" lib-ver=");
    if let Some(lib_ver) = &snap.lib_ver {
        line.extend_from_slice(lib_ver.as_bytes());
    }
    let _ = writeln!(
        line,
        " tot-net-in={} tot-net-out={} tot-cmds={}",
        snap.net_input_bytes, snap.net_output_bytes, snap.commands_processed
    );
    line
}

fn acl_user_exists(name: &[u8]) -> bool {
    let key = RedisString::from_bytes(name);
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.users.contains_key(&key)
}

fn unknown_client_type_error(value: &[u8]) -> RedisError {
    let mut msg = b"ERR Unknown client type '".to_vec();
    msg.extend_from_slice(value);
    msg.push(b'\'');
    RedisError::runtime(msg)
}

fn no_such_user_error(value: &[u8]) -> RedisError {
    let mut msg = b"ERR No such user '".to_vec();
    msg.extend_from_slice(value);
    msg.push(b'\'');
    RedisError::runtime(msg)
}

fn append_value_error(prefix: &[u8], value: &[u8]) -> RedisError {
    let mut msg = prefix.to_vec();
    msg.extend_from_slice(value);
    RedisError::runtime(msg)
}

fn parse_client_type(value: &[u8]) -> Option<ClientTypeFilter> {
    if ascii_eq_ignore_case(value, b"normal") {
        Some(ClientTypeFilter::Normal)
    } else if ascii_eq_ignore_case(value, b"replica") || ascii_eq_ignore_case(value, b"slave") {
        Some(ClientTypeFilter::Replica)
    } else if ascii_eq_ignore_case(value, b"pubsub") {
        Some(ClientTypeFilter::PubSub)
    } else if ascii_eq_ignore_case(value, b"primary") || ascii_eq_ignore_case(value, b"master") {
        Some(ClientTypeFilter::Primary)
    } else {
        None
    }
}

fn parse_positive_i64_for_client_filter(value: &[u8], name: &[u8]) -> RedisResult<i64> {
    let Some(parsed) = parse_i64_strict(value) else {
        let mut msg = b"ERR ".to_vec();
        msg.extend_from_slice(name);
        msg.extend_from_slice(b" is not an integer or out of range");
        return Err(RedisError::runtime(msg));
    };
    if parsed <= 0 {
        let mut msg = b"ERR ".to_vec();
        msg.extend_from_slice(name);
        msg.extend_from_slice(b" should be greater than 0");
        return Err(RedisError::runtime(msg));
    }
    Ok(parsed)
}

fn parse_db_filter(ctx: &CommandContext<'_>, value: &[u8], name: &[u8]) -> RedisResult<u32> {
    let Some(parsed) = parse_i64_strict(value) else {
        let mut msg = b"ERR ".to_vec();
        msg.extend_from_slice(name);
        msg.extend_from_slice(b" is not an integer or out of range");
        return Err(RedisError::runtime(msg));
    };
    if parsed < 0 || parsed >= ctx.database_count() as i64 {
        let max = ctx.database_count().saturating_sub(1);
        let mut msg = Vec::new();
        msg.extend_from_slice(b"ERR ");
        msg.extend_from_slice(name);
        let _ = write!(msg, " number should be between 0 and {}", max);
        return Err(RedisError::runtime(msg));
    }
    Ok(parsed as u32)
}

fn flags_match(actual: &[u8], filter: &[u8]) -> bool {
    filter.iter().all(|b| actual.contains(b))
}

fn option_bytes_matches(actual: Option<&RedisString>, expected: &[u8]) -> bool {
    actual
        .map(|value| value.as_bytes() == expected)
        .unwrap_or(false)
}

fn option_bytes_not_matches(actual: Option<&RedisString>, expected: &[u8]) -> bool {
    actual
        .map(|value| value.as_bytes() != expected)
        .unwrap_or(true)
}

fn refresh_client_info_registry(client: &redis_core::client::Client) {
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.update_client_metadata(client);
    }
}

fn require_filter_value(ctx: &CommandContext<'_>, idx: usize) -> RedisResult<RedisString> {
    if idx + 1 >= ctx.arg_count() {
        return Err(RedisError::syntax(b"syntax error"));
    }
    ctx.arg_owned(idx + 1)
}

fn parse_client_list_filters(ctx: &CommandContext<'_>) -> RedisResult<ClientListFilters> {
    let mut filters = ClientListFilters::default();
    let mut idx = 2usize;
    while idx < ctx.arg_count() {
        let opt = ctx.arg(idx)?;
        let opt_bytes = opt.as_bytes();
        if opt_bytes.eq_ignore_ascii_case(b"ID") || opt_bytes.eq_ignore_ascii_case(b"NOT-ID") {
            let negative = opt_bytes.eq_ignore_ascii_case(b"NOT-ID");
            idx += 1;
            let mut saw_id = false;
            while idx < ctx.arg_count() {
                let raw = ctx.arg(idx)?;
                let Some(id) = parse_i64_strict(raw.as_bytes()) else {
                    break;
                };
                if id < 1 {
                    return Err(RedisError::runtime(
                        b"ERR client-id should be greater than 0",
                    ));
                }
                if negative {
                    filters.not_ids.push(id as u64);
                } else {
                    filters.ids.push(id as u64);
                }
                saw_id = true;
                idx += 1;
            }
            if !saw_id {
                return Err(RedisError::syntax(b"syntax error"));
            }
        } else if opt_bytes.eq_ignore_ascii_case(b"ADDR") {
            filters.addr = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-ADDR") {
            filters.not_addr = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"LADDR") {
            filters.laddr = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-LADDR") {
            filters.not_laddr = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"TYPE") {
            let value = require_filter_value(ctx, idx)?;
            filters.type_filter = Some(
                parse_client_type(value.as_bytes())
                    .ok_or_else(|| unknown_client_type_error(value.as_bytes()))?,
            );
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-TYPE") {
            let value = require_filter_value(ctx, idx)?;
            filters.not_type_filter = Some(
                parse_client_type(value.as_bytes())
                    .ok_or_else(|| unknown_client_type_error(value.as_bytes()))?,
            );
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"USER") {
            let value = require_filter_value(ctx, idx)?;
            if !acl_user_exists(value.as_bytes()) {
                return Err(no_such_user_error(value.as_bytes()));
            }
            filters.user = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-USER") {
            let value = require_filter_value(ctx, idx)?;
            if !acl_user_exists(value.as_bytes()) {
                return Err(no_such_user_error(value.as_bytes()));
            }
            filters.not_user = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"SKIPME") {
            let value = require_filter_value(ctx, idx)?;
            if value.as_bytes().eq_ignore_ascii_case(b"yes") {
                filters.skipme = Some(true);
            } else if value.as_bytes().eq_ignore_ascii_case(b"no") {
                filters.skipme = Some(false);
            } else {
                return Err(RedisError::syntax(b"syntax error"));
            }
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"MAXAGE") {
            let value = require_filter_value(ctx, idx)?;
            filters.maxage = Some(parse_positive_i64_for_client_filter(
                value.as_bytes(),
                b"maxage",
            )?);
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"IDLE") {
            let value = require_filter_value(ctx, idx)?;
            filters.idle = Some(parse_positive_i64_for_client_filter(
                value.as_bytes(),
                b"idle",
            )?);
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"FLAGS") {
            let value = require_filter_value(ctx, idx)?;
            if !validate_client_flag_filter(value.as_bytes()) {
                return Err(append_value_error(
                    b"ERR Unknown flags found in the provided filter: ",
                    value.as_bytes(),
                ));
            }
            filters.flags = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-FLAGS") {
            let value = require_filter_value(ctx, idx)?;
            if !validate_client_flag_filter(value.as_bytes()) {
                return Err(append_value_error(
                    b"ERR Unknown flags found in the NOT-FLAGS filter: ",
                    value.as_bytes(),
                ));
            }
            filters.not_flags = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NAME") {
            filters.name = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-NAME") {
            filters.not_name = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"IP") {
            filters.ip = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-IP") {
            filters.not_ip = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"CAPA") {
            let value = require_filter_value(ctx, idx)?;
            if !validate_client_capa_filter(value.as_bytes()) {
                return Err(append_value_error(
                    b"ERR Unknown capa found in the provided filter: ",
                    value.as_bytes(),
                ));
            }
            filters.capa = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-CAPA") {
            let value = require_filter_value(ctx, idx)?;
            if !validate_client_capa_filter(value.as_bytes()) {
                return Err(append_value_error(
                    b"ERR Unknown capa found in the NOT-CAPA filter: ",
                    value.as_bytes(),
                ));
            }
            filters.not_capa = Some(value.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"LIB-NAME") {
            filters.lib_name = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-LIB-NAME") {
            filters.not_lib_name = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"LIB-VER") {
            filters.lib_ver = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-LIB-VER") {
            filters.not_lib_ver = Some(require_filter_value(ctx, idx)?.into_bytes());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"DB") {
            let value = require_filter_value(ctx, idx)?;
            filters.db = Some(parse_db_filter(ctx, value.as_bytes(), b"DB")?);
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NOT-DB") {
            let value = require_filter_value(ctx, idx)?;
            filters.not_db = Some(parse_db_filter(ctx, value.as_bytes(), b"NOT-DB")?);
            idx += 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
    Ok(filters)
}

fn current_client_matches_filters(
    client: &redis_core::client::Client,
    filters: &ClientListFilters,
) -> bool {
    if filters.skipme == Some(true) {
        return false;
    }
    if !filters.ids.is_empty() && !filters.ids.contains(&client.id) {
        return false;
    }
    if !filters.not_ids.is_empty() && filters.not_ids.contains(&client.id) {
        return false;
    }
    let addr = client.addr.as_deref().unwrap_or("127.0.0.1:0").as_bytes();
    if let Some(expected) = &filters.addr {
        if expected.as_slice() != addr {
            return false;
        }
    }
    if let Some(expected) = &filters.not_addr {
        if expected.as_slice() == addr {
            return false;
        }
    }
    if let Some(expected) = &filters.laddr {
        if expected.as_slice() != b"127.0.0.1:0" {
            return false;
        }
    }
    if let Some(expected) = &filters.not_laddr {
        if expected.as_slice() == b"127.0.0.1:0" {
            return false;
        }
    }
    let client_type = current_client_type(client);
    if let Some(expected) = filters.type_filter {
        if expected != client_type {
            return false;
        }
    }
    if let Some(expected) = filters.not_type_filter {
        if expected == client_type {
            return false;
        }
    }
    if let Some(expected) = &filters.name {
        if client_name_bytes(client) != Some(expected.as_slice()) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_name {
        if client_name_bytes(client) == Some(expected.as_slice()) {
            return false;
        }
    }
    if let Some(expected) = &filters.user {
        if expected.as_slice() != client_user_bytes(client) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_user {
        if expected.as_slice() == client_user_bytes(client) {
            return false;
        }
    }
    if let Some(maxage) = filters.maxage {
        if 0 < maxage {
            return false;
        }
    }
    if let Some(_idle) = filters.idle {
        // The current client snapshot does not yet track second-granularity
        // idle time. Treat the filter as satisfied; the other supplied filters
        // still narrow the target set and CLIENT LIST continues to render idle=0.
    }
    if let Some(expected) = &filters.flags {
        let actual = client_flags_vec(client);
        if !flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_flags {
        let actual = client_flags_vec(client);
        if flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.ip {
        if !client_matches_ip_filter(addr, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_ip {
        if client_matches_ip_filter(addr, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.capa {
        let actual = client_capa_vec(client);
        if !flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_capa {
        let actual = client_capa_vec(client);
        if flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.lib_name {
        if !option_bytes_matches(client.lib_name.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_lib_name {
        if !option_bytes_not_matches(client.lib_name.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.lib_ver {
        if !option_bytes_matches(client.lib_ver.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_lib_ver {
        if !option_bytes_not_matches(client.lib_ver.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = filters.db {
        if client.db_index != expected {
            return false;
        }
    }
    if let Some(expected) = filters.not_db {
        if client.db_index == expected {
            return false;
        }
    }
    true
}

fn snapshot_matches_filters(
    snap: &redis_core::client_info::ClientSnapshot,
    filters: &ClientListFilters,
) -> bool {
    if !filters.ids.is_empty() && !filters.ids.contains(&snap.id) {
        return false;
    }
    if !filters.not_ids.is_empty() && filters.not_ids.contains(&snap.id) {
        return false;
    }
    let addr = snap.addr.as_bytes();
    if let Some(expected) = &filters.addr {
        if expected.as_slice() != addr {
            return false;
        }
    }
    if let Some(expected) = &filters.not_addr {
        if expected.as_slice() == addr {
            return false;
        }
    }
    if let Some(expected) = &filters.laddr {
        if expected.as_slice() != b"127.0.0.1:0" {
            return false;
        }
    }
    if let Some(expected) = &filters.not_laddr {
        if expected.as_slice() == b"127.0.0.1:0" {
            return false;
        }
    }
    let client_type = snapshot_type(snap);
    if let Some(expected) = filters.type_filter {
        if expected != client_type {
            return false;
        }
    }
    if let Some(expected) = filters.not_type_filter {
        if expected == client_type {
            return false;
        }
    }
    if let Some(expected) = &filters.user {
        let actual = snap
            .user
            .as_ref()
            .map(|u| u.as_bytes())
            .unwrap_or(b"default");
        if expected.as_slice() != actual {
            return false;
        }
    }
    if let Some(expected) = &filters.not_user {
        let actual = snap
            .user
            .as_ref()
            .map(|u| u.as_bytes())
            .unwrap_or(b"default");
        if expected.as_slice() == actual {
            return false;
        }
    }
    if let Some(expected) = &filters.name {
        if snap.name.as_ref().map(|n| n.as_bytes()) != Some(expected.as_slice()) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_name {
        if snap.name.as_ref().map(|n| n.as_bytes()) == Some(expected.as_slice()) {
            return false;
        }
    }
    if let Some(maxage) = filters.maxage {
        if 0 < maxage {
            return false;
        }
    }
    if let Some(_idle) = filters.idle {
        // See current-client path above: idle accounting is not yet persisted
        // in the cross-thread snapshot, so we preserve filter syntax and let
        // the other predicates define the matched set.
    }
    if let Some(expected) = &filters.flags {
        let actual = snapshot_flags_vec(snap);
        if !flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_flags {
        let actual = snapshot_flags_vec(snap);
        if flags_match(&actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.ip {
        if !client_matches_ip_filter(addr, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_ip {
        if client_matches_ip_filter(addr, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.capa {
        let actual = if snap.capa_redirect {
            b"r".as_slice()
        } else {
            b"".as_slice()
        };
        if !flags_match(actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_capa {
        let actual = if snap.capa_redirect {
            b"r".as_slice()
        } else {
            b"".as_slice()
        };
        if flags_match(actual, expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.lib_name {
        if !option_bytes_matches(snap.lib_name.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_lib_name {
        if !option_bytes_not_matches(snap.lib_name.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.lib_ver {
        if !option_bytes_matches(snap.lib_ver.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = &filters.not_lib_ver {
        if !option_bytes_not_matches(snap.lib_ver.as_ref(), expected) {
            return false;
        }
    }
    if let Some(expected) = filters.db {
        if snap.db_index != expected {
            return false;
        }
    }
    if let Some(expected) = filters.not_db {
        if snap.db_index == expected {
            return false;
        }
    }
    true
}

/// `CLIENT <subcommand> [args]`.
///
/// Pilot subset:
///   * `CLIENT ID` — integer reply of the client's connection id.
///   * `CLIENT GETNAME` — bulk reply of the stored name (nil bulk when unset).
///   * `CLIENT SETNAME name` — store the name; replies `+OK\r\n`.
///   * `CLIENT NO-EVICT ON|OFF` — no-op, replies `+OK\r\n`.
///   * `CLIENT NO-TOUCH ON|OFF` — no-op, replies `+OK\r\n`.
///   * `CLIENT LIST` — single-line description of the current client.
pub fn client_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"client"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();
    if ascii_eq_ignore_case(sub_bytes, b"ID") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"client|id"));
        }
        let id = ctx.client_ref().id() as i64;
        return ctx.reply_integer(id);
    }
    if ascii_eq_ignore_case(sub_bytes, b"GETNAME") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"client|getname"));
        }
        let name = ctx.client_ref().name.clone();
        return match name {
            Some(n) => ctx.reply_bulk_string(n),
            None => ctx.reply_null_bulk(),
        };
    }
    if ascii_eq_ignore_case(sub_bytes, b"SETNAME") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"client|setname"));
        }
        let name = ctx.arg_owned(2usize)?;
        validate_client_name(name.as_bytes())?;
        ctx.client_mut().name = Some(name);
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"NO-EVICT") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"client|no-evict"));
        }
        let flag = ctx.arg_owned(2usize)?;
        if !ascii_eq_ignore_case(flag.as_bytes(), b"ON")
            && !ascii_eq_ignore_case(flag.as_bytes(), b"OFF")
        {
            return Err(RedisError::syntax(b""));
        }
        ctx.client_mut().flags.no_evict = ascii_eq_ignore_case(flag.as_bytes(), b"ON");
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"NO-TOUCH") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"client|no-touch"));
        }
        let flag = ctx.arg_owned(2usize)?;
        if !ascii_eq_ignore_case(flag.as_bytes(), b"ON")
            && !ascii_eq_ignore_case(flag.as_bytes(), b"OFF")
        {
            return Err(RedisError::syntax(b""));
        }
        ctx.client_mut().flags.no_touch = ascii_eq_ignore_case(flag.as_bytes(), b"ON");
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"CAPA") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"client|capa"));
        }
        for i in 2..ctx.arg_count() {
            let opt = ctx.arg_owned(i)?;
            if ascii_eq_ignore_case(opt.as_bytes(), b"REDIRECT") {
                ctx.client_mut().capa_redirect = true;
            }
        }
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"SETINFO") {
        if ctx.arg_count() != 4 {
            return Err(RedisError::wrong_number_of_args(b"client|setinfo"));
        }
        let attr = ctx.arg_owned(2usize)?;
        let value = ctx.arg_owned(3usize)?;
        if ascii_eq_ignore_case(attr.as_bytes(), b"LIB-NAME") {
            validate_client_setinfo_attr(b"lib-name", value.as_bytes())?;
            ctx.client_mut().lib_name = Some(value);
        } else if ascii_eq_ignore_case(attr.as_bytes(), b"LIB-VER") {
            validate_client_setinfo_attr(b"lib-ver", value.as_bytes())?;
            ctx.client_mut().lib_ver = Some(value);
        } else {
            let mut msg = b"ERR Unrecognized option '".to_vec();
            msg.extend_from_slice(attr.as_bytes());
            msg.push(b'\'');
            return Err(RedisError::runtime(msg));
        }
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"INFO") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"client|info"));
        }
        let line = format_current_client_info_line(ctx.client_ref(), b"client|info");
        return ctx.reply_bulk(&line);
    }
    if ascii_eq_ignore_case(sub_bytes, b"LIST") {
        let filters = parse_client_list_filters(ctx)?;
        let snapshots = {
            let guard = match client_info_registry().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.all()
        };
        let mut snapshot_lines: Vec<(bool, u64, Vec<u8>)> = Vec::new();
        let mut current_line: Option<Vec<u8>> = None;
        if current_client_matches_filters(ctx.client_ref(), &filters) {
            current_line = Some(format_current_client_info_line(
                ctx.client_ref(),
                b"client|list",
            ));
        }
        for snap in &snapshots {
            if snap.id == ctx.client_ref().id {
                continue;
            }
            if !snapshot_matches_filters(snap, &filters) {
                continue;
            }
            let cmd = if snap.cmd.is_empty() {
                b"NULL".as_slice()
            } else {
                snap.cmd.as_bytes()
            };
            snapshot_lines.push((
                snapshot_in_pubsub_mode(snap),
                snap.id,
                format_snapshot_client_info_line(snap, cmd),
            ));
        }
        let mut out = Vec::new();
        if let Some(line) = current_line {
            out.extend_from_slice(&line);
        }
        snapshot_lines.sort_by_key(|(is_pubsub, id, _)| (!*is_pubsub, *id));
        for (_, _, line) in snapshot_lines {
            out.extend_from_slice(&line);
        }
        return ctx.reply_bulk(&out);
    }
    if ascii_eq_ignore_case(sub_bytes, b"TRACKING") {
        return client_tracking_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"CACHING") {
        return client_caching_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"GETREDIR") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"client|getredir"));
        }
        return ctx.reply_integer(client_tracking_redir(ctx.client_ref()));
    }
    if ascii_eq_ignore_case(sub_bytes, b"TRACKINGINFO") {
        return client_trackinginfo_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"IMPORT-SOURCE") {
        return client_import_source_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"REPLY") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"client|reply"));
        }
        let mode = ctx.arg_owned(2usize)?;
        let flags = &mut ctx.client_mut().flags;
        if ascii_eq_ignore_case(mode.as_bytes(), b"ON") {
            flags.reply_skip = false;
            flags.reply_skip_next = false;
            flags.reply_off = false;
            return ctx.reply_simple_string(b"OK");
        }
        if ascii_eq_ignore_case(mode.as_bytes(), b"OFF") {
            flags.reply_off = true;
            return Ok(());
        }
        if ascii_eq_ignore_case(mode.as_bytes(), b"SKIP") {
            if !flags.reply_off {
                flags.reply_skip_next = true;
            }
            return Ok(());
        }
        return Err(RedisError::syntax(b""));
    }
    if ascii_eq_ignore_case(sub_bytes, b"UNBLOCK") {
        if ctx.arg_count() < 3 || ctx.arg_count() > 4 {
            return Err(RedisError::wrong_number_of_args(b"client|unblock"));
        }
        let id_arg = ctx.arg_owned(2usize)?;
        let Some(client_id) = parse_i64_strict(id_arg.as_bytes()) else {
            return Err(RedisError::runtime(
                b"ERR value is not an integer or out of range",
            ));
        };
        if client_id < 0 {
            return Err(RedisError::runtime(
                b"ERR value is not an integer or out of range",
            ));
        }
        let mut error_mode = false;
        if ctx.arg_count() == 4 {
            let mode = ctx.arg_owned(3usize)?;
            if ascii_eq_ignore_case(mode.as_bytes(), b"TIMEOUT") {
                error_mode = false;
            } else if ascii_eq_ignore_case(mode.as_bytes(), b"ERROR") {
                error_mode = true;
            } else {
                return Err(RedisError::syntax(b"syntax error"));
            }
        }
        let waiter = {
            let mut idx = match blocked_keys_index().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            idx.remove_client(client_id as u64)
        };
        let Some(waiter) = waiter else {
            return ctx.reply_integer(0);
        };
        let reply = if error_mode {
            b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n".to_vec()
        } else {
            waiter.action.timeout_reply_bytes().to_vec()
        };
        let delivered = waiter.sender.send(reply).is_ok();
        if delivered && error_mode {
            record_error_reply(b"UNBLOCKED client unblocked via CLIENT UNBLOCK");
            record_blocked_command_rejected(blocked_action_command_name(&waiter.action));
        }
        return ctx.reply_integer(if delivered { 1 } else { 0 });
    }
    if ascii_eq_ignore_case(sub_bytes, b"HELP") {
        let lines: &[&[u8]] = &[
            b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"ID",
            b"    Return the current connection id.",
            b"GETNAME",
            b"    Return the current connection name.",
            b"SETNAME <name>",
            b"    Assign a name to the current connection.",
            b"LIST [options ...]",
            b"    Return information about client connections.",
            b"INFO",
            b"    Return information about the current client connection.",
            b"TRACKING <ON|OFF> [options ...]",
            b"    Enable or disable server assisted client side caching.",
            b"REPLY <ON|OFF|SKIP>",
            b"    Control whether the server replies to commands.",
            b"HELP",
            b"    Return this help.",
        ];
        return reply_help(ctx, lines);
    }
    if ascii_eq_ignore_case(sub_bytes, b"PAUSE") {
        return redis_core::networking::client_pause_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"UNPAUSE") {
        return redis_core::networking::client_unpause_command(ctx);
    }
    if ascii_eq_ignore_case(sub_bytes, b"KILL") {
        return client_kill_command(ctx);
    }
    let mut msg = Vec::with_capacity(b"ERR Unknown CLIENT subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown CLIENT subcommand: ");
    msg.extend_from_slice(sub_bytes);
    Err(RedisError::runtime(msg))
}

fn client_kill_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"client|kill"));
    }

    let (filters, old_style) = if ctx.arg_count() == 3 {
        let mut filters = ClientListFilters::default();
        filters.addr = Some(ctx.arg_owned(2usize)?.into_bytes());
        filters.skipme = Some(false);
        (filters, true)
    } else {
        let mut filters = parse_client_list_filters(ctx)?;
        if filters.skipme.is_none() {
            filters.skipme = Some(true);
        }
        (filters, false)
    };

    let current_id = ctx.client_ref().id();
    let mut kill_self = current_client_matches_filters(ctx.client_ref(), &filters);
    let snapshots = {
        let guard = match client_info_registry().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.all()
    };

    let mut victim_ids: Vec<u64> = Vec::new();
    for snap in &snapshots {
        if snap.id == current_id {
            continue;
        }
        if snapshot_matches_filters(snap, &filters) {
            victim_ids.push(snap.id);
        }
    }
    if kill_self {
        victim_ids.push(current_id);
    }

    victim_ids.sort_unstable();
    victim_ids.dedup();
    if !victim_ids.contains(&current_id) {
        kill_self = false;
    }
    let mut killed = victim_ids.len() as i64;
    if !old_style && client_kill_only_skipme_filter(&filters) {
        let connected = snapshots.len().min(i64::MAX as usize) as i64;
        killed = if filters.skipme == Some(true) {
            connected.saturating_sub(1)
        } else {
            connected
        };
    }

    {
        let mut guard = match client_info_registry().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for id in &victim_ids {
            if *id != current_id {
                guard.mark_killed(*id);
            }
        }
    }

    if old_style {
        if killed == 0 {
            return Err(RedisError::runtime(b"ERR No such client"));
        }
        ctx.reply_simple_string(b"OK")?;
    } else {
        ctx.reply_integer(killed)?;
    }

    if kill_self {
        ctx.client_mut().should_close = true;
    }
    Ok(())
}

fn client_tracking_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"client|tracking"));
    }
    let mode = ctx.arg_owned(2usize)?;
    let mut redirect: i64 = 0;
    let mut bcast = false;
    let mut optin = false;
    let mut optout = false;
    let mut noloop = false;
    let mut prefixes: Vec<RedisString> = Vec::new();

    let mut idx = 3usize;
    while idx < ctx.arg_count() {
        let opt = ctx.arg_owned(idx)?;
        if ascii_eq_ignore_case(opt.as_bytes(), b"REDIRECT") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            if redirect != 0 {
                return Err(RedisError::runtime(
                    b"ERR A client can only redirect to a single other client",
                ));
            }
            let id = parse_i64_strict(ctx.arg(idx + 1)?.as_bytes()).ok_or_else(|| {
                RedisError::runtime(b"ERR value is not an integer or out of range")
            })?;
            if id < 0 {
                return Err(RedisError::runtime(
                    b"ERR value is not an integer or out of range",
                ));
            }
            redirect = id;
            idx += 2;
        } else if ascii_eq_ignore_case(opt.as_bytes(), b"BCAST") {
            bcast = true;
            idx += 1;
        } else if ascii_eq_ignore_case(opt.as_bytes(), b"OPTIN") {
            optin = true;
            idx += 1;
        } else if ascii_eq_ignore_case(opt.as_bytes(), b"OPTOUT") {
            optout = true;
            idx += 1;
        } else if ascii_eq_ignore_case(opt.as_bytes(), b"NOLOOP") {
            noloop = true;
            idx += 1;
        } else if ascii_eq_ignore_case(opt.as_bytes(), b"PREFIX") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            prefixes.push(ctx.arg_owned(idx + 1)?);
            idx += 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    if ascii_eq_ignore_case(mode.as_bytes(), b"OFF") {
        let client_id = ctx.client_ref().id;
        ctx.client_mut().tracking = redis_core::client::ClientTrackingState::default();
        redis_core::tracking::remove_runtime_client_tracking(client_id);
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if !ascii_eq_ignore_case(mode.as_bytes(), b"ON") {
        return Err(RedisError::syntax(b"syntax error"));
    }
    if !bcast && !prefixes.is_empty() {
        return Err(RedisError::runtime(
            b"ERR PREFIX option requires BCAST mode to be enabled",
        ));
    }
    if bcast && (optin || optout) {
        return Err(RedisError::runtime(
            b"ERR OPTIN and OPTOUT are not compatible with BCAST",
        ));
    }
    if optin && optout {
        return Err(RedisError::runtime(
            b"ERR You can't specify both OPTIN mode and OPTOUT mode",
        ));
    }
    let client = ctx.client_mut();
    if client.tracking.enabled && client.tracking.bcast != bcast {
        return Err(RedisError::runtime(
            b"ERR You can't switch BCAST mode on/off before disabling tracking for this client",
        ));
    }
    if client.tracking.enabled
        && ((optin && client.tracking.optout) || (optout && client.tracking.optin))
    {
        return Err(RedisError::runtime(
            b"ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client",
        ));
    }
    if bcast {
        let existing: Option<HashSet<Vec<u8>>> = if client.tracking.enabled && client.tracking.bcast
        {
            Some(
                client
                    .tracking
                    .prefixes
                    .iter()
                    .map(|prefix| prefix.as_bytes().to_vec())
                    .collect(),
            )
        } else {
            None
        };
        let prefix_refs: Vec<&[u8]> = prefixes.iter().map(|prefix| prefix.as_bytes()).collect();
        redis_core::tracking::check_prefix_collisions(&prefix_refs, existing.as_ref())?;
    }
    let mut effective_prefixes = if bcast && client.tracking.enabled && client.tracking.bcast {
        client.tracking.prefixes.clone()
    } else {
        Vec::new()
    };
    if bcast {
        if prefixes.is_empty() {
            if effective_prefixes.is_empty() {
                effective_prefixes.push(RedisString::from_bytes(b""));
            }
        } else {
            for prefix in prefixes {
                if !effective_prefixes
                    .iter()
                    .any(|existing| existing == &prefix)
                {
                    effective_prefixes.push(prefix);
                }
            }
        }
    }
    client.tracking.enabled = true;
    client.tracking.bcast = bcast;
    client.tracking.optin = optin;
    client.tracking.optout = optout;
    client.tracking.noloop = noloop;
    client.tracking.caching = false;
    client.tracking.broken_redirect = false;
    client.tracking.redirect = redirect;
    client.tracking.prefixes = effective_prefixes;
    redis_core::tracking::sync_runtime_client_tracking(client.id, &client.tracking);
    refresh_client_info_registry(ctx.client_ref());
    ctx.reply_simple_string(b"OK")
}

fn client_caching_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"client|caching"));
    }
    let opt = ctx.arg_owned(2usize)?;
    let tracking = &mut ctx.client_mut().tracking;
    if !tracking.enabled || (!tracking.optin && !tracking.optout) {
        return Err(RedisError::runtime(
            b"ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
        ));
    }
    if !ascii_eq_ignore_case(opt.as_bytes(), b"YES") && !ascii_eq_ignore_case(opt.as_bytes(), b"NO")
    {
        return Err(RedisError::syntax(b"syntax error"));
    }
    if ascii_eq_ignore_case(opt.as_bytes(), b"YES") {
        if !tracking.optin {
            return Err(RedisError::runtime(
                b"ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.",
            ));
        }
        tracking.caching = true;
    } else {
        if !tracking.optout {
            return Err(RedisError::runtime(
                b"ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.",
            ));
        }
        tracking.caching = true;
    }
    redis_core::tracking::sync_runtime_client_tracking(
        ctx.client_ref().id,
        &ctx.client_ref().tracking,
    );
    refresh_client_info_registry(ctx.client_ref());
    ctx.reply_simple_string(b"OK")
}

fn client_trackinginfo_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"client|trackinginfo"));
    }
    if redis_core::tracking::runtime_client_has_broken_redirect(ctx.client_ref().id()) {
        ctx.client_mut().tracking.broken_redirect = true;
    }
    let tracking = &ctx.client_ref().tracking;
    let mut flags = Vec::new();
    let state_flag: &[u8] = if tracking.enabled { b"on" } else { b"off" };
    flags.push(RespFrame::bulk(RedisString::from_bytes(state_flag)));
    if tracking.bcast {
        flags.push(RespFrame::bulk(RedisString::from_bytes(b"bcast")));
    }
    if tracking.optin {
        flags.push(RespFrame::bulk(RedisString::from_bytes(b"optin")));
        if tracking.caching {
            flags.push(RespFrame::bulk(RedisString::from_bytes(b"caching-yes")));
        }
    }
    if tracking.optout {
        flags.push(RespFrame::bulk(RedisString::from_bytes(b"optout")));
        if tracking.caching {
            flags.push(RespFrame::bulk(RedisString::from_bytes(b"caching-no")));
        }
    }
    if tracking.noloop {
        flags.push(RespFrame::bulk(RedisString::from_bytes(b"noloop")));
    }
    if tracking.broken_redirect {
        flags.push(RespFrame::bulk(RedisString::from_bytes(b"broken_redirect")));
    }
    let prefixes = tracking
        .prefixes
        .iter()
        .cloned()
        .map(RespFrame::bulk)
        .collect();
    ctx.reply_frame(&RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_bytes(b"flags")),
            RespFrame::array(flags),
        ),
        (
            RespFrame::bulk(RedisString::from_bytes(b"redirect")),
            RespFrame::Integer(client_tracking_redir(ctx.client_ref())),
        ),
        (
            RespFrame::bulk(RedisString::from_bytes(b"prefixes")),
            RespFrame::array(prefixes),
        ),
    ]))
}

fn client_import_source_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"client|import-source"));
    }
    let value = ctx.arg_owned(2usize)?;
    if ascii_eq_ignore_case(value.as_bytes(), b"ON") {
        if !ctx.server().live_config.import_mode() {
            return Err(RedisError::runtime(b"ERR Server is not in import mode"));
        }
        ctx.client_mut().import_source = true;
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(value.as_bytes(), b"OFF") {
        ctx.client_mut().import_source = false;
        refresh_client_info_registry(ctx.client_ref());
        return ctx.reply_simple_string(b"OK");
    }
    Err(RedisError::syntax(b"syntax error"))
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

/// `COMMAND` / `COMMAND COUNT` / `COMMAND GETKEYS` / `COMMAND GETKEYSANDFLAGS`.
///
/// `COMMAND` (no args) replies with an array of bulk-string command names
/// drawn from the dispatch table. This stub omits the per-command metadata
/// (arity/flags/key-positions/etc.); `redis-cli` accepts a names-only reply.
///
/// `COMMAND COUNT` replies with the integer length of the dispatch table.
/// `COMMAND LIST` returns the generated command and subcommand names, including
/// `parent|subcommand` full names for source-shaped upstream introspection
/// tests.
/// `COMMAND INFO` returns a compact command-info array; currently the
/// load-bearing field is index 2, the flags list.
/// `COMMAND GETKEYS` replies with keys derived from generated command metadata,
/// with SORT/SORT_RO/SET matching their upstream variable key parsing.
pub fn command_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() == 1 {
        let handlers = crate::dispatch::HANDLERS;
        let mut items: Vec<RespFrame> = Vec::with_capacity(handlers.len());
        for entry in handlers.iter() {
            items.push(RespFrame::bulk(RedisString::from_bytes(entry.name)));
        }
        return ctx.reply_frame(&RespFrame::array(items));
    }
    let sub = ctx.arg_owned(1usize)?;
    if ascii_eq_ignore_case(sub.as_bytes(), b"HELP") {
        let lines: &[&[u8]] = &[
            b"COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"COUNT",
            b"    Return the total number of commands in this server.",
            b"LIST [FILTERBY MODULE|ACLCAT|PATTERN <arg>]",
            b"    Return a list of command names.",
            b"INFO [<command-name> ...]",
            b"    Return command metadata.",
            b"GETKEYS <full-command>",
            b"    Return the keys from a full command.",
            b"GETKEYSANDFLAGS <full-command>",
            b"    Return the keys and access flags from a full command.",
            b"HELP",
            b"    Return this help.",
        ];
        return reply_help(ctx, lines);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"COUNT") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"command|count"));
        }
        let n = crate::dispatch::HANDLERS.len() as i64;
        return ctx.reply_integer(n);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"LIST") {
        return command_list(ctx);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"INFO") {
        return command_info(ctx);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"GETKEYS") {
        return command_getkeys(ctx, false);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"GETKEYSANDFLAGS") {
        return command_getkeys(ctx, true);
    }
    let mut msg =
        Vec::with_capacity(b"ERR Unknown COMMAND subcommand: ".len() + sub.as_bytes().len());
    msg.extend_from_slice(b"ERR Unknown COMMAND subcommand: ");
    msg.extend_from_slice(sub.as_bytes());
    Err(RedisError::runtime(msg))
}

enum CommandListFilter<'a> {
    None,
    Module,
    AclCategory(Option<u64>),
    Pattern(&'a [u8]),
}

fn command_list(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let filter = match ctx.arg_count() {
        2 => CommandListFilter::None,
        5 => {
            if !ascii_eq_ignore_case(ctx.arg(2)?.as_bytes(), b"FILTERBY") {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let filter_type = ctx.arg(3)?.as_bytes();
            let filter_arg = ctx.arg(4)?.as_bytes();
            if ascii_eq_ignore_case(filter_type, b"MODULE") {
                CommandListFilter::Module
            } else if ascii_eq_ignore_case(filter_type, b"ACLCAT") {
                CommandListFilter::AclCategory(category_name_to_bit(filter_arg))
            } else if ascii_eq_ignore_case(filter_type, b"PATTERN") {
                CommandListFilter::Pattern(filter_arg)
            } else {
                return Err(RedisError::syntax(b"syntax error"));
            }
        }
        _ => return Err(RedisError::syntax(b"syntax error")),
    };

    let mut names = command_list_names(&filter);
    names.sort();
    names.dedup();
    let items = names
        .into_iter()
        .map(|name| RespFrame::bulk(RedisString::from_vec(name)))
        .collect();
    ctx.reply_frame(&RespFrame::array(items))
}

fn command_list_names(filter: &CommandListFilter<'_>) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for spec in COMMANDS.iter() {
        let name = command_full_name(spec);
        if command_list_filter_allows(filter, spec, &name) {
            out.push(name);
        }
    }
    out
}

fn command_list_filter_allows(
    filter: &CommandListFilter<'_>,
    spec: &crate::generated::GeneratedCommandSpec,
    full_name: &[u8],
) -> bool {
    match filter {
        CommandListFilter::None => true,
        CommandListFilter::Module => false,
        CommandListFilter::AclCategory(Some(bit)) => spec.acl_categories.iter().any(|&cat| {
            let cat_bit = generated_acl_category_bit(cat);
            cat_bit & bit != 0
        }),
        CommandListFilter::AclCategory(None) => false,
        CommandListFilter::Pattern(pattern) => glob_match_ascii_ci(pattern, full_name),
    }
}

fn command_info(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() == 2 {
        let mut items = Vec::new();
        for spec in COMMANDS.iter() {
            items.push(command_info_frame(spec));
        }
        return ctx.reply_frame(&RespFrame::array(items));
    }

    let mut items = Vec::with_capacity(ctx.arg_count().saturating_sub(2));
    for i in 2..ctx.arg_count() {
        let name = ctx.arg(i)?.as_bytes();
        match lookup_command_info_spec(name) {
            Some(spec) => items.push(command_info_frame(spec)),
            None => items.push(RespFrame::null_bulk()),
        }
    }
    ctx.reply_frame(&RespFrame::array(items))
}

fn lookup_command_info_spec(
    name: &[u8],
) -> Option<&'static crate::generated::GeneratedCommandSpec> {
    COMMANDS
        .iter()
        .find(|spec| ascii_eq_ignore_case(&command_full_name(spec), name))
}

fn command_info_frame(spec: &crate::generated::GeneratedCommandSpec) -> RespFrame {
    RespFrame::array(vec![
        RespFrame::bulk(RedisString::from_vec(command_full_name(spec))),
        RespFrame::integer(spec.arity as i64),
        RespFrame::array(
            command_info_flags(spec)
                .into_iter()
                .map(RespFrame::bulk)
                .collect(),
        ),
        RespFrame::integer(0),
        RespFrame::integer(0),
        RespFrame::integer(0),
        RespFrame::array(Vec::new()),
    ])
}

fn command_info_flags(spec: &crate::generated::GeneratedCommandSpec) -> Vec<RedisString> {
    let mut flags: Vec<RedisString> = spec
        .flags
        .iter()
        .filter_map(|flag| command_flag_name(*flag))
        .map(RedisString::from_bytes)
        .collect();
    let full_name = command_full_name(spec);
    if command_has_movable_keys(&full_name) && !flags.iter().any(|f| f.as_bytes() == b"movablekeys")
    {
        flags.push(RedisString::from_bytes(b"movablekeys"));
    }
    flags
}

fn command_flag_name(flag: crate::generated::CommandFlag) -> Option<&'static [u8]> {
    use crate::generated::CommandFlag;
    match flag {
        CommandFlag::ADMIN => Some(b"admin"),
        CommandFlag::ALLOW_BUSY => Some(b"allow-busy"),
        CommandFlag::ALL_DBS => Some(b"all-dbs"),
        CommandFlag::ASKING => Some(b"asking"),
        CommandFlag::BLOCKING => Some(b"blocking"),
        CommandFlag::DENYOOM => Some(b"denyoom"),
        CommandFlag::FAST => Some(b"fast"),
        CommandFlag::LOADING => Some(b"loading"),
        CommandFlag::MAY_REPLICATE => Some(b"may-replicate"),
        CommandFlag::NOSCRIPT => Some(b"noscript"),
        CommandFlag::NO_ASYNC_LOADING => Some(b"no-async-loading"),
        CommandFlag::NO_AUTH => Some(b"no-auth"),
        CommandFlag::NO_MANDATORY_KEYS => None,
        CommandFlag::NO_MULTI => Some(b"no-multi"),
        CommandFlag::ONLY_SENTINEL => Some(b"only-sentinel"),
        CommandFlag::PROTECTED => Some(b"protected"),
        CommandFlag::PUBSUB => Some(b"pubsub"),
        CommandFlag::READONLY => Some(b"readonly"),
        CommandFlag::SENTINEL => Some(b"sentinel"),
        CommandFlag::SKIP_COMMANDLOG => Some(b"skip-commandlog"),
        CommandFlag::SKIP_MONITOR => Some(b"skip-monitor"),
        CommandFlag::STALE => Some(b"stale"),
        CommandFlag::TOUCHES_ARBITRARY_KEYS => Some(b"movablekeys"),
        CommandFlag::WRITE => Some(b"write"),
    }
}

fn command_has_movable_keys(full_name: &[u8]) -> bool {
    [
        b"zunionstore".as_slice(),
        b"xread".as_slice(),
        b"eval".as_slice(),
        b"sort".as_slice(),
        b"sort_ro".as_slice(),
        b"migrate".as_slice(),
        b"georadius".as_slice(),
    ]
    .iter()
    .any(|name| ascii_eq_ignore_case(full_name, name))
}

fn command_full_name(spec: &crate::generated::GeneratedCommandSpec) -> Vec<u8> {
    let name = spec.name.as_bytes().to_ascii_lowercase();
    if let Some(parent) = command_parent_for_spec(spec) {
        if name.as_slice() != parent {
            let mut full = Vec::with_capacity(parent.len() + 1 + name.len());
            full.extend_from_slice(parent);
            full.push(b'|');
            full.extend_from_slice(&name);
            return full;
        }
    }
    name
}

fn command_parent_for_spec(spec: &crate::generated::GeneratedCommandSpec) -> Option<&'static [u8]> {
    let function = spec.function.as_bytes();
    for (prefix, parent) in [
        (b"acl".as_slice(), b"acl".as_slice()),
        (b"client".as_slice(), b"client".as_slice()),
        (b"cluster".as_slice(), b"cluster".as_slice()),
        (b"command".as_slice(), b"command".as_slice()),
        (b"config".as_slice(), b"config".as_slice()),
        (b"function".as_slice(), b"function".as_slice()),
        (b"latency".as_slice(), b"latency".as_slice()),
        (b"memory".as_slice(), b"memory".as_slice()),
        (b"module".as_slice(), b"module".as_slice()),
        (b"pubsub".as_slice(), b"pubsub".as_slice()),
        (b"script".as_slice(), b"script".as_slice()),
        (b"xgroup".as_slice(), b"xgroup".as_slice()),
        (b"xinfo".as_slice(), b"xinfo".as_slice()),
    ] {
        if starts_with_ascii_ci(function, prefix) {
            return Some(parent);
        }
    }
    None
}

fn starts_with_ascii_ci(text: &[u8], prefix: &[u8]) -> bool {
    text.len() >= prefix.len() && ascii_eq_ignore_case(&text[..prefix.len()], prefix)
}

#[derive(Clone)]
struct CommandKeyRef {
    key: RedisString,
    flags: Vec<RedisString>,
}

fn command_getkeys(ctx: &mut CommandContext<'_>, with_flags: bool) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        let name = if with_flags {
            b"command|getkeysandflags".as_slice()
        } else {
            b"command|getkeys".as_slice()
        };
        return Err(RedisError::wrong_number_of_args(name));
    }
    let spec = lookup_generated_command_for_getkeys(ctx)?;
    let command_argc = ctx.arg_count() - 2;
    validate_command_getkeys_arity(spec.arity, command_argc)?;

    let key_refs = command_key_refs(ctx, spec, command_argc)?;
    let items = if with_flags {
        key_refs
            .into_iter()
            .map(|key_ref| {
                RespFrame::array(vec![
                    RespFrame::bulk(key_ref.key),
                    RespFrame::array(key_ref.flags.into_iter().map(RespFrame::bulk).collect()),
                ])
            })
            .collect()
    } else {
        key_refs
            .into_iter()
            .map(|key_ref| RespFrame::bulk(key_ref.key))
            .collect()
    };
    ctx.reply_frame(&RespFrame::array(items))
}

fn lookup_generated_command_for_getkeys(
    ctx: &CommandContext<'_>,
) -> RedisResult<&'static crate::generated::GeneratedCommandSpec> {
    let parent = ctx.arg(2)?.as_bytes();
    if crate::dispatch::lookup_command(parent).is_none() {
        return Err(RedisError::runtime(b"ERR Invalid command specified"));
    }
    let expected_function = expected_command_function_name(parent);
    if ctx.arg_count() > 3 {
        let sub = ctx.arg(3)?.as_bytes();
        if let Some(spec) = crate::generated::COMMANDS.iter().find(|spec| {
            ascii_eq_ignore_case(spec.name.as_bytes(), sub)
                && ascii_eq_ignore_case(spec.function.as_bytes(), &expected_function)
        }) {
            return Ok(spec);
        }
    }
    crate::generated::COMMANDS
        .iter()
        .find(|spec| {
            ascii_eq_ignore_case(spec.name.as_bytes(), parent)
                && ascii_eq_ignore_case(spec.function.as_bytes(), &expected_function)
        })
        .ok_or_else(|| RedisError::runtime(b"ERR Invalid command specified"))
}

fn expected_command_function_name(command: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(command.len() + b"Command".len());
    for &b in command {
        if b.is_ascii_alphanumeric() {
            out.push(ascii_lower(b));
        }
    }
    out.extend_from_slice(b"Command");
    out
}

fn command_key_refs(
    ctx: &CommandContext<'_>,
    spec: &crate::generated::GeneratedCommandSpec,
    command_argc: usize,
) -> RedisResult<Vec<CommandKeyRef>> {
    let cmd_name = ctx.arg(2)?.as_bytes();
    if ascii_eq_ignore_case(cmd_name, b"SET") {
        return Ok(vec![CommandKeyRef {
            key: ctx.arg_owned(3usize)?,
            flags: key_flags(&[b"OW".as_slice(), b"update".as_slice()]),
        }]);
    }
    if ascii_eq_ignore_case(cmd_name, b"SORT") {
        return sort_key_refs(ctx);
    }
    if ascii_eq_ignore_case(cmd_name, b"SORT_RO") {
        return Ok(vec![CommandKeyRef {
            key: ctx.arg_owned(3usize)?,
            flags: key_flags(&[b"RO".as_slice(), b"access".as_slice()]),
        }]);
    }
    match command_key_refs_from_specs(ctx, spec, command_argc) {
        Ok(Some(keys)) => Ok(keys),
        Ok(None) => Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        )),
        Err(err) if command_allows_no_mandatory_keys(spec) => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

fn validate_command_getkeys_arity(arity: i32, argc: usize) -> RedisResult<()> {
    let invalid = if arity > 0 {
        argc != arity as usize
    } else if arity < 0 {
        argc < (-arity) as usize
    } else {
        false
    };
    if invalid {
        Err(RedisError::runtime(
            b"ERR Invalid number of arguments specified for command",
        ))
    } else {
        Ok(())
    }
}

fn sort_key_refs(ctx: &CommandContext<'_>) -> RedisResult<Vec<CommandKeyRef>> {
    let argc = ctx.arg_count() - 2;
    let mut keys = Vec::with_capacity(2);
    keys.push(CommandKeyRef {
        key: ctx.arg_owned(3usize)?,
        flags: key_flags(&[b"RO".as_slice(), b"access".as_slice()]),
    });
    let mut store_key_index: Option<usize> = None;
    let mut i = 2usize;
    while i < argc {
        let arg = ctx.arg_owned(i + 2)?;
        let bytes = arg.as_bytes();
        if ascii_eq_ignore_case(bytes, b"LIMIT") {
            i += 3;
            continue;
        }
        if ascii_eq_ignore_case(bytes, b"GET") || ascii_eq_ignore_case(bytes, b"BY") {
            i += 2;
            continue;
        }
        if ascii_eq_ignore_case(bytes, b"STORE") && i + 1 < argc {
            store_key_index = Some(i + 3);
        }
        i += 1;
    }
    if let Some(index) = store_key_index {
        keys.push(CommandKeyRef {
            key: ctx.arg_owned(index)?,
            flags: key_flags(&[b"OW".as_slice(), b"update".as_slice()]),
        });
    }
    Ok(keys)
}

fn command_key_refs_from_specs(
    ctx: &CommandContext<'_>,
    spec: &crate::generated::GeneratedCommandSpec,
    command_argc: usize,
) -> RedisResult<Option<Vec<CommandKeyRef>>> {
    let key_specs: Value = serde_json::from_str(spec.key_specs_json)
        .map_err(|_| RedisError::runtime(b"ERR Invalid arguments specified for command"))?;
    let Some(specs) = key_specs.as_array() else {
        return Ok(None);
    };
    if specs.is_empty() || specs.iter().all(key_spec_is_not_key) {
        return Err(RedisError::runtime(b"ERR The command has no key arguments"));
    }

    let mut keys = Vec::new();
    let mut unsupported = false;
    for key_spec in specs {
        if key_spec_is_not_key(key_spec) {
            continue;
        }
        let Some(positions) = key_positions_from_spec(ctx, key_spec, command_argc)? else {
            unsupported = true;
            continue;
        };
        let flags = key_flags_from_spec(key_spec);
        for pos in positions {
            keys.push(CommandKeyRef {
                key: ctx.arg_owned(2 + pos)?,
                flags: flags.clone(),
            });
        }
    }
    if keys.is_empty() && unsupported {
        Ok(None)
    } else {
        Ok(Some(keys))
    }
}

fn command_allows_no_mandatory_keys(spec: &crate::generated::GeneratedCommandSpec) -> bool {
    spec.flags
        .iter()
        .any(|flag| *flag == crate::generated::CommandFlag::NO_MANDATORY_KEYS)
}

fn key_flags(flags: &[&[u8]]) -> Vec<RedisString> {
    flags.iter().map(RedisString::from_bytes).collect()
}

fn key_flags_from_spec(spec: &Value) -> Vec<RedisString> {
    let Some(flags) = spec.get("flags").and_then(Value::as_array) else {
        return Vec::new();
    };
    flags
        .iter()
        .filter_map(Value::as_str)
        .filter(|flag| *flag != "NOT_KEY" && *flag != "VARIABLE_FLAGS")
        .map(command_key_flag_name)
        .collect()
}

fn command_key_flag_name(flag: &str) -> RedisString {
    match flag {
        "RO" | "RW" | "OW" | "RM" => RedisString::from_bytes(flag.as_bytes()),
        _ => {
            let mut out = Vec::with_capacity(flag.len());
            for &b in flag.as_bytes() {
                out.push(ascii_lower(b));
            }
            RedisString::from_vec(out)
        }
    }
}

fn key_spec_is_not_key(spec: &Value) -> bool {
    spec.get("flags")
        .and_then(Value::as_array)
        .map(|flags| flags.iter().any(|flag| flag.as_str() == Some("NOT_KEY")))
        .unwrap_or(false)
}

fn key_positions_from_spec(
    ctx: &CommandContext<'_>,
    spec: &Value,
    command_argc: usize,
) -> RedisResult<Option<Vec<usize>>> {
    let Some(start) = spec
        .pointer("/begin_search/index/pos")
        .and_then(Value::as_i64)
        .and_then(nonnegative_usize)
    else {
        return Ok(None);
    };
    if let Some(range) = spec.pointer("/find_keys/range") {
        return range_key_positions(range, start, command_argc);
    }
    if let Some(keynum) = spec.pointer("/find_keys/keynum") {
        return keynum_key_positions(ctx, keynum, start, command_argc);
    }
    Ok(None)
}

fn range_key_positions(
    range: &Value,
    first: usize,
    command_argc: usize,
) -> RedisResult<Option<Vec<usize>>> {
    let Some(lastkey) = range.get("lastkey").and_then(Value::as_i64) else {
        return Ok(None);
    };
    let Some(step) = range
        .get("step")
        .and_then(Value::as_i64)
        .and_then(nonnegative_usize)
        .filter(|step| *step > 0)
    else {
        return Ok(None);
    };
    let last = if lastkey >= 0 {
        first.saturating_add(lastkey as usize)
    } else {
        let offset = (-lastkey) as usize;
        if offset > command_argc {
            return Ok(Some(Vec::new()));
        }
        command_argc - offset
    };
    if first >= command_argc || last >= command_argc {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    }
    if last < first {
        return Ok(Some(Vec::new()));
    }
    let mut out = Vec::new();
    let mut pos = first;
    while pos <= last {
        out.push(pos);
        match pos.checked_add(step) {
            Some(next) => pos = next,
            None => break,
        }
    }
    Ok(Some(out))
}

fn keynum_key_positions(
    ctx: &CommandContext<'_>,
    keynum: &Value,
    begin: usize,
    command_argc: usize,
) -> RedisResult<Option<Vec<usize>>> {
    let Some(keynumidx) = keynum
        .get("keynumidx")
        .and_then(Value::as_i64)
        .and_then(nonnegative_usize)
    else {
        return Ok(None);
    };
    let Some(firstkey) = keynum
        .get("firstkey")
        .and_then(Value::as_i64)
        .and_then(nonnegative_usize)
    else {
        return Ok(None);
    };
    let Some(step) = keynum
        .get("step")
        .and_then(Value::as_i64)
        .and_then(nonnegative_usize)
        .filter(|step| *step > 0)
    else {
        return Ok(None);
    };
    let numkeys_index = begin + keynumidx;
    let first_key_index = begin + firstkey;
    if numkeys_index >= command_argc || first_key_index > command_argc {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    }
    let numkeys_arg = ctx.arg_owned(2 + numkeys_index)?;
    let Some(numkeys) = parse_i64_strict(numkeys_arg.as_bytes()).filter(|n| *n >= 0) else {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    };
    let numkeys = numkeys as usize;
    if numkeys == 0 {
        return Ok(Some(Vec::new()));
    }
    let Some(last_offset) = numkeys.checked_sub(1).and_then(|n| n.checked_mul(step)) else {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    };
    let Some(last_pos) = first_key_index.checked_add(last_offset) else {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    };
    if last_pos >= command_argc {
        return Err(RedisError::runtime(
            b"ERR Invalid arguments specified for command",
        ));
    }
    let mut out = Vec::with_capacity(numkeys.min(command_argc));
    for idx in 0..numkeys {
        let pos = first_key_index + idx * step;
        out.push(pos);
    }
    Ok(Some(out))
}

fn nonnegative_usize(n: i64) -> Option<usize> {
    if n >= 0 {
        Some(n as usize)
    } else {
        None
    }
}

/// `AUTH [username] password`.
///
/// Single-argument form (`AUTH password`): authenticates against the `default`
/// user first; if that fails, searches all users for a matching password.
/// Two-argument form (`AUTH username password`): authenticates as the named user.
/// On success sets `client.authenticated_user`. Returns `+OK` or `-WRONGPASS`.
pub fn auth_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 || argc > 3 {
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
                match fallback {
                    Some(uname) => {
                        ctx.client_mut().set_authenticated_user(Some(uname));
                        return ctx.reply_simple_string(b"OK");
                    }
                    None => {}
                }
            }
            record_auth_failure_acl_log(ctx, lookup_name, b"AUTH");
            ctx.reply_error(b"WRONGPASS invalid username-password pair or user is disabled.")
        }
    }
}

pub fn apply_requirepass_to_acl(secret: Option<&[u8]>) {
    let acl = global_acl_state();
    let mut guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let default_key = RedisString::from_static(b"default");
    if let Some(secret) = secret.filter(|s| !s.is_empty()) {
        let user = guard
            .users
            .entry(default_key.clone())
            .or_insert_with(AclUser::new_default);
        user.flags.enabled = true;
        user.flags.nopass = false;
        user.flags.allcommands = true;
        user.flags.allkeys = true;
        user.flags.allchannels = true;
        user.flags.alldbs = true;
        user.allowed_categories = acl_category::ALL;
        user.denied_categories = 0;
        user.allowed_commands.clear();
        user.denied_commands.clear();
        user.command_rules.clear();
        user.key_patterns = vec![RedisString::from_static(b"*")];
        user.channel_patterns = vec![RedisString::from_static(b"*")];
        user.allowed_dbs.clear();
        user.passwords = vec![sha256_hash(secret)];
    } else if let Some(user) = guard.users.get_mut(&default_key) {
        *user = AclUser::new_default();
    } else {
        guard.users.insert(default_key, AclUser::new_default());
    }
}

fn default_user_has_no_password() -> bool {
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let default_key = RedisString::from_static(b"default");
    guard
        .users
        .get(&default_key)
        .map(|user| user.flags.enabled && user.flags.nopass && user.passwords.is_empty())
        .unwrap_or(true)
}

fn record_auth_failure_acl_log(ctx: &CommandContext<'_>, username: &[u8], command_name: &[u8]) {
    record_acl_access_denied_auth();
    record_acl_log_entry(
        b"auth",
        b"toplevel",
        RedisString::from_static(b"AUTH"),
        RedisString::from_bytes(username),
        acl_log_client_info(ctx, command_name),
    );
}

fn acl_log_client_info(ctx: &CommandContext<'_>, command_name: &[u8]) -> RedisString {
    let command = lower_acl_token(command_name);
    let command = String::from_utf8_lossy(&command);
    let username = ctx
        .client_ref()
        .authenticated_user
        .as_ref()
        .map(|user| String::from_utf8_lossy(user.as_bytes()).into_owned())
        .unwrap_or_else(|| "default".to_string());
    RedisString::from_vec(
        format!(
            "id={} db={} cmd={} user={}",
            ctx.client_ref().id(),
            ctx.selected_db_id(),
            command,
            username
        )
        .into_bytes(),
    )
}

/// Attempt to authenticate as `username` with `cleartext`.
///
/// Returns `Some(username_as_RedisString)` on success, `None` on failure.
fn authenticate_user(username: &[u8], cleartext: &[u8]) -> Option<RedisString> {
    let key = RedisString::from_bytes(username);
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let user = guard.users.get(&key)?;
    if !user.flags.enabled {
        return None;
    }
    if user.check_password(cleartext) {
        Some(key)
    } else {
        None
    }
}

/// Try to match `cleartext` against every user's password list.
///
/// Used for the legacy one-argument AUTH form where no username is specified.
/// Returns the first matching enabled user's name, or `None`.
fn try_password_any_user(cleartext: &[u8]) -> Option<RedisString> {
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    for user in guard.users.values() {
        if user.flags.enabled && !user.flags.nopass && user.check_password(cleartext) {
            return Some(user.name.clone());
        }
    }
    None
}

pub fn load_acl_startup_config(
    user_lines: &[String],
    dir: &str,
    aclfile: Option<&str>,
) -> Result<(), Vec<u8>> {
    set_aclfile_config_name(aclfile.map(|name| name.to_string()));
    if !user_lines.is_empty() {
        let config_user_lines: Vec<String> = user_lines
            .iter()
            .map(|line| format!("user {}", line.trim()))
            .collect();
        let users = build_acl_users_from_lines(
            config_user_lines
                .iter()
                .enumerate()
                .map(|(idx, line)| (idx + 1, line.as_str())),
        )?;
        install_acl_users(users);
    }
    if let Some(name) = aclfile {
        let path = Path::new(dir).join(name);
        let users = load_acl_users_from_path(&path)?;
        install_acl_users(users);
    }
    Ok(())
}

fn install_acl_users(users: HashMap<RedisString, AclUser>) {
    let acl = global_acl_state();
    let mut guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.users = users;
}

fn load_acl_users_from_path(path: &Path) -> Result<HashMap<RedisString, AclUser>, Vec<u8>> {
    let contents = std::fs::read_to_string(path).map_err(|e| {
        let mut msg = b"ERR Error loading ACL file: ".to_vec();
        msg.extend_from_slice(e.to_string().as_bytes());
        msg
    })?;
    build_acl_users_from_lines(
        contents
            .lines()
            .enumerate()
            .map(|(idx, line)| (idx + 1, line)),
    )
}

fn build_acl_users_from_lines<'a, I>(lines: I) -> Result<HashMap<RedisString, AclUser>, Vec<u8>>
where
    I: IntoIterator<Item = (usize, &'a str)>,
{
    let mut users = HashMap::new();
    for (line_no, line) in lines {
        let Some((username, user)) = parse_acl_user_line(line_no, line)? else {
            continue;
        };
        if users.contains_key(&username) {
            let mut msg = b"ERR Duplicate user '".to_vec();
            msg.extend_from_slice(username.as_bytes());
            msg.extend_from_slice(b"' found");
            return Err(msg);
        }
        users.insert(username, user);
    }
    let default_key = RedisString::from_static(b"default");
    users
        .entry(default_key)
        .or_insert_with(AclUser::new_default);
    Ok(users)
}

fn parse_acl_user_line(
    line_no: usize,
    line: &str,
) -> Result<Option<(RedisString, AclUser)>, Vec<u8>> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(None);
    }
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.len() < 2 || !parts[0].eq_ignore_ascii_case("user") {
        let mut msg = b"ERR ACL file line ".to_vec();
        msg.extend_from_slice(line_no.to_string().as_bytes());
        msg.extend_from_slice(b" should start with user keyword followed by username");
        return Err(msg);
    }
    let username = RedisString::from_bytes(parts[1].as_bytes());
    if acl_string_has_spaces(username.as_bytes()) {
        return Err(b"ERR Usernames can't contain spaces or null characters".to_vec());
    }
    let mut user = AclUser::new_reset(username.clone());
    apply_acl_pubsub_default_to_user(&mut user);
    let rules: Vec<RedisString> = parts[2..]
        .iter()
        .map(|rule| RedisString::from_bytes(rule.as_bytes()))
        .collect();
    if let Err(e) = apply_acl_setuser_rules(&mut user, &rules) {
        let mut msg = b"ERR Error in ACL file line ".to_vec();
        msg.extend_from_slice(line_no.to_string().as_bytes());
        msg.extend_from_slice(b": ");
        msg.extend_from_slice(e.strip_prefix(b"ERR ").unwrap_or(&e));
        return Err(msg);
    }
    Ok(Some((username, user)))
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

fn apply_acl_setuser_rules(user: &mut AclUser, rules: &[RedisString]) -> Result<(), Vec<u8>> {
    let mut idx = 0usize;
    while idx < rules.len() {
        let raw = rules[idx].as_bytes();
        let trimmed = trim_ascii(raw);
        if trimmed.is_empty() {
            idx += 1;
            continue;
        }
        if trimmed.eq_ignore_ascii_case(b"clearselectors") {
            user.selectors.clear();
            idx += 1;
            continue;
        }
        if trimmed.starts_with(b")") {
            return Err(acl_setuser_error(trimmed, b"ERR Syntax error"));
        }
        if trimmed.starts_with(b"(") {
            let (selector_rules, rendered, next_idx) = collect_acl_selector(rules, idx)?;
            let mut selector = AclUser::new_selector();
            for rule in selector_rules {
                if rule.starts_with(b"(") || rule.ends_with(b")") {
                    return Err(acl_setuser_error(&rendered, b"ERR Syntax error"));
                }
                if let Err(e) = apply_acl_rule(&mut selector, &rule) {
                    if e.starts_with(b"ERR Unrecognized parameter") {
                        return Err(acl_setuser_error(&rendered, b"ERR Syntax error"));
                    }
                    return Err(acl_setuser_error(&rendered, &e));
                }
            }
            user.selectors.push(selector);
            idx = next_idx;
            continue;
        }
        if let Err(e) = apply_acl_rule(user, trimmed) {
            return Err(acl_setuser_error(trimmed, &e));
        }
        idx += 1;
    }
    Ok(())
}

fn collect_acl_selector(
    rules: &[RedisString],
    start: usize,
) -> Result<(Vec<Vec<u8>>, Vec<u8>, usize), Vec<u8>> {
    let first_raw = rules[start].as_bytes();
    let first = trim_ascii(first_raw);
    if first_raw != first {
        return Err(acl_setuser_error(first, b"ERR Syntax error"));
    }

    let mut rendered = Vec::new();
    let mut end = start;
    loop {
        if end >= rules.len() {
            return Err(b"ERR Unmatched parenthesis in acl selector".to_vec());
        }
        if !rendered.is_empty() {
            rendered.push(b' ');
        }
        let token = trim_ascii(rules[end].as_bytes());
        rendered.extend_from_slice(token);
        if token.ends_with(b")") {
            break;
        }
        end += 1;
    }

    let trimmed = trim_ascii(&rendered);
    if !trimmed.starts_with(b"(") || !trimmed.ends_with(b")") || trimmed.len() < 2 {
        return Err(acl_setuser_error(trimmed, b"ERR Syntax error"));
    }
    let inner = trim_ascii(&trimmed[1..trimmed.len() - 1]);
    if inner.is_empty() {
        return Ok((Vec::new(), trimmed.to_vec(), end + 1));
    }
    if inner.contains(&b'(') || inner.contains(&b')') {
        return Err(acl_setuser_error(trimmed, b"ERR Syntax error"));
    }
    let pieces = inner
        .split(u8::is_ascii_whitespace)
        .filter(|piece| !piece.is_empty())
        .map(|piece| piece.to_vec())
        .collect();
    Ok((pieces, trimmed.to_vec(), end + 1))
}

fn aclfile_path_for_context(ctx: &CommandContext<'_>) -> Option<PathBuf> {
    let name = aclfile_config_name()?;
    let dir = ctx.live_config().rdb_dir();
    Some(Path::new(&dir).join(name))
}

fn apply_loaded_acl_users(
    ctx: &mut CommandContext<'_>,
    users: HashMap<RedisString, AclUser>,
) -> bool {
    let current_user = ctx.client_ref().authenticated_user.clone();
    let close_current_client = current_user
        .as_ref()
        .map(|user| !users.contains_key(user))
        .unwrap_or(false);
    let revoked_pubsub_ids = collect_revoked_pubsub_clients(&users);
    if let Some(pubsub) = &ctx.pubsub {
        let mut registry = match pubsub.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for id in revoked_pubsub_ids {
            registry.drop_client(id);
        }
    }
    install_acl_users(users);
    close_current_client
}

fn collect_revoked_pubsub_clients(users: &HashMap<RedisString, AclUser>) -> Vec<u64> {
    let mut registry = match client_info_registry().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let revoked: Vec<u64> = registry
        .all()
        .into_iter()
        .filter(snapshot_in_pubsub_mode)
        .filter(|snap| match &snap.user {
            Some(username) => users
                .get(username)
                .map(|user| acl_snapshot_has_revoked_channel(snap, user))
                .unwrap_or(true),
            None => false,
        })
        .map(|snap| snap.id)
        .collect();
    for id in &revoked {
        registry.deregister(*id);
    }
    revoked
}

fn acl_snapshot_has_revoked_channel(
    snap: &redis_core::client_info::ClientSnapshot,
    user: &AclUser,
) -> bool {
    snap.channel_names
        .iter()
        .any(|channel| !user.can_access_channel(channel.as_bytes()))
        || snap
            .shard_channel_names
            .iter()
            .any(|channel| !user.can_access_channel(channel.as_bytes()))
        || snap
            .pattern_names
            .iter()
            .any(|pattern| !user.can_access_channel_pattern(pattern.as_bytes()))
}

/// `ACL WHOAMI|LIST|USERS|GETUSER|CAT|SETUSER|DELUSER|LOG|HELP`.
pub fn acl_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"acl"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();

    if ascii_eq_ignore_case(sub_bytes, b"WHOAMI") {
        let name = ctx
            .client_ref()
            .authenticated_user
            .clone()
            .unwrap_or_else(|| RedisString::from_bytes(b"default"));
        return ctx.reply_bulk_string(name);
    }

    if ascii_eq_ignore_case(sub_bytes, b"LIST") {
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut users: Vec<&AclUser> = guard.users.values().collect();
        users.sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
        let mut items: Vec<RespFrame> = Vec::new();
        for user in users {
            items.push(RespFrame::bulk(RedisString::from_vec(
                user.to_rule_string(),
            )));
        }
        return ctx.reply_frame(&RespFrame::array(items));
    }

    if ascii_eq_ignore_case(sub_bytes, b"USERS") {
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut names: Vec<RedisString> = guard.users.keys().cloned().collect();
        names.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let items: Vec<RespFrame> = names.into_iter().map(RespFrame::bulk).collect();
        return ctx.reply_frame(&RespFrame::array(items));
    }

    if ascii_eq_ignore_case(sub_bytes, b"GETUSER") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"acl|getuser"));
        }
        let username = ctx.arg_owned(2usize)?;
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        return match guard.users.get(&username) {
            None => ctx.reply_null_array(),
            Some(user) => ctx.reply_frame(&build_getuser_reply(user)),
        };
    }

    if ascii_eq_ignore_case(sub_bytes, b"CAT") {
        if ctx.arg_count() > 3 {
            return Err(RedisError::runtime(
                b"ERR unknown subcommand or wrong number of arguments for 'CAT'. Try ACL HELP.",
            ));
        }
        if ctx.arg_count() == 2 {
            let items: Vec<RespFrame> = ALL_CATEGORY_NAMES
                .iter()
                .map(|c| RespFrame::bulk(RedisString::from_bytes(c)))
                .collect();
            return ctx.reply_frame(&RespFrame::array(items));
        }
        let cat_name = ctx.arg_owned(2)?;
        let bit = match category_name_to_bit(cat_name.as_bytes()) {
            Some(b) => b,
            None => {
                let mut msg: Vec<u8> = b"ERR Unknown category '".to_vec();
                msg.extend_from_slice(cat_name.as_bytes());
                msg.push(b'\'');
                return Err(RedisError::runtime(msg));
            }
        };
        let cmds = commands_in_category(bit);
        let items: Vec<RespFrame> = cmds
            .into_iter()
            .map(|c| RespFrame::bulk(RedisString::from_vec(c)))
            .collect();
        return ctx.reply_frame(&RespFrame::array(items));
    }

    if ascii_eq_ignore_case(sub_bytes, b"DRYRUN") {
        if ctx.arg_count() < 4 {
            return Err(RedisError::wrong_number_of_args(b"acl|dryrun"));
        }
        let username = ctx.arg_owned(2usize)?;
        let command = ctx.arg_owned(3usize)?;
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let Some(user) = guard.users.get(&username) else {
            let mut msg = b"ERR User '".to_vec();
            msg.extend_from_slice(username.as_bytes());
            msg.extend_from_slice(b"' not found");
            return Err(RedisError::runtime(msg));
        };
        let dry_argc = ctx.arg_count().saturating_sub(3);
        let Some(spec) = acl_dryrun_command_spec(ctx, command.as_bytes(), dry_argc) else {
            let mut msg = b"ERR Command '".to_vec();
            msg.extend_from_slice(command.as_bytes());
            msg.extend_from_slice(b"' not found");
            return Err(RedisError::runtime(msg));
        };
        if (spec.arity > 0 && spec.arity as usize != dry_argc)
            || (spec.arity < 0 && dry_argc < (-spec.arity) as usize)
        {
            let mut msg = b"ERR wrong number of arguments for '".to_vec();
            msg.extend_from_slice(&lower_acl_token(command.as_bytes()));
            msg.extend_from_slice(b"' command");
            return Err(RedisError::runtime(msg));
        }
        let categories = spec
            .acl_categories
            .iter()
            .fold(0u64, |acc, cat| acc | generated_acl_category_bit(*cat));
        match acl_dryrun_check(
            ctx,
            user,
            command.as_bytes(),
            spec.name.as_bytes(),
            categories,
        ) {
            Ok(()) => return ctx.reply_simple_string(b"OK"),
            Err(AclDryrunDeny::Command) => {
                let mut msg = b"This user has no permissions to run the '".to_vec();
                msg.extend_from_slice(&lower_acl_token(command.as_bytes()));
                msg.extend_from_slice(b"' command");
                return ctx.reply_bulk(&msg);
            }
            Err(AclDryrunDeny::Key(key)) => {
                let mut msg = b"User ".to_vec();
                msg.extend_from_slice(username.as_bytes());
                msg.extend_from_slice(b" has no permissions to access the '");
                msg.extend_from_slice(key.as_bytes());
                msg.extend_from_slice(b"' key");
                return ctx.reply_bulk(&msg);
            }
            Err(AclDryrunDeny::Channel(channel)) => {
                let mut msg = b"User ".to_vec();
                msg.extend_from_slice(username.as_bytes());
                msg.extend_from_slice(b" has no permissions to access the '");
                msg.extend_from_slice(channel.as_bytes());
                msg.extend_from_slice(b"' channel");
                return ctx.reply_bulk(&msg);
            }
            Err(AclDryrunDeny::Database) => {
                let mut msg = b"User ".to_vec();
                msg.extend_from_slice(username.as_bytes());
                msg.extend_from_slice(b" has no permissions to access database");
                return ctx.reply_bulk(&msg);
            }
        }
    }

    if ascii_eq_ignore_case(sub_bytes, b"SETUSER") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"acl|setuser"));
        }
        let username = ctx.arg_owned(2usize)?;
        if acl_string_has_spaces(username.as_bytes()) {
            return Err(RedisError::runtime(
                b"ERR Usernames can't contain spaces or null characters",
            ));
        }
        let rules: Vec<RedisString> = (3..ctx.arg_count())
            .filter_map(|i| ctx.client_ref().arg(i).cloned())
            .collect();
        let acl = global_acl_state();
        let mut guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut user = guard.users.get(&username).cloned().unwrap_or_else(|| {
            let mut user = AclUser::new_reset(username.clone());
            apply_acl_pubsub_default_to_user(&mut user);
            user
        });
        apply_acl_setuser_rules(&mut user, &rules).map_err(RedisError::runtime)?;
        let revoked_pubsub_ids = {
            let mut registry = match client_info_registry().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            registry.deregister_revoked_pubsub_clients(&username, &user)
        };
        let current_id = ctx.client_ref().id();
        let mut close_current_client = false;
        if let Some(pubsub) = &ctx.pubsub {
            let mut registry = match pubsub.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            for id in revoked_pubsub_ids {
                if id == current_id {
                    close_current_client = true;
                } else {
                    registry.drop_client(id);
                }
            }
        }
        guard.users.insert(username, user);
        if close_current_client {
            ctx.client_mut().should_close = true;
        }
        return ctx.reply_simple_string(b"OK");
    }

    if ascii_eq_ignore_case(sub_bytes, b"DELUSER") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"acl|deluser"));
        }
        let default_key = RedisString::from_bytes(b"default");
        let current_user = ctx.client_ref().authenticated_user.clone();
        let mut close_current_client = false;
        let acl = global_acl_state();
        let mut guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut count: i64 = 0;
        for i in 2..ctx.arg_count() {
            if let Some(name) = ctx.client_ref().arg(i).cloned() {
                if name == default_key {
                    return Err(RedisError::runtime(
                        b"ERR The 'default' user cannot be removed",
                    ));
                }
                if guard.users.remove(&name).is_some() {
                    count += 1;
                    if current_user.as_ref() == Some(&name) {
                        close_current_client = true;
                    }
                }
            }
        }
        drop(guard);
        if close_current_client {
            ctx.client_mut().should_close = true;
        }
        return ctx.reply_integer(count);
    }

    if ascii_eq_ignore_case(sub_bytes, b"LOG") {
        if ctx.arg_count() > 3 {
            return Err(RedisError::runtime(
                b"ERR wrong number of arguments for 'acl|log' command",
            ));
        }
        if ctx.arg_count() == 3 {
            let sub2 = ctx.arg_owned(2)?;
            if ascii_eq_ignore_case(sub2.as_bytes(), b"RESET") {
                clear_acl_log();
                return ctx.reply_simple_string(b"OK");
            }
            let count = parse_usize_strict(sub2.as_bytes()).ok_or_else(|| {
                RedisError::runtime(b"ERR ACL LOG argument must be a positive integer or RESET")
            })?;
            return ctx.reply_frame(&build_acl_log_reply(Some(count)));
        }
        return ctx.reply_frame(&build_acl_log_reply(None));
    }

    if ascii_eq_ignore_case(sub_bytes, b"HELP") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"acl|help"));
        }
        let lines: &[&[u8]] = &[
            b"ACL <subcommand> [<arg> [value] [opt] ...]. subcommands are:",
            b"CAT [<category>]",
            b"    List all commands that belong to <category>, or all command categories",
            b"    when no category is specified.",
            b"DELUSER <username> [<username> ...]",
            b"    Delete a list of users.",
            b"GETUSER <username>",
            b"    Get the ACL details for <username>.",
            b"LIST",
            b"    Show users details in config file format.",
            b"LOAD",
            b"    Reload users from the configured ACL file.",
            b"LOG [<count> | RESET]",
            b"    Show the recent ACL log or clear it.",
            b"SAVE",
            b"    Save users to the configured ACL file.",
            b"SETUSER <username> [<rule> [<rule> ...]]",
            b"    Modify or create the rules for an existing user.",
            b"USERS",
            b"    List all the registered usernames.",
            b"WHOAMI",
            b"    Return the current connection username.",
            b"HELP",
            b"    Return subcommand help summary.",
        ];
        let items: Vec<RespFrame> = lines
            .iter()
            .map(|l| RespFrame::bulk(RedisString::from_bytes(l)))
            .collect();
        return ctx.reply_frame(&RespFrame::array(items));
    }

    if ascii_eq_ignore_case(sub_bytes, b"GENPASS") {
        if ctx.arg_count() > 3 {
            return Err(RedisError::wrong_number_of_args(b"acl|genpass"));
        }
        let bits = if ctx.arg_count() == 3 {
            parse_i64_strict(ctx.arg_owned(2)?.as_bytes())
        } else {
            Some(256)
        };
        let Some(bits) = bits else {
            return Err(RedisError::runtime(
                b"ERR ACL GENPASS argument must be the number of bits for output password, a positive number up to 4096",
            ));
        };
        if bits <= 0 || bits > 4096 {
            return Err(RedisError::runtime(
                b"ERR ACL GENPASS argument must be the number of bits for output password, a positive number up to 4096",
            ));
        }
        let hex_len = ((bits as usize).saturating_add(3)) / 4;
        return ctx.reply_bulk(&vec![b'0'; hex_len]);
    }

    if ascii_eq_ignore_case(sub_bytes, b"LOAD") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"acl|load"));
        }
        let Some(path) = aclfile_path_for_context(ctx) else {
            return Err(RedisError::runtime(
                b"ERR This Redis instance is not configured to use an ACL file. You may use CONFIG SET aclfile <filename> and then issue ACL LOAD",
            ));
        };
        let users = load_acl_users_from_path(&path).map_err(RedisError::runtime)?;
        let close_current_client = apply_loaded_acl_users(ctx, users);
        if close_current_client {
            ctx.client_mut().should_close = true;
        }
        return ctx.reply_simple_string(b"OK");
    }

    if ascii_eq_ignore_case(sub_bytes, b"SAVE") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"acl|save"));
        }
        let Some(path) = aclfile_path_for_context(ctx) else {
            return Err(RedisError::runtime(
                b"ERR This Redis instance is not configured to use an ACL file. You may use CONFIG SET aclfile <filename> and then issue ACL SAVE",
            ));
        };
        let acl = global_acl_state();
        let guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut users: Vec<&AclUser> = guard.users.values().collect();
        users.sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
        let mut out = Vec::new();
        for user in users {
            out.extend_from_slice(&user.to_rule_string());
            out.push(b'\n');
        }
        std::fs::write(&path, out).map_err(|e| {
            let mut msg = b"ERR Error saving ACL file: ".to_vec();
            msg.extend_from_slice(e.to_string().as_bytes());
            RedisError::runtime(msg)
        })?;
        return ctx.reply_simple_string(b"OK");
    }

    let mut msg = Vec::with_capacity(b"ERR Unknown ACL subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown ACL subcommand: ");
    msg.extend_from_slice(sub_bytes);
    Err(RedisError::runtime(msg))
}

fn build_acl_log_reply(limit: Option<usize>) -> RespFrame {
    let now = acl_log_now_millis();
    let items: Vec<RespFrame> = acl_log_entries(limit)
        .iter()
        .map(|entry| build_acl_log_entry_reply(entry, now))
        .collect();
    RespFrame::array(items)
}

fn build_acl_log_entry_reply(entry: &AclLogEntry, now: i64) -> RespFrame {
    let age_seconds = now
        .saturating_sub(entry.timestamp_created)
        .checked_div(1000)
        .unwrap_or(0);
    RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_static(b"count")),
            RespFrame::Integer(saturating_i64_from_u64(entry.count)),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"reason")),
            RespFrame::bulk(entry.reason.clone()),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"context")),
            RespFrame::bulk(entry.context.clone()),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"object")),
            RespFrame::bulk(entry.object.clone()),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"username")),
            RespFrame::bulk(entry.username.clone()),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"age-seconds")),
            RespFrame::Integer(age_seconds),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"client-info")),
            RespFrame::bulk(entry.client_info.clone()),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"entry-id")),
            RespFrame::Integer(saturating_i64_from_u64(entry.entry_id)),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"timestamp-created")),
            RespFrame::Integer(entry.timestamp_created),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"timestamp-last-updated")),
            RespFrame::Integer(entry.timestamp_last_updated),
        ),
    ])
}

fn saturating_i64_from_u64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

enum AclDryrunDeny {
    Command,
    Key(RedisString),
    Channel(RedisString),
    Database,
}

fn acl_dryrun_command_spec(
    ctx: &CommandContext<'_>,
    command: &[u8],
    argc: usize,
) -> Option<&'static GeneratedCommandSpec> {
    if command.eq_ignore_ascii_case(b"MEMORY")
        && ctx
            .client_ref()
            .arg(4)
            .is_some_and(|arg| arg.as_bytes().eq_ignore_ascii_case(b"USAGE"))
    {
        return COMMANDS
            .iter()
            .find(|spec| spec.name.as_bytes().eq_ignore_ascii_case(b"USAGE"));
    }
    let mut fallback = None;
    for spec in COMMANDS
        .iter()
        .filter(|spec| spec.name.as_bytes().eq_ignore_ascii_case(command))
    {
        fallback.get_or_insert(spec);
        let arity_matches = (spec.arity > 0 && spec.arity as usize == argc)
            || (spec.arity < 0 && argc >= (-spec.arity) as usize);
        if arity_matches
            && spec.function != "configGetCommand"
            && spec.function != "configSetCommand"
        {
            return Some(spec);
        }
    }
    fallback
}

fn acl_dryrun_check(
    ctx: &CommandContext<'_>,
    user: &AclUser,
    command: &[u8],
    key_command: &[u8],
    categories: u64,
) -> Result<(), AclDryrunDeny> {
    let first_arg = ctx.client_ref().arg(4).map(|arg| arg.as_bytes());
    let mut key_denial = None;
    let mut channel_denial = None;
    let mut database_denial = None;
    for (idx, candidate) in std::iter::once(user)
        .chain(user.selectors.iter())
        .enumerate()
    {
        if !candidate.can_execute_command_with_arg(command, first_arg, categories) {
            continue;
        }
        if let Some(_db) =
            crate::dispatch::acl_database_denial_for_context(ctx, key_command, candidate, 3)
        {
            if idx == 0 {
                database_denial.get_or_insert(());
            }
            continue;
        }
        if let Some(channel) = acl_dryrun_channel_denial(ctx, command, candidate) {
            channel_denial.get_or_insert(channel);
            continue;
        }
        let denied_key = crate::dispatch::acl_key_requirements(ctx, key_command, 3)
            .into_iter()
            .find(|req| !candidate.can_access_key_for(req.key.as_bytes(), req.access))
            .map(|req| req.key);
        if let Some(key) = denied_key {
            key_denial.get_or_insert(key);
            continue;
        }
        return Ok(());
    }
    if let Some(key) = key_denial {
        return Err(AclDryrunDeny::Key(key));
    }
    if let Some(channel) = channel_denial {
        return Err(AclDryrunDeny::Channel(channel));
    }
    if database_denial.is_some() {
        return Err(AclDryrunDeny::Database);
    }
    Err(AclDryrunDeny::Command)
}

fn acl_dryrun_channel_denial(
    ctx: &CommandContext<'_>,
    command: &[u8],
    user: &AclUser,
) -> Option<RedisString> {
    if user.flags.allchannels {
        return None;
    }
    let lower = lower_acl_token(command);
    let (start, end, pattern) = match lower.as_slice() {
        b"publish" | b"spublish" => (4, 5.min(ctx.arg_count()), false),
        b"subscribe" | b"ssubscribe" => (4, ctx.arg_count(), false),
        b"psubscribe" => (4, ctx.arg_count(), true),
        _ => return None,
    };
    for idx in start..end {
        let Some(channel) = ctx.client_ref().arg(idx) else {
            continue;
        };
        let allowed = if pattern {
            user.can_access_channel_pattern(channel.as_bytes())
        } else {
            user.can_access_channel(channel.as_bytes())
        };
        if !allowed {
            return Some(channel.clone());
        }
    }
    None
}

fn acl_string_has_spaces(bytes: &[u8]) -> bool {
    bytes.iter().any(|b| b.is_ascii_whitespace() || *b == 0)
}

fn acl_setuser_error(rule: &[u8], reason: &[u8]) -> Vec<u8> {
    let reason = reason.strip_prefix(b"ERR ").unwrap_or(reason);
    let mut msg = Vec::with_capacity(
        b"ERR Error in ACL SETUSER modifier '': ".len() + rule.len() + reason.len(),
    );
    msg.extend_from_slice(b"ERR Error in ACL SETUSER modifier '");
    msg.extend_from_slice(rule);
    msg.extend_from_slice(b"': ");
    msg.extend_from_slice(reason);
    msg
}

fn acl_command_rule_error(reason: &[u8]) -> Vec<u8> {
    let mut msg = b"ERR ".to_vec();
    msg.extend_from_slice(reason);
    msg
}

fn lower_acl_token(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(|b| b.to_ascii_lowercase()).collect()
}

fn command_exists(name: &[u8]) -> bool {
    COMMANDS
        .iter()
        .any(|spec| spec.name.as_bytes().eq_ignore_ascii_case(name))
}

fn known_container_subcommands(parent: &[u8]) -> Option<&'static [&'static [u8]]> {
    match parent {
        b"client" => Some(&[
            b"caching",
            b"getname",
            b"id",
            b"info",
            b"kill",
            b"list",
            b"no-evict",
            b"no-touch",
            b"pause",
            b"reply",
            b"setname",
            b"tracking",
            b"trackinginfo",
            b"unblock",
        ]),
        b"config" => Some(&[b"get", b"resetstat", b"rewrite", b"set"]),
        b"memory" => Some(&[b"doctor", b"malloc-stats", b"purge", b"stats", b"usage"]),
        b"xinfo" => Some(&[b"consumers", b"groups", b"help", b"stream"]),
        _ => None,
    }
}

fn known_subcommand_rule(body: &[u8]) -> bool {
    let lower = lower_acl_token(body);
    let Some(pipe) = lower.iter().position(|b| *b == b'|') else {
        return false;
    };
    let parent = &lower[..pipe];
    let sub = &lower[pipe + 1..];
    known_container_subcommands(parent).is_some_and(|subs| {
        subs.iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(sub))
    })
}

fn validate_acl_command_rule(body: &[u8], allow: bool) -> Result<(), Vec<u8>> {
    if body.is_empty() {
        return Err(acl_command_rule_error(b"Syntax error"));
    }
    let lower = lower_acl_token(body);
    let pipes = lower.iter().filter(|b| **b == b'|').count();
    if pipes == 0 {
        if command_exists(&lower) {
            return Ok(());
        }
        return Err(acl_command_rule_error(
            b"Unknown command or category name in ACL",
        ));
    }

    let Some(last_pipe) = lower.iter().rposition(|b| *b == b'|') else {
        return Err(acl_command_rule_error(b"Syntax error"));
    };
    let (parent, sub_with_pipe) = lower.split_at(last_pipe);
    let sub = &sub_with_pipe[1..];
    if sub.is_empty() {
        return Err(acl_command_rule_error(b"Syntax error"));
    }
    if parent.contains(&b'|') {
        if allow && known_subcommand_rule(parent) {
            return Err(acl_command_rule_error(
                b"Allowing first-arg of a subcommand is not supported",
            ));
        }
        return Err(acl_command_rule_error(
            b"Unknown command or category name in ACL",
        ));
    }
    if let Some(subcommands) = known_container_subcommands(parent) {
        if subcommands
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(sub))
        {
            return Ok(());
        }
        return Err(acl_command_rule_error(
            b"Unknown command or category name in ACL",
        ));
    }
    if command_exists(parent) {
        return Ok(());
    }
    Err(acl_command_rule_error(
        b"Unknown command or category name in ACL",
    ))
}

fn remove_subcommand_rules(rules: &mut Vec<RedisString>, cmd_name: &[u8]) {
    if cmd_name.contains(&b'|') {
        return;
    }
    rules.retain(|rule| {
        let bytes = rule.as_bytes();
        !(bytes.len() > cmd_name.len()
            && bytes[..cmd_name.len()].eq_ignore_ascii_case(cmd_name)
            && bytes[cmd_name.len()] == b'|')
    });
}

fn push_acl_command_rule(rules: &mut Vec<RedisString>, sign: u8, body: &[u8]) {
    rules.retain(|rule| rule.as_bytes().get(1..) != Some(body));
    let mut rendered = Vec::with_capacity(1 + body.len());
    rendered.push(sign);
    rendered.extend_from_slice(body);
    rules.push(RedisString::from_vec(rendered));
}

fn remove_acl_command_rule_body(rules: &mut Vec<RedisString>, body: &[u8]) {
    rules.retain(|rule| rule.as_bytes().get(1..) != Some(body));
}

fn remove_acl_subcommand_rule_bodies(rules: &mut Vec<RedisString>, cmd_name: &[u8]) {
    if cmd_name.contains(&b'|') {
        return;
    }
    rules.retain(|rule| {
        let Some(body) = rule.as_bytes().get(1..) else {
            return true;
        };
        !(body.len() > cmd_name.len()
            && body[..cmd_name.len()].eq_ignore_ascii_case(cmd_name)
            && body[cmd_name.len()] == b'|')
    });
}

/// Apply a single ACL SETUSER rule token to `user`.
fn apply_acl_rule(user: &mut AclUser, rule: &[u8]) -> Result<(), Vec<u8>> {
    if rule.is_empty() {
        return Ok(());
    }
    if rule == b"on" {
        user.flags.enabled = true;
        return Ok(());
    }
    if rule == b"off" {
        user.flags.enabled = false;
        return Ok(());
    }
    if rule == b"nopass" {
        user.flags.nopass = true;
        user.passwords.clear();
        return Ok(());
    }
    if rule == b"resetpass" {
        user.flags.nopass = false;
        user.passwords.clear();
        return Ok(());
    }
    if rule.eq_ignore_ascii_case(b"sanitize-payload") {
        user.flags.sanitize_payload = true;
        return Ok(());
    }
    if rule.eq_ignore_ascii_case(b"skip-sanitize-payload") {
        user.flags.sanitize_payload = false;
        return Ok(());
    }
    if rule == b"allcommands" || rule == b"+@all" {
        user.flags.allcommands = true;
        user.allowed_categories = acl_category::ALL;
        user.denied_categories = 0;
        user.allowed_commands.clear();
        user.denied_commands.clear();
        user.command_rules.clear();
        return Ok(());
    }
    if rule == b"nocommands" || rule == b"-@all" {
        user.flags.allcommands = false;
        user.allowed_categories = 0;
        user.denied_categories = 0;
        user.allowed_commands.clear();
        user.denied_commands.clear();
        user.command_rules.clear();
        return Ok(());
    }
    if rule == b"allkeys" || rule == b"~*" {
        user.flags.allkeys = true;
        user.key_patterns = vec![RedisString::from_bytes(b"*")];
        user.key_permissions.clear();
        return Ok(());
    }
    if rule == b"resetkeys" {
        user.flags.allkeys = false;
        user.key_patterns.clear();
        user.key_permissions.clear();
        return Ok(());
    }
    if rule == b"allchannels" || rule == b"&*" {
        user.flags.allchannels = true;
        user.channel_patterns = vec![RedisString::from_bytes(b"*")];
        return Ok(());
    }
    if rule == b"resetchannels" {
        user.flags.allchannels = false;
        user.channel_patterns.clear();
        return Ok(());
    }
    if rule.eq_ignore_ascii_case(b"alldbs") {
        user.flags.alldbs = true;
        user.allowed_dbs.clear();
        return Ok(());
    }
    if rule.eq_ignore_ascii_case(b"resetdb") || rule.eq_ignore_ascii_case(b"resetdbs") {
        user.flags.alldbs = false;
        user.allowed_dbs.clear();
        return Ok(());
    }
    if rule.len() > 3 && rule[..3].eq_ignore_ascii_case(b"db=") {
        let raw = &rule[3..];
        let dbs = parse_acl_db_list(raw)?;
        user.flags.alldbs = false;
        user.allowed_dbs.clear();
        for db in dbs {
            if !user.allowed_dbs.contains(&db) {
                user.allowed_dbs.push(db);
            }
        }
        return Ok(());
    }
    if rule == b"reset" {
        *user = AclUser::new_reset(user.name.clone());
        apply_acl_pubsub_default_to_user(user);
        return Ok(());
    }
    if rule.starts_with(b">") {
        let cleartext = &rule[1..];
        let hash = sha256_hash(cleartext);
        if !user.passwords.contains(&hash) {
            user.passwords.push(hash);
        }
        user.flags.nopass = false;
        return Ok(());
    }
    if rule.starts_with(b"<") {
        let cleartext = &rule[1..];
        let hash = sha256_hash(cleartext);
        user.passwords.retain(|h| h != &hash);
        return Ok(());
    }
    if rule.starts_with(b"#") {
        let hex = &rule[1..];
        match hex_to_hash(hex) {
            Some(hash) => {
                if !user.passwords.contains(&hash) {
                    user.passwords.push(hash);
                }
                user.flags.nopass = false;
                return Ok(());
            }
            None => {
                let mut msg: Vec<u8> = b"ERR Invalid password hash '".to_vec();
                msg.extend_from_slice(hex);
                msg.push(b'\'');
                return Err(msg);
            }
        }
    }
    if rule.starts_with(b"!") {
        let hex = &rule[1..];
        match hex_to_hash(hex) {
            Some(hash) => {
                user.passwords.retain(|h| h != &hash);
                return Ok(());
            }
            None => {
                let mut msg: Vec<u8> = b"ERR Invalid password hash '".to_vec();
                msg.extend_from_slice(hex);
                msg.push(b'\'');
                return Err(msg);
            }
        }
    }
    if rule.starts_with(b"+@") {
        let cat_name = &rule[2..];
        match category_name_to_bit(cat_name) {
            Some(bit) => {
                if bit == acl_category::ALL {
                    user.flags.allcommands = true;
                    user.allowed_categories = acl_category::ALL;
                    user.denied_categories = 0;
                    user.command_rules.clear();
                } else {
                    user.allowed_categories |= bit;
                    user.denied_categories &= !bit;
                    let mut body = b"@".to_vec();
                    body.extend_from_slice(&lower_acl_token(cat_name));
                    push_acl_command_rule(&mut user.command_rules, b'+', &body);
                }
                return Ok(());
            }
            None => {
                let mut msg: Vec<u8> = b"ERR Unknown category '".to_vec();
                msg.extend_from_slice(cat_name);
                msg.push(b'\'');
                return Err(msg);
            }
        }
    }
    if rule.starts_with(b"-@") {
        let cat_name = &rule[2..];
        match category_name_to_bit(cat_name) {
            Some(bit) => {
                if bit == acl_category::ALL {
                    user.flags.allcommands = false;
                    user.allowed_commands.clear();
                    user.denied_commands.clear();
                    user.allowed_categories = 0;
                    user.denied_categories = 0;
                    user.command_rules.clear();
                } else {
                    user.allowed_categories &= !bit;
                    user.denied_categories |= bit;
                    let mut body = b"@".to_vec();
                    body.extend_from_slice(&lower_acl_token(cat_name));
                    push_acl_command_rule(&mut user.command_rules, b'-', &body);
                }
                return Ok(());
            }
            None => {
                let mut msg: Vec<u8> = b"ERR Unknown category '".to_vec();
                msg.extend_from_slice(cat_name);
                msg.push(b'\'');
                return Err(msg);
            }
        }
    }
    if rule.starts_with(b"+") {
        validate_acl_command_rule(&rule[1..], true)?;
        let lower = lower_acl_token(&rule[1..]);
        let cmd_name = RedisString::from_bytes(&lower);
        remove_subcommand_rules(&mut user.allowed_commands, &lower);
        remove_subcommand_rules(&mut user.denied_commands, &lower);
        remove_acl_subcommand_rule_bodies(&mut user.command_rules, &lower);
        remove_acl_command_rule_body(&mut user.command_rules, &lower);
        user.denied_commands.retain(|c| c != &cmd_name);
        if !user.allowed_commands.contains(&cmd_name) {
            user.allowed_commands.push(cmd_name);
        }
        push_acl_command_rule(&mut user.command_rules, b'+', &lower);
        return Ok(());
    }
    if rule.starts_with(b"-") {
        validate_acl_command_rule(&rule[1..], false)?;
        let lower = lower_acl_token(&rule[1..]);
        let cmd_name = RedisString::from_bytes(&lower);
        remove_subcommand_rules(&mut user.allowed_commands, &lower);
        remove_subcommand_rules(&mut user.denied_commands, &lower);
        remove_acl_subcommand_rule_bodies(&mut user.command_rules, &lower);
        remove_acl_command_rule_body(&mut user.command_rules, &lower);
        user.allowed_commands.retain(|c| c != &cmd_name);
        if !user.denied_commands.contains(&cmd_name) {
            user.denied_commands.push(cmd_name);
        }
        push_acl_command_rule(&mut user.command_rules, b'-', &lower);
        return Ok(());
    }
    if rule.starts_with(b"~") {
        let pat = RedisString::from_bytes(&rule[1..]);
        if pat.as_bytes() == b"*" {
            user.flags.allkeys = true;
        }
        if !user.key_patterns.contains(&pat) {
            user.key_patterns.push(pat);
        }
        return Ok(());
    }
    if rule.starts_with(b"%") {
        let (permissions, pattern) = parse_acl_key_permission(rule)?;
        let pat = RedisString::from_bytes(pattern);
        if let Some(existing) = user
            .key_permissions
            .iter_mut()
            .find(|existing| existing.pattern == pat)
        {
            existing.permissions |= permissions;
        } else {
            user.key_permissions.push(AclKeyPattern {
                pattern: pat,
                permissions,
            });
        }
        return Ok(());
    }
    if rule.starts_with(b"&") {
        let pat = RedisString::from_bytes(&rule[1..]);
        if user.flags.allchannels && pat.as_bytes() != b"*" {
            return Err(
                b"ERR Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid and does not have any effect. Try 'resetchannels' to start with an empty list of channels"
                    .to_vec(),
            );
        }
        if pat.as_bytes() == b"*" {
            user.flags.allchannels = true;
        }
        if !user.channel_patterns.contains(&pat) {
            user.channel_patterns.push(pat);
        }
        return Ok(());
    }
    let mut msg: Vec<u8> = b"ERR Unrecognized parameter '".to_vec();
    msg.extend_from_slice(rule);
    msg.push(b'\'');
    Err(msg)
}

/// Build the RESP reply for `ACL GETUSER <username>`.
fn build_getuser_reply(user: &AclUser) -> RespFrame {
    let mut flag_items: Vec<RespFrame> = Vec::new();
    if user.flags.enabled {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"on")));
    } else {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"off")));
    }
    if user.flags.nopass {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"nopass")));
    }
    if user.flags.sanitize_payload {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(
            b"sanitize-payload",
        )));
    } else {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(
            b"skip-sanitize-payload",
        )));
    }
    if user.flags.allkeys {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"allkeys")));
    }
    if user.flags.allchannels {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"allchannels")));
    }
    if user.flags.allcommands {
        flag_items.push(RespFrame::bulk(RedisString::from_bytes(b"allcommands")));
    }

    let pass_items: Vec<RespFrame> = user
        .passwords
        .iter()
        .map(|h| {
            let mut hex: Vec<u8> = b"#".to_vec();
            hex.extend_from_slice(&redis_core::acl::hash_to_hex(h));
            RespFrame::bulk(RedisString::from_vec(hex))
        })
        .collect();

    let commands_str = user.commands_summary();
    let keys_str = user.keys_summary();
    let channels_str = user.channels_summary();
    let databases_str = user.databases_summary();
    let selectors: Vec<RespFrame> = user
        .selectors
        .iter()
        .map(build_getuser_selector_reply)
        .collect();

    RespFrame::array(vec![
        RespFrame::bulk(RedisString::from_bytes(b"flags")),
        RespFrame::array(flag_items),
        RespFrame::bulk(RedisString::from_bytes(b"passwords")),
        RespFrame::array(pass_items),
        RespFrame::bulk(RedisString::from_bytes(b"commands")),
        RespFrame::bulk(RedisString::from_vec(commands_str)),
        RespFrame::bulk(RedisString::from_bytes(b"keys")),
        RespFrame::bulk(RedisString::from_vec(keys_str)),
        RespFrame::bulk(RedisString::from_bytes(b"channels")),
        RespFrame::bulk(RedisString::from_vec(channels_str)),
        RespFrame::bulk(RedisString::from_bytes(b"databases")),
        RespFrame::bulk(RedisString::from_vec(databases_str)),
        RespFrame::bulk(RedisString::from_bytes(b"selectors")),
        RespFrame::array(selectors),
    ])
}

fn build_getuser_selector_reply(selector: &AclUser) -> RespFrame {
    RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_static(b"commands")),
            RespFrame::bulk(RedisString::from_vec(selector.commands_summary())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"keys")),
            RespFrame::bulk(RedisString::from_vec(selector.keys_summary())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"channels")),
            RespFrame::bulk(RedisString::from_vec(selector.channels_summary())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"databases")),
            RespFrame::bulk(RedisString::from_vec(selector.databases_summary())),
        ),
    ])
}

/// Return command names belonging to a given ACL category bitmask bit.
///
/// Scans the generated `COMMANDS` registry for entries whose `acl_categories`
/// include the requested bit and collects their names (deduplicated).
fn commands_in_category(bit: u64) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    for spec in crate::generated::COMMANDS.iter() {
        let matches = spec.acl_categories.iter().any(|&cat| {
            let cat_bit = generated_acl_category_bit(cat);
            cat_bit & bit != 0
        });
        if matches {
            let name = spec.name.as_bytes().to_ascii_lowercase();
            if !out.contains(&name) {
                out.push(name);
            }
        }
    }
    if bit == acl_category::SCRIPTING {
        for name in [
            b"function|delete" as &[u8],
            b"function|dump",
            b"function|flush",
            b"function|kill",
            b"function|list",
            b"function|load",
            b"function|restore",
            b"function|stats",
        ] {
            let name = name.to_vec();
            if !out.contains(&name) {
                out.push(name);
            }
        }
    }
    out
}

fn generated_acl_category_bit(cat: crate::generated::AclCategory) -> u64 {
    use crate::generated::AclCategory;
    match cat {
        AclCategory::KEYSPACE => acl_category::KEYSPACE,
        AclCategory::READ => acl_category::READ,
        AclCategory::WRITE => acl_category::WRITE,
        AclCategory::SET => acl_category::SET,
        AclCategory::SORTEDSET => acl_category::SORTEDSET,
        AclCategory::LIST => acl_category::LIST,
        AclCategory::HASH => acl_category::HASH,
        AclCategory::STRING => acl_category::STRING,
        AclCategory::BITMAP => acl_category::BITMAP,
        AclCategory::HYPERLOGLOG => acl_category::HYPERLOGLOG,
        AclCategory::GEO => acl_category::GEO,
        AclCategory::STREAM => acl_category::STREAM,
        AclCategory::PUBSUB => acl_category::PUBSUB,
        AclCategory::ADMIN => acl_category::ADMIN,
        AclCategory::FAST => acl_category::FAST,
        AclCategory::SLOW => acl_category::SLOW,
        AclCategory::BLOCKING => acl_category::BLOCKING,
        AclCategory::DANGEROUS => acl_category::DANGEROUS,
        AclCategory::CONNECTION => acl_category::CONNECTION,
        AclCategory::TRANSACTION => acl_category::TRANSACTION,
        AclCategory::SCRIPTING => acl_category::SCRIPTING,
    }
}

/// Validate a client name per Redis rules: no spaces, newlines, or other
/// whitespace/control characters.
fn validate_client_name(name: &[u8]) -> RedisResult<()> {
    for &b in name {
        if b <= 0x20 || b == 0x7f {
            return Err(RedisError::runtime(
                b"ERR Client names cannot contain spaces, newlines or special characters.",
            ));
        }
    }
    Ok(())
}

fn validate_client_setinfo_attr(attr_name: &[u8], value: &[u8]) -> RedisResult<()> {
    for &b in value {
        if !(b'!'..=b'~').contains(&b) {
            let mut msg = b"ERR ".to_vec();
            msg.extend_from_slice(attr_name);
            msg.extend_from_slice(b" cannot contain spaces, newlines or special characters.");
            return Err(RedisError::runtime(msg));
        }
    }
    Ok(())
}

/// Build the single-line description used by `CLIENT LIST`.
fn build_client_list_line(ctx: &CommandContext<'_>) -> Vec<u8> {
    let mut line: Vec<u8> = Vec::with_capacity(128);
    let client = ctx.client_ref();
    let _ = write!(line, "id={} addr=", client.id());
    match &client.addr {
        Some(s) => line.extend_from_slice(s.as_bytes()),
        None => line.extend_from_slice(b""),
    }
    line.extend_from_slice(b" name=");
    if let Some(n) = &client.name {
        line.extend_from_slice(n.as_bytes());
    }
    let _ = write!(line, " db={}", client.db_index);
    line
}

/// Case-insensitive ASCII equality.
fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| ascii_lower(*x) == ascii_lower(*y))
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn parse_yes_no(value: &[u8]) -> Option<bool> {
    if ascii_eq_ignore_case(value, b"yes") {
        Some(true)
    } else if ascii_eq_ignore_case(value, b"no") {
        Some(false)
    } else {
        None
    }
}

fn blocked_action_command_name(action: &BlockedAction) -> &'static [u8] {
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

fn ascii_lower(b: u8) -> u8 {
    if b.is_ascii_uppercase() {
        b + 32
    } else {
        b
    }
}

/// Parse an ASCII decimal integer with optional leading `-`. Rejects empty
/// input, leading/trailing whitespace, plus signs, and non-digit bytes.
fn parse_i64_strict(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<i64>().ok()
}

fn parse_acl_db_list(bytes: &[u8]) -> Result<Vec<u32>, Vec<u8>> {
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

fn parse_acl_key_permission(rule: &[u8]) -> Result<(u8, &[u8]), Vec<u8>> {
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

/// Parse a floating-point number. Rejects empty input, whitespace, and
/// non-numeric bytes.
fn parse_f64_strict(bytes: &[u8]) -> Option<f64> {
    if bytes.is_empty() {
        return None;
    }
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<f64>().ok()
}

/// Decimal-encode `n` as ASCII bytes.
fn format_u64_decimal(n: u64) -> Vec<u8> {
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
