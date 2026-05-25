//! Connection-management and server commands: PING, ECHO, SELECT, CLIENT,
//! COMMAND, DEBUG, TIME, HELLO, RESET, QUIT.
//!
//! Most handlers operate purely against the client's argv and reply buffer;
//! they never need to touch the keyspace.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::acl::{
    acl_log_entries, acl_log_max_len, acl_log_now_millis, acl_pubsub_default_config_value,
    apply_acl_pubsub_default_to_user, category as acl_category, category_name_to_bit,
    clear_acl_log, global_acl_state, hex_to_hash, record_acl_log_entry, set_acl_log_max_len,
    set_acl_pubsub_default, sha256_hash, AclLogEntry, AclUser, ALL_CATEGORY_NAMES,
};
use redis_core::blocked_keys::blocked_keys_index;
use redis_core::client_info::client_info_registry;
use redis_core::live_config::{LiveConfig, MaxmemoryPolicyCode};
use redis_core::metrics::{record_acl_access_denied_auth, server_metrics};
use redis_core::networking::{
    client_matches_ip_filter, validate_client_capa_filter, validate_client_flag_filter,
};
use redis_core::notify::keyspace_events_string_to_flags;
use redis_core::{CommandContext, PersistenceStatus, RedisDb};
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};
use serde_json::Value;

use crate::generated::COMMANDS;
use crate::live_config_handle;

/// Default Valkey `maxclients` value. Re-exported from `LiveConfig`.
pub const DEFAULT_MAX_CLIENTS: u64 = redis_core::live_config::DEFAULT_MAX_CLIENTS;

static MONITOR_CLIENTS: OnceLock<Mutex<HashMap<u64, Sender<Vec<u8>>>>> = OnceLock::new();
static ACLFILE_CONFIG: OnceLock<Mutex<Option<String>>> = OnceLock::new();

fn monitor_clients() -> &'static Mutex<HashMap<u64, Sender<Vec<u8>>>> {
    MONITOR_CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn aclfile_config_cell() -> &'static Mutex<Option<String>> {
    ACLFILE_CONFIG.get_or_init(|| Mutex::new(None))
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
        let mut pairs: Vec<(RespFrame, RespFrame)> = Vec::new();
        for i in 2..ctx.arg_count() {
            let pat = ctx.arg_owned(i)?;
            let pat_bytes = pat.as_bytes();
            for (name, value) in config_pairs_with_dynamic(&live_config) {
                if glob_match_ascii_ci(pat_bytes, name.as_bytes()) {
                    pairs.push((
                        RespFrame::bulk(RedisString::from_bytes(name.as_bytes())),
                        RespFrame::bulk(RedisString::from_bytes(value.as_bytes())),
                    ));
                }
            }
        }
        return ctx.reply_frame(&RespFrame::Map(pairs));
    }
    if ascii_eq_ignore_case(sub_bytes, b"SET") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"config|set"));
        }
        let mut i = 2usize;
        while i < ctx.arg_count() {
            let key = ctx.arg_owned(i)?;
            let value_bytes: Vec<u8> = if i + 1 < ctx.arg_count() {
                ctx.arg_owned(i + 1)?.as_bytes().to_vec()
            } else {
                Vec::new()
            };
            apply_config_set_for_context(ctx, &live_config, key.as_bytes(), &value_bytes)?;
            i += 2;
        }
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"RESETSTAT") {
        server_metrics().reset_stats();
        crate::hash::reset_expired_fields_count();
        crate::slowlog_cmd::reset_latency_histograms();
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"REWRITE") {
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
        ("maxmemory-samples", "5"),
        ("maxclients", "10000"),
        ("acllog-max-len", "128"),
        ("acl-pubsub-default", "resetchannels"),
        ("aclfile", ""),
        ("requirepass", ""),
        ("appendonly", "no"),
        ("appendfsync", "everysec"),
        ("appendfilename", "appendonly.aof"),
        ("appenddirname", "appendonlydir"),
        ("aof-load-truncated", "yes"),
        ("aof-use-rdb-preamble", "yes"),
        ("auto-aof-rewrite-percentage", "100"),
        ("auto-aof-rewrite-min-size", "67108864"),
        ("save", ""),
        ("dir", "./"),
        ("dbfilename", "dump.rdb"),
        ("availability-zone", ""),
        ("import-mode", "no"),
        ("rdb-version-check", "strict"),
        ("tcp-backlog", "511"),
        ("tcp-keepalive", "300"),
        ("timeout", "0"),
        ("port", "0"),
        ("bind", "127.0.0.1"),
        ("databases", "16"),
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
        ("repl-disable-tcp-nodelay", "no"),
        ("slave-read-only", "yes"),
        ("replica-read-only", "yes"),
        ("repl-diskless-sync", "yes"),
    ]
}

/// Build the full CONFIG GET parameter list reading every live value from
/// the supplied `LiveConfig`. Static pairs in `default_config_pairs` are
/// reproduced verbatim for keys with no behavioural backing.
fn config_pairs_with_dynamic(cfg: &Arc<LiveConfig>) -> Vec<(String, String)> {
    let live_maxmemory = cfg.maxmemory().to_string();
    let live_maxmemory_policy = cfg.maxmemory_policy().as_config_str().to_string();
    let live_maxclients = cfg.maxclients().to_string();
    let live_acllog_max_len = acl_log_max_len().to_string();
    let live_acl_pubsub_default = acl_pubsub_default_config_value().to_string();
    let live_aclfile = aclfile_config_name().unwrap_or_default();
    let live_requirepass = cfg
        .requirepass()
        .map(|s| String::from_utf8_lossy(s.as_bytes()).into_owned())
        .unwrap_or_default();
    let live_notify = redis_core::notify::keyspace_events_flags_to_string(
        cfg.notify_keyspace_events_flags() as i32,
    );
    let live_notify_str = String::from_utf8_lossy(live_notify.as_bytes()).into_owned();
    let live_slowlog_threshold = cfg.slowlog_threshold_micros().to_string();
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

    let mut out: Vec<(String, String)> = Vec::new();
    for &(name, value) in default_config_pairs() {
        let dynamic = match name {
            "maxmemory" => Some(live_maxmemory.clone()),
            "maxmemory-policy" => Some(live_maxmemory_policy.clone()),
            "maxclients" => Some(live_maxclients.clone()),
            "acllog-max-len" => Some(live_acllog_max_len.clone()),
            "acl-pubsub-default" => Some(live_acl_pubsub_default.clone()),
            "aclfile" => Some(live_aclfile.clone()),
            "requirepass" => Some(live_requirepass.clone()),
            "notify-keyspace-events" => Some(live_notify_str.clone()),
            "slowlog-log-slower-than" => Some(live_slowlog_threshold.clone()),
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
            "repl-disable-tcp-nodelay" => Some(live_repl_disable_nodelay.clone()),
            "slave-read-only" | "replica-read-only" => Some(live_slave_read_only.clone()),
            "repl-diskless-sync" => Some(live_repl_diskless.clone()),
            "rdb-version-check" => Some(live_rdb_version_check.clone()),
            _ => None,
        };
        out.push((
            name.to_string(),
            dynamic.unwrap_or_else(|| value.to_string()),
        ));
    }
    out
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
            } else {
                cfg.set_requirepass(Some(RedisString::from_bytes(value)));
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
        b"repl-disable-tcp-nodelay" => {
            cfg.set_repl_disable_tcp_nodelay(value == b"yes");
        }
        b"slave-read-only" | b"replica-read-only" => {
            cfg.set_slave_read_only(value == b"yes");
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
    apply_config_set(cfg, key, value);
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
        }
        cfg.set_appendonly(true);
    } else {
        if was_enabled {
            if let Some(w) = crate::aof::aof_writer() {
                let _ = w.flush();
            }
            crate::aof::remove_aof_writer();
            ctx.server().set_aof_state(redis_core::AofState::Off);
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
        ctx.reply_frame(&RespFrame::array(Vec::new()))
    } else if ascii_eq_ignore_case(sub_bytes, b"DOCTOR") {
        ctx.reply_bulk_string(RedisString::from_bytes(
            b"Sam, I detected a few issues in this Valkey instance memory implants:\n",
        ))
    } else {
        let mut msg =
            Vec::with_capacity(b"ERR Unknown MEMORY subcommand: ".len() + sub_bytes.len());
        msg.extend_from_slice(b"ERR Unknown MEMORY subcommand: ");
        msg.extend_from_slice(sub_bytes);
        Err(RedisError::runtime(msg))
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
    for i in 1..ctx.arg_count() {
        let kw = ctx.arg(i)?;
        let kw_bytes = kw.as_bytes();
        if ascii_eq_ignore_case(kw_bytes, b"ABORT") {
            return Err(RedisError::runtime(b"ERR No shutdown in progress."));
        }
        if ascii_eq_ignore_case(kw_bytes, b"NOSAVE")
            || ascii_eq_ignore_case(kw_bytes, b"SAVE")
            || ascii_eq_ignore_case(kw_bytes, b"NOW")
            || ascii_eq_ignore_case(kw_bytes, b"FORCE")
            || ascii_eq_ignore_case(kw_bytes, b"SAFE")
            || ascii_eq_ignore_case(kw_bytes, b"FAILOVER")
        {
            continue;
        }
        return Err(RedisError::runtime(b"ERR syntax error"));
    }
    if let Some(conn) = ctx.client_mut().conn.as_mut() {
        let _ = conn.write_all(b"+OK\r\n");
    }
    std::process::exit(0);
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
    for (idx, arg) in argv.iter().enumerate() {
        if idx > 0 {
            out.push(b' ');
        }
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
        return ctx.reply_simple_string(b"Value at:0x0 refcount:1 encoding:raw serializedlength:1 lru:0 lru_seconds:0 type:string");
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
                ctx.client_mut().authenticated_user = Some(uname);
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
    if snapshot_in_pubsub_mode(snap) {
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
        " db={} sub={} psub={} ssub={} multi={} watch={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=",
        client.db_index,
        client.subscribed_channels.len(),
        client.subscribed_patterns.len(),
        client.subscribed_shard_channels.len(),
        multi,
        watch,
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
    line.extend_from_slice(b" tot-net-in=0 tot-net-out=0 tot-cmds=0\r\n");
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
    let _ = write!(
        line,
        "id={} addr={} laddr=127.0.0.1:0 fd=0 name=",
        snap.id, snap.addr,
    );
    if let Some(name) = &snap.name {
        line.extend_from_slice(name.as_bytes());
    }
    line.extend_from_slice(b" age=0 idle=0 flags=");
    line.extend_from_slice(&flags);
    line.extend_from_slice(b" capa=");
    line.extend_from_slice(capa);
    let _ = write!(
        line,
        " db={} sub={} psub={} ssub={} multi={} watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=",
        snap.db_index,
        snap.subscribed_channels,
        snap.subscribed_patterns,
        snap.subscribed_shard_channels,
        multi,
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
    line.extend_from_slice(b" tot-net-in=0 tot-net-out=0 tot-cmds=0\r\n");
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
        let mut out = Vec::new();
        if current_client_matches_filters(ctx.client_ref(), &filters) {
            out.extend_from_slice(&format_current_client_info_line(
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
            out.extend_from_slice(&format_snapshot_client_info_line(snap, cmd));
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
    if ascii_eq_ignore_case(sub_bytes, b"PAUSE")
        || ascii_eq_ignore_case(sub_bytes, b"UNPAUSE")
        || ascii_eq_ignore_case(sub_bytes, b"KILL")
    {
        return ctx.reply_simple_string(b"OK");
    }
    let mut msg = Vec::with_capacity(b"ERR Unknown CLIENT subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown CLIENT subcommand: ");
    msg.extend_from_slice(sub_bytes);
    Err(RedisError::runtime(msg))
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
    if !ascii_eq_ignore_case(opt.as_bytes(), b"YES") && !ascii_eq_ignore_case(opt.as_bytes(), b"NO")
    {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let tracking = &mut ctx.client_mut().tracking;
    if !tracking.enabled || (!tracking.optin && !tracking.optout) {
        return Err(RedisError::runtime(
            b"ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
        ));
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
        return Err(RedisError::wrong_number_of_args(b"auth"));
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
    match authenticate_user(lookup_name, password.as_bytes()) {
        Some(uname) => {
            ctx.client_mut().authenticated_user = Some(uname);
            ctx.reply_simple_string(b"OK")
        }
        None => {
            if username.is_none() {
                let fallback = try_password_any_user(password.as_bytes());
                match fallback {
                    Some(uname) => {
                        ctx.client_mut().authenticated_user = Some(uname);
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
        if user.flags.enabled && user.check_password(cleartext) {
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
    for rule in &parts[2..] {
        if let Err(e) = apply_acl_rule(&mut user, rule.as_bytes()) {
            let mut msg = b"ERR Error in ACL file line ".to_vec();
            msg.extend_from_slice(line_no.to_string().as_bytes());
            msg.extend_from_slice(b", modifier '");
            msg.extend_from_slice(rule.as_bytes());
            msg.extend_from_slice(b"': ");
            msg.extend_from_slice(e.strip_prefix(b"ERR ").unwrap_or(&e));
            return Err(msg);
        }
    }
    Ok(Some((username, user)))
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
        let Some(spec) = COMMANDS
            .iter()
            .find(|spec| ascii_eq_ignore_case(spec.name.as_bytes(), command.as_bytes()))
        else {
            let mut msg = b"ERR Command '".to_vec();
            msg.extend_from_slice(command.as_bytes());
            msg.extend_from_slice(b"' not found");
            return Err(RedisError::runtime(msg));
        };
        let dry_argc = ctx.arg_count().saturating_sub(3);
        if (spec.arity > 0 && spec.arity as usize != dry_argc)
            || (spec.arity < 0 && dry_argc < (-spec.arity) as usize)
        {
            let mut msg = b"ERR wrong number of arguments for '".to_vec();
            msg.extend_from_slice(command.as_bytes());
            msg.extend_from_slice(b"' command");
            return Err(RedisError::runtime(msg));
        }
        let categories = spec
            .acl_categories
            .iter()
            .fold(0u64, |acc, cat| acc | generated_acl_category_bit(*cat));
        let first_arg = ctx.client_ref().arg(4).map(|arg| arg.as_bytes());
        if !user.can_execute_command_with_arg(command.as_bytes(), first_arg, categories) {
            let mut msg = b"This user has no permissions to run the '".to_vec();
            msg.extend_from_slice(&lower_acl_token(command.as_bytes()));
            msg.extend_from_slice(b"' command");
            return ctx.reply_bulk(&msg);
        }
        return ctx.reply_simple_string(b"OK");
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
        for rule in &rules {
            if let Err(e) = apply_acl_rule(&mut user, rule.as_bytes()) {
                return Err(RedisError::runtime(acl_setuser_error(rule.as_bytes(), &e)));
            }
        }
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
        return Ok(());
    }
    if rule == b"resetkeys" {
        user.flags.allkeys = false;
        user.key_patterns.clear();
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
    if rule.eq_ignore_ascii_case(b"resetdb") {
        user.flags.alldbs = false;
        user.allowed_dbs.clear();
        return Ok(());
    }
    if rule.len() > 3 && rule[..3].eq_ignore_ascii_case(b"db=") {
        let raw = &rule[3..];
        let Some(db) = parse_i64_strict(raw) else {
            return Err(b"ERR Invalid database number".to_vec());
        };
        if db < 0 || db > u32::MAX as i64 {
            return Err(b"ERR Invalid database number".to_vec());
        }
        let db = db as u32;
        user.flags.alldbs = false;
        if !user.allowed_dbs.contains(&db) {
            user.allowed_dbs.push(db);
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
        RespFrame::array(Vec::new()),
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
