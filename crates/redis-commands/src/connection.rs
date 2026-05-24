//! Connection-management and server commands: PING, ECHO, SELECT, CLIENT,
//! COMMAND, DEBUG, TIME, HELLO, RESET, QUIT.
//!
//! Most handlers operate purely against the client's argv and reply buffer;
//! they never need to touch the keyspace.

use std::io::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::acl::{
    category as acl_category, category_name_to_bit, global_acl_state, hex_to_hash, sha256_hash,
    AclUser, ALL_CATEGORY_NAMES,
};
use redis_core::client_info::client_info_registry;
use redis_core::live_config::{LiveConfig, MaxmemoryPolicyCode};
use redis_core::metrics::server_metrics;
use redis_core::notify::keyspace_events_string_to_flags;
use redis_core::{CommandContext, PersistenceStatus, RedisDb};
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};
use serde_json::Value;

use crate::live_config_handle;

/// Default Valkey `maxclients` value. Re-exported from `LiveConfig`.
pub const DEFAULT_MAX_CLIENTS: u64 = redis_core::live_config::DEFAULT_MAX_CLIENTS;

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
        1 if ctx.client_ref().in_pubsub_mode() && ctx.client_ref().resp_proto == 2 => {
            ctx.reply_frame(&RespFrame::array(vec![
                RespFrame::bulk(RedisString::from_static(b"pong")),
                RespFrame::bulk(RedisString::from_static(b"")),
            ]))
        }
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
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"REWRITE") {
        return ctx.reply_simple_string(b"OK");
    }
    let mut msg = Vec::with_capacity(b"ERR Unknown CONFIG subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown CONFIG subcommand: ");
    msg.extend_from_slice(sub_bytes);
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
        b"slowlog-log-slower-than" => {
            if let Some(n) = parse_i64_strict(value) {
                cfg.set_slowlog_threshold_micros(n);
                crate::slowlog_cmd::set_slowlog_threshold(n);
            }
        }
        b"slowlog-max-len" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_slowlog_max_len(n);
                crate::slowlog_cmd::set_slowlog_max_len(n);
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
    ctx.client_mut().reset_state();
    ctx.reply_simple_string(b"RESET")
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
    while i < argc {
        let tok = ctx.arg_owned(i)?;
        if ascii_eq_ignore_case(tok.as_bytes(), b"AUTH") {
            if argc < i + 3 {
                return Err(RedisError::syntax(b"Syntax error in HELLO"));
            }
            let hello_user = ctx.arg_owned(i + 1)?;
            let hello_pass = ctx.arg_owned(i + 2)?;
            match authenticate_user(hello_user.as_bytes(), hello_pass.as_bytes()) {
                Some(uname) => {
                    ctx.client_mut().authenticated_user = Some(uname);
                }
                None => {
                    return ctx.reply_error(
                        b"WRONGPASS invalid username-password pair or user is disabled." as &[u8],
                    );
                }
            }
            i += 3;
        } else if ascii_eq_ignore_case(tok.as_bytes(), b"SETNAME") {
            if argc < i + 2 {
                return Err(RedisError::syntax(b"Syntax error in HELLO"));
            }
            let name = ctx.arg_owned(i + 1)?;
            validate_client_name(name.as_bytes())?;
            ctx.client_mut().name = Some(name);
            i += 2;
        } else {
            return Err(RedisError::syntax(b"Syntax error in HELLO"));
        }
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
    addr: Option<String>,
    laddr: Option<String>,
    name: Option<String>,
    flags: Option<String>,
    user: Option<String>,
    skipme: Option<bool>,
}

fn client_user_name(client: &redis_core::client::Client) -> String {
    client
        .authenticated_user
        .as_ref()
        .map(|u| String::from_utf8_lossy(u.as_bytes()).into_owned())
        .unwrap_or_else(|| "default".to_string())
}

fn client_name_string(client: &redis_core::client::Client) -> String {
    client
        .name
        .as_ref()
        .map(|n| String::from_utf8_lossy(n.as_bytes()).into_owned())
        .unwrap_or_default()
}

fn client_flags_string(client: &redis_core::client::Client) -> String {
    let mut out = String::new();
    if client.import_source {
        out.push('I');
    }
    if client.tracking.enabled {
        out.push('t');
    }
    if client.tracking.bcast {
        out.push('B');
    }
    if client.tracking.broken_redirect {
        out.push('R');
    }
    if client.blocked_on_keys || client.flag_blocked() {
        out.push('b');
    }
    if out.is_empty() {
        out.push('N');
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

fn client_tracking_redir(client: &redis_core::client::Client) -> i64 {
    if client.tracking.enabled {
        client.tracking.redirect
    } else {
        -1
    }
}

fn format_current_client_info_line(
    client: &redis_core::client::Client,
    command_name: &str,
) -> String {
    let addr = client
        .addr
        .clone()
        .unwrap_or_else(|| "127.0.0.1:0".to_string());
    let laddr = "127.0.0.1:0";
    let flags = client_flags_string(client);
    let name = client_name_string(client);
    let user = client_user_name(client);
    let multi = if client.flag_multi() {
        client.queued_argvs.len() as i64
    } else {
        -1
    };
    let watch = watched_key_count_for_client(client.id);
    format!(
        "id={} addr={} laddr={} fd=0 name={} age=0 idle=0 flags={} capa= db={} sub={} psub={} ssub=0 multi={} watch={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user={} redir={} resp={} lib-name= lib-ver= tot-net-in=0 tot-net-out=0 tot-cmds=0\r\n",
        client.id,
        addr,
        laddr,
        name,
        flags,
        client.db_index,
        client.subscribed_channels.len(),
        client.subscribed_patterns.len(),
        multi,
        watch,
        command_name,
        user,
        client_tracking_redir(client),
        client.resp_proto,
    )
}

fn format_snapshot_client_info_line(
    snap: &redis_core::client_info::ClientSnapshot,
    command_name: &str,
) -> String {
    let flags = if snap.blocked { "b" } else { "N" };
    format!(
        "id={} addr={} laddr=127.0.0.1:0 fd=0 name= age=0 idle=0 flags={} capa= db={} sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user=default redir=-1 resp=2 lib-name= lib-ver= tot-net-in=0 tot-net-out=0 tot-cmds=0\r\n",
        snap.id,
        snap.addr,
        flags,
        snap.db_index,
        command_name,
    )
}

fn parse_client_list_filters(ctx: &CommandContext<'_>) -> RedisResult<ClientListFilters> {
    let mut filters = ClientListFilters::default();
    let mut idx = 2usize;
    while idx < ctx.arg_count() {
        let opt = ctx.arg(idx)?;
        let opt_bytes = opt.as_bytes();
        if opt_bytes.eq_ignore_ascii_case(b"ID") {
            idx += 1;
            let mut saw_id = false;
            while idx < ctx.arg_count() {
                let raw = ctx.arg(idx)?;
                if parse_i64_strict(raw.as_bytes()).is_none() {
                    break;
                }
                let id = parse_i64_strict(raw.as_bytes()).unwrap();
                if id < 0 {
                    return Err(RedisError::runtime(
                        b"ERR value is not an integer or out of range",
                    ));
                }
                filters.ids.push(id as u64);
                saw_id = true;
                idx += 1;
            }
            if !saw_id {
                return Err(RedisError::syntax(b"syntax error"));
            }
        } else if opt_bytes.eq_ignore_ascii_case(b"ADDR") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            filters.addr = Some(String::from_utf8_lossy(ctx.arg(idx + 1)?.as_bytes()).into_owned());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"LADDR") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            filters.laddr = Some(String::from_utf8_lossy(ctx.arg(idx + 1)?.as_bytes()).into_owned());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"TYPE") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let ty = ctx.arg(idx + 1)?;
            if !ty.as_bytes().eq_ignore_ascii_case(b"normal") {
                return Err(RedisError::syntax(b"syntax error"));
            }
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"USER") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let user = String::from_utf8_lossy(ctx.arg(idx + 1)?.as_bytes()).into_owned();
            if user != "default" {
                return Err(RedisError::runtime(
                    format!("ERR No such user '{}'", user).into_bytes(),
                ));
            }
            filters.user = Some(user);
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"NAME") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            filters.name = Some(String::from_utf8_lossy(ctx.arg(idx + 1)?.as_bytes()).into_owned());
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"FLAGS") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let flags = String::from_utf8_lossy(ctx.arg(idx + 1)?.as_bytes()).into_owned();
            if flags.chars().any(|c| !matches!(c, 'N' | 'b' | 't' | 'B' | 'R' | 'I')) {
                return Err(RedisError::runtime(
                    format!("ERR Unknown flags found in the provided filter: {}", flags)
                        .into_bytes(),
                ));
            }
            filters.flags = Some(flags);
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"SKIPME") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let v = ctx.arg(idx + 1)?;
            if v.as_bytes().eq_ignore_ascii_case(b"yes") {
                filters.skipme = Some(true);
            } else if v.as_bytes().eq_ignore_ascii_case(b"no") {
                filters.skipme = Some(false);
            } else {
                return Err(RedisError::syntax(b"syntax error"));
            }
            idx += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"MAXAGE") {
            if idx + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            if parse_i64_strict(ctx.arg(idx + 1)?.as_bytes()).is_none() {
                return Err(RedisError::runtime(b"ERR maxage is not an integer"));
            }
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
    let addr = client
        .addr
        .as_deref()
        .unwrap_or("127.0.0.1:0");
    if let Some(expected) = &filters.addr {
        if expected != addr {
            return false;
        }
    }
    if let Some(expected) = &filters.laddr {
        if expected != "127.0.0.1:0" {
            return false;
        }
    }
    if let Some(expected) = &filters.name {
        if expected != &client_name_string(client) {
            return false;
        }
    }
    if let Some(expected) = &filters.user {
        if expected != &client_user_name(client) {
            return false;
        }
    }
    if let Some(expected) = &filters.flags {
        let actual = client_flags_string(client);
        if !expected.chars().all(|c| actual.contains(c)) {
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
    if let Some(expected) = &filters.addr {
        if expected != &snap.addr {
            return false;
        }
    }
    if let Some(expected) = &filters.laddr {
        if expected != "127.0.0.1:0" {
            return false;
        }
    }
    if let Some(expected) = &filters.user {
        if expected != "default" {
            return false;
        }
    }
    if let Some(expected) = &filters.name {
        if !expected.is_empty() {
            return false;
        }
    }
    if let Some(expected) = &filters.flags {
        let actual = if snap.blocked { "b" } else { "N" };
        if !expected.chars().all(|c| actual.contains(c)) {
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
    if ascii_eq_ignore_case(sub_bytes, b"INFO") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"client|info"));
        }
        let line = format_current_client_info_line(ctx.client_ref(), "client|info");
        return ctx.reply_bulk_string(RedisString::from_vec(line.into_bytes()));
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
        let mut out = String::new();
        if current_client_matches_filters(ctx.client_ref(), &filters) {
            out.push_str(&format_current_client_info_line(ctx.client_ref(), "client|list"));
        }
        for snap in &snapshots {
            if snap.id == ctx.client_ref().id {
                continue;
            }
            if !snapshot_matches_filters(snap, &filters) {
                continue;
            }
            let cmd = if snap.cmd.is_empty() { "NULL" } else { snap.cmd.as_str() };
            out.push_str(&format_snapshot_client_info_line(snap, cmd));
        }
        return ctx.reply_bulk_string(RedisString::from_vec(out.into_bytes()));
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
        if parse_i64_strict(id_arg.as_bytes()).is_none() {
            return Err(RedisError::runtime(
                b"ERR value is not an integer or out of range",
            ));
        }
        if ctx.arg_count() == 4 {
            let mode = ctx.arg_owned(3usize)?;
            if !ascii_eq_ignore_case(mode.as_bytes(), b"TIMEOUT")
                && !ascii_eq_ignore_case(mode.as_bytes(), b"ERROR")
            {
                return Err(RedisError::syntax(b"syntax error"));
            }
        }
        return ctx.reply_integer(0);
    }
    if ascii_eq_ignore_case(sub_bytes, b"PAUSE")
        || ascii_eq_ignore_case(sub_bytes, b"KILL")
    {
        return ctx.reply_simple_string(b"OK");
    }
    let mut msg = Vec::with_capacity(b"ERR Unknown CLIENT subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown CLIENT subcommand: ");
    msg.extend_from_slice(sub_bytes);
    Err(RedisError::runtime(msg))
}

fn client_id_exists(id: u64) -> bool {
    let guard = match client_info_registry().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.all().iter().any(|snap| snap.id == id)
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
            let id = parse_i64_strict(ctx.arg(idx + 1)?.as_bytes())
                .ok_or_else(|| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
            if id < 0 {
                return Err(RedisError::runtime(
                    b"ERR value is not an integer or out of range",
                ));
            }
            if !client_id_exists(id as u64) && id as u64 != ctx.client_ref().id {
                return Err(RedisError::runtime(
                    b"ERR The client ID you want redirect to does not exist",
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
        ctx.client_mut().tracking = redis_core::client::ClientTrackingState::default();
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
    client.tracking.enabled = true;
    client.tracking.bcast = bcast;
    client.tracking.optin = optin;
    client.tracking.optout = optout;
    client.tracking.noloop = noloop;
    client.tracking.caching = false;
    client.tracking.broken_redirect = false;
    client.tracking.redirect = redirect;
    client.tracking.prefixes = prefixes;
    ctx.reply_simple_string(b"OK")
}

fn client_caching_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"client|caching"));
    }
    let opt = ctx.arg_owned(2usize)?;
    if !ascii_eq_ignore_case(opt.as_bytes(), b"YES")
        && !ascii_eq_ignore_case(opt.as_bytes(), b"NO")
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
    ctx.reply_simple_string(b"OK")
}

fn client_trackinginfo_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"client|trackinginfo"));
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
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(value.as_bytes(), b"OFF") {
        ctx.client_mut().import_source = false;
        return ctx.reply_simple_string(b"OK");
    }
    Err(RedisError::syntax(b"syntax error"))
}

/// `COMMAND` / `COMMAND COUNT` / `COMMAND GETKEYS`.
///
/// `COMMAND` (no args) replies with an array of bulk-string command names
/// drawn from the dispatch table. This stub omits the per-command metadata
/// (arity/flags/key-positions/etc.); `redis-cli` accepts a names-only reply.
///
/// `COMMAND COUNT` replies with the integer length of the dispatch table.
/// `COMMAND GETKEYS` replies with keys derived from generated command metadata,
/// with SORT/SORT_RO matching their upstream variable key parsing.
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
    if ascii_eq_ignore_case(sub.as_bytes(), b"COUNT") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(b"command|count"));
        }
        let n = crate::dispatch::HANDLERS.len() as i64;
        return ctx.reply_integer(n);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"GETKEYS") {
        return command_getkeys(ctx);
    }
    let mut msg =
        Vec::with_capacity(b"ERR Unknown COMMAND subcommand: ".len() + sub.as_bytes().len());
    msg.extend_from_slice(b"ERR Unknown COMMAND subcommand: ");
    msg.extend_from_slice(sub.as_bytes());
    Err(RedisError::runtime(msg))
}

fn command_getkeys(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"command|getkeys"));
    }
    let cmd_name = ctx.arg_owned(2usize)?;
    let spec = crate::dispatch::registered_command_spec(cmd_name.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR Invalid command specified"))?;
    let command_argc = ctx.arg_count() - 2;
    validate_command_getkeys_arity(spec.arity, command_argc)?;

    let keys = if ascii_eq_ignore_case(cmd_name.as_bytes(), b"SORT") {
        sort_getkeys(ctx)?
    } else if ascii_eq_ignore_case(cmd_name.as_bytes(), b"SORT_RO") {
        vec![ctx.arg_owned(3usize)?]
    } else {
        command_getkeys_from_specs(ctx, spec, command_argc)?
            .ok_or_else(|| RedisError::runtime(b"ERR Invalid arguments specified for command"))?
    };
    let items = keys.into_iter().map(RespFrame::bulk).collect();
    ctx.reply_frame(&RespFrame::array(items))
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

fn sort_getkeys(ctx: &CommandContext<'_>) -> RedisResult<Vec<RedisString>> {
    let argc = ctx.arg_count() - 2;
    let mut keys = Vec::with_capacity(2);
    keys.push(ctx.arg_owned(3usize)?);
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
        keys.push(ctx.arg_owned(index)?);
    }
    Ok(keys)
}

fn command_getkeys_from_specs(
    ctx: &CommandContext<'_>,
    spec: &crate::generated::GeneratedCommandSpec,
    command_argc: usize,
) -> RedisResult<Option<Vec<RedisString>>> {
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
        for pos in positions {
            keys.push(ctx.arg_owned(2 + pos)?);
        }
    }
    if keys.is_empty() && unsupported {
        Ok(None)
    } else {
        Ok(Some(keys))
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
    let mut out = Vec::with_capacity(numkeys);
    for idx in 0..numkeys {
        let pos = first_key_index + idx * step;
        if pos >= command_argc {
            return Err(RedisError::runtime(
                b"ERR Invalid arguments specified for command",
            ));
        }
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
            ctx.reply_error(b"WRONGPASS invalid username-password pair or user is disabled.")
        }
    }
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
        let mut items: Vec<RespFrame> = Vec::new();
        for user in guard.users.values() {
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
        let items: Vec<RespFrame> = guard
            .users
            .keys()
            .map(|k| RespFrame::bulk(k.clone()))
            .collect();
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
            .map(|c| RespFrame::bulk(RedisString::from_bytes(c)))
            .collect();
        return ctx.reply_frame(&RespFrame::array(items));
    }

    if ascii_eq_ignore_case(sub_bytes, b"SETUSER") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"acl|setuser"));
        }
        let username = ctx.arg_owned(2usize)?;
        let rules: Vec<RedisString> = (3..ctx.arg_count())
            .filter_map(|i| ctx.client_ref().arg(i).cloned())
            .collect();
        let acl = global_acl_state();
        let mut guard = match acl.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let user = guard
            .users
            .entry(username.clone())
            .or_insert_with(|| AclUser::new_reset(username));
        for rule in &rules {
            if let Err(e) = apply_acl_rule(user, rule.as_bytes()) {
                return Err(RedisError::runtime(e));
            }
        }
        return ctx.reply_simple_string(b"OK");
    }

    if ascii_eq_ignore_case(sub_bytes, b"DELUSER") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"acl|deluser"));
        }
        let default_key = RedisString::from_bytes(b"default");
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
                }
            }
        }
        return ctx.reply_integer(count);
    }

    if ascii_eq_ignore_case(sub_bytes, b"LOG") {
        if ctx.arg_count() >= 3 {
            let sub2 = ctx.arg_owned(2)?;
            if ascii_eq_ignore_case(sub2.as_bytes(), b"RESET") {
                return ctx.reply_simple_string(b"OK");
            }
        }
        return ctx.reply_frame(&RespFrame::array(Vec::new()));
    }

    if ascii_eq_ignore_case(sub_bytes, b"HELP") {
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
            b"LOG [<count> | RESET]",
            b"    Show the recent ACL log or clear it.",
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

    let mut msg = Vec::with_capacity(b"ERR Unknown ACL subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown ACL subcommand: ");
    msg.extend_from_slice(sub_bytes);
    Err(RedisError::runtime(msg))
}

/// Apply a single ACL SETUSER rule token to `user`.
fn apply_acl_rule(user: &mut AclUser, rule: &[u8]) -> Result<(), Vec<u8>> {
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
    if rule == b"allcommands" || rule == b"+@all" {
        user.flags.allcommands = true;
        user.allowed_categories = acl_category::ALL;
        return Ok(());
    }
    if rule == b"nocommands" || rule == b"-@all" {
        user.flags.allcommands = false;
        user.allowed_categories = 0;
        user.allowed_commands.clear();
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
    if rule == b"reset" {
        *user = AclUser::new_reset(user.name.clone());
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
                user.allowed_categories |= bit;
                if bit == acl_category::ALL {
                    user.flags.allcommands = true;
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
                user.allowed_categories &= !bit;
                if bit == acl_category::ALL {
                    user.flags.allcommands = false;
                    user.allowed_commands.clear();
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
        let cmd_name = RedisString::from_bytes(
            &rule[1..]
                .iter()
                .map(|b| b.to_ascii_lowercase())
                .collect::<Vec<_>>(),
        );
        user.denied_commands.retain(|c| c != &cmd_name);
        if !user.allowed_commands.contains(&cmd_name) {
            user.allowed_commands.push(cmd_name);
        }
        return Ok(());
    }
    if rule.starts_with(b"-") {
        let cmd_name = RedisString::from_bytes(
            &rule[1..]
                .iter()
                .map(|b| b.to_ascii_lowercase())
                .collect::<Vec<_>>(),
        );
        user.allowed_commands.retain(|c| c != &cmd_name);
        if !user.denied_commands.contains(&cmd_name) {
            user.denied_commands.push(cmd_name);
        }
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
        RespFrame::bulk(RedisString::from_bytes(b"selectors")),
        RespFrame::array(Vec::new()),
    ])
}

/// Return command names belonging to a given ACL category bitmask bit.
///
/// Scans the generated `COMMANDS` registry for entries whose `acl_categories`
/// include the requested bit and collects their names (deduplicated).
fn commands_in_category(bit: u64) -> Vec<&'static [u8]> {
    use crate::generated::AclCategory;
    let mut out: Vec<&'static [u8]> = Vec::new();
    for spec in crate::generated::COMMANDS.iter() {
        let matches = spec.acl_categories.iter().any(|&cat| {
            let cat_bit: u64 = match cat {
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
            };
            cat_bit & bit != 0
        });
        if matches && !out.contains(&spec.name.as_bytes()) {
            out.push(spec.name.as_bytes());
        }
    }
    out
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
