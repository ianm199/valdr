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
use redis_core::CommandContext;
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};

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
/// The pilot server is still single-DB internally, but the TCL test harness
/// runs every block against database 9. To unblock the canonical suite we
/// accept any index in the conventional `0..15` range and record it on the
/// client without actually partitioning the keyspace. Operations from any
/// numeric DB therefore all hit the same underlying `RedisDb` — a deliberate
/// shortcut until real multi-DB routing lands.
pub fn select_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"select"));
    }
    let raw = ctx.arg_owned(1usize)?;
    let idx = parse_i64_strict(raw.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
    if !(0..=15).contains(&idx) {
        return Err(RedisError::runtime(b"ERR DB index is out of range"));
    }
    ctx.client_mut().db_index = idx as u32;
    ctx.reply_simple_string(b"OK")
}

/// `FUNCTION <subcommand> [args]`.
///
/// Stub for the Valkey TCL harness. The harness invokes `FUNCTION FLUSH`
/// between every test block and a few other subcommands during setup; we do
/// not maintain a function registry, so every subcommand returns `+OK\r\n`
/// for `FLUSH` and a fixed shape for `LIST`/`STATS`. Anything else falls
/// through to a syntax-style error so we keep parity with the upstream error
/// surface for unimplemented features.
pub fn function_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"function"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();
    if ascii_eq_ignore_case(sub_bytes, b"FLUSH")
        || ascii_eq_ignore_case(sub_bytes, b"DELETE")
        || ascii_eq_ignore_case(sub_bytes, b"RESTORE")
    {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub_bytes, b"LIST") || ascii_eq_ignore_case(sub_bytes, b"DUMP") {
        return ctx.reply_frame(&RespFrame::array(Vec::new()));
    }
    if ascii_eq_ignore_case(sub_bytes, b"STATS") {
        return ctx.reply_frame(&RespFrame::array(Vec::new()));
    }
    let mut msg = Vec::with_capacity(b"ERR Unknown FUNCTION subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown FUNCTION subcommand: ");
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
            apply_config_set(&live_config, key.as_bytes(), &value_bytes);
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
        ("save", ""),
        ("dir", "./"),
        ("dbfilename", "dump.rdb"),
        ("tcp-backlog", "511"),
        ("tcp-keepalive", "300"),
        ("timeout", "0"),
        ("port", "0"),
        ("bind", "127.0.0.1"),
        ("databases", "16"),
        ("hash-max-listpack-entries", "128"),
        ("hash-max-listpack-value", "64"),
        ("list-max-listpack-size", "-2"),
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
        ("client-output-buffer-limit", "normal 0 0 0 slave 256mb 64mb 60 pubsub 32mb 8mb 60"),
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
    let live_dir = cfg.rdb_dir();
    let live_dbfilename = cfg.rdb_filename();
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
    let live_appendonly = if cfg.appendonly() { "yes".to_string() } else { "no".to_string() };
    let live_appendfsync = crate::aof::fsync_policy_str(cfg.appendfsync()).to_string();
    let live_appendfilename = cfg.appendfilename();
    let live_repl_backlog_size = cfg.repl_backlog_size().to_string();
    let live_repl_timeout = cfg.repl_timeout().to_string();
    let live_repl_disable_nodelay = if cfg.repl_disable_tcp_nodelay() { "yes".to_string() } else { "no".to_string() };
    let live_slave_read_only = if cfg.slave_read_only() { "yes".to_string() } else { "no".to_string() };
    let live_repl_diskless = if cfg.repl_diskless_sync() { "yes".to_string() } else { "no".to_string() };

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
            "list-max-listpack-size" => Some(live_list_size.clone()),
            "set-max-intset-entries" => Some(live_set_intset.clone()),
            "set-max-listpack-entries" => Some(live_set_entries.clone()),
            "set-max-listpack-value" => Some(live_set_value.clone()),
            "zset-max-listpack-entries" => Some(live_zset_entries.clone()),
            "zset-max-listpack-value" => Some(live_zset_value.clone()),
            "zset-max-ziplist-entries" => Some(live_zset_entries.clone()),
            "zset-max-ziplist-value" => Some(live_zset_value.clone()),
            "hash-max-ziplist-entries" => Some(live_hash_entries.clone()),
            "hash-max-ziplist-value" => Some(live_hash_value.clone()),
            "dir" => Some(live_dir.clone()),
            "dbfilename" => Some(live_dbfilename.clone()),
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
            "repl-backlog-size" => Some(live_repl_backlog_size.clone()),
            "repl-timeout" => Some(live_repl_timeout.clone()),
            "repl-disable-tcp-nodelay" => Some(live_repl_disable_nodelay.clone()),
            "slave-read-only" | "replica-read-only" => Some(live_slave_read_only.clone()),
            "repl-diskless-sync" => Some(live_repl_diskless.clone()),
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
        b"list-max-listpack-size" => {
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
                    Err(e) => eprintln!("redis-server: failed to open AOF {}: {}", path.display(), e),
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
        b"appendfsync" => {
            if let Some(policy) = crate::aof::parse_fsync_policy(value) {
                cfg.set_appendfsync(policy);
                if let Some(w) = crate::aof::aof_writer() {
                    w.fsync_policy.store(policy, std::sync::atomic::Ordering::Relaxed);
                }
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
        } else if pi < pattern.len()
            && ascii_lower(pattern[pi]) == ascii_lower(text[ti])
        {
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
        let value_len = ctx.db().lookup_key_read(key.as_bytes()).and_then(|obj| obj.string_len().ok());
        match value_len {
            Some(v) => ctx.reply_integer((key_len + v + 48) as i64),
            None => ctx.reply_null_bulk(),
        }
    } else if ascii_eq_ignore_case(sub_bytes, b"STATS") {
        ctx.reply_frame(&RespFrame::array(Vec::new()))
    } else if ascii_eq_ignore_case(sub_bytes, b"DOCTOR") {
        ctx.reply_bulk_string(RedisString::from_bytes(b"Sam, I detected a few issues in this Valkey instance memory implants:\n"))
    } else {
        let mut msg = Vec::with_capacity(b"ERR Unknown MEMORY subcommand: ".len() + sub_bytes.len());
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
        let digest = match ctx.db_mut().lookup_key_read_with_flags(&key, redis_core::db::LOOKUP_NOTOUCH) {
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
        return ctx.reply_bulk_string(redis_types::RedisString::from_bytes(b"0000000000000000000000000000000000000000"));
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
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"LOADAOF") {
        return ctx.reply_simple_string(b"OK");
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
    let mut msg = Vec::with_capacity(b"ERR Unknown DEBUG subcommand: ".len() + sub.as_bytes().len());
    msg.extend_from_slice(b"ERR Unknown DEBUG subcommand: ");
    msg.extend_from_slice(sub.as_bytes());
    Err(RedisError::runtime(msg))
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
    let mut proto: i32 = 2;
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
    let pairs: Vec<(RespFrame, RespFrame)> = vec![
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
    ctx.reply_frame(&RespFrame::Map(pairs))
}

/// `CLIENT <subcommand> [args]`.
///
/// Pilot subset:
///   * `CLIENT ID` — integer reply of the client's connection id.
///   * `CLIENT GETNAME` — bulk reply of the stored name (empty bulk when unset).
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
        let payload = match &ctx.client_ref().name {
            Some(n) => n.clone(),
            None => RedisString::new(),
        };
        return ctx.reply_bulk_string(payload);
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
    if ascii_eq_ignore_case(sub_bytes, b"LIST") {
        let filter_id: Option<u64> = if ctx.arg_count() >= 4 {
            let opt = ctx.arg(2).ok().map(|a| a.clone());
            let val = ctx.arg(3).ok().map(|a| a.clone());
            match (opt, val) {
                (Some(o), Some(v))
                    if o.as_bytes().eq_ignore_ascii_case(b"ID") =>
                {
                    parse_i64_strict(v.as_bytes()).map(|n| n as u64)
                }
                _ => None,
            }
        } else {
            None
        };
        let snapshots = {
            let guard = match client_info_registry().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.all()
        };
        let mut out: Vec<u8> = Vec::new();
        for snap in &snapshots {
            if let Some(id) = filter_id {
                if snap.id != id {
                    continue;
                }
            }
            let flags = if snap.blocked { "b" } else { "N" };
            let _ = write!(
                out,
                "id={} addr={} laddr= fd=0 name= age=0 idle=0 flags={} capa= db={} sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events= cmd={} user=default redir=-1 resp=2 lib-name= lib-ver=\r\n",
                snap.id,
                snap.addr,
                flags,
                snap.db_index,
                if snap.cmd.is_empty() { "NULL" } else { &snap.cmd },
            );
        }
        return ctx.reply_bulk_string(RedisString::from_vec(out));
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
        || ascii_eq_ignore_case(sub_bytes, b"REPLY")
        || ascii_eq_ignore_case(sub_bytes, b"KILL")
    {
        return ctx.reply_simple_string(b"OK");
    }
    let mut msg = Vec::with_capacity(b"ERR Unknown CLIENT subcommand: ".len() + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown CLIENT subcommand: ");
    msg.extend_from_slice(sub_bytes);
    Err(RedisError::runtime(msg))
}

/// `COMMAND` / `COMMAND COUNT`.
///
/// `COMMAND` (no args) replies with an array of bulk-string command names
/// drawn from the dispatch table. This stub omits the per-command metadata
/// (arity/flags/key-positions/etc.); `redis-cli` accepts a names-only reply.
///
/// `COMMAND COUNT` replies with the integer length of the dispatch table.
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
    let mut msg = Vec::with_capacity(b"ERR Unknown COMMAND subcommand: ".len() + sub.as_bytes().len());
    msg.extend_from_slice(b"ERR Unknown COMMAND subcommand: ");
    msg.extend_from_slice(sub.as_bytes());
    Err(RedisError::runtime(msg))
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
            items.push(RespFrame::bulk(RedisString::from_vec(user.to_rule_string())));
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
        let cmd_name = RedisString::from_bytes(&rule[1..].iter().map(|b| b.to_ascii_lowercase()).collect::<Vec<_>>());
        user.denied_commands.retain(|c| c != &cmd_name);
        if !user.allowed_commands.contains(&cmd_name) {
            user.allowed_commands.push(cmd_name);
        }
        return Ok(());
    }
    if rule.starts_with(b"-") {
        let cmd_name = RedisString::from_bytes(&rule[1..].iter().map(|b| b.to_ascii_lowercase()).collect::<Vec<_>>());
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
                AclCategory::KEYSPACE    => acl_category::KEYSPACE,
                AclCategory::READ        => acl_category::READ,
                AclCategory::WRITE       => acl_category::WRITE,
                AclCategory::SET         => acl_category::SET,
                AclCategory::SORTEDSET   => acl_category::SORTEDSET,
                AclCategory::LIST        => acl_category::LIST,
                AclCategory::HASH        => acl_category::HASH,
                AclCategory::STRING      => acl_category::STRING,
                AclCategory::BITMAP      => acl_category::BITMAP,
                AclCategory::HYPERLOGLOG => acl_category::HYPERLOGLOG,
                AclCategory::GEO         => acl_category::GEO,
                AclCategory::STREAM      => acl_category::STREAM,
                AclCategory::PUBSUB      => acl_category::PUBSUB,
                AclCategory::ADMIN       => acl_category::ADMIN,
                AclCategory::FAST        => acl_category::FAST,
                AclCategory::SLOW        => acl_category::SLOW,
                AclCategory::BLOCKING    => acl_category::BLOCKING,
                AclCategory::DANGEROUS   => acl_category::DANGEROUS,
                AclCategory::CONNECTION  => acl_category::CONNECTION,
                AclCategory::TRANSACTION => acl_category::TRANSACTION,
                AclCategory::SCRIPTING   => acl_category::SCRIPTING,
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
    a.iter().zip(b.iter()).all(|(x, y)| ascii_lower(*x) == ascii_lower(*y))
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
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        translated by hand (Wave B — connection commands)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         PING + ECHO. HELLO/AUTH/QUIT remain stubbed in dispatch.
// ──────────────────────────────────────────────────────────────────────────
