//! AUTO-EXTRACTED from connection.rs by refactor/file-structure-splits.
//! Module-level doc lives in lib.rs.
#![allow(unused_imports, dead_code, unused_variables, unused_mut)]

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;
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
use redis_core::live_config::{LiveConfig, MaxmemoryPolicyCode, ReplDisklessLoadMode};
use redis_core::metrics::{
    record_acl_access_denied_auth, record_blocked_command_rejected, record_error_reply,
    server_metrics,
};
use redis_core::networking::{
    client_matches_ip_filter, validate_client_capa_filter, validate_client_flag_filter,
};
use redis_core::notify::{keyspace_events_string_to_flags, NOTIFY_EVICTED};
use redis_core::object::object_compute_size;
use redis_core::{CommandContext, PersistenceStatus, RedisDb};
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};
use serde_json::Value;

use crate::client_limits::*;
use crate::connection::*;
use crate::generated::{GeneratedCommandSpec, COMMANDS};
use crate::listeners::*;
use crate::live_config_handle;
use crate::shutdown_signals::*;

pub static CONFIG_OVERRIDES: OnceLock<Mutex<HashMap<Vec<u8>, String>>> = OnceLock::new();
pub static CONFIG_FILE_PATH: OnceLock<Mutex<Option<String>>> = OnceLock::new();

pub fn default_config_pairs() -> &'static [(&'static str, &'static str)] {
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
        ("aof-timestamp-enabled", "no"),
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
        ("unixsocket", ""),
        ("unixsocketperm", "0"),
        ("unixsocketgroup", ""),
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
        ("tls-auth-clients", "yes"),
        ("tls-protocols", ""),
        ("tls-ciphers", ""),
        ("tls-ciphersuites", ""),
        ("tls-prefer-server-ciphers", "no"),
        ("repl-backlog-size", "1048576"),
        ("repl-backlog-ttl", "3600"),
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
        ("lua-time-limit", "5000"),
        ("busy-reply-threshold", "5000"),
        ("repl-diskless-sync", "yes"),
        ("repl-diskless-load", "disabled"),
    ]
}

/// Build the full CONFIG GET parameter list reading every live value
/// the supplied `LiveConfig`. Static pairs in `default_config_pairs` are
/// reproduced verbatim for keys with no behavioural backing.
pub fn config_pairs_with_dynamic(cfg: &Arc<LiveConfig>) -> Vec<(String, String)> {
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
    let live_tls_protocols = cfg.tls_protocols();
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
    let live_aof_timestamp_enabled = yes_no(cfg.aof_timestamp_enabled()).to_string();
    let live_auto_aof_rewrite_percentage = cfg.auto_aof_rewrite_percentage().to_string();
    let live_auto_aof_rewrite_min_size = cfg.auto_aof_rewrite_min_size().to_string();
    let live_repl_backlog_size = cfg.repl_backlog_size().to_string();
    let live_repl_backlog_ttl = cfg.repl_backlog_ttl().to_string();
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
    let live_lua_time_limit = cfg.lua_time_limit_ms().to_string();
    let live_repl_diskless = if cfg.repl_diskless_sync() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let live_dual_channel_replication = if cfg.dual_channel_replication_enabled() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let live_repl_diskless_load = cfg.repl_diskless_load().as_config_str().to_string();
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
            "tls-protocols" => Some(live_tls_protocols.clone()),
            "appendonly" => Some(live_appendonly.clone()),
            "appendfsync" => Some(live_appendfsync.clone()),
            "appendfilename" => Some(live_appendfilename.clone()),
            "appenddirname" => Some(live_appenddirname.clone()),
            "aof-load-truncated" => Some(live_aof_load_truncated.clone()),
            "aof-use-rdb-preamble" => Some(live_aof_use_rdb_preamble.clone()),
            "aof-timestamp-enabled" => Some(live_aof_timestamp_enabled.clone()),
            "auto-aof-rewrite-percentage" => Some(live_auto_aof_rewrite_percentage.clone()),
            "auto-aof-rewrite-min-size" => Some(live_auto_aof_rewrite_min_size.clone()),
            "repl-backlog-size" => Some(live_repl_backlog_size.clone()),
            "repl-backlog-ttl" => Some(live_repl_backlog_ttl.clone()),
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
            "lua-time-limit" | "busy-reply-threshold" => Some(live_lua_time_limit.clone()),
            "repl-diskless-sync" => Some(live_repl_diskless.clone()),
            "dual-channel-replication-enabled" => Some(live_dual_channel_replication.clone()),
            "repl-diskless-load" => Some(live_repl_diskless_load.clone()),
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

pub fn config_overrides() -> &'static Mutex<HashMap<Vec<u8>, String>> {
    CONFIG_OVERRIDES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn config_file_path() -> &'static Mutex<Option<String>> {
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

pub fn normalize_config_key(key: &[u8]) -> Vec<u8> {
    key.iter().map(|b| b.to_ascii_lowercase()).collect()
}

pub fn has_glob_meta(pattern: &[u8]) -> bool {
    pattern
        .iter()
        .any(|b| matches!(*b, b'*' | b'?' | b'[' | b']'))
}

pub fn config_override_or_default(key: &[u8], default_value: &str) -> String {
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

pub fn config_override_value(key: &[u8]) -> Option<String> {
    let normalized = normalize_config_key(key);
    let guard = match config_overrides().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.get(&normalized).cloned()
}

pub fn remember_config_override(key: &[u8], value: &[u8]) {
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

pub fn rewrite_config_file(cfg: &Arc<LiveConfig>) -> RedisResult<()> {
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

pub fn config_value_for_key(cfg: &Arc<LiveConfig>, key: &[u8]) -> Option<String> {
    let normalized = normalize_config_key(key);
    if ascii_eq_ignore_case(&normalized, b"key-load-delay") {
        return Some(config_override_or_default(&normalized, "0"));
    }
    config_pairs_with_dynamic(cfg)
        .into_iter()
        .find(|(name, _)| ascii_eq_ignore_case(name.as_bytes(), &normalized))
        .map(|(_, value)| value)
}

pub fn rollback_config_updates(cfg: &Arc<LiveConfig>, backups: &[(Vec<u8>, String)]) {
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

pub fn config_value_is_live_only(key: &[u8]) -> bool {
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
        b"tls-protocols",
        b"tls-ciphers",
        b"tls-ciphersuites",
        b"tls-prefer-server-ciphers",
        b"appendonly",
        b"appendfsync",
        b"appendfilename",
        b"appenddirname",
        b"aof-load-truncated",
        b"aof-use-rdb-preamble",
        b"aof-timestamp-enabled",
        b"auto-aof-rewrite-percentage",
        b"auto-aof-rewrite-min-size",
        b"repl-backlog-size",
        b"repl-backlog-ttl",
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
        b"lua-time-limit",
        b"busy-reply-threshold",
        b"repl-diskless-sync",
        b"repl-diskless-load",
        b"rdb-version-check",
        b"client-output-buffer-limit",
    ];
    LIVE_KEYS.contains(&key)
}

pub fn validate_config_set_pair(key: &[u8], value: &[u8]) -> RedisResult<()> {
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
    if ascii_eq_ignore_case(key, b"repl-backlog-size")
        && parse_memsize(value)
            .and_then(|n| usize::try_from(n).ok())
            .is_none()
    {
        return Err(RedisError::runtime(
            b"ERR CONFIG SET failed (possibly related to argument 'repl-backlog-size')",
        ));
    }
    if ascii_eq_ignore_case(key, b"repl-backlog-ttl") && parse_usize_strict(value).is_none() {
        return Err(RedisError::runtime(
            b"ERR CONFIG SET failed (possibly related to argument 'repl-backlog-ttl')",
        ));
    }
    if ascii_eq_ignore_case(key, b"repl-diskless-load")
        && ReplDisklessLoadMode::parse(value).is_none()
    {
        return Err(RedisError::runtime(
            b"ERR CONFIG SET failed (possibly related to argument 'repl-diskless-load')",
        ));
    }
    if ascii_eq_ignore_case(key, b"dual-channel-replication-enabled")
        && parse_yes_no(value).is_none()
    {
        return Err(RedisError::runtime(
            b"ERR CONFIG SET failed (possibly related to argument 'dual-channel-replication-enabled')",
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
/// Unknown keys are silently ignored so the TCL test harness can issue
/// arbitrary `CONFIG SET` calls without aborting. Values that cannot be
/// parsed are also silently ignored — the existing value remains in effect.
pub fn apply_config_set(cfg: &Arc<LiveConfig>, key: &[u8], value: &[u8]) {
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
            redis_core::tls::notify_tls_port_set(cfg.tls_port());
        }
        b"tls-key-file" => {
            if value.is_empty() {
                cfg.set_tls_key_file(None);
            } else if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_tls_key_file(Some(std::path::PathBuf::from(s)));
            }
            redis_core::tls::notify_tls_port_set(cfg.tls_port());
        }
        b"tls-ca-cert-file" => {
            if value.is_empty() {
                cfg.set_tls_ca_cert_file(None);
            } else if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_tls_ca_cert_file(Some(std::path::PathBuf::from(s)));
            }
            redis_core::tls::notify_tls_port_set(cfg.tls_port());
        }
        b"tls-auth-clients" => {
            let mode = match value {
                b"yes" => 1u8,
                b"optional" => 2u8,
                _ => 0u8,
            };
            cfg.set_tls_auth_clients(mode);
            redis_core::tls::notify_tls_port_set(cfg.tls_port());
        }
        b"tls-protocols" => {
            if let Ok(s) = std::str::from_utf8(value) {
                cfg.set_tls_protocols(s.to_string());
                redis_core::tls::notify_tls_port_set(cfg.tls_port());
            }
        }
        b"tls-ciphers" | b"tls-ciphersuites" | b"tls-prefer-server-ciphers" => {
            // Accepted for upstream-config compatibility but inert: rustls
            // refuses CBC suites and always prefers server ciphers, so these
            // OpenSSL knobs have no effect. Documented as a deliberate
            // security-upgrade divergence on the site.
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
        b"aof-timestamp-enabled" => {
            if ascii_eq_ignore_case(value, b"yes") {
                cfg.set_aof_timestamp_enabled(true);
                crate::aof::set_aof_timestamp_enabled(true);
            } else if ascii_eq_ignore_case(value, b"no") {
                cfg.set_aof_timestamp_enabled(false);
                crate::aof::set_aof_timestamp_enabled(false);
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
                if let Ok(size) = usize::try_from(n) {
                    redis_core::replication::global_replication_state()
                        .resize_backlog_preserving_history(size);
                }
            }
        }
        b"repl-backlog-ttl" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_repl_backlog_ttl(n as u64);
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
        b"lua-time-limit" | b"busy-reply-threshold" => {
            if let Some(n) = parse_usize_strict(value) {
                cfg.set_lua_time_limit_ms(n as u64);
            }
        }
        b"repl-diskless-sync" => {
            cfg.set_repl_diskless_sync(value == b"yes");
        }
        b"dual-channel-replication-enabled" => {
            if let Some(enabled) = parse_yes_no(value) {
                cfg.set_dual_channel_replication_enabled(enabled);
            }
        }
        b"repl-diskless-load" => {
            if let Some(mode) = ReplDisklessLoadMode::parse(value) {
                cfg.set_repl_diskless_load(mode);
            }
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
/// `b`, `k`/`kb`, `m`/`mb`, `g`/`gb` (case-insensitive). Suffixes follow
/// upstream Valkey convention of base-2 multipliers (1k = 1024). Returns
/// `None` on any parse failure so callers can preserve the prior value.
pub fn parse_memsize(bytes: &[u8]) -> Option<u64> {
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

pub fn parse_maxmemory_clients(bytes: &[u8]) -> Option<i64> {
    if let Some(raw) = bytes.strip_suffix(b"%") {
        let pct = parse_usize_strict(raw)?;
        return Some(-(pct as i64));
    }
    parse_memsize(bytes).and_then(|n| i64::try_from(n).ok())
}

pub fn render_maxmemory_clients(value: i64) -> String {
    if value < 0 {
        format!("{}%", value.saturating_abs())
    } else {
        value.to_string()
    }
}

/// Parses a non-negative integer from ASCII decimal bytes. Returns `None` if
/// the bytes do not represent a valid non-negative integer.
pub fn parse_usize_strict(bytes: &[u8]) -> Option<usize> {
    let n = parse_i64_strict(bytes)?;
    if n < 0 {
        return None;
    }
    Some(n as usize)
}

pub fn apply_config_set_for_context(
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

pub fn apply_port_config_set(value: &[u8]) -> RedisResult<()> {
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

pub fn valid_bind_config_value(value: &[u8]) -> bool {
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

pub fn apply_bind_config_set(value: &[u8]) -> RedisResult<()> {
    if let Some(hook) = TCP_BIND_SET_HOOK.get() {
        let listeners = hook(value, tcp_port_config()).map_err(|err| {
            let text = String::from_utf8_lossy(&err);
            if text.starts_with("ERR ") {
                RedisError::runtime(text.as_bytes())
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

pub fn apply_client_output_buffer_limit_config_set(value: &[u8]) -> RedisResult<()> {
    let value_str = std::str::from_utf8(value)
        .map_err(|_| RedisError::runtime(b"ERR Wrong number of arguments"))?;
    let tokens: Vec<&str> = value_str.split_whitespace().collect();
    if tokens.is_empty() || !tokens.len().is_multiple_of(4) {
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
    store_client_obuf_limit_snapshot(next);
    redis_core::replication::global_replication_state()
        .set_replica_output_buffer_hard_limit(next.replica.hard);
    Ok(())
}

pub fn apply_appendonly_config_set(
    ctx: &mut CommandContext<'_>,
    cfg: &Arc<LiveConfig>,
    value: &[u8],
) -> RedisResult<()> {
    if !crate::aof::flush_thread_aof_batch_for_lifecycle(
        &ctx.server().persistence,
        "AOF appendonly config barrier flush failed",
    ) {
        return Err(RedisError::runtime(
            b"ERR CONFIG SET appendonly failed while flushing pending AOF writes",
        ));
    }
    let enabled = ascii_eq_ignore_case(value, b"yes");
    let was_enabled = cfg.appendonly();
    if enabled {
        if !was_enabled {
            cfg.set_appendonly(true);
            if ctx.client_ref().flag_deny_blocking()
                || ctx.server().in_exec()
                || ctx.server().rdb_child_pid() != 0
            {
                schedule_initial_aof_enable(ctx);
            } else {
                ctx.server()
                    .set_aof_state(redis_core::AofState::WaitRewrite);
                ctx.server().persistence.set_aof_rewrite_scheduled(false);
                ctx.server().persistence.set_aof_rewrite_in_progress(true);
                redis_core::metrics::record_total_fork();
                if let Err(err) = install_initial_aof_writer(ctx, cfg) {
                    cfg.set_appendonly(false);
                    ctx.server().set_aof_state(redis_core::AofState::Off);
                    ctx.server().persistence.set_aof_rewrite_in_progress(false);
                    ctx.server().persistence.set_aof_rewrite_scheduled(false);
                    ctx.server().persistence.set_aof_last_bgrewrite_status(
                        redis_core::persistence::PersistenceStatus::Err,
                    );
                    return Err(err);
                }
                ctx.server().set_aof_state(redis_core::AofState::On);
                ctx.server().persistence.set_aof_rewrite_scheduled(false);
                clear_initial_aof_rewrite_visibility(ctx.server_arc());
            }
        } else {
            cfg.set_appendonly(true);
        }
    } else {
        if was_enabled {
            if let Some(w) = crate::aof::aof_writer() {
                let _ = w.flush();
            }
            if ctx.server().persistence.aof_rewrite_in_progress() {
                crate::connection::log_server_notice(
                    "Killing AOF child because appendonly was disabled",
                );
                ctx.server().persistence.set_aof_rewrite_in_progress(false);
                ctx.server()
                    .persistence
                    .set_aof_last_bgrewrite_status(redis_core::persistence::PersistenceStatus::Err);
            }
            ctx.server().persistence.set_aof_rewrite_scheduled(false);
            crate::aof::remove_aof_writer();
            ctx.server().set_aof_state(redis_core::AofState::Off);
            crate::replication::unblock_waitaof_local_disabled();
        }
        cfg.set_appendonly(false);
    }
    Ok(())
}

fn install_initial_aof_writer(ctx: &mut CommandContext<'_>, cfg: &LiveConfig) -> RedisResult<()> {
    let snapshot = ctx.snapshot_all_dbs()?;
    let dbs = snapshot.to_dbs();
    let dir = cfg.rdb_dir();
    let filename = cfg.appendfilename();
    let dirname = cfg.appenddirname();
    let policy = cfg.appendfsync();
    let (writer, base_size, current_size) = crate::aof::open_manifest_current_incr_writer(
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
    ctx.server().persistence.set_aof_base_size(base_size);
    ctx.server().persistence.set_aof_current_size(current_size);
    ctx.server()
        .persistence
        .set_aof_last_bgrewrite_status(redis_core::persistence::PersistenceStatus::Ok);
    let repl = redis_core::replication::global_replication_state();
    crate::aof::force_current_writer_fsynced_repl_offset(repl.master_offset());
    Ok(())
}

fn schedule_initial_aof_enable(ctx: &mut CommandContext<'_>) {
    ctx.server()
        .set_aof_state(redis_core::AofState::WaitRewrite);
    ctx.server().persistence.set_aof_rewrite_in_progress(false);
    ctx.server().persistence.set_aof_rewrite_scheduled(true);
    ctx.server()
        .persistence
        .set_aof_last_bgrewrite_status(redis_core::persistence::PersistenceStatus::Ok);

    if ctx.server().rdb_child_pid() != 0 {
        crate::connection::log_server_notice(
            "AOF was enabled but there is already another background operation. An AOF background was scheduled to start when possible.",
        );
    } else {
        crate::connection::log_server_notice(
            "AOF was enabled during a transaction. An AOF background was scheduled to start when possible.",
        );
    }
}

fn clear_initial_aof_rewrite_visibility(server: Arc<redis_core::RedisServer>) {
    let _ = thread::Builder::new()
        .name("initial-aof-rewrite-clear".to_string())
        .spawn(move || {
            thread::sleep(Duration::from_millis(750));
            if !server.persistence.aof_rewrite_scheduled() {
                server.persistence.set_aof_rewrite_in_progress(false);
            }
        });
}

pub fn maybe_start_scheduled_initial_aof(ctx: &mut CommandContext<'_>) -> RedisResult<bool> {
    if !ctx.live_config().appendonly()
        || ctx.server().aof_state() != redis_core::AofState::WaitRewrite
        || !ctx.server().persistence.aof_rewrite_scheduled()
        || ctx.server().persistence.aof_rewrite_in_progress()
        || ctx.server().rdb_child_pid() != 0
        || ctx.client_ref().flag_deny_blocking()
        || ctx.server().in_exec()
    {
        return Ok(false);
    }

    ctx.server().persistence.set_aof_rewrite_in_progress(true);
    redis_core::metrics::record_total_fork();
    let cfg = Arc::clone(&ctx.server().live_config);
    if let Err(err) = install_initial_aof_writer(ctx, &cfg) {
        ctx.server().persistence.set_aof_rewrite_in_progress(false);
        ctx.server()
            .persistence
            .set_aof_last_bgrewrite_status(redis_core::persistence::PersistenceStatus::Err);
        return Err(err);
    }
    ctx.server().set_aof_state(redis_core::AofState::On);
    ctx.server().persistence.set_aof_rewrite_scheduled(false);
    ctx.server().persistence.set_aof_rewrite_in_progress(false);
    Ok(true)
}

pub fn wait_for_scheduled_initial_aof(
    ctx: &mut CommandContext<'_>,
    timeout_ms: i64,
) -> RedisResult<bool> {
    if !ctx.live_config().appendonly()
        || ctx.server().aof_state() != redis_core::AofState::WaitRewrite
        || !ctx.server().persistence.aof_rewrite_scheduled()
    {
        return Ok(false);
    }
    if timeout_ms > 0 {
        let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms as u64);
        while ctx.server().rdb_child_pid() != 0 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
    }
    maybe_start_scheduled_initial_aof(ctx)
}

/// Glob-style ASCII matcher used by CONFIG GET. Supports `*` and `?` only;
/// brackets are treated as literal characters. Comparison is case-insensitive
/// to match the canonical CONFIG behaviour, where `config get MaxMemory`
/// returns the same pair as `config get maxmemory`.
pub fn glob_match_ascii_ci(pattern: &[u8], text: &[u8]) -> bool {
    glob_match_inner(pattern, text)
}

pub fn glob_match_inner(pattern: &[u8], text: &[u8]) -> bool {
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

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        extracted from connection.rs (refactor/file-structure-splits)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Extracted from the 7,184-LOC god-file. Re-exports in
//                  connection.rs keep external paths working.
// ──────────────────────────────────────────────────────────────────────────
