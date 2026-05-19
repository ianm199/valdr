//! Server introspection: INFO and LASTSAVE.
//!
//! INFO is intentionally excluded from the wire-diff oracle corpus because
//! most fields (pid, port, uptime, used_memory, command stats) differ between
//! processes by definition. The implementation here exists so clients that
//! call INFO do not crash on a null reply.
//!
//! Memory accounting uses the estimator approach: `used_memory_estimated =
//! dict.len() * 80 + sum_of_string_bytes`. This is declared in
//! `docs/PATH_TO_DEF3.md` §Eviction as the approved heuristic for Def 3.

use std::io::Write;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::memory::approximate_memory_used;
use redis_core::metrics::{rss_bytes, server_metrics};
use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult, RedisString};

use crate::connection::get_max_clients;

/// Process start time (unix seconds), captured on first call.
///
/// Used by both `INFO server` (for `uptime_in_seconds`) and `LASTSAVE`.
/// Because the pilot server does not persist to disk, the "last save" time
/// is reported as the process start time — every client treats this as a
/// monotonic value rather than a real RDB timestamp.
fn server_start_time() -> u64 {
    static START: OnceLock<u64> = OnceLock::new();
    *START.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    })
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}


fn format_human_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2}M", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2}K", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}

/// `INFO [section]`.
///
/// Returns the canonical Redis multi-section text blob as a bulk string.
/// The default reply (no section) emits every section; a section argument
/// such as `server`, `clients`, `memory`, `stats`, `replication`, `cpu`, or
/// `keyspace` emits only that one. Unknown section names produce a near-empty
/// reply to match real Redis behaviour.
pub fn info_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();

    let mut sections: Vec<RedisString> = Vec::new();
    for i in 1..argc {
        sections.push(ctx.arg_owned(i)?);
    }

    let has_all = sections.iter().any(|s| {
        ascii_eq_ignore_case(s.as_bytes(), b"all")
            || ascii_eq_ignore_case(s.as_bytes(), b"everything")
    });
    let has_default = sections.is_empty()
        || sections
            .iter()
            .any(|s| ascii_eq_ignore_case(s.as_bytes(), b"default"));

    let dbsize = ctx.db().size();
    let expires_count = ctx.db().expires_count();
    let pid = std::process::id();
    let uptime = now_unix_seconds().saturating_sub(server_start_time());
    let metrics = server_metrics();
    let connected_clients = metrics.connected_clients.load(Ordering::Relaxed);
    let total_connections = metrics.total_connections_received.load(Ordering::Relaxed);
    let total_commands = metrics.total_commands_processed.load(Ordering::Relaxed);
    let hits = metrics.keyspace_hits.load(Ordering::Relaxed);
    let misses = metrics.keyspace_misses.load(Ordering::Relaxed);
    let rejected = metrics.rejected_connections.load(Ordering::Relaxed);
    let expired_keys = metrics.expired_keys.load(Ordering::Relaxed);
    let evicted_keys = metrics.evicted_keys.load(Ordering::Relaxed);
    let active_time_us = metrics.active_time_main_thread_us.load(Ordering::Relaxed);
    let maxclients = get_max_clients();

    let want = |name: &[u8]| -> bool {
        if has_all || has_default {
            return true;
        }
        sections
            .iter()
            .any(|s| ascii_eq_ignore_case(s.as_bytes(), name))
    };

    let want_commandstats = has_all
        || sections
            .iter()
            .any(|s| ascii_eq_ignore_case(s.as_bytes(), b"commandstats"));

    let mut buf: Vec<u8> = Vec::with_capacity(2048);

    if want(b"server") {
        let _ = writeln!(buf, "# Server\r");
        let _ = writeln!(buf, "redis_version:7.0.0\r");
        let _ = writeln!(buf, "redis_git_sha1:00000000\r");
        let _ = writeln!(buf, "redis_git_dirty:0\r");
        let _ = writeln!(buf, "redis_build_id:redis-rs-port\r");
        let _ = writeln!(buf, "redis_mode:standalone\r");
        let _ = writeln!(buf, "os:{}\r", std::env::consts::OS);
        let _ = writeln!(buf, "arch_bits:64\r");
        let _ = writeln!(buf, "process_id:{}\r", pid);
        let _ = writeln!(buf, "tcp_port:{}\r", metrics.tcp_port.load(Ordering::Relaxed));
        let _ = writeln!(buf, "uptime_in_seconds:{}\r", uptime);
        let _ = writeln!(buf, "uptime_in_days:{}\r", uptime / 86400);
        let _ = writeln!(buf, "hz:10\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"clients") {
        let blocked = match redis_core::blocked_keys::blocked_keys_index().lock() {
            Ok(g) => g.len(),
            Err(p) => p.into_inner().len(),
        };
        let _ = writeln!(buf, "# Clients\r");
        let _ = writeln!(buf, "connected_clients:{}\r", connected_clients);
        let _ = writeln!(buf, "maxclients:{}\r", maxclients);
        let _ = writeln!(buf, "blocked_clients:{}\r", blocked);
        let _ = writeln!(buf, "tracking_clients:0\r");
        let _ = writeln!(buf, "clients_in_timeout_table:0\r");
        let _ = writeln!(buf, "watching_clients:0\r");
        let _ = writeln!(buf, "client_recent_max_input_buffer:0\r");
        let _ = writeln!(buf, "cluster_connections:0\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"memory") {
        let used_memory = approximate_memory_used(ctx.db());
        let used_memory_human = format_human_bytes(used_memory);
        let peak = metrics.max_clients_seen.load(Ordering::Relaxed);
        let (rss, rss_source) = match rss_bytes() {
            Some(r) => (r, "proc"),
            None => (used_memory, "estimated"),
        };
        let _ = writeln!(buf, "# Memory\r");
        let _ = writeln!(buf, "used_memory:{}\r", used_memory);
        let _ = writeln!(buf, "used_memory_human:{}\r", used_memory_human);
        let _ = writeln!(buf, "used_memory_rss:{}\r", rss);
        let _ = writeln!(buf, "used_memory_rss_human:{}\r", format_human_bytes(rss));
        let _ = writeln!(buf, "used_memory_rss_source:{}\r", rss_source);
        let _ = writeln!(buf, "used_memory_peak:{}\r", used_memory);
        let _ = writeln!(buf, "used_memory_peak_human:{}\r", format_human_bytes(used_memory));
        let _ = writeln!(buf, "used_memory_estimated:true\r");
        let _ = writeln!(buf, "total_system_memory:0\r");
        let live_maxmemory = ctx.live_config().maxmemory();
        let live_policy = ctx.live_config().maxmemory_policy();
        let _ = writeln!(buf, "maxmemory:{}\r", live_maxmemory);
        let _ = writeln!(buf, "maxmemory_policy:{}\r", live_policy.as_config_str());
        let _ = writeln!(buf, "mem_fragmentation_ratio:1.00\r");
        let _ = writeln!(buf, "mem_allocator:rust-std\r");
        let _ = writeln!(buf, "max_clients_seen:{}\r", peak);
        let _ = writeln!(buf, "\r");
    }
    if want(b"persistence") {
        let _ = writeln!(buf, "# Persistence\r");
        let _ = writeln!(buf, "loading:0\r");
        let _ = writeln!(buf, "rdb_changes_since_last_save:0\r");
        let _ = writeln!(buf, "rdb_bgsave_in_progress:0\r");
        let _ = writeln!(buf, "rdb_last_save_time:{}\r", server_start_time());
        let _ = writeln!(buf, "rdb_last_bgsave_status:ok\r");
        let _ = writeln!(buf, "aof_enabled:0\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"stats") {
        let _ = writeln!(buf, "# Stats\r");
        let _ = writeln!(buf, "total_connections_received:{}\r", total_connections);
        let _ = writeln!(buf, "total_commands_processed:{}\r", total_commands);
        let _ = writeln!(buf, "instantaneous_ops_per_sec:0\r");
        let _ = writeln!(buf, "total_net_input_bytes:0\r");
        let _ = writeln!(buf, "total_net_output_bytes:0\r");
        let _ = writeln!(buf, "rejected_connections:{}\r", rejected);
        let _ = writeln!(buf, "expired_keys:{}\r", expired_keys);
        let _ = writeln!(buf, "evicted_keys:{}\r", evicted_keys);
        let _ = writeln!(buf, "keyspace_hits:{}\r", hits);
        let _ = writeln!(buf, "keyspace_misses:{}\r", misses);
        let _ = writeln!(buf, "pubsub_channels:0\r");
        let _ = writeln!(buf, "pubsub_patterns:0\r");
        let _ = writeln!(buf, "used_active_time_main_thread:{}\r", active_time_us);
        let _ = writeln!(buf, "\r");
    }
    if want(b"replication") {
        let repl = redis_core::replication::global_replication_state();
        let role = if repl.is_replica() { "slave" } else { "master" };
        let runid_str = std::str::from_utf8(repl.runid())
            .unwrap_or("0000000000000000000000000000000000000000");
        let (backlog_first, master_offset, backlog_histlen, backlog_size) =
            repl.backlog_snapshot();
        let replicas = repl.replicas_snapshot();
        let connected = replicas.len();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let _ = writeln!(buf, "# Replication\r");
        let _ = writeln!(buf, "role:{}\r", role);
        let _ = writeln!(buf, "connected_slaves:{}\r", connected);
        for (idx, (_cid, state, port, offset, last_ack_ms)) in replicas.iter().enumerate() {
            let lag = if *last_ack_ms == 0 {
                0
            } else {
                (now_ms - last_ack_ms) / 1000
            };
            let _ = writeln!(
                buf,
                "slave{}:ip=?,port={},state={},offset={},lag={}\r",
                idx, port, state, offset, lag
            );
        }
        let _ = writeln!(buf, "master_replid:{}\r", runid_str);
        let _ = writeln!(buf, "master_repl_offset:{}\r", master_offset);
        let backlog_active = if backlog_size > 0 && backlog_histlen > 0 { 1 } else { 0 };
        let _ = writeln!(buf, "repl_backlog_active:{}\r", backlog_active);
        let _ = writeln!(buf, "repl_backlog_size:{}\r", backlog_size);
        let _ = writeln!(buf, "repl_backlog_first_byte_offset:{}\r", backlog_first);
        let _ = writeln!(buf, "repl_backlog_histlen:{}\r", backlog_histlen);
        let _ = writeln!(buf, "\r");
    }
    if want(b"cpu") {
        let _ = writeln!(buf, "# CPU\r");
        let _ = writeln!(buf, "used_cpu_sys:0.0\r");
        let _ = writeln!(buf, "used_cpu_user:0.0\r");
        let _ = writeln!(buf, "\r");
    }
    if want_commandstats {
        // TODO(architect): replace stub with real per-command call/usec counters
        // once dispatch.rs timing wrap (OV-2) accumulates into a shared HashMap.
        let _ = writeln!(buf, "# Commandstats\r");
        let _ = writeln!(
            buf,
            "cmdstat_info:calls={},usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=0\r",
            total_commands
        );
        let _ = writeln!(buf, "\r");
    }
    if want(b"cluster") {
        let _ = writeln!(buf, "# Cluster\r");
        let _ = writeln!(buf, "cluster_enabled:0\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"keyspace") {
        let _ = writeln!(buf, "# Keyspace\r");
        if dbsize > 0 {
            let _ = writeln!(
                buf,
                "db0:keys={},expires={},avg_ttl=0\r",
                dbsize, expires_count
            );
        }
        let _ = writeln!(buf, "\r");
    }

    ctx.reply_bulk_string(RedisString::from_vec(buf))
}

/// `LASTSAVE`.
///
/// Returns the Unix timestamp (seconds) of the last successful RDB save,
/// or the process start time when no save has occurred in this session.
pub fn lastsave_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"lastsave"));
    }
    let last = ctx.server().live_config.last_save_unix();
    if last == 0 {
        ctx.reply_integer(server_start_time() as i64)
    } else {
        ctx.reply_integer(last)
    }
}

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
