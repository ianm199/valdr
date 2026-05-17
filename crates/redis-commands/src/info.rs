//! Server introspection: INFO and LASTSAVE.
//!
//! Pilot-stage implementations sized for client compatibility — most numbers
//! are best-effort stubs, but the reply shapes match what real clients (and
//! the Valkey TCL harness) expect.
//!
//! INFO is intentionally excluded from the wire-diff oracle corpus because
//! most fields (pid, port, uptime, used_memory, command stats) differ between
//! processes by definition. The implementation here exists so clients that
//! call INFO do not crash on a null reply.

use std::io::Write;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult, RedisString};

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

/// `INFO [section]`.
///
/// Returns the canonical Redis multi-section text blob as a bulk string.
/// The default reply (no section) emits every section; a section argument
/// such as `server`, `clients`, `memory`, `stats`, `replication`, `cpu`, or
/// `keyspace` emits only that one. Unknown section names produce a near-empty
/// reply to match real Redis behaviour. Numeric fields are best-effort stubs.
pub fn info_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    let section: Option<RedisString> = if argc >= 2 {
        Some(ctx.arg_owned(1usize)?)
    } else {
        None
    };

    let dbsize = ctx.db().size();
    let pid = std::process::id();
    let uptime = now_unix_seconds().saturating_sub(server_start_time());
    let want = |name: &[u8]| -> bool {
        match &section {
            None => true,
            Some(s) => {
                ascii_eq_ignore_case(s.as_bytes(), name)
                    || ascii_eq_ignore_case(s.as_bytes(), b"all")
                    || ascii_eq_ignore_case(s.as_bytes(), b"default")
                    || ascii_eq_ignore_case(s.as_bytes(), b"everything")
            }
        }
    };

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
        let _ = writeln!(buf, "tcp_port:0\r");
        let _ = writeln!(buf, "uptime_in_seconds:{}\r", uptime);
        let _ = writeln!(buf, "uptime_in_days:{}\r", uptime / 86400);
        let _ = writeln!(buf, "hz:10\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"clients") {
        let _ = writeln!(buf, "# Clients\r");
        let _ = writeln!(buf, "connected_clients:1\r");
        let _ = writeln!(buf, "maxclients:10000\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"memory") {
        let _ = writeln!(buf, "# Memory\r");
        let _ = writeln!(buf, "used_memory:0\r");
        let _ = writeln!(buf, "used_memory_human:0B\r");
        let _ = writeln!(buf, "used_memory_rss:0\r");
        let _ = writeln!(buf, "used_memory_peak:0\r");
        let _ = writeln!(buf, "maxmemory:0\r");
        let _ = writeln!(buf, "maxmemory_policy:noeviction\r");
        let _ = writeln!(buf, "mem_fragmentation_ratio:1.00\r");
        let _ = writeln!(buf, "mem_allocator:rust-std\r");
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
        let _ = writeln!(buf, "total_connections_received:1\r");
        let _ = writeln!(buf, "total_commands_processed:0\r");
        let _ = writeln!(buf, "instantaneous_ops_per_sec:0\r");
        let _ = writeln!(buf, "total_net_input_bytes:0\r");
        let _ = writeln!(buf, "total_net_output_bytes:0\r");
        let _ = writeln!(buf, "rejected_connections:0\r");
        let _ = writeln!(buf, "expired_keys:0\r");
        let _ = writeln!(buf, "evicted_keys:0\r");
        let _ = writeln!(buf, "keyspace_hits:0\r");
        let _ = writeln!(buf, "keyspace_misses:0\r");
        let _ = writeln!(buf, "pubsub_channels:0\r");
        let _ = writeln!(buf, "pubsub_patterns:0\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"replication") {
        let _ = writeln!(buf, "# Replication\r");
        let _ = writeln!(buf, "role:master\r");
        let _ = writeln!(buf, "connected_slaves:0\r");
        let _ = writeln!(buf, "master_replid:0000000000000000000000000000000000000000\r");
        let _ = writeln!(buf, "master_repl_offset:0\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"cpu") {
        let _ = writeln!(buf, "# CPU\r");
        let _ = writeln!(buf, "used_cpu_sys:0.0\r");
        let _ = writeln!(buf, "used_cpu_user:0.0\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"commandstats") {
        let _ = writeln!(buf, "# Commandstats\r");
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
            let _ = writeln!(buf, "db0:keys={},expires=0,avg_ttl=0\r", dbsize);
        }
        let _ = writeln!(buf, "\r");
    }

    ctx.reply_bulk_string(RedisString::from_vec(buf))
}

/// `LASTSAVE`.
///
/// Returns the unix timestamp of the last successful RDB save. The pilot
/// server has no persistence, so this is reported as the process start time.
/// Real clients (including the TCL test suite's bg-save assertion helpers)
/// only check that the value is non-zero and monotonic.
pub fn lastsave_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"lastsave"));
    }
    ctx.reply_integer(server_start_time() as i64)
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
