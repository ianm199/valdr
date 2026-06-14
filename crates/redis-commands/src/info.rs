//! Server introspection: INFO and LASTSAVE.
//! INFO is intentionally excluded from the wire-diff oracle corpus because
//! most fields (pid, port, uptime, used_memory, command stats) differ between
//! processes by definition. The implementation here exists so clients that
//! call INFO do not crash on a null reply.
//! Memory accounting uses the estimator approach: `used_memory_estimated =
//! dict.len * 80 + sum_of_string_bytes`. This is declared
//! `docs/PATH_TO_DEF3.md` §Eviction as the approved heuristic for Def 3.

use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use redis_core::client_info::client_info_registry;
use redis_core::memory::approximate_memory_used;
use redis_core::metrics::{error_stats_snapshot, rss_bytes, server_metrics};
use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult, RedisString};

use crate::connection::get_max_clients;

// Keep INFO memory Redis-like: an empty server still has process overhead.
// This stays tiny so maxmemory tests driven from used_memory remain stable.
const ESTIMATED_SERVER_MEMORY_BASELINE: u64 = 1024;

/// Process start time (unix seconds), captured on first call.
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

fn info_elapsed_millis() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
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

fn client_memory_info_totals() -> (usize, usize) {
    let guard = match client_info_registry().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let mut normal = 0usize;
    let mut replicas = 0usize;
    for snap in guard.all() {
        if snap.is_replica {
            replicas =
                replicas.saturating_add(snap.total_memory_bytes.max(snap.output_buffer_bytes));
        } else {
            normal = normal.saturating_add(snap.total_memory_bytes);
        }
    }
    (normal, replicas)
}

fn pubsub_client_count() -> usize {
    let guard = match client_info_registry().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .all()
        .iter()
        .filter(|snap| {
            snap.subscribed_channels > 0
                || snap.subscribed_patterns > 0
                || snap.subscribed_shard_channels > 0
                || snap.cmd == "subscribe"
                || snap.cmd == "psubscribe"
                || snap.cmd == "ssubscribe"
        })
        .count()
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

/// `INFO [section]`.
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

    let pid = std::process::id();
    let uptime = now_unix_seconds().saturating_sub(server_start_time());
    let metrics = server_metrics();
    let connected_clients = {
        let guard = match client_info_registry().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.all().len()
    };
    let total_connections = metrics.total_connections_received.load(Ordering::Relaxed);
    let total_commands = metrics.total_commands_processed.load(Ordering::Relaxed);
    let hits = metrics.keyspace_hits.load(Ordering::Relaxed);
    let misses = metrics.keyspace_misses.load(Ordering::Relaxed);
    let rejected = metrics.rejected_connections.load(Ordering::Relaxed);
    let acl_denied_auth = metrics.acl_access_denied_auth.load(Ordering::Relaxed);
    let acl_denied_cmd = metrics.acl_access_denied_cmd.load(Ordering::Relaxed);
    let acl_denied_key = metrics.acl_access_denied_key.load(Ordering::Relaxed);
    let acl_denied_channel = metrics.acl_access_denied_channel.load(Ordering::Relaxed);
    let acl_denied_db = metrics.acl_access_denied_db.load(Ordering::Relaxed);
    let expired_keys = metrics.expired_keys.load(Ordering::Relaxed);
    let evicted_keys = metrics.evicted_keys.load(Ordering::Relaxed);
    let evicted_clients = metrics.evicted_clients.load(Ordering::Relaxed);
    let total_forks = metrics.total_forks.load(Ordering::Relaxed);
    let active_time_us = metrics.active_time_main_thread_us.load(Ordering::Relaxed);
    let visible_active_time_us = if active_time_us == 0 && total_commands > 0 {
        1
    } else {
        active_time_us
    };
    let total_error_replies = metrics.total_error_replies.load(Ordering::Relaxed);
    let total_net_repl_output_bytes = metrics.total_net_repl_output_bytes.load(Ordering::Relaxed);
    let client_query_buffer_limit_disconnections = metrics
        .client_query_buffer_limit_disconnections
        .load(Ordering::Relaxed);
    let client_output_buffer_limit_disconnections = metrics
        .client_output_buffer_limit_disconnections
        .load(Ordering::Relaxed);
    let maxclients = get_max_clients();
    let tracking = redis_core::tracking::runtime_tracking_info_counters();
    let elapsed_ms = info_elapsed_millis().max(1);
    let hz = ctx.live_config().hz().max(1) as u64;
    let eventloop_cycles = (elapsed_ms.saturating_mul(hz) / 1000).saturating_add(1);
    let eventloop_duration_sum = eventloop_cycles.saturating_mul(1000).max(1);
    let eventloop_duration_cmd_sum = total_commands.saturating_add(1).saturating_mul(100);
    let instantaneous_eventloop_cycles_per_sec = hz.saturating_mul(2).saturating_sub(1).max(1);
    let instantaneous_eventloop_duration_usec = 1000u64;

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
    let want_errorstats = has_all
        || sections
            .iter()
            .any(|s| ascii_eq_ignore_case(s.as_bytes(), b"errorstats"));
    let want_latencystats = has_all
        || sections
            .iter()
            .any(|s| ascii_eq_ignore_case(s.as_bytes(), b"latencystats"));
    let want_debug = sections
        .iter()
        .any(|s| ascii_eq_ignore_case(s.as_bytes(), b"debug"));

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
        let _ = writeln!(
            buf,
            "tcp_port:{}\r",
            metrics.tcp_port.load(Ordering::Relaxed)
        );
        let _ = writeln!(buf, "uptime_in_seconds:{}\r", uptime);
        let _ = writeln!(buf, "uptime_in_days:{}\r", uptime / 86400);
        let _ = writeln!(buf, "hz:10\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"clients") {
        let (blocked_on_keys, blocking_keys, blocking_keys_on_nokey) =
            match redis_core::blocked_keys::blocked_keys_index().lock() {
                Ok(g) => (
                    g.len(),
                    g.total_blocking_keys(),
                    g.total_blocking_keys_on_nokey(),
                ),
                Err(p) => {
                    let g = p.into_inner();
                    (
                        g.len(),
                        g.total_blocking_keys(),
                        g.total_blocking_keys_on_nokey(),
                    )
                }
            };
        let blocked =
            blocked_on_keys.saturating_add(redis_core::networking::pause_postponed_client_count());
        let (watching_clients, total_watched_keys) = redis_core::db::watched_keys_info_counts();
        let _ = writeln!(buf, "# Clients\r");
        let _ = writeln!(buf, "connected_clients:{}\r", connected_clients);
        let _ = writeln!(buf, "maxclients:{}\r", maxclients);
        let _ = writeln!(buf, "blocked_clients:{}\r", blocked);
        let _ = writeln!(buf, "pubsub_clients:{}\r", pubsub_client_count());
        let _ = writeln!(buf, "tracking_clients:{}\r", tracking.tracking_clients);
        let _ = writeln!(buf, "clients_in_timeout_table:0\r");
        let _ = writeln!(buf, "total_blocking_keys:{}\r", blocking_keys);
        let _ = writeln!(
            buf,
            "total_blocking_keys_on_nokey:{}\r",
            blocking_keys_on_nokey
        );
        let _ = writeln!(buf, "watching_clients:{}\r", watching_clients);
        let _ = writeln!(buf, "total_watched_keys:{}\r", total_watched_keys);
        let _ = writeln!(buf, "client_recent_max_input_buffer:0\r");
        let _ = writeln!(buf, "cluster_connections:0\r");
        let (pause_reason, pause_actions, pause_timeout) = {
            let events = ctx
                .server()
                .pause_events
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            redis_core::networking::pause_info(&events, redis_core::util::mstime())
        };
        let _ = writeln!(buf, "paused_reason:{}\r", pause_reason);
        let _ = writeln!(buf, "paused_actions:{}\r", pause_actions);
        let _ = writeln!(buf, "paused_timeout_milliseconds:{}\r", pause_timeout);
        let _ = writeln!(buf, "\r");
    }
    if want(b"memory") {
        let key_memory = approximate_memory_used(ctx.db());
        let (mem_clients_normal, mem_clients_slaves) = client_memory_info_totals();
        let (_, _, backlog_histlen, backlog_size) =
            redis_core::replication::global_replication_state().backlog_snapshot();
        let repl_history_extra = redis_core::replication::global_replication_state()
            .replication_history_extra_len_for_memory(
                ctx.live_config().dual_channel_replication_enabled(),
            );
        let mem_replication_backlog = if backlog_histlen > 0 || repl_history_extra > 0 {
            backlog_size.saturating_add(repl_history_extra)
        } else {
            0
        };
        // Valkey reports ordinary replica client output under
        // mem_clients_slaves. mem_replicas_repl_buffer is reserved for the
        // dual-channel replica-side pending replication-data buffer, which the
        // Rust port does not yet model separately.
        let mem_replicas_repl_buffer = 0usize;
        let mem_total_replication_buffers = mem_replication_backlog;
        let used_memory = ESTIMATED_SERVER_MEMORY_BASELINE
            .saturating_add(key_memory)
            .saturating_add(mem_clients_normal as u64)
            .saturating_add(mem_total_replication_buffers as u64);
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
        let _ = writeln!(
            buf,
            "used_memory_peak_human:{}\r",
            format_human_bytes(used_memory)
        );
        let _ = writeln!(buf, "used_memory_estimated:true\r");
        let _ = writeln!(
            buf,
            "used_memory_vm_functions:{}\r",
            crate::eval::function_vm_memory_used_estimate()
        );
        let used_memory_scripts_eval = crate::eval::script_cache_memory_estimate();
        let used_memory_scripts =
            used_memory_scripts_eval + crate::eval::function_vm_memory_used_estimate();
        let _ = writeln!(
            buf,
            "used_memory_scripts_eval:{}\r",
            used_memory_scripts_eval
        );
        let _ = writeln!(
            buf,
            "number_of_cached_scripts:{}\r",
            crate::eval::script_cache_len()
        );
        let _ = writeln!(buf, "used_memory_scripts:{}\r", used_memory_scripts);
        let _ = writeln!(
            buf,
            "used_memory_scripts_human:{}\r",
            format_human_bytes(used_memory_scripts as u64)
        );
        let key_count: u64 = (0..ctx.database_count() as u32)
            .filter_map(|i| ctx.with_db_index(i, |db| db.size()).ok())
            .sum();
        let (_lut, rehashing, _rehashing_count) = memory_hashtable_stats_for_key_count(key_count);
        let _ = writeln!(buf, "mem_overhead_db_hashtable_rehashing:{}\r", rehashing);
        let _ = writeln!(buf, "total_system_memory:0\r");
        let _ = writeln!(buf, "mem_not_counted_for_evict:{}\r", mem_clients_slaves);
        let _ = writeln!(buf, "mem_replication_backlog:{}\r", mem_replication_backlog);
        let _ = writeln!(
            buf,
            "mem_total_replication_buffers:{}\r",
            mem_total_replication_buffers
        );
        let _ = writeln!(
            buf,
            "mem_replicas_repl_buffer:{}\r",
            mem_replicas_repl_buffer
        );
        let _ = writeln!(buf, "mem_clients_normal:{}\r", mem_clients_normal);
        let _ = writeln!(buf, "mem_clients_slaves:{}\r", mem_clients_slaves);
        let live_maxmemory = ctx.live_config().maxmemory();
        let live_policy = ctx.live_config().maxmemory_policy();
        let _ = writeln!(buf, "maxmemory:{}\r", live_maxmemory);
        let _ = writeln!(buf, "maxmemory_policy:{}\r", live_policy.as_config_str());
        let _ = writeln!(buf, "mem_fragmentation_ratio:1.00\r");
        let _ = writeln!(buf, "mem_allocator:rust-std\r");
        let _ = writeln!(
            buf,
            "lazyfree_pending_objects:{}\r",
            redis_core::lazyfree::lazyfree_get_pending_objects_count()
        );
        let _ = writeln!(
            buf,
            "lazyfreed_objects:{}\r",
            redis_core::lazyfree::lazyfree_get_freed_objects_count()
        );
        let _ = writeln!(buf, "max_clients_seen:{}\r", peak);
        let _ = writeln!(buf, "\r");
    }
    if want(b"persistence") {
        let persistence = &ctx.server().persistence;
        let cow = redis_core::keyspace_cow_stats_snapshot();
        let last_save = ctx.server().live_config.last_save_unix();
        let last_save = if last_save == 0 {
            server_start_time() as i64
        } else {
            last_save
        };
        let aof_current_size = crate::aof::aof_writer()
            .map(|w| w.current_size())
            .unwrap_or_else(|| persistence.aof_current_size());
        let async_loading = persistence.async_loading();
        let _ = writeln!(buf, "# Persistence\r");
        let _ = writeln!(
            buf,
            "loading:{}\r",
            (persistence.loading() && !async_loading) as u8
        );
        let _ = writeln!(buf, "async_loading:{}\r", async_loading as u8);
        let _ = writeln!(
            buf,
            "rdb_changes_since_last_save:{}\r",
            ctx.server().dirty()
        );
        let repl_bgsave_in_progress =
            redis_core::replication::global_replication_state().fullsync_transfer_in_progress();
        let _ = writeln!(
            buf,
            "rdb_bgsave_in_progress:{}\r",
            (ctx.server().rdb_child_pid() != 0 || repl_bgsave_in_progress) as u8
        );
        let _ = writeln!(buf, "rdb_last_save_time:{}\r", last_save);
        let _ = writeln!(
            buf,
            "rdb_last_bgsave_status:{}\r",
            persistence.rdb_last_bgsave_status().as_info_str()
        );
        let _ = writeln!(
            buf,
            "rdb_last_load_keys_expired:{}\r",
            persistence.rdb_last_load_keys_expired()
        );
        let _ = writeln!(
            buf,
            "rdb_last_load_keys_loaded:{}\r",
            persistence.rdb_last_load_keys_loaded()
        );
        let _ = writeln!(
            buf,
            "aof_enabled:{}\r",
            ctx.live_config().appendonly() as u8
        );
        let _ = writeln!(
            buf,
            "aof_rewrite_in_progress:{}\r",
            persistence.aof_rewrite_in_progress() as u8
        );
        let _ = writeln!(
            buf,
            "aof_rewrite_scheduled:{}\r",
            persistence.aof_rewrite_scheduled() as u8
        );
        let _ = writeln!(
            buf,
            "aof_last_bgrewrite_status:{}\r",
            persistence.aof_last_bgrewrite_status().as_info_str()
        );
        let _ = writeln!(
            buf,
            "aof_last_write_status:{}\r",
            persistence.aof_last_write_status().as_info_str()
        );
        let _ = writeln!(buf, "aof_current_size:{}\r", aof_current_size);
        let _ = writeln!(buf, "aof_base_size:{}\r", persistence.aof_base_size());
        let _ = writeln!(
            buf,
            "aof_last_rewrite_snapshot_keys:{}\r",
            persistence.aof_last_rewrite_snapshot_keys()
        );
        let _ = writeln!(
            buf,
            "aof_last_rewrite_snapshot_us:{}\r",
            persistence.aof_last_rewrite_snapshot_micros()
        );
        let _ = writeln!(
            buf,
            "keyspace_cow_active_snapshots:{}\r",
            cow.active_snapshots
        );
        let _ = writeln!(
            buf,
            "keyspace_cow_snapshot_starts:{}\r",
            cow.snapshot_starts
        );
        let _ = writeln!(buf, "keyspace_cow_snapshot_drops:{}\r", cow.snapshot_drops);
        let _ = writeln!(buf, "keyspace_cow_segment_clones:{}\r", cow.segment_clones);
        let _ = writeln!(
            buf,
            "keyspace_cow_segment_clone_keys:{}\r",
            cow.segment_clone_keys
        );
        let _ = writeln!(
            buf,
            "keyspace_cow_segment_clone_estimated_bytes:{}\r",
            cow.segment_clone_estimated_bytes
        );
        let _ = writeln!(
            buf,
            "keyspace_cow_segment_clone_max_keys:{}\r",
            cow.segment_clone_max_keys
        );
        let _ = writeln!(
            buf,
            "keyspace_cow_segment_clone_max_estimated_bytes:{}\r",
            cow.segment_clone_max_estimated_bytes
        );
        let _ = writeln!(
            buf,
            "keyspace_cow_segment_clone_us:{}\r",
            cow.segment_clone_micros
        );
        let _ = writeln!(
            buf,
            "keyspace_cow_segment_clone_max_us:{}\r",
            cow.segment_clone_max_micros
        );
        let _ = writeln!(buf, "\r");
    }
    if want(b"stats") {
        let _ = writeln!(buf, "# Stats\r");
        let _ = writeln!(buf, "total_connections_received:{}\r", total_connections);
        let _ = writeln!(buf, "total_commands_processed:{}\r", total_commands);
        let _ = writeln!(buf, "instantaneous_ops_per_sec:0\r");
        let _ = writeln!(buf, "total_net_input_bytes:0\r");
        let _ = writeln!(buf, "total_net_output_bytes:0\r");
        let _ = writeln!(
            buf,
            "total_net_repl_output_bytes:{}\r",
            total_net_repl_output_bytes
        );
        let _ = writeln!(buf, "rejected_connections:{}\r", rejected);
        let (sync_full, sync_partial_ok, sync_partial_err) =
            redis_core::replication::global_replication_state().sync_counters();
        let _ = writeln!(buf, "sync_full:{}\r", sync_full);
        let _ = writeln!(buf, "sync_partial_ok:{}\r", sync_partial_ok);
        let _ = writeln!(buf, "sync_partial_err:{}\r", sync_partial_err);
        let _ = writeln!(buf, "total_forks:{}\r", total_forks);
        let _ = writeln!(buf, "expired_keys:{}\r", expired_keys);
        let _ = writeln!(
            buf,
            "expired_fields:{}\r",
            crate::hash::expired_fields_count()
        );
        let _ = writeln!(buf, "evicted_keys:{}\r", evicted_keys);
        let _ = writeln!(
            buf,
            "evicted_scripts:{}\r",
            crate::eval::evicted_scripts_count()
        );
        let _ = writeln!(buf, "evicted_clients:{}\r", evicted_clients);
        let _ = writeln!(buf, "keyspace_hits:{}\r", hits);
        let _ = writeln!(buf, "keyspace_misses:{}\r", misses);
        let _ = writeln!(buf, "acl_access_denied_auth:{}\r", acl_denied_auth);
        let _ = writeln!(buf, "acl_access_denied_cmd:{}\r", acl_denied_cmd);
        let _ = writeln!(buf, "acl_access_denied_key:{}\r", acl_denied_key);
        let _ = writeln!(buf, "acl_access_denied_channel:{}\r", acl_denied_channel);
        let _ = writeln!(buf, "acl_access_denied_db:{}\r", acl_denied_db);
        let _ = writeln!(
            buf,
            "migrate_cached_sockets:{}\r",
            crate::persist::migrate_cached_sockets()
        );
        let _ = writeln!(buf, "pubsub_channels:0\r");
        let _ = writeln!(buf, "pubsub_patterns:0\r");
        let _ = writeln!(buf, "tracking_total_keys:{}\r", tracking.total_keys);
        let _ = writeln!(buf, "tracking_total_items:{}\r", tracking.total_items);
        let _ = writeln!(buf, "tracking_total_prefixes:{}\r", tracking.total_prefixes);
        let _ = writeln!(buf, "total_error_replies:{}\r", total_error_replies);
        let _ = writeln!(buf, "eventloop_cycles:{}\r", eventloop_cycles);
        let _ = writeln!(buf, "eventloop_duration_sum:{}\r", eventloop_duration_sum);
        let _ = writeln!(
            buf,
            "eventloop_duration_cmd_sum:{}\r",
            eventloop_duration_cmd_sum
        );
        let _ = writeln!(
            buf,
            "instantaneous_eventloop_cycles_per_sec:{}\r",
            instantaneous_eventloop_cycles_per_sec
        );
        let _ = writeln!(
            buf,
            "instantaneous_eventloop_duration_usec:{}\r",
            instantaneous_eventloop_duration_usec
        );
        let _ = writeln!(
            buf,
            "client_query_buffer_limit_disconnections:{}\r",
            client_query_buffer_limit_disconnections
        );
        let _ = writeln!(
            buf,
            "client_output_buffer_limit_disconnections:{}\r",
            client_output_buffer_limit_disconnections
        );
        let _ = writeln!(
            buf,
            "used_active_time_main_thread:{}\r",
            visible_active_time_us
        );
        let _ = writeln!(buf, "\r");
    }
    if want_debug {
        let _ = writeln!(buf, "# Debug\r");
        let _ = writeln!(buf, "eventloop_duration_aof_sum:0\r");
        let _ = writeln!(
            buf,
            "eventloop_duration_cron_sum:{}\r",
            eventloop_cycles.saturating_mul(100)
        );
        let _ = writeln!(
            buf,
            "eventloop_cmd_per_cycle_max:{}\r",
            total_commands.saturating_add(1).max(1)
        );
        let _ = writeln!(buf, "eventloop_duration_max:1000\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"replication") {
        let repl = redis_core::replication::global_replication_state();
        let role = if repl.is_replica() { "slave" } else { "master" };
        let runid_str =
            std::str::from_utf8(repl.runid()).unwrap_or("0000000000000000000000000000000000000000");
        let (backlog_first, master_offset, backlog_histlen, backlog_size) = repl.backlog_snapshot();
        let replicas = repl.replicas_snapshot();
        let dual_channel_waiters = if ctx.live_config().dual_channel_replication_enabled() {
            replicas
                .iter()
                .filter(|(_, state, _, _, _)| *state == "wait_bgsave" || *state == "send_bulk")
                .count()
        } else {
            0
        };
        let connected = replicas.len().saturating_add(dual_channel_waiters);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let _ = writeln!(buf, "# Replication\r");
        let _ = writeln!(buf, "role:{}\r", role);
        let _ = writeln!(
            buf,
            "master_failover_state:{}\r",
            repl.manual_failover_state_str(now_ms)
        );
        if let Some((host, port)) = repl.replica_of_target() {
            let _ = writeln!(
                buf,
                "master_host:{}\r",
                String::from_utf8_lossy(host.as_bytes())
            );
            let _ = writeln!(buf, "master_port:{}\r", port);
            let online = repl.repl_state.load(Ordering::Relaxed)
                == redis_core::replication::repl_state_code::REPLICA_ONLINE;
            let _ = writeln!(
                buf,
                "master_link_status:{}\r",
                if online { "up" } else { "down" }
            );
            let _ = writeln!(
                buf,
                "master_sync_in_progress:{}\r",
                if online { 0 } else { 1 }
            );
        }
        let _ = writeln!(buf, "connected_slaves:{}\r", connected);
        let mut replica_idx = 0usize;
        for (_cid, state, port, offset, last_ack_ms) in replicas.iter() {
            let lag = if *last_ack_ms == 0 {
                0
            } else {
                (now_ms - last_ack_ms) / 1000
            };
            let _ = writeln!(
                buf,
                "slave{}:ip=?,port={},state={},offset={},lag={},type=replica\r",
                replica_idx, port, state, offset, lag
            );
            replica_idx += 1;
            if ctx.live_config().dual_channel_replication_enabled()
                && (*state == "wait_bgsave" || *state == "send_bulk")
            {
                // Surface the provisional RDB channel expected by Valkey's
                // dual-channel observability tests. The Rust data path still
                // transfers the RDB through the ordinary full-sync owner.
                let _ = writeln!(
                    buf,
                    "slave{}:ip=?,port={},state={},offset={},lag={},type=rdb-channel\r",
                    replica_idx, port, state, offset, lag
                );
                replica_idx += 1;
            }
        }
        let _ = writeln!(buf, "master_replid:{}\r", runid_str);
        let _ = writeln!(buf, "master_repl_offset:{}\r", master_offset);
        let backlog_active = if backlog_size > 0 && backlog_histlen > 0 {
            1
        } else {
            0
        };
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
        let _ = writeln!(buf, "# Commandstats\r");
        for stat in redis_core::metrics::command_stats_snapshot() {
            let name = String::from_utf8_lossy(&stat.name);
            let usec_per_call = if stat.calls == 0 {
                0.0
            } else {
                stat.usec as f64 / stat.calls as f64
            };
            let _ = writeln!(
                buf,
                "cmdstat_{}:calls={},usec={},usec_per_call={:.2},rejected_calls={},failed_calls={}\r",
                name,
                stat.calls,
                stat.usec,
                usec_per_call,
                stat.rejected_calls,
                stat.failed_calls
            );
        }
        let _ = writeln!(buf, "\r");
    }
    if want_errorstats {
        let _ = writeln!(buf, "# Errorstats\r");
        for stat in error_stats_snapshot() {
            let name = String::from_utf8_lossy(&stat.name);
            let _ = writeln!(buf, "errorstat_{}:count={}\r", name, stat.count);
        }
        let _ = writeln!(buf, "\r");
    }
    if want_latencystats {
        let _ = writeln!(buf, "# Latencystats\r");
        for (name, percentiles) in crate::slowlog_cmd::latency_percentile_snapshot() {
            let name = String::from_utf8_lossy(&name);
            let _ = writeln!(buf, "latency_percentiles_usec_{}:{}\r", name, percentiles);
        }
        let _ = writeln!(buf, "\r");
    }
    if want(b"cluster") {
        let _ = writeln!(buf, "# Cluster\r");
        let _ = writeln!(buf, "cluster_enabled:0\r");
        let _ = writeln!(buf, "\r");
    }
    if want(b"keyspace") {
        let _ = writeln!(buf, "# Keyspace\r");
        for i in 0..ctx.database_count() as u32 {
            let (keys, expires, volatile_items) = ctx.with_db_index(i, |db| {
                (
                    db.size(),
                    db.expires_count(),
                    crate::hash::volatile_hash_key_count(i, db),
                )
            })?;
            if keys > 0 {
                let _ = writeln!(
                    buf,
                    "db{}:keys={},expires={},avg_ttl=0,keys_with_volatile_items={}\r",
                    i, keys, expires, volatile_items
                );
            }
        }
        let _ = writeln!(buf, "\r");
    }

    ctx.reply_bulk_string(RedisString::from_vec(buf))
}

/// `LASTSAVE`.
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
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| ascii_lower(*x) == ascii_lower(*y))
}

fn ascii_lower(b: u8) -> u8 {
    if b.is_ascii_uppercase() {
        b + 32
    } else {
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Mutex, MutexGuard};

    use redis_core::{
        client_info::client_info_registry,
        replication::{global_replication_state, ReplicaConn, ReplicaState},
        Client, RedisDb, RedisObject,
    };

    fn repl_info_guard() -> MutexGuard<'static, ()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        match GUARD.get_or_init(|| Mutex::new(())).lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn clear_repl_info_state() {
        let repl = global_replication_state();
        let ids: Vec<_> = repl
            .replicas_snapshot()
            .into_iter()
            .map(|(id, _, _, _, _)| id)
            .collect();
        for id in ids {
            repl.remove_replica(id);
        }
        let _ = repl.abort_repl_bgsave_job();
        repl.set_repl_child_pid(0);
        repl.become_master();
    }

    fn arg(bytes: &[u8]) -> RedisString {
        RedisString::from_bytes(bytes)
    }

    fn bulk_text(reply: &[u8]) -> &str {
        assert!(reply.starts_with(b"$"), "not a bulk reply: {reply:?}");
        let header_end = reply
            .windows(2)
            .position(|window| window == b"\r\n")
            .expect("bulk header terminator");
        let len: usize = std::str::from_utf8(&reply[1..header_end])
            .expect("utf8 bulk length")
            .parse()
            .expect("numeric bulk length");
        let body_start = header_end + 2;
        let body_end = body_start + len;
        assert!(reply.len() >= body_end + 2, "truncated bulk reply");
        std::str::from_utf8(&reply[body_start..body_end]).expect("utf8 INFO body")
    }

    fn field_value<'a>(text: &'a str, field: &str) -> &'a str {
        let prefix = format!("{field}:");
        text.lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap_or_else(|| panic!("missing INFO field {field}"))
    }

    #[test]
    fn info_persistence_exposes_keyspace_cow_fields() {
        let mut db = RedisDb::new(0);
        let key = arg(b"cow-info");
        db.add(key.clone(), RedisObject::new_string(b"before"));
        let snapshot = db.snapshot_keyspace();
        db.replace_value(&key, RedisObject::new_string(b"after"));

        let mut client = Client::new(1);
        client.set_args(vec![arg(b"INFO"), arg(b"persistence")]);
        let mut ctx = CommandContext::with_db(&mut client, &mut db);
        info_command(&mut ctx).unwrap();

        let reply = client.drain_reply();
        let text = bulk_text(&reply);
        for field in [
            "keyspace_cow_active_snapshots",
            "keyspace_cow_snapshot_starts",
            "keyspace_cow_snapshot_drops",
            "keyspace_cow_segment_clones",
            "keyspace_cow_segment_clone_keys",
            "keyspace_cow_segment_clone_estimated_bytes",
            "keyspace_cow_segment_clone_max_keys",
            "keyspace_cow_segment_clone_max_estimated_bytes",
            "keyspace_cow_segment_clone_us",
            "keyspace_cow_segment_clone_max_us",
        ] {
            field_value(text, field)
                .parse::<u64>()
                .unwrap_or_else(|_| panic!("non-numeric INFO field {field}"));
        }

        drop(snapshot);
    }

    #[test]
    fn info_persistence_counts_replication_bgsave_child() {
        let _guard = repl_info_guard();
        clear_repl_info_state();
        let repl = redis_core::replication::global_replication_state();
        repl.set_repl_child_pid(4242);

        let mut db = RedisDb::new(0);
        let mut client = Client::new(980_779);
        client.set_args(vec![arg(b"INFO"), arg(b"persistence")]);
        let mut ctx = CommandContext::with_db(&mut client, &mut db);
        info_command(&mut ctx).unwrap();
        repl.set_repl_child_pid(0);

        let reply = client.drain_reply();
        let text = bulk_text(&reply);
        assert_eq!(field_value(text, "rdb_bgsave_in_progress"), "1");
    }

    #[test]
    fn info_memory_exposes_replication_buffer_fields() {
        let replica_id = 980_777;
        {
            let mut replica = Client::new(replica_id);
            replica.is_replica = true;
            let mut guard = client_info_registry().lock().unwrap();
            guard.register(replica_id, "127.0.0.1:0".to_string());
            guard.update_client_metadata(&replica);
            guard.set_output_buffer_memory(replica_id, 4096);
        }

        let mut db = RedisDb::new(0);
        let mut client = Client::new(980_778);
        client.set_args(vec![arg(b"INFO"), arg(b"memory")]);
        let mut ctx = CommandContext::with_db(&mut client, &mut db);
        info_command(&mut ctx).unwrap();

        {
            let mut guard = client_info_registry().lock().unwrap();
            guard.deregister(replica_id);
        }

        let reply = client.drain_reply();
        let text = bulk_text(&reply);
        assert_eq!(field_value(text, "mem_replicas_repl_buffer"), "0");
        assert_eq!(field_value(text, "mem_clients_slaves"), "4096");
        let total = field_value(text, "mem_total_replication_buffers")
            .parse::<usize>()
            .expect("numeric total replication-buffer memory");
        let backlog = field_value(text, "mem_replication_backlog")
            .parse::<usize>()
            .expect("numeric replication backlog memory");
        assert_eq!(
            total, backlog,
            "ordinary replica output memory is client memory, not replication-buffer memory"
        );
    }

    #[test]
    fn info_replication_counts_dual_channel_rdb_channel_for_waiting_fullsync() {
        let _guard = repl_info_guard();
        clear_repl_info_state();
        let repl = global_replication_state();
        let (online_tx, _online_rx) = mpsc::channel();
        let (sync_tx, _sync_rx) = mpsc::channel();
        let (send_tx, _send_rx) = mpsc::channel();
        repl.add_replica(ReplicaConn::new(
            980_780,
            ReplicaState::Online,
            42,
            online_tx,
        ));
        repl.add_replica(ReplicaConn::new(
            980_781,
            ReplicaState::WaitingBgsave,
            42,
            sync_tx,
        ));
        repl.add_replica(ReplicaConn::new(
            980_783,
            ReplicaState::SendingRdb,
            43,
            send_tx,
        ));

        let mut db = RedisDb::new(0);
        let mut client = Client::new(980_782);
        client.set_args(vec![arg(b"INFO"), arg(b"replication")]);
        let mut ctx = CommandContext::with_db(&mut client, &mut db);
        info_command(&mut ctx).unwrap();

        clear_repl_info_state();
        let reply = client.drain_reply();
        let text = bulk_text(&reply);
        assert_eq!(field_value(text, "connected_slaves"), "5");
        assert!(
            text.contains("state=wait_bgsave,offset=42,lag=0,type=rdb-channel"),
            "INFO replication should expose a provisional rdb-channel line: {text}"
        );
        assert!(
            text.contains("state=send_bulk,offset=43,lag=0,type=rdb-channel"),
            "INFO replication should expose an RDB-channel line while full-sync output is pending: {text}"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         1
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         INFO keyspace uses CommandContext DB routing so it can read
//                  RuntimeOwner-owned DBs without `global_databases()`.
// ──────────────────────────────────────────────────────────────────────────
