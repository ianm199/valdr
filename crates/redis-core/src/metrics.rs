//! Server-wide metrics — connection counts, command throughput, keyspace stats.
//!
//! All counters use `AtomicU64` with `Ordering::Relaxed` for throughput; exact
//! consistency is not required since INFO is sampled at human timescales.
//!
//! Exposed via a process-global `OnceLock<Arc<ServerMetrics>>` so that
//! `info.rs`, `db.rs`, and `main.rs` can all reach the same instance without
//! threading the object through every call-site parameter list.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

static METRICS: OnceLock<Arc<ServerMetrics>> = OnceLock::new();
static COMMAND_STATS: OnceLock<Arc<Mutex<HashMap<Vec<u8>, CommandStat>>>> = OnceLock::new();
static ERROR_STATS: OnceLock<Arc<Mutex<HashMap<Vec<u8>, u64>>>> = OnceLock::new();

#[derive(Clone, Debug, Default)]
struct CommandStat {
    calls: u64,
    usec: u64,
    rejected_calls: u64,
    failed_calls: u64,
}

#[derive(Clone, Debug, Default)]
pub struct CommandStatSnapshot {
    pub name: Vec<u8>,
    pub calls: u64,
    pub usec: u64,
    pub rejected_calls: u64,
    pub failed_calls: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ErrorStatSnapshot {
    pub name: Vec<u8>,
    pub count: u64,
}

/// Install the global metrics instance. Must be called once at server startup
/// before any connection is accepted. Subsequent calls return the existing
/// instance unchanged.
pub fn server_metrics() -> &'static Arc<ServerMetrics> {
    METRICS.get_or_init(|| {
        let start_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Arc::new(ServerMetrics::new(start_ms))
    })
}

fn command_stats_handle() -> &'static Arc<Mutex<HashMap<Vec<u8>, CommandStat>>> {
    COMMAND_STATS.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

fn error_stats_handle() -> &'static Arc<Mutex<HashMap<Vec<u8>, u64>>> {
    ERROR_STATS.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

pub fn record_command_stat(name: &[u8], elapsed_us: u64, rejected_call: bool, failed_call: bool) {
    let key = command_stats_key(name);
    if key.is_empty() {
        return;
    }
    let mut stats = match command_stats_handle().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let row = stats.entry(key).or_default();
    if rejected_call {
        row.rejected_calls = row.rejected_calls.saturating_add(1);
        return;
    }
    row.calls = row.calls.saturating_add(1);
    row.usec = row.usec.saturating_add(elapsed_us);
    if failed_call {
        row.failed_calls = row.failed_calls.saturating_add(1);
    }
}

pub fn record_blocked_command_rejected(name: &[u8]) {
    let key = command_stats_key(name);
    if key.is_empty() {
        return;
    }
    let mut stats = match command_stats_handle().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let row = stats.entry(key).or_default();
    row.calls = row.calls.saturating_sub(1);
    row.rejected_calls = row.rejected_calls.saturating_add(1);
}

fn command_stats_key(name: &[u8]) -> Vec<u8> {
    let mut key = name.to_ascii_lowercase();
    key.retain(|b| *b != b'\r' && *b != b'\n');
    key
}

pub fn command_stats_snapshot() -> Vec<CommandStatSnapshot> {
    let stats = match command_stats_handle().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let mut out: Vec<CommandStatSnapshot> = stats
        .iter()
        .map(|(name, stat)| CommandStatSnapshot {
            name: name.clone(),
            calls: stat.calls,
            usec: stat.usec,
            rejected_calls: stat.rejected_calls,
            failed_calls: stat.failed_calls,
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

pub fn reset_command_stats() {
    let mut stats = match command_stats_handle().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    stats.clear();
}

pub fn record_error_reply(payload: &[u8]) {
    let mut payload = payload;
    if payload.first() == Some(&b'-') {
        payload = &payload[1..];
    }
    let payload = payload
        .split(|b| *b == b'\r' || *b == b'\n')
        .next()
        .unwrap_or(payload);
    let code = payload
        .split(|b| *b == b' ' || *b == b'\t')
        .next()
        .unwrap_or(payload);
    if code.is_empty() {
        return;
    }

    server_metrics()
        .total_error_replies
        .fetch_add(1, Ordering::Relaxed);

    let mut key = code.to_ascii_uppercase();
    key.retain(|b| *b != b'\r' && *b != b'\n');
    let mut stats = match error_stats_handle().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let count = stats.entry(key).or_default();
    *count = count.saturating_add(1);
}

pub fn error_stats_snapshot() -> Vec<ErrorStatSnapshot> {
    let stats = match error_stats_handle().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let mut out: Vec<ErrorStatSnapshot> = stats
        .iter()
        .map(|(name, count)| ErrorStatSnapshot {
            name: name.clone(),
            count: *count,
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

pub fn reset_error_stats() {
    let mut stats = match error_stats_handle().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    stats.clear();
}

/// Atomically tracked server-wide counters.
pub struct ServerMetrics {
    /// Unix milliseconds when the server process started.
    pub start_time_ms: u64,
    /// TCP port the server is bound to. Written once at startup.
    pub tcp_port: AtomicU16,
    /// Number of clients whose TCP session is currently open.
    pub connected_clients: AtomicU64,
    /// Peak value of `connected_clients` ever observed.
    pub max_clients_seen: AtomicU64,
    /// Total accepted connections since startup.
    pub total_connections_received: AtomicU64,
    /// Total commands dispatched since startup.
    pub total_commands_processed: AtomicU64,
    /// Successful key lookups (key found, not expired).
    pub keyspace_hits: AtomicU64,
    /// Failed key lookups (key absent or expired).
    pub keyspace_misses: AtomicU64,
    /// Connections rejected because connected_clients >= maxclients.
    pub rejected_connections: AtomicU64,
    /// ACL authentication denials.
    pub acl_access_denied_auth: AtomicU64,
    /// ACL command denials.
    pub acl_access_denied_cmd: AtomicU64,
    /// ACL key access denials.
    pub acl_access_denied_key: AtomicU64,
    /// ACL channel access denials.
    pub acl_access_denied_channel: AtomicU64,
    /// Keys removed by lazy or active expiration.
    pub expired_keys: AtomicU64,
    /// Keys removed by the maxmemory eviction policy.
    pub evicted_keys: AtomicU64,
    /// Clients disconnected by maxmemory-clients eviction.
    pub evicted_clients: AtomicU64,
    /// Cumulative microseconds spent inside command dispatch on the main thread.
    pub active_time_main_thread_us: AtomicU64,
    /// Total error replies emitted since the last `CONFIG RESETSTAT`.
    pub total_error_replies: AtomicU64,
    /// Number of BGSAVE child processes that exited with status 0.
    pub rdb_saves_succeeded: AtomicU64,
    /// Number of BGSAVE child processes that exited with a non-zero status.
    pub rdb_saves_failed: AtomicU64,
}

impl ServerMetrics {
    fn new(start_time_ms: u64) -> Self {
        Self {
            start_time_ms,
            tcp_port: AtomicU16::new(0),
            connected_clients: AtomicU64::new(0),
            max_clients_seen: AtomicU64::new(0),
            total_connections_received: AtomicU64::new(0),
            total_commands_processed: AtomicU64::new(0),
            keyspace_hits: AtomicU64::new(0),
            keyspace_misses: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            acl_access_denied_auth: AtomicU64::new(0),
            acl_access_denied_cmd: AtomicU64::new(0),
            acl_access_denied_key: AtomicU64::new(0),
            acl_access_denied_channel: AtomicU64::new(0),
            expired_keys: AtomicU64::new(0),
            evicted_keys: AtomicU64::new(0),
            evicted_clients: AtomicU64::new(0),
            active_time_main_thread_us: AtomicU64::new(0),
            total_error_replies: AtomicU64::new(0),
            rdb_saves_succeeded: AtomicU64::new(0),
            rdb_saves_failed: AtomicU64::new(0),
        }
    }

    /// Increment `connected_clients`, updating `max_clients_seen` if the new
    /// value exceeds the recorded peak.
    pub fn on_connect(&self) {
        let prev = self.connected_clients.fetch_add(1, Ordering::Relaxed);
        let new = prev + 1;
        let mut peak = self.max_clients_seen.load(Ordering::Relaxed);
        while new > peak {
            match self.max_clients_seen.compare_exchange_weak(
                peak,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => peak = current,
            }
        }
    }

    /// Decrement `connected_clients` on disconnect.
    pub fn on_disconnect(&self) {
        self.connected_clients.fetch_sub(1, Ordering::Relaxed);
    }

    /// Store the TCP port the server bound. Called once after `TcpListener::bind`
    /// succeeds, before any connection is accepted.
    pub fn set_tcp_port(&self, port: u16) {
        self.tcp_port.store(port, Ordering::Relaxed);
    }

    /// Reset all statistical counters to zero.
    ///
    /// Corresponds to `CONFIG RESETSTAT`. Preserves `start_time_ms`, `tcp_port`,
    /// `connected_clients`, and `max_clients_seen` since those reflect live state
    /// rather than historical counters.
    pub fn reset_stats(&self) {
        self.total_connections_received.store(0, Ordering::Relaxed);
        self.total_commands_processed.store(0, Ordering::Relaxed);
        self.keyspace_hits.store(0, Ordering::Relaxed);
        self.keyspace_misses.store(0, Ordering::Relaxed);
        self.rejected_connections.store(0, Ordering::Relaxed);
        self.acl_access_denied_auth.store(0, Ordering::Relaxed);
        self.acl_access_denied_cmd.store(0, Ordering::Relaxed);
        self.acl_access_denied_key.store(0, Ordering::Relaxed);
        self.acl_access_denied_channel.store(0, Ordering::Relaxed);
        self.expired_keys.store(0, Ordering::Relaxed);
        self.evicted_keys.store(0, Ordering::Relaxed);
        self.evicted_clients.store(0, Ordering::Relaxed);
        self.active_time_main_thread_us.store(0, Ordering::Relaxed);
        self.total_error_replies.store(0, Ordering::Relaxed);
        self.rdb_saves_succeeded.store(0, Ordering::Relaxed);
        self.rdb_saves_failed.store(0, Ordering::Relaxed);
        reset_command_stats();
        reset_error_stats();
    }
}

pub fn record_acl_access_denied_auth() {
    server_metrics()
        .acl_access_denied_auth
        .fetch_add(1, Ordering::Relaxed);
}

pub fn record_acl_access_denied_cmd() {
    server_metrics()
        .acl_access_denied_cmd
        .fetch_add(1, Ordering::Relaxed);
}

pub fn record_acl_access_denied_key() {
    server_metrics()
        .acl_access_denied_key
        .fetch_add(1, Ordering::Relaxed);
}

pub fn record_acl_access_denied_channel() {
    server_metrics()
        .acl_access_denied_channel
        .fetch_add(1, Ordering::Relaxed);
}

/// Return the resident set size of this process in bytes, or `None` if the
/// platform does not expose `/proc/self/statm` or reading it fails.
///
/// On Linux each field in `/proc/self/statm` is measured in pages. The second
/// field is the resident set size. Multiply by `sysconf(_SC_PAGESIZE)` —
/// approximated here as 4096 bytes because `libc::sysconf` would add a C FFI
/// dependency. Real page sizes other than 4096 (e.g. huge-page kernels) will
/// produce a proportional error, which is acceptable for the INFO estimator.
///
/// TODO(architect): replace the 4096 constant with a `sysconf(_SC_PAGESIZE)`
/// call once we accept the `libc` dependency.
pub fn rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string("/proc/self/statm").ok()?;
        let rss_pages: u64 = text.split_whitespace().nth(1)?.parse().ok()?;
        Some(rss_pages * 4096)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
