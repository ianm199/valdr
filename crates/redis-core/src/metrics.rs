//! Server-wide metrics — connection counts, command throughput, keyspace stats.
//!
//! All counters use `AtomicU64` with `Ordering::Relaxed` for throughput; exact
//! consistency is not required since INFO is sampled at human timescales.
//!
//! Exposed via a process-global `OnceLock<Arc<ServerMetrics>>` so that
//! `info.rs`, `db.rs`, and `main.rs` can all reach the same instance without
//! threading the object through every call-site parameter list.

use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

static METRICS: OnceLock<Arc<ServerMetrics>> = OnceLock::new();

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
    /// Keys removed by lazy or active expiration.
    pub expired_keys: AtomicU64,
    /// Keys removed by the maxmemory eviction policy.
    pub evicted_keys: AtomicU64,
    /// Cumulative microseconds spent inside command dispatch on the main thread.
    pub active_time_main_thread_us: AtomicU64,
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
            expired_keys: AtomicU64::new(0),
            evicted_keys: AtomicU64::new(0),
            active_time_main_thread_us: AtomicU64::new(0),
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
        self.expired_keys.store(0, Ordering::Relaxed);
        self.evicted_keys.store(0, Ordering::Relaxed);
        self.active_time_main_thread_us.store(0, Ordering::Relaxed);
        self.rdb_saves_succeeded.store(0, Ordering::Relaxed);
        self.rdb_saves_failed.store(0, Ordering::Relaxed);
    }
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
        let rss_pages: u64 = text
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()?;
        Some(rss_pages * 4096)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
