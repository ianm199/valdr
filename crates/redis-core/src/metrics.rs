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

struct AtomicCommandStat {
    calls: AtomicU64,
    usec: AtomicU64,
    rejected_calls: AtomicU64,
    failed_calls: AtomicU64,
}

impl AtomicCommandStat {
    const fn new() -> Self {
        Self {
            calls: AtomicU64::new(0),
            usec: AtomicU64::new(0),
            rejected_calls: AtomicU64::new(0),
            failed_calls: AtomicU64::new(0),
        }
    }

    fn record(&self, elapsed_us: u64, rejected_call: bool, failed_call: bool) {
        if rejected_call {
            self.rejected_calls.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.usec.fetch_add(elapsed_us, Ordering::Relaxed);
        if failed_call {
            self.failed_calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_failure(&self) {
        self.failed_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_blocked_rejected(&self) {
        self.rejected_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_reprocessed_rejected(&self) {
        let _ = self
            .calls
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |calls| {
                Some(calls.saturating_sub(1))
            });
        self.rejected_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, name: &[u8]) -> Option<CommandStatSnapshot> {
        let calls = self.calls.load(Ordering::Relaxed);
        let usec = self.usec.load(Ordering::Relaxed);
        let rejected_calls = self.rejected_calls.load(Ordering::Relaxed);
        let failed_calls = self.failed_calls.load(Ordering::Relaxed);
        if calls == 0 && usec == 0 && rejected_calls == 0 && failed_calls == 0 {
            return None;
        }
        Some(CommandStatSnapshot {
            name: name.to_vec(),
            calls,
            usec,
            rejected_calls,
            failed_calls,
        })
    }

    fn reset(&self) {
        self.calls.store(0, Ordering::Relaxed);
        self.usec.store(0, Ordering::Relaxed);
        self.rejected_calls.store(0, Ordering::Relaxed);
        self.failed_calls.store(0, Ordering::Relaxed);
    }
}

static GET_COMMAND_STATS: AtomicCommandStat = AtomicCommandStat::new();
static SET_COMMAND_STATS: AtomicCommandStat = AtomicCommandStat::new();
static PING_COMMAND_STATS: AtomicCommandStat = AtomicCommandStat::new();
static INCR_COMMAND_STATS: AtomicCommandStat = AtomicCommandStat::new();
static SADD_COMMAND_STATS: AtomicCommandStat = AtomicCommandStat::new();
static HSET_COMMAND_STATS: AtomicCommandStat = AtomicCommandStat::new();
static ZADD_COMMAND_STATS: AtomicCommandStat = AtomicCommandStat::new();
static SPOP_COMMAND_STATS: AtomicCommandStat = AtomicCommandStat::new();
static ZPOPMIN_COMMAND_STATS: AtomicCommandStat = AtomicCommandStat::new();

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
    if let Some((_, stat)) = hot_command_stat(name) {
        stat.record(elapsed_us, rejected_call, failed_call);
        return;
    }
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

/// Mark an already-counted command as failed (increment `failed_calls` only).
///
/// Used when a blocked command — counted as a call when it first dispatched
/// and parked — is later unblocked with an error reply. Incrementing `calls`
/// again would double-count, so only `failed_calls` moves.
pub fn record_command_failure(name: &[u8]) {
    if let Some((_, stat)) = hot_command_stat(name) {
        stat.record_failure();
        return;
    }
    let key = command_stats_key(name);
    if key.is_empty() {
        return;
    }
    let mut stats = match command_stats_handle().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let row = stats.entry(key).or_default();
    row.failed_calls = row.failed_calls.saturating_add(1);
}

pub fn record_blocked_command_rejected(name: &[u8]) {
    if let Some((_, stat)) = hot_command_stat(name) {
        stat.record_blocked_rejected();
        return;
    }
    let key = command_stats_key(name);
    if key.is_empty() {
        return;
    }
    let mut stats = match command_stats_handle().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let row = stats.entry(key).or_default();
    row.rejected_calls = row.rejected_calls.saturating_add(1);
}

/// Reclassify a blocked command that parked successfully but was later
/// rejected when the server retried it after wakeup.
pub fn record_blocked_command_reprocessed_rejected(name: &[u8]) {
    if let Some((_, stat)) = hot_command_stat(name) {
        stat.record_reprocessed_rejected();
        return;
    }
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

pub fn record_total_fork() {
    server_metrics().total_forks.fetch_add(1, Ordering::Relaxed);
}

fn command_stats_key(name: &[u8]) -> Vec<u8> {
    let mut key = name.to_ascii_lowercase();
    key.retain(|b| *b != b'\r' && *b != b'\n');
    key
}

fn hot_command_stat(name: &[u8]) -> Option<(&'static [u8], &'static AtomicCommandStat)> {
    match name {
        [a, b, c]
            if ascii_lower(*a) == b'g' && ascii_lower(*b) == b'e' && ascii_lower(*c) == b't' =>
        {
            Some((b"get", &GET_COMMAND_STATS))
        }
        [a, b, c]
            if ascii_lower(*a) == b's' && ascii_lower(*b) == b'e' && ascii_lower(*c) == b't' =>
        {
            Some((b"set", &SET_COMMAND_STATS))
        }
        [a, b, c, d]
            if ascii_lower(*a) == b'p'
                && ascii_lower(*b) == b'i'
                && ascii_lower(*c) == b'n'
                && ascii_lower(*d) == b'g' =>
        {
            Some((b"ping", &PING_COMMAND_STATS))
        }
        [a, b, c, d]
            if ascii_lower(*a) == b'i'
                && ascii_lower(*b) == b'n'
                && ascii_lower(*c) == b'c'
                && ascii_lower(*d) == b'r' =>
        {
            Some((b"incr", &INCR_COMMAND_STATS))
        }
        [a, b, c, d]
            if ascii_lower(*a) == b's'
                && ascii_lower(*b) == b'a'
                && ascii_lower(*c) == b'd'
                && ascii_lower(*d) == b'd' =>
        {
            Some((b"sadd", &SADD_COMMAND_STATS))
        }
        [a, b, c, d]
            if ascii_lower(*a) == b'h'
                && ascii_lower(*b) == b's'
                && ascii_lower(*c) == b'e'
                && ascii_lower(*d) == b't' =>
        {
            Some((b"hset", &HSET_COMMAND_STATS))
        }
        [a, b, c, d]
            if ascii_lower(*a) == b'z'
                && ascii_lower(*b) == b'a'
                && ascii_lower(*c) == b'd'
                && ascii_lower(*d) == b'd' =>
        {
            Some((b"zadd", &ZADD_COMMAND_STATS))
        }
        [a, b, c, d]
            if ascii_lower(*a) == b's'
                && ascii_lower(*b) == b'p'
                && ascii_lower(*c) == b'o'
                && ascii_lower(*d) == b'p' =>
        {
            Some((b"spop", &SPOP_COMMAND_STATS))
        }
        [a, b, c, d, e, f, g]
            if ascii_lower(*a) == b'z'
                && ascii_lower(*b) == b'p'
                && ascii_lower(*c) == b'o'
                && ascii_lower(*d) == b'p'
                && ascii_lower(*e) == b'm'
                && ascii_lower(*f) == b'i'
                && ascii_lower(*g) == b'n' =>
        {
            Some((b"zpopmin", &ZPOPMIN_COMMAND_STATS))
        }
        _ => None,
    }
}

#[inline]
fn ascii_lower(byte: u8) -> u8 {
    if byte.is_ascii_uppercase() {
        byte + 32
    } else {
        byte
    }
}

fn append_hot_command_stats(out: &mut Vec<CommandStatSnapshot>) {
    for (name, stat) in [
        (b"get".as_slice(), &GET_COMMAND_STATS),
        (b"hset".as_slice(), &HSET_COMMAND_STATS),
        (b"incr".as_slice(), &INCR_COMMAND_STATS),
        (b"ping".as_slice(), &PING_COMMAND_STATS),
        (b"sadd".as_slice(), &SADD_COMMAND_STATS),
        (b"set".as_slice(), &SET_COMMAND_STATS),
        (b"zadd".as_slice(), &ZADD_COMMAND_STATS),
        (b"spop".as_slice(), &SPOP_COMMAND_STATS),
        (b"zpopmin".as_slice(), &ZPOPMIN_COMMAND_STATS),
    ] {
        if let Some(snapshot) = stat.snapshot(name) {
            out.push(snapshot);
        }
    }
}

fn reset_hot_command_stats() {
    GET_COMMAND_STATS.reset();
    HSET_COMMAND_STATS.reset();
    INCR_COMMAND_STATS.reset();
    PING_COMMAND_STATS.reset();
    SADD_COMMAND_STATS.reset();
    SET_COMMAND_STATS.reset();
    ZADD_COMMAND_STATS.reset();
    SPOP_COMMAND_STATS.reset();
    ZPOPMIN_COMMAND_STATS.reset();
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
    append_hot_command_stats(&mut out);
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

pub fn reset_command_stats() {
    reset_hot_command_stats();
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
    if !stats.contains_key(&key) && stats.len() >= 128 && !is_builtin_error_code(&key) {
        key = b"ERRORSTATS_OVERFLOW".to_vec();
    }
    let count = stats.entry(key).or_default();
    *count = count.saturating_add(1);
}

fn is_builtin_error_code(code: &[u8]) -> bool {
    matches!(
        code,
        b"ASK"
            | b"BUSY"
            | b"CLUSTERDOWN"
            | b"CROSSSLOT"
            | b"ERR"
            | b"EXECABORT"
            | b"LOADING"
            | b"MOVED"
            | b"NOAUTH"
            | b"NOGROUP"
            | b"NOSCRIPT"
            | b"NOPERM"
            | b"OOM"
            | b"READONLY"
            | b"TRYAGAIN"
            | b"UNBLOCKED"
            | b"WRONGPASS"
            | b"WRONGTYPE"
    )
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
    /// ACL database access denials.
    pub acl_access_denied_db: AtomicU64,
    /// Keys removed by lazy or active expiration.
    pub expired_keys: AtomicU64,
    /// Keys removed by the maxmemory eviction policy.
    pub evicted_keys: AtomicU64,
    /// Clients disconnected by maxmemory-clients eviction.
    pub evicted_clients: AtomicU64,
    /// Clients disconnected after exceeding `client-query-buffer-limit`.
    pub client_query_buffer_limit_disconnections: AtomicU64,
    /// Clients disconnected after exceeding `client-output-buffer-limit`.
    pub client_output_buffer_limit_disconnections: AtomicU64,
    /// Cumulative microseconds spent inside command dispatch on the main thread.
    pub active_time_main_thread_us: AtomicU64,
    /// Total error replies emitted since the last `CONFIG RESETSTAT`.
    pub total_error_replies: AtomicU64,
    /// Number of logical fork/background-persistence starts.
    pub total_forks: AtomicU64,
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
            acl_access_denied_db: AtomicU64::new(0),
            expired_keys: AtomicU64::new(0),
            evicted_keys: AtomicU64::new(0),
            evicted_clients: AtomicU64::new(0),
            client_query_buffer_limit_disconnections: AtomicU64::new(0),
            client_output_buffer_limit_disconnections: AtomicU64::new(0),
            active_time_main_thread_us: AtomicU64::new(0),
            total_error_replies: AtomicU64::new(0),
            total_forks: AtomicU64::new(0),
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
        self.acl_access_denied_db.store(0, Ordering::Relaxed);
        self.expired_keys.store(0, Ordering::Relaxed);
        self.evicted_keys.store(0, Ordering::Relaxed);
        self.evicted_clients.store(0, Ordering::Relaxed);
        self.client_query_buffer_limit_disconnections
            .store(0, Ordering::Relaxed);
        self.client_output_buffer_limit_disconnections
            .store(0, Ordering::Relaxed);
        self.active_time_main_thread_us.store(0, Ordering::Relaxed);
        self.total_error_replies.store(0, Ordering::Relaxed);
        self.total_forks.store(0, Ordering::Relaxed);
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

pub fn record_acl_access_denied_db() {
    server_metrics()
        .acl_access_denied_db
        .fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incr_uses_hot_command_stat_bucket() {
        reset_command_stats();

        record_command_stat(b"INCR", 7, false, false);

        let snapshots = command_stats_snapshot();
        let incr = snapshots
            .iter()
            .find(|snapshot| snapshot.name == b"incr")
            .expect("INCR command stat snapshot");
        assert_eq!(incr.calls, 1);
        assert_eq!(incr.usec, 7);
        assert_eq!(incr.rejected_calls, 0);
        assert_eq!(incr.failed_calls, 0);

        reset_command_stats();
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
        let rss_pages: u64 = text.split_whitespace().nth(1)?.parse().ok()?;
        Some(rss_pages * 4096)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
