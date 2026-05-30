//! `RedisServer` — global server state shared via `Arc`.
//! Round 15a refactor: `RedisServer` is now an `Arc`-able container that
//! accept loop builds once at startup and every command handler reaches via
//! `ctx.server`. Live-tunable config knobs (maxmemory, requirepass,
//! notify-keyspace-events, encoding thresholds, …) live behind `Arc<LiveConfig>`
//! with per-field atomics so reads are lock-free and CONFIG SET writes are
//! visible to every thread immediately.
//! Mutable counters (`alloc_client_id`, `dirty`, `in_exec`, …) use interior
//! atomics so the server is shareable through `&RedisServer` without giving
//! out a `&mut` reference.

use std::sync::atomic::{
    AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};
use std::sync::{Arc, Mutex};

use crate::client::ClientId;
use crate::db::RedisDb;
use crate::live_config::LiveConfig;
use crate::persistence::{AofState, PersistenceState};

/// Stub command table handle.
/// TODO(architect): real type later — should be a reference to the registry
/// in `redis-commands::generated::COMMANDS` plus a `HashMap<&[u8], &spec>`
/// case-insensitive lookup.
#[derive(Debug, Default, Clone, Copy)]
pub struct CommandTableHandle;

/// Stub listener handle. TODO(architect): real type later (collapse with
/// `connection::ConnListener` once the vtable registry has live backends).
#[derive(Debug, Default)]
pub struct ListenerHandle {
 /// Number of bound file descriptors (0 when the listener is inactive).
    pub fd_count: i32,
}

/// Top-level server state. Wrapped in `Arc<RedisServer>` once at startup so
/// every connection reads the same live config without lock acquisition.
pub struct RedisServer {
    next_client_id: AtomicU64,
 /// Databases. Standalone defaults to 16 dbs; pilot uses just 1. The vec
 /// itself is fixed at startup; per-db state is interior-mutable through
 /// `&RedisDb`.
    dbs: Vec<RedisDb>,
 /// Bind port (configured at startup).
    pub port: u16,
 /// Bind addresses as raw bytes (e.g. `b"127.0.0.1"`).
    pub bind_addrs: Vec<Vec<u8>>,
 /// Live (CONFIG SET-tunable) configuration knobs.
    pub live_config: Arc<LiveConfig>,
    /// Command-table handle. TODO(architect): real type later.
    pub commands_table: CommandTableHandle,
 /// Persistence runtime state reported by INFO and updated by reapers.
    pub persistence: PersistenceState,
 /// Cached command-time snapshot in milliseconds since epoch.
    pub cmd_time_snapshot: AtomicI64,
 /// Active TCP listeners.
    pub listeners: Vec<ListenerHandle>,
 /// Number of clients currently in a MULTI block watching keys.
    pub watching_clients: AtomicU64,
 /// Dirty counter — increments per write command for AOF/replication.
    pub dirty: AtomicI64,
 /// Whether the server is in the middle of an EXEC dispatch.
    pub in_exec: AtomicBool,
 /// Whether the server is paused (CLIENT PAUSE / failover).
    pub pause_cron: AtomicBool,
 /// Client-pause state, one `PauseEvent` per `PausePurpose`. Written by
 /// CLIENT PAUSE/UNPAUSE and read by the command gate, INFO, and
 /// eviction/active-expire paths. Mirrors `server.client_pause_*` in C.
    pub pause_events: Mutex<[crate::networking::PauseEvent; 4]>,
 /// Cached aggregate of active pause actions. The common unpaused path can
 /// read this without taking `pause_events` or sampling wall-clock time.
    pub cached_paused_actions: AtomicU32,
 /// Maximum size of a bulk reply payload in bytes.
    pub proto_max_bulk_len: AtomicI64,
 /// Server start time (Unix milliseconds).
    pub start_time_ms: i64,
 /// Shutdown flag — checked by the event loop and accept loop.
    pub shutdown_asap: AtomicBool,
 /// Per-db count budget for the eviction pool's round-robin scan.
    pub eviction_db_cursor: AtomicUsize,
 /// PID of the in-flight BGSAVE child process, or 0 when no child is running.
 /// Written by the BGSAVE fork path and cleared by the reaper thread once
 /// child exits. Polled by a background thread every 500 ms.
    pub rdb_child_pid: AtomicI32,
}

/// Default value of `server.proto_max_bulk_len` (512 MiB).
pub const PROTO_MAX_BULK_LEN_DEFAULT: i64 = 512 * 1024 * 1024;

/// Read-side compatibility shim for code that historically used the public
/// `pub config: ServerConfig` field. The struct only carries the values
/// remaining call sites still consult; everything else has migrated
/// `LiveConfig`.
#[derive(Debug, Default, Clone)]
pub struct ServerConfig {
    pub max_memory: u64,
    pub enable_debug_command: bool,
}

impl Default for RedisServer {
    fn default() -> Self {
        Self::new(6379)
    }
}

impl RedisServer {
 /// Construct a `RedisServer` bound at the given port with one DB and a
 /// fresh default `LiveConfig`.
    pub fn new(port: u16) -> Self {
        Self {
            next_client_id: AtomicU64::new(0),
            dbs: vec![RedisDb::new(0)],
            port,
            bind_addrs: Vec::new(),
            live_config: Arc::new(LiveConfig::new()),
            commands_table: CommandTableHandle,
            persistence: PersistenceState::new(),
            cmd_time_snapshot: AtomicI64::new(0),
            listeners: Vec::new(),
            watching_clients: AtomicU64::new(0),
            dirty: AtomicI64::new(0),
            in_exec: AtomicBool::new(false),
            pause_cron: AtomicBool::new(false),
            pause_events: Mutex::new(<[crate::networking::PauseEvent; 4]>::default()),
            cached_paused_actions: AtomicU32::new(0),
            proto_max_bulk_len: AtomicI64::new(PROTO_MAX_BULK_LEN_DEFAULT),
            start_time_ms: 0,
            shutdown_asap: AtomicBool::new(false),
            eviction_db_cursor: AtomicUsize::new(0),
            rdb_child_pid: AtomicI32::new(0),
        }
    }

 /// Construct sharing a caller-supplied `LiveConfig` (e.g. the accept loop
 /// has already populated it from CLI/config-file parsing).
    pub fn with_live_config(port: u16, live_config: Arc<LiveConfig>) -> Self {
        let mut server = Self::new(port);
        server.live_config = live_config;
        server
    }

 /// Compatibility shim returning a snapshot view of the legacy
 /// `ServerConfig` struct. Reads pass through `LiveConfig`.
    pub fn config(&self) -> ServerConfig {
        ServerConfig {
            max_memory: self.live_config.maxmemory(),
            enable_debug_command: false,
        }
    }

 /// Atomically allocate the next client id.
    pub fn alloc_client_id(&self) -> ClientId {
        self.next_client_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn db(&self, index: u32) -> Option<&RedisDb> {
        self.dbs.get(index as usize)
    }

    pub fn db_count(&self) -> usize {
        self.dbs.len()
    }

 /// Add additional databases (standalone Redis defaults to 16). Intended
 /// for the startup path before the server is wrapped in `Arc`.
    pub fn set_db_count(&mut self, n: usize) {
        while self.dbs.len() < n {
            let id = self.dbs.len() as u32;
            self.dbs.push(RedisDb::new(id));
        }
        self.dbs.truncate(n);
    }

 /// Whether cluster mode is enabled. STUB — Phase B placeholder.
    pub fn cluster_enabled(&self) -> bool {
        false
    }

 /// Maximum idle time, in seconds, before an idle client is closed.
 /// STUB — Phase B placeholder.
    pub fn max_idle_time(&self) -> i64 {
        0
    }

 /// Set the server-wide `in_exec` flag (true while EXEC is mid-flight).
    pub fn set_in_exec(&self, value: bool) {
        self.in_exec.store(value, Ordering::Relaxed);
    }

    pub fn in_exec(&self) -> bool {
        self.in_exec.load(Ordering::Relaxed)
    }

    pub fn shutdown_asap(&self) -> bool {
        self.shutdown_asap.load(Ordering::Relaxed)
    }

    pub fn set_shutdown_asap(&self, value: bool) {
        self.shutdown_asap.store(value, Ordering::Relaxed);
    }

    pub fn dirty(&self) -> i64 {
        self.dirty.load(Ordering::Relaxed)
    }

    pub fn add_dirty(&self, delta: i64) {
        self.dirty.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn set_dirty(&self, value: i64) {
        self.dirty.store(value, Ordering::Relaxed);
    }

    pub fn cmd_time_snapshot(&self) -> i64 {
        self.cmd_time_snapshot.load(Ordering::Relaxed)
    }

    pub fn set_cmd_time_snapshot(&self, ms: i64) {
        self.cmd_time_snapshot.store(ms, Ordering::Relaxed);
    }

    pub fn proto_max_bulk_len(&self) -> i64 {
        self.proto_max_bulk_len.load(Ordering::Relaxed)
    }

    pub fn set_proto_max_bulk_len(&self, n: i64) {
        self.proto_max_bulk_len.store(n, Ordering::Relaxed);
    }

 /// Return the PID of the in-flight BGSAVE child, or 0 if none is running.
    pub fn rdb_child_pid(&self) -> i32 {
        self.rdb_child_pid.load(Ordering::SeqCst)
    }

 /// Store the PID of the newly-forked BGSAVE child.
    pub fn set_rdb_child_pid(&self, pid: i32) {
        self.rdb_child_pid.store(pid, Ordering::SeqCst);
    }

    pub fn aof_state(&self) -> AofState {
        self.persistence.aof_state()
    }

    pub fn set_aof_state(&self, state: AofState) {
        self.persistence.set_aof_state(state);
    }

 /// Stub random number used by lolwut. Centralised here so command handlers
 /// can call `ctx.server.pseudo_random_f32_minus1_to_1` without an
 /// external `rand` dependency.
    pub fn pseudo_random_f32_minus1_to_1(&self) -> f32 {
        let seed = self.next_client_id.load(Ordering::Relaxed);
        let scaled = (seed.wrapping_mul(2654435761) & 0xFFFF) as f32 / 32768.0;
        scaled - 1.0
    }
}

// ──────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Round 15a refactor — Arc<RedisServer> with live config
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Live config moved to LiveConfig (atomic per-field reads).
//                  rdb_child_pid (AtomicI32) tracks the in-flight BGSAVE child.
//                  Mutation now goes through interior atomics so the server
//                  ships through Arc to every CommandContext.
// ──────────────────────────────────────────────────────────────────────
