//! `RedisServer` — global server state.
//!
//! STUB. Just enough surface for command implementations to reach a
//! database, the next client id, and a few config knobs. Replication,
//! cluster, persistence, modules — all deferred to their own phases.

use crate::db::RedisDb;
use crate::client::ClientId;

#[derive(Debug)]
pub struct RedisServer {
    /// Tick counter for assigning client ids.
    next_client_id: ClientId,
    /// Databases. Standalone defaults to 16 dbs; pilot uses just 1.
    dbs: Vec<RedisDb>,
    /// Bind port (configured at startup).
    pub port: u16,
    /// Single-source-of-truth config flags (more land later).
    pub config: ServerConfig,
}

#[derive(Debug, Default, Clone)]
pub struct ServerConfig {
    /// `--maxmemory` equivalent (bytes; 0 = unlimited).
    pub max_memory: u64,
    /// Whether DEBUG command is enabled.
    pub enable_debug_command: bool,
}

impl Default for RedisServer {
    fn default() -> Self {
        Self::new(6379)
    }
}

impl RedisServer {
    pub fn new(port: u16) -> Self {
        Self {
            next_client_id: 0,
            dbs: vec![RedisDb::new(0)],
            port,
            config: ServerConfig::default(),
        }
    }

    pub fn alloc_client_id(&mut self) -> ClientId {
        let id = self.next_client_id;
        self.next_client_id = self.next_client_id.wrapping_add(1);
        id
    }

    pub fn db(&self, index: u32) -> Option<&RedisDb> {
        self.dbs.get(index as usize)
    }

    pub fn db_mut(&mut self, index: u32) -> Option<&mut RedisDb> {
        self.dbs.get_mut(index as usize)
    }

    pub fn db_count(&self) -> usize {
        self.dbs.len()
    }

    /// Add additional databases (standalone Redis defaults to 16).
    pub fn set_db_count(&mut self, n: usize) {
        while self.dbs.len() < n {
            let id = self.dbs.len() as u32;
            self.dbs.push(RedisDb::new(id));
        }
        self.dbs.truncate(n);
    }

    /// Whether cluster mode is enabled (maps to C `server.cluster_enabled`).
    ///
    /// STUB — Phase B placeholder; cluster wiring is Phase 3+.
    pub fn cluster_enabled(&self) -> bool {
        false
    }

    /// Maximum idle time, in seconds, before an idle client is closed
    /// (maps to C `server.maxidletime`).
    ///
    /// STUB — Phase B placeholder returning 0 (disabled). Real value comes
    /// from CONFIG once config.c is fully wired.
    pub fn max_idle_time(&self) -> i64 {
        0
    }
}

// ──────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (stub for translate-loop unblock)
//   target_crate:  redis-core
//   confidence:    skeleton
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Minimal global state. Replication/cluster/persist/modules deferred to their phases.
// ──────────────────────────────────────────────────────────────────────
