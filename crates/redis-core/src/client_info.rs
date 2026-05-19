//! Global per-client metadata snapshot for cross-thread `CLIENT LIST` queries.
//!
//! Each connection thread registers itself on accept and updates its entry on
//! every command dispatch. The `CLIENT LIST` handler on any thread can read a
//! consistent snapshot of all live connections without holding the db lock.
//!
//! Layout:
//!   * `ClientSnapshot` — immutable view of one client's current state.
//!   * `ClientInfoRegistry` — `ClientId → ClientSnapshot` map.
//!   * `client_info_registry()` — process-wide singleton accessor.
//!
//! Fields intentionally kept minimal: only what `CLIENT LIST` actually needs to
//! satisfy the upstream TCL test suite (id, addr, db, flags, cmd).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::client::ClientId;

/// A point-in-time snapshot of one client's observable state.
#[derive(Clone, Default)]
pub struct ClientSnapshot {
    pub id: ClientId,
    pub addr: String,
    pub db_index: u32,
    pub cmd: String,
    pub blocked: bool,
}

/// Server-wide client info table.
#[derive(Default)]
pub struct ClientInfoRegistry {
    entries: HashMap<ClientId, ClientSnapshot>,
}

impl ClientInfoRegistry {
    fn new() -> Self {
        Self::default()
    }

    /// Register a freshly accepted connection.
    pub fn register(&mut self, id: ClientId, addr: String) {
        self.entries.insert(id, ClientSnapshot {
            id,
            addr,
            db_index: 0,
            cmd: String::new(),
            blocked: false,
        });
    }

    /// Update the current command name for `id`.
    pub fn set_cmd(&mut self, id: ClientId, cmd: &str) {
        if let Some(e) = self.entries.get_mut(&id) {
            e.cmd = cmd.to_ascii_lowercase();
            e.blocked = false;
        }
    }

    /// Mark `id` as blocked (waiting on a blocking command).
    pub fn set_blocked(&mut self, id: ClientId, blocked: bool) {
        if let Some(e) = self.entries.get_mut(&id) {
            e.blocked = blocked;
        }
    }

    /// Update `id`'s selected database index.
    pub fn set_db(&mut self, id: ClientId, db_index: u32) {
        if let Some(e) = self.entries.get_mut(&id) {
            e.db_index = db_index;
        }
    }

    /// Remove a connection that has disconnected.
    pub fn deregister(&mut self, id: ClientId) {
        self.entries.remove(&id);
    }

    /// Snapshot of all currently registered clients.
    pub fn all(&self) -> Vec<ClientSnapshot> {
        self.entries.values().cloned().collect()
    }
}

static CLIENT_INFO_REGISTRY: OnceLock<Arc<Mutex<ClientInfoRegistry>>> = OnceLock::new();

/// Return the process-wide client info registry singleton.
pub fn client_info_registry() -> &'static Arc<Mutex<ClientInfoRegistry>> {
    CLIENT_INFO_REGISTRY.get_or_init(|| Arc::new(Mutex::new(ClientInfoRegistry::new())))
}
