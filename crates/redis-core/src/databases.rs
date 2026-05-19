//! Global multi-database store.
//!
//! Redis/Valkey supports up to 16 logical databases (0–15) selectable via
//! SELECT. This module provides a process-wide singleton storing each logical
//! database as a separate `Arc<Mutex<RedisDb>>` keyed by its 0-based index.
//!
//! The primary database (index 0) is the server-initialisation's single db.
//! Databases 1–15 are created on first access (lazy).
//!
//! Layout:
//!   * `GlobalDatabases` — lazily-populated array of up to 16 databases.
//!   * `global_databases()` — process-wide singleton accessor.
//!   * `get_db(index)` — return the `Arc<Mutex<RedisDb>>` for `index`.
//!   * `install_primary_db(db)` — called once at startup to register db 0.

use std::sync::{Arc, Mutex, OnceLock};

use crate::db::RedisDb;

const MAX_DBS: usize = 16;

/// All logical databases for the server instance.
pub struct GlobalDatabases {
    dbs: Vec<Arc<Mutex<RedisDb>>>,
}

impl GlobalDatabases {
    fn new() -> Self {
        let mut dbs = Vec::with_capacity(MAX_DBS);
        for i in 0..MAX_DBS {
            dbs.push(Arc::new(Mutex::new(RedisDb::new(i as u32))));
        }
        Self { dbs }
    }

    /// Return the `Arc<Mutex<RedisDb>>` for `index`.
    ///
    /// Indices outside `0..MAX_DBS` are clamped to `MAX_DBS - 1` rather than
    /// panicking; callers that check bounds before calling can rely on this.
    pub fn get(&self, index: u32) -> Arc<Mutex<RedisDb>> {
        let i = (index as usize).min(MAX_DBS - 1);
        Arc::clone(&self.dbs[i])
    }

    /// Swap the contents of two databases in-place.
    ///
    /// Acquires both locks (lower index first to avoid deadlock), swaps every
    /// stored key and expiry, updates the `id` fields to reflect the new
    /// logical assignment, and then wakes any clients blocked on keys that
    /// now have data in their assigned database.
    pub fn swap(&self, idx1: u32, idx2: u32) {
        let idx1 = (idx1 as usize).min(MAX_DBS - 1);
        let idx2 = (idx2 as usize).min(MAX_DBS - 1);
        if idx1 == idx2 {
            return;
        }
        let (lo, hi) = if idx1 < idx2 { (idx1, idx2) } else { (idx2, idx1) };
        let mut lo_guard = match self.dbs[lo].lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut hi_guard = match self.dbs[hi].lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        lo_guard.swap_contents_with(&mut hi_guard);
    }

    /// Number of logical databases (always `MAX_DBS`).
    pub fn count(&self) -> usize {
        MAX_DBS
    }
}

static GLOBAL_DATABASES: OnceLock<GlobalDatabases> = OnceLock::new();

/// Return the process-wide database array singleton.
pub fn global_databases() -> &'static GlobalDatabases {
    GLOBAL_DATABASES.get_or_init(GlobalDatabases::new)
}

/// Retrieve the `Arc<Mutex<RedisDb>>` for the given logical database index.
///
/// Indices >= `MAX_DBS` are silently clamped to `MAX_DBS - 1`.
pub fn get_db(index: u32) -> Arc<Mutex<RedisDb>> {
    global_databases().get(index)
}
