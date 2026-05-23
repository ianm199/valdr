//! Command registry and command implementations.
//!
//! Owners (per `harness/type-vocabulary.tsv`):
//!   - `CommandSpec` (spec.rs)
//!
//! The command table is generated from `reference/valkey/src/commands/*.json`
//! by `harness/gen-command-registry.py` (TODO). The generator's output
//! lives at `src/generated.rs` and must not be hand-edited.
//!
//! Pilot commands: PING, ECHO, HELLO, COMMAND (Phase 2); SET, GET, DEL,
//! EXISTS, INCR (Phase 3).

pub mod aof;
pub mod bitops;
pub mod bloom;
pub mod connection;
pub mod json;
pub mod dispatch;
pub mod eval;
pub mod geo;
pub mod geohash_geohash;
pub mod geohash_geohash_helper;
pub mod generated;
pub mod hash;
pub mod hyperloglog;
pub mod info;
pub mod list;
pub mod multi;
pub mod persist;
pub mod pubsub;
pub mod replica_dialer;
pub mod replication;
pub mod set;
pub mod slowlog_cmd;
pub mod sort;
pub mod stream;
pub mod string;
pub mod zset;

pub use dispatch::{dispatch, lookup_command, DispatchEntry, Handler};
pub use list::wake_blocked_after_swapdb;

use std::sync::{Arc, OnceLock};

use redis_core::live_config::LiveConfig;

/// Process-wide handle to the live config, registered once at startup.
///
/// `connection::config_command` writes through this handle so CONFIG SET takes
/// effect immediately even when the writing connection is not the one reading
/// (live state is shared, not per-context). The accept loop installs the same
/// `Arc<LiveConfig>` it shares with `RedisServer` via
/// [`install_live_config_handle`].
static LIVE_CONFIG_HANDLE: OnceLock<Arc<LiveConfig>> = OnceLock::new();

/// Install the process-wide live config. Idempotent.
pub fn install_live_config_handle(config: Arc<LiveConfig>) {
    let _ = LIVE_CONFIG_HANDLE.set(config);
}

/// Return the active live config. Falls back to a fresh default `LiveConfig`
/// when no install has happened (unit tests).
pub fn live_config_handle() -> Arc<LiveConfig> {
    LIVE_CONFIG_HANDLE
        .get_or_init(|| Arc::new(LiveConfig::new()))
        .clone()
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        (none — scaffolding placeholder)
//   target_crate:  redis-commands
//   confidence:    skeleton
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         scaffolding; gen-command-registry.py is the first deliverable
// ──────────────────────────────────────────────────────────────────────────
