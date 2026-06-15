//! Command registry and command implementations.
//! Owners (per `harness/type-vocabulary.tsv`):
//! - `CommandSpec` (spec.rs)
//! The command table is generated from `reference/valkey/src/commands/*.json`
//! by `harness/gen-command-registry.py`. The generator's output lives at
//! `src/generated.rs` and must not be hand-edited. Hand-written modules in
//! this crate own command behavior, replication-facing command effects, and
//! scripting integration used by the server runtime.

pub mod acl_cmd;
pub mod aof;
pub mod bitops;
pub mod bloom;
pub mod client_cmd;
pub mod client_limits;
pub mod cluster;
pub mod command_meta;
pub mod config_cmd;
pub mod connection;
pub mod debug_cmd;
pub mod dispatch;
pub mod eval;
pub mod generated;
pub mod geo;
pub mod geohash_geohash;
pub mod geohash_geohash_helper;
pub mod hash;
pub mod hyperloglog;
pub mod info;
pub mod json;
pub mod list;
pub mod listeners;
pub mod multi;
pub mod persist;
pub mod pubsub;
pub mod replica_dialer;
pub mod replication;
pub mod set;
pub mod shutdown_signals;
pub mod slowlog_cmd;
pub mod sort;
pub mod stream;
pub mod string;
pub mod vector;
pub mod zset;

pub use dispatch::{dispatch, lookup_command, DispatchEntry, Handler};
pub use list::wake_blocked_after_swapdb;

use std::sync::{Arc, OnceLock};

use redis_core::live_config::LiveConfig;

/// Process-wide handle to the live config, registered once at startup.
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

// PORT STATUS: active command surface. Keep unresolved command-family work in
// the owning modules with TODO(port)/TODO(architect) markers instead of a
// crate-level placeholder label.
