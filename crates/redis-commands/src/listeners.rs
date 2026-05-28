//! AUTO-EXTRACTED from connection.rs by refactor/file-structure-splits.
//! Module-level doc lives in lib.rs.
#![allow(unused_imports, dead_code, unused_variables, unused_mut)]

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::acl::{
    acl_log_entries, acl_log_max_len, acl_log_now_millis, acl_pubsub_default_config_value,
    apply_acl_pubsub_default_to_user, category as acl_category, category_name_to_bit,
    clear_acl_log, global_acl_state, hex_to_hash, record_acl_log_entry, set_acl_log_max_len,
    set_acl_pubsub_default, sha256_hash, AclKeyPattern, AclLogEntry, AclUser, ACL_KEY_READ,
    ACL_KEY_READ_WRITE, ACL_KEY_WRITE, ALL_CATEGORY_NAMES,
};
use redis_core::blocked_keys::{blocked_keys_index, BlockedAction};
use redis_core::client_info::client_info_registry;
use redis_core::eviction::{try_evict_to_fit, EvictionOutcome};
use redis_core::live_config::{LiveConfig, MaxmemoryPolicyCode};
use redis_core::metrics::{
    record_acl_access_denied_auth, record_blocked_command_rejected, record_error_reply,
    server_metrics,
};
use redis_core::networking::{
    client_matches_ip_filter, validate_client_capa_filter, validate_client_flag_filter,
};
use redis_core::notify::{keyspace_events_string_to_flags, NOTIFY_EVICTED};
use redis_core::object::object_compute_size;
use redis_core::{CommandContext, PersistenceStatus, RedisDb};
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};
use serde_json::Value;

use crate::connection::*;
use crate::client_limits::*;
use crate::config_cmd::*;
use crate::shutdown_signals::*;
use crate::generated::{GeneratedCommandSpec, COMMANDS};
use crate::live_config_handle;

pub type TcpPortSetHook = dyn Fn(u16) -> Result<Vec<TcpListener>, Vec<u8>> + Send + Sync + 'static;
pub type TcpBindSetHook =
    dyn Fn(&[u8], u16) -> Result<Vec<TcpListener>, Vec<u8>> + Send + Sync + 'static;
pub static TCP_PORT_SET_HOOK: OnceLock<Box<TcpPortSetHook>> = OnceLock::new();
pub static TCP_BIND_SET_HOOK: OnceLock<Box<TcpBindSetHook>> = OnceLock::new();
pub static PENDING_TCP_LISTENERS: OnceLock<Mutex<Vec<TcpListener>>> = OnceLock::new();
pub static PENDING_TCP_LISTENER_REPLACEMENT: OnceLock<Mutex<Option<Vec<TcpListener>>>> =
    OnceLock::new();
pub static TCP_PORT_CONFIG: AtomicU16 = AtomicU16::new(0);

pub fn set_tcp_port_config(port: u16) {
    TCP_PORT_CONFIG.store(port, Ordering::Relaxed);
}

pub fn tcp_port_config() -> u16 {
    TCP_PORT_CONFIG.load(Ordering::Relaxed)
}

pub fn install_tcp_port_set_hook(hook: Box<TcpPortSetHook>) {
    let _ = TCP_PORT_SET_HOOK.set(hook);
}

pub fn install_tcp_bind_set_hook(hook: Box<TcpBindSetHook>) {
    let _ = TCP_BIND_SET_HOOK.set(hook);
}

pub fn drain_pending_tcp_listeners() -> Vec<TcpListener> {
    let Some(cell) = PENDING_TCP_LISTENERS.get() else {
        return Vec::new();
    };
    let mut guard = match cell.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    std::mem::take(&mut *guard)
}

pub fn drain_pending_tcp_listener_replacement() -> Option<Vec<TcpListener>> {
    let cell = PENDING_TCP_LISTENER_REPLACEMENT.get()?;
    let mut guard = match cell.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.take()
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        extracted from connection.rs (refactor/file-structure-splits)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Extracted from the 7,184-LOC god-file. Re-exports in
//                  connection.rs keep external paths working.
// ──────────────────────────────────────────────────────────────────────────
