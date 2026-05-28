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
use crate::listeners::*;
use crate::generated::{GeneratedCommandSpec, COMMANDS};
use crate::live_config_handle;

pub static SHUTDOWN_SIGNAL_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static SHUTDOWN_SIGNAL_NUMBER: AtomicI32 = AtomicI32::new(0);
pub static SHUTDOWN_PENDING: AtomicBool = AtomicBool::new(false);
pub static SHUTDOWN_SAVE_FAILED: AtomicBool = AtomicBool::new(false);
pub static SHUTDOWN_ON_SIGTERM_FORCE: AtomicBool = AtomicBool::new(false);
pub static DEBUG_PAUSE_CRON: AtomicBool = AtomicBool::new(false);

pub fn note_shutdown_signal(signal: i32) {
    SHUTDOWN_SIGNAL_NUMBER.store(signal, Ordering::SeqCst);
    SHUTDOWN_SIGNAL_COUNT.fetch_add(1, Ordering::SeqCst);
}

pub fn shutdown_signal_count() -> usize {
    SHUTDOWN_SIGNAL_COUNT.load(Ordering::SeqCst)
}

pub fn shutdown_signal_number() -> i32 {
    SHUTDOWN_SIGNAL_NUMBER.load(Ordering::SeqCst)
}

pub fn shutdown_pending() -> bool {
    SHUTDOWN_PENDING.load(Ordering::SeqCst)
}

pub fn set_shutdown_pending(value: bool) {
    SHUTDOWN_PENDING.store(value, Ordering::SeqCst);
}

pub fn abort_shutdown_pending() -> bool {
    SHUTDOWN_PENDING.swap(false, Ordering::SeqCst)
}

pub fn mark_shutdown_save_failed() {
    SHUTDOWN_SAVE_FAILED.store(true, Ordering::SeqCst);
}

pub fn shutdown_save_failed() -> bool {
    SHUTDOWN_SAVE_FAILED.load(Ordering::SeqCst)
}

pub fn shutdown_on_sigterm_force() -> bool {
    SHUTDOWN_ON_SIGTERM_FORCE.load(Ordering::SeqCst)
}

pub fn set_debug_pause_cron(value: bool) {
    DEBUG_PAUSE_CRON.store(value, Ordering::SeqCst);
}

pub fn debug_pause_cron() -> bool {
    DEBUG_PAUSE_CRON.load(Ordering::SeqCst)
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
