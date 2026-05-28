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
use crate::config_cmd::*;
use crate::listeners::*;
use crate::shutdown_signals::*;
use crate::generated::{GeneratedCommandSpec, COMMANDS};
use crate::live_config_handle;

pub static RDB_KEY_SAVE_DELAY_US: AtomicU64 = AtomicU64::new(0);

pub static CLIENT_OBUF_LIMITS: OnceLock<Mutex<ClientOutputBufferLimits>> = OnceLock::new();
pub static CLIENT_OBUF_LIMIT_VERSION: AtomicU64 = AtomicU64::new(0);
pub static CLIENT_OBUF_NORMAL_HARD: AtomicUsize = AtomicUsize::new(0);
pub static CLIENT_OBUF_NORMAL_SOFT: AtomicUsize = AtomicUsize::new(0);
pub static CLIENT_OBUF_NORMAL_SOFT_SECONDS: AtomicU64 = AtomicU64::new(0);
pub static CLIENT_OBUF_REPLICA_HARD: AtomicUsize = AtomicUsize::new(256 * 1024 * 1024);
pub static CLIENT_OBUF_REPLICA_SOFT: AtomicUsize = AtomicUsize::new(64 * 1024 * 1024);
pub static CLIENT_OBUF_REPLICA_SOFT_SECONDS: AtomicU64 = AtomicU64::new(60);
pub static CLIENT_OBUF_PUBSUB_HARD: AtomicUsize = AtomicUsize::new(32 * 1024 * 1024);
pub static CLIENT_OBUF_PUBSUB_SOFT: AtomicUsize = AtomicUsize::new(8 * 1024 * 1024);
pub static CLIENT_OBUF_PUBSUB_SOFT_SECONDS: AtomicU64 = AtomicU64::new(60);
pub static CLIENT_QUERY_BUFFER_LIMIT: AtomicUsize = AtomicUsize::new(1024 * 1024 * 1024);


#[derive(Clone, Copy)]
pub struct ClientOutputBufferLimit {
    pub hard: usize,
    pub soft: usize,
    pub soft_seconds: u64,
}

#[derive(Clone, Copy)]
pub struct ClientOutputBufferLimits {
    pub normal: ClientOutputBufferLimit,
    pub replica: ClientOutputBufferLimit,
    pub pubsub: ClientOutputBufferLimit,
}

impl Default for ClientOutputBufferLimits {
    fn default() -> Self {
        Self {
            normal: ClientOutputBufferLimit {
                hard: 0,
                soft: 0,
                soft_seconds: 0,
            },
            replica: ClientOutputBufferLimit {
                hard: 256 * 1024 * 1024,
                soft: 64 * 1024 * 1024,
                soft_seconds: 60,
            },
            pubsub: ClientOutputBufferLimit {
                hard: 32 * 1024 * 1024,
                soft: 8 * 1024 * 1024,
                soft_seconds: 60,
            },
        }
    }
}

pub(crate) fn monitor_clients() -> &'static Mutex<HashMap<u64, Sender<Vec<u8>>>> {
    MONITOR_CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn aclfile_config_cell() -> &'static Mutex<Option<String>> {
    ACLFILE_CONFIG.get_or_init(|| Mutex::new(None))
}

pub fn client_obuf_limits_cell() -> &'static Mutex<ClientOutputBufferLimits> {
    CLIENT_OBUF_LIMITS.get_or_init(|| Mutex::new(ClientOutputBufferLimits::default()))
}

pub fn store_client_obuf_limit_snapshot(next: ClientOutputBufferLimits) {
    CLIENT_OBUF_LIMIT_VERSION.fetch_add(1, Ordering::Release);
    CLIENT_OBUF_NORMAL_HARD.store(next.normal.hard, Ordering::Relaxed);
    CLIENT_OBUF_NORMAL_SOFT.store(next.normal.soft, Ordering::Relaxed);
    CLIENT_OBUF_NORMAL_SOFT_SECONDS.store(next.normal.soft_seconds, Ordering::Relaxed);
    CLIENT_OBUF_REPLICA_HARD.store(next.replica.hard, Ordering::Relaxed);
    CLIENT_OBUF_REPLICA_SOFT.store(next.replica.soft, Ordering::Relaxed);
    CLIENT_OBUF_REPLICA_SOFT_SECONDS.store(next.replica.soft_seconds, Ordering::Relaxed);
    CLIENT_OBUF_PUBSUB_HARD.store(next.pubsub.hard, Ordering::Relaxed);
    CLIENT_OBUF_PUBSUB_SOFT.store(next.pubsub.soft, Ordering::Relaxed);
    CLIENT_OBUF_PUBSUB_SOFT_SECONDS.store(next.pubsub.soft_seconds, Ordering::Relaxed);
    CLIENT_OBUF_LIMIT_VERSION.fetch_add(1, Ordering::Release);
}

pub fn load_client_obuf_limit_snapshot(is_pubsub: bool) -> ClientOutputBufferLimit {
    loop {
        let before = CLIENT_OBUF_LIMIT_VERSION.load(Ordering::Acquire);
        if before & 1 != 0 {
            std::hint::spin_loop();
            continue;
        }
        let limit = if is_pubsub {
            ClientOutputBufferLimit {
                hard: CLIENT_OBUF_PUBSUB_HARD.load(Ordering::Relaxed),
                soft: CLIENT_OBUF_PUBSUB_SOFT.load(Ordering::Relaxed),
                soft_seconds: CLIENT_OBUF_PUBSUB_SOFT_SECONDS.load(Ordering::Relaxed),
            }
        } else {
            ClientOutputBufferLimit {
                hard: CLIENT_OBUF_NORMAL_HARD.load(Ordering::Relaxed),
                soft: CLIENT_OBUF_NORMAL_SOFT.load(Ordering::Relaxed),
                soft_seconds: CLIENT_OBUF_NORMAL_SOFT_SECONDS.load(Ordering::Relaxed),
            }
        };
        if before == CLIENT_OBUF_LIMIT_VERSION.load(Ordering::Acquire) {
            return limit;
        }
    }
}


pub fn client_output_buffer_hard_limit(is_pubsub: bool) -> usize {
    client_output_buffer_limit(is_pubsub).hard
}

pub fn client_query_buffer_limit() -> usize {
    CLIENT_QUERY_BUFFER_LIMIT.load(Ordering::Relaxed)
}

pub fn rdb_key_save_delay_us() -> u64 {
    RDB_KEY_SAVE_DELAY_US.load(Ordering::Relaxed)
}



pub fn set_client_query_buffer_limit(limit: usize) {
    CLIENT_QUERY_BUFFER_LIMIT.store(limit, Ordering::Relaxed);
}

pub fn client_output_buffer_limit(is_pubsub: bool) -> ClientOutputBufferLimit {
    load_client_obuf_limit_snapshot(is_pubsub)
}

pub fn client_output_buffer_limit_config_string() -> String {
    let guard = match client_obuf_limits_cell().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    format!(
        "normal {} {} {} slave {} {} {} pubsub {} {} {}",
        guard.normal.hard,
        guard.normal.soft,
        guard.normal.soft_seconds,
        guard.replica.hard,
        guard.replica.soft,
        guard.replica.soft_seconds,
        guard.pubsub.hard,
        guard.pubsub.soft,
        guard.pubsub.soft_seconds,
    )
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
