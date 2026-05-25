//! Global per-client metadata snapshot for cross-thread `CLIENT LIST` queries.
//!
//! Each connection thread registers itself on accept and periodically updates
//! its entry after processing a read batch. The `CLIENT LIST` handler on any
//! thread can read a consistent snapshot of all live connections without
//! holding the db lock.
//!
//! Layout:
//!   * `ClientSnapshot` — immutable view of one client's current state.
//!   * `ClientInfoRegistry` — `ClientId → ClientSnapshot` map.
//!   * `client_info_registry()` — process-wide singleton accessor.
//!
//! Fields intentionally kept minimal: only what `CLIENT LIST` actually needs to
//! satisfy the upstream TCL test suite (id, addr, db, flags, cmd).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use redis_types::RedisString;

use crate::acl::AclUser;
use crate::client::Client;
use crate::client::ClientId;

/// A point-in-time snapshot of one client's observable state.
#[derive(Clone, Default)]
pub struct ClientSnapshot {
    pub id: ClientId,
    pub addr: String,
    pub db_index: u32,
    pub cmd: String,
    pub blocked: bool,
    pub name: Option<RedisString>,
    pub user: Option<RedisString>,
    pub resp_proto: i32,
    pub tracking: bool,
    pub tracking_bcast: bool,
    pub tracking_broken_redirect: bool,
    pub import_source: bool,
    pub capa_redirect: bool,
    pub is_replica: bool,
    pub readonly: bool,
    pub lib_name: Option<RedisString>,
    pub lib_ver: Option<RedisString>,
    pub subscribed_channels: usize,
    pub subscribed_patterns: usize,
    pub subscribed_shard_channels: usize,
    pub channel_names: Vec<RedisString>,
    pub pattern_names: Vec<RedisString>,
    pub shard_channel_names: Vec<RedisString>,
    pub queued_multi_count: Option<usize>,
    pub output_buffer_bytes: usize,
    pub query_buffer_bytes: usize,
    pub argv_memory_bytes: usize,
    pub multi_memory_bytes: usize,
    pub total_memory_bytes: usize,
    pub net_input_bytes: u64,
    pub net_output_bytes: u64,
    pub commands_processed: u64,
}

/// Server-wide client info table.
#[derive(Default)]
pub struct ClientInfoRegistry {
    entries: HashMap<ClientId, ClientSnapshot>,
    kill_marks: HashSet<ClientId>,
}

impl ClientInfoRegistry {
    fn new() -> Self {
        Self::default()
    }

    /// Register a freshly accepted connection.
    pub fn register(&mut self, id: ClientId, addr: String) {
        self.entries.insert(
            id,
            ClientSnapshot {
                id,
                addr,
                db_index: 0,
                cmd: String::new(),
                blocked: false,
                name: None,
                user: Some(RedisString::from_static(b"default")),
                resp_proto: 2,
                tracking: false,
                tracking_bcast: false,
                tracking_broken_redirect: false,
                import_source: false,
                capa_redirect: false,
                is_replica: false,
                readonly: false,
                lib_name: None,
                lib_ver: None,
                subscribed_channels: 0,
                subscribed_patterns: 0,
                subscribed_shard_channels: 0,
                channel_names: Vec::new(),
                pattern_names: Vec::new(),
                shard_channel_names: Vec::new(),
                queued_multi_count: None,
                output_buffer_bytes: 0,
                query_buffer_bytes: 0,
                argv_memory_bytes: 0,
                multi_memory_bytes: 0,
                total_memory_bytes: 0,
                net_input_bytes: 0,
                net_output_bytes: 0,
                commands_processed: 0,
            },
        );
    }

    /// Update the externally visible command/db/blocking snapshot for `id`.
    ///
    /// This is intentionally batch-oriented rather than called for every
    /// command in a pipeline. `CLIENT LIST` observes the last command completed
    /// by the connection, which is the useful stable state for diagnostics and
    /// avoids pushing a global mutex into every GET/SET hot path.
    pub fn update_snapshot(&mut self, id: ClientId, cmd: &[u8], db_index: u32, blocked: bool) {
        if let Some(e) = self.entries.get_mut(&id) {
            e.cmd = cmd.iter().map(|b| b.to_ascii_lowercase() as char).collect();
            e.db_index = db_index;
            e.blocked = blocked;
        }
    }

    /// Refresh the metadata fields that are not passed through the hot-path
    /// `update_snapshot` call.
    pub fn update_client_metadata(&mut self, client: &Client) {
        if let Some(e) = self.entries.get_mut(&client.id) {
            e.db_index = client.db_index;
            e.blocked = client.blocked_on_keys;
            e.name = client.name.clone();
            e.user = client.authenticated_user.clone();
            e.resp_proto = client.resp_proto;
            e.tracking = client.tracking.enabled;
            e.tracking_bcast = client.tracking.bcast;
            e.tracking_broken_redirect = client.tracking.broken_redirect;
            e.import_source = client.import_source;
            e.capa_redirect = client.capa_redirect;
            e.is_replica = client.is_replica;
            e.readonly = client.flags.readonly;
            e.lib_name = client.lib_name.clone();
            e.lib_ver = client.lib_ver.clone();
            e.subscribed_channels = client.subscribed_channels.len();
            e.subscribed_patterns = client.subscribed_patterns.len();
            e.subscribed_shard_channels = client.subscribed_shard_channels.len();
            e.channel_names = client.subscribed_channels.iter().cloned().collect();
            e.pattern_names = client.subscribed_patterns.iter().cloned().collect();
            e.shard_channel_names = client.subscribed_shard_channels.iter().cloned().collect();
            e.queued_multi_count = client.flags.multi.then_some(client.queued_argvs.len());
            e.net_input_bytes = client.net_input_bytes;
            e.net_output_bytes = client.net_output_bytes;
            e.commands_processed = client.commands_processed;
        }
    }

    pub fn set_output_buffer_memory(&mut self, id: ClientId, bytes: usize) {
        if let Some(e) = self.entries.get_mut(&id) {
            e.output_buffer_bytes = bytes;
        }
    }

    pub fn set_memory_usage(
        &mut self,
        id: ClientId,
        query_buffer_bytes: usize,
        argv_memory_bytes: usize,
        multi_memory_bytes: usize,
        total_memory_bytes: usize,
    ) {
        if let Some(e) = self.entries.get_mut(&id) {
            e.query_buffer_bytes = query_buffer_bytes;
            e.argv_memory_bytes = argv_memory_bytes;
            e.multi_memory_bytes = multi_memory_bytes;
            e.total_memory_bytes = total_memory_bytes;
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
        self.kill_marks.remove(&id);
    }

    /// Mark a connection for asynchronous teardown.
    ///
    /// The command handler cannot directly own another connection's socket in
    /// the thread-per-client runtime. Removing the snapshot makes CLIENT LIST
    /// observe Valkey-like immediate disappearance; the kill mark is consumed
    /// by the target connection loop before it processes the next read.
    pub fn mark_killed(&mut self, id: ClientId) {
        if self.entries.remove(&id).is_some() {
            self.kill_marks.insert(id);
        }
    }

    /// Return and clear the pending kill bit for `id`.
    pub fn take_killed(&mut self, id: ClientId) -> bool {
        self.kill_marks.remove(&id)
    }

    /// Remove pub/sub clients authenticated as `username` whose active
    /// subscriptions are no longer allowed by `updated_user`.
    pub fn deregister_revoked_pubsub_clients(
        &mut self,
        username: &RedisString,
        updated_user: &AclUser,
    ) -> Vec<ClientId> {
        let revoked: Vec<ClientId> = self
            .entries
            .values()
            .filter(|snap| snap.user.as_ref() == Some(username))
            .filter(|snap| snapshot_has_revoked_channel(snap, updated_user))
            .map(|snap| snap.id)
            .collect();
        for id in &revoked {
            self.entries.remove(id);
        }
        revoked
    }

    /// Snapshot of all currently registered clients.
    pub fn all(&self) -> Vec<ClientSnapshot> {
        let mut out: Vec<ClientSnapshot> = self.entries.values().cloned().collect();
        out.sort_by_key(|snap| snap.id);
        out
    }
}

fn snapshot_has_revoked_channel(snap: &ClientSnapshot, user: &AclUser) -> bool {
    snap.channel_names
        .iter()
        .any(|channel| !user.can_access_channel(channel.as_bytes()))
        || snap
            .shard_channel_names
            .iter()
            .any(|channel| !user.can_access_channel(channel.as_bytes()))
        || snap
            .pattern_names
            .iter()
            .any(|pattern| !user.can_access_channel_pattern(pattern.as_bytes()))
}

static CLIENT_INFO_REGISTRY: OnceLock<Arc<Mutex<ClientInfoRegistry>>> = OnceLock::new();

/// Return the process-wide client info registry singleton.
pub fn client_info_registry() -> &'static Arc<Mutex<ClientInfoRegistry>> {
    CLIENT_INFO_REGISTRY.get_or_init(|| Arc::new(Mutex::new(ClientInfoRegistry::new())))
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        networking.c CLIENT LIST/INFO support state
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         1
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Snapshot registry for observable per-client metadata; full
//                  Valkey client accounting remains broader than this model.
// ──────────────────────────────────────────────────────────────────────────
