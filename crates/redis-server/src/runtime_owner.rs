//! RuntimeOwner mio readiness plain-TCP path.
//! This module names the owner-loop vocabulary
//! `harness/architecture/object-vocabulary.tsv` and implements the bounded
//! `mio` owner loop approved by
//! `harness/architecture/decisions/runtime-ownership.md`.
//! RuntimeOwner owns accepted plain-TCP sockets, client parser state, per-slot
//! foreign payload receivers, ordinary reply flushing, and the live
//! `Vec<RedisDb>` used by normal command execution. Commands still enter
//! `redis_commands::dispatch` through `CommandContext`; the context DB-list
//! route points at the owner-held DB slice instead of `global_databases`.

use std::collections::{HashSet, VecDeque};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use mio::{Events, Interest, Poll, Registry as MioRegistry, Token};
use redis_core::client_info::client_info_registry;
use redis_core::conn_tls::{session_read_pump, session_write_pump};
use redis_core::db::RedisDb;
use redis_core::eviction::{try_evict_to_fit, EvictionOutcome};
use redis_core::expire::run_active_expire_tick_on_db;
use redis_core::memory::approximate_memory_used;
use redis_core::metrics::server_metrics;
use redis_core::networking::{
    current_paused_actions, get_client_eviction_limit, note_pause_postponed_client,
    note_pause_resumed_client, pause_postponed_client_count, PAUSE_ACTION_CLIENT_ALL,
    PAUSE_ACTION_CLIENT_WRITE, PAUSE_ACTION_EVICT, PAUSE_ACTION_EXPIRE,
};
use redis_core::pubsub_registry::PubSubRegistry;
use redis_core::{Client, Connection};
use redis_protocol::parse_inline_or_multibulk_into;
use redis_types::{RedisError, RedisString};
use rustls::{ServerConfig, ServerConnection};

pub const DEFAULT_DATABASE_COUNT: u32 = 16;
const DEFAULT_EVENT_CAPACITY: usize = 1024;
const READ_BUFFER_SIZE: usize = 16 * 1024;
const MAX_COMMANDS_PER_SLOT_TICK: usize = 128;
const MAX_LISTENER_TOKENS: usize = 16;
const SLOT_TOKEN_BASE: usize = MAX_LISTENER_TOKENS;
const POLL_TIMEOUT: Duration = Duration::from_millis(2);
const ACTIVE_EXPIRE_FALLBACK_INTERVAL: Duration = Duration::from_millis(100);
const ACTIVE_EXPIRE_DBS_PER_STEP: usize = 16;

/// Typed key into the future RuntimeOwner client-slot table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SlotId(u32);

impl SlotId {
    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    fn from_index(index: usize) -> Option<Self> {
        u32::try_from(index).ok().map(Self)
    }

    fn as_index(self) -> usize {
        self.0 as usize
    }
}

/// Readiness-poller handle for the owner-loop path.
/// `runtime-owner-8-mio-poller-owner-loop` installs `mio` for plain TCP only.
/// TLS remains on the existing thread-per-connection path, and raw platform
/// poller code stays outside this packet.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PollDriverHandle {
    installed: bool,
    epoch: u64,
}

impl PollDriverHandle {
    pub const fn abstract_placeholder() -> Self {
        Self {
            installed: false,
            epoch: 0,
        }
    }

    pub const fn mio(epoch: u64) -> Self {
        Self {
            installed: true,
            epoch,
        }
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub const fn is_installed(self) -> bool {
        self.installed
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub const fn epoch(self) -> u64 {
        self.epoch
    }
}

/// Typed knobs for owner-loop experiments.
/// `enabled` defaults to false. Constructing this value does not change
/// accept loop, command dispatch, or database ownership in `main.rs`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeOwnerConfig {
    enabled: bool,
    database_count: u32,
    max_pending_events: usize,
}

impl RuntimeOwnerConfig {
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            database_count: DEFAULT_DATABASE_COUNT,
            max_pending_events: DEFAULT_EVENT_CAPACITY,
        }
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub const fn enabled(self) -> bool {
        self.enabled
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub const fn database_count(self) -> u32 {
        self.database_count
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub const fn max_pending_events(self) -> usize {
        self.max_pending_events
    }

    pub const fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub const fn with_database_count(mut self, database_count: u32) -> Self {
        self.database_count = database_count;
        self
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub const fn with_max_pending_events(mut self, max_pending_events: usize) -> Self {
        self.max_pending_events = max_pending_events;
        self
    }
}

impl Default for RuntimeOwnerConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Per-slot outbound bytes drained by the owner write step.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ClientWriteBuffer {
    bytes: Vec<u8>,
    consumed: usize,
}

impl ClientWriteBuffer {
    pub fn new() -> Self {
        Self {
            bytes: Vec::new(),
            consumed: 0,
        }
    }

    pub fn append(&mut self, bytes: &[u8]) {
        self.compact_if_empty();
        self.bytes.extend_from_slice(bytes);
    }

    pub fn append_owned(&mut self, mut bytes: Vec<u8>) {
        if bytes.is_empty() {
            return;
        }
        self.compact_if_empty();
        if self.bytes.is_empty() {
            self.bytes = bytes;
        } else {
            self.bytes.append(&mut bytes);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.consumed >= self.bytes.len()
    }

    pub fn len(&self) -> usize {
        self.bytes.len().saturating_sub(self.consumed)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[self.consumed..]
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn take(&mut self) -> Vec<u8> {
        if self.consumed == 0 {
            return std::mem::take(&mut self.bytes);
        }
        let pending = self.bytes.split_off(self.consumed);
        self.bytes.clear();
        self.consumed = 0;
        pending
    }

    pub fn consume_front(&mut self, n: usize) {
        let n = n.min(self.len());
        if n == self.len() {
            self.bytes.clear();
            self.consumed = 0;
        } else if n > 0 {
            self.consumed += n;
        }
    }

    fn compact_if_empty(&mut self) {
        if self.consumed >= self.bytes.len() {
            self.bytes.clear();
            self.consumed = 0;
        }
    }
}

/// Owner-loop representation of one connected client.
pub struct ClientSlot {
    id: SlotId,
    client: Client,
    stream: Option<MioTcpStream>,
    foreign_rx: Option<Receiver<Vec<u8>>>,
    write_buffer: ClientWriteBuffer,
    output_accounted_bytes: usize,
    output_reported_bytes: usize,
    query_buffer_reported_bytes: usize,
    last_client_activity: Instant,
    last_client_info_memory_refresh: Instant,
    pause_postponed: bool,
    obuf_soft_limit_since: Option<Instant>,
    writable_interest: bool,
    closed: bool,
    close_after_flush: bool,
    debug_loadaof_pending: bool,
    /// Present iff this is a TLS connection: the rustls server session layered
    /// over `stream`. `None` for plain TCP (the untouched fast path).
    tls: Option<Box<ServerConnection>>,
    /// TLS handshake completed — only then is `stream` carrying app data.
    tls_handshake_done: bool,
}

impl ClientSlot {
    pub fn new(id: SlotId, client: Client) -> Self {
        let now = Instant::now();
        Self {
            id,
            client,
            stream: None,
            foreign_rx: None,
            write_buffer: ClientWriteBuffer::new(),
            output_accounted_bytes: 0,
            output_reported_bytes: 0,
            query_buffer_reported_bytes: 0,
            last_client_activity: now,
            last_client_info_memory_refresh: now
                .checked_sub(CLIENT_INFO_MEMORY_REFRESH_INTERVAL)
                .unwrap_or(now),
            pause_postponed: false,
            obuf_soft_limit_since: None,
            writable_interest: false,
            closed: false,
            close_after_flush: false,
            debug_loadaof_pending: false,
            tls: None,
            tls_handshake_done: false,
        }
    }

    fn with_stream(
        id: SlotId,
        client: Client,
        stream: MioTcpStream,
        foreign_rx: Receiver<Vec<u8>>,
    ) -> Self {
        let now = Instant::now();
        Self {
            id,
            client,
            stream: Some(stream),
            foreign_rx: Some(foreign_rx),
            write_buffer: ClientWriteBuffer::new(),
            output_accounted_bytes: 0,
            output_reported_bytes: 0,
            query_buffer_reported_bytes: 0,
            last_client_activity: now,
            last_client_info_memory_refresh: now
                .checked_sub(CLIENT_INFO_MEMORY_REFRESH_INTERVAL)
                .unwrap_or(now),
            pause_postponed: false,
            obuf_soft_limit_since: None,
            writable_interest: false,
            closed: false,
            close_after_flush: false,
            debug_loadaof_pending: false,
            tls: None,
            tls_handshake_done: false,
        }
    }

    pub fn id(&self) -> SlotId {
        self.id
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn client(&self) -> &Client {
        &self.client
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    pub fn ingest(&mut self, bytes: &[u8]) {
        self.client.query_buf.extend_from_slice(bytes);
        self.last_client_activity = Instant::now();
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn query_buffer(&self) -> &[u8] {
        &self.client.query_buf
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn clear_query_buffer(&mut self) {
        self.client.query_buf.clear();
    }

    fn observe_incomplete_query_buffer(&mut self) {
        let estimate = estimated_query_buffer_allocation(&self.client.query_buf, 0);
        if estimate > 0 {
            self.query_buffer_reported_bytes = self.query_buffer_reported_bytes.max(estimate);
            self.last_client_activity = Instant::now();
        }
    }

    fn observe_completed_query_buffer(&mut self, consumed_total: usize) {
        if consumed_total == 0 {
            return;
        }
        if consumed_total > QUERY_BUFFER_RESIZE_THRESHOLD || self.query_buffer_reported_bytes > 0 {
            self.query_buffer_reported_bytes =
                estimated_query_buffer_allocation(&self.client.query_buf, consumed_total);
        }
        self.last_client_activity = Instant::now();
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn stage_argv(&mut self, argv: Vec<RedisString>) {
        self.client.argv = argv;
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn argv(&self) -> &[RedisString] {
        &self.client.argv
    }

    pub fn queue_write(&mut self, bytes: &[u8]) {
        self.output_accounted_bytes = self.output_accounted_bytes.saturating_add(bytes.len());
        self.write_buffer.append(bytes);
        self.refresh_output_buffer_state();
    }

    fn queue_write_owned(&mut self, bytes: Vec<u8>) {
        self.output_accounted_bytes = self.output_accounted_bytes.saturating_add(bytes.len());
        self.write_buffer.append_owned(bytes);
        self.refresh_output_buffer_state();
    }

    fn queue_client_reply_preserving_capacity(&mut self) -> bool {
        if self.client.reply_buf.is_empty() {
            return false;
        }
        self.output_accounted_bytes = self
            .output_accounted_bytes
            .saturating_add(self.client.reply_buf.len());
        self.write_buffer.append(&self.client.reply_buf);
        self.client.reply_buf.clear();
        self.check_output_buffer_limits();
        true
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn pending_write_len(&self) -> usize {
        self.write_buffer.len()
    }

    pub fn take_pending_write(&mut self) -> Vec<u8> {
        self.write_buffer.take()
    }

    pub fn mark_closed(&mut self) {
        self.closed = true;
    }

    fn mark_closed_by_output_buffer_limit(&mut self) {
        if !self.closed {
            server_metrics()
                .client_output_buffer_limit_disconnections
                .fetch_add(1, Ordering::Relaxed);
        }
        self.mark_closed();
    }

    fn refresh_output_buffer_state(&mut self) {
        self.refresh_output_buffer_state_at(Instant::now());
    }

    fn refresh_output_buffer_state_at(&mut self, now: Instant) {
        let pending = self.output_accounted_bytes;
        self.publish_output_buffer_memory(pending);
        self.check_output_buffer_limits_at(now);
    }

    fn publish_output_buffer_memory(&mut self, pending: usize) {
        if pending == self.output_reported_bytes {
            return;
        }
        if let Ok(mut guard) = client_info_registry().lock() {
            guard.set_output_buffer_memory(self.client.id, pending);
            self.output_reported_bytes = pending;
        }
    }

    fn check_output_buffer_limits(&mut self) {
        self.check_output_buffer_limits_at(Instant::now());
    }

    fn check_output_buffer_limits_at(&mut self, now: Instant) {
        let pending = self.output_accounted_bytes;
        self.refresh_client_memory_snapshot_at(now);
        let limit =
            redis_commands::connection::client_output_buffer_limit(self.client.in_pubsub_mode());
        if limit.hard > 0 && pending > limit.hard {
            self.mark_closed_by_output_buffer_limit();
            return;
        }
        if limit.soft > 0 && limit.soft_seconds > 0 && pending > limit.soft {
            let since = *self.obuf_soft_limit_since.get_or_insert(now);
            if now.duration_since(since) >= Duration::from_secs(limit.soft_seconds) {
                self.mark_closed_by_output_buffer_limit();
            }
        } else {
            self.obuf_soft_limit_since = None;
        }
    }

    fn reconcile_output_buffer_after_write(&mut self) {
        self.output_accounted_bytes = self.write_buffer.len();
        self.check_output_buffer_limits();
        let pending = self.output_accounted_bytes;
        if pending > 0 || self.output_reported_bytes != 0 {
            self.publish_output_buffer_memory(pending);
        }
    }

    fn note_written_bytes(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.client.net_output_bytes = self.client.net_output_bytes.saturating_add(bytes as u64);
        if self.client.is_replica {
            redis_core::replication::global_replication_state()
                .account_replica_output_drained(self.client.id, bytes);
        }
    }

    fn publish_client_metadata(&self) {
        if let Ok(mut guard) = client_info_registry().lock() {
            guard.update_client_metadata(&self.client);
        }
    }

    fn mark_close_after_flush(&mut self) {
        self.close_after_flush = true;
    }

    fn mark_pause_postponed(&mut self) {
        if !self.pause_postponed {
            self.pause_postponed = true;
            note_pause_postponed_client();
        }
    }

    fn clear_pause_postponed(&mut self) {
        if self.pause_postponed {
            self.pause_postponed = false;
            note_pause_resumed_client();
        }
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    fn client_memory_usage(&self) -> usize {
        self.client_memory_usage_with_query_len(self.client.query_buf.len())
    }

    fn client_memory_usage_with_query_len(&self, query_len: usize) -> usize {
        let argv_mem = self.current_argv_memory_usage();
        let multi_mem = self.multi_memory_usage();
        self.output_accounted_bytes
            .saturating_add(query_len)
            .saturating_add(argv_mem)
            .saturating_add(multi_mem)
            .saturating_add(self.subscription_memory_usage())
            .saturating_add(self.tracking_memory_usage())
            .saturating_add(self.watched_key_memory_usage())
            .saturating_add(self.name_memory_usage())
    }

    fn reported_query_buffer_bytes(&mut self) -> usize {
        if self.query_buffer_reported_bytes > 0
            && self.last_client_activity.elapsed() >= QUERY_BUFFER_IDLE_SHRINK_AFTER
        {
            self.query_buffer_reported_bytes =
                if self.query_buffer_reported_bytes > QUERY_BUFFER_RESIZE_THRESHOLD {
                    QUERY_BUFFER_IOBUF_LEN
                } else if self.client.query_buf.is_empty() {
                    0
                } else {
                    self.query_buffer_reported_bytes
                };
        }
        self.query_buffer_reported_bytes
    }

    fn refresh_client_memory_snapshot(&mut self) {
        let now = Instant::now();
        self.refresh_client_memory_snapshot_at(now);
    }

    fn refresh_client_memory_snapshot_at(&mut self, now: Instant) {
        if now.duration_since(self.last_client_info_memory_refresh)
            < CLIENT_INFO_MEMORY_REFRESH_INTERVAL
        {
            return;
        }
        self.last_client_info_memory_refresh = now;
        let reported_query_buffer_bytes = self.reported_query_buffer_bytes();
        if let Ok(mut guard) = client_info_registry().lock() {
            guard.set_memory_usage(
                self.client.id,
                reported_query_buffer_bytes,
                self.current_argv_memory_usage(),
                self.multi_memory_usage(),
                self.client_memory_usage(),
            );
            guard.set_idle_seconds(
                self.client.id,
                now.duration_since(self.last_client_activity).as_secs(),
            );
        }
    }

    fn current_argv_memory_usage(&self) -> usize {
        self.client.argv.iter().map(|s| s.as_bytes().len()).sum()
    }

    fn queued_argv_memory_usage(&self) -> usize {
        self.client
            .queued_argvs
            .iter()
            .flat_map(|argv| argv.iter())
            .map(|s| s.as_bytes().len())
            .sum()
    }

    fn multi_memory_usage(&self) -> usize {
        const WATCH_OVERHEAD: usize = 64;

        self.queued_argv_memory_usage().saturating_add(
            self.client
                .mstate
                .as_ref()
                .map(|m| {
                    m.argv_len_sums
                        + m.watched_keys.len() * WATCH_OVERHEAD
                        + m.watched_keys
                            .iter()
                            .map(|w| w.key.string_bytes().len())
                            .sum::<usize>()
                })
                .unwrap_or(0),
        )
    }

    fn subscription_memory_usage(&self) -> usize {
        const SUBSCRIPTION_OVERHEAD: usize = 64;
        let subscription_count = self.client.subscribed_channels.len()
            + self.client.subscribed_patterns.len()
            + self.client.subscribed_shard_channels.len();
        subscription_count * SUBSCRIPTION_OVERHEAD
            + self
                .client
                .subscribed_channels
                .iter()
                .chain(self.client.subscribed_patterns.iter())
                .chain(self.client.subscribed_shard_channels.iter())
                .map(|s| s.as_bytes().len())
                .sum::<usize>()
    }

    fn tracking_memory_usage(&self) -> usize {
        const TRACKING_PREFIX_OVERHEAD: usize = 64;
        self.client.tracking.prefixes.len() * TRACKING_PREFIX_OVERHEAD
            + self
                .client
                .tracking
                .prefixes
                .iter()
                .map(|s| s.as_bytes().len())
                .sum::<usize>()
    }

    fn name_memory_usage(&self) -> usize {
        self.client
            .name
            .as_ref()
            .map(|s| s.as_bytes().len())
            .unwrap_or(0)
    }

    fn watched_key_memory_usage(&self) -> usize {
        const WATCH_OVERHEAD: usize = 64;
        let idx = redis_core::db::watched_keys_index();
        let guard = match idx.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard
            .watched
            .iter()
            .filter(|(_, watchers)| watchers.contains(&self.client.id))
            .map(|((_, key), _)| WATCH_OVERHEAD + key.as_bytes().len())
            .sum()
    }

    fn client_memory_usage_after_parsed_command(&self, consumed_total: usize) -> usize {
        let remaining_query = self.client.query_buf.len().saturating_sub(consumed_total);
        self.client_memory_usage_with_query_len(remaining_query)
    }

    fn can_be_evicted_for_memory(&self) -> bool {
        !self.closed && !self.close_after_flush && !self.client.flags.no_evict
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn into_client(self) -> Client {
        self.client
    }
}

/// Single ordered event stream from background subsystems into the owner.
#[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeEvent {
    Publish {
        channel: RedisString,
        payload: RedisString,
    },
    WakeBlocked {
        slot_id: SlotId,
        reason: RedisString,
    },
    Expire {
        db_index: u32,
        key: RedisString,
    },
    ReplicaAck {
        offset: i64,
    },
    AofFlushRequested,
    BackgroundChildDone {
        kind: RedisString,
        status: i32,
    },
    ShutdownRequested,
}

/// Outcome of one owner-loop command dispatch.
#[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerCommandResult {
    Replied { slot_id: SlotId },
    Blocked { slot_id: SlotId },
    Closed { slot_id: SlotId },
    PendingMore { slot_id: SlotId },
}

impl OwnerCommandResult {
    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn slot_id(self) -> SlotId {
        match self {
            Self::Replied { slot_id }
            | Self::Blocked { slot_id }
            | Self::Closed { slot_id }
            | Self::PendingMore { slot_id } => slot_id,
        }
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Closed { .. })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SlotDispatchOutcome {
    progressed: bool,
    queued_write: bool,
    reschedule: bool,
}

type DebugLoadAofResult = io::Result<(Vec<RedisDb>, Option<(usize, u64)>)>;

struct DebugLoadAofJob {
    slot_id: SlotId,
    rx: Receiver<DebugLoadAofResult>,
}

/// Owner of normal plain-TCP command execution for the mio readiness path.
pub struct RuntimeOwner {
    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    config: RuntimeOwnerConfig,
    poll_driver: PollDriverHandle,
    slots: Vec<Option<ClientSlot>>,
    free_slots: Vec<SlotId>,
    continuation_queue: VecDeque<SlotId>,
    queued_continuations: HashSet<SlotId>,
    dbs: Vec<RedisDb>,
    active_expire_cursor: usize,
    last_active_expire: Instant,
    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    events: VecDeque<RuntimeEvent>,
    replica_apply_rx: Option<Receiver<redis_commands::replica_dialer::ReplicaApplyRequest>>,
    debug_loadaof_jobs: Vec<DebugLoadAofJob>,
    replica_apply_db_index: u32,
    /// rustls server config for TLS listeners; `None` when TLS is disabled.
    tls_config: Option<Arc<ServerConfig>>,
    /// First listener-token index that is a TLS listener. Tokens
    /// `tls_listener_start..listeners.len` accept TLS; below it, plain TCP.
    tls_listener_start: usize,
}

impl RuntimeOwner {
    /// `new` inlines the database-creation logic directly so there is no
    /// separate free `create_databases` helper needed.
    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn new(config: RuntimeOwnerConfig) -> Self {
        let count = config.database_count().max(1);
        let dbs: Vec<RedisDb> = (0..count).map(RedisDb::new).collect();
        Self::with_databases(config, dbs)
    }

    pub fn with_databases(config: RuntimeOwnerConfig, mut dbs: Vec<RedisDb>) -> Self {
        if dbs.is_empty() {
            dbs.push(RedisDb::new(0));
        }
        for (idx, db) in dbs.iter_mut().enumerate() {
            db.id = idx as u32;
        }
        Self {
            config,
            poll_driver: PollDriverHandle::abstract_placeholder(),
            slots: Vec::new(),
            free_slots: Vec::new(),
            continuation_queue: VecDeque::new(),
            queued_continuations: HashSet::new(),
            dbs,
            active_expire_cursor: 0,
            last_active_expire: Instant::now(),
            events: VecDeque::new(),
            replica_apply_rx: None,
            debug_loadaof_jobs: Vec::new(),
            replica_apply_db_index: 0,
            tls_config: None,
            tls_listener_start: usize::MAX,
        }
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn disabled() -> Self {
        Self::new(RuntimeOwnerConfig::disabled())
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn config(&self) -> RuntimeOwnerConfig {
        self.config
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn poll_driver(&self) -> PollDriverHandle {
        self.poll_driver
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn database_count(&self) -> usize {
        self.dbs.len()
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn dbs(&self) -> &[RedisDb] {
        &self.dbs
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn dbs_mut(&mut self) -> &mut [RedisDb] {
        &mut self.dbs
    }

    pub fn active_slot_count(&self) -> usize {
        self.slots.iter().filter(|slot| slot.is_some()).count()
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn event_queue_len(&self) -> usize {
        self.events.len()
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn is_event_queue_empty(&self) -> bool {
        self.events.is_empty()
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn queue_event(&mut self, event: RuntimeEvent) -> Result<(), RuntimeEvent> {
        if self.events.len() >= self.config.max_pending_events() {
            return Err(event);
        }
        self.events.push_back(event);
        Ok(())
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn pop_event(&mut self) -> Option<RuntimeEvent> {
        self.events.pop_front()
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn insert_client(&mut self, client: Client) -> Option<SlotId> {
        if let Some(slot_id) = self.free_slots.pop() {
            let idx = slot_id.as_index();
            if idx < self.slots.len() {
                self.slots[idx] = Some(ClientSlot::new(slot_id, client));
                return Some(slot_id);
            }
        }

        let slot_id = SlotId::from_index(self.slots.len())?;
        self.slots.push(Some(ClientSlot::new(slot_id, client)));
        Some(slot_id)
    }

    fn insert_connected_client(
        &mut self,
        client: Client,
        stream: MioTcpStream,
        foreign_rx: Receiver<Vec<u8>>,
    ) -> Option<SlotId> {
        if let Some(slot_id) = self.free_slots.pop() {
            let idx = slot_id.as_index();
            if idx < self.slots.len() {
                self.slots[idx] =
                    Some(ClientSlot::with_stream(slot_id, client, stream, foreign_rx));
                return Some(slot_id);
            }
        }

        let slot_id = SlotId::from_index(self.slots.len())?;
        self.slots.push(Some(ClientSlot::with_stream(
            slot_id, client, stream, foreign_rx,
        )));
        Some(slot_id)
    }

    pub fn run_plain_tcp(
        listeners: Vec<StdTcpListener>,
        shutdown: Arc<AtomicBool>,
        next_client_id: Arc<AtomicU64>,
        registry: Arc<Mutex<PubSubRegistry>>,
        server: Arc<redis_core::RedisServer>,
        tcp_port: u16,
        tls_listeners: Vec<StdTcpListener>,
        tls_config: Option<Arc<ServerConfig>>,
        initial_dbs: Vec<RedisDb>,
        replica_apply_rx: Receiver<redis_commands::replica_dialer::ReplicaApplyRequest>,
    ) {
        let _ = tcp_port;
        let mut listeners: Vec<MioTcpListener> = listeners
            .into_iter()
            .map(MioTcpListener::from_std)
            .collect();
        // TLS listeners follow the plain ones; their token indices are
        // `plain_listener_count..listeners.len`.
        let plain_listener_count = listeners.len();
        let tls_listener_count = tls_listeners.len();
        for l in tls_listeners {
            listeners.push(MioTcpListener::from_std(l));
        }
        if listeners.is_empty() {
            eprintln!("redis-server: no plain TCP listeners installed");
        }
        if listeners.len() > MAX_LISTENER_TOKENS {
            eprintln!(
                "redis-server: {} TCP listeners exceeds supported maximum {}",
                listeners.len(),
                MAX_LISTENER_TOKENS
            );
            return;
        }
        let mut poll = match Poll::new() {
            Ok(poll) => poll,
            Err(e) => {
                eprintln!("redis-server: mio Poll::new failed: {}", e);
                return;
            }
        };
        for (idx, listener) in listeners.iter_mut().enumerate() {
            if let Err(e) = poll
                .registry()
                .register(listener, Token(idx), Interest::READABLE)
            {
                eprintln!("redis-server: mio listener registration failed: {}", e);
                return;
            }
        }
        let mut events = Events::with_capacity(DEFAULT_EVENT_CAPACITY);
        let config = RuntimeOwnerConfig::disabled()
            .with_enabled(true)
            .with_database_count(initial_dbs.len() as u32);
        let mut owner = RuntimeOwner::with_databases(config, initial_dbs);
        owner.replica_apply_rx = Some(replica_apply_rx);
        owner.poll_driver = PollDriverHandle::mio(1);
        owner.tls_config = tls_config;
        owner.tls_listener_start = if tls_listener_count > 0 {
            plain_listener_count
        } else {
            usize::MAX
        };
        if tls_listener_count > 0 {
            eprintln!(
                "redis-server: {} TLS listener(s) enabled (token >= {})",
                tls_listener_count, plain_listener_count
            );
        }
        eprintln!("redis-server: RuntimeOwner mio plain TCP loop enabled with owner-owned DBs");

        while !shutdown.load(Ordering::SeqCst) {
            let mut progressed = false;

            progressed |= redis_commands::replication::drive_manual_failover_once(&server);
            progressed |= owner.active_expire_step(&server);
            progressed |= owner.drain_debug_loadaof_jobs(poll.registry(), &server);
            progressed |= owner.drain_replica_apply_requests(&registry, &server);
            progressed |= owner.dispatch_scheduled_commands(poll.registry(), &registry, &server);
            progressed |= owner.schedule_unpaused_postponed_commands(&server);
            progressed |= owner.apply_pending_listener_replacement(&mut listeners, poll.registry());
            progressed |= owner.install_pending_dynamic_listeners(&mut listeners, poll.registry());
            progressed |= owner.close_pending_killed_clients();

            let timeout = if owner.has_scheduled_commands() {
                Some(Duration::from_millis(0))
            } else {
                Some(POLL_TIMEOUT)
            };
            match poll.poll(&mut events, timeout) {
                Ok(()) => {}
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    eprintln!("redis-server: mio poll failed: {}", e);
                    break;
                }
            }

            progressed |= owner.handle_poll_events(
                &events,
                &listeners,
                poll.registry(),
                &next_client_id,
                &registry,
                &server,
            );
            progressed |= owner.install_pending_dynamic_listeners(&mut listeners, poll.registry());
            progressed |= owner.drain_foreign_payloads(poll.registry());
            progressed |= redis_commands::replication::drive_manual_failover_once(&server);
            progressed |= owner.drain_debug_loadaof_jobs(poll.registry(), &server);
            progressed |= owner.drain_replica_apply_requests(&registry, &server);
            progressed |= owner.dispatch_scheduled_commands(poll.registry(), &registry, &server);
            progressed |= owner.schedule_unpaused_postponed_commands(&server);
            progressed |= owner.apply_pending_listener_replacement(&mut listeners, poll.registry());
            progressed |= owner.install_pending_dynamic_listeners(&mut listeners, poll.registry());
            owner.sweep_output_buffer_limits();
            progressed |= owner.enforce_client_memory_limits(&server);
            progressed |= owner.close_pending_killed_clients();
            progressed |= owner.cleanup_closed_clients(poll.registry(), &registry, &server);

            let _ = progressed;
        }

        owner.close_all_clients(poll.registry(), &registry, &server);
    }

    fn install_pending_dynamic_listeners(
        &mut self,
        listeners: &mut Vec<MioTcpListener>,
        poll_registry: &MioRegistry,
    ) -> bool {
        let pending = redis_commands::connection::drain_pending_tcp_listeners();
        if pending.is_empty() {
            return false;
        }

        let mut progressed = false;
        for listener in pending {
            if listeners.len() >= MAX_LISTENER_TOKENS {
                eprintln!(
                    "redis-server: dynamic TCP listener ignored; listener token capacity {} exhausted",
                    MAX_LISTENER_TOKENS
                );
                continue;
            }
            let token = Token(listeners.len());
            let mut listener = MioTcpListener::from_std(listener);
            match poll_registry.register(&mut listener, token, Interest::READABLE) {
                Ok(()) => {
                    eprintln!(
                        "redis-server: dynamic TCP listener registered at token {}",
                        token.0
                    );
                    listeners.push(listener);
                    progressed = true;
                }
                Err(e) => {
                    eprintln!(
                        "redis-server: dynamic TCP listener registration failed: {}",
                        e
                    );
                }
            }
        }
        progressed
    }

    fn apply_pending_listener_replacement(
        &mut self,
        listeners: &mut Vec<MioTcpListener>,
        poll_registry: &MioRegistry,
    ) -> bool {
        let Some(replacement) =
            redis_commands::connection::drain_pending_tcp_listener_replacement()
        else {
            return false;
        };

        for listener in listeners.iter_mut() {
            let _ = poll_registry.deregister(listener);
        }
        listeners.clear();

        for listener in replacement.into_iter().take(MAX_LISTENER_TOKENS) {
            let token = Token(listeners.len());
            let mut listener = MioTcpListener::from_std(listener);
            match poll_registry.register(&mut listener, token, Interest::READABLE) {
                Ok(()) => listeners.push(listener),
                Err(e) => eprintln!(
                    "redis-server: replacement TCP listener registration failed: {}",
                    e
                ),
            }
        }
        true
    }

    fn accept_ready(
        &mut self,
        listener: &MioTcpListener,
        poll_registry: &MioRegistry,
        next_client_id: &Arc<AtomicU64>,
        registry: &Arc<Mutex<PubSubRegistry>>,
        is_tls: bool,
    ) -> bool {
        let mut progressed = false;
        loop {
            match listener.accept() {
                Ok((stream, peer_addr)) => {
                    progressed = true;
                    let metrics = server_metrics();
                    let current = metrics
                        .connected_clients
                        .load(Ordering::Relaxed)
                        .max(self.active_slot_count() as u64);
                    let limit = redis_commands::connection::get_max_clients();
                    if current >= limit {
                        metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
                        let mut stream: StdTcpStream = stream.into();
                        let _ = stream.write_all(b"-ERR max number of clients reached\r\n");
                        drop(stream);
                        continue;
                    }

                    let stream: StdTcpStream = stream.into();
                    if let Err(e) = stream.set_nodelay(true) {
                        eprintln!("redis-server: set_nodelay failed: {}", e);
                    }
                    if let Err(e) = stream.set_nonblocking(true) {
                        eprintln!("redis-server: client set_nonblocking(true) failed: {}", e);
                        drop(stream);
                        continue;
                    }
                    let conn_stream = match stream.try_clone() {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("redis-server: try_clone failed for {}: {}", peer_addr, e);
                            drop(stream);
                            continue;
                        }
                    };
                    if let Err(e) = conn_stream.set_nonblocking(true) {
                        eprintln!(
                            "redis-server: cloned client set_nonblocking(true) failed: {}",
                            e
                        );
                    }
                    let mio_stream = MioTcpStream::from_std(stream);

                    let tls_session = if is_tls {
                        match redis_core::tls::current_server_config() {
                            Some(cfg) => match ServerConnection::new(cfg) {
                                Ok(mut s) => {
                                    // Unlimit rustls' plaintext buffer; app-layer
                                    // client-query-buffer-limit is the real bound.
                                    s.set_buffer_limit(None);
                                    Some(Box::new(s))
                                }
                                Err(e) => {
                                    eprintln!(
                                        "redis-server: tls ServerConnection::new failed for {}: {}",
                                        peer_addr, e
                                    );
                                    continue;
                                }
                            },
                            None => {
                                eprintln!(
                                    "redis-server: TLS accept but no tls_config; dropping {}",
                                    peer_addr
                                );
                                continue;
                            }
                        }
                    } else {
                        None
                    };

                    let id = next_client_id.fetch_add(1, Ordering::Relaxed);
                    let (tx, rx) = mpsc::channel::<Vec<u8>>();
                    {
                        let mut guard = match registry.lock() {
                            Ok(g) => g,
                            Err(p) => p.into_inner(),
                        };
                        guard.register_sender(id, tx);
                        guard.set_resp_proto(id, 2);
                    }

                    let peer = peer_addr.to_string();
                    if let Ok(mut guard) = client_info_registry().lock() {
                        guard.register(id, peer.clone());
                    }

                    let mut client = Client::with_connection(Connection::Tcp(conn_stream));
                    client.id = id;
                    client.addr = Some(peer);
                    client.set_authenticated_user(super::determine_initial_user());

                    let slot_id = match self.insert_connected_client(client, mio_stream, rx) {
                        Some(slot_id) => slot_id,
                        None => {
                            if let Ok(mut guard) = registry.lock() {
                                guard.drop_client(id);
                            }
                            if let Ok(mut guard) = client_info_registry().lock() {
                                guard.deregister(id);
                            }
                            metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    if let Some(session) = tls_session {
                        if let Some(slot) = self.slot_mut(slot_id) {
                            slot.tls = Some(session);
                        }
                    }

                    metrics.on_connect();
                    metrics
                        .total_connections_received
                        .fetch_add(1, Ordering::Relaxed);

                    if let Some(slot) = self.slot_mut(slot_id) {
                        if let Some(stream) = slot.stream.as_mut() {
                            if let Err(e) = poll_registry.register(
                                stream,
                                token_for_slot(slot_id),
                                client_interest(false),
                            ) {
                                eprintln!(
                                    "redis-server: mio client registration failed for {}: {}",
                                    peer_addr, e
                                );
                                slot.mark_closed();
                            }
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    eprintln!("redis-server: accept failed: {}", e);
                    break;
                }
            }
        }
        progressed
    }

    pub fn remove_client(&mut self, slot_id: SlotId) -> Option<ClientSlot> {
        let slot = self.slots.get_mut(slot_id.as_index())?;
        let removed = slot.take();
        if removed.is_some() {
            self.free_slots.push(slot_id);
        }
        removed
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn slot(&self, slot_id: SlotId) -> Option<&ClientSlot> {
        self.slots.get(slot_id.as_index())?.as_ref()
    }

    pub fn slot_mut(&mut self, slot_id: SlotId) -> Option<&mut ClientSlot> {
        self.slots.get_mut(slot_id.as_index())?.as_mut()
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn queue_write(&mut self, slot_id: SlotId, bytes: &[u8]) -> bool {
        let slot = match self.slot_mut(slot_id) {
            Some(slot) => slot,
            None => return false,
        };
        slot.queue_write(bytes);
        true
    }

    #[allow(dead_code)] // owner-loop vocabulary, see object-vocabulary.tsv
    pub fn take_pending_write(&mut self, slot_id: SlotId) -> Option<Vec<u8>> {
        self.slot_mut(slot_id).map(ClientSlot::take_pending_write)
    }

    fn has_scheduled_commands(&self) -> bool {
        !self.continuation_queue.is_empty()
    }

    fn schedule_command_continuation(&mut self, slot_id: SlotId) {
        if self.queued_continuations.insert(slot_id) {
            self.continuation_queue.push_back(slot_id);
        }
    }

    fn pop_command_continuation(&mut self) -> Option<SlotId> {
        let slot_id = self.continuation_queue.pop_front()?;
        self.queued_continuations.remove(&slot_id);
        Some(slot_id)
    }

    fn handle_poll_events(
        &mut self,
        events: &Events,
        listeners: &[MioTcpListener],
        poll_registry: &MioRegistry,
        next_client_id: &Arc<AtomicU64>,
        registry: &Arc<Mutex<PubSubRegistry>>,
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        let mut progressed = false;
        let mut read_buf = [0u8; READ_BUFFER_SIZE];

        for event in events.iter() {
            if event.token().0 < listeners.len() {
                if event.is_readable() {
                    let is_tls = event.token().0 >= self.tls_listener_start;
                    progressed |= self.accept_ready(
                        &listeners[event.token().0],
                        poll_registry,
                        next_client_id,
                        registry,
                        is_tls,
                    );
                }
                continue;
            }

            let slot_id = match slot_id_from_token(event.token()) {
                Some(slot_id) => slot_id,
                None => continue,
            };
            let idx = slot_id.as_index();

            if event.is_readable() {
                progressed |= self.read_slot(idx, &mut read_buf);
                let outcome = self.dispatch_slot_commands(idx, registry, server);
                progressed |= outcome.progressed;
                if self.slot_is_tls(idx) {
                    // Flush handshake output and/or encrypted replies; the TLS
                    // flush path owns interest recomputation.
                    progressed |= self.flush_slot_pending_write(idx, poll_registry);
                } else if outcome.queued_write {
                    progressed |= self.flush_slot_pending_write(idx, poll_registry);
                    progressed |= self.ensure_writable_interest(poll_registry, slot_id);
                }
                if outcome.reschedule {
                    self.schedule_command_continuation(slot_id);
                }
            }

            if event.is_writable() {
                progressed |= self.flush_slot_pending_write(idx, poll_registry);
            }

            if event.is_error() || event.is_read_closed() || event.is_write_closed() {
                if let Some(slot) = self.slot_mut(slot_id) {
                    if slot.write_buffer.is_empty() {
                        slot.mark_closed();
                    } else {
                        slot.mark_close_after_flush();
                    }
                }
            }
        }

        progressed
    }

    fn dispatch_scheduled_commands(
        &mut self,
        poll_registry: &MioRegistry,
        registry: &Arc<Mutex<PubSubRegistry>>,
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        let mut progressed = false;
        let initial_len = self.continuation_queue.len();
        for _ in 0..initial_len {
            let slot_id = match self.pop_command_continuation() {
                Some(slot_id) => slot_id,
                None => break,
            };
            let outcome = self.dispatch_slot_commands(slot_id.as_index(), registry, server);
            progressed |= outcome.progressed;
            if outcome.queued_write {
                progressed |= self.flush_slot_pending_write(slot_id.as_index(), poll_registry);
                progressed |= self.ensure_writable_interest(poll_registry, slot_id);
            }
            if outcome.reschedule {
                self.schedule_command_continuation(slot_id);
            }
        }
        progressed
    }

    fn schedule_unpaused_postponed_commands(
        &mut self,
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        if pause_postponed_client_count() == 0 {
            return false;
        }
        let paused_actions = current_paused_actions(server);
        let mut scheduled = false;
        let ids: Vec<SlotId> = self
            .slots
            .iter()
            .flatten()
            .filter(|slot| slot.pause_postponed && !slot_command_is_paused(slot, paused_actions))
            .map(ClientSlot::id)
            .collect();
        for slot_id in ids {
            self.schedule_command_continuation(slot_id);
            scheduled = true;
        }
        scheduled
    }

    fn ensure_writable_interest(&mut self, poll_registry: &MioRegistry, slot_id: SlotId) -> bool {
        let slot = match self.slot_mut(slot_id) {
            Some(slot) => slot,
            None => return false,
        };
        if slot.write_buffer.is_empty() || slot.writable_interest || slot.closed {
            return false;
        }
        let stream = match slot.stream.as_mut() {
            Some(stream) => stream,
            None => {
                slot.mark_closed();
                return false;
            }
        };
        match poll_registry.reregister(stream, token_for_slot(slot_id), client_interest(true)) {
            Ok(()) => {
                slot.writable_interest = true;
                true
            }
            Err(e) => {
                eprintln!("redis-server: mio writable reregister failed: {}", e);
                slot.mark_closed();
                true
            }
        }
    }

    fn drain_foreign_payloads(&mut self, poll_registry: &MioRegistry) -> bool {
        let mut progressed = false;
        let mut writable_slots = Vec::new();
        for slot in self.slots.iter_mut().flatten() {
            loop {
                let recv_result = match slot.foreign_rx.as_mut() {
                    Some(rx) => rx.try_recv(),
                    None => break,
                };
                match recv_result {
                    Ok(payload) => {
                        if payload.is_empty() {
                            slot.mark_closed();
                            progressed = true;
                            break;
                        }
                        if slot.client.blocked_on_keys {
                            slot.client.blocked_on_keys = false;
                            slot.client.commands_processed =
                                slot.client.commands_processed.saturating_add(1);
                            if let Ok(mut guard) = client_info_registry().lock() {
                                guard.update_client_metadata(&slot.client);
                            }
                        }
                        slot.queue_write_owned(payload);
                        writable_slots.push(slot.id());
                        progressed = true;
                    }
                    Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => {
                        break;
                    }
                }
            }
        }
        for slot_id in writable_slots {
            self.ensure_writable_interest(poll_registry, slot_id);
        }
        progressed
    }

    fn drain_debug_loadaof_jobs(
        &mut self,
        poll_registry: &MioRegistry,
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        let mut progressed = false;
        let had_jobs = !self.debug_loadaof_jobs.is_empty();
        let mut i = 0usize;
        while i < self.debug_loadaof_jobs.len() {
            let recv = self.debug_loadaof_jobs[i].rx.try_recv();
            match recv {
                Ok(result) => {
                    let job = self.debug_loadaof_jobs.swap_remove(i);
                    self.complete_debug_loadaof_job(job.slot_id, result, poll_registry, server);
                    progressed = true;
                }
                Err(TryRecvError::Empty) => {
                    i += 1;
                }
                Err(TryRecvError::Disconnected) => {
                    let job = self.debug_loadaof_jobs.swap_remove(i);
                    self.complete_debug_loadaof_job(
                        job.slot_id,
                        Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "DEBUG LOADAOF worker disconnected",
                        )),
                        poll_registry,
                        server,
                    );
                    progressed = true;
                }
            }
        }
        if had_jobs && self.debug_loadaof_jobs.is_empty() {
            server.persistence.set_loading(false);
        }
        progressed
    }

    fn complete_debug_loadaof_job(
        &mut self,
        slot_id: SlotId,
        result: DebugLoadAofResult,
        poll_registry: &MioRegistry,
        server: &Arc<redis_core::RedisServer>,
    ) {
        let reply = match result {
            Ok((mut loaded, _summary)) => {
                for (idx, db) in loaded.iter_mut().enumerate() {
                    db.id = idx as u32;
                }
                self.dbs = loaded;
                b"+OK\r\n".to_vec()
            }
            Err(err) => debug_loadaof_error_reply(&err),
        };

        if self.debug_loadaof_jobs.is_empty() {
            server.persistence.set_loading(false);
        }

        let should_schedule = if let Some(slot) = self.slot_mut(slot_id) {
            slot.debug_loadaof_pending = false;
            slot.client.commands_processed = slot.client.commands_processed.saturating_add(1);
            slot.queue_write_owned(reply);
            !slot.client.query_buf.is_empty() && has_complete_command(&slot.client.query_buf)
        } else {
            false
        };
        let _ = self.ensure_writable_interest(poll_registry, slot_id);
        if should_schedule {
            self.schedule_command_continuation(slot_id);
        }
    }

    fn drain_replica_apply_requests(
        &mut self,
        registry: &Arc<Mutex<PubSubRegistry>>,
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        let mut progressed = false;
        loop {
            let request = {
                let rx = match self.replica_apply_rx.as_ref() {
                    Some(rx) => rx,
                    None => return progressed,
                };
                match rx.try_recv() {
                    Ok(request) => request,
                    Err(TryRecvError::Empty) => return progressed,
                    Err(TryRecvError::Disconnected) => {
                        self.replica_apply_rx = None;
                        return progressed;
                    }
                }
            };
            let ok = match request.kind {
                redis_commands::replica_dialer::ReplicaApplyKind::Command(argv) => {
                    self.apply_replica_command(argv, registry, server)
                }
                redis_commands::replica_dialer::ReplicaApplyKind::CommandBatch(commands) => {
                    self.apply_replica_command_batch(commands, registry, server)
                }
                redis_commands::replica_dialer::ReplicaApplyKind::LoadRdb(bytes) => {
                    self.load_replica_rdb(&bytes, server)
                }
            };
            let _ = request.done.send(ok);
            if ok {
                redis_commands::aof::note_current_writer_repl_offset(request.offset_after);
                redis_core::replication::global_replication_state()
                    .master_repl_offset
                    .store(request.offset_after, Ordering::SeqCst);
                redis_commands::replication::maybe_wake_wait_clients();
            }
            progressed = true;
        }
    }

    fn apply_replica_command(
        &mut self,
        argv: Vec<RedisString>,
        registry: &Arc<Mutex<PubSubRegistry>>,
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        if argv.is_empty() {
            return true;
        }

        let mut client = self.new_replica_apply_client();
        client.set_args(argv);

        super::process_current_command_with_db_list(&mut client, &mut self.dbs, registry, server);
        self.replica_apply_db_index = client.db_index;

        !client.reply_buf.starts_with(b"-")
    }

    fn apply_replica_command_batch(
        &mut self,
        commands: Vec<Vec<RedisString>>,
        registry: &Arc<Mutex<PubSubRegistry>>,
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        let mut client = self.new_replica_apply_client();
        for argv in commands {
            let reply_start = client.reply_buf.len();
            client.set_args(argv);
            super::process_current_command_with_db_list(
                &mut client,
                &mut self.dbs,
                registry,
                server,
            );
            let failed = client.reply_buf[reply_start..].starts_with(b"-");
            client.reply_buf.truncate(reply_start);
            if failed {
                self.replica_apply_db_index = client.db_index;
                return false;
            }
        }
        self.replica_apply_db_index = client.db_index;
        true
    }

    fn new_replica_apply_client(&self) -> Client {
        let mut client = Client::new(0);
        client.replication_apply = true;
        client.suppress_monitor = true;
        client.authenticated_user = Some(RedisString::from_bytes(b"default"));
        client.db_index = self.replica_apply_db_index;
        client
    }

    /// Load a full-resync RDB snapshot into the owned databases, replacing their
    /// contents. The dialer ships the bytes off the master stream; the owner is
    /// the only thread allowed to mutate `self.dbs`, so the load happens here.
    /// The bytes are staged through a temp file because the RDB loader reads
    /// from a path.
    fn load_replica_rdb(&mut self, bytes: &[u8], server: &Arc<redis_core::RedisServer>) -> bool {
        let temp_path =
            std::env::temp_dir().join(format!("valdr-replica-incoming-{}.rdb", std::process::id()));
        if let Err(e) = std::fs::write(&temp_path, bytes) {
            eprintln!("redis-server: replica: staging incoming RDB failed: {}", e);
            return false;
        }
        let result = redis_core::rdb::load_replacement_plan(self.dbs.len(), &temp_path);
        let _ = std::fs::remove_file(&temp_path);
        let plan = match result {
            Ok(plan) => plan,
            Err(e) => {
                eprintln!("redis-server: replica: full-resync RDB load failed: {}", e);
                return false;
            }
        };
        let functions = match redis_commands::eval::prepare_rdb_function_replacement(
            &plan.outcome.function_payloads,
        ) {
            Ok(functions) => functions,
            Err(e) => {
                eprintln!(
                    "redis-server: replica: full-resync function load failed: {}",
                    e
                );
                return false;
            }
        };
        let keys_loaded = plan.outcome.keys_loaded;
        let msg = plan.outcome.message;
        self.dbs = plan.dbs;
        self.replica_apply_db_index = 0;
        redis_core::replication::global_replication_state().remember_primary_stream_db(0);
        redis_commands::eval::install_rdb_function_replacement(functions);
        redis_core::replication::global_replication_state()
            .set_zero_offset_partial_resync_allowed(keys_loaded == 0);
        eprintln!("redis-server: replica: full-resync RDB loaded: {}", msg);
        self.refresh_replica_aof_after_fullsync(bytes, server)
    }

    fn refresh_replica_aof_after_fullsync(
        &mut self,
        rdb_bytes: &[u8],
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        let cfg = Arc::clone(&server.live_config);
        if !cfg.appendonly() {
            return true;
        }

        let dir = PathBuf::from(cfg.rdb_dir());
        let filename = cfg.appendfilename();
        let dirname = cfg.appenddirname();
        let fsync_policy = cfg.appendfsync();
        let use_rdb_preamble = cfg.aof_use_rdb_preamble();
        let result = if use_rdb_preamble && !cfg.repl_diskless_sync() {
            let result = redis_commands::aof::install_fullsync_rdb_as_manifest_base(
                &dir,
                &filename,
                &dirname,
                rdb_bytes,
                fsync_policy,
            );
            if result.is_ok() {
                let msg = "redis-server: Reused RDB file from primary sync as AOF base file";
                eprintln!("{}", msg);
                println!("{}", msg);
            }
            result
        } else {
            redis_commands::aof::rewrite_manifest_aof_from_dbs(
                &dir,
                &filename,
                &dirname,
                &self.dbs,
                fsync_policy,
                use_rdb_preamble,
            )
        };

        match result {
            Ok((base_size, current_size)) => {
                server.persistence.set_aof_base_size(base_size);
                server.persistence.set_aof_current_size(current_size);
                server
                    .persistence
                    .set_aof_last_bgrewrite_status(redis_core::PersistenceStatus::Ok);
                true
            }
            Err(err) => {
                eprintln!(
                    "redis-server: replica full-sync AOF refresh failed: {}",
                    err
                );
                server
                    .persistence
                    .set_aof_last_bgrewrite_status(redis_core::PersistenceStatus::Err);
                false
            }
        }
    }

    fn slot_is_tls(&self, idx: usize) -> bool {
        self.slots
            .get(idx)
            .and_then(Option::as_ref)
            .is_some_and(|s| s.tls.is_some())
    }

    /// TLS counterpart of `read_slot`: advance the rustls handshake first, then
    /// deliver decrypted plaintext into the query buffer. Reuses the shared,
    /// harness-tested `session_read_pump`/`session_write_pump`. All `slot`
    /// mutations happen outside the `stream`/`session` borrow scope.
    fn read_slot_tls(&mut self, idx: usize, read_buf: &mut [u8; READ_BUFFER_SIZE]) -> bool {
        let slot = match self.slots.get_mut(idx).and_then(Option::as_mut) {
            Some(slot) => slot,
            None => return false,
        };
        if slot.closed || slot.close_after_flush {
            return false;
        }
        if slot.stream.is_none() || slot.tls.is_none() {
            slot.mark_closed();
            return false;
        }

        let mut progressed = false;
        let mut plaintext: Vec<u8> = Vec::new();
        let mut transport_closed = false;
        let mut handshake_just_finished = false;
        let mut errored = false;
        let mut still_handshaking = false;

        {
            let stream = slot.stream.as_mut().unwrap();
            let session = slot.tls.as_mut().unwrap().as_mut();

            if !slot.tls_handshake_done {
                if session_write_pump(session, stream).is_err() {
                    errored = true;
                } else {
                    match session_read_pump(session, stream) {
                        Ok(eof) => {
                            let _ = session_write_pump(session, stream);
                            if session.is_handshaking() {
                                transport_closed = eof;
                                still_handshaking = true;
                            } else {
                                handshake_just_finished = true;
                            }
                        }
                        Err(_) => errored = true,
                    }
                }
            }

            if !errored && !still_handshaking {
                // Interleaved drain+read+process. rustls' internal "received
                // plaintext" queue has a finite ceiling — feeding ciphertext
                // without draining decrypted plaintext between cycles makes
                // read_tls return "received plaintext buffer full". Drain
                // the *start* of each iteration so any plaintext already
                // queued (e.g. left by the handshake-side pump that read past
                // ServerFinished into early app data) is consumed first.
                'pump: loop {
                    // 1. drain any plaintext currently buffered in the session.
                    loop {
                        match session.reader().read(read_buf) {
                            Ok(0) => {
                                transport_closed = true;
                                break 'pump;
                            }
                            Ok(n) => plaintext.extend_from_slice(&read_buf[..n]),
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                transport_closed = true;
                                break 'pump;
                            }
                        }
                    }
                    // 2. read more ciphertext, process, and loop (which drains).
                    match session.read_tls(stream) {
                        Ok(0) => {
                            transport_closed = true;
                            break 'pump;
                        }
                        Ok(_) => {
                            if session.process_new_packets().is_err() {
                                errored = true;
                                break 'pump;
                            }
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break 'pump,
                        Err(_) => {
                            errored = true;
                            break 'pump;
                        }
                    }
                }
            }
        }

        if handshake_just_finished {
            slot.tls_handshake_done = true;
        }
        if errored {
            slot.mark_closed();
            return progressed;
        }
        if still_handshaking {
            if transport_closed {
                slot.mark_closed();
            }
            return progressed;
        }
        if !plaintext.is_empty() {
            if super::client_has_pending_kill(slot.client.id) {
                slot.mark_closed();
                return progressed;
            }
            slot.client.net_input_bytes = slot
                .client
                .net_input_bytes
                .saturating_add(plaintext.len() as u64);
            slot.ingest(&plaintext);
            slot.refresh_client_memory_snapshot();
            let query_limit = redis_commands::connection::client_query_buffer_limit();
            if query_limit > 0 && slot.client.query_buf.len() > query_limit {
                slot.mark_closed();
                server_metrics()
                    .client_query_buffer_limit_disconnections
                    .fetch_add(1, Ordering::Relaxed);
                return progressed;
            }
            progressed = true;
        }
        if transport_closed && plaintext.is_empty() {
            slot.mark_closed();
        }
        progressed
    }

    /// TLS counterpart of `flush_slot_pending_write`: move plaintext replies into
    /// the rustls session, flush ciphertext to the socket, and recompute Poll
    /// interest from the session's write intent (the `updateSSLEvent` analog).
    fn flush_slot_pending_write_tls(&mut self, idx: usize, poll_registry: &MioRegistry) -> bool {
        let slot = match self.slots.get_mut(idx).and_then(Option::as_mut) {
            Some(slot) => slot,
            None => return false,
        };
        if slot.closed {
            return false;
        }
        if slot.stream.is_none() || slot.tls.is_none() {
            slot.mark_closed();
            return false;
        }

        let mut progressed = false;
        let mut consumed = 0usize;
        let mut errored = false;

        {
            let stream = slot.stream.as_mut().unwrap();
            let session = slot.tls.as_mut().unwrap().as_mut();

            // 1. Encrypt as much queued plaintext as the session buffer accepts.
            let bytes = slot.write_buffer.as_bytes();
            if !bytes.is_empty() {
                match session.writer().write(bytes) {
                    Ok(n) => consumed = n,
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(_) => errored = true,
                }
            }
            // 2. Flush ciphertext (handshake output + encrypted replies).
            if !errored && session_write_pump(session, stream).is_err() {
                errored = true;
            }
        }

        if consumed > 0 {
            slot.write_buffer.consume_front(consumed);
            slot.note_written_bytes(consumed);
            slot.reconcile_output_buffer_after_write();
            slot.publish_client_metadata();
            progressed = true;
        }
        if errored {
            slot.mark_closed();
            return progressed;
        }

        // 3. Recompute interest: WRITABLE iff there is unflushed ciphertext or
        // still-queued plaintext.
        let want_write =
            !slot.write_buffer.is_empty() || slot.tls.as_ref().is_some_and(|s| s.wants_write());
        if want_write != slot.writable_interest {
            let token = token_for_slot(slot.id());
            if let Some(stream) = slot.stream.as_mut() {
                if poll_registry
                    .reregister(stream, token, client_interest(want_write))
                    .is_ok()
                {
                    slot.writable_interest = want_write;
                }
            }
        }
        progressed
    }

    fn read_slot(&mut self, idx: usize, read_buf: &mut [u8; READ_BUFFER_SIZE]) -> bool {
        if self.slot_is_tls(idx) {
            return self.read_slot_tls(idx, read_buf);
        }
        let slot = match self.slots.get_mut(idx).and_then(Option::as_mut) {
            Some(slot) => slot,
            None => return false,
        };
        if slot.closed || slot.close_after_flush {
            return false;
        }

        let mut progressed = false;
        loop {
            let n = {
                let stream = match slot.stream.as_mut() {
                    Some(stream) => stream,
                    None => {
                        slot.mark_closed();
                        return progressed;
                    }
                };
                match stream.read(read_buf) {
                    Ok(0) => {
                        slot.mark_closed();
                        break;
                    }
                    Ok(n) => n,
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => {
                        slot.mark_closed();
                        break;
                    }
                }
            };
            slot.client.net_input_bytes = slot.client.net_input_bytes.saturating_add(n as u64);
            if super::client_has_pending_kill(slot.client.id) {
                slot.mark_closed();
                break;
            }
            slot.ingest(&read_buf[..n]);
            slot.refresh_client_memory_snapshot();
            let query_limit = redis_commands::connection::client_query_buffer_limit();
            if query_limit > 0 && slot.client.query_buf.len() > query_limit {
                slot.mark_closed();
                server_metrics()
                    .client_query_buffer_limit_disconnections
                    .fetch_add(1, Ordering::Relaxed);
                progressed = true;
                break;
            }
            progressed = true;
            // A read shorter than the buffer means the socket receive queue is
            // now drained: a stream `read` returns everything available up
            // the buffer size, so `n < READ_BUFFER_SIZE` proves there is no
            // more pending data. Under mio's edge-triggered kqueue/epoll
            // next arrival re-arms the readable event, so we can stop here
            // without paying a second `read` syscall purely to observe
            // `WouldBlock`. This matches Valkey's single read per readable
            // event (its `ae` loop is level-triggered) and the userspace
            // readiness optimization in tokio PR #4840. At pipeline=1 it halves
            // the read syscalls per request, the dominant per-request overhead.
            if n < read_buf.len() {
                break;
            }
        }
        progressed
    }

    fn dispatch_slot_commands(
        &mut self,
        idx: usize,
        registry: &Arc<Mutex<PubSubRegistry>>,
        server: &Arc<redis_core::RedisServer>,
    ) -> SlotDispatchOutcome {
        let slot = match self.slots.get_mut(idx).and_then(Option::as_mut) {
            Some(slot) => slot,
            None => return SlotDispatchOutcome::default(),
        };
        let has_parked_paused_command = slot.pause_postponed && !slot.client.argv.is_empty();
        if slot.closed
            || slot.close_after_flush
            || slot.debug_loadaof_pending
            || (slot.client.query_buf.is_empty() && !has_parked_paused_command)
        {
            return SlotDispatchOutcome::default();
        }

        let mut consumed_total = 0usize;
        let mut commands = 0usize;
        let mut saw_command = false;
        let mut saw_incomplete_query = false;
        let mut paused_before_dispatch = false;
        let mut last_cmd_name: Vec<u8> = Vec::new();
        let mut paused_actions = current_paused_actions(server);
        redis_commands::aof::begin_thread_aof_batch();

        while commands < MAX_COMMANDS_PER_SLOT_TICK {
            if slot.pause_postponed && !slot.client.argv.is_empty() {
                commands += 1;
                saw_command = true;
                last_cmd_name.clear();
                if let Some(cmd) = slot.client.arg(0) {
                    last_cmd_name.extend_from_slice(cmd.as_bytes());
                }
                if slot_command_is_paused(slot, paused_actions) {
                    paused_before_dispatch = true;
                    break;
                }
                slot.clear_pause_postponed();
            } else {
                if let Some(err) = super::unauthenticated_protocol_limit_error(
                    &slot.client,
                    &slot.client.query_buf[consumed_total..],
                ) {
                    super::queue_error_reply(&mut slot.client, &err);
                    slot.mark_close_after_flush();
                    break;
                }
                let parsed = slot.client.parse_query_buffer_into_argv(consumed_total);
                match parsed {
                    Ok(Some(consumed)) => {
                        consumed_total += consumed;
                        if slot.client.argv.is_empty() {
                            continue;
                        }
                        commands += 1;
                        saw_command = true;
                        last_cmd_name.clear();
                        if let Some(cmd) = slot.client.arg(0) {
                            last_cmd_name.extend_from_slice(cmd.as_bytes());
                        }
                        if super::is_client_info_observer(&last_cmd_name) {
                            super::update_client_info_snapshot(&slot.client, &last_cmd_name);
                        }
                        if slot_command_is_paused(slot, paused_actions) {
                            slot.mark_pause_postponed();
                            paused_before_dispatch = true;
                            break;
                        }
                        slot.clear_pause_postponed();
                    }
                    Ok(None) => {
                        saw_incomplete_query = consumed_total < slot.client.query_buf.len();
                        break;
                    }
                    Err(err) => {
                        super::queue_error_reply(&mut slot.client, &err);
                        slot.mark_close_after_flush();
                        break;
                    }
                }
            }

            if client_exceeds_own_memory_limit_after_parse(slot, server, consumed_total) {
                slot.mark_closed();
                server_metrics()
                    .evicted_clients
                    .fetch_add(1, Ordering::Relaxed);
                break;
            }

            if (slot.client.db_index as usize) >= self.dbs.len() {
                super::queue_error_reply(
                    &mut slot.client,
                    &redis_types::RedisError::runtime(b"ERR DB index is out of range"),
                );
                slot.mark_close_after_flush();
                break;
            }

            if is_debug_loadaof_command(&slot.client) {
                if self.debug_loadaof_jobs.is_empty() {
                    server.persistence.set_loading(true);
                    slot.debug_loadaof_pending = true;
                    let job =
                        spawn_debug_loadaof_job(slot.id(), Arc::clone(server), self.dbs.len());
                    self.debug_loadaof_jobs.push(job);
                } else {
                    slot.client
                        .reply_buf
                        .extend_from_slice(&loading_error_reply());
                }
                slot.client.reset_args();
                continue;
            }

            super::process_current_command_with_db_list(
                &mut slot.client,
                &mut self.dbs,
                registry,
                server,
            );

            if slot.client.should_close {
                slot.mark_close_after_flush();
                break;
            }
            paused_actions = current_paused_actions(server);
        }

        let reschedule = commands == MAX_COMMANDS_PER_SLOT_TICK
            && consumed_total < slot.client.query_buf.len()
            && has_complete_command(&slot.client.query_buf[consumed_total..]);

        if saw_incomplete_query {
            slot.observe_incomplete_query_buffer();
        }
        if consumed_total > 0 {
            slot.observe_completed_query_buffer(consumed_total);
            if consumed_total >= slot.client.query_buf.len() {
                slot.client.query_buf.clear();
            } else {
                slot.client.query_buf.drain(..consumed_total);
            }
        }

        // Command dispatch has already applied CLIENT REPLY OFF/SKIP while
        // retaining Pub/Sub push bytes in the shared reply buffer.
        redis_commands::aof::finish_thread_aof_batch(&server.persistence);
        let queued_write = slot.queue_client_reply_preserving_capacity();
        slot.refresh_client_memory_snapshot();

        if saw_command {
            super::update_client_info_snapshot(&slot.client, &last_cmd_name);
        }

        SlotDispatchOutcome {
            progressed: saw_command || consumed_total > 0,
            queued_write,
            reschedule: reschedule && !paused_before_dispatch,
        }
    }

    fn active_expire_step(&mut self, server: &redis_core::RedisServer) -> bool {
        if self.dbs.is_empty() {
            return false;
        }
        if current_paused_actions(server) & PAUSE_ACTION_EXPIRE != 0 {
            return false;
        }
        if server.live_config.import_mode() {
            return false;
        }
        let (effort, hz) = redis_core::expire::active_expire_config().snapshot();
        if effort == 0 {
            return false;
        }
        let interval = if hz == 0 {
            ACTIVE_EXPIRE_FALLBACK_INTERVAL
        } else {
            Duration::from_millis((1000 / hz).max(1) as u64)
        };
        if self.last_active_expire.elapsed() < interval {
            return false;
        }
        self.last_active_expire = Instant::now();
        let metrics = server_metrics();
        let start = Instant::now();
        let mut deleted_total = 0u64;
        let dbs_to_scan = ACTIVE_EXPIRE_DBS_PER_STEP.min(self.dbs.len());
        for _ in 0..dbs_to_scan {
            let idx = self.active_expire_cursor % self.dbs.len();
            self.active_expire_cursor = (self.active_expire_cursor + 1) % self.dbs.len();
            deleted_total = deleted_total.saturating_add(run_active_expire_tick_on_db(
                &mut self.dbs[idx],
                effort,
                Some(metrics.as_ref()),
            ));
            deleted_total = deleted_total.saturating_add(
                redis_commands::hash::run_active_hash_field_expire_tick_on_db(
                    &mut self.dbs[idx],
                    effort,
                ),
            );
        }
        if deleted_total > 0 {
            let elapsed_ms = start.elapsed().as_millis().max(1) as u64;
            redis_commands::slowlog_cmd::report_latency_event(b"expire-cycle", elapsed_ms);
        }
        deleted_total > 0
    }

    fn sweep_output_buffer_limits(&mut self) {
        let now = Instant::now();
        for slot in self.slots.iter_mut().flatten() {
            slot.refresh_output_buffer_state_at(now);
        }
    }

    fn close_pending_killed_clients(&mut self) -> bool {
        let killed_ids = match client_info_registry().lock() {
            Ok(guard) => guard.killed_ids(),
            Err(poison) => poison.into_inner().killed_ids(),
        };
        if killed_ids.is_empty() {
            return false;
        }
        let killed_ids: HashSet<u64> = killed_ids.into_iter().collect();
        let mut progressed = false;
        for slot in self.slots.iter_mut().flatten() {
            if killed_ids.contains(&slot.client.id) && !slot.closed {
                slot.mark_closed();
                progressed = true;
            }
        }
        progressed
    }

    fn enforce_client_memory_limits(&mut self, server: &redis_core::RedisServer) -> bool {
        let client_memory = self.total_client_memory();
        let maxmemory_clients = server.live_config.maxmemory_clients();
        let client_limit =
            get_client_eviction_limit(maxmemory_clients, server.live_config.maxmemory());
        if client_limit > 0 {
            return self.evict_clients_to_limit(client_limit, client_memory);
        }

        if server.live_config.import_mode() {
            return false;
        }

        let maxmemory = server.live_config.maxmemory();
        if maxmemory == 0 {
            return false;
        }
        if current_paused_actions(server) & PAUSE_ACTION_EVICT != 0 {
            return false;
        }
        let evictable_client_memory = self.total_evictable_client_memory();
        if maxmemory_clients == 0 && evictable_client_memory < 1024 * 1024 {
            return false;
        }
        let key_memory: u64 = self.dbs.iter().map(approximate_memory_used).sum();
        if key_memory.saturating_add(evictable_client_memory as u64) <= maxmemory {
            return false;
        }
        let target_key_memory = maxmemory.saturating_sub(evictable_client_memory as u64);
        self.evict_keys_to_total(target_key_memory, server)
    }

    fn total_client_memory(&self) -> usize {
        self.slots
            .iter()
            .flatten()
            .filter(|slot| !slot.closed && !slot.close_after_flush)
            .map(ClientSlot::client_memory_usage)
            .sum()
    }

    fn total_evictable_client_memory(&self) -> usize {
        self.slots
            .iter()
            .flatten()
            .filter(|slot| !slot.closed && !slot.close_after_flush && !slot.client.is_replica)
            .map(ClientSlot::client_memory_usage)
            .sum()
    }

    fn evict_clients_to_limit(&mut self, limit: usize, mut total: usize) -> bool {
        let mut progressed = false;
        while total > limit {
            let victim = self
                .slots
                .iter()
                .flatten()
                .filter(|slot| slot.can_be_evicted_for_memory())
                .max_by_key(|slot| slot.client_memory_usage())
                .map(|slot| (slot.id(), slot.client_memory_usage()));
            let Some((slot_id, usage)) = victim else {
                break;
            };
            if usage == 0 {
                break;
            }
            if let Some(slot) = self.slot_mut(slot_id) {
                slot.mark_closed();
                server_metrics()
                    .evicted_clients
                    .fetch_add(1, Ordering::Relaxed);
                progressed = true;
            }
            total = total.saturating_sub(usage);
        }
        progressed
    }

    fn evict_keys_to_total(
        &mut self,
        target_key_memory: u64,
        server: &redis_core::RedisServer,
    ) -> bool {
        let policy = server.live_config.maxmemory_policy();
        let log_factor = server.live_config.lfu_log_factor();
        let decay_time = server.live_config.lfu_decay_time();
        let mut total: u64 = self.dbs.iter().map(approximate_memory_used).sum();
        if total <= target_key_memory {
            return false;
        }
        let mut progressed = false;
        for db in &mut self.dbs {
            if total <= target_key_memory {
                break;
            }
            let before = approximate_memory_used(db);
            if before == 0 {
                continue;
            }
            let outcome = try_evict_to_fit(db, 0, policy, log_factor, decay_time);
            let evicted_any = matches!(
                outcome,
                EvictionOutcome::Evicted(ref keys) | EvictionOutcome::StillOver(ref keys)
                    if !keys.is_empty()
            );
            if evicted_any {
                progressed = true;
            }
            let after = approximate_memory_used(db);
            total = total.saturating_sub(before.saturating_sub(after));
        }
        progressed
    }

    fn flush_slot_pending_write(&mut self, idx: usize, poll_registry: &MioRegistry) -> bool {
        if self.slot_is_tls(idx) {
            return self.flush_slot_pending_write_tls(idx, poll_registry);
        }
        let mut progressed = false;
        let slot = match self.slots.get_mut(idx).and_then(Option::as_mut) {
            Some(slot) => slot,
            None => return false,
        };
        if slot.closed {
            return false;
        }
        if slot.write_buffer.is_empty() {
            if slot.writable_interest {
                let token = token_for_slot(slot.id());
                if let Some(stream) = slot.stream.as_mut() {
                    if poll_registry
                        .reregister(stream, token, client_interest(false))
                        .is_ok()
                    {
                        slot.writable_interest = false;
                    }
                }
            }
            return false;
        }
        let (stream, buffer) = match (slot.stream.as_mut(), &mut slot.write_buffer) {
            (Some(stream), buffer) => (stream, buffer),
            (None, _) => {
                slot.mark_closed();
                return false;
            }
        };
        let mut written_total = 0usize;
        while !buffer.is_empty() {
            match stream.write(buffer.as_bytes()) {
                Ok(0) => {
                    slot.mark_closed();
                    break;
                }
                Ok(n) => {
                    buffer.consume_front(n);
                    written_total = written_total.saturating_add(n);
                    progressed = true;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    slot.mark_closed();
                    break;
                }
            }
        }
        slot.note_written_bytes(written_total);
        slot.reconcile_output_buffer_after_write();
        if progressed {
            slot.publish_client_metadata();
        }
        if slot.write_buffer.is_empty() && slot.writable_interest {
            let token = token_for_slot(slot.id());
            if let Some(stream) = slot.stream.as_mut() {
                match poll_registry.reregister(stream, token, client_interest(false)) {
                    Ok(()) => slot.writable_interest = false,
                    Err(e) => {
                        eprintln!("redis-server: mio readable reregister failed: {}", e);
                        slot.mark_closed();
                    }
                }
            }
        }
        progressed
    }

    fn cleanup_closed_clients(
        &mut self,
        poll_registry: &MioRegistry,
        registry: &Arc<Mutex<PubSubRegistry>>,
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        let mut to_remove = Vec::new();
        for slot in self.slots.iter().flatten() {
            if slot.closed || (slot.close_after_flush && slot.write_buffer.is_empty()) {
                to_remove.push(slot.id());
            }
        }

        let progressed = !to_remove.is_empty();
        for slot_id in to_remove {
            self.queued_continuations.remove(&slot_id);
            if let Some(mut slot) = self.remove_client(slot_id) {
                if let Some(stream) = slot.stream.as_mut() {
                    let _ = poll_registry.deregister(stream);
                }
                cleanup_slot(slot, registry, server);
            }
        }
        progressed
    }

    fn close_all_clients(
        &mut self,
        poll_registry: &MioRegistry,
        registry: &Arc<Mutex<PubSubRegistry>>,
        server: &Arc<redis_core::RedisServer>,
    ) {
        let ids: Vec<SlotId> = self.slots.iter().flatten().map(ClientSlot::id).collect();
        for slot_id in ids {
            self.queued_continuations.remove(&slot_id);
            if let Some(mut slot) = self.remove_client(slot_id) {
                if let Some(stream) = slot.stream.as_mut() {
                    let _ = poll_registry.deregister(stream);
                }
                cleanup_slot(slot, registry, server);
            }
        }
    }
}

fn token_for_slot(slot_id: SlotId) -> Token {
    Token(SLOT_TOKEN_BASE + slot_id.as_index())
}

fn slot_id_from_token(token: Token) -> Option<SlotId> {
    token
        .0
        .checked_sub(SLOT_TOKEN_BASE)
        .and_then(SlotId::from_index)
}

fn client_interest(writable: bool) -> Interest {
    if writable {
        Interest::READABLE.add(Interest::WRITABLE)
    } else {
        Interest::READABLE
    }
}

fn is_debug_loadaof_command(client: &Client) -> bool {
    if client.argv.len() != 2 {
        return false;
    }
    let Some(cmd) = client.arg(0) else {
        return false;
    };
    let Some(subcmd) = client.arg(1) else {
        return false;
    };
    cmd.as_bytes().eq_ignore_ascii_case(b"DEBUG")
        && subcmd.as_bytes().eq_ignore_ascii_case(b"LOADAOF")
}

fn spawn_debug_loadaof_job(
    slot_id: SlotId,
    server: Arc<redis_core::RedisServer>,
    db_count: usize,
) -> DebugLoadAofJob {
    let cfg = Arc::clone(&server.live_config);
    let dir = PathBuf::from(cfg.rdb_dir());
    let filename = cfg.appendfilename();
    let dirname = cfg.appenddirname();
    let options = redis_commands::aof::AofLoadOptions {
        load_truncated: cfg.aof_load_truncated(),
        allow_rdb_preamble: cfg.aof_use_rdb_preamble(),
        lua_time_limit_ms: cfg.lua_time_limit_ms(),
    };
    let (tx, rx) = mpsc::channel::<DebugLoadAofResult>();
    std::thread::spawn(move || {
        let count = db_count.max(1);
        let mut loaded: Vec<RedisDb> = (0..count as u32).map(RedisDb::new).collect();
        let result = redis_commands::aof::load_append_only_files(
            &dir,
            &filename,
            &dirname,
            &mut loaded,
            options,
        )
        .map(|summary| (loaded, summary));
        let _ = tx.send(result);
    });
    DebugLoadAofJob { slot_id, rx }
}

fn loading_error_reply() -> Vec<u8> {
    error_payload_reply(RedisError::loading().to_resp_payload().as_bytes())
}

fn debug_loadaof_error_reply(err: &io::Error) -> Vec<u8> {
    error_payload_reply(format!("ERR DEBUG LOADAOF failed: {}", err).as_bytes())
}

fn error_payload_reply(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 3);
    out.push(b'-');
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
    out
}

fn has_complete_command(bytes: &[u8]) -> bool {
    let mut argv: Vec<RedisString> = Vec::new();
    matches!(
        parse_inline_or_multibulk_into(bytes, &mut argv),
        Ok(Some(_))
    )
}

const QUERY_BUFFER_IOBUF_LEN: usize = 16 * 1024;
const QUERY_BUFFER_RESIZE_THRESHOLD: usize = 32 * 1024;
const QUERY_BUFFER_IDLE_SHRINK_AFTER: Duration = Duration::from_secs(2);
const CLIENT_INFO_MEMORY_REFRESH_INTERVAL: Duration = Duration::from_millis(100);

fn estimated_query_buffer_allocation(bytes: &[u8], consumed_hint: usize) -> usize {
    let observed = visible_query_buffer_len(bytes)
        .max(declared_bulk_argument_len(bytes))
        .max(consumed_hint);
    if observed == 0 {
        0
    } else if observed <= QUERY_BUFFER_RESIZE_THRESHOLD {
        QUERY_BUFFER_IOBUF_LEN
    } else {
        observed
    }
}

fn visible_query_buffer_len(bytes: &[u8]) -> usize {
    incomplete_multibulk_first_payload_start(bytes)
        .map(|start| bytes.len().saturating_sub(start))
        .unwrap_or(bytes.len())
}

fn declared_bulk_argument_len(bytes: &[u8]) -> usize {
    let mut max_len = 0usize;
    let mut pos = 0usize;
    while let Some(rel) = bytes[pos..].iter().position(|&b| b == b'$') {
        let dollar = pos + rel;
        let line_start = dollar + 1;
        let Some(line_end) = find_crlf(bytes, line_start) else {
            break;
        };
        if line_end > line_start {
            let mut n = 0usize;
            let mut valid = true;
            for &b in &bytes[line_start..line_end] {
                if !b.is_ascii_digit() {
                    valid = false;
                    break;
                }
                n = n.saturating_mul(10).saturating_add((b - b'0') as usize);
            }
            if valid {
                max_len = max_len.max(n);
            }
        }
        pos = line_end + 2;
    }
    max_len
}

fn incomplete_multibulk_first_payload_start(bytes: &[u8]) -> Option<usize> {
    if !bytes.starts_with(b"*") {
        return None;
    }
    let array_line_end = find_crlf(bytes, 1)?;
    let bulk_prefix = array_line_end + 2;
    if bytes.get(bulk_prefix) != Some(&b'$') {
        return None;
    }
    let bulk_line_end = find_crlf(bytes, bulk_prefix + 1)?;
    Some(bulk_line_end + 2)
}

fn find_crlf(bytes: &[u8], start: usize) -> Option<usize> {
    bytes
        .get(start..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|pos| start + pos)
}

fn client_exceeds_own_memory_limit_after_parse(
    slot: &ClientSlot,
    server: &redis_core::RedisServer,
    consumed_total: usize,
) -> bool {
    if !slot.can_be_evicted_for_memory() {
        return false;
    }
    let limit = get_client_eviction_limit(
        server.live_config.maxmemory_clients(),
        server.live_config.maxmemory(),
    );
    limit > 0 && slot.client_memory_usage_after_parsed_command(consumed_total) > limit
}

fn slot_command_is_paused(slot: &ClientSlot, paused_actions: u32) -> bool {
    if paused_actions & PAUSE_ACTION_CLIENT_ALL != 0 {
        return !pause_exempt_current_command(&slot.client.argv);
    }
    if paused_actions & PAUSE_ACTION_CLIENT_WRITE != 0 {
        return redis_commands::dispatch::command_is_paused_by_client_pause(
            &slot.client.argv,
            &slot.client,
        );
    }
    false
}

fn pause_exempt_current_command(argv: &[RedisString]) -> bool {
    let Some(name) = argv.first().map(|s| s.as_bytes()) else {
        return true;
    };
    if name.eq_ignore_ascii_case(b"CLIENT") {
        return argv.get(1).is_some_and(|subcmd| {
            subcmd.as_bytes().eq_ignore_ascii_case(b"UNPAUSE")
                || subcmd.as_bytes().eq_ignore_ascii_case(b"CAPA")
        });
    }
    name.eq_ignore_ascii_case(b"INFO")
        || name.eq_ignore_ascii_case(b"PING")
        || name.eq_ignore_ascii_case(b"SELECT")
        || name.eq_ignore_ascii_case(b"HELLO")
        || name.eq_ignore_ascii_case(b"AUTH")
        || name.eq_ignore_ascii_case(b"QUIT")
        || name.eq_ignore_ascii_case(b"RESET")
}

fn cleanup_slot(
    mut slot: ClientSlot,
    registry: &Arc<Mutex<PubSubRegistry>>,
    server: &redis_core::RedisServer,
) {
    let id = slot.client.id;
    slot.clear_pause_postponed();
    let _ = redis_commands::pubsub::drop_client_from_registry(registry, id);
    remove_replica_for_disconnect(id, server);
    redis_core::tracking::remove_runtime_client_tracking(id);
    redis_core::db::watched_keys_index_remove_client(id);
    let _ = redis_core::db::watched_keys_take_dirty(id);
    slot.client.clear_blocked_on_keys();
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.deregister(id);
    }
    if let Some(stream) = slot.stream.take() {
        let _ = stream.shutdown(Shutdown::Both);
    }
    server_metrics().on_disconnect();
}

fn remove_replica_for_disconnect(id: u64, server: &redis_core::RedisServer) {
    let repl = redis_core::replication::global_replication_state();
    let outcome = repl.remove_replica(id);
    let Some(child_pid) = outcome.useless_repl_child_pid else {
        return;
    };
    if server.live_config.save_enabled() {
        return;
    }

    #[cfg(unix)]
    unsafe {
        let _ = libc::kill(child_pid as libc::pid_t, libc::SIGUSR1);
    }
    eprintln!(
        "redis-server: replication BGSAVE child {} has no waiting replicas; cancellation requested",
        child_pid
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_id_round_trips_as_newtype() {
        let slot_id = SlotId::new(42);
        assert_eq!(slot_id.as_u32(), 42);
        assert_eq!(slot_id.as_index(), 42);
        assert_eq!(slot_id_from_token(token_for_slot(slot_id)), Some(slot_id));
        assert_eq!(slot_id_from_token(Token(0)), None);
    }

    #[test]
    fn complete_command_probe_distinguishes_partial_buffers() {
        assert!(has_complete_command(b"*1\r\n$4\r\nPING\r\n"));
        assert!(!has_complete_command(b"*1\r\n$4\r\nPI"));
    }

    #[test]
    fn write_buffer_preserves_order_and_drains() {
        let mut buffer = ClientWriteBuffer::new();
        assert!(buffer.is_empty());

        buffer.append(b"+OK");
        buffer.append(b"\r\n");

        assert_eq!(buffer.len(), 5);
        assert_eq!(buffer.as_bytes(), b"+OK\r\n");
        assert_eq!(buffer.take(), b"+OK\r\n");
        assert!(buffer.is_empty());

        buffer.append_owned(b"abcdef".to_vec());
        buffer.consume_front(2);
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.as_bytes(), b"cdef");
        buffer.append_owned(b"gh".to_vec());
        assert_eq!(buffer.as_bytes(), b"cdefgh");
        assert_eq!(buffer.take(), b"cdefgh");
        assert!(buffer.is_empty());
    }

    #[test]
    fn client_slot_owns_query_argv_and_write_staging() {
        let mut slot = ClientSlot::new(SlotId::new(3), Client::new(99));

        slot.ingest(b"*1\r\n");
        slot.ingest(b"$4\r\nPING\r\n");
        slot.stage_argv(vec![RedisString::from_static(b"PING")]);
        slot.queue_write(b"+PONG\r\n");

        assert_eq!(slot.id(), SlotId::new(3));
        assert_eq!(slot.client().id(), 99);
        assert_eq!(slot.query_buffer(), b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(slot.argv()[0].as_bytes(), b"PING");
        assert_eq!(slot.pending_write_len(), 7);
        assert_eq!(slot.take_pending_write(), b"+PONG\r\n");

        slot.clear_query_buffer();
        slot.mark_closed();
        assert!(slot.query_buffer().is_empty());
        assert!(slot.is_closed());
    }

    #[test]
    fn replica_slot_write_drain_updates_replication_pending_output() {
        let repl = redis_core::replication::global_replication_state();
        let replica_id = 9_900_123;
        repl.remove_replica(replica_id);

        let (tx, _rx) = mpsc::channel();
        repl.add_replica(redis_core::replication::ReplicaConn::new(
            replica_id,
            redis_core::replication::ReplicaState::SendingRdb,
            0,
            tx,
        ));
        assert!(repl.send_to_replica(replica_id, b"abcdef".to_vec()));

        let mut client = Client::new(replica_id);
        client.is_replica = true;
        let mut slot = ClientSlot::new(SlotId::new(4), client);
        let pending_for_replica = || {
            let guard = match repl.replicas.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard
                .get(&replica_id)
                .expect("replica record")
                .pending_output_bytes
                .load(Ordering::Relaxed)
        };

        slot.note_written_bytes(2);
        assert_eq!(pending_for_replica(), 4);
        assert_eq!(
            repl.replicas_snapshot()
                .into_iter()
                .find(|(id, _, _, _, _)| *id == replica_id)
                .map(|(_, state, _, _, _)| state),
            Some("send_bulk")
        );

        slot.note_written_bytes(4);
        assert_eq!(pending_for_replica(), 0);
        assert_eq!(
            repl.replicas_snapshot()
                .into_iter()
                .find(|(id, _, _, _, _)| *id == replica_id)
                .map(|(_, state, _, _, _)| state),
            Some("send_bulk")
        );

        repl.acknowledge_replica(replica_id, 6, None, 1_000);
        assert_eq!(
            repl.replicas_snapshot()
                .into_iter()
                .find(|(id, _, _, _, _)| *id == replica_id)
                .map(|(_, state, _, _, _)| state),
            Some("online")
        );

        repl.remove_replica(replica_id);
    }

    #[test]
    fn runtime_owner_constructs_disabled_and_inert() {
        let owner = RuntimeOwner::disabled();

        assert!(!owner.config().enabled());
        assert_eq!(owner.database_count(), DEFAULT_DATABASE_COUNT as usize);
        assert_eq!(owner.active_slot_count(), 0);
        assert!(owner.is_event_queue_empty());
        assert!(!owner.poll_driver().is_installed());
    }

    #[test]
    fn runtime_event_queue_is_fifo_and_capacity_limited() {
        let config = RuntimeOwnerConfig::disabled().with_max_pending_events(2);
        let mut owner = RuntimeOwner::new(config);

        let publish = RuntimeEvent::Publish {
            channel: RedisString::from_static(b"chan"),
            payload: RedisString::from_static(b"payload"),
        };
        let wake = RuntimeEvent::WakeBlocked {
            slot_id: SlotId::new(8),
            reason: RedisString::from_static(b"ready"),
        };
        let overflow = RuntimeEvent::ShutdownRequested;

        assert_eq!(owner.queue_event(publish.clone()), Ok(()));
        assert_eq!(owner.queue_event(wake.clone()), Ok(()));
        assert_eq!(owner.queue_event(overflow.clone()), Err(overflow));
        assert_eq!(owner.event_queue_len(), 2);
        assert_eq!(owner.pop_event(), Some(publish));
        assert_eq!(owner.pop_event(), Some(wake));
        assert_eq!(owner.pop_event(), None);
    }

    #[test]
    fn runtime_owner_allocates_reuses_and_queues_slot_writes() {
        let mut owner = RuntimeOwner::new(
            RuntimeOwnerConfig::disabled()
                .with_database_count(1)
                .with_max_pending_events(4),
        );

        let first = owner.insert_client(Client::new(1)).unwrap();
        let second = owner.insert_client(Client::new(2)).unwrap();
        assert_eq!(first, SlotId::new(0));
        assert_eq!(second, SlotId::new(1));
        assert_eq!(owner.active_slot_count(), 2);

        assert!(owner.queue_write(first, b"+OK"));
        assert!(owner.queue_write(first, b"\r\n"));
        assert_eq!(owner.take_pending_write(first), Some(b"+OK\r\n".to_vec()));

        let removed = owner.remove_client(first).unwrap();
        assert_eq!(removed.id(), first);
        assert_eq!(owner.active_slot_count(), 1);

        let reused = owner.insert_client(Client::new(3)).unwrap();
        assert_eq!(reused, first);
        assert!(owner.slot(second).is_some());
    }

    #[test]
    fn replica_fullsync_load_resets_apply_db_before_db0_catchup() {
        fn argv(parts: &[&[u8]]) -> Vec<RedisString> {
            parts
                .iter()
                .map(|part| RedisString::from_bytes(part))
                .collect()
        }

        let mut owner = RuntimeOwner::new(
            RuntimeOwnerConfig::disabled()
                .with_database_count(16)
                .with_max_pending_events(4),
        );
        let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
        let server = Arc::new(redis_core::RedisServer::default());

        assert!(
            owner.apply_replica_command_batch(vec![argv(&[b"SELECT", b"11"])], &registry, &server),
            "stale pre-fullsync upstream stream DB should be observable"
        );
        assert_eq!(owner.replica_apply_db_index, 11);

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "valdr-runtime-owner-fullsync-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp RDB dir");
        let rdb_path = dir.join("dump.rdb");

        let mut source_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let mut db11_set = redis_core::object::RedisObject::new_set();
        db11_set
            .set_mut()
            .expect("new_set constructs a set")
            .insert(RedisString::from_static(b"-912526146933"));
        source_dbs[11].add(RedisString::from_static(b"929"), db11_set);
        redis_core::rdb::save_rdb_databases(&source_dbs, &rdb_path).expect("save fullsync RDB");
        let rdb_bytes = std::fs::read(&rdb_path).expect("read fullsync RDB");
        let _ = std::fs::remove_dir_all(&dir);

        assert!(
            owner.load_replica_rdb(&rdb_bytes, &server),
            "fullsync RDB should load"
        );
        assert_eq!(
            owner.replica_apply_db_index, 0,
            "fullsync RDB load must reset the replica apply selected DB"
        );

        assert!(
            owner.apply_replica_command_batch(
                vec![argv(&[b"SADD", b"929", b"-1822692859"])],
                &registry,
                &server,
            ),
            "DB 0 catch-up SADD without SELECT should apply after fullsync"
        );

        let db0_set = owner.dbs[0]
            .lookup_key_read(b"929")
            .and_then(|obj| obj.set())
            .expect("DB 0 should receive the post-RDB catch-up set member");
        assert!(db0_set.contains(&RedisString::from_static(b"-1822692859")));
        assert!(!db0_set.contains(&RedisString::from_static(b"-912526146933")));

        let db11_set = owner.dbs[11]
            .lookup_key_read(b"929")
            .and_then(|obj| obj.set())
            .expect("DB 11 should retain the RDB set member");
        assert!(db11_set.contains(&RedisString::from_static(b"-912526146933")));
        assert!(
            !db11_set.contains(&RedisString::from_static(b"-1822692859")),
            "DB 0 catch-up member must not merge into DB 11 after fullsync"
        );
    }

    #[test]
    fn replica_apply_batch_preserves_multi_state_until_exec() {
        fn argv(parts: &[&[u8]]) -> Vec<RedisString> {
            parts
                .iter()
                .map(|part| RedisString::from_bytes(part))
                .collect()
        }

        let mut owner = RuntimeOwner::new(
            RuntimeOwnerConfig::disabled()
                .with_database_count(1)
                .with_max_pending_events(4),
        );
        let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
        let server = Arc::new(redis_core::RedisServer::default());

        assert!(
            owner.apply_replica_command_batch(
                vec![
                    argv(&[b"MULTI"]),
                    argv(&[b"SET", b"tx-key", b"v"]),
                    argv(&[b"EXEC"]),
                ],
                &registry,
                &server,
            ),
            "replicated MULTI/EXEC catch-up must be applied with one pseudo-client"
        );
        assert_eq!(
            owner.dbs[0]
                .lookup_key_read(b"tx-key")
                .expect("transaction key")
                .string_bytes()
                .as_ref(),
            b"v"
        );

        assert!(
            !owner.apply_replica_command_batch(vec![argv(&[b"EXEC"])], &registry, &server),
            "a malformed replicated transaction envelope should still fail"
        );
    }

    #[test]
    fn owner_command_result_carries_slot_and_terminal_state() {
        let slot_id = SlotId::new(11);
        let replied = OwnerCommandResult::Replied { slot_id };
        let closed = OwnerCommandResult::Closed { slot_id };

        assert_eq!(replied.slot_id(), slot_id);
        assert!(!replied.is_terminal());
        assert_eq!(closed.slot_id(), slot_id);
        assert!(closed.is_terminal());
    }

    #[test]
    fn failover_pause_exempts_client_capa_but_pauses_data_reads() {
        let mut capa = ClientSlot::new(SlotId::new(12), Client::new(120));
        capa.stage_argv(vec![
            RedisString::from_static(b"CLIENT"),
            RedisString::from_static(b"CAPA"),
            RedisString::from_static(b"REDIRECT"),
        ]);
        assert!(
            !slot_command_is_paused(&capa, PAUSE_ACTION_CLIENT_ALL),
            "CLIENT CAPA must remain available so redirect-aware clients can \
             declare capability during failover"
        );

        let mut select = ClientSlot::new(SlotId::new(14), Client::new(122));
        select.stage_argv(vec![
            RedisString::from_static(b"SELECT"),
            RedisString::from_static(b"9"),
        ]);
        assert!(
            !slot_command_is_paused(&select, PAUSE_ACTION_CLIENT_ALL),
            "SELECT must remain available so newly accepted deferring clients \
             can finish connection setup during failover"
        );

        let mut get = ClientSlot::new(SlotId::new(13), Client::new(121));
        get.stage_argv(vec![
            RedisString::from_static(b"GET"),
            RedisString::from_static(b"foo"),
        ]);
        assert!(
            slot_command_is_paused(&get, PAUSE_ACTION_CLIENT_ALL),
            "data reads should still be paused during failover"
        );
    }

    #[test]
    fn failover_all_pause_counts_postponed_data_but_allows_info() {
        static PAUSE_TEST_GUARD: Mutex<()> = Mutex::new(());
        let _guard = PAUSE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());

        fn resp(parts: &[&[u8]]) -> Vec<u8> {
            let mut out = Vec::new();
            out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
            for part in parts {
                out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
                out.extend_from_slice(part);
                out.extend_from_slice(b"\r\n");
            }
            out
        }

        struct PauseCleanup {
            server: Arc<redis_core::RedisServer>,
        }

        impl Drop for PauseCleanup {
            fn drop(&mut self) {
                redis_core::networking::clear_failover_pause(&self.server);
                while redis_core::networking::pause_postponed_client_count() > 0 {
                    redis_core::networking::note_pause_resumed_client();
                }
            }
        }

        let server = Arc::new(redis_core::RedisServer::default());
        let _cleanup = PauseCleanup {
            server: Arc::clone(&server),
        };
        let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
        let mut owner = RuntimeOwner::new(
            RuntimeOwnerConfig::disabled()
                .with_database_count(1)
                .with_max_pending_events(4),
        );
        redis_core::networking::apply_failover_pause(&server, i64::MAX);

        let data_slot = owner.insert_client(Client::new(130)).unwrap();
        {
            let slot = owner.slot_mut(data_slot).expect("data slot");
            slot.client_mut().capa_redirect = true;
            slot.ingest(&resp(&[b"GET", b"foo"]));
        }
        let data_outcome = owner.dispatch_slot_commands(data_slot.as_index(), &registry, &server);
        assert!(data_outcome.progressed);
        assert!(!data_outcome.queued_write);
        assert!(
            owner.slot(data_slot).expect("data slot").pause_postponed,
            "failover all-client pause should park data commands before dispatch"
        );
        assert_eq!(
            pause_postponed_client_count(),
            1,
            "INFO blocked_clients relies on pause-postponed clients being counted"
        );

        let select_slot = owner.insert_client(Client::new(132)).unwrap();
        owner
            .slot_mut(select_slot)
            .expect("select slot")
            .ingest(&resp(&[b"SELECT", b"0"]));
        let select_outcome =
            owner.dispatch_slot_commands(select_slot.as_index(), &registry, &server);
        assert!(select_outcome.progressed);
        assert!(select_outcome.queued_write);
        assert_eq!(
            owner.take_pending_write(select_slot),
            Some(b"+OK\r\n".to_vec()),
            "deferring client setup SELECT should not be parked by failover pause"
        );
        assert_eq!(
            pause_postponed_client_count(),
            1,
            "pause-postponed count should still only include the data command"
        );

        let info_slot = owner.insert_client(Client::new(131)).unwrap();
        owner
            .slot_mut(info_slot)
            .expect("info slot")
            .ingest(&resp(&[b"INFO", b"clients"]));
        let info_outcome = owner.dispatch_slot_commands(info_slot.as_index(), &registry, &server);
        assert!(info_outcome.progressed);
        assert!(info_outcome.queued_write);
        let reply = owner
            .take_pending_write(info_slot)
            .expect("INFO clients should reply during failover pause");
        assert!(
            reply
                .windows(b"blocked_clients:1".len())
                .any(|w| w == b"blocked_clients:1"),
            "INFO clients should remain pause-exempt and expose postponed clients, got {:?}",
            String::from_utf8_lossy(&reply)
        );
        assert!(
            reply
                .windows(b"paused_actions:all".len())
                .any(|w| w == b"paused_actions:all"),
            "INFO clients should report failover all-client pause, got {:?}",
            String::from_utf8_lossy(&reply)
        );

        owner
            .slot_mut(data_slot)
            .expect("data slot")
            .clear_pause_postponed();
        redis_core::networking::clear_failover_pause(&server);
    }
}

// --------------------------------------------------------------------------
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-server
//   confidence:    high
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Mio readiness plain-TCP owner loop with stable slot tokens
//                  and owner-owned live DB storage for normal command dispatch.
//                  Dead-code pass: create_databases free fn deleted (inlined
//                  into RuntimeOwner::new); owner-loop vocabulary items
//                  annotated with #[allow(dead_code)] per object-vocabulary.tsv.
//                  Replica writer drain now clears output memory only; ACK
//                  timing owns send_bulk -> online promotion.
// --------------------------------------------------------------------------
