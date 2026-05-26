//! RuntimeOwner mio readiness plain-TCP path.
//!
//! This module names the owner-loop vocabulary from
//! `harness/architecture/object-vocabulary.tsv` and implements the bounded
//! `mio` owner loop approved by
//! `harness/architecture/decisions/runtime-ownership.md`.
//!
//! RuntimeOwner owns accepted plain-TCP sockets, client parser state, per-slot
//! foreign payload receivers, ordinary reply flushing, and the live
//! `Vec<RedisDb>` used by normal command execution. Commands still enter
//! `redis_commands::dispatch` through `CommandContext`; the context DB-list
//! route points at the owner-held DB slice instead of `global_databases()`.

use std::collections::{HashSet, VecDeque};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use mio::{Events, Interest, Poll, Registry as MioRegistry, Token};
use redis_core::client_info::client_info_registry;
use redis_core::db::RedisDb;
use redis_core::eviction::{try_evict_to_fit, EvictionOutcome};
use redis_core::expire::run_active_expire_tick_on_db;
use redis_core::memory::approximate_memory_used;
use redis_core::metrics::server_metrics;
use redis_core::networking::get_client_eviction_limit;
use redis_core::pubsub_registry::PubSubRegistry;
use redis_core::{Client, Connection};
use redis_protocol::parse_inline_or_multibulk_into;
use redis_types::RedisString;

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
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

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
///
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

    pub const fn is_installed(self) -> bool {
        self.installed
    }

    pub const fn epoch(self) -> u64 {
        self.epoch
    }
}

/// Typed knobs for owner-loop experiments.
///
/// `enabled` defaults to false. Constructing this value does not change the
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

    pub const fn enabled(self) -> bool {
        self.enabled
    }

    pub const fn database_count(self) -> u32 {
        self.database_count
    }

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
    obuf_soft_limit_since: Option<Instant>,
    writable_interest: bool,
    closed: bool,
    close_after_flush: bool,
}

impl ClientSlot {
    pub fn new(id: SlotId, client: Client) -> Self {
        Self {
            id,
            client,
            stream: None,
            foreign_rx: None,
            write_buffer: ClientWriteBuffer::new(),
            output_accounted_bytes: 0,
            obuf_soft_limit_since: None,
            writable_interest: false,
            closed: false,
            close_after_flush: false,
        }
    }

    fn with_stream(
        id: SlotId,
        client: Client,
        stream: MioTcpStream,
        foreign_rx: Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            id,
            client,
            stream: Some(stream),
            foreign_rx: Some(foreign_rx),
            write_buffer: ClientWriteBuffer::new(),
            output_accounted_bytes: 0,
            obuf_soft_limit_since: None,
            writable_interest: false,
            closed: false,
            close_after_flush: false,
        }
    }

    pub fn id(&self) -> SlotId {
        self.id
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    pub fn ingest(&mut self, bytes: &[u8]) {
        self.client.query_buf.extend_from_slice(bytes);
    }

    pub fn query_buffer(&self) -> &[u8] {
        &self.client.query_buf
    }

    pub fn clear_query_buffer(&mut self) {
        self.client.query_buf.clear();
    }

    pub fn stage_argv(&mut self, argv: Vec<RedisString>) {
        self.client.argv = argv;
    }

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

    pub fn pending_write_len(&self) -> usize {
        self.write_buffer.len()
    }

    pub fn take_pending_write(&mut self) -> Vec<u8> {
        self.write_buffer.take()
    }

    pub fn mark_closed(&mut self) {
        self.closed = true;
    }

    fn refresh_output_buffer_state(&mut self) {
        self.refresh_output_buffer_state_at(Instant::now());
    }

    fn refresh_output_buffer_state_at(&mut self, now: Instant) {
        let pending = self.output_accounted_bytes;
        if let Ok(mut guard) = client_info_registry().lock() {
            guard.set_output_buffer_memory(self.client.id, pending);
        }
        self.refresh_client_memory_snapshot();
        let limit =
            redis_commands::connection::client_output_buffer_limit(self.client.in_pubsub_mode());
        if limit.hard > 0 && pending > limit.hard {
            self.mark_closed();
            return;
        }
        if limit.soft > 0 && limit.soft_seconds > 0 && pending > limit.soft {
            let since = *self.obuf_soft_limit_since.get_or_insert(now);
            if now.duration_since(since) >= Duration::from_secs(limit.soft_seconds) {
                self.mark_closed();
            }
        } else {
            self.obuf_soft_limit_since = None;
        }
    }

    fn reconcile_output_buffer_after_write(&mut self) {
        if !self.client.in_pubsub_mode() {
            self.output_accounted_bytes = self.write_buffer.len();
        }
        self.refresh_output_buffer_state();
    }

    fn mark_close_after_flush(&mut self) {
        self.close_after_flush = true;
    }

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
            .saturating_add(self.write_buffer.len())
            .saturating_add(query_len)
            .saturating_add(argv_mem)
            .saturating_add(multi_mem)
            .saturating_add(self.subscription_memory_usage())
            .saturating_add(self.tracking_memory_usage())
            .saturating_add(self.watched_key_memory_usage())
            .saturating_add(self.name_memory_usage())
    }

    fn refresh_client_memory_snapshot(&self) {
        if let Ok(mut guard) = client_info_registry().lock() {
            guard.set_memory_usage(
                self.client.id,
                visible_query_buffer_len(&self.client.query_buf),
                self.current_argv_memory_usage(),
                self.multi_memory_usage(),
                self.client_memory_usage(),
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

    pub fn into_client(self) -> Client {
        self.client
    }
}

/// Single ordered event stream from background subsystems into the owner.
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerCommandResult {
    Replied { slot_id: SlotId },
    Blocked { slot_id: SlotId },
    Closed { slot_id: SlotId },
    PendingMore { slot_id: SlotId },
}

impl OwnerCommandResult {
    pub fn slot_id(self) -> SlotId {
        match self {
            Self::Replied { slot_id }
            | Self::Blocked { slot_id }
            | Self::Closed { slot_id }
            | Self::PendingMore { slot_id } => slot_id,
        }
    }

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

/// Owner of normal plain-TCP command execution for the mio readiness path.
pub struct RuntimeOwner {
    config: RuntimeOwnerConfig,
    poll_driver: PollDriverHandle,
    slots: Vec<Option<ClientSlot>>,
    free_slots: Vec<SlotId>,
    continuation_queue: VecDeque<SlotId>,
    queued_continuations: HashSet<SlotId>,
    dbs: Vec<RedisDb>,
    active_expire_cursor: usize,
    last_active_expire: Instant,
    events: VecDeque<RuntimeEvent>,
    replica_apply_rx: Option<Receiver<redis_commands::replica_dialer::ReplicaApplyRequest>>,
    replica_apply_db_index: u32,
}

impl RuntimeOwner {
    pub fn new(config: RuntimeOwnerConfig) -> Self {
        let dbs = create_databases(config.database_count());
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
            replica_apply_db_index: 0,
        }
    }

    pub fn disabled() -> Self {
        Self::new(RuntimeOwnerConfig::disabled())
    }

    pub fn config(&self) -> RuntimeOwnerConfig {
        self.config
    }

    pub fn poll_driver(&self) -> PollDriverHandle {
        self.poll_driver
    }

    pub fn database_count(&self) -> usize {
        self.dbs.len()
    }

    pub fn dbs(&self) -> &[RedisDb] {
        &self.dbs
    }

    pub fn dbs_mut(&mut self) -> &mut [RedisDb] {
        &mut self.dbs
    }

    pub fn active_slot_count(&self) -> usize {
        self.slots.iter().filter(|slot| slot.is_some()).count()
    }

    pub fn event_queue_len(&self) -> usize {
        self.events.len()
    }

    pub fn is_event_queue_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn queue_event(&mut self, event: RuntimeEvent) -> Result<(), RuntimeEvent> {
        if self.events.len() >= self.config.max_pending_events() {
            return Err(event);
        }
        self.events.push_back(event);
        Ok(())
    }

    pub fn pop_event(&mut self) -> Option<RuntimeEvent> {
        self.events.pop_front()
    }

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
        initial_dbs: Vec<RedisDb>,
        replica_apply_rx: Receiver<redis_commands::replica_dialer::ReplicaApplyRequest>,
    ) {
        let _ = tcp_port;
        let mut listeners: Vec<MioTcpListener> = listeners
            .into_iter()
            .map(MioTcpListener::from_std)
            .collect();
        if listeners.is_empty() {
            eprintln!("redis-server: no plain TCP listeners installed");
            return;
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
        eprintln!("redis-server: RuntimeOwner mio plain TCP loop enabled with owner-owned DBs");

        while !shutdown.load(Ordering::SeqCst) {
            let mut progressed = false;

            progressed |= owner.active_expire_step();
            progressed |= owner.drain_replica_apply_requests(&registry, &server);
            progressed |= owner.dispatch_scheduled_commands(poll.registry(), &registry, &server);
            progressed |= owner.install_pending_dynamic_listeners(&mut listeners, poll.registry());

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
            progressed |= owner.drain_replica_apply_requests(&registry, &server);
            progressed |= owner.dispatch_scheduled_commands(poll.registry(), &registry, &server);
            progressed |= owner.install_pending_dynamic_listeners(&mut listeners, poll.registry());
            owner.sweep_output_buffer_limits();
            progressed |= owner.enforce_client_memory_limits(&server);
            progressed |= owner.cleanup_closed_clients(poll.registry(), &registry);

            let _ = progressed;
        }

        owner.close_all_clients(poll.registry(), &registry);
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

    fn accept_ready(
        &mut self,
        listener: &MioTcpListener,
        poll_registry: &MioRegistry,
        next_client_id: &Arc<AtomicU64>,
        registry: &Arc<Mutex<PubSubRegistry>>,
    ) -> bool {
        let mut progressed = false;
        loop {
            match listener.accept() {
                Ok((stream, peer_addr)) => {
                    progressed = true;
                    let metrics = server_metrics();
                    let current = metrics.connected_clients.load(Ordering::Relaxed);
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

    pub fn slot(&self, slot_id: SlotId) -> Option<&ClientSlot> {
        self.slots.get(slot_id.as_index())?.as_ref()
    }

    pub fn slot_mut(&mut self, slot_id: SlotId) -> Option<&mut ClientSlot> {
        self.slots.get_mut(slot_id.as_index())?.as_mut()
    }

    pub fn queue_write(&mut self, slot_id: SlotId, bytes: &[u8]) -> bool {
        let slot = match self.slot_mut(slot_id) {
            Some(slot) => slot,
            None => return false,
        };
        slot.queue_write(bytes);
        true
    }

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
                    progressed |= self.accept_ready(
                        &listeners[event.token().0],
                        poll_registry,
                        next_client_id,
                        registry,
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
                if outcome.queued_write {
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
                progressed |= self.ensure_writable_interest(poll_registry, slot_id);
            }
            if outcome.reschedule {
                self.schedule_command_continuation(slot_id);
            }
        }
        progressed
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
            let ok = self.apply_replica_command(request.argv, registry, server);
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

        let mut client = Client::new(0);
        client.replication_apply = true;
        client.suppress_monitor = true;
        client.authenticated_user = Some(RedisString::from_bytes(b"default"));
        client.db_index = self.replica_apply_db_index;
        client.set_args(argv);

        super::process_current_command_with_db_list(&mut client, &mut self.dbs, registry, server);
        self.replica_apply_db_index = client.db_index;

        !client.reply_buf.starts_with(b"-")
    }

    fn read_slot(&mut self, idx: usize, read_buf: &mut [u8; READ_BUFFER_SIZE]) -> bool {
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
            progressed = true;
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
        if slot.closed || slot.close_after_flush || slot.client.query_buf.is_empty() {
            return SlotDispatchOutcome::default();
        }

        let mut consumed_total = 0usize;
        let mut commands = 0usize;
        let mut saw_command = false;
        let mut last_cmd_name: Vec<u8> = Vec::new();

        while commands < MAX_COMMANDS_PER_SLOT_TICK {
            if let Some(err) = super::unauthenticated_protocol_limit_error(
                &slot.client,
                &slot.client.query_buf[consumed_total..],
            ) {
                super::queue_error_reply(&mut slot.client, &err);
                slot.mark_close_after_flush();
                break;
            }
            let parsed = parse_inline_or_multibulk_into(
                &slot.client.query_buf[consumed_total..],
                &mut slot.client.argv,
            );
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
                }
                Ok(None) => break,
                Err(err) => {
                    super::queue_error_reply(&mut slot.client, &err);
                    slot.mark_close_after_flush();
                    break;
                }
            }
        }

        let reschedule = commands == MAX_COMMANDS_PER_SLOT_TICK
            && consumed_total < slot.client.query_buf.len()
            && has_complete_command(&slot.client.query_buf[consumed_total..]);

        if consumed_total > 0 {
            if consumed_total >= slot.client.query_buf.len() {
                slot.client.query_buf.clear();
            } else {
                slot.client.query_buf.drain(..consumed_total);
            }
        }

        // Command dispatch has already applied CLIENT REPLY OFF/SKIP while
        // retaining Pub/Sub push bytes in the shared reply buffer.
        let reply = slot.client.drain_reply();
        let queued_write = !reply.is_empty();
        if !reply.is_empty() {
            slot.queue_write_owned(reply);
        }
        slot.refresh_client_memory_snapshot();

        if saw_command {
            super::update_client_info_snapshot(&slot.client, &last_cmd_name);
        }

        SlotDispatchOutcome {
            progressed: saw_command || consumed_total > 0,
            queued_write,
            reschedule,
        }
    }

    fn active_expire_step(&mut self) -> bool {
        if self.dbs.is_empty() {
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

    fn enforce_client_memory_limits(&mut self, server: &redis_core::RedisServer) -> bool {
        let client_memory = self.total_client_memory();
        let maxmemory_clients = server.live_config.maxmemory_clients();
        let client_limit =
            get_client_eviction_limit(maxmemory_clients, server.live_config.maxmemory());
        if client_limit > 0 {
            return self.evict_clients_to_limit(client_limit, client_memory);
        }

        let maxmemory = server.live_config.maxmemory();
        if maxmemory == 0 {
            return false;
        }
        if maxmemory_clients == 0 && client_memory < 1024 * 1024 {
            return false;
        }
        let key_memory: u64 = self.dbs.iter().map(approximate_memory_used).sum();
        if key_memory.saturating_add(client_memory as u64) <= maxmemory {
            return false;
        }
        let target_key_memory = maxmemory.saturating_sub(client_memory as u64);
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
        let mut progressed = false;
        let slot = match self.slots.get_mut(idx).and_then(Option::as_mut) {
            Some(slot) => slot,
            None => return false,
        };
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
        while !buffer.is_empty() {
            match stream.write(buffer.as_bytes()) {
                Ok(0) => {
                    slot.mark_closed();
                    break;
                }
                Ok(n) => {
                    buffer.consume_front(n);
                    slot.client.net_output_bytes =
                        slot.client.net_output_bytes.saturating_add(n as u64);
                    if let Ok(mut guard) = client_info_registry().lock() {
                        guard.update_client_metadata(&slot.client);
                    }
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
        slot.reconcile_output_buffer_after_write();
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
                cleanup_slot(slot, registry);
            }
        }
        progressed
    }

    fn close_all_clients(
        &mut self,
        poll_registry: &MioRegistry,
        registry: &Arc<Mutex<PubSubRegistry>>,
    ) {
        let ids: Vec<SlotId> = self.slots.iter().flatten().map(ClientSlot::id).collect();
        for slot_id in ids {
            self.queued_continuations.remove(&slot_id);
            if let Some(mut slot) = self.remove_client(slot_id) {
                if let Some(stream) = slot.stream.as_mut() {
                    let _ = poll_registry.deregister(stream);
                }
                cleanup_slot(slot, registry);
            }
        }
    }
}

fn create_databases(count: u32) -> Vec<RedisDb> {
    let count = count.max(1);
    (0..count).map(RedisDb::new).collect()
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

fn has_complete_command(bytes: &[u8]) -> bool {
    let mut argv: Vec<RedisString> = Vec::new();
    matches!(
        parse_inline_or_multibulk_into(bytes, &mut argv),
        Ok(Some(_))
    )
}

fn visible_query_buffer_len(bytes: &[u8]) -> usize {
    incomplete_multibulk_first_payload_start(bytes)
        .map(|start| bytes.len().saturating_sub(start))
        .unwrap_or(bytes.len())
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

fn cleanup_slot(mut slot: ClientSlot, registry: &Arc<Mutex<PubSubRegistry>>) {
    let id = slot.client.id;
    let _ = redis_commands::pubsub::drop_client_from_registry(registry, id);
    redis_core::replication::global_replication_state().remove_replica(id);
    redis_core::tracking::remove_runtime_client_tracking(id);
    slot.client.clear_blocked_on_keys();
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.deregister(id);
    }
    if let Some(stream) = slot.stream.take() {
        let _ = stream.shutdown(Shutdown::Both);
    }
    server_metrics().on_disconnect();
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
        assert_eq!(slot_id_from_token(LISTENER_TOKEN), None);
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
    fn owner_command_result_carries_slot_and_terminal_state() {
        let slot_id = SlotId::new(11);
        let replied = OwnerCommandResult::Replied { slot_id };
        let closed = OwnerCommandResult::Closed { slot_id };

        assert_eq!(replied.slot_id(), slot_id);
        assert!(!replied.is_terminal());
        assert_eq!(closed.slot_id(), slot_id);
        assert!(closed.is_terminal());
    }
}

// --------------------------------------------------------------------------
// PORT STATUS
//   source:        src/server.c runtime-owner architecture packet
//   target_crate:  redis-server
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Mio readiness plain-TCP owner loop with stable slot tokens
//                  and owner-owned live DB storage for normal command dispatch.
// --------------------------------------------------------------------------
