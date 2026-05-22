//! RuntimeOwner std nonblocking plain-TCP experiment.
//!
//! This module names the owner-loop vocabulary from
//! `harness/architecture/object-vocabulary.tsv` and implements the bounded
//! std-only owner loop approved by
//! `harness/architecture/decisions/runtime-ownership.md`.
//!
//! The transitional DB model is intentional: command dispatch still locks the
//! existing `global_databases()` handles. RuntimeOwner owns plain-TCP sockets,
//! client parser state, per-slot foreign payload receivers, and ordinary reply
//! flushing, but it does not create a second live `Vec<RedisDb>`.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;

use redis_core::client_info::client_info_registry;
use redis_core::databases::global_databases;
use redis_core::metrics::server_metrics;
use redis_core::pubsub_registry::PubSubRegistry;
use redis_core::{Client, Connection};
use redis_protocol::parse_inline_or_multibulk_into;
use redis_types::RedisString;

const DEFAULT_DATABASE_COUNT: u32 = 16;
const DEFAULT_EVENT_CAPACITY: usize = 1024;
const READ_BUFFER_SIZE: usize = 16 * 1024;
const MAX_COMMANDS_PER_SLOT_TICK: usize = 128;

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

/// Abstract poller handle.
///
/// The concrete readiness backend is intentionally absent here. Choosing
/// `mio`, `polling`, `tokio`, or a raw platform poller is still a
/// TODO(human) decision in the runtime-ownership architecture doc.
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
    stream: Option<TcpStream>,
    foreign_rx: Option<Receiver<Vec<u8>>>,
    write_buffer: ClientWriteBuffer,
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
            closed: false,
            close_after_flush: false,
        }
    }

    fn with_stream(
        id: SlotId,
        client: Client,
        stream: TcpStream,
        foreign_rx: Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            id,
            client,
            stream: Some(stream),
            foreign_rx: Some(foreign_rx),
            write_buffer: ClientWriteBuffer::new(),
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
        self.write_buffer.append(bytes);
    }

    fn queue_write_owned(&mut self, bytes: Vec<u8>) {
        self.write_buffer.append_owned(bytes);
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

    fn mark_close_after_flush(&mut self) {
        self.close_after_flush = true;
    }

    pub fn is_closed(&self) -> bool {
        self.closed
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

/// Owner of normal plain-TCP command execution for the std experiment.
///
/// This is still transitional: it owns accepted plain-TCP sockets and client
/// slots, but dispatch reaches live keyspace state through `global_databases()`
/// handles rather than an owner-held `Vec<RedisDb>`.
pub struct RuntimeOwner {
    config: RuntimeOwnerConfig,
    poll_driver: PollDriverHandle,
    slots: Vec<Option<ClientSlot>>,
    free_slots: Vec<SlotId>,
    database_count: u32,
    events: VecDeque<RuntimeEvent>,
}

impl RuntimeOwner {
    pub fn new(config: RuntimeOwnerConfig) -> Self {
        Self {
            database_count: config.database_count(),
            config,
            poll_driver: PollDriverHandle::abstract_placeholder(),
            slots: Vec::new(),
            free_slots: Vec::new(),
            events: VecDeque::new(),
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
        self.database_count as usize
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
        stream: TcpStream,
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
        listener: TcpListener,
        shutdown: Arc<AtomicBool>,
        next_client_id: Arc<AtomicU64>,
        registry: Arc<Mutex<PubSubRegistry>>,
        server: Arc<redis_core::RedisServer>,
        tcp_port: u16,
    ) {
        let _ = tcp_port;
        if let Err(e) = listener.set_nonblocking(true) {
            eprintln!("redis-server: set_nonblocking(true) failed: {}", e);
        }

        let config = RuntimeOwnerConfig::disabled()
            .with_enabled(true)
            .with_database_count(global_databases().count() as u32);
        let mut owner = RuntimeOwner::new(config);
        eprintln!("redis-server: RuntimeOwner plain TCP loop enabled");

        while !shutdown.load(Ordering::SeqCst) {
            let mut progressed = false;
            progressed |= owner.accept_ready(&listener, &next_client_id, &registry);
            progressed |= owner.drain_foreign_payloads();
            progressed |= owner.read_ready_clients(&registry, &server);
            progressed |= owner.flush_pending_writes();
            progressed |= owner.cleanup_closed_clients(&registry);

            if !progressed {
                thread::yield_now();
            }
        }

        owner.close_all_clients(&registry);
    }

    fn accept_ready(
        &mut self,
        listener: &TcpListener,
        next_client_id: &Arc<AtomicU64>,
        registry: &Arc<Mutex<PubSubRegistry>>,
    ) -> bool {
        let mut progressed = false;
        loop {
            match listener.accept() {
                Ok((mut stream, peer_addr)) => {
                    progressed = true;
                    let metrics = server_metrics();
                    let current = metrics.connected_clients.load(Ordering::Relaxed);
                    let limit = redis_commands::connection::get_max_clients();
                    if current >= limit {
                        metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
                        let _ = stream.write_all(b"-ERR max number of clients reached\r\n");
                        drop(stream);
                        continue;
                    }

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
                    client.authenticated_user = super::determine_initial_user();

                    if self.insert_connected_client(client, stream, rx).is_some() {
                        metrics.on_connect();
                        metrics
                            .total_connections_received
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        if let Ok(mut guard) = registry.lock() {
                            guard.drop_client(id);
                        }
                        if let Ok(mut guard) = client_info_registry().lock() {
                            guard.deregister(id);
                        }
                        metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
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

    fn drain_foreign_payloads(&mut self) -> bool {
        let mut progressed = false;
        for slot in self.slots.iter_mut().flatten() {
            loop {
                let recv_result = match slot.foreign_rx.as_mut() {
                    Some(rx) => rx.try_recv(),
                    None => break,
                };
                match recv_result {
                    Ok(payload) => {
                        slot.queue_write_owned(payload);
                        progressed = true;
                    }
                    Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => {
                        break;
                    }
                }
            }
        }
        progressed
    }

    fn read_ready_clients(
        &mut self,
        registry: &Arc<Mutex<PubSubRegistry>>,
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        let mut progressed = false;
        let mut read_buf = [0u8; READ_BUFFER_SIZE];
        for idx in 0..self.slots.len() {
            progressed |= self.read_slot(idx, &mut read_buf);
            progressed |= self.dispatch_slot_commands(idx, registry, server);
        }
        progressed
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
            slot.ingest(&read_buf[..n]);
            progressed = true;
        }
        progressed
    }

    fn dispatch_slot_commands(
        &mut self,
        idx: usize,
        registry: &Arc<Mutex<PubSubRegistry>>,
        server: &Arc<redis_core::RedisServer>,
    ) -> bool {
        let slot = match self.slots.get_mut(idx).and_then(Option::as_mut) {
            Some(slot) => slot,
            None => return false,
        };
        if slot.closed || slot.close_after_flush || slot.client.query_buf.is_empty() {
            return false;
        }

        let db0 = global_databases().get(0);
        let mut batch_db0_guard = if slot.client.db_index == 0 {
            Some(super::lock_redis_db(&db0))
        } else {
            None
        };
        let mut consumed_total = 0usize;
        let mut commands = 0usize;
        let mut saw_command = false;
        let mut last_cmd_name: Vec<u8> = Vec::new();

        while commands < MAX_COMMANDS_PER_SLOT_TICK {
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

                    if slot.client.db_index == 0 {
                        if batch_db0_guard.is_none() {
                            batch_db0_guard = Some(super::lock_redis_db(&db0));
                        }
                        if let Some(db_guard) = batch_db0_guard.as_mut() {
                            super::process_current_command_with_db(
                                &mut slot.client,
                                db_guard,
                                registry,
                                server,
                            );
                        }
                    } else {
                        batch_db0_guard = None;
                        super::process_current_command(&mut slot.client, registry, server);
                    }

                    if slot.client.db_index != 0 || slot.client.blocked_on_keys {
                        batch_db0_guard = None;
                    }

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

        if consumed_total > 0 {
            if consumed_total >= slot.client.query_buf.len() {
                slot.client.query_buf.clear();
            } else {
                slot.client.query_buf.drain(..consumed_total);
            }
        }
        drop(batch_db0_guard);

        let reply = slot.client.drain_reply();
        if !reply.is_empty() {
            slot.queue_write_owned(reply);
        }

        if saw_command {
            super::update_client_info_snapshot(&slot.client, &last_cmd_name);
        }

        saw_command || consumed_total > 0
    }

    fn flush_pending_writes(&mut self) -> bool {
        let mut progressed = false;
        for slot in self.slots.iter_mut().flatten() {
            if slot.write_buffer.is_empty() {
                continue;
            }
            let (stream, buffer) = match (slot.stream.as_mut(), &mut slot.write_buffer) {
                (Some(stream), buffer) => (stream, buffer),
                (None, _) => {
                    slot.mark_closed();
                    continue;
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
        }
        progressed
    }

    fn cleanup_closed_clients(&mut self, registry: &Arc<Mutex<PubSubRegistry>>) -> bool {
        let mut to_remove = Vec::new();
        for slot in self.slots.iter().flatten() {
            if slot.closed || (slot.close_after_flush && slot.write_buffer.is_empty()) {
                to_remove.push(slot.id());
            }
        }

        let progressed = !to_remove.is_empty();
        for slot_id in to_remove {
            if let Some(slot) = self.remove_client(slot_id) {
                cleanup_slot(slot, registry);
            }
        }
        progressed
    }

    fn close_all_clients(&mut self, registry: &Arc<Mutex<PubSubRegistry>>) {
        let ids: Vec<SlotId> = self.slots.iter().flatten().map(ClientSlot::id).collect();
        for slot_id in ids {
            if let Some(slot) = self.remove_client(slot_id) {
                cleanup_slot(slot, registry);
            }
        }
    }
}

fn cleanup_slot(mut slot: ClientSlot, registry: &Arc<Mutex<PubSubRegistry>>) {
    let id = slot.client.id;
    let _ = redis_commands::pubsub::drop_client_from_registry(registry, id);
    redis_core::replication::global_replication_state().remove_replica(id);
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
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         Std nonblocking plain-TCP owner loop. PollDriverHandle is
//                  still abstract; no concrete poller dependency, command
//                  fast path, or owner-owned live DB migration is introduced.
// --------------------------------------------------------------------------
