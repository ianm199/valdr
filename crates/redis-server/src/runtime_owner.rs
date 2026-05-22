//! Inert RuntimeOwner scaffold.
//!
//! This module names the owner-loop vocabulary from
//! `harness/architecture/object-vocabulary.tsv` without wiring it into the
//! default server path. The current product path still lives in `main.rs`:
//! blocking accept, one thread per connection, `Arc<Mutex<RedisDb>>`, and
//! normal `redis_commands::dispatch`.

use std::collections::VecDeque;

use redis_core::{Client, RedisDb};
use redis_types::RedisString;

const DEFAULT_DATABASE_COUNT: u32 = 16;
const DEFAULT_EVENT_CAPACITY: usize = 1024;

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

/// Per-slot outbound bytes drained by the future owner write step.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ClientWriteBuffer {
    bytes: Vec<u8>,
}

impl ClientWriteBuffer {
    pub fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub fn append(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.bytes)
    }
}

/// Owner-loop representation of one connected client.
pub struct ClientSlot {
    id: SlotId,
    client: Client,
    query_buf: Vec<u8>,
    argv: Vec<RedisString>,
    write_buffer: ClientWriteBuffer,
    closed: bool,
}

impl ClientSlot {
    pub fn new(id: SlotId, client: Client) -> Self {
        Self {
            id,
            client,
            query_buf: Vec::new(),
            argv: Vec::new(),
            write_buffer: ClientWriteBuffer::new(),
            closed: false,
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
        self.query_buf.extend_from_slice(bytes);
    }

    pub fn query_buffer(&self) -> &[u8] {
        &self.query_buf
    }

    pub fn clear_query_buffer(&mut self) {
        self.query_buf.clear();
    }

    pub fn stage_argv(&mut self, argv: Vec<RedisString>) {
        self.argv = argv;
    }

    pub fn argv(&self) -> &[RedisString] {
        &self.argv
    }

    pub fn queue_write(&mut self, bytes: &[u8]) {
        self.write_buffer.append(bytes);
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

/// Planned owner of normal command execution.
///
/// This scaffold is deliberately inert. It can be constructed and unit-tested,
/// but `main.rs` does not send accepted sockets, parsed requests, or live
/// database locks through it.
pub struct RuntimeOwner {
    config: RuntimeOwnerConfig,
    poll_driver: PollDriverHandle,
    slots: Vec<Option<ClientSlot>>,
    free_slots: Vec<SlotId>,
    databases: Vec<RedisDb>,
    events: VecDeque<RuntimeEvent>,
}

impl RuntimeOwner {
    pub fn new(config: RuntimeOwnerConfig) -> Self {
        let mut databases = Vec::new();
        for id in 0..config.database_count() {
            databases.push(RedisDb::new(id));
        }
        Self {
            config,
            poll_driver: PollDriverHandle::abstract_placeholder(),
            slots: Vec::new(),
            free_slots: Vec::new(),
            databases,
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
        self.databases.len()
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
//   port_notes:    2
//   unsafe_blocks: 0
//   notes:         Inert scaffold only. PollDriverHandle is abstract; no
//                  concrete poller dependency, default product-path wiring,
//                  command fast path, or live DB migration is introduced here.
// --------------------------------------------------------------------------
