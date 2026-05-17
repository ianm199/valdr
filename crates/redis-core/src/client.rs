//! `Client` — per-connection state.
//!
//! Minimal scaffolding for the pilot. Holds parsed-command args and
//! pending reply bytes. No event-loop integration or connection
//! abstraction yet — those land in Phase 2-3 with the architect deciding
//! sync/async strategy after we measure.

use redis_protocol::RespFrame;
use redis_types::RedisString;
use std::collections::{HashMap, HashSet};

use crate::object::RedisObject;

pub type ClientId = u64;

/// Placeholder for a reference into the server command table.
///
/// STUB — full command dispatch lives in Phase 3. Until then this is a function
/// pointer alias matching the salvaged multi.rs `CommandFn` shape.
pub type CommandFn = fn(&mut crate::command_context::CommandContext)
    -> Result<(), redis_types::RedisError>;

/// A single command queued inside a MULTI block.
///
/// PORT NOTE: migrated from `redis-commands::multi` per the architect TODO in
/// `multi.rs` ("MultiState and MultiCmd belong in redis-core/src/client.rs").
/// Concrete shape preserved from the salvaged Phase A definition.
pub struct MultiCmd {
    /// Positional arguments (argv[0] is the command name object).
    pub argv: Vec<RedisObject>,
    /// Length in bytes of all argument strings combined.
    pub argv_len: i32,
    /// Number of arguments.
    pub argc: i32,
    /// Handler for this command (placeholder type).
    pub cmd: Option<CommandFn>,
    /// Cluster slot (−1 when clustering is disabled).
    pub slot: i32,
}

/// A single watched-key record, owned by `MultiState::watched_keys`.
///
/// PORT NOTE: migrated from `redis-commands::multi`.
pub struct WatchedKey {
    /// The watched key object.
    pub key: RedisObject,
    /// Which database this watch is on.
    pub db_id: i32,
    /// True if the key was already expired when `watchForKey` was called.
    pub expired: bool,
}

/// Per-client MULTI/EXEC transaction state.
///
/// PORT NOTE: migrated from `redis-commands::multi` per the architect TODO.
pub struct MultiState {
    /// Queued commands.
    pub commands: Vec<MultiCmd>,
    /// OR of all queued command flags.
    pub cmd_flags: u64,
    /// OR of `~flags` for each queued command.
    pub cmd_inv_flags: u64,
    /// Total argv byte-size across all queued commands.
    pub argv_len_sums: usize,
    /// Allocated capacity (mirrors C `alloc_count`).
    pub alloc_count: i32,
    /// Keys being watched for CAS semantics (client-side list).
    pub watched_keys: Vec<WatchedKey>,
    /// Per-db O(1) membership check: `db_id → set of watched key bytes`.
    pub watched_keys_by_db: HashMap<i32, HashSet<RedisString>>,
    /// The db id selected (via SELECT) inside this transaction.
    pub transaction_db_id: i32,
}

impl MultiState {
    /// Create a fresh `MultiState` for `db_id`.
    pub fn new(db_id: i32) -> Self {
        MultiState {
            commands: Vec::new(),
            cmd_flags: 0,
            cmd_inv_flags: 0,
            argv_len_sums: 0,
            alloc_count: 0,
            watched_keys: Vec::new(),
            watched_keys_by_db: HashMap::new(),
            transaction_db_id: db_id,
        }
    }
}

pub struct Client {
    /// Server-assigned client identifier (CLIENT ID).
    pub id: ClientId,
    /// Parsed args of the current command (cleared per command).
    pub argv: Vec<RedisString>,
    /// Pending reply bytes, drained by the I/O layer.
    pub reply_buf: Vec<u8>,
    /// Selected database index (Phase 3 with RedisDb).
    pub db_index: u32,
    /// MULTI/EXEC transaction state (lazily initialised; `None` when the client
    /// is not in a transaction).
    pub mstate: Option<Box<MultiState>>,
    /// Cluster slot for the current command (`-1` when clustering disabled).
    ///
    /// STUB — Phase B placeholder.
    pub slot: i32,
    /// Bitfield of per-client flags.
    ///
    /// STUB — Phase B placeholder. C: `clientFlags flag` bitfield.
    pub flags: ClientFlags,
}

/// Per-client transient flags.
///
/// STUB — Phase B placeholder mirroring a small subset of C's `clientFlags`
/// bitfield. Each named bit gets its own bool here for clarity.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClientFlags {
    pub multi: bool,
    pub dirty_cas: bool,
    pub dirty_exec: bool,
    pub deny_blocking: bool,
    pub blocked: bool,
    pub aof_client: bool,
}

impl Client {
    pub fn new(id: ClientId) -> Self {
        Self {
            id,
            argv: Vec::new(),
            reply_buf: Vec::new(),
            db_index: 0,
            mstate: None,
            slot: -1,
            flags: ClientFlags::default(),
        }
    }

    pub fn arg(&self, i: usize) -> Option<&RedisString> {
        self.argv.get(i)
    }

    pub fn arg_count(&self) -> usize {
        self.argv.len()
    }

    pub fn reset_args(&mut self) {
        self.argv.clear();
    }

    pub fn set_args(&mut self, args: Vec<RedisString>) {
        self.argv = args;
    }

    /// Append an encoded RESP frame to the pending-reply buffer.
    pub fn write_frame(&mut self, frame: &RespFrame) {
        redis_protocol::encode_resp2(frame, &mut self.reply_buf);
    }

    /// Drain the reply buffer; caller (I/O layer) writes to the socket.
    pub fn drain_reply(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.reply_buf)
    }

    /// `process_input` parses raw bytes from the socket into commands.
    /// Translation packet for `networking.c::processInputBuffer` fills this.
    pub fn process_input(&mut self, _bytes: &[u8]) -> redis_types::RedisResult<()> {
        // TODO(port): port networking.c::processInputBuffer here.
        todo!("port networking.c::processInputBuffer in Phase 2")
    }

    /// Whether the client is currently blocked (BLPOP, WAIT, etc).
    ///
    /// STUB — Phase B placeholder; real blocking state lives in a future
    /// `bstate` field tracking `flag.blocked` plus the per-blocktype payload.
    pub fn is_blocked(&self) -> bool {
        false
    }

    /// Whether the client is in pub/sub mode (SUBSCRIBE / PSUBSCRIBE).
    ///
    /// STUB — Phase B placeholder; full pub/sub state lands with notify.c.
    pub fn is_pubsub(&self) -> bool {
        false
    }

    /// Whether the client is a replica (slave) connection.
    ///
    /// STUB — Phase B placeholder; replication state is Phase 6+.
    pub fn is_replica(&self) -> bool {
        false
    }

    /// Whether the client carries the `must-obey` flag (used by AOF/RDB
    /// loaders and the master-link).
    ///
    /// STUB — Phase B placeholder.
    pub fn must_obey(&self) -> bool {
        false
    }

    /// Blocking deadline in milliseconds (0 = block forever).
    ///
    /// STUB — Phase B placeholder; real value lives in the future `bstate`.
    pub fn blocking_timeout(&self) -> i64 {
        0
    }

    /// Whether this client is currently registered in the
    /// `clients_timeout_table` radix tree.
    ///
    /// STUB — Phase B placeholder; backing flag lands when bstate is added.
    pub fn in_timeout_table(&self) -> bool {
        false
    }

    /// Set/clear the in-timeout-table flag.
    ///
    /// STUB — Phase B placeholder; no backing storage yet.
    pub fn set_in_timeout_table(&mut self, _value: bool) {
        // TODO(port): persist on Client when bstate field is added.
    }

    /// Unix-time seconds of the last client interaction (read or write).
    ///
    /// STUB — Phase B placeholder; updated by the event loop in Phase 3.
    pub fn last_interaction(&self) -> i64 {
        0
    }

    /// Client id accessor (mirrors the public `id` field; provided so call
    /// sites can use `client.id()` interchangeably with `client.id`).
    pub fn id(&self) -> ClientId {
        self.id
    }

    /// Database id (currently the same as `db_index` cast to `i32`).
    ///
    /// STUB — Phase B placeholder; real `RedisDb` reference comes from
    /// `RedisServer` lookup by `db_index` in Phase 3.
    pub fn db_id(&self) -> i32 {
        self.db_index as i32
    }

    /// Cluster slot of the current command.
    pub fn slot(&self) -> i32 {
        self.slot
    }

    /// Number of arguments in `argv` (alias of `arg_count`).
    pub fn argc(&self) -> i32 {
        self.argv.len() as i32
    }

    /// Total byte-length of all `argv` entries.
    ///
    /// STUB — Phase B placeholder; real C value is maintained as `c->argv_len`.
    pub fn argv_len(&self) -> i32 {
        self.argv.iter().map(|s| s.as_bytes().len() as i32).sum()
    }

    /// Move out the current argv and reset to empty.
    ///
    /// PORT NOTE: returns `Vec<RedisObject>` (not `Vec<RedisString>`) because
    /// translated MULTI code stores queued args as `RedisObject::String`.
    pub fn take_argv(&mut self) -> Vec<RedisObject> {
        std::mem::take(&mut self.argv)
            .into_iter()
            .map(RedisObject::String)
            .collect()
    }

    /// Current command function pointer.
    ///
    /// STUB — Phase B placeholder returning `None` until command dispatch lands.
    pub fn current_cmd_fn(&self) -> Option<CommandFn> {
        None
    }

    pub fn flag_multi(&self) -> bool { self.flags.multi }
    pub fn flag_dirty_cas(&self) -> bool { self.flags.dirty_cas }
    pub fn flag_dirty_exec(&self) -> bool { self.flags.dirty_exec }
    pub fn flag_deny_blocking(&self) -> bool { self.flags.deny_blocking }
    pub fn flag_blocked(&self) -> bool { self.flags.blocked }
    pub fn is_aof_client(&self) -> bool { self.flags.aof_client }

    pub fn set_flag_multi(&mut self, v: bool) { self.flags.multi = v; }
    pub fn set_flag_dirty_cas(&mut self, v: bool) { self.flags.dirty_cas = v; }
    pub fn set_flag_dirty_exec(&mut self, v: bool) { self.flags.dirty_exec = v; }
    pub fn set_flag_deny_blocking(&mut self, v: bool) { self.flags.deny_blocking = v; }

    /// Install commands[index].argv/argc/argv_len/cmd as the client's current
    /// command. STUB — Phase B placeholder.
    pub fn set_current_queued_command(&mut self, _index: usize) {
        // TODO(port): wire when MULTI execution lands.
    }

    /// Save current argv/cmd back into commands[index]. STUB — Phase B
    /// placeholder.
    pub fn save_queued_command_state(&mut self, _index: usize) {
        // TODO(port): wire when MULTI execution lands.
    }

    /// Release the saved original argv. STUB — Phase B placeholder.
    pub fn free_original_argv(&mut self) {
        // TODO(port): wire when MULTI execution lands.
    }

    /// Restore the original argv saved before MULTI execution. STUB — Phase B
    /// placeholder.
    pub fn restore_orig_argv(&mut self) {
        // TODO(port): wire when MULTI execution lands.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_frame_appends_to_reply_buf() {
        let mut c = Client::new(1);
        c.write_frame(&RespFrame::simple(b"OK".as_slice()));
        c.write_frame(&RespFrame::integer(42));
        let bytes = c.drain_reply();
        assert_eq!(bytes, b"+OK\r\n:42\r\n");
        assert!(c.drain_reply().is_empty());
    }

    #[test]
    fn args_access() {
        let mut c = Client::new(2);
        c.set_args(vec![
            RedisString::from_bytes(b"SET"),
            RedisString::from_bytes(b"foo"),
            RedisString::from_bytes(b"bar"),
        ]);
        assert_eq!(c.arg_count(), 3);
        assert_eq!(c.arg(0).unwrap().as_bytes(), b"SET");
        assert_eq!(c.arg(99), None);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (PORTING.md §2 #5 + types.tsv:client mapping)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         1
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Minimal Client; process_input is todo!() until networking.c is ported.
// ──────────────────────────────────────────────────────────────────────────
