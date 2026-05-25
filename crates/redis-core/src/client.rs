//! `Client` — per-connection state.
//!
//! Minimal scaffolding for the pilot. Holds parsed-command args and
//! pending reply bytes. No event-loop integration or connection
//! abstraction yet — those land in Phase 2-3 with the architect deciding
//! sync/async strategy after we measure.

use redis_protocol::RespFrame;
use redis_types::RedisString;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::acl::global_acl_state;
use crate::object::RedisObject;
use crate::transport::Connection;

pub type ClientId = u64;

static DEBUG_CLIENT_ENFORCE_REPLY_LIST: AtomicBool = AtomicBool::new(false);

pub fn set_debug_client_enforce_reply_list(enabled: bool) {
    DEBUG_CLIENT_ENFORCE_REPLY_LIST.store(enabled, Ordering::Relaxed);
}

pub fn debug_client_enforce_reply_list() -> bool {
    DEBUG_CLIENT_ENFORCE_REPLY_LIST.load(Ordering::Relaxed)
}

/// Placeholder for a reference into the server command table.
///
/// STUB — full command dispatch lives in Phase 3. Until then this is a function
/// pointer alias matching the salvaged multi.rs `CommandFn` shape.
pub type CommandFn =
    fn(&mut crate::command_context::CommandContext) -> Result<(), redis_types::RedisError>;

fn append_decimal_u64(buf: &mut Vec<u8>, mut n: u64) {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    loop {
        i -= 1;
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    buf.extend_from_slice(&tmp[i..]);
}

fn append_decimal_i64(buf: &mut Vec<u8>, n: i64) {
    if n < 0 {
        buf.push(b'-');
    }
    append_decimal_u64(buf, n.unsigned_abs());
}

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
    /// Ranges in `reply_buf` that are Pub/Sub push replies from the current
    /// command and must bypass CLIENT REPLY OFF/SKIP.
    push_reply_segments: Vec<(usize, usize)>,
    /// Tracking push frames deferred while an inner execution context is
    /// building a transaction or script reply.
    pending_tracking_pushes: Vec<RespFrame>,
    /// Selected database index (Phase 3 with RedisDb).
    pub db_index: u32,
    /// MULTI/EXEC transaction state (lazily initialised; `None` when the client
    /// is not in a transaction).
    pub mstate: Option<Box<MultiState>>,
    /// Raw `argv` of each command queued inside a MULTI block.
    ///
    /// PORT NOTE: complements `mstate.commands` (which uses `RedisObject` /
    /// command-function-pointer shape from the salvaged C port). The Round 8b
    /// dispatch-level integration re-routes queued bytes through the same
    /// dispatcher used for non-MULTI commands, so it operates on raw
    /// `RedisString` argv vectors here.
    pub queued_argvs: Vec<Vec<RedisString>>,
    /// Cluster slot for the current command (`-1` when clustering disabled).
    ///
    /// STUB — Phase B placeholder.
    pub slot: i32,
    /// Bitfield of per-client flags.
    ///
    /// STUB — Phase B placeholder. C: `clientFlags flag` bitfield.
    pub flags: ClientFlags,
    /// Live transport for this client.
    ///
    /// `None` for pre-handshake clients, AOF/RDB pseudo-clients, and unit
    /// tests; `Some` for real network clients accepted by the event loop.
    pub conn: Option<Connection>,
    /// Partial read buffer; bytes accumulated by the I/O layer between command
    /// boundaries.
    ///
    /// STUB — Phase B placeholder. The Wave A event loop owns this directly;
    /// later phases will move it onto Client for compatibility with the C
    /// `c->querybuf` field.
    pub query_buf: Vec<u8>,
    /// Optional client name set via `CLIENT SETNAME`.
    ///
    /// `None` until the client invokes `CLIENT SETNAME`; cleared by `RESET`.
    /// Real Redis stores this as a byte string; arbitrary bytes are allowed
    /// except whitespace and special characters (validated at the setter).
    pub name: Option<RedisString>,
    /// Connection-tear-down request flag (set by `QUIT`).
    ///
    /// The accept loop checks this after each dispatched command, flushes the
    /// pending reply, and closes the socket when `true`.
    pub should_close: bool,
    /// Peer address recorded at accept time (e.g. `"127.0.0.1:54231"`).
    ///
    /// Used by `CLIENT LIST` to fill the `addr=` field. `None` for clients
    /// that have no live transport (unit tests, pseudo-clients).
    pub addr: Option<String>,
    /// RESP protocol version negotiated by `HELLO` (2 or 3).
    ///
    /// Defaults to 2 (the version implied by every legacy RESP2 client).
    /// RESP3 upgrade path is a TODO.
    pub resp_proto: i32,
    /// CLIENT TRACKING visible state.
    ///
    /// This intentionally records only the per-client flags needed by CLIENT
    /// subcommands and diagnostics. The global invalidation table remains a
    /// separate architectural packet.
    pub tracking: ClientTrackingState,
    /// True after `CLIENT IMPORT-SOURCE ON`.
    pub import_source: bool,
    /// Set by a command handler when the current command had no effect on the
    /// dataset and must therefore not be propagated to replicas or the AOF.
    ///
    /// Mirrors C's `CLIENT_PREVENT_PROP`. Reset at the top of `dispatch` for
    /// every command. A no-op write (e.g. `EXPIRE` on a missing key, `SET NX`
    /// that did not store) sets this so it does not leak into the replication
    /// stream, which `assert_replication_stream` asserts on exactly.
    pub prevent_propagation: bool,
    /// The ACL username this client is authenticated as.
    ///
    /// `None` means the client has not yet authenticated (pre-AUTH state).
    /// `Some(name)` means the client has successfully authenticated as that
    /// user. The default user (`default on nopass`) grants immediate access on
    /// connect without requiring AUTH; the accept loop sets this to
    /// `Some("default")` when the default user is enabled and has `nopass`.
    /// Authentication state persists across RESET (real Redis behaviour).
    pub authenticated_user: Option<RedisString>,
    /// True after this connection has authenticated at least once.
    ///
    /// C Redis marks once-authenticated clients exempt from the tiny pre-AUTH
    /// output-buffer cap even after RESET drops them back into NOAUTH state.
    pub ever_authenticated: bool,
    /// Channels this client is subscribed to.
    ///
    /// Round 8a per-client pub/sub bookkeeping; mirrors the channel half of
    /// `PubSubRegistry` so the read loop can tell when the client is in
    /// subscribe mode without consulting the global lock.
    pub subscribed_channels: HashSet<RedisString>,
    /// Glob patterns this client is subscribed to.
    pub subscribed_patterns: HashSet<RedisString>,
    /// Shard channels this client is subscribed to.
    pub subscribed_shard_channels: HashSet<RedisString>,
    /// True while the client is parked inside the global `BlockedKeysIndex`
    /// from a BLPOP / BRPOP / BLMOVE / BRPOPLPUSH / BLMPOP call.
    ///
    /// Set by the blocking command handler immediately before it returns
    /// without writing a reply; cleared by the wake hook in the LIST push
    /// path or the per-server timeout thread when those deliver the reply
    /// via the client's outbound mpsc.
    pub blocked_on_keys: bool,
    /// Keys that need blocked-waiter wakes deferred until after EXEC drains.
    ///
    /// Populated by list push/move commands when `flag_deny_blocking` is set
    /// (i.e. the command is running inside an EXEC drain). After the drain
    /// completes and `flag_deny_blocking` is cleared, `exec_command` takes
    /// this vec and fires the real `wake_blocked_for_key` for each entry in
    /// insertion order.
    pub pending_wakes: Vec<RedisString>,
    /// True once this client has completed the PSYNC handshake on the master
    /// side and is treated as a replica.
    ///
    /// When set, the dispatch path stops handing the client's argv to
    /// command handlers (replicas do not issue commands to the master); the
    /// reader thread keeps draining for REPLCONF ACK frames, which Wave B
    /// will parse. The flag is cleared on disconnect via the standard
    /// cleanup path.
    pub is_replica: bool,
    /// CLIENT CAPA REDIRECT visible capability bit.
    pub capa_redirect: bool,
    /// Optional client library name set via `CLIENT SETINFO lib-name`.
    pub lib_name: Option<RedisString>,
    /// Optional client library version set via `CLIENT SETINFO lib-ver`.
    pub lib_ver: Option<RedisString>,
    /// Bytes read from this client connection.
    pub net_input_bytes: u64,
    /// Bytes written to this client connection.
    pub net_output_bytes: u64,
    /// Commands completed by this client. The currently executing command is
    /// not counted until after its handler returns, matching CLIENT INFO.
    pub commands_processed: u64,
    /// Temporarily suppress MONITOR feed for nested execution contexts such as
    /// EXEC draining queued commands that were already logged at queue time.
    pub suppress_monitor: bool,
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
    pub lua: bool,
    pub blocked: bool,
    pub monitor: bool,
    pub readonly: bool,
    pub no_evict: bool,
    pub no_touch: bool,
    pub aof_client: bool,
    pub reply_off: bool,
    pub reply_skip_next: bool,
    pub reply_skip: bool,
}

#[derive(Debug, Default, Clone)]
pub struct ClientTrackingState {
    pub enabled: bool,
    pub bcast: bool,
    pub optin: bool,
    pub optout: bool,
    pub caching: bool,
    pub noloop: bool,
    pub broken_redirect: bool,
    pub redirect: i64,
    pub prefixes: Vec<RedisString>,
}

/// Determine the initial `authenticated_user` for a new `Client`.
///
/// Consults the global ACL state: if the `default` user is enabled and has
/// `nopass`, the client starts authenticated as `default` (backwards compat).
/// Otherwise returns `None` — the client must run AUTH before other commands.
fn initial_authenticated_user() -> Option<RedisString> {
    let default_key = RedisString::from_bytes(b"default");
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if let Some(user) = guard.users.get(&default_key) {
        if user.flags.enabled && user.flags.nopass {
            return Some(default_key);
        }
    }
    None
}

impl Client {
    pub fn new(id: ClientId) -> Self {
        let authenticated_user = initial_authenticated_user();
        let ever_authenticated = authenticated_user.is_some();
        Self {
            id,
            argv: Vec::new(),
            reply_buf: Vec::new(),
            push_reply_segments: Vec::new(),
            pending_tracking_pushes: Vec::new(),
            db_index: 0,
            mstate: None,
            queued_argvs: Vec::new(),
            slot: -1,
            flags: ClientFlags::default(),
            conn: None,
            query_buf: Vec::new(),
            name: None,
            should_close: false,
            addr: None,
            resp_proto: 2,
            tracking: ClientTrackingState::default(),
            import_source: false,
            prevent_propagation: false,
            subscribed_channels: HashSet::new(),
            subscribed_patterns: HashSet::new(),
            subscribed_shard_channels: HashSet::new(),
            blocked_on_keys: false,
            pending_wakes: Vec::new(),
            authenticated_user,
            ever_authenticated,
            is_replica: false,
            capa_redirect: false,
            lib_name: None,
            lib_ver: None,
            net_input_bytes: 0,
            net_output_bytes: 0,
            commands_processed: 0,
            suppress_monitor: false,
        }
    }

    /// Construct a `Client` bound to a live transport.
    ///
    /// The id is left as `0`; callers should call `RedisServer::alloc_client_id`
    /// and assign `client.id` if they need a unique identifier.
    pub fn with_connection(conn: Connection) -> Self {
        let mut c = Self::new(0);
        c.conn = Some(conn);
        c
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

    /// Mark the current command as a no-op so it is not propagated to replicas
    /// or the AOF. See `prevent_propagation`.
    pub fn set_prevent_propagation(&mut self) {
        self.prevent_propagation = true;
    }

    pub fn prevent_propagation(&self) -> bool {
        self.prevent_propagation
    }

    pub fn set_authenticated_user(&mut self, user: Option<RedisString>) {
        if user.is_some() {
            self.ever_authenticated = true;
        }
        self.authenticated_user = user;
    }

    /// Reset transient connection state, mirroring real Redis `RESET`.
    ///
    /// Clears the client name, MULTI transaction state, queued reply bytes,
    /// the selected database (back to 0), and per-client flags. The client
    /// id and live transport are preserved — the connection remains open.
    pub fn reset_state(&mut self) {
        self.name = None;
        self.mstate = None;
        self.queued_argvs.clear();
        self.reply_buf.clear();
        self.push_reply_segments.clear();
        self.pending_tracking_pushes.clear();
        self.db_index = 0;
        self.flags = ClientFlags::default();
        self.resp_proto = 2;
        self.tracking = ClientTrackingState::default();
        crate::tracking::remove_runtime_client_tracking(self.id);
        self.import_source = false;
        self.authenticated_user = initial_authenticated_user();
        self.capa_redirect = false;
        self.subscribed_channels.clear();
        self.subscribed_patterns.clear();
        self.subscribed_shard_channels.clear();
        self.pending_wakes.clear();
        crate::db::watched_keys_index_remove_client(self.id);
        let _ = crate::db::watched_keys_take_dirty(self.id);
        self.clear_blocked_on_keys();
        self.suppress_monitor = false;
    }

    /// Drop the client from the global blocked-keys index, if registered.
    ///
    /// Called from `RESET`, from the per-connection cleanup path when a
    /// socket closes, and after a successful BLPOP wake/timeout reply has
    /// been delivered through the outbound mpsc.
    pub fn clear_blocked_on_keys(&mut self) {
        if self.blocked_on_keys {
            self.blocked_on_keys = false;
            if let Ok(mut idx) = crate::blocked_keys::blocked_keys_index().lock() {
                let _ = idx.remove_client(self.id);
            }
        }
    }

    /// Total per-client pub/sub subscriptions across channels and patterns.
    pub fn pubsub_subscription_count(&self) -> usize {
        self.subscribed_channels.len()
            + self.subscribed_patterns.len()
            + self.subscribed_shard_channels.len()
    }

    /// Whether this client is currently in pub/sub subscribe mode.
    pub fn in_pubsub_mode(&self) -> bool {
        self.pubsub_subscription_count() > 0
    }

    /// Append an encoded RESP frame to the pending-reply buffer.
    ///
    /// Encoding follows the client's negotiated `resp_proto`: RESP3 emits
    /// the dedicated native frame shapes (`%`, `~`, `,`, `_`, `#`, `>`, …)
    /// while RESP2 degrades the RESP3-only variants to their nearest RESP2
    /// equivalent. The RESP2 wire bytes for the legacy frame variants are
    /// identical regardless of `resp_proto`.
    pub fn write_frame(&mut self, frame: &RespFrame) {
        redis_protocol::encode_for_proto(frame, self.resp_proto, &mut self.reply_buf);
    }

    /// Append a Pub/Sub-style push frame.
    ///
    /// Valkey marks these writes with CLIENT_PUSHING, so reply silencing
    /// suppresses ordinary command replies without hiding notifications.
    pub fn write_push_frame(&mut self, frame: &RespFrame) {
        let start = self.reply_buf.len();
        redis_protocol::encode_for_proto(frame, self.resp_proto, &mut self.reply_buf);
        let end = self.reply_buf.len();
        if end > start {
            self.push_reply_segments.push((start, end));
        }
    }

    /// Queue a tracking push until the surrounding nested execution context is
    /// done writing its command reply.
    pub fn defer_tracking_push_frame(&mut self, frame: RespFrame) {
        self.pending_tracking_pushes.push(frame);
    }

    /// Append deferred tracking pushes after the surrounding command reply.
    pub fn flush_deferred_tracking_pushes(&mut self) {
        let pending = std::mem::take(&mut self.pending_tracking_pushes);
        for frame in pending {
            self.write_push_frame(&frame);
        }
    }

    /// Finish one command's reply lifecycle, applying CLIENT REPLY OFF/SKIP.
    pub fn finish_command_reply(&mut self, reply_start: usize) {
        if self.flags.reply_off || self.flags.reply_skip {
            self.suppress_non_push_reply_since(reply_start);
        } else {
            self.push_reply_segments.clear();
        }

        self.flags.reply_skip = false;
        if self.flags.reply_skip_next {
            self.flags.reply_skip = true;
            self.flags.reply_skip_next = false;
        }
    }

    fn suppress_non_push_reply_since(&mut self, reply_start: usize) {
        if reply_start >= self.reply_buf.len() {
            self.push_reply_segments.clear();
            return;
        }

        let segments: Vec<(usize, usize)> = self
            .push_reply_segments
            .iter()
            .copied()
            .filter(|(start, end)| *start >= reply_start && *end <= self.reply_buf.len())
            .collect();

        if segments.is_empty() {
            self.reply_buf.truncate(reply_start);
            self.push_reply_segments.clear();
            return;
        }

        let mut kept = Vec::with_capacity(
            reply_start
                + segments
                    .iter()
                    .map(|(start, end)| end.saturating_sub(*start))
                    .sum::<usize>(),
        );
        kept.extend_from_slice(&self.reply_buf[..reply_start]);
        for (start, end) in segments {
            kept.extend_from_slice(&self.reply_buf[start..end]);
        }
        self.reply_buf = kept;
        self.push_reply_segments.clear();
    }

    fn append_prefixed_line(&mut self, prefix: u8, bytes: &[u8]) {
        self.reply_buf.reserve(bytes.len() + 3);
        self.reply_buf.push(prefix);
        self.reply_buf.extend_from_slice(bytes);
        self.reply_buf.extend_from_slice(b"\r\n");
    }

    fn append_len_header_usize(&mut self, prefix: u8, len: usize) {
        self.reply_buf.reserve(1 + 20 + 2);
        self.reply_buf.push(prefix);
        append_decimal_u64(&mut self.reply_buf, len as u64);
        self.reply_buf.extend_from_slice(b"\r\n");
    }

    fn append_len_header_i64(&mut self, prefix: u8, len: i64) {
        self.reply_buf.reserve(1 + 20 + 2);
        self.reply_buf.push(prefix);
        append_decimal_i64(&mut self.reply_buf, len);
        self.reply_buf.extend_from_slice(b"\r\n");
    }

    /// Append a RESP simple-string reply without constructing a `RespFrame`.
    pub fn write_simple_string(&mut self, bytes: &[u8]) {
        self.append_prefixed_line(b'+', bytes);
    }

    /// Append a RESP error reply without constructing a `RespFrame`.
    pub fn write_error(&mut self, bytes: &[u8]) {
        self.append_prefixed_line(b'-', bytes);
    }

    /// Append a RESP integer reply without constructing a `RespFrame`.
    pub fn write_integer(&mut self, n: i64) {
        self.append_len_header_i64(b':', n);
    }

    /// Append a RESP bulk-string reply without constructing a `RespFrame`.
    pub fn write_bulk(&mut self, bytes: &[u8]) {
        self.append_len_header_usize(b'$', bytes.len());
        self.reply_buf.extend_from_slice(bytes);
        self.reply_buf.extend_from_slice(b"\r\n");
    }

    /// Append a RESP bulk-string reply from a `RedisString` without cloning it.
    pub fn write_bulk_string(&mut self, s: &RedisString) {
        self.write_bulk(s.as_bytes());
    }

    /// Append the protocol-appropriate null bulk reply.
    pub fn write_null_bulk(&mut self) {
        if self.resp_proto == 3 {
            self.reply_buf.extend_from_slice(b"_\r\n");
        } else {
            self.reply_buf.extend_from_slice(b"$-1\r\n");
        }
    }

    /// Append the protocol-appropriate null array reply.
    pub fn write_null_array(&mut self) {
        if self.resp_proto == 3 {
            self.reply_buf.extend_from_slice(b"_\r\n");
        } else {
            self.reply_buf.extend_from_slice(b"*-1\r\n");
        }
    }

    /// Append a RESP array header.
    pub fn write_array_header(&mut self, len: i64) {
        self.append_len_header_i64(b'*', len);
    }

    /// Append a RESP push header for RESP3 clients, or an array header for RESP2.
    pub fn write_push_or_array_header(&mut self, len: i64) {
        let prefix = if self.resp_proto == 3 { b'>' } else { b'*' };
        self.append_len_header_i64(prefix, len);
    }

    /// Append a RESP map header for RESP3 clients, or a flat array header for RESP2.
    pub fn write_map_header(&mut self, n_pairs: i64) {
        if self.resp_proto == 3 {
            self.append_len_header_i64(b'%', n_pairs);
        } else {
            self.append_len_header_i64(b'*', n_pairs.saturating_mul(2));
        }
    }

    /// Append a RESP set header for RESP3 clients, or an array header for RESP2.
    pub fn write_set_header(&mut self, len: i64) {
        let prefix = if self.resp_proto == 3 { b'~' } else { b'*' };
        self.append_len_header_i64(prefix, len);
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
    pub fn is_pubsub(&self) -> bool {
        self.in_pubsub_mode()
    }

    /// Whether the client is a replica (slave) connection.
    ///
    /// Set to true once the client completes the PSYNC handshake on the
    /// master side (Session 3A). The dispatch path checks this flag and
    /// rejects normal command bytes — replicas are write-only targets.
    pub fn is_replica(&self) -> bool {
        self.is_replica
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
    /// translated MULTI code stores queued args as string-encoded objects.
    pub fn take_argv(&mut self) -> Vec<RedisObject> {
        std::mem::take(&mut self.argv)
            .into_iter()
            .map(RedisObject::from_string)
            .collect()
    }

    /// Current command function pointer.
    ///
    /// STUB — Phase B placeholder returning `None` until command dispatch lands.
    pub fn current_cmd_fn(&self) -> Option<CommandFn> {
        None
    }

    pub fn flag_multi(&self) -> bool {
        self.flags.multi
    }
    pub fn flag_dirty_cas(&self) -> bool {
        self.flags.dirty_cas
    }
    pub fn flag_dirty_exec(&self) -> bool {
        self.flags.dirty_exec
    }
    pub fn flag_deny_blocking(&self) -> bool {
        self.flags.deny_blocking
    }
    pub fn flag_lua(&self) -> bool {
        self.flags.lua
    }
    pub fn flag_blocked(&self) -> bool {
        self.flags.blocked
    }
    pub fn is_aof_client(&self) -> bool {
        self.flags.aof_client
    }

    pub fn set_flag_multi(&mut self, v: bool) {
        self.flags.multi = v;
    }
    pub fn set_flag_dirty_cas(&mut self, v: bool) {
        self.flags.dirty_cas = v;
    }
    pub fn set_flag_dirty_exec(&mut self, v: bool) {
        self.flags.dirty_exec = v;
    }
    pub fn set_flag_deny_blocking(&mut self, v: bool) {
        self.flags.deny_blocking = v;
    }
    pub fn set_flag_lua(&mut self, v: bool) {
        self.flags.lua = v;
    }

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
    fn client_reply_off_keeps_push_frames() {
        let mut c = Client::new(1);
        c.flags.reply_off = true;
        c.write_simple_string(b"OK");
        c.write_push_frame(&RespFrame::array(vec![
            RespFrame::bulk(RedisString::from_static(b"message")),
            RespFrame::bulk(RedisString::from_static(b"chan")),
            RespFrame::bulk(RedisString::from_static(b"payload")),
        ]));
        c.finish_command_reply(0);
        assert_eq!(
            c.drain_reply(),
            b"*3\r\n$7\r\nmessage\r\n$4\r\nchan\r\n$7\r\npayload\r\n"
        );
    }

    #[test]
    fn client_reply_skip_suppresses_one_command() {
        let mut c = Client::new(1);
        c.flags.reply_skip_next = true;
        c.finish_command_reply(0);
        assert!(c.flags.reply_skip);

        let start = c.reply_buf.len();
        c.write_simple_string(b"OK");
        c.finish_command_reply(start);
        assert!(c.drain_reply().is_empty());
        assert!(!c.flags.reply_skip);

        let start = c.reply_buf.len();
        c.write_simple_string(b"OK");
        c.finish_command_reply(start);
        assert_eq!(c.drain_reply(), b"+OK\r\n");
    }

    #[test]
    fn direct_resp_helpers_match_hot_reply_shapes() {
        let mut c = Client::new(1);
        c.write_simple_string(b"OK");
        c.write_integer(42);
        c.write_bulk(b"value");
        c.write_null_bulk();
        c.write_array_header(2);
        c.write_bulk(b"a");
        c.write_bulk(b"b");
        assert_eq!(
            c.drain_reply(),
            b"+OK\r\n:42\r\n$5\r\nvalue\r\n$-1\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
        );
    }

    #[test]
    fn direct_resp_helpers_keep_resp3_null_and_header_shapes() {
        let mut c = Client::new(1);
        c.resp_proto = 3;
        c.write_null_bulk();
        c.write_null_array();
        c.write_push_or_array_header(1);
        c.write_map_header(2);
        c.write_set_header(3);
        assert_eq!(c.drain_reply(), b"_\r\n_\r\n>1\r\n%2\r\n~3\r\n");
    }

    #[test]
    fn direct_resp_integer_formatting_handles_edges() {
        let mut c = Client::new(1);
        c.write_integer(0);
        c.write_integer(-1);
        c.write_integer(i64::MIN);
        c.write_bulk(&[0; 10]);
        assert_eq!(
            c.drain_reply(),
            b":0\r\n:-1\r\n:-9223372036854775808\r\n$10\r\n\0\0\0\0\0\0\0\0\0\0\r\n"
        );
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
