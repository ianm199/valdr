//! Networking — port.
//! This module implements:
//! * Client reply-buffer management — the `addReply*` family as free functions
//! operating on `Client` state.
//! * RESP protocol input parsing — inline (plain-text) and multibulk (RESP2/3).
//! * Output-buffer lifecycle — encoded buffers with copy-avoidance, deferred-len
//! placeholders, and the writev scatter path.
//! * CLIENT command subcommands (KILL, LIST, INFO, SETNAME, GETNAME, …).
//! * HELLO / QUIT / RESET protocol commands.
//! * Client pause and unpause mechanisms.
//! * Client eviction by output-buffer limits.
//! * I/O thread hooks (`io_thread_read_query_from_client`, `io_thread_write_to_client`).
//! # Canonical-type imports
//! Types in `harness/type-vocabulary.tsv` are imported, not redefined.

// TODO(architect): add dep edge redis-core → redis-protocol for RespFrame.
// TODO(architect): confirm Connection type for networking I/O abstraction.

use std::sync::atomic::{AtomicI32, AtomicU8, AtomicUsize, Ordering};
use std::time::Instant;

use redis_types::{RedisError, RedisResult, RedisString};

use crate::client::{Client, ClientId};
use crate::command_context::CommandContext;
use crate::server::RedisServer;

// ── Protocol / buffer constants ─────────────────────────────────────────────

/// Default reply-chunk allocation size (16 KiB).
pub const PROTO_REPLY_CHUNK_BYTES: usize = 16 * 1024;

/// Minimum reply size for deferred replies (256 B).
pub const PROTO_REPLY_MIN_BYTES: usize = 256;

/// I/O read-buffer size (16 KiB).
pub const PROTO_IOBUF_LEN: usize = 16 * 1024;

/// Maximum inline-request size (64 KiB).
pub const PROTO_INLINE_MAX_SIZE: usize = 64 * 1024;

/// Threshold above which bulk args are treated as "big" (32 KiB).
pub const PROTO_MBULK_BIG_ARG: i64 = 32 * 1024;

/// Shared-header length cut-off (use cached header strings below this).
pub const OBJ_SHARED_BULKHDR_LEN: i64 = 128;

/// Max bytes written per event-loop iteration (64 MiB).
pub const NET_MAX_WRITES_PER_EVENT: usize = 64 * 1024 * 1024;

/// Minimum command-queue capacity.
#[allow(dead_code)]
const COMMAND_QUEUE_MIN_CAPACITY: usize = 16;

/// Maximum replica reads per I/O event.
#[allow(dead_code)]
const REPL_MAX_READS_PER_IO_EVENT: usize = 25;

// ── Error flags (read_flags bitmask values) ──────────────────────────────────

pub const READ_FLAGS_REPLICATED: u32 = 1 << 0;
pub const READ_FLAGS_AUTH_REQUIRED: u32 = 1 << 1;
pub const READ_FLAGS_PARSING_COMPLETED: u32 = 1 << 2;
pub const READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN: u32 = 1 << 3;
pub const READ_FLAGS_INLINE_ZERO_QUERY_LEN: u32 = 1 << 4;
pub const READ_FLAGS_ERROR_BIG_INLINE_REQUEST: u32 = 1 << 5;
pub const READ_FLAGS_ERROR_BIG_MULTIBULK: u32 = 1 << 6;
pub const READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN: u32 = 1 << 7;
pub const READ_FLAGS_ERROR_UNAUTHENTICATED_MULTIBULK_LEN: u32 = 1 << 8;
pub const READ_FLAGS_ERROR_UNAUTHENTICATED_BULK_LEN: u32 = 1 << 9;
pub const READ_FLAGS_ERROR_BIG_BULK_COUNT: u32 = 1 << 10;
pub const READ_FLAGS_ERROR_MBULK_UNEXPECTED_CHARACTER: u32 = 1 << 11;
pub const READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN: u32 = 1 << 12;
pub const READ_FLAGS_ERROR_UNBALANCED_QUOTES: u32 = 1 << 13;
pub const READ_FLAGS_ERROR_INVALID_CRLF: u32 = 1 << 14;
pub const READ_FLAGS_ERROR_UNEXPECTED_INLINE_FROM_REPLICATED_CLIENT: u32 = 1 << 15;
pub const READ_FLAGS_QB_LIMIT_REACHED: u32 = 1 << 16;
pub const READ_FLAGS_DONT_PARSE: u32 = 1 << 17;
pub const READ_FLAGS_PREFETCHED: u32 = 1 << 18;
pub const READ_FLAGS_BAD_ARITY: u32 = 1 << 19;
pub const READ_FLAGS_COMMAND_NOT_FOUND: u32 = 1 << 20;

/// Mask of all error-flag bits (excluding QB limit — handled separately).
const READ_FLAGS_ALL_PARSE_ERRORS: u32 = READ_FLAGS_ERROR_BIG_INLINE_REQUEST
    | READ_FLAGS_ERROR_BIG_MULTIBULK
    | READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN
    | READ_FLAGS_ERROR_UNAUTHENTICATED_MULTIBULK_LEN
    | READ_FLAGS_ERROR_UNAUTHENTICATED_BULK_LEN
    | READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN
    | READ_FLAGS_ERROR_BIG_BULK_COUNT
    | READ_FLAGS_ERROR_MBULK_UNEXPECTED_CHARACTER
    | READ_FLAGS_ERROR_UNEXPECTED_INLINE_FROM_REPLICATED_CLIENT
    | READ_FLAGS_ERROR_UNBALANCED_QUOTES
    | READ_FLAGS_ERROR_INVALID_CRLF;

// ── Write-flag bits ───────────────────────────────────────────────────────────

pub const WRITE_FLAGS_WRITE_ERROR: u32 = 1 << 0;
pub const WRITE_FLAGS_IS_REPLICA: u32 = 1 << 1;

// ── Error-reply flag bits ────────────────────────────────────────────────────

pub const ERR_REPLY_FLAG_NO_STATS_UPDATE: u32 = 1 << 0;
pub const ERR_REPLY_FLAG_CUSTOM: u32 = 1 << 1;

// ── Client-type constants ────────────────────────────────────────────────────

pub const CLIENT_TYPE_NORMAL: i32 = 0;
pub const CLIENT_TYPE_REPLICA: i32 = 1;
pub const CLIENT_TYPE_PUBSUB: i32 = 2;
pub const CLIENT_TYPE_PRIMARY: i32 = 3;
pub const CLIENT_TYPE_SLOT_IMPORT: i32 = 4;
pub const CLIENT_TYPE_SLOT_EXPORT: i32 = 5;

// ── Client-capa bits ─────────────────────────────────────────────────────────

pub const CLIENT_CAPA_REDIRECT: u32 = 1 << 0;

// ── Pause-action bits ────────────────────────────────────────────────────────

pub const PAUSE_ACTION_CLIENT_WRITE: u32 = 1 << 0;
pub const PAUSE_ACTION_CLIENT_ALL: u32 = 1 << 1;
pub const PAUSE_ACTION_EXPIRE: u32 = 1 << 2;
pub const PAUSE_ACTION_EVICT: u32 = 1 << 3;
pub const PAUSE_ACTION_REPLICA: u32 = 1 << 4;
pub const PAUSE_ACTIONS_CLIENT_WRITE_SET: u32 =
    PAUSE_ACTION_CLIENT_WRITE | PAUSE_ACTION_EXPIRE | PAUSE_ACTION_EVICT | PAUSE_ACTION_REPLICA;
pub const PAUSE_ACTIONS_CLIENT_ALL_SET: u32 =
    PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_EXPIRE | PAUSE_ACTION_EVICT | PAUSE_ACTION_REPLICA;

// ── Payload type ─────────────────────────────────────────────────────────────

/// Discriminant for the two kinds of payload stored in an encoded reply buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PayloadType {
    PlainReply = 0,
    BulkStrRef = 1,
}

// ── PayloadHeader ────────────────────────────────────────────────────────────

/// Header prefixed before every payload chunk in an encoded reply buffer.
/// The C definition uses `__attribute__((__packed__))`, C bitfields, and an
/// atomic field. In Rust these are represented as regular fields; the packed
/// layout is not preserved because accessing fields of a `#[repr(packed)]`
/// struct through references requires `unsafe`, which is banned in pilot crates.
#[derive(Debug)]
pub struct PayloadHeader {
 /// Length of the payload data that follows this header.
    pub payload_len: usize,
 /// Actual reply length for non-plain payloads (set lazily on write path).
    pub reply_len: usize,
 /// Cluster slot for per-slot byte tracking; -1 means no tracking.
    pub slot: i16,
 /// Packed bitfield byte: bits [0] = payload_type, [1] = track_bytes,
 /// [2..7] = reserved.
    /// TODO(port): replace with accessor methods once layout is frozen.
    pub flags_byte: u8,
 /// Set to 1 once `reply_len` has been accounted for in `io_tracked_reply_len`.
    /// TODO(architect): AtomicU8 in a packed struct — safe only with align=1.
    pub tracked_for_cob: AtomicU8,
}

impl PayloadHeader {
    pub fn payload_type(&self) -> PayloadType {
        if self.flags_byte & 0x01 != 0 {
            PayloadType::BulkStrRef
        } else {
            PayloadType::PlainReply
        }
    }

    pub fn track_bytes(&self) -> bool {
        self.flags_byte & 0x02 != 0
    }
}

// ── BulkStrRef ───────────────────────────────────────────────────────────────

/// A reference to a string object used for copy-avoidance on the write path.
/// In C this holds a raw `robj *obj` (for refcount management) and an `sds str`
/// pointer into the object. In Rust, `Arc<RedisObject>` manages the lifetime
/// `RedisString` is the byte value.
/// TODO(architect): RedisObject is in crates/redis-core/src/object.rs; need
/// Arc-based interior mutability strategy before this is live.
#[derive(Debug, Clone)]
pub struct BulkStrRef {
 /// Owned reference keeps the object alive while the write is in flight.
    /// TODO(port): replace placeholder Vec<u8> with Arc<RedisObject>.
    pub data: RedisString,
}

// ── ClientReplyBlock ─────────────────────────────────────────────────────────

/// A block in the client's linked-list reply buffer.
#[derive(Debug)]
pub struct ClientReplyBlock {
 /// Bytes used in `buf`.
    pub used: usize,
 /// Whether the buffer contains encoded (PayloadHeader-prefixed) data.
    pub buf_encoded: bool,
 /// Last payload header written into `buf` (for in-place extension).
    /// TODO(port): raw pointer in C; use offset index for Phase A.
    pub last_header_offset: Option<usize>,
 /// Payload bytes.
    pub buf: Vec<u8>,
}

impl ClientReplyBlock {
    pub fn new(size: usize) -> Self {
        Self {
            used: 0,
            buf_encoded: false,
            last_header_offset: None,
            buf: vec![0u8; size],
        }
    }

    pub fn available(&self) -> usize {
        self.buf.len() - self.used
    }
}

// ── ParsedCommand ────────────────────────────────────────────────────────────

/// A fully parsed command in the pipeline queue.
#[derive(Debug, Default, Clone)]
pub struct ParsedCommand {
    pub argc: usize,
 /// Argument strings (owned).
    pub argv: Vec<RedisString>,
    pub argv_len_sum: usize,
    pub input_bytes: u64,
    pub read_flags: u32,
 /// Cache: resolved command struct (pointer in C; deferred later).
    /// TODO(architect): replace with Arc<CommandSpec> once registry is live.
    pub cmd_name: Option<RedisString>,
    pub slot: i16,
}

// ── CommandQueue ─────────────────────────────────────────────────────────────

/// Pipeline queue of pre-parsed commands awaiting execution.
#[derive(Debug, Default)]
pub struct CommandQueue {
 /// Parsed commands waiting for execution.
    pub cmds: Vec<ParsedCommand>,
 /// Index of the next command to pop.
    pub off: usize,
}

impl CommandQueue {
    pub fn len(&self) -> usize {
        self.cmds.len()
    }

    pub fn is_empty(&self) -> bool {
        self.off >= self.cmds.len()
    }

 /// Pop the next parsed command from the queue, returning ownership.
 /// C uses a raw index and never removes elements until exhausted. Rust's
 /// ownership model requires returning an owned value. We use `Vec::remove`
 /// which is O(n) — acceptable for the small queue sizes used in practice.
    pub fn pop(&mut self) -> Option<ParsedCommand> {
        if self.off < self.cmds.len() {
            let p = self.cmds.remove(self.off);
            if self.cmds.is_empty() {
                self.off = 0;
            }
            Some(p)
        } else {
            None
        }
    }

    pub fn discard_all(&mut self) {
        self.cmds.clear();
        self.off = 0;
    }
}

// ── ClientFilter ─────────────────────────────────────────────────────────────

/// Filtering criteria for CLIENT KILL / CLIENT LIST operations.
/// Each field is `Option<T>` — `None` means "no filter on this attribute".
/// The `not_*` variants invert the match.
#[derive(Debug, Default)]
pub struct ClientFilter {
 /// Positive client-ID set filter.
    pub ids: Option<Vec<u64>>,
 /// Negative client-ID set filter.
    pub not_ids: Option<Vec<u64>>,
 /// Maximum age in seconds (connections younger than this do NOT match).
    pub max_age: Option<i64>,
 /// Positive peer-address filter.
    pub addr: Option<Vec<u8>>,
 /// Negative peer-address filter.
    pub not_addr: Option<Vec<u8>>,
 /// Positive local-address filter.
    pub laddr: Option<Vec<u8>>,
 /// Negative local-address filter.
    pub not_laddr: Option<Vec<u8>>,
 /// Positive user filter (ACL user name).
    pub user: Option<Vec<u8>>,
 /// Negative user filter.
    pub not_user: Option<Vec<u8>>,
 /// Positive client-type filter (`CLIENT_TYPE_*`). -1 = no filter.
    pub client_type: i32,
 /// Negative client-type filter. -1 = no filter.
    pub not_client_type: i32,
 /// Skip the caller itself when iterating.
    pub skipme: bool,
 /// Positive client-name filter.
    pub name: Option<Vec<u8>>,
 /// Negative client-name filter.
    pub not_name: Option<Vec<u8>>,
 /// Minimum idle time in seconds. 0 = no filter.
    pub idle: i64,
 /// Flag-string filter (each char is a flag letter from CLIENT LIST).
    pub flags: Option<Vec<u8>>,
 /// Negative flag-string filter.
    pub not_flags: Option<Vec<u8>>,
 /// Positive lib-name filter.
    pub lib_name: Option<RedisString>,
 /// Negative lib-name filter.
    pub not_lib_name: Option<RedisString>,
 /// Positive lib-ver filter.
    pub lib_ver: Option<RedisString>,
 /// Negative lib-ver filter.
    pub not_lib_ver: Option<RedisString>,
 /// Positive database-index filter. -1 = no filter.
    pub db_number: i32,
 /// Negative database-index filter. -1 = no filter.
    pub not_db_number: i32,
 /// Positive capa-string filter.
    pub capa: Option<Vec<u8>>,
 /// Negative capa-string filter.
    pub not_capa: Option<Vec<u8>>,
 /// Positive IP filter.
    pub ip: Option<Vec<u8>>,
 /// Negative IP filter.
    pub not_ip: Option<Vec<u8>>,
}

impl ClientFilter {
 /// Create a default filter: no restrictions, skipme = true.
    pub fn new_kill_default() -> Self {
        Self {
            client_type: -1,
            not_client_type: -1,
            db_number: -1,
            not_db_number: -1,
            skipme: true,
            ..Default::default()
        }
    }

 /// Create a default filter for LIST: no restrictions, skipme = false.
    pub fn new_list_default() -> Self {
        Self {
            client_type: -1,
            not_client_type: -1,
            db_number: -1,
            not_db_number: -1,
            skipme: false,
            ..Default::default()
        }
    }
}

// ── ParseResult ──────────────────────────────────────────────────────────────

/// Outcome of a single parse attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseResult {
    Ok = 0,
    Err = -1,
    NeedMore = -2,
}

// ── PausePurpose ─────────────────────────────────────────────────────────────

/// Reason a client-pause was initiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PausePurpose {
    ByClientCommand = 0,
    DuringShutdown = 1,
    DuringFailover = 2,
    DuringSlotMigration = 3,
 /// Sentinel: not paused.
    NumPausePurposes = 4,
}

// ── PauseEvent ───────────────────────────────────────────────────────────────

/// State for one pause purpose.
#[derive(Debug, Default, Clone)]
pub struct PauseEvent {
 /// Bitmask of paused actions (`PAUSE_ACTION_*`).
    pub paused_actions: u32,
 /// Expiry time in milliseconds since epoch.
    pub end: i64,
}

// ── ReplyIOV / BufWriteMetadata ──────────────────────────────────────────────

/// Gathered-write IOV builder used by `writev_to_client`.
/// TODO(architect): The C impl uses `struct iovec` arrays (OS-level vectored
/// I/O). In Rust the equivalent is `std::io::IoSlice`.
#[derive(Debug)]
pub struct ReplyIov {
 /// Total bytes referenced by all slices.
    pub iov_len_total: usize,
 /// Whether the iteration stopped early due to hitting a limit.
    pub limit_reached: bool,
 /// Bytes already written in a prior partial write (skip this many from buf start).
    pub last_written_len: usize,
 /// Number of bulk-string prefix headers generated so far.
    pub prefix_count: usize,
}

/// Metadata about one buffer segment scattered into a `ReplyIov`.
#[derive(Debug, Default)]
pub struct BufWriteMetadata {
 /// Start offset of this buffer segment in the reply list.
    pub buf_offset: usize,
 /// Number of bytes in this buffer up to `bufpos`.
    pub bufpos: usize,
 /// Actual wire bytes (differs from `bufpos` for encoded buffers).
    pub data_len: usize,
 /// Whether the entire buffer was scattered (vs. hitting limit mid-buffer).
    pub complete: bool,
}

// ── ClientIoState ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientIoState {
    Idle,
    PendingIo,
    CompletedIo,
}

// ── IoLastWritten ────────────────────────────────────────────────────────────

/// Tracks the last buffer partially or completely written to the socket.
#[derive(Debug, Default, Clone)]
pub struct IoLastWritten {
 /// Which reply-list node (by index) was last written. None = c->buf.
    pub node_index: Option<usize>,
 /// Position in that buffer at which writing was complete (0 = incomplete).
    pub bufpos: usize,
 /// Actual data bytes written from that buffer.
    pub data_len: usize,
}

// ─────────────────────────────────────────────────────────────────────────────
// Thread-local shared query buffer
// ─────────────────────────────────────────────────────────────────────────────

// Rust thread_local! wraps an Option<Vec<u8>>. The shared-qb optimisation
// avoids allocating a new query buffer for every client read; a client "takes
// ownership" by calling init_shared_query_buf when it needs to hold
// data past a read.
thread_local! {
    static THREAD_SHARED_QB: std::cell::RefCell<Option<Vec<u8>>> =
        const { std::cell::RefCell::new(None) };
}

/// Initialise (or re-initialise) the thread-local shared query buffer.
pub fn init_shared_query_buf() {
    THREAD_SHARED_QB.with(|qb| {
        *qb.borrow_mut() = Some(Vec::with_capacity(PROTO_IOBUF_LEN));
    });
}

/// Release the thread-local shared query buffer.
pub fn free_shared_query_buf() {
    THREAD_SHARED_QB.with(|qb| {
        *qb.borrow_mut() = None;
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Global: processing-events-while-blocked counter
// ─────────────────────────────────────────────────────────────────────────────

// TODO(architect): global mutable state — needs single-threaded guarantee or
// AtomicI32 with I/O threads. Using AtomicI32 here so the code compiles
// without unsafe.
static PROCESSING_EVENTS_WHILE_BLOCKED: AtomicI32 = AtomicI32::new(0);
static PAUSE_POSTPONED_CLIENTS: AtomicUsize = AtomicUsize::new(0);

pub fn is_processing_events_while_blocked() -> bool {
    PROCESSING_EVENTS_WHILE_BLOCKED.load(Ordering::Relaxed) > 0
}

pub fn pause_postponed_client_count() -> usize {
    PAUSE_POSTPONED_CLIENTS.load(Ordering::Relaxed)
}

pub fn note_pause_postponed_client() {
    PAUSE_POSTPONED_CLIENTS.fetch_add(1, Ordering::Relaxed);
}

pub fn note_pause_resumed_client() {
    let _ = PAUSE_POSTPONED_CLIENTS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
        Some(count.saturating_sub(1))
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// get_string_object_sds_used_memory / get_string_object_len
// ─────────────────────────────────────────────────────────────────────────────

/// Returns the allocated memory consumed by a string object's payload.
/// TODO(port): depends on `RedisObject` encoding variants.
pub fn get_string_object_sds_used_memory(s: &RedisString) -> usize {
    s.as_bytes().len()
}

/// Returns the logical length of a string object's payload (excluding padding).
/// TODO(port): integer-encoded objects have length 0 in the C version.
pub fn get_string_object_len(s: &RedisString) -> usize {
    s.as_bytes().len()
}

// ─────────────────────────────────────────────────────────────────────────────
// Client creation and authentication helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Create a new client, optionally with a live connection.
/// Passing `None` creates a "fake" (non-connected) client, useful for Lua
/// module contexts that need to run commands without a socket.
/// TODO(architect): Connection type is not yet defined; using `Option<()>` as
/// placeholder for the connection handle.
pub fn create_client(server: &mut RedisServer, conn: Option<()>) -> Client {
    let id = server.alloc_client_id();
    let c = Client::new(id);
    // TODO(port): initialise all client fields once Client is expanded to hold
 // the full state from (querybuf, cmd_queue, flags, repl_data, …).
 // Currently Client is a minimal pilot struct.
    if conn.is_some() {
        // TODO(port): set read handler, link client into server.clients list,
 // insert into server.clients_index radix tree.
    }
    c
}

/// Link a client into the global client list and index.
/// TODO(architect): server.clients linked-list and server.clients_index radix
/// tree are not yet in the RedisServer stub.
pub fn link_client(_server: &mut RedisServer, _client_id: ClientId) {
    // TODO(port): listAddNodeTail(server.clients, c);
    // TODO(port): raxInsert(server.clients_index, &id, sizeof(id), c, NULL);
}

/// Set authentication state on a client.
pub fn client_set_user(_c: &mut Client, _user_name: &[u8], authenticated: bool) {
    // TODO(port): store user reference on Client once ACL types are available.
    let _ = authenticated;
 // PORT NOTE: `ever_authenticated` flag set to avoid low-level output-buf limiting.
}

/// Returns true if authentication is required for this client.
pub fn auth_required(_c: &Client) -> bool {
    // TODO(port): check DefaultUser flags and c->flag.authenticated.
    false
}

/// Prepare a client to write — install write handler if needed.
/// Returns `Ok(` if the caller may append to the output buffer, or
/// `Err(RedisError::Closed)` if no data should be written to this client.
pub fn prepare_client_to_write(_c: &mut Client) -> Result<(), ()> {
    // TODO(port): check c->flag.script, c->flag.module, c->flag.close_asap,
 // c->flag.reply_off, c->flag.reply_skip, c->flag.primary, c->flag.fake.
 // For the pilot all clients are writable.
    Ok(())
}

/// Returns true if the client has pending (unsent) replies.
pub fn client_has_pending_replies(c: &Client) -> bool {
    !c.reply_buf.is_empty()
}

// ─────────────────────────────────────────────────────────────────────────────
// Low-level reply helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Append raw protocol bytes to the client's reply buffer.
/// This is the innermost write path; all `addReply*` variants funnel through it.
pub fn add_reply_proto(c: &mut Client, data: &[u8]) {
    if prepare_client_to_write(c).is_err() {
        return;
    }
    c.reply_buf.extend_from_slice(data);
}

/// Append a RESP error reply (`-ERR <msg>\r\n`) to the output buffer.
/// If `msg` already starts with `-`, the caller-supplied error code is used.
pub fn add_reply_error_length(c: &mut Client, msg: &[u8]) {
    if msg.is_empty() || msg[0] != b'-' {
        add_reply_proto(c, b"-ERR ");
    }
    add_reply_proto(c, msg);
    add_reply_proto(c, b"\r\n");
}

/// Append a RESP error reply from a byte-string message.
pub fn add_reply_error(c: &mut Client, msg: &[u8]) {
    add_reply_error_length(c, msg);
    // TODO(port): call afterErrorReply equivalent for stats/logging.
}

/// Append a formatted error reply.
pub fn add_reply_error_format(c: &mut Client, msg: Vec<u8>) {
    let cleaned: Vec<u8> = msg
        .iter()
        .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b })
        .collect();
    add_reply_error_length(c, &cleaned);
    // TODO(port): afterErrorReply stats update.
}

/// Send an "wrong number of arguments" error for the given command.
pub fn add_reply_error_arity(c: &mut Client, cmd_name: &[u8]) {
    let mut msg = b"wrong number of arguments for '".to_vec();
    msg.extend_from_slice(cmd_name);
    msg.extend_from_slice(b"' command");
    add_reply_error(c, &msg);
}

/// Send "invalid expire time" error for the given command.
pub fn add_reply_error_expire_time(c: &mut Client, cmd_name: &[u8]) {
    let mut msg = b"invalid expire time in '".to_vec();
    msg.extend_from_slice(cmd_name);
    msg.extend_from_slice(b"' command");
    add_reply_error(c, &msg);
}

/// Append a RESP simple-string reply (`+<status>\r\n`).
pub fn add_reply_status_length(c: &mut Client, status: &[u8]) {
    add_reply_proto(c, b"+");
    add_reply_proto(c, status);
    add_reply_proto(c, b"\r\n");
}

/// Append a RESP simple-string reply from a byte slice.
pub fn add_reply_status(c: &mut Client, status: &[u8]) {
    add_reply_status_length(c, status);
}

/// Append a RESP integer reply (`:N\r\n`).
pub fn add_reply_long_long(c: &mut Client, ll: i64) {
    if prepare_client_to_write(c).is_err() {
        return;
    }
    let mut buf = [0u8; 32];
    buf[0] = b':';
    let s = ll.to_string();
    let sb = s.as_bytes();
    buf[1..1 + sb.len()].copy_from_slice(sb);
    buf[1 + sb.len()] = b'\r';
    buf[2 + sb.len()] = b'\n';
    add_reply_proto(c, &buf[..3 + sb.len()]);
}

/// Append a RESP null reply (`$-1\r\n` for RESP2, `_\r\n` for RESP3).
pub fn add_reply_null(c: &mut Client, resp: u8) {
    if resp == 2 {
        add_reply_proto(c, b"$-1\r\n");
    } else {
        add_reply_proto(c, b"_\r\n");
    }
}

/// Append a RESP boolean reply for RESP3; integer 0/1 for RESP2.
pub fn add_reply_bool(c: &mut Client, resp: u8, b: bool) {
    if resp == 2 {
        add_reply_long_long(c, if b { 1 } else { 0 });
    } else {
        add_reply_proto(c, if b { b"#t\r\n" } else { b"#f\r\n" });
    }
}

/// Append a null-array reply (`*-1\r\n` for RESP2, `_\r\n` for RESP3).
pub fn add_reply_null_array(c: &mut Client, resp: u8) {
    if resp == 2 {
        add_reply_proto(c, b"*-1\r\n");
    } else {
        add_reply_proto(c, b"_\r\n");
    }
}

/// Append a RESP bulk-string reply (`$N\r\n<data>\r\n`).
pub fn add_reply_bulk(c: &mut Client, data: &[u8]) {
    if prepare_client_to_write(c).is_err() {
        return;
    }
    let len_str = data.len().to_string();
    add_reply_proto(c, b"$");
    add_reply_proto(c, len_str.as_bytes());
    add_reply_proto(c, b"\r\n");
    add_reply_proto(c, data);
    add_reply_proto(c, b"\r\n");
}

/// Append a bulk-string reply for a `RedisString`.
pub fn add_reply_bulk_string(c: &mut Client, s: &RedisString) {
    add_reply_bulk(c, s.as_bytes());
}

/// Append a bulk-long-long reply (integer as bulk string).
pub fn add_reply_bulk_long_long(c: &mut Client, ll: i64) {
    let s = ll.to_string();
    add_reply_bulk(c, s.as_bytes());
}

/// Append an aggregate-length header (`prefix N\r\n`).
pub fn add_reply_aggregate_len(c: &mut Client, length: i64, prefix: u8) {
    debug_assert!(length >= 0);
    if prepare_client_to_write(c).is_err() {
        return;
    }
    let s = length.to_string();
    add_reply_proto(c, &[prefix]);
    add_reply_proto(c, s.as_bytes());
    add_reply_proto(c, b"\r\n");
}

/// Append a RESP array-length header.
pub fn add_reply_array_len(c: &mut Client, length: i64) {
    add_reply_aggregate_len(c, length, b'*');
}

/// Append a RESP map-length header (RESP3: `%N`; RESP2: `*2N`).
pub fn add_reply_map_len(c: &mut Client, resp: u8, length: i64) {
    if resp == 2 {
        add_reply_aggregate_len(c, length * 2, b'*');
    } else {
        add_reply_aggregate_len(c, length, b'%');
    }
}

/// Append a RESP set-length header (RESP3: `~N`; RESP2: `*N`).
pub fn add_reply_set_len(c: &mut Client, resp: u8, length: i64) {
    let prefix = if resp == 2 { b'*' } else { b'~' };
    add_reply_aggregate_len(c, length, prefix);
}

/// Append a RESP attribute-length header (RESP3 only, `|N`).
pub fn add_reply_attribute_len(c: &mut Client, length: i64) {
    add_reply_aggregate_len(c, length, b'|');
}

/// Append a RESP push-length header (RESP3 only, `>N`).
pub fn add_reply_push_len(c: &mut Client, length: i64) {
    add_reply_aggregate_len(c, length, b'>');
}

/// Append a big-number reply (`(N\r\n` for RESP3; bulk string for RESP2).
pub fn add_reply_big_num(c: &mut Client, resp: u8, num: &[u8]) {
    if resp == 2 {
        add_reply_bulk(c, num);
    } else {
        add_reply_proto(c, b"(");
        add_reply_proto(c, num);
        add_reply_proto(c, b"\r\n");
    }
}

/// Append a double reply (`,D\r\n` for RESP3; bulk string for RESP2).
pub fn add_reply_double(c: &mut Client, resp: u8, d: f64) {
    if resp == 3 {
        let s = format!(",{}\r\n", d);
        add_reply_proto(c, s.as_bytes());
    } else {
        let s = format!("{}", d);
        add_reply_bulk(c, s.as_bytes());
    }
}

/// Append a verbatim-string reply (`=N\r\next:...\r\n` for RESP3; bulk for RESP2).
/// Format is `=<total_len>\r\n<3-char-ext>:<data>\r\n`
/// where total_len = len(data) + 4 (3 ext chars + colon separator).
pub fn add_reply_verbatim(c: &mut Client, resp: u8, data: &[u8], ext: &[u8; 3]) {
    if resp == 2 {
        add_reply_bulk(c, data);
    } else {
        let total = data.len() + 4; // ext(3) + colon(1)
        let prefix = format!("={}\r\n", total);
        add_reply_proto(c, prefix.as_bytes());
        add_reply_proto(c, ext);
        add_reply_proto(c, b":");
        add_reply_proto(c, data);
        add_reply_proto(c, b"\r\n");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Deferred-length ("aggregate header placeholder") system
// ─────────────────────────────────────────────────────────────────────────────

/// A handle returned by `add_reply_deferred_len`. The caller fills it in with
/// `set_deferred_array_len` (or the appropriate variant) once the count is known.
/// TODO(port): full deferred-len scheme needs the reply-list (not a flat Vec<u8>).
/// For now this is a simple in-place index.
#[derive(Debug, Clone, Copy)]
pub struct DeferredLen {
 /// Byte offset in `Client::reply_buf` where the placeholder starts.
    pub offset: usize,
 /// Number of bytes reserved for the length string (padded with spaces).
    pub reserved: usize,
}

/// Reserve space for an aggregate-length header to be filled in later.
/// TODO(port): the C implementation adds a NULL node to the reply list;
/// our flat-buffer approximation reserves fixed space with spaces.
pub fn add_reply_deferred_len(c: &mut Client) -> Option<DeferredLen> {
    if prepare_client_to_write(c).is_err() {
        return None;
    }
    let offset = c.reply_buf.len();
 // Reserve 16 bytes (enough for `*999999999\r\n`) filled with spaces.
    c.reply_buf.extend_from_slice(b"                ");
    Some(DeferredLen {
        offset,
        reserved: 16,
    })
}

/// Fill in a previously reserved aggregate-length slot.
pub fn set_deferred_aggregate_len(
    c: &mut Client,
    node: Option<DeferredLen>,
    length: i64,
    prefix: u8,
) {
    let node = match node {
        Some(n) => n,
        None => return,
    };
    debug_assert!(length >= 0);
    let header = format!("{}{}\r\n", prefix as char, length);
    let hb = header.as_bytes();
 // Overwrite the placeholder; pad with spaces if shorter.
    let end = node.offset + node.reserved;
    if end <= c.reply_buf.len() {
        let dest = &mut c.reply_buf[node.offset..end];
        let copy_len = hb.len().min(node.reserved);
        dest[..copy_len].copy_from_slice(&hb[..copy_len]);
        // TODO(port): use a proper reply-list with inline memmove as in C.
    }
}

/// Fill in a deferred array-length header.
pub fn set_deferred_array_len(c: &mut Client, node: Option<DeferredLen>, length: i64) {
    set_deferred_aggregate_len(c, node, length, b'*');
}

/// Fill in a deferred map-length header (RESP3: `%`; RESP2: `*`×2).
pub fn set_deferred_map_len(c: &mut Client, resp: u8, node: Option<DeferredLen>, length: i64) {
    if resp == 2 {
        set_deferred_aggregate_len(c, node, length * 2, b'*');
    } else {
        set_deferred_aggregate_len(c, node, length, b'%');
    }
}

/// Fill in a deferred set-length header.
pub fn set_deferred_set_len(c: &mut Client, resp: u8, node: Option<DeferredLen>, length: i64) {
    let prefix = if resp == 2 { b'*' } else { b'~' };
    set_deferred_aggregate_len(c, node, length, prefix);
}

/// Fill in a deferred attribute-length header (RESP3 only).
pub fn set_deferred_attribute_len(c: &mut Client, node: Option<DeferredLen>, length: i64) {
    set_deferred_aggregate_len(c, node, length, b'|');
}

/// Fill in a deferred push-length header (RESP3 only).
pub fn set_deferred_push_len(c: &mut Client, node: Option<DeferredLen>, length: i64) {
    set_deferred_aggregate_len(c, node, length, b'>');
}

// ─────────────────────────────────────────────────────────────────────────────
// Help / subcommand-syntax error reply helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Send the standard RESP "unknown subcommand" error.
pub fn add_reply_subcommand_syntax_error(c: &mut Client, cmd_name: &[u8], sub_name: &[u8]) {
    let truncated_sub = if sub_name.len() > 128 {
        &sub_name[..128]
    } else {
        sub_name
    };
    let mut cmd_upper = cmd_name.to_vec();
    cmd_upper.make_ascii_uppercase();
    let mut msg = b"unknown subcommand or wrong number of arguments for '".to_vec();
    msg.extend_from_slice(truncated_sub);
    msg.extend_from_slice(b"'. Try ");
    msg.extend_from_slice(&cmd_upper);
    msg.extend_from_slice(b" HELP.");
    add_reply_error(c, &msg);
}

/// Emit a HELP array reply from a string slice array.
pub fn add_reply_help(c: &mut Client, resp: u8, help: &[&[u8]]) {
    let count = help.len() + 2; // header line + HELP sub + footer lines
    let node = add_reply_deferred_len(c);
    let mut blen: i64 = 0;
    for line in help {
        add_reply_status(c, line);
        blen += 1;
    }
    add_reply_status(c, b"HELP");
    add_reply_status(c, b"    Print this help.");
    blen += 2;
    set_deferred_array_len(c, node, blen);
    let _ = (resp, count);
}

// ─────────────────────────────────────────────────────────────────────────────
// Output-buffer limits / deferred reply management
// ─────────────────────────────────────────────────────────────────────────────

/// Returns true if the deferred reply buffer is currently active.
/// TODO(port): uses only `reply_buf`; no separate deferred buffer.
pub fn is_deferred_reply_enabled(_c: &Client) -> bool {
    false
}

/// Move deferred reply into the main reply buffer and queue for writing.
/// TODO(port): implement with full deferred-reply list.
pub fn commit_deferred_reply_buffer(_c: &mut Client, _skip_if_blocked: bool) {
 // No-op: deferred reply not yet implemented.
}

// ─────────────────────────────────────────────────────────────────────────────
// Protocol parsing — inline
// ─────────────────────────────────────────────────────────────────────────────

/// Parse one inline (non-RESP) command from `buf[pos..]`.
/// Sets bits in `read_flags` to communicate the outcome. If successful,
/// `argv` and `argc` are populated.
pub fn parse_inline_buffer(
    buf: &[u8],
    pos: &mut usize,
    read_flags: &mut u32,
    is_replicated: bool,
) -> (Vec<RedisString>, u64) {
    let slice = &buf[*pos..];
    let newline_pos = match slice.iter().position(|&b| b == b'\n') {
        Some(p) => p,
        None => {
            if slice.len() > PROTO_INLINE_MAX_SIZE {
                *read_flags |= READ_FLAGS_ERROR_BIG_INLINE_REQUEST;
            }
            return (Vec::new(), 0);
        }
    };

    let linefeed_chars = if newline_pos > 0 && slice[newline_pos - 1] == b'\r' {
        2usize
    } else {
        1
    };
    let querylen = newline_pos - (linefeed_chars - 1);

    if querylen == 0 {
        *read_flags |= READ_FLAGS_INLINE_ZERO_QUERY_LEN;
        *pos += linefeed_chars;
        return (Vec::new(), 0);
    }

    if is_replicated {
        *read_flags |= READ_FLAGS_ERROR_UNEXPECTED_INLINE_FROM_REPLICATED_CLIENT;
        return (Vec::new(), 0);
    }

    let line = &slice[..querylen];
    let argv = split_inline_args(line, read_flags);
    if *read_flags & READ_FLAGS_ERROR_UNBALANCED_QUOTES != 0 {
        return (Vec::new(), 0);
    }

    *pos += querylen + linefeed_chars;

    let argv_len_sum: usize = argv.iter().map(|a| a.as_bytes().len()).sum();
    let argc = argv.len();
    let net_input_bytes = (argv_len_sum + (argc.saturating_sub(1)) + 2) as u64;

    *read_flags |= READ_FLAGS_PARSING_COMPLETED;
    (argv, net_input_bytes)
}

/// Split an inline argument line into tokens, handling shell-style quoting.
/// TODO(port): this is a simplified version; the real implementation handles
/// escaped chars and single/double quotes. Replace once available.
pub fn split_inline_args(line: &[u8], read_flags: &mut u32) -> Vec<RedisString> {
    let mut result = Vec::new();
    let mut i = 0;
    while i < line.len() {
        while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
            i += 1;
        }
        if i >= line.len() {
            break;
        }
        if line[i] == b'\'' || line[i] == b'"' {
            let quote = line[i];
            i += 1;
            let start = i;
            while i < line.len() && line[i] != quote {
                i += 1;
            }
            if i >= line.len() {
                *read_flags |= READ_FLAGS_ERROR_UNBALANCED_QUOTES;
                return Vec::new();
            }
            result.push(RedisString::from_bytes(&line[start..i]));
            i += 1; // skip closing quote
        } else {
            let start = i;
            while i < line.len() && line[i] != b' ' && line[i] != b'\t' {
                i += 1;
            }
            result.push(RedisString::from_bytes(&line[start..i]));
        }
    }
    result
}

// ─────────────────────────────────────────────────────────────────────────────
// Protocol parsing — multibulk (RESP)
// ─────────────────────────────────────────────────────────────────────────────

/// Internal multibulk parser state (per-client, carried across reads).
#[derive(Debug, Default)]
pub struct MultibulkState {
 /// Remaining bulk arguments to read (> 0 means parse is in progress).
    pub multibulklen: i64,
 /// Expected length of the current bulk argument (-1 = not yet known).
    pub bulklen: i64,
}

/// Parse one multibulk (RESP) command from `buf[pos..]`.
/// Returns `(argv, net_input_bytes, read_flags_addend)`.
/// The caller should OR the returned flags into `c->read_flags`.
pub fn parse_multibulk(
    buf: &[u8],
    pos: &mut usize,
    state: &mut MultibulkState,
    is_replicated: bool,
    auth_required: bool,
    proto_max_bulk_len: i64,
    existing_argv: &mut Vec<RedisString>,
    argv_len_sum: &mut usize,
    net_input_bytes: &mut u64,
) -> u32 {
    if state.multibulklen == 0 {
 // Parse the `*N\r\n` line.
        let slice = &buf[*pos..];
        let cr_pos = match memchr(slice, b'\r') {
            Some(p) => p,
            None => {
                if slice.len() > PROTO_INLINE_MAX_SIZE {
                    return READ_FLAGS_ERROR_BIG_MULTIBULK;
                }
                return 0;
            }
        };

        if cr_pos + 1 >= slice.len() {
            return 0; // need more data for \n
        }
        if slice[cr_pos + 1] != b'\n' {
            return READ_FLAGS_ERROR_INVALID_CRLF;
        }

 // The first byte must be `*`.
        debug_assert_eq!(slice[0], b'*');
        let num_bytes = &slice[1..cr_pos];
        let ll = match parse_i64(num_bytes) {
            Some(v) if v <= i32::MAX as i64 => v,
            _ => return READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN,
        };

        if ll > 10 && auth_required {
            return READ_FLAGS_ERROR_UNAUTHENTICATED_MULTIBULK_LEN;
        }

        *pos += cr_pos + 2; // skip `*N\r\n`
        let multibulklen_slen = num_bytes.len();

        if ll <= 0 {
            return READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN;
        }

        state.multibulklen = ll;
        state.bulklen = -1;
        existing_argv.clear();
        *argv_len_sum = 0;
        *net_input_bytes += (multibulklen_slen + 3) as u64;
    }

 // Read individual bulk arguments.
    while state.multibulklen > 0 {
        let slice = &buf[*pos..];

        if state.bulklen == -1 {
 // Parse `$N\r\n`
            let cr_pos = match memchr(slice, b'\r') {
                Some(p) => p,
                None => {
                    if slice.len() > PROTO_INLINE_MAX_SIZE {
                        return READ_FLAGS_ERROR_BIG_BULK_COUNT;
                    }
                    break; // need more data
                }
            };

            if cr_pos + 1 >= slice.len() {
                return 0;
            }

            if slice[0] != b'$' {
                return READ_FLAGS_ERROR_MBULK_UNEXPECTED_CHARACTER;
            }

            if slice[cr_pos + 1] != b'\n' {
                return READ_FLAGS_ERROR_INVALID_CRLF;
            }

            let bulklen_bytes = &slice[1..cr_pos];
            let ll = match parse_i64(bulklen_bytes) {
                Some(v) if v >= 0 => v,
                _ => return READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN,
            };

            if !is_replicated && ll > proto_max_bulk_len {
                return READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN;
            }
            if ll > 16384 && auth_required {
                return READ_FLAGS_ERROR_UNAUTHENTICATED_BULK_LEN;
            }

            let bulklen_slen = bulklen_bytes.len();
            *pos += cr_pos + 2; // skip `$N\r\n`
            state.bulklen = ll;
            *net_input_bytes += (bulklen_slen + 3) as u64;
        }

 // Read the bulk data.
        let remaining = &buf[*pos..];
        let needed = (state.bulklen + 2) as usize; // data + \r\n
        if remaining.len() < needed {
            break; // need more data
        }

 // Validate trailing CRLF.
        let data_end = state.bulklen as usize;
        if remaining[data_end] != b'\r' || remaining[data_end + 1] != b'\n' {
            return READ_FLAGS_ERROR_INVALID_CRLF;
        }

        let arg = RedisString::from_bytes(&remaining[..state.bulklen as usize]);
        *argv_len_sum += state.bulklen as usize;
        existing_argv.push(arg);
        *pos += needed;
        state.bulklen = -1;
        state.multibulklen -= 1;
    }

    if state.multibulklen == 0 {
        let argc = existing_argv.len();
        *net_input_bytes += (*argv_len_sum + argc * 2) as u64;
        return READ_FLAGS_PARSING_COMPLETED;
    }
    0
}

/// Tiny helper: find first occurrence of `needle` in `haystack`.
fn memchr(haystack: &[u8], needle: u8) -> Option<usize> {
    haystack.iter().position(|&b| b == needle)
}

/// Parse a decimal integer from ASCII bytes, returning None on failure.
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut result: i64 = 0;
    let mut neg = false;
    let mut start = 0;
    if bytes[0] == b'-' {
        neg = true;
        start = 1;
    }
    if start >= bytes.len() {
        return None;
    }
    for &b in &bytes[start..] {
        if !(b'0'..=b'9').contains(&b) {
            return None;
        }
        result = result.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    if neg {
        Some(-result)
    } else {
        Some(result)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// handleParseResults / handleParseError
// ─────────────────────────────────────────────────────────────────────────────

/// Map `read_flags` to a `ParseResult` and emit error replies if needed.
pub fn handle_parse_results(c: &mut Client, read_flags: u32, resp: u8) -> ParseResult {
    if is_parsing_error(read_flags) {
        handle_parse_error(c, read_flags, resp);
        return ParseResult::Err;
    }
    if read_flags & READ_FLAGS_INLINE_ZERO_QUERY_LEN != 0 {
        return ParseResult::Ok;
    }
    if read_flags & READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN != 0 {
        return ParseResult::Ok;
    }
    if read_flags & READ_FLAGS_PARSING_COMPLETED != 0 {
        ParseResult::Ok
    } else {
        ParseResult::NeedMore
    }
}

/// Returns true if any error-flag is set in `read_flags`.
pub fn is_parsing_error(read_flags: u32) -> bool {
    read_flags & READ_FLAGS_ALL_PARSE_ERRORS != 0
}

/// Emit the appropriate error reply for a parse-error `read_flags` bitmap.
pub fn handle_parse_error(c: &mut Client, read_flags: u32, _resp: u8) {
    if read_flags & READ_FLAGS_ERROR_BIG_INLINE_REQUEST != 0 {
        add_reply_error(c, b"Protocol error: too big inline request");
    } else if read_flags & READ_FLAGS_ERROR_BIG_MULTIBULK != 0 {
        add_reply_error(c, b"Protocol error: too big mbulk count string");
    } else if read_flags & READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN != 0 {
        add_reply_error(c, b"Protocol error: invalid multibulk length");
    } else if read_flags & READ_FLAGS_ERROR_UNAUTHENTICATED_MULTIBULK_LEN != 0 {
        add_reply_error(c, b"Protocol error: unauthenticated multibulk length");
    } else if read_flags & READ_FLAGS_ERROR_UNAUTHENTICATED_BULK_LEN != 0 {
        add_reply_error(c, b"Protocol error: unauthenticated bulk length");
    } else if read_flags & READ_FLAGS_ERROR_BIG_BULK_COUNT != 0 {
        add_reply_error(c, b"Protocol error: too big bulk count string");
    } else if read_flags & READ_FLAGS_ERROR_MBULK_UNEXPECTED_CHARACTER != 0 {
        add_reply_error(c, b"Protocol error: expected '$', got unexpected character");
    } else if read_flags & READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN != 0 {
        add_reply_error(c, b"Protocol error: invalid bulk length");
    } else if read_flags & READ_FLAGS_ERROR_UNBALANCED_QUOTES != 0 {
        add_reply_error(c, b"Protocol error: unbalanced quotes in request");
    } else if read_flags & READ_FLAGS_ERROR_INVALID_CRLF != 0 {
        add_reply_error(c, b"Protocol error: invalid CRLF in request");
    } else if read_flags & READ_FLAGS_ERROR_UNEXPECTED_INLINE_FROM_REPLICATED_CLIENT != 0 {
 // logged by the caller; no RESP reply sent to a replica
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// processInputBuffer
// ─────────────────────────────────────────────────────────────────────────────

/// Incrementally process the client's query buffer, parsing and executing
/// commands until the buffer is exhausted or a blocking/error condition occurs.
/// Returns `Ok(` if processing should continue; `Err(RedisError::Closed)`
/// if the client was freed as a side effect.
/// TODO(port): the full implementation requires access to the server state
/// (processCommand, etc.) and the client's multibulk state. This is a
/// structural placeholder that captures the control flow.
pub fn process_input_buffer(
    c: &mut Client,
    buf: &[u8],
    mb_state: &mut MultibulkState,
    resp: u8,
    proto_max_bulk_len: i64,
) -> Result<(), RedisError> {
    let mut pos = 0;

    loop {
        if pos >= buf.len() {
            break;
        }

        let is_replicated = false; // TODO(port): check c->read_flags & READ_FLAGS_REPLICATED
        let auth_req = auth_required(c);
        let mut read_flags = if is_replicated {
            READ_FLAGS_REPLICATED
        } else {
            0
        };
        if auth_req {
            read_flags |= READ_FLAGS_AUTH_REQUIRED;
        }

        let (argv, net_bytes) = if buf[pos] == b'*' {
            let mut argv = Vec::new();
            let mut argv_len_sum = 0usize;
            let mut net_input = 0u64;
            let flags_addend = parse_multibulk(
                buf,
                &mut pos,
                mb_state,
                is_replicated,
                auth_req,
                proto_max_bulk_len,
                &mut argv,
                &mut argv_len_sum,
                &mut net_input,
            );
            read_flags |= flags_addend;
            (argv, net_input)
        } else {
            let (argv, net_bytes) =
                parse_inline_buffer(buf, &mut pos, &mut read_flags, is_replicated);
            (argv, net_bytes)
        };

        match handle_parse_results(c, read_flags, resp) {
            ParseResult::NeedMore => break,
            ParseResult::Err => return Err(RedisError::runtime(b"protocol error")),
            ParseResult::Ok => {}
        }

        if argv.is_empty() {
            continue;
        }

        // TODO(port): call processCommand(c, argv) here once server context
 // is available. For now we just store the parsed args.
        c.set_args(argv);
        // TODO(port): commandProcessed(c) — update replication offset, reset client.
        let _ = net_bytes;
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Write path
// ─────────────────────────────────────────────────────────────────────────────

/// Write pending reply data from `client.reply_buf` to the socket.
/// Returns `Ok(bytes_written)` or an I/O error.
/// TODO(architect): Connection type not defined; `writer` is a generic
/// `std::io::Write` placeholder.
pub fn write_to_client<W: std::io::Write>(
    c: &mut Client,
    writer: &mut W,
) -> Result<usize, RedisError> {
    if c.reply_buf.is_empty() {
        return Ok(0);
    }
    let n = writer
        .write(&c.reply_buf)
        .map_err(|e| RedisError::io(e.kind()))?;
    c.reply_buf.drain(..n);
    Ok(n)
}

/// Mark a client as needing asynchronous close (add to clients_to_close list).
/// TODO(port): server.clients_to_close list not yet in RedisServer stub.
pub fn free_client_async(_server: &mut RedisServer, _client_id: ClientId) {
    // TODO(port): add to server.clients_to_close if not already present.
}

/// Reset client state between commands (clear argv, flags, slot, etc.).
pub fn reset_client(c: &mut Client) {
    c.reset_args();
    // TODO(port): reset all additional fields once Client is expanded:
 // redact_arg_bitmap, cur_script, net_input_bytes_curr_cmd, slot,
 // flag.executing_command, flag.replication_done, flag.buffered_reply,
 // flag.keyspace_notified, net_output_bytes_curr_cmd, deferred_reply_errors.
}

// ─────────────────────────────────────────────────────────────────────────────
// Client peer-id / sockname helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Get or cache the client's peer address string.
/// TODO(architect): Connection type needed to call connFormatAddr.
pub fn get_client_peer_id(_c: &Client) -> Vec<u8> {
    // TODO(port): call conn.format_addr(remote=true) and cache in c->peerid.
    b"?:0".to_vec()
}

/// Get or cache the client's local socket name string.
/// TODO(architect): Connection type needed to call connFormatAddr.
pub fn get_client_sockname(_c: &Client) -> Vec<u8> {
    // TODO(port): call conn.format_addr(remote=false) and cache in c->sockname.
    b"?:0".to_vec()
}

// ─────────────────────────────────────────────────────────────────────────────
// CLIENT sub-command implementations
// ─────────────────────────────────────────────────────────────────────────────

/// CLIENT ID — return the client's numeric ID.
pub fn client_id_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let id = ctx.client.id;
    add_reply_long_long(ctx.client, id as i64);
    Ok(())
}

/// CLIENT INFO — return verbose info about the current connection.
pub fn client_info_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let info = cat_client_info_string(ctx.client, false);
    let mut full_info = info;
    full_info.push(b'\n');
    add_reply_verbatim(ctx.client, 2, &full_info, b"txt");
    Ok(())
}

/// CLIENT GETNAME — return the current connection name.
/// TODO(port): client name stored on Client struct (not yet in pilot stub).
pub fn client_get_name_command(ctx: &mut CommandContext) -> RedisResult<()> {
    // TODO(port): if c->name exists, addReplyBulk(c, c->name) else addReplyNull.
    add_reply_null(ctx.client, 2);
    Ok(())
}

/// CLIENT SETNAME — set the current connection name.
/// TODO(port): validate and store name on Client struct.
pub fn client_set_name_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let name = ctx.arg(2)?;
    if validate_client_attr(name.as_bytes()) {
        // TODO(port): store name on c->name.
        add_reply_status(ctx.client, b"OK");
    } else {
        add_reply_error(
            ctx.client,
            b"Client names cannot contain spaces, newlines or special characters.",
        );
    }
    Ok(())
}

/// CLIENT REPLY ON|OFF|SKIP
/// TODO(port): set c->flag.reply_off / c->flag.reply_skip_next flags.
pub fn client_reply_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let mode = ctx.arg(2)?;
    match mode.as_bytes() {
        b"on" | b"ON" => {
            // TODO(port): c->flag.reply_skip = 0; c->flag.reply_off = 0;
            add_reply_status(ctx.client, b"OK");
        }
        b"off" | b"OFF" => {
            // TODO(port): c->flag.reply_off = 1;
        }
        b"skip" | b"SKIP" => {
            // TODO(port): if !c->flag.reply_off { c->flag.reply_skip_next = 1; }
        }
        _ => {
            add_reply_error(ctx.client, b"ERR syntax error");
        }
    }
    Ok(())
}

/// CLIENT NO-EVICT ON|OFF
pub fn client_no_evict_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let mode = ctx.arg(2)?;
    match mode.as_bytes() {
        b"on" | b"ON" => {
            // TODO(port): c->flag.no_evict = 1; removeClientFromMemUsageBucket(c, 0);
            add_reply_status(ctx.client, b"OK");
        }
        b"off" | b"OFF" => {
            // TODO(port): c->flag.no_evict = 0; updateClientMemUsageAndBucket(c);
            add_reply_status(ctx.client, b"OK");
        }
        _ => {
            add_reply_error(ctx.client, b"ERR syntax error");
        }
    }
    Ok(())
}

/// CLIENT NO-TOUCH ON|OFF
pub fn client_no_touch_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let mode = ctx.arg(2)?;
    match mode.as_bytes() {
        b"on" | b"ON" => {
            // TODO(port): c->flag.no_touch = 1;
            add_reply_status(ctx.client, b"OK");
        }
        b"off" | b"OFF" => {
            // TODO(port): c->flag.no_touch = 0;
            add_reply_status(ctx.client, b"OK");
        }
        _ => {
            add_reply_error(ctx.client, b"ERR syntax error");
        }
    }
    Ok(())
}

/// CLIENT CAPA <option>… — declare client capabilities.
pub fn client_capa_command(ctx: &mut CommandContext) -> RedisResult<()> {
    // TODO(port): iterate argv[2..] and set c->capa bits.
    add_reply_status(ctx.client, b"OK");
    Ok(())
}

/// CLIENT IMPORT-SOURCE ON|OFF
pub fn client_import_source_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let mode = ctx.arg(2)?;
    match mode.as_bytes() {
        b"on" | b"ON" => {
            // TODO(port): check server.import_mode; set c->flag.import_source = 1;
            add_reply_status(ctx.client, b"OK");
        }
        b"off" | b"OFF" => {
            // TODO(port): c->flag.import_source = 0;
            add_reply_status(ctx.client, b"OK");
        }
        _ => {
            add_reply_error(ctx.client, b"ERR syntax error");
        }
    }
    Ok(())
}

/// CLIENT GETREDIR — return the redirection target ID.
pub fn client_get_redir_command(ctx: &mut CommandContext) -> RedisResult<()> {
    // TODO(port): if c->flag.tracking { addReplyLongLong(c->pubsub_data->client_tracking_redirection) }
    add_reply_long_long(ctx.client, -1);
    Ok(())
}

/// CLIENT UNBLOCK <id> [TIMEOUT|ERROR]
/// TODO(port): server client-index lookup not yet available.
pub fn client_unblock_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let id_rs = ctx.arg(2)?;
    let id_str = id_rs.as_bytes();
    let _id = parse_i64(id_str).ok_or_else(RedisError::not_integer)?;
    // TODO(port): lookupClientByID(id); if found and blocked, unblockClient.
    add_reply_long_long(ctx.client, 0); // TODO(port): return 1 if client was unblocked
    Ok(())
}

/// CLIENT KILL <addr> | CLIENT KILL <option> <value>…
/// TODO(port): requires full server.clients iteration.
pub fn client_kill_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"CLIENT"));
    }
    // TODO(port): parse filter options, iterate clients, kill matching ones.
    add_reply_long_long(ctx.client, 0);
    Ok(())
}

/// CLIENT LIST
/// TODO(port): requires server.clients iteration and catClientInfoString.
pub fn client_list_command(ctx: &mut CommandContext) -> RedisResult<()> {
    // TODO(port): getAllClientsInfoString / getAllFilteredClientsInfoString.
    let info = cat_client_info_string(ctx.client, false);
    let mut response = info;
    response.push(b'\n');
    add_reply_verbatim(ctx.client, 2, &response, b"txt");
    Ok(())
}

/// CLIENT PAUSE <timeout> [WRITE|ALL]
/// `CLIENT PAUSE <timeout-ms> [WRITE|ALL]` (default ALL). Sets
/// `ByClientCommand` pause event; `pause_clients_by_client` keeps the most
/// restrictive action and the longest end time across overlapping calls.
pub fn client_pause_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let timeout = match parse_i64(ctx.arg(2)?.as_bytes()) {
        Some(t) if t >= 0 => t,
        Some(_) => {
            add_reply_error(ctx.client, b"timeout is negative");
            return Ok(());
        }
        _ => {
            add_reply_error(ctx.client, b"timeout is not an integer or out of range");
            return Ok(());
        }
    };
    let pause_all = if ctx.arg_count() > 3 {
        let mode = ctx.arg(3)?;
        if mode.as_bytes().eq_ignore_ascii_case(b"write") {
            false
        } else if mode.as_bytes().eq_ignore_ascii_case(b"all") {
            true
        } else {
            add_reply_error(ctx.client, b"ERR syntax error");
            return Ok(());
        }
    } else {
        true
    };
    let end = crate::util::mstime().saturating_add(timeout);
    apply_client_pause(ctx.server(), end, pause_all);
    add_reply_status(ctx.client, b"OK");
    Ok(())
}

pub fn apply_client_pause(server: &RedisServer, end: i64, pause_all: bool) {
    let mut events = server
        .pause_events
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    pause_clients_by_client(&mut events, end, pause_all);
    refresh_cached_paused_actions(server, &events, crate::util::mstime());
}

pub fn apply_failover_write_pause(server: &RedisServer, end: i64) {
    let mut events = server
        .pause_events
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    pause_actions(
        &mut events,
        PausePurpose::DuringFailover,
        end,
        PAUSE_ACTIONS_CLIENT_WRITE_SET,
    );
    refresh_cached_paused_actions(server, &events, crate::util::mstime());
}

pub fn apply_failover_pause(server: &RedisServer, end: i64) {
    let mut events = server
        .pause_events
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    pause_actions(
        &mut events,
        PausePurpose::DuringFailover,
        end,
        PAUSE_ACTIONS_CLIENT_ALL_SET,
    );
    refresh_cached_paused_actions(server, &events, crate::util::mstime());
}

pub fn clear_failover_pause(server: &RedisServer) {
    let mut events = server
        .pause_events
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    unpause_actions(&mut events, PausePurpose::DuringFailover);
    refresh_cached_paused_actions(server, &events, crate::util::mstime());
}

/// CLIENT UNPAUSE
/// Clears the client-command pause and (via the gate) resumes any postponed clients.
pub fn client_unpause_command(ctx: &mut CommandContext) -> RedisResult<()> {
    {
        let mut events = ctx
            .server()
            .pause_events
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        unpause_actions(&mut events, PausePurpose::ByClientCommand);
        refresh_cached_paused_actions(ctx.server(), &events, crate::util::mstime());
    }
    add_reply_status(ctx.client, b"OK");
    Ok(())
}

/// Human-readable pause summary for `INFO clients`: returns
/// `(paused_reason, paused_actions, paused_timeout_milliseconds)`. Reports
/// `("none", "none", 0)` when nothing is paused.
pub fn pause_info(events: &[PauseEvent; 4], now: i64) -> (&'static str, &'static str, i64) {
    let paused = update_paused_actions(events, now);
    if paused == 0 {
        return ("none", "none", 0);
    }
    let timeout_action = if paused & PAUSE_ACTION_CLIENT_ALL != 0 {
        PAUSE_ACTION_CLIENT_ALL
    } else {
        PAUSE_ACTION_CLIENT_WRITE
    };
    let (timeout, purpose) = get_paused_action_timeout(events, timeout_action, now);
    let reason = match purpose {
        PausePurpose::ByClientCommand => "client_pause",
        PausePurpose::DuringShutdown => "shutdown",
        PausePurpose::DuringFailover => "failover",
        PausePurpose::DuringSlotMigration => "slot_migration",
        PausePurpose::NumPausePurposes => "none",
    };
    let actions = if paused & PAUSE_ACTION_CLIENT_ALL != 0 {
        "all"
    } else {
        "write"
    };
    (reason, actions, timeout.max(0))
}

/// CLIENT TRACKING ON|OFF [REDIRECT <id>] [BCAST] [PREFIX <p>…] [OPTIN] [OPTOUT] [NOLOOP]
/// TODO(port): enableTracking / disableTracking not yet available.
pub fn client_tracking_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let mode = ctx.arg(2)?;
    match mode.as_bytes() {
        b"on" | b"ON" => {
            // TODO(port): parse options, call enableTracking(c, redir, options, prefix, numprefix).
            add_reply_status(ctx.client, b"OK");
        }
        b"off" | b"OFF" => {
            // TODO(port): disableTracking(c);
            add_reply_status(ctx.client, b"OK");
        }
        _ => {
            add_reply_error(ctx.client, b"ERR syntax error");
        }
    }
    Ok(())
}

/// CLIENT CACHING YES|NO
pub fn client_caching_command(ctx: &mut CommandContext) -> RedisResult<()> {
    // TODO(port): check c->flag.tracking / tracking_optin / tracking_optout.
    let opt = ctx.arg(2)?;
    match opt.as_bytes() {
        b"yes" | b"YES" => {
            // TODO(port): c->flag.tracking_caching = 1;
            add_reply_status(ctx.client, b"OK");
        }
        b"no" | b"NO" => {
            // TODO(port): c->flag.tracking_caching = 1 (optout path).
            add_reply_status(ctx.client, b"OK");
        }
        _ => {
            add_reply_error(ctx.client, b"ERR syntax error");
        }
    }
    Ok(())
}

/// CLIENT TRACKINGINFO
pub fn client_tracking_info_command(ctx: &mut CommandContext) -> RedisResult<()> {
    // TODO(port): full tracking info requires pubsub_data and tracking prefixes.
    add_reply_map_len(ctx.client, 2, 3);
    add_reply_bulk(ctx.client, b"flags");
    add_reply_array_len(ctx.client, 1);
    add_reply_bulk(ctx.client, b"off");
    add_reply_bulk(ctx.client, b"redirect");
    add_reply_long_long(ctx.client, -1);
    add_reply_bulk(ctx.client, b"prefixes");
    add_reply_array_len(ctx.client, 0);
    Ok(())
}

/// Top-level CLIENT dispatcher (falls through to subcommand-syntax error).
pub fn client_command(ctx: &mut CommandContext) -> RedisResult<()> {
 // The subcommand table is generated; the top-level just errors.
    let subcommand = ctx
        .arg(1)
        .map(|s| s.as_bytes().to_vec())
        .unwrap_or_default();
    add_reply_subcommand_syntax_error(ctx.client, b"CLIENT", &subcommand);
    Ok(())
}

/// CLIENT SETINFO <attr> <value>
pub fn client_setinfo_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let attr = ctx.arg(2)?;
    let val = ctx.arg(3)?;
    match attr.as_bytes() {
        b"lib-name" | b"LIB-NAME" => {
            if !validate_client_attr(val.as_bytes()) {
                add_reply_error(
                    ctx.client,
                    b"lib-name cannot contain spaces, newlines or special characters.",
                );
                return Ok(());
            }
            // TODO(port): store c->lib_name = val;
        }
        b"lib-ver" | b"LIB-VER" => {
            if !validate_client_attr(val.as_bytes()) {
                add_reply_error(
                    ctx.client,
                    b"lib-ver cannot contain spaces, newlines or special characters.",
                );
                return Ok(());
            }
            // TODO(port): store c->lib_ver = val;
        }
        other => {
            let mut msg = b"Unrecognized option '".to_vec();
            msg.extend_from_slice(other);
            msg.push(b'\'');
            add_reply_error(ctx.client, &msg);
            return Ok(());
        }
    }
    add_reply_status(ctx.client, b"OK");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// QUIT, RESET, HELLO, security-warning commands
// ─────────────────────────────────────────────────────────────────────────────

/// QUIT — close the connection after sending OK.
pub fn quit_command(ctx: &mut CommandContext) -> RedisResult<()> {
    add_reply_status(ctx.client, b"OK");
    // TODO(port): c->flag.close_after_reply = 1;
    Ok(())
}

/// RESET — reset the client state to freshly-connected.
pub fn reset_command_impl(ctx: &mut CommandContext) -> RedisResult<()> {
    // TODO(port): check replica/primary/module flags; call clearClientConnectionState.
    add_reply_status(ctx.client, b"RESET");
    Ok(())
}

/// HELLO [<version> [AUTH <user> <pass>] [SETNAME <name>]]
/// TODO(port): auth, setname, server.sentinel_mode, server.cluster_enabled,
/// server.extended_redis_compat, and module list not yet wired.
pub fn hello_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let mut ver: i64 = 0;
    if ctx.arg_count() >= 2 {
        let ver_rs = ctx.arg(1)?;
        ver = parse_i64(ver_rs.as_bytes()).ok_or_else(|| {
            RedisError::runtime(b"Protocol version is not an integer or out of range")
        })?;
        if !(2..=3).contains(&ver) {
            add_reply_error(ctx.client, b"-NOPROTO unsupported protocol version");
            return Ok(());
        }
    }

    // TODO(port): full option parsing: AUTH, SETNAME, ACL authentication,
 // module notification, sentinel mode, availability zone.

    let resp = if ver == 0 { 2u8 } else { ver as u8 };

    add_reply_map_len(ctx.client, resp, 6);

    add_reply_bulk(ctx.client, b"server");
    add_reply_bulk(ctx.client, b"valkey");

    add_reply_bulk(ctx.client, b"version");
    add_reply_bulk(ctx.client, b"8.0.0"); // TODO(port): use VALKEY_VERSION constant

    add_reply_bulk(ctx.client, b"proto");
    add_reply_long_long(ctx.client, resp as i64);

    add_reply_bulk(ctx.client, b"id");
    add_reply_long_long(ctx.client, ctx.client.id as i64);

    add_reply_bulk(ctx.client, b"mode");
    add_reply_bulk(ctx.client, b"standalone"); // TODO(port): check sentinel/cluster

    add_reply_bulk(ctx.client, b"role");
    add_reply_bulk(ctx.client, b"master"); // TODO(port): check replication state

 // "modules" array is omitted later.
    // TODO(port): add_reply_bulk(b"modules"); addReplyLoadedModules(c);

    Ok(())
}

/// Handler for POST and "Host:" pseudo-commands (cross-protocol scripting guard).
pub fn security_warning_command(ctx: &mut CommandContext) -> RedisResult<()> {
    // TODO(port): log security warning with rate limiting; call freeClientAsync.
 // For now just close the client.
    // TODO(architect): need server reference to call freeClientAsync.
    let _ = ctx;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Client-filter helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Validate that a client-attribute value contains only printable non-space bytes.
pub fn validate_client_attr(val: &[u8]) -> bool {
    val.iter().all(|&b| (b'!'..=b'~').contains(&b))
}

/// Validate the flag-filter string: each byte must be a known CLIENT LIST flag char.
pub fn validate_client_flag_filter(filter: &[u8]) -> bool {
    const VALID: &[u8] = b"OSMPxbtRBdcuAUreTIiEN";
    filter.iter().all(|b| VALID.contains(b))
}

/// Validate the capa-filter string: currently only 'r' is defined.
pub fn validate_client_capa_filter(filter: &[u8]) -> bool {
    filter.iter().all(|&b| b == b'r')
}

/// Test whether a client's IP matches the filter string.
pub fn client_matches_ip_filter(peer_id: &[u8], ip_filter: &[u8]) -> bool {
    let peer = if peer_id.first() == Some(&b'[') {
        &peer_id[1..] // IPv6
    } else {
        peer_id
    };
    if !peer.starts_with(ip_filter) {
        return false;
    }
    let rest = &peer[ip_filter.len()..];
    let rest = if rest.first() == Some(&b']') {
        &rest[1..]
    } else {
        rest
    };
    rest.first() == Some(&b':') && rest.get(1) != Some(&b'0')
}

// ─────────────────────────────────────────────────────────────────────────────
// catClientInfoString — CLIENT INFO / CLIENT LIST output
// ─────────────────────────────────────────────────────────────────────────────

/// Build the CLIENT INFO string for a client.
/// TODO(port): most fields are placeholders until Client is expanded.
pub fn cat_client_info_string(c: &Client, _hide_user_data: bool) -> Vec<u8> {
    let id = c.id;
    // TODO(port): fill in all fields: addr, laddr, fd, name, age, idle,
 // flags, capa, db, sub, psub, ssub, multi, watch, qbuf, qbuf-free,
 // argv-mem, multi-mem, rbs, rbp, obl, oll, omem, tot-mem, events,
 // cmd, user, redir, resp, lib-name, lib-ver, tot-net-, tot-net-out,
 // tot-cmds.
    format!("id={} addr=?:0 laddr=?:0 fd=-1 name= age=0 idle=0 flags=N capa= db=0 sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events= cmd=NULL user=(superuser) redir=-1 resp=2 lib-name= lib-ver= tot-net-in=0 tot-net-out=0 tot-cmds=0", id)
        .into_bytes()
}

// ─────────────────────────────────────────────────────────────────────────────
// Output-buffer limit checking
// ─────────────────────────────────────────────────────────────────────────────

/// Check if a client's output buffer has exceeded soft or hard limits.
/// TODO(port): server client_obuf_limits config not yet accessible here.
pub fn check_client_output_buffer_limits(
    reply_bytes: usize,
    client_type: i32,
    hard_limit: usize,
    soft_limit: usize,
    soft_limit_seconds: u64,
    soft_limit_reached_time: Option<Instant>,
    now: Instant,
) -> bool {
    let _ = (
        client_type,
        soft_limit_seconds,
        soft_limit_reached_time,
        now,
    );
    if hard_limit > 0 && reply_bytes >= hard_limit {
        return true;
    }
    if soft_limit > 0 && reply_bytes >= soft_limit {
        // TODO(port): check elapsed time against soft_limit_seconds.
        return false; // first time — don't kill yet
    }
    false
}

// ─────────────────────────────────────────────────────────────────────────────
// Client-type name mapping
// ─────────────────────────────────────────────────────────────────────────────

/// Map a client-type name string to its `CLIENT_TYPE_*` constant.
pub fn get_client_type_by_name(name: &[u8]) -> i32 {
    let lower: Vec<u8> = name.iter().map(|b| b.to_ascii_lowercase()).collect();
    match lower.as_slice() {
        b"normal" => CLIENT_TYPE_NORMAL,
        b"slave" | b"replica" => CLIENT_TYPE_REPLICA,
        b"pubsub" => CLIENT_TYPE_PUBSUB,
        b"master" | b"primary" => CLIENT_TYPE_PRIMARY,
        _ => -1,
    }
}

/// Map a `CLIENT_TYPE_*` constant to its display name.
pub fn get_client_type_name(client_type: i32) -> Option<&'static [u8]> {
    match client_type {
        CLIENT_TYPE_NORMAL => Some(b"normal"),
        CLIENT_TYPE_REPLICA => Some(b"slave"),
        CLIENT_TYPE_PUBSUB => Some(b"pubsub"),
        CLIENT_TYPE_PRIMARY => Some(b"master"),
        CLIENT_TYPE_SLOT_IMPORT => Some(b"slot-import"),
        CLIENT_TYPE_SLOT_EXPORT => Some(b"slot-export"),
        _ => None,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pause / unpause
// ─────────────────────────────────────────────────────────────────────────────

/// Return the human-readable pause-reason string.
pub fn get_paused_reason(purpose: PausePurpose) -> &'static [u8] {
    match purpose {
        PausePurpose::ByClientCommand => b"client_pause",
        PausePurpose::DuringShutdown => b"shutdown_in_progress",
        PausePurpose::DuringFailover => b"failover_in_progress",
        PausePurpose::DuringSlotMigration => b"slot_migration_in_progress",
        PausePurpose::NumPausePurposes => b"none",
    }
}

/// Return the currently-paused action bitmask and timeout for `action`.
pub fn get_paused_action_timeout(
    events: &[PauseEvent; 4],
    action: u32,
    mstime: i64,
) -> (i64, PausePurpose) {
    let mut timeout = 0i64;
    let mut purpose = PausePurpose::NumPausePurposes;
    for (i, p) in events.iter().enumerate() {
        if p.paused_actions & action != 0 {
            let t = p.end - mstime;
            if t > timeout {
                timeout = t;
                purpose = match i {
                    0 => PausePurpose::ByClientCommand,
                    1 => PausePurpose::DuringShutdown,
                    2 => PausePurpose::DuringFailover,
                    3 => PausePurpose::DuringSlotMigration,
                    _ => PausePurpose::NumPausePurposes,
                };
            }
        }
    }
    (timeout, purpose)
}

/// Recompute the aggregate `paused_actions` bitmask from all purpose events.
pub fn update_paused_actions(events: &[PauseEvent; 4], mstime: i64) -> u32 {
    let mut paused = 0u32;
    for p in events {
        if p.end > mstime {
            paused |= p.paused_actions;
        }
    }
    paused
}

fn refresh_cached_paused_actions(server: &RedisServer, events: &[PauseEvent; 4], now: i64) -> u32 {
    let paused = update_paused_actions(events, now);
    server
        .cached_paused_actions
        .store(paused, Ordering::Relaxed);
    paused
}

pub fn current_paused_actions(server: &RedisServer) -> u32 {
    if server.cached_paused_actions.load(Ordering::Relaxed) == 0 {
        return 0;
    }
    let now = crate::util::mstime();
    let events = server
        .pause_events
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    refresh_cached_paused_actions(server, &events, now)
}

pub fn is_server_paused_for(server: &RedisServer, action: u32) -> bool {
    current_paused_actions(server) & action != 0
}

/// Set pause for `PAUSE_BY_CLIENT_COMMAND` (CLIENT PAUSE implementation).
pub fn pause_clients_by_client(events: &mut [PauseEvent; 4], end_time: i64, pause_all: bool) {
    let p = &mut events[PausePurpose::ByClientCommand as usize];
    let old_pause_all_is_active =
        p.end > crate::util::mstime() && p.paused_actions & PAUSE_ACTION_CLIENT_ALL != 0;
    let actions = if pause_all {
        PAUSE_ACTIONS_CLIENT_ALL_SET
    } else if old_pause_all_is_active {
        PAUSE_ACTIONS_CLIENT_ALL_SET // keep most restrictive
    } else {
        PAUSE_ACTIONS_CLIENT_WRITE_SET
    };
    pause_actions(events, PausePurpose::ByClientCommand, end_time, actions);
}

/// Apply a pause for the given purpose, end-time and action bitmask.
pub fn pause_actions(events: &mut [PauseEvent; 4], purpose: PausePurpose, end: i64, actions: u32) {
    let p = &mut events[purpose as usize];
    p.paused_actions = actions;
    if p.end < end {
        p.end = end;
    }
}

/// Clear a pause for the given purpose.
pub fn unpause_actions(events: &mut [PauseEvent; 4], purpose: PausePurpose) {
    let p = &mut events[purpose as usize];
    p.end = 0;
    p.paused_actions = 0;
}

/// Check whether any of the `actions_bitmask` bits are currently paused.
pub fn is_paused_actions(paused: u32, actions_bitmask: u32) -> u32 {
    paused & actions_bitmask
}

// ─────────────────────────────────────────────────────────────────────────────
// Client-argument rewriting helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Returns true if argument `i` should be redacted in logs/commandlog.
pub fn client_command_arg_should_be_redacted(redact_bitmap: u32, arg_index: usize) -> bool {
    if arg_index < 1 {
        return false;
    }
    if arg_index >= 32 {
        return redact_bitmap & 1 != 0;
    }
    (redact_bitmap >> arg_index) & 1 != 0
}

/// Mark argument `argc` as redactable.
pub fn redact_client_command_argument(redact_bitmap: &mut u32, argc: usize) {
    debug_assert!(argc >= 1);
    if argc < 32 {
        *redact_bitmap |= 1 << argc;
    } else {
        *redact_bitmap |= 1;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// getClientEvictionLimit / evictClients
// ─────────────────────────────────────────────────────────────────────────────

/// Compute the effective client memory eviction limit.
pub fn get_client_eviction_limit(maxmemory_clients: i64, maxmemory: u64) -> usize {
    const MIN_LIMIT: usize = 128 * 1024;
    let raw = if maxmemory_clients < 0 && maxmemory > 0 {
        let pct = -maxmemory_clients as f64 / 100.0;
        (maxmemory as f64 * pct) as usize
    } else if maxmemory_clients > 0 {
        maxmemory_clients as usize
    } else {
        return 0;
    };
    raw.max(MIN_LIMIT)
}

// ─────────────────────────────────────────────────────────────────────────────
// I/O thread entry points
// ─────────────────────────────────────────────────────────────────────────────

/// Called from an I/O thread to read and parse a client's query buffer.
/// TODO(architect): I/O thread architecture is a future architect decision.
/// This stub preserves the high-level control flow only.
pub fn io_thread_read_query_from_client(
    _c: &mut Client,
    _mb_state: &mut MultibulkState,
    _read_flags: &mut u32,
    _proto_max_bulk_len: i64,
) {
    // TODO(port): readToQueryBuf(c); parseInputBuffer(c); trimCommandQueue(c);
 // prepareCommandQueue(c); trim querybuf; set io_read_state = CLIENT_COMPLETED_IO;
 // sendToMainThread(c, JOB_RES_READ_CLIENT);
}

/// Called from an I/O thread to write a client's pending reply data.
/// TODO(architect): see `io_thread_read_query_from_client`.
pub fn io_thread_write_to_client(_c: &mut Client, _is_replica: bool) {
    // TODO(port): _writeToClient(c) or writeToReplica(c);
 // set io_write_state = CLIENT_COMPLETED_IO;
 // sendToMainThread(c, JOB_RES_WRITE_CLIENT);
}

// ─────────────────────────────────────────────────────────────────────────────
// processEventsWhileBlocked
// ─────────────────────────────────────────────────────────────────────────────

/// Process event-loop events while the server is blocked (e.g. in a Lua script).
/// TODO(architect): event-loop integration is deferred to a future phase.
pub fn process_events_while_blocked() {
    PROCESSING_EVENTS_WHILE_BLOCKED.fetch_add(1, Ordering::Relaxed);
    // TODO(port): loop 4 times calling aeProcessEvents; whileBlockedCron();
    PROCESSING_EVENTS_WHILE_BLOCKED.fetch_sub(1, Ordering::Relaxed);
    debug_assert!(PROCESSING_EVENTS_WHILE_BLOCKED.load(Ordering::Relaxed) >= 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregate output-buffer memory / CLIENT memory usage
// ─────────────────────────────────────────────────────────────────────────────

/// Estimate the total output-buffer memory for a client.
pub fn get_client_output_buffer_memory_usage(
    reply_buf_len: usize,
    reply_list_count: usize,
) -> usize {
    const LIST_NODE_OVERHEAD: usize = 32; // sizeof(listNode) + sizeof(clientReplyBlock) approx.
    reply_buf_len + LIST_NODE_OVERHEAD * reply_list_count
}

// ─────────────────────────────────────────────────────────────────────────────
//  PORT STATUS
// ─────────────────────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         97
//   port_notes:    5
//   unsafe_blocks: 0
//   notes:         Structural port — all major sections translated.
//                  Key gaps: full Client struct expansion (needs arch packet),
//                  Connection abstraction, I/O-thread wiring (Phase 3),
//                  copy-avoidance encoded buffer scheme (PayloadHeader layout),
//                  deferred-len reply-list (Phase A uses flat Vec<u8>),
//                  server.clients iteration for CLIENT KILL/LIST/INFO,
//                  ACL/tracking/pubsub integration (Phase 5+).
//                  Logic for pure-Rust functions (parsers, pause, filter,
//                  type-name maps) is faithfully translated and should be
//                  correct with mechanical import fixes in Phase B.
// ──────────────────────────────────────────────────────────────────────────
