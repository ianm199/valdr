//! Request/response logging for Redis clients.
//!
//! Implements the interface for logging clients' requests and responses to a
//! file. Mirrors the behaviour of C's `logreqres.c`, which is compiled-in only
//! when the `LOG_REQ_RES` preprocessor macro is defined. In Rust the equivalent
//! gate is the `log_req_res` Cargo feature; the public API symbols are present
//! regardless, but their implementations become no-ops when the feature is
//! absent.
//!
//! **Log format** — each completed command cycle writes:
//! ```text
//! <arg0-len>\r\n<arg0-bytes>\r\n
//! <arg1-len>\r\n<arg1-bytes>\r\n
//! ...
//! 12\r\n__argv_end__\r\n
//! <RESP-encoded response bytes>
//! ```
//!
//! C: logreqres.c (303 lines, 8 functions)

use redis_types::{RedisError, RedisString};
use std::io::Write as _;

// ── Constants ──────────────────────────────────────────────────────────────

/// Sentinel written after all argv entries to delimit request from response.
///
/// C: `"__argv_end__"` literal (logreqres.c:208).
const ARGV_END: &[u8] = b"__argv_end__";

/// Minimum initial capacity for the log accumulation buffer.
///
/// C: `max(len, 1024)` in `reqresAppendBuffer` (logreqres.c:97).
const MIN_BUF_CAPACITY: usize = 1024;

// ── Sub-structs ────────────────────────────────────────────────────────────

/// Byte offset into the tail reply-list node at the moment the offset is saved.
///
/// C: anonymous `last_node` struct inside `clientReqResInfo` (server.h:1108–1111).
#[derive(Debug, Default, Clone, Copy)]
pub struct ReplyNodeOffset {
    /// Zero-based index of the tail reply-list node when the offset was saved.
    pub index: usize,
    /// Bytes used in that node when the offset was saved.
    pub used: usize,
}

/// Reply-buffer offsets captured once at the start of each command cycle.
///
/// C: anonymous `offset` struct inside `clientReqResInfo` (server.h:1102–1112).
#[derive(Debug, Default, Clone)]
pub struct ReplyOffset {
    /// Becomes `true` after the offset is saved; subsequent save calls no-op.
    pub saved: bool,
    /// Byte position in the static reply buffer (`client.bufpos`) at save time.
    pub bufpos: usize,
    /// Snapshot of the reply-list tail node at save time.
    pub last_node: ReplyNodeOffset,
}

/// Per-client request/response logging state.
///
/// C: `clientReqResInfo` (server.h:1094–1113).
///
/// TODO(architect): add `pub reqres: ClientReqResInfo` to `Client` in
/// `crates/redis-core/src/client.rs`. The field should be unconditionally
/// present; keeping it in a `Vec<u8>` with zero capacity is zero-cost when
/// logging is disabled and avoids an `Option` wrapper at every call site.
#[derive(Debug, Default)]
pub struct ClientReqResInfo {
    /// `true` once the command argv has been logged for the current cycle.
    pub argv_logged: bool,
    /// Accumulation buffer holding request bytes and response bytes before
    /// they are flushed to the log file.
    ///
    /// C: `unsigned char *buf` + `size_t used` + `size_t capacity`
    /// (server.h:1098–1100). Rust `Vec<u8>` manages capacity natively.
    pub buf: Vec<u8>,
    /// Saved reply-buffer offsets for the current command cycle.
    pub offset: ReplyOffset,
}

/// A single block in the client's reply-block linked list.
///
/// C: `clientReplyBlock` (server.h:864–869).
///
/// PORT NOTE: the C `Client` has a static inline buffer (`c->buf`/`c->bufpos`)
/// *and* a linked list of `clientReplyBlock` nodes (`c->reply`). The current
/// Rust `Client` uses a single flat `reply_buf: Vec<u8>`. This struct is
/// retained for a faithful translation of `reqres_append_response`; callers
/// should pass `reply_blocks = &[]` until the two-area reply model lands.
/// TODO(architect): introduce static-buffer + reply-block-list model on
/// `Client` before `reqres_append_response` can be fully wired in Phase B.
#[derive(Debug)]
pub struct ClientReplyBlock {
    /// Bytes stored in this block.
    pub buf: Vec<u8>,
}

/// High-level client classification used by the logging gate.
///
/// C: `CLIENT_TYPE_*` constants (server.h:359–362). Only the subset relevant
/// to logging decisions is modelled here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientType {
    Normal,
    Replica,
    Pubsub,
    Primary,
    Other,
}

// ── Private helpers ────────────────────────────────────────────────────────

/// Return `true` if the command name should be excluded from req/res logging.
///
/// The excluded commands either emit streaming or non-standard responses that
/// would break the log format, or are explicitly hazardous (DEBUG SEGFAULT).
/// Comparison is ASCII-case-insensitive, matching C's `strcasecmp`.
///
/// C: logreqres.c:187–191 (inline filter inside `reqresAppendRequest`).
fn is_excluded_command(name: &[u8]) -> bool {
    const EXCLUDED: &[&[u8]] = &[
        b"debug",
        b"sync",
        b"psync",
        b"monitor",
        b"subscribe",
        b"unsubscribe",
        b"ssubscribe",
        b"sunsubscribe",
        b"psubscribe",
        b"punsubscribe",
    ];
    EXCLUDED.iter().any(|&ex| ex.eq_ignore_ascii_case(name))
}

// ── impl ClientReqResInfo ──────────────────────────────────────────────────

impl ClientReqResInfo {
    /// Construct a zero-initialised instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append raw bytes to the accumulation buffer, growing capacity as needed.
    ///
    /// C: `reqresAppendBuffer` (logreqres.c:94–106). The C version manages a
    /// raw `zmalloc`/`zrealloc` buffer; Rust `Vec` handles growth natively.
    fn append_buffer(&mut self, data: &[u8]) -> usize {
        if self.buf.capacity() == 0 {
            self.buf.reserve(data.len().max(MIN_BUF_CAPACITY));
        }
        self.buf.extend_from_slice(data);
        data.len()
    }

    /// Append one argument in log format: `<decimal-length>\r\n<bytes>\r\n`.
    ///
    /// C: `reqresAppendArg` (logreqres.c:110–118). C uses `ll2string` to
    /// convert the length; Rust's `itoa`-style decimal is identical.
    ///
    /// PERF(port): `format!` allocates a temporary `String` for the length
    /// decimal — profile in Phase B; consider a stack buffer instead.
    fn append_arg(&mut self, arg: &[u8]) -> usize {
        let len_decimal = format!("{}", arg.len());
        let mut written = self.append_buffer(len_decimal.as_bytes());
        written += self.append_buffer(b"\r\n");
        written += self.append_buffer(arg);
        written += self.append_buffer(b"\r\n");
        written
    }

    /// Reset the logging state, optionally releasing the accumulation buffer.
    ///
    /// When `free_buf` is `true` the buffer heap memory is released (`Vec::new`
    /// replaces it); otherwise `Vec::clear` is used so capacity is retained for
    /// the next command cycle.
    ///
    /// C: `reqresReset` (logreqres.c:125–128).
    pub fn reset(&mut self, free_buf: bool) {
        if free_buf {
            self.buf = Vec::new();
        } else {
            self.buf.clear();
        }
        self.argv_logged = false;
        self.offset = ReplyOffset::default();
    }
}

// ── Public API ─────────────────────────────────────────────────────────────

/// Determine whether req/res logging should be active for the given client.
///
/// All parameters correspond to fields that will exist on `Client` once the
/// `TODO(architect)` items above are resolved. Call sites in `networking.c`
/// will become calls on `Client` methods once the client struct is extended.
///
/// C: `reqresShouldLog` (logreqres.c:77–92, `static`).
pub fn reqres_should_log(
    log_file_configured: bool,
    is_fake: bool,
    has_conn: bool,
    is_pubsub: bool,
    is_monitor: bool,
    is_replica: bool,
    client_type: ClientType,
) -> bool {
    if !log_file_configured {
        return false;
    }
    if is_fake || !has_conn {
        return false;
    }
    if is_pubsub || is_monitor || is_replica {
        return false;
    }
    client_type == ClientType::Normal
}

/// Reset `info`, optionally releasing the accumulation buffer.
///
/// C: `reqresReset` — public entry point (logreqres.c:125–128 / 283–286).
pub fn reqres_reset(info: &mut ClientReqResInfo, free_buf: bool) {
    info.reset(free_buf);
}

/// Save the client's current reply-buffer offsets so that
/// `reqres_append_response` can later compute which bytes belong to this command.
///
/// Only the first call per command cycle has any effect; once
/// `info.offset.saved` is `true`, subsequent calls are no-ops. This guards
/// against double-saving in pipelining scenarios.
///
/// Parameters:
/// - `should_log` — pre-computed result of `reqres_should_log`.
/// - `static_bufpos` — current value of `client.bufpos`.
/// - `reply_list_tail` — `Some((index, used))` when the reply-block list is
///   non-empty (index = last-node index, used = bytes in that node); `None`
///   when the list is empty.
///
/// C: `reqresSaveClientReplyOffset` (logreqres.c:160–175 / 288–290).
pub fn reqres_save_client_reply_offset(
    info: &mut ClientReqResInfo,
    should_log: bool,
    static_bufpos: usize,
    reply_list_tail: Option<(usize, usize)>,
) {
    if !should_log {
        return;
    }
    if info.offset.saved {
        return;
    }

    info.offset.saved = true;
    info.offset.bufpos = static_bufpos;

    match reply_list_tail {
        Some((index, used)) => {
            info.offset.last_node = ReplyNodeOffset { index, used };
        }
        None => {
            info.offset.last_node = ReplyNodeOffset { index: 0, used: 0 };
        }
    }
}

/// Append the command's argv to the accumulation buffer.
///
/// Each argument is written as `<decimal-length>\r\n<bytes>\r\n`, followed by
/// the `__argv_end__` sentinel in the same format.
///
/// Returns the total bytes appended, or `0` when logging is suppressed.
///
/// PORT NOTE: C's implementation branches on `sdsEncodedObject` vs
/// `OBJ_ENCODING_INT` to format integer-encoded objects as decimal strings.
/// `RedisString` always stores bytes, so no such branch is needed here.
/// C: logreqres.c:177–209 / 292–295.
pub fn reqres_append_request(
    info: &mut ClientReqResInfo,
    should_log: bool,
    argv: &[RedisString],
) -> usize {
    debug_assert!(!argv.is_empty(), "reqres_append_request: empty argv");

    if !should_log {
        return 0;
    }

    let cmd_name = argv[0].as_bytes();
    if is_excluded_command(cmd_name) {
        return 0;
    }

    info.argv_logged = true;

    let mut ret: usize = 0;
    for arg in argv {
        let bytes: &[u8] = arg.as_bytes();
        ret += info.append_arg(bytes);
    }
    ret + info.append_arg(ARGV_END)
}

/// Capture the command's response bytes and flush request + response to the
/// log file.
///
/// Bytes in both the static reply buffer and the reply-block list that arrived
/// *after* `reqres_save_client_reply_offset` was called are appended to
/// `info.buf`. The full accumulated buffer (request + response) is then
/// written to `log_file_path` in append mode.
///
/// Returns the number of response bytes captured, or `0` when logging is
/// suppressed.
///
/// Parameters:
/// - `should_log` — pre-computed result of `reqres_should_log`.
/// - `static_buf` — the full static reply buffer (`client.buf`).
/// - `static_bufpos` — write position in `static_buf` (`client.bufpos`).
/// - `reply_blocks` — slice of reply-list blocks; pass `&[]` for commands
///   whose reply fits in the static buffer.
/// - `log_file_path` — destination log file; `None` skips the write.
///
/// PORT NOTE: C opens the file, writes, then closes on every call. In Rust
/// we do the same via `OpenOptions::append`. A persistent `BufWriter`
/// would be more efficient; defer to Phase B once the server config wires
/// the file handle lifetime. C: logreqres.c:211–277 / 297–300.
pub fn reqres_append_response(
    info: &mut ClientReqResInfo,
    should_log: bool,
    static_buf: &[u8],
    static_bufpos: usize,
    reply_blocks: &[ClientReplyBlock],
    log_file_path: Option<&std::path::Path>,
) -> Result<usize, RedisError> {
    if !should_log {
        return Ok(0);
    }
    if !info.argv_logged {
        // C: logreqres.c:216 — "Example: UNSUBSCRIBE"
        return Ok(0);
    }
    if !info.offset.saved {
        // C: logreqres.c:219 — "Example: module client blocked on keys + CLIENT KILL"
        return Ok(0);
    }

    let mut ret: usize = 0;

    // Append bytes from the static reply buffer produced by this command.
    // C: logreqres.c:223–226.
    if static_bufpos > info.offset.bufpos {
        let slice = &static_buf[info.offset.bufpos..static_bufpos];
        ret += info.append_buffer(slice);
    }

    // Determine the current tail position of the reply-block list.
    // C: logreqres.c:228–233.
    let curr_index = if reply_blocks.is_empty() {
        0
    } else {
        reply_blocks.len() - 1
    };
    let curr_used = reply_blocks.last().map_or(0, |b| b.buf.len());

    // Append bytes from reply-block list nodes produced by this command.
    // C: logreqres.c:235–267.
    if curr_index > info.offset.last_node.index || curr_used > info.offset.last_node.used {
        for (i, block) in reply_blocks.iter().enumerate() {
            if block.buf.is_empty() {
                continue;
            }

            if i < info.offset.last_node.index {
                // Entirely predates the saved offset; skip.
                continue;
            }

            let slice = if i == info.offset.last_node.index {
                // Partially-written node: only bytes after the saved position.
                // C: logreqres.c:256–259.
                &block.buf[info.offset.last_node.used..]
            } else {
                // New node: all bytes.
                // C: logreqres.c:261–263.
                &block.buf[..]
            };

            ret += info.append_buffer(slice);
        }
    }

    debug_assert!(
        ret > 0,
        "reqres_append_response: zero response bytes captured"
    );

    // Flush accumulated buffer (request + response) to the log file.
    // C: logreqres.c:270–274 — fopen/fwrite/fclose.
    if let Some(path) = log_file_path {
        let mut fp = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .map_err(|e: std::io::Error| RedisError::Io(e.kind()))?;
        fp.write_all(&info.buf)
            .map_err(|e: std::io::Error| RedisError::Io(e.kind()))?;
    }

    Ok(ret)
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/logreqres.c  (303 lines, 8 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         3
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:         Logic translated faithfully. Two TODOs(architect) block
//                  Phase B wiring: (1) ClientReqResInfo must become a field
//                  on Client; (2) Client needs the static-buffer + reply-block-
//                  list two-area reply model before reqres_append_response can
//                  be fully exercised. Feature-gating (log_req_res Cargo
//                  feature) is not yet enforced; all symbols are always compiled.
// ──────────────────────────────────────────────────────────────────────────
