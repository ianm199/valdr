//! RESP2 incremental request parser.
//!
//! Parses bytes flowing from a client to the server. Supports the two
//! historical request encodings used by Redis clients:
//!
//! * **Inline** — whitespace-separated tokens terminated by `\r\n`. Used by
//!   telnet/`redis-cli` for quick tests.
//! * **Multibulk** — RESP2 array of bulk strings: `*N\r\n$L\r\n<bytes>\r\n…`.
//!   The wire encoding used by every modern client library.
//!
//! C reference: `networking.c::processInlineBuffer` and
//! `networking.c::processMultibulkBuffer`.
//!
//! # Contract
//!
//! [`parse_inline_or_multibulk`] is the entry point. It returns:
//!
//! * `Ok(Some((argv, consumed)))` — a complete command was parsed. `consumed`
//!   is the number of bytes from `buf` that should be drained.
//! * `Ok(None)` — the buffer holds a partial command; the caller should keep
//!   reading and call again.
//! * `Err(RedisError)` — a protocol error. The caller should close the
//!   connection.

use redis_types::{RedisError, RedisString};

/// Maximum inline command length (matches C `PROTO_INLINE_MAX_SIZE`, 64 KiB).
pub const PROTO_INLINE_MAX_SIZE: usize = 64 * 1024;

/// Maximum number of arguments in a multibulk command (matches C
/// `PROTO_REQ_MULTIBULK_MAX_LEN`, 1 million).
pub const PROTO_REQ_MULTIBULK_MAX_LEN: i64 = 1_000_000;

/// Maximum bulk-string payload length, also bounds inline tokens.
/// 512 MiB matches the C `PROTO_MAX_BULK_LEN` default.
pub const PROTO_MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

/// Parse one complete RESP2 request from `buf`.
///
/// Sniffs the first byte: `*` selects the multibulk path, anything else falls
/// through to inline. Returns `Ok(None)` if the buffer does not yet hold a
/// complete frame.
pub fn parse_inline_or_multibulk(
    buf: &[u8],
) -> Result<Option<(Vec<RedisString>, usize)>, RedisError> {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] == b'*' {
        parse_multibulk(buf)
    } else {
        parse_inline(buf)
    }
}

/// Parse one complete RESP2 request from `buf`, reusing `out` as argv storage.
///
/// This is the hot-path variant used by the live server. It preserves the
/// public [`parse_inline_or_multibulk`] contract but avoids allocating a fresh
/// argv vector for every pipelined command. Argument byte strings are still
/// owned `RedisString`s; moving those to borrowed slices is a larger command
/// API change.
pub fn parse_inline_or_multibulk_into(
    buf: &[u8],
    out: &mut Vec<RedisString>,
) -> Result<Option<usize>, RedisError> {
    out.clear();
    let parsed = if buf.is_empty() {
        Ok(None)
    } else if buf[0] == b'*' {
        parse_multibulk_into(buf, out)
    } else {
        parse_inline_into(buf, out)
    };
    if matches!(parsed, Ok(None) | Err(_)) {
        out.clear();
    }
    parsed
}

/// Parse a multibulk request: `*N\r\n$L\r\n<bytes>\r\n…`.
///
/// C: `networking.c::processMultibulkBuffer`.
fn parse_multibulk(buf: &[u8]) -> Result<Option<(Vec<RedisString>, usize)>, RedisError> {
    let mut pos: usize = 1;

    let (argc, after_argc) = match read_multibulk_count(buf, pos)? {
        Some(v) => v,
        None => return Ok(None),
    };
    pos = after_argc;

    if argc <= 0 {
        return Ok(Some((Vec::new(), pos)));
    }
    if argc > PROTO_REQ_MULTIBULK_MAX_LEN {
        return Err(RedisError::runtime(b"Protocol error: invalid multibulk length"));
    }

    let mut argv: Vec<RedisString> = Vec::with_capacity(argc as usize);

    for _ in 0..argc {
        if pos >= buf.len() {
            return Ok(None);
        }
        if buf[pos] != b'$' {
            let got = buf[pos];
            let msg = format!("Protocol error: expected '$', got '{}'", got as char);
            return Err(RedisError::runtime(msg));
        }
        pos += 1;

        let (bulklen, after_len) = match read_bulk_length(buf, pos)? {
            Some(v) => v,
            None => return Ok(None),
        };
        pos = after_len;

        if bulklen < 0 || bulklen > PROTO_MAX_BULK_LEN {
            return Err(RedisError::runtime(b"Protocol error: invalid bulk length"));
        }
        let bulklen = bulklen as usize;

        let payload_end = pos
            .checked_add(bulklen)
            .ok_or_else(|| RedisError::runtime(b"Protocol error: bulk length overflow"))?;
        let frame_end = payload_end
            .checked_add(2)
            .ok_or_else(|| RedisError::runtime(b"Protocol error: bulk length overflow"))?;
        if frame_end > buf.len() {
            return Ok(None);
        }
        if &buf[payload_end..frame_end] != b"\r\n" {
            return Err(RedisError::runtime(
                b"Protocol error: invalid CRLF in request",
            ));
        }

        argv.push(RedisString::from_bytes(&buf[pos..payload_end]));
        pos = frame_end;
    }

    Ok(Some((argv, pos)))
}

/// Same parser as [`parse_multibulk`], but pushes args into caller-owned argv
/// storage.
fn parse_multibulk_into(
    buf: &[u8],
    out: &mut Vec<RedisString>,
) -> Result<Option<usize>, RedisError> {
    let mut pos: usize = 1;

    let (argc, after_argc) = match read_multibulk_count(buf, pos)? {
        Some(v) => v,
        None => return Ok(None),
    };
    pos = after_argc;

    if argc <= 0 {
        return Ok(Some(pos));
    }
    if argc > PROTO_REQ_MULTIBULK_MAX_LEN {
        return Err(RedisError::runtime(b"Protocol error: invalid multibulk length"));
    }

    out.reserve(argc as usize);
    for _ in 0..argc {
        if pos >= buf.len() {
            return Ok(None);
        }
        if buf[pos] != b'$' {
            let got = buf[pos];
            let msg = format!("Protocol error: expected '$', got '{}'", got as char);
            return Err(RedisError::runtime(msg));
        }
        pos += 1;

        let (bulklen, after_len) = match read_bulk_length(buf, pos)? {
            Some(v) => v,
            None => return Ok(None),
        };
        pos = after_len;

        if bulklen < 0 || bulklen > PROTO_MAX_BULK_LEN {
            return Err(RedisError::runtime(b"Protocol error: invalid bulk length"));
        }
        let bulklen = bulklen as usize;

        let payload_end = pos
            .checked_add(bulklen)
            .ok_or_else(|| RedisError::runtime(b"Protocol error: bulk length overflow"))?;
        let frame_end = payload_end
            .checked_add(2)
            .ok_or_else(|| RedisError::runtime(b"Protocol error: bulk length overflow"))?;
        if frame_end > buf.len() {
            return Ok(None);
        }
        if &buf[payload_end..frame_end] != b"\r\n" {
            return Err(RedisError::runtime(
                b"Protocol error: invalid CRLF in request",
            ));
        }

        out.push(RedisString::from_bytes(&buf[pos..payload_end]));
        pos = frame_end;
    }

    Ok(Some(pos))
}

/// Parse an inline command: whitespace-separated tokens ending in `\r\n` or `\n`.
///
/// C: `networking.c::processInlineBuffer`.
fn parse_inline(buf: &[u8]) -> Result<Option<(Vec<RedisString>, usize)>, RedisError> {
    let newline = match buf.iter().position(|&b| b == b'\n') {
        Some(n) => n,
        None => {
            if buf.len() > PROTO_INLINE_MAX_SIZE {
                return Err(RedisError::runtime(
                    b"Protocol error: too big inline request",
                ));
            }
            return Ok(None);
        }
    };

    let line_end = if newline > 0 && buf[newline - 1] == b'\r' {
        newline - 1
    } else {
        newline
    };
    let line = &buf[..line_end];

    let argv = split_inline_tokens(line)?;
    Ok(Some((argv, newline + 1)))
}

fn parse_inline_into(
    buf: &[u8],
    out: &mut Vec<RedisString>,
) -> Result<Option<usize>, RedisError> {
    let (argv, consumed) = match parse_inline(buf)? {
        Some(v) => v,
        None => return Ok(None),
    };
    out.extend(argv);
    Ok(Some(consumed))
}

/// Read a multibulk array count (`*N`) terminated by `\r\n` starting at `pos`.
///
/// Returns `Ok(Some((value, new_pos)))` on success, `Ok(None)` if incomplete,
/// or `Err` with `"Protocol error: invalid multibulk length"` when the integer
/// field contains non-digit bytes — matching Valkey's `string2ll` failure path
/// that sets `READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN`.
///
/// C: `parseMultibulk` — `string2ll` on the `*` count field.
fn read_multibulk_count(buf: &[u8], pos: usize) -> Result<Option<(i64, usize)>, RedisError> {
    read_resp_integer(buf, pos, b"Protocol error: invalid multibulk length")
}

/// Read a bulk-string length (`$N`) terminated by `\r\n` starting at `pos`.
///
/// Returns `Ok(Some((value, new_pos)))` on success, `Ok(None)` if incomplete,
/// or `Err` with `"Protocol error: invalid bulk length"` when the integer
/// field contains non-digit bytes — matching Valkey's `string2ll` failure path
/// that sets `READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN`.
///
/// C: `parseMultibulk` — `string2ll` on the `$` length field.
fn read_bulk_length(buf: &[u8], pos: usize) -> Result<Option<(i64, usize)>, RedisError> {
    read_resp_integer(buf, pos, b"Protocol error: invalid bulk length")
}

/// Shared RESP integer-line reader used by both [`read_multibulk_count`] and
/// [`read_bulk_length`].
///
/// Locates the `\r\n` terminator, validates the `\r\n` sequence, and delegates
/// digit parsing to `parse_i64_ascii`. `err_msg` is the context-specific error
/// text emitted when the digit field is invalid — this lets callers produce the
/// exact Valkey wire text.
///
/// When no `\r` is found and the remaining buffer exceeds `PROTO_INLINE_MAX_SIZE`
/// the field is clearly malformed; returns `err_msg` immediately. This matches
/// Valkey's `READ_FLAGS_ERROR_BIG_BULK_COUNT` / `READ_FLAGS_ERROR_BIG_MULTIBULK`
/// size guards in `parseMultibulk`.
///
/// C: common `string2ll` + CRLF check pattern in `parseMultibulk`.
fn read_resp_integer(
    buf: &[u8],
    pos: usize,
    err_msg: &'static [u8],
) -> Result<Option<(i64, usize)>, RedisError> {
    if pos >= buf.len() {
        return Ok(None);
    }
    let cr_offset = match buf[pos..].iter().position(|&b| b == b'\r') {
        Some(o) => o,
        None => {
            if buf.len() - pos > PROTO_INLINE_MAX_SIZE {
                return Err(RedisError::runtime(err_msg));
            }
            return Ok(None);
        }
    };
    let cr_idx = pos + cr_offset;
    if cr_idx + 1 >= buf.len() {
        return Ok(None);
    }
    if buf[cr_idx + 1] != b'\n' {
        return Err(RedisError::runtime(b"Protocol error: invalid CRLF in request"));
    }
    let value = parse_i64_ascii(&buf[pos..cr_idx], err_msg)?;
    Ok(Some((value, cr_idx + 2)))
}

/// Parse an ASCII signed decimal `i64` from `bytes`.
///
/// Returns `Err` with the caller-supplied `err_msg` when the field is empty
/// or contains non-digit bytes. This lets each call site emit the Valkey-exact
/// error text for its context (multibulk count vs. bulk length).
///
/// C: `string2ll()` from `util.c`, restricted to the RESP integer field cases.
fn parse_i64_ascii(bytes: &[u8], err_msg: &'static [u8]) -> Result<i64, RedisError> {
    if bytes.is_empty() {
        return Err(RedisError::runtime(err_msg));
    }
    let (negative, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else if bytes[0] == b'+' {
        (false, &bytes[1..])
    } else {
        (false, bytes)
    };
    if digits.is_empty() {
        return Err(RedisError::runtime(err_msg));
    }
    let mut acc: i64 = 0;
    for &b in digits {
        let d = match b {
            b'0'..=b'9' => (b - b'0') as i64,
            _ => return Err(RedisError::runtime(err_msg)),
        };
        acc = acc
            .checked_mul(10)
            .and_then(|v| v.checked_add(d))
            .ok_or_else(|| RedisError::runtime(err_msg))?;
    }
    Ok(if negative { -acc } else { acc })
}

/// Split an inline command line into argv tokens.
///
/// Ports Valkey's `sdsnsplitargs` / `sdsparsearg` from `sds.c`. Handles:
///
/// - Bare (unquoted) tokens terminated by ASCII whitespace.
/// - Double-quoted strings (`"…"`) with `\n`, `\r`, `\t`, `\b`, `\a`,
///   `\\`, `\"`, and `\xHH` escape sequences.
/// - Single-quoted strings (`'…'`) with `\'` as the only escape.
/// - Adjacent quoted/unquoted segments are concatenated into one token,
///   e.g. `"foo"bar` → `foobar` (Valkey behaviour).
///
/// Returns `Err` with `"Protocol error: unbalanced quotes in request"` when
/// quotes are not properly closed or when a closed-quote is immediately
/// followed by a non-space character (e.g. `"foo"bar` is actually NOT an
/// error in Valkey — adjacent is allowed, but `"foo"'bar` transitions work
/// because after `"` closes, the outer loop either hits whitespace and ends
/// the token, OR hits another quote character and opens a new quote context).
///
/// Matches Valkey's `sdsnsplitargs_internal` returning `NULL` on parse failure,
/// which maps to `READ_FLAGS_ERROR_UNBALANCED_QUOTES` →
/// `"Protocol error: unbalanced quotes in request"`.
///
/// C: `sds.c:sdsnsplitargs_internal` + `sdsparsearg`.
fn split_inline_tokens(line: &[u8]) -> Result<Vec<RedisString>, RedisError> {
    let mut argv: Vec<RedisString> = Vec::new();
    let mut i = 0;

    while i < line.len() {
        while i < line.len() && is_inline_whitespace(line[i]) {
            i += 1;
        }
        if i >= line.len() {
            break;
        }

        let mut token: Vec<u8> = Vec::new();
        loop {
            if i >= line.len() {
                break;
            }
            match line[i] {
                b'"' => {
                    i += 1;
                    loop {
                        if i >= line.len() {
                            return Err(RedisError::runtime(
                                b"Protocol error: unbalanced quotes in request",
                            ));
                        }
                        match line[i] {
                            b'"' => {
                                i += 1;
                                break;
                            }
                            b'\\' if i + 1 < line.len() => {
                                i += 1;
                                match line[i] {
                                    b'n' => { token.push(b'\n'); i += 1; }
                                    b'r' => { token.push(b'\r'); i += 1; }
                                    b't' => { token.push(b'\t'); i += 1; }
                                    b'b' => { token.push(0x08); i += 1; }
                                    b'a' => { token.push(0x07); i += 1; }
                                    b'x' if i + 2 < line.len()
                                        && is_hex(line[i + 1])
                                        && is_hex(line[i + 2]) =>
                                    {
                                        token.push(
                                            (hex_val(line[i + 1]) << 4) | hex_val(line[i + 2]),
                                        );
                                        i += 3;
                                    }
                                    other => { token.push(other); i += 1; }
                                }
                            }
                            other => { token.push(other); i += 1; }
                        }
                    }
                }
                b'\'' => {
                    i += 1;
                    loop {
                        if i >= line.len() {
                            return Err(RedisError::runtime(
                                b"Protocol error: unbalanced quotes in request",
                            ));
                        }
                        match line[i] {
                            b'\'' => {
                                i += 1;
                                break;
                            }
                            b'\\' if i + 1 < line.len() && line[i + 1] == b'\'' => {
                                token.push(b'\'');
                                i += 2;
                            }
                            other => { token.push(other); i += 1; }
                        }
                    }
                }
                b if is_inline_whitespace(b) => break,
                other => { token.push(other); i += 1; }
            }
        }
        argv.push(RedisString::from_bytes(&token));
    }
    Ok(argv)
}

fn is_inline_whitespace(b: u8) -> bool {
    matches!(b, b' ' | b'\t')
}

fn is_hex(b: u8) -> bool {
    matches!(b, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_buffer_is_incomplete() {
        assert!(matches!(parse_inline_or_multibulk(b""), Ok(None)));
    }

    #[test]
    fn inline_ping() {
        let (argv, n) = parse_inline_or_multibulk(b"PING\r\n").unwrap().unwrap();
        assert_eq!(n, 6);
        assert_eq!(argv.len(), 1);
        assert_eq!(argv[0].as_bytes(), b"PING");
    }

    #[test]
    fn inline_with_args_lf_only() {
        let (argv, n) = parse_inline_or_multibulk(b"SET foo bar\n").unwrap().unwrap();
        assert_eq!(n, 12);
        assert_eq!(argv.len(), 3);
        assert_eq!(argv[0].as_bytes(), b"SET");
        assert_eq!(argv[1].as_bytes(), b"foo");
        assert_eq!(argv[2].as_bytes(), b"bar");
    }

    #[test]
    fn multibulk_ping() {
        let buf = b"*1\r\n$4\r\nPING\r\n";
        let (argv, n) = parse_inline_or_multibulk(buf).unwrap().unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(argv.len(), 1);
        assert_eq!(argv[0].as_bytes(), b"PING");
    }

    #[test]
    fn multibulk_set_command() {
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (argv, n) = parse_inline_or_multibulk(buf).unwrap().unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(argv.len(), 3);
        assert_eq!(argv[0].as_bytes(), b"SET");
        assert_eq!(argv[1].as_bytes(), b"foo");
        assert_eq!(argv[2].as_bytes(), b"bar");
    }

    #[test]
    fn multibulk_into_reuses_argv_storage() {
        let buf = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let mut argv = Vec::with_capacity(8);
        argv.push(RedisString::from_bytes(b"stale"));
        let cap = argv.capacity();

        let n = parse_inline_or_multibulk_into(buf, &mut argv).unwrap().unwrap();

        assert_eq!(n, buf.len());
        assert!(argv.capacity() >= cap);
        assert_eq!(argv.len(), 2);
        assert_eq!(argv[0].as_bytes(), b"GET");
        assert_eq!(argv[1].as_bytes(), b"key");
    }

    #[test]
    fn multibulk_partial_header_returns_none() {
        assert!(matches!(parse_inline_or_multibulk(b"*3\r"), Ok(None)));
    }

    #[test]
    fn multibulk_partial_payload_returns_none() {
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nfo";
        assert!(matches!(parse_inline_or_multibulk(buf), Ok(None)));
    }

    #[test]
    fn multibulk_zero_args_consumes_header() {
        let (argv, n) = parse_inline_or_multibulk(b"*0\r\n").unwrap().unwrap();
        assert_eq!(n, 4);
        assert!(argv.is_empty());
    }

    #[test]
    fn multibulk_rejects_bad_bulk_length() {
        let err = parse_inline_or_multibulk(b"*1\r\n$-2\r\n").unwrap_err();
        assert!(matches!(err, RedisError::Runtime(_)));
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        networking.c::processInlineBuffer + processMultibulkBuffer
//                  sds.c::sdsnsplitargs_internal + sdsparsearg
//   target_crate:  redis-protocol
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Mirrors processInlineBuffer + processMultibulkBuffer.
//                  Full sdssplitargs port (double-quoted, single-quoted,
//                  escape sequences, adjacent-quote concatenation). Error
//                  messages match Valkey wire text byte-for-byte so the
//                  upstream Tcl protocol test suite passes.
// ──────────────────────────────────────────────────────────────────────────
