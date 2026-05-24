//! `RespFrame` — Rust enum representing one RESP2/RESP3 wire frame.
//!
//! Per PORTING.md §2 #2. RESP2 variants land now (Simple, Error,
//! Integer, Bulk, Array, Null). RESP3 variants stubbed; encoders /
//! decoders for them are todo!() until Phase 2 protocol translation
//! packets land.
//!
//! `Bulk(None)` represents a RESP2 null bulk string (`$-1\r\n`); the
//! dedicated `Null` variant is the RESP3 null (`_\r\n`).

use redis_types::RedisString;

/// Note: not `Eq` because `Double(f64)` can be NaN. Use `PartialEq` for
/// comparisons; tests should not put RespFrame in a HashSet or use it as
/// a HashMap key unless they exclude RESP3 Double frames.
#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    // ── RESP2 (Phase 2) ───────────────────────────────────────────
    /// `+OK\r\n` — simple string. Bytes excluding the leading `+` and trailing CRLF.
    Simple(RedisString),
    /// `-ERR ...\r\n` — error line. Bytes excluding the leading `-` and trailing CRLF.
    Error(RedisString),
    /// `:<n>\r\n` — integer.
    Integer(i64),
    /// `$<len>\r\n<bytes>\r\n` or `$-1\r\n` (None).
    Bulk(Option<RedisString>),
    /// `*<n>\r\n<frame>...` or `*-1\r\n` (None).
    Array(Option<Vec<RespFrame>>),

    // ── RESP3 (Phase 2 or later) ──────────────────────────────────
    /// `_\r\n` — RESP3 explicit null.
    Null,
    /// `#t\r\n` / `#f\r\n` — RESP3 boolean.
    Boolean(bool),
    /// `,<repr>\r\n` — RESP3 double.
    Double(f64),
    /// `(<digits>\r\n` — RESP3 big number.
    BigNumber(RedisString),
    /// `!<len>\r\n<msg>\r\n` — RESP3 bulk-style error.
    BulkError(RedisString),
    /// `=<len>\r\n<3chars>:<bytes>\r\n` — RESP3 verbatim string with format tag.
    VerbatimString { format: [u8; 3], data: RedisString },
    /// `%<n>\r\n<key>\r\n<value>\r\n...` — RESP3 map.
    Map(Vec<(RespFrame, RespFrame)>),
    /// `~<n>\r\n<frame>...` — RESP3 set.
    Set(Vec<RespFrame>),
    /// `|<n>\r\n<key>\r\n<value>\r\n...` — RESP3 attribute (out-of-band).
    Attribute(Vec<(RespFrame, RespFrame)>),
    /// `><n>\r\n<frame>...` — RESP3 push (server-initiated).
    Push(Vec<RespFrame>),
}

impl RespFrame {
    pub fn simple(s: impl Into<RedisString>) -> Self {
        RespFrame::Simple(s.into())
    }

    pub fn error(s: impl Into<RedisString>) -> Self {
        RespFrame::Error(s.into())
    }

    pub fn integer(n: i64) -> Self {
        RespFrame::Integer(n)
    }

    pub fn bulk(s: impl Into<RedisString>) -> Self {
        RespFrame::Bulk(Some(s.into()))
    }

    pub fn null_bulk() -> Self {
        RespFrame::Bulk(None)
    }

    pub fn array(items: Vec<RespFrame>) -> Self {
        RespFrame::Array(Some(items))
    }

    pub fn null_array() -> Self {
        RespFrame::Array(None)
    }
}

/// Encode a RespFrame onto the wire as RESP2 bytes.
///
/// RESP3-only variants are degraded to their nearest RESP2 equivalent
/// (Null → `$-1`, Boolean → `:1`/`:0`, Double → bulk string, BigNumber → bulk
/// string, BulkError → simple error, VerbatimString → bulk string, Map →
/// flat alternating array, Set → array, Push → array, Attribute → dropped).
/// This makes it safe for command handlers that build a single `RespFrame`
/// tree to be encoded under either protocol by selecting the right encoder.
pub fn encode_resp2(frame: &RespFrame, buf: &mut Vec<u8>) {
    use std::io::Write;
    match frame {
        RespFrame::Simple(s) => {
            buf.push(b'+');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::Error(s) => {
            buf.push(b'-');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::Integer(n) => {
            buf.push(b':');
            let _ = write!(buf, "{}", n);
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::Bulk(None) => {
            buf.extend_from_slice(b"$-1\r\n");
        }
        RespFrame::Bulk(Some(data)) => {
            buf.push(b'$');
            let _ = write!(buf, "{}", data.len());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(data.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::Array(None) => {
            buf.extend_from_slice(b"*-1\r\n");
        }
        RespFrame::Array(Some(items)) => {
            buf.push(b'*');
            let _ = write!(buf, "{}", items.len());
            buf.extend_from_slice(b"\r\n");
            for it in items {
                encode_resp2(it, buf);
            }
        }
        RespFrame::Null => {
            buf.extend_from_slice(b"$-1\r\n");
        }
        RespFrame::Boolean(b) => {
            buf.extend_from_slice(if *b { b":1\r\n" } else { b":0\r\n" });
        }
        RespFrame::Double(d) => {
            let formatted = format_double_text(*d);
            buf.push(b'$');
            let _ = write!(buf, "{}", formatted.len());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(&formatted);
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::BigNumber(s) => {
            buf.push(b'$');
            let _ = write!(buf, "{}", s.len());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::BulkError(s) => {
            buf.push(b'-');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::VerbatimString { data, .. } => {
            buf.push(b'$');
            let _ = write!(buf, "{}", data.len());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(data.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::Map(pairs) => {
            buf.push(b'*');
            let _ = write!(buf, "{}", pairs.len() * 2);
            buf.extend_from_slice(b"\r\n");
            for (k, v) in pairs {
                encode_resp2(k, buf);
                encode_resp2(v, buf);
            }
        }
        RespFrame::Set(items) => {
            buf.push(b'*');
            let _ = write!(buf, "{}", items.len());
            buf.extend_from_slice(b"\r\n");
            for it in items {
                encode_resp2(it, buf);
            }
        }
        RespFrame::Attribute(_) => {}
        RespFrame::Push(items) => {
            buf.push(b'*');
            let _ = write!(buf, "{}", items.len());
            buf.extend_from_slice(b"\r\n");
            for it in items {
                encode_resp2(it, buf);
            }
        }
    }
}

/// Encode a RespFrame onto the wire as RESP3 bytes.
///
/// All RESP2 frame shapes are still valid RESP3 (RESP3 is a superset). The
/// dedicated RESP3 frame variants emit their native wire form.
pub fn encode_resp3(frame: &RespFrame, buf: &mut Vec<u8>) {
    use std::io::Write;
    match frame {
        RespFrame::Simple(s) => {
            buf.push(b'+');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::Error(s) => {
            buf.push(b'-');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::Integer(n) => {
            buf.push(b':');
            let _ = write!(buf, "{}", n);
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::Bulk(None) => {
            buf.extend_from_slice(b"_\r\n");
        }
        RespFrame::Bulk(Some(data)) => {
            buf.push(b'$');
            let _ = write!(buf, "{}", data.len());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(data.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::Array(None) => {
            buf.extend_from_slice(b"_\r\n");
        }
        RespFrame::Array(Some(items)) => {
            buf.push(b'*');
            let _ = write!(buf, "{}", items.len());
            buf.extend_from_slice(b"\r\n");
            for it in items {
                encode_resp3(it, buf);
            }
        }
        RespFrame::Null => {
            buf.extend_from_slice(b"_\r\n");
        }
        RespFrame::Boolean(b) => {
            buf.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
        }
        RespFrame::Double(d) => {
            buf.push(b',');
            buf.extend_from_slice(&format_double_text(*d));
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::BigNumber(s) => {
            buf.push(b'(');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::BulkError(s) => {
            buf.push(b'!');
            let _ = write!(buf, "{}", s.len());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::VerbatimString { format, data } => {
            let total = 4 + data.len();
            buf.push(b'=');
            let _ = write!(buf, "{}", total);
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(format);
            buf.push(b':');
            buf.extend_from_slice(data.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespFrame::Map(pairs) => {
            buf.push(b'%');
            let _ = write!(buf, "{}", pairs.len());
            buf.extend_from_slice(b"\r\n");
            for (k, v) in pairs {
                encode_resp3(k, buf);
                encode_resp3(v, buf);
            }
        }
        RespFrame::Set(items) => {
            buf.push(b'~');
            let _ = write!(buf, "{}", items.len());
            buf.extend_from_slice(b"\r\n");
            for it in items {
                encode_resp3(it, buf);
            }
        }
        RespFrame::Attribute(pairs) => {
            buf.push(b'|');
            let _ = write!(buf, "{}", pairs.len());
            buf.extend_from_slice(b"\r\n");
            for (k, v) in pairs {
                encode_resp3(k, buf);
                encode_resp3(v, buf);
            }
        }
        RespFrame::Push(items) => {
            buf.push(b'>');
            let _ = write!(buf, "{}", items.len());
            buf.extend_from_slice(b"\r\n");
            for it in items {
                encode_resp3(it, buf);
            }
        }
    }
}

/// Format an `f64` for textual RESP wire emission.
///
/// `inf`/`-inf`/`nan` map to the lowercase Redis spellings, integer-valued
/// doubles within `i64` range render without a fractional part, and finite
/// non-integer doubles use Rust's default `{}` representation. Mirrors the
/// `format_score` helper in the zset command surface.
pub fn format_double_text(d: f64) -> Vec<u8> {
    if d.is_nan() {
        return b"nan".to_vec();
    }
    if d.is_infinite() {
        return if d > 0.0 {
            b"inf".to_vec()
        } else {
            b"-inf".to_vec()
        };
    }
    if d == 0.0 {
        return b"0".to_vec();
    }
    if d == d.trunc() && d.abs() < 1e17 {
        return format!("{}", d as i64).into_bytes();
    }
    format!("{}", d).into_bytes()
}

/// Encoder entry point that selects RESP2 or RESP3 based on the supplied
/// protocol version. `proto == 3` selects RESP3; any other value selects
/// RESP2.
pub fn encode_for_proto(frame: &RespFrame, proto: i32, buf: &mut Vec<u8>) {
    if proto == 3 {
        encode_resp3(frame, buf);
    } else {
        encode_resp2(frame, buf);
    }
}

/// Emit a RESP3 map header (`%N\r\n`).
pub fn encode_map_header(buf: &mut Vec<u8>, n_pairs: usize) {
    use std::io::Write;
    buf.push(b'%');
    let _ = write!(buf, "{}", n_pairs);
    buf.extend_from_slice(b"\r\n");
}

/// Emit a RESP3 set header (`~N\r\n`).
pub fn encode_set_header(buf: &mut Vec<u8>, n: usize) {
    use std::io::Write;
    buf.push(b'~');
    let _ = write!(buf, "{}", n);
    buf.extend_from_slice(b"\r\n");
}

/// Emit a RESP3 push header (`>N\r\n`).
pub fn encode_push_header(buf: &mut Vec<u8>, n: usize) {
    use std::io::Write;
    buf.push(b'>');
    let _ = write!(buf, "{}", n);
    buf.extend_from_slice(b"\r\n");
}

/// Emit a RESP3 null (`_\r\n`).
pub fn encode_null_resp3(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"_\r\n");
}

/// Emit a RESP3 boolean (`#t\r\n` / `#f\r\n`).
pub fn encode_boolean(buf: &mut Vec<u8>, b: bool) {
    buf.extend_from_slice(if b { b"#t\r\n" } else { b"#f\r\n" });
}

/// Emit a RESP3 double (`,<text>\r\n`).
pub fn encode_double(buf: &mut Vec<u8>, d: f64) {
    buf.push(b',');
    buf.extend_from_slice(&format_double_text(d));
    buf.extend_from_slice(b"\r\n");
}

/// Emit a RESP3 big number (`(<digits>\r\n`).
pub fn encode_big_number(buf: &mut Vec<u8>, digits: &[u8]) {
    buf.push(b'(');
    buf.extend_from_slice(digits);
    buf.extend_from_slice(b"\r\n");
}

/// Emit a RESP3 verbatim string. `format` is a 3-byte tag like `b"txt"` or
/// `b"mkd"`; `bytes` is the payload.
pub fn encode_verbatim_string(buf: &mut Vec<u8>, format: &[u8; 3], bytes: &[u8]) {
    use std::io::Write;
    let total = 4 + bytes.len();
    buf.push(b'=');
    let _ = write!(buf, "{}", total);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(format);
    buf.push(b':');
    buf.extend_from_slice(bytes);
    buf.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(frame: RespFrame) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_resp2(&frame, &mut buf);
        buf
    }

    #[test]
    fn simple_ok() {
        assert_eq!(enc(RespFrame::simple(b"OK".as_slice())), b"+OK\r\n");
    }

    #[test]
    fn error_line() {
        assert_eq!(
            enc(RespFrame::error(b"ERR foo".as_slice())),
            b"-ERR foo\r\n"
        );
    }

    #[test]
    fn integer_zero_and_negative() {
        assert_eq!(enc(RespFrame::integer(0)), b":0\r\n");
        assert_eq!(enc(RespFrame::integer(-42)), b":-42\r\n");
    }

    #[test]
    fn bulk_with_bytes() {
        assert_eq!(enc(RespFrame::bulk(b"hi".as_slice())), b"$2\r\nhi\r\n");
    }

    #[test]
    fn null_bulk_resp2() {
        assert_eq!(enc(RespFrame::null_bulk()), b"$-1\r\n");
    }

    #[test]
    fn empty_array() {
        assert_eq!(enc(RespFrame::array(vec![])), b"*0\r\n");
    }

    #[test]
    fn nested_array() {
        let f = RespFrame::array(vec![
            RespFrame::integer(1),
            RespFrame::bulk(b"x".as_slice()),
        ]);
        assert_eq!(enc(f), b"*2\r\n:1\r\n$1\r\nx\r\n");
    }

    #[test]
    fn bulk_round_trips_non_utf8() {
        let bytes = vec![0xff, 0x00, 0xfe];
        let f = RespFrame::bulk(bytes.clone());
        let out = enc(f);
        assert_eq!(out, [b"$3\r\n".as_slice(), &bytes[..], b"\r\n"].concat());
    }

    fn enc3(frame: RespFrame) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_resp3(&frame, &mut buf);
        buf
    }

    #[test]
    fn resp3_null() {
        assert_eq!(enc3(RespFrame::Null), b"_\r\n");
        assert_eq!(enc3(RespFrame::null_bulk()), b"_\r\n");
        assert_eq!(enc3(RespFrame::null_array()), b"_\r\n");
    }

    #[test]
    fn resp3_boolean() {
        assert_eq!(enc3(RespFrame::Boolean(true)), b"#t\r\n");
        assert_eq!(enc3(RespFrame::Boolean(false)), b"#f\r\n");
    }

    #[test]
    fn resp3_double_integer_form() {
        assert_eq!(enc3(RespFrame::Double(1.0)), b",1\r\n");
        assert_eq!(enc3(RespFrame::Double(-7.0)), b",-7\r\n");
    }

    #[test]
    fn resp3_double_fractional() {
        assert_eq!(enc3(RespFrame::Double(1.5)), b",1.5\r\n");
    }

    #[test]
    fn resp3_double_specials() {
        assert_eq!(enc3(RespFrame::Double(f64::INFINITY)), b",inf\r\n");
        assert_eq!(enc3(RespFrame::Double(f64::NEG_INFINITY)), b",-inf\r\n");
        assert_eq!(enc3(RespFrame::Double(f64::NAN)), b",nan\r\n");
    }

    #[test]
    fn resp3_big_number() {
        let f = RespFrame::BigNumber(RedisString::from_bytes(
            b"3492890328409238509324850943850943825024385",
        ));
        assert_eq!(
            enc3(f),
            b"(3492890328409238509324850943850943825024385\r\n".as_slice(),
        );
    }

    #[test]
    fn resp3_map() {
        let f = RespFrame::Map(vec![
            (RespFrame::bulk(b"first".as_slice()), RespFrame::Integer(1)),
            (RespFrame::bulk(b"second".as_slice()), RespFrame::Integer(2)),
        ]);
        assert_eq!(
            enc3(f),
            b"%2\r\n$5\r\nfirst\r\n:1\r\n$6\r\nsecond\r\n:2\r\n".as_slice(),
        );
    }

    #[test]
    fn resp3_set() {
        let f = RespFrame::Set(vec![RespFrame::Integer(1), RespFrame::Integer(2)]);
        assert_eq!(enc3(f), b"~2\r\n:1\r\n:2\r\n".as_slice());
    }

    #[test]
    fn resp3_verbatim_string() {
        let f = RespFrame::VerbatimString {
            format: *b"txt",
            data: RedisString::from_bytes(b"Some string"),
        };
        assert_eq!(enc3(f), b"=15\r\ntxt:Some string\r\n".as_slice());
    }

    #[test]
    fn resp3_push_frame() {
        let f = RespFrame::Push(vec![
            RespFrame::bulk(b"pubsub".as_slice()),
            RespFrame::bulk(b"message".as_slice()),
            RespFrame::bulk(b"ch".as_slice()),
            RespFrame::bulk(b"hi".as_slice()),
        ]);
        assert_eq!(
            enc3(f),
            b">4\r\n$6\r\npubsub\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n".as_slice(),
        );
    }

    #[test]
    fn resp3_map_falls_back_to_flat_array_under_resp2() {
        let f = RespFrame::Map(vec![
            (
                RespFrame::bulk(b"a".as_slice()),
                RespFrame::bulk(b"1".as_slice()),
            ),
            (
                RespFrame::bulk(b"b".as_slice()),
                RespFrame::bulk(b"2".as_slice()),
            ),
        ]);
        assert_eq!(
            enc(f),
            b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n".as_slice(),
        );
    }

    #[test]
    fn resp3_double_degrades_to_bulk_under_resp2() {
        assert_eq!(enc(RespFrame::Double(1.5)), b"$3\r\n1.5\r\n".as_slice());
    }

    #[test]
    fn resp3_null_degrades_to_null_bulk_under_resp2() {
        assert_eq!(enc(RespFrame::Null), b"$-1\r\n".as_slice());
    }

    #[test]
    fn resp3_boolean_degrades_to_integer_under_resp2() {
        assert_eq!(enc(RespFrame::Boolean(true)), b":1\r\n".as_slice());
        assert_eq!(enc(RespFrame::Boolean(false)), b":0\r\n".as_slice());
    }

    #[test]
    fn map_header_helper() {
        let mut buf = Vec::new();
        encode_map_header(&mut buf, 3);
        assert_eq!(buf, b"%3\r\n");
    }

    #[test]
    fn push_header_helper() {
        let mut buf = Vec::new();
        encode_push_header(&mut buf, 4);
        assert_eq!(buf, b">4\r\n");
    }

    #[test]
    fn encode_for_proto_switches_on_version() {
        let mut buf2 = Vec::new();
        let mut buf3 = Vec::new();
        encode_for_proto(&RespFrame::Boolean(true), 2, &mut buf2);
        encode_for_proto(&RespFrame::Boolean(true), 3, &mut buf3);
        assert_eq!(buf2, b":1\r\n");
        assert_eq!(buf3, b"#t\r\n");
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (PORTING.md §2 #2)
//   target_crate:  redis-protocol
//   confidence:    high
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         RESP2 encoder complete; RESP3 variants present, encoder is todo!() (translator packet).
// ──────────────────────────────────────────────────────────────────────────
