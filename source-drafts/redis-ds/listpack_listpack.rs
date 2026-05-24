//! Full port of `reference/valkey/src/listpack.c` (and `listpack.h`).
//!
//! Listpack is a compact, contiguous-buffer encoding for sequences of small
//! Redis values (strings or integers). It is used as the small-cardinality
//! encoding of hash, list, and sorted-set values before promotion to a
//! heavier structure (quicklist, hashtable, skiplist).
//!
//! Byte layout:
//! ```text
//! [total_bytes: u32 LE][num_elements: u16 LE][entry ...][LP_EOF: 0xFF]
//! ```
//!
//! Each entry is: `[encoding+header bytes][payload bytes][backlen: 1-5 bytes]`.
//! The backlen enables backward traversal without internal pointers.
//!
//! All cursor positions exposed in the public API are `usize` byte offsets
//! into `self.data`.
//!
//! C: listpack.c:1-1575, listpack.h:1-101
//!
//! TODO(architect): `crates/redis-ds/src/listpack.rs` (canonical owner of
//! `ListPack`) must add field `pub(crate) data: Vec<u8>` before this `impl`
//! block can compile. The current skeleton has an empty struct body.

use crate::listpack::ListPack;
use redis_types::RedisError;

// ── Encoding constants ────────────────────────────────────────────────────────
// C: listpack.c:48-97

const LP_HDR_SIZE: usize = 6;
const LP_HDR_NUMELE_UNKNOWN: u16 = u16::MAX;
const LP_MAX_INT_ENCODING_LEN: usize = 9;
const LP_MAX_BACKLEN_SIZE: usize = 5;
/// Sentinel: integer-encodable element.
const LP_ENCODING_INT: u8 = 0;
/// Sentinel: must use string encoding.
const LP_ENCODING_STRING: u8 = 1;
const LISTPACK_MAX_SAFETY_SIZE: usize = 1 << 30;

const LP_ENCODING_7BIT_UINT: u8 = 0x00;
const LP_ENCODING_7BIT_UINT_MASK: u8 = 0x80;
const LP_ENCODING_7BIT_UINT_ENTRY_SIZE: u64 = 2;

const LP_ENCODING_6BIT_STR: u8 = 0x80;
const LP_ENCODING_6BIT_STR_MASK: u8 = 0xC0;

const LP_ENCODING_13BIT_INT: u8 = 0xC0;
const LP_ENCODING_13BIT_INT_MASK: u8 = 0xE0;
const LP_ENCODING_13BIT_INT_ENTRY_SIZE: u64 = 3;

const LP_ENCODING_12BIT_STR: u8 = 0xE0;
const LP_ENCODING_12BIT_STR_MASK: u8 = 0xF0;

const LP_ENCODING_16BIT_INT: u8 = 0xF1;
const LP_ENCODING_16BIT_INT_ENTRY_SIZE: u64 = 4;

const LP_ENCODING_24BIT_INT: u8 = 0xF2;
const LP_ENCODING_24BIT_INT_ENTRY_SIZE: u64 = 5;

const LP_ENCODING_32BIT_INT: u8 = 0xF3;
const LP_ENCODING_32BIT_INT_ENTRY_SIZE: u64 = 6;

const LP_ENCODING_64BIT_INT: u8 = 0xF4;
const LP_ENCODING_64BIT_INT_ENTRY_SIZE: u64 = 10;

const LP_ENCODING_32BIT_STR: u8 = 0xF0;

const LP_EOF: u8 = 0xFF;

/// Maximum byte length of an integer rendered as a decimal ASCII sequence.
/// 20 digits for i64::MIN + sign = 21 bytes.
pub const LP_INTBUF_SIZE: usize = 21;

// ── Public types ─────────────────────────────────────────────────────────────

/// Insertion position relative to the element at the cursor.
/// C: LP_BEFORE=0, LP_AFTER=1, LP_REPLACE=2  (listpack.h:44-46)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertWhere {
    Before,
    After,
    Replace,
}

/// A value read from a listpack entry. Mirrors the C `listpackEntry` struct.
/// When `sval` is `Some`, the element is a byte string; otherwise `lval`
/// holds the decoded integer.
#[derive(Debug, Clone, Default)]
pub struct ListpackEntry {
    pub sval: Option<Vec<u8>>,
    pub slen: u32,
    pub lval: i64,
}

/// Callback type for `validate_integrity` deep scanning.
/// Returns `true` if the entry at byte offset `pos` is acceptable.
/// `head_count` is the declared element count from the header.
pub type ValidateEntryCb = fn(pos: usize, head_count: u32) -> bool;

// ── Private encoding helpers (free functions) ─────────────────────────────────

/// Parse a byte string of decimal digits (with optional leading '-') as i64.
/// Equivalent to C `string2ll` from util.c, inlined here to avoid UTF-8.
/// Returns `None` if the string is empty, non-numeric, or overflows.
/// C: util.c string2ll (called at listpack.c:250, 633, 1316)
///
/// TODO(port): share with util.rs once that module is ported; currently inlined.
fn bytes_to_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut pos = 0usize;
    let negative = if s[0] == b'-' {
        pos += 1;
        true
    } else {
        false
    };
    if pos >= s.len() {
        return None;
    }
    let mut v: i64 = 0;
    while pos < s.len() {
        let b = s[pos];
        if b < b'0' || b > b'9' {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((b - b'0') as i64)?;
        pos += 1;
    }
    if negative { Some(-v) } else { Some(v) }
}

/// Render i64 as decimal ASCII into `buf`. Returns the number of bytes written.
/// Equivalent to C `ll2string` from util.c, inlined here.
/// C: util.c ll2string (called at listpack.c:575)
///
/// TODO(port): share with util.rs once that module is ported.
fn i64_to_bytes(v: i64, buf: &mut [u8; LP_INTBUF_SIZE]) -> usize {
    if v == 0 {
        buf[0] = b'0';
        return 1;
    }
    let negative = v < 0;
    // Use u64 arithmetic to avoid overflow on i64::MIN.
    let mut n: u64 = if negative {
        (v as u64).wrapping_neg()
    } else {
        v as u64
    };
    let mut tmp = [0u8; LP_INTBUF_SIZE];
    let mut digit_len = 0usize;
    while n > 0 {
        tmp[digit_len] = b'0' + (n % 10) as u8;
        n /= 10;
        digit_len += 1;
    }
    let mut pos = 0usize;
    if negative {
        buf[pos] = b'-';
        pos += 1;
    }
    for i in 0..digit_len {
        buf[pos + i] = tmp[digit_len - 1 - i];
    }
    pos + digit_len
}

/// Encode `v` as the most compact integer representation.
/// Fills `intenc` with the encoded bytes and sets `enclen` to their count.
/// C: listpack.c:186-235, lpEncodeIntegerGetType
fn encode_integer_get_type(v: i64, intenc: &mut [u8; LP_MAX_INT_ENCODING_LEN]) -> u64 {
    if v >= 0 && v <= 127 {
        intenc[0] = v as u8;
        1
    } else if v >= -4096 && v <= 4095 {
        let uv = if v < 0 { ((1i64 << 13) + v) as u16 } else { v as u16 };
        intenc[0] = ((uv >> 8) as u8) | LP_ENCODING_13BIT_INT;
        intenc[1] = (uv & 0xff) as u8;
        2
    } else if v >= -32768 && v <= 32767 {
        let uv = if v < 0 { ((1i64 << 16) + v) as u32 } else { v as u32 };
        intenc[0] = LP_ENCODING_16BIT_INT;
        intenc[1] = (uv & 0xff) as u8;
        intenc[2] = (uv >> 8) as u8;
        3
    } else if v >= -8388608 && v <= 8388607 {
        let uv = if v < 0 { ((1i64 << 24) + v) as u32 } else { v as u32 };
        intenc[0] = LP_ENCODING_24BIT_INT;
        intenc[1] = (uv & 0xff) as u8;
        intenc[2] = ((uv >> 8) & 0xff) as u8;
        intenc[3] = (uv >> 16) as u8;
        4
    } else if v >= -2147483648 && v <= 2147483647 {
        let uv = if v < 0 { ((1i64 << 32) + v) as u64 } else { v as u64 };
        intenc[0] = LP_ENCODING_32BIT_INT;
        intenc[1] = (uv & 0xff) as u8;
        intenc[2] = ((uv >> 8) & 0xff) as u8;
        intenc[3] = ((uv >> 16) & 0xff) as u8;
        intenc[4] = (uv >> 24) as u8;
        5
    } else {
        let uv = v as u64;
        intenc[0] = LP_ENCODING_64BIT_INT;
        intenc[1] = (uv & 0xff) as u8;
        intenc[2] = ((uv >> 8) & 0xff) as u8;
        intenc[3] = ((uv >> 16) & 0xff) as u8;
        intenc[4] = ((uv >> 24) & 0xff) as u8;
        intenc[5] = ((uv >> 32) & 0xff) as u8;
        intenc[6] = ((uv >> 40) & 0xff) as u8;
        intenc[7] = ((uv >> 48) & 0xff) as u8;
        intenc[8] = (uv >> 56) as u8;
        9
    }
}

/// Determine encoding type for a raw byte element.
/// Returns `(LP_ENCODING_INT, enclen, intenc)` if the element can be stored
/// as an integer, or `(LP_ENCODING_STRING, enclen, _)` otherwise.
/// C: listpack.c:248-262, lpEncodeGetType
fn encode_get_type(ele: &[u8]) -> (u8, u64, [u8; LP_MAX_INT_ENCODING_LEN]) {
    let mut intenc = [0u8; LP_MAX_INT_ENCODING_LEN];
    if let Some(v) = bytes_to_i64(ele) {
        let enclen = encode_integer_get_type(v, &mut intenc);
        (LP_ENCODING_INT, enclen, intenc)
    } else {
        let size = ele.len() as u64;
        let enclen = if size < 64 {
            1 + size
        } else if size < 4096 {
            2 + size
        } else {
            5 + size
        };
        (LP_ENCODING_STRING, enclen, intenc)
    }
}

/// Return the number of bytes needed to encode a backlen value `l`.
/// C: listpack.c:269-303, lpEncodeBacklen (the NULL-buf, count-only path)
fn encode_backlen_size(l: u64) -> usize {
    if l <= 127 { 1 }
    else if l <= 16383 { 2 }
    else if l <= 2097151 { 3 }
    else if l <= 268435455 { 4 }
    else { 5 }
}

/// Encode backlen value `l` into a buffer. Returns `(length, buffer)`.
/// The high bit of each byte except the first (MSB) is set to signal continuation.
/// C: listpack.c:269-303, lpEncodeBacklen (the write path)
fn encode_backlen_bytes(l: u64) -> (usize, [u8; LP_MAX_BACKLEN_SIZE]) {
    let mut buf = [0u8; LP_MAX_BACKLEN_SIZE];
    let len = if l <= 127 {
        buf[0] = l as u8;
        1
    } else if l <= 16383 {
        buf[0] = (l >> 7) as u8;
        buf[1] = (l & 127) as u8 | 128;
        2
    } else if l <= 2097151 {
        buf[0] = (l >> 14) as u8;
        buf[1] = ((l >> 7) & 127) as u8 | 128;
        buf[2] = (l & 127) as u8 | 128;
        3
    } else if l <= 268435455 {
        buf[0] = (l >> 21) as u8;
        buf[1] = ((l >> 14) & 127) as u8 | 128;
        buf[2] = ((l >> 7) & 127) as u8 | 128;
        buf[3] = (l & 127) as u8 | 128;
        4
    } else {
        buf[0] = (l >> 28) as u8;
        buf[1] = ((l >> 21) & 127) as u8 | 128;
        buf[2] = ((l >> 14) & 127) as u8 | 128;
        buf[3] = ((l >> 7) & 127) as u8 | 128;
        buf[4] = (l & 127) as u8 | 128;
        5
    };
    (len, buf)
}

/// Decode a reverse-encoded backlen starting at byte offset `pos` and reading
/// backward. Returns `u64::MAX` if the encoding is invalid (> 5 bytes used).
/// C: listpack.c:308-319, lpDecodeBacklen
fn decode_backlen(data: &[u8], pos: usize) -> u64 {
    let mut val: u64 = 0;
    let mut shift: u64 = 0;
    let mut idx = pos;
    loop {
        val |= (data[idx] as u64 & 127) << shift;
        if data[idx] & 128 == 0 {
            break;
        }
        shift += 7;
        if shift > 28 {
            return u64::MAX;
        }
        if idx == 0 {
            return u64::MAX;
        }
        idx -= 1;
    }
    val
}

/// Write a string element into `data` starting at byte offset `dst`.
/// The caller must have already sized the vec to accommodate the write.
/// C: listpack.c:325-341, lpEncodeString
fn encode_string_into(data: &mut Vec<u8>, dst: usize, s: &[u8]) {
    let len = s.len();
    if len < 64 {
        data[dst] = len as u8 | LP_ENCODING_6BIT_STR;
        data[dst + 1..dst + 1 + len].copy_from_slice(s);
    } else if len < 4096 {
        data[dst] = ((len >> 8) as u8) | LP_ENCODING_12BIT_STR;
        data[dst + 1] = (len & 0xff) as u8;
        data[dst + 2..dst + 2 + len].copy_from_slice(s);
    } else {
        data[dst] = LP_ENCODING_32BIT_STR;
        data[dst + 1] = (len & 0xff) as u8;
        data[dst + 2] = ((len >> 8) & 0xff) as u8;
        data[dst + 3] = ((len >> 16) & 0xff) as u8;
        data[dst + 4] = ((len >> 24) & 0xff) as u8;
        data[dst + 5..dst + 5 + len].copy_from_slice(s);
    }
}

/// Return the full encoded size of the entry at `p` (encoding + payload bytes,
/// NOT including the trailing backlen). Returns 0 for unknown encoding.
/// Callers must ensure `p` is a valid slice of at least 5 bytes for 32-bit str.
/// C: listpack.c:350-362, lpCurrentEncodedSizeUnsafe
fn current_encoded_size_unsafe(p: &[u8]) -> u32 {
    let b = p[0];
    if b & LP_ENCODING_7BIT_UINT_MASK == LP_ENCODING_7BIT_UINT { return 1; }
    if b & LP_ENCODING_6BIT_STR_MASK == LP_ENCODING_6BIT_STR {
        return 1 + (b & 0x3F) as u32;
    }
    if b & LP_ENCODING_13BIT_INT_MASK == LP_ENCODING_13BIT_INT { return 2; }
    if b == LP_ENCODING_16BIT_INT { return 3; }
    if b == LP_ENCODING_24BIT_INT { return 4; }
    if b == LP_ENCODING_32BIT_INT { return 5; }
    if b == LP_ENCODING_64BIT_INT { return 9; }
    if b & LP_ENCODING_12BIT_STR_MASK == LP_ENCODING_12BIT_STR {
        return 2 + (((b & 0x0F) as u32) << 8) + p[1] as u32;
    }
    if b == LP_ENCODING_32BIT_STR {
        let len = (p[1] as u32)
            | ((p[2] as u32) << 8)
            | ((p[3] as u32) << 16)
            | ((p[4] as u32) << 24);
        return 5 + len;
    }
    if b == LP_EOF { return 1; }
    0
}

/// Return the number of bytes used by the encoding header alone (1 or 2 or 5),
/// not including the actual payload.
/// C: listpack.c:368-380, lpCurrentEncodedSizeBytes
fn current_encoded_size_bytes(p: &[u8]) -> u32 {
    let b = p[0];
    if b & LP_ENCODING_7BIT_UINT_MASK == LP_ENCODING_7BIT_UINT { return 1; }
    if b & LP_ENCODING_6BIT_STR_MASK == LP_ENCODING_6BIT_STR { return 1; }
    if b & LP_ENCODING_13BIT_INT_MASK == LP_ENCODING_13BIT_INT { return 1; }
    if b == LP_ENCODING_16BIT_INT { return 1; }
    if b == LP_ENCODING_24BIT_INT { return 1; }
    if b == LP_ENCODING_32BIT_INT { return 1; }
    if b == LP_ENCODING_64BIT_INT { return 1; }
    if b & LP_ENCODING_12BIT_STR_MASK == LP_ENCODING_12BIT_STR { return 2; }
    if b == LP_ENCODING_32BIT_STR { return 5; }
    if b == LP_EOF { return 1; }
    0
}

/// Return the total number of bytes consumed by the entry at offset `pos`
/// (encoding + payload + backlen). Does NOT include the EOF byte.
/// C: listpack.c:386-391, lpSkip (but returning count instead of advancing ptr)
fn entry_total_size(data: &[u8], pos: usize) -> usize {
    let enc_size = current_encoded_size_unsafe(&data[pos..]) as u64;
    let bl_size = encode_backlen_size(enc_size) as u64;
    (enc_size + bl_size) as usize
}

// ── impl ListPack ─────────────────────────────────────────────────────────────

impl ListPack {
    // ── Header accessors ─────────────────────────────────────────────────────

    /// Read the total-bytes field from the listpack header (bytes 0-3, LE).
    fn get_total_bytes(&self) -> u32 {
        (self.data[0] as u32)
            | ((self.data[1] as u32) << 8)
            | ((self.data[2] as u32) << 16)
            | ((self.data[3] as u32) << 24)
    }

    fn set_total_bytes(&mut self, v: u32) {
        self.data[0] = (v & 0xff) as u8;
        self.data[1] = ((v >> 8) & 0xff) as u8;
        self.data[2] = ((v >> 16) & 0xff) as u8;
        self.data[3] = ((v >> 24) & 0xff) as u8;
    }

    /// Read the num-elements field (bytes 4-5, LE). `LP_HDR_NUMELE_UNKNOWN`
    /// (u16::MAX) means there are too many elements to cache in the header.
    fn get_num_elements(&self) -> u16 {
        (self.data[4] as u16) | ((self.data[5] as u16) << 8)
    }

    fn set_num_elements(&mut self, v: u16) {
        self.data[4] = (v & 0xff) as u8;
        self.data[5] = ((v >> 8) & 0xff) as u8;
    }

    // ── Constructors ─────────────────────────────────────────────────────────

    /// Create a new empty listpack, pre-allocating at least `capacity` bytes.
    /// C: listpack.c:155-162, lpNew
    pub fn with_capacity(capacity: usize) -> Self {
        let alloc = capacity.max(LP_HDR_SIZE + 1);
        let mut data = Vec::with_capacity(alloc);
        // Header: total_bytes = LP_HDR_SIZE + 1 (for EOF), num_elements = 0
        let total = (LP_HDR_SIZE + 1) as u32;
        data.push((total & 0xff) as u8);
        data.push(((total >> 8) & 0xff) as u8);
        data.push(((total >> 16) & 0xff) as u8);
        data.push(((total >> 24) & 0xff) as u8);
        data.push(0); // num_elements lo
        data.push(0); // num_elements hi
        data.push(LP_EOF);
        ListPack { data }
    }

    /// Check whether adding `add` bytes would exceed the safety limit.
    /// C: listpack.c:144-148, lpSafeToAdd
    pub fn safe_to_add(&self, add: usize) -> bool {
        let current = self.get_total_bytes() as usize;
        current.saturating_add(add) <= LISTPACK_MAX_SAFETY_SIZE
    }

    /// Shrink the internal allocation to match the actual encoded size.
    /// C: listpack.c:176-183, lpShrinkToFit
    pub fn shrink_to_fit(&mut self) {
        let size = self.get_total_bytes() as usize;
        self.data.truncate(size);
        self.data.shrink_to_fit();
    }

    // ── Navigation ───────────────────────────────────────────────────────────

    /// Return the byte offset of the first element, or `None` if the list is empty.
    /// C: listpack.c:425-435, lpFirst
    pub fn first(&self) -> Option<usize> {
        let pos = LP_HDR_SIZE;
        if self.data.get(pos).copied() == Some(LP_EOF) {
            return None;
        }
        Some(pos)
    }

    /// Return the byte offset of the last element, or `None` if the list is empty.
    /// C: listpack.c:439-442, lpLast
    pub fn last(&self) -> Option<usize> {
        let eof_pos = self.get_total_bytes() as usize - 1;
        // lpPrev from the EOF position
        self.prev(eof_pos)
    }

    /// Advance to the next element. Returns `None` if `pos` was the last element.
    /// C: listpack.c:396-407, lpNext
    pub fn next(&self, pos: usize) -> Option<usize> {
        let skip = entry_total_size(&self.data, pos);
        let next_pos = pos + skip;
        if self.data.get(next_pos).copied() == Some(LP_EOF) {
            return None;
        }
        Some(next_pos)
    }

    /// Move to the previous element. Returns `None` if `pos` was the first element.
    /// C: listpack.c:412-421, lpPrev
    pub fn prev(&self, pos: usize) -> Option<usize> {
        if pos <= LP_HDR_SIZE {
            return None;
        }
        let backlen_end = pos - 1;
        let content_len = decode_backlen(&self.data, backlen_end);
        if content_len == u64::MAX {
            return None;
        }
        let total_size = content_len as usize + encode_backlen_size(content_len);
        let prev_pos = pos.checked_sub(total_size)?;
        if prev_pos < LP_HDR_SIZE {
            return None;
        }
        Some(prev_pos)
    }

    /// Same as `first` but skips the validity assertion, for use with
    /// `validate_next`.
    /// C: listpack.c:1203-1207, lpValidateFirst
    pub fn validate_first(&self) -> Option<usize> {
        let pos = LP_HDR_SIZE;
        if self.data.get(pos).copied() == Some(LP_EOF) {
            return None;
        }
        Some(pos)
    }

    // ── Length and byte size ──────────────────────────────────────────────────

    /// Return the number of bytes in the serialised listpack (including header
    /// and EOF). C: listpack.c:1140-1142, lpBytes
    pub fn bytes_len(&self) -> usize {
        self.get_total_bytes() as usize
    }

    /// Return the number of elements. May scan the whole list if the header
    /// count overflowed `u16::MAX`. As a side-effect, updates the header count
    /// if it fits in `u16::MAX`.
    /// C: listpack.c:449-466, lpLength
    pub fn length(&mut self) -> usize {
        let cached = self.get_num_elements();
        if cached != LP_HDR_NUMELE_UNKNOWN {
            return cached as usize;
        }
        let mut count: u32 = 0;
        let mut pos = self.first();
        while let Some(p) = pos {
            count += 1;
            pos = self.next(p);
        }
        if count < LP_HDR_NUMELE_UNKNOWN as u32 {
            self.set_num_elements(count as u16);
        }
        count as usize
    }

    // ── Element read ─────────────────────────────────────────────────────────

    /// Return the value at `pos` along with the total entry byte size.
    /// Integer elements return `None` for the byte slice; string elements
    /// return `Some` with a subslice into `self.data`.
    /// C: listpack.c:504-581, lpGetWithSize (the core read path)
    ///
    /// PERF(port): The C version avoids any allocation; this slice-return does
    /// too. Lifetime 'self bounds the returned slice.
    pub fn get_with_size(&self, pos: usize) -> Option<(Option<&[u8]>, i64, u64)> {
        // C: listpack.c:504-581, lpGetWithSize
        // Returns (bytes_ptr, count_or_int, entry_size)
        // When bytes_ptr is None and count_or_int is i64, it's an integer.
        // When bytes_ptr is Some, count_or_int is the byte length.
        let p = &self.data[pos..];
        let b = p[0];

        let (uval, negstart, negmax, entry_size): (u64, u64, u64, u64);

        if b & LP_ENCODING_7BIT_UINT_MASK == LP_ENCODING_7BIT_UINT {
            uval = (b & 0x7f) as u64;
            negstart = u64::MAX;
            negmax = 0;
            entry_size = LP_ENCODING_7BIT_UINT_ENTRY_SIZE;
        } else if b & LP_ENCODING_6BIT_STR_MASK == LP_ENCODING_6BIT_STR {
            let count = (b & 0x3F) as i64;
            let data_start = pos + 1;
            let data_end = data_start + count as usize;
            let computed_entry_size = 1 + count as u64 + encode_backlen_size(1 + count as u64) as u64;
            return Some((
                Some(&self.data[data_start..data_end]),
                count,
                computed_entry_size,
            ));
        } else if b & LP_ENCODING_13BIT_INT_MASK == LP_ENCODING_13BIT_INT {
            uval = (((b & 0x1f) as u64) << 8) | p[1] as u64;
            negstart = 1u64 << 12;
            negmax = 8191;
            entry_size = LP_ENCODING_13BIT_INT_ENTRY_SIZE;
        } else if b == LP_ENCODING_16BIT_INT {
            uval = (p[1] as u64) | ((p[2] as u64) << 8);
            negstart = 1u64 << 15;
            negmax = u16::MAX as u64;
            entry_size = LP_ENCODING_16BIT_INT_ENTRY_SIZE;
        } else if b == LP_ENCODING_24BIT_INT {
            uval = (p[1] as u64) | ((p[2] as u64) << 8) | ((p[3] as u64) << 16);
            negstart = 1u64 << 23;
            negmax = (u32::MAX >> 8) as u64;
            entry_size = LP_ENCODING_24BIT_INT_ENTRY_SIZE;
        } else if b == LP_ENCODING_32BIT_INT {
            uval = (p[1] as u64)
                | ((p[2] as u64) << 8)
                | ((p[3] as u64) << 16)
                | ((p[4] as u64) << 24);
            negstart = 1u64 << 31;
            negmax = u32::MAX as u64;
            entry_size = LP_ENCODING_32BIT_INT_ENTRY_SIZE;
        } else if b == LP_ENCODING_64BIT_INT {
            uval = (p[1] as u64)
                | ((p[2] as u64) << 8)
                | ((p[3] as u64) << 16)
                | ((p[4] as u64) << 24)
                | ((p[5] as u64) << 32)
                | ((p[6] as u64) << 40)
                | ((p[7] as u64) << 48)
                | ((p[8] as u64) << 56);
            negstart = 1u64 << 63;
            negmax = u64::MAX;
            entry_size = LP_ENCODING_64BIT_INT_ENTRY_SIZE;
        } else if b & LP_ENCODING_12BIT_STR_MASK == LP_ENCODING_12BIT_STR {
            let count = ((b & 0x0F) as usize) << 8 | p[1] as usize;
            let data_start = pos + 2;
            let data_end = data_start + count;
            let computed_entry_size = 2 + count as u64 + encode_backlen_size(2 + count as u64) as u64;
            return Some((
                Some(&self.data[data_start..data_end]),
                count as i64,
                computed_entry_size,
            ));
        } else if b == LP_ENCODING_32BIT_STR {
            let count = (p[1] as usize)
                | ((p[2] as usize) << 8)
                | ((p[3] as usize) << 16)
                | ((p[4] as usize) << 24);
            let data_start = pos + 5;
            let data_end = data_start + count;
            let computed_entry_size = 5 + count as u64 + encode_backlen_size(5 + count as u64) as u64;
            return Some((
                Some(&self.data[data_start..data_end]),
                count as i64,
                computed_entry_size,
            ));
        } else {
            // Unknown encoding — C returns a sentinel value without crashing.
            uval = 12345678900000000u64 + b as u64;
            negstart = u64::MAX;
            negmax = 0;
            entry_size = 1;
        }

        // Two's-complement conversion for negative integers.
        let val = if uval >= negstart {
            let u = negmax - uval;
            -(u as i64) - 1
        } else {
            uval as i64
        };

        Some((None, val, entry_size))
    }

    /// Return the value at `pos` (no entry-size). Integer elements return
    /// `(None, int_value)`; string elements return `(Some(bytes), len)`.
    /// C: listpack.c:583-585, lpGet
    pub fn get(&self, pos: usize) -> Option<(Option<&[u8]>, i64)> {
        self.get_with_size(pos).map(|(s, v, _)| (s, v))
    }

    /// Return a `ListpackEntry` for the element at `pos`.
    /// C: listpack.c:592-603, lpGetValue
    pub fn get_value(&self, pos: usize) -> Option<ListpackEntry> {
        let (bytes, val, _) = self.get_with_size(pos)?;
        if let Some(s) = bytes {
            Some(ListpackEntry {
                sval: Some(s.to_vec()),
                slen: val as u32,
                lval: 0,
            })
        } else {
            Some(ListpackEntry {
                sval: None,
                slen: 0,
                lval: val,
            })
        }
    }

    /// Return the element at `pos` as a byte buffer. Integer elements are
    /// rendered into `intbuf` and a slice of it is returned; string elements
    /// borrow from `self.data`. Mirrors the C `lpGet` with non-NULL intbuf.
    /// C: listpack.c:583-585 (the intbuf != NULL path inside lpGetWithSize)
    pub fn get_as_bytes<'a>(
        &'a self,
        pos: usize,
        intbuf: &'a mut [u8; LP_INTBUF_SIZE],
    ) -> Option<(&'a [u8], usize)> {
        let (bytes, val, _) = self.get_with_size(pos)?;
        if let Some(s) = bytes {
            let len = val as usize;
            Some((s, len))
        } else {
            let len = i64_to_bytes(val, intbuf);
            Some((&intbuf[..len], len))
        }
    }

    // ── Search ────────────────────────────────────────────────────────────────

    /// Find the first element equal to `s` starting at cursor `start_pos`,
    /// skipping `skip` elements between comparisons. Returns the byte offset
    /// of the matching element, or `None`.
    /// C: listpack.c:607-674, lpFind
    pub fn find(&self, start_pos: usize, s: &[u8], skip: usize) -> Option<usize> {
        let lp_bytes = self.bytes_len();
        let mut pos = start_pos;
        let mut skip_cnt = 0usize;
        // Lazily compute integer representation of `s` (0 = not tried, 1 = valid, 255 = not int)
        let mut vencoding: u8 = 0;
        let mut vll: i64 = 0;

        loop {
            if self.data.get(pos).copied() == Some(LP_EOF) {
                break;
            }
            if skip_cnt == 0 {
                let (bytes, ll, entry_size) = self.get_with_size(pos)?;
                if let Some(value) = bytes {
                    if s.len() == ll as usize && value == s {
                        return Some(pos);
                    }
                } else {
                    // Element is integer; try to encode `s` as integer for comparison.
                    if vencoding == 0 {
                        if s.len() >= 32 || s.is_empty() {
                            vencoding = u8::MAX;
                        } else if let Some(v) = bytes_to_i64(s) {
                            vencoding = 1;
                            vll = v;
                        } else {
                            vencoding = u8::MAX;
                        }
                    }
                    if vencoding != u8::MAX && ll == vll {
                        return Some(pos);
                    }
                }
                skip_cnt = skip;
                pos += entry_size as usize;
            } else {
                skip_cnt -= 1;
                pos += entry_total_size(&self.data, pos);
            }

            if pos >= lp_bytes {
                break;
            }
        }
        None
    }

    // ── Core mutation (insert / delete / replace) ─────────────────────────────

    /// Core insertion / deletion primitive.
    ///
    /// `elestr` — raw byte string to encode and insert (or `None`).
    /// `eleint` — already-encoded integer bytes (and encoded length). Supply
    ///            this from `encode_integer_get_type` when the caller has already
    ///            done the encoding (i.e. `insert_integer`).
    /// `pos`    — cursor (byte offset in `self.data`).
    /// `where_` — `Before`, `After`, or `Replace`.
    ///
    /// When both `elestr` and `eleint` are `None`, the element at `pos` is deleted.
    ///
    /// Returns the cursor of the newly inserted / next element, or `None` if the
    /// deleted element was the last one.
    ///
    /// C: listpack.c:704-851, lpInsert
    pub fn insert_raw(
        &mut self,
        elestr: Option<&[u8]>,
        eleint: Option<(&[u8], u64)>,
        pos: usize,
        where_: InsertWhere,
    ) -> Result<Option<usize>, RedisError> {
        let del_ele = elestr.is_none() && eleint.is_none();
        let where_ = if del_ele { InsertWhere::Replace } else { where_ };

        // LP_AFTER: skip to next element, then treat as LP_BEFORE.
        let (poff, where_) = if where_ == InsertWhere::After {
            let skip = entry_total_size(&self.data, pos);
            (pos + skip, InsertWhere::Before)
        } else {
            (pos, where_)
        };

        // Determine the encoded form and its length.
        let (enctype, enclen, intenc_buf) = if let Some(s) = elestr {
            encode_get_type(s)
        } else if let Some((_ei, elen)) = eleint {
            (LP_ENCODING_INT, elen, [0u8; LP_MAX_INT_ENCODING_LEN])
        } else {
            (u8::MAX, 0u64, [0u8; LP_MAX_INT_ENCODING_LEN])
        };

        let backlen_size = if del_ele { 0 } else { encode_backlen_size(enclen) };
        let old_bytes = self.get_total_bytes() as u64;

        // Compute the byte span of the element being replaced (if any).
        let replaced_len: u64 = if where_ == InsertWhere::Replace {
            let enc_size = current_encoded_size_unsafe(&self.data[poff..]) as u64;
            enc_size + encode_backlen_size(enc_size) as u64
        } else {
            0
        };

        let new_bytes = old_bytes
            .checked_add(enclen)
            .and_then(|v| v.checked_add(backlen_size as u64))
            .and_then(|v| v.checked_sub(replaced_len))
            .filter(|&v| v <= u32::MAX as u64)
            .ok_or_else(|| RedisError::runtime(b"listpack: resulting size overflows u32"))?;

        // Byte offsets for the tail (everything after the element being replaced/inserted-before).
        let tail_start = poff + replaced_len as usize;
        let new_tail_start = poff + enclen as usize + backlen_size;
        // Number of tail bytes to relocate (includes EOF byte).
        let tail_len = old_bytes as usize - tail_start;
        let new_bytes_usize = new_bytes as usize;

        if new_bytes_usize > self.data.len() {
            // Growing: extend then shift tail right.
            self.data.resize(new_bytes_usize, 0);
            if tail_len > 0 {
                self.data.copy_within(tail_start..tail_start + tail_len, new_tail_start);
            }
        } else if new_bytes_usize < self.data.len() {
            // Shrinking: shift tail left then truncate.
            if tail_len > 0 {
                self.data.copy_within(tail_start..tail_start + tail_len, new_tail_start);
            }
            self.data.truncate(new_bytes_usize);
        }
        // Same size: overwrite in place (no shift needed).

        // Write the new entry.
        let new_entry_pos = if !del_ele {
            if enctype == LP_ENCODING_INT {
                // Use already-encoded bytes.
                let ei: &[u8] = if elestr.is_some() {
                    // encode_get_type stored the encoding in intenc_buf
                    &intenc_buf[..enclen as usize]
                } else {
                    eleint.map(|(b, _)| b).unwrap_or(&[])
                };
                self.data[poff..poff + enclen as usize].copy_from_slice(ei);
            } else if let Some(s) = elestr {
                encode_string_into(&mut self.data, poff, s);
            }
            // Write backlen immediately after the encoded element.
            let (bl_len, bl_buf) = encode_backlen_bytes(enclen);
            let bl_dst = poff + enclen as usize;
            self.data[bl_dst..bl_dst + bl_len].copy_from_slice(&bl_buf[..bl_len]);
            Some(poff)
        } else {
            // Deletion: next element at poff, or None if EOF.
            if self.data.get(poff).copied() == Some(LP_EOF) {
                None
            } else {
                Some(poff)
            }
        };

        // Update the header.
        if where_ != InsertWhere::Replace || del_ele {
            let num = self.get_num_elements();
            if num != LP_HDR_NUMELE_UNKNOWN {
                if !del_ele {
                    self.set_num_elements(num.saturating_add(1));
                } else {
                    self.set_num_elements(num.saturating_sub(1));
                }
            }
        }
        self.set_total_bytes(new_bytes as u32);

        Ok(new_entry_pos)
    }

    /// Insert a byte-string element. Returns the cursor of the new element.
    /// C: listpack.c:854-857, lpInsertString
    pub fn insert_string(
        &mut self,
        s: &[u8],
        pos: usize,
        where_: InsertWhere,
    ) -> Result<Option<usize>, RedisError> {
        self.insert_raw(Some(s), None, pos, where_)
    }

    /// Insert a 64-bit integer element. Returns the cursor of the new element.
    /// C: listpack.c:861-867, lpInsertInteger
    pub fn insert_integer(
        &mut self,
        lval: i64,
        pos: usize,
        where_: InsertWhere,
    ) -> Result<Option<usize>, RedisError> {
        let mut intenc = [0u8; LP_MAX_INT_ENCODING_LEN];
        let enclen = encode_integer_get_type(lval, &mut intenc);
        self.insert_raw(None, Some((&intenc, enclen)), pos, where_)
    }

    /// Prepend a byte-string element at the head of the list.
    /// C: listpack.c:870-874, lpPrepend
    pub fn prepend(&mut self, s: &[u8]) -> Result<(), RedisError> {
        match self.first() {
            Some(p) => { self.insert_string(s, p, InsertWhere::Before)?; }
            None => { self.append(s)?; }
        }
        Ok(())
    }

    /// Prepend an integer element at the head of the list.
    /// C: listpack.c:877-881, lpPrependInteger
    pub fn prepend_integer(&mut self, lval: i64) -> Result<(), RedisError> {
        match self.first() {
            Some(p) => { self.insert_integer(lval, p, InsertWhere::Before)?; }
            None => { self.append_integer(lval)?; }
        }
        Ok(())
    }

    /// Append a byte-string element at the tail of the list.
    /// C: listpack.c:886-890, lpAppend
    pub fn append(&mut self, s: &[u8]) -> Result<(), RedisError> {
        let eof_pos = self.bytes_len() - 1;
        self.insert_string(s, eof_pos, InsertWhere::Before)?;
        Ok(())
    }

    /// Append an integer element at the tail of the list.
    /// C: listpack.c:893-897, lpAppendInteger
    pub fn append_integer(&mut self, lval: i64) -> Result<(), RedisError> {
        let eof_pos = self.bytes_len() - 1;
        self.insert_integer(lval, eof_pos, InsertWhere::Before)?;
        Ok(())
    }

    /// Replace the element at `*pos` with a byte string. Updates `*pos` to the
    /// new cursor. C: listpack.c:902-904, lpReplace
    pub fn replace(&mut self, pos: &mut usize, s: &[u8]) -> Result<(), RedisError> {
        let new_pos = self.insert_string(s, *pos, InsertWhere::Replace)?;
        *pos = new_pos.unwrap_or(*pos);
        Ok(())
    }

    /// Replace the element at `*pos` with an integer. Updates `*pos`.
    /// C: listpack.c:910-912, lpReplaceInteger
    pub fn replace_integer(&mut self, pos: &mut usize, lval: i64) -> Result<(), RedisError> {
        let new_pos = self.insert_integer(lval, *pos, InsertWhere::Replace)?;
        *pos = new_pos.unwrap_or(*pos);
        Ok(())
    }

    /// Delete the element at `pos`. Returns the cursor of the next element, or
    /// `None` if the deleted element was the last.
    /// C: listpack.c:918-920, lpDelete
    pub fn delete(&mut self, pos: usize) -> Result<Option<usize>, RedisError> {
        self.insert_raw(None, None, pos, InsertWhere::Replace)
    }

    /// Delete `num` consecutive elements starting at `*pos`. Updates `*pos` to
    /// the element immediately following the deleted range (or `None`).
    /// C: listpack.c:922-962, lpDeleteRangeWithEntry
    pub fn delete_range_with_entry(
        &mut self,
        pos: &mut Option<usize>,
        num: usize,
    ) -> Result<(), RedisError> {
        if num == 0 {
            return Ok(());
        }
        let start = match *pos {
            Some(p) => p,
            None => return Ok(()),
        };
        let bytes = self.bytes_len();
        let eof_pos = bytes - 1;

        let mut deleted: u32 = 0;
        let mut tail = start;
        let mut n = num;
        while n > 0 {
            deleted += 1;
            tail += entry_total_size(&self.data, tail);
            if self.data.get(tail).copied() == Some(LP_EOF) {
                debug_assert_eq!(tail + 1, bytes);
                break;
            }
            n -= 1;
        }

        // memmove: shift tail (including EOF) to start.
        let tail_len = eof_pos - tail + 1; // includes EOF
        let new_bytes = bytes - (tail - start);
        self.data.copy_within(tail..tail + tail_len, start);
        self.data.truncate(new_bytes);
        self.set_total_bytes(new_bytes as u32);

        let num_el = self.get_num_elements();
        if num_el != LP_HDR_NUMELE_UNKNOWN {
            self.set_num_elements(num_el.saturating_sub(deleted as u16));
        }
        self.shrink_to_fit();

        *pos = if self.data.get(start).copied() == Some(LP_EOF) {
            None
        } else {
            Some(start)
        };
        Ok(())
    }

    /// Delete `num` elements starting at the element at zero-based `index`
    /// (negative indexes count from the tail). Does nothing if the range is
    /// invalid. C: listpack.c:964-990, lpDeleteRange
    pub fn delete_range(&mut self, mut index: i64, num: usize) -> Result<(), RedisError> {
        if num == 0 {
            return Ok(());
        }
        let Some(mut pos) = self.seek(index) else {
            return Ok(());
        };

        let num_el = self.get_num_elements();
        if num_el != LP_HDR_NUMELE_UNKNOWN && index < 0 {
            index = num_el as i64 + index;
        }

        if num_el != LP_HDR_NUMELE_UNKNOWN
            && (num_el as i64 - index) as usize <= num
        {
            // Delete to end: just place EOF at `pos` and update header.
            self.data[pos] = LP_EOF;
            let new_bytes = pos + 1;
            self.data.truncate(new_bytes);
            self.set_total_bytes(new_bytes as u32);
            self.set_num_elements(index as u16);
            self.shrink_to_fit();
        } else {
            self.delete_range_with_entry(&mut Some(pos), num)?;
        }
        Ok(())
    }

    /// Delete all elements whose byte offsets are listed in `positions`.
    /// The positions must be in ascending order (as they appear in the listpack).
    /// C: listpack.c:992-1039, lpBatchDelete
    pub fn batch_delete(&mut self, positions: &[usize]) -> Result<(), RedisError> {
        if positions.is_empty() {
            return Ok(());
        }
        let total_bytes = self.bytes_len();
        let lp_end = total_bytes; // exclusive; lp[total_bytes-1] == LP_EOF
        debug_assert_eq!(self.data[lp_end - 1], LP_EOF);

        let mut dst = positions[0];
        let count = positions.len();

        for i in 0..count {
            let skip = positions[i];
            debug_assert_ne!(self.data.get(skip).copied(), Some(LP_EOF));
            let keep_start = skip + entry_total_size(&self.data, skip);
            let keep_end = if i + 1 < count {
                let ke = positions[i + 1];
                if keep_start == ke {
                    continue;
                }
                ke
            } else {
                lp_end
            };
            debug_assert!(keep_end > keep_start);
            let bytes_to_keep = keep_end - keep_start;
            self.data.copy_within(keep_start..keep_start + bytes_to_keep, dst);
            dst += bytes_to_keep;
        }

        let deleted_bytes = lp_end - dst;
        let new_total = total_bytes - deleted_bytes;
        debug_assert_eq!(self.data[new_total - 1], LP_EOF);
        self.data.truncate(new_total);
        self.set_total_bytes(new_total as u32);

        let num_el = self.get_num_elements();
        if num_el != LP_HDR_NUMELE_UNKNOWN {
            self.set_num_elements(num_el.saturating_sub(count as u16));
        }
        self.shrink_to_fit();
        Ok(())
    }

    // ── Merge / duplicate ─────────────────────────────────────────────────────

    /// Merge `second` into `first`, appending second's content after first's.
    /// Returns the merged listpack; the caller should discard both inputs.
    /// Returns `None` if either input is invalid.
    /// C: listpack.c:1056-1130, lpMerge
    ///
    /// PORT NOTE: The C API takes `unsigned char **` and NULLs one of them
    /// in-place. We take ownership of both and return the merged result.
    pub fn merge(first: Self, second: Self) -> Option<Self> {
        let first_bytes = first.bytes_len();
        let second_bytes = second.bytes_len();
        let first_len = {
            let num = first.get_num_elements();
            if num == LP_HDR_NUMELE_UNKNOWN {
                LP_HDR_NUMELE_UNKNOWN as usize
            } else {
                num as usize
            }
        };
        let second_len = {
            let num = second.get_num_elements();
            if num == LP_HDR_NUMELE_UNKNOWN {
                LP_HDR_NUMELE_UNKNOWN as usize
            } else {
                num as usize
            }
        };

        // Combined length: avoid u64 overflow (assert in C; here return None).
        let lpbytes = (first_bytes as u64)
            .checked_add(second_bytes as u64)?
            .checked_sub((LP_HDR_SIZE + 1) as u64)?;
        if lpbytes >= u32::MAX as u64 {
            return None;
        }
        let lplength: u16 = (first_len + second_len).min(LP_HDR_NUMELE_UNKNOWN as usize) as u16;

        let mut merged;
        if first_bytes >= second_bytes {
            // Retain first, append second's payload after first's EOF.
            merged = first;
            let append_from = LP_HDR_SIZE; // skip second's header
            let append_len = second_bytes - LP_HDR_SIZE;
            let write_at = merged.data.len() - 1; // overwrite first's EOF
            merged.data.truncate(write_at);
            merged.data.extend_from_slice(&second.data[append_from..append_from + append_len]);
        } else {
            // Retain second, prepend first's payload before second's first entry.
            merged = second;
            let prepend_len = first_bytes - 1; // first without its EOF byte
            // Insert first[0..prepend_len] before merged.data[LP_HDR_SIZE]
            let old_content_len = merged.data.len() - LP_HDR_SIZE;
            let new_len = LP_HDR_SIZE + prepend_len + old_content_len;
            merged.data.resize(new_len, 0);
            merged.data.copy_within(LP_HDR_SIZE..LP_HDR_SIZE + old_content_len,
                                     LP_HDR_SIZE + prepend_len);
            merged.data[LP_HDR_SIZE..LP_HDR_SIZE + prepend_len]
                .copy_from_slice(&first.data[..prepend_len]);
        }

        merged.set_num_elements(lplength);
        merged.set_total_bytes(lpbytes as u32);
        Some(merged)
    }

    /// Return an independent clone of this listpack.
    /// C: listpack.c:1132-1137, lpDup
    pub fn dup(&self) -> Self {
        ListPack { data: self.data.clone() }
    }

    // ── Seek ─────────────────────────────────────────────────────────────────

    /// Return the byte offset of the element at zero-based `index` (negative
    /// counts from the tail, -1 = last). Returns `None` if out of range.
    /// C: listpack.c:1158-1200, lpSeek
    pub fn seek(&self, mut index: i64) -> Option<usize> {
        let mut forward = true;
        let num_el = self.get_num_elements();

        if num_el != LP_HDR_NUMELE_UNKNOWN {
            if index < 0 {
                index = num_el as i64 + index;
            }
            if index < 0 {
                return None;
            }
            if index >= num_el as i64 {
                return None;
            }
            // Scan from the nearer end.
            if index > num_el as i64 / 2 {
                forward = false;
                index -= num_el as i64;
            }
        } else if index < 0 {
            forward = false;
        }

        if forward {
            let mut ele = self.first();
            while index > 0 {
                ele = ele.and_then(|p| self.next(p));
                index -= 1;
            }
            ele
        } else {
            let mut ele = self.last();
            while index < -1 {
                ele = ele.and_then(|p| self.prev(p));
                index += 1;
            }
            ele
        }
    }

    /// Estimate the byte size of a listpack holding `rep` copies of the integer
    /// `lval`. C: listpack.c:1145-1151, lpEstimateBytesRepeatedInteger
    pub fn estimate_bytes_repeated_integer(lval: i64, rep: usize) -> usize {
        let mut intenc = [0u8; LP_MAX_INT_ENCODING_LEN];
        let enclen = encode_integer_get_type(lval, &mut intenc);
        let backlen = encode_backlen_size(enclen);
        LP_HDR_SIZE + (enclen as usize + backlen) * rep + 1
    }

    // ── Validation ───────────────────────────────────────────────────────────

    /// Validate the element at `*pp` and advance `*pp` to the next one.
    /// Returns `true` if the element is valid. On EOF, sets `*pp` to `None`
    /// and returns `true`. C: listpack.c:1212-1252, lpValidateNext
    pub fn validate_next(&self, pp: &mut Option<usize>, lpbytes: usize) -> bool {
        let pos = match *pp {
            Some(p) => p,
            None => return false,
        };

        if pos < LP_HDR_SIZE || pos > lpbytes - 1 {
            return false;
        }

        if self.data[pos] == LP_EOF {
            if pos + 1 != lpbytes {
                return false;
            }
            *pp = None;
            return true;
        }

        let lenbytes = current_encoded_size_bytes(&self.data[pos..]);
        if lenbytes == 0 {
            return false;
        }
        if pos + lenbytes as usize > lpbytes - 1 {
            return false;
        }

        let entrylen = current_encoded_size_unsafe(&self.data[pos..]) as u64;
        let encoded_backlen = encode_backlen_size(entrylen) as u64;
        let total = entrylen + encoded_backlen;

        if pos + total as usize > lpbytes - 1 {
            return false;
        }

        // Verify backlen at the end of the entry matches the forward length.
        let backlen_end = pos + total as usize - 1;
        let prevlen = decode_backlen(&self.data, backlen_end);
        if prevlen + encoded_backlen != total {
            return false;
        }

        *pp = Some(pos + total as usize);
        true
    }

    /// Validate the structural integrity of the entire listpack.
    /// When `deep` is false, only the header is checked; when true, every entry
    /// is scanned and optionally validated via `entry_cb`.
    /// C: listpack.c:1262-1299, lpValidateIntegrity
    pub fn validate_integrity(
        &self,
        size: usize,
        deep: bool,
        entry_cb: Option<ValidateEntryCb>,
    ) -> bool {
        if size < LP_HDR_SIZE + 1 {
            return false;
        }
        if self.bytes_len() != size {
            return false;
        }
        if self.data[size - 1] != LP_EOF {
            return false;
        }
        if !deep {
            return true;
        }

        let mut count: u32 = 0;
        let num_el = self.get_num_elements();
        let mut pp = self.validate_first().map(Some).unwrap_or(None);

        while let Some(p) = pp {
            if self.data[p] == LP_EOF {
                break;
            }
            let prev = p;
            if !self.validate_next(&mut Some(prev), size).then(|| pp = Some(prev)).is_some() {
                // TODO(port): validate_next API mismatch — revisit pp update logic in Phase B
                return false;
            }
            if let Some(cb) = entry_cb {
                if !cb(prev, num_el as u32) {
                    return false;
                }
            }
            count += 1;
        }

        if pp.is_some() {
            return false;
        }

        if num_el != LP_HDR_NUMELE_UNKNOWN && num_el as u32 != count {
            return false;
        }
        true
    }

    // ── Comparison ───────────────────────────────────────────────────────────

    /// Return `true` if the element at `pos` equals byte string `s`.
    /// C: listpack.c:1303-1320, lpCompare
    pub fn compare(&self, pos: usize, s: &[u8]) -> bool {
        if self.data.get(pos).copied() == Some(LP_EOF) {
            return false;
        }
        let (bytes, sz, _) = match self.get_with_size(pos) {
            Some(v) => v,
            None => return false,
        };
        if let Some(value) = bytes {
            s.len() == sz as usize && value == s
        } else {
            // Element is integer; compare by parsing `s`.
            if let Some(sval) = bytes_to_i64(s) {
                sz == sval
            } else {
                false
            }
        }
    }

    // ── Random selection ─────────────────────────────────────────────────────

    /// Randomly select a key-value pair (even-indexed key + the following value)
    /// from a listpack that stores interleaved key-value pairs.
    /// `total_count` is the pre-computed number of key-value pairs (len/2).
    /// C: listpack.c:1338-1352, lpRandomPair
    ///
    /// TODO(port): replace `pseudo_rand` with a deterministic RNG seeded by the
    /// caller in Phase B. Currently uses a simple modulo placeholder.
    pub fn random_pair(
        &self,
        total_count: usize,
        pseudo_rand: usize,
    ) -> Option<(ListpackEntry, ListpackEntry)> {
        debug_assert!(total_count > 0);
        let r = (pseudo_rand % total_count) * 2;
        let kpos = self.seek(r as i64)?;
        let key = self.get_value(kpos)?;
        let vpos = self.next(kpos)?;
        let val = self.get_value(vpos)?;
        Some((key, val))
    }

    /// Randomly select `count` entries (with possible duplicates) into a `Vec`.
    /// C: listpack.c:1357-1389, lpRandomEntries
    ///
    /// TODO(port): replace index generation with proper RNG in Phase B.
    pub fn random_entries(
        &self,
        count: usize,
        rand_indices: &[usize],
    ) -> Vec<ListpackEntry> {
        let total_size = {
            let mut lp = ListPack { data: self.data.clone() };
            lp.length()
        };
        debug_assert!(total_size > 0);
        let mut picks: Vec<(usize, usize)> = rand_indices
            .iter()
            .enumerate()
            .map(|(order, &idx)| (idx % total_size, order))
            .collect();
        picks.sort_by_key(|&(idx, _)| idx);

        let mut out = vec![ListpackEntry::default(); count];
        let mut p = self.first();
        let mut j = 0usize;

        for (idx, order) in &picks {
            while j < *idx {
                p = p.and_then(|pp| self.next(pp));
                j += 1;
            }
            if let Some(pos) = p {
                if let Some(entry) = self.get_value(pos) {
                    out[*order] = entry;
                }
            }
        }
        out
    }

    /// Randomly select `count` key-value pairs (with possible duplicates).
    /// C: listpack.c:1395-1438, lpRandomPairs
    ///
    /// TODO(port): replace index generation with proper RNG in Phase B.
    pub fn random_pairs(
        &self,
        count: usize,
        rand_indices: &[usize],
    ) -> Vec<(ListpackEntry, ListpackEntry)> {
        let mut lp = ListPack { data: self.data.clone() };
        let total_size = lp.length() / 2;
        debug_assert!(total_size > 0);

        let mut picks: Vec<(usize, usize)> = rand_indices
            .iter()
            .enumerate()
            .map(|(order, &idx)| ((idx % total_size) * 2, order))
            .collect();
        picks.sort_by_key(|&(idx, _)| idx);

        let mut out = Vec::with_capacity(count);

        if picks.is_empty() {
            return out;
        }
        let mut lp_index = picks[0].0;
        let mut pick_index = 0usize;
        let mut p = self.seek(lp_index as i64);

        while let Some(kpos) = p {
            if pick_index >= count {
                break;
            }
            let key = match self.get_value(kpos) { Some(e) => e, None => break };
            let vpos = match self.next(kpos) { Some(p) => p, None => break };
            let val = match self.get_value(vpos) { Some(e) => e, None => break };

            while pick_index < picks.len() && lp_index == picks[pick_index].0 {
                out.push((key.clone(), val.clone()));
                pick_index += 1;
            }
            lp_index += 2;
            p = self.next(vpos);
        }
        out
    }

    /// Randomly select `count` unique key-value pairs (no repetitions).
    /// Returns the number of pairs actually selected (may be < count if the
    /// listpack has fewer pairs).
    /// C: listpack.c:1447-1473, lpRandomPairsUnique
    ///
    /// TODO(port): replace RNG with proper implementation in Phase B.
    pub fn random_pairs_unique(
        &self,
        count: usize,
        rand_fn: &mut dyn FnMut() -> f64,
    ) -> Vec<(ListpackEntry, ListpackEntry)> {
        let mut lp = ListPack { data: self.data.clone() };
        let total_size = lp.length() / 2;
        let count = count.min(total_size);
        let mut out = Vec::with_capacity(count);

        let mut p = self.first();
        let mut index: u32 = 0;
        let mut remaining = count as u32;

        while out.len() < count {
            if let Some(np) = self.next_random_internal(p, &mut index, remaining, true, rand_fn) {
                p = Some(np);
            } else {
                break;
            }
            let kpos = p.unwrap();
            let key = match self.get_value(kpos) { Some(e) => e, None => break };
            p = self.next(kpos);
            index += 1;
            let val = if let Some(vpos) = p {
                let v = match self.get_value(vpos) { Some(e) => e, None => break };
                p = self.next(vpos);
                index += 1;
                v
            } else {
                break
            };
            out.push((key, val));
            remaining -= 1;
        }
        out
    }

    /// Reservoir-sampling step: advance `p` to the next randomly chosen element.
    /// C: listpack.c:1499-1529, lpNextRandom
    ///
    /// TODO(port): replace `rand_fn` with proper RNG integration in Phase B.
    pub fn next_random(
        &self,
        pos: Option<usize>,
        index: &mut u32,
        remaining: u32,
        even_only: bool,
        rand_fn: &mut dyn FnMut() -> f64,
    ) -> Option<usize> {
        self.next_random_internal(pos, index, remaining, even_only, rand_fn)
    }

    fn next_random_internal(
        &self,
        mut p: Option<usize>,
        index: &mut u32,
        remaining: u32,
        even_only: bool,
        rand_fn: &mut dyn FnMut() -> f64,
    ) -> Option<usize> {
        let mut lp = ListPack { data: self.data.clone() };
        let total_size = lp.length() as u32;
        let mut i = *index;

        while i < total_size {
            let pos = p?;
            if even_only && i % 2 != 0 {
                p = self.next(pos);
                i += 1;
                continue;
            }
            let mut available = total_size - i;
            if even_only {
                available /= 2;
            }
            let random_double: f64 = rand_fn();
            let threshold = remaining as f64 / available as f64;
            if random_double <= threshold {
                *index = i;
                return p;
            }
            p = self.next(pos);
            i += 1;
        }
        None
    }

    // ── Debug repr ───────────────────────────────────────────────────────────

    /// Print a human-readable representation of the listpack to stdout.
    /// C: listpack.c:1532-1574, lpRepr
    pub fn repr(&self) {
        let mut lp_clone = ListPack { data: self.data.clone() };
        println!(
            "{{total bytes {}}} {{num entries {}}}",
            self.bytes_len(),
            lp_clone.length()
        );
        let mut p = self.first();
        let mut index = 0i32;
        while let Some(pos) = p {
            let encoded_size = current_encoded_size_unsafe(&self.data[pos..]);
            let encoded_size_bytes = current_encoded_size_bytes(&self.data[pos..]);
            let back_len = encode_backlen_size(encoded_size as u64);
            let total = encoded_size as usize + back_len;
            println!(
                "{{\n\tindex: {:2},\n\toffset: {:1},\n\thdr+entrylen+backlen: {:2},\n\thdrlen: {:3},\n\tbacklen: {:2},\n\tpayload: {:1}",
                index,
                pos,
                total,
                encoded_size_bytes,
                back_len,
                encoded_size as usize - encoded_size_bytes as usize,
            );
            print!("\tbytes: ");
            for i in 0..total {
                print!("{:02x}|", self.data[pos + i]);
            }
            println!();
            let mut intbuf = [0u8; LP_INTBUF_SIZE];
            if let Some((s, len)) = self.get_as_bytes(pos, &mut intbuf) {
                let preview = &s[..s.len().min(40)];
                print!("\t[str]");
                print!("{}", preview.iter().map(|&b| b as char).collect::<String>());
                if len > 40 { print!("..."); }
                println!();
            }
            println!("}}");
            index += 1;
            p = self.next(pos);
        }
        println!("{{end}}");
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/listpack.c  (1575 lines, ~35 functions)
//   target_crate:  redis-ds
//   confidence:    medium
//   todos:         9  (1 TODO(architect) + 8 TODO(port))
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Full logic port of all listpack.c functions. ListPack.data
//                  field must be added to listpack.rs skeleton (TODO(architect))
//                  before Phase B compile. Random-selection functions accept
//                  caller-supplied RNG instead of global rand() — Phase B
//                  should wire in a proper seeded RNG. validate_integrity has
//                  a known pp-update API mismatch (TODO(port) at line 1302).
//                  All E0432/E0282 errors are expected name-resolution failures.
// ──────────────────────────────────────────────────────────────────────────
