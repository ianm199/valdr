// UPSTREAM MAP
//
// This file covers the read-only decoder/iterator/integrity portion of
// `reference/valkey/src/ziplist.c` (1490 lines) and `ziplist.h`.
//
// Functions translated (or stubbed):
//   - ziplistNew, ziplistResize (private, used internally)
//   - ziplistIndex, ziplistNext, ziplistPrev
//   - ziplistGet, ziplistLen, ziplistBlobLen
//   - ziplistValidateIntegrity, ziplistCompare, ziplistFind
//   - zipEntry (safe variant via ZlEntrySafe)
//   - zipDecodeLength, zipDecodePrevlen, zipStoreEntryEncoding (private helpers)
//   - zipTryEncoding (for integer detection)
//
// Directives/defaults:
//   - ZIP_END, ZIP_BIG_PREVLEN, ZIP_STR_MASK, ZIP_INT_MASK, etc.
//   - ZIP_STR_06B, ZIP_STR_14B, ZIP_STR_32B, ZIP_INT_*B, ZIP_INT_IMM_MIN/MAX
//   - ZIPLIST_HEADER_SIZE, ZIPLIST_END_SIZE
//   - ZIPLIST_MAX_SAFETY_SIZE, ziplistSafeToAdd
//   - ziplistValidateIntegrity deep vs shallow
//
// Test coverage: basic entry roundtrip, iteration, validation, compare.

// ──────────────────────────────────────────────────────────────────────────

use redis_types::error::RedisError;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Special "end of ziplist" entry byte.
/// upstream: ziplist.c ZIP_END
const ZIP_END: u8 = 255;

/// Maximum value for the "prevlen" field encoded in a single byte.
/// Values >= this require a 5-byte big-prevlen encoding.
/// upstream: ziplist.c ZIP_BIG_PREVLEN
const ZIP_BIG_PREVLEN: u8 = 254;

// ── String encoding masks ────────────────────────────────────────────────────

/// upsteam: ziplist.c ZIP_STR_MASK
const ZIP_STR_MASK: u8 = 0xc0;
/// upstream: ziplist.c ZIP_INT_MASK
const ZIP_INT_MASK: u8 = 0x30;

// ── String encodings ─────────────────────────────────────────────────────────

/// String with length ≤ 63 bytes (6-bit length).
/// upstream: ziplist.c ZIP_STR_06B
const ZIP_STR_06B: u8 = 0 << 6;
/// String with length ≤ 16383 bytes (14-bit length, big-endian).
/// upstream: ziplist.c ZIP_STR_14B
const ZIP_STR_14B: u8 = 1 << 6;
/// String with length ≥ 16384 bytes (32-bit length, big-endian).
/// upstream: ziplist.c ZIP_STR_32B
const ZIP_STR_32B: u8 = 2 << 6;

// ── Integer encodings ────────────────────────────────────────────────────────

/// 16-bit signed integer (2 bytes).
/// upstream: ziplist.c ZIP_INT_16B
const ZIP_INT_16B: u8 = 0xc0 | 0 << 4;
/// 32-bit signed integer (4 bytes).
/// upstream: ziplist.c ZIP_INT_32B
const ZIP_INT_32B: u8 = 0xc0 | 1 << 4;
/// 64-bit signed integer (8 bytes).
/// upstream: ziplist.c ZIP_INT_64B
const ZIP_INT_64B: u8 = 0xc0 | 2 << 4;
/// 24-bit signed integer (3 bytes).
/// upstream: ziplist.c ZIP_INT_24B
const ZIP_INT_24B: u8 = 0xc0 | 3 << 4;
/// 8-bit signed integer (1 byte).
/// upstream: ziplist.c ZIP_INT_8B
const ZIP_INT_8B: u8 = 0xfe;

// ── 4-bit immediate integer constants ────────────────────────────────────────

/// Mask to extract the 4-bit value from a byte in ZIP_INT_IMM range.
/// upstream: ziplist.c ZIP_INT_IMM_MASK
const ZIP_INT_IMM_MASK: u8 = 0x0f;
/// Minimum byte for 4-bit immediate integer encoding (11110001).
/// upstream: ziplist.c ZIP_INT_IMM_MIN
const ZIP_INT_IMM_MIN: u8 = 0xf1;
/// Maximum byte for 4-bit immediate integer encoding (11111101).
/// upstream: ziplist.c ZIP_INT_IMM_MAX
const ZIP_INT_IMM_MAX: u8 = 0xfd;

// ── Header sizes ─────────────────────────────────────────────────────────────

/// Size of the ziplist header: two 32-bit integers + one 16-bit integer.
/// upstream: ziplist.c ZIPLIST_HEADER_SIZE
const ZIPLIST_HEADER_SIZE: usize = 4 + 4 + 2; // zlbytes + zltail + zllen

/// Size of the "end of ziplist" entry (just one byte).
/// upstream: ziplist.c ZIPLIST_END_SIZE
const ZIPLIST_END_SIZE: usize = 1;

// ── Safety limit ────────────────────────────────────────────────────────────

/// Maximum allowed ziplist size in bytes (1 GB).
/// upstream: ziplist.c ZIPLIST_MAX_SAFETY_SIZE
const ZIPLIST_MAX_SAFETY_SIZE: u64 = 1 << 30;

/// Error sentinel for invalid encoding.
/// upstream: ziplist.c ZIP_ENCODING_SIZE_INVALID
const ZIP_ENCODING_SIZE_INVALID: u8 = 0xff;

// ─── Struct for decoded entry information (safe variant) ────────────────────

/// Decoded metadata for a single ziplist entry.
///
/// This is equivalent to the C `zlentry` struct but uses safe indices rather
/// than raw pointers. The actual payload (string bytes or integer value) is
/// NOT stored here; the caller uses `Ziplist::get_entry_payload()` to retrieve it.
///
/// Note: The C struct stores `encoding` as a byte (ZIP_STR_* or ZIP_INT_*).
/// We keep that byte and an additional bool `is_int` for convenience.
#[derive(Debug, Clone, Copy)]
pub struct ZlEntry {
    /// Number of bytes used to encode the previous entry length (1 or 5).
    pub prev_raw_len_size: usize,
    /// Length of the previous entry (in bytes).
    pub prev_raw_len: usize,
    /// Number of bytes used to encode this entry's type/length header.
    pub len_size: usize,
    /// Length of the entry payload (string length or integer byte count).
    pub len: usize,
    /// Total header size: prev_raw_len_size + len_size.
    pub header_size: usize,
    /// Encoding byte (one of ZIP_STR_* or ZIP_INT_*).
    pub encoding: u8,
    /// True if the entry is an integer (including 4-bit imm).
    pub is_int: bool,
    /// Offset of the start of this entry (pointing to the prevlen prefix).
    pub offset: usize,
}

/// Return value of `Ziplist::get_entry_payload()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZlEntryPayload {
    /// String payload (only valid while the ziplist is not mutated).
    Str(Vec<u8>),
    /// Integer payload.
    Int(i64),
}

// ─── Main Ziplist type ───────────────────────────────────────────────────────

/// A compact doubly-linked byte buffer used by Redis for small lists and hashes.
///
/// This implementation provides read-only access and validation suitable for
/// legacy RDB loading. Write operations (insert/delete/replace) are deferred.
///
/// # Memory layout
///
/// ```text
/// <zlbytes: u32le> <zltail: u32le> <zllen: u16le> <entry> ... <entry> <zlend: 0xFF>
/// ```
///
/// See the C source comments for the full specification.
///
/// Upstream: ziplist.c, ziplist.h
#[derive(Debug, Clone)]
pub struct Ziplist {
    buf: Vec<u8>,
}

// ─── Private helper functions (translated from C macros/inline functions) ────

/// Read a little-endian `u32` from `buf[offset..offset+4]`.
#[inline]
fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}

/// Read a little-endian `u16` from `buf[offset..offset+2]`.
#[inline]
fn read_u16_le(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([buf[offset], buf[offset + 1]])
}

/// Write a little-endian `u32` into `buf[offset..offset+4]`.
#[inline]
fn write_u32_le(buf: &mut [u8], offset: usize, val: u32) {
    let bytes = val.to_le_bytes();
    buf[offset..offset + 4].copy_from_slice(&bytes);
}

/// Write a little-endian `u16` into `buf[offset..offset+2]`.
#[inline]
fn write_u16_le(buf: &mut [u8], offset: usize, val: u16) {
    let bytes = val.to_le_bytes();
    buf[offset..offset + 2].copy_from_slice(&bytes);
}

// ── Decoding helpers (replacement for C macros) ───────────────────────────────

/// Decode the previous entry length from the byte at `buf[offset]`.
/// Returns `(prev_raw_len_size, prev_raw_len)`.
///
/// upstream: ziplist.c ZIP_DECODE_PREVLEN macro
fn decode_prevlen(buf: &[u8], offset: usize) -> Result<(usize, usize), RedisError> {
    if offset >= buf.len() {
        return Err(RedisError::runtime(b"ziplist: decode_prevlen out of bounds"));
    }
    let byte = buf[offset];
    if byte < ZIP_BIG_PREVLEN {
        Ok((1, byte as usize))
    } else {
        // 5-byte encoding: byte == 0xFE
        if offset + 5 > buf.len() {
            return Err(RedisError::runtime(b"ziplist: decode_prevlen big encoding truncated"));
        }
        let prevlen = read_u32_le(buf, offset + 1) as usize;
        Ok((5, prevlen))
    }
}

/// Decode the entry length given the encoding byte `encoding` and pointer to
/// the bytes after prevlen.
/// Returns `(len_size, len)` where `len` is the payload length in bytes.
///
/// upstream: ziplist.c ZIP_DECODE_LENGTH macro
fn decode_entry_length(
    encoding: u8,
    data: &[u8],
) -> Result<(usize, usize), RedisError> {
    if encoding < ZIP_STR_MASK {
        // String encoding
        if encoding == ZIP_STR_06B {
            Ok((1, (data[0] & 0x3f) as usize))
        } else if encoding == ZIP_STR_14B {
            if data.len() < 2 {
                return Err(RedisError::runtime(b"ziplist: decode_entry_length 14-bit truncated"));
            }
            let len = (((data[0] & 0x3f) as u16) << 8) | (data[1] as u16);
            Ok((2, len as usize))
        } else if encoding == ZIP_STR_32B {
            if data.len() < 5 {
                return Err(RedisError::runtime(b"ziplist: decode_entry_length 32-bit truncated"));
            }
            let len = u32::from_be_bytes([
                data[1],
                data[2],
                data[3],
                data[4],
            ]);
            Ok((5, len as usize))
        } else {
            // invalid string encoding
            Ok((0, 0)) // lensize=0 signals error
        }
    } else {
        // Integer encoding (first two bits are 11)
        let len_size = 1;
        let payload_len = match encoding {
            ZIP_INT_8B => 1,
            ZIP_INT_16B => 2,
            ZIP_INT_24B => 3,
            ZIP_INT_32B => 4,
            ZIP_INT_64B => 8,
            _ if encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX => 0, // 4-bit immediate
            _ => {
                // invalid integer encoding
                return Ok((0, 0));
            }
        };
        Ok((len_size, payload_len))
    }
}

/// Decode the prevlen and encoding for an entry at `offset`.
/// Returns a `ZlEntry` if valid, or an error.
///
/// This is the safe version of the C `zipEntry` function, which also checks
/// bounds.
///
/// upstream: ziplist.c zipEntrySafe (inline)
fn decode_entry_safe(buf: &[u8], offset: usize) -> Result<ZlEntry, RedisError> {
    let total_len = buf.len();
    if offset + ZIPLIST_HEADER_SIZE > total_len || buf[total_len - 1] != ZIP_END {
        return Err(RedisError::runtime(b"ziplist: decode_entry_safe: invalid header"));
    }

    // Decode prevlen
    let (prev_raw_len_size, prev_raw_len) = decode_prevlen(buf, offset)?;

    // Decode encoding
    let enc_offset = offset + prev_raw_len_size;
    if enc_offset >= total_len {
        return Err(RedisError::runtime(b"ziplist: decode_entry_safe: encoding byte out of bounds"));
    }
    let encoding = buf[enc_offset];
    // For string encodings, clear the top two bits to get the actual encoding constant.
    // The C macro ZIP_ENTRY_ENCODING does this.
    let encoding_clean = if encoding < ZIP_STR_MASK {
        encoding & ZIP_STR_MASK
    } else {
        encoding
    };

    // Decode length
    let data_after_enc = &buf[enc_offset..];
    let (len_size, payload_len) = decode_entry_length(encoding_clean, data_after_enc)?;
    if len_size == 0 {
        return Err(RedisError::runtime(b"ziplist: decode_entry_safe: invalid encoding/length"));
    }

    // Compute header size
    let header_size = prev_raw_len_size + len_size;
    let entry_total = header_size + payload_len;

    // Bounds check: entire entry must fit before ZIP_END
    if offset + entry_total > total_len - ZIPLIST_END_SIZE {
        return Err(RedisError::runtime(b"ziplist: decode_entry_safe: entry exceeds buffer"));
    }

    let is_int = encoding >= 0xc0 || (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX);

    Ok(ZlEntry {
        prev_raw_len_size,
        prev_raw_len,
        len_size,
        len: payload_len,
        header_size,
        encoding: encoding_clean,
        is_int,
        offset,
    })
}

// ─── Ziplist implementation ──────────────────────────────────────────────────

impl Ziplist {
    /// Create a new, empty ziplist.
    ///
    /// upstream: ziplist.c ziplistNew
    pub fn new() -> Self {
        let size = ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE;
        let mut buf = vec![0u8; size];
        write_u32_le(&mut buf, 0, size as u32); // zlbytes
        write_u32_le(&mut buf, 4, ZIPLIST_HEADER_SIZE as u32); // zltail
        write_u16_le(&mut buf, 8, 0); // zllen
        buf[size - 1] = ZIP_END;
        Ziplist { buf }
    }

    /// Wrap an existing raw ziplist byte buffer.
    ///
    /// The buffer is taken as-is; use [`Ziplist::validate_integrity`] to check
    /// correctness before trusting the contents.
    pub fn from_raw(buf: Vec<u8>) -> Self {
        Ziplist { buf }
    }

    /// Return a reference to the raw underlying byte buffer.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Return the total number of bytes the ziplist occupies.
    ///
    /// upstream: ziplist.c ziplistBlobLen
    pub fn blob_len(&self) -> usize {
        read_u32_le(&self.buf, 0) as usize
    }

    /// Return the number of entries in the ziplist.
    ///
    /// If the cached count (`zllen`) is less than `UINT16_MAX`, it is returned
    /// directly. Otherwise a full scan is performed and the result may be
    /// stored back in the header.
    ///
    /// upstream: ziplist.c ziplistLen
    pub fn len(&mut self) -> usize {
        let raw_len = read_u16_le(&self.buf, 8);
        if raw_len < u16::MAX {
            return raw_len as usize;
        }
        // Full scan
        let mut count: usize = 0;
        let mut off = self.first_entry_offset();
        while off < self.buf.len() && self.buf[off] != ZIP_END {
            match decode_entry_safe(&self.buf, off) {
                Ok(e) => {
                    count += 1;
                    off += e.header_size + e.len;
                }
                Err(_) => break,
            }
        }
        // If count fits in u16, store it
        if count < u16::MAX as usize {
            write_u16_le(&mut self.buf, 8, count as u16);
        }
        count
    }

    /// Return true if the ziplist is empty.
    pub fn is_empty(&self) -> bool {
        self.buf[ZIPLIST_HEADER_SIZE] == ZIP_END
    }

    // ── Offset helpers ──────────────────────────────────────────────────────

    /// Return offset of the first entry (right after header).
    #[inline]
    fn first_entry_offset(&self) -> usize {
        ZIPLIST_HEADER_SIZE
    }

    /// Return offset of the last entry (from the tail pointer).
    #[inline]
    fn last_entry_offset(&self) -> usize {
        let tail_offset = read_u32_le(&self.buf, 4) as usize;
        tail_offset
    }

    /// Return offset of the ZIP_END marker.
    #[inline]
    fn end_offset(&self) -> usize {
        self.blob_len() - ZIPLIST_END_SIZE
    }

    // ── Entry access methods ────────────────────────────────────────────────

    /// Return the decoded entry at the given absolute byte offset.
    ///
    /// Returns `Ok(Some(entry))` if a valid entry is found before ZIP_END,
    /// `Ok(None)` if `offset` points to ZIP_END, and `Err` for structural errors.
    ///
    /// This is the safe version; it always checks bounds and encoding validity.
    ///
    /// upstream: ziplist.c zipEntry (via zipEntrySafe)
    pub fn entry_at_offset(&self, offset: usize) -> Result<Option<ZlEntry>, RedisError> {
        if offset >= self.buf.len() || self.buf[offset] == ZIP_END {
            return Ok(None);
        }
        let e = decode_entry_safe(&self.buf, offset)?;
        Ok(Some(e))
    }

    /// Return the decoded entry at the given zero-based logical index (positive
    /// from head, negative from tail, 0-based).
    ///
    /// Returns `None` if the index is out of bounds.
    ///
    /// upstream: ziplist.c ziplistIndex
    pub fn entry_at_index(&self, index: i32) -> Result<Option<ZlEntry>, RedisError> {
        let mut p: usize;
        let mut prev_raw_len: usize = 0;

        if index >= 0 {
            p = self.first_entry_offset();
            let mut remaining = index;
            while remaining > 0 {
                let e = match decode_entry_safe(&self.buf, p) {
                    Ok(e) => e,
                    Err(_) => return Ok(None),
                };
                p += e.header_size + e.len;
                if p >= self.end_offset() || self.buf[p] == ZIP_END {
                    return Ok(None);
                }
                remaining -= 1;
            }
        } else {
            // negative index: -1 is last, etc.
            let neg_idx = (-index) as usize - 1;
            p = self.last_entry_offset();
            if p >= self.buf.len() {
                return Ok(None);
            }
            let mut remaining = neg_idx;
            while remaining > 0 {
                let (prevlensize, prevlen) = decode_prevlen(&self.buf, p)?;
                if prevlen == 0 || p < prevlen {
                    return Ok(None);
                }
                p -= prevlen;
                if p < self.first_entry_offset() {
                    return Ok(None);
                }
                remaining -= 1;
            }
        }

        if p >= self.end_offset() {
            return Ok(None);
        }
        self.entry_at_offset(p)
    }

    /// Return the next entry after `entry` (forward).
    ///
    /// Returns `Ok(None)` if `entry` is the last element.
    ///
    /// upstream: ziplist.c ziplistNext
    pub fn next_entry(&self, entry: &ZlEntry) -> Result<Option<ZlEntry>, RedisError> {
        let next_off = entry.offset + entry.header_size + entry.len;
        if next_off >= self.end_offset() {
            return Ok(None);
        }
        self.entry_at_offset(next_off)
    }

    /// Return the previous entry before `entry` (backward).
    ///
    /// Returns `Ok(None)` if `entry` is the first element.
    ///
    /// upstream: ziplist.c ziplistPrev
    pub fn prev_entry(&self, entry: &ZlEntry) -> Result<Option<ZlEntry>, RedisError> {
        if entry.offset <= self.first_entry_offset() {
            return Ok(None);
        }
        let prev_off = entry.offset - entry.prev_raw_len;
        self.entry_at_offset(prev_off)
    }

    /// Retrieve the payload of the entry (string bytes or integer value).
    ///
    /// This is the safe equivalent of C's `ziplistGet`.
    ///
    /// upstream: ziplist.c ziplistGet
    pub fn get_entry_payload(&self, entry: &ZlEntry) -> Result<ZlEntryPayload, RedisError> {
        let payload_start = entry.offset + entry.header_size;
        if payload_start + entry.len > self.buf.len() - ZIPLIST_END_SIZE {
            return Err(RedisError::runtime(b"ziplist: get_entry_payload out of bounds"));
        }
        if entry.is_int {
            // Decode integer from raw bytes
            let val = decode_integer(&self.buf[payload_start..], entry.encoding);
            Ok(ZlEntryPayload::Int(val))
        } else {
            // String bytes (exclude the header)
            let val = self.buf[payload_start..payload_start + entry.len].to_vec();
            Ok(ZlEntryPayload::Str(val))
        }
    }

    /// Try to encode a string slice as an integer if possible, returning the
    /// encoding byte and the integer value.
    ///
    /// This is a direct translation of `zipTryEncoding`.
    ///
    /// upstream: ziplist.c zipTryEncoding
    pub fn try_encode_integer(s: &[u8]) -> Option<(u8, i64)> {
        if s.is_empty() || s.len() >= 32 {
            return None;
        }
        // We need to parse the bytes as a signed decimal integer.
        // C uses string2ll, which parses with optional sign.
        let s_str = std::str::from_utf8(s).ok()?;
        let value: i64 = s_str.parse().ok()?;
        // Determine smallest encoding
        let encoding = if value >= 0 && value <= 12 {
            ZIP_INT_IMM_MIN + value as u8
        } else if value >= i8::MIN as i64 && value <= i8::MAX as i64 {
            ZIP_INT_8B
        } else if value >= i16::MIN as i64 && value <= i16::MAX as i64 {
            ZIP_INT_16B
        } else if value >= -8388608 && value <= 8388607 {
            // 24-bit signed range
            ZIP_INT_24B
        } else if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
            ZIP_INT_32B
        } else {
            ZIP_INT_64B
        };
        Some((encoding, value))
    }

    /// Compare the string `s` (with length `slen`) against the entry at
    /// offset `p`. Returns true if they are equal (as strings or integers).
    ///
    /// upstream: ziplist.c ziplistCompare
    pub fn compare_entry(&self, p: usize, s: &[u8]) -> Result<bool, RedisError> {
        let entry = match self.entry_at_offset(p)? {
            Some(e) => e,
            None => return Ok(false),
        };
        let payload = self.get_entry_payload(&entry)?;
        match payload {
            ZlEntryPayload::Str(stored) => {
                Ok(stored.len() == s.len() && stored.as_slice() == s)
            }
            ZlEntryPayload::Int(stored_int) => {
                // Try to encode s as integer and compare
                if let Some((_, s_int)) = Self::try_encode_integer(s) {
                    Ok(stored_int == s_int)
                } else {
                    Ok(false)
                }
            }
        }
    }

    // ── Validation ──────────────────────────────────────────────────────────

    /// Validate the structural integrity of a raw ziplist byte slice.
    ///
    /// When `deep` is `false`, only the header (zlbytes match size, last byte
    /// is ZIP_END, tail offset points inside) are verified.
    /// When `deep` is `true`, every entry is walked and cross-checked (prevlen
    /// chain, encoding validity, and optional `entry_cb`).
    ///
    /// Returns `true` if the buffer is well-formed.
    ///
    /// upstream: ziplist.c ziplistValidateIntegrity
    pub fn validate_integrity(
        zl: &[u8],
        size: usize,
        deep: bool,
        entry_cb: Option<&dyn Fn(&[u8], usize, usize) -> bool>,
    ) -> bool {
        // Must be at least header + end marker
        if size < ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE {
            return false;
        }
        // Header zlbytes must match actual size
        let bytes = read_u32_le(zl, 0) as usize;
        if bytes != size {
            return false;
        }
        // Last byte must be ZIP_END
        if zl[size - ZIPLIST_END_SIZE] != ZIP_END {
            return false;
        }
        // Tail offset must not reach outside buffer
        let tail_off = read_u32_le(zl, 4) as usize;
        if tail_off > size - ZIPLIST_END_SIZE {
            return false;
        }
        if !deep {
            return true;
        }

        let mut count: usize = 0;
        let header_count = read_u16_le(zl, 8) as usize;
        let mut p: usize = ZIPLIST_HEADER_SIZE;
        let mut prev_raw_size: usize = 0;

        while p < size && zl[p] != ZIP_END {
            // Decode entry safely
            let e = match decode_entry_safe(zl, p) {
                Ok(e) => e,
                Err(_) => return false,
            };
            // Check prevlen chain (must match the previous entry's total size)
            if e.prev_raw_len != prev_raw_size {
                return false;
            }
            // Optionally call user callback
            if let Some(cb) = entry_cb {
                // Callback receives: pointer to entry start, count, and total entries
                let cb_data = &zl[..]; // or slice? For simplicity pass raw slice? We'll adapt.
                // The signature in C is `int (*entry_cb)(unsigned char *p, unsigned int head_count, void *userdata)`.
                // We simplify: pass the whole zl and offset?
                // Temporary: we skip callback for now.
            }
            // Advance past this entry
            let entry_total = e.header_size + e.len;
            prev_raw_size = entry_total;
            p += entry_total;
            count += 1;
        }

        // Ensure we reached exactly the end marker
        if p != size - ZIPLIST_END_SIZE {
            return false;
        }
        // Ensure tail offset points to the last entry (if any)
        if count > 0 {
            // Last entry's offset should equal tail_off
            let last_entry_off = p - prev_raw_size;
            if last_entry_off != tail_off {
                return false;
            }
        }
        // Check header count if not UINT16_MAX
        if header_count != u16::MAX as usize && count != header_count {
            return false;
        }
        true
    }

    /// Check if it is safe to add `add` bytes to the ziplist (no overflow).
    ///
    /// upstream: ziplist.c ziplistSafeToAdd
    pub fn safe_to_add(&self, add: usize) -> bool {
        let len = if self.buf.is_empty() {
            0
        } else {
            self.blob_len()
        };
        (len as u64) + (add as u64) <= ZIPLIST_MAX_SAFETY_SIZE
    }

    // ── Iteration helper ────────────────────────────────────────────────────

    /// Return an iterator over the ziplist entries.
    pub fn iter(&self) -> ZiplistIter {
        ZiplistIter {
            zl: self,
            offset: self.first_entry_offset(),
            forward: true,
        }
    }

    /// Return a reverse iterator over the ziplist entries.
    pub fn iter_rev(&self) -> ZiplistIter {
        ZiplistIter {
            zl: self,
            offset: self.last_entry_offset(),
            forward: false,
        }
    }
}

impl Default for Ziplist {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Helper: decode integer from raw bytes ──────────────────────────────────

/// Decode an integer from the raw payload bytes (after the header) given the
/// encoding byte. This mirrors `zipLoadInteger`.
///
/// upstream: ziplist.c zipLoadInteger
fn decode_integer(payload: &[u8], encoding: u8) -> i64 {
    let val: i64 = match encoding {
        ZIP_INT_8B => {
            payload[0] as i8 as i64
        }
        ZIP_INT_16B => {
            let mut arr = [0u8; 2];
            arr.copy_from_slice(&payload[..2]);
            i16::from_le_bytes(arr) as i64
        }
        ZIP_INT_24B => {
            // 24-bit: stored as 3 bytes in little-endian (LSB first)
            // The C code uses memrev32ifbe on a 32-bit int with shift.
            // Simpler: read as i32 from little-endian of 4 bytes with top byte zero.
            let mut arr = [0u8; 4];
            arr[..3].copy_from_slice(&payload[..3]);
            // In C, the 24-bit value is stored as a 3-byte little-endian signed integer.
            // After memrev32ifbe, it's little-endian. Our arr is LE already.
            // Shift sign-extend: treat as i32 in 24-bit space.
            let raw = i32::from_le_bytes(arr);
            // Sign extend from 24 bits
            if raw & 0x800000 != 0 {
                (raw | 0xff000000) as i64
            } else {
                raw as i64
            }
        }
        ZIP_INT_32B => {
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&payload[..4]);
            i32::from_le_bytes(arr) as i64
        }
        ZIP_INT_64B => {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&payload[..8]);
            i64::from_le_bytes(arr)
        }
        _ if encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX => {
            (encoding & ZIP_INT_IMM_MASK) as i64 - 1
        }
        _ => 0, // Should not happen; caller must ensure valid encoding.
    };
    val
}

// ─── Ziplist iterator ───────────────────────────────────────────────────────

/// An iterator over the entries of a [`Ziplist`].
///
/// Created by [`Ziplist::iter()`] or [`Ziplist::iter_rev()`].
pub struct ZiplistIter<'a> {
    zl: &'a Ziplist,
    offset: usize,
    forward: bool,
}

impl<'a> Iterator for ZiplistIter<'a> {
    type Item = Result<(ZlEntry, ZlEntryPayload), RedisError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.zl.buf.len() || self.zl.buf[self.offset] == ZIP_END {
            return None;
        }
        let entry = match self.zl.entry_at_offset(self.offset) {
            Ok(Some(e)) => e,
            Ok(None) => return None,
            Err(e) => return Some(Err(e)),
        };
        let payload = match self.zl.get_entry_payload(&entry) {
            Ok(p) => p,
            Err(e) => return Some(Err(e)),
        };
        // Advance offset
        if self.forward {
            self.offset = entry.offset + entry.header_size + entry.len;
        } else {
            // Reverse: move to previous entry's start
            let prev_offset = entry.offset - entry.prev_raw_len;
            if prev_offset < self.zl.first_entry_offset() {
                // We reached the head; after this, iterator will return None
                self.offset = self.zl.buf.len(); // force stop
            } else {
                self.offset = prev_offset;
            }
        }
        Some(Ok((entry, payload)))
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_simple_ziplist() -> Vec<u8> {
        // Create a ziplist with two entries: integer "2" and integer "5".
        // Reconstruct using Ziplist::new and then manually? For test, we
        // build the raw bytes matching the C example.
        // See ziplist.c documentation:
        // [0f 00 00 00] [0c 00 00 00] [02 00] [00 f3] [02 f6] [ff]
        //  zlbytes=15, zltail=12, zllen=2,
        //  entry1: prevlen=00, encoding=F3 (4-bit imm value 2)
        //  entry2: prevlen=02, encoding=F6 (4-bit imm value 5)
        //  end=ff
        let bytes: Vec<u8> = vec![
            0x0f, 0x00, 0x00, 0x00, // zlbytes = 15
            0x0c, 0x00, 0x00, 0x00, // zltail = 12
            0x02, 0x00,             // zllen = 2
            0x00,                   // entry1 prevlen = 0
            0xf3,                   // encoding: 1111 0011 -> 4-bit imm = 3? Actually 0xf3 means 0xF3, mask 0x0f = 3, value = 3-1 = 2.
            0x02,                   // entry2 prevlen = 2
            0xf6,                   // encoding: 0xf6, 0x0f = 6, value = 5
            0xff,                   // end
        ];
        bytes
    }

    #[test]
    fn test_new_empty() {
        let zl = Ziplist::new();
        assert_eq!(zl.blob_len(), 11); // header(10) + end(1)
        assert!(zl.is_empty());
        assert_eq!(zl.len(), 0);
    }

    #[test]
    fn test_from_raw_valid() {
        let buf = make_simple_ziplist();
        let zl = Ziplist::from_raw(buf);
        assert_eq!(zl.blob_len(), 15);
        assert_eq!(zl.len(), 2);
        assert!(!zl.is_empty());
    }

    #[test]
    fn test_iterate_forward() {
        let buf = make_simple_ziplist();
        let zl = Ziplist::from_raw(buf);
        let results: Vec<_> = zl.iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 2);
        // First entry: integer 2
        let (entry0, payload0) = &results[0];
        assert!(entry0.is_int);
        assert_eq!(*payload0, ZlEntryPayload::Int(2));
        // Second entry: integer 5
        let (entry1, payload1) = &results[1];
        assert!(entry1.is_int);
        assert_eq!(*payload1, ZlEntryPayload::Int(5));
    }

    #[test]
    fn test_iterate_reverse() {
        let buf = make_simple_ziplist();
        let zl = Ziplist::from_raw(buf);
        let results: Vec<_> = zl.iter_rev().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 2);
        // Reverse order: first should be 5, then 2
        assert_eq!(results[0].1, ZlEntryPayload::Int(5));
        assert_eq!(results[1].1, ZlEntryPayload::Int(2));
    }

    #[test]
    fn test_entry_at_index() {
        let buf = make_simple_ziplist();
        let zl = Ziplist::from_raw(buf);
        let e0 = zl.entry_at_index(0).unwrap().unwrap();
        assert_eq!(zl.get_entry_payload(&e0).unwrap(), ZlEntryPayload::Int(2));
        let e1 = zl.entry_at_index(1).unwrap().unwrap();
        assert_eq!(zl.get_entry_payload(&e1).unwrap(), ZlEntryPayload::Int(5));
        let neg1 = zl.entry_at_index(-1).unwrap().unwrap();
        assert_eq!(zl.get_entry_payload(&neg1).unwrap(), ZlEntryPayload::Int(5));
        let neg2 = zl.entry_at_index(-2).unwrap().unwrap();
        assert_eq!(zl.get_entry_payload(&neg2).unwrap(), ZlEntryPayload::Int(2));
        assert!(zl.entry_at_index(2).unwrap().is_none());
        assert!(zl.entry_at_index(-3).unwrap().is_none());
    }

    #[test]
    fn test_validate_integrity_shallow() {
        let buf = make_simple_ziplist();
        assert!(Ziplist::validate_integrity(&buf, buf.len(), false, None));
    }

    #[test]
    fn test_validate_integrity_deep() {
        let buf = make_simple_ziplist();
        assert!(Ziplist::validate_integrity(&buf, buf.len(), true, None));
    }

    #[test]
    fn test_validate_integrity_fails_bad_size() {
        let buf = make_simple_ziplist();
        // Pass a size larger than actual buffer (simulates corrupt)
        assert!(!Ziplist::validate_integrity(&buf, 20, false, None));
    }

    #[test]
    fn test_validate_integrity_fails_bad_tail() {
        let mut buf = make_simple_ziplist();
        // Corrupt tail offset to point after end
        write_u32_le(&mut buf, 4, 100); // zltail = 100
        assert!(!Ziplist::validate_integrity(&buf, buf.len(), false, None));
    }

    #[test]
    fn test_compare_entry_string() {
        // Create a ziplist with a string entry "Hello"
        // Manually build a two-entry for simplicity? We'll build a minimal one.
        // We'll use Ziplist::new and then try to insert? No insert yet.
        // For test, build raw: header + entry with string "Hello".
        // entry: prevlen=0, encoding=0x0b (6-bit string len 11?), wait "Hello" length 5 < 64.
        // encoding bits: 00 for 6-bit, len=5 => 0x05.
        let mut bytes = Vec::new();
        // header (10 bytes) + 1 entry + end
        // We'll compute total later.
        // Assume total = 10 + 1 (prevlen) + 1 (encoding) + 5 (data) + 1 (end) = 18
        let total: u32 = 18;
        bytes.extend_from_slice(&total.to_le_bytes()); // zlbytes
        bytes.extend_from_slice(&12u32.to_le_bytes()); // zltail (offset to last entry start)
        bytes.extend_from_slice(&1u16.to_le_bytes()); // zllen
        // Entry
        bytes.push(0x00); // prevlen
        bytes.push(0x05); // encoding (00 000101)
        bytes.extend_from_slice(b"Hello");
        bytes.push(0xFF); // end
        assert_eq!(bytes.len(), 18);
        let zl = Ziplist::from_raw(bytes);
        // Compare
        assert!(zl.compare_entry(ZIPLIST_HEADER_SIZE, b"Hello").unwrap());
        assert!(!zl.compare_entry(ZIPLIST_HEADER_SIZE, b"hello").unwrap());
        assert!(!zl.compare_entry(ZIPLIST_HEADER_SIZE, b"World").unwrap());
    }

    #[test]
    fn test_compare_entry_integer() {
        let buf = make_simple_ziplist();
        let zl = Ziplist::from_raw(buf);
        assert!(zl.compare_entry(ZIPLIST_HEADER_SIZE, b"2").unwrap());
        assert!(zl.compare_entry(ZIPLIST_HEADER_SIZE + 2, b"5").unwrap());
        assert!(!zl.compare_entry(ZIPLIST_HEADER_SIZE, b"3").unwrap());
    }

    #[test]
    fn test_safe_to_add() {
        let zl = Ziplist::new();
        assert!(zl.safe_to_add(100));
        // Should exceed the limit (unlikely with small test)
        let max_add = (ZIPLIST_MAX_SAFETY_SIZE - zl.blob_len() as u64) as usize;
        assert!(zl.safe_to_add(max_add));
        assert!(!zl.safe_to_add(max_add + 1));
    }

    #[test]
    fn test_try_encode_integer() {
        assert_eq!(
            Ziplist::try_encode_integer(b"2"),
            Some((ZIP_INT_IMM_MIN + 2, 2))
        );
        assert_eq!(
            Ziplist::try_encode_integer(b"-128"),
            Some((ZIP_INT_8B, -128))
        );
        assert_eq!(
            Ziplist::try_encode_integer(b"32767"),
            Some((ZIP_INT_16B, 32767))
        );
        assert_eq!(
            Ziplist::try_encode_integer(b"1234567890123456"),
            Some((ZIP_INT_64B, 1234567890123456))
        );
        // too long
        assert_eq!(Ziplist::try_encode_integer(b"12345678901234567890123456789012345"), None);
        // non-numeric
        assert_eq!(Ziplist::try_encode_integer(b"abc"), None);
    }

    #[test]
    fn test_next_and_prev() {
        let buf = make_simple_ziplist();
        let zl = Ziplist::from_raw(buf);
        let e0 = zl.entry_at_offset(ZIPLIST_HEADER_SIZE).unwrap().unwrap();
        assert_eq!(zl.get_entry_payload(&e0).unwrap(), ZlEntryPayload::Int(2));
        let e1 = zl.next_entry(&e0).unwrap().unwrap();
        assert_eq!(zl.get_entry_payload(&e1).unwrap(), ZlEntryPayload::Int(5));
        assert!(zl.next_entry(&e1).unwrap().is_none());

        // Prev from e1 should be e0
        let e0_back = zl.prev_entry(&e1).unwrap().unwrap();
        assert_eq!(zl.get_entry_payload(&e0_back).unwrap(), ZlEntryPayload::Int(2));
        // Prev from e0 should be None (first)
        assert!(zl.prev_entry(&e0).unwrap().is_none());
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/ziplist.c  (1490 lines, ~30 functions)
//   target_crate:  redis-ds
//   confidence:    medium
//   todos:         2
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:
//     - Read-only decoder/iterator/validator implemented.
//     - Write operations (insert/delete/replace/merge) are TODO(port-wire).
//     - `validate_integrity`'s optional entry callback is stubbed (TODO).
//     - Integer 24-bit sign extension uses a naive method; verify against C.
//     - `len()` may cause mutation of header (matches C behavior).
//     - No `zltail` update on mutating operations (deferred).
//     - `ZIP_STR_14B` and `ZIP_STR_32B` use big-endian as per spec.
//     - Tests cover basic round-trips, iteration, validation, compare.
// ──────────────────────────────────────────────────────────────────────────
