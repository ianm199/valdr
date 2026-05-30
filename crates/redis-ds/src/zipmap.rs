//! `ZipMap` — compact byte-string-to-byte-string map optimised for small size.
//!
//! # On-wire format
//!
//! ```text
//! <zmlen><len>"key"<len><free>"value"...<0xFF>
//! ```
//!
//! - `zmlen` (1 byte): entry count when < 254; value 254 means "count not
//!   tracked" (≥ 254 entries).
//! - `<len>`: 1-byte encoding for lengths 0–253; 5-byte encoding (marker
//!   `0xFE` followed by a 4-byte little-endian `u32`) for lengths ≥ 254.
//! - `<free>` (1 byte): number of unused padding bytes trailing the value
//!   payload (result of in-place shrink updates).
//! - `0xFF`: end-of-map sentinel.
//!
//! Lookup is O(n) in entry count. The Hash type uses this encoding for hashes
//! below the listpack/hashtable promotion threshold.
//!
//! # Deviations from the C source
//!
//! The C API is pointer-based (`unsigned char *`). The Rust API is index-based
//! on an owned `Vec<u8>`, which eliminates raw-pointer arithmetic without
//! changing the observable behaviour.
//!
//! `zipmapRewind` / `zipmapNext` (raw-pointer cursor pair in C) are
//! translated to `ZipMap::rewind()` (returns a `usize` offset) and
//! `ZipMap::next_entry(offset)` (advances the offset). Callers loop until
//! `next_entry` returns `None`.

use redis_types::error::RedisError;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Lengths ≥ this sentinel use the 5-byte big-length encoding.
const ZIPMAP_BIGLEN: u8 = 254;

/// Byte value that signals the end of the zipmap.
const ZIPMAP_END: u8 = 255;

// ─── Public types ─────────────────────────────────────────────────────────────

/// A compact key→value map backed by a contiguous byte buffer.
///
/// Lookup is O(n) in the number of entries. Designed for small hashes where
/// memory density matters more than lookup speed.
#[derive(Debug, Clone)]
pub struct ZipMap {
    buf: Vec<u8>,
}

/// One key/value pair yielded by iterating through a [`ZipMap`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZipMapEntry {
    /// Decoded key bytes (no length prefix).
    pub key: Vec<u8>,
    /// Decoded value bytes (no length prefix, no free padding).
    pub value: Vec<u8>,
}

// ─── Private length-encoding helpers ─────────────────────────────────────────

/// Returns the number of bytes required to encode `len` in the zipmap
/// variable-length format: 1 byte for lengths < `ZIPMAP_BIGLEN`, 5 bytes
/// otherwise.
fn len_bytes_needed(len: u32) -> usize {
    if len < ZIPMAP_BIGLEN as u32 {
        1
    } else {
        5
    }
}

/// Decode the variable-length integer at `buf[offset]`.
///
/// Returns `(decoded_value, bytes_consumed)`.
fn decode_length(buf: &[u8], offset: usize) -> Result<(u32, usize), RedisError> {
    if offset >= buf.len() {
        return Err(RedisError::runtime(b"zipmap: decode_length out of bounds"));
    }
    let first = buf[offset];
    if first < ZIPMAP_BIGLEN {
        Ok((first as u32, 1))
    } else if first == ZIPMAP_BIGLEN {
        if offset + 5 > buf.len() {
            return Err(RedisError::runtime(
                b"zipmap: decode_length big-len encoding truncated",
            ));
        }
        let bytes = [
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
        ];
        // C uses memrev32ifbe which is a no-op on little-endian; u32::from_le_bytes
        // is the portable equivalent.
        let val = u32::from_le_bytes(bytes);
        Ok((val, 5))
    } else {
        // first == ZIPMAP_END (0xFF); callers must not invoke decode_length on
        // the end sentinel.
        Err(RedisError::runtime(
            b"zipmap: decode_length called on end sentinel",
        ))
    }
}

/// Encode `len` into `buf` starting at `buf[offset]`.
///
/// Returns the number of bytes written (1 or 5).
/// The caller must ensure `buf` has sufficient capacity.
#[allow(dead_code)] // legacy zipmap RDB-format helper; kept for RDB round-trip work
fn encode_length(buf: &mut [u8], offset: usize, len: u32) -> usize {
    if len < ZIPMAP_BIGLEN as u32 {
        buf[offset] = len as u8;
        1
    } else {
        buf[offset] = ZIPMAP_BIGLEN;
        let bytes = len.to_le_bytes();
        buf[offset + 1] = bytes[0];
        buf[offset + 2] = bytes[1];
        buf[offset + 3] = bytes[2];
        buf[offset + 4] = bytes[3];
        5
    }
}

/// Returns the byte count of the length encoding at `buf[offset]` (1 or 5).
fn encoded_length_size(buf: &[u8], offset: usize) -> usize {
    if buf[offset] < ZIPMAP_BIGLEN {
        1
    } else {
        5
    }
}

/// Returns the total number of bytes occupied by the key entry at `buf[offset]`
/// (length encoding + key data bytes).
fn raw_key_length(buf: &[u8], offset: usize) -> Result<usize, RedisError> {
    let (l, _) = decode_length(buf, offset)?;
    Ok(len_bytes_needed(l) + l as usize)
}

/// Returns the total number of bytes occupied by the value entry at `buf[offset]`
/// (length encoding + free byte + value data bytes + free padding bytes).
fn raw_value_length(buf: &[u8], offset: usize) -> Result<usize, RedisError> {
    let (l, _) = decode_length(buf, offset)?;
    let enc_size = len_bytes_needed(l);
    let free_byte_idx = offset + enc_size;
    if free_byte_idx >= buf.len() {
        return Err(RedisError::runtime(
            b"zipmap: raw_value_length free-byte out of bounds",
        ));
    }
    let free = buf[free_byte_idx] as usize;
    // enc_size + 1 (free byte field) + l (value data) + free (padding)
    Ok(enc_size + 1 + l as usize + free)
}

// ─── ZipMap implementation ────────────────────────────────────────────────────

impl ZipMap {
    /// Create a new, empty zipmap.
    ///
    /// The initial buffer is `[0x00, 0xFF]`: zmlen = 0, end sentinel.
    pub fn new() -> Self {
        ZipMap {
            buf: vec![0x00, ZIPMAP_END],
        }
    }

    /// Wrap an existing raw zipmap byte buffer.
    ///
    /// The buffer is taken as-is; use [`ZipMap::validate_integrity`] to check
    /// correctness before trusting the contents.
    pub fn from_raw(buf: Vec<u8>) -> Self {
        ZipMap { buf }
    }

    /// Return a reference to the raw underlying byte buffer.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Return the `zmlen` header byte.
    ///
    /// Values 0–253 are the exact entry count; 254 means the count is not
    /// tracked.
    pub fn zmlen(&self) -> u8 {
        self.buf[0]
    }

    /// Return the byte offset of the first entry, i.e., 1 (past the zmlen byte).
    ///
    /// Pass the returned offset to [`ZipMap::next_entry`] to begin iteration.
    pub fn rewind(&self) -> usize {
        1
    }

    /// Advance the iteration cursor `offset` by one entry, returning the
    /// decoded key/value pair and the next cursor position.
    ///
    /// Returns `Ok(None)` when the end sentinel `0xFF` is reached.
    /// Returns `Err` if the buffer is structurally malformed.
    ///
    /// # Usage
    ///
    /// ```rust,ignore
    /// let mut off = zm.rewind();
    /// while let Some((entry, next)) = zm.next_entry(off)? {
    ///     // use entry.key, entry.value
    ///     off = next;
    /// }
    /// ```
    pub fn next_entry(&self, offset: usize) -> Result<Option<(ZipMapEntry, usize)>, RedisError> {
        let buf = &self.buf;
        if offset >= buf.len() || buf[offset] == ZIPMAP_END {
            return Ok(None);
        }

        // ── Key ──────────────────────────────────────────────────────────────
        let (klen, kenc) = decode_length(buf, offset)?;
        let key_start = offset + kenc;
        let key_end = key_start + klen as usize;
        if key_end > buf.len() {
            return Err(RedisError::runtime(b"zipmap: key data out of bounds"));
        }
        let key = buf[key_start..key_end].to_vec();

        // Advance cursor past the full key entry (encoding + data).
        let val_offset = offset + raw_key_length(buf, offset)?;

        // ── Value ─────────────────────────────────────────────────────────────
        // Value entry layout: [<len_encoding>][<free_byte>][<data>][<padding>]
        //    *value += ZIPMAP_LEN_BYTES(*vlen);
        // The C sets *value = zm + 1, then adds ZIPMAP_LEN_BYTES. For 1-byte
        // encoding that is zm+2; for 5-byte encoding that is zm+6. Both land
        // on the first byte of actual value data (after the free byte).
        let (vlen, venc) = decode_length(buf, val_offset)?;
        let free_byte_idx = val_offset + venc;
        if free_byte_idx >= buf.len() {
            return Err(RedisError::runtime(
                b"zipmap: value free-byte out of bounds",
            ));
        }
        let val_start = free_byte_idx + 1; // skip the free byte
        let val_end = val_start + vlen as usize;
        if val_end > buf.len() {
            return Err(RedisError::runtime(b"zipmap: value data out of bounds"));
        }
        let value = buf[val_start..val_end].to_vec();

        // Advance cursor past the full value entry.
        let next_offset = val_offset + raw_value_length(buf, val_offset)?;

        Ok(Some((ZipMapEntry { key, value }, next_offset)))
    }

    /// Validate the structural integrity of a raw zipmap byte slice.
    ///
    /// When `deep` is `false`, only the minimum header (size ≥ 2) and end
    /// sentinel are verified. When `deep` is `true`, every entry's length
    /// fields are walked and cross-checked.
    ///
    /// Returns `true` if the buffer is well-formed.
    ///
    /// This is a static method so callers may validate before constructing a ZipMap.
    pub fn validate_integrity(zm: &[u8], deep: bool) -> bool {
        // Must have at least zmlen byte + end sentinel.
        if zm.len() < 2 {
            return false;
        }
        // Last byte must be the end sentinel.
        if zm[zm.len() - 1] != ZIPMAP_END {
            return false;
        }
        if !deep {
            return true;
        }

        let mut count: u32 = 0;
        // Start past the zmlen byte. C: unsigned char *p = zm + 1;
        let mut p: usize = 1;

        while p < zm.len() && zm[p] != ZIPMAP_END {
            // ── Key length encoding ───────────────────────────────────────────
            //    if (OUT_OF_RANGE(p + s)) return 0;
            let s = encoded_length_size(zm, p);
            if p + s >= zm.len() {
                return false;
            }

            //    if (l < ZIPMAP_BIGLEN && s != 1) return 0;
            let (l, _) = match decode_length(zm, p) {
                Ok(v) => v,
                Err(_) => return false,
            };
            if l < ZIPMAP_BIGLEN as u32 && s != 1 {
                return false;
            }

            //    if (OUT_OF_RANGE(p)) return 0;
            p += s;
            p += l as usize;
            if p >= zm.len() {
                return false;
            }

            // ── Value length encoding ─────────────────────────────────────────
            //    if (OUT_OF_RANGE(p + s)) return 0;
            let s = encoded_length_size(zm, p);
            if p + s >= zm.len() {
                return false;
            }

            //    if (l < ZIPMAP_BIGLEN && s != 1) return 0;
            let (l, _) = match decode_length(zm, p) {
                Ok(v) => v,
                Err(_) => return false,
            };
            if l < ZIPMAP_BIGLEN as u32 && s != 1 {
                return false;
            }

            p += s;
            if p >= zm.len() {
                return false;
            }
            let e = zm[p] as usize;
            p += 1; // consume the free-byte field
            p += l as usize + e; // skip value data + padding bytes
            count += 1;

            if p >= zm.len() {
                return false;
            }
        }

        // Must have found at least one entry.
        if count == 0 {
            return false;
        }

        // If the header count is tracked (not ZIPMAP_BIGLEN), it must match
        // the number of entries we walked.
        if zm[0] != ZIPMAP_BIGLEN && zm[0] as u32 != count {
            return false;
        }

        true
    }
}

impl Default for ZipMap {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_simple_zipmap() -> Vec<u8> {
        // Encodes {"foo": "bar"} — the example from the C file comment.
        // "\x01\x03foo\x03\x00bar\xff"
        vec![
            0x01, // zmlen = 1
            0x03, // key len = 3
            b'f', b'o', b'o', // key data
            0x03, // val len = 3
            0x00, // free = 0
            b'b', b'a', b'r', // val data
            0xFF, // ZIPMAP_END
        ]
    }

    #[test]
    fn validate_well_formed_shallow() {
        let zm = make_simple_zipmap();
        assert!(ZipMap::validate_integrity(&zm, false));
    }

    #[test]
    fn validate_well_formed_deep() {
        let zm = make_simple_zipmap();
        assert!(ZipMap::validate_integrity(&zm, true));
    }

    #[test]
    fn validate_rejects_too_short() {
        assert!(!ZipMap::validate_integrity(&[0x00], false));
        assert!(!ZipMap::validate_integrity(&[], false));
    }

    #[test]
    fn validate_rejects_missing_end_sentinel() {
        let zm = vec![0x01, 0x03, b'f', b'o', b'o', 0x03, 0x00, b'b', b'a', b'r'];
        assert!(!ZipMap::validate_integrity(&zm, false));
    }

    #[test]
    fn validate_rejects_empty_entries_deep() {
        // Header says 0 entries, only ZIPMAP_END — deep scan requires ≥ 1.
        let zm = vec![0x00, 0xFF];
        assert!(!ZipMap::validate_integrity(&zm, true));
    }

    #[test]
    fn validate_rejects_mismatched_header_count() {
        // zmlen says 2, but there is only 1 entry.
        let mut zm = make_simple_zipmap();
        zm[0] = 0x02;
        assert!(!ZipMap::validate_integrity(&zm, true));
    }

    #[test]
    fn iterate_single_entry() {
        let zm = ZipMap::from_raw(make_simple_zipmap());
        let off = zm.rewind();
        let (entry, next) = zm
            .next_entry(off)
            .expect("no error")
            .expect("entry expected");
        assert_eq!(entry.key, b"foo");
        assert_eq!(entry.value, b"bar");
        let end = zm.next_entry(next).expect("no error");
        assert!(end.is_none(), "expected end-of-map after single entry");
    }

    #[test]
    fn iterate_two_entries() {
        // {"foo": "bar", "hello": "world"}
        // "\x02\x03foo\x03\x00bar\x05hello\x05\x00world\xff"
        let zm_bytes: Vec<u8> = vec![
            0x02, 0x03, b'f', b'o', b'o', 0x03, 0x00, b'b', b'a', b'r', 0x05, b'h', b'e', b'l',
            b'l', b'o', 0x05, 0x00, b'w', b'o', b'r', b'l', b'd', 0xFF,
        ];
        let zm = ZipMap::from_raw(zm_bytes);
        let mut off = zm.rewind();
        let (e1, next1) = zm.next_entry(off).unwrap().unwrap();
        assert_eq!(e1.key, b"foo");
        assert_eq!(e1.value, b"bar");
        off = next1;
        let (e2, next2) = zm.next_entry(off).unwrap().unwrap();
        assert_eq!(e2.key, b"hello");
        assert_eq!(e2.value, b"world");
        off = next2;
        assert!(zm.next_entry(off).unwrap().is_none());
    }

    #[test]
    fn iterate_entry_with_free_bytes() {
        // Value has 2 free padding bytes after the payload.
        // key="k" (1 byte), val="v" (1 byte), free=2
        let zm_bytes: Vec<u8> = vec![
            0x01, 0x01, b'k', 0x01, 0x02, b'v', 0x00, 0x00, // free=2, 2 padding bytes
            0xFF,
        ];
        let zm = ZipMap::from_raw(zm_bytes);
        let off = zm.rewind();
        let (entry, _) = zm.next_entry(off).unwrap().unwrap();
        assert_eq!(entry.key, b"k");
        assert_eq!(entry.value, b"v"); // free bytes must not appear in value
    }

    #[test]
    fn new_zipmap_is_empty() {
        let zm = ZipMap::new();
        assert_eq!(zm.zmlen(), 0x00);
        assert_eq!(zm.as_bytes(), &[0x00, 0xFF]);
        assert!(zm.next_entry(zm.rewind()).unwrap().is_none());
    }

    #[test]
    fn len_bytes_needed_small() {
        assert_eq!(len_bytes_needed(0), 1);
        assert_eq!(len_bytes_needed(253), 1);
    }

    #[test]
    fn len_bytes_needed_big() {
        assert_eq!(len_bytes_needed(254), 5);
        assert_eq!(len_bytes_needed(u32::MAX), 5);
    }

    #[test]
    fn encode_decode_roundtrip_small() {
        let mut buf = vec![0u8; 1];
        let written = encode_length(&mut buf, 0, 42);
        assert_eq!(written, 1);
        let (val, consumed) = decode_length(&buf, 0).unwrap();
        assert_eq!(val, 42);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn encode_decode_roundtrip_big() {
        let mut buf = vec![0u8; 5];
        let written = encode_length(&mut buf, 0, 300);
        assert_eq!(written, 5);
        let (val, consumed) = decode_length(&buf, 0).unwrap();
        assert_eq!(val, 300);
        assert_eq!(consumed, 5);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/zipmap.c  (237 lines, 7 functions)
//   target_crate:  redis-ds
//   confidence:    high
//   todos:         0
//   port_notes:    2
//   unsafe_blocks: 0
//   notes:         raw-pointer cursor API translated to index-based ZipMap
//                  methods; encode_length takes &mut [u8] slice instead of
//                  the C dual-mode NULL-or-write pattern; memrev32ifbe
//                  replaced with u32::from_le_bytes / to_le_bytes
// ──────────────────────────────────────────────────────────────────────────
