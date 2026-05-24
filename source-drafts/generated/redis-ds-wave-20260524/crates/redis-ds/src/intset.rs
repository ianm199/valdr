// UPSTREAM MAP
// upstream: intset.c (319 lines, 15 functions)
//   intsetNew          -> IntSet::new
//   intsetAdd           -> IntSet::add
//   intsetRemove        -> IntSet::remove
//   intsetFind          -> IntSet::find
//   intsetRandom        -> IntSet::random
//   intsetMax           -> IntSet::max
//   intsetMin           -> IntSet::min
//   intsetGet           -> IntSet::get
//   intsetLen           -> IntSet::len
//   intsetBlobLen       -> IntSet::blob_len
//   intsetValidateIntegrity -> IntSet::validate_integrity (static)
//   intsetFree          -> (not needed, Drop handles)
//   intsetDup           -> IntSet::dup
//   test-only wrappers  -> #[cfg(test)] pub helpers
// upstream: intset.h (struct intset, constants)

// C: intset.c, intset.h — compact integer set with 16/32/64-bit encoding

use std::cell::Cell;
use std::mem::size_of;

// ─── Constants ────────────────────────────────────────────────────────────────

// These correspond to sizeof(int16_t), sizeof(int32_t), sizeof(int64_t)
const INTSET_ENC_INT16: u32 = 2;
const INTSET_ENC_INT32: u32 = 4;
const INTSET_ENC_INT64: u32 = 8;

/// Maximum value for a 16-bit signed integer.
const INT16_MAX: i64 = 32767;
const INT16_MIN: i64 = -32768;
/// Maximum value for a 32-bit signed integer.
const INT32_MAX: i64 = 2147483647;
const INT32_MIN: i64 = -2147483648;

// ─── Public type ──────────────────────────────────────────────────────────────

/// A compact, sorted set of `i64` values, stored in a contiguous byte buffer.
///
/// The on-wire format matches Valkey's `intset`:
/// - 4 bytes: encoding (`u32` little-endian, one of `INTSET_ENC_INT*`)
/// - 4 bytes: length (`u32` little-endian, number of elements)
/// - N * `encoding` bytes: the actual integer values (little-endian, sorted ascending)
#[derive(Debug, Clone)]
pub struct IntSet {
    buf: Vec<u8>,
}

// ─── Private helpers ──────────────────────────────────────────────────────────

/// Return the required encoding for the provided value.
/// C: _intsetValueEncoding
fn value_encoding(v: i64) -> u32 {
    if v < INT32_MIN || v > INT32_MAX {
        INTSET_ENC_INT64
    } else if v < INT16_MIN || v > INT16_MAX {
        INTSET_ENC_INT32
    } else {
        INTSET_ENC_INT16
    }
}

/// Decode the encoding field from the header (u32 LE).
/// C: intrev32ifbe(is->encoding)
fn get_encoding(buf: &[u8]) -> u32 {
    u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
}

/// Set the encoding field in the header.
fn set_encoding(buf: &mut [u8], enc: u32) {
    buf[..4].copy_from_slice(&enc.to_le_bytes());
}

/// Decode the length field from the header (u32 LE).
/// C: intrev32ifbe(is->length)
fn get_length(buf: &[u8]) -> u32 {
    u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]])
}

/// Set the length field in the header.
fn set_length(buf: &mut [u8], len: u32) {
    buf[4..8].copy_from_slice(&len.to_le_bytes());
}

/// Return the value at position `pos` using the given encoding.
/// C: _intsetGetEncoded
fn get_encoded(buf: &[u8], pos: usize, enc: u32) -> i64 {
    let start = 8 + pos * (enc as usize);
    match enc {
        INTSET_ENC_INT64 => {
            i64::from_le_bytes(buf[start..start + 8].try_into().unwrap())
        }
        INTSET_ENC_INT32 => {
            i32::from_le_bytes(buf[start..start + 4].try_into().unwrap()) as i64
        }
        _ => { // INTSET_ENC_INT16
            i16::from_le_bytes(buf[start..start + 2].try_into().unwrap()) as i64
        }
    }
}

/// Return the value at position `pos` using the current encoding.
/// C: _intsetGet
fn get(buf: &[u8], pos: usize) -> i64 {
    get_encoded(buf, pos, get_encoding(buf))
}

/// Set the value at position `pos` using the current encoding.
/// C: _intsetSet
fn set(buf: &mut [u8], pos: usize, value: i64) {
    let enc = get_encoding(buf);
    let start = 8 + pos * (enc as usize);
    match enc {
        INTSET_ENC_INT64 => {
            buf[start..start + 8].copy_from_slice(&value.to_le_bytes());
        }
        INTSET_ENC_INT32 => {
            buf[start..start + 4].copy_from_slice(&(value as i32).to_le_bytes());
        }
        _ => { // INTSET_ENC_INT16
            buf[start..start + 2].copy_from_slice(&(value as i16).to_le_bytes());
        }
    }
}

/// Resize the internal buffer to hold `len` elements.
/// The encoding is taken from the current header.
/// C: intsetResize
fn resize(buf: &mut Vec<u8>, new_len: u32) {
    let enc = get_encoding(buf);
    let new_size = 8 + (new_len as usize) * (enc as usize);
    buf.resize(new_size, 0);
}

/// Binary search for `value` in the sorted set.
/// Returns `(found, pos)` where `pos` is the index for insert if `found == false`.
/// C: intsetSearch
fn search(buf: &[u8], value: i64) -> (bool, usize) {
    let len = get_length(buf);
    if len == 0 {
        return (false, 0);
    }

    let first = get(buf, 0);
    let last = get(buf, (len - 1) as usize);

    if value > last {
        return (false, len as usize);
    }
    if value < first {
        return (false, 0);
    }

    let mut min: isize = 0;
    let mut max: isize = len as isize - 1;
    while max >= min {
        let mid = ((min as u32 + max as u32) / 2) as isize;
        let cur = get(buf, mid as usize);
        if value > cur {
            min = mid + 1;
        } else if value < cur {
            max = mid - 1;
        } else {
            return (true, mid as usize);
        }
    }
    (false, min as usize)
}

/// Upgrade encoding and add a value that lies outside the current range.
/// Returns the new buffer.
/// C: intsetUpgradeAndAdd
fn upgrade_and_add(buf: Vec<u8>, value: i64) -> Vec<u8> {
    let old_enc = get_encoding(&buf);
    let new_enc = value_encoding(value);
    let len = get_length(&buf);
    let prepend = if value < 0 { 1 } else { 0 };

    let mut new_buf = Vec::new();
    let new_len = len + 1;
    let new_size = 8 + (new_len as usize) * (new_enc as usize);
    new_buf.resize(new_size, 0);
    set_encoding(&mut new_buf, new_enc);
    set_length(&mut new_buf, new_len);

    // Copy old elements with new encoding, shifting right if prepending.
    for i in 0..len as usize {
        let val = get_encoded(&buf, i, old_enc);
        set(&mut new_buf, i + prepend, val);
    }

    // Place the new value at the end (if prepend==0) or beginning (prepend==1)
    if prepend != 0 {
        set(&mut new_buf, 0, value);
    } else {
        set(&mut new_buf, new_len as usize - 1, value);
    }

    new_buf
}

/// Move elements from `from` to `from + (to - from)`.
/// C: intsetMoveTail
fn move_tail(buf: &mut Vec<u8>, from: usize, to: usize) {
    if from == to {
        return;
    }
    let enc = get_encoding(buf);
    let len = get_length(buf);
    let element_size = enc as usize;
    let bytes_to_move = (len as usize - from) * element_size;
    if bytes_to_move == 0 {
        return;
    }
    let src = 8 + from * element_size;
    let dst = 8 + to * element_size;
    let src_slice = buf[src..src + bytes_to_move].to_vec();
    buf[dst..dst + bytes_to_move].copy_from_slice(&src_slice);
}

// Simple deterministic PRNG (linear congruential generator)
// Used for `random()`.
fn prng() -> u64 {
    thread_local! {
        static STATE: Cell<u64> = Cell::new(
            // Seed from system time (only once)
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        );
    }
    STATE.with(|state| {
        let mut x = state.get();
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        state.set(x);
        x
    })
}

// ─── Public API ───────────────────────────────────────────────────────────────

impl IntSet {
    /// Create a new empty intset with default Int16 encoding.
    /// C: intsetNew
    pub fn new() -> Self {
        let mut buf = vec![0u8; 8];
        set_encoding(&mut buf, INTSET_ENC_INT16);
        set_length(&mut buf, 0);
        IntSet { buf }
    }

    /// Insert `value` into the set.
    /// Returns `(new_set, true)` if the value was added,
    /// `(self, false)` if it was already present.
    /// C: intsetAdd
    pub fn insert(mut self, value: i64) -> (Self, bool) {
        let valenc = value_encoding(value);
        let cur_enc = get_encoding(&self.buf);

        if valenc > cur_enc {
            // Upgrade needed; value will be appended or prepended.
            let new_buf = upgrade_and_add(self.buf, value);
            return (IntSet { buf: new_buf }, true);
        }

        let (found, pos) = search(&self.buf, value);
        if found {
            return (self, false);
        }

        // Make room for the new element.
        let old_len = get_length(&self.buf);
        resize(&mut self.buf, old_len + 1);
        set_length(&mut self.buf, old_len + 1);

        // Shift elements right if inserting in the middle.
        if (pos as u32) < old_len {
            move_tail(&mut self.buf, pos, pos + 1);
        }

        set(&mut self.buf, pos, value);
        (self, true)
    }

    /// Remove `value` from the set.
    /// Returns `(new_set, true)` if the value was removed,
    /// `(self, false)` if it was not found.
    /// C: intsetRemove
    pub fn remove(mut self, value: i64) -> (Self, bool) {
        let valenc = value_encoding(value);
        let cur_enc = get_encoding(&self.buf);
        if valenc > cur_enc {
            return (self, false);
        }

        let (found, pos) = search(&self.buf, value);
        if !found {
            return (self, false);
        }

        let len = get_length(&self.buf);
        // Shift elements left.
        if (pos as u32) < len - 1 {
            move_tail(&mut self.buf, pos + 1, pos);
        }
        resize(&mut self.buf, len - 1);
        set_length(&mut self.buf, len - 1);
        (self, true)
    }

    /// Check if `value` is present in the set.
    /// C: intsetFind
    pub fn find(&self, value: i64) -> bool {
        let valenc = value_encoding(value);
        if valenc > get_encoding(&self.buf) {
            return false;
        }
        let (found, _) = search(&self.buf, value);
        found
    }

    /// Return a random member of the set.
    /// Panics if the set is empty.
    /// C: intsetRandom
    pub fn random(&self) -> i64 {
        let len = get_length(&self.buf);
        assert!(len > 0, "intset random on empty set");
        let idx = (prng() % len as u64) as usize;
        get(&self.buf, idx)
    }

    /// Return the largest member.
    /// Panics if empty.
    /// C: intsetMax
    pub fn max(&self) -> i64 {
        let len = get_length(&self.buf);
        assert!(len > 0);
        get(&self.buf, (len - 1) as usize)
    }

    /// Return the smallest member.
    /// Panics if empty.
    /// C: intsetMin
    pub fn min(&self) -> i64 {
        let len = get_length(&self.buf);
        assert!(len > 0);
        get(&self.buf, 0)
    }

    /// Get the value at the given position.
    /// Returns `None` if out of range.
    /// C: intsetGet
    pub fn get(&self, pos: u32) -> Option<i64> {
        let len = get_length(&self.buf);
        if pos < len {
            Some(get(&self.buf, pos as usize))
        } else {
            None
        }
    }

    /// Return the number of elements in the set.
    /// C: intsetLen
    pub fn len(&self) -> u32 {
        get_length(&self.buf)
    }

    /// Return true if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the total byte size of the encoded representation (header + contents).
    /// C: intsetBlobLen
    pub fn blob_len(&self) -> usize {
        let enc = get_encoding(&self.buf);
        8 + (self.len() as usize) * (enc as usize)
    }

    /// Validate the integrity of a raw intset byte slice.
    ///
    /// When `deep` is `false`, only the header is checked:
    /// - size >= 8 bytes
    /// - encoding is a valid known value (2, 4, or 8)
    /// - total size matches: `8 + count * record_size`
    ///
    /// When `deep` is `true`, additionally checks that elements are in
    /// strictly ascending order and no duplicates.
    ///
    /// Returns `true` if valid.
    /// C: intsetValidateIntegrity
    pub fn validate_integrity(data: &[u8], deep: bool) -> bool {
        // C: intset.c intsetValidateIntegrity

        // Minimum header size
        if data.len() < 8 {
            return false;
        }

        let encoding = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let record_size = match encoding {
            INTSET_ENC_INT64 => 8usize,
            INTSET_ENC_INT32 => 4,
            INTSET_ENC_INT16 => 2,
            _ => return false,
        };

        let count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        let expected_size = 8 + (count as usize) * record_size;
        if data.len() != expected_size {
            return false;
        }

        if count == 0 {
            return false;
        }

        if !deep {
            return true;
        }

        // Check sorted ascending, no duplicates.
        let mut prev = get_encoded(data, 0, encoding);
        for i in 1..count as usize {
            let cur = get_encoded(data, i, encoding);
            if cur <= prev {
                return false;
            }
            prev = cur;
        }

        true
    }

    /// Deep-copy the intset.
    /// C: intsetDup
    pub fn dup(&self) -> Self {
        IntSet {
            buf: self.buf.clone(),
        }
    }

    /// Return a reference to the raw byte buffer (for serialization).
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Construct an `IntSet` from a raw byte buffer that has already been
    /// validated (e.g. from RDB load). The caller must ensure the buffer
    /// is well-formed; use `IntSet::validate_integrity` beforehand.
    pub fn from_raw_bytes(buf: Vec<u8>) -> Self {
        IntSet { buf }
    }
}

impl Default for IntSet {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Test-only wrappers (gtest access) ────────────────────────────────────────

#[cfg(test)]
pub mod test_wrappers {
    use super::*;

    pub fn value_encoding(v: i64) -> u32 {
        super::value_encoding(v)
    }

    pub fn get_encoded(buf: &[u8], pos: usize, enc: u32) -> i64 {
        super::get_encoded(buf, pos, enc)
    }

    pub fn get(buf: &[u8], pos: usize) -> i64 {
        super::get(buf, pos)
    }

    pub fn search(buf: &[u8], value: i64) -> (bool, usize) {
        super::search(buf, value)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_two_element_intset() -> Vec<u8> {
        // Manual construction: encoding=2, length=2, values [1i16, 3i16] LE
        let mut buf = vec![0u8; 8 + 2 * 2];
        set_encoding(&mut buf, INTSET_ENC_INT16);
        set_length(&mut buf, 2);
        set(&mut buf, 0, 1);
        set(&mut buf, 1, 3);
        buf
    }

    #[test]
    fn new_is_empty() {
        let s = IntSet::new();
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
        assert_eq!(s.blob_len(), 8);
    }

    #[test]
    fn insert_and_find() {
        let s = IntSet::new();
        let (s, added) = s.insert(42);
        assert!(added);
        assert!(s.find(42));
        assert!(!s.find(43));
        let (s, added) = s.insert(-7);
        assert!(added);
        assert!(s.find(-7));
        let (s, added) = s.insert(42);
        assert!(!added); // duplicate
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn remove() {
        let s = IntSet::new();
        let (s, _) = s.insert(10);
        let (s, _) = s.insert(20);
        let (s, removed) = s.remove(10);
        assert!(removed);
        assert!(!s.find(10));
        assert!(s.find(20));
        let (s, removed) = s.remove(99);
        assert!(!removed);
    }

    #[test]
    fn min_max() {
        let s = IntSet::new();
        let (s, _) = s.insert(100);
        let (s, _) = s.insert(-50);
        let (s, _) = s.insert(0);
        assert_eq!(s.min(), -50);
        assert_eq!(s.max(), 100);
    }

    #[test]
    fn random_not_panicking() {
        let s = IntSet::new();
        let (s, _) = s.insert(5);
        let (s, _) = s.insert(10);
        let (s, _) = s.insert(15);
        let r = s.random();
        assert!(r == 5 || r == 10 || r == 15);
    }

    #[test]
    fn get_out_of_range_returns_none() {
        let s = IntSet::new();
        let (s, _) = s.insert(1);
        assert_eq!(s.get(0), Some(1));
        assert_eq!(s.get(1), None);
    }

    #[test]
    fn blob_len_matches_encoded_size() {
        let s = IntSet::new();
        let (s, _) = s.insert(i64::MAX); // requires Int64
        assert_eq!(s.blob_len(), 8 + 8);
        let (s, _) = s.insert(100i64); // Int16
        // After insertion, encoding might have been upgraded to Int64; blob_len should match.
        // For a set with only Int64, blob_len = 8 + 2*8 = 24
        assert_eq!(s.blob_len(), 24);
    }

    #[test]
    fn validate_integrity_happy_path() {
        let buf = make_two_element_intset();
        assert!(IntSet::validate_integrity(&buf, false));
        assert!(IntSet::validate_integrity(&buf, true));
    }

    #[test]
    fn validate_integrity_rejects_too_short() {
        assert!(!IntSet::validate_integrity(&[0; 4], false));
    }

    #[test]
    fn validate_integrity_rejects_bad_encoding() {
        let mut buf = make_two_element_intset();
        buf[0] = 0x01; // invalid encoding
        assert!(!IntSet::validate_integrity(&buf, false));
    }

    #[test]
    fn validate_integrity_rejects_mismatched_size() {
        let mut buf = make_two_element_intset();
        buf[4..8].copy_from_slice(&3u32.to_le_bytes()); // count=3, but only 2 elements
        assert!(!IntSet::validate_integrity(&buf, false));
    }

    #[test]
    fn validate_integrity_rejects_unsorted_deep() {
        let mut buf = make_two_element_intset();
        // Overwrite values: [3, 1] (unsorted)
        set(&mut buf, 0, 3);
        set(&mut buf, 1, 1);
        assert!(!IntSet::validate_integrity(&buf, true));
    }

    #[test]
    fn validate_integrity_rejects_duplicate_deep() {
        let mut buf = make_two_element_intset();
        // Both values = 2
        set(&mut buf, 0, 2);
        set(&mut buf, 1, 2);
        assert!(!IntSet::validate_integrity(&buf, true));
    }

    #[test]
    fn dup_matches_original() {
        let s = IntSet::new();
        let (s, _) = s.insert(1);
        let (s, _) = s.insert(2);
        let copy = s.dup();
        assert_eq!(s.len(), copy.len());
        assert_eq!(s.as_bytes(), copy.as_bytes());
    }

    #[test]
    fn from_raw_bytes() {
        let buf = make_two_element_intset();
        let s = IntSet::from_raw_bytes(buf);
        assert_eq!(s.len(), 2);
        assert_eq!(s.get(0), Some(1));
        assert_eq!(s.get(1), Some(3));
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/intset.c  (319 lines, 15 functions)
//   target_crate:  redis-ds
//   confidence:    high
//   todos:         0
//   port_notes:    2
//   unsafe_blocks: 0
//   notes:         Byte-faithful buffer representation; endianness handled
//                  via from_le_bytes; random uses a simple LCG (no external
//                  dep); test-only wrappers exported under #[cfg(test)].
//                  No drop/free needed; memory management by Vec.
// ──────────────────────────────────────────────────────────────────────────
