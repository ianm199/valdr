//! `IntSet` - sorted contiguous-buffer encoding for integer sets.
//!
//! The blob layout is byte-compatible: `[encoding:u32-le][length:u32-le][contents...]`,
//! where each content value is a signed little-endian 16-, 32-, or 64-bit integer
//! and the array is sorted.

use std::cell::Cell;
use std::time::{SystemTime, UNIX_EPOCH};

const HEADER_LEN: usize = 8;

pub const INTSET_ENC_INT16: u32 = 2;
pub const INTSET_ENC_INT32: u32 = 4;
pub const INTSET_ENC_INT64: u32 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Encoding {
    Int16,
    Int32,
    Int64,
}

impl Encoding {
    const fn tag(self) -> u32 {
        match self {
            Self::Int16 => INTSET_ENC_INT16,
            Self::Int32 => INTSET_ENC_INT32,
            Self::Int64 => INTSET_ENC_INT64,
        }
    }

    const fn width(self) -> usize {
        self.tag() as usize
    }

    fn from_tag(tag: u32) -> Option<Self> {
        match tag {
            INTSET_ENC_INT16 => Some(Self::Int16),
            INTSET_ENC_INT32 => Some(Self::Int32),
            INTSET_ENC_INT64 => Some(Self::Int64),
            _ => None,
        }
    }

    fn for_value(value: i64) -> Self {
        if value < i32::MIN as i64 || value > i32::MAX as i64 {
            Self::Int64
        } else if value < i16::MIN as i64 || value > i16::MAX as i64 {
            Self::Int32
        } else {
            Self::Int16
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntSet {
    buf: Vec<u8>,
}

impl IntSet {
    /// Create a new empty intset with the default 16-bit encoding.
    pub fn new() -> Self {
        let mut buf = vec![0; HEADER_LEN];
        write_u32_le(&mut buf, 0, INTSET_ENC_INT16);
        write_u32_le(&mut buf, 4, 0);
        Self { buf }
    }

    /// Insert `value`. Returns `true` when the value was added.
    pub fn add(&mut self, value: i64) -> bool {
        let value_encoding = Encoding::for_value(value);
        let current_encoding = self.encoding();

        if value_encoding > current_encoding {
            return self.upgrade_and_add(value, value_encoding);
        }

        let (found, pos) = self.search(value);
        if found {
            return false;
        }

        let old_len = self.len_u32();
        let Some(new_len) = old_len.checked_add(1) else {
            return false;
        };
        if !self.resize_contents(new_len) {
            return false;
        }

        if pos < old_len {
            let moved = move_tail(
                &mut self.buf,
                current_encoding,
                old_len,
                pos as usize,
                pos as usize + 1,
            );
            if !moved {
                return false;
            }
        }

        if !write_value(&mut self.buf, pos as usize, current_encoding, value) {
            return false;
        }
        self.set_len(new_len);
        true
    }

    /// Source-draft compatibility alias. Prefer `add` in new code.
    pub fn insert(mut self, value: i64) -> (Self, bool) {
        let added = self.add(value);
        (self, added)
    }

    /// Remove `value`. Returns `true` when an existing value was removed.
    pub fn remove(&mut self, value: i64) -> bool {
        let value_encoding = Encoding::for_value(value);
        let current_encoding = self.encoding();
        if value_encoding > current_encoding {
            return false;
        }

        let (found, pos) = self.search(value);
        if !found {
            return false;
        }

        let old_len = self.len_u32();
        if pos < old_len.saturating_sub(1) {
            let moved = move_tail(
                &mut self.buf,
                current_encoding,
                old_len,
                pos as usize + 1,
                pos as usize,
            );
            if !moved {
                return false;
            }
        }

        let new_len = old_len.saturating_sub(1);
        if !self.resize_contents(new_len) {
            return false;
        }
        self.set_len(new_len);
        true
    }

    /// Determine whether `value` belongs to this intset.
    pub fn find(&self, value: i64) -> bool {
        if Encoding::for_value(value) > self.encoding() {
            return false;
        }
        self.search(value).0
    }

    /// Return a pseudo-random member, or `None` for an empty intset.
    pub fn random(&self) -> Option<i64> {
        let len = self.len_u32();
        if len == 0 {
            return None;
        }
        let idx = (next_random_u64() % len as u64) as u32;
        self.get(idx)
    }

    /// Return the largest member, or `None` when empty.
    pub fn max(&self) -> Option<i64> {
        let len = self.len_u32();
        if len == 0 {
            None
        } else {
            self.get(len - 1)
        }
    }

    /// Return the smallest member, or `None` when empty.
    pub fn min(&self) -> Option<i64> {
        self.get(0)
    }

    /// Return the value at `pos`, or `None` when out of range.
    pub fn get(&self, pos: u32) -> Option<i64> {
        if pos >= self.len_u32() {
            return None;
        }
        read_value(&self.buf, pos as usize, self.encoding())
    }

    /// Number of elements in the intset.
    pub fn len(&self) -> usize {
        self.len_u32() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len_u32() == 0
    }

    /// Total byte length of the encoded blob.
    pub fn blob_len(&self) -> usize {
        self.buf.len()
    }

    /// Validate a raw intset blob.
    pub fn validate_integrity(data: &[u8], deep: bool) -> bool {
        if data.len() < HEADER_LEN {
            return false;
        }

        let Some(encoding_tag) = read_u32_le(data, 0) else {
            return false;
        };
        let Some(encoding) = Encoding::from_tag(encoding_tag) else {
            return false;
        };
        let Some(count) = read_u32_le(data, 4) else {
            return false;
        };
        let Some(expected_size) = blob_len_for(count, encoding) else {
            return false;
        };
        if data.len() != expected_size {
            return false;
        }

        // Empty live intsets are valid during construction, but empty serialized
        // payloads are rejected.
        if count == 0 {
            return false;
        }

        if !deep {
            return true;
        }

        let Some(mut prev) = read_value(data, 0, encoding) else {
            return false;
        };
        for pos in 1..count as usize {
            let Some(cur) = read_value(data, pos, encoding) else {
                return false;
            };
            if cur <= prev {
                return false;
            }
            prev = cur;
        }

        true
    }

    /// Construct an intset from a raw blob after deep validation.
    pub fn from_raw_bytes(buf: Vec<u8>) -> Option<Self> {
        if Self::validate_integrity(&buf, true) {
            Some(Self { buf })
        } else {
            None
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Deep-copy helper.
    pub fn dup(&self) -> Self {
        self.clone()
    }

    fn encoding(&self) -> Encoding {
        let Some(tag) = read_u32_le(&self.buf, 0) else {
            return Encoding::Int16;
        };
        match Encoding::from_tag(tag) {
            Some(encoding) => encoding,
            None => Encoding::Int16,
        }
    }

    fn len_u32(&self) -> u32 {
        read_u32_le(&self.buf, 4).unwrap_or_default()
    }

    fn set_len(&mut self, len: u32) {
        write_u32_le(&mut self.buf, 4, len);
    }

    fn resize_contents(&mut self, len: u32) -> bool {
        let Some(new_size) = blob_len_for(len, self.encoding()) else {
            return false;
        };
        self.buf.resize(new_size, 0);
        true
    }

    fn search(&self, value: i64) -> (bool, u32) {
        let len = self.len_u32();
        if len == 0 {
            return (false, 0);
        }

        let Some(first) = self.get(0) else {
            return (false, 0);
        };
        let Some(last) = self.get(len - 1) else {
            return (false, len);
        };
        if value > last {
            return (false, len);
        }
        if value < first {
            return (false, 0);
        }

        let mut min: i64 = 0;
        let mut max: i64 = len as i64 - 1;
        while max >= min {
            let mid = ((min as u64 + max as u64) >> 1) as i64;
            let Some(cur) = self.get(mid as u32) else {
                return (false, len);
            };
            if value > cur {
                min = mid + 1;
            } else if value < cur {
                max = mid - 1;
            } else {
                return (true, mid as u32);
            }
        }

        (false, min as u32)
    }

    fn upgrade_and_add(&mut self, value: i64, new_encoding: Encoding) -> bool {
        let old_encoding = self.encoding();
        let old_len = self.len_u32();
        let Some(new_len) = old_len.checked_add(1) else {
            return false;
        };
        let Some(new_size) = blob_len_for(new_len, new_encoding) else {
            return false;
        };

        let old_buf = std::mem::replace(&mut self.buf, vec![0; new_size]);
        write_u32_le(&mut self.buf, 0, new_encoding.tag());
        self.set_len(new_len);

        let prepend = value < 0;
        let shift = if prepend { 1 } else { 0 };
        for src in 0..old_len as usize {
            let Some(old_value) = read_value(&old_buf, src, old_encoding) else {
                return false;
            };
            if !write_value(&mut self.buf, src + shift, new_encoding, old_value) {
                return false;
            }
        }

        let dst = if prepend { 0 } else { old_len as usize };
        write_value(&mut self.buf, dst, new_encoding, value)
    }
}

impl Default for IntSet {
    fn default() -> Self {
        Self::new()
    }
}

fn read_u32_le(buf: &[u8], offset: usize) -> Option<u32> {
    let bytes = buf.get(offset..offset.checked_add(4)?)?;
    Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn write_u32_le(buf: &mut [u8], offset: usize, value: u32) -> bool {
    let Some(end) = offset.checked_add(4) else {
        return false;
    };
    let Some(dst) = buf.get_mut(offset..end) else {
        return false;
    };
    dst.copy_from_slice(&value.to_le_bytes());
    true
}

fn blob_len_for(len: u32, encoding: Encoding) -> Option<usize> {
    HEADER_LEN.checked_add((len as usize).checked_mul(encoding.width())?)
}

fn element_offset(pos: usize, encoding: Encoding) -> Option<usize> {
    HEADER_LEN.checked_add(pos.checked_mul(encoding.width())?)
}

fn read_value(buf: &[u8], pos: usize, encoding: Encoding) -> Option<i64> {
    let offset = element_offset(pos, encoding)?;
    match encoding {
        Encoding::Int16 => {
            let bytes = buf.get(offset..offset.checked_add(2)?)?;
            Some(i16::from_le_bytes([bytes[0], bytes[1]]) as i64)
        }
        Encoding::Int32 => {
            let bytes = buf.get(offset..offset.checked_add(4)?)?;
            Some(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64)
        }
        Encoding::Int64 => {
            let bytes = buf.get(offset..offset.checked_add(8)?)?;
            Some(i64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))
        }
    }
}

fn write_value(buf: &mut [u8], pos: usize, encoding: Encoding, value: i64) -> bool {
    let Some(offset) = element_offset(pos, encoding) else {
        return false;
    };
    match encoding {
        Encoding::Int16 => {
            let Some(end) = offset.checked_add(2) else {
                return false;
            };
            let Some(dst) = buf.get_mut(offset..end) else {
                return false;
            };
            dst.copy_from_slice(&(value as i16).to_le_bytes());
            true
        }
        Encoding::Int32 => {
            let Some(end) = offset.checked_add(4) else {
                return false;
            };
            let Some(dst) = buf.get_mut(offset..end) else {
                return false;
            };
            dst.copy_from_slice(&(value as i32).to_le_bytes());
            true
        }
        Encoding::Int64 => {
            let Some(end) = offset.checked_add(8) else {
                return false;
            };
            let Some(dst) = buf.get_mut(offset..end) else {
                return false;
            };
            dst.copy_from_slice(&value.to_le_bytes());
            true
        }
    }
}

fn move_tail(buf: &mut [u8], encoding: Encoding, len: u32, from: usize, to: usize) -> bool {
    if from == to {
        return true;
    }
    let len = len as usize;
    if from > len {
        return false;
    }

    let width = encoding.width();
    let count = len - from;
    let Some(bytes) = count.checked_mul(width) else {
        return false;
    };
    if bytes == 0 {
        return true;
    }

    let Some(src) = element_offset(from, encoding) else {
        return false;
    };
    let Some(dst) = element_offset(to, encoding) else {
        return false;
    };
    let Some(src_end) = src.checked_add(bytes) else {
        return false;
    };
    let Some(dst_end) = dst.checked_add(bytes) else {
        return false;
    };
    if src_end > buf.len() || dst_end > buf.len() {
        return false;
    }

    buf.copy_within(src..src_end, dst);
    true
}

fn seed_random() -> u64 {
    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos() as u64,
        Err(_) => 0x9e37_79b9_7f4a_7c15,
    };
    if nanos == 0 {
        0xa076_1d64_78bd_642f
    } else {
        nanos
    }
}

fn next_random_u64() -> u64 {
    thread_local! {
        static STATE: Cell<u64> = Cell::new(seed_random());
    }

    STATE.with(|state| {
        let mut x = state.get();
        if x == 0 {
            x = 0xa076_1d64_78bd_642f;
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        state.set(x);
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_encoding(set: &IntSet, encoding: u32) {
        assert_eq!(read_u32_le(set.as_bytes(), 0), Some(encoding));
    }

    fn make_raw(values: &[i64], encoding: Encoding) -> Vec<u8> {
        let len = values.len() as u32;
        let size = match blob_len_for(len, encoding) {
            Some(size) => size,
            None => HEADER_LEN,
        };
        let mut buf = vec![0; size];
        write_u32_le(&mut buf, 0, encoding.tag());
        write_u32_le(&mut buf, 4, len);
        for (idx, value) in values.iter().enumerate() {
            assert!(write_value(&mut buf, idx, encoding, *value));
        }
        buf
    }

    #[test]
    fn intset_new_has_valkey_empty_header() {
        let set = IntSet::new();
        assert_eq!(set.len(), 0);
        assert!(set.is_empty());
        assert_eq!(set.blob_len(), HEADER_LEN);
        assert_eq!(
            set.as_bytes(),
            &[INTSET_ENC_INT16 as u8, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn intset_add_keeps_values_sorted_and_deduplicated() {
        let mut set = IntSet::new();
        assert!(set.add(5));
        assert!(set.add(-1));
        assert!(set.add(3));
        assert!(!set.add(5));

        assert_eq!(set.len(), 3);
        assert_eq!(set.get(0), Some(-1));
        assert_eq!(set.get(1), Some(3));
        assert_eq!(set.get(2), Some(5));
        assert_eq!(set.get(3), None);
        assert!(set.find(5));
        assert!(!set.find(4));
    }

    #[test]
    fn intset_insert_alias_returns_updated_set() {
        let (set, added) = IntSet::new().insert(11);
        assert!(added);
        assert_eq!(set.get(0), Some(11));
    }

    #[test]
    fn intset_upgrade_appends_positive_out_of_range_value() {
        let mut set = IntSet::new();
        assert!(set.add(1));
        assert!(set.add(i16::MAX as i64 + 1));

        assert_encoding(&set, INTSET_ENC_INT32);
        assert_eq!(set.len(), 2);
        assert_eq!(set.get(0), Some(1));
        assert_eq!(set.get(1), Some(i16::MAX as i64 + 1));
        assert_eq!(set.blob_len(), HEADER_LEN + 2 * 4);
    }

    #[test]
    fn intset_upgrade_prepends_negative_out_of_range_value() {
        let mut set = IntSet::new();
        assert!(set.add(1));
        assert!(set.add(i16::MIN as i64 - 1));

        assert_encoding(&set, INTSET_ENC_INT32);
        assert_eq!(set.get(0), Some(i16::MIN as i64 - 1));
        assert_eq!(set.get(1), Some(1));
    }

    #[test]
    fn intset_upgrade_to_i64_preserves_existing_values() {
        let mut set = IntSet::new();
        assert!(set.add(i32::MIN as i64));
        assert!(set.add(i64::MAX));

        assert_encoding(&set, INTSET_ENC_INT64);
        assert_eq!(set.get(0), Some(i32::MIN as i64));
        assert_eq!(set.get(1), Some(i64::MAX));
    }

    #[test]
    fn intset_remove_shifts_tail_and_keeps_encoding() {
        let mut set = IntSet::new();
        assert!(set.add(1));
        assert!(set.add(2));
        assert!(set.add(3));
        assert!(set.remove(2));
        assert!(!set.remove(42));

        assert_eq!(set.len(), 2);
        assert_eq!(set.get(0), Some(1));
        assert_eq!(set.get(1), Some(3));
        assert_eq!(set.get(2), None);
    }

    #[test]
    fn intset_min_max_random_are_non_panicking() {
        let mut set = IntSet::new();
        assert_eq!(set.min(), None);
        assert_eq!(set.max(), None);
        assert_eq!(set.random(), None);

        assert!(set.add(10));
        assert!(set.add(-10));
        assert!(set.add(0));

        assert_eq!(set.min(), Some(-10));
        assert_eq!(set.max(), Some(10));
        let random = set.random();
        assert!(matches!(random, Some(-10) | Some(0) | Some(10)));
    }

    #[test]
    fn intset_validate_integrity_matches_valkey_header_rules() {
        let raw = make_raw(&[1, 3], Encoding::Int16);
        assert!(IntSet::validate_integrity(&raw, false));
        assert!(IntSet::validate_integrity(&raw, true));

        assert!(!IntSet::validate_integrity(&raw[..4], false));

        let mut bad_encoding = raw.clone();
        write_u32_le(&mut bad_encoding, 0, 1);
        assert!(!IntSet::validate_integrity(&bad_encoding, false));

        let mut bad_size = raw.clone();
        write_u32_le(&mut bad_size, 4, 3);
        assert!(!IntSet::validate_integrity(&bad_size, false));

        let empty = IntSet::new();
        assert!(!IntSet::validate_integrity(empty.as_bytes(), false));
    }

    #[test]
    fn intset_validate_integrity_deep_rejects_unsorted_or_duplicate_values() {
        let unsorted = make_raw(&[3, 1], Encoding::Int16);
        assert!(IntSet::validate_integrity(&unsorted, false));
        assert!(!IntSet::validate_integrity(&unsorted, true));

        let duplicate = make_raw(&[2, 2], Encoding::Int16);
        assert!(IntSet::validate_integrity(&duplicate, false));
        assert!(!IntSet::validate_integrity(&duplicate, true));
    }

    #[test]
    fn intset_from_raw_bytes_requires_deep_valid_blob() {
        let raw = make_raw(&[-7, 9], Encoding::Int16);
        let set = IntSet::from_raw_bytes(raw.clone());
        assert!(set.is_some());
        if let Some(set) = set {
            assert_eq!(set.as_bytes(), raw.as_slice());
            assert_eq!(set.get(0), Some(-7));
            assert_eq!(set.get(1), Some(9));
        }

        assert!(IntSet::from_raw_bytes(make_raw(&[4, 4], Encoding::Int16)).is_none());
    }

    #[test]
    fn intset_dup_copies_the_encoded_blob() {
        let mut set = IntSet::new();
        assert!(set.add(1));
        assert!(set.add(2));
        let copy = set.dup();
        assert_eq!(set, copy);
        assert_eq!(set.as_bytes(), copy.as_bytes());
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        (sorted integer set, Redis stdlib)
//   target_crate:  redis-ds
//   confidence:    high
//   todos:         0
//   port_notes:    2
//   unsafe_blocks: 0
//   notes:         Safe byte-buffer implementation with little-endian blob layout
//                  preserved. Public random/min/max are non-panicking Option APIs.
// ──────────────────────────────────────────────────────────────────────────
