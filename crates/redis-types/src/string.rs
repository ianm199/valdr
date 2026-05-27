//! `RedisString` — the canonical byte-string type for Redis keys,
//! values, and RESP payloads.
//!
//! Per PORTING.md §2 #3: a Vec<u8> newtype for now. Cheap interning
//! and Arc backing are architect decisions deferred until we measure
//! actual allocation pressure in Phase 4 (data-structure encodings).
//!
//! NEVER use `String` / `&str` / `from_utf8` for Redis data. Keys
//! and values are byte strings and must round-trip arbitrary bytes
//! through RESP.

use std::fmt;

#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct RedisString(Vec<u8>);

impl RedisString {
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    pub fn from_bytes(b: impl AsRef<[u8]>) -> Self {
        Self(b.as_ref().to_vec())
    }

    pub fn from_vec(v: Vec<u8>) -> Self {
        Self(v)
    }

    pub fn replace_from_slice(&mut self, b: &[u8]) {
        self.0.clear();
        self.0.extend_from_slice(b);
    }

    pub fn from_static(b: &'static [u8]) -> Self {
        Self(b.to_vec())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn extend_from_slice(&mut self, b: &[u8]) {
        self.0.extend_from_slice(b)
    }

    pub fn push(&mut self, byte: u8) {
        self.0.push(byte)
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    /// Byte-slice view (alias of `as_bytes`, named for compatibility with
    /// `Vec<u8>::as_slice` callers in translated command code).
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Owned `Vec<u8>` copy of the bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.clone()
    }

    /// Iterator over the bytes (alias of `as_bytes().iter()`).
    pub fn iter(&self) -> std::slice::Iter<'_, u8> {
        self.0.iter()
    }
}

impl std::ops::Deref for RedisString {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<[u8]> for RedisString {
    fn eq(&self, other: &[u8]) -> bool {
        self.0 == other
    }
}

impl PartialEq<&[u8]> for RedisString {
    fn eq(&self, other: &&[u8]) -> bool {
        self.0 == *other
    }
}

impl<const N: usize> PartialEq<[u8; N]> for RedisString {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.0 == other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for RedisString {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.0 == *other
    }
}

impl fmt::Debug for RedisString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "RedisString({:?})", s),
            Err(_) => write!(f, "RedisString({:?})", &self.0),
        }
    }
}

impl AsRef<[u8]> for RedisString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for RedisString {
    fn from(b: &[u8]) -> Self {
        Self::from_bytes(b)
    }
}

impl From<Vec<u8>> for RedisString {
    fn from(v: Vec<u8>) -> Self {
        Self::from_vec(v)
    }
}

impl From<&str> for RedisString {
    fn from(s: &str) -> Self {
        Self::from_bytes(s.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_bytes() {
        let s = RedisString::from_bytes(b"hello");
        assert_eq!(s.as_bytes(), b"hello");
        assert_eq!(s.len(), 5);
        assert!(!s.is_empty());
    }

    #[test]
    fn replace_from_slice_reuses_existing_string() {
        let mut s = RedisString::from_bytes(b"hello");
        s.replace_from_slice(b"bye");
        assert_eq!(s.as_bytes(), b"bye");
    }

    #[test]
    fn round_trip_non_utf8() {
        let bytes = vec![0xff, 0xfe, 0x00, 0x80];
        let s = RedisString::from_vec(bytes.clone());
        assert_eq!(s.as_bytes(), &bytes[..]);
    }

    #[test]
    fn debug_falls_back_for_non_utf8() {
        let s = RedisString::from_vec(vec![0xff, 0x00]);
        let dbg = format!("{:?}", s);
        assert!(dbg.starts_with("RedisString(["));
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;
        let a = RedisString::from_bytes(b"x");
        let b: RedisString = b"x".as_slice().into();
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (no direct C source — design decision in PORTING.md §2 #3)
//   target_crate:  redis-types
//   confidence:    high
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Vec<u8> newtype. Interning / Arc backing deferred to Phase 4 architect decision.
// ──────────────────────────────────────────────────────────────────────────
