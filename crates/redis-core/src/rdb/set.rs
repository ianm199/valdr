//! RDB set type serialization — Round 21.
//!
//! Implements `save_set_object` and `load_set_object` for `RDB_TYPE_SET`
//! (the flat-string-array wire form, type byte 0x02).
//!
//! Wire layout after the type byte:
//!   - `save_len(N)` — number of members
//!   - For each member: raw-byte string (length-prefixed)
//!
//! Design decision: we always emit `RDB_TYPE_SET` regardless of set size.
//! C Valkey loads this form for sets of any size without error. The compact
//! forms that C Valkey emits for small sets — `RDB_TYPE_SET_LISTPACK` (20)
//! and `RDB_TYPE_SET_INTSET` (11) — are NOT supported by us in Phase 1.
//! The oracle corpus tests coerce C Valkey into the flat form via:
//!   CONFIG SET set-max-intset-entries 0
//!   CONFIG SET set-max-listpack-entries 0
//!
//! Load compatibility:
//!   - `RDB_TYPE_SET` (2)         — fully handled
//!   - `RDB_TYPE_SET_INTSET` (11) — graceful Unsupported error
//!   - `RDB_TYPE_SET_LISTPACK` (20) — graceful Unsupported error

use std::collections::HashSet;
use std::io::{self, Read, Write};

use redis_types::RedisString;

use crate::object::RedisObject;

use super::header::{read_rdb_string, write_rdb_string};
use super::varint::{load_len, write_len};

/// Serialize an `RDB_TYPE_SET` value payload.
///
/// The type byte is written by the caller; this function writes the member
/// count followed by each member as a raw-byte length-prefixed string.
pub fn save_set_object(w: &mut impl Write, obj: &RedisObject) -> io::Result<()> {
    let set = obj.set().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "save_set_object called on non-set object")
    })?;
    write_len(w, set.len() as u64)?;
    for member in set {
        write_rdb_string(w, member.as_bytes())?;
    }
    Ok(())
}

/// Deserialize an `RDB_TYPE_SET` value payload, producing a `RedisObject`.
///
/// Reads from `r` starting immediately after the type byte.
pub fn load_set_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let (n, _is_encoded) = load_len(r)?;
    let mut members: HashSet<RedisString> = HashSet::with_capacity(n as usize);
    for _ in 0..n {
        let member_bytes = read_rdb_string(r)?;
        members.insert(RedisString::from_vec(member_bytes));
    }
    Ok(RedisObject::new_set_from_set(members))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(members: &[&str]) -> HashSet<RedisString> {
        let mut set: HashSet<RedisString> = HashSet::new();
        for m in members {
            set.insert(RedisString::from_bytes(m.as_bytes()));
        }
        let obj = RedisObject::new_set_from_set(set);

        let mut buf: Vec<u8> = Vec::new();
        save_set_object(&mut buf, &obj).unwrap();
        let mut cursor = Cursor::new(&buf);
        let loaded = load_set_object(&mut cursor).unwrap();
        loaded.set().unwrap().clone()
    }

    #[test]
    fn empty_set_roundtrip() {
        let result = roundtrip(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn single_member_roundtrip() {
        let result = roundtrip(&["hello"]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&RedisString::from_bytes(b"hello")));
    }

    #[test]
    fn multi_member_roundtrip() {
        let members = ["alpha", "beta", "gamma", "delta"];
        let result = roundtrip(&members);
        assert_eq!(result.len(), 4);
        for m in &members {
            assert!(result.contains(&RedisString::from_bytes(m.as_bytes())));
        }
    }

    #[test]
    fn deduplication_preserved() {
        let result = roundtrip(&["dup", "dup", "unique"]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn binary_member_roundtrip() {
        let member: Vec<u8> = (0u8..=255).collect();
        let mut set: HashSet<RedisString> = HashSet::new();
        set.insert(RedisString::from_vec(member.clone()));
        let obj = RedisObject::new_set_from_set(set);
        let mut buf: Vec<u8> = Vec::new();
        save_set_object(&mut buf, &obj).unwrap();
        let mut cursor = Cursor::new(&buf);
        let loaded = load_set_object(&mut cursor).unwrap();
        assert!(loaded.set().unwrap().contains(&RedisString::from_vec(member)));
    }
}
