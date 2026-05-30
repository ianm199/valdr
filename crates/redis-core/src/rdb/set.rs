//! RDB set type serialization — Round 21.
//! Implements `save_set_object` and `load_set_object` for `RDB_TYPE_SET`
//! (the flat-string-array wire form, type byte 0x02).
//! Wire layout after the type byte:
//! - `save_len(N)` — number of members
//! - For each member: raw-byte string (length-prefixed)
//! Design decision: we always emit `RDB_TYPE_SET` regardless of set size.
//! C Valkey loads this form for sets of any size without error. The compact
//! forms that C Valkey emits for small sets — `RDB_TYPE_SET_LISTPACK` (20)
//! and `RDB_TYPE_SET_INTSET` (11) — are NOT supported by us in Phase 1.
//! The oracle corpus tests coerce C Valkey into the flat form via:
//! CONFIG SET set-max-intset-entries 0
//! CONFIG SET set-max-listpack-entries 0
//! Load compatibility:
//! - `RDB_TYPE_SET` (2) — fully handled
//! - `RDB_TYPE_SET_INTSET` (11) — graceful Unsupported error
//! - `RDB_TYPE_SET_LISTPACK` (20) — graceful Unsupported error

use std::collections::HashSet;
use std::io::{self, Read, Write};

use redis_types::RedisString;

use crate::object::RedisObject;

use super::header::{read_rdb_string, write_rdb_string};
use super::listpack::decode_listpack;
use super::varint::{load_len, write_len};

/// Serialize an `RDB_TYPE_SET` value payload.
/// The type byte is written by the caller; this function writes the member
/// count followed by each member as a raw-byte length-prefixed string.
pub fn save_set_object(w: &mut impl Write, obj: &RedisObject) -> io::Result<()> {
    let set = obj.set().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "save_set_object called on non-set object",
        )
    })?;
    write_len(w, set.len() as u64)?;
    for member in set {
        write_rdb_string(w, member.as_bytes())?;
    }
    Ok(())
}

/// Deserialize an `RDB_TYPE_SET` value payload, producing a `RedisObject`.
/// Reads from `r` starting immediately after the type byte.
pub fn load_set_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let (n, _is_encoded) = load_len(r)?;
    let mut members: HashSet<RedisString> = HashSet::with_capacity(super::prealloc_capacity(n));
    for _ in 0..n {
        let member_bytes = read_rdb_string(r)?;
        members.insert(RedisString::from_vec(member_bytes));
    }
    Ok(RedisObject::new_set_from_set(members))
}

/// Deserialize an `RDB_TYPE_SET_LISTPACK` value: a single listpack blob whose
/// elements are the set members.
pub fn load_set_listpack_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let blob = read_rdb_string(r)?;
    let elements = decode_listpack(&blob)?;
    let mut members: HashSet<RedisString> = HashSet::with_capacity(elements.len());
    for member in elements {
        if !members.insert(RedisString::from_vec(member)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate member in set listpack",
            ));
        }
    }
    Ok(RedisObject::new_set_from_set(members))
}

/// Deserialize an `RDB_TYPE_SET_INTSET` value: an intset blob stored as an RDB
/// string. Layout: `u32 LE encoding (2|4|8 bytes per int)`, `u32 LE length`,
/// then `length` little-endian signed integers. Members are stored as their
/// decimal string form, matching how Valkey materialises an intset on load.
pub fn load_set_intset_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let blob = read_rdb_string(r)?;
    if blob.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "intset blob too short for header",
        ));
    }
    let encoding = u32::from_le_bytes([blob[0], blob[1], blob[2], blob[3]]) as usize;
    let length = u32::from_le_bytes([blob[4], blob[5], blob[6], blob[7]]) as usize;
    if encoding != 2 && encoding != 4 && encoding != 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid intset encoding width {}", encoding),
        ));
    }
    let body = &blob[8..];
    if body.len() != length.saturating_mul(encoding) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "intset length does not match blob size",
        ));
    }
    let mut members: HashSet<RedisString> = HashSet::with_capacity(length);
    let mut prev: Option<i64> = None;
    for i in 0..length {
        let off = i * encoding;
        let value: i64 = match encoding {
            2 => i16::from_le_bytes([body[off], body[off + 1]]) as i64,
            4 => {
                i32::from_le_bytes([body[off], body[off + 1], body[off + 2], body[off + 3]]) as i64
            }
            _ => i64::from_le_bytes([
                body[off],
                body[off + 1],
                body[off + 2],
                body[off + 3],
                body[off + 4],
                body[off + 5],
                body[off + 6],
                body[off + 7],
            ]),
        };
        if let Some(p) = prev {
            if value <= p {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "intset entries not strictly ascending (corrupt or unsorted)",
                ));
            }
        }
        prev = Some(value);
        members.insert(RedisString::from_vec(value.to_string().into_bytes()));
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
        assert!(loaded
            .set()
            .unwrap()
            .contains(&RedisString::from_vec(member)));
    }
}
