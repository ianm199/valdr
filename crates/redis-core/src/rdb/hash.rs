//! RDB hash type serialization — Round 20.
//!
//! Implements `save_hash_object` and `load_hash_object` for `RDB_TYPE_HASH`
//! (the HASHTABLE wire form, type byte 0x04).
//!
//! Wire layout after the type byte:
//!   - `save_len(num_fields)` — number of field/value pairs
//!   - For each pair: field bytes (length-prefixed), value bytes (length-prefixed)
//!
//! Design decision: we always emit `RDB_TYPE_HASH` regardless of hash size.
//! C Valkey loads this form for hashes of any size without error. The
//! `RDB_TYPE_HASH_LISTPACK` form (type 16) that C Valkey emits for small hashes
//! is NOT emitted by us in Phase 1; instead we document below how to force C
//! Valkey into HASHTABLE mode for oracle corpus tests by setting
//! `hash-max-listpack-entries 0` before SAVE.
//!
//! Load compatibility:
//!   - `RDB_TYPE_HASH` (4)         — fully handled
//!   - `RDB_TYPE_HASH_ZIPLIST` (13) — graceful error: not yet implemented
//!   - `RDB_TYPE_HASH_LISTPACK` (16) — graceful error: not yet implemented
//!   - `RDB_TYPE_HASH_2` (22)       — graceful error: field-level expiry not yet implemented
//!
//! When the oracle corpus test uses `CONFIG SET hash-max-listpack-entries 0`
//! before saving, C Valkey emits `RDB_TYPE_HASH` and round-trip passes
//! without implementing the listpack binary parser.

use std::collections::HashMap;
use std::io::{self, Read, Write};

use redis_types::RedisString;

use crate::object::RedisObject;

use super::header::read_rdb_string;
use super::varint::{load_len, write_len};

/// Serialize an `RDB_TYPE_HASH` value payload.
///
/// The type byte is written by the caller; this function writes the count
/// followed by alternating raw-byte field and value strings.
pub fn save_hash_object(w: &mut impl Write, obj: &RedisObject) -> io::Result<()> {
    let hash = obj.hash().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "save_hash_object called on non-hash object",
        )
    })?;
    write_len(w, hash.len() as u64)?;
    for (field, value) in hash {
        save_raw_field(w, field.as_bytes())?;
        save_raw_field(w, value.as_bytes())?;
    }
    Ok(())
}

/// Deserialize an `RDB_TYPE_HASH` value payload, producing a `RedisObject`.
///
/// Reads from `r` starting immediately after the type byte.
pub fn load_hash_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let (n, _is_encoded) = load_len(r)?;
    let mut hash: HashMap<RedisString, RedisString> = HashMap::with_capacity(n as usize);
    for _ in 0..n {
        let field_bytes = read_rdb_string(r)?;
        let value_bytes = read_rdb_string(r)?;
        hash.insert(
            RedisString::from_vec(field_bytes),
            RedisString::from_vec(value_bytes),
        );
    }
    Ok(RedisObject::new_hash_from_map(hash))
}

/// Write a raw byte slice as a length-prefixed string (no integer encoding).
///
/// Used for hash field and value bytes where we carry `Vec<u8>` without
/// separate integer-encoding metadata, so we always emit the raw form.
fn save_raw_field(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    write_len(w, bytes.len() as u64)?;
    w.write_all(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(pairs: &[(&str, &str)]) -> HashMap<RedisString, RedisString> {
        let mut hash = HashMap::new();
        for (f, v) in pairs {
            hash.insert(
                RedisString::from_bytes(f.as_bytes()),
                RedisString::from_bytes(v.as_bytes()),
            );
        }
        let obj = RedisObject::new_hash_from_map(hash);

        let mut buf: Vec<u8> = Vec::new();
        save_hash_object(&mut buf, &obj).unwrap();
        let mut cursor = Cursor::new(&buf);
        let loaded = load_hash_object(&mut cursor).unwrap();
        loaded.hash().unwrap().clone()
    }

    #[test]
    fn empty_hash_roundtrip() {
        let result = roundtrip(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn single_field_roundtrip() {
        let result = roundtrip(&[("field1", "value1")]);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result
                .get(&RedisString::from_bytes(b"field1"))
                .map(|v| v.as_bytes()),
            Some(b"value1".as_slice())
        );
    }

    #[test]
    fn multi_field_roundtrip() {
        let pairs = [("f1", "v1"), ("f2", "v2"), ("f3", "v3")];
        let result = roundtrip(&pairs);
        assert_eq!(result.len(), 3);
        for (f, v) in &pairs {
            assert_eq!(
                result
                    .get(&RedisString::from_bytes(f.as_bytes()))
                    .map(|r| r.as_bytes()),
                Some(v.as_bytes())
            );
        }
    }

    #[test]
    fn binary_field_roundtrip() {
        let field: Vec<u8> = (0u8..=255).collect();
        let value: Vec<u8> = (0u8..=127).collect();
        let mut hash = HashMap::new();
        hash.insert(
            RedisString::from_vec(field.clone()),
            RedisString::from_vec(value.clone()),
        );
        let obj = RedisObject::new_hash_from_map(hash);
        let mut buf: Vec<u8> = Vec::new();
        save_hash_object(&mut buf, &obj).unwrap();
        let mut cursor = Cursor::new(&buf);
        let loaded = load_hash_object(&mut cursor).unwrap();
        let result = loaded.hash().unwrap();
        assert_eq!(
            result
                .get(&RedisString::from_vec(field))
                .map(|v| v.as_bytes()),
            Some(value.as_slice())
        );
    }
}
