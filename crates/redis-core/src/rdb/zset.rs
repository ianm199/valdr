//! RDB sorted-set type serialization — Round 22.
//!
//! Implements `save_zset_object` and `load_zset_object` for `RDB_TYPE_ZSET_2`
//! (the IEEE-754 binary-double wire form, type byte 0x05).
//!
//! Wire layout after the type byte:
//!   - `save_len(N)` — number of member/score pairs
//!   - For each pair: member bytes (length-prefixed), score as 8-byte LE double
//!
//! Design decision: we always emit `RDB_TYPE_ZSET_2` regardless of zset size.
//! C Valkey loads this form for zsets of any size without error. The
//! `RDB_TYPE_ZSET_LISTPACK` form (type 17) that C Valkey emits for small zsets
//! is NOT emitted by us in Phase 1. To force C Valkey into ZSET_2 mode for
//! oracle corpus tests, run `CONFIG SET zset-max-listpack-entries 0` before SAVE.
//!
//! Load compatibility:
//!   - `RDB_TYPE_ZSET_2` (5)          — fully handled
//!   - `RDB_TYPE_ZSET` (3)            — graceful Unsupported error (text-encoded scores)
//!   - `RDB_TYPE_ZSET_ZIPLIST` (12)   — graceful Unsupported error
//!   - `RDB_TYPE_ZSET_LISTPACK` (17)  — graceful Unsupported error
//!
//! NaN scores: rejected on load with an InvalidData error. Redis semantics
//! prohibit NaN scores at the parsing boundary; our `F64Ord` type encodes this
//! invariant. `+inf` and `-inf` are valid and round-trip correctly through 8-byte
//! IEEE-754 little-endian encoding.

use std::io::{self, Read, Write};

use crate::object::{InlineZSet, RedisObject};

use super::header::read_rdb_string;
use super::varint::{load_len, write_len};

/// Serialize an `RDB_TYPE_ZSET_2` value payload.
///
/// The type byte is written by the caller; this function writes the member count
/// followed by alternating member strings and 8-byte LE binary doubles.
pub fn save_zset_object(w: &mut impl Write, obj: &RedisObject) -> io::Result<()> {
    let zset = obj.zset().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "save_zset_object called on non-zset object")
    })?;
    write_len(w, zset.len() as u64)?;
    for (score, member) in &zset.by_order {
        write_zset_member(w, member.as_bytes())?;
        w.write_all(&score.get().to_le_bytes())?;
    }
    Ok(())
}

/// Deserialize an `RDB_TYPE_ZSET_2` value payload, producing a `RedisObject`.
///
/// Reads from `r` starting immediately after the type byte. Returns an error
/// if any loaded score is NaN, which is forbidden by Redis semantics.
pub fn load_zset_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let (n, _is_encoded) = load_len(r)?;
    let mut zset = InlineZSet::new();
    for _ in 0..n {
        let member_bytes = read_rdb_string(r)?;
        let mut score_bytes = [0u8; 8];
        r.read_exact(&mut score_bytes)?;
        let score = f64::from_le_bytes(score_bytes);
        if score.is_nan() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "NaN score in RDB_TYPE_ZSET_2 payload — rejected at load boundary",
            ));
        }
        let member = redis_types::RedisString::from_vec(member_bytes);
        zset.upsert(member, score);
    }
    Ok(RedisObject::new_zset_from_inline(zset))
}

/// Write a zset member as a raw length-prefixed string (no integer encoding).
fn write_zset_member(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    write_len(w, bytes.len() as u64)?;
    w.write_all(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(pairs: &[(&str, f64)]) -> InlineZSet {
        let mut zset = InlineZSet::new();
        for (m, s) in pairs {
            zset.upsert(redis_types::RedisString::from_bytes(m.as_bytes()), *s);
        }
        let obj = RedisObject::new_zset_from_inline(zset);
        let mut buf: Vec<u8> = Vec::new();
        save_zset_object(&mut buf, &obj).unwrap();
        let mut cursor = Cursor::new(&buf);
        let loaded = load_zset_object(&mut cursor).unwrap();
        loaded.zset().unwrap().clone()
    }

    #[test]
    fn empty_zset_roundtrip() {
        let result = roundtrip(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn single_member_roundtrip() {
        let result = roundtrip(&[("alpha", 1.0)]);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.score(&redis_types::RedisString::from_bytes(b"alpha")),
            Some(1.0)
        );
    }

    #[test]
    fn multi_member_roundtrip() {
        let pairs = [("alpha", 1.0), ("beta", 2.0), ("gamma", 3.0)];
        let result = roundtrip(&pairs);
        assert_eq!(result.len(), 3);
        for (m, s) in &pairs {
            assert_eq!(
                result.score(&redis_types::RedisString::from_bytes(m.as_bytes())),
                Some(*s)
            );
        }
    }

    #[test]
    fn float_scores_roundtrip() {
        let pairs = [("neg", -1.5), ("zero", 0.0), ("half", 0.5), ("pi", std::f64::consts::PI)];
        let result = roundtrip(&pairs);
        for (m, s) in &pairs {
            let loaded = result.score(&redis_types::RedisString::from_bytes(m.as_bytes())).unwrap();
            assert_eq!(loaded.to_bits(), s.to_bits(), "score mismatch for {m}");
        }
    }

    #[test]
    fn inf_scores_roundtrip() {
        let pairs = [("pos_inf", f64::INFINITY), ("neg_inf", f64::NEG_INFINITY)];
        let result = roundtrip(&pairs);
        for (m, s) in &pairs {
            let loaded = result.score(&redis_types::RedisString::from_bytes(m.as_bytes())).unwrap();
            assert_eq!(loaded.to_bits(), s.to_bits(), "score mismatch for {m}");
        }
    }

    #[test]
    fn nan_score_rejected_on_load() {
        let mut buf: Vec<u8> = Vec::new();
        write_len(&mut buf, 1u64).unwrap();
        write_zset_member(&mut buf, b"bad").unwrap();
        buf.extend_from_slice(&f64::NAN.to_le_bytes());
        let mut cursor = Cursor::new(&buf);
        let result = load_zset_object(&mut cursor);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn tied_scores_all_members_survive() {
        let pairs = [("apple", 1.0), ("banana", 1.0), ("cherry", 1.0)];
        let result = roundtrip(&pairs);
        assert_eq!(result.len(), 3);
    }
}
