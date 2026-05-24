//! Memory accounting helpers shared by INFO and eviction.
//!
//! The estimator approximates a database's in-memory footprint as
//!
//!   bytes_in_keys + bytes_in_values + dict.len() * 80
//!
//! per `docs/PATH_TO_DEF3.md` §Memory-estimator. The 80-byte-per-entry
//! overhead approximates HashMap bucket plus per-object header overhead;
//! non-string values contribute a coarse per-element estimate rather than an
//! exact allocator walk.
//!
//! INFO reports the result as `used_memory_estimated`; the maxmemory eviction
//! path (Round 16b+) compares it against `LiveConfig::maxmemory` to decide
//! when to evict.

use crate::db::RedisDb;
use crate::object::{
    HashEncoding, ListEncoding, ObjectKind, SetEncoding, StringEncoding, ZSetEncoding,
};

/// Estimate the in-memory footprint of `db` in bytes.
///
/// Single source of truth for the heuristic; both INFO and eviction call this.
pub fn approximate_memory_used(db: &RedisDb) -> u64 {
    let mut bytes: u64 = db.len() as u64 * 80;
    let snapshot = db.keys_snapshot_with_types();
    for (key, _kind_name) in &snapshot {
        bytes += key.as_bytes().len() as u64;
    }
    for (key, _) in &snapshot {
        if let Some(obj) = db.find(key) {
            bytes += approximate_object_bytes(&obj.kind);
        }
    }
    bytes
}

/// Coarse byte estimate for the value payload of a single object.
pub fn approximate_object_bytes(kind: &ObjectKind) -> u64 {
    match kind {
        ObjectKind::String(enc) => match enc {
            StringEncoding::Int(_) => 8,
            StringEncoding::Raw(s) | StringEncoding::Embstr(s) => s.as_bytes().len() as u64,
        },
        ObjectKind::List(enc) => match enc {
            ListEncoding::Inline(d) => d.iter().map(|s| s.as_bytes().len() as u64 + 16).sum(),
            ListEncoding::ListPack(b) => b.len() as u64,
            ListEncoding::QuickList(v) => v.iter().map(|s| s.as_bytes().len() as u64 + 16).sum(),
        },
        ObjectKind::Hash(enc) => match enc {
            HashEncoding::Inline(m) => m
                .iter()
                .map(|(k, v)| k.as_bytes().len() as u64 + v.as_bytes().len() as u64 + 32)
                .sum(),
            HashEncoding::ListPack(b) => b.len() as u64,
            HashEncoding::HashTable(m) => m
                .iter()
                .map(|(k, v)| k.as_bytes().len() as u64 + v.as_bytes().len() as u64 + 32)
                .sum(),
        },
        ObjectKind::Set(enc) => match enc {
            SetEncoding::Inline(s) => s.data.iter().map(|m| m.as_bytes().len() as u64 + 16).sum(),
            SetEncoding::ListPack(b) => b.len() as u64,
            SetEncoding::IntSet(v) => v.len() as u64 * 8,
            SetEncoding::HashTable(h) => h.iter().map(|s| s.as_bytes().len() as u64 + 16).sum(),
        },
        ObjectKind::ZSet(enc) => match enc {
            ZSetEncoding::Inline(z) => z
                .by_member
                .iter()
                .map(|(k, _)| k.as_bytes().len() as u64 + 24)
                .sum(),
            ZSetEncoding::ListPack(b) => b.len() as u64,
            ZSetEncoding::SkipList(v) => {
                v.iter().map(|(k, _)| k.as_bytes().len() as u64 + 24).sum()
            }
        },
        ObjectKind::Stream(_) => 64,
        ObjectKind::Module => 0,
        ObjectKind::Json(v) => v.to_string().len() as u64,
        ObjectKind::Bloom(bf) => bf.bits.len() as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_types::RedisString;

    #[test]
    fn empty_db_uses_zero_bytes() {
        let db = RedisDb::new(0);
        assert_eq!(approximate_memory_used(&db), 0);
    }

    #[test]
    fn string_keys_contribute_their_payload() {
        let mut db = RedisDb::new(0);
        db.insert(
            RedisString::from_bytes(b"k"),
            crate::object::RedisObject::from_string(RedisString::from_bytes(b"hello")),
        );
        let bytes = approximate_memory_used(&db);
        assert!(bytes >= 80 + b"k".len() as u64 + b"hello".len() as u64);
    }
}
