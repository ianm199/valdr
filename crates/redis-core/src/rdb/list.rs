//! RDB list type serialization — Round 21.
//! Implements `save_list_object` / `load_list_object` for `RDB_TYPE_LIST` (type 0x01)
//! and `load_quicklist2_object` for `RDB_TYPE_LIST_QUICKLIST_2` (type 0x12 = 18),
//! which is the format that C Valkey always emits.
//! Save wire layout (RDB_TYPE_LIST) after the type byte:
//! - `save_len(N)` — number of elements
//! - For each element: raw-byte string (length-prefixed)
//! Load wire layout (RDB_TYPE_LIST_QUICKLIST_2) after the type byte:
//! - `save_len(num_nodes)` — number of quicklist nodes
//! - For each node:
//! - `save_len(container)` — 1 = PLAIN, 2 = PACKED (listpack blob)
//! - raw-byte string: the element bytes (PLAIN) or listpack blob (PACKED)
//! PACKED nodes contain a listpack binary that holds one or more string/integer
//! entries. PLAIN nodes hold a single oversized element directly.
//! Design decision: we emit `RDB_TYPE_LIST` on save because it is a simpler
//! format that C Valkey loads without error (compatibility direction A: us → C).
//! For direction B (C → us) we parse `RDB_TYPE_LIST_QUICKLIST_2` by decoding
//! both PLAIN and PACKED nodes via the minimal listpack decoder
//! `rdb/listpack.rs`.

use std::collections::VecDeque;
use std::io::{self, Read, Write};

use redis_types::RedisString;

use crate::object::RedisObject;

use super::header::{read_rdb_string, write_rdb_string};
use super::listpack::decode_listpack;
use super::varint::{load_len, write_len};

const QUICKLIST_NODE_CONTAINER_PLAIN: u64 = 1;
const QUICKLIST_NODE_CONTAINER_PACKED: u64 = 2;

/// Serialize an `RDB_TYPE_LIST` value payload.
/// The type byte is written by the caller; this function writes the element
/// count followed by each element as a raw-byte length-prefixed string.
pub fn save_list_object(w: &mut impl Write, obj: &RedisObject) -> io::Result<()> {
    let list = obj.list().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "save_list_object called on non-list object",
        )
    })?;
    write_len(w, list.len() as u64)?;
    for elem in list {
        write_rdb_string(w, elem.as_bytes())?;
    }
    Ok(())
}

/// Deserialize an `RDB_TYPE_LIST` value payload, producing a `RedisObject`.
/// Reads from `r` starting immediately after the type byte.
pub fn load_list_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let (n, _is_encoded) = load_len(r)?;
    let mut list: VecDeque<RedisString> = VecDeque::with_capacity(super::prealloc_capacity(n));
    for _ in 0..n {
        let elem_bytes = read_rdb_string(r)?;
        list.push_back(RedisString::from_vec(elem_bytes));
    }
    Ok(RedisObject::new_list_from_vec(list))
}

/// Deserialize an `RDB_TYPE_LIST_QUICKLIST_2` value payload, producing a `RedisObject`.
/// C Valkey always emits this format for lists. Each node carries a container
/// tag (1 = PLAIN, 2 = PACKED) followed by raw bytes. PLAIN nodes hold one
/// oversized element directly. PACKED nodes hold a listpack binary with one
/// or more entries.
pub fn load_quicklist2_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let (num_nodes, _) = load_len(r)?;
    let mut list: VecDeque<RedisString> = VecDeque::new();
    for _ in 0..num_nodes {
        let (container, _) = load_len(r)?;
        let blob = read_rdb_string(r)?;
        match container {
            QUICKLIST_NODE_CONTAINER_PLAIN => {
                list.push_back(RedisString::from_vec(blob));
            }
            QUICKLIST_NODE_CONTAINER_PACKED => {
                let entries = decode_listpack(&blob).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("quicklist2 listpack decode failed: {}", e),
                    )
                })?;
                for entry_bytes in entries {
                    list.push_back(RedisString::from_vec(entry_bytes));
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown quicklist node container tag {}", container),
                ));
            }
        }
    }
    Ok(RedisObject::new_list_from_vec(list))
}

/// Deserialize an `RDB_TYPE_LIST_ZIPLIST` value: a single ziplist blob whose
/// entries are the list elements (the obsolete pre-quicklist encoding).
pub fn load_list_ziplist_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let blob = read_rdb_string(r)?;
    let entries = super::ziplist::decode_ziplist(&blob)?;
    let mut list: VecDeque<RedisString> = VecDeque::with_capacity(entries.len());
    for entry in entries {
        list.push_back(RedisString::from_vec(entry));
    }
    Ok(RedisObject::new_list_from_vec(list))
}

/// Deserialize an `RDB_TYPE_LIST_QUICKLIST` (v1) value: `load_len` node count,
/// then each node is a ziplist blob stored as an RDB string. Pre-7.0 lists.
pub fn load_quicklist_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let (num_nodes, _) = load_len(r)?;
    let mut list: VecDeque<RedisString> = VecDeque::new();
    for _ in 0..num_nodes {
        let blob = read_rdb_string(r)?;
        let entries = super::ziplist::decode_ziplist(&blob).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("quicklist ziplist node decode failed: {}", e),
            )
        })?;
        for entry in entries {
            list.push_back(RedisString::from_vec(entry));
        }
    }
    Ok(RedisObject::new_list_from_vec(list))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(elems: &[&str]) -> VecDeque<RedisString> {
        let mut deque: VecDeque<RedisString> = VecDeque::new();
        for e in elems {
            deque.push_back(RedisString::from_bytes(e.as_bytes()));
        }
        let obj = RedisObject::new_list_from_vec(deque);

        let mut buf: Vec<u8> = Vec::new();
        save_list_object(&mut buf, &obj).unwrap();
        let mut cursor = Cursor::new(&buf);
        let loaded = load_list_object(&mut cursor).unwrap();
        loaded.list().unwrap().clone()
    }

    #[test]
    fn empty_list_roundtrip() {
        let result = roundtrip(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn single_elem_roundtrip() {
        let result = roundtrip(&["hello"]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_bytes(), b"hello");
    }

    #[test]
    fn multi_elem_roundtrip() {
        let elems = ["alpha", "beta", "gamma", "delta"];
        let result = roundtrip(&elems);
        assert_eq!(result.len(), 4);
        for (i, e) in elems.iter().enumerate() {
            assert_eq!(result[i].as_bytes(), e.as_bytes());
        }
    }

    #[test]
    fn order_preserved() {
        let elems: Vec<String> = (0..100).map(|i| format!("item{}", i)).collect();
        let refs: Vec<&str> = elems.iter().map(|s| s.as_str()).collect();
        let result = roundtrip(&refs);
        assert_eq!(result.len(), 100);
        for (i, e) in elems.iter().enumerate() {
            assert_eq!(result[i].as_bytes(), e.as_bytes());
        }
    }

 /// A crafted payload declares billions of elements but supplies no data.
 /// Without the pre-allocation cap this would attempt a multi-gigabyte
 /// `VecDeque` allocation and abort the process; with the cap it must fail
 /// cleanly on the first absent element read instead.
    #[test]
    fn hostile_length_prefix_errors_without_aborting() {
        let mut buf: Vec<u8> = Vec::new();
        write_len(&mut buf, u32::MAX as u64).unwrap();
        let mut cursor = Cursor::new(&buf);
        assert!(load_list_object(&mut cursor).is_err());
    }

    #[test]
    fn binary_elem_roundtrip() {
        let elem: Vec<u8> = (0u8..=255).collect();
        let mut deque: VecDeque<RedisString> = VecDeque::new();
        deque.push_back(RedisString::from_vec(elem.clone()));
        let obj = RedisObject::new_list_from_vec(deque);
        let mut buf: Vec<u8> = Vec::new();
        save_list_object(&mut buf, &obj).unwrap();
        let mut cursor = Cursor::new(&buf);
        let loaded = load_list_object(&mut cursor).unwrap();
        assert_eq!(loaded.list().unwrap()[0].as_bytes(), elem.as_slice());
    }
}
