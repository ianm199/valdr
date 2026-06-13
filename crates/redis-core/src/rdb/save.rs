//! RDB save path — `RdbSaver` writes the complete RDB file.
//! Round 19a: replaces the Round 18 empty-payload placeholder with real
//! `RDB_TYPE_STRING` serialization for all three string encodings: Int,
//! Embstr, Raw. Non-string types still emit an empty-payload placeholder
//! (Rounds 20–23 will replace those).
//! File layout:
//! - Magic header + AUX fields
//! - SELECTDB + RESIZEDB hints for every non-empty logical DB
//! - Per-key: optional EXPIRETIME_MS + type byte + value payload
//! - EOF + CRC64 trailer
//! The saver accumulates everything in a `Vec<u8>` so the CRC can be computed
//! over the entire byte stream before writing to disk.

use std::io::{self, Write};
use std::path::Path;

use crate::db::RedisDb;
use crate::object::{ObjectKind, RedisObject, EXPIRY_NONE};

use super::crc::crc64;
use super::hash::save_hash_object;
use super::header::{
    write_aux_fields, write_magic, write_rdb_string, RDB_OPCODE_EOF, RDB_OPCODE_EXPIRETIME_MS,
    RDB_OPCODE_FUNCTION2, RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB, RDB_TYPE_BLOOM_NATIVE,
    RDB_TYPE_HASH, RDB_TYPE_JSON_NATIVE, RDB_TYPE_LIST, RDB_TYPE_SET, RDB_TYPE_STREAM_LISTPACKS_3,
    RDB_TYPE_STRING, RDB_TYPE_ZSET_2,
};
use super::list::save_list_object;
use super::set::save_set_object;
use super::stream::save_stream_object;
use super::string::save_string_object;
use super::varint::write_len;
use super::zset::save_zset_object;

fn write_rdb_dbs_to_buf(
    dbs: &[RedisDb],
    function_payloads: &[Vec<u8>],
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    write_magic(buf)?;
    write_aux_fields(buf)?;
    write_rdb_function_payloads(function_payloads, buf)?;

    for db in dbs {
        if db.size() == 0 {
            continue;
        }

        buf.write_all(&[RDB_OPCODE_SELECTDB])?;
        write_len(buf, db.id as u64)?;

        let total_keys = db.size();
        let expires_count = db.expires_count();
        buf.write_all(&[RDB_OPCODE_RESIZEDB])?;
        write_len(buf, total_keys)?;
        write_len(buf, expires_count)?;

        for (key, obj) in db.iter_for_eviction() {
            if obj.expire != EXPIRY_NONE {
                buf.write_all(&[RDB_OPCODE_EXPIRETIME_MS])?;
                buf.write_all(&obj.expire.to_le_bytes())?;
            }

            let type_byte = match rdb_type_for_object(obj) {
                Ok(t) => t,
                Err(_) => continue,
            };
            buf.write_all(&[type_byte])?;
            write_rdb_string(buf, key.as_bytes())?;
            save_object_payload(buf, obj)?;
        }
    }

    buf.write_all(&[RDB_OPCODE_EOF])?;

    let checksum = crc64(0, buf);
    buf.write_all(&checksum.to_le_bytes())?;

    Ok(())
}

fn write_rdb_function_payloads(
    function_payloads: &[Vec<u8>],
    buf: &mut impl Write,
) -> io::Result<()> {
    for payload in function_payloads {
        buf.write_all(&[RDB_OPCODE_FUNCTION2])?;
        write_rdb_string(buf, payload)?;
    }
    Ok(())
}

/// Serialize a `ObjectKind::Json` value as a length-prefixed UTF-8 JSON string.
/// Wire format: `write_rdb_string(serde_json::to_string(value))`.
/// This is a private opcode (RDB_TYPE_JSON_NATIVE = 200) understood only by
/// this implementation; real Valkey will reject the file.
fn save_json_object(buf: &mut impl Write, obj: &crate::object::RedisObject) -> io::Result<()> {
    let ObjectKind::Json(value) = &obj.kind else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected Json kind",
        ));
    };
    let json_str = serde_json::to_string(value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    write_rdb_string(buf, json_str.as_bytes())
}

/// Serialize a `ObjectKind::Bloom` value as a fixed binary record followed by a
/// length-prefixed byte slice for the bit array.
/// Wire format (all integers little-endian):
/// capacity u64 (8 bytes)
/// item_count u64 (8 bytes)
/// n_hashes u32 (4 bytes)
/// error_rate f64 (8 bytes, IEEE 754)
/// expansion u32 (4 bytes)
/// nonscaling u8 (1 byte, 0 or 1)
/// bits write_rdb_string(bf.bits)
/// This is a private opcode (RDB_TYPE_BLOOM_NATIVE = 201) understood only by
/// this implementation; real Valkey will reject the file.
fn save_bloom_object(buf: &mut impl Write, obj: &crate::object::RedisObject) -> io::Result<()> {
    let ObjectKind::Bloom(bf) = &obj.kind else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected Bloom kind",
        ));
    };
    buf.write_all(&bf.capacity.to_le_bytes())?;
    buf.write_all(&bf.item_count.to_le_bytes())?;
    buf.write_all(&bf.n_hashes.to_le_bytes())?;
    buf.write_all(&bf.error_rate.to_le_bytes())?;
    buf.write_all(&bf.expansion.to_le_bytes())?;
    buf.write_all(&[bf.nonscaling as u8])?;
    write_rdb_string(buf, &bf.bits)
}

/// Return the RDB type byte used to encode one Redis object.
/// `ObjectKind::Module` has no in-tree serializer yet, so callers get an
/// `Unsupported` error rather than a placeholder byte.
pub fn rdb_type_for_object(obj: &RedisObject) -> io::Result<u8> {
    match &obj.kind {
        ObjectKind::String(_) => Ok(RDB_TYPE_STRING),
        ObjectKind::Hash(_) => Ok(RDB_TYPE_HASH),
        ObjectKind::List(_) => Ok(RDB_TYPE_LIST),
        ObjectKind::Set(_) => Ok(RDB_TYPE_SET),
        ObjectKind::ZSet(_) => Ok(RDB_TYPE_ZSET_2),
        ObjectKind::Stream(_) => Ok(RDB_TYPE_STREAM_LISTPACKS_3),
        ObjectKind::Json(_) => Ok(RDB_TYPE_JSON_NATIVE),
        ObjectKind::Bloom(_) => Ok(RDB_TYPE_BLOOM_NATIVE),
        ObjectKind::Module => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "module object RDB payloads are not supported",
        )),
    }
}

/// Serialize only an object's value payload, excluding type byte, key, expiry,
/// and DB-level opcodes.
pub fn save_object_payload(buf: &mut impl Write, obj: &RedisObject) -> io::Result<()> {
    match &obj.kind {
        ObjectKind::String(_) => save_string_object(buf, obj),
        ObjectKind::Hash(_) => save_hash_object(buf, obj),
        ObjectKind::List(_) => save_list_object(buf, obj),
        ObjectKind::Set(_) => save_set_object(buf, obj),
        ObjectKind::ZSet(_) => save_zset_object(buf, obj),
        ObjectKind::Stream(_) => save_stream_object(buf, obj),
        ObjectKind::Json(_) => save_json_object(buf, obj),
        ObjectKind::Bloom(_) => save_bloom_object(buf, obj),
        ObjectKind::Module => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "module object RDB payloads are not supported",
        )),
    }
}

/// Create the byte payload returned by `DUMP`.
/// Layout: `<type byte><object payload><u16 RDB version LE><u64 CRC64 LE>`.
pub fn create_dump_payload(obj: &RedisObject) -> io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(64);
    buf.write_all(&[rdb_type_for_object(obj)?])?;
    save_object_payload(&mut buf, obj)?;
    buf.write_all(&super::header::RDB_DUMP_VERSION.to_le_bytes())?;
    let checksum = crc64(0, &buf);
    buf.write_all(&checksum.to_le_bytes())?;
    Ok(buf)
}

/// Save `db` to the file at `path`, using an atomic write-then-rename strategy.
/// A temporary file `<path>.tmp` is written first; on success it is renamed
/// over `path`. This ensures the on-disk file is never partially written.
pub fn save_rdb(db: &RedisDb, path: &Path) -> io::Result<()> {
    save_rdb_databases(std::slice::from_ref(db), path)
}

/// Save every non-empty logical DB to `path`.
pub fn save_rdb_databases(dbs: &[RedisDb], path: &Path) -> io::Result<()> {
    save_rdb_databases_with_functions(dbs, &[], path)
}

/// Save databases plus opaque function-library payloads to `path`.
///
/// `redis-core` intentionally treats function payloads as bytes. The command
/// crate owns Lua/function parsing and supplies the payload for native saves,
/// replica full sync, and startup restore.
pub fn save_rdb_databases_with_functions(
    dbs: &[RedisDb],
    function_payloads: &[Vec<u8>],
    path: &Path,
) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    write_rdb_dbs_to_buf(dbs, function_payloads, &mut buf)?;

    let tmp_path = path.with_extension("rdb.tmp");
    {
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(&buf)?;
        f.flush()?;
    }
    std::fs::rename(&tmp_path, path)
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         RuntimeOwner persistence can now save a caller-owned DB
//                  slice without reading the transitional global DB store.
// ──────────────────────────────────────────────────────────────────────────
