//! RDB save path — `RdbSaver` writes the complete RDB file.
//!
//! Round 19a: replaces the Round 18 empty-payload placeholder with real
//! `RDB_TYPE_STRING` serialization for all three string encodings: Int,
//! Embstr, Raw. Non-string types still emit an empty-payload placeholder
//! (Rounds 20–23 will replace those).
//!
//! File layout:
//!   - Magic header + AUX fields
//!   - SELECTDB(0) + RESIZEDB hints
//!   - Per-key: optional EXPIRETIME_MS + type byte + value payload
//!   - EOF + CRC64 trailer
//!
//! The saver accumulates everything in a `Vec<u8>` so the CRC can be computed
//! over the entire byte stream before writing to disk.

use std::io::{self, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::RedisDb;
use crate::object::{ObjectKind, EXPIRY_NONE};

use super::crc::crc64;
use super::hash::save_hash_object;
use super::header::{
    write_aux_fields, write_magic, write_rdb_string, RDB_OPCODE_EOF, RDB_OPCODE_EXPIRETIME_MS,
    RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB, RDB_TYPE_HASH, RDB_TYPE_LIST, RDB_TYPE_SET,
    RDB_TYPE_STRING, RDB_TYPE_ZSET_2,
};
use super::list::save_list_object;
use super::set::save_set_object;
use super::string::save_string_object;
use super::varint::write_len;
use super::zset::save_zset_object;

/// Write the complete RDB representation of `db` to the byte buffer `buf`.
///
/// The buffer includes magic, AUX fields, one DB section (id 0), all keys
/// with optional EXPIRETIME_MS opcodes, EOF, and the CRC64 trailer.
fn write_rdb_to_buf(db: &RedisDb, buf: &mut Vec<u8>) -> io::Result<()> {
    write_magic(buf)?;
    write_aux_fields(buf)?;

    buf.write_all(&[RDB_OPCODE_SELECTDB])?;
    write_len(buf, db.id as u64)?;

    let total_keys = db.size();
    let expires_count = db.expires_count();
    buf.write_all(&[RDB_OPCODE_RESIZEDB])?;
    write_len(buf, total_keys)?;
    write_len(buf, expires_count)?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    for (key, obj) in db.iter_for_eviction() {
        if obj.expire != EXPIRY_NONE && obj.expire < now_ms {
            continue;
        }

        if obj.expire != EXPIRY_NONE {
            buf.write_all(&[RDB_OPCODE_EXPIRETIME_MS])?;
            buf.write_all(&obj.expire.to_le_bytes())?;
        }

        let type_byte = match &obj.kind {
            ObjectKind::String(_) => RDB_TYPE_STRING,
            ObjectKind::Hash(_) => RDB_TYPE_HASH,
            ObjectKind::List(_) => RDB_TYPE_LIST,
            ObjectKind::Set(_) => RDB_TYPE_SET,
            ObjectKind::ZSet(_) => RDB_TYPE_ZSET_2,
            _ => continue,
        };
        buf.write_all(&[type_byte])?;
        write_rdb_string(buf, key.as_bytes())?;
        match &obj.kind {
            ObjectKind::String(_) => save_string_object(buf, obj)?,
            ObjectKind::Hash(_) => save_hash_object(buf, obj)?,
            ObjectKind::List(_) => save_list_object(buf, obj)?,
            ObjectKind::Set(_) => save_set_object(buf, obj)?,
            ObjectKind::ZSet(_) => save_zset_object(buf, obj)?,
            _ => unreachable!(),
        }
    }

    buf.write_all(&[RDB_OPCODE_EOF])?;

    let checksum = crc64(0, buf);
    buf.write_all(&checksum.to_le_bytes())?;

    Ok(())
}

/// Save `db` to the file at `path`, using an atomic write-then-rename strategy.
///
/// A temporary file `<path>.tmp` is written first; on success it is renamed
/// over `path`. This ensures the on-disk file is never partially written.
pub fn save_rdb(db: &RedisDb, path: &Path) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    write_rdb_to_buf(db, &mut buf)?;

    let tmp_path = path.with_extension("rdb.tmp");
    {
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(&buf)?;
        f.flush()?;
    }
    std::fs::rename(&tmp_path, path)
}
