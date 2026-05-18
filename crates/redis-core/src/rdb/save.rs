//! RDB save path — `RdbSaver` writes the complete RDB file.
//!
//! Round 18 framework:
//!   - Magic header + AUX fields
//!   - SELECTDB(0) + RESIZEDB hints
//!   - Per-key: optional EXPIRETIME_MS + RDB_TYPE_STRING + empty-string payload
//!     (placeholder; Round 19+ replaces the payload with real serialization)
//!   - EOF + CRC64 trailer
//!
//! The saver accumulates everything in a `Vec<u8>` so the CRC can be computed
//! over the entire byte stream before writing to disk.

use std::io::{self, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::RedisDb;
use crate::object::EXPIRY_NONE;

use super::crc::crc64;
use super::header::{
    write_aux_fields, write_magic, write_rdb_string, RDB_OPCODE_EOF, RDB_OPCODE_EXPIRETIME_MS,
    RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB, RDB_TYPE_STRING,
};
use super::varint::write_len;

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

        buf.write_all(&[RDB_TYPE_STRING])?;
        write_rdb_string(buf, key.as_bytes())?;
        write_rdb_string(buf, b"")?;
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
