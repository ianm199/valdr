//! RDB load path — `load_into` reads an RDB file and populates a `RedisDb`.
//!
//! Round 18 reads the framework opcodes (SELECTDB, RESIZEDB, AUX, EXPIRETIME_MS,
//! EXPIRETIME, EOF) and handles `RDB_TYPE_STRING` key-value pairs. The value
//! payload is read and discarded for all types except STRING (where it is
//! loaded as the key's value). Unknown type bytes are rejected.
//!
//! The CRC64 trailer is verified when present and non-zero.

use std::io::{self, BufReader, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::RedisDb;
use crate::object::{RedisObject, EXPIRY_NONE};
use redis_types::RedisString;

use super::crc::crc64;
use super::header::{
    read_magic, read_rdb_string, RDB_OPCODE_AUX, RDB_OPCODE_EOF, RDB_OPCODE_EXPIRETIME,
    RDB_OPCODE_EXPIRETIME_MS, RDB_OPCODE_FREQ, RDB_OPCODE_FUNCTION2, RDB_OPCODE_IDLE,
    RDB_OPCODE_MODULE_AUX, RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB, RDB_OPCODE_SLOT_IMPORT,
    RDB_OPCODE_SLOT_INFO, RDB_TYPE_STRING,
};
use super::varint::load_len;

/// Read exactly one byte from `reader`.
fn read_byte(reader: &mut impl Read) -> io::Result<u8> {
    let mut b = [0u8; 1];
    reader.read_exact(&mut b)?;
    Ok(b[0])
}

/// Read a 64-bit little-endian integer (used for EXPIRETIME_MS and the CRC trailer).
fn read_u64_le(reader: &mut impl Read) -> io::Result<u64> {
    let mut b = [0u8; 8];
    reader.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

/// Read a 32-bit big-endian integer (used for EXPIRETIME in seconds, legacy form).
fn read_u32_le(reader: &mut impl Read) -> io::Result<u32> {
    let mut b = [0u8; 4];
    reader.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

/// Skip a varint-length-prefixed blob (skips AUX value, IDLE, etc.).
fn skip_rdb_string(reader: &mut impl Read) -> io::Result<()> {
    let (len, is_encoded) = load_len(reader)?;
    if is_encoded {
        let enc = len as u8;
        let skip_bytes: usize = match enc {
            0 => 1,
            1 => 2,
            2 => 4,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "cannot skip unknown encoded string type",
                ))
            }
        };
        let mut discard = vec![0u8; skip_bytes];
        reader.read_exact(&mut discard)?;
    } else {
        let mut discard = vec![0u8; len as usize];
        reader.read_exact(&mut discard)?;
    }
    Ok(())
}

/// Load an RDB file at `path` into `db`, returning a human-readable log line.
///
/// On success the loaded key count and (if known) source version are returned
/// in the `Ok` string for the caller to log. On failure an `io::Error` is
/// returned; the caller should log and continue without crashing.
pub fn load_into(db: &mut RedisDb, path: &Path) -> io::Result<String> {
    let file = std::fs::File::open(path)?;
    let mut raw = BufReader::new(file);

    let mut body: Vec<u8> = Vec::new();
    raw.read_to_end(&mut body)?;

    if body.len() < 9 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "RDB file too short"));
    }

    let stored_crc = u64::from_le_bytes(
        body[body.len() - 8..]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "cannot read CRC"))?,
    );
    let payload = &body[..body.len() - 8];

    if stored_crc != 0 {
        let computed = crc64(0, payload);
        if computed != stored_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "RDB CRC mismatch: file has 0x{:016x}, computed 0x{:016x}",
                    stored_crc, computed
                ),
            ));
        }
    }

    let mut reader = std::io::Cursor::new(payload);
    let version = read_magic(&mut reader)?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let mut pending_expire: i64 = EXPIRY_NONE;
    let mut keys_loaded: u64 = 0;

    loop {
        let opcode = read_byte(&mut reader)?;

        match opcode {
            RDB_OPCODE_AUX => {
                skip_rdb_string(&mut reader)?;
                skip_rdb_string(&mut reader)?;
            }

            RDB_OPCODE_SELECTDB => {
                let (_db_id, _is_enc) = load_len(&mut reader)?;
            }

            RDB_OPCODE_RESIZEDB => {
                let (_dict_size, _) = load_len(&mut reader)?;
                let (_expires_size, _) = load_len(&mut reader)?;
            }

            RDB_OPCODE_EXPIRETIME_MS => {
                pending_expire = read_u64_le(&mut reader)? as i64;
            }

            RDB_OPCODE_EXPIRETIME => {
                let secs = read_u32_le(&mut reader)?;
                pending_expire = (secs as i64) * 1000;
            }

            RDB_OPCODE_IDLE => {
                let (_idle, _) = load_len(&mut reader)?;
            }

            RDB_OPCODE_FREQ => {
                read_byte(&mut reader)?;
            }

            RDB_OPCODE_MODULE_AUX | RDB_OPCODE_FUNCTION2 | RDB_OPCODE_SLOT_INFO
            | RDB_OPCODE_SLOT_IMPORT => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("RDB opcode 0x{:02x} not supported in Round 18", opcode),
                ));
            }

            RDB_OPCODE_EOF => break,

            type_byte => {
                let key_bytes = read_rdb_string(&mut reader)?;
                let value_bytes = read_value(&mut reader, type_byte)?;

                let expire = pending_expire;
                pending_expire = EXPIRY_NONE;

                if expire != EXPIRY_NONE && expire < now_ms {
                    continue;
                }

                let key = RedisString::from_vec(key_bytes);
                let mut obj = RedisObject::new_raw_string(&value_bytes);
                obj.expire = expire;
                db.insert(key, obj);
                keys_loaded += 1;
            }
        }
    }

    Ok(format!(
        "DB loaded from RDB version {} — {} keys",
        version, keys_loaded
    ))
}

/// Read the value payload for a given RDB type byte and return the raw bytes.
///
/// For `RDB_TYPE_STRING` the returned bytes are the string payload.
/// For unknown types we return an error rather than silently skipping, since
/// Round 18 only commits to handling STRING. Later rounds expand this.
fn read_value(reader: &mut impl Read, type_byte: u8) -> io::Result<Vec<u8>> {
    if type_byte == RDB_TYPE_STRING {
        read_rdb_string(reader)
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("RDB type 0x{:02x} not handled in Round 18", type_byte),
        ))
    }
}
