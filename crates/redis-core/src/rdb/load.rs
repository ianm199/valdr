//! RDB load path — `load_into` reads an RDB file and populates a `RedisDb`.
//!
//! Round 19a: `RDB_TYPE_STRING` is now loaded with full encoding fidelity via
//! `load_string_object` — producing `StringEncoding::Int`, `Embstr`, or `Raw`
//! depending on the wire encoding. The `OBJECT ENCODING` command will report the
//! correct encoding after a round-trip.
//!
//! Framework opcodes handled: SELECTDB, RESIZEDB, AUX, EXPIRETIME_MS,
//! EXPIRETIME, IDLE, FREQ, EOF. Unknown type bytes are rejected.
//!
//! The CRC64 trailer is verified when present and non-zero.

use std::io::{self, BufReader, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::RedisDb;
use crate::object::EXPIRY_NONE;
use redis_types::RedisString;

use super::crc::crc64;
use super::hash::load_hash_object;
use super::header::{
    read_magic, read_rdb_string, RDB_OPCODE_AUX, RDB_OPCODE_EOF, RDB_OPCODE_EXPIRETIME,
    RDB_OPCODE_EXPIRETIME_MS, RDB_OPCODE_FREQ, RDB_OPCODE_FUNCTION2, RDB_OPCODE_IDLE,
    RDB_OPCODE_MODULE_AUX, RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB, RDB_OPCODE_SLOT_IMPORT,
    RDB_OPCODE_SLOT_INFO, RDB_TYPE_HASH, RDB_TYPE_HASH_2, RDB_TYPE_HASH_LISTPACK,
    RDB_TYPE_HASH_ZIPLIST, RDB_TYPE_LIST, RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_LIST_QUICKLIST_2,
    RDB_TYPE_LIST_ZIPLIST, RDB_TYPE_SET, RDB_TYPE_SET_INTSET, RDB_TYPE_SET_LISTPACK,
    RDB_TYPE_STRING, RDB_TYPE_ZSET, RDB_TYPE_ZSET_2, RDB_TYPE_ZSET_LISTPACK,
    RDB_TYPE_ZSET_ZIPLIST,
};
use super::list::{load_list_object, load_quicklist2_object};
use super::set::load_set_object;
use super::string::load_string_object;
use super::varint::load_len;
use super::zset::load_zset_object;

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
                let mut obj = load_value(&mut reader, type_byte)?;

                let expire = pending_expire;
                pending_expire = EXPIRY_NONE;

                if expire != EXPIRY_NONE && expire < now_ms {
                    continue;
                }

                obj.expire = expire;
                let key = RedisString::from_vec(key_bytes);
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

/// Load the value payload for a given RDB type byte, returning a `RedisObject`.
///
/// `RDB_TYPE_STRING` uses the encoding-aware `load_string_object`.
/// `RDB_TYPE_HASH` uses `load_hash_object` (flat field/value pairs).
/// `RDB_TYPE_HASH_ZIPLIST`, `RDB_TYPE_HASH_LISTPACK`, and `RDB_TYPE_HASH_2`
/// return a graceful error so the caller can decide whether to skip or abort.
/// Unknown type bytes are rejected with an unsupported error.
fn load_value(reader: &mut impl Read, type_byte: u8) -> io::Result<crate::object::RedisObject> {
    match type_byte {
        RDB_TYPE_STRING => load_string_object(reader),
        RDB_TYPE_HASH => load_hash_object(reader),
        RDB_TYPE_LIST => load_list_object(reader),
        RDB_TYPE_SET => load_set_object(reader),
        RDB_TYPE_HASH_ZIPLIST => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "RDB_TYPE_HASH_ZIPLIST (13) not yet supported on load; set hash-max-listpack-entries 0 in C Valkey before SAVE",
        )),
        RDB_TYPE_HASH_LISTPACK => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "RDB_TYPE_HASH_LISTPACK (16) not yet supported on load; set hash-max-listpack-entries 0 in C Valkey before SAVE",
        )),
        RDB_TYPE_HASH_2 => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "RDB_TYPE_HASH_2 (22) field-level expiry not yet supported on load",
        )),
        RDB_TYPE_LIST_QUICKLIST_2 => load_quicklist2_object(reader),
        RDB_TYPE_LIST_ZIPLIST | RDB_TYPE_LIST_QUICKLIST => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "LIST ziplist/quicklist (v1) encoding not supported on load; these are obsolete formats from Redis < 7.0",
        )),
        RDB_TYPE_SET_INTSET | RDB_TYPE_SET_LISTPACK => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "SET intset/listpack encoding not supported on load; set set-max-intset-entries 0 and set-max-listpack-entries 0 in C Valkey before SAVE",
        )),
        RDB_TYPE_ZSET_2 => load_zset_object(reader),
        RDB_TYPE_ZSET => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "RDB_TYPE_ZSET (3) text-encoded scores not supported; set zset-max-listpack-entries 0 in C Valkey 7+ and use SAVE to produce ZSET_2",
        )),
        RDB_TYPE_ZSET_ZIPLIST => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "RDB_TYPE_ZSET_ZIPLIST (12) not supported on load; set zset-max-listpack-entries 0 before SAVE",
        )),
        RDB_TYPE_ZSET_LISTPACK => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "RDB_TYPE_ZSET_LISTPACK (17) not supported on load; set zset-max-listpack-entries 0 before SAVE",
        )),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("RDB type 0x{:02x} not yet handled (Round 23+)", type_byte),
        )),
    }
}
