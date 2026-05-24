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

use std::io::{self, BufReader, Cursor, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::RedisDb;
use crate::object::EXPIRY_NONE;
use redis_types::RedisString;

use super::crc::crc64;
use super::hash::load_hash_object;
use super::header::{
    read_magic, read_rdb_string, RDB_DUMP_VERSION, RDB_OPCODE_AUX, RDB_OPCODE_EOF,
    RDB_OPCODE_EXPIRETIME, RDB_OPCODE_EXPIRETIME_MS, RDB_OPCODE_FREQ, RDB_OPCODE_FUNCTION2,
    RDB_OPCODE_IDLE, RDB_OPCODE_MODULE_AUX, RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB,
    RDB_OPCODE_SLOT_IMPORT, RDB_OPCODE_SLOT_INFO, RDB_TYPE_BLOOM_NATIVE, RDB_TYPE_HASH,
    RDB_TYPE_HASH_2, RDB_TYPE_HASH_LISTPACK, RDB_TYPE_HASH_ZIPLIST, RDB_TYPE_JSON_NATIVE,
    RDB_TYPE_LIST, RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_LIST_QUICKLIST_2, RDB_TYPE_LIST_ZIPLIST,
    RDB_TYPE_SET, RDB_TYPE_SET_INTSET, RDB_TYPE_SET_LISTPACK, RDB_TYPE_STREAM_LISTPACKS,
    RDB_TYPE_STREAM_LISTPACKS_2, RDB_TYPE_STREAM_LISTPACKS_3, RDB_TYPE_STRING, RDB_TYPE_ZSET,
    RDB_TYPE_ZSET_2, RDB_TYPE_ZSET_LISTPACK, RDB_TYPE_ZSET_ZIPLIST, RDB_VERSION,
};
use super::list::{load_list_object, load_quicklist2_object};
use super::set::load_set_object;
use super::stream::{load_stream_object_2, load_stream_object_3};
use super::string::load_string_object;
use super::varint::load_len;
use super::zset::load_zset_object;

/// Options controlling whole-RDB load behavior.
///
/// `allow_dup` and `aof_preamble` are represented now so command paths can
/// carry the same intent as upstream even though the current HashMap-backed
/// loader naturally overwrites duplicate keys and the RDB preamble path uses
/// the same whole-file framing as ordinary RDB loads.
#[derive(Debug, Clone, Copy)]
pub struct RdbLoadOptions {
    pub allow_dup: bool,
    pub skip_expired: bool,
    pub aof_preamble: bool,
}

impl Default for RdbLoadOptions {
    fn default() -> Self {
        Self {
            allow_dup: false,
            skip_expired: true,
            aof_preamble: false,
        }
    }
}

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
    load_into_dbs(std::slice::from_mut(db), path)
}

/// Load an RDB file at `path` into the supplied logical DB vector.
///
/// `SELECTDB` opcodes switch the destination DB, matching Valkey startup load
/// into `server.db[]`. The caller owns the DB vector; this helper does not
/// touch `global_databases()`.
pub fn load_into_dbs(dbs: &mut [RedisDb], path: &Path) -> io::Result<String> {
    load_into_dbs_with_options(dbs, path, RdbLoadOptions::default())
}

/// Load an RDB file at `path` with explicit load options.
pub fn load_into_dbs_with_options(
    dbs: &mut [RedisDb],
    path: &Path,
    options: RdbLoadOptions,
) -> io::Result<String> {
    if dbs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RDB load requires at least one database",
        ));
    }
    let file = std::fs::File::open(path)?;
    let mut raw = BufReader::new(file);

    let mut body: Vec<u8> = Vec::new();
    raw.read_to_end(&mut body)?;

    if body.len() < 9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RDB file too short",
        ));
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
    let mut selected_db: usize = 0;

    loop {
        let opcode = read_byte(&mut reader)?;

        match opcode {
            RDB_OPCODE_AUX => {
                skip_rdb_string(&mut reader)?;
                skip_rdb_string(&mut reader)?;
            }

            RDB_OPCODE_SELECTDB => {
                let (db_id, _is_enc) = load_len(&mut reader)?;
                let db_index = db_id as usize;
                if db_index >= dbs.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("RDB SELECTDB {} exceeds configured DB count", db_id),
                    ));
                }
                selected_db = db_index;
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

            RDB_OPCODE_MODULE_AUX
            | RDB_OPCODE_FUNCTION2
            | RDB_OPCODE_SLOT_INFO
            | RDB_OPCODE_SLOT_IMPORT => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("RDB opcode 0x{:02x} not supported in Round 18", opcode),
                ));
            }

            RDB_OPCODE_EOF => break,

            type_byte => {
                let key_bytes = read_rdb_string(&mut reader)?;
                let mut obj = load_value_payload(&mut reader, type_byte)?;

                let expire = pending_expire;
                pending_expire = EXPIRY_NONE;

                if options.skip_expired && expire != EXPIRY_NONE && expire < now_ms {
                    continue;
                }

                obj.expire = expire;
                let key = RedisString::from_vec(key_bytes);
                dbs[selected_db].insert(key, obj);
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
pub fn load_value_payload(
    reader: &mut impl Read,
    type_byte: u8,
) -> io::Result<crate::object::RedisObject> {
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
        RDB_TYPE_STREAM_LISTPACKS_3 => load_stream_object_3(reader),
        RDB_TYPE_STREAM_LISTPACKS_2 => load_stream_object_2(reader),
        RDB_TYPE_STREAM_LISTPACKS => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "RDB_TYPE_STREAM_LISTPACKS (15) is a pre-Redis-7 legacy format without first_id / max_deleted_id / entries_added metadata; not supported on load — use Redis 7+ which writes RDB_TYPE_STREAM_LISTPACKS_2 (19) or _3 (21)",
        )),
        RDB_TYPE_JSON_NATIVE => load_json_object(reader),
        RDB_TYPE_BLOOM_NATIVE => load_bloom_object(reader),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("RDB type 0x{:02x} not yet handled (Round 23+)", type_byte),
        )),
    }
}

/// Verify a `DUMP` payload footer and return the embedded RDB version.
///
/// Layout: `<type byte><object payload><u16 RDB version LE><u64 CRC64 LE>`.
/// Strict mode rejects future versions other than Valkey's current no-magic
/// DUMP version; relaxed mode accepts them, matching
/// `CONFIG SET rdb-version-check relaxed`.
pub fn verify_dump_payload(bytes: &[u8], relaxed_version: bool) -> io::Result<u16> {
    if bytes.len() < 10 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DUMP payload too short",
        ));
    }

    let footer = bytes.len() - 10;
    let version = u16::from_le_bytes([bytes[footer], bytes[footer + 1]]);
    let accepted_strict = version <= RDB_VERSION || version == RDB_DUMP_VERSION;
    if version < 1 || (!relaxed_version && !accepted_strict) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DUMP payload RDB version rejected",
        ));
    }

    let stored_crc = u64::from_le_bytes(
        bytes[bytes.len() - 8..]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "cannot read DUMP CRC"))?,
    );
    let computed_crc = crc64(0, &bytes[..bytes.len() - 8]);
    if stored_crc != computed_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DUMP payload CRC mismatch",
        ));
    }

    Ok(version)
}

/// Deserialize a verified `DUMP` payload into a Redis object.
pub fn load_dump_payload(
    bytes: &[u8],
    relaxed_version: bool,
) -> io::Result<crate::object::RedisObject> {
    verify_dump_payload(bytes, relaxed_version)?;
    let body = &bytes[..bytes.len() - 10];
    let mut reader = Cursor::new(body);
    let type_byte = read_byte(&mut reader)?;
    let obj = load_value_payload(&mut reader, type_byte)?;
    if reader.position() != body.len() as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes in DUMP payload",
        ));
    }
    Ok(obj)
}

/// Deserialize a `ObjectKind::Json` value from a length-prefixed UTF-8 JSON string.
///
/// Wire format: `read_rdb_string()` → UTF-8 bytes → `serde_json::from_slice`.
fn load_json_object(reader: &mut impl Read) -> io::Result<crate::object::RedisObject> {
    let bytes = read_rdb_string(reader)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(crate::object::RedisObject::new_json(value))
}

/// Deserialize a `ObjectKind::Bloom` value from the fixed binary record written by
/// `save_bloom_object`.
///
/// Wire format (all integers little-endian):
///   capacity    u64  (8 bytes)
///   item_count  u64  (8 bytes)
///   n_hashes    u32  (4 bytes)
///   error_rate  f64  (8 bytes, IEEE 754)
///   expansion   u32  (4 bytes)
///   nonscaling  u8   (1 byte, 0 or 1)
///   bits        read_rdb_string() → Vec<u8>
fn load_bloom_object(reader: &mut impl Read) -> io::Result<crate::object::RedisObject> {
    let mut buf8 = [0u8; 8];
    let mut buf4 = [0u8; 4];

    reader.read_exact(&mut buf8)?;
    let capacity = u64::from_le_bytes(buf8);

    reader.read_exact(&mut buf8)?;
    let item_count = u64::from_le_bytes(buf8);

    reader.read_exact(&mut buf4)?;
    let n_hashes = u32::from_le_bytes(buf4);

    reader.read_exact(&mut buf8)?;
    let error_rate = f64::from_le_bytes(buf8);

    reader.read_exact(&mut buf4)?;
    let expansion = u32::from_le_bytes(buf4);

    let nonscaling_byte = read_byte(reader)?;
    let nonscaling = nonscaling_byte != 0;

    let bits = read_rdb_string(reader)?;

    let bf = crate::object::BloomFilter {
        capacity,
        item_count,
        n_hashes,
        error_rate,
        expansion,
        nonscaling,
        bits,
    };
    Ok(crate::object::RedisObject::new_bloom_from_filter(bf))
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/rdb.c SELECTDB load semantics
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Startup load can populate a caller-owned DB slice; SELECTDB
//                  is bounded by that slice instead of `global_databases()`.
// ──────────────────────────────────────────────────────────────────────────
