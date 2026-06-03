//! RDB load path — `load_into` reads an RDB file and populates a `RedisDb`.
//! Round 19a: `RDB_TYPE_STRING` is now loaded with full encoding fidelity via
//! `load_string_object` — producing `StringEncoding::Int`, `Embstr`, or `Raw`
//! depending on the wire encoding. The `OBJECT ENCODING` command will report
//! correct encoding after a round-trip.
//! Framework opcodes handled: SELECTDB, RESIZEDB, AUX, EXPIRETIME_MS,
//! EXPIRETIME, IDLE, FREQ, EOF. Unknown type bytes are rejected.
//! The CRC64 trailer is verified when present and non-zero.

use std::io::{self, BufReader, Cursor, Read};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Whether RDB/DUMP CRC64 checksums are verified on load. Toggled by
/// `DEBUG SET-SKIP-CHECKSUM-VALIDATION`; mirrors `server.skip_checksum_validation`
/// in Valkey. The corrupt-dump tests set this so a payload whose body is
/// deliberately corrupt but whose trailing checksum was not recomputed still
/// reaches the type-level integrity checks instead of being rejected on CRC.
static SKIP_CHECKSUM_VALIDATION: AtomicBool = AtomicBool::new(false);
static LAST_LOAD_KEYS_EXPIRED: AtomicI64 = AtomicI64::new(0);
static LAST_LOAD_KEYS_LOADED: AtomicI64 = AtomicI64::new(0);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RdbLoadStats {
    pub keys_expired: i64,
    pub keys_loaded: i64,
}

pub fn set_skip_checksum_validation(skip: bool) {
    SKIP_CHECKSUM_VALIDATION.store(skip, Ordering::Relaxed);
}

pub fn skip_checksum_validation() -> bool {
    SKIP_CHECKSUM_VALIDATION.load(Ordering::Relaxed)
}

pub fn last_load_stats() -> RdbLoadStats {
    RdbLoadStats {
        keys_expired: LAST_LOAD_KEYS_EXPIRED.load(Ordering::Relaxed),
        keys_loaded: LAST_LOAD_KEYS_LOADED.load(Ordering::Relaxed),
    }
}

fn store_last_load_stats(stats: RdbLoadStats) {
    LAST_LOAD_KEYS_EXPIRED.store(stats.keys_expired.max(0), Ordering::Relaxed);
    LAST_LOAD_KEYS_LOADED.store(stats.keys_loaded.max(0), Ordering::Relaxed);
}

use crate::db::RedisDb;
use crate::object::EXPIRY_NONE;
use redis_types::RedisString;

use super::crc::crc64;
use super::hash::load_hash_object;
use super::header::{
    read_magic, read_rdb_string, RDB_DUMP_VERSION, RDB_MAGIC_REDIS, RDB_MAGIC_VALKEY,
    RDB_OPCODE_AUX, RDB_OPCODE_EOF, RDB_OPCODE_EXPIRETIME, RDB_OPCODE_EXPIRETIME_MS,
    RDB_OPCODE_FREQ, RDB_OPCODE_FUNCTION2, RDB_OPCODE_IDLE, RDB_OPCODE_MODULE_AUX,
    RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB, RDB_OPCODE_SLOT_IMPORT, RDB_OPCODE_SLOT_INFO,
    RDB_TYPE_BLOOM_NATIVE, RDB_TYPE_HASH, RDB_TYPE_HASH_2, RDB_TYPE_HASH_LISTPACK,
    RDB_TYPE_HASH_ZIPLIST, RDB_TYPE_HASH_ZIPMAP, RDB_TYPE_JSON_NATIVE, RDB_TYPE_LIST,
    RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_LIST_QUICKLIST_2, RDB_TYPE_LIST_ZIPLIST, RDB_TYPE_SET,
    RDB_TYPE_SET_INTSET, RDB_TYPE_SET_LISTPACK, RDB_TYPE_STREAM_LISTPACKS,
    RDB_TYPE_STREAM_LISTPACKS_2, RDB_TYPE_STREAM_LISTPACKS_3, RDB_TYPE_STRING, RDB_TYPE_ZSET,
    RDB_TYPE_ZSET_2, RDB_TYPE_ZSET_LISTPACK, RDB_TYPE_ZSET_ZIPLIST, RDB_VERSION,
};
use super::list::{load_list_object, load_quicklist2_object};
use super::set::load_set_object;
use super::stream::{load_stream_object_2, load_stream_object_3, load_stream_object_legacy};
use super::string::load_string_object;
use super::varint::load_len;
use super::zset::{load_zset_object, load_zset_v1_object};

/// Options controlling whole-RDB load behavior.
/// `allow_dup` and `aof_preamble` are represented now so command paths can
/// carry the same intent as upstream even though the current HashMap-backed
/// loader naturally overwrites duplicate keys and the RDB preamble path uses
/// the same whole-file framing as ordinary RDB loads.
#[derive(Debug, Clone, Copy)]
pub struct RdbLoadOptions {
    pub allow_dup: bool,
    pub skip_expired: bool,
    pub aof_preamble: bool,
    pub relaxed_version_check: bool,
}

impl Default for RdbLoadOptions {
    fn default() -> Self {
        Self {
            allow_dup: false,
            skip_expired: true,
            aof_preamble: false,
            relaxed_version_check: false,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RdbMagic {
    Redis,
    Valkey,
}

fn read_file_magic(body: &[u8]) -> io::Result<(RdbMagic, u16)> {
    if body.len() < 9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RDB file too short",
        ));
    }

    if body[..9].starts_with(RDB_MAGIC_REDIS) {
        let version_str = std::str::from_utf8(&body[5..9])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 RDB version"))?;
        let version = version_str
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-numeric RDB version"))?;
        Ok((RdbMagic::Redis, version))
    } else if body[..9].starts_with(RDB_MAGIC_VALKEY) {
        let version_str = std::str::from_utf8(&body[6..9]).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 VALKEY RDB version")
        })?;
        let version = version_str.parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "non-numeric VALKEY RDB version")
        })?;
        Ok((RdbMagic::Valkey, version))
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RDB magic",
        ))
    }
}

const RDB_FOREIGN_VERSION_MIN: u16 = 12;
const RDB_FOREIGN_VERSION_MAX: u16 = 79;
const RDB_NATIVE_VALKEY_VERSION: u16 = RDB_DUMP_VERSION;
const RDB_FOREIGN_TYPE_MIN: u8 = RDB_TYPE_HASH_2;
const RDB_FOREIGN_TYPE_MAX: u8 = RDB_OPCODE_SLOT_IMPORT;

fn is_foreign_rdb_version(version: u16) -> bool {
    (RDB_FOREIGN_VERSION_MIN..=RDB_FOREIGN_VERSION_MAX).contains(&version)
}

fn is_foreign_rdb_type_or_opcode(type_byte: u8) -> bool {
    (RDB_FOREIGN_TYPE_MIN..=RDB_FOREIGN_TYPE_MAX).contains(&type_byte)
}

fn validate_file_version(magic: RdbMagic, version: u16, options: RdbLoadOptions) -> io::Result<()> {
    let invalid_magic_version = match magic {
        RdbMagic::Redis => version > RDB_FOREIGN_VERSION_MAX,
        RdbMagic::Valkey => version <= RDB_FOREIGN_VERSION_MAX,
    };
    if version < 1 || invalid_magic_version {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Can't handle RDB format version {}", version),
        ));
    }

    if !options.relaxed_version_check
        && (version > RDB_NATIVE_VALKEY_VERSION || is_foreign_rdb_version(version))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Can't handle RDB format version {}", version),
        ));
    }

    Ok(())
}

fn unknown_type_or_opcode_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Unknown type or opcode when loading DB. Unrecoverable error, aborting now.",
    )
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
/// On success the loaded key count and (if known) source version are returned
/// in the `Ok` string for the caller to log. On failure an `io::Error` is
/// returned; the caller should log and continue without crashing.
pub fn load_into(db: &mut RedisDb, path: &Path) -> io::Result<String> {
    load_into_dbs(std::slice::from_mut(db), path)
}

/// Load an RDB file at `path` into the supplied logical DB vector.
/// `SELECTDB` opcodes switch the destination DB, matching Valkey startup load
/// into `server.db[]`. The caller owns the DB vector; this helper does not
/// touch `global_databases`.
pub fn load_into_dbs(dbs: &mut [RedisDb], path: &Path) -> io::Result<String> {
    load_into_dbs_with_options(dbs, path, RdbLoadOptions::default())
}

/// Load an RDB file at `path` with explicit load options.
pub fn load_into_dbs_with_options(
    dbs: &mut [RedisDb],
    path: &Path,
    options: RdbLoadOptions,
) -> io::Result<String> {
    store_last_load_stats(RdbLoadStats::default());
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

    let (magic, version) = read_file_magic(&body)?;
    validate_file_version(magic, version, options)?;

    let (payload, stored_crc) = if version >= 5 {
        if body.len() < 17 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RDB file too short for checksum trailer",
            ));
        }
        let stored_crc = u64::from_le_bytes(
            body[body.len() - 8..]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "cannot read CRC"))?,
        );
        (&body[..body.len() - 8], Some(stored_crc))
    } else {
        (&body[..], None)
    };

    let mut reader = std::io::Cursor::new(payload);
    let version = read_magic(&mut reader)?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let mut pending_expire: i64 = EXPIRY_NONE;
    let mut keys_loaded: i64 = 0;
    let mut keys_expired: i64 = 0;
    let mut selected_db: usize = 0;

    loop {
        let opcode = read_byte(&mut reader)?;

        if magic == RdbMagic::Redis
            && is_foreign_rdb_version(version)
            && is_foreign_rdb_type_or_opcode(opcode)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Can't handle foreign type or opcode {} in RDB with version {}",
                    opcode, version
                ),
            ));
        }

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
                let mut obj = match load_value_payload(&mut reader, type_byte) {
                    Ok(obj) => obj,
                    Err(err)
                        if err.kind() == io::ErrorKind::Unsupported
                            && err.to_string().contains("not yet handled") =>
                    {
                        return Err(unknown_type_or_opcode_error());
                    }
                    Err(err) => return Err(err),
                };

                let expire = pending_expire;
                pending_expire = EXPIRY_NONE;

                if options.skip_expired && expire != EXPIRY_NONE && expire < now_ms {
                    keys_expired += 1;
                    continue;
                }

                obj.expire = expire;
                let key = RedisString::from_vec(key_bytes);
                dbs[selected_db].insert(key, obj);
                keys_loaded += 1;
            }
        }
    }

    if let Some(stored_crc) = stored_crc {
        if stored_crc != 0 && !skip_checksum_validation() {
            let computed = crc64(0, payload);
            if computed != stored_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "RDB CRC error: wrong RDB checksum expected 0x{:016x} but got 0x{:016x}",
                        computed, stored_crc
                    ),
                ));
            }
        }
    }

    store_last_load_stats(RdbLoadStats {
        keys_expired,
        keys_loaded,
    });

    Ok(format!(
        "DB loaded from RDB version {} — {} keys",
        version, keys_loaded
    ))
}

const CHECK_FOREIGN_MIN: u16 = 12;
const CHECK_FOREIGN_MAX: u16 = 79;
const CHECK_NATIVE_VERSION: u16 = 80;

/// Result of a `valkey-check-rdb` scan: the offset-prefixed report lines (each
/// already `[offset N] msg`) and whether the file is OK.
pub struct RdbCheckReport {
    pub lines: Vec<String>,
    pub ok: bool,
}

/// Scan an RDB file the way `valkey-check-rdb` does: validate the signature
/// version, classify foreign (12-79) / future (>80) versions, walk every
/// opcode/object tracking the byte offset, and report the first error or a
/// final "RDB looks OK" line.
pub fn check_rdb_file(path: &Path) -> RdbCheckReport {
    let mut lines: Vec<String> = Vec::new();
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) => {
            lines.push("--- RDB ERROR DETECTED ---".to_string());
            lines.push(format!(
                "[offset 0] Can't open RDB file {}: {}",
                path.display(),
                e
            ));
            return RdbCheckReport { lines, ok: false };
        }
    };
    if data.len() < 9 {
        lines.push("--- RDB ERROR DETECTED ---".to_string());
        lines.push("[offset 0] Unexpected EOF reading RDB file".to_string());
        return RdbCheckReport { lines, ok: false };
    }
    let is_redis = &data[0..6] == b"REDIS0";
    let is_valkey = &data[0..6] == b"VALKEY";
    if !is_redis && !is_valkey {
        lines.push("--- RDB ERROR DETECTED ---".to_string());
        lines.push("[offset 0] Wrong signature trying to load DB from file".to_string());
        return RdbCheckReport { lines, ok: false };
    }
    let version: u16 = std::str::from_utf8(&data[6..9])
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    if version < 1
        || (version < CHECK_FOREIGN_MIN && !is_redis)
        || (version > CHECK_FOREIGN_MAX && !is_valkey)
    {
        lines.push("--- RDB ERROR DETECTED ---".to_string());
        lines.push(format!(
            "[offset 9] Can't handle RDB format version {}",
            version
        ));
        return RdbCheckReport { lines, ok: false };
    }
    let is_future = version > CHECK_NATIVE_VERSION;
    let is_foreign = (CHECK_FOREIGN_MIN..=CHECK_FOREIGN_MAX).contains(&version);
    let version_word = if is_future { "future" } else { "foreign" };
    if is_future {
        lines.push(format!(
            "[offset 9] Future RDB version {} detected",
            version
        ));
    } else if is_foreign {
        lines.push(format!(
            "[offset 9] Foreign RDB version {} detected",
            version
        ));
    }

    // Scan the whole file: the RDB_OPCODE_EOF marker is the true end of
    // object stream. Any trailing CRC64 (present only for rdb_ver >= 5) follows
    // EOF and is simply never read — and old versions (e.g. RDB v4) have no CRC
    // at all, so stripping a fixed 8-byte footer would truncate real data.
    let mut reader = Cursor::new(&data[..]);
    reader.set_position(9);

    let scan: io::Result<()> = (|| {
        loop {
            let opcode = read_byte(&mut reader)?;
            match opcode {
                RDB_OPCODE_AUX => {
                    skip_rdb_string(&mut reader)?;
                    skip_rdb_string(&mut reader)?;
                }
                RDB_OPCODE_SELECTDB | RDB_OPCODE_IDLE => {
                    let _ = load_len(&mut reader)?;
                }
                RDB_OPCODE_RESIZEDB => {
                    let _ = load_len(&mut reader)?;
                    let _ = load_len(&mut reader)?;
                }
                RDB_OPCODE_EXPIRETIME_MS => {
                    read_u64_le(&mut reader)?;
                }
                RDB_OPCODE_EXPIRETIME => {
                    read_u32_le(&mut reader)?;
                }
                RDB_OPCODE_FREQ => {
                    read_byte(&mut reader)?;
                }
                RDB_OPCODE_EOF => break,
                RDB_OPCODE_MODULE_AUX
                | RDB_OPCODE_FUNCTION2
                | RDB_OPCODE_SLOT_INFO
                | RDB_OPCODE_SLOT_IMPORT => {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!("unsupported RDB opcode 0x{:02x}", opcode),
                    ));
                }
                type_byte => {
                    let _key = read_rdb_string(&mut reader)?;
                    if let Err(e) = load_value_payload(&mut reader, type_byte) {
                        if e.to_string().contains("not yet handled") {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "Unknown object type {} in RDB file with {} version {}",
                                    type_byte, version_word, version
                                ),
                            ));
                        }
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    })();

    match scan {
        Ok(()) => {
            let off = reader.position();
            if is_foreign || is_future {
                lines.push(format!(
                    "[offset {}] \\o/ RDB looks OK, but loading requires config 'rdb-version-check relaxed'",
                    off
                ));
            } else {
                lines.push(format!("[offset {}] \\o/ RDB looks OK! \\o/", off));
            }
            RdbCheckReport { lines, ok: true }
        }
        Err(e) => {
            let off = reader.position();
            lines.push("--- RDB ERROR DETECTED ---".to_string());
            lines.push(format!("[offset {}] {}", off, e));
            RdbCheckReport { lines, ok: false }
        }
    }
}

/// Load the value payload for a given RDB type byte, returning a `RedisObject`.
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
        RDB_TYPE_HASH_LISTPACK => super::hash::load_hash_listpack_object(reader),
        RDB_TYPE_HASH_2 => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "RDB_TYPE_HASH_2 (22) field-level expiry not yet supported on load",
        )),
        RDB_TYPE_HASH_ZIPLIST => super::hash::load_hash_ziplist_object(reader),
        RDB_TYPE_HASH_ZIPMAP => super::hash::load_hash_zipmap_object(reader),
        RDB_TYPE_LIST_QUICKLIST_2 => load_quicklist2_object(reader),
        RDB_TYPE_LIST_ZIPLIST => super::list::load_list_ziplist_object(reader),
        RDB_TYPE_LIST_QUICKLIST => super::list::load_quicklist_object(reader),
        RDB_TYPE_SET_INTSET => super::set::load_set_intset_object(reader),
        RDB_TYPE_SET_LISTPACK => super::set::load_set_listpack_object(reader),
        RDB_TYPE_ZSET_2 => load_zset_object(reader),
        RDB_TYPE_ZSET => load_zset_v1_object(reader),
        RDB_TYPE_ZSET_ZIPLIST => super::zset::load_zset_ziplist_object(reader),
        RDB_TYPE_ZSET_LISTPACK => super::zset::load_zset_listpack_object(reader),
        RDB_TYPE_STREAM_LISTPACKS_3 => load_stream_object_3(reader),
        RDB_TYPE_STREAM_LISTPACKS_2 => load_stream_object_2(reader),
        RDB_TYPE_STREAM_LISTPACKS => load_stream_object_legacy(reader),
        RDB_TYPE_JSON_NATIVE => load_json_object(reader),
        RDB_TYPE_BLOOM_NATIVE => load_bloom_object(reader),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("RDB type 0x{:02x} not yet handled (Round 23+)", type_byte),
        )),
    }
}

/// Verify a `DUMP` payload footer and return the embedded RDB version.
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
    if !skip_checksum_validation() && stored_crc != computed_crc {
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
/// Wire format: `read_rdb_string` → UTF-8 bytes → `serde_json::from_slice`.
fn load_json_object(reader: &mut impl Read) -> io::Result<crate::object::RedisObject> {
    let bytes = read_rdb_string(reader)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(crate::object::RedisObject::new_json(value))
}

/// Deserialize a `ObjectKind::Bloom` value from the fixed binary record written by
/// `save_bloom_object`.
/// Wire format (all integers little-endian):
/// capacity u64 (8 bytes)
/// item_count u64 (8 bytes)
/// n_hashes u32 (4 bytes)
/// error_rate f64 (8 bytes, IEEE 754)
/// expansion u32 (4 bytes)
/// nonscaling u8 (1 byte, 0 or 1)
/// bits read_rdb_string → Vec<u8>
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

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("reference/valkey/tests/assets")
            .join(name)
    }

    #[test]
    fn redis_rdb_v4_loads_without_checksum_trailer() {
        let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let msg = load_into_dbs_with_options(
            &mut dbs,
            &fixture("encodings.rdb"),
            RdbLoadOptions::default(),
        )
        .expect("RDB v4 fixture has no checksum trailer and should load");

        assert!(msg.contains("RDB version 4"));
        assert_eq!(dbs.iter().map(RedisDb::len).sum::<usize>(), 13);
        assert!(dbs[0].find(&RedisString::from_static(b"string")).is_some());
    }

    #[test]
    fn future_rdb_requires_relaxed_version_check() {
        let path = fixture("encodings-rdb987.rdb");
        let mut strict: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let err = load_into_dbs_with_options(&mut strict, &path, RdbLoadOptions::default())
            .expect_err("future RDB fixture must be rejected in strict mode");
        assert!(err
            .to_string()
            .contains("Can't handle RDB format version 987"));

        let mut relaxed: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let msg = load_into_dbs_with_options(
            &mut relaxed,
            &path,
            RdbLoadOptions {
                relaxed_version_check: true,
                ..Default::default()
            },
        )
        .expect("relaxed version check should load the future fixture");
        assert!(msg.contains("RDB version 987"));
        assert_eq!(relaxed.iter().map(RedisDb::len).sum::<usize>(), 13);
    }

    #[test]
    fn relaxed_future_unknown_type_reports_valkey_fatal_reason() {
        let path = fixture("encodings-rdb987-unknown-types.rdb");
        let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let err = load_into_dbs_with_options(
            &mut dbs,
            &path,
            RdbLoadOptions {
                relaxed_version_check: true,
                ..Default::default()
            },
        )
        .expect_err("future RDB with unknown types must fail after version acceptance");

        assert_eq!(
            err.to_string(),
            "Unknown type or opcode when loading DB. Unrecoverable error, aborting now."
        );
    }

    #[test]
    fn relaxed_foreign_unknown_type_reports_foreign_opcode() {
        let path = fixture("encodings-rdb75-unknown-types.rdb");
        let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let err = load_into_dbs_with_options(
            &mut dbs,
            &path,
            RdbLoadOptions {
                relaxed_version_check: true,
                ..Default::default()
            },
        )
        .expect_err("foreign RDB with unknown type/opcode must fail explicitly");

        assert_eq!(
            err.to_string(),
            "Can't handle foreign type or opcode 150 in RDB with version 75"
        );
    }

    #[test]
    fn load_stats_count_loaded_and_expired_keys() {
        let path = std::env::temp_dir().join(format!(
            "redis-rs-rdb-load-stats-{}-{}.rdb",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let mut source = RedisDb::new(0);
        source.insert(
            RedisString::from_static(b"live"),
            crate::object::RedisObject::from_string(RedisString::from_static(b"value")),
        );
        let mut expired = crate::object::RedisObject::from_string(RedisString::from_static(b"old"));
        expired.expire = 1;
        source.insert(RedisString::from_static(b"expired"), expired);

        crate::rdb::save::save_rdb_databases(&[source], &path).expect("test RDB should save");
        let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let msg = load_into_dbs_with_options(&mut dbs, &path, RdbLoadOptions::default())
            .expect("test RDB should load");
        let stats = last_load_stats();
        let _ = std::fs::remove_file(&path);

        assert!(msg.contains("1 keys"));
        assert_eq!(stats.keys_loaded, 1);
        assert_eq!(stats.keys_expired, 1);
        assert!(dbs[0].find(&RedisString::from_static(b"live")).is_some());
        assert!(dbs[0].find(&RedisString::from_static(b"expired")).is_none());
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Startup load can populate a caller-owned DB slice; SELECTDB
//                  is bounded by that slice instead of `global_databases()`.
// ──────────────────────────────────────────────────────────────────────────
