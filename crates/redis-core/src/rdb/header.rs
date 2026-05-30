//! RDB file header: magic bytes, version, AUX fields, EOF.
//! Write path: `write_header` emits `REDIS0011` + mandatory AUX fields.
//! Read path: `read_header` validates the magic and version.
//! RDB v11 uses the `REDIS` magic prefix (not `VALKEY`, which is v80).
//! Valkey 8.x reads RDB v11 files written with the REDIS magic without error.

use std::io::{self, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use super::varint::{write_len, RDB_ENCVAL, RDB_ENC_INT16, RDB_ENC_INT32};

pub const RDB_VERSION: u16 = 11;
/// Valkey's no-magic DUMP/RESTORE payload version.
/// Full files use `REDIS0011` for cross-version compatibility with the RDB
/// corpus. DUMP payloads have no magic prefix, so Valkey 8.x identifies
/// payload by this version footer instead.
pub const RDB_DUMP_VERSION: u16 = 80;
pub const RDB_MAGIC_REDIS: &[u8] = b"REDIS";
pub const RDB_MAGIC_VALKEY: &[u8] = b"VALKEY";

/// RDB opcodes.
pub const RDB_OPCODE_EOF: u8 = 0xFF;
pub const RDB_OPCODE_SELECTDB: u8 = 0xFE;
pub const RDB_OPCODE_EXPIRETIME: u8 = 0xFD;
pub const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
pub const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
pub const RDB_OPCODE_AUX: u8 = 0xFA;
pub const RDB_OPCODE_FREQ: u8 = 0xF9;
pub const RDB_OPCODE_IDLE: u8 = 0xF8;
pub const RDB_OPCODE_MODULE_AUX: u8 = 247;
pub const RDB_OPCODE_FUNCTION2: u8 = 245;
pub const RDB_OPCODE_SLOT_INFO: u8 = 244;
pub const RDB_OPCODE_SLOT_IMPORT: u8 = 243;

/// RDB type constants.
pub const RDB_TYPE_STRING: u8 = 0;
pub const RDB_TYPE_LIST: u8 = 1;
pub const RDB_TYPE_SET: u8 = 2;
pub const RDB_TYPE_HASH: u8 = 4;
pub const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
pub const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
pub const RDB_TYPE_SET_INTSET: u8 = 11;
pub const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
pub const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
pub const RDB_TYPE_HASH_LISTPACK: u8 = 16;
pub const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
pub const RDB_TYPE_SET_LISTPACK: u8 = 20;
pub const RDB_TYPE_HASH_2: u8 = 22;

/// ZSET type constants — Round 22.
pub const RDB_TYPE_ZSET: u8 = 3;
pub const RDB_TYPE_ZSET_2: u8 = 5;
pub const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
pub const RDB_TYPE_ZSET_LISTPACK: u8 = 17;

/// STREAM type constants — Round 23.
/// We always emit `RDB_TYPE_STREAM_LISTPACKS_3` (with consumer `active_time`)
/// on save. On load we accept `_3` and `_2`. The legacy `RDB_TYPE_STREAM_LISTPACKS`
/// (v1) is rejected because it lacks the explicit `first_id`, `max_deleted_id`,
/// and `entries_added` metadata fields that our `InlineStream` model requires
/// to round-trip without inference.
pub const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;
pub const RDB_TYPE_STREAM_LISTPACKS_2: u8 = 19;
pub const RDB_TYPE_STREAM_LISTPACKS_3: u8 = 21;

/// Private opcodes for native types that have no byte-compatible counterpart
/// real Valkey. These values are intentionally above all known standard type
/// bytes (max 22 for HASH_2) and below the lowest opcode (0xF8 = 248) so they
/// cannot be confused with either category. Real Valkey will reject RDB files
/// that contain these bytes; that is expected — they are only meaningful in a
/// our-save ↔ our-load round-trip.
pub const RDB_TYPE_JSON_NATIVE: u8 = 200;
pub const RDB_TYPE_BLOOM_NATIVE: u8 = 201;

/// Write an RDB string (raw bytes prefixed by length).
/// For this round we always emit raw bytes without integer encoding or LZF.
/// Round 20 will add integer encoding; Round 27 will add LZF compression.
pub fn write_rdb_string(writer: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    write_len(writer, bytes.len() as u64)?;
    writer.write_all(bytes)
}

/// Write a signed integer value as an RDB-encoded string.
/// Uses the compact `RDB_ENCVAL + RDB_ENC_INT{8,16,32}` form when
/// value fits; falls back to decimal text for larger values.
fn write_rdb_integer_as_string(writer: &mut impl Write, n: i64) -> io::Result<()> {
    if n >= i8::MIN as i64 && n <= i8::MAX as i64 {
        writer.write_all(&[(RDB_ENCVAL << 6) | super::varint::RDB_ENC_INT8, n as u8])
    } else if n >= i16::MIN as i64 && n <= i16::MAX as i64 {
        let bytes = (n as i16).to_le_bytes();
        writer.write_all(&[(RDB_ENCVAL << 6) | RDB_ENC_INT16])?;
        writer.write_all(&bytes)
    } else if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
        let bytes = (n as i32).to_le_bytes();
        writer.write_all(&[(RDB_ENCVAL << 6) | RDB_ENC_INT32])?;
        writer.write_all(&bytes)
    } else {
        let s = n.to_string();
        write_rdb_string(writer, s.as_bytes())
    }
}

/// Write an AUX field (`RDB_OPCODE_AUX key value`).
fn write_aux(writer: &mut impl Write, key: &[u8], value: &[u8]) -> io::Result<()> {
    writer.write_all(&[RDB_OPCODE_AUX])?;
    write_rdb_string(writer, key)?;
    write_rdb_string(writer, value)
}

/// Write an AUX field whose value is a decimal integer.
fn write_aux_integer(writer: &mut impl Write, key: &[u8], n: i64) -> io::Result<()> {
    writer.write_all(&[RDB_OPCODE_AUX])?;
    write_rdb_string(writer, key)?;
    write_rdb_integer_as_string(writer, n)
}

/// Write the 9-byte RDB magic header: `REDIS0011`.
pub fn write_magic(writer: &mut impl Write) -> io::Result<()> {
    let header = format!(
        "{}{:04}",
        std::str::from_utf8(RDB_MAGIC_REDIS).unwrap(),
        RDB_VERSION
    );
    writer.write_all(header.as_bytes())
}

/// Write the mandatory AUX fields that Valkey emits on every save.
pub fn write_aux_fields(writer: &mut impl Write) -> io::Result<()> {
    write_aux(writer, b"redis-ver", b"7.2.0")?;
    write_aux_integer(writer, b"redis-bits", 64)?;
    let ctime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    write_aux_integer(writer, b"ctime", ctime)?;
    write_aux_integer(writer, b"used-mem", 0)?;
    write_aux_integer(writer, b"aof-base", 0)
}

/// Read and validate the RDB magic header (9 bytes total for REDIS, 9 for VALKEY).
/// Format: `REDIS<4-digit-version>` (9 bytes) or `VALKEY<3-digit-version>` (9 bytes).
/// Returns `Ok(version)` on a recognised magic prefix.
pub fn read_magic(reader: &mut impl Read) -> io::Result<u16> {
    let mut magic = [0u8; 9];
    reader.read_exact(&mut magic)?;

    if magic.starts_with(RDB_MAGIC_REDIS) {
        let version_str = std::str::from_utf8(&magic[5..9])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 RDB version"))?;
        version_str
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-numeric RDB version"))
    } else if magic.starts_with(RDB_MAGIC_VALKEY) {
        let version_str = std::str::from_utf8(&magic[6..9]).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 VALKEY RDB version")
        })?;
        version_str.parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "non-numeric VALKEY RDB version")
        })
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RDB magic",
        ))
    }
}

/// Read and parse a raw RDB string (length-prefixed, possibly encoded).
/// Returns the raw bytes on success. Handles `RDB_ENCVAL` integer encodings
/// by converting back to their decimal string representation, which is what
/// Valkey does internally before storing in the key dictionary on load.
pub fn read_rdb_string(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let (len, is_encoded) = super::varint::load_len(reader)?;
    if is_encoded {
        let enc = len as u8;
        match enc {
            super::varint::RDB_ENC_INT8 => {
                let mut b = [0u8; 1];
                reader.read_exact(&mut b)?;
                Ok((b[0] as i8).to_string().into_bytes())
            }
            super::varint::RDB_ENC_INT16 => {
                let mut b = [0u8; 2];
                reader.read_exact(&mut b)?;
                Ok(i16::from_le_bytes(b).to_string().into_bytes())
            }
            super::varint::RDB_ENC_INT32 => {
                let mut b = [0u8; 4];
                reader.read_exact(&mut b)?;
                Ok(i32::from_le_bytes(b).to_string().into_bytes())
            }
            super::varint::RDB_ENC_LZF => {
                let (clen, _) = super::varint::load_len(reader)?;
                let (ulen, _) = super::varint::load_len(reader)?;
                let mut compressed = vec![0u8; clen as usize];
                reader.read_exact(&mut compressed)?;
                super::lzf::lzf_decompress(&compressed, ulen as usize)
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown RDB string encoding",
            )),
        }
    } else {
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}
