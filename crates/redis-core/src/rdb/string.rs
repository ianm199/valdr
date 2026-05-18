//! RDB string type serialization — Round 19a.
//!
//! Implements `save_string_object` and `load_string_object`, replacing the
//! Round 18 empty-payload placeholder for `RDB_TYPE_STRING`.
//!
//! Save encoding rules (mirroring `rdbSaveRawString` / `rdbSaveLongLongAsStringObject`):
//!   - `StringEncoding::Int(n)`: emit `RDB_ENCVAL | RDB_ENC_INT8/16/32` + little-endian
//!     bytes when `n` fits in i8/i16/i32; fall through to decimal text for larger values.
//!   - `StringEncoding::Embstr(bytes)` and `StringEncoding::Raw(bytes)`: emit
//!     `save_len(bytes.len())` followed by the raw bytes. LZF compression is deferred to
//!     Round 27.
//!
//! Load encoding rules (mirroring `rdbLoadEncodedStringObject` + `tryObjectEncoding`):
//!   - `is_encoded` flag from `load_len`: dispatch on `RDB_ENC_INT8/16/32` →
//!     `StringEncoding::Int(n as i64)`. `RDB_ENC_LZF` → error (Round 27).
//!   - Raw byte payload: `len <= 44` → `StringEncoding::Embstr`, else `StringEncoding::Raw`.

use std::io::{self, Read, Write};

use crate::object::{is_canonical_i64_ascii, ObjectKind, RedisObject, StringEncoding};

use super::lzf::lzf_decompress;
use super::varint::{load_len, write_len, RDB_ENC_INT16, RDB_ENC_INT32, RDB_ENC_INT8, RDB_ENC_LZF, RDB_ENCVAL};

/// Threshold in bytes below which a loaded raw string gets `Embstr` encoding.
const EMBSTR_LIMIT: usize = 44;

/// Serialize a `RDB_TYPE_STRING` value payload for `obj`.
///
/// The type byte itself is written by the caller; this function writes only
/// the value payload that follows it.
pub fn save_string_object(w: &mut impl Write, obj: &RedisObject) -> io::Result<()> {
    match &obj.kind {
        ObjectKind::String(StringEncoding::Int(n)) => save_integer(*n, w),
        ObjectKind::String(StringEncoding::Embstr(bytes)) => save_raw_bytes(w, bytes.as_bytes()),
        ObjectKind::String(StringEncoding::Raw(bytes)) => save_raw_bytes(w, bytes.as_bytes()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "save_string_object called on non-string object",
        )),
    }
}

/// Deserialize a `RDB_TYPE_STRING` value payload, producing a `RedisObject`.
///
/// Reads from `r` starting immediately after the type byte.
pub fn load_string_object(r: &mut impl Read) -> io::Result<RedisObject> {
    let (len, is_encoded) = load_len(r)?;
    if is_encoded {
        load_encoded_string(r, len as u8)
    } else {
        load_raw_bytes(r, len as usize)
    }
}

fn save_integer(n: i64, w: &mut impl Write) -> io::Result<()> {
    if n >= i8::MIN as i64 && n <= i8::MAX as i64 {
        w.write_all(&[(RDB_ENCVAL << 6) | RDB_ENC_INT8, n as u8])
    } else if n >= i16::MIN as i64 && n <= i16::MAX as i64 {
        w.write_all(&[(RDB_ENCVAL << 6) | RDB_ENC_INT16])?;
        w.write_all(&(n as i16).to_le_bytes())
    } else if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
        w.write_all(&[(RDB_ENCVAL << 6) | RDB_ENC_INT32])?;
        w.write_all(&(n as i32).to_le_bytes())
    } else {
        save_raw_bytes(w, n.to_string().as_bytes())
    }
}

fn save_raw_bytes(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    write_len(w, bytes.len() as u64)?;
    w.write_all(bytes)
}

fn load_encoded_string(r: &mut impl Read, enc: u8) -> io::Result<RedisObject> {
    match enc {
        RDB_ENC_INT8 => {
            let mut buf = [0u8; 1];
            r.read_exact(&mut buf)?;
            Ok(RedisObject::new_int_string(buf[0] as i8 as i64))
        }
        RDB_ENC_INT16 => {
            let mut buf = [0u8; 2];
            r.read_exact(&mut buf)?;
            Ok(RedisObject::new_int_string(i16::from_le_bytes(buf) as i64))
        }
        RDB_ENC_INT32 => {
            let mut buf = [0u8; 4];
            r.read_exact(&mut buf)?;
            Ok(RedisObject::new_int_string(i32::from_le_bytes(buf) as i64))
        }
        RDB_ENC_LZF => {
            let (clen, _) = load_len(r)?;
            let (ulen, _) = load_len(r)?;
            let mut compressed = vec![0u8; clen as usize];
            r.read_exact(&mut compressed)?;
            let bytes = lzf_decompress(&compressed, ulen as usize)?;
            load_raw_bytes(&mut std::io::Cursor::new(bytes), ulen as usize)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown RDB string encoding byte: 0x{:02x}", enc),
        )),
    }
}

fn load_raw_bytes(r: &mut impl Read, len: usize) -> io::Result<RedisObject> {
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    if is_canonical_i64_ascii(&buf) {
        if let Ok(s) = std::str::from_utf8(&buf) {
            if let Ok(n) = s.parse::<i64>() {
                return Ok(RedisObject::new_int_string(n));
            }
        }
    }
    if len <= EMBSTR_LIMIT {
        Ok(RedisObject::new_embstr(&buf))
    } else {
        Ok(RedisObject::new_raw_string(&buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(obj: RedisObject) -> RedisObject {
        let mut buf: Vec<u8> = Vec::new();
        save_string_object(&mut buf, &obj).unwrap();
        let mut cursor = Cursor::new(&buf);
        load_string_object(&mut cursor).unwrap()
    }

    #[test]
    fn int8_roundtrip() {
        let obj = RedisObject::new_int_string(42);
        let loaded = roundtrip(obj);
        match loaded.kind {
            ObjectKind::String(StringEncoding::Int(n)) => assert_eq!(n, 42),
            other => panic!("expected Int, got {:?}", other),
        }
    }

    #[test]
    fn int8_negative_roundtrip() {
        let obj = RedisObject::new_int_string(-100);
        let loaded = roundtrip(obj);
        match loaded.kind {
            ObjectKind::String(StringEncoding::Int(n)) => assert_eq!(n, -100),
            other => panic!("expected Int, got {:?}", other),
        }
    }

    #[test]
    fn int16_roundtrip() {
        let obj = RedisObject::new_int_string(1000);
        let loaded = roundtrip(obj);
        match loaded.kind {
            ObjectKind::String(StringEncoding::Int(n)) => assert_eq!(n, 1000),
            other => panic!("expected Int, got {:?}", other),
        }
    }

    #[test]
    fn int32_roundtrip() {
        let obj = RedisObject::new_int_string(100_000);
        let loaded = roundtrip(obj);
        match loaded.kind {
            ObjectKind::String(StringEncoding::Int(n)) => assert_eq!(n, 100_000),
            other => panic!("expected Int, got {:?}", other),
        }
    }

    #[test]
    fn int64_too_large_for_rdb_int_encoding_is_promoted_on_load() {
        let n: i64 = i32::MAX as i64 + 1;
        let obj = RedisObject::new_int_string(n);
        let loaded = roundtrip(obj);
        match loaded.kind {
            ObjectKind::String(StringEncoding::Int(v)) => assert_eq!(v, n),
            other => panic!("expected Int after load promotion, got {:?}", other),
        }
    }

    #[test]
    fn embstr_roundtrip() {
        let obj = RedisObject::new_embstr(b"hello");
        let loaded = roundtrip(obj);
        match loaded.kind {
            ObjectKind::String(StringEncoding::Embstr(s)) => {
                assert_eq!(s.as_bytes(), b"hello")
            }
            other => panic!("expected Embstr, got {:?}", other),
        }
    }

    #[test]
    fn raw_roundtrip() {
        let long_str: Vec<u8> = b"x".repeat(100);
        let obj = RedisObject::new_raw_string(&long_str);
        let loaded = roundtrip(obj);
        match loaded.kind {
            ObjectKind::String(StringEncoding::Raw(s)) => {
                assert_eq!(s.as_bytes(), long_str.as_slice())
            }
            other => panic!("expected Raw, got {:?}", other),
        }
    }

    #[test]
    fn embstr_threshold_boundary() {
        let at_limit = b"x".repeat(44);
        let obj = RedisObject::new_embstr(&at_limit);
        let loaded = roundtrip(obj);
        match loaded.kind {
            ObjectKind::String(StringEncoding::Embstr(_)) => {}
            other => panic!("expected Embstr at threshold, got {:?}", other),
        }

        let over_limit = b"x".repeat(45);
        let obj2 = RedisObject::new_raw_string(&over_limit);
        let loaded2 = roundtrip(obj2);
        match loaded2.kind {
            ObjectKind::String(StringEncoding::Raw(_)) => {}
            other => panic!("expected Raw over threshold, got {:?}", other),
        }
    }
}
