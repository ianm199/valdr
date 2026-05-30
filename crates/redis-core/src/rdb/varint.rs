//! RDB length encoding — `rdbSaveLen` / `rdbLoadLen`.
//! The top 2 bits of the first byte select the width:
//! 00xxxxxx — 6-bit length (0–63)
//! 01xxxxxx xxxxxxxx — 14-bit length (64–16383), big-endian within field
//! 10000000 + 4 bytes — 32-bit big-endian length
//! 10000001 + 8 bytes — 64-bit big-endian length
//! 11xxxxxx — special encoding marker; low 6 bits are RDB_ENC_*

use std::io::{self, Read, Write};

/// Top-2-bit encodings.
pub const RDB_6BITLEN: u8 = 0;
pub const RDB_14BITLEN: u8 = 1;
pub const RDB_32BITLEN: u8 = 0x80;
pub const RDB_64BITLEN: u8 = 0x81;
pub const RDB_ENCVAL: u8 = 3;

/// RDB_ENC_* sub-types that follow an RDB_ENCVAL prefix byte.
pub const RDB_ENC_INT8: u8 = 0;
pub const RDB_ENC_INT16: u8 = 1;
pub const RDB_ENC_INT32: u8 = 2;
pub const RDB_ENC_LZF: u8 = 3;

/// Encode `len` using the RDB variable-length format and return the bytes.
///:232).
pub fn save_len(len: u64) -> Vec<u8> {
    if len <= 63 {
        vec![(RDB_6BITLEN << 6) | (len as u8)]
    } else if len <= 16383 {
        let first = (RDB_14BITLEN << 6) | ((len >> 8) as u8 & 0x3f);
        let second = (len & 0xff) as u8;
        vec![first, second]
    } else if len <= u32::MAX as u64 {
        let mut out = vec![RDB_32BITLEN];
        out.extend_from_slice(&(len as u32).to_be_bytes());
        out
    } else {
        let mut out = vec![RDB_64BITLEN];
        out.extend_from_slice(&len.to_be_bytes());
        out
    }
}

/// Decode one length-encoded value from `reader`.
/// Returns `(length, is_encoded)`. When `is_encoded` is `true`
/// `length` field holds the low-6-bit `RDB_ENC_*` discriminant and
/// caller must handle the encoded-object branch.
///:275).
pub fn load_len(reader: &mut impl Read) -> io::Result<(u64, bool)> {
    let mut first = [0u8; 1];
    reader.read_exact(&mut first)?;
    let kind = (first[0] & 0xc0) >> 6;
    match kind {
        0 => Ok(((first[0] & 0x3f) as u64, false)),
        1 => {
            let mut second = [0u8; 1];
            reader.read_exact(&mut second)?;
            let len = (((first[0] & 0x3f) as u64) << 8) | (second[0] as u64);
            Ok((len, false))
        }
        2 => {
            let sub = first[0] & 0x3f;
            if sub == 0 {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                Ok((u32::from_be_bytes(buf) as u64, false))
            } else if sub == 1 {
                let mut buf = [0u8; 8];
                reader.read_exact(&mut buf)?;
                Ok((u64::from_be_bytes(buf), false))
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown RDB length sub-encoding",
                ))
            }
        }
        3 => {
            let enc = (first[0] & 0x3f) as u64;
            Ok((enc, true))
        }
        _ => unreachable!(),
    }
}

/// Write a length-encoded integer directly into `writer`.
pub fn write_len(writer: &mut impl Write, len: u64) -> io::Result<()> {
    writer.write_all(&save_len(len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(n: u64) {
        let encoded = save_len(n);
        let mut cursor = Cursor::new(&encoded);
        let (decoded, is_enc) = load_len(&mut cursor).unwrap();
        assert!(!is_enc, "n={} should not be an encoded value", n);
        assert_eq!(decoded, n, "roundtrip failed for n={}", n);
    }

    #[test]
    fn roundtrip_boundary_values() {
        for n in [
            0u64,
            1,
            62,
            63,
            64,
            127,
            255,
            16382,
            16383,
            16384,
            65535,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            u64::MAX / 2,
        ] {
            roundtrip(n);
        }
    }
}
