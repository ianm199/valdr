//! Minimal ziplist binary decoder — the pre-listpack compact encoding used by
//! `RDB_TYPE_{HASH,ZSET,LIST}_ZIPLIST` and the nodes of `RDB_TYPE_LIST_QUICKLIST`.
//! Wire layout:
//! - `[zlbytes: u32-le]` total blob length including header and the end byte
//! - `[zltail: u32-le]` offset of the last entry
//! - `[zllen: u16-le]` entry count (0xFFFF means "scan to find")
//! - zero or more entries, each:
//! - prevlen: 1 byte if `< 0xFE`, else `0xFE` + 4-byte little-endian length
//! - encoding byte (+ optional extra length/value bytes)
//! - string data for string entries
//! - `0xFF` end-of-ziplist marker
//! Encoding byte rules:
//! - top 2 bits != 0b11 → string: `00` 6-bit len, `01` 14-bit len, `10` (0x80)
//! 32-bit big-endian len
//! - `0xC0` int16-le, `0xD0` int32-le, `0xE0` int64-le, `0xF0` int24-le,
//! `0xFE` int8, `0xF1..=0xFD` 4-bit immediate (value = low nibble - 1)
//! Integers are returned as decimal ASCII strings, matching `decode_listpack`.

use std::io;

const ZIP_END: u8 = 0xFF;
const ZIP_BIG_PREVLEN: u8 = 0xFE;
const ZIP_STR_MASK: u8 = 0xC0;
const ZIP_STR_06B: u8 = 0x00;
const ZIP_STR_14B: u8 = 0x40;
const ZIP_STR_32B: u8 = 0x80;
const ZIP_INT_16B: u8 = 0xC0;
const ZIP_INT_32B: u8 = 0xD0;
const ZIP_INT_64B: u8 = 0xE0;
const ZIP_INT_24B: u8 = 0xF0;
const ZIP_INT_8B: u8 = 0xFE;
const ZIPLIST_HEADER_SIZE: usize = 10;

fn corrupt(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// Decode all entries from a raw ziplist blob, each as a byte vector. Integer
/// entries are returned as their decimal ASCII form.
pub fn decode_ziplist(blob: &[u8]) -> io::Result<Vec<Vec<u8>>> {
    if blob.len() < ZIPLIST_HEADER_SIZE + 1 {
        return Err(corrupt("ziplist blob too short"));
    }
    let zlbytes = u32::from_le_bytes(blob[0..4].try_into().unwrap()) as usize;
    if zlbytes != blob.len() {
        return Err(corrupt("ziplist zlbytes does not match blob length"));
    }

    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut p = ZIPLIST_HEADER_SIZE;
    loop {
        if p >= blob.len() {
            return Err(corrupt("ziplist overran without end byte"));
        }
        if blob[p] == ZIP_END {
            break;
        }

        if blob[p] < ZIP_BIG_PREVLEN {
            p += 1;
        } else {
            if p + 5 > blob.len() {
                return Err(corrupt("ziplist truncated large prevlen"));
            }
            p += 5;
        }
        if p >= blob.len() {
            return Err(corrupt("ziplist truncated before encoding byte"));
        }

        let enc = blob[p];
        if (enc & ZIP_STR_MASK) != ZIP_STR_MASK {
            let (len, hdr) = match enc & ZIP_STR_MASK {
                ZIP_STR_06B => ((enc & 0x3f) as usize, 1usize),
                ZIP_STR_14B => {
                    if p + 2 > blob.len() {
                        return Err(corrupt("ziplist truncated 14-bit string length"));
                    }
                    ((((enc & 0x3f) as usize) << 8) | blob[p + 1] as usize, 2)
                }
                ZIP_STR_32B => {
                    if p + 5 > blob.len() {
                        return Err(corrupt("ziplist truncated 32-bit string length"));
                    }
                    (
                        u32::from_be_bytes(blob[p + 1..p + 5].try_into().unwrap()) as usize,
                        5,
                    )
                }
                _ => unreachable!(),
            };
            let start = p + hdr;
            let end = start
                .checked_add(len)
                .ok_or_else(|| corrupt("ziplist string length overflow"))?;
            if end > blob.len() {
                return Err(corrupt("ziplist string data overran blob"));
            }
            out.push(blob[start..end].to_vec());
            p = end;
        } else {
            let (value, hdr): (i64, usize) = match enc {
                ZIP_INT_8B => {
                    if p + 2 > blob.len() {
                        return Err(corrupt("ziplist truncated int8"));
                    }
                    (blob[p + 1] as i8 as i64, 2)
                }
                ZIP_INT_16B => {
                    if p + 3 > blob.len() {
                        return Err(corrupt("ziplist truncated int16"));
                    }
                    (
                        i16::from_le_bytes(blob[p + 1..p + 3].try_into().unwrap()) as i64,
                        3,
                    )
                }
                ZIP_INT_24B => {
                    if p + 4 > blob.len() {
                        return Err(corrupt("ziplist truncated int24"));
                    }
                    let sign = if blob[p + 3] & 0x80 != 0 { 0xff } else { 0x00 };
                    (
                        i32::from_le_bytes([blob[p + 1], blob[p + 2], blob[p + 3], sign]) as i64,
                        4,
                    )
                }
                ZIP_INT_32B => {
                    if p + 5 > blob.len() {
                        return Err(corrupt("ziplist truncated int32"));
                    }
                    (
                        i32::from_le_bytes(blob[p + 1..p + 5].try_into().unwrap()) as i64,
                        5,
                    )
                }
                ZIP_INT_64B => {
                    if p + 9 > blob.len() {
                        return Err(corrupt("ziplist truncated int64"));
                    }
                    (
                        i64::from_le_bytes(blob[p + 1..p + 9].try_into().unwrap()),
                        9,
                    )
                }
                _ => {
                    if (0xf1..=0xfd).contains(&enc) {
                        ((enc & 0x0f) as i64 - 1, 1)
                    } else {
                        return Err(corrupt("ziplist invalid integer encoding"));
                    }
                }
            };
            out.push(value.to_string().into_bytes());
            p += hdr;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_ziplist(entries: &[&[u8]]) -> Vec<u8> {
 // Build a minimal ziplist with 6-bit string entries and 1-byte prevlens.
        let mut body: Vec<u8> = Vec::new();
        let mut prevlen = 0u8;
        for e in entries {
            body.push(prevlen);
            assert!(e.len() < 64, "test helper only does 6-bit strings");
            body.push(e.len() as u8);
            body.extend_from_slice(e);
            prevlen = (1 + 1 + e.len()) as u8;
        }
        let total = ZIPLIST_HEADER_SIZE + body.len() + 1;
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&(total as u32).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        out.extend_from_slice(&body);
        out.push(ZIP_END);
        out
    }

    #[test]
    fn decodes_string_entries() {
        let zl = build_ziplist(&[b"a", b"1", b"bb", b"22"]);
        let decoded = decode_ziplist(&zl).unwrap();
        assert_eq!(
            decoded,
            vec![b"a".to_vec(), b"1".to_vec(), b"bb".to_vec(), b"22".to_vec()]
        );
    }

    #[test]
    fn rejects_zlbytes_mismatch() {
        let mut zl = build_ziplist(&[b"a"]);
        zl[0] = zl[0].wrapping_add(1);
        assert!(decode_ziplist(&zl).is_err());
    }

    #[test]
    fn rejects_missing_end_byte() {
        let mut zl = build_ziplist(&[b"a"]);
        let last = zl.len() - 1;
        zl[last] = 0x00;
        assert!(decode_ziplist(&zl).is_err());
    }
}
