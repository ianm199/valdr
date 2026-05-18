//! Minimal listpack binary decoder — Round 21.
//!
//! Listpack is a compact serialization of a sequence of strings or integers.
//! It is used as the payload of PACKED quicklist nodes in `RDB_TYPE_LIST_QUICKLIST_2`.
//!
//! Wire layout (from listpack.c LP_HDR_SIZE and entry encoding macros):
//!   - `[total_bytes: u32-le]` — total blob length including header and EOF byte
//!   - `[num_elements: u16-le]` — element count (UINT16_MAX means "unknown")
//!   - Zero or more entries, each:
//!       - 1-byte encoding prefix (defines type + inline value or length)
//!       - optional extra bytes (value data or extended length)
//!       - 1-byte backlen (number of bytes in the entry not counting backlen itself)
//!   - `0xFF` end-of-listpack marker
//!
//! Entry encoding prefix rules (listpack.c lines 52–101):
//!   - `0xxxxxxx` (bit 7 = 0) — 7-bit unsigned integer, value in bits [6:0]
//!   - `10xxxxxx` (bits 7:6 = 0b10) — 6-bit string, length in bits [5:0], data follows
//!   - `110xxxxx` (bits 7:5 = 0b110) — 13-bit signed integer across 2 bytes
//!   - `1110xxxx` (bits 7:4 = 0b1110) — 12-bit string, length in low nibble + next byte, data follows
//!   - `11110001` (0xF1) — 16-bit signed integer in next 2 bytes (little-endian)
//!   - `11110010` (0xF2) — 24-bit signed integer in next 3 bytes (little-endian)
//!   - `11110011` (0xF3) — 32-bit signed integer in next 4 bytes (little-endian)
//!   - `11110100` (0xF4) — 64-bit signed integer in next 8 bytes (little-endian)
//!   - `11110000` (0xF0) — 32-bit string, length in next 4 bytes (little-endian), data follows
//!
//! All integers are returned as their decimal string representation to match
//! how the C Redis server presents them to clients after loading.

use std::io;

const LP_HDR_SIZE: usize = 6;
const LP_EOF: u8 = 0xFF;

const LP_ENCODING_7BIT_UINT_MASK: u8 = 0x80;
const LP_ENCODING_6BIT_STR_MASK: u8 = 0xC0;
const LP_ENCODING_6BIT_STR: u8 = 0x80;
const LP_ENCODING_13BIT_INT_MASK: u8 = 0xE0;
const LP_ENCODING_13BIT_INT: u8 = 0xC0;
const LP_ENCODING_16BIT_INT: u8 = 0xF1;
const LP_ENCODING_24BIT_INT: u8 = 0xF2;
const LP_ENCODING_32BIT_INT: u8 = 0xF3;
const LP_ENCODING_64BIT_INT: u8 = 0xF4;
const LP_ENCODING_12BIT_STR: u8 = 0xE0;
const LP_ENCODING_12BIT_STR_MASK: u8 = 0xF0;
const LP_ENCODING_32BIT_STR: u8 = 0xF0;

/// Decode all entries from a raw listpack blob, returning each as a byte vector.
///
/// Integer entries are returned as decimal ASCII strings, matching the
/// representation Redis presents to clients. String entries are returned as
/// their raw bytes.
pub fn decode_listpack(blob: &[u8]) -> io::Result<Vec<Vec<u8>>> {
    if blob.len() < LP_HDR_SIZE + 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "listpack blob too short",
        ));
    }

    let total_bytes = u32::from_le_bytes(blob[0..4].try_into().unwrap()) as usize;
    if total_bytes != blob.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "listpack total_bytes {} != blob.len() {}",
                total_bytes,
                blob.len()
            ),
        ));
    }

    let mut pos = LP_HDR_SIZE;
    let mut entries: Vec<Vec<u8>> = Vec::new();

    loop {
        if pos >= blob.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "listpack missing EOF marker",
            ));
        }

        let enc = blob[pos];
        if enc == LP_EOF {
            break;
        }

        let entry_bytes = decode_entry(blob, pos)?;
        pos += entry_bytes.advance;
        entries.push(entry_bytes.value);
    }

    Ok(entries)
}

struct EntryResult {
    value: Vec<u8>,
    advance: usize,
}

fn decode_entry(blob: &[u8], pos: usize) -> io::Result<EntryResult> {
    let enc = blob[pos];

    if (enc & LP_ENCODING_7BIT_UINT_MASK) == 0 {
        let val = (enc & 0x7F) as i64;
        let backlen_pos = pos + 1;
        let advance = 1 + 1;
        check_bounds(blob, backlen_pos, advance)?;
        return Ok(EntryResult {
            value: val.to_string().into_bytes(),
            advance,
        });
    }

    if (enc & LP_ENCODING_6BIT_STR_MASK) == LP_ENCODING_6BIT_STR {
        let slen = (enc & 0x3F) as usize;
        let data_start = pos + 1;
        let backlen_pos = data_start + slen;
        let advance = 1 + slen + 1;
        check_bounds(blob, backlen_pos, advance)?;
        return Ok(EntryResult {
            value: blob[data_start..data_start + slen].to_vec(),
            advance,
        });
    }

    if (enc & LP_ENCODING_13BIT_INT_MASK) == LP_ENCODING_13BIT_INT {
        if pos + 1 >= blob.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack 13-bit int truncated"));
        }
        let high = ((enc & 0x1F) as i16) << 8;
        let low = blob[pos + 1] as i16;
        let val = sign_extend_13bit(high | low);
        let advance = 2 + 1;
        check_bounds(blob, pos + 2, advance)?;
        return Ok(EntryResult {
            value: val.to_string().into_bytes(),
            advance,
        });
    }

    if (enc & LP_ENCODING_12BIT_STR_MASK) == LP_ENCODING_12BIT_STR {
        need_bytes(blob, pos, 2)?;
        let slen = (((enc & 0x0F) as usize) << 8) | (blob[pos + 1] as usize);
        let data_start = pos + 2;
        need_bytes(blob, pos, 2 + slen + 1)?;
        return Ok(EntryResult {
            value: blob[data_start..data_start + slen].to_vec(),
            advance: 2 + slen + 1,
        });
    }

    match enc {
        LP_ENCODING_16BIT_INT => {
            need_bytes(blob, pos, 2 + 1 + 1)?;
            let val = i16::from_le_bytes([blob[pos + 1], blob[pos + 2]]) as i64;
            Ok(EntryResult {
                value: val.to_string().into_bytes(),
                advance: 3 + 1,
            })
        }
        LP_ENCODING_24BIT_INT => {
            need_bytes(blob, pos, 3 + 1 + 1)?;
            let raw = [blob[pos + 1], blob[pos + 2], blob[pos + 3], 0];
            let unsigned = u32::from_le_bytes(raw);
            let val = sign_extend_24bit(unsigned);
            Ok(EntryResult {
                value: val.to_string().into_bytes(),
                advance: 4 + 1,
            })
        }
        LP_ENCODING_32BIT_INT => {
            need_bytes(blob, pos, 4 + 1 + 1)?;
            let val = i32::from_le_bytes([blob[pos + 1], blob[pos + 2], blob[pos + 3], blob[pos + 4]]) as i64;
            Ok(EntryResult {
                value: val.to_string().into_bytes(),
                advance: 5 + 1,
            })
        }
        LP_ENCODING_64BIT_INT => {
            need_bytes(blob, pos, 8 + 1 + 1)?;
            let val = i64::from_le_bytes([
                blob[pos + 1], blob[pos + 2], blob[pos + 3], blob[pos + 4],
                blob[pos + 5], blob[pos + 6], blob[pos + 7], blob[pos + 8],
            ]);
            Ok(EntryResult {
                value: val.to_string().into_bytes(),
                advance: 9 + 1,
            })
        }
        LP_ENCODING_32BIT_STR => {
            need_bytes(blob, pos, 4 + 1)?;
            let slen = u32::from_le_bytes([blob[pos + 1], blob[pos + 2], blob[pos + 3], blob[pos + 4]]) as usize;
            let data_start = pos + 5;
            need_bytes(blob, pos, 5 + slen + 1)?;
            Ok(EntryResult {
                value: blob[data_start..data_start + slen].to_vec(),
                advance: 5 + slen + 1,
            })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown listpack encoding byte 0x{:02x} at offset {}", enc, pos),
        )),
    }
}

fn check_bounds(blob: &[u8], backlen_pos: usize, advance: usize) -> io::Result<()> {
    let _ = advance;
    if backlen_pos >= blob.len() {
        Err(io::Error::new(io::ErrorKind::InvalidData, "listpack entry out of bounds"))
    } else {
        Ok(())
    }
}

fn need_bytes(blob: &[u8], pos: usize, needed: usize) -> io::Result<()> {
    if pos + needed > blob.len() {
        Err(io::Error::new(io::ErrorKind::InvalidData, "listpack truncated"))
    } else {
        Ok(())
    }
}

fn sign_extend_13bit(v: i16) -> i64 {
    if v & 0x1000 != 0 {
        (v | -0x2000i16) as i64
    } else {
        v as i64
    }
}

fn sign_extend_24bit(v: u32) -> i64 {
    if v & 0x800000 != 0 {
        (v | 0xFF000000) as i32 as i64
    } else {
        v as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_listpack(entries: &[&[u8]]) -> Vec<u8> {
        let mut body: Vec<u8> = Vec::new();
        for entry in entries {
            let slen = entry.len();
            if slen <= 63 {
                let enc = LP_ENCODING_6BIT_STR | (slen as u8);
                body.push(enc);
                body.extend_from_slice(entry);
                body.push((1 + slen) as u8);
            } else {
                body.push(LP_ENCODING_32BIT_STR);
                body.extend_from_slice(&(slen as u32).to_be_bytes());
                body.extend_from_slice(entry);
                body.push(0);
            }
        }
        body.push(LP_EOF);
        let total = LP_HDR_SIZE + body.len();
        let mut lp = Vec::with_capacity(total);
        lp.extend_from_slice(&(total as u32).to_le_bytes());
        lp.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        lp.extend_from_slice(&body);
        lp
    }

    #[test]
    fn empty_listpack() {
        let lp = make_listpack(&[]);
        let result = decode_listpack(&lp).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn single_string_entry() {
        let lp = make_listpack(&[b"hello"]);
        let result = decode_listpack(&lp).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], b"hello");
    }

    #[test]
    fn multiple_string_entries() {
        let lp = make_listpack(&[b"a", b"bb", b"ccc"]);
        let result = decode_listpack(&lp).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"a");
        assert_eq!(result[1], b"bb");
        assert_eq!(result[2], b"ccc");
    }

    #[test]
    fn seven_bit_integer_entry() {
        let mut lp = Vec::new();
        let entry_byte: u8 = 42;
        let body = vec![entry_byte, 1u8, LP_EOF];
        let total = LP_HDR_SIZE + body.len();
        lp.extend_from_slice(&(total as u32).to_le_bytes());
        lp.extend_from_slice(&1u16.to_le_bytes());
        lp.extend_from_slice(&body);
        let result = decode_listpack(&lp).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], b"42");
    }
}
