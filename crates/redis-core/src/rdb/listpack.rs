//! Minimal listpack binary decoder — Round 21.
//! Listpack is a compact serialization of a sequence of strings or integers.
//! It is used as the payload of PACKED quicklist nodes in `RDB_TYPE_LIST_QUICKLIST_2`.
//! Wire layout:
//! - `[total_bytes: u32-le]` — total blob length including header and EOF byte
//! - `[num_elements: u16-le]` — element count (UINT16_MAX means "unknown")
//! - Zero or more entries, each:
//! - 1-byte encoding prefix (defines type + inline value or length)
//! - optional extra bytes (value data or extended length)
//! - 1-byte backlen (number of bytes in the entry not counting backlen itself)
//! - `0xFF` end-of-listpack marker
//! Entry encoding prefix rules:
//! - `0xxxxxxx` (bit 7 = 0) — 7-bit unsigned integer, value in bits [6:0]
//! - `10xxxxxx` (bits 7:6 = 0b10) — 6-bit string, length in bits [5:0], data follows
//! - `110xxxxx` (bits 7:5 = 0b110) — 13-bit signed integer across 2 bytes
//! - `1110xxxx` (bits 7:4 = 0b1110) — 12-bit string, length in low nibble + next byte, data follows
//! - `11110001` (0xF1) — 16-bit signed integer in next 2 bytes (little-endian)
//! - `11110010` (0xF2) — 24-bit signed integer in next 3 bytes (little-endian)
//! - `11110011` (0xF3) — 32-bit signed integer in next 4 bytes (little-endian)
//! - `11110100` (0xF4) — 64-bit signed integer in next 8 bytes (little-endian)
//! - `11110000` (0xF0) — 32-bit string, length in next 4 bytes (little-endian), data follows
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
/// Integer entries are returned as decimal ASCII strings, matching
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

    let (value, encoded_size): (Vec<u8>, usize) = if (enc & LP_ENCODING_7BIT_UINT_MASK) == 0 {
        let val = (enc & 0x7F) as i64;
        (val.to_string().into_bytes(), 1)
    } else if (enc & LP_ENCODING_6BIT_STR_MASK) == LP_ENCODING_6BIT_STR {
        let slen = (enc & 0x3F) as usize;
        let data_start = pos + 1;
        need_bytes(blob, pos, 1 + slen)?;
        (blob[data_start..data_start + slen].to_vec(), 1 + slen)
    } else if (enc & LP_ENCODING_13BIT_INT_MASK) == LP_ENCODING_13BIT_INT {
        need_bytes(blob, pos, 2)?;
        let high = ((enc & 0x1F) as i16) << 8;
        let low = blob[pos + 1] as i16;
        let val = sign_extend_13bit(high | low);
        (val.to_string().into_bytes(), 2)
    } else if (enc & LP_ENCODING_12BIT_STR_MASK) == LP_ENCODING_12BIT_STR {
        need_bytes(blob, pos, 2)?;
        let slen = (((enc & 0x0F) as usize) << 8) | (blob[pos + 1] as usize);
        let data_start = pos + 2;
        need_bytes(blob, pos, 2 + slen)?;
        (blob[data_start..data_start + slen].to_vec(), 2 + slen)
    } else {
        match enc {
            LP_ENCODING_16BIT_INT => {
                need_bytes(blob, pos, 3)?;
                let val = i16::from_le_bytes([blob[pos + 1], blob[pos + 2]]) as i64;
                (val.to_string().into_bytes(), 3)
            }
            LP_ENCODING_24BIT_INT => {
                need_bytes(blob, pos, 4)?;
                let raw = [blob[pos + 1], blob[pos + 2], blob[pos + 3], 0];
                let unsigned = u32::from_le_bytes(raw);
                let val = sign_extend_24bit(unsigned);
                (val.to_string().into_bytes(), 4)
            }
            LP_ENCODING_32BIT_INT => {
                need_bytes(blob, pos, 5)?;
                let val = i32::from_le_bytes([
                    blob[pos + 1],
                    blob[pos + 2],
                    blob[pos + 3],
                    blob[pos + 4],
                ]) as i64;
                (val.to_string().into_bytes(), 5)
            }
            LP_ENCODING_64BIT_INT => {
                need_bytes(blob, pos, 9)?;
                let val = i64::from_le_bytes([
                    blob[pos + 1],
                    blob[pos + 2],
                    blob[pos + 3],
                    blob[pos + 4],
                    blob[pos + 5],
                    blob[pos + 6],
                    blob[pos + 7],
                    blob[pos + 8],
                ]);
                (val.to_string().into_bytes(), 9)
            }
            LP_ENCODING_32BIT_STR => {
                need_bytes(blob, pos, 5)?;
                let slen = u32::from_le_bytes([
                    blob[pos + 1],
                    blob[pos + 2],
                    blob[pos + 3],
                    blob[pos + 4],
                ]) as usize;
                let data_start = pos + 5;
                need_bytes(blob, pos, 5 + slen)?;
                (blob[data_start..data_start + slen].to_vec(), 5 + slen)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "unknown listpack encoding byte 0x{:02x} at offset {}",
                        enc, pos
                    ),
                ));
            }
        }
    };

    let backlen_size = backlen_byte_count(encoded_size as u64);
    need_bytes(blob, pos, encoded_size + backlen_size)?;
    Ok(EntryResult {
        value,
        advance: encoded_size + backlen_size,
    })
}

/// Number of bytes the backlen field consumes for an entry whose
/// `encoding+value` size is `l`. Matches `lpEncodeBacklen`.
fn backlen_byte_count(l: u64) -> usize {
    if l <= 127 {
        1
    } else if l <= 16_383 {
        2
    } else if l <= 2_097_151 {
        3
    } else if l <= 268_435_455 {
        4
    } else {
        5
    }
}

fn need_bytes(blob: &[u8], pos: usize, needed: usize) -> io::Result<()> {
    if pos + needed > blob.len() {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "listpack truncated",
        ))
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

/// Incremental listpack encoder used by the stream RDB serializer.
/// Entries are appended one at a time as either integers (`append_int`) or
/// raw byte strings (`append_string`). `finalize` produces the complete
/// listpack blob with header, body, backlens, and `0xFF` EOF terminator
/// matching the on-disk layout produced by C Valkey's `lpAppendInteger` /
/// `lpAppendString`.
pub struct ListpackBuilder {
    body: Vec<u8>,
    num_elements: usize,
}

impl Default for ListpackBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ListpackBuilder {
    pub fn new() -> Self {
        Self {
            body: Vec::new(),
            num_elements: 0,
        }
    }

    /// Append an integer entry using the smallest encoding that fits `v`.
    pub fn append_int(&mut self, v: i64) {
        let mut enc_buf = [0u8; 9];
        let enc_len = encode_integer(v, &mut enc_buf);
        let entry_payload = &enc_buf[..enc_len];
        self.body.extend_from_slice(entry_payload);
        let backlen_bytes = encode_backlen(enc_len as u64);
        self.body.extend_from_slice(&backlen_bytes);
        self.num_elements += 1;
    }

    /// Append a byte-string entry using the smallest string encoding that fits.
    pub fn append_string(&mut self, s: &[u8]) {
        let slen = s.len();
        if slen < 64 {
            self.body.push(LP_ENCODING_6BIT_STR | (slen as u8));
            self.body.extend_from_slice(s);
            let total = 1 + slen;
            self.body.extend_from_slice(&encode_backlen(total as u64));
        } else if slen < 4096 {
            self.body
                .push(LP_ENCODING_12BIT_STR | ((slen >> 8) as u8 & 0x0F));
            self.body.push((slen & 0xFF) as u8);
            self.body.extend_from_slice(s);
            let total = 2 + slen;
            self.body.extend_from_slice(&encode_backlen(total as u64));
        } else {
            self.body.push(LP_ENCODING_32BIT_STR);
            let slen_u32 = slen as u32;
            self.body.push((slen_u32 & 0xFF) as u8);
            self.body.push(((slen_u32 >> 8) & 0xFF) as u8);
            self.body.push(((slen_u32 >> 16) & 0xFF) as u8);
            self.body.push(((slen_u32 >> 24) & 0xFF) as u8);
            self.body.extend_from_slice(s);
            let total = 5 + slen;
            self.body.extend_from_slice(&encode_backlen(total as u64));
        }
        self.num_elements += 1;
    }

    /// Number of entries appended so far.
    pub fn num_elements(&self) -> usize {
        self.num_elements
    }

    /// Build the final blob: 6-byte header + entries + 0xFF EOF byte.
    /// `num_elements` is clamped to `UINT16_MAX` to match C's
    /// `LP_HDR_NUMELE_UNKNOWN` sentinel; the loader treats values >= UINT16_MAX
    /// as "unknown" and walks the body to count entries.
    pub fn finalize(self) -> Vec<u8> {
        let total = LP_HDR_SIZE + self.body.len() + 1;
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&(total as u32).to_le_bytes());
        let num_elements_field: u16 = if self.num_elements > u16::MAX as usize {
            u16::MAX
        } else {
            self.num_elements as u16
        };
        out.extend_from_slice(&num_elements_field.to_le_bytes());
        out.extend_from_slice(&self.body);
        out.push(LP_EOF);
        out
    }
}

/// Encode a signed integer using the smallest fitting listpack encoding,
/// writing into `buf` and returning the number of bytes written (1..=9).
fn encode_integer(v: i64, buf: &mut [u8; 9]) -> usize {
    if (0..=127).contains(&v) {
        buf[0] = v as u8;
        1
    } else if (-4096..=4095).contains(&v) {
        let adj = if v < 0 {
            ((1i64 << 13) + v) as u64
        } else {
            v as u64
        };
        buf[0] = ((adj >> 8) as u8) | LP_ENCODING_13BIT_INT;
        buf[1] = (adj & 0xFF) as u8;
        2
    } else if (-32_768..=32_767).contains(&v) {
        let adj = if v < 0 {
            ((1i64 << 16) + v) as u64
        } else {
            v as u64
        };
        buf[0] = LP_ENCODING_16BIT_INT;
        buf[1] = (adj & 0xFF) as u8;
        buf[2] = ((adj >> 8) & 0xFF) as u8;
        3
    } else if (-8_388_608..=8_388_607).contains(&v) {
        let adj = if v < 0 {
            ((1i64 << 24) + v) as u64
        } else {
            v as u64
        };
        buf[0] = LP_ENCODING_24BIT_INT;
        buf[1] = (adj & 0xFF) as u8;
        buf[2] = ((adj >> 8) & 0xFF) as u8;
        buf[3] = ((adj >> 16) & 0xFF) as u8;
        4
    } else if (-2_147_483_648..=2_147_483_647).contains(&v) {
        let adj = if v < 0 {
            ((1i64 << 32) + v) as u64
        } else {
            v as u64
        };
        buf[0] = LP_ENCODING_32BIT_INT;
        buf[1] = (adj & 0xFF) as u8;
        buf[2] = ((adj >> 8) & 0xFF) as u8;
        buf[3] = ((adj >> 16) & 0xFF) as u8;
        buf[4] = ((adj >> 24) & 0xFF) as u8;
        5
    } else {
        let uv = v as u64;
        buf[0] = LP_ENCODING_64BIT_INT;
        buf[1] = (uv & 0xFF) as u8;
        buf[2] = ((uv >> 8) & 0xFF) as u8;
        buf[3] = ((uv >> 16) & 0xFF) as u8;
        buf[4] = ((uv >> 24) & 0xFF) as u8;
        buf[5] = ((uv >> 32) & 0xFF) as u8;
        buf[6] = ((uv >> 40) & 0xFF) as u8;
        buf[7] = ((uv >> 48) & 0xFF) as u8;
        buf[8] = ((uv >> 56) & 0xFF) as u8;
        9
    }
}

/// Encode the backlen of an entry whose `encoding+value` size is `l`.
/// The encoded form is 1..=5 bytes. Every byte except the last has its
/// top bit set; the last byte's top bit is clear. The most significant
/// 7-bit chunk is written first.
fn encode_backlen(l: u64) -> Vec<u8> {
    if l <= 127 {
        vec![l as u8]
    } else if l <= 16_383 {
        vec![(l >> 7) as u8, ((l & 127) | 128) as u8]
    } else if l <= 2_097_151 {
        vec![
            (l >> 14) as u8,
            (((l >> 7) & 127) | 128) as u8,
            ((l & 127) | 128) as u8,
        ]
    } else if l <= 268_435_455 {
        vec![
            (l >> 21) as u8,
            (((l >> 14) & 127) | 128) as u8,
            (((l >> 7) & 127) | 128) as u8,
            ((l & 127) | 128) as u8,
        ]
    } else {
        vec![
            (l >> 28) as u8,
            (((l >> 21) & 127) | 128) as u8,
            (((l >> 14) & 127) | 128) as u8,
            (((l >> 7) & 127) | 128) as u8,
            ((l & 127) | 128) as u8,
        ]
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

    #[test]
    fn builder_empty_roundtrip() {
        let blob = ListpackBuilder::new().finalize();
        let entries = decode_listpack(&blob).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn builder_int_roundtrip_all_widths() {
        let values: [i64; 11] = [
            0,
            1,
            127,
            -1,
            -4096,
            4095,
            -32_768,
            32_767,
            i32::MIN as i64,
            i32::MAX as i64,
            i64::MIN,
        ];
        let mut builder = ListpackBuilder::new();
        for &v in &values {
            builder.append_int(v);
        }
        let blob = builder.finalize();
        let entries = decode_listpack(&blob).unwrap();
        assert_eq!(entries.len(), values.len());
        for (i, &v) in values.iter().enumerate() {
            assert_eq!(
                entries[i],
                v.to_string().into_bytes(),
                "value {} at index {}",
                v,
                i
            );
        }
    }

    #[test]
    fn builder_string_roundtrip_all_widths() {
        let short = b"hello".to_vec();
        let medium: Vec<u8> = (0..200).map(|i| (i % 256) as u8).collect();
        let large: Vec<u8> = (0..5000).map(|i| ((i * 7) % 256) as u8).collect();
        let mut builder = ListpackBuilder::new();
        builder.append_string(&short);
        builder.append_string(&medium);
        builder.append_string(&large);
        let blob = builder.finalize();
        let entries = decode_listpack(&blob).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], short);
        assert_eq!(entries[1], medium);
        assert_eq!(entries[2], large);
    }

    #[test]
    fn builder_mixed_roundtrip() {
        let mut builder = ListpackBuilder::new();
        builder.append_int(1);
        builder.append_string(b"hello");
        builder.append_int(256);
        builder.append_int(-1);
        builder.append_string(b"");
        let blob = builder.finalize();
        let entries = decode_listpack(&blob).unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0], b"1");
        assert_eq!(entries[1], b"hello");
        assert_eq!(entries[2], b"256");
        assert_eq!(entries[3], b"-1");
        assert_eq!(entries[4], b"");
    }
}
