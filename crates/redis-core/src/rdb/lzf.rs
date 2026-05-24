//! LZF decompression — direct port of `lzf_d.c` from the Valkey/liblzf source.
//!
//! LZF is the stream format used by Redis/Valkey when `rdbcompression yes`
//! (the default) and a string value exceeds 20 bytes.  The decompressor is a
//! single-pass byte scanner: each control byte selects either a literal run
//! (copy N following bytes verbatim) or a back-reference (copy N+2 bytes from
//! a position up to 8192 bytes behind the current output pointer).
//!
//! Wire format consumed by `lzf_decompress`:
//!   literal run   — ctrl in 0..=31: copy (ctrl + 1) bytes verbatim.
//!   back-reference — ctrl in 32..=255:
//!       len    = ctrl >> 5                  (3 bits)
//!       offset = ((ctrl & 0x1f) << 8) + 1  (high bits, before adding low byte)
//!       if len == 7: len += next_byte       (extended length)
//!       offset += next_byte                 (low 8 bits)
//!       copy (len + 2) bytes from output[op - offset ..]
//!       (self-referential / overlapping copies are intentional RLE)

use std::io;

/// Decompress `input` into a buffer of exactly `output_len` bytes.
///
/// Returns `Err(InvalidData)` when the compressed stream is malformed or when
/// the actual decompressed length differs from `output_len`.
pub fn lzf_decompress(input: &[u8], output_len: usize) -> io::Result<Vec<u8>> {
    let mut out = vec![0u8; output_len];
    let mut ip: usize = 0;
    let mut op: usize = 0;

    while ip < input.len() {
        let ctrl = input[ip] as usize;
        ip += 1;

        if ctrl < 32 {
            let run = ctrl + 1;
            if op + run > output_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "lzf: output overflow in literal run",
                ));
            }
            if ip + run > input.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "lzf: input underrun in literal run",
                ));
            }
            out[op..op + run].copy_from_slice(&input[ip..ip + run]);
            ip += run;
            op += run;
        } else {
            let mut len = ctrl >> 5;
            let mut back = ((ctrl & 0x1f) << 8) + 1;

            if ip >= input.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "lzf: truncated back-reference",
                ));
            }

            if len == 7 {
                len += input[ip] as usize;
                ip += 1;
                if ip >= input.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "lzf: truncated extended-length byte",
                    ));
                }
            }

            back += input[ip] as usize;
            ip += 1;

            len += 2;

            if op + len > output_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "lzf: output overflow in back-reference",
                ));
            }
            if back > op {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "lzf: back-reference before output start",
                ));
            }

            let mut src = op - back;
            for _ in 0..len {
                out[op] = out[src];
                op += 1;
                src += 1;
            }
        }
    }

    if op != output_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("lzf: decompressed {} bytes but expected {}", op, output_len),
        ));
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_lzf_literal_stream(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let run = (data.len() - pos).min(32);
            out.push((run - 1) as u8);
            out.extend_from_slice(&data[pos..pos + run]);
            pos += run;
        }
        out
    }

    #[test]
    fn literal_only_roundtrip() {
        let original = b"hello world this is a test string";
        let compressed = make_lzf_literal_stream(original);
        let decompressed = lzf_decompress(&compressed, original.len()).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn back_reference_copies_prior_bytes() {
        let compressed = vec![0x00, b'A', (1u8 << 5) | 0u8, 0u8];
        let decompressed = lzf_decompress(&compressed, 4).unwrap();
        assert_eq!(decompressed, b"AAAA");
    }

    #[test]
    fn wrong_output_len_is_error() {
        let original = b"hello";
        let compressed = make_lzf_literal_stream(original);
        assert!(lzf_decompress(&compressed, 10).is_err());
    }
}
