//! CRC-64 (Jones variant) — direct port.
//! The C code uses `crcspeed64native` which builds a lookup table from
//! bit-by-bit `_crc64` reference implementation. `_crc64` processes each
//! input byte LSB-first (ReflectIn=true), applies the Jones polynomial
//! `0xad93d23594c935a9`, then reflects the entire 64-bit state at the end
//! (ReflectOut=true). Initial state and XorOut are both 0.
//! The efficient single-table implementation precomputes CRC values for each
//! possible byte using the REFLECTED polynomial `0x95ac9329ac4bc9b5` (which is
//! the bit-reversal of the Jones polynomial). The running state is updated by
//! indexing the table with `(state ^ byte) & 0xff` and XOR-ing the result
//! with `state >> 8`.
//! Calling convention matches `crc64(crc, data, len)`:
//! pass `crc = 0` for the first call; the returned value can be passed back
//! in for incremental updates.

const REFLECTED_POLY: u64 = 0x95ac9329ac4bc9b5;

const fn make_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u64;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ REFLECTED_POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC64_TABLE: [u64; 256] = make_table();

/// Compute the running CRC-64 checksum.
/// `crc` is the running state (pass `0` for the first call). The returned
/// value is the new state, which can be passed back for incremental updates.
/// Calling convention matches Valkey's `crc64(crc, data, len)`:
/// `crc64(0, b"...")` produces the checksum of the given bytes.
pub fn crc64(crc: u64, data: &[u8]) -> u64 {
    let mut state = crc;
    for &byte in data {
        let index = ((state ^ (byte as u64)) & 0xff) as usize;
        state = (state >> 8) ^ CRC64_TABLE[index];
    }
    state
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_jones_known_vector() {
        let result = crc64(0, b"123456789");
        assert_eq!(
            result, 0xe9c6d914c4b8d9ca,
            "CRC64-Jones of '123456789' must be 0xe9c6d914c4b8d9ca, got 0x{:016x}",
            result
        );
    }

    #[test]
    fn crc64_empty_input() {
        assert_eq!(crc64(0, b""), 0);
    }

    #[test]
    fn crc64_incremental_matches_single_call() {
        let data = b"hello world";
        let single = crc64(0, data);
        let incremental = crc64(crc64(0, &data[..5]), &data[5..]);
        assert_eq!(single, incremental);
    }
}
