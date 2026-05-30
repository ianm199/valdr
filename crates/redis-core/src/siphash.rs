/*!
SipHash 1-2 — locale-free, byte-exact port of `siphash.c`.

Original C copyright (c) 2012-2016 Jean-Philippe Aumasson, Daniel J. Bernstein,
and (c) 2017 Redis Ltd. Released to the public domain under CC0.

This module implements the Redis-flavoured **SipHash 1-2** variant:
- 1 compression round, 2 finalisation rounds (vs the reference 2-4).
- Returns a `u64` directly instead of writing to an output buffer.
- Provides a case-insensitive variant (`siphash_nocase`) that folds ASCII
  A-Z to a-z before mixing, without allocating a temporary buffer.

# Endianness

The C source uses a compile-time `UNALIGNED_LE_CPU` branch that casts a raw
pointer to `*uint64_t` for a single-instruction 8-byte load. That requires
`unsafe` in Rust and is blocked in pilot crates.

PORT NOTE: We use `u64::from_le_bytes` unconditionally. On x86-64 and
arm64 the compiler emits the identical single-instruction load; on
big-endian hosts it emits a byte-swap. The hash output is therefore
little-endian-defined on all platforms — identical to what the C code
produces on the LE path (which covers every Redis-supported server arch).
*/


// ── Internal helpers ──────────────────────────────────────────────────────────

/// Locale-free ASCII lowercase fold: A-Z → a-z; everything else unchanged.
#[inline]
fn sip_to_lower(c: u8) -> u8 {
    if (b'A'..=b'Z').contains(&c) {
        c + (b'a' - b'A')
    } else {
        c
    }
}

/// Read 8 bytes from `p` as a little-endian `u64`.
/// portable (non-`UNALIGNED_LE_CPU`) path.
/// # Precondition
/// `p.len >= 8`. Caller guarantees this; see call sites.
#[inline]
fn u8_to_u64_le(p: &[u8]) -> u64 {
    u64::from_le_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]])
}

/// Read 8 bytes from `p` as a little-endian `u64`, ASCII-lowercasing each byte.
/// # Precondition
/// `p.len >= 8`. Caller guarantees this; see call sites.
#[inline]
fn u8_to_u64_le_nocase(p: &[u8]) -> u64 {
    u64::from_le_bytes([
        sip_to_lower(p[0]),
        sip_to_lower(p[1]),
        sip_to_lower(p[2]),
        sip_to_lower(p[3]),
        sip_to_lower(p[4]),
        sip_to_lower(p[5]),
        sip_to_lower(p[6]),
        sip_to_lower(p[7]),
    ])
}

/// One SipRound: the 14-operation ARX permutation.
#[inline(always)]
fn sip_round(v0: &mut u64, v1: &mut u64, v2: &mut u64, v3: &mut u64) {
    *v0 = v0.wrapping_add(*v1);
    *v1 = v1.rotate_left(13);
    *v1 ^= *v0;
    *v0 = v0.rotate_left(32);
    *v2 = v2.wrapping_add(*v3);
    *v3 = v3.rotate_left(16);
    *v3 ^= *v2;
    *v0 = v0.wrapping_add(*v3);
    *v3 = v3.rotate_left(21);
    *v3 ^= *v0;
    *v2 = v2.wrapping_add(*v1);
    *v1 = v1.rotate_left(17);
    *v1 ^= *v2;
    *v2 = v2.rotate_left(32);
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Compute the SipHash 1-2 of `data` using the 128-bit secret `key`.
/// Returns a `u64` hash value. The result is little-endian-defined: it
/// matches what the C implementation produces on x86/x86-64/arm64 hosts.
pub fn siphash(data: &[u8], key: &[u8; 16]) -> u64 {
    let mut v0: u64 = 0x736f6d6570736575;
    let mut v1: u64 = 0x646f72616e646f6d;
    let mut v2: u64 = 0x6c7967656e657261;
    let mut v3: u64 = 0x7465646279746573;

    let k0 = u8_to_u64_le(&key[0..8]);
    let k1 = u8_to_u64_le(&key[8..16]);

    let inlen = data.len();
    let left = inlen & 7;

 // Length is encoded in the high byte of the last block.
    let mut b: u64 = (inlen as u64) << 56;

    v3 ^= k1;
    v2 ^= k0;
    v1 ^= k1;
    v0 ^= k0;

 // Compress 8-byte aligned blocks.
    let body = &data[..inlen - left];
    for chunk in body.chunks_exact(8) {
        let m = u8_to_u64_le(chunk);
        v3 ^= m;
        sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
        v0 ^= m;
    }

 // Pack the remaining 0-7 bytes into `b`.
    let tail = &data[inlen - left..];
    if left >= 7 {
        b |= (tail[6] as u64) << 48;
    }
    if left >= 6 {
        b |= (tail[5] as u64) << 40;
    }
    if left >= 5 {
        b |= (tail[4] as u64) << 32;
    }
    if left >= 4 {
        b |= (tail[3] as u64) << 24;
    }
    if left >= 3 {
        b |= (tail[2] as u64) << 16;
    }
    if left >= 2 {
        b |= (tail[1] as u64) << 8;
    }
    if left >= 1 {
        b |= tail[0] as u64;
    }

 // Final block.
    v3 ^= b;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    v0 ^= b;

 // Finalisation (2 rounds).
    v2 ^= 0xff;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);

    v0 ^ v1 ^ v2 ^ v3
}

/// Compute the SipHash 1-2 of `data` using the 128-bit secret `key`,
/// treating ASCII uppercase letters as lowercase before mixing.
/// This avoids a temporary buffer allocation for case-folding: each byte is
/// lowercased during the 8-byte word assembly step.
pub fn siphash_nocase(data: &[u8], key: &[u8; 16]) -> u64 {
    let mut v0: u64 = 0x736f6d6570736575;
    let mut v1: u64 = 0x646f72616e646f6d;
    let mut v2: u64 = 0x6c7967656e657261;
    let mut v3: u64 = 0x7465646279746573;

    let k0 = u8_to_u64_le(&key[0..8]);
    let k1 = u8_to_u64_le(&key[8..16]);

    let inlen = data.len();
    let left = inlen & 7;

    let mut b: u64 = (inlen as u64) << 56;

    v3 ^= k1;
    v2 ^= k0;
    v1 ^= k1;
    v0 ^= k0;

 // Compress 8-byte aligned blocks with inline case-folding.
    let body = &data[..inlen - left];
    for chunk in body.chunks_exact(8) {
        let m = u8_to_u64_le_nocase(chunk);
        v3 ^= m;
        sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
        v0 ^= m;
    }

 // Pack the remaining 0-7 bytes (case-folded) into `b`.
    let tail = &data[inlen - left..];
    if left >= 7 {
        b |= (sip_to_lower(tail[6]) as u64) << 48;
    }
    if left >= 6 {
        b |= (sip_to_lower(tail[5]) as u64) << 40;
    }
    if left >= 5 {
        b |= (sip_to_lower(tail[4]) as u64) << 32;
    }
    if left >= 4 {
        b |= (sip_to_lower(tail[3]) as u64) << 24;
    }
    if left >= 3 {
        b |= (sip_to_lower(tail[2]) as u64) << 16;
    }
    if left >= 2 {
        b |= (sip_to_lower(tail[1]) as u64) << 8;
    }
    if left >= 1 {
        b |= sip_to_lower(tail[0]) as u64;
    }

 // Final block.
    v3 ^= b;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    v0 ^= b;

 // Finalisation (2 rounds).
    v2 ^= 0xff;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);

    v0 ^ v1 ^ v2 ^ v3
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

 /// Reference test vectors for SipHash **2-4** (not 1-2).
 /// PORT NOTE: The production functions use SipHash 1-2 (Redis trade-off for
 /// speed vs. strength). These vectors are for the 2-4 reference variant.
 /// They exist here for documentation and to validate a hypothetical 2-4
 /// implementation. The `test_siphash_2_4_vectors` test is `#[ignore]`d by
 /// default because the production implementation uses 1-2 rounds.
    #[rustfmt::skip]
    const VECTORS_SIP64: [[u8; 8]; 64] = [
        [0x31, 0x0e, 0x0e, 0xdd, 0x47, 0xdb, 0x6f, 0x72],
        [0xfd, 0x67, 0xdc, 0x93, 0xc5, 0x39, 0xf8, 0x74],
        [0x5a, 0x4f, 0xa9, 0xd9, 0x09, 0x80, 0x6c, 0x0d],
        [0x2d, 0x7e, 0xfb, 0xd7, 0x96, 0x66, 0x67, 0x85],
        [0xb7, 0x87, 0x71, 0x27, 0xe0, 0x94, 0x27, 0xcf],
        [0x8d, 0xa6, 0x99, 0xcd, 0x64, 0x55, 0x76, 0x18],
        [0xce, 0xe3, 0xfe, 0x58, 0x6e, 0x46, 0xc9, 0xcb],
        [0x37, 0xd1, 0x01, 0x8b, 0xf5, 0x00, 0x02, 0xab],
        [0x62, 0x24, 0x93, 0x9a, 0x79, 0xf5, 0xf5, 0x93],
        [0xb0, 0xe4, 0xa9, 0x0b, 0xdf, 0x82, 0x00, 0x9e],
        [0xf3, 0xb9, 0xdd, 0x94, 0xc5, 0xbb, 0x5d, 0x7a],
        [0xa7, 0xad, 0x6b, 0x22, 0x46, 0x2f, 0xb3, 0xf4],
        [0xfb, 0xe5, 0x0e, 0x86, 0xbc, 0x8f, 0x1e, 0x75],
        [0x90, 0x3d, 0x84, 0xc0, 0x27, 0x56, 0xea, 0x14],
        [0xee, 0xf2, 0x7a, 0x8e, 0x90, 0xca, 0x23, 0xf7],
        [0xe5, 0x45, 0xbe, 0x49, 0x61, 0xca, 0x29, 0xa1],
        [0xdb, 0x9b, 0xc2, 0x57, 0x7f, 0xcc, 0x2a, 0x3f],
        [0x94, 0x47, 0xbe, 0x2c, 0xf5, 0xe9, 0x9a, 0x69],
        [0x9c, 0xd3, 0x8d, 0x96, 0xf0, 0xb3, 0xc1, 0x4b],
        [0xbd, 0x61, 0x79, 0xa7, 0x1d, 0xc9, 0x6d, 0xbb],
        [0x98, 0xee, 0xa2, 0x1a, 0xf2, 0x5c, 0xd6, 0xbe],
        [0xc7, 0x67, 0x3b, 0x2e, 0xb0, 0xcb, 0xf2, 0xd0],
        [0x88, 0x3e, 0xa3, 0xe3, 0x95, 0x67, 0x53, 0x93],
        [0xc8, 0xce, 0x5c, 0xcd, 0x8c, 0x03, 0x0c, 0xa8],
        [0x94, 0xaf, 0x49, 0xf6, 0xc6, 0x50, 0xad, 0xb8],
        [0xea, 0xb8, 0x85, 0x8a, 0xde, 0x92, 0xe1, 0xbc],
        [0xf3, 0x15, 0xbb, 0x5b, 0xb8, 0x35, 0xd8, 0x17],
        [0xad, 0xcf, 0x6b, 0x07, 0x63, 0x61, 0x2e, 0x2f],
        [0xa5, 0xc9, 0x1d, 0xa7, 0xac, 0xaa, 0x4d, 0xde],
        [0x71, 0x65, 0x95, 0x87, 0x66, 0x50, 0xa2, 0xa6],
        [0x28, 0xef, 0x49, 0x5c, 0x53, 0xa3, 0x87, 0xad],
        [0x42, 0xc3, 0x41, 0xd8, 0xfa, 0x92, 0xd8, 0x32],
        [0xce, 0x7c, 0xf2, 0x72, 0x2f, 0x51, 0x27, 0x71],
        [0xe3, 0x78, 0x59, 0xf9, 0x46, 0x23, 0xf3, 0xa7],
        [0x38, 0x12, 0x05, 0xbb, 0x1a, 0xb0, 0xe0, 0x12],
        [0xae, 0x97, 0xa1, 0x0f, 0xd4, 0x34, 0xe0, 0x15],
        [0xb4, 0xa3, 0x15, 0x08, 0xbe, 0xff, 0x4d, 0x31],
        [0x81, 0x39, 0x62, 0x29, 0xf0, 0x90, 0x79, 0x02],
        [0x4d, 0x0c, 0xf4, 0x9e, 0xe5, 0xd4, 0xdc, 0xca],
        [0x5c, 0x73, 0x33, 0x6a, 0x76, 0xd8, 0xbf, 0x9a],
        [0xd0, 0xa7, 0x04, 0x53, 0x6b, 0xa9, 0x3e, 0x0e],
        [0x92, 0x59, 0x58, 0xfc, 0xd6, 0x42, 0x0c, 0xad],
        [0xa9, 0x15, 0xc2, 0x9b, 0xc8, 0x06, 0x73, 0x18],
        [0x95, 0x2b, 0x79, 0xf3, 0xbc, 0x0a, 0xa6, 0xd4],
        [0xf2, 0x1d, 0xf2, 0xe4, 0x1d, 0x45, 0x35, 0xf9],
        [0x87, 0x57, 0x75, 0x19, 0x04, 0x8f, 0x53, 0xa9],
        [0x10, 0xa5, 0x6c, 0xf5, 0xdf, 0xcd, 0x9a, 0xdb],
        [0xeb, 0x75, 0x09, 0x5c, 0xcd, 0x98, 0x6c, 0xd0],
        [0x51, 0xa9, 0xcb, 0x9e, 0xcb, 0xa3, 0x12, 0xe6],
        [0x96, 0xaf, 0xad, 0xfc, 0x2c, 0xe6, 0x66, 0xc7],
        [0x72, 0xfe, 0x52, 0x97, 0x5a, 0x43, 0x64, 0xee],
        [0x5a, 0x16, 0x45, 0xb2, 0x76, 0xd5, 0x92, 0xa1],
        [0xb2, 0x74, 0xcb, 0x8e, 0xbf, 0x87, 0x87, 0x0a],
        [0x6f, 0x9b, 0xb4, 0x20, 0x3d, 0xe7, 0xb3, 0x81],
        [0xea, 0xec, 0xb2, 0xa3, 0x0b, 0x22, 0xa8, 0x7f],
        [0x99, 0x24, 0xa4, 0x3c, 0xc1, 0x31, 0x57, 0x24],
        [0xbd, 0x83, 0x8d, 0x3a, 0xaf, 0xbf, 0x8d, 0xb7],
        [0x0b, 0x1a, 0x2a, 0x32, 0x65, 0xd5, 0x1a, 0xea],
        [0x13, 0x50, 0x79, 0xa3, 0x23, 0x1c, 0xe6, 0x60],
        [0x93, 0x2b, 0x28, 0x46, 0xe4, 0xd7, 0x06, 0x66],
        [0xe1, 0x91, 0x5f, 0x5c, 0xb1, 0xec, 0xa4, 0x6c],
        [0xf3, 0x25, 0x96, 0x5c, 0xa1, 0x6d, 0x62, 0x9f],
        [0x57, 0x5f, 0xf2, 0x8e, 0x60, 0x38, 0x1b, 0xe5],
        [0x72, 0x45, 0x06, 0xeb, 0x4c, 0x32, 0x8a, 0x95],
    ];

 /// Smoke-test: `siphash` on the empty input does not panic.
    #[test]
    fn test_siphash_empty() {
        let key = [0u8; 16];
        let _ = siphash(&[], &key);
    }

 /// Verify SipHash 2-4 test vectors.
 /// the vector portion.
 /// IMPORTANT: This test is `#[ignore]`d because the production
 /// `siphash` uses 1-2 rounds, not 2-4 rounds. Temporarily swap
 /// `sip_round` call counts to 2 compression + 4 finalisation to verify
 /// these vectors during development. Seefor
 /// original note.
    #[test]
    #[ignore = "vectors are for SipHash 2-4; production uses 1-2"]
    fn test_siphash_2_4_vectors() {
        let mut k = [0u8; 16];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }

        let mut input = [0u8; 64];
        for i in 0usize..64 {
            input[i] = i as u8;
            let hash = siphash(&input[..i], &k);
            let expected = u64::from_le_bytes(VECTORS_SIP64[i]);
            assert_eq!(
                hash, expected,
                "SipHash 2-4 vector mismatch at i={i}: got {hash:#018x}, want {expected:#018x}"
            );
        }
    }

 /// Case-insensitive variant: all-lowercase input must hash the same as
 /// `siphash_nocase` on the same input.
    #[test]
    fn test_nocase_matches_lowercase() {
        let key = b"1234567812345678";
        let h1 = siphash(b"hello world", key);
        let h2 = siphash_nocase(b"hello world", key);
        assert_eq!(
            h1, h2,
            "all-lowercase: siphash and siphash_nocase must agree"
        );
    }

 /// Case-insensitive variant: mixed-case input must hash the same as
 /// its lowercase equivalent.
    #[test]
    fn test_nocase_matches_uppercase_folded() {
        let key = b"1234567812345678";
        let h1 = siphash(b"hello world", key);
        let h2 = siphash_nocase(b"HELLO world", key);
        assert_eq!(
            h1, h2,
            "nocase hash of 'HELLO world' must equal case-sensitive hash of 'hello world'"
        );
    }

 /// Case-sensitive `siphash` must treat uppercase differently from lowercase.
    #[test]
    fn test_case_sensitive_differs() {
        let key = b"1234567812345678";
        let h1 = siphash(b"HELLO world", key);
        let h2 = siphash_nocase(b"HELLO world", key);
        assert_ne!(
            h1, h2,
            "case-sensitive and case-insensitive hashes of 'HELLO world' must differ"
        );
    }

 /// `sip_to_lower` must fold A-Z only.
    #[test]
    fn test_sip_to_lower() {
        assert_eq!(sip_to_lower(b'A'), b'a');
        assert_eq!(sip_to_lower(b'Z'), b'z');
        assert_eq!(sip_to_lower(b'a'), b'a');
        assert_eq!(sip_to_lower(b'z'), b'z');
        assert_eq!(sip_to_lower(b'0'), b'0');
        assert_eq!(sip_to_lower(b' '), b' ');
        assert_eq!(sip_to_lower(b'\xff'), b'\xff');
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    high
//   todos:         0
//   port_notes:    2
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         Portable u64::from_le_bytes replaces the UNALIGNED_LE_CPU
//                  pointer-cast path (which would need unsafe). wrapping_add
//                  used for all u64 additions in SIPROUND. Test vectors are
//                  preserved but #[ignore]d because production uses 1-2 rounds,
//                  not the 2-4 rounds the vectors target.
// ──────────────────────────────────────────────────────────────────────────
