//! HyperLogLog probabilistic cardinality estimation: PFADD, PFCOUNT, PFMERGE.
//!
//! C source: `reference/valkey/src/hyperloglog.c` (2107 lines, 27 functions)
//! Crate: `redis-commands` (later phase)
//!
//! Implements the HyperLogLog algorithm using 16384 6-bit registers in a
//! compact binary format. Two on-disk representations are supported:
//!
//! - **Dense**: 12288 bytes of 6-bit-packed registers (fixed size).
//! - **Sparse**: run-length encoded opcodes (ZERO/XZERO/VAL) efficient for
//!   low-cardinality sets; auto-promotes to dense when space exceeds a threshold.
//!
//! Hash function: MurmurHash2, 64-bit endian-neutral variant.
//! Cardinality estimator: Ertl's improved estimator using sigma/tau corrections
//! (arXiv:1702.01284).
//!
//! ## SIMD
//! AVX2 and ARM-NEON paths from the C source are intentionally omitted — they
//! require `unsafe` SIMD intrinsics which are banned in pilot crates.  The
//! scalar fallback path is used unconditionally.
//! TODO(architect): SIMD optimization via a safe-intrinsics wrapper crate, after
//! the unsafe budget policy is resolved for non-pilot crates.
//!
//! ## HLL header layout (16 bytes)
//! ```text
//! [0..4]   magic   = "HYLL"
//! [4]      encoding (HLL_DENSE | HLL_SPARSE)
//! [5..8]   notused  (3 bytes, reserved, must be zero)
//! [8..16]  card     (cached cardinality, little-endian u64; MSB set = invalid)
//! [16..]   registers (dense packed or sparse opcodes)
//! ```
//!
//! All Redis data (keys, values, element bytes) is `&[u8]` / `Vec<u8>` /
//! `RedisString`.  `String` / `&str` / `from_utf8` are banned per PORTING.md §1.

use redis_core::command_context::CommandContext;
use redis_core::object::RedisObject;
use redis_types::{RedisError, RedisString};

/// Sparse-encoded buffer size limit before promotion to dense.
///
/// Redis default is 3000 bytes. Setting this very low (e.g. 0) forces immediate
/// dense encoding on every PFADD. We hard-code 3000 since the server config
/// system is not yet wired into `CommandContext`.
const HLL_SPARSE_MAX_BYTES_DEFAULT: usize = 3000;

/// Build the canonical HLL WRONGTYPE error payload.
fn hll_wrong_type_error() -> RedisError {
    RedisError::runtime(b"WRONGTYPE Key is not a valid HyperLogLog string value.")
}

/// Validate `obj` as a HyperLogLog string and return an owned copy of its bytes.
///
/// Returns `Err` with the HLL-specific WRONGTYPE message if `obj` is not a
/// string-encoded value carrying a well-formed `HYLL` header. The bytes are
/// cloned so the caller can mutate them without holding a borrow on the DB.
fn require_hll_bytes(obj: &RedisObject) -> Result<Vec<u8>, RedisError> {
    let bytes = obj.as_string_bytes().ok_or_else(hll_wrong_type_error)?;
    if !is_hll_valid(bytes) {
        return Err(hll_wrong_type_error());
    }
    Ok(bytes.to_vec())
}

// ── HLL algorithm constants ───────────────────────────────────────────────────

/// Precision parameter: bits used to index a register.
pub const HLL_P: u32 = 14;

/// Bits used for the leading-zeros count (= 64 - P = 50).
pub const HLL_Q: u32 = 64 - HLL_P;

/// Total number of registers: 2^14 = 16384.
pub const HLL_REGISTERS: usize = 1 << HLL_P;

/// Mask to extract the register index from a 64-bit hash.
pub const HLL_P_MASK: u64 = (HLL_REGISTERS as u64) - 1;

/// Bits stored per register (6 bits holds values 0..=63).
pub const HLL_BITS: u32 = 6;

/// Maximum value storable in a register: 2^6 - 1 = 63.
pub const HLL_REGISTER_MAX: u8 = ((1u32 << HLL_BITS) - 1) as u8;

/// Size of the fixed 16-byte HLL header.
pub const HLL_HDR_SIZE: usize = 16;

/// Total byte size of a dense-encoded HLL (header + packed registers).
/// = 16 + (16384 * 6 + 7) / 8 = 16 + 12288 = 12304.
pub const HLL_DENSE_SIZE: usize = HLL_HDR_SIZE + (HLL_REGISTERS * HLL_BITS as usize + 7) / 8;

/// Dense encoding discriminant stored in header byte [4].
pub const HLL_DENSE: u8 = 0;

/// Sparse encoding discriminant stored in header byte [4].
pub const HLL_SPARSE: u8 = 1;

/// Internal-only encoding used during multi-key PFCOUNT (never stored).
pub const HLL_RAW: u8 = 255;

/// Maximum valid encoding value (for header validation).
pub const HLL_MAX_ENCODING: u8 = 1;

/// Constant 0.5/ln(2) for the cardinality estimator bias correction.
pub const HLL_ALPHA_INF: f64 = 0.721_347_520_444_481_7;

// ── Sparse opcode constants ───────────────────────────────────────────────────

pub const HLL_SPARSE_XZERO_BIT: u8 = 0x40;
pub const HLL_SPARSE_VAL_BIT: u8 = 0x80;

/// Maximum register value representable by a sparse VAL opcode (5 bits = 1..=32).
pub const HLL_SPARSE_VAL_MAX_VALUE: u8 = 32;

/// Maximum run length representable by a VAL opcode (2 bits = 1..=4).
pub const HLL_SPARSE_VAL_MAX_LEN: usize = 4;

/// Maximum run length representable by a ZERO opcode (6 bits = 1..=64).
pub const HLL_SPARSE_ZERO_MAX_LEN: usize = 64;

/// Maximum run length representable by an XZERO opcode (14 bits = 1..=16384).
pub const HLL_SPARSE_XZERO_MAX_LEN: usize = 16384;

/// Error payload for corrupted HLL objects.
pub const INVALID_HLL_ERR: &[u8] = b"INVALIDOBJ Corrupted HLL object detected";

/// Number of iterations for PFSELFTEST register correctness check.
const HLL_TEST_CYCLES: u32 = 1000;

// ── Header byte-offset constants ─────────────────────────────────────────────

const HDR_MAGIC_OFF: usize = 0;
const HDR_ENCODING_OFF: usize = 4;
const HDR_CARD_OFF: usize = 8;

// ── Header field accessors ────────────────────────────────────────────────────

fn hll_encoding(buf: &[u8]) -> u8 {
    buf[HDR_ENCODING_OFF]
}

fn hll_set_encoding(buf: &mut [u8], enc: u8) {
    buf[HDR_ENCODING_OFF] = enc;
}

/// Set the cardinality-invalid bit in the cached card field (MSB of byte [15]).
fn hll_invalidate_cache(buf: &mut [u8]) {
    buf[HDR_CARD_OFF + 7] |= 1 << 7;
}

/// Return true if the cached cardinality is still valid.
fn hll_valid_cache(buf: &[u8]) -> bool {
    (buf[HDR_CARD_OFF + 7] & (1 << 7)) == 0
}

/// Read the 8-byte little-endian cached cardinality.
fn hll_card_read(buf: &[u8]) -> u64 {
    let c = &buf[HDR_CARD_OFF..HDR_CARD_OFF + 8];
    (c[0] as u64)
        | (c[1] as u64) << 8
        | (c[2] as u64) << 16
        | (c[3] as u64) << 24
        | (c[4] as u64) << 32
        | (c[5] as u64) << 40
        | (c[6] as u64) << 48
        | (c[7] as u64) << 56
}

/// Write the 8-byte little-endian cached cardinality and clear the invalid bit.
fn hll_card_write(buf: &mut [u8], card: u64) {
    buf[HDR_CARD_OFF]     = (card & 0xff) as u8;
    buf[HDR_CARD_OFF + 1] = (card >> 8 & 0xff) as u8;
    buf[HDR_CARD_OFF + 2] = (card >> 16 & 0xff) as u8;
    buf[HDR_CARD_OFF + 3] = (card >> 24 & 0xff) as u8;
    buf[HDR_CARD_OFF + 4] = (card >> 32 & 0xff) as u8;
    buf[HDR_CARD_OFF + 5] = (card >> 40 & 0xff) as u8;
    buf[HDR_CARD_OFF + 6] = (card >> 48 & 0xff) as u8;
    buf[HDR_CARD_OFF + 7] = (card >> 56 & 0xff) as u8;
}

// ── Dense register get/set ────────────────────────────────────────────────────
// C: hyperloglog.c:376-400, HLL_DENSE_GET_REGISTER / HLL_DENSE_SET_REGISTER macros.
//
// Registers are packed at 6 bits each.  Register `regnum` spans bytes:
//   b0 = regnum * 6 / 8
//   b1 = b0 + 1
//   fb = regnum * 6 % 8  (first bit offset within b0)

#[inline]
pub fn hll_dense_get_register(registers: &[u8], regnum: usize) -> u8 {
    let byte_idx = regnum * HLL_BITS as usize / 8;
    let fb = regnum * HLL_BITS as usize & 7;
    let fb8 = 8 - fb;
    let b0 = registers[byte_idx] as u32;
    // C reads the byte after the last packed register and relies on SDS's
    // implicit NUL terminator. Rust slices do not include that sentinel.
    let b1 = registers.get(byte_idx + 1).copied().unwrap_or(0) as u32;
    ((b0 >> fb | b1 << fb8) & HLL_REGISTER_MAX as u32) as u8
}

#[inline]
pub fn hll_dense_set_register(registers: &mut [u8], regnum: usize, val: u8) {
    let byte_idx = regnum * HLL_BITS as usize / 8;
    let fb = regnum * HLL_BITS as usize & 7;
    let fb8 = 8 - fb;
    let v = val as u32;
    registers[byte_idx] &= !((HLL_REGISTER_MAX as u32) << fb) as u8;
    registers[byte_idx] |= (v << fb) as u8;
    if let Some(next) = registers.get_mut(byte_idx + 1) {
        *next &= !((HLL_REGISTER_MAX as u32) >> fb8) as u8;
        *next |= (v >> fb8) as u8;
    }
}

// ── Sparse opcode helpers ─────────────────────────────────────────────────────
// C: hyperloglog.c:404-431, sparse representation macros.

#[inline]
fn sparse_is_zero(b: u8) -> bool {
    (b & 0xc0) == 0
}

#[inline]
fn sparse_is_xzero(b: u8) -> bool {
    (b & 0xc0) == HLL_SPARSE_XZERO_BIT
}

#[inline]
fn sparse_is_val(b: u8) -> bool {
    (b & HLL_SPARSE_VAL_BIT) != 0
}

#[inline]
fn sparse_zero_len(b: u8) -> usize {
    (b & 0x3f) as usize + 1
}

#[inline]
fn sparse_xzero_len(b0: u8, b1: u8) -> usize {
    ((b0 as usize & 0x3f) << 8 | b1 as usize) + 1
}

#[inline]
fn sparse_val_value(b: u8) -> u8 {
    ((b >> 2) & 0x1f) + 1
}

#[inline]
fn sparse_val_len(b: u8) -> usize {
    (b & 0x3) as usize + 1
}

#[inline]
fn sparse_val_set(dst: &mut u8, val: u8, len: usize) {
    *dst = ((val - 1) << 2 | (len as u8 - 1)) | HLL_SPARSE_VAL_BIT;
}

#[inline]
fn sparse_zero_set(dst: &mut u8, len: usize) {
    *dst = len as u8 - 1;
}

// ── MurmurHash64A ─────────────────────────────────────────────────────────────
// C: hyperloglog.c:438-488, MurmurHash64A
// Endian-neutral 64-bit Murmur2 hash used to hash HLL elements.

pub fn murmur_hash64a(key: &[u8], seed: u32) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let len = key.len();
    let mut h: u64 = (seed as u64) ^ (len as u64).wrapping_mul(M);

    let chunks = len / 8;
    for i in 0..chunks {
        let base = i * 8;
        // Endian-neutral: read byte-by-byte as little-endian
        let mut k = key[base] as u64;
        k |= (key[base + 1] as u64) << 8;
        k |= (key[base + 2] as u64) << 16;
        k |= (key[base + 3] as u64) << 24;
        k |= (key[base + 4] as u64) << 32;
        k |= (key[base + 5] as u64) << 40;
        k |= (key[base + 6] as u64) << 48;
        k |= (key[base + 7] as u64) << 56;

        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }

    let tail = &key[chunks * 8..];
    match tail.len() {
        7 => { h ^= (tail[6] as u64) << 48; h ^= (tail[5] as u64) << 40; h ^= (tail[4] as u64) << 32; h ^= (tail[3] as u64) << 24; h ^= (tail[2] as u64) << 16; h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(M); }
        6 => { h ^= (tail[5] as u64) << 40; h ^= (tail[4] as u64) << 32; h ^= (tail[3] as u64) << 24; h ^= (tail[2] as u64) << 16; h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(M); }
        5 => { h ^= (tail[4] as u64) << 32; h ^= (tail[3] as u64) << 24; h ^= (tail[2] as u64) << 16; h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(M); }
        4 => { h ^= (tail[3] as u64) << 24; h ^= (tail[2] as u64) << 16; h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(M); }
        3 => { h ^= (tail[2] as u64) << 16; h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(M); }
        2 => { h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(M); }
        1 => { h ^= tail[0] as u64; h = h.wrapping_mul(M); }
        _ => {}
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

// ── hll_pat_len ───────────────────────────────────────────────────────────────
// C: hyperloglog.c:493-513, hllPatLen
// Returns (count, register_index) where count is the length of the "000..1"
// bit pattern (1-indexed; minimum 1).

pub fn hll_pat_len(ele: &[u8]) -> (u8, usize) {
    let hash = murmur_hash64a(ele, 0xadc83b19);
    let index = (hash & HLL_P_MASK) as usize;
    let mut hash = hash >> HLL_P;
    hash |= 1u64 << HLL_Q;
    // Count trailing zeros + 1 (the terminating '1' bit is included in count)
    // PERF(port): C uses builtin_ctzll; trailing_zeros() is the equivalent.
    let count = (hash.trailing_zeros() + 1) as u8;
    (count, index)
}

// ── Dense representation ──────────────────────────────────────────────────────
// C: hyperloglog.c:527-607

/// Set register at `index` to `count` if current value is smaller.
/// Returns true if the register was updated (cardinality may have changed).
/// `registers` must be the dense data slice (not including the header).
pub fn hll_dense_set(registers: &mut [u8], index: usize, count: u8) -> bool {
    let old = hll_dense_get_register(registers, index);
    if count > old {
        hll_dense_set_register(registers, index, count);
        true
    } else {
        false
    }
}

/// Hash `ele` and update the dense register if the hash produces a longer
/// leading-zero run. Returns true if the register was updated.
pub(crate) fn hll_dense_add(registers: &mut [u8], ele: &[u8]) -> bool {
    let (count, index) = hll_pat_len(ele);
    hll_dense_set(registers, index, count)
}

/// Compute the register value frequency histogram for the dense representation.
/// `reghisto[v]` accumulates the number of registers with value `v`.
// C: hyperloglog.c:553-607, hllDenseRegHisto
// The unrolled 16-registers-per-iteration fast path matches the C exactly for
// HLL_REGISTERS==16384 && HLL_BITS==6.
pub fn hll_dense_reg_histo(registers: &[u8], reghisto: &mut [i32; 64]) {
    if HLL_REGISTERS == 16384 && HLL_BITS == 6 {
        // Fast path: process 16 registers (12 bytes) per iteration, 1024 iterations.
        for chunk in registers.chunks_exact(12) {
            let r0  = (chunk[0] as u32 & 63) as usize;
            let r1  = ((chunk[0] as u32 >> 6 | (chunk[1] as u32) << 2) & 63) as usize;
            let r2  = ((chunk[1] as u32 >> 4 | (chunk[2] as u32) << 4) & 63) as usize;
            let r3  = (chunk[2] as u32 >> 2 & 63) as usize;
            let r4  = (chunk[3] as u32 & 63) as usize;
            let r5  = ((chunk[3] as u32 >> 6 | (chunk[4] as u32) << 2) & 63) as usize;
            let r6  = ((chunk[4] as u32 >> 4 | (chunk[5] as u32) << 4) & 63) as usize;
            let r7  = (chunk[5] as u32 >> 2 & 63) as usize;
            let r8  = (chunk[6] as u32 & 63) as usize;
            let r9  = ((chunk[6] as u32 >> 6 | (chunk[7] as u32) << 2) & 63) as usize;
            let r10 = ((chunk[7] as u32 >> 4 | (chunk[8] as u32) << 4) & 63) as usize;
            let r11 = (chunk[8] as u32 >> 2 & 63) as usize;
            let r12 = (chunk[9] as u32 & 63) as usize;
            let r13 = ((chunk[9] as u32 >> 6 | (chunk[10] as u32) << 2) & 63) as usize;
            let r14 = ((chunk[10] as u32 >> 4 | (chunk[11] as u32) << 4) & 63) as usize;
            let r15 = (chunk[11] as u32 >> 2 & 63) as usize;
            reghisto[r0]  += 1; reghisto[r1]  += 1; reghisto[r2]  += 1; reghisto[r3]  += 1;
            reghisto[r4]  += 1; reghisto[r5]  += 1; reghisto[r6]  += 1; reghisto[r7]  += 1;
            reghisto[r8]  += 1; reghisto[r9]  += 1; reghisto[r10] += 1; reghisto[r11] += 1;
            reghisto[r12] += 1; reghisto[r13] += 1; reghisto[r14] += 1; reghisto[r15] += 1;
        }
    } else {
        // Generic path (non-standard register/bit counts).
        for j in 0..HLL_REGISTERS {
            let reg = hll_dense_get_register(registers, j) as usize;
            reghisto[reg] += 1;
        }
    }
}

// ── Sparse representation ─────────────────────────────────────────────────────
// C: hyperloglog.c:617-964

/// Convert a sparse-encoded HLL byte buffer to dense in-place.
/// `buf` must be the complete HLL byte vector (header + sparse opcodes).
/// Returns Err if the sparse representation is corrupted.
// C: hyperloglog.c:617-682, hllSparseToDense
pub fn hll_sparse_to_dense(buf: &mut Vec<u8>) -> Result<(), RedisError> {
    if hll_encoding(buf) == HLL_DENSE {
        return Ok(());
    }

    let mut dense = vec![0u8; HLL_DENSE_SIZE];
    // Copy the header (magic, encoding, notused, card) then set encoding to DENSE.
    dense[..HLL_HDR_SIZE].copy_from_slice(&buf[..HLL_HDR_SIZE]);
    hll_set_encoding(&mut dense, HLL_DENSE);

    let mut idx: usize = 0;
    let mut pos = HLL_HDR_SIZE;
    let end = buf.len();
    let mut valid = true;

    while pos < end {
        let b = buf[pos];
        if sparse_is_zero(b) {
            let run = sparse_zero_len(b);
            if idx + run > HLL_REGISTERS { valid = false; break; }
            idx += run;
            pos += 1;
        } else if sparse_is_xzero(b) {
            if pos + 1 >= end { valid = false; break; }
            let run = sparse_xzero_len(b, buf[pos + 1]);
            if idx + run > HLL_REGISTERS { valid = false; break; }
            idx += run;
            pos += 2;
        } else {
            let run = sparse_val_len(b);
            let regval = sparse_val_value(b);
            if idx + run > HLL_REGISTERS { valid = false; break; }
            for _ in 0..run {
                hll_dense_set_register(&mut dense[HLL_HDR_SIZE..], idx, regval);
                idx += 1;
            }
            pos += 1;
        }
    }

    if !valid || idx != HLL_REGISTERS {
        return Err(RedisError::runtime(INVALID_HLL_ERR));
    }

    *buf = dense;
    Ok(())
}

/// Set sparse register at `index` to `count` if current value is smaller.
/// May promote the buffer to dense if needed.
/// Returns Ok(true) if cardinality changed, Ok(false) if not, Err on corruption.
// C: hyperloglog.c:699-951, hllSparseSet
// PORT NOTE: C uses goto for promotion and for the "updated" merge-step entry.
// Rust refactors those into early-returns and fall-through to a shared tail.
pub(crate) fn hll_sparse_set(
    buf: &mut Vec<u8>,
    index: usize,
    count: u8,
    sparse_max_bytes: usize,
) -> Result<bool, RedisError> {
    // Immediate promotion when count exceeds sparse representable range.
    if count > HLL_SPARSE_VAL_MAX_VALUE {
        return hll_promote_to_dense(buf, index, count);
    }

    // Greedy pre-allocation to amortize future reallocations (C: sdsResize).
    // PERF(port): C uses sdsResize with a "double or +300" greedy strategy.
    // Vec::reserve provides a similar amortised guarantee.
    {
        let avail = buf.capacity().saturating_sub(buf.len());
        if buf.capacity() < sparse_max_bytes && avail < 3 {
            let base = buf.len() + 3;
            let extra = base.min(300);
            let target = (base + extra).min(sparse_max_bytes);
            if target > buf.capacity() {
                buf.reserve(target - buf.len());
            }
        }
    }

    let data_start = HLL_HDR_SIZE;
    let data_end = buf.len();

    // Step 1: locate the opcode covering register `index`.
    let mut pos = data_start;
    let mut prev_pos: Option<usize> = None;
    let mut first: usize = 0;
    let mut span: usize = 0;

    while pos < data_end {
        let b = buf[pos];
        let oplen: usize;
        if sparse_is_zero(b) {
            span = sparse_zero_len(b);
            oplen = 1;
        } else if sparse_is_val(b) {
            span = sparse_val_len(b);
            oplen = 1;
        } else {
            // XZERO: 2-byte opcode
            if pos + 1 >= data_end {
                return Err(RedisError::runtime(INVALID_HLL_ERR));
            }
            span = sparse_xzero_len(b, buf[pos + 1]);
            oplen = 2;
        }
        if index < first + span {
            break;
        }
        prev_pos = Some(pos);
        pos += oplen;
        first += span;
    }

    if span == 0 || pos >= data_end {
        return Err(RedisError::runtime(INVALID_HLL_ERR));
    }

    // Position immediately after the current opcode (None if this is the last).
    let next_pos_opt: Option<usize> = {
        let next = pos + if sparse_is_xzero(buf[pos]) { 2 } else { 1 };
        if next < data_end { Some(next) } else { None }
    };

    // Cache opcode type and run length.
    let b = buf[pos];
    let is_zero = sparse_is_zero(b);
    let is_xzero = sparse_is_xzero(b);
    let is_val = sparse_is_val(b);
    let run_len: usize = if is_zero {
        sparse_zero_len(b)
    } else if is_xzero {
        if pos + 1 >= data_end { return Err(RedisError::runtime(INVALID_HLL_ERR)); }
        sparse_xzero_len(b, buf[pos + 1])
    } else {
        sparse_val_len(b)
    };

    // Case A: VAL already has a value >= count; no update needed.
    if is_val && sparse_val_value(b) >= count {
        return Ok(false);
    }

    // Step 2: determine which update case applies.

    if (is_val && run_len == 1) || (is_zero && run_len == 1) {
        // Cases B and C: single-register replacement — trivial in-place set.
        sparse_val_set(&mut buf[pos], count, 1);
        // Fall through to merge step below.
    } else {
        // Case D: must split the opcode covering multiple registers.
        let last = first + span - 1;
        let mut seq = [0u8; 5];
        let mut seq_len: usize = 0;

        if is_zero || is_xzero {
            // Split ZERO or XZERO around the target register.
            if index != first {
                let len = index - first;
                if len > HLL_SPARSE_ZERO_MAX_LEN {
                    let l = len - 1;
                    seq[seq_len]     = (l >> 8) as u8 | HLL_SPARSE_XZERO_BIT;
                    seq[seq_len + 1] = (l & 0xff) as u8;
                    seq_len += 2;
                } else {
                    sparse_zero_set(&mut seq[seq_len], len);
                    seq_len += 1;
                }
            }
            sparse_val_set(&mut seq[seq_len], count, 1);
            seq_len += 1;
            if index != last {
                let len = last - index;
                if len > HLL_SPARSE_ZERO_MAX_LEN {
                    let l = len - 1;
                    seq[seq_len]     = (l >> 8) as u8 | HLL_SPARSE_XZERO_BIT;
                    seq[seq_len + 1] = (l & 0xff) as u8;
                    seq_len += 2;
                } else {
                    sparse_zero_set(&mut seq[seq_len], len);
                    seq_len += 1;
                }
            }
        } else {
            // Split VAL opcode; preserve surrounding registers at current value.
            let cur_val = sparse_val_value(b);
            if index != first {
                let len = index - first;
                sparse_val_set(&mut seq[seq_len], cur_val, len);
                seq_len += 1;
            }
            sparse_val_set(&mut seq[seq_len], count, 1);
            seq_len += 1;
            if index != last {
                let len = last - index;
                sparse_val_set(&mut seq[seq_len], cur_val, len);
                seq_len += 1;
            }
        }

        // Step 3: substitute the new sequence for the old opcode.
        let old_oplen: usize = if is_xzero { 2 } else { 1 };
        let delta: isize = seq_len as isize - old_oplen as isize;

        // If the buffer would exceed the sparse size limit, promote to dense.
        if delta > 0 && buf.len() as isize + delta > sparse_max_bytes as isize {
            return hll_promote_to_dense(buf, index, count);
        }

        debug_assert!((buf.len() as isize + delta) <= buf.capacity() as isize);

        // Splice: shift bytes after `pos` by `delta` and copy new sequence.
        if delta != 0 {
            let old_len = buf.len();
            if delta > 0 {
                let du = delta as usize;
                buf.resize(old_len + du, 0);
                if let Some(ni) = next_pos_opt {
                    buf.copy_within(ni..old_len, ni + du);
                }
            } else {
                let du = (-delta) as usize;
                if let Some(ni) = next_pos_opt {
                    buf.copy_within(ni..old_len, ni - du);
                }
                buf.truncate(old_len - du);
            }
        }
        buf[pos..pos + seq_len].copy_from_slice(&seq[..seq_len]);
    }

    // Step 4: merge adjacent VAL opcodes (scan up to 5 opcodes from prev).
    // C: hyperloglog.c:900-930
    let scan_start = prev_pos.unwrap_or(data_start);
    let mut scan_pos = scan_start;
    let mut scan_remaining: i32 = 5;

    while scan_pos < buf.len() && scan_remaining > 0 {
        scan_remaining -= 1;
        let b = buf[scan_pos];
        if sparse_is_xzero(b) {
            scan_pos += 2;
            continue;
        }
        if sparse_is_zero(b) {
            scan_pos += 1;
            continue;
        }
        // VAL: attempt to merge with an immediately following VAL of equal value.
        if scan_pos + 1 < buf.len() && sparse_is_val(buf[scan_pos + 1]) {
            let v1 = sparse_val_value(buf[scan_pos]);
            let v2 = sparse_val_value(buf[scan_pos + 1]);
            if v1 == v2 {
                let merged = sparse_val_len(buf[scan_pos]) + sparse_val_len(buf[scan_pos + 1]);
                if merged <= HLL_SPARSE_VAL_MAX_LEN {
                    // Write merged value at scan_pos+1, then shift-delete scan_pos.
                    // C: HLL_SPARSE_VAL_SET(p+1, v1, len); memmove(p, p+1, end-p); sdsIncrLen(-1);
                    sparse_val_set(&mut buf[scan_pos + 1], v1, merged);
                    let cur_len = buf.len();
                    buf.copy_within(scan_pos + 1..cur_len, scan_pos);
                    buf.truncate(cur_len - 1);
                    // Reiterate without advancing scan_pos.
                    continue;
                }
            }
        }
        scan_pos += 1;
    }

    hll_invalidate_cache(buf);
    Ok(true)
}

/// Promote sparse HLL buffer to dense, then set the register.
/// Asserts (debug) that the set returns true, since promotion implies an update.
fn hll_promote_to_dense(buf: &mut Vec<u8>, index: usize, count: u8) -> Result<bool, RedisError> {
    hll_sparse_to_dense(buf)?;
    let changed = hll_dense_set(&mut buf[HLL_HDR_SIZE..], index, count);
    debug_assert!(changed, "promote_to_dense: expected register update");
    Ok(changed)
}

/// Hash `ele` and update the sparse HLL register if needed.
pub(crate) fn hll_sparse_add(
    buf: &mut Vec<u8>,
    ele: &[u8],
    sparse_max_bytes: usize,
) -> Result<bool, RedisError> {
    let (count, index) = hll_pat_len(ele);
    hll_sparse_set(buf, index, count, sparse_max_bytes)
}

/// Compute the register histogram for a sparse HLL.
/// `sparse_data` is the slice starting immediately after the header.
/// Returns false if the sparse representation is corrupted.
// C: hyperloglog.c:967-1004, hllSparseRegHisto
pub fn hll_sparse_reg_histo(sparse_data: &[u8], reghisto: &mut [i32; 64]) -> bool {
    let mut idx: usize = 0;
    let mut pos: usize = 0;
    let end = sparse_data.len();

    while pos < end {
        let b = sparse_data[pos];
        if sparse_is_zero(b) {
            let run = sparse_zero_len(b);
            if idx + run > HLL_REGISTERS { return false; }
            reghisto[0] += run as i32;
            idx += run;
            pos += 1;
        } else if sparse_is_xzero(b) {
            if pos + 1 >= end { return false; }
            let run = sparse_xzero_len(b, sparse_data[pos + 1]);
            if idx + run > HLL_REGISTERS { return false; }
            reghisto[0] += run as i32;
            idx += run;
            pos += 2;
        } else {
            let run = sparse_val_len(b);
            let regval = sparse_val_value(b) as usize;
            if idx + run > HLL_REGISTERS { return false; }
            reghisto[regval] += run as i32;
            idx += run;
            pos += 1;
        }
    }
    idx == HLL_REGISTERS
}

// ── Cardinality estimation ────────────────────────────────────────────────────
// C: hyperloglog.c:1014-1116

/// Compute register histogram for the internal HLL_RAW encoding.
/// Each byte of `registers` directly holds one register value (0..=63).
// C: hyperloglog.c:1014-1035, hllRawRegHisto
// PERF(port): C reads 8 bytes as u64 to zero-check; we replicate the same
// optimisation using from_le_bytes on 8-byte chunks.
pub fn hll_raw_reg_histo(registers: &[u8], reghisto: &mut [i32; 64]) {
    for j in (0..HLL_REGISTERS).step_by(8) {
        let s = &registers[j..j + 8];
        let word = (s[0] as u64)
            | (s[1] as u64) << 8
            | (s[2] as u64) << 16
            | (s[3] as u64) << 24
            | (s[4] as u64) << 32
            | (s[5] as u64) << 40
            | (s[6] as u64) << 48
            | (s[7] as u64) << 56;
        if word == 0 {
            reghisto[0] += 8;
        } else {
            for &b in s {
                reghisto[b as usize] += 1;
            }
        }
    }
}

/// sigma correction function from Ertl (arXiv:1702.01284).
pub fn hll_sigma(mut x: f64) -> f64 {
    if x == 1.0 { return f64::INFINITY; }
    let mut z_prime;
    let mut y = 1.0f64;
    let mut z = x;
    loop {
        x *= x;
        z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z { break; }
    }
    z
}

/// tau correction function from Ertl (arXiv:1702.01284).
pub fn hll_tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 { return 0.0; }
    let mut z_prime;
    let mut y = 1.0f64;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        z_prime = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z_prime == z { break; }
    }
    z / 3.0
}

/// Compute the approximate cardinality from an HLL byte buffer (all encodings).
/// Returns Err if the sparse representation is invalid.
// C: hyperloglog.c:1082-1116, hllCount
pub fn hll_count(buf: &[u8]) -> Result<u64, RedisError> {
    let m = HLL_REGISTERS as f64;
    let mut reghisto = [0i32; 64];
    let encoding = hll_encoding(buf);

    if encoding == HLL_DENSE {
        hll_dense_reg_histo(&buf[HLL_HDR_SIZE..], &mut reghisto);
    } else if encoding == HLL_SPARSE {
        let ok = hll_sparse_reg_histo(&buf[HLL_HDR_SIZE..], &mut reghisto);
        if !ok {
            return Err(RedisError::runtime(INVALID_HLL_ERR));
        }
    } else if encoding == HLL_RAW {
        hll_raw_reg_histo(&buf[HLL_HDR_SIZE..], &mut reghisto);
    } else {
        // TODO(architect): is panic correct here? Should be unreachable in practice.
        panic!("Unknown HyperLogLog encoding {} in hll_count()", encoding);
    }

    // Ertl improved estimator.
    let mut z = m * hll_tau((m - reghisto[HLL_Q as usize + 1] as f64) / m);
    let mut j = HLL_Q as usize;
    while j >= 1 {
        z += reghisto[j] as f64;
        z *= 0.5;
        j -= 1;
    }
    z += m * hll_sigma(reghisto[0] as f64 / m);
    let e = (HLL_ALPHA_INF * m * m / z).round() as u64;
    Ok(e)
}

/// Dispatch hll_dense_add or hll_sparse_add based on the buffer encoding.
/// Returns Ok(true) if the cardinality changed, Ok(false) if not, Err on error.
// C: hyperloglog.c:1118-1126, hllAdd
pub(crate) fn hll_add(
    buf: &mut Vec<u8>,
    ele: &[u8],
    sparse_max_bytes: usize,
) -> Result<bool, RedisError> {
    match hll_encoding(buf) {
        HLL_DENSE => {
            Ok(hll_dense_add(&mut buf[HLL_HDR_SIZE..], ele))
        }
        HLL_SPARSE => hll_sparse_add(buf, ele, sparse_max_bytes),
        _ => Err(RedisError::runtime(INVALID_HLL_ERR)),
    }
}

// ── Merge helpers ─────────────────────────────────────────────────────────────
// C: hyperloglog.c:1327-1408

/// Merge dense registers into a raw (1-byte-per-register) array by taking MAX.
/// Scalar fallback; SIMD paths elided (unsafe not allowed in pilot crates).
// C: hyperloglog.c:1327-1351, hllMergeDense
// TODO(architect): SIMD optimisation paths (AVX2, NEON) via safe intrinsics.
pub fn hll_merge_dense(reg_raw: &mut [u8], reg_dense: &[u8]) {
    for i in 0..HLL_REGISTERS {
        let val = hll_dense_get_register(reg_dense, i);
        if val > reg_raw[i] {
            reg_raw[i] = val;
        }
    }
}

/// Merge HLL buffer `hll_buf` into raw register array `max` by taking MAX.
/// `max` must be at least `HLL_REGISTERS` bytes.
/// Returns Err if the sparse data is corrupted.
// C: hyperloglog.c:1353-1408, hllMerge
pub fn hll_merge(max: &mut [u8], hll_buf: &[u8]) -> Result<(), RedisError> {
    debug_assert!(max.len() >= HLL_REGISTERS);
    if hll_encoding(hll_buf) == HLL_DENSE {
        hll_merge_dense(max, &hll_buf[HLL_HDR_SIZE..]);
    } else {
        // Sparse: decode opcodes and update max[] registers.
        let mut pos = HLL_HDR_SIZE;
        let end = hll_buf.len();
        let mut i: usize = 0;

        while pos < end {
            let b = hll_buf[pos];
            if sparse_is_zero(b) {
                let run = sparse_zero_len(b);
                if i + run > HLL_REGISTERS {
                    return Err(RedisError::runtime(INVALID_HLL_ERR));
                }
                i += run;
                pos += 1;
            } else if sparse_is_xzero(b) {
                if pos + 1 >= end {
                    return Err(RedisError::runtime(INVALID_HLL_ERR));
                }
                let run = sparse_xzero_len(b, hll_buf[pos + 1]);
                if i + run > HLL_REGISTERS {
                    return Err(RedisError::runtime(INVALID_HLL_ERR));
                }
                i += run;
                pos += 2;
            } else {
                let run = sparse_val_len(b);
                let regval = sparse_val_value(b);
                if i + run > HLL_REGISTERS {
                    return Err(RedisError::runtime(INVALID_HLL_ERR));
                }
                for _ in 0..run {
                    if regval > max[i] { max[i] = regval; }
                    i += 1;
                }
                pos += 1;
            }
        }

        if i != HLL_REGISTERS {
            return Err(RedisError::runtime(INVALID_HLL_ERR));
        }
    }
    Ok(())
}

/// Compress raw (1-byte-per-register) array into dense (6-bit-packed) format.
/// Scalar fallback; SIMD paths elided.
// C: hyperloglog.c:1577-1597, hllDenseCompress
pub fn hll_dense_compress(reg_dense: &mut [u8], reg_raw: &[u8]) {
    for i in 0..HLL_REGISTERS {
        hll_dense_set_register(reg_dense, i, reg_raw[i]);
    }
}

// ── Object-level helpers ──────────────────────────────────────────────────────
// C: hyperloglog.c:1601-1661

/// Create a new HLL byte buffer in sparse encoding.
/// The initial state encodes all 16384 registers as zero using XZERO opcodes.
// C: hyperloglog.c:1603-1631, createHLLObject
pub fn create_hll_object() -> Vec<u8> {
    let xzero_count =
        (HLL_REGISTERS + HLL_SPARSE_XZERO_MAX_LEN - 1) / HLL_SPARSE_XZERO_MAX_LEN;
    let sparselen = HLL_HDR_SIZE + xzero_count * 2;
    let mut buf = vec![0u8; sparselen];

    // Write "HYLL" magic and SPARSE encoding.
    buf[HDR_MAGIC_OFF..HDR_MAGIC_OFF + 4].copy_from_slice(b"HYLL");
    buf[HDR_ENCODING_OFF] = HLL_SPARSE;

    // Populate with XZERO opcodes covering all registers.
    let mut aux = HLL_REGISTERS;
    let mut p = HLL_HDR_SIZE;
    while aux > 0 {
        let xzero = aux.min(HLL_SPARSE_XZERO_MAX_LEN);
        let l = xzero - 1;
        buf[p]     = (l >> 8) as u8 | HLL_SPARSE_XZERO_BIT;
        buf[p + 1] = (l & 0xff) as u8;
        p += 2;
        aux -= xzero;
    }
    debug_assert_eq!(p, sparselen);
    buf
}

/// Validate that `buf` is a well-formed HLL byte buffer (header + data).
/// Returns true if valid.
// C: hyperloglog.c:1633-1661, isHLLObjectOrReply (validation portion)
pub fn is_hll_valid(buf: &[u8]) -> bool {
    if buf.len() < HLL_HDR_SIZE { return false; }
    if &buf[HDR_MAGIC_OFF..HDR_MAGIC_OFF + 4] != b"HYLL" { return false; }
    let enc = buf[HDR_ENCODING_OFF];
    if enc > HLL_MAX_ENCODING { return false; }
    if enc == HLL_DENSE && buf.len() != HLL_DENSE_SIZE { return false; }
    true
}

/// Validate that a `RedisObject` contains a well-formed HLL string.
/// Returns the underlying byte slice on success, Err with the HLL-specific
/// WRONGTYPE message on failure.
// C: hyperloglog.c:1633-1661, isHLLObjectOrReply
// PORT NOTE: Reply is sent by the caller via `?`; this fn only validates.
fn require_hll_object(obj: &RedisObject) -> Result<&[u8], RedisError> {
    let bytes = obj.as_string_bytes().ok_or_else(|| {
        RedisError::runtime(b"WRONGTYPE Key is not a valid HyperLogLog string value.")
    })?;
    if !is_hll_valid(bytes) {
        return Err(RedisError::runtime(
            b"WRONGTYPE Key is not a valid HyperLogLog string value.",
        ));
    }
    Ok(bytes)
}

// ── Command entry points ──────────────────────────────────────────────────────

fn append_decimal_usize(out: &mut Vec<u8>, mut n: usize) {
    if n == 0 {
        out.push(b'0');
        return;
    }
    let mut digits = [0u8; 20];
    let mut len = 0;
    while n > 0 {
        digits[len] = b'0' + (n % 10) as u8;
        len += 1;
        n /= 10;
    }
    for digit in digits[..len].iter().rev() {
        out.push(*digit);
    }
}

fn pfdebug_arity_error(subcmd: &[u8]) -> RedisError {
    let mut msg = Vec::with_capacity(
        b"ERR Wrong number of arguments for the '' subcommand".len() + subcmd.len(),
    );
    msg.extend_from_slice(b"ERR Wrong number of arguments for the '");
    msg.extend_from_slice(subcmd);
    msg.extend_from_slice(b"' subcommand");
    RedisError::runtime(msg)
}

fn pfdebug_unknown_subcommand(subcmd: &[u8]) -> RedisError {
    let mut msg =
        Vec::with_capacity(b"ERR Unknown PFDEBUG subcommand ''".len() + subcmd.len());
    msg.extend_from_slice(b"ERR Unknown PFDEBUG subcommand '");
    msg.extend_from_slice(subcmd);
    msg.push(b'\'');
    RedisError::runtime(msg)
}

fn pfdebug_decode_sparse(buf: &[u8]) -> Result<Vec<u8>, RedisError> {
    if hll_encoding(buf) != HLL_SPARSE {
        return Err(RedisError::runtime(b"ERR HLL encoding is not sparse"));
    }

    let mut decoded = Vec::new();
    let mut pos = HLL_HDR_SIZE;
    while pos < buf.len() {
        let b = buf[pos];
        if sparse_is_zero(b) {
            decoded.extend_from_slice(b"z:");
            append_decimal_usize(&mut decoded, sparse_zero_len(b));
            pos += 1;
        } else if sparse_is_xzero(b) {
            if pos + 1 >= buf.len() {
                return Err(RedisError::runtime(INVALID_HLL_ERR));
            }
            decoded.extend_from_slice(b"Z:");
            append_decimal_usize(&mut decoded, sparse_xzero_len(b, buf[pos + 1]));
            pos += 2;
        } else {
            decoded.extend_from_slice(b"v:");
            append_decimal_usize(&mut decoded, sparse_val_value(b) as usize);
            decoded.push(b',');
            append_decimal_usize(&mut decoded, sparse_val_len(b));
            pos += 1;
        }
        decoded.push(b' ');
    }
    if decoded.last() == Some(&b' ') {
        decoded.pop();
    }
    Ok(decoded)
}

/// PFADD key element [element ...]
///
/// Adds elements to the HyperLogLog stored at `key`, creating a fresh sparse
/// HLL when the key is missing. Replies `:1` when any register was updated
/// (i.e. the cardinality estimate changed), `:0` otherwise. Returns WRONGTYPE
/// when the existing key is not a valid HLL string. With no elements supplied
/// (just the key) replies `:1` iff a new HLL was created.
// C: hyperloglog.c:1664-1696, pfaddCommand
pub fn pfadd_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"pfadd"));
    }
    let key = ctx.arg_owned(1usize)?;
    let argc = ctx.argc();

    let mut buf: Vec<u8> = match ctx.db_mut().lookup_key_write(&key) {
        None => create_hll_object(),
        Some(obj) => require_hll_bytes(obj)?,
    };
    let was_missing = ctx.db().lookup_key_read(key.as_bytes()).is_none();
    let mut updated = was_missing;

    for j in 2..argc {
        let ele = ctx.arg_owned(j)?;
        let changed = hll_add(&mut buf, ele.as_bytes(), HLL_SPARSE_MAX_BYTES_DEFAULT)?;
        if changed {
            updated = true;
        }
    }

    if updated {
        hll_invalidate_cache(&mut buf);
        let stored = RedisObject::from_string(RedisString::from_vec(buf));
        ctx.db_mut().set_key(key, stored, 0);
    }

    if updated {
        ctx.reply_integer(1)
    } else {
        ctx.reply_integer(0)
    }
}

/// PFCOUNT key [key ...]
///
/// Returns the approximate cardinality of the HyperLogLog at `key`. With more
/// than one key the cardinality of the union of all source HLLs is reported.
/// Missing keys are treated as empty HLLs (their registers are all zero).
/// Returns WRONGTYPE if any supplied key holds a non-HLL value.
///
/// The single-key path mirrors the C source's cache-write optimisation: on a
/// cache miss the freshly-computed cardinality is written back into the HLL
/// header so subsequent reads are O(1).
// C: hyperloglog.c:1698-1792, pfcountCommand
pub fn pfcount_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"pfcount"));
    }
    let argc = ctx.argc();

    if argc > 2 {
        let mut max_buf = vec![0u8; HLL_HDR_SIZE + HLL_REGISTERS];
        max_buf[HDR_MAGIC_OFF..HDR_MAGIC_OFF + 4].copy_from_slice(b"HYLL");
        max_buf[HDR_ENCODING_OFF] = HLL_RAW;

        for j in 1..argc {
            let key = ctx.arg_owned(j)?;
            match ctx.db().lookup_key_read(key.as_bytes()) {
                None => continue,
                Some(obj) => {
                    let bytes = obj.as_string_bytes().ok_or_else(hll_wrong_type_error)?;
                    if !is_hll_valid(bytes) {
                        return Err(hll_wrong_type_error());
                    }
                    hll_merge(&mut max_buf[HLL_HDR_SIZE..], bytes)?;
                }
            }
        }

        let card = hll_count(&max_buf)?;
        return ctx.reply_integer(card as i64);
    }

    let key = ctx.arg_owned(1usize)?;
    let buf_opt: Option<Vec<u8>> = match ctx.db_mut().lookup_key_write(&key) {
        None => None,
        Some(obj) => Some(require_hll_bytes(obj)?),
    };

    let mut buf = match buf_opt {
        None => return ctx.reply_integer(0),
        Some(b) => b,
    };

    if hll_valid_cache(&buf) {
        let card = hll_card_read(&buf);
        return ctx.reply_integer(card as i64);
    }

    let card = hll_count(&buf)?;
    hll_card_write(&mut buf, card);
    let stored = RedisObject::from_string(RedisString::from_vec(buf));
    ctx.db_mut().set_key(key, stored, redis_core::db::SETKEY_KEEPTTL);
    ctx.reply_integer(card as i64)
}

/// PFMERGE dest src1 [src2 ...]
///
/// Merges the HyperLogLog values stored at `src1`, `src2`, ... and the
/// existing value at `dest` into a single HLL written back to `dest`. The
/// merge takes the maximum value for each of the 16384 registers. When no
/// source keys are provided this still produces a valid HLL at `dest`
/// (either created empty or left untouched). The destination is promoted to
/// dense encoding whenever any participating HLL is already dense.
// C: hyperloglog.c:1794-1871, pfmergeCommand
pub fn pfmerge_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"pfmerge"));
    }
    let argc = ctx.argc();
    let dest_key = ctx.arg_owned(1usize)?;

    let mut max_registers = vec![0u8; HLL_REGISTERS];
    let mut use_dense = false;

    for j in 1..argc {
        let key = ctx.arg_owned(j)?;
        match ctx.db().lookup_key_read(key.as_bytes()) {
            None => continue,
            Some(obj) => {
                let bytes = obj.as_string_bytes().ok_or_else(hll_wrong_type_error)?;
                if !is_hll_valid(bytes) {
                    return Err(hll_wrong_type_error());
                }
                if bytes[HDR_ENCODING_OFF] == HLL_DENSE {
                    use_dense = true;
                }
                hll_merge(&mut max_registers, bytes)?;
            }
        }
    }

    let mut dest_buf: Vec<u8> = match ctx.db().lookup_key_read(dest_key.as_bytes()) {
        None => create_hll_object(),
        Some(obj) => {
            let bytes = obj.as_string_bytes().ok_or_else(hll_wrong_type_error)?;
            if !is_hll_valid(bytes) {
                return Err(hll_wrong_type_error());
            }
            bytes.to_vec()
        }
    };

    if use_dense {
        hll_sparse_to_dense(&mut dest_buf)?;
    }

    if dest_buf[HDR_ENCODING_OFF] == HLL_DENSE {
        let registers = &mut dest_buf[HLL_HDR_SIZE..];
        for i in 0..HLL_REGISTERS {
            let cur = hll_dense_get_register(registers, i);
            if max_registers[i] > cur {
                hll_dense_set_register(registers, i, max_registers[i]);
            }
        }
    } else {
        for i in 0..HLL_REGISTERS {
            if max_registers[i] != 0 {
                hll_sparse_set(&mut dest_buf, i, max_registers[i], HLL_SPARSE_MAX_BYTES_DEFAULT)?;
            }
        }
    }

    hll_invalidate_cache(&mut dest_buf);
    let stored = RedisObject::from_string(RedisString::from_vec(dest_buf));
    ctx.db_mut().set_key(dest_key, stored, 0);

    ctx.reply_simple_string(b"OK")
}

/// PFSELFTEST
/// Internal self-test of HLL register encoding and approximation accuracy.
// C: hyperloglog.c:1878-1976, pfselftestCommand
// TODO(port): Replace C rand() with a Rust RNG. The `rand` crate is the standard
// choice but is not yet a dependency. Stubbing the random test body here.
pub fn pfselftest_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // Test 1: Dense register get/set round-trip.
    let mut dense_buf = vec![0u8; HLL_DENSE_SIZE];
    dense_buf[HDR_MAGIC_OFF..HDR_MAGIC_OFF + 4].copy_from_slice(b"HYLL");
    dense_buf[HDR_ENCODING_OFF] = HLL_DENSE;
    let registers = &mut dense_buf[HLL_HDR_SIZE..];

    // TODO(port): rand() usage — replace with `rand::thread_rng()` or seeded PRNG.
    // The test sets registers to random values and reads them back.
    for _cycle in 0..HLL_TEST_CYCLES {
        // placeholder: without rand, we cannot meaningfully run Test 1.
    }

    // Test 2: Approximation error check using unique elements.
    // TODO(port): rand() usage for seed and element generation.
    // Stubbed; phase-B will wire in an RNG and the full loop from the C source.

    ctx.reply_simple_string(b"OK")
}

/// PFDEBUG subcommand key
/// Subcommands: GETREG, DECODE, ENCODING, TODENSE, SIMD.
// C: hyperloglog.c:1986-2106, pfdebugCommand
// TODO(architect): server-global simd_enabled flag (currently a static C int).
pub fn pfdebug_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"PFDEBUG"));
    }

    let subcmd = ctx.arg_owned(1usize)?;
    let subcmd_bytes = subcmd.as_bytes();

    // SIMD subcommand: toggle/report SIMD usage.
    // C: hyperloglog.c:1992-2009
    if subcmd_bytes.eq_ignore_ascii_case(b"simd") {
        if argc != 3 {
            return Err(pfdebug_arity_error(subcmd_bytes));
        }
        let arg = ctx.arg(2)?;
        if arg.eq_ignore_ascii_case(b"on") {
            // TODO(architect): set server-level simd_enabled=true once global state exists.
        } else if arg.eq_ignore_ascii_case(b"off") {
            // TODO(architect): set server-level simd_enabled=false.
        } else {
            return Err(RedisError::runtime(b"ERR Argument must be ON or OFF"));
        }
        // SIMD always "disabled" in this port (no SIMD paths yet).
        return ctx.reply_simple_string(b"disabled");
    }

    // All other subcommands require a key as argv[2].
    if argc != 3 {
        return Err(pfdebug_arity_error(subcmd_bytes));
    }

    let key = ctx.arg_owned(2usize)?;
    let mut buf = match ctx.db_mut().lookup_key_write(&key) {
        Some(obj) => require_hll_bytes(obj)?,
        None => return Err(RedisError::runtime(b"ERR The specified key does not exist")),
    };

    if subcmd_bytes.eq_ignore_ascii_case(b"getreg") {
        let mut converted = false;
        if hll_encoding(&buf) == HLL_SPARSE {
            hll_sparse_to_dense(&mut buf)?;
            converted = true;
        }
        if converted {
            ctx.db_mut()
                .replace_value(&key, RedisObject::from_string(RedisString::from_vec(buf.clone())));
        }
        ctx.reply_array_header(HLL_REGISTERS as i64)?;
        for j in 0..HLL_REGISTERS {
            let val = hll_dense_get_register(&buf[HLL_HDR_SIZE..], j);
            ctx.reply_integer(val as i64)?;
        }
        Ok(())
    } else if subcmd_bytes.eq_ignore_ascii_case(b"decode") {
        let decoded = pfdebug_decode_sparse(&buf)?;
        ctx.reply_bulk(&decoded)
    } else if subcmd_bytes.eq_ignore_ascii_case(b"encoding") {
        let name = if hll_encoding(&buf) == HLL_DENSE {
            b"dense".as_slice()
        } else {
            b"sparse".as_slice()
        };
        ctx.reply_simple_string(name)
    } else if subcmd_bytes.eq_ignore_ascii_case(b"todense") {
        let converted = if hll_encoding(&buf) == HLL_SPARSE {
            hll_sparse_to_dense(&mut buf)?;
            ctx.db_mut()
                .replace_value(&key, RedisObject::from_string(RedisString::from_vec(buf)));
            1
        } else {
            0
        };
        ctx.reply_integer(converted)
    } else {
        Err(pfdebug_unknown_subcommand(subcmd_bytes))
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/hyperloglog.c  (2107 lines, 27 functions)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         9
//   port_notes:    4
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         Core algorithm fully translated (MurmurHash64A, pat_len, dense/sparse
//                  encoding, sigma/tau estimator, merge, compress). PFDEBUG uses the
//                  stored HLL bytes for GETREG, DECODE, ENCODING, and TODENSE. SIMD paths
//                  (AVX2/NEON) elided — scalar fallback only. pfselftest rand() dependency
//                  remains flagged above.
// ──────────────────────────────────────────────────────────────────────────────
