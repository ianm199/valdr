//! Utility functions.
//! Provides:
//! - Glob-style pattern matching (`string_match_len`, `string_match`, `prefix_match_len`)
//! - Human-readable memory-size parsing (`mem_to_ull`)
//! - Fast integer-string conversion (`digits10`, `ll_to_string`, `ull_to_string`)
//! - Strict string-to-number converters (`string2ll`, `string2ull`, `string2l`,
//! `string2ul_base16_async_signal_safe`, `string2ld`, `string2d`)
//! - Float-string converters (`d2string`, `fixedpoint_d2string`, `ld2string`,
//! `trim_double_string`, `double2ll`)
//! - SHA256-HMAC counter-mode random bytes / random hex chars
//! - Filesystem helpers (`get_absolute_path`, `dir_create_if_missing`, `dir_remove`, …)
//! - Async-signal-safe I/O helpers (`fgets_async_signal_safe`, `vsnprintf_async_signal_safe`)
//! - Time helpers (`ustime`, `mstime`)
//! - Miscellaneous: `wang_hash64`, `escape_json_string`, `write_pointer_with_padding`

use std::io;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

// TODO(architect): sha2 crate dependency required for SHA256 operations used in
// `get_hash_seed_from_string` and `get_random_bytes`. is marked SKIP
// in harness/file-deps.tsv. Add `sha2 = "0.10"` to crates/redis-core/Cargo.toml
// and import `sha2::{Sha256, Digest}` here once the dependency is wired.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum characters needed to represent a `long double` as a string.
pub const MAX_LONG_DOUBLE_CHARS: usize = 5 * 1024;

/// Maximum characters needed to represent a `double` with `%f`.
pub const MAX_DOUBLE_CHARS: usize = 400;

/// Maximum characters for `d2string` / `fpconv_dtoa`.
pub const MAX_D2STRING_CHARS: usize = 128;

/// Bytes needed for a `long long` → string, including null terminator.
pub const LONG_STR_SIZE: usize = 21;

/// SHA-256 digest size in bytes.
const SHA256_BLOCK_SIZE: usize = 32;

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

/// Millisecond timestamp. C: `typedef long long mstime_t`.
pub type MsTime = i64;

/// Microsecond timestamp. C: `typedef long long ustime_t`.
pub type UsTime = i64;

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Formatting mode for `ld2string`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ld2StringMode {
 /// `%.17Lg` — automatic exponential format.
    Auto,
 /// `%.17Lf` with trailing-zero trimming.
    Human,
 /// `%La` — hexadecimal float representation.
    Hex,
}

// ---------------------------------------------------------------------------
// Global random state (protected by Mutex; C original is NOT thread-safe)
// ---------------------------------------------------------------------------

/// Internal state for the SHA256-HMAC counter-mode random generator.
/// `static uint64_t counter` inside `getRandomBytes`.
/// PORT NOTE: In C the state is unprotected globals; here we wrap in `Mutex`
/// to be correct under Rust's threading rules, even though the C server is
/// pre-pilot single-threaded.
struct RandomState {
    seed: [u8; 64],
    initialized: bool,
    counter: u64,
}

impl RandomState {
    const fn new() -> Self {
        Self {
            seed: [0u8; 64],
            initialized: false,
            counter: 0,
        }
    }
}

static RANDOM_STATE: Mutex<RandomState> = Mutex::new(RandomState::new());

// ---------------------------------------------------------------------------
// Pattern matching
// ---------------------------------------------------------------------------

/// Core recursive glob-pattern match implementation.
/// Recursion depth is capped at 1000 to prevent stack exhaustion on
/// pathological inputs (mirrors the C guard `if (nesting > 1000) return 0`).
fn stringmatchlen_impl(
    pattern: &[u8],
    string: &[u8],
    nocase: bool,
    skip_longer_matches: &mut bool,
    nesting: i32,
) -> bool {
    if nesting > 1000 {
        return false;
    }

    let mut pat = pattern;
    let mut s = string;

    while !pat.is_empty() && !s.is_empty() {
        match pat[0] {
            b'*' => {
 // Collapse consecutive '*'
                while pat.len() > 1 && pat[1] == b'*' {
                    pat = &pat[1..];
                }
                if pat.len() == 1 {
                    return true;
                }
                while !s.is_empty() {
                    if stringmatchlen_impl(&pat[1..], s, nocase, skip_longer_matches, nesting + 1) {
                        return true;
                    }
                    if *skip_longer_matches {
                        return false;
                    }
                    s = &s[1..];
                }
                *skip_longer_matches = true;
                return false;
            }
            b'?' => {
                s = &s[1..];
            }
            b'[' => {
                pat = &pat[1..];
                let not_op = !pat.is_empty() && pat[0] == b'^';
                if not_op {
                    pat = &pat[1..];
                }
                let mut matched = false;
                loop {
                    if pat.len() >= 2 && pat[0] == b'\\' {
                        pat = &pat[1..];
                        if pat[0] == s[0] {
                            matched = true;
                        }
                    } else if pat.is_empty() {
 // Malformed pattern: back up to include the ']' we'll skip below
 // In our slice representation we can't back up; skip break only.
                        break;
                    } else if pat[0] == b']' {
                        break;
                    } else if pat.len() >= 3 && pat[1] == b'-' {
                        let (mut start, mut end, mut c) = (pat[0], pat[2], s[0]);
                        if start > end {
                            std::mem::swap(&mut start, &mut end);
                        }
                        if nocase {
                            start = start.to_ascii_lowercase();
                            end = end.to_ascii_lowercase();
                            c = c.to_ascii_lowercase();
                        }
                        pat = &pat[2..];
                        if c >= start && c <= end {
                            matched = true;
                        }
                    } else {
                        let pc = if nocase {
                            pat[0].to_ascii_lowercase()
                        } else {
                            pat[0]
                        };
                        let sc = if nocase {
                            s[0].to_ascii_lowercase()
                        } else {
                            s[0]
                        };
                        if pc == sc {
                            matched = true;
                        }
                    }
                    if pat.is_empty() {
                        break;
                    }
                    pat = &pat[1..];
                }
                if not_op {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                s = &s[1..];
            }
            b'\\' => {
                if pat.len() >= 2 {
                    pat = &pat[1..];
                }
 // fall through to default literal match
                let pc = if nocase {
                    pat[0].to_ascii_lowercase()
                } else {
                    pat[0]
                };
                let sc = if nocase {
                    s[0].to_ascii_lowercase()
                } else {
                    s[0]
                };
                if pc != sc {
                    return false;
                }
                s = &s[1..];
            }
            _ => {
                let pc = if nocase {
                    pat[0].to_ascii_lowercase()
                } else {
                    pat[0]
                };
                let sc = if nocase {
                    s[0].to_ascii_lowercase()
                } else {
                    s[0]
                };
                if pc != sc {
                    return false;
                }
                s = &s[1..];
            }
        }

        pat = &pat[1..];

        if s.is_empty() {
 // Consume trailing '*' characters
            while !pat.is_empty() && pat[0] == b'*' {
                pat = &pat[1..];
            }
            break;
        }
    }

    pat.is_empty() && s.is_empty()
}

/// Glob-style pattern match with explicit lengths.
/// Returns `true` if `string` matches `pattern`. `nocase = true` makes
/// comparison case-insensitive.
/// int stringLen, int nocase)`.
pub fn string_match_len(pattern: &[u8], string: &[u8], nocase: bool) -> bool {
    let mut skip = false;
    stringmatchlen_impl(pattern, string, nocase, &mut skip, 0)
}

/// Glob-style pattern match on NUL-terminated-style byte slices.
pub fn string_match(pattern: &[u8], string: &[u8], nocase: bool) -> bool {
    string_match_len(pattern, string, nocase)
}

/// Returns `true` if `string` matches a prefix-glob `pattern` (must end with `*`).
/// Fast-path for the common case of an exact `"*"` pattern; rejects patterns
/// that don't end with `*`.
pub fn prefix_match_len(pattern: &[u8], string: &[u8], nocase: bool) -> bool {
    if pattern == b"*" {
        return true;
    }
    if pattern.is_empty() || *pattern.last().expect("non-empty already checked") != b'*' {
        return false;
    }
    string_match_len(pattern, string, nocase)
}

/// Fuzz-test `string_match_len` with random inputs.
/// Returns total number of matches observed over 10,000,000 random iterations.
pub fn string_match_len_fuzz_test() -> i32 {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0u64, |d| d.as_nanos() as u64);
    let mut rng = seed;
    let mut total_matches: i32 = 0;
    let mut cycles = 10_000_000i32;

    let lcg = |state: &mut u64| -> u8 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (*state >> 33) as u8 & 0x7f
    };

    while cycles > 0 {
        cycles -= 1;
        let slen = (lcg(&mut rng) as usize) % 32;
        let plen = (lcg(&mut rng) as usize) % 32;
        let s: Vec<u8> = (0..slen).map(|_| lcg(&mut rng) % 128).collect();
        let p: Vec<u8> = (0..plen).map(|_| lcg(&mut rng) % 128).collect();
        if string_match_len(&p, &s, false) {
            total_matches += 1;
        }
    }
    total_matches
}

// ---------------------------------------------------------------------------
// Memory-size parsing
// ---------------------------------------------------------------------------

/// Parse a human-readable memory size string into bytes.
/// Accepted suffixes (case-insensitive): `b`, `k`, `kb`, `m`, `mb`, `g`, `gb`.
/// Returns `None` on parse error (C returns 0 and sets `*err = 1`).
pub fn mem_to_ull(p: &[u8]) -> Option<u64> {
    if p.is_empty() || p[0] == b'-' {
        return None;
    }

    let digit_end = p.iter().take_while(|&&b| b.is_ascii_digit()).count();
    let suffix = &p[digit_end..];
    let digits = &p[..digit_end];

    let mul: u64 = if suffix.is_empty() || suffix.eq_ignore_ascii_case(b"b") {
        1
    } else if suffix.eq_ignore_ascii_case(b"k") {
        1_000
    } else if suffix.eq_ignore_ascii_case(b"kb") {
        1_024
    } else if suffix.eq_ignore_ascii_case(b"m") {
        1_000_000
    } else if suffix.eq_ignore_ascii_case(b"mb") {
        1_024 * 1_024
    } else if suffix.eq_ignore_ascii_case(b"g") {
        1_000_000_000
    } else if suffix.eq_ignore_ascii_case(b"gb") {
        1_024 * 1_024 * 1_024
    } else {
        return None;
    };

    if digits.is_empty() || digits.len() >= 128 {
        return None;
    }

 // Parse digit string without UTF-8 conversion: all bytes are ASCII digits.
    let mut val: u64 = 0;
    for &b in digits {
        val = val.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    val.checked_mul(mul)
}

/// Search a byte buffer for the first occurrence of any byte in `chars`.
/// Mirrors `strpbrk` but works on length-prefixed buffers.
/// Returns the index of the first match, or `None`.
pub fn mempbrk(s: &[u8], chars: &[u8]) -> Option<usize> {
    for (j, &b) in s.iter().enumerate() {
        if chars.contains(&b) {
            return Some(j);
        }
    }
    None
}

/// In-place byte translation: replace each byte in `from` with
/// corresponding byte in `to` across the buffer `s`.
/// size_t setlen)`.
pub fn memmapchars(s: &mut [u8], from: &[u8], to: &[u8]) {
    debug_assert_eq!(from.len(), to.len(), "from/to must have equal length");
    for b in s.iter_mut() {
        if let Some(i) = from.iter().position(|&f| f == *b) {
            *b = to[i];
        }
    }
}

// ---------------------------------------------------------------------------
// Integer-digit counting and fast integer→string formatting
// ---------------------------------------------------------------------------

/// Return the number of decimal digits needed to represent `v`.
pub fn digits10(v: u64) -> u32 {
    if v < 10 {
        return 1;
    }
    if v < 100 {
        return 2;
    }
    if v < 1_000 {
        return 3;
    }
    if v < 1_000_000_000_000 {
        if v < 100_000_000 {
            if v < 1_000_000 {
                if v < 10_000 {
                    return 4;
                }
                return 5 + (v >= 100_000) as u32;
            }
            return 7 + (v >= 10_000_000) as u32;
        }
        if v < 10_000_000_000 {
            return 9 + (v >= 1_000_000_000) as u32;
        }
        return 11 + (v >= 100_000_000_000) as u32;
    }
    12 + digits10(v / 1_000_000_000_000)
}

/// Like `digits10` but accounts for the minus sign on negative values.
pub fn sdigits10(v: i64) -> u32 {
    if v < 0 {
        let uv: u64 = if v != i64::MIN {
            (-v) as u64
        } else {
            (i64::MAX as u64) + 1
        };
        digits10(uv) + 1
    } else {
        digits10(v as u64)
    }
}

/// Lookup table of two-digit decimal strings used by `ull_to_string`.
const DIGITS: &[u8; 200] = b"00010203040506070809\
                              10111213141516171819\
                              20212223242526272829\
                              30313233343536373839\
                              40414243444546474849\
                              50515253545556575859\
                              60616263646566676869\
                              70717273747576777879\
                              80818283848586878889\
                              90919293949596979899";

/// Write `value` as a decimal string into `dst`.
/// Returns the number of bytes written (excluding a null terminator the C
/// version appended), or 0 if the buffer is too small.
/// The Rust version writes into a `&mut [u8]` slice and does
/// NOT write a null terminator (callers that need one must add it separately).
pub fn ull_to_string(dst: &mut [u8], value: u64) -> usize {
    let length = digits10(value) as usize;
    if length > dst.len() {
        if !dst.is_empty() {
            dst[0] = 0;
        }
        return 0;
    }

    let mut v = value;
    let mut next = length - 1;

    while v >= 100 {
        let i = ((v % 100) * 2) as usize;
        v /= 100;
        dst[next] = DIGITS[i + 1];
        dst[next - 1] = DIGITS[i];
        next = next.wrapping_sub(2);
    }

    if v < 10 {
        dst[next] = b'0' + v as u8;
    } else {
        let i = (v * 2) as usize;
        dst[next] = DIGITS[i + 1];
        dst[next - 1] = DIGITS[i];
    }

    length
}

/// Write `svalue` (signed) as a decimal string into `dst`.
/// Returns the number of bytes written or 0 if the buffer is too small.
pub fn ll_to_string(dst: &mut [u8], svalue: i64) -> usize {
    if svalue < 0 {
        let value: u64 = if svalue != i64::MIN {
            (-svalue) as u64
        } else {
            (i64::MAX as u64) + 1
        };
        if dst.len() < 2 {
            if !dst.is_empty() {
                dst[0] = 0;
            }
            return 0;
        }
        dst[0] = b'-';
        let written = ull_to_string(&mut dst[1..], value);
        if written == 0 {
            return 0;
        }
        written + 1
    } else {
        ull_to_string(dst, svalue as u64)
    }
}

// ---------------------------------------------------------------------------
// String → integer conversion
// ---------------------------------------------------------------------------

/// Strict conversion of a decimal byte string to `i64`.
/// Returns `Some(v)` iff `s` represents a valid, non-overflowing signed
/// 64-bit integer with no leading zeros (except the bare `"0"`) and no
/// surrounding whitespace.
/// The AVX-512 SIMD path (`string2llAVX512`) is intentionally
/// omitted; use the scalar path for all targets.
/// PERF(port): C selects AVX-512 at runtime via `ifunc`; Rust always uses
/// scalar. Profile in Phase B if this is hot.
pub fn string2ll(s: &[u8]) -> Option<i64> {
 // Empty string or too long
    if s.is_empty() || s.len() >= LONG_STR_SIZE {
        return None;
    }

 // Single-digit fast path
    if s.len() == 1 && s[0] >= b'0' && s[0] <= b'9' {
        return Some((s[0] - b'0') as i64);
    }

    let mut idx = 0usize;
    let negative = s[0] == b'-';
    if negative {
        idx += 1;
        if idx == s.len() {
            return None; // lone '-'
        }
    }

 // No leading zeros allowed (unless the whole string is "0")
    if s[idx] < b'1' || s[idx] > b'9' {
        return None;
    }

    let mut v = (s[idx] - b'0') as u64;
    idx += 1;

    while idx < s.len() {
        let c = s[idx];
        if !(b'0'..=b'9').contains(&c) {
            return None;
        }
        if v > u64::MAX / 10 {
            return None; // overflow
        }
        v *= 10;
        let digit = (c - b'0') as u64;
        if v > u64::MAX - digit {
            return None; // overflow
        }
        v += digit;
        idx += 1;
    }

    if negative {
 // The most-negative i64 has magnitude 2^63 = i64::MIN.unsigned_abs
        let neg_limit: u64 = 1u64 << 63; // = 9223372036854775808
        if v > neg_limit {
            return None;
        }
 // wrapping_neg handles the i64::MIN edge case without overflow
        Some((v as i64).wrapping_neg())
    } else {
        if v > i64::MAX as u64 {
            return None;
        }
        Some(v as i64)
    }
}

/// Strict conversion of a decimal byte string to `u64`.
/// Tries `string2ll` first (covers values ≤ `i64::MAX`). Falls back to a
/// manual digit-by-digit parser for values in `(i64::MAX, u64::MAX]`.
pub fn string2ull(s: &[u8]) -> Option<u64> {
    if let Some(ll) = string2ll(s) {
        return if ll < 0 { None } else { Some(ll as u64) };
    }
 // Fallback for values > i64::MAX (up to u64::MAX)
    if s.is_empty() || s[0] == b'-' {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        if !(b'0'..=b'9').contains(&c) {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
    }
    Some(v)
}

/// Strict conversion of a decimal byte string to `i64` (represents C `long`).
/// PORT NOTE: C `long` is 64-bit on the Valkey 64-bit target, so the result
/// type is `i64`. On ILP64 systems the range check is a no-op.
pub fn string2l(s: &[u8]) -> Option<i64> {
    let ll = string2ll(s)?;
 // C checks LONG_MIN / LONG_MAX; on 64-bit these equal i64::MIN / i64::MAX.
    Some(ll)
}

/// Determine whether a byte is in an inclusive ASCII range.
#[inline(always)]
fn safe_is_in_range(c: u8, start: u8, end: u8) -> bool {
    c >= start && c <= end
}

/// Classify a hex-digit byte: 0 = `'0'–'9'`, 1 = `'a'–'f'`, 2 = `'A'–'F'`, -1 = invalid.
fn base16_char_type(c: u8) -> i32 {
    if safe_is_in_range(c, b'0', b'9') {
        0
    } else if safe_is_in_range(c, b'a', b'f') {
        1
    } else if safe_is_in_range(c, b'A', b'F') {
        2
    } else {
        -1
    }
}

/// Async-signal-safe hexadecimal string → `usize` converter.
/// Parses hex digits (uppercase or lowercase) until a non-hex byte or
/// end of the slice. Returns `Err(` on overflow, `Ok(value)` on success.
/// unsigned long *result_output)`.
/// PORT NOTE: C returns 1 on success and -1 on overflow; translated
/// `Result<usize, >`. The `unsigned long` return type maps to `usize`.
pub fn string2ul_base16_async_signal_safe(src: &[u8]) -> Result<usize, ()> {
 // Lookup: subtract this from the raw byte to get its decimal value.
    static ASCII_TO_DEC: [u8; 3] = [b'0', b'a' - 10, b'A' - 10];

    let mut result: usize = 0;
    const BASE: usize = 16;

    for &byte in src {
        let char_type = base16_char_type(byte);
        if char_type == -1 {
            break;
        }
        let curr_val = (byte - ASCII_TO_DEC[char_type as usize]) as usize;
 // Overflow check mirroring the C: result > ULONG_MAX/base || result > (ULONG_MAX-curr_val)/base
        if result > usize::MAX / BASE || result > (usize::MAX - curr_val) / BASE {
            return Err(());
        }
        result = result * BASE + curr_val;
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// String → float conversion
// ---------------------------------------------------------------------------

/// Strict conversion of a byte string to `f64` (representing C `long double`).
/// TODO(port): C uses `long double` (80- or 128-bit extended precision on x86).
/// Rust only has `f64` (64-bit) stably. Results will differ for extreme values
/// near `LDBL_MAX` / `LDBL_MIN`. When `f128` stabilises, revisit.
/// TODO(port): Needs a `&[u8]` → float parser that avoids `str::from_utf8`.
/// Consider the `lexical` or `fast-float` crate (architect decision on dep).
pub fn string2ld(s: &[u8]) -> Option<f64> {
    if s.is_empty() || s.len() >= MAX_LONG_DOUBLE_CHARS {
        return None;
    }
    if s[0].is_ascii_whitespace() {
        return None;
    }
 // All valid float literals are pure ASCII; reject non-ASCII eagerly.
    if s.iter().any(|&b| b > 127) {
        return None;
    }
    // TODO(port): parse f64 from &[u8] without from_utf8; stub returns None.
 // Replace with `lexical::parse::<f64>(s).ok` or equivalent.
    None
}

/// Strict conversion of a byte string to `f64`.
/// TODO(port): C calls `valkey_strtod_n` (from `valkey_strtod.c`, crate
/// `redis-core/src/strtod.rs`, phase `defer`). Stub until that module lands.
pub fn string2d(s: &[u8]) -> Option<f64> {
    if s.is_empty() || s[0].is_ascii_whitespace() {
        return None;
    }
    if s.iter().any(|&b| b > 127) {
        return None;
    }
    // TODO(port): call strtod::valkey_strtod_n once strtod.rs is ported.
 // For now stub returns None; replace with lexical parse or strtod call.
    None
}

/// Return `Some(ll)` if `d` can be losslessly represented as `i64`.
pub fn double2ll(d: f64) -> Option<i64> {
 // Only meaningful when double has ≥ 52 mantissa bits and i64 is 64-bit.
 // The C #if guards the same invariant.
    if d < (i64::MIN / 2) as f64 || d > (i64::MAX / 2) as f64 {
        return None;
    }
    let ll = d as i64;
    if ll as f64 == d {
        Some(ll)
    } else {
        None
    }
}

/// Convert `value` to a compact decimal or special-value string in `buf`.
/// Writes `"nan"`, `"inf"`, `"-inf"`, `"-0"`, `"0"`, an integer string, or a
/// Grisu-formatted decimal. Returns the number of bytes written.
/// TODO(port): The general float path calls `fpconv_dtoa` from `fpconv_dtoa.h`
/// (not yet ported). Replaced with `format!` temporarily; Phase B should link
/// against a `ryu`-based implementation for bit-exact wire output.
pub fn d2string(buf: &mut [u8], value: f64) -> usize {
    let s: &[u8] = if value.is_nan() {
        b"nan"
    } else if value.is_infinite() {
        if value < 0.0 {
            b"-inf"
        } else {
            b"inf"
        }
    } else if value == 0.0 {
        if 1.0_f64 / value < 0.0 {
            b"-0"
        } else {
            b"0"
        }
    } else if let Some(ll) = double2ll(value) {
        let n = ll_to_string(buf, ll);
        return n;
    } else {
        // TODO(port): replace with fpconv_dtoa / ryu for wire-exact output.
 // Using Rust's default float formatting as a placeholder.
        let s = format!("{}", value);
        let bytes = s.as_bytes();
        let n = bytes.len().min(buf.len());
        buf[..n].copy_from_slice(&bytes[..n]);
        return n;
    };

    let n = s.len().min(buf.len());
    buf[..n].copy_from_slice(&s[..n]);
    n
}

/// Convert `dvalue` to a fixed-point decimal string with exactly
/// `fractional_digits` digits after the decimal point.
/// Returns the number of bytes written, or 0 on error (buffer too small or
/// `fractional_digits` out of range 1–17).
/// int fractional_digits)`.
pub fn fixedpoint_d2string(dst: &mut [u8], dvalue: f64, fractional_digits: i32) -> usize {
    let fd = fractional_digits as usize;
    if !(1..=17).contains(&fractional_digits) {
        if !dst.is_empty() {
            dst[0] = 0;
        }
        return 0;
    }
    if dst.len() < fd + 3 {
        if !dst.is_empty() {
            dst[0] = 0;
        }
        return 0;
    }

    if dvalue == 0.0 {
        dst[0] = b'0';
        dst[1] = b'.';
        for i in 0..fd {
            dst[2 + i] = b'0';
        }
        dst[fd + 2] = 0;
        return fd + 2;
    }

    static POWERS_OF_TEN: [f64; 18] = [
        1.0,
        10.0,
        100.0,
        1_000.0,
        10_000.0,
        100_000.0,
        1_000_000.0,
        10_000_000.0,
        100_000_000.0,
        1_000_000_000.0,
        10_000_000_000.0,
        100_000_000_000.0,
        1_000_000_000_000.0,
        10_000_000_000_000.0,
        100_000_000_000_000.0,
        1_000_000_000_000_000.0,
        10_000_000_000_000_000.0,
        100_000_000_000_000_000.0,
    ];

    let svalue = (dvalue * POWERS_OF_TEN[fd]).round() as i64;

    let negative = svalue < 0;
    let mut value: u64 = if negative {
        if svalue != i64::MIN {
            (-svalue) as u64
        } else {
            (i64::MAX as u64) + 1
        }
    } else {
        svalue as u64
    };

 // Offset into dst where the digit content begins (0 or 1 for the '-' sign).
    let sign_len: usize = if negative { 1 } else { 0 };

    let ndigits = digits10(value) as usize;
    let integer_digits = if (ndigits as i32) - fractional_digits < 1 {
        1usize
    } else {
        (ndigits as i32 - fractional_digits) as usize
    };

 // Total bytes written: sign + integer_digits + '.' + fractional_digits + '\0'
    let content_size = integer_digits + 1 + fd;
    if sign_len + content_size + 1 > dst.len() {
        if !dst.is_empty() {
            dst[0] = 0;
        }
        return 0;
    }

 // Write sign
    if negative {
        dst[0] = b'-';
    }

    let off = sign_len; // where digit content starts

 // If all digits are fractional, put a leading '0' in the integer part.
    if (ndigits as i32) - fractional_digits < 1 {
        dst[off] = b'0';
    }

    dst[off + integer_digits] = b'.';
 // Pre-fill fractional digits with '0'
    for i in 0..fd {
        dst[off + integer_digits + 1 + i] = b'0';
    }

    let mut next = off + content_size - 1;
    while value >= 100 {
        let i = ((value % 100) * 2) as usize;
        value /= 100;
        dst[next] = DIGITS[i + 1];
        dst[next - 1] = DIGITS[i];
        next = next.wrapping_sub(2);
 // Skip over the '.' position
        if next == off + integer_digits {
            next = next.wrapping_sub(1);
        }
    }

    if value < 10 {
        dst[next] = b'0' + value as u8;
    } else {
        let i = (value * 2) as usize;
        dst[next] = DIGITS[i + 1];
        dst[next - 1] = DIGITS[i];
    }

    dst[off + content_size] = 0;
    content_size + sign_len
}

/// Remove trailing zeros (and a bare trailing `.`) from a decimal string.
/// Modifies `buf` in-place and returns the new length.
pub fn trim_double_string(buf: &mut [u8], mut len: usize) -> usize {
    if buf[..len].contains(&b'.') {
        while len > 0 && buf[len - 1] == b'0' {
            len -= 1;
        }
        if len > 0 && buf[len - 1] == b'.' {
            len -= 1;
        }
    }
    buf[len] = 0;
    len
}

/// Convert a `f64` (representing `long double`) to a string in `buf`.
/// Returns the number of bytes written (excluding null terminator), or 0 on
/// error (buffer too small).
/// TODO(port): C uses `long double`; mapped to `f64`.  Precision differs for
/// extreme values. The `LD_STR_HEX` mode (`%La`) has no direct `f64`
/// equivalent; using `{:e}` as placeholder.
/// ld2string_mode mode)`.
pub fn ld2string(buf: &mut [u8], value: f64, mode: Ld2StringMode) -> usize {
    let cap = buf.len();
    if cap == 0 {
        return 0;
    }

    let formatted: Vec<u8> = if value.is_infinite() {
        if cap < 5 {
            buf[0] = 0;
            return 0;
        }
        if value > 0.0 {
            b"inf".to_vec()
        } else {
            b"-inf".to_vec()
        }
    } else if value.is_nan() {
        if cap < 4 {
            buf[0] = 0;
            return 0;
        }
        b"nan".to_vec()
    } else {
        match mode {
            Ld2StringMode::Auto => {
                // TODO(port): C uses %.17Lg; placeholder uses Rust default float format.
                format!("{:.17e}", value).into_bytes()
            }
            Ld2StringMode::Hex => {
                // TODO(port): C uses %La (hex float); no stable Rust equivalent.
                format!("{:e}", value).into_bytes()
            }
            Ld2StringMode::Human => {
 // %.17Lf with trailing-zero trim
                let mut s = format!("{:.17}", value).into_bytes();
                let mut l = s.len();
                if s.contains(&b'.') {
                    while l > 0 && s[l - 1] == b'0' {
                        l -= 1;
                    }
                    if l > 0 && s[l - 1] == b'.' {
                        l -= 1;
                    }
                }
                s.truncate(l);
 // Normalise "-0" to "0"
                if s == b"-0" {
                    s = b"0".to_vec();
                }
                s
            }
        }
    };

    if formatted.len() + 1 > cap {
        buf[0] = 0;
        return 0;
    }
    let n = formatted.len();
    buf[..n].copy_from_slice(&formatted);
    buf[n] = 0;
    n
}

// ---------------------------------------------------------------------------
// Hash / random
// ---------------------------------------------------------------------------

/// Populate `seed_array` with the first `outlen` bytes of `SHA256(value)`.
/// TODO(architect): requires sha2 crate.  Function body is a stub until the
/// dependency is wired (see top-of-file TODO(architect)).
/// const char *value)`.
pub fn get_hash_seed_from_string(seed_array: &mut [u8], value: &[u8]) {
    // TODO(architect): sha2 dependency needed; stub zeroes the output.
    let fill_len = seed_array.len().min(SHA256_BLOCK_SIZE).min(value.len());
    for b in seed_array[..fill_len].iter_mut() {
        *b = 0;
    }
}

/// Parse a `"major.minor.patch"` version string into a packed integer `0xMMmmpp`.
/// Returns `None` on parse error.
/// PORT NOTE: C returns -1 on error; translated to `None`.
pub fn version2num(version: &[u8]) -> Option<i32> {
    let mut v: i32 = 0;
    let mut part: i32 = 0;
    let mut numdots: i32 = 0;
    let mut iter = version.iter().peekable();

    loop {
        let &c = iter.next()?; // returns None if we reach the end before the loop exits
        if (b'0'..=b'9').contains(&c) {
            part = part * 10 + (c - b'0') as i32;
            if part > 255 {
                return None;
            }
        } else if c == b'.' {
            numdots += 1;
            if numdots > 2 {
                return None;
            }
            v = (v << 8) | part;
            part = 0;
        } else {
            return None;
        }

        if iter.peek().is_none() {
            break;
        }
    }

    if numdots != 2 {
        return None;
    }
    v = (v << 8) | part;
    Some(v)
}

/// Initialise the random seed from `/dev/urandom`, or fall back to a
/// time-/pid-based seed.
fn initialize_random_seed(state: &mut RandomState) {
    debug_assert!(!state.initialized, "seed already initialized");

    let mut file_ok = false;
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use io::Read as _;
        if f.read_exact(&mut state.seed).is_ok() {
            file_ok = true;
        }
    }

    if file_ok {
        state.initialized = true;
    } else {
 // Weak fallback: mix SystemTime + process id for each seed byte.
 // C mixes tv_sec ^ tv_usec ^ pid ^ (long)fp; we approximate.
        let pid = std::process::id() as u64;
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0u64, |d| d.as_nanos() as u64);
        for (i, b) in state.seed.iter_mut().enumerate() {
            *b = ((ts >> (i % 8 * 8)) ^ pid ^ i as u64) as u8;
        }
 // Not marking initialized so each call re-seeds (mirrors C behaviour).
    }
}

/// Set the 64-byte random seed from a 128-hexadecimal-digit string.
pub fn set_random_seed(seed_str: &[u8]) {
    debug_assert_eq!(seed_str.len(), 128, "seed_str must be 128 hex digits");
    // TODO(port): panic-free hex decoding; using manual byte-pair conversion.
    let mut state = RANDOM_STATE.lock().unwrap_or_else(|e| e.into_inner());
    for i in 0..64 {
        let hi = hex_nibble(seed_str[i * 2]);
        let lo = hex_nibble(seed_str[i * 2 + 1]);
        state.seed[i] = (hi << 4) | lo;
    }
    state.initialized = true;
}

/// Get the current 64-byte random seed as a 129-byte buffer (128 hex digits + `\0`).
pub fn get_random_seed(buf: &mut [u8]) {
    debug_assert_eq!(
        buf.len(),
        129,
        "buf must be 129 bytes (128 hex digits + NUL)"
    );
    let mut state = RANDOM_STATE.lock().unwrap_or_else(|e| e.into_inner());
    if !state.initialized {
        initialize_random_seed(&mut state);
    }
    static HEX: &[u8; 16] = b"0123456789ABCDEF";
    for i in 0..64 {
        buf[i * 2] = HEX[(state.seed[i] >> 4) as usize];
        buf[i * 2 + 1] = HEX[(state.seed[i] & 0x0f) as usize];
    }
    buf[128] = 0;
}

/// Fill `p` with cryptographically-adequate random bytes using a SHA256-HMAC
/// counter-mode generator seeded from `/dev/urandom`.
/// TODO(architect): SHA256 calls replaced with stubs; entire body is a
/// placeholder until the sha2 crate dep is wired.
pub fn get_random_bytes(p: &mut [u8]) {
    let mut state = RANDOM_STATE.lock().unwrap_or_else(|e| e.into_inner());
    if !state.initialized {
        initialize_random_seed(&mut state);
    }

    // TODO(architect): implement SHA256-HMAC counter mode once sha2 dep is wired.
 // two rounds of sha256 (IKEY xor 0x36, OKEY xor 0x5C)
 // For now fill with a trivial LCG so the stub compiles and produces bytes.
    for b in p.iter_mut() {
        state.counter = state
            .counter
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *b = (state.counter >> 33) as u8;
    }
}

/// Fill `p` with `len` random lowercase hex characters.
pub fn get_random_hex_chars(p: &mut [u8]) {
    get_random_bytes(p);
    static CHARSET: &[u8; 16] = b"0123456789abcdef";
    for b in p.iter_mut() {
        *b = CHARSET[(*b & 0x0f) as usize];
    }
}

// ---------------------------------------------------------------------------
// Filesystem utilities
// ---------------------------------------------------------------------------

/// Return the absolute path for `filename`, resolving leading `../` segments.
/// Returns `None` on failure (e.g., `getcwd` fails).
/// PORT NOTE: Uses `std::env::current_dir` and `PathBuf` instead of sds
/// manipulation; path bytes are preserved as `Vec<u8>` via `OsStr::as_encoded_bytes`.
pub fn get_absolute_path(filename: &[u8]) -> Option<Vec<u8>> {
 // Strip leading/trailing whitespace (\r\n\t\x20)
    let trimmed = trim_ascii(filename);

    if trimmed.first() == Some(&b'/') {
        return Some(trimmed.to_vec());
    }

    let cwd = std::env::current_dir().ok()?;
    let mut abspath = cwd.into_os_string().into_encoded_bytes();
    if abspath.last() != Some(&b'/') {
        abspath.push(b'/');
    }

    let mut relpath = trimmed.to_vec();

 // Strip leading "../" components, removing corresponding trailing path
 // components from `abspath`.
    while relpath.len() >= 3 && relpath.starts_with(b"../") {
        relpath.drain(..3);
        if abspath.len() > 1 {
 // Remove the last path component (up to and including the preceding '/')
 // e.g. "/foo/bar/" → "/foo/"
            let without_slash = &abspath[..abspath.len() - 1]; // drop trailing '/'
            let cut = without_slash.iter().rposition(|&b| b == b'/')? + 1;
            abspath.truncate(cut);
        }
    }

    abspath.extend_from_slice(&relpath);
    Some(abspath)
}

/// Return the UTC offset in seconds for the local timezone.
/// TODO(port): C uses the POSIX `timezone` global on Linux and
/// `gettimeofday(NULL, &tz)` on other platforms. Both require `libc` access.
/// For now, returns 0. Phase B should call `libc::timezone` or use
/// `chrono` / `time` crate.
pub fn get_time_zone() -> i64 {
    // TODO(port): libc::timezone or chrono/time crate needed.
    0
}

/// Return `true` if `path` contains no `/` or `\` characters.
pub fn path_is_base_name(path: &[u8]) -> bool {
    !path.contains(&b'/') && !path.contains(&b'\\')
}

/// Return `true` if `filename` exists and is a regular file.
pub fn file_exist(filename: &Path) -> bool {
    filename.is_file()
}

/// Return `true` if `dname` exists and is a directory.
pub fn dir_exists(dname: &Path) -> bool {
    dname.is_dir()
}

/// Create a directory at `dname` if it does not already exist.
/// Returns `Ok(` on success or if the directory already exists.
/// Returns `Err(io::Error)` if creation fails for another reason.
/// PORT NOTE: C returns -1 and sets `errno = ENOTDIR` if a non-directory
/// entry exists at the path; `std::fs::create_dir` returns an `Err` with
/// `ErrorKind::AlreadyExists` in that case, which callers should inspect.
pub fn dir_create_if_missing(dname: &Path) -> io::Result<()> {
    match std::fs::create_dir(dname) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            if dname.is_dir() {
                Ok(())
            } else {
                Err(io::Error::from_raw_os_error(libc_enotdir()))
            }
        }
        Err(e) => Err(e),
    }
}

/// Remove a directory and all its contents recursively.
/// PORT NOTE: `std::fs::remove_dir_all` provides the same semantics.
pub fn dir_remove(dname: &Path) -> io::Result<()> {
    std::fs::remove_dir_all(dname)
}

/// Concatenate `path` and `filename` with a `/` separator.
pub fn make_path(path: &[u8], filename: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(path.len() + 1 + filename.len());
    result.extend_from_slice(path);
    result.push(b'/');
    result.extend_from_slice(filename);
    result
}

/// `fsync` the directory that contains `filename`.
/// This is step 5 of the safe atomic-file-overwrite pattern (create temp →
/// write → fsync → rename → fsync dir).
pub fn fsync_file_dir(filename: &Path) -> io::Result<()> {
    let parent = filename.parent().unwrap_or(Path::new("."));
    let dir = std::fs::File::open(parent)?;
 // Some OS (e.g. macOS) return EINVAL when fsync-ing a directory; treat as OK.
    match dir.sync_all() {
        Ok(()) => Ok(()),
        Err(e) => match e.raw_os_error() {
            Some(LIBC_EBADF) | Some(LIBC_EINVAL) => Ok(()),
            _ => Err(e),
        },
    }
}

/// Drop OS page-cache pages backing `[offset, offset+length)` in `fd`.
/// No-op on platforms without `posix_fadvise`.
/// TODO(port): `posix_fadvise(POSIX_FADV_DONTNEED)` requires `libc`; stub
/// returns `Ok(` for now.
pub fn reclaim_file_page_cache(_fd: i32, _offset: u64, _length: u64) -> io::Result<()> {
    // TODO(port): call libc::posix_fadvise(fd, offset, length, POSIX_FADV_DONTNEED)
 // when the libc dep is confirmed available in redis-core.
    Ok(())
}

// ---------------------------------------------------------------------------
// Async-signal-safe I/O helpers
// ---------------------------------------------------------------------------

/// Async-signal-safe `fgets`-equivalent: read one line from a raw file descriptor.
/// Reads until `'\n'`, EOF, or `buf.len - 1` bytes consumed.
/// Returns `Some(bytes_read)` or `None` on EOF/error with no bytes read.
/// PORT NOTE: C takes a raw fd (`int`); translated to `std::os::unix::io::RawFd`.
/// Not actually signal-safe in the Rust runtime sense; the name is preserved
/// for API compatibility.
#[cfg(unix)]
pub fn fgets_async_signal_safe(buf: &mut [u8], fd: std::os::unix::io::RawFd) -> Option<usize> {
    let _ = (buf, fd);
    // TODO(architect): FromRawFd::from_raw_fd requires unsafe; this function
 // cannot be implemented in a pilot crate under the unsafe budget.
 // Options: (a) raise redis-core ceiling to 1 for this fd wrapper,
 // (b) use a safe fd-reading crate (e.g. rustix), or (c) refactor callers
 // to pass a &mut dyn Read instead of a raw fd.
    None
}

/// Write a `u64` in the given base (10 or 16) as ASCII into `out[..n]`.
#[allow(dead_code)]
/// Returns `n`, the number of bytes written (written at `out[0..n]`, left-aligned).
/// The C version fills right-to-left from the end of a
/// caller-provided stack buffer. Here we use a local temp buffer and copy
/// left-to-right into `out`, eliminating the pointer-arithmetic lifetime issues.
/// PORT NOTE: Return convention changed from a pointer into the buffer to a
/// length; callers use `&out[..n]` for the result slice.
fn u2string_signal_safe(base: u32, mut val: u64, out: &mut [u8]) -> usize {
    static HEX: &[u8; 16] = b"0123456789abcdef";
    let mut tmp = [0u8; 22]; // enough for base-10 u64 + sign
    let mut i = tmp.len();
    loop {
        i -= 1;
        tmp[i] = HEX[(val % base as u64) as usize];
        val /= base as u64;
        if val == 0 {
            break;
        }
    }
    let digits = &tmp[i..];
    let n = digits.len().min(out.len());
    out[..n].copy_from_slice(&digits[..n]);
    n
}

/// Write an `i64` in the given base (10 or 16) as ASCII into `out[..n]`.
#[allow(dead_code)]
/// Returns `n`, the number of bytes written.
/// Two's-complement hex for negative base-16 values;
/// minus-prefix for base-10.
/// TODO(port): The C version pads to 16 hex digits for negative base-16 values
/// and inverts each nibble (one's-complement trick). The Rust version uses
/// wrapping two's-complement cast which produces the correct bit pattern but
/// does not pad to a fixed width. Revisit if callers depend on fixed width.
fn i2string_signal_safe(base: u32, val: i64, out: &mut [u8]) -> usize {
    if val >= 0 {
        return u2string_signal_safe(base, val as u64, out);
    }
    if base == 10 {
 // Minus-prefix then absolute value
        if out.is_empty() {
            return 0;
        }
        out[0] = b'-';
        let abs_val: u64 = if val != i64::MIN {
            (-val) as u64
        } else {
            (i64::MAX as u64) + 1
        };
        let n = u2string_signal_safe(10, abs_val, &mut out[1..]);
        return 1 + n;
    }
 // Base 16: write the two's-complement 64-bit pattern (all 16 nibbles).
    // TODO(port): C inverts digits after computing abs value; we cast directly.
    u2string_signal_safe(16, val as u64, out)
}

/// Async-signal-safe `vsnprintf`-like formatter supporting `%d`, `%i`, `%u`,
/// `%x`, `%p`, `%s` with optional `l`/`ll` length modifiers.
/// const char *format, va_list ap)`.
/// TODO(port): C uses `va_list`; Rust has no stable equivalent.  This function
/// is provided as a skeleton; callers must be adapted. The body is a stub
/// that always returns 0.
pub fn vsnprintf_async_signal_safe(
    _to: &mut [u8],
    _format: &[u8],
    // TODO(port): no va_list equivalent — callers must be refactored.
) -> usize {
    // TODO(port): implement or replace with a format! wrapper once callers are
 // identified and ported.
    0
}

/// Async-signal-safe `snprintf`-like formatter.
/// TODO(port): delegates to `vsnprintf_async_signal_safe`; va_list not supported.
pub fn snprintf_async_signal_safe(_to: &mut [u8], _fmt: &[u8]) -> usize {
    // TODO(port): va_list not available; stub always returns 0.
    0
}

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

/// Return the current UNIX timestamp in microseconds.
/// PERF(port): C has a fast path using a hardware monotonic clock via
/// `getMonotonicUs`. This version always
/// calls `SystemTime::now`. Revisit in Phase B once `monotonic.rs` lands.
pub fn ustime() -> UsTime {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

/// Return the current UNIX timestamp in milliseconds.
pub fn mstime() -> MsTime {
    ustime() / 1_000
}

// ---------------------------------------------------------------------------
// Miscellaneous
// ---------------------------------------------------------------------------

/// Write the address of a pointer into an 8-byte field, zero-padding on
/// 32-bit targets.
/// TODO(architect): writing a raw pointer address requires `unsafe`.  This
/// function cannot be implemented without `unsafe` in a pilot crate. Phase B
/// decision: either gate behind `#[cfg(target_pointer_width = "64")]` and use
/// a `u64` directly, or escalate for an `unsafe` budget exception.
pub fn write_pointer_with_padding(_buf: &mut [u8; 8], _ptr_addr: u64) {
    // TODO(architect): raw pointer address serialisation needs unsafe or a
 // redesign. Current stub is a no-op.
}

/// Escape a byte slice as a JSON string, appending the result (with surrounding
/// `"`) to `s`.
pub fn escape_json_string(mut s: Vec<u8>, p: &[u8]) -> Vec<u8> {
    s.push(b'"');
    for &c in p {
        match c {
            b'\\' | b'"' => {
                s.push(b'\\');
                s.push(c);
            }
            b'\n' => s.extend_from_slice(b"\\n"),
            b'\x0c' => s.extend_from_slice(b"\\f"), // '\f'
            b'\r' => s.extend_from_slice(b"\\r"),
            b'\t' => s.extend_from_slice(b"\\t"),
            b'\x08' => s.extend_from_slice(b"\\b"), // '\b'
            c if c <= 0x1f => {
 // \uXXXX escape for control chars
                let hex = format!("\\u{:04x}", c);
                s.extend_from_slice(hex.as_bytes());
            }
            c => s.push(c),
        }
    }
    s.push(b'"');
    s
}

/// Tomas Wang's 64-bit integer hash.
pub fn wang_hash64(mut hash: u64) -> u64 {
    hash = (!hash).wrapping_add(hash << 21);
    hash ^= hash >> 24;
    hash = hash.wrapping_add(hash << 3).wrapping_add(hash << 8);
    hash ^= hash >> 14;
    hash = hash.wrapping_add(hash << 2).wrapping_add(hash << 4);
    hash ^= hash >> 28;
    hash = hash.wrapping_add(hash << 31);
    hash
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Trim leading/trailing ASCII whitespace from a byte slice.
fn trim_ascii(s: &[u8]) -> &[u8] {
    let start = s
        .iter()
        .position(|&b| !b.is_ascii_whitespace())
        .unwrap_or(s.len());
    let end = s
        .iter()
        .rposition(|&b| !b.is_ascii_whitespace())
        .map_or(0, |i| i + 1);
    &s[start..end.max(start)]
}

/// Decode a single ASCII hex nibble (panics in debug if invalid; safe in release).
fn hex_nibble(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

/// Helper: return ENOTDIR errno value portably.
fn libc_enotdir() -> i32 {
    // TODO(port): replace with libc::ENOTDIR when libc dep is confirmed.
    20 // POSIX ENOTDIR = 20 on Linux and macOS
}

/// Stub constants for errno values used in `fsync_file_dir`.
/// TODO(port): replace with libc::EBADF / libc::EINVAL once libc dep is wired.
const LIBC_EBADF: i32 = 9; // POSIX EBADF
const LIBC_EINVAL: i32 = 22; // POSIX EINVAL

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         35
//   port_notes:    9
//   unsafe_blocks: 0   (fgets_async_signal_safe stubbed — see TODO(architect))
//   notes: |
//     - SHA256 stubs (get_hash_seed_from_string, get_random_bytes) need sha2
//       crate wired — see TODO(architect) at top.
//     - string2ld / string2d stub (None) — need &[u8] float parser without
//       from_utf8 (lexical / fast-float crate, architect decision).
//     - d2string general float path uses format! placeholder; replace with
//       ryu/fpconv_dtoa for wire-exact output in Phase B.
//     - ld2string maps long double → f64; precision differs for extreme values.
//     - vsnprintf_async_signal_safe / snprintf_async_signal_safe: va_list has
//       no Rust equivalent; callers must be refactored in Phase B.
//     - write_pointer_with_padding: needs unsafe; escalated to architect.
//     - ustime fast-path (getMonotonicUs) deferred until monotonic.rs lands.
//     - fgets_async_signal_safe: stubbed with None — FromRawFd requires unsafe
//       which exceeds pilot crate budget.  C reads fd byte-by-byte; Rust returns
//       None until Phase B resolution.  Tracked in needs_architect.txt.
//     - get_time_zone: libc::timezone not accessible without unsafe; stub 0.
//     - string2ll scalar only; AVX-512 SIMD path intentionally omitted.
// ──────────────────────────────────────────────────────────────────────────
