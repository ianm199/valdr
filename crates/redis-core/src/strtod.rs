//! Fast decimal floating-point parsing for Valkey byte strings.
//! The C implementation uses
//! — a single-header C99 port of the Rust `fast_float` library by
//! Koleman Nix. This translation uses the original Rust `fast_float` crate
//! directly, which is the upstream source that was.
//! # Parsing options (mirrors `valkey_strtod_options` in C)
//! The C code configures `ffc` with:
//! - `FFC_PRESET_GENERAL` = `FFC_FORMAT_FLAG_FIXED | FFC_FORMAT_FLAG_SCIENTIFIC`
//! (accepts both `123.456` and `1.23e4` notation)
//! - `FFC_FORMAT_FLAG_ALLOW_LEADING_PLUS` (accepts `+1.5`)
//! - Decimal point: `'.'`
//! - No `FFC_FORMAT_FLAG_NO_INFNAN`, so `inf`/`nan` literals are accepted.
//! # C vs Rust API mapping
//! | C function | Rust equivalent |
//! |----------------------|-----------------------------------------------|
//! | `valkey_strtod` | `strtod(input: &[u8])` |
//! | `valkey_strtod_n` | `strtod(input: &[u8])` (same — slice carries length) |
//! | `valkey_strtod_sds` | `strtod_sds(input: &RedisString)` |
//! The `endptr` output-parameter pattern from C is replaced by returning
//! number of bytes consumed as the second element of the `Ok` tuple.

// TODO(architect): add `fast-float = "0.2"` to redis-core Cargo.toml dependencies.
// The `fast_float` crate is the Rust original; is a C port of it.

use redis_types::error::RedisError;
use redis_types::string::RedisString;

/// Parse a `f64` from a byte slice.
/// Returns `(value, bytes_consumed)` on success. `bytes_consumed` is
/// number of bytes from `input` that were read — the Rust replacement for
/// C `endptr` pattern. The caller can obtain the remaining slice with
/// `&input[bytes_consumed..]`.
/// Accepts fixed (`123.456`), scientific (`1.23e4`), leading-plus (`+1.5`),
/// `inf`, `-inf`, and `nan` (all case-insensitive), mirroring
/// `valkey_strtod_options` in C.
/// # Errors
/// - [`RedisError::not_float`] — input is not a recognisable number literal
/// (maps to C `errno = EINVAL` / `FFC_OUTCOME_INVALID_INPUT`).
/// - [`RedisError::out_of_range`] — the value overflows `f64` range (maps
/// C `errno = ERANGE` / `FFC_OUTCOME_OUT_OF_RANGE`).
/// # PORT NOTE
/// In C, `valkey_strtod` takes a NUL-terminated `const char *` and calls
/// `strlen` internally, while `valkey_strtod_n` takes `(ptr, len)`. Both are
/// collapsed into this single function because `&[u8]` already carries its
/// length, making the distinction meaningless in Rust.
pub fn strtod(input: &[u8]) -> Result<(f64, usize), RedisError> {
    // TODO(port): fast_float::parse_partial cannot currently distinguish a
 // literal "inf" input (FFC_OUTCOME_OK in C) from a finite value that
 // overflows to INFINITY (FFC_OUTCOME_OUT_OF_RANGE in C), because both
 // return Ok(f64::INFINITY). The helper below uses a byte-prefix heuristic
 // to recover the distinction. This should be validated in Phase B against
 // the wire-diff oracle with overflow inputs like "1e9999".
    match fast_float::parse_partial::<f64, &[u8]>(input) {
        Err(_) => Err(RedisError::not_float()),
        Ok((value, n)) => {
            if value.is_infinite() && !starts_with_inf_literal(input) {
                Err(RedisError::out_of_range())
            } else {
                Ok((value, n))
            }
        }
    }
}

/// Parse a `f64` from a `RedisString`.
/// Convenience wrapper around [`strtod`] that passes the full string bytes.
/// Mirrors `valkey_strtod_sds` in C, which calls `valkey_strtod_n(str,
/// sdslen(str), endptr)`.
pub fn strtod_sds(input: &RedisString) -> Result<(f64, usize), RedisError> {
    strtod(input.as_bytes())
}

/// Returns `true` if `input` begins with an `inf` or `infinity` literal
/// (optionally preceded by `+` or `-`, all case-insensitive).
/// Used to distinguish a genuine infinity literal from an out-of-range finite
/// value that `fast_float` also maps to `f64::INFINITY`.
/// Mirrors the set of infinity prefixes accepted by `FFC_PRESET_GENERAL`
/// (no `FFC_FORMAT_FLAG_NO_INFNAN` flag was set).
/// PORT NOTE: `FFC_PRESET_GENERAL` does not define exactly which infinity
/// spellings are accepted; this mirrors fast_float's Rust behaviour which
/// accepts "inf" and "infinity" (case-insensitive), with an optional leading
/// sign.
fn starts_with_inf_literal(input: &[u8]) -> bool {
    let body = match input {
        [b'+', rest @ ..] | [b'-', rest @ ..] => rest,
        other => other,
    };
    body.len() >= 3
        && body[0].eq_ignore_ascii_case(&b'i')
        && body[1].eq_ignore_ascii_case(&b'n')
        && body[2].eq_ignore_ascii_case(&b'f')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_integer() {
        let (v, n) = strtod(b"42").unwrap();
        assert_eq!(v, 42.0_f64);
        assert_eq!(n, 2);
    }

    #[test]
    fn parses_scientific() {
        let (v, n) = strtod(b"1.5e2").unwrap();
        assert_eq!(v, 150.0_f64);
        assert_eq!(n, 5);
    }

    #[test]
    fn parses_leading_plus() {
        let (v, n) = strtod(b"+3.14").unwrap();
        assert!((v - 3.14_f64).abs() < 1e-10);
        assert_eq!(n, 5);
    }

    #[test]
    fn parses_inf_literal() {
        let (v, n) = strtod(b"inf").unwrap();
        assert!(v.is_infinite() && v.is_sign_positive());
        assert_eq!(n, 3);
    }

    #[test]
    fn parses_negative_inf() {
        let (v, _n) = strtod(b"-inf").unwrap();
        assert!(v.is_infinite() && v.is_sign_negative());
    }

    #[test]
    fn parses_nan() {
        let (v, _n) = strtod(b"nan").unwrap();
        assert!(v.is_nan());
    }

    #[test]
    fn error_on_invalid() {
        assert!(matches!(strtod(b"abc"), Err(RedisError::NotFloat)));
    }

    #[test]
    fn error_on_overflow() {
        assert!(matches!(strtod(b"1e9999"), Err(RedisError::OutOfRange)));
    }

    #[test]
    fn partial_parse_stops_at_trailing() {
        let (v, n) = strtod(b"1.5 remaining").unwrap();
        assert_eq!(v, 1.5_f64);
        assert_eq!(n, 3);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//                  src/valkey_strtod.h  (merged — declares the same 3 fns)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         2
//   port_notes:    2
//   unsafe_blocks: 0
//   notes:         valkey_strtod and valkey_strtod_n collapse into one Rust
//                  function (strtod) because &[u8] carries its own length.
//                  fast_float crate must be added to redis-core Cargo.toml.
//                  The ERANGE vs EINVAL distinction for overflow relies on a
//                  byte-prefix heuristic (starts_with_inf_literal); validate
//                  in Phase B with the wire-diff oracle using values like
//                  "1e9999" and the literal strings "inf"/"infinity".
// ──────────────────────────────────────────────────────────────────────────
