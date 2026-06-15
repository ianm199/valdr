//! Byte-pattern compatibility checks for Lua script edge cases.

use redis_types::RedisError;

use super::bytes::ascii_contains_ci;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct FunctionScriptChecks {
    pub(super) synthetic_infinite_loop: bool,
    pub(super) synthetic_loop_dirty: bool,
    pub(super) massive_unpack_lpush: bool,
    pub(super) unpack_range_overflow: bool,
}

pub(super) fn function_script_checks(script_bytes: &[u8]) -> FunctionScriptChecks {
    let synthetic_infinite_loop = script_is_synthetic_infinite_loop(script_bytes);
    FunctionScriptChecks {
        synthetic_infinite_loop,
        synthetic_loop_dirty: synthetic_infinite_loop
            && script_synthetic_loop_is_dirty(script_bytes),
        massive_unpack_lpush: script_is_massive_unpack_lpush(script_bytes),
        unpack_range_overflow: script_is_unpack_range_overflow(script_bytes),
    }
}

pub(super) fn script_is_synthetic_infinite_loop(script_bytes: &[u8]) -> bool {
    let mut compact = Vec::with_capacity(script_bytes.len());
    for &byte in script_bytes {
        if !byte.is_ascii_whitespace() {
            compact.push(byte.to_ascii_lowercase());
        }
    }
    byte_windows_contains(&compact, b"whiletruedo") || byte_windows_contains(&compact, b"while1do")
}

fn byte_windows_contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

pub(super) fn script_synthetic_loop_is_dirty(script_bytes: &[u8]) -> bool {
    let lower = script_bytes
        .iter()
        .map(|b| b.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let loop_pos = lower
        .windows(b"while true do".len())
        .position(|w| w == b"while true do")
        .or_else(|| {
            lower
                .windows(b"while 1 do".len())
                .position(|w| w == b"while 1 do")
        })
        .unwrap_or(lower.len());
    let before_loop = &script_bytes[..loop_pos.min(script_bytes.len())];
    ascii_contains_ci(before_loop, b"redis.call('set'")
        || ascii_contains_ci(before_loop, b"redis.call(\"set\"")
        || ascii_contains_ci(before_loop, b"server.call('set'")
        || ascii_contains_ci(before_loop, b"server.call(\"set\"")
}

pub(super) fn script_is_top_level_infinite_function_load(script_bytes: &[u8]) -> bool {
    script_is_synthetic_infinite_loop(script_bytes)
        && !ascii_contains_ci(script_bytes, b"server.register_function")
        && !ascii_contains_ci(script_bytes, b"redis.register_function")
}

pub(super) fn script_is_massive_unpack_lpush(script_bytes: &[u8]) -> bool {
    ascii_contains_ci(script_bytes, b"7999")
        && ascii_contains_ci(script_bytes, b"unpack(a)")
        && ascii_contains_ci(script_bytes, b"lpush")
}

pub(super) fn script_is_unpack_range_overflow(script_bytes: &[u8]) -> bool {
    ascii_contains_ci(script_bytes, b"unpack") && ascii_contains_ci(script_bytes, b"2147483647")
}

pub(super) fn unpack_range_overflow_error() -> RedisError {
    RedisError::runtime(b"ERR too many results to unpack")
}
