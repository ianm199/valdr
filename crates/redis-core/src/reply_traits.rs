//! Reply-adapter traits used by `CommandContext` reply builders.
//! Extracted from `command_context.rs` by refactor/file-structure-splits.
//! These trait + impl blocks define how `i64/usize/i32` widen into
//! reply-array-length argument, how `RedisError/&[u8]/&[u8; N]` adapt
//! into error-reply payloads, and how `usize/i64/i32` index `argv`.

use redis_types::{RedisError, RedisResult, RedisString};

pub trait ReplyErrorArg {
    fn into_reply_error_payload(self) -> RedisString;
}

impl ReplyErrorArg for &RedisError {
    fn into_reply_error_payload(self) -> RedisString {
        self.to_resp_payload()
    }
}

impl ReplyErrorArg for &[u8] {
    fn into_reply_error_payload(self) -> RedisString {
        RedisString::from_bytes(self)
    }
}

impl<const N: usize> ReplyErrorArg for &[u8; N] {
    fn into_reply_error_payload(self) -> RedisString {
        RedisString::from_bytes(self)
    }
}

/// Flexible reply-array length argument.
/// Translated callers pass `usize`, `i64`, and `i32` interchangeably; this
/// trait normalises them to `i64` for the underlying writer. Phase 3 may
/// tighten this once we settle on a single int type for protocol sizes.
pub trait ReplyArrayLen {
    fn into_reply_len(self) -> i64;
}

impl ReplyArrayLen for i64 {
    fn into_reply_len(self) -> i64 {
        self
    }
}
impl ReplyArrayLen for usize {
    fn into_reply_len(self) -> i64 {
        self as i64
    }
}
impl ReplyArrayLen for i32 {
    fn into_reply_len(self) -> i64 {
        self as i64
    }
}

/// Flexible argv-index trait. Translated code mixes `usize`, `i32`,
/// arithmetic on `i64` for indexing into `client.argv`.
pub trait ArgIndex {
    fn into_arg_index(self) -> RedisResult<usize>;
}

impl ArgIndex for usize {
    fn into_arg_index(self) -> RedisResult<usize> {
        Ok(self)
    }
}
impl ArgIndex for i64 {
    fn into_arg_index(self) -> RedisResult<usize> {
        usize::try_from(self).map_err(|_| RedisError::runtime(b"argv index out of range"))
    }
}
impl ArgIndex for i32 {
    fn into_arg_index(self) -> RedisResult<usize> {
        usize::try_from(self).map_err(|_| RedisError::runtime(b"argv index out of range"))
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        extracted from command_context.rs (refactor/file-structure-splits)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         ReplyErrorArg + ReplyArrayLen + ArgIndex adapters.
//                  Re-exported from command_context.rs to preserve the public path.
// ──────────────────────────────────────────────────────────────────────────
