//! `CommandContext` — the contract every command implementation works against.
//!
//! Per PORTING.md §2 #5: bundles `&mut Client`, parsed args, and reply
//! writer helpers. Returns `Result<(), RedisError>`. NOT the C `client *c`
//! parameter — commands never touch the raw connection or buffer-list.
//!
//! `RedisServer` reference comes via the orchestrator (Phase 3 architect
//! packet adds it).

use crate::client::Client;
use crate::db::RedisDb;
use crate::object::RedisObject;
use redis_protocol::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};

/// Bundle of context every command receives. Wraps a mutable Client and
/// exposes argument access + reply-writer methods.
///
/// PORT NOTE: `db` is an owned `RedisDb` in this stub. Phase 3 will replace
/// it with `&'a mut RedisServer` (and `db()` will route through the server's
/// db list keyed by `client.db_index`).
pub struct CommandContext<'a> {
    pub client: &'a mut Client,
    /// Per-context DB scratch. STUB — Phase 3 replaces with server-owned dbs.
    pub stub_db: RedisDb,
}

/// Flexible reply-array length argument.
///
/// Translated callers pass `usize`, `i64`, and `i32` interchangeably; this
/// trait normalises them to `i64` for the underlying writer. Phase 3 may
/// tighten this once we settle on a single int type for protocol sizes.
pub trait ReplyArrayLen {
    fn into_reply_len(self) -> i64;
}

impl ReplyArrayLen for i64 {
    fn into_reply_len(self) -> i64 { self }
}
impl ReplyArrayLen for usize {
    fn into_reply_len(self) -> i64 { self as i64 }
}
impl ReplyArrayLen for i32 {
    fn into_reply_len(self) -> i64 { self as i64 }
}

/// Flexible argv-index trait. Translated code mixes `usize`, `i32`, and
/// arithmetic on `i64` for indexing into `client.argv`.
pub trait ArgIndex {
    fn into_arg_index(self) -> RedisResult<usize>;
}

impl ArgIndex for usize {
    fn into_arg_index(self) -> RedisResult<usize> { Ok(self) }
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

impl<'a> CommandContext<'a> {
    pub fn new(client: &'a mut Client) -> Self {
        Self { client, stub_db: RedisDb::new(0) }
    }

    // ── Args ──────────────────────────────────────────────────────

    pub fn arg(&self, i: usize) -> RedisResult<&RedisString> {
        self.client
            .arg(i)
            .ok_or_else(|| RedisError::wrong_number_of_args(self.command_name()))
    }

    pub fn arg_count(&self) -> usize {
        self.client.arg_count()
    }

    /// Arg 0 is the command name (uppercase by Redis convention).
    pub fn command_name(&self) -> &[u8] {
        self.client
            .arg(0)
            .map(|s| s.as_bytes())
            .unwrap_or(b"<unknown>")
    }

    // ── Reply writers ─────────────────────────────────────────────

    pub fn reply_simple_string(&mut self, bytes: &[u8]) -> RedisResult<()> {
        self.client
            .write_frame(&RespFrame::Simple(RedisString::from_bytes(bytes)));
        Ok(())
    }

    pub fn reply_bulk(&mut self, bytes: &[u8]) -> RedisResult<()> {
        self.client
            .write_frame(&RespFrame::Bulk(Some(RedisString::from_bytes(bytes))));
        Ok(())
    }

    pub fn reply_bulk_string(&mut self, s: RedisString) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Bulk(Some(s)));
        Ok(())
    }

    pub fn reply_null_bulk(&mut self) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Bulk(None));
        Ok(())
    }

    pub fn reply_integer(&mut self, n: i64) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Integer(n));
        Ok(())
    }

    pub fn reply_array_header<L: ReplyArrayLen>(&mut self, len: L) -> RedisResult<()> {
        self.reply_array_header_i64(len.into_reply_len())
    }

    pub fn reply_null_array(&mut self) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Array(None));
        Ok(())
    }

    pub fn reply_frame(&mut self, frame: &RespFrame) -> RedisResult<()> {
        self.client.write_frame(frame);
        Ok(())
    }

    /// Reply with a RedisError. Equivalent of C's addReplyError* family.
    /// The error becomes a `-...` RESP error line; doesn't return Err.
    pub fn reply_error(&mut self, err: &RedisError) -> RedisResult<()> {
        self.client
            .write_frame(&RespFrame::Error(err.to_resp_payload()));
        Ok(())
    }

    // ── Phase-B stubs needed by translated command code ────────────

    /// Argument count, C-style (alias of `arg_count`).
    pub fn argc(&self) -> usize {
        self.client.arg_count()
    }

    /// Owned-copy argv accessor.
    ///
    /// Returns a cloned `RedisString` for the given index. Translated code
    /// uses this where it wants to retain a copy across borrows of `ctx`.
    pub fn arg_owned<I: ArgIndex>(&self, i: I) -> RedisResult<RedisString> {
        let idx = i.into_arg_index()?;
        self.client
            .arg(idx)
            .cloned()
            .ok_or_else(|| RedisError::wrong_number_of_args(self.command_name()))
    }

    /// Argv accessor returning a `RedisObject::String` wrapper.
    ///
    /// STUB — Phase B placeholder mapping a raw argv `RedisString` into the
    /// `RedisObject::String` variant. Eventually arguments will already be
    /// `RedisObject`-typed once shared-object interning lands.
    pub fn arg_as_object<I: ArgIndex>(&self, i: I) -> RedisResult<RedisObject> {
        let s = self.arg_owned(i)?;
        Ok(RedisObject::String(s))
    }

    /// Null bulk reply (alias of `reply_null_bulk`).
    pub fn reply_null(&mut self) -> RedisResult<()> {
        self.reply_null_bulk()
    }

    /// Push or array header — RESP3 push frame in client RESP3 mode,
    /// fall back to RESP2 array header otherwise.
    ///
    /// STUB — Phase B emits an array header regardless of protocol mode.
    /// Full RESP3 push-frame support lands when networking is ported.
    pub fn reply_push_or_array_header<L: ReplyArrayLen>(
        &mut self,
        len: L,
    ) -> RedisResult<()> {
        self.reply_array_header_i64(len.into_reply_len())
    }

    fn reply_array_header_i64(&mut self, len: i64) -> RedisResult<()> {
        let mut buf = Vec::new();
        buf.push(b'*');
        use std::io::Write;
        let _ = write!(buf, "{}", len);
        buf.extend_from_slice(b"\r\n");
        self.client.reply_buf.extend_from_slice(&buf);
        Ok(())
    }

    /// Per-context database. STUB — Phase 3 routes through the server.
    pub fn db(&self) -> &RedisDb {
        &self.stub_db
    }

    /// Mutable view of the per-context database. STUB — Phase 3 routes through
    /// the server keyed by `client.db_index`.
    pub fn db_mut(&mut self) -> &mut RedisDb {
        &mut self.stub_db
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_with_args(args: &[&[u8]]) -> (Client, ) {
        let mut c = Client::new(1);
        c.set_args(args.iter().map(|s| RedisString::from_bytes(s)).collect());
        (c,)
    }

    #[test]
    fn arg_access_returns_err_when_oob() {
        let (mut c,) = ctx_with_args(&[b"SET", b"foo"]);
        let ctx = CommandContext::new(&mut c);
        assert!(ctx.arg(0).is_ok());
        assert!(ctx.arg(1).is_ok());
        let err = ctx.arg(2).unwrap_err();
        assert!(matches!(err, RedisError::WrongNumberOfArgs(_)));
    }

    #[test]
    fn reply_simple_string_writes_resp() {
        let (mut c,) = ctx_with_args(&[b"PING"]);
        let mut ctx = CommandContext::new(&mut c);
        ctx.reply_simple_string(b"PONG").unwrap();
        assert_eq!(c.drain_reply(), b"+PONG\r\n");
    }

    #[test]
    fn reply_array_header_emits_correct_prefix() {
        let (mut c,) = ctx_with_args(&[]);
        let mut ctx = CommandContext::new(&mut c);
        ctx.reply_array_header(3).unwrap();
        ctx.reply_integer(1).unwrap();
        ctx.reply_integer(2).unwrap();
        ctx.reply_integer(3).unwrap();
        assert_eq!(c.drain_reply(), b"*3\r\n:1\r\n:2\r\n:3\r\n");
    }

    #[test]
    fn reply_error_emits_error_line() {
        let (mut c,) = ctx_with_args(&[]);
        let mut ctx = CommandContext::new(&mut c);
        ctx.reply_error(&RedisError::wrong_type()).unwrap();
        assert_eq!(
            c.drain_reply(),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (PORTING.md §2 #5 + §4.5 reply mapping)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         1
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Reply writer + arg access. RedisServer reference deferred to Phase 3.
// ──────────────────────────────────────────────────────────────────────────
