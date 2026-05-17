//! Command dispatch table — maps argv[0] (case-insensitive) to a handler fn.
//!
//! Wave A wires up the *lookup* side only. Most handler bodies are still
//! `todo!()`; this module just routes the call. Handler bodies land in Waves
//! B/C/D.
//!
//! Two-layer lookup:
//!
//! 1. The generated registry in `generated::COMMANDS` is the source of truth
//!    for command metadata (arity, flags, ACL category).
//! 2. A small static `HANDLERS` table maps an uppercase ASCII command name to
//!    a Rust function. Commands with no handler yet are intentionally absent;
//!    callers receive an `unknown command` error.

use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult, RedisString};

/// A command handler.
pub type Handler = fn(&mut CommandContext) -> RedisResult<()>;

/// One entry in the static dispatch table.
pub struct DispatchEntry {
    /// Uppercase ASCII name (e.g. `b"PING"`). Compared case-insensitively.
    pub name: &'static [u8],
    /// Handler function pointer.
    pub handler: Handler,
}

/// Look up the handler for `name` (case-insensitive ASCII).
///
/// Returns `Some(entry)` if a handler is registered, `None` otherwise.
pub fn lookup_command(name: &[u8]) -> Option<&'static DispatchEntry> {
    HANDLERS
        .iter()
        .find(|entry| ascii_eq_ignore_case(entry.name, name))
}

/// Dispatch one command using `ctx.client.argv[0]` as the command name.
///
/// Returns an error if argv is empty or the command is unknown. The handler's
/// result is returned verbatim — handlers may write a reply *and* return `Ok`,
/// or return `Err` (which the I/O layer renders as a `-ERR ...` reply).
///
/// When the client is inside a MULTI block (`client.flag_multi()` is true)
/// every command except the transaction-control set (MULTI / EXEC / DISCARD /
/// WATCH / UNWATCH / RESET) is appended to `client.queued_argvs` and the
/// client receives `+QUEUED\r\n` instead of executing immediately.
pub fn dispatch(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let name: RedisString = match ctx.client_ref().arg(0) {
        Some(s) => s.clone(),
        None => return Err(RedisError::runtime(b"ERR empty command")),
    };
    if ctx.client_ref().flag_multi() {
        if crate::multi::is_no_multi_command(name.as_bytes()) {
            return Err(crate::multi::reject_no_multi_command(name.as_bytes()));
        }
        if !crate::multi::is_tx_control_command(name.as_bytes()) {
            return crate::multi::queue_current_command(ctx);
        }
    }
    if ctx.client_ref().in_pubsub_mode()
        && !crate::pubsub::is_allowed_in_subscribe_mode(name.as_bytes())
    {
        return Err(crate::pubsub::subscribe_mode_error(name.as_bytes()));
    }
    dispatch_command_name(ctx, name.as_bytes())
}

/// Dispatch using an externally-supplied command name.
///
/// Skips the MULTI-queueing pre-check. Used by `EXEC` to drain each queued
/// argv without re-entering the queue logic.
pub fn dispatch_command_name(ctx: &mut CommandContext<'_>, name: &[u8]) -> RedisResult<()> {
    match lookup_command(name) {
        Some(entry) => (entry.handler)(ctx),
        None => Err(unknown_command_error(name)),
    }
}

/// Build the canonical `unknown command '<name>'` error.
fn unknown_command_error(name: &[u8]) -> RedisError {
    let mut buf = Vec::with_capacity(b"ERR unknown command '".len() + name.len() + 1);
    buf.extend_from_slice(b"ERR unknown command '");
    buf.extend_from_slice(name);
    buf.push(b'\'');
    RedisError::runtime(buf)
}

/// Case-insensitive ASCII equality. Non-ASCII bytes compare strictly.
fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| ascii_lower(*x) == ascii_lower(*y))
}

fn ascii_lower(b: u8) -> u8 {
    if b.is_ascii_uppercase() {
        b + 32
    } else {
        b
    }
}

/// Wave A placeholder handler that returns `Err(RedisError::runtime(b"ERR …"))`.
///
/// Handler bodies in Waves B/C/D will replace these one by one. Routing to
/// the stub proves the table is wired correctly. Retained for new commands
/// scaffolded but not yet implemented.
#[allow(dead_code)]
fn unimplemented_handler(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let name = ctx.client_ref().arg(0).map(|s| s.as_bytes().to_vec()).unwrap_or_default();
    let mut msg = Vec::with_capacity(b"ERR command not implemented yet: ".len() + name.len());
    msg.extend_from_slice(b"ERR command not implemented yet: ");
    msg.extend_from_slice(&name);
    Err(RedisError::runtime(msg))
}

/// Static dispatch table.
///
/// Only includes commands whose handlers exist in this crate (even if the
/// handler body is `todo!()`). Wave B fills in PING + ECHO bodies; Wave C
/// fills in SET/GET/DEL/EXISTS/INCR.
///
/// PORT NOTE: For Wave A we route every entry to `unimplemented_handler`
/// rather than the real handler. The Wave B agent flips PING/ECHO over to
/// `crate::connection::ping_command` / `echo_command` once those exist;
/// Wave C does the same for string commands. This avoids `todo!()` panics
/// crashing the server during Wave A smoke testing.
pub static HANDLERS: &[DispatchEntry] = &[
    DispatchEntry { name: b"PING", handler: crate::connection::ping_command },
    DispatchEntry { name: b"ECHO", handler: crate::connection::echo_command },
    DispatchEntry { name: b"HELLO", handler: crate::connection::hello_command },
    DispatchEntry { name: b"COMMAND", handler: crate::connection::command_command },
    DispatchEntry { name: b"QUIT", handler: crate::connection::quit_command },
    DispatchEntry { name: b"SELECT", handler: crate::connection::select_command },
    DispatchEntry { name: b"CLIENT", handler: crate::connection::client_command },
    DispatchEntry { name: b"DEBUG", handler: crate::connection::debug_command },
    DispatchEntry { name: b"TIME", handler: crate::connection::time_command },
    DispatchEntry { name: b"RESET", handler: crate::connection::reset_command },
    DispatchEntry { name: b"SET", handler: crate::string::set_command },
    DispatchEntry { name: b"GET", handler: crate::string::get_command },
    DispatchEntry { name: b"DEL", handler: redis_core::db::del_command },
    DispatchEntry { name: b"EXISTS", handler: redis_core::db::exists_command },
    DispatchEntry { name: b"INCR", handler: crate::string::incr_command },
    DispatchEntry { name: b"DECR", handler: crate::string::decr_command },
    DispatchEntry { name: b"INCRBY", handler: crate::string::incrby_command },
    DispatchEntry { name: b"DECRBY", handler: crate::string::decrby_command },
    // ── GENERIC-KEY-OPS (Round 1, agent E2) ────────────────────────────────
    DispatchEntry { name: b"TYPE", handler: redis_core::db::type_command },
    DispatchEntry { name: b"RENAME", handler: redis_core::db::rename_command },
    DispatchEntry { name: b"RENAMENX", handler: redis_core::db::renamenx_command },
    DispatchEntry { name: b"RANDOMKEY", handler: redis_core::db::randomkey_command },
    DispatchEntry { name: b"DBSIZE", handler: redis_core::db::dbsize_command },
    DispatchEntry { name: b"FLUSHDB", handler: redis_core::db::flushdb_command },
    DispatchEntry { name: b"FLUSHALL", handler: redis_core::db::flushall_command },
    DispatchEntry { name: b"TOUCH", handler: redis_core::db::touch_command },
    DispatchEntry { name: b"UNLINK", handler: redis_core::db::unlink_command },
    DispatchEntry { name: b"KEYS", handler: redis_core::db::keys_command },
    DispatchEntry { name: b"COPY", handler: redis_core::db::copy_command },
    // ── STRING (Round 1, agent E1) ─────────────────────────────────────────
    DispatchEntry { name: b"APPEND", handler: crate::string::append_command },
    DispatchEntry { name: b"STRLEN", handler: crate::string::strlen_command },
    DispatchEntry { name: b"MGET", handler: crate::string::mget_command },
    DispatchEntry { name: b"MSET", handler: crate::string::mset_command },
    DispatchEntry { name: b"MSETNX", handler: crate::string::msetnx_command },
    DispatchEntry { name: b"SETNX", handler: crate::string::setnx_command },
    DispatchEntry { name: b"GETSET", handler: crate::string::getset_command },
    DispatchEntry { name: b"GETDEL", handler: crate::string::getdel_command },
    DispatchEntry { name: b"GETRANGE", handler: crate::string::getrange_command },
    DispatchEntry { name: b"SETRANGE", handler: crate::string::setrange_command },
    DispatchEntry { name: b"SUBSTR", handler: crate::string::getrange_command },
    DispatchEntry { name: b"SETEX", handler: crate::string::setex_command },
    DispatchEntry { name: b"PSETEX", handler: crate::string::psetex_command },
    DispatchEntry { name: b"GETEX", handler: crate::string::getex_command },
    DispatchEntry { name: b"MSETEX", handler: crate::string::msetex_command },
    DispatchEntry { name: b"INCRBYFLOAT", handler: crate::string::incrbyfloat_command },
    DispatchEntry { name: b"LCS", handler: crate::string::lcs_command },
    // ── LIST (Round 2) ─────────────────────────────────────────────────────
    DispatchEntry { name: b"LPUSH", handler: crate::list::lpush_command },
    DispatchEntry { name: b"RPUSH", handler: crate::list::rpush_command },
    DispatchEntry { name: b"LPUSHX", handler: crate::list::lpushx_command },
    DispatchEntry { name: b"RPUSHX", handler: crate::list::rpushx_command },
    DispatchEntry { name: b"LPOP", handler: crate::list::lpop_command },
    DispatchEntry { name: b"RPOP", handler: crate::list::rpop_command },
    DispatchEntry { name: b"LLEN", handler: crate::list::llen_command },
    DispatchEntry { name: b"LRANGE", handler: crate::list::lrange_command },
    DispatchEntry { name: b"LINDEX", handler: crate::list::lindex_command },
    DispatchEntry { name: b"LSET", handler: crate::list::lset_command },
    DispatchEntry { name: b"LREM", handler: crate::list::lrem_command },
    DispatchEntry { name: b"LTRIM", handler: crate::list::ltrim_command },
    DispatchEntry { name: b"LINSERT", handler: crate::list::linsert_command },
    DispatchEntry { name: b"LMOVE", handler: crate::list::lmove_command },
    DispatchEntry { name: b"RPOPLPUSH", handler: crate::list::rpoplpush_command },
    // ── HASH (Round 3) ─────────────────────────────────────────────────────
    DispatchEntry { name: b"HSET", handler: crate::hash::hset_command },
    DispatchEntry { name: b"HSETNX", handler: crate::hash::hsetnx_command },
    DispatchEntry { name: b"HGET", handler: crate::hash::hget_command },
    DispatchEntry { name: b"HMGET", handler: crate::hash::hmget_command },
    DispatchEntry { name: b"HMSET", handler: crate::hash::hmset_command },
    DispatchEntry { name: b"HDEL", handler: crate::hash::hdel_command },
    DispatchEntry { name: b"HEXISTS", handler: crate::hash::hexists_command },
    DispatchEntry { name: b"HLEN", handler: crate::hash::hlen_command },
    DispatchEntry { name: b"HSTRLEN", handler: crate::hash::hstrlen_command },
    DispatchEntry { name: b"HGETALL", handler: crate::hash::hgetall_command },
    DispatchEntry { name: b"HKEYS", handler: crate::hash::hkeys_command },
    DispatchEntry { name: b"HVALS", handler: crate::hash::hvals_command },
    DispatchEntry { name: b"HINCRBY", handler: crate::hash::hincrby_command },
    DispatchEntry { name: b"HINCRBYFLOAT", handler: crate::hash::hincrbyfloat_command },
    DispatchEntry { name: b"HRANDFIELD", handler: crate::hash::hrandfield_command },
    // ── SET (Round 4) ──────────────────────────────────────────────────────
    DispatchEntry { name: b"SADD", handler: crate::set::sadd_command },
    DispatchEntry { name: b"SREM", handler: crate::set::srem_command },
    DispatchEntry { name: b"SMEMBERS", handler: crate::set::smembers_command },
    DispatchEntry { name: b"SISMEMBER", handler: crate::set::sismember_command },
    DispatchEntry { name: b"SMISMEMBER", handler: crate::set::smismember_command },
    DispatchEntry { name: b"SCARD", handler: crate::set::scard_command },
    DispatchEntry { name: b"SPOP", handler: crate::set::spop_command },
    DispatchEntry { name: b"SRANDMEMBER", handler: crate::set::srandmember_command },
    DispatchEntry { name: b"SMOVE", handler: crate::set::smove_command },
    DispatchEntry { name: b"SINTER", handler: crate::set::sinter_command },
    DispatchEntry { name: b"SINTERSTORE", handler: crate::set::sinterstore_command },
    DispatchEntry { name: b"SINTERCARD", handler: crate::set::sintercard_command },
    DispatchEntry { name: b"SUNION", handler: crate::set::sunion_command },
    DispatchEntry { name: b"SUNIONSTORE", handler: crate::set::sunionstore_command },
    DispatchEntry { name: b"SDIFF", handler: crate::set::sdiff_command },
    DispatchEntry { name: b"SDIFFSTORE", handler: crate::set::sdiffstore_command },
    // ── TTL / EXPIRATION (Round 6) ─────────────────────────────────────────
    DispatchEntry { name: b"EXPIRE", handler: redis_core::expire::expire_command },
    DispatchEntry { name: b"PEXPIRE", handler: redis_core::expire::pexpire_command },
    DispatchEntry { name: b"EXPIREAT", handler: redis_core::expire::expireat_command },
    DispatchEntry { name: b"PEXPIREAT", handler: redis_core::expire::pexpireat_command },
    DispatchEntry { name: b"PERSIST", handler: redis_core::expire::persist_command },
    DispatchEntry { name: b"TTL", handler: redis_core::expire::ttl_command },
    DispatchEntry { name: b"PTTL", handler: redis_core::expire::pttl_command },
    DispatchEntry { name: b"EXPIRETIME", handler: redis_core::expire::expiretime_command },
    DispatchEntry { name: b"PEXPIRETIME", handler: redis_core::expire::pexpiretime_command },
    DispatchEntry { name: b"OBJECT", handler: redis_core::object::object_command },
    // ── ZSET (Round 5) ─────────────────────────────────────────────────────
    DispatchEntry { name: b"ZADD", handler: crate::zset::zadd_command },
    DispatchEntry { name: b"ZSCORE", handler: crate::zset::zscore_command },
    DispatchEntry { name: b"ZMSCORE", handler: crate::zset::zmscore_command },
    DispatchEntry { name: b"ZCARD", handler: crate::zset::zcard_command },
    DispatchEntry { name: b"ZINCRBY", handler: crate::zset::zincrby_command },
    DispatchEntry { name: b"ZRANGE", handler: crate::zset::zrange_command },
    DispatchEntry { name: b"ZRANGEBYSCORE", handler: crate::zset::zrangebyscore_command },
    DispatchEntry { name: b"ZREVRANGE", handler: crate::zset::zrevrange_command },
    DispatchEntry { name: b"ZREVRANGEBYSCORE", handler: crate::zset::zrevrangebyscore_command },
    DispatchEntry { name: b"ZRANK", handler: crate::zset::zrank_command },
    DispatchEntry { name: b"ZREVRANK", handler: crate::zset::zrevrank_command },
    DispatchEntry { name: b"ZREM", handler: crate::zset::zrem_command },
    DispatchEntry { name: b"ZCOUNT", handler: crate::zset::zcount_command },
    DispatchEntry { name: b"ZPOPMIN", handler: crate::zset::zpopmin_command },
    DispatchEntry { name: b"ZPOPMAX", handler: crate::zset::zpopmax_command },
    DispatchEntry { name: b"ZREMRANGEBYRANK", handler: crate::zset::zremrangebyrank_command },
    DispatchEntry { name: b"ZREMRANGEBYSCORE", handler: crate::zset::zremrangebyscore_command },
    // ── SCAN + ZSET-EXTRAS (Round 7) ───────────────────────────────────────
    DispatchEntry { name: b"SCAN", handler: redis_core::db::scan_command },
    DispatchEntry { name: b"HSCAN", handler: crate::hash::hscan_command },
    DispatchEntry { name: b"SSCAN", handler: crate::set::sscan_command },
    DispatchEntry { name: b"ZSCAN", handler: crate::zset::zscan_command },
    DispatchEntry { name: b"ZRANGEBYLEX", handler: crate::zset::zrangebylex_command },
    DispatchEntry { name: b"ZREVRANGEBYLEX", handler: crate::zset::zrevrangebylex_command },
    DispatchEntry { name: b"ZLEXCOUNT", handler: crate::zset::zlexcount_command },
    DispatchEntry { name: b"ZREMRANGEBYLEX", handler: crate::zset::zremrangebylex_command },
    DispatchEntry { name: b"ZUNIONSTORE", handler: crate::zset::zunionstore_command },
    DispatchEntry { name: b"ZINTERSTORE", handler: crate::zset::zinterstore_command },
    DispatchEntry { name: b"ZDIFFSTORE", handler: crate::zset::zdiffstore_command },
    DispatchEntry { name: b"ZUNION", handler: crate::zset::zunion_command },
    DispatchEntry { name: b"ZINTER", handler: crate::zset::zinter_command },
    DispatchEntry { name: b"ZDIFF", handler: crate::zset::zdiff_command },
    DispatchEntry { name: b"ZINTERCARD", handler: crate::zset::zintercard_command },
    DispatchEntry { name: b"ZRANGESTORE", handler: crate::zset::zrangestore_command },
    DispatchEntry { name: b"ZRANDMEMBER", handler: crate::zset::zrandmember_command },
    DispatchEntry { name: b"ZMPOP", handler: crate::zset::zmpop_command },
    // ── BITMAP (Round 8c) ──────────────────────────────────────────────────
    DispatchEntry { name: b"SETBIT", handler: crate::bitops::setbit_command },
    DispatchEntry { name: b"GETBIT", handler: crate::bitops::getbit_command },
    DispatchEntry { name: b"BITCOUNT", handler: crate::bitops::bitcount_command },
    DispatchEntry { name: b"BITPOS", handler: crate::bitops::bitpos_command },
    DispatchEntry { name: b"BITOP", handler: crate::bitops::bitop_command },
    DispatchEntry { name: b"BITFIELD", handler: crate::bitops::bitfield_command },
    DispatchEntry { name: b"BITFIELD_RO", handler: crate::bitops::bitfield_ro_command },
    // ── TRANSACTIONS (Round 8b) ────────────────────────────────────────────
    DispatchEntry { name: b"MULTI", handler: crate::multi::multi_command },
    DispatchEntry { name: b"EXEC", handler: crate::multi::exec_command },
    DispatchEntry { name: b"DISCARD", handler: crate::multi::discard_command },
    DispatchEntry { name: b"WATCH", handler: crate::multi::watch_command },
    DispatchEntry { name: b"UNWATCH", handler: crate::multi::unwatch_command },
    // ── TCL HARNESS STUBS (Round 9) ────────────────────────────────────────
    DispatchEntry { name: b"FUNCTION", handler: crate::connection::function_command },
    DispatchEntry { name: b"CONFIG", handler: crate::connection::config_command },
    DispatchEntry { name: b"MEMORY", handler: crate::connection::memory_command },
    // ── PUB/SUB (Round 8a) ─────────────────────────────────────────────────
    DispatchEntry { name: b"SUBSCRIBE", handler: crate::pubsub::subscribe_command },
    DispatchEntry { name: b"UNSUBSCRIBE", handler: crate::pubsub::unsubscribe_command },
    DispatchEntry { name: b"PSUBSCRIBE", handler: crate::pubsub::psubscribe_command },
    DispatchEntry { name: b"PUNSUBSCRIBE", handler: crate::pubsub::punsubscribe_command },
    DispatchEntry { name: b"PUBLISH", handler: crate::pubsub::publish_command },
    DispatchEntry { name: b"PUBSUB", handler: crate::pubsub::pubsub_command },
    // ── HYPERLOGLOG (Round 9 HLL) ──────────────────────────────────────────
    DispatchEntry { name: b"PFADD", handler: crate::hyperloglog::pfadd_command },
    DispatchEntry { name: b"PFCOUNT", handler: crate::hyperloglog::pfcount_command },
    DispatchEntry { name: b"PFMERGE", handler: crate::hyperloglog::pfmerge_command },
    // ── INTROSPECTION (Round 9 INFO/CONFIG) ────────────────────────────────
    DispatchEntry { name: b"INFO", handler: crate::info::info_command },
    DispatchEntry { name: b"LASTSAVE", handler: crate::info::lastsave_command },
    // ── STREAMS (Round 9) ──────────────────────────────────────────────────
    DispatchEntry { name: b"XADD", handler: crate::stream::xadd_command },
    DispatchEntry { name: b"XLEN", handler: crate::stream::xlen_command },
    DispatchEntry { name: b"XRANGE", handler: crate::stream::xrange_command },
    DispatchEntry { name: b"XREVRANGE", handler: crate::stream::xrevrange_command },
    DispatchEntry { name: b"XDEL", handler: crate::stream::xdel_command },
    DispatchEntry { name: b"XTRIM", handler: crate::stream::xtrim_command },
    DispatchEntry { name: b"XREAD", handler: crate::stream::xread_command },
    DispatchEntry { name: b"XINFO", handler: crate::stream::xinfo_command },
];

#[cfg(test)]
mod tests {
    use super::*;
    use redis_core::Client;

    #[test]
    fn lookup_is_case_insensitive() {
        assert!(lookup_command(b"PING").is_some());
        assert!(lookup_command(b"ping").is_some());
        assert!(lookup_command(b"Ping").is_some());
        assert!(lookup_command(b"PiNg").is_some());
    }

    #[test]
    fn unknown_command_is_none() {
        assert!(lookup_command(b"NOTACOMMAND").is_none());
    }

    #[test]
    fn dispatch_unknown_returns_err() {
        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"NOTACOMMAND")]);
        let mut ctx = CommandContext::new(&mut c);
        let err = dispatch(&mut ctx).unwrap_err();
        match err {
            RedisError::Runtime(s) => {
                assert!(s.as_bytes().starts_with(b"ERR unknown command"));
            }
            _ => panic!("expected Runtime error"),
        }
    }

    #[test]
    fn dispatch_routes_known_command() {
        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"HELLO")]);
        let mut ctx = CommandContext::new(&mut c);
        dispatch(&mut ctx).unwrap();
        let reply = c.drain_reply();
        assert!(reply.starts_with(b"*"));
        assert!(reply.windows(b"server".len()).any(|w| w == b"server"));
    }

    #[test]
    fn dispatch_routes_ping_to_real_handler() {
        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"PING")]);
        let mut ctx = CommandContext::new(&mut c);
        dispatch(&mut ctx).unwrap();
        assert_eq!(c.drain_reply(), b"+PONG\r\n");
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (Wave A — dispatch lookup fn)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Lookup + routing wired. Handler bodies are stubbed via
//                  unimplemented_handler so the binary returns a clean error
//                  reply for any command; Waves B/C wire the real bodies.
// ──────────────────────────────────────────────────────────────────────────
