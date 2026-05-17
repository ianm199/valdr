//! MULTI/EXEC transaction block and WATCH/UNWATCH CAS implementation.
//!
//! C source: `reference/valkey/src/multi.c` (585 lines, 18 functions).
//!
//! Phase B integration (Round 8b)
//! ------------------------------
//! This module ships the five user-facing transaction commands wired through
//! the same `dispatch` entrypoint used by every other command. Queueing is
//! implemented by `dispatch::dispatch` itself — once `client.flag_multi()` is
//! set, every subsequent command other than `MULTI`/`EXEC`/`DISCARD`/`WATCH`/
//! `UNWATCH`/`RESET` is pushed onto `client.queued_argvs` and the client
//! receives `+QUEUED\r\n`.
//!
//! Cross-connection WATCH invalidation goes through a process-wide index in
//! `redis-core::db::watched_keys_index()`. Every mutation through
//! `RedisDb::set_key` / `sync_delete` / `clear` marks watching clients as
//! dirty there. `EXEC` consults the index at the top of its body, sees its
//! own client id in the dirty set, and aborts with `*-1\r\n`.
//!
//! Architectural shortcut: the process-wide index lives behind a `OnceLock`
//! because the current `RedisDb` does not own a reference back to
//! `RedisServer`, and `RedisServer` does not own the live `Client` list.
//! Once both are wired (Phase 3), the index can move onto `RedisServer` and
//! the OnceLock can be retired.

use redis_core::client::Client;
use redis_core::command_context::CommandContext;
use redis_core::db::{
    watched_keys_index_add, watched_keys_index_remove_client, watched_keys_take_dirty,
};
use redis_protocol::frame::{encode_resp2, RespFrame};
use redis_types::{RedisError, RedisResult, RedisString};

use crate::dispatch::{dispatch_command_name, lookup_command};

const NAME_RESET: &[u8] = b"RESET";
const NAME_MULTI: &[u8] = b"MULTI";
const NAME_EXEC: &[u8] = b"EXEC";
const NAME_DISCARD: &[u8] = b"DISCARD";
const NAME_WATCH: &[u8] = b"WATCH";
const NAME_UNWATCH: &[u8] = b"UNWATCH";

/// True when `name` runs eagerly even inside a MULTI block.
///
/// EXEC, DISCARD, UNWATCH, and RESET tear down or progress the transaction;
/// they must not be queued. MULTI and WATCH carry `CMD_NO_MULTI` in the C
/// command table and are rejected by [`reject_no_multi_command`] before
/// reaching the queue path — they are listed here too so dispatch routes
/// them through the rejection helper instead of the queue.
pub fn is_tx_control_command(name: &[u8]) -> bool {
    is_no_multi_command(name)
        || eq_ignore_ascii(name, NAME_EXEC)
        || eq_ignore_ascii(name, NAME_DISCARD)
        || eq_ignore_ascii(name, NAME_UNWATCH)
        || eq_ignore_ascii(name, NAME_RESET)
}

/// True when `name` carries the C `CMD_NO_MULTI` flag.
///
/// Per `commands/*.json`, only `MULTI` and `WATCH` are tagged. Inside a MULTI
/// block these commands are rejected with the generic `Command 'X' not
/// allowed inside a transaction` error real Valkey emits (see C
/// `server.c::processCommand` after the `flag.multi && CMD_NO_MULTI` check).
pub fn is_no_multi_command(name: &[u8]) -> bool {
    eq_ignore_ascii(name, NAME_MULTI) || eq_ignore_ascii(name, NAME_WATCH)
}

fn eq_ignore_ascii(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Reject a `CMD_NO_MULTI` command issued inside a MULTI block.
///
/// Reproduces the exact C error text from `server.c` line 4410. Lowercases
/// the command name to mirror the C `c->cmd->fullname` lookup (the canonical
/// dispatch table stores lowercase names).
pub fn reject_no_multi_command(name: &[u8]) -> RedisError {
    let mut lower = Vec::with_capacity(name.len());
    for b in name {
        lower.push(b.to_ascii_lowercase());
    }
    let mut msg = Vec::with_capacity(b"ERR Command '' not allowed inside a transaction".len() + lower.len());
    msg.extend_from_slice(b"ERR Command '");
    msg.extend_from_slice(&lower);
    msg.extend_from_slice(b"' not allowed inside a transaction");
    RedisError::runtime(msg)
}

/// Append the client's current `argv` to its MULTI queue and reply `+QUEUED`.
///
/// Caller has already verified `client.flag_multi()` is true and the command
/// is not a transaction-control command. Performs a basic command-existence
/// check; on miss it sets the dirty-exec flag so the EXEC step responds with
/// the `EXECABORT` error real Redis emits.
pub fn queue_current_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argv: Vec<RedisString> = ctx.client_ref().argv.clone();
    let name = match argv.first() {
        Some(n) => n.clone(),
        None => return Err(RedisError::runtime(b"ERR empty command")),
    };
    if lookup_command(name.as_bytes()).is_none() {
        ctx.client_mut().set_flag_dirty_exec(true);
        let mut msg = Vec::with_capacity(b"ERR unknown command '".len() + name.as_bytes().len() + 1);
        msg.extend_from_slice(b"ERR unknown command '");
        msg.extend_from_slice(name.as_bytes());
        msg.push(b'\'');
        return Err(RedisError::runtime(msg));
    }
    ctx.client_mut().queued_argvs.push(argv);
    ctx.reply_simple_string(b"QUEUED")
}

/// `MULTI` — begin a transaction block.
///
/// Dispatch rejects nested `MULTI` at the `CMD_NO_MULTI` gate before this
/// handler is reached, so by the time we run the client is guaranteed to be
/// outside any transaction.
pub fn multi_command(ctx: &mut CommandContext) -> RedisResult<()> {
    ctx.client_mut().set_flag_multi(true);
    ctx.reply_simple_string(b"OK")
}

/// `DISCARD` — abort a transaction block.
pub fn discard_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if !ctx.client_ref().flag_multi() {
        return Err(RedisError::runtime(b"ERR DISCARD without MULTI"));
    }
    reset_multi_state(ctx.client_mut());
    ctx.reply_simple_string(b"OK")
}

/// `EXEC` — run every queued command and emit the array of replies.
pub fn exec_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if !ctx.client_ref().flag_multi() {
        return Err(RedisError::runtime(b"ERR EXEC without MULTI"));
    }

    let cid = ctx.client_ref().id();
    let dirty_from_index = watched_keys_take_dirty(cid);
    if dirty_from_index {
        ctx.client_mut().set_flag_dirty_cas(true);
    }

    if ctx.client_ref().flag_dirty_exec() {
        reset_multi_state(ctx.client_mut());
        return Err(RedisError::runtime(
            b"EXECABORT Transaction discarded because of previous errors.",
        ));
    }
    if ctx.client_ref().flag_dirty_cas() {
        reset_multi_state(ctx.client_mut());
        ctx.reply_null_array()?;
        return Ok(());
    }

    let queued: Vec<Vec<RedisString>> = std::mem::take(&mut ctx.client_mut().queued_argvs);
    ctx.client_mut().set_flag_multi(false);

    ctx.reply_array_header(queued.len())?;

    for argv in queued.into_iter() {
        run_one_queued(ctx, argv);
    }

    reset_multi_state(ctx.client_mut());
    Ok(())
}

/// Run a single queued argv as if the client had just sent it directly.
///
/// Replies (including errors) are written into `client.reply_buf` exactly as
/// they would be for a top-level dispatch — that's what gives EXEC its array
/// of inner frames.
fn run_one_queued(ctx: &mut CommandContext, argv: Vec<RedisString>) {
    ctx.client_mut().set_args(argv);
    let name = ctx.client_ref().arg(0).cloned();
    let result = match name {
        Some(n) => dispatch_command_name(ctx, n.as_bytes()),
        None => Err(RedisError::runtime(b"ERR empty queued command")),
    };
    if let Err(err) = result {
        let payload = err.to_resp_payload();
        encode_resp2(&RespFrame::Error(payload), &mut ctx.client_mut().reply_buf);
    }
}

/// `WATCH key [key …]` — register CAS watchers on each key.
///
/// `CMD_NO_MULTI` causes dispatch to reject `WATCH` inside an open
/// transaction with the standard "Command 'watch' not allowed inside a
/// transaction" message before this handler runs.
pub fn watch_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.argc();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"WATCH"));
    }
    let cid = ctx.client_ref().id();
    for j in 1..argc {
        let key = ctx.arg_owned(j)?;
        watched_keys_index_add(&key, cid);
    }
    ctx.reply_simple_string(b"OK")
}

/// `UNWATCH` — remove every CAS watcher for this client.
pub fn unwatch_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let cid = ctx.client_ref().id();
    watched_keys_index_remove_client(cid);
    let _ = watched_keys_take_dirty(cid);
    ctx.client_mut().set_flag_dirty_cas(false);
    ctx.reply_simple_string(b"OK")
}

/// Clear the multi-bit, the queue, the dirty flags, and the WATCH set.
///
/// Called by `DISCARD`, `EXEC` (after run), and `Client::reset_state`. Mirrors
/// `multi.c::discardTransaction`.
pub fn reset_multi_state(client: &mut Client) {
    client.queued_argvs.clear();
    client.set_flag_multi(false);
    client.set_flag_dirty_cas(false);
    client.set_flag_dirty_exec(false);
    watched_keys_index_remove_client(client.id());
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_core::Client;

    #[test]
    fn multi_then_discard_clears_flag() {
        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"MULTI")]);
        let mut ctx = CommandContext::new(&mut c);
        multi_command(&mut ctx).unwrap();
        assert!(ctx.client_ref().flag_multi());

        c.set_args(vec![RedisString::from_bytes(b"DISCARD")]);
        let mut ctx = CommandContext::new(&mut c);
        discard_command(&mut ctx).unwrap();
        assert!(!ctx.client_ref().flag_multi());
    }

    #[test]
    fn no_multi_rejection_uses_canonical_text() {
        let err = reject_no_multi_command(b"MULTI");
        let payload = err.to_resp_payload();
        assert!(payload
            .as_bytes()
            .starts_with(b"ERR Command 'multi' not allowed inside a transaction"));
    }

    #[test]
    fn discard_without_multi_errors() {
        let mut c = Client::new(3);
        c.set_args(vec![RedisString::from_bytes(b"DISCARD")]);
        let mut ctx = CommandContext::new(&mut c);
        let err = discard_command(&mut ctx).unwrap_err();
        assert!(matches!(err, RedisError::Runtime(_)));
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/multi.c (Round 8b dispatch-integration rewrite)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         3
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Cross-conn WATCH dirty propagation runs through the global
//                  watched-keys index in redis-core::db. CLIENT PAUSE during
//                  EXEC, scripting (EVAL inside MULTI), and proper EXEC ACL
//                  re-checks are deferred. SELECT-tracking inside MULTI is
//                  also a TODO until per-tx db routing lands.
// ──────────────────────────────────────────────────────────────────────────
