//! Inner command dispatch bridge for Lua `redis.call` / `redis.pcall`.

use std::cell::Cell;
use std::sync::atomic::Ordering;

use redis_core::metrics::{record_command_stat, record_error_reply};
use redis_core::CommandContext;
use redis_types::{RedisError, RedisString};

use super::bytes::ascii_eq_ci;
use super::command_policy::{
    call_is_write_command, function_command_would_exceed_maxmemory, function_oom_error,
};
use super::resp_bridge::{parse_reply_value, ReplyValue};
use crate::dispatch::{command_is_denyoom, dispatch_command_name};

/// Execute one inner command for `redis.call` / `redis.pcall`, capturing
/// the reply bytes the handler appended to `reply_buf` and parsing them
/// back into a [`ReplyValue`].
/// Restores the caller's argv and reply prefix unconditionally so the outer
/// EVAL/FCALL reply is unaffected by inner dispatch side effects.
pub(super) fn run_inner_command(
    ctx: &mut CommandContext<'_>,
    args: &[Vec<u8>],
    script_dirty: Option<&Cell<bool>>,
) -> Result<ReplyValue, RedisError> {
    if args.is_empty() {
        return Err(RedisError::runtime(
            b"Please specify at least one argument for this call",
        ));
    }

    let saved_argv = ctx.client_ref().argv.clone();
    let saved_reply_len = ctx.client_ref().reply_buf.len();
    let name_bytes = args[0].clone();

    if ascii_eq_ci(&name_bytes, b"WAIT") {
        return script_wait_reply(ctx, args);
    }

    if command_is_denyoom(&name_bytes)
        && !script_dirty.is_some_and(Cell::get)
        && function_command_would_exceed_maxmemory(ctx)
    {
        record_command_stat(&name_bytes, 0, true, false);
        record_error_reply(b"OOM command not allowed when used memory > 'maxmemory'.");
        return Err(function_oom_error());
    }

    let new_argv: Vec<RedisString> = args
        .iter()
        .map(|b| RedisString::from_bytes(b.as_slice()))
        .collect();
    ctx.client_mut().set_args(new_argv);

    let old_deny_blocking = ctx.client_ref().flag_deny_blocking();
    let old_lua = ctx.client_ref().flag_lua();
    ctx.client_mut().set_flag_deny_blocking(true);
    ctx.client_mut().set_flag_lua(true);

    let dispatch_result = dispatch_command_name(ctx, &name_bytes);
    ctx.client_mut().commands_processed = ctx.client_ref().commands_processed.saturating_add(1);
    ctx.client_mut().set_flag_deny_blocking(old_deny_blocking);
    ctx.client_mut().set_flag_lua(old_lua);

    let raw_reply: Vec<u8> = {
        let buf = &mut ctx.client_mut().reply_buf;

        buf.split_off(saved_reply_len)
    };

    ctx.client_mut().set_args(saved_argv);

    if let Err(ref err) = dispatch_result {
        if raw_reply.is_empty() {
            record_error_reply(err.to_resp_payload().as_bytes());
            return Err(err.clone());
        }
    }

    if raw_reply.is_empty() {
        if dispatch_result.is_ok() && call_is_write_command(args) {
            if let Some(dirty) = script_dirty {
                dirty.set(true);
            }
        }
        return Ok(ReplyValue::Nil);
    }

    let reply = parse_reply_value(&raw_reply)?;
    if let ReplyValue::Error(msg) = &reply {
        record_error_reply(msg);
    } else if call_is_write_command(args) {
        if let Some(dirty) = script_dirty {
            dirty.set(true);
        }
    }
    Ok(reply)
}

fn script_wait_reply(ctx: &CommandContext<'_>, args: &[Vec<u8>]) -> Result<ReplyValue, RedisError> {
    if args.len() != 3 {
        return Err(RedisError::wrong_number_of_args(b"wait"));
    }
    parse_script_i64(&args[1])?;
    let timeout = parse_script_i64(&args[2])?;
    if timeout < 0 {
        return Err(RedisError::runtime(b"ERR timeout is negative"));
    }
    let target = ctx.client_ref().last_write_repl_offset;
    let repl = redis_core::replication::global_replication_state();
    let count = {
        let guard = match repl.replicas.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard
            .values()
            .filter(|replica| {
                replica.state() == redis_core::replication::ReplicaState::Online
                    && replica.offset.load(Ordering::Relaxed) >= target
            })
            .count()
    };
    Ok(ReplyValue::Integer(count as i64))
}

fn parse_script_i64(bytes: &[u8]) -> Result<i64, RedisError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| RedisError::runtime(b"ERR value is not an integer or out of range"))
}
