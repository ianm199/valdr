//! Shared command-policy helpers for Lua script and function execution.
//!
//! These checks are backend-neutral: both the mlua and lua-rs execution paths
//! use them to decide whether an inner command may run, whether writes are
//! allowed on replicas, and how script-level rejection errors are recorded.

use mlua::{Error as LuaError, Lua, MultiValue, Value as LuaValue};
use redis_core::acl::global_acl_state;
use redis_core::db::glob_match;
use redis_core::memory::approximate_memory_used;
use redis_core::metrics::{record_command_stat, record_error_reply};
use redis_core::CommandContext;
use redis_types::{RedisError, RedisString};

use crate::dispatch::command_acl_categories;

use super::bytes::ascii_eq_ci;
use super::resp_bridge::LUA_ERROR_ALREADY_RECORDED_FIELD;
use super::script_errors::{lua_arg_to_bytes, lua_script_call_error_payload};
use super::REPLICA_READONLY_ERROR_PAYLOAD;

pub(super) fn record_script_rejected_command(args: &[Vec<u8>], payload: &[u8]) {
    if let Some(name) = args.first() {
        record_command_stat(name, 0, true, false);
    }
    record_error_reply(payload);
}

pub(super) fn function_oom_error() -> RedisError {
    RedisError::runtime(b"OOM command not allowed when used memory > 'maxmemory'.")
}

pub(super) fn function_command_would_exceed_maxmemory(ctx: &CommandContext<'_>) -> bool {
    let maxmemory = ctx.live_config().maxmemory();
    if maxmemory == 0 {
        return false;
    }
    approximate_memory_used(ctx.db()).saturating_add(1024) > maxmemory
}

pub(super) fn stale_replica_scripts_blocked(ctx: &CommandContext<'_>) -> bool {
    crate::dispatch::stale_replica_blocked(ctx)
}

pub(super) fn replica_readonly_script_blocked(ctx: &CommandContext<'_>) -> bool {
    redis_core::replication::global_replication_state().is_replica()
        && !ctx.client_ref().is_replica
        && !ctx.client_ref().replication_apply
}

pub(super) fn replica_readonly_error() -> RedisError {
    RedisError::runtime(REPLICA_READONLY_ERROR_PAYLOAD)
}

pub(super) fn replica_readonly_lua_call_payload() -> Vec<u8> {
    lua_script_call_error_payload(REPLICA_READONLY_ERROR_PAYLOAD.to_vec())
}

pub(super) fn replica_readonly_lua_call_error() -> LuaError {
    LuaError::RuntimeError(
        String::from_utf8_lossy(&replica_readonly_lua_call_payload()).into_owned(),
    )
}

pub(super) fn replica_readonly_lua_call_table(lua: &Lua) -> mlua::Result<LuaValue> {
    let t = lua.create_table()?;
    t.raw_set(
        "err",
        lua.create_string(&replica_readonly_lua_call_payload())?,
    )?;
    t.raw_set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
    Ok(LuaValue::Table(t))
}

pub(super) fn replica_readonly_lua_call_blocked(
    ctx: &CommandContext<'_>,
    args: &[Vec<u8>],
) -> bool {
    call_is_write_command(args)
        && replica_readonly_script_blocked(ctx)
        && ctx.live_config().slave_read_only()
}

pub(super) fn good_replicas_status(ctx: &CommandContext<'_>) -> bool {
    let min_replicas = ctx.live_config().repl_min_replicas_to_write();
    let max_lag_secs = ctx.live_config().repl_min_replicas_max_lag();
    if min_replicas == 0 || max_lag_secs == 0 {
        return true;
    }
    let repl = redis_core::replication::global_replication_state();
    if repl.is_replica() {
        return true;
    }
    repl.good_replicas_count(max_lag_secs) as u64 >= min_replicas
}

pub(super) const NOREPLICAS_ERROR: &str = "NOREPLICAS Not enough good replicas to write.";
pub(super) fn noreplicas_error() -> RedisError {
    RedisError::runtime(NOREPLICAS_ERROR.as_bytes())
}

pub(super) fn noreplicas_lua_error() -> LuaError {
    LuaError::RuntimeError(NOREPLICAS_ERROR.to_string())
}

pub(super) fn noreplicas_lua_table(lua: &Lua) -> mlua::Result<LuaValue> {
    let t = lua.create_table()?;
    t.raw_set("err", lua.create_string(NOREPLICAS_ERROR)?)?;
    t.raw_set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
    Ok(LuaValue::Table(t))
}

pub(super) fn stale_replica_masterdown_error() -> RedisError {
    RedisError::runtime(
        b"MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.",
    )
}

pub(super) fn stale_replica_lua_call_allowed(args: &[Vec<u8>]) -> bool {
    args.first().is_some_and(|name| {
        let name = name.as_slice();
        ascii_eq_ci(name, b"ECHO") || ascii_eq_ci(name, b"INFO")
    })
}

pub(super) fn stale_replica_lua_call_error() -> LuaError {
    LuaError::RuntimeError("Can not execute the command on a stale replica".to_string())
}

pub(super) fn script_command_not_allowed(args: &[Vec<u8>]) -> bool {
    args.first()
        .is_some_and(|name| ascii_eq_ci(name.as_slice(), b"CLUSTER"))
}

pub(super) fn acl_check_cmd_allowed(
    ctx: &CommandContext<'_>,
    args: &[Vec<u8>],
) -> mlua::Result<bool> {
    let Some(command) = args.first() else {
        return Err(LuaError::RuntimeError(
            "ERR Invalid command passed to server.acl_check_cmd()".to_string(),
        ));
    };
    let Some(categories) = command_acl_categories(command) else {
        return Err(LuaError::RuntimeError(
            "ERR Invalid command passed to server.acl_check_cmd()".to_string(),
        ));
    };

    let default_name = RedisString::from_bytes(b"default");
    let user_name = ctx
        .client_ref()
        .authenticated_user
        .clone()
        .unwrap_or(default_name);
    let acl = global_acl_state();
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let Some(user) = guard.users.get(&user_name) else {
        return Ok(false);
    };
    if !user.can_execute_command(command, categories) {
        return Ok(false);
    }
    if user.flags.allkeys || args.len() < 2 {
        return Ok(true);
    }
    let key = &args[1];
    Ok(user
        .key_patterns
        .iter()
        .any(|pattern| glob_match(pattern.as_bytes(), key)))
}

pub(super) fn call_is_write_command(args: &[Vec<u8>]) -> bool {
    let Some(command) = args.first() else {
        return false;
    };
    let name = command.as_slice();
    if crate::dispatch::command_is_write_or_may_replicate(name) {
        return true;
    }
    ascii_eq_ci(name, b"SET")
        || ascii_eq_ci(name, b"SETEX")
        || ascii_eq_ci(name, b"PSETEX")
        || ascii_eq_ci(name, b"SETNX")
        || ascii_eq_ci(name, b"GETSET")
        || ascii_eq_ci(name, b"DEL")
        || ascii_eq_ci(name, b"UNLINK")
        || ascii_eq_ci(name, b"EXPIRE")
        || ascii_eq_ci(name, b"PEXPIRE")
        || ascii_eq_ci(name, b"EXPIREAT")
        || ascii_eq_ci(name, b"PEXPIREAT")
        || ascii_eq_ci(name, b"PERSIST")
        || ascii_eq_ci(name, b"HSET")
        || ascii_eq_ci(name, b"HDEL")
        || ascii_eq_ci(name, b"LPUSH")
        || ascii_eq_ci(name, b"RPUSH")
        || ascii_eq_ci(name, b"LPOP")
        || ascii_eq_ci(name, b"RPOP")
        || ascii_eq_ci(name, b"SADD")
        || ascii_eq_ci(name, b"SREM")
        || ascii_eq_ci(name, b"ZADD")
        || ascii_eq_ci(name, b"ZREM")
        || ascii_eq_ci(name, b"INCR")
        || ascii_eq_ci(name, b"DECR")
        || ascii_eq_ci(name, b"INCRBY")
        || ascii_eq_ci(name, b"DECRBY")
        || ascii_eq_ci(name, b"APPEND")
        || ascii_eq_ci(name, b"FLUSHDB")
        || ascii_eq_ci(name, b"FLUSHALL")
}

pub(super) fn collect_call_args(args: MultiValue) -> Result<Vec<Vec<u8>>, LuaError> {
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(args.len());
    for v in args {
        out.push(lua_arg_to_bytes(&v)?);
    }
    Ok(out)
}
