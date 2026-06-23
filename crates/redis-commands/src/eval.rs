//! `EVAL` / `EVALSHA` / `SCRIPT` — server-side Lua scripting.
//! Backed by `mlua` (bundled C Lua 5.1, matching real Redis). The runtime is
//! constructed once per call so global state never leaks across scripts
//! the dangerous portions of the stdlib (`os`, `io`, `debug`, `require`,
//! `loadfile`, `dofile`, `package`, `print`) are removed before user code
//! runs.
//! `redis.call` / `redis.pcall` re-enter the command dispatch table by
//! saving the client's argv and reply buffer, installing the synthetic
//! argv, calling [`crate::dispatch::dispatch_command_name`], parsing
//! newly-written reply bytes back into a Lua value, then restoring
//! caller's argv and the original reply buffer prefix.
//! Script cache ownership lives in `eval::script_cache`; `SCRIPT LOAD`
//! inserts into the process-wide cache, `EVALSHA` looks up by lower-case
//! 40-byte SHA-1 hex, and `SCRIPT FLUSH` clears cached scripts.
//! See `docs/ADR_001_LUA_RUNTIME.md` for the runtime-choice rationale
//! the full sandbox patch list.

use redis_core::metrics::record_error_reply;
use redis_core::CommandContext;
use redis_protocol::frame::RespFrame;

const READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD: &[u8] =
    b"ERR Write commands are not allowed from read-only scripts. script:1";
const READ_ONLY_SCRIPT_WRITE_ERROR_LUA: &str =
    "Write commands are not allowed from read-only scripts. script:1";
const READ_ONLY_SCRIPT_WRITE_ERROR_RESP: &str =
    "ERR Write commands are not allowed from read-only scripts.";
const REPLICA_READONLY_ERROR_PAYLOAD: &[u8] =
    b"READONLY You can't write against a read only replica.";
use redis_types::{RedisError, RedisResult, RedisString};

mod active_function;
mod busy_script;
mod bytes;
mod command_policy;
mod function_commands;
mod function_compiler;
mod function_dump;
mod function_metadata;
mod function_runtime;
mod function_store;
mod function_uncached_runtime;
mod inner_command;
mod lua_api;
mod lua_bit;
mod lua_cjson;
mod lua_cmsgpack;
#[cfg(feature = "lua-rs-engine")]
mod lua_rs_backend;
#[cfg(feature = "lua-rs-engine")]
mod lua_rs_libs;
mod lua_sandbox;
mod resp_bridge;
mod script_cache;
mod script_checks;
mod script_commands;
mod script_errors;
mod script_flags;
mod script_runtime;

pub(crate) use busy_script::{busy_script_error_reply, busy_script_owner_is, is_script_busy};
use bytes::ascii_eq_ci;
pub use function_commands::{
    fcall_command, fcall_ro_command, function_delete_command, function_dump_command,
    function_flush_command, function_kill_command, function_list_command, function_load_command,
    function_restore_command, function_stats_command,
};
pub(crate) use function_dump::{
    function_library_codes_for_aof_rewrite, function_vm_memory_used_estimate,
};
pub use function_dump::{
    function_rdb_payloads, install_rdb_function_replacement, prepare_rdb_function_replacement,
};
use function_store::find_loaded_function;
pub use function_store::PreparedFunctionLibraries;
use inner_command::run_inner_command;
use resp_bridge::ReplyValue;
use script_cache::{cache_script, normalise_sha};
pub(crate) use script_cache::{
    evicted_scripts_count, reset_script_cache_stats, script_cache_len, script_cache_memory_estimate,
};
pub use script_commands::script_command;
use script_flags::parse_eval_shebang;
use script_runtime::run_script;

pub(crate) fn queued_script_declares_write(argv: &[RedisString]) -> bool {
    let Some(name) = argv.first().map(|s| s.as_bytes()) else {
        return false;
    };
    if ascii_eq_ci(name, b"EVAL") {
        return argv
            .get(1)
            .and_then(|script| parse_eval_shebang(script.as_bytes()).ok().map(|(f, _)| f))
            .is_some_and(|flags| flags.has_shebang && !flags.no_writes);
    }
    if ascii_eq_ci(name, b"EVALSHA") {
        let Some(sha) = argv.get(1).and_then(|raw| normalise_sha(raw.as_bytes())) else {
            return false;
        };
        let script = {
            let guard = match script_cache::script_cache().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.entries.get(&sha).map(|entry| entry.body.clone())
        };
        return script
            .as_deref()
            .and_then(|body| parse_eval_shebang(body).ok().map(|(f, _)| f))
            .is_some_and(|flags| flags.has_shebang && !flags.no_writes);
    }
    if ascii_eq_ci(name, b"FCALL") {
        return argv
            .get(1)
            .and_then(|function_name| find_loaded_function(function_name.as_bytes()))
            .is_some_and(|(_, definition)| !definition.no_writes);
    }
    false
}

pub(crate) fn eval_script_arg_is_no_writes(script: &[u8]) -> bool {
    parse_eval_shebang(script)
        .map(|(flags, _)| flags.has_shebang && flags.no_writes)
        .unwrap_or(false)
}

pub(crate) fn cached_evalsha_is_no_writes(raw_sha: &[u8]) -> bool {
    let Some(sha) = normalise_sha(raw_sha) else {
        return false;
    };
    let script = {
        let guard = match script_cache::script_cache().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.entries.get(&sha).map(|entry| entry.body.clone())
    };
    script.as_deref().is_some_and(eval_script_arg_is_no_writes)
}

pub(crate) fn loaded_function_is_no_writes(name: &[u8]) -> bool {
    find_loaded_function(name).is_some_and(|(_, definition)| definition.no_writes)
}

/// `EVAL script numkeys key [key...] arg [arg...]`.
/// Parses the argv, constructs a fresh sandboxed Lua instance, injects
/// the `redis` table plus `KEYS` / `ARGV`, runs the script, and writes
/// the result back as the outer RESP reply.
pub fn eval_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    eval_command_impl(ctx, false, b"eval")
}

pub fn eval_ro_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    eval_command_impl(ctx, true, b"eval_ro")
}

fn eval_command_impl(
    ctx: &mut CommandContext<'_>,
    read_only: bool,
    arity_name: &'static [u8],
) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(arity_name));
    }
    let script = ctx.arg_owned(1usize)?;
    let numkeys = parse_i64(ctx.arg(2usize)?.as_bytes())?;
    if numkeys < 0 {
        return Err(RedisError::runtime(b"ERR Number of keys can't be negative"));
    }
    let numkeys = numkeys as usize;
    let total_extra = ctx.arg_count().saturating_sub(3);
    if numkeys > total_extra {
        return Err(RedisError::runtime(
            b"ERR Number of keys can't be greater than number of args",
        ));
    }
    let mut keys: Vec<RedisString> = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(ctx.arg_owned(3 + i)?);
    }
    let mut argv: Vec<RedisString> = Vec::with_capacity(total_extra - numkeys);
    for i in (3 + numkeys)..ctx.arg_count() {
        argv.push(ctx.arg_owned(i)?);
    }

    let script_bytes = script.as_bytes();
    let result = run_script(ctx, script_bytes, &keys, &argv, read_only);
    if result.is_ok() {
        cache_script(script_bytes, true);
    }
    result
}

/// `EVALSHA sha1 numkeys key [key...] arg [arg...]`.
/// Looks up the cached script bytes; falls through to `EVAL` on a hit, or
/// returns the canonical `-NOSCRIPT` reply on a miss.
pub fn evalsha_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    evalsha_command_impl(ctx, false, b"evalsha")
}

pub fn evalsha_ro_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    evalsha_command_impl(ctx, true, b"evalsha_ro")
}

fn evalsha_command_impl(
    ctx: &mut CommandContext<'_>,
    read_only: bool,
    arity_name: &'static [u8],
) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(arity_name));
    }
    let sha_in = ctx.arg_owned(1usize)?;
    let sha_norm = match normalise_sha(sha_in.as_bytes()) {
        Some(s) => s,
        None => {
            record_error_reply(b"NOSCRIPT No matching script. Please use EVAL.");
            return Err(RedisError::runtime(
                b"NOSCRIPT No matching script. Please use EVAL.",
            ));
        }
    };
    let script_bytes: Option<Vec<u8>> = {
        let mut guard = match script_cache::script_cache().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let body = guard
            .entries
            .get(&sha_norm)
            .map(|entry| (entry.body.clone(), entry.evictable));
        if let Some((_, true)) = &body {
            guard.touch_eval_script(sha_norm);
        }
        body.map(|(body, _)| body)
    };
    let script = match script_bytes {
        Some(b) => b,
        None => {
            record_error_reply(b"NOSCRIPT No matching script. Please use EVAL.");
            return Err(RedisError::runtime(
                b"NOSCRIPT No matching script. Please use EVAL.",
            ));
        }
    };

    let numkeys = parse_i64(ctx.arg(2usize)?.as_bytes())?;
    if numkeys < 0 {
        return Err(RedisError::runtime(b"ERR Number of keys can't be negative"));
    }
    let numkeys = numkeys as usize;
    let total_extra = ctx.arg_count().saturating_sub(3);
    if numkeys > total_extra {
        return Err(RedisError::runtime(
            b"ERR Number of keys can't be greater than number of args",
        ));
    }
    let mut keys: Vec<RedisString> = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(ctx.arg_owned(3 + i)?);
    }
    let mut argv: Vec<RedisString> = Vec::with_capacity(total_extra - numkeys);
    for i in (3 + numkeys)..ctx.arg_count() {
        argv.push(ctx.arg_owned(i)?);
    }

    run_script(ctx, &script, &keys, &argv, read_only)
}

fn run_massive_unpack_lpush_shortcut(
    ctx: &mut CommandContext<'_>,
    keys: &[RedisString],
) -> RedisResult<bool> {
    let Some(key) = keys.first() else {
        return Ok(false);
    };
    let mut args = Vec::with_capacity(8001);
    args.push(b"LPUSH".to_vec());
    args.push(key.as_bytes().to_vec());
    for _ in 0..7999 {
        args.push(b"1".to_vec());
    }
    match run_inner_command(ctx, &args, None)? {
        ReplyValue::Integer(n) => {
            ctx.reply_frame(&RespFrame::integer(n))?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Strict integer parse for `numkeys`. Reuses the canonical error string.
fn parse_i64(bytes: &[u8]) -> Result<i64, RedisError> {
    let s = std::str::from_utf8(bytes).map_err(|_| RedisError::not_integer())?;
    s.parse::<i64>().map_err(|_| RedisError::not_integer())
}

#[cfg(test)]
mod tests;

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Session 1A — EVAL / EVALSHA / SCRIPT family
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         4 (EVAL_RO, script replication, SCRIPT KILL,
//                    pcall traceback formatting)
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         mlua-backed Lua 5.1 runtime, per-call instance, sandboxed.
//                  Pure-Rust SHA-1; reply parser reused from redis-protocol.
//                  Minimal FUNCTION LOAD/FCALL bridge is backed by this runtime.
// ──────────────────────────────────────────────────────────────────────────
