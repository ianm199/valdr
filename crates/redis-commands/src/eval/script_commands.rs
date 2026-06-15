//! `SCRIPT` subcommand router and script-cache management commands.

use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult};

use super::busy_script::{
    busy_script_error, busy_script_snapshot, clear_busy_script, BusyScriptKind,
};
use super::bytes::ascii_eq_ci;
use super::script_cache::{cache_script, normalise_sha, script_cache};

/// `SCRIPT` subcommand router: LOAD / EXISTS / SHOW / FLUSH / KILL / DEBUG /
/// HELP.
pub fn script_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"script"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();
    if ascii_eq_ci(sub_bytes, b"LOAD") {
        return script_load(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"EXISTS") {
        return script_exists(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"SHOW") {
        return script_show(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"FLUSH") {
        return script_flush(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"KILL") {
        return script_kill(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"DEBUG") {
        return script_debug(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"HELP") {
        return script_help(ctx);
    }
    let mut msg = Vec::with_capacity(64 + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown SCRIPT subcommand or wrong number of arguments for '");
    msg.extend_from_slice(sub_bytes);
    msg.push(b'\'');
    Err(RedisError::runtime(msg))
}

fn script_kill(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"script|kill"));
    }
    match busy_script_snapshot() {
        None => Err(RedisError::runtime(
            b"NOTBUSY No scripts in execution right now.",
        )),
        Some(state) if state.kind != BusyScriptKind::Eval => Err(busy_script_error()),
        Some(state) if state.dirty => Err(RedisError::runtime(
            b"UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.",
        )),
        Some(_) => {
            clear_busy_script();
            ctx.reply_simple_string(b"OK")
        }
    }
}

fn script_debug(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 && ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"script|debug"));
    }
    if ctx.arg_count() == 4 {
        let engine = ctx.arg_owned(3usize)?;
        if !ascii_eq_ci(engine.as_bytes(), b"LUA") {
            return Err(RedisError::runtime(
                format!(
                    "ERR No scripting engine found with name '{}'",
                    String::from_utf8_lossy(engine.as_bytes())
                )
                .into_bytes(),
            ));
        }
    }
    let mode = ctx.arg_owned(2usize)?;
    let mode = mode.as_bytes();
    if ascii_eq_ci(mode, b"NO") || ascii_eq_ci(mode, b"YES") || ascii_eq_ci(mode, b"SYNC") {
        return ctx.reply_simple_string(b"OK");
    }
    Err(RedisError::runtime(b"ERR Use SCRIPT DEBUG YES/SYNC/NO"))
}

fn script_load(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"script|load"));
    }
    let body = ctx.arg_owned(2usize)?;
    let hex = cache_script(body.as_bytes(), false);
    ctx.reply_bulk(&hex)
}

fn script_exists(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"script|exists"));
    }
    let guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let n = ctx.arg_count() - 2;
    ctx.reply_array_header(n as i64)?;
    for i in 0..n {
        let raw = ctx.arg_owned(2 + i)?;
        let exists = normalise_sha(raw.as_bytes())
            .map(|h| guard.entries.contains_key(&h))
            .unwrap_or(false);
        ctx.reply_integer(if exists { 1 } else { 0 })?;
    }
    Ok(())
}

fn script_show(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"script|show"));
    }
    let raw = ctx.arg_owned(2usize)?;
    let Some(sha) = normalise_sha(raw.as_bytes()) else {
        return Err(RedisError::runtime(
            b"NOSCRIPT No matching script. Please use EVAL.",
        ));
    };
    let guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    match guard.entries.get(&sha) {
        Some(script) => ctx.reply_bulk(&script.body),
        None => Err(RedisError::runtime(
            b"NOSCRIPT No matching script. Please use EVAL.",
        )),
    }
}

fn script_flush(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() > 3 {
        return Err(RedisError::wrong_number_of_args(b"script|flush"));
    }
    if ctx.arg_count() == 3 {
        let mode = ctx.arg_owned(2usize)?;
        let b = mode.as_bytes();
        if !ascii_eq_ci(b, b"ASYNC") && !ascii_eq_ci(b, b"SYNC") {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
    let mut guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.entries.clear();
    guard.lru.clear();
    ctx.reply_simple_string(b"OK")
}

fn script_help(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let lines: &[&[u8]] = &[
        b"SCRIPT EXISTS <sha1> [<sha1> ...]",
        b"    Return information about the existence of the scripts in the script cache.",
        b"SCRIPT FLUSH [ASYNC|SYNC]",
        b"    Flush the Lua scripts cache. Very dangerous on replicas.",
        b"SCRIPT LOAD <script>",
        b"    Load a script into the scripts cache without executing it.",
        b"SCRIPT DEBUG YES|SYNC|NO",
        b"    Set the debug mode for subsequent scripts executed by the Lua engine.",
        b"HELP",
        b"    Prints this help.",
    ];
    ctx.reply_array_header(lines.len() as i64)?;
    for ln in lines {
        ctx.reply_bulk(ln)?;
    }
    Ok(())
}
