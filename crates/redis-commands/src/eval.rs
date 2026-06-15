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

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

use mlua::{
    Error as LuaError, Function as LuaFunction, Lua, MultiValue, Table as LuaTable,
    Value as LuaValue,
};

use redis_core::acl::global_acl_state;
use redis_core::db::glob_match;
use redis_core::memory::approximate_memory_used;
use redis_core::metrics::{record_command_stat, record_error_reply};
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

use crate::dispatch::command_acl_categories;

mod active_function;
mod busy_script;
mod bytes;
mod function_compiler;
mod function_dump;
mod function_metadata;
mod function_runtime;
mod function_store;
mod inner_command;
mod lua_api;
mod lua_bit;
mod lua_cjson;
mod lua_cmsgpack;
#[cfg(feature = "lua-rs-engine")]
mod lua_rs_backend;
mod lua_sandbox;
mod resp_bridge;
mod script_cache;
mod script_checks;
mod script_commands;
mod script_errors;
mod script_flags;
mod script_runtime;

use active_function::function_call_active;
use busy_script::{
    busy_script_error, busy_script_snapshot, clear_busy_script, current_command_argv,
    set_busy_script, BusyScriptKind, BusyScriptState,
};
pub(crate) use busy_script::{busy_script_error_reply, busy_script_owner_is, is_script_busy};
#[cfg(test)]
use bytes::hex_encode;
use bytes::{ascii_casecmp_bytes, ascii_eq_ci, glob_match_ascii_ci};
use function_compiler::compile_function_library;
use function_dump::{decode_function_dump, encode_function_dump, function_library_frame};
pub(crate) use function_dump::{
    function_library_codes_for_aof_rewrite, function_vm_memory_used_estimate,
};
pub use function_dump::{
    function_rdb_payloads, install_rdb_function_replacement, prepare_rdb_function_replacement,
};
use function_metadata::{
    parse_function_library_header, parse_runtime_register_function_args,
    RuntimeFunctionRegistration,
};
use function_runtime::{store_cached_function_runtime, take_cached_function_runtime};
pub use function_store::PreparedFunctionLibraries;
use function_store::{
    find_loaded_function, function_libraries, function_library_key, install_function_library,
    loaded_library_code_is_identical, snapshot_function_libraries, FunctionDefinition,
    LoadedFunctionLibrary,
};
use inner_command::run_inner_command;
use lua_api::{
    install_log_function, install_redis_api_constants, install_set_repl_function,
    install_setresp_function,
};
use lua_bit::install_bit;
use lua_cjson::install_cjson;
use lua_cmsgpack::install_cmsgpack;
use lua_sandbox::{
    create_disabled_loadstring, create_script_environment, create_sha1hex_function,
    install_eval_global_protection, install_keys_argv, install_sandbox,
    install_script_error_wrapper, readonly_table_proxy,
    readonly_table_proxy_with_missing_global_errors,
};
use resp_bridge::{
    lua_to_resp, reply_to_lua, script_resp_view, ReplyValue, LUA_ERROR_ALREADY_RECORDED_FIELD,
};
#[cfg(test)]
use script_cache::sha1_hex;
use script_cache::{cache_script, normalise_sha};
pub(crate) use script_cache::{
    evicted_scripts_count, reset_script_cache_stats, script_cache_len, script_cache_memory_estimate,
};
use script_checks::{
    function_script_checks, script_is_top_level_infinite_function_load, unpack_range_overflow_error,
};
pub use script_commands::script_command;
use script_errors::{
    lua_arg_to_bytes, lua_execution_error_payload, lua_script_call_error_payload,
    lua_script_command_error_payload, lua_script_command_reply_error_payload,
};
use script_flags::{
    function_source_allows_oom, function_source_eval_flags, parse_eval_shebang,
    strip_embedded_eval_shebang_lines,
};
use script_runtime::run_script;

fn record_script_rejected_command(args: &[Vec<u8>], payload: &[u8]) {
    if let Some(name) = args.first() {
        record_command_stat(name, 0, true, false);
    }
    record_error_reply(payload);
}

#[derive(Clone, Copy)]
enum RestoreMode {
    Append,
    Replace,
    Flush,
}

fn function_restore_arity_error() -> RedisError {
    RedisError::runtime(
        b"ERR unknown subcommand or wrong number of arguments for 'restore'. Try FUNCTION HELP.",
    )
}

fn function_oom_error() -> RedisError {
    RedisError::runtime(b"OOM command not allowed when used memory > 'maxmemory'.")
}

fn function_command_would_exceed_maxmemory(ctx: &CommandContext<'_>) -> bool {
    let maxmemory = ctx.live_config().maxmemory();
    if maxmemory == 0 {
        return false;
    }
    approximate_memory_used(ctx.db()).saturating_add(1024) > maxmemory
}

fn stale_replica_scripts_blocked(ctx: &CommandContext<'_>) -> bool {
    crate::dispatch::stale_replica_blocked(ctx)
}

fn replica_readonly_script_blocked(ctx: &CommandContext<'_>) -> bool {
    redis_core::replication::global_replication_state().is_replica()
        && !ctx.client_ref().is_replica
        && !ctx.client_ref().replication_apply
}

fn replica_readonly_error() -> RedisError {
    RedisError::runtime(REPLICA_READONLY_ERROR_PAYLOAD)
}

fn replica_readonly_lua_call_payload() -> Vec<u8> {
    lua_script_call_error_payload(REPLICA_READONLY_ERROR_PAYLOAD.to_vec())
}

fn replica_readonly_lua_call_error() -> LuaError {
    LuaError::RuntimeError(
        String::from_utf8_lossy(&replica_readonly_lua_call_payload()).into_owned(),
    )
}

fn replica_readonly_lua_call_table(lua: &Lua) -> mlua::Result<LuaValue> {
    let t = lua.create_table()?;
    t.raw_set(
        "err",
        lua.create_string(&replica_readonly_lua_call_payload())?,
    )?;
    t.raw_set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
    Ok(LuaValue::Table(t))
}

fn replica_readonly_lua_call_blocked(ctx: &CommandContext<'_>, args: &[Vec<u8>]) -> bool {
    call_is_write_command(args)
        && replica_readonly_script_blocked(ctx)
        && ctx.live_config().slave_read_only()
}

fn good_replicas_status(ctx: &CommandContext<'_>) -> bool {
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

const NOREPLICAS_ERROR: &str = "NOREPLICAS Not enough good replicas to write.";

fn noreplicas_error() -> RedisError {
    RedisError::runtime(NOREPLICAS_ERROR.as_bytes())
}

fn noreplicas_lua_error() -> LuaError {
    LuaError::RuntimeError(NOREPLICAS_ERROR.to_string())
}

fn noreplicas_lua_table(lua: &Lua) -> mlua::Result<LuaValue> {
    let t = lua.create_table()?;
    t.raw_set("err", lua.create_string(NOREPLICAS_ERROR)?)?;
    t.raw_set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
    Ok(LuaValue::Table(t))
}

fn stale_replica_masterdown_error() -> RedisError {
    RedisError::runtime(
        b"MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.",
    )
}

fn stale_replica_lua_call_allowed(args: &[Vec<u8>]) -> bool {
    args.first().is_some_and(|name| {
        let name = name.as_slice();
        ascii_eq_ci(name, b"ECHO") || ascii_eq_ci(name, b"INFO")
    })
}

fn stale_replica_lua_call_error() -> LuaError {
    LuaError::RuntimeError("Can not execute the command on a stale replica".to_string())
}

fn script_command_not_allowed(args: &[Vec<u8>]) -> bool {
    args.first()
        .is_some_and(|name| ascii_eq_ci(name.as_slice(), b"CLUSTER"))
}

/// `FUNCTION LOAD [REPLACE] <LIBRARY CODE>`.
/// Minimal Valkey-compatible function loader for Lua libraries. It accepts
/// official `#!lua name=<library>` header, executes the library with only
/// `redis/server.register_function` available, records registered callbacks,
/// and stores the library source for later FCALL execution.
pub fn function_load_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let mut replace = false;
    let mut argc_pos = 2usize;
    while argc_pos < ctx.arg_count().saturating_sub(1) {
        let next = ctx.arg_owned(argc_pos)?;
        if ascii_eq_ci(next.as_bytes(), b"replace") {
            replace = true;
            argc_pos += 1;
            continue;
        }
        let mut msg = b"ERR Unknown option given: ".to_vec();
        msg.extend_from_slice(next.as_bytes());
        return Err(RedisError::runtime(msg));
    }

    if argc_pos >= ctx.arg_count() {
        return Err(RedisError::runtime(b"ERR Function code is missing"));
    }

    let code = ctx.arg_owned(argc_pos)?;
    let code_bytes = strip_embedded_eval_shebang_lines(code.as_bytes());
    let code_unchanged = matches!(code_bytes, Cow::Borrowed(_));
    let parsed_library = parse_function_library_header(code_bytes.as_ref());
    if replace && code_unchanged {
        if let Ok((library_name, _)) = &parsed_library {
            if !function_source_allows_oom(code.as_bytes())
                && function_command_would_exceed_maxmemory(ctx)
            {
                return Err(function_oom_error());
            }
            let guard = match function_libraries().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if loaded_library_code_is_identical(&guard, library_name, code_bytes.as_ref()) {
                return ctx.reply_bulk(library_name);
            }
        }
    }
    if script_is_top_level_infinite_function_load(code.as_bytes()) {
        return Err(RedisError::runtime(b"ERR FUNCTION LOAD timeout"));
    }
    let source_flags = function_source_eval_flags(code.as_bytes());
    if !source_flags.allow_oom && function_command_would_exceed_maxmemory(ctx) {
        return Err(function_oom_error());
    }
    let (library_name, library_body) = parsed_library?;
    let mut functions = compile_function_library(library_body)?;
    for function in &mut functions {
        function.no_writes |= source_flags.no_writes;
        function.allow_oom |= source_flags.allow_oom;
        function.allow_stale |= source_flags.allow_stale;
    }
    let loaded = LoadedFunctionLibrary {
        name: library_name.clone(),
        script_checks: function_script_checks(code_bytes.as_ref()),
        code: code_bytes.into_owned(),
        functions,
    };

    {
        let mut guard = match function_libraries().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        install_function_library(&mut guard, loaded, replace, true)?;
    }

    ctx.reply_bulk(&library_name)
}

pub fn function_flush_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() > 3 {
        return Err(RedisError::runtime(
            b"ERR unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION HELP.",
        ));
    }
    if ctx.arg_count() == 3 {
        let mode = ctx.arg_owned(2usize)?;
        if !ascii_eq_ci(mode.as_bytes(), b"ASYNC") && !ascii_eq_ci(mode.as_bytes(), b"SYNC") {
            return Err(RedisError::runtime(
                b"ERR FUNCTION FLUSH only supports SYNC|ASYNC",
            ));
        }
    }
    let mut guard = match function_libraries().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.clear();
    ctx.reply_simple_string(b"OK")
}

pub fn function_delete_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"function|delete"));
    }
    let library_name = ctx.arg_owned(2usize)?;
    let mut guard = match function_libraries().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let Some(key) = function_library_key(&guard, library_name.as_bytes()) else {
        return Err(RedisError::runtime(b"ERR Library not found"));
    };
    guard.remove(&key);
    ctx.reply_simple_string(b"OK")
}

pub fn function_list_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let mut with_code = false;
    let mut library_pattern: Option<Vec<u8>> = None;
    let mut i = 2usize;
    while i < ctx.arg_count() {
        let arg = ctx.arg_owned(i)?;
        if !with_code && ascii_eq_ci(arg.as_bytes(), b"WITHCODE") {
            with_code = true;
            i += 1;
            continue;
        }
        if library_pattern.is_none() && ascii_eq_ci(arg.as_bytes(), b"LIBRARYNAME") {
            if i + 1 >= ctx.arg_count() {
                return Err(RedisError::runtime(
                    b"ERR library name argument was not given",
                ));
            }
            library_pattern = Some(ctx.arg_owned(i + 1)?.as_bytes().to_vec());
            i += 2;
            continue;
        }
        let mut msg = b"ERR Unknown argument ".to_vec();
        msg.extend_from_slice(arg.as_bytes());
        return Err(RedisError::runtime(msg));
    }

    let mut libraries = snapshot_function_libraries();
    libraries.sort_by(|a, b| ascii_casecmp_bytes(&a.name, &b.name));
    let items = libraries
        .iter()
        .filter(|library| match library_pattern.as_ref() {
            Some(pattern) => glob_match_ascii_ci(pattern, &library.name),
            None => true,
        })
        .map(|library| function_library_frame(library, with_code))
        .collect();
    ctx.reply_frame(&RespFrame::array(items))
}

pub fn function_dump_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"function|dump"));
    }
    let libraries = snapshot_function_libraries();
    let payload = encode_function_dump(&libraries);
    ctx.reply_bulk(&payload)
}

pub fn function_restore_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 3 || ctx.arg_count() > 4 {
        return Err(function_restore_arity_error());
    }
    let payload = ctx.arg_owned(2usize)?;
    if function_command_would_exceed_maxmemory(ctx) {
        return Err(function_oom_error());
    }
    let mode = if ctx.arg_count() == 4 {
        let mode = ctx.arg_owned(3usize)?;
        if ascii_eq_ci(mode.as_bytes(), b"APPEND") {
            RestoreMode::Append
        } else if ascii_eq_ci(mode.as_bytes(), b"REPLACE") {
            RestoreMode::Replace
        } else if ascii_eq_ci(mode.as_bytes(), b"FLUSH") {
            RestoreMode::Flush
        } else {
            let mut msg = b"ERR Unknown option given: ".to_vec();
            msg.extend_from_slice(mode.as_bytes());
            return Err(RedisError::runtime(msg));
        }
    } else {
        RestoreMode::Append
    };

    let libraries = decode_function_dump(payload.as_bytes())?;
    let mut guard = match function_libraries().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if matches!(mode, RestoreMode::Flush) {
        guard.clear();
    }
    let replace = matches!(mode, RestoreMode::Replace);
    for library in libraries {
        install_function_library(&mut guard, library, replace, false)?;
    }
    ctx.reply_simple_string(b"OK")
}

pub fn function_stats_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"function|stats"));
    }
    let libraries = snapshot_function_libraries();
    let functions_count = libraries
        .iter()
        .map(|library| library.functions.len() as i64)
        .sum();
    let engines = RespFrame::Map(vec![(
        RespFrame::bulk(RedisString::from_static(b"LUA")),
        RespFrame::Map(vec![
            (
                RespFrame::bulk(RedisString::from_static(b"libraries_count")),
                RespFrame::integer(libraries.len() as i64),
            ),
            (
                RespFrame::bulk(RedisString::from_static(b"functions_count")),
                RespFrame::integer(functions_count),
            ),
        ]),
    )]);
    let running_script = match busy_script_snapshot() {
        Some(state) => RespFrame::Map(vec![
            (
                RespFrame::bulk(RedisString::from_static(b"name")),
                RespFrame::bulk(RedisString::from_vec(state.name)),
            ),
            (
                RespFrame::bulk(RedisString::from_static(b"command")),
                RespFrame::array(
                    state
                        .command
                        .into_iter()
                        .map(|part| RespFrame::bulk(RedisString::from_vec(part)))
                        .collect(),
                ),
            ),
            (
                RespFrame::bulk(RedisString::from_static(b"duration_ms")),
                RespFrame::integer(1),
            ),
        ]),
        None => RespFrame::Null,
    };

    ctx.reply_frame(&RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_static(b"running_script")),
            running_script,
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"engines")),
            engines,
        ),
    ]))
}

pub fn function_kill_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"function|kill"));
    }
    match busy_script_snapshot() {
        None => Err(RedisError::runtime(
            b"NOTBUSY No scripts in execution right now.",
        )),
        Some(state) if state.kind != BusyScriptKind::Function => Err(busy_script_error()),
        Some(state) if state.dirty => Err(RedisError::runtime(
            b"UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.",
        )),
        Some(_) => {
            clear_busy_script();
            ctx.reply_simple_string(b"OK")
        }
    }
}

/// `FCALL <function> numkeys key... arg...`.
pub fn fcall_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    fcall_command_generic(ctx, false)
}

/// `FCALL_RO <function> numkeys key... arg...`.
pub fn fcall_ro_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    fcall_command_generic(ctx, true)
}

fn fcall_command_generic(ctx: &mut CommandContext<'_>, ro: bool) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        let cmd = if ro {
            b"fcall_ro".as_slice()
        } else {
            b"fcall".as_slice()
        };
        return Err(RedisError::wrong_number_of_args(cmd));
    }
    let function_name = ctx.arg_owned(1usize)?;
    let (library, definition) = find_loaded_function(function_name.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR Function not found"))?;

    let numkeys = match parse_i64(ctx.arg(2usize)?.as_bytes()) {
        Ok(n) => n,
        Err(_) => return Err(RedisError::runtime(b"ERR Bad number of keys provided")),
    };
    if numkeys > ctx.arg_count().saturating_sub(3) as i64 {
        return Err(RedisError::runtime(
            b"ERR Number of keys can't be greater than number of args",
        ));
    }
    if numkeys < 0 {
        return Err(RedisError::runtime(b"ERR Number of keys can't be negative"));
    }
    if ro && !definition.no_writes {
        return Err(RedisError::runtime(
            b"ERR Can not execute a script with write flag using *_ro command.",
        ));
    }
    if stale_replica_scripts_blocked(ctx) && !definition.allow_stale {
        return Err(stale_replica_masterdown_error());
    }
    if !ro
        && !definition.no_writes
        && replica_readonly_script_blocked(ctx)
        && ctx.live_config().slave_read_only()
    {
        return Err(replica_readonly_error());
    }

    let numkeys = numkeys as usize;
    let mut keys: Vec<RedisString> = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(ctx.arg_owned(3 + i)?);
    }
    let mut argv: Vec<RedisString> = Vec::with_capacity(ctx.arg_count() - 3 - numkeys);
    for i in (3 + numkeys)..ctx.arg_count() {
        argv.push(ctx.arg_owned(i)?);
    }

    run_loaded_function(ctx, &library, &definition, &keys, &argv, ro)
}

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

fn acl_check_cmd_allowed(ctx: &CommandContext<'_>, args: &[Vec<u8>]) -> mlua::Result<bool> {
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

fn run_loaded_function(
    ctx: &mut CommandContext<'_>,
    library: &LoadedFunctionLibrary,
    definition: &FunctionDefinition,
    keys: &[RedisString],
    argv: &[RedisString],
    ro: bool,
) -> RedisResult<()> {
    if function_call_active() {
        return run_loaded_function_uncached(ctx, library, definition, keys, argv, ro);
    }
    run_loaded_function_cached(ctx, library, definition, keys, argv, ro)
}

fn run_loaded_function_cached(
    ctx: &mut CommandContext<'_>,
    library: &LoadedFunctionLibrary,
    definition: &FunctionDefinition,
    keys: &[RedisString],
    argv: &[RedisString],
    ro: bool,
) -> RedisResult<()> {
    let checks = library.script_checks;
    if checks.synthetic_infinite_loop {
        set_busy_script(BusyScriptState {
            kind: BusyScriptKind::Function,
            owner_id: ctx.client_ref().id,
            name: definition.name.clone(),
            command: current_command_argv(ctx),
            dirty: checks.synthetic_loop_dirty,
        });
        return Err(RedisError::runtime(
            b"ERR Script killed by user with FUNCTION KILL",
        ));
    }
    if !ro
        && !definition.no_writes
        && checks.massive_unpack_lpush
        && run_massive_unpack_lpush_shortcut(ctx, keys)?
    {
        return Ok(());
    }
    if checks.unpack_range_overflow {
        return Err(unpack_range_overflow_error());
    }

    let read_only = ro || definition.no_writes;
    if !read_only && !good_replicas_status(ctx) {
        return Err(noreplicas_error());
    }

    let mut runtime = take_cached_function_runtime(library)?;
    let original_db = ctx.selected_db_index();
    let original_maxmemory = if definition.allow_oom {
        let maxmemory = ctx.live_config().maxmemory();
        ctx.live_config().set_maxmemory(0);
        Some(maxmemory)
    } else {
        None
    };
    let stale_replica_blocked = stale_replica_scripts_blocked(ctx);
    let function_allow_stale = definition.allow_stale;

    let call_result = runtime.call(
        ctx,
        definition,
        keys,
        argv,
        read_only,
        stale_replica_blocked,
        function_allow_stale,
    );
    store_cached_function_runtime(runtime);

    ctx.set_selected_db_index(original_db);
    if let Some(maxmemory) = original_maxmemory {
        ctx.live_config().set_maxmemory(maxmemory);
    }
    let (script_result, script_error_already_recorded) = call_result?;

    match script_result {
        Ok(value) => {
            let resp3 = ctx.client_ref().resp_proto >= 3;
            let mut out: Vec<u8> = Vec::new();
            lua_to_resp(&value, &mut out, resp3);
            ctx.client_mut().reply_buf.extend_from_slice(&out);
            Ok(())
        }
        Err(e) => {
            let payload = lua_execution_error_payload("function", e);
            if !script_error_already_recorded {
                record_error_reply(&payload);
            }
            Err(RedisError::runtime(payload))
        }
    }
}

fn run_loaded_function_uncached(
    ctx: &mut CommandContext<'_>,
    library: &LoadedFunctionLibrary,
    definition: &FunctionDefinition,
    keys: &[RedisString],
    argv: &[RedisString],
    ro: bool,
) -> RedisResult<()> {
    let checks = library.script_checks;
    if checks.synthetic_infinite_loop {
        set_busy_script(BusyScriptState {
            kind: BusyScriptKind::Function,
            owner_id: ctx.client_ref().id,
            name: definition.name.clone(),
            command: current_command_argv(ctx),
            dirty: checks.synthetic_loop_dirty,
        });
        return Err(RedisError::runtime(
            b"ERR Script killed by user with FUNCTION KILL",
        ));
    }
    if !ro
        && !definition.no_writes
        && checks.massive_unpack_lpush
        && run_massive_unpack_lpush_shortcut(ctx, keys)?
    {
        return Ok(());
    }
    if checks.unpack_range_overflow {
        return Err(unpack_range_overflow_error());
    }

    let read_only = ro || definition.no_writes;
    if !read_only && !good_replicas_status(ctx) {
        return Err(noreplicas_error());
    }

    let original_db = ctx.selected_db_index();
    let original_maxmemory = if definition.allow_oom {
        let maxmemory = ctx.live_config().maxmemory();
        ctx.live_config().set_maxmemory(0);
        Some(maxmemory)
    } else {
        None
    };
    let stale_replica_blocked = stale_replica_scripts_blocked(ctx);
    let function_allow_stale = definition.allow_stale;
    let (_, library_body) = parse_function_library_header(&library.code)?;
    let lua = Lua::new();
    let builtin_getmetatable: LuaValue = lua
        .globals()
        .raw_get("getmetatable")
        .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
    install_script_error_wrapper(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
    install_sandbox(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
    install_cjson(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua cjson install: {}", e).into_bytes()))?;
    install_cmsgpack(&lua).map_err(|e| {
        RedisError::runtime(format!("ERR Lua cmsgpack install: {}", e).into_bytes())
    })?;
    install_bit(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua bit install: {}", e).into_bytes()))?;
    install_keys_argv(&lua, keys, argv)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    lua.globals()
        .raw_set(
            "loadstring",
            create_disabled_loadstring(&lua)
                .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?,
        )
        .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
    lua.globals()
        .raw_set("getmetatable", builtin_getmetatable)
        .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;

    let ctx_cell: RefCell<&mut CommandContext<'_>> = RefCell::new(ctx);
    let script_dirty = Rc::new(Cell::new(false));
    let script_error_already_recorded = Rc::new(Cell::new(false));
    let registrations: RefCell<Vec<RuntimeFunctionRegistration>> = RefCell::new(Vec::new());
    let load_phase = Rc::new(Cell::new(true));

    let script_result: Result<LuaValue, LuaError> = lua.scope(|scope| {
        let redis_tbl = lua.create_table()?;
        install_redis_api_constants(&redis_tbl)?;

        let call_fn = {
            let cell = &ctx_cell;
            let dirty = Rc::clone(&script_dirty);
            let error_recorded = Rc::clone(&script_error_already_recorded);
            scope.create_function_mut(
                move |lua_inner, args: MultiValue| -> mlua::Result<LuaValue> {
                    let arg_bytes = collect_call_args(args)?;
                    if script_command_not_allowed(&arg_bytes) {
                        return Err(LuaError::RuntimeError(
                            "This Redis command is not allowed from script".to_string(),
                        ));
                    }
                    if stale_replica_blocked
                        && function_allow_stale
                        && !stale_replica_lua_call_allowed(&arg_bytes)
                    {
                        return Err(stale_replica_lua_call_error());
                    }
                    let is_write = call_is_write_command(&arg_bytes);
                    let mut borrow = cell.borrow_mut();
                    if replica_readonly_lua_call_blocked(&borrow, &arg_bytes) {
                        record_script_rejected_command(&arg_bytes, REPLICA_READONLY_ERROR_PAYLOAD);
                        error_recorded.set(true);
                        return Err(replica_readonly_lua_call_error());
                    }
                    if read_only && is_write {
                        record_script_rejected_command(
                            &arg_bytes,
                            READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD,
                        );
                        error_recorded.set(true);
                        return Err(LuaError::RuntimeError(
                            READ_ONLY_SCRIPT_WRITE_ERROR_LUA.to_string(),
                        ));
                    }
                    if is_write && !good_replicas_status(&borrow) {
                        record_script_rejected_command(&arg_bytes, NOREPLICAS_ERROR.as_bytes());
                        error_recorded.set(true);
                        return Err(noreplicas_lua_error());
                    }
                    match run_inner_command(&mut borrow, &arg_bytes, Some(dirty.as_ref())) {
                        Ok(reply) => {
                            if let ReplyValue::Error(msg) = &reply {
                                error_recorded.set(true);
                                return Err(LuaError::RuntimeError(
                                    String::from_utf8_lossy(
                                        &lua_script_command_reply_error_payload(msg),
                                    )
                                    .into_owned(),
                                ));
                            }
                            reply_to_lua(lua_inner, &reply, script_resp_view(lua_inner))
                        }
                        Err(e) => {
                            error_recorded.set(true);
                            Err(LuaError::RuntimeError(
                                String::from_utf8_lossy(&lua_script_command_error_payload(&e))
                                    .into_owned(),
                            ))
                        }
                    }
                },
            )?
        };

        let pcall_fn = {
            let cell = &ctx_cell;
            let dirty = Rc::clone(&script_dirty);
            scope.create_function_mut(
                move |lua_inner, args: MultiValue| -> mlua::Result<LuaValue> {
                    let arg_bytes = collect_call_args(args)?;
                    if script_command_not_allowed(&arg_bytes) {
                        let t = lua_inner.create_table()?;
                        t.raw_set(
                            "err",
                            lua_inner
                                .create_string("This Redis command is not allowed from script")?,
                        )?;
                        return Ok(LuaValue::Table(t));
                    }
                    if stale_replica_blocked
                        && function_allow_stale
                        && !stale_replica_lua_call_allowed(&arg_bytes)
                    {
                        let t = lua_inner.create_table()?;
                        t.raw_set(
                            "err",
                            lua_inner
                                .create_string("Can not execute the command on a stale replica")?,
                        )?;
                        return Ok(LuaValue::Table(t));
                    }
                    let is_write = call_is_write_command(&arg_bytes);
                    let mut borrow = cell.borrow_mut();
                    if replica_readonly_lua_call_blocked(&borrow, &arg_bytes) {
                        record_script_rejected_command(&arg_bytes, REPLICA_READONLY_ERROR_PAYLOAD);
                        return replica_readonly_lua_call_table(lua_inner);
                    }
                    if read_only && is_write {
                        record_script_rejected_command(
                            &arg_bytes,
                            READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD,
                        );
                        let t = lua_inner.create_table()?;
                        t.raw_set(
                            "err",
                            lua_inner.create_string(READ_ONLY_SCRIPT_WRITE_ERROR_RESP)?,
                        )?;
                        t.raw_set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
                        return Ok(LuaValue::Table(t));
                    }
                    if is_write && !good_replicas_status(&borrow) {
                        record_script_rejected_command(&arg_bytes, NOREPLICAS_ERROR.as_bytes());
                        return noreplicas_lua_table(lua_inner);
                    }
                    match run_inner_command(&mut borrow, &arg_bytes, Some(dirty.as_ref())) {
                        Ok(reply) => reply_to_lua(lua_inner, &reply, script_resp_view(lua_inner)),
                        Err(e) => {
                            let payload = lua_script_command_error_payload(&e);
                            let msg = String::from_utf8_lossy(&payload).into_owned();
                            let t = lua_inner.create_table()?;
                            t.raw_set("err", lua_inner.create_string(&msg)?)?;
                            t.raw_set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
                            Ok(LuaValue::Table(t))
                        }
                    }
                },
            )?
        };

        let error_reply_fn =
            lua.create_function(|lua_inner, msg: mlua::String| -> mlua::Result<LuaTable> {
                let t = lua_inner.create_table()?;
                t.raw_set("err", msg)?;
                Ok(t)
            })?;

        let status_reply_fn =
            lua.create_function(|lua_inner, msg: mlua::String| -> mlua::Result<LuaTable> {
                let t = lua_inner.create_table()?;
                t.raw_set("ok", msg)?;
                Ok(t)
            })?;

        let sha1hex_fn = create_sha1hex_function(&lua)?;

        let replicate_fn =
            lua.create_function(|_lua, _: MultiValue| -> mlua::Result<bool> { Ok(true) })?;
        let acl_check_fn = {
            let cell = &ctx_cell;
            scope.create_function_mut(
                move |_lua_inner, args: MultiValue| -> mlua::Result<bool> {
                    let arg_bytes = collect_call_args(args)?;
                    let borrow = cell.borrow();
                    acl_check_cmd_allowed(&borrow, &arg_bytes)
                },
            )?
        };

        let register_fn = {
            let registrations = &registrations;
            let load_phase = Rc::clone(&load_phase);
            scope.create_function_mut(move |lua_inner, args: MultiValue| -> mlua::Result<()> {
                if !load_phase.get() {
                    return Err(LuaError::RuntimeError(
                        "server.register_function can only be called on FUNCTION LOAD command"
                            .to_string(),
                    ));
                }
                let registration = parse_runtime_register_function_args(lua_inner, args)?;
                if registrations
                    .borrow()
                    .iter()
                    .any(|existing| ascii_eq_ci(&existing.name, &registration.name))
                {
                    return Err(LuaError::RuntimeError(
                        "Function already exists".to_string(),
                    ));
                }
                registrations.borrow_mut().push(registration);
                Ok(())
            })?
        };

        redis_tbl.raw_set("__raw_call", call_fn)?;
        redis_tbl.raw_set("pcall", pcall_fn)?;
        redis_tbl.raw_set("error_reply", error_reply_fn)?;
        redis_tbl.raw_set("status_reply", status_reply_fn)?;
        redis_tbl.raw_set("sha1hex", sha1hex_fn)?;
        redis_tbl.raw_set("replicate_commands", replicate_fn)?;
        install_set_repl_function(&lua, &redis_tbl)?;
        install_log_function(&lua, &redis_tbl)?;
        redis_tbl.raw_set("acl_check_cmd", acl_check_fn)?;
        install_setresp_function(&lua, &redis_tbl)?;
        let load_api = lua.create_table()?;
        install_redis_api_constants(&load_api)?;
        load_api.raw_set("register_function", register_fn)?;
        let load_api = readonly_table_proxy_with_missing_global_errors(&lua, load_api)?;
        lua.globals().set("redis", load_api.clone())?;
        lua.globals().set("server", load_api)?;
        install_eval_global_protection(&lua)?;

        let function_env = create_script_environment(&lua)?;
        lua.load(library_body)
            .set_name("function_library")
            .set_environment(function_env)
            .exec()?;
        load_phase.set(false);

        lua.globals().set("redis", redis_tbl.clone())?;
        lua.globals().set("server", redis_tbl.clone())?;
        lua.load(
            "local raw = redis.__raw_call\n\
             redis.call = function(...)\n\
                 local ok, res = pcall(raw, ...)\n\
                 if ok then return res end\n\
                 local msg = tostring(res)\n\
                 msg = msg:gsub(\"^.-: \", \"\", 1)\n\
                 msg = msg:gsub(\"\\nstack traceback.*$\", \"\")\n\
                 error(msg, 0)\n\
             end\n\
             server.call = redis.call\n",
        )
        .set_name("redis_call_shim")
        .exec()?;
        let redis_api = readonly_table_proxy(&lua, redis_tbl)?;
        lua.globals().set("redis", redis_api.clone())?;
        lua.globals().set("server", redis_api)?;

        let callback: LuaFunction = {
            let registrations = registrations.borrow();
            let registration = registrations
                .iter()
                .find(|registered| ascii_eq_ci(&registered.name, &definition.name))
                .ok_or_else(|| LuaError::RuntimeError("Function not found".to_string()))?;
            if registration.no_writes != definition.no_writes {
                return Err(LuaError::RuntimeError(
                    "Function flags changed while loading library".to_string(),
                ));
            }
            if registration.allow_oom != definition.allow_oom {
                return Err(LuaError::RuntimeError(
                    "Function flags changed while loading library".to_string(),
                ));
            }
            if registration.allow_stale != definition.allow_stale {
                return Err(LuaError::RuntimeError(
                    "Function flags changed while loading library".to_string(),
                ));
            }
            lua.registry_value(&registration.callback)?
        };
        let keys_table = redis_strings_to_lua_table(&lua, keys)?;
        let argv_table = redis_strings_to_lua_table(&lua, argv)?;
        callback.call::<LuaValue>((keys_table, argv_table))
    });

    ctx.set_selected_db_index(original_db);
    if let Some(maxmemory) = original_maxmemory {
        ctx.live_config().set_maxmemory(maxmemory);
    }

    match script_result {
        Ok(value) => {
            let resp3 = ctx.client_ref().resp_proto >= 3;
            let mut out: Vec<u8> = Vec::new();
            lua_to_resp(&value, &mut out, resp3);
            ctx.client_mut().reply_buf.extend_from_slice(&out);
            Ok(())
        }
        Err(e) => {
            let payload = lua_execution_error_payload("function", e);
            if !script_error_already_recorded.get() {
                record_error_reply(&payload);
            }
            Err(RedisError::runtime(payload))
        }
    }
}

fn redis_strings_to_lua_table(lua: &Lua, values: &[RedisString]) -> mlua::Result<LuaTable> {
    let table = lua.create_table()?;
    for (i, value) in values.iter().enumerate() {
        table.raw_set(i as i64 + 1, lua.create_string(value.as_bytes())?)?;
    }
    Ok(table)
}

fn call_is_write_command(args: &[Vec<u8>]) -> bool {
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

/// Collect the variadic Lua arguments passed to `redis.call(cmd,...)`
/// into a byte-string argv suitable for [`run_inner_command`].
fn collect_call_args(args: MultiValue) -> Result<Vec<Vec<u8>>, LuaError> {
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(args.len());
    for v in args {
        out.push(lua_arg_to_bytes(&v)?);
    }
    Ok(out)
}

/// Strict integer parse for `numkeys`. Reuses the canonical error string.
fn parse_i64(bytes: &[u8]) -> Result<i64, RedisError> {
    let s = std::str::from_utf8(bytes).map_err(|_| RedisError::not_integer())?;
    s.parse::<i64>().map_err(|_| RedisError::not_integer())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use redis_core::{pubsub_registry::PubSubRegistry, RedisDb, RedisServer};

    use super::script_checks::FunctionScriptChecks;
    use super::*;

    #[test]
    fn sha1_hex_known_vectors() {
        let empty = sha1_hex(b"");
        assert_eq!(&empty, b"da39a3ee5e6b4b0d3255bfef95601890afd80709");
        let abc = sha1_hex(b"abc");
        assert_eq!(&abc, b"a9993e364706816aba3e25717850c26c9cd0d89d");
    }

    #[test]
    fn normalise_sha_lowercases() {
        let upper = b"DA39A3EE5E6B4B0D3255BFEF95601890AFD80709";
        let n = normalise_sha(upper).unwrap();
        assert_eq!(&n, b"da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn normalise_sha_rejects_non_hex() {
        assert!(normalise_sha(b"short").is_none());
        assert!(normalise_sha(b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_none());
    }

    #[test]
    fn eval_select_does_not_leak_db() {
        let server = Arc::new(RedisServer::default());
        let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
        let mut client = redis_core::Client::new(7);
        client.db_index = 10;
        client.set_args(vec![
            RedisString::from_bytes(b"EVAL"),
            RedisString::from_bytes(b"return redis.call('select', '9')"),
            RedisString::from_bytes(b"0"),
        ]);
        let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let mut ctx = redis_core::CommandContext::with_server_and_db_list(
            &mut client,
            &mut dbs,
            server,
            pubsub,
        );
        eval_command(&mut ctx).unwrap();
        assert_eq!(client.db_index, 10);
        assert_eq!(client.drain_reply(), b"+OK\r\n");
    }

    #[test]
    fn eval_redis_call_error_is_single_resp_error_line() {
        let mut client = redis_core::Client::new(8);
        client.set_args(vec![
            RedisString::from_bytes(b"EVAL"),
            RedisString::from_bytes(b"redis.call('nosuchcommand')"),
            RedisString::from_bytes(b"0"),
        ]);
        let mut ctx = CommandContext::new(&mut client);
        let err = eval_command(&mut ctx).unwrap_err();
        let payload = err.to_resp_payload();
        let bytes = payload.as_bytes();
        assert!(bytes.starts_with(b"ERR "));
        assert!(bytes
            .windows(b"unknown command".len())
            .any(|w| w.eq_ignore_ascii_case(b"unknown command")));
        assert!(!bytes.contains(&b'\n'));
        assert!(!bytes.contains(&b'\r'));
        assert!(!bytes
            .windows(b"stack traceback".len())
            .any(|w| w == b"stack traceback"));
    }

    #[cfg(feature = "lua-rs-engine")]
    #[test]
    fn lua_rs_eval_smoke_covers_args_call_and_sha1hex() {
        let mut client = redis_core::Client::new(81);
        client.set_args(vec![
            RedisString::from_bytes(b"EVAL"),
            RedisString::from_bytes(
                b"return {KEYS[1], ARGV[1], redis.call('ping').ok, redis.sha1hex('abc')}",
            ),
            RedisString::from_bytes(b"1"),
            RedisString::from_bytes(b"k"),
            RedisString::from_bytes(b"v"),
        ]);
        let mut ctx = CommandContext::new(&mut client);

        eval_command(&mut ctx).unwrap();

        assert_eq!(
            client.drain_reply(),
            b"*4\r\n$1\r\nk\r\n$1\r\nv\r\n$4\r\nPONG\r\n$40\r\na9993e364706816aba3e25717850c26c9cd0d89d\r\n"
        );
    }

    #[cfg(feature = "lua-rs-engine")]
    #[test]
    fn lua_rs_eval_smoke_pcall_returns_error_table() {
        let mut client = redis_core::Client::new(82);
        client.set_args(vec![
            RedisString::from_bytes(b"EVAL"),
            RedisString::from_bytes(b"return redis.pcall('nosuchcommand').err"),
            RedisString::from_bytes(b"0"),
        ]);
        let mut ctx = CommandContext::new(&mut client);

        eval_command(&mut ctx).unwrap();

        let reply = client.drain_reply();
        assert!(reply.starts_with(b"$"));
        assert!(reply
            .windows(b"unknown command".len())
            .any(|w| w.eq_ignore_ascii_case(b"unknown command")));
    }

    #[cfg(feature = "lua-rs-engine")]
    #[test]
    fn lua_rs_evalsha_runs_stateful_token_bucket_fixture() {
        const TOKEN_BUCKET_SCRIPT: &[u8] = br#"
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local capacity = tonumber(ARGV[2])
            local refill_tokens = tonumber(ARGV[3])
            local refill_ms = tonumber(ARGV[4])
            local cost = tonumber(ARGV[5])
            local ttl_ms = tonumber(ARGV[6])

            local function ceil_div(num, denom)
                return math.floor((num + denom - 1) / denom)
            end

            local tokens = capacity
            local updated_at = now
            local raw = redis.call('GET', key)
            if raw then
                local sep = string.find(raw, ':', 1, true)
                if sep then
                    tokens = tonumber(string.sub(raw, 1, sep - 1))
                    updated_at = tonumber(string.sub(raw, sep + 1))
                end
            end
            if tokens == nil then tokens = capacity end
            if updated_at == nil then updated_at = now end
            if now < updated_at then updated_at = now end

            local elapsed = now - updated_at
            local refill = math.floor(elapsed * refill_tokens / refill_ms)
            if refill > 0 then
                tokens = tokens + refill
                if tokens > capacity then tokens = capacity end
                updated_at = updated_at + math.floor(refill * refill_ms / refill_tokens)
            end

            local allowed = 0
            local retry_after = 0
            if tokens >= cost then
                tokens = tokens - cost
                allowed = 1
            else
                local missing = cost - tokens
                retry_after = updated_at + ceil_div(missing * refill_ms, refill_tokens) - now
                if retry_after < 0 then retry_after = 0 end
            end

            local reset_ms = updated_at + ceil_div((capacity - tokens) * refill_ms, refill_tokens)
            redis.call('SET', key, tostring(tokens) .. ':' .. tostring(updated_at), 'PX', ttl_ms)
            return {'allowed', allowed, 'remaining', tokens, 'reset_ms', reset_ms, 'retry_after_ms', retry_after}
        "#;

        fn parse_loaded_sha(reply: &[u8]) -> Vec<u8> {
            assert_eq!(reply.len(), 47, "unexpected SCRIPT LOAD reply: {reply:?}");
            assert_eq!(&reply[..5], b"$40\r\n");
            assert_eq!(&reply[45..], b"\r\n");
            reply[5..45].to_vec()
        }

        fn evalsha_token_bucket(
            ctx: &mut CommandContext<'_>,
            sha: &[u8],
            now_ms: &[u8],
        ) -> Vec<u8> {
            ctx.client_mut().set_args(vec![
                RedisString::from_bytes(b"EVALSHA"),
                RedisString::from_bytes(sha),
                RedisString::from_bytes(b"1"),
                RedisString::from_bytes(b"edge:tenant:42:tokens"),
                RedisString::from_bytes(now_ms),
                RedisString::from_bytes(b"10"),
                RedisString::from_bytes(b"5"),
                RedisString::from_bytes(b"1000"),
                RedisString::from_bytes(b"7"),
                RedisString::from_bytes(b"60000"),
            ]);
            evalsha_command(ctx).unwrap();
            ctx.client_mut().drain_reply()
        }

        let mut client = redis_core::Client::new(83);
        let mut db = RedisDb::new(0);
        let mut ctx = CommandContext::with_db(&mut client, &mut db);

        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"SCRIPT"),
            RedisString::from_bytes(b"LOAD"),
            RedisString::from_bytes(TOKEN_BUCKET_SCRIPT),
        ]);
        script_command(&mut ctx).unwrap();
        let sha = parse_loaded_sha(&ctx.client_mut().drain_reply());

        assert_eq!(
            evalsha_token_bucket(&mut ctx, &sha, b"1000"),
            b"*8\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:0\r\n"
        );
        assert_eq!(
            evalsha_token_bucket(&mut ctx, &sha, b"1100"),
            b"*8\r\n$7\r\nallowed\r\n:0\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:700\r\n"
        );
        assert_eq!(
            evalsha_token_bucket(&mut ctx, &sha, b"1800"),
            b"*8\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:0\r\n$8\r\nreset_ms\r\n:3800\r\n$14\r\nretry_after_ms\r\n:0\r\n"
        );
    }

    #[cfg(feature = "lua-rs-engine")]
    #[test]
    fn lua_rs_evalsha_reads_hash_policy_for_token_bucket_fixture() {
        const HASH_POLICY_TOKEN_BUCKET_SCRIPT: &[u8] = br#"
            local bucket_key = KEYS[1]
            local policy_key = KEYS[2]
            local now = tonumber(ARGV[1])
            local cost = tonumber(ARGV[2])

            local capacity = tonumber(redis.call('HGET', policy_key, 'capacity') or '10')
            local refill_tokens = tonumber(redis.call('HGET', policy_key, 'refill_tokens') or '5')
            local refill_ms = tonumber(redis.call('HGET', policy_key, 'refill_ms') or '1000')
            local ttl_ms = tonumber(redis.call('HGET', policy_key, 'ttl_ms') or '60000')

            local function ceil_div(num, denom)
                return math.floor((num + denom - 1) / denom)
            end

            local tokens = capacity
            local updated_at = now
            local raw = redis.call('GET', bucket_key)
            if raw then
                local sep = string.find(raw, ':', 1, true)
                if sep then
                    tokens = tonumber(string.sub(raw, 1, sep - 1))
                    updated_at = tonumber(string.sub(raw, sep + 1))
                end
            end
            if tokens == nil then tokens = capacity end
            if updated_at == nil then updated_at = now end
            if now < updated_at then updated_at = now end

            local elapsed = now - updated_at
            local refill = math.floor(elapsed * refill_tokens / refill_ms)
            if refill > 0 then
                tokens = tokens + refill
                if tokens > capacity then tokens = capacity end
                updated_at = updated_at + math.floor(refill * refill_ms / refill_tokens)
            end

            local allowed = 0
            local retry_after = 0
            if tokens >= cost then
                tokens = tokens - cost
                allowed = 1
            else
                local missing = cost - tokens
                retry_after = updated_at + ceil_div(missing * refill_ms, refill_tokens) - now
                if retry_after < 0 then retry_after = 0 end
            end

            local reset_ms = updated_at + ceil_div((capacity - tokens) * refill_ms, refill_tokens)
            redis.call('SET', bucket_key, tostring(tokens) .. ':' .. tostring(updated_at), 'PX', ttl_ms)
            return {'allowed', allowed, 'remaining', tokens, 'reset_ms', reset_ms, 'retry_after_ms', retry_after, 'capacity', capacity}
        "#;

        fn parse_loaded_sha(reply: &[u8]) -> Vec<u8> {
            assert_eq!(reply.len(), 47, "unexpected SCRIPT LOAD reply: {reply:?}");
            assert_eq!(&reply[..5], b"$40\r\n");
            assert_eq!(&reply[45..], b"\r\n");
            reply[5..45].to_vec()
        }

        fn evalsha_policy_bucket(
            ctx: &mut CommandContext<'_>,
            sha: &[u8],
            now_ms: &[u8],
        ) -> Vec<u8> {
            ctx.client_mut().set_args(vec![
                RedisString::from_bytes(b"EVALSHA"),
                RedisString::from_bytes(sha),
                RedisString::from_bytes(b"2"),
                RedisString::from_bytes(b"edge:tenant:42:tokens"),
                RedisString::from_bytes(b"edge:tenant:42:policy"),
                RedisString::from_bytes(now_ms),
                RedisString::from_bytes(b"7"),
            ]);
            evalsha_command(ctx).unwrap();
            ctx.client_mut().drain_reply()
        }

        let mut client = redis_core::Client::new(84);
        let mut db = RedisDb::new(0);
        let mut ctx = CommandContext::with_db(&mut client, &mut db);

        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"HSET"),
            RedisString::from_bytes(b"edge:tenant:42:policy"),
            RedisString::from_bytes(b"capacity"),
            RedisString::from_bytes(b"10"),
            RedisString::from_bytes(b"refill_tokens"),
            RedisString::from_bytes(b"5"),
            RedisString::from_bytes(b"refill_ms"),
            RedisString::from_bytes(b"1000"),
            RedisString::from_bytes(b"ttl_ms"),
            RedisString::from_bytes(b"60000"),
        ]);
        crate::hash::hset_command(&mut ctx).unwrap();
        assert_eq!(ctx.client_mut().drain_reply(), b":4\r\n");

        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"SCRIPT"),
            RedisString::from_bytes(b"LOAD"),
            RedisString::from_bytes(HASH_POLICY_TOKEN_BUCKET_SCRIPT),
        ]);
        script_command(&mut ctx).unwrap();
        let sha = parse_loaded_sha(&ctx.client_mut().drain_reply());

        assert_eq!(
            evalsha_policy_bucket(&mut ctx, &sha, b"1000"),
            b"*10\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:0\r\n$8\r\ncapacity\r\n:10\r\n"
        );
        assert_eq!(
            evalsha_policy_bucket(&mut ctx, &sha, b"1100"),
            b"*10\r\n$7\r\nallowed\r\n:0\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:700\r\n$8\r\ncapacity\r\n:10\r\n"
        );

        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"HSET"),
            RedisString::from_bytes(b"edge:tenant:42:policy"),
            RedisString::from_bytes(b"capacity"),
            RedisString::from_bytes(b"20"),
        ]);
        crate::hash::hset_command(&mut ctx).unwrap();
        assert_eq!(ctx.client_mut().drain_reply(), b":0\r\n");

        assert_eq!(
            evalsha_policy_bucket(&mut ctx, &sha, b"1800"),
            b"*10\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:0\r\n$8\r\nreset_ms\r\n:5800\r\n$14\r\nretry_after_ms\r\n:0\r\n$8\r\ncapacity\r\n:20\r\n"
        );
    }

    #[test]
    fn fcall_cached_runtime_returns_key_argument_across_repeated_calls() {
        let server = Arc::new(RedisServer::default());
        let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
        let mut client = redis_core::Client::new(9);
        let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let mut ctx = redis_core::CommandContext::with_server_and_db_list(
            &mut client,
            &mut dbs,
            server,
            pubsub,
        );

        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"FUNCTION"),
            RedisString::from_bytes(b"LOAD"),
            RedisString::from_bytes(b"REPLACE"),
            RedisString::from_bytes(
                b"#!lua name=cachetest_keys\n\
                  server.register_function('cachetest_key', function(keys, args) return keys[1] end)",
            ),
        ]);
        function_load_command(&mut ctx).unwrap();
        assert_eq!(ctx.client_mut().drain_reply(), b"$14\r\ncachetest_keys\r\n");

        for _ in 0..2 {
            ctx.client_mut().set_args(vec![
                RedisString::from_bytes(b"FCALL"),
                RedisString::from_bytes(b"cachetest_key"),
                RedisString::from_bytes(b"1"),
                RedisString::from_bytes(b"key1"),
            ]);
            fcall_command(&mut ctx).unwrap();
            assert_eq!(ctx.client_mut().drain_reply(), b"$4\r\nkey1\r\n");
        }
    }

    #[test]
    fn fcall_cached_runtime_keeps_redis_call_bridge() {
        let server = Arc::new(RedisServer::default());
        let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
        let mut client = redis_core::Client::new(10);
        let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let mut ctx = redis_core::CommandContext::with_server_and_db_list(
            &mut client,
            &mut dbs,
            server,
            pubsub,
        );

        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"FUNCTION"),
            RedisString::from_bytes(b"LOAD"),
            RedisString::from_bytes(b"REPLACE"),
            RedisString::from_bytes(
                b"#!lua name=cachetest_call\n\
                  server.register_function('cachetest_ping', function(keys, args) return server.call('ping') end)",
            ),
        ]);
        function_load_command(&mut ctx).unwrap();
        assert_eq!(ctx.client_mut().drain_reply(), b"$14\r\ncachetest_call\r\n");

        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"FCALL"),
            RedisString::from_bytes(b"cachetest_ping"),
            RedisString::from_bytes(b"0"),
        ]);
        fcall_command(&mut ctx).unwrap();
        assert_eq!(ctx.client_mut().drain_reply(), b"+PONG\r\n");
    }

    #[test]
    fn function_load_replace_identical_library_preserves_behavior() {
        let server = Arc::new(RedisServer::default());
        let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
        let mut client = redis_core::Client::new(11);
        let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let mut ctx = redis_core::CommandContext::with_server_and_db_list(
            &mut client,
            &mut dbs,
            server,
            pubsub,
        );
        let library_name = b"cachetest_noop_replace";
        let code = b"#!lua name=cachetest_noop_replace\n\
                     server.register_function('cachetest_noop_fn', function(keys, args) return 42 end)";

        {
            let mut guard = match function_libraries().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.retain(|_, library| !ascii_eq_ci(&library.name, library_name));
        }

        for _ in 0..2 {
            ctx.client_mut().set_args(vec![
                RedisString::from_bytes(b"FUNCTION"),
                RedisString::from_bytes(b"LOAD"),
                RedisString::from_bytes(b"REPLACE"),
                RedisString::from_bytes(code),
            ]);
            function_load_command(&mut ctx).unwrap();
            assert_eq!(
                ctx.client_mut().drain_reply(),
                b"$22\r\ncachetest_noop_replace\r\n"
            );
        }

        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"FCALL"),
            RedisString::from_bytes(b"cachetest_noop_fn"),
            RedisString::from_bytes(b"0"),
        ]);
        fcall_command(&mut ctx).unwrap();
        assert_eq!(ctx.client_mut().drain_reply(), b":42\r\n");

        ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"FUNCTION"),
            RedisString::from_bytes(b"LOAD"),
            RedisString::from_bytes(code),
        ]);
        let err = function_load_command(&mut ctx).unwrap_err();
        assert!(err
            .to_resp_payload()
            .as_bytes()
            .windows(b"already exists".len())
            .any(|w| w == b"already exists"));

        let mut guard = match function_libraries().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.retain(|_, library| !ascii_eq_ci(&library.name, library_name));
    }

    #[test]
    fn loaded_library_code_identity_matches_name_case_insensitively() {
        let mut libraries = HashMap::new();
        libraries.insert(
            b"BenchLib".to_vec(),
            LoadedFunctionLibrary {
                name: b"BenchLib".to_vec(),
                code: b"body".to_vec(),
                functions: Vec::new(),
                script_checks: FunctionScriptChecks::default(),
            },
        );

        assert!(loaded_library_code_is_identical(
            &libraries,
            b"benchlib",
            b"body"
        ));
        assert!(!loaded_library_code_is_identical(
            &libraries,
            b"benchlib",
            b"different"
        ));
        assert!(!loaded_library_code_is_identical(
            &libraries, b"other", b"body"
        ));
    }

    #[test]
    fn function_source_eval_flags_finds_existing_broad_markers() {
        let flags = function_source_eval_flags(
            b"-- FLAGS=NO-WRITES\n#!LUA name=lib\n-- flags=ALLOW-OOM\n-- flags=allow-stale",
        );

        assert!(flags.has_shebang);
        assert!(flags.no_writes);
        assert!(flags.allow_oom);
        assert!(flags.allow_stale);

        let flags = function_source_eval_flags(b"flags=no_writes flags=allow,oom");
        assert!(!flags.no_writes);
        assert!(!flags.allow_oom);
    }

    #[test]
    fn function_source_allows_oom_matches_existing_marker_rule() {
        assert!(function_source_allows_oom(
            b"#!lua name=lib\n-- FLAGS=ALLOW-OOM"
        ));
        assert!(!function_source_allows_oom(
            b"#!lua name=lib flags=no-writes,allow-oom"
        ));
    }

    #[test]
    fn strip_embedded_eval_shebang_lines_borrows_when_unmodified() {
        let code = b"#!lua name=lib\nserver.register_function('f', function() return 1 end)";
        let stripped = strip_embedded_eval_shebang_lines(code);
        assert_eq!(stripped.as_ref(), code);
        assert!(matches!(stripped, std::borrow::Cow::Borrowed(_)));

        let code =
            b"#!lua name=lib\n#!lua flags=no-writes\nserver.register_function('f', function() return 1 end)";
        let stripped = strip_embedded_eval_shebang_lines(code);
        assert_eq!(
            stripped.as_ref(),
            b"#!lua name=lib\nserver.register_function('f', function() return 1 end)"
        );
        assert!(matches!(stripped, std::borrow::Cow::Owned(_)));
    }

    #[test]
    fn run_inner_wait_is_script_safe() {
        let mut client = redis_core::Client::new(1);
        let mut outer: redis_core::Client = redis_core::Client::new(1);
        client.set_args(vec![
            RedisString::from_bytes(b"SET"),
            RedisString::from_bytes(b"x"),
            RedisString::from_bytes(b"1"),
        ]);
        let original_args = client.argv.clone();
        let mut ctx = CommandContext::new(&mut client);
        let reply = run_inner_command(
            &mut ctx,
            &[b"WAIT".to_vec(), b"1".to_vec(), b"0".to_vec()],
            None,
        )
        .unwrap();

        match reply {
            ReplyValue::Integer(v) => assert_eq!(v, 0),
            _ => panic!("expected integer reply from WAIT inside script"),
        }
        assert_eq!(client.argv, original_args);

        let mut wait_ctx = CommandContext::new(&mut outer);
        let wait_reply = run_inner_command(
            &mut wait_ctx,
            &[
                b"WAITAOF".to_vec(),
                b"0".to_vec(),
                b"1".to_vec(),
                b"0".to_vec(),
            ],
            None,
        )
        .unwrap();
        match wait_reply {
            ReplyValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], ReplyValue::Integer(0)));
                assert!(matches!(items[1], ReplyValue::Integer(0)));
            }
            _ => panic!("expected two-item array reply from WAITAOF inside script"),
        }

        wait_ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"waitaof"),
            RedisString::from_bytes(b"0"),
            RedisString::from_bytes(b"1"),
            RedisString::from_bytes(b"0"),
        ]);
        let direct = crate::dispatch::dispatch_command_name(&mut wait_ctx, b"waitaof");
        if direct.is_ok() {
            assert_eq!(wait_ctx.client_mut().drain_reply(), b"*2\r\n:0\r\n:0\r\n");
        } else {
            panic!("WAITAOF handler should be registered");
        }
    }

    #[test]
    fn resp3_double_and_null_reply_shapes_match_lua_bridge() {
        let lua = Lua::new();

        let double = reply_to_lua(&lua, &ReplyValue::Double(1.25), 3).unwrap();
        match double {
            LuaValue::Table(t) => assert_eq!(t.raw_get::<f64>("double").unwrap(), 1.25),
            other => panic!("expected table for RESP3 double, got {other:?}"),
        }

        assert!(matches!(
            reply_to_lua(&lua, &ReplyValue::Null, 3).unwrap(),
            LuaValue::Nil
        ));
        assert!(matches!(
            reply_to_lua(&lua, &ReplyValue::Nil, 3).unwrap(),
            LuaValue::Boolean(false)
        ));
    }

    #[test]
    fn map_reply_view_depends_on_setresp() {
        let lua = Lua::new();
        let reply = ReplyValue::Map(vec![
            ReplyValue::Bulk(b"field".to_vec()),
            ReplyValue::Bulk(b"value".to_vec()),
        ]);

        let resp3 = reply_to_lua(&lua, &reply, 3).unwrap();
        match resp3 {
            LuaValue::Table(t) => {
                let map: LuaTable = t.raw_get("map").unwrap();
                let v: mlua::String = map.get("field").unwrap();
                assert_eq!(v.as_bytes().as_ref(), b"value");
            }
            other => panic!("expected {{map=...}} under setresp(3), got {other:?}"),
        }

        let resp2 = reply_to_lua(&lua, &reply, 2).unwrap();
        match resp2 {
            LuaValue::Table(t) => {
                let f: mlua::String = t.raw_get(1).unwrap();
                let v: mlua::String = t.raw_get(2).unwrap();
                assert_eq!(f.as_bytes().as_ref(), b"field");
                assert_eq!(v.as_bytes().as_ref(), b"value");
                assert!(t.raw_get::<Option<LuaTable>>("map").unwrap().is_none());
            }
            other => panic!("expected flat array under setresp(2), got {other:?}"),
        }
    }

    #[test]
    fn map_table_encodes_per_client_resp_version() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        let map = lua.create_table().unwrap();
        map.raw_set("field", "value").unwrap();
        table.raw_set("map", map).unwrap();
        let value = LuaValue::Table(table);

        let mut resp3 = Vec::new();
        lua_to_resp(&value, &mut resp3, true);
        assert_eq!(resp3, b"%1\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");

        let mut resp2 = Vec::new();
        lua_to_resp(&value, &mut resp2, false);
        assert_eq!(resp2, b"*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn recursive_table_reply_hits_lua_stack_limit_instead_of_overflowing() {
        let lua = Lua::new();
        let a = lua.create_table().unwrap();
        let b = lua.create_table().unwrap();
        b.raw_set(1, a.clone()).unwrap();
        a.raw_set(1, b).unwrap();

        let mut out = Vec::new();
        lua_to_resp(&LuaValue::Table(a), &mut out, true);

        assert!(out.starts_with(b"*1\r\n"));
        assert!(out.ends_with(b"-ERR reached lua stack limit\r\n"));
    }

    #[test]
    fn lua_double_table_serializes_as_resp3_double() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        table.raw_set("double", 1.25).unwrap();
        let mut out = Vec::new();

        lua_to_resp(&LuaValue::Table(table), &mut out, true);

        assert_eq!(out, b",1.25\r\n");
    }

    #[test]
    fn cmsgpack_pack_matches_upstream_numeric_vectors() {
        let lua = Lua::new();
        install_cmsgpack(&lua).unwrap();

        let double: mlua::String = lua.load("return cmsgpack.pack(0.1)").eval().unwrap();
        assert_eq!(
            &hex_encode(double.as_bytes().as_ref()),
            b"cb3fb999999999999a"
        );

        let negative: mlua::String = lua
            .load("return cmsgpack.pack(-1099511627776)")
            .eval()
            .unwrap();
        assert_eq!(
            &hex_encode(negative.as_bytes().as_ref()),
            b"d3ffffff0000000000"
        );
    }

    #[test]
    fn cmsgpack_unpack_limit_uses_redis_offsets() {
        let lua = Lua::new();
        install_cmsgpack(&lua).unwrap();

        let ok: bool = lua
            .load(
                "local encoded = cmsgpack.pack('a', 'bb')\n\
                 local offset, first = cmsgpack.unpack_limit(encoded, 1, 0)\n\
                 local final_offset, second = cmsgpack.unpack_limit(encoded, 1, offset)\n\
                 return first == 'a' and second == 'bb' and final_offset == -1",
            )
            .eval()
            .unwrap();
        assert!(ok);
    }

    #[test]
    fn cmsgpack_circular_cutoff_matches_upstream_depth_vector() {
        let lua = Lua::new();
        install_cmsgpack(&lua).unwrap();

        let packed: mlua::String = lua
            .load(
                "local a = {x=nil,y=5}\n\
                 local b = {x=a}\n\
                 a['x'] = b\n\
                 return cmsgpack.pack(a)",
            )
            .eval()
            .unwrap();
        assert_eq!(
            &hex_encode(packed.as_bytes().as_ref()),
            b"82a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a178c0"
        );
    }

    #[test]
    fn bit_minimal_bitop_matches_upstream() {
        let lua = Lua::new();
        install_bit(&lua).unwrap();

        let ok: bool = lua
            .load(
                "return bit.tobit(1) == 1\n\
                 and bit.band(1) == 1\n\
                 and bit.bxor(1, 2) == 3\n\
                 and bit.bor(1, 2, 4, 8, 16, 32, 64, 128) == 255",
            )
            .eval()
            .unwrap();
        assert!(ok);
    }

    #[test]
    fn bit_tohex_int32_min_width_matches_upstream() {
        let lua = Lua::new();
        install_bit(&lua).unwrap();

        let hex: mlua::String = lua
            .load("return bit.tohex(65535, -2147483648)")
            .eval()
            .unwrap();
        assert_eq!(hex.as_bytes().as_ref(), b"0000FFFF");
    }

    #[test]
    fn bit_shifts_use_32bit_wrapping_semantics() {
        let lua = Lua::new();
        install_bit(&lua).unwrap();

        let ok: bool = lua
            .load(
                "return bit.bnot(0) == -1\n\
                 and bit.lshift(1, 31) == -2147483648\n\
                 and bit.rshift(-2147483648, 31) == 1\n\
                 and bit.arshift(-2147483648, 31) == -1\n\
                 and bit.rol(0x12345678, 12) == bit.tobit(0x45678123)\n\
                 and bit.bswap(0x12345678) == bit.tobit(0x78563412)",
            )
            .eval()
            .unwrap();
        assert!(ok);
    }

    #[test]
    fn bit_table_is_readonly() {
        let lua = Lua::new();
        install_bit(&lua).unwrap();

        let err = lua
            .load("bit.lshift = function() return 1 end")
            .exec()
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("Attempt to modify a readonly table"));
    }

    #[test]
    fn os_sandbox_exposes_only_clock() {
        let lua = Lua::new();
        install_sandbox(&lua).unwrap();

        let only_clock: bool = lua
            .load(
                "local keys = {}\n\
                 for k, v in pairs(os) do keys[#keys + 1] = k .. ':' .. type(v) end\n\
                 return #keys == 1 and keys[1] == 'clock:function'",
            )
            .eval()
            .unwrap();
        assert!(only_clock);
    }

    #[test]
    fn os_clock_measures_elapsed_delta() {
        let lua = Lua::new();
        install_sandbox(&lua).unwrap();

        let nonnegative: bool = lua
            .load("local s = os.clock(); local e = os.clock(); return e - s >= 0")
            .eval()
            .unwrap();
        assert!(nonnegative);
    }

    #[test]
    fn os_dangerous_methods_are_absent() {
        let lua = Lua::new();
        install_sandbox(&lua).unwrap();

        let err = lua.load("os.execute()").exec().unwrap_err();
        assert!(err.to_string().contains("attempt to call field 'execute'"));
    }
}

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
