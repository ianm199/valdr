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

use mlua::{Error as LuaError, Lua, MultiValue, Table as LuaTable, Value as LuaValue};

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
mod function_uncached_runtime;
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
use bytes::{ascii_casecmp_bytes, ascii_eq_ci, glob_match_ascii_ci};
use function_compiler::compile_function_library;
use function_dump::{decode_function_dump, encode_function_dump, function_library_frame};
pub(crate) use function_dump::{
    function_library_codes_for_aof_rewrite, function_vm_memory_used_estimate,
};
pub use function_dump::{
    function_rdb_payloads, install_rdb_function_replacement, prepare_rdb_function_replacement,
};
use function_metadata::parse_function_library_header;
use function_runtime::{store_cached_function_runtime, take_cached_function_runtime};
pub use function_store::PreparedFunctionLibraries;
use function_store::{
    find_loaded_function, function_libraries, function_library_key, install_function_library,
    loaded_library_code_is_identical, snapshot_function_libraries, FunctionDefinition,
    LoadedFunctionLibrary,
};
use function_uncached_runtime::run_loaded_function_uncached;
use inner_command::run_inner_command;
use resp_bridge::{lua_to_resp, ReplyValue, LUA_ERROR_ALREADY_RECORDED_FIELD};
use script_cache::{cache_script, normalise_sha};
pub(crate) use script_cache::{
    evicted_scripts_count, reset_script_cache_stats, script_cache_len, script_cache_memory_estimate,
};
use script_checks::{
    function_script_checks, script_is_top_level_infinite_function_load, unpack_range_overflow_error,
};
pub use script_commands::script_command;
use script_errors::{lua_arg_to_bytes, lua_execution_error_payload, lua_script_call_error_payload};
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
