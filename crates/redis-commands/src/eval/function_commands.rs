//! `FUNCTION` and `FCALL` command surface for Lua functions.
//!
//! The parent `eval` module keeps EVAL/EVALSHA parsing and compatibility
//! exports. This module owns FUNCTION subcommands, FCALL argument parsing, and
//! the cached function runtime wrapper.

use std::borrow::Cow;

use mlua::{Lua, Table as LuaTable};
use redis_core::metrics::record_error_reply;
use redis_core::CommandContext;
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};

use super::active_function::function_call_active;
use super::busy_script::{
    busy_script_error, busy_script_snapshot, clear_busy_script, current_command_argv,
    set_busy_script, BusyScriptKind, BusyScriptState,
};
use super::bytes::{ascii_casecmp_bytes, ascii_eq_ci, glob_match_ascii_ci};
use super::command_policy::{
    function_command_would_exceed_maxmemory, function_oom_error, good_replicas_status,
    noreplicas_error, replica_readonly_error, replica_readonly_script_blocked,
    stale_replica_masterdown_error, stale_replica_scripts_blocked,
};
use super::function_compiler::compile_function_library;
use super::function_dump::{decode_function_dump, encode_function_dump, function_library_frame};
use super::function_metadata::parse_function_library_header;
use super::function_runtime::{store_cached_function_runtime, take_cached_function_runtime};
use super::function_store::{
    find_loaded_function, function_libraries, function_library_key, install_function_library,
    loaded_library_code_is_identical, snapshot_function_libraries, FunctionDefinition,
    LoadedFunctionLibrary,
};
use super::function_uncached_runtime::run_loaded_function_uncached;
use super::resp_bridge::lua_to_resp;
use super::script_checks::{
    function_script_checks, script_is_top_level_infinite_function_load, unpack_range_overflow_error,
};
use super::script_errors::lua_execution_error_payload;
use super::script_flags::{
    function_source_allows_oom, function_source_eval_flags, strip_embedded_eval_shebang_lines,
};
use super::{parse_i64, run_massive_unpack_lpush_shortcut};

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

pub(super) fn redis_strings_to_lua_table(
    lua: &Lua,
    values: &[RedisString],
) -> mlua::Result<LuaTable> {
    let table = lua.create_table()?;
    for (i, value) in values.iter().enumerate() {
        table.raw_set(i as i64 + 1, lua.create_string(value.as_bytes())?)?;
    }
    Ok(table)
}
