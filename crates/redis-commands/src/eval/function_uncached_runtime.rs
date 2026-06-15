//! Uncached FCALL runtime path.
//!
//! The cached runtime lives in `function_runtime`; this module keeps the
//! one-shot fallback path out of the command/parser surface in `eval.rs`.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use mlua::{
    Error as LuaError, Function as LuaFunction, Lua, MultiValue, Table as LuaTable,
    Value as LuaValue,
};
use redis_core::metrics::record_error_reply;
use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult, RedisString};

use super::busy_script::{current_command_argv, set_busy_script, BusyScriptKind, BusyScriptState};
use super::bytes::ascii_eq_ci;
use super::function_metadata::{
    parse_function_library_header, parse_runtime_register_function_args,
    RuntimeFunctionRegistration,
};
use super::function_store::{FunctionDefinition, LoadedFunctionLibrary};
use super::inner_command::run_inner_command;
use super::lua_api::{
    install_log_function, install_redis_api_constants, install_set_repl_function,
    install_setresp_function,
};
use super::lua_bit::install_bit;
use super::lua_cjson::install_cjson;
use super::lua_cmsgpack::install_cmsgpack;
use super::lua_sandbox::{
    create_disabled_loadstring, create_script_environment, create_sha1hex_function,
    install_eval_global_protection, install_keys_argv, install_sandbox,
    install_script_error_wrapper, readonly_table_proxy,
    readonly_table_proxy_with_missing_global_errors,
};
use super::resp_bridge::{
    lua_to_resp, reply_to_lua, script_resp_view, ReplyValue, LUA_ERROR_ALREADY_RECORDED_FIELD,
};
use super::script_checks::unpack_range_overflow_error;
use super::script_errors::{
    lua_execution_error_payload, lua_script_command_error_payload,
    lua_script_command_reply_error_payload,
};
use super::{
    acl_check_cmd_allowed, call_is_write_command, collect_call_args, good_replicas_status,
    noreplicas_error, noreplicas_lua_error, noreplicas_lua_table, record_script_rejected_command,
    redis_strings_to_lua_table, replica_readonly_lua_call_blocked, replica_readonly_lua_call_error,
    replica_readonly_lua_call_table, run_massive_unpack_lpush_shortcut, script_command_not_allowed,
    stale_replica_lua_call_allowed, stale_replica_lua_call_error, stale_replica_scripts_blocked,
    NOREPLICAS_ERROR, READ_ONLY_SCRIPT_WRITE_ERROR_LUA, READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD,
    READ_ONLY_SCRIPT_WRITE_ERROR_RESP, REPLICA_READONLY_ERROR_PAYLOAD,
};

pub(super) fn run_loaded_function_uncached(
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
