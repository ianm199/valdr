//! Cached Lua function runtime adapter.
//!
//! FCALL keeps one compiled Lua runtime per thread when possible. This module
//! owns that cache plus the callback bridge used by cached `redis.call` and
//! `redis.pcall` handlers.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use mlua::{
    Error as LuaError, Function as LuaFunction, Lua, MultiValue, Table as LuaTable,
    Value as LuaValue,
};
use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult, RedisString};

use super::active_function::{
    active_function_call, active_function_dirty, active_function_error_recorded,
    enter_active_function_call, with_active_function_context,
};
use super::bytes::ascii_eq_ci;
use super::function_compiler::function_load_lua_error;
use super::function_metadata::{
    parse_function_library_header, parse_runtime_register_function_args,
    RuntimeFunctionRegistration,
};
use super::function_store::{FunctionDefinition, LoadedFunctionLibrary};
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
    reply_to_lua, script_resp_view, ReplyValue, LUA_ERROR_ALREADY_RECORDED_FIELD,
};
use super::script_errors::{
    lua_script_command_error_payload, lua_script_command_reply_error_payload,
};
use super::{
    acl_check_cmd_allowed, call_is_write_command, collect_call_args, good_replicas_status,
    install_log_function, install_redis_api_constants, install_set_repl_function,
    install_setresp_function, noreplicas_lua_error, noreplicas_lua_table,
    record_script_rejected_command, redis_strings_to_lua_table, replica_readonly_lua_call_blocked,
    replica_readonly_lua_call_error, replica_readonly_lua_call_table, run_inner_command,
    script_command_not_allowed, stale_replica_lua_call_allowed, stale_replica_lua_call_error,
    NOREPLICAS_ERROR, READ_ONLY_SCRIPT_WRITE_ERROR_LUA, READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD,
    READ_ONLY_SCRIPT_WRITE_ERROR_RESP, REPLICA_READONLY_ERROR_PAYLOAD,
};

pub(super) struct CachedFunctionRuntime {
    library_name: Vec<u8>,
    library_code: Vec<u8>,
    lua: Lua,
    registrations: Vec<RuntimeFunctionRegistration>,
}

thread_local! {
    static CACHED_FUNCTION_RUNTIME: RefCell<Option<CachedFunctionRuntime>> = const { RefCell::new(None) };
}

fn cached_function_call(lua_inner: &Lua, args: MultiValue) -> mlua::Result<LuaValue> {
    let arg_bytes = collect_call_args(args)?;
    let is_write = call_is_write_command(&arg_bytes);
    if script_command_not_allowed(&arg_bytes) {
        return Err(LuaError::RuntimeError(
            "This Redis command is not allowed from script".to_string(),
        ));
    }
    let active = active_function_call()?;
    if active.stale_replica_blocked
        && active.function_allow_stale
        && !stale_replica_lua_call_allowed(&arg_bytes)
    {
        return Err(stale_replica_lua_call_error());
    }

    with_active_function_context(|ctx, active| {
        if replica_readonly_lua_call_blocked(&*ctx, &arg_bytes) {
            record_script_rejected_command(&arg_bytes, REPLICA_READONLY_ERROR_PAYLOAD);
            active_function_error_recorded(active).set(true);
            return Err(replica_readonly_lua_call_error());
        }
        if active.read_only && is_write {
            record_script_rejected_command(&arg_bytes, READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD);
            active_function_error_recorded(active).set(true);
            return Err(LuaError::RuntimeError(
                READ_ONLY_SCRIPT_WRITE_ERROR_LUA.to_string(),
            ));
        }
        if is_write && !good_replicas_status(&*ctx) {
            record_script_rejected_command(&arg_bytes, NOREPLICAS_ERROR.as_bytes());
            active_function_error_recorded(active).set(true);
            return Err(noreplicas_lua_error());
        }
        match run_inner_command(ctx, &arg_bytes, Some(active_function_dirty(active))) {
            Ok(reply) => {
                if let ReplyValue::Error(msg) = &reply {
                    active_function_error_recorded(active).set(true);
                    return Err(LuaError::RuntimeError(
                        String::from_utf8_lossy(&lua_script_command_reply_error_payload(msg))
                            .into_owned(),
                    ));
                }
                reply_to_lua(lua_inner, &reply, script_resp_view(lua_inner))
            }
            Err(e) => {
                active_function_error_recorded(active).set(true);
                Err(LuaError::RuntimeError(
                    String::from_utf8_lossy(&lua_script_command_error_payload(&e)).into_owned(),
                ))
            }
        }
    })
}

fn cached_function_pcall(lua_inner: &Lua, args: MultiValue) -> mlua::Result<LuaValue> {
    let arg_bytes = collect_call_args(args)?;
    let is_write = call_is_write_command(&arg_bytes);
    if script_command_not_allowed(&arg_bytes) {
        let t = lua_inner.create_table()?;
        t.raw_set(
            "err",
            lua_inner.create_string("This Redis command is not allowed from script")?,
        )?;
        return Ok(LuaValue::Table(t));
    }
    let active = active_function_call()?;
    if active.stale_replica_blocked
        && active.function_allow_stale
        && !stale_replica_lua_call_allowed(&arg_bytes)
    {
        let t = lua_inner.create_table()?;
        t.raw_set(
            "err",
            lua_inner.create_string("Can not execute the command on a stale replica")?,
        )?;
        return Ok(LuaValue::Table(t));
    }

    with_active_function_context(|ctx, active| {
        if replica_readonly_lua_call_blocked(&*ctx, &arg_bytes) {
            record_script_rejected_command(&arg_bytes, REPLICA_READONLY_ERROR_PAYLOAD);
            return replica_readonly_lua_call_table(lua_inner);
        }
        if active.read_only && is_write {
            record_script_rejected_command(&arg_bytes, READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD);
            let t = lua_inner.create_table()?;
            t.raw_set(
                "err",
                lua_inner.create_string(READ_ONLY_SCRIPT_WRITE_ERROR_RESP)?,
            )?;
            t.raw_set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
            return Ok(LuaValue::Table(t));
        }
        if is_write && !good_replicas_status(&*ctx) {
            record_script_rejected_command(&arg_bytes, NOREPLICAS_ERROR.as_bytes());
            return noreplicas_lua_table(lua_inner);
        }
        match run_inner_command(ctx, &arg_bytes, Some(active_function_dirty(active))) {
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
    })
}

impl CachedFunctionRuntime {
    fn matches_library(&self, library: &LoadedFunctionLibrary) -> bool {
        self.library_name == library.name && self.library_code == library.code
    }

    pub(super) fn call(
        &mut self,
        ctx: &mut CommandContext<'_>,
        definition: &FunctionDefinition,
        keys: &[RedisString],
        argv: &[RedisString],
        read_only: bool,
        stale_replica_blocked: bool,
        function_allow_stale: bool,
    ) -> RedisResult<(Result<LuaValue, LuaError>, bool)> {
        let registration = self
            .registrations
            .iter()
            .find(|registered| ascii_eq_ci(&registered.name, &definition.name))
            .ok_or_else(|| RedisError::runtime(b"ERR Function not found"))?;
        if registration.no_writes != definition.no_writes {
            return Err(RedisError::runtime(
                b"ERR Function flags changed while loading library",
            ));
        }
        if registration.allow_oom != definition.allow_oom {
            return Err(RedisError::runtime(
                b"ERR Function flags changed while loading library",
            ));
        }
        if registration.allow_stale != definition.allow_stale {
            return Err(RedisError::runtime(
                b"ERR Function flags changed while loading library",
            ));
        }

        install_keys_argv(&self.lua, keys, argv)
            .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
        let keys_table = redis_strings_to_lua_table(&self.lua, keys)
            .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
        let argv_table = redis_strings_to_lua_table(&self.lua, argv)
            .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
        let callback: LuaFunction =
            self.lua
                .registry_value(&registration.callback)
                .map_err(|e| {
                    RedisError::runtime(format!("ERR Error running function: {}", e).into_bytes())
                })?;

        let script_dirty = Cell::new(false);
        let script_error_already_recorded = Cell::new(false);
        let result = {
            let _guard = enter_active_function_call(
                ctx,
                read_only,
                stale_replica_blocked,
                function_allow_stale,
                &script_dirty,
                &script_error_already_recorded,
            );
            callback.call::<LuaValue>((keys_table, argv_table))
        };

        Ok((result, script_error_already_recorded.get()))
    }
}

fn compile_cached_function_runtime(
    library: &LoadedFunctionLibrary,
) -> RedisResult<CachedFunctionRuntime> {
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

    let redis_tbl = lua
        .create_table()
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    install_redis_api_constants(&redis_tbl)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    redis_tbl
        .raw_set(
            "__raw_call",
            lua.create_function_mut(cached_function_call)
                .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?,
        )
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    redis_tbl
        .raw_set(
            "call",
            lua.create_function_mut(cached_function_call)
                .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?,
        )
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    redis_tbl
        .raw_set(
            "pcall",
            lua.create_function_mut(cached_function_pcall)
                .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?,
        )
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    redis_tbl
        .raw_set(
            "error_reply",
            lua.create_function(|lua_inner, msg: mlua::String| -> mlua::Result<LuaTable> {
                let t = lua_inner.create_table()?;
                t.raw_set("err", msg)?;
                Ok(t)
            })
            .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?,
        )
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    redis_tbl
        .raw_set(
            "status_reply",
            lua.create_function(|lua_inner, msg: mlua::String| -> mlua::Result<LuaTable> {
                let t = lua_inner.create_table()?;
                t.raw_set("ok", msg)?;
                Ok(t)
            })
            .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?,
        )
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    redis_tbl
        .raw_set(
            "sha1hex",
            create_sha1hex_function(&lua)
                .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?,
        )
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    redis_tbl
        .raw_set(
            "replicate_commands",
            lua.create_function(|_lua, _: MultiValue| -> mlua::Result<bool> { Ok(true) })
                .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?,
        )
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    install_set_repl_function(&lua, &redis_tbl)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    install_log_function(&lua, &redis_tbl)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    redis_tbl
        .raw_set(
            "acl_check_cmd",
            lua.create_function_mut(|_lua_inner, args: MultiValue| -> mlua::Result<bool> {
                let arg_bytes = collect_call_args(args)?;
                with_active_function_context(|ctx, _| acl_check_cmd_allowed(&*ctx, &arg_bytes))
            })
            .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?,
        )
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    install_setresp_function(&lua, &redis_tbl)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;

    let registrations: Rc<RefCell<Vec<RuntimeFunctionRegistration>>> =
        Rc::new(RefCell::new(Vec::new()));
    let load_phase = Rc::new(Cell::new(true));
    let register_fn = {
        let registrations = Rc::clone(&registrations);
        let load_phase = Rc::clone(&load_phase);
        lua.create_function_mut(move |lua_inner, args: MultiValue| -> mlua::Result<()> {
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
        })
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?
    };

    let load_api = lua
        .create_table()
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    install_redis_api_constants(&load_api)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    load_api
        .raw_set("register_function", register_fn)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    let load_api = readonly_table_proxy_with_missing_global_errors(&lua, load_api)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    lua.globals()
        .set("redis", load_api.clone())
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    lua.globals()
        .set("server", load_api)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    install_eval_global_protection(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;

    let function_env = create_script_environment(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    let load_result = lua
        .load(library_body)
        .set_name("function_library")
        .set_environment(function_env)
        .exec();
    if let Err(err) = load_result {
        return Err(function_load_lua_error(err));
    }
    load_phase.set(false);

    lua.globals()
        .raw_set("redis", redis_tbl.clone())
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    lua.globals()
        .raw_set("server", redis_tbl.clone())
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    let redis_api = readonly_table_proxy(&lua, redis_tbl)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    lua.globals()
        .raw_set("redis", redis_api.clone())
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;
    lua.globals()
        .raw_set("server", redis_api)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;

    let registrations = registrations.borrow_mut().drain(..).collect::<Vec<_>>();
    if registrations.is_empty() {
        return Err(RedisError::runtime(
            b"ERR No functions registered in library",
        ));
    }
    Ok(CachedFunctionRuntime {
        library_name: library.name.clone(),
        library_code: library.code.clone(),
        lua,
        registrations,
    })
}

pub(super) fn take_cached_function_runtime(
    library: &LoadedFunctionLibrary,
) -> RedisResult<CachedFunctionRuntime> {
    CACHED_FUNCTION_RUNTIME.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|runtime| runtime.matches_library(library))
        {
            Ok(slot.take().expect("checked cache presence"))
        } else {
            *slot = None;
            compile_cached_function_runtime(library)
        }
    })
}

pub(super) fn store_cached_function_runtime(runtime: CachedFunctionRuntime) {
    CACHED_FUNCTION_RUNTIME.with(|slot| {
        *slot.borrow_mut() = Some(runtime);
    });
}
