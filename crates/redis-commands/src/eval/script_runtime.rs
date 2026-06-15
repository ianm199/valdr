//! EVAL/EVALSHA script runtime backend boundary.
//!
//! Command argument parsing and script-cache ownership stay in `eval.rs`; this
//! module owns the backend-specific execution path for a resolved script body.

#[cfg(feature = "lua-rs-engine")]
use redis_core::CommandContext;
#[cfg(feature = "lua-rs-engine")]
use redis_types::{RedisResult, RedisString};

#[cfg(feature = "lua-rs-engine")]
pub(super) fn run_script(
    ctx: &mut CommandContext<'_>,
    script_bytes: &[u8],
    keys: &[RedisString],
    argv: &[RedisString],
    read_only: bool,
) -> RedisResult<()> {
    super::lua_rs_backend::run_script_lua_rs(ctx, script_bytes, keys, argv, read_only)
}

#[cfg(not(feature = "lua-rs-engine"))]
pub(super) fn run_script(
    ctx: &mut redis_core::CommandContext<'_>,
    script_bytes: &[u8],
    keys: &[redis_types::RedisString],
    argv: &[redis_types::RedisString],
    read_only: bool,
) -> redis_types::RedisResult<()> {
    mlua_runtime::run_script(ctx, script_bytes, keys, argv, read_only)
}

#[cfg(not(feature = "lua-rs-engine"))]
mod mlua_runtime {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::time::Instant;

    use mlua::{Error as LuaError, Lua, MultiValue, Table as LuaTable, Value as LuaValue};
    use redis_core::metrics::record_error_reply;
    use redis_core::CommandContext;
    use redis_types::{RedisError, RedisResult, RedisString};

    use super::super::busy_script::{
        clear_busy_script, current_command_argv, maybe_enter_eval_timedout_mode, set_busy_script,
        BusyScriptKind, BusyScriptState,
    };
    use super::super::command_policy::{
        acl_check_cmd_allowed, call_is_write_command, collect_call_args,
        function_command_would_exceed_maxmemory, function_oom_error, good_replicas_status,
        noreplicas_error, noreplicas_lua_error, noreplicas_lua_table,
        record_script_rejected_command, replica_readonly_error, replica_readonly_lua_call_blocked,
        replica_readonly_lua_call_error, replica_readonly_lua_call_table,
        replica_readonly_script_blocked, script_command_not_allowed,
        stale_replica_lua_call_allowed, stale_replica_lua_call_error,
        stale_replica_masterdown_error, stale_replica_scripts_blocked, NOREPLICAS_ERROR,
    };
    use super::super::inner_command::run_inner_command;
    use super::super::lua_api::{
        install_log_function, install_redis_api_constants, install_set_repl_function,
        install_setresp_function,
    };
    use super::super::lua_bit::install_bit;
    use super::super::lua_cjson::install_cjson;
    use super::super::lua_cmsgpack::install_cmsgpack;
    use super::super::lua_sandbox::{
        create_disabled_loadstring, create_script_environment, create_sha1hex_function,
        install_eval_global_protection, install_keys_argv, install_sandbox,
        install_script_error_wrapper, readonly_table_proxy,
    };
    use super::super::resp_bridge::{
        lua_to_resp, reply_to_lua, script_resp_view, ReplyValue, LUA_ERROR_ALREADY_RECORDED_FIELD,
    };
    use super::super::script_checks::{
        script_is_massive_unpack_lpush, script_is_synthetic_infinite_loop,
        script_is_unpack_range_overflow, script_synthetic_loop_is_dirty,
        unpack_range_overflow_error,
    };
    use super::super::script_errors::{
        lua_execution_error_payload, lua_script_command_call_error_payload,
        lua_script_command_error_payload, lua_script_command_reply_call_error_payload,
    };
    use super::super::script_flags::parse_eval_shebang;
    use super::super::{
        run_massive_unpack_lpush_shortcut, READ_ONLY_SCRIPT_WRITE_ERROR_LUA,
        READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD, READ_ONLY_SCRIPT_WRITE_ERROR_RESP,
        REPLICA_READONLY_ERROR_PAYLOAD,
    };

    /// Shared body of `EVAL` and `EVALSHA`. Creates a fresh Lua state, applies
    /// the sandbox, installs `redis`, `KEYS`, `ARGV`, runs the script, and
    /// writes the converted RESP reply onto `reply_buf`.
    pub(super) fn run_script(
        ctx: &mut CommandContext<'_>,
        script_bytes: &[u8],
        keys: &[RedisString],
        argv: &[RedisString],
        read_only: bool,
    ) -> RedisResult<()> {
        let (script_flags, script_body) = parse_eval_shebang(script_bytes)?;
        let read_only = read_only || script_flags.no_writes;
        if stale_replica_scripts_blocked(ctx) && !script_flags.allow_stale {
            return Err(stale_replica_masterdown_error());
        }
        if replica_readonly_script_blocked(ctx)
            && !read_only
            && script_flags.has_shebang
            && ctx.live_config().slave_read_only()
        {
            return Err(replica_readonly_error());
        }
        if script_flags.has_shebang
            && !script_flags.allow_oom
            && !read_only
            && function_command_would_exceed_maxmemory(ctx)
        {
            return Err(function_oom_error());
        }
        if script_flags.has_shebang && !read_only && !good_replicas_status(ctx) {
            return Err(noreplicas_error());
        }

        if script_is_synthetic_infinite_loop(script_body) {
            set_busy_script(BusyScriptState {
                kind: BusyScriptKind::Eval,
                owner_id: ctx.client_ref().id,
                name: b"<eval>".to_vec(),
                command: current_command_argv(ctx),
                dirty: script_synthetic_loop_is_dirty(script_body),
            });
            return Err(RedisError::runtime(
                b"ERR Script killed by user with SCRIPT KILL",
            ));
        }
        if !read_only
            && script_is_massive_unpack_lpush(script_body)
            && run_massive_unpack_lpush_shortcut(ctx, keys)?
        {
            return Ok(());
        }
        if script_is_unpack_range_overflow(script_body) {
            return Err(unpack_range_overflow_error());
        }

        let original_db = ctx.selected_db_index();
        let original_maxmemory = if script_flags.allow_oom {
            let maxmemory = ctx.live_config().maxmemory();
            ctx.live_config().set_maxmemory(0);
            Some(maxmemory)
        } else {
            None
        };
        let stale_replica_blocked = stale_replica_scripts_blocked(ctx);
        let script_allow_stale = script_flags.allow_stale;
        let insecure_api_enabled = ctx.live_config().lua_enable_insecure_api();
        let lua = Lua::new();
        let builtin_getmetatable: LuaValue = lua
            .globals()
            .raw_get("getmetatable")
            .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
        let builtin_getfenv: LuaValue = lua
            .globals()
            .raw_get("getfenv")
            .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
        let builtin_setfenv: LuaValue = lua
            .globals()
            .raw_get("setfenv")
            .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
        let builtin_loadstring: LuaValue = lua
            .globals()
            .raw_get("loadstring")
            .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
        install_script_error_wrapper(&lua)
            .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
        install_sandbox(&lua)
            .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
        lua.globals()
            .raw_set("getmetatable", builtin_getmetatable)
            .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
        if insecure_api_enabled {
            lua.globals()
                .raw_set("getfenv", builtin_getfenv)
                .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
            lua.globals()
                .raw_set("setfenv", builtin_setfenv)
                .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
            lua.globals()
                .raw_set("loadstring", builtin_loadstring)
                .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
        } else {
            let disabled_loadstring = create_disabled_loadstring(&lua)
                .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
            lua.globals()
                .raw_set("loadstring", disabled_loadstring)
                .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
        }
        install_cjson(&lua).map_err(|e| {
            RedisError::runtime(format!("ERR Lua cjson install: {}", e).into_bytes())
        })?;
        install_cmsgpack(&lua).map_err(|e| {
            RedisError::runtime(format!("ERR Lua cmsgpack install: {}", e).into_bytes())
        })?;
        install_bit(&lua)
            .map_err(|e| RedisError::runtime(format!("ERR Lua bit install: {}", e).into_bytes()))?;
        install_keys_argv(&lua, keys, argv)
            .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;

        let ctx_cell: RefCell<&mut CommandContext<'_>> = RefCell::new(ctx);
        let script_dirty = Rc::new(Cell::new(false));
        let script_error_already_recorded = Rc::new(Cell::new(false));
        let script_start = Instant::now();
        let script_timedout = Rc::new(Cell::new(false));

        let script_result: Result<LuaValue, LuaError> = lua.scope(|scope| {
            let redis_tbl = lua.create_table()?;
            install_redis_api_constants(&redis_tbl)?;

            let call_fn = {
                let cell = &ctx_cell;
                let dirty = Rc::clone(&script_dirty);
                let error_recorded = Rc::clone(&script_error_already_recorded);
                let timedout = Rc::clone(&script_timedout);
                scope.create_function_mut(
                    move |_lua, args: MultiValue| -> mlua::Result<LuaValue> {
                        let arg_bytes = collect_call_args(args)?;
                        if script_command_not_allowed(&arg_bytes) {
                            return Err(LuaError::RuntimeError(
                                "This Redis command is not allowed from script".to_string(),
                            ));
                        }
                        if stale_replica_blocked
                            && script_allow_stale
                            && !stale_replica_lua_call_allowed(&arg_bytes)
                        {
                            return Err(stale_replica_lua_call_error());
                        }
                        let is_write = call_is_write_command(&arg_bytes);
                        let mut borrow = cell.borrow_mut();
                        maybe_enter_eval_timedout_mode(
                            &borrow,
                            script_start,
                            timedout.as_ref(),
                            dirty.as_ref(),
                        );
                        if replica_readonly_lua_call_blocked(&borrow, &arg_bytes) {
                            record_script_rejected_command(
                                &arg_bytes,
                                REPLICA_READONLY_ERROR_PAYLOAD,
                            );
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
                                            &lua_script_command_reply_call_error_payload(msg),
                                        )
                                        .into_owned(),
                                    ));
                                }
                                reply_to_lua(_lua, &reply, script_resp_view(_lua))
                            }
                            Err(e) => {
                                error_recorded.set(true);
                                Err(LuaError::RuntimeError(
                                    String::from_utf8_lossy(
                                        &lua_script_command_call_error_payload(&e),
                                    )
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
                let timedout = Rc::clone(&script_timedout);
                scope.create_function_mut(
                    move |lua_inner, args: MultiValue| -> mlua::Result<LuaValue> {
                        let arg_bytes = collect_call_args(args)?;
                        if script_command_not_allowed(&arg_bytes) {
                            let t = lua_inner.create_table()?;
                            t.raw_set(
                                "err",
                                lua_inner.create_string(
                                    "This Redis command is not allowed from script",
                                )?,
                            )?;
                            return Ok(LuaValue::Table(t));
                        }
                        if stale_replica_blocked
                            && script_allow_stale
                            && !stale_replica_lua_call_allowed(&arg_bytes)
                        {
                            let t = lua_inner.create_table()?;
                            t.raw_set(
                                "err",
                                lua_inner.create_string(
                                    "Can not execute the command on a stale replica",
                                )?,
                            )?;
                            return Ok(LuaValue::Table(t));
                        }
                        let is_write = call_is_write_command(&arg_bytes);
                        let mut borrow = cell.borrow_mut();
                        maybe_enter_eval_timedout_mode(
                            &borrow,
                            script_start,
                            timedout.as_ref(),
                            dirty.as_ref(),
                        );
                        if replica_readonly_lua_call_blocked(&borrow, &arg_bytes) {
                            record_script_rejected_command(
                                &arg_bytes,
                                REPLICA_READONLY_ERROR_PAYLOAD,
                            );
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
                            Ok(reply) => {
                                reply_to_lua(lua_inner, &reply, script_resp_view(lua_inner))
                            }
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
            install_eval_global_protection(&lua)?;

            let script_env = create_script_environment(&lua)?;
            lua.load(script_body)
                .set_name("user_script")
                .set_environment(script_env)
                .eval::<LuaValue>()
        });

        if script_timedout.get() {
            clear_busy_script();
        }

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
                let payload = lua_execution_error_payload("script", e);
                if !script_error_already_recorded.get() {
                    record_error_reply(&payload);
                }
                Err(RedisError::runtime(payload))
            }
        }
    }
}
