use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Instant;

use lua_rs_runtime::{
    Lua, LuaError, LuaString, LuaVersion, Table as LuaTable, Value as LuaValue, Variadic,
};
use redis_core::acl::global_acl_state;
use redis_core::db::glob_match;
use redis_core::metrics::record_error_reply;
use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult, RedisString};

use super::{
    call_is_write_command, clear_busy_script, current_command_argv,
    function_command_would_exceed_maxmemory, function_oom_error, good_replicas_status,
    lua_error_reply_wire_bytes, lua_script_command_call_error_payload,
    lua_script_command_error_payload, lua_script_command_reply_call_error_payload,
    maybe_enter_eval_timedout_mode, noreplicas_error, parse_eval_shebang,
    record_script_rejected_command, replica_readonly_error, replica_readonly_lua_call_blocked,
    replica_readonly_lua_call_payload, replica_readonly_script_blocked, run_inner_command,
    run_massive_unpack_lpush_shortcut, runtime_error_payload, script_command_not_allowed,
    script_is_massive_unpack_lpush, script_is_synthetic_infinite_loop,
    script_is_unpack_range_overflow, script_synthetic_loop_is_dirty, set_busy_script, sha1_hex,
    stale_replica_lua_call_allowed, stale_replica_masterdown_error, stale_replica_scripts_blocked,
    unpack_range_overflow_error, BusyScriptKind, BusyScriptState, ReplyValue,
    LUA_ERROR_ALREADY_RECORDED_FIELD, LUA_REDIS_VERSION, LUA_REDIS_VERSION_NUM, NOREPLICAS_ERROR,
    READ_ONLY_SCRIPT_WRITE_ERROR_LUA, READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD,
    READ_ONLY_SCRIPT_WRITE_ERROR_RESP, REPLICA_READONLY_ERROR_PAYLOAD,
};

pub(super) fn run_script_lua_rs(
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
    if replica_readonly_script_blocked(ctx) && !read_only && script_flags.has_shebang {
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
    let lua = Lua::new_versioned(LuaVersion::V51);

    install_script_error_wrapper_lua_rs(&lua).map_err(|e| {
        RedisError::runtime(format!("ERR Lua sandbox: {}", e.message_lossy()).into_bytes())
    })?;
    install_sandbox_lua_rs(&lua).map_err(|e| {
        RedisError::runtime(format!("ERR Lua sandbox: {}", e.message_lossy()).into_bytes())
    })?;
    install_keys_argv_lua_rs(&lua, keys, argv).map_err(|e| {
        RedisError::runtime(format!("ERR Lua install: {}", e.message_lossy()).into_bytes())
    })?;

    let ctx_cell: RefCell<&mut CommandContext<'_>> = RefCell::new(ctx);
    let script_dirty = Rc::new(Cell::new(false));
    let script_error_already_recorded = Rc::new(Cell::new(false));
    let script_start = Instant::now();
    let script_timedout = Rc::new(Cell::new(false));
    let resp_view = Rc::new(Cell::new(2_u8));

    let script_result: Result<LuaValue, LuaError> = lua.scope(|scope| {
        let redis_tbl = lua.create_table()?;
        install_redis_api_constants_lua_rs(&redis_tbl)?;

        let call_fn = {
            let cell = &ctx_cell;
            let dirty = Rc::clone(&script_dirty);
            let error_recorded = Rc::clone(&script_error_already_recorded);
            let timedout = Rc::clone(&script_timedout);
            let resp_view = Rc::clone(&resp_view);
            scope.create_function_mut(
                &lua,
                move |lua_inner, args: Variadic<LuaValue>| -> lua_rs_runtime::Result<LuaValue> {
                    let arg_bytes = collect_call_args_lua_rs(args)?;
                    if script_command_not_allowed(&arg_bytes) {
                        return Err(lua_runtime_error(
                            "This Redis command is not allowed from script",
                        ));
                    }
                    if stale_replica_blocked
                        && script_allow_stale
                        && !stale_replica_lua_call_allowed(&arg_bytes)
                    {
                        return Err(lua_runtime_error(
                            "Can not execute the command on a stale replica",
                        ));
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
                        record_script_rejected_command(&arg_bytes, REPLICA_READONLY_ERROR_PAYLOAD);
                        error_recorded.set(true);
                        return Err(lua_runtime_error_bytes(&replica_readonly_lua_call_payload()));
                    }
                    if read_only && is_write {
                        record_script_rejected_command(
                            &arg_bytes,
                            READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD,
                        );
                        error_recorded.set(true);
                        return Err(lua_runtime_error(READ_ONLY_SCRIPT_WRITE_ERROR_LUA));
                    }
                    if is_write && !good_replicas_status(&borrow) {
                        record_script_rejected_command(&arg_bytes, NOREPLICAS_ERROR.as_bytes());
                        error_recorded.set(true);
                        return Err(lua_runtime_error(NOREPLICAS_ERROR));
                    }
                    match run_inner_command(&mut borrow, &arg_bytes, Some(dirty.as_ref())) {
                        Ok(reply) => {
                            if let ReplyValue::Error(msg) = &reply {
                                error_recorded.set(true);
                                return Err(lua_runtime_error_bytes(
                                    &lua_script_command_reply_call_error_payload(msg),
                                ));
                            }
                            reply_to_lua_rs(lua_inner, &reply, resp_view.get())
                        }
                        Err(e) => {
                            error_recorded.set(true);
                            Err(lua_runtime_error_bytes(
                                &lua_script_command_call_error_payload(&e),
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
            let resp_view = Rc::clone(&resp_view);
            scope.create_function_mut(
                &lua,
                move |lua_inner, args: Variadic<LuaValue>| -> lua_rs_runtime::Result<LuaValue> {
                    let arg_bytes = collect_call_args_lua_rs(args)?;
                    if script_command_not_allowed(&arg_bytes) {
                        return error_table_lua_rs(
                            lua_inner,
                            b"This Redis command is not allowed from script",
                            false,
                        );
                    }
                    if stale_replica_blocked
                        && script_allow_stale
                        && !stale_replica_lua_call_allowed(&arg_bytes)
                    {
                        return error_table_lua_rs(
                            lua_inner,
                            b"Can not execute the command on a stale replica",
                            false,
                        );
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
                        record_script_rejected_command(&arg_bytes, REPLICA_READONLY_ERROR_PAYLOAD);
                        return error_table_lua_rs(
                            lua_inner,
                            &replica_readonly_lua_call_payload(),
                            true,
                        );
                    }
                    if read_only && is_write {
                        record_script_rejected_command(
                            &arg_bytes,
                            READ_ONLY_SCRIPT_WRITE_ERROR_PAYLOAD,
                        );
                        return error_table_lua_rs(
                            lua_inner,
                            READ_ONLY_SCRIPT_WRITE_ERROR_RESP.as_bytes(),
                            true,
                        );
                    }
                    if is_write && !good_replicas_status(&borrow) {
                        record_script_rejected_command(&arg_bytes, NOREPLICAS_ERROR.as_bytes());
                        return error_table_lua_rs(lua_inner, NOREPLICAS_ERROR.as_bytes(), true);
                    }
                    match run_inner_command(&mut borrow, &arg_bytes, Some(dirty.as_ref())) {
                        Ok(reply) => reply_to_lua_rs(lua_inner, &reply, resp_view.get()),
                        Err(e) => error_table_lua_rs(
                            lua_inner,
                            &lua_script_command_error_payload(&e),
                            true,
                        ),
                    }
                },
            )?
        };

        let error_reply_fn = lua.create_function(
            |lua_inner, msg: LuaString| -> lua_rs_runtime::Result<LuaTable> {
                let table = lua_inner.create_table()?;
                table.set("err", msg)?;
                Ok(table)
            },
        )?;
        let status_reply_fn = lua.create_function(
            |lua_inner, msg: LuaString| -> lua_rs_runtime::Result<LuaTable> {
                let table = lua_inner.create_table()?;
                table.set("ok", msg)?;
                Ok(table)
            },
        )?;
        let sha1hex_fn = lua.create_function(
            |_lua_inner, args: Variadic<LuaValue>| -> lua_rs_runtime::Result<String> {
                if args.len() != 1 {
                    return Err(lua_runtime_error(
                        "wrong number of arguments to redis.sha1hex",
                    ));
                }
                let Some(LuaValue::String(s)) = args.first() else {
                    return Err(lua_runtime_error("bad argument #1 to redis.sha1hex"));
                };
                let bytes = s.as_bytes()?;
                let hex = sha1_hex(&bytes);
                Ok(String::from_utf8(hex.to_vec()).unwrap_or_default())
            },
        )?;
        let replicate_fn = lua.create_function(
            |_lua_inner, _args: Variadic<LuaValue>| -> lua_rs_runtime::Result<bool> { Ok(true) },
        )?;
        let setresp_fn = {
            let resp_view = Rc::clone(&resp_view);
            lua.create_function(move |_lua_inner, n: i64| -> lua_rs_runtime::Result<()> {
                if n != 2 && n != 3 {
                    return Err(lua_runtime_error("ERR RESP version must be 2 or 3."));
                }
                resp_view.set(n as u8);
                Ok(())
            })?
        };
        let set_repl_fn = lua.create_function(
            |_lua_inner, args: Variadic<LuaValue>| -> lua_rs_runtime::Result<()> {
                if args.len() != 1 {
                    return Err(lua_runtime_error(
                        "ERR server.set_repl() requires one argument.",
                    ));
                }
                let flags = match args.first() {
                    Some(LuaValue::Integer(n)) => *n,
                    Some(LuaValue::Number(n)) => *n as i64,
                    _ => 0,
                };
                if !(0..=3).contains(&flags) {
                    return Err(lua_runtime_error("Invalid replication flags"));
                }
                Ok(())
            },
        )?;
        let log_fn = lua.create_function(
            |_lua_inner, args: Variadic<LuaValue>| -> lua_rs_runtime::Result<()> {
                if args.len() < 2 {
                    return Err(lua_runtime_error(
                        "ERR server.log() requires two arguments or more.",
                    ));
                }
                let level = match args.first() {
                    Some(LuaValue::Integer(n)) => *n,
                    Some(LuaValue::Number(n)) => *n as i64,
                    _ => {
                        return Err(lua_runtime_error(
                            "ERR First argument must be a number (log level).",
                        ));
                    }
                };
                if !(0..=3).contains(&level) {
                    return Err(lua_runtime_error("ERR Invalid log level."));
                }
                let message = args
                    .iter()
                    .skip(1)
                    .filter_map(lua_rs_log_arg_to_string)
                    .collect::<Vec<_>>()
                    .join(" ");
                crate::connection::log_server_notice(&message);
                Ok(())
            },
        )?;
        let acl_check_fn = {
            let cell = &ctx_cell;
            scope.create_function_mut(
                &lua,
                move |_lua_inner, args: Variadic<LuaValue>| -> lua_rs_runtime::Result<bool> {
                    let arg_bytes = collect_call_args_lua_rs(args)?;
                    let borrow = cell.borrow();
                    acl_check_cmd_allowed_lua_rs(&borrow, &arg_bytes)
                },
            )?
        };

        redis_tbl.set("__raw_call", call_fn.clone())?;
        redis_tbl.set("call", call_fn)?;
        redis_tbl.set("pcall", pcall_fn)?;
        redis_tbl.set("error_reply", error_reply_fn)?;
        redis_tbl.set("status_reply", status_reply_fn)?;
        redis_tbl.set("sha1hex", sha1hex_fn)?;
        redis_tbl.set("replicate_commands", replicate_fn)?;
        redis_tbl.set("set_repl", set_repl_fn)?;
        redis_tbl.set("setresp", setresp_fn)?;
        redis_tbl.set("log", log_fn)?;
        redis_tbl.set("acl_check_cmd", acl_check_fn)?;
        lua.globals().set("redis", redis_tbl.clone())?;
        lua.globals().set("server", redis_tbl)?;
        install_eval_global_protection_lua_rs(&lua)?;

        lua.load(script_body)
            .set_name("user_script")
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
            lua_rs_to_resp(&value, &mut out, resp3).map_err(|e| {
                RedisError::runtime(lua_execution_error_payload_lua_rs("script", e))
            })?;
            ctx.client_mut().reply_buf.extend_from_slice(&out);
            Ok(())
        }
        Err(e) => {
            let payload = lua_execution_error_payload_lua_rs("script", e);
            if !script_error_already_recorded.get() {
                record_error_reply(&payload);
            }
            Err(RedisError::runtime(payload))
        }
    }
}

fn install_script_error_wrapper_lua_rs(lua: &Lua) -> lua_rs_runtime::Result<()> {
    lua.load(
        r#"
        local raw_error = error
        local raw_getinfo = debug and debug.getinfo

        local function sanitize_error_message(msg)
            if type(msg) ~= "string" then
                msg = "ERR unknown error"
            end
            msg = string.match(msg, "^[^\r\n]*") or ""
            if msg == "" then
                msg = "ERR"
            end
            if string.sub(msg, 1, 1) == "-" then
                msg = string.sub(msg, 2)
            end
            local code = string.match(msg, "^[^ \t]*") or ""
            if string.sub(msg, 1, 4) ~= "ERR " and not string.match(code, "^[A-Z0-9_]+$") then
                msg = "ERR " .. msg
            end
            return msg
        end

        error = function(value, level)
            if type(value) == "table" then
                raw_error(sanitize_error_message(value.err) .. " script: unknown", 0)
            end

            if level ~= nil then
                if level > 0 then
                    level = level + 1
                end
                raw_error(value, level)
            end

            if type(value) == "string" then
                if string.sub(value, 1, 1) == "-" or string.find(value, "\r", 1, true) or string.find(value, "\n", 1, true) then
                    raw_error(sanitize_error_message(value) .. " script: unknown", 0)
                end
                local src = "user_script"
                local line = 1
                if raw_getinfo ~= nil then
                    local info = raw_getinfo(2, "Sl")
                    if info ~= nil then
                        src = info.short_src or src
                        line = info.currentline or line
                    end
                end
                if src == '[string "user_script"]' then
                    src = "user_script"
                end
                raw_error("ERR " .. src .. ":" .. tostring(line) .. ": " .. value .. " script: unknown", 0)
            end

            raw_error(sanitize_error_message(nil) .. " script: unknown", 0)
        end
        "#,
    )
    .set_name("script_error_wrapper")
    .exec()
}

fn install_sandbox_lua_rs(lua: &Lua) -> lua_rs_runtime::Result<()> {
    let globals = lua.globals();
    for name in [
        "io",
        "debug",
        "package",
        "require",
        "load",
        "loadfile",
        "dofile",
        "loadstring",
        "print",
        "getfenv",
        "setfenv",
    ] {
        globals.set(name, LuaValue::Nil)?;
    }
    let os = lua.create_table()?;
    os.set(
        "clock",
        lua.create_function(|_lua_inner, ()| Ok(super::os_clock_seconds()))?,
    )?;
    globals.set("os", os)?;
    Ok(())
}

fn install_eval_global_protection_lua_rs(lua: &Lua) -> lua_rs_runtime::Result<()> {
    lua.load(
        r#"
        local raw_setmetatable = setmetatable
        local raw_getmetatable = getmetatable
        local global_meta = {
            __index = function(_, key)
                error("Script attempted to access nonexistent global variable '" .. tostring(key) .. "'", 2)
            end,
            __newindex = function(_, key, _)
                error("Attempt to modify a readonly table", 2)
            end
        }
        local readonly_global_meta = {}
        raw_setmetatable(readonly_global_meta, {
            __index = global_meta,
            __newindex = function(_, _, _)
                error("Attempt to modify a readonly table", 2)
            end,
            __metatable = false
        })
        global_meta.__metatable = readonly_global_meta
        local protected_setmetatable = function(t, mt)
            if t == _G then
                error("Attempt to modify a readonly table", 2)
            end
            return raw_setmetatable(t, mt)
        end
        local protected_getmetatable = function(t)
            if t == _G then
                return readonly_global_meta
            end
            if type(t) ~= "table" then
                return nil
            end
            return raw_getmetatable(t)
        end
        setmetatable = protected_setmetatable
        getmetatable = protected_getmetatable
        raw_setmetatable(_G, global_meta)
        "#,
    )
    .set_name("eval_global_protection")
    .exec()
}

fn install_keys_argv_lua_rs(
    lua: &Lua,
    keys: &[RedisString],
    argv: &[RedisString],
) -> lua_rs_runtime::Result<()> {
    let keys_t = lua.create_table()?;
    for (i, key) in keys.iter().enumerate() {
        keys_t.set(i as i64 + 1, lua.create_string(key.as_bytes())?)?;
    }
    lua.globals().set("KEYS", keys_t)?;

    let argv_t = lua.create_table()?;
    for (i, arg) in argv.iter().enumerate() {
        argv_t.set(i as i64 + 1, lua.create_string(arg.as_bytes())?)?;
    }
    lua.globals().set("ARGV", argv_t)?;
    Ok(())
}

fn install_redis_api_constants_lua_rs(redis_tbl: &LuaTable) -> lua_rs_runtime::Result<()> {
    redis_tbl.set("REDIS_VERSION", LUA_REDIS_VERSION)?;
    redis_tbl.set("REDIS_VERSION_NUM", LUA_REDIS_VERSION_NUM)?;
    redis_tbl.set("REPL_NONE", 0_i64)?;
    redis_tbl.set("REPL_AOF", 1_i64)?;
    redis_tbl.set("REPL_SLAVE", 2_i64)?;
    redis_tbl.set("REPL_REPLICA", 2_i64)?;
    redis_tbl.set("REPL_ALL", 3_i64)?;
    redis_tbl.set("LOG_DEBUG", 0_i64)?;
    redis_tbl.set("LOG_VERBOSE", 1_i64)?;
    redis_tbl.set("LOG_NOTICE", 2_i64)?;
    redis_tbl.set("LOG_WARNING", 3_i64)?;
    Ok(())
}

fn reply_to_lua_rs(
    lua: &Lua,
    value: &ReplyValue,
    resp_view: u8,
) -> lua_rs_runtime::Result<LuaValue> {
    match value {
        ReplyValue::Null => Ok(LuaValue::Nil),
        ReplyValue::Nil => Ok(LuaValue::Boolean(false)),
        ReplyValue::SimpleString(s) => {
            let table = lua.create_table()?;
            table.set("ok", lua.create_string(s)?)?;
            Ok(LuaValue::Table(table))
        }
        ReplyValue::Error(s) => {
            let table = lua.create_table()?;
            table.set("err", lua.create_string(s)?)?;
            table.set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
            Ok(LuaValue::Table(table))
        }
        ReplyValue::Integer(n) => Ok(LuaValue::Integer(*n)),
        ReplyValue::Bool(v) => Ok(LuaValue::Boolean(*v)),
        ReplyValue::Double(n) => {
            let table = lua.create_table()?;
            table.set("double", *n)?;
            Ok(LuaValue::Table(table))
        }
        ReplyValue::BigNumber(n) => {
            let table = lua.create_table()?;
            table.set("big_number", lua.create_string(n)?)?;
            Ok(LuaValue::Table(table))
        }
        ReplyValue::VerbatimString { format, data } => {
            let table = lua.create_table()?;
            let verbatim = lua.create_table()?;
            verbatim.set("string", lua.create_string(data)?)?;
            verbatim.set("format", lua.create_string(format)?)?;
            table.set("verbatim_string", verbatim)?;
            Ok(LuaValue::Table(table))
        }
        ReplyValue::Bulk(b) => Ok(LuaValue::String(lua.create_string(b)?)),
        ReplyValue::Array(items) => {
            let table = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                table.set(i as i64 + 1, reply_to_lua_rs(lua, item, resp_view)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        ReplyValue::Map(items) => {
            if resp_view >= 3 {
                let out = lua.create_table()?;
                let map = lua.create_table()?;
                for pair in items.chunks(2) {
                    if pair.len() != 2 {
                        continue;
                    }
                    let key = reply_to_lua_rs(lua, &pair[0], resp_view)?;
                    let value = reply_to_lua_rs(lua, &pair[1], resp_view)?;
                    map.set(key, value)?;
                }
                out.set("map", map)?;
                Ok(LuaValue::Table(out))
            } else {
                let table = lua.create_table()?;
                for (i, item) in items.iter().enumerate() {
                    table.set(i as i64 + 1, reply_to_lua_rs(lua, item, resp_view)?)?;
                }
                Ok(LuaValue::Table(table))
            }
        }
        ReplyValue::Set(items) => {
            if resp_view >= 3 {
                let out = lua.create_table()?;
                let set = lua.create_table()?;
                for item in items {
                    set.set(reply_to_lua_rs(lua, item, resp_view)?, true)?;
                }
                out.set("set", set)?;
                Ok(LuaValue::Table(out))
            } else {
                let table = lua.create_table()?;
                for (i, item) in items.iter().enumerate() {
                    table.set(i as i64 + 1, reply_to_lua_rs(lua, item, resp_view)?)?;
                }
                Ok(LuaValue::Table(table))
            }
        }
    }
}

fn lua_rs_to_resp(value: &LuaValue, out: &mut Vec<u8>, resp3: bool) -> lua_rs_runtime::Result<()> {
    lua_rs_to_resp_inner(value, out, resp3, 0)
}

fn lua_rs_to_resp_inner(
    value: &LuaValue,
    out: &mut Vec<u8>,
    resp3: bool,
    depth: usize,
) -> lua_rs_runtime::Result<()> {
    if depth > super::LUA_REPLY_MAX_DEPTH {
        out.extend_from_slice(b"-ERR reached lua stack limit\r\n");
        return Ok(());
    }

    match value {
        LuaValue::Nil => out.extend_from_slice(b"$-1\r\n"),
        LuaValue::Boolean(true) => out.extend_from_slice(b":1\r\n"),
        LuaValue::Boolean(false) => out.extend_from_slice(b"$-1\r\n"),
        LuaValue::Integer(n) => {
            out.push(b':');
            out.extend_from_slice(n.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        LuaValue::Number(f) => {
            let n = *f as i64;
            out.push(b':');
            out.extend_from_slice(n.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        LuaValue::String(s) => {
            let bytes = s.as_bytes()?;
            out.push(b'$');
            out.extend_from_slice(bytes.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(&bytes);
            out.extend_from_slice(b"\r\n");
        }
        LuaValue::Table(t) => {
            if let Some(err) = table_string_bytes(t, "err")? {
                let wire_error = lua_error_reply_wire_bytes(&err);
                let already_recorded = t
                    .get::<_, Option<bool>>(LUA_ERROR_ALREADY_RECORDED_FIELD)?
                    .unwrap_or(false);
                if !already_recorded {
                    record_error_reply(&wire_error);
                }
                out.extend_from_slice(&wire_error);
                out.extend_from_slice(b"\r\n");
                return Ok(());
            }
            if let Some(ok) = table_string_bytes(t, "ok")? {
                out.push(b'+');
                out.extend_from_slice(&ok);
                out.extend_from_slice(b"\r\n");
                return Ok(());
            }
            if let Some(n) = t.get::<_, Option<f64>>("double")? {
                out.push(b',');
                out.extend_from_slice(n.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                return Ok(());
            }

            // TODO(lua-rs-port): Redis RESP3 map/set conversion needs public
            // table iteration or raw-entry APIs in lua-rs-runtime.
            let _ = resp3;

            let mut items: Vec<LuaValue> = Vec::new();
            let mut i: i64 = 1;
            loop {
                let value = t.get::<_, LuaValue>(i)?;
                if matches!(value, LuaValue::Nil) {
                    break;
                }
                items.push(value);
                i += 1;
            }
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in &items {
                lua_rs_to_resp_inner(item, out, resp3, depth + 1)?;
            }
        }
        _ => out.extend_from_slice(b"$-1\r\n"),
    }
    Ok(())
}

fn table_string_bytes(table: &LuaTable, key: &str) -> lua_rs_runtime::Result<Option<Vec<u8>>> {
    match table.get::<_, Option<LuaString>>(key)? {
        Some(s) => Ok(Some(s.as_bytes()?)),
        None => Ok(None),
    }
}

fn collect_call_args_lua_rs(args: Variadic<LuaValue>) -> lua_rs_runtime::Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(args.len());
    for value in args {
        out.push(lua_rs_arg_to_bytes(&value)?);
    }
    Ok(out)
}

fn lua_rs_arg_to_bytes(value: &LuaValue) -> lua_rs_runtime::Result<Vec<u8>> {
    match value {
        LuaValue::String(s) => s.as_bytes(),
        LuaValue::Integer(n) => Ok(n.to_string().into_bytes()),
        LuaValue::Number(f) => {
            if f.fract() == 0.0 && f.is_finite() {
                Ok((*f as i64).to_string().into_bytes())
            } else {
                Ok(format!("{f}").into_bytes())
            }
        }
        LuaValue::Boolean(true) => Ok(b"1".to_vec()),
        LuaValue::Boolean(false) => Ok(b"0".to_vec()),
        _ => Err(lua_runtime_error(
            "Command arguments must be strings or integers",
        )),
    }
}

fn error_table_lua_rs(
    lua: &Lua,
    message: &[u8],
    already_recorded: bool,
) -> lua_rs_runtime::Result<LuaValue> {
    let table = lua.create_table()?;
    table.set("err", lua.create_string(message)?)?;
    if already_recorded {
        table.set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
    }
    Ok(LuaValue::Table(table))
}

fn lua_rs_log_arg_to_string(value: &LuaValue) -> Option<String> {
    match value {
        LuaValue::String(s) => s
            .as_bytes()
            .ok()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned()),
        LuaValue::Integer(n) => Some(n.to_string()),
        LuaValue::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn acl_check_cmd_allowed_lua_rs(
    ctx: &CommandContext<'_>,
    args: &[Vec<u8>],
) -> lua_rs_runtime::Result<bool> {
    let Some(command) = args.first() else {
        return Err(lua_runtime_error(
            "ERR Invalid command passed to server.acl_check_cmd()",
        ));
    };
    let Some(categories) = crate::dispatch::command_acl_categories(command) else {
        return Err(lua_runtime_error(
            "ERR Invalid command passed to server.acl_check_cmd()",
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

fn lua_runtime_error(message: &str) -> LuaError {
    LuaError::runtime(format_args!("{message}"))
}

fn lua_runtime_error_bytes(message: &[u8]) -> LuaError {
    LuaError::runtime(format_args!("{}", String::from_utf8_lossy(message)))
}

fn lua_execution_error_payload_lua_rs(kind: &str, err: LuaError) -> Vec<u8> {
    match err {
        LuaError::Syntax(_) => runtime_error_payload(&format!(
            "ERR Error compiling {kind}: {}",
            err.message_lossy()
        )),
        other => runtime_error_payload(&other.message_lossy()),
    }
}
