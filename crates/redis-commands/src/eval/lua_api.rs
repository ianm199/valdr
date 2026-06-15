//! Shared Lua `redis` / `server` API installers.

use mlua::{
    Error as LuaError, Function as LuaFunction, Lua, MultiValue, Table as LuaTable,
    Value as LuaValue,
};

pub(super) const LUA_REDIS_VERSION: &str = "7.0.0";
pub(super) const LUA_REDIS_VERSION_NUM: i64 = 7 << 16;

pub(super) fn install_redis_api_constants(redis_tbl: &LuaTable) -> mlua::Result<()> {
    redis_tbl.raw_set("REDIS_VERSION", LUA_REDIS_VERSION)?;
    redis_tbl.raw_set("REDIS_VERSION_NUM", LUA_REDIS_VERSION_NUM)?;
    redis_tbl.raw_set("REPL_NONE", 0)?;
    redis_tbl.raw_set("REPL_AOF", 1)?;
    redis_tbl.raw_set("REPL_SLAVE", 2)?;
    redis_tbl.raw_set("REPL_REPLICA", 2)?;
    redis_tbl.raw_set("REPL_ALL", 3)?;
    redis_tbl.raw_set("LOG_DEBUG", 0)?;
    redis_tbl.raw_set("LOG_VERBOSE", 1)?;
    redis_tbl.raw_set("LOG_NOTICE", 2)?;
    redis_tbl.raw_set("LOG_WARNING", 3)?;
    Ok(())
}

fn create_set_repl_function(lua: &Lua) -> mlua::Result<LuaFunction> {
    lua.create_function(|lua_inner, args: MultiValue| -> mlua::Result<()> {
        if args.len() != 1 {
            return Err(LuaError::RuntimeError(
                "ERR server.set_repl() requires one argument.".to_string(),
            ));
        }
        let flags = match args.front() {
            Some(LuaValue::Integer(n)) => *n,
            Some(LuaValue::Number(n)) => *n as i64,
            _ => 0,
        };
        if !(0..=3).contains(&flags) {
            return Err(LuaError::RuntimeError(
                "Invalid replication flags".to_string(),
            ));
        }
        lua_inner.set_named_registry_value("__redis_repl_flags", flags)?;
        Ok(())
    })
}

pub(super) fn install_set_repl_function(lua: &Lua, redis_tbl: &LuaTable) -> mlua::Result<()> {
    let raw_set_repl = create_set_repl_function(lua)?;
    install_error_normalizing_function(lua, redis_tbl, "__raw_set_repl", "set_repl", raw_set_repl)
}

fn lua_log_arg_to_string(value: &LuaValue) -> Option<String> {
    match value {
        LuaValue::String(s) => Some(String::from_utf8_lossy(&s.as_bytes()).into_owned()),
        LuaValue::Integer(n) => Some(n.to_string()),
        LuaValue::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn install_error_normalizing_function(
    lua: &Lua,
    redis_tbl: &LuaTable,
    raw_name: &str,
    public_name: &str,
    raw: LuaFunction,
) -> mlua::Result<()> {
    redis_tbl.raw_set(raw_name, raw.clone())?;
    let shim = lua
        .load(
            "local raw = ...\n\
             return function(...)\n\
                 local ok, res = pcall(raw, ...)\n\
                 if ok then return res end\n\
                 local msg = tostring(res)\n\
                 msg = msg:gsub('^.-: ', '', 1)\n\
                 msg = msg:gsub('\\nstack traceback.*$', '')\n\
                 if msg == '' then msg = 'ERR' end\n\
                 local code = string.match(msg, '^[^ \\t]*') or ''\n\
                 if string.sub(msg, 1, 4) ~= 'ERR ' and not string.match(code, '^[A-Z0-9_]+$') then\n\
                     msg = 'ERR ' .. msg\n\
                 end\n\
                 error(msg, 0)\n\
             end\n",
        )
        .set_name("redis_api_error_shim")
        .call::<LuaFunction>(raw)?;
    redis_tbl.raw_set(public_name, shim)?;
    Ok(())
}

pub(super) fn install_log_function(lua: &Lua, redis_tbl: &LuaTable) -> mlua::Result<()> {
    let raw_log = lua.create_function(|_lua_inner, args: MultiValue| -> mlua::Result<()> {
        if args.len() < 2 {
            return Err(LuaError::RuntimeError(
                "ERR server.log() requires two arguments or more.".to_string(),
            ));
        }
        let level = match args.front() {
            Some(LuaValue::Integer(n)) => *n,
            Some(LuaValue::Number(n)) => *n as i64,
            _ => {
                return Err(LuaError::RuntimeError(
                    "ERR First argument must be a number (log level).".to_string(),
                ));
            }
        };
        if !(0..=3).contains(&level) {
            return Err(LuaError::RuntimeError("ERR Invalid log level.".to_string()));
        }
        let message = args
            .iter()
            .skip(1)
            .filter_map(lua_log_arg_to_string)
            .collect::<Vec<_>>()
            .join(" ");
        crate::connection::log_server_notice(&message);
        Ok(())
    })?;
    install_error_normalizing_function(lua, redis_tbl, "__raw_log", "log", raw_log)
}

pub(super) fn install_setresp_function(lua: &Lua, redis_tbl: &LuaTable) -> mlua::Result<()> {
    let raw_setresp = lua.create_function(|lua_inner, n: i64| -> mlua::Result<()> {
        if n != 2 && n != 3 {
            return Err(LuaError::RuntimeError(
                "ERR RESP version must be 2 or 3.".to_string(),
            ));
        }
        lua_inner.set_named_registry_value("__redis_resp_view", n)?;
        Ok(())
    })?;
    install_error_normalizing_function(lua, redis_tbl, "__raw_setresp", "setresp", raw_setresp)
}
