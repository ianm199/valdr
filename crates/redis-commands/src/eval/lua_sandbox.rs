//! Lua sandbox, globals, and module table helpers for the `mlua` backend.

use std::sync::OnceLock;
use std::time::Instant;

use mlua::{
    Error as LuaError, Function as LuaFunction, Lua, MultiValue, Table as LuaTable,
    Value as LuaValue,
};
use redis_types::RedisString;

use super::script_cache::sha1_hex;

pub(super) fn create_disabled_loadstring(lua: &Lua) -> mlua::Result<LuaFunction> {
    lua.create_function(|_, _: MultiValue| -> mlua::Result<LuaValue> { Ok(LuaValue::Nil) })
}

pub(super) fn install_script_error_wrapper(lua: &Lua) -> mlua::Result<()> {
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

pub(super) fn create_sha1hex_function(lua: &Lua) -> mlua::Result<LuaFunction> {
    lua.create_function(|_lua, args: MultiValue| -> mlua::Result<String> {
        if args.len() != 1 {
            return Err(LuaError::RuntimeError(
                "wrong number of arguments to redis.sha1hex".to_string(),
            ));
        }
        let Some(LuaValue::String(s)) = args.front() else {
            return Err(LuaError::RuntimeError(
                "bad argument #1 to redis.sha1hex".to_string(),
            ));
        };
        let hex = sha1_hex(&s.as_bytes());
        Ok(String::from_utf8(hex.to_vec()).unwrap_or_default())
    })
}

/// Sandbox an `mlua::Lua` instance by removing globals that would let a
/// user script reach the filesystem or the host process. Mirrors
/// real-Redis sandbox.
pub(super) fn install_sandbox(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();
    for name in [
        "io",
        "debug",
        "package",
        "require",
        "loadfile",
        "dofile",
        "loadstring",
        "print",
        "getfenv",
        "setfenv",
        "getmetatable",
    ] {
        globals.set(name, LuaValue::Nil)?;
    }
    globals.set("os", create_os_table(lua)?)?;
    Ok(())
}

pub(super) fn install_global_protection(lua: &Lua) -> mlua::Result<()> {
    lua.load(
        r#"
        setmetatable(_G, {
            __index = function(_, key)
                error("Script attempted to access nonexistent global variable '" .. tostring(key) .. "'", 2)
            end,
            __newindex = function(_, key, _)
                error("Attempt to modify a readonly table", 2)
            end
        })
        "#,
    )
    .set_name("global_protection")
    .exec()
}

pub(super) fn install_eval_global_protection(lua: &Lua) -> mlua::Result<()> {
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
        raw_setmetatable(_G, global_meta)
        setmetatable = function(t, mt)
            if t == _G then
                error("Attempt to modify a readonly table", 2)
            end
            return raw_setmetatable(t, mt)
        end
        getmetatable = function(t)
            if t == _G then
                return readonly_global_meta
            end
            if type(t) ~= "table" then
                return nil
            end
            return raw_getmetatable(t)
        end
        "#,
    )
    .set_name("eval_global_protection")
    .exec()
}

pub(super) fn create_script_environment(lua: &Lua) -> mlua::Result<LuaTable> {
    let env = lua.create_table()?;
    let globals = lua.globals();
    let install: LuaFunction = lua
        .load(
            r#"
        return function(env, globals)
            setmetatable(env, {
                __index = function(_, key)
                    local value = rawget(globals, key)
                    if value == nil then
                        error("Script attempted to access nonexistent global variable '" .. tostring(key) .. "'", 2)
                    end
                    return value
                end,
                __newindex = function()
                    error("Attempt to modify a readonly table", 2)
                end,
                __metatable = false
            })
        end
        "#,
        )
        .set_name("script_environment")
        .eval()?;
    install.call::<()>((env.clone(), globals))?;
    Ok(env)
}

/// Process-relative seconds for `os.clock`. Valkey's Lua sandbox keeps only
/// `os.clock` from the standard `os` library, and every script uses it as a
/// delta (`os.clock - start`), so an arbitrary monotonic epoch is faithful.
pub(super) fn os_clock_seconds() -> f64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_secs_f64()
}

/// Build the sandboxed `os` global. Valkey exposes a plain table holding only
/// `os.clock`; every other `os.*` is absent, so a script calling e.g.
/// `os.execute` hits the Lua "attempt to call field 'execute' (a nil value)"
/// error the suite asserts. The table must stay a plain (non-proxy) table
/// because the sandbox test iterates it with `pairs(os)`, which in Lua 5.1
/// sees only raw keys.
fn create_os_table(lua: &Lua) -> mlua::Result<LuaTable> {
    let table = lua.create_table()?;
    table.raw_set(
        "clock",
        lua.create_function(|_, ()| Ok(os_clock_seconds()))?,
    )?;
    Ok(table)
}

/// Install `KEYS` and `ARGV` into the per-call Lua globals.
pub(super) fn install_keys_argv(
    lua: &Lua,
    keys: &[RedisString],
    argv: &[RedisString],
) -> mlua::Result<()> {
    let keys_t = lua.create_table()?;
    for (i, k) in keys.iter().enumerate() {
        keys_t.raw_set(i as i64 + 1, lua.create_string(k.as_bytes())?)?;
    }
    lua.globals().raw_set("KEYS", keys_t)?;

    let argv_t = lua.create_table()?;
    for (i, a) in argv.iter().enumerate() {
        argv_t.raw_set(i as i64 + 1, lua.create_string(a.as_bytes())?)?;
    }
    lua.globals().raw_set("ARGV", argv_t)?;
    Ok(())
}

pub(super) fn readonly_table_proxy(lua: &Lua, table: LuaTable) -> mlua::Result<LuaTable> {
    let proxy = lua.create_table()?;
    let metatable = lua.create_table()?;
    metatable.raw_set("__index", table)?;
    metatable.raw_set(
        "__newindex",
        lua.create_function(|_, _: MultiValue| -> mlua::Result<()> {
            Err(LuaError::RuntimeError(
                "Attempt to modify a readonly table".to_string(),
            ))
        })?,
    )?;
    metatable.raw_set("__metatable", false)?;
    proxy.set_metatable(Some(metatable));
    Ok(proxy)
}

fn lua_key_name(key: &LuaValue) -> String {
    match key {
        LuaValue::String(s) => String::from_utf8_lossy(&s.as_bytes()).into_owned(),
        LuaValue::Integer(n) => n.to_string(),
        LuaValue::Number(n) => n.to_string(),
        LuaValue::Boolean(v) => v.to_string(),
        LuaValue::Nil => "nil".to_string(),
        _ => key.type_name().to_string(),
    }
}

pub(super) fn readonly_table_proxy_with_missing_global_errors(
    lua: &Lua,
    table: LuaTable,
) -> mlua::Result<LuaTable> {
    let proxy = lua.create_table()?;
    let metatable = lua.create_table()?;
    let lookup = table.clone();
    metatable.raw_set(
        "__index",
        lua.create_function(
            move |_, (_table, key): (LuaValue, LuaValue)| -> mlua::Result<LuaValue> {
                let value: LuaValue = lookup.raw_get(key.clone())?;
                if matches!(value, LuaValue::Nil) {
                    return Err(LuaError::RuntimeError(format!(
                        "Script attempted to access nonexistent global variable '{}'",
                        lua_key_name(&key)
                    )));
                }
                Ok(value)
            },
        )?,
    )?;
    metatable.raw_set(
        "__newindex",
        lua.create_function(|_, _: MultiValue| -> mlua::Result<()> {
            Err(LuaError::RuntimeError(
                "Attempt to modify a readonly table".to_string(),
            ))
        })?,
    )?;
    metatable.raw_set("__metatable", false)?;
    proxy.set_metatable(Some(metatable));
    Ok(proxy)
}
