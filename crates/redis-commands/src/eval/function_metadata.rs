//! Function library metadata and `server.register_function` argument parsing.

use mlua::{
    Error as LuaError, Function as LuaFunction, Lua, MultiValue, Table as LuaTable,
    Value as LuaValue,
};
use redis_types::{RedisError, RedisResult};

use super::bytes::ascii_eq_ci;
use super::{function_store::FunctionDefinition, RuntimeFunctionRegistration};

pub(super) fn parse_function_library_header(code: &[u8]) -> RedisResult<(Vec<u8>, &[u8])> {
    if !code.starts_with(b"#!") {
        return Err(RedisError::runtime(b"ERR Missing library metadata"));
    }
    let line_end = code
        .iter()
        .position(|b| *b == b'\n')
        .ok_or_else(|| RedisError::runtime(b"ERR Invalid library metadata"))?;
    let header = &code[..line_end];
    let body = &code[line_end..];
    let parts = split_function_metadata_args(header)
        .ok_or_else(|| RedisError::runtime(b"ERR Invalid library metadata"))?;
    if parts.is_empty() {
        return Err(RedisError::runtime(b"ERR Invalid library metadata"));
    }
    let engine = parts[0]
        .strip_prefix(b"#!")
        .ok_or_else(|| RedisError::runtime(b"ERR Missing library metadata"))?;

    let mut library_name: Option<Vec<u8>> = None;
    for token in parts.iter().skip(1) {
        if let Some(name) = token.strip_prefix(b"name=") {
            if library_name.is_some() {
                return Err(RedisError::runtime(
                    b"ERR Invalid metadata value, name argument was given multiple times",
                ));
            }
            library_name = Some(name.to_vec());
        } else {
            let mut msg = b"ERR Invalid metadata value given: ".to_vec();
            msg.extend_from_slice(token);
            return Err(RedisError::runtime(msg));
        }
    }

    let library_name =
        library_name.ok_or_else(|| RedisError::runtime(b"ERR Library name was not given"))?;
    validate_library_name(&library_name)?;
    if !ascii_eq_ci(engine, b"lua") {
        let mut msg = b"ERR Engine '".to_vec();
        msg.extend_from_slice(engine);
        msg.extend_from_slice(b"' not found");
        return Err(RedisError::runtime(msg));
    }
    Ok((library_name, body))
}

fn split_function_metadata_args(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut args = Vec::new();
    let mut i = 0usize;
    while i < line.len() {
        while i < line.len() && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= line.len() {
            break;
        }
        let mut arg = Vec::new();
        while i < line.len() && !line[i].is_ascii_whitespace() {
            match line[i] {
                b'\'' | b'"' => {
                    let quote = line[i];
                    i += 1;
                    let mut closed = false;
                    while i < line.len() {
                        if line[i] == quote {
                            i += 1;
                            closed = true;
                            break;
                        }
                        if line[i] == b'\\' && i + 1 < line.len() {
                            i += 1;
                        }
                        arg.push(line[i]);
                        i += 1;
                    }
                    if !closed {
                        return None;
                    }
                }
                byte => {
                    arg.push(byte);
                    i += 1;
                }
            }
        }
        args.push(arg);
    }
    Some(args)
}

pub(super) fn parse_register_function_args(args: MultiValue) -> mlua::Result<FunctionDefinition> {
    let values = args.into_iter().collect::<Vec<_>>();
    if values.is_empty() || values.len() > 2 {
        return Err(LuaError::RuntimeError(
            "wrong number of arguments to server.register_function".to_string(),
        ));
    }
    let first = values[0].clone();

    if values.len() == 1 {
        let LuaValue::Table(table) = first else {
            return Err(LuaError::RuntimeError(
                "calling server.register_function with a single argument is only applicable to Lua table (representing named arguments).".to_string(),
            ));
        };
        let (name, description, no_writes, allow_oom, allow_stale, _) =
            parse_register_function_named_args(table)?;
        validate_function_name(&name)?;
        return Ok(FunctionDefinition {
            name,
            description,
            no_writes,
            allow_oom,
            allow_stale,
        });
    }

    let name = lua_string_value_bytes(
        first,
        "first argument to server.register_function must be a string",
    )?;
    require_lua_function(
        values[1].clone(),
        "second argument to server.register_function must be a function",
    )?;
    validate_function_name(&name)?;
    Ok(FunctionDefinition {
        name,
        description: None,
        no_writes: false,
        allow_oom: false,
        allow_stale: false,
    })
}

pub(super) fn parse_runtime_register_function_args(
    lua: &Lua,
    args: MultiValue,
) -> mlua::Result<RuntimeFunctionRegistration> {
    let values = args.into_iter().collect::<Vec<_>>();
    if values.is_empty() || values.len() > 2 {
        return Err(LuaError::RuntimeError(
            "wrong number of arguments to server.register_function".to_string(),
        ));
    }
    let first = values[0].clone();

    if values.len() == 1 {
        let LuaValue::Table(table) = first else {
            return Err(LuaError::RuntimeError(
                "calling server.register_function with a single argument is only applicable to Lua table (representing named arguments).".to_string(),
            ));
        };
        let (name, _, no_writes, allow_oom, allow_stale, callback) =
            parse_register_function_named_args(table)?;
        validate_function_name(&name)?;
        return Ok(RuntimeFunctionRegistration {
            name,
            callback: lua.create_registry_value(callback)?,
            no_writes,
            allow_oom,
            allow_stale,
        });
    }

    let name = lua_string_value_bytes(
        first,
        "first argument to server.register_function must be a string",
    )?;
    let callback = require_lua_function(
        values[1].clone(),
        "second argument to server.register_function must be a function",
    )?;
    validate_function_name(&name)?;
    Ok(RuntimeFunctionRegistration {
        name,
        callback: lua.create_registry_value(callback)?,
        no_writes: false,
        allow_oom: false,
        allow_stale: false,
    })
}

fn parse_register_function_named_args(
    table: LuaTable,
) -> mlua::Result<(Vec<u8>, Option<Vec<u8>>, bool, bool, bool, LuaFunction)> {
    let mut name: Option<Vec<u8>> = None;
    let mut callback: Option<LuaFunction> = None;
    let mut description: Option<Vec<u8>> = None;
    let mut no_writes = false;
    let mut allow_oom = false;
    let mut allow_stale = false;

    for pair in table.pairs::<LuaValue, LuaValue>() {
        let (key, value) = pair?;
        let key = lua_string_value_bytes(
            key,
            "named argument key given to server.register_function is not a string",
        )?;
        if ascii_eq_ci(&key, b"function_name") {
            name = Some(lua_string_value_bytes(
                value,
                "function_name argument given to server.register_function must be a string",
            )?);
        } else if ascii_eq_ci(&key, b"callback") {
            callback = Some(require_lua_function(
                value,
                "callback argument given to server.register_function must be a function",
            )?);
        } else if ascii_eq_ci(&key, b"description") {
            description = Some(lua_string_value_bytes(
                value,
                "description argument given to server.register_function must be a string",
            )?);
        } else if ascii_eq_ci(&key, b"flags") {
            let LuaValue::Table(flags) = value else {
                return Err(LuaError::RuntimeError(
                    "flags argument to server.register_function must be a table representing function flags"
                        .to_string(),
                ));
            };
            let parsed = parse_function_flags(&flags)?;
            no_writes = parsed.no_writes;
            allow_oom = parsed.allow_oom;
            allow_stale = parsed.allow_stale;
        } else {
            return Err(LuaError::RuntimeError(
                "unknown argument given to server.register_function".to_string(),
            ));
        }
    }

    let name = name.ok_or_else(|| {
        LuaError::RuntimeError(
            "server.register_function must get a function name argument".to_string(),
        )
    })?;
    let callback = callback.ok_or_else(|| {
        LuaError::RuntimeError("server.register_function must get a callback argument".to_string())
    })?;
    Ok((
        name,
        description,
        no_writes,
        allow_oom,
        allow_stale,
        callback,
    ))
}

fn lua_string_value_bytes(value: LuaValue, error: &str) -> mlua::Result<Vec<u8>> {
    match value {
        LuaValue::String(s) => Ok(s.as_bytes().to_vec()),
        _ => Err(LuaError::RuntimeError(error.to_string())),
    }
}

fn require_lua_function(value: LuaValue, error: &str) -> mlua::Result<LuaFunction> {
    match value {
        LuaValue::Function(f) => Ok(f),
        _ => Err(LuaError::RuntimeError(error.to_string())),
    }
}

fn validate_function_name(name: &[u8]) -> mlua::Result<()> {
    if !valid_function_library_name(name) {
        return Err(LuaError::RuntimeError(
            "Function names can only contain letters, numbers, or underscores(_) and must be at least one character long".to_string(),
        ));
    }
    Ok(())
}

fn validate_library_name(name: &[u8]) -> RedisResult<()> {
    if !valid_function_library_name(name) {
        return Err(RedisError::runtime(
            b"ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long",
        ));
    }
    Ok(())
}

fn valid_function_library_name(name: &[u8]) -> bool {
    !name.is_empty() && name.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

#[derive(Clone, Copy, Debug, Default)]
struct FunctionFlags {
    no_writes: bool,
    allow_oom: bool,
    allow_stale: bool,
}

fn parse_function_flags(flags: &LuaTable) -> mlua::Result<FunctionFlags> {
    let mut parsed = FunctionFlags::default();
    let mut index = 1i64;
    loop {
        let value: LuaValue = flags.raw_get(index)?;
        match value {
            LuaValue::Nil => return Ok(parsed),
            LuaValue::String(s) => {
                let flag = s.as_bytes();
                if ascii_eq_ci(flag.as_ref(), b"no-writes") {
                    parsed.no_writes = true;
                } else if ascii_eq_ci(flag.as_ref(), b"allow-oom") {
                    parsed.allow_oom = true;
                } else if ascii_eq_ci(flag.as_ref(), b"allow-stale") {
                    parsed.allow_stale = true;
                } else if !is_known_function_flag(flag.as_ref()) {
                    return Err(LuaError::RuntimeError("unknown flag given".to_string()));
                }
                index += 1;
            }
            _ => return Err(LuaError::RuntimeError("unknown flag given".to_string())),
        }
    }
}

fn is_known_function_flag(flag: &[u8]) -> bool {
    ascii_eq_ci(flag, b"no-writes")
        || ascii_eq_ci(flag, b"allow-oom")
        || ascii_eq_ci(flag, b"allow-stale")
        || ascii_eq_ci(flag, b"no-cluster")
        || ascii_eq_ci(flag, b"allow-cross-slot-keys")
}
