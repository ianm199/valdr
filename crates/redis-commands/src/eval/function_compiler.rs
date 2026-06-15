//! Function library compilation helpers.
//!
//! This module owns the load-time Lua environment used to discover
//! `server.register_function` metadata. Runtime FCALL execution remains in the
//! main eval implementation and cached runtime adapter.

use std::cell::RefCell;

use mlua::{Error as LuaError, Lua, MultiValue, Value as LuaValue};
use redis_types::{RedisError, RedisResult};

use super::bytes::ascii_eq_ci;
use super::function_metadata::parse_register_function_args;
use super::function_store::FunctionDefinition;
use super::lua_api::install_redis_api_constants;
use super::lua_bit::install_bit;
use super::lua_cjson::install_cjson;
use super::lua_cmsgpack::install_cmsgpack;
use super::lua_sandbox::{
    install_global_protection, install_sandbox, install_script_error_wrapper,
    readonly_table_proxy_with_missing_global_errors,
};

pub(super) fn compile_function_library(
    library_body: &[u8],
) -> RedisResult<Vec<FunctionDefinition>> {
    let lua = Lua::new();
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
        .set("math", LuaValue::Nil)
        .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;

    let registered: RefCell<Vec<FunctionDefinition>> = RefCell::new(Vec::new());
    let load_result: Result<(), LuaError> = lua.scope(|scope| {
        let api = lua.create_table()?;
        install_redis_api_constants(&api)?;
        let register_fn = {
            let registered = &registered;
            scope.create_function_mut(move |_lua, args: MultiValue| -> mlua::Result<()> {
                let definition = parse_register_function_args(args)?;
                if registered
                    .borrow()
                    .iter()
                    .any(|existing| ascii_eq_ci(&existing.name, &definition.name))
                {
                    return Err(LuaError::RuntimeError(
                        "Function already exists in the library".to_string(),
                    ));
                }
                registered.borrow_mut().push(definition);
                Ok(())
            })?
        };
        api.raw_set("register_function", register_fn)?;
        let api = readonly_table_proxy_with_missing_global_errors(&lua, api)?;
        lua.globals().set("redis", api.clone())?;
        lua.globals().set("server", api)?;
        install_global_protection(&lua)?;
        lua.load(library_body).set_name("function_library").exec()
    });

    match load_result {
        Ok(()) => {
            let functions = registered.into_inner();
            if functions.is_empty() {
                Err(RedisError::runtime(
                    b"ERR No functions registered in library",
                ))
            } else {
                Ok(functions)
            }
        }
        Err(err) => Err(function_load_lua_error(err)),
    }
}

pub(super) fn function_load_lua_error(err: LuaError) -> RedisError {
    let prefix = if matches!(err, LuaError::SyntaxError { .. }) {
        "ERR Error compiling function library"
    } else {
        "ERR Error loading function library"
    };
    let detail = lua_error_detail(&err);
    RedisError::runtime(format!("{}: {}", prefix, lua_error_first_line(&detail)).into_bytes())
}

fn lua_error_detail(err: &LuaError) -> String {
    match err {
        LuaError::SyntaxError { message, .. } | LuaError::RuntimeError(message) => message.clone(),
        LuaError::CallbackError { cause, .. } => lua_error_detail(cause.as_ref()),
        other => other.to_string(),
    }
}

fn lua_error_first_line(message: &str) -> &str {
    message
        .split_once("\nstack traceback")
        .map(|(head, _)| head)
        .unwrap_or(message)
        .split(['\r', '\n'])
        .next()
        .unwrap_or("")
        .trim()
}
