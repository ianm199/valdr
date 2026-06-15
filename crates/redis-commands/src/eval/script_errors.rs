//! Lua script error normalization and command error payload shaping.

use mlua::{Error as LuaError, Value as LuaValue};
use redis_types::RedisError;

fn lua_error_code_token(bytes: &[u8]) -> &[u8] {
    bytes
        .split(|b| *b == b' ' || *b == b'\t' || *b == b'\r' || *b == b'\n')
        .next()
        .unwrap_or(bytes)
}

fn lua_error_token_is_code(token: &[u8]) -> bool {
    !token.is_empty()
        && token
            .iter()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || *b == b'_')
}

pub(super) fn runtime_error_payload(message: &str) -> Vec<u8> {
    let without_trace = message
        .split_once("\nstack traceback")
        .map(|(head, _)| head)
        .unwrap_or(message);
    let first_line = without_trace
        .split(['\r', '\n'])
        .next()
        .unwrap_or("")
        .trim();
    let mut normalized = first_line.to_owned();
    if normalized.is_empty() {
        normalized = "ERR Error running script".to_string();
    }
    if normalized.starts_with("ERR unknown command") {
        normalized.replace_range(4..11, "Unknown");
    }
    if normalized.contains("wrong number of arguments") {
        normalized = normalized.replace("wrong number of arguments", "wrong number of args");
    }
    if let Some(rest) = normalized.strip_prefix("[string \"user_script\"]:") {
        normalized = format!("user_script:{}", rest);
    }

    let bytes = normalized.as_bytes();
    let first_token_is_error_code = lua_error_token_is_code(lua_error_code_token(bytes));

    let mut out = Vec::new();
    if !bytes.starts_with(b"ERR ") && !first_token_is_error_code {
        out.extend_from_slice(b"ERR ");
    }
    out.extend_from_slice(bytes);
    if out.starts_with(b"ERR user_script:") && !byte_windows_contains(&out, b" script: ") {
        out.extend_from_slice(b" script: unknown");
    }
    out
}

pub(super) fn lua_execution_error_payload(kind: &str, err: LuaError) -> Vec<u8> {
    match err {
        LuaError::RuntimeError(msg) => runtime_error_payload(&msg),
        LuaError::CallbackError { cause, .. } => {
            lua_execution_error_payload(kind, cause.as_ref().clone())
        }
        LuaError::SyntaxError { message, .. } => {
            runtime_error_payload(&format!("ERR Error compiling {kind}: {message}"))
        }
        other => runtime_error_payload(&format!("ERR Error running {kind}: {other}")),
    }
}

/// Coerce one Lua argument passed to `redis.call(...)` into the byte
/// string the dispatch table expects. Integers/numbers are stringified
/// using Lua's `tostring`-compatible rule (integers stay integral).
pub(super) fn lua_arg_to_bytes(v: &LuaValue) -> Result<Vec<u8>, LuaError> {
    match v {
        LuaValue::String(s) => Ok(s.as_bytes().to_vec()),
        LuaValue::Integer(n) => Ok(n.to_string().into_bytes()),
        LuaValue::Number(f) => {
            if f.fract() == 0.0 && f.is_finite() {
                Ok(((*f) as i64).to_string().into_bytes())
            } else {
                Ok(format!("{}", f).into_bytes())
            }
        }
        LuaValue::Boolean(true) => Ok(b"1".to_vec()),
        LuaValue::Boolean(false) => Ok(b"0".to_vec()),
        _ => Err(LuaError::RuntimeError(
            "Command arguments must be strings or integers".to_string(),
        )),
    }
}

pub(super) fn lua_script_command_error_payload(err: &RedisError) -> Vec<u8> {
    lua_script_command_reply_error_payload(err.to_resp_payload().as_bytes())
}

pub(super) fn lua_script_command_call_error_payload(err: &RedisError) -> Vec<u8> {
    lua_script_call_error_payload(lua_script_command_error_payload(err))
}

pub(super) fn lua_script_command_reply_call_error_payload(bytes: &[u8]) -> Vec<u8> {
    lua_script_call_error_payload(lua_script_command_reply_error_payload(bytes))
}

pub(super) fn lua_script_call_error_payload(mut payload: Vec<u8>) -> Vec<u8> {
    if !byte_windows_contains(&payload, b" script: ") {
        payload.extend_from_slice(b" script: unknown");
    }
    payload
}

pub(super) fn lua_script_command_reply_error_payload(bytes: &[u8]) -> Vec<u8> {
    let payload = bytes.strip_prefix(b"-").unwrap_or(bytes);
    let first_line = payload
        .split(|b| *b == b'\r' || *b == b'\n')
        .next()
        .unwrap_or(payload);
    let lower = first_line.to_ascii_lowercase();
    if lower.starts_with(b"err wrong number of args")
        || lower.starts_with(b"err wrong number of arguments")
    {
        return b"ERR Wrong number of args calling command from script".to_vec();
    }
    if first_line.starts_with(b"NOPERM ") {
        let mut out = b"ERR ACL failure in script: ".to_vec();
        let detail = &first_line[b"NOPERM ".len()..];
        if detail == b"No permissions to access a database" {
            out.extend_from_slice(b"No permissions to access database");
        } else {
            out.extend_from_slice(detail);
        }
        return out;
    }
    first_line.to_vec()
}

fn byte_windows_contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
