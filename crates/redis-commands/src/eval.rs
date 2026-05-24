//! `EVAL` / `EVALSHA` / `SCRIPT` — server-side Lua scripting.
//!
//! Backed by `mlua` (bundled C Lua 5.1, matching real Redis). The runtime is
//! constructed once per call so global state never leaks across scripts and
//! the dangerous portions of the stdlib (`os`, `io`, `debug`, `require`,
//! `loadfile`, `dofile`, `package`, `print`) are removed before user code
//! runs.
//!
//! `redis.call` / `redis.pcall` re-enter the command dispatch table by
//! saving the client's argv and reply buffer, installing the synthetic
//! argv, calling [`crate::dispatch::dispatch_command_name`], parsing the
//! newly-written reply bytes back into a Lua value, then restoring the
//! caller's argv and the original reply buffer prefix.
//!
//! Script cache is a process-wide `Mutex<HashMap<sha1_hex, bytes>>` keyed
//! by the lower-case 40-byte SHA-1 hex of the source bytes. `SCRIPT LOAD`
//! inserts into the cache; `EVALSHA` looks up; `SCRIPT FLUSH` clears.
//!
//! See `docs/ADR_001_LUA_RUNTIME.md` for the runtime-choice rationale and
//! the full sandbox patch list.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};

use mlua::{
    Error as LuaError, Function as LuaFunction, LightUserData, Lua, MultiValue, RegistryKey,
    Table as LuaTable, Value as LuaValue,
};

use redis_core::CommandContext;
use redis_protocol::frame::RespFrame;
use redis_protocol::parser::{ParserCallbacks, ParserCursor};
use redis_types::{RedisError, RedisResult, RedisString};

use crate::dispatch::dispatch_command_name;

/// One captured reply from a `redis.call` re-entry.
///
/// Parsed from the RESP bytes the inner dispatch wrote into `reply_buf`.
/// Used as an intermediate before the value is converted to a Lua value.
#[derive(Debug, Clone)]
enum ReplyValue {
    Null,
    Nil,
    SimpleString(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Vec<u8>),
    Bool(bool),
    Double(f64),
    BigNumber(Vec<u8>),
    VerbatimString { format: Vec<u8>, data: Vec<u8> },
    Array(Vec<ReplyValue>),
    Map(Vec<ReplyValue>),
    Set(Vec<ReplyValue>),
}

#[derive(Debug, Clone)]
struct FunctionDefinition {
    name: Vec<u8>,
    no_writes: bool,
}

#[derive(Debug, Clone)]
struct LoadedFunctionLibrary {
    name: Vec<u8>,
    code: Vec<u8>,
    functions: Vec<FunctionDefinition>,
}

struct RuntimeFunctionRegistration {
    name: Vec<u8>,
    callback: RegistryKey,
    no_writes: bool,
}

/// Parser-callback adapter that accumulates one RESP frame into a
/// [`ReplyValue`] tree. Built once per inner dispatch and consumed with a
/// single `parse_next` call.
struct ReplyBuilder {
    stack: Vec<Vec<ReplyValue>>,
    pending_lens: Vec<i64>,
    out: Option<ReplyValue>,
    errored: bool,
}

impl ReplyBuilder {
    fn new() -> Self {
        Self {
            stack: Vec::new(),
            pending_lens: Vec::new(),
            out: None,
            errored: false,
        }
    }

    fn deliver(&mut self, v: ReplyValue) {
        if let Some(top) = self.stack.last_mut() {
            top.push(v);
            let popped = self
                .pending_lens
                .last_mut()
                .map(|n| {
                    *n -= 1;
                    *n
                })
                .unwrap_or(0);
            if popped <= 0 {
                let frame = self.stack.pop().unwrap_or_default();
                self.pending_lens.pop();
                self.deliver(ReplyValue::Array(frame));
            }
        } else {
            self.out = Some(v);
        }
    }
}

impl ParserCallbacks for ReplyBuilder {
    fn on_null_bulk_string(&mut self, _proto: &[u8]) {
        self.deliver(ReplyValue::Nil);
    }
    fn on_null_array(&mut self, _proto: &[u8]) {
        self.deliver(ReplyValue::Nil);
    }
    fn on_bulk_string(&mut self, data: &[u8], _proto: &[u8]) {
        self.deliver(ReplyValue::Bulk(data.to_vec()));
    }
    fn on_error(&mut self, data: &[u8], _proto: &[u8]) {
        self.deliver(ReplyValue::Error(data.to_vec()));
    }
    fn on_simple_str(&mut self, data: &[u8], _proto: &[u8]) {
        self.deliver(ReplyValue::SimpleString(data.to_vec()));
    }
    fn on_long(&mut self, val: i64, _proto: &[u8]) {
        self.deliver(ReplyValue::Integer(val));
    }
    fn on_array(&mut self, cursor: &mut ParserCursor<'_>, len: i64, _proto: &[u8]) {
        if len <= 0 {
            self.deliver(ReplyValue::Array(Vec::new()));
            return;
        }
        self.stack.push(Vec::with_capacity(len as usize));
        self.pending_lens.push(len);
        for _ in 0..len {
            if cursor.parse_next(self).is_err() {
                self.errored = true;
                break;
            }
        }
    }
    fn on_set(&mut self, cursor: &mut ParserCursor<'_>, len: i64, _proto: &[u8]) {
        let mut items: Vec<ReplyValue> = Vec::with_capacity(len.max(0) as usize);
        for _ in 0..len {
            let mut tmp = ReplyBuilder::new();
            if cursor.parse_next(&mut tmp).is_err() {
                self.errored = true;
                return;
            }
            if let Some(v) = tmp.out {
                items.push(v);
            }
        }
        self.deliver(ReplyValue::Set(items));
    }
    fn on_map(&mut self, cursor: &mut ParserCursor<'_>, len: i64, _proto: &[u8]) {
        let pair_count = len.max(0) * 2;
        let mut items: Vec<ReplyValue> = Vec::with_capacity(pair_count as usize);
        for _ in 0..pair_count {
            let mut tmp = ReplyBuilder::new();
            if cursor.parse_next(&mut tmp).is_err() {
                self.errored = true;
                return;
            }
            if let Some(v) = tmp.out {
                items.push(v);
            }
        }
        self.deliver(ReplyValue::Map(items));
    }
    fn on_bool(&mut self, val: bool, _proto: &[u8]) {
        self.deliver(ReplyValue::Bool(val));
    }
    fn on_double(&mut self, val: f64, _proto: &[u8]) {
        self.deliver(ReplyValue::Double(val));
    }
    fn on_big_number(&mut self, data: &[u8], _proto: &[u8]) {
        self.deliver(ReplyValue::BigNumber(data.to_vec()));
    }
    fn on_verbatim_string(&mut self, format: &[u8], data: &[u8], _proto: &[u8]) {
        self.deliver(ReplyValue::VerbatimString {
            format: format.to_vec(),
            data: data.to_vec(),
        });
    }
    fn on_attribute(&mut self, cursor: &mut ParserCursor<'_>, len: i64, _proto: &[u8]) {
        let pair_count = len.max(0) * 2;
        for _ in 0..pair_count {
            let mut tmp = ReplyBuilder::new();
            if cursor.parse_next(&mut tmp).is_err() {
                self.errored = true;
                return;
            }
        }
    }
    fn on_null(&mut self, _proto: &[u8]) {
        self.deliver(ReplyValue::Null);
    }
    fn on_parse_error(&mut self) {
        self.errored = true;
    }
}

/// Convert a [`ReplyValue`] tree to a Lua value following the Redis Lua
/// semantics: bulk and simple strings become Lua strings, integers become
/// Lua integers, nil becomes Lua nil, errors become `{err = msg}`, arrays
/// become 1-indexed Lua tables.
fn reply_to_lua(lua: &Lua, value: &ReplyValue) -> mlua::Result<LuaValue> {
    match value {
        ReplyValue::Null => Ok(LuaValue::Nil),
        ReplyValue::Nil => Ok(LuaValue::Boolean(false)),
        ReplyValue::SimpleString(s) => {
            let t = lua.create_table()?;
            t.raw_set("ok", lua.create_string(s)?)?;
            Ok(LuaValue::Table(t))
        }
        ReplyValue::Error(s) => {
            let t = lua.create_table()?;
            t.raw_set("err", lua.create_string(s)?)?;
            Ok(LuaValue::Table(t))
        }
        ReplyValue::Integer(n) => Ok(LuaValue::Integer(*n)),
        ReplyValue::Bool(v) => Ok(LuaValue::Boolean(*v)),
        ReplyValue::Double(n) => {
            let t = lua.create_table()?;
            t.raw_set("double", *n)?;
            Ok(LuaValue::Table(t))
        }
        ReplyValue::BigNumber(n) => {
            let t = lua.create_table()?;
            t.raw_set("big_number", lua.create_string(n)?)?;
            Ok(LuaValue::Table(t))
        }
        ReplyValue::VerbatimString { format, data } => {
            let t = lua.create_table()?;
            let verbatim_table = lua.create_table()?;
            verbatim_table.raw_set("string", lua.create_string(data)?)?;
            verbatim_table.raw_set("format", lua.create_string(format)?)?;
            t.raw_set("verbatim_string", verbatim_table)?;
            Ok(LuaValue::Table(t))
        }
        ReplyValue::Bulk(b) => Ok(LuaValue::String(lua.create_string(b)?)),
        ReplyValue::Array(items) => {
            let t = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                let v = reply_to_lua(lua, item)?;
                t.raw_set(i as i64 + 1, v)?;
            }
            Ok(LuaValue::Table(t))
        }
        ReplyValue::Map(items) => {
            let out = lua.create_table()?;
            let map = lua.create_table()?;
            for pair in items.chunks(2) {
                if pair.len() != 2 {
                    continue;
                }
                let key = reply_to_lua(lua, &pair[0])?;
                let value = reply_to_lua(lua, &pair[1])?;
                map.raw_set(key, value)?;
            }
            out.raw_set("map", map)?;
            Ok(LuaValue::Table(out))
        }
        ReplyValue::Set(items) => {
            let out = lua.create_table()?;
            let set = lua.create_table()?;
            for item in items {
                let value = reply_to_lua(lua, item)?;
                set.raw_set(value, true)?;
            }
            out.raw_set("set", set)?;
            Ok(LuaValue::Table(out))
        }
    }
}

/// Encode a Lua value as a RESP frame on the wire.
///
/// Mirrors real Redis script-to-protocol conversion: nil → null bulk,
/// integers / numbers → integer (numbers truncated), strings → bulk,
/// booleans → `:1` / null, tables → status if `.ok`, error if `.err`,
/// otherwise a 1-indexed array (terminated at the first nil per Lua-array
/// convention).
fn lua_to_resp(value: &LuaValue, out: &mut Vec<u8>) {
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
            let bytes = s.as_bytes();
            out.push(b'$');
            out.extend_from_slice(bytes.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(&bytes);
            out.extend_from_slice(b"\r\n");
        }
        LuaValue::Table(t) => {
            if let Ok(Some(err)) = t.get::<Option<mlua::String>>("err") {
                let bytes = err.as_bytes();
                out.push(b'-');
                if !bytes.starts_with(b"ERR ")
                    && !bytes
                        .iter()
                        .take_while(|b| **b != b' ')
                        .all(u8::is_ascii_uppercase)
                {
                    out.extend_from_slice(b"ERR ");
                }
                out.extend_from_slice(&bytes);
                out.extend_from_slice(b"\r\n");
                return;
            }
            if let Ok(Some(ok)) = t.get::<Option<mlua::String>>("ok") {
                let bytes = ok.as_bytes();
                out.push(b'+');
                out.extend_from_slice(&bytes);
                out.extend_from_slice(b"\r\n");
                return;
            }
            let mut items: Vec<LuaValue> = Vec::new();
            let mut i: i64 = 1;
            loop {
                let v: LuaValue = match t.raw_get(i) {
                    Ok(v) => v,
                    Err(_) => break,
                };
                if matches!(v, LuaValue::Nil) {
                    break;
                }
                items.push(v);
                i += 1;
            }
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for it in &items {
                lua_to_resp(it, out);
            }
        }
        _ => out.extend_from_slice(b"$-1\r\n"),
    }
}

fn runtime_error_payload(message: &str) -> Vec<u8> {
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

    let bytes = normalized.as_bytes();
    let first_token_is_error_code = bytes
        .iter()
        .take_while(|b| **b != b' ')
        .all(u8::is_ascii_uppercase);

    let mut out = Vec::new();
    if !bytes.starts_with(b"ERR ") && !first_token_is_error_code {
        out.extend_from_slice(b"ERR ");
    }
    out.extend_from_slice(bytes);
    out
}

/// Coerce one Lua argument passed to `redis.call(...)` into the byte
/// string the dispatch table expects. Integers/numbers are stringified
/// using Lua's `tostring`-compatible rule (integers stay integral).
fn lua_arg_to_bytes(v: &LuaValue) -> Result<Vec<u8>, LuaError> {
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
            "Lua redis() command arguments must be strings or integers".to_string(),
        )),
    }
}

/// Sandbox an `mlua::Lua` instance by removing globals that would let a
/// user script reach the filesystem or the host process. Mirrors the
/// real-Redis sandbox.
fn install_sandbox(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();
    for name in [
        "os", "io", "debug", "package", "require", "loadfile", "dofile", "print",
    ] {
        globals.set(name, LuaValue::Nil)?;
    }
    Ok(())
}

/// Install `KEYS` and `ARGV` into the per-call Lua globals.
fn install_keys_argv(lua: &Lua, keys: &[RedisString], argv: &[RedisString]) -> mlua::Result<()> {
    let keys_t = lua.create_table()?;
    for (i, k) in keys.iter().enumerate() {
        keys_t.raw_set(i as i64 + 1, lua.create_string(k.as_bytes())?)?;
    }
    lua.globals().set("KEYS", keys_t)?;

    let argv_t = lua.create_table()?;
    for (i, a) in argv.iter().enumerate() {
        argv_t.raw_set(i as i64 + 1, lua.create_string(a.as_bytes())?)?;
    }
    lua.globals().set("ARGV", argv_t)?;
    Ok(())
}

#[derive(Debug, Clone)]
struct CjsonConfig {
    encode_max_depth: usize,
    decode_max_depth: usize,
    encode_invalid_numbers: bool,
    decode_invalid_numbers: bool,
    encode_keep_buffer: bool,
    encode_number_precision: i64,
}

impl Default for CjsonConfig {
    fn default() -> Self {
        Self {
            encode_max_depth: 1000,
            decode_max_depth: 1000,
            encode_invalid_numbers: false,
            decode_invalid_numbers: false,
            encode_keep_buffer: true,
            encode_number_precision: 14,
        }
    }
}

fn cjson_null_value() -> LuaValue {
    LuaValue::LightUserData(LightUserData(std::ptr::null_mut()))
}

fn is_cjson_null(value: &LuaValue) -> bool {
    matches!(value, LuaValue::LightUserData(data) if data.0.is_null())
}

fn json_escape_string(bytes: &[u8]) -> mlua::Result<String> {
    serde_json::to_string(String::from_utf8_lossy(bytes).as_ref())
        .map_err(|err| LuaError::RuntimeError(format!("Cannot serialise string: {}", err)))
}

fn encode_json_number(n: f64, allow_invalid: bool) -> mlua::Result<String> {
    if n.is_finite() {
        if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
            Ok((n as i64).to_string())
        } else {
            Ok(format!("{}", n))
        }
    } else if allow_invalid {
        if n.is_nan() {
            Ok("NaN".to_string())
        } else if n.is_sign_positive() {
            Ok("Infinity".to_string())
        } else {
            Ok("-Infinity".to_string())
        }
    } else {
        Err(LuaError::RuntimeError(
            "Cannot serialise number: must not be NaN or Infinity".to_string(),
        ))
    }
}

fn lua_value_to_json_string(
    value: LuaValue,
    cfg: &CjsonConfig,
    depth: usize,
) -> mlua::Result<String> {
    if is_cjson_null(&value) {
        return Ok("null".to_string());
    }
    match value {
        LuaValue::Nil => Ok("null".to_string()),
        LuaValue::Boolean(v) => Ok(if v { "true" } else { "false" }.to_string()),
        LuaValue::Integer(n) => Ok(n.to_string()),
        LuaValue::Number(n) => encode_json_number(n, cfg.encode_invalid_numbers),
        LuaValue::String(s) => json_escape_string(s.as_bytes().as_ref()),
        LuaValue::Table(t) => lua_table_to_json_string(t, cfg, depth + 1),
        LuaValue::LightUserData(data) if data.0.is_null() => Ok("null".to_string()),
        _ => Err(LuaError::RuntimeError(
            "Cannot serialise value: unsupported Lua type".to_string(),
        )),
    }
}

fn lua_table_to_json_string(
    table: LuaTable,
    cfg: &CjsonConfig,
    depth: usize,
) -> mlua::Result<String> {
    if depth > cfg.encode_max_depth {
        return Err(LuaError::RuntimeError(
            "Cannot serialise, excessive nesting".to_string(),
        ));
    }

    let mut entries: Vec<(LuaValue, LuaValue)> = Vec::new();
    for pair in table.pairs::<LuaValue, LuaValue>() {
        entries.push(pair?);
    }

    let mut numeric_indexes: Vec<i64> = Vec::new();
    let mut all_array_keys = !entries.is_empty();
    for (key, _) in &entries {
        match key {
            LuaValue::Integer(i) if *i > 0 => numeric_indexes.push(*i),
            LuaValue::Number(n)
                if n.is_finite() && n.fract() == 0.0 && *n > 0.0 && *n <= i64::MAX as f64 =>
            {
                numeric_indexes.push(*n as i64)
            }
            _ => all_array_keys = false,
        }
    }
    if all_array_keys {
        numeric_indexes.sort_unstable();
        let contiguous = numeric_indexes
            .iter()
            .enumerate()
            .all(|(idx, key)| *key == idx as i64 + 1);
        if contiguous {
            let mut by_index: Vec<(i64, LuaValue)> = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                let idx = match key {
                    LuaValue::Integer(i) => i,
                    LuaValue::Number(n) => n as i64,
                    _ => unreachable!(),
                };
                by_index.push((idx, value));
            }
            by_index.sort_by_key(|(idx, _)| *idx);
            let mut out = String::from("[");
            for (idx, (_, value)) in by_index.into_iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                out.push_str(&lua_value_to_json_string(value, cfg, depth)?);
            }
            out.push(']');
            return Ok(out);
        }
    }

    let mut out = String::from("{");
    for (idx, (key, value)) in entries.into_iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        let key_string = match key {
            LuaValue::String(s) => json_escape_string(s.as_bytes().as_ref())?,
            LuaValue::Integer(i) => serde_json::to_string(&i.to_string())
                .map_err(|err| LuaError::RuntimeError(err.to_string()))?,
            LuaValue::Number(n)
                if n.is_finite() && n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 =>
            {
                serde_json::to_string(&(n as i64).to_string())
                    .map_err(|err| LuaError::RuntimeError(err.to_string()))?
            }
            _ => {
                return Err(LuaError::RuntimeError(
                    "Cannot serialise table: table key must be a number or string".to_string(),
                ))
            }
        };
        out.push_str(&key_string);
        out.push(':');
        out.push_str(&lua_value_to_json_string(value, cfg, depth)?);
    }
    out.push('}');
    Ok(out)
}

fn json_value_to_lua(
    lua: &Lua,
    value: &serde_json::Value,
    max_depth: usize,
    depth: usize,
) -> mlua::Result<LuaValue> {
    match value {
        serde_json::Value::Null => Ok(cjson_null_value()),
        serde_json::Value::Bool(v) => Ok(LuaValue::Boolean(*v)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(LuaValue::Integer(i))
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    Ok(LuaValue::Integer(u as i64))
                } else {
                    Ok(LuaValue::Number(u as f64))
                }
            } else if let Some(f) = n.as_f64() {
                Ok(LuaValue::Number(f))
            } else {
                Err(LuaError::RuntimeError(
                    "Cannot deserialise number".to_string(),
                ))
            }
        }
        serde_json::Value::String(s) => Ok(LuaValue::String(lua.create_string(s.as_bytes())?)),
        serde_json::Value::Array(items) => {
            if depth + 1 > max_depth {
                return Err(LuaError::RuntimeError(
                    "Found too many nested data structures".to_string(),
                ));
            }
            let table = lua.create_table()?;
            for (idx, item) in items.iter().enumerate() {
                table.raw_set(
                    idx as i64 + 1,
                    json_value_to_lua(lua, item, max_depth, depth + 1)?,
                )?;
            }
            Ok(LuaValue::Table(table))
        }
        serde_json::Value::Object(map) => {
            if depth + 1 > max_depth {
                return Err(LuaError::RuntimeError(
                    "Found too many nested data structures".to_string(),
                ));
            }
            let table = lua.create_table()?;
            for (key, item) in map {
                table.raw_set(
                    key.as_str(),
                    json_value_to_lua(lua, item, max_depth, depth + 1)?,
                )?;
            }
            Ok(LuaValue::Table(table))
        }
    }
}

fn cjson_bool_config<F>(
    args: MultiValue,
    cfg: &Rc<RefCell<CjsonConfig>>,
    get: F,
) -> mlua::Result<LuaValue>
where
    F: Fn(&mut CjsonConfig) -> &mut bool,
{
    let mut guard = cfg.borrow_mut();
    let slot = get(&mut guard);
    let old = *slot;
    if args.is_empty() {
        return Ok(LuaValue::Boolean(old));
    }
    if args.len() != 1 {
        return Err(LuaError::RuntimeError("expected 1 argument".to_string()));
    }
    *slot = match args.front() {
        Some(LuaValue::Boolean(v)) => *v,
        Some(LuaValue::Integer(v)) => *v != 0,
        Some(LuaValue::Number(v)) => *v != 0.0,
        _ => {
            return Err(LuaError::RuntimeError(
                "expected boolean argument".to_string(),
            ))
        }
    };
    Ok(LuaValue::Boolean(old))
}

fn cjson_i64_config<F>(
    args: MultiValue,
    cfg: &Rc<RefCell<CjsonConfig>>,
    get: F,
) -> mlua::Result<LuaValue>
where
    F: Fn(&mut CjsonConfig) -> &mut i64,
{
    let mut guard = cfg.borrow_mut();
    let slot = get(&mut guard);
    let old = *slot;
    if args.is_empty() {
        return Ok(LuaValue::Integer(old));
    }
    if args.len() != 1 {
        return Err(LuaError::RuntimeError("expected 1 argument".to_string()));
    }
    let next = match args.front() {
        Some(LuaValue::Integer(v)) => *v,
        Some(LuaValue::Number(v)) if v.is_finite() => *v as i64,
        _ => {
            return Err(LuaError::RuntimeError(
                "expected integer argument".to_string(),
            ))
        }
    };
    if next <= 0 {
        return Err(LuaError::RuntimeError(
            "expected positive integer argument".to_string(),
        ));
    }
    *slot = next;
    Ok(LuaValue::Integer(old))
}

fn cjson_depth_config<F>(
    args: MultiValue,
    cfg: &Rc<RefCell<CjsonConfig>>,
    get: F,
) -> mlua::Result<LuaValue>
where
    F: Fn(&mut CjsonConfig) -> &mut usize,
{
    let mut guard = cfg.borrow_mut();
    let slot = get(&mut guard);
    let old = *slot as i64;
    if args.is_empty() {
        return Ok(LuaValue::Integer(old));
    }
    if args.len() != 1 {
        return Err(LuaError::RuntimeError("expected 1 argument".to_string()));
    }
    let next = match args.front() {
        Some(LuaValue::Integer(v)) => *v,
        Some(LuaValue::Number(v)) if v.is_finite() => *v as i64,
        _ => {
            return Err(LuaError::RuntimeError(
                "expected integer argument".to_string(),
            ))
        }
    };
    if next <= 0 {
        return Err(LuaError::RuntimeError(
            "expected positive integer argument".to_string(),
        ));
    }
    *slot = next as usize;
    Ok(LuaValue::Integer(old))
}

fn create_cjson_table(lua: &Lua, cfg: Rc<RefCell<CjsonConfig>>) -> mlua::Result<LuaTable> {
    let table = lua.create_table()?;

    let encode_cfg = Rc::clone(&cfg);
    table.raw_set(
        "encode",
        lua.create_function(move |_lua, value: LuaValue| {
            let cfg = encode_cfg.borrow();
            lua_value_to_json_string(value, &cfg, 0)
        })?,
    )?;

    let decode_cfg = Rc::clone(&cfg);
    table.raw_set(
        "decode",
        lua.create_function(move |lua, input: mlua::String| {
            let parsed: serde_json::Value =
                serde_json::from_slice(input.as_bytes().as_ref()).map_err(|err| {
                    LuaError::RuntimeError(format!("Expected value but found invalid JSON: {}", err))
                })?;
            let max_depth = decode_cfg.borrow().decode_max_depth;
            json_value_to_lua(lua, &parsed, max_depth, 0)
        })?,
    )?;

    let keep_cfg = Rc::clone(&cfg);
    table.raw_set(
        "encode_keep_buffer",
        lua.create_function(move |_lua, args: MultiValue| {
            cjson_bool_config(args, &keep_cfg, |cfg| &mut cfg.encode_keep_buffer)
        })?,
    )?;

    let enc_depth_cfg = Rc::clone(&cfg);
    table.raw_set(
        "encode_max_depth",
        lua.create_function(move |_lua, args: MultiValue| {
            cjson_depth_config(args, &enc_depth_cfg, |cfg| &mut cfg.encode_max_depth)
        })?,
    )?;

    let dec_depth_cfg = Rc::clone(&cfg);
    table.raw_set(
        "decode_max_depth",
        lua.create_function(move |_lua, args: MultiValue| {
            cjson_depth_config(args, &dec_depth_cfg, |cfg| &mut cfg.decode_max_depth)
        })?,
    )?;

    let invalid_cfg = Rc::clone(&cfg);
    table.raw_set(
        "encode_invalid_numbers",
        lua.create_function(move |_lua, args: MultiValue| {
            cjson_bool_config(args, &invalid_cfg, |cfg| &mut cfg.encode_invalid_numbers)
        })?,
    )?;

    let dec_invalid_cfg = Rc::clone(&cfg);
    table.raw_set(
        "decode_invalid_numbers",
        lua.create_function(move |_lua, args: MultiValue| {
            cjson_bool_config(args, &dec_invalid_cfg, |cfg| &mut cfg.decode_invalid_numbers)
        })?,
    )?;

    let precision_cfg = Rc::clone(&cfg);
    table.raw_set(
        "encode_number_precision",
        lua.create_function(move |_lua, args: MultiValue| {
            cjson_i64_config(args, &precision_cfg, |cfg| &mut cfg.encode_number_precision)
        })?,
    )?;

    table.raw_set(
        "encode_sparse_array",
        lua.create_function(|_lua, _args: MultiValue| Ok(LuaValue::Boolean(false)))?,
    )?;
    table.raw_set(
        "new",
        lua.create_function(|lua, _args: MultiValue| {
            create_cjson_table(lua, Rc::new(RefCell::new(CjsonConfig::default())))
        })?,
    )?;
    table.raw_set("null", cjson_null_value())?;
    table.raw_set("_NAME", "cjson")?;
    table.raw_set("_VERSION", "2.1.0")?;
    Ok(table)
}

fn install_cjson(lua: &Lua) -> mlua::Result<()> {
    let cjson = create_cjson_table(lua, Rc::new(RefCell::new(CjsonConfig::default())))?;
    lua.globals().set("cjson", cjson)
}

/// Execute one inner command for `redis.call` / `redis.pcall`, capturing
/// the reply bytes the handler appended to `reply_buf` and parsing them
/// back into a [`ReplyValue`].
///
/// Restores the caller's argv and reply prefix unconditionally so the
/// outer EVAL reply is unaffected by inner dispatch side-effects.
fn run_inner_command(
    ctx: &mut CommandContext<'_>,
    args: &[Vec<u8>],
) -> Result<ReplyValue, RedisError> {
    if args.is_empty() {
        return Err(RedisError::runtime(
            b"Please specify at least one argument for this call",
        ));
    }

    let saved_argv = ctx.client_ref().argv.clone();
    let saved_reply_len = ctx.client_ref().reply_buf.len();

    let new_argv: Vec<RedisString> = args
        .iter()
        .map(|b| RedisString::from_bytes(b.as_slice()))
        .collect();
    ctx.client_mut().set_args(new_argv);

    let old_deny_blocking = ctx.client_ref().flag_deny_blocking();
    ctx.client_mut().set_flag_deny_blocking(true);

    let name_bytes = args[0].clone();
    let dispatch_result = dispatch_command_name(ctx, &name_bytes);
    ctx.client_mut().set_flag_deny_blocking(old_deny_blocking);

    let raw_reply: Vec<u8> = {
        let buf = &mut ctx.client_mut().reply_buf;
        let tail = buf.split_off(saved_reply_len);
        tail
    };

    ctx.client_mut().set_args(saved_argv);

    if let Err(err) = dispatch_result {
        if raw_reply.is_empty() {
            return Err(err);
        }
    }

    if raw_reply.is_empty() {
        return Ok(ReplyValue::Nil);
    }

    let mut cursor = ParserCursor::new(&raw_reply);
    let mut builder = ReplyBuilder::new();
    if cursor.parse_next(&mut builder).is_err() || builder.errored {
        return Err(RedisError::runtime(b"ERR could not parse inner reply"));
    }
    builder
        .out
        .ok_or_else(|| RedisError::runtime(b"ERR empty inner reply"))
}

/// Process-wide script cache. Keys are the 40-byte lowercase SHA-1 hex of
/// the source bytes. Values are the source bytes themselves.
fn script_cache() -> &'static Mutex<HashMap<[u8; 40], Vec<u8>>> {
    static CACHE: OnceLock<Mutex<HashMap<[u8; 40], Vec<u8>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn function_libraries() -> &'static Mutex<HashMap<Vec<u8>, LoadedFunctionLibrary>> {
    static LIBRARIES: OnceLock<Mutex<HashMap<Vec<u8>, LoadedFunctionLibrary>>> = OnceLock::new();
    LIBRARIES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn snapshot_function_libraries() -> Vec<LoadedFunctionLibrary> {
    let guard = match function_libraries().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.values().cloned().collect()
}

fn function_library_frame(library: &LoadedFunctionLibrary, with_code: bool) -> RespFrame {
    let mut functions = library.functions.clone();
    functions.sort_by(|a, b| ascii_casecmp_bytes(&a.name, &b.name));
    let function_items = functions.iter().map(function_definition_frame).collect();
    let mut fields = vec![
        (
            RespFrame::bulk(RedisString::from_static(b"library_name")),
            RespFrame::bulk(RedisString::from_vec(library.name.clone())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"engine")),
            RespFrame::bulk(RedisString::from_static(b"LUA")),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"functions")),
            RespFrame::array(function_items),
        ),
    ];
    if with_code {
        fields.push((
            RespFrame::bulk(RedisString::from_static(b"library_code")),
            RespFrame::bulk(RedisString::from_vec(library.code.clone())),
        ));
    }
    RespFrame::Map(fields)
}

fn function_definition_frame(function: &FunctionDefinition) -> RespFrame {
    let flags = if function.no_writes {
        RespFrame::array(vec![RespFrame::bulk(RedisString::from_static(b"no-writes"))])
    } else {
        RespFrame::array(Vec::new())
    };
    RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_static(b"name")),
            RespFrame::bulk(RedisString::from_vec(function.name.clone())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"description")),
            RespFrame::null_bulk(),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"flags")),
            flags,
        ),
    ])
}

#[derive(Clone, Copy)]
enum RestoreMode {
    Append,
    Replace,
    Flush,
}

const FUNCTION_DUMP_MAGIC: &[u8] = b"VALKEYRSFUNC1\n";

fn encode_function_dump(libraries: &[LoadedFunctionLibrary]) -> Vec<u8> {
    let mut libraries = libraries.to_vec();
    libraries.sort_by(|a, b| ascii_casecmp_bytes(&a.name, &b.name));
    let mut out = FUNCTION_DUMP_MAGIC.to_vec();
    for library in libraries {
        out.extend_from_slice(&hex_encode(&library.name));
        out.push(b' ');
        out.extend_from_slice(&hex_encode(&library.code));
        out.push(b'\n');
    }
    out
}

fn decode_function_dump(payload: &[u8]) -> RedisResult<Vec<LoadedFunctionLibrary>> {
    decode_function_dump_inner(payload).ok_or_else(function_dump_payload_error)
}

fn decode_function_dump_inner(payload: &[u8]) -> Option<Vec<LoadedFunctionLibrary>> {
    let rest = payload.strip_prefix(FUNCTION_DUMP_MAGIC)?;
    let mut libraries = Vec::new();
    for line in rest.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let split = line.iter().position(|b| *b == b' ')?;
        let name = hex_decode(&line[..split])?;
        let code = hex_decode(&line[split + 1..])?;
        let (parsed_name, library_body) = parse_function_library_header(&code).ok()?;
        if parsed_name != name {
            return None;
        }
        let functions = compile_function_library(library_body).ok()?;
        libraries.push(LoadedFunctionLibrary {
            name: parsed_name,
            code,
            functions,
        });
    }
    Some(libraries)
}

fn function_dump_payload_error() -> RedisError {
    RedisError::runtime(b"ERR DUMP payload version or checksum are wrong")
}

fn function_restore_arity_error() -> RedisError {
    RedisError::runtime(
        b"ERR unknown subcommand or wrong number of arguments for 'restore'. Try FUNCTION HELP.",
    )
}

fn hex_encode(bytes: &[u8]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 0x0f) as usize]);
    }
    out
}

fn hex_decode(bytes: &[u8]) -> Option<Vec<u8>> {
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = hex_value(pair[0])?;
        let lo = hex_value(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn glob_match_ascii_ci(pattern: &[u8], text: &[u8]) -> bool {
    let (mut pi, mut ti, mut star, mut match_i) = (0usize, 0usize, None, 0usize);
    while ti < text.len() {
        if pi < pattern.len() && pattern[pi] == b'?' {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && ascii_lower(pattern[pi]) == ascii_lower(text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star = Some(pi);
            match_i = ti;
            pi += 1;
        } else if let Some(star_i) = star {
            pi = star_i + 1;
            match_i += 1;
            ti = match_i;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// `FUNCTION LOAD [REPLACE] <LIBRARY CODE>`.
///
/// Minimal Valkey-compatible function loader for Lua libraries. It accepts the
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
    let (library_name, library_body) = parse_function_library_header(code.as_bytes())?;
    let functions = compile_function_library(library_body)?;
    let loaded = LoadedFunctionLibrary {
        name: library_name.clone(),
        code: code.as_bytes().to_vec(),
        functions,
    };

    {
        let mut guard = match function_libraries().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        install_function_library(&mut guard, loaded, replace)?;
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
    if guard.remove(library_name.as_bytes()).is_none() {
        return Err(RedisError::runtime(b"ERR Library not found"));
    }
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
                return Err(RedisError::runtime(b"ERR library name argument was not given"));
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
        install_function_library(&mut guard, library, replace)?;
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
    ctx.reply_frame(&RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_static(b"running_script")),
            RespFrame::Null,
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
    Err(RedisError::runtime(
        b"NOTBUSY No scripts in execution right now.",
    ))
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
            b"ERR Can not execute a function with write flag using *_ro command.",
        ));
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

fn install_function_library(
    libraries: &mut HashMap<Vec<u8>, LoadedFunctionLibrary>,
    loaded: LoadedFunctionLibrary,
    replace: bool,
) -> RedisResult<()> {
    if libraries.contains_key(&loaded.name) && !replace {
        let mut msg = b"ERR Library '".to_vec();
        msg.extend_from_slice(&loaded.name);
        msg.extend_from_slice(b"' already exists");
        return Err(RedisError::runtime(msg));
    }
    for library in libraries.values() {
        if library.name == loaded.name {
            continue;
        }
        for existing in &library.functions {
            if loaded
                .functions
                .iter()
                .any(|new_fn| ascii_eq_ci(&new_fn.name, &existing.name))
            {
                return Err(RedisError::runtime(b"ERR Function already exists"));
            }
        }
    }
    libraries.insert(loaded.name.clone(), loaded);
    Ok(())
}

fn find_loaded_function(name: &[u8]) -> Option<(LoadedFunctionLibrary, FunctionDefinition)> {
    let guard = match function_libraries().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    for library in guard.values() {
        for function in &library.functions {
            if ascii_eq_ci(&function.name, name) {
                return Some((library.clone(), function.clone()));
            }
        }
    }
    None
}

fn parse_function_library_header(code: &[u8]) -> RedisResult<(Vec<u8>, &[u8])> {
    if !code.starts_with(b"#!") {
        return Err(RedisError::runtime(
            b"ERR Missing library metadata. The first line must be #!lua name=<library>",
        ));
    }
    let line_end = code.iter().position(|b| *b == b'\n').unwrap_or(code.len());
    let header = &code[2..line_end];
    let body = if line_end < code.len() {
        &code[line_end + 1..]
    } else {
        &[]
    };

    let mut tokens = header
        .split(u8::is_ascii_whitespace)
        .filter(|t| !t.is_empty());
    let engine = tokens
        .next()
        .ok_or_else(|| RedisError::runtime(b"ERR Missing library engine"))?;
    if !ascii_eq_ci(engine, b"lua") {
        let mut msg = b"ERR Engine '".to_vec();
        msg.extend_from_slice(engine);
        msg.extend_from_slice(b"' not found");
        return Err(RedisError::runtime(msg));
    }

    let mut library_name: Option<Vec<u8>> = None;
    for token in tokens {
        if let Some(name) = token.strip_prefix(b"name=") {
            if library_name.is_some() {
                return Err(RedisError::runtime(b"ERR Duplicate library name metadata"));
            }
            validate_library_name(name)?;
            library_name = Some(name.to_vec());
        } else {
            let mut msg = b"ERR Unknown library metadata: ".to_vec();
            msg.extend_from_slice(token);
            return Err(RedisError::runtime(msg));
        }
    }

    let library_name =
        library_name.ok_or_else(|| RedisError::runtime(b"ERR Missing library name metadata"))?;
    Ok((library_name, body))
}

fn compile_function_library(library_body: &[u8]) -> RedisResult<Vec<FunctionDefinition>> {
    let lua = Lua::new();
    install_sandbox(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
    install_cjson(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua cjson install: {}", e).into_bytes()))?;

    let registered: RefCell<Vec<FunctionDefinition>> = RefCell::new(Vec::new());
    let load_result: Result<(), LuaError> = lua.scope(|scope| {
        let api = lua.create_table()?;
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
                    "Function already exists".to_string(),
                ));
                }
                registered.borrow_mut().push(definition);
                Ok(())
            })?
        };
        api.raw_set("register_function", register_fn)?;
        lua.globals().set("redis", api.clone())?;
        lua.globals().set("server", api)?;
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

fn parse_register_function_args(args: MultiValue) -> mlua::Result<FunctionDefinition> {
    let mut values = args.into_iter();
    let Some(first) = values.next() else {
        return Err(LuaError::RuntimeError(
            "wrong number of arguments to server.register_function".to_string(),
        ));
    };

    if let LuaValue::Table(table) = first {
        if values.next().is_some() {
            return Err(LuaError::RuntimeError(
                "wrong number of arguments to server.register_function".to_string(),
            ));
        }
        let name_value: LuaValue = table.get("function_name")?;
        let callback_value: LuaValue = table.get("callback")?;
        let name = lua_string_value_bytes(
            name_value,
            "function_name argument given to server.register_function must be a string",
        )?;
        require_lua_function(
            callback_value,
            "callback argument given to server.register_function must be a function",
        )?;
        let no_writes = match table.get::<LuaValue>("flags")? {
            LuaValue::Nil => false,
            LuaValue::Table(flags) => flags_table_has_no_writes(&flags)?,
            _ => {
                return Err(LuaError::RuntimeError(
                    "flags argument to server.register_function must be a table representing function flags"
                        .to_string(),
                ));
            }
        };
        validate_function_name(&name)?;
        return Ok(FunctionDefinition { name, no_writes });
    }

    let name = lua_string_value_bytes(
        first,
        "first argument to server.register_function must be a string",
    )?;
    let Some(callback_value) = values.next() else {
        return Err(LuaError::RuntimeError(
            "wrong number of arguments to server.register_function".to_string(),
        ));
    };
    require_lua_function(
        callback_value,
        "second argument to server.register_function must be a function",
    )?;
    let mut no_writes = false;
    if let Some(flags_value) = values.next() {
        if let LuaValue::Table(flags) = flags_value {
            no_writes = flags_table_has_no_writes(&flags)?;
        }
    }
    if values.next().is_some() {
        return Err(LuaError::RuntimeError(
            "wrong number of arguments to server.register_function".to_string(),
        ));
    }
    validate_function_name(&name)?;
    Ok(FunctionDefinition { name, no_writes })
}

fn parse_runtime_register_function_args(
    lua: &Lua,
    args: MultiValue,
) -> mlua::Result<RuntimeFunctionRegistration> {
    let mut values = args.into_iter();
    let Some(first) = values.next() else {
        return Err(LuaError::RuntimeError(
            "wrong number of arguments to server.register_function".to_string(),
        ));
    };

    if let LuaValue::Table(table) = first {
        if values.next().is_some() {
            return Err(LuaError::RuntimeError(
                "wrong number of arguments to server.register_function".to_string(),
            ));
        }
        let name = lua_string_value_bytes(
            table.get::<LuaValue>("function_name")?,
            "function_name argument given to server.register_function must be a string",
        )?;
        let callback_value: LuaValue = table.get("callback")?;
        let callback = require_lua_function(
            callback_value,
            "callback argument given to server.register_function must be a function",
        )?;
        let no_writes = match table.get::<LuaValue>("flags")? {
            LuaValue::Nil => false,
            LuaValue::Table(flags) => flags_table_has_no_writes(&flags)?,
            _ => {
                return Err(LuaError::RuntimeError(
                    "flags argument to server.register_function must be a table representing function flags"
                        .to_string(),
                ));
            }
        };
        validate_function_name(&name)?;
        return Ok(RuntimeFunctionRegistration {
            name,
            callback: lua.create_registry_value(callback)?,
            no_writes,
        });
    }

    let name = lua_string_value_bytes(
        first,
        "first argument to server.register_function must be a string",
    )?;
    let Some(callback_value) = values.next() else {
        return Err(LuaError::RuntimeError(
            "wrong number of arguments to server.register_function".to_string(),
        ));
    };
    let callback = require_lua_function(
        callback_value,
        "second argument to server.register_function must be a function",
    )?;
    let mut no_writes = false;
    if let Some(flags_value) = values.next() {
        if let LuaValue::Table(flags) = flags_value {
            no_writes = flags_table_has_no_writes(&flags)?;
        }
    }
    if values.next().is_some() {
        return Err(LuaError::RuntimeError(
            "wrong number of arguments to server.register_function".to_string(),
        ));
    }
    validate_function_name(&name)?;
    Ok(RuntimeFunctionRegistration {
        name,
        callback: lua.create_registry_value(callback)?,
        no_writes,
    })
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
    !name.is_empty()
        && name
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

fn flags_table_has_no_writes(flags: &LuaTable) -> mlua::Result<bool> {
    let mut index = 1i64;
    loop {
        let value: LuaValue = flags.raw_get(index)?;
        match value {
            LuaValue::Nil => return Ok(false),
            LuaValue::String(s) if s.as_bytes().as_ref() == b"no-writes" => return Ok(true),
            _ => index += 1,
        }
    }
}

fn function_load_lua_error(err: LuaError) -> RedisError {
    match err {
        LuaError::SyntaxError { message, .. } => RedisError::runtime(
            format!("ERR Error compiling function library: {}", message).into_bytes(),
        ),
        LuaError::RuntimeError(message) => RedisError::runtime(
            format!("ERR Error loading function library: {}", message).into_bytes(),
        ),
        other => RedisError::runtime(
            format!("ERR Error loading function library: {}", other).into_bytes(),
        ),
    }
}

fn run_loaded_function(
    ctx: &mut CommandContext<'_>,
    library: &LoadedFunctionLibrary,
    definition: &FunctionDefinition,
    keys: &[RedisString],
    argv: &[RedisString],
    ro: bool,
) -> RedisResult<()> {
    let original_db = ctx.selected_db_index();
    let (_, library_body) = parse_function_library_header(&library.code)?;
    let lua = Lua::new();
    install_sandbox(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
    install_cjson(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua cjson install: {}", e).into_bytes()))?;
    install_keys_argv(&lua, keys, argv)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;

    let ctx_cell: RefCell<&mut CommandContext<'_>> = RefCell::new(ctx);
    let registrations: RefCell<Vec<RuntimeFunctionRegistration>> = RefCell::new(Vec::new());

    let script_result: Result<LuaValue, LuaError> = lua.scope(|scope| {
        let redis_tbl = lua.create_table()?;

        let call_fn = {
            let cell = &ctx_cell;
            scope.create_function_mut(
                move |lua_inner, args: MultiValue| -> mlua::Result<LuaValue> {
                    let arg_bytes = collect_call_args(args)?;
                    if ro && call_is_write_command(&arg_bytes) {
                        return Err(LuaError::RuntimeError(
                            "Write commands are not allowed from read-only scripts".to_string(),
                        ));
                    }
                    let mut borrow = cell.borrow_mut();
                    match run_inner_command(&mut **borrow, &arg_bytes) {
                        Ok(reply) => {
                            if let ReplyValue::Error(msg) = &reply {
                                return Err(LuaError::RuntimeError(
                                    String::from_utf8_lossy(msg).into_owned(),
                                ));
                            }
                            reply_to_lua(lua_inner, &reply)
                        }
                        Err(e) => Err(LuaError::RuntimeError(
                            String::from_utf8_lossy(e.to_resp_payload().as_bytes()).into_owned(),
                        )),
                    }
                },
            )?
        };

        let pcall_fn = {
            let cell = &ctx_cell;
            scope.create_function_mut(
                move |lua_inner, args: MultiValue| -> mlua::Result<LuaValue> {
                    let arg_bytes = collect_call_args(args)?;
                    if ro && call_is_write_command(&arg_bytes) {
                        let t = lua_inner.create_table()?;
                        t.raw_set(
                            "err",
                            lua_inner.create_string(
                                "Write commands are not allowed from read-only scripts",
                            )?,
                        )?;
                        return Ok(LuaValue::Table(t));
                    }
                    let mut borrow = cell.borrow_mut();
                    match run_inner_command(&mut **borrow, &arg_bytes) {
                        Ok(reply) => reply_to_lua(lua_inner, &reply),
                        Err(e) => {
                            let msg = String::from_utf8_lossy(e.to_resp_payload().as_bytes())
                                .into_owned();
                            let t = lua_inner.create_table()?;
                            t.raw_set("err", lua_inner.create_string(&msg)?)?;
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

        let sha1hex_fn = lua.create_function(|_lua, s: mlua::String| -> mlua::Result<String> {
            let hex = sha1_hex(&s.as_bytes());
            Ok(String::from_utf8(hex.to_vec()).unwrap_or_default())
        })?;

        let replicate_fn =
            lua.create_function(|_lua, _: MultiValue| -> mlua::Result<bool> { Ok(true) })?;

        let register_fn = {
            let registrations = &registrations;
            scope.create_function_mut(move |lua_inner, args: MultiValue| -> mlua::Result<()> {
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
        redis_tbl.raw_set("register_function", register_fn)?;
        lua.globals().set("redis", redis_tbl.clone())?;
        lua.globals().set("server", redis_tbl)?;

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

        lua.load(library_body).set_name("function_library").exec()?;

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
            lua.registry_value(&registration.callback)?
        };
        let keys_table = redis_strings_to_lua_table(&lua, keys)?;
        let argv_table = redis_strings_to_lua_table(&lua, argv)?;
        callback.call::<LuaValue>((keys_table, argv_table))
    });

    ctx.set_selected_db_index(original_db);

    match script_result {
        Ok(value) => {
            let mut out: Vec<u8> = Vec::new();
            lua_to_resp(&value, &mut out);
            ctx.client_mut().reply_buf.extend_from_slice(&out);
            Ok(())
        }
        Err(LuaError::RuntimeError(msg)) => Err(RedisError::runtime(runtime_error_payload(&msg))),
        Err(LuaError::SyntaxError { message, .. }) => Err(RedisError::runtime(
            format!("ERR Error compiling function: {}", message).into_bytes(),
        )),
        Err(e) => Err(RedisError::runtime(
            format!("ERR Error running function: {}", e).into_bytes(),
        )),
    }
}

fn redis_strings_to_lua_table(lua: &Lua, values: &[RedisString]) -> mlua::Result<LuaTable> {
    let table = lua.create_table()?;
    for (i, value) in values.iter().enumerate() {
        table.raw_set(i as i64 + 1, lua.create_string(value.as_bytes())?)?;
    }
    Ok(table)
}

fn call_is_write_command(args: &[Vec<u8>]) -> bool {
    let Some(command) = args.first() else {
        return false;
    };
    let name = command.as_slice();
    ascii_eq_ci(name, b"SET")
        || ascii_eq_ci(name, b"SETEX")
        || ascii_eq_ci(name, b"PSETEX")
        || ascii_eq_ci(name, b"SETNX")
        || ascii_eq_ci(name, b"GETSET")
        || ascii_eq_ci(name, b"DEL")
        || ascii_eq_ci(name, b"UNLINK")
        || ascii_eq_ci(name, b"EXPIRE")
        || ascii_eq_ci(name, b"PEXPIRE")
        || ascii_eq_ci(name, b"EXPIREAT")
        || ascii_eq_ci(name, b"PEXPIREAT")
        || ascii_eq_ci(name, b"PERSIST")
        || ascii_eq_ci(name, b"HSET")
        || ascii_eq_ci(name, b"HDEL")
        || ascii_eq_ci(name, b"LPUSH")
        || ascii_eq_ci(name, b"RPUSH")
        || ascii_eq_ci(name, b"LPOP")
        || ascii_eq_ci(name, b"RPOP")
        || ascii_eq_ci(name, b"SADD")
        || ascii_eq_ci(name, b"SREM")
        || ascii_eq_ci(name, b"ZADD")
        || ascii_eq_ci(name, b"ZREM")
        || ascii_eq_ci(name, b"INCR")
        || ascii_eq_ci(name, b"DECR")
        || ascii_eq_ci(name, b"INCRBY")
        || ascii_eq_ci(name, b"DECRBY")
        || ascii_eq_ci(name, b"APPEND")
        || ascii_eq_ci(name, b"FLUSHDB")
        || ascii_eq_ci(name, b"FLUSHALL")
}

/// `EVAL script numkeys key [key ...] arg [arg ...]`.
///
/// Parses the argv, constructs a fresh sandboxed Lua instance, injects
/// the `redis` table plus `KEYS` / `ARGV`, runs the script, and writes
/// the result back as the outer RESP reply.
pub fn eval_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"eval"));
    }
    let script = ctx.arg_owned(1usize)?;
    let numkeys = parse_i64(ctx.arg(2usize)?.as_bytes())?;
    if numkeys < 0 {
        return Err(RedisError::runtime(b"ERR Number of keys can't be negative"));
    }
    let numkeys = numkeys as usize;
    let total_extra = ctx.arg_count().saturating_sub(3);
    if numkeys > total_extra {
        return Err(RedisError::runtime(
            b"ERR Number of keys can't be greater than number of args",
        ));
    }
    let mut keys: Vec<RedisString> = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(ctx.arg_owned(3 + i)?);
    }
    let mut argv: Vec<RedisString> = Vec::with_capacity(total_extra - numkeys);
    for i in (3 + numkeys)..ctx.arg_count() {
        argv.push(ctx.arg_owned(i)?);
    }

    run_script(ctx, script.as_bytes(), &keys, &argv)
}

/// `EVALSHA sha1 numkeys key [key ...] arg [arg ...]`.
///
/// Looks up the cached script bytes; falls through to `EVAL` on a hit, or
/// returns the canonical `-NOSCRIPT` reply on a miss.
pub fn evalsha_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"evalsha"));
    }
    let sha_in = ctx.arg_owned(1usize)?;
    let sha_norm = match normalise_sha(sha_in.as_bytes()) {
        Some(s) => s,
        None => {
            return Err(RedisError::runtime(
                b"NOSCRIPT No matching script. Please use EVAL.",
            ));
        }
    };
    let script_bytes: Option<Vec<u8>> = {
        let guard = match script_cache().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.get(&sha_norm).cloned()
    };
    let script = match script_bytes {
        Some(b) => b,
        None => {
            return Err(RedisError::runtime(
                b"NOSCRIPT No matching script. Please use EVAL.",
            ));
        }
    };

    let numkeys = parse_i64(ctx.arg(2usize)?.as_bytes())?;
    if numkeys < 0 {
        return Err(RedisError::runtime(b"ERR Number of keys can't be negative"));
    }
    let numkeys = numkeys as usize;
    let total_extra = ctx.arg_count().saturating_sub(3);
    if numkeys > total_extra {
        return Err(RedisError::runtime(
            b"ERR Number of keys can't be greater than number of args",
        ));
    }
    let mut keys: Vec<RedisString> = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(ctx.arg_owned(3 + i)?);
    }
    let mut argv: Vec<RedisString> = Vec::with_capacity(total_extra - numkeys);
    for i in (3 + numkeys)..ctx.arg_count() {
        argv.push(ctx.arg_owned(i)?);
    }

    run_script(ctx, &script, &keys, &argv)
}

/// Shared body of `EVAL` and `EVALSHA`. Creates a fresh Lua state, applies
/// the sandbox, installs `redis`, `KEYS`, `ARGV`, runs the script, and
/// converts the return value to a RESP frame written onto `reply_buf`.
fn run_script(
    ctx: &mut CommandContext<'_>,
    script_bytes: &[u8],
    keys: &[RedisString],
    argv: &[RedisString],
) -> RedisResult<()> {
    let original_db = ctx.selected_db_index();
    let lua = Lua::new();
    install_sandbox(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
    install_cjson(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua cjson install: {}", e).into_bytes()))?;
    install_keys_argv(&lua, keys, argv)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;

    let ctx_cell: RefCell<&mut CommandContext<'_>> = RefCell::new(ctx);

    let script_result: Result<LuaValue, LuaError> = lua.scope(|scope| {
        let redis_tbl = lua.create_table()?;

        let call_fn = {
            let cell = &ctx_cell;
            scope.create_function_mut(move |_lua, args: MultiValue| -> mlua::Result<LuaValue> {
                let arg_bytes = collect_call_args(args)?;
                let mut borrow = cell.borrow_mut();
                match run_inner_command(&mut **borrow, &arg_bytes) {
                    Ok(reply) => {
                        if let ReplyValue::Error(msg) = &reply {
                            return Err(LuaError::RuntimeError(
                                String::from_utf8_lossy(msg).into_owned(),
                            ));
                        }
                        reply_to_lua(_lua, &reply)
                    }
                    Err(e) => Err(LuaError::RuntimeError(
                        String::from_utf8_lossy(e.to_resp_payload().as_bytes()).into_owned(),
                    )),
                }
            })?
        };

        let pcall_fn = {
            let cell = &ctx_cell;
            scope.create_function_mut(
                move |lua_inner, args: MultiValue| -> mlua::Result<LuaValue> {
                    let arg_bytes = collect_call_args(args)?;
                    let mut borrow = cell.borrow_mut();
                    match run_inner_command(&mut **borrow, &arg_bytes) {
                        Ok(reply) => reply_to_lua(lua_inner, &reply),
                        Err(e) => {
                            let msg = String::from_utf8_lossy(e.to_resp_payload().as_bytes())
                                .into_owned();
                            let t = lua_inner.create_table()?;
                            t.raw_set("err", lua_inner.create_string(&msg)?)?;
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

        let sha1hex_fn = lua.create_function(|_lua, s: mlua::String| -> mlua::Result<String> {
            let hex = sha1_hex(&s.as_bytes());
            Ok(String::from_utf8(hex.to_vec()).unwrap_or_default())
        })?;

        let replicate_fn =
            lua.create_function(|_lua, _: MultiValue| -> mlua::Result<bool> { Ok(true) })?;

        redis_tbl.raw_set("__raw_call", call_fn)?;
        redis_tbl.raw_set("pcall", pcall_fn)?;
        redis_tbl.raw_set("error_reply", error_reply_fn)?;
        redis_tbl.raw_set("status_reply", status_reply_fn)?;
        redis_tbl.raw_set("sha1hex", sha1hex_fn)?;
        redis_tbl.raw_set("replicate_commands", replicate_fn)?;
        lua.globals().set("redis", redis_tbl.clone())?;
        lua.globals().set("server", redis_tbl)?;

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

        lua.load(script_bytes)
            .set_name("user_script")
            .eval::<LuaValue>()
    });

    ctx.set_selected_db_index(original_db);

    match script_result {
        Ok(value) => {
            let mut out: Vec<u8> = Vec::new();
            lua_to_resp(&value, &mut out);
            ctx.client_mut().reply_buf.extend_from_slice(&out);
            Ok(())
        }
        Err(LuaError::RuntimeError(msg)) => Err(RedisError::runtime(runtime_error_payload(&msg))),
        Err(LuaError::SyntaxError { message, .. }) => Err(RedisError::runtime(
            format!("ERR Error compiling script: {}", message).into_bytes(),
        )),
        Err(e) => Err(RedisError::runtime(
            format!("ERR Error running script: {}", e).into_bytes(),
        )),
    }
}

/// Collect the variadic Lua arguments passed to `redis.call(cmd, ...)`
/// into a byte-string argv suitable for [`run_inner_command`].
fn collect_call_args(args: MultiValue) -> Result<Vec<Vec<u8>>, LuaError> {
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(args.len());
    for v in args {
        out.push(lua_arg_to_bytes(&v)?);
    }
    Ok(out)
}

/// `SCRIPT` subcommand router: LOAD / EXISTS / FLUSH / HELP.
pub fn script_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"script"));
    }
    let sub = ctx.arg_owned(1usize)?;
    let sub_bytes = sub.as_bytes();
    if ascii_eq_ci(sub_bytes, b"LOAD") {
        return script_load(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"EXISTS") {
        return script_exists(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"FLUSH") {
        return script_flush(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"HELP") {
        return script_help(ctx);
    }
    let mut msg = Vec::with_capacity(64 + sub_bytes.len());
    msg.extend_from_slice(b"ERR Unknown SCRIPT subcommand or wrong number of arguments for '");
    msg.extend_from_slice(sub_bytes);
    msg.push(b'\'');
    Err(RedisError::runtime(msg))
}

fn script_load(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"script|load"));
    }
    let body = ctx.arg_owned(2usize)?;
    let hex = sha1_hex(body.as_bytes());
    {
        let mut guard = match script_cache().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.insert(hex, body.as_bytes().to_vec());
    }
    ctx.reply_bulk(&hex)
}

fn script_exists(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"script|exists"));
    }
    let guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let n = ctx.arg_count() - 2;
    ctx.reply_array_header(n as i64)?;
    for i in 0..n {
        let raw = ctx.arg_owned(2 + i)?;
        let exists = normalise_sha(raw.as_bytes())
            .map(|h| guard.contains_key(&h))
            .unwrap_or(false);
        ctx.reply_integer(if exists { 1 } else { 0 })?;
    }
    Ok(())
}

fn script_flush(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() > 3 {
        return Err(RedisError::wrong_number_of_args(b"script|flush"));
    }
    if ctx.arg_count() == 3 {
        let mode = ctx.arg_owned(2usize)?;
        let b = mode.as_bytes();
        if !ascii_eq_ci(b, b"ASYNC") && !ascii_eq_ci(b, b"SYNC") {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
    let mut guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.clear();
    ctx.reply_simple_string(b"OK")
}

fn script_help(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let lines: &[&[u8]] = &[
        b"SCRIPT EXISTS <sha1> [<sha1> ...]",
        b"    Return information about the existence of the scripts in the script cache.",
        b"SCRIPT FLUSH [ASYNC|SYNC]",
        b"    Flush the Lua scripts cache. Very dangerous on replicas.",
        b"SCRIPT LOAD <script>",
        b"    Load a script into the scripts cache without executing it.",
        b"HELP",
        b"    Prints this help.",
    ];
    ctx.reply_array_header(lines.len() as i64)?;
    for ln in lines {
        ctx.reply_bulk(ln)?;
    }
    Ok(())
}

/// Strict integer parse for `numkeys`. Reuses the canonical error string.
fn parse_i64(bytes: &[u8]) -> Result<i64, RedisError> {
    let s = std::str::from_utf8(bytes).map_err(|_| RedisError::not_integer())?;
    s.parse::<i64>().map_err(|_| RedisError::not_integer())
}

/// Accept any case for the input sha; return `Some` with the lowercase
/// canonical 40-byte buffer when the input is exactly 40 hex bytes.
fn normalise_sha(bytes: &[u8]) -> Option<[u8; 40]> {
    if bytes.len() != 40 {
        return None;
    }
    let mut out = [0u8; 40];
    for (i, b) in bytes.iter().enumerate() {
        let c = match *b {
            b'0'..=b'9' | b'a'..=b'f' => *b,
            b'A'..=b'F' => *b + 32,
            _ => return None,
        };
        out[i] = c;
    }
    Some(out)
}

fn ascii_eq_ci(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| ascii_lower(*x) == ascii_lower(*y))
}

fn ascii_casecmp_bytes(a: &[u8], b: &[u8]) -> Ordering {
    let mut ai = a.iter();
    let mut bi = b.iter();
    loop {
        match (ai.next(), bi.next()) {
            (Some(x), Some(y)) => match ascii_lower(*x).cmp(&ascii_lower(*y)) {
                Ordering::Equal => continue,
                other => return other,
            },
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (None, None) => return Ordering::Equal,
        }
    }
}

fn ascii_lower(b: u8) -> u8 {
    if b.is_ascii_uppercase() {
        b + 32
    } else {
        b
    }
}

/// Compute the lowercase 40-byte SHA-1 hex digest of `data` using a
/// pure-Rust implementation. Stays inside this crate so we do not pull in
/// a hash-crate dependency for a single use site.
fn sha1_hex(data: &[u8]) -> [u8; 40] {
    let digest = sha1_digest(data);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 40];
    for (i, byte) in digest.iter().enumerate() {
        out[i * 2] = HEX[(byte >> 4) as usize];
        out[i * 2 + 1] = HEX[(byte & 0x0f) as usize];
    }
    out
}

/// Compute the raw 20-byte SHA-1 digest of `data`.
///
/// Direct translation of FIPS 180-4 §6.1.2; zero unsafe, no dependency.
fn sha1_digest(data: &[u8]) -> [u8; 20] {
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;

    let bit_len: u64 = (data.len() as u64) * 8;

    let mut padded: Vec<u8> = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;

        for i in 0..80 {
            let (f, k) = if i < 20 {
                ((b & c) | ((!b) & d), 0x5A827999u32)
            } else if i < 40 {
                (b ^ c ^ d, 0x6ED9EBA1u32)
            } else if i < 60 {
                ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32)
            } else {
                (b ^ c ^ d, 0xCA62C1D6u32)
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[i]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use redis_core::{pubsub_registry::PubSubRegistry, RedisDb, RedisServer};

    use super::*;

    #[test]
    fn sha1_hex_known_vectors() {
        let empty = sha1_hex(b"");
        assert_eq!(&empty, b"da39a3ee5e6b4b0d3255bfef95601890afd80709");
        let abc = sha1_hex(b"abc");
        assert_eq!(&abc, b"a9993e364706816aba3e25717850c26c9cd0d89d");
    }

    #[test]
    fn normalise_sha_lowercases() {
        let upper = b"DA39A3EE5E6B4B0D3255BFEF95601890AFD80709";
        let n = normalise_sha(upper).unwrap();
        assert_eq!(&n, b"da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn normalise_sha_rejects_non_hex() {
        assert!(normalise_sha(b"short").is_none());
        assert!(normalise_sha(b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_none());
    }

    #[test]
    fn eval_select_does_not_leak_db() {
        let server = Arc::new(RedisServer::default());
        let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
        let mut client = redis_core::Client::new(7);
        client.db_index = 10;
        client.set_args(vec![
            RedisString::from_bytes(b"EVAL"),
            RedisString::from_bytes(b"return redis.call('select', '9')"),
            RedisString::from_bytes(b"0"),
        ]);
        let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
        let mut ctx = redis_core::CommandContext::with_server_and_db_list(
            &mut client,
            &mut dbs,
            server,
            pubsub,
        );
        eval_command(&mut ctx).unwrap();
        assert_eq!(client.db_index, 10);
        assert_eq!(client.drain_reply(), b"+OK\r\n");
    }

    #[test]
    fn eval_redis_call_error_is_single_resp_error_line() {
        let mut client = redis_core::Client::new(8);
        client.set_args(vec![
            RedisString::from_bytes(b"EVAL"),
            RedisString::from_bytes(b"redis.call('nosuchcommand')"),
            RedisString::from_bytes(b"0"),
        ]);
        let mut ctx = CommandContext::new(&mut client);
        let err = eval_command(&mut ctx).unwrap_err();
        let payload = err.to_resp_payload();
        let bytes = payload.as_bytes();
        assert!(bytes.starts_with(b"ERR "));
        assert!(bytes
            .windows(b"unknown command".len())
            .any(|w| w.eq_ignore_ascii_case(b"unknown command")));
        assert!(!bytes.contains(&b'\n'));
        assert!(!bytes.contains(&b'\r'));
        assert!(!bytes
            .windows(b"stack traceback".len())
            .any(|w| w == b"stack traceback"));
    }

    #[test]
    fn run_inner_wait_is_script_safe() {
        let mut client = redis_core::Client::new(1);
        let mut outer: redis_core::Client = redis_core::Client::new(1);
        client.set_args(vec![
            RedisString::from_bytes(b"SET"),
            RedisString::from_bytes(b"x"),
            RedisString::from_bytes(b"1"),
        ]);
        let original_args = client.argv.clone();
        let mut ctx = CommandContext::new(&mut client);
        let reply =
            run_inner_command(&mut ctx, &[b"WAIT".to_vec(), b"1".to_vec(), b"0".to_vec()]).unwrap();

        match reply {
            ReplyValue::Integer(v) => assert_eq!(v, 0),
            _ => panic!("expected integer reply from WAIT inside script"),
        }
        assert_eq!(client.argv, original_args);

        let mut wait_ctx = CommandContext::new(&mut outer);
        let wait_reply = run_inner_command(
            &mut wait_ctx,
            &[
                b"WAITAOF".to_vec(),
                b"0".to_vec(),
                b"1".to_vec(),
                b"0".to_vec(),
            ],
        )
        .unwrap();
        match wait_reply {
            ReplyValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], ReplyValue::Integer(0)));
                assert!(matches!(items[1], ReplyValue::Integer(0)));
            }
            _ => panic!("expected two-item array reply from WAITAOF inside script"),
        }

        wait_ctx.client_mut().set_args(vec![
            RedisString::from_bytes(b"waitaof"),
            RedisString::from_bytes(b"0"),
            RedisString::from_bytes(b"1"),
            RedisString::from_bytes(b"0"),
        ]);
        let direct = crate::dispatch::dispatch_command_name(&mut wait_ctx, b"waitaof");
        if direct.is_ok() {
            assert_eq!(wait_ctx.client_mut().drain_reply(), b"*2\r\n:0\r\n:0\r\n");
        } else {
            panic!("WAITAOF handler should be registered");
        }
    }

    #[test]
    fn resp3_double_and_null_reply_shapes_match_lua_bridge() {
        let lua = Lua::new();

        let double = reply_to_lua(&lua, &ReplyValue::Double(1.25)).unwrap();
        match double {
            LuaValue::Table(t) => assert_eq!(t.raw_get::<f64>("double").unwrap(), 1.25),
            other => panic!("expected table for RESP3 double, got {other:?}"),
        }

        assert!(matches!(
            reply_to_lua(&lua, &ReplyValue::Null).unwrap(),
            LuaValue::Nil
        ));
        assert!(matches!(
            reply_to_lua(&lua, &ReplyValue::Nil).unwrap(),
            LuaValue::Boolean(false)
        ));
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Session 1A — EVAL / EVALSHA / SCRIPT family
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         4 (EVAL_RO, script replication, SCRIPT KILL,
//                    pcall traceback formatting)
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         mlua-backed Lua 5.1 runtime, per-call instance, sandboxed.
//                  Pure-Rust SHA-1; reply parser reused from redis-protocol.
//                  Minimal FUNCTION LOAD/FCALL bridge is backed by this runtime.
// ──────────────────────────────────────────────────────────────────────────
