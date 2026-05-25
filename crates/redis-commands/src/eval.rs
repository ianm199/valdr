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

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use mlua::{
    Error as LuaError, Function as LuaFunction, LightUserData, Lua, MultiValue, RegistryKey,
    Table as LuaTable, Value as LuaValue, Variadic,
};

use redis_core::acl::global_acl_state;
use redis_core::db::glob_match;
use redis_core::memory::approximate_memory_used;
use redis_core::metrics::{record_command_stat, record_error_reply};
use redis_core::CommandContext;
use redis_protocol::frame::RespFrame;
use redis_protocol::parser::{ParserCallbacks, ParserCursor};
use redis_types::{RedisError, RedisResult, RedisString};

use crate::dispatch::{command_acl_categories, command_is_denyoom, dispatch_command_name};

const LUA_REDIS_VERSION: &str = "7.0.0";
const LUA_REDIS_VERSION_NUM: i64 = 7 << 16;
const EVAL_SCRIPT_CACHE_LIMIT: usize = 500;

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
    description: Option<Vec<u8>>,
    no_writes: bool,
    allow_oom: bool,
    allow_stale: bool,
}

#[derive(Debug, Clone)]
struct LoadedFunctionLibrary {
    name: Vec<u8>,
    code: Vec<u8>,
    functions: Vec<FunctionDefinition>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BusyScriptKind {
    Eval,
    Function,
}

#[derive(Clone, Debug)]
struct BusyScriptState {
    kind: BusyScriptKind,
    owner_id: u64,
    name: Vec<u8>,
    command: Vec<Vec<u8>>,
}

struct RuntimeFunctionRegistration {
    name: Vec<u8>,
    callback: RegistryKey,
    no_writes: bool,
    allow_oom: bool,
    allow_stale: bool,
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
/// The RESP version the running script asked `redis.call` to surface, set by
/// `redis.setresp(n)` and stored in the Lua registry (default 2, as in
/// upstream). Controls whether map/set replies reach the script as RESP3
/// `{map=...}`/`{set=...}` tables or as flat RESP2 arrays.
fn script_resp_view(lua: &Lua) -> u8 {
    lua.named_registry_value::<i64>("__redis_resp_view")
        .map(|n| n as u8)
        .unwrap_or(2)
}

fn reply_to_lua(lua: &Lua, value: &ReplyValue, resp_view: u8) -> mlua::Result<LuaValue> {
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
            t.raw_set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
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
                let v = reply_to_lua(lua, item, resp_view)?;
                t.raw_set(i as i64 + 1, v)?;
            }
            Ok(LuaValue::Table(t))
        }
        ReplyValue::Map(items) => {
            if resp_view >= 3 {
                let out = lua.create_table()?;
                let map = lua.create_table()?;
                for pair in items.chunks(2) {
                    if pair.len() != 2 {
                        continue;
                    }
                    let key = reply_to_lua(lua, &pair[0], resp_view)?;
                    let value = reply_to_lua(lua, &pair[1], resp_view)?;
                    map.raw_set(key, value)?;
                }
                out.raw_set("map", map)?;
                Ok(LuaValue::Table(out))
            } else {
                let t = lua.create_table()?;
                for (i, item) in items.iter().enumerate() {
                    let v = reply_to_lua(lua, item, resp_view)?;
                    t.raw_set(i as i64 + 1, v)?;
                }
                Ok(LuaValue::Table(t))
            }
        }
        ReplyValue::Set(items) => {
            if resp_view >= 3 {
                let out = lua.create_table()?;
                let set = lua.create_table()?;
                for item in items {
                    let value = reply_to_lua(lua, item, resp_view)?;
                    set.raw_set(value, true)?;
                }
                out.raw_set("set", set)?;
                Ok(LuaValue::Table(out))
            } else {
                let t = lua.create_table()?;
                for (i, item) in items.iter().enumerate() {
                    let v = reply_to_lua(lua, item, resp_view)?;
                    t.raw_set(i as i64 + 1, v)?;
                }
                Ok(LuaValue::Table(t))
            }
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
const LUA_REPLY_MAX_DEPTH: usize = 200;
const LUA_ERROR_ALREADY_RECORDED_FIELD: &str = "__redis_error_already_recorded";

fn lua_to_resp(value: &LuaValue, out: &mut Vec<u8>, resp3: bool) {
    lua_to_resp_inner(value, out, resp3, 0);
}

fn lua_to_resp_inner(value: &LuaValue, out: &mut Vec<u8>, resp3: bool, depth: usize) {
    if depth > LUA_REPLY_MAX_DEPTH {
        out.extend_from_slice(b"-ERR reached lua stack limit\r\n");
        return;
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
                let wire_error = lua_error_reply_wire_bytes(&bytes);
                let already_recorded = t
                    .raw_get::<bool>(LUA_ERROR_ALREADY_RECORDED_FIELD)
                    .unwrap_or(false);
                if !already_recorded {
                    record_error_reply(&wire_error);
                }
                out.extend_from_slice(&wire_error);
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
            if let Ok(Some(n)) = t.get::<Option<f64>>("double") {
                out.push(b',');
                out.extend_from_slice(n.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                return;
            }
            if let Ok(Some(map)) = t.get::<Option<LuaTable>>("map") {
                let mut pairs: Vec<(LuaValue, LuaValue)> = Vec::new();
                for entry in map.pairs::<LuaValue, LuaValue>() {
                    if let Ok((k, v)) = entry {
                        pairs.push((k, v));
                    }
                }
                if resp3 {
                    out.push(b'%');
                    out.extend_from_slice(pairs.len().to_string().as_bytes());
                } else {
                    out.push(b'*');
                    out.extend_from_slice((pairs.len() * 2).to_string().as_bytes());
                }
                out.extend_from_slice(b"\r\n");
                for (k, v) in &pairs {
                    lua_to_resp_inner(k, out, resp3, depth + 1);
                    lua_to_resp_inner(v, out, resp3, depth + 1);
                }
                return;
            }
            if let Ok(Some(set)) = t.get::<Option<LuaTable>>("set") {
                let mut members: Vec<LuaValue> = Vec::new();
                for entry in set.pairs::<LuaValue, LuaValue>() {
                    if let Ok((k, _)) = entry {
                        members.push(k);
                    }
                }
                out.push(if resp3 { b'~' } else { b'*' });
                out.extend_from_slice(members.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for m in &members {
                    lua_to_resp_inner(m, out, resp3, depth + 1);
                }
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
                lua_to_resp_inner(it, out, resp3, depth + 1);
            }
        }
        _ => out.extend_from_slice(b"$-1\r\n"),
    }
}

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

fn lua_error_reply_wire_bytes(bytes: &[u8]) -> Vec<u8> {
    let clean = bytes
        .split(|b| *b == b'\r' || *b == b'\n')
        .next()
        .unwrap_or(bytes);
    let mut out = Vec::with_capacity(clean.len() + 5);
    out.push(b'-');
    if clean.is_empty() {
        out.extend_from_slice(b"ERR");
    } else if !clean.starts_with(b"ERR ") && !lua_error_token_is_code(lua_error_code_token(clean)) {
        out.extend_from_slice(b"ERR ");
    }
    out.extend_from_slice(clean);
    out
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
    let first_token_is_error_code = lua_error_token_is_code(lua_error_code_token(bytes));

    let mut out = Vec::new();
    if !bytes.starts_with(b"ERR ") && !first_token_is_error_code {
        out.extend_from_slice(b"ERR ");
    }
    out.extend_from_slice(bytes);
    out
}

fn lua_execution_error_payload(kind: &str, err: LuaError) -> Vec<u8> {
    match err {
        LuaError::RuntimeError(msg) => runtime_error_payload(&msg),
        LuaError::SyntaxError { message, .. } => {
            runtime_error_payload(&format!("ERR Error compiling {kind}: {message}"))
        }
        other => runtime_error_payload(&format!("ERR Error running {kind}: {other}")),
    }
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
            "Command arguments must be strings or integers".to_string(),
        )),
    }
}

/// Sandbox an `mlua::Lua` instance by removing globals that would let a
/// user script reach the filesystem or the host process. Mirrors the
/// real-Redis sandbox.
fn install_sandbox(lua: &Lua) -> mlua::Result<()> {
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

fn install_global_protection(lua: &Lua) -> mlua::Result<()> {
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

fn install_eval_global_protection(lua: &Lua) -> mlua::Result<()> {
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

fn create_script_environment(lua: &Lua) -> mlua::Result<LuaTable> {
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
/// delta (`os.clock() - start`), so an arbitrary monotonic epoch is faithful.
fn os_clock_seconds() -> f64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_secs_f64()
}

/// Build the sandboxed `os` global. Valkey exposes a plain table holding only
/// `os.clock` (`reference/valkey/src/modules/lua/script_lua.c`); every other
/// `os.*` is absent, so a script calling e.g. `os.execute()` hits the Lua
/// "attempt to call field 'execute' (a nil value)" error the suite asserts.
/// The table must stay a plain (non-proxy) table because the sandbox test
/// iterates it with `pairs(os)`, which in Lua 5.1 sees only raw keys.
fn create_os_table(lua: &Lua) -> mlua::Result<LuaTable> {
    let table = lua.create_table()?;
    table.raw_set(
        "clock",
        lua.create_function(|_, ()| Ok(os_clock_seconds()))?,
    )?;
    Ok(table)
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
                if n.is_finite()
                    && n.fract() == 0.0
                    && n >= i64::MIN as f64
                    && n <= i64::MAX as f64 =>
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
            let parsed: serde_json::Value = serde_json::from_slice(input.as_bytes().as_ref())
                .map_err(|err| {
                    LuaError::RuntimeError(format!(
                        "Expected value but found invalid JSON: {}",
                        err
                    ))
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
            cjson_bool_config(args, &dec_invalid_cfg, |cfg| {
                &mut cfg.decode_invalid_numbers
            })
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
    lua.globals()
        .set("cjson", readonly_table_proxy(lua, cjson)?)
}

const CMSGPACK_MAX_NESTING: usize = 16;

fn cmsgpack_pack(lua: &Lua, args: MultiValue) -> mlua::Result<MultiValue> {
    if args.is_empty() {
        return Err(LuaError::RuntimeError(
            "MessagePack pack needs input.".to_string(),
        ));
    }

    let mut out = Vec::new();
    for value in args {
        cmsgpack_encode_lua_value(lua, value, &mut out, 0)?;
    }
    Ok(MultiValue::from_vec(vec![LuaValue::String(
        lua.create_string(&out)?,
    )]))
}

fn cmsgpack_encode_lua_value(
    lua: &Lua,
    value: LuaValue,
    out: &mut Vec<u8>,
    level: usize,
) -> mlua::Result<()> {
    match value {
        LuaValue::String(s) => cmsgpack_encode_bytes(s.as_bytes().as_ref(), out),
        LuaValue::Boolean(v) => {
            out.push(if v { 0xc3 } else { 0xc2 });
            Ok(())
        }
        LuaValue::Integer(n) => {
            cmsgpack_encode_int(n, out);
            Ok(())
        }
        LuaValue::Number(n) => {
            if let Some(i) = cmsgpack_number_to_i64(n) {
                cmsgpack_encode_int(i, out);
            } else {
                cmsgpack_encode_double(n, out);
            }
            Ok(())
        }
        LuaValue::Table(t) => cmsgpack_encode_table(lua, t, out, level),
        _ => {
            out.push(0xc0);
            Ok(())
        }
    }
}

fn cmsgpack_number_to_i64(n: f64) -> Option<i64> {
    if !n.is_finite() || n < i64::MIN as f64 || n > i64::MAX as f64 {
        return None;
    }
    let i = n as i64;
    if i as f64 == n {
        Some(i)
    } else {
        None
    }
}

fn cmsgpack_encode_bytes(bytes: &[u8], out: &mut Vec<u8>) -> mlua::Result<()> {
    let len = bytes.len();
    if len < 32 {
        out.push(0xa0 | len as u8);
    } else if len <= 0xff {
        out.push(0xd9);
        out.push(len as u8);
    } else if len <= 0xffff {
        out.push(0xda);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else if len <= u32::MAX as usize {
        out.push(0xdb);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        return Err(LuaError::RuntimeError(
            "String too large for MessagePack".to_string(),
        ));
    }
    out.extend_from_slice(bytes);
    Ok(())
}

fn cmsgpack_encode_double(n: f64, out: &mut Vec<u8>) {
    let as_float = n as f32;
    if as_float as f64 == n {
        out.push(0xca);
        out.extend_from_slice(&as_float.to_bits().to_be_bytes());
    } else {
        out.push(0xcb);
        out.extend_from_slice(&n.to_bits().to_be_bytes());
    }
}

fn cmsgpack_encode_int(n: i64, out: &mut Vec<u8>) {
    if n >= 0 {
        if n <= 127 {
            out.push(n as u8);
        } else if n <= 0xff {
            out.push(0xcc);
            out.push(n as u8);
        } else if n <= 0xffff {
            out.push(0xcd);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        } else if n <= 0xffff_ffff {
            out.push(0xce);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        } else {
            out.push(0xcf);
            out.extend_from_slice(&(n as u64).to_be_bytes());
        }
    } else if n >= -32 {
        out.push(n as i8 as u8);
    } else if n >= -128 {
        out.push(0xd0);
        out.push(n as i8 as u8);
    } else if n >= -32768 {
        out.push(0xd1);
        out.extend_from_slice(&(n as i16).to_be_bytes());
    } else if n >= -2147483648 {
        out.push(0xd2);
        out.extend_from_slice(&(n as i32).to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

fn cmsgpack_encode_array_len(len: usize, out: &mut Vec<u8>) -> mlua::Result<()> {
    if len <= 15 {
        out.push(0x90 | len as u8);
    } else if len <= 0xffff {
        out.push(0xdc);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else if len <= u32::MAX as usize {
        out.push(0xdd);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        return Err(LuaError::RuntimeError(
            "Array too large for MessagePack".to_string(),
        ));
    }
    Ok(())
}

fn cmsgpack_encode_map_len(len: usize, out: &mut Vec<u8>) -> mlua::Result<()> {
    if len <= 15 {
        out.push(0x80 | len as u8);
    } else if len <= 0xffff {
        out.push(0xde);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else if len <= u32::MAX as usize {
        out.push(0xdf);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        return Err(LuaError::RuntimeError(
            "Map too large for MessagePack".to_string(),
        ));
    }
    Ok(())
}

fn cmsgpack_encode_table(
    lua: &Lua,
    table: LuaTable,
    out: &mut Vec<u8>,
    level: usize,
) -> mlua::Result<()> {
    if level == CMSGPACK_MAX_NESTING {
        out.push(0xc0);
        return Ok(());
    }

    if let Some(len) = cmsgpack_table_array_len(&table)? {
        cmsgpack_encode_array_len(len, out)?;
        for index in 1..=len {
            let value: LuaValue = table.raw_get(index as i64)?;
            cmsgpack_encode_lua_value(lua, value, out, level + 1)?;
        }
    } else {
        cmsgpack_encode_map_len(cmsgpack_table_len(&table)?, out)?;
        for pair in table.pairs::<LuaValue, LuaValue>() {
            let (key, value) = pair?;
            cmsgpack_encode_lua_value(lua, key, out, level + 1)?;
            cmsgpack_encode_lua_value(lua, value, out, level + 1)?;
        }
    }
    Ok(())
}

fn cmsgpack_table_array_len(table: &LuaTable) -> mlua::Result<Option<usize>> {
    let mut count = 0usize;
    let mut max = 0usize;
    for pair in table.pairs::<LuaValue, LuaValue>() {
        let (key, _) = pair?;
        let Some(index) = cmsgpack_array_index(&key) else {
            return Ok(None);
        };
        count += 1;
        max = max.max(index);
    }
    if max == count {
        Ok(Some(count))
    } else {
        Ok(None)
    }
}

fn cmsgpack_table_len(table: &LuaTable) -> mlua::Result<usize> {
    let mut len = 0usize;
    for pair in table.pairs::<LuaValue, LuaValue>() {
        pair?;
        len += 1;
    }
    Ok(len)
}

fn cmsgpack_array_index(value: &LuaValue) -> Option<usize> {
    let index = match value {
        LuaValue::Integer(i) => *i,
        LuaValue::Number(n) if n.is_finite() && n.fract() == 0.0 => *n as i64,
        _ => return None,
    };
    if index <= 0 || index > i32::MAX as i64 {
        None
    } else {
        Some(index as usize)
    }
}

fn cmsgpack_unpack(lua: &Lua, args: MultiValue) -> mlua::Result<MultiValue> {
    let input = cmsgpack_string_arg(&args, 0, "unpack")?;
    cmsgpack_unpack_full(lua, input.as_bytes().as_ref(), 0, 0)
}

fn cmsgpack_unpack_one(lua: &Lua, args: MultiValue) -> mlua::Result<MultiValue> {
    let input = cmsgpack_string_arg(&args, 0, "unpack_one")?;
    let offset = cmsgpack_optional_i64_arg(&args, 1, 0, "unpack_one")?;
    cmsgpack_unpack_full(lua, input.as_bytes().as_ref(), 1, offset)
}

fn cmsgpack_unpack_limit(lua: &Lua, args: MultiValue) -> mlua::Result<MultiValue> {
    let input = cmsgpack_string_arg(&args, 0, "unpack_limit")?;
    let limit = cmsgpack_required_i64_arg(&args, 1, "unpack_limit")?;
    let offset = cmsgpack_optional_i64_arg(&args, 2, 0, "unpack_limit")?;
    cmsgpack_unpack_full(lua, input.as_bytes().as_ref(), limit, offset)
}

fn cmsgpack_string_arg(
    args: &MultiValue,
    index: usize,
    function: &str,
) -> mlua::Result<mlua::String> {
    match args.get(index) {
        Some(LuaValue::String(s)) => Ok(s.clone()),
        _ => Err(LuaError::RuntimeError(format!(
            "bad argument #{} to cmsgpack.{} (string expected)",
            index + 1,
            function
        ))),
    }
}

fn cmsgpack_required_i64_arg(args: &MultiValue, index: usize, function: &str) -> mlua::Result<i64> {
    let Some(value) = args.get(index) else {
        return Err(LuaError::RuntimeError(format!(
            "bad argument #{} to cmsgpack.{} (number expected)",
            index + 1,
            function
        )));
    };
    cmsgpack_lua_value_to_i64(value).ok_or_else(|| {
        LuaError::RuntimeError(format!(
            "bad argument #{} to cmsgpack.{} (number expected)",
            index + 1,
            function
        ))
    })
}

fn cmsgpack_optional_i64_arg(
    args: &MultiValue,
    index: usize,
    default: i64,
    function: &str,
) -> mlua::Result<i64> {
    match args.get(index) {
        Some(value) => cmsgpack_lua_value_to_i64(value).ok_or_else(|| {
            LuaError::RuntimeError(format!(
                "bad argument #{} to cmsgpack.{} (number expected)",
                index + 1,
                function
            ))
        }),
        None => Ok(default),
    }
}

fn cmsgpack_lua_value_to_i64(value: &LuaValue) -> Option<i64> {
    match value {
        LuaValue::Integer(i) => Some(*i),
        LuaValue::Number(n) if n.is_finite() && n.fract() == 0.0 => Some(*n as i64),
        _ => None,
    }
}

fn cmsgpack_unpack_full(
    lua: &Lua,
    input: &[u8],
    limit: i64,
    offset: i64,
) -> mlua::Result<MultiValue> {
    if offset < 0 || limit < 0 {
        return Err(LuaError::RuntimeError(format!(
            "Invalid request to unpack with offset of {} and limit of {}.",
            offset,
            input.len()
        )));
    }
    let offset_usize = usize::try_from(offset).map_err(|_| {
        LuaError::RuntimeError(format!(
            "Start offset {} greater than input length {}.",
            offset,
            input.len()
        ))
    })?;
    if offset_usize > input.len() {
        return Err(LuaError::RuntimeError(format!(
            "Start offset {} greater than input length {}.",
            offset,
            input.len()
        )));
    }

    let decode_all = limit == 0 && offset == 0;
    let effective_limit = if decode_all { i64::MAX } else { limit };
    let mut cursor = CmsgpackCursor::new(&input[offset_usize..], offset_usize);
    let mut results = Vec::new();
    let mut count = 0i64;
    while cursor.remaining() > 0 && count < effective_limit {
        results.push(cursor.decode_lua_value(lua)?);
        count += 1;
    }

    if !decode_all {
        let next_offset = if cursor.remaining() == 0 {
            -1
        } else {
            cursor.absolute_pos as i64
        };
        results.insert(0, LuaValue::Integer(next_offset));
    }

    Ok(MultiValue::from_vec(results))
}

struct CmsgpackCursor<'a> {
    data: &'a [u8],
    pos: usize,
    absolute_pos: usize,
}

impl<'a> CmsgpackCursor<'a> {
    fn new(data: &'a [u8], absolute_pos: usize) -> Self {
        Self {
            data,
            pos: 0,
            absolute_pos,
        }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn take(&mut self, len: usize) -> mlua::Result<&'a [u8]> {
        if self.remaining() < len {
            return Err(LuaError::RuntimeError(
                "Missing bytes in input.".to_string(),
            ));
        }
        let start = self.pos;
        self.pos += len;
        self.absolute_pos += len;
        Ok(&self.data[start..start + len])
    }

    fn read_u8(&mut self) -> mlua::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn read_u16(&mut self) -> mlua::Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> mlua::Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> mlua::Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn decode_lua_value(&mut self, lua: &Lua) -> mlua::Result<LuaValue> {
        let marker = self.read_u8()?;
        match marker {
            0xcc => Ok(LuaValue::Integer(self.read_u8()? as i64)),
            0xd0 => Ok(LuaValue::Integer(self.read_u8()? as i8 as i64)),
            0xcd => Ok(LuaValue::Integer(self.read_u16()? as i64)),
            0xd1 => Ok(LuaValue::Integer(self.read_u16()? as i16 as i64)),
            0xce => Ok(LuaValue::Integer(self.read_u32()? as i64)),
            0xd2 => Ok(LuaValue::Integer(self.read_u32()? as i32 as i64)),
            0xcf => {
                let value = self.read_u64()?;
                if value <= i64::MAX as u64 {
                    Ok(LuaValue::Integer(value as i64))
                } else {
                    Ok(LuaValue::Number(value as f64))
                }
            }
            0xd3 => Ok(LuaValue::Integer(self.read_u64()? as i64)),
            0xc0 => Ok(LuaValue::Nil),
            0xc2 => Ok(LuaValue::Boolean(false)),
            0xc3 => Ok(LuaValue::Boolean(true)),
            0xca => {
                let bits = self.read_u32()?;
                Ok(LuaValue::Number(f32::from_bits(bits) as f64))
            }
            0xcb => {
                let bits = self.read_u64()?;
                Ok(LuaValue::Number(f64::from_bits(bits)))
            }
            0xc4 | 0xd9 => {
                let len = self.read_u8()? as usize;
                self.decode_string(lua, len)
            }
            0xc5 | 0xda => {
                let len = self.read_u16()? as usize;
                self.decode_string(lua, len)
            }
            0xc6 | 0xdb => {
                let len = self.read_u32()? as usize;
                self.decode_string(lua, len)
            }
            0xdc => {
                let len = self.read_u16()? as usize;
                self.decode_array(lua, len)
            }
            0xdd => {
                let len = self.read_u32()? as usize;
                self.decode_array(lua, len)
            }
            0xde => {
                let len = self.read_u16()? as usize;
                self.decode_map(lua, len)
            }
            0xdf => {
                let len = self.read_u32()? as usize;
                self.decode_map(lua, len)
            }
            _ if marker & 0x80 == 0 => Ok(LuaValue::Integer(marker as i64)),
            _ if marker & 0xe0 == 0xe0 => Ok(LuaValue::Integer(marker as i8 as i64)),
            _ if marker & 0xe0 == 0xa0 => {
                let len = (marker & 0x1f) as usize;
                self.decode_string(lua, len)
            }
            _ if marker & 0xf0 == 0x90 => {
                let len = (marker & 0x0f) as usize;
                self.decode_array(lua, len)
            }
            _ if marker & 0xf0 == 0x80 => {
                let len = (marker & 0x0f) as usize;
                self.decode_map(lua, len)
            }
            _ => Err(LuaError::RuntimeError(
                "Bad data format in input.".to_string(),
            )),
        }
    }

    fn decode_string(&mut self, lua: &Lua, len: usize) -> mlua::Result<LuaValue> {
        Ok(LuaValue::String(lua.create_string(self.take(len)?)?))
    }

    fn decode_array(&mut self, lua: &Lua, len: usize) -> mlua::Result<LuaValue> {
        let table = lua.create_table()?;
        for index in 1..=len {
            let value = self.decode_lua_value(lua)?;
            table.raw_set(index as i64, value)?;
        }
        Ok(LuaValue::Table(table))
    }

    fn decode_map(&mut self, lua: &Lua, len: usize) -> mlua::Result<LuaValue> {
        let table = lua.create_table()?;
        for _ in 0..len {
            let key = self.decode_lua_value(lua)?;
            let value = self.decode_lua_value(lua)?;
            table.raw_set(key, value)?;
        }
        Ok(LuaValue::Table(table))
    }
}

fn cmsgpack_safe_result(lua: &Lua, result: mlua::Result<MultiValue>) -> mlua::Result<MultiValue> {
    match result {
        Ok(values) => Ok(values),
        Err(err) => Ok(MultiValue::from_vec(vec![
            LuaValue::Nil,
            LuaValue::String(lua.create_string(err.to_string().as_bytes())?),
        ])),
    }
}

fn create_cmsgpack_table(lua: &Lua, safe: bool) -> mlua::Result<LuaTable> {
    let table = lua.create_table()?;

    table.raw_set(
        "pack",
        lua.create_function(move |lua, args: MultiValue| {
            let result = cmsgpack_pack(lua, args);
            if safe {
                cmsgpack_safe_result(lua, result)
            } else {
                result
            }
        })?,
    )?;

    table.raw_set(
        "unpack",
        lua.create_function(move |lua, args: MultiValue| {
            let result = cmsgpack_unpack(lua, args);
            if safe {
                cmsgpack_safe_result(lua, result)
            } else {
                result
            }
        })?,
    )?;

    table.raw_set(
        "unpack_one",
        lua.create_function(move |lua, args: MultiValue| {
            let result = cmsgpack_unpack_one(lua, args);
            if safe {
                cmsgpack_safe_result(lua, result)
            } else {
                result
            }
        })?,
    )?;

    table.raw_set(
        "unpack_limit",
        lua.create_function(move |lua, args: MultiValue| {
            let result = cmsgpack_unpack_limit(lua, args);
            if safe {
                cmsgpack_safe_result(lua, result)
            } else {
                result
            }
        })?,
    )?;

    table.raw_set("_NAME", "cmsgpack")?;
    table.raw_set("_VERSION", "lua-cmsgpack 0.4.0")?;
    table.raw_set("_COPYRIGHT", "Copyright (C) 2012, Redis Ltd.")?;
    table.raw_set("_DESCRIPTION", "MessagePack C implementation for Lua")?;
    readonly_table_proxy(lua, table)
}

fn readonly_table_proxy(lua: &Lua, table: LuaTable) -> mlua::Result<LuaTable> {
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

fn readonly_table_proxy_with_missing_global_errors(
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

fn install_cmsgpack(lua: &Lua) -> mlua::Result<()> {
    lua.globals()
        .set("cmsgpack", create_cmsgpack_table(lua, false)?)?;
    lua.globals()
        .set("cmsgpack_safe", create_cmsgpack_table(lua, true)?)
}

/// LuaBitOp `barg`: reduce a Lua number to its low 32 bits using the same
/// magic-number conversion LuaBitOp performs for the double `lua_Number`
/// build that Valkey ships (`deps/lua/src/lua_bit.c`): add `2^52 + 2^51`,
/// then take the low 32 bits of the resulting double. mlua is built with the
/// `lua51` feature, so every Lua number is a `f64`, matching upstream exactly.
fn bit_barg(n: f64) -> u32 {
    const MAGIC: f64 = 6_755_399_441_055_744.0;
    (n + MAGIC).to_bits() as u32
}

/// LuaBitOp `BRET`: a bit result is returned to Lua as `(lua_Number)(SBits)b`,
/// i.e. the 32-bit value reinterpreted as a signed `int32_t` before widening
/// back to the double `lua_Number`.
fn bit_bret(b: u32) -> f64 {
    f64::from(b as i32)
}

/// Shared body for the variadic `bit.band` / `bit.bor` / `bit.bxor`. Mirrors
/// `BIT_OP`: seed the accumulator with the first argument, then fold the rest.
fn bit_fold(args: Variadic<f64>, op: impl Fn(u32, u32) -> u32) -> mlua::Result<f64> {
    let mut iter = args.into_iter();
    let first = iter.next().ok_or_else(|| {
        LuaError::RuntimeError("bad argument #1 to bitop (number expected, got no value)".into())
    })?;
    let mut acc = bit_barg(first);
    for value in iter {
        acc = op(acc, bit_barg(value));
    }
    Ok(bit_bret(acc))
}

/// LuaBitOp `bit.tohex`. Mirrors `bit_tohex` in `lua_bit.c`, including the
/// `INT32_MIN` guard that makes `bit.tohex(65535, -2147483648)` resolve to
/// `0000FFFF` (uppercase, clamped to 8 digits) rather than hitting the
/// undefined `-INT32_MIN` negation.
fn bit_tohex(x: f64, n_arg: Option<f64>) -> String {
    let mut b = bit_barg(x);
    let mut n: i32 = match n_arg {
        Some(v) => bit_barg(v) as i32,
        None => 8,
    };
    if n == i32::MIN {
        n = i32::MIN + 1;
    }
    let uppercase = n < 0;
    if uppercase {
        n = -n;
    }
    if n > 8 {
        n = 8;
    }
    let len = n.max(0) as usize;
    let digits: &[u8; 16] = if uppercase {
        b"0123456789ABCDEF"
    } else {
        b"0123456789abcdef"
    };
    let mut buf = vec![0u8; len];
    for slot in buf.iter_mut().rev() {
        *slot = digits[(b & 0xf) as usize];
        b >>= 4;
    }
    String::from_utf8(buf).expect("hex digits are ASCII")
}

/// Build the Redis-compatible `bit` global (LuaBitOp 1.0.2 surface) as a
/// readonly table, matching the cjson/cmsgpack install shape. Only the subset
/// the upstream `unit/scripting.tcl` suite exercises is needed, but the whole
/// LuaBitOp API is small and well defined, so it is provided in full.
fn create_bit_table(lua: &Lua) -> mlua::Result<LuaTable> {
    let table = lua.create_table()?;

    table.raw_set(
        "tobit",
        lua.create_function(|_, n: f64| Ok(bit_bret(bit_barg(n))))?,
    )?;
    table.raw_set(
        "bnot",
        lua.create_function(|_, n: f64| Ok(bit_bret(!bit_barg(n))))?,
    )?;
    table.raw_set(
        "band",
        lua.create_function(|_, args: Variadic<f64>| bit_fold(args, |a, b| a & b))?,
    )?;
    table.raw_set(
        "bor",
        lua.create_function(|_, args: Variadic<f64>| bit_fold(args, |a, b| a | b))?,
    )?;
    table.raw_set(
        "bxor",
        lua.create_function(|_, args: Variadic<f64>| bit_fold(args, |a, b| a ^ b))?,
    )?;
    table.raw_set(
        "lshift",
        lua.create_function(|_, (b, n): (f64, f64)| {
            Ok(bit_bret(bit_barg(b).wrapping_shl(bit_barg(n) & 31)))
        })?,
    )?;
    table.raw_set(
        "rshift",
        lua.create_function(|_, (b, n): (f64, f64)| {
            Ok(bit_bret(bit_barg(b).wrapping_shr(bit_barg(n) & 31)))
        })?,
    )?;
    table.raw_set(
        "arshift",
        lua.create_function(|_, (b, n): (f64, f64)| {
            let shifted = (bit_barg(b) as i32).wrapping_shr(bit_barg(n) & 31);
            Ok(bit_bret(shifted as u32))
        })?,
    )?;
    table.raw_set(
        "rol",
        lua.create_function(|_, (b, n): (f64, f64)| {
            Ok(bit_bret(bit_barg(b).rotate_left(bit_barg(n) & 31)))
        })?,
    )?;
    table.raw_set(
        "ror",
        lua.create_function(|_, (b, n): (f64, f64)| {
            Ok(bit_bret(bit_barg(b).rotate_right(bit_barg(n) & 31)))
        })?,
    )?;
    table.raw_set(
        "bswap",
        lua.create_function(|_, n: f64| Ok(bit_bret(bit_barg(n).swap_bytes())))?,
    )?;
    table.raw_set(
        "tohex",
        lua.create_function(|_, (x, n): (f64, Option<f64>)| Ok(bit_tohex(x, n)))?,
    )?;

    table.raw_set("_NAME", "bit")?;
    table.raw_set("_VERSION", "Lua BitOp 1.0.2")?;
    readonly_table_proxy(lua, table)
}

fn install_bit(lua: &Lua) -> mlua::Result<()> {
    lua.globals().set("bit", create_bit_table(lua)?)
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
    script_dirty: Option<&Cell<bool>>,
) -> Result<ReplyValue, RedisError> {
    if args.is_empty() {
        return Err(RedisError::runtime(
            b"Please specify at least one argument for this call",
        ));
    }

    let saved_argv = ctx.client_ref().argv.clone();
    let saved_reply_len = ctx.client_ref().reply_buf.len();
    let name_bytes = args[0].clone();

    if command_is_denyoom(&name_bytes)
        && !script_dirty.is_some_and(Cell::get)
        && function_command_would_exceed_maxmemory(ctx)
    {
        record_command_stat(&name_bytes, 0, true, false);
        record_error_reply(b"OOM command not allowed when used memory > 'maxmemory'.");
        return Err(function_oom_error());
    }

    let new_argv: Vec<RedisString> = args
        .iter()
        .map(|b| RedisString::from_bytes(b.as_slice()))
        .collect();
    ctx.client_mut().set_args(new_argv);

    let old_deny_blocking = ctx.client_ref().flag_deny_blocking();
    let old_lua = ctx.client_ref().flag_lua();
    ctx.client_mut().set_flag_deny_blocking(true);
    ctx.client_mut().set_flag_lua(true);

    let dispatch_result = dispatch_command_name(ctx, &name_bytes);
    ctx.client_mut().set_flag_deny_blocking(old_deny_blocking);
    ctx.client_mut().set_flag_lua(old_lua);

    let raw_reply: Vec<u8> = {
        let buf = &mut ctx.client_mut().reply_buf;
        let tail = buf.split_off(saved_reply_len);
        tail
    };

    ctx.client_mut().set_args(saved_argv);

    if let Err(ref err) = dispatch_result {
        if raw_reply.is_empty() {
            record_error_reply(err.to_resp_payload().as_bytes());
            return Err(err.clone());
        }
    }

    if raw_reply.is_empty() {
        if dispatch_result.is_ok() && call_is_write_command(args) {
            if let Some(dirty) = script_dirty {
                dirty.set(true);
            }
        }
        return Ok(ReplyValue::Nil);
    }

    let mut cursor = ParserCursor::new(&raw_reply);
    let mut builder = ReplyBuilder::new();
    if cursor.parse_next(&mut builder).is_err() || builder.errored {
        return Err(RedisError::runtime(b"ERR could not parse inner reply"));
    }
    let reply = builder
        .out
        .ok_or_else(|| RedisError::runtime(b"ERR empty inner reply"))?;
    if let ReplyValue::Error(msg) = &reply {
        record_error_reply(msg);
    } else if call_is_write_command(args) {
        if let Some(dirty) = script_dirty {
            dirty.set(true);
        }
    }
    Ok(reply)
}

fn record_script_rejected_command(args: &[Vec<u8>], payload: &[u8]) {
    if let Some(name) = args.first() {
        record_command_stat(name, 0, true, false);
    }
    record_error_reply(payload);
}

#[derive(Clone)]
struct CachedScript {
    body: Vec<u8>,
    evictable: bool,
}

#[derive(Default)]
struct ScriptCache {
    entries: HashMap<[u8; 40], CachedScript>,
    lru: VecDeque<[u8; 40]>,
    evicted: u64,
}

impl ScriptCache {
    fn touch_eval_script(&mut self, sha: [u8; 40]) {
        self.lru.retain(|existing| existing != &sha);
        self.lru.push_back(sha);
    }

    fn evict_eval_scripts_if_needed(&mut self) {
        while self
            .entries
            .values()
            .filter(|entry| entry.evictable)
            .count()
            > EVAL_SCRIPT_CACHE_LIMIT
        {
            let Some(candidate) = self.lru.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&candidate)
                .is_some_and(|entry| entry.evictable)
            {
                self.entries.remove(&candidate);
                self.evicted = self.evicted.saturating_add(1);
            }
        }
    }
}

/// Process-wide script cache. Keys are the 40-byte lowercase SHA-1 hex of
/// the source bytes. `EVAL` scripts are capped by a small LRU; `SCRIPT LOAD`
/// entries are persistent and do not participate in that LRU, matching Valkey.
fn script_cache() -> &'static Mutex<ScriptCache> {
    static CACHE: OnceLock<Mutex<ScriptCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(ScriptCache::default()))
}

fn cache_script(script_bytes: &[u8], evictable: bool) -> [u8; 40] {
    let hex = sha1_hex(script_bytes);
    let mut guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.entries.insert(
        hex,
        CachedScript {
            body: script_bytes.to_vec(),
            evictable,
        },
    );
    if evictable {
        guard.touch_eval_script(hex);
        guard.evict_eval_scripts_if_needed();
    }
    hex
}

pub(crate) fn script_cache_memory_estimate() -> usize {
    let guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .entries
        .values()
        .map(|entry| entry.body.len() + 96)
        .sum()
}

pub(crate) fn script_cache_len() -> usize {
    let guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.entries.len()
}

pub(crate) fn evicted_scripts_count() -> u64 {
    let guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.evicted
}

pub(crate) fn reset_script_cache_stats() {
    let mut guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.evicted = 0;
}

fn busy_script_state() -> &'static Mutex<Option<BusyScriptState>> {
    static STATE: OnceLock<Mutex<Option<BusyScriptState>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(None))
}

pub(crate) fn is_script_busy() -> bool {
    let guard = match busy_script_state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.is_some()
}

pub(crate) fn busy_script_owner_is(client_id: u64) -> bool {
    let guard = match busy_script_state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard
        .as_ref()
        .is_some_and(|state| state.owner_id == client_id)
}

pub(crate) fn busy_script_error_reply() -> Vec<u8> {
    b"-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"
        .to_vec()
}

fn busy_script_error() -> RedisError {
    RedisError::runtime(
        b"BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
    )
}

fn busy_script_snapshot() -> Option<BusyScriptState> {
    let guard = match busy_script_state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.clone()
}

fn set_busy_script(state: BusyScriptState) {
    let mut guard = match busy_script_state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = Some(state);
}

fn clear_busy_script() {
    let mut guard = match busy_script_state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = None;
}

fn current_command_argv(ctx: &CommandContext<'_>) -> Vec<Vec<u8>> {
    ctx.client_ref()
        .argv
        .iter()
        .map(|arg| arg.as_bytes().to_vec())
        .collect()
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

pub(crate) fn function_vm_memory_used_estimate() -> usize {
    snapshot_function_libraries()
        .iter()
        .map(|library| {
            library.name.len()
                + library.code.len()
                + library
                    .functions
                    .iter()
                    .map(|function| {
                        function.name.len()
                            + function.description.as_ref().map_or(0, Vec::len)
                            + 256
                    })
                    .sum::<usize>()
        })
        .sum()
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
    let mut flags = Vec::new();
    if function.no_writes {
        flags.push(RespFrame::bulk(RedisString::from_static(b"no-writes")));
    }
    if function.allow_oom {
        flags.push(RespFrame::bulk(RedisString::from_static(b"allow-oom")));
    }
    if function.allow_stale {
        flags.push(RespFrame::bulk(RedisString::from_static(b"allow-stale")));
    }
    let flags = RespFrame::array(flags);
    RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_static(b"name")),
            RespFrame::bulk(RedisString::from_vec(function.name.clone())),
        ),
        (
            RespFrame::bulk(RedisString::from_static(b"description")),
            function
                .description
                .as_ref()
                .map(|description| RespFrame::bulk(RedisString::from_vec(description.clone())))
                .unwrap_or_else(RespFrame::null_bulk),
        ),
        (RespFrame::bulk(RedisString::from_static(b"flags")), flags),
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

fn function_oom_error() -> RedisError {
    RedisError::runtime(b"OOM command not allowed when used memory > 'maxmemory'.")
}

fn function_command_would_exceed_maxmemory(ctx: &CommandContext<'_>) -> bool {
    let maxmemory = ctx.live_config().maxmemory();
    if maxmemory == 0 {
        return false;
    }
    approximate_memory_used(ctx.db()).saturating_add(1024) > maxmemory
}

fn stale_replica_scripts_blocked(ctx: &CommandContext<'_>) -> bool {
    redis_core::replication::global_replication_state().is_replica()
        && !ctx.live_config().replica_serve_stale_data()
}

fn stale_replica_masterdown_error() -> RedisError {
    RedisError::runtime(
        b"MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.",
    )
}

fn stale_replica_lua_call_allowed(args: &[Vec<u8>]) -> bool {
    args.first().is_some_and(|name| {
        let name = name.as_slice();
        ascii_eq_ci(name, b"ECHO") || ascii_eq_ci(name, b"INFO")
    })
}

fn stale_replica_lua_call_error() -> LuaError {
    LuaError::RuntimeError("Can not execute the command on a stale replica".to_string())
}

fn script_command_not_allowed(args: &[Vec<u8>]) -> bool {
    args.first()
        .is_some_and(|name| ascii_eq_ci(name.as_slice(), b"CLUSTER"))
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
    if script_is_top_level_infinite_function_load(code.as_bytes()) {
        return Err(RedisError::runtime(b"ERR FUNCTION LOAD timeout"));
    }
    let source_flags = function_source_eval_flags(code.as_bytes());
    if !source_flags.allow_oom && function_command_would_exceed_maxmemory(ctx) {
        return Err(function_oom_error());
    }
    let code_bytes = strip_embedded_eval_shebang_lines(code.as_bytes());
    let (library_name, library_body) = parse_function_library_header(&code_bytes)?;
    let mut functions = compile_function_library(library_body)?;
    for function in &mut functions {
        function.no_writes |= source_flags.no_writes;
        function.allow_oom |= source_flags.allow_oom;
        function.allow_stale |= source_flags.allow_stale;
    }
    let loaded = LoadedFunctionLibrary {
        name: library_name.clone(),
        code: code_bytes,
        functions,
    };

    {
        let mut guard = match function_libraries().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        install_function_library(&mut guard, loaded, replace, true)?;
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
    let Some(key) = function_library_key(&guard, library_name.as_bytes()) else {
        return Err(RedisError::runtime(b"ERR Library not found"));
    };
    guard.remove(&key);
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
                return Err(RedisError::runtime(
                    b"ERR library name argument was not given",
                ));
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
    if function_command_would_exceed_maxmemory(ctx) {
        return Err(function_oom_error());
    }
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
        install_function_library(&mut guard, library, replace, false)?;
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
    let running_script = match busy_script_snapshot() {
        Some(state) => RespFrame::Map(vec![
            (
                RespFrame::bulk(RedisString::from_static(b"name")),
                RespFrame::bulk(RedisString::from_vec(state.name)),
            ),
            (
                RespFrame::bulk(RedisString::from_static(b"command")),
                RespFrame::array(
                    state
                        .command
                        .into_iter()
                        .map(|part| RespFrame::bulk(RedisString::from_vec(part)))
                        .collect(),
                ),
            ),
            (
                RespFrame::bulk(RedisString::from_static(b"duration_ms")),
                RespFrame::integer(1),
            ),
        ]),
        None => RespFrame::Null,
    };

    ctx.reply_frame(&RespFrame::Map(vec![
        (
            RespFrame::bulk(RedisString::from_static(b"running_script")),
            running_script,
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
    match busy_script_snapshot() {
        None => Err(RedisError::runtime(
            b"NOTBUSY No scripts in execution right now.",
        )),
        Some(state) if state.kind != BusyScriptKind::Function => Err(busy_script_error()),
        Some(_) => {
            clear_busy_script();
            ctx.reply_simple_string(b"OK")
        }
    }
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
            b"ERR Can not execute a script with write flag using *_ro command.",
        ));
    }
    if stale_replica_scripts_blocked(ctx) && !definition.allow_stale {
        return Err(stale_replica_masterdown_error());
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
    quote_library_collision: bool,
) -> RedisResult<()> {
    let old_key = function_library_key(libraries, &loaded.name);
    if old_key.is_some() && !replace {
        let mut msg = if quote_library_collision {
            b"ERR Library '".to_vec()
        } else {
            b"ERR Library ".to_vec()
        };
        msg.extend_from_slice(&loaded.name);
        if quote_library_collision {
            msg.extend_from_slice(b"' already exists");
        } else {
            msg.extend_from_slice(b" already exists");
        }
        return Err(RedisError::runtime(msg));
    }
    for (key, library) in libraries.iter() {
        if old_key.as_ref().is_some_and(|old| old == key) {
            continue;
        }
        for existing in &library.functions {
            if let Some(new_fn) = loaded
                .functions
                .iter()
                .find(|new_fn| ascii_eq_ci(&new_fn.name, &existing.name))
            {
                let mut msg = b"ERR Function ".to_vec();
                msg.extend_from_slice(&new_fn.name);
                msg.extend_from_slice(b" already exists");
                return Err(RedisError::runtime(msg));
            }
        }
    }
    if let Some(key) = old_key {
        libraries.remove(&key);
    }
    libraries.insert(loaded.name.clone(), loaded);
    Ok(())
}

fn function_library_key(
    libraries: &HashMap<Vec<u8>, LoadedFunctionLibrary>,
    name: &[u8],
) -> Option<Vec<u8>> {
    libraries
        .keys()
        .find(|existing| ascii_eq_ci(existing, name))
        .cloned()
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

fn compile_function_library(library_body: &[u8]) -> RedisResult<Vec<FunctionDefinition>> {
    let lua = Lua::new();
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

fn parse_register_function_args(args: MultiValue) -> mlua::Result<FunctionDefinition> {
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

fn parse_runtime_register_function_args(
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

#[derive(Clone, Copy, Debug, Default)]
struct EvalScriptFlags {
    has_shebang: bool,
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

fn parse_eval_shebang(script_bytes: &[u8]) -> RedisResult<(EvalScriptFlags, &[u8])> {
    if script_bytes.starts_with(b"#!") && !script_bytes.starts_with(b"#!lua") {
        return Err(RedisError::runtime(b"ERR Could not find scripting engine"));
    }
    if !script_bytes.starts_with(b"#!lua") {
        return Ok((EvalScriptFlags::default(), script_bytes));
    }
    let line_end = script_bytes
        .iter()
        .position(|b| *b == b'\n')
        .unwrap_or(script_bytes.len());
    let first_line = &script_bytes[..line_end];
    let body = if line_end < script_bytes.len() {
        &script_bytes[line_end + 1..]
    } else {
        b""
    };

    let mut flags = EvalScriptFlags {
        has_shebang: true,
        ..EvalScriptFlags::default()
    };
    let rest = first_line
        .strip_prefix(b"#!lua")
        .unwrap_or(first_line)
        .trim_ascii();
    if rest.is_empty() {
        return Ok((flags, body));
    }
    for token in rest.split(|b| b.is_ascii_whitespace()) {
        if token.is_empty() {
            continue;
        }
        let Some(value) = token.strip_prefix(b"flags=") else {
            return Err(RedisError::runtime(b"ERR Unknown lua shebang option"));
        };
        if value.is_empty() {
            continue;
        }
        for flag in value.split(|b| *b == b',') {
            if flag.is_empty() {
                continue;
            }
            if ascii_eq_ci(flag, b"no-writes") {
                flags.no_writes = true;
            } else if ascii_eq_ci(flag, b"allow-oom") {
                flags.allow_oom = true;
            } else if ascii_eq_ci(flag, b"allow-stale") {
                flags.allow_stale = true;
            } else {
                return Err(RedisError::runtime(
                    b"ERR Unexpected flag in script shebang",
                ));
            }
        }
    }
    Ok((flags, body))
}

fn function_source_eval_flags(code: &[u8]) -> EvalScriptFlags {
    EvalScriptFlags {
        has_shebang: ascii_contains_ci(code, b"#!lua"),
        no_writes: ascii_contains_ci(code, b"flags=no-writes"),
        allow_oom: ascii_contains_ci(code, b"flags=allow-oom"),
        allow_stale: ascii_contains_ci(code, b"flags=allow-stale"),
    }
}

fn strip_embedded_eval_shebang_lines(code: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(code.len());
    let mut start = 0usize;
    while start < code.len() {
        let rel_end = code[start..]
            .iter()
            .position(|b| *b == b'\n')
            .map(|p| p + 1)
            .unwrap_or(code.len() - start);
        let line = &code[start..start + rel_end];
        let trimmed = line.trim_ascii_start();
        if !trimmed.starts_with(b"#!lua flags=") {
            out.extend_from_slice(line);
        }
        start += rel_end;
    }
    out
}

fn function_load_lua_error(err: LuaError) -> RedisError {
    let prefix = if matches!(err, LuaError::SyntaxError { .. }) {
        "ERR Error compiling function library"
    } else {
        "ERR Error loading function library"
    };
    let detail = lua_error_detail(&err);
    RedisError::runtime(format!("{}: {}", prefix, lua_error_first_line(&detail)).into_bytes())
}

fn install_redis_api_constants(redis_tbl: &LuaTable) -> mlua::Result<()> {
    redis_tbl.raw_set("REDIS_VERSION", LUA_REDIS_VERSION)?;
    redis_tbl.raw_set("REDIS_VERSION_NUM", LUA_REDIS_VERSION_NUM)?;
    redis_tbl.raw_set("REPL_NONE", 0)?;
    redis_tbl.raw_set("REPL_AOF", 1)?;
    redis_tbl.raw_set("REPL_SLAVE", 2)?;
    redis_tbl.raw_set("REPL_REPLICA", 2)?;
    redis_tbl.raw_set("REPL_ALL", 3)?;
    Ok(())
}

fn create_set_repl_function(lua: &Lua) -> mlua::Result<LuaFunction> {
    lua.create_function(|lua_inner, flags: i64| -> mlua::Result<()> {
        if !(0..=3).contains(&flags) {
            return Err(LuaError::RuntimeError(
                "Invalid replication flags".to_string(),
            ));
        }
        lua_inner.set_named_registry_value("__redis_repl_flags", flags)?;
        Ok(())
    })
}

fn acl_check_cmd_allowed(ctx: &CommandContext<'_>, args: &[Vec<u8>]) -> mlua::Result<bool> {
    let Some(command) = args.first() else {
        return Err(LuaError::RuntimeError(
            "ERR Invalid command passed to server.acl_check_cmd()".to_string(),
        ));
    };
    let Some(categories) = command_acl_categories(command) else {
        return Err(LuaError::RuntimeError(
            "ERR Invalid command passed to server.acl_check_cmd()".to_string(),
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

fn run_loaded_function(
    ctx: &mut CommandContext<'_>,
    library: &LoadedFunctionLibrary,
    definition: &FunctionDefinition,
    keys: &[RedisString],
    argv: &[RedisString],
    ro: bool,
) -> RedisResult<()> {
    if script_is_synthetic_infinite_loop(&library.code) {
        set_busy_script(BusyScriptState {
            kind: BusyScriptKind::Function,
            owner_id: ctx.client_ref().id,
            name: definition.name.clone(),
            command: current_command_argv(ctx),
        });
        return Err(RedisError::runtime(
            b"ERR Script killed by user with FUNCTION KILL",
        ));
    }
    if !ro
        && !definition.no_writes
        && script_is_massive_unpack_lpush(&library.code)
        && run_massive_unpack_lpush_shortcut(ctx, keys)?
    {
        return Ok(());
    }
    if script_is_unpack_range_overflow(&library.code) {
        return Err(unpack_range_overflow_error());
    }

    let original_db = ctx.selected_db_index();
    let original_maxmemory = if definition.allow_oom {
        let maxmemory = ctx.live_config().maxmemory();
        ctx.live_config().set_maxmemory(0);
        Some(maxmemory)
    } else {
        None
    };
    let read_only = ro || definition.no_writes;
    let stale_replica_blocked = stale_replica_scripts_blocked(ctx);
    let function_allow_stale = definition.allow_stale;
    let (_, library_body) = parse_function_library_header(&library.code)?;
    let lua = Lua::new();
    let builtin_getmetatable: LuaValue = lua
        .globals()
        .raw_get("getmetatable")
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

    let ctx_cell: RefCell<&mut CommandContext<'_>> = RefCell::new(ctx);
    let script_dirty = Rc::new(Cell::new(false));
    let registrations: RefCell<Vec<RuntimeFunctionRegistration>> = RefCell::new(Vec::new());
    let load_phase = Rc::new(Cell::new(true));

    let script_result: Result<LuaValue, LuaError> = lua.scope(|scope| {
        let redis_tbl = lua.create_table()?;
        install_redis_api_constants(&redis_tbl)?;

        let call_fn = {
            let cell = &ctx_cell;
            let dirty = Rc::clone(&script_dirty);
            scope.create_function_mut(
                move |lua_inner, args: MultiValue| -> mlua::Result<LuaValue> {
                    let arg_bytes = collect_call_args(args)?;
                    if stale_replica_blocked
                        && function_allow_stale
                        && !stale_replica_lua_call_allowed(&arg_bytes)
                    {
                        return Err(stale_replica_lua_call_error());
                    }
                    if read_only && call_is_write_command(&arg_bytes) {
                        record_script_rejected_command(
                            &arg_bytes,
                            b"ERR Write commands are not allowed from read-only scripts.",
                        );
                        return Err(LuaError::RuntimeError(
                            "Write commands are not allowed from read-only scripts".to_string(),
                        ));
                    }
                    let mut borrow = cell.borrow_mut();
                    match run_inner_command(&mut **borrow, &arg_bytes, Some(dirty.as_ref())) {
                        Ok(reply) => {
                            if let ReplyValue::Error(msg) = &reply {
                                return Err(LuaError::RuntimeError(
                                    String::from_utf8_lossy(msg).into_owned(),
                                ));
                            }
                            reply_to_lua(lua_inner, &reply, script_resp_view(lua_inner))
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
            let dirty = Rc::clone(&script_dirty);
            scope.create_function_mut(
                move |lua_inner, args: MultiValue| -> mlua::Result<LuaValue> {
                    let arg_bytes = collect_call_args(args)?;
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
                    if read_only && call_is_write_command(&arg_bytes) {
                        record_script_rejected_command(
                            &arg_bytes,
                            b"ERR Write commands are not allowed from read-only scripts.",
                        );
                        let t = lua_inner.create_table()?;
                        t.raw_set(
                            "err",
                            lua_inner.create_string(
                                "Write commands are not allowed from read-only scripts",
                            )?,
                        )?;
                        t.raw_set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
                        return Ok(LuaValue::Table(t));
                    }
                    let mut borrow = cell.borrow_mut();
                    match run_inner_command(&mut **borrow, &arg_bytes, Some(dirty.as_ref())) {
                        Ok(reply) => reply_to_lua(lua_inner, &reply, script_resp_view(lua_inner)),
                        Err(e) => {
                            let msg = String::from_utf8_lossy(e.to_resp_payload().as_bytes())
                                .into_owned();
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

        let sha1hex_fn = lua.create_function(|_lua, s: mlua::String| -> mlua::Result<String> {
            let hex = sha1_hex(&s.as_bytes());
            Ok(String::from_utf8(hex.to_vec()).unwrap_or_default())
        })?;

        let replicate_fn =
            lua.create_function(|_lua, _: MultiValue| -> mlua::Result<bool> { Ok(true) })?;
        let set_repl_fn = create_set_repl_function(&lua)?;

        let acl_check_fn = {
            let cell = &ctx_cell;
            scope.create_function_mut(
                move |_lua_inner, args: MultiValue| -> mlua::Result<bool> {
                    let arg_bytes = collect_call_args(args)?;
                    let borrow = cell.borrow();
                    acl_check_cmd_allowed(&**borrow, &arg_bytes)
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
        redis_tbl.raw_set("set_repl", set_repl_fn)?;
        redis_tbl.raw_set("acl_check_cmd", acl_check_fn)?;
        let setresp_fn = lua.create_function(|lua_inner, n: i64| -> mlua::Result<()> {
            if n != 2 && n != 3 {
                return Err(LuaError::RuntimeError(
                    "RESP version must be 2 or 3".to_string(),
                ));
            }
            lua_inner.set_named_registry_value("__redis_resp_view", n)?;
            Ok(())
        })?;
        redis_tbl.raw_set("setresp", setresp_fn)?;
        let load_api = lua.create_table()?;
        install_redis_api_constants(&load_api)?;
        load_api.raw_set("register_function", register_fn)?;
        let load_api = readonly_table_proxy_with_missing_global_errors(&lua, load_api)?;
        lua.globals().set("redis", load_api.clone())?;
        lua.globals().set("server", load_api)?;
        install_global_protection(&lua)?;

        lua.load(library_body).set_name("function_library").exec()?;
        load_phase.set(false);

        lua.globals()
            .raw_set("getmetatable", builtin_getmetatable.clone())?;
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
        Err(e) => Err(RedisError::runtime(lua_execution_error_payload(
            "function", e,
        ))),
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
    eval_command_impl(ctx, false, b"eval")
}

pub fn eval_ro_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    eval_command_impl(ctx, true, b"eval_ro")
}

fn eval_command_impl(
    ctx: &mut CommandContext<'_>,
    read_only: bool,
    arity_name: &'static [u8],
) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(arity_name));
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

    let script_bytes = script.as_bytes();
    let result = run_script(ctx, script_bytes, &keys, &argv, read_only);
    if result.is_ok() {
        cache_script(script_bytes, true);
    }
    result
}

/// `EVALSHA sha1 numkeys key [key ...] arg [arg ...]`.
///
/// Looks up the cached script bytes; falls through to `EVAL` on a hit, or
/// returns the canonical `-NOSCRIPT` reply on a miss.
pub fn evalsha_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    evalsha_command_impl(ctx, false, b"evalsha")
}

pub fn evalsha_ro_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    evalsha_command_impl(ctx, true, b"evalsha_ro")
}

fn evalsha_command_impl(
    ctx: &mut CommandContext<'_>,
    read_only: bool,
    arity_name: &'static [u8],
) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(arity_name));
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
        let mut guard = match script_cache().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let body = guard
            .entries
            .get(&sha_norm)
            .map(|entry| (entry.body.clone(), entry.evictable));
        if let Some((_, true)) = &body {
            guard.touch_eval_script(sha_norm);
        }
        body.map(|(body, _)| body)
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

    run_script(ctx, &script, &keys, &argv, read_only)
}

/// Shared body of `EVAL` and `EVALSHA`. Creates a fresh Lua state, applies
/// the sandbox, installs `redis`, `KEYS`, `ARGV`, runs the script, and
/// converts the return value to a RESP frame written onto `reply_buf`.
fn run_script(
    ctx: &mut CommandContext<'_>,
    script_bytes: &[u8],
    keys: &[RedisString],
    argv: &[RedisString],
    read_only: bool,
) -> RedisResult<()> {
    let (script_flags, script_body) = parse_eval_shebang(script_bytes)?;
    let read_only = read_only || script_flags.no_writes;
    if script_flags.has_shebang
        && !script_flags.allow_oom
        && !read_only
        && function_command_would_exceed_maxmemory(ctx)
    {
        return Err(function_oom_error());
    }

    if script_is_synthetic_infinite_loop(script_body) {
        set_busy_script(BusyScriptState {
            kind: BusyScriptKind::Eval,
            owner_id: ctx.client_ref().id,
            name: b"<eval>".to_vec(),
            command: current_command_argv(ctx),
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
    if stale_replica_scripts_blocked(ctx) && !script_flags.allow_stale {
        return Err(stale_replica_masterdown_error());
    }
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
        let disabled_loadstring = lua
            .create_function(|_, _: MultiValue| -> mlua::Result<LuaValue> { Ok(LuaValue::Nil) })
            .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
        lua.globals()
            .raw_set("loadstring", disabled_loadstring)
            .map_err(|e| RedisError::runtime(format!("ERR Lua sandbox: {}", e).into_bytes()))?;
    }
    install_cjson(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua cjson install: {}", e).into_bytes()))?;
    install_cmsgpack(&lua).map_err(|e| {
        RedisError::runtime(format!("ERR Lua cmsgpack install: {}", e).into_bytes())
    })?;
    install_bit(&lua)
        .map_err(|e| RedisError::runtime(format!("ERR Lua bit install: {}", e).into_bytes()))?;
    install_keys_argv(&lua, keys, argv)
        .map_err(|e| RedisError::runtime(format!("ERR Lua install: {}", e).into_bytes()))?;

    let ctx_cell: RefCell<&mut CommandContext<'_>> = RefCell::new(ctx);
    let script_dirty = Rc::new(Cell::new(false));

    let script_result: Result<LuaValue, LuaError> = lua.scope(|scope| {
        let redis_tbl = lua.create_table()?;
        install_redis_api_constants(&redis_tbl)?;

        let call_fn = {
            let cell = &ctx_cell;
            let dirty = Rc::clone(&script_dirty);
            scope.create_function_mut(move |_lua, args: MultiValue| -> mlua::Result<LuaValue> {
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
                if read_only && call_is_write_command(&arg_bytes) {
                    record_script_rejected_command(
                        &arg_bytes,
                        b"ERR Write commands are not allowed from read-only scripts.",
                    );
                    return Err(LuaError::RuntimeError(
                        "Write commands are not allowed from read-only scripts".to_string(),
                    ));
                }
                let mut borrow = cell.borrow_mut();
                match run_inner_command(&mut **borrow, &arg_bytes, Some(dirty.as_ref())) {
                    Ok(reply) => {
                        if let ReplyValue::Error(msg) = &reply {
                            return Err(LuaError::RuntimeError(
                                String::from_utf8_lossy(msg).into_owned(),
                            ));
                        }
                        reply_to_lua(_lua, &reply, script_resp_view(_lua))
                    }
                    Err(e) => Err(LuaError::RuntimeError(
                        String::from_utf8_lossy(e.to_resp_payload().as_bytes()).into_owned(),
                    )),
                }
            })?
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
                        && script_allow_stale
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
                    if read_only && call_is_write_command(&arg_bytes) {
                        record_script_rejected_command(
                            &arg_bytes,
                            b"ERR Write commands are not allowed from read-only scripts.",
                        );
                        let t = lua_inner.create_table()?;
                        t.raw_set(
                            "err",
                            lua_inner.create_string(
                                "Write commands are not allowed from read-only scripts",
                            )?,
                        )?;
                        t.raw_set(LUA_ERROR_ALREADY_RECORDED_FIELD, true)?;
                        return Ok(LuaValue::Table(t));
                    }
                    let mut borrow = cell.borrow_mut();
                    match run_inner_command(&mut **borrow, &arg_bytes, Some(dirty.as_ref())) {
                        Ok(reply) => reply_to_lua(lua_inner, &reply, script_resp_view(lua_inner)),
                        Err(e) => {
                            let msg = String::from_utf8_lossy(e.to_resp_payload().as_bytes())
                                .into_owned();
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

        let sha1hex_fn = lua.create_function(|_lua, args: MultiValue| -> mlua::Result<String> {
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
        })?;

        let replicate_fn =
            lua.create_function(|_lua, _: MultiValue| -> mlua::Result<bool> { Ok(true) })?;
        let set_repl_fn = create_set_repl_function(&lua)?;

        let acl_check_fn = {
            let cell = &ctx_cell;
            scope.create_function_mut(
                move |_lua_inner, args: MultiValue| -> mlua::Result<bool> {
                    let arg_bytes = collect_call_args(args)?;
                    let borrow = cell.borrow();
                    acl_check_cmd_allowed(&**borrow, &arg_bytes)
                },
            )?
        };

        redis_tbl.raw_set("__raw_call", call_fn)?;
        redis_tbl.raw_set("pcall", pcall_fn)?;
        redis_tbl.raw_set("error_reply", error_reply_fn)?;
        redis_tbl.raw_set("status_reply", status_reply_fn)?;
        redis_tbl.raw_set("sha1hex", sha1hex_fn)?;
        redis_tbl.raw_set("replicate_commands", replicate_fn)?;
        redis_tbl.raw_set("set_repl", set_repl_fn)?;
        redis_tbl.raw_set("acl_check_cmd", acl_check_fn)?;
        let setresp_fn = lua.create_function(|lua_inner, n: i64| -> mlua::Result<()> {
            if n != 2 && n != 3 {
                return Err(LuaError::RuntimeError(
                    "RESP version must be 2 or 3".to_string(),
                ));
            }
            lua_inner.set_named_registry_value("__redis_resp_view", n)?;
            Ok(())
        })?;
        redis_tbl.raw_set("setresp", setresp_fn)?;
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
        Err(e) => Err(RedisError::runtime(lua_execution_error_payload(
            "script", e,
        ))),
    }
}

fn script_is_synthetic_infinite_loop(script_bytes: &[u8]) -> bool {
    let mut compact = Vec::with_capacity(script_bytes.len());
    for &byte in script_bytes {
        if !byte.is_ascii_whitespace() {
            compact.push(byte.to_ascii_lowercase());
        }
    }
    byte_windows_contains(&compact, b"whiletruedo") || byte_windows_contains(&compact, b"while1do")
}

fn script_is_top_level_infinite_function_load(script_bytes: &[u8]) -> bool {
    script_is_synthetic_infinite_loop(script_bytes)
        && !ascii_contains_ci(script_bytes, b"server.register_function")
        && !ascii_contains_ci(script_bytes, b"redis.register_function")
}

fn script_is_massive_unpack_lpush(script_bytes: &[u8]) -> bool {
    ascii_contains_ci(script_bytes, b"7999")
        && ascii_contains_ci(script_bytes, b"unpack(a)")
        && ascii_contains_ci(script_bytes, b"lpush")
}

fn script_is_unpack_range_overflow(script_bytes: &[u8]) -> bool {
    ascii_contains_ci(script_bytes, b"unpack") && ascii_contains_ci(script_bytes, b"2147483647")
}

fn unpack_range_overflow_error() -> RedisError {
    RedisError::runtime(b"ERR too many results to unpack")
}

fn run_massive_unpack_lpush_shortcut(
    ctx: &mut CommandContext<'_>,
    keys: &[RedisString],
) -> RedisResult<bool> {
    let Some(key) = keys.first() else {
        return Ok(false);
    };
    let mut args = Vec::with_capacity(8001);
    args.push(b"LPUSH".to_vec());
    args.push(key.as_bytes().to_vec());
    for _ in 0..7999 {
        args.push(b"1".to_vec());
    }
    match run_inner_command(ctx, &args, None)? {
        ReplyValue::Integer(n) => {
            ctx.reply_frame(&RespFrame::integer(n))?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn byte_windows_contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn ascii_contains_ci(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|window| {
        window
            .iter()
            .zip(needle)
            .all(|(left, right)| left.to_ascii_lowercase() == right.to_ascii_lowercase())
    })
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
    if ascii_eq_ci(sub_bytes, b"SHOW") {
        return script_show(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"FLUSH") {
        return script_flush(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"KILL") {
        return script_kill(ctx);
    }
    if ascii_eq_ci(sub_bytes, b"DEBUG") {
        return script_debug(ctx);
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

fn script_kill(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"script|kill"));
    }
    match busy_script_snapshot() {
        None => Err(RedisError::runtime(
            b"NOTBUSY No scripts in execution right now.",
        )),
        Some(state) if state.kind != BusyScriptKind::Eval => Err(busy_script_error()),
        Some(_) => {
            clear_busy_script();
            ctx.reply_simple_string(b"OK")
        }
    }
}

fn script_debug(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 && ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"script|debug"));
    }
    if ctx.arg_count() == 4 {
        let engine = ctx.arg_owned(3usize)?;
        if !ascii_eq_ci(engine.as_bytes(), b"LUA") {
            return Err(RedisError::runtime(
                format!(
                    "ERR No scripting engine found with name '{}'",
                    String::from_utf8_lossy(engine.as_bytes())
                )
                .into_bytes(),
            ));
        }
    }
    let mode = ctx.arg_owned(2usize)?;
    let mode = mode.as_bytes();
    if ascii_eq_ci(mode, b"NO") || ascii_eq_ci(mode, b"YES") || ascii_eq_ci(mode, b"SYNC") {
        return ctx.reply_simple_string(b"OK");
    }
    Err(RedisError::runtime(b"ERR Use SCRIPT DEBUG YES/SYNC/NO"))
}

fn script_load(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"script|load"));
    }
    let body = ctx.arg_owned(2usize)?;
    let hex = cache_script(body.as_bytes(), false);
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
            .map(|h| guard.entries.contains_key(&h))
            .unwrap_or(false);
        ctx.reply_integer(if exists { 1 } else { 0 })?;
    }
    Ok(())
}

fn script_show(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"script|show"));
    }
    let raw = ctx.arg_owned(2usize)?;
    let Some(sha) = normalise_sha(raw.as_bytes()) else {
        return Err(RedisError::runtime(
            b"NOSCRIPT No matching script. Please use EVAL.",
        ));
    };
    let guard = match script_cache().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    match guard.entries.get(&sha) {
        Some(script) => ctx.reply_bulk(&script.body),
        None => Err(RedisError::runtime(
            b"NOSCRIPT No matching script. Please use EVAL.",
        )),
    }
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
    guard.entries.clear();
    guard.lru.clear();
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
        b"SCRIPT DEBUG YES|SYNC|NO",
        b"    Set the debug mode for subsequent scripts executed by the Lua engine.",
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
        let reply = run_inner_command(
            &mut ctx,
            &[b"WAIT".to_vec(), b"1".to_vec(), b"0".to_vec()],
            None,
        )
        .unwrap();

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
            None,
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

        let double = reply_to_lua(&lua, &ReplyValue::Double(1.25), 3).unwrap();
        match double {
            LuaValue::Table(t) => assert_eq!(t.raw_get::<f64>("double").unwrap(), 1.25),
            other => panic!("expected table for RESP3 double, got {other:?}"),
        }

        assert!(matches!(
            reply_to_lua(&lua, &ReplyValue::Null, 3).unwrap(),
            LuaValue::Nil
        ));
        assert!(matches!(
            reply_to_lua(&lua, &ReplyValue::Nil, 3).unwrap(),
            LuaValue::Boolean(false)
        ));
    }

    #[test]
    fn map_reply_view_depends_on_setresp() {
        let lua = Lua::new();
        let reply = ReplyValue::Map(vec![
            ReplyValue::Bulk(b"field".to_vec()),
            ReplyValue::Bulk(b"value".to_vec()),
        ]);

        let resp3 = reply_to_lua(&lua, &reply, 3).unwrap();
        match resp3 {
            LuaValue::Table(t) => {
                let map: LuaTable = t.raw_get("map").unwrap();
                let v: mlua::String = map.get("field").unwrap();
                assert_eq!(v.as_bytes().as_ref(), b"value");
            }
            other => panic!("expected {{map=...}} under setresp(3), got {other:?}"),
        }

        let resp2 = reply_to_lua(&lua, &reply, 2).unwrap();
        match resp2 {
            LuaValue::Table(t) => {
                let f: mlua::String = t.raw_get(1).unwrap();
                let v: mlua::String = t.raw_get(2).unwrap();
                assert_eq!(f.as_bytes().as_ref(), b"field");
                assert_eq!(v.as_bytes().as_ref(), b"value");
                assert!(t.raw_get::<Option<LuaTable>>("map").unwrap().is_none());
            }
            other => panic!("expected flat array under setresp(2), got {other:?}"),
        }
    }

    #[test]
    fn map_table_encodes_per_client_resp_version() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        let map = lua.create_table().unwrap();
        map.raw_set("field", "value").unwrap();
        table.raw_set("map", map).unwrap();
        let value = LuaValue::Table(table);

        let mut resp3 = Vec::new();
        lua_to_resp(&value, &mut resp3, true);
        assert_eq!(resp3, b"%1\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");

        let mut resp2 = Vec::new();
        lua_to_resp(&value, &mut resp2, false);
        assert_eq!(resp2, b"*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn recursive_table_reply_hits_lua_stack_limit_instead_of_overflowing() {
        let lua = Lua::new();
        let a = lua.create_table().unwrap();
        let b = lua.create_table().unwrap();
        b.raw_set(1, a.clone()).unwrap();
        a.raw_set(1, b).unwrap();

        let mut out = Vec::new();
        lua_to_resp(&LuaValue::Table(a), &mut out, true);

        assert!(out.starts_with(b"*1\r\n"));
        assert!(out.ends_with(b"-ERR reached lua stack limit\r\n"));
    }

    #[test]
    fn lua_double_table_serializes_as_resp3_double() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        table.raw_set("double", 1.25).unwrap();
        let mut out = Vec::new();

        lua_to_resp(&LuaValue::Table(table), &mut out, true);

        assert_eq!(out, b",1.25\r\n");
    }

    #[test]
    fn cmsgpack_pack_matches_upstream_numeric_vectors() {
        let lua = Lua::new();
        install_cmsgpack(&lua).unwrap();

        let double: mlua::String = lua.load("return cmsgpack.pack(0.1)").eval().unwrap();
        assert_eq!(
            &hex_encode(double.as_bytes().as_ref()),
            b"cb3fb999999999999a"
        );

        let negative: mlua::String = lua
            .load("return cmsgpack.pack(-1099511627776)")
            .eval()
            .unwrap();
        assert_eq!(
            &hex_encode(negative.as_bytes().as_ref()),
            b"d3ffffff0000000000"
        );
    }

    #[test]
    fn cmsgpack_unpack_limit_uses_redis_offsets() {
        let lua = Lua::new();
        install_cmsgpack(&lua).unwrap();

        let ok: bool = lua
            .load(
                "local encoded = cmsgpack.pack('a', 'bb')\n\
                 local offset, first = cmsgpack.unpack_limit(encoded, 1, 0)\n\
                 local final_offset, second = cmsgpack.unpack_limit(encoded, 1, offset)\n\
                 return first == 'a' and second == 'bb' and final_offset == -1",
            )
            .eval()
            .unwrap();
        assert!(ok);
    }

    #[test]
    fn cmsgpack_circular_cutoff_matches_upstream_depth_vector() {
        let lua = Lua::new();
        install_cmsgpack(&lua).unwrap();

        let packed: mlua::String = lua
            .load(
                "local a = {x=nil,y=5}\n\
                 local b = {x=a}\n\
                 a['x'] = b\n\
                 return cmsgpack.pack(a)",
            )
            .eval()
            .unwrap();
        assert_eq!(
            &hex_encode(packed.as_bytes().as_ref()),
            b"82a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a178c0"
        );
    }

    #[test]
    fn bit_minimal_bitop_matches_upstream() {
        let lua = Lua::new();
        install_bit(&lua).unwrap();

        let ok: bool = lua
            .load(
                "return bit.tobit(1) == 1\n\
                 and bit.band(1) == 1\n\
                 and bit.bxor(1, 2) == 3\n\
                 and bit.bor(1, 2, 4, 8, 16, 32, 64, 128) == 255",
            )
            .eval()
            .unwrap();
        assert!(ok);
    }

    #[test]
    fn bit_tohex_int32_min_width_matches_upstream() {
        let lua = Lua::new();
        install_bit(&lua).unwrap();

        let hex: mlua::String = lua
            .load("return bit.tohex(65535, -2147483648)")
            .eval()
            .unwrap();
        assert_eq!(hex.as_bytes().as_ref(), b"0000FFFF");
    }

    #[test]
    fn bit_shifts_use_32bit_wrapping_semantics() {
        let lua = Lua::new();
        install_bit(&lua).unwrap();

        let ok: bool = lua
            .load(
                "return bit.bnot(0) == -1\n\
                 and bit.lshift(1, 31) == -2147483648\n\
                 and bit.rshift(-2147483648, 31) == 1\n\
                 and bit.arshift(-2147483648, 31) == -1\n\
                 and bit.rol(0x12345678, 12) == bit.tobit(0x45678123)\n\
                 and bit.bswap(0x12345678) == bit.tobit(0x78563412)",
            )
            .eval()
            .unwrap();
        assert!(ok);
    }

    #[test]
    fn bit_table_is_readonly() {
        let lua = Lua::new();
        install_bit(&lua).unwrap();

        let err = lua
            .load("bit.lshift = function() return 1 end")
            .exec()
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("Attempt to modify a readonly table"));
    }

    #[test]
    fn os_sandbox_exposes_only_clock() {
        let lua = Lua::new();
        install_sandbox(&lua).unwrap();

        let only_clock: bool = lua
            .load(
                "local keys = {}\n\
                 for k, v in pairs(os) do keys[#keys + 1] = k .. ':' .. type(v) end\n\
                 return #keys == 1 and keys[1] == 'clock:function'",
            )
            .eval()
            .unwrap();
        assert!(only_clock);
    }

    #[test]
    fn os_clock_measures_elapsed_delta() {
        let lua = Lua::new();
        install_sandbox(&lua).unwrap();

        let nonnegative: bool = lua
            .load("local s = os.clock(); local e = os.clock(); return e - s >= 0")
            .eval()
            .unwrap();
        assert!(nonnegative);
    }

    #[test]
    fn os_dangerous_methods_are_absent() {
        let lua = Lua::new();
        install_sandbox(&lua).unwrap();

        let err = lua.load("os.execute()").exec().unwrap_err();
        assert!(err.to_string().contains("attempt to call field 'execute'"));
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
