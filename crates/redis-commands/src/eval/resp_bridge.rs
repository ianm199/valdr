//! RESP/Lua bridge helpers for EVAL and FCALL command re-entry.
//!
//! Inner `redis.call` dispatches write RESP bytes into the client reply buffer.
//! This module parses those bytes into a small reply tree, exposes that tree to
//! Lua with Redis-compatible script semantics, and encodes Lua return values
//! back to RESP for the outer command reply.

use mlua::{Lua, Table as LuaTable, Value as LuaValue};
use redis_core::metrics::record_error_reply;
use redis_protocol::parser::{ParserCallbacks, ParserCursor};
use redis_types::RedisError;

/// One captured reply from a `redis.call` re-entry.
/// Parsed from the RESP bytes the inner dispatch wrote into `reply_buf`.
/// Used as an intermediate before the value is converted to a Lua value.
#[derive(Debug, Clone)]
pub(super) enum ReplyValue {
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

pub(super) fn parse_reply_value(raw_reply: &[u8]) -> Result<ReplyValue, RedisError> {
    let mut cursor = ParserCursor::new(raw_reply);
    let mut builder = ReplyBuilder::new();
    if cursor.parse_next(&mut builder).is_err() || builder.errored {
        return Err(RedisError::runtime(b"ERR could not parse inner reply"));
    }
    builder
        .out
        .ok_or_else(|| RedisError::runtime(b"ERR empty inner reply"))
}

/// The RESP version the running script asked `redis.call` to surface, set by
/// `redis.setresp(n)` and stored in the Lua registry (default 2, as upstream).
/// Controls whether map/set replies reach the script as RESP3 `{map=...}` /
/// `{set=...}` tables or as flat RESP2 arrays.
pub(super) fn script_resp_view(lua: &Lua) -> u8 {
    lua.named_registry_value::<i64>("__redis_resp_view")
        .map(|n| n as u8)
        .unwrap_or(2)
}

/// Convert a [`ReplyValue`] tree to a Lua value following Redis Lua semantics:
/// bulk and simple strings become Lua strings, integers become Lua integers,
/// nil becomes Lua nil, errors become `{err = msg}`, and arrays become
/// 1-indexed Lua tables.
pub(super) fn reply_to_lua(lua: &Lua, value: &ReplyValue, resp_view: u8) -> mlua::Result<LuaValue> {
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
/// Mirrors Redis script-to-protocol conversion: nil -> null bulk, integers /
/// numbers -> integer (numbers truncated), strings -> bulk, booleans -> `:1` /
/// null, tables -> status if `.ok`, error if `.err`, otherwise a 1-indexed
/// array (terminated at the first nil per Lua-array convention).
pub(super) const LUA_REPLY_MAX_DEPTH: usize = 200;
pub(super) const LUA_ERROR_ALREADY_RECORDED_FIELD: &str = "__redis_error_already_recorded";

pub(super) fn lua_to_resp(value: &LuaValue, out: &mut Vec<u8>, resp3: bool) {
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
                for (k, v) in map.pairs::<LuaValue, LuaValue>().flatten() {
                    pairs.push((k, v));
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
                for (k, _) in set.pairs::<LuaValue, LuaValue>().flatten() {
                    members.push(k);
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

pub(super) fn lua_error_reply_wire_bytes(bytes: &[u8]) -> Vec<u8> {
    let clean = bytes
        .split(|b| *b == b'\r' || *b == b'\n')
        .next()
        .unwrap_or(bytes);
    let mut out = Vec::with_capacity(clean.len() + 5);
    out.push(b'-');
    if clean.is_empty() {
        out.extend_from_slice(b"ERR");
    } else {
        out.extend_from_slice(clean);
    }
    out
}
