//! Redis-injected `cjson`, `cmsgpack`, and `bit` libraries for the lua-rs
//! (omnilua) EVAL backend.
//!
//! These mirror the mlua-typed implementations in `lua_cjson.rs`,
//! `lua_cmsgpack.rs`, and `lua_bit.rs` byte-for-byte where the upstream Tcl
//! scripting suite checks behavior. The pure, engine-agnostic logic (bit folds,
//! JSON number/string formatting, MessagePack scalar byte layout) is shared
//! from those modules; only the Lua-binding glue is rebound onto
//! `lua_rs_runtime` here.
//!
//! Two lua-rs API limitations shape the bindings:
//!
//! * `lua_rs_runtime::Table` exposes only `get`/`set`/`len` — there is no public
//!   `pairs`/`raw_get`/`raw_set` or metatable setter. Table iteration is done by
//!   driving the base-library `next` function captured as a
//!   [`Function`](lua_rs_runtime::Function); readonly proxies are built with a
//!   Lua-side `setmetatable` helper. This is tracked upstream as
//!   LUA-RS-REDIS-005 (public table iteration) / LUA-RS-REDIS-001 (readonly
//!   table semantics).
//! * Both libs are installed *before* `install_eval_global_protection_lua_rs`
//!   locks `_G`, matching the mlua install ordering.

use std::cell::RefCell;
use std::rc::Rc;

use lua_rs_runtime::{
    Error as LuaRsErr, Function, Lua, LuaError, LuaString, Table as LuaTable, Value as LuaValue,
    Variadic,
};

use super::lua_bit::{bit_barg, bit_bret, bit_fold_values, bit_tohex};
use super::lua_cjson::CjsonConfig;
use super::lua_cmsgpack::{
    cmsgpack_encode_array_len, cmsgpack_encode_bytes, cmsgpack_encode_double, cmsgpack_encode_int,
    cmsgpack_encode_map_len, cmsgpack_number_to_i64, CMSGPACK_MAX_NESTING,
};

fn rt_error(message: impl Into<String>) -> LuaRsErr {
    let message = message.into();
    LuaError::runtime(format_args!("{message}")).into()
}

/// Install `cjson`, `cmsgpack`, `cmsgpack_safe`, and `bit` as global tables on
/// the lua-rs runtime. Call before locking `_G`.
pub(super) fn install_redis_libs_lua_rs(lua: &Lua) -> lua_rs_runtime::Result<()> {
    install_bit_lua_rs(lua)?;
    install_cjson_lua_rs(lua)?;
    install_cmsgpack_lua_rs(lua)?;
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────
// Shared lua-rs helpers
// ──────────────────────────────────────────────────────────────────────────

/// Wrap `table` in a readonly proxy whose writes raise
/// "Attempt to modify a readonly table", using a Lua-side `setmetatable`
/// (the lua-rs `Table` has no Rust-side metatable setter). Matches
/// `lua_sandbox::readonly_table_proxy` for the mlua backend.
fn readonly_table_proxy_lua_rs(lua: &Lua, table: LuaTable) -> lua_rs_runtime::Result<LuaTable> {
    let builder: Function = lua
        .load(
            r#"
            return function(inner)
                local proxy = {}
                setmetatable(proxy, {
                    __index = inner,
                    __newindex = function()
                        error("Attempt to modify a readonly table")
                    end,
                    __metatable = false
                })
                return proxy
            end
            "#,
        )
        .set_name("readonly_table_proxy_lua_rs")
        .eval::<Function>()?;
    builder.call::<_, LuaTable>(table)
}

/// Capture the base-library `next` function so table iteration works even after
/// `_G` is later locked.
fn capture_next(lua: &Lua) -> lua_rs_runtime::Result<Function> {
    lua.globals().get::<_, Function>("next")
}

/// Iterate every key/value pair of `table` by driving `next`, mirroring Lua's
/// generic `pairs` traversal. Used because lua-rs has no public `pairs`.
fn for_each_pair(
    next_fn: &Function,
    table: &LuaTable,
    mut visit: impl FnMut(LuaValue, LuaValue) -> lua_rs_runtime::Result<()>,
) -> lua_rs_runtime::Result<()> {
    let mut key = LuaValue::Nil;
    loop {
        let (next_key, next_value): (LuaValue, LuaValue) =
            next_fn.call((table.clone(), key.clone()))?;
        if matches!(next_key, LuaValue::Nil) {
            break;
        }
        visit(next_key.clone(), next_value)?;
        key = next_key;
    }
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────
// bit
// ──────────────────────────────────────────────────────────────────────────

fn bit_fold_lua_rs(args: Variadic<f64>, op: impl Fn(u32, u32) -> u32) -> lua_rs_runtime::Result<f64> {
    bit_fold_values(args, op)
        .ok_or_else(|| rt_error("bad argument #1 to bitop (number expected, got no value)"))
}

fn install_bit_lua_rs(lua: &Lua) -> lua_rs_runtime::Result<()> {
    let table = lua.create_table()?;

    table.set(
        "tobit",
        lua.create_function(|_, n: f64| Ok(bit_bret(bit_barg(n))))?,
    )?;
    table.set(
        "bnot",
        lua.create_function(|_, n: f64| Ok(bit_bret(!bit_barg(n))))?,
    )?;
    table.set(
        "band",
        lua.create_function(|_, args: Variadic<f64>| bit_fold_lua_rs(args, |a, b| a & b))?,
    )?;
    table.set(
        "bor",
        lua.create_function(|_, args: Variadic<f64>| bit_fold_lua_rs(args, |a, b| a | b))?,
    )?;
    table.set(
        "bxor",
        lua.create_function(|_, args: Variadic<f64>| bit_fold_lua_rs(args, |a, b| a ^ b))?,
    )?;
    table.set(
        "lshift",
        lua.create_function(|_, (b, n): (f64, f64)| {
            Ok(bit_bret(bit_barg(b).wrapping_shl(bit_barg(n) & 31)))
        })?,
    )?;
    table.set(
        "rshift",
        lua.create_function(|_, (b, n): (f64, f64)| {
            Ok(bit_bret(bit_barg(b).wrapping_shr(bit_barg(n) & 31)))
        })?,
    )?;
    table.set(
        "arshift",
        lua.create_function(|_, (b, n): (f64, f64)| {
            let shifted = (bit_barg(b) as i32).wrapping_shr(bit_barg(n) & 31);
            Ok(bit_bret(shifted as u32))
        })?,
    )?;
    table.set(
        "rol",
        lua.create_function(|_, (b, n): (f64, f64)| {
            Ok(bit_bret(bit_barg(b).rotate_left(bit_barg(n) & 31)))
        })?,
    )?;
    table.set(
        "ror",
        lua.create_function(|_, (b, n): (f64, f64)| {
            Ok(bit_bret(bit_barg(b).rotate_right(bit_barg(n) & 31)))
        })?,
    )?;
    table.set(
        "bswap",
        lua.create_function(|_, n: f64| Ok(bit_bret(bit_barg(n).swap_bytes())))?,
    )?;
    table.set(
        "tohex",
        lua.create_function(|_, (x, n): (f64, Option<f64>)| Ok(bit_tohex(x, n)))?,
    )?;

    table.set("_NAME", "bit")?;
    table.set("_VERSION", "Lua BitOp 1.0.2")?;

    let proxy = readonly_table_proxy_lua_rs(lua, table)?;
    lua.globals().set("bit", proxy)
}

// ──────────────────────────────────────────────────────────────────────────
// cjson
// ──────────────────────────────────────────────────────────────────────────

fn cjson_null_value() -> LuaValue {
    LuaValue::LightUserData(std::ptr::null_mut())
}

fn is_cjson_null(value: &LuaValue) -> bool {
    matches!(value, LuaValue::LightUserData(ptr) if ptr.is_null())
}

fn lua_rs_value_to_json_string(
    next_fn: &Function,
    value: LuaValue,
    cfg: &CjsonConfig,
    depth: usize,
) -> lua_rs_runtime::Result<String> {
    if is_cjson_null(&value) {
        return Ok("null".to_string());
    }
    match value {
        LuaValue::Nil => Ok("null".to_string()),
        LuaValue::Boolean(v) => Ok(if v { "true" } else { "false" }.to_string()),
        LuaValue::Integer(n) => Ok(n.to_string()),
        LuaValue::Number(n) => {
            super::lua_cjson::encode_json_number(n, cfg.encode_invalid_numbers).map_err(rt_error)
        }
        LuaValue::String(s) => {
            super::lua_cjson::json_escape_string(&s.as_bytes()?).map_err(rt_error)
        }
        LuaValue::Table(t) => lua_rs_table_to_json_string(next_fn, t, cfg, depth + 1),
        LuaValue::LightUserData(ptr) if ptr.is_null() => Ok("null".to_string()),
        _ => Err(rt_error("Cannot serialise value: unsupported Lua type")),
    }
}

fn lua_rs_table_to_json_string(
    next_fn: &Function,
    table: LuaTable,
    cfg: &CjsonConfig,
    depth: usize,
) -> lua_rs_runtime::Result<String> {
    if depth > cfg.encode_max_depth {
        return Err(rt_error("Cannot serialise, excessive nesting"));
    }

    let mut entries: Vec<(LuaValue, LuaValue)> = Vec::new();
    for_each_pair(next_fn, &table, |key, value| {
        entries.push((key, value));
        Ok(())
    })?;

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
                out.push_str(&lua_rs_value_to_json_string(next_fn, value, cfg, depth)?);
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
            LuaValue::String(s) => {
                super::lua_cjson::json_escape_string(&s.as_bytes()?).map_err(rt_error)?
            }
            LuaValue::Integer(i) => serde_json::to_string(&i.to_string())
                .map_err(|err| rt_error(err.to_string()))?,
            LuaValue::Number(n)
                if n.is_finite()
                    && n.fract() == 0.0
                    && n >= i64::MIN as f64
                    && n <= i64::MAX as f64 =>
            {
                serde_json::to_string(&(n as i64).to_string())
                    .map_err(|err| rt_error(err.to_string()))?
            }
            _ => {
                return Err(rt_error(
                    "Cannot serialise table: table key must be a number or string",
                ))
            }
        };
        out.push_str(&key_string);
        out.push(':');
        out.push_str(&lua_rs_value_to_json_string(next_fn, value, cfg, depth)?);
    }
    out.push('}');
    Ok(out)
}

fn json_value_to_lua_rs(
    lua: &Lua,
    value: &serde_json::Value,
    max_depth: usize,
    depth: usize,
) -> lua_rs_runtime::Result<LuaValue> {
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
                Err(rt_error("Cannot deserialise number"))
            }
        }
        serde_json::Value::String(s) => Ok(LuaValue::String(lua.create_string(s.as_bytes())?)),
        serde_json::Value::Array(items) => {
            if depth + 1 > max_depth {
                return Err(rt_error("Found too many nested data structures"));
            }
            let table = lua.create_table()?;
            for (idx, item) in items.iter().enumerate() {
                table.set(idx as i64 + 1, json_value_to_lua_rs(lua, item, max_depth, depth + 1)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        serde_json::Value::Object(map) => {
            if depth + 1 > max_depth {
                return Err(rt_error("Found too many nested data structures"));
            }
            let table = lua.create_table()?;
            for (key, item) in map {
                table.set(key.as_str(), json_value_to_lua_rs(lua, item, max_depth, depth + 1)?)?;
            }
            Ok(LuaValue::Table(table))
        }
    }
}

fn cjson_bool_config_lua_rs<F>(
    args: Variadic<LuaValue>,
    cfg: &Rc<RefCell<CjsonConfig>>,
    get: F,
) -> lua_rs_runtime::Result<bool>
where
    F: Fn(&mut CjsonConfig) -> &mut bool,
{
    let mut guard = cfg.borrow_mut();
    let slot = get(&mut guard);
    let old = *slot;
    if args.is_empty() {
        return Ok(old);
    }
    if args.len() != 1 {
        return Err(rt_error("expected 1 argument"));
    }
    *slot = match args.first() {
        Some(LuaValue::Boolean(v)) => *v,
        Some(LuaValue::Integer(v)) => *v != 0,
        Some(LuaValue::Number(v)) => *v != 0.0,
        _ => return Err(rt_error("expected boolean argument")),
    };
    Ok(old)
}

fn cjson_i64_config_lua_rs<F>(
    args: Variadic<LuaValue>,
    cfg: &Rc<RefCell<CjsonConfig>>,
    get: F,
) -> lua_rs_runtime::Result<i64>
where
    F: Fn(&mut CjsonConfig) -> &mut i64,
{
    let mut guard = cfg.borrow_mut();
    let slot = get(&mut guard);
    let old = *slot;
    if args.is_empty() {
        return Ok(old);
    }
    if args.len() != 1 {
        return Err(rt_error("expected 1 argument"));
    }
    let next = match args.first() {
        Some(LuaValue::Integer(v)) => *v,
        Some(LuaValue::Number(v)) if v.is_finite() => *v as i64,
        _ => return Err(rt_error("expected integer argument")),
    };
    if next <= 0 {
        return Err(rt_error("expected positive integer argument"));
    }
    *slot = next;
    Ok(old)
}

fn cjson_depth_config_lua_rs<F>(
    args: Variadic<LuaValue>,
    cfg: &Rc<RefCell<CjsonConfig>>,
    get: F,
) -> lua_rs_runtime::Result<i64>
where
    F: Fn(&mut CjsonConfig) -> &mut usize,
{
    let mut guard = cfg.borrow_mut();
    let slot = get(&mut guard);
    let old = *slot as i64;
    if args.is_empty() {
        return Ok(old);
    }
    if args.len() != 1 {
        return Err(rt_error("expected 1 argument"));
    }
    let next = match args.first() {
        Some(LuaValue::Integer(v)) => *v,
        Some(LuaValue::Number(v)) if v.is_finite() => *v as i64,
        _ => return Err(rt_error("expected integer argument")),
    };
    if next <= 0 {
        return Err(rt_error("expected positive integer argument"));
    }
    *slot = next as usize;
    Ok(old)
}

fn create_cjson_table_lua_rs(
    lua: &Lua,
    cfg: Rc<RefCell<CjsonConfig>>,
) -> lua_rs_runtime::Result<LuaTable> {
    let table = lua.create_table()?;

    let encode_cfg = Rc::clone(&cfg);
    table.set(
        "encode",
        lua.create_function(move |lua, value: LuaValue| {
            let next_fn = capture_next(lua)?;
            let cfg = encode_cfg.borrow();
            lua_rs_value_to_json_string(&next_fn, value, &cfg, 0)
        })?,
    )?;

    let decode_cfg = Rc::clone(&cfg);
    table.set(
        "decode",
        lua.create_function(move |lua, input: LuaString| {
            let bytes = input.as_bytes()?;
            let parsed: serde_json::Value = serde_json::from_slice(&bytes).map_err(|err| {
                rt_error(format!("Expected value but found invalid JSON: {}", err))
            })?;
            let max_depth = decode_cfg.borrow().decode_max_depth;
            json_value_to_lua_rs(lua, &parsed, max_depth, 0)
        })?,
    )?;

    let keep_cfg = Rc::clone(&cfg);
    table.set(
        "encode_keep_buffer",
        lua.create_function(move |_, args: Variadic<LuaValue>| {
            cjson_bool_config_lua_rs(args, &keep_cfg, |cfg| &mut cfg.encode_keep_buffer)
        })?,
    )?;

    let enc_depth_cfg = Rc::clone(&cfg);
    table.set(
        "encode_max_depth",
        lua.create_function(move |_, args: Variadic<LuaValue>| {
            cjson_depth_config_lua_rs(args, &enc_depth_cfg, |cfg| &mut cfg.encode_max_depth)
        })?,
    )?;

    let dec_depth_cfg = Rc::clone(&cfg);
    table.set(
        "decode_max_depth",
        lua.create_function(move |_, args: Variadic<LuaValue>| {
            cjson_depth_config_lua_rs(args, &dec_depth_cfg, |cfg| &mut cfg.decode_max_depth)
        })?,
    )?;

    let invalid_cfg = Rc::clone(&cfg);
    table.set(
        "encode_invalid_numbers",
        lua.create_function(move |_, args: Variadic<LuaValue>| {
            cjson_bool_config_lua_rs(args, &invalid_cfg, |cfg| &mut cfg.encode_invalid_numbers)
        })?,
    )?;

    let dec_invalid_cfg = Rc::clone(&cfg);
    table.set(
        "decode_invalid_numbers",
        lua.create_function(move |_, args: Variadic<LuaValue>| {
            cjson_bool_config_lua_rs(args, &dec_invalid_cfg, |cfg| &mut cfg.decode_invalid_numbers)
        })?,
    )?;

    let precision_cfg = Rc::clone(&cfg);
    table.set(
        "encode_number_precision",
        lua.create_function(move |_, args: Variadic<LuaValue>| {
            cjson_i64_config_lua_rs(args, &precision_cfg, |cfg| &mut cfg.encode_number_precision)
        })?,
    )?;

    table.set(
        "encode_sparse_array",
        lua.create_function(|_, _args: Variadic<LuaValue>| Ok(false))?,
    )?;
    table.set(
        "new",
        lua.create_function(|lua, _args: Variadic<LuaValue>| {
            create_cjson_table_lua_rs(lua, Rc::new(RefCell::new(CjsonConfig::default())))
        })?,
    )?;
    table.set("null", cjson_null_value())?;
    table.set("_NAME", "cjson")?;
    table.set("_VERSION", "2.1.0")?;
    Ok(table)
}

fn install_cjson_lua_rs(lua: &Lua) -> lua_rs_runtime::Result<()> {
    let cjson = create_cjson_table_lua_rs(lua, Rc::new(RefCell::new(CjsonConfig::default())))?;
    let proxy = readonly_table_proxy_lua_rs(lua, cjson)?;
    lua.globals().set("cjson", proxy)
}

// ──────────────────────────────────────────────────────────────────────────
// cmsgpack
// ──────────────────────────────────────────────────────────────────────────

fn cmsgpack_pack_lua_rs(
    lua: &Lua,
    next_fn: &Function,
    args: Variadic<LuaValue>,
) -> lua_rs_runtime::Result<LuaValue> {
    if args.is_empty() {
        return Err(rt_error("MessagePack pack needs input."));
    }
    let mut out = Vec::new();
    for value in args {
        cmsgpack_encode_lua_rs_value(lua, next_fn, value, &mut out, 0)?;
    }
    Ok(LuaValue::String(lua.create_string(&out)?))
}

fn cmsgpack_encode_lua_rs_value(
    lua: &Lua,
    next_fn: &Function,
    value: LuaValue,
    out: &mut Vec<u8>,
    level: usize,
) -> lua_rs_runtime::Result<()> {
    match value {
        LuaValue::String(s) => cmsgpack_encode_bytes(&s.as_bytes()?, out).map_err(rt_error),
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
        LuaValue::Table(t) => cmsgpack_encode_lua_rs_table(lua, next_fn, t, out, level),
        _ => {
            out.push(0xc0);
            Ok(())
        }
    }
}

fn cmsgpack_encode_lua_rs_table(
    lua: &Lua,
    next_fn: &Function,
    table: LuaTable,
    out: &mut Vec<u8>,
    level: usize,
) -> lua_rs_runtime::Result<()> {
    if level == CMSGPACK_MAX_NESTING {
        out.push(0xc0);
        return Ok(());
    }

    let mut entries: Vec<(LuaValue, LuaValue)> = Vec::new();
    for_each_pair(next_fn, &table, |key, value| {
        entries.push((key, value));
        Ok(())
    })?;

    if let Some(len) = cmsgpack_table_array_len(&entries) {
        cmsgpack_encode_array_len(len, out).map_err(rt_error)?;
        for index in 1..=len {
            let value: LuaValue = table.get(index as i64)?;
            cmsgpack_encode_lua_rs_value(lua, next_fn, value, out, level + 1)?;
        }
    } else {
        cmsgpack_encode_map_len(entries.len(), out).map_err(rt_error)?;
        for (key, value) in entries {
            cmsgpack_encode_lua_rs_value(lua, next_fn, key, out, level + 1)?;
            cmsgpack_encode_lua_rs_value(lua, next_fn, value, out, level + 1)?;
        }
    }
    Ok(())
}

fn cmsgpack_table_array_len(entries: &[(LuaValue, LuaValue)]) -> Option<usize> {
    let mut count = 0usize;
    let mut max = 0usize;
    for (key, _) in entries {
        let index = cmsgpack_array_index(key)?;
        count += 1;
        max = max.max(index);
    }
    if max == count {
        Some(count)
    } else {
        None
    }
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

fn cmsgpack_string_arg_lua_rs(
    args: &Variadic<LuaValue>,
    index: usize,
    function: &str,
) -> lua_rs_runtime::Result<LuaString> {
    match args.get(index) {
        Some(LuaValue::String(s)) => Ok(s.clone()),
        _ => Err(rt_error(format!(
            "bad argument #{} to cmsgpack.{} (string expected)",
            index + 1,
            function
        ))),
    }
}

fn cmsgpack_required_i64_arg_lua_rs(
    args: &Variadic<LuaValue>,
    index: usize,
    function: &str,
) -> lua_rs_runtime::Result<i64> {
    let Some(value) = args.get(index) else {
        return Err(rt_error(format!(
            "bad argument #{} to cmsgpack.{} (number expected)",
            index + 1,
            function
        )));
    };
    cmsgpack_lua_value_to_i64(value).ok_or_else(|| {
        rt_error(format!(
            "bad argument #{} to cmsgpack.{} (number expected)",
            index + 1,
            function
        ))
    })
}

fn cmsgpack_optional_i64_arg_lua_rs(
    args: &Variadic<LuaValue>,
    index: usize,
    default: i64,
    function: &str,
) -> lua_rs_runtime::Result<i64> {
    match args.get(index) {
        Some(value) => cmsgpack_lua_value_to_i64(value).ok_or_else(|| {
            rt_error(format!(
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

fn cmsgpack_unpack_lua_rs(
    lua: &Lua,
    args: Variadic<LuaValue>,
) -> lua_rs_runtime::Result<Variadic<LuaValue>> {
    let input = cmsgpack_string_arg_lua_rs(&args, 0, "unpack")?;
    cmsgpack_unpack_full_lua_rs(lua, &input.as_bytes()?, 0, 0)
}

fn cmsgpack_unpack_one_lua_rs(
    lua: &Lua,
    args: Variadic<LuaValue>,
) -> lua_rs_runtime::Result<Variadic<LuaValue>> {
    let input = cmsgpack_string_arg_lua_rs(&args, 0, "unpack_one")?;
    let offset = cmsgpack_optional_i64_arg_lua_rs(&args, 1, 0, "unpack_one")?;
    cmsgpack_unpack_full_lua_rs(lua, &input.as_bytes()?, 1, offset)
}

fn cmsgpack_unpack_limit_lua_rs(
    lua: &Lua,
    args: Variadic<LuaValue>,
) -> lua_rs_runtime::Result<Variadic<LuaValue>> {
    let input = cmsgpack_string_arg_lua_rs(&args, 0, "unpack_limit")?;
    let limit = cmsgpack_required_i64_arg_lua_rs(&args, 1, "unpack_limit")?;
    let offset = cmsgpack_optional_i64_arg_lua_rs(&args, 2, 0, "unpack_limit")?;
    cmsgpack_unpack_full_lua_rs(lua, &input.as_bytes()?, limit, offset)
}

fn cmsgpack_unpack_full_lua_rs(
    lua: &Lua,
    input: &[u8],
    limit: i64,
    offset: i64,
) -> lua_rs_runtime::Result<Variadic<LuaValue>> {
    if offset < 0 || limit < 0 {
        return Err(rt_error(format!(
            "Invalid request to unpack with offset of {} and limit of {}.",
            offset,
            input.len()
        )));
    }
    let offset_usize = usize::try_from(offset).map_err(|_| {
        rt_error(format!(
            "Start offset {} greater than input length {}.",
            offset,
            input.len()
        ))
    })?;
    if offset_usize > input.len() {
        return Err(rt_error(format!(
            "Start offset {} greater than input length {}.",
            offset,
            input.len()
        )));
    }

    let decode_all = limit == 0 && offset == 0;
    let effective_limit = if decode_all { i64::MAX } else { limit };
    let mut cursor = CmsgpackCursorLuaRs::new(&input[offset_usize..], offset_usize);
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

    Ok(Variadic::from(results))
}

struct CmsgpackCursorLuaRs<'a> {
    data: &'a [u8],
    pos: usize,
    absolute_pos: usize,
}

impl<'a> CmsgpackCursorLuaRs<'a> {
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

    fn take(&mut self, len: usize) -> lua_rs_runtime::Result<&'a [u8]> {
        if self.remaining() < len {
            return Err(rt_error("Missing bytes in input."));
        }
        let start = self.pos;
        self.pos += len;
        self.absolute_pos += len;
        Ok(&self.data[start..start + len])
    }

    fn read_u8(&mut self) -> lua_rs_runtime::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn read_u16(&mut self) -> lua_rs_runtime::Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> lua_rs_runtime::Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> lua_rs_runtime::Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn decode_lua_value(&mut self, lua: &Lua) -> lua_rs_runtime::Result<LuaValue> {
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
            _ => Err(rt_error("Bad data format in input.")),
        }
    }

    fn decode_string(&mut self, lua: &Lua, len: usize) -> lua_rs_runtime::Result<LuaValue> {
        Ok(LuaValue::String(lua.create_string(self.take(len)?)?))
    }

    fn decode_array(&mut self, lua: &Lua, len: usize) -> lua_rs_runtime::Result<LuaValue> {
        let table = lua.create_table()?;
        for index in 1..=len {
            let value = self.decode_lua_value(lua)?;
            table.set(index as i64, value)?;
        }
        Ok(LuaValue::Table(table))
    }

    fn decode_map(&mut self, lua: &Lua, len: usize) -> lua_rs_runtime::Result<LuaValue> {
        let table = lua.create_table()?;
        for _ in 0..len {
            let key = self.decode_lua_value(lua)?;
            let value = self.decode_lua_value(lua)?;
            table.set(key, value)?;
        }
        Ok(LuaValue::Table(table))
    }
}

fn cmsgpack_safe_result_lua_rs(
    lua: &Lua,
    result: lua_rs_runtime::Result<Variadic<LuaValue>>,
) -> lua_rs_runtime::Result<Variadic<LuaValue>> {
    match result {
        Ok(values) => Ok(values),
        Err(err) => Ok(Variadic::from(vec![
            LuaValue::Nil,
            LuaValue::String(lua.create_string(err.to_string().as_bytes())?),
        ])),
    }
}

fn create_cmsgpack_table_lua_rs(lua: &Lua, safe: bool) -> lua_rs_runtime::Result<LuaTable> {
    let table = lua.create_table()?;

    table.set(
        "pack",
        lua.create_function(move |lua, args: Variadic<LuaValue>| {
            let next_fn = capture_next(lua)?;
            let result = cmsgpack_pack_lua_rs(lua, &next_fn, args).map(|v| Variadic::from(vec![v]));
            if safe {
                cmsgpack_safe_result_lua_rs(lua, result)
            } else {
                result
            }
        })?,
    )?;

    table.set(
        "unpack",
        lua.create_function(move |lua, args: Variadic<LuaValue>| {
            let result = cmsgpack_unpack_lua_rs(lua, args);
            if safe {
                cmsgpack_safe_result_lua_rs(lua, result)
            } else {
                result
            }
        })?,
    )?;

    table.set(
        "unpack_one",
        lua.create_function(move |lua, args: Variadic<LuaValue>| {
            let result = cmsgpack_unpack_one_lua_rs(lua, args);
            if safe {
                cmsgpack_safe_result_lua_rs(lua, result)
            } else {
                result
            }
        })?,
    )?;

    table.set(
        "unpack_limit",
        lua.create_function(move |lua, args: Variadic<LuaValue>| {
            let result = cmsgpack_unpack_limit_lua_rs(lua, args);
            if safe {
                cmsgpack_safe_result_lua_rs(lua, result)
            } else {
                result
            }
        })?,
    )?;

    table.set("_NAME", "cmsgpack")?;
    table.set("_VERSION", "lua-cmsgpack 0.4.0")?;
    table.set("_COPYRIGHT", "Copyright (C) 2012, Redis Ltd.")?;
    table.set("_DESCRIPTION", "MessagePack C implementation for Lua")?;
    readonly_table_proxy_lua_rs(lua, table)
}

fn install_cmsgpack_lua_rs(lua: &Lua) -> lua_rs_runtime::Result<()> {
    lua.globals()
        .set("cmsgpack", create_cmsgpack_table_lua_rs(lua, false)?)?;
    lua.globals()
        .set("cmsgpack_safe", create_cmsgpack_table_lua_rs(lua, true)?)
}
