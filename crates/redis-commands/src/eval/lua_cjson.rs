//! Redis-compatible `cjson` surface for server-side Lua.

use std::cell::RefCell;
use std::rc::Rc;

use mlua::{
    Error as LuaError, LightUserData, Lua, MultiValue, Table as LuaTable, Value as LuaValue,
};

use super::lua_sandbox::readonly_table_proxy;

#[derive(Debug, Clone)]
pub(super) struct CjsonConfig {
    pub(super) encode_max_depth: usize,
    pub(super) decode_max_depth: usize,
    pub(super) encode_invalid_numbers: bool,
    pub(super) decode_invalid_numbers: bool,
    pub(super) encode_keep_buffer: bool,
    pub(super) encode_number_precision: i64,
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

/// Engine-agnostic JSON string escaping. Returns `Err(message)` on failure so
/// either the mlua or lua-rs binding can wrap it in its own error type.
pub(super) fn json_escape_string(bytes: &[u8]) -> Result<String, String> {
    serde_json::to_string(String::from_utf8_lossy(bytes).as_ref())
        .map_err(|err| format!("Cannot serialise string: {}", err))
}

/// Engine-agnostic JSON number formatting. Returns `Err(message)` on failure.
pub(super) fn encode_json_number(n: f64, allow_invalid: bool) -> Result<String, String> {
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
        Err("Cannot serialise number: must not be NaN or Infinity".to_string())
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
        LuaValue::Number(n) => {
            encode_json_number(n, cfg.encode_invalid_numbers).map_err(LuaError::RuntimeError)
        }
        LuaValue::String(s) => {
            json_escape_string(s.as_bytes().as_ref()).map_err(LuaError::RuntimeError)
        }
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
            LuaValue::String(s) => {
                json_escape_string(s.as_bytes().as_ref()).map_err(LuaError::RuntimeError)?
            }
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

pub(super) fn install_cjson(lua: &Lua) -> mlua::Result<()> {
    let cjson = create_cjson_table(lua, Rc::new(RefCell::new(CjsonConfig::default())))?;
    lua.globals()
        .set("cjson", readonly_table_proxy(lua, cjson)?)
}
