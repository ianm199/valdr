//! Redis-compatible `cmsgpack` and `cmsgpack_safe` surfaces for Lua scripts.

use mlua::{Error as LuaError, Lua, MultiValue, Table as LuaTable, Value as LuaValue};

use super::lua_sandbox::readonly_table_proxy;

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

pub(super) fn install_cmsgpack(lua: &Lua) -> mlua::Result<()> {
    lua.globals()
        .set("cmsgpack", create_cmsgpack_table(lua, false)?)?;
    lua.globals()
        .set("cmsgpack_safe", create_cmsgpack_table(lua, true)?)
}
