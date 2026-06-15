//! Redis-compatible LuaBitOp surface for server-side Lua.

use mlua::{Error as LuaError, Lua, Table as LuaTable, Variadic};

use super::lua_sandbox::readonly_table_proxy;

/// LuaBitOp `barg`: reduce a Lua number to its low 32 bits using the same
/// magic-number conversion LuaBitOp performs for the double `lua_Number`
/// build that Valkey ships: add `2^52 + 2^51`,
/// then take the low 32 bits of the resulting double. mlua is built with
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

/// LuaBitOp `bit.tohex`, including
/// `INT32_MIN` guard that makes `bit.tohex(65535, -2147483648)` resolve
/// `0000FFFF` (uppercase, clamped to 8 digits) rather than hitting
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

pub(super) fn install_bit(lua: &Lua) -> mlua::Result<()> {
    lua.globals().set("bit", create_bit_table(lua)?)
}
