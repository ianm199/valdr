//! Bit operations: SETBIT, GETBIT, BITOP, BITCOUNT, BITPOS, BITFIELD, BITFIELD_RO.
//!
//! Port of `src/bitops.c`. Storage uses `ObjectKind::String(StringEncoding::Raw)`.
//! Bit numbering follows Redis convention: bit 0 is the MSB of byte 0.

use redis_core::command_context::CommandContext;
use redis_core::object::RedisObject;
use redis_types::{RedisError, RedisResult, RedisString};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OverflowType {
    Wrap,
    Sat,
    Fail,
}

impl Default for OverflowType {
    fn default() -> Self {
        OverflowType::Wrap
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitOp {
    And,
    Or,
    Xor,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitfieldOpCode {
    Get,
    Set,
    IncrBy,
}

struct BitfieldOp {
    offset: u64,
    incr: i64,
    opcode: BitfieldOpCode,
    owtype: OverflowType,
    bits: u32,
    sign: bool,
}

/// Count set bits in `data`.
pub(crate) fn server_popcount(data: &[u8]) -> i64 {
    data.iter().map(|b| b.count_ones() as i64).sum()
}

/// Return the position of the first bit equal to `bit` within `data[..count]`.
///
/// If `bit == 0` and no clear bit is found, returns `count * 8` (zero-padded right).
/// If `bit == 1` and no set bit is found, returns `-1`.
pub(crate) fn server_bitpos(data: &[u8], count: usize, bit: i32) -> i64 {
    let target = bit != 0;
    let skip_byte: u8 = if target { 0x00 } else { 0xFF };
    let count = count.min(data.len());
    let mut pos: i64 = 0;
    let mut i = 0usize;

    while i < count && data[i] == skip_byte {
        pos += 8;
        i += 1;
    }

    if i >= count {
        return if target { -1 } else { pos };
    }

    let byte = data[i];
    for shift in (0u32..8).rev() {
        let is_set = (byte >> shift) & 1 != 0;
        if is_set == target {
            return pos;
        }
        pos += 1;
    }

    pos
}

/// Write an unsigned integer of `bits` width into `p` at bit `offset`.
pub(crate) fn set_unsigned_bitfield(p: &mut [u8], offset: u64, bits: u64, value: u64) {
    for j in 0..bits {
        let bitval = ((value >> (bits - 1 - j)) & 1) as u8;
        let byte_idx = ((offset + j) >> 3) as usize;
        let bit_pos = 7 - ((offset + j) & 0x7);
        let byte = p[byte_idx];
        p[byte_idx] = (byte & !(1 << bit_pos)) | (bitval << bit_pos);
    }
}

/// Write a signed integer into `p` at bit `offset` using two's complement.
pub(crate) fn set_signed_bitfield(p: &mut [u8], offset: u64, bits: u64, value: i64) {
    set_unsigned_bitfield(p, offset, bits, value as u64);
}

/// Read an unsigned integer of `bits` width from `p` at bit `offset`.
pub(crate) fn get_unsigned_bitfield(p: &[u8], offset: u64, bits: u64) -> u64 {
    let mut value: u64 = 0;
    for j in 0..bits {
        let byte_idx = ((offset + j) >> 3) as usize;
        let bit_pos = 7 - ((offset + j) & 0x7);
        let byteval = p[byte_idx] as u64;
        let bitval = (byteval >> bit_pos) & 1;
        value = (value << 1) | bitval;
    }
    value
}

/// Read a signed integer of `bits` width from `p` at bit `offset`.
pub(crate) fn get_signed_bitfield(p: &[u8], offset: u64, bits: u64) -> i64 {
    let u = get_unsigned_bitfield(p, offset, bits);
    let mut value = u as i64;
    if bits < 64 && (u & (1u64 << (bits - 1))) != 0 {
        value |= (u64::MAX << bits) as i64;
    }
    value
}

/// Check unsigned bitfield overflow, returning `(direction, wrapped_value)`.
///
/// `direction` is `0` (none), `1` (overflow), or `-1` (underflow).
pub(crate) fn check_unsigned_bitfield_overflow(
    value: u64,
    incr: i64,
    bits: u64,
    owtype: OverflowType,
) -> (i32, u64) {
    let max: u64 = if bits == 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    };
    let maxincr = (max - value) as i64;
    let minincr = -(value as i64);

    let compute_wrap = || -> u64 {
        let mask: u64 = if bits == 64 { 0 } else { u64::MAX << bits };
        value.wrapping_add(incr as u64) & !mask
    };

    if value > max || (incr > 0 && incr > maxincr) {
        let limit = match owtype {
            OverflowType::Wrap => compute_wrap(),
            OverflowType::Sat => max,
            OverflowType::Fail => 0,
        };
        (1, limit)
    } else if incr < 0 && incr < minincr {
        let limit = match owtype {
            OverflowType::Wrap => compute_wrap(),
            OverflowType::Sat => 0,
            OverflowType::Fail => 0,
        };
        (-1, limit)
    } else {
        (0, 0)
    }
}

/// Check signed bitfield overflow, returning `(direction, wrapped_value)`.
pub(crate) fn check_signed_bitfield_overflow(
    value: i64,
    incr: i64,
    bits: u64,
    owtype: OverflowType,
) -> (i32, i64) {
    let max: i64 = if bits == 64 {
        i64::MAX
    } else {
        (1i64 << (bits - 1)) - 1
    };
    let min: i64 = (-max) - 1;

    let maxincr = ((max as u64).wrapping_sub(value as u64)) as i64;
    let minincr = ((min as u64).wrapping_sub(value as u64)) as i64;

    let compute_wrap = || -> i64 {
        let msb = 1u64 << (bits - 1);
        let c = (value as u64).wrapping_add(incr as u64);
        let c = if bits < 64 {
            let mask = u64::MAX << bits;
            if c & msb != 0 {
                c | mask
            } else {
                c & !mask
            }
        } else {
            c
        };
        c as i64
    };

    let overflowed =
        value > max || (bits != 64 && incr > maxincr) || (value >= 0 && incr > 0 && incr > maxincr);
    let underflowed =
        value < min || (bits != 64 && incr < minincr) || (value < 0 && incr < 0 && incr < minincr);

    if overflowed {
        let limit = match owtype {
            OverflowType::Wrap => compute_wrap(),
            OverflowType::Sat => max,
            OverflowType::Fail => 0,
        };
        (1, limit)
    } else if underflowed {
        let limit = match owtype {
            OverflowType::Wrap => compute_wrap(),
            OverflowType::Sat => min,
            OverflowType::Fail => 0,
        };
        (-1, limit)
    } else {
        (0, 0)
    }
}

/// Parse a decimal integer from an ASCII byte slice.
fn parse_i64_from_bytes(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    if digits.is_empty() {
        return None;
    }
    let mut val: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    if neg {
        val.checked_neg()
    } else {
        Some(val)
    }
}

/// Parse a bit offset, honoring the `#<n>` BITFIELD hash form when `hash` is true.
fn get_bit_offset_from_arg(arg: &[u8], hash: bool, bits: i32) -> Result<u64, RedisError> {
    const ERR: &[u8] = b"bit offset is not an integer or out of range";
    const PROTO_MAX_BULK_LEN: u64 = 512 * 1024 * 1024;

    let use_hash = arg.first() == Some(&b'#') && hash && bits > 0;
    let slice = if use_hash { &arg[1..] } else { arg };

    let mut loffset = parse_i64_from_bytes(slice).ok_or_else(|| RedisError::runtime(ERR))?;

    if use_hash {
        loffset = loffset
            .checked_mul(bits as i64)
            .ok_or_else(|| RedisError::runtime(ERR))?;
    }

    if loffset < 0 || (loffset >> 3) as u64 >= PROTO_MAX_BULK_LEN {
        return Err(RedisError::runtime(ERR));
    }

    Ok(loffset as u64)
}

/// Parse a BITFIELD type specifier (`u<N>` or `i<N>`), returning `(is_signed, bits)`.
fn get_bitfield_type_from_arg(arg: &[u8]) -> Result<(bool, u32), RedisError> {
    const ERR: &[u8] =
        b"Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.";

    let sign = match arg.first() {
        Some(b'i') => true,
        Some(b'u') => false,
        _ => return Err(RedisError::runtime(ERR)),
    };

    let llbits = parse_i64_from_bytes(&arg[1..]).ok_or_else(|| RedisError::runtime(ERR))?;

    if llbits < 1 || (sign && llbits > 64) || (!sign && llbits > 63) {
        return Err(RedisError::runtime(ERR));
    }

    Ok((sign, llbits as u32))
}

/// Read the current string bytes for `key`, preserving the missing-key distinction.
///
/// Returns `Err(WrongType)` if the key exists and holds a non-string value.
fn lookup_string_bytes(
    ctx: &CommandContext,
    key: &RedisString,
) -> Result<Option<Vec<u8>>, RedisError> {
    match ctx.db().find(key) {
        None => Ok(None),
        Some(obj) => {
            if !obj.is_string() {
                return Err(RedisError::wrong_type());
            }
            Ok(Some(obj.string_bytes_owned()))
        }
    }
}

/// Read the current string bytes for `key`, returning an empty vec for missing keys.
fn read_string_bytes(ctx: &CommandContext, key: &RedisString) -> Result<Vec<u8>, RedisError> {
    Ok(lookup_string_bytes(ctx, key)?.unwrap_or_default())
}

/// SETBIT key offset bitvalue
pub fn setbit_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"setbit"));
    }
    let key = ctx.arg_owned(1usize)?;
    let offset_arg = ctx.arg_owned(2usize)?;
    let val_arg = ctx.arg_owned(3usize)?;

    let bitoffset = get_bit_offset_from_arg(offset_arg.as_bytes(), false, 0)?;

    let on = parse_i64_from_bytes(val_arg.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"bit is not an integer or out of range"))?;
    if on & !1 != 0 {
        return Err(RedisError::runtime(
            b"bit is not an integer or out of range",
        ));
    }
    let on = on != 0;

    let min_len = ((bitoffset >> 3) + 1) as usize;
    let mut bytes = read_string_bytes(ctx, &key)?;
    if bytes.len() < min_len {
        bytes.resize(min_len, 0u8);
    }

    let byte_idx = (bitoffset >> 3) as usize;
    let bit_shift = 7 - (bitoffset & 0x7);
    let byteval = bytes[byte_idx];
    let bitval = (byteval >> bit_shift) & 1 != 0;

    bytes[byte_idx] = (byteval & !(1u8 << bit_shift)) | (if on { 1u8 << bit_shift } else { 0 });

    let obj = RedisObject::new_raw_string(&bytes);
    ctx.db_mut()
        .set_key(key, obj, redis_core::db::SETKEY_KEEPTTL);

    ctx.reply_integer(if bitval { 1 } else { 0 })
}

/// GETBIT key offset
pub fn getbit_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"getbit"));
    }
    let key = ctx.arg_owned(1usize)?;
    let offset_arg = ctx.arg_owned(2usize)?;
    let bitoffset = get_bit_offset_from_arg(offset_arg.as_bytes(), false, 0)?;

    let bytes = read_string_bytes(ctx, &key)?;
    if bytes.is_empty() {
        return ctx.reply_integer(0);
    }

    let byte_idx = (bitoffset >> 3) as usize;
    let bit_shift = 7 - (bitoffset & 0x7);
    let bitval = if byte_idx < bytes.len() {
        (bytes[byte_idx] >> bit_shift) & 1
    } else {
        0
    };

    ctx.reply_integer(bitval as i64)
}

/// BITOP op destkey srckey [srckey …]
pub fn bitop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(b"bitop"));
    }
    let op_arg = ctx.arg_owned(1usize)?;
    let op_bytes = op_arg.as_bytes();

    let op = if op_bytes.eq_ignore_ascii_case(b"and") {
        BitOp::And
    } else if op_bytes.eq_ignore_ascii_case(b"or") {
        BitOp::Or
    } else if op_bytes.eq_ignore_ascii_case(b"xor") {
        BitOp::Xor
    } else if op_bytes.eq_ignore_ascii_case(b"not") {
        BitOp::Not
    } else {
        return Err(RedisError::syntax(b"syntax error"));
    };

    if op == BitOp::Not && argc != 4 {
        return Err(RedisError::runtime(
            b"BITOP NOT must be called with a single source key.",
        ));
    }

    let target_key = ctx.arg_owned(2usize)?;
    let numkeys = argc - 3;
    let mut sources: Vec<Vec<u8>> = Vec::with_capacity(numkeys);
    let mut maxlen: usize = 0;

    for j in 0..numkeys {
        let src_key = ctx.arg_owned(j + 3)?;
        let bytes = read_string_bytes(ctx, &src_key)?;
        if bytes.len() > maxlen {
            maxlen = bytes.len();
        }
        sources.push(bytes);
    }

    if maxlen == 0 {
        ctx.db_mut().sync_delete(&target_key);
        return ctx.reply_integer(0);
    }

    let mut res = vec![0u8; maxlen];
    for pos in 0..maxlen {
        let first = sources[0].get(pos).copied().unwrap_or(0);
        let mut output = if op == BitOp::Not { !first } else { first };

        for src in sources.iter().take(numkeys).skip(1) {
            let byte = src.get(pos).copied().unwrap_or(0);
            match op {
                BitOp::And => output &= byte,
                BitOp::Or => output |= byte,
                BitOp::Xor => output ^= byte,
                BitOp::Not => {}
            }
        }
        res[pos] = output;
    }

    let obj = RedisObject::new_raw_string(&res);
    ctx.db_mut().set_key(target_key, obj, 0);
    ctx.reply_integer(maxlen as i64)
}

/// BITCOUNT key [start [end [BIT|BYTE]]]
pub fn bitcount_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();

    if argc == 2 {
        let key = ctx.arg_owned(1usize)?;
        let Some(bytes) = lookup_string_bytes(ctx, &key)? else {
            return ctx.reply_integer(0);
        };
        return ctx.reply_integer(server_popcount(&bytes));
    } else if argc != 3 && argc != 4 && argc != 5 {
        return Err(RedisError::syntax(b"syntax error"));
    }

    let key = ctx.arg_owned(1usize)?;
    let start_arg = ctx.arg_owned(2usize)?;
    let mut start =
        parse_i64_from_bytes(start_arg.as_bytes()).ok_or_else(RedisError::not_integer)?;
    let mut end;

    let mut isbit = false;
    if argc == 5 {
        let unit_arg = ctx.arg_owned(4usize)?;
        let unit = unit_arg.as_bytes();
        if unit.eq_ignore_ascii_case(b"bit") {
            isbit = true;
        } else if unit.eq_ignore_ascii_case(b"byte") {
            isbit = false;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
    if argc >= 4 {
        let end_arg = ctx.arg_owned(3usize)?;
        end = parse_i64_from_bytes(end_arg.as_bytes()).ok_or_else(RedisError::not_integer)?;
    } else {
        end = 0;
    }

    let Some(bytes) = lookup_string_bytes(ctx, &key)? else {
        return ctx.reply_integer(0);
    };
    let strlen = bytes.len() as i64;
    let mut totlen = strlen;

    if argc < 4 {
        end = totlen - 1;
    }

    if start < 0 && end < 0 && start > end {
        return ctx.reply_integer(0);
    }
    if isbit {
        totlen <<= 3;
    }

    if start < 0 {
        start += totlen;
    }
    if end < 0 {
        end += totlen;
    }
    if start < 0 {
        start = 0;
    }
    if end < 0 {
        end = 0;
    }
    if end >= totlen {
        end = totlen - 1;
    }

    let mut first_byte_neg_mask: u8 = 0;
    let mut last_byte_neg_mask: u8 = 0;
    if isbit && start <= end {
        first_byte_neg_mask = (!((1u32 << (8 - (start & 7))) - 1) & 0xFF) as u8;
        last_byte_neg_mask = ((1u32 << (7 - (end & 7))) - 1) as u8;
        start >>= 3;
        end >>= 3;
    }

    if start > end {
        return ctx.reply_integer(0);
    }

    let byte_start = start as usize;
    let byte_end = end as usize;
    let region = &bytes[byte_start..=byte_end];
    let mut count = server_popcount(region);

    if first_byte_neg_mask != 0 || last_byte_neg_mask != 0 {
        let edge_bytes = [
            if first_byte_neg_mask != 0 {
                bytes[byte_start] & first_byte_neg_mask
            } else {
                0
            },
            if last_byte_neg_mask != 0 {
                bytes[byte_end] & last_byte_neg_mask
            } else {
                0
            },
        ];
        count -= server_popcount(&edge_bytes);
    }

    ctx.reply_integer(count)
}

/// BITPOS key bit [start [end [BIT|BYTE]]]
pub fn bitpos_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 || argc > 6 {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let key = ctx.arg_owned(1usize)?;
    let bit_arg = ctx.arg_owned(2usize)?;
    let bit = parse_i64_from_bytes(bit_arg.as_bytes()).ok_or_else(RedisError::not_integer)? as i32;

    if bit != 0 && bit != 1 {
        return Err(RedisError::runtime(b"The bit argument must be 1 or 0."));
    }

    let mut start: i64 = 0;
    let mut end: Option<i64> = None;
    let mut end_given = false;
    let mut isbit = false;

    if argc >= 4 {
        let start_arg = ctx.arg_owned(3usize)?;
        start = parse_i64_from_bytes(start_arg.as_bytes()).ok_or_else(RedisError::not_integer)?;
        if argc >= 5 {
            let end_arg = ctx.arg_owned(4usize)?;
            if argc == 6 {
                let unit_arg = ctx.arg_owned(5usize)?;
                let unit = unit_arg.as_bytes();
                if unit.eq_ignore_ascii_case(b"bit") {
                    isbit = true;
                } else if unit.eq_ignore_ascii_case(b"byte") {
                    isbit = false;
                } else {
                    return Err(RedisError::syntax(b"syntax error"));
                }
            }
            end =
                Some(parse_i64_from_bytes(end_arg.as_bytes()).ok_or_else(RedisError::not_integer)?);
            end_given = true;
        }
    }

    let Some(bytes) = lookup_string_bytes(ctx, &key)? else {
        return ctx.reply_integer(if bit == 1 { -1 } else { 0 });
    };
    let strlen = bytes.len() as i64;
    let mut end = end.unwrap_or(strlen - 1);
    let mut totlen = strlen;
    if isbit {
        totlen <<= 3;
    }
    if start < 0 {
        start += totlen;
    }
    if end < 0 {
        end += totlen;
    }
    if start < 0 {
        start = 0;
    }
    if end < 0 {
        end = 0;
    }
    if end >= totlen {
        end = totlen - 1;
    }

    let mut first_byte_neg_mask: u8 = 0;
    let mut last_byte_neg_mask: u8 = 0;
    if isbit && start <= end {
        first_byte_neg_mask = (!((1u32 << (8 - (start & 7))) - 1) & 0xFF) as u8;
        last_byte_neg_mask = ((1u32 << (7 - (end & 7))) - 1) as u8;
        start >>= 3;
        end >>= 3;
    }

    if start > end {
        return ctx.reply_integer(-1);
    }

    let p = &bytes;
    let mut search_start = start;
    let mut nbytes = end - start + 1;

    let pos: i64 = 'find_pos: {
        if first_byte_neg_mask != 0 {
            let mut tmpchar = if bit == 1 {
                p[search_start as usize] & !first_byte_neg_mask
            } else {
                p[search_start as usize] | first_byte_neg_mask
            };
            if last_byte_neg_mask != 0 && nbytes == 1 {
                tmpchar = if bit == 1 {
                    tmpchar & !last_byte_neg_mask
                } else {
                    tmpchar | last_byte_neg_mask
                };
            }
            let pos = server_bitpos(&[tmpchar], 1, bit);
            if nbytes == 1 || (pos != -1 && pos != 8) {
                break 'find_pos pos;
            }
            search_start += 1;
            nbytes -= 1;
        }

        let curbytes = nbytes - if last_byte_neg_mask != 0 { 1 } else { 0 };
        if curbytes > 0 {
            let slice = &p[search_start as usize..(search_start + curbytes) as usize];
            let pos = server_bitpos(slice, curbytes as usize, bit);
            if nbytes == curbytes || (pos != -1 && pos != (curbytes as i64) << 3) {
                break 'find_pos pos;
            }
            search_start += curbytes;
            nbytes -= curbytes;
        }

        let tmpchar = if bit == 1 {
            p[end as usize] & !last_byte_neg_mask
        } else {
            p[end as usize] | last_byte_neg_mask
        };
        let _ = nbytes;
        server_bitpos(&[tmpchar], 1, bit)
    };

    if end_given && bit == 0 && pos != -1 && pos == (nbytes as i64) << 3 {
        return ctx.reply_integer(-1);
    }

    let final_pos = if pos != -1 {
        pos + (search_start << 3)
    } else {
        -1
    };
    ctx.reply_integer(final_pos)
}

/// Core implementation for BITFIELD and BITFIELD_RO.
fn bitfield_generic(ctx: &mut CommandContext, readonly: bool) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"bitfield"));
    }
    let key = ctx.arg_owned(1usize)?;

    let mut ops: Vec<BitfieldOp> = Vec::new();
    let mut owtype = OverflowType::Wrap;
    let mut is_readonly_ops = true;
    let mut highest_write_offset: u64 = 0;

    let mut j = 2usize;
    while j < argc {
        let remargs = argc - j - 1;
        let subcmd_arg = ctx.arg_owned(j)?;
        let subcmd = subcmd_arg.as_bytes();

        if subcmd.eq_ignore_ascii_case(b"get") && remargs >= 2 {
            let type_arg = ctx.arg_owned(j + 1)?;
            let (sign, bits) = get_bitfield_type_from_arg(type_arg.as_bytes())?;
            let off_arg = ctx.arg_owned(j + 2)?;
            let offset = get_bit_offset_from_arg(off_arg.as_bytes(), true, bits as i32)?;
            ops.push(BitfieldOp {
                offset,
                incr: 0,
                opcode: BitfieldOpCode::Get,
                owtype,
                bits,
                sign,
            });
            j += 3;
        } else if subcmd.eq_ignore_ascii_case(b"set") && remargs >= 3 {
            if readonly {
                return Err(RedisError::runtime(
                    b"BITFIELD_RO only supports the GET subcommand",
                ));
            }
            let type_arg = ctx.arg_owned(j + 1)?;
            let (sign, bits) = get_bitfield_type_from_arg(type_arg.as_bytes())?;
            let off_arg = ctx.arg_owned(j + 2)?;
            let offset = get_bit_offset_from_arg(off_arg.as_bytes(), true, bits as i32)?;
            let val_arg = ctx.arg_owned(j + 3)?;
            let val = parse_i64_from_bytes(val_arg.as_bytes())
                .ok_or_else(|| RedisError::runtime(b"value is not an integer or out of range"))?;

            is_readonly_ops = false;
            let span = offset + bits as u64 - 1;
            if highest_write_offset < span {
                highest_write_offset = span;
            }
            ops.push(BitfieldOp {
                offset,
                incr: val,
                opcode: BitfieldOpCode::Set,
                owtype,
                bits,
                sign,
            });
            j += 4;
        } else if subcmd.eq_ignore_ascii_case(b"incrby") && remargs >= 3 {
            if readonly {
                return Err(RedisError::runtime(
                    b"BITFIELD_RO only supports the GET subcommand",
                ));
            }
            let type_arg = ctx.arg_owned(j + 1)?;
            let (sign, bits) = get_bitfield_type_from_arg(type_arg.as_bytes())?;
            let off_arg = ctx.arg_owned(j + 2)?;
            let offset = get_bit_offset_from_arg(off_arg.as_bytes(), true, bits as i32)?;
            let incr_arg = ctx.arg_owned(j + 3)?;
            let val = parse_i64_from_bytes(incr_arg.as_bytes())
                .ok_or_else(|| RedisError::runtime(b"value is not an integer or out of range"))?;

            is_readonly_ops = false;
            let span = offset + bits as u64 - 1;
            if highest_write_offset < span {
                highest_write_offset = span;
            }
            ops.push(BitfieldOp {
                offset,
                incr: val,
                opcode: BitfieldOpCode::IncrBy,
                owtype,
                bits,
                sign,
            });
            j += 4;
        } else if subcmd.eq_ignore_ascii_case(b"overflow") && remargs >= 1 {
            let ow_arg = ctx.arg_owned(j + 1)?;
            let ow = ow_arg.as_bytes();
            owtype = if ow.eq_ignore_ascii_case(b"wrap") {
                OverflowType::Wrap
            } else if ow.eq_ignore_ascii_case(b"sat") {
                OverflowType::Sat
            } else if ow.eq_ignore_ascii_case(b"fail") {
                OverflowType::Fail
            } else {
                return Err(RedisError::runtime(b"Invalid OVERFLOW type specified"));
            };
            j += 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    let existing = read_string_bytes(ctx, &key)?;
    let existing_len = existing.len();
    let key_existed = ctx.db().find(&key).is_some();
    let mut bytes: Vec<u8> = if is_readonly_ops {
        existing
    } else {
        let min_len = ((highest_write_offset >> 3) + 1) as usize;
        let mut b = existing;
        if b.len() < min_len {
            b.resize(min_len, 0u8);
        }
        b
    };
    let extended = !is_readonly_ops && bytes.len() != existing_len;

    ctx.reply_array_header(ops.len())?;

    let mut changes = 0usize;

    for op in &ops {
        if op.opcode == BitfieldOpCode::Get {
            let mut buf = [0u8; 9];
            let byte_offset = (op.offset >> 3) as usize;
            for (i, slot) in buf.iter_mut().enumerate() {
                let src_idx = byte_offset + i;
                if src_idx < bytes.len() {
                    *slot = bytes[src_idx];
                }
            }
            let local_offset = op.offset - (byte_offset as u64 * 8);
            let val: i64 = if op.sign {
                get_signed_bitfield(&buf, local_offset, op.bits as u64)
            } else {
                get_unsigned_bitfield(&buf, local_offset, op.bits as u64) as i64
            };
            ctx.reply_integer(val)?;
        } else if op.sign {
            let oldval = get_signed_bitfield(&bytes, op.offset, op.bits as u64);
            let (overflow_dir, wrapped) =
                check_signed_bitfield_overflow(oldval, op.incr, op.bits as u64, op.owtype);
            let overflowed = overflow_dir != 0;
            let (newval, retval) = if op.opcode == BitfieldOpCode::IncrBy {
                let nv = if overflowed {
                    wrapped
                } else {
                    oldval.wrapping_add(op.incr)
                };
                (nv, nv)
            } else {
                let (set_overflow, set_wrapped) =
                    check_signed_bitfield_overflow(op.incr, 0, op.bits as u64, op.owtype);
                let nv = if set_overflow != 0 {
                    set_wrapped
                } else {
                    op.incr
                };
                (nv, oldval)
            };

            let fail_now = overflowed && op.owtype == OverflowType::Fail;
            if !fail_now {
                ctx.reply_integer(retval)?;
                set_signed_bitfield(&mut bytes, op.offset, op.bits as u64, newval);
                if oldval != newval {
                    changes += 1;
                }
            } else {
                ctx.reply_null()?;
            }
        } else {
            let oldval = get_unsigned_bitfield(&bytes, op.offset, op.bits as u64);
            let (overflow_dir, wrapped) =
                check_unsigned_bitfield_overflow(oldval, op.incr, op.bits as u64, op.owtype);
            let overflowed = overflow_dir != 0;
            let (newval, retval) = if op.opcode == BitfieldOpCode::IncrBy {
                let raw = oldval.wrapping_add(op.incr as u64);
                let nv = if overflowed { wrapped } else { raw };
                (nv, nv)
            } else {
                let setval = op.incr as u64;
                let (set_overflow, set_wrapped) =
                    check_unsigned_bitfield_overflow(setval, 0, op.bits as u64, op.owtype);
                let nv = if set_overflow != 0 {
                    set_wrapped
                } else {
                    setval
                };
                (nv, oldval)
            };

            let fail_now = overflowed && op.owtype == OverflowType::Fail;
            if !fail_now {
                ctx.reply_integer(retval as i64)?;
                set_unsigned_bitfield(&mut bytes, op.offset, op.bits as u64, newval);
                if oldval != newval {
                    changes += 1;
                }
            } else {
                ctx.reply_null()?;
            }
        }
    }

    if !is_readonly_ops && (changes > 0 || extended || !key_existed) {
        let obj = RedisObject::new_raw_string(&bytes);
        let flags = if key_existed {
            redis_core::db::SETKEY_KEEPTTL
        } else {
            0
        };
        ctx.db_mut().set_key(key, obj, flags);
    }

    Ok(())
}

/// BITFIELD key [GET type offset | SET type offset value | INCRBY type offset increment]
///            [OVERFLOW WRAP|SAT|FAIL]
pub fn bitfield_command(ctx: &mut CommandContext) -> RedisResult<()> {
    bitfield_generic(ctx, false)
}

/// BITFIELD_RO key [GET type offset ...]
pub fn bitfield_ro_command(ctx: &mut CommandContext) -> RedisResult<()> {
    bitfield_generic(ctx, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_core::db::RedisDb;
    use redis_core::Client;

    fn rs(bytes: &[u8]) -> RedisString {
        RedisString::from_bytes(bytes)
    }

    fn set_args(client: &mut Client, args: &[&[u8]]) {
        client.set_args(args.iter().map(|arg| rs(arg)).collect());
    }

    #[test]
    fn bitcount_accepts_start_without_end() {
        let mut client = Client::new(1);
        let mut db = RedisDb::new(0);
        db.set_key(rs(b"s"), RedisObject::new_raw_string(b"foobar"), 0);
        set_args(&mut client, &[b"BITCOUNT", b"s", b"1"]);

        let mut ctx = CommandContext::with_db(&mut client, &mut db);
        bitcount_command(&mut ctx).unwrap();

        assert_eq!(client.drain_reply(), b":22\r\n");
    }

    #[test]
    fn bitcount_parses_integer_args_before_type_check() {
        let mut client = Client::new(1);
        let mut db = RedisDb::new(0);
        db.set_key(rs(b"s"), RedisObject::new_list(), 0);
        set_args(&mut client, &[b"BITCOUNT", b"s", b"a", b"b"]);

        let mut ctx = CommandContext::with_db(&mut client, &mut db);
        let err = bitcount_command(&mut ctx).unwrap_err();

        assert_eq!(
            err.to_resp_payload().as_bytes(),
            b"ERR value is not an integer or out of range"
        );
    }

    #[test]
    fn bitpos_parses_range_before_missing_key_reply() {
        let mut client = Client::new(1);
        let mut db = RedisDb::new(0);
        set_args(&mut client, &[b"BITPOS", b"missing", b"0", b"a", b"b"]);

        let mut ctx = CommandContext::with_db(&mut client, &mut db);
        let err = bitpos_command(&mut ctx).unwrap_err();

        assert_eq!(
            err.to_resp_payload().as_bytes(),
            b"ERR value is not an integer or out of range"
        );
    }

    #[test]
    fn bitpos_validates_unit_before_end_integer_when_unit_slot_exists() {
        let mut client = Client::new(1);
        let mut db = RedisDb::new(0);
        set_args(
            &mut client,
            &[b"BITPOS", b"missing", b"0", b"1", b"hello", b"hello2"],
        );

        let mut ctx = CommandContext::with_db(&mut client, &mut db);
        let err = bitpos_command(&mut ctx).unwrap_err();

        assert_eq!(err.to_resp_payload().as_bytes(), b"ERR syntax error");
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/bitops.c  (1431 lines, bitmap commands and helpers)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Safe bitmap command port; BITFIELD arithmetic remains under Tcl frontier coverage.
// ──────────────────────────────────────────────────────────────────────────
