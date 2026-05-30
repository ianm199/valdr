//! Bloom filter commands: BF.RESERVE, BF.ADD, BF.MADD, BF.EXISTS, BF.MEXISTS,
//! BF.INSERT, BF.INFO.
//!
//! Implements a standard single-layer Bloom filter using the
//! Kirsch-Mitzenmacher double-hashing technique with MurmurHash64A (same hash
//! function used by the HyperLogLog implementation in hyperloglog.rs).
//!
//! Hash scheme: given item bytes, compute h1 = murmur(item, SEED1) and
//! h2 = murmur(item, SEED2); the i-th probe position is
//! `(h1 + i * h2) mod m` for i in 0..k, where m is the bit-array size and
//! k is the number of hash functions.
//!
//! Bloom filter math (standard):
//!   m = ceil(-n * ln(p) / ln(2)^2)
//!   k = round(m/n * ln(2))
//!
//! Storage: `ObjectKind::Bloom(BloomFilter)` in redis-core/src/object.rs.

use redis_core::command_context::CommandContext;
use redis_core::object::{BloomFilter, RedisObject};
use redis_types::{RedisError, RedisResult, RedisString};

use crate::hyperloglog::murmur_hash64a;

const SEED1: u32 = 0xadc8_3b19;
const SEED2: u32 = 0x1234_5678;

const DEFAULT_ERROR_RATE: f64 = 0.01;
const DEFAULT_CAPACITY: u64 = 100;

fn bloom_wrong_type_error() -> RedisError {
    RedisError::runtime(b"WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Compute Bloom filter parameters for a given capacity and error rate.
///
/// Returns `(bit_count, n_hashes)`.
fn bloom_params(capacity: u64, error_rate: f64) -> (u64, u32) {
    let n = capacity as f64;
    let ln2 = std::f64::consts::LN_2;
    let m = ((-n * error_rate.ln()) / (ln2 * ln2)).ceil() as u64;
    let m = m.max(8);
    let k = ((m as f64 / n * ln2).round() as u32).max(1);
    (m, k)
}

/// Allocate a new BloomFilter with the given parameters.
fn new_bloom_filter(
    capacity: u64,
    error_rate: f64,
    expansion: u32,
    nonscaling: bool,
) -> BloomFilter {
    let (bit_count, n_hashes) = bloom_params(capacity, error_rate);
    let byte_count = bit_count.div_ceil(8) as usize;
    BloomFilter {
        bits: vec![0u8; byte_count],
        n_hashes,
        capacity,
        item_count: 0,
        error_rate,
        expansion,
        nonscaling,
    }
}

/// Test whether `item` is probably present in `bf`.
///
/// Returns `true` when all k bit positions derived from `item` are set.
fn bloom_check(bf: &BloomFilter, item: &[u8]) -> bool {
    let m = bf.bit_count();
    if m == 0 {
        return false;
    }
    let h1 = murmur_hash64a(item, SEED1);
    let h2 = murmur_hash64a(item, SEED2);
    for i in 0..bf.n_hashes as u64 {
        let pos = h1.wrapping_add(i.wrapping_mul(h2)) % m;
        let byte_idx = (pos / 8) as usize;
        let bit_idx = pos % 8;
        if bf.bits[byte_idx] & (1u8 << bit_idx) == 0 {
            return false;
        }
    }
    true
}

/// Add `item` to `bf`.
///
/// Returns `true` when the item was newly inserted (all k positions were not
/// already set), `false` when it was probably already present.
fn bloom_add(bf: &mut BloomFilter, item: &[u8]) -> bool {
    let m = bf.bit_count();
    if m == 0 {
        return false;
    }
    let h1 = murmur_hash64a(item, SEED1);
    let h2 = murmur_hash64a(item, SEED2);
    let mut was_new = false;
    for i in 0..bf.n_hashes as u64 {
        let pos = h1.wrapping_add(i.wrapping_mul(h2)) % m;
        let byte_idx = (pos / 8) as usize;
        let bit_idx = pos % 8;
        if bf.bits[byte_idx] & (1u8 << bit_idx) == 0 {
            bf.bits[byte_idx] |= 1u8 << bit_idx;
            was_new = true;
        }
    }
    if was_new {
        bf.item_count += 1;
    }
    was_new
}


/// Parse a float argument, returning a protocol-level error on failure.
fn parse_float_arg(bytes: &[u8]) -> Result<f64, RedisError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| RedisError::runtime(b"ERR Bad arguments - could not parse float"))
}

/// Parse a u64 argument, returning a protocol-level error on failure.
fn parse_u64_arg(bytes: &[u8]) -> Result<u64, RedisError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| RedisError::runtime(b"ERR Bad arguments - could not parse integer"))
}

/// Parse a u32 argument, returning a protocol-level error on failure.
fn parse_u32_arg(bytes: &[u8]) -> Result<u32, RedisError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .ok_or_else(|| RedisError::runtime(b"ERR Bad arguments - could not parse integer"))
}

/// `BF.RESERVE key error_rate capacity [EXPANSION n] [NONSCALING]`
///
/// Create an empty Bloom filter at `key`. Returns an error when the key
/// already exists (regardless of type).
pub fn bf_reserve_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg(1)?.clone();
    let error_rate = parse_float_arg(ctx.arg(2)?.as_bytes())?;
    let capacity = parse_u64_arg(ctx.arg(3)?.as_bytes())?;

    if error_rate <= 0.0 || error_rate >= 1.0 {
        return Err(RedisError::runtime(b"ERR (0 < error rate range < 1) "));
    }
    if capacity == 0 {
        return Err(RedisError::runtime(
            b"ERR (capacity should be larger than 0)",
        ));
    }

    let mut expansion: u32 = 2;
    let mut nonscaling = false;
    let mut i = 4usize;
    while i < ctx.arg_count() {
        let flag = ctx.arg(i)?.as_bytes().to_ascii_uppercase();
        if flag == b"EXPANSION" {
            i += 1;
            expansion = parse_u32_arg(ctx.arg(i)?.as_bytes())?;
        } else if flag == b"NONSCALING" {
            nonscaling = true;
        } else {
            return Err(RedisError::runtime(b"ERR unknown argument"));
        }
        i += 1;
    }

    if ctx.db().find(&key).is_some() {
        return Err(RedisError::runtime(b"ERR item exists"));
    }

    let bf = new_bloom_filter(capacity, error_rate, expansion, nonscaling);
    let obj = RedisObject::new_bloom_from_filter(bf);
    ctx.db_mut().insert(key, obj);
    ctx.reply_simple_string(b"OK")
}

/// `BF.ADD key item`
///
/// Add one item to the filter at `key`. Auto-creates the filter with default
/// parameters when the key does not yet exist. Returns `:1` when newly added,
/// `:0` when the item is probably already present.
pub fn bf_add_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg(1)?.clone();
    let item = ctx.arg(2)?.as_bytes().to_vec();

    ensure_bloom_exists(ctx, &key)?;

    let mut bf = match ctx.db_mut().lookup_key_write(&key) {
        None => {
            return Err(RedisError::runtime(
                b"ERR internal: bloom key vanished after auto-create",
            ))
        }
        Some(obj) => match obj.bloom_mut() {
            Some(b) => b.clone(),
            None => return Err(bloom_wrong_type_error()),
        },
    };

    let added = bloom_add(&mut bf, &item);
    ctx.db_mut()
        .insert(key, RedisObject::new_bloom_from_filter(bf));
    ctx.reply_integer(if added { 1 } else { 0 })
}

/// `BF.MADD key item [item ...]`
///
/// Add multiple items in one call. Returns an array reply: `:1` per newly
/// added item, `:0` for items that were probably already present.
pub fn bf_madd_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg(1)?.clone();
    let items: Vec<Vec<u8>> = (2..ctx.arg_count())
        .map(|i| ctx.arg(i).map(|s| s.as_bytes().to_vec()))
        .collect::<Result<_, _>>()?;

    ensure_bloom_exists(ctx, &key)?;

    let mut bf = match ctx.db_mut().lookup_key_write(&key) {
        None => {
            return Err(RedisError::runtime(
                b"ERR internal: bloom key vanished after auto-create",
            ))
        }
        Some(obj) => match obj.bloom_mut() {
            Some(b) => b.clone(),
            None => return Err(bloom_wrong_type_error()),
        },
    };

    let results: Vec<bool> = items.iter().map(|item| bloom_add(&mut bf, item)).collect();
    ctx.db_mut()
        .insert(key, RedisObject::new_bloom_from_filter(bf));

    ctx.reply_array_header(results.len())?;
    for added in results {
        ctx.reply_integer(if added { 1 } else { 0 })?;
    }
    Ok(())
}

/// `BF.EXISTS key item`
///
/// Test whether `item` is probably present in the filter at `key`. Returns
/// `:1` (probably present) or `:0` (definitely absent). Missing key → `:0`.
pub fn bf_exists_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg(1)?.clone();
    let item = ctx.arg(2)?.as_bytes().to_vec();

    let obj = ctx.db_mut().lookup_key_read_with_flags(&key, 0);
    match obj {
        None => ctx.reply_integer(0),
        Some(o) => match o.bloom() {
            Some(bf) => {
                let exists = bloom_check(bf, &item);
                ctx.reply_integer(if exists { 1 } else { 0 })
            }
            None => Err(bloom_wrong_type_error()),
        },
    }
}

/// `BF.MEXISTS key item [item ...]`
///
/// Test multiple items for probable presence. Returns an array of `:1`/`:0`
/// replies in the same order as the arguments.
pub fn bf_mexists_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg(1)?.clone();
    let items: Vec<Vec<u8>> = (2..ctx.arg_count())
        .map(|i| ctx.arg(i).map(|s| s.as_bytes().to_vec()))
        .collect::<Result<_, _>>()?;

    let obj = ctx.db_mut().lookup_key_read_with_flags(&key, 0);
    match obj {
        None => {
            ctx.reply_array_header(items.len())?;
            for _ in &items {
                ctx.reply_integer(0)?;
            }
            Ok(())
        }
        Some(o) => match o.bloom() {
            Some(bf) => {
                let results: Vec<bool> = items.iter().map(|item| bloom_check(bf, item)).collect();
                ctx.reply_array_header(results.len())?;
                for r in results {
                    ctx.reply_integer(if r { 1 } else { 0 })?;
                }
                Ok(())
            }
            None => Err(bloom_wrong_type_error()),
        },
    }
}

/// `BF.INSERT key [CAPACITY n] [ERROR rate] [EXPANSION n] [NONSCALING] [NOCREATE] ITEMS item [...]`
///
/// Combined reserve + add. Creates the filter when it does not yet exist
/// (unless NOCREATE is given, which causes an error when the key is absent).
pub fn bf_insert_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg(1)?.clone();

    let mut capacity = DEFAULT_CAPACITY;
    let mut error_rate = DEFAULT_ERROR_RATE;
    let mut expansion: u32 = 2;
    let mut nonscaling = false;
    let mut nocreate = false;
    let mut items_start = 0usize;

    let mut i = 2usize;
    while i < ctx.arg_count() {
        let flag = ctx.arg(i)?.as_bytes().to_ascii_uppercase();
        if flag == b"CAPACITY" {
            i += 1;
            capacity = parse_u64_arg(ctx.arg(i)?.as_bytes())?;
        } else if flag == b"ERROR" {
            i += 1;
            error_rate = parse_float_arg(ctx.arg(i)?.as_bytes())?;
        } else if flag == b"EXPANSION" {
            i += 1;
            expansion = parse_u32_arg(ctx.arg(i)?.as_bytes())?;
        } else if flag == b"NONSCALING" {
            nonscaling = true;
        } else if flag == b"NOCREATE" {
            nocreate = true;
        } else if flag == b"ITEMS" {
            items_start = i + 1;
            break;
        } else {
            return Err(RedisError::runtime(b"ERR unknown argument"));
        }
        i += 1;
    }

    if items_start == 0 || items_start >= ctx.arg_count() {
        return Err(RedisError::runtime(b"ERR ITEMS keyword required"));
    }

    let items: Vec<Vec<u8>> = (items_start..ctx.arg_count())
        .map(|j| ctx.arg(j).map(|s| s.as_bytes().to_vec()))
        .collect::<Result<_, _>>()?;

    let key_exists = ctx.db().find(&key).is_some();

    if key_exists {
        let mut bf = match ctx.db_mut().lookup_key_write(&key) {
            None => return Err(RedisError::runtime(b"ERR internal: key vanished")),
            Some(obj) => match obj.bloom_mut() {
                Some(b) => b.clone(),
                None => return Err(bloom_wrong_type_error()),
            },
        };
        let results: Vec<bool> = items.iter().map(|item| bloom_add(&mut bf, item)).collect();
        ctx.db_mut()
            .insert(key, RedisObject::new_bloom_from_filter(bf));
        ctx.reply_array_header(results.len())?;
        for added in results {
            ctx.reply_integer(if added { 1 } else { 0 })?;
        }
    } else {
        if nocreate {
            return Err(RedisError::runtime(b"ERR not found"));
        }
        if error_rate <= 0.0 || error_rate >= 1.0 {
            return Err(RedisError::runtime(b"ERR (0 < error rate range < 1) "));
        }
        let mut bf = new_bloom_filter(capacity, error_rate, expansion, nonscaling);
        let results: Vec<bool> = items.iter().map(|item| bloom_add(&mut bf, item)).collect();
        let obj = RedisObject::new_bloom_from_filter(bf);
        ctx.db_mut().insert(key, obj);
        ctx.reply_array_header(results.len())?;
        for r in results {
            ctx.reply_integer(if r { 1 } else { 0 })?;
        }
    }

    Ok(())
}

/// `BF.INFO key [CAPACITY|SIZE|FILTERS|ITEMS|EXPANSION]`
///
/// Return metadata about the filter. With no subfield, returns all fields as a
/// flat alternating key/value array. With a subfield, returns only that value.
pub fn bf_info_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg(1)?.clone();

    let obj = ctx.db_mut().lookup_key_read_with_flags(&key, 0);
    let obj = match obj {
        None => return Err(RedisError::runtime(b"ERR not found")),
        Some(o) => o,
    };
    let bf = match obj.bloom() {
        Some(b) => b,
        None => return Err(bloom_wrong_type_error()),
    };

    let capacity = bf.capacity;
    let size = bf.bits.len() as u64;
    let item_count = bf.item_count;
    let expansion = bf.expansion;

    if ctx.arg_count() == 2 {
        ctx.reply_array_header(10usize)?;
        ctx.reply_bulk(b"Capacity")?;
        ctx.reply_integer(capacity as i64)?;
        ctx.reply_bulk(b"Size")?;
        ctx.reply_integer(size as i64)?;
        ctx.reply_bulk(b"Number of filters")?;
        ctx.reply_integer(1)?;
        ctx.reply_bulk(b"Number of items inserted")?;
        ctx.reply_integer(item_count as i64)?;
        ctx.reply_bulk(b"Expansion rate")?;
        ctx.reply_integer(expansion as i64)?;
        return Ok(());
    }

    let subfield = ctx.arg(2)?.as_bytes().to_ascii_uppercase();
    if subfield == b"CAPACITY" {
        ctx.reply_integer(capacity as i64)
    } else if subfield == b"SIZE" {
        ctx.reply_integer(size as i64)
    } else if subfield == b"FILTERS" {
        ctx.reply_integer(1)
    } else if subfield == b"ITEMS" {
        ctx.reply_integer(item_count as i64)
    } else if subfield == b"EXPANSION" {
        ctx.reply_integer(expansion as i64)
    } else {
        Err(RedisError::runtime(b"ERR unknown info subfield"))
    }
}

/// Ensure a Bloom filter exists at `key`, creating one with defaults if absent.
///
/// Returns a WRONGTYPE error when the key exists but holds a non-Bloom object.
fn ensure_bloom_exists(ctx: &mut CommandContext, key: &RedisString) -> RedisResult<()> {
    match ctx.db_mut().lookup_key_write(key) {
        None => {
            let bf = new_bloom_filter(DEFAULT_CAPACITY, DEFAULT_ERROR_RATE, 2, false);
            let obj = RedisObject::new_bloom_from_filter(bf);
            ctx.db_mut().insert(key.clone(), obj);
            Ok(())
        }
        Some(obj) => {
            if obj.bloom_mut().is_some() {
                Ok(())
            } else {
                Err(bloom_wrong_type_error())
            }
        }
    }
}
