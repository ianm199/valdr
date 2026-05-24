//! Hash type and command implementations.
//!
//! Covers the byte-exact wire surface of HSET, HSETNX, HGET, HGETDEL, HDEL,
//! HEXISTS, HLEN, HSTRLEN, HGETALL, HKEYS, HVALS, HMGET, HMSET, HINCRBY,
//! HINCRBYFLOAT, and HRANDFIELD for Round 3.
//!
//! C source: `reference/valkey/src/t_hash.c`
//!
//! # Storage shape
//!
//! Round 3 uses the pragmatic `ObjectKind::Hash(HashEncoding::Inline(_))`
//! encoding from `redis-core::object` — a `HashMap<RedisString,
//! RedisString>` providing O(1) field lookups and updates. The real
//! `ListPack` / `HashTable` encodings land in Phase 4 when `redis-ds`
//! exposes those types.
//!
//! # Architect items
//!
//! TODO(architect): swap the `Inline` encoding for the real `ListPack` /
//! `HashTable` types from `redis-ds` once Phase 4 makes them usable.
//!
//! TODO(architect): HSCAN cursor iteration depends on the cursor support
//! that the keyspace scan implementation needs from redis-ds — not yet
//! ported.
//!
//! TODO(architect): HRANDFIELD currently iterates the underlying HashMap
//! in arbitrary insertion order rather than drawing from the server's PRNG
//! state. Replace with the real `serverGenRandomNumber` style sampling
//! once the RNG state is exposed through `CommandContext`.
//!
//! TODO(architect): replace the pragmatic per-field expiry side table with
//! object-owned HASH_2 metadata once the real hash table encoding lands.
//!
//! TODO(port): HINCRBYFLOAT formats the result with Rust's default
//! f64 `{}` formatter rather than the C long-double `%.17Lg` routine.
//! Sufficient for byte-exact diffs on small magnitudes but should be
//! tightened once a dedicated float-to-string helper exists.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

use redis_core::command_context::CommandContext;
use redis_core::db::glob_match;
use redis_core::notify::{NOTIFY_GENERIC, NOTIFY_HASH};
use redis_core::object::RedisObject;
use redis_types::{RedisError, RedisResult, RedisString};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct HashFieldExpiryKey {
    dbid: u32,
    key: RedisString,
    field: RedisString,
}

static HASH_FIELD_EXPIRES: OnceLock<Mutex<HashMap<HashFieldExpiryKey, i64>>> = OnceLock::new();
static EXPIRED_FIELDS: AtomicU64 = AtomicU64::new(0);

fn field_expires() -> &'static Mutex<HashMap<HashFieldExpiryKey, i64>> {
    HASH_FIELD_EXPIRES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn expired_fields_count() -> u64 {
    EXPIRED_FIELDS.load(Ordering::Relaxed)
}

pub(crate) fn reset_expired_fields_count() {
    EXPIRED_FIELDS.store(0, Ordering::Relaxed);
}

pub fn volatile_hash_key_count(dbid: u32, db: &redis_core::db::RedisDb) -> u64 {
    let guard = match field_expires().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let now = now_ms();
    db.iter_for_eviction()
        .filter(|(key, obj)| {
            let Some(hash) = obj.hash() else {
                return false;
            };
            guard.iter().any(|(exp, when)| {
                exp.dbid == dbid && exp.key == **key && *when > now && hash.contains_key(&exp.field)
            })
        })
        .count() as u64
}

fn expiry_key(dbid: u32, key: &RedisString, field: &RedisString) -> HashFieldExpiryKey {
    HashFieldExpiryKey {
        dbid,
        key: key.clone(),
        field: field.clone(),
    }
}

fn set_field_expiry(dbid: u32, key: &RedisString, field: &RedisString, when_ms: i64) {
    let mut guard = match field_expires().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.insert(expiry_key(dbid, key, field), when_ms);
}

fn remove_field_expiry(dbid: u32, key: &RedisString, field: &RedisString) -> bool {
    let mut guard = match field_expires().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.remove(&expiry_key(dbid, key, field)).is_some()
}

fn clear_hash_expiries(dbid: u32, key: &RedisString) {
    let mut guard = match field_expires().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.retain(|exp, _| !(exp.dbid == dbid && exp.key == *key));
}

pub(crate) fn copy_hash_field_expiries(
    src_dbid: u32,
    src_key: &RedisString,
    dst_dbid: u32,
    dst_key: &RedisString,
) {
    let mut guard = match field_expires().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let copied: Vec<(HashFieldExpiryKey, i64)> = guard
        .iter()
        .filter(|(exp, _)| exp.dbid == src_dbid && exp.key == *src_key)
        .map(|(exp, when)| {
            (
                HashFieldExpiryKey {
                    dbid: dst_dbid,
                    key: dst_key.clone(),
                    field: exp.field.clone(),
                },
                *when,
            )
        })
        .collect();
    guard.retain(|exp, _| !(exp.dbid == dst_dbid && exp.key == *dst_key));
    guard.extend(copied);
}

fn reset_hash_expiry_state_if_db_empty(ctx: &CommandContext) {
    if ctx.db().size() != 0 {
        return;
    }
    let dbid = ctx.selected_db_id();
    let mut guard = match field_expires().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.retain(|exp, _| exp.dbid != dbid);
}

fn field_expiry_ms(dbid: u32, key: &RedisString, field: &RedisString) -> Option<i64> {
    let guard = match field_expires().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.get(&expiry_key(dbid, key, field)).copied()
}

fn purge_expired_hash_fields(ctx: &mut CommandContext, key: &RedisString) -> RedisResult<()> {
    if ctx.live_config().import_mode() {
        return Ok(());
    }
    let dbid = ctx.selected_db_id();
    let now = now_ms();
    let expired_fields: Vec<RedisString> = {
        let guard = match field_expires().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard
            .iter()
            .filter(|(exp, when)| exp.dbid == dbid && exp.key == *key && **when <= now)
            .map(|(exp, _)| exp.field.clone())
            .collect()
    };
    if expired_fields.is_empty() {
        return Ok(());
    }

    let mut removed = 0u64;
    let key_removed = {
        let map = match as_hash_mut(ctx.db_mut().lookup_key_write(key))? {
            None => {
                clear_hash_expiries(dbid, key);
                return Ok(());
            }
            Some(h) => h,
        };
        for field in &expired_fields {
            remove_field_expiry(dbid, key, field);
            if map.remove(field).is_some() {
                removed += 1;
            }
        }
        map.is_empty()
    };
    if key_removed {
        ctx.db_mut().sync_delete(key);
        clear_hash_expiries(dbid, key);
    }
    if removed > 0 {
        EXPIRED_FIELDS.fetch_add(removed, Ordering::Relaxed);
        ctx.notify_keyspace_event(NOTIFY_HASH, b"hexpired", key);
        if key_removed {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", key);
        }
    }
    Ok(())
}

/// Parse a `RedisString` as a base-10 `i64` using Redis' strict rules.
///
/// Rejects leading or trailing whitespace, embedded NUL bytes, and any
/// non-ASCII-digit payload. Returns `Err(RedisError::not_integer())` on
/// any failure to match real Redis' error reply.
fn parse_strict_i64(bytes: &[u8]) -> Result<i64, RedisError> {
    if bytes.is_empty() {
        return Err(RedisError::not_integer());
    }
    let s = core::str::from_utf8(bytes).map_err(|_| RedisError::not_integer())?;
    if s.starts_with(char::is_whitespace) || s.ends_with(char::is_whitespace) {
        return Err(RedisError::not_integer());
    }
    s.parse::<i64>().map_err(|_| RedisError::not_integer())
}

/// Parse a `RedisString` as an `f64` for HINCRBYFLOAT.
///
/// Rejects whitespace, NaN, and infinity to match the C implementation's
/// `getLongDoubleFromObject` rules.
fn parse_strict_f64(bytes: &[u8]) -> Result<f64, RedisError> {
    if bytes.is_empty() {
        return Err(RedisError::not_float());
    }
    let s = core::str::from_utf8(bytes).map_err(|_| RedisError::not_float())?;
    if s.starts_with(char::is_whitespace) || s.ends_with(char::is_whitespace) {
        return Err(RedisError::not_float());
    }
    let v: f64 = s.parse().map_err(|_| RedisError::not_float())?;
    if v.is_nan() || v.is_infinite() {
        return Err(RedisError::not_float());
    }
    Ok(v)
}

/// Borrow the inner hash `HashMap` of a hash-encoded `RedisObject`,
/// raising `WRONGTYPE` if `obj` is any other kind.
///
/// Returns `Ok(None)` if the key is absent so callers can preserve
/// existence semantics without nesting `match` on the lookup result.
fn as_hash_ref(
    obj: Option<&RedisObject>,
) -> Result<Option<&HashMap<RedisString, RedisString>>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => o.hash().map(Some).ok_or_else(RedisError::wrong_type),
    }
}

/// Mutable variant of `as_hash_ref`.
fn as_hash_mut(
    obj: Option<&mut RedisObject>,
) -> Result<Option<&mut HashMap<RedisString, RedisString>>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => {
            if o.is_hash() {
                Ok(o.hash_mut())
            } else {
                Err(RedisError::wrong_type())
            }
        }
    }
}

/// Format an `i64` as ASCII decimal bytes.
fn long_long_to_bytes(n: i64) -> Vec<u8> {
    n.to_string().into_bytes()
}

/// Format an `f64` for HINCRBYFLOAT replies, matching Redis wire output.
///
/// Rust's `Display` for `f64` uses the shortest round-trip decimal, which
/// matches Redis for most values. Scientific notation (e.g. `1e10`) is
/// converted to fixed-point by stripping trailing zeros from a 17-decimal
/// expansion. Redis does not append `.0` to integer-valued floats.
fn float_to_bytes(v: f64) -> Vec<u8> {
    let s = format!("{}", v);
    if s.contains('e') || s.contains('E') {
        let precise = format!("{:.17}", v);
        let trimmed = precise.trim_end_matches('0').trim_end_matches('.');
        if trimmed.is_empty() {
            return b"0".to_vec();
        }
        return trimmed.as_bytes().to_vec();
    }
    s.into_bytes()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HashExpiryKind {
    Ex,
    Px,
    ExAt,
    PxAt,
}

impl HashExpiryKind {
    fn command_name(self) -> &'static [u8] {
        match self {
            HashExpiryKind::Ex => b"hexpire",
            HashExpiryKind::Px => b"hpexpire",
            HashExpiryKind::ExAt => b"hexpireat",
            HashExpiryKind::PxAt => b"hpexpireat",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HashSetExpiry {
    None,
    KeepTtl,
    Set(i64),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct HashExpireFlags {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct HSetExOptions {
    key_nx: bool,
    key_xx: bool,
    field_nx: bool,
    field_xx: bool,
    expiry: HashSetExpiry,
}

impl Default for HashSetExpiry {
    fn default() -> Self {
        Self::None
    }
}

fn expire_time_from(kind: HashExpiryKind, raw: i64) -> i64 {
    match kind {
        HashExpiryKind::Ex => now_ms().saturating_add(raw.saturating_mul(1000)),
        HashExpiryKind::Px => now_ms().saturating_add(raw),
        HashExpiryKind::ExAt => raw.saturating_mul(1000),
        HashExpiryKind::PxAt => raw,
    }
}

fn parse_hash_expiry_kind(bytes: &[u8]) -> Option<HashExpiryKind> {
    if bytes.eq_ignore_ascii_case(b"EX") || bytes.eq_ignore_ascii_case(b"HEXPIRE") {
        Some(HashExpiryKind::Ex)
    } else if bytes.eq_ignore_ascii_case(b"PX") || bytes.eq_ignore_ascii_case(b"HPEXPIRE") {
        Some(HashExpiryKind::Px)
    } else if bytes.eq_ignore_ascii_case(b"EXAT") || bytes.eq_ignore_ascii_case(b"HEXPIREAT") {
        Some(HashExpiryKind::ExAt)
    } else if bytes.eq_ignore_ascii_case(b"PXAT") || bytes.eq_ignore_ascii_case(b"HPEXPIREAT") {
        Some(HashExpiryKind::PxAt)
    } else {
        None
    }
}

fn invalid_expire_time(command: &[u8]) -> RedisError {
    let cmd = String::from_utf8_lossy(command).to_ascii_lowercase();
    RedisError::runtime(format!("ERR invalid expire time in '{}' command", cmd).into_bytes())
}

fn find_fields_index(ctx: &CommandContext, start: usize) -> RedisResult<usize> {
    for idx in start..ctx.arg_count() {
        if ctx.arg(idx)?.as_bytes().eq_ignore_ascii_case(b"FIELDS") {
            return Ok(idx);
        }
    }
    Err(RedisError::syntax(b"syntax error"))
}

fn parse_fields_count(ctx: &CommandContext, fields_idx: usize) -> Result<usize, RedisError> {
    if !ctx
        .arg(fields_idx)?
        .as_bytes()
        .eq_ignore_ascii_case(b"FIELDS")
    {
        return Err(RedisError::syntax(b"syntax error"));
    }
    if fields_idx + 1 >= ctx.arg_count() {
        return Err(RedisError::runtime(
            b"ERR numfields should be greater than 0 and match the provided number of fields",
        ));
    }
    let n = parse_strict_i64(ctx.arg(fields_idx + 1)?.as_bytes())?;
    if n <= 0 {
        return Err(RedisError::runtime(
            b"ERR numfields should be greater than 0 and match the provided number of fields",
        ));
    }
    Ok(n as usize)
}

fn parse_hash_field_list(
    ctx: &CommandContext,
    fields_idx: usize,
) -> Result<Vec<RedisString>, RedisError> {
    let n = parse_fields_count(ctx, fields_idx)?;
    let start = fields_idx + 2;
    if ctx.arg_count() != start + n {
        return Err(RedisError::runtime(
            b"ERR numfields should be greater than 0 and match the provided number of fields",
        ));
    }
    let mut fields = Vec::with_capacity(n);
    for i in start..start + n {
        fields.push(ctx.arg_owned(i)?);
    }
    Ok(fields)
}

fn parse_hash_field_value_list(
    ctx: &CommandContext,
    fields_idx: usize,
) -> Result<Vec<(RedisString, RedisString)>, RedisError> {
    let n = parse_fields_count(ctx, fields_idx)?;
    let start = fields_idx + 2;
    if ctx.arg_count() != start + n * 2 {
        return Err(RedisError::runtime(
            b"ERR numfields should be greater than 0 and match the provided number of fields",
        ));
    }
    let mut pairs = Vec::with_capacity(n);
    let mut i = start;
    while i < start + n * 2 {
        pairs.push((ctx.arg_owned(i)?, ctx.arg_owned(i + 1)?));
        i += 2;
    }
    Ok(pairs)
}

fn parse_expire_time_arg(
    ctx: &CommandContext,
    idx: usize,
    kind: HashExpiryKind,
) -> RedisResult<i64> {
    let raw = parse_strict_i64(ctx.arg(idx)?.as_bytes())?;
    if raw < 0 {
        return Err(invalid_expire_time(ctx.arg(0)?.as_bytes()));
    }
    Ok(expire_time_from(kind, raw))
}

fn parse_hgetex_options(ctx: &CommandContext) -> RedisResult<(HashSetExpiry, Vec<RedisString>)> {
    let fields_idx = find_fields_index(ctx, 2)?;
    let mut expiry = HashSetExpiry::None;
    let mut idx = 2usize;
    while idx < fields_idx {
        let token = ctx.arg(idx)?.as_bytes();
        if token.eq_ignore_ascii_case(b"PERSIST") {
            if !matches!(expiry, HashSetExpiry::None | HashSetExpiry::KeepTtl) {
                return Err(RedisError::syntax(b"syntax error"));
            }
            expiry = HashSetExpiry::KeepTtl;
            idx += 1;
        } else if let Some(kind) = parse_hash_expiry_kind(token) {
            if !matches!(expiry, HashSetExpiry::None) || idx + 1 >= fields_idx {
                return Err(RedisError::syntax(b"syntax error"));
            }
            expiry = HashSetExpiry::Set(parse_expire_time_arg(ctx, idx + 1, kind)?);
            idx += 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
    Ok((expiry, parse_hash_field_list(ctx, fields_idx)?))
}

fn parse_hsetex_options(
    ctx: &CommandContext,
) -> RedisResult<(HSetExOptions, Vec<(RedisString, RedisString)>)> {
    let fields_idx = find_fields_index(ctx, 2)?;
    let mut opts = HSetExOptions::default();
    let mut idx = 2usize;
    while idx < fields_idx {
        let token = ctx.arg(idx)?.as_bytes();
        if token.eq_ignore_ascii_case(b"NX") {
            if opts.key_xx {
                return Err(RedisError::syntax(b"syntax error"));
            }
            opts.key_nx = true;
            idx += 1;
        } else if token.eq_ignore_ascii_case(b"XX") {
            if opts.key_nx {
                return Err(RedisError::syntax(b"syntax error"));
            }
            opts.key_xx = true;
            idx += 1;
        } else if token.eq_ignore_ascii_case(b"FNX") {
            if opts.field_xx {
                return Err(RedisError::syntax(b"syntax error"));
            }
            opts.field_nx = true;
            idx += 1;
        } else if token.eq_ignore_ascii_case(b"FXX") {
            if opts.field_nx {
                return Err(RedisError::syntax(b"syntax error"));
            }
            opts.field_xx = true;
            idx += 1;
        } else if token.eq_ignore_ascii_case(b"KEEPTTL") {
            if !matches!(opts.expiry, HashSetExpiry::None | HashSetExpiry::KeepTtl) {
                return Err(RedisError::syntax(b"syntax error"));
            }
            opts.expiry = HashSetExpiry::KeepTtl;
            idx += 1;
        } else if let Some(kind) = parse_hash_expiry_kind(token) {
            if !matches!(opts.expiry, HashSetExpiry::None) || idx + 1 >= fields_idx {
                return Err(RedisError::syntax(b"syntax error"));
            }
            opts.expiry = HashSetExpiry::Set(parse_expire_time_arg(ctx, idx + 1, kind)?);
            idx += 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
    Ok((opts, parse_hash_field_value_list(ctx, fields_idx)?))
}

fn redis_arg(bytes: &[u8]) -> RedisString {
    RedisString::from_bytes(bytes)
}

fn redis_i64_arg(value: i64) -> RedisString {
    RedisString::from_bytes(value.to_string().as_bytes())
}

fn rewrite_hsetex_for_propagation(
    ctx: &mut CommandContext,
    key: &RedisString,
    opts: HSetExOptions,
    pairs: &[(RedisString, RedisString)],
) {
    let mut argv = Vec::with_capacity(5 + pairs.len() * 2);
    argv.push(redis_arg(b"HSETEX"));
    argv.push(key.clone());

    match opts.expiry {
        HashSetExpiry::None => {}
        HashSetExpiry::KeepTtl => argv.push(redis_arg(b"KEEPTTL")),
        HashSetExpiry::Set(when) => {
            argv.push(redis_arg(b"PXAT"));
            argv.push(redis_i64_arg(when));
        }
    }

    argv.push(redis_arg(b"FIELDS"));
    argv.push(redis_i64_arg(pairs.len() as i64));
    for (field, value) in pairs {
        argv.push(field.clone());
        argv.push(value.clone());
    }

    ctx.client_mut().set_args(argv);
}

fn parse_hash_expire_flags(
    ctx: &CommandContext,
    fields_idx: usize,
) -> RedisResult<HashExpireFlags> {
    let mut flags = HashExpireFlags::default();
    let mut idx = 3usize;
    while idx < fields_idx {
        let token = ctx.arg(idx)?.as_bytes();
        if token.eq_ignore_ascii_case(b"NX") {
            flags.nx = true;
        } else if token.eq_ignore_ascii_case(b"XX") {
            flags.xx = true;
        } else if token.eq_ignore_ascii_case(b"GT") {
            flags.gt = true;
        } else if token.eq_ignore_ascii_case(b"LT") {
            flags.lt = true;
        } else {
            return Err(RedisError::runtime(b"ERR Unsupported option"));
        }
        idx += 1;
    }
    if flags.nx && (flags.xx || flags.gt || flags.lt) {
        return Err(RedisError::runtime(
            b"ERR NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if flags.gt && flags.lt {
        return Err(RedisError::runtime(
            b"ERR GT and LT options at the same time are not compatible",
        ));
    }
    Ok(flags)
}

fn cleanup_hash_after_field_deletes(
    ctx: &mut CommandContext,
    dbid: u32,
    key: &RedisString,
    removed: i64,
    event: &[u8],
) -> RedisResult<()> {
    if removed == 0 {
        return Ok(());
    }
    let key_removed = matches!(
        ctx.db().lookup_key_read(key),
        Some(o) if o.hash().map(|h| h.is_empty()).unwrap_or(false)
    );
    if key_removed {
        ctx.db_mut().sync_delete(key);
        clear_hash_expiries(dbid, key);
    }
    ctx.notify_keyspace_event(NOTIFY_HASH, event, key);
    if key_removed {
        ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", key);
    }
    Ok(())
}

fn reply_optional_values(
    ctx: &mut CommandContext,
    values: Vec<Option<RedisString>>,
) -> RedisResult<()> {
    ctx.reply_array_header(values.len())?;
    for value in values {
        match value {
            Some(v) => ctx.reply_bulk_string(v)?,
            None => ctx.reply_null_bulk()?,
        }
    }
    Ok(())
}

/// HSET key field value [field value ...]
///
/// Returns the number of fields newly added (excludes updates). Creates
/// the hash with the Inline encoding when the key is missing. Errors with
/// `WRONGTYPE` if the key exists but is not a hash, and with the
/// wrong-arity error when the trailing field/value pairs are unbalanced.
pub fn hset_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 || (argc - 2) % 2 != 0 {
        return Err(RedisError::wrong_number_of_args(b"hset"));
    }
    reset_hash_expiry_state_if_db_empty(ctx);
    let key = ctx.arg_owned(1usize)?;
    let key_ref = key.clone();
    let mut pairs: Vec<(RedisString, RedisString)> = Vec::with_capacity((argc - 2) / 2);
    let mut j = 2;
    while j < argc {
        let field = ctx.arg_owned(j)?;
        let value = ctx.arg_owned(j + 1)?;
        pairs.push((field, value));
        j += 2;
    }
    purge_expired_hash_fields(ctx, &key)?;
    let dbid = ctx.selected_db_id();
    let existing = ctx.db_mut().lookup_key_write(&key);
    let added: i64 = match existing {
        None => {
            let mut obj = RedisObject::new_hash();
            let inserted = {
                let map = obj.hash_mut().expect("new_hash constructs an Inline hash");
                let mut count: i64 = 0;
                for (f, v) in pairs {
                    remove_field_expiry(dbid, &key, &f);
                    if map.insert(f, v).is_none() {
                        count += 1;
                    }
                }
                count
            };
            ctx.db_mut().set_key(key, obj, 0);
            inserted
        }
        Some(obj) => {
            if !obj.is_hash() {
                return Err(RedisError::wrong_type());
            }
            let map = obj.hash_mut().expect("is_hash confirms hash encoding");
            let mut count: i64 = 0;
            for (f, v) in pairs {
                remove_field_expiry(dbid, &key, &f);
                if map.insert(f, v).is_none() {
                    count += 1;
                }
            }
            count
        }
    };
    ctx.notify_keyspace_event(NOTIFY_HASH, b"hset", &key_ref);
    ctx.reply_integer(added)
}

/// HMSET key field value [field value ...]
///
/// Deprecated alias of HSET that replies `+OK\r\n` instead of the
/// new-field count.
pub fn hmset_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 || (argc - 2) % 2 != 0 {
        return Err(RedisError::wrong_number_of_args(b"hmset"));
    }
    reset_hash_expiry_state_if_db_empty(ctx);
    let key = ctx.arg_owned(1usize)?;
    let key_ref = key.clone();
    let mut pairs: Vec<(RedisString, RedisString)> = Vec::with_capacity((argc - 2) / 2);
    let mut j = 2;
    while j < argc {
        let field = ctx.arg_owned(j)?;
        let value = ctx.arg_owned(j + 1)?;
        pairs.push((field, value));
        j += 2;
    }
    purge_expired_hash_fields(ctx, &key)?;
    let dbid = ctx.selected_db_id();
    match ctx.db_mut().lookup_key_write(&key) {
        None => {
            let mut obj = RedisObject::new_hash();
            {
                let map = obj.hash_mut().expect("new_hash constructs an Inline hash");
                for (f, v) in pairs {
                    remove_field_expiry(dbid, &key, &f);
                    map.insert(f, v);
                }
            }
            ctx.db_mut().set_key(key, obj, 0);
        }
        Some(obj) => {
            if !obj.is_hash() {
                return Err(RedisError::wrong_type());
            }
            let map = obj.hash_mut().expect("is_hash confirms hash encoding");
            for (f, v) in pairs {
                remove_field_expiry(dbid, &key, &f);
                map.insert(f, v);
            }
        }
    }
    ctx.notify_keyspace_event(NOTIFY_HASH, b"hset", &key_ref);
    ctx.reply_simple_string(b"OK")
}

/// HSETNX key field value
///
/// Sets the field only when it does not exist yet. Replies `:1\r\n` on
/// insert, `:0\r\n` when the field already has a value.
pub fn hsetnx_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"hsetnx"));
    }
    reset_hash_expiry_state_if_db_empty(ctx);
    let key = ctx.arg_owned(1usize)?;
    let key_ref = key.clone();
    let field = ctx.arg_owned(2usize)?;
    let value = ctx.arg_owned(3usize)?;
    purge_expired_hash_fields(ctx, &key)?;
    let dbid = ctx.selected_db_id();
    let inserted: i64 = match ctx.db_mut().lookup_key_write(&key) {
        None => {
            let mut obj = RedisObject::new_hash();
            {
                let map = obj.hash_mut().expect("new_hash constructs an Inline hash");
                remove_field_expiry(dbid, &key, &field);
                map.insert(field, value);
            }
            ctx.db_mut().set_key(key, obj, 0);
            1
        }
        Some(obj) => {
            if !obj.is_hash() {
                return Err(RedisError::wrong_type());
            }
            let map = obj.hash_mut().expect("is_hash confirms hash encoding");
            if map.contains_key(&field) {
                0
            } else {
                remove_field_expiry(dbid, &key, &field);
                map.insert(field, value);
                1
            }
        }
    };
    if inserted == 1 {
        ctx.notify_keyspace_event(NOTIFY_HASH, b"hset", &key_ref);
    }
    ctx.reply_integer(inserted)
}

/// HGET key field
///
/// Replies with the field's bulk-string value, or `$-1\r\n` when the key
/// or field is absent.
pub fn hget_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"hget"));
    }
    let key = ctx.arg_owned(1usize)?;
    let field = ctx.arg_owned(2usize)?;
    purge_expired_hash_fields(ctx, &key)?;
    let value: Option<RedisString> = match as_hash_ref(ctx.db().lookup_key_read(&key))? {
        None => None,
        Some(h) => h.get(&field).cloned(),
    };
    match value {
        None => ctx.reply_null_bulk(),
        Some(v) => ctx.reply_bulk_string(v),
    }
}

/// HMGET key field [field ...]
///
/// Returns an array with one element per requested field. Each element is
/// either a bulk-string value or a nil bulk when the field is missing.
pub fn hmget_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"hmget"));
    }
    let key = ctx.arg_owned(1usize)?;
    let mut fields: Vec<RedisString> = Vec::with_capacity(argc - 2);
    for j in 2..argc {
        fields.push(ctx.arg_owned(j)?);
    }
    purge_expired_hash_fields(ctx, &key)?;
    let mut values: Vec<Option<RedisString>> = Vec::with_capacity(fields.len());
    match as_hash_ref(ctx.db().lookup_key_read(&key))? {
        None => {
            for _ in &fields {
                values.push(None);
            }
        }
        Some(h) => {
            for f in &fields {
                values.push(h.get(f).cloned());
            }
        }
    }
    ctx.reply_array_header(values.len())?;
    for v in values {
        match v {
            None => ctx.reply_null_bulk()?,
            Some(s) => ctx.reply_bulk_string(s)?,
        }
    }
    Ok(())
}

/// HDEL key field [field ...]
///
/// Removes the listed fields and replies with the deletion count. Deletes
/// the key itself when the last field is removed.
pub fn hdel_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"hdel"));
    }
    let key = ctx.arg_owned(1usize)?;
    let mut fields: Vec<RedisString> = Vec::with_capacity(argc - 2);
    for j in 2..argc {
        fields.push(ctx.arg_owned(j)?);
    }
    purge_expired_hash_fields(ctx, &key)?;
    let dbid = ctx.selected_db_id();
    let removed: i64 = {
        let map = match as_hash_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return ctx.reply_integer(0),
            Some(h) => h,
        };
        let mut count: i64 = 0;
        for f in &fields {
            if map.remove(f).is_some() {
                remove_field_expiry(dbid, &key, f);
                count += 1;
            }
        }
        count
    };
    let empty_after = matches!(
        ctx.db().lookup_key_read(&key),
        Some(o) if o.hash().map(|h| h.is_empty()).unwrap_or(false)
    );
    if empty_after {
        ctx.db_mut().sync_delete(&key);
    }
    if removed > 0 {
        ctx.notify_keyspace_event(NOTIFY_HASH, b"hdel", &key);
        if empty_after {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
        }
    }
    ctx.reply_integer(removed)
}

/// HGETDEL key FIELDS num field [field ...]
///
/// Returns an array of the previous values for the requested fields, using nil
/// bulk replies for missing fields, and removes fields that existed. Deletes
/// the hash key when the last field is removed.
pub fn hgetdel_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 5 {
        return Err(RedisError::wrong_number_of_args(b"hgetdel"));
    }

    let key = ctx.arg_owned(1usize)?;
    let num_fields = parse_strict_i64(ctx.arg(3)?.as_bytes())?;
    let field_count = argc - 4;
    if num_fields <= 0 || num_fields as usize != field_count {
        return Err(RedisError::runtime(
            b"ERR numfields should be greater than 0 and match the provided number of fields",
        ));
    }

    let mut fields: Vec<RedisString> = Vec::with_capacity(field_count);
    for j in 4..argc {
        fields.push(ctx.arg_owned(j)?);
    }
    purge_expired_hash_fields(ctx, &key)?;
    let dbid = ctx.selected_db_id();

    let mut removed: i64 = 0;
    let key_removed: bool;
    let values: Vec<Option<RedisString>> = {
        let map = match as_hash_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return reply_hgetdel_values(ctx, vec![None; field_count]),
            Some(h) => h,
        };
        let mut values = Vec::with_capacity(field_count);
        for field in &fields {
            match map.remove(field) {
                Some(value) => {
                    remove_field_expiry(dbid, &key, field);
                    removed += 1;
                    values.push(Some(value));
                }
                None => values.push(None),
            }
        }
        key_removed = removed > 0 && map.is_empty();
        values
    };

    if key_removed {
        ctx.db_mut().sync_delete(&key);
    }
    if removed > 0 {
        ctx.notify_keyspace_event(NOTIFY_HASH, b"hdel", &key);
        if key_removed {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
        }
    }

    reply_hgetdel_values(ctx, values)
}

fn reply_hgetdel_values(
    ctx: &mut CommandContext,
    values: Vec<Option<RedisString>>,
) -> RedisResult<()> {
    ctx.reply_array_header(values.len())?;
    for value in values {
        match value {
            Some(s) => ctx.reply_bulk_string(s)?,
            None => ctx.reply_null_bulk()?,
        }
    }
    Ok(())
}

/// HGETEX key [EX seconds|PX milliseconds|EXAT seconds|PXAT milliseconds|PERSIST]
///        FIELDS num field [field ...]
pub fn hgetex_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 5 {
        return Err(RedisError::wrong_number_of_args(b"hgetex"));
    }
    let key = ctx.arg_owned(1usize)?;
    let (mode, fields) = parse_hgetex_options(ctx)?;
    purge_expired_hash_fields(ctx, &key)?;
    let dbid = ctx.selected_db_id();
    let now = now_ms();

    let mut values = Vec::with_capacity(fields.len());
    let mut changed = 0i64;
    let mut expired = 0i64;
    {
        let map = match as_hash_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return reply_optional_values(ctx, vec![None; fields.len()]),
            Some(h) => h,
        };
        for field in &fields {
            values.push(map.get(field).cloned());
        }
        match mode {
            HashSetExpiry::None => {}
            HashSetExpiry::KeepTtl => {
                for field in &fields {
                    if map.contains_key(field) && remove_field_expiry(dbid, &key, field) {
                        changed += 1;
                    }
                }
            }
            HashSetExpiry::Set(when) if when <= now => {
                for field in &fields {
                    remove_field_expiry(dbid, &key, field);
                    if map.remove(field).is_some() {
                        changed += 1;
                        expired += 1;
                    }
                }
            }
            HashSetExpiry::Set(when) => {
                for field in &fields {
                    if map.contains_key(field) {
                        set_field_expiry(dbid, &key, field, when);
                        changed += 1;
                    }
                }
            }
        }
    }

    match mode {
        HashSetExpiry::KeepTtl if changed > 0 => {
            ctx.notify_keyspace_event(NOTIFY_HASH, b"hpersist", &key);
        }
        HashSetExpiry::Set(when) if changed > 0 && when <= now => {
            EXPIRED_FIELDS.fetch_add(expired as u64, Ordering::Relaxed);
            cleanup_hash_after_field_deletes(ctx, dbid, &key, expired, b"hexpired")?;
        }
        HashSetExpiry::Set(_) if changed > 0 => {
            ctx.notify_keyspace_event(NOTIFY_HASH, b"hexpire", &key);
        }
        _ => {}
    }

    reply_optional_values(ctx, values)
}

/// HSETEX key [NX|XX] [FNX|FXX] [EX seconds|PX milliseconds|EXAT seconds|
/// PXAT milliseconds|KEEPTTL] FIELDS num field value [field value ...]
pub fn hsetex_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 5 {
        return Err(RedisError::wrong_number_of_args(b"hsetex"));
    }
    reset_hash_expiry_state_if_db_empty(ctx);
    let key = ctx.arg_owned(1usize)?;
    let (opts, pairs) = parse_hsetex_options(ctx)?;
    if let Some(obj) = ctx.db().lookup_key_read(&key) {
        if !obj.is_hash() {
            return Err(RedisError::wrong_type());
        }
    }
    purge_expired_hash_fields(ctx, &key)?;

    let dbid = ctx.selected_db_id();
    let now = now_ms();
    let key_exists = ctx.db().lookup_key_read(&key).is_some();
    if (opts.key_nx && key_exists) || (opts.key_xx && !key_exists) {
        return ctx.reply_integer(0);
    }

    if opts.field_nx || opts.field_xx {
        let existing = as_hash_ref(ctx.db().lookup_key_read(&key))?;
        if opts.field_xx && existing.is_none() {
            return ctx.reply_integer(0);
        }
        if let Some(map) = existing {
            for (field, _) in &pairs {
                if (opts.field_nx && map.contains_key(field))
                    || (opts.field_xx && !map.contains_key(field))
                {
                    return ctx.reply_integer(0);
                }
            }
        }
    }

    let import_mode = ctx.live_config().import_mode();
    let expires_now =
        matches!(opts.expiry, HashSetExpiry::Set(when) if when <= now) && !import_mode;
    if expires_now {
        let mut removed_existing = 0i64;
        if let Some(map) = as_hash_mut(ctx.db_mut().lookup_key_write(&key))? {
            for (field, _) in &pairs {
                remove_field_expiry(dbid, &key, field);
                if map.remove(field).is_some() {
                    removed_existing += 1;
                }
            }
        }
        let expired = pairs.len() as i64;
        EXPIRED_FIELDS.fetch_add(expired as u64, Ordering::Relaxed);
        ctx.notify_keyspace_event(NOTIFY_HASH, b"hset", &key);
        ctx.notify_keyspace_event(NOTIFY_HASH, b"hexpire", &key);
        cleanup_hash_after_field_deletes(ctx, dbid, &key, removed_existing, b"hexpired")?;
        if removed_existing == 0 {
            ctx.notify_keyspace_event(NOTIFY_HASH, b"hexpired", &key);
        }
        let mut argv = Vec::with_capacity(2 + pairs.len());
        argv.push(redis_arg(b"HDEL"));
        argv.push(key.clone());
        for (field, _) in &pairs {
            argv.push(field.clone());
        }
        ctx.client_mut().set_args(argv);
        return ctx.reply_integer(1);
    }

    if ctx.db().lookup_key_read(&key).is_none() {
        ctx.db_mut()
            .set_key(key.clone(), RedisObject::new_hash(), 0);
    }
    {
        let map = as_hash_mut(ctx.db_mut().lookup_key_write(&key))?
            .expect("hash was created or already checked");
        for (field, value) in &pairs {
            map.insert(field.clone(), value.clone());
            match opts.expiry {
                HashSetExpiry::None => {
                    remove_field_expiry(dbid, &key, field);
                }
                HashSetExpiry::KeepTtl => {}
                HashSetExpiry::Set(when) => set_field_expiry(dbid, &key, field, when),
            }
        }
    }
    if import_mode && matches!(opts.expiry, HashSetExpiry::Set(when) if when <= now) {
        if let Some(obj) = ctx.db_mut().lookup_key_write(&key) {
            obj.promote_hash_to_hashtable();
        }
    }
    ctx.notify_keyspace_event(NOTIFY_HASH, b"hset", &key);
    if matches!(opts.expiry, HashSetExpiry::Set(_)) {
        ctx.notify_keyspace_event(NOTIFY_HASH, b"hexpire", &key);
    }
    if opts.key_nx
        || opts.key_xx
        || opts.field_nx
        || opts.field_xx
        || matches!(opts.expiry, HashSetExpiry::Set(_))
    {
        rewrite_hsetex_for_propagation(ctx, &key, opts, &pairs);
    }
    ctx.reply_integer(1)
}

/// HEXISTS key field
///
/// Replies `:1\r\n` if the field is present, `:0\r\n` otherwise.
pub fn hexists_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"hexists"));
    }
    let key = ctx.arg_owned(1usize)?;
    let field = ctx.arg_owned(2usize)?;
    purge_expired_hash_fields(ctx, &key)?;
    let present: i64 = match as_hash_ref(ctx.db().lookup_key_read(&key))? {
        None => 0,
        Some(h) => i64::from(h.contains_key(&field)),
    };
    ctx.reply_integer(present)
}

/// HLEN key
///
/// Replies with the number of fields stored under the key, `:0\r\n` if the
/// key is missing.
pub fn hlen_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"hlen"));
    }
    let key = ctx.arg_owned(1usize)?;
    purge_expired_hash_fields(ctx, &key)?;
    let len: i64 = match as_hash_ref(ctx.db().lookup_key_read(&key))? {
        None => 0,
        Some(h) => h.len() as i64,
    };
    ctx.reply_integer(len)
}

/// HSTRLEN key field
///
/// Replies with the byte length of the field's value, `:0\r\n` if the key
/// or field is absent.
pub fn hstrlen_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"hstrlen"));
    }
    let key = ctx.arg_owned(1usize)?;
    let field = ctx.arg_owned(2usize)?;
    purge_expired_hash_fields(ctx, &key)?;
    let len: i64 = match as_hash_ref(ctx.db().lookup_key_read(&key))? {
        None => 0,
        Some(h) => h
            .get(&field)
            .map(|v| v.as_bytes().len() as i64)
            .unwrap_or(0),
    };
    ctx.reply_integer(len)
}

/// HGETALL key
///
/// Replies with a flat array of alternating field, value bulk strings.
/// `*0\r\n` for a missing key. The emission order follows the underlying
/// `HashMap` iteration order, which the wire-diff oracle treats as
/// non-deterministic.
pub fn hgetall_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"hgetall"));
    }
    let key = ctx.arg_owned(1usize)?;
    purge_expired_hash_fields(ctx, &key)?;
    let pairs: Vec<(RedisString, RedisString)> = match as_hash_ref(ctx.db().lookup_key_read(&key))?
    {
        None => Vec::new(),
        Some(h) => h.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
    };
    ctx.reply_map_header(pairs.len())?;
    for (f, v) in pairs {
        ctx.reply_bulk_string(f)?;
        ctx.reply_bulk_string(v)?;
    }
    Ok(())
}

/// HKEYS key
///
/// Replies with the array of field names, `*0\r\n` if the key is absent.
pub fn hkeys_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"hkeys"));
    }
    let key = ctx.arg_owned(1usize)?;
    purge_expired_hash_fields(ctx, &key)?;
    let fields: Vec<RedisString> = match as_hash_ref(ctx.db().lookup_key_read(&key))? {
        None => Vec::new(),
        Some(h) => h.keys().cloned().collect(),
    };
    ctx.reply_array_header(fields.len())?;
    for f in fields {
        ctx.reply_bulk_string(f)?;
    }
    Ok(())
}

/// HVALS key
///
/// Replies with the array of field values, `*0\r\n` if the key is absent.
pub fn hvals_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"hvals"));
    }
    let key = ctx.arg_owned(1usize)?;
    purge_expired_hash_fields(ctx, &key)?;
    let values: Vec<RedisString> = match as_hash_ref(ctx.db().lookup_key_read(&key))? {
        None => Vec::new(),
        Some(h) => h.values().cloned().collect(),
    };
    ctx.reply_array_header(values.len())?;
    for v in values {
        ctx.reply_bulk_string(v)?;
    }
    Ok(())
}

/// HINCRBY key field delta
///
/// Reads the field as a strict `i64`, adds the signed delta with overflow
/// checking, stores the result back as ASCII decimal, and replies with the
/// new value. Missing fields start at 0; non-integer existing values raise
/// the canonical Redis error, mirroring t_string.c's incr family.
pub fn hincrby_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"hincrby"));
    }
    reset_hash_expiry_state_if_db_empty(ctx);
    let key = ctx.arg_owned(1usize)?;
    let field = ctx.arg_owned(2usize)?;
    let delta = parse_strict_i64(ctx.arg(3)?.as_bytes())?;
    purge_expired_hash_fields(ctx, &key)?;
    let key_existed = ctx.db().lookup_key_read(&key).is_some();
    let next: i64 = if key_existed {
        let map = match as_hash_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => unreachable!("key_existed implies a live entry"),
            Some(h) => h,
        };
        let current: i64 = match map.get(&field) {
            None => 0,
            Some(v) => parse_strict_i64(v.as_bytes())
                .map_err(|_| RedisError::runtime(b"ERR hash value is not an integer"))?,
        };
        let next = match current.checked_add(delta) {
            Some(v) => v,
            None => {
                return Err(RedisError::runtime(
                    b"ERR increment or decrement would overflow",
                ))
            }
        };
        map.insert(field, RedisString::from_bytes(&long_long_to_bytes(next)));
        next
    } else {
        let mut obj = RedisObject::new_hash();
        {
            let map = obj.hash_mut().expect("new_hash constructs an Inline hash");
            map.insert(field, RedisString::from_bytes(&long_long_to_bytes(delta)));
        }
        ctx.db_mut().set_key(key.clone(), obj, 0);
        delta
    };
    ctx.notify_keyspace_event(NOTIFY_HASH, b"hincrby", &key);
    ctx.reply_integer(next)
}

/// HINCRBYFLOAT key field delta
///
/// Reads the field as a strict `f64`, adds the delta, stores the result
/// back formatted, and replies with the new value as a bulk string.
pub fn hincrbyfloat_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"hincrbyfloat"));
    }
    reset_hash_expiry_state_if_db_empty(ctx);
    let key = ctx.arg_owned(1usize)?;
    let field = ctx.arg_owned(2usize)?;
    let delta = parse_strict_f64(ctx.arg(3)?.as_bytes())?;
    purge_expired_hash_fields(ctx, &key)?;
    let key_existed = ctx.db().lookup_key_read(&key).is_some();
    let result_bytes: Vec<u8> = if key_existed {
        let map = match as_hash_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => unreachable!("key_existed implies a live entry"),
            Some(h) => h,
        };
        let current: f64 = match map.get(&field) {
            None => 0.0,
            Some(v) => parse_strict_f64(v.as_bytes())?,
        };
        let next = current + delta;
        if next.is_nan() || next.is_infinite() {
            return Err(RedisError::runtime(
                b"ERR increment would produce NaN or Infinity",
            ));
        }
        let bytes = float_to_bytes(next);
        map.insert(field, RedisString::from_bytes(&bytes));
        bytes
    } else {
        let mut obj = RedisObject::new_hash();
        let bytes = float_to_bytes(delta);
        {
            let map = obj.hash_mut().expect("new_hash constructs an Inline hash");
            map.insert(field, RedisString::from_bytes(&bytes));
        }
        ctx.db_mut().set_key(key.clone(), obj, 0);
        bytes
    };
    ctx.notify_keyspace_event(NOTIFY_HASH, b"hincrbyfloat", &key);
    ctx.reply_bulk_string(RedisString::from_vec(result_bytes))
}

/// Parse an HRANDFIELD count argument, applying the Redis range rules.
///
/// Redis uses `getRangeLongFromObjectOrReply(c, argv, -LONG_MAX, LONG_MAX, ...)`
/// which excludes `i64::MIN` (equal to `LONG_MIN`, one below `-LONG_MAX`).
/// Values inside that range are accepted as-is.
fn parse_hrandfield_count(bytes: &[u8]) -> Result<i64, RedisError> {
    let v = parse_strict_i64(bytes)?;
    if v == i64::MIN {
        return Err(RedisError::out_of_range());
    }
    Ok(v)
}

/// Derive a pseudo-random seed from wall-clock nanoseconds and a client id.
///
/// This is a cheap xorshift mix — not cryptographic, but good enough to
/// spread access across all hash fields within a test run. The per-call
/// clock read ensures the same client gets a different seed on every call.
fn pseudo_random_seed(client_id: u64) -> u64 {
    let ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let mut x = ns ^ client_id.wrapping_add(1);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// HRANDFIELD key [count [WITHVALUES]]
///
/// Without count: replies a single random field bulk, or `$-1\r\n` when
/// the key is absent. With count: replies an array. Positive counts emit
/// up to `count` distinct fields, negative counts emit `|count|` fields
/// allowing duplicates. With `WITHVALUES` the elements alternate
/// field/value bulks in RESP2, and nest as 2-element arrays in RESP3.
pub fn hrandfield_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 || argc > 4 {
        return Err(RedisError::wrong_number_of_args(b"hrandfield"));
    }
    let key = ctx.arg_owned(1usize)?;
    purge_expired_hash_fields(ctx, &key)?;
    let count_opt: Option<i64> = if argc >= 3 {
        Some(parse_hrandfield_count(ctx.arg(2)?.as_bytes())?)
    } else {
        None
    };
    let with_values: bool = if argc == 4 {
        if !ctx.arg(3)?.as_bytes().eq_ignore_ascii_case(b"withvalues") {
            return Err(RedisError::syntax(b"syntax error"));
        }
        true
    } else {
        false
    };
    if argc == 4 && count_opt.is_none() {
        return Err(RedisError::syntax(b"syntax error"));
    }

    if let Some(count) = count_opt {
        if with_values && (count < -(i64::MAX / 2) || count > i64::MAX / 2) {
            return Err(RedisError::runtime(b"ERR value is out of range"));
        }
    }

    let resp3 = ctx.client.resp_proto == 3;
    let client_id = ctx.client.id;

    let pairs: Vec<(RedisString, RedisString)> = match as_hash_ref(ctx.db().lookup_key_read(&key))?
    {
        None => Vec::new(),
        Some(h) => h.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
    };

    match count_opt {
        None => {
            if pairs.is_empty() {
                return ctx.reply_null_bulk();
            }
            let idx = (pseudo_random_seed(client_id) as usize) % pairs.len();
            let (f, _) = &pairs[idx];
            ctx.reply_bulk_string(f.clone())
        }
        Some(count) => {
            if pairs.is_empty() || count == 0 {
                return ctx.reply_array_header(0usize);
            }
            let seed = pseudo_random_seed(client_id);
            let mut emitted: Vec<(RedisString, RedisString)> = Vec::new();
            if count > 0 {
                let take = (count as usize).min(pairs.len());
                let start = (seed as usize) % pairs.len();
                for i in 0..take {
                    emitted.push(pairs[(start + i) % pairs.len()].clone());
                }
            } else {
                let take = count.unsigned_abs() as usize;
                let start = (seed as usize) % pairs.len();
                for i in 0..take {
                    emitted.push(pairs[(start + i) % pairs.len()].clone());
                }
            }
            if with_values && resp3 {
                ctx.reply_array_header(emitted.len())?;
                for (f, v) in emitted {
                    ctx.reply_array_header(2usize)?;
                    ctx.reply_bulk_string(f)?;
                    ctx.reply_bulk_string(v)?;
                }
            } else if with_values {
                ctx.reply_array_header(emitted.len() * 2)?;
                for (f, v) in emitted {
                    ctx.reply_bulk_string(f)?;
                    ctx.reply_bulk_string(v)?;
                }
            } else {
                ctx.reply_array_header(emitted.len())?;
                for (f, _v) in emitted {
                    ctx.reply_bulk_string(f)?;
                }
            }
            Ok(())
        }
    }
}

fn hash_expire_command(ctx: &mut CommandContext, kind: HashExpiryKind) -> RedisResult<()> {
    if ctx.arg_count() < 6 {
        return Err(RedisError::wrong_number_of_args(kind.command_name()));
    }
    let key = ctx.arg_owned(1usize)?;
    let when = parse_expire_time_arg(ctx, 2, kind)?;
    let fields_idx = find_fields_index(ctx, 3)?;
    let flags = parse_hash_expire_flags(ctx, fields_idx)?;
    let fields = parse_hash_field_list(ctx, fields_idx)?;
    purge_expired_hash_fields(ctx, &key)?;

    let dbid = ctx.selected_db_id();
    let now = now_ms();
    let mut results = Vec::with_capacity(fields.len());
    let mut updated = 0i64;
    let mut expired = 0i64;
    {
        let map = match as_hash_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => {
                results.resize(fields.len(), -2);
                return reply_integer_array(ctx, &results);
            }
            Some(h) => h,
        };
        for field in &fields {
            if !map.contains_key(field) {
                results.push(-2);
                continue;
            }
            let current = field_expiry_ms(dbid, &key, field);
            if flags.nx && current.is_some() {
                results.push(0);
                continue;
            }
            if flags.xx && current.is_none() {
                results.push(0);
                continue;
            }
            if flags.gt && current.map_or(true, |old| when <= old) {
                results.push(0);
                continue;
            }
            if flags.lt && current.is_some_and(|old| when >= old) {
                results.push(0);
                continue;
            }
            if when <= now {
                remove_field_expiry(dbid, &key, field);
                map.remove(field);
                expired += 1;
                results.push(2);
            } else {
                set_field_expiry(dbid, &key, field, when);
                updated += 1;
                results.push(1);
            }
        }
    }

    if expired > 0 {
        EXPIRED_FIELDS.fetch_add(expired as u64, Ordering::Relaxed);
        cleanup_hash_after_field_deletes(ctx, dbid, &key, expired, b"hexpired")?;
    } else if updated > 0 {
        ctx.notify_keyspace_event(NOTIFY_HASH, b"hexpire", &key);
    }
    reply_integer_array(ctx, &results)
}

pub fn hexpire_command(ctx: &mut CommandContext) -> RedisResult<()> {
    hash_expire_command(ctx, HashExpiryKind::Ex)
}

pub fn hpexpire_command(ctx: &mut CommandContext) -> RedisResult<()> {
    hash_expire_command(ctx, HashExpiryKind::Px)
}

pub fn hexpireat_command(ctx: &mut CommandContext) -> RedisResult<()> {
    hash_expire_command(ctx, HashExpiryKind::ExAt)
}

pub fn hpexpireat_command(ctx: &mut CommandContext) -> RedisResult<()> {
    hash_expire_command(ctx, HashExpiryKind::PxAt)
}

pub fn hpersist_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 5 {
        return Err(RedisError::wrong_number_of_args(b"hpersist"));
    }
    let key = ctx.arg_owned(1usize)?;
    let fields = parse_hash_field_list(ctx, 2)?;
    purge_expired_hash_fields(ctx, &key)?;
    let dbid = ctx.selected_db_id();
    let mut results = Vec::with_capacity(fields.len());
    let mut changed = 0i64;
    match as_hash_ref(ctx.db().lookup_key_read(&key))? {
        None => results.resize(fields.len(), -2),
        Some(map) => {
            for field in &fields {
                if !map.contains_key(field) {
                    results.push(-2);
                } else if remove_field_expiry(dbid, &key, field) {
                    changed += 1;
                    results.push(1);
                } else {
                    results.push(-1);
                }
            }
        }
    }
    if changed > 0 {
        ctx.notify_keyspace_event(NOTIFY_HASH, b"hpersist", &key);
    }
    reply_integer_array(ctx, &results)
}

#[derive(Clone, Copy)]
enum HashTtlMode {
    TtlSeconds,
    TtlMilliseconds,
    ExpireTimeSeconds,
    ExpireTimeMilliseconds,
}

impl HashTtlMode {
    fn command_name(self) -> &'static [u8] {
        match self {
            HashTtlMode::TtlSeconds => b"httl",
            HashTtlMode::TtlMilliseconds => b"hpttl",
            HashTtlMode::ExpireTimeSeconds => b"hexpiretime",
            HashTtlMode::ExpireTimeMilliseconds => b"hpexpiretime",
        }
    }
}

fn hash_ttl_command(ctx: &mut CommandContext, mode: HashTtlMode) -> RedisResult<()> {
    if ctx.arg_count() < 5 {
        return Err(RedisError::wrong_number_of_args(mode.command_name()));
    }
    let key = ctx.arg_owned(1usize)?;
    let fields = parse_hash_field_list(ctx, 2)?;
    purge_expired_hash_fields(ctx, &key)?;
    let dbid = ctx.selected_db_id();
    let now = now_ms();
    let mut results = Vec::with_capacity(fields.len());
    match as_hash_ref(ctx.db().lookup_key_read(&key))? {
        None => results.resize(fields.len(), -2),
        Some(map) => {
            for field in &fields {
                if !map.contains_key(field) {
                    results.push(-2);
                    continue;
                }
                match field_expiry_ms(dbid, &key, field) {
                    None => results.push(-1),
                    Some(when) => {
                        let value = match mode {
                            HashTtlMode::TtlMilliseconds => when.saturating_sub(now).max(0),
                            HashTtlMode::TtlSeconds => {
                                (when.saturating_sub(now).max(0) + 500) / 1000
                            }
                            HashTtlMode::ExpireTimeMilliseconds => when,
                            HashTtlMode::ExpireTimeSeconds => (when + 500) / 1000,
                        };
                        results.push(value);
                    }
                }
            }
        }
    }
    reply_integer_array(ctx, &results)
}

pub fn httl_command(ctx: &mut CommandContext) -> RedisResult<()> {
    hash_ttl_command(ctx, HashTtlMode::TtlSeconds)
}

pub fn hpttl_command(ctx: &mut CommandContext) -> RedisResult<()> {
    hash_ttl_command(ctx, HashTtlMode::TtlMilliseconds)
}

pub fn hexpiretime_command(ctx: &mut CommandContext) -> RedisResult<()> {
    hash_ttl_command(ctx, HashTtlMode::ExpireTimeSeconds)
}

pub fn hpexpiretime_command(ctx: &mut CommandContext) -> RedisResult<()> {
    hash_ttl_command(ctx, HashTtlMode::ExpireTimeMilliseconds)
}

fn reply_integer_array(ctx: &mut CommandContext, values: &[i64]) -> RedisResult<()> {
    ctx.reply_array_header(values.len())?;
    for value in values {
        ctx.reply_integer(*value)?;
    }
    Ok(())
}

/// HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
///
/// Linear-cursor iteration over the field/value pairs of a hash. Matches
/// real Redis's reply shape — a two-element array of `[next_cursor, items]`
/// — but uses the simplified Phase-B cursor scheme (a `u64` offset into a
/// snapshot). `NOVALUES` (Redis 7.4+) emits only the field bulks instead
/// of interleaved field/value pairs.
///
/// TODO(architect): swap for the resize-safe reverse-binary cursor once
/// the kvstore primitive lands.
pub fn hscan_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"hscan"));
    }
    let key = ctx.arg_owned(1usize)?;
    let cursor = parse_u64_cursor(ctx.arg(2)?.as_bytes())?;
    purge_expired_hash_fields(ctx, &key)?;

    let mut pattern: Option<Vec<u8>> = None;
    let mut count: i64 = 10;
    let mut no_values = false;
    let mut j = 3usize;
    while j < argc {
        let opt = ctx.arg(j)?;
        let bytes = opt.as_bytes();
        if bytes.eq_ignore_ascii_case(b"MATCH") {
            if j + 1 >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            pattern = Some(ctx.arg(j + 1)?.as_bytes().to_vec());
            j += 2;
        } else if bytes.eq_ignore_ascii_case(b"COUNT") {
            if j + 1 >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let n = parse_strict_i64(ctx.arg(j + 1)?.as_bytes())?;
            if n < 1 {
                return Err(RedisError::syntax(b"syntax error"));
            }
            count = n;
            j += 2;
        } else if bytes.eq_ignore_ascii_case(b"NOVALUES") {
            no_values = true;
            j += 1;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    let pairs: Vec<(RedisString, RedisString)> = match as_hash_ref(ctx.db().lookup_key_read(&key))?
    {
        None => Vec::new(),
        Some(h) => h.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
    };
    let total = pairs.len() as u64;
    let start = cursor as usize;
    let stop = (start + count as usize).min(pairs.len());
    let next_cursor: u64 = if stop as u64 >= total { 0 } else { stop as u64 };

    let mut matched: Vec<(RedisString, RedisString)> = Vec::new();
    for (f, v) in pairs.into_iter().skip(start).take(count as usize) {
        if let Some(ref pat) = pattern {
            if !glob_match(pat, f.as_bytes()) {
                continue;
            }
        }
        matched.push((f, v));
    }

    ctx.reply_array_header(2usize)?;
    ctx.reply_bulk(next_cursor.to_string().as_bytes())?;
    let header = if no_values {
        matched.len()
    } else {
        matched.len() * 2
    };
    ctx.reply_array_header(header)?;
    for (f, v) in matched {
        ctx.reply_bulk_string(f)?;
        if !no_values {
            ctx.reply_bulk_string(v)?;
        }
    }
    Ok(())
}

/// Parse a `RedisString` as an unsigned cursor value.
fn parse_u64_cursor(bytes: &[u8]) -> Result<u64, RedisError> {
    if bytes.is_empty() {
        return Err(RedisError::runtime(b"ERR invalid cursor"));
    }
    let mut n: u64 = 0;
    for &c in bytes {
        if !c.is_ascii_digit() {
            return Err(RedisError::runtime(b"ERR invalid cursor"));
        }
        n = n
            .checked_mul(10)
            .and_then(|v| v.checked_add((c - b'0') as u64))
            .ok_or_else(|| RedisError::runtime(b"ERR invalid cursor"))?;
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_core::client::Client;
    use redis_core::db::RedisDb;

    fn rs(bytes: &[u8]) -> RedisString {
        RedisString::from_bytes(bytes)
    }

    fn hash_obj(pairs: &[(&[u8], &[u8])]) -> RedisObject {
        let mut obj = RedisObject::new_hash();
        {
            let map = obj.hash_mut().expect("new_hash constructs a hash");
            for (field, value) in pairs {
                map.insert(rs(field), rs(value));
            }
        }
        obj
    }

    fn run_hgetdel(args: &[&[u8]], db: &mut RedisDb) -> (RedisResult<()>, Vec<u8>) {
        let mut client = Client::new(1);
        client.set_args(args.iter().map(|arg| rs(arg)).collect());
        let result = {
            let mut ctx = CommandContext::with_db(&mut client, db);
            hgetdel_command(&mut ctx)
        };
        let reply = client.drain_reply();
        (result, reply)
    }

    #[test]
    fn hgetdel_returns_values_and_deletes_empty_hash() {
        let key = rs(b"myhash");
        let mut db = RedisDb::new(0);
        db.set_key(
            key.clone(),
            hash_obj(&[
                (b"a".as_slice(), b"1".as_slice()),
                (b"b".as_slice(), b"2".as_slice()),
            ]),
            0,
        );

        let (result, reply) = run_hgetdel(
            &[
                b"HGETDEL".as_slice(),
                b"myhash".as_slice(),
                b"FIELDS".as_slice(),
                b"3".as_slice(),
                b"a".as_slice(),
                b"missing".as_slice(),
                b"b".as_slice(),
            ],
            &mut db,
        );

        assert!(result.is_ok(), "unexpected error: {:?}", result);
        assert_eq!(reply, b"*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n");
        assert!(db.lookup_key_read(&key).is_none());
    }

    #[test]
    fn hgetdel_missing_key_returns_nil_array() {
        let mut db = RedisDb::new(0);

        let (result, reply) = run_hgetdel(
            &[
                b"HGETDEL".as_slice(),
                b"missing".as_slice(),
                b"FIELDS".as_slice(),
                b"2".as_slice(),
                b"a".as_slice(),
                b"b".as_slice(),
            ],
            &mut db,
        );

        assert!(result.is_ok(), "unexpected error: {:?}", result);
        assert_eq!(reply, b"*2\r\n$-1\r\n$-1\r\n");
    }

    #[test]
    fn hgetdel_rejects_wrong_type_before_reply() {
        let key = rs(b"wrongtype");
        let mut db = RedisDb::new(0);
        db.set_key(key, RedisObject::new_raw_string(b"value"), 0);

        let (result, reply) = run_hgetdel(
            &[
                b"HGETDEL".as_slice(),
                b"wrongtype".as_slice(),
                b"FIELDS".as_slice(),
                b"1".as_slice(),
                b"a".as_slice(),
            ],
            &mut db,
        );

        assert!(matches!(result, Err(RedisError::WrongType)));
        assert!(reply.is_empty());
    }

    #[test]
    fn hgetdel_rejects_numfields_mismatch() {
        let mut db = RedisDb::new(0);

        let (result, reply) = run_hgetdel(
            &[
                b"HGETDEL".as_slice(),
                b"myhash".as_slice(),
                b"FIELDS".as_slice(),
                b"2".as_slice(),
                b"a".as_slice(),
            ],
            &mut db,
        );

        assert_eq!(
            result.unwrap_err(),
            RedisError::runtime(
                b"ERR numfields should be greater than 0 and match the provided number of fields",
            )
        );
        assert!(reply.is_empty());
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/t_hash.c
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         3
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Hash commands use pragmatic HashEncoding::Inline storage.
//                  Basic hash field expiry commands are backed by a side table
//                  pending object-owned HASH_2 metadata. Active expiry,
//                  replication/AOF rewrite parity, true HRANDFIELD randomness,
//                  and long-double HINCRBYFLOAT parity remain TODO.
// ──────────────────────────────────────────────────────────────────────────
