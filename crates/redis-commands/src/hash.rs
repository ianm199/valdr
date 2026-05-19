//! Hash type and command implementations.
//!
//! Covers the byte-exact wire surface of HSET, HSETNX, HGET, HDEL, HEXISTS,
//! HLEN, HSTRLEN, HGETALL, HKEYS, HVALS, HMGET, HMSET, HINCRBY,
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
//! TODO(architect): HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT /
//! HEXPIRETIME / HPEXPIRETIME / HPERSIST / HTTL / HPTTL family needs the
//! per-field expiry data model from t_hash.c — defer to Phase 5.
//!
//! TODO(port): HINCRBYFLOAT formats the result with Rust's default
//! f64 `{}` formatter rather than the C long-double `%.17Lg` routine.
//! Sufficient for byte-exact diffs on small magnitudes but should be
//! tightened once a dedicated float-to-string helper exists.

use std::collections::HashMap;
use std::time::SystemTime;

use redis_core::command_context::CommandContext;
use redis_core::db::glob_match;
use redis_core::notify::{NOTIFY_GENERIC, NOTIFY_HASH};
use redis_core::object::RedisObject;
use redis_types::{RedisError, RedisResult, RedisString};

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
    let existing = ctx.db_mut().lookup_key_write(&key);
    let added: i64 = match existing {
        None => {
            let mut obj = RedisObject::new_hash();
            let inserted = {
                let map = obj
                    .hash_mut()
                    .expect("new_hash constructs an Inline hash");
                let mut count: i64 = 0;
                for (f, v) in pairs {
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
    match ctx.db_mut().lookup_key_write(&key) {
        None => {
            let mut obj = RedisObject::new_hash();
            {
                let map = obj
                    .hash_mut()
                    .expect("new_hash constructs an Inline hash");
                for (f, v) in pairs {
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
    let key = ctx.arg_owned(1usize)?;
    let key_ref = key.clone();
    let field = ctx.arg_owned(2usize)?;
    let value = ctx.arg_owned(3usize)?;
    let inserted: i64 = match ctx.db_mut().lookup_key_write(&key) {
        None => {
            let mut obj = RedisObject::new_hash();
            {
                let map = obj
                    .hash_mut()
                    .expect("new_hash constructs an Inline hash");
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
    let removed: i64 = {
        let map = match as_hash_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return ctx.reply_integer(0),
            Some(h) => h,
        };
        let mut count: i64 = 0;
        for f in &fields {
            if map.remove(f).is_some() {
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

/// HEXISTS key field
///
/// Replies `:1\r\n` if the field is present, `:0\r\n` otherwise.
pub fn hexists_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"hexists"));
    }
    let key = ctx.arg_owned(1usize)?;
    let field = ctx.arg_owned(2usize)?;
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
    let len: i64 = match as_hash_ref(ctx.db().lookup_key_read(&key))? {
        None => 0,
        Some(h) => h.get(&field).map(|v| v.as_bytes().len() as i64).unwrap_or(0),
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
    let key = ctx.arg_owned(1usize)?;
    let field = ctx.arg_owned(2usize)?;
    let delta = parse_strict_i64(ctx.arg(3)?.as_bytes())?;
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
            let map = obj
                .hash_mut()
                .expect("new_hash constructs an Inline hash");
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
    let key = ctx.arg_owned(1usize)?;
    let field = ctx.arg_owned(2usize)?;
    let delta = parse_strict_f64(ctx.arg(3)?.as_bytes())?;
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
            return Err(RedisError::runtime(b"ERR increment would produce NaN or Infinity"));
        }
        let bytes = float_to_bytes(next);
        map.insert(field, RedisString::from_bytes(&bytes));
        bytes
    } else {
        let mut obj = RedisObject::new_hash();
        let bytes = float_to_bytes(delta);
        {
            let map = obj
                .hash_mut()
                .expect("new_hash constructs an Inline hash");
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
    let header = if no_values { matched.len() } else { matched.len() * 2 };
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

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/t_hash.c
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         4
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Round 3 byte-exact implementations for HSET, HSETNX,
//                  HGET, HDEL, HEXISTS, HLEN, HSTRLEN, HGETALL, HKEYS,
//                  HVALS, HMGET, HMSET, HINCRBY, HINCRBYFLOAT, and
//                  HRANDFIELD backed by the pragmatic HashEncoding::Inline
//                  encoding from redis-core::object. HSCAN, HEXPIRE
//                  family, true HRANDFIELD randomness, and long-double
//                  HINCRBYFLOAT parity remain TODO. Phase 4 will swap
//                  Inline for the real ListPack / HashTable encodings
//                  from redis-ds.
// ──────────────────────────────────────────────────────────────────────────
