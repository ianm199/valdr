//! Sorted-set (`zset`) command implementations.
//!
//! Covers the byte-exact wire surface of ZADD, ZSCORE, ZMSCORE, ZCARD,
//! ZINCRBY, ZRANGE, ZRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZRANK,
//! ZREVRANK, ZREM, ZCOUNT, ZPOPMIN, ZPOPMAX, ZREMRANGEBYRANK, and
//! ZREMRANGEBYSCORE for Round 5.
//!
//! C source: `reference/valkey/src/t_zset.c`.
//!
//! # Storage shape
//!
//! Round 5 uses the pragmatic `ObjectKind::ZSet(ZSetEncoding::Inline(_))`
//! encoding from `redis-core::object` — an `InlineZSet` whose dual
//! `HashMap` + `BTreeSet` mirror the dict + zskiplist pair in real
//! Redis. Phase 4 swaps this for the real `redis_ds::ZSet` once that
//! crate ships the listpack / skiplist primitives.
//!
//! # Architect items
//!
//! TODO(architect): swap the `Inline` encoding for real `ListPack` /
//! `SkipList` types from `redis-ds` once Phase 4 makes them usable.
//!
//! TODO(architect): score formatter parity — Rust's default
//! `f64::to_string` differs from C's `humanfriendly_number_to_string`
//! (`%.17g` + trailing-zero trim) on some edge cases. The smoke corpus
//! sticks to scores whose representation matches under both formatters.
//!
//! TODO(architect): ZRANGEBYLEX / ZREVRANGEBYLEX / ZLEXCOUNT /
//! ZREMRANGEBYLEX / ZRANGESTORE / ZUNIONSTORE / ZINTERSTORE /
//! ZDIFFSTORE / ZUNION / ZINTER / ZDIFF / ZINTERCARD / ZRANDMEMBER /
//! ZMPOP / BZPOPMIN / BZPOPMAX land in follow-on rounds.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use redis_core::blocked_keys::{
    blocked_keys_index, deadline_from_timeout_secs, BlockedAction, BlockedWaiter,
};
use redis_core::command_context::CommandContext;
use redis_core::db::{glob_match, RedisDb};
use redis_core::notify::{NOTIFY_GENERIC, NOTIFY_ZSET};
use redis_core::object::{InlineZSet, RedisObject};
use redis_types::{RedisError, RedisResult, RedisString};

/// Parse a score expressed in Redis's float syntax.
///
/// Accepts ASCII decimal, scientific notation, and `+inf` / `-inf` /
/// `inf` (case-insensitive). Rejects NaN, whitespace, empty strings,
/// and any trailing garbage with the canonical Redis error reply.
fn parse_score(bytes: &[u8]) -> Result<f64, RedisError> {
    if bytes.is_empty() {
        return Err(RedisError::not_float());
    }
    let s = core::str::from_utf8(bytes).map_err(|_| RedisError::not_float())?;
    if s.starts_with(char::is_whitespace) || s.ends_with(char::is_whitespace) {
        return Err(RedisError::not_float());
    }
    let lower = s.to_ascii_lowercase();
    if lower == "inf" || lower == "+inf" || lower == "infinity" || lower == "+infinity" {
        return Ok(f64::INFINITY);
    }
    if lower == "-inf" || lower == "-infinity" {
        return Ok(f64::NEG_INFINITY);
    }
    if lower == "nan" || lower == "+nan" || lower == "-nan" {
        return Err(RedisError::not_float());
    }
    let v: f64 = s.parse().map_err(|_| RedisError::not_float())?;
    if v.is_nan() {
        return Err(RedisError::not_float());
    }
    Ok(v)
}

/// Parse a strict base-10 `i64` matching Redis's accept rules.
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

/// Parse one side of a score range, handling the exclusive `(` prefix.
///
/// Returns `(score, exclusive)`.
fn parse_score_range(bytes: &[u8]) -> Result<(f64, bool), RedisError> {
    let (excl, rest) = match bytes.first() {
        Some(b'(') => (true, &bytes[1..]),
        _ => (false, bytes),
    };
    let score =
        parse_score(rest).map_err(|_| RedisError::runtime(b"ERR min or max is not a float"))?;
    Ok((score, excl))
}

/// Format a score for bulk-string replies.
///
/// Uses Rust's default `f64::to_string` plus an explicit `inf` / `-inf`
/// short-form to match Redis's `humanfriendly_number_to_string` output.
///
/// TODO(architect): full `%.17g` parity once a dedicated formatter
/// helper is wired in.
fn format_score(score: f64) -> Vec<u8> {
    if score.is_infinite() {
        if score > 0.0 {
            b"inf".to_vec()
        } else {
            b"-inf".to_vec()
        }
    } else if score == 0.0 {
        b"0".to_vec()
    } else if score == score.trunc() && score.abs() < 1e17 {
        format!("{}", score as i64).into_bytes()
    } else if score.abs() >= 1e17 {
        let raw = format!("{:.16e}", score);
        let Some((mantissa, exponent)) = raw.split_once('e') else {
            return raw.into_bytes();
        };
        let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');
        let exponent = exponent.parse::<i32>().unwrap_or(0);
        format!("{mantissa}e{exponent:+}").into_bytes()
    } else {
        format!("{}", score).into_bytes()
    }
}

/// Borrow the inner `InlineZSet` of a zset-encoded `RedisObject`,
/// raising `WRONGTYPE` if `obj` is any other kind.
fn as_zset_ref(obj: Option<&RedisObject>) -> Result<Option<&InlineZSet>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => o.zset().map(Some).ok_or_else(RedisError::wrong_type),
    }
}

/// Mutable counterpart of `as_zset_ref`.
fn as_zset_mut(obj: Option<&mut RedisObject>) -> Result<Option<&mut InlineZSet>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => {
            if o.is_zset() {
                Ok(o.zset_mut())
            } else {
                Err(RedisError::wrong_type())
            }
        }
    }
}

/// Resolve `start`/`stop` for inclusive range queries.
///
/// Mirrors `zslGetRangeInLen` — clamps negatives to zero, clamps
/// `stop >= len` to `len-1`, and returns `None` when the range is
/// empty after clamping.
fn clamp_rank_range(start: i64, stop: i64, len: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let s = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };
    let e = if stop < 0 { len + stop } else { stop };
    if s >= len || e < s {
        return None;
    }
    let e = e.min(len - 1);
    Some((s as usize, e as usize))
}

/// Delete the key when its zset has become empty.
fn delete_if_empty(ctx: &mut CommandContext, key: &RedisString) {
    let empty = matches!(
        ctx.db().lookup_key_read(key),
        Some(o) if o.zset().map(|z| z.is_empty()).unwrap_or(false)
    );
    if empty {
        ctx.db_mut().sync_delete(key);
    }
}

/// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
///
/// Adds one or more `(score, member)` pairs to the sorted set at `key`,
/// creating the key when absent. Without `CH` the reply is the number
/// of *new* members; with `CH` it counts newly-added plus updated
/// scores. The `INCR` flag toggles single-pair increment semantics.
pub fn zadd_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(b"zadd"));
    }

    let key = ctx.arg_owned(1usize)?;
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut incr = false;

    let mut idx = 2usize;
    while idx < argc {
        let opt = ctx.arg(idx)?;
        let bytes = opt.as_bytes();
        if bytes.eq_ignore_ascii_case(b"NX") {
            nx = true;
        } else if bytes.eq_ignore_ascii_case(b"XX") {
            xx = true;
        } else if bytes.eq_ignore_ascii_case(b"GT") {
            gt = true;
        } else if bytes.eq_ignore_ascii_case(b"LT") {
            lt = true;
        } else if bytes.eq_ignore_ascii_case(b"CH") {
            ch = true;
        } else if bytes.eq_ignore_ascii_case(b"INCR") {
            incr = true;
        } else {
            break;
        }
        idx += 1;
    }

    if nx && xx {
        return Err(RedisError::runtime(
            b"ERR XX and NX options at the same time are not compatible",
        ));
    }
    if (gt || lt) && nx {
        return Err(RedisError::runtime(
            b"ERR GT, LT, and/or NX options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return Err(RedisError::runtime(
            b"ERR GT, LT, and/or NX options at the same time are not compatible",
        ));
    }

    let remaining = argc - idx;
    if remaining == 0 || remaining % 2 != 0 {
        return Err(RedisError::syntax(b"syntax error"));
    }
    if incr && remaining != 2 {
        return Err(RedisError::runtime(
            b"ERR INCR option supports a single increment-element pair",
        ));
    }

    let mut pairs: Vec<(f64, RedisString)> = Vec::with_capacity(remaining / 2);
    let mut j = idx;
    while j < argc {
        let score = parse_score(ctx.arg(j)?.as_bytes())?;
        let member = ctx.arg_owned(j + 1)?;
        pairs.push((score, member));
        j += 2;
    }

    if let Some(existing) = ctx.db().lookup_key_read(&key) {
        if !existing.is_zset() {
            return Err(RedisError::wrong_type());
        }
    }

    if ctx.db().lookup_key_read(&key).is_none() {
        if xx {
            if incr {
                return ctx.reply_null_bulk();
            }
            return ctx.reply_integer(0);
        }
        let obj = RedisObject::new_zset();
        ctx.db_mut().set_key(key.clone(), obj, 0);
    }

    let mut added: i64 = 0;
    let mut changed: i64 = 0;
    let mut incr_reply: Option<Option<f64>> = None;

    let zset = ctx
        .db_mut()
        .lookup_key_write(&key)
        .and_then(|o| o.zset_mut())
        .expect("zset created or pre-validated above");

    for (score, member) in pairs {
        let prev = zset.score(&member);
        match prev {
            None => {
                if xx {
                    if incr {
                        incr_reply = Some(None);
                    }
                    continue;
                }
                let final_score = score;
                zset.upsert(member, final_score);
                added += 1;
                changed += 1;
                if incr {
                    incr_reply = Some(Some(final_score));
                }
            }
            Some(prev_score) => {
                if nx {
                    if incr {
                        incr_reply = Some(None);
                    }
                    continue;
                }
                let candidate = if incr { prev_score + score } else { score };
                if candidate.is_nan() {
                    return Err(RedisError::runtime(
                        b"ERR resulting score is not a number (NaN)",
                    ));
                }
                if gt && !(candidate > prev_score) {
                    if incr {
                        incr_reply = Some(None);
                    }
                    continue;
                }
                if lt && !(candidate < prev_score) {
                    if incr {
                        incr_reply = Some(None);
                    }
                    continue;
                }
                if candidate.to_bits() != prev_score.to_bits() {
                    zset.upsert(member, candidate);
                    changed += 1;
                }
                if incr {
                    incr_reply = Some(Some(candidate));
                }
            }
        }
    }

    delete_if_empty(ctx, &key);

    if added > 0 || changed > 0 {
        ctx.notify_keyspace_event(NOTIFY_ZSET, b"zadd", &key);
        schedule_or_wake_zset(ctx, &key);
    }

    if incr {
        match incr_reply {
            Some(Some(score)) => ctx.reply_double(score),
            _ => ctx.reply_null_bulk(),
        }
    } else if ch {
        ctx.reply_integer(changed)
    } else {
        ctx.reply_integer(added)
    }
}

/// ZSCORE key member
pub fn zscore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"zscore"));
    }
    let key = ctx.arg_owned(1usize)?;
    let member = ctx.arg_owned(2usize)?;
    let score = match as_zset_ref(ctx.db().lookup_key_read(&key))? {
        None => None,
        Some(z) => z.score(&member),
    };
    match score {
        Some(s) if ctx.client_ref().resp_proto == 3 => ctx.reply_double(s),
        Some(s) => ctx.reply_bulk(&format_score(s)),
        None => ctx.reply_null_bulk(),
    }
}

/// ZMSCORE key member [member ...]
pub fn zmscore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"zmscore"));
    }
    let key = ctx.arg_owned(1usize)?;
    let mut members: Vec<RedisString> = Vec::with_capacity(argc - 2);
    for j in 2..argc {
        members.push(ctx.arg_owned(j)?);
    }
    let scores: Vec<Option<f64>> = match as_zset_ref(ctx.db().lookup_key_read(&key))? {
        None => members.iter().map(|_| None).collect(),
        Some(z) => members.iter().map(|m| z.score(m)).collect(),
    };
    ctx.reply_array_header(scores.len())?;
    for s in scores {
        match s {
            Some(v) => ctx.reply_double(v)?,
            None => ctx.reply_null_bulk()?,
        }
    }
    Ok(())
}

/// ZCARD key
pub fn zcard_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"zcard"));
    }
    let key = ctx.arg_owned(1usize)?;
    let len = match as_zset_ref(ctx.db().lookup_key_read(&key))? {
        None => 0,
        Some(z) => z.len() as i64,
    };
    ctx.reply_integer(len)
}

/// ZINCRBY key delta member
pub fn zincrby_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"zincrby"));
    }
    let key = ctx.arg_owned(1usize)?;
    let delta = parse_score(ctx.arg(2)?.as_bytes())?;
    let member = ctx.arg_owned(3usize)?;

    if let Some(existing) = ctx.db().lookup_key_read(&key) {
        if !existing.is_zset() {
            return Err(RedisError::wrong_type());
        }
    }
    if ctx.db().lookup_key_read(&key).is_none() {
        ctx.db_mut()
            .set_key(key.clone(), RedisObject::new_zset(), 0);
    }
    let zset = ctx
        .db_mut()
        .lookup_key_write(&key)
        .and_then(|o| o.zset_mut())
        .expect("zset created above");
    let new_score = match zset.score(&member) {
        Some(prev) => prev + delta,
        None => delta,
    };
    if new_score.is_nan() {
        return Err(RedisError::runtime(
            b"ERR resulting score is not a number (NaN)",
        ));
    }
    zset.upsert(member, new_score);
    ctx.notify_keyspace_event(NOTIFY_ZSET, b"zincrby", &key);
    schedule_or_wake_zset(ctx, &key);
    ctx.reply_double(new_score)
}

/// ZREM key member [member ...]
pub fn zrem_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"zrem"));
    }
    let key = ctx.arg_owned(1usize)?;
    let mut members: Vec<RedisString> = Vec::with_capacity(argc - 2);
    for j in 2..argc {
        members.push(ctx.arg_owned(j)?);
    }
    let removed = {
        let zset = match as_zset_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return ctx.reply_integer(0),
            Some(z) => z,
        };
        let mut count: i64 = 0;
        for m in members {
            if zset.remove(&m).is_some() {
                count += 1;
            }
        }
        count
    };
    delete_if_empty(ctx, &key);
    if removed > 0 {
        ctx.notify_keyspace_event(NOTIFY_ZSET, b"zrem", &key);
        let now_empty = ctx.db().lookup_key_read(&key).is_none();
        if now_empty {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
        }
    }
    ctx.reply_integer(removed)
}

/// ZRANK / ZREVRANK shared body.
fn rank_inner(ctx: &mut CommandContext, reverse: bool, cmd: &[u8]) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc != 3 && argc != 4 {
        return Err(RedisError::wrong_number_of_args(cmd));
    }
    let withscore = if argc == 4 {
        let opt = ctx.arg(3)?;
        if !opt.as_bytes().eq_ignore_ascii_case(b"WITHSCORE") {
            return Err(RedisError::syntax(b"syntax error"));
        }
        true
    } else {
        false
    };
    let key = ctx.arg_owned(1usize)?;
    let member = ctx.arg_owned(2usize)?;

    let result: Option<(i64, f64)> = match as_zset_ref(ctx.db().lookup_key_read(&key))? {
        None => None,
        Some(z) => {
            let n = z.len() as i64;
            let mut found: Option<(i64, f64)> = None;
            for (i, (score, m)) in z.iter_ascending().enumerate() {
                if m == &member {
                    let rank = if reverse {
                        n - 1 - (i as i64)
                    } else {
                        i as i64
                    };
                    found = Some((rank, score));
                    break;
                }
            }
            found
        }
    };

    match (result, withscore) {
        (None, false) => ctx.reply_null_bulk(),
        (None, true) => ctx.reply_null_array(),
        (Some((rank, _)), false) => ctx.reply_integer(rank),
        (Some((rank, score)), true) => {
            ctx.reply_array_header(2usize)?;
            ctx.reply_integer(rank)?;
            ctx.reply_double(score)
        }
    }
}

/// ZRANK key member [WITHSCORE]
pub fn zrank_command(ctx: &mut CommandContext) -> RedisResult<()> {
    rank_inner(ctx, false, b"zrank")
}

/// ZREVRANK key member [WITHSCORE]
pub fn zrevrank_command(ctx: &mut CommandContext) -> RedisResult<()> {
    rank_inner(ctx, true, b"zrevrank")
}

/// ZCOUNT key min max
pub fn zcount_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"zcount"));
    }
    let key = ctx.arg_owned(1usize)?;
    let (min, min_excl) = parse_score_range(ctx.arg(2)?.as_bytes())?;
    let (max, max_excl) = parse_score_range(ctx.arg(3)?.as_bytes())?;
    let count: i64 = match as_zset_ref(ctx.db().lookup_key_read(&key))? {
        None => 0,
        Some(z) => z
            .iter_ascending()
            .filter(|(s, _)| score_in_range(*s, min, min_excl, max, max_excl))
            .count() as i64,
    };
    ctx.reply_integer(count)
}

/// Inclusive/exclusive score-range membership test.
fn score_in_range(s: f64, min: f64, min_excl: bool, max: f64, max_excl: bool) -> bool {
    let lower_ok = if min_excl { s > min } else { s >= min };
    let upper_ok = if max_excl { s < max } else { s <= max };
    lower_ok && upper_ok
}

/// Common rank-based range body shared by ZRANGE (default) and ZREVRANGE.
fn range_by_rank(
    ctx: &mut CommandContext,
    key: &RedisString,
    start: i64,
    stop: i64,
    reverse: bool,
    withscores: bool,
) -> RedisResult<()> {
    let entries: Vec<(f64, RedisString)> = match as_zset_ref(ctx.db().lookup_key_read(key))? {
        None => Vec::new(),
        Some(z) => {
            let len = z.len() as i64;
            match clamp_rank_range(start, stop, len) {
                None => Vec::new(),
                Some((lo, hi)) => {
                    if reverse {
                        z.iter_ascending()
                            .rev()
                            .skip(lo)
                            .take(hi - lo + 1)
                            .map(|(s, m)| (s, m.clone()))
                            .collect()
                    } else {
                        z.iter_ascending()
                            .skip(lo)
                            .take(hi - lo + 1)
                            .map(|(s, m)| (s, m.clone()))
                            .collect()
                    }
                }
            }
        }
    };
    emit_range_reply(ctx, entries, withscores)
}

/// Reply with `entries` as either a flat member array or interleaved
/// member/score array, depending on `withscores`.
fn emit_range_reply(
    ctx: &mut CommandContext,
    entries: Vec<(f64, RedisString)>,
    withscores: bool,
) -> RedisResult<()> {
    if withscores && ctx.client_ref().resp_proto == 3 {
        ctx.reply_array_header(entries.len())?;
        for (score, member) in entries {
            ctx.reply_array_header(2usize)?;
            ctx.reply_bulk_string(member)?;
            ctx.reply_double(score)?;
        }
        return Ok(());
    }

    let len = if withscores {
        entries.len() * 2
    } else {
        entries.len()
    };
    ctx.reply_array_header(len)?;
    for (score, member) in entries {
        ctx.reply_bulk_string(member)?;
        if withscores {
            ctx.reply_bulk(&format_score(score))?;
        }
    }
    Ok(())
}

/// Common score-range body shared by ZRANGEBYSCORE and ZREVRANGEBYSCORE.
fn range_by_score(
    ctx: &mut CommandContext,
    key: &RedisString,
    min: f64,
    min_excl: bool,
    max: f64,
    max_excl: bool,
    reverse: bool,
    offset: i64,
    count: i64,
    withscores: bool,
) -> RedisResult<()> {
    let entries: Vec<(f64, RedisString)> = match as_zset_ref(ctx.db().lookup_key_read(key))? {
        None => Vec::new(),
        Some(z) => {
            let all: Vec<(f64, RedisString)> = z
                .iter_ascending()
                .filter(|(s, _)| score_in_range(*s, min, min_excl, max, max_excl))
                .map(|(s, m)| (s, m.clone()))
                .collect();
            let iter: Box<dyn Iterator<Item = (f64, RedisString)>> = if reverse {
                Box::new(all.into_iter().rev())
            } else {
                Box::new(all.into_iter())
            };
            let skipped: Box<dyn Iterator<Item = (f64, RedisString)>> = if offset > 0 {
                Box::new(iter.skip(offset as usize))
            } else {
                iter
            };
            if count < 0 {
                skipped.collect()
            } else {
                skipped.take(count as usize).collect()
            }
        }
    };
    emit_range_reply(ctx, entries, withscores)
}

/// ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
pub fn zrange_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(b"zrange"));
    }
    let key = ctx.arg_owned(1usize)?;
    let start_bytes = ctx.arg_owned(2usize)?;
    let stop_bytes = ctx.arg_owned(3usize)?;

    let mut by_score = false;
    let mut by_lex = false;
    let mut reverse = false;
    let mut withscores = false;
    let mut offset: i64 = 0;
    let mut count: i64 = -1;
    let mut have_limit = false;

    let mut idx = 4usize;
    while idx < argc {
        let opt = ctx.arg(idx)?;
        let bytes = opt.as_bytes();
        if bytes.eq_ignore_ascii_case(b"BYSCORE") {
            by_score = true;
            idx += 1;
        } else if bytes.eq_ignore_ascii_case(b"BYLEX") {
            by_lex = true;
            idx += 1;
        } else if bytes.eq_ignore_ascii_case(b"REV") {
            reverse = true;
            idx += 1;
        } else if bytes.eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            idx += 1;
        } else if bytes.eq_ignore_ascii_case(b"LIMIT") {
            if idx + 2 >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            offset = parse_strict_i64(ctx.arg(idx + 1)?.as_bytes())?;
            count = parse_strict_i64(ctx.arg(idx + 2)?.as_bytes())?;
            have_limit = true;
            idx += 3;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    if have_limit && !by_score && !by_lex {
        return Err(RedisError::runtime(
            b"ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }
    if by_lex {
        if withscores {
            return Err(RedisError::syntax(b"syntax error"));
        }
        let (min, max) = if reverse {
            (
                parse_lex_bound(stop_bytes.as_bytes())?,
                parse_lex_bound(start_bytes.as_bytes())?,
            )
        } else {
            (
                parse_lex_bound(start_bytes.as_bytes())?,
                parse_lex_bound(stop_bytes.as_bytes())?,
            )
        };
        return rangebylex_inner_with_bounds(ctx, &key, min, max, reverse, offset, count);
    }

    if by_score {
        let (a_score, a_excl) = parse_score_range(start_bytes.as_bytes())?;
        let (b_score, b_excl) = parse_score_range(stop_bytes.as_bytes())?;
        let (min, min_excl, max, max_excl) = if reverse {
            (b_score, b_excl, a_score, a_excl)
        } else {
            (a_score, a_excl, b_score, b_excl)
        };
        return range_by_score(
            ctx, &key, min, min_excl, max, max_excl, reverse, offset, count, withscores,
        );
    }

    let start = parse_strict_i64(start_bytes.as_bytes())?;
    let stop = parse_strict_i64(stop_bytes.as_bytes())?;
    range_by_rank(ctx, &key, start, stop, reverse, withscores)
}

/// ZREVRANGE key start stop [WITHSCORES]
pub fn zrevrange_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc != 4 && argc != 5 {
        return Err(RedisError::wrong_number_of_args(b"zrevrange"));
    }
    let key = ctx.arg_owned(1usize)?;
    let start = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
    let stop = parse_strict_i64(ctx.arg(3)?.as_bytes())?;
    let withscores = if argc == 5 {
        let opt = ctx.arg(4)?;
        if !opt.as_bytes().eq_ignore_ascii_case(b"WITHSCORES") {
            return Err(RedisError::syntax(b"syntax error"));
        }
        true
    } else {
        false
    };
    range_by_rank(ctx, &key, start, stop, true, withscores)
}

/// Shared body for ZRANGEBYSCORE and ZREVRANGEBYSCORE.
fn rangebyscore_inner(ctx: &mut CommandContext, reverse: bool, cmd: &[u8]) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(cmd));
    }
    let key = ctx.arg_owned(1usize)?;
    let arg_a = ctx.arg_owned(2usize)?;
    let arg_b = ctx.arg_owned(3usize)?;

    let mut withscores = false;
    let mut offset: i64 = 0;
    let mut count: i64 = -1;
    let mut idx = 4usize;
    while idx < argc {
        let opt = ctx.arg(idx)?;
        let bytes = opt.as_bytes();
        if bytes.eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            idx += 1;
        } else if bytes.eq_ignore_ascii_case(b"LIMIT") {
            if idx + 2 >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            offset = parse_strict_i64(ctx.arg(idx + 1)?.as_bytes())?;
            count = parse_strict_i64(ctx.arg(idx + 2)?.as_bytes())?;
            idx += 3;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    let (a_score, a_excl) = parse_score_range(arg_a.as_bytes())?;
    let (b_score, b_excl) = parse_score_range(arg_b.as_bytes())?;
    let (min, min_excl, max, max_excl) = if reverse {
        (b_score, b_excl, a_score, a_excl)
    } else {
        (a_score, a_excl, b_score, b_excl)
    };
    range_by_score(
        ctx, &key, min, min_excl, max, max_excl, reverse, offset, count, withscores,
    )
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
pub fn zrangebyscore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    rangebyscore_inner(ctx, false, b"zrangebyscore")
}

/// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
pub fn zrevrangebyscore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    rangebyscore_inner(ctx, true, b"zrevrangebyscore")
}

/// Shared body for ZPOPMIN and ZPOPMAX.
fn popminmax_inner(ctx: &mut CommandContext, reverse: bool, cmd: &[u8]) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 || argc > 3 {
        return Err(RedisError::wrong_number_of_args(cmd));
    }
    let key = ctx.arg_owned(1usize)?;
    let count_arg: Option<i64> = if argc == 3 {
        Some(parse_strict_i64(ctx.arg(2)?.as_bytes())?)
    } else {
        None
    };
    if let Some(n) = count_arg {
        if n < 0 {
            return Err(RedisError::runtime(
                b"ERR value is out of range, must be positive",
            ));
        }
    }

    let popped: Vec<(f64, RedisString)> = {
        let zset = match as_zset_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => Vec::new(),
            Some(z) => {
                let count = count_arg.unwrap_or(1) as usize;
                let take = count.min(z.len());
                let mut targets: Vec<(f64, RedisString)> = Vec::with_capacity(take);
                if reverse {
                    for (score, member) in z.iter_ascending().rev().take(take) {
                        targets.push((score, member.clone()));
                    }
                } else {
                    for (score, member) in z.iter_ascending().take(take) {
                        targets.push((score, member.clone()));
                    }
                }
                for (_, m) in &targets {
                    z.remove(m);
                }
                targets
            }
        };
        zset
    };
    delete_if_empty(ctx, &key);

    if !popped.is_empty() {
        let event = if reverse {
            b"zpopmax" as &[u8]
        } else {
            b"zpopmin" as &[u8]
        };
        ctx.notify_keyspace_event(NOTIFY_ZSET, event, &key);
        let now_empty = ctx.db().lookup_key_read(&key).is_none();
        if now_empty {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
        }
    }

    let resp3 = ctx.client_ref().resp_proto == 3;
    match count_arg {
        None => {
            if popped.is_empty() {
                ctx.reply_array_header(0usize)?;
                return Ok(());
            }
            ctx.reply_array_header(2usize)?;
            let (score, member) = popped.into_iter().next().expect("popped non-empty");
            ctx.reply_bulk_string(member)?;
            ctx.reply_double(score)
        }
        Some(_) => {
            if resp3 {
                ctx.reply_array_header(popped.len())?;
                for (score, member) in popped {
                    ctx.reply_array_header(2usize)?;
                    ctx.reply_bulk_string(member)?;
                    ctx.reply_double(score)?;
                }
            } else {
                ctx.reply_array_header(popped.len() * 2)?;
                for (score, member) in popped {
                    ctx.reply_bulk_string(member)?;
                    ctx.reply_double(score)?;
                }
            }
            Ok(())
        }
    }
}

/// ZPOPMIN key [count]
pub fn zpopmin_command(ctx: &mut CommandContext) -> RedisResult<()> {
    popminmax_inner(ctx, false, b"zpopmin")
}

/// ZPOPMAX key [count]
pub fn zpopmax_command(ctx: &mut CommandContext) -> RedisResult<()> {
    popminmax_inner(ctx, true, b"zpopmax")
}

/// ZREMRANGEBYRANK key start stop
pub fn zremrangebyrank_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"zremrangebyrank"));
    }
    let key = ctx.arg_owned(1usize)?;
    let start = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
    let stop = parse_strict_i64(ctx.arg(3)?.as_bytes())?;
    let removed = {
        let zset = match as_zset_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return ctx.reply_integer(0),
            Some(z) => z,
        };
        let len = zset.len() as i64;
        let (lo, hi) = match clamp_rank_range(start, stop, len) {
            None => return ctx.reply_integer(0),
            Some(r) => r,
        };
        let to_remove: Vec<RedisString> = zset
            .iter_ascending()
            .skip(lo)
            .take(hi - lo + 1)
            .map(|(_, m)| m.clone())
            .collect();
        let count = to_remove.len() as i64;
        for m in to_remove {
            zset.remove(&m);
        }
        count
    };
    delete_if_empty(ctx, &key);
    if removed > 0 {
        ctx.notify_keyspace_event(NOTIFY_ZSET, b"zremrangebyrank", &key);
        let now_empty = ctx.db().lookup_key_read(&key).is_none();
        if now_empty {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
        }
    }
    ctx.reply_integer(removed)
}

/// ZREMRANGEBYSCORE key min max
pub fn zremrangebyscore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"zremrangebyscore"));
    }
    let key = ctx.arg_owned(1usize)?;
    let (min, min_excl) = parse_score_range(ctx.arg(2)?.as_bytes())?;
    let (max, max_excl) = parse_score_range(ctx.arg(3)?.as_bytes())?;
    let removed = {
        let zset = match as_zset_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return ctx.reply_integer(0),
            Some(z) => z,
        };
        let to_remove: Vec<RedisString> = zset
            .iter_ascending()
            .filter(|(s, _)| score_in_range(*s, min, min_excl, max, max_excl))
            .map(|(_, m)| m.clone())
            .collect();
        let count = to_remove.len() as i64;
        for m in to_remove {
            zset.remove(&m);
        }
        count
    };
    delete_if_empty(ctx, &key);
    if removed > 0 {
        ctx.notify_keyspace_event(NOTIFY_ZSET, b"zremrangebyscore", &key);
        let now_empty = ctx.db().lookup_key_read(&key).is_none();
        if now_empty {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
        }
    }
    ctx.reply_integer(removed)
}

/// Lex-range bound: `-` / `+` / `[member` / `(member`.
///
/// Mirrors the `zslLexRangeSpec` C struct. Comparison is byte-wise on the
/// member; the two infinity sentinels short-circuit comparison so any real
/// member is strictly between `Min` and `Max`.
#[derive(Debug, Clone)]
enum LexBound {
    Min,
    Max,
    Inclusive(Vec<u8>),
    Exclusive(Vec<u8>),
}

/// Parse one side of a lex range.
fn parse_lex_bound(bytes: &[u8]) -> Result<LexBound, RedisError> {
    match bytes.first() {
        None => Err(RedisError::runtime(
            b"ERR min or max not valid string range item",
        )),
        Some(b'-') if bytes.len() == 1 => Ok(LexBound::Min),
        Some(b'+') if bytes.len() == 1 => Ok(LexBound::Max),
        Some(b'[') => Ok(LexBound::Inclusive(bytes[1..].to_vec())),
        Some(b'(') => Ok(LexBound::Exclusive(bytes[1..].to_vec())),
        _ => Err(RedisError::runtime(
            b"ERR min or max not valid string range item",
        )),
    }
}

/// Test whether `member` is `>=` (or `>` when exclusive) the lower bound.
fn lex_above_min(member: &[u8], min: &LexBound) -> bool {
    match min {
        LexBound::Min => true,
        LexBound::Max => false,
        LexBound::Inclusive(b) => member >= b.as_slice(),
        LexBound::Exclusive(b) => member > b.as_slice(),
    }
}

/// Test whether `member` is `<=` (or `<` when exclusive) the upper bound.
fn lex_below_max(member: &[u8], max: &LexBound) -> bool {
    match max {
        LexBound::Min => false,
        LexBound::Max => true,
        LexBound::Inclusive(b) => member <= b.as_slice(),
        LexBound::Exclusive(b) => member < b.as_slice(),
    }
}

/// Apply LIMIT offset/count to an iterator of `(score, member)` pairs.
fn apply_limit(items: Vec<(f64, RedisString)>, offset: i64, count: i64) -> Vec<(f64, RedisString)> {
    let skipped: Box<dyn Iterator<Item = (f64, RedisString)>> = if offset > 0 {
        Box::new(items.into_iter().skip(offset as usize))
    } else {
        Box::new(items.into_iter())
    };
    if count < 0 {
        skipped.collect()
    } else {
        skipped.take(count as usize).collect()
    }
}

/// Shared body for ZRANGEBYLEX and ZREVRANGEBYLEX.
fn rangebylex_inner(ctx: &mut CommandContext, reverse: bool, cmd: &[u8]) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(cmd));
    }
    let key = ctx.arg_owned(1usize)?;
    let arg_a = ctx.arg_owned(2usize)?;
    let arg_b = ctx.arg_owned(3usize)?;

    let mut offset: i64 = 0;
    let mut count: i64 = -1;
    let mut idx = 4usize;
    while idx < argc {
        let opt = ctx.arg(idx)?;
        if opt.as_bytes().eq_ignore_ascii_case(b"LIMIT") {
            if idx + 2 >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            offset = parse_strict_i64(ctx.arg(idx + 1)?.as_bytes())?;
            count = parse_strict_i64(ctx.arg(idx + 2)?.as_bytes())?;
            idx += 3;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    let (min, max) = if reverse {
        (
            parse_lex_bound(arg_b.as_bytes())?,
            parse_lex_bound(arg_a.as_bytes())?,
        )
    } else {
        (
            parse_lex_bound(arg_a.as_bytes())?,
            parse_lex_bound(arg_b.as_bytes())?,
        )
    };
    rangebylex_inner_with_bounds(ctx, &key, min, max, reverse, offset, count)
}

fn rangebylex_inner_with_bounds(
    ctx: &mut CommandContext,
    key: &RedisString,
    min: LexBound,
    max: LexBound,
    reverse: bool,
    offset: i64,
    count: i64,
) -> RedisResult<()> {
    let mut entries: Vec<(f64, RedisString)> = match as_zset_ref(ctx.db().lookup_key_read(key))? {
        None => Vec::new(),
        Some(z) => z
            .iter_ascending()
            .filter(|(_, m)| lex_above_min(m.as_bytes(), &min) && lex_below_max(m.as_bytes(), &max))
            .map(|(s, m)| (s, m.clone()))
            .collect(),
    };
    if reverse {
        entries.reverse();
    }
    let limited = apply_limit(entries, offset, count);
    ctx.reply_array_header(limited.len())?;
    for (_, m) in limited {
        ctx.reply_bulk_string(m)?;
    }
    Ok(())
}

/// ZRANGEBYLEX key min max [LIMIT offset count]
pub fn zrangebylex_command(ctx: &mut CommandContext) -> RedisResult<()> {
    rangebylex_inner(ctx, false, b"zrangebylex")
}

/// ZREVRANGEBYLEX key max min [LIMIT offset count]
pub fn zrevrangebylex_command(ctx: &mut CommandContext) -> RedisResult<()> {
    rangebylex_inner(ctx, true, b"zrevrangebylex")
}

/// ZLEXCOUNT key min max
pub fn zlexcount_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"zlexcount"));
    }
    let key = ctx.arg_owned(1usize)?;
    let min = parse_lex_bound(ctx.arg(2)?.as_bytes())?;
    let max = parse_lex_bound(ctx.arg(3)?.as_bytes())?;
    let count: i64 = match as_zset_ref(ctx.db().lookup_key_read(&key))? {
        None => 0,
        Some(z) => z
            .iter_ascending()
            .filter(|(_, m)| lex_above_min(m.as_bytes(), &min) && lex_below_max(m.as_bytes(), &max))
            .count() as i64,
    };
    ctx.reply_integer(count)
}

/// ZREMRANGEBYLEX key min max
pub fn zremrangebylex_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"zremrangebylex"));
    }
    let key = ctx.arg_owned(1usize)?;
    let min = parse_lex_bound(ctx.arg(2)?.as_bytes())?;
    let max = parse_lex_bound(ctx.arg(3)?.as_bytes())?;
    let removed = {
        let zset = match as_zset_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return ctx.reply_integer(0),
            Some(z) => z,
        };
        let targets: Vec<RedisString> = zset
            .iter_ascending()
            .filter(|(_, m)| lex_above_min(m.as_bytes(), &min) && lex_below_max(m.as_bytes(), &max))
            .map(|(_, m)| m.clone())
            .collect();
        let count = targets.len() as i64;
        for m in targets {
            zset.remove(&m);
        }
        count
    };
    delete_if_empty(ctx, &key);
    if removed > 0 {
        ctx.notify_keyspace_event(NOTIFY_ZSET, b"zremrangebylex", &key);
        let now_empty = ctx.db().lookup_key_read(&key).is_none();
        if now_empty {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
        }
    }
    ctx.reply_integer(removed)
}

/// Aggregation mode for ZUNIONSTORE / ZINTERSTORE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

/// Parsed `WEIGHTS` / `AGGREGATE` / `WITHSCORES` modifiers shared by the
/// zset set-algebra commands.
#[derive(Debug, Clone)]
struct ZAlgebraOpts {
    weights: Vec<f64>,
    aggregate: Aggregate,
    withscores: bool,
}

/// Snapshot one source key as a `(member -> score)` map. Plain sets are
/// promoted to score `1.0` per real Redis. Returns `Err(WRONGTYPE)` for any
/// other type and `Ok(empty)` for missing keys.
fn snapshot_zset_or_set(
    ctx: &CommandContext,
    key: &RedisString,
) -> Result<HashMap<RedisString, f64>, RedisError> {
    let obj = ctx.db().lookup_key_read(key);
    match obj {
        None => Ok(HashMap::new()),
        Some(o) => {
            if let Some(z) = o.zset() {
                let mut out = HashMap::with_capacity(z.len());
                for (s, m) in z.iter_ascending() {
                    out.insert(m.clone(), s);
                }
                Ok(out)
            } else if let Some(s) = o.set() {
                let mut out = HashMap::with_capacity(s.len());
                for m in s {
                    out.insert(m.clone(), 1.0);
                }
                Ok(out)
            } else {
                Err(RedisError::wrong_type())
            }
        }
    }
}

/// Parse the trailing `[WEIGHTS w1 ...] [AGGREGATE SUM|MIN|MAX]
/// [WITHSCORES]` block. `withscores_allowed` controls whether
/// `WITHSCORES` is accepted (only on the non-`*STORE` variants).
fn parse_zalgebra_opts(
    ctx: &CommandContext,
    start: usize,
    numkeys: usize,
    withscores_allowed: bool,
) -> Result<ZAlgebraOpts, RedisError> {
    let argc = ctx.arg_count();
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = Aggregate::Sum;
    let mut withscores = false;
    let mut idx = start;
    while idx < argc {
        let opt = ctx.arg(idx)?;
        let bytes = opt.as_bytes();
        if bytes.eq_ignore_ascii_case(b"WEIGHTS") {
            if idx + numkeys >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            for (j, slot) in weights.iter_mut().enumerate() {
                *slot = parse_score(ctx.arg(idx + 1 + j)?.as_bytes())
                    .map_err(|_| RedisError::runtime(b"ERR weight value is not a float"))?;
            }
            idx += 1 + numkeys;
        } else if bytes.eq_ignore_ascii_case(b"AGGREGATE") {
            if idx + 1 >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let mode = ctx.arg(idx + 1)?;
            let mb = mode.as_bytes();
            aggregate = if mb.eq_ignore_ascii_case(b"SUM") {
                Aggregate::Sum
            } else if mb.eq_ignore_ascii_case(b"MIN") {
                Aggregate::Min
            } else if mb.eq_ignore_ascii_case(b"MAX") {
                Aggregate::Max
            } else {
                return Err(RedisError::syntax(b"syntax error"));
            };
            idx += 2;
        } else if withscores_allowed && bytes.eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            idx += 1;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
    Ok(ZAlgebraOpts {
        weights,
        aggregate,
        withscores,
    })
}

/// Combine two scores per the requested aggregation mode.
///
/// Mirrors Redis's `C99` double arithmetic: `+inf + -inf` yields 0.0,
/// matching the behavior documented in `t_zset.c:zunionInterDiffGenericCommand`.
fn combine_scores(existing: f64, new: f64, mode: Aggregate) -> f64 {
    match mode {
        Aggregate::Sum => {
            let r = existing + new;
            if r.is_nan() {
                0.0
            } else {
                r
            }
        }
        Aggregate::Min => existing.min(new),
        Aggregate::Max => existing.max(new),
    }
}

/// Compute the union of `sources` with per-source `weights` and the given
/// aggregation mode. Returns a `(member, final_score)` vector sorted by
/// score ascending (and member byte-wise for ties).
fn zunion_inner(
    sources: Vec<HashMap<RedisString, f64>>,
    opts: &ZAlgebraOpts,
) -> Vec<(RedisString, f64)> {
    let mut acc: HashMap<RedisString, f64> = HashMap::new();
    for (i, src) in sources.into_iter().enumerate() {
        let w = opts.weights[i];
        for (m, s) in src {
            let weighted = if w == 0.0 { 0.0 } else { s * w };
            acc.entry(m)
                .and_modify(|cur| *cur = combine_scores(*cur, weighted, opts.aggregate))
                .or_insert(weighted);
        }
    }
    let mut out: Vec<(RedisString, f64)> = acc.into_iter().collect();
    out.sort_by(|a, b| {
        a.1.total_cmp(&b.1)
            .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
    });
    out
}

/// Compute the intersection of `sources` with per-source `weights`. A
/// member must appear in every source to survive; aggregation is then
/// applied to the weighted scores.
fn zinter_inner(
    sources: Vec<HashMap<RedisString, f64>>,
    opts: &ZAlgebraOpts,
) -> Vec<(RedisString, f64)> {
    if sources.is_empty() || sources.iter().any(|s| s.is_empty()) {
        return Vec::new();
    }
    let mut iter = sources.into_iter();
    let first = iter.next().expect("non-empty sources");
    let first_w = opts.weights[0];
    let rest: Vec<(HashMap<RedisString, f64>, f64)> = iter
        .enumerate()
        .map(|(i, m)| (m, opts.weights[i + 1]))
        .collect();
    let mut acc: HashMap<RedisString, f64> = HashMap::new();
    for (m, s) in first {
        let mut score = s * first_w;
        let mut all = true;
        for (other, w) in &rest {
            match other.get(&m) {
                None => {
                    all = false;
                    break;
                }
                Some(os) => {
                    score = combine_scores(score, os * w, opts.aggregate);
                }
            }
        }
        if all {
            acc.insert(m, score);
        }
    }
    let mut out: Vec<(RedisString, f64)> = acc.into_iter().collect();
    out.sort_by(|a, b| {
        a.1.total_cmp(&b.1)
            .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
    });
    out
}

/// Compute `sources[0]` minus every following source. WEIGHTS / AGGREGATE
/// are not applied — ZDIFF emits the first source's scores unchanged.
fn zdiff_inner(sources: Vec<HashMap<RedisString, f64>>) -> Vec<(RedisString, f64)> {
    if sources.is_empty() {
        return Vec::new();
    }
    let mut iter = sources.into_iter();
    let mut acc = iter.next().expect("non-empty sources");
    for other in iter {
        for k in other.keys() {
            acc.remove(k);
        }
    }
    let mut out: Vec<(RedisString, f64)> = acc.into_iter().collect();
    out.sort_by(|a, b| {
        a.1.total_cmp(&b.1)
            .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
    });
    out
}

/// Collect `numkeys` source-key snapshots starting at `start`.
fn collect_zalgebra_sources(
    ctx: &CommandContext,
    start: usize,
    numkeys: usize,
) -> Result<Vec<HashMap<RedisString, f64>>, RedisError> {
    let mut out = Vec::with_capacity(numkeys);
    for j in 0..numkeys {
        let key = ctx.arg(start + j)?.clone();
        out.push(snapshot_zset_or_set(ctx, &key)?);
    }
    Ok(out)
}

/// Replace `dst` with a new zset built from `entries`. Deletes `dst` when
/// `entries` is empty, matching real Redis's `*STORE` semantics.
fn store_zset(ctx: &mut CommandContext, dst: RedisString, entries: Vec<(RedisString, f64)>) -> i64 {
    if entries.is_empty() {
        ctx.db_mut().sync_delete(&dst);
        return 0;
    }
    let len = entries.len() as i64;
    let mut obj = RedisObject::new_zset();
    {
        let z = obj.zset_mut().expect("new_zset constructs an Inline zset");
        for (m, s) in entries {
            z.upsert(m, s);
        }
    }
    ctx.db_mut().set_key(dst, obj, 0);
    len
}

/// Emit a zset-algebra result as a wire array, with or without scores.
///
/// In RESP3 with WITHSCORES, emits a nested `*N [ *2 [member, double] ... ]`
/// structure matching real Redis/Valkey's RESP3 shape. In RESP2 (or without
/// WITHSCORES), emits a flat alternating `*2N [m1, s1, m2, s2, ...]` array.
fn emit_zalgebra_reply(
    ctx: &mut CommandContext,
    entries: Vec<(RedisString, f64)>,
    withscores: bool,
) -> RedisResult<()> {
    let resp3 = withscores && ctx.client_ref().resp_proto == 3;
    if resp3 {
        ctx.reply_array_header(entries.len())?;
        for (m, s) in entries {
            ctx.reply_array_header(2usize)?;
            ctx.reply_bulk_string(m)?;
            ctx.reply_double(s)?;
        }
    } else {
        let header = if withscores {
            entries.len() * 2
        } else {
            entries.len()
        };
        ctx.reply_array_header(header)?;
        for (m, s) in entries {
            ctx.reply_bulk_string(m)?;
            if withscores {
                ctx.reply_bulk(&format_score(s))?;
            }
        }
    }
    Ok(())
}

/// Shared body for ZUNIONSTORE / ZINTERSTORE.
fn algebra_store_inner(ctx: &mut CommandContext, cmd: &[u8], op: AlgebraOp) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(cmd));
    }
    let dst = ctx.arg_owned(1usize)?;
    let numkeys = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
    if numkeys <= 0 {
        let cmd_lower = core::str::from_utf8(cmd)
            .unwrap_or("cmd")
            .to_ascii_lowercase();
        let msg = format!(
            "ERR at least 1 input key is needed for '{}' command",
            cmd_lower
        );
        return Err(RedisError::runtime(msg.as_bytes()));
    }
    let numkeys = numkeys as usize;
    if argc < 3 + numkeys {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let opts = parse_zalgebra_opts(ctx, 3 + numkeys, numkeys, false)?;
    let sources = collect_zalgebra_sources(ctx, 3, numkeys)?;
    let result = match op {
        AlgebraOp::Union => zunion_inner(sources, &opts),
        AlgebraOp::Inter => zinter_inner(sources, &opts),
    };
    let event = match op {
        AlgebraOp::Union => b"zunionstore" as &[u8],
        AlgebraOp::Inter => b"zinterstore" as &[u8],
    };
    let stored = store_zset(ctx, dst.clone(), result);
    ctx.notify_keyspace_event(NOTIFY_ZSET, event, &dst);
    ctx.reply_integer(stored)
}

/// Shared body for ZUNION / ZINTER.
fn algebra_inner(ctx: &mut CommandContext, cmd: &[u8], op: AlgebraOp) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(cmd));
    }
    let numkeys = parse_strict_i64(ctx.arg(1)?.as_bytes())?;
    if numkeys <= 0 {
        let cmd_lower = core::str::from_utf8(cmd)
            .unwrap_or("cmd")
            .to_ascii_lowercase();
        let msg = format!(
            "ERR at least 1 input key is needed for '{}' command",
            cmd_lower
        );
        return Err(RedisError::runtime(msg.as_bytes()));
    }
    let numkeys = numkeys as usize;
    if argc < 2 + numkeys {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let opts = parse_zalgebra_opts(ctx, 2 + numkeys, numkeys, true)?;
    let sources = collect_zalgebra_sources(ctx, 2, numkeys)?;
    let result = match op {
        AlgebraOp::Union => zunion_inner(sources, &opts),
        AlgebraOp::Inter => zinter_inner(sources, &opts),
    };
    emit_zalgebra_reply(ctx, result, opts.withscores)
}

/// Which set-algebra operation a shared helper should perform.
#[derive(Debug, Clone, Copy)]
enum AlgebraOp {
    Union,
    Inter,
}

/// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS w1 ...] [AGGREGATE SUM|MIN|MAX]
pub fn zunionstore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    algebra_store_inner(ctx, b"zunionstore", AlgebraOp::Union)
}

/// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS w1 ...] [AGGREGATE SUM|MIN|MAX]
pub fn zinterstore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    algebra_store_inner(ctx, b"zinterstore", AlgebraOp::Inter)
}

/// ZDIFFSTORE destination numkeys key [key ...]
pub fn zdiffstore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(b"zdiffstore"));
    }
    let dst = ctx.arg_owned(1usize)?;
    let numkeys = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
    if numkeys <= 0 {
        return Err(RedisError::runtime(
            b"ERR at least 1 input key is needed for 'zdiffstore' command",
        ));
    }
    let numkeys = numkeys as usize;
    if argc != 3 + numkeys {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let sources = collect_zalgebra_sources(ctx, 3, numkeys)?;
    let result = zdiff_inner(sources);
    let stored = store_zset(ctx, dst.clone(), result);
    ctx.notify_keyspace_event(NOTIFY_ZSET, b"zdiffstore", &dst);
    ctx.reply_integer(stored)
}

/// ZUNION numkeys key [key ...] [WEIGHTS w1 ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
pub fn zunion_command(ctx: &mut CommandContext) -> RedisResult<()> {
    algebra_inner(ctx, b"zunion", AlgebraOp::Union)
}

/// ZINTER numkeys key [key ...] [WEIGHTS w1 ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
pub fn zinter_command(ctx: &mut CommandContext) -> RedisResult<()> {
    algebra_inner(ctx, b"zinter", AlgebraOp::Inter)
}

/// ZDIFF numkeys key [key ...] [WITHSCORES]
pub fn zdiff_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"zdiff"));
    }
    let numkeys = parse_strict_i64(ctx.arg(1)?.as_bytes())?;
    if numkeys <= 0 {
        return Err(RedisError::runtime(
            b"ERR at least 1 input key is needed for 'zdiff' command",
        ));
    }
    let numkeys = numkeys as usize;
    if argc < 2 + numkeys {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let mut withscores = false;
    let trailing = argc - (2 + numkeys);
    if trailing == 1 {
        if !ctx
            .arg(2 + numkeys)?
            .as_bytes()
            .eq_ignore_ascii_case(b"WITHSCORES")
        {
            return Err(RedisError::syntax(b"syntax error"));
        }
        withscores = true;
    } else if trailing > 1 {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let sources = collect_zalgebra_sources(ctx, 2, numkeys)?;
    let result = zdiff_inner(sources);
    emit_zalgebra_reply(ctx, result, withscores)
}

/// ZINTERCARD numkeys key [key ...] [LIMIT N]
pub fn zintercard_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"zintercard"));
    }
    let numkeys = parse_strict_i64(ctx.arg(1)?.as_bytes())?;
    if numkeys <= 0 {
        return Err(RedisError::runtime(
            b"ERR at least 1 input key is needed for 'zintercard' command",
        ));
    }
    let numkeys = numkeys as usize;
    if argc < 2 + numkeys {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let mut limit: i64 = 0;
    let trailing_start = 2 + numkeys;
    if argc == trailing_start + 2 {
        let opt = ctx.arg(trailing_start)?;
        if !opt.as_bytes().eq_ignore_ascii_case(b"LIMIT") {
            return Err(RedisError::syntax(b"syntax error"));
        }
        let n = parse_strict_i64(ctx.arg(trailing_start + 1)?.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR LIMIT value is not a valid integer"))?;
        if n < 0 {
            return Err(RedisError::runtime(b"ERR LIMIT can't be negative"));
        }
        limit = n;
    } else if argc != trailing_start {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let sources = collect_zalgebra_sources(ctx, 2, numkeys)?;
    let opts = ZAlgebraOpts {
        weights: vec![1.0; numkeys],
        aggregate: Aggregate::Sum,
        withscores: false,
    };
    let result = zinter_inner(sources, &opts);
    let card = if limit > 0 {
        (result.len() as i64).min(limit)
    } else {
        result.len() as i64
    };
    ctx.reply_integer(card)
}

/// ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
///
/// Computes the same range as `ZRANGE` would, then stores the resulting
/// `(member, score)` pairs at `dst`. Empty results delete `dst`.
pub fn zrangestore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 5 {
        return Err(RedisError::wrong_number_of_args(b"zrangestore"));
    }
    let dst = ctx.arg_owned(1usize)?;
    let src = ctx.arg_owned(2usize)?;
    let start_bytes = ctx.arg_owned(3usize)?;
    let stop_bytes = ctx.arg_owned(4usize)?;

    let mut by_score = false;
    let mut by_lex = false;
    let mut reverse = false;
    let mut offset: i64 = 0;
    let mut count: i64 = -1;
    let mut have_limit = false;

    let mut idx = 5usize;
    while idx < argc {
        let opt = ctx.arg(idx)?;
        let bytes = opt.as_bytes();
        if bytes.eq_ignore_ascii_case(b"BYSCORE") {
            by_score = true;
            idx += 1;
        } else if bytes.eq_ignore_ascii_case(b"BYLEX") {
            by_lex = true;
            idx += 1;
        } else if bytes.eq_ignore_ascii_case(b"REV") {
            reverse = true;
            idx += 1;
        } else if bytes.eq_ignore_ascii_case(b"LIMIT") {
            if idx + 2 >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            offset = parse_strict_i64(ctx.arg(idx + 1)?.as_bytes())?;
            count = parse_strict_i64(ctx.arg(idx + 2)?.as_bytes())?;
            have_limit = true;
            idx += 3;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    if have_limit && !by_score && !by_lex {
        return Err(RedisError::runtime(
            b"ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }

    let entries: Vec<(RedisString, f64)> = if by_lex {
        let (min, max) = if reverse {
            (
                parse_lex_bound(stop_bytes.as_bytes())?,
                parse_lex_bound(start_bytes.as_bytes())?,
            )
        } else {
            (
                parse_lex_bound(start_bytes.as_bytes())?,
                parse_lex_bound(stop_bytes.as_bytes())?,
            )
        };
        let mut items: Vec<(f64, RedisString)> = match as_zset_ref(ctx.db().lookup_key_read(&src))?
        {
            None => Vec::new(),
            Some(z) => z
                .iter_ascending()
                .filter(|(_, m)| {
                    lex_above_min(m.as_bytes(), &min) && lex_below_max(m.as_bytes(), &max)
                })
                .map(|(s, m)| (s, m.clone()))
                .collect(),
        };
        if reverse {
            items.reverse();
        }
        apply_limit(items, offset, count)
            .into_iter()
            .map(|(s, m)| (m, s))
            .collect()
    } else if by_score {
        let (a_score, a_excl) = parse_score_range(start_bytes.as_bytes())?;
        let (b_score, b_excl) = parse_score_range(stop_bytes.as_bytes())?;
        let (min, min_excl, max, max_excl) = if reverse {
            (b_score, b_excl, a_score, a_excl)
        } else {
            (a_score, a_excl, b_score, b_excl)
        };
        let mut items: Vec<(f64, RedisString)> = match as_zset_ref(ctx.db().lookup_key_read(&src))?
        {
            None => Vec::new(),
            Some(z) => z
                .iter_ascending()
                .filter(|(s, _)| score_in_range(*s, min, min_excl, max, max_excl))
                .map(|(s, m)| (s, m.clone()))
                .collect(),
        };
        if reverse {
            items.reverse();
        }
        apply_limit(items, offset, count)
            .into_iter()
            .map(|(s, m)| (m, s))
            .collect()
    } else {
        let start = parse_strict_i64(start_bytes.as_bytes())?;
        let stop = parse_strict_i64(stop_bytes.as_bytes())?;
        let items: Vec<(f64, RedisString)> = match as_zset_ref(ctx.db().lookup_key_read(&src))? {
            None => Vec::new(),
            Some(z) => {
                let len = z.len() as i64;
                match clamp_rank_range(start, stop, len) {
                    None => Vec::new(),
                    Some((lo, hi)) => {
                        if reverse {
                            z.iter_ascending()
                                .rev()
                                .skip(lo)
                                .take(hi - lo + 1)
                                .map(|(s, m)| (s, m.clone()))
                                .collect()
                        } else {
                            z.iter_ascending()
                                .skip(lo)
                                .take(hi - lo + 1)
                                .map(|(s, m)| (s, m.clone()))
                                .collect()
                        }
                    }
                }
            }
        };
        items.into_iter().map(|(s, m)| (m, s)).collect()
    };

    let stored = store_zset(ctx, dst.clone(), entries);
    ctx.notify_keyspace_event(NOTIFY_ZSET, b"zrangestore", &dst);
    ctx.reply_integer(stored)
}

static ZRANDMEMBER_CURSOR: AtomicU64 = AtomicU64::new(0);

/// Parse a ZRANDMEMBER count argument, applying Redis's range rules.
fn parse_zrandmember_count(bytes: &[u8]) -> Result<i64, RedisError> {
    let v = parse_strict_i64(bytes)?;
    if v == i64::MIN {
        return Err(RedisError::out_of_range());
    }
    Ok(v)
}

fn next_zrandmember_start(len: usize) -> usize {
    if len == 0 {
        0
    } else {
        (ZRANDMEMBER_CURSOR.fetch_add(1, Ordering::Relaxed) as usize) % len
    }
}

/// ZRANDMEMBER key [count [WITHSCORES]]
///
/// Uses a rotating cursor over the sorted-set snapshot. This is not true
/// server PRNG sampling, but it preserves the Redis reply shapes and covers
/// all members under repeated calls until PRNG state is exposed here.
pub fn zrandmember_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 || argc > 4 {
        return Err(RedisError::wrong_number_of_args(b"zrandmember"));
    }
    let key = ctx.arg_owned(1usize)?;
    let count_opt: Option<i64> = if argc >= 3 {
        Some(parse_zrandmember_count(ctx.arg(2)?.as_bytes())?)
    } else {
        None
    };
    let withscores: bool = if argc == 4 {
        if !ctx.arg(3)?.as_bytes().eq_ignore_ascii_case(b"WITHSCORES") {
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
        if withscores && (count < -(i64::MAX / 2) || count > i64::MAX / 2) {
            return Err(RedisError::runtime(b"ERR value is out of range"));
        }
    }

    let entries: Vec<(f64, RedisString)> = match as_zset_ref(ctx.db().lookup_key_read(&key))? {
        None => Vec::new(),
        Some(z) => z.iter_ascending().map(|(s, m)| (s, m.clone())).collect(),
    };

    match count_opt {
        None => {
            if entries.is_empty() {
                return ctx.reply_null_bulk();
            }
            let (_, m) = &entries[next_zrandmember_start(entries.len())];
            ctx.reply_bulk_string(m.clone())
        }
        Some(count) => {
            if entries.is_empty() || count == 0 {
                return ctx.reply_array_header(0usize);
            }
            let mut emitted: Vec<(f64, RedisString)> = Vec::new();
            let start = next_zrandmember_start(entries.len());
            if count > 0 {
                let take = (count as usize).min(entries.len());
                for i in 0..take {
                    emitted.push(entries[(start + i) % entries.len()].clone());
                }
            } else {
                let take = count.unsigned_abs() as usize;
                for i in 0..take {
                    emitted.push(entries[(start + i) % entries.len()].clone());
                }
            }

            if withscores && ctx.client_ref().resp_proto == 3 {
                ctx.reply_array_header(emitted.len())?;
                for (s, m) in emitted {
                    ctx.reply_array_header(2usize)?;
                    ctx.reply_bulk_string(m)?;
                    ctx.reply_double(s)?;
                }
            } else if withscores {
                ctx.reply_array_header(emitted.len() * 2)?;
                for (s, m) in emitted {
                    ctx.reply_bulk_string(m)?;
                    ctx.reply_bulk(&format_score(s))?;
                }
            } else {
                ctx.reply_array_header(emitted.len())?;
                for (_s, m) in emitted {
                    ctx.reply_bulk_string(m)?;
                }
            }
            Ok(())
        }
    }
}

/// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
///
/// Pops up to `count` (default 1) members from the first non-empty source
/// key. Replies with `[key, [[member, score], ...]]`, or `*-1\r\n` when
/// every supplied key is empty or missing.
pub fn zmpop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(b"zmpop"));
    }
    let numkeys = parse_strict_i64(ctx.arg(1)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR numkeys value is not an integer or out of range"))?;
    if numkeys <= 0 {
        return Err(RedisError::runtime(b"ERR numkeys should be greater than 0"));
    }
    let numkeys = numkeys as usize;
    if argc < 3 + numkeys {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let where_arg = ctx.arg(1 + numkeys + 1)?.clone();
    let reverse = if where_arg.as_bytes().eq_ignore_ascii_case(b"MIN") {
        false
    } else if where_arg.as_bytes().eq_ignore_ascii_case(b"MAX") {
        true
    } else {
        return Err(RedisError::syntax(b"syntax error"));
    };
    let mut count: i64 = 1;
    let trailing_start = 1 + numkeys + 2;
    if argc == trailing_start + 2 {
        let opt = ctx.arg(trailing_start)?;
        if !opt.as_bytes().eq_ignore_ascii_case(b"COUNT") {
            return Err(RedisError::syntax(b"syntax error"));
        }
        let n = parse_strict_i64(ctx.arg(trailing_start + 1)?.as_bytes()).map_err(|_| {
            RedisError::runtime(b"ERR count value is not an integer or out of range")
        })?;
        if n < 1 {
            return Err(RedisError::runtime(b"ERR count should be greater than 0"));
        }
        count = n;
    } else if argc != trailing_start {
        return Err(RedisError::syntax(b"syntax error"));
    }

    let mut keys: Vec<RedisString> = Vec::with_capacity(numkeys);
    for j in 0..numkeys {
        keys.push(ctx.arg(2 + j)?.clone());
    }

    for key in keys {
        if let Some(o) = ctx.db().lookup_key_read(&key) {
            if !o.is_zset() {
                return Err(RedisError::wrong_type());
            }
        }
        let popped: Vec<(f64, RedisString)> = {
            let zset = match as_zset_mut(ctx.db_mut().lookup_key_write(&key))? {
                None => continue,
                Some(z) => z,
            };
            if zset.is_empty() {
                continue;
            }
            let take = (count as usize).min(zset.len());
            let mut targets: Vec<(f64, RedisString)> = Vec::with_capacity(take);
            if reverse {
                for (s, m) in zset.iter_ascending().rev().take(take) {
                    targets.push((s, m.clone()));
                }
            } else {
                for (s, m) in zset.iter_ascending().take(take) {
                    targets.push((s, m.clone()));
                }
            }
            for (_, m) in &targets {
                zset.remove(m);
            }
            targets
        };
        if popped.is_empty() {
            continue;
        }
        delete_if_empty(ctx, &key);
        ctx.reply_array_header(2usize)?;
        ctx.reply_bulk_string(key)?;
        ctx.reply_array_header(popped.len())?;
        for (s, m) in popped {
            ctx.reply_array_header(2usize)?;
            ctx.reply_bulk_string(m)?;
            ctx.reply_double(s)?;
        }
        return Ok(());
    }
    ctx.reply_null_array()
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count] [NOSCORES]
///
/// Linear-cursor iteration over the `(member, score)` pairs of a sorted
/// set in ascending score order. Reply shape mirrors HSCAN — a two-element
/// array of `[next_cursor, items]` where `items` is interleaved
/// `member, score` bulks unless `NOSCORES` requests member-only output.
///
/// TODO(architect): MATCH currently filters on member bytes only. Real
/// Redis also surfaces the score in `OBJ_ENCODING_LISTPACK` zsets but the
/// glob matcher's input is the member key.
pub fn zscan_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"zscan"));
    }
    let key = ctx.arg_owned(1usize)?;
    let cursor = parse_u64_cursor(ctx.arg(2)?.as_bytes())?;

    let mut pattern: Option<Vec<u8>> = None;
    let mut count: i64 = 10;
    let mut no_scores = false;
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
        } else if bytes.eq_ignore_ascii_case(b"NOSCORES") {
            no_scores = true;
            j += 1;
        } else if bytes.eq_ignore_ascii_case(b"NOVALUES") {
            return Err(RedisError::runtime(
                b"ERR NOVALUES option can only be used in HSCAN",
            ));
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    let entries: Vec<(f64, RedisString)> = match as_zset_ref(ctx.db().lookup_key_read(&key))? {
        None => Vec::new(),
        Some(z) => z.iter_ascending().map(|(s, m)| (s, m.clone())).collect(),
    };
    let total = entries.len() as u64;
    let start = cursor as usize;
    let stop = (start + count as usize).min(entries.len());
    let next_cursor: u64 = if stop as u64 >= total { 0 } else { stop as u64 };

    let mut matched: Vec<(f64, RedisString)> = Vec::new();
    for (s, m) in entries.into_iter().skip(start).take(count as usize) {
        if let Some(ref pat) = pattern {
            if !glob_match(pat, m.as_bytes()) {
                continue;
            }
        }
        matched.push((s, m));
    }

    ctx.reply_array_header(2usize)?;
    ctx.reply_bulk(next_cursor.to_string().as_bytes())?;
    let header = if no_scores {
        matched.len()
    } else {
        matched.len() * 2
    };
    ctx.reply_array_header(header)?;
    for (s, m) in matched {
        ctx.reply_bulk_string(m)?;
        if !no_scores {
            ctx.reply_bulk(&format_score(s))?;
        }
    }
    Ok(())
}

/// Parse an unsigned decimal cursor.
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

/// Pop one (member, score) pair from the zset at `key`.
///
/// Returns the lowest-scored member when `reverse` is false (BZPOPMIN),
/// or the highest-scored member when `reverse` is true (BZPOPMAX). Returns
/// `None` when the key is absent or holds an empty zset. Deletes the key when
/// the pop leaves it empty.
fn zset_pop_one(db: &mut RedisDb, key: &RedisString, reverse: bool) -> Option<(RedisString, f64)> {
    let zset = db.lookup_key_write(key)?.zset_mut()?;
    let target: (f64, RedisString) = if reverse {
        zset.iter_ascending()
            .next_back()
            .map(|(s, m)| (s, m.clone()))?
    } else {
        zset.iter_ascending().next().map(|(s, m)| (s, m.clone()))?
    };
    let (score, member) = target;
    zset.remove(&member);
    let empty = matches!(
        db.lookup_key_read(key),
        Some(o) if o.zset().map(|z| z.is_empty()).unwrap_or(false)
    );
    if empty {
        db.sync_delete(key);
    }
    Some((member, score))
}

/// Pop up to `count` (member, score) pairs from the zset at `key`.
///
/// Returns an empty vec when the key is absent or holds an empty zset. Deletes
/// the key when the pops leave it empty.
fn zset_pop_many(
    db: &mut RedisDb,
    key: &RedisString,
    reverse: bool,
    count: usize,
) -> Vec<(RedisString, f64)> {
    let zset = match db.lookup_key_write(key).and_then(|o| o.zset_mut()) {
        Some(z) => z,
        None => return Vec::new(),
    };
    let take = count.min(zset.len());
    let mut targets: Vec<(f64, RedisString)> = Vec::with_capacity(take);
    if reverse {
        for (s, m) in zset.iter_ascending().rev().take(take) {
            targets.push((s, m.clone()));
        }
    } else {
        for (s, m) in zset.iter_ascending().take(take) {
            targets.push((s, m.clone()));
        }
    }
    for (_, m) in &targets {
        zset.remove(m);
    }
    let out: Vec<(RedisString, f64)> = targets.into_iter().map(|(s, m)| (m, s)).collect();
    let empty = matches!(
        db.lookup_key_read(key),
        Some(o) if o.zset().map(|z| z.is_empty()).unwrap_or(false)
    );
    if empty {
        db.sync_delete(key);
    }
    out
}

/// Append an f64 score in the appropriate wire format for `resp_proto`.
///
/// RESP2: `$N\r\n<text>\r\n` (bulk string).
/// RESP3: `,<text>\r\n` (double).
fn append_score_frame(buf: &mut Vec<u8>, score: f64, resp_proto: i32) {
    let text = redis_protocol::frame::format_double_text(score);
    if resp_proto == 3 {
        buf.push(b',');
        buf.extend_from_slice(&text);
        buf.extend_from_slice(b"\r\n");
    } else {
        buf.push(b'$');
        buf.extend_from_slice(text.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(&text);
        buf.extend_from_slice(b"\r\n");
    }
}

/// Encode a `*3 [key, member, score]` BZPOPMIN/BZPOPMAX reply.
fn encode_bzpop_reply(
    key: &RedisString,
    member: &RedisString,
    score: f64,
    resp_proto: i32,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64 + key.len() + member.len());
    buf.extend_from_slice(b"*3\r\n$");
    buf.extend_from_slice(key.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(key.as_bytes());
    buf.extend_from_slice(b"\r\n$");
    buf.extend_from_slice(member.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(member.as_bytes());
    buf.extend_from_slice(b"\r\n");
    append_score_frame(&mut buf, score, resp_proto);
    buf
}

/// Encode a `*2 [key, *N [[m1,s1],[m2,s2],...]]` BZMPOP wake reply.
fn encode_bzmpop_reply(
    key: &RedisString,
    pairs: &[(RedisString, f64)],
    resp_proto: i32,
) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    buf.extend_from_slice(b"*2\r\n$");
    buf.extend_from_slice(key.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(key.as_bytes());
    buf.extend_from_slice(b"\r\n*");
    buf.extend_from_slice(pairs.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    for (member, score) in pairs {
        buf.extend_from_slice(b"*2\r\n$");
        buf.extend_from_slice(member.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(member.as_bytes());
        buf.extend_from_slice(b"\r\n");
        append_score_frame(&mut buf, *score, resp_proto);
    }
    buf
}

/// Deliver a popped zset element or set of elements to a woken waiter.
///
/// Called from `list::deliver_to_waiter` when the action is `ZSetPop`. When
/// `count == 0` (BZPOPMIN / BZPOPMAX shape) pops one element and replies
/// `*3 [key, member, score]`. When `count >= 1` (BZMPOP shape) pops up to
/// `count` elements and replies `*2 [key, *N [[m1,s1],...]]`.
///
/// If the sender is gone the popped values are restored by re-inserting them
/// into the zset (creating it first when needed), preserving fairness for the
/// next waiter.
pub fn deliver_zset_to_waiter(db: &mut RedisDb, key: &RedisString, waiter: BlockedWaiter) {
    let (reverse, count) = match waiter.action {
        BlockedAction::ZSetPop { reverse, count } => (reverse, count),
        _ => return,
    };
    if count == 0 {
        let pair = match zset_pop_one(db, key, reverse) {
            Some(p) => p,
            None => return,
        };
        let reply = encode_bzpop_reply(key, &pair.0, pair.1, waiter.resp_proto);
        if waiter.sender.send(reply).is_err() {
            match db.lookup_key_write(key) {
                Some(obj) => {
                    if let Some(z) = obj.zset_mut() {
                        z.upsert(pair.0, pair.1);
                    }
                }
                None => {
                    let mut obj = RedisObject::new_zset();
                    if let Some(z) = obj.zset_mut() {
                        z.upsert(pair.0, pair.1);
                    }
                    db.set_key(key.clone(), obj, 0);
                }
            }
        }
    } else {
        let pairs = zset_pop_many(db, key, reverse, count as usize);
        if pairs.is_empty() {
            return;
        }
        let reply = encode_bzmpop_reply(key, &pairs, waiter.resp_proto);
        if waiter.sender.send(reply).is_err() {
            match db.lookup_key_write(key) {
                Some(obj) => {
                    if let Some(z) = obj.zset_mut() {
                        for (m, s) in pairs {
                            z.upsert(m, s);
                        }
                    }
                }
                None => {
                    let mut obj = RedisObject::new_zset();
                    if let Some(z) = obj.zset_mut() {
                        for (m, s) in pairs {
                            z.upsert(m, s);
                        }
                    }
                    db.set_key(key.clone(), obj, 0);
                }
            }
        }
    }
}

/// Wake blocked zset waiters after data is added to `key`.
///
/// Should be called by ZADD and ZINCRBY after successfully inserting into a
/// zset that has blocked waiters. Mirrors `list::wake_blocked_for_key` but
/// dispatches through `deliver_zset_to_waiter`.
pub fn wake_blocked_zset_for_key(db: &mut RedisDb, key: &RedisString) {
    loop {
        let has_data = matches!(
            db.lookup_key_read(key),
            Some(o) if o.zset().map(|z| !z.is_empty()).unwrap_or(false)
        );
        if !has_data {
            return;
        }
        let waiter = {
            let mut idx = match blocked_keys_index().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            match idx.peek_zset_waiter(key) {
                Some(w) => w,
                None => return,
            }
        };
        deliver_zset_to_waiter(db, key, waiter);
    }
}

/// Parse a BLPOP-style timeout value (decimal seconds, non-negative).
///
/// Accepts integer and floating-point. Rejects negative values and
/// non-numeric input with the canonical Redis error messages.
fn parse_blocking_timeout_zset(bytes: &[u8]) -> Result<f64, RedisError> {
    let s = core::str::from_utf8(bytes)
        .map_err(|_| RedisError::runtime(b"ERR timeout is not a float or out of range"))?;
    if s.starts_with(char::is_whitespace) || s.ends_with(char::is_whitespace) {
        return Err(RedisError::runtime(
            b"ERR timeout is not a float or out of range",
        ));
    }
    let parsed = s
        .parse::<f64>()
        .map_err(|_| RedisError::runtime(b"ERR timeout is not a float or out of range"))?;
    if !parsed.is_finite() {
        return Err(RedisError::runtime(
            b"ERR timeout is not a float or out of range",
        ));
    }
    if parsed < 0.0 {
        return Err(RedisError::runtime(b"ERR timeout is negative"));
    }
    let ms = parsed * 1000.0;
    if ms > i64::MAX as f64 {
        return Err(RedisError::runtime(b"ERR timeout is out of range"));
    }
    Ok(parsed)
}

/// Park a client that is blocked waiting on one or more zset keys.
///
/// Mirrors `list::park_blocked_client` but the action is always `ZSetPop`.
fn park_zset_blocked_client(
    ctx: &mut CommandContext,
    keys: Vec<RedisString>,
    reverse: bool,
    count: u64,
    timeout_secs: f64,
) -> RedisResult<()> {
    if ctx.client_ref().flag_deny_blocking() {
        return ctx.reply_null_array();
    }
    let registry = match ctx.pubsub.as_ref() {
        Some(r) => r.clone(),
        None => return ctx.reply_null_array(),
    };
    let sender = {
        let guard = match registry.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.sender_for(ctx.client_ref().id)
    };
    let sender = match sender {
        Some(s) => s,
        None => return ctx.reply_null_array(),
    };
    let waiter = BlockedWaiter {
        client_id: ctx.client_ref().id,
        sender,
        keys: keys.clone(),
        action: BlockedAction::ZSetPop { reverse, count },
        deadline_ms: deadline_from_timeout_secs(timeout_secs),
        resp_proto: ctx.client_ref().resp_proto,
        username: ctx.client_ref().authenticated_user.clone(),
    };
    {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.add(waiter);
    }
    ctx.client_mut().blocked_on_keys = true;
    Ok(())
}

/// Shared body for BZPOPMIN and BZPOPMAX.
///
/// For each key in order: if the zset is non-empty, pop the
/// lowest-scored (or highest-scored when `reverse`) member and reply
/// `*3 [key, member, score]` immediately. When every key is empty the
/// client is parked in the global blocked-keys index until either a
/// ZADD/ZINCRBY wakes it or the timeout elapses (replying `*-1`).
fn bzpop_generic(ctx: &mut CommandContext, reverse: bool) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let timeout_raw = ctx.arg_owned(argc - 1)?;
    let timeout_secs = parse_blocking_timeout_zset(timeout_raw.as_bytes())?;
    let mut keys: Vec<RedisString> = Vec::with_capacity(argc - 2);
    for j in 1..(argc - 1) {
        keys.push(ctx.arg_owned(j)?);
    }
    for key in &keys {
        match ctx.db().find(key) {
            None => continue,
            Some(o) => {
                if !o.is_zset() {
                    return Err(RedisError::wrong_type());
                }
                if o.zset().map(|z| z.is_empty()).unwrap_or(true) {
                    continue;
                }
            }
        }
        let pair = match zset_pop_one(ctx.db_mut(), key, reverse) {
            Some(p) => p,
            None => continue,
        };
        let empty_after = ctx.db().lookup_key_read(key).is_none();
        let event = if reverse {
            b"zpopmax" as &[u8]
        } else {
            b"zpopmin" as &[u8]
        };
        ctx.notify_keyspace_event(NOTIFY_ZSET, event, key);
        if empty_after {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", key);
        }
        ctx.reply_array_header(3usize)?;
        ctx.reply_bulk_string(key.clone())?;
        ctx.reply_bulk_string(pair.0)?;
        return ctx.reply_double(pair.1);
    }
    park_zset_blocked_client(ctx, keys, reverse, 0, timeout_secs)
}

/// BZPOPMIN key [key ...] timeout
///
/// Pops the lowest-scored member from the first non-empty key. Blocks the
/// client when all listed keys are empty. Replies `*3 [key, member, score]`
/// on success or `*-1` on timeout.
pub fn bzpopmin_command(ctx: &mut CommandContext) -> RedisResult<()> {
    bzpop_generic(ctx, false)
}

/// BZPOPMAX key [key ...] timeout
///
/// Pops the highest-scored member from the first non-empty key. Blocks the
/// client when all listed keys are empty. Replies `*3 [key, member, score]`
/// on success or `*-1` on timeout.
pub fn bzpopmax_command(ctx: &mut CommandContext) -> RedisResult<()> {
    bzpop_generic(ctx, true)
}

/// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
///
/// When some key has data: pops up to `count` (default 1) members from the
/// first non-empty key and replies `*2 [key, [[m1,s1],...]]`. Otherwise
/// parks the client on every supplied key; a later ZADD on any one wakes the
/// waiter and satisfies the `count` request. Timeout `0` blocks forever.
pub fn bzmpop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 5 {
        return Err(RedisError::wrong_number_of_args(b"bzmpop"));
    }
    let timeout_raw = ctx.arg_owned(1usize)?;
    let timeout_secs = parse_blocking_timeout_zset(timeout_raw.as_bytes())?;
    let numkeys_signed = parse_strict_i64(ctx.arg(2)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR numkeys should be greater than 0"))?;
    if numkeys_signed <= 0 {
        return Err(RedisError::runtime(b"ERR numkeys should be greater than 0"));
    }
    let numkeys = numkeys_signed as usize;
    if numkeys + 4 > argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let mut keys: Vec<RedisString> = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(ctx.arg_owned(3 + i)?);
    }
    let dir_arg = ctx.arg(3 + numkeys)?;
    let reverse = if dir_arg.as_bytes().eq_ignore_ascii_case(b"MIN") {
        false
    } else if dir_arg.as_bytes().eq_ignore_ascii_case(b"MAX") {
        true
    } else {
        return Err(RedisError::syntax(b"syntax error"));
    };
    let mut count: i64 = 1;
    let mut got_count = false;
    let mut j = 4 + numkeys;
    while j < argc {
        let opt = ctx.arg(j)?;
        if !opt.as_bytes().eq_ignore_ascii_case(b"COUNT") {
            return Err(RedisError::syntax(b"syntax error"));
        }
        if got_count || j + 1 >= argc {
            return Err(RedisError::syntax(b"syntax error"));
        }
        count = parse_strict_i64(ctx.arg(j + 1)?.as_bytes())
            .map_err(|_| RedisError::runtime(b"ERR count should be greater than 0"))?;
        if count <= 0 {
            return Err(RedisError::runtime(b"ERR count should be greater than 0"));
        }
        got_count = true;
        j += 2;
    }
    for key in &keys {
        match ctx.db().find(key) {
            Some(o) if !o.is_zset() => return Err(RedisError::wrong_type()),
            Some(o) if o.zset().map(|z| z.is_empty()).unwrap_or(true) => continue,
            None => continue,
            Some(_) => {}
        }
        let pairs = zset_pop_many(ctx.db_mut(), key, reverse, count as usize);
        if pairs.is_empty() {
            continue;
        }
        let empty_after = ctx.db().lookup_key_read(key).is_none();
        let event = if reverse {
            b"zpopmax" as &[u8]
        } else {
            b"zpopmin" as &[u8]
        };
        ctx.notify_keyspace_event(NOTIFY_ZSET, event, key);
        if empty_after {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", key);
        }
        ctx.reply_array_header(2usize)?;
        ctx.reply_bulk_string(key.clone())?;
        ctx.reply_array_header(pairs.len())?;
        for (member, score) in pairs {
            ctx.reply_array_header(2usize)?;
            ctx.reply_bulk_string(member)?;
            ctx.reply_double(score)?;
        }
        return Ok(());
    }
    park_zset_blocked_client(ctx, keys, reverse, count as u64, timeout_secs)
}

/// Emit a wake to any blocked zset waiters on `key` after a successful ZADD
/// or ZINCRBY that added data to the zset.
pub fn schedule_or_wake_zset(ctx: &mut CommandContext, key: &RedisString) {
    if ctx.client_ref().flag_deny_blocking() {
        ctx.client_mut().pending_wakes.push(key.clone());
    } else {
        wake_blocked_zset_for_key(ctx.db_mut(), key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_core::Client;

    fn arg(bytes: &[u8]) -> RedisString {
        RedisString::from_bytes(bytes)
    }

    #[test]
    fn zscan_noscores_omits_score_bulks() {
        let key = arg(b"zs");
        let mut obj = RedisObject::new_zset();
        {
            let z = obj.zset_mut().expect("new_zset constructs an Inline zset");
            z.upsert(arg(b"one"), 1.0);
            z.upsert(arg(b"two"), 2.0);
        }

        let mut db = RedisDb::new(0);
        db.set_key(key.clone(), obj, 0);

        let mut client = Client::new(1);
        client.set_args(vec![arg(b"zscan"), key, arg(b"0"), arg(b"noscores")]);

        let mut ctx = CommandContext::with_db(&mut client, &mut db);
        zscan_command(&mut ctx).expect("ZSCAN NOSCORES should succeed");

        assert_eq!(
            ctx.client_ref().reply_buf.as_slice(),
            b"*2\r\n$1\r\n0\r\n*2\r\n$3\r\none\r\n$3\r\ntwo\r\n"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/t_zset.c
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         5
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Round 5 byte-exact implementations for ZADD, ZSCORE,
//                  ZMSCORE, ZCARD, ZINCRBY, ZRANGE, ZRANGEBYSCORE,
//                  ZREVRANGE, ZREVRANGEBYSCORE, ZRANK, ZREVRANK, ZREM,
//                  ZCOUNT, ZPOPMIN, ZPOPMAX, ZREMRANGEBYRANK, and
//                  ZREMRANGEBYSCORE backed by the pragmatic
//                  ZSetEncoding::Inline encoding from redis-core::object.
//                  Score formatting uses Rust's f64 Display plus integer
//                  shortcut; Phase 4 will install a %.17g-faithful
//                  helper. ZRANGEBYLEX / ZREMRANGEBYLEX / ZLEXCOUNT and
//                  ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE / ZUNION /
//                  ZINTER / ZDIFF / ZINTERCARD / ZRANDMEMBER / ZMPOP /
//                  and ZSCAN backed by InlineZSet. BZPOPMIN / BZPOPMAX
//                  land in follow-on rounds.
// ──────────────────────────────────────────────────────────────────────────
