//! Stream commands — Round 9 byte-exact port.
//!
//! Implements: XADD, XLEN, XRANGE, XREVRANGE, XREAD (non-blocking),
//! XDEL, XTRIM, XINFO STREAM (basic), XINFO GROUPS (empty).
//!
//! C source: `reference/valkey/src/t_stream.c`
//!
//! # Storage shape
//!
//! Uses the pragmatic `ObjectKind::Stream(StreamEncoding::Inline(_))`
//! encoding from `redis-core::object` — a sorted `Vec<StreamEntry>` in
//! `redis_ds::stream::InlineStream`. Phase 5 swaps this for the real
//! `rax` + `listpack` representation.
//!
//! # Architect items
//!
//! TODO(architect): consumer-group commands XGROUP / XREADGROUP /
//! XACK / XPENDING / XCLAIM / XAUTOCLAIM / XSETID / XINFO CONSUMERS
//! require persistent PEL state and are deferred.
//!
//! TODO(architect): XREAD BLOCK requires the `blockForKeys` infrastructure
//! from `redis-core/src/blocked.rs` which is not yet wired into dispatch.
//! Non-blocking semantics are full; `$` id with no new entries returns
//! `$-1` (nil) instead of blocking.
//!
//! TODO(architect): MAXLEN/MINID `~` approximate trimming behaves
//! identically to `=` exact trimming for the inline encoding (no
//! listpack-boundary quirks to honour).

use redis_core::command_context::CommandContext;
use redis_core::object::RedisObject;
use redis_core::util::mstime;
use redis_ds::stream::{parse_stream_id, InlineStream, StreamEntry, StreamId};
use redis_types::{RedisError, RedisResult, RedisString};

// ─────────────────────────────────────────────────────────────────────────────
// ID parsing
// ─────────────────────────────────────────────────────────────────────────────

/// Side of a range bound: `Start` defaults missing seq to 0, `End`
/// defaults missing seq to `u64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundSide {
    Start,
    End,
}

/// A range-bound parsed from XRANGE/XREVRANGE.
///
/// `Inclusive`/`Exclusive` capture the `(` prefix used by streams to
/// request exclusive bounds. `Min`/`Max` are the `-` / `+` sentinels.
#[derive(Debug, Clone, Copy)]
enum Bound {
    Min,
    Max,
    Inclusive(StreamId),
    Exclusive(StreamId),
}

fn parse_range_bound(arg: &[u8], side: BoundSide) -> Result<Bound, RedisError> {
    if arg == b"-" {
        return Ok(Bound::Min);
    }
    if arg == b"+" {
        return Ok(Bound::Max);
    }
    let (exclusive, body) = if let Some(rest) = arg.strip_prefix(b"(") {
        (true, rest)
    } else {
        (false, arg)
    };
    let default_seq = match side {
        BoundSide::Start => 0,
        BoundSide::End => u64::MAX,
    };
    let id = parse_stream_id(body, default_seq).map_err(|_| invalid_stream_id_err())?;
    if exclusive {
        Ok(Bound::Exclusive(id))
    } else {
        Ok(Bound::Inclusive(id))
    }
}

/// Parse an explicit id given to XADD/XDEL/XREAD (no `-` / `+` / `(`
/// prefixes; seq defaults to 0 when absent).
fn parse_explicit_id(arg: &[u8]) -> Result<StreamId, RedisError> {
    parse_stream_id(arg, 0).map_err(|_| invalid_stream_id_err())
}

fn invalid_stream_id_err() -> RedisError {
    RedisError::runtime(b"ERR Invalid stream ID specified as stream command argument")
}

// ─────────────────────────────────────────────────────────────────────────────
// Encoding helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Resolve the half-open `[start_idx, end_idx)` slice of `stream.entries`
/// covered by `start` and `end` inclusive-or-exclusive bounds.
///
/// Returns `None` if the requested range is empty (start > end after
/// resolution, or the slice is empty).
fn resolve_range(
    stream: &InlineStream,
    start: Bound,
    end: Bound,
) -> Option<(usize, usize)> {
    let entries = &stream.entries;
    if entries.is_empty() {
        return None;
    }
    let start_idx = match start {
        Bound::Min => 0,
        Bound::Max => return None,
        Bound::Inclusive(id) => stream.lower_bound(&id),
        Bound::Exclusive(id) => stream.upper_bound(&id),
    };
    let end_idx = match end {
        Bound::Min => return None,
        Bound::Max => entries.len(),
        Bound::Inclusive(id) => stream.upper_bound(&id),
        Bound::Exclusive(id) => stream.lower_bound(&id),
    };
    if start_idx >= end_idx {
        return None;
    }
    Some((start_idx, end_idx))
}

/// Borrow the inner `InlineStream` of a stream-encoded object, raising
/// `WRONGTYPE` if `obj` is any other kind.
fn as_stream_ref(obj: Option<&RedisObject>) -> Result<Option<&InlineStream>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => o.stream().map(Some).ok_or_else(RedisError::wrong_type),
    }
}

/// Mutable variant of `as_stream_ref`.
fn as_stream_mut(
    obj: Option<&mut RedisObject>,
) -> Result<Option<&mut InlineStream>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => {
            if o.is_stream() {
                Ok(o.stream_mut())
            } else {
                Err(RedisError::wrong_type())
            }
        }
    }
}

/// Reply with a single stream entry as `[id, [f1, v1, f2, v2, ...]]`.
fn reply_entry(ctx: &mut CommandContext, entry: &StreamEntry) -> RedisResult<()> {
    ctx.reply_array_header(2usize)?;
    ctx.reply_bulk_string(RedisString::from_vec(entry.id.to_display_bytes()))?;
    ctx.reply_array_header(entry.fields.len() * 2)?;
    for (f, v) in &entry.fields {
        ctx.reply_bulk_string(f.clone())?;
        ctx.reply_bulk_string(v.clone())?;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// XADD options + auto-id
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum TrimStrategy {
    None,
    MaxLen(i64),
    MinId(StreamId),
}

#[derive(Debug)]
struct AddOptions {
    no_mkstream: bool,
    trim: TrimStrategy,
}

/// Parse `[NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]]`
/// and return the index of the first remaining argument (the id or `*`).
fn parse_add_options(ctx: &CommandContext) -> Result<(AddOptions, usize), RedisError> {
    let mut opts = AddOptions {
        no_mkstream: false,
        trim: TrimStrategy::None,
    };
    let argc = ctx.arg_count();
    let mut i = 2usize;
    while i < argc {
        let arg = ctx.arg(i)?.as_bytes();
        if arg.eq_ignore_ascii_case(b"NOMKSTREAM") {
            opts.no_mkstream = true;
            i += 1;
            continue;
        }
        if arg.eq_ignore_ascii_case(b"MAXLEN") || arg.eq_ignore_ascii_case(b"MINID") {
            let is_maxlen = arg.eq_ignore_ascii_case(b"MAXLEN");
            i += 1;
            if i >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let mut peek = ctx.arg(i)?.as_bytes();
            if peek == b"=" || peek == b"~" {
                i += 1;
                if i >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                peek = ctx.arg(i)?.as_bytes();
            }
            opts.trim = if is_maxlen {
                let n = parse_strict_i64(peek)?;
                if n < 0 {
                    return Err(RedisError::syntax(
                        b"MAXLEN argument must be >= 0",
                    ));
                }
                TrimStrategy::MaxLen(n)
            } else {
                TrimStrategy::MinId(parse_explicit_id(peek)?)
            };
            i += 1;
            if i < argc && ctx.arg(i)?.as_bytes().eq_ignore_ascii_case(b"LIMIT") {
                i += 2;
                if i > argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
            }
            continue;
        }
        break;
    }
    Ok((opts, i))
}

/// Compute the auto-id for `XADD * ...`. Uses wall-clock ms and either
/// bumps the seq (same ms as last_id) or resets seq to 0 (new ms).
/// Falls back to incrementing last_id when the clock has gone backwards.
fn auto_next_id(last_id: StreamId) -> StreamId {
    let now_ms = mstime();
    let now = if now_ms < 0 { 0u64 } else { now_ms as u64 };
    if now > last_id.ms {
        StreamId { ms: now, seq: 0 }
    } else {
        match last_id.checked_succ() {
            Some(next) => next,
            None => last_id,
        }
    }
}

/// Apply a `MAXLEN N` or `MINID id` trim. Returns the count evicted.
fn apply_trim(stream: &mut InlineStream, strategy: TrimStrategy) -> usize {
    match strategy {
        TrimStrategy::None => 0,
        TrimStrategy::MaxLen(n) => {
            let n = if n < 0 { 0 } else { n as usize };
            let len = stream.entries.len();
            if len <= n {
                return 0;
            }
            let evict = len - n;
            for entry in stream.entries.drain(0..evict) {
                if entry.id > stream.max_deleted_id {
                    stream.max_deleted_id = entry.id;
                }
            }
            evict
        }
        TrimStrategy::MinId(min) => {
            let cut = stream.lower_bound(&min);
            if cut == 0 {
                return 0;
            }
            for entry in stream.entries.drain(0..cut) {
                if entry.id > stream.max_deleted_id {
                    stream.max_deleted_id = entry.id;
                }
            }
            cut
        }
    }
}

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

// ─────────────────────────────────────────────────────────────────────────────
// XADD
// ─────────────────────────────────────────────────────────────────────────────

/// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] id|* field value ...
pub fn xadd_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 5 {
        return Err(RedisError::wrong_number_of_args(b"xadd"));
    }
    let key = ctx.arg_owned(1usize)?;
    let (opts, mut idx) = parse_add_options(ctx)?;
    if idx >= ctx.arg_count() {
        return Err(RedisError::wrong_number_of_args(b"xadd"));
    }
    let id_arg = ctx.arg_owned(idx)?;
    idx += 1;
    let remaining = ctx.arg_count() - idx;
    if remaining == 0 || remaining % 2 != 0 {
        return Err(RedisError::wrong_number_of_args(b"xadd"));
    }
    let mut fields: Vec<(RedisString, RedisString)> = Vec::with_capacity(remaining / 2);
    while idx < ctx.arg_count() {
        let f = ctx.arg_owned(idx)?;
        let v = ctx.arg_owned(idx + 1)?;
        fields.push((f, v));
        idx += 2;
    }

    let existing = ctx.db_mut().lookup_key_write(&key);
    let already_exists = existing.is_some();
    if !already_exists && opts.no_mkstream {
        return ctx.reply_null_bulk();
    }

    let new_id = {
        let mut obj_owned: Option<RedisObject> = None;
        let stream: &mut InlineStream = if already_exists {
            let obj = ctx
                .db_mut()
                .lookup_key_write(&key)
                .ok_or_else(|| RedisError::runtime(b"ERR unreachable: key vanished"))?;
            if !obj.is_stream() {
                return Err(RedisError::wrong_type());
            }
            obj.stream_mut()
                .ok_or_else(|| RedisError::runtime(b"ERR unreachable: stream encoding lost"))?
        } else {
            obj_owned = Some(RedisObject::new_stream());
            obj_owned
                .as_mut()
                .and_then(|o| o.stream_mut())
                .ok_or_else(|| RedisError::runtime(b"ERR unreachable: new stream lost"))?
        };

        let id_bytes = id_arg.as_bytes();
        let new_id = if id_bytes == b"*" {
            auto_next_id(stream.last_id)
        } else {
            parse_explicit_id(id_bytes)?
        };
        if id_bytes != b"*" && new_id <= stream.last_id && !(stream.entries_added == 0 && new_id == StreamId::ZERO && stream.last_id == StreamId::ZERO) {
            return Err(RedisError::runtime(
                b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
            ));
        }
        if id_bytes != b"*" && new_id == StreamId::ZERO {
            return Err(RedisError::runtime(
                b"ERR The ID specified in XADD must be greater than 0-0",
            ));
        }
        stream.append(StreamEntry { id: new_id, fields });
        apply_trim(stream, opts.trim);
        if let Some(obj) = obj_owned {
            ctx.db_mut().set_key(key, obj, 0);
        }
        new_id
    };

    ctx.reply_bulk_string(RedisString::from_vec(new_id.to_display_bytes()))
}

// ─────────────────────────────────────────────────────────────────────────────
// XLEN
// ─────────────────────────────────────────────────────────────────────────────

/// XLEN key
pub fn xlen_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"xlen"));
    }
    let key = ctx.arg_owned(1usize)?;
    let len = match as_stream_ref(ctx.db().lookup_key_read(&key))? {
        None => 0,
        Some(s) => s.len() as i64,
    };
    ctx.reply_integer(len)
}

// ─────────────────────────────────────────────────────────────────────────────
// XRANGE / XREVRANGE
// ─────────────────────────────────────────────────────────────────────────────

fn parse_optional_count(ctx: &CommandContext, base_argc: usize) -> Result<Option<i64>, RedisError> {
    let argc = ctx.arg_count();
    if argc == base_argc {
        return Ok(None);
    }
    if argc != base_argc + 2 {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let kw = ctx.arg(base_argc)?.as_bytes();
    if !kw.eq_ignore_ascii_case(b"COUNT") {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let n = parse_strict_i64(ctx.arg(base_argc + 1)?.as_bytes())?;
    if n < 0 {
        return Err(RedisError::syntax(
            b"COUNT must be a positive integer",
        ));
    }
    Ok(Some(n))
}

fn xrange_generic(ctx: &mut CommandContext, rev: bool) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(if rev {
            b"xrevrange" as &[u8]
        } else {
            b"xrange" as &[u8]
        }));
    }
    let key = ctx.arg_owned(1usize)?;
    let (lo_arg, hi_arg) = if rev {
        (ctx.arg(3)?.as_bytes().to_vec(), ctx.arg(2)?.as_bytes().to_vec())
    } else {
        (ctx.arg(2)?.as_bytes().to_vec(), ctx.arg(3)?.as_bytes().to_vec())
    };
    let start = parse_range_bound(&lo_arg, BoundSide::Start)?;
    let end = parse_range_bound(&hi_arg, BoundSide::End)?;
    let count = parse_optional_count(ctx, 4)?;

    let collected: Vec<StreamEntry> = match as_stream_ref(ctx.db().lookup_key_read(&key))? {
        None => Vec::new(),
        Some(s) => match resolve_range(s, start, end) {
            None => Vec::new(),
            Some((lo, hi)) => {
                let slice = &s.entries[lo..hi];
                let max = match count {
                    None => slice.len(),
                    Some(n) => (n as usize).min(slice.len()),
                };
                if rev {
                    slice.iter().rev().take(max).cloned().collect()
                } else {
                    slice.iter().take(max).cloned().collect()
                }
            }
        },
    };
    ctx.reply_array_header(collected.len())?;
    for entry in &collected {
        reply_entry(ctx, entry)?;
    }
    Ok(())
}

/// XRANGE key start end [COUNT n]
pub fn xrange_command(ctx: &mut CommandContext) -> RedisResult<()> {
    xrange_generic(ctx, false)
}

/// XREVRANGE key end start [COUNT n]
pub fn xrevrange_command(ctx: &mut CommandContext) -> RedisResult<()> {
    xrange_generic(ctx, true)
}

// ─────────────────────────────────────────────────────────────────────────────
// XDEL
// ─────────────────────────────────────────────────────────────────────────────

/// XDEL key id [id ...]
pub fn xdel_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"xdel"));
    }
    let key = ctx.arg_owned(1usize)?;
    let mut ids: Vec<StreamId> = Vec::with_capacity(ctx.arg_count() - 2);
    for i in 2..ctx.arg_count() {
        ids.push(parse_explicit_id(ctx.arg(i)?.as_bytes())?);
    }
    let deleted: i64 = match as_stream_mut(ctx.db_mut().lookup_key_write(&key))? {
        None => 0,
        Some(stream) => {
            let mut count = 0i64;
            for id in &ids {
                if stream.delete(id) {
                    count += 1;
                }
            }
            count
        }
    };
    ctx.reply_integer(deleted)
}

// ─────────────────────────────────────────────────────────────────────────────
// XTRIM
// ─────────────────────────────────────────────────────────────────────────────

fn parse_trim_args(ctx: &CommandContext, start: usize) -> Result<TrimStrategy, RedisError> {
    let argc = ctx.arg_count();
    if start >= argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let kw = ctx.arg(start)?.as_bytes();
    let is_maxlen = kw.eq_ignore_ascii_case(b"MAXLEN");
    let is_minid = kw.eq_ignore_ascii_case(b"MINID");
    if !is_maxlen && !is_minid {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let mut idx = start + 1;
    if idx >= argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let mut threshold_bytes = ctx.arg(idx)?.as_bytes();
    if threshold_bytes == b"=" || threshold_bytes == b"~" {
        idx += 1;
        if idx >= argc {
            return Err(RedisError::syntax(b"syntax error"));
        }
        threshold_bytes = ctx.arg(idx)?.as_bytes();
    }
    let strategy = if is_maxlen {
        let n = parse_strict_i64(threshold_bytes)?;
        if n < 0 {
            return Err(RedisError::syntax(b"MAXLEN argument must be >= 0"));
        }
        TrimStrategy::MaxLen(n)
    } else {
        TrimStrategy::MinId(parse_explicit_id(threshold_bytes)?)
    };
    idx += 1;
    if idx < argc && ctx.arg(idx)?.as_bytes().eq_ignore_ascii_case(b"LIMIT") {
        idx += 2;
        if idx > argc {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
    if idx != argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    Ok(strategy)
}

/// XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
pub fn xtrim_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(b"xtrim"));
    }
    let key = ctx.arg_owned(1usize)?;
    let strategy = parse_trim_args(ctx, 2)?;
    let evicted: i64 = match as_stream_mut(ctx.db_mut().lookup_key_write(&key))? {
        None => 0,
        Some(stream) => apply_trim(stream, strategy) as i64,
    };
    ctx.reply_integer(evicted)
}

// ─────────────────────────────────────────────────────────────────────────────
// XREAD (non-blocking)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum ReadStartId {
    /// `$` — only entries strictly after the current `last_id`.
    Now,
    /// Explicit id; XREAD returns entries with id strictly greater than this.
    After(StreamId),
}

/// XREAD [COUNT n] STREAMS key [key ...] id [id ...]
pub fn xread_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(b"xread"));
    }
    let mut count: Option<i64> = None;
    let mut i = 1usize;
    let argc = ctx.arg_count();
    let mut streams_idx: Option<usize> = None;
    while i < argc {
        let arg = ctx.arg(i)?.as_bytes();
        if arg.eq_ignore_ascii_case(b"COUNT") {
            if i + 1 >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let n = parse_strict_i64(ctx.arg(i + 1)?.as_bytes())?;
            if n < 0 {
                return Err(RedisError::syntax(b"COUNT must be a positive integer"));
            }
            count = Some(n);
            i += 2;
            continue;
        }
        if arg.eq_ignore_ascii_case(b"BLOCK") {
            if i + 1 >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let _ = parse_strict_i64(ctx.arg(i + 1)?.as_bytes())?;
            i += 2;
            continue;
        }
        if arg.eq_ignore_ascii_case(b"STREAMS") {
            streams_idx = Some(i + 1);
            break;
        }
        return Err(RedisError::syntax(b"syntax error"));
    }
    let streams_start = match streams_idx {
        None => return Err(RedisError::syntax(b"syntax error")),
        Some(s) => s,
    };
    let remaining = argc - streams_start;
    if remaining == 0 || remaining % 2 != 0 {
        return Err(RedisError::runtime(
            b"ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
        ));
    }
    let n_keys = remaining / 2;
    let mut keys: Vec<RedisString> = Vec::with_capacity(n_keys);
    let mut ids: Vec<ReadStartId> = Vec::with_capacity(n_keys);
    for k in 0..n_keys {
        keys.push(ctx.arg_owned(streams_start + k)?);
    }
    for k in 0..n_keys {
        let raw = ctx.arg(streams_start + n_keys + k)?.as_bytes();
        let id = if raw == b"$" {
            ReadStartId::Now
        } else {
            ReadStartId::After(parse_explicit_id(raw)?)
        };
        ids.push(id);
    }

    let mut results: Vec<(RedisString, Vec<StreamEntry>)> = Vec::with_capacity(n_keys);
    for (key, start_id) in keys.iter().zip(ids.iter()) {
        let entries: Vec<StreamEntry> = match as_stream_ref(ctx.db().lookup_key_read(key))? {
            None => Vec::new(),
            Some(stream) => {
                let after = match start_id {
                    ReadStartId::Now => stream.last_id,
                    ReadStartId::After(id) => *id,
                };
                let start_idx = stream.upper_bound(&after);
                let slice = &stream.entries[start_idx..];
                let max = match count {
                    None => slice.len(),
                    Some(n) => (n as usize).min(slice.len()),
                };
                slice.iter().take(max).cloned().collect()
            }
        };
        if !entries.is_empty() {
            results.push((key.clone(), entries));
        }
    }

    if results.is_empty() {
        return ctx.reply_null_array();
    }
    ctx.reply_array_header(results.len())?;
    for (key, entries) in &results {
        ctx.reply_array_header(2usize)?;
        ctx.reply_bulk_string(key.clone())?;
        ctx.reply_array_header(entries.len())?;
        for entry in entries {
            reply_entry(ctx, entry)?;
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// XINFO STREAM / GROUPS (basic)
// ─────────────────────────────────────────────────────────────────────────────

/// XINFO STREAM key | XINFO GROUPS key | XINFO HELP
pub fn xinfo_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"xinfo"));
    }
    let sub = ctx.arg(1)?.as_bytes().to_ascii_uppercase();
    match sub.as_slice() {
        b"HELP" => {
            let lines: &[&[u8]] = &[
                b"XINFO STREAM <key>",
                b"    Show information about the stream.",
                b"XINFO GROUPS <key>",
                b"    Show the stream consumer groups.",
                b"XINFO HELP",
                b"    Print this help.",
            ];
            ctx.reply_array_header(lines.len())?;
            for line in lines {
                ctx.reply_bulk(line)?;
            }
            Ok(())
        }
        b"STREAM" => {
            if ctx.arg_count() < 3 {
                return Err(RedisError::wrong_number_of_args(b"xinfo"));
            }
            let key = ctx.arg_owned(2usize)?;
            let stream = match as_stream_ref(ctx.db().lookup_key_read(&key))? {
                None => return Err(RedisError::runtime(b"ERR no such key")),
                Some(s) => s,
            };
            let length = stream.len() as i64;
            let last_id = stream.last_id;
            let max_del = stream.max_deleted_id;
            let entries_added = stream.entries_added as i64;
            let first = stream.entries.first().cloned();
            let last = stream.entries.last().cloned();
            ctx.reply_array_header(14usize)?;
            ctx.reply_bulk(b"length")?;
            ctx.reply_integer(length)?;
            ctx.reply_bulk(b"radix-tree-keys")?;
            ctx.reply_integer(0)?;
            ctx.reply_bulk(b"radix-tree-nodes")?;
            ctx.reply_integer(0)?;
            ctx.reply_bulk(b"last-generated-id")?;
            ctx.reply_bulk_string(RedisString::from_vec(last_id.to_display_bytes()))?;
            ctx.reply_bulk(b"max-deleted-entry-id")?;
            ctx.reply_bulk_string(RedisString::from_vec(max_del.to_display_bytes()))?;
            ctx.reply_bulk(b"entries-added")?;
            ctx.reply_integer(entries_added)?;
            ctx.reply_bulk(b"first-entry")?;
            match first {
                None => ctx.reply_null_array()?,
                Some(e) => reply_entry(ctx, &e)?,
            }
            ctx.reply_bulk(b"last-entry")?;
            match last {
                None => ctx.reply_null_array()?,
                Some(e) => reply_entry(ctx, &e)?,
            }
            Ok(())
        }
        b"GROUPS" => {
            if ctx.arg_count() < 3 {
                return Err(RedisError::wrong_number_of_args(b"xinfo"));
            }
            let key = ctx.arg_owned(2usize)?;
            let _ = match as_stream_ref(ctx.db().lookup_key_read(&key))? {
                None => return Err(RedisError::runtime(b"ERR no such key")),
                Some(s) => s,
            };
            ctx.reply_empty_array()
        }
        _ => Err(RedisError::syntax(
            b"syntax error, try XINFO HELP",
        )),
    }
}
