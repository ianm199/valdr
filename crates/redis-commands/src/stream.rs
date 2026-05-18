//! Stream commands — Round 9 byte-exact port, Round 13b blocking extension.
//!
//! Implements: XADD, XLEN, XRANGE, XREVRANGE, XREAD (blocking + non-blocking),
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
//! TODO(architect): MAXLEN/MINID `~` approximate trimming behaves
//! identically to `=` exact trimming for the inline encoding (no
//! listpack-boundary quirks to honour).

use redis_core::blocked_keys::{
    blocked_keys_index, current_time_ms, BlockedAction, BlockedWaiter,
};
use redis_core::command_context::CommandContext;
use redis_core::db::RedisDb;
use redis_core::notify::NOTIFY_STREAM;
use redis_core::object::RedisObject;
use redis_core::util::mstime;
use redis_ds::stream::{
    parse_stream_id, Consumer, ConsumerGroup, InlineStream, PelEntry, StreamEntry, StreamId,
};
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

    let fields_for_wake = fields.clone();
    let key_for_wake = key.clone();

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

    let new_entry = StreamEntry { id: new_id, fields: fields_for_wake };
    wake_blocked_for_stream(ctx.db(), &key_for_wake, &new_entry);

    ctx.notify_keyspace_event(NOTIFY_STREAM, b"xadd", &key_for_wake);
    ctx.reply_bulk_string(RedisString::from_vec(new_id.to_display_bytes()))
}

// ─────────────────────────────────────────────────────────────────────────────
// XREAD BLOCK wake hook
// ─────────────────────────────────────────────────────────────────────────────

/// Encode an XREAD reply for a single stream key carrying one entry.
///
/// Wire shape: `*1\r\n*2\r\n$<klen>\r\n<key>\r\n*1\r\n*2\r\n$<ilen>\r\n<id>\r\n*<2n>\r\n<f1><v1>...`
fn encode_xread_single_entry(key: &RedisString, entry: &StreamEntry) -> Vec<u8> {
    let id_bytes = entry.id.to_display_bytes();
    let fields_count = entry.fields.len() * 2;

    let mut buf = Vec::with_capacity(256);

    let write_bulk = |buf: &mut Vec<u8>, bytes: &[u8]| {
        buf.push(b'$');
        buf.extend_from_slice(bytes.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(bytes);
        buf.extend_from_slice(b"\r\n");
    };

    let write_array = |buf: &mut Vec<u8>, n: usize| {
        buf.push(b'*');
        buf.extend_from_slice(n.to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
    };

    write_array(&mut buf, 1);
    write_array(&mut buf, 2);
    write_bulk(&mut buf, key.as_bytes());
    write_array(&mut buf, 1);
    write_array(&mut buf, 2);
    write_bulk(&mut buf, &id_bytes);
    write_array(&mut buf, fields_count);
    for (f, v) in &entry.fields {
        write_bulk(&mut buf, f.as_bytes());
        write_bulk(&mut buf, v.as_bytes());
    }
    buf
}

/// Wake all XREAD BLOCK waiters on `key` whose `id_after` is strictly less
/// than `new_entry.id`.
///
/// Unlike the list pop wake (FIFO, one pop per waiter), streams use broadcast
/// semantics: every waiting reader receives a copy of the new entry. This
/// mirrors real Redis's `signalKeyAsReady` / `serveClientsBlockedOnListOrZset`
/// pattern for streams.
pub fn wake_blocked_for_stream(db: &RedisDb, key: &RedisString, new_entry: &StreamEntry) {
    let waiters = {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.take_stream_waiters_for(key, new_entry.id)
    };
    if waiters.is_empty() {
        return;
    }
    let reply = encode_xread_single_entry(key, new_entry);
    for waiter in waiters {
        let _ = waiter.sender.send(reply.clone());
    }
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
    if deleted > 0 {
        ctx.notify_keyspace_event(NOTIFY_STREAM, b"xdel", &key);
    }
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
    if evicted > 0 {
        ctx.notify_keyspace_event(NOTIFY_STREAM, b"xtrim", &key);
    }
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

/// Park an XREAD BLOCK client in the global blocked-keys index.
///
/// Registers one waiter per (key, id_after) pair so that the first XADD on
/// any of those keys will wake this client. If the client lacks a sender
/// (unit tests / pseudo-clients) the function returns `$-1\r\n` immediately.
fn park_xread_block(
    ctx: &mut CommandContext,
    keys: Vec<RedisString>,
    ids_after: Vec<StreamId>,
    block_ms: i64,
) -> RedisResult<()> {
    if ctx.client_ref().flag_deny_blocking() {
        return ctx.reply_null_bulk();
    }
    let registry = match ctx.pubsub.as_ref() {
        Some(r) => r.clone(),
        None => return ctx.reply_null_bulk(),
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
        None => return ctx.reply_null_bulk(),
    };
    let deadline_ms = if block_ms == 0 {
        i64::MAX
    } else {
        current_time_ms().saturating_add(block_ms)
    };
    {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for (key, id_after) in keys.iter().zip(ids_after.iter()) {
            let waiter = BlockedWaiter {
                client_id: ctx.client_ref().id,
                sender: sender.clone(),
                keys: vec![key.clone()],
                action: BlockedAction::Stream { id_after: *id_after },
                deadline_ms,
            };
            idx.add(waiter);
        }
    }
    ctx.client_mut().blocked_on_keys = true;
    Ok(())
}

/// XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]
pub fn xread_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(b"xread"));
    }
    let mut count: Option<i64> = None;
    let mut block_ms: Option<i64> = None;
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
            let ms = parse_strict_i64(ctx.arg(i + 1)?.as_bytes())?;
            if ms < 0 {
                return Err(RedisError::runtime(b"ERR timeout is negative"));
            }
            block_ms = Some(ms);
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

    if !results.is_empty() {
        ctx.reply_array_header(results.len())?;
        for (key, entries) in &results {
            ctx.reply_array_header(2usize)?;
            ctx.reply_bulk_string(key.clone())?;
            ctx.reply_array_header(entries.len())?;
            for entry in entries {
                reply_entry(ctx, entry)?;
            }
        }
        return Ok(());
    }

    if let Some(ms) = block_ms {
        let resolved_ids: Vec<StreamId> = keys
            .iter()
            .zip(ids.iter())
            .map(|(key, start_id)| match start_id {
                ReadStartId::After(id) => *id,
                ReadStartId::Now => match as_stream_ref(ctx.db().lookup_key_read(key)) {
                    Ok(Some(stream)) => stream.last_id,
                    _ => StreamId::ZERO,
                },
            })
            .collect();
        return park_xread_block(ctx, keys, resolved_ids, ms);
    }

    ctx.reply_null_array()
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
                b"XINFO CONSUMERS <key> <group>",
                b"    Show the consumers of <group>.",
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
        b"GROUPS" => xinfo_groups(ctx),
        b"CONSUMERS" => xinfo_consumers(ctx),
        _ => Err(RedisError::syntax(
            b"syntax error, try XINFO HELP",
        )),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CONSUMER GROUPS (Round 13c)
// ─────────────────────────────────────────────────────────────────────────────
//
// Implements: XGROUP (CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER),
// XREADGROUP, XACK, XPENDING (summary + extended), XCLAIM, XAUTOCLAIM,
// XSETID, XINFO CONSUMERS, XINFO GROUPS.
//
// Storage extension lives in `redis_ds::stream::{ConsumerGroup, Consumer,
// PelEntry}`. PEL entries are mirrored between `group.pel` and the
// matching `consumer.pel`; helpers below keep them consistent.
//
// TODO(architect): XREADGROUP BLOCK is unimplemented this round. Round
// 13b wired XREAD BLOCK against the blocked-keys index with
// `BlockedAction::Stream`; group-aware blocking needs an additional
// per-group last-delivered cursor on the waker side. Behaviour right
// now: BLOCK with no new entries returns `$-1` (nil bulk).
//
// TODO(architect): XAUTOCLAIM cursor pagination is implemented as a
// simple ID cursor (next id to resume from). Valkey uses a more nuanced
// rax-cursor; this is close-enough for the inline encoding.
//
// TODO(architect): XINFO STREAM FULL form is not implemented.

fn no_such_key_or_group_err(key: &[u8], group: &[u8], cmd: &[u8]) -> RedisError {
    let mut buf = Vec::with_capacity(64 + key.len() + group.len() + cmd.len());
    buf.extend_from_slice(b"NOGROUP No such key '");
    buf.extend_from_slice(key);
    buf.extend_from_slice(b"' or consumer group '");
    buf.extend_from_slice(group);
    buf.extend_from_slice(b"' in ");
    buf.extend_from_slice(cmd);
    buf.extend_from_slice(b" with GROUP option");
    RedisError::runtime(buf)
}

fn no_such_key_or_group_short(key: &[u8], group: &[u8]) -> RedisError {
    let mut buf = Vec::with_capacity(48 + key.len() + group.len());
    buf.extend_from_slice(b"NOGROUP No such key '");
    buf.extend_from_slice(key);
    buf.extend_from_slice(b"' or consumer group '");
    buf.extend_from_slice(group);
    buf.push(b'\'');
    RedisError::runtime(buf)
}

fn now_ms_clamped() -> i64 {
    let t = mstime();
    if t < 0 {
        0
    } else {
        t
    }
}

/// Lookup or create a consumer inside the given group. Returns a bool
/// indicating whether the consumer was created.
fn touch_or_create_consumer(
    group: &mut ConsumerGroup,
    name: &RedisString,
    now_ms: i64,
) -> bool {
    let exists = group.consumers.contains_key(name);
    if !exists {
        group
            .consumers
            .insert(name.clone(), Consumer::new(name.clone(), now_ms));
    } else if let Some(c) = group.consumers.get_mut(name) {
        c.seen_time_ms = now_ms;
    }
    !exists
}

/// Insert/update the PEL entry in both `group.pel` and `consumer.pel`.
fn pel_add(group: &mut ConsumerGroup, consumer_name: &RedisString, entry: PelEntry) {
    group.pel_upsert(entry.clone());
    if let Some(consumer) = group.consumers.get_mut(consumer_name) {
        let idx = consumer.pel_lower_bound(&entry.entry_id);
        if idx < consumer.pel.len() && consumer.pel[idx].entry_id == entry.entry_id {
            consumer.pel[idx] = entry;
        } else {
            consumer.pel.insert(idx, entry);
        }
    }
}

/// Remove a PEL entry from both group and consumer. Returns `true` if
/// the entry existed in the group PEL.
fn pel_remove(group: &mut ConsumerGroup, target: &StreamId) -> bool {
    let removed = group.pel_remove(target);
    if removed.is_none() {
        return false;
    }
    for consumer in group.consumers.values_mut() {
        if let Some(idx) = consumer.pel_find(target) {
            consumer.pel.remove(idx);
        }
    }
    true
}

/// Re-home a PEL entry to a different consumer. The group PEL keeps
/// the (possibly updated) NACK; only the per-consumer location moves.
fn pel_reassign(
    group: &mut ConsumerGroup,
    target: &StreamId,
    new_owner: &RedisString,
    delivery_time_ms: i64,
    delivery_count: u64,
) {
    let new_entry = PelEntry {
        entry_id: *target,
        delivery_time_ms,
        delivery_count,
    };
    group.pel_upsert(new_entry.clone());
    for (name, consumer) in group.consumers.iter_mut() {
        if name == new_owner {
            continue;
        }
        if let Some(idx) = consumer.pel_find(target) {
            consumer.pel.remove(idx);
        }
    }
    if let Some(consumer) = group.consumers.get_mut(new_owner) {
        let idx = consumer.pel_lower_bound(target);
        if idx < consumer.pel.len() && consumer.pel[idx].entry_id == *target {
            consumer.pel[idx] = new_entry;
        } else {
            consumer.pel.insert(idx, new_entry);
        }
    }
}

/// Borrow the mutable `InlineStream` for `key` without auto-creating
/// the key. Used by all consumer-group write commands.
fn stream_for_write<'db>(
    db: &'db mut RedisDb,
    key: &RedisString,
) -> Result<Option<&'db mut InlineStream>, RedisError> {
    as_stream_mut(db.lookup_key_write(key))
}

/// Parse a stream id that may also be `$` (meaning "current last_id").
fn parse_id_or_dollar(arg: &[u8], stream: &InlineStream) -> Result<StreamId, RedisError> {
    if arg == b"$" {
        Ok(stream.last_id)
    } else {
        parse_explicit_id(arg)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// XGROUP
// ─────────────────────────────────────────────────────────────────────────────

/// XGROUP CREATE | SETID | DESTROY | CREATECONSUMER | DELCONSUMER | HELP
pub fn xgroup_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"xgroup"));
    }
    let sub = ctx.arg(1)?.as_bytes().to_ascii_uppercase();
    match sub.as_slice() {
        b"CREATE" => xgroup_create(ctx),
        b"SETID" => xgroup_setid(ctx),
        b"DESTROY" => xgroup_destroy(ctx),
        b"CREATECONSUMER" => xgroup_createconsumer(ctx),
        b"DELCONSUMER" => xgroup_delconsumer(ctx),
        b"HELP" => {
            let lines: &[&[u8]] = &[
                b"XGROUP CREATE <key> <groupname> <id|$> [MKSTREAM] [ENTRIESREAD entries-read]",
                b"    Create a new consumer group.",
                b"XGROUP SETID <key> <groupname> <id|$> [ENTRIESREAD entries-read]",
                b"    Set the current group ID.",
                b"XGROUP DESTROY <key> <groupname>",
                b"    Remove the specified group.",
                b"XGROUP CREATECONSUMER <key> <groupname> <consumer>",
                b"    Create a new consumer in the group.",
                b"XGROUP DELCONSUMER <key> <groupname> <consumer>",
                b"    Remove the specified consumer.",
                b"XGROUP HELP",
                b"    Print this help.",
            ];
            ctx.reply_array_header(lines.len())?;
            for line in lines {
                ctx.reply_bulk(line)?;
            }
            Ok(())
        }
        _ => Err(RedisError::syntax(b"syntax error, try XGROUP HELP")),
    }
}

fn parse_entries_read_suffix(
    ctx: &CommandContext,
    start: usize,
) -> Result<(bool, u64), RedisError> {
    let argc = ctx.arg_count();
    if start >= argc {
        return Ok((false, 0));
    }
    if !ctx.arg(start)?.as_bytes().eq_ignore_ascii_case(b"ENTRIESREAD") {
        return Err(RedisError::syntax(b"syntax error"));
    }
    if start + 1 >= argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let n = parse_strict_i64(ctx.arg(start + 1)?.as_bytes())?;
    if n < 0 {
        return Err(RedisError::syntax(b"ENTRIESREAD must be a non-negative integer"));
    }
    if start + 2 != argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    Ok((true, n as u64))
}

fn xgroup_create(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 5 {
        return Err(RedisError::wrong_number_of_args(b"xgroup"));
    }
    let key = ctx.arg_owned(2usize)?;
    let group_name = ctx.arg_owned(3usize)?;
    let id_arg = ctx.arg(4)?.as_bytes().to_vec();
    let mut mkstream = false;
    let mut tail_start = 5usize;
    while tail_start < argc {
        let arg = ctx.arg(tail_start)?.as_bytes();
        if arg.eq_ignore_ascii_case(b"MKSTREAM") {
            mkstream = true;
            tail_start += 1;
            continue;
        }
        break;
    }
    let (had_entries_read, entries_read) = parse_entries_read_suffix(ctx, tail_start)?;

    if stream_for_write(ctx.db_mut(), &key)?.is_none() {
        if !mkstream {
            return Err(RedisError::runtime(
                b"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
            ));
        }
        ctx.db_mut().set_key(key.clone(), RedisObject::new_stream(), 0);
    }
    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return Err(RedisError::runtime(b"ERR unreachable: stream vanished")),
    };
    let new_id = parse_id_or_dollar(&id_arg, stream)?;
    if stream.groups.contains_key(&group_name) {
        return Err(RedisError::runtime(b"BUSYGROUP Consumer Group name already exists"));
    }
    let mut group = ConsumerGroup::new(group_name.clone(), new_id);
    if had_entries_read {
        group.entries_read = entries_read;
    }
    stream.groups.insert(group_name, group);
    ctx.reply_simple_string(b"OK")
}

fn xgroup_setid(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 5 {
        return Err(RedisError::wrong_number_of_args(b"xgroup"));
    }
    let key = ctx.arg_owned(2usize)?;
    let group_name = ctx.arg_owned(3usize)?;
    let id_arg = ctx.arg(4)?.as_bytes().to_vec();
    let (had_entries_read, entries_read) = parse_entries_read_suffix(ctx, 5)?;

    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
    };
    let new_id = parse_id_or_dollar(&id_arg, stream)?;
    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
    };
    group.last_delivered_id = new_id;
    if had_entries_read {
        group.entries_read = entries_read;
    }
    ctx.reply_simple_string(b"OK")
}

fn xgroup_destroy(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"xgroup"));
    }
    let key = ctx.arg_owned(2usize)?;
    let group_name = ctx.arg_owned(3usize)?;
    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return ctx.reply_integer(0),
    };
    let removed = stream.groups.remove(&group_name).is_some();
    ctx.reply_integer(if removed { 1 } else { 0 })
}

fn xgroup_createconsumer(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 5 {
        return Err(RedisError::wrong_number_of_args(b"xgroup"));
    }
    let key = ctx.arg_owned(2usize)?;
    let group_name = ctx.arg_owned(3usize)?;
    let consumer_name = ctx.arg_owned(4usize)?;
    let now = now_ms_clamped();
    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
    };
    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
    };
    let created = touch_or_create_consumer(group, &consumer_name, now);
    ctx.reply_integer(if created { 1 } else { 0 })
}

fn xgroup_delconsumer(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 5 {
        return Err(RedisError::wrong_number_of_args(b"xgroup"));
    }
    let key = ctx.arg_owned(2usize)?;
    let group_name = ctx.arg_owned(3usize)?;
    let consumer_name = ctx.arg_owned(4usize)?;
    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
    };
    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
    };
    let consumer = match group.consumers.remove(&consumer_name) {
        Some(c) => c,
        None => return ctx.reply_integer(0),
    };
    let pending = consumer.pel.len() as i64;
    for entry in &consumer.pel {
        group.pel_remove(&entry.entry_id);
    }
    ctx.reply_integer(pending)
}

// ─────────────────────────────────────────────────────────────────────────────
// XREADGROUP
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum GroupReadStartId {
    /// `>` — deliver new entries with id > group.last_delivered_id.
    New,
    /// Explicit id — re-read entries from the consumer PEL with id >= this.
    From(StreamId),
}

/// XREADGROUP GROUP <group> <consumer> [COUNT n] [BLOCK ms] [NOACK]
///            STREAMS key [key ...] id [id ...]
pub fn xreadgroup_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 7 {
        return Err(RedisError::wrong_number_of_args(b"xreadgroup"));
    }
    if !ctx.arg(1)?.as_bytes().eq_ignore_ascii_case(b"GROUP") {
        return Err(RedisError::syntax(
            b"Missing GROUP option for XREADGROUP",
        ));
    }
    let group_name = ctx.arg_owned(2usize)?;
    let consumer_name = ctx.arg_owned(3usize)?;
    let mut i = 4usize;
    let mut count: Option<i64> = None;
    let mut noack = false;
    let mut _block_ms: Option<i64> = None;
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
            let ms = parse_strict_i64(ctx.arg(i + 1)?.as_bytes())?;
            if ms < 0 {
                return Err(RedisError::runtime(b"ERR timeout is negative"));
            }
            _block_ms = Some(ms);
            i += 2;
            continue;
        }
        if arg.eq_ignore_ascii_case(b"NOACK") {
            noack = true;
            i += 1;
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
            b"ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.",
        ));
    }
    let n_keys = remaining / 2;
    let mut keys: Vec<RedisString> = Vec::with_capacity(n_keys);
    let mut ids: Vec<GroupReadStartId> = Vec::with_capacity(n_keys);
    for k in 0..n_keys {
        keys.push(ctx.arg_owned(streams_start + k)?);
    }
    for k in 0..n_keys {
        let raw = ctx.arg(streams_start + n_keys + k)?.as_bytes();
        let id = if raw == b">" {
            GroupReadStartId::New
        } else {
            GroupReadStartId::From(parse_explicit_id(raw)?)
        };
        ids.push(id);
    }

    let now = now_ms_clamped();
    let mut results: Vec<(RedisString, Vec<(StreamEntry, bool)>)> = Vec::with_capacity(n_keys);
    for (key, start_id) in keys.iter().zip(ids.iter()) {
        let key_bytes = key.as_bytes().to_vec();
        let stream = match stream_for_write(ctx.db_mut(), key)? {
            Some(s) => s,
            None => {
                return Err(no_such_key_or_group_err(
                    &key_bytes,
                    group_name.as_bytes(),
                    b"XREADGROUP",
                ))
            }
        };
        if !stream.groups.contains_key(&group_name) {
            return Err(no_such_key_or_group_err(
                &key_bytes,
                group_name.as_bytes(),
                b"XREADGROUP",
            ));
        }
        match start_id {
            GroupReadStartId::New => {
                let after = match stream.groups.get(&group_name) {
                    Some(g) => g.last_delivered_id,
                    None => continue,
                };
                let start_idx = stream.upper_bound(&after);
                let slice_len = stream.entries.len() - start_idx;
                let max = match count {
                    None => slice_len,
                    Some(n) => (n as usize).min(slice_len),
                };
                if max == 0 {
                    continue;
                }
                let to_deliver: Vec<StreamEntry> =
                    stream.entries[start_idx..start_idx + max].to_vec();
                let new_last = to_deliver
                    .last()
                    .map(|e| e.id)
                    .expect("max > 0 implies entries");
                let group = stream
                    .groups
                    .get_mut(&group_name)
                    .expect("group existence checked above");
                touch_or_create_consumer(group, &consumer_name, now);
                if let Some(consumer) = group.consumers.get_mut(&consumer_name) {
                    consumer.active_time_ms = now;
                }
                group.last_delivered_id = new_last;
                group.entries_read = group.entries_read.saturating_add(max as u64);
                if !noack {
                    for entry in &to_deliver {
                        pel_add(
                            group,
                            &consumer_name,
                            PelEntry {
                                entry_id: entry.id,
                                delivery_time_ms: now,
                                delivery_count: 1,
                            },
                        );
                    }
                }
                let delivered: Vec<(StreamEntry, bool)> =
                    to_deliver.into_iter().map(|e| (e, true)).collect();
                results.push((key.clone(), delivered));
            }
            GroupReadStartId::From(from_id) => {
                let group = match stream.groups.get_mut(&group_name) {
                    Some(g) => g,
                    None => continue,
                };
                touch_or_create_consumer(group, &consumer_name, now);
                let pending_ids: Vec<StreamId> = match group.consumers.get(&consumer_name) {
                    Some(c) => {
                        let start = c.pel_lower_bound(from_id);
                        let take = match count {
                            None => c.pel.len() - start,
                            Some(n) => (n as usize).min(c.pel.len() - start),
                        };
                        c.pel[start..start + take]
                            .iter()
                            .map(|p| p.entry_id)
                            .collect()
                    }
                    None => Vec::new(),
                };
                for id in &pending_ids {
                    if let Some(consumer) = group.consumers.get_mut(&consumer_name) {
                        if let Some(idx) = consumer.pel_find(id) {
                            consumer.pel[idx].delivery_time_ms = now;
                            consumer.pel[idx].delivery_count =
                                consumer.pel[idx].delivery_count.saturating_add(1);
                        }
                    }
                    if let Some(idx) = group.pel_find(id) {
                        group.pel[idx].delivery_time_ms = now;
                        group.pel[idx].delivery_count =
                            group.pel[idx].delivery_count.saturating_add(1);
                    }
                }
                let mut collected: Vec<(StreamEntry, bool)> = Vec::with_capacity(pending_ids.len());
                for id in &pending_ids {
                    let entry_opt = match stream.entries.binary_search_by(|e| e.id.cmp(id)) {
                        Ok(idx) => Some(stream.entries[idx].clone()),
                        Err(_) => None,
                    };
                    match entry_opt {
                        Some(e) => collected.push((e, true)),
                        None => collected.push((
                            StreamEntry {
                                id: *id,
                                fields: Vec::new(),
                            },
                            false,
                        )),
                    }
                }
                results.push((key.clone(), collected));
            }
        }
    }

    let non_empty: Vec<&(RedisString, Vec<(StreamEntry, bool)>)> =
        results.iter().filter(|(_, v)| !v.is_empty()).collect();
    if non_empty.is_empty() {
        return ctx.reply_null_array();
    }
    ctx.reply_array_header(non_empty.len())?;
    for (key, entries) in &non_empty {
        ctx.reply_array_header(2usize)?;
        ctx.reply_bulk_string(key.clone())?;
        ctx.reply_array_header(entries.len())?;
        for (entry, present) in entries.iter() {
            ctx.reply_array_header(2usize)?;
            ctx.reply_bulk_string(RedisString::from_vec(entry.id.to_display_bytes()))?;
            if *present {
                ctx.reply_array_header(entry.fields.len() * 2)?;
                for (f, v) in &entry.fields {
                    ctx.reply_bulk_string(f.clone())?;
                    ctx.reply_bulk_string(v.clone())?;
                }
            } else {
                ctx.reply_null_array()?;
            }
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// XACK
// ─────────────────────────────────────────────────────────────────────────────

/// XACK key group id [id ...]
pub fn xack_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(b"xack"));
    }
    let key = ctx.arg_owned(1usize)?;
    let group_name = ctx.arg_owned(2usize)?;
    let mut ids: Vec<StreamId> = Vec::with_capacity(ctx.arg_count() - 3);
    for i in 3..ctx.arg_count() {
        ids.push(parse_explicit_id(ctx.arg(i)?.as_bytes())?);
    }
    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return ctx.reply_integer(0),
    };
    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => return ctx.reply_integer(0),
    };
    let mut removed = 0i64;
    for id in &ids {
        if pel_remove(group, id) {
            removed += 1;
        }
    }
    ctx.reply_integer(removed)
}

// ─────────────────────────────────────────────────────────────────────────────
// XPENDING
// ─────────────────────────────────────────────────────────────────────────────

/// XPENDING key group [[IDLE ms] start end count [consumer]]
pub fn xpending_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"xpending"));
    }
    if argc != 3 && (argc < 6 || argc > 9) {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let key = ctx.arg_owned(1usize)?;
    let group_name = ctx.arg_owned(2usize)?;

    let group_owned: ConsumerGroup = match as_stream_ref(ctx.db().lookup_key_read(&key))? {
        None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
        Some(s) => match s.groups.get(&group_name) {
            None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
            Some(g) => g.clone(),
        },
    };
    let group = &group_owned;

    if argc == 3 {
        return xpending_summary(ctx, group);
    }

    let mut idx = 3usize;
    let mut min_idle = 0i64;
    if ctx.arg(idx)?.as_bytes().eq_ignore_ascii_case(b"IDLE") {
        if idx + 1 >= argc {
            return Err(RedisError::syntax(b"syntax error"));
        }
        min_idle = parse_strict_i64(ctx.arg(idx + 1)?.as_bytes())?;
        if min_idle < 0 {
            min_idle = 0;
        }
        idx += 2;
    }
    if idx + 3 > argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let start = parse_range_bound(ctx.arg(idx)?.as_bytes(), BoundSide::Start)?;
    let end = parse_range_bound(ctx.arg(idx + 1)?.as_bytes(), BoundSide::End)?;
    let count = parse_strict_i64(ctx.arg(idx + 2)?.as_bytes())?;
    let count = if count < 0 { 0 } else { count as usize };
    let consumer_filter: Option<RedisString> = if idx + 3 < argc {
        Some(ctx.arg_owned(idx + 3)?)
    } else {
        None
    };
    if idx + 4 < argc {
        return Err(RedisError::syntax(b"syntax error"));
    }

    let now = now_ms_clamped();
    let pel: &[PelEntry] = match &consumer_filter {
        None => group.pel.as_slice(),
        Some(name) => match group.consumers.get(name) {
            Some(c) => c.pel.as_slice(),
            None => {
                return ctx.reply_empty_array();
            }
        },
    };
    let owner_lookup: std::collections::HashMap<StreamId, RedisString> = group
        .consumers
        .iter()
        .flat_map(|(name, consumer)| {
            consumer
                .pel
                .iter()
                .map(move |entry| (entry.entry_id, name.clone()))
        })
        .collect();

    let start_id = match start {
        Bound::Min => StreamId::ZERO,
        Bound::Max => StreamId::MAX,
        Bound::Inclusive(id) => id,
        Bound::Exclusive(id) => match id.checked_succ() {
            Some(next) => next,
            None => StreamId::MAX,
        },
    };
    let end_id = match end {
        Bound::Min => StreamId::ZERO,
        Bound::Max => StreamId::MAX,
        Bound::Inclusive(id) => id,
        Bound::Exclusive(id) => match id.checked_pred() {
            Some(prev) => prev,
            None => StreamId::ZERO,
        },
    };

    let mut matched: Vec<(StreamId, RedisString, i64, u64)> = Vec::new();
    for entry in pel {
        if entry.entry_id < start_id || entry.entry_id > end_id {
            continue;
        }
        let idle = (now - entry.delivery_time_ms).max(0);
        if idle < min_idle {
            continue;
        }
        let owner = match &consumer_filter {
            Some(name) => name.clone(),
            None => match owner_lookup.get(&entry.entry_id) {
                Some(n) => n.clone(),
                None => RedisString::from_bytes(b""),
            },
        };
        matched.push((entry.entry_id, owner, idle, entry.delivery_count));
        if matched.len() >= count {
            break;
        }
    }

    ctx.reply_array_header(matched.len())?;
    for (id, owner, idle, dc) in &matched {
        ctx.reply_array_header(4usize)?;
        ctx.reply_bulk_string(RedisString::from_vec(id.to_display_bytes()))?;
        ctx.reply_bulk_string(owner.clone())?;
        ctx.reply_integer(*idle)?;
        ctx.reply_integer(*dc as i64)?;
    }
    Ok(())
}

fn xpending_summary(ctx: &mut CommandContext, group: &ConsumerGroup) -> RedisResult<()> {
    if group.pel.is_empty() {
        ctx.reply_array_header(4usize)?;
        ctx.reply_integer(0)?;
        ctx.reply_null_bulk()?;
        ctx.reply_null_bulk()?;
        return ctx.reply_null_array();
    }
    ctx.reply_array_header(4usize)?;
    ctx.reply_integer(group.pel.len() as i64)?;
    let min_id = group.pel.first().expect("non-empty").entry_id;
    let max_id = group.pel.last().expect("non-empty").entry_id;
    ctx.reply_bulk_string(RedisString::from_vec(min_id.to_display_bytes()))?;
    ctx.reply_bulk_string(RedisString::from_vec(max_id.to_display_bytes()))?;
    let mut by_consumer: Vec<(RedisString, usize)> = group
        .consumers
        .iter()
        .filter_map(|(name, c)| {
            if c.pel.is_empty() {
                None
            } else {
                Some((name.clone(), c.pel.len()))
            }
        })
        .collect();
    by_consumer.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    ctx.reply_array_header(by_consumer.len())?;
    for (name, n) in &by_consumer {
        ctx.reply_array_header(2usize)?;
        ctx.reply_bulk_string(name.clone())?;
        let s = format!("{}", n);
        ctx.reply_bulk_string(RedisString::from_vec(s.into_bytes()))?;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// XCLAIM
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct ClaimOptions {
    idle: Option<i64>,
    time: Option<i64>,
    retrycount: Option<u64>,
    force: bool,
    justid: bool,
}

/// XCLAIM key group consumer min-idle-time id [id ...]
///        [IDLE ms] [TIME unix-ms] [RETRYCOUNT n] [FORCE] [JUSTID]
pub fn xclaim_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 6 {
        return Err(RedisError::wrong_number_of_args(b"xclaim"));
    }
    let key = ctx.arg_owned(1usize)?;
    let group_name = ctx.arg_owned(2usize)?;
    let consumer_name = ctx.arg_owned(3usize)?;
    let min_idle = parse_strict_i64(ctx.arg(4)?.as_bytes())?;
    let min_idle = if min_idle < 0 { 0 } else { min_idle };

    let mut ids: Vec<StreamId> = Vec::new();
    let mut idx = 5usize;
    while idx < argc {
        let arg = ctx.arg(idx)?.as_bytes();
        let upper = arg.to_ascii_uppercase();
        match upper.as_slice() {
            b"IDLE" | b"TIME" | b"RETRYCOUNT" | b"FORCE" | b"JUSTID" | b"LASTID" => break,
            _ => {
                ids.push(parse_explicit_id(arg)?);
                idx += 1;
            }
        }
    }
    if ids.is_empty() {
        return Err(RedisError::syntax(b"syntax error"));
    }

    let mut opts = ClaimOptions::default();
    while idx < argc {
        let arg = ctx.arg(idx)?.as_bytes();
        let upper = arg.to_ascii_uppercase();
        match upper.as_slice() {
            b"FORCE" => {
                opts.force = true;
                idx += 1;
            }
            b"JUSTID" => {
                opts.justid = true;
                idx += 1;
            }
            b"IDLE" => {
                if idx + 1 >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                opts.idle = Some(parse_strict_i64(ctx.arg(idx + 1)?.as_bytes())?);
                idx += 2;
            }
            b"TIME" => {
                if idx + 1 >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                opts.time = Some(parse_strict_i64(ctx.arg(idx + 1)?.as_bytes())?);
                idx += 2;
            }
            b"RETRYCOUNT" => {
                if idx + 1 >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                let n = parse_strict_i64(ctx.arg(idx + 1)?.as_bytes())?;
                if n < 0 {
                    return Err(RedisError::syntax(b"RETRYCOUNT must be >= 0"));
                }
                opts.retrycount = Some(n as u64);
                idx += 2;
            }
            b"LASTID" => {
                if idx + 1 >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                let _ = parse_explicit_id(ctx.arg(idx + 1)?.as_bytes())?;
                idx += 2;
            }
            _ => return Err(RedisError::syntax(b"syntax error")),
        }
    }

    let now = now_ms_clamped();
    let new_delivery_time = match (opts.time, opts.idle) {
        (Some(t), _) => t,
        (None, Some(i)) => now - i,
        (None, None) => now,
    };

    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
    };
    if !stream.groups.contains_key(&group_name) {
        return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes()));
    }

    let entries_index: std::collections::HashMap<StreamId, StreamEntry> = stream
        .entries
        .iter()
        .map(|e| (e.id, e.clone()))
        .collect();

    let group = stream
        .groups
        .get_mut(&group_name)
        .expect("group existence checked above");
    touch_or_create_consumer(group, &consumer_name, now);
    if let Some(c) = group.consumers.get_mut(&consumer_name) {
        c.active_time_ms = now;
    }

    let mut claimed: Vec<(StreamId, Option<StreamEntry>, u64)> = Vec::new();
    for id in &ids {
        let in_pel = group.pel_find(id).is_some();
        if !in_pel && !opts.force {
            continue;
        }
        let stream_entry = entries_index.get(id).cloned();
        if !in_pel && stream_entry.is_none() {
            continue;
        }
        let prev_count = match group.pel_find(id) {
            Some(i) => group.pel[i].delivery_count,
            None => 0,
        };
        let prev_idle = match group.pel_find(id) {
            Some(i) => now - group.pel[i].delivery_time_ms,
            None => i64::MAX,
        };
        if in_pel && prev_idle < min_idle {
            continue;
        }
        let delivery_count = match opts.retrycount {
            Some(n) => n,
            None => {
                if opts.justid {
                    prev_count
                } else {
                    prev_count.saturating_add(1)
                }
            }
        };
        pel_reassign(group, id, &consumer_name, new_delivery_time, delivery_count);
        claimed.push((*id, stream_entry, delivery_count));
    }

    ctx.reply_array_header(claimed.len())?;
    for (id, entry_opt, _dc) in &claimed {
        if opts.justid {
            ctx.reply_bulk_string(RedisString::from_vec(id.to_display_bytes()))?;
        } else {
            ctx.reply_array_header(2usize)?;
            ctx.reply_bulk_string(RedisString::from_vec(id.to_display_bytes()))?;
            match entry_opt {
                Some(e) => {
                    ctx.reply_array_header(e.fields.len() * 2)?;
                    for (f, v) in &e.fields {
                        ctx.reply_bulk_string(f.clone())?;
                        ctx.reply_bulk_string(v.clone())?;
                    }
                }
                None => ctx.reply_null_array()?,
            }
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// XAUTOCLAIM
// ─────────────────────────────────────────────────────────────────────────────

/// XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID]
pub fn xautoclaim_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 6 {
        return Err(RedisError::wrong_number_of_args(b"xautoclaim"));
    }
    let key = ctx.arg_owned(1usize)?;
    let group_name = ctx.arg_owned(2usize)?;
    let consumer_name = ctx.arg_owned(3usize)?;
    let min_idle = parse_strict_i64(ctx.arg(4)?.as_bytes())?;
    let min_idle = if min_idle < 0 { 0 } else { min_idle };
    let start_bytes = ctx.arg(5)?.as_bytes().to_vec();
    let start_id = if start_bytes == b"-" {
        StreamId::ZERO
    } else if start_bytes == b"+" {
        StreamId::MAX
    } else {
        parse_explicit_id(&start_bytes)?
    };

    let mut count_limit: usize = 100;
    let mut justid = false;
    let mut idx = 6usize;
    while idx < argc {
        let arg = ctx.arg(idx)?.as_bytes();
        let upper = arg.to_ascii_uppercase();
        match upper.as_slice() {
            b"COUNT" => {
                if idx + 1 >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                let n = parse_strict_i64(ctx.arg(idx + 1)?.as_bytes())?;
                if n <= 0 {
                    return Err(RedisError::syntax(b"COUNT must be > 0"));
                }
                count_limit = n as usize;
                idx += 2;
            }
            b"JUSTID" => {
                justid = true;
                idx += 1;
            }
            _ => return Err(RedisError::syntax(b"syntax error")),
        }
    }

    let now = now_ms_clamped();
    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
    };
    if !stream.groups.contains_key(&group_name) {
        return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes()));
    }
    let entries_index: std::collections::HashMap<StreamId, StreamEntry> = stream
        .entries
        .iter()
        .map(|e| (e.id, e.clone()))
        .collect();
    let group = stream
        .groups
        .get_mut(&group_name)
        .expect("group existence checked above");
    touch_or_create_consumer(group, &consumer_name, now);
    if let Some(c) = group.consumers.get_mut(&consumer_name) {
        c.active_time_ms = now;
    }

    let start_pos = group.pel_lower_bound(&start_id);
    let candidates: Vec<StreamId> = group.pel[start_pos..]
        .iter()
        .map(|p| p.entry_id)
        .collect();

    let mut claimed: Vec<(StreamId, Option<StreamEntry>)> = Vec::new();
    let mut deleted_ids: Vec<StreamId> = Vec::new();
    let mut next_cursor = StreamId::ZERO;
    let mut visited = 0usize;
    for id in &candidates {
        if claimed.len() + deleted_ids.len() >= count_limit {
            next_cursor = *id;
            break;
        }
        visited += 1;
        let idle_ok = match group.pel_find(id) {
            Some(i) => now - group.pel[i].delivery_time_ms >= min_idle,
            None => false,
        };
        if !idle_ok {
            continue;
        }
        let entry_opt = entries_index.get(id).cloned();
        if entry_opt.is_none() {
            group.pel_remove(id);
            for c in group.consumers.values_mut() {
                if let Some(i) = c.pel_find(id) {
                    c.pel.remove(i);
                }
            }
            deleted_ids.push(*id);
            continue;
        }
        let prev_count = match group.pel_find(id) {
            Some(i) => group.pel[i].delivery_count,
            None => 0,
        };
        let delivery_count = if justid {
            prev_count
        } else {
            prev_count.saturating_add(1)
        };
        pel_reassign(group, id, &consumer_name, now, delivery_count);
        claimed.push((*id, entry_opt));
    }
    if visited == candidates.len() {
        next_cursor = StreamId::ZERO;
    }

    ctx.reply_array_header(3usize)?;
    ctx.reply_bulk_string(RedisString::from_vec(next_cursor.to_display_bytes()))?;
    ctx.reply_array_header(claimed.len())?;
    for (id, entry_opt) in &claimed {
        if justid {
            ctx.reply_bulk_string(RedisString::from_vec(id.to_display_bytes()))?;
        } else {
            ctx.reply_array_header(2usize)?;
            ctx.reply_bulk_string(RedisString::from_vec(id.to_display_bytes()))?;
            match entry_opt {
                Some(e) => {
                    ctx.reply_array_header(e.fields.len() * 2)?;
                    for (f, v) in &e.fields {
                        ctx.reply_bulk_string(f.clone())?;
                        ctx.reply_bulk_string(v.clone())?;
                    }
                }
                None => ctx.reply_null_array()?,
            }
        }
    }
    ctx.reply_array_header(deleted_ids.len())?;
    for id in &deleted_ids {
        ctx.reply_bulk_string(RedisString::from_vec(id.to_display_bytes()))?;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// XSETID
// ─────────────────────────────────────────────────────────────────────────────

/// XSETID key id [ENTRIESADDED n] [MAXDELETEDID id]
pub fn xsetid_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"xsetid"));
    }
    let key = ctx.arg_owned(1usize)?;
    let id_bytes = ctx.arg(2)?.as_bytes().to_vec();
    let new_id = parse_explicit_id(&id_bytes)?;
    let mut entries_added: Option<u64> = None;
    let mut max_deleted: Option<StreamId> = None;
    let mut i = 3usize;
    while i < argc {
        let arg = ctx.arg(i)?.as_bytes();
        let upper = arg.to_ascii_uppercase();
        match upper.as_slice() {
            b"ENTRIESADDED" => {
                if i + 1 >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                let n = parse_strict_i64(ctx.arg(i + 1)?.as_bytes())?;
                if n < 0 {
                    return Err(RedisError::syntax(b"ENTRIESADDED must be >= 0"));
                }
                entries_added = Some(n as u64);
                i += 2;
            }
            b"MAXDELETEDID" => {
                if i + 1 >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                max_deleted = Some(parse_explicit_id(ctx.arg(i + 1)?.as_bytes())?);
                i += 2;
            }
            _ => return Err(RedisError::syntax(b"syntax error")),
        }
    }
    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return Err(RedisError::runtime(b"ERR no such key")),
    };
    if let Some(top) = stream.entries.last() {
        if new_id < top.id {
            return Err(RedisError::runtime(
                b"ERR The ID specified in XSETID is smaller than the target stream top item",
            ));
        }
    }
    stream.last_id = new_id;
    if let Some(n) = entries_added {
        stream.entries_added = n;
    }
    if let Some(m) = max_deleted {
        stream.max_deleted_id = m;
    }
    ctx.reply_simple_string(b"OK")
}

// ─────────────────────────────────────────────────────────────────────────────
// XINFO GROUPS / CONSUMERS
// ─────────────────────────────────────────────────────────────────────────────

fn xinfo_groups(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"xinfo"));
    }
    let key = ctx.arg_owned(2usize)?;
    let stream = match as_stream_ref(ctx.db().lookup_key_read(&key))? {
        None => return Err(RedisError::runtime(b"ERR no such key")),
        Some(s) => s,
    };
    let mut group_views: Vec<(RedisString, usize, usize, StreamId, u64, u64)> = stream
        .groups
        .iter()
        .map(|(name, g)| {
            (
                name.clone(),
                g.consumers.len(),
                g.pel.len(),
                g.last_delivered_id,
                g.entries_read,
                stream.entries_added.saturating_sub(g.entries_read),
            )
        })
        .collect();
    group_views.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    ctx.reply_array_header(group_views.len())?;
    for (name, consumers, pending, last_id, entries_read, lag) in &group_views {
        ctx.reply_array_header(12usize)?;
        ctx.reply_bulk(b"name")?;
        ctx.reply_bulk_string(name.clone())?;
        ctx.reply_bulk(b"consumers")?;
        ctx.reply_integer(*consumers as i64)?;
        ctx.reply_bulk(b"pending")?;
        ctx.reply_integer(*pending as i64)?;
        ctx.reply_bulk(b"last-delivered-id")?;
        ctx.reply_bulk_string(RedisString::from_vec(last_id.to_display_bytes()))?;
        ctx.reply_bulk(b"entries-read")?;
        ctx.reply_integer(*entries_read as i64)?;
        ctx.reply_bulk(b"lag")?;
        ctx.reply_integer(*lag as i64)?;
    }
    Ok(())
}

fn xinfo_consumers(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(b"xinfo"));
    }
    let key = ctx.arg_owned(2usize)?;
    let group_name = ctx.arg_owned(3usize)?;
    let mut snapshot: Vec<(RedisString, usize, i64, i64)> = {
        let stream = match as_stream_ref(ctx.db().lookup_key_read(&key))? {
            None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
            Some(s) => s,
        };
        let group = match stream.groups.get(&group_name) {
            None => return Err(no_such_key_or_group_short(key.as_bytes(), group_name.as_bytes())),
            Some(g) => g,
        };
        group
            .consumers
            .values()
            .map(|c| (c.name.clone(), c.pel.len(), c.seen_time_ms, c.active_time_ms))
            .collect()
    };
    snapshot.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let now = now_ms_clamped();
    ctx.reply_array_header(snapshot.len())?;
    for (name, pending, seen, active) in &snapshot {
        let idle = (now - *seen).max(0);
        let inactive = (now - *active).max(0);
        ctx.reply_array_header(8usize)?;
        ctx.reply_bulk(b"name")?;
        ctx.reply_bulk_string(name.clone())?;
        ctx.reply_bulk(b"pending")?;
        ctx.reply_integer(*pending as i64)?;
        ctx.reply_bulk(b"idle")?;
        ctx.reply_integer(idle)?;
        ctx.reply_bulk(b"inactive")?;
        ctx.reply_integer(inactive)?;
    }
    Ok(())
}
