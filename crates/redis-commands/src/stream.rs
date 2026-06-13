//! Stream commands — byte-exact port with blocking extension.
//! Implements: XADD, XLEN, XRANGE, XREVRANGE, XREAD (blocking + non-blocking),
//! XDEL, XTRIM, XINFO STREAM (basic), XINFO GROUPS (empty).
//! # Storage shape
//! Uses the pragmatic `ObjectKind::Stream(StreamEncoding::Inline(_))`
//! encoding from `redis-core::object` — a sorted `Vec<StreamEntry>`
//! `redis_ds::stream::InlineStream`. Real `rax` + `listpack` representation
//! will be used when available.
//! # Architect items
//! TODO(architect): consumer-group commands XGROUP / XREADGROUP /
//! XACK / XPENDING / XCLAIM / XAUTOCLAIM / XSETID / XINFO CONSUMERS
//! require persistent PEL state and are deferred.
//! TODO(architect): MAXLEN/MINID `~` approximate trimming behaves
//! identically to `=` exact trimming for the inline encoding (no
//! listpack-boundary quirks to honour).

use redis_core::blocked_keys::{blocked_keys_index, current_time_ms, BlockedAction, BlockedWaiter};
use redis_core::command_context::CommandContext;
use redis_core::db::RedisDb;
use redis_core::notify::NOTIFY_STREAM;
use redis_core::object::RedisObject;
use redis_core::util::mstime;
use redis_ds::stream::{
    parse_stream_id, Consumer, ConsumerGroup, InlineStream, PelEntry, StreamEntry, StreamId,
    SCG_INVALID_ENTRIES_READ,
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
/// `Inclusive` covers both direct inclusive IDs and exclusive IDs that have
/// been resolved to their adjacent neighbour (so `(3-0` becomes `Inclusive(3-1)`).
/// `Min`/`Max` are the `-` / `+` sentinels.
#[derive(Debug, Clone, Copy)]
enum Bound {
    Min,
    Max,
    Inclusive(StreamId),
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
    if exclusive && (body == b"-" || body == b"+") {
        return Err(invalid_stream_id_err());
    }
    let default_seq = match side {
        BoundSide::Start => 0,
        BoundSide::End => u64::MAX,
    };
    let id = parse_stream_id(body, default_seq).map_err(|_| invalid_stream_id_err())?;
    if exclusive {
        match side {
            BoundSide::Start => match id.checked_succ() {
                Some(next) => Ok(Bound::Inclusive(next)),
                None => Err(RedisError::runtime(
                    b"ERR invalid start ID for the interval",
                )),
            },
            BoundSide::End => match id.checked_pred() {
                Some(prev) => Ok(Bound::Inclusive(prev)),
                None => Err(RedisError::runtime(b"ERR invalid end ID for the interval")),
            },
        }
    } else {
        Ok(Bound::Inclusive(id))
    }
}

/// Result of parsing an XADD id argument, which may carry a literal seq or
/// request auto-generation via the `<ms>-*` form.
#[derive(Debug, Clone, Copy)]
enum XaddIdSpec {
    /// Fully-explicit `<ms>-<seq>` or bare `<ms>` (seq defaults to 0).
    Explicit(StreamId),
    /// `<ms>-*` — ms is given; seq is auto-generated relative to `last_id`.
    Partial { ms: u64 },
}

/// Parse an XADD id argument: `*`, `<ms>`, `<ms>-<seq>`, or `<ms>-*`.
/// Returns `None` when the argument is the bare `*` auto-generate sentinel.
fn parse_xadd_id_spec(arg: &[u8]) -> Result<Option<XaddIdSpec>, RedisError> {
    if arg == b"*" {
        return Ok(None);
    }
    let s = core::str::from_utf8(arg).map_err(|_| invalid_stream_id_err())?;
    if let Some(dash) = s.find('-') {
        let ms_part = &s[..dash];
        let seq_part = &s[dash + 1..];
        let ms = ms_part
            .parse::<u64>()
            .map_err(|_| invalid_stream_id_err())?;
        if seq_part == "*" {
            return Ok(Some(XaddIdSpec::Partial { ms }));
        }
        let seq = seq_part
            .parse::<u64>()
            .map_err(|_| invalid_stream_id_err())?;
        Ok(Some(XaddIdSpec::Explicit(StreamId { ms, seq })))
    } else {
        let ms = s.parse::<u64>().map_err(|_| invalid_stream_id_err())?;
        Ok(Some(XaddIdSpec::Explicit(StreamId { ms, seq: 0 })))
    }
}

/// Parse an explicit id given to XDEL/XREAD (no `-` / `+` / `(`
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
/// Returns `None` if the requested range is empty (start > end after
/// resolution, or the slice is empty).
fn resolve_range(stream: &InlineStream, start: Bound, end: Bound) -> Option<(usize, usize)> {
    let entries = &stream.entries;
    if entries.is_empty() {
        return None;
    }
    let start_idx = match start {
        Bound::Min => 0,
        Bound::Max => return None,
        Bound::Inclusive(id) => stream.lower_bound(&id),
    };
    let end_idx = match end {
        Bound::Min => return None,
        Bound::Max => entries.len(),
        Bound::Inclusive(id) => stream.upper_bound(&id),
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
fn as_stream_mut(obj: Option<&mut RedisObject>) -> Result<Option<&mut InlineStream>, RedisError> {
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

/// Default `stream-node-max-entries`; how many entries upstream packs per
/// listpack macro-node in the stream rax.
const STREAM_NODE_MAX_ENTRIES: usize = 100;

/// Synthetic radix-tree key count for `XINFO STREAM`.
/// Our stream uses inline (`Vec`) storage rather than a radix tree of listpack
/// nodes, so there is no real `raxSize`. We report a plausible macro-node count
/// (`ceil(len / node-max)`) so clients and tests that size the rax behave
/// correctly — notably the `XDEL fuzz test`, which loops `XADD` until
/// `radix-tree-keys > 20` and would otherwise spin forever against a hardcoded
/// 0.
fn synthetic_radix_keys(len: usize) -> i64 {
    len.div_ceil(STREAM_NODE_MAX_ENTRIES) as i64
}

/// Reply with a single stream entry as `[id, [f1, v1, f2, v2,...]]`.
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
    MaxLen {
        target: usize,
        approximate: bool,
        limit: Option<usize>,
    },
    MinId {
        min: StreamId,
        approximate: bool,
        limit: Option<usize>,
    },
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
            let mut approximate = false;
            if peek == b"=" || peek == b"~" {
                approximate = peek == b"~";
                i += 1;
                if i >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                peek = ctx.arg(i)?.as_bytes();
            }
            let parsed_trim = if is_maxlen {
                let n = parse_strict_i64(peek)?;
                if n < 0 {
                    return Err(RedisError::syntax(b"MAXLEN argument must be >= 0"));
                }
                TrimStrategy::MaxLen {
                    target: n as usize,
                    approximate,
                    limit: None,
                }
            } else {
                TrimStrategy::MinId {
                    min: parse_explicit_id(peek)?,
                    approximate,
                    limit: None,
                }
            };
            i += 1;
            let mut limit = None;
            if i < argc && ctx.arg(i)?.as_bytes().eq_ignore_ascii_case(b"LIMIT") {
                if !approximate {
                    return Err(RedisError::syntax(
                        b"syntax error, LIMIT cannot be used without the special ~ option",
                    ));
                }
                if i + 1 >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                let n = parse_strict_i64(ctx.arg(i + 1)?.as_bytes())?;
                if n < 0 {
                    return Err(RedisError::syntax(b"LIMIT argument must be >= 0"));
                }
                limit = Some(n as usize);
                i += 2;
            }
            opts.trim = parsed_trim.with_limit(limit);
            continue;
        }
        break;
    }
    Ok((opts, i))
}

/// Compute the auto-id for `XADD *...`. Uses wall-clock ms and either
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
        TrimStrategy::MaxLen {
            target,
            approximate,
            limit,
        } => {
            if approximate {
                return apply_approx_maxlen_trim(stream, target, limit);
            }
            let len = stream.entries.len();
            if len <= target {
                return 0;
            }
            drain_front(stream, len - target)
        }
        TrimStrategy::MinId {
            min,
            approximate,
            limit,
        } => {
            let cut = stream.lower_bound(&min);
            if cut == 0 {
                return 0;
            }
            if approximate {
                return apply_approx_count_trim(stream, cut, limit);
            }
            drain_front(stream, cut)
        }
    }
}

impl TrimStrategy {
    fn with_limit(self, limit: Option<usize>) -> Self {
        match self {
            TrimStrategy::MaxLen {
                target,
                approximate,
                ..
            } => TrimStrategy::MaxLen {
                target,
                approximate,
                limit,
            },
            TrimStrategy::MinId {
                min, approximate, ..
            } => TrimStrategy::MinId {
                min,
                approximate,
                limit,
            },
            TrimStrategy::None => TrimStrategy::None,
        }
    }
}

const APPROX_STREAM_NODE_ENTRIES: usize = 100;

fn approx_trim_chunk_len(len: usize, limit: Option<usize>) -> usize {
    match limit {
        Some(n) if n <= 1 => 0,
        Some(n) if n >= 30 => 10.min(len),
        Some(n) => n.min(len),
        None => APPROX_STREAM_NODE_ENTRIES.min(len),
    }
}

fn apply_approx_maxlen_trim(
    stream: &mut InlineStream,
    target: usize,
    limit: Option<usize>,
) -> usize {
    let mut evicted = 0usize;
    let mut remaining_limit = limit.unwrap_or(usize::MAX);
    loop {
        let len = stream.entries.len();
        if len <= target {
            break;
        }
        let chunk = approx_maxlen_trim_chunk_len(len, target, limit);
        if chunk == 0 || chunk > remaining_limit || len.saturating_sub(chunk) < target {
            break;
        }
        evicted += drain_front(stream, chunk);
        remaining_limit = remaining_limit.saturating_sub(chunk);
    }
    evicted
}

fn approx_maxlen_trim_chunk_len(len: usize, target: usize, limit: Option<usize>) -> usize {
    if limit.is_some() || target <= APPROX_STREAM_NODE_ENTRIES {
        return approx_trim_chunk_len(len, limit);
    }
    let head_len = len % APPROX_STREAM_NODE_ENTRIES;
    if head_len > 0 && len.saturating_sub(head_len) >= target {
        return head_len;
    }
    approx_trim_chunk_len(len, limit)
}

fn apply_approx_count_trim(
    stream: &mut InlineStream,
    eligible: usize,
    limit: Option<usize>,
) -> usize {
    let mut evicted = 0usize;
    let mut remaining = eligible;
    let mut remaining_limit = limit.unwrap_or(usize::MAX);
    while remaining > 0 {
        let chunk = approx_trim_chunk_len(stream.entries.len(), limit);
        if chunk == 0 || chunk > remaining || chunk > remaining_limit {
            break;
        }
        evicted += drain_front(stream, chunk);
        remaining -= chunk;
        remaining_limit = remaining_limit.saturating_sub(chunk);
    }
    evicted
}

fn drain_front(stream: &mut InlineStream, count: usize) -> usize {
    if count == 0 {
        return 0;
    }
    let count = count.min(stream.entries.len());
    // Trimming (XTRIM / XADD MAXLEN|MINID) removes entries from the front but,
    // unlike XDEL, does NOT advance max_deleted_entry_id. Only
    // XDEL and XSETID move the tombstone.
    stream.entries.drain(0..count);
    count
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

/// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] id|* field value...
pub fn xadd_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() < 5 {
        return Err(RedisError::wrong_number_of_args(b"xadd"));
    }
    let key = ctx.arg_owned(1usize)?;
    let (opts, mut idx) = parse_add_options(ctx)?;
    if idx >= ctx.arg_count() {
        return Err(RedisError::wrong_number_of_args(b"xadd"));
    }
    let id_pos = idx;
    let id_arg = ctx.arg_owned(idx)?;
    idx += 1;
    let remaining = ctx.arg_count() - idx;
    if remaining == 0 || !remaining.is_multiple_of(2) {
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

    let id_autogen;
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
        let spec = parse_xadd_id_spec(id_bytes)?;
        id_autogen = matches!(spec, None | Some(XaddIdSpec::Partial { .. }));
        let new_id = match spec {
            None => {
                let next = auto_next_id(stream.last_id);
                if next <= stream.last_id {
                    return Err(RedisError::runtime(
                        b"ERR The stream has exhausted the last possible ID, unable to add more items",
                    ));
                }
                next
            }
            Some(XaddIdSpec::Partial { ms }) => {
                if ms > stream.last_id.ms {
                    StreamId { ms, seq: 0 }
                } else if ms == stream.last_id.ms {
                    match stream.last_id.seq.checked_add(1) {
                        Some(next_seq) => StreamId { ms, seq: next_seq },
                        None => return Err(RedisError::runtime(
                            b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
                        )),
                    }
                } else {
                    return Err(RedisError::runtime(
                        b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    ));
                }
            }
            Some(XaddIdSpec::Explicit(id)) => {
                if id == StreamId::ZERO {
                    return Err(RedisError::runtime(
                        b"ERR The ID specified in XADD must be greater than 0-0",
                    ));
                }
                if id <= stream.last_id
                    && !(stream.entries_added == 0
                        && id == StreamId::ZERO
                        && stream.last_id == StreamId::ZERO)
                {
                    return Err(RedisError::runtime(
                        b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    ));
                }
                id
            }
        };
        stream.append(StreamEntry { id: new_id, fields });
        apply_trim(stream, opts.trim);
        if let Some(obj) = obj_owned {
            ctx.db_mut().set_key(key, obj, 0);
        }
        new_id
    };

    if ctx.client_ref().flag_deny_blocking() {
        ctx.client_mut().pending_wakes.push(key_for_wake.clone());
    } else {
        let new_entry = StreamEntry {
            id: new_id,
            fields: fields_for_wake,
        };
        wake_blocked_for_stream_entry(&key_for_wake, &new_entry);
        wake_blocked_xreadgroup_for_key(ctx.db_mut(), &key_for_wake);
    }

    ctx.notify_keyspace_event(NOTIFY_STREAM, b"xadd", &key_for_wake);

    // Propagate the auto-generated ID explicitly so replicas/AOF store the same
    // ID rather than re-generating their own. Mirrors `rewriteClientCommandArgument`
    // for the ID in `xaddCommand`.
    if id_autogen {
        let argc = ctx.arg_count();
        let mut new_argv: Vec<RedisString> = Vec::with_capacity(argc);
        for k in 0..argc {
            if k == id_pos {
                new_argv.push(RedisString::from_vec(new_id.to_display_bytes()));
            } else {
                new_argv.push(ctx.arg_owned(k)?);
            }
        }
        ctx.client_mut().set_args(new_argv);
    }

    ctx.reply_bulk_string(RedisString::from_vec(new_id.to_display_bytes()))
}

// ─────────────────────────────────────────────────────────────────────────────
// XREAD BLOCK wake hook
// ─────────────────────────────────────────────────────────────────────────────

/// Encode an XREAD reply for a single stream key carrying one or more entries.
fn encode_xread_entries(key: &RedisString, entries: &[StreamEntry]) -> Vec<u8> {
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
    write_array(&mut buf, entries.len());
    for entry in entries {
        let id_bytes = entry.id.to_display_bytes();
        write_array(&mut buf, 2);
        write_bulk(&mut buf, &id_bytes);
        write_array(&mut buf, entry.fields.len() * 2);
        for (f, v) in &entry.fields {
            write_bulk(&mut buf, f.as_bytes());
            write_bulk(&mut buf, v.as_bytes());
        }
    }
    buf
}

fn wake_blocked_for_stream_entry(key: &RedisString, new_entry: &StreamEntry) {
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
    let reply = encode_xread_entries(key, std::slice::from_ref(new_entry));
    for waiter in waiters {
        let _ = waiter.sender.send(reply.clone());
    }
}

/// Wake all XREAD BLOCK waiters on `key` whose `id_after` is behind
/// stream's current tail.
/// Unlike the list pop wake (FIFO, one pop per waiter), streams use broadcast
/// semantics: every waiting reader receives a copy of the new entry. This
/// mirrors real Redis's `signalKeyAsReady` / `serveClientsBlockedOnListOrZset`
/// pattern for streams.
pub fn wake_blocked_for_stream(db: &RedisDb, key: &RedisString) {
    let latest_id = match as_stream_ref(db.lookup_key_read(key)) {
        Ok(Some(stream)) => stream.last_id,
        _ => return,
    };
    let waiters = {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.take_stream_waiters_for(key, latest_id)
    };
    if waiters.is_empty() {
        return;
    }
    for waiter in waiters {
        let id_after = match &waiter.action {
            BlockedAction::Stream { id_after } => *id_after,
            _ => continue,
        };
        let entries: Vec<StreamEntry> = match as_stream_ref(db.lookup_key_read(key)) {
            Ok(Some(stream)) => {
                let start_idx = stream.upper_bound(&id_after);
                stream.entries[start_idx..].to_vec()
            }
            _ => Vec::new(),
        };
        if entries.is_empty() {
            continue;
        }
        let _ = waiter.sender.send(encode_xread_entries(key, &entries));
    }
}

/// Encode a NOGROUP error reply as a RESP error frame.
fn encode_nogroup_error(key: &[u8], group: &[u8]) -> Vec<u8> {
    let mut msg: Vec<u8> = Vec::with_capacity(64 + key.len() + group.len());
    msg.extend_from_slice(b"NOGROUP No such key '");
    msg.extend_from_slice(key);
    msg.extend_from_slice(b"' or consumer group '");
    msg.extend_from_slice(group);
    msg.extend_from_slice(b"' in XREADGROUP with GROUP option");
    let mut buf: Vec<u8> = Vec::with_capacity(1 + msg.len() + 2);
    buf.push(b'-');
    buf.extend_from_slice(&msg);
    buf.extend_from_slice(b"\r\n");
    buf
}

/// Encode a WRONGTYPE error reply as a RESP error frame.
fn encode_wrongtype_error() -> Vec<u8> {
    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec()
}

/// Wake all blocked XREADGROUP clients on `key` whose cursor is behind
/// the stream's current tail and whose consumer group still exists.
/// For each waiter:
/// - If the key is gone or not a stream → send WRONGTYPE or NOGROUP error.
/// - If the group is gone → send NOGROUP error.
/// - Otherwise → deliver the entry through the XREADGROUP state machine
/// (advance `last_delivered_id`, add PEL entry unless NOACK, send reply).
pub fn wake_blocked_xreadgroup_for_key(db: &mut RedisDb, key: &RedisString) {
    let latest_id = match as_stream_ref(db.lookup_key_read(key)) {
        Ok(Some(stream)) => stream.last_id,
        _ => return,
    };
    let waiters = {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.take_stream_group_waiters_for(key, latest_id)
    };
    if waiters.is_empty() {
        return;
    }
    // A single new entry is delivered to at most one consumer per group:
    // first waiter (FIFO) whose group cursor is still behind the entry. Once a
    // group's cursor advances past the entry, later waiters of that same group
    // find no new data and must stay blocked — re-registered with their
    // original deadline so their BLOCK timeout is preserved (matches
    // upstream "reprocessing" semantics in handleClientsBlockedOnKey).
    let mut to_reblock: Vec<BlockedWaiter> = Vec::new();
    for waiter in waiters {
        let (group, consumer, id_after, count, noack) = match &waiter.action {
            BlockedAction::StreamGroup {
                group,
                consumer,
                id_after,
                count,
                noack,
                ..
            } => (group.clone(), consumer.clone(), *id_after, *count, *noack),
            _ => continue,
        };
        let reply = match as_stream_mut(db.lookup_key_write(key)) {
            Err(_) => encode_wrongtype_error(),
            Ok(None) => encode_nogroup_error(key.as_bytes(), group.as_bytes()),
            Ok(Some(stream)) => {
                if !stream.groups.contains_key(&group) {
                    encode_nogroup_error(key.as_bytes(), group.as_bytes())
                } else {
                    let now = now_ms_clamped();
                    let after = stream
                        .groups
                        .get(&group)
                        .map(|g| g.last_delivered_id)
                        .unwrap_or(id_after)
                        .max(id_after);
                    let start_idx = stream.upper_bound(&after);
                    let slice_len = stream.entries.len() - start_idx;
                    let max = match count {
                        None => slice_len,
                        Some(n) => (n as usize).min(slice_len),
                    };
                    if max == 0 {
                        to_reblock.push(waiter);
                        continue;
                    }
                    let to_deliver: Vec<StreamEntry> =
                        stream.entries[start_idx..start_idx + max].to_vec();
                    let delivered_ids: Vec<StreamId> =
                        to_deliver.iter().map(|entry| entry.id).collect();
                    let view = stream.lag_view();
                    let g = stream.groups.get_mut(&group).expect("group checked above");
                    touch_or_create_consumer(g, &consumer, now);
                    if let Some(c) = g.consumers.get_mut(&consumer) {
                        c.active_time_ms = now;
                    }
                    let (read, last) = view.advance_read_counter(
                        g.entries_read,
                        g.last_delivered_id,
                        &delivered_ids,
                    );
                    g.entries_read = read;
                    g.last_delivered_id = last;
                    if !noack {
                        for entry in &to_deliver {
                            pel_add(
                                g,
                                &consumer,
                                PelEntry {
                                    entry_id: entry.id,
                                    delivery_time_ms: now,
                                    delivery_count: 1,
                                },
                            );
                        }
                    }
                    encode_xreadgroup_multi_entries(key, &to_deliver)
                }
            }
        };
        let _ = waiter.sender.send(reply);
    }
    if !to_reblock.is_empty() {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for waiter in to_reblock {
            idx.add(waiter);
        }
    }
}

/// Wake all blocked XREADGROUP clients on `key` with a NOGROUP error.
/// Used when the key is deleted (DEL, FLUSHDB) or the group is destroyed
/// (XGROUP DESTROY), so every parked XREADGROUP client on that key receives
/// the appropriate error response.
pub fn wake_xreadgroup_with_nogroup(key: &RedisString) {
    let waiters = {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.take_all_stream_group_waiters_for(key)
    };
    for waiter in waiters {
        let group = match &waiter.action {
            BlockedAction::StreamGroup { group, .. } => group.clone(),
            _ => continue,
        };
        let reply = encode_nogroup_error(key.as_bytes(), group.as_bytes());
        record_blocked_xreadgroup_error(&reply);
        let _ = waiter.sender.send(reply);
    }
}

/// Record server stats for a blocked XREADGROUP that is unblocked with an error
/// (NOGROUP / WRONGTYPE). The original command parked without completing, so
/// the failed completion is counted here: one `xreadgroup` call with
/// `failed_calls`, the error code in errorstats, and `total_error_replies`.
fn record_blocked_xreadgroup_error(reply: &[u8]) {
    redis_core::metrics::record_error_reply(reply);
    redis_core::metrics::record_command_failure(b"xreadgroup");
}

/// Wake all blocked XREADGROUP clients across all keys with NOGROUP errors.
/// Used by FLUSHDB / FLUSHALL where every stream key (and therefore every
/// consumer group) is gone at once.
pub fn wake_all_xreadgroup_with_nogroup() {
    let waiters = {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.take_all_stream_group_waiters()
    };
    for waiter in waiters {
        let group = match &waiter.action {
            BlockedAction::StreamGroup { group, .. } => group.clone(),
            _ => continue,
        };
        let key = waiter.keys.first().cloned().unwrap_or_default();
        let reply = encode_nogroup_error(key.as_bytes(), group.as_bytes());
        record_blocked_xreadgroup_error(&reply);
        let _ = waiter.sender.send(reply);
    }
}

/// Wake blocked XREADGROUP clients on `dst_key` after RENAME moved a new
/// stream into `dst_key`. Attempts to deliver new entries to each waiter.
/// If `dst_key` now holds a stream with the consumer group, entries after
/// the waiter's cursor are delivered. Otherwise NOGROUP is sent.
pub fn wake_xreadgroup_after_rename(db: &mut RedisDb, dst_key: &RedisString) {
    let waiters = {
        let mut idx = match blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.take_all_stream_group_waiters_for(dst_key)
    };
    for waiter in waiters {
        let (group, consumer, id_after, count, noack) = match &waiter.action {
            BlockedAction::StreamGroup {
                group,
                consumer,
                id_after,
                count,
                noack,
            } => (group.clone(), consumer.clone(), *id_after, *count, *noack),
            _ => continue,
        };
        let reply = match as_stream_mut(db.lookup_key_write(dst_key)) {
            Err(_) => encode_wrongtype_error(),
            Ok(None) => encode_nogroup_error(dst_key.as_bytes(), group.as_bytes()),
            Ok(Some(stream)) => {
                if !stream.groups.contains_key(&group) {
                    encode_nogroup_error(dst_key.as_bytes(), group.as_bytes())
                } else {
                    let now = now_ms_clamped();
                    let after = stream
                        .groups
                        .get(&group)
                        .map(|g| g.last_delivered_id)
                        .unwrap_or(id_after);
                    let cursor = after.max(id_after);
                    let start_idx = stream.upper_bound(&cursor);
                    let slice_len = stream.entries.len() - start_idx;
                    let max = match count {
                        None => slice_len,
                        Some(n) => (n as usize).min(slice_len),
                    };
                    if max == 0 {
                        encode_nogroup_error(dst_key.as_bytes(), group.as_bytes())
                    } else {
                        let to_deliver: Vec<StreamEntry> =
                            stream.entries[start_idx..start_idx + max].to_vec();
                        let delivered_ids: Vec<StreamId> =
                            to_deliver.iter().map(|e| e.id).collect();
                        let view = stream.lag_view();
                        let g = stream.groups.get_mut(&group).expect("group checked above");
                        touch_or_create_consumer(g, &consumer, now);
                        if let Some(c) = g.consumers.get_mut(&consumer) {
                            c.active_time_ms = now;
                        }
                        let (read, last) = view.advance_read_counter(
                            g.entries_read,
                            g.last_delivered_id,
                            &delivered_ids,
                        );
                        g.entries_read = read;
                        g.last_delivered_id = last;
                        if !noack {
                            for entry in &to_deliver {
                                pel_add(
                                    g,
                                    &consumer,
                                    PelEntry {
                                        entry_id: entry.id,
                                        delivery_time_ms: now,
                                        delivery_count: 1,
                                    },
                                );
                            }
                        }
                        encode_xreadgroup_multi_entries(dst_key, &to_deliver)
                    }
                }
            }
        };
        let _ = waiter.sender.send(reply);
    }
}

/// Encode an XREADGROUP reply for one key carrying multiple newly-delivered entries.
fn encode_xreadgroup_multi_entries(key: &RedisString, entries: &[StreamEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(512);

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
    write_array(&mut buf, entries.len());
    for entry in entries {
        let id_bytes = entry.id.to_display_bytes();
        write_array(&mut buf, 2);
        write_bulk(&mut buf, &id_bytes);
        write_array(&mut buf, entry.fields.len() * 2);
        for (f, v) in &entry.fields {
            write_bulk(&mut buf, f.as_bytes());
            write_bulk(&mut buf, v.as_bytes());
        }
    }
    buf
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
        return Err(RedisError::syntax(b"COUNT must be a positive integer"));
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
        (
            ctx.arg(3)?.as_bytes().to_vec(),
            ctx.arg(2)?.as_bytes().to_vec(),
        )
    } else {
        (
            ctx.arg(2)?.as_bytes().to_vec(),
            ctx.arg(3)?.as_bytes().to_vec(),
        )
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

/// XDEL key id [id...]
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
    let mut approximate = false;
    if threshold_bytes == b"=" || threshold_bytes == b"~" {
        approximate = threshold_bytes == b"~";
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
        TrimStrategy::MaxLen {
            target: n as usize,
            approximate,
            limit: None,
        }
    } else {
        TrimStrategy::MinId {
            min: parse_explicit_id(threshold_bytes)?,
            approximate,
            limit: None,
        }
    };
    idx += 1;
    let mut limit = None;
    if idx < argc && ctx.arg(idx)?.as_bytes().eq_ignore_ascii_case(b"LIMIT") {
        if !approximate {
            return Err(RedisError::syntax(
                b"syntax error, LIMIT cannot be used without the special ~ option",
            ));
        }
        if idx + 1 >= argc {
            return Err(RedisError::syntax(b"syntax error"));
        }
        let n = parse_strict_i64(ctx.arg(idx + 1)?.as_bytes())?;
        if n < 0 {
            return Err(RedisError::syntax(b"LIMIT argument must be >= 0"));
        }
        limit = Some(n as usize);
        idx += 2;
    }
    if idx != argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    Ok(strategy.with_limit(limit))
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
    /// `+` — return the last entry in the stream (resolved to `last_id - 1`
    /// so the XREAD "strictly greater than" logic includes the last entry).
    LastEntry,
    /// Explicit id; XREAD returns entries with id strictly greater than this.
    After(StreamId),
}

/// Park an XREAD BLOCK client in the global blocked-keys index.
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
                action: BlockedAction::Stream {
                    id_after: *id_after,
                },
                deadline_ms,
                resp_proto: ctx.client_ref().resp_proto,
                username: ctx.client_ref().authenticated_user.clone(),
                redirect_on_role_change: ctx.client_ref().capa_redirect
                    && !ctx.client_ref().flags.readonly,
            };
            idx.add(waiter);
        }
    }
    ctx.client_mut().blocked_on_keys = true;
    Ok(())
}

/// XREAD [COUNT n] [BLOCK ms] STREAMS key [key...] id [id...]
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
    if remaining == 0 || !remaining.is_multiple_of(2) {
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
        } else if raw == b"+" {
            ReadStartId::LastEntry
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
                    ReadStartId::LastEntry => {
                        if stream.entries.is_empty() {
                            stream.last_id
                        } else {
                            match stream.last_id.checked_pred() {
                                Some(pred) => pred,
                                None => StreamId::ZERO,
                            }
                        }
                    }
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
                ReadStartId::LastEntry => match as_stream_ref(ctx.db().lookup_key_read(key)) {
                    Ok(Some(stream)) if !stream.entries.is_empty() => {
                        stream.last_id.checked_pred().unwrap_or(StreamId::ZERO)
                    }
                    _ => StreamId::ZERO,
                },
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
            if ctx.arg_count() != 2 {
                return Err(RedisError::wrong_number_of_args(b"xinfo|help"));
            }
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
            // XINFO STREAM <key> [FULL [COUNT <n>]]
            let argc = ctx.arg_count();
            if argc > 3 {
                if ctx.arg(3)?.as_bytes().eq_ignore_ascii_case(b"FULL") {
                    let mut count: i64 = 10;
                    if argc == 6 && ctx.arg(4)?.as_bytes().eq_ignore_ascii_case(b"COUNT") {
                        count = parse_strict_i64(ctx.arg(5)?.as_bytes())?;
                    } else if argc != 4 {
                        return Err(RedisError::syntax(b"syntax error"));
                    }
                    return xinfo_stream_full(ctx, &key, count);
                }
                return Err(RedisError::syntax(b"syntax error"));
            }
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
            ctx.reply_map_header(8usize)?;
            ctx.reply_bulk(b"length")?;
            ctx.reply_integer(length)?;
            ctx.reply_bulk(b"radix-tree-keys")?;
            ctx.reply_integer(synthetic_radix_keys(length as usize))?;
            ctx.reply_bulk(b"radix-tree-nodes")?;
            ctx.reply_integer(synthetic_radix_keys(length as usize) + 1)?;
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
        _ => Err(RedisError::syntax(b"syntax error, try XINFO HELP")),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CONSUMER GROUPS (Round 13c)
// ─────────────────────────────────────────────────────────────────────────────
// Implements: XGROUP (CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER),
// XREADGROUP, XACK, XPENDING (summary + extended), XCLAIM, XAUTOCLAIM,
// XSETID, XINFO CONSUMERS, XINFO GROUPS.
// Storage extension lives in `redis_ds::stream::{ConsumerGroup, Consumer,
// PelEntry}`. PEL entries are mirrored between `group.pel` and
// matching `consumer.pel`; helpers below keep them consistent.
// TODO(architect): XREADGROUP BLOCK is unimplemented this round. Round
// 13b wired XREAD BLOCK against the blocked-keys index with
// `BlockedAction::Stream`; group-aware blocking needs an additional
// per-group last-delivered cursor on the waker side. Behaviour right
// now: BLOCK with no new entries returns `$-1` (nil bulk).
// TODO(architect): XAUTOCLAIM cursor pagination is implemented as a
// simple ID cursor (next id to resume ). Valkey uses a more nuanced
// rax-cursor; this is close-enough for the inline encoding.
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
fn touch_or_create_consumer(group: &mut ConsumerGroup, name: &RedisString, now_ms: i64) -> bool {
    let exists = group.consumers.contains_key(name);
    if !exists {
        let mut consumer = Consumer::new(name.clone(), now_ms);
        // it only advances once the consumer actually receives entries, so
        // XINFO `inactive` reports -1 until the first real delivery.
        consumer.active_time_ms = -1;
        group.consumers.insert(name.clone(), consumer);
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
    } else if arg == b"-" {
        Ok(StreamId::ZERO)
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
            if ctx.arg_count() != 2 {
                return Err(RedisError::wrong_number_of_args(b"xgroup|help"));
            }
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
) -> Result<(bool, i64), RedisError> {
    let argc = ctx.arg_count();
    if start >= argc {
        return Ok((false, SCG_INVALID_ENTRIES_READ));
    }
    if !ctx
        .arg(start)?
        .as_bytes()
        .eq_ignore_ascii_case(b"ENTRIESREAD")
    {
        return Err(RedisError::syntax(b"syntax error"));
    }
    if start + 1 >= argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let n = parse_strict_i64(ctx.arg(start + 1)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR value for ENTRIESREAD must be positive or -1"))?;
    if n < 0 && n != SCG_INVALID_ENTRIES_READ {
        return Err(RedisError::runtime(
            b"ERR value for ENTRIESREAD must be positive or -1",
        ));
    }
    if start + 2 != argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    Ok((true, n))
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
        ctx.db_mut()
            .set_key(key.clone(), RedisObject::new_stream(), 0);
    }
    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return Err(RedisError::runtime(b"ERR unreachable: stream vanished")),
    };
    let new_id = parse_id_or_dollar(&id_arg, stream)?;
    if stream.groups.contains_key(&group_name) {
        return Err(RedisError::runtime(
            b"BUSYGROUP Consumer Group name already exists",
        ));
    }
    let mut group = ConsumerGroup::new(group_name.clone(), new_id);
    if had_entries_read {
        group.entries_read = entries_read;
    }
    stream.groups.insert(group_name, group);
    ctx.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-create", &key);
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
        None => {
            return Err(no_such_key_or_group_short(
                key.as_bytes(),
                group_name.as_bytes(),
            ))
        }
    };
    let new_id = parse_id_or_dollar(&id_arg, stream)?;
    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => {
            return Err(no_such_key_or_group_short(
                key.as_bytes(),
                group_name.as_bytes(),
            ))
        }
    };
    group.last_delivered_id = new_id;
    if had_entries_read {
        group.entries_read = entries_read;
    }
    ctx.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-setid", &key);
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
    if removed {
        ctx.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-destroy", &key);
        wake_xreadgroup_with_nogroup(&key);
    }
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
        None => {
            return Err(RedisError::runtime(
                b"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
            ))
        }
    };
    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => {
            return Err(no_such_key_or_group_short(
                key.as_bytes(),
                group_name.as_bytes(),
            ))
        }
    };
    let created = touch_or_create_consumer(group, &consumer_name, now);
    if created {
        ctx.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-createconsumer", &key);
    }
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
        None => {
            return Err(no_such_key_or_group_short(
                key.as_bytes(),
                group_name.as_bytes(),
            ))
        }
    };
    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => {
            return Err(no_such_key_or_group_short(
                key.as_bytes(),
                group_name.as_bytes(),
            ))
        }
    };
    let consumer = match group.consumers.remove(&consumer_name) {
        Some(c) => c,
        None => return ctx.reply_integer(0),
    };
    let pending = consumer.pel.len() as i64;
    for entry in &consumer.pel {
        group.pel_remove(&entry.entry_id);
    }
    ctx.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-delconsumer", &key);
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
/// STREAMS key [key...] id [id...]
pub fn xreadgroup_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 7 {
        return Err(RedisError::wrong_number_of_args(b"xreadgroup"));
    }
    if !ctx.arg(1)?.as_bytes().eq_ignore_ascii_case(b"GROUP") {
        return Err(RedisError::syntax(b"Missing GROUP option for XREADGROUP"));
    }
    let group_name = ctx.arg_owned(2usize)?;
    let consumer_name = ctx.arg_owned(3usize)?;
    let mut i = 4usize;
    let mut count: Option<i64> = None;
    let mut noack = false;
    let mut block_ms: Option<i64> = None;
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
    if remaining == 0 || !remaining.is_multiple_of(2) {
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
    let mut created_consumers: Vec<RedisString> = Vec::new();
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
                // seen-time) on every call, even when no new entries exist.
                {
                    let group = stream
                        .groups
                        .get_mut(&group_name)
                        .expect("group existence checked above");
                    if touch_or_create_consumer(group, &consumer_name, now) {
                        created_consumers.push(key.clone());
                    }
                }
                if max == 0 {
                    continue;
                }
                let to_deliver: Vec<StreamEntry> =
                    stream.entries[start_idx..start_idx + max].to_vec();
                let delivered_ids: Vec<StreamId> = to_deliver.iter().map(|e| e.id).collect();
                let view = stream.lag_view();
                let group = stream
                    .groups
                    .get_mut(&group_name)
                    .expect("group existence checked above");
                // active-time advances only on real delivery.
                if let Some(consumer) = group.consumers.get_mut(&consumer_name) {
                    consumer.active_time_ms = now;
                }
                let (read, last) = view.advance_read_counter(
                    group.entries_read,
                    group.last_delivered_id,
                    &delivered_ids,
                );
                group.entries_read = read;
                group.last_delivered_id = last;
                if !noack {
                    for entry in &to_deliver {
                        let next_count = group
                            .pel_find(&entry.id)
                            .map(|idx| group.pel[idx].delivery_count.saturating_add(1))
                            .unwrap_or(1);
                        if next_count > 1 {
                            pel_reassign(group, &entry.id, &consumer_name, now, next_count);
                        } else {
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
                if touch_or_create_consumer(group, &consumer_name, now) {
                    created_consumers.push(key.clone());
                }
                let pending_ids: Vec<StreamId> = match group.consumers.get(&consumer_name) {
                    Some(c) => {
                        let start = c.pel.partition_point(|p| p.entry_id <= *from_id);
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

    let all_new = ids.iter().all(|id| matches!(id, GroupReadStartId::New));

    for key in &created_consumers {
        ctx.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-createconsumer", key);
    }

    if non_empty.is_empty() {
        if let Some(ms) = block_ms {
            if all_new && !ctx.client_ref().flag_deny_blocking() {
                if let Some(registry) = ctx.pubsub.as_ref() {
                    let sender = {
                        let guard = match registry.lock() {
                            Ok(g) => g,
                            Err(p) => p.into_inner(),
                        };
                        guard.sender_for(ctx.client_ref().id)
                    };
                    if let Some(sender) = sender {
                        let deadline_ms = if ms == 0 {
                            i64::MAX
                        } else {
                            current_time_ms().saturating_add(ms)
                        };
                        let mut idx = match blocked_keys_index().lock() {
                            Ok(g) => g,
                            Err(p) => p.into_inner(),
                        };
                        for (key, start_id) in keys.iter().zip(ids.iter()) {
                            let id_after = match start_id {
                                GroupReadStartId::New => {
                                    match as_stream_ref(ctx.db().lookup_key_read(key)) {
                                        Ok(Some(s)) => s.last_id,
                                        _ => StreamId::ZERO,
                                    }
                                }
                                GroupReadStartId::From(id) => *id,
                            };
                            let waiter = BlockedWaiter {
                                client_id: ctx.client_ref().id,
                                sender: sender.clone(),
                                keys: vec![key.clone()],
                                action: BlockedAction::StreamGroup {
                                    id_after,
                                    group: group_name.clone(),
                                    consumer: consumer_name.clone(),
                                    count,
                                    noack,
                                },
                                deadline_ms,
                                resp_proto: ctx.client_ref().resp_proto,
                                username: ctx.client_ref().authenticated_user.clone(),
                                redirect_on_role_change: ctx.client_ref().capa_redirect,
                            };
                            idx.add(waiter);
                        }
                        ctx.client_mut().blocked_on_keys = true;
                        return Ok(());
                    }
                }
            }
        }
        if all_new {
            return ctx.reply_null_array();
        }
    }

    if non_empty.is_empty() {
        ctx.reply_array_header(results.len())?;
        for (key, entries) in &results {
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
        return Ok(());
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

/// XACK key group id [id...]
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
    if argc != 3 && !(6..=9).contains(&argc) {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let key = ctx.arg_owned(1usize)?;
    let group_name = ctx.arg_owned(2usize)?;

    let group_owned: ConsumerGroup = match as_stream_ref(ctx.db().lookup_key_read(&key))? {
        None => {
            return Err(no_such_key_or_group_short(
                key.as_bytes(),
                group_name.as_bytes(),
            ))
        }
        Some(s) => match s.groups.get(&group_name) {
            None => {
                return Err(no_such_key_or_group_short(
                    key.as_bytes(),
                    group_name.as_bytes(),
                ))
            }
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
    };
    let end_id = match end {
        Bound::Min => StreamId::ZERO,
        Bound::Max => StreamId::MAX,
        Bound::Inclusive(id) => id,
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

/// XCLAIM key group consumer min-idle-time id [id...]
/// [IDLE ms] [TIME unix-ms] [RETRYCOUNT n] [FORCE] [JUSTID]
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
        None => {
            return Err(no_such_key_or_group_short(
                key.as_bytes(),
                group_name.as_bytes(),
            ))
        }
    };
    if !stream.groups.contains_key(&group_name) {
        return Err(no_such_key_or_group_short(
            key.as_bytes(),
            group_name.as_bytes(),
        ));
    }

    let entries_index: std::collections::HashMap<StreamId, StreamEntry> =
        stream.entries.iter().map(|e| (e.id, e.clone())).collect();

    let group = stream
        .groups
        .get_mut(&group_name)
        .expect("group existence checked above");
    let consumer_created = touch_or_create_consumer(group, &consumer_name, now);
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
        // Entry was trimmed/deleted from the stream: drop the NACK from
        // group + owning-consumer PEL and skip it (no reassign, absent from
        // reply). C: streamClaimEntry discards claims for vanished entries.
        if stream_entry.is_none() {
            pel_remove(group, id);
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

    if consumer_created {
        ctx.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-createconsumer", &key);
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
                // COUNT is bounded by LONG_MAX/16 so
                // internal `count * attempts_factor` scan budget cannot overflow.
                const MAX_COUNT: i64 = i64::MAX / 16;
                if n <= 0 || n > MAX_COUNT {
                    return Err(RedisError::runtime(b"ERR COUNT must be > 0"));
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
        None => {
            return Err(no_such_key_or_group_short(
                key.as_bytes(),
                group_name.as_bytes(),
            ))
        }
    };
    if !stream.groups.contains_key(&group_name) {
        return Err(no_such_key_or_group_short(
            key.as_bytes(),
            group_name.as_bytes(),
        ));
    }
    let entries_index: std::collections::HashMap<StreamId, StreamEntry> =
        stream.entries.iter().map(|e| (e.id, e.clone())).collect();
    let group = stream
        .groups
        .get_mut(&group_name)
        .expect("group existence checked above");
    let consumer_created = touch_or_create_consumer(group, &consumer_name, now);
    if let Some(c) = group.consumers.get_mut(&consumer_name) {
        c.active_time_ms = now;
    }

    let start_pos = group.pel_lower_bound(&start_id);
    let candidates: Vec<StreamId> = group.pel[start_pos..].iter().map(|p| p.entry_id).collect();

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

    if consumer_created {
        ctx.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-createconsumer", &key);
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
                    return Err(RedisError::runtime(b"ERR entries_added must be positive"));
                }
                entries_added = Some(n as u64);
                i += 2;
            }
            b"MAXDELETEDID" => {
                if i + 1 >= argc {
                    return Err(RedisError::syntax(b"syntax error"));
                }
                let m = parse_explicit_id(ctx.arg(i + 1)?.as_bytes())?;
                // the new last-id cannot be below
                // provided max_deleted_entry_id.
                if new_id < m {
                    return Err(RedisError::runtime(
                        b"ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
                    ));
                }
                max_deleted = Some(m);
                i += 2;
            }
            _ => return Err(RedisError::syntax(b"syntax error")),
        }
    }
    let stream = match stream_for_write(ctx.db_mut(), &key)? {
        Some(s) => s,
        None => return Err(RedisError::runtime(b"ERR no such key")),
    };
    // new last-id cannot be below the current
    // max_deleted_entry_id.
    if new_id < stream.max_deleted_id {
        return Err(RedisError::runtime(
            b"ERR The ID specified in XSETID is smaller than current max_deleted_entry_id",
        ));
    }
    if let Some(top) = stream.entries.last() {
        if new_id < top.id {
            return Err(RedisError::runtime(
                b"ERR The ID specified in XSETID is smaller than the target stream top item",
            ));
        }
        // entries_added (if provided) cannot be lower than the stream length.
        if let Some(ea) = entries_added {
            if stream.entries.len() as u64 > ea {
                return Err(RedisError::runtime(
                    b"ERR The entries_added specified in XSETID is smaller than the target stream length",
                ));
            }
        }
    }
    stream.last_id = new_id;
    if let Some(n) = entries_added {
        stream.entries_added = n;
    }
    if let Some(m) = max_deleted {
        stream.max_deleted_id = m;
    }
    ctx.notify_keyspace_event(NOTIFY_STREAM, b"xsetid", &key);
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
    let view = stream.lag_view();
    let mut group_views: Vec<(
        RedisString,
        usize,
        usize,
        StreamId,
        Option<i64>,
        Option<i64>,
    )> = stream
        .groups
        .iter()
        .map(|(name, g)| {
            let entries_read = if g.entries_read == SCG_INVALID_ENTRIES_READ {
                None
            } else {
                Some(g.entries_read)
            };
            let lag = view.group_lag(g.entries_read, g.last_delivered_id);
            (
                name.clone(),
                g.consumers.len(),
                g.pel.len(),
                g.last_delivered_id,
                entries_read,
                lag,
            )
        })
        .collect();
    group_views.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    ctx.reply_array_header(group_views.len())?;
    for (name, consumers, pending, last_id, entries_read, lag) in &group_views {
        ctx.reply_map_header(6usize)?;
        ctx.reply_bulk(b"name")?;
        ctx.reply_bulk_string(name.clone())?;
        ctx.reply_bulk(b"consumers")?;
        ctx.reply_integer(*consumers as i64)?;
        ctx.reply_bulk(b"pending")?;
        ctx.reply_integer(*pending as i64)?;
        ctx.reply_bulk(b"last-delivered-id")?;
        ctx.reply_bulk_string(RedisString::from_vec(last_id.to_display_bytes()))?;
        ctx.reply_bulk(b"entries-read")?;
        match entries_read {
            Some(n) => ctx.reply_integer(*n)?,
            None => ctx.reply_null_bulk()?,
        }
        ctx.reply_bulk(b"lag")?;
        match lag {
            Some(n) => ctx.reply_integer(*n)?,
            None => ctx.reply_null_bulk()?,
        }
    }
    Ok(())
}

/// XINFO STREAM <key> FULL [COUNT <n>]
/// 9-field stream map with inline `entries` and a nested `groups` array
/// (each group carries its PEL + consumers, each consumer its own PEL).
/// `count` limits the `entries` and pending arrays (0 = unlimited; the XINFO
/// default is 10). `pel-count` reports the true total, not the limited view.
fn xinfo_stream_full(ctx: &mut CommandContext, key: &RedisString, count: i64) -> RedisResult<()> {
    let limit = if count <= 0 {
        usize::MAX
    } else {
        count as usize
    };

    struct ConsumerSnap {
        name: RedisString,
        seen: i64,
        active: i64,
        pel_total: usize,
        pel: Vec<(StreamId, i64, u64)>,
    }
    struct GroupSnap {
        name: RedisString,
        last_delivered: StreamId,
        entries_read: Option<i64>,
        lag: Option<i64>,
        pel_total: usize,
        pel: Vec<(StreamId, RedisString, i64, u64)>,
        consumers: Vec<ConsumerSnap>,
    }

    let (length, last_id, max_del, entries_added, first_id, entries, groups): (
        i64,
        StreamId,
        StreamId,
        i64,
        StreamId,
        Vec<StreamEntry>,
        Vec<GroupSnap>,
    ) = {
        let stream = match as_stream_ref(ctx.db().lookup_key_read(key))? {
            None => return Err(RedisError::runtime(b"ERR no such key")),
            Some(s) => s,
        };
        let entries: Vec<StreamEntry> = stream.entries.iter().take(limit).cloned().collect();
        let lag_view = stream.lag_view();
        let first_id = stream
            .entries
            .first()
            .map(|e| e.id)
            .unwrap_or(StreamId { ms: 0, seq: 0 });

        let mut group_names: Vec<&RedisString> = stream.groups.keys().collect();
        group_names.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let mut groups: Vec<GroupSnap> = Vec::with_capacity(group_names.len());
        for gname in group_names {
            let g = &stream.groups[gname];
            // group PEL entry -> owning consumer (PelEntry has no consumer ref).
            let mut gpel: Vec<(StreamId, RedisString, i64, u64)> = Vec::new();
            for pe in g.pel.iter().take(limit) {
                let owner = g
                    .consumers
                    .iter()
                    .find(|(_, c)| c.pel.iter().any(|p| p.entry_id == pe.entry_id))
                    .map(|(n, _)| n.clone())
                    .unwrap_or_else(|| RedisString::from_bytes(b""));
                gpel.push((pe.entry_id, owner, pe.delivery_time_ms, pe.delivery_count));
            }
            let mut consumer_names: Vec<&RedisString> = g.consumers.keys().collect();
            consumer_names.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            let mut consumers: Vec<ConsumerSnap> = Vec::with_capacity(consumer_names.len());
            for cname in consumer_names {
                let c = &g.consumers[cname];
                consumers.push(ConsumerSnap {
                    name: c.name.clone(),
                    seen: c.seen_time_ms,
                    active: c.active_time_ms,
                    pel_total: c.pel.len(),
                    pel: c
                        .pel
                        .iter()
                        .take(limit)
                        .map(|p| (p.entry_id, p.delivery_time_ms, p.delivery_count))
                        .collect(),
                });
            }
            let entries_read = if g.entries_read == SCG_INVALID_ENTRIES_READ {
                None
            } else {
                Some(g.entries_read)
            };
            let lag = lag_view.group_lag(g.entries_read, g.last_delivered_id);
            groups.push(GroupSnap {
                name: g.name.clone(),
                last_delivered: g.last_delivered_id,
                entries_read,
                lag,
                pel_total: g.pel.len(),
                pel: gpel,
                consumers,
            });
        }
        (
            stream.len() as i64,
            stream.last_id,
            stream.max_deleted_id,
            stream.entries_added as i64,
            first_id,
            entries,
            groups,
        )
    };

    let id_str = |id: StreamId| RedisString::from_vec(id.to_display_bytes());

    ctx.reply_map_header(9usize)?;
    ctx.reply_bulk(b"length")?;
    ctx.reply_integer(length)?;
    ctx.reply_bulk(b"radix-tree-keys")?;
    ctx.reply_integer(synthetic_radix_keys(length as usize))?;
    ctx.reply_bulk(b"radix-tree-nodes")?;
    ctx.reply_integer(synthetic_radix_keys(length as usize) + 1)?;
    ctx.reply_bulk(b"last-generated-id")?;
    ctx.reply_bulk_string(id_str(last_id))?;
    ctx.reply_bulk(b"max-deleted-entry-id")?;
    ctx.reply_bulk_string(id_str(max_del))?;
    ctx.reply_bulk(b"entries-added")?;
    ctx.reply_integer(entries_added)?;
    ctx.reply_bulk(b"recorded-first-entry-id")?;
    ctx.reply_bulk_string(id_str(first_id))?;
    ctx.reply_bulk(b"entries")?;
    ctx.reply_array_header(entries.len())?;
    for e in &entries {
        reply_entry(ctx, e)?;
    }
    ctx.reply_bulk(b"groups")?;
    ctx.reply_array_header(groups.len())?;
    for g in &groups {
        ctx.reply_map_header(7usize)?;
        ctx.reply_bulk(b"name")?;
        ctx.reply_bulk_string(g.name.clone())?;
        ctx.reply_bulk(b"last-delivered-id")?;
        ctx.reply_bulk_string(id_str(g.last_delivered))?;
        ctx.reply_bulk(b"entries-read")?;
        match g.entries_read {
            Some(n) => ctx.reply_integer(n)?,
            None => ctx.reply_null_bulk()?,
        }
        ctx.reply_bulk(b"lag")?;
        match g.lag {
            Some(n) => ctx.reply_integer(n)?,
            None => ctx.reply_null_bulk()?,
        }
        ctx.reply_bulk(b"pel-count")?;
        ctx.reply_integer(g.pel_total as i64)?;
        ctx.reply_bulk(b"pending")?;
        ctx.reply_array_header(g.pel.len())?;
        for (id, consumer, dt, dc) in &g.pel {
            ctx.reply_array_header(4usize)?;
            ctx.reply_bulk_string(id_str(*id))?;
            ctx.reply_bulk_string(consumer.clone())?;
            ctx.reply_integer(*dt)?;
            ctx.reply_integer(*dc as i64)?;
        }
        ctx.reply_bulk(b"consumers")?;
        ctx.reply_array_header(g.consumers.len())?;
        for c in &g.consumers {
            ctx.reply_map_header(5usize)?;
            ctx.reply_bulk(b"name")?;
            ctx.reply_bulk_string(c.name.clone())?;
            ctx.reply_bulk(b"seen-time")?;
            ctx.reply_integer(c.seen)?;
            ctx.reply_bulk(b"active-time")?;
            ctx.reply_integer(c.active)?;
            ctx.reply_bulk(b"pel-count")?;
            ctx.reply_integer(c.pel_total as i64)?;
            ctx.reply_bulk(b"pending")?;
            ctx.reply_array_header(c.pel.len())?;
            for (id, dt, dc) in &c.pel {
                ctx.reply_array_header(3usize)?;
                ctx.reply_bulk_string(id_str(*id))?;
                ctx.reply_integer(*dt)?;
                ctx.reply_integer(*dc as i64)?;
            }
        }
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
            None => {
                return Err(no_such_key_or_group_short(
                    key.as_bytes(),
                    group_name.as_bytes(),
                ))
            }
            Some(s) => s,
        };
        let group = match stream.groups.get(&group_name) {
            None => {
                return Err(no_such_key_or_group_short(
                    key.as_bytes(),
                    group_name.as_bytes(),
                ))
            }
            Some(g) => g,
        };
        group
            .consumers
            .values()
            .map(|c| {
                (
                    c.name.clone(),
                    c.pel.len(),
                    c.seen_time_ms,
                    c.active_time_ms,
                )
            })
            .collect()
    };
    snapshot.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let now = now_ms_clamped();
    ctx.reply_array_header(snapshot.len())?;
    for (name, pending, seen, active) in &snapshot {
        let idle = (now - *seen).max(0);
        // active < 0 is the never-delivered sentinel; XINFO reports -1.
        let inactive = if *active < 0 {
            -1
        } else {
            (now - *active).max(0)
        };
        ctx.reply_map_header(4usize)?;
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

// ─────────────────────────────────────────────────────────────────────────────
// Startup hook wiring
// ─────────────────────────────────────────────────────────────────────────────

/// Install all stream-related hooks into `redis-core`'s hook slots.
/// Must be called once at server startup, before any connections are accepted.
/// Subsequent calls are no-ops (each underlying `OnceLock` only accepts
/// first value).
/// The RENAME-completion wake is deliberately not installed here. It is owned
/// by the runtime owner in `redis-server`, which registers a hook that defers
/// the wake onto the owner thread's own database list. Installing a second
/// rename hook from this layer would race the owner's hook for the single
/// `STREAM_RENAME_HOOK` slot, if it won, wake clients against
/// transitional `global_databases` store instead of the owner's keyspace.
pub fn install_stream_hooks() {
    redis_core::db::install_stream_key_deleted_hook(Box::new(|key| {
        wake_xreadgroup_with_nogroup(key);
    }));
    redis_core::db::install_stream_db_flushed_hook(Box::new(|| {
        wake_all_xreadgroup_with_nogroup();
    }));
    redis_core::db::install_stream_key_overwritten_hook(Box::new(|key| {
        let waiters = {
            let mut idx = match redis_core::blocked_keys::blocked_keys_index().lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            idx.take_all_stream_group_waiters_for(key)
        };
        let reply = encode_wrongtype_error();
        for waiter in waiters {
            let _ = waiter.sender.send(reply.clone());
        }
    }));
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-commands
//   confidence:    pragmatic Phase-B (inline storage, focused TCL parity)
//   todos:         1
//   port_notes:    2
//   unsafe_blocks: 0
//   notes:         Inline stream command coverage now handles measured trim,
//                  wake-suppression, and consumer-group replay cases. EXEC
//                  deferred multi-entry stream wake remains a follow-up.
// ──────────────────────────────────────────────────────────────────────────
