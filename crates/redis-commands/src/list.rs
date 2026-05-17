//! List type and command implementations.
//!
//! Covers the byte-exact wire surface of LPUSH, RPUSH, LPUSHX, RPUSHX,
//! LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LREM, LTRIM, LINSERT, LMOVE,
//! and RPOPLPUSH for Round 2.
//!
//! C source: `reference/valkey/src/t_list.c`
//!
//! # Storage shape
//!
//! Round 2 uses the pragmatic `ObjectKind::List(ListEncoding::Inline(_))`
//! encoding from `redis-core::object` — a `VecDeque<RedisString>` providing
//! O(1) push/pop on both ends. The real `ListPack` / `QuickList` encodings
//! land in Phase 4 when `redis-ds` exposes those types.
//!
//! # Architect items
//!
//! TODO(architect): blocking variants (BLPOP, BRPOP, BLMOVE, BRPOPLPUSH,
//! BLMPOP) need the `blockForKeys` infrastructure from
//! `redis-core/src/blocked.rs`, which is not yet wired into the dispatch.
//!
//! TODO(architect): keyspace-event notifications, replication command
//! rewriting, and the deferred-array-length protocol are all stubbed —
//! they have no observable wire effect for the in-tree smoke tests but
//! must land before the AOF / replication phases.
//!
//! TODO(architect): swap the `Inline` encoding for real `ListPack` /
//! `QuickList` types from `redis-ds` once Phase 4 makes them usable.

use std::collections::VecDeque;

use redis_core::command_context::CommandContext;
use redis_core::object::RedisObject;
use redis_types::{RedisError, RedisResult, RedisString};

/// Which end of the list to operate on.
///
/// Matches C's `LIST_HEAD` / `LIST_TAIL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListPosition {
    Head,
    Tail,
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

/// Parse "LEFT" or "RIGHT" from a command argument (case-insensitive).
///
/// Mirrors C's `getListPositionFromObjectOrReply`.
fn parse_list_position(arg: &[u8]) -> Result<ListPosition, RedisError> {
    if arg.eq_ignore_ascii_case(b"left") {
        Ok(ListPosition::Head)
    } else if arg.eq_ignore_ascii_case(b"right") {
        Ok(ListPosition::Tail)
    } else {
        Err(RedisError::syntax(b"syntax error"))
    }
}

/// Borrow the inner `VecDeque` of a list-encoded `RedisObject`, raising
/// `WRONGTYPE` if `obj` is any other kind.
///
/// Returns `Ok(None)` if the key is absent so callers can preserve
/// existence semantics without nesting `match` on the lookup result.
fn as_list_ref(obj: Option<&RedisObject>) -> Result<Option<&VecDeque<RedisString>>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => o.list().map(Some).ok_or_else(RedisError::wrong_type),
    }
}

/// Mutable variant of `as_list_ref`.
fn as_list_mut(
    obj: Option<&mut RedisObject>,
) -> Result<Option<&mut VecDeque<RedisString>>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => {
            if o.is_list() {
                Ok(o.list_mut())
            } else {
                Err(RedisError::wrong_type())
            }
        }
    }
}

/// Normalise a signed list index into `[0, len]` for read access.
///
/// Negative indexes count from the tail. Returns `None` when the
/// resulting index is out of range, matching `LINDEX` / `LSET` semantics.
fn resolve_read_index(index: i64, len: usize) -> Option<usize> {
    let len_i = len as i64;
    let resolved = if index < 0 { index + len_i } else { index };
    if resolved < 0 || resolved >= len_i {
        return None;
    }
    Some(resolved as usize)
}

/// Resolve `start` / `stop` for range commands (`LRANGE`, `LTRIM`).
///
/// Returns `None` when the requested range is empty. Otherwise returns
/// `Some((start, stop))` where both are clamped, non-negative, and
/// `start <= stop < len`.
fn resolve_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len_i = len as i64;
    let mut s = if start < 0 { start + len_i } else { start };
    let mut e = if stop < 0 { stop + len_i } else { stop };
    if s < 0 {
        s = 0;
    }
    if s > e || s >= len_i {
        return None;
    }
    if e >= len_i {
        e = len_i - 1;
    }
    Some((s as usize, e as usize))
}

/// Implementation shared by LPUSH / RPUSH / LPUSHX / RPUSHX.
///
/// When `xx` is true (LPUSHX / RPUSHX), a missing key short-circuits to
/// `:0\r\n` without creating one. Otherwise the key is auto-created with
/// the pragmatic Inline encoding before pushing.
fn push_generic(
    ctx: &mut CommandContext,
    position: ListPosition,
    xx: bool,
) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg_owned(1usize)?;
    let mut values: Vec<RedisString> = Vec::with_capacity(argc - 2);
    for j in 2..argc {
        values.push(ctx.arg_owned(j)?);
    }
    let existing = ctx.db_mut().lookup_key_write(&key);
    match existing {
        None => {
            if xx {
                return ctx.reply_integer(0);
            }
            let mut obj = RedisObject::new_list();
            {
                let deque = obj
                    .list_mut()
                    .expect("new_list constructs an Inline list");
                for v in values {
                    match position {
                        ListPosition::Head => deque.push_front(v),
                        ListPosition::Tail => deque.push_back(v),
                    }
                }
            }
            let new_len = obj.list().map(|d| d.len()).unwrap_or(0) as i64;
            ctx.db_mut().set_key(key, obj, 0);
            ctx.reply_integer(new_len)
        }
        Some(obj) => {
            if !obj.is_list() {
                return Err(RedisError::wrong_type());
            }
            let deque = obj
                .list_mut()
                .expect("is_list confirms list encoding");
            for v in values {
                match position {
                    ListPosition::Head => deque.push_front(v),
                    ListPosition::Tail => deque.push_back(v),
                }
            }
            let new_len = deque.len() as i64;
            ctx.reply_integer(new_len)
        }
    }
}

/// Implementation shared by LPOP / RPOP.
///
/// Without a count argument: replies a single bulk or `$-1\r\n` for an
/// absent key. With a count: replies an array of up to `count` elements
/// in pop order, or `$-1\r\n` (null array) if the key is absent. Deletes
/// the key when the last element is removed.
fn pop_generic(ctx: &mut CommandContext, position: ListPosition) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 || argc > 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg_owned(1usize)?;
    let count_raw = if argc == 3 {
        Some(ctx.arg_owned(2usize)?)
    } else {
        None
    };
    let count: Option<i64> = match count_raw {
        None => None,
        Some(raw) => {
            let n = parse_strict_i64(raw.as_bytes())?;
            if n < 0 {
                return Err(RedisError::runtime(
                    b"ERR value is out of range, must be positive",
                ));
            }
            Some(n)
        }
    };

    let popped: Option<Vec<RedisString>> = {
        let obj = match ctx.db_mut().lookup_key_write(&key) {
            None => None,
            Some(o) => {
                if !o.is_list() {
                    return Err(RedisError::wrong_type());
                }
                Some(o)
            }
        };
        match obj {
            None => None,
            Some(o) => {
                let deque = o
                    .list_mut()
                    .expect("is_list confirms list encoding");
                let take = match count {
                    None => 1,
                    Some(n) => (n as usize).min(deque.len()),
                };
                let mut out = Vec::with_capacity(take);
                for _ in 0..take {
                    let next = match position {
                        ListPosition::Head => deque.pop_front(),
                        ListPosition::Tail => deque.pop_back(),
                    };
                    match next {
                        Some(v) => out.push(v),
                        None => break,
                    }
                }
                Some(out)
            }
        }
    };

    let empty_after = matches!(
        ctx.db().lookup_key_read(&key),
        Some(o) if o.list().map(|d| d.is_empty()).unwrap_or(false)
    );
    if empty_after {
        ctx.db_mut().sync_delete(&key);
    }

    match (count, popped) {
        (None, None) => ctx.reply_null_bulk(),
        (None, Some(mut v)) => {
            if let Some(first) = v.pop() {
                ctx.reply_bulk_string(first)
            } else {
                ctx.reply_null_bulk()
            }
        }
        (Some(_), None) => ctx.reply_null_array(),
        (Some(_), Some(v)) => {
            ctx.reply_array_header(v.len())?;
            for elem in v {
                ctx.reply_bulk_string(elem)?;
            }
            Ok(())
        }
    }
}

/// LPUSH key value [value ...]
pub fn lpush_command(ctx: &mut CommandContext) -> RedisResult<()> {
    push_generic(ctx, ListPosition::Head, false)
}

/// RPUSH key value [value ...]
pub fn rpush_command(ctx: &mut CommandContext) -> RedisResult<()> {
    push_generic(ctx, ListPosition::Tail, false)
}

/// LPUSHX key value [value ...]
pub fn lpushx_command(ctx: &mut CommandContext) -> RedisResult<()> {
    push_generic(ctx, ListPosition::Head, true)
}

/// RPUSHX key value [value ...]
pub fn rpushx_command(ctx: &mut CommandContext) -> RedisResult<()> {
    push_generic(ctx, ListPosition::Tail, true)
}

/// LPOP key [count]
pub fn lpop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    pop_generic(ctx, ListPosition::Head)
}

/// RPOP key [count]
pub fn rpop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    pop_generic(ctx, ListPosition::Tail)
}

/// LLEN key
pub fn llen_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"llen"));
    }
    let key = ctx.arg_owned(1usize)?;
    let len = match as_list_ref(ctx.db().lookup_key_read(&key))? {
        None => 0,
        Some(d) => d.len() as i64,
    };
    ctx.reply_integer(len)
}

/// LRANGE key start stop
pub fn lrange_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"lrange"));
    }
    let key = ctx.arg_owned(1usize)?;
    let start = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
    let stop = parse_strict_i64(ctx.arg(3)?.as_bytes())?;
    let collected: Option<Vec<RedisString>> = match as_list_ref(ctx.db().lookup_key_read(&key))? {
        None => None,
        Some(d) => match resolve_range(start, stop, d.len()) {
            None => Some(Vec::new()),
            Some((s, e)) => {
                let mut out = Vec::with_capacity(e - s + 1);
                for i in s..=e {
                    if let Some(item) = d.get(i) {
                        out.push(item.clone());
                    }
                }
                Some(out)
            }
        },
    };
    let items = collected.unwrap_or_default();
    ctx.reply_array_header(items.len())?;
    for item in items {
        ctx.reply_bulk_string(item)?;
    }
    Ok(())
}

/// LINDEX key index
pub fn lindex_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"lindex"));
    }
    let key = ctx.arg_owned(1usize)?;
    let index = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
    let value: Option<RedisString> = match as_list_ref(ctx.db().lookup_key_read(&key))? {
        None => None,
        Some(d) => resolve_read_index(index, d.len()).and_then(|i| d.get(i).cloned()),
    };
    match value {
        None => ctx.reply_null_bulk(),
        Some(v) => ctx.reply_bulk_string(v),
    }
}

/// LSET key index value
pub fn lset_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"lset"));
    }
    let key = ctx.arg_owned(1usize)?;
    let index = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
    let value = ctx.arg_owned(3usize)?;
    let deque = match as_list_mut(ctx.db_mut().lookup_key_write(&key))? {
        None => return Err(RedisError::runtime(b"ERR no such key")),
        Some(d) => d,
    };
    let resolved = resolve_read_index(index, deque.len());
    match resolved {
        None => Err(RedisError::runtime(b"ERR index out of range")),
        Some(i) => {
            if let Some(slot) = deque.get_mut(i) {
                *slot = value;
            }
            ctx.reply_simple_string(b"OK")
        }
    }
}

/// LREM key count element
///
/// Positive `count` scans from the head, negative scans from the tail,
/// zero removes every match. Deletes the key when the list ends empty.
pub fn lrem_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"lrem"));
    }
    let key = ctx.arg_owned(1usize)?;
    let count = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
    let element = ctx.arg_owned(3usize)?;
    let removed = {
        let deque = match as_list_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return ctx.reply_integer(0),
            Some(d) => d,
        };
        let limit = count.unsigned_abs() as usize;
        let target = element.as_bytes();
        let mut removed: i64 = 0;
        if count >= 0 {
            let mut i = 0usize;
            while i < deque.len() {
                if deque[i].as_bytes() == target {
                    deque.remove(i);
                    removed += 1;
                    if count > 0 && removed as usize >= limit {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else {
            let mut i = deque.len();
            while i > 0 {
                i -= 1;
                if deque[i].as_bytes() == target {
                    deque.remove(i);
                    removed += 1;
                    if removed as usize >= limit {
                        break;
                    }
                }
            }
        }
        removed
    };
    let empty_after = matches!(
        ctx.db().lookup_key_read(&key),
        Some(o) if o.list().map(|d| d.is_empty()).unwrap_or(false)
    );
    if empty_after {
        ctx.db_mut().sync_delete(&key);
    }
    ctx.reply_integer(removed)
}

/// LTRIM key start stop
///
/// Trims the list to the inclusive range `[start, stop]`. Deletes the
/// key when the resulting list is empty.
pub fn ltrim_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"ltrim"));
    }
    let key = ctx.arg_owned(1usize)?;
    let start = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
    let stop = parse_strict_i64(ctx.arg(3)?.as_bytes())?;
    let key_empty = {
        let deque = match as_list_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return ctx.reply_simple_string(b"OK"),
            Some(d) => d,
        };
        let len = deque.len();
        match resolve_range(start, stop, len) {
            None => {
                deque.clear();
                true
            }
            Some((s, e)) => {
                for _ in 0..s {
                    deque.pop_front();
                }
                let new_len = e - s + 1;
                while deque.len() > new_len {
                    deque.pop_back();
                }
                deque.is_empty()
            }
        }
    };
    if key_empty {
        ctx.db_mut().sync_delete(&key);
    }
    ctx.reply_simple_string(b"OK")
}

/// LINSERT key BEFORE|AFTER pivot element
///
/// Returns the new list length on success, `:0\r\n` when the key is
/// missing, and `:-1\r\n` when the pivot is not found.
pub fn linsert_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 5 {
        return Err(RedisError::wrong_number_of_args(b"linsert"));
    }
    let key = ctx.arg_owned(1usize)?;
    let direction = ctx.arg_owned(2usize)?;
    let after = if direction.as_bytes().eq_ignore_ascii_case(b"after") {
        true
    } else if direction.as_bytes().eq_ignore_ascii_case(b"before") {
        false
    } else {
        return Err(RedisError::syntax(b"syntax error"));
    };
    let pivot = ctx.arg_owned(3usize)?;
    let value = ctx.arg_owned(4usize)?;
    let outcome: i64 = {
        let deque = match as_list_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return ctx.reply_integer(0),
            Some(d) => d,
        };
        let pivot_bytes = pivot.as_bytes();
        let mut found: Option<usize> = None;
        for (i, item) in deque.iter().enumerate() {
            if item.as_bytes() == pivot_bytes {
                found = Some(i);
                break;
            }
        }
        match found {
            None => -1,
            Some(i) => {
                let insert_at = if after { i + 1 } else { i };
                deque.insert(insert_at, value);
                deque.len() as i64
            }
        }
    };
    ctx.reply_integer(outcome)
}

/// Shared body of LMOVE / RPOPLPUSH.
///
/// Atomically pops from `src` and pushes onto `dst`. The pop side enforces
/// `WRONGTYPE`; the push side does the same when `dst` exists.
fn lmove_generic(
    ctx: &mut CommandContext,
    src_key: RedisString,
    dst_key: RedisString,
    wherefrom: ListPosition,
    whereto: ListPosition,
) -> RedisResult<()> {
    if let Some(dst_obj) = ctx.db().lookup_key_read(&dst_key) {
        if !dst_obj.is_list() {
            return Err(RedisError::wrong_type());
        }
    }
    let popped = {
        let deque = match as_list_mut(ctx.db_mut().lookup_key_write(&src_key))? {
            None => None,
            Some(d) => match wherefrom {
                ListPosition::Head => d.pop_front(),
                ListPosition::Tail => d.pop_back(),
            },
        };
        deque
    };
    let value = match popped {
        None => return ctx.reply_null_bulk(),
        Some(v) => v,
    };

    let src_empty = matches!(
        ctx.db().lookup_key_read(&src_key),
        Some(o) if o.list().map(|d| d.is_empty()).unwrap_or(false)
    );
    if src_empty {
        ctx.db_mut().sync_delete(&src_key);
    }

    match ctx.db_mut().lookup_key_write(&dst_key) {
        Some(obj) => {
            let deque = obj
                .list_mut()
                .expect("WRONGTYPE pre-check confirmed list encoding");
            match whereto {
                ListPosition::Head => deque.push_front(value.clone()),
                ListPosition::Tail => deque.push_back(value.clone()),
            }
        }
        None => {
            let mut obj = RedisObject::new_list();
            {
                let deque = obj
                    .list_mut()
                    .expect("new_list constructs an Inline list");
                match whereto {
                    ListPosition::Head => deque.push_front(value.clone()),
                    ListPosition::Tail => deque.push_back(value.clone()),
                }
            }
            ctx.db_mut().set_key(dst_key, obj, 0);
        }
    }
    ctx.reply_bulk_string(value)
}

/// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
pub fn lmove_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 5 {
        return Err(RedisError::wrong_number_of_args(b"lmove"));
    }
    let src_key = ctx.arg_owned(1usize)?;
    let dst_key = ctx.arg_owned(2usize)?;
    let wherefrom = parse_list_position(ctx.arg(3)?.as_bytes())?;
    let whereto = parse_list_position(ctx.arg(4)?.as_bytes())?;
    lmove_generic(ctx, src_key, dst_key, wherefrom, whereto)
}

/// RPOPLPUSH source destination — deprecated alias for `LMOVE src dst RIGHT LEFT`.
pub fn rpoplpush_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"rpoplpush"));
    }
    let src_key = ctx.arg_owned(1usize)?;
    let dst_key = ctx.arg_owned(2usize)?;
    lmove_generic(ctx, src_key, dst_key, ListPosition::Tail, ListPosition::Head)
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/t_list.c
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         3
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Round 2 byte-exact implementations for LPUSH, RPUSH,
//                  LPUSHX, RPUSHX, LPOP, RPOP, LLEN, LRANGE, LINDEX,
//                  LSET, LREM, LTRIM, LINSERT, LMOVE, RPOPLPUSH backed by
//                  the pragmatic ListEncoding::Inline encoding from
//                  redis-core::object. Blocking variants (BLPOP, BRPOP,
//                  BLMOVE, BRPOPLPUSH, BLMPOP) and LPOS / LMPOP remain
//                  TODO until blocked-key infra and deferred-reply
//                  protocol land. Phase 4 will swap Inline for real
//                  ListPack / QuickList encodings from redis-ds.
// ──────────────────────────────────────────────────────────────────────────
