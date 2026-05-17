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

/// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
///
/// Pops one or more elements from the first non-empty list among `numkeys`
/// keys. Replies a two-element array of `[key, [popped elements]]`, or a
/// null array if every key is missing or empty.
///
/// C: `lmpopCommand` (t_list.c).
pub fn lmpop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(b"lmpop"));
    }
    let numkeys_signed = parse_strict_i64(ctx.arg(1)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR numkeys should be greater than 0"))?;
    if numkeys_signed <= 0 {
        return Err(RedisError::runtime(
            b"ERR numkeys should be greater than 0",
        ));
    }
    let numkeys = numkeys_signed as usize;
    if numkeys + 3 > argc {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let mut keys: Vec<RedisString> = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(ctx.arg_owned(2 + i)?);
    }
    let dir_arg = ctx.arg(2 + numkeys)?;
    let position = parse_list_position(dir_arg.as_bytes())?;
    let mut count: i64 = 1;
    let mut got_count = false;
    let mut j = 3 + numkeys;
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
            return Err(RedisError::runtime(
                b"ERR count should be greater than 0",
            ));
        }
        got_count = true;
        j += 2;
    }
    for key in &keys {
        let has_data = match ctx.db().find(key) {
            Some(o) if o.is_list() => o.list().map(|d| !d.is_empty()).unwrap_or(false),
            Some(_) => return Err(RedisError::wrong_type()),
            None => false,
        };
        if !has_data {
            continue;
        }
        let mut popped: Vec<RedisString> = Vec::with_capacity(count as usize);
        if let Some(obj) = ctx.db_mut().lookup_key_write(key) {
            let deque = obj
                .list_mut()
                .expect("is_list confirmed above");
            let take = (count as usize).min(deque.len());
            for _ in 0..take {
                let next = match position {
                    ListPosition::Head => deque.pop_front(),
                    ListPosition::Tail => deque.pop_back(),
                };
                match next {
                    Some(v) => popped.push(v),
                    None => break,
                }
            }
        }
        let empty_after = matches!(
            ctx.db().lookup_key_read(key),
            Some(o) if o.list().map(|d| d.is_empty()).unwrap_or(false)
        );
        if empty_after {
            ctx.db_mut().sync_delete(key);
        }
        ctx.reply_array_header(2)?;
        ctx.reply_bulk_string(key.clone())?;
        ctx.reply_array_header(popped.len())?;
        for v in popped {
            ctx.reply_bulk_string(v)?;
        }
        return Ok(());
    }
    ctx.reply_null_array()
}

/// LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
///
/// Returns the index (or indices) of `element` in the list at `key`. With
/// `RANK` negative, scans the list from tail to head. `COUNT 0` returns all
/// matches; positive `COUNT n` caps the result at `n`. `MAXLEN n` limits how
/// many list entries are examined (0 means unlimited).
///
/// Replies with `:integer` (single match), `*array` of indices (with COUNT),
/// or `$-1` / `*0` for a no-match case.
///
/// C: `lposCommand` (t_list.c).
pub fn lpos_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"lpos"));
    }
    let key = ctx.arg_owned(1usize)?;
    let element = ctx.arg_owned(2usize)?;
    let mut rank: i64 = 1;
    let mut count: Option<i64> = None;
    let mut maxlen: i64 = 0;
    let mut j = 3usize;
    while j < argc {
        let opt = ctx.arg(j)?;
        let ob = opt.as_bytes();
        if j + 1 >= argc {
            return Err(RedisError::syntax(b"syntax error"));
        }
        let val = ctx.arg(j + 1)?;
        let parsed = parse_strict_i64(val.as_bytes())?;
        if ob.eq_ignore_ascii_case(b"RANK") {
            if parsed == 0 {
                return Err(RedisError::runtime(
                    b"ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                ));
            }
            if parsed.checked_neg().is_none() {
                return Err(RedisError::runtime(
                    b"ERR value is out of range",
                ));
            }
            rank = parsed;
        } else if ob.eq_ignore_ascii_case(b"COUNT") {
            if parsed < 0 {
                return Err(RedisError::runtime(
                    b"ERR COUNT can't be negative",
                ));
            }
            count = Some(parsed);
        } else if ob.eq_ignore_ascii_case(b"MAXLEN") {
            if parsed < 0 {
                return Err(RedisError::runtime(
                    b"ERR MAXLEN can't be negative",
                ));
            }
            maxlen = parsed;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
        j += 2;
    }
    let list_opt = as_list_ref(ctx.db().lookup_key_read(&key))?;
    let list = match list_opt {
        None => {
            return match count {
                None => ctx.reply_null_bulk(),
                Some(_) => ctx.reply_array_header(0),
            };
        }
        Some(d) => d,
    };
    let len = list.len();
    let forward = rank > 0;
    let skip = rank.unsigned_abs() as usize - 1;
    let mut matches: Vec<i64> = Vec::new();
    let mut seen = 0usize;
    let mut scanned = 0usize;
    let want_all = matches!(count, Some(0));
    let want_one = count.is_none();
    let limit = count.map(|c| c as usize);
    if forward {
        for (idx, item) in list.iter().enumerate() {
            if maxlen != 0 && scanned >= maxlen as usize {
                break;
            }
            scanned += 1;
            if item.as_bytes() == element.as_bytes() {
                if seen >= skip {
                    matches.push(idx as i64);
                    if want_one {
                        break;
                    }
                    if let Some(c) = limit {
                        if !want_all && matches.len() >= c {
                            break;
                        }
                    }
                }
                seen += 1;
            }
        }
    } else {
        for (rev_idx, item) in list.iter().rev().enumerate() {
            let idx = len - 1 - rev_idx;
            if maxlen != 0 && scanned >= maxlen as usize {
                break;
            }
            scanned += 1;
            if item.as_bytes() == element.as_bytes() {
                if seen >= skip {
                    matches.push(idx as i64);
                    if want_one {
                        break;
                    }
                    if let Some(c) = limit {
                        if !want_all && matches.len() >= c {
                            break;
                        }
                    }
                }
                seen += 1;
            }
        }
    }
    if want_one {
        match matches.first() {
            None => ctx.reply_null_bulk(),
            Some(v) => ctx.reply_integer(*v),
        }
    } else {
        ctx.reply_array_header(matches.len())?;
        for v in matches {
            ctx.reply_integer(v)?;
        }
        Ok(())
    }
}

/// Parse a BLPOP-style timeout value (decimal seconds, non-negative).
///
/// Real Redis accepts both integer and floating-point timeouts. A negative
/// timeout is rejected with the canonical `ERR timeout is negative` error;
/// non-numeric values are rejected with `ERR timeout is not a float or out
/// of range`. The blocking stubs treat the parsed value as advisory only —
/// they never actually block — but the parse must still happen so callers
/// learn about invalid arguments.
fn parse_blocking_timeout(bytes: &[u8]) -> Result<f64, RedisError> {
    let s = core::str::from_utf8(bytes).map_err(|_| {
        RedisError::runtime(b"ERR timeout is not a float or out of range")
    })?;
    if s.starts_with(char::is_whitespace) || s.ends_with(char::is_whitespace) {
        return Err(RedisError::runtime(
            b"ERR timeout is not a float or out of range",
        ));
    }
    let parsed = if let Some(stripped) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        i64::from_str_radix(stripped, 16)
            .map(|v| v as f64)
            .map_err(|_| RedisError::runtime(b"ERR timeout is not a float or out of range"))?
    } else if let Some(stripped) = s.strip_prefix("-0x").or_else(|| s.strip_prefix("-0X")) {
        i64::from_str_radix(stripped, 16)
            .map(|v| -(v as f64))
            .map_err(|_| RedisError::runtime(b"ERR timeout is not a float or out of range"))?
    } else {
        s.parse::<f64>()
            .map_err(|_| RedisError::runtime(b"ERR timeout is not a float or out of range"))?
    };
    if !parsed.is_finite() {
        return Err(RedisError::runtime(
            b"ERR timeout is not a float or out of range",
        ));
    }
    if parsed < 0.0 {
        return Err(RedisError::runtime(b"ERR timeout is negative"));
    }
    let ms = parsed * 1000.0;
    if ms > i64::MAX as f64 || ms.is_nan() {
        return Err(RedisError::runtime(b"ERR timeout is out of range"));
    }
    Ok(parsed)
}

/// Shared implementation for BLPOP / BRPOP.
///
/// Args: `name key [key ...] timeout`. Validates the timeout, then pops one
/// element from the first non-empty list. Returns `[key, value]` on success
/// or a null array immediately when every key is empty — the blocking-wait
/// path is not yet wired up so the stub degrades gracefully into the same
/// shape that a timed-out blocking call would return.
fn bpop_generic(ctx: &mut CommandContext, position: ListPosition) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let timeout_raw = ctx.arg_owned(argc - 1)?;
    let _ = parse_blocking_timeout(timeout_raw.as_bytes())?;
    let mut keys: Vec<RedisString> = Vec::with_capacity(argc - 2);
    for j in 1..(argc - 1) {
        keys.push(ctx.arg_owned(j)?);
    }
    for key in &keys {
        let has_data = match ctx.db().find(key) {
            None => false,
            Some(o) => {
                if !o.is_list() {
                    return Err(RedisError::wrong_type());
                }
                o.list().map(|d| !d.is_empty()).unwrap_or(false)
            }
        };
        if !has_data {
            continue;
        }
        let popped = match ctx.db_mut().lookup_key_write(key) {
            None => None,
            Some(obj) => {
                let deque = obj.list_mut().expect("is_list confirmed above");
                match position {
                    ListPosition::Head => deque.pop_front(),
                    ListPosition::Tail => deque.pop_back(),
                }
            }
        };
        let value = match popped {
            None => continue,
            Some(v) => v,
        };
        let empty_after = matches!(
            ctx.db().lookup_key_read(key),
            Some(o) if o.list().map(|d| d.is_empty()).unwrap_or(false)
        );
        if empty_after {
            ctx.db_mut().sync_delete(key);
        }
        ctx.reply_array_header(2)?;
        ctx.reply_bulk_string(key.clone())?;
        return ctx.reply_bulk_string(value);
    }
    ctx.reply_null_array()
}

/// BLPOP key [key ...] timeout
///
/// Non-blocking stub: behaves like LPOP on the first non-empty key and
/// returns null-array immediately when every key is empty rather than
/// suspending the client. Real blocking requires the `blockForKeys`
/// scheduler in `redis-core::blocked` to be wired into the I/O loop.
pub fn blpop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    bpop_generic(ctx, ListPosition::Head)
}

/// BRPOP key [key ...] timeout — non-blocking stub mirroring `blpop_command`.
pub fn brpop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    bpop_generic(ctx, ListPosition::Tail)
}

/// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
///
/// Non-blocking stub: delegates to the LMOVE path. Returns a null bulk
/// immediately when the source list is empty or missing.
pub fn blmove_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 6 {
        return Err(RedisError::wrong_number_of_args(b"blmove"));
    }
    let src_key = ctx.arg_owned(1usize)?;
    let dst_key = ctx.arg_owned(2usize)?;
    let wherefrom = parse_list_position(ctx.arg(3)?.as_bytes())?;
    let whereto = parse_list_position(ctx.arg(4)?.as_bytes())?;
    let timeout_raw = ctx.arg_owned(5usize)?;
    let _ = parse_blocking_timeout(timeout_raw.as_bytes())?;
    lmove_generic(ctx, src_key, dst_key, wherefrom, whereto)
}

/// BRPOPLPUSH source destination timeout
///
/// Non-blocking stub: delegates to the RPOPLPUSH path.
pub fn brpoplpush_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"brpoplpush"));
    }
    let src_key = ctx.arg_owned(1usize)?;
    let dst_key = ctx.arg_owned(2usize)?;
    let timeout_raw = ctx.arg_owned(3usize)?;
    let _ = parse_blocking_timeout(timeout_raw.as_bytes())?;
    lmove_generic(ctx, src_key, dst_key, ListPosition::Tail, ListPosition::Head)
}

/// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
///
/// Non-blocking stub: validates args and delegates to LMPOP's pop loop.
pub fn blmpop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 5 {
        return Err(RedisError::wrong_number_of_args(b"blmpop"));
    }
    let timeout_raw = ctx.arg_owned(1usize)?;
    let _ = parse_blocking_timeout(timeout_raw.as_bytes())?;
    let numkeys_signed = parse_strict_i64(ctx.arg(2)?.as_bytes())
        .map_err(|_| RedisError::runtime(b"ERR numkeys should be greater than 0"))?;
    if numkeys_signed <= 0 {
        return Err(RedisError::runtime(
            b"ERR numkeys should be greater than 0",
        ));
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
    let position = parse_list_position(dir_arg.as_bytes())?;
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
        let has_data = match ctx.db().find(key) {
            Some(o) if o.is_list() => o.list().map(|d| !d.is_empty()).unwrap_or(false),
            Some(_) => return Err(RedisError::wrong_type()),
            None => false,
        };
        if !has_data {
            continue;
        }
        let mut popped: Vec<RedisString> = Vec::with_capacity(count as usize);
        if let Some(obj) = ctx.db_mut().lookup_key_write(key) {
            let deque = obj.list_mut().expect("is_list confirmed above");
            let take = (count as usize).min(deque.len());
            for _ in 0..take {
                let next = match position {
                    ListPosition::Head => deque.pop_front(),
                    ListPosition::Tail => deque.pop_back(),
                };
                match next {
                    Some(v) => popped.push(v),
                    None => break,
                }
            }
        }
        let empty_after = matches!(
            ctx.db().lookup_key_read(key),
            Some(o) if o.list().map(|d| d.is_empty()).unwrap_or(false)
        );
        if empty_after {
            ctx.db_mut().sync_delete(key);
        }
        ctx.reply_array_header(2)?;
        ctx.reply_bulk_string(key.clone())?;
        ctx.reply_array_header(popped.len())?;
        for v in popped {
            ctx.reply_bulk_string(v)?;
        }
        return Ok(());
    }
    ctx.reply_null_array()
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
//                  redis-core::object. Round 10 adds non-blocking stubs
//                  for BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BLMPOP that
//                  short-circuit to a null reply when the source list is
//                  empty rather than suspending the client; the real
//                  blockForKeys infrastructure remains TODO. Phase 4 will
//                  swap Inline for real ListPack / QuickList encodings
//                  from redis-ds.
// ──────────────────────────────────────────────────────────────────────────
