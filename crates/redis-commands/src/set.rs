//! Set type and command implementations.
//!
//! Covers the byte-exact wire surface of SADD, SREM, SMEMBERS, SISMEMBER,
//! SMISMEMBER, SCARD, SPOP, SRANDMEMBER, SMOVE, SINTER, SINTERSTORE,
//! SINTERCARD, SUNION, SUNIONSTORE, SDIFF, and SDIFFSTORE for Round 4.
//!
//! C source: `reference/valkey/src/t_set.c`
//!
//! # Storage shape
//!
//! Round 4 uses the pragmatic `ObjectKind::Set(SetEncoding::Inline(_))`
//! encoding from `redis-core::object` — a `HashSet<RedisString>` providing
//! O(1) membership, add, and remove. The real `ListPack` / `IntSet` /
//! `HashTable` encodings land in Phase 4 when `redis-ds` exposes those
//! types.
//!
//! # Architect items
//!
//! TODO(architect): SPOP/SRANDMEMBER currently return deterministic
//! members (first-iter-order). Real Redis randomises element selection.
//! Wire-diff fidelity requires plumbing a seeded RNG once the random
//! infrastructure lands.
//!
//! TODO(architect): keyspace-event notifications and replication command
//! rewriting are stubbed — they have no observable wire effect for the
//! in-tree smoke tests but must land before the AOF / replication phases.
//!
//! TODO(architect): swap the `Inline` encoding for real `ListPack` /
//! `IntSet` / `HashTable` types from `redis-ds` once Phase 4 makes them
//! usable.

use std::collections::HashSet;

use redis_core::command_context::CommandContext;
use redis_core::db::glob_match;
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

/// Borrow the inner `HashSet` of a set-encoded `RedisObject`, raising
/// `WRONGTYPE` if `obj` is any other kind.
///
/// Returns `Ok(None)` if the key is absent so callers can preserve
/// existence semantics without nesting `match` on the lookup result.
fn as_set_ref(obj: Option<&RedisObject>) -> Result<Option<&HashSet<RedisString>>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => o.set().map(Some).ok_or_else(RedisError::wrong_type),
    }
}

/// Mutable variant of `as_set_ref`.
fn as_set_mut(
    obj: Option<&mut RedisObject>,
) -> Result<Option<&mut HashSet<RedisString>>, RedisError> {
    match obj {
        None => Ok(None),
        Some(o) => {
            if o.is_set() {
                Ok(o.set_mut())
            } else {
                Err(RedisError::wrong_type())
            }
        }
    }
}

/// Snapshot a set's members into an owned `HashSet`, returning `None`
/// when the key is absent and `Err(WRONGTYPE)` when the key is not a set.
///
/// Used by the read-only multi-key set algebra commands (`SINTER`,
/// `SUNION`, `SDIFF`) and their `*STORE` variants to side-step lifetime
/// issues stemming from borrowing the same database mutably during the
/// store phase.
fn snapshot_set(
    ctx: &CommandContext,
    key: &RedisString,
) -> Result<Option<HashSet<RedisString>>, RedisError> {
    match as_set_ref(ctx.db().lookup_key_read(key))? {
        None => Ok(None),
        Some(h) => Ok(Some(h.clone())),
    }
}

/// SADD key member [member ...]
///
/// Returns the number of new members added (duplicates do not count).
/// Creates the key with the pragmatic Inline encoding when absent.
pub fn sadd_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg_owned(1usize)?;
    let mut members: Vec<RedisString> = Vec::with_capacity(argc - 2);
    for j in 2..argc {
        members.push(ctx.arg_owned(j)?);
    }
    let added = match ctx.db_mut().lookup_key_write(&key) {
        None => {
            let mut obj = RedisObject::new_set();
            let h = obj
                .set_mut()
                .expect("new_set constructs an Inline set");
            let mut count: i64 = 0;
            for m in members {
                if h.insert(m) {
                    count += 1;
                }
            }
            ctx.db_mut().set_key(key, obj, 0);
            count
        }
        Some(obj) => {
            if !obj.is_set() {
                return Err(RedisError::wrong_type());
            }
            let h = obj
                .set_mut()
                .expect("is_set confirms set encoding");
            let mut count: i64 = 0;
            for m in members {
                if h.insert(m) {
                    count += 1;
                }
            }
            count
        }
    };
    ctx.reply_integer(added)
}

/// SREM key member [member ...]
///
/// Returns the number of members actually removed. Deletes the key when
/// the resulting set is empty.
pub fn srem_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(ctx.command_name()));
    }
    let key = ctx.arg_owned(1usize)?;
    let mut members: Vec<RedisString> = Vec::with_capacity(argc - 2);
    for j in 2..argc {
        members.push(ctx.arg_owned(j)?);
    }
    let removed = {
        let h = match as_set_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => return ctx.reply_integer(0),
            Some(h) => h,
        };
        let mut count: i64 = 0;
        for m in members {
            if h.remove(&m) {
                count += 1;
            }
        }
        count
    };
    let empty_after = matches!(
        ctx.db().lookup_key_read(&key),
        Some(o) if o.set().map(|h| h.is_empty()).unwrap_or(false)
    );
    if empty_after {
        ctx.db_mut().sync_delete(&key);
    }
    ctx.reply_integer(removed)
}

/// SMEMBERS key
///
/// Returns every member of the set in unspecified order. Replies with an
/// empty array if the key is absent.
pub fn smembers_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"smembers"));
    }
    let key = ctx.arg_owned(1usize)?;
    let items: Vec<RedisString> = match as_set_ref(ctx.db().lookup_key_read(&key))? {
        None => Vec::new(),
        Some(h) => h.iter().cloned().collect(),
    };
    ctx.reply_array_header(items.len())?;
    for item in items {
        ctx.reply_bulk_string(item)?;
    }
    Ok(())
}

/// SISMEMBER key member
///
/// Returns `:1\r\n` if `member` is in the set, `:0\r\n` otherwise.
pub fn sismember_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"sismember"));
    }
    let key = ctx.arg_owned(1usize)?;
    let member = ctx.arg_owned(2usize)?;
    let present = match as_set_ref(ctx.db().lookup_key_read(&key))? {
        None => false,
        Some(h) => h.contains(&member),
    };
    ctx.reply_integer(present as i64)
}

/// SMISMEMBER key member [member ...]
///
/// Returns an array of `:0\r\n` / `:1\r\n` flags matching the order of the
/// queried members.
pub fn smismember_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"smismember"));
    }
    let key = ctx.arg_owned(1usize)?;
    let mut members: Vec<RedisString> = Vec::with_capacity(argc - 2);
    for j in 2..argc {
        members.push(ctx.arg_owned(j)?);
    }
    let flags: Vec<i64> = match as_set_ref(ctx.db().lookup_key_read(&key))? {
        None => members.iter().map(|_| 0).collect(),
        Some(h) => members.iter().map(|m| h.contains(m) as i64).collect(),
    };
    ctx.reply_array_header(flags.len())?;
    for f in flags {
        ctx.reply_integer(f)?;
    }
    Ok(())
}

/// SCARD key
///
/// Returns the set cardinality, or `:0\r\n` when the key is absent.
pub fn scard_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"scard"));
    }
    let key = ctx.arg_owned(1usize)?;
    let len = match as_set_ref(ctx.db().lookup_key_read(&key))? {
        None => 0,
        Some(h) => h.len() as i64,
    };
    ctx.reply_integer(len)
}

/// SPOP key [count]
///
/// Without `count`: replies with a single random member as a bulk string,
/// or `$-1\r\n` when the key is absent. With `count`: replies with an
/// array of up to `count` distinct members in unspecified order. Deletes
/// the key when the last element is removed.
///
/// TODO(architect): selection is deterministic (first iterator order)
/// instead of random. Plumb a seeded RNG once the random infrastructure
/// lands so the wire-diff oracle can be tightened.
pub fn spop_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 || argc > 3 {
        return Err(RedisError::wrong_number_of_args(b"spop"));
    }
    let key = ctx.arg_owned(1usize)?;
    let count: Option<i64> = if argc == 3 {
        let n = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
        if n < 0 {
            return Err(RedisError::runtime(
                b"ERR value is out of range, must be positive",
            ));
        }
        Some(n)
    } else {
        None
    };

    let popped: Option<Vec<RedisString>> = {
        let h = match as_set_mut(ctx.db_mut().lookup_key_write(&key))? {
            None => None,
            Some(h) => Some(h),
        };
        match h {
            None => None,
            Some(h) => {
                let take = match count {
                    None => 1usize.min(h.len()),
                    Some(n) => (n as usize).min(h.len()),
                };
                let targets: Vec<RedisString> = h.iter().take(take).cloned().collect();
                for m in &targets {
                    h.remove(m);
                }
                Some(targets)
            }
        }
    };

    let empty_after = matches!(
        ctx.db().lookup_key_read(&key),
        Some(o) if o.set().map(|h| h.is_empty()).unwrap_or(false)
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
        (Some(_), None) => {
            ctx.reply_array_header(0usize)?;
            Ok(())
        }
        (Some(_), Some(v)) => {
            ctx.reply_array_header(v.len())?;
            for elem in v {
                ctx.reply_bulk_string(elem)?;
            }
            Ok(())
        }
    }
}

/// SRANDMEMBER key [count]
///
/// Without `count`: replies with a single random member as a bulk string,
/// or `$-1\r\n` when the key is absent. With a positive `count`: replies
/// with an array of up to `count` distinct members. With a negative
/// `count`: replies with an array of `|count|` members allowing
/// duplicates. The set is not modified.
///
/// TODO(architect): selection is deterministic (first iterator order)
/// instead of random.
pub fn srandmember_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 || argc > 3 {
        return Err(RedisError::wrong_number_of_args(b"srandmember"));
    }
    let key = ctx.arg_owned(1usize)?;
    let count: Option<i64> = if argc == 3 {
        Some(parse_strict_i64(ctx.arg(2)?.as_bytes())?)
    } else {
        None
    };
    let members: Vec<RedisString> = match as_set_ref(ctx.db().lookup_key_read(&key))? {
        None => Vec::new(),
        Some(h) => h.iter().cloned().collect(),
    };

    match count {
        None => {
            if members.is_empty() {
                ctx.reply_null_bulk()
            } else {
                let first = members
                    .into_iter()
                    .next()
                    .expect("non-empty members yields at least one element");
                ctx.reply_bulk_string(first)
            }
        }
        Some(n) if n >= 0 => {
            let take = (n as usize).min(members.len());
            ctx.reply_array_header(take)?;
            for elem in members.into_iter().take(take) {
                ctx.reply_bulk_string(elem)?;
            }
            Ok(())
        }
        Some(n) => {
            let take = n.unsigned_abs() as usize;
            ctx.reply_array_header(take)?;
            if members.is_empty() {
                return Ok(());
            }
            for i in 0..take {
                let pick = members[i % members.len()].clone();
                ctx.reply_bulk_string(pick)?;
            }
            Ok(())
        }
    }
}

/// SMOVE source destination member
///
/// Atomically moves `member` from `source` to `destination`. Returns
/// `:1\r\n` on success and `:0\r\n` when `member` is not in `source`.
/// Raises `WRONGTYPE` if either key holds a non-set value. Deletes
/// `source` when its last element is moved out. Creates `destination`
/// with the pragmatic Inline encoding when absent.
pub fn smove_command(ctx: &mut CommandContext) -> RedisResult<()> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"smove"));
    }
    let src_key = ctx.arg_owned(1usize)?;
    let dst_key = ctx.arg_owned(2usize)?;
    let member = ctx.arg_owned(3usize)?;

    if let Some(dst_obj) = ctx.db().lookup_key_read(&dst_key) {
        if !dst_obj.is_set() {
            return Err(RedisError::wrong_type());
        }
    }

    let removed = {
        let h = match as_set_mut(ctx.db_mut().lookup_key_write(&src_key))? {
            None => return ctx.reply_integer(0),
            Some(h) => h,
        };
        h.remove(&member)
    };
    if !removed {
        return ctx.reply_integer(0);
    }

    let src_empty = matches!(
        ctx.db().lookup_key_read(&src_key),
        Some(o) if o.set().map(|h| h.is_empty()).unwrap_or(false)
    );
    if src_empty {
        ctx.db_mut().sync_delete(&src_key);
    }

    match ctx.db_mut().lookup_key_write(&dst_key) {
        Some(obj) => {
            let h = obj
                .set_mut()
                .expect("WRONGTYPE pre-check confirmed set encoding");
            h.insert(member);
        }
        None => {
            let mut obj = RedisObject::new_set();
            let h = obj
                .set_mut()
                .expect("new_set constructs an Inline set");
            h.insert(member);
            ctx.db_mut().set_key(dst_key, obj, 0);
        }
    }
    ctx.reply_integer(1)
}

/// Snapshot every set named in `argv[start..end]`. Returns `Err(WRONGTYPE)`
/// for any non-set key. Missing keys yield an empty set so the algebra
/// operations handle them uniformly.
fn collect_set_snapshots(
    ctx: &CommandContext,
    start: usize,
    end: usize,
) -> Result<Vec<HashSet<RedisString>>, RedisError> {
    let mut out = Vec::with_capacity(end - start);
    for j in start..end {
        let key = ctx.arg(j)?.clone();
        match snapshot_set(ctx, &key)? {
            None => out.push(HashSet::new()),
            Some(h) => out.push(h),
        }
    }
    Ok(out)
}

/// Compute the intersection of `sets`. Returns an empty set when any
/// input is empty.
fn intersect_sets(sets: Vec<HashSet<RedisString>>) -> HashSet<RedisString> {
    if sets.is_empty() {
        return HashSet::new();
    }
    if sets.iter().any(|s| s.is_empty()) {
        return HashSet::new();
    }
    let mut iter = sets.into_iter();
    let mut acc = iter
        .next()
        .expect("non-empty sets guarantees a first element");
    for s in iter {
        acc.retain(|m| s.contains(m));
        if acc.is_empty() {
            break;
        }
    }
    acc
}

/// Compute the union of `sets`.
fn union_sets(sets: Vec<HashSet<RedisString>>) -> HashSet<RedisString> {
    let mut acc: HashSet<RedisString> = HashSet::new();
    for s in sets {
        for m in s {
            acc.insert(m);
        }
    }
    acc
}

/// Compute `sets[0]` minus every following set. Returns an empty set
/// when `sets` is empty.
fn diff_sets(mut sets: Vec<HashSet<RedisString>>) -> HashSet<RedisString> {
    if sets.is_empty() {
        return HashSet::new();
    }
    let mut acc = sets.remove(0);
    for s in sets {
        for m in s {
            acc.remove(&m);
        }
    }
    acc
}

/// Reply with an unordered array of the supplied members.
fn reply_member_array(
    ctx: &mut CommandContext,
    members: HashSet<RedisString>,
) -> RedisResult<()> {
    let collected: Vec<RedisString> = members.into_iter().collect();
    ctx.reply_array_header(collected.len())?;
    for m in collected {
        ctx.reply_bulk_string(m)?;
    }
    Ok(())
}

/// Store `members` at `dst`, deleting `dst` when `members` is empty.
fn store_set(
    ctx: &mut CommandContext,
    dst: RedisString,
    members: HashSet<RedisString>,
) -> i64 {
    if members.is_empty() {
        ctx.db_mut().sync_delete(&dst);
        return 0;
    }
    let len = members.len() as i64;
    let mut obj = RedisObject::new_set();
    {
        let h = obj
            .set_mut()
            .expect("new_set constructs an Inline set");
        for m in members {
            h.insert(m);
        }
    }
    ctx.db_mut().set_key(dst, obj, 0);
    len
}

/// SINTER key [key ...]
pub fn sinter_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"sinter"));
    }
    let snapshots = collect_set_snapshots(ctx, 1, argc)?;
    let result = intersect_sets(snapshots);
    reply_member_array(ctx, result)
}

/// SINTERSTORE destination key [key ...]
pub fn sinterstore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"sinterstore"));
    }
    let dst = ctx.arg_owned(1usize)?;
    let snapshots = collect_set_snapshots(ctx, 2, argc)?;
    let result = intersect_sets(snapshots);
    let stored = store_set(ctx, dst, result);
    ctx.reply_integer(stored)
}

/// SINTERCARD numkeys key [key ...] [LIMIT limit]
///
/// Returns the cardinality of the intersection of the supplied sets,
/// optionally capped at `limit`. A `limit` of `0` means "no cap".
pub fn sintercard_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"sintercard"));
    }
    let numkeys = parse_strict_i64(ctx.arg(1)?.as_bytes())?;
    if numkeys <= 0 {
        return Err(RedisError::runtime(
            b"ERR numkeys should be greater than 0",
        ));
    }
    let numkeys = numkeys as usize;
    if argc < 2 + numkeys {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let limit = if argc == 2 + numkeys {
        0i64
    } else if argc == 4 + numkeys {
        let opt = ctx.arg(2 + numkeys)?;
        if !opt.as_bytes().eq_ignore_ascii_case(b"limit") {
            return Err(RedisError::syntax(b"syntax error"));
        }
        let n = parse_strict_i64(ctx.arg(3 + numkeys)?.as_bytes())?;
        if n < 0 {
            return Err(RedisError::runtime(
                b"ERR LIMIT can't be negative",
            ));
        }
        n
    } else {
        return Err(RedisError::syntax(b"syntax error"));
    };
    let snapshots = collect_set_snapshots(ctx, 2, 2 + numkeys)?;
    let result = intersect_sets(snapshots);
    let card = if limit > 0 {
        (result.len() as i64).min(limit)
    } else {
        result.len() as i64
    };
    ctx.reply_integer(card)
}

/// SUNION key [key ...]
pub fn sunion_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"sunion"));
    }
    let snapshots = collect_set_snapshots(ctx, 1, argc)?;
    let result = union_sets(snapshots);
    reply_member_array(ctx, result)
}

/// SUNIONSTORE destination key [key ...]
pub fn sunionstore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"sunionstore"));
    }
    let dst = ctx.arg_owned(1usize)?;
    let snapshots = collect_set_snapshots(ctx, 2, argc)?;
    let result = union_sets(snapshots);
    let stored = store_set(ctx, dst, result);
    ctx.reply_integer(stored)
}

/// SDIFF key [key ...]
pub fn sdiff_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"sdiff"));
    }
    let snapshots = collect_set_snapshots(ctx, 1, argc)?;
    let result = diff_sets(snapshots);
    reply_member_array(ctx, result)
}

/// SDIFFSTORE destination key [key ...]
pub fn sdiffstore_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"sdiffstore"));
    }
    let dst = ctx.arg_owned(1usize)?;
    let snapshots = collect_set_snapshots(ctx, 2, argc)?;
    let result = diff_sets(snapshots);
    let stored = store_set(ctx, dst, result);
    ctx.reply_integer(stored)
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
///
/// Linear-cursor iteration over the members of a set. Returns a two-element
/// reply `[next_cursor, members]`, matching real Redis's wire shape. The
/// cursor is a `u64` byte-offset into the snapshot taken at call time;
/// the resize-safe reverse-binary cursor lands once the kvstore primitive
/// is ported.
pub fn sscan_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"sscan"));
    }
    let key = ctx.arg_owned(1usize)?;
    let cursor = parse_u64_cursor(ctx.arg(2)?.as_bytes())?;

    let mut pattern: Option<Vec<u8>> = None;
    let mut count: i64 = 10;
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
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    let members: Vec<RedisString> = match as_set_ref(ctx.db().lookup_key_read(&key))? {
        None => Vec::new(),
        Some(h) => h.iter().cloned().collect(),
    };
    let total = members.len() as u64;
    let start = cursor as usize;
    let stop = (start + count as usize).min(members.len());
    let next_cursor: u64 = if stop as u64 >= total { 0 } else { stop as u64 };

    let mut matched: Vec<RedisString> = Vec::new();
    for m in members.into_iter().skip(start).take(count as usize) {
        if let Some(ref pat) = pattern {
            if !glob_match(pat, m.as_bytes()) {
                continue;
            }
        }
        matched.push(m);
    }

    ctx.reply_array_header(2usize)?;
    ctx.reply_bulk(next_cursor.to_string().as_bytes())?;
    ctx.reply_array_header(matched.len())?;
    for m in matched {
        ctx.reply_bulk_string(m)?;
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

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/t_set.c
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         3
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Round 4 byte-exact implementations for SADD, SREM,
//                  SMEMBERS, SISMEMBER, SMISMEMBER, SCARD, SPOP,
//                  SRANDMEMBER, SMOVE, SINTER, SINTERSTORE, SINTERCARD,
//                  SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE backed by the
//                  pragmatic SetEncoding::Inline encoding from
//                  redis-core::object. SPOP and SRANDMEMBER currently use
//                  deterministic element selection (first iterator order)
//                  pending a seeded RNG. Phase 4 will swap Inline for the
//                  real ListPack / IntSet / HashTable encodings from
//                  redis-ds.
// ──────────────────────────────────────────────────────────────────────────
