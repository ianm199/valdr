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
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::command_context::CommandContext;
use redis_core::db::glob_match;
use redis_core::notify::{NOTIFY_GENERIC, NOTIFY_SET};
use redis_core::object::{
    get_encoding_thresholds, is_canonical_i64_ascii, InlineSetEncoding, RedisObject,
};
use redis_types::{RedisError, RedisResult, RedisString};

/// Advance the sticky encoding on an `InlineSet` to the minimum encoding
/// required by its current members and the active configuration thresholds.
///
/// Real Redis promotes set encoding (intset → listpack → hashtable) but never
/// automatically demotes it. After every SADD we call this to record any
/// promotion so that subsequent SREM calls do not reset the observed encoding.
fn update_sticky_encoding(s: &mut redis_core::object::InlineSet) {
    let t = get_encoding_thresholds();
    let mut all_integer = true;
    let mut max_len: usize = 0;
    for m in &s.data {
        let bytes = m.as_bytes();
        if bytes.len() > max_len {
            max_len = bytes.len();
        }
        if all_integer && !is_canonical_i64_ascii(bytes) {
            all_integer = false;
        }
    }
    let computed = if all_integer && s.data.len() <= t.set_max_intset_entries {
        InlineSetEncoding::Auto
    } else if s.data.len() <= t.set_max_listpack_entries && max_len <= t.set_max_listpack_value {
        InlineSetEncoding::ForcedListpack
    } else {
        InlineSetEncoding::ForcedHashtable
    };
    if computed > s.sticky {
        s.sticky = computed;
    }
}

#[derive(Clone, Copy)]
struct SetInsertEncodingDelta {
    count: usize,
    all_integer: bool,
    max_len: usize,
}

impl SetInsertEncodingDelta {
    const fn empty() -> Self {
        Self {
            count: 0,
            all_integer: true,
            max_len: 0,
        }
    }

    fn record_parts(&mut self, len: usize, is_integer: bool) {
        self.count += 1;
        self.max_len = self.max_len.max(len);
        self.all_integer &= is_integer;
    }
}

/// Update a set's sticky encoding after successful insertions without scanning
/// the full member set on every SADD. The full scan is only needed on rare
/// promotion boundaries, such as an intset-shaped set receiving its first
/// non-integer member.
fn update_sticky_encoding_after_insert(
    s: &mut redis_core::object::InlineSet,
    delta: SetInsertEncodingDelta,
) {
    if delta.count == 0 || s.sticky == InlineSetEncoding::ForcedHashtable {
        return;
    }

    let t = get_encoding_thresholds();
    if s.sticky == InlineSetEncoding::ForcedListpack {
        if s.data.len() <= t.set_max_listpack_entries && delta.max_len <= t.set_max_listpack_value {
            return;
        }
        update_sticky_encoding(s);
        return;
    }

    if delta.all_integer && s.data.len() <= t.set_max_intset_entries {
        return;
    }
    update_sticky_encoding(s);
}

/// Return a seed derived from the current system time in nanoseconds.
///
/// Used to bootstrap the xorshift64 PRNG in SRANDMEMBER and SPOP so that
/// element selection is non-deterministic across commands. The seed is not
/// cryptographically strong but is sufficient for the statistical distribution
/// properties that the TCL test suite verifies (all members reachable over
/// repeated calls).
fn time_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 ^ (d.as_secs().wrapping_mul(6364136223846793005)))
        .unwrap_or(12345678901234567)
}

/// Xorshift64 step — advances `state` and returns the new value.
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
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
            let s = obj
                .inline_set_mut()
                .expect("new_set constructs an Inline set");
            let mut count: i64 = 0;
            let mut delta = SetInsertEncodingDelta::empty();
            for m in members {
                let member_len = m.as_bytes().len();
                let member_is_integer = is_canonical_i64_ascii(m.as_bytes());
                if s.data.insert(m) {
                    delta.record_parts(member_len, member_is_integer);
                    count += 1;
                }
            }
            update_sticky_encoding_after_insert(s, delta);
            ctx.db_mut().set_key(key.clone(), obj, 0);
            count
        }
        Some(obj) => {
            if !obj.is_set() {
                return Err(RedisError::wrong_type());
            }
            let s = obj.inline_set_mut().expect("is_set confirms set encoding");
            let mut count: i64 = 0;
            let mut delta = SetInsertEncodingDelta::empty();
            for m in members {
                let member_len = m.as_bytes().len();
                let member_is_integer = is_canonical_i64_ascii(m.as_bytes());
                if s.data.insert(m) {
                    delta.record_parts(member_len, member_is_integer);
                    count += 1;
                }
            }
            update_sticky_encoding_after_insert(s, delta);
            count
        }
    };
    if added > 0 {
        ctx.notify_keyspace_event(NOTIFY_SET, b"sadd", &key);
    }
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
    if removed > 0 {
        ctx.notify_keyspace_event(NOTIFY_SET, b"srem", &key);
        if empty_after {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
        }
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
                let mut rng = time_seed();
                let mut all: Vec<RedisString> = h.iter().cloned().collect();
                for i in 0..take.min(all.len()) {
                    let j = i + (xorshift64(&mut rng) as usize) % (all.len() - i);
                    all.swap(i, j);
                }
                let targets: Vec<RedisString> = all.into_iter().take(take).collect();
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
    let did_pop = popped.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
    if did_pop {
        ctx.notify_keyspace_event(NOTIFY_SET, b"spop", &key);
        if empty_after {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
        }
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
pub fn srandmember_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 2 || argc > 3 {
        return Err(RedisError::wrong_number_of_args(b"srandmember"));
    }
    let key = ctx.arg_owned(1usize)?;
    let count: Option<i64> = if argc == 3 {
        let n = parse_strict_i64(ctx.arg(2)?.as_bytes())?;
        if n < -i64::MAX {
            return Err(RedisError::out_of_range());
        }
        Some(n)
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
                let mut rng = time_seed();
                let idx = (xorshift64(&mut rng) as usize) % members.len();
                ctx.reply_bulk_string(members.into_iter().nth(idx).expect("idx < len"))
            }
        }
        Some(n) if n >= 0 => {
            let take = (n as usize).min(members.len());
            ctx.reply_array_header(take)?;
            if take == 0 {
                return Ok(());
            }
            let mut rng = time_seed();
            let mut indices: Vec<usize> = (0..members.len()).collect();
            for i in 0..take {
                let j = i + (xorshift64(&mut rng) as usize) % (members.len() - i);
                indices.swap(i, j);
            }
            for i in 0..take {
                ctx.reply_bulk_string(members[indices[i]].clone())?;
            }
            Ok(())
        }
        Some(n) => {
            if members.is_empty() {
                ctx.reply_array_header(0)?;
                return Ok(());
            }
            let take = n.unsigned_abs() as usize;
            ctx.reply_array_header(take)?;
            let mut rng = time_seed();
            for _ in 0..take {
                let idx = (xorshift64(&mut rng) as usize) % members.len();
                ctx.reply_bulk_string(members[idx].clone())?;
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
            let h = obj.set_mut().expect("new_set constructs an Inline set");
            h.insert(member);
            ctx.db_mut().set_key(dst_key.clone(), obj, 0);
        }
    }
    ctx.notify_keyspace_event(NOTIFY_SET, b"srem", &src_key);
    if src_empty {
        ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &src_key);
    }
    ctx.notify_keyspace_event(NOTIFY_SET, b"sadd", &dst_key);
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
fn reply_member_array(ctx: &mut CommandContext, members: HashSet<RedisString>) -> RedisResult<()> {
    let collected: Vec<RedisString> = members.into_iter().collect();
    ctx.reply_array_header(collected.len())?;
    for m in collected {
        ctx.reply_bulk_string(m)?;
    }
    Ok(())
}

/// Store `members` at `dst`, deleting `dst` when `members` is empty.
fn store_set(ctx: &mut CommandContext, dst: RedisString, members: HashSet<RedisString>) -> i64 {
    if members.is_empty() {
        ctx.db_mut().sync_delete(&dst);
        return 0;
    }
    let len = members.len() as i64;
    let mut obj = RedisObject::new_set();
    {
        let h = obj.set_mut().expect("new_set constructs an Inline set");
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
    let stored = store_set(ctx, dst.clone(), result);
    ctx.notify_keyspace_event(NOTIFY_SET, b"sinterstore", &dst);
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
    let numkeys_raw = ctx.arg(1)?.as_bytes().to_vec();
    let numkeys = parse_strict_i64(&numkeys_raw)
        .ok()
        .filter(|&n| n >= 1)
        .ok_or_else(|| RedisError::runtime(b"ERR numkeys should be greater than 0"))?;
    let numkeys = numkeys as usize;
    if numkeys > argc - 2 {
        return Err(RedisError::runtime(
            b"ERR Number of keys can't be greater than number of args",
        ));
    }
    let mut j = 2 + numkeys;
    let mut limit: i64 = 0;
    while j < argc {
        let opt = ctx.arg(j)?.as_bytes().to_vec();
        if opt.eq_ignore_ascii_case(b"limit") {
            if j + 1 >= argc {
                return Err(RedisError::syntax(b"syntax error"));
            }
            let limit_raw = ctx.arg(j + 1)?.as_bytes().to_vec();
            limit = parse_strict_i64(&limit_raw)
                .ok()
                .filter(|&n| n >= 0)
                .ok_or_else(|| RedisError::runtime(b"ERR LIMIT can't be negative"))?;
            j += 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
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
    let stored = store_set(ctx, dst.clone(), result);
    ctx.notify_keyspace_event(NOTIFY_SET, b"sunionstore", &dst);
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
    let stored = store_set(ctx, dst.clone(), result);
    ctx.notify_keyspace_event(NOTIFY_SET, b"sdiffstore", &dst);
    ctx.reply_integer(stored)
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
///
/// Snapshot iteration over the members of a set. Returns a two-element reply
/// `[next_cursor, members]`, matching real Redis's wire shape.
///
/// PORT NOTE: until the real kvstore/reverse-binary cursor primitive lands,
/// SSCAN replies with every currently matching member in one call and cursor
/// `0`. Redis/Valkey only treats COUNT as a work hint, so returning the full
/// snapshot is legal and avoids dropping stable members when the set shrinks
/// between cursor calls (the upstream issue #4906 regression).
pub fn sscan_command(ctx: &mut CommandContext) -> RedisResult<()> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"sscan"));
    }
    let key = ctx.arg_owned(1usize)?;
    let _cursor = parse_u64_cursor(ctx.arg(2)?.as_bytes())?;

    let mut pattern: Option<Vec<u8>> = None;
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
            j += 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    let members: Vec<RedisString> = match as_set_ref(ctx.db().lookup_key_read(&key))? {
        None => Vec::new(),
        Some(h) => h.iter().cloned().collect(),
    };
    let mut matched: Vec<RedisString> = Vec::new();
    for m in members {
        if let Some(ref pat) = pattern {
            if !glob_match(pat, m.as_bytes()) {
                continue;
            }
        }
        matched.push(m);
    }

    ctx.reply_array_header(2usize)?;
    ctx.reply_bulk(b"0")?;
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
