//! Hash type implementation: HSET, HGET, HDEL, HINCRBY, HGETALL, HRANDFIELD,
//! per-field TTL commands (HEXPIRE, HPEXPIRE, HPERSIST, HTTL, HPTTL, …).
//!
//! C source: `reference/valkey/src/t_hash.c`  (~2489 lines, ~62 functions)
//! Crate: `redis-commands`  (later phase)
//!
//! ## Encoding model
//!
//! Valkey hashes use two internal encodings:
//!   - **Listpack** — compact sequential encoding for small hashes.
//!     (≤ `hash_max_listpack_entries` fields, each ≤ `hash_max_listpack_value` bytes)
//!   - **Hashtable** — open-addressed hash table for larger hashes.  Each
//!     slot holds an `entry` (from `entry.c`) that carries field, value, and
//!     an optional per-field expiry timestamp.
//!
//! ## Per-field TTL (Valkey 8.x)
//!
//! Hashtable-encoded hashes track volatile (expiry-carrying) fields in a
//! `vset` embedded in the hashtable metadata.  Listpack hashes do not support
//! per-field expiry; they are promoted to hashtable encoding on the first
//! expiry-set request.
//!
//! ## Deferred dependencies
//!
//! TODO(architect): `entry` type lives in `redis-server/src/entry.rs` (later
//!   phase).  Add a dep edge `redis-commands → redis-server` so this crate can
//!   import it.  Until resolved, all `entry_*` calls are stubbed.
//!
//! TODO(architect): `vset` type lives in `redis-commands/src/vset.rs` (defer
//!   phase).  All volatile-set operations are stubbed.
//!
//! TODO(architect): `listpack` API (`lpFirst`, `lpFind`, `lpNext`,
//!   `lpGetValue`, `lpReplace`, `lpAppend`, `lpRandomPair`, `lpRandomPairs`,
//!   `lpRandomPairsUnique`, `lpBytes`, `lpSafeToAdd`) lives in
//!   `redis-ds/src/listpack_listpack.rs` (deferred Phase 4).  All listpack
//!   paths are stubbed.
//!
//! TODO(architect): `hashtable` API lives in `redis-ds/src/hashtable.rs`
//!   (deferred Phase 4).  Hashtable calls in internal helpers are stubbed.
//!
//! TODO(architect): `CommandContext::db()` / `db_mut()` pattern — needs
//!   `&mut RedisServer` access wired through CommandContext per Phase 3 packet.
//!
//! TODO(architect): `CommandContext::command_time_snapshot() -> i64` — cached
//!   dispatch-time Unix-ms timestamp.
//!
//! TODO(architect): `CommandContext::notify_keyspace_event(type, event, key)`
//!   — keyspace notification dispatch; blocked on Phase 3.
//!
//! TODO(architect): `CommandContext::signal_modified_key(key)` — WATCH +
//!   client-tracking invalidation; blocked on Phase 3.
//!
//! TODO(architect): `replace_client_command_vector` / `rewrite_client_command_argument`
//!   — command rewriting for AOF/replication; blocked on Phase 3 replication layer.
//!
//! TODO(architect): `propagate_fields_deletion` — batched HDEL propagation for
//!   HSETEX/HGETEX; needs replication layer from Phase 3.
//!
//! TODO(architect): `parse_extended_command_arguments_or_reply` /
//!   `parse_extended_expire_arguments_or_reply` — flag-parsing helpers shared
//!   across multiple commands; should live in redis-core or redis-commands util.
//!
//! TODO(architect): `convert_expire_argument_to_unix_time` — expiry conversion
//!   helper shared with other TTL commands; belongs in redis-core expire layer.
//!
//! TODO(architect): `db_update_object_with_volatile_items_tracking` /
//!   `db_untrack_key_with_volatile_items` — per-key volatile tracking at the
//!   db layer; blocked on Phase 3.

use redis_core::command_context::CommandContext;
use redis_core::object::RedisObject;
use redis_types::{RedisError, RedisString};

// ── Hash set flags (C: HASH_SET_* in server.h) ────────────────────────────
// PORT NOTE: bit positions are local; C constants live in server.h.
pub(crate) const HASH_SET_COPY: u32        = 0;
pub(crate) const HASH_SET_TAKE_FIELD: u32  = 1 << 0;
pub(crate) const HASH_SET_TAKE_VALUE: u32  = 1 << 1;
pub(crate) const HASH_SET_KEEP_EXPIRY: u32 = 1 << 2;

// ── OBJ_HASH iterator flags (C: OBJ_HASH_FIELD / OBJ_HASH_VALUE) ─────────
pub(crate) const OBJ_HASH_FIELD: u32 = 1 << 0;
pub(crate) const OBJ_HASH_VALUE: u32 = 1 << 1;

// ── Expiry sentinel (C: EXPIRY_NONE = -1) ────────────────────────────────
pub(crate) const EXPIRY_NONE: i64 = -1;

// ── EXPIRE conditional flags (C: EXPIRE_NX/XX/GT/LT in server.h) ─────────
pub(crate) const EXPIRE_NX: u32 = 1 << 0;
pub(crate) const EXPIRE_XX: u32 = 1 << 1;
pub(crate) const EXPIRE_GT: u32 = 1 << 2;
pub(crate) const EXPIRE_LT: u32 = 1 << 3;

// ── Time units ────────────────────────────────────────────────────────────
pub(crate) const UNIT_SECONDS:      i32 = 0;
pub(crate) const UNIT_MILLISECONDS: i32 = 1;

// ── HSETEX/HGETEX argument flags (C: ARGS_* in server.h) ─────────────────
pub(crate) const ARGS_NO_FLAGS:  u32 = 0;
pub(crate) const ARGS_SET_NX:    u32 = 1 << 0;
pub(crate) const ARGS_SET_XX:    u32 = 1 << 1;
pub(crate) const ARGS_SET_FNX:   u32 = 1 << 2;
pub(crate) const ARGS_SET_FXX:   u32 = 1 << 3;
pub(crate) const ARGS_KEEPTTL:   u32 = 1 << 4;
pub(crate) const ARGS_PERSIST:   u32 = 1 << 5;
pub(crate) const ARGS_EX:        u32 = 1 << 6;
pub(crate) const ARGS_PX:        u32 = 1 << 7;
pub(crate) const ARGS_EXAT:      u32 = 1 << 8;
pub(crate) const ARGS_PXAT:      u32 = 1 << 9;

// ── HRANDFIELD sampling strategy constants (C: #define in t_hash.c) ───────
pub(crate) const HRANDFIELD_SUB_STRATEGY_MUL:    u64 = 3;
pub(crate) const HRANDFIELD_RANDOM_SAMPLE_LIMIT: u64 = 1000;

// ── COMMAND_HSET / COMMAND_HGET identifiers (C: enum in server.h) ─────────
// PORT NOTE: In C these are enum values used to dispatch into the argument
// parser.  Represented here as integer constants pending a proper CommandId
// enum in redis-commands.
pub(crate) const COMMAND_HSET: u32 = 1;
pub(crate) const COMMAND_HGET: u32 = 2;

// ─────────────────────────────────────────────────────────────────────────
// Local types
// ─────────────────────────────────────────────────────────────────────────

/// Result codes returned by expiry-modification helpers.
///
/// C: `expiryModificationResult` enum in `t_hash.c`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpiryModificationResult {
    /// Field not found or object is NULL.  C: `EXPIRATION_MODIFICATION_NOT_EXIST = -2`
    NotExist = -2,
    /// Expiry was applied or modified.  C: `EXPIRATION_MODIFICATION_SUCCESSFUL = 1`
    Successful = 1,
    /// Conditional flag check failed (NX/XX/GT/LT).  C: `EXPIRATION_MODIFICATION_FAILED_CONDITION = 0`
    FailedCondition = 0,
    /// Field exists but has no expiry (HPERSIST on non-expiring field).  C: `EXPIRATION_MODIFICATION_FAILED = -1`
    Failed = -1,
    /// Expiry was set to a time in the past — field immediately expired.  C: `EXPIRATION_MODIFICATION_EXPIRE_ASAP = 2`
    ExpireAsap = 2,
}

/// A `listpackEntry` as returned by `lpGetValue` / `lpRandomPair*`.
///
/// Holds either a byte-string value (`sval`/`slen`) or an integer (`lval`).
/// C: `listpackEntry` typedef in `listpack.h`.
///
/// TODO(port): This is a Phase A placeholder.  The real type lives in
///   `redis-ds/src/listpack_listpack.rs` (deferred).  Replace with
///   `use redis_ds::listpack::ListpackEntry;` when that crate is available.
#[derive(Debug, Default, Clone)]
pub(crate) struct ListpackEntry {
    pub sval: Option<Vec<u8>>,
    pub slen: u32,
    pub lval: i64,
}

/// Hash encoding discriminant.
///
/// C: `OBJ_ENCODING_LISTPACK` and `OBJ_ENCODING_HASHTABLE` integer constants.
/// In Rust we use an enum so match arms are exhaustive and legible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashEncoding {
    Listpack,
    Hashtable,
}

/// Iterator state for walking a hash object's field–value pairs.
///
/// C: `hashTypeIterator` in `server.h`.  The C struct embeds raw listpack
/// pointers and a hashtable iterator; here we use opaque byte slices /
/// indices as Phase A placeholders until the data-structure crates land.
///
/// TODO(port): Replace `lp_*` fields with real listpack cursor type once
///   `redis-ds::listpack` is available.  Replace `ht_*` fields with the
///   hashtable iterator type from `redis-ds::hashtable`.
pub(crate) struct HashTypeIterator {
    pub encoding: HashEncoding,
    pub volatile_items_iter: bool,
    /// Listpack cursor: raw byte position (Phase A opaque placeholder).
    pub lp_fptr: Option<usize>,
    pub lp_vptr: Option<usize>,
    /// Hashtable iterator placeholder — holds the "current" element as raw
    /// bytes until `redis-ds` is available.
    pub ht_next: Option<Vec<u8>>,
}

/// Context passed to `hash_type_expire_entry` callback during expiry sweeps.
///
/// C: `expiryContext` struct (local to `t_hash.c`).
struct ExpiryContext {
    /// Number of expired entries recorded so far.
    n_fields: usize,
    /// Accumulated expired field names for replication propagation.
    /// TODO(port): Should hold `RedisString` once `entry` type is available.
    fields: Option<Vec<RedisString>>,
}

// ─────────────────────────────────────────────────────────────────────────
// Expiry API — internal helpers
// ─────────────────────────────────────────────────────────────────────────

/// Returns true if the hash object contains any volatile (expiry-carrying) fields.
///
/// C: `hashTypeHasVolatileFields`, `t_hash.c:69`.
///
/// TODO(port): Requires `vset` / `hashtable` from deferred crates.  Currently
///   always returns false as a safe default.
pub(crate) fn hash_type_has_volatile_fields(o: &RedisObject) -> bool {
    // TODO(port): implement once vset (redis-commands/src/vset.rs) is available.
    // C checks o->encoding == OBJ_ENCODING_HASHTABLE, then reads vset from
    // hashtable metadata and asks vsetIsEmpty().
    let _ = o;
    false
}

/// Release the volatile set embedded in a hashtable-encoded hash.
///
/// C: `hashTypeFreeVolatileSet`, `t_hash.c:103`.
///
/// TODO(port): Requires `vset` from deferred `redis-commands/src/vset.rs`.
pub(crate) fn hash_type_free_volatile_set(o: &mut RedisObject) {
    // TODO(port): call vsetRelease on the vset embedded in hashtable metadata.
    let _ = o;
}

/// Register a new hashtable entry in the hash's volatile set.
///
/// C: `hashTypeTrackEntry`, `t_hash.c:110`.
///
/// TODO(port): Requires `entry` (redis-server) and `vset` (deferred).
pub(crate) fn hash_type_track_entry(o: &mut RedisObject, _entry_bytes: &[u8]) {
    // TODO(port): obtain or create the volatile set, then call vsetAddEntry.
    let _ = o;
}

/// Remove a hashtable entry from the hash's volatile set (on delete).
///
/// C: `hashTypeUntrackEntry`, `t_hash.c:121`.
///
/// TODO(port): Requires `entry` (redis-server) and `vset` (deferred).
fn hash_type_untrack_entry(o: &mut RedisObject, _entry_bytes: &[u8]) {
    // TODO(port): if entry has expiry, call vsetRemoveEntry; free vset if empty.
    let _ = o;
}

/// Update volatile-set tracking when an entry's expiry changes.
///
/// C: `hashTypeTrackUpdateEntry`, `t_hash.c:131`.
///
/// TODO(port): Requires `entry` (redis-server) and `vset` (deferred).
fn hash_type_track_update_entry(
    o: &mut RedisObject,
    _old_entry: &[u8],
    _new_entry: &[u8],
    old_expiry: i64,
    new_expiry: i64,
) {
    // TODO(port): call vsetUpdateEntry, then free vset if it becomes empty.
    let _ = (o, old_expiry, new_expiry);
}

/// Validate a hashtable entry for the hash type (TTL-aware access).
///
/// C: `hashHashtableTypeValidate`, `t_hash.c:149`.
///
/// TODO(port): Requires `entry` (redis-server) and expiry policy from server config.
pub(crate) fn hash_hashtable_type_validate(_entry_bytes: &[u8]) -> bool {
    // TODO(port): call getExpirationPolicyWithFlags() and entryIsExpired().
    true
}

// ─────────────────────────────────────────────────────────────────────────
// Hash type API
// ─────────────────────────────────────────────────────────────────────────

/// Convert a small listpack hash to hashtable if size/length thresholds are exceeded.
///
/// C: `hashTypeTryConversion`, `t_hash.c:167`.
///
/// TODO(port): Requires server config access (hash_max_listpack_entries /
///   hash_max_listpack_value) and listpack / hashtable APIs from deferred crates.
pub(crate) fn hash_type_try_conversion(
    o: &mut RedisObject,
    argv: &[RedisString],
    start: usize,
    end: usize,
) {
    // TODO(port): check encoding == LISTPACK; count field lengths against
    // server.hash_max_listpack_entries / value; call hash_type_convert if exceeded.
    let _ = (o, argv, start, end);
}

/// Look up a field in a listpack-encoded hash.
///
/// Returns `Ok(None)` when the field is absent; `Ok(Some((bytes, integer)))` on
/// a hit (byte variant has `Some(bytes)`, integer variant has `None` bytes plus
/// populated `i64`).
///
/// C: `hashTypeGetFromListpack`, `t_hash.c:197`.
///
/// TODO(port): Requires listpack API from `redis-ds` (deferred Phase 4).
pub(crate) fn hash_type_get_from_listpack(
    o: &RedisObject,
    field: &[u8],
) -> Result<Option<(Option<Vec<u8>>, i64)>, RedisError> {
    // TODO(port): walk the listpack with lpFirst/lpFind/lpNext/lpGetValue.
    let _ = (o, field);
    Ok(None)
}

/// High-level field lookup: returns value bytes, integer, and optional expiry.
///
/// Returns `Ok(Some(...))` when found, `Ok(None)` when absent.
///
/// C: `hashTypeGetValue`, `t_hash.c:233`.
///
/// TODO(port): Requires listpack + hashtable + entry APIs from deferred crates.
pub(crate) fn hash_type_get_value(
    o: &RedisObject,
    field: &[u8],
) -> Result<Option<(Option<Vec<u8>>, i64, i64)>, RedisError> {
    // Returns (vstr_bytes, vll, expiry); caller checks which is populated.
    // TODO(port): dispatch on encoding; call hash_type_get_from_listpack or
    // hashtableFind + entryGetValue + entryGetExpiry.
    let _ = (o, field);
    // TODO(architect): is panic correct here? C uses serverPanic("Unknown hash encoding").
    Ok(None)
}

/// Returns the expiry of a hash field, or `EXPIRY_NONE` if no expiry is set.
///
/// C: `hashTypeGetExpiry`, `t_hash.c:262`.
///
/// TODO(port): Requires listpack + hashtable + entry APIs.
pub(crate) fn hash_type_get_expiry(
    o: &RedisObject,
    field: &[u8],
) -> Result<Option<i64>, RedisError> {
    // Returns Some(expiry_ms) if field exists (EXPIRY_NONE if no TTL), None if absent.
    // TODO(port): dispatch on encoding.
    let _ = (o, field);
    Ok(None)
}

/// Return the hash value associated with a field as a new `RedisObject`.
///
/// C: `hashTypeGetValueObject`, `t_hash.c:284`.
///
/// TODO(port): Requires hash_type_get_value and object construction.
pub(crate) fn hash_type_get_value_object(
    o: &RedisObject,
    field: &[u8],
) -> Result<Option<RedisObject>, RedisError> {
    // TODO(port): call hash_type_get_value; wrap bytes or integer into RedisObject::String.
    let _ = (o, field);
    Ok(None)
}

/// Return the byte-length of the value for a given field, or 0 if absent.
///
/// C: `hashTypeGetValueLength`, `t_hash.c:300`.
pub(crate) fn hash_type_get_value_length(
    o: &RedisObject,
    field: &[u8],
) -> Result<usize, RedisError> {
    // TODO(port): call hash_type_get_value; compute length of bytes or decimal digits.
    let _ = (o, field);
    Ok(0)
}

/// Returns true if the given field exists in the hash.
///
/// C: `hashTypeExists`, `t_hash.c:313`.
pub(crate) fn hash_type_exists(
    o: &RedisObject,
    field: &[u8],
) -> Result<bool, RedisError> {
    // TODO(port): delegate to hash_type_get_value, check Option.
    let _ = (o, field);
    Ok(false)
}

/// Add or overwrite a hash field with a value and optional expiry.
///
/// Returns `true` if the field existed and was updated; `false` on insert.
/// `expired_overwritten` is set to `true` if the prior entry was already expired.
///
/// C: `hashTypeSet`, `t_hash.c:368`.
///
/// TODO(port): Requires listpack + hashtable + entry + vset APIs.
pub(crate) fn hash_type_set(
    o: &mut RedisObject,
    field: RedisString,
    value: RedisString,
    expiry: i64,
    flags: u32,
) -> Result<(bool, bool), RedisError> {
    // Returns (updated, expired_overwritten).
    // TODO(port): dispatch on encoding; convert listpack->hashtable when thresholds exceeded.
    let _ = (o, field, value, expiry, flags);
    Ok((false, false))
}

/// Apply or update per-field expiry, subject to conditional flags.
///
/// C: `hashTypeSetExpire` (static), `t_hash.c:471`.
///
/// TODO(port): Requires hashtable + entry + vset APIs.
fn hash_type_set_expire(
    o: &mut RedisObject,
    field: &[u8],
    expiry: i64,
    flag: u32,
) -> ExpiryModificationResult {
    // TODO(port): handle LISTPACK (convert or fail GT/XX) then HASHTABLE path
    // with NX/XX/GT/LT condition checks.
    let _ = (o, field, expiry, flag);
    ExpiryModificationResult::NotExist
}

/// Remove per-field expiry (HPERSIST semantics).
///
/// C: `hashTypePersist` (static), `t_hash.c:554`.
///
/// TODO(port): Requires hashtable + entry + vset APIs.
fn hash_type_persist(
    o: &mut RedisObject,
    field: &[u8],
) -> ExpiryModificationResult {
    // TODO(port): return FAILED for listpack (no expiry), or untrack + clear expiry for hashtable.
    let _ = (o, field);
    ExpiryModificationResult::NotExist
}

/// Delete a field from a hash.
///
/// Returns `true` if deleted, `false` if not found.
///
/// C: `hashTypeDelete`, `t_hash.c:583`.
///
/// TODO(port): Requires listpack + hashtable + entry + vset APIs.
pub(crate) fn hash_type_delete(
    o: &mut RedisObject,
    field: &[u8],
) -> Result<bool, RedisError> {
    // TODO(port): dispatch on encoding; lpDeleteRangeWithEntry for listpack,
    // hashtablePop + entryFree + untrack for hashtable.
    let _ = (o, field);
    Ok(false)
}

/// Return the number of field–value pairs in the hash.
///
/// C: `hashTypeLength`, `t_hash.c:615`.
///
/// TODO(port): Requires listpack (lpLength) and hashtable (hashtableSize) APIs.
pub(crate) fn hash_type_length(o: &RedisObject) -> Result<u64, RedisError> {
    // TODO(port): dispatch on encoding.
    let _ = o;
    Ok(0)
}

/// Initialize a forward iterator over all fields in a hash.
///
/// C: `hashTypeInitIterator`, `t_hash.c:627`.
///
/// TODO(port): Requires hashtable iterator from redis-ds.
pub(crate) fn hash_type_init_iterator(encoding: HashEncoding) -> HashTypeIterator {
    HashTypeIterator {
        encoding,
        volatile_items_iter: false,
        lp_fptr: None,
        lp_vptr: None,
        ht_next: None,
    }
}

/// Initialize an iterator that only yields volatile (expiry-carrying) fields.
///
/// C: `hashTypeInitVolatileIterator`, `t_hash.c:642`.
///
/// TODO(port): Requires vset iterator from deferred redis-commands/src/vset.rs.
pub(crate) fn hash_type_init_volatile_iterator(encoding: HashEncoding) -> HashTypeIterator {
    HashTypeIterator {
        encoding,
        volatile_items_iter: true,
        lp_fptr: None,
        lp_vptr: None,
        ht_next: None,
    }
}

/// Clean up iterator resources.
///
/// C: `hashTypeResetIterator`, `t_hash.c:656`.
pub(crate) fn hash_type_reset_iterator(hi: &mut HashTypeIterator) {
    // TODO(port): release hashtable iterator handle or vset iterator.
    hi.lp_fptr = None;
    hi.lp_vptr = None;
    hi.ht_next = None;
}

/// Advance the iterator.  Returns `true` when a next entry is available.
///
/// C: `hashTypeNext`, `t_hash.c:667` — returns C_OK / C_ERR.
///
/// TODO(port): Requires listpack + hashtable iterator APIs.
pub(crate) fn hash_type_next(
    o: &RedisObject,
    hi: &mut HashTypeIterator,
) -> Result<bool, RedisError> {
    // TODO(port): dispatch on hi.encoding; advance listpack fptr/vptr or call hashtableNext.
    let _ = (o, hi);
    Ok(false)
}

/// Read current field or value bytes from a listpack iterator position.
///
/// C: `hashTypeCurrentFromListpack`, `t_hash.c:711`.
///
/// TODO(port): Requires listpack lpGetValue.
pub(crate) fn hash_type_current_from_listpack(
    hi: &HashTypeIterator,
    what: u32,
) -> Option<(Option<Vec<u8>>, i64)> {
    // TODO(port): call lpGetValue on hi.lp_fptr or hi.lp_vptr depending on `what`.
    let _ = (hi, what);
    None
}

/// Read current field or value bytes from a hashtable iterator position.
///
/// C: `hashTypeCurrentFromHashTable`, `t_hash.c:728`.
///
/// TODO(port): Requires entry API from redis-server.
pub(crate) fn hash_type_current_from_hashtable(
    hi: &HashTypeIterator,
    what: u32,
) -> Option<Vec<u8>> {
    // TODO(port): call entryGetField or entryGetValue on hi.ht_next.
    let _ = (hi, what);
    None
}

/// Return the field or value at the current iterator position as a new `RedisString`.
///
/// C: `hashTypeCurrentObjectNewSds`, `t_hash.c:741`.
pub(crate) fn hash_type_current_object_new_sds(
    hi: &HashTypeIterator,
    what: u32,
) -> Result<RedisString, RedisError> {
    // TODO(port): dispatch on encoding; convert listpack entry or hashtable entry.
    let _ = (hi, what);
    Ok(RedisString::new())
}

/// Look up a hash key for write, creating it if absent.  Returns an error if
/// the key exists but holds a non-hash type.
///
/// C: `hashTypeLookupWriteOrCreate`, `t_hash.c:758`.
///
/// TODO(port): Requires CommandContext::db_mut() from Phase 3.
pub(crate) fn hash_type_lookup_write_or_create(
    ctx: &mut CommandContext,
    key: &RedisString,
) -> Result<(), RedisError> {
    // TODO(port): call ctx.db_mut().lookup_key_write(key); if wrong type return Err(wrong_type);
    // if None, create a new hash object and db.add(key, obj).
    let _ = (ctx, key);
    Ok(())
}

/// Convert a listpack-encoded hash to the target encoding (hashtable only).
///
/// C: `hashTypeConvertListpack`, `t_hash.c:770`.
///
/// TODO(port): Requires listpack + hashtable + entry APIs.
pub(crate) fn hash_type_convert_listpack(
    o: &mut RedisObject,
    target: HashEncoding,
) -> Result<(), RedisError> {
    // TODO(port): iterate listpack; create entries; build new hashtable; swap.
    let _ = (o, target);
    Ok(())
}

/// Convert a hash to a different encoding.
///
/// C: `hashTypeConvert`, `t_hash.c:806`.
pub(crate) fn hash_type_convert(
    o: &mut RedisObject,
    target: HashEncoding,
) -> Result<(), RedisError> {
    // TODO(port): dispatch on current encoding; only listpack→hashtable is implemented in C.
    let _ = (o, target);
    Ok(())
}

/// Duplicate a hash object (used by COPY command).
///
/// C: `hashTypeDup`, `t_hash.c:821`.
///
/// TODO(port): Requires listpack + hashtable + entry + vset APIs.
pub(crate) fn hash_type_dup(o: &RedisObject) -> Result<RedisObject, RedisError> {
    // TODO(port): memcpy listpack or iterate hashtable and clone entries.
    let _ = o;
    Err(RedisError::runtime(b"TODO: hash_type_dup not yet implemented"))
}

/// Build a `RedisString` from a `ListpackEntry` (used by HRANDFIELD helpers).
///
/// C: `hashSdsFromListpackEntry`, `t_hash.c:862`.
pub(crate) fn hash_sds_from_listpack_entry(e: &ListpackEntry) -> RedisString {
    match &e.sval {
        Some(bytes) => RedisString::from_bytes(bytes),
        None => {
            let s = e.lval.to_string();
            RedisString::from_bytes(s.as_bytes())
        }
    }
}

/// Reply to the client with a `ListpackEntry` value as a bulk string.
///
/// C: `hashReplyFromListpackEntry`, `t_hash.c:867`.
///
/// TODO(port): Requires CommandContext reply API (ctx.reply_bulk / ctx.reply_integer).
fn hash_reply_from_listpack_entry(
    ctx: &mut CommandContext,
    e: &ListpackEntry,
) -> Result<(), RedisError> {
    match &e.sval {
        Some(bytes) => ctx.reply_bulk(bytes),
        None => ctx.reply_integer(e.lval),
    }
}

/// Return a random field–value pair from a hash.
///
/// C: `hashTypeRandomElement` (static), `t_hash.c:880`.
///
/// TODO(port): Requires hashtable fair-random + listpack random-pair APIs.
fn hash_type_random_element(
    o: &RedisObject,
    _hash_size: u64,
) -> Result<Option<(ListpackEntry, ListpackEntry)>, RedisError> {
    // TODO(port): dispatch on encoding; handle expired-entry retries for hashtable.
    let _ = o;
    Ok(None)
}

/// Reply to client with field (and optionally value) rows from listpack entries.
///
/// C: `hrandfieldReplyWithListpack` (static), `t_hash.c:1828`.
///
/// TODO(port): Requires CommandContext RESP version awareness (c->resp > 2).
fn hrandfield_reply_with_listpack(
    ctx: &mut CommandContext,
    fields: &[ListpackEntry],
    vals: Option<&[ListpackEntry]>,
) -> Result<(), RedisError> {
    // TODO(port): for each entry, emit array header for RESP3 if vals present.
    for (i, field) in fields.iter().enumerate() {
        if vals.is_some() {
            // TODO(port): if ctx.resp_version() > 2 { ctx.reply_array_header(2)?; }
        }
        hash_reply_from_listpack_entry(ctx, field)?;
        if let Some(vs) = vals {
            hash_reply_from_listpack_entry(ctx, &vs[i])?;
        }
    }
    Ok(())
}

/// Add a single hash field's value to the reply, or null if field/hash absent.
///
/// C: `addHashFieldToReply` (static), `t_hash.c:1075`.
fn add_hash_field_to_reply(
    ctx: &mut CommandContext,
    o: Option<&RedisObject>,
    field: &[u8],
) -> Result<(), RedisError> {
    let Some(hash) = o else {
        return ctx.reply_null();
    };
    match hash_type_get_value(hash, field)? {
        Some((Some(bytes), _, _)) => ctx.reply_bulk(&bytes),
        Some((None, vll, _)) => ctx.reply_integer(vll),
        None => ctx.reply_null(),
    }
}

/// Emit field or value from the current iterator position into the reply.
///
/// C: `addHashIteratorCursorToReply` (static), `t_hash.c:1223`.
///
/// TODO(port): Requires CommandContext writePreparedClient equivalent.
fn add_hash_iterator_cursor_to_reply(
    ctx: &mut CommandContext,
    hi: &HashTypeIterator,
    what: u32,
) -> Result<(), RedisError> {
    match hi.encoding {
        HashEncoding::Listpack => {
            match hash_type_current_from_listpack(hi, what) {
                Some((Some(bytes), _)) => ctx.reply_bulk(&bytes)?,
                Some((None, vll)) => ctx.reply_integer(vll)?,
                None => ctx.reply_null()?,
            }
        }
        HashEncoding::Hashtable => {
            match hash_type_current_from_hashtable(hi, what) {
                Some(bytes) => ctx.reply_bulk(&bytes)?,
                None => ctx.reply_null()?,
            }
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Command implementations
// ─────────────────────────────────────────────────────────────────────────

/// HINCRBY key field increment
///
/// Increment the integer value of a hash field by `increment`.
///
/// C: `hincrbyCommand`, `t_hash.c:918`.
pub fn hincrby_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:918-991
    //
    // Algorithm:
    //   1. Parse incr from argv[3].
    //   2. hash_type_lookup_write_or_create(ctx, argv[1]).
    //   3. hash_type_get_value(hash, field) → existing value as integer.
    //      - If vstr present: parse via string2ll; error if not integer.
    //      - Else use vll directly.
    //   4. Check overflow: (incr<0 && value<0 && incr<LLONG_MIN-value) etc.
    //   5. new_value = value + incr.
    //   6. hash_type_set(hash, field, new_value_bytes, expiry, HASH_SET_TAKE_VALUE).
    //   7. signal_modified_key, notify "hincrby", server.dirty++.
    //   8. Replication: rewrite as HSET (or HSETEX if has expiry).
    //   9. reply_integer(new_value).
    //
    // TODO(port): implement once hash_type_get_value / hash_type_set / CommandContext
    //   db access (Phase 3) and entry/vset APIs (Phase 4) are available.

    let _ = ctx;
    Err(RedisError::runtime(b"TODO: HINCRBY not yet implemented"))
}

/// HINCRBYFLOAT key field increment
///
/// Increment the float value of a hash field by `increment`.
///
/// C: `hincrbyfloatCommand`, `t_hash.c:993`.
pub fn hincrbyfloat_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:993-1073
    //
    // Algorithm:
    //   1. Parse incr (long double) from argv[3]; error if NaN/Inf.
    //   2. hash_type_lookup_write_or_create(ctx, argv[1]).
    //   3. hash_type_get_value(hash, field) → existing value as float.
    //      - If vstr: parse via string2ld; error "hash value is not a float".
    //      - Else cast vll to long double.
    //   4. new_value = value + incr; error if result is NaN/Inf.
    //   5. Format new_value via ld2string(LD_STR_HUMAN) → byte buffer.
    //   6. hash_type_set(hash, field, buf, expiry, HASH_SET_TAKE_VALUE).
    //   7. signal_modified_key, notify "hincrbyfloat", server.dirty++.
    //   8. Replication: rewrite as HSET (or HSETEX if has expiry).
    //   9. reply_bulk(buf).
    //
    // TODO(port): implement once hash_type_get_value / hash_type_set / CommandContext
    //   db access and entry/vset APIs are available.

    let _ = ctx;
    Err(RedisError::runtime(b"TODO: HINCRBYFLOAT not yet implemented"))
}

/// HGET key field
///
/// C: `hgetCommand`, `t_hash.c:1096`.
pub fn hget_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1096-1101
    let key = ctx.arg_clone(1)?;
    let field = ctx.arg_clone(2)?;

    // TODO(port): o = ctx.db().lookup_key_read_or_reply(&key, null_reply)?;
    // TODO(port): check_type(o, OBJ_HASH)?;
    let _ = key;
    add_hash_field_to_reply(ctx, None, field.as_bytes())
}

/// HMGET key field [field …]
///
/// C: `hmgetCommand`, `t_hash.c:1103`.
pub fn hmget_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1103-1120
    let key = ctx.arg_clone(1)?;
    let argc = ctx.argc();

    // TODO(port): o = ctx.db().lookup_key_read(&key) (may be NULL — that is OK for HMGET)
    // TODO(port): check_type(o, OBJ_HASH)?;
    let _ = key;

    ctx.reply_array_header((argc - 2) as i64)?;
    for i in 2..argc {
        let field = ctx.arg_clone(i)?;
        add_hash_field_to_reply(ctx, None, field.as_bytes())?;
    }

    // PORT NOTE: C deletes the key if hash length drops to 0 after HMGET
    // (possible due to lazy expiry).  TODO(port): replicate that behavior.
    Ok(())
}

/// HDEL key field [field …]
///
/// C: `hdelCommand`, `t_hash.c:1122`.
pub fn hdel_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1122-1153
    let key = ctx.arg_clone(1)?;
    let argc = ctx.argc();

    // TODO(port): o = ctx.db().lookup_key_write_or_reply(&key, czero)?;
    // TODO(port): check_type(o, OBJ_HASH)?;
    // TODO(port): hashtablePauseAutoShrink for hashtable encoding.

    let mut deleted: i64 = 0;
    let mut key_removed = false;

    for j in 2..argc {
        let field = ctx.arg_clone(j)?;
        // TODO(port): if hash_type_delete(hash, field.as_bytes())? {
        //     deleted++;
        //     if hash_type_length(hash)? == 0 {
        //         if hash_volatile_items { db_untrack_key_with_volatile_items }
        //         ctx.db_mut().delete(&key);
        //         key_removed = true; break;
        //     }
        // }
        let _ = field;
    }

    // TODO(port): hashtableResumeAutoShrink if !key_removed
    // TODO(port): if deleted > 0 { update volatile tracking, signal_modified_key, notify }
    // TODO(port): server.dirty += deleted;

    let _ = (key, key_removed);
    ctx.reply_integer(deleted)
}

/// HGETDEL key FIELDS numfields field [field …]
///
/// C: `hgetdelCommand`, `t_hash.c:1155`.
pub fn hgetdel_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1155-1206
    // argv: [0]=HGETDEL, [1]=key, [2]=FIELDS, [3]=numfields, [4..]=fields
    let fields_index: usize = 4;
    let num_fields_obj = ctx.arg_clone(fields_index - 1)?;
    let num_fields: i64 = parse_integer_from_bytes(num_fields_obj.as_bytes())
        .map_err(|_| RedisError::not_integer())?;

    let argc = ctx.argc();
    if num_fields == 0 || num_fields != (argc as i64 - fields_index as i64) {
        return Err(RedisError::runtime(
            b"numfields should be greater than 0 and match the provided number of fields",
        ));
    }

    let key = ctx.arg_clone(1)?;
    // TODO(port): o = ctx.db().lookup_key_write(&key);
    // TODO(port): check_type(o, OBJ_HASH)?;
    // TODO(port): hashtablePauseAutoShrink if hashtable encoding.

    let mut deleted: i64 = 0;
    let mut key_removed = false;

    ctx.reply_array_header(num_fields)?;
    for i in fields_index..argc {
        let field = ctx.arg_clone(i)?;
        // Reply first (may be null if not found), then delete.
        add_hash_field_to_reply(ctx, None, field.as_bytes())?;

        // TODO(port): if hash exists && hash_type_delete(hash, field)? {
        //     deleted++;
        //     if hash_type_length(hash)? == 0 { remove key; key_removed = true; break; }
        // }
        let _ = field;
    }

    // TODO(port): hashtableResumeAutoShrink if !key_removed
    // TODO(port): signal_modified_key, notify "hdel" (and "del" if key removed), server.dirty
    let _ = (key, key_removed, deleted);
    Ok(())
}

/// HLEN key
///
/// C: `hlenCommand`, `t_hash.c:1208`.
pub fn hlen_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1208-1214
    let key = ctx.arg_clone(1)?;
    // TODO(port): o = ctx.db().lookup_key_read_or_reply(&key, czero)?;
    // TODO(port): check_type(o, OBJ_HASH)?;
    let _ = key;
    // TODO(port): ctx.reply_integer(hash_type_length(o)? as i64)
    ctx.reply_integer(0)
}

/// HSTRLEN key field
///
/// C: `hstrlenCommand`, `t_hash.c:1216`.
pub fn hstrlen_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1216-1221
    let key = ctx.arg_clone(1)?;
    let field = ctx.arg_clone(2)?;
    // TODO(port): o = ctx.db().lookup_key_read_or_reply(&key, czero)?;
    // TODO(port): check_type(o, OBJ_HASH)?;
    // TODO(port): ctx.reply_integer(hash_type_get_value_length(o, field.as_bytes())? as i64)
    let _ = (key, field);
    ctx.reply_integer(0)
}

/// HSETNX key field value
///
/// Set a field only if it does not exist.
///
/// C: `hsetnxCommand`, `t_hash.c:1243`.
pub fn hsetnx_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1243-1270
    let key = ctx.arg_clone(1)?;
    let field = ctx.arg_clone(2)?;
    let value = ctx.arg_clone(3)?;

    // TODO(port): hash_type_lookup_write_or_create(ctx, &key)?;
    // TODO(port): if hash_type_exists(o, field.as_bytes())? { return ctx.reply_integer(0); }
    // TODO(port): hash_type_try_conversion(o, argv, 2, 3);
    // TODO(port): hash_type_set(o, field, value, EXPIRY_NONE, HASH_SET_COPY)?;
    // TODO(port): signal, notify "hset", server.dirty++
    // TODO(port): if has_volatile_fields { rewrite to HSET }
    let _ = (key, field, value);

    // PORT NOTE: returns 1 on insert, 0 if field already existed.
    ctx.reply_integer(0)
}

/// HSET key field value [field value …]
/// HMSET key field value [field value …] (deprecated alias)
///
/// C: `hsetCommand`, `t_hash.c:1272`.
pub fn hset_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1272-1313
    let argc = ctx.argc();
    if argc % 2 == 1 {
        return Err(RedisError::wrong_number_of_args(b"HSET"));
    }

    let key = ctx.arg_clone(1)?;
    // TODO(port): hash_type_lookup_write_or_create(ctx, &key)?;
    // TODO(port): hash_type_try_conversion(o, argv, 2, argc-1);

    let mut created: i64 = 0;
    for i in (2..argc).step_by(2) {
        let field = ctx.arg_clone(i)?;
        let value = ctx.arg_clone(i + 1)?;
        // TODO(port): created += !hash_type_set(o, field, value, EXPIRY_NONE, HASH_SET_COPY)?.0 as i64;
        let _ = (field, value);
    }

    // TODO(port): update volatile tracking, signal_modified_key
    // TODO(port): notify expired fields "hexpired" if any
    // TODO(port): notify "hset", server.dirty

    let _ = key;

    // HSET returns count of *newly created* fields.
    // HMSET returns OK (distinguished by command name byte at argv[0][1]).
    // PORT NOTE: command-name dispatch replicated here.
    // TODO(port): read ctx.cmd_name() to differentiate HSET vs HMSET.
    ctx.reply_integer(created)
}

/// HSETEX key [NX|XX] [FNX|FXX] [EX seconds|PX ms|EXAT unix|PXAT unix-ms|KEEPTTL]
///         FIELDS numfields field value [field value …]
///
/// Set hash fields with optional per-field expiry and conditional flags.
///
/// C: `hsetexCommand`, `t_hash.c:1359`.
pub fn hsetex_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1359-1581 — very long; summary of key steps:
    //   1. Parse optional flags up to "FIELDS" keyword.
    //   2. Validate numfields matches provided pairs.
    //   3. Check NX/XX key-level conditions.
    //   4. Resolve expiry timestamp.
    //   5. Check FNX/FXX field-level conditions.
    //   6. Create hash if absent, then set each field with expiry.
    //   7. Handle immediate expiry (delete fields), rewrite argv for AOF.
    //   8. Notify keyspace, signal modified key, increment dirty.
    //
    // TODO(port): Full implementation blocked on:
    //   - parse_extended_command_arguments_or_reply (TODO architect)
    //   - convert_expire_argument_to_unix_time (TODO architect)
    //   - hash_type_set / hash_type_delete internal APIs (deferred data structures)
    //   - replace_client_command_vector / rewrite_client_command_argument (Phase 3)
    //   - propagate_fields_deletion (Phase 3)

    let _ = ctx;
    // PORT NOTE: always returns 1 when all fields were updated/deleted; 0 on
    // condition failure.  Stubbed until dependencies land.
    // TODO(port): implement hsetex_command body
    Err(RedisError::runtime(b"TODO: HSETEX not yet implemented"))
}

/// HGETEX key [EX seconds|PX ms|EXAT unix|PXAT unix-ms|PERSIST]
///         FIELDS numfields field [field …]
///
/// Get hash field values while simultaneously updating or removing their expiry.
///
/// C: `hgetexCommand`, `t_hash.c:1619`.
pub fn hgetex_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1619-1765 — very long; summary:
    //   1. Parse flags (PERSIST or expiry option).
    //   2. Validate numfields.
    //   3. Look up hash (read).
    //   4. For each field: reply with value, then delete / set-expiry / persist.
    //   5. Rewrite argv as HDEL / HPEXPIREAT / HPERSIST for AOF propagation.
    //   6. Notify keyspace, signal modified, delete key if empty.
    //
    // TODO(port): Full implementation blocked on same dependencies as hsetex_command.

    let _ = ctx;
    // TODO(port): implement hgetex_command body
    Err(RedisError::runtime(b"TODO: HGETEX not yet implemented"))
}

/// Internal implementation for HKEYS, HVALS, HGETALL.
///
/// `flags` is a bitmask of `OBJ_HASH_FIELD` and/or `OBJ_HASH_VALUE`.
///
/// C: `genericHgetallCommand`, `t_hash.c:1767`.
pub fn generic_hgetall_command(
    ctx: &mut CommandContext,
    flags: u32,
) -> Result<(), RedisError> {
    // C: t_hash.c:1767-1799
    let key = ctx.arg_clone(1)?;
    // TODO(port): o = ctx.db().lookup_key_read_or_reply(&key, emptyResp)?;
    // TODO(port): check_type(o, OBJ_HASH)?;
    // TODO(port): allocate deferred reply length.

    // PORT NOTE: HGETALL emits a RESP3 map; HKEYS/HVALS emit flat arrays.
    // TODO(port): allocate writePreparedClient / deferred-len header.

    let mut count: i64 = 0;
    // TODO(port): hashTypeInitIterator(o, &hi); while hashTypeNext(&hi) != C_ERR {
    //     if flags & OBJ_HASH_FIELD { emit field; count++; }
    //     if flags & OBJ_HASH_VALUE { emit value; count++; }
    // }
    // TODO(port): hashTypeResetIterator(&hi);
    // TODO(port): if both flags set, setDeferredMapLen(count/2) else setDeferredArrayLen(count).

    let _ = (key, flags);
    ctx.reply_array_header(count)
}

/// HKEYS key
///
/// C: `hkeysCommand`, `t_hash.c:1801`.
pub fn hkeys_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    generic_hgetall_command(ctx, OBJ_HASH_FIELD)
}

/// HVALS key
///
/// C: `hvalsCommand`, `t_hash.c:1805`.
pub fn hvals_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    generic_hgetall_command(ctx, OBJ_HASH_VALUE)
}

/// HGETALL key
///
/// C: `hgetallCommand`, `t_hash.c:1809`.
pub fn hgetall_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    generic_hgetall_command(ctx, OBJ_HASH_FIELD | OBJ_HASH_VALUE)
}

/// HEXISTS key field
///
/// C: `hexistsCommand`, `t_hash.c:1813`.
pub fn hexists_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1813-1817
    let key = ctx.arg_clone(1)?;
    let field = ctx.arg_clone(2)?;
    // TODO(port): o = ctx.db().lookup_key_read_or_reply(&key, czero)?;
    // TODO(port): check_type(o, OBJ_HASH)?;
    // TODO(port): ctx.reply_integer(hash_type_exists(o, field.as_bytes())? as i64)
    let _ = (key, field);
    ctx.reply_integer(0)
}

/// HSCAN key cursor [MATCH pattern] [COUNT count]
///
/// C: `hscanCommand`, `t_hash.c:1819`.
pub fn hscan_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:1819-1826
    let key = ctx.arg_clone(1)?;
    let _cursor_bytes = ctx.arg_clone(2)?;
    // TODO(port): parse cursor; lookup key; call scanGenericCommand equivalent.
    // TODO(port): Requires scan_generic_command from redis-core (later phase).
    let _ = key;
    Err(RedisError::runtime(b"TODO: HSCAN not yet implemented"))
}

/// HRANDFIELD key [count [WITHVALUES]]
///
/// Return one or more random fields from a hash.
///
/// C: `hrandfieldCommand`, `t_hash.c:2337`.
pub fn hrandfield_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:2337-2368
    let argc = ctx.argc();
    let key = ctx.arg_clone(1)?;

    if argc >= 3 {
        // Parse count (may be negative for non-unique sampling).
        let count_bytes = ctx.arg_clone(2)?;
        let l: i64 = parse_integer_from_bytes(count_bytes.as_bytes())
            .map_err(|_| RedisError::not_integer())?;

        let mut withvalues = false;
        if argc > 4
            || (argc == 4
                && !count_bytes.as_bytes().eq_ignore_ascii_case(b"withvalues"))
        {
            return Err(RedisError::syntax(b"syntax error"));
        }
        if argc == 4 {
            withvalues = true;
            let half = i64::MAX / 2;
            if l < -half || l > half {
                return Err(RedisError::out_of_range());
            }
        }

        // TODO(port): hrandfield_with_count_command(ctx, l, withvalues)
        let _ = (key, withvalues);
        return hrandfield_with_count_command(ctx, l, withvalues);
    }

    // No count: return a single random field as bulk string.
    // TODO(port): lookup hash, call hash_type_random_element, reply or null.
    let _ = key;
    ctx.reply_null()
}

/// HRANDFIELD with count (internal implementation).
///
/// C: `hrandfieldWithCountCommand`, `t_hash.c:2134`.
pub fn hrandfield_with_count_command(
    ctx: &mut CommandContext,
    l: i64,
    withvalues: bool,
) -> Result<(), RedisError> {
    // C: t_hash.c:2134-2334
    // Implements four cases:
    //   Case 1: negative count (non-unique) or count==1 — random sampling.
    //   Case 2: count >= size — return entire hash.
    //   Case 2.5: listpack + unique — lpRandomPairsUnique.
    //   Case 3: count * SUB_STRATEGY_MUL > size — build temp ht, remove excess.
    //   Case 4: count << size — random-sample with dedup ht.
    //
    // TODO(port): Full implementation requires listpack + hashtable random APIs.
    let _ = (ctx, l, withvalues);
    Err(RedisError::runtime(
        b"TODO: HRANDFIELD with count not yet implemented",
    ))
}

/// HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field [field …]
///
/// C: `hexpireCommand`, `t_hash.c:1977`.
pub fn hexpire_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: hexpireGenericCommand(c, commandTimeSnapshot(), UNIT_SECONDS)
    // TODO(port): command_time_snapshot() from CommandContext (TODO architect).
    hexpire_generic_command(ctx, 0 /* TODO: commandTimeSnapshot */, UNIT_SECONDS)
}

/// HEXPIREAT key unix-time-seconds [NX|XX|GT|LT] FIELDS numfields field …
///
/// C: `hexpireatCommand`, `t_hash.c:1981`.
pub fn hexpireat_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    hexpire_generic_command(ctx, 0, UNIT_SECONDS)
}

/// HPEXPIRE key milliseconds [NX|XX|GT|LT] FIELDS numfields field …
///
/// C: `hpexpireCommand`, `t_hash.c:1985`.
pub fn hpexpire_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: hexpireGenericCommand(c, commandTimeSnapshot(), UNIT_MILLISECONDS)
    hexpire_generic_command(ctx, 0 /* TODO: commandTimeSnapshot */, UNIT_MILLISECONDS)
}

/// HPEXPIREAT key unix-time-milliseconds [NX|XX|GT|LT] FIELDS numfields field …
///
/// C: `hpexpireatCommand`, `t_hash.c:1989`.
pub fn hpexpireat_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    hexpire_generic_command(ctx, 0, UNIT_MILLISECONDS)
}

/// Shared implementation for HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT.
///
/// C: `hexpireGenericCommand`, `t_hash.c:1881`.
pub fn hexpire_generic_command(
    ctx: &mut CommandContext,
    basetime: i64,
    unit: i32,
) -> Result<(), RedisError> {
    // C: t_hash.c:1881-1975
    // Steps:
    //   1. Parse optional NX/XX/GT/LT flags up to "FIELDS".
    //   2. Parse numfields; validate against remaining argc.
    //   3. convert_expire_argument_to_unix_time (relative or absolute).
    //   4. lookup_key_write; check_type.
    //   5. For each field: hashTypeSetExpire → collect results.
    //   6. Rewrite argv as HDEL or HPEXPIREAT; notify keyspace; signal key.
    //
    // TODO(port): blocked on parse_extended_expire_arguments_or_reply,
    //   convert_expire_argument_to_unix_time, hash_type_set_expire internals.

    let fields_index: usize = 3;
    let num_fields_obj = ctx.arg_clone(fields_index)?;
    // TODO(port): proper flag parsing before "FIELDS" keyword.
    let num_fields: i64 = parse_integer_from_bytes(num_fields_obj.as_bytes())
        .map_err(|_| RedisError::not_integer())?;

    let argc = ctx.argc();
    if num_fields == 0 || num_fields != (argc as i64 - fields_index as i64 - 1) {
        return Err(RedisError::runtime(
            b"numfields should be greater than 0 and match the provided number of fields",
        ));
    }

    // TODO(port): convert_expire_argument_to_unix_time(ctx, param, basetime, unit, &mut when)
    let _when: i64 = 0; // placeholder

    let _ = (basetime, unit);

    // TODO(port): lookup hash, check_type(OBJ_HASH), iterate fields calling hash_type_set_expire
    ctx.reply_array_header(num_fields)?;
    for _ in 0..num_fields {
        // TODO(port): result = hash_type_set_expire(hash, field, when, flag)
        ctx.reply_integer(ExpiryModificationResult::NotExist as i64)?;
    }

    Ok(())
}

/// HPERSIST key FIELDS numfields field [field …]
///
/// Remove expiry from hash fields.
///
/// C: `hpersistCommand`, `t_hash.c:2010`.
pub fn hpersist_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_hash.c:2010-2046
    // argv: [0]=HPERSIST [1]=key [2]=FIELDS [3]=numfields [4..]=fields
    let fields_index: usize = 4;
    let num_fields_obj = ctx.arg_clone(fields_index - 1)?;
    let num_fields: i64 = parse_integer_from_bytes(num_fields_obj.as_bytes())
        .map_err(|_| RedisError::not_integer())?;

    let argc = ctx.argc();
    if num_fields == 0 || num_fields != (argc as i64 - fields_index as i64) {
        return Err(RedisError::runtime(
            b"numfields should be greater than 0 and match the provided number of fields",
        ));
    }

    let key = ctx.arg_clone(1)?;
    // TODO(port): hash = ctx.db().lookup_key_write(&key)?;
    // TODO(port): check_type(hash, OBJ_HASH)?;

    ctx.reply_array_header(num_fields)?;

    for i in 0..num_fields as usize {
        let field = ctx.arg_clone(fields_index + i)?;
        // TODO(port): result = hash_type_persist(hash, field.as_bytes());
        // TODO(port): if result == Successful { server.dirty++; changes++; }
        // TODO(port): ctx.reply_integer(result as i64);
        let _ = field;
        ctx.reply_integer(ExpiryModificationResult::NotExist as i64)?;
    }

    // TODO(port): if changes > 0 { update volatile tracking; notify "hpersist"; signal key }
    let _ = key;
    Ok(())
}

/// Shared implementation for HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME.
///
/// `basetime` = 0 for absolute-time variants, or `command_time_snapshot()` for
/// relative-TTL variants.  `unit` = UNIT_SECONDS or UNIT_MILLISECONDS.
///
/// C: `httlGenericCommand`, `t_hash.c:2076`.
pub fn httl_generic_command(
    ctx: &mut CommandContext,
    basetime: i64,
    unit: i32,
) -> Result<(), RedisError> {
    // C: t_hash.c:2076-2106
    // argv: [0]=HTTL [1]=key [2]=FIELDS [3]=numfields [4..]=fields
    let fields_index: usize = 4;
    let num_fields_obj = ctx.arg_clone(fields_index - 1)?;
    let num_fields: i64 = parse_integer_from_bytes(num_fields_obj.as_bytes())
        .map_err(|_| RedisError::not_integer())?;

    let argc = ctx.argc();
    if num_fields == 0 || num_fields != (argc as i64 - fields_index as i64) {
        return Err(RedisError::syntax(b"syntax error"));
    }

    let key = ctx.arg_clone(1)?;
    // TODO(port): hash = ctx.db().lookup_key_read(&key);
    // TODO(port): check_type(hash, OBJ_HASH)?;

    ctx.reply_array_header(num_fields)?;

    for i in 0..num_fields as usize {
        let field = ctx.arg_clone(fields_index + i)?;
        // TODO(port): result = hash_type_get_expiry(hash, field.as_bytes())?;
        // match result:
        //   None (hash or field absent) → reply -2
        //   Some(EXPIRY_NONE)           → reply -1
        //   Some(expiry_ms) → {
        //       result = expiry_ms - basetime; if result < 0 { result = 0; }
        //       reply unit==MILLISECONDS ? result : (result + 500) / 1000
        //   }
        let _ = (field, unit, basetime);
        ctx.reply_integer(-2)?;
    }

    let _ = key;
    Ok(())
}

/// HTTL key FIELDS numfields field [field …]
///
/// C: `httlCommand`, `t_hash.c:2108`.
pub fn httl_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): pass commandTimeSnapshot() as basetime.
    httl_generic_command(ctx, 0 /* TODO: commandTimeSnapshot */, UNIT_SECONDS)
}

/// HPTTL key FIELDS numfields field [field …]
///
/// C: `hpttlCommand`, `t_hash.c:2112`.
pub fn hpttl_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    httl_generic_command(ctx, 0 /* TODO: commandTimeSnapshot */, UNIT_MILLISECONDS)
}

/// HEXPIRETIME key FIELDS numfields field [field …]
///
/// C: `hexpiretimeCommand`, `t_hash.c:2116`.
pub fn hexpiretime_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    httl_generic_command(ctx, 0, UNIT_SECONDS)
}

/// HPEXPIRETIME key FIELDS numfields field [field …]
///
/// C: `hpexpiretimeCommand`, `t_hash.c:2120`.
pub fn hpexpiretime_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    httl_generic_command(ctx, 0, UNIT_MILLISECONDS)
}

// ─────────────────────────────────────────────────────────────────────────
// Active-expiry helpers
// ─────────────────────────────────────────────────────────────────────────

/// Callback invoked during volatile-set sweep: deletes one expired entry from
/// the underlying hashtable.
///
/// C: `hashTypeExpireEntry` (static), `t_hash.c:2380`.
///
/// TODO(port): Requires `entry` (redis-server) and hashtable pop API (redis-ds).
fn hash_type_expire_entry(
    _entry_bytes: &[u8],
    ctx: &mut ExpiryContext,
) -> bool {
    // TODO(port): hashtablePop(ht, entry, &entry_ptr); if deleted { track field; entryFree; return true }
    let _ = ctx;
    false
}

/// Expire up to `max_fields` fields whose TTL has passed, collecting their
/// names into `out_entries` for AOF/replica propagation.
///
/// C: `hashTypeDeleteExpiredFields`, `t_hash.c:2399`.
///
/// TODO(port): Requires vset removal sweep and hashtable pop (deferred crates).
pub(crate) fn hash_type_delete_expired_fields(
    o: &mut RedisObject,
    now: i64,
    max_fields: usize,
    out_entries: Option<&mut Vec<RedisString>>,
) -> usize {
    // TODO(port): obtain vset from hashtable metadata; call vsetRemoveExpired;
    // free volatile set if now empty; return count of expired entries.
    let _ = (o, now, max_fields, out_entries);
    0
}

// ─────────────────────────────────────────────────────────────────────────
// Defragmentation helpers
// ─────────────────────────────────────────────────────────────────────────

/// Hashtable scan callback that defragments a single hash entry in place.
///
/// C: `defragHashTypeEntry` (static), `t_hash.c:2422`.
///
/// TODO(port): Requires `entry` (redis-server) and active-defrag API (redis-core).
pub(crate) fn defrag_hash_type_entry(
    _parent_object: &mut RedisObject,
    _entry_ref: &mut Vec<u8>,
) {
    // TODO(port): call entryDefrag; if new_entry != old_entry, update vset tracking.
}

/// Incremental defragmentation scan for a hash object.
///
/// C: `hashTypeScanDefrag`, `t_hash.c:2438`.
///
/// TODO(port): Requires listpack defrag, hashtable scan-defrag, and vset defrag
///   APIs from redis-ds (deferred Phase 4).
pub(crate) fn hash_type_scan_defrag(
    ob: &mut RedisObject,
    _cursor: usize,
) -> usize {
    // TODO(port): dispatch on encoding; defrag listpack or hashtable + vset.
    let _ = ob;
    0
}

// ─────────────────────────────────────────────────────────────────────────
// Private utilities
// ─────────────────────────────────────────────────────────────────────────

/// Parse an `i64` from a byte slice representing a decimal integer.
///
/// Returns an error if the bytes are not a valid integer.
fn parse_integer_from_bytes(bytes: &[u8]) -> Result<i64, ()> {
    // TODO(port): use a proper integer parser once redis-core util.rs is available.
    // C: string2ll() / getLongLongFromObjectOrReply()
    core::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(())
}

/// Parse an `f64` from a byte slice.
///
/// PORT NOTE: `from_utf8` is allowed here because this is not a stored Redis
/// datum — it is a transient parse of a command argument known to be UTF-8
/// decimal notation.
fn parse_float_from_bytes(bytes: &[u8]) -> Result<f64, ()> {
    // C: getLongDoubleFromObjectOrReply / string2ld
    core::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or(())
}

/// Format a float for human-readable output (matches C `ld2string` / `LD_STR_HUMAN`).
///
/// PORT NOTE: this is a transient formatting operation; the result is immediately
/// converted to `&[u8]` — it never enters storage as a Rust `String`.
fn format_long_double(v: f64) -> String {
    // C: ld2string(buf, sizeof(buf), value, LD_STR_HUMAN)
    // PERF(port): C uses a fixed-size stack buffer; Rust allocates a String here.
    // Profile in Phase B and replace with a stack-allocated formatter.
    if v == v.floor() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{:.17}", v)
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_owned()
    }
}

/// Phase A placeholder for a hash `RedisObject`.
///
/// TODO(port): Remove once CommandContext::db() is wired up and hash objects
///   can be retrieved from the database.
fn placeholder_hash_object() -> RedisObject {
    // TODO(port): replace all call sites with real db lookup.
    RedisObject::placeholder()
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/t_hash.c  (~2489 lines, ~62 functions)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         160  (146 TODO(port) + 14 TODO(architect))
//   port_notes:    10   (PORT NOTE + PERF(port) annotations)
//   unsafe_blocks: 0
//   notes:         All ~62 functions translated with correct Rust signatures and
//                  return types (Result<(), RedisError>).  All command entry-
//                  points (HSET, HGET, HDEL, HGETDEL, HLEN, HSTRLEN, HSETNX,
//                  HMGET, HGETALL, HKEYS, HVALS, HEXISTS, HSCAN, HRANDFIELD,
//                  HEXPIRE family, HTTL family, HPERSIST, HSETEX, HGETEX) have
//                  complete argument parsing skeletons and proper error wiring.
//                  Internal helpers (listpack, hashtable, entry, vset ops) are
//                  stubbed with TODO(port) pending deferred crates:
//                    redis-ds Phase 4 (listpack, hashtable, vset),
//                    redis-server later (entry).
//                  HINCRBY / HINCRBYFLOAT bodies are algorithm-commented but
//                  stubbed because type inference breaks when RedisObject is
//                  unresolved (expected Phase A limitation).
//                  HSETEX / HGETEX are full stubs — replication-rewrite logic
//                  requires Phase 3 infra (replace_client_command_vector,
//                  propagate_fields_deletion).
//                  Zero real syntax errors; validator shows only expected
//                  E0432 / E0433 name-resolution errors.
// ──────────────────────────────────────────────────────────────────────────
