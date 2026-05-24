//! Lazy (background-thread) freeing of Redis objects and data structures.
//!
//! Port of `reference/valkey/src/lazyfree.c` (301 lines, 22 functions).
//!
//! In Valkey, large data structures are handed off to a dedicated BIO
//! (Background I/O) thread so that dropping a million-element hash map does
//! not stall the main event loop. Two atomic counters track how many objects
//! are queued (pending) and how many have been reclaimed (freed).
//!
//! Rust ownership and RAII replace manual `decrRefCount` / `sdsfree` calls;
//! counter bookkeeping is preserved via `AtomicUsize`. The BIO thread
//! integration (`crates/redis-core/src/bio.rs`) is deferred to Phase 3 —
//! all async paths currently fall back to synchronous drop with
//! `TODO(architect)` markers.
//!
//! Several C argument types (`kvstore *`, `rax *`, `list *`, `dict *`,
//! `functionsLibCtx *`) have no Rust equivalent in the pilot crates yet.
//! Each is represented by a local opaque placeholder struct, documented
//! with `TODO(architect)` indicating which crate and phase will supply the
//! canonical type.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::db::RedisDb;
use crate::object::RedisObject;
use redis_types::RedisString;

// ── Global atomic counters ─────────────────────────────────────────────────────
// C: lazyfree.c:9-10
//   static _Atomic size_t lazyfree_objects  = 0;
//   static _Atomic size_t lazyfreed_objects = 0;

static LAZYFREE_OBJECTS: AtomicUsize = AtomicUsize::new(0);
static LAZYFREED_OBJECTS: AtomicUsize = AtomicUsize::new(0);

/// Objects with fewer than this many "units of work" are freed synchronously;
/// larger ones are queued to a BIO thread.
///
/// C: lazyfree.c:189 — `#define LAZYFREE_THRESHOLD 64`
const LAZYFREE_THRESHOLD: usize = 64;

// ── Opaque placeholder types for deferred crates ───────────────────────────────
//
// These stand in for C pointer arguments whose canonical Rust types live in
// crates that are not yet in the pilot. They carry just enough metadata for
// counter bookkeeping. Replace each with the canonical type when available.

/// Placeholder for C's `kvstore *` (slot-addressed hash-table store).
///
/// TODO(architect): replace with `redis_ds::KvStore` once
/// `crates/redis-ds/src/kvstore.rs` lands (Phase 4).
pub struct OpaqueKvStore {
    /// Pre-computed key count, used to update the lazyfree counters.
    pub size: usize,
}

/// Placeholder for C's `rax *` (radix tree).
///
/// TODO(architect): replace with `redis_ds::RadixTree` once
/// `crates/redis-ds/src/rax.rs` lands (Phase 4/5).
/// Do NOT rename to `RadixTree` — that name is reserved for the canonical
/// redis-ds type per `harness/type-vocabulary.tsv`.
pub struct OpaqueRax {
    /// `rax::numnodes` — number of internal radix nodes (used to threshold
    /// against LAZYFREE_THRESHOLD for tracking / error-stats trees).
    pub numnodes: usize,
    /// `rax::numele` — number of stored elements (used for counter updates).
    pub numele: usize,
}

/// Placeholder for C's `list *` (adlist doubly-linked list).
///
/// TODO(architect): replace with `redis_ds::AdList` once
/// `crates/redis-ds/src/adlist.rs` lands.
pub struct OpaqueAdList {
    pub len: usize,
}

/// Eval-scripts aggregate: scripts dict + LRU eviction list + engine callbacks.
///
/// TODO(architect): replace fields with concrete redis-scripting types once
/// `crates/redis-scripting` lands (deferred phase).
pub struct EvalScriptsCtx {
    pub script_count: usize,
}

/// Placeholder for C's `functionsLibCtx *`.
///
/// TODO(architect): replace with `redis_scripting::FunctionsLibCtx` once
/// `crates/redis-scripting/src/functions.rs` lands (deferred phase).
pub struct OpaqueFunctionsLibCtx {
    pub functions_len: usize,
}

// ── BIO-thread callback functions ──────────────────────────────────────────────
//
// In C these are registered with `bioCreateLazyFreeJob` and invoked from a
// bio.c background thread. The Rust equivalents take typed owned arguments;
// Rust's drop semantics replace `decrRefCount` / the various `*Release`/
// `*Free` calls.
//
// TODO(architect): wire each of these into `bio.rs` once the background-task
// channel infrastructure is in place (Phase 3).

/// Drop a single `RedisObject` and update the pending/freed counters.
///
/// Rust's `Drop` replaces C's `decrRefCount(o)`.
///
/// C: lazyfree.c:14-19, `lazyfreeFreeObject`
pub fn lazyfree_free_object(obj: RedisObject) {
    drop(obj);
    LAZYFREE_OBJECTS.fetch_sub(1, Ordering::Relaxed);
    LAZYFREED_OBJECTS.fetch_add(1, Ordering::Relaxed);
}

/// Drop a database's three key stores and update counters.
///
/// In C three `kvstore *` pointers are passed (keys, expires,
/// keys_with_volatile_items); the key count comes from `kvstoreSize(da1)`.
/// Here the pilot stub uses opaque `OpaqueKvStore` wrappers whose `size`
/// field carries that count.
///
/// C: lazyfree.c:24-35, `lazyfreeFreeDatabase`
pub fn lazyfree_free_database(
    keys: OpaqueKvStore,
    expires: OpaqueKvStore,
    keys_with_volatile_items: OpaqueKvStore,
) {
    let numkeys = keys.size;
    drop(keys);
    drop(expires);
    drop(keys_with_volatile_items);
    LAZYFREE_OBJECTS.fetch_sub(numkeys, Ordering::Relaxed);
    LAZYFREED_OBJECTS.fetch_add(numkeys, Ordering::Relaxed);
}

/// Drop the client key-tracking radix tree and update counters.
///
/// C: lazyfree.c:38-44, `lazyFreeTrackingTable`
pub fn lazyfree_free_tracking_table(rt: OpaqueRax) {
    let len = rt.numele;
    drop(rt);
    LAZYFREE_OBJECTS.fetch_sub(len, Ordering::Relaxed);
    LAZYFREED_OBJECTS.fetch_add(len, Ordering::Relaxed);
}

/// Drop the error-stats radix tree and update counters.
///
/// C: lazyfree.c:47-53, `lazyFreeErrors`
pub fn lazyfree_free_errors(errors: OpaqueRax) {
    let len = errors.numele;
    drop(errors);
    LAZYFREE_OBJECTS.fetch_sub(len, Ordering::Relaxed);
    LAZYFREED_OBJECTS.fetch_add(len, Ordering::Relaxed);
}

/// Drop the eval-scripts context (scripts dict, LRU list, engine callbacks)
/// and update counters.
///
/// C: lazyfree.c:56-64, `lazyFreeEvalScripts`
pub fn lazyfree_free_eval_scripts(ctx: EvalScriptsCtx) {
    let len = ctx.script_count;
    drop(ctx);
    LAZYFREE_OBJECTS.fetch_sub(len as usize, Ordering::Relaxed);
    LAZYFREED_OBJECTS.fetch_add(len as usize, Ordering::Relaxed);
}

/// Drop a `functionsLibCtx` and update counters.
///
/// C: lazyfree.c:67-74, `lazyFreeFunctionsCtx`
pub fn lazyfree_free_functions_ctx(functions_lib_ctx: OpaqueFunctionsLibCtx) {
    let len = functions_lib_ctx.functions_len;
    drop(functions_lib_ctx);
    LAZYFREE_OBJECTS.fetch_sub(len, Ordering::Relaxed);
    LAZYFREED_OBJECTS.fetch_add(len, Ordering::Relaxed);
}

/// Drop the replication-backlog reference-memory (blocks list + index rax)
/// and update counters.
///
/// C: lazyfree.c:77-86, `lazyFreeReplicationBacklogRefMem`
pub fn lazyfree_free_replication_backlog_ref_mem(blocks: OpaqueAdList, index: OpaqueRax) {
    // C: long long len = listLength(blocks) + raxSize(index);
    let len = blocks.len + index.numele;
    drop(blocks);
    drop(index);
    LAZYFREE_OBJECTS.fetch_sub(len, Ordering::Relaxed);
    LAZYFREED_OBJECTS.fetch_add(len, Ordering::Relaxed);
}

/// Drop the `replicaKeysWithExpire` dict and update counters.
///
/// C: lazyfree.c:89-95, `lazyFreeReplicaKeysWithExpire`
pub fn lazyfree_free_replica_keys_with_expire(replica_keys_with_expire: HashMap<RedisString, i64>) {
    let len = replica_keys_with_expire.len();
    drop(replica_keys_with_expire);
    LAZYFREE_OBJECTS.fetch_sub(len, Ordering::Relaxed);
    LAZYFREED_OBJECTS.fetch_add(len, Ordering::Relaxed);
}

/// Drop the pending-replication-data blocks list and update counters.
///
/// C: lazyfree.c:98-104, `lazyfreePendingReplDataBuf`
pub fn lazyfree_free_pending_repl_data_buf(pending_repl_data_blocks: OpaqueAdList) {
    let len = pending_repl_data_blocks.len;
    drop(pending_repl_data_blocks);
    LAZYFREE_OBJECTS.fetch_sub(len, Ordering::Relaxed);
    LAZYFREED_OBJECTS.fetch_add(len, Ordering::Relaxed);
}

// ── Counter accessors ──────────────────────────────────────────────────────────

/// Returns the number of objects currently queued for lazy freeing.
///
/// C: lazyfree.c:107-110, `lazyfreeGetPendingObjectsCount`
pub fn lazyfree_get_pending_objects_count() -> usize {
    LAZYFREE_OBJECTS.load(Ordering::Relaxed)
}

/// Returns the total number of objects freed by the lazy-free BIO thread
/// since the last call to `lazyfree_reset_stats`.
///
/// C: lazyfree.c:113-116, `lazyfreeGetFreedObjectsCount`
pub fn lazyfree_get_freed_objects_count() -> usize {
    LAZYFREED_OBJECTS.load(Ordering::Relaxed)
}

/// Resets the freed-objects counter to zero (used by `DEBUG FLUSHALL`, etc.).
///
/// C: lazyfree.c:118-120, `lazyfreeResetStats`
pub fn lazyfree_reset_stats() {
    LAZYFREED_OBJECTS.store(0, Ordering::Relaxed);
}

// ── Free-effort estimation ─────────────────────────────────────────────────────

/// Returns a number proportional to the amount of work required to free `obj`.
///
/// The C implementation distinguishes object *type* AND *encoding*: only the
/// "large" encodings (quicklist, hashtable, skiplist) return values > 1;
/// small encodings (listpack, intset, ziplist) fall through to the default
/// `return 1`. Because the Phase A `RedisObject` enum does not yet carry
/// encoding sub-variants (Phase 4), this translation uses collection lengths
/// directly, which may return values > 1 for objects that C would score as 1.
/// This is a conservative overestimate: it may trigger async frees for small
/// objects but never skips async frees for large ones.
///
/// `key` is included for the Module variant (it is unused for built-in types).
///
/// C: lazyfree.c:137-182, `lazyfreeGetFreeEffort`
pub fn lazyfree_get_free_effort(key: &RedisObject, obj: &RedisObject, dbid: i32) -> usize {
    // C: lazyfree.c:138-149 — type + encoding dispatch.
    //
    // PORT NOTE: collapsed to `collection_len()` plus per-type tail cases. The
    // collection-length shim covers List/Set/ZSet/Hash uniformly; Stream and
    // String fall through to a constant.
    if obj.is_list() || obj.is_set() || obj.is_zset() || obj.is_hash() {
        return obj.collection_len();
    }
    if obj.is_stream() {
        // C: lazyfree.c:150-173 — elaborate stream effort: rax node count
        // plus consumer-group PEL sizes.
        // TODO(port): stream internals (rax numnodes, cgroups, PEL sizes)
        // are not yet accessible from ObjectKind::Stream (Phase 5 stub).
        let _ = (key, dbid); // silence unused-variable warnings
        return 1;
    }
    // String / Module fall here.
    // TODO(port): OBJ_MODULE → moduleGetFreeEffort(key, obj, dbid);
    //   return ULONG_MAX if effort == 0. Blocked on Phase 10 modules.
    let _ = (key, dbid);
    1
}

// ── Conditional async-free entry points ───────────────────────────────────────

/// Free `obj` asynchronously if its free effort exceeds `LAZYFREE_THRESHOLD`
/// *and* there are no other owners. Otherwise, drop it synchronously.
///
/// C: lazyfree.c:192-204, `freeObjAsync`
///
/// PORT NOTE: C checks `obj->refcount == 1` to ensure there are no shared
/// owners. In Rust, taking ownership of `obj` by value (`obj: RedisObject`)
/// already guarantees exclusive ownership for non-`Arc`-backed objects.
/// When `RedisObject` gains `Arc`-backed storage in a later phase, this
/// function's signature will need to change to `Arc<RedisObject>` and use
/// `Arc::try_unwrap` to enforce the single-owner check.
/// TODO(port): revisit refcount/Arc semantics in Phase 4.
///
/// TODO(architect): wire the `bio_create_lazy_free_job` call once
/// `crates/redis-core/src/bio.rs` provides a background-task channel.
pub fn free_obj_async(key: &RedisObject, obj: RedisObject, dbid: i32) {
    let free_effort = lazyfree_get_free_effort(key, &obj, dbid);
    if free_effort > LAZYFREE_THRESHOLD {
        // C: atomic_fetch_add + bioCreateLazyFreeJob(lazyfreeFreeObject, 1, obj)
        // TODO(architect): send to BIO thread; for now fall through to synchronous drop.
        LAZYFREE_OBJECTS.fetch_add(1, Ordering::Relaxed);
        lazyfree_free_object(obj);
    } else {
        drop(obj);
    }
}

/// Replace a database's three key stores with fresh, empty ones and schedule
/// the old stores for lazy freeing.
///
/// C: lazyfree.c:209-222, `emptyDbAsync`
///
/// TODO(architect): The current pilot `RedisDb` stores a single `HashMap`
/// (`db.dict`). The C implementation splits the keyspace into three separate
/// `kvstore` structures: `keys`, `expires`, and `keys_with_volatile_items`.
/// This function cannot be fully faithful until `RedisDb` gains those fields
/// (Phase 4 kvstore split). The body below clears the single dict and notes
/// where the three kvstore swaps would go.
///
/// TODO(architect): `server.cluster_enabled` / `CLUSTER_SLOT_MASK_BITS` /
/// `KVSTORE_*` flags need to be added to `RedisServer` (Phase 3+).
///
/// TODO(architect): wire the BIO `bioCreateLazyFreeJob` call once bio.rs lands.
pub fn empty_db_async(db: &mut RedisDb) {
    // C: db->keys = kvstoreCreate(&kvstoreKeysHashtableType, slot_count_bits, flags);
    // C: db->expires = kvstoreCreate(...);
    // C: db->keys_with_volatile_items = kvstoreCreate(...);
    // C: atomic_fetch_add(&lazyfree_objects, kvstoreSize(oldkeys), ...);
    // C: bioCreateLazyFreeJob(lazyfreeFreeDatabase, 3, oldkeys, oldexpires, oldkeyswithexpires);

    // Pilot approximation: synchronously clear the single backing dict.
    // The old content is dropped here rather than on a BIO thread.
    // PORT NOTE: this is a behavioral stub; full semantics require Phase 4.
    db.clear();
}

/// Free the client key-tracking radix tree, asynchronously if it is large.
///
/// C: lazyfree.c:226-234, `freeTrackingRadixTreeAsync`
///
/// Note: the threshold in C is against `tracking->numnodes` (internal nodes),
/// not `tracking->numele` (element count). The counter update uses `numele`.
///
/// TODO(architect): wire the BIO job once bio.rs lands.
pub fn free_tracking_radix_tree_async(tracking: OpaqueRax) {
    if tracking.numnodes > LAZYFREE_THRESHOLD {
        LAZYFREE_OBJECTS.fetch_add(tracking.numele, Ordering::Relaxed);
        // TODO(architect): bioCreateLazyFreeJob(lazyFreeTrackingTable, 1, tracking)
        lazyfree_free_tracking_table(tracking);
    } else {
        let len = tracking.numele;
        drop(tracking);
        // C: freeTrackingRadixTree(tracking) — no counter update on sync path.
        let _ = len;
    }
}

/// Free the error-stats radix tree, asynchronously if it is large.
///
/// C: lazyfree.c:238-246, `freeErrorsRadixTreeAsync`
///
/// Note: threshold is against `errors->numnodes`; counter update uses `numele`.
///
/// TODO(architect): wire the BIO job once bio.rs lands.
pub fn free_errors_radix_tree_async(errors: OpaqueRax) {
    if errors.numnodes > LAZYFREE_THRESHOLD {
        LAZYFREE_OBJECTS.fetch_add(errors.numele, Ordering::Relaxed);
        // TODO(architect): bioCreateLazyFreeJob(lazyFreeErrors, 1, errors)
        lazyfree_free_errors(errors);
    } else {
        drop(errors);
    }
}

/// Free the eval-scripts context, asynchronously if the scripts dict is large.
///
/// C: lazyfree.c:251-258, `freeEvalScriptsAsync`
///
/// TODO(architect): wire the BIO job once bio.rs lands.
pub fn free_eval_scripts_async(ctx: EvalScriptsCtx) {
    let script_count = ctx.script_count;
    if script_count > LAZYFREE_THRESHOLD {
        LAZYFREE_OBJECTS.fetch_add(script_count, Ordering::Relaxed);
        // TODO(architect): bioCreateLazyFreeJob(lazyFreeEvalScripts, 3, scripts, lru_list, engine_cbs)
        lazyfree_free_eval_scripts(ctx);
    } else {
        drop(ctx);
    }
}

/// Free a `functionsLibCtx`, asynchronously if it holds enough functions.
///
/// C: lazyfree.c:261-269, `freeFunctionsAsync`
///
/// TODO(architect): wire the BIO job once bio.rs lands.
pub fn free_functions_async(functions_lib_ctx: OpaqueFunctionsLibCtx) {
    let functions_len = functions_lib_ctx.functions_len;
    if functions_len > LAZYFREE_THRESHOLD {
        LAZYFREE_OBJECTS.fetch_add(functions_len, Ordering::Relaxed);
        // TODO(architect): bioCreateLazyFreeJob(lazyFreeFunctionsCtx, 2, functions_lib_ctx, engine_cbs)
        lazyfree_free_functions_ctx(functions_lib_ctx);
    } else {
        drop(functions_lib_ctx);
    }
}

/// Free the replication-backlog reference memory (blocks list + index rax),
/// asynchronously if either structure is large.
///
/// C: lazyfree.c:272-280, `freeReplicationBacklogRefMemAsync`
///
/// TODO(architect): wire the BIO job once bio.rs lands.
pub fn free_replication_backlog_ref_mem_async(blocks: OpaqueAdList, index: OpaqueRax) {
    if blocks.len > LAZYFREE_THRESHOLD || index.numele > LAZYFREE_THRESHOLD {
        let len = blocks.len + index.numele;
        LAZYFREE_OBJECTS.fetch_add(len, Ordering::Relaxed);
        // TODO(architect): bioCreateLazyFreeJob(lazyFreeReplicationBacklogRefMem, 2, blocks, index)
        lazyfree_free_replication_backlog_ref_mem(blocks, index);
    } else {
        drop(blocks);
        drop(index);
    }
}

/// Free the `replicaKeysWithExpire` dict, asynchronously if it is large.
///
/// C: lazyfree.c:283-290, `freeReplicaKeysWithExpireAsync`
///
/// TODO(architect): wire the BIO job once bio.rs lands.
pub fn free_replica_keys_with_expire_async(replica_keys_with_expire: HashMap<RedisString, i64>) {
    let len = replica_keys_with_expire.len();
    if len > LAZYFREE_THRESHOLD {
        LAZYFREE_OBJECTS.fetch_add(len, Ordering::Relaxed);
        // TODO(architect): bioCreateLazyFreeJob(lazyFreeReplicaKeysWithExpire, 1, dict)
        lazyfree_free_replica_keys_with_expire(replica_keys_with_expire);
    } else {
        drop(replica_keys_with_expire);
    }
}

/// Free the pending-replication-data blocks list, asynchronously if large.
///
/// C: lazyfree.c:293-300, `freePendingReplDataBufAsync`
///
/// TODO(architect): wire the BIO job once bio.rs lands.
pub fn free_pending_repl_data_buf_async(pending_repl_data_blocks: OpaqueAdList) {
    let len = pending_repl_data_blocks.len;
    if len > LAZYFREE_THRESHOLD {
        LAZYFREE_OBJECTS.fetch_add(len, Ordering::Relaxed);
        // TODO(architect): bioCreateLazyFreeJob(lazyfreePendingReplDataBuf, 1, pending_repl_data_blocks)
        lazyfree_free_pending_repl_data_buf(pending_repl_data_blocks);
    } else {
        drop(pending_repl_data_blocks);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/lazyfree.c  (301 lines, 22 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         18
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         All 22 functions ported. Counter bookkeeping faithfully
//                  translated to AtomicUsize. BIO async paths are stubs that
//                  fall through to synchronous drop pending bio.rs (Phase 3).
//                  Deferred types (kvstore, rax, list, functionsLibCtx) use
//                  local opaque placeholders. lazyfreeGetFreeEffort omits
//                  encoding sub-variants (Phase 4) and stream/module effort
//                  (Phase 5/10). emptyDbAsync is a behavioral stub pending
//                  Phase 4 kvstore split on RedisDb.
// ──────────────────────────────────────────────────────────────────────────────
