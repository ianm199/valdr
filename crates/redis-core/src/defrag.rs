//! Active memory defragmentation.
// Deferred feature: active-defrag engine; waiting on jemalloc internal extensions
// per architecture decision. All items here are faithful ports, not dead code.
#![allow(dead_code, private_interfaces)]
//!
//! Port of `src/defrag.c` (1357 lines, ~40 functions).
//!
//! # Overview
//!
//! Active defragmentation scans the keyspace and asks the allocator whether
//! each live allocation should be relocated to a fresher, less-fragmented
//! region.  When the answer is yes, the data is copied to a fresh allocation,
//! every pointer to the old location is updated, and the old block is freed.
//!
//! Defrag runs as an event-loop timer (`active_defrag_time_proc`).  It works
//! through a queue of **stages**, each responsible for scanning one logical
//! slice of server state (main key-store, expire store, pubsub, Lua, modules).
//!
//! # Phase A limitations
//!
//! * **Allocator introspection** — all `allocator_*` helpers are no-op stubs.
//!   See `TODO(architect)` below.
//! * **Deferred data structures** — kvstore, quicklist, rax, zset, stream, and
//!   hashtable functions are stubs; they return immediately without scanning.
//!   These will be filled in when `redis-ds` crate is available.
//! * **Global state** — `DefragContext` lives in a `thread_local!` for Phase A.
//!   It should be moved to `RedisServer` before Phase B.
//! * **Event-loop integration** — timer registration / de-registration is a
//!   no-op stub; requires Phase 2 event-loop port.
//! * **Server stats** — hit/miss/scanned counters are placeholders; the real
//!   fields live on `RedisServer` (not yet threaded through).
//!
//! # TODO(architect): allocator introspection
//!
//! The C defrag engine requires jemalloc extensions:
//! `allocatorShouldDefrag`, `allocatorDefragAlloc`, `allocatorDefragFree`,
//! `zmalloc_size`, `getAllocatorFragmentation`.  In Rust these require
//! the unstable `allocator_api` feature plus a defrag-aware allocator
//! (e.g. `tikv-jemallocator`).  Define a `DefragAllocator` trait in
//! `redis-types` and thread it through `RedisServer` before Phase B.
//!
//! # TODO(architect): move DefragContext into RedisServer
//!
//! `DEFRAG_CTX` is currently a `thread_local!`.  Move it into `RedisServer`
//! as `pub defrag: Option<DefragContext>` so the event-loop callback receives
//! it through `&mut RedisServer` with no global state.
//!
//! # TODO(architect): event-loop integration
//!
//! `begin_defrag_cycle` must call `aeCreateTimeEvent(server.el, 0, callback)`.
//! `end_defrag_cycle` must call `aeDeleteTimeEvent(server.el, timeproc_id)`.
//! Resolve when `crates/redis-core/src/event_loop.rs` is ported (Phase 2).

#![allow(dead_code, unused_variables, unused_mut, unused_assignments)]

use crate::db::RedisDb;
use crate::object::RedisObject;
use redis_types::RedisString;
use std::collections::VecDeque;

// ── Constants ──────────────────────────────────────────────────────────────────

/// Event-loop timer sentinel: timer was deleted or never created.
/// C: `AE_DELETED_EVENT_ID` (ae.h)
const AE_DELETED_EVENT_ID: i64 = -1;

/// Event-loop return value: do not reschedule this timer.
/// C: `AE_NOMORE` (ae.h)
const AE_NOMORE: i64 = -1;

/// Kvstore-pass sentinel: defrag the lookup-table structures before slot iteration.
/// C: `KVS_SLOT_DEFRAG_LUT (-2)`
const KVS_SLOT_DEFRAG_LUT: i32 = -2;

/// Kvstore-pass sentinel: no slot is currently assigned.
/// C: `KVS_SLOT_UNASSIGNED (-1)`
const KVS_SLOT_UNASSIGNED: i32 = -1;

// ── Type aliases ───────────────────────────────────────────────────────────────

/// Monotonic timestamp in microseconds.
/// C: `monotime` (uint64_t, from `getMonotonicUs()`)
pub type Monotime = u64;

/// A defrag stage closure.
///
/// In C, stages are `defragStageFn(endtime, void *target, void *privdata)`.
/// In Rust, `target` and `privdata` are captured by the closure.
///
/// Contract: calling with `endtime == 0` initialises the stage (clears internal
/// state, returns `NotDone`).  Calling with `endtime > 0` does real work until
/// the deadline or completion.
pub type StageFnBox = Box<dyn FnMut(Monotime) -> DoneStatus>;

/// Channel-accessor function type (placeholder).
/// C: `typedef hashtable *(*getClientChannelsFn)(client *)`
///
/// TODO(architect): replace placeholder signature once `Client` and the
/// pubsub hashtable type are available.
pub(crate) type GetClientChannelsFn = fn() -> ();

// ── Enums ──────────────────────────────────────────────────────────────────────

/// Whether a defrag stage has finished its work for this invocation.
/// C: `doneStatus { DEFRAG_NOT_DONE = 0, DEFRAG_DONE = 1 }`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoneStatus {
    NotDone = 0,
    Done = 1,
}

/// Value type stored in a dict being defrag-scanned.
/// C: `DEFRAG_SDS_DICT_NO_VAL`, `_VAL_IS_SDS`, `_VAL_IS_STROB`,
///    `_VAL_VOID_PTR`, `_VAL_LUA_SCRIPT`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SdsDictValType {
    NoVal = 0,
    IsSds = 1,
    IsStrob = 2,
    VoidPtr = 3,
    LuaScript = 4,
}

// ── Structs ────────────────────────────────────────────────────────────────────

/// Descriptor for one defrag stage.
/// C: `StageDescriptor { defragStageFn stage_fn; void *target; void *privdata; }`
///
/// In Rust, `target` and `privdata` are captured by the closure.
pub(crate) struct StageDescriptor {
    pub stage_fn: StageFnBox,
}

impl std::fmt::Debug for StageDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StageDescriptor")
            .field("stage_fn", &"<closure>")
            .finish()
    }
}

/// Kvstore iteration state, persisted across timer-proc invocations.
/// C: `kvstoreIterState { kvstore *kvs; int slot; unsigned long cursor; }`
///
/// In C, this struct MUST begin at offset 0 within the `defragKeysCtx` and
/// `defragPubSubCtx` structs (verified via `static_assert`).  In Rust, this
/// is guaranteed because `kvstate` is the first field of those structs.
///
/// TODO(architect): replace `kvs_id: usize` with a real `KvStore` handle
/// from `redis-ds` when Phase 3 lands.
#[derive(Debug, Default, Clone)]
pub(crate) struct KvstoreIterState {
    /// Token identifying which kvstore is being iterated.
    /// Used to detect flushdb/swapdb changes between calls.
    pub kvs_id: usize,
    pub slot: i32,
    pub cursor: u64,
}

impl KvstoreIterState {
    fn new_for_kvs(kvs_id: usize) -> Self {
        Self {
            kvs_id,
            slot: KVS_SLOT_DEFRAG_LUT,
            cursor: 0,
        }
    }
}

/// Private data for the main-keys defrag stage.
/// C: `defragKeysCtx { kvstoreIterState kvstate; int dbid; }`
#[derive(Debug, Default, Clone)]
pub(crate) struct DefragKeysCtx {
    pub kvstate: KvstoreIterState,
    pub dbid: usize,
}

/// Private data for a pubsub channels defrag stage.
/// C: `defragPubSubCtx { kvstoreIterState kvstate; getClientChannelsFn getPubSubChannels; }`
#[derive(Clone)]
pub(crate) struct DefragPubSubCtx {
    pub kvstate: KvstoreIterState,
    pub get_pubsub_channels: GetClientChannelsFn,
}

/// Global defrag context.
/// C: `static struct DefragContext defrag;` plus module-level statics
/// `defrag_later` and `defrag_later_cursor`.
///
/// Static-local variables from individual C functions are promoted to fields
/// here to avoid `static mut`:
/// - `kvstore_iter_state`: from `static kvstoreIterState state` in
///   `defragStageKvstoreHelper`
/// - `prev_cpu_percent`: from `static int prevCpuPercent` in
///   `computeDefragCycleUs`
/// - `defrag_later` / `defrag_later_cursor`: from module-level C statics
#[derive(Debug)]
pub struct DefragContext {
    /// µs when the current cycle started. C: `start_cycle`
    pub start_cycle: Monotime,
    /// `stat_active_defrag_hits` at cycle start. C: `start_defrag_hits`
    pub start_defrag_hits: i64,
    /// Stages waiting to execute. C: `remaining_stages` (list*)
    pub remaining_stages: VecDeque<StageDescriptor>,
    /// Stage currently executing. C: `current_stage`
    pub current_stage: Option<StageDescriptor>,
    /// Event-loop timer ID, or `AE_DELETED_EVENT_ID`. C: `timeproc_id`
    pub timeproc_id: i64,
    /// End-µs of the previous timeproc call. C: `timeproc_end_time`
    pub timeproc_end_time: Monotime,
    /// Accumulated CPU-overage in µs. C: `timeproc_overage_us`
    pub timeproc_overage_us: i64,
    /// Persisted kvstore-iteration state. C: `static kvstoreIterState state`
    pub kvstore_iter_state: KvstoreIterState,
    /// Last-seen CPU-target %; detects config changes. C: `static int prevCpuPercent`
    pub prev_cpu_percent: i32,
    /// Keys deferred for large-item processing. C: `static list *defrag_later`
    pub defrag_later: VecDeque<RedisString>,
    /// Cursor within the current deferred item. C: `static unsigned long defrag_later_cursor`
    pub defrag_later_cursor: u64,
}

impl DefragContext {
    pub fn new() -> Self {
        Self {
            start_cycle: 0,
            start_defrag_hits: 0,
            remaining_stages: VecDeque::new(),
            current_stage: None,
            timeproc_id: AE_DELETED_EVENT_ID,
            timeproc_end_time: 0,
            timeproc_overage_us: 0,
            kvstore_iter_state: KvstoreIterState::default(),
            prev_cpu_percent: 0,
            defrag_later: VecDeque::new(),
            defrag_later_cursor: 0,
        }
    }

    /// Whether a defrag cycle is currently running.
    /// C: `defragIsRunning() { return defrag.timeproc_id > 0; }`
    pub fn is_running(&self) -> bool {
        self.timeproc_id > 0
    }
}

impl Default for DefragContext {
    fn default() -> Self {
        Self::new()
    }
}

// ── Global defrag singleton ────────────────────────────────────────────────────

// Module-level defrag state.
// C: `static struct DefragContext defrag;`
//
// TODO(architect): move into `RedisServer` as `pub defrag: Option<DefragContext>`
// so the event-loop callback receives it via `&mut RedisServer`.
thread_local! {
    pub static DEFRAG_CTX: std::cell::RefCell<DefragContext> =
        std::cell::RefCell::new(DefragContext::new());
}

// ── Allocator introspection stubs ──────────────────────────────────────────────
//
// TODO(architect): the helpers below wrap jemalloc custom defrag extensions.
// In Rust they require `allocator_api` (nightly) + a defrag-aware allocator.
// All return "no relocation needed" in Phase A.

/// C: `allocatorShouldDefrag(ptr)` — should this allocation be relocated?
#[inline(always)]
fn allocator_should_defrag() -> bool {
    false
}

/// C: `allocatorDefragAlloc(size)` — fresh allocation outside the thread cache.
fn allocator_defrag_alloc(_size: usize) -> Option<Vec<u8>> {
    None
}

/// C: `allocatorDefragFree(ptr, size)` — free the old allocation.
fn allocator_defrag_free(_data: Vec<u8>) {}

/// C: `zmalloc_size(ptr)` — allocator-tracked allocation size.
fn zmalloc_size() -> usize {
    0
}

/// C: `getAllocatorFragmentation(&frag_bytes)` — fragmentation ratio and bytes.
fn get_allocator_fragmentation() -> (f32, usize) {
    (0.0, 0)
}

// ── Monotonic time helpers ─────────────────────────────────────────────────────

/// C: `getMonotonicUs()` → monotime (µs).
///
/// TODO(architect): replace with `crates/redis-core/src/monotonic.rs` once ported.
/// PERF(port): `SystemTime::now()` is not monotonic; Phase B must use a real clock.
fn get_monotonic_us() -> Monotime {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// C: `elapsedMs(start)` — elapsed ms since `start`.
fn elapsed_ms(start: Monotime) -> i64 {
    (get_monotonic_us().saturating_sub(start) / 1_000) as i64
}

// ── Server-stat / process helpers (stubs) ─────────────────────────────────────

/// C: `server.stat_active_defrag_hits`
/// TODO(architect): wire to `RedisServer` stats field.
fn server_stat_defrag_hits() -> i64 {
    0
}

/// C: `server.stat_active_defrag_scanned`
/// TODO(architect): wire to `RedisServer` stats field.
fn server_stat_defrag_scanned() -> u64 {
    0
}

/// C: `hasActiveChildProcess()` — bgsave/bgrewrite/etc. in progress.
/// TODO(architect): wire to `RedisServer`.
fn has_active_child_process() -> bool {
    false
}

// ── Generic allocation defrag ──────────────────────────────────────────────────

/// Try to defrag an allocation without freeing the old block.
/// Caller must free the old block when `Some` is returned.
///
/// C: `activeDefragAllocWithoutFree(ptr, *allocation_size)` — src/defrag.c:163
///
/// TODO(architect): allocator introspection required; always returns `None`.
fn active_defrag_alloc_without_free(_data: &[u8]) -> Option<Vec<u8>> {
    None
}

/// Defrag a generic byte allocation.
/// Returns `Some(new_data)` if relocated; old data must NOT be accessed.
///
/// C: `activeDefragAlloc(ptr)` — src/defrag.c:187
///
/// TODO(architect): allocator introspection required; always returns `None`.
pub fn active_defrag_alloc(_data: Vec<u8>) -> Option<Vec<u8>> {
    None
}

/// Defrag a `RedisString` (C: `sds`).
/// Returns `Some(new_string)` if relocated; old string must NOT be accessed.
///
/// C: `activeDefragSds(sdsptr)` — src/defrag.c:199
///
/// PORT NOTE: In C, `sds` is a header + data block; defrag adjusts the
/// internal header-offset pointer inside the new block.  In Rust,
/// `RedisString` owns a `Vec<u8>` with no such offset; defrag still makes
/// sense (moving the heap block) but requires allocator introspection.
///
/// TODO(architect): allocator introspection required; always returns `None`.
pub fn active_defrag_sds(_s: RedisString) -> Option<RedisString> {
    None
}

// ── Object-level defrag ────────────────────────────────────────────────────────

/// Try to defrag a `RedisObject` without freeing the old block.
/// C: `activeDefragStringObWithoutFree(ob, *size)` — src/defrag.c:214
/// TODO(architect): allocator introspection required; always returns `None`.
fn active_defrag_string_ob_without_free(_ob: &RedisObject) -> Option<RedisObject> {
    None
}

/// Defrag a string-type `RedisObject`.
/// Returns `Some(new_obj)` if relocated; old obj must NOT be accessed.
///
/// C: `activeDefragStringOb(ob)` — src/defrag.c:231
///
/// PERF(port): C checks `ob->refcount != 1` to skip shared objects.
/// In Rust, `RedisObject` is owned and never shared by reference count.
///
/// TODO(architect): allocator introspection required; always returns `None`.
pub fn active_defrag_string_ob(_ob: &RedisObject) -> Option<RedisObject> {
    None
}

// ── Sorted-set / skiplist defrag (Phase 4 stubs) ──────────────────────────────

/// Update skiplist forward/backward pointers after a node relocation.
/// C: `zslUpdateNode` — src/defrag.c:240
/// TODO(port): Phase 4 — `zskiplist`/`zskiplistNode` not yet defined.
fn zsl_update_node_after_defrag() {}

/// Hashtable scan callback: defrag one skiplist node.
/// C: `activeDefragZsetNode` — src/defrag.c:258
/// TODO(port): Phase 4 — ZSet types not yet defined.
fn active_defrag_zset_node() {}

// ── Dict / hashtable defrag stubs ─────────────────────────────────────────────

/// Scan callback: defrag a dict entry (struct + key + value).
/// C: `activeDefragDictCallback` — src/defrag.c:308
/// TODO(port): `dict`/`dictEntry` (redis-ds) not yet available.
fn active_defrag_dict_callback() {}

/// Defrag a dict with `sds` keys and optionally-typed values.
/// C: `activeDefragSdsDict(d, val_type)` — src/defrag.c:334
/// TODO(port): `dict` (redis-ds) not yet available.
fn active_defrag_sds_dict(_val_type: SdsDictValType) {}

/// Hashtable scan callback: defrag the `sds` value of each entry.
/// C: `activeDefragSdsHashtableCallback` — src/defrag.c:349
/// TODO(port): `hashtable` (redis-ds) not yet available.
fn active_defrag_sds_hashtable_callback() {}

// ── QuickList defrag stubs ─────────────────────────────────────────────────────

/// Defrag a single quicklist node.
/// C: `activeDefragQuickListNode` — src/defrag.c:357
/// TODO(port): `quicklist`/`quicklistNode` (redis-ds) not yet available.
fn active_defrag_quick_list_node() {}

/// Defrag all nodes of a quicklist.
/// C: `activeDefragQuickListNodes` — src/defrag.c:374
/// TODO(port): `quicklist` (redis-ds) not yet available.
fn active_defrag_quick_list_nodes() {}

// ── Deferred (large-object) queue ─────────────────────────────────────────────

/// Queue a large object's key for deferred defrag to avoid a latency spike.
/// C: `defragLater(obj)` — src/defrag.c:385
///
/// C: `sds key = sdsdup(objectGetKey(obj)); listAddNodeTail(defrag_later, key);`
///
/// PORT NOTE: In C, `robj` carries its kvstore key via `objectGetKey`.
/// `RedisObject` does not carry its own key.  The key is passed separately here.
/// TODO(architect): decide whether to add a key field to `RedisObject` or always
/// pass the key alongside the object at call sites.
fn defrag_later(defrag_ctx: &mut DefragContext, key: RedisString) {
    defrag_ctx.defrag_later.push_back(key);
}

// ── Large-item scan continuations ─────────────────────────────────────────────

/// Continue defragging a large quicklist-encoded list.
/// Returns `true` if time expired and more work remains.
///
/// C: `scanLaterList(ob, *cursor, endtime)` — src/defrag.c:396
/// TODO(port): `quicklist` (redis-ds) not yet available.
fn scan_later_list(_ob: &mut RedisObject, cursor: &mut u64, _endtime: Monotime) -> bool {
    *cursor = 0;
    false
}

/// Continue defragging a large skiplist-encoded sorted set.
/// C: `scanLaterZset(ob, *cursor)` — src/defrag.c:441
/// TODO(port): Phase 4 — ZSet types not yet defined.
fn scan_later_zset(_ob: &mut RedisObject, cursor: &mut u64) {
    *cursor = 0;
}

/// Hashtable scan callback that only bumps `stat_active_defrag_scanned`.
/// C: `scanHashtableCallbackCountScanned` — src/defrag.c:449
/// TODO(port): `hashtable` (redis-ds) not yet available.
fn scan_hashtable_callback_count_scanned() {}

/// Continue defragging a large hashtable-encoded set.
/// C: `scanLaterSet(ob, *cursor)` — src/defrag.c:455
/// TODO(port): `hashtable` (redis-ds) not yet available.
fn scan_later_set(_ob: &mut RedisObject, cursor: &mut u64) {
    *cursor = 0;
}

/// Continue defragging a large hashtable-encoded hash.
/// C: `scanLaterHash(ob, *cursor)` — src/defrag.c:461
/// TODO(port): `hashTypeScanDefrag` / `hashtable` (redis-ds) not yet available.
fn scan_later_hash(_ob: &mut RedisObject, cursor: &mut u64) {
    *cursor = 0;
}

// ── Type-specific defrag ───────────────────────────────────────────────────────

/// Defrag a quicklist-encoded list object.
/// C: `defragQuicklist(ob)` — src/defrag.c:466
/// TODO(port): `quicklist` (redis-ds) not yet available.
fn defrag_quicklist(_defrag_ctx: &mut DefragContext, _ob: &mut RedisObject) {}

/// Defrag a skiplist-encoded sorted set.
/// C: `defragZsetSkiplist(ob)` — src/defrag.c:479
/// TODO(port): Phase 4 — `zset`/`zskiplist`/`hashtable` not yet available.
fn defrag_zset_skiplist(_defrag_ctx: &mut DefragContext, _ob: &mut RedisObject) {}

/// Defrag a hash object (hashtable- or listpack-encoded).
/// C: `defragHash(ob)` — src/defrag.c:509
/// TODO(port): `hashTypeScanDefrag` / `hashtable` (redis-ds) not yet available.
fn defrag_hash(_defrag_ctx: &mut DefragContext, _ob: &mut RedisObject) {}

/// Defrag a hashtable-encoded set.
/// C: `defragSet(ob)` — src/defrag.c:524
/// TODO(port): `hashtable` (redis-ds) not yet available.
fn defrag_set(_defrag_ctx: &mut DefragContext, _ob: &mut RedisObject) {}

// ── Radix-tree / stream defrag (Phase 4/5 stubs) ──────────────────────────────

/// Defrag a single rax node.
/// C: `defragRaxNode(noderef)` — src/defrag.c:542
/// TODO(port): Phase 4/5 — `rax`/`raxNode` (RadixTree, redis-ds) not yet available.
fn defrag_rax_node() -> bool {
    false
}

/// Continue defragging a stream's listpack entries.
/// Returns `true` if time expired with more work remaining.
///
/// C: `scanLaterStreamListpacks(ob, *cursor, endtime)` — src/defrag.c:552
///
/// PORT NOTE: C uses `static unsigned char last[sizeof(streamID)]` inside this
/// function to remember the last-processed stream ID across calls.  In Rust,
/// that state would need to live in `DefragContext` or a captured closure.
///
/// TODO(port): Phase 5 — `stream`/`streamID`/`raxIterator` not yet defined.
fn scan_later_stream_listpacks(
    _ob: &mut RedisObject,
    cursor: &mut u64,
    _endtime: Monotime,
) -> bool {
    *cursor = 0;
    false
}

/// Defrag a radix tree (struct + nodes + optional per-element data + callback).
/// C: `defragRadixTree(raxref, defrag_data, element_cb, element_cb_data)` — src/defrag.c:607
/// TODO(port): Phase 4/5 — `rax` (RadixTree, redis-ds) not yet available.
fn defrag_radix_tree() {}

/// Defrag a stream consumer's pending-entry NACKs.
/// C: `defragStreamConsumerPendingEntry(ri, privdata)` — src/defrag.c:630
/// TODO(port): Phase 5 — `streamNACK`/`raxInsert` not yet available.
fn defrag_stream_consumer_pending_entry() {}

/// Defrag a stream consumer struct.
/// C: `defragStreamConsumer(ri, privdata)` — src/defrag.c:644
/// TODO(port): Phase 5 — `streamConsumer` not yet available.
fn defrag_stream_consumer() {}

/// Defrag a stream consumer group.
/// C: `defragStreamConsumerGroup(ri, privdata)` — src/defrag.c:660
/// TODO(port): Phase 5 — `streamCG` not yet available.
fn defrag_stream_consumer_group() {}

/// Defrag a stream object.
/// C: `defragStream(ob)` — src/defrag.c:668
/// TODO(port): Phase 5 — stream types not yet available.
fn defrag_stream(_defrag_ctx: &mut DefragContext, _ob: &mut RedisObject) {}

/// Defrag a module object.
/// C: `defragModule(db, obj)` — src/defrag.c:691
/// TODO(port): Phase 10 — `moduleDefragValue` not yet available.
fn defrag_module(_defrag_ctx: &mut DefragContext, _db: &mut RedisDb, _ob: &mut RedisObject) {}

// ── defragKey — type-dispatch defrag ──────────────────────────────────────────

/// For each key scanned in the main dict, attempt to defrag all its pointers.
///
/// C: `defragKey(ctx, elemref)` — src/defrag.c:704-767
fn defrag_key(defrag_ctx: &mut DefragContext, db: &mut RedisDb, ob: &mut RedisObject) {
    // C: Try to defrag the robj struct itself via activeDefragStringOb.
    // In Rust: meaningful only with allocator introspection.
    // TODO(architect): allocator introspection required for actual relocation.

    // C: After potentially relocating ob, update the expire table pointer if the
    //    object has an expiry, and the keys_with_volatile_items table if the object
    //    is a hash with volatile fields.
    // TODO(port): expire/volatile-item table pointer updates require kvstore hashtable.

    if ob.is_string() {
        // C: Already handled in activeDefragStringOb; nothing further to do.
    } else if ob.is_list() {
        // C: OBJ_ENCODING_QUICKLIST → defragQuicklist(ob)
        //    OBJ_ENCODING_LISTPACK  → activeDefragAlloc(listpack ptr)
        // PORT NOTE: encoding sub-variants now modelled on ObjectKind::List but
        // the defrag work still treats them uniformly.
        defrag_quicklist(defrag_ctx, ob);
    } else if ob.is_set() {
        // C: OBJ_ENCODING_HASHTABLE     → defragSet(ob)
        //    OBJ_ENCODING_INTSET
        //    | OBJ_ENCODING_LISTPACK    → activeDefragAlloc(ptr)
        defrag_set(defrag_ctx, ob);
    } else if ob.is_zset() {
        // C: OBJ_ENCODING_LISTPACK  → activeDefragAlloc(listpack ptr)
        //    OBJ_ENCODING_SKIPLIST  → defragZsetSkiplist(ob)
        defrag_zset_skiplist(defrag_ctx, ob);
    } else if ob.is_hash() {
        defrag_hash(defrag_ctx, ob);
    } else if ob.is_stream() {
        defrag_stream(defrag_ctx, ob);
    }
    // TODO(port): OBJ_MODULE → defrag_module(defrag_ctx, db, ob) — Phase 10
}

// ── Scan callbacks ─────────────────────────────────────────────────────────────

/// Defrag scan callback for the main db key dictionary.
///
/// C: `dbKeysScanCallback(privdata, elemref)` — src/defrag.c:770-778
///
/// TODO(port): per-key hit/miss stats (`stat_active_defrag_key_hits`, etc.)
/// require `&mut RedisServer`; thread through when the signature stabilises.
fn db_keys_scan_callback(defrag_ctx: &mut DefragContext, db: &mut RedisDb, ob: &mut RedisObject) {
    // C: long long hits_before = server.stat_active_defrag_hits;
    defrag_key(defrag_ctx, db, ob);
    // C: if (hits != hits_before) stat_active_defrag_key_hits++ else key_misses++
    // C: stat_active_defrag_scanned++
    // TODO(port): stat updates require &mut RedisServer.
}

/// Defrag scan callback for a pubsub channels hashtable.
///
/// C: `defragPubsubScanCallback(privdata, elemref)` — src/defrag.c:781-812
///
/// TODO(port): client channel hashtable / `hashtableReplaceReallocatedEntry`
/// not yet available.
fn defrag_pubsub_scan_callback(_ctx: &mut DefragPubSubCtx) {}

// ── defragLaterItem ────────────────────────────────────────────────────────────

/// Continue defragging one deferred large object.
/// Returns `true` if time expired and the item is not yet complete.
///
/// C: `defragLaterItem(ob, *cursor, endtime, dbid)` — src/defrag.c:816-843
fn defrag_later_item(
    ob: Option<&mut RedisObject>,
    cursor: &mut u64,
    endtime: Monotime,
    _dbid: usize,
) -> bool {
    let Some(ob) = ob else {
        *cursor = 0; // C: object deleted; reset cursor and continue
        return false;
    };
    if ob.is_list() {
        scan_later_list(ob, cursor, endtime)
    } else if ob.is_set() {
        scan_later_set(ob, cursor);
        false
    } else if ob.is_zset() {
        scan_later_zset(ob, cursor);
        false
    } else if ob.is_hash() {
        scan_later_hash(ob, cursor);
        false
    } else if ob.is_stream() {
        scan_later_stream_listpacks(ob, cursor, endtime)
    } else {
        // TODO(port): OBJ_MODULE → moduleLateDefrag(key, ob, cursor, endtime, dbid) — Phase 10
        // C: type/encoding may have changed since scheduling; just reset.
        *cursor = 0;
        false
    }
}

// ── defragLaterStep ────────────────────────────────────────────────────────────

/// Process deferred large objects before continuing the kvstore scan.
///
/// C: `defragLaterStep(endtime, privdata)` — src/defrag.c:847-886
///
/// This is a `kvstoreHelperPreContinueFn`:
///   `endtime == 0` → init: return `NotDone` immediately.
///   `endtime > 0`  → drain `defrag_later` until empty or time expires.
///
/// TODO(port): `kvstoreHashtableFind(kvs, slot, key, &found)` — key→object
/// lookup before calling `defrag_later_item` — not yet available (redis-ds).
/// For Phase A, items are consumed without the actual lookup/defrag.
fn defrag_later_step(defrag_ctx: &mut DefragContext, endtime: Monotime) -> DoneStatus {
    if endtime == 0 {
        return DoneStatus::NotDone; // init: per stage-function contract
    }

    let mut iterations: u32 = 0;
    let mut prev_defragged = server_stat_defrag_hits();
    let mut prev_scanned = server_stat_defrag_scanned();

    while !defrag_ctx.defrag_later.is_empty() {
        // C: key = listFirst(defrag_later)->value;
        //    kvstoreHashtableFind(kvs, slot, key, &found);
        //    defragLaterItem(found, &defrag_later_cursor, endtime, dbid);
        // TODO(port): kvstoreHashtableFind not yet available.
        // Placeholder: treat front item as processed immediately.
        let timed_out = false;

        if timed_out {
            break;
        }

        if defrag_ctx.defrag_later_cursor == 0 {
            // C: item finished; move to next
            defrag_ctx.defrag_later.pop_front();
        }

        iterations += 1;
        if iterations > 16
            || server_stat_defrag_hits() > prev_defragged
            || server_stat_defrag_scanned().wrapping_sub(prev_scanned) > 64
        {
            if get_monotonic_us() > endtime {
                break;
            }
            iterations = 0;
            prev_defragged = server_stat_defrag_hits();
            prev_scanned = server_stat_defrag_scanned();
        }
    }

    if defrag_ctx.defrag_later.is_empty() {
        DoneStatus::Done
    } else {
        DoneStatus::NotDone
    }
}

// ── defragStageKvstoreHelper ───────────────────────────────────────────────────

/// Iterate over one kvstore, scanning every slot for defrag opportunities.
///
/// C: `defragStageKvstoreHelper(endtime, kvs, scan_fn, precontinue_fn, privdata)`
///    — src/defrag.c:892-955
///
/// The C version uses a `static kvstoreIterState state` inside this function.
/// In Rust, that state lives in `DefragContext::kvstore_iter_state`.
///
/// `endtime == 0` → initialise state; return `NotDone`.
/// `endtime > 0`  → scan until deadline or all slots exhausted.
///
/// TODO(port): All kvstore iteration functions
/// (`kvstoreHashtableDefragTables`, `kvstoreHashtableScanDefrag`,
/// `kvstoreGetFirstNonEmptyHashtableIndex`, `kvstoreGetNextNonEmptyHashtableIndex`)
/// are not yet available (redis-ds, deferred).  The control-flow skeleton is
/// faithfully translated; the actual scan calls are no-op placeholders.
fn defrag_stage_kvstore_helper(
    defrag_ctx: &mut DefragContext,
    kvs_id: usize,
    endtime: Monotime,
) -> DoneStatus {
    // C: if (endtime == 0) { state.kvs = kvs; state.slot = KVS_SLOT_DEFRAG_LUT; ... }
    if endtime == 0 {
        defrag_ctx.kvstore_iter_state = KvstoreIterState::new_for_kvs(kvs_id);
        return DoneStatus::NotDone;
    }

    // C: if (kvs != state.kvs) return DEFRAG_DONE; (flushdb/swapdb changed the store)
    if kvs_id != defrag_ctx.kvstore_iter_state.kvs_id {
        return DoneStatus::Done;
    }

    // C: Phase 1 — defrag the main hashtable struct / bucket arrays before iterating.
    if defrag_ctx.kvstore_iter_state.slot == KVS_SLOT_DEFRAG_LUT {
        loop {
            // C: state.cursor = kvstoreHashtableDefragTables(kvs, state.cursor, activeDefragAlloc)
            // TODO(port): kvstoreHashtableDefragTables not yet available.
            defrag_ctx.kvstore_iter_state.cursor = 0; // placeholder: LUT pass done immediately
            if get_monotonic_us() >= endtime {
                return DoneStatus::NotDone;
            }
            if defrag_ctx.kvstore_iter_state.cursor == 0 {
                break;
            }
        }
        defrag_ctx.kvstore_iter_state.slot = KVS_SLOT_UNASSIGNED;
    }

    // C: Phase 2 — scan each non-empty kvstore slot
    let mut iterations: u32 = 0;
    loop {
        iterations += 1;
        if iterations > 16 {
            if get_monotonic_us() >= endtime {
                break;
            }
            iterations = 0;
        }

        // C: if (precontinue_fn) { update privdata->kvstate = state; call precontinue_fn; }
        // PORT NOTE: `precontinue_fn` (= `defragLaterStep` for the main-keys stage)
        // is not threaded through this helper in the Rust port.  Callers that need
        // pre-continue work invoke `defrag_later_step` directly in their stage fn.

        if defrag_ctx.kvstore_iter_state.cursor == 0 {
            // C: advance to the next non-empty kvstore slot
            if defrag_ctx.kvstore_iter_state.slot == KVS_SLOT_UNASSIGNED {
                // C: state.slot = kvstoreGetFirstNonEmptyHashtableIndex(kvs)
                // TODO(port): not yet available.
                defrag_ctx.kvstore_iter_state.slot = KVS_SLOT_UNASSIGNED; // placeholder: no slots
            } else {
                // C: state.slot = kvstoreGetNextNonEmptyHashtableIndex(kvs, state.slot)
                // TODO(port): not yet available.
                defrag_ctx.kvstore_iter_state.slot = KVS_SLOT_UNASSIGNED; // placeholder: done
            }
            if defrag_ctx.kvstore_iter_state.slot == KVS_SLOT_UNASSIGNED {
                return DoneStatus::Done;
            }
        }

        // C: state.cursor = kvstoreHashtableScanDefrag(kvs, slot, cursor, scan_fn, ...)
        // TODO(port): kvstoreHashtableScanDefrag not yet available.
        defrag_ctx.kvstore_iter_state.cursor = 0; // placeholder: bucket done immediately
    }

    DoneStatus::NotDone
}

// ── Individual stage functions ─────────────────────────────────────────────────

/// Stage: defrag the main key kvstore of one database.
///
/// C: `defragStageDbKeys(endtime, target, privdata)` — src/defrag.c:959-973
///
/// `target` in C is `(void*)(uintptr_t)dbid`; in Rust `dbid` is captured
/// by the closure created in `begin_defrag_cycle`.
///
/// C's `static defragKeysCtx ctx` is captured in the closure as well.
fn defrag_stage_db_keys(
    defrag_ctx: &mut DefragContext,
    dbid: usize,
    ctx: &mut DefragKeysCtx,
    endtime: Monotime,
) -> DoneStatus {
    debug_assert_eq!(ctx.dbid, dbid);
    // TODO(port): obtain real kvs_id from server.db[dbid].keys pointer identity.
    let kvs_id = dbid;
    if endtime == 0 {
        ctx.dbid = dbid; /* fall through to helper init */
    }
    defrag_stage_kvstore_helper(defrag_ctx, kvs_id, endtime)
}

/// Stage: defrag the expire kvstore of one database.
/// C: `defragStageExpiresKvstore(endtime, target, privdata)` — src/defrag.c:977
fn defrag_stage_expires_kvstore(
    defrag_ctx: &mut DefragContext,
    dbid: usize,
    endtime: Monotime,
) -> DoneStatus {
    // TODO(port): obtain real kvs_id from server.db[dbid].expires.
    let kvs_id = dbid.wrapping_add(1_000_000);
    defrag_stage_kvstore_helper(defrag_ctx, kvs_id, endtime)
}

/// Stage: defrag the `keys_with_volatile_items` kvstore of one database.
/// C: `defragStageKeysWithvolaItemsKvstore(endtime, target, privdata)` — src/defrag.c:986
fn defrag_stage_keys_with_vola_items(
    defrag_ctx: &mut DefragContext,
    dbid: usize,
    endtime: Monotime,
) -> DoneStatus {
    // TODO(port): obtain real kvs_id from server.db[dbid].keys_with_volatile_items.
    let kvs_id = dbid.wrapping_add(2_000_000);
    defrag_stage_kvstore_helper(defrag_ctx, kvs_id, endtime)
}

/// Stage: defrag a pubsub or pubsubshard channels kvstore.
/// C: `defragStagePubsubKvstore(endtime, target, privdata)` — src/defrag.c:995
/// TODO(port): pubsub kvstore / client channel accessor not yet available.
fn defrag_stage_pubsub_kvstore(
    defrag_ctx: &mut DefragContext,
    kvs_id: usize,
    endtime: Monotime,
) -> DoneStatus {
    defrag_stage_kvstore_helper(defrag_ctx, kvs_id, endtime)
}

/// Stage: defrag the Lua script cache.
/// C: `defragLuaScripts(endtime, target, privdata)` — src/defrag.c:1005-1014
/// TODO(port): Phase 7 — `evalScriptsDict`/`scriptIsRunning` not yet available.
fn defrag_stage_lua_scripts(_defrag_ctx: &mut DefragContext, endtime: Monotime) -> DoneStatus {
    if endtime == 0 {
        return DoneStatus::NotDone;
    }
    // C: if (scriptIsRunning()) return DEFRAG_DONE;
    // C: activeDefragSdsDict(evalScriptsDict(), DEFRAG_SDS_DICT_VAL_LUA_SCRIPT);
    // TODO(port): Phase 7 — Lua scripting not yet available.
    DoneStatus::Done
}

/// Stage: defrag module global state.
/// C: `defragModuleGlobals(endtime, target, privdata)` — src/defrag.c:1017-1023
/// TODO(port): Phase 10 — `moduleDefragGlobals` not yet available.
fn defrag_stage_module_globals(_defrag_ctx: &mut DefragContext, endtime: Monotime) -> DoneStatus {
    if endtime == 0 {
        return DoneStatus::NotDone;
    }
    // C: moduleDefragGlobals();
    // TODO(port): Phase 10 — module API not yet available.
    DoneStatus::Done
}

// ── Stage queue management ─────────────────────────────────────────────────────

/// Append a stage closure to the pending stage queue.
/// C: `addDefragStage(stage_fn, target, privdata)` — src/defrag.c:1031-1037
fn add_defrag_stage(defrag_ctx: &mut DefragContext, stage_fn: StageFnBox) {
    defrag_ctx
        .remaining_stages
        .push_back(StageDescriptor { stage_fn });
}

// ── Cycle lifecycle ────────────────────────────────────────────────────────────

/// Called at the end of a defrag cycle (normal completion or forced termination).
///
/// C: `endDefragCycle(normal_termination)` — src/defrag.c:1041-1081
///
/// TODO(port): `aeDeleteTimeEvent(server.el, timeproc_id)` — event loop not available.
/// TODO(port): server stat writes (`stat_total_active_defrag_time`, etc.)
///   require `&mut RedisServer`; thread through in Phase B.
fn end_defrag_cycle(defrag_ctx: &mut DefragContext, normal_termination: bool) {
    if normal_termination {
        // C: serverAssert(!defrag.current_stage && listLength(remaining_stages) == 0)
        debug_assert!(defrag_ctx.current_stage.is_none());
        debug_assert!(defrag_ctx.remaining_stages.is_empty());
    } else {
        // C: aeDeleteTimeEvent(server.el, defrag.timeproc_id);
        // TODO(port): event loop timer deregistration not yet available.
        defrag_ctx.current_stage = None;
        defrag_ctx.remaining_stages.clear();
    }

    defrag_ctx.timeproc_id = AE_DELETED_EVENT_ID;
    defrag_ctx.remaining_stages.clear();

    // C: listRelease(defrag_later); defrag_later = NULL; defrag_later_cursor = 0;
    defrag_ctx.defrag_later.clear();
    defrag_ctx.defrag_later_cursor = 0;

    let (frag_pct, frag_bytes) = get_allocator_fragmentation();
    // C: serverLog(LL_VERBOSE, "Active defrag done in %dms, reallocated=%d, frag=%.0f%%, ...")
    eprintln!(
        "Active defrag done in {}ms, frag={:.0}%, frag_bytes={}",
        elapsed_ms(defrag_ctx.start_cycle),
        frag_pct,
        frag_bytes,
    );

    // C: server.stat_total_active_defrag_time += elapsedUs(stat_last_active_defrag_time);
    //    server.stat_last_active_defrag_time = 0;
    //    server.active_defrag_cpu_percent = 0;
    // TODO(port): server stat/config writes require &mut RedisServer.

    // C: monitorActiveDefrag(); — check if another cycle should begin immediately.
    // TODO(port): cannot call monitor_active_defrag here without &mut RedisServer.
}

// ── Timing computations ────────────────────────────────────────────────────────

/// Compute the duty-cycle length in µs for this timeproc invocation.
/// Must be called at the **start** of the timeproc.
///
/// C: `computeDefragCycleUs()` — src/defrag.c:1086-1135
///
/// C's `static int prevCpuPercent` lives in `DefragContext::prev_cpu_percent`.
///
/// `target_cpu_percent`: `server.active_defrag_cpu_percent` (1–99)
/// `cycle_us`:           `server.active_defrag_cycle_us` (minimum duty-cycle µs)
fn compute_defrag_cycle_us(
    defrag_ctx: &mut DefragContext,
    target_cpu_percent: i32,
    cycle_us: i64,
) -> i64 {
    debug_assert!(target_cpu_percent > 0 && target_cpu_percent < 100);

    if target_cpu_percent != defrag_ctx.prev_cpu_percent {
        // C: target% changed; don't consider wait time (prevents stale adjustment).
        defrag_ctx.timeproc_end_time = 0;
        defrag_ctx.prev_cpu_percent = target_cpu_percent;
    }

    if defrag_ctx.timeproc_end_time == 0 {
        // C: First call, or resumed after a pause.
        defrag_ctx.timeproc_overage_us = 0;
        return cycle_us;
    }

    let waited_us = get_monotonic_us().saturating_sub(defrag_ctx.timeproc_end_time) as i64;
    // C: D = P * W / (100 - P)  — duty time to achieve target CPU%
    // C: With D = duty, W = wait, P = percent:  D/(D+W) = P/100  → D = P*W/(100-P)
    let mut dc = (target_cpu_percent as i64) * waited_us / (100 - target_cpu_percent as i64);
    // C: Adjust for accumulated overage from previous cycles.
    dc -= defrag_ctx.timeproc_overage_us;
    defrag_ctx.timeproc_overage_us = 0;

    if dc < cycle_us {
        // C: Never reduce below the minimum cycle time; track the shortfall.
        defrag_ctx.timeproc_overage_us = cycle_us - dc;
        cycle_us
    } else {
        dc
    }
}

/// Compute the inter-cycle delay in ms to achieve `target_cpu_percent`.
/// Must be called at the **end** of the timeproc.
///
/// C: `computeDelayMs(intendedEndtime)` — src/defrag.c:1140-1162
fn compute_delay_ms(
    defrag_ctx: &mut DefragContext,
    intended_endtime: Monotime,
    target_cpu_percent: i32,
    cycle_us: i64,
) -> i64 {
    defrag_ctx.timeproc_end_time = get_monotonic_us();
    let overage = defrag_ctx.timeproc_end_time as i64 - intended_endtime as i64;
    defrag_ctx.timeproc_overage_us += overage;
    // C: Allow underage to reduce existing overage, but don't accumulate underage.
    if defrag_ctx.timeproc_overage_us < 0 {
        defrag_ctx.timeproc_overage_us = 0;
    }

    debug_assert!(target_cpu_percent > 0 && target_cpu_percent < 100);
    // C: totalCycleTimeUs = cycle_us * 100 / targetCpuPercent
    // C: We run for cycle_us and want that to be targetCpuPercent% of the total.
    let total_cycle_time_us = cycle_us * 100 / (target_cpu_percent as i64);
    let mut delay_us = total_cycle_time_us - cycle_us;
    // C: Only count the non-duty fraction of the overage as extra delay.
    delay_us += defrag_ctx.timeproc_overage_us * (100 - target_cpu_percent as i64) / 100;
    if delay_us < 0 {
        delay_us = 0;
    }
    delay_us / 1_000 // µs → ms (round down)
}

// ── activeDefragTimeProc ───────────────────────────────────────────────────────

/// Event-loop timer that drives active defragmentation.
///
/// Called frequently while defrag is running; returns the ms delay until the
/// next invocation, or `AE_NOMORE` (-1) when the cycle is complete.
///
/// C: `activeDefragTimeProc(eventLoop, id, clientData)` — src/defrag.c:1167-1225
///
/// In C this matches the `aeTimeProc` signature.  In Rust, the event-loop
/// callback machinery is not yet ported, so this function takes its inputs
/// as explicit arguments rather than reading from the global `server` struct.
///
/// TODO(port): event-loop callback signature will change when Phase 2 lands.
/// TODO(port): `latencyStartMonitor`/`latencyEndMonitor` not yet available.
pub fn active_defrag_time_proc(
    defrag_ctx: &mut DefragContext,
    active_defrag_enabled: bool,
    target_cpu_percent: i32,
    cycle_us: i64,
) -> i64 {
    // C: serverAssert(defrag.current_stage || listLength(remaining_stages) > 0)
    debug_assert!(defrag_ctx.current_stage.is_some() || !defrag_ctx.remaining_stages.is_empty());

    if !active_defrag_enabled {
        // C: Defrag disabled while running → terminate.
        end_defrag_cycle(defrag_ctx, false);
        return AE_NOMORE;
    }

    if has_active_child_process() {
        // C: Pause while child active; reset end_time to prevent starvation recovery.
        defrag_ctx.timeproc_end_time = 0;
        return 100; // poll again in 100ms
    }

    let starttime = get_monotonic_us();
    let duty_cycle_us = compute_defrag_cycle_us(defrag_ctx, target_cpu_percent, cycle_us);
    let endtime = starttime.saturating_add(duty_cycle_us as u64);
    let mut have_more_work = true;

    // C: latencyStartMonitor(latency); — TODO(port): latency tracking not yet available.

    loop {
        // C: if (!defrag.current_stage) { pop next; init with endtime=0; }
        if defrag_ctx.current_stage.is_none() {
            if let Some(mut stage) = defrag_ctx.remaining_stages.pop_front() {
                let status = (stage.stage_fn)(0);
                // C: serverAssert(status == DEFRAG_NOT_DONE)
                debug_assert_eq!(
                    status,
                    DoneStatus::NotDone,
                    "stage initialisation must return NotDone"
                );
                defrag_ctx.current_stage = Some(stage);
            }
        }

        if let Some(ref mut stage) = defrag_ctx.current_stage {
            let status = (stage.stage_fn)(endtime);
            if status == DoneStatus::Done {
                defrag_ctx.current_stage = None;
            }
        }

        have_more_work =
            defrag_ctx.current_stage.is_some() || !defrag_ctx.remaining_stages.is_empty();

        // C: while (haveMoreWork && getMonotonicUs() <= endtime - active_defrag_cycle_us)
        // If a stage completed early and a full cycle's time remains, start another.
        if !have_more_work || get_monotonic_us() > endtime.saturating_sub(cycle_us as u64) {
            break;
        }
    }

    // C: latencyEndMonitor/latencyAddSampleIfNeeded — TODO(port): not yet available.

    if have_more_work {
        compute_delay_ms(defrag_ctx, endtime, target_cpu_percent, cycle_us)
    } else {
        end_defrag_cycle(defrag_ctx, true);
        AE_NOMORE
    }
}

// ── defragWhileBlocked ────────────────────────────────────────────────────────

/// Simulate one timer-proc call while the server is blocked (loading, scripts).
///
/// C: `defragWhileBlocked()` — src/defrag.c:1231-1248
///
/// TODO(port): `aeDeleteTimeEvent` not yet available.
/// TODO(port): `monitorActiveDefrag()` call when not running requires config
///   from `RedisServer`; placeholder call passes caller-supplied args.
pub fn defrag_while_blocked(
    defrag_ctx: &mut DefragContext,
    active_defrag_enabled: bool,
    target_cpu_percent: i32,
    cycle_us: i64,
    db_count: usize,
) {
    if !defrag_ctx.is_running() {
        // C: if (!defragIsRunning()) monitorActiveDefrag();
        monitor_active_defrag(
            defrag_ctx,
            active_defrag_enabled,
            target_cpu_percent,
            db_count,
        );
    }

    if !defrag_ctx.is_running() {
        return;
    }

    let timeproc_id = defrag_ctx.timeproc_id;
    let reschedule_delay = active_defrag_time_proc(
        defrag_ctx,
        active_defrag_enabled,
        target_cpu_percent,
        cycle_us,
    );
    if reschedule_delay == AE_NOMORE {
        // C: aeDeleteTimeEvent(server.el, timeproc_id);
        // TODO(port): event loop timer deregistration not yet available.
        let _ = timeproc_id;
    }
    // C: Otherwise ignore delay; timer fires next time the event loop can run.
}

// ── beginDefragCycle ──────────────────────────────────────────────────────────

/// Build the stage queue and register the event-loop timer for a new cycle.
///
/// C: `beginDefragCycle()` — src/defrag.c:1251-1284
///
/// TODO(port): stage closures need `&mut DefragContext` during execution, but
///   they are stored inside `DefragContext::remaining_stages`.  This circular
///   ownership is resolved in C via global state; in Rust it requires either
///   splitting `DefragContext` or passing state through a separate argument.
///   For Phase A, closures are placeholder stubs that return `Done` immediately.
///   TODO(architect): resolve DefragContext split before Phase B.
///
/// TODO(port): `aeCreateTimeEvent` not yet available (Phase 2 event loop).
pub fn begin_defrag_cycle(defrag_ctx: &mut DefragContext, db_count: usize) {
    debug_assert!(!defrag_ctx.is_running());
    debug_assert!(defrag_ctx.remaining_stages.is_empty());

    for dbid in 0..db_count {
        // C: if (server.db[dbid] == NULL) continue;
        // TODO(port): null-db check requires access to the server.db array.

        // C: addDefragStage(defragStageDbKeys, (void*)(uintptr_t)dbid, NULL)
        add_defrag_stage(defrag_ctx, {
            let mut ctx = DefragKeysCtx {
                kvstate: KvstoreIterState::default(),
                dbid,
            };
            Box::new(move |_endtime: Monotime| {
                // TODO(port): call defrag_stage_db_keys(defrag_ctx, dbid, &mut ctx, endtime)
                // once the DefragContext-ownership split is resolved (see TODO(architect)).
                DoneStatus::Done
            })
        });

        // C: addDefragStage(defragStageExpiresKvstore, (void*)(uintptr_t)dbid, NULL)
        add_defrag_stage(
            defrag_ctx,
            Box::new(move |_endtime: Monotime| {
                // TODO(port): call defrag_stage_expires_kvstore(defrag_ctx, dbid, endtime)
                DoneStatus::Done
            }),
        );

        // C: addDefragStage(defragStageKeysWithvolaItemsKvstore, ...)
        add_defrag_stage(
            defrag_ctx,
            Box::new(move |_endtime: Monotime| {
                // TODO(port): call defrag_stage_keys_with_vola_items(defrag_ctx, dbid, endtime)
                DoneStatus::Done
            }),
        );
    }

    // C: addDefragStage(defragStagePubsubKvstore, server.pubsub_channels, &fn_wrapper)
    add_defrag_stage(
        defrag_ctx,
        Box::new(|endtime: Monotime| {
            if endtime == 0 {
                return DoneStatus::NotDone;
            }
            // TODO(port): pubsub_channels kvstore identity not yet accessible here.
            DoneStatus::Done
        }),
    );

    // C: addDefragStage(defragStagePubsubKvstore, server.pubsubshard_channels, &fn_wrapper)
    add_defrag_stage(
        defrag_ctx,
        Box::new(|endtime: Monotime| {
            if endtime == 0 {
                return DoneStatus::NotDone;
            }
            // TODO(port): pubsubshard_channels kvstore identity not yet accessible here.
            DoneStatus::Done
        }),
    );

    // C: addDefragStage(defragLuaScripts, NULL, NULL)
    add_defrag_stage(
        defrag_ctx,
        Box::new(|endtime: Monotime| {
            if endtime == 0 {
                return DoneStatus::NotDone;
            }
            // TODO(port): Phase 7 — Lua script defrag not yet available.
            DoneStatus::Done
        }),
    );

    // C: addDefragStage(defragModuleGlobals, NULL, NULL)
    add_defrag_stage(
        defrag_ctx,
        Box::new(|endtime: Monotime| {
            if endtime == 0 {
                return DoneStatus::NotDone;
            }
            // TODO(port): Phase 10 — module defrag not yet available.
            DoneStatus::Done
        }),
    );

    defrag_ctx.current_stage = None;
    defrag_ctx.start_cycle = get_monotonic_us();
    defrag_ctx.start_defrag_hits = server_stat_defrag_hits();
    defrag_ctx.timeproc_end_time = 0;
    defrag_ctx.timeproc_overage_us = 0;

    // C: defrag.timeproc_id = aeCreateTimeEvent(server.el, 0, activeDefragTimeProc, NULL, NULL);
    // TODO(port): event loop timer registration not yet available (Phase 2).
    defrag_ctx.timeproc_id = 1; // placeholder: non-zero signals "running"

    // C: elapsedStart(&server.stat_last_active_defrag_time);
    // TODO(port): server stat update requires &mut RedisServer.
}

// ── updateDefragCpuPercent ────────────────────────────────────────────────────

/// Determine defrag aggressiveness from the current fragmentation level.
/// Returns the recommended CPU percent (0 = no defrag needed).
///
/// C: `updateDefragCpuPercent()` — src/defrag.c:1291-1319
///
/// In C this reads from and writes to `server.*` directly.  In Rust the
/// caller supplies the config and applies the returned value.
///
/// C macros used:
///   `INTERPOLATE(x, x1, x2, y1, y2)` — linear interpolation
///   `LIMIT(y, min, max)` — clamp
pub fn update_defrag_cpu_percent(
    defrag_ctx: &DefragContext,
    current_cpu_percent: i32,
    threshold_lower: f32,
    threshold_upper: f32,
    cpu_min: i32,
    cpu_max: i32,
    ignore_bytes: usize,
    configuration_changed: bool,
) -> i32 {
    let (frag_pct, frag_bytes) = get_allocator_fragmentation();

    if current_cpu_percent == 0 && (frag_pct < threshold_lower || frag_bytes < ignore_bytes) {
        return 0;
    }

    // C: INTERPOLATE(frag_pct, threshold_lower, threshold_upper, cpu_min, cpu_max)
    let cpu_pct = if (threshold_upper - threshold_lower).abs() > f32::EPSILON {
        let t = (frag_pct - threshold_lower) / (threshold_upper - threshold_lower);
        cpu_min + (t * (cpu_max - cpu_min) as f32) as i32
    } else {
        cpu_min
    };
    // C: LIMIT(cpu_pct, cpu_min, cpu_max)
    let cpu_pct = cpu_pct.clamp(cpu_min, cpu_max);

    if cpu_pct > current_cpu_percent || configuration_changed {
        if defrag_ctx.is_running() {
            eprintln!(
                "Changing active defrag CPU, frag={:.0}%, frag_bytes={}, cpu={}%",
                frag_pct, frag_bytes, cpu_pct,
            );
        } else {
            eprintln!(
                "Starting active defrag, frag={:.0}%, frag_bytes={}, cpu={}%",
                frag_pct, frag_bytes, cpu_pct,
            );
        }
        return cpu_pct;
    }

    current_cpu_percent
}

// ── monitorActiveDefrag ────────────────────────────────────────────────────────

/// Check fragmentation and start/adjust an active defrag cycle as needed.
/// Called from the server cron function.
///
/// C: `monitorActiveDefrag()` — src/defrag.c:1322-1332
///
/// TODO(port): in C this reads/writes `server.*` directly.  Here the caller
/// must supply config params and apply the resulting `cpu_percent`.
/// TODO(port): `updateDefragCpuPercent` needs full config params; for Phase A
/// the caller is responsible for computing and passing `target_cpu_percent`.
pub fn monitor_active_defrag(
    defrag_ctx: &mut DefragContext,
    active_defrag_enabled: bool,
    target_cpu_percent: i32,
    db_count: usize,
) {
    if !active_defrag_enabled {
        return;
    }
    // C: if (hasActiveChildProcess()) return;
    if has_active_child_process() {
        return;
    }
    // C: updateDefragCpuPercent(); — adjusts server.active_defrag_cpu_percent.
    // PORT NOTE: updateDefragCpuPercent is now a pure function; the caller must
    // apply the returned value.  This function uses the pre-updated percent.
    if target_cpu_percent > 0 && !defrag_ctx.is_running() {
        begin_defrag_cycle(defrag_ctx, db_count);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/defrag.c  (1357 lines, ~40 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         28
//   port_notes:    7
//   unsafe_blocks: 0
//   notes:         Stage-machine, timing math, and monitoring logic are
//                  faithfully translated.  All allocator-introspection calls
//                  and deferred data-structure functions (kvstore, quicklist,
//                  rax, zset, stream, hashtable) are no-op stubs.  Global
//                  singleton is a thread_local! pending move into RedisServer.
//                  Event-loop timer registration is a placeholder.
// ──────────────────────────────────────────────────────────────────────────────
