//! Sorted set (ZSet) commands and data structures.
//!
//! ZSETs are ordered sets using two data structures:
//!   - A skip list ordered by (score, element) — O(log N) insert/delete
//!   - A hash table mapping element bytes to skiplist node references
//!
//! For small sets the listpack encoding is used instead (Phase 4 dependency).
//!
//! C source: `reference/valkey/src/t_zset.c` (4423 lines, ~80 functions)
//! Crate: `redis-commands` (phase: later)
//!
//! ## Architect items
//!
//! TODO(architect): need dependency edges: redis-commands → redis-types,
//! redis-commands → redis-core for CommandContext / RedisObject / RedisDb.
//!
//! TODO(architect): need redis-ds dependency for ListPack (Phase 4).
//! All listpack-backed sorted set paths are stub-implemented until then.
//!
//! TODO(architect): ZSkipList uses Rc<RefCell<>> for Phase A safety.
//! Phase B should evaluate arena allocation for performance (self-referential
//! structure; raw pointers or an index-based approach may be faster).
//!
//! TODO(architect): CommandContext API — ctx.db_mut(), ctx.server_dirty_incr(),
//! ctx.notify_keyspace_event(), ctx.signal_modified_key() — blocked on Phase 3
//! RedisServer access in CommandContext.
//!
//! TODO(architect): blocking command infrastructure (blockForKeys, BLOCKED_ZSET)
//! not yet available; bzpopmin/bzpopmax/bzmpop command bodies are stubs.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use redis_core::command_context::CommandContext;
use redis_core::object::RedisObject;
use redis_types::{RedisError, RedisString};

// ─── ZADD input flags ─────────────────────────────────────────────────────────
// C: ZADD_IN_* from server.h

pub const ZADD_IN_NONE: i32 = 0;
/// Increment mode: add score to existing rather than replace.
pub const ZADD_IN_INCR: i32 = 1 << 0;
/// Only add when element does not exist.
pub const ZADD_IN_NX: i32 = 1 << 1;
/// Only update when element already exists.
pub const ZADD_IN_XX: i32 = 1 << 2;
/// GT: only update when new score is greater than current.
pub const ZADD_IN_GT: i32 = 1 << 3;
/// LT: only update when new score is less than current.
pub const ZADD_IN_LT: i32 = 1 << 4;

// ─── ZADD output flags ────────────────────────────────────────────────────────
// C: ZADD_OUT_* from server.h

/// Element was added (did not exist before).
pub const ZADD_OUT_ADDED: i32 = 1 << 0;
/// Element score was updated.
pub const ZADD_OUT_UPDATED: i32 = 1 << 1;
/// No operation was performed (NX/XX/GT/LT condition not met).
pub const ZADD_OUT_NOP: i32 = 1 << 2;
/// Result score is NaN.
pub const ZADD_OUT_NAN: i32 = 1 << 3;

// ─── Zpop direction ───────────────────────────────────────────────────────────

/// Pop / range from the minimum-score end.
pub const ZSET_MIN: i32 = 0;
/// Pop / range from the maximum-score end.
pub const ZSET_MAX: i32 = 1;

// ─── Set operation codes ──────────────────────────────────────────────────────

pub const SET_OP_UNION: i32 = 0;
pub const SET_OP_INTER: i32 = 1;
pub const SET_OP_DIFF: i32 = 2;

// ─── Keyspace notification type bits ─────────────────────────────────────────
// C: NOTIFY_* from server.h (local copies for this module)

pub const NOTIFY_ZSET: u32 = 1 << 4;
pub const NOTIFY_GENERIC: u32 = 1 << 2;

// ─── Skiplist constants ────────────────────────────────────────────────────────

/// Maximum number of levels in the skip list.
/// C: ZSKIPLIST_MAXLEVEL
pub const ZSKIPLIST_MAXLEVEL: i32 = 32;

/// Threshold below which linear node-by-node traversal is cheaper than
/// descending from the highest-level node.
/// C: ZSKIPLIST_MAX_SEARCH
pub const ZSKIPLIST_MAX_SEARCH: i64 = 1000;

// ─── ZRange control enums ─────────────────────────────────────────────────────

/// Range type for ZRANGE / ZRANGESTORE family commands.
/// C: zrange_type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZRangeType {
    /// Detect from command arguments (ZRANGE / ZRANGESTORE).
    Auto = 0,
    /// Range by rank (0-based index).
    Rank,
    /// Range by float score.
    Score,
    /// Range by lexicographic order.
    Lex,
}

/// Traversal direction for range operations.
/// C: zrange_direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZRangeDirection {
    /// Detect from command arguments.
    Auto = 0,
    Forward,
    Reverse,
}

/// Whether the range handler sends results to a client or stores them.
/// C: zrange_consumer_type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZRangeConsumerType {
    Client = 0,
    Internal,
}

// ─── Aggregate type for ZUNION / ZINTER ───────────────────────────────────────
// C: REDIS_AGGR_SUM / MIN / MAX

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AggregateType {
    #[default]
    Sum,
    Min,
    Max,
}

// ─── Score range specification ────────────────────────────────────────────────

/// Float score range for ZRANGEBYSCORE / ZCOUNT etc.
/// C: zrangespec
#[derive(Debug, Clone)]
pub struct ZRangeSpec {
    pub min: f64,
    pub max: f64,
    /// True when `min` is exclusive (open interval).
    pub minex: bool,
    /// True when `max` is exclusive (open interval).
    pub maxex: bool,
}

// ─── Lexicographic range specification ───────────────────────────────────────

/// One endpoint of a lexicographic range.
///
/// PORT NOTE: The C code uses pointer identity to detect the special
/// `shared.minstring` ("-") and `shared.maxstring` ("+") sentinels.
/// This enum models the same concept without pointer tricks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound {
    /// Sentinel "-": all strings are >= this bound.
    NegInfinity,
    /// Sentinel "+": all strings are <= this bound.
    PosInfinity,
    /// A real string bound with inclusive semantics (ZRANGEBYLEX "[foo").
    Included(Vec<u8>),
    /// A real string bound with exclusive semantics (ZRANGEBYLEX "(foo").
    Excluded(Vec<u8>),
}

/// Lexicographic range for ZRANGEBYLEX / ZLEXCOUNT etc.
/// C: zlexrangespec { sds min, max; int minex, maxex; }
/// PORT NOTE: exclusivity is encoded into `LexBound` variants, removing the
/// separate `minex` / `maxex` integer flags.
#[derive(Debug, Clone)]
pub struct ZLexRangeSpec {
    pub min: LexBound,
    pub max: LexBound,
}

// ─── Skip list node and list ──────────────────────────────────────────────────

/// One level within a skip list node.
/// C: struct zskiplistLevel { zskiplistNode *forward; unsigned long span; }
///
/// PORT NOTE: In C, level[0].span of a node stores the node's *height* (not
/// the span between nodes at level 0). That hijacking is replaced here by an
/// explicit `height` field on `ZSkipListNode`; `levels[0].span` is the real
/// span at level 0 (always 1 for non-tail nodes).
#[derive(Debug, Clone)]
pub struct ZSkipListLevel {
    /// Forward node reference at this level (None = end of list).
    pub forward: Option<ZSkipListNodeRef>,
    /// Number of nodes between this node and `forward` at this level.
    pub span: u64,
}

/// A node in the sorted set skip list.
/// C: zskiplistNode (element sds is embedded in the same allocation)
///
/// PORT NOTE: C embeds the element SDS immediately after the level array for
/// cache locality.  Rust uses a separate `Vec<u8>`.
/// PERF(port): measure whether embedding (via a custom arena) wins in Phase B.
///
/// TODO(architect): Phase B: replace Rc<RefCell<>> with arena + index for
/// better cache behaviour and to avoid the per-node heap allocation overhead.
#[derive(Debug)]
pub struct ZSkipListNode {
    pub score: f64,
    /// Sorted set member bytes (owned copy).
    pub element: Vec<u8>,
    /// Backward pointer used by ZREVRANGE traversal.
    /// Weak to avoid reference cycles with the forward chain.
    /// C: zskiplistNode *backward (raw pointer, always set)
    pub backward: Option<std::rc::Weak<RefCell<ZSkipListNode>>>,
    /// Levels array.  `levels.len()` equals the node height.
    pub levels: Vec<ZSkipListLevel>,
}

/// Shared-ownership reference to a skip list node.
pub type ZSkipListNodeRef = Rc<RefCell<ZSkipListNode>>;

/// The sorted set skip list.
/// C: struct zskiplist { zskiplistNode header; zskiplistNode *tail;
///                       unsigned long length; int level; }
///
/// PORT NOTE: In C the header is a value embedded in the struct (not a pointer).
/// Here it is an `Rc<RefCell<>>` for uniformity with other nodes.
#[derive(Debug)]
pub struct ZSkipList {
    /// Sentinel header node (score irrelevant; element empty).
    /// `header.borrow().levels` has exactly `ZSKIPLIST_MAXLEVEL` entries.
    pub header: ZSkipListNodeRef,
    /// Last node in ascending order (None for empty list).
    pub tail: Option<ZSkipListNodeRef>,
    /// Number of real elements (not counting the header sentinel).
    pub length: u64,
    /// Current maximum level in use (1-based, up to ZSKIPLIST_MAXLEVEL).
    pub height: i32,
}

// ─── Internal ZSet struct ─────────────────────────────────────────────────────

/// The internal skiplist-encoded representation of a sorted set.
/// C: struct zset { zskiplist *zsl; hashtable *ht; }
///
/// PORT NOTE: The C hashtable maps element bytes → raw skiplist node pointer.
/// `HashMap<Vec<u8>, ZSkipListNodeRef>` achieves the same without unsafe and
/// keeps the node alive until it is removed from both structures.
#[derive(Debug)]
pub struct ZSet {
    pub zsl: ZSkipList,
    /// Maps element bytes to the corresponding skip list node.
    pub ht: HashMap<Vec<u8>, ZSkipListNodeRef>,
}

// ─── Listpack placeholder ─────────────────────────────────────────────────────
//
// ListPack is in redis-ds (Phase 4). Until that crate exists, all zzl*
// functions stub-implement their bodies and return errors.
//
// TODO(architect): replace `Listpack` alias and all lp_* stubs with imports
// from redis_ds::listpack once Phase 4 is complete.

/// Opaque byte buffer representing an encoded listpack.
/// Real type: redis_ds::ListPack
pub type Listpack = Vec<u8>;

/// An entry decoded from a listpack — either a raw byte string or an integer.
/// C: listpackEntry { unsigned char *sval; uint32_t slen; long long lval; }
#[derive(Debug, Clone, Default)]
pub struct ListpackEntry {
    /// String value bytes if the entry is a string, otherwise None.
    pub sval: Option<Vec<u8>>,
    /// Integer value, valid when `sval` is None.
    pub lval: i64,
}

// ─── ZSet set-operation iterator ─────────────────────────────────────────────

/// Opval flags mirroring the C #define constants.
/// C: #define OPVAL_DIRTY_SDS 1 / OPVAL_DIRTY_LL 2 / OPVAL_VALID_LL 4
const OPVAL_DIRTY_ELE: u8 = 1;
const OPVAL_VALID_INT: u8 = 4;

/// An element/score value encountered while iterating a ZSetOpSrc.
/// C: zsetopval { int flags; sds ele; unsigned char *estr; unsigned int elen;
///               long long ell; double score; }
///
/// PORT NOTE: The C struct uses raw pointers into listpack memory or SDS for
/// lazy conversion. Rust uses owned Vec<u8> after materialisation.
#[derive(Debug, Default)]
pub struct ZSetOpVal {
    pub flags: u8,
    /// Materialised element bytes (owned). Built lazily from raw_bytes / raw_int.
    pub ele: Option<Vec<u8>>,
    /// Raw bytes from listpack storage (reference-counted copy for Phase A).
    pub raw_bytes: Option<Vec<u8>>,
    /// Raw integer from intset / integer-encoded listpack entry.
    pub raw_int: Option<i64>,
    pub score: f64,
}

/// Iterator state for one source key in ZUNION / ZINTER / ZDIFF.
/// C: union _iterset / union _iterzset (inside zsetopsrc)
///
/// PORT NOTE: C uses C-union to overlay set and zset iterator state.
/// Rust uses an enum variant which is safer and equally compact.
#[derive(Debug)]
pub enum ZSetOpIterState {
    /// Source key does not exist — contributes no elements.
    Empty,
    /// Iterating a set stored as an intset.
    /// TODO(port): intset type not yet available; using Vec<i64> placeholder
    SetIntset { values: Vec<i64>, pos: usize },
    /// Iterating a set stored as a hashtable (keys as Vec<u8>).
    SetHashtable { keys: Vec<Vec<u8>>, pos: usize },
    /// Iterating a set stored as a listpack.
    SetListpack { entries: Vec<Vec<u8>>, pos: usize },
    /// Iterating a zset stored as a listpack (reversed for algorithm efficiency).
    ZSetListpack { pairs: Vec<(Vec<u8>, f64)>, pos: usize },
    /// Iterating a zset stored as a skiplist (reversed traversal).
    ZSetSkiplist {
        /// Collected (element, score) pairs in reverse sorted order.
        /// TODO(port): Phase B should use direct skiplist traversal.
        pairs: Vec<(Vec<u8>, f64)>,
        pos: usize,
    },
}

/// One source operand for ZUNION / ZINTER / ZDIFF.
/// C: zsetopsrc { robj *subject; int type; int encoding; double weight;
///                union { ... set; ... zset; } iter; }
#[derive(Debug)]
pub struct ZSetOpSrc {
    /// The source object (None = key did not exist).
    pub subject: Option<RedisObject>,
    /// Score multiplier for WEIGHTS option.
    pub weight: f64,
    /// Iterator state (initialised by `zui_init_iterator`).
    pub iter: ZSetOpIterState,
}

// ─── ZRange result-handler trait ─────────────────────────────────────────────
//
// C uses a struct with function pointers (hand-rolled vtable):
//   zrangeResultBeginFunction beginResultEmission;
//   zrangeResultFinalizeFunction finalizeResultEmission;
//   zrangeResultEmitCBufferFunction emitResultFromCBuffer;
//   zrangeResultEmitLongLongFunction emitResultFromLongLong;
//
// Rust uses a trait object.  PORT NOTE: intentional restructuring.

/// Interface implemented by both the "reply to client" and "store to key"
/// variants of the ZRANGE result handler.
/// C: struct zrange_result_handler (function-pointer fields)
pub trait ZRangeEmitter {
    /// Called once before any elements are emitted; `length` is the known
    /// result count (-1 means unknown / deferred).
    fn begin(&mut self, length: i64);
    /// Called once after all elements are emitted.
    fn finalize(&mut self, result_count: usize);
    /// Emit one result element from raw bytes.
    fn emit_buffer(&mut self, value: &[u8], score: f64);
    /// Emit one result element from an integer-encoded listpack entry.
    fn emit_longlong(&mut self, value: i64, score: f64);
    fn withscores(&self) -> bool;
    fn should_emit_array_len(&self) -> bool;
}

/// Range result handler that sends RESP replies directly to the client.
/// C: zrangeResultBeginClient / zrangeResultEmitCBufferToClient /
///    zrangeResultFinalizeClient
pub struct ClientRangeEmitter<'a> {
    pub ctx: &'a mut CommandContext,
    pub withscores: bool,
    /// True when RESP3 nested-array format is needed (one [elem, score] sub-array
    /// per result element).
    pub should_emit_array_len: bool,
    /// Tracks whether the result length was deferred.
    /// TODO(port): deferred-len mechanism not yet defined in CommandContext
    pub deferred: bool,
    pub emitted: usize,
}

/// Range result handler that stores results into a destination sorted set.
/// C: zrangeResultBeginStore / zrangeResultEmitCBufferForStore /
///    zrangeResultFinalizeStore
pub struct StoreRangeEmitter<'a> {
    pub ctx: &'a mut CommandContext,
    pub dstkey: Vec<u8>,
    pub dstobj: Option<Box<ZSet>>,
    pub result_count: usize,
}

// ─── implementations ──────────────────────────────────────────────────────────

// ═══════════════════════════════════════════════════════════════════════════
// Skip list — low-level implementation
// C: t_zset.c:80-843
// ═══════════════════════════════════════════════════════════════════════════

/// Compare elements for skip list ordering (by element bytes only).
/// Returns negative / zero / positive like memcmp.
/// C: sdscmp (used inside zslCompareNodes)
fn sds_cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    a.cmp(b)
}

/// Compare two skip list nodes for ordering.
/// Returns:
///   < 0  if a comes before b
///   > 0  if a comes after b
///   = 0  if equal
/// C: zslCompareNodes
fn zsl_compare_nodes_score_ele(a_score: f64, a_ele: &[u8], b_score: f64, b_ele: &[u8]) -> i32 {
    if a_score > b_score {
        return 1;
    }
    if a_score < b_score {
        return -1;
    }
    match a_ele.cmp(b_ele) {
        std::cmp::Ordering::Greater => 1,
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
    }
}

/// Return a random level for a new skip list node.
/// Distribution: level k is returned with probability p^(k-1) where p ≈ 0.25,
/// capped at ZSKIPLIST_MAXLEVEL.
/// C: zslRandomLevel — uses __builtin_clzll(rand) / 2 + 1
///
/// TODO(port): replace rand::random with genrand64_int64 from redis-core mt19937
/// module once that crate is available.
fn zsl_random_level() -> i32 {
    // TODO(port): use mt19937-64 genrand64_int64 from redis-core
    let rand_val: u64 = {
        // Placeholder: use std's thread_rng for Phase A structure correctness
        // PERF(port): replace with deterministic mt19937 for reproducibility
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0)
            .hash(&mut h);
        h.finish()
    };
    if rand_val == 0 {
        ZSKIPLIST_MAXLEVEL
    } else {
        // __builtin_clzll(rand) / 2 + 1, capped
        let level = (rand_val.leading_zeros() / 2 + 1) as i32;
        level.min(ZSKIPLIST_MAXLEVEL)
    }
}

/// Create a new, empty skip list sentinel node with `height` levels.
/// C: zslCreateNode (with embedded SDS)
fn zsl_create_sentinel_node(height: i32) -> ZSkipListNode {
    ZSkipListNode {
        score: 0.0,
        element: Vec::new(),
        backward: None,
        levels: (0..height)
            .map(|_| ZSkipListLevel { forward: None, span: 0 })
            .collect(),
    }
}

/// Create a new skip list element node.
/// C: zslCreateNode
fn zsl_create_element_node(height: i32, score: f64, element: Vec<u8>) -> ZSkipListNode {
    ZSkipListNode {
        score,
        element,
        backward: None,
        levels: (0..height)
            .map(|_| ZSkipListLevel { forward: None, span: 0 })
            .collect(),
    }
}

/// Create a new empty skip list.
/// C: zslCreate
pub fn zsl_create() -> ZSkipList {
    let header = Rc::new(RefCell::new(zsl_create_sentinel_node(ZSKIPLIST_MAXLEVEL)));
    ZSkipList {
        header,
        tail: None,
        length: 0,
        height: 1,
    }
}

/// Create a new ZSet with an empty skip list and empty hash table.
/// C: createZsetObject (allocates zset + calls zslCreate + hashtableCreate)
pub fn zset_create() -> ZSet {
    ZSet {
        zsl: zsl_create(),
        ht: HashMap::new(),
    }
}

// ─── Skip list insert ──────────────────────────────────────────────────────

/// Insert a new element with the given score into the skip list.
/// Assumes the element does not already exist (caller is responsible).
/// Returns a reference to the newly inserted node.
/// C: zslInsert (creates node + calls zslInsertNode)
pub fn zsl_insert(zsl: &mut ZSkipList, score: f64, element: Vec<u8>) -> ZSkipListNodeRef {
    let level = zsl_random_level();
    let new_node = Rc::new(RefCell::new(zsl_create_element_node(level, score, element)));
    zsl_insert_node(zsl, Rc::clone(&new_node));
    new_node
}

/// Insert an already-created node into the skip list.
/// C: zslInsertNode — takes ownership of node
fn zsl_insert_node(zsl: &mut ZSkipList, node: ZSkipListNodeRef) {
    // C: zskiplistNode *update[ZSKIPLIST_MAXLEVEL]; unsigned long rank[ZSKIPLIST_MAXLEVEL];
    let mut update: Vec<ZSkipListNodeRef> =
        vec![Rc::clone(&zsl.header); ZSKIPLIST_MAXLEVEL as usize];
    let mut rank: Vec<u64> = vec![0u64; ZSKIPLIST_MAXLEVEL as usize];

    let node_level = node.borrow().levels.len() as i32;
    let (node_score, node_ele) = {
        let nb = node.borrow();
        (nb.score, nb.element.clone())
    };

    debug_assert!(!node_score.is_nan());

    // Descend from the top level, tracking predecessors.
    let mut x = Rc::clone(&zsl.header);
    for i in (0..zsl.height).rev() {
        rank[i as usize] = if i == zsl.height - 1 { 0 } else { rank[i as usize + 1] };
        loop {
            let fwd = {
                let xb = x.borrow();
                if (i as usize) < xb.levels.len() {
                    xb.levels[i as usize].forward.as_ref().map(Rc::clone)
                } else {
                    None
                }
            };
            match fwd {
                None => break,
                Some(fwd_node) => {
                    let (fwd_score, fwd_ele) = {
                        let fb = fwd_node.borrow();
                        (fb.score, fb.element.clone())
                    };
                    if zsl_compare_nodes_score_ele(fwd_score, &fwd_ele, node_score, &node_ele) < 0 {
                        rank[i as usize] += x.borrow().levels[i as usize].span;
                        let next_x = Rc::clone(&fwd_node);
                        x = next_x;
                    } else {
                        break;
                    }
                }
            }
        }
        update[i as usize] = Rc::clone(&x);
    }

    // Extend the list height if the new node is taller.
    if node_level > zsl.height {
        for i in zsl.height..node_level {
            rank[i as usize] = 0;
            update[i as usize] = Rc::clone(&zsl.header);
            zsl.header.borrow_mut().levels[i as usize].span = zsl.length;
        }
        zsl.height = node_level;
    }

    // Splice the node into each level.
    for i in 0..node_level as usize {
        let old_fwd = update[i].borrow().levels[i].forward.as_ref().map(Rc::clone);
        node.borrow_mut().levels[i].forward = old_fwd.as_ref().map(Rc::clone);
        update[i].borrow_mut().levels[i].forward = Some(Rc::clone(&node));

        let update_span = update[i].borrow().levels[i].span;
        let rank_diff = rank[0].saturating_sub(rank[i]);
        node.borrow_mut().levels[i].span = update_span.saturating_sub(rank_diff);
        update[i].borrow_mut().levels[i].span = rank_diff + 1;
    }

    // Increment span for levels above node_level.
    for i in node_level as usize..zsl.height as usize {
        let cur = update[i].borrow().levels[i].span;
        update[i].borrow_mut().levels[i].span = cur.saturating_add(1);
    }

    // Set backward pointer.
    let new_backward = if Rc::ptr_eq(&update[0], &zsl.header) {
        None
    } else {
        Some(Rc::downgrade(&update[0]))
    };
    node.borrow_mut().backward = new_backward;

    // Update the next node's backward pointer, or set tail.
    let next_fwd = node.borrow().levels[0].forward.as_ref().map(Rc::clone);
    match next_fwd {
        Some(next_node) => {
            next_node.borrow_mut().backward = Some(Rc::downgrade(&node));
        }
        None => {
            zsl.tail = Some(Rc::clone(&node));
        }
    }
    zsl.length += 1;
}

// ─── Skip list delete ─────────────────────────────────────────────────────

/// Remove a node from the skip list (internal: given the predecessor array).
/// C: zslDeleteNode
fn zsl_delete_node(zsl: &mut ZSkipList, node: &ZSkipListNodeRef, update: &[ZSkipListNodeRef]) {
    let node_level = node.borrow().levels.len();
    for i in 0..zsl.height as usize {
        let fwd_matches = update[i]
            .borrow()
            .levels[i]
            .forward
            .as_ref()
            .map_or(false, |f| Rc::ptr_eq(f, node));
        if fwd_matches {
            let node_span = if i < node_level { node.borrow().levels[i].span } else { 0 };
            let update_span = update[i].borrow().levels[i].span;
            update[i].borrow_mut().levels[i].span =
                update_span.saturating_add(node_span).saturating_sub(1);
            let node_fwd = node.borrow().levels[i].forward.as_ref().map(Rc::clone);
            update[i].borrow_mut().levels[i].forward = node_fwd;
        } else {
            let update_span = update[i].borrow().levels[i].span;
            update[i].borrow_mut().levels[i].span = update_span.saturating_sub(1);
        }
    }

    // Update backward pointer of the successor.
    let node_fwd0 = node.borrow().levels[0].forward.as_ref().map(Rc::clone);
    match node_fwd0 {
        Some(next_node) => {
            next_node.borrow_mut().backward = node.borrow().backward.clone();
        }
        None => {
            // node was the tail; new tail is node's predecessor
            zsl.tail = node
                .borrow()
                .backward
                .as_ref()
                .and_then(|w| w.upgrade());
        }
    }

    // Shrink height if top levels are now empty.
    while zsl.height > 1
        && zsl.header.borrow().levels[zsl.height as usize - 1]
            .forward
            .is_none()
    {
        zsl.height -= 1;
    }
    zsl.length -= 1;
}

/// Build the predecessor (`update`) array for a node with the given
/// (score, element), used before delete/update operations.
fn zsl_find_predecessors(
    zsl: &ZSkipList,
    score: f64,
    element: &[u8],
) -> Vec<ZSkipListNodeRef> {
    let mut update: Vec<ZSkipListNodeRef> =
        vec![Rc::clone(&zsl.header); ZSKIPLIST_MAXLEVEL as usize];
    let mut x = Rc::clone(&zsl.header);
    for i in (0..zsl.height).rev() {
        loop {
            let fwd = {
                let xb = x.borrow();
                xb.levels[i as usize].forward.as_ref().map(Rc::clone)
            };
            match fwd {
                None => break,
                Some(f) => {
                    let (fs, fe) = {
                        let fb = f.borrow();
                        (fb.score, fb.element.clone())
                    };
                    if zsl_compare_nodes_score_ele(fs, &fe, score, element) < 0 {
                        x = Rc::clone(&f);
                    } else {
                        break;
                    }
                }
            }
        }
        update[i as usize] = Rc::clone(&x);
    }
    update
}

/// Delete a node identified by (score, element) from the skip list.
/// C: zslDelete (wraps zslDeleteNode + zslFreeNode)
pub fn zsl_delete(zsl: &mut ZSkipList, node: &ZSkipListNodeRef) {
    let (score, element) = {
        let nb = node.borrow();
        (nb.score, nb.element.clone())
    };
    let update = zsl_find_predecessors(zsl, score, &element);
    debug_assert!(
        update[0]
            .borrow()
            .levels[0]
            .forward
            .as_ref()
            .map_or(false, |f| Rc::ptr_eq(f, node)),
        "zsl_delete: expected node not found at predecessor"
    );
    zsl_delete_node(zsl, node, &update);
    // The Rc in the caller / hashtable keeps the node alive until those
    // references are also dropped.  No explicit free needed.
}

/// Update the score of an existing node.  If the position in the sorted order
/// does not change, the node is updated in place (cheap path).  Otherwise the
/// node is removed and re-inserted.
/// Returns `Some(new_node_ref)` when the node was re-inserted, `None` when
/// updated in place (node reference is still valid in either case).
/// C: zslUpdateScore
pub fn zsl_update_score(
    zsl: &mut ZSkipList,
    node: &ZSkipListNodeRef,
    new_score: f64,
) -> Option<ZSkipListNodeRef> {
    // Cheap path: score change doesn't affect sorted position.
    let (prev_score, next_score) = {
        let nb = node.borrow();
        let prev = nb
            .backward
            .as_ref()
            .and_then(|w| w.upgrade())
            .map(|p| p.borrow().score);
        let next = nb.levels[0].forward.as_ref().map(|f| f.borrow().score);
        (prev, next)
    };
    let in_place = prev_score.map_or(true, |p| p < new_score)
        && next_score.map_or(true, |n| n > new_score);
    if in_place {
        node.borrow_mut().score = new_score;
        return None;
    }

    // Need to remove and re-insert.
    let element = node.borrow().element.clone();
    let update = zsl_find_predecessors(zsl, node.borrow().score, &element);
    debug_assert!(
        update[0]
            .borrow()
            .levels[0]
            .forward
            .as_ref()
            .map_or(false, |f| Rc::ptr_eq(f, node))
    );
    zsl_delete_node(zsl, node, &update);
    node.borrow_mut().score = new_score;
    zsl_insert_node(zsl, Rc::clone(node));
    Some(Rc::clone(node))
}

// ─── Score range predicates ───────────────────────────────────────────────

/// Returns true if `value` >= the minimum of `spec`.
/// C: zslValueGteMin
pub fn zsl_value_gte_min(value: f64, spec: &ZRangeSpec) -> bool {
    if spec.minex {
        value > spec.min
    } else {
        value >= spec.min
    }
}

/// Returns true if `value` <= the maximum of `spec`.
/// C: zslValueLteMax
pub fn zsl_value_lte_max(value: f64, spec: &ZRangeSpec) -> bool {
    if spec.maxex {
        value < spec.max
    } else {
        value <= spec.max
    }
}

/// Returns true if any element in the skip list falls within `range`.
/// C: zslIsInRange
pub fn zsl_is_in_range(zsl: &ZSkipList, range: &ZRangeSpec) -> bool {
    if range.min > range.max || (range.min == range.max && (range.minex || range.maxex)) {
        return false;
    }
    let tail_score = zsl.tail.as_ref().map(|t| t.borrow().score);
    if tail_score.map_or(true, |s| !zsl_value_gte_min(s, range)) {
        return false;
    }
    let head_score = zsl
        .header
        .borrow()
        .levels[0]
        .forward
        .as_ref()
        .map(|f| f.borrow().score);
    if head_score.map_or(true, |s| !zsl_value_lte_max(s, range)) {
        return false;
    }
    true
}

/// Find the N-th node in `range` (0-based).  Negative N counts from the end
/// (-1 = last in range).  Returns None if no element in range or offset
/// exceeds range size.
/// C: zslNthInRange
pub fn zsl_nth_in_range(
    zsl: &ZSkipList,
    range: &ZRangeSpec,
    n: i64,
    rank_out: Option<&mut i64>,
) -> Option<ZSkipListNodeRef> {
    if !zsl_is_in_range(zsl, range) {
        return None;
    }

    // TODO(port): the full fast-path with last_highest_level_node is omitted
    // for Phase A readability; Phase B can add it for performance.
    // C: t_zset.c:419-489
    if n >= 0 {
        // Forward scan: find first element >= min, then step n times.
        let mut x = Rc::clone(&zsl.header);
        for i in (0..zsl.height).rev() {
            loop {
                let fwd = {
                    let xb = x.borrow();
                    xb.levels[i as usize].forward.as_ref().map(Rc::clone)
                };
                match fwd {
                    None => break,
                    Some(f) if !zsl_value_gte_min(f.borrow().score, range) => {
                        x = Rc::clone(&f);
                    }
                    _ => break,
                }
            }
        }
        // x is now the last node with score < min.
        for _ in 0..=n {
            let fwd = x.borrow().levels[0].forward.as_ref().map(Rc::clone);
            match fwd {
                None => return None,
                Some(f) => x = f,
            }
        }
        // Check upper bound.
        if !zsl_value_lte_max(x.borrow().score, range) {
            return None;
        }
        // TODO(port): rank_out calculation omitted for Phase A
        let _ = rank_out;
        Some(x)
    } else {
        // Reverse scan: find last element <= max, then step backwards.
        let mut x = Rc::clone(&zsl.header);
        for i in (0..zsl.height).rev() {
            loop {
                let fwd = {
                    let xb = x.borrow();
                    xb.levels[i as usize].forward.as_ref().map(Rc::clone)
                };
                match fwd {
                    None => break,
                    Some(f) if zsl_value_lte_max(f.borrow().score, range) => {
                        x = Rc::clone(&f);
                    }
                    _ => break,
                }
            }
        }
        // x is now the last node with score <= max.
        let steps = (-n - 1) as u64;
        for _ in 0..steps {
            let bwd = x
                .borrow()
                .backward
                .as_ref()
                .and_then(|w| w.upgrade());
            match bwd {
                None => return None,
                Some(b) if Rc::ptr_eq(&b, &zsl.header) => return None,
                Some(b) => x = b,
            }
        }
        if !zsl_value_gte_min(x.borrow().score, range) {
            return None;
        }
        let _ = rank_out;
        Some(x)
    }
}

/// Find the rank of a node (1-based).
/// C: zslGetRank
pub fn zsl_get_rank(zsl: &ZSkipList, node: &ZSkipListNodeRef) -> u64 {
    let (score, element) = {
        let nb = node.borrow();
        (nb.score, nb.element.clone())
    };
    let mut rank: u64 = 0;
    let mut x = Rc::clone(&zsl.header);
    'outer: for i in (0..zsl.height).rev() {
        loop {
            let fwd = {
                let xb = x.borrow();
                xb.levels[i as usize].forward.as_ref().map(Rc::clone)
            };
            match fwd {
                None => break,
                Some(f) => {
                    let (fs, fe) = {
                        let fb = f.borrow();
                        (fb.score, fb.element.clone())
                    };
                    if zsl_compare_nodes_score_ele(fs, &fe, score, &element) <= 0 {
                        rank += x.borrow().levels[i as usize].span;
                        if Rc::ptr_eq(&f, node) {
                            break 'outer;
                        }
                        x = Rc::clone(&f);
                    } else {
                        break;
                    }
                }
            }
        }
    }
    rank
}

/// Find the element at 1-based rank (descending from the header).
/// C: zslGetElementByRank
pub fn zsl_get_element_by_rank(zsl: &ZSkipList, rank: u64) -> Option<ZSkipListNodeRef> {
    let mut traversed: u64 = 0;
    let mut x = Rc::clone(&zsl.header);
    for i in (0..zsl.height).rev() {
        loop {
            let (fwd, span) = {
                let xb = x.borrow();
                let lvl = &xb.levels[i as usize];
                (lvl.forward.as_ref().map(Rc::clone), lvl.span)
            };
            if fwd.is_some() && traversed + span <= rank {
                traversed += span;
                let f = fwd.unwrap();
                if traversed == rank {
                    return Some(f);
                }
                x = f;
            } else {
                break;
            }
        }
        if traversed == rank {
            // We reached rank via the final move; x is now the target.
            // But x == header means rank 0 — shouldn't happen.
            if !Rc::ptr_eq(&x, &zsl.header) {
                return Some(x);
            }
        }
    }
    None
}

// ─── Delete ranges from skiplist ──────────────────────────────────────────

/// Delete all elements with score in `range` from the skip list, also
/// removing them from the hash table `ht`.
/// C: zslDeleteRangeByScore
fn zsl_delete_range_by_score(
    zsl: &mut ZSkipList,
    range: &ZRangeSpec,
    ht: &mut HashMap<Vec<u8>, ZSkipListNodeRef>,
) -> u64 {
    let mut update: Vec<ZSkipListNodeRef> =
        vec![Rc::clone(&zsl.header); ZSKIPLIST_MAXLEVEL as usize];
    let mut x = Rc::clone(&zsl.header);
    for i in (0..zsl.height).rev() {
        loop {
            let fwd = x.borrow().levels[i as usize].forward.as_ref().map(Rc::clone);
            match fwd {
                None => break,
                Some(f) if !zsl_value_gte_min(f.borrow().score, range) => {
                    x = Rc::clone(&f);
                }
                _ => break,
            }
        }
        update[i as usize] = Rc::clone(&x);
    }
    let mut removed: u64 = 0;
    let mut cur = x.borrow().levels[0].forward.as_ref().map(Rc::clone);
    while let Some(node) = cur {
        if !zsl_value_lte_max(node.borrow().score, range) {
            break;
        }
        let next = node.borrow().levels[0].forward.as_ref().map(Rc::clone);
        let ele = node.borrow().element.clone();
        zsl_delete_node(zsl, &node, &update);
        ht.remove(&ele);
        removed += 1;
        cur = next;
    }
    removed
}

/// Delete all elements whose element bytes fall within `range` from the skip
/// list and hash table.
/// C: zslDeleteRangeByLex
fn zsl_delete_range_by_lex(
    zsl: &mut ZSkipList,
    range: &ZLexRangeSpec,
    ht: &mut HashMap<Vec<u8>, ZSkipListNodeRef>,
) -> u64 {
    let mut update: Vec<ZSkipListNodeRef> =
        vec![Rc::clone(&zsl.header); ZSKIPLIST_MAXLEVEL as usize];
    let mut x = Rc::clone(&zsl.header);
    for i in (0..zsl.height).rev() {
        loop {
            let fwd = x.borrow().levels[i as usize].forward.as_ref().map(Rc::clone);
            match fwd {
                None => break,
                Some(f) if !zsl_lex_value_gte_min(&f.borrow().element, range) => {
                    x = Rc::clone(&f);
                }
                _ => break,
            }
        }
        update[i as usize] = Rc::clone(&x);
    }
    let mut removed: u64 = 0;
    let mut cur = x.borrow().levels[0].forward.as_ref().map(Rc::clone);
    while let Some(node) = cur {
        if !zsl_lex_value_lte_max(&node.borrow().element, range) {
            break;
        }
        let next = node.borrow().levels[0].forward.as_ref().map(Rc::clone);
        let ele = node.borrow().element.clone();
        zsl_delete_node(zsl, &node, &update);
        ht.remove(&ele);
        removed += 1;
        cur = next;
    }
    removed
}

/// Delete all elements with rank in `[start, end]` (1-based inclusive)
/// from the skip list and hash table.
/// C: zslDeleteRangeByRank
fn zsl_delete_range_by_rank(
    zsl: &mut ZSkipList,
    start: u64,
    end: u64,
    ht: &mut HashMap<Vec<u8>, ZSkipListNodeRef>,
) -> u64 {
    let mut update: Vec<ZSkipListNodeRef> =
        vec![Rc::clone(&zsl.header); ZSKIPLIST_MAXLEVEL as usize];
    let mut traversed: u64 = 0;
    let mut x = Rc::clone(&zsl.header);
    for i in (0..zsl.height).rev() {
        loop {
            let (fwd, span) = {
                let xb = x.borrow();
                let lvl = &xb.levels[i as usize];
                (lvl.forward.as_ref().map(Rc::clone), lvl.span)
            };
            if fwd.is_some() && traversed + span < start {
                traversed += span;
                x = fwd.unwrap();
            } else {
                break;
            }
        }
        update[i as usize] = Rc::clone(&x);
    }
    traversed += 1;
    let mut cur = x.borrow().levels[0].forward.as_ref().map(Rc::clone);
    let mut removed: u64 = 0;
    while let Some(node) = cur {
        if traversed > end {
            break;
        }
        let next = node.borrow().levels[0].forward.as_ref().map(Rc::clone);
        let ele = node.borrow().element.clone();
        zsl_delete_node(zsl, &node, &update);
        ht.remove(&ele);
        removed += 1;
        traversed += 1;
        cur = next;
    }
    removed
}

// ─── Lexicographic range predicates ──────────────────────────────────────

/// Compare two byte slices with sentinel support for NegInfinity / PosInfinity.
/// C: sdscmplex
fn lex_cmp(a: &LexBound, b: &LexBound) -> std::cmp::Ordering {
    use LexBound::*;
    match (a, b) {
        (NegInfinity, NegInfinity) => std::cmp::Ordering::Equal,
        (PosInfinity, PosInfinity) => std::cmp::Ordering::Equal,
        (NegInfinity, _) | (_, PosInfinity) => std::cmp::Ordering::Less,
        (PosInfinity, _) | (_, NegInfinity) => std::cmp::Ordering::Greater,
        (Included(a_bytes) | Excluded(a_bytes), Included(b_bytes) | Excluded(b_bytes)) => {
            a_bytes.as_slice().cmp(b_bytes.as_slice())
        }
    }
}

/// Compare element bytes against a LexBound, handling sentinels.
fn lex_cmp_bytes(value: &[u8], bound: &LexBound) -> std::cmp::Ordering {
    match bound {
        LexBound::NegInfinity => std::cmp::Ordering::Greater,
        LexBound::PosInfinity => std::cmp::Ordering::Less,
        LexBound::Included(b) | LexBound::Excluded(b) => value.cmp(b.as_slice()),
    }
}

/// Returns true if `value` satisfies the minimum bound of `spec`.
/// C: zslLexValueGteMin
pub fn zsl_lex_value_gte_min(value: &[u8], spec: &ZLexRangeSpec) -> bool {
    match &spec.min {
        LexBound::NegInfinity => true,
        LexBound::PosInfinity => false,
        LexBound::Included(b) => value >= b.as_slice(),
        LexBound::Excluded(b) => value > b.as_slice(),
    }
}

/// Returns true if `value` satisfies the maximum bound of `spec`.
/// C: zslLexValueLteMax
pub fn zsl_lex_value_lte_max(value: &[u8], spec: &ZLexRangeSpec) -> bool {
    match &spec.max {
        LexBound::PosInfinity => true,
        LexBound::NegInfinity => false,
        LexBound::Included(b) => value <= b.as_slice(),
        LexBound::Excluded(b) => value < b.as_slice(),
    }
}

/// Returns true if the skip list contains any element in `range`.
/// C: zslIsInLexRange
fn zsl_is_in_lex_range(zsl: &ZSkipList, range: &ZLexRangeSpec) -> bool {
    let cmp = lex_cmp(&range.min, &range.max);
    if cmp == std::cmp::Ordering::Greater {
        return false;
    }
    if cmp == std::cmp::Ordering::Equal {
        // If bounds are equal and either is exclusive, range is empty.
        let min_excl = matches!(&range.min, LexBound::Excluded(_));
        let max_excl = matches!(&range.max, LexBound::Excluded(_));
        if min_excl || max_excl {
            return false;
        }
    }
    let tail_ok = zsl.tail.as_ref().map_or(false, |t| {
        zsl_lex_value_gte_min(&t.borrow().element, range)
    });
    if !tail_ok {
        return false;
    }
    let head_ok = zsl
        .header
        .borrow()
        .levels[0]
        .forward
        .as_ref()
        .map_or(false, |f| zsl_lex_value_lte_max(&f.borrow().element, range));
    head_ok
}

/// Find the N-th element in the lex range (0-based; negative counts from end).
/// C: zslNthInLexRange
pub fn zsl_nth_in_lex_range(
    zsl: &ZSkipList,
    range: &ZLexRangeSpec,
    n: i64,
) -> Option<ZSkipListNodeRef> {
    if !zsl_is_in_lex_range(zsl, range) {
        return None;
    }
    // TODO(port): fast-path with last_highest_level_node omitted (Phase B perf)
    if n >= 0 {
        let mut x = Rc::clone(&zsl.header);
        for i in (0..zsl.height).rev() {
            loop {
                let fwd = x.borrow().levels[i as usize].forward.as_ref().map(Rc::clone);
                match fwd {
                    None => break,
                    Some(f) if !zsl_lex_value_gte_min(&f.borrow().element.clone(), range) => {
                        x = Rc::clone(&f);
                    }
                    _ => break,
                }
            }
        }
        for _ in 0..=n {
            let fwd = x.borrow().levels[0].forward.as_ref().map(Rc::clone);
            match fwd {
                None => return None,
                Some(f) => x = f,
            }
        }
        if !zsl_lex_value_lte_max(&x.borrow().element.clone(), range) {
            return None;
        }
        Some(x)
    } else {
        let mut x = Rc::clone(&zsl.header);
        for i in (0..zsl.height).rev() {
            loop {
                let fwd = x.borrow().levels[i as usize].forward.as_ref().map(Rc::clone);
                match fwd {
                    None => break,
                    Some(f) if zsl_lex_value_lte_max(&f.borrow().element.clone(), range) => {
                        x = Rc::clone(&f);
                    }
                    _ => break,
                }
            }
        }
        let steps = (-n - 1) as u64;
        for _ in 0..steps {
            let bwd = x.borrow().backward.as_ref().and_then(|w| w.upgrade());
            match bwd {
                None => return None,
                Some(b) if Rc::ptr_eq(&b, &zsl.header) => return None,
                Some(b) => x = b,
            }
        }
        if !zsl_lex_value_gte_min(&x.borrow().element.clone(), range) {
            return None;
        }
        Some(x)
    }
}

// ─── Parse range specs from command args ──────────────────────────────────

/// Parse a score range from two `RedisObject` arguments.
/// C: zslParseRange
pub fn zsl_parse_range(min_obj: &RedisObject, max_obj: &RedisObject) -> Result<ZRangeSpec, RedisError> {
    // TODO(port): this depends on RedisObject::as_bytes() and encoding detection
    // which is not yet stabilised; using placeholder implementation.
    // C: t_zset.c:623-661
    let parse_endpoint = |obj: &RedisObject, is_min: bool| -> Result<(f64, bool), RedisError> {
        // TODO(port): extract bytes from RedisObject (encoding INT vs SDS)
        // For Phase A, defer to TODO; this will be fixed in Phase B.
        let _ = (obj, is_min);
        Err(RedisError::runtime(b"TODO(port): zsl_parse_range not implemented"))
    };
    let (min, minex) = parse_endpoint(min_obj, true)?;
    let (max, maxex) = parse_endpoint(max_obj, false)?;
    Ok(ZRangeSpec { min, max, minex, maxex })
}

/// Parse a lexicographic range from two `RedisObject` arguments.
/// C: zsetParseLexRange
pub fn zset_parse_lex_range(
    min_obj: &RedisObject,
    max_obj: &RedisObject,
) -> Result<ZLexRangeSpec, RedisError> {
    // TODO(port): extract bytes from RedisObject; implement parsing of
    // "[foo", "(foo", "-", "+" prefixes.  Blocked on RedisObject byte accessor.
    // C: t_zset.c:716-729
    let _ = (min_obj, max_obj);
    Err(RedisError::runtime(b"TODO(port): zset_parse_lex_range not implemented"))
}

// ═══════════════════════════════════════════════════════════════════════════
// Listpack-backed sorted set (zzl* functions)
// C: t_zset.c:844-1257
//
// All functions here depend on the redis-ds ListPack type (Phase 4).
// They are stubbed with TODO(port) and return errors until Phase 4 is ready.
// ═══════════════════════════════════════════════════════════════════════════

/// Get the score stored at a listpack score pointer.
/// C: zzlGetScore
/// TODO(port): implement when redis-ds listpack is available
pub fn zzl_get_score(_sptr: &[u8]) -> f64 {
    0.0 // TODO(port): lpGetValue + strtod
}

/// Return the element at a listpack element pointer as owned bytes.
/// C: lpGetObject
/// TODO(port): implement when redis-ds listpack is available
pub fn lp_get_object(_sptr: &[u8]) -> Vec<u8> {
    Vec::new() // TODO(port): lpGetValue + format integer if needed
}

/// Return the number of elements in a listpack-encoded sorted set.
/// Each element takes two entries (member + score), so lpLength / 2.
/// C: zzlLength
/// TODO(port): implement when redis-ds listpack is available
pub fn zzl_length(_zl: &Listpack) -> usize {
    0 // TODO(port): lpLength(zl) / 2
}

/// Check if the listpack-encoded zset has any score in `range`.
/// C: zzlIsInRange
/// TODO(port): implement when redis-ds listpack is available
pub fn zzl_is_in_range(_zl: &Listpack, _range: &ZRangeSpec) -> bool {
    false
}

/// Find the first element pointer in `range`.
/// C: zzlFirstInRange
/// TODO(port): implement when redis-ds listpack is available
pub fn zzl_first_in_range<'a>(_zl: &'a Listpack, _range: &ZRangeSpec) -> Option<usize> {
    None
}

/// Find the last element pointer in `range`.
/// C: zzlLastInRange
/// TODO(port): implement when redis-ds listpack is available
pub fn zzl_last_in_range<'a>(_zl: &'a Listpack, _range: &ZRangeSpec) -> Option<usize> {
    None
}

/// Check if the listpack-encoded zset has any element in lex `range`.
/// C: zzlIsInLexRange
/// TODO(port): implement when redis-ds listpack is available
pub fn zzl_is_in_lex_range(_zl: &Listpack, _range: &ZLexRangeSpec) -> bool {
    false
}

/// Find the first element in lex `range`.
/// C: zzlFirstInLexRange
/// TODO(port): implement when redis-ds listpack is available
pub fn zzl_first_in_lex_range<'a>(_zl: &'a Listpack, _range: &ZLexRangeSpec) -> Option<usize> {
    None
}

/// Find the last element in lex `range`.
/// C: zzlLastInLexRange
/// TODO(port): implement when redis-ds listpack is available
pub fn zzl_last_in_lex_range<'a>(_zl: &'a Listpack, _range: &ZLexRangeSpec) -> Option<usize> {
    None
}

/// Find element `ele` in the listpack; if found, set `*score` and return offset.
/// C: zzlFind (static)
/// TODO(port): implement when redis-ds listpack is available
fn zzl_find(_lp: &Listpack, _ele: &[u8]) -> Option<(usize, f64)> {
    None
}

/// Delete the element at `eptr` (and its following score entry).
/// Returns the updated listpack.
/// C: zzlDelete (static)
/// TODO(port): implement when redis-ds listpack is available
fn zzl_delete(zl: Listpack, _eptr: usize) -> Listpack {
    zl // TODO(port): lpDeleteRangeWithEntry
}

/// Insert (element, score) at position `eptr` (None = append).
/// C: zzlInsertAt (static)
/// TODO(port): implement when redis-ds listpack is available
fn zzl_insert_at(zl: Listpack, _eptr: Option<usize>, _ele: &[u8], _score: f64) -> Listpack {
    zl // TODO(port): lpInsert* / lpAppend*
}

/// Insert (element, score) into the sorted position in the listpack.
/// C: zzlInsert (static)
/// TODO(port): implement when redis-ds listpack is available
fn zzl_insert(zl: Listpack, _ele: &[u8], _score: f64) -> Listpack {
    zl // TODO(port): zzlInsertAt at correct position
}

/// Delete all elements with score in `range`.
/// C: zzlDeleteRangeByScore (static)
/// TODO(port): implement when redis-ds listpack is available
fn zzl_delete_range_by_score(
    zl: Listpack,
    _range: &ZRangeSpec,
    deleted: &mut u64,
) -> Listpack {
    *deleted = 0;
    zl
}

/// Delete all elements with lex in `range`.
/// C: zzlDeleteRangeByLex (static)
/// TODO(port): implement when redis-ds listpack is available
fn zzl_delete_range_by_lex(
    zl: Listpack,
    _range: &ZLexRangeSpec,
    deleted: &mut u64,
) -> Listpack {
    *deleted = 0;
    zl
}

/// Delete all elements with rank in `[start, end]` (1-based).
/// C: zzlDeleteRangeByRank
/// TODO(port): implement when redis-ds listpack is available
pub fn zzl_delete_range_by_rank(
    zl: Listpack,
    _start: u32,
    _end: u32,
    deleted: &mut u64,
) -> Listpack {
    *deleted = 0;
    zl // TODO(port): lpDeleteRange(zl, 2*(start-1), 2*num)
}

// ═══════════════════════════════════════════════════════════════════════════
// Common sorted set API (encoding-agnostic)
// C: t_zset.c:1259-1780
// ═══════════════════════════════════════════════════════════════════════════

/// Return the number of elements in a sorted set (any encoding).
/// C: zsetLength
pub fn zset_length(zobj: &RedisObject) -> u64 {
    // TODO(port): match on RedisObject::ZSet variant once object.rs is stable
    // C: checks OBJ_ENCODING_LISTPACK vs OBJ_ENCODING_SKIPLIST
    match zobj {
        // TODO(port): RedisObject::ZSet(inner) => match inner encoding
        _ => 0, // placeholder
    }
}

/// Create a new sorted set object sized for `size_hint` elements with
/// `val_len_hint` bytes per element.  Uses listpack for small sets.
/// C: zsetTypeCreate
pub fn zset_type_create(size_hint: usize, val_len_hint: usize) -> RedisObject {
    // TODO(port): consult server.zset_max_listpack_entries / value limits
    // For now always create a skiplist-encoded ZSet.
    // C: t_zset.c:1283-1292
    let _ = (size_hint, val_len_hint);
    // TODO(port): return RedisObject::ZSet(...)
    // Placeholder — Phase B will hook up the actual variant
    todo!("TODO(port): zset_type_create needs RedisObject::ZSet variant")
}

/// Convert a sorted set to `target_encoding`, pre-sizing for `cap` elements.
/// C: zsetConvertAndExpand
pub fn zset_convert_and_expand(zobj: &mut RedisObject, target_encoding: i32, _cap: u64) {
    // TODO(port): match on current encoding, convert listpack ↔ skiplist
    // C: t_zset.c:1311-1383
    let _ = (zobj, target_encoding);
    // TODO(port): implement conversion logic in Phase B
}

/// Convert a sorted set to `target_encoding`.
/// C: zsetConvert
pub fn zset_convert(zobj: &mut RedisObject, encoding: i32) {
    let len = zset_length(zobj);
    zset_convert_and_expand(zobj, encoding, len);
}

/// Convert to listpack if the set has grown small enough.
/// C: zsetConvertToListpackIfNeeded
pub fn zset_convert_to_listpack_if_needed(
    zobj: &mut RedisObject,
    _maxelelen: usize,
    _totelelen: usize,
) {
    // TODO(port): check server limits and convert if appropriate
    let _ = zobj;
}

/// Maybe convert a listpack-encoded set to skiplist based on size hints.
/// C: zsetTypeMaybeConvert
pub fn zset_type_maybe_convert(
    zobj: &mut RedisObject,
    size_hint: usize,
    value_len_hint: usize,
) {
    // TODO(port): check server.zset_max_listpack_entries / value limits
    let _ = (zobj, size_hint, value_len_hint);
}

/// Get the score of `member` in the sorted set.
/// C: zsetScore
pub fn zset_score(zobj: &RedisObject, member: &[u8]) -> Result<f64, RedisError> {
    // TODO(port): match on RedisObject encoding
    // C: t_zset.c:1402-1417
    let _ = (zobj, member);
    Err(RedisError::runtime(b"TODO(port): zset_score encoding dispatch"))
}

/// Add or update an element in the sorted set.
///
/// `in_flags` is a bitmask of `ZADD_IN_*`.
/// `out_flags` receives a bitmask of `ZADD_OUT_*`.
/// If `ZADD_IN_INCR` is set, `newscore` receives the final score.
///
/// Returns true on success, false on NaN error.
/// C: zsetAdd (t_zset.c:1464-1589)
pub fn zset_add(
    zobj: &mut RedisObject,
    score: f64,
    ele: &[u8],
    in_flags: i32,
    out_flags: &mut i32,
    newscore: &mut f64,
) -> bool {
    let incr = (in_flags & ZADD_IN_INCR) != 0;
    let nx = (in_flags & ZADD_IN_NX) != 0;
    let xx = (in_flags & ZADD_IN_XX) != 0;
    let gt = (in_flags & ZADD_IN_GT) != 0;
    let lt = (in_flags & ZADD_IN_LT) != 0;
    *out_flags = 0;

    if score.is_nan() {
        *out_flags = ZADD_OUT_NAN;
        return false;
    }

    // TODO(port): match on RedisObject encoding (listpack vs skiplist)
    // C: t_zset.c:1481-1589
    // The logic below mirrors the C skiplist path only.
    // Listpack path: TODO(port) pending redis-ds Phase 4.
    match zobj {
        // TODO(port): RedisObject::ZSet(ref mut inner) => {
        //   match inner encoding {
        //     listpack: use zzl_find / zzl_insert / zzl_delete ...
        //     skiplist: use zset.ht / zsl_insert / zsl_update_score ...
        //   }
        // }
        _ => {
            *out_flags = ZADD_OUT_NOP;
            let _ = (incr, nx, xx, gt, lt, ele, newscore);
            true
        }
    }
}

/// Delete element `ele` from the sorted set.
/// Returns true if the element existed and was removed.
/// C: zsetDel
pub fn zset_del(zobj: &mut RedisObject, ele: &[u8]) -> bool {
    // TODO(port): dispatch on encoding
    // C: t_zset.c:1609-1626
    let _ = (zobj, ele);
    false
}

/// Return the 0-based rank of `ele` in the sorted set.
/// If `reverse` is true, ranks are reversed (highest score = rank 0).
/// Returns -1 if the element does not exist.
/// C: zsetRank (static)
pub fn zset_rank(
    zobj: &RedisObject,
    ele: &[u8],
    reverse: bool,
    output_score: Option<&mut f64>,
) -> i64 {
    // TODO(port): dispatch on encoding
    // C: t_zset.c:1639-1688
    let _ = (zobj, ele, reverse, output_score);
    -1
}

/// Duplicate a sorted set, preserving encoding.
/// C: zsetDup
pub fn zset_dup(o: &RedisObject) -> Result<RedisObject, RedisError> {
    // TODO(port): match on encoding and deep-copy
    // C: t_zset.c:1695-1737
    let _ = o;
    Err(RedisError::runtime(b"TODO(port): zset_dup not implemented"))
}

/// Create an owned `Vec<u8>` from a ListpackEntry.
/// C: zsetSdsFromListpackEntry
pub fn zset_sds_from_listpack_entry(e: &ListpackEntry) -> Vec<u8> {
    if let Some(ref sv) = e.sval {
        sv.clone()
    } else {
        format!("{}", e.lval).into_bytes()
    }
}

/// Reply to the client with a ListpackEntry as a bulk string.
/// C: zsetReplyFromListpackEntry
pub fn zset_reply_from_listpack_entry(
    ctx: &mut CommandContext,
    e: &ListpackEntry,
) -> Result<(), RedisError> {
    if let Some(ref sv) = e.sval {
        ctx.reply_bulk(sv)
    } else {
        ctx.reply_integer(e.lval)
    }
}

/// Pick a random element from a non-empty sorted set.
/// C: zsetTypeRandomElement (static)
pub fn zset_type_random_element(
    zsetobj: &RedisObject,
    _zsetsize: u64,
    key: &mut ListpackEntry,
    score: Option<&mut f64>,
) {
    // TODO(port): dispatch on encoding, use hashtableFairRandomEntry or lpRandomPair
    // C: t_zset.c:1757-1780
    let _ = (zsetobj, key, score);
}

// ═══════════════════════════════════════════════════════════════════════════
// Sorted set commands
// C: t_zset.c:1782-4423
// ═══════════════════════════════════════════════════════════════════════════

/// Generic implementation shared by ZADD and ZINCRBY.
/// `flags` is a bitmask of ZADD_IN_* pre-set by the caller.
/// C: zaddGenericCommand (static)
fn zadd_generic_command(ctx: &mut CommandContext, flags: i32) -> Result<(), RedisError> {
    // C: t_zset.c:1787-1920
    let mut incr = (flags & ZADD_IN_INCR) != 0;
    let mut nx = (flags & ZADD_IN_NX) != 0;
    let mut xx = (flags & ZADD_IN_XX) != 0;
    let mut gt = (flags & ZADD_IN_GT) != 0;
    let mut lt = (flags & ZADD_IN_LT) != 0;
    let mut ch = false;

    // Parse options starting at argv[2].
    let mut scoreidx: usize = 2;
    let argc = ctx.argc();
    while scoreidx < argc {
        let opt = ctx.arg_bytes(scoreidx)?;
        match opt {
            b"nx" | b"NX" => flags_set_nx(&mut flags.clone(), &mut nx),
            b"xx" | b"XX" => flags_set_xx(&mut flags.clone(), &mut xx),
            b"ch" | b"CH" => ch = true,
            b"incr" | b"INCR" => {
                incr = true;
                let _ = flags; // flags ZADD_IN_INCR already set by caller
            }
            b"gt" | b"GT" => gt = true,
            b"lt" | b"LT" => lt = true,
            _ => break,
        }
        scoreidx += 1;
    }
    // TODO(port): case-insensitive comparison above uses literal byte slices;
    // Phase B should use a proper case-folding helper for Redis string args.

    let elements = argc.saturating_sub(scoreidx);
    if elements % 2 != 0 || elements == 0 {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let pair_count = elements / 2;

    // Validate options.
    if nx && xx {
        return Err(RedisError::runtime(
            b"XX and NX options at the same time are not compatible",
        ));
    }
    if (gt && nx) || (lt && nx) || (gt && lt) {
        return Err(RedisError::runtime(
            b"GT, LT, and/or NX options at the same time are not compatible",
        ));
    }
    if incr && pair_count > 1 {
        return Err(RedisError::runtime(
            b"INCR option supports a single increment-element pair",
        ));
    }

    // Parse all scores up front.
    let mut scores: Vec<f64> = Vec::with_capacity(pair_count);
    let mut maxelelen: usize = 0;
    for j in 0..pair_count {
        let score_bytes = ctx.arg_bytes(scoreidx + j * 2)?;
        let score = parse_double_from_bytes(score_bytes)?;
        scores.push(score);
        let ele_len = ctx.arg_bytes(scoreidx + 1 + j * 2)?.len();
        if ele_len > maxelelen {
            maxelelen = ele_len;
        }
    }

    // Lookup or create the sorted set.
    let key = ctx.arg_bytes(1)?.to_vec();

    // TODO(port): ctx.db_mut().lookup_key_write / dbAdd / zsetTypeMaybeConvert
    // C: t_zset.c:1872-1898
    let mut added: i32 = 0;
    let mut updated: i32 = 0;
    let mut processed: i32 = 0;
    let mut final_score: f64 = 0.0;
    let mut nan_error = false;

    // TODO(port): actual ZSet access and mutation via ctx
    // Placeholder loop — wired up in Phase B when db access is available.
    for _j in 0..pair_count {
        let _score = scores[_j];
        let _ele = ctx.arg_bytes(scoreidx + 1 + _j * 2)?;
        // TODO(port): call zset_add, track added/updated/processed
    }
    let _ = (maxelelen, nan_error, &key);

    if nan_error {
        return Err(RedisError::runtime(
            b"resulting score is not a number (NaN)",
        ));
    }
    if incr {
        if processed > 0 {
            ctx.reply_double(final_score)
        } else {
            ctx.reply_null()
        }
    } else {
        let reply_count = if ch { added + updated } else { added };
        ctx.reply_integer(reply_count as i64)
    }
}

/// Bit-flag helpers (avoid mutable borrow of a copy).
#[inline(always)]
fn flags_set_nx(_f: &mut i32, nx: &mut bool) { *nx = true; }
#[inline(always)]
fn flags_set_xx(_f: &mut i32, xx: &mut bool) { *xx = true; }

/// Parse a `f64` from a Redis argument byte slice.
fn parse_double_from_bytes(b: &[u8]) -> Result<f64, RedisError> {
    // TODO(port): use valkey_strtod_n equivalent from redis-core
    // C: getDoubleFromObjectOrReply uses strtod
    let s = std::str::from_utf8(b).map_err(|_| RedisError::not_float())?;
    // PORT NOTE: from_utf8 is permitted here because score is a pure ASCII number,
    // not a user-data byte string.
    s.parse::<f64>().map_err(|_| RedisError::not_float())
}

/// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
/// C: zaddCommand
pub fn zadd_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zadd_generic_command(ctx, ZADD_IN_NONE)
}

/// ZINCRBY key increment member
/// C: zincrbyCommand
pub fn zincrby_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zadd_generic_command(ctx, ZADD_IN_INCR)
}

/// ZREM key member [member ...]
/// C: zremCommand
pub fn zrem_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:1930-1955
    let _key = ctx.arg_bytes(1)?.to_vec();
    let mut deleted: i32 = 0;

    // TODO(port): lookup zobj from db, call zset_del for each member
    // C: for (j = 2; j < c->argc; j++) { if (zsetDel(zobj, ...)) deleted++; }
    for _j in 2..ctx.argc() {
        // TODO(port): zset_del(zobj, ctx.arg_bytes(j)?)
        let _ = ctx.arg_bytes(_j)?;
    }
    // TODO(port): notify_keyspace_event, signal_modified_key, server.dirty
    ctx.reply_integer(deleted as i64)
}

/// Implements ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX.
/// C: zremrangeGenericCommand
fn zremrange_generic_command(
    ctx: &mut CommandContext,
    rangetype: ZRangeType,
) -> Result<(), RedisError> {
    // C: t_zset.c:1964-2057
    // TODO(port): parse range, lookup key, dispatch by encoding, notify, reply
    let _ = rangetype;
    ctx.reply_integer(0) // placeholder
}

/// ZREMRANGEBYRANK key start stop
/// C: zremrangebyrankCommand
pub fn zremrangebyrank_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zremrange_generic_command(ctx, ZRangeType::Rank)
}

/// ZREMRANGEBYSCORE key min max
/// C: zremrangebyscoreCommand
pub fn zremrangebyscore_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zremrange_generic_command(ctx, ZRangeType::Score)
}

/// ZREMRANGEBYLEX key min max
/// C: zremrangebylexCommand
pub fn zremrangebylex_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zremrange_generic_command(ctx, ZRangeType::Lex)
}

// ─── ZUNION / ZINTER / ZDIFF infrastructure ──────────────────────────────

/// Initialise the iterator for a ZSetOpSrc.
/// C: zuiInitIterator (static)
fn zui_init_iterator(op: &mut ZSetOpSrc) {
    // TODO(port): inspect op.subject encoding and populate op.iter
    // C: t_zset.c:2132-2169
    op.iter = ZSetOpIterState::Empty;
}

/// Release iterator resources.
/// C: zuiClearIterator (static)
fn zui_clear_iterator(op: &mut ZSetOpSrc) {
    op.iter = ZSetOpIterState::Empty;
}

/// Return the number of elements in `op`.
/// C: zuiLength (static)
fn zui_length(op: &ZSetOpSrc) -> u64 {
    // TODO(port): return actual set/zset length from encoding
    match &op.iter {
        ZSetOpIterState::Empty => 0,
        ZSetOpIterState::SetIntset { values, .. } => values.len() as u64,
        ZSetOpIterState::SetHashtable { keys, .. } => keys.len() as u64,
        ZSetOpIterState::SetListpack { entries, .. } => entries.len() as u64,
        ZSetOpIterState::ZSetListpack { pairs, .. } => pairs.len() as u64,
        ZSetOpIterState::ZSetSkiplist { pairs, .. } => pairs.len() as u64,
    }
}

/// Advance the iterator and fill `val` with the current element.
/// Returns false when the iterator is exhausted.
/// C: zuiNext (static)
fn zui_next(op: &mut ZSetOpSrc, val: &mut ZSetOpVal) -> bool {
    // TODO(port): full implementation per encoding
    // C: t_zset.c:2229-2286
    *val = ZSetOpVal::default();
    match &mut op.iter {
        ZSetOpIterState::Empty => false,
        ZSetOpIterState::SetIntset { values, pos } => {
            if *pos >= values.len() { return false; }
            val.raw_int = Some(values[*pos]);
            val.score = 1.0;
            *pos += 1;
            true
        }
        ZSetOpIterState::SetHashtable { keys, pos } => {
            if *pos >= keys.len() { return false; }
            val.raw_bytes = Some(keys[*pos].clone());
            val.score = 1.0;
            *pos += 1;
            true
        }
        ZSetOpIterState::SetListpack { entries, pos } => {
            if *pos >= entries.len() { return false; }
            val.raw_bytes = Some(entries[*pos].clone());
            val.score = 1.0;
            *pos += 1;
            true
        }
        ZSetOpIterState::ZSetListpack { pairs, pos }
        | ZSetOpIterState::ZSetSkiplist { pairs, pos } => {
            if *pos >= pairs.len() { return false; }
            let (ref e, s) = pairs[*pos];
            val.raw_bytes = Some(e.clone());
            val.score = s;
            *pos += 1;
            true
        }
    }
}

/// Get the materialised element bytes from a ZSetOpVal.
/// C: zuiSdsFromValue (static)
fn zui_bytes_from_val(val: &mut ZSetOpVal) -> Vec<u8> {
    if let Some(ref ele) = val.ele {
        return ele.clone();
    }
    let bytes = if let Some(ref rb) = val.raw_bytes {
        rb.clone()
    } else if let Some(int_val) = val.raw_int {
        format!("{}", int_val).into_bytes()
    } else {
        Vec::new()
    };
    val.ele = Some(bytes.clone());
    val.flags |= OPVAL_DIRTY_ELE;
    bytes
}

/// Return a new owned copy of the element bytes, consuming the dirty flag.
/// C: zuiNewSdsFromValue (static)
fn zui_new_bytes_from_val(val: &mut ZSetOpVal) -> Vec<u8> {
    if val.flags & OPVAL_DIRTY_ELE != 0 {
        let bytes = val.ele.take().unwrap_or_default();
        val.flags &= !OPVAL_DIRTY_ELE;
        return bytes;
    }
    if let Some(ref ele) = val.ele {
        return ele.clone();
    }
    if let Some(ref rb) = val.raw_bytes {
        return rb.clone();
    }
    if let Some(int_val) = val.raw_int {
        return format!("{}", int_val).into_bytes();
    }
    Vec::new()
}

/// Find `val`'s element in the source `op`.  If found, write score to `score`.
/// C: zuiFind (static)
fn zui_find(op: &ZSetOpSrc, val: &mut ZSetOpVal, score: &mut f64) -> bool {
    // TODO(port): full implementation per encoding
    // C: t_zset.c:2320-2358
    let _ = (op, val, score);
    false
}

/// Aggregate scores per the chosen aggregate strategy.
/// C: zunionInterAggregate (inline static)
fn zunion_inter_aggregate(target: &mut f64, val: f64, aggregate: AggregateType) {
    match aggregate {
        AggregateType::Sum => {
            *target += val;
            if target.is_nan() {
                *target = 0.0;
            }
        }
        AggregateType::Min => {
            if val < *target {
                *target = val;
            }
        }
        AggregateType::Max => {
            if val > *target {
                *target = val;
            }
        }
    }
}

/// DIFF algorithm 1: iterate first set, skip elements found in any other set.
/// C: zdiffAlgorithm1 (static)
fn zdiff_algorithm1(
    src: &mut [ZSetOpSrc],
    dstzset: &mut ZSet,
    maxelelen: &mut usize,
    totelelen: &mut usize,
) {
    // C: t_zset.c:2412-2464
    let setnum = src.len();
    // Sort sets 1..setnum by decreasing cardinality for early exits.
    // PERF(port): qsort(src+1, ...) — Rust sort is stable but functionally equivalent
    if setnum > 1 {
        src[1..].sort_by(|a, b| zui_length(b).cmp(&zui_length(a)));
    }

    let mut zval = ZSetOpVal::default();
    zui_init_iterator(&mut src[0]);
    while zui_next(&mut src[0], &mut zval) {
        let mut exists = false;
        for j in 1..setnum {
            if std::ptr::eq(&src[j] as *const _, &src[0] as *const _) {
                exists = true;
                break;
            }
            let mut dummy_score = 0.0;
            if zui_find(&src[j], &mut zval, &mut dummy_score) {
                exists = true;
                break;
            }
        }
        if !exists {
            let tmp = zui_new_bytes_from_val(&mut zval);
            let elen = tmp.len();
            let node = zsl_insert(&mut dstzset.zsl, zval.score, tmp.clone());
            dstzset.ht.insert(tmp, node);
            if elen > *maxelelen { *maxelelen = elen; }
            *totelelen += elen;
        }
    }
    zui_clear_iterator(&mut src[0]);
}

/// DIFF algorithm 2: add first set, remove elements from subsequent sets.
/// C: zdiffAlgorithm2 (static)
fn zdiff_algorithm2(
    src: &mut [ZSetOpSrc],
    dstzset: &mut ZSet,
    maxelelen: &mut usize,
    totelelen: &mut usize,
) {
    // C: t_zset.c:2467-2527
    let setnum = src.len();
    let mut cardinality: i64 = 0;
    for j in 0..setnum {
        if zui_length(&src[j]) == 0 { continue; }
        let mut zval = ZSetOpVal::default();
        zui_init_iterator(&mut src[j]);
        while zui_next(&mut src[j], &mut zval) {
            if j == 0 {
                let tmp = zui_new_bytes_from_val(&mut zval);
                let node = zsl_insert(&mut dstzset.zsl, zval.score, tmp.clone());
                dstzset.ht.insert(tmp, node);
                cardinality += 1;
            } else {
                let tmp = zui_bytes_from_val(&mut zval);
                if dstzset.ht.contains_key(&tmp) {
                    dstzset.ht.remove(&tmp);
                    // TODO(port): also remove from skiplist (zsetRemoveFromSkiplist)
                    cardinality -= 1;
                }
            }
            if cardinality == 0 { break; }
        }
        zui_clear_iterator(&mut src[j]);
        if cardinality == 0 { break; }
    }
    // Measure max element length after the fact.
    for node_ref in dstzset.ht.values() {
        let len = node_ref.borrow().element.len();
        if len > *maxelelen { *maxelelen = len; }
        *totelelen += len;
    }
}

/// Choose DIFF algorithm: returns 0 (empty result), 1, or 2.
/// C: zsetChooseDiffAlgorithm (static)
fn zset_choose_diff_algorithm(src: &[ZSetOpSrc]) -> i32 {
    let setnum = src.len();
    let mut algo_one: i64 = 0;
    let mut algo_two: i64 = 0;
    for j in 0..setnum {
        if j > 0
            && std::ptr::eq(
                src[0].subject.as_ref().map_or(std::ptr::null(), |s| s as *const RedisObject),
                src[j].subject.as_ref().map_or(std::ptr::null(), |s| s as *const RedisObject),
            )
        {
            return 0;
        }
        algo_one += zui_length(&src[0]) as i64;
        algo_two += zui_length(&src[j]) as i64;
    }
    algo_one /= 2;
    if algo_one <= algo_two { 1 } else { 2 }
}

/// Compute the sorted set DIFF into `dstzset`.
/// C: zdiff (static)
fn zdiff(
    src: &mut [ZSetOpSrc],
    dstzset: &mut ZSet,
    maxelelen: &mut usize,
    totelelen: &mut usize,
) {
    if zui_length(&src[0]) > 0 {
        match zset_choose_diff_algorithm(src) {
            1 => zdiff_algorithm1(src, dstzset, maxelelen, totelelen),
            2 => zdiff_algorithm2(src, dstzset, maxelelen, totelelen),
            0 => { /* empty result — nothing to do */ }
            _ => {
                // TODO(architect): is panic correct here? C: serverPanic
                panic!("Unknown ZDIFF algorithm");
            }
        }
    }
}

/// Generic implementation for ZUNION, ZINTER, ZDIFF (store and non-store variants)
/// and ZINTERCARD.
/// C: zunionInterDiffGenericCommand (static)
fn zunion_inter_diff_generic_command(
    ctx: &mut CommandContext,
    _dstkey: Option<Vec<u8>>,
    numkeys_index: usize,
    op: i32,
    cardinality_only: bool,
) -> Result<(), RedisError> {
    // C: t_zset.c:2591-2868
    let setnum_bytes = ctx.arg_bytes(numkeys_index)?;
    let setnum = parse_long_from_bytes(setnum_bytes)?;
    if setnum < 1 {
        return Err(RedisError::runtime(
            b"at least 1 input key is needed for this command",
        ));
    }
    if setnum as usize > ctx.argc().saturating_sub(numkeys_index + 1) {
        return Err(RedisError::syntax(b"syntax error"));
    }

    // Allocate source array.
    let setnum_usize = setnum as usize;
    let mut src: Vec<ZSetOpSrc> = (0..setnum_usize)
        .map(|_| ZSetOpSrc {
            subject: None,
            weight: 1.0,
            iter: ZSetOpIterState::Empty,
        })
        .collect();

    // Read keys.
    for i in 0..setnum_usize {
        let j = numkeys_index + 1 + i;
        let _key = ctx.arg_bytes(j)?;
        // TODO(port): src[i].subject = ctx.db().lookup_key_read(key)
        src[i].weight = 1.0;
    }

    // Parse optional WEIGHTS / AGGREGATE / WITHSCORES / LIMIT.
    let mut aggregate = AggregateType::Sum;
    let mut withscores = false;
    let mut limit: i64 = 0;
    let mut j = numkeys_index + 1 + setnum_usize;
    while j < ctx.argc() {
        let opt = ctx.arg_bytes(j)?;
        match opt {
            b"WEIGHTS" | b"weights" if op != SET_OP_DIFF && !cardinality_only => {
                j += 1;
                for i in 0..setnum_usize {
                    let w_bytes = ctx.arg_bytes(j + i)?;
                    src[i].weight = parse_double_from_bytes(w_bytes)
                        .map_err(|_| RedisError::runtime(b"weight value is not a float"))?;
                }
                j += setnum_usize;
                continue;
            }
            b"AGGREGATE" | b"aggregate" if op != SET_OP_DIFF && !cardinality_only => {
                j += 1;
                let agg_bytes = ctx.arg_bytes(j)?;
                aggregate = match agg_bytes {
                    b"SUM" | b"sum" => AggregateType::Sum,
                    b"MIN" | b"min" => AggregateType::Min,
                    b"MAX" | b"max" => AggregateType::Max,
                    _ => return Err(RedisError::syntax(b"syntax error")),
                };
            }
            b"WITHSCORES" | b"withscores" if _dstkey.is_none() && !cardinality_only => {
                withscores = true;
            }
            b"LIMIT" | b"limit" if cardinality_only => {
                j += 1;
                limit = parse_long_from_bytes(ctx.arg_bytes(j)?)
                    .map_err(|_| RedisError::runtime(b"LIMIT can't be negative"))?;
                if limit < 0 {
                    return Err(RedisError::runtime(b"LIMIT can't be negative"));
                }
            }
            _ => return Err(RedisError::syntax(b"syntax error")),
        }
        j += 1;
    }

    // Sort for performance (INTER/UNION: smallest first).
    if op != SET_OP_DIFF {
        src.sort_by(|a, b| zui_length(a).cmp(&zui_length(b)));
    }

    // Build result.
    let mut dstzset = zset_create();
    let mut maxelelen: usize = 0;
    let mut totelelen: usize = 0;
    let mut cardinality: u64 = 0;
    let mut zval = ZSetOpVal::default();

    if op == SET_OP_INTER {
        if zui_length(&src[0]) > 0 {
            zui_init_iterator(&mut src[0]);
            while zui_next(&mut src[0], &mut zval) {
                let mut score = src[0].weight * zval.score;
                if score.is_nan() { score = 0.0; }
                let mut present_in_all = true;
                for k in 1..setnum_usize {
                    let mut value = 0.0;
                    if !zui_find(&src[k], &mut zval, &mut value) {
                        present_in_all = false;
                        break;
                    }
                    value *= src[k].weight;
                    zunion_inter_aggregate(&mut score, value, aggregate);
                }
                if present_in_all {
                    if cardinality_only {
                        cardinality += 1;
                        if limit > 0 && cardinality >= limit as u64 { break; }
                    } else {
                        let tmp = zui_new_bytes_from_val(&mut zval);
                        let elen = tmp.len();
                        let node = zsl_insert(&mut dstzset.zsl, score, tmp.clone());
                        dstzset.ht.insert(tmp, node);
                        totelelen += elen;
                        if elen > maxelelen { maxelelen = elen; }
                    }
                }
            }
            zui_clear_iterator(&mut src[0]);
        }
    } else if op == SET_OP_UNION {
        for i in 0..setnum_usize {
            if zui_length(&src[i]) == 0 { continue; }
            zui_init_iterator(&mut src[i]);
            while zui_next(&mut src[i], &mut zval) {
                let mut score = src[i].weight * zval.score;
                if score.is_nan() { score = 0.0; }
                let ele = zui_bytes_from_val(&mut zval);
                if let Some(existing) = dstzset.ht.get(&ele) {
                    let cur = existing.borrow().score;
                    let new_score = match aggregate {
                        AggregateType::Sum => {
                            let s = cur + score;
                            if s.is_nan() { 0.0 } else { s }
                        }
                        AggregateType::Min => if score < cur { score } else { cur },
                        AggregateType::Max => if score > cur { score } else { cur },
                    };
                    existing.borrow_mut().score = new_score;
                } else {
                    let tmp = zui_new_bytes_from_val(&mut zval);
                    let elen = tmp.len();
                    let node = zsl_insert(&mut dstzset.zsl, score, tmp.clone());
                    // PORT NOTE: C defers skiplist insertion to step 2; Rust
                    // inserts immediately for simplicity.  Logic is equivalent.
                    dstzset.ht.insert(tmp, node);
                    totelelen += elen;
                    if elen > maxelelen { maxelelen = elen; }
                }
            }
            zui_clear_iterator(&mut src[i]);
        }
    } else if op == SET_OP_DIFF {
        zdiff(&mut src, &mut dstzset, &mut maxelelen, &mut totelelen);
    } else {
        // TODO(architect): is panic correct here?
        panic!("Unknown operator in zunion_inter_diff_generic_command");
    }

    // Reply or store.
    let dst_len = dstzset.zsl.length;
    if let Some(ref dk) = _dstkey {
        if dst_len > 0 {
            zset_convert_to_listpack_if_needed(
                // TODO(port): need RedisObject wrapper for dstzset
                &mut create_dummy_zset_object(),
                maxelelen,
                totelelen,
            );
            // TODO(port): setKey(ctx, dk, dstobj)
            // TODO(port): notify_keyspace_event
            let _ = dk;
            ctx.reply_integer(dst_len as i64)
        } else {
            // TODO(port): dbDelete(ctx, dk)
            ctx.reply_integer(0)
        }
    } else if cardinality_only {
        ctx.reply_integer(cardinality as i64)
    } else {
        // Emit results in order.
        let resp2 = ctx.resp_version() == 2;
        let result_len = dst_len;
        if withscores && resp2 {
            ctx.reply_array_header((result_len * 2) as i64)?;
        } else {
            ctx.reply_array_header(result_len as i64)?;
        }
        let mut node_opt = dstzset.zsl.header.borrow().levels[0].forward.as_ref().map(Rc::clone);
        while let Some(node) = node_opt {
            let (ele, score) = {
                let nb = node.borrow();
                (nb.element.clone(), nb.score)
            };
            if withscores && !resp2 {
                ctx.reply_array_header(2)?;
            }
            ctx.reply_bulk(&ele)?;
            if withscores {
                ctx.reply_double(score)?;
            }
            node_opt = node.borrow().levels[0].forward.as_ref().map(Rc::clone);
        }
        Ok(())
    }
}

/// Helper placeholder — creates a dummy RedisObject wrapper.
/// TODO(port): remove once RedisObject::ZSet variant is available.
fn create_dummy_zset_object() -> RedisObject {
    // TODO(port): RedisObject::ZSet(...)
    todo!("TODO(port): create_dummy_zset_object — needs RedisObject::ZSet variant")
}

/// Parse an i64 from a Redis argument byte slice.
fn parse_long_from_bytes(b: &[u8]) -> Result<i64, RedisError> {
    // TODO(port): use getLongFromObjectOrReply equivalent
    let s = std::str::from_utf8(b).map_err(|_| RedisError::not_integer())?;
    // PORT NOTE: from_utf8 permitted here — numeric argument, not user data.
    s.parse::<i64>().map_err(|_| RedisError::not_integer())
}

/// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX]
/// C: zunionstoreCommand
pub fn zunionstore_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let dstkey = ctx.arg_bytes(1)?.to_vec();
    zunion_inter_diff_generic_command(ctx, Some(dstkey), 2, SET_OP_UNION, false)
}

/// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX]
/// C: zinterstoreCommand
pub fn zinterstore_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let dstkey = ctx.arg_bytes(1)?.to_vec();
    zunion_inter_diff_generic_command(ctx, Some(dstkey), 2, SET_OP_INTER, false)
}

/// ZDIFFSTORE destination numkeys key [key ...]
/// C: zdiffstoreCommand
pub fn zdiffstore_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let dstkey = ctx.arg_bytes(1)?.to_vec();
    zunion_inter_diff_generic_command(ctx, Some(dstkey), 2, SET_OP_DIFF, false)
}

/// ZUNION numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
/// C: zunionCommand
pub fn zunion_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zunion_inter_diff_generic_command(ctx, None, 1, SET_OP_UNION, false)
}

/// ZINTER numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
/// C: zinterCommand
pub fn zinter_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zunion_inter_diff_generic_command(ctx, None, 1, SET_OP_INTER, false)
}

/// ZINTERCARD numkeys key [key ...] [LIMIT limit]
/// C: zinterCardCommand
pub fn zintercard_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zunion_inter_diff_generic_command(ctx, None, 1, SET_OP_INTER, true)
}

/// ZDIFF numkeys key [key ...] [WITHSCORES]
/// C: zdiffCommand
pub fn zdiff_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zunion_inter_diff_generic_command(ctx, None, 1, SET_OP_DIFF, false)
}

// ─── ZRange result handler implementations ───────────────────────────────

impl<'a> ZRangeEmitter for ClientRangeEmitter<'a> {
    fn begin(&mut self, length: i64) {
        if length > 0 {
            let reply_len = if self.withscores && self.ctx.resp_version() == 2 {
                length * 2
            } else {
                length
            };
            // TODO(port): ctx.reply_array_header(reply_len)
            let _ = reply_len;
            self.deferred = false;
        } else {
            // TODO(port): self.ctx.reply_deferred_array_header()
            self.deferred = true;
        }
    }

    fn finalize(&mut self, result_count: usize) {
        if self.deferred {
            let count = if self.withscores && self.ctx.resp_version() == 2 {
                result_count * 2
            } else {
                result_count
            };
            // TODO(port): self.ctx.set_deferred_len(count)
            let _ = count;
        }
    }

    fn emit_buffer(&mut self, value: &[u8], score: f64) {
        if self.should_emit_array_len {
            // TODO(port): self.ctx.reply_array_header(2)
        }
        // TODO(port): self.ctx.reply_bulk(value)
        let _ = (value, score);
        if self.withscores {
            // TODO(port): self.ctx.reply_double(score)
        }
        self.emitted += 1;
    }

    fn emit_longlong(&mut self, value: i64, score: f64) {
        if self.should_emit_array_len {
            // TODO(port): self.ctx.reply_array_header(2)
        }
        // TODO(port): self.ctx.reply_bulk_longlong(value)
        let _ = (value, score);
        if self.withscores {
            // TODO(port): self.ctx.reply_double(score)
        }
        self.emitted += 1;
    }

    fn withscores(&self) -> bool { self.withscores }
    fn should_emit_array_len(&self) -> bool { self.should_emit_array_len }
}

impl<'a> ZRangeEmitter for StoreRangeEmitter<'a> {
    fn begin(&mut self, length: i64) {
        // Pre-size the destination object.
        let hint = if length > 0 { length as usize } else { 0 };
        // TODO(port): self.dstobj = Some(Box::new(zset_type_create(hint, 0)))
        let _ = hint;
    }

    fn finalize(&mut self, result_count: usize) {
        self.result_count = result_count;
        if result_count > 0 {
            // TODO(port): setKey, notifyKeyspaceEvent, server.dirty++
            // TODO(port): self.ctx.reply_integer(result_count)
        } else {
            // TODO(port): dbDelete, notifyKeyspaceEvent, server.dirty++
            // TODO(port): self.ctx.reply_integer(0)
        }
    }

    fn emit_buffer(&mut self, value: &[u8], score: f64) {
        // TODO(port): zset_add(dstobj, score, value, ZADD_IN_NONE, ...)
        let _ = (value, score);
    }

    fn emit_longlong(&mut self, value: i64, score: f64) {
        let bytes = format!("{}", value).into_bytes();
        self.emit_buffer(&bytes, score);
    }

    fn withscores(&self) -> bool { true }
    fn should_emit_array_len(&self) -> bool { false }
}

// ─── Generic ZRANGE by rank ───────────────────────────────────────────────

/// ZRANGE/ZREVRANGE over rank (index) bounds.
/// C: genericZrangebyrankCommand
pub fn generic_zrangebyrank_command(
    handler: &mut dyn ZRangeEmitter,
    zobj: &RedisObject,
    start: i64,
    end: i64,
    withscores: bool,
    reverse: bool,
) {
    // C: t_zset.c:3081-3172
    let llen = zset_length(zobj) as i64;
    let mut start = start;
    let mut end = end;
    if start < 0 { start += llen; }
    if end < 0 { end += llen; }
    if start < 0 { start = 0; }

    if start > end || start >= llen {
        handler.begin(0);
        handler.finalize(0);
        return;
    }
    if end >= llen { end = llen - 1; }
    let rangelen = (end - start + 1) as usize;
    handler.begin(rangelen as i64);

    // TODO(port): dispatch on encoding (listpack vs skiplist)
    // C: OBJ_ENCODING_LISTPACK path uses lpSeek + zzlNext/zzlPrev
    //    OBJ_ENCODING_SKIPLIST path uses zslGetElementByRank then traversal
    // Phase A: placeholder — emit nothing; Phase B wires up the encoding dispatch
    let _ = (zobj, withscores, reverse);

    handler.finalize(rangelen);
}

/// ZRANGEBYSCORE / ZREVRANGEBYSCORE generic implementation.
/// C: genericZrangebyscoreCommand
pub fn generic_zrangebyscore_command(
    handler: &mut dyn ZRangeEmitter,
    range: &ZRangeSpec,
    zobj: &RedisObject,
    offset: i64,
    limit: i64,
    reverse: bool,
) {
    // C: t_zset.c:3197-3302
    let mut rangelen: usize = 0;
    handler.begin(-1);
    if offset > 0 && offset >= zset_length(zobj) as i64 {
        handler.finalize(0);
        return;
    }
    // TODO(port): dispatch on encoding
    let _ = (range, zobj, offset, limit, reverse);
    handler.finalize(rangelen);
}

/// ZRANGEBYLEX / ZREVRANGEBYLEX generic implementation.
/// C: genericZrangebylexCommand
pub fn generic_zrangebylex_command(
    handler: &mut dyn ZRangeEmitter,
    range: &ZLexRangeSpec,
    zobj: &RedisObject,
    withscores: bool,
    offset: i64,
    limit: i64,
    reverse: bool,
) {
    // C: t_zset.c:3470-3571
    let rangelen: usize = 0;
    handler.begin(-1);
    // TODO(port): dispatch on encoding
    let _ = (range, zobj, withscores, offset, limit, reverse);
    handler.finalize(rangelen);
}

/// Master ZRANGE dispatcher — handles ZRANGE, ZRANGESTORE, and the deprecated
/// Z[REV]RANGE[BYSCORE|BYLEX] family.
/// C: zrangeGenericCommand
pub fn zrange_generic_command(
    handler: &mut dyn ZRangeEmitter,
    ctx: &mut CommandContext,
    argc_start: usize,
    store: bool,
    rangetype: ZRangeType,
    direction: ZRangeDirection,
) -> Result<(), RedisError> {
    // C: t_zset.c:3597-3733
    let _key = ctx.arg_bytes(argc_start)?.to_vec();
    let minidx = argc_start + 1;
    let maxidx = argc_start + 2;

    let mut opt_start: i64 = 0;
    let mut opt_end: i64 = 0;
    let mut opt_withscores = false;
    let mut opt_offset: i64 = 0;
    let mut opt_limit: i64 = -1;
    let mut direction = direction;
    let mut rangetype = rangetype;

    // Parse optional trailing arguments.
    let mut j = argc_start + 3;
    while j < ctx.argc() {
        let leftargs = ctx.argc() - j - 1;
        let arg = ctx.arg_bytes(j)?;
        match arg {
            b"withscores" | b"WITHSCORES" if !store => { opt_withscores = true; }
            b"limit" | b"LIMIT" if leftargs >= 2 => {
                opt_offset = parse_long_from_bytes(ctx.arg_bytes(j + 1)?)?;
                opt_limit = parse_long_from_bytes(ctx.arg_bytes(j + 2)?)?;
                j += 2;
            }
            b"rev" | b"REV" if direction == ZRangeDirection::Auto => {
                direction = ZRangeDirection::Reverse;
            }
            b"bylex" | b"BYLEX" if rangetype == ZRangeType::Auto => {
                rangetype = ZRangeType::Lex;
            }
            b"byscore" | b"BYSCORE" if rangetype == ZRangeType::Auto => {
                rangetype = ZRangeType::Score;
            }
            _ => return Err(RedisError::syntax(b"syntax error")),
        }
        j += 1;
    }

    if direction == ZRangeDirection::Auto { direction = ZRangeDirection::Forward; }
    if rangetype == ZRangeType::Auto { rangetype = ZRangeType::Rank; }

    if opt_limit != -1 && rangetype == ZRangeType::Rank {
        return Err(RedisError::runtime(
            b"syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }
    if opt_withscores && rangetype == ZRangeType::Lex {
        return Err(RedisError::runtime(
            b"syntax error, WITHSCORES not supported in combination with BYLEX",
        ));
    }

    // Swap min/max indices for reversed score/lex ranges.
    let (final_minidx, final_maxidx) = if direction == ZRangeDirection::Reverse
        && (rangetype == ZRangeType::Score || rangetype == ZRangeType::Lex)
    {
        (maxidx, minidx)
    } else {
        (minidx, maxidx)
    };

    // Parse range.
    match rangetype {
        ZRangeType::Auto | ZRangeType::Rank => {
            opt_start = parse_long_from_bytes(ctx.arg_bytes(final_minidx)?)?;
            opt_end = parse_long_from_bytes(ctx.arg_bytes(final_maxidx)?)?;
        }
        ZRangeType::Score => {
            // TODO(port): zsl_parse_range blocked on RedisObject byte accessor
        }
        ZRangeType::Lex => {
            // TODO(port): zset_parse_lex_range blocked on RedisObject byte accessor
        }
    }

    if opt_withscores || store {
        // TODO(port): enable score emission on handler
    }

    // Lookup key.
    // TODO(port): zobj = ctx.db().lookup_key_read(key)
    let _zobj_placeholder: Option<RedisObject> = None;

    match rangetype {
        ZRangeType::Auto | ZRangeType::Rank => {
            // TODO(port): generic_zrangebyrank_command(handler, zobj, opt_start, opt_end, ...)
            let _ = (opt_start, opt_end, opt_withscores, store);
        }
        ZRangeType::Score => {
            // TODO(port): generic_zrangebyscore_command(handler, &range, zobj, opt_offset, opt_limit, ...)
            let _ = (opt_offset, opt_limit);
        }
        ZRangeType::Lex => {
            // TODO(port): generic_zrangebylex_command(handler, &lexrange, zobj, ...)
        }
    }
    Ok(())
}

/// ZRANGESTORE <dst> <src> <min> <max> [BYSCORE | BYLEX] [REV] [LIMIT offset count]
/// C: zrangestoreCommand
pub fn zrangestore_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let dstkey = ctx.arg_bytes(1)?.to_vec();
    let mut handler = StoreRangeEmitter {
        ctx,
        dstkey,
        dstobj: None,
        result_count: 0,
    };
    // TODO(port): zrange_generic_command needs split borrows; placeholder
    // zrange_generic_command(&mut handler, ctx, 2, true, ZRangeType::Auto, ZRangeDirection::Auto)
    let _ = handler;
    Ok(()) // TODO(port): wire up handler + ctx
}

/// ZRANGE <key> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count]
/// C: zrangeCommand
pub fn zrange_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut handler = ClientRangeEmitter {
        ctx,
        withscores: false,
        should_emit_array_len: false,
        deferred: false,
        emitted: 0,
    };
    // TODO(port): wire up zrange_generic_command
    let _ = handler;
    Ok(())
}

/// ZREVRANGE <key> <start> <stop> [WITHSCORES]
/// C: zrevrangeCommand
pub fn zrevrange_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut handler = ClientRangeEmitter {
        ctx,
        withscores: false,
        should_emit_array_len: false,
        deferred: false,
        emitted: 0,
    };
    // TODO(port): wire up zrange_generic_command with RANK + REVERSE
    let _ = handler;
    Ok(())
}

/// ZRANGEBYSCORE <key> <min> <max> [WITHSCORES] [LIMIT offset count]
/// C: zrangebyscoreCommand
pub fn zrangebyscore_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut handler = ClientRangeEmitter {
        ctx,
        withscores: false,
        should_emit_array_len: false,
        deferred: false,
        emitted: 0,
    };
    let _ = handler;
    Ok(()) // TODO(port): wire up with SCORE + FORWARD
}

/// ZREVRANGEBYSCORE <key> <max> <min> [WITHSCORES] [LIMIT offset count]
/// C: zrevrangebyscoreCommand
pub fn zrevrangebyscore_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut handler = ClientRangeEmitter {
        ctx,
        withscores: false,
        should_emit_array_len: false,
        deferred: false,
        emitted: 0,
    };
    let _ = handler;
    Ok(()) // TODO(port): wire up with SCORE + REVERSE
}

/// ZRANGEBYLEX <key> <min> <max> [LIMIT offset count]
/// C: zrangebylexCommand
pub fn zrangebylex_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut handler = ClientRangeEmitter {
        ctx,
        withscores: false,
        should_emit_array_len: false,
        deferred: false,
        emitted: 0,
    };
    let _ = handler;
    Ok(()) // TODO(port): wire up with LEX + FORWARD
}

/// ZREVRANGEBYLEX <key> <max> <min> [LIMIT offset count]
/// C: zrevrangebylexCommand
pub fn zrevrangebylex_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut handler = ClientRangeEmitter {
        ctx,
        withscores: false,
        should_emit_array_len: false,
        deferred: false,
        emitted: 0,
    };
    let _ = handler;
    Ok(()) // TODO(port): wire up with LEX + REVERSE
}

/// ZCOUNT key min max
/// C: zcountCommand
pub fn zcount_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:3318-3390
    let _key = ctx.arg_bytes(1)?.to_vec();
    // TODO(port): parse range from argv[2..3] via zsl_parse_range
    // TODO(port): lookup zobj, dispatch on encoding
    // Placeholder:
    ctx.reply_integer(0)
}

/// ZLEXCOUNT key min max
/// C: zlexcountCommand
pub fn zlexcount_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:3392-3467
    let _key = ctx.arg_bytes(1)?.to_vec();
    // TODO(port): parse lex range, lookup zobj, dispatch on encoding
    ctx.reply_integer(0)
}

/// ZCARD key
/// C: zcardCommand
pub fn zcard_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:3735-3742
    let _key = ctx.arg_bytes(1)?.to_vec();
    // TODO(port): lookup zobj, return zset_length(zobj)
    ctx.reply_integer(0)
}

/// ZSCORE key member
/// C: zscoreCommand
pub fn zscore_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:3744-3756
    let _key = ctx.arg_bytes(1)?.to_vec();
    let _member = ctx.arg_bytes(2)?.to_vec();
    // TODO(port): lookup zobj, call zset_score
    ctx.reply_null()
}

/// ZMSCORE key member [member ...]
/// C: zmscoreCommand
pub fn zmscore_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:3758-3774
    let _key = ctx.arg_bytes(1)?.to_vec();
    let count = ctx.argc() - 2;
    ctx.reply_array_header(count as i64)?;
    for j in 2..ctx.argc() {
        let _member = ctx.arg_bytes(j)?;
        // TODO(port): lookup zobj, call zset_score for each member
        ctx.reply_null()?;
    }
    Ok(())
}

/// Generic ZRANK / ZREVRANK implementation.
/// C: zrankGenericCommand
fn zrank_generic_command(ctx: &mut CommandContext, reverse: bool) -> Result<(), RedisError> {
    // C: t_zset.c:3776-3818
    let argc = ctx.argc();
    if argc > 4 {
        return Err(RedisError::wrong_number_of_args(b"ZRANK"));
    }
    let mut opt_withscore = false;
    if argc > 3 {
        let opt = ctx.arg_bytes(3)?;
        if opt.eq_ignore_ascii_case(b"withscore") {
            opt_withscore = true;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }
    let _key = ctx.arg_bytes(1)?.to_vec();
    let _ele = ctx.arg_bytes(2)?.to_vec();
    // TODO(port): lookup zobj, call zset_rank
    // rank = zset_rank(zobj, ele, reverse, opt_withscore.then(|| &mut score))
    let rank: i64 = -1; // placeholder
    if rank >= 0 {
        if opt_withscore {
            ctx.reply_array_header(2)?;
        }
        ctx.reply_integer(rank)?;
        if opt_withscore {
            ctx.reply_double(0.0)?; // TODO(port): actual score
        }
    } else if opt_withscore {
        ctx.reply_null_array()
    } else {
        ctx.reply_null()
    }
}

/// ZRANK key member [WITHSCORE]
/// C: zrankCommand
pub fn zrank_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zrank_generic_command(ctx, false)
}

/// ZREVRANK key member [WITHSCORE]
/// C: zrevrankCommand
pub fn zrevrank_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zrank_generic_command(ctx, true)
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count]
/// C: zscanCommand
pub fn zscan_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:3828-3835
    // TODO(port): parseScanCursorOrReply, lookupKeyRead, scanGenericCommand
    ctx.reply_array_header(2)?;
    ctx.reply_bulk(b"0")?;
    ctx.reply_array_header(0)
}

// ─── ZPOPMIN / ZPOPMAX ────────────────────────────────────────────────────

/// Emit the initial array header for a zpop-family reply.
/// C: addZpopInitialReply
fn add_zpop_initial_reply(
    ctx: &mut CommandContext,
    emitkey: bool,
    use_nested_array: bool,
    rangelen: i64,
    key: &[u8],
) -> Result<(), RedisError> {
    // C: t_zset.c:3837-3854
    if !use_nested_array && !emitkey {
        ctx.reply_array_header(rangelen * 2)
    } else if use_nested_array && !emitkey {
        ctx.reply_array_header(rangelen)
    } else if !use_nested_array && emitkey {
        ctx.reply_array_header(rangelen * 2 + 1)?;
        ctx.reply_bulk(key)
    } else {
        // use_nested_array && emitkey
        ctx.reply_array_header(2)?;
        ctx.reply_bulk(key)?;
        ctx.reply_array_header(rangelen)
    }
}

/// Generic ZPOP implementation shared by ZPOPMIN, ZPOPMAX, BZPOPMIN,
/// BZPOPMAX, and ZMPOP.
/// C: genericZpopCommand
pub fn generic_zpop_command(
    ctx: &mut CommandContext,
    keys: &[Vec<u8>],
    where_: i32,
    emitkey: bool,
    count: i64,
    use_nested_array: bool,
    reply_nil_when_empty: bool,
    deleted: Option<&mut bool>,
) -> Result<(), RedisError> {
    // C: t_zset.c:3876-3999
    if let Some(d) = deleted { *d = false; }

    // Find the first non-empty key.
    let mut found_key: Option<&[u8]> = None;
    for key in keys {
        // TODO(port): zobj = ctx.db().lookup_key_write(key)
        // if zobj is Some and is OBJ_ZSET with length > 0: found_key = Some(key); break
        found_key = Some(key); // placeholder
        break;
    }

    if found_key.is_none() {
        if reply_nil_when_empty {
            return ctx.reply_null_array();
        } else {
            return ctx.reply_empty_array();
        }
    }

    let count = if count == -1 { 1 } else { count };
    if count == 0 {
        return ctx.reply_empty_array();
    }

    let key = found_key.unwrap();
    // TODO(port): actual pop logic — get element from listpack or skiplist
    // per where_ (ZSET_MIN vs ZSET_MAX), emit replies, delete key if empty.
    let rangelen: i64 = 1; // placeholder
    add_zpop_initial_reply(ctx, emitkey, use_nested_array, rangelen, key)?;
    // TODO(port): emit elements + scores, call zset_del, signal_modified_key

    let _ = (where_, use_nested_array, deleted);
    Ok(())
}

/// ZPOPMIN key [<count>]
/// C: zpopminCommand
pub fn zpopmin_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:4018-4021
    if ctx.argc() > 3 {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let count: i64 = if ctx.argc() == 3 {
        parse_long_from_bytes(ctx.arg_bytes(2)?)?
    } else {
        -1
    };
    let use_nested_array = ctx.resp_version() > 2 && count != -1;
    let key = ctx.arg_bytes(1)?.to_vec();
    generic_zpop_command(ctx, &[key], ZSET_MIN, false, count, use_nested_array, false, None)
}

/// ZPOPMAX key [<count>]
/// C: zpopmaxCommand
pub fn zpopmax_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:4023-4026
    if ctx.argc() > 3 {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let count: i64 = if ctx.argc() == 3 {
        parse_long_from_bytes(ctx.arg_bytes(2)?)?
    } else {
        -1
    };
    let use_nested_array = ctx.resp_version() > 2 && count != -1;
    let key = ctx.arg_bytes(1)?.to_vec();
    generic_zpop_command(ctx, &[key], ZSET_MAX, false, count, use_nested_array, false, None)
}

/// Blocking ZPOP implementation shared by BZPOPMIN, BZPOPMAX, BZMPOP.
/// C: blockingGenericZpopCommand
pub fn blocking_generic_zpop_command(
    ctx: &mut CommandContext,
    keys: &[Vec<u8>],
    where_: i32,
    timeout_idx: usize,
    count: i64,
    use_nested_array: bool,
    reply_nil_when_empty: bool,
) -> Result<(), RedisError> {
    // C: t_zset.c:4041-4093
    let _timeout_bytes = ctx.arg_bytes(timeout_idx)?;
    // TODO(port): getTimeoutFromObjectOrReply

    for key in keys {
        // TODO(port): lookup key, if non-empty zset call generic_zpop_command
        let _ = key;
    }

    // TODO(port): if client has deny_blocking flag, reply null array
    // TODO(port): blockForKeys(c, BLOCKED_ZSET, keys, numkeys, timeout, 0)
    // TODO(architect): blocking infrastructure not yet available
    let _ = (where_, count, use_nested_array, reply_nil_when_empty);
    ctx.reply_null_array()
}

/// BZPOPMIN key [key ...] timeout
/// C: bzpopminCommand
pub fn bzpopmin_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:4096-4098
    let argc = ctx.argc();
    let keys: Vec<Vec<u8>> = (1..argc - 1)
        .map(|i| ctx.arg_bytes(i).map(|b: &[u8]| b.to_vec()))
        .collect::<Result<_, _>>()?;
    blocking_generic_zpop_command(ctx, &keys, ZSET_MIN, argc - 1, -1, false, false)
}

/// BZPOPMAX key [key ...] timeout
/// C: bzpopmaxCommand
pub fn bzpopmax_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:4100-4102
    let argc = ctx.argc();
    let keys: Vec<Vec<u8>> = (1..argc - 1)
        .map(|i| ctx.arg_bytes(i).map(|b: &[u8]| b.to_vec()))
        .collect::<Result<_, _>>()?;
    blocking_generic_zpop_command(ctx, &keys, ZSET_MAX, argc - 1, -1, false, false)
}

// ─── ZRANDMEMBER ──────────────────────────────────────────────────────────

/// Emit a fixed number of random (element, score?) pairs from a listpack.
/// C: zrandmemberReplyWithListpack (static)
fn zrandmember_reply_with_listpack(
    ctx: &mut CommandContext,
    count: usize,
    keys: &[ListpackEntry],
    vals: Option<&[ListpackEntry]>,
) -> Result<(), RedisError> {
    // C: t_zset.c:4105-4119
    for i in 0..count {
        if vals.is_some() && ctx.resp_version() > 2 {
            ctx.reply_array_header(2)?;
        }
        if let Some(ref sv) = keys[i].sval {
            ctx.reply_bulk(sv)?;
        } else {
            ctx.reply_integer(keys[i].lval)?;
        }
        if let Some(vs) = vals {
            let score = if let Some(ref sv) = vs[i].sval {
                parse_double_from_bytes(sv).unwrap_or(0.0)
            } else {
                vs[i].lval as f64
            };
            ctx.reply_double(score)?;
        }
    }
    Ok(())
}

/// ZRANDMEMBER key count [WITHSCORES] implementation when count is given.
/// C: zrandmemberWithCountCommand
pub fn zrandmember_with_count_command(
    ctx: &mut CommandContext,
    l: i64,
    withscores: bool,
) -> Result<(), RedisError> {
    // C: t_zset.c:4131-4324
    let _key = ctx.arg_bytes(1)?;
    // TODO(port): lookup zobj, dispatch on encoding, handle unique/non-unique cases
    // C: CASE 1 (non-unique), CASE 2.5 (listpack unique), CASE 3 (sub-strategy),
    //    CASE 4 (rejection sampling)
    let _ = (l, withscores);
    ctx.reply_empty_array()
}

/// ZRANDMEMBER key [<count> [WITHSCORES]]
/// C: zrandmemberCommand
pub fn zrandmember_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: t_zset.c:4326-4356
    let argc = ctx.argc();
    if argc >= 3 {
        let l = parse_long_from_bytes(ctx.arg_bytes(2)?)?;
        let mut withscores = false;
        if argc > 4 {
            return Err(RedisError::wrong_number_of_args(b"ZRANDMEMBER"));
        }
        if argc == 4 {
            if !ctx.arg_bytes(3)?.eq_ignore_ascii_case(b"withscores") {
                return Err(RedisError::syntax(b"syntax error"));
            }
            withscores = true;
            if l < i64::MIN / 2 || l > i64::MAX / 2 {
                return Err(RedisError::out_of_range());
            }
        }
        return zrandmember_with_count_command(ctx, l, withscores);
    }

    // Single element (no count).
    let _key = ctx.arg_bytes(1)?;
    // TODO(port): lookup zobj, zset_type_random_element, reply
    ctx.reply_null()
}

// ─── ZMPOP / BZMPOP ───────────────────────────────────────────────────────

/// Generic ZMPOP / BZMPOP implementation.
/// C: zmpopGenericCommand
fn zmpop_generic_command(
    ctx: &mut CommandContext,
    numkeys_idx: usize,
    is_block: bool,
) -> Result<(), RedisError> {
    // C: t_zset.c:4362-4413
    let numkeys = parse_long_from_bytes(ctx.arg_bytes(numkeys_idx)?)?;
    if numkeys < 1 {
        return Err(RedisError::runtime(b"numkeys should be greater than 0"));
    }

    let where_idx = numkeys_idx + numkeys as usize + 1;
    if where_idx >= ctx.argc() {
        return Err(RedisError::syntax(b"syntax error"));
    }
    let where_ = match ctx.arg_bytes(where_idx)? {
        b if b.eq_ignore_ascii_case(b"MIN") => ZSET_MIN,
        b if b.eq_ignore_ascii_case(b"MAX") => ZSET_MAX,
        _ => return Err(RedisError::syntax(b"syntax error")),
    };

    let mut count: i64 = -1;
    let mut j = where_idx + 1;
    while j < ctx.argc() {
        let opt = ctx.arg_bytes(j)?;
        let moreargs = ctx.argc() - 1 - j;
        if count == -1 && opt.eq_ignore_ascii_case(b"COUNT") && moreargs >= 1 {
            j += 1;
            count = parse_long_from_bytes(ctx.arg_bytes(j)?)?;
            if count < 1 {
                return Err(RedisError::runtime(b"count should be greater than 0"));
            }
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
        j += 1;
    }
    if count == -1 { count = 1; }

    let keys: Vec<Vec<u8>> = ((numkeys_idx + 1)..(numkeys_idx + 1 + numkeys as usize))
        .map(|i| ctx.arg_bytes(i).map(|b: &[u8]| b.to_vec()))
        .collect::<Result<_, _>>()?;

    if is_block {
        blocking_generic_zpop_command(ctx, &keys, where_, 1, count, true, true)
    } else {
        generic_zpop_command(ctx, &keys, where_, true, count, true, true, None)
    }
}

/// ZMPOP numkeys key [<key> ...] MIN|MAX [COUNT count]
/// C: zmpopCommand
pub fn zmpop_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zmpop_generic_command(ctx, 1, false)
}

/// BZMPOP timeout numkeys key [<key> ...] MIN|MAX [COUNT count]
/// C: bzmpopCommand
pub fn bzmpop_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    zmpop_generic_command(ctx, 2, true)
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/t_zset.c  (4423 lines, ~80 functions)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         140
//   port_notes:    15
//   unsafe_blocks: 0
//   notes:         Phase A complete. Skiplist uses Rc<RefCell<>> for safety
//                  (no unsafe); Phase B should evaluate arena allocation.
//                  Listpack-backed paths (zzl*) stub-implemented pending
//                  redis-ds Phase 4. Command bodies have correct signatures
//                  and arg-parsing logic; db access (lookup/modify/notify)
//                  blocked on Phase 3 CommandContext API.
//                  0 real syntax errors; only expected cross-crate name-
//                  resolution errors from rustc --emit=metadata check.
// ──────────────────────────────────────────────────────────────────────────
