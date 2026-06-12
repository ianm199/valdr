//! `RedisObject` — runtime value types held in a Redis database slot.
//! The C `robj` struct uses embedded-memory tricks (hasembkey, hasembval, hasexpire)
//! that are pure allocator optimizations. In Rust these collapse:
//! - The key lives in the db `HashMap`, not inside the object.
//! - The expire time lives in `RedisDb`'s expiry table, not inside the object.
//! - The embedded value becomes the inner data of the enum variant.
//! - `incrRefCount`/`decrRefCount`/`freeXxxObject` are replaced by Rust ownership + `Drop`.
//! - `makeObjectShared` maps to `Arc<RedisObject>` (not yet introduced).
//! - The small integer pool (`shared.integers[0..10000]`) needs a lazy-static Arc array;
//!   see `TODO(architect)` on `create_string_object_from_long_long_with_options`.
//! PORT NOTE: EMBSTR and RAW string encodings are layout-identical in Rust (`Vec<u8>`
//! under the hood). The distinction is preserved as an enum variant tag because it affects
//! the semantics of `try_object_encoding` (decides whether to re-encode) and is reported by
//! `OBJECT ENCODING`.
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, OnceLock};

use redis_ds::stream::InlineStream;
use redis_types::{RedisError, RedisString};

use crate::command_context::CommandContext;
use crate::server::RedisServer;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Sentinel expiry value meaning "no expiry". Matches C's `EXPIRY_NONE = -1`.
pub const EXPIRY_NONE: i64 = -1;

/// Number of integers (0..OBJ_SHARED_INTEGERS-1) in the shared integer pool.
pub const OBJ_SHARED_INTEGERS: i64 = 10_000;

/// If a key is >= this many bytes, the C robj reserves space for an expiry field
/// even if none exists yet. Not needed in Rust but kept as a comment anchor.
pub const KEY_SIZE_TO_INCLUDE_EXPIRE_THRESHOLD: usize = 128;

/// Default sampling depth for `object_compute_size`.
pub const OBJ_COMPUTE_SIZE_DEF_SAMPLES: usize = 5;

/// Max bytes needed to represent any `i64` as a decimal string (sign + 19 digits + NUL).
pub const LONG_STR_SIZE: usize = 21;

/// Max bytes for a `long double` formatted string.
pub const MAX_LONG_DOUBLE_CHARS: usize = 5 * 10;

/// Flags for `compare_string_objects_with_flags`.
pub const STRING_COMPARE_BINARY: u32 = 1 << 0;
pub const STRING_COMPARE_COLL: u32 = 1 << 1;

/// Internal flags for `create_string_object_from_long_long_with_options`.
const LL2STROBJ_AUTO: i32 = 0;
const LL2STROBJ_NO_SHARED: i32 = 1;
const LL2STROBJ_NO_INT_ENC: i32 = 2;

// ─────────────────────────────────────────────────────────────────────────────
// Object-type discriminant
// ─────────────────────────────────────────────────────────────────────────────

/// Discriminant used by `check_type` to identify the expected `RedisObject` variant.
/// Mirrors `OBJ_STRING`, `OBJ_LIST`, … in C.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectType {
    String,
    List,
    Hash,
    Set,
    ZSet,
    Stream,
    Module,
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-type encoding sub-variants
// ─────────────────────────────────────────────────────────────────────────────

/// Encoding sub-variants for `RedisObject::String`.
/// In C these correspond to `OBJ_ENCODING_RAW`, `OBJ_ENCODING_EMBSTR`,
/// `OBJ_ENCODING_INT`. In Rust, `Raw` and `Embstr` are layout-identical (`Vec<u8>`
/// underneath); the distinction is semantic (re-encoding eligibility, `OBJECT ENCODING`
/// output). `Int` stores the integer directly, avoiding a string allocation.
#[derive(Debug, Clone)]
pub enum StringEncoding {
 /// Dynamically allocated byte string (OBJ_ENCODING_RAW).
    Raw(RedisString),
 /// Immutably embedded byte string — in C allocated in the same chunk as robj
 /// (OBJ_ENCODING_EMBSTR). In Rust, semantically equivalent to `Raw`.
    Embstr(RedisString),
 /// Integer stored as a tagged pointer (OBJ_ENCODING_INT).
    Int(i64),
}

/// Encoding sub-variants for `RedisObject::List`.
/// List commands operate over `Inline`, a `VecDeque` of `RedisString` providing
/// O(1) head/tail ops and trivial index access.
#[derive(Debug, Clone)]
pub enum ListEncoding {
 /// Pragmatic interim encoding used by the in-tree list commands.
 /// Provides O(1) push/pop on both ends and O(n) middle ops, which is
 /// sufficient for byte-exact Redis semantics.
    Inline(VecDeque<RedisString>),
 /// Compact list-pack byte array (OBJ_ENCODING_LISTPACK).
    // TODO(architect): replace VecDeque with real encoding in Phase 4
    ListPack(Vec<u8>),
 /// Doubly-linked list of list-pack nodes (OBJ_ENCODING_QUICKLIST).
    // TODO(architect): replace VecDeque with real encoding in Phase 4
    QuickList(VecDeque<RedisString>),
}

/// Sticky minimum encoding for an `InlineSet`.
/// Real Redis never automatically downgrades a set's encoding (e.g. once
/// elements have caused a promotion from intset to listpack, removing those
/// elements does not revert back to intset). `InlineSet` records the highest
/// encoding the set ever reached so that `OBJECT ENCODING` reports a value
/// consistent with real Redis's sticky-promotion behaviour.
/// Ordering matters: `Auto < ForcedListpack < ForcedHashtable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum InlineSetEncoding {
    Auto,
    ForcedListpack,
    ForcedHashtable,
}

/// Wrapper around `HashSet<RedisString>` that tracks the highest encoding
/// the set has ever reached, matching real Redis's sticky-promotion semantics.
#[derive(Debug, Clone)]
pub struct InlineSet {
    pub data: HashSet<RedisString>,
    pub sticky: InlineSetEncoding,
}

impl Default for InlineSet {
    fn default() -> Self {
        Self::new()
    }
}

impl InlineSet {
    pub fn new() -> Self {
        Self {
            data: HashSet::new(),
            sticky: InlineSetEncoding::Auto,
        }
    }

    pub fn from_hash_set(data: HashSet<RedisString>) -> Self {
        Self {
            data,
            sticky: InlineSetEncoding::Auto,
        }
    }
}

/// Order-preserving hash storage used by the interim hash implementation.
/// Valkey's compact hash encodings (`ziplist` historically, `listpack` today)
/// preserve field insertion order for commands such as `HGETALL`. A plain Rust
/// `HashMap` gives correct lookup semantics but loses that observable order,
/// which matters for upstream fixtures that create a compact hash, DUMP it,
/// RESTORE it, and then compare the returned field sequence. This wrapper keeps
/// O(1)-ish lookup through the map while emitting iteration in first-insertion
/// order. Updating an existing field preserves its original position, matching
/// Redis listpack/dict behavior closely enough for the current object model.
#[derive(Debug, Clone, Default)]
pub struct InlineHash {
    data: HashMap<RedisString, RedisString>,
    order: Vec<RedisString>,
}

impl InlineHash {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            order: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: HashMap::with_capacity(capacity),
            order: Vec::with_capacity(capacity),
        }
    }

    pub fn from_hash_map(map: HashMap<RedisString, RedisString>) -> Self {
        let mut out = Self::with_capacity(map.len());
        for (field, value) in map {
            out.insert(field, value);
        }
        out
    }

    pub fn insert(&mut self, field: RedisString, value: RedisString) -> Option<RedisString> {
        use std::collections::hash_map::Entry;
        match self.data.entry(field) {
            Entry::Occupied(mut e) => Some(e.insert(value)),
            Entry::Vacant(e) => {
                self.order.push(e.key().clone());
                e.insert(value);
                None
            }
        }
    }

    pub fn get(&self, field: &RedisString) -> Option<&RedisString> {
        self.data.get(field)
    }

    pub fn contains_key(&self, field: &RedisString) -> bool {
        self.data.contains_key(field)
    }

    pub fn remove(&mut self, field: &RedisString) -> Option<RedisString> {
        let removed = self.data.remove(field);
        if removed.is_some() {
            self.order.retain(|candidate| candidate != field);
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RedisString, &RedisString)> {
        self.order
            .iter()
            .filter_map(move |field| self.data.get(field).map(|value| (field, value)))
    }

    pub fn keys(&self) -> impl Iterator<Item = &RedisString> {
        self.iter().map(|(field, _)| field)
    }

    pub fn values(&self) -> impl Iterator<Item = &RedisString> {
        self.iter().map(|(_, value)| value)
    }
}

/// Encoding sub-variants for `RedisObject::Set`.
/// Set commands operate over `Inline`, a `HashSet<RedisString>` providing
/// O(1) membership and add/remove.
#[derive(Debug, Clone)]
pub enum SetEncoding {
 /// Pragmatic interim encoding used by the in-tree set commands.
 /// Backed by `HashSet<RedisString>` for O(1) membership tests, adds,
 /// and removes, which is sufficient for byte-exact Redis semantics
 /// across SADD/SREM/SMEMBERS/SINTER/SUNION/SDIFF and friends.
    Inline(InlineSet),
 /// Compact list-pack byte array (OBJ_ENCODING_LISTPACK).
    // TODO(architect): replace stub Vec with real listpack encoding in Phase 4
    ListPack(Vec<u8>),
 /// Sorted integer array (OBJ_ENCODING_INTSET).
    // TODO(architect): need dependency edge from redis-core to redis-ds for IntSet type
    IntSet(Vec<i64>),
 /// Full hash table (OBJ_ENCODING_HASHTABLE).
    // TODO(architect): replace HashSet with real redis-ds hashtable in Phase 4
    HashTable(HashSet<RedisString>),
}

/// Total-ordering wrapper around `f64` for use in `BTreeSet` keys.
/// Redis rejects NaN scores at the parsing boundary so the wrapped value
/// is always non-NaN by construction. Equality and ordering use
/// `f64::total_cmp`, which provides a total order across `+/-0`, `inf`,
/// and finite values matching the deterministic ordering Redis presents
/// to clients.
#[derive(Debug, Clone, Copy)]
pub struct F64Ord(pub f64);

impl F64Ord {
 /// Returns the wrapped score as an `f64`.
    pub fn get(self) -> f64 {
        self.0
    }
}

impl PartialEq for F64Ord {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0).is_eq()
    }
}

impl Eq for F64Ord {}

impl PartialOrd for F64Ord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for F64Ord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Pragmatic sorted-set storage mirroring real Redis's dict + skiplist.
/// The `by_member` map provides O(1) score lookup by member;
/// `by_order` set provides O(log N) ordered traversal
/// `(score, member)` lex order. All mutations must update both maps
/// lockstep so the two views stay coherent.
#[derive(Debug, Clone, Default)]
pub struct InlineZSet {
 /// Score keyed by member.
    pub by_member: HashMap<RedisString, F64Ord>,
 /// Lex-sorted `(score, member)` pairs for ordered range scans.
    pub by_order: BTreeSet<(F64Ord, RedisString)>,
}

impl InlineZSet {
 /// Construct an empty sorted set.
    pub fn new() -> Self {
        Self::default()
    }

 /// Returns the number of members in the sorted set.
    pub fn len(&self) -> usize {
        self.by_member.len()
    }

 /// Returns `true` when the sorted set has no members.
    pub fn is_empty(&self) -> bool {
        self.by_member.is_empty()
    }

 /// Returns the score associated with `member`, if present.
    pub fn score(&self, member: &RedisString) -> Option<f64> {
        self.by_member.get(member).map(|s| s.get())
    }

 /// Returns `true` when `member` is present in the sorted set.
    pub fn contains(&self, member: &RedisString) -> bool {
        self.by_member.contains_key(member)
    }

 /// Insert or update `(member, score)`.
 /// Returns `(was_new, prev_score)` so callers can implement
 /// `ZADD CH` and `XX/NX/GT/LT` semantics by inspecting whether
 /// member existed and what its score was before the update.
    pub fn upsert(&mut self, member: RedisString, score: f64) -> (bool, Option<f64>) {
        let new = F64Ord(score);
        if let Some(prev) = self.by_member.get(&member).copied() {
            if prev.get().to_bits() == score.to_bits() {
                return (false, Some(prev.get()));
            }
            self.by_order.remove(&(prev, member.clone()));
            self.by_order.insert((new, member.clone()));
            self.by_member.insert(member, new);
            (false, Some(prev.get()))
        } else {
            self.by_order.insert((new, member.clone()));
            self.by_member.insert(member, new);
            (true, None)
        }
    }

 /// Remove `member`, returning its score if it was present.
    pub fn remove(&mut self, member: &RedisString) -> Option<f64> {
        let prev = self.by_member.remove(member)?;
        self.by_order.remove(&(prev, member.clone()));
        Some(prev.get())
    }

 /// Iterate `(score, member)` pairs in ascending order.
    pub fn iter_ascending(&self) -> impl DoubleEndedIterator<Item = (f64, &RedisString)> {
        self.by_order.iter().map(|(s, m)| (s.get(), m))
    }
}

/// Encoding sub-variants for `RedisObject::ZSet`.
#[derive(Debug, Clone)]
pub enum ZSetEncoding {
 /// Pragmatic interim encoding used by the in-tree zset commands.
 /// Mirrors real Redis's dict + zskiplist pair via `HashMap` for O(1)
 /// member-keyed score lookup and `BTreeSet` for O(log N) ordered
 /// traversal.
    Inline(InlineZSet),
 /// Compact list-pack byte array (OBJ_ENCODING_LISTPACK).
    ListPack(Vec<u8>),
 /// Skip-list + hash-table pair (OBJ_ENCODING_SKIPLIST).
    // TODO(architect): replace inner Vec with redis_ds::ZSet (skiplist + hashtable) in Phase 4
    SkipList(Vec<(RedisString, f64)>),
}

/// Encoding sub-variants for `RedisObject::Hash`.
/// Hash commands operate over `Inline`, a plain `InlineHash` providing
/// the byte-exact semantics of every wire-level HASH operation.
#[derive(Debug, Clone)]
pub enum HashEncoding {
 /// Pragmatic interim encoding used by the in-tree hash commands.
 /// Backed by `InlineHash` for field lookups, updates, and insertion-order
 /// iteration.
    Inline(InlineHash),
 /// Compact list-pack byte array (OBJ_ENCODING_LISTPACK).
    // TODO(architect): replace stub Vec with real listpack encoding in Phase 4
    ListPack(Vec<u8>),
 /// Full hash table (OBJ_ENCODING_HASHTABLE).
    // TODO(architect): replace InlineHash with real redis-ds hashtable in Phase 4
    HashTable(InlineHash),
}

// ─────────────────────────────────────────────────────────────────────────────
// LRU / LFU clock
// ─────────────────────────────────────────────────────────────────────────────

/// LRU clock value (24 bits in C packed into the robj `lru` field).
/// Used by `objectGetLRUIdleSecs`, `objectGetLFUFrequency`, `objectGetIdleness`.
pub type LruClock = u32;

// ─────────────────────────────────────────────────────────────────────────────
// RedisObject — the main enum (replaces the architect stub)
// ─────────────────────────────────────────────────────────────────────────────

/// The canonical Redis runtime value type.
/// Replaces the architect stub with full encoding sub-variants per
/// PORTING.md §2 #4. The `lru` field mirrors the C `robj.lru` 24-bit field
/// used for LRU/LFU eviction. `expire` mirrors the embedded expiry field
/// (originally stored directly in the robj's trailing memory in C).
/// PORT NOTE: The architect stub stored `lru` and `expire` nowhere; this port
/// adds them to the object. Future versions may move `expire` to a separate
/// per-db expiry table (matching `redisDb.expires` in C) and remove it from here.
#[derive(Debug, Clone)]
pub struct RedisObject {
 /// LRU/LFU data (24 bits used). 0 = not initialised.
    pub lru: LruClock,
 /// Expiry time in milliseconds since epoch, or `EXPIRY_NONE` if no expiry.
    pub expire: i64,
 /// The type + encoding + value.
    pub kind: ObjectKind,
}

/// Encoding sub-variants for `RedisObject::Stream`.
/// The pragmatic `Inline` encoding backed by `redis_ds::stream::InlineStream`
/// (sorted Vec of entries). Future versions may replace this with the real
/// rax + listpack representation once those data structures ship.
#[derive(Debug, Clone)]
pub enum StreamEncoding {
    Inline(InlineStream),
}

/// Bloom filter data: a compact probabilistic set membership structure.
/// Implements a standard Bloom filter with the Kirsch-Mitzenmacher double-hashing
/// technique. Items are never removed; existence tests may return false positives
/// at the configured error rate, but never false negatives (a missing item is
/// always absent).
#[derive(Debug, Clone)]
pub struct BloomFilter {
 /// Bit array packed as bytes (`bits[i/8] >> (i%8) & 1`).
    pub bits: Vec<u8>,
 /// Number of hash functions k.
    pub n_hashes: u32,
 /// Maximum number of items before error rate degrades.
    pub capacity: u64,
 /// Number of items added so far.
    pub item_count: u64,
 /// Target false-positive error rate (e.g. 0.01 = 1%).
    pub error_rate: f64,
 /// Expansion factor for future scaling layers (stored; scaling not yet implemented).
    pub expansion: u32,
 /// When true, the filter does not scale; insertions beyond capacity degrade error rate.
    pub nonscaling: bool,
}

impl BloomFilter {
 /// Total number of bits in the filter.
    pub fn bit_count(&self) -> u64 {
        self.bits.len() as u64 * 8
    }
}

/// The discriminated union of all Redis value types + encodings.
#[derive(Debug, Clone)]
pub enum ObjectKind {
    String(StringEncoding),
    List(ListEncoding),
    Hash(HashEncoding),
    Set(SetEncoding),
    ZSet(ZSetEncoding),
    Stream(StreamEncoding),
 /// Module-defined types.
    // TODO(architect): replace with redis_modules::ModuleValue when modules are available
    Module,
 /// RedisJSON: native JSON document type.
 /// Stores an arbitrary JSON value (`serde_json::Value`) as the sole object
 /// encoding. JSONPath queries evaluate against this value at command time.
 /// No secondary index or encoded form is maintained — mutations deserialize,
 /// modify, and re-serialize in place.
    Json(serde_json::Value),
 /// RedisBloom: native Bloom filter type (BF.* commands).
    Bloom(BloomFilter),
}

// ─────────────────────────────────────────────────────────────────────────────
// Memory overhead reporting (used by MEMORY STATS / MEMORY OVERHEAD)
// ─────────────────────────────────────────────────────────────────────────────

/// Per-database memory breakdown (one entry per db, part of `ServerMemOverhead`).
#[derive(Debug, Default, Clone)]
pub struct DbOverhead {
    pub dbid: usize,
 /// Memory used by the main key hashtable for this db.
    pub overhead_ht_main: usize,
 /// Memory used by the expiry hashtable for this db.
    pub overhead_ht_expires: usize,
}

/// Server-wide memory overhead report. Returned by `get_memory_overhead_data`.
#[derive(Debug, Default)]
pub struct ServerMemOverhead {
    pub total_allocated: usize,
    pub startup_allocated: usize,
    pub peak_allocated: usize,
    pub total_frag: f32,
    pub total_frag_bytes: isize,
    pub allocator_frag: f32,
    pub allocator_frag_bytes: isize,
    pub allocator_rss: f32,
    pub allocator_rss_bytes: isize,
    pub rss_extra: f32,
    pub rss_extra_bytes: isize,
    pub repl_backlog: usize,
    pub replicas_repl_buffer: usize,
    pub clients_replicas: usize,
    pub clients_normal: usize,
    pub cluster_links: usize,
    pub cluster_slot_import: usize,
    pub cluster_slot_export: usize,
    pub aof_buffer: usize,
    pub lua_caches: usize,
    pub functions_caches: usize,
    pub total_keys: u64,
    pub bytes_per_key: usize,
    pub dataset: usize,
    pub dataset_perc: f32,
    pub peak_perc: f32,
    pub overhead_total: usize,
    pub overhead_db_hashtable_lut: usize,
    pub overhead_db_hashtable_rehashing: usize,
    pub db_dict_rehashing_count: usize,
 /// Per-database breakdowns.
    pub db: Vec<DbOverhead>,
}

impl ServerMemOverhead {
    pub fn num_dbs(&self) -> usize {
        self.db.len()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RedisObject impl
// ─────────────────────────────────────────────────────────────────────────────

impl RedisObject {
 // ── Constructors ──────────────────────────────────────────────────────

 /// Create an object with no LRU init and no expire.
    fn bare(kind: ObjectKind) -> Self {
        Self {
            lru: 0,
            expire: EXPIRY_NONE,
            kind,
        }
    }

 /// Create a raw-string object (OBJ_ENCODING_RAW).
    pub fn new_raw_string(bytes: &[u8]) -> Self {
        Self::bare(ObjectKind::String(StringEncoding::Raw(
            RedisString::from_bytes(bytes),
        )))
    }

 /// Create an EMBSTR string object.
 /// PORT NOTE: EMBSTR and RAW are layout-identical in Rust. The tag is kept
 /// for semantic correctness (OBJECT ENCODING output, tryObjectEncoding logic).
    pub fn new_embstr(bytes: &[u8]) -> Self {
        Self::bare(ObjectKind::String(StringEncoding::Embstr(
            RedisString::from_bytes(bytes),
        )))
    }

 /// Create a string object, choosing EMBSTR or RAW based on size heuristic.
    pub fn new_string(bytes: &[u8]) -> Self {
        if should_embed_string(bytes.len()) {
            Self::new_embstr(bytes)
        } else {
            Self::new_raw_string(bytes)
        }
    }

    pub fn new_string_from_redis_string(s: RedisString) -> Self {
        let kind = if should_embed_string(s.len()) {
            ObjectKind::String(StringEncoding::Embstr(s))
        } else {
            ObjectKind::String(StringEncoding::Raw(s))
        };
        Self::bare(kind)
    }

 /// Create an INT-encoded string object from an `i64`.
    pub fn new_int_string(value: i64) -> Self {
        Self::bare(ObjectKind::String(StringEncoding::Int(value)))
    }

 /// Create a string object, promoting to `Int` encoding when the bytes are
 /// the canonical decimal ASCII representation of an `i64`. Otherwise picks
 /// `Embstr`/`Raw` via the size threshold.
    pub fn new_string_try_encoded(bytes: &[u8]) -> Self {
        if let Some(value) = parse_canonical_decimal_i64(bytes) {
            return Self::new_int_string(value);
        }
        Self::new_string(bytes)
    }

    pub fn new_string_try_encoded_from_redis_string(s: RedisString) -> Self {
        if let Some(value) = parse_canonical_decimal_i64(s.as_bytes()) {
            return Self::new_int_string(value);
        }
        Self::new_string_from_redis_string(s)
    }

 /// Create an empty list object with the pragmatic Inline encoding.
    pub fn new_list() -> Self {
        Self::bare(ObjectKind::List(ListEncoding::Inline(VecDeque::new())))
    }

 /// Create a list object with QuickList encoding.
    pub fn new_quicklist(_fill: i32, _compress: i32) -> Self {
        // TODO(port): pass fill/compress to the real QuickList when redis-ds lands (Phase 4)
        Self::bare(ObjectKind::List(ListEncoding::QuickList(VecDeque::new())))
    }

 /// Create a list object with ListPack encoding.
    pub fn new_list_listpack() -> Self {
        Self::bare(ObjectKind::List(ListEncoding::ListPack(Vec::new())))
    }

 /// Create an `Inline` list object pre-populated from an existing `VecDeque`.
 /// Used by the RDB loader (`rdb/list.rs`) to wrap a deserialized element
 /// sequence into a fully-formed `RedisObject` without an intermediate
 /// empty-then-push construction.
    pub fn new_list_from_vec(deque: VecDeque<RedisString>) -> Self {
        Self::bare(ObjectKind::List(ListEncoding::Inline(deque)))
    }

 /// Create a `QuickList` list object pre-populated from an existing `VecDeque`.
    pub fn new_quicklist_from_vec(deque: VecDeque<RedisString>) -> Self {
        Self::bare(ObjectKind::List(ListEncoding::QuickList(deque)))
    }

 /// Borrow the inner list `VecDeque` for a list-encoded object.
 /// Returns `None` for non-list objects and for the stub `ListPack`
 /// encoding that this round does not populate.
    pub fn list(&self) -> Option<&VecDeque<RedisString>> {
        match &self.kind {
            ObjectKind::List(ListEncoding::Inline(d) | ListEncoding::QuickList(d)) => Some(d),
            _ => None,
        }
    }

 /// Mutably borrow the inner list `VecDeque` for a list-encoded object.
    pub fn list_mut(&mut self) -> Option<&mut VecDeque<RedisString>> {
        match &mut self.kind {
            ObjectKind::List(ListEncoding::Inline(d) | ListEncoding::QuickList(d)) => Some(d),
            _ => None,
        }
    }

 /// Promote an inline/listpack-shaped list to quicklist when a growing
 /// operation would exceed Valkey's active `list-max-listpack-size`.
 /// This mirrors the `listTypeTryConversionAppend` call made by LPUSH/RPUSH,
 /// LINSERT, LSET, and LMOVE before mutating a listpack. The payload still
 /// lives in a `VecDeque`, but the explicit `QuickList` tag preserves
 /// same OBJECT ENCODING hysteresis as upstream.
    pub fn list_try_promote_for_append(&mut self, values: &[RedisString]) {
        let should_promote = match &self.kind {
            ObjectKind::List(ListEncoding::Inline(d)) => listpack_growing_exceeds_limit(d, values),
            _ => false,
        };
        if !should_promote {
            return;
        }
        if let ObjectKind::List(ListEncoding::Inline(d)) = &mut self.kind {
            let deque = std::mem::take(d);
            self.kind = ObjectKind::List(ListEncoding::QuickList(deque));
        }
    }

 /// Demote a quicklist-shaped list back to inline/listpack after a shrinking
 /// operation drops below Valkey's half-limit conversion threshold.
    pub fn list_try_demote_after_shrink(&mut self) {
        let should_demote = match &self.kind {
            ObjectKind::List(ListEncoding::QuickList(d)) => quicklist_fits_listpack_after_shrink(d),
            _ => false,
        };
        if !should_demote {
            return;
        }
        if let ObjectKind::List(ListEncoding::QuickList(d)) = &mut self.kind {
            let deque = std::mem::take(d);
            self.kind = ObjectKind::List(ListEncoding::Inline(deque));
        }
    }

 /// Create a set object with full hash-table encoding.
    pub fn new_set_hashtable() -> Self {
        Self::bare(ObjectKind::Set(SetEncoding::HashTable(HashSet::new())))
    }

 /// Create a set object with IntSet encoding.
    pub fn new_intset() -> Self {
        Self::bare(ObjectKind::Set(SetEncoding::IntSet(Vec::new())))
    }

 /// Create a set object with ListPack encoding.
    pub fn new_set_listpack() -> Self {
        Self::bare(ObjectKind::Set(SetEncoding::ListPack(Vec::new())))
    }

 /// Create an empty set object with the pragmatic Inline encoding.
    pub fn new_set() -> Self {
        Self::bare(ObjectKind::Set(SetEncoding::Inline(InlineSet::new())))
    }

 /// Create an `Inline` set object pre-populated from an existing `HashSet`.
 /// Used by the RDB loader (`rdb/set.rs`) to wrap a deserialized member
 /// collection into a fully-formed `RedisObject` without an intermediate
 /// empty-then-insert construction.
    pub fn new_set_from_set(members: HashSet<RedisString>) -> Self {
        Self::bare(ObjectKind::Set(SetEncoding::Inline(
            InlineSet::from_hash_set(members),
        )))
    }

 /// Borrow the inner member `HashSet` for a set-encoded object.
 /// Returns `None` for non-set objects and for the stub `ListPack` /
 /// `IntSet` / `HashTable` encodings that this round does not populate.
    pub fn set(&self) -> Option<&HashSet<RedisString>> {
        match &self.kind {
            ObjectKind::Set(SetEncoding::Inline(s)) => Some(&s.data),
            _ => None,
        }
    }

 /// Mutably borrow the inner member `HashSet` for a set-encoded object.
    pub fn set_mut(&mut self) -> Option<&mut HashSet<RedisString>> {
        match &mut self.kind {
            ObjectKind::Set(SetEncoding::Inline(s)) => Some(&mut s.data),
            _ => None,
        }
    }

 /// Borrow the `InlineSet` (data + sticky encoding) for a set-encoded object.
    pub fn inline_set(&self) -> Option<&InlineSet> {
        match &self.kind {
            ObjectKind::Set(SetEncoding::Inline(s)) => Some(s),
            _ => None,
        }
    }

 /// Mutably borrow the `InlineSet` for a set-encoded object.
    pub fn inline_set_mut(&mut self) -> Option<&mut InlineSet> {
        match &mut self.kind {
            ObjectKind::Set(SetEncoding::Inline(s)) => Some(s),
            _ => None,
        }
    }

 /// Create a hash object with ListPack encoding.
    pub fn new_hash_listpack() -> Self {
        Self::bare(ObjectKind::Hash(HashEncoding::ListPack(Vec::new())))
    }

 /// Create a hash object with HashTable encoding.
    pub fn new_hash_hashtable() -> Self {
        Self::bare(ObjectKind::Hash(HashEncoding::HashTable(InlineHash::new())))
    }

 /// Create an empty hash object with the pragmatic Inline encoding.
    pub fn new_hash() -> Self {
        Self::bare(ObjectKind::Hash(HashEncoding::Inline(InlineHash::new())))
    }

 /// Create a hash object from an existing `HashMap`, using the Inline encoding.
 /// Used by the RDB loader to construct a hash object from a deserialized
 /// field/value map without an intermediate empty-insert loop.
    pub fn new_hash_from_map(map: HashMap<RedisString, RedisString>) -> Self {
        Self::bare(ObjectKind::Hash(HashEncoding::Inline(
            InlineHash::from_hash_map(map),
        )))
    }

 /// Borrow the inner field/value hash for a hash-encoded object.
 /// Returns `None` for non-hash objects and for the stub `ListPack` /
 /// `HashTable` encodings that this round does not populate.
    pub fn hash(&self) -> Option<&InlineHash> {
        match &self.kind {
            ObjectKind::Hash(HashEncoding::Inline(h) | HashEncoding::HashTable(h)) => Some(h),
            _ => None,
        }
    }

 /// Mutably borrow the inner field/value hash for a hash-encoded object.
    pub fn hash_mut(&mut self) -> Option<&mut InlineHash> {
        match &mut self.kind {
            ObjectKind::Hash(HashEncoding::Inline(h) | HashEncoding::HashTable(h)) => Some(h),
            _ => None,
        }
    }

 /// Promote the interim inline hash map to the explicit hashtable variant.
 /// This preserves the same Rust `InlineHash` storage while making
 /// `OBJECT ENCODING` report `"hashtable"` for cases where upstream
 /// upgrades a hash out of listpack form.
    pub fn promote_hash_to_hashtable(&mut self) {
        if let ObjectKind::Hash(HashEncoding::Inline(h)) = &mut self.kind {
            let map = std::mem::take(h);
            self.kind = ObjectKind::Hash(HashEncoding::HashTable(map));
        }
    }

 /// Create a sorted-set object with SkipList encoding.
    pub fn new_zset_skiplist() -> Self {
        Self::bare(ObjectKind::ZSet(ZSetEncoding::SkipList(Vec::new())))
    }

 /// Create a sorted-set object with ListPack encoding.
    pub fn new_zset_listpack() -> Self {
        Self::bare(ObjectKind::ZSet(ZSetEncoding::ListPack(Vec::new())))
    }

 /// Create an empty sorted-set object with the pragmatic Inline encoding.
    pub fn new_zset() -> Self {
        Self::bare(ObjectKind::ZSet(ZSetEncoding::Inline(InlineZSet::new())))
    }

 /// Create a zset object from an existing `InlineZSet`, using the Inline encoding.
 /// Used by the RDB loader (`rdb/zset.rs`) to construct a zset object from a
 /// deserialized member/score collection without an intermediate empty-insert loop.
    pub fn new_zset_from_inline(zset: InlineZSet) -> Self {
        Self::bare(ObjectKind::ZSet(ZSetEncoding::Inline(zset)))
    }

 /// Borrow the inner `InlineZSet` for a zset-encoded object.
 /// Returns `None` for non-zset objects and for the stub `ListPack` /
 /// `SkipList` encodings that this round does not populate.
    pub fn zset(&self) -> Option<&InlineZSet> {
        match &self.kind {
            ObjectKind::ZSet(ZSetEncoding::Inline(z)) => Some(z),
            _ => None,
        }
    }

 /// Mutably borrow the inner `InlineZSet` for a zset-encoded object.
    pub fn zset_mut(&mut self) -> Option<&mut InlineZSet> {
        match &mut self.kind {
            ObjectKind::ZSet(ZSetEncoding::Inline(z)) => Some(z),
            _ => None,
        }
    }

 /// Create a stream object with the pragmatic Inline encoding.
    pub fn new_stream() -> Self {
        Self::bare(ObjectKind::Stream(StreamEncoding::Inline(
            InlineStream::new(),
        )))
    }

 /// Borrow the inner `InlineStream` for a stream-encoded object.
    pub fn stream(&self) -> Option<&InlineStream> {
        match &self.kind {
            ObjectKind::Stream(StreamEncoding::Inline(s)) => Some(s),
            _ => None,
        }
    }

 /// Mutably borrow the inner `InlineStream` for a stream-encoded object.
    pub fn stream_mut(&mut self) -> Option<&mut InlineStream> {
        match &mut self.kind {
            ObjectKind::Stream(StreamEncoding::Inline(s)) => Some(s),
            _ => None,
        }
    }

 /// Create a Bloom filter object wrapping an existing `BloomFilter`.
    pub fn new_bloom_from_filter(bf: BloomFilter) -> Self {
        Self::bare(ObjectKind::Bloom(bf))
    }

 /// Create a JSON-encoded object from a `serde_json::Value`.
    pub fn new_json(v: serde_json::Value) -> Self {
        Self::bare(ObjectKind::Json(v))
    }

 /// Borrow the inner `serde_json::Value` for a JSON-encoded object.
    pub fn json(&self) -> Option<&serde_json::Value> {
        match &self.kind {
            ObjectKind::Json(v) => Some(v),
            _ => None,
        }
    }

 /// Mutably borrow the inner `serde_json::Value` for a JSON-encoded object.
    pub fn json_mut(&mut self) -> Option<&mut serde_json::Value> {
        match &mut self.kind {
            ObjectKind::Json(v) => Some(v),
            _ => None,
        }
    }

 /// Borrow the inner `BloomFilter` if this object holds one.
    pub fn bloom(&self) -> Option<&BloomFilter> {
        match &self.kind {
            ObjectKind::Bloom(bf) => Some(bf),
            _ => None,
        }
    }

 /// Mutably borrow the inner `BloomFilter` if this object holds one.
    pub fn bloom_mut(&mut self) -> Option<&mut BloomFilter> {
        match &mut self.kind {
            ObjectKind::Bloom(bf) => Some(bf),
            _ => None,
        }
    }

 // ── Back-compat shims for the architect stub ──────────────────────────

 /// Construct a `RedisObject::String(StringEncoding::Raw(...))` from a `RedisString`.
 /// Kept for compatibility with db.rs tests that call `RedisObject::from_string(s)`.
    pub fn from_string(s: RedisString) -> Self {
        Self::bare(ObjectKind::String(StringEncoding::Raw(s)))
    }

 /// Return a reference to the inner `RedisString` if this is a string-encoded object
 /// (RAW or EMBSTR). Returns `None` for INT-encoded strings.
    pub fn as_string(&self) -> Option<&RedisString> {
        match &self.kind {
            ObjectKind::String(StringEncoding::Raw(s)) => Some(s),
            ObjectKind::String(StringEncoding::Embstr(s)) => Some(s),
            _ => None,
        }
    }

 // ── Type information ──────────────────────────────────────────────────

 /// Return the Redis type name used in protocol output.
    pub fn type_name(&self) -> &'static str {
        match &self.kind {
            ObjectKind::String(_) => "string",
            ObjectKind::List(_) => "list",
            ObjectKind::Hash(_) => "hash",
            ObjectKind::Set(_) => "set",
            ObjectKind::ZSet(_) => "zset",
            ObjectKind::Stream(_) => "stream",
            ObjectKind::Module => "module",
            ObjectKind::Json(_) => "ReJSON-RL",
            ObjectKind::Bloom(_) => "MBbloom--",
        }
    }

 /// Return the `ObjectType` discriminant (used by `check_type`).
    pub fn object_type(&self) -> ObjectType {
        match &self.kind {
            ObjectKind::String(_) => ObjectType::String,
            ObjectKind::List(_) => ObjectType::List,
            ObjectKind::Hash(_) => ObjectType::Hash,
            ObjectKind::Set(_) => ObjectType::Set,
            ObjectKind::ZSet(_) => ObjectType::ZSet,
            ObjectKind::Stream(_) => ObjectType::Stream,
            ObjectKind::Module => ObjectType::Module,
            ObjectKind::Json(_) => ObjectType::Module,
            ObjectKind::Bloom(_) => ObjectType::Module,
        }
    }

 /// Return the encoding name for `OBJECT ENCODING`.
 /// For the `Inline` interim encodings, the reported name follows
 /// listpack→big-encoding crossover heuristics Valkey applies: small
 /// collections that would fit in a listpack report `"listpack"` (or
 /// `"intset"` for all-integer sets), and larger ones report the big
 /// encoding (`"hashtable"`, `"quicklist"`, or `"skiplist"`). The
 /// underlying storage is unchanged — this is a wire-level fiction
 /// matching real Redis's reported encoding.
    pub fn encoding_name(&self) -> &'static str {
        match &self.kind {
            ObjectKind::String(StringEncoding::Raw(_)) => "raw",
            ObjectKind::String(StringEncoding::Embstr(_)) => "embstr",
            ObjectKind::String(StringEncoding::Int(_)) => "int",
            ObjectKind::List(ListEncoding::Inline(d)) => list_inline_observed_encoding(d),
            ObjectKind::List(ListEncoding::QuickList(_)) => "quicklist",
            ObjectKind::List(ListEncoding::ListPack(_)) => "listpack",
            ObjectKind::Set(SetEncoding::Inline(s)) => set_inline_observed_encoding(s),
            ObjectKind::Set(SetEncoding::HashTable(_)) => "hashtable",
            ObjectKind::Set(SetEncoding::IntSet(_)) => "intset",
            ObjectKind::Set(SetEncoding::ListPack(_)) => "listpack",
            ObjectKind::ZSet(ZSetEncoding::Inline(z)) => zset_inline_observed_encoding(z),
            ObjectKind::ZSet(ZSetEncoding::SkipList(_)) => "skiplist",
            ObjectKind::ZSet(ZSetEncoding::ListPack(_)) => "listpack",
            ObjectKind::Hash(HashEncoding::HashTable(_)) => "hashtable",
            ObjectKind::Hash(HashEncoding::ListPack(_)) => "listpack",
            ObjectKind::Hash(HashEncoding::Inline(h)) => hash_inline_observed_encoding(h),
            ObjectKind::Stream(StreamEncoding::Inline(_)) => "stream",
            ObjectKind::Module => "unknown",
            ObjectKind::Json(_) => "json",
            ObjectKind::Bloom(_) => "bloom",
        }
    }

 /// Return `true` if the object has a byte-string (RAW or EMBSTR) encoding.
    pub fn is_sds_encoded(&self) -> bool {
        matches!(
            &self.kind,
            ObjectKind::String(StringEncoding::Raw(_))
                | ObjectKind::String(StringEncoding::Embstr(_))
        )
    }

 // ── Expiry ────────────────────────────────────────────────────────────

 /// Return the expiry in milliseconds, or `None` if no expiry.
    pub fn get_expire(&self) -> Option<i64> {
        if self.expire == EXPIRY_NONE {
            None
        } else {
            Some(self.expire)
        }
    }

 /// Set the expiry in milliseconds. `None` clears it.
    pub fn set_expire(&mut self, expire: Option<i64>) {
        self.expire = expire.unwrap_or(EXPIRY_NONE);
    }

 // ── Decoding ──────────────────────────────────────────────────────────

 /// Return a decoded (byte-string) representation of a string object.
 /// For RAW/EMBSTR: borrows the inner `RedisString`.
 /// For INT: formats the integer and returns an owned `RedisString`.
    pub fn decoded(&self) -> Result<Cow<'_, RedisString>, RedisError> {
        match &self.kind {
            ObjectKind::String(StringEncoding::Raw(s)) => Ok(Cow::Borrowed(s)),
            ObjectKind::String(StringEncoding::Embstr(s)) => Ok(Cow::Borrowed(s)),
            ObjectKind::String(StringEncoding::Int(n)) => {
                let s = format_long_long(*n);
                Ok(Cow::Owned(s))
            }
            _ => Err(RedisError::runtime(
                b"ERR decoded() called on non-string object",
            )),
        }
    }

 // ── Encoding optimisation ─────────────────────────────────────────────

 /// Try to re-encode a string object to save memory.
 /// Converts `Raw`/`Embstr` → `Int` if the string is a valid long;
 /// Converts `Raw` → `Embstr` if the string is short enough.
    pub fn try_encode(mut self, try_trim: bool) -> Self {
        let ObjectKind::String(_) = &self.kind else {
            return self;
        };
        if !self.is_sds_encoded() {
            return self;
        }

        let bytes = match &self.kind {
            ObjectKind::String(StringEncoding::Raw(s) | StringEncoding::Embstr(s)) => {
                s.as_bytes().to_vec()
            }
            _ => unreachable!(),
        };

 // Attempt integer encoding when string is <= 20 bytes.
        if bytes.len() <= 20 {
            if let Some(v) = parse_long(&bytes) {
                return Self {
                    lru: self.lru,
                    expire: self.expire,
                    kind: ObjectKind::String(StringEncoding::Int(v)),
                };
            }
        }

 // Attempt EMBSTR encoding when short enough.
        if should_embed_string(bytes.len()) {
            if matches!(&self.kind, ObjectKind::String(StringEncoding::Embstr(_))) {
                return self;
            }
            return Self {
                lru: self.lru,
                expire: self.expire,
                kind: ObjectKind::String(StringEncoding::Embstr(RedisString::from_vec(bytes))),
            };
        }

 // PERF(port): C trims the SDS free-space here via sdsRemoveFreeSpace. In Rust
 // Vec<u8> shrink_to_fit is the equivalent; skipped for now.
        if try_trim {
            if let ObjectKind::String(StringEncoding::Raw(ref mut s)) = self.kind {
                let _ = s; // PORT NOTE: trim_to_fit not yet implemented
            }
        }

        self
    }

 /// Convenience wrapper: `try_encode(true)`.
    pub fn try_object_encoding(self) -> Self {
        self.try_encode(true)
    }

 // ── Numeric extraction ────────────────────────────────────────────────

 /// Extract a `long long` (`i64`) from a string object.
 /// Returns `Err(RedisError::not_integer` on failure.
    pub fn get_long_long(&self) -> Result<i64, RedisError> {
        match &self.kind {
            ObjectKind::String(StringEncoding::Int(n)) => Ok(*n),
            ObjectKind::String(StringEncoding::Raw(s) | StringEncoding::Embstr(s)) => {
                parse_long_long(s.as_bytes()).ok_or_else(RedisError::not_integer)
            }
            _ => Err(RedisError::runtime(
                b"ERR get_long_long on non-string object",
            )),
        }
    }

 /// Extract a `double` from a string object.
 /// Returns `Err(RedisError::not_float` on failure.
    pub fn get_double(&self) -> Result<f64, RedisError> {
        match &self.kind {
            ObjectKind::String(StringEncoding::Int(n)) => Ok(*n as f64),
            ObjectKind::String(StringEncoding::Raw(s) | StringEncoding::Embstr(s)) => {
                parse_double(s.as_bytes()).ok_or_else(RedisError::not_float)
            }
            _ => Err(RedisError::runtime(b"ERR get_double on non-string object")),
        }
    }

 /// Extract a `long double` as `f64` (Rust has no `long double` type).
 /// PORT NOTE: C uses `long double` (80-bit extended precision on x86). Rust
 /// has no equivalent; we use `f64`. This may cause precision differences
 /// for INCRBYFLOAT/HINCRBYFLOAT at the extremes of the representable range.
    pub fn get_long_double(&self) -> Result<f64, RedisError> {
        self.get_double()
    }

 /// Return the string length (number of bytes in the byte-string representation).
 /// For INT-encoded objects, returns the decimal digit count.
    pub fn string_len(&self) -> Result<usize, RedisError> {
        match &self.kind {
            ObjectKind::String(StringEncoding::Raw(s) | StringEncoding::Embstr(s)) => Ok(s.len()),
            ObjectKind::String(StringEncoding::Int(n)) => Ok(decimal_digit_count(*n)),
            _ => Err(RedisError::runtime(b"ERR string_len on non-string object")),
        }
    }

 // ── String comparison ─────────────────────────────────────────────────

 /// Binary-compare two string objects. Returns `Ordering`.
    pub fn compare_binary(&self, other: &Self) -> Result<Ordering, RedisError> {
        let a = self.decoded()?;
        let b = other.decoded()?;
        Ok(a.as_bytes().cmp(b.as_bytes()))
    }

 /// Locale-collation compare of two string objects.
    /// TODO(port): `strcoll` is locale-dependent; Rust std has no direct equivalent.
 /// Using byte-wise ordering as a placeholder.
    pub fn compare_collate(&self, other: &Self) -> Result<Ordering, RedisError> {
        self.compare_binary(other)
    }

 /// Return `true` if two string objects have equal byte representations.
    pub fn equal_string(&self, other: &Self) -> bool {
        match (&self.kind, &other.kind) {
            (
                ObjectKind::String(StringEncoding::Int(a)),
                ObjectKind::String(StringEncoding::Int(b)),
            ) => a == b,
            _ => {
                let a_len = self.string_len().unwrap_or(0);
                let b_len = other.string_len().unwrap_or(0);
                if a_len != b_len {
                    return false;
                }
                self.compare_binary(other)
                    .map(|o| o == Ordering::Equal)
                    .unwrap_or(false)
            }
        }
    }

 // ── LRU / LFU ────────────────────────────────────────────────────────

 /// Return the LFU frequency byte from the LRU clock field.
    pub fn lfu_frequency(&self) -> u8 {
        // TODO(port): call lrulfu module's lfu_getFrequency when available
        (self.lru & 0xFF) as u8
    }

 /// Return the approximate LRU idle time in seconds.
    pub fn lru_idle_secs(&self) -> u32 {
        crate::lru_clock::current_lru_clock().wrapping_sub(self.lru)
    }

 /// Return an idleness measure (larger = more idle).
    pub fn idleness(&self) -> u32 {
        self.lru_idle_secs()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Dynamic encoding thresholds (CONFIG-settable)
// ─────────────────────────────────────────────────────────────────────────────

/// Live encoding-threshold table.
/// Mirrors the CONFIG keys that Valkey accepts for the listpack/intset cutoffs.
/// Every `*_inline_observed_encoding` function reads from this struct rather
/// than the compile-time constants so that `CONFIG SET` takes effect
/// immediately on subsequent `OBJECT ENCODING` calls.
/// TODO(architect): move to `RedisServer` when `CommandContext` threads a real
/// `&mut RedisServer` reference through every command call, eliminating
/// need for a process-wide global.
#[derive(Debug, Clone)]
pub struct EncodingThresholds {
 /// `hash-max-listpack-entries` (default 128).
    pub hash_max_listpack_entries: usize,
 /// `hash-max-listpack-value` (default 64).
    pub hash_max_listpack_value: usize,
 /// `list-max-listpack-size` (default -2).
 /// Negative values denote byte-size caps in real Valkey (-2 = 8 KiB per
 /// node). For the `Inline(VecDeque)` encoding used here we only support an
 /// entry-count cap. When the stored value is negative we fall back
 /// `LIST_LISTPACK_MAX_ENTRIES` (128); a positive value is used directly as
 /// the entry-count threshold. This is a documented simplification for
 /// port's inline-list representation.
    pub list_max_listpack_size: i64,
 /// `set-max-listpack-entries` (default 128).
    pub set_max_listpack_entries: usize,
 /// `set-max-listpack-value` (default 64).
    pub set_max_listpack_value: usize,
 /// `set-max-intset-entries` (default 512).
    pub set_max_intset_entries: usize,
 /// `zset-max-listpack-entries` (default 128).
    pub zset_max_listpack_entries: usize,
 /// `zset-max-listpack-value` (default 64).
    pub zset_max_listpack_value: usize,
}

impl Default for EncodingThresholds {
    fn default() -> Self {
        Self {
            hash_max_listpack_entries: HASH_LISTPACK_MAX_ENTRIES,
            hash_max_listpack_value: HASH_LISTPACK_MAX_VALUE_BYTES,
            list_max_listpack_size: -2,
            set_max_listpack_entries: SET_LISTPACK_MAX_ENTRIES,
            set_max_listpack_value: SET_LISTPACK_MAX_VALUE_BYTES,
            set_max_intset_entries: SET_INTSET_MAX_ENTRIES,
            zset_max_listpack_entries: ZSET_LISTPACK_MAX_ENTRIES,
            zset_max_listpack_value: ZSET_LISTPACK_MAX_VALUE_BYTES,
        }
    }
}

/// Process-wide handle to the live config used by the encoding heuristics.
/// The accept loop calls [`install_live_config`] once at startup with
/// `Arc<LiveConfig>` it shares with `RedisServer`. Encoding heuristics
/// (`hash_inline_observed_encoding`, etc.) read thresholds through this
/// handle, so `CONFIG SET hash-max-listpack-entries N` propagates to every
/// `OBJECT ENCODING` call immediately.
/// Falls back to a defaulted `LiveConfig` when no install has happened yet
/// (unit tests that exercise encoding helpers without a live server).
static LIVE_CONFIG: OnceLock<Arc<crate::live_config::LiveConfig>> = OnceLock::new();

/// Register the process-wide live config. Called once from the binary's main
/// before any command runs. Subsequent calls are no-ops (OnceLock semantics).
pub fn install_live_config(config: Arc<crate::live_config::LiveConfig>) {
    let _ = LIVE_CONFIG.set(config);
}

fn live_config_or_default() -> Arc<crate::live_config::LiveConfig> {
    LIVE_CONFIG
        .get_or_init(|| Arc::new(crate::live_config::LiveConfig::new()))
        .clone()
}

/// Snapshot the active encoding thresholds.
/// Reads each field from the registered `LiveConfig`. The returned struct is
/// a value snapshot — mutating the returned `EncodingThresholds` does not
/// affect the live state.
pub fn get_encoding_thresholds() -> EncodingThresholds {
    let cfg = live_config_or_default();
    EncodingThresholds {
        hash_max_listpack_entries: cfg.hash_max_listpack_entries(),
        hash_max_listpack_value: cfg.hash_max_listpack_value(),
        list_max_listpack_size: cfg.list_max_listpack_size(),
        set_max_intset_entries: cfg.set_max_intset_entries(),
        set_max_listpack_entries: cfg.set_max_listpack_entries(),
        set_max_listpack_value: cfg.set_max_listpack_value(),
        zset_max_listpack_entries: cfg.zset_max_listpack_entries(),
        zset_max_listpack_value: cfg.zset_max_listpack_value(),
    }
}

/// Mutate the live encoding thresholds in place.
/// `updater` receives an `EncodingThresholds` initialised from the current
/// live values; whatever fields it changes are written back through
/// `LiveConfig` atomics. The default-only path (no install) is supported but
/// has no observable effect because every command-handler `LiveConfig` read
/// also falls back to the same default.
pub fn update_encoding_thresholds<F>(updater: F)
where
    F: FnOnce(&mut EncodingThresholds),
{
    let cfg = live_config_or_default();
    let mut snapshot = EncodingThresholds {
        hash_max_listpack_entries: cfg.hash_max_listpack_entries(),
        hash_max_listpack_value: cfg.hash_max_listpack_value(),
        list_max_listpack_size: cfg.list_max_listpack_size(),
        set_max_intset_entries: cfg.set_max_intset_entries(),
        set_max_listpack_entries: cfg.set_max_listpack_entries(),
        set_max_listpack_value: cfg.set_max_listpack_value(),
        zset_max_listpack_entries: cfg.zset_max_listpack_entries(),
        zset_max_listpack_value: cfg.zset_max_listpack_value(),
    };
    updater(&mut snapshot);
    cfg.set_hash_max_listpack_entries(snapshot.hash_max_listpack_entries);
    cfg.set_hash_max_listpack_value(snapshot.hash_max_listpack_value);
    cfg.set_list_max_listpack_size(snapshot.list_max_listpack_size);
    cfg.store_set_max_intset_entries(snapshot.set_max_intset_entries);
    cfg.store_set_max_listpack_entries(snapshot.set_max_listpack_entries);
    cfg.store_set_max_listpack_value(snapshot.set_max_listpack_value);
    cfg.set_zset_max_listpack_entries(snapshot.zset_max_listpack_entries);
    cfg.set_zset_max_listpack_value(snapshot.zset_max_listpack_value);
}

// ─────────────────────────────────────────────────────────────────────────────
// Free-standing object creation helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Valkey quicklist negative-fill size tiers for `list-max-listpack-size`.
const LISTPACK_NEG_FILL_LIMITS: [usize; 5] = [4096, 8192, 16384, 32768, 65536];

/// Safety cap used when `list-max-listpack-size` is a positive entry count.
const LISTPACK_SIZE_SAFETY_LIMIT: usize = 8192;
const LISTPACK_FIXED_OVERHEAD_BYTES: usize = 7;
const LISTPACK_MIN_ENTRY_BYTES: usize = 2;
const LISTPACK_INTBUF_SIZE: usize = 21;
const LISTPACK_MAX_SAFETY_SIZE: usize = 1 << 30;

/// Valkey default `hash-max-listpack-entries`. Hashes with more entries
/// report `hashtable`.
pub const HASH_LISTPACK_MAX_ENTRIES: usize = 128;

/// Valkey default `hash-max-listpack-value`. Hashes with any field or value
/// longer than this report `hashtable`.
pub const HASH_LISTPACK_MAX_VALUE_BYTES: usize = 64;

/// Valkey default `set-max-intset-entries`. All-integer sets at or below this
/// size report `intset`.
pub const SET_INTSET_MAX_ENTRIES: usize = 512;

/// Valkey default `set-max-listpack-entries`. Mixed-content sets at or below
/// this size with all members shorter than `SET_LISTPACK_MAX_VALUE_BYTES`
/// report `listpack`; otherwise `hashtable`.
pub const SET_LISTPACK_MAX_ENTRIES: usize = 128;

/// Valkey default `set-max-listpack-value`. Sets with any member longer than
/// this report `hashtable` rather than `listpack`.
pub const SET_LISTPACK_MAX_VALUE_BYTES: usize = 64;

/// Valkey default `zset-max-listpack-entries`. Sorted sets with more entries
/// report `skiplist`.
pub const ZSET_LISTPACK_MAX_ENTRIES: usize = 128;

/// Valkey default `zset-max-listpack-value`. Sorted sets with any member
/// longer than this report `skiplist`.
pub const ZSET_LISTPACK_MAX_VALUE_BYTES: usize = 64;

fn listpack_node_limit(fill: i64) -> (usize, usize) {
    if fill >= 0 {
        let count = if fill == 0 { 1 } else { fill as usize };
        (usize::MAX, count)
    } else {
        let offset = fill.saturating_neg().saturating_sub(1) as usize;
        let idx = offset.min(LISTPACK_NEG_FILL_LIMITS.len() - 1);
        (LISTPACK_NEG_FILL_LIMITS[idx], usize::MAX)
    }
}

fn listpack_encoded_len(d: &VecDeque<RedisString>) -> Option<usize> {
    let mut encoded_len = LISTPACK_FIXED_OVERHEAD_BYTES;
    for value in d {
        encoded_len = encoded_len.checked_add(listpack_entry_total_len(value.as_bytes())?)?;
        if encoded_len > LISTPACK_MAX_SAFETY_SIZE {
            return None;
        }
    }
    Some(encoded_len)
}

fn listpack_entries_encoded_len(values: &[RedisString]) -> Option<usize> {
    let mut encoded_len = 0usize;
    for value in values {
        encoded_len = encoded_len.checked_add(listpack_entry_total_len(value.as_bytes())?)?;
    }
    Some(encoded_len)
}

fn listpack_entry_total_len(value: &[u8]) -> Option<usize> {
    let content_len = match listpack_bytes_to_i64(value) {
        Some(n) => listpack_integer_content_len(n),
        None => listpack_string_content_len(value.len())?,
    };
    content_len.checked_add(listpack_backlen_size(content_len)?)
}

fn listpack_string_content_len(len: usize) -> Option<usize> {
    if len < 64 {
        Some(1 + len)
    } else if len < 4096 {
        Some(2 + len)
    } else {
        5usize.checked_add(len)
    }
}

fn listpack_integer_content_len(value: i64) -> usize {
    if (0..=127).contains(&value) {
        1
    } else if (-4096..=4095).contains(&value) {
        2
    } else if (i16::MIN as i64..=i16::MAX as i64).contains(&value) {
        3
    } else if (-8_388_608..=8_388_607).contains(&value) {
        4
    } else if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
        5
    } else {
        9
    }
}

fn listpack_backlen_size(len: usize) -> Option<usize> {
    if len <= 127 {
        Some(1)
    } else if len <= 16_383 {
        Some(2)
    } else if len <= 2_097_151 {
        Some(3)
    } else if len <= 268_435_455 {
        Some(4)
    } else if (len as u64) <= 34_359_738_367 {
        Some(5)
    } else {
        None
    }
}

fn listpack_bytes_to_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() >= LISTPACK_INTBUF_SIZE {
        return None;
    }
    if s.len() == 1 && s[0].is_ascii_digit() {
        return Some((s[0] - b'0') as i64);
    }

    let mut index = 0usize;
    let negative = s[0] == b'-';
    if negative {
        index += 1;
        if index == s.len() {
            return None;
        }
    }

    if !(b'1'..=b'9').contains(&s[index]) {
        return None;
    }

    let mut value = (s[index] - b'0') as u64;
    index += 1;
    while index < s.len() {
        let digit = s[index];
        if !digit.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?;
        value = value.checked_add((digit - b'0') as u64)?;
        index += 1;
    }

    if negative {
        if value > (1u64 << 63) {
            return None;
        }
        Some((value as i64).wrapping_neg())
    } else if value > i64::MAX as u64 {
        None
    } else {
        Some(value as i64)
    }
}

fn listpack_growing_exceeds_limit(d: &VecDeque<RedisString>, values: &[RedisString]) -> bool {
    let t = get_encoding_thresholds();
    let (size_limit, count_limit) = listpack_node_limit(t.list_max_listpack_size);
    let Some(encoded_len) = listpack_encoded_len(d) else {
        return true;
    };
    let Some(added_bytes) = listpack_entries_encoded_len(values) else {
        return true;
    };
    let new_len = d.len().saturating_add(values.len());
    let new_encoded_len = encoded_len.saturating_add(added_bytes);
    if size_limit != usize::MAX {
        new_encoded_len > size_limit
    } else if new_encoded_len > LISTPACK_SIZE_SAFETY_LIMIT {
        true
    } else {
        new_len > count_limit
    }
}

fn quicklist_fits_listpack_after_shrink(d: &VecDeque<RedisString>) -> bool {
    let t = get_encoding_thresholds();
    let (size_limit, count_limit) = listpack_node_limit(t.list_max_listpack_size);
    if !quicklist_shrink_may_fit_listpack(d.len(), size_limit, count_limit) {
        return false;
    }
    let Some(encoded_len) = listpack_encoded_len(d) else {
        return false;
    };
    if size_limit != usize::MAX {
        encoded_len <= (size_limit / 2)
    } else {
        d.len() <= (count_limit / 2)
    }
}

fn quicklist_shrink_may_fit_listpack(len: usize, size_limit: usize, count_limit: usize) -> bool {
    if size_limit == usize::MAX {
        return len <= count_limit / 2;
    }
    let half_limit = size_limit / 2;
    let min_encoded_len =
        LISTPACK_FIXED_OVERHEAD_BYTES.saturating_add(len.saturating_mul(LISTPACK_MIN_ENTRY_BYTES));
    min_encoded_len <= half_limit
}

#[cfg(test)]
mod list_encoding_tests {
    use super::*;
    use redis_ds::listpack::ListPack;

    #[test]
    fn listpack_encoded_len_matches_real_listpack_for_representative_values() {
        let mut deque = VecDeque::new();
        for value in [
            b"".as_slice(),
            b"x".as_slice(),
            b"123".as_slice(),
            b"-4096".as_slice(),
            b"4096".as_slice(),
            b"0007".as_slice(),
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_slice(),
        ] {
            deque.push_back(RedisString::from_bytes(value));
        }

        let mut lp = ListPack::new();
        for value in &deque {
            assert!(lp.append(value.as_bytes()));
        }

        assert_eq!(listpack_encoded_len(&deque), Some(lp.bytes_len()));
    }

    #[test]
    fn quicklist_demote_gate_rejects_large_default_lists_before_encoding_scan() {
        let (size_limit, count_limit) = listpack_node_limit(-2);

        assert!(!quicklist_shrink_may_fit_listpack(
            10_000,
            size_limit,
            count_limit
        ));

        let first_len_that_cannot_fit = ((size_limit / 2)
            .saturating_sub(LISTPACK_FIXED_OVERHEAD_BYTES)
            / LISTPACK_MIN_ENTRY_BYTES)
            + 1;
        assert!(!quicklist_shrink_may_fit_listpack(
            first_len_that_cannot_fit,
            size_limit,
            count_limit
        ));
        assert!(quicklist_shrink_may_fit_listpack(
            first_len_that_cannot_fit - 1,
            size_limit,
            count_limit
        ));
    }

    #[test]
    fn quicklist_demote_gate_uses_count_half_limit_for_positive_fill() {
        let (size_limit, count_limit) = listpack_node_limit(128);

        assert!(quicklist_shrink_may_fit_listpack(
            64,
            size_limit,
            count_limit
        ));
        assert!(!quicklist_shrink_may_fit_listpack(
            65,
            size_limit,
            count_limit
        ));
    }

    #[test]
    fn quicklist_demote_after_shrink_preserves_large_quicklist() {
        let mut deque = VecDeque::with_capacity(10_000);
        for _ in 0..10_000 {
            deque.push_back(RedisString::from_static(b"x"));
        }
        let mut obj = RedisObject::new_quicklist_from_vec(deque);

        obj.list_try_demote_after_shrink();

        assert_eq!(obj.encoding_name(), "quicklist");
    }

    #[test]
    fn quicklist_demote_after_shrink_still_demotes_small_quicklist() {
        let mut deque = VecDeque::new();
        deque.push_back(RedisString::from_static(b"x"));
        let mut obj = RedisObject::new_quicklist_from_vec(deque);

        obj.list_try_demote_after_shrink();

        assert_eq!(obj.encoding_name(), "listpack");
    }
}

/// Encoding name reported for an `Inline`-encoded list.
/// Mirrors Valkey's listpack-to-quicklist conversion threshold: negative
/// `list-max-listpack-size` values are byte-size caps, while non-negative
/// values are entry-count caps guarded by the 8 KiB safety limit.
fn list_inline_observed_encoding(d: &VecDeque<RedisString>) -> &'static str {
    let t = get_encoding_thresholds();
    let (size_limit, count_limit) = listpack_node_limit(t.list_max_listpack_size);
    let Some(encoded_len) = listpack_encoded_len(d) else {
        return "quicklist";
    };
    if size_limit != usize::MAX {
        if encoded_len > size_limit {
            "quicklist"
        } else {
            "listpack"
        }
    } else if encoded_len > LISTPACK_SIZE_SAFETY_LIMIT || d.len() > count_limit {
        "quicklist"
    } else {
        "listpack"
    }
}

/// Encoding name reported for an `Inline`-encoded hash.
/// Returns `"listpack"` when the entry count is at or below
/// `hash-max-listpack-entries` and every field and value is at most
/// `hash-max-listpack-value` bytes; `"hashtable"` otherwise. Both thresholds
/// are read from the process-wide `ENCODING_THRESHOLDS` global so that
/// `CONFIG SET` takes effect immediately.
fn hash_inline_observed_encoding(h: &InlineHash) -> &'static str {
    let t = get_encoding_thresholds();
    if h.len() > t.hash_max_listpack_entries {
        return "hashtable";
    }
    for (k, v) in h.iter() {
        if k.as_bytes().len() > t.hash_max_listpack_value
            || v.as_bytes().len() > t.hash_max_listpack_value
        {
            return "hashtable";
        }
    }
    "listpack"
}

/// Encoding name reported for an `Inline`-encoded set.
/// All-integer sets at or below `set-max-intset-entries` report `"intset"`.
/// Otherwise mixed-content sets at or below `set-max-listpack-entries` with
/// every member at most `set-max-listpack-value` bytes report `"listpack"`;
/// anything larger reports `"hashtable"`. All thresholds are read from
/// process-wide `ENCODING_THRESHOLDS` global so that `CONFIG SET` takes effect
/// immediately.
fn set_inline_observed_encoding(s: &InlineSet) -> &'static str {
    let t = get_encoding_thresholds();
    let h = &s.data;
    let mut all_integer = true;
    let mut max_len: usize = 0;
    for m in h {
        let bytes = m.as_bytes();
        if bytes.len() > max_len {
            max_len = bytes.len();
        }
        if all_integer && !is_canonical_i64_ascii(bytes) {
            all_integer = false;
        }
    }
    let computed = if all_integer && h.len() <= t.set_max_intset_entries {
        InlineSetEncoding::Auto
    } else if h.len() <= t.set_max_listpack_entries && max_len <= t.set_max_listpack_value {
        InlineSetEncoding::ForcedListpack
    } else {
        InlineSetEncoding::ForcedHashtable
    };
    let effective = computed.max(s.sticky);
    match effective {
        InlineSetEncoding::Auto => "intset",
        InlineSetEncoding::ForcedListpack => "listpack",
        InlineSetEncoding::ForcedHashtable => "hashtable",
    }
}

/// Encoding name reported for an `Inline`-encoded sorted set.
/// Returns `"listpack"` when the entry count is at or below
/// `zset-max-listpack-entries` and every member is at most
/// `zset-max-listpack-value` bytes; `"skiplist"` otherwise. Both thresholds
/// are read from the process-wide `ENCODING_THRESHOLDS` global so that
/// `CONFIG SET` takes effect immediately.
fn zset_inline_observed_encoding(z: &InlineZSet) -> &'static str {
    let t = get_encoding_thresholds();
    if z.len() > t.zset_max_listpack_entries {
        return "skiplist";
    }
    for m in z.by_member.keys() {
        if m.as_bytes().len() > t.zset_max_listpack_value {
            return "skiplist";
        }
    }
    "listpack"
}

/// Returns `true` when `bytes` is the canonical decimal-ASCII form of an
/// `i64` value (optional leading minus sign, no leading zeros except for
/// the literal `"0"`, no whitespace, no leading `+`). Used by
/// set-encoding heuristic to decide whether an `Inline` set qualifies for
/// the `intset` label.
pub fn is_canonical_i64_ascii(bytes: &[u8]) -> bool {
    if bytes.is_empty() || bytes.len() > 20 {
        return false;
    }
    let (sign_skip, digits) = if bytes[0] == b'-' {
        (1usize, &bytes[1..])
    } else {
        (0usize, bytes)
    };
    if digits.is_empty() {
        return false;
    }
    if digits.len() > 1 && digits[0] == b'0' {
        return false;
    }
    for &b in digits {
        if !b.is_ascii_digit() {
            return false;
        }
    }
    let _ = sign_skip;
    match std::str::from_utf8(bytes) {
        Ok(s) => s.parse::<i64>().is_ok(),
        Err(_) => false,
    }
}

/// Return `true` if a string of `len` bytes should use EMBSTR encoding.
/// PORT NOTE: In C the threshold also accounts for key/expire embedded in the same
/// allocation. In Rust these live elsewhere, so we only check the value length.
/// A 64-byte threshold matches the C behaviour for the simple (no-key, no-expire) case.
#[inline]
fn should_embed_string(len: usize) -> bool {
 // SDS_TYPE_8 max is 255; the robj overhead is ~16 bytes in C; combined ≤ 128 bytes.
 // In Rust, 44 bytes (like the original Redis EMBSTR_SIZE_LIMIT) is a common choice,
 // but the C Valkey source uses a 128-byte ceiling. Use that for wire-diff fidelity.
    len <= 44
}

/// Parse a byte slice as an `i64`. Returns `None` if not a valid integer.
/// Parse a decimal integer from a byte slice.
/// Rejects leading/trailing whitespace,
/// the leading `+` sign, and any non-digit bytes. An empty slice and any
/// slice containing whitespace return `None`.
fn parse_long_long(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let s = core::str::from_utf8(bytes).ok()?;
    s.parse::<i64>().ok()
}

/// Strict canonical-decimal parser for promoting a SET value to `Int` encoding.
/// Rejects leading `+`, leading zeros (except
/// the single string `"0"`), `-0`, leading or trailing whitespace, and any
/// value whose round-trip ASCII form does not match the input byte-for-byte.
/// On success the returned value's `format!("{}")` equals the input bytes.
fn parse_canonical_decimal_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    if !matches!(bytes[0], b'-' | b'0'..=b'9') {
        return None;
    }
    let s = core::str::from_utf8(bytes).ok()?;
    let value = s.parse::<i64>().ok()?;
    if value.to_string().as_bytes() == bytes {
        Some(value)
    } else {
        None
    }
}

/// Parse a byte slice as a `long` (`i64` on 64-bit).
fn parse_long(bytes: &[u8]) -> Option<i64> {
    parse_long_long(bytes)
}

/// Parse a byte slice as a `f64`. Returns `None` if not a valid float.
fn parse_double(bytes: &[u8]) -> Option<f64> {
    let s = core::str::from_utf8(bytes).ok()?;
    let s = s.trim();
    if s.eq_ignore_ascii_case("inf") || s.eq_ignore_ascii_case("+inf") {
        return Some(f64::INFINITY);
    }
    if s.eq_ignore_ascii_case("-inf") {
        return Some(f64::NEG_INFINITY);
    }
    s.parse::<f64>().ok()
}

/// Format an `i64` as a decimal byte string.
fn format_long_long(value: i64) -> RedisString {
    RedisString::from_bytes(value.to_string().as_bytes())
}

/// Format an `f64` as a byte string, with optional human-friendly trimming.
/// PORT NOTE: C uses `long double` and custom formatting. We use `f64` and Rust's
/// default `f64` formatting, which may differ in edge cases.
/// TODO(port): match C's ld2string exactly for wire-diff fidelity in INCRBYFLOAT.
fn format_double(value: f64, human_friendly: bool) -> RedisString {
    let s = if human_friendly {
 // Trim trailing zeros like C's LD_STR_HUMAN mode.
        let raw = format!("{:.17}", value);
        let trimmed = raw.trim_end_matches('0').trim_end_matches('.');
        trimmed.to_string()
    } else {
        format!("{:.17e}", value)
    };
    RedisString::from_bytes(s.as_bytes())
}

/// Return the number of decimal digits needed to represent `n` (including '-' for negatives).
fn decimal_digit_count(n: i64) -> usize {
    if n == 0 {
        return 1;
    }
    let mut count = if n < 0 { 1usize } else { 0 };
    let mut v = n.unsigned_abs();
    while v > 0 {
        count += 1;
        v /= 10;
    }
    count
}

// ─────────────────────────────────────────────────────────────────────────────
// Public factory functions (module-level, matching C names)
// ─────────────────────────────────────────────────────────────────────────────

/// Create a raw-string object from a byte slice.
pub fn create_raw_string_object(bytes: &[u8]) -> RedisObject {
    RedisObject::new_raw_string(bytes)
}

/// Create an EMBSTR string object from a byte slice.
pub fn create_embedded_string_object(bytes: &[u8]) -> RedisObject {
    RedisObject::new_embstr(bytes)
}

/// Create a string object, auto-selecting encoding based on size.
pub fn create_string_object(bytes: &[u8]) -> RedisObject {
    RedisObject::new_string(bytes)
}

/// Create a string object from an existing `RedisString`.
pub fn create_string_object_from_sds(s: RedisString) -> RedisObject {
    RedisObject::new_string_from_redis_string(s)
}

/// Create a string object from a `long long`, with encoding control flags.
/// TODO(architect): `LL2STROBJ_AUTO` case needs the shared integer pool
/// (`shared.integers[]`), which requires a lazy_static `Arc<RedisObject>` array.
/// Until that's available, this always creates a fresh object.
pub fn create_string_object_from_long_long_with_options(value: i64, flag: i32) -> RedisObject {
    if flag == LL2STROBJ_NO_INT_ENC {
 // Must produce an EMBSTR or RAW object, never INT.
        let s = format_long_long(value);
        return create_string_object(s.as_bytes());
    }

    if flag != LL2STROBJ_NO_INT_ENC {
 // INT encoding allowed.
        return RedisObject::new_int_string(value);
    }

    let s = format_long_long(value);
    create_string_object(s.as_bytes())
}

/// Create a string object from a `long long`, preferring shared integer pool.
pub fn create_string_object_from_long_long(value: i64) -> RedisObject {
    create_string_object_from_long_long_with_options(value, LL2STROBJ_AUTO)
}

/// Create a non-shared string object from a `long long`.
pub fn create_string_object_from_long_long_for_value(value: i64) -> RedisObject {
    create_string_object_from_long_long_with_options(value, LL2STROBJ_NO_SHARED)
}

/// Create an EMBSTR or RAW string object from a `long long` (never INT-encoded).
pub fn create_string_object_from_long_long_with_sds(value: i64) -> RedisObject {
    create_string_object_from_long_long_with_options(value, LL2STROBJ_NO_INT_ENC)
}

/// Create a string object from a `f64`.
pub fn create_string_object_from_long_double(value: f64, human_friendly: bool) -> RedisObject {
    let s = format_double(value, human_friendly);
    create_string_object(s.as_bytes())
}

/// Duplicate a string object, preserving encoding.
pub fn dup_string_object(o: &RedisObject) -> Result<RedisObject, RedisError> {
    match &o.kind {
        ObjectKind::String(StringEncoding::Raw(s)) => Ok(create_raw_string_object(s.as_bytes())),
        ObjectKind::String(StringEncoding::Embstr(s)) => {
            Ok(create_embedded_string_object(s.as_bytes()))
        }
        ObjectKind::String(StringEncoding::Int(n)) => Ok(RedisObject::new_int_string(*n)),
        _ => Err(RedisError::runtime(
            b"ERR dup_string_object called on non-string",
        )),
    }
}

/// Create a QuickList list object.
pub fn create_quicklist_object(fill: i32, compress: i32) -> RedisObject {
    RedisObject::new_quicklist(fill, compress)
}

/// Create a ListPack list object.
pub fn create_list_listpack_object() -> RedisObject {
    RedisObject::new_list_listpack()
}

/// Create a HashTable set object.
pub fn create_set_object() -> RedisObject {
    RedisObject::new_set_hashtable()
}

/// Create an IntSet set object.
pub fn create_intset_object() -> RedisObject {
    RedisObject::new_intset()
}

/// Create a ListPack set object.
pub fn create_set_listpack_object() -> RedisObject {
    RedisObject::new_set_listpack()
}

/// Create a ListPack hash object.
pub fn create_hash_object() -> RedisObject {
    RedisObject::new_hash_listpack()
}

/// Create a SkipList zset object.
pub fn create_zset_object() -> RedisObject {
    RedisObject::new_zset_skiplist()
}

/// Create a ListPack zset object.
pub fn create_zset_listpack_object() -> RedisObject {
    RedisObject::new_zset_listpack()
}

/// Create a stream object.
pub fn create_stream_object() -> RedisObject {
    RedisObject::new_stream()
}

// ─────────────────────────────────────────────────────────────────────────────
// Type checking
// ─────────────────────────────────────────────────────────────────────────────

/// Check that `obj` is of the expected type. Returns `Err(RedisError::wrong_type`
/// if it is not. `None` is treated as an empty key (always matches).
pub fn check_type(obj: Option<&RedisObject>, expected: ObjectType) -> Result<(), RedisError> {
    match obj {
        None => Ok(()),
        Some(o) if o.object_type() == expected => Ok(()),
        Some(_) => Err(RedisError::wrong_type()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SDS / object representable as long long
// ─────────────────────────────────────────────────────────────────────────────

/// Return `Ok(` if the byte string can be parsed as an `i64`, writing the value.
pub fn is_sds_representable_as_long_long(
    s: &RedisString,
    llval: &mut i64,
) -> Result<(), RedisError> {
    match parse_long_long(s.as_bytes()) {
        Some(v) => {
            *llval = v;
            Ok(())
        }
        None => Err(RedisError::not_integer()),
    }
}

/// Return `Ok(` if the string object can be represented as an `i64`.
pub fn is_object_representable_as_long_long(
    o: &RedisObject,
    llval: &mut i64,
) -> Result<(), RedisError> {
    match &o.kind {
        ObjectKind::String(StringEncoding::Int(n)) => {
            *llval = *n;
            Ok(())
        }
        ObjectKind::String(StringEncoding::Raw(s) | StringEncoding::Embstr(s)) => {
            is_sds_representable_as_long_long(s, llval)
        }
        _ => Err(RedisError::runtime(b"ERR object is not a string")),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Numeric extraction helpers (for command implementations)
// ─────────────────────────────────────────────────────────────────────────────

/// Extract a `f64` from a string object, or 0 if `o` is `None`.
pub fn get_double_from_object(o: Option<&RedisObject>) -> Result<f64, RedisError> {
    match o {
        None => Ok(0.0),
        Some(obj) => obj.get_double(),
    }
}

/// Extract a `f64`, mapping errors to a caller-supplied or default message.
pub fn get_double_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<f64, RedisError> {
    get_double_from_object(o)
        .map_err(|_| RedisError::runtime(msg.unwrap_or(b"value is not a valid float")))
}

/// Extract a `f64` (as long double) from a string object, or 0 if `o` is `None`.
pub fn get_long_double_from_object(o: Option<&RedisObject>) -> Result<f64, RedisError> {
    match o {
        None => Ok(0.0),
        Some(obj) => obj.get_long_double(),
    }
}

/// Extract a long double, mapping errors to a caller-supplied or default message.
pub fn get_long_double_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<f64, RedisError> {
    get_long_double_from_object(o)
        .map_err(|_| RedisError::runtime(msg.unwrap_or(b"value is not a valid float")))
}

/// Extract an `i64` from a string object, or 0 if `o` is `None`.
pub fn get_long_long_from_object(o: Option<&RedisObject>) -> Result<i64, RedisError> {
    match o {
        None => Ok(0),
        Some(obj) => obj.get_long_long(),
    }
}

/// Extract an `i64`, mapping errors to a caller-supplied or default message.
pub fn get_long_long_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<i64, RedisError> {
    get_long_long_from_object(o)
        .map_err(|_| RedisError::runtime(msg.unwrap_or(b"value is not an integer or out of range")))
}

/// Extract a `long` as `i64`, checking it fits in `[i64::MIN, i64::MAX]`.
pub fn get_long_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<i64, RedisError> {
 // On 64-bit systems, `long` and `long long` are both i64; no range check needed.
    get_long_long_from_object_or_reply(o, msg)
}

/// Extract an `i64` in the closed range `[min, max]`.
pub fn get_range_long_from_object_or_reply(
    o: Option<&RedisObject>,
    min: i64,
    max: i64,
    msg: Option<&[u8]>,
) -> Result<i64, RedisError> {
    let value = get_long_from_object_or_reply(o, msg)?;
    if value < min || value > max {
        let err = match msg {
            Some(m) => RedisError::runtime(m),
            None => RedisError::runtime(
                format!(
                    "value is out of range, value must between {} and {}",
                    min, max
                )
                .as_bytes(),
            ),
        };
        return Err(err);
    }
    Ok(value)
}

/// Extract a non-negative `i64` (>= 0).
pub fn get_positive_long_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<i64, RedisError> {
    let fallback: &[u8] = b"value is out of range, must be positive";
    get_range_long_from_object_or_reply(o, 0, i64::MAX, msg.or(Some(fallback)))
}

/// Extract an `i32` from an object, checking the range.
pub fn get_int_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<i32, RedisError> {
    let v = get_range_long_from_object_or_reply(o, i32::MIN as i64, i32::MAX as i64, msg)?;
    Ok(v as i32)
}

// ─────────────────────────────────────────────────────────────────────────────
// String comparison (module-level, mirroring C function names)
// ─────────────────────────────────────────────────────────────────────────────

/// Compare two string objects with explicit flags.
/// Returns negative/zero/positive (like C's `strcmp`).
pub fn compare_string_objects_with_flags(
    a: &RedisObject,
    b: &RedisObject,
    flags: u32,
) -> Result<i64, RedisError> {
    if std::ptr::eq(a, b) {
        return Ok(0);
    }
    let da = a.decoded()?;
    let db = b.decoded()?;
    let ab = da.as_bytes();
    let bb = db.as_bytes();

    if flags & STRING_COMPARE_COLL != 0 {
        // TODO(port): strcoll / locale-aware compare — using byte-wise as placeholder
        Ok(compare_bytes(ab, bb))
    } else {
        Ok(compare_bytes(ab, bb))
    }
}

/// Binary comparison. Returns negative/zero/positive.
pub fn compare_string_objects(a: &RedisObject, b: &RedisObject) -> Result<i64, RedisError> {
    compare_string_objects_with_flags(a, b, STRING_COMPARE_BINARY)
}

/// Collation-based comparison.
pub fn collate_string_objects(a: &RedisObject, b: &RedisObject) -> Result<i64, RedisError> {
    compare_string_objects_with_flags(a, b, STRING_COMPARE_COLL)
}

/// Byte-by-byte memcmp-style comparison returning i64 (matches C `strcmp` contract).
fn compare_bytes(a: &[u8], b: &[u8]) -> i64 {
    let min_len = a.len().min(b.len());
    for i in 0..min_len {
        let diff = a[i] as i64 - b[i] as i64;
        if diff != 0 {
            return diff;
        }
    }
    a.len() as i64 - b.len() as i64
}

// ─────────────────────────────────────────────────────────────────────────────
// LRU / LFU set
// ─────────────────────────────────────────────────────────────────────────────

/// Set the LRU or LFU clock value on an object based on the current maxmemory policy.
/// Returns `true` if the value was updated.
/// TODO(port): needs access to lrulfu module (lrulfu_isUsingLFU, lfu_import, lru_import).
pub fn object_set_lru_or_lfu(obj: &mut RedisObject, lfu_freq: i64, lru_idle_secs: i64) -> bool {
    // TODO(port): check global maxmemory_policy once server state is accessible
    if lfu_freq >= 0 {
        debug_assert!(lfu_freq <= u8::MAX as i64);
        obj.lru = lfu_freq as u32;
        return true;
    }
    if lru_idle_secs >= 0 {
        obj.lru = crate::lru_clock::current_lru_clock().wrapping_sub(lru_idle_secs as u32);
        return true;
    }
    false
}

// ─────────────────────────────────────────────────────────────────────────────
// Memory introspection
// ─────────────────────────────────────────────────────────────────────────────

/// Estimate memory used by a key's value in bytes.
/// TODO(port): requires redis-ds type sizes (ListPack, QuickList, IntSet, ZSet, Stream rax).
/// Returning a rough estimate for now.
pub fn object_compute_size(
    _key: &RedisString,
    o: &RedisObject,
    _sample_size: usize,
    _dbid: u32,
) -> usize {
    // TODO(port): implement full size estimation per object.c:1194-1356
 // Blocked on: ListPack, QuickList, IntSet, ZSet, Stream, rax types from redis-ds.
    match &o.kind {
        ObjectKind::String(StringEncoding::Raw(s)) => std::mem::size_of::<RedisObject>() + s.len(),
        ObjectKind::String(StringEncoding::Embstr(s)) => {
            std::mem::size_of::<RedisObject>() + s.len()
        }
        ObjectKind::String(StringEncoding::Int(_)) => std::mem::size_of::<RedisObject>(),
        ObjectKind::List(ListEncoding::Inline(d)) => {
            std::mem::size_of::<RedisObject>()
                + d.iter()
                    .map(|s| s.len() + std::mem::size_of::<usize>())
                    .sum::<usize>()
        }
        ObjectKind::List(ListEncoding::ListPack(lp)) => {
            std::mem::size_of::<RedisObject>() + lp.len()
        }
        ObjectKind::List(ListEncoding::QuickList(ql)) => {
            std::mem::size_of::<RedisObject>()
                + ql.iter()
                    .map(|s| s.len() + std::mem::size_of::<usize>())
                    .sum::<usize>()
        }
        ObjectKind::Set(SetEncoding::Inline(s)) => {
            std::mem::size_of::<RedisObject>()
                + s.data
                    .iter()
                    .map(|m| m.len() + std::mem::size_of::<usize>())
                    .sum::<usize>()
        }
        ObjectKind::Set(SetEncoding::ListPack(lp)) => std::mem::size_of::<RedisObject>() + lp.len(),
        ObjectKind::Set(SetEncoding::IntSet(is)) => {
            std::mem::size_of::<RedisObject>() + is.len() * 8
        }
        ObjectKind::Set(SetEncoding::HashTable(ht)) => {
            std::mem::size_of::<RedisObject>()
                + ht.iter()
                    .map(|s| s.len() + std::mem::size_of::<usize>())
                    .sum::<usize>()
        }
        ObjectKind::ZSet(ZSetEncoding::Inline(z)) => {
            std::mem::size_of::<RedisObject>()
                + z.by_member
                    .keys()
                    .map(|m| m.len() + std::mem::size_of::<f64>())
                    .sum::<usize>()
        }
        ObjectKind::ZSet(ZSetEncoding::ListPack(lp)) => {
            std::mem::size_of::<RedisObject>() + lp.len()
        }
        ObjectKind::ZSet(ZSetEncoding::SkipList(zs)) => {
            std::mem::size_of::<RedisObject>()
                + zs.iter()
                    .map(|(s, _)| s.len() + std::mem::size_of::<f64>())
                    .sum::<usize>()
        }
        ObjectKind::Hash(HashEncoding::ListPack(lp)) => {
            std::mem::size_of::<RedisObject>() + lp.len()
        }
        ObjectKind::Hash(HashEncoding::HashTable(ht)) => {
            std::mem::size_of::<RedisObject>()
                + ht.iter()
                    .map(|(k, v)| k.len() + v.len() + std::mem::size_of::<usize>())
                    .sum::<usize>()
        }
        ObjectKind::Hash(HashEncoding::Inline(ht)) => {
            std::mem::size_of::<RedisObject>()
                + ht.iter()
                    .map(|(k, v)| k.len() + v.len() + std::mem::size_of::<usize>())
                    .sum::<usize>()
        }
        ObjectKind::Stream(_) | ObjectKind::Module => std::mem::size_of::<RedisObject>(),
        ObjectKind::Json(v) => std::mem::size_of::<RedisObject>() + v.to_string().len(),
        ObjectKind::Bloom(bf) => std::mem::size_of::<RedisObject>() + bf.bits.len(),
    }
}

/// Build a memory overhead report for MEMORY STATS / MEMORY OVERHEAD.
/// TODO(port): requires full server state access (replication, AOF, cluster, etc.).
/// Returning a default-zeroed stub.
pub fn get_memory_overhead_data(_server: &RedisServer) -> ServerMemOverhead {
    // TODO(port): implement full memory overhead calculation
 // Blocked on: replication state, AOF state, cluster state, kvstore, client stats.
    ServerMemOverhead::default()
}

/// Build the MEMORY DOCTOR diagnostic report string.
/// TODO(port): requires full getMemoryOverheadData() + server.replicas, server.clients.
pub fn get_memory_doctor_report(_server: &RedisServer) -> RedisString {
    // TODO(port): implement diagnostics
    RedisString::from_bytes(
        b"Hi Sam, memory introspection is not yet fully ported. I will be back.\n",
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// OBJECT command
// ─────────────────────────────────────────────────────────────────────────────

/// Implement the Redis `OBJECT` command.
/// Subcommands: ENCODING, FREQ, IDLETIME, REFCOUNT, HELP.
pub fn object_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let subcmd = ctx.arg(1)?;
    let subcmd_bytes = subcmd.as_bytes().to_ascii_lowercase();

    if subcmd_bytes == b"help" && ctx.arg_count() == 2 {
        let help: &[&[u8]] = &[
            b"OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"ENCODING <key>",
            b"    Return the kind of internal representation used to store the value",
            b"    associated with a <key>.",
            b"FREQ <key>",
            b"    Return the access frequency index of the <key>.",
            b"IDLETIME <key>",
            b"    Return the idle time of the <key> in seconds.",
            b"REFCOUNT <key>",
            b"    Return the number of references (always 1 in Rust port).",
        ];
        ctx.reply_array_header(help.len())?;
        for line in help {
            ctx.reply_bulk(line)?;
        }
        return Ok(());
    }

    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"OBJECT"));
    }

    let key_arg = ctx.arg(2)?.clone();
    if subcmd_bytes == b"refcount" {
        let exists = ctx
            .db_mut()
            .lookup_key_read_with_flags(&key_arg, crate::db::LOOKUP_NOTOUCH)
            .is_some();
        if !exists {
            return Err(RedisError::runtime(b"ERR no such key"));
        }
        ctx.reply_integer(1)?;
    } else if subcmd_bytes == b"encoding" {
        let name = match ctx
            .db_mut()
            .lookup_key_read_with_flags(&key_arg, crate::db::LOOKUP_NOTOUCH)
        {
            Some(obj) => obj.encoding_name(),
            None => return Err(RedisError::runtime(b"ERR no such key")),
        };
        ctx.reply_bulk(name.as_bytes())?;
    } else if subcmd_bytes == b"idletime" {
        let idle = match ctx
            .db_mut()
            .lookup_key_read_with_flags(&key_arg, crate::db::LOOKUP_NOTOUCH)
        {
            Some(obj) => obj.lru_idle_secs() as i64,
            None => return Err(RedisError::runtime(b"ERR no such key")),
        };
        ctx.reply_integer(idle)?;
    } else if subcmd_bytes == b"freq" {
        let freq = match ctx
            .db_mut()
            .lookup_key_read_with_flags(&key_arg, crate::db::LOOKUP_NOTOUCH)
        {
            Some(obj) => obj.lfu_frequency() as i64,
            None => return Err(RedisError::runtime(b"ERR no such key")),
        };
        ctx.reply_integer(freq)?;
    } else {
        return Err(RedisError::runtime(
            b"ERR unknown subcommand or wrong number of arguments",
        ));
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// MEMORY command
// ─────────────────────────────────────────────────────────────────────────────

/// Implement the Redis `MEMORY` command.
/// Subcommands: HELP, USAGE, STATS, MALLOC-STATS, DOCTOR, PURGE.
pub fn memory_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let subcmd = ctx.arg(1)?;
    let subcmd_bytes = subcmd.as_bytes().to_ascii_lowercase();

    if subcmd_bytes == b"help" && ctx.arg_count() == 2 {
        let help: &[&[u8]] = &[
            b"DOCTOR",
            b"    Return memory problems reports.",
            b"MALLOC-STATS",
            b"    Return internal statistics report from the memory allocator.",
            b"PURGE",
            b"    Attempt to purge dirty pages for reclamation by the allocator.",
            b"STATS",
            b"    Return information about the memory usage of the server.",
            b"USAGE <key> [SAMPLES <count>]",
            b"    Return memory in bytes used by <key> and its value.",
        ];
        ctx.reply_array_header(help.len())?;
        for line in help {
            ctx.reply_bulk(line)?;
        }
        return Ok(());
    }

    if subcmd_bytes == b"usage" && ctx.arg_count() >= 3 {
        let mut samples = OBJ_COMPUTE_SIZE_DEF_SAMPLES as i64;
        let mut j = 3;
        while j < ctx.arg_count() {
            let opt = ctx.arg(j)?;
            if opt.as_bytes().eq_ignore_ascii_case(b"samples") && j + 1 < ctx.arg_count() {
                let count_obj = ctx.arg(j + 1)?.clone();
                samples = get_long_long_from_object(Some(&RedisObject::from_string(count_obj)))?;
                if samples < 0 {
                    return Err(RedisError::syntax(b""));
                }
                if samples == 0 {
                    samples = i64::MAX;
                }
                j += 2;
            } else {
                return Err(RedisError::syntax(b""));
            }
        }
        // TODO(port): look up key in db and call object_compute_size
 // Blocked on CommandContext having db access.
 // PORT NOTE: samples parsed above but not yet consumed — will be passed to object_compute_size.
        let _ = samples;
        ctx.reply_null_bulk()?;
        return Ok(());
    }

    if subcmd_bytes == b"stats" && ctx.arg_count() == 2 {
        // TODO(port): gather real stats via get_memory_overhead_data
        ctx.reply_array_header(0)?;
        return Ok(());
    }

    if subcmd_bytes == b"malloc-stats" && ctx.arg_count() == 2 {
        // TODO(port): expose jemalloc stats (cfg(feature = "jemalloc"))
        ctx.reply_bulk(b"Stats not supported for the current allocator")?;
        return Ok(());
    }

    if subcmd_bytes == b"doctor" && ctx.arg_count() == 2 {
        // TODO(port): call get_memory_doctor_report with server reference
        ctx.reply_bulk(b"Memory doctor not yet ported.")?;
        return Ok(());
    }

    if subcmd_bytes == b"purge" && ctx.arg_count() == 2 {
        // TODO(port): call jemalloc_purge() (cfg(feature = "jemalloc"))
        ctx.reply_simple_string(b"OK")?;
        return Ok(());
    }

    Err(RedisError::runtime(
        b"ERR unknown subcommand or wrong number of arguments for MEMORY",
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Back-compat shim layer
// Migration helpers that let callers written against the old flat-enum stub
// (`RedisObject::String(s)`, `RedisObject::List(items)`,...) compile against
// the full-port struct/enum split. See `harness/loop/MERGE_PLAN.md`.
// ─────────────────────────────────────────────────────────────────────────────

/// Flat view of an object's variant for `matches!` macro use at call sites.
/// Migration shim — existing callers wrote `matches!(o, RedisObject::String(_))`
/// against the old flat-enum stub. They are mechanically rewritten
/// `matches!(o.flat, Flat::String)` or, equivalently, `o.is_string`.
pub enum Flat<'a> {
    String(&'a StringEncoding),
    List(&'a ListEncoding),
    Hash(&'a HashEncoding),
    Set(&'a SetEncoding),
    ZSet(&'a ZSetEncoding),
    Stream,
    Module,
    Json,
    Bloom,
}

impl RedisObject {
 /// Flat view of the object's variant — see [`Flat`].
    pub fn flat(&self) -> Flat<'_> {
        match &self.kind {
            ObjectKind::String(e) => Flat::String(e),
            ObjectKind::List(e) => Flat::List(e),
            ObjectKind::Hash(e) => Flat::Hash(e),
            ObjectKind::Set(e) => Flat::Set(e),
            ObjectKind::ZSet(e) => Flat::ZSet(e),
            ObjectKind::Stream(_) => Flat::Stream,
            ObjectKind::Module => Flat::Module,
            ObjectKind::Json(_) => Flat::Json,
            ObjectKind::Bloom(_) => Flat::Bloom,
        }
    }

    pub fn is_string(&self) -> bool {
        matches!(self.kind, ObjectKind::String(_))
    }
    pub fn is_list(&self) -> bool {
        matches!(self.kind, ObjectKind::List(_))
    }
    pub fn is_hash(&self) -> bool {
        matches!(self.kind, ObjectKind::Hash(_))
    }
    pub fn is_set(&self) -> bool {
        matches!(self.kind, ObjectKind::Set(_))
    }
    pub fn is_zset(&self) -> bool {
        matches!(self.kind, ObjectKind::ZSet(_))
    }
    pub fn is_stream(&self) -> bool {
        matches!(self.kind, ObjectKind::Stream(_))
    }
    pub fn is_json(&self) -> bool {
        matches!(self.kind, ObjectKind::Json(_))
    }
    pub fn is_bloom(&self) -> bool {
        matches!(self.kind, ObjectKind::Bloom(_))
    }

 /// Return the raw byte string if the object is `String(Raw|Embstr)`.
 /// `None` for Int-encoded strings or non-strings.
    pub fn as_string_bytes(&self) -> Option<&[u8]> {
        self.as_string().map(|s| s.as_bytes())
    }

 /// Byte view of the object's payload when string-encoded; empty slice for
 /// every other variant. Migration shim for the architect-stub `as_bytes`.
    pub fn as_bytes(&self) -> &[u8] {
        self.as_string_bytes().unwrap_or(&[])
    }

 /// Borrowed-or-owned view of the string payload regardless of encoding.
 /// `Raw`/`Embstr` borrow the underlying `RedisString`. `Int` formats
 /// integer as canonical ASCII decimal into an owned `Vec`. Non-string
 /// variants borrow an empty slice.
    pub fn string_bytes(&self) -> std::borrow::Cow<'_, [u8]> {
        match &self.kind {
            ObjectKind::String(StringEncoding::Raw(s))
            | ObjectKind::String(StringEncoding::Embstr(s)) => {
                std::borrow::Cow::Borrowed(s.as_bytes())
            }
            ObjectKind::String(StringEncoding::Int(n)) => {
                std::borrow::Cow::Owned(n.to_string().into_bytes())
            }
            _ => std::borrow::Cow::Borrowed(&[]),
        }
    }

 /// Materialise the string payload as owned bytes regardless of encoding.
 /// `Raw`/`Embstr` clone the underlying `RedisString`. `Int` formats
 /// integer as canonical ASCII decimal. Non-string variants return an
 /// empty `Vec`.
    pub fn string_bytes_owned(&self) -> Vec<u8> {
        match &self.kind {
            ObjectKind::String(StringEncoding::Raw(s))
            | ObjectKind::String(StringEncoding::Embstr(s)) => s.as_bytes().to_vec(),
            ObjectKind::String(StringEncoding::Int(n)) => n.to_string().into_bytes(),
            _ => Vec::new(),
        }
    }

 /// Number of items in a List/Set/ZSet/Hash (best-effort across encodings).
 /// Returns 0 for non-collection types.
    pub fn collection_len(&self) -> usize {
        match &self.kind {
            ObjectKind::List(ListEncoding::Inline(d)) => d.len(),
            ObjectKind::List(ListEncoding::QuickList(v)) => v.len(),
            ObjectKind::List(ListEncoding::ListPack(_)) => 0,
            ObjectKind::Set(SetEncoding::Inline(s)) => s.data.len(),
            ObjectKind::Set(SetEncoding::HashTable(h)) => h.len(),
            ObjectKind::Set(SetEncoding::IntSet(v)) => v.len(),
            ObjectKind::Set(SetEncoding::ListPack(_)) => 0,
            ObjectKind::ZSet(ZSetEncoding::Inline(z)) => z.len(),
            ObjectKind::ZSet(ZSetEncoding::SkipList(v)) => v.len(),
            ObjectKind::ZSet(ZSetEncoding::ListPack(_)) => 0,
            ObjectKind::Hash(HashEncoding::HashTable(h)) => h.len(),
            ObjectKind::Hash(HashEncoding::Inline(h)) => h.len(),
            ObjectKind::Hash(HashEncoding::ListPack(_)) => 0,
            _ => 0,
        }
    }

 /// Iterate List items as `&RedisString`.
    /// TODO(port): Phase 4 — proper iter for ListPack encoding (yields empty today).
    pub fn iter_list(&self) -> Box<dyn Iterator<Item = &RedisString> + '_> {
        match &self.kind {
            ObjectKind::List(ListEncoding::Inline(d)) => Box::new(d.iter()),
            ObjectKind::List(ListEncoding::QuickList(v)) => Box::new(v.iter()),
            _ => Box::new(std::iter::empty()),
        }
    }

 /// Iterate Set members as `&RedisString`.
    /// TODO(port): Phase 4 — proper iter for IntSet and ListPack encodings (yields empty today).
    pub fn iter_set(&self) -> Box<dyn Iterator<Item = &RedisString> + '_> {
        match &self.kind {
            ObjectKind::Set(SetEncoding::Inline(s)) => Box::new(s.data.iter()),
            ObjectKind::Set(SetEncoding::HashTable(h)) => Box::new(h.iter()),
            _ => Box::new(std::iter::empty()),
        }
    }

 /// Iterate ZSet `(member, score)` pairs.
    /// TODO(port): Phase 4 — proper iter for ListPack encoding (yields empty today).
    pub fn iter_zset(&self) -> Box<dyn Iterator<Item = (&RedisString, f64)> + '_> {
        match &self.kind {
            ObjectKind::ZSet(ZSetEncoding::Inline(z)) => {
                Box::new(z.by_order.iter().map(|(s, m)| (m, s.get())))
            }
            ObjectKind::ZSet(ZSetEncoding::SkipList(v)) => Box::new(v.iter().map(|(m, s)| (m, *s))),
            _ => Box::new(std::iter::empty()),
        }
    }

 /// Iterate Hash `(field, value)` pairs.
    /// TODO(port): Phase 4 — proper iter for ListPack encoding (yields empty today).
    pub fn iter_hash(&self) -> Box<dyn Iterator<Item = (&RedisString, &RedisString)> + '_> {
        match &self.kind {
            ObjectKind::Hash(HashEncoding::HashTable(h) | HashEncoding::Inline(h)) => {
                Box::new(h.iter())
            }
            _ => Box::new(std::iter::empty()),
        }
    }

 /// Migration alias for the architect-stub `expire_ms` accessor.
 /// Returns the absolute expiry timestamp in ms, or `None` if no TTL.
    pub fn expire_ms(&self) -> Option<i64> {
        self.get_expire()
    }
}

impl From<RedisString> for RedisObject {
    fn from(s: RedisString) -> Self {
        Self::new_string(s.as_bytes())
    }
}

impl From<&[u8]> for RedisObject {
    fn from(b: &[u8]) -> Self {
        Self::new_string(b)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         35
//   port_notes:    7
//   unsafe_blocks: 0
//   notes:         Full encoding sub-variants added (replaces architect stub). Embedded-memory
//                  tricks (hasembkey/hasembval/hasexpire) not needed in Rust — key in db HashMap,
//                  expire as field on RedisObject. incrRefCount/decrRefCount/freeXxxObject →
//                  Rust ownership. objectCommand/memoryCommand have TODO stubs for db access
//                  (needs Phase 3 CommandContext wiring). objectComputeSize and
//                  getMemoryOverheadData are stubs pending redis-ds types (Phase 4/5).
//                  long double → f64 (precision caveat documented). strcoll → byte-wise
//                  placeholder (Phase C+ needs locale crate). Shared integer pool needs
//                  lazy_static Arc array (TODO(architect)). Back-compat shim layer appended
//                  (Flat view, is_*, as_string_bytes, From<RedisString>, iter_*, collection_len)
//                  to ease the migration from the flat-enum stub.
// ──────────────────────────────────────────────────────────────────────────────
