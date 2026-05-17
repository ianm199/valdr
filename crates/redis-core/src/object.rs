//! `RedisObject` — runtime value types held in a Redis database slot.
//!
//! Translation of `src/object.c` (1931 lines, ~58 functions).
//!
//! The C `robj` struct uses embedded-memory tricks (hasembkey, hasembval, hasexpire)
//! that are pure allocator optimizations. In Rust these collapse:
//! - The key lives in the db `HashMap`, not inside the object.
//! - The expire time lives in `RedisDb`'s expiry table, not inside the object.
//! - The embedded value becomes the inner data of the enum variant.
//! - `incrRefCount`/`decrRefCount`/`freeXxxObject` are replaced by Rust ownership + `Drop`.
//! - `makeObjectShared` maps to `Arc<RedisObject>` (not yet introduced — Phase 3+).
//! - The small integer pool (`shared.integers[0..10000]`) needs a lazy-static Arc array;
//!   see `TODO(architect)` on `create_string_object_from_long_long_with_options`.
//!
//! PORT NOTE: EMBSTR and RAW string encodings are layout-identical in Rust (`Vec<u8>`
//! under the hood). The distinction is preserved as an enum variant tag because it affects
//! the semantics of `try_object_encoding` (decides whether to re-encode) and is reported by
//! `OBJECT ENCODING`. Phase 4 may collapse them if benchmarks show no benefit.

// C: object.c:31-41 (includes)
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use redis_types::{RedisError, RedisString};

use crate::command_context::CommandContext;
use crate::server::RedisServer;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// C: server.h (various OBJ_* and related constants)
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
// C: OBJ_ENCODING_* constants in server.h
// ─────────────────────────────────────────────────────────────────────────────

/// Encoding sub-variants for `RedisObject::String`.
///
/// In C these correspond to `OBJ_ENCODING_RAW`, `OBJ_ENCODING_EMBSTR`, and
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
///
/// Phase 4 will replace `ListPack` and `QuickList` with real `redis_ds`
/// types. Until then, list commands operate over `Inline`, a `VecDeque` of
/// `RedisString` providing O(1) head/tail ops and trivial index access.
#[derive(Debug, Clone)]
pub enum ListEncoding {
    /// Pragmatic interim encoding used by the in-tree list commands.
    ///
    /// Provides O(1) push/pop on both ends and O(n) middle ops, which is
    /// sufficient for byte-exact Redis semantics. Phase 4 replaces this with
    /// the real ListPack/QuickList encodings once `redis-ds` is ready.
    Inline(VecDeque<RedisString>),
    /// Compact list-pack byte array (OBJ_ENCODING_LISTPACK).
    // TODO(architect): replace VecDeque with real encoding in Phase 4
    ListPack(Vec<u8>),
    /// Doubly-linked list of list-pack nodes (OBJ_ENCODING_QUICKLIST).
    // TODO(architect): replace VecDeque with real encoding in Phase 4
    QuickList(Vec<RedisString>),
}

/// Encoding sub-variants for `RedisObject::Set`.
///
/// Phase 4 will replace `ListPack`, `IntSet`, and `HashTable` with real
/// `redis_ds` encodings. Until then, set commands operate over `Inline`,
/// a `HashSet<RedisString>` providing O(1) membership and add/remove.
#[derive(Debug, Clone)]
pub enum SetEncoding {
    /// Pragmatic interim encoding used by the in-tree set commands.
    ///
    /// Backed by `HashSet<RedisString>` for O(1) membership tests, adds,
    /// and removes, which is sufficient for byte-exact Redis semantics
    /// across SADD/SREM/SMEMBERS/SINTER/SUNION/SDIFF and friends. Phase 4
    /// swaps this for real ListPack / IntSet / HashTable encodings once
    /// `redis-ds` ships the underlying datastructures.
    Inline(HashSet<RedisString>),
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
///
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

/// Pragmatic Phase-B sorted-set storage mirroring real Redis's dict + skiplist.
///
/// The `by_member` map provides O(1) score lookup by member; the
/// `by_order` set provides O(log N) ordered traversal in
/// `(score, member)` lex order. All mutations must update both maps in
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
    ///
    /// Returns `(was_new, prev_score)` so callers can implement the
    /// `ZADD CH` and `XX/NX/GT/LT` semantics by inspecting whether the
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
    ///
    /// Mirrors real Redis's dict + zskiplist pair via `HashMap` for O(1)
    /// member-keyed score lookup and `BTreeSet` for O(log N) ordered
    /// traversal. Phase 4 swaps this for the real `redis_ds::ZSet`
    /// (skiplist + hashtable) once that crate ships the underlying
    /// datastructures.
    Inline(InlineZSet),
    /// Compact list-pack byte array (OBJ_ENCODING_LISTPACK).
    ListPack(Vec<u8>),
    /// Skip-list + hash-table pair (OBJ_ENCODING_SKIPLIST).
    // TODO(architect): replace inner Vec with redis_ds::ZSet (skiplist + hashtable) in Phase 4
    SkipList(Vec<(RedisString, f64)>),
}

/// Encoding sub-variants for `RedisObject::Hash`.
///
/// Phase 4 will replace `ListPack` and `HashTable` with real `redis_ds`
/// types. Until then, hash commands operate over `Inline`, a plain
/// `HashMap<RedisString, RedisString>` providing the byte-exact semantics
/// of every wire-level HASH operation.
#[derive(Debug, Clone)]
pub enum HashEncoding {
    /// Pragmatic interim encoding used by the in-tree hash commands.
    ///
    /// Backed by `HashMap<RedisString, RedisString>` for O(1) field lookups
    /// and updates. Phase 4 swaps this for real ListPack / HashTable
    /// encodings once `redis-ds` ships the underlying datastructures.
    Inline(HashMap<RedisString, RedisString>),
    /// Compact list-pack byte array (OBJ_ENCODING_LISTPACK).
    // TODO(architect): replace stub Vec with real listpack encoding in Phase 4
    ListPack(Vec<u8>),
    /// Full hash table (OBJ_ENCODING_HASHTABLE).
    // TODO(architect): replace HashMap with real redis-ds hashtable in Phase 4
    HashTable(HashMap<RedisString, RedisString>),
}

// ─────────────────────────────────────────────────────────────────────────────
// LRU / LFU clock
// ─────────────────────────────────────────────────────────────────────────────

/// LRU clock value (24 bits in C packed into the robj `lru` field).
/// Used by `objectGetLRUIdleSecs`, `objectGetLFUFrequency`, `objectGetIdleness`.
pub type LruClock = u32;

// ─────────────────────────────────────────────────────────────────────────────
// RedisObject — the main enum (replaces the architect stub)
// C: robj (typedef in server.h)
// ─────────────────────────────────────────────────────────────────────────────

/// The canonical Redis runtime value type.
///
/// Replaces the architect stub with full encoding sub-variants per
/// PORTING.md §2 #4. The `lru` field mirrors the C `robj.lru` 24-bit field
/// used for LRU/LFU eviction. `expire` mirrors the embedded expiry field
/// (originally stored directly in the robj's trailing memory in C).
///
/// PORT NOTE: The architect stub stored `lru` and `expire` nowhere; this port
/// adds them to the object. Phase 4 may move `expire` to a separate per-db
/// expiry table (matching `redisDb.expires` in C) and remove it from here.
#[derive(Debug, Clone)]
pub struct RedisObject {
    /// LRU/LFU data (24 bits used). 0 = not initialised.
    pub lru: LruClock,
    /// Expiry time in milliseconds since epoch, or `EXPIRY_NONE` if no expiry.
    pub expire: i64,
    /// The type + encoding + value.
    pub kind: ObjectKind,
}

/// The discriminated union of all Redis value types + encodings.
#[derive(Debug, Clone)]
pub enum ObjectKind {
    String(StringEncoding),
    List(ListEncoding),
    Hash(HashEncoding),
    Set(SetEncoding),
    ZSet(ZSetEncoding),
    /// Phase 5: streams. Placeholder until redis-ds::Stream is available.
    // TODO(architect): replace with redis_ds::Stream when Phase 5 lands
    Stream,
    /// Phase 10: module-defined types.
    // TODO(architect): replace with redis_modules::ModuleValue when Phase 10 lands
    Module,
}

// ─────────────────────────────────────────────────────────────────────────────
// Memory overhead reporting (used by MEMORY STATS / MEMORY OVERHEAD)
// C: struct serverMemOverhead in server.h
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
/// C: struct serverMemOverhead.
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
        Self { lru: 0, expire: EXPIRY_NONE, kind }
    }

    /// Create a raw-string object (OBJ_ENCODING_RAW).
    /// C: createRawStringObject(ptr, len) → object.c:139
    pub fn new_raw_string(bytes: &[u8]) -> Self {
        Self::bare(ObjectKind::String(StringEncoding::Raw(
            RedisString::from_bytes(bytes),
        )))
    }

    /// Create an EMBSTR string object.
    /// C: createEmbeddedStringObject(ptr, len) → object.c:225
    /// PORT NOTE: EMBSTR and RAW are layout-identical in Rust. The tag is kept
    /// for semantic correctness (OBJECT ENCODING output, tryObjectEncoding logic).
    pub fn new_embstr(bytes: &[u8]) -> Self {
        Self::bare(ObjectKind::String(StringEncoding::Embstr(
            RedisString::from_bytes(bytes),
        )))
    }

    /// Create a string object, choosing EMBSTR or RAW based on size heuristic.
    /// C: createStringObject(ptr, len) → object.c:244
    pub fn new_string(bytes: &[u8]) -> Self {
        if should_embed_string(bytes.len()) {
            Self::new_embstr(bytes)
        } else {
            Self::new_raw_string(bytes)
        }
    }

    /// Create an INT-encoded string object from an `i64`.
    /// C: createObject(OBJ_STRING, NULL) + set encoding INT → object.c:414
    pub fn new_int_string(value: i64) -> Self {
        Self::bare(ObjectKind::String(StringEncoding::Int(value)))
    }

    /// Create an empty list object with the pragmatic Inline encoding.
    ///
    /// Phase 4 will replace this with one of the real `redis-ds` encodings
    /// (ListPack for small lists, QuickList for larger ones). For now the
    /// `Inline` `VecDeque<RedisString>` is the single working encoding used
    /// by every list command in the redis-commands crate.
    pub fn new_list() -> Self {
        Self::bare(ObjectKind::List(ListEncoding::Inline(VecDeque::new())))
    }

    /// Create a list object with QuickList encoding.
    /// C: createQuicklistObject(fill, compress) → object.c:481
    pub fn new_quicklist(_fill: i32, _compress: i32) -> Self {
        // TODO(port): pass fill/compress to the real QuickList when redis-ds lands (Phase 4)
        Self::bare(ObjectKind::List(ListEncoding::QuickList(Vec::new())))
    }

    /// Create a list object with ListPack encoding.
    /// C: createListListpackObject() → object.c:488
    pub fn new_list_listpack() -> Self {
        Self::bare(ObjectKind::List(ListEncoding::ListPack(Vec::new())))
    }

    /// Borrow the inner list `VecDeque` for a list-encoded object.
    ///
    /// Returns `None` for non-list objects and for the stub `ListPack`/
    /// `QuickList` encodings that this round does not populate.
    pub fn list(&self) -> Option<&VecDeque<RedisString>> {
        match &self.kind {
            ObjectKind::List(ListEncoding::Inline(d)) => Some(d),
            _ => None,
        }
    }

    /// Mutably borrow the inner list `VecDeque` for a list-encoded object.
    pub fn list_mut(&mut self) -> Option<&mut VecDeque<RedisString>> {
        match &mut self.kind {
            ObjectKind::List(ListEncoding::Inline(d)) => Some(d),
            _ => None,
        }
    }

    /// Create a set object with full hash-table encoding.
    /// C: createSetObject() → object.c:495
    pub fn new_set_hashtable() -> Self {
        Self::bare(ObjectKind::Set(SetEncoding::HashTable(HashSet::new())))
    }

    /// Create a set object with IntSet encoding.
    /// C: createIntsetObject() → object.c:502
    pub fn new_intset() -> Self {
        Self::bare(ObjectKind::Set(SetEncoding::IntSet(Vec::new())))
    }

    /// Create a set object with ListPack encoding.
    /// C: createSetListpackObject() → object.c:509
    pub fn new_set_listpack() -> Self {
        Self::bare(ObjectKind::Set(SetEncoding::ListPack(Vec::new())))
    }

    /// Create an empty set object with the pragmatic Inline encoding.
    ///
    /// Phase 4 will replace this with one of the real `redis-ds` encodings
    /// (ListPack for small sets, IntSet for all-integer sets, HashTable
    /// for larger ones). For now the `Inline` `HashSet<RedisString>` is
    /// the single working encoding used by every set command in the
    /// redis-commands crate.
    pub fn new_set() -> Self {
        Self::bare(ObjectKind::Set(SetEncoding::Inline(HashSet::new())))
    }

    /// Borrow the inner member `HashSet` for a set-encoded object.
    ///
    /// Returns `None` for non-set objects and for the stub `ListPack` /
    /// `IntSet` / `HashTable` encodings that this round does not populate.
    pub fn set(&self) -> Option<&HashSet<RedisString>> {
        match &self.kind {
            ObjectKind::Set(SetEncoding::Inline(h)) => Some(h),
            _ => None,
        }
    }

    /// Mutably borrow the inner member `HashSet` for a set-encoded object.
    pub fn set_mut(&mut self) -> Option<&mut HashSet<RedisString>> {
        match &mut self.kind {
            ObjectKind::Set(SetEncoding::Inline(h)) => Some(h),
            _ => None,
        }
    }

    /// Create a hash object with ListPack encoding.
    /// C: createHashObject() → object.c:516
    pub fn new_hash_listpack() -> Self {
        Self::bare(ObjectKind::Hash(HashEncoding::ListPack(Vec::new())))
    }

    /// Create a hash object with HashTable encoding.
    pub fn new_hash_hashtable() -> Self {
        Self::bare(ObjectKind::Hash(HashEncoding::HashTable(HashMap::new())))
    }

    /// Create an empty hash object with the pragmatic Inline encoding.
    ///
    /// Phase 4 will replace this with one of the real `redis-ds` encodings
    /// (ListPack for small hashes, HashTable for larger ones). For now the
    /// `Inline` `HashMap<RedisString, RedisString>` is the single working
    /// encoding used by every hash command in the redis-commands crate.
    pub fn new_hash() -> Self {
        Self::bare(ObjectKind::Hash(HashEncoding::Inline(HashMap::new())))
    }

    /// Borrow the inner field/value `HashMap` for a hash-encoded object.
    ///
    /// Returns `None` for non-hash objects and for the stub `ListPack` /
    /// `HashTable` encodings that this round does not populate.
    pub fn hash(&self) -> Option<&HashMap<RedisString, RedisString>> {
        match &self.kind {
            ObjectKind::Hash(HashEncoding::Inline(h)) => Some(h),
            _ => None,
        }
    }

    /// Mutably borrow the inner field/value `HashMap` for a hash-encoded object.
    pub fn hash_mut(&mut self) -> Option<&mut HashMap<RedisString, RedisString>> {
        match &mut self.kind {
            ObjectKind::Hash(HashEncoding::Inline(h)) => Some(h),
            _ => None,
        }
    }

    /// Create a sorted-set object with SkipList encoding.
    /// C: createZsetObject() → object.c:523
    pub fn new_zset_skiplist() -> Self {
        Self::bare(ObjectKind::ZSet(ZSetEncoding::SkipList(Vec::new())))
    }

    /// Create a sorted-set object with ListPack encoding.
    /// C: createZsetListpackObject() → object.c:534
    pub fn new_zset_listpack() -> Self {
        Self::bare(ObjectKind::ZSet(ZSetEncoding::ListPack(Vec::new())))
    }

    /// Create an empty sorted-set object with the pragmatic Inline encoding.
    ///
    /// Phase 4 will replace this with the real `redis-ds` encodings
    /// (ListPack for small zsets, SkipList for larger ones). For now the
    /// `Inline` `InlineZSet` is the single working encoding used by every
    /// zset command in the redis-commands crate.
    pub fn new_zset() -> Self {
        Self::bare(ObjectKind::ZSet(ZSetEncoding::Inline(InlineZSet::new())))
    }

    /// Borrow the inner `InlineZSet` for a zset-encoded object.
    ///
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

    /// Create a stream object.
    /// C: createStreamObject() → object.c:541
    pub fn new_stream() -> Self {
        Self::bare(ObjectKind::Stream)
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
            ObjectKind::Stream => "stream",
            ObjectKind::Module => "module",
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
            ObjectKind::Stream => ObjectType::Stream,
            ObjectKind::Module => ObjectType::Module,
        }
    }

    /// Return the encoding name for `OBJECT ENCODING`.
    /// C: strEncoding(encoding) → object.c:1171
    pub fn encoding_name(&self) -> &'static str {
        match &self.kind {
            ObjectKind::String(StringEncoding::Raw(_)) => "raw",
            ObjectKind::String(StringEncoding::Embstr(_)) => "embstr",
            ObjectKind::String(StringEncoding::Int(_)) => "int",
            ObjectKind::List(ListEncoding::Inline(_)) => "listpack",
            ObjectKind::List(ListEncoding::QuickList(_)) => "quicklist",
            ObjectKind::List(ListEncoding::ListPack(_)) => "listpack",
            ObjectKind::Set(SetEncoding::Inline(_)) => "listpack",
            ObjectKind::Set(SetEncoding::HashTable(_)) => "hashtable",
            ObjectKind::Set(SetEncoding::IntSet(_)) => "intset",
            ObjectKind::Set(SetEncoding::ListPack(_)) => "listpack",
            ObjectKind::ZSet(ZSetEncoding::Inline(_)) => "skiplist",
            ObjectKind::ZSet(ZSetEncoding::SkipList(_)) => "skiplist",
            ObjectKind::ZSet(ZSetEncoding::ListPack(_)) => "listpack",
            ObjectKind::Hash(HashEncoding::HashTable(_)) => "hashtable",
            ObjectKind::Hash(HashEncoding::ListPack(_)) => "listpack",
            ObjectKind::Hash(HashEncoding::Inline(_)) => "hashtable",
            ObjectKind::Stream => "stream",
            ObjectKind::Module => "unknown",
        }
    }

    /// Return `true` if the object has a byte-string (RAW or EMBSTR) encoding.
    /// C: sdsEncodedObject(o) macro → `o->encoding == OBJ_ENCODING_RAW || == OBJ_ENCODING_EMBSTR`
    pub fn is_sds_encoded(&self) -> bool {
        matches!(
            &self.kind,
            ObjectKind::String(StringEncoding::Raw(_))
                | ObjectKind::String(StringEncoding::Embstr(_))
        )
    }

    // ── Expiry ────────────────────────────────────────────────────────────

    /// Return the expiry in milliseconds, or `None` if no expiry.
    /// C: objectGetExpire(o) → object.c:299
    pub fn get_expire(&self) -> Option<i64> {
        if self.expire == EXPIRY_NONE { None } else { Some(self.expire) }
    }

    /// Set the expiry in milliseconds. `None` clears it.
    /// C: objectSetExpire(o, expire) → object.c:311
    pub fn set_expire(&mut self, expire: Option<i64>) {
        self.expire = expire.unwrap_or(EXPIRY_NONE);
    }

    // ── Decoding ──────────────────────────────────────────────────────────

    /// Return a decoded (byte-string) representation of a string object.
    /// For RAW/EMBSTR: borrows the inner `RedisString`.
    /// For INT: formats the integer and returns an owned `RedisString`.
    /// C: getDecodedObject(o) → object.c:928
    pub fn decoded(&self) -> Result<Cow<'_, RedisString>, RedisError> {
        match &self.kind {
            ObjectKind::String(StringEncoding::Raw(s)) => Ok(Cow::Borrowed(s)),
            ObjectKind::String(StringEncoding::Embstr(s)) => Ok(Cow::Borrowed(s)),
            ObjectKind::String(StringEncoding::Int(n)) => {
                let s = format_long_long(*n);
                Ok(Cow::Owned(s))
            }
            _ => Err(RedisError::runtime(b"ERR decoded() called on non-string object")),
        }
    }

    // ── Encoding optimisation ─────────────────────────────────────────────

    /// Try to re-encode a string object to save memory.
    /// Converts `Raw`/`Embstr` → `Int` if the string is a valid long;
    /// Converts `Raw` → `Embstr` if the string is short enough.
    /// C: tryObjectEncodingEx(o, try_trim) → object.c:865
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
        // Vec<u8> shrink_to_fit() is the equivalent; skipped for now.
        if try_trim {
            if let ObjectKind::String(StringEncoding::Raw(ref mut s)) = self.kind {
                let _ = s; // PORT NOTE: trim_to_fit not yet implemented
            }
        }

        self
    }

    /// Convenience wrapper: `try_encode(true)`.
    /// C: tryObjectEncoding(o) → object.c:922
    pub fn try_object_encoding(self) -> Self {
        self.try_encode(true)
    }

    // ── Numeric extraction ────────────────────────────────────────────────

    /// Extract a `long long` (`i64`) from a string object.
    /// Returns `Err(RedisError::not_integer())` on failure.
    /// C: getLongLongFromObject(o, target) → object.c:1092
    pub fn get_long_long(&self) -> Result<i64, RedisError> {
        match &self.kind {
            ObjectKind::String(StringEncoding::Int(n)) => Ok(*n),
            ObjectKind::String(StringEncoding::Raw(s) | StringEncoding::Embstr(s)) => {
                parse_long_long(s.as_bytes()).ok_or_else(RedisError::not_integer)
            }
            _ => Err(RedisError::runtime(b"ERR get_long_long on non-string object")),
        }
    }

    /// Extract a `double` from a string object.
    /// Returns `Err(RedisError::not_float())` on failure.
    /// C: getDoubleFromObject(o, target) → object.c:1026
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
    /// C: getLongDoubleFromObject(o, target) → object.c:1059
    /// PORT NOTE: C uses `long double` (80-bit extended precision on x86). Rust
    /// has no equivalent; we use `f64`. This may cause precision differences
    /// for INCRBYFLOAT/HINCRBYFLOAT at the extremes of the representable range.
    /// TODO(port): evaluate f64 vs f128 (nightly) when Phase C oracle testing starts.
    pub fn get_long_double(&self) -> Result<f64, RedisError> {
        self.get_double()
    }

    /// Return the string length (number of bytes in the byte-string representation).
    /// For INT-encoded objects, returns the decimal digit count.
    /// C: stringObjectLen(o) → object.c:1017
    pub fn string_len(&self) -> Result<usize, RedisError> {
        match &self.kind {
            ObjectKind::String(StringEncoding::Raw(s) | StringEncoding::Embstr(s)) => {
                Ok(s.len())
            }
            ObjectKind::String(StringEncoding::Int(n)) => Ok(decimal_digit_count(*n)),
            _ => Err(RedisError::runtime(b"ERR string_len on non-string object")),
        }
    }

    // ── String comparison ─────────────────────────────────────────────────

    /// Binary-compare two string objects. Returns `Ordering`.
    /// C: compareStringObjects(a, b) → object.c:990
    pub fn compare_binary(&self, other: &Self) -> Result<Ordering, RedisError> {
        let a = self.decoded()?;
        let b = other.decoded()?;
        Ok(a.as_bytes().cmp(b.as_bytes()))
    }

    /// Locale-collation compare of two string objects.
    /// C: collateStringObjects(a, b) → object.c:995
    /// TODO(port): `strcoll` is locale-dependent; Rust std has no direct equivalent.
    /// Using byte-wise ordering as a placeholder. Phase C+ must use the `locale` crate.
    pub fn compare_collate(&self, other: &Self) -> Result<Ordering, RedisError> {
        self.compare_binary(other)
    }

    /// Return `true` if two string objects have equal byte representations.
    /// C: equalStringObjects(a, b) → object.c:1003
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
    /// C: objectGetLFUFrequency(o) → object.c:1645
    pub fn lfu_frequency(&self) -> u8 {
        // TODO(port): call lrulfu module's lfu_getFrequency when available
        (self.lru & 0xFF) as u8
    }

    /// Return the approximate LRU idle time in seconds.
    /// C: objectGetLRUIdleSecs(o) → object.c:1652
    pub fn lru_idle_secs(&self) -> u32 {
        // TODO(port): call lrulfu module's lru_getIdleSecs when available
        self.lru
    }

    /// Return an idleness measure (larger = more idle).
    /// C: objectGetIdleness(o) → object.c:1657
    pub fn idleness(&self) -> u32 {
        // TODO(port): call lrulfu_getIdleness when available
        self.lru
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Free-standing object creation helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Return `true` if a string of `len` bytes should use EMBSTR encoding.
/// C: shouldEmbedStringObject(val_len, key, expire) → object.c:229
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
fn parse_long_long(bytes: &[u8]) -> Option<i64> {
    let s = core::str::from_utf8(bytes).ok()?;
    s.trim().parse::<i64>().ok()
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
/// C: ll2string(buf, sizeof(buf), value) → util.c
fn format_long_long(value: i64) -> RedisString {
    RedisString::from_bytes(value.to_string().as_bytes())
}

/// Format an `f64` as a byte string, with optional human-friendly trimming.
/// C: ld2string(buf, sizeof(buf), value, flag) → util.c
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
/// C: sdigits10((long)objectGetVal(o)) → util.c
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
/// C: createRawStringObject(ptr, len) → object.c:139
pub fn create_raw_string_object(bytes: &[u8]) -> RedisObject {
    RedisObject::new_raw_string(bytes)
}

/// Create an EMBSTR string object from a byte slice.
pub fn create_embedded_string_object(bytes: &[u8]) -> RedisObject {
    RedisObject::new_embstr(bytes)
}

/// Create a string object, auto-selecting encoding based on size.
/// C: createStringObject(ptr, len) → object.c:244
pub fn create_string_object(bytes: &[u8]) -> RedisObject {
    RedisObject::new_string(bytes)
}

/// Create a string object from an existing `RedisString`.
/// C: createStringObjectFromSds(s) → object.c:252
pub fn create_string_object_from_sds(s: RedisString) -> RedisObject {
    if should_embed_string(s.len()) {
        RedisObject::new_embstr(s.as_bytes())
    } else {
        RedisObject::new_raw_string(s.as_bytes())
    }
}

/// Create a string object from a `long long`, with encoding control flags.
/// C: createStringObjectFromLongLongWithOptions(value, flag) → object.c:407
///
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
/// C: createStringObjectFromLongLong(value) → object.c:428
pub fn create_string_object_from_long_long(value: i64) -> RedisObject {
    create_string_object_from_long_long_with_options(value, LL2STROBJ_AUTO)
}

/// Create a non-shared string object from a `long long`.
/// C: createStringObjectFromLongLongForValue(value) → object.c:434
pub fn create_string_object_from_long_long_for_value(value: i64) -> RedisObject {
    create_string_object_from_long_long_with_options(value, LL2STROBJ_NO_SHARED)
}

/// Create an EMBSTR or RAW string object from a `long long` (never INT-encoded).
/// C: createStringObjectFromLongLongWithSds(value) → object.c:440
pub fn create_string_object_from_long_long_with_sds(value: i64) -> RedisObject {
    create_string_object_from_long_long_with_options(value, LL2STROBJ_NO_INT_ENC)
}

/// Create a string object from a `f64`.
/// C: createStringObjectFromLongDouble(value, humanfriendly) → object.c:450
pub fn create_string_object_from_long_double(value: f64, human_friendly: bool) -> RedisObject {
    let s = format_double(value, human_friendly);
    create_string_object(s.as_bytes())
}

/// Duplicate a string object, preserving encoding.
/// C: dupStringObject(o) → object.c:464
pub fn dup_string_object(o: &RedisObject) -> Result<RedisObject, RedisError> {
    match &o.kind {
        ObjectKind::String(StringEncoding::Raw(s)) => {
            Ok(create_raw_string_object(s.as_bytes()))
        }
        ObjectKind::String(StringEncoding::Embstr(s)) => {
            Ok(create_embedded_string_object(s.as_bytes()))
        }
        ObjectKind::String(StringEncoding::Int(n)) => Ok(RedisObject::new_int_string(*n)),
        _ => Err(RedisError::runtime(b"ERR dup_string_object called on non-string")),
    }
}

/// Create a QuickList list object.
/// C: createQuicklistObject(fill, compress) → object.c:481
pub fn create_quicklist_object(fill: i32, compress: i32) -> RedisObject {
    RedisObject::new_quicklist(fill, compress)
}

/// Create a ListPack list object.
/// C: createListListpackObject() → object.c:488
pub fn create_list_listpack_object() -> RedisObject {
    RedisObject::new_list_listpack()
}

/// Create a HashTable set object.
/// C: createSetObject() → object.c:495
pub fn create_set_object() -> RedisObject {
    RedisObject::new_set_hashtable()
}

/// Create an IntSet set object.
/// C: createIntsetObject() → object.c:502
pub fn create_intset_object() -> RedisObject {
    RedisObject::new_intset()
}

/// Create a ListPack set object.
/// C: createSetListpackObject() → object.c:509
pub fn create_set_listpack_object() -> RedisObject {
    RedisObject::new_set_listpack()
}

/// Create a ListPack hash object.
/// C: createHashObject() → object.c:516
pub fn create_hash_object() -> RedisObject {
    RedisObject::new_hash_listpack()
}

/// Create a SkipList zset object.
/// C: createZsetObject() → object.c:523
pub fn create_zset_object() -> RedisObject {
    RedisObject::new_zset_skiplist()
}

/// Create a ListPack zset object.
/// C: createZsetListpackObject() → object.c:534
pub fn create_zset_listpack_object() -> RedisObject {
    RedisObject::new_zset_listpack()
}

/// Create a stream object.
/// C: createStreamObject() → object.c:541
pub fn create_stream_object() -> RedisObject {
    RedisObject::new_stream()
}

// ─────────────────────────────────────────────────────────────────────────────
// Type checking
// ─────────────────────────────────────────────────────────────────────────────

/// Check that `obj` is of the expected type. Returns `Err(RedisError::wrong_type())`
/// if it is not. `None` is treated as an empty key (always matches).
/// C: checkType(c, o, type) → object.c:824
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

/// Return `Ok(())` if the byte string can be parsed as an `i64`, writing the value.
/// C: isSdsRepresentableAsLongLong(s, llval) → object.c:833
pub fn is_sds_representable_as_long_long(s: &RedisString, llval: &mut i64) -> Result<(), RedisError> {
    match parse_long_long(s.as_bytes()) {
        Some(v) => {
            *llval = v;
            Ok(())
        }
        None => Err(RedisError::not_integer()),
    }
}

/// Return `Ok(())` if the string object can be represented as an `i64`.
/// C: isObjectRepresentableAsLongLong(o, llval) → object.c:837
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
/// C: getDoubleFromObject(o, target) → object.c:1026
pub fn get_double_from_object(o: Option<&RedisObject>) -> Result<f64, RedisError> {
    match o {
        None => Ok(0.0),
        Some(obj) => obj.get_double(),
    }
}

/// Extract a `f64`, mapping errors to a caller-supplied or default message.
/// C: getDoubleFromObjectOrReply(c, o, target, msg) → object.c:1045
pub fn get_double_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<f64, RedisError> {
    get_double_from_object(o).map_err(|_| {
        RedisError::runtime(msg.unwrap_or(b"value is not a valid float"))
    })
}

/// Extract a `f64` (as long double) from a string object, or 0 if `o` is `None`.
/// C: getLongDoubleFromObject(o, target) → object.c:1059
pub fn get_long_double_from_object(o: Option<&RedisObject>) -> Result<f64, RedisError> {
    match o {
        None => Ok(0.0),
        Some(obj) => obj.get_long_double(),
    }
}

/// Extract a long double, mapping errors to a caller-supplied or default message.
/// C: getLongDoubleFromObjectOrReply(c, o, target, msg) → object.c:1078
pub fn get_long_double_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<f64, RedisError> {
    get_long_double_from_object(o).map_err(|_| {
        RedisError::runtime(msg.unwrap_or(b"value is not a valid float"))
    })
}

/// Extract an `i64` from a string object, or 0 if `o` is `None`.
/// C: getLongLongFromObject(o, target) → object.c:1092
pub fn get_long_long_from_object(o: Option<&RedisObject>) -> Result<i64, RedisError> {
    match o {
        None => Ok(0),
        Some(obj) => obj.get_long_long(),
    }
}

/// Extract an `i64`, mapping errors to a caller-supplied or default message.
/// C: getLongLongFromObjectOrReply(c, o, target, msg) → object.c:1111
pub fn get_long_long_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<i64, RedisError> {
    get_long_long_from_object(o).map_err(|_| {
        RedisError::runtime(msg.unwrap_or(b"value is not an integer or out of range"))
    })
}

/// Extract a `long` as `i64`, checking it fits in `[i64::MIN, i64::MAX]`.
/// C: getLongFromObjectOrReply(c, o, target, msg) → object.c:1125
pub fn get_long_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<i64, RedisError> {
    // On 64-bit systems, `long` and `long long` are both i64; no range check needed.
    get_long_long_from_object_or_reply(o, msg)
}

/// Extract an `i64` in the closed range `[min, max]`.
/// C: getRangeLongFromObjectOrReply(c, o, min, max, target, msg) → object.c:1141
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
                format!("value is out of range, value must between {} and {}", min, max)
                    .as_bytes(),
            ),
        };
        return Err(err);
    }
    Ok(value)
}

/// Extract a non-negative `i64` (>= 0).
/// C: getPositiveLongFromObjectOrReply(c, o, target, msg) → object.c:1154
pub fn get_positive_long_from_object_or_reply(
    o: Option<&RedisObject>,
    msg: Option<&[u8]>,
) -> Result<i64, RedisError> {
    let fallback: &[u8] = b"value is out of range, must be positive";
    get_range_long_from_object_or_reply(o, 0, i64::MAX, msg.or(Some(fallback)))
}

/// Extract an `i32` from an object, checking the range.
/// C: getIntFromObjectOrReply(c, o, target, msg) → object.c:1162
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
/// C: compareStringObjectsWithFlags(a, b, flags) → object.c:957
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
/// C: compareStringObjects(a, b) → object.c:990
pub fn compare_string_objects(a: &RedisObject, b: &RedisObject) -> Result<i64, RedisError> {
    compare_string_objects_with_flags(a, b, STRING_COMPARE_BINARY)
}

/// Collation-based comparison.
/// C: collateStringObjects(a, b) → object.c:995
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
/// C: objectSetLRUOrLFU(val, lfu_freq, lru_idle_secs) → object.c:1668
/// TODO(port): needs access to lrulfu module (lrulfu_isUsingLFU, lfu_import, lru_import).
pub fn object_set_lru_or_lfu(
    obj: &mut RedisObject,
    lfu_freq: i64,
    lru_idle_secs: i64,
) -> bool {
    // TODO(port): check global maxmemory_policy once server state is accessible
    if lfu_freq >= 0 {
        debug_assert!(lfu_freq <= u8::MAX as i64);
        obj.lru = lfu_freq as u32;
        return true;
    }
    if lru_idle_secs >= 0 {
        obj.lru = lru_idle_secs as u32;
        return true;
    }
    false
}

// ─────────────────────────────────────────────────────────────────────────────
// Memory introspection
// ─────────────────────────────────────────────────────────────────────────────

/// Estimate memory used by a key's value in bytes.
/// C: objectComputeSize(key, o, sample_size, dbid) → object.c:1194
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
        ObjectKind::String(StringEncoding::Raw(s)) => {
            std::mem::size_of::<RedisObject>() + s.len()
        }
        ObjectKind::String(StringEncoding::Embstr(s)) => {
            std::mem::size_of::<RedisObject>() + s.len()
        }
        ObjectKind::String(StringEncoding::Int(_)) => std::mem::size_of::<RedisObject>(),
        ObjectKind::List(ListEncoding::Inline(d)) => {
            std::mem::size_of::<RedisObject>()
                + d.iter().map(|s| s.len() + std::mem::size_of::<usize>()).sum::<usize>()
        }
        ObjectKind::List(ListEncoding::ListPack(lp)) => {
            std::mem::size_of::<RedisObject>() + lp.len()
        }
        ObjectKind::List(ListEncoding::QuickList(ql)) => {
            std::mem::size_of::<RedisObject>()
                + ql.iter().map(|s| s.len() + std::mem::size_of::<usize>()).sum::<usize>()
        }
        ObjectKind::Set(SetEncoding::Inline(ht)) => {
            std::mem::size_of::<RedisObject>()
                + ht.iter().map(|s| s.len() + std::mem::size_of::<usize>()).sum::<usize>()
        }
        ObjectKind::Set(SetEncoding::ListPack(lp)) => {
            std::mem::size_of::<RedisObject>() + lp.len()
        }
        ObjectKind::Set(SetEncoding::IntSet(is)) => {
            std::mem::size_of::<RedisObject>() + is.len() * 8
        }
        ObjectKind::Set(SetEncoding::HashTable(ht)) => {
            std::mem::size_of::<RedisObject>()
                + ht.iter().map(|s| s.len() + std::mem::size_of::<usize>()).sum::<usize>()
        }
        ObjectKind::ZSet(ZSetEncoding::Inline(z)) => {
            std::mem::size_of::<RedisObject>()
                + z.by_member
                    .iter()
                    .map(|(m, _)| m.len() + std::mem::size_of::<f64>())
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
        ObjectKind::Stream | ObjectKind::Module => std::mem::size_of::<RedisObject>(),
    }
}

/// Build a memory overhead report for MEMORY STATS / MEMORY OVERHEAD.
/// C: getMemoryOverheadData() → object.c:1368
/// TODO(port): requires full server state access (replication, AOF, cluster, etc.).
/// Returning a default-zeroed stub.
pub fn get_memory_overhead_data(_server: &RedisServer) -> ServerMemOverhead {
    // TODO(port): implement full memory overhead calculation per object.c:1368-1480
    // Blocked on: replication state, AOF state, cluster state, kvstore, client stats.
    ServerMemOverhead::default()
}

/// Build the MEMORY DOCTOR diagnostic report string.
/// C: getMemoryDoctorReport() → object.c:1492
/// TODO(port): requires full getMemoryOverheadData() + server.replicas, server.clients.
pub fn get_memory_doctor_report(_server: &RedisServer) -> RedisString {
    // TODO(port): implement diagnostics per object.c:1492-1642
    RedisString::from_bytes(
        b"Hi Sam, memory introspection is not yet fully ported. I will be back.\n",
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// OBJECT command
// C: objectCommand(client *c) → object.c:1698
// ─────────────────────────────────────────────────────────────────────────────

/// Implement the Redis `OBJECT` command.
/// Subcommands: ENCODING, FREQ, IDLETIME, REFCOUNT, HELP.
/// C: objectCommand(client *c) → object.c:1698
pub fn object_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let subcmd = ctx.arg(1)?;
    let subcmd_bytes = subcmd.as_bytes().to_ascii_lowercase();

    if subcmd_bytes == b"help" && ctx.arg_count() == 2 {
        let help: &[&[u8]] = &[
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
    let exists = ctx
        .db_mut()
        .lookup_key_read_with_flags(&key_arg, crate::db::LOOKUP_NOTOUCH)
        .is_some();
    if !exists {
        return Err(RedisError::runtime(b"ERR no such key"));
    }

    if subcmd_bytes == b"refcount" {
        ctx.reply_integer(1)?;
    } else if subcmd_bytes == b"encoding" {
        let name: &[u8] = match ctx.db().find(&key_arg) {
            Some(obj) => match &obj.kind {
                ObjectKind::String(_) => b"raw",
                ObjectKind::List(_) => b"quicklist",
                ObjectKind::Hash(_) => b"hashtable",
                ObjectKind::Set(_) => b"hashtable",
                ObjectKind::ZSet(_) => b"skiplist",
                ObjectKind::Stream => b"stream",
                ObjectKind::Module => b"raw",
            },
            None => b"none",
        };
        ctx.reply_bulk(name)?;
    } else if subcmd_bytes == b"idletime" {
        ctx.reply_integer(0)?;
    } else if subcmd_bytes == b"freq" {
        ctx.reply_integer(0)?;
    } else {
        return Err(RedisError::runtime(b"ERR unknown subcommand or wrong number of arguments"));
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// MEMORY command
// C: memoryCommand(client *c) → object.c:1748
// ─────────────────────────────────────────────────────────────────────────────

/// Implement the Redis `MEMORY` command.
/// Subcommands: HELP, USAGE, STATS, MALLOC-STATS, DOCTOR, PURGE.
/// C: memoryCommand(client *c) → object.c:1748
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
            if opt.as_bytes().to_ascii_lowercase() == b"samples" && j + 1 < ctx.arg_count() {
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
        // Blocked on CommandContext having db access (Phase 3).
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

    Err(RedisError::runtime(b"ERR unknown subcommand or wrong number of arguments for MEMORY"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Back-compat shim layer
//
// Migration helpers that let callers written against the old flat-enum stub
// (`RedisObject::String(s)`, `RedisObject::List(items)`, ...) compile against
// the full-port struct/enum split. See `harness/loop/MERGE_PLAN.md`.
// ─────────────────────────────────────────────────────────────────────────────

/// Flat view of an object's variant for `matches!` macro use at call sites.
///
/// Migration shim — existing callers wrote `matches!(o, RedisObject::String(_))`
/// against the old flat-enum stub. They are mechanically rewritten to
/// `matches!(o.flat(), Flat::String)` or, equivalently, `o.is_string()`.
pub enum Flat<'a> {
    String(&'a StringEncoding),
    List(&'a ListEncoding),
    Hash(&'a HashEncoding),
    Set(&'a SetEncoding),
    ZSet(&'a ZSetEncoding),
    Stream,
    Module,
}

impl RedisObject {
    /// Flat view of the object's variant — see [`Flat`].
    pub fn flat(&self) -> Flat<'_> {
        match &self.kind {
            ObjectKind::String(e) => Flat::String(e),
            ObjectKind::List(e)   => Flat::List(e),
            ObjectKind::Hash(e)   => Flat::Hash(e),
            ObjectKind::Set(e)    => Flat::Set(e),
            ObjectKind::ZSet(e)   => Flat::ZSet(e),
            ObjectKind::Stream    => Flat::Stream,
            ObjectKind::Module    => Flat::Module,
        }
    }

    pub fn is_string(&self) -> bool { matches!(self.kind, ObjectKind::String(_)) }
    pub fn is_list(&self)   -> bool { matches!(self.kind, ObjectKind::List(_)) }
    pub fn is_hash(&self)   -> bool { matches!(self.kind, ObjectKind::Hash(_)) }
    pub fn is_set(&self)    -> bool { matches!(self.kind, ObjectKind::Set(_)) }
    pub fn is_zset(&self)   -> bool { matches!(self.kind, ObjectKind::ZSet(_)) }
    pub fn is_stream(&self) -> bool { matches!(self.kind, ObjectKind::Stream) }

    /// Return the raw byte string if the object is `String(Raw|Embstr)`.
    /// `None` for Int-encoded strings or non-strings.
    pub fn as_string_bytes(&self) -> Option<&[u8]> {
        self.as_string().map(|s| s.as_bytes())
    }

    /// Byte view of the object's payload when string-encoded; empty slice for
    /// every other variant. Migration shim for the architect-stub `as_bytes()`.
    pub fn as_bytes(&self) -> &[u8] {
        self.as_string_bytes().unwrap_or(&[])
    }

    /// Number of items in a List/Set/ZSet/Hash (best-effort across encodings).
    /// Returns 0 for non-collection types.
    pub fn collection_len(&self) -> usize {
        match &self.kind {
            ObjectKind::List(ListEncoding::Inline(d))    => d.len(),
            ObjectKind::List(ListEncoding::QuickList(v)) => v.len(),
            ObjectKind::List(ListEncoding::ListPack(_))  => 0,
            ObjectKind::Set(SetEncoding::Inline(h))      => h.len(),
            ObjectKind::Set(SetEncoding::HashTable(h))   => h.len(),
            ObjectKind::Set(SetEncoding::IntSet(v))      => v.len(),
            ObjectKind::Set(SetEncoding::ListPack(_))    => 0,
            ObjectKind::ZSet(ZSetEncoding::Inline(z))    => z.len(),
            ObjectKind::ZSet(ZSetEncoding::SkipList(v))  => v.len(),
            ObjectKind::ZSet(ZSetEncoding::ListPack(_))  => 0,
            ObjectKind::Hash(HashEncoding::HashTable(h)) => h.len(),
            ObjectKind::Hash(HashEncoding::Inline(h))    => h.len(),
            ObjectKind::Hash(HashEncoding::ListPack(_))  => 0,
            _ => 0,
        }
    }

    /// Iterate List items as `&RedisString`.
    ///
    /// TODO(port): Phase 4 — proper iter for ListPack encoding (yields empty today).
    pub fn iter_list(&self) -> Box<dyn Iterator<Item = &RedisString> + '_> {
        match &self.kind {
            ObjectKind::List(ListEncoding::Inline(d)) => Box::new(d.iter()),
            ObjectKind::List(ListEncoding::QuickList(v)) => Box::new(v.iter()),
            _ => Box::new(std::iter::empty()),
        }
    }

    /// Iterate Set members as `&RedisString`.
    ///
    /// TODO(port): Phase 4 — proper iter for IntSet and ListPack encodings (yields empty today).
    pub fn iter_set(&self) -> Box<dyn Iterator<Item = &RedisString> + '_> {
        match &self.kind {
            ObjectKind::Set(SetEncoding::Inline(h)) => Box::new(h.iter()),
            ObjectKind::Set(SetEncoding::HashTable(h)) => Box::new(h.iter()),
            _ => Box::new(std::iter::empty()),
        }
    }

    /// Iterate ZSet `(member, score)` pairs.
    ///
    /// TODO(port): Phase 4 — proper iter for ListPack encoding (yields empty today).
    pub fn iter_zset(&self) -> Box<dyn Iterator<Item = (&RedisString, f64)> + '_> {
        match &self.kind {
            ObjectKind::ZSet(ZSetEncoding::Inline(z)) => {
                Box::new(z.by_order.iter().map(|(s, m)| (m, s.get())))
            }
            ObjectKind::ZSet(ZSetEncoding::SkipList(v)) => {
                Box::new(v.iter().map(|(m, s)| (m, *s)))
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    /// Iterate Hash `(field, value)` pairs.
    ///
    /// TODO(port): Phase 4 — proper iter for ListPack encoding (yields empty today).
    pub fn iter_hash(&self) -> Box<dyn Iterator<Item = (&RedisString, &RedisString)> + '_> {
        match &self.kind {
            ObjectKind::Hash(HashEncoding::HashTable(h)) => Box::new(h.iter()),
            ObjectKind::Hash(HashEncoding::Inline(h)) => Box::new(h.iter()),
            _ => Box::new(std::iter::empty()),
        }
    }

    /// Migration alias for the architect-stub `expire_ms()` accessor.
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
//   source:        src/object.c  (1931 lines, ~58 functions)
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
