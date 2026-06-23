//! Wasm-safe embedded Valdr command engine.
//!
//! This crate is intentionally smaller than `redis-core` + `redis-commands`.
//! It is the first EdgeStash boundary: no networking, TLS, process APIs,
//! background workers, native filesystem access, or C Lua.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};

use indexmap::{IndexMap, IndexSet};
use lua_rs_runtime::{
    Lua, LuaError, LuaString, LuaVersion, Table as LuaTable, Value as LuaValue, Variadic,
};
use redis_protocol::{encode_resp2, RespFrame};
use redis_types::RedisString;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostError {
    Unavailable,
    Message(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    InvalidJson,
    InvalidVersion,
    MissingField(&'static str),
    InvalidField(&'static str),
    InvalidHex,
}

pub trait Host {
    fn now_millis(&self) -> u64;

    fn random_bytes(&mut self, _out: &mut [u8]) -> Result<(), HostError> {
        Err(HostError::Unavailable)
    }

    fn persist_append(&mut self, _record: &[u8]) -> Result<(), HostError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct NoopHost {
    now_millis: u64,
}

impl NoopHost {
    pub fn new(now_millis: u64) -> Self {
        Self { now_millis }
    }

    pub fn set_now_millis(&mut self, now_millis: u64) {
        self.now_millis = now_millis;
    }
}

impl Host for NoopHost {
    fn now_millis(&self) -> u64 {
        self.now_millis
    }
}

#[derive(Debug, Clone)]
struct Entry {
    value: StoredValue,
    expire_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
enum StoredValue {
    String(Vec<u8>),
    /// A hash value. `IndexMap` preserves **field insertion order** (matching
    /// valkey's listpack/hashtable ordering for HGETALL/HKEYS/HVALS/HSCAN)
    /// while keeping O(1) field lookup. New fields append at the end; an
    /// overwrite of an existing field keeps its position (`IndexMap::insert`
    /// semantics); deletions use `shift_remove` to preserve the order of the
    /// remaining fields, mirroring `hashTypeDelete`.
    Hash(IndexMap<Vec<u8>, HashField>),
    ZSet(HashMap<Vec<u8>, f64>),
    List(VecDeque<Vec<u8>>),
    /// A set value. `IndexSet` preserves **member insertion order** (matching
    /// valkey's listpack ordering for SMEMBERS/SSCAN on a non-integer set)
    /// while keeping O(1) membership. New members append at the end; re-adding
    /// an existing member is a no-op that keeps its position
    /// (`IndexSet::insert` semantics); removals use `shift_remove` to preserve
    /// the order of the remaining members, mirroring `setTypeRemove` on a
    /// listpack. An all-integer set's stored order in valkey is a *sorted*
    /// intset rather than insertion order; the DUMP path replays valkey's
    /// `setTypeAddAux` encoding state machine over this insertion order to
    /// reproduce the exact stored bytes (see `rdb_set_to_dump`).
    Set(IndexSet<Vec<u8>>),
    Stream(StreamValue),
}

/// One field of a hash value. Mirrors a hashtable-encoded hash `entry`
/// (`t_hash.c`): the field's bytes plus an optional absolute per-field
/// expiry deadline in host milliseconds. `expire_at_ms == None` is the C
/// `EXPIRY_NONE` sentinel — the field has no TTL. The hash-field-TTL command
/// family (HEXPIRE/HTTL/HPERSIST/HGETEX/HGETDEL/HSETEX) reads and writes this
/// deadline; a plain HSET/HMSET/HSETNX/HINCRBY overwrite clears it back to
/// `None`, matching `hashTypeSet(..., EXPIRY_NONE, ...)`.
#[derive(Debug, Clone)]
struct HashField {
    value: Vec<u8>,
    expire_at_ms: Option<u64>,
}

impl HashField {
    /// A field with no TTL (the common case for plain HSET writes).
    fn new(value: Vec<u8>) -> Self {
        HashField {
            value,
            expire_at_ms: None,
        }
    }
}

/// A stream `ms-seq` ID, mirroring the C `streamID` struct (`stream.h`).
/// Ordering follows `streamCompareID` (`t_stream.c`): compare `ms`, then `seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
struct StreamId {
    ms: u64,
    seq: u64,
}

impl StreamId {
    const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    /// `streamIncrID` (`t_stream.c`): the successor ID. Returns `None` when
    /// `self` is the maximal possible ID (the C function wraps to 0-0 and
    /// returns `C_ERR`).
    fn incr(self) -> Option<StreamId> {
        if self.seq == u64::MAX {
            if self.ms == u64::MAX {
                None
            } else {
                Some(StreamId {
                    ms: self.ms + 1,
                    seq: 0,
                })
            }
        } else {
            Some(StreamId {
                ms: self.ms,
                seq: self.seq + 1,
            })
        }
    }

    /// `streamDecrID` (`t_stream.c`): the predecessor ID. Returns `None` when
    /// `self` is the minimal possible ID (0-0).
    fn decr(self) -> Option<StreamId> {
        if self.seq == 0 {
            if self.ms == 0 {
                None
            } else {
                Some(StreamId {
                    ms: self.ms - 1,
                    seq: u64::MAX,
                })
            }
        } else {
            Some(StreamId {
                ms: self.ms,
                seq: self.seq - 1,
            })
        }
    }

    /// `createStreamIDString` / `streamID2string` (`t_stream.c`): render as
    /// `<ms>-<seq>`.
    fn to_string_bytes(self) -> Vec<u8> {
        format!("{}-{}", self.ms, self.seq).into_bytes()
    }
}

/// The sentinel `entries_read` value meaning "the group's logical read counter
/// is not obtainable" (`SCG_INVALID_ENTRIES_READ`, `stream.h`). Stored as the
/// `-1` C sentinel; rendered as a RESP null in XINFO GROUPS.
const SCG_INVALID_ENTRIES_READ: i64 = -1;

/// One entry in a group's PEL (pending entries list), mirroring `streamNACK`
/// (`stream.h`). `delivery_time_ms` is the host clock at delivery and is
/// serialized but never asserted in differential fixtures (clock-nondeterministic).
#[derive(Debug, Clone)]
struct PendingEntry {
    consumer: Vec<u8>,
    delivery_time_ms: u64,
    delivery_count: u64,
}

/// One consumer in a group, mirroring `streamConsumer` (`stream.h`). `pending`
/// is the set of IDs this consumer owns in the group PEL. `seen_time_ms` /
/// `active_time_ms` are host-clock timestamps, serialized but clock-nondeterministic.
#[derive(Debug, Clone, Default)]
struct Consumer {
    pending: std::collections::BTreeSet<StreamId>,
    seen_time_ms: u64,
    active_time_ms: u64,
}

/// One consumer group, mirroring `streamCG` (`stream.h`). `pending` is the
/// group PEL keyed by ID; `consumers` maps consumer name → `Consumer`.
/// `entries_read` is the group's logical read counter (or `SCG_INVALID_ENTRIES_READ`).
#[derive(Debug, Clone)]
struct Group {
    last_delivered_id: StreamId,
    pending: std::collections::BTreeMap<StreamId, PendingEntry>,
    consumers: std::collections::HashMap<Vec<u8>, Consumer>,
    entries_read: i64,
}

impl Default for Group {
    fn default() -> Self {
        Group {
            last_delivered_id: StreamId::MIN,
            pending: std::collections::BTreeMap::new(),
            consumers: std::collections::HashMap::new(),
            entries_read: SCG_INVALID_ENTRIES_READ,
        }
    }
}

/// The internal state of a single stream value, mirroring the parts of the C
/// `stream` struct (`stream.h`) the non-blocking core needs. Entries are an
/// ordered map by ID; the flat `BTreeMap` replaces the C radix-tree-of-listpacks
/// but preserves the same observable ordering and range semantics. `groups`
/// holds consumer-group state (`cgroups`). `first_id` mirrors `s->first_id`
/// (the recorded-first-entry-id), which is only recomputed when the actual
/// first entry is removed — it is NOT simply the current minimum.
#[derive(Debug, Clone, Default)]
struct StreamValue {
    entries: std::collections::BTreeMap<StreamId, Vec<(Vec<u8>, Vec<u8>)>>,
    last_id: StreamId,
    max_deleted_id: StreamId,
    entries_added: u64,
    first_id: StreamId,
    groups: std::collections::HashMap<Vec<u8>, Group>,
}

impl StoredValue {
    /// The Valkey `TYPE` name for this value, mirroring `getObjectTypeName`
    /// (`db.c`). Only the variants the edge engine currently models appear
    /// here; later type waves extend both the enum and this mapping.
    fn type_name(&self) -> &'static [u8] {
        match self {
            StoredValue::String(_) => b"string",
            StoredValue::Hash(_) => b"hash",
            StoredValue::ZSet(_) => b"zset",
            StoredValue::List(_) => b"list",
            StoredValue::Set(_) => b"set",
            StoredValue::Stream(_) => b"stream",
        }
    }
}

/// Unit of the relative/absolute argument to the EXPIRE family. Seconds inputs
/// are scaled to milliseconds before being applied as absolute deadlines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpireUnit {
    Seconds,
    Milliseconds,
}

/// Which end of a list a push/pop targets, mirroring the `LPUSH`/`RPUSH`
/// (`where == LIST_HEAD`) vs `RPUSH`/`RPOP` (`where == LIST_TAIL`) split in
/// `t_list.c`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListEnd {
    Head,
    Tail,
}

/// Which multi-key set operation a `SINTER`/`SUNION`/`SDIFF` (or its `*STORE`
/// / `SINTERCARD` variant) computes, mirroring the `op` argument threaded
/// through `sinterGenericCommand` / `sunionDiffGenericCommand` in `t_set.c`.
/// `Diff` is order-sensitive: the result is the first key's set minus every
/// later key's set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetOp {
    Inter,
    Union,
    Diff,
}

/// What a SCAN-family command iterates, mirroring the `o == NULL` vs
/// `o->type` dispatch in `scanGenericCommand` (`db.c`). `Keyspace` is the plain
/// `SCAN` over key names (the only variant that accepts `TYPE`); the others are
/// `HSCAN`/`SSCAN`/`ZSCAN` over one collection's elements. The variant also
/// gates which terminal option is legal (`NOVALUES` only for `Hash`, `NOSCORES`
/// only for `ZSet`), matching `parseScanOptionsOrReply`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CollectionScan {
    Keyspace,
    Hash,
    Set,
    ZSet,
}

/// Parsed SCAN-family options, mirroring the subset of the C `scanOptions`
/// struct the edge engine honours: `MATCH` (`stringmatchlen` glob, `None` when
/// absent or the `*` fast-path), `TYPE` (`Keyspace` only), and the
/// `NOVALUES`/`NOSCORES` "only keys" flag. `COUNT` is parsed and validated but
/// not retained — the engine always completes in a single pass.
struct ScanOptions {
    pattern: Option<Vec<u8>>,
    type_filter: Option<Vec<u8>>,
    only_keys: bool,
}

/// Which multi-key sorted-set operation a `ZUNION`/`ZINTER`/`ZDIFF` (or its
/// `*STORE` / `ZINTERCARD` variant) computes, mirroring the `op` argument
/// threaded through `zunionInterDiffGenericCommand` in `t_zset.c`. `Diff`
/// subtracts every later source from the first; `Union`/`Inter` combine scores
/// per the `AGGREGATE` rule after applying per-source `WEIGHTS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZSetOp {
    Inter,
    Union,
    Diff,
}

/// How `ZUNION`/`ZINTER` combine the score of a member present in multiple
/// sources, mirroring `REDIS_AGGR_*` and `zunionInterAggregate` (`t_zset.c`).
/// `Sum` is the default; `Sum` of `+inf` and `-inf` yields `0` per the C rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZAggregate {
    Sum,
    Min,
    Max,
}

/// Optional trailing condition flag of the EXPIRE family
/// (`parseExtendedExpireArgumentsOrReply`, `expire.c`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpireCondition {
    Nx,
    Xx,
    Gt,
    Lt,
}

/// Maximum number of distinct scripts retained in the per-engine cache before
/// the least-recently-used entry is evicted. The reference server never evicts,
/// but at the edge one hot engine lives for the whole life of a Durable Object,
/// so an unbounded cache is a slow-burn memory-growth surface for a tenant that
/// keeps loading distinct scripts. Eviction is safe: a dropped script answers
/// `EVALSHA` with `NOSCRIPT`, which clients already handle by re-sending `EVAL`.
const MAX_CACHED_SCRIPTS: usize = 256;

/// Aggregate byte ceiling for cached script bodies. A single script larger than
/// this is still cached when it is the only entry (mirroring that one `EVAL`
/// must always run); the ceiling only forces eviction of older entries.
const MAX_SCRIPT_CACHE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub struct Engine<H> {
    host: H,
    db: HashMap<Vec<u8>, Entry>,
    scripts: HashMap<[u8; 40], Vec<u8>>,
    /// SHA order from least- to most-recently used, kept in lockstep with
    /// `scripts` for LRU eviction. Touch-on-use keeps a frequently invoked
    /// script (e.g. a tenant's limiter) resident even under load from
    /// one-off scripts.
    script_lru: Vec<[u8; 40]>,
    script_cache_bytes: usize,
    mutation_epoch: u64,
    /// Keys whose snapshot-visible state changed since the last `take_dirty`.
    /// Drives per-key persistence: a host writes only these keys back to
    /// storage instead of re-serializing the whole database on every command.
    /// Populated at every write/delete/expiry site (including `redis.call`
    /// mutations inside scripts); passive expiry does not add to it because a
    /// stale persisted copy of an expired key is harmless under absolute
    /// `expire_at_ms`.
    dirty: HashSet<Vec<u8>>,
    /// True between `MULTI` and the closing `EXEC`/`DISCARD`. While set, every
    /// command other than the transaction-control verbs is queued rather than
    /// executed, mirroring `c->flag.multi` (`multi.c`).
    in_multi: bool,
    /// Commands accumulated since `MULTI`, each as its own argv vector. Replayed
    /// in order by `EXEC`, mirroring `c->mstate->commands`.
    multi_queue: Vec<Vec<Vec<u8>>>,
    /// Set when a queued command failed queue-time validation (unknown command
    /// or wrong arity), mirroring `c->flag.dirty_exec`. Makes `EXEC` answer
    /// `EXECABORT` and discard the batch.
    multi_error: bool,
    /// Watched key → the per-key version observed at `WATCH` time, mirroring the
    /// per-db `watched_keys` CAS index (`multi.c`). `EXEC` compares against the
    /// live versions to decide whether the transaction was invalidated.
    watched: HashMap<Vec<u8>, u64>,
    /// Set when any watched key was touched after `WATCH`, mirroring
    /// `c->flag.dirty_cas`. Makes `EXEC` return a null array and discard the
    /// batch.
    dirty_cas: bool,
    /// Monotonic per-key write counter. `note_write` bumps the entry for the
    /// touched key; the version of an absent/never-written key is `0`. `WATCH`
    /// snapshots these and `EXEC`'s CAS check compares them, so a watched key's
    /// create/modify/delete trips `dirty_cas`. Connection state, never
    /// serialized into snapshots and excluded from `mutation_epoch`/`dirty`.
    key_versions: HashMap<Vec<u8>, u64>,
    /// True only while an EVAL_RO/EVALSHA_RO script is executing, mirroring
    /// `SCRIPT_READ_ONLY` (`script.c`). While set, any `redis.call`/`redis.pcall`
    /// of a command carrying the WRITE (or MAY_REPLICATE) command flag is
    /// rejected by `execute_inner` with valkey's exact read-only-script error,
    /// before the command runs. Reset to false once the script returns, so
    /// ordinary EVAL and direct commands are never gated.
    script_readonly: bool,
}

impl Engine<NoopHost> {
    pub fn new_in_memory() -> Self {
        Self::new(NoopHost::default())
    }
}

impl<H: Host> Engine<H> {
    pub fn new(host: H) -> Self {
        Self {
            host,
            db: HashMap::new(),
            scripts: HashMap::new(),
            script_lru: Vec::new(),
            script_cache_bytes: 0,
            mutation_epoch: 0,
            dirty: HashSet::new(),
            in_multi: false,
            multi_queue: Vec::new(),
            multi_error: false,
            watched: HashMap::new(),
            dirty_cas: false,
            key_versions: HashMap::new(),
            script_readonly: false,
        }
    }

    /// Insert (or refresh) a script in the bounded LRU cache. Re-caching an
    /// already-resident script only marks it most-recently-used. After
    /// inserting a new body, the oldest entries are evicted until both the
    /// count and aggregate-byte ceilings hold, never evicting the entry just
    /// inserted. Never bumps the mutation epoch — the script cache is excluded
    /// from snapshots.
    fn cache_script(&mut self, sha: [u8; 40], body: &[u8]) {
        if self.scripts.contains_key(&sha) {
            self.touch_script(&sha);
            return;
        }
        self.scripts.insert(sha, body.to_vec());
        self.script_lru.push(sha);
        self.script_cache_bytes += body.len();
        while self.script_lru.len() > 1
            && (self.script_lru.len() > MAX_CACHED_SCRIPTS
                || self.script_cache_bytes > MAX_SCRIPT_CACHE_BYTES)
        {
            let evicted = self.script_lru.remove(0);
            if let Some(body) = self.scripts.remove(&evicted) {
                self.script_cache_bytes -= body.len();
            }
        }
    }

    /// Mark a resident script most-recently-used.
    fn touch_script(&mut self, sha: &[u8; 40]) {
        if let Some(index) = self.script_lru.iter().position(|entry| entry == sha) {
            let sha = self.script_lru.remove(index);
            self.script_lru.push(sha);
        }
    }

    fn clear_script_cache(&mut self) {
        self.scripts.clear();
        self.script_lru.clear();
        self.script_cache_bytes = 0;
    }

    /// Monotonic counter bumped whenever snapshot-visible state changes:
    /// key writes, deletes, and expiry updates, including those made through
    /// `redis.call` inside scripts. Passive expiry of already-dead keys and
    /// script-cache changes do not bump it because absolute `expire_at_ms`
    /// values make a stale persisted copy of an expired key harmless and the
    /// script cache is excluded from snapshots. Persistence layers compare
    /// epochs to skip exporting and rewriting unchanged state.
    pub fn mutation_epoch(&self) -> u64 {
        self.mutation_epoch
    }

    /// Record that `key` was written, deleted, or had its expiry changed:
    /// bumps the mutation epoch and marks the key dirty for per-key
    /// persistence. Every mutating command path calls this with the exact
    /// key(s) it touched, so `take_dirty` yields precisely the keys a host
    /// must flush.
    fn note_write(&mut self, key: &[u8]) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        self.dirty.insert(key.to_vec());
        self.bump_key_version(key);
    }

    /// Advance the WATCH/CAS version of `key` and, if the key is currently
    /// watched, mark the transaction dirty so the next `EXEC` aborts. Mirrors
    /// `touchWatchedKey` (`multi.c`): any create/modify/delete of a watched key
    /// — including by this same connection before `EXEC` — invalidates the CAS.
    /// Transaction bookkeeping only; never touches `mutation_epoch`/`dirty` and
    /// is excluded from snapshots.
    fn bump_key_version(&mut self, key: &[u8]) {
        let version = self.key_versions.entry(key.to_vec()).or_insert(0);
        *version = version.wrapping_add(1);
        if self.watched.contains_key(key) {
            self.dirty_cas = true;
        }
    }

    /// Drain the set of keys changed since the last call, sorted for
    /// deterministic flush order. A host persists each returned key by calling
    /// `export_key` (write the bytes) or, when it returns `None`, deleting the
    /// key from storage.
    pub fn take_dirty(&mut self) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = self.dirty.drain().collect();
        keys.sort();
        keys
    }

    /// Serialize one key's live entry to the same JSON shape used inside a
    /// full snapshot. Returns `None` when the key is absent or already expired
    /// (the host then deletes it from storage). Purges the key first so an
    /// expired key never round-trips.
    pub fn export_key(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.purge_if_expired(key);
        let entry = self.db.get(key)?;
        let object = encode_entry(key, entry);
        Some(serde_json::to_vec(&JsonValue::Object(object)).unwrap_or_default())
    }

    /// Restore one key from bytes produced by `export_key`. Does not mark the
    /// key dirty — this is a load from authoritative storage, not a mutation.
    pub fn import_key(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        let value: JsonValue =
            serde_json::from_slice(bytes).map_err(|_| SnapshotError::InvalidJson)?;
        let object = value
            .as_object()
            .ok_or(SnapshotError::InvalidField("key"))?;
        let (key, entry) = decode_entry(object)?;
        self.db.insert(key, entry);
        Ok(())
    }

    fn mark_all_dirty(&mut self) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        let keys: Vec<Vec<u8>> = self.db.keys().cloned().collect();
        for key in keys {
            self.bump_key_version(&key);
            self.dirty.insert(key);
        }
    }

    pub fn host(&self) -> &H {
        &self.host
    }

    pub fn host_mut(&mut self) -> &mut H {
        &mut self.host
    }

    /// Top-level client entry point. This is the only place transaction
    /// queueing happens: `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` always run
    /// immediately, and while a transaction is open every other command is
    /// validated and queued instead of executed. Script `redis.call` goes
    /// straight to `execute_inner` and is therefore never queued — a script
    /// runs atomically, mirroring that the reference server dispatches script
    /// commands outside `processCommand`'s MULTI gate.
    pub fn execute(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        let Some(command) = argv.first() else {
            return unknown_command_error(b"", &[]);
        };

        if ascii_eq(command, b"MULTI") {
            return self.multi_command(argv);
        } else if ascii_eq(command, b"EXEC") {
            return self.exec_command(argv);
        } else if ascii_eq(command, b"DISCARD") {
            return self.discard_command(argv);
        } else if ascii_eq(command, b"WATCH") {
            return self.watch_command(argv);
        } else if ascii_eq(command, b"UNWATCH") {
            return self.unwatch_command(argv);
        }

        if self.in_multi {
            return self.queue_command(argv);
        }

        self.execute_inner(argv, false)
    }

    /// `MULTI` — open a transaction block. Nesting is rejected with the same
    /// `CMD_NO_MULTI` message the reference server emits (`server.c`); the
    /// rejection flags the open transaction dirty so its `EXEC` aborts.
    fn multi_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 1 {
            return wrong_arity(b"multi");
        }
        if self.in_multi {
            self.multi_error = true;
            return err(b"ERR Command 'multi' not allowed inside a transaction");
        }
        self.in_multi = true;
        simple(b"OK")
    }

    /// `DISCARD` — abort the open transaction, clearing the queue, the
    /// dirty flags, and the WATCH set. Mirrors `discardCommand`/`discardTransaction`.
    fn discard_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 1 {
            return wrong_arity(b"discard");
        }
        if !self.in_multi {
            return err(b"ERR DISCARD without MULTI");
        }
        self.reset_transaction();
        simple(b"OK")
    }

    /// `EXEC` — run the queued batch. Mirrors `execCommand`: aborts with
    /// `EXECABORT` when a queued command failed validation, returns a null
    /// array when a watched key changed (CAS), otherwise replays every queued
    /// command via `execute_inner` and returns the array of replies. A queued
    /// command that errors at runtime contributes its error frame to the array
    /// — only queue-time validation errors abort the whole transaction.
    fn exec_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 1 {
            return wrong_arity(b"exec");
        }
        if !self.in_multi {
            return err(b"ERR EXEC without MULTI");
        }
        if self.multi_error {
            self.reset_transaction();
            return err(b"EXECABORT Transaction discarded because of previous errors.");
        }
        if self.dirty_cas {
            self.reset_transaction();
            return RespFrame::null_array();
        }
        let queued = std::mem::take(&mut self.multi_queue);
        let mut replies = Vec::with_capacity(queued.len());
        for command in &queued {
            replies.push(self.execute_inner(command, false));
        }
        self.reset_transaction();
        RespFrame::array(replies)
    }

    /// `WATCH key [key …]` — snapshot each key's current version for CAS.
    /// Rejected inside a transaction with the `CMD_NO_MULTI` message (which
    /// flags the open transaction dirty), mirroring the reference server.
    fn watch_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"watch");
        }
        if self.in_multi {
            self.multi_error = true;
            return err(b"ERR Command 'watch' not allowed inside a transaction");
        }
        for key in &argv[1..] {
            let version = self.key_versions.get(key.as_slice()).copied().unwrap_or(0);
            self.watched.insert(key.clone(), version);
        }
        simple(b"OK")
    }

    /// `UNWATCH` — drop every CAS watcher and clear the dirty-CAS flag.
    /// Mirrors `unwatchCommand`/`unwatchAllKeys`.
    fn unwatch_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 1 {
            return wrong_arity(b"unwatch");
        }
        self.watched.clear();
        self.dirty_cas = false;
        simple(b"OK")
    }

    /// Validate a command for queueing and either queue it (`+QUEUED`) or reject
    /// it. A queue-time validation failure (unknown command or wrong arity)
    /// flags the transaction dirty so the eventual `EXEC` answers `EXECABORT`,
    /// mirroring `flagTransaction` being called from `processCommand`'s
    /// pre-execution checks (`server.c`). Mirrors `queueMultiCommand` short-
    /// circuiting once the transaction is already aborted.
    fn queue_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if let Err(reply) = self.validate_for_queue(argv) {
            self.multi_error = true;
            return reply;
        }
        if !self.multi_error {
            self.multi_queue.push(argv.to_vec());
        }
        simple(b"QUEUED")
    }

    /// Queue-time command validation, mirroring the command lookup + arity
    /// checks the reference server runs in `processCommand` before deciding to
    /// queue. Returns the exact rejection frame (`unknown command` / wrong
    /// arity) on failure. Uses the engine's own dispatchable-command set so the
    /// check matches what `execute_inner` would actually accept.
    fn validate_for_queue(&self, argv: &[Vec<u8>]) -> Result<(), RespFrame> {
        let command = &argv[0];
        let Some(arity) = command_arity(command) else {
            return Err(unknown_command_error(command, &argv[1..]));
        };
        let argc = argv.len() as i64;
        if (arity > 0 && arity != argc) || argc < -arity {
            return Err(wrong_arity(&command.to_ascii_lowercase()));
        }
        Ok(())
    }

    /// Clear all transaction state: the multi bit, the queue, both dirty flags,
    /// and the WATCH set. Mirrors `discardTransaction`. Called by `EXEC` (after
    /// the batch runs), `DISCARD`, and the abort paths.
    fn reset_transaction(&mut self) {
        self.in_multi = false;
        self.multi_queue.clear();
        self.multi_error = false;
        self.watched.clear();
        self.dirty_cas = false;
    }

    pub fn export_snapshot(&mut self) -> Vec<u8> {
        self.purge_expired_keys();

        let mut keys: Vec<_> = self.db.iter().collect();
        keys.sort_by(|(left, _), (right, _)| left.cmp(right));

        let mut encoded_keys = Vec::with_capacity(keys.len());
        for (key, entry) in keys {
            encoded_keys.push(JsonValue::Object(encode_entry(key, entry)));
        }

        serde_json::to_vec(&json!({
            "format": "valdr-engine-snapshot",
            "version": 1,
            "keys": encoded_keys,
        }))
        .unwrap_or_else(|_| {
            b"{\"format\":\"valdr-engine-snapshot\",\"version\":1,\"keys\":[]}".to_vec()
        })
    }

    pub fn import_snapshot(&mut self, snapshot: &[u8]) -> Result<(), SnapshotError> {
        let json: JsonValue =
            serde_json::from_slice(snapshot).map_err(|_| SnapshotError::InvalidJson)?;
        if json.get("format").and_then(JsonValue::as_str) != Some("valdr-engine-snapshot") {
            return Err(SnapshotError::InvalidField("format"));
        }
        if json.get("version").and_then(JsonValue::as_u64) != Some(1) {
            return Err(SnapshotError::InvalidVersion);
        }
        let keys = json
            .get("keys")
            .and_then(JsonValue::as_array)
            .ok_or(SnapshotError::MissingField("keys"))?;

        let mut next_db = HashMap::new();
        for item in keys {
            let object = item
                .as_object()
                .ok_or(SnapshotError::InvalidField("keys"))?;
            let (key, entry) = decode_entry(object)?;
            next_db.insert(key, entry);
        }

        self.db = next_db;
        self.clear_script_cache();
        self.mark_all_dirty();
        Ok(())
    }

    pub fn execute_rest(&mut self, request: RestRequest<'_>) -> RestResponse {
        if !matches!(
            request.method,
            RestMethod::Get | RestMethod::Post | RestMethod::Put | RestMethod::Head
        ) {
            return RestResponse::json_error(405, b"ERR unsupported HTTP method");
        }

        let command_result = match rest_command_from_request(request) {
            Ok(RestCommand::Single(argv)) => {
                let frame = self.execute(&argv);
                if request.method == RestMethod::Head {
                    return RestResponse {
                        status: rest_status_for_frame(&frame),
                        content_type: rest_content_type(request.response_format),
                        body: Vec::new(),
                    };
                }
                match request.response_format {
                    RestResponseFormat::Json => RestResponse::json_frame(frame),
                    RestResponseFormat::Resp2 => RestResponse::resp2_frame(frame),
                }
            }
            Ok(RestCommand::Pipeline(commands)) => {
                let mut items = Vec::with_capacity(commands.len());
                for argv in commands {
                    items.push(rest_frame_json(self.execute(&argv)));
                }
                match serde_json::to_vec(&JsonValue::Array(items)) {
                    Ok(body) => RestResponse {
                        status: 200,
                        content_type: APPLICATION_JSON,
                        body,
                    },
                    Err(_) => RestResponse::json_error(500, b"ERR JSON encode failed"),
                }
            }
            Err(error) => RestResponse::json_error(400, &error),
        };

        command_result
    }

    fn execute_inner(&mut self, argv: &[Vec<u8>], from_script: bool) -> RespFrame {
        let Some(command) = argv.first() else {
            return unknown_command_error(b"", &[]);
        };
        if from_script && script_blocked_command(command) {
            return err(b"ERR This Redis command is not allowed from script");
        }
        if from_script && self.script_readonly && argv_is_write(argv) {
            return err(b"ERR Write commands are not allowed from read-only scripts.");
        }

        if ascii_eq(command, b"GET") {
            self.get_command(argv)
        } else if ascii_eq(command, b"SET") {
            self.set_command(argv)
        } else if ascii_eq(command, b"SETEX") {
            self.setex_command(argv)
        } else if ascii_eq(command, b"DEL") {
            self.del_command(argv, b"del")
        } else if ascii_eq(command, b"EXISTS") {
            self.exists_command(argv)
        } else if ascii_eq(command, b"INCR") {
            self.incr_command(argv)
        } else if ascii_eq(command, b"INCRBY") {
            self.incrby_command(argv)
        } else if ascii_eq(command, b"DECR") {
            self.decr_command(argv)
        } else if ascii_eq(command, b"DECRBY") {
            self.decrby_command(argv)
        } else if ascii_eq(command, b"APPEND") {
            self.append_command(argv)
        } else if ascii_eq(command, b"STRLEN") {
            self.strlen_command(argv)
        } else if ascii_eq(command, b"SETNX") {
            self.setnx_command(argv)
        } else if ascii_eq(command, b"GETSET") {
            self.getset_command(argv)
        } else if ascii_eq(command, b"GETDEL") {
            self.getdel_command(argv)
        } else if ascii_eq(command, b"MGET") {
            self.mget_command(argv)
        } else if ascii_eq(command, b"EXPIRE") {
            self.expire_command(argv, ExpireUnit::Seconds, false)
        } else if ascii_eq(command, b"PEXPIRE") {
            self.expire_command(argv, ExpireUnit::Milliseconds, false)
        } else if ascii_eq(command, b"EXPIREAT") {
            self.expire_command(argv, ExpireUnit::Seconds, true)
        } else if ascii_eq(command, b"PEXPIREAT") {
            self.expire_command(argv, ExpireUnit::Milliseconds, true)
        } else if ascii_eq(command, b"PERSIST") {
            self.persist_command(argv)
        } else if ascii_eq(command, b"TTL") {
            self.ttl_command(argv, false, false)
        } else if ascii_eq(command, b"PTTL") {
            self.ttl_command(argv, true, false)
        } else if ascii_eq(command, b"EXPIRETIME") {
            self.ttl_command(argv, false, true)
        } else if ascii_eq(command, b"PEXPIRETIME") {
            self.ttl_command(argv, true, true)
        } else if ascii_eq(command, b"TYPE") {
            self.type_command(argv)
        } else if ascii_eq(command, b"RENAME") {
            self.rename_command(argv, false)
        } else if ascii_eq(command, b"RENAMENX") {
            self.rename_command(argv, true)
        } else if ascii_eq(command, b"COPY") {
            self.copy_command(argv)
        } else if ascii_eq(command, b"TOUCH") {
            self.touch_command(argv)
        } else if ascii_eq(command, b"UNLINK") {
            self.del_command(argv, b"unlink")
        } else if ascii_eq(command, b"PING") {
            self.ping_command(argv)
        } else if ascii_eq(command, b"ECHO") {
            self.echo_command(argv)
        } else if ascii_eq(command, b"FLUSHALL") {
            self.flushall_command(argv)
        } else if ascii_eq(command, b"HSET") {
            self.hset_command(argv)
        } else if ascii_eq(command, b"HGET") {
            self.hget_command(argv)
        } else if ascii_eq(command, b"HGETALL") {
            self.hgetall_command(argv)
        } else if ascii_eq(command, b"HDEL") {
            self.hdel_command(argv)
        } else if ascii_eq(command, b"HEXISTS") {
            self.hexists_command(argv)
        } else if ascii_eq(command, b"HLEN") {
            self.hlen_command(argv)
        } else if ascii_eq(command, b"HMGET") {
            self.hmget_command(argv)
        } else if ascii_eq(command, b"HKEYS") {
            self.hkeys_command(argv)
        } else if ascii_eq(command, b"HVALS") {
            self.hvals_command(argv)
        } else if ascii_eq(command, b"HSTRLEN") {
            self.hstrlen_command(argv)
        } else if ascii_eq(command, b"HSETNX") {
            self.hsetnx_command(argv)
        } else if ascii_eq(command, b"HINCRBY") {
            self.hincrby_command(argv)
        } else if ascii_eq(command, b"HMSET") {
            self.hmset_command(argv)
        } else if ascii_eq(command, b"HEXPIRE") {
            self.hexpire_command(argv, ExpireUnit::Seconds, false)
        } else if ascii_eq(command, b"HPEXPIRE") {
            self.hexpire_command(argv, ExpireUnit::Milliseconds, false)
        } else if ascii_eq(command, b"HEXPIREAT") {
            self.hexpire_command(argv, ExpireUnit::Seconds, true)
        } else if ascii_eq(command, b"HPEXPIREAT") {
            self.hexpire_command(argv, ExpireUnit::Milliseconds, true)
        } else if ascii_eq(command, b"HTTL") {
            self.httl_command(argv, false, false)
        } else if ascii_eq(command, b"HPTTL") {
            self.httl_command(argv, true, false)
        } else if ascii_eq(command, b"HEXPIRETIME") {
            self.httl_command(argv, false, true)
        } else if ascii_eq(command, b"HPEXPIRETIME") {
            self.httl_command(argv, true, true)
        } else if ascii_eq(command, b"HPERSIST") {
            self.hpersist_command(argv)
        } else if ascii_eq(command, b"HGETEX") {
            self.hgetex_command(argv)
        } else if ascii_eq(command, b"HGETDEL") {
            self.hgetdel_command(argv)
        } else if ascii_eq(command, b"HSETEX") {
            self.hsetex_command(argv)
        } else if ascii_eq(command, b"ZADD") {
            self.zadd_command(argv)
        } else if ascii_eq(command, b"ZSCORE") {
            self.zscore_command(argv)
        } else if ascii_eq(command, b"ZINCRBY") {
            self.zincrby_command(argv)
        } else if ascii_eq(command, b"ZREM") {
            self.zrem_command(argv)
        } else if ascii_eq(command, b"ZCARD") {
            self.zcard_command(argv)
        } else if ascii_eq(command, b"ZRANK") {
            self.zrank_command(argv, false)
        } else if ascii_eq(command, b"ZREVRANK") {
            self.zrank_command(argv, true)
        } else if ascii_eq(command, b"ZRANGE") {
            self.zrange_command(argv)
        } else if ascii_eq(command, b"ZRANGEBYSCORE") {
            self.zrangebyscore_command(argv, false)
        } else if ascii_eq(command, b"ZREVRANGEBYSCORE") {
            self.zrangebyscore_command(argv, true)
        } else if ascii_eq(command, b"ZREVRANGE") {
            self.zrevrange_command(argv)
        } else if ascii_eq(command, b"ZPOPMIN") {
            self.zpop_command(argv, false)
        } else if ascii_eq(command, b"ZPOPMAX") {
            self.zpop_command(argv, true)
        } else if ascii_eq(command, b"ZMSCORE") {
            self.zmscore_command(argv)
        } else if ascii_eq(command, b"ZCOUNT") {
            self.zcount_command(argv)
        } else if ascii_eq(command, b"ZLEXCOUNT") {
            self.zlexcount_command(argv)
        } else if ascii_eq(command, b"ZRANGEBYLEX") {
            self.zrangebylex_command(argv, false)
        } else if ascii_eq(command, b"ZREVRANGEBYLEX") {
            self.zrangebylex_command(argv, true)
        } else if ascii_eq(command, b"ZREMRANGEBYRANK") {
            self.zremrangebyrank_command(argv)
        } else if ascii_eq(command, b"ZREMRANGEBYSCORE") {
            self.zremrangebyscore_command(argv)
        } else if ascii_eq(command, b"ZREMRANGEBYLEX") {
            self.zremrangebylex_command(argv)
        } else if ascii_eq(command, b"ZUNIONSTORE") {
            self.zunion_inter_diff_command(argv, ZSetOp::Union, true, false)
        } else if ascii_eq(command, b"ZINTERSTORE") {
            self.zunion_inter_diff_command(argv, ZSetOp::Inter, true, false)
        } else if ascii_eq(command, b"ZDIFFSTORE") {
            self.zunion_inter_diff_command(argv, ZSetOp::Diff, true, false)
        } else if ascii_eq(command, b"ZUNION") {
            self.zunion_inter_diff_command(argv, ZSetOp::Union, false, false)
        } else if ascii_eq(command, b"ZINTER") {
            self.zunion_inter_diff_command(argv, ZSetOp::Inter, false, false)
        } else if ascii_eq(command, b"ZDIFF") {
            self.zunion_inter_diff_command(argv, ZSetOp::Diff, false, false)
        } else if ascii_eq(command, b"ZINTERCARD") {
            self.zunion_inter_diff_command(argv, ZSetOp::Inter, false, true)
        } else if ascii_eq(command, b"ZRANGESTORE") {
            self.zrangestore_command(argv)
        } else if ascii_eq(command, b"ZMPOP") {
            self.zmpop_command(argv)
        } else if ascii_eq(command, b"LPUSH") {
            self.push_command(argv, ListEnd::Head, false)
        } else if ascii_eq(command, b"RPUSH") {
            self.push_command(argv, ListEnd::Tail, false)
        } else if ascii_eq(command, b"LPUSHX") {
            self.push_command(argv, ListEnd::Head, true)
        } else if ascii_eq(command, b"RPUSHX") {
            self.push_command(argv, ListEnd::Tail, true)
        } else if ascii_eq(command, b"LPOP") {
            self.pop_command(argv, ListEnd::Head)
        } else if ascii_eq(command, b"RPOP") {
            self.pop_command(argv, ListEnd::Tail)
        } else if ascii_eq(command, b"LLEN") {
            self.llen_command(argv)
        } else if ascii_eq(command, b"LRANGE") {
            self.lrange_command(argv)
        } else if ascii_eq(command, b"LINDEX") {
            self.lindex_command(argv)
        } else if ascii_eq(command, b"LSET") {
            self.lset_command(argv)
        } else if ascii_eq(command, b"LINSERT") {
            self.linsert_command(argv)
        } else if ascii_eq(command, b"LREM") {
            self.lrem_command(argv)
        } else if ascii_eq(command, b"LTRIM") {
            self.ltrim_command(argv)
        } else if ascii_eq(command, b"RPOPLPUSH") {
            self.rpoplpush_command(argv)
        } else if ascii_eq(command, b"LMOVE") {
            self.lmove_command(argv)
        } else if ascii_eq(command, b"LMPOP") {
            self.lmpop_command(argv)
        } else if ascii_eq(command, b"LPOS") {
            self.lpos_command(argv)
        } else if ascii_eq(command, b"SADD") {
            self.sadd_command(argv)
        } else if ascii_eq(command, b"SREM") {
            self.srem_command(argv)
        } else if ascii_eq(command, b"SCARD") {
            self.scard_command(argv)
        } else if ascii_eq(command, b"SISMEMBER") {
            self.sismember_command(argv)
        } else if ascii_eq(command, b"SMISMEMBER") {
            self.smismember_command(argv)
        } else if ascii_eq(command, b"SMEMBERS") {
            self.smembers_command(argv)
        } else if ascii_eq(command, b"SMOVE") {
            self.smove_command(argv)
        } else if ascii_eq(command, b"SINTER") {
            self.set_op_command(argv, SetOp::Inter, None, false)
        } else if ascii_eq(command, b"SUNION") {
            self.set_op_command(argv, SetOp::Union, None, false)
        } else if ascii_eq(command, b"SDIFF") {
            self.set_op_command(argv, SetOp::Diff, None, false)
        } else if ascii_eq(command, b"SINTERCARD") {
            self.sintercard_command(argv)
        } else if ascii_eq(command, b"SINTERSTORE") {
            self.set_store_command(argv, SetOp::Inter)
        } else if ascii_eq(command, b"SUNIONSTORE") {
            self.set_store_command(argv, SetOp::Union)
        } else if ascii_eq(command, b"SDIFFSTORE") {
            self.set_store_command(argv, SetOp::Diff)
        } else if ascii_eq(command, b"XADD") {
            self.xadd_command(argv)
        } else if ascii_eq(command, b"XLEN") {
            self.xlen_command(argv)
        } else if ascii_eq(command, b"XRANGE") {
            self.xrange_command(argv, false)
        } else if ascii_eq(command, b"XREVRANGE") {
            self.xrange_command(argv, true)
        } else if ascii_eq(command, b"XDEL") {
            self.xdel_command(argv)
        } else if ascii_eq(command, b"XTRIM") {
            self.xtrim_command(argv)
        } else if ascii_eq(command, b"XSETID") {
            self.xsetid_command(argv)
        } else if ascii_eq(command, b"XREAD") {
            self.xread_command(argv)
        } else if ascii_eq(command, b"XGROUP") {
            self.xgroup_command(argv)
        } else if ascii_eq(command, b"XREADGROUP") {
            self.xreadgroup_command(argv)
        } else if ascii_eq(command, b"XACK") {
            self.xack_command(argv)
        } else if ascii_eq(command, b"XPENDING") {
            self.xpending_command(argv)
        } else if ascii_eq(command, b"XINFO") {
            self.xinfo_command(argv)
        } else if ascii_eq(command, b"GETRANGE") || ascii_eq(command, b"SUBSTR") {
            self.getrange_command(argv)
        } else if ascii_eq(command, b"SETRANGE") {
            self.setrange_command(argv)
        } else if ascii_eq(command, b"MSET") {
            self.mset_command(argv, false)
        } else if ascii_eq(command, b"MSETNX") {
            self.mset_command(argv, true)
        } else if ascii_eq(command, b"MSETEX") {
            self.msetex_command(argv)
        } else if ascii_eq(command, b"DELIFEQ") {
            self.delifeq_command(argv)
        } else if ascii_eq(command, b"PSETEX") {
            self.psetex_command(argv)
        } else if ascii_eq(command, b"GETEX") {
            self.getex_command(argv)
        } else if ascii_eq(command, b"SETBIT") {
            self.setbit_command(argv)
        } else if ascii_eq(command, b"GETBIT") {
            self.getbit_command(argv)
        } else if ascii_eq(command, b"BITCOUNT") {
            self.bitcount_command(argv)
        } else if ascii_eq(command, b"BITPOS") {
            self.bitpos_command(argv)
        } else if ascii_eq(command, b"BITFIELD") {
            self.bitfield_command(argv, false)
        } else if ascii_eq(command, b"BITFIELD_RO") {
            self.bitfield_command(argv, true)
        } else if ascii_eq(command, b"BITOP") {
            self.bitop_command(argv)
        } else if ascii_eq(command, b"SCRIPT") {
            self.script_command(argv)
        } else if ascii_eq(command, b"EVAL") {
            self.eval_command(argv, false)
        } else if ascii_eq(command, b"EVAL_RO") {
            self.eval_command(argv, true)
        } else if ascii_eq(command, b"EVALSHA") {
            self.evalsha_command(argv, false)
        } else if ascii_eq(command, b"EVALSHA_RO") {
            self.evalsha_command(argv, true)
        } else if ascii_eq(command, b"INCRBYFLOAT") {
            self.incrbyfloat_command(argv)
        } else if ascii_eq(command, b"HINCRBYFLOAT") {
            self.hincrbyfloat_command(argv)
        } else if ascii_eq(command, b"KEYS") {
            self.keys_command(argv)
        } else if ascii_eq(command, b"SCAN") {
            self.scan_command(argv)
        } else if ascii_eq(command, b"HSCAN") {
            self.collection_scan_command(argv, CollectionScan::Hash)
        } else if ascii_eq(command, b"SSCAN") {
            self.collection_scan_command(argv, CollectionScan::Set)
        } else if ascii_eq(command, b"ZSCAN") {
            self.collection_scan_command(argv, CollectionScan::ZSet)
        } else if ascii_eq(command, b"LCS") {
            self.lcs_command(argv)
        } else if ascii_eq(command, b"PFADD") {
            self.pfadd_command(argv)
        } else if ascii_eq(command, b"PFCOUNT") {
            self.pfcount_command(argv)
        } else if ascii_eq(command, b"PFMERGE") {
            self.pfmerge_command(argv)
        } else if ascii_eq(command, b"SORT") {
            self.sort_command(argv, false)
        } else if ascii_eq(command, b"SORT_RO") {
            self.sort_command(argv, true)
        } else if ascii_eq(command, b"GEOADD") {
            self.geoadd_command(argv)
        } else if ascii_eq(command, b"GEOPOS") {
            self.geopos_command(argv)
        } else if ascii_eq(command, b"GEODIST") {
            self.geodist_command(argv)
        } else if ascii_eq(command, b"GEOHASH") {
            self.geohash_command(argv)
        } else if ascii_eq(command, b"GEOSEARCH") {
            self.georadius_generic(argv, 1, GEO_FLAG_SEARCH)
        } else if ascii_eq(command, b"GEOSEARCHSTORE") {
            self.georadius_generic(argv, 2, GEO_FLAG_SEARCH | GEO_FLAG_SEARCHSTORE)
        } else if ascii_eq(command, b"GEORADIUS") {
            self.georadius_generic(argv, 1, GEO_FLAG_COORDS)
        } else if ascii_eq(command, b"GEORADIUS_RO") {
            self.georadius_generic(argv, 1, GEO_FLAG_COORDS | GEO_FLAG_NOSTORE)
        } else if ascii_eq(command, b"GEORADIUSBYMEMBER") {
            self.georadius_generic(argv, 1, GEO_FLAG_MEMBER)
        } else if ascii_eq(command, b"GEORADIUSBYMEMBER_RO") {
            self.georadius_generic(argv, 1, GEO_FLAG_MEMBER | GEO_FLAG_NOSTORE)
        } else if ascii_eq(command, b"DUMP") {
            self.dump_command(argv)
        } else if ascii_eq(command, b"RESTORE") {
            self.restore_command(argv)
        } else {
            unknown_command_error(command, &argv[1..])
        }
    }

    /// `DUMP key` (`dumpCommand`, `cluster.c`). Produces the serialized
    /// representation of the value stored at `key`: a bulk string of
    /// `<type-byte><serialized-value><2-byte RDB version LE><8-byte CRC64 LE>`,
    /// byte-identical to the reference (the differential oracle compares these
    /// bytes). Returns a null bulk when the key is missing. Aggregate value
    /// types are deferred this wave; DUMP of a non-string returns a deferral
    /// error so we never emit a non-faithful payload (see `ku-dump-aggregate`).
    fn dump_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"dump");
        }
        self.purge_if_expired(&argv[1]);
        let value = match self.db.get(&argv[1]) {
            None => return RespFrame::null_bulk(),
            Some(entry) => &entry.value,
        };
        match rdb_create_dump_payload(value) {
            Some(payload) => bulk(&payload),
            None => err(b"ERR DUMP of this value type is not yet supported by the edge engine"),
        }
    }

    /// `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME n]
    /// [FREQ n]` (`restoreCommand`, `cluster.c`). Parses the binary DUMP blob,
    /// verifies the trailing RDB version and CRC64, decodes the value, and
    /// stores it with the requested TTL. Option parsing, check ordering, and
    /// error strings mirror the reference exactly. IDLETIME/FREQ are validated
    /// (range-checked) but otherwise ignored — the edge engine has no
    /// LRU/LFU eviction. String values are restored faithfully; a payload
    /// holding a non-string aggregate type errors with "Bad data format"
    /// (the engine cannot reconstruct aggregate encodings this wave).
    fn restore_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"restore");
        }
        let mut replace = false;
        let mut absttl = false;
        let mut lru_idle: i64 = -1;
        let mut lfu_freq: i64 = -1;
        let mut j = 4;
        while j < argv.len() {
            let additional = argv.len() - j - 1;
            if ascii_eq(&argv[j], b"REPLACE") {
                replace = true;
            } else if ascii_eq(&argv[j], b"ABSTTL") {
                absttl = true;
            } else if ascii_eq(&argv[j], b"IDLETIME") && additional >= 1 && lfu_freq == -1 {
                let Some(v) = parse_i64(&argv[j + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if v < 0 {
                    return err(b"ERR Invalid IDLETIME value, must be >= 0");
                }
                lru_idle = v;
                j += 1;
            } else if ascii_eq(&argv[j], b"FREQ") && additional >= 1 && lru_idle == -1 {
                let Some(v) = parse_i64(&argv[j + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if !(0..=255).contains(&v) {
                    return err(b"ERR Invalid FREQ value, must be >= 0 and <= 255");
                }
                lfu_freq = v;
                j += 1;
            } else {
                return err(b"ERR syntax error");
            }
            j += 1;
        }

        let key = &argv[1];
        self.purge_if_expired(key);
        if !replace && self.db.contains_key(key) {
            return err(b"BUSYKEY Target key name already exists.");
        }

        let Some(ttl) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if ttl < 0 {
            return err(b"ERR Invalid TTL value, must be >= 0");
        }

        let blob = &argv[3];
        if rdb_verify_dump_payload(blob).is_none() {
            return err(b"ERR DUMP payload version or checksum are wrong");
        }

        let value = match rdb_load_dump_value(blob) {
            Some(v) => v,
            None => return err(b"ERR Bad data format"),
        };

        let key = argv[1].clone();
        let expire_at_ms = if ttl == 0 {
            None
        } else if absttl {
            Some(ttl as u64)
        } else {
            match self.host.now_millis().checked_add(ttl as u64) {
                Some(deadline) => Some(deadline),
                None => return err(b"ERR Invalid TTL value, must be >= 0"),
            }
        };

        if let Some(deadline) = expire_at_ms {
            if deadline <= self.host.now_millis() {
                self.db.remove(&key);
                self.note_write(&key);
                return simple(b"OK");
            }
        }

        self.db.insert(
            key.clone(),
            Entry {
                value,
                expire_at_ms,
            },
        );
        self.note_write(&key);
        simple(b"OK")
    }

    fn get_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"get");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => bulk(value),
            Some(_) => wrong_type(),
            None => RespFrame::null_bulk(),
        }
    }

    fn set_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"set");
        }
        let mut expire_at_ms = None;
        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        let mut keepttl = false;
        let mut index = 3;
        while index < argv.len() {
            if ascii_eq(&argv[index], b"NX") {
                if xx {
                    return err(b"ERR syntax error");
                }
                nx = true;
                index += 1;
            } else if ascii_eq(&argv[index], b"XX") {
                if nx {
                    return err(b"ERR syntax error");
                }
                xx = true;
                index += 1;
            } else if ascii_eq(&argv[index], b"GET") {
                get = true;
                index += 1;
            } else if ascii_eq(&argv[index], b"PX") || ascii_eq(&argv[index], b"EX") {
                if expire_at_ms.is_some() || keepttl {
                    return err(b"ERR syntax error");
                }
                if index + 1 >= argv.len() {
                    return err(b"ERR syntax error");
                }
                let Some(raw) = parse_i64(&argv[index + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if raw <= 0 {
                    return invalid_expire_time(b"set");
                }
                let unit = if ascii_eq(&argv[index], b"PX") {
                    1
                } else {
                    1000
                };
                let Some(ttl) = checked_ttl_ms(raw, unit) else {
                    return invalid_expire_time(b"set");
                };
                expire_at_ms = self.host.now_millis().checked_add(ttl);
                if expire_at_ms.is_none() {
                    return invalid_expire_time(b"set");
                }
                index += 2;
            } else if ascii_eq(&argv[index], b"EXAT") || ascii_eq(&argv[index], b"PXAT") {
                if expire_at_ms.is_some() || keepttl {
                    return err(b"ERR syntax error");
                }
                if index + 1 >= argv.len() {
                    return err(b"ERR syntax error");
                }
                let Some(raw) = parse_i64(&argv[index + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if raw <= 0 {
                    return invalid_expire_time(b"set");
                }
                let unit = if ascii_eq(&argv[index], b"PXAT") { 1 } else { 1000 };
                let Some(absolute) = (raw as u64).checked_mul(unit) else {
                    return invalid_expire_time(b"set");
                };
                expire_at_ms = Some(absolute);
                index += 2;
            } else if ascii_eq(&argv[index], b"KEEPTTL") {
                if expire_at_ms.is_some() {
                    return err(b"ERR syntax error");
                }
                keepttl = true;
                index += 1;
            } else {
                return err(b"ERR syntax error");
            }
        }
        self.purge_if_expired(&argv[1]);
        let mut exists = false;
        let mut old_string = None;
        match self.db.get(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => {
                exists = true;
                old_string = Some(value.clone());
            }
            Some(_) => {
                if get {
                    return wrong_type();
                }
                exists = true;
            }
            None => {}
        }
        if (nx && exists) || (xx && !exists) {
            return if get {
                old_string_reply(old_string)
            } else {
                RespFrame::null_bulk()
            };
        }
        if keepttl {
            expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        }
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[2].clone()),
                expire_at_ms,
            },
        );
        self.note_write(&argv[1]);
        if get {
            old_string_reply(old_string)
        } else {
            simple(b"OK")
        }
    }

    fn setex_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"setex");
        }
        let Some(seconds) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if seconds <= 0 {
            return invalid_expire_time(b"setex");
        }
        let Some(ttl) = checked_ttl_ms(seconds, 1000) else {
            return invalid_expire_time(b"setex");
        };
        let Some(expire_at_ms) = self.host.now_millis().checked_add(ttl) else {
            return invalid_expire_time(b"setex");
        };
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[3].clone()),
                expire_at_ms: Some(expire_at_ms),
            },
        );
        self.note_write(&argv[1]);
        simple(b"OK")
    }

    fn del_command(&mut self, argv: &[Vec<u8>], name: &[u8]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(name);
        }
        let mut deleted = 0;
        for key in &argv[1..] {
            self.purge_if_expired(key);
            if self.db.remove(key).is_some() {
                self.note_write(key);
                deleted += 1;
            }
        }
        RespFrame::integer(deleted)
    }

    fn exists_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"exists");
        }
        let mut count = 0;
        for key in &argv[1..] {
            if self.get_value(key).is_some() {
                count += 1;
            }
        }
        RespFrame::integer(count)
    }

    fn incr_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"incr");
        }
        self.apply_increment(&argv[1], 1)
    }

    fn incrby_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"incrby");
        }
        let Some(delta) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        self.apply_increment(&argv[1], delta)
    }

    fn apply_increment(&mut self, key: &[u8], delta: i64) -> RespFrame {
        let current = match self.get_value(key) {
            Some(Entry {
                value: StoredValue::String(value),
                ..
            }) => match parse_i64(value) {
                Some(n) => n,
                None => return err(b"ERR value is not an integer or out of range"),
            },
            Some(_) => return wrong_type(),
            None => 0,
        };
        let Some(next) = current.checked_add(delta) else {
            return err(b"ERR increment or decrement would overflow");
        };
        let expire_at_ms = self.db.get(key).and_then(|entry| entry.expire_at_ms);
        self.db.insert(
            key.to_vec(),
            Entry {
                value: StoredValue::String(next.to_string().into_bytes()),
                expire_at_ms,
            },
        );
        self.note_write(key);
        RespFrame::integer(next)
    }

    fn decr_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"decr");
        }
        self.apply_increment(&argv[1], -1)
    }

    fn decrby_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"decrby");
        }
        let Some(delta) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let Some(negated) = delta.checked_neg() else {
            return err(b"ERR decrement would overflow");
        };
        self.apply_increment(&argv[1], negated)
    }

    /// INCRBYFLOAT key increment (`incrbyfloatCommand`, `t_string.c`). Parses the
    /// stored value and the increment as `long double`-equivalent floats, adds
    /// them, rejects a NaN/Inf result, then stores and replies the value
    /// formatted with `ld2string(LD_STR_HUMAN)` = `%.17Lf` with trailing zeros
    /// (and a trailing `.`) stripped. The stored value must reject Inf/NaN
    /// (string2ld rejects NaN and a stored Inf would already be impossible), the
    /// increment may be Inf (it then trips the result guard). TTL is preserved
    /// (the C path uses a SET … KEEPTTL rewrite that leaves the expiry intact).
    fn incrbyfloat_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"incrbyfloat");
        }
        let incr = match parse_incr_float(&argv[2]) {
            Some(v) => v,
            None => return err(b"ERR value is not a valid float"),
        };
        let current = match self.get_value(&argv[1]) {
            Some(Entry {
                value: StoredValue::String(value),
                ..
            }) => match parse_stored_float(value) {
                Some(v) => v,
                None => return err(b"ERR value is not a valid float"),
            },
            Some(_) => return wrong_type(),
            None => 0.0,
        };
        let next = current + incr;
        if next.is_nan() || next.is_infinite() {
            return err(b"ERR increment would produce NaN or Infinity");
        }
        let formatted = format_human_long_double(next);
        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(formatted.clone()),
                expire_at_ms,
            },
        );
        self.note_write(&argv[1]);
        bulk(formatted)
    }

    fn append_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"append");
        }
        self.purge_if_expired(&argv[1]);
        let new_len = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::String(value),
                ..
            }) => {
                value.extend_from_slice(&argv[2]);
                value.len()
            }
            Some(_) => return wrong_type(),
            None => {
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::String(argv[2].clone()),
                        expire_at_ms: None,
                    },
                );
                argv[2].len()
            }
        };
        self.note_write(&argv[1]);
        RespFrame::integer(new_len as i64)
    }

    fn strlen_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"strlen");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => RespFrame::integer(value.len() as i64),
            Some(_) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    fn setnx_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"setnx");
        }
        self.purge_if_expired(&argv[1]);
        if self.db.contains_key(&argv[1]) {
            return RespFrame::integer(0);
        }
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[2].clone()),
                expire_at_ms: None,
            },
        );
        self.note_write(&argv[1]);
        RespFrame::integer(1)
    }

    fn getset_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"getset");
        }
        self.purge_if_expired(&argv[1]);
        let old = match self.db.get(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => Some(value.clone()),
            Some(_) => return wrong_type(),
            None => None,
        };
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[2].clone()),
                expire_at_ms: None,
            },
        );
        self.note_write(&argv[1]);
        old_string_reply(old)
    }

    fn getdel_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"getdel");
        }
        self.purge_if_expired(&argv[1]);
        match self.db.remove(&argv[1]) {
            Some(Entry {
                value: StoredValue::String(value),
                ..
            }) => {
                self.note_write(&argv[1]);
                bulk(value)
            }
            Some(other) => {
                self.db.insert(argv[1].clone(), other);
                wrong_type()
            }
            None => RespFrame::null_bulk(),
        }
    }

    fn mget_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"mget");
        }
        let mut items = Vec::with_capacity(argv.len() - 1);
        for key in &argv[1..] {
            let item = match self.get_value(key).map(|entry| &entry.value) {
                Some(StoredValue::String(value)) => bulk(value),
                _ => RespFrame::null_bulk(),
            };
            items.push(item);
        }
        RespFrame::array(items)
    }

    /// GETRANGE / SUBSTR key start end (`getrangeCommand`, `t_string.c`).
    /// Both indices are signed byte offsets; negatives count from the end and
    /// the range is clamped to the string. A missing key, an out-of-range
    /// window, or an empty string all reply with an empty bulk string.
    fn getrange_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"getrange");
        }
        let Some(mut start) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let Some(mut end) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let value = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => value.clone(),
            Some(_) => return wrong_type(),
            None => return bulk(b""),
        };
        let strlen = value.len() as i64;
        if start < 0 && end < 0 && start > end {
            return bulk(b"");
        }
        if start < 0 {
            start += strlen;
        }
        if end < 0 {
            end += strlen;
        }
        if start < 0 {
            start = 0;
        }
        if end < 0 {
            end = 0;
        }
        if end >= strlen {
            end = strlen - 1;
        }
        if start > end || strlen == 0 {
            bulk(b"")
        } else {
            bulk(&value[start as usize..=end as usize])
        }
    }

    /// SETRANGE key offset value (`setrangeCommand`, `t_string.c`). Overwrites
    /// (and zero-pads/extends) the string starting at `offset`, replying with
    /// the resulting length. Offset 0 with an empty value on a missing key is a
    /// no-op returning 0; an offset that would exceed the proto byte ceiling is
    /// an error.
    fn setrange_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"setrange");
        }
        let Some(offset) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if offset < 0 {
            return err(b"ERR offset is out of range");
        }
        let value = &argv[3];
        self.purge_if_expired(&argv[1]);
        match self.db.get(&argv[1]).map(|entry| &entry.value) {
            None => {
                if value.is_empty() {
                    return RespFrame::integer(0);
                }
                if let Some(frame) = check_string_length(offset, value.len() as i64) {
                    return frame;
                }
            }
            Some(StoredValue::String(existing)) => {
                let olen = existing.len();
                if value.is_empty() {
                    return RespFrame::integer(olen as i64);
                }
                if let Some(frame) = check_string_length(offset, value.len() as i64) {
                    return frame;
                }
            }
            Some(_) => return wrong_type(),
        }
        let offset = offset as usize;
        let needed = offset + value.len();
        let (mut bytes, preserved_ttl) = match self.db.remove(&argv[1]) {
            Some(Entry {
                value: StoredValue::String(existing),
                expire_at_ms,
            }) => (existing, expire_at_ms),
            _ => (Vec::new(), None),
        };
        if bytes.len() < needed {
            bytes.resize(needed, 0u8);
        }
        bytes[offset..needed].copy_from_slice(value);
        let new_len = bytes.len();
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(bytes),
                expire_at_ms: preserved_ttl,
            },
        );
        self.note_write(&argv[1]);
        RespFrame::integer(new_len as i64)
    }

    /// MSET / MSETNX (`msetGenericCommand`, `t_string.c`). MSET sets every pair
    /// and replies `+OK`; MSETNX is atomic all-or-nothing — it sets nothing and
    /// replies `:0` if any target key already exists, otherwise sets all and
    /// replies `:1`. Both clear any existing TTL on overwrite.
    fn mset_command(&mut self, argv: &[Vec<u8>], nx: bool) -> RespFrame {
        if argv.len() < 3 || argv.len() % 2 != 1 {
            return wrong_arity(if nx { b"msetnx" } else { b"mset" });
        }
        if nx {
            let mut index = 1;
            while index < argv.len() {
                self.purge_if_expired(&argv[index]);
                if self.db.contains_key(&argv[index]) {
                    return RespFrame::integer(0);
                }
                index += 2;
            }
        }
        let mut index = 1;
        while index < argv.len() {
            self.db.insert(
                argv[index].clone(),
                Entry {
                    value: StoredValue::String(argv[index + 1].clone()),
                    expire_at_ms: None,
                },
            );
            self.note_write(&argv[index]);
            index += 2;
        }
        if nx {
            RespFrame::integer(1)
        } else {
            simple(b"OK")
        }
    }

    /// DELIFEQ key value (`delifeqCommand`, `t_string.c`). A Valkey extension:
    /// delete `key` only if it holds a STRING binary-equal to `value`. Replies
    /// `:1` when the key was deleted, `:0` when the key is missing or its value
    /// differs, and WRONGTYPE when the key holds a non-string. `note_write` is
    /// called exactly once, only on the deleting path (mirroring the C
    /// `signalModifiedKey` + `server.dirty++` that run only after the delete).
    fn delifeq_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"delifeq");
        }
        self.purge_if_expired(&argv[1]);
        match self.db.get(&argv[1]).map(|entry| &entry.value) {
            None => RespFrame::integer(0),
            Some(StoredValue::String(value)) => {
                if value.as_slice() != argv[2].as_slice() {
                    return RespFrame::integer(0);
                }
                self.db.remove(&argv[1]);
                self.note_write(&argv[1]);
                RespFrame::integer(1)
            }
            Some(_) => wrong_type(),
        }
    }

    /// MSETEX numkeys key value [key value ...] [NX|XX]
    ///        [EX sec | PX ms | EXAT ts | PXAT ts | KEEPTTL]
    /// (`msetexCommand`, `t_string.c`). Atomically sets `numkeys` key/value
    /// pairs, optionally with a shared expiry and an optional all-or-nothing
    /// NX/XX precondition. Replies `:1` when all keys were set, `:0` when the
    /// NX/XX condition rejected the whole batch. Check order mirrors the C: parse
    /// numkeys, validate the pair count, parse the trailing options, validate the
    /// expiry, evaluate the NX/XX precondition over all keys, then apply.
    fn msetex_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"msetex");
        }
        let Some(numkeys) = parse_i64(&argv[1]) else {
            return err(b"ERR invalid numkeys value or out of range");
        };
        if numkeys < 1 || numkeys > i32::MAX as i64 {
            return err(b"ERR invalid numkeys value or out of range");
        }
        let numkeys = numkeys as usize;
        let args_start = 2 + numkeys * 2;
        if args_start > argv.len() {
            return err(b"ERR syntax error");
        }

        let mut nx = false;
        let mut xx = false;
        let mut keepttl = false;
        let mut expire: Option<MsetexExpire> = None;
        let mut index = args_start;
        while index < argv.len() {
            let opt = &argv[index];
            let next = argv.get(index + 1);
            if ascii_eq(opt, b"NX") && !(xx) {
                nx = true;
                index += 1;
            } else if ascii_eq(opt, b"XX") && !(nx) {
                xx = true;
                index += 1;
            } else if ascii_eq(opt, b"KEEPTTL")
                && !keepttl
                && expire.is_none()
            {
                keepttl = true;
                index += 1;
            } else if ascii_eq(opt, b"EX") && !keepttl && next.is_some() {
                expire = Some(MsetexExpire::Relative {
                    raw: next.unwrap().clone(),
                    unit_ms: 1000,
                });
                index += 2;
            } else if ascii_eq(opt, b"PX") && !keepttl && next.is_some() {
                expire = Some(MsetexExpire::Relative {
                    raw: next.unwrap().clone(),
                    unit_ms: 1,
                });
                index += 2;
            } else if ascii_eq(opt, b"EXAT") && !keepttl && next.is_some() {
                expire = Some(MsetexExpire::Absolute {
                    raw: next.unwrap().clone(),
                    unit_ms: 1000,
                });
                index += 2;
            } else if ascii_eq(opt, b"PXAT") && !keepttl && next.is_some() {
                expire = Some(MsetexExpire::Absolute {
                    raw: next.unwrap().clone(),
                    unit_ms: 1,
                });
                index += 2;
            } else {
                return err(b"ERR syntax error");
            }
        }

        let expire_at_ms = match expire {
            None => None,
            Some(MsetexExpire::Relative { raw, unit_ms }) => {
                let Some(value) = parse_i64(&raw) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if value <= 0 {
                    return invalid_expire_time(b"msetex");
                }
                let Some(ttl) = checked_ttl_ms(value, unit_ms) else {
                    return invalid_expire_time(b"msetex");
                };
                let Some(at) = self.host.now_millis().checked_add(ttl) else {
                    return invalid_expire_time(b"msetex");
                };
                Some(at)
            }
            Some(MsetexExpire::Absolute { raw, unit_ms }) => {
                let Some(value) = parse_i64(&raw) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if value <= 0 {
                    return invalid_expire_time(b"msetex");
                }
                let Some(at) = (value as u64).checked_mul(unit_ms) else {
                    return invalid_expire_time(b"msetex");
                };
                Some(at)
            }
        };

        if nx || xx {
            let mut j = 2;
            while j < args_start {
                self.purge_if_expired(&argv[j]);
                let exists = self.db.contains_key(&argv[j]);
                if (nx && exists) || (xx && !exists) {
                    return RespFrame::integer(0);
                }
                j += 2;
            }
        }

        let mut j = 2;
        while j < args_start {
            let key = &argv[j];
            let value = argv[j + 1].clone();
            let new_expire = if keepttl {
                self.db.get(key).and_then(|entry| entry.expire_at_ms)
            } else {
                expire_at_ms
            };
            self.db.insert(
                key.clone(),
                Entry {
                    value: StoredValue::String(value),
                    expire_at_ms: new_expire,
                },
            );
            self.note_write(key);
            j += 2;
        }
        RespFrame::integer(1)
    }

    /// PSETEX key milliseconds value (`psetexCommand`, `t_string.c`): SETEX with
    /// a millisecond TTL. Mirrors `setex_command`'s validation/error shape but
    /// names the command `psetex` in the invalid-expire-time error.
    fn psetex_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"psetex");
        }
        let Some(millis) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if millis <= 0 {
            return invalid_expire_time(b"psetex");
        }
        let Some(ttl) = checked_ttl_ms(millis, 1) else {
            return invalid_expire_time(b"psetex");
        };
        let Some(expire_at_ms) = self.host.now_millis().checked_add(ttl) else {
            return invalid_expire_time(b"psetex");
        };
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(argv[3].clone()),
                expire_at_ms: Some(expire_at_ms),
            },
        );
        self.note_write(&argv[1]);
        simple(b"OK")
    }

    /// GETEX key [EX s|PX ms|EXAT ts|PXAT ts|PERSIST] (`getexCommand`,
    /// `t_string.c`). Returns the value like GET, but may also set, refresh, or
    /// drop the TTL. With no option the TTL is left untouched. Mutation is
    /// recorded only when the TTL actually changes.
    fn getex_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"getex");
        }
        let mut expire_at_ms: Option<u64> = None;
        let mut persist = false;
        let mut absolute_expire = false;
        let mut already_expired = false;
        let mut index = 2;
        while index < argv.len() {
            if ascii_eq(&argv[index], b"PERSIST") {
                if expire_at_ms.is_some() || persist {
                    return err(b"ERR syntax error");
                }
                persist = true;
                index += 1;
            } else if ascii_eq(&argv[index], b"EX") || ascii_eq(&argv[index], b"PX") {
                if expire_at_ms.is_some() || persist || index + 1 >= argv.len() {
                    return err(b"ERR syntax error");
                }
                let Some(raw) = parse_i64(&argv[index + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                let unit = if ascii_eq(&argv[index], b"PX") { 1 } else { 1000 };
                if raw <= 0 || (unit == 1000 && raw > i64::MAX / 1000) {
                    return invalid_expire_time(b"getex");
                }
                let Some(ttl) = checked_ttl_ms(raw, unit) else {
                    return invalid_expire_time(b"getex");
                };
                let Some(deadline) = self.host.now_millis().checked_add(ttl) else {
                    return invalid_expire_time(b"getex");
                };
                expire_at_ms = Some(deadline);
                index += 2;
            } else if ascii_eq(&argv[index], b"EXAT") || ascii_eq(&argv[index], b"PXAT") {
                if expire_at_ms.is_some() || persist || index + 1 >= argv.len() {
                    return err(b"ERR syntax error");
                }
                let Some(raw) = parse_i64(&argv[index + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                let unit = if ascii_eq(&argv[index], b"PXAT") { 1 } else { 1000 };
                if raw <= 0 || (unit == 1000 && raw > i64::MAX / 1000) {
                    return invalid_expire_time(b"getex");
                }
                let Some(absolute) = (raw as u64).checked_mul(unit) else {
                    return invalid_expire_time(b"getex");
                };
                expire_at_ms = Some(absolute);
                absolute_expire = true;
                already_expired = absolute <= self.host.now_millis();
                index += 2;
            } else {
                return err(b"ERR syntax error");
            }
        }
        self.purge_if_expired(&argv[1]);
        let value = match self.db.get(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => value.clone(),
            Some(_) => return wrong_type(),
            None => return RespFrame::null_bulk(),
        };
        if absolute_expire && already_expired {
            self.db.remove(&argv[1]);
            self.note_write(&argv[1]);
        } else if expire_at_ms.is_some() {
            if let Some(entry) = self.db.get_mut(&argv[1]) {
                entry.expire_at_ms = expire_at_ms;
            }
            self.note_write(&argv[1]);
        } else if persist {
            if let Some(entry) = self.db.get_mut(&argv[1]) {
                if entry.expire_at_ms.is_some() {
                    entry.expire_at_ms = None;
                    self.note_write(&argv[1]);
                }
            }
        }
        bulk(value)
    }

    /// SETBIT key offset bit (`setbitCommand`, `bitops.c`). Sets/clears the bit
    /// at `offset`, growing and zero-padding the string as needed, and replies
    /// with the bit's prior value.
    fn setbit_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"setbit");
        }
        let bitoffset = match get_bit_offset_from_arg(&argv[2]) {
            Ok(offset) => offset,
            Err(frame) => return frame,
        };
        let bit_err = b"ERR bit is not an integer or out of range";
        let Some(on) = parse_i64(&argv[3]) else {
            return err(bit_err);
        };
        if on & !1 != 0 {
            return err(bit_err);
        }
        let on = on != 0;
        let min_len = ((bitoffset >> 3) + 1) as usize;
        self.purge_if_expired(&argv[1]);
        let key_existed = match self.db.get(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(_)) => true,
            Some(_) => return wrong_type(),
            None => false,
        };
        let (mut bytes, preserved_ttl) = match self.db.remove(&argv[1]) {
            Some(Entry {
                value: StoredValue::String(existing),
                expire_at_ms,
            }) => (existing, expire_at_ms),
            _ => (Vec::new(), None),
        };
        let existing_len = bytes.len();
        if bytes.len() < min_len {
            bytes.resize(min_len, 0u8);
        }
        let extended = bytes.len() != existing_len;
        let byte_idx = (bitoffset >> 3) as usize;
        let bit_shift = 7 - (bitoffset & 0x7);
        let byteval = bytes[byte_idx];
        let bitval = (byteval >> bit_shift) & 1 != 0;
        bytes[byte_idx] =
            (byteval & !(1u8 << bit_shift)) | (if on { 1u8 << bit_shift } else { 0 });
        let changed = !key_existed || extended || bitval != on;
        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(bytes),
                expire_at_ms: preserved_ttl,
            },
        );
        if changed {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(if bitval { 1 } else { 0 })
    }

    /// GETBIT key offset (`getbitCommand`, `bitops.c`). Replies with the bit at
    /// `offset`, or 0 when the offset is beyond the string (or the key missing).
    fn getbit_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"getbit");
        }
        let bitoffset = match get_bit_offset_from_arg(&argv[2]) {
            Ok(offset) => offset,
            Err(frame) => return frame,
        };
        let bytes = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => value.clone(),
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        let byte_idx = (bitoffset >> 3) as usize;
        let bit_shift = 7 - (bitoffset & 0x7);
        let bitval = if byte_idx < bytes.len() {
            (bytes[byte_idx] >> bit_shift) & 1
        } else {
            0
        };
        RespFrame::integer(bitval as i64)
    }

    /// BITCOUNT key [start end [BYTE|BIT]] (`bitcountCommand`, `bitops.c`).
    /// Counts set bits over the whole string or an optional (negative-aware,
    /// byte- or bit-granular) range.
    fn bitcount_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        let argc = argv.len();
        if argc == 2 {
            return match self.get_value(&argv[1]).map(|entry| &entry.value) {
                Some(StoredValue::String(value)) => {
                    RespFrame::integer(server_popcount(value))
                }
                Some(_) => wrong_type(),
                None => RespFrame::integer(0),
            };
        }
        if argc != 3 && argc != 4 && argc != 5 {
            return err(b"ERR syntax error");
        }
        let Some(mut start) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let mut isbit = false;
        if argc == 5 {
            if ascii_eq(&argv[4], b"bit") {
                isbit = true;
            } else if ascii_eq(&argv[4], b"byte") {
                isbit = false;
            } else {
                return err(b"ERR syntax error");
            }
        }
        let mut end = 0i64;
        if argc >= 4 {
            let Some(parsed_end) = parse_i64(&argv[3]) else {
                return err(b"ERR value is not an integer or out of range");
            };
            end = parsed_end;
        }
        let bytes = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => value.clone(),
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        let strlen = bytes.len() as i64;
        let mut totlen = strlen;
        if argc < 4 {
            end = totlen - 1;
        }
        if start < 0 && end < 0 && start > end {
            return RespFrame::integer(0);
        }
        if isbit {
            totlen <<= 3;
        }
        if start < 0 {
            start += totlen;
        }
        if end < 0 {
            end += totlen;
        }
        if start < 0 {
            start = 0;
        }
        if end < 0 {
            end = 0;
        }
        if end >= totlen {
            end = totlen - 1;
        }
        let mut first_byte_neg_mask: u8 = 0;
        let mut last_byte_neg_mask: u8 = 0;
        if isbit && start <= end {
            first_byte_neg_mask = (!((1u32 << (8 - (start & 7))) - 1) & 0xFF) as u8;
            last_byte_neg_mask = ((1u32 << (7 - (end & 7))) - 1) as u8;
            start >>= 3;
            end >>= 3;
        }
        if start > end {
            return RespFrame::integer(0);
        }
        let byte_start = start as usize;
        let byte_end = end as usize;
        let mut count = server_popcount(&bytes[byte_start..=byte_end]);
        if first_byte_neg_mask != 0 || last_byte_neg_mask != 0 {
            let edge_bytes = [
                if first_byte_neg_mask != 0 {
                    bytes[byte_start] & first_byte_neg_mask
                } else {
                    0
                },
                if last_byte_neg_mask != 0 {
                    bytes[byte_end] & last_byte_neg_mask
                } else {
                    0
                },
            ];
            count -= server_popcount(&edge_bytes);
        }
        RespFrame::integer(count)
    }

    /// BITPOS key bit [start [end [BYTE|BIT]]] (`bitposCommand`, `bitops.c`).
    /// Finds the first set/clear bit, honoring Valkey's exact not-found rules
    /// (notably the difference between searching for a 0 with vs. without an
    /// explicit end on an all-ones string).
    fn bitpos_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        let argc = argv.len();
        if !(3..=6).contains(&argc) {
            return err(b"ERR syntax error");
        }
        let Some(bit) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if bit != 0 && bit != 1 {
            return err(b"ERR The bit argument must be 1 or 0.");
        }
        let bit = bit as i32;
        let mut start: i64 = 0;
        let mut end_opt: Option<i64> = None;
        let mut end_given = false;
        let mut isbit = false;
        if argc >= 4 {
            let Some(parsed_start) = parse_i64(&argv[3]) else {
                return err(b"ERR value is not an integer or out of range");
            };
            start = parsed_start;
            if argc >= 5 {
                if argc == 6 {
                    if ascii_eq(&argv[5], b"bit") {
                        isbit = true;
                    } else if ascii_eq(&argv[5], b"byte") {
                        isbit = false;
                    } else {
                        return err(b"ERR syntax error");
                    }
                }
                let Some(parsed_end) = parse_i64(&argv[4]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                end_opt = Some(parsed_end);
                end_given = true;
            }
        }
        let bytes = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => value.clone(),
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(if bit == 1 { -1 } else { 0 }),
        };
        let strlen = bytes.len() as i64;
        let mut end = end_opt.unwrap_or(strlen - 1);
        let mut totlen = strlen;
        if isbit {
            totlen <<= 3;
        }
        if start < 0 {
            start += totlen;
        }
        if end < 0 {
            end += totlen;
        }
        if start < 0 {
            start = 0;
        }
        if end < 0 {
            end = 0;
        }
        if end >= totlen {
            end = totlen - 1;
        }
        let mut first_byte_neg_mask: u8 = 0;
        let mut last_byte_neg_mask: u8 = 0;
        if isbit && start <= end {
            first_byte_neg_mask = (!((1u32 << (8 - (start & 7))) - 1) & 0xFF) as u8;
            last_byte_neg_mask = ((1u32 << (7 - (end & 7))) - 1) as u8;
            start >>= 3;
            end >>= 3;
        }
        if start > end {
            return RespFrame::integer(-1);
        }
        let p = &bytes;
        let mut search_start = start;
        let mut nbytes = end - start + 1;
        let pos: i64 = 'find_pos: {
            if first_byte_neg_mask != 0 {
                let mut tmpchar = if bit == 1 {
                    p[search_start as usize] & !first_byte_neg_mask
                } else {
                    p[search_start as usize] | first_byte_neg_mask
                };
                if last_byte_neg_mask != 0 && nbytes == 1 {
                    tmpchar = if bit == 1 {
                        tmpchar & !last_byte_neg_mask
                    } else {
                        tmpchar | last_byte_neg_mask
                    };
                }
                let pos = server_bitpos(&[tmpchar], 1, bit);
                if nbytes == 1 || (pos != -1 && pos != 8) {
                    break 'find_pos pos;
                }
                search_start += 1;
                nbytes -= 1;
            }
            let curbytes = nbytes - if last_byte_neg_mask != 0 { 1 } else { 0 };
            if curbytes > 0 {
                let slice = &p[search_start as usize..(search_start + curbytes) as usize];
                let pos = server_bitpos(slice, curbytes as usize, bit);
                if nbytes == curbytes || (pos != -1 && pos != curbytes << 3) {
                    break 'find_pos pos;
                }
                search_start += curbytes;
                nbytes -= curbytes;
            }
            let tmpchar = if bit == 1 {
                p[end as usize] & !last_byte_neg_mask
            } else {
                p[end as usize] | last_byte_neg_mask
            };
            server_bitpos(&[tmpchar], 1, bit)
        };
        if end_given && bit == 0 && pos != -1 && pos == nbytes << 3 {
            return RespFrame::integer(-1);
        }
        let final_pos = if pos != -1 {
            pos + (search_start << 3)
        } else {
            -1
        };
        RespFrame::integer(final_pos)
    }

    /// BITFIELD / BITFIELD_RO (`bitfieldGeneric`, `bitops.c`). Parses every
    /// GET/SET/INCRBY/OVERFLOW subcommand in a first pass (all argument errors
    /// surface before any write), then executes them in order against a single
    /// zero-extended string buffer, replying with one array element per op
    /// (SET returns the old value, INCRBY the new value, FAIL-overflow nil).
    /// When `readonly` is true only GET is accepted.
    fn bitfield_command(&mut self, argv: &[Vec<u8>], readonly: bool) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(if readonly { b"bitfield_ro" } else { b"bitfield" });
        }
        let mut ops: Vec<BitfieldOp> = Vec::new();
        let mut owtype = BfOverflow::Wrap;
        let mut is_readonly_access = true;
        let mut highest_write_offset: u64 = 0;
        let mut j = 2;
        while j < argv.len() {
            let remargs = argv.len() - j - 1;
            let subcmd = &argv[j];
            let opcode = if ascii_eq(subcmd, b"get") && remargs >= 2 {
                BitfieldOpcode::Get
            } else if ascii_eq(subcmd, b"set") && remargs >= 3 {
                BitfieldOpcode::Set
            } else if ascii_eq(subcmd, b"incrby") && remargs >= 3 {
                BitfieldOpcode::IncrBy
            } else if ascii_eq(subcmd, b"overflow") && remargs >= 1 {
                let owtypename = &argv[j + 1];
                j += 1;
                if ascii_eq(owtypename, b"wrap") {
                    owtype = BfOverflow::Wrap;
                } else if ascii_eq(owtypename, b"sat") {
                    owtype = BfOverflow::Sat;
                } else if ascii_eq(owtypename, b"fail") {
                    owtype = BfOverflow::Fail;
                } else {
                    return err(b"ERR Invalid OVERFLOW type specified");
                }
                j += 1;
                continue;
            } else {
                return err(b"ERR syntax error");
            };

            let (sign, bits) = match get_bitfield_type_from_argument(&argv[j + 1]) {
                Ok(parsed) => (parsed.0, parsed.1),
                Err(frame) => return frame,
            };
            let bitoffset = match get_bitfield_offset_from_argument(&argv[j + 2], bits) {
                Ok(offset) => offset,
                Err(frame) => return frame,
            };

            let mut i64val = 0i64;
            if opcode != BitfieldOpcode::Get {
                is_readonly_access = false;
                if highest_write_offset < bitoffset + bits as u64 - 1 {
                    highest_write_offset = bitoffset + bits as u64 - 1;
                }
                let Some(parsed) = parse_i64(&argv[j + 3]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                i64val = parsed;
            }

            ops.push(BitfieldOp {
                offset: bitoffset,
                i64: i64val,
                opcode,
                owtype,
                bits,
                sign,
            });

            j += 4 - usize::from(opcode == BitfieldOpcode::Get);
        }

        let mut buffer: Vec<u8>;
        let mut preserved_ttl: Option<u64> = None;
        let mut dirty = false;

        if is_readonly_access {
            self.purge_if_expired(&argv[1]);
            buffer = match self.db.get(&argv[1]).map(|entry| &entry.value) {
                Some(StoredValue::String(value)) => value.clone(),
                Some(_) => return wrong_type(),
                None => Vec::new(),
            };
        } else {
            if readonly {
                return err(b"ERR BITFIELD_RO only supports the GET subcommand");
            }
            self.purge_if_expired(&argv[1]);
            let byte = (highest_write_offset >> 3) as usize;
            let min_len = byte + 1;
            match self.db.remove(&argv[1]) {
                Some(Entry {
                    value: StoredValue::String(existing),
                    expire_at_ms,
                }) => {
                    buffer = existing;
                    preserved_ttl = expire_at_ms;
                }
                Some(other) => {
                    self.db.insert(argv[1].clone(), other);
                    return wrong_type();
                }
                None => {
                    buffer = Vec::new();
                    dirty = true;
                }
            }
            if buffer.len() < min_len {
                buffer.resize(min_len, 0u8);
                dirty = true;
            }
        }

        let mut changes = 0i64;
        let mut replies: Vec<RespFrame> = Vec::with_capacity(ops.len());
        for op in &ops {
            match op.opcode {
                BitfieldOpcode::Set | BitfieldOpcode::IncrBy => {
                    if op.sign {
                        let oldval = get_signed_bitfield(&buffer, op.offset, op.bits);
                        let (overflow, wrapped, newval, retval) =
                            if op.opcode == BitfieldOpcode::IncrBy {
                                let (overflow, wrapped) = check_signed_bitfield_overflow(
                                    oldval, op.i64, op.bits, op.owtype,
                                );
                                let newval = if overflow {
                                    wrapped
                                } else {
                                    oldval.wrapping_add(op.i64)
                                };
                                (overflow, wrapped, newval, newval)
                            } else {
                                let mut newval = op.i64;
                                let (overflow, wrapped) = check_signed_bitfield_overflow(
                                    newval, 0, op.bits, op.owtype,
                                );
                                if overflow {
                                    newval = wrapped;
                                }
                                (overflow, wrapped, newval, oldval)
                            };
                        let _ = wrapped;
                        if !(overflow && op.owtype == BfOverflow::Fail) {
                            replies.push(RespFrame::integer(retval));
                            set_signed_bitfield(&mut buffer, op.offset, op.bits, newval);
                            if dirty || oldval != newval {
                                changes += 1;
                            }
                        } else {
                            replies.push(RespFrame::null_bulk());
                        }
                    } else {
                        let oldval = get_unsigned_bitfield(&buffer, op.offset, op.bits);
                        let (overflow, newval, retval) =
                            if op.opcode == BitfieldOpcode::IncrBy {
                                let mut newval = oldval.wrapping_add(op.i64 as u64);
                                let (overflow, wrapped) = check_unsigned_bitfield_overflow(
                                    oldval, op.i64, op.bits, op.owtype,
                                );
                                if overflow {
                                    newval = wrapped;
                                }
                                (overflow, newval, newval)
                            } else {
                                let mut newval = op.i64 as u64;
                                let (overflow, wrapped) = check_unsigned_bitfield_overflow(
                                    newval, 0, op.bits, op.owtype,
                                );
                                if overflow {
                                    newval = wrapped;
                                }
                                (overflow, newval, oldval)
                            };
                        if !(overflow && op.owtype == BfOverflow::Fail) {
                            replies.push(RespFrame::integer(retval as i64));
                            set_unsigned_bitfield(&mut buffer, op.offset, op.bits, newval);
                            if dirty || oldval != newval {
                                changes += 1;
                            }
                        } else {
                            replies.push(RespFrame::null_bulk());
                        }
                    }
                }
                BitfieldOpcode::Get => {
                    let mut buf = [0u8; 9];
                    let byte = (op.offset >> 3) as usize;
                    for (i, slot) in buf.iter_mut().enumerate() {
                        if i + byte >= buffer.len() {
                            break;
                        }
                        *slot = buffer[i + byte];
                    }
                    let local_offset = op.offset - (byte as u64) * 8;
                    if op.sign {
                        let val = get_signed_bitfield(&buf, local_offset, op.bits);
                        replies.push(RespFrame::integer(val));
                    } else {
                        let val = get_unsigned_bitfield(&buf, local_offset, op.bits);
                        replies.push(RespFrame::integer(val as i64));
                    }
                }
            }
        }

        if !is_readonly_access {
            self.db.insert(
                argv[1].clone(),
                Entry {
                    value: StoredValue::String(buffer),
                    expire_at_ms: preserved_ttl,
                },
            );
            if changes > 0 {
                self.note_write(&argv[1]);
            }
        }

        RespFrame::array(replies)
    }

    /// BITOP AND|OR|XOR|NOT destkey srckey [srckey...] (`bitopCommand`,
    /// `bitops.c`). Combines source strings (zero-extended to the longest)
    /// bitwise into destkey and replies with the result's byte length. An
    /// all-empty result deletes destkey. NOT accepts exactly one source.
    fn bitop_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"bitop");
        }
        let op = if ascii_eq(&argv[1], b"and") {
            BitopKind::And
        } else if ascii_eq(&argv[1], b"or") {
            BitopKind::Or
        } else if ascii_eq(&argv[1], b"xor") {
            BitopKind::Xor
        } else if ascii_eq(&argv[1], b"not") {
            BitopKind::Not
        } else {
            return err(b"ERR syntax error");
        };
        if op == BitopKind::Not && argv.len() != 4 {
            return err(b"ERR BITOP NOT must be called with a single source key.");
        }

        let targetkey = &argv[2];
        let numkeys = argv.len() - 3;
        let mut srcs: Vec<Vec<u8>> = Vec::with_capacity(numkeys);
        for src_key in &argv[3..] {
            self.purge_if_expired(src_key);
            match self.db.get(src_key).map(|entry| &entry.value) {
                Some(StoredValue::String(value)) => srcs.push(value.clone()),
                Some(_) => return wrong_type(),
                None => srcs.push(Vec::new()),
            }
        }

        let maxlen = srcs.iter().map(|s| s.len()).max().unwrap_or(0);
        let mut res = Vec::new();
        if maxlen > 0 {
            res = vec![0u8; maxlen];
            for (idx, out) in res.iter_mut().enumerate() {
                let mut output = if srcs[0].len() <= idx { 0 } else { srcs[0][idx] };
                if op == BitopKind::Not {
                    output = !output;
                }
                for src in &srcs[1..] {
                    let byte = if src.len() <= idx { 0 } else { src[idx] };
                    match op {
                        BitopKind::And => {
                            output &= byte;
                            if output == 0 {
                                break;
                            }
                        }
                        BitopKind::Or => {
                            output |= byte;
                            if output == 0xff {
                                break;
                            }
                        }
                        BitopKind::Xor => output ^= byte,
                        BitopKind::Not => {}
                    }
                }
                *out = output;
            }
        }

        if maxlen > 0 {
            let res_len = res.len() as i64;
            let preserved_ttl = None;
            self.db.insert(
                targetkey.clone(),
                Entry {
                    value: StoredValue::String(res),
                    expire_at_ms: preserved_ttl,
                },
            );
            self.note_write(targetkey);
            RespFrame::integer(res_len)
        } else {
            if self.db.remove(targetkey).is_some() {
                self.note_write(targetkey);
            }
            RespFrame::integer(0)
        }
    }

    fn hset_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 || argv.len() % 2 != 0 {
            return wrong_arity(b"hset");
        }
        self.purge_expired_fields(&argv[1]);

        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                let mut added = 0;
                for pair in argv[2..].chunks_exact(2) {
                    if fields
                        .insert(pair[0].clone(), HashField::new(pair[1].clone()))
                        .is_none()
                    {
                        added += 1;
                    }
                }
                self.note_write(&argv[1]);
                RespFrame::integer(added)
            }
            Some(Entry {
                value: StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_),
                ..
            }) => wrong_type(),
            None => {
                let mut fields = IndexMap::new();
                let mut added = 0;
                for pair in argv[2..].chunks_exact(2) {
                    if fields
                        .insert(pair[0].clone(), HashField::new(pair[1].clone()))
                        .is_none()
                    {
                        added += 1;
                    }
                }
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Hash(fields),
                        expire_at_ms,
                    },
                );
                self.note_write(&argv[1]);
                RespFrame::integer(added)
            }
        }
    }

    fn hget_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"hget");
        }
        self.purge_expired_fields(&argv[1]);
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => match fields.get(&argv[2]) {
                Some(field) => bulk(&field.value),
                None => RespFrame::null_bulk(),
            },
            Some(StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_)) => {
                wrong_type()
            }
            None => RespFrame::null_bulk(),
        }
    }

    fn hgetall_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"hgetall");
        }
        self.purge_expired_fields(&argv[1]);
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                // Insertion order, matching valkey's listpack/hashtable iteration
                // (`hashTypeGetAll` → field, value pairs in stored order).
                let mut items = Vec::with_capacity(fields.len() * 2);
                for (field, field_value) in fields {
                    items.push(bulk(field));
                    items.push(bulk(&field_value.value));
                }
                RespFrame::array(items)
            }
            Some(StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_)) => {
                wrong_type()
            }
            None => RespFrame::array(Vec::new()),
        }
    }

    fn hdel_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"hdel");
        }
        self.purge_expired_fields(&argv[1]);
        let mut remove_empty_hash = false;
        let mut mutated = false;
        let response = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                let mut deleted = 0;
                for field in &argv[2..] {
                    if fields.shift_remove(field).is_some() {
                        deleted += 1;
                    }
                }
                remove_empty_hash = fields.is_empty();
                mutated = deleted > 0;
                RespFrame::integer(deleted)
            }
            Some(Entry {
                value: StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_),
                ..
            }) => wrong_type(),
            None => RespFrame::integer(0),
        };
        if remove_empty_hash {
            self.db.remove(&argv[1]);
        }
        if mutated {
            self.note_write(&argv[1]);
        }
        response
    }

    fn hexists_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"hexists");
        }
        self.purge_expired_fields(&argv[1]);
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                RespFrame::integer(if fields.contains_key(&argv[2]) { 1 } else { 0 })
            }
            Some(StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_)) => {
                wrong_type()
            }
            None => RespFrame::integer(0),
        }
    }

    fn hlen_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"hlen");
        }
        self.purge_expired_fields(&argv[1]);
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => RespFrame::integer(fields.len() as i64),
            Some(StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_)) => {
                wrong_type()
            }
            None => RespFrame::integer(0),
        }
    }

    fn hmget_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"hmget");
        }
        self.purge_expired_fields(&argv[1]);
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                let mut items = Vec::with_capacity(argv.len() - 2);
                for field in &argv[2..] {
                    let item = match fields.get(field) {
                        Some(field_value) => bulk(&field_value.value),
                        None => RespFrame::null_bulk(),
                    };
                    items.push(item);
                }
                RespFrame::array(items)
            }
            Some(StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_)) => {
                wrong_type()
            }
            None => {
                let items = (2..argv.len()).map(|_| RespFrame::null_bulk()).collect();
                RespFrame::array(items)
            }
        }
    }

    fn hkeys_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"hkeys");
        }
        self.purge_expired_fields(&argv[1]);
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                // Insertion order (`hashTypeGetAll` with HASH_KEY).
                RespFrame::array(fields.keys().map(|field| bulk(field)).collect())
            }
            Some(StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_)) => {
                wrong_type()
            }
            None => RespFrame::array(Vec::new()),
        }
    }

    fn hvals_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"hvals");
        }
        self.purge_expired_fields(&argv[1]);
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => {
                // Insertion order (`hashTypeGetAll` with HASH_VALUE).
                RespFrame::array(
                    fields
                        .values()
                        .map(|field_value| bulk(&field_value.value))
                        .collect(),
                )
            }
            Some(StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_)) => {
                wrong_type()
            }
            None => RespFrame::array(Vec::new()),
        }
    }

    fn hstrlen_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"hstrlen");
        }
        self.purge_expired_fields(&argv[1]);
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => match fields.get(&argv[2]) {
                Some(field) => RespFrame::integer(field.value.len() as i64),
                None => RespFrame::integer(0),
            },
            Some(StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_)) => {
                wrong_type()
            }
            None => RespFrame::integer(0),
        }
    }

    fn hsetnx_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"hsetnx");
        }
        self.purge_expired_fields(&argv[1]);
        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                if fields.contains_key(&argv[2]) {
                    return RespFrame::integer(0);
                }
                fields.insert(argv[2].clone(), HashField::new(argv[3].clone()));
                self.note_write(&argv[1]);
                RespFrame::integer(1)
            }
            Some(Entry {
                value: StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_),
                ..
            }) => wrong_type(),
            None => {
                let mut fields = IndexMap::new();
                fields.insert(argv[2].clone(), HashField::new(argv[3].clone()));
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Hash(fields),
                        expire_at_ms,
                    },
                );
                self.note_write(&argv[1]);
                RespFrame::integer(1)
            }
        }
    }

    fn hincrby_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"hincrby");
        }
        let Some(incr) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        self.purge_expired_fields(&argv[1]);
        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        let fields = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => fields,
            Some(Entry {
                value: StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_),
                ..
            }) => return wrong_type(),
            None => {
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Hash(IndexMap::new()),
                        expire_at_ms,
                    },
                );
                match &mut self.db.get_mut(&argv[1]).expect("just inserted").value {
                    StoredValue::Hash(fields) => fields,
                    _ => unreachable!(),
                }
            }
        };
        // HINCRBY preserves an existing field TTL (it does not reset it),
        // mirroring `hashTypeSet(..., HASH_SET_KEEP_EXPIRY)` for the increment
        // path in the reference.
        let (current, field_ttl) = match fields.get(&argv[2]) {
            Some(field) => match parse_i64(&field.value) {
                Some(n) => (n, field.expire_at_ms),
                None => return err(b"ERR hash value is not an integer"),
            },
            None => (0, None),
        };
        let Some(next) = current.checked_add(incr) else {
            return err(b"ERR increment or decrement would overflow");
        };
        fields.insert(
            argv[2].clone(),
            HashField {
                value: next.to_string().into_bytes(),
                expire_at_ms: field_ttl,
            },
        );
        self.note_write(&argv[1]);
        RespFrame::integer(next)
    }

    /// HINCRBYFLOAT key field increment (`hincrbyfloatCommand`, `t_hash.c`).
    /// Parses the increment first; an Inf/NaN increment is rejected up front with
    /// "value is NaN or Infinity" (note: distinct from INCRBYFLOAT, which lets
    /// Inf reach the result guard). The current field value (0 when absent)
    /// parses as a float — a bad field value yields "hash value is not a float".
    /// The summed result is rejected if NaN/Inf, otherwise stored and replied
    /// with the same `%.17Lf` human formatting as INCRBYFLOAT.
    fn hincrbyfloat_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"hincrbyfloat");
        }
        let incr = match parse_incr_float(&argv[3]) {
            Some(v) => v,
            None => return err(b"ERR value is not a valid float"),
        };
        if incr.is_nan() || incr.is_infinite() {
            return err(b"ERR value is NaN or Infinity");
        }
        self.purge_expired_fields(&argv[1]);
        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        let fields = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => fields,
            Some(_) => return wrong_type(),
            None => {
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Hash(IndexMap::new()),
                        expire_at_ms,
                    },
                );
                match &mut self.db.get_mut(&argv[1]).expect("just inserted").value {
                    StoredValue::Hash(fields) => fields,
                    _ => unreachable!(),
                }
            }
        };
        // Like HINCRBY, HINCRBYFLOAT preserves an existing field TTL.
        let (current, field_ttl) = match fields.get(&argv[2]) {
            Some(field) => match parse_stored_float(&field.value) {
                Some(v) => (v, field.expire_at_ms),
                None => return err(b"ERR hash value is not a float"),
            },
            None => (0.0, None),
        };
        let next = current + incr;
        if next.is_nan() || next.is_infinite() {
            return err(b"ERR increment would produce NaN or Infinity");
        }
        let formatted = format_human_long_double(next);
        fields.insert(
            argv[2].clone(),
            HashField {
                value: formatted.clone(),
                expire_at_ms: field_ttl,
            },
        );
        self.note_write(&argv[1]);
        bulk(formatted)
    }

    fn hmset_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 || argv.len() % 2 != 0 {
            return wrong_arity(b"hmset");
        }
        self.purge_expired_fields(&argv[1]);
        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                for pair in argv[2..].chunks_exact(2) {
                    fields.insert(pair[0].clone(), HashField::new(pair[1].clone()));
                }
            }
            Some(Entry {
                value: StoredValue::String(_) | StoredValue::ZSet(_) | StoredValue::List(_) | StoredValue::Set(_) | StoredValue::Stream(_),
                ..
            }) => return wrong_type(),
            None => {
                let mut fields = IndexMap::new();
                for pair in argv[2..].chunks_exact(2) {
                    fields.insert(pair[0].clone(), HashField::new(pair[1].clone()));
                }
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Hash(fields),
                        expire_at_ms,
                    },
                );
            }
        }
        self.note_write(&argv[1]);
        simple(b"OK")
    }

    /// HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT
    /// (`hexpireGenericCommand`, `t_hash.c`). Grammar:
    /// `HEXPIRE key time [NX|XX|GT|LT] FIELDS numfields field [field...]`.
    /// `unit` selects seconds vs milliseconds for the time argument; `absolute`
    /// distinguishes the *AT variants (the time is already an absolute Unix
    /// deadline rather than a relative TTL). Replies an array of per-field
    /// status codes: `1` set, `0` condition (NX/XX/GT/LT) not met, `2` field
    /// immediately expired and deleted (time in the past), `-2` no such field
    /// (or the key/hash does not exist).
    fn hexpire_command(
        &mut self,
        argv: &[Vec<u8>],
        unit: ExpireUnit,
        absolute: bool,
    ) -> RespFrame {
        let name: &[u8] = match (unit, absolute) {
            (ExpireUnit::Seconds, false) => b"hexpire",
            (ExpireUnit::Milliseconds, false) => b"hpexpire",
            (ExpireUnit::Seconds, true) => b"hexpireat",
            (ExpireUnit::Milliseconds, true) => b"hpexpireat",
        };
        if argv.len() < 6 {
            return wrong_arity(name);
        }

        // Scan for the FIELDS keyword: any NX/XX/GT/LT flags lie between the
        // time argument (argv[2]) and FIELDS, mirroring the C scan loop.
        let mut fields_kw: Option<usize> = None;
        for index in 3..argv.len() - 1 {
            if ascii_eq(&argv[index], b"FIELDS") {
                fields_kw = Some(index);
                break;
            }
        }
        let Some(fields_kw) = fields_kw else {
            return err(b"ERR Mandatory keyword FIELDS is missing or not at the right position");
        };
        let condition = match parse_expire_condition(&argv[3..fields_kw]) {
            Ok(condition) => condition,
            Err(frame) => return frame,
        };
        if fields_kw + 1 >= argv.len() {
            return err(
                b"ERR numfields should be greater than 0 and match the provided number of fields",
            );
        }
        let Some(num_fields) = parse_i64(&argv[fields_kw + 1]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let provided = (argv.len() - (fields_kw + 2)) as i64;
        if num_fields <= 0 || num_fields != provided {
            return err(
                b"ERR numfields should be greater than 0 and match the provided number of fields",
            );
        }
        let field_args = &argv[fields_kw + 2..];

        // Convert the time argument to an absolute millisecond deadline,
        // mirroring `convertExpireArgumentToUnixTime`. A negative TTL is the
        // "invalid expire time" error; the *AT variants take the value as-is.
        let Some(mut when) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if when < 0 {
            return invalid_expire_time(name);
        }
        if unit == ExpireUnit::Seconds {
            if when > i64::MAX / 1000 {
                return invalid_expire_time(name);
            }
            when *= 1000;
        }
        let basetime: i64 = if absolute {
            0
        } else {
            self.host.now_millis() as i64
        };
        if when > i64::MAX - basetime {
            return invalid_expire_time(name);
        }
        when += basetime;

        self.purge_expired_fields(&argv[1]);
        let now = self.host.now_millis() as i64;
        let time_is_expired = when <= now;

        let fields = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => fields,
            Some(_) => return wrong_type(),
            None => {
                // No key: -2 for every requested field.
                let items = field_args.iter().map(|_| RespFrame::integer(-2)).collect();
                return RespFrame::array(items);
            }
        };

        let mut items = Vec::with_capacity(field_args.len());
        let mut changed = false;
        for field in field_args {
            let result = match fields.get_mut(field) {
                None => -2,
                Some(stored) => {
                    let current = stored.expire_at_ms;
                    let blocked = condition.is_some_and(|condition| match condition {
                        ExpireCondition::Nx => current.is_some(),
                        ExpireCondition::Xx => current.is_none(),
                        ExpireCondition::Gt => match current {
                            None => true,
                            Some(value) => (when as u64) <= value,
                        },
                        ExpireCondition::Lt => match current {
                            None => false,
                            Some(value) => (when as u64) >= value,
                        },
                    });
                    if blocked {
                        0
                    } else if time_is_expired {
                        fields.shift_remove(field);
                        changed = true;
                        2
                    } else {
                        stored.expire_at_ms = Some(when as u64);
                        changed = true;
                        1
                    }
                }
            };
            items.push(RespFrame::integer(result));
        }

        if fields.is_empty() {
            self.db.remove(&argv[1]);
        }
        if changed {
            self.note_write(&argv[1]);
        }
        RespFrame::array(items)
    }

    /// HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME
    /// (`httlGenericCommand`, `t_hash.c`). Grammar:
    /// `HTTL key FIELDS numfields field [field...]` — note these variants do
    /// NOT take condition flags, so `argv[2]` is the literal FIELDS token,
    /// `argv[3]` is numfields, and the C code does not actually require the
    /// token to read "FIELDS". `milliseconds` selects ms vs second output;
    /// `absolute` returns the absolute deadline instead of the remaining TTL.
    /// Per field: `-2` no such field/key, `-1` field has no TTL, else the
    /// TTL/timestamp. A numfields mismatch is a plain syntax error here.
    fn httl_command(
        &mut self,
        argv: &[Vec<u8>],
        milliseconds: bool,
        absolute: bool,
    ) -> RespFrame {
        let name: &[u8] = match (milliseconds, absolute) {
            (false, false) => b"httl",
            (true, false) => b"hpttl",
            (false, true) => b"hexpiretime",
            (true, true) => b"hpexpiretime",
        };
        if argv.len() < 5 {
            return wrong_arity(name);
        }
        let Some(num_fields) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let provided = (argv.len() - 4) as i64;
        if num_fields <= 0 || num_fields != provided {
            return err(b"ERR syntax error");
        }
        let field_args = &argv[4..];

        self.purge_expired_fields(&argv[1]);
        let now = self.host.now_millis();
        let fields = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Hash(fields)) => Some(fields),
            Some(_) => return wrong_type(),
            None => None,
        };

        let mut items = Vec::with_capacity(field_args.len());
        for field in field_args {
            let result = match fields.and_then(|fields| fields.get(field)) {
                None => -2,
                Some(stored) => match stored.expire_at_ms {
                    None => -1,
                    Some(deadline) => {
                        let value = if absolute {
                            deadline as i64
                        } else {
                            let mut remaining = deadline as i64 - now as i64;
                            if remaining < 0 {
                                remaining = 0;
                            }
                            remaining
                        };
                        if milliseconds {
                            value
                        } else {
                            (value + 500) / 1000
                        }
                    }
                },
            };
            items.push(RespFrame::integer(result));
        }
        RespFrame::array(items)
    }

    /// HPERSIST (`hpersistCommand`, `t_hash.c`). Grammar:
    /// `HPERSIST key FIELDS numfields field [field...]`. Like HTTL it does not
    /// take condition flags and uses argv[3] as numfields. Per field: `1` TTL
    /// removed, `-1` field exists but has no TTL, `-2` no such field/key. A
    /// numfields mismatch is the "numfields should be..." error.
    fn hpersist_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 5 {
            return wrong_arity(b"hpersist");
        }
        let Some(num_fields) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let provided = (argv.len() - 4) as i64;
        if num_fields <= 0 || num_fields != provided {
            return err(
                b"ERR numfields should be greater than 0 and match the provided number of fields",
            );
        }
        let field_args = &argv[4..];

        self.purge_expired_fields(&argv[1]);
        let fields = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => fields,
            Some(_) => return wrong_type(),
            None => {
                let items = field_args.iter().map(|_| RespFrame::integer(-2)).collect();
                return RespFrame::array(items);
            }
        };

        let mut items = Vec::with_capacity(field_args.len());
        let mut changed = false;
        for field in field_args {
            let result = match fields.get_mut(field) {
                None => -2,
                Some(stored) => {
                    if stored.expire_at_ms.is_some() {
                        stored.expire_at_ms = None;
                        changed = true;
                        1
                    } else {
                        -1
                    }
                }
            };
            items.push(RespFrame::integer(result));
        }
        if changed {
            self.note_write(&argv[1]);
        }
        RespFrame::array(items)
    }

    /// HGETEX (`hgetexCommand`, `t_hash.c`). Grammar:
    /// `HGETEX key [EX|PX|EXAT|PXAT seconds|PERSIST] FIELDS numfields field...`.
    /// Replies the field values (like HMGET), and optionally adjusts each
    /// returned field's TTL: sets a new expiry (EX/PX/EXAT/PXAT — deleting the
    /// field if already in the past), or clears it (PERSIST).
    fn hgetex_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 5 {
            return wrong_arity(b"hgetex");
        }
        let Some(fields_kw) = find_fields_keyword(argv) else {
            return err(b"ERR syntax error");
        };
        let opts = match parse_hash_set_options(&argv[2..fields_kw], HashExtMode::HGet) {
            Ok(opts) => opts,
            Err(frame) => return frame,
        };
        if fields_kw + 1 >= argv.len() {
            return err(
                b"ERR numfields should be greater than 0 and match the provided number of fields",
            );
        }
        let Some(num_fields) = parse_i64(&argv[fields_kw + 1]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let provided = (argv.len() - (fields_kw + 2)) as i64;
        if num_fields <= 0 || num_fields != provided {
            return err(
                b"ERR numfields should be greater than 0 and match the provided number of fields",
            );
        }
        let field_args: Vec<Vec<u8>> = argv[fields_kw + 2..].to_vec();

        // Resolve the requested expiry change (if any) to an absolute deadline.
        let mut set_deadline: Option<u64> = None;
        let mut set_expired = false;
        let mut persist = false;
        if opts.persist {
            persist = true;
        } else if let Some((unit, value, absolute)) = opts.expire {
            match self.resolve_field_deadline(unit, value, absolute) {
                Ok(deadline) => {
                    if deadline <= self.host.now_millis() {
                        set_expired = true;
                    } else {
                        set_deadline = Some(deadline);
                    }
                }
                Err(_) => return invalid_expire_time(b"hgetex"),
            }
        }

        self.purge_expired_fields(&argv[1]);
        let hash_exists = matches!(
            self.db.get(&argv[1]).map(|entry| &entry.value),
            Some(StoredValue::Hash(_))
        );
        if !hash_exists {
            match self.db.get(&argv[1]) {
                Some(_) => return wrong_type(),
                None => {
                    let items = field_args.iter().map(|_| RespFrame::null_bulk()).collect();
                    return RespFrame::array(items);
                }
            }
        }

        let mut items = Vec::with_capacity(field_args.len());
        let mut changed = false;
        for field in &field_args {
            let Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) = self.db.get_mut(&argv[1])
            else {
                items.push(RespFrame::null_bulk());
                continue;
            };
            match fields.get_mut(field) {
                None => items.push(RespFrame::null_bulk()),
                Some(stored) => {
                    items.push(bulk(&stored.value));
                    if set_expired {
                        fields.shift_remove(field);
                        changed = true;
                        if fields.is_empty() {
                            self.db.remove(&argv[1]);
                        }
                    } else if let Some(deadline) = set_deadline {
                        stored.expire_at_ms = Some(deadline);
                        changed = true;
                    } else if persist && stored.expire_at_ms.is_some() {
                        stored.expire_at_ms = None;
                        changed = true;
                    }
                }
            }
        }
        if changed {
            self.note_write(&argv[1]);
        }
        RespFrame::array(items)
    }

    /// HGETDEL (`hgetdelCommand`, `t_hash.c`). Grammar:
    /// `HGETDEL key FIELDS numfields field [field...]`. Replies the field
    /// values (like HMGET) and deletes those fields; if the hash becomes
    /// empty the key is removed.
    fn hgetdel_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 5 {
            return wrong_arity(b"hgetdel");
        }
        let Some(num_fields) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let provided = (argv.len() - 4) as i64;
        if num_fields <= 0 || num_fields != provided {
            return err(
                b"ERR numfields should be greater than 0 and match the provided number of fields",
            );
        }
        let field_args = &argv[4..];

        self.purge_expired_fields(&argv[1]);
        let fields = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => fields,
            Some(_) => return wrong_type(),
            None => {
                let items = field_args.iter().map(|_| RespFrame::null_bulk()).collect();
                return RespFrame::array(items);
            }
        };

        let mut items = Vec::with_capacity(field_args.len());
        let mut changed = false;
        for field in field_args {
            match fields.shift_remove(field) {
                Some(stored) => {
                    items.push(bulk(&stored.value));
                    changed = true;
                }
                None => items.push(RespFrame::null_bulk()),
            }
        }
        if fields.is_empty() {
            self.db.remove(&argv[1]);
        }
        if changed {
            self.note_write(&argv[1]);
        }
        RespFrame::array(items)
    }

    /// HSETEX (`hsetexCommand`, `t_hash.c`). Grammar:
    /// `HSETEX key [FNX|FXX] [EX|PX|EXAT|PXAT seconds|KEEPTTL] FIELDS numfields
    /// field value [field value...]`. Sets fields, applying the chosen TTL to
    /// each (or keeping the existing TTL with KEEPTTL; a plain HSETEX with no
    /// expiry option clears the field TTL). Replies `1` when the write was
    /// applied, `0` when an FNX/FXX condition blocked it.
    fn hsetex_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 6 {
            return wrong_arity(b"hsetex");
        }
        let Some(fields_kw) = find_fields_keyword(argv) else {
            return err(b"ERR syntax error");
        };
        let opts = match parse_hash_set_options(&argv[2..fields_kw], HashExtMode::HSet) {
            Ok(opts) => opts,
            Err(frame) => return frame,
        };
        if fields_kw + 1 >= argv.len() {
            return err(
                b"ERR numfields should be greater than 0 and match the provided number of fields",
            );
        }
        let Some(num_fields) = parse_i64(&argv[fields_kw + 1]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let pair_args = &argv[fields_kw + 2..];
        if num_fields <= 0
            || num_fields > i64::MAX / 2
            || num_fields * 2 != pair_args.len() as i64
        {
            return err(
                b"ERR numfields should be greater than 0 and match the provided number of fields",
            );
        }

        // Resolve the chosen expiry option to an absolute deadline up front.
        let mut set_deadline: Option<u64> = None;
        let mut set_expired = false;
        if let Some((unit, value, absolute)) = opts.expire {
            match self.resolve_field_deadline(unit, value, absolute) {
                Ok(deadline) => {
                    if deadline <= self.host.now_millis() {
                        set_expired = true;
                    } else {
                        set_deadline = Some(deadline);
                    }
                }
                Err(_) => return invalid_expire_time(b"hsetex"),
            }
        }

        self.purge_expired_fields(&argv[1]);
        let key_exists = self.db.contains_key(&argv[1]);
        if let Some(entry) = self.db.get(&argv[1]) {
            if !matches!(entry.value, StoredValue::Hash(_)) {
                return wrong_type();
            }
        }

        // Key-level NX/XX conditions (HSETEX uses FNX/FXX at field level, but
        // the parser also accepts NX/XX as key-level conditions per COMMAND_HSET).
        if (opts.key_nx && key_exists) || (opts.key_xx && !key_exists) {
            return RespFrame::integer(0);
        }

        // Field-level FNX/FXX conditions: every pair must satisfy them or the
        // whole command is a no-op replying 0.
        if opts.fnx || opts.fxx {
            let existing: Option<&IndexMap<Vec<u8>, HashField>> =
                match self.db.get(&argv[1]).map(|entry| &entry.value) {
                    Some(StoredValue::Hash(fields)) => Some(fields),
                    _ => None,
                };
            for pair in pair_args.chunks_exact(2) {
                let present = existing.is_some_and(|fields| fields.contains_key(&pair[0]));
                if (opts.fnx && present) || (opts.fxx && !present) {
                    return RespFrame::integer(0);
                }
            }
        }

        // Ensure the hash exists, then apply every pair.
        if !self.db.contains_key(&argv[1]) {
            self.db.insert(
                argv[1].clone(),
                Entry {
                    value: StoredValue::Hash(IndexMap::new()),
                    expire_at_ms: None,
                },
            );
        }
        let Some(Entry {
            value: StoredValue::Hash(fields),
            ..
        }) = self.db.get_mut(&argv[1])
        else {
            unreachable!("hash ensured present above");
        };

        for pair in pair_args.chunks_exact(2) {
            if set_expired {
                fields.shift_remove(&pair[0]);
            } else if opts.keepttl {
                // Keep any existing field TTL; new fields get no TTL.
                let existing_ttl = fields.get(&pair[0]).and_then(|stored| stored.expire_at_ms);
                fields.insert(
                    pair[0].clone(),
                    HashField {
                        value: pair[1].clone(),
                        expire_at_ms: existing_ttl,
                    },
                );
            } else {
                fields.insert(
                    pair[0].clone(),
                    HashField {
                        value: pair[1].clone(),
                        expire_at_ms: set_deadline,
                    },
                );
            }
        }
        if fields.is_empty() {
            self.db.remove(&argv[1]);
        }
        self.note_write(&argv[1]);
        RespFrame::integer(1)
    }

    /// Convert a relative/absolute hash-field expiry argument into an absolute
    /// host-millisecond deadline, mirroring `convertExpireArgumentToUnixTime`.
    /// Returns `Err(())` for a negative value or an overflow (the caller maps
    /// that to the "invalid expire time" error).
    fn resolve_field_deadline(
        &self,
        unit: ExpireUnit,
        value: i64,
        absolute: bool,
    ) -> Result<u64, ()> {
        if value < 0 {
            return Err(());
        }
        let mut when = value;
        if unit == ExpireUnit::Seconds {
            if when > i64::MAX / 1000 {
                return Err(());
            }
            when *= 1000;
        }
        let basetime: i64 = if absolute {
            0
        } else {
            self.host.now_millis() as i64
        };
        if when > i64::MAX - basetime {
            return Err(());
        }
        when += basetime;
        u64::try_from(when).map_err(|_| ())
    }

    /// ZADD with the NX | XX | CH flag subset. Mirrors the reference
    /// server's check order: flag parse, pair-shape syntax check, NX+XX
    /// conflict, score parse for every pair, then the WRONGTYPE check.
    fn zadd_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"zadd");
        }
        let mut nx = false;
        let mut xx = false;
        let mut ch = false;
        let mut index = 2;
        while index < argv.len() {
            if ascii_eq(&argv[index], b"NX") {
                nx = true;
            } else if ascii_eq(&argv[index], b"XX") {
                xx = true;
            } else if ascii_eq(&argv[index], b"CH") {
                ch = true;
            } else {
                break;
            }
            index += 1;
        }
        let pairs = &argv[index..];
        if pairs.is_empty() || pairs.len() % 2 != 0 {
            return err(b"ERR syntax error");
        }
        if nx && xx {
            return err(b"ERR XX and NX options at the same time are not compatible");
        }
        let mut scored = Vec::with_capacity(pairs.len() / 2);
        for pair in pairs.chunks_exact(2) {
            let Some(score) = parse_score(&pair[0]) else {
                return err(b"ERR value is not a valid float");
            };
            scored.push((normalize_zero(score), pair[1].clone()));
        }
        self.purge_if_expired(&argv[1]);
        let (added, updated) = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => apply_zadd(members, scored, nx, xx),
            Some(_) => return wrong_type(),
            None => {
                if xx {
                    (0, 0)
                } else {
                    let mut members = HashMap::new();
                    let counts = apply_zadd(&mut members, scored, nx, xx);
                    self.db.insert(
                        argv[1].clone(),
                        Entry {
                            value: StoredValue::ZSet(members),
                            expire_at_ms: None,
                        },
                    );
                    counts
                }
            }
        };
        if added + updated > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(if ch { added + updated } else { added })
    }

    fn zscore_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"zscore");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => match members.get(&argv[2]) {
                Some(score) => bulk(format_score(*score)),
                None => RespFrame::null_bulk(),
            },
            Some(_) => wrong_type(),
            None => RespFrame::null_bulk(),
        }
    }

    fn zincrby_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"zincrby");
        }
        let Some(increment) = parse_score(&argv[2]) else {
            return err(b"ERR value is not a valid float");
        };
        self.purge_if_expired(&argv[1]);
        match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => {
                let next = match members.get(&argv[3]) {
                    Some(current) => current + increment,
                    None => increment,
                };
                if next.is_nan() {
                    return err(b"ERR resulting score is not a number (NaN)");
                }
                members.insert(argv[3].clone(), normalize_zero(next));
                self.note_write(&argv[1]);
                bulk(format_score(next))
            }
            Some(_) => wrong_type(),
            None => {
                let mut members = HashMap::new();
                members.insert(argv[3].clone(), normalize_zero(increment));
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::ZSet(members),
                        expire_at_ms: None,
                    },
                );
                self.note_write(&argv[1]);
                bulk(format_score(increment))
            }
        }
    }

    fn zrem_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"zrem");
        }
        self.purge_if_expired(&argv[1]);
        let mut remove_empty_zset = false;
        let mut deleted = 0;
        let response = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => {
                for member in &argv[2..] {
                    if members.remove(member).is_some() {
                        deleted += 1;
                    }
                }
                remove_empty_zset = members.is_empty();
                RespFrame::integer(deleted)
            }
            Some(_) => wrong_type(),
            None => RespFrame::integer(0),
        };
        if remove_empty_zset {
            self.db.remove(&argv[1]);
        }
        if deleted > 0 {
            self.note_write(&argv[1]);
        }
        response
    }

    fn zcard_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"zcard");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => RespFrame::integer(members.len() as i64),
            Some(_) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    fn zrank_command(&mut self, argv: &[Vec<u8>], reverse: bool) -> RespFrame {
        let name: &[u8] = if reverse { b"zrevrank" } else { b"zrank" };
        if argv.len() < 3 || argv.len() > 4 {
            return wrong_arity(name);
        }
        if argv.len() == 4 {
            return err(b"ERR syntax error");
        }
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::null_bulk(),
        };
        let ordered = sorted_zset_entries(members);
        match ordered.iter().position(|(member, _)| member == &argv[2]) {
            Some(rank) => {
                let rank = if reverse {
                    ordered.len() - 1 - rank
                } else {
                    rank
                };
                RespFrame::integer(rank as i64)
            }
            None => RespFrame::null_bulk(),
        }
    }

    /// ZRANGE in its index form only, with the REV and WITHSCORES options.
    fn zrange_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"zrange");
        }
        let mut rev = false;
        let mut withscores = false;
        for option in &argv[4..] {
            if ascii_eq(option, b"REV") {
                rev = true;
            } else if ascii_eq(option, b"WITHSCORES") {
                withscores = true;
            } else {
                return err(b"ERR syntax error");
            }
        }
        let Some(start) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let Some(stop) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::array(Vec::new()),
        };
        let mut ordered = sorted_zset_entries(members);
        if rev {
            ordered.reverse();
        }
        let len = ordered.len() as i64;
        let mut start = if start < 0 { start + len } else { start };
        let mut stop = if stop < 0 { stop + len } else { stop };
        if start < 0 {
            start = 0;
        }
        if start > stop || start >= len {
            return RespFrame::array(Vec::new());
        }
        if stop >= len {
            stop = len - 1;
        }
        let mut items = Vec::new();
        for (member, score) in &ordered[start as usize..=stop as usize] {
            items.push(bulk(member));
            if withscores {
                items.push(bulk(format_score(*score)));
            }
        }
        RespFrame::array(items)
    }

    /// `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` and its
    /// reverse twin `ZREVRANGEBYSCORE key max min ...`. Score form only.
    /// Read-only: never marks a mutation. Mirrors the reference command's
    /// argument order, parsing the trailing options (WITHSCORES, LIMIT) before
    /// the min/max bounds so that a bogus option or a non-integer LIMIT
    /// argument is reported ahead of a malformed bound, matching
    /// `zrangeGenericCommand`. In the reverse form the two range arguments are
    /// supplied max-then-min and the matched members are emitted in descending
    /// order, with the LIMIT window applied after the reversal.
    fn zrangebyscore_command(&mut self, argv: &[Vec<u8>], reverse: bool) -> RespFrame {
        let name: &[u8] = if reverse {
            b"zrevrangebyscore"
        } else {
            b"zrangebyscore"
        };
        if argv.len() < 4 {
            return wrong_arity(name);
        }
        let mut withscores = false;
        let mut limit: Option<(i64, i64)> = None;
        let mut index = 4;
        while index < argv.len() {
            let option = &argv[index];
            if ascii_eq(option, b"WITHSCORES") {
                withscores = true;
                index += 1;
            } else if ascii_eq(option, b"LIMIT") && argv.len() - index - 1 >= 2 {
                let Some(offset) = parse_limit_arg(&argv[index + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                let Some(count) = parse_limit_arg(&argv[index + 2]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                limit = Some((offset, count));
                index += 3;
            } else {
                return err(b"ERR syntax error");
            }
        }
        let (min_arg, max_arg) = if reverse {
            (&argv[3], &argv[2])
        } else {
            (&argv[2], &argv[3])
        };
        let Some(min) = parse_score_bound(min_arg) else {
            return err(b"ERR min or max is not a float");
        };
        let Some(max) = parse_score_bound(max_arg) else {
            return err(b"ERR min or max is not a float");
        };
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::array(Vec::new()),
        };
        let mut in_range: Vec<(Vec<u8>, f64)> = sorted_zset_entries(members)
            .into_iter()
            .filter(|(_, score)| min.gte_min(*score) && max.lte_max(*score))
            .collect();
        if reverse {
            in_range.reverse();
        }
        let selected = apply_score_limit(&in_range, limit);
        let mut items = Vec::new();
        for (member, score) in selected {
            items.push(bulk(member));
            if withscores {
                items.push(bulk(format_score(*score)));
            }
        }
        RespFrame::array(items)
    }

    /// `ZREVRANGE key start stop [WITHSCORES]`: ZRANGE in its index form with
    /// the ordering reversed (`zrevrangeCommand` delegates to
    /// `zrangeGenericCommand` with `reverse=1`). The start/stop indices address
    /// the already-reversed (descending) order, so this is the index form of
    /// `ZRANGE ... REV`.
    fn zrevrange_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"zrevrange");
        }
        let mut withscores = false;
        for option in &argv[4..] {
            if ascii_eq(option, b"WITHSCORES") {
                withscores = true;
            } else {
                return err(b"ERR syntax error");
            }
        }
        let Some(start) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let Some(stop) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::array(Vec::new()),
        };
        let mut ordered = sorted_zset_entries(members);
        ordered.reverse();
        let len = ordered.len() as i64;
        let mut start = if start < 0 { start + len } else { start };
        let mut stop = if stop < 0 { stop + len } else { stop };
        if start < 0 {
            start = 0;
        }
        if start > stop || start >= len {
            return RespFrame::array(Vec::new());
        }
        if stop >= len {
            stop = len - 1;
        }
        let mut items = Vec::new();
        for (member, score) in &ordered[start as usize..=stop as usize] {
            items.push(bulk(member));
            if withscores {
                items.push(bulk(format_score(*score)));
            }
        }
        RespFrame::array(items)
    }

    /// `ZPOPMIN key [count]` / `ZPOPMAX key [count]` (`genericZpopCommand`).
    /// Without a count, pops the single lowest- (ZPOPMIN) or highest-scoring
    /// (ZPOPMAX) member and replies the `member, score` pair as a flat array,
    /// or an empty array if the key is absent. With a count, pops up to `count`
    /// members in pop order and replies them as a flat `member, score, ...`
    /// array. A negative count is rejected. Removing the final member deletes
    /// the key. `note_write` fires only when at least one member is popped.
    fn zpop_command(&mut self, argv: &[Vec<u8>], reverse: bool) -> RespFrame {
        let name: &[u8] = if reverse { b"zpopmax" } else { b"zpopmin" };
        if argv.len() < 2 || argv.len() > 3 {
            return wrong_arity(name);
        }
        let count_arg: Option<i64> = if argv.len() == 3 {
            let Some(count) = parse_i64(&argv[2]) else {
                return err(b"ERR value is out of range, must be positive");
            };
            if count < 0 {
                return err(b"ERR value is out of range, must be positive");
            }
            Some(count)
        } else {
            None
        };
        self.purge_if_expired(&argv[1]);
        let mut popped: Vec<(Vec<u8>, f64)> = Vec::new();
        let became_empty = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => {
                let mut ordered = sorted_zset_entries(members);
                if reverse {
                    ordered.reverse();
                }
                let take = count_arg.unwrap_or(1).max(0) as usize;
                let take = take.min(ordered.len());
                for (member, score) in ordered.into_iter().take(take) {
                    members.remove(&member);
                    popped.push((member, score));
                }
                members.is_empty()
            }
            Some(_) => return wrong_type(),
            None => false,
        };
        if became_empty {
            self.db.remove(&argv[1]);
        }
        if !popped.is_empty() {
            self.note_write(&argv[1]);
        }
        let mut items = Vec::new();
        for (member, score) in popped {
            items.push(bulk(&member));
            items.push(bulk(format_score(score)));
        }
        RespFrame::array(items)
    }

    /// `ZMSCORE key member [member...]` (`zmscoreCommand`): replies an array of
    /// one element per requested member — its bulk-string score, or a null bulk
    /// when the member (or the key) is absent. Read-only.
    fn zmscore_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"zmscore");
        }
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => Some(members),
            Some(_) => return wrong_type(),
            None => None,
        };
        let mut items = Vec::new();
        for member in &argv[2..] {
            let score = members.and_then(|m| m.get(member));
            match score {
                Some(score) => items.push(bulk(format_score(*score))),
                None => items.push(RespFrame::null_bulk()),
            }
        }
        RespFrame::array(items)
    }

    /// `ZCOUNT key min max` (`zcountCommand`): counts members whose score is in
    /// the `[min,max]` score interval, honouring the `(` exclusive form and the
    /// inf/infinity spellings on either bound. Read-only.
    fn zcount_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"zcount");
        }
        let Some(min) = parse_score_bound(&argv[2]) else {
            return err(b"ERR min or max is not a float");
        };
        let Some(max) = parse_score_bound(&argv[3]) else {
            return err(b"ERR min or max is not a float");
        };
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        let count = members
            .values()
            .filter(|score| min.gte_min(**score) && max.lte_max(**score))
            .count();
        RespFrame::integer(count as i64)
    }

    /// `ZLEXCOUNT key min max` (`zlexcountCommand`): counts members in the lex
    /// range. The zset is assumed to have equal scores; the `[`/`(`/`-`/`+`
    /// bound forms are parsed exactly like the reference `zslParseLexRangeItem`.
    /// Read-only.
    fn zlexcount_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"zlexcount");
        }
        let Some(min) = parse_lex_bound(&argv[2]) else {
            return err(b"ERR min or max not valid string range item");
        };
        let Some(max) = parse_lex_bound(&argv[3]) else {
            return err(b"ERR min or max not valid string range item");
        };
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        let count = members
            .keys()
            .filter(|member| min.gte_min(member) && max.lte_max(member))
            .count();
        RespFrame::integer(count as i64)
    }

    /// `ZRANGEBYLEX key min max [LIMIT offset count]` and its reverse twin
    /// `ZREVRANGEBYLEX key max min ...` (`genericZrangebylexCommand`). Returns
    /// the members in the lex range; the reverse form supplies the bounds
    /// max-then-min and emits members in descending order, applying the LIMIT
    /// window after the reversal. Read-only.
    fn zrangebylex_command(&mut self, argv: &[Vec<u8>], reverse: bool) -> RespFrame {
        let name: &[u8] = if reverse {
            b"zrevrangebylex"
        } else {
            b"zrangebylex"
        };
        if argv.len() < 4 {
            return wrong_arity(name);
        }
        let mut limit: Option<(i64, i64)> = None;
        let mut index = 4;
        while index < argv.len() {
            if ascii_eq(&argv[index], b"LIMIT") && argv.len() - index - 1 >= 2 {
                let Some(offset) = parse_limit_arg(&argv[index + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                let Some(count) = parse_limit_arg(&argv[index + 2]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                limit = Some((offset, count));
                index += 3;
            } else {
                return err(b"ERR syntax error");
            }
        }
        let (min_arg, max_arg) = if reverse {
            (&argv[3], &argv[2])
        } else {
            (&argv[2], &argv[3])
        };
        let Some(min) = parse_lex_bound(min_arg) else {
            return err(b"ERR min or max not valid string range item");
        };
        let Some(max) = parse_lex_bound(max_arg) else {
            return err(b"ERR min or max not valid string range item");
        };
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::array(Vec::new()),
        };
        let mut in_range: Vec<(Vec<u8>, f64)> = sorted_zset_entries(members)
            .into_iter()
            .filter(|(member, _)| min.gte_min(member) && max.lte_max(member))
            .collect();
        if reverse {
            in_range.reverse();
        }
        let selected = apply_score_limit(&in_range, limit);
        let items = selected.into_iter().map(|(member, _)| bulk(member)).collect();
        RespFrame::array(items)
    }

    /// `ZREMRANGEBYRANK key start stop` (`zremrangeGenericCommand`,
    /// `ZRANGE_RANK`): removes the members whose ascending rank falls in the
    /// clamped `[start,stop]` window and replies the number removed. Negative
    /// indices count from the end. Emptying the zset deletes the key;
    /// `note_write` fires only when at least one member is removed.
    fn zremrangebyrank_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"zremrangebyrank");
        }
        let Some(start) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let Some(stop) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        self.purge_if_expired(&argv[1]);
        let mut removed = 0;
        let became_empty = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => {
                let ordered = sorted_zset_entries(members);
                let len = ordered.len() as i64;
                let mut lo = if start < 0 { start + len } else { start };
                let hi = if stop < 0 { stop + len } else { stop };
                if lo < 0 {
                    lo = 0;
                }
                if lo <= hi && lo < len {
                    let hi = if hi >= len { len - 1 } else { hi };
                    for (member, _) in &ordered[lo as usize..=hi as usize] {
                        members.remove(member);
                        removed += 1;
                    }
                }
                members.is_empty()
            }
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        if became_empty {
            self.db.remove(&argv[1]);
        }
        if removed > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(removed)
    }

    /// `ZREMRANGEBYSCORE key min max` (`zremrangeGenericCommand`,
    /// `ZRANGE_SCORE`): removes members whose score is in the `[min,max]`
    /// interval and replies the number removed. Emptying the zset deletes the
    /// key; `note_write` fires only when at least one member is removed.
    fn zremrangebyscore_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"zremrangebyscore");
        }
        let Some(min) = parse_score_bound(&argv[2]) else {
            return err(b"ERR min or max is not a float");
        };
        let Some(max) = parse_score_bound(&argv[3]) else {
            return err(b"ERR min or max is not a float");
        };
        self.purge_if_expired(&argv[1]);
        let mut removed = 0;
        let became_empty = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => {
                let targets: Vec<Vec<u8>> = members
                    .iter()
                    .filter(|(_, score)| min.gte_min(**score) && max.lte_max(**score))
                    .map(|(member, _)| member.clone())
                    .collect();
                for member in targets {
                    members.remove(&member);
                    removed += 1;
                }
                members.is_empty()
            }
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        if became_empty {
            self.db.remove(&argv[1]);
        }
        if removed > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(removed)
    }

    /// `ZREMRANGEBYLEX key min max` (`zremrangeGenericCommand`, `ZRANGE_LEX`):
    /// removes members in the lex range and replies the number removed.
    /// Emptying the zset deletes the key; `note_write` fires only when at least
    /// one member is removed.
    fn zremrangebylex_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"zremrangebylex");
        }
        let Some(min) = parse_lex_bound(&argv[2]) else {
            return err(b"ERR min or max not valid string range item");
        };
        let Some(max) = parse_lex_bound(&argv[3]) else {
            return err(b"ERR min or max not valid string range item");
        };
        self.purge_if_expired(&argv[1]);
        let mut removed = 0;
        let became_empty = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => {
                let targets: Vec<Vec<u8>> = members
                    .keys()
                    .filter(|member| min.gte_min(member) && max.lte_max(member))
                    .cloned()
                    .collect();
                for member in targets {
                    members.remove(&member);
                    removed += 1;
                }
                members.is_empty()
            }
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        if became_empty {
            self.db.remove(&argv[1]);
        }
        if removed > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(removed)
    }

    /// ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE / ZUNION / ZINTER / ZDIFF /
    /// ZINTERCARD (`zunionInterDiffGenericCommand`, `t_zset.c`).
    ///
    /// `store` selects the `*STORE` form: the destination is `argv[1]` and
    /// `numkeys` starts at `argv[2]`; otherwise `numkeys` starts at `argv[1]`.
    /// `cardinality_only` selects ZINTERCARD (`op` must be `Inter`). Source keys
    /// missing = empty, a zset contributes its scores, a set contributes score
    /// `1.0` per member, and any other type is a WRONGTYPE error. WEIGHTS scales
    /// each source's scores (default `1.0`); AGGREGATE SUM (default)/MIN/MAX
    /// combines per-member scores. The store form replies the stored cardinality
    /// (deleting the destination on an empty result); ZINTERCARD replies the
    /// intersection cardinality capped by LIMIT (`0` = uncapped); the bare form
    /// replies the sorted members, optionally with scores.
    fn zunion_inter_diff_command(
        &mut self,
        argv: &[Vec<u8>],
        op: ZSetOp,
        store: bool,
        cardinality_only: bool,
    ) -> RespFrame {
        let full_name: &[u8] = match (op, store, cardinality_only) {
            (ZSetOp::Union, true, _) => b"zunionstore",
            (ZSetOp::Inter, true, _) => b"zinterstore",
            (ZSetOp::Diff, true, _) => b"zdiffstore",
            (ZSetOp::Union, false, _) => b"zunion",
            (ZSetOp::Inter, false, true) => b"zintercard",
            (ZSetOp::Inter, false, false) => b"zinter",
            (ZSetOp::Diff, false, _) => b"zdiff",
        };
        let numkeys_index = if store { 2usize } else { 1usize };
        if argv.len() <= numkeys_index {
            return wrong_arity(full_name);
        }
        let Some(setnum) = parse_i64(&argv[numkeys_index]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        if setnum < 1 {
            let mut message = b"ERR at least 1 input key is needed for '".to_vec();
            message.extend_from_slice(full_name);
            message.extend_from_slice(b"' command");
            return err(&message);
        }
        let setnum = setnum as usize;
        let first_key = numkeys_index + 1;
        if setnum > argv.len() - first_key {
            return err(b"ERR syntax error");
        }

        let mut weights: Vec<f64> = vec![1.0; setnum];
        let mut aggregate = ZAggregate::Sum;
        let mut withscores = false;
        let mut limit: i64 = 0;
        let mut j = first_key + setnum;
        while j < argv.len() {
            let remaining = argv.len() - j;
            if op != ZSetOp::Diff
                && !cardinality_only
                && remaining >= setnum + 1
                && ascii_eq(&argv[j], b"WEIGHTS")
            {
                j += 1;
                for weight in weights.iter_mut() {
                    let Some(value) = parse_score(&argv[j]) else {
                        return err(b"ERR weight value is not a float");
                    };
                    *weight = value;
                    j += 1;
                }
            } else if op != ZSetOp::Diff
                && !cardinality_only
                && remaining >= 2
                && ascii_eq(&argv[j], b"AGGREGATE")
            {
                aggregate = if ascii_eq(&argv[j + 1], b"SUM") {
                    ZAggregate::Sum
                } else if ascii_eq(&argv[j + 1], b"MIN") {
                    ZAggregate::Min
                } else if ascii_eq(&argv[j + 1], b"MAX") {
                    ZAggregate::Max
                } else {
                    return err(b"ERR syntax error");
                };
                j += 2;
            } else if remaining >= 1
                && !store
                && !cardinality_only
                && ascii_eq(&argv[j], b"WITHSCORES")
            {
                withscores = true;
                j += 1;
            } else if cardinality_only && remaining >= 2 && ascii_eq(&argv[j], b"LIMIT") {
                let Some(value) = parse_i64(&argv[j + 1]) else {
                    return err(b"ERR LIMIT can't be negative");
                };
                if value < 0 {
                    return err(b"ERR LIMIT can't be negative");
                }
                limit = value;
                j += 2;
            } else {
                return err(b"ERR syntax error");
            }
        }

        let mut sources: Vec<HashMap<Vec<u8>, f64>> = Vec::with_capacity(setnum);
        for key in &argv[first_key..first_key + setnum] {
            match self.get_value(key).map(|entry| &entry.value) {
                Some(StoredValue::ZSet(members)) => sources.push(members.clone()),
                Some(StoredValue::Set(members)) => {
                    sources.push(members.iter().map(|m| (m.clone(), 1.0)).collect())
                }
                Some(_) => return wrong_type(),
                None => sources.push(HashMap::new()),
            }
        }

        if cardinality_only {
            let cardinality = self.zinter_cardinality(&sources, limit);
            return RespFrame::integer(cardinality);
        }

        let result = compute_zset_op(op, &sources, &weights, aggregate);

        if store {
            let dstkey = argv[1].clone();
            if result.is_empty() {
                let existed = self.db.remove(&dstkey).is_some();
                if existed {
                    self.note_write(&dstkey);
                }
                return RespFrame::integer(0);
            }
            let cardinality = result.len() as i64;
            self.db.insert(
                dstkey.clone(),
                Entry {
                    value: StoredValue::ZSet(result),
                    expire_at_ms: None,
                },
            );
            self.note_write(&dstkey);
            return RespFrame::integer(cardinality);
        }

        let ordered = sorted_zset_entries(&result);
        let mut items = Vec::new();
        for (member, score) in ordered {
            items.push(bulk(&member));
            if withscores {
                items.push(bulk(format_score(score)));
            }
        }
        RespFrame::array(items)
    }

    /// Cardinality of the intersection of `sources` for ZINTERCARD, stopping
    /// once `limit` members are found (`limit == 0` means uncapped). Weights and
    /// the aggregate do not affect membership, so only presence matters, exactly
    /// like the `cardinality_only` branch of `zunionInterDiffGenericCommand`.
    fn zinter_cardinality(&self, sources: &[HashMap<Vec<u8>, f64>], limit: i64) -> i64 {
        let Some(smallest) = sources.iter().min_by_key(|set| set.len()) else {
            return 0;
        };
        if smallest.is_empty() {
            return 0;
        }
        let mut cardinality: i64 = 0;
        for member in smallest.keys() {
            if sources.iter().all(|set| set.contains_key(member)) {
                cardinality += 1;
                if limit > 0 && cardinality >= limit {
                    break;
                }
            }
        }
        cardinality
    }

    /// ZRANGESTORE dst src <ZRANGE range args> (`zrangestoreCommand`,
    /// `t_zset.c`): computes the same range `ZRANGE` would over `src` and stores
    /// the `(member, score)` pairs at `dst`, replying the stored cardinality. An
    /// empty result deletes `dst`. Supports the full ZRANGE grammar
    /// (BYSCORE/BYLEX/REV/LIMIT) on the source.
    fn zrangestore_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 5 {
            return wrong_arity(b"zrangestore");
        }
        let entries = match self.zrange_select(&argv[2], &argv[3..]) {
            Ok(entries) => entries,
            Err(frame) => return frame,
        };
        let dstkey = argv[1].clone();
        if entries.is_empty() {
            let existed = self.db.remove(&dstkey).is_some();
            if existed {
                self.note_write(&dstkey);
            }
            return RespFrame::integer(0);
        }
        let cardinality = entries.len() as i64;
        let members: HashMap<Vec<u8>, f64> = entries.into_iter().collect();
        self.db.insert(
            dstkey.clone(),
            Entry {
                value: StoredValue::ZSet(members),
                expire_at_ms: None,
            },
        );
        self.note_write(&dstkey);
        RespFrame::integer(cardinality)
    }

    /// Computes the ordered `(member, score)` selection a ZRANGE-family read
    /// would produce over `key`, given the range arguments `rest` (start, stop,
    /// then the trailing `[BYSCORE|BYLEX] [REV] [LIMIT offset count]
    /// [WITHSCORES]` options). Shared by ZRANGESTORE; mirrors
    /// `zrangeGenericCommand`'s option parsing, validation order, and the
    /// LIMIT-only-with-BYSCORE/BYLEX guard. Read-only.
    fn zrange_select(
        &mut self,
        key: &[u8],
        rest: &[Vec<u8>],
    ) -> Result<Vec<(Vec<u8>, f64)>, RespFrame> {
        let start_bytes = &rest[0];
        let stop_bytes = &rest[1];
        let mut by_score = false;
        let mut by_lex = false;
        let mut reverse = false;
        let mut withscores = false;
        let mut limit: Option<(i64, i64)> = None;
        let mut have_limit = false;
        let mut index = 2;
        while index < rest.len() {
            let option = &rest[index];
            if ascii_eq(option, b"BYSCORE") {
                by_score = true;
                index += 1;
            } else if ascii_eq(option, b"BYLEX") {
                by_lex = true;
                index += 1;
            } else if ascii_eq(option, b"REV") {
                reverse = true;
                index += 1;
            } else if ascii_eq(option, b"WITHSCORES") {
                withscores = true;
                index += 1;
            } else if ascii_eq(option, b"LIMIT") && rest.len() - index - 1 >= 2 {
                let Some(offset) = parse_limit_arg(&rest[index + 1]) else {
                    return Err(err(b"ERR value is not an integer or out of range"));
                };
                let Some(count) = parse_limit_arg(&rest[index + 2]) else {
                    return Err(err(b"ERR value is not an integer or out of range"));
                };
                limit = Some((offset, count));
                have_limit = true;
                index += 3;
            } else {
                return Err(err(b"ERR syntax error"));
            }
        }
        let _ = withscores;
        if by_score && by_lex {
            return Err(err(b"ERR syntax error"));
        }
        if have_limit && !by_score && !by_lex {
            return Err(err(
                b"ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
            ));
        }

        if by_lex {
            let (min_arg, max_arg) = if reverse {
                (stop_bytes, start_bytes)
            } else {
                (start_bytes, stop_bytes)
            };
            let Some(min) = parse_lex_bound(min_arg) else {
                return Err(err(b"ERR min or max not valid string range item"));
            };
            let Some(max) = parse_lex_bound(max_arg) else {
                return Err(err(b"ERR min or max not valid string range item"));
            };
            let members = match self.get_value(key).map(|entry| &entry.value) {
                Some(StoredValue::ZSet(members)) => members,
                Some(_) => return Err(wrong_type()),
                None => return Ok(Vec::new()),
            };
            let mut in_range: Vec<(Vec<u8>, f64)> = sorted_zset_entries(members)
                .into_iter()
                .filter(|(member, _)| min.gte_min(member) && max.lte_max(member))
                .collect();
            if reverse {
                in_range.reverse();
            }
            return Ok(apply_score_limit(&in_range, limit)
                .into_iter()
                .cloned()
                .collect());
        }

        if by_score {
            let (min_arg, max_arg) = if reverse {
                (stop_bytes, start_bytes)
            } else {
                (start_bytes, stop_bytes)
            };
            let Some(min) = parse_score_bound(min_arg) else {
                return Err(err(b"ERR min or max is not a float"));
            };
            let Some(max) = parse_score_bound(max_arg) else {
                return Err(err(b"ERR min or max is not a float"));
            };
            let members = match self.get_value(key).map(|entry| &entry.value) {
                Some(StoredValue::ZSet(members)) => members,
                Some(_) => return Err(wrong_type()),
                None => return Ok(Vec::new()),
            };
            let mut in_range: Vec<(Vec<u8>, f64)> = sorted_zset_entries(members)
                .into_iter()
                .filter(|(_, score)| min.gte_min(*score) && max.lte_max(*score))
                .collect();
            if reverse {
                in_range.reverse();
            }
            return Ok(apply_score_limit(&in_range, limit)
                .into_iter()
                .cloned()
                .collect());
        }

        let Some(start) = parse_i64(start_bytes) else {
            return Err(err(b"ERR value is not an integer or out of range"));
        };
        let Some(stop) = parse_i64(stop_bytes) else {
            return Err(err(b"ERR value is not an integer or out of range"));
        };
        let members = match self.get_value(key).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return Err(wrong_type()),
            None => return Ok(Vec::new()),
        };
        let mut ordered = sorted_zset_entries(members);
        if reverse {
            ordered.reverse();
        }
        let len = ordered.len() as i64;
        let mut lo = if start < 0 { start + len } else { start };
        let mut hi = if stop < 0 { stop + len } else { stop };
        if lo < 0 {
            lo = 0;
        }
        if lo > hi || lo >= len {
            return Ok(Vec::new());
        }
        if hi >= len {
            hi = len - 1;
        }
        Ok(ordered[lo as usize..=hi as usize].to_vec())
    }

    /// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count] (`zmpopGenericCommand`
    /// / `genericZpopCommand`, `t_zset.c`, non-blocking only). Pops up to
    /// `count` (default 1) members from the first non-empty key, in MIN or MAX
    /// score order. The reply is `[key, [[member, score], ...]]`, or a null
    /// array when every key is missing/empty. Emptying a key deletes it;
    /// `note_write` fires for the popped key only.
    fn zmpop_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"zmpop");
        }
        let Some(numkeys) = parse_i64(&argv[1]) else {
            return err(b"ERR numkeys should be greater than 0");
        };
        if numkeys < 1 {
            return err(b"ERR numkeys should be greater than 0");
        }
        let numkeys = numkeys as usize;
        let where_idx = 1 + numkeys + 1;
        if where_idx >= argv.len() {
            return err(b"ERR syntax error");
        }
        let reverse = if ascii_eq(&argv[where_idx], b"MIN") {
            false
        } else if ascii_eq(&argv[where_idx], b"MAX") {
            true
        } else {
            return err(b"ERR syntax error");
        };
        let mut count: i64 = -1;
        let mut j = where_idx + 1;
        while j < argv.len() {
            let moreargs = (argv.len() - 1) - j;
            if count == -1 && ascii_eq(&argv[j], b"COUNT") && moreargs > 0 {
                j += 1;
                let Some(value) = parse_i64(&argv[j]) else {
                    return err(b"ERR count should be greater than 0");
                };
                if value < 1 {
                    return err(b"ERR count should be greater than 0");
                }
                count = value;
                j += 1;
            } else {
                return err(b"ERR syntax error");
            }
        }
        let count = if count == -1 { 1 } else { count } as usize;

        let keys: Vec<Vec<u8>> = argv[2..2 + numkeys].to_vec();
        for key in &keys {
            self.purge_if_expired(key);
            let mut popped: Vec<(Vec<u8>, f64)> = Vec::new();
            let became_empty = match self.db.get_mut(key) {
                Some(Entry {
                    value: StoredValue::ZSet(members),
                    ..
                }) => {
                    let mut ordered = sorted_zset_entries(members);
                    if reverse {
                        ordered.reverse();
                    }
                    let take = count.min(ordered.len());
                    for (member, score) in ordered.into_iter().take(take) {
                        members.remove(&member);
                        popped.push((member, score));
                    }
                    members.is_empty()
                }
                Some(_) => return wrong_type(),
                None => continue,
            };
            if popped.is_empty() {
                continue;
            }
            if became_empty {
                self.db.remove(key);
            }
            self.note_write(key);
            let pairs: Vec<RespFrame> = popped
                .into_iter()
                .map(|(member, score)| {
                    RespFrame::array(vec![bulk(&member), bulk(format_score(score))])
                })
                .collect();
            return RespFrame::array(vec![bulk(key), RespFrame::array(pairs)]);
        }
        RespFrame::null_array()
    }

    /// LPUSH / RPUSH / LPUSHX / RPUSHX (`pushGenericCommand`, `t_list.c`).
    /// Each value is added to the chosen `end` in argument order, so
    /// `LPUSH k a b c` yields `c, b, a` at the head. The `x` (LPUSHX/RPUSHX)
    /// variants never create a missing key — they reply `:0`. A non-list value
    /// is rejected with WRONGTYPE. Replies the new list length.
    fn push_command(&mut self, argv: &[Vec<u8>], end: ListEnd, x: bool) -> RespFrame {
        if argv.len() < 3 {
            let name: &[u8] = match (end, x) {
                (ListEnd::Head, false) => b"lpush",
                (ListEnd::Tail, false) => b"rpush",
                (ListEnd::Head, true) => b"lpushx",
                (ListEnd::Tail, true) => b"rpushx",
            };
            return wrong_arity(name);
        }
        self.purge_if_expired(&argv[1]);
        let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        let new_len = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::List(items),
                ..
            }) => {
                for value in &argv[2..] {
                    match end {
                        ListEnd::Head => items.push_front(value.clone()),
                        ListEnd::Tail => items.push_back(value.clone()),
                    }
                }
                items.len() as i64
            }
            Some(_) => return wrong_type(),
            None => {
                if x {
                    return RespFrame::integer(0);
                }
                let mut items = VecDeque::new();
                for value in &argv[2..] {
                    match end {
                        ListEnd::Head => items.push_front(value.clone()),
                        ListEnd::Tail => items.push_back(value.clone()),
                    }
                }
                let len = items.len() as i64;
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::List(items),
                        expire_at_ms,
                    },
                );
                len
            }
        };
        self.note_write(&argv[1]);
        RespFrame::integer(new_len)
    }

    /// LPOP / RPOP (`popGenericCommand`, `t_list.c`). Without a count: replies
    /// a bulk of the popped element, or a null bulk when the key is absent.
    /// With a count (>= 0): replies an array of up to `count` elements popped
    /// in order, or a null array (`*-1`) when the key is absent; `count == 0`
    /// on a present key replies an empty array. A negative count is an error.
    /// Emptying the list deletes the key.
    fn pop_command(&mut self, argv: &[Vec<u8>], end: ListEnd) -> RespFrame {
        if !(2..=3).contains(&argv.len()) {
            let name: &[u8] = match end {
                ListEnd::Head => b"lpop",
                ListEnd::Tail => b"rpop",
            };
            return wrong_arity(name);
        }
        let count: Option<i64> = if argv.len() == 3 {
            let Some(n) = parse_i64(&argv[2]) else {
                return err(b"ERR value is not an integer or out of range");
            };
            if n < 0 {
                return err(b"ERR value is out of range, must be positive");
            }
            Some(n)
        } else {
            None
        };

        self.purge_if_expired(&argv[1]);
        let popped: Option<Vec<Vec<u8>>> = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::List(items),
                ..
            }) => {
                let take = match count {
                    None => 1,
                    Some(n) => (n as usize).min(items.len()),
                };
                let mut out = Vec::with_capacity(take);
                for _ in 0..take {
                    let next = match end {
                        ListEnd::Head => items.pop_front(),
                        ListEnd::Tail => items.pop_back(),
                    };
                    match next {
                        Some(v) => out.push(v),
                        None => break,
                    }
                }
                Some(out)
            }
            Some(_) => return wrong_type(),
            None => None,
        };

        let list_now_empty = matches!(
            self.db.get(&argv[1]),
            Some(Entry { value: StoredValue::List(items), .. }) if items.is_empty()
        );
        if list_now_empty {
            self.db.remove(&argv[1]);
        }
        if popped.as_ref().is_some_and(|out| !out.is_empty()) {
            self.note_write(&argv[1]);
        }

        match (count, popped) {
            (None, None) => RespFrame::null_bulk(),
            (None, Some(mut out)) => match out.pop() {
                Some(first) => bulk(first),
                None => RespFrame::null_bulk(),
            },
            (Some(_), None) => RespFrame::null_array(),
            (Some(_), Some(out)) => {
                RespFrame::array(out.into_iter().map(bulk).collect())
            }
        }
    }

    /// LLEN key (`llenCommand`, `t_list.c`): the list length, or `:0` for a
    /// missing key. A non-list value is WRONGTYPE.
    fn llen_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"llen");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::List(items)) => RespFrame::integer(items.len() as i64),
            Some(_) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    /// LRANGE key start stop (`lrangeCommand`, `t_list.c`). Negative indices
    /// count from the tail; the range is clamped to `[0, len-1]`. An empty or
    /// inverted range (or a missing key) replies an empty array.
    fn lrange_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"lrange");
        }
        let Some(start) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let Some(stop) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::List(items)) => match resolve_list_range(start, stop, items.len()) {
                None => RespFrame::array(Vec::new()),
                Some((s, e)) => {
                    let mut out = Vec::with_capacity(e - s + 1);
                    for i in s..=e {
                        if let Some(item) = items.get(i) {
                            out.push(bulk(item));
                        }
                    }
                    RespFrame::array(out)
                }
            },
            Some(_) => wrong_type(),
            None => RespFrame::array(Vec::new()),
        }
    }

    /// LINDEX key index (`lindexCommand`, `t_list.c`). Negative index counts
    /// from the tail; an out-of-range index (or missing key) replies a null
    /// bulk.
    fn lindex_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"lindex");
        }
        let Some(index) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::List(items)) => {
                match resolve_list_index(index, items.len()).and_then(|i| items.get(i)) {
                    Some(value) => bulk(value),
                    None => RespFrame::null_bulk(),
                }
            }
            Some(_) => wrong_type(),
            None => RespFrame::null_bulk(),
        }
    }

    /// LSET key index value (`lsetCommand`, `t_list.c`). "ERR no such key" when
    /// the key is missing, "ERR index out of range" when the (tail-relative)
    /// index falls outside the list, otherwise sets the slot and replies +OK.
    fn lset_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"lset");
        }
        let Some(index) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        self.purge_if_expired(&argv[1]);
        match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::List(items),
                ..
            }) => match resolve_list_index(index, items.len()) {
                None => err(b"ERR index out of range"),
                Some(i) => {
                    items[i] = argv[3].clone();
                    self.note_write(&argv[1]);
                    simple(b"OK")
                }
            },
            Some(_) => wrong_type(),
            None => err(b"ERR no such key"),
        }
    }

    /// LINSERT key BEFORE|AFTER pivot element (`linsertCommand`, `t_list.c`).
    /// Replies the new length on success, `:0` when the key is missing, and
    /// `:-1` when the pivot is not present.
    fn linsert_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 5 {
            return wrong_arity(b"linsert");
        }
        let after = if ascii_eq(&argv[2], b"after") {
            true
        } else if ascii_eq(&argv[2], b"before") {
            false
        } else {
            return err(b"ERR syntax error");
        };
        self.purge_if_expired(&argv[1]);
        let outcome = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::List(items),
                ..
            }) => {
                let found = items.iter().position(|item| item == &argv[3]);
                match found {
                    None => -1,
                    Some(i) => {
                        let insert_at = if after { i + 1 } else { i };
                        items.insert(insert_at, argv[4].clone());
                        items.len() as i64
                    }
                }
            }
            Some(_) => return wrong_type(),
            None => 0,
        };
        if outcome > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(outcome)
    }

    /// LREM key count element (`lremCommand`, `t_list.c`). Positive `count`
    /// removes up to `count` matches scanning from the head, negative scans
    /// from the tail, `0` removes every match. Replies the number removed and
    /// deletes the key when the list ends empty.
    fn lrem_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"lrem");
        }
        let Some(count) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        self.purge_if_expired(&argv[1]);
        let removed = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::List(items),
                ..
            }) => {
                let limit = count.unsigned_abs() as usize;
                let target = &argv[3];
                let mut removed: i64 = 0;
                if count >= 0 {
                    let mut i = 0usize;
                    while i < items.len() {
                        if &items[i] == target {
                            items.remove(i);
                            removed += 1;
                            if count > 0 && removed as usize >= limit {
                                break;
                            }
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    let mut i = items.len();
                    while i > 0 {
                        i -= 1;
                        if &items[i] == target {
                            items.remove(i);
                            removed += 1;
                            if removed as usize >= limit {
                                break;
                            }
                        }
                    }
                }
                removed
            }
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        let list_now_empty = matches!(
            self.db.get(&argv[1]),
            Some(Entry { value: StoredValue::List(items), .. }) if items.is_empty()
        );
        if list_now_empty {
            self.db.remove(&argv[1]);
        }
        if removed > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(removed)
    }

    /// LTRIM key start stop (`ltrimCommand`, `t_list.c`). Retains only the
    /// inclusive `[start, stop]` range (negative indices count from the tail,
    /// clamped). An empty resulting list deletes the key. Always replies +OK.
    fn ltrim_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"ltrim");
        }
        let Some(start) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        let Some(stop) = parse_i64(&argv[3]) else {
            return err(b"ERR value is not an integer or out of range");
        };
        self.purge_if_expired(&argv[1]);
        let removed = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::List(items),
                ..
            }) => {
                let len = items.len();
                match resolve_list_range(start, stop, len) {
                    None => {
                        items.clear();
                        len as i64
                    }
                    Some((s, e)) => {
                        for _ in 0..s {
                            items.pop_front();
                        }
                        let new_len = e - s + 1;
                        while items.len() > new_len {
                            items.pop_back();
                        }
                        len.saturating_sub(new_len) as i64
                    }
                }
            }
            Some(_) => return wrong_type(),
            None => return simple(b"OK"),
        };
        let list_now_empty = matches!(
            self.db.get(&argv[1]),
            Some(Entry { value: StoredValue::List(items), .. }) if items.is_empty()
        );
        if list_now_empty {
            self.db.remove(&argv[1]);
        }
        if removed > 0 {
            self.note_write(&argv[1]);
        }
        simple(b"OK")
    }

    /// RPOPLPUSH source destination (`rpoplpushCommand`, `t_list.c`): pop the
    /// tail of `source`, push it to the head of `destination`, and reply the
    /// moved element. Equivalent to `LMOVE source destination RIGHT LEFT`.
    fn rpoplpush_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"rpoplpush");
        }
        self.lmove_generic(&argv[1], &argv[2], ListEnd::Tail, ListEnd::Head)
    }

    /// LMOVE source destination LEFT|RIGHT LEFT|RIGHT (`lmoveCommand`,
    /// `t_list.c`): pop from the `from` end of `source` and push to the `to`
    /// end of `destination`, replying the moved element.
    fn lmove_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 5 {
            return wrong_arity(b"lmove");
        }
        let Some(wherefrom) = parse_list_position(&argv[3]) else {
            return err(b"ERR syntax error");
        };
        let Some(whereto) = parse_list_position(&argv[4]) else {
            return err(b"ERR syntax error");
        };
        self.lmove_generic(&argv[1], &argv[2], wherefrom, whereto)
    }

    /// Shared core of LMOVE / RPOPLPUSH (`lmoveGenericCommand`, `t_list.c`).
    /// Replies a null bulk when `source` is missing or empty. WRONGTYPE if
    /// either key holds a non-list value (destination is type-checked before
    /// the pop, matching the C order). `source == destination` rotates the list
    /// in place. Emptying `source` deletes the key; `destination` is created on
    /// demand. Both keys are noted as written.
    fn lmove_generic(
        &mut self,
        src: &[u8],
        dst: &[u8],
        wherefrom: ListEnd,
        whereto: ListEnd,
    ) -> RespFrame {
        self.purge_if_expired(src);
        self.purge_if_expired(dst);

        match self.db.get(src) {
            Some(Entry { value: StoredValue::List(_), .. }) => {}
            Some(_) => return wrong_type(),
            None => return RespFrame::null_bulk(),
        }
        match self.db.get(dst) {
            Some(Entry { value: StoredValue::List(_), .. }) | None => {}
            Some(_) => return wrong_type(),
        }

        let value = match self.db.get_mut(src) {
            Some(Entry { value: StoredValue::List(items), .. }) => match wherefrom {
                ListEnd::Head => items.pop_front(),
                ListEnd::Tail => items.pop_back(),
            },
            _ => None,
        };
        let value = match value {
            Some(v) => v,
            None => return RespFrame::null_bulk(),
        };

        let src_empty = matches!(
            self.db.get(src),
            Some(Entry { value: StoredValue::List(items), .. }) if items.is_empty()
        );
        if src_empty {
            self.db.remove(src);
        }

        match self.db.get_mut(dst) {
            Some(Entry { value: StoredValue::List(items), .. }) => match whereto {
                ListEnd::Head => items.push_front(value.clone()),
                ListEnd::Tail => items.push_back(value.clone()),
            },
            _ => {
                let mut items = VecDeque::new();
                match whereto {
                    ListEnd::Head => items.push_front(value.clone()),
                    ListEnd::Tail => items.push_back(value.clone()),
                }
                self.db.insert(
                    dst.to_vec(),
                    Entry {
                        value: StoredValue::List(items),
                        expire_at_ms: None,
                    },
                );
            }
        }

        self.note_write(src);
        self.note_write(dst);
        bulk(value)
    }

    /// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
    /// (`lmpopGenericCommand` + `mpopGenericCommand`, `t_list.c`). Pops up to
    /// `count` (default 1) elements from the first non-empty list among the
    /// keys and replies `[key, [elem, ...]]`. When every key is missing or
    /// empty replies a null array (`*-1`). Popping with `RIGHT` returns the
    /// elements in reverse list order (tail first). Emptying a list deletes the
    /// key. A non-list value among the keys is WRONGTYPE.
    fn lmpop_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"lmpop");
        }
        let Some(numkeys) = parse_i64(&argv[1]) else {
            return err(b"ERR numkeys should be greater than 0");
        };
        if numkeys <= 0 {
            return err(b"ERR numkeys should be greater than 0");
        }
        let numkeys = numkeys as usize;
        let where_idx = 1 + numkeys + 1;
        if where_idx >= argv.len() {
            return err(b"ERR syntax error");
        }
        let Some(position) = parse_list_position(&argv[where_idx]) else {
            return err(b"ERR syntax error");
        };
        let mut count: i64 = -1;
        let mut j = where_idx + 1;
        while j < argv.len() {
            let moreargs = argv.len() - 1 - j;
            if count == -1 && ascii_eq(&argv[j], b"COUNT") && moreargs >= 1 {
                let Some(parsed) = parse_i64(&argv[j + 1]) else {
                    return err(b"ERR count should be greater than 0");
                };
                if parsed <= 0 {
                    return err(b"ERR count should be greater than 0");
                }
                count = parsed;
                j += 2;
            } else {
                return err(b"ERR syntax error");
            }
        }
        let count = if count == -1 { 1 } else { count } as usize;

        let keys: Vec<Vec<u8>> = argv[2..2 + numkeys].to_vec();
        for key in &keys {
            self.purge_if_expired(key);
            let has_data = match self.db.get(key) {
                Some(Entry { value: StoredValue::List(items), .. }) => !items.is_empty(),
                Some(_) => return wrong_type(),
                None => false,
            };
            if !has_data {
                continue;
            }
            let popped: Vec<Vec<u8>> = match self.db.get_mut(key) {
                Some(Entry { value: StoredValue::List(items), .. }) => {
                    let take = count.min(items.len());
                    let mut out = Vec::with_capacity(take);
                    for _ in 0..take {
                        let next = match position {
                            ListEnd::Head => items.pop_front(),
                            ListEnd::Tail => items.pop_back(),
                        };
                        match next {
                            Some(v) => out.push(v),
                            None => break,
                        }
                    }
                    out
                }
                _ => Vec::new(),
            };
            let list_now_empty = matches!(
                self.db.get(key),
                Some(Entry { value: StoredValue::List(items), .. }) if items.is_empty()
            );
            if list_now_empty {
                self.db.remove(key);
            }
            self.note_write(key);
            let elems = popped.into_iter().map(bulk).collect();
            return RespFrame::array(vec![bulk(key), RespFrame::array(elems)]);
        }
        RespFrame::null_array()
    }

    /// LPOS key element [RANK rank] [COUNT num] [MAXLEN len] (`lposCommand`,
    /// `t_list.c`). Without COUNT, replies the index of the (rank-th) match as
    /// an integer, or a null bulk when there is no match / no key. With COUNT,
    /// replies an array of indices (empty when no key / no match); `COUNT 0`
    /// returns every match. A negative RANK scans from the tail; `RANK 0` is an
    /// error. MAXLEN caps the number of entries compared (0 = unlimited).
    fn lpos_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"lpos");
        }
        let element = &argv[2];
        let mut rank: i64 = 1;
        let mut count: Option<i64> = None;
        let mut maxlen: i64 = 0;
        let mut j = 3usize;
        while j < argv.len() {
            let moreargs = argv.len() - 1 - j;
            if ascii_eq(&argv[j], b"RANK") && moreargs >= 1 {
                let Some(parsed) = parse_i64(&argv[j + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if parsed == 0 {
                    return err(b"ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list");
                }
                if parsed.checked_neg().is_none() {
                    return err(b"ERR value is out of range");
                }
                rank = parsed;
                j += 2;
            } else if ascii_eq(&argv[j], b"COUNT") && moreargs >= 1 {
                let Some(parsed) = parse_i64(&argv[j + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if parsed < 0 {
                    return err(b"ERR COUNT can't be negative");
                }
                count = Some(parsed);
                j += 2;
            } else if ascii_eq(&argv[j], b"MAXLEN") && moreargs >= 1 {
                let Some(parsed) = parse_i64(&argv[j + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if parsed < 0 {
                    return err(b"ERR MAXLEN can't be negative");
                }
                maxlen = parsed;
                j += 2;
            } else {
                return err(b"ERR syntax error");
            }
        }

        let items = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::List(items)) => items,
            Some(_) => return wrong_type(),
            None => {
                return match count {
                    None => RespFrame::null_bulk(),
                    Some(_) => RespFrame::array(Vec::new()),
                };
            }
        };

        let len = items.len();
        let forward = rank > 0;
        let skip = rank.unsigned_abs() as usize - 1;
        let want_all = matches!(count, Some(0));
        let want_one = count.is_none();
        let limit = count.map(|c| c as usize);
        let maxlen = maxlen as usize;
        let mut matches: Vec<i64> = Vec::new();
        let mut seen = 0usize;
        let mut scanned = 0usize;

        if forward {
            for (idx, item) in items.iter().enumerate() {
                if maxlen != 0 && scanned >= maxlen {
                    break;
                }
                scanned += 1;
                if item == element {
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
            for (rev_idx, item) in items.iter().rev().enumerate() {
                if maxlen != 0 && scanned >= maxlen {
                    break;
                }
                scanned += 1;
                if item == element {
                    if seen >= skip {
                        matches.push((len - 1 - rev_idx) as i64);
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
                None => RespFrame::null_bulk(),
                Some(v) => RespFrame::integer(*v),
            }
        } else {
            RespFrame::array(matches.into_iter().map(RespFrame::integer).collect())
        }
    }

    /// SADD key member [member ...] (`saddCommand`, `t_set.c`). Creates the set
    /// when missing, adds each member that is not already present, and replies
    /// the count of newly-added members. Mutates (and bumps the epoch) only
    /// when at least one member was added. A non-set value is WRONGTYPE.
    fn sadd_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"sadd");
        }
        self.purge_if_expired(&argv[1]);
        let added = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Set(members),
                ..
            }) => {
                let mut added = 0i64;
                for member in &argv[2..] {
                    if members.insert(member.clone()) {
                        added += 1;
                    }
                }
                added
            }
            Some(_) => return wrong_type(),
            None => {
                let mut members = IndexSet::new();
                let mut added = 0i64;
                for member in &argv[2..] {
                    if members.insert(member.clone()) {
                        added += 1;
                    }
                }
                self.db.insert(
                    argv[1].clone(),
                    Entry {
                        value: StoredValue::Set(members),
                        expire_at_ms: None,
                    },
                );
                added
            }
        };
        if added > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(added)
    }

    /// SREM key member [member ...] (`sremCommand`, `t_set.c`). Removes each
    /// present member, replies the count removed, and deletes the key when the
    /// set becomes empty. Mutates only when at least one member was removed.
    fn srem_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"srem");
        }
        self.purge_if_expired(&argv[1]);
        let removed = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Set(members),
                ..
            }) => {
                let mut removed = 0i64;
                for member in &argv[2..] {
                    if members.shift_remove(member) {
                        removed += 1;
                    }
                }
                removed
            }
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        let set_now_empty = matches!(
            self.db.get(&argv[1]),
            Some(Entry { value: StoredValue::Set(members), .. }) if members.is_empty()
        );
        if set_now_empty {
            self.db.remove(&argv[1]);
        }
        if removed > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(removed)
    }

    /// SCARD key (`scardCommand`, `t_set.c`): the set cardinality, or `:0` for
    /// a missing key. A non-set value is WRONGTYPE.
    fn scard_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"scard");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Set(members)) => RespFrame::integer(members.len() as i64),
            Some(_) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    /// SISMEMBER key member (`sismemberCommand`, `t_set.c`): `:1` when the
    /// member is present, `:0` otherwise (including a missing key).
    fn sismember_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(b"sismember");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Set(members)) => {
                RespFrame::integer(if members.contains(&argv[2]) { 1 } else { 0 })
            }
            Some(_) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    /// SMISMEMBER key member [member ...] (`smismemberCommand`, `t_set.c`):
    /// an array of `:1`/`:0` per queried member. A missing key is treated as
    /// an empty set, so every answer is `:0`.
    fn smismember_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"smismember");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Set(members)) => {
                let items = argv[2..]
                    .iter()
                    .map(|member| RespFrame::integer(if members.contains(member) { 1 } else { 0 }))
                    .collect();
                RespFrame::array(items)
            }
            Some(_) => wrong_type(),
            None => {
                let items = argv[2..].iter().map(|_| RespFrame::integer(0)).collect();
                RespFrame::array(items)
            }
        }
    }

    /// SMEMBERS key (`smembersCommand` -> `sinterCommand` with one key,
    /// `t_set.c`): the set's members as an array, or an empty array for a
    /// missing key. Members are returned in valkey's stored order
    /// (`set_member_order`): an all-integer intset is iterated **sorted
    /// ascending**, a listpack/hashtable set in **insertion order**.
    fn smembers_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"smembers");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Set(members)) => {
                RespFrame::array(set_member_order(members).iter().map(bulk).collect())
            }
            Some(_) => wrong_type(),
            None => RespFrame::array(Vec::new()),
        }
    }

    /// SMOVE source destination member (`smoveCommand`, `t_set.c`). Replies
    /// `:0` when the source is missing or the member is not in the source,
    /// `:1` when the member moves. WRONGTYPE when either key holds a non-set.
    /// Removing the last member deletes the source; the destination is created
    /// when absent.
    fn smove_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 4 {
            return wrong_arity(b"smove");
        }
        self.purge_if_expired(&argv[1]);
        self.purge_if_expired(&argv[2]);

        match self.db.get(&argv[1]) {
            None => return RespFrame::integer(0),
            Some(Entry {
                value: StoredValue::Set(_),
                ..
            }) => {}
            Some(_) => return wrong_type(),
        }
        match self.db.get(&argv[2]) {
            None
            | Some(Entry {
                value: StoredValue::Set(_),
                ..
            }) => {}
            Some(_) => return wrong_type(),
        }

        let member = &argv[3];
        if argv[1] == argv[2] {
            let present = matches!(
                self.db.get(&argv[1]),
                Some(Entry { value: StoredValue::Set(members), .. }) if members.contains(member)
            );
            return RespFrame::integer(if present { 1 } else { 0 });
        }

        let removed = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Set(members),
                ..
            }) => members.shift_remove(member),
            _ => false,
        };
        if !removed {
            return RespFrame::integer(0);
        }
        let src_now_empty = matches!(
            self.db.get(&argv[1]),
            Some(Entry { value: StoredValue::Set(members), .. }) if members.is_empty()
        );
        if src_now_empty {
            self.db.remove(&argv[1]);
        }
        match self.db.get_mut(&argv[2]) {
            Some(Entry {
                value: StoredValue::Set(members),
                ..
            }) => {
                members.insert(member.clone());
            }
            _ => {
                let mut members = IndexSet::new();
                members.insert(member.clone());
                self.db.insert(
                    argv[2].clone(),
                    Entry {
                        value: StoredValue::Set(members),
                        expire_at_ms: None,
                    },
                );
            }
        }
        self.note_write(&argv[1]);
        self.note_write(&argv[2]);
        RespFrame::integer(1)
    }

    /// SINTER / SUNION / SDIFF (and their `*STORE` / `SINTERCARD` callers),
    /// mirroring `sinterGenericCommand` / `sunionDiffGenericCommand`
    /// (`t_set.c`). Reads the source sets at `argv[key_start..]`, computes the
    /// `op`, and either replies the result array (when `dstkey` is None and
    /// `cardinality_only` is false), the integer cardinality (`cardinality_only`),
    /// or stores into `dstkey`. A missing source key is an empty set; any
    /// non-set source is WRONGTYPE.
    fn set_op_command(
        &mut self,
        argv: &[Vec<u8>],
        op: SetOp,
        dstkey: Option<&[u8]>,
        cardinality_only: bool,
    ) -> RespFrame {
        let name: &[u8] = match (op, dstkey.is_some(), cardinality_only) {
            (SetOp::Inter, false, false) => b"sinter",
            (SetOp::Union, false, false) => b"sunion",
            (SetOp::Diff, false, false) => b"sdiff",
            (SetOp::Inter, true, _) => b"sinterstore",
            (SetOp::Union, true, _) => b"sunionstore",
            (SetOp::Diff, true, _) => b"sdiffstore",
            (SetOp::Inter, false, true) => b"sintercard",
            _ => b"set",
        };
        let key_start = 1usize;
        if argv.len() <= key_start {
            return wrong_arity(name);
        }
        let result = match self.collect_set_op(&argv[key_start..], op) {
            Ok(result) => result,
            Err(frame) => return frame,
        };
        self.finish_set_op(result, dstkey, cardinality_only, None)
    }

    /// SINTERCARD numkeys key [key ...] [LIMIT limit] (`sinterCardCommand`,
    /// `t_set.c`). Parses `numkeys` (must be >= 1 and not exceed the supplied
    /// key count) and an optional non-negative `LIMIT`, then replies the
    /// integer cardinality of the intersection, capped by the limit when set.
    fn sintercard_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"sintercard");
        }
        let Some(numkeys) = parse_i64(&argv[1]) else {
            return err(b"ERR numkeys should be greater than 0");
        };
        if numkeys < 1 {
            return err(b"ERR numkeys should be greater than 0");
        }
        let numkeys = numkeys as usize;
        if numkeys > argv.len() - 2 {
            return err(b"ERR Number of keys can't be greater than number of args");
        }
        let keys = &argv[2..2 + numkeys];
        let mut limit: i64 = 0;
        let mut j = 2 + numkeys;
        while j < argv.len() {
            let more = (argv.len() - 1) - j;
            if ascii_eq(&argv[j], b"LIMIT") && more > 0 {
                j += 1;
                let Some(value) = parse_i64(&argv[j]) else {
                    return err(b"ERR LIMIT can't be negative");
                };
                if value < 0 {
                    return err(b"ERR LIMIT can't be negative");
                }
                limit = value;
            } else {
                return err(b"ERR syntax error");
            }
            j += 1;
        }
        let result = match self.collect_set_op(keys, SetOp::Inter) {
            Ok(result) => result,
            Err(frame) => return frame,
        };
        let limit = if limit > 0 { Some(limit as usize) } else { None };
        self.finish_set_op(result, None, true, limit)
    }

    /// SINTERSTORE / SUNIONSTORE / SDIFFSTORE destination key [key ...]
    /// (`sinterstoreCommand` / `sunionstoreCommand` / `sdiffstoreCommand`,
    /// `t_set.c`). The destination is `argv[1]`; the source keys follow. Stores
    /// the result into the destination (overwriting any existing value) and
    /// replies its cardinality, or deletes the destination and replies `:0`
    /// when the result is empty.
    fn set_store_command(&mut self, argv: &[Vec<u8>], op: SetOp) -> RespFrame {
        let name: &[u8] = match op {
            SetOp::Inter => b"sinterstore",
            SetOp::Union => b"sunionstore",
            SetOp::Diff => b"sdiffstore",
        };
        if argv.len() < 3 {
            return wrong_arity(name);
        }
        let dstkey = argv[1].clone();
        let result = match self.collect_set_op(&argv[2..], op) {
            Ok(result) => result,
            Err(frame) => return frame,
        };
        self.finish_set_op(result, Some(&dstkey), false, None)
    }

    /// Read the `keys` as sets (missing = empty, non-set = WRONGTYPE error
    /// frame) and compute `op`. Returns the resulting members; `Diff` subtracts
    /// every later key's set from the first key's set in order. The result
    /// order is unspecified, matching the C iterator order being
    /// implementation-defined.
    fn collect_set_op(
        &mut self,
        keys: &[Vec<u8>],
        op: SetOp,
    ) -> Result<IndexSet<Vec<u8>>, RespFrame> {
        let mut sets: Vec<Option<IndexSet<Vec<u8>>>> = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get_value(key).map(|entry| &entry.value) {
                Some(StoredValue::Set(members)) => sets.push(Some(members.clone())),
                Some(_) => return Err(wrong_type()),
                None => sets.push(None),
            }
        }
        let result = match op {
            SetOp::Inter => {
                if sets.iter().any(|set| set.is_none()) {
                    IndexSet::new()
                } else {
                    // Intersection keeps the first set's iteration order, dropping
                    // members absent from any later set (`sinterGenericCommand`
                    // walks the first set and tests the rest).
                    let mut iter = sets.into_iter().map(|set| set.unwrap());
                    let mut acc = iter.next().unwrap_or_default();
                    for other in iter {
                        acc.retain(|member| other.contains(member));
                        if acc.is_empty() {
                            break;
                        }
                    }
                    acc
                }
            }
            SetOp::Union => {
                // Union appends each source's members in source order, skipping
                // any already present (`IndexSet::extend` keeps first-seen
                // position), matching `sunionDiffGenericCommand`'s sequential add.
                let mut acc = IndexSet::new();
                for set in sets.into_iter().flatten() {
                    acc.extend(set);
                }
                acc
            }
            SetOp::Diff => {
                // Difference keeps the first set's order minus members present in
                // any later set.
                let mut iter = sets.into_iter();
                let mut acc = iter.next().flatten().unwrap_or_default();
                for other in iter.flatten() {
                    for member in &other {
                        acc.shift_remove(member);
                    }
                    if acc.is_empty() {
                        break;
                    }
                }
                acc
            }
        };
        Ok(result)
    }

    /// Emit the result of a multi-key set op: store it (when `dstkey` is set),
    /// reply just the cardinality (when `cardinality_only`, capped by `limit`),
    /// or reply the members as an array.
    fn finish_set_op(
        &mut self,
        result: IndexSet<Vec<u8>>,
        dstkey: Option<&[u8]>,
        cardinality_only: bool,
        limit: Option<usize>,
    ) -> RespFrame {
        if let Some(dstkey) = dstkey {
            if result.is_empty() {
                let existed = self.db.remove(dstkey).is_some();
                if existed {
                    self.note_write(dstkey);
                }
                return RespFrame::integer(0);
            }
            let cardinality = result.len() as i64;
            self.db.insert(
                dstkey.to_vec(),
                Entry {
                    value: StoredValue::Set(result),
                    expire_at_ms: None,
                },
            );
            self.note_write(dstkey);
            return RespFrame::integer(cardinality);
        }
        if cardinality_only {
            let count = match limit {
                Some(limit) => result.len().min(limit),
                None => result.len(),
            };
            return RespFrame::integer(count as i64);
        }
        RespFrame::array(result.iter().map(bulk).collect())
    }

    /// The generic EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT implementation,
    /// mirroring `expireGenericCommand` (`expire.c`). `unit` scales `argv[2]`;
    /// `absolute` selects the basetime (0 for the *AT variants, the current
    /// wall clock for the relative variants). The optional trailing flag is
    /// parsed exactly like `parseExtendedExpireArgumentsOrReply`, including the
    /// NX/XX/GT/LT conflict checks.
    fn expire_command(
        &mut self,
        argv: &[Vec<u8>],
        unit: ExpireUnit,
        absolute: bool,
    ) -> RespFrame {
        let name: &[u8] = match (unit, absolute) {
            (ExpireUnit::Seconds, false) => b"expire",
            (ExpireUnit::Milliseconds, false) => b"pexpire",
            (ExpireUnit::Seconds, true) => b"expireat",
            (ExpireUnit::Milliseconds, true) => b"pexpireat",
        };
        if argv.len() < 3 {
            return wrong_arity(name);
        }

        let condition = match parse_expire_condition(&argv[3..]) {
            Ok(condition) => condition,
            Err(frame) => return frame,
        };

        let Some(mut when) = parse_i64(&argv[2]) else {
            return err(b"ERR value is not an integer or out of range");
        };

        if unit == ExpireUnit::Seconds {
            if when > i64::MAX / 1000 || when < i64::MIN / 1000 {
                return invalid_expire_time(name);
            }
            when *= 1000;
        }

        let basetime: i64 = if absolute {
            0
        } else {
            self.host.now_millis() as i64
        };
        if when > i64::MAX - basetime {
            return invalid_expire_time(name);
        }
        when += basetime;
        if when < 0 {
            when = 0;
        }

        self.purge_if_expired(&argv[1]);
        let current_expire: Option<u64> = match self.db.get(&argv[1]) {
            Some(entry) => entry.expire_at_ms,
            None => return RespFrame::integer(0),
        };

        if let Some(condition) = condition {
            let current = current_expire.map(|value| value as i64);
            let blocked = match condition {
                ExpireCondition::Nx => current.is_some(),
                ExpireCondition::Xx => current.is_none(),
                ExpireCondition::Gt => match current {
                    None => true,
                    Some(value) => when <= value,
                },
                ExpireCondition::Lt => match current {
                    None => false,
                    Some(value) => when >= value,
                },
            };
            if blocked {
                return RespFrame::integer(0);
            }
        }

        if when <= self.host.now_millis() as i64 {
            self.db.remove(&argv[1]);
            self.note_write(&argv[1]);
            return RespFrame::integer(1);
        }

        if let Some(entry) = self.db.get_mut(&argv[1]) {
            entry.expire_at_ms = Some(when as u64);
            self.note_write(&argv[1]);
        }
        RespFrame::integer(1)
    }

    /// PERSIST key (`persistCommand`, `expire.c`): drop a key's TTL, replying
    /// :1 only when a TTL was actually removed.
    fn persist_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"persist");
        }
        self.purge_if_expired(&argv[1]);
        match self.db.get_mut(&argv[1]) {
            Some(entry) if entry.expire_at_ms.is_some() => {
                entry.expire_at_ms = None;
                self.note_write(&argv[1]);
                RespFrame::integer(1)
            }
            _ => RespFrame::integer(0),
        }
    }

    /// TTL / PTTL / EXPIRETIME / PEXPIRETIME (`ttlGenericCommand`, `expire.c`).
    /// `milliseconds` selects ms vs second granularity; `absolute` returns the
    /// absolute expiry timestamp rather than the remaining TTL.
    fn ttl_command(&mut self, argv: &[Vec<u8>], milliseconds: bool, absolute: bool) -> RespFrame {
        let name: &[u8] = match (milliseconds, absolute) {
            (false, false) => b"ttl",
            (true, false) => b"pttl",
            (false, true) => b"expiretime",
            (true, true) => b"pexpiretime",
        };
        if argv.len() != 2 {
            return wrong_arity(name);
        }
        self.purge_if_expired(&argv[1]);
        let Some(entry) = self.db.get(&argv[1]) else {
            return RespFrame::integer(-2);
        };
        let Some(expire_at_ms) = entry.expire_at_ms else {
            return RespFrame::integer(-1);
        };
        let ttl = if absolute {
            expire_at_ms
        } else {
            expire_at_ms.saturating_sub(self.host.now_millis())
        };
        if milliseconds {
            RespFrame::integer(ttl as i64)
        } else {
            RespFrame::integer(((ttl + 500) / 1000) as i64)
        }
    }

    /// XADD key [NOMKSTREAM] [MAXLEN|MINID [~|=] threshold [LIMIT n]] <id|*>
    /// field value ... (`xaddCommand`, `t_stream.c`). Appends an entry, returns
    /// its ID as a bulk string. `NOMKSTREAM` on a missing key replies a null
    /// bulk. An explicit ID must be greater than the stream top item.
    fn xadd_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 5 {
            return wrong_arity(b"xadd");
        }
        let key = &argv[1];

        let mut trim: Option<StreamTrim> = None;
        let mut no_mkstream = false;
        let mut id_arg: Option<&[u8]> = None;
        let mut field_pos = argv.len();

        let mut i = 2usize;
        while i < argv.len() {
            let opt = &argv[i];
            let moreargs = argv.len() - 1 - i;
            if opt.len() == 1 && opt[0] == b'*' {
                id_arg = Some(opt);
                field_pos = i + 1;
                break;
            } else if ascii_eq(opt, b"MAXLEN") && moreargs >= 1 {
                if trim.is_some() {
                    return err(b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible");
                }
                match self.parse_trim(argv, &mut i, true) {
                    Ok(t) => trim = Some(t),
                    Err(frame) => return frame,
                }
            } else if ascii_eq(opt, b"MINID") && moreargs >= 1 {
                if trim.is_some() {
                    return err(b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible");
                }
                match self.parse_trim(argv, &mut i, false) {
                    Ok(t) => trim = Some(t),
                    Err(frame) => return frame,
                }
            } else if ascii_eq(opt, b"LIMIT") && moreargs >= 1 {
                if parse_i64(&argv[i + 1]).is_none() {
                    return err(b"ERR value is not an integer or out of range");
                }
                match &mut trim {
                    Some(t) => t.limit_given = true,
                    None => {}
                }
                i += 1;
            } else if ascii_eq(opt, b"NOMKSTREAM") {
                no_mkstream = true;
            } else {
                match parse_stream_id_strict(opt, 0) {
                    Ok(_) => {
                        id_arg = Some(opt);
                        field_pos = i + 1;
                        break;
                    }
                    Err(frame) => return frame,
                }
            }
            i += 1;
        }

        let Some(id_arg) = id_arg else {
            return wrong_arity(b"xadd");
        };

        if argv.len() <= field_pos
            || (argv.len() - field_pos) < 2
            || (argv.len() - field_pos) % 2 == 1
        {
            return wrong_arity(b"xadd");
        }

        if let Some(t) = &trim {
            if t.limit_given && !t.approx {
                return err(b"ERR syntax error, LIMIT cannot be used without the special ~ option");
            }
        }

        let auto_id = id_arg.len() == 1 && id_arg[0] == b'*';
        let (use_id, seq_given) = if auto_id {
            (None, true)
        } else {
            match parse_stream_id_strict(id_arg, 0) {
                Ok((id, seq_given)) => (Some(id), seq_given),
                Err(frame) => return frame,
            }
        };

        if let Some(id) = use_id {
            if seq_given && id == StreamId::MIN {
                return err(b"ERR The ID specified in XADD must be greater than 0-0");
            }
        }

        self.purge_if_expired(key);
        let exists = matches!(
            self.db.get(key),
            Some(Entry {
                value: StoredValue::Stream(_),
                ..
            })
        );
        match self.db.get(key) {
            Some(Entry {
                value: StoredValue::Stream(_),
                ..
            })
            | None => {}
            Some(_) => return wrong_type(),
        }
        if !exists && no_mkstream {
            return RespFrame::null_bulk();
        }

        let fields: Vec<(Vec<u8>, Vec<u8>)> = argv[field_pos..]
            .chunks_exact(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect();

        let now = self.host.now_millis();
        if !exists {
            self.db.insert(
                key.clone(),
                Entry {
                    value: StoredValue::Stream(StreamValue::default()),
                    expire_at_ms: None,
                },
            );
        }
        let stream = match self.db.get_mut(key) {
            Some(Entry {
                value: StoredValue::Stream(stream),
                ..
            }) => stream,
            _ => unreachable!("stream just created or verified"),
        };

        if stream.last_id == StreamId::MAX {
            return err(
                b"ERR The stream has exhausted the last possible ID, unable to add more items",
            );
        }

        let new_id = match stream_next_append_id(stream.last_id, use_id, seq_given, now) {
            Some(id) => id,
            None => {
                return err(b"ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        };

        stream.entries.insert(new_id, fields);
        stream.last_id = new_id;
        stream.entries_added += 1;
        if stream.entries.len() == 1 {
            stream.first_id = new_id;
        }

        if let Some(t) = &trim {
            apply_stream_trim(stream, t);
        }

        let reply = new_id.to_string_bytes();
        self.note_write(key);
        bulk(reply)
    }

    /// Parse a `MAXLEN`/`MINID [~|=] threshold` trim option in place, advancing
    /// `i` past the consumed arguments. `is_maxlen` selects the threshold type.
    fn parse_trim(
        &self,
        argv: &[Vec<u8>],
        i: &mut usize,
        is_maxlen: bool,
    ) -> Result<StreamTrim, RespFrame> {
        let mut approx = false;
        let mut idx = *i + 1;
        if idx < argv.len() && argv[idx].len() == 1 && argv[idx][0] == b'~' {
            approx = true;
            idx += 1;
        } else if idx < argv.len() && argv[idx].len() == 1 && argv[idx][0] == b'=' {
            idx += 1;
        }
        if idx >= argv.len() {
            return Err(err(b"ERR syntax error"));
        }
        let threshold = if is_maxlen {
            let Some(n) = parse_i64(&argv[idx]) else {
                return Err(err(b"ERR value is not an integer or out of range"));
            };
            if n < 0 {
                return Err(err(b"ERR The MAXLEN argument must be >= 0."));
            }
            StreamTrimThreshold::MaxLen(n as u64)
        } else {
            match parse_stream_id_strict(&argv[idx], 0) {
                Ok((id, _)) => StreamTrimThreshold::MinId(id),
                Err(frame) => return Err(frame),
            }
        };
        *i = idx;
        Ok(StreamTrim {
            threshold,
            approx,
            limit_given: false,
        })
    }

    /// XLEN key (`xlenCommand`, `t_stream.c`): the entry count, `:0` for a
    /// missing key, WRONGTYPE for a non-stream.
    fn xlen_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"xlen");
        }
        match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::Stream(stream)) => RespFrame::integer(stream.entries.len() as i64),
            Some(_) => wrong_type(),
            None => RespFrame::integer(0),
        }
    }

    /// XRANGE key start end [COUNT n] / XREVRANGE key end start [COUNT n]
    /// (`xrangeGenericCommand`, `t_stream.c`). `-`/`+` are min/max; a partial
    /// `ms` start gets seq 0 and a partial end gets seq UINT64_MAX; a `(` prefix
    /// makes the bound exclusive. A missing key replies an empty array.
    fn xrange_command(&mut self, argv: &[Vec<u8>], rev: bool) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(if rev { b"xrevrange" } else { b"xrange" });
        }
        let start_arg = if rev { &argv[3] } else { &argv[2] };
        let end_arg = if rev { &argv[2] } else { &argv[3] };

        let startid = match parse_stream_range_id(start_arg, 0) {
            Ok((id, exclusive)) => {
                if exclusive {
                    match id.incr() {
                        Some(id) => id,
                        None => return err(b"ERR invalid start ID for the interval"),
                    }
                } else {
                    id
                }
            }
            Err(frame) => return frame,
        };
        let endid = match parse_stream_range_id(end_arg, u64::MAX) {
            Ok((id, exclusive)) => {
                if exclusive {
                    match id.decr() {
                        Some(id) => id,
                        None => return err(b"ERR invalid end ID for the interval"),
                    }
                } else {
                    id
                }
            }
            Err(frame) => return frame,
        };

        let mut count: i64 = -1;
        let mut j = 4usize;
        while j < argv.len() {
            let additional = argv.len() - j - 1;
            if ascii_eq(&argv[j], b"COUNT") && additional >= 1 {
                let Some(n) = parse_i64(&argv[j + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                count = if n < 0 { 0 } else { n };
                j += 1;
            } else {
                return err(b"ERR syntax error");
            }
            j += 1;
        }

        let entries: Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)> =
            match self.get_value(&argv[1]).map(|entry| &entry.value) {
                Some(StoredValue::Stream(stream)) => {
                    if count == 0 {
                        return RespFrame::null_array();
                    }
                    if startid > endid {
                        Vec::new()
                    } else {
                        stream
                            .entries
                            .range(startid..=endid)
                            .map(|(id, fields)| (*id, fields.clone()))
                            .collect()
                    }
                }
                Some(_) => return wrong_type(),
                None => return RespFrame::array(Vec::new()),
            };

        let limit = if count <= 0 {
            entries.len()
        } else {
            (count as usize).min(entries.len())
        };
        let mut out = Vec::with_capacity(limit);
        let iter: Box<dyn Iterator<Item = &(StreamId, Vec<(Vec<u8>, Vec<u8>)>)>> = if rev {
            Box::new(entries.iter().rev())
        } else {
            Box::new(entries.iter())
        };
        for (id, fields) in iter.take(limit) {
            out.push(render_stream_entry(*id, fields));
        }
        RespFrame::array(out)
    }

    /// XDEL key id [id ...] (`xdelCommand`, `t_stream.c`): delete the named
    /// entries, return the count deleted, advance `max_deleted_id`.
    /// `entries_added` is NOT decremented.
    fn xdel_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"xdel");
        }
        let mut ids = Vec::with_capacity(argv.len() - 2);
        for raw in &argv[2..] {
            match parse_stream_id_strict(raw, 0) {
                Ok((id, _)) => ids.push(id),
                Err(frame) => return frame,
            }
        }
        self.purge_if_expired(&argv[1]);
        let stream = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Stream(stream),
                ..
            }) => stream,
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        let mut deleted = 0i64;
        let mut first_entry_removed = false;
        for id in ids {
            if stream.entries.remove(&id).is_some() {
                if id == stream.first_id {
                    first_entry_removed = true;
                }
                if id > stream.max_deleted_id {
                    stream.max_deleted_id = id;
                }
                deleted += 1;
            }
        }
        if deleted > 0 {
            if stream.entries.is_empty() {
                stream.first_id = StreamId::MIN;
            } else if first_entry_removed {
                stream.first_id = *stream.entries.keys().next().expect("non-empty");
            }
            self.note_write(&argv[1]);
        }
        RespFrame::integer(deleted)
    }

    /// XTRIM key MAXLEN|MINID [~|=] threshold [LIMIT n] (`xtrimCommand`,
    /// `t_stream.c`): trim the stream and return the number of entries removed.
    /// A missing key replies `:0`.
    fn xtrim_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"xtrim");
        }
        let mut trim: Option<StreamTrim> = None;
        let mut i = 2usize;
        while i < argv.len() {
            let opt = &argv[i];
            let moreargs = argv.len() - 1 - i;
            if ascii_eq(opt, b"MAXLEN") && moreargs >= 1 {
                if trim.is_some() {
                    return err(b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible");
                }
                match self.parse_trim(argv, &mut i, true) {
                    Ok(t) => trim = Some(t),
                    Err(frame) => return frame,
                }
            } else if ascii_eq(opt, b"MINID") && moreargs >= 1 {
                if trim.is_some() {
                    return err(b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible");
                }
                match self.parse_trim(argv, &mut i, false) {
                    Ok(t) => trim = Some(t),
                    Err(frame) => return frame,
                }
            } else if ascii_eq(opt, b"LIMIT") && moreargs >= 1 {
                if parse_i64(&argv[i + 1]).is_none() {
                    return err(b"ERR value is not an integer or out of range");
                }
                if let Some(t) = &mut trim {
                    t.limit_given = true;
                }
                i += 1;
            } else {
                return err(b"ERR syntax error");
            }
            i += 1;
        }
        let Some(trim) = trim else {
            return err(b"ERR syntax error, XTRIM must be called with a trimming strategy");
        };
        if trim.limit_given && !trim.approx {
            return err(b"ERR syntax error, LIMIT cannot be used without the special ~ option");
        }

        self.purge_if_expired(&argv[1]);
        let stream = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Stream(stream),
                ..
            }) => stream,
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        let deleted = apply_stream_trim(stream, &trim);
        if deleted > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(deleted as i64)
    }

    /// XSETID key id [ENTRIESADDED n] [MAXDELETEDID id] (`xsetidCommand`,
    /// `t_stream.c`): set the stream's last ID (and optionally entries_added /
    /// max_deleted_id), enforcing the monotonicity rules. Replies +OK.
    fn xsetid_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"xsetid");
        }
        let (id, _) = match parse_stream_id_strict(&argv[2], 0) {
            Ok(v) => v,
            Err(frame) => return frame,
        };
        let mut entries_added: Option<u64> = None;
        let mut max_xdel_id = StreamId::MIN;
        let mut i = 3usize;
        while i < argv.len() {
            let moreargs = argv.len() - 1 - i;
            let opt = &argv[i];
            if ascii_eq(opt, b"ENTRIESADDED") && moreargs >= 1 {
                let Some(n) = parse_i64(&argv[i + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if n < 0 {
                    return err(b"ERR entries_added must be positive");
                }
                entries_added = Some(n as u64);
                i += 2;
            } else if ascii_eq(opt, b"MAXDELETEDID") && moreargs >= 1 {
                let (mxid, _) = match parse_stream_id_strict(&argv[i + 1], 0) {
                    Ok(v) => v,
                    Err(frame) => return frame,
                };
                if id < mxid {
                    return err(b"ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id");
                }
                max_xdel_id = mxid;
                i += 2;
            } else {
                return err(b"ERR syntax error");
            }
        }

        self.purge_if_expired(&argv[1]);
        let stream = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Stream(stream),
                ..
            }) => stream,
            Some(_) => return wrong_type(),
            None => return err(b"ERR no such key"),
        };

        if id < stream.max_deleted_id {
            return err(b"ERR The ID specified in XSETID is smaller than current max_deleted_entry_id");
        }
        if !stream.entries.is_empty() {
            let maxid = *stream.entries.keys().next_back().expect("non-empty");
            if id < maxid {
                return err(
                    b"ERR The ID specified in XSETID is smaller than the target stream top item",
                );
            }
            if let Some(ea) = entries_added {
                if (stream.entries.len() as u64) > ea {
                    return err(b"ERR The entries_added specified in XSETID is smaller than the target stream length");
                }
            }
        }

        stream.last_id = id;
        if let Some(ea) = entries_added {
            stream.entries_added = ea;
        }
        if max_xdel_id != StreamId::MIN {
            stream.max_deleted_id = max_xdel_id;
        }
        self.note_write(&argv[1]);
        simple(b"OK")
    }

    /// XREAD [COUNT n] STREAMS key [key ...] id [id ...] (`xreadCommand`,
    /// `t_stream.c`, non-blocking path only). For each key, returns entries with
    /// ID greater than the supplied ID. `$` means the stream's last_id (so a
    /// non-blocking read yields nothing new). Reply: array of
    /// `[key, [[id,[fields...]]...]]`, or null array if nothing to serve.
    /// `BLOCK` is parsed but treated as a 0-timeout (non-blocking): if nothing
    /// can be served immediately, replies a null array.
    fn xread_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"xread");
        }
        let mut count: i64 = 0;
        let mut streams_arg: Option<usize> = None;
        let mut i = 1usize;
        while i < argv.len() {
            let moreargs = argv.len() - i - 1;
            let opt = &argv[i];
            if ascii_eq(opt, b"BLOCK") && moreargs >= 1 {
                if parse_i64(&argv[i + 1]).is_none() {
                    return err(b"ERR timeout is not an integer or out of range");
                }
                i += 1;
            } else if ascii_eq(opt, b"COUNT") && moreargs >= 1 {
                let Some(n) = parse_i64(&argv[i + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                count = if n < 0 { 0 } else { n };
                i += 1;
            } else if ascii_eq(opt, b"STREAMS") && moreargs >= 1 {
                streams_arg = Some(i + 1);
                break;
            } else {
                return err(b"ERR syntax error");
            }
            i += 1;
        }

        let Some(streams_arg) = streams_arg else {
            return err(b"ERR syntax error");
        };
        let remaining = argv.len() - streams_arg;
        if remaining == 0 || remaining % 2 != 0 {
            return err(b"ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.");
        }
        let streams_count = remaining / 2;

        let mut gts: Vec<StreamId> = Vec::with_capacity(streams_count);
        for k in 0..streams_count {
            let key = &argv[streams_arg + k];
            let id_arg = &argv[streams_arg + streams_count + k];
            if id_arg.len() == 1 && id_arg[0] == b'$' {
                self.purge_if_expired(key);
                let last = match self.db.get(key) {
                    Some(Entry {
                        value: StoredValue::Stream(stream),
                        ..
                    }) => stream.last_id,
                    Some(_) => return wrong_type(),
                    None => StreamId::MIN,
                };
                gts.push(last);
            } else if id_arg.len() == 1 && id_arg[0] == b'+' {
                self.purge_if_expired(key);
                let last = match self.db.get(key) {
                    Some(Entry {
                        value: StoredValue::Stream(stream),
                        ..
                    }) if !stream.entries.is_empty() => stream.last_id.decr().unwrap_or(StreamId::MIN),
                    Some(Entry {
                        value: StoredValue::Stream(_),
                        ..
                    }) => StreamId::MIN,
                    Some(_) => return wrong_type(),
                    None => StreamId::MIN,
                };
                gts.push(last);
            } else {
                match parse_stream_id_strict(id_arg, 0) {
                    Ok((id, _)) => gts.push(id),
                    Err(frame) => return frame,
                }
            }
        }

        let mut result = Vec::new();
        for k in 0..streams_count {
            let key = &argv[streams_arg + k];
            self.purge_if_expired(key);
            let stream = match self.db.get(key) {
                Some(Entry {
                    value: StoredValue::Stream(stream),
                    ..
                }) => stream,
                Some(_) => return wrong_type(),
                None => continue,
            };
            let gt = gts[k];
            let start = match gt.incr() {
                Some(id) => id,
                None => continue,
            };
            let mut entries = Vec::new();
            for (id, fields) in stream.entries.range(start..) {
                entries.push(render_stream_entry(*id, fields));
                if count > 0 && entries.len() as i64 == count {
                    break;
                }
            }
            if !entries.is_empty() {
                result.push(RespFrame::array(vec![
                    bulk(key),
                    RespFrame::array(entries),
                ]));
            }
        }

        if result.is_empty() {
            RespFrame::null_array()
        } else {
            RespFrame::array(result)
        }
    }

    /// XGROUP CREATE|SETID|DESTROY|CREATECONSUMER|DELCONSUMER (`xgroupCommand`,
    /// `t_stream.c`). Manages consumer-group lifecycle. The missing-key and
    /// missing-group checks mirror the C ordering exactly.
    fn xgroup_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"xgroup");
        }
        let subcommand = &argv[1];
        let create = ascii_eq(subcommand, b"CREATE");
        let setid = ascii_eq(subcommand, b"SETID");
        let destroy = ascii_eq(subcommand, b"DESTROY");
        let createconsumer = ascii_eq(subcommand, b"CREATECONSUMER");
        let delconsumer = ascii_eq(subcommand, b"DELCONSUMER");

        if !(create || setid || destroy || createconsumer || delconsumer) {
            return subcommand_syntax_error(b"XGROUP", subcommand);
        }
        if argv.len() < 4 {
            return subcommand_syntax_error(b"XGROUP", subcommand);
        }

        let mut mkstream = false;
        let mut entries_read: i64 = SCG_INVALID_ENTRIES_READ;
        let mut i = 5usize;
        while i < argv.len() {
            if create && ascii_eq(&argv[i], b"MKSTREAM") {
                mkstream = true;
                i += 1;
            } else if (create || setid) && ascii_eq(&argv[i], b"ENTRIESREAD") && i + 1 < argv.len() {
                let Some(n) = parse_i64(&argv[i + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                if n < 0 && n != SCG_INVALID_ENTRIES_READ {
                    return err(b"ERR value for ENTRIESREAD must be positive or -1");
                }
                entries_read = n;
                i += 2;
            } else {
                return subcommand_syntax_error(b"XGROUP", subcommand);
            }
        }

        let key = &argv[2];
        self.purge_if_expired(key);
        let key_exists = match self.db.get(key) {
            Some(Entry {
                value: StoredValue::Stream(_),
                ..
            }) => true,
            Some(_) => return wrong_type(),
            None => false,
        };
        let grpname = &argv[3];

        if !mkstream && !key_exists {
            return err(
                b"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
            );
        }

        if !mkstream && key_exists && (setid || createconsumer || delconsumer) {
            let group_exists = match self.db.get(key) {
                Some(Entry {
                    value: StoredValue::Stream(stream),
                    ..
                }) => stream.groups.contains_key(grpname.as_slice()),
                _ => false,
            };
            if !group_exists {
                let mut msg = b"NOGROUP No such consumer group '".to_vec();
                msg.extend_from_slice(grpname);
                msg.extend_from_slice(b"' for key name '");
                msg.extend_from_slice(key);
                msg.extend_from_slice(b"'");
                return err(&msg);
            }
        }

        if create {
            if argv.len() < 5 || argv.len() > 8 {
                return subcommand_syntax_error(b"XGROUP", subcommand);
            }
            let id = if argv[4].len() == 1 && argv[4][0] == b'$' {
                match self.db.get(key) {
                    Some(Entry {
                        value: StoredValue::Stream(stream),
                        ..
                    }) => stream.last_id,
                    _ => StreamId::MIN,
                }
            } else {
                match parse_stream_id_strict(&argv[4], 0) {
                    Ok((id, _)) => id,
                    Err(frame) => return frame,
                }
            };
            if !key_exists {
                self.db.insert(
                    key.clone(),
                    Entry {
                        value: StoredValue::Stream(StreamValue::default()),
                        expire_at_ms: None,
                    },
                );
            }
            let stream = match self.db.get_mut(key) {
                Some(Entry {
                    value: StoredValue::Stream(stream),
                    ..
                }) => stream,
                _ => unreachable!("stream just created or verified"),
            };
            if stream.groups.contains_key(grpname.as_slice()) {
                return err(b"BUSYGROUP Consumer Group name already exists");
            }
            stream.groups.insert(
                grpname.clone(),
                Group {
                    last_delivered_id: id,
                    entries_read,
                    ..Group::default()
                },
            );
            self.note_write(key);
            simple(b"OK")
        } else if setid {
            if argv.len() != 5 && argv.len() != 7 {
                return subcommand_syntax_error(b"XGROUP", subcommand);
            }
            let id = if argv[4].len() == 1 && argv[4][0] == b'$' {
                match self.db.get(key) {
                    Some(Entry {
                        value: StoredValue::Stream(stream),
                        ..
                    }) => stream.last_id,
                    _ => StreamId::MIN,
                }
            } else {
                match parse_stream_range_id_or_normal(&argv[4]) {
                    Ok(id) => id,
                    Err(frame) => return frame,
                }
            };
            let stream = self.stream_mut_unchecked(key);
            let group = stream.groups.get_mut(grpname.as_slice()).expect("verified");
            group.last_delivered_id = id;
            group.entries_read = entries_read;
            self.note_write(key);
            simple(b"OK")
        } else if destroy {
            if argv.len() != 4 {
                return subcommand_syntax_error(b"XGROUP", subcommand);
            }
            let stream = self.stream_mut_unchecked(key);
            let removed = stream.groups.remove(grpname.as_slice()).is_some();
            if removed {
                self.note_write(key);
            }
            RespFrame::integer(if removed { 1 } else { 0 })
        } else if createconsumer {
            if argv.len() != 5 {
                return subcommand_syntax_error(b"XGROUP", subcommand);
            }
            let now = self.host.now_millis();
            let stream = self.stream_mut_unchecked(key);
            let group = stream.groups.get_mut(grpname.as_slice()).expect("verified");
            let consumer_name = &argv[4];
            let created = if group.consumers.contains_key(consumer_name.as_slice()) {
                false
            } else {
                group.consumers.insert(
                    consumer_name.clone(),
                    Consumer {
                        seen_time_ms: now,
                        ..Consumer::default()
                    },
                );
                true
            };
            if created {
                self.note_write(key);
            }
            RespFrame::integer(if created { 1 } else { 0 })
        } else {
            if argv.len() != 5 {
                return subcommand_syntax_error(b"XGROUP", subcommand);
            }
            let stream = self.stream_mut_unchecked(key);
            let group = stream.groups.get_mut(grpname.as_slice()).expect("verified");
            let consumer_name = &argv[4];
            let pending = match group.consumers.remove(consumer_name.as_slice()) {
                Some(consumer) => {
                    let count = consumer.pending.len() as i64;
                    for id in &consumer.pending {
                        group.pending.remove(id);
                    }
                    count
                }
                None => 0,
            };
            if pending > 0 {
                self.note_write(key);
            }
            RespFrame::integer(pending)
        }
    }

    /// Borrow the stream at `key` mutably, asserting it exists and is a stream.
    /// Only call after the caller has already verified both conditions.
    fn stream_mut_unchecked(&mut self, key: &[u8]) -> &mut StreamValue {
        match self.db.get_mut(key) {
            Some(Entry {
                value: StoredValue::Stream(stream),
                ..
            }) => stream,
            _ => unreachable!("stream existence verified by caller"),
        }
    }

    /// XREADGROUP GROUP g c [COUNT n] [NOACK] STREAMS key... id...
    /// (`xreadCommand`, group path, `t_stream.c`). For id `>` delivers new
    /// messages after the group's last_delivered_id and records them in the PEL;
    /// for an explicit id serves the consumer's own history from the PEL.
    fn xreadgroup_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"xreadgroup");
        }
        let mut count: i64 = 0;
        let mut noack = false;
        let mut groupname: Option<Vec<u8>> = None;
        let mut consumername: Option<Vec<u8>> = None;
        let mut streams_arg: Option<usize> = None;
        let mut i = 1usize;
        while i < argv.len() {
            let moreargs = argv.len() - i - 1;
            let opt = &argv[i];
            if ascii_eq(opt, b"BLOCK") && moreargs >= 1 {
                if parse_i64(&argv[i + 1]).is_none() {
                    return err(b"ERR timeout is not an integer or out of range");
                }
                i += 1;
            } else if ascii_eq(opt, b"COUNT") && moreargs >= 1 {
                let Some(n) = parse_i64(&argv[i + 1]) else {
                    return err(b"ERR value is not an integer or out of range");
                };
                count = if n < 0 { 0 } else { n };
                i += 1;
            } else if ascii_eq(opt, b"GROUP") && moreargs >= 2 {
                groupname = Some(argv[i + 1].clone());
                consumername = Some(argv[i + 2].clone());
                i += 2;
            } else if ascii_eq(opt, b"NOACK") {
                noack = true;
            } else if ascii_eq(opt, b"STREAMS") && moreargs >= 1 {
                streams_arg = Some(i + 1);
                break;
            } else {
                return err(b"ERR syntax error");
            }
            i += 1;
        }

        let Some(streams_arg) = streams_arg else {
            return err(b"ERR syntax error");
        };
        let Some(groupname) = groupname else {
            return err(b"ERR Missing GROUP option for XREADGROUP");
        };
        let consumername = consumername.expect("set alongside groupname");

        let remaining = argv.len() - streams_arg;
        if remaining == 0 || remaining % 2 != 0 {
            return err(b"ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.");
        }
        let streams_count = remaining / 2;

        enum ReadId {
            New,
            History(StreamId),
        }
        let mut read_ids: Vec<ReadId> = Vec::with_capacity(streams_count);
        for k in 0..streams_count {
            let key = &argv[streams_arg + k];
            let id_arg = &argv[streams_arg + streams_count + k];
            self.purge_if_expired(key);
            let group_present = match self.db.get(key) {
                Some(Entry {
                    value: StoredValue::Stream(stream),
                    ..
                }) => stream.groups.contains_key(groupname.as_slice()),
                Some(_) => return wrong_type(),
                None => false,
            };
            if !group_present {
                let mut msg = b"NOGROUP No such key '".to_vec();
                msg.extend_from_slice(key);
                msg.extend_from_slice(b"' or consumer group '");
                msg.extend_from_slice(&groupname);
                msg.extend_from_slice(b"' in XREADGROUP with GROUP option");
                return err(&msg);
            }
            if id_arg.len() == 1 && id_arg[0] == b'$' {
                return err(
                    b"ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.",
                );
            } else if id_arg.len() == 1 && id_arg[0] == b'+' {
                return err(
                    b"ERR The + ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The + ID would just return an empty result set.",
                );
            } else if id_arg.len() == 1 && id_arg[0] == b'>' {
                read_ids.push(ReadId::New);
            } else {
                match parse_stream_id_strict(id_arg, 0) {
                    Ok((id, _)) => read_ids.push(ReadId::History(id)),
                    Err(frame) => return frame,
                }
            }
        }

        let now = self.host.now_millis();
        let mut result = Vec::new();
        for k in 0..streams_count {
            let key = argv[streams_arg + k].clone();
            match &read_ids[k] {
                ReadId::New => {
                    if let Some(frame) =
                        self.xreadgroup_serve_new(&key, &groupname, &consumername, count, noack, now)
                    {
                        result.push(frame);
                    }
                }
                ReadId::History(start_after) => {
                    let entries = self.xreadgroup_serve_history(
                        &key,
                        &groupname,
                        &consumername,
                        *start_after,
                        count,
                        now,
                    );
                    result.push(RespFrame::array(vec![bulk(&key), RespFrame::array(entries)]));
                }
            }
        }

        if result.is_empty() {
            RespFrame::null_array()
        } else {
            RespFrame::array(result)
        }
    }

    /// Serve the `>` (new messages) path for one stream in XREADGROUP. Delivers
    /// undelivered entries after the group's last_delivered_id, advances it,
    /// updates the group entries_read counter, and records the deliveries in the
    /// group + consumer PEL unless `noack`. Returns the `[key, entries]` frame
    /// when at least one entry was served, else `None`.
    fn xreadgroup_serve_new(
        &mut self,
        key: &[u8],
        groupname: &[u8],
        consumername: &[u8],
        count: i64,
        noack: bool,
        now: u64,
    ) -> Option<RespFrame> {
        let stream = self.stream_mut_unchecked(key);
        let group = stream.groups.get(groupname).expect("verified");
        let start = match group.last_delivered_id.incr() {
            Some(id) => id,
            None => return None,
        };
        let to_deliver: Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)> = stream
            .entries
            .range(start..)
            .take(if count > 0 { count as usize } else { usize::MAX })
            .map(|(id, fields)| (*id, fields.clone()))
            .collect();

        let group = stream.groups.get_mut(groupname).expect("verified");
        if !group.consumers.contains_key(consumername) {
            group.consumers.insert(
                consumername.to_vec(),
                Consumer {
                    seen_time_ms: now,
                    ..Consumer::default()
                },
            );
        }
        if let Some(consumer) = group.consumers.get_mut(consumername) {
            consumer.seen_time_ms = now;
        }

        if to_deliver.is_empty() {
            return None;
        }

        let mut out = Vec::with_capacity(to_deliver.len());
        let first_id = stream.first_id;
        let stream_entries_added = stream.entries_added;
        let stream_length = stream.entries.len() as u64;
        let stream_last_id = stream.last_id;
        let stream_max_deleted = stream.max_deleted_id;
        for (id, fields) in &to_deliver {
            out.push(render_stream_entry(*id, fields));
        }

        let group = stream.groups.get_mut(groupname).expect("verified");
        for (id, _) in &to_deliver {
            if *id > group.last_delivered_id {
                if group.entries_read != SCG_INVALID_ENTRIES_READ
                    && group.last_delivered_id >= first_id
                    && stream_max_deleted == StreamId::MIN
                {
                    group.entries_read += 1;
                } else if stream_entries_added != 0 {
                    group.entries_read = estimate_distance_from_first(
                        stream_entries_added,
                        stream_length,
                        first_id,
                        stream_last_id,
                        stream_max_deleted,
                        *id,
                    );
                }
                group.last_delivered_id = *id;
            }
            if !noack {
                if let Some(existing) = group.pending.get(id).cloned() {
                    if let Some(prev) = group.consumers.get_mut(&existing.consumer) {
                        prev.pending.remove(id);
                    }
                }
                group.pending.insert(
                    *id,
                    PendingEntry {
                        consumer: consumername.to_vec(),
                        delivery_time_ms: now,
                        delivery_count: 1,
                    },
                );
                if let Some(consumer) = group.consumers.get_mut(consumername) {
                    consumer.pending.insert(*id);
                    consumer.active_time_ms = now;
                }
            }
        }
        self.note_write(key);
        Some(RespFrame::array(vec![bulk(key), RespFrame::array(out)]))
    }

    /// Serve the history path (explicit id) for one stream in XREADGROUP,
    /// re-reading the consumer's own PEL with id > `start_after`
    /// (`streamReplyWithRangeFromConsumerPEL`, `t_stream.c`). Entries no longer
    /// present in the stream are emitted as `[id, nil]`.
    fn xreadgroup_serve_history(
        &mut self,
        key: &[u8],
        groupname: &[u8],
        consumername: &[u8],
        start_after: StreamId,
        count: i64,
        now: u64,
    ) -> Vec<RespFrame> {
        let stream = self.stream_mut_unchecked(key);
        let group = stream.groups.get_mut(groupname).expect("verified");
        if !group.consumers.contains_key(consumername) {
            group.consumers.insert(
                consumername.to_vec(),
                Consumer {
                    seen_time_ms: now,
                    ..Consumer::default()
                },
            );
        }
        if let Some(consumer) = group.consumers.get_mut(consumername) {
            consumer.seen_time_ms = now;
        }

        let pending_ids: Vec<StreamId> = match (group.consumers.get(consumername), start_after.incr()) {
            (Some(consumer), Some(range_start)) => consumer
                .pending
                .range(range_start..)
                .take(if count > 0 { count as usize } else { usize::MAX })
                .copied()
                .collect(),
            _ => Vec::new(),
        };

        let mut out = Vec::with_capacity(pending_ids.len());
        for id in pending_ids {
            match stream.entries.get(&id) {
                Some(fields) => {
                    out.push(render_stream_entry(id, fields));
                    if let Some(nack) = stream
                        .groups
                        .get_mut(groupname)
                        .expect("verified")
                        .pending
                        .get_mut(&id)
                    {
                        nack.delivery_time_ms = now;
                        nack.delivery_count += 1;
                    }
                }
                None => {
                    out.push(RespFrame::array(vec![
                        bulk(id.to_string_bytes()),
                        RespFrame::null_array(),
                    ]));
                }
            }
        }
        out
    }

    /// XACK key group id... (`xackCommand`, `t_stream.c`): remove the named IDs
    /// from the group PEL and their owning consumer's PEL, returning the count
    /// actually acknowledged. Missing key/group → `:0`.
    fn xack_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"xack");
        }
        let mut ids = Vec::with_capacity(argv.len() - 3);
        for raw in &argv[3..] {
            match parse_stream_id_strict(raw, 0) {
                Ok((id, _)) => ids.push(id),
                Err(frame) => return frame,
            }
        }
        self.purge_if_expired(&argv[1]);
        let stream = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::Stream(stream),
                ..
            }) => stream,
            Some(_) => return wrong_type(),
            None => return RespFrame::integer(0),
        };
        let Some(group) = stream.groups.get_mut(argv[2].as_slice()) else {
            return RespFrame::integer(0);
        };
        let mut acknowledged = 0i64;
        for id in ids {
            if let Some(nack) = group.pending.remove(&id) {
                if let Some(consumer) = group.consumers.get_mut(&nack.consumer) {
                    consumer.pending.remove(&id);
                }
                acknowledged += 1;
            }
        }
        if acknowledged > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(acknowledged)
    }

    /// XPENDING key group (`xpendingCommand`, summary form, `t_stream.c`):
    /// `[total, min-id, max-id, [[consumer, count]...]]`, or `[0, nil, nil, nil]`
    /// when the PEL is empty. The extended IDLE/start/end/count/consumer form is
    /// deferred (it exposes consumer idle time = host clock).
    fn xpending_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"xpending");
        }
        if argv.len() != 3 && (argv.len() < 6 || argv.len() > 9) {
            return err(b"ERR syntax error");
        }
        self.purge_if_expired(&argv[1]);
        let group = match self.db.get(&argv[1]) {
            Some(Entry {
                value: StoredValue::Stream(stream),
                ..
            }) => stream.groups.get(argv[2].as_slice()),
            Some(_) => return wrong_type(),
            None => None,
        };
        let Some(group) = group else {
            let mut msg = b"NOGROUP No such key '".to_vec();
            msg.extend_from_slice(&argv[1]);
            msg.extend_from_slice(b"' or consumer group '");
            msg.extend_from_slice(&argv[2]);
            msg.extend_from_slice(b"'");
            return err(&msg);
        };
        if argv.len() != 3 {
            return err(
                b"ERR The extended XPENDING form is not supported by this engine in this wave",
            );
        }
        let total = group.pending.len();
        if total == 0 {
            return RespFrame::array(vec![
                RespFrame::integer(0),
                RespFrame::null_bulk(),
                RespFrame::null_bulk(),
                RespFrame::null_array(),
            ]);
        }
        let min_id = *group.pending.keys().next().expect("non-empty");
        let max_id = *group.pending.keys().next_back().expect("non-empty");
        let mut consumer_counts: std::collections::BTreeMap<&[u8], usize> =
            std::collections::BTreeMap::new();
        for nack in group.pending.values() {
            *consumer_counts.entry(nack.consumer.as_slice()).or_insert(0) += 1;
        }
        let consumers: Vec<RespFrame> = consumer_counts
            .into_iter()
            .map(|(name, count)| {
                RespFrame::array(vec![
                    bulk(name),
                    bulk(count.to_string().into_bytes()),
                ])
            })
            .collect();
        RespFrame::array(vec![
            RespFrame::integer(total as i64),
            bulk(min_id.to_string_bytes()),
            bulk(max_id.to_string_bytes()),
            RespFrame::array(consumers),
        ])
    }

    /// XINFO STREAM|GROUPS (`xinfoCommand`, `t_stream.c`). CONSUMERS is deferred
    /// (it exposes idle/inactive = host clock). STREAM FULL is deferred.
    fn xinfo_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"xinfo");
        }
        let subcommand = &argv[1];
        if ascii_eq(subcommand, b"STREAM") {
            if argv.len() < 3 {
                return subcommand_syntax_error(b"XINFO", subcommand);
            }
            if argv.len() > 3 {
                return err(
                    b"ERR XINFO STREAM FULL is not supported by this engine in this wave",
                );
            }
            self.purge_if_expired(&argv[2]);
            let stream = match self.db.get(&argv[2]) {
                Some(Entry {
                    value: StoredValue::Stream(stream),
                    ..
                }) => stream,
                Some(_) => return wrong_type(),
                None => return err(b"ERR no such key"),
            };
            self.xinfo_stream_reply(stream)
        } else if ascii_eq(subcommand, b"GROUPS") {
            if argv.len() != 3 {
                return subcommand_syntax_error(b"XINFO", subcommand);
            }
            self.purge_if_expired(&argv[2]);
            let stream = match self.db.get(&argv[2]) {
                Some(Entry {
                    value: StoredValue::Stream(stream),
                    ..
                }) => stream,
                Some(_) => return wrong_type(),
                None => return err(b"ERR no such key"),
            };
            self.xinfo_groups_reply(stream)
        } else if ascii_eq(subcommand, b"CONSUMERS") {
            err(b"ERR XINFO CONSUMERS is not supported by this engine in this wave")
        } else {
            subcommand_syntax_error(b"XINFO", subcommand)
        }
    }

    /// Build the XINFO STREAM map (RESP2 flat array). `radix-tree-keys` /
    /// `radix-tree-nodes` are modelled for the single-listpack-node case that a
    /// flat BTreeMap represents: 0 entries → keys 0 / nodes 1, else keys 1 /
    /// nodes 2 (matching valkey for streams that fit one listpack node).
    fn xinfo_stream_reply(&self, stream: &StreamValue) -> RespFrame {
        let length = stream.entries.len();
        let (radix_keys, radix_nodes) = if length == 0 { (0, 1) } else { (1, 2) };
        let first_entry = match stream.entries.iter().next() {
            Some((id, fields)) => render_stream_entry(*id, fields),
            None => RespFrame::null_bulk(),
        };
        let last_entry = match stream.entries.iter().next_back() {
            Some((id, fields)) => render_stream_entry(*id, fields),
            None => RespFrame::null_bulk(),
        };
        RespFrame::array(vec![
            bulk(b"length"),
            RespFrame::integer(length as i64),
            bulk(b"radix-tree-keys"),
            RespFrame::integer(radix_keys),
            bulk(b"radix-tree-nodes"),
            RespFrame::integer(radix_nodes),
            bulk(b"last-generated-id"),
            bulk(stream.last_id.to_string_bytes()),
            bulk(b"max-deleted-entry-id"),
            bulk(stream.max_deleted_id.to_string_bytes()),
            bulk(b"entries-added"),
            RespFrame::integer(stream.entries_added as i64),
            bulk(b"recorded-first-entry-id"),
            bulk(stream.first_id.to_string_bytes()),
            bulk(b"groups"),
            RespFrame::integer(stream.groups.len() as i64),
            bulk(b"first-entry"),
            first_entry,
            bulk(b"last-entry"),
            last_entry,
        ])
    }

    /// Build the XINFO GROUPS reply (array of per-group maps). Each group:
    /// name, consumers, pending, last-delivered-id, entries-read, lag — all
    /// deterministic (entries-read/lag derived from the same counters valkey uses).
    fn xinfo_groups_reply(&self, stream: &StreamValue) -> RespFrame {
        let mut names: Vec<&Vec<u8>> = stream.groups.keys().collect();
        names.sort();
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let group = &stream.groups[name];
            let entries_read = if group.entries_read != SCG_INVALID_ENTRIES_READ {
                RespFrame::integer(group.entries_read)
            } else {
                RespFrame::null_bulk()
            };
            let lag = stream_cg_lag(stream, group);
            out.push(RespFrame::array(vec![
                bulk(b"name"),
                bulk(name),
                bulk(b"consumers"),
                RespFrame::integer(group.consumers.len() as i64),
                bulk(b"pending"),
                RespFrame::integer(group.pending.len() as i64),
                bulk(b"last-delivered-id"),
                bulk(group.last_delivered_id.to_string_bytes()),
                bulk(b"entries-read"),
                entries_read,
                bulk(b"lag"),
                lag,
            ]));
        }
        RespFrame::array(out)
    }

    /// SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC]
    /// [ALPHA] [STORE destination], and SORT_RO (same minus STORE).
    /// `sortCommandGeneric` (`sort.c`). Sorts the multiset of elements held by a
    /// LIST, SET, or ZSET. Default is an ascending numeric sort: each element is
    /// parsed as a double, and any non-numeric element (without ALPHA) yields
    /// "ERR One or more scores can't be converted into double". ALPHA switches to
    /// a lexicographic byte sort; DESC reverses; numeric ties (and ALPHA ties)
    /// break on the element bytes for determinism. BY substitutes the first `*`
    /// in the pattern with the element to form a weight key (a hash field via
    /// `key->field`); a missing weight is score 0 (numeric) / NULL (alpha, sorts
    /// first); a BY pattern with no `*` means "don't sort" (native order). GET
    /// emits the value of the substituted key per element, `GET #` emits the
    /// element itself, missing GET keys reply nil. LIMIT slices after sorting.
    /// STORE writes the result list and replies its length, deleting the
    /// destination when the result is empty. SORT_RO rejects STORE (it is not a
    /// recognized token there, so it falls through to the syntax error).
    fn sort_command(&mut self, argv: &[Vec<u8>], readonly: bool) -> RespFrame {
        let cmd_name: &[u8] = if readonly { b"sort_ro" } else { b"sort" };
        if argv.len() < 2 {
            return wrong_arity(cmd_name);
        }

        let mut desc = false;
        let mut alpha = false;
        let mut limit_start: i64 = 0;
        let mut limit_count: i64 = -1;
        let mut dontsort = false;
        let mut sortby: Option<Vec<u8>> = None;
        let mut storekey: Option<Vec<u8>> = None;
        let mut operations: Vec<Vec<u8>> = Vec::new();

        let mut j = 2usize;
        while j < argv.len() {
            let leftargs = argv.len() - j - 1;
            let arg = &argv[j];
            if ascii_eq(arg, b"asc") {
                desc = false;
            } else if ascii_eq(arg, b"desc") {
                desc = true;
            } else if ascii_eq(arg, b"alpha") {
                alpha = true;
            } else if ascii_eq(arg, b"limit") && leftargs >= 2 {
                let start = match parse_i64(&argv[j + 1]) {
                    Some(n) => n,
                    None => return err(b"ERR value is not an integer or out of range"),
                };
                let count = match parse_i64(&argv[j + 2]) {
                    Some(n) => n,
                    None => return err(b"ERR value is not an integer or out of range"),
                };
                limit_start = start;
                limit_count = count;
                j += 2;
            } else if !readonly && ascii_eq(arg, b"store") && leftargs >= 1 {
                storekey = Some(argv[j + 1].clone());
                j += 1;
            } else if ascii_eq(arg, b"by") && leftargs >= 1 {
                let by_arg = &argv[j + 1];
                if !by_arg.contains(&b'*') {
                    dontsort = true;
                }
                sortby = Some(argv[j + 1].clone());
                j += 1;
            } else if ascii_eq(arg, b"get") && leftargs >= 1 {
                operations.push(argv[j + 1].clone());
                j += 1;
            } else {
                return err(b"ERR syntax error");
            }
            j += 1;
        }

        let key = &argv[1];
        self.purge_if_expired(key);

        // Validate type and collect the element multiset into `elements`.
        let elements: Vec<Vec<u8>> = match self.db.get(key).map(|entry| &entry.value) {
            None => Vec::new(),
            Some(StoredValue::List(items)) => items.iter().cloned().collect(),
            Some(StoredValue::Set(items)) => items.iter().cloned().collect(),
            Some(StoredValue::ZSet(members)) => members.keys().cloned().collect(),
            Some(_) => return wrong_type(),
        };

        let is_set = matches!(
            self.db.get(key).map(|entry| &entry.value),
            Some(StoredValue::Set(_))
        );
        let is_list = matches!(
            self.db.get(key).map(|entry| &entry.value),
            Some(StoredValue::List(_))
        );
        let is_zset = matches!(
            self.db.get(key).map(|entry| &entry.value),
            Some(StoredValue::ZSet(_))
        );

        // `dontsort` override: a SET sorted in native (hash) order is
        // non-deterministic across scripting/replication, so the C code forces
        // ALPHA when the result would be stored. The engine has no script
        // context, so only the STORE arm of `(storekey || script)` applies.
        if dontsort && is_set && storekey.is_some() {
            dontsort = false;
            alpha = true;
            sortby = None;
        }

        let getop = std::mem::take(&mut operations);

        // Build the sort vector in native order. For a ZSet, native order is
        // ascending-by-score; we approximate the C skiplist walk by sorting on
        // score (stored members are unique so ties cannot occur here for a real
        // SORT path because `dontsort` ZSets are rare; numeric/alpha SORT
        // re-sorts the vector anyway).
        let mut vector: Vec<Vec<u8>> = if is_zset {
            let mut scored: Vec<(f64, Vec<u8>)> = match self.db.get(key).map(|e| &e.value) {
                Some(StoredValue::ZSet(members)) => members
                    .iter()
                    .map(|(member, score)| (*score, member.clone()))
                    .collect(),
                _ => Vec::new(),
            };
            scored.sort_by(|(sa, ma), (sb, mb)| {
                sa.partial_cmp(sb)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| ma.cmp(mb))
            });
            scored.into_iter().map(|(_, m)| m).collect()
        } else {
            // List preserves push order; Set's iteration order is sorted below
            // when it actually participates in a sort, and a SET with `dontsort`
            // and no STORE is the only native-order case (deterministic only as
            // far as our fixtures avoid it).
            elements
        };
        let _ = is_set;

        let vectorlen = vector.len() as i64;

        // LIMIT sanity (mirrors the C clamping arithmetic exactly).
        let start = limit_start.max(0).min(vectorlen);
        let limit_count = limit_count.max(-1).min(vectorlen);
        let end0 = if limit_count < 0 {
            vectorlen - 1
        } else {
            start + limit_count - 1
        };
        let (start, mut end) = if start >= vectorlen {
            (vectorlen - 1, vectorlen - 2)
        } else {
            (start, end0)
        };
        if end >= vectorlen {
            end = vectorlen - 1;
        }

        // For a list/zset with `dontsort`, honor DESC by reversing the native
        // order. (Numeric/alpha sorts ignore `desc` here; the comparator applies
        // it.) This matches the C direct-load path for lists and zsets.
        if dontsort && (is_list || is_zset) && desc {
            vector.reverse();
        }

        // ── Load scores / compare keys, then sort ──────────────────────────
        let mut int_conversion_error = false;
        // Per-element comparison key. `Numeric(f64)` for the default sort,
        // `Alpha(Option<Vec<u8>>)` for ALPHA (None == missing BY weight).
        enum Cmp {
            Numeric(f64),
            Alpha(Option<Vec<u8>>),
        }
        let mut keyed: Vec<(Cmp, Vec<u8>)> = Vec::with_capacity(vector.len());

        if !dontsort {
            for element in vector.iter() {
                let byval: Option<Vec<u8>> = if let Some(by) = &sortby {
                    self.sort_lookup_by_pattern(by, element)
                } else {
                    Some(element.clone())
                };

                let cmp = if alpha {
                    if sortby.is_some() {
                        Cmp::Alpha(byval)
                    } else {
                        Cmp::Alpha(Some(element.clone()))
                    }
                } else {
                    match byval {
                        None => Cmp::Numeric(0.0),
                        Some(bytes) => match parse_score(&bytes) {
                            Some(f) => Cmp::Numeric(f),
                            None => {
                                int_conversion_error = true;
                                Cmp::Numeric(0.0)
                            }
                        },
                    }
                };
                keyed.push((cmp, element.clone()));
            }

            keyed.sort_by(|(ca, ea), (cb, eb)| {
                let ord = match (ca, cb) {
                    (Cmp::Numeric(a), Cmp::Numeric(b)) => match a.partial_cmp(b) {
                        Some(o) if o != std::cmp::Ordering::Equal => o,
                        _ => ea.cmp(eb),
                    },
                    (Cmp::Alpha(a), Cmp::Alpha(b)) => match (a, b) {
                        (None, None) => std::cmp::Ordering::Equal,
                        (None, Some(_)) => std::cmp::Ordering::Less,
                        (Some(_), None) => std::cmp::Ordering::Greater,
                        (Some(a), Some(b)) => a.cmp(b),
                    },
                    _ => std::cmp::Ordering::Equal,
                };
                if desc {
                    ord.reverse()
                } else {
                    ord
                }
            });
            vector = keyed.into_iter().map(|(_, e)| e).collect();
        }

        // ── Compute output length ──────────────────────────────────────────
        let range_len = if end >= start { (end - start + 1) as usize } else { 0 };
        let getn = getop.len();
        let outputlen = if getn > 0 { getn * range_len } else { range_len };

        if int_conversion_error {
            return err(b"ERR One or more scores can't be converted into double");
        }

        if storekey.is_none() {
            let mut items: Vec<RespFrame> = Vec::with_capacity(outputlen);
            if range_len > 0 {
                for idx in start..=end {
                    let element = &vector[idx as usize];
                    if getn == 0 {
                        items.push(bulk(element));
                    }
                    for pattern in &getop {
                        match self.sort_lookup_by_pattern(pattern, element) {
                            Some(val) => items.push(bulk(&val)),
                            None => items.push(RespFrame::null_bulk()),
                        }
                    }
                }
            }
            RespFrame::array(items)
        } else {
            let store_key = storekey.expect("storekey present");
            let mut result: VecDeque<Vec<u8>> = VecDeque::with_capacity(outputlen);
            if range_len > 0 {
                for idx in start..=end {
                    let element = &vector[idx as usize];
                    if getn == 0 {
                        result.push_back(element.clone());
                    } else {
                        for pattern in &getop {
                            match self.sort_lookup_by_pattern(pattern, element) {
                                Some(val) => result.push_back(val),
                                None => result.push_back(Vec::new()),
                            }
                        }
                    }
                }
            }

            if !result.is_empty() {
                let entry = Entry {
                    value: StoredValue::List(result),
                    expire_at_ms: None,
                };
                self.db.insert(store_key.clone(), entry);
                self.note_write(&store_key);
            } else if self.db.remove(&store_key).is_some() {
                self.note_write(&store_key);
            }
            RespFrame::integer(outputlen as i64)
        }
    }

    /// `lookupKeyByPattern` (`sort.c`): resolve a BY/GET pattern against one
    /// element. `#` returns the element itself. Otherwise the first `*` is
    /// replaced by the element; if a `->field` suffix is present the substituted
    /// key is dereferenced as a hash field, else it is read as a plain string
    /// value. Returns `None` when the pattern has no `*`, the key is absent, the
    /// type is wrong, or the hash field is missing.
    fn sort_lookup_by_pattern(&mut self, pattern: &[u8], subst: &[u8]) -> Option<Vec<u8>> {
        if pattern == b"#" {
            return Some(subst.to_vec());
        }
        let star = pattern.iter().position(|&b| b == b'*')?;
        let prefix = &pattern[..star];
        let after = &pattern[star + 1..];

        // Detect a `->field` hash dereference in the bytes after the `*`.
        let (postfix, field): (&[u8], Option<&[u8]>) =
            match after.windows(2).position(|w| w == b"->") {
                Some(arrow) if arrow + 2 < after.len() => {
                    (&after[..arrow], Some(&after[arrow + 2..]))
                }
                _ => (after, None),
            };

        let mut keybytes = Vec::with_capacity(prefix.len() + subst.len() + postfix.len());
        keybytes.extend_from_slice(prefix);
        keybytes.extend_from_slice(subst);
        keybytes.extend_from_slice(postfix);

        self.purge_if_expired(&keybytes);
        match (self.db.get(&keybytes).map(|e| &e.value), field) {
            (Some(StoredValue::Hash(fields)), Some(field_name)) => {
                fields.get(field_name).map(|f| f.value.clone())
            }
            (Some(StoredValue::String(value)), None) => Some(value.clone()),
            _ => None,
        }
    }

    /// TYPE key (`typeCommand`, `db.c`): a simple-string naming the value's
    /// type, or `none` for a missing key. Only the variants the engine models
    /// today are reachable; later type waves extend `StoredValue::type_name`.
    fn type_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"type");
        }
        self.purge_if_expired(&argv[1]);
        match self.db.get(&argv[1]) {
            Some(entry) => simple(entry.value.type_name()),
            None => simple(b"none"),
        }
    }

    /// KEYS pattern (`keysCommand`, `db.c`). Returns an unordered array of the
    /// keys matching `pattern` under valkey's `stringmatchlen` glob semantics
    /// (`*`, `?`, `[...]` classes with `^` negation, `a-z` ranges, `\` escape;
    /// case-sensitive here). Expired keys are skipped without being purged as a
    /// side effect of the scan. `*` short-circuits to "all keys" exactly as C's
    /// `allkeys` fast path. Read-only: never calls `note_write`.
    fn keys_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"keys");
        }
        let pattern = &argv[1];
        let allkeys = pattern.as_slice() == b"*";
        let now = self.host.now_millis();
        let mut matches = Vec::new();
        for (key, entry) in &self.db {
            if entry
                .expire_at_ms
                .is_some_and(|deadline| deadline <= now)
            {
                continue;
            }
            if allkeys || string_match_len(pattern, key) {
                matches.push(bulk(key));
            }
        }
        RespFrame::array(matches)
    }

    /// SCAN cursor [MATCH pattern] [COUNT n] [TYPE type] (`scanCommand` →
    /// `scanGenericCommand`, `db.c`). The engine stores the keyspace in a
    /// `HashMap` with no stable dict cursor, so this is a single-pass scan: it
    /// returns cursor `"0"` (a bulk string) plus every matching key in one call.
    /// That is valid SCAN semantics — the reference also completes in one pass
    /// (cursor 0) for any collection smaller than `COUNT`. `COUNT` is parsed and
    /// validated (`< 1` → syntax error) but otherwise ignored. `MATCH` applies
    /// the `stringmatchlen` glob to the key name; `TYPE` filters by
    /// `type_name`. Expired keys are skipped (mirroring `keysCommand`).
    /// Cursor-parse precedes option-parse, matching the C order. Read-only.
    fn scan_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"scan");
        }
        if parse_scan_cursor(&argv[1]).is_none() {
            return err(b"ERR invalid cursor");
        }
        let opts = match parse_scan_options(&argv[2..], CollectionScan::Keyspace) {
            Ok(opts) => opts,
            Err(frame) => return frame,
        };
        let now = self.host.now_millis();
        let mut items = Vec::new();
        for (key, entry) in &self.db {
            if entry
                .expire_at_ms
                .is_some_and(|deadline| deadline <= now)
            {
                continue;
            }
            if let Some(type_filter) = &opts.type_filter {
                if entry.value.type_name() != type_filter.as_slice() {
                    continue;
                }
            }
            if let Some(pattern) = &opts.pattern {
                if !string_match_len(pattern, key) {
                    continue;
                }
            }
            items.push(bulk(key));
        }
        scan_reply(items)
    }

    /// HSCAN/SSCAN/ZSCAN key cursor [MATCH pattern] [COUNT n] [NOVALUES|NOSCORES]
    /// (`hscanCommand`/`sscanCommand`/`zscanCommand` → `scanGenericCommand`,
    /// `t_hash.c`/`t_set.c`/`t_zset.c`). Single-pass over the collection: returns
    /// cursor `"0"` plus every matching element in one call, the same shape the
    /// reference produces for any listpack-encoded (small) collection. The cursor
    /// is parsed before the key lookup, matching the C order; a missing key
    /// replies the shared `emptyscan` frame `["0", []]`; a wrong-typed key is
    /// WRONGTYPE. `MATCH` applies to the FIELD (HSCAN) / MEMBER (SSCAN/ZSCAN).
    /// HSCAN emits `field, value` pairs (NOVALUES → fields only); SSCAN emits
    /// members; ZSCAN emits `member, score` pairs with the score formatted like
    /// ZSCORE (NOSCORES → members only). Read-only.
    fn collection_scan_command(&mut self, argv: &[Vec<u8>], kind: CollectionScan) -> RespFrame {
        let name: &[u8] = match kind {
            CollectionScan::Hash => b"hscan",
            CollectionScan::Set => b"sscan",
            CollectionScan::ZSet => b"zscan",
            CollectionScan::Keyspace => unreachable!(),
        };
        if argv.len() < 3 {
            return wrong_arity(name);
        }
        if parse_scan_cursor(&argv[2]).is_none() {
            return err(b"ERR invalid cursor");
        }
        if matches!(kind, CollectionScan::Hash) {
            self.purge_expired_fields(&argv[1]);
        } else {
            self.purge_if_expired(&argv[1]);
        }
        let well_typed = match self.db.get(&argv[1]) {
            None => return scan_reply(Vec::new()),
            Some(entry) => matches!(
                (&entry.value, kind),
                (StoredValue::Hash(_), CollectionScan::Hash)
                    | (StoredValue::Set(_), CollectionScan::Set)
                    | (StoredValue::ZSet(_), CollectionScan::ZSet)
            ),
        };
        if !well_typed {
            return wrong_type();
        }
        let opts = match parse_scan_options(&argv[3..], kind) {
            Ok(opts) => opts,
            Err(frame) => return frame,
        };
        let entry = self
            .db
            .get(&argv[1])
            .expect("key presence verified before option parse");
        let mut items = Vec::new();
        match (&entry.value, kind) {
            (StoredValue::Hash(fields), CollectionScan::Hash) => {
                for (field, field_value) in fields {
                    if let Some(pattern) = &opts.pattern {
                        if !string_match_len(pattern, field) {
                            continue;
                        }
                    }
                    items.push(bulk(field));
                    if !opts.only_keys {
                        items.push(bulk(&field_value.value));
                    }
                }
            }
            (StoredValue::Set(members), CollectionScan::Set) => {
                for member in members {
                    if let Some(pattern) = &opts.pattern {
                        if !string_match_len(pattern, member) {
                            continue;
                        }
                    }
                    items.push(bulk(member));
                }
            }
            (StoredValue::ZSet(members), CollectionScan::ZSet) => {
                for (member, score) in members {
                    if let Some(pattern) = &opts.pattern {
                        if !string_match_len(pattern, member) {
                            continue;
                        }
                    }
                    items.push(bulk(member));
                    if !opts.only_keys {
                        items.push(bulk(format_score(*score)));
                    }
                }
            }
            _ => unreachable!("collection type verified before option parse"),
        }
        scan_reply(items)
    }

    /// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN] (`lcsCommand`,
    /// `t_string.c`). Computes the longest common subsequence of two string
    /// values (a missing key is treated as the empty string). A non-string key
    /// yields "ERR The specified keys must contain string values". Plain replies
    /// the LCS bulk string; LEN replies its length; IDX replies the match-index
    /// map `{matches: [...], len: N}`. MINMATCHLEN filters short matches and
    /// WITHMATCHLEN appends each match's length. The RESP shape is copied
    /// byte-for-byte from the C deferred-reply nesting.
    fn lcs_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"lcs");
        }
        self.purge_if_expired(&argv[1]);
        self.purge_if_expired(&argv[2]);
        let a = match self.db.get(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => value.clone(),
            Some(_) => {
                return err(b"ERR The specified keys must contain string values");
            }
            None => Vec::new(),
        };
        let b = match self.db.get(&argv[2]).map(|entry| &entry.value) {
            Some(StoredValue::String(value)) => value.clone(),
            Some(_) => {
                return err(b"ERR The specified keys must contain string values");
            }
            None => Vec::new(),
        };

        let mut minmatchlen: i64 = 0;
        let mut getlen = false;
        let mut getidx = false;
        let mut withmatchlen = false;
        let mut j = 3;
        while j < argv.len() {
            let opt = &argv[j];
            let moreargs = argv.len() - 1 - j;
            if ascii_eq(opt, b"IDX") {
                getidx = true;
            } else if ascii_eq(opt, b"LEN") {
                getlen = true;
            } else if ascii_eq(opt, b"WITHMATCHLEN") {
                withmatchlen = true;
            } else if ascii_eq(opt, b"MINMATCHLEN") && moreargs > 0 {
                match parse_i64(&argv[j + 1]) {
                    Some(n) => minmatchlen = n.max(0),
                    None => return err(b"ERR value is not an integer or out of range"),
                }
                j += 1;
            } else {
                return err(b"ERR syntax error");
            }
            j += 1;
        }

        if getlen && getidx {
            return err(
                b"ERR If you want both the length and indexes, please just use IDX.",
            );
        }

        let alen = a.len();
        let blen = b.len();
        let stride = blen + 1;
        let mut lcs = vec![0u32; (alen + 1) * stride];
        let at = |i: usize, j: usize| i * stride + j;
        for i in 1..=alen {
            for k in 1..=blen {
                lcs[at(i, k)] = if a[i - 1] == b[k - 1] {
                    lcs[at(i - 1, k - 1)] + 1
                } else {
                    lcs[at(i - 1, k)].max(lcs[at(i, k - 1)])
                };
            }
        }

        let total_len = lcs[at(alen, blen)];
        let computelcs = getidx || !getlen;
        let mut result = vec![0u8; total_len as usize];
        let mut idx = total_len as usize;

        let mut ranges: Vec<RespFrame> = Vec::new();
        let arange_unset = alen;
        let mut arange_start = arange_unset;
        let mut arange_end = 0usize;
        let mut brange_start = 0usize;
        let mut brange_end = 0usize;

        let mut i = alen;
        let mut k = blen;
        while computelcs && i > 0 && k > 0 {
            let mut emit_range = false;
            if a[i - 1] == b[k - 1] {
                result[idx - 1] = a[i - 1];
                if arange_start == arange_unset {
                    arange_start = i - 1;
                    arange_end = i - 1;
                    brange_start = k - 1;
                    brange_end = k - 1;
                } else if arange_start == i && brange_start == k {
                    arange_start -= 1;
                    brange_start -= 1;
                } else {
                    emit_range = true;
                }
                if arange_start == 0 || brange_start == 0 {
                    emit_range = true;
                }
                idx -= 1;
                i -= 1;
                k -= 1;
            } else {
                if lcs[at(i - 1, k)] > lcs[at(i, k - 1)] {
                    i -= 1;
                } else {
                    k -= 1;
                }
                if arange_start != arange_unset {
                    emit_range = true;
                }
            }

            if emit_range {
                let match_len = arange_end - arange_start + 1;
                if minmatchlen == 0 || match_len as i64 >= minmatchlen {
                    if getidx {
                        let mut range = vec![
                            RespFrame::array(vec![
                                RespFrame::integer(arange_start as i64),
                                RespFrame::integer(arange_end as i64),
                            ]),
                            RespFrame::array(vec![
                                RespFrame::integer(brange_start as i64),
                                RespFrame::integer(brange_end as i64),
                            ]),
                        ];
                        if withmatchlen {
                            range.push(RespFrame::integer(match_len as i64));
                        }
                        ranges.push(RespFrame::array(range));
                    }
                }
                arange_start = arange_unset;
            }
        }

        if getidx {
            RespFrame::Map(vec![
                (bulk(b"matches"), RespFrame::array(ranges)),
                (bulk(b"len"), RespFrame::integer(total_len as i64)),
            ])
        } else if getlen {
            RespFrame::integer(total_len as i64)
        } else {
            bulk(result)
        }
    }

    /// RENAME / RENAMENX (`renameGenericCommand`, `db.c`). The source value and
    /// its TTL move to the destination, overwriting any existing destination
    /// (RENAME) unless RENAMENX finds the destination already present. A missing
    /// source replies "ERR no such key". src == dst is a no-op that still
    /// validates the source exists.
    fn rename_command(&mut self, argv: &[Vec<u8>], nx: bool) -> RespFrame {
        if argv.len() != 3 {
            return wrong_arity(if nx { b"renamenx" } else { b"rename" });
        }
        let samekey = argv[1] == argv[2];
        self.purge_if_expired(&argv[1]);
        if !self.db.contains_key(&argv[1]) {
            return err(b"ERR no such key");
        }
        if samekey {
            return if nx {
                RespFrame::integer(0)
            } else {
                simple(b"OK")
            };
        }
        self.purge_if_expired(&argv[2]);
        if self.db.contains_key(&argv[2]) && nx {
            return RespFrame::integer(0);
        }
        let entry = self.db.remove(&argv[1]).expect("source verified present");
        self.db.insert(argv[2].clone(), entry);
        self.note_write(&argv[1]);
        self.note_write(&argv[2]);
        if nx {
            RespFrame::integer(1)
        } else {
            simple(b"OK")
        }
    }

    /// COPY src dst [REPLACE] (`copyCommand`, `db.c`). Deep-copies the source
    /// value and its TTL to the destination. Replies :0 when the source is
    /// missing, or when the destination already exists without REPLACE. src ==
    /// dst replies the same-object error. The optional DB target form is out of
    /// scope for the single-database edge engine.
    fn copy_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(b"copy");
        }
        let mut replace = false;
        let mut index = 3;
        while index < argv.len() {
            if ascii_eq(&argv[index], b"REPLACE") {
                replace = true;
                index += 1;
            } else {
                return err(b"ERR syntax error");
            }
        }
        if argv[1] == argv[2] {
            return err(b"ERR source and destination objects are the same");
        }
        self.purge_if_expired(&argv[1]);
        let Some(entry) = self.db.get(&argv[1]).cloned() else {
            return RespFrame::integer(0);
        };
        self.purge_if_expired(&argv[2]);
        if self.db.contains_key(&argv[2]) && !replace {
            return RespFrame::integer(0);
        }
        self.db.insert(argv[2].clone(), entry);
        self.note_write(&argv[2]);
        RespFrame::integer(1)
    }

    /// TOUCH key [key ...] (`touchCommand`, `expire.c`): count of existing keys
    /// with no mutation.
    fn touch_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"touch");
        }
        let mut touched = 0;
        for key in &argv[1..] {
            if self.get_value(key).is_some() {
                touched += 1;
            }
        }
        RespFrame::integer(touched)
    }

    /// PING [message] (`pingCommand`, `server.c`): +PONG with no argument, the
    /// echoed message otherwise. More than one argument is an arity error.
    fn ping_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() > 2 {
            return wrong_arity(b"ping");
        }
        if argv.len() == 2 {
            bulk(argv[1].clone())
        } else {
            simple(b"PONG")
        }
    }

    /// ECHO message (`echoCommand`, `server.c`): bulk-string echo of the
    /// argument.
    fn echo_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() != 2 {
            return wrong_arity(b"echo");
        }
        bulk(argv[1].clone())
    }

    /// FLUSHALL [ASYNC|SYNC] (`flushallCommand`, `db.c`): clear every key and
    /// reply +OK. Every removed key is marked dirty *before* the database is
    /// cleared, so a host flushes the deletions back to storage.
    fn flushall_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() == 2 {
            if !(ascii_eq(&argv[1], b"ASYNC") || ascii_eq(&argv[1], b"SYNC")) {
                return err(b"ERR syntax error");
            }
        } else if argv.len() != 1 {
            return err(b"ERR syntax error");
        }
        self.mark_all_dirty();
        self.db.clear();
        simple(b"OK")
    }

    fn script_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"script");
        }
        if ascii_eq(&argv[1], b"LOAD") {
            if argv.len() != 3 {
                return wrong_arity(b"script|load");
            }
            if let Some(message) = compile_error_message(&argv[2]) {
                return compile_error_reply(&message);
            }
            let sha = sha1_hex(&argv[2]);
            self.cache_script(sha, &argv[2]);
            bulk(sha.to_vec())
        } else if ascii_eq(&argv[1], b"EXISTS") {
            if argv.len() < 3 {
                return wrong_arity(b"script|exists");
            }
            RespFrame::array(
                argv[2..]
                    .iter()
                    .map(|raw| {
                        let exists = normalise_sha(raw)
                            .map(|sha| self.scripts.contains_key(&sha))
                            .unwrap_or(false);
                        RespFrame::integer(if exists { 1 } else { 0 })
                    })
                    .collect(),
            )
        } else if ascii_eq(&argv[1], b"FLUSH") {
            if argv.len() == 3 {
                if !(ascii_eq(&argv[2], b"SYNC") || ascii_eq(&argv[2], b"ASYNC")) {
                    return err(b"ERR SCRIPT FLUSH only support SYNC|ASYNC option");
                }
            } else if argv.len() != 2 {
                return err(b"ERR SCRIPT FLUSH only support SYNC|ASYNC option");
            }
            self.clear_script_cache();
            simple(b"OK")
        } else {
            let mut msg = b"ERR unknown subcommand '".to_vec();
            msg.extend_from_slice(&argv[1]);
            msg.extend_from_slice(b"'. Try SCRIPT HELP.");
            err(&msg)
        }
    }

    /// EVAL registers the script in the cache exactly like the reference
    /// server, so a later SCRIPT EXISTS / EVALSHA of the same body succeeds.
    /// The cache insert deliberately does not bump the mutation epoch: the
    /// script cache is excluded from snapshots.
    /// `read_only` is true for EVAL_RO, mirroring `evalRoCommand` passing `ro=1`
    /// to `evalGenericCommand` (`eval.c`). It threads through to `eval_script`
    /// so a write `redis.call` inside the script is rejected exactly as valkey
    /// rejects it; the script body, cache, and reply shapes are otherwise
    /// identical to EVAL.
    fn eval_command(&mut self, argv: &[Vec<u8>], read_only: bool) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(if read_only { b"eval_ro" } else { b"eval" });
        }
        let numkeys = match validate_numkeys(&argv[2..]) {
            Ok(numkeys) => numkeys,
            Err(frame) => return frame,
        };
        if let Some(message) = compile_error_message(&argv[1]) {
            return compile_error_reply(&message);
        }
        let sha = sha1_hex(&argv[1]);
        self.cache_script(sha, &argv[1]);
        self.eval_script(&argv[1], &argv[2..], numkeys, read_only)
    }

    fn evalsha_command(&mut self, argv: &[Vec<u8>], read_only: bool) -> RespFrame {
        if argv.len() < 3 {
            return wrong_arity(if read_only { b"evalsha_ro" } else { b"evalsha" });
        }
        let numkeys = match validate_numkeys(&argv[2..]) {
            Ok(numkeys) => numkeys,
            Err(frame) => return frame,
        };
        let Some(sha) = normalise_sha(&argv[1]) else {
            return err(b"NOSCRIPT No matching script.");
        };
        let Some(script) = self.scripts.get(&sha).cloned() else {
            return err(b"NOSCRIPT No matching script.");
        };
        self.touch_script(&sha);
        self.eval_script(&script, &argv[2..], numkeys, read_only)
    }

    /// Runs `script` and, when `read_only` is set, marks the engine read-only
    /// for the duration so `execute_inner` rejects write `redis.call`s
    /// (`SCRIPT_READ_ONLY` in `script.c`). The flag is always cleared after the
    /// run, even on error, so it can never leak into a later non-RO command.
    fn eval_script(
        &mut self,
        script: &[u8],
        rest: &[Vec<u8>],
        numkeys: usize,
        read_only: bool,
    ) -> RespFrame {
        let keys = rest[1..1 + numkeys].to_vec();
        let args = rest[1 + numkeys..].to_vec();
        let previous = self.script_readonly;
        self.script_readonly = read_only;
        let result = self.run_lua_script(script, &keys, &args);
        self.script_readonly = previous;
        match result {
            Ok(frame) => frame,
            Err(error) => {
                let mut msg = b"ERR ".to_vec();
                msg.extend_from_slice(error.message_lossy().as_bytes());
                err(&msg)
            }
        }
    }

    /// Runs `script` wrapped in a Lua-level pcall harness.
    ///
    /// The harness implements Redis's Lua error semantics entirely inside the
    /// interpreter: `redis.call` raises on an error reply while `redis.pcall`
    /// returns it, and an otherwise-uncaught script error becomes an `{err=...}`
    /// reply instead of aborting the call. The host `redis.call`/`redis.pcall`
    /// functions both return `{err=...}` tables (never raise); the prelude
    /// rebinds `redis.call` in Lua to re-raise that table, and the suffix's
    /// `pcall` catches everything and returns it. Errors therefore stay Lua
    /// values throughout — `lua_to_resp` maps the `{err=...}` table to a RESP
    /// error verbatim, preserving codes such as WRONGTYPE from `redis.call`,
    /// and the engine never has to construct (or correctly root) a
    /// value-carrying Rust error.
    ///
    /// Historically this also dodged an omnilua use-after-sweep when an error
    /// value was raised through `lua.scope` — fixed upstream in omnilua 0.2.0
    /// (issue #189, regression-tested in `scope_error_rooting.rs`), so the
    /// harness is now a deliberate fidelity choice, not a safety workaround.
    fn run_lua_script(
        &mut self,
        script: &[u8],
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
    ) -> lua_rs_runtime::Result<RespFrame> {
        let lua = Lua::new_versioned(LuaVersion::V51);
        install_sandbox(&lua)?;
        install_keys_argv(&lua, keys, args)?;

        let engine_cell: RefCell<&mut Engine<H>> = RefCell::new(self);
        let value = lua.scope(|scope| {
            let redis_table = lua.create_table()?;

            let call_fn = {
                let cell = &engine_cell;
                scope.create_function_mut(
                    &lua,
                    move |lua_inner, call_args: Variadic<LuaValue>| {
                        host_call(cell, lua_inner, call_args)
                    },
                )?
            };

            let pcall_fn = {
                let cell = &engine_cell;
                scope.create_function_mut(
                    &lua,
                    move |lua_inner, call_args: Variadic<LuaValue>| {
                        host_call(cell, lua_inner, call_args)
                    },
                )?
            };

            let status_reply_fn = lua.create_function(
                |lua_inner, msg: LuaString| -> lua_rs_runtime::Result<LuaTable> {
                    let table = lua_inner.create_table()?;
                    table.set("ok", msg)?;
                    Ok(table)
                },
            )?;
            let error_reply_fn = lua.create_function(
                |lua_inner, msg: LuaString| -> lua_rs_runtime::Result<LuaTable> {
                    let table = lua_inner.create_table()?;
                    table.set("err", msg)?;
                    Ok(table)
                },
            )?;
            let sha1hex_fn = lua.create_function(
                |_lua_inner, msg: LuaString| -> lua_rs_runtime::Result<String> {
                    let hex = sha1_hex(&msg.as_bytes()?);
                    Ok(String::from_utf8(hex.to_vec()).unwrap_or_default())
                },
            )?;

            redis_table.set("call", call_fn)?;
            redis_table.set("pcall", pcall_fn)?;
            redis_table.set("status_reply", status_reply_fn)?;
            redis_table.set("error_reply", error_reply_fn)?;
            redis_table.set("sha1hex", sha1hex_fn)?;
            lua.globals().set("redis", redis_table.clone())?;
            lua.globals().set("server", redis_table)?;

            lua.load(wrap_script_in_pcall_harness(script))
                .set_name("user_script")
                .eval::<LuaValue>()
        })?;
        lua_to_resp(&value)
    }

    /// PFADD key [element ...] (`pfaddCommand`, `hyperloglog.c`).
    ///
    /// Adds elements to the HyperLogLog stored at `key`, creating a fresh
    /// (dense, all-zero) HLL when the key is missing. Replies `:1` when the key
    /// was created or any register was updated (cardinality estimate may have
    /// changed), `:0` otherwise. Returns the HLL-specific WRONGTYPE error when
    /// the existing key holds a value that is not a valid `HYLL` string. A bare
    /// `PFADD key` with no elements still creates an empty HLL and replies `:1`.
    ///
    /// `note_write` is called exactly once, only when the key was created or a
    /// register changed (mirroring the C `if (updated)` guard around
    /// `signalModifiedKey`).
    fn pfadd_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"pfadd");
        }
        self.purge_if_expired(&argv[1]);

        let mut updated = false;
        let mut buf = match self.db.get(&argv[1]).map(|entry| &entry.value) {
            None => {
                updated = true;
                hll_create_dense()
            }
            Some(StoredValue::String(bytes)) => {
                if !hll_is_valid(bytes) {
                    return err(HLL_WRONG_TYPE_ERR);
                }
                bytes.clone()
            }
            Some(_) => return err(HLL_WRONG_TYPE_ERR),
        };

        let registers = &mut buf[HLL_HDR_SIZE..];
        for ele in &argv[2..] {
            if hll_dense_add(registers, ele) {
                updated = true;
            }
        }

        if updated {
            hll_invalidate_cache(&mut buf);
            let expire_at_ms = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
            self.db.insert(
                argv[1].clone(),
                Entry {
                    value: StoredValue::String(buf),
                    expire_at_ms,
                },
            );
            self.note_write(&argv[1]);
            RespFrame::integer(1)
        } else {
            RespFrame::integer(0)
        }
    }

    /// PFCOUNT key [key ...] (`pfcountCommand`, `hyperloglog.c`).
    ///
    /// Returns the approximate cardinality of the HyperLogLog at `key`. With
    /// more than one key the cardinality of the *union* of the source HLLs is
    /// returned: the keys are merged into a temporary register array that is
    /// never written back, so PFCOUNT never mutates the database. Missing keys
    /// count as empty HLLs. Returns the HLL-specific WRONGTYPE error if any key
    /// holds a non-HLL value. This handler never calls `note_write` (read-only):
    /// the C cache write-back is a pure micro-optimisation with no observable
    /// effect, so the dense-only port recomputes the estimate each time.
    fn pfcount_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"pfcount");
        }

        if argv.len() > 2 {
            let mut max = vec![0u8; HLL_REGISTERS];
            for key in &argv[1..] {
                self.purge_if_expired(key);
                match self.db.get(key).map(|entry| &entry.value) {
                    None => continue,
                    Some(StoredValue::String(bytes)) => {
                        if !hll_is_valid(bytes) {
                            return err(HLL_WRONG_TYPE_ERR);
                        }
                        hll_merge_dense(&mut max, &bytes[HLL_HDR_SIZE..]);
                    }
                    Some(_) => return err(HLL_WRONG_TYPE_ERR),
                }
            }
            return RespFrame::integer(hll_count_raw(&max) as i64);
        }

        self.purge_if_expired(&argv[1]);
        match self.db.get(&argv[1]).map(|entry| &entry.value) {
            None => RespFrame::integer(0),
            Some(StoredValue::String(bytes)) => {
                if !hll_is_valid(bytes) {
                    return err(HLL_WRONG_TYPE_ERR);
                }
                RespFrame::integer(hll_count_dense(&bytes[HLL_HDR_SIZE..]) as i64)
            }
            Some(_) => err(HLL_WRONG_TYPE_ERR),
        }
    }

    /// PFMERGE destkey [srckey ...] (`pfmergeCommand`, `hyperloglog.c`).
    ///
    /// Merges all source HLLs *and* the existing destination HLL register-wise
    /// (taking the max of each of the 16384 registers) and writes the result
    /// back to `destkey`, creating it if absent. Replies `+OK`. Returns the
    /// HLL-specific WRONGTYPE error if `destkey` or any source key holds a
    /// non-HLL value. Always calls `note_write` on the destination (the C
    /// command unconditionally marks the dest modified).
    fn pfmerge_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"pfmerge");
        }

        let mut max = vec![0u8; HLL_REGISTERS];
        for key in &argv[1..] {
            self.purge_if_expired(key);
            match self.db.get(key).map(|entry| &entry.value) {
                None => continue,
                Some(StoredValue::String(bytes)) => {
                    if !hll_is_valid(bytes) {
                        return err(HLL_WRONG_TYPE_ERR);
                    }
                    hll_merge_dense(&mut max, &bytes[HLL_HDR_SIZE..]);
                }
                Some(_) => return err(HLL_WRONG_TYPE_ERR),
            }
        }

        let dest_expire = self.db.get(&argv[1]).and_then(|entry| entry.expire_at_ms);
        let mut dest = hll_create_dense();
        let registers = &mut dest[HLL_HDR_SIZE..];
        for (i, &val) in max.iter().enumerate() {
            if val != 0 {
                hll_dense_set_register(registers, i, val);
            }
        }
        hll_invalidate_cache(&mut dest);

        self.db.insert(
            argv[1].clone(),
            Entry {
                value: StoredValue::String(dest),
                expire_at_ms: dest_expire,
            },
        );
        self.note_write(&argv[1]);
        RespFrame::simple("OK")
    }

    /// `GEOADD key [NX|XX] [CH] lon lat member [lon lat member ...]`. Encodes
    /// each `(lon, lat)` into the 52-bit interleaved geohash that the reference
    /// `geoaddCommand` synthesises as a ZADD score, then delegates to the ZSET
    /// add semantics. Mirrors the reference option/triple parsing,
    /// longitude/latitude validation, and the `NX`+`XX` incompatibility (a plain
    /// `ERR syntax error`).
    fn geoadd_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 5 {
            return wrong_arity(b"geoadd");
        }
        let mut nx = false;
        let mut xx = false;
        let mut ch = false;
        let mut longidx = 2;
        while longidx < argv.len() {
            let opt = &argv[longidx];
            if ascii_eq(opt, b"NX") {
                nx = true;
            } else if ascii_eq(opt, b"XX") {
                xx = true;
            } else if ascii_eq(opt, b"CH") {
                ch = true;
            } else {
                break;
            }
            longidx += 1;
        }
        if (argv.len() - longidx) % 3 != 0 || (argv.len() - longidx) == 0 || (xx && nx) {
            return err(b"ERR syntax error");
        }
        let mut scored: Vec<(f64, Vec<u8>)> = Vec::new();
        let mut index = longidx;
        while index < argv.len() {
            let lon = match parse_geo_double(&argv[index]) {
                Some(v) => v,
                None => return err(b"ERR value is not a valid float"),
            };
            let lat = match parse_geo_double(&argv[index + 1]) {
                Some(v) => v,
                None => return err(b"ERR value is not a valid float"),
            };
            if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&lon)
                || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat)
            {
                let mut msg = b"ERR invalid longitude,latitude pair ".to_vec();
                msg.extend_from_slice(format_c_double(lon).as_bytes());
                msg.push(b',');
                msg.extend_from_slice(format_c_double(lat).as_bytes());
                return err(&msg);
            }
            let bits = geo_encode_score(lon, lat);
            scored.push((bits as f64, argv[index + 2].clone()));
            index += 3;
        }
        self.purge_if_expired(&argv[1]);
        let (added, updated) = match self.db.get_mut(&argv[1]) {
            Some(Entry {
                value: StoredValue::ZSet(members),
                ..
            }) => apply_zadd(members, scored, nx, xx),
            Some(_) => return wrong_type(),
            None => {
                if xx {
                    (0, 0)
                } else {
                    let mut members = HashMap::new();
                    let counts = apply_zadd(&mut members, scored, nx, xx);
                    self.db.insert(
                        argv[1].clone(),
                        Entry {
                            value: StoredValue::ZSet(members),
                            expire_at_ms: None,
                        },
                    );
                    counts
                }
            }
        };
        if added + updated > 0 {
            self.note_write(&argv[1]);
        }
        RespFrame::integer(if ch { added + updated } else { added })
    }

    /// `GEOPOS key member [member ...]`: an array of two-element `[lon, lat]`
    /// arrays decoded from each member's geohash score, or a null-array element
    /// for a missing member. Coordinates are emitted with the same
    /// `addReplyHumanLongDouble` formatting (`%.17Lf` + trailing-zero trim) the
    /// reference uses. Read-only.
    fn geopos_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"geopos");
        }
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => Some(members.clone()),
            Some(_) => return wrong_type(),
            None => None,
        };
        let mut items = Vec::with_capacity(argv.len() - 2);
        for member in &argv[2..] {
            let score = members.as_ref().and_then(|m| m.get(member).copied());
            match score {
                Some(score) => {
                    let (x, y) = geo_decode_score(score as u64);
                    items.push(RespFrame::array(vec![
                        bulk(format_human_long_double(x)),
                        bulk(format_human_long_double(y)),
                    ]));
                }
                None => items.push(RespFrame::null_array()),
            }
        }
        RespFrame::array(items)
    }

    /// `GEODIST key member1 member2 [unit]`: the haversine great-circle distance
    /// between the two members' decoded coordinates, divided by the unit factor
    /// and rendered with `fixedpoint_d2string(_, 4)` (4 fractional digits,
    /// round-half-to-even). A missing key or member yields a null bulk; a bad
    /// unit yields the reference unit error. Read-only.
    fn geodist_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 4 {
            return wrong_arity(b"geodist");
        }
        let to_meter = if argv.len() == 4 {
            1.0
        } else if argv.len() == 5 {
            match geo_unit_to_meters(&argv[4]) {
                Some(v) => v,
                None => return err(b"ERR unsupported unit provided. please use M, KM, FT, MI"),
            }
        } else {
            return err(b"ERR syntax error");
        };
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => members,
            Some(_) => return wrong_type(),
            None => return RespFrame::null_bulk(),
        };
        let (score1, score2) = match (members.get(&argv[2]), members.get(&argv[3])) {
            (Some(a), Some(b)) => (*a, *b),
            _ => return RespFrame::null_bulk(),
        };
        let (x1, y1) = geo_decode_score(score1 as u64);
        let (x2, y2) = geo_decode_score(score2 as u64);
        let distance = geo_haversine(x1, y1, x2, y2) / to_meter;
        bulk(geo_format_distance(distance))
    }

    /// `GEOHASH key member [member ...]`: an array of 11-character standard
    /// geohash strings. Each member's internal score is decoded back to
    /// `(lon, lat)` and re-encoded with the standard `-180..180`/`-90..90` ranges
    /// at step 26, then rendered through valkey's specific base32 alphabet with a
    /// trailing zero-padding character (`geohashCommand`). Missing members and a
    /// missing key both yield null bulk elements. Read-only.
    fn geohash_command(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        if argv.len() < 2 {
            return wrong_arity(b"geohash");
        }
        let members = match self.get_value(&argv[1]).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => Some(members.clone()),
            Some(_) => return wrong_type(),
            None => None,
        };
        let mut items = Vec::with_capacity(argv.len() - 2);
        for member in &argv[2..] {
            match members.as_ref().and_then(|m| m.get(member).copied()) {
                Some(score) => items.push(bulk(geo_hash_string(score as u64))),
                None => items.push(RespFrame::null_bulk()),
            }
        }
        RespFrame::array(items)
    }

    /// Shared engine for `GEOSEARCH`, `GEOSEARCHSTORE`, `GEORADIUS[BYMEMBER]` and
    /// the `_RO` variants — a faithful port of `georadiusGeneric`. `src_key_index`
    /// is the source-key argument position (1 for everything except
    /// GEOSEARCHSTORE, whose argv[1] is the destination). `flags` selects the
    /// argument layout and the allowed options. Builds a `GeoShape`, computes the
    /// 9 candidate geohash boxes, filters the source ZSET by membership in the
    /// shape, optionally sorts by distance, then either replies (with the
    /// requested WITH* annotations) or stores the result into `dest`.
    fn georadius_generic(
        &mut self,
        argv: &[Vec<u8>],
        src_key_index: usize,
        flags: u32,
    ) -> RespFrame {
        let min_args: usize = if flags & GEO_FLAG_COORDS != 0 {
            6
        } else if flags & GEO_FLAG_MEMBER != 0 {
            5
        } else if flags & GEO_FLAG_SEARCHSTORE != 0 {
            8
        } else {
            7
        };
        if argv.len() < min_args {
            let name: &[u8] = if flags & GEO_FLAG_SEARCHSTORE != 0 {
                b"geosearchstore"
            } else if flags & GEO_FLAG_SEARCH != 0 {
                b"geosearch"
            } else if flags & GEO_FLAG_MEMBER != 0 {
                if flags & GEO_FLAG_NOSTORE != 0 {
                    b"georadiusbymember_ro"
                } else {
                    b"georadiusbymember"
                }
            } else if flags & GEO_FLAG_NOSTORE != 0 {
                b"georadius_ro"
            } else {
                b"georadius"
            };
            return wrong_arity(name);
        }

        let src_key = argv[src_key_index].clone();
        let zset = match self.get_value(&src_key).map(|entry| &entry.value) {
            Some(StoredValue::ZSet(members)) => Some(members.clone()),
            Some(_) => return wrong_type(),
            None => None,
        };

        let mut storekey: Option<Vec<u8>> = None;
        let mut storedist = false;
        let mut shape = GeoShape::default();
        let base_args: usize;

        if flags & GEO_FLAG_COORDS != 0 {
            base_args = 6;
            shape.kind = GEO_CIRCULAR;
            let lon = match parse_geo_double(&argv[2]) {
                Some(v) => v,
                None => return err(b"ERR value is not a valid float"),
            };
            let lat = match parse_geo_double(&argv[3]) {
                Some(v) => v,
                None => return err(b"ERR value is not a valid float"),
            };
            if let Some(e) = geo_check_lonlat(lon, lat) {
                return e;
            }
            shape.xy = [lon, lat];
            match geo_extract_distance(&argv[4], &argv[5]) {
                Ok((conversion, radius)) => {
                    shape.conversion = conversion;
                    shape.radius = radius;
                }
                Err(e) => return e,
            }
        } else if flags & GEO_FLAG_MEMBER != 0 && zset.is_none() {
            base_args = 5;
        } else if flags & GEO_FLAG_MEMBER != 0 {
            base_args = 5;
            shape.kind = GEO_CIRCULAR;
            let members = zset.as_ref().expect("zset present in this branch");
            let score = match members.get(&argv[2]) {
                Some(s) => *s,
                None => {
                    let mut m = b"ERR member ".to_vec();
                    m.extend_from_slice(&argv[2]);
                    m.extend_from_slice(b" does not exist");
                    return err(&m);
                }
            };
            let (x, y) = geo_decode_score(score as u64);
            shape.xy = [x, y];
            match geo_extract_distance(&argv[3], &argv[4]) {
                Ok((conversion, radius)) => {
                    shape.conversion = conversion;
                    shape.radius = radius;
                }
                Err(e) => return e,
            }
        } else if flags & GEO_FLAG_SEARCH != 0 {
            base_args = if flags & GEO_FLAG_SEARCHSTORE != 0 {
                storekey = Some(argv[1].clone());
                3
            } else {
                2
            };
        } else {
            return err(b"ERR Unknown georadius search type");
        }

        let mut withdist = false;
        let mut withhash = false;
        let mut withcoords = false;
        let mut frommember = false;
        let mut fromloc = false;
        let mut byradius = false;
        let mut bybox = false;
        let mut sort = GEO_SORT_NONE;
        let mut any = false;
        let mut count: i64 = 0;

        if argv.len() > base_args {
            let remaining = argv.len() - base_args;
            let mut i = 0usize;
            while i < remaining {
                let arg = &argv[base_args + i];
                if ascii_eq(arg, b"withdist") {
                    withdist = true;
                } else if ascii_eq(arg, b"withhash") {
                    withhash = true;
                } else if ascii_eq(arg, b"withcoord") {
                    withcoords = true;
                } else if ascii_eq(arg, b"any") {
                    any = true;
                } else if ascii_eq(arg, b"asc") {
                    sort = GEO_SORT_ASC;
                } else if ascii_eq(arg, b"desc") {
                    sort = GEO_SORT_DESC;
                } else if ascii_eq(arg, b"count") && (i + 1) < remaining {
                    let Some(value) = parse_i64(&argv[base_args + i + 1]) else {
                        return err(b"ERR value is not an integer or out of range");
                    };
                    if value <= 0 {
                        return err(b"ERR COUNT must be > 0");
                    }
                    count = value;
                    i += 1;
                } else if ascii_eq(arg, b"store")
                    && (i + 1) < remaining
                    && flags & GEO_FLAG_NOSTORE == 0
                    && flags & GEO_FLAG_SEARCH == 0
                {
                    storekey = Some(argv[base_args + i + 1].clone());
                    storedist = false;
                    i += 1;
                } else if ascii_eq(arg, b"storedist")
                    && (i + 1) < remaining
                    && flags & GEO_FLAG_NOSTORE == 0
                    && flags & GEO_FLAG_SEARCH == 0
                {
                    storekey = Some(argv[base_args + i + 1].clone());
                    storedist = true;
                    i += 1;
                } else if ascii_eq(arg, b"storedist")
                    && flags & GEO_FLAG_SEARCH != 0
                    && flags & GEO_FLAG_SEARCHSTORE != 0
                {
                    storedist = true;
                } else if ascii_eq(arg, b"frommember")
                    && (i + 1) < remaining
                    && flags & GEO_FLAG_SEARCH != 0
                    && !fromloc
                {
                    if zset.is_none() {
                        frommember = true;
                        i += 1;
                        i += 1;
                        continue;
                    }
                    let members = zset.as_ref().expect("zset present");
                    let member = &argv[base_args + i + 1];
                    let score = match members.get(member) {
                        Some(s) => *s,
                        None => {
                            let mut m = b"ERR member ".to_vec();
                            m.extend_from_slice(member);
                            m.extend_from_slice(b" does not exist");
                            return err(&m);
                        }
                    };
                    let (x, y) = geo_decode_score(score as u64);
                    shape.xy = [x, y];
                    frommember = true;
                    i += 1;
                } else if ascii_eq(arg, b"fromlonlat")
                    && (i + 2) < remaining
                    && flags & GEO_FLAG_SEARCH != 0
                    && !frommember
                {
                    let lon = match parse_geo_double(&argv[base_args + i + 1]) {
                        Some(v) => v,
                        None => return err(b"ERR value is not a valid float"),
                    };
                    let lat = match parse_geo_double(&argv[base_args + i + 2]) {
                        Some(v) => v,
                        None => return err(b"ERR value is not a valid float"),
                    };
                    if let Some(e) = geo_check_lonlat(lon, lat) {
                        return e;
                    }
                    shape.xy = [lon, lat];
                    fromloc = true;
                    i += 2;
                } else if ascii_eq(arg, b"byradius")
                    && (i + 2) < remaining
                    && flags & GEO_FLAG_SEARCH != 0
                    && !bybox
                {
                    match geo_extract_distance(&argv[base_args + i + 1], &argv[base_args + i + 2]) {
                        Ok((conversion, radius)) => {
                            shape.conversion = conversion;
                            shape.radius = radius;
                        }
                        Err(e) => return e,
                    }
                    shape.kind = GEO_CIRCULAR;
                    byradius = true;
                    i += 2;
                } else if ascii_eq(arg, b"bybox")
                    && (i + 3) < remaining
                    && flags & GEO_FLAG_SEARCH != 0
                    && !byradius
                {
                    match geo_extract_box(
                        &argv[base_args + i + 1],
                        &argv[base_args + i + 2],
                        &argv[base_args + i + 3],
                    ) {
                        Ok((conversion, width, height)) => {
                            shape.conversion = conversion;
                            shape.width = width;
                            shape.height = height;
                        }
                        Err(e) => return e,
                    }
                    shape.kind = GEO_RECTANGLE;
                    bybox = true;
                    i += 3;
                } else {
                    return err(b"ERR syntax error");
                }
                i += 1;
            }
        }

        if storekey.is_some() && (withdist || withhash || withcoords) {
            let prefix: &[u8] = if flags & GEO_FLAG_SEARCHSTORE != 0 {
                b"ERR GEOSEARCHSTORE"
            } else {
                b"ERR STORE option in GEORADIUS"
            };
            let mut msg = prefix.to_vec();
            msg.extend_from_slice(
                b" is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            );
            return err(&msg);
        }

        if flags & GEO_FLAG_SEARCH != 0 && !(frommember || fromloc) {
            let mut msg = b"ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for ".to_vec();
            msg.extend_from_slice(&argv[0]);
            return err(&msg);
        }
        if flags & GEO_FLAG_SEARCH != 0 && !(byradius || bybox) {
            let mut msg = b"ERR exactly one of BYRADIUS, BYBOX and BYPOLYGON can be specified for ".to_vec();
            msg.extend_from_slice(&argv[0]);
            return err(&msg);
        }
        if any && count == 0 {
            return err(b"ERR the ANY argument requires COUNT argument");
        }

        if zset.is_none() {
            if let Some(dest) = storekey {
                let existed = self.db.remove(&dest).is_some();
                if existed {
                    self.note_write(&dest);
                }
                return RespFrame::integer(0);
            }
            return RespFrame::array(Vec::new());
        }

        if count != 0 && sort == GEO_SORT_NONE && !any {
            sort = GEO_SORT_ASC;
        }

        let members = zset.expect("zset present past this point");
        let limit = if any { count as u64 } else { 0 };
        let mut points = geo_search_points(&members, &shape, limit);

        if storekey.is_none() && points.is_empty() {
            return RespFrame::array(Vec::new());
        }

        let result_length = points.len() as i64;
        let returned_items = if count == 0 || result_length < count {
            result_length
        } else {
            count
        } as usize;

        if sort == GEO_SORT_ASC {
            points.sort_by(|a, b| a.dist.partial_cmp(&b.dist).expect("geo dist is finite"));
        } else if sort == GEO_SORT_DESC {
            points.sort_by(|a, b| b.dist.partial_cmp(&a.dist).expect("geo dist is finite"));
        }

        match storekey {
            None => {
                let mut option_length = 0;
                if withdist {
                    option_length += 1;
                }
                if withcoords {
                    option_length += 1;
                }
                if withhash {
                    option_length += 1;
                }
                let mut out = Vec::with_capacity(returned_items);
                for gp in points.iter().take(returned_items) {
                    let dist = gp.dist / shape.conversion;
                    if option_length > 0 {
                        let mut sub = Vec::with_capacity(option_length + 1);
                        sub.push(bulk(&gp.member));
                        if withdist {
                            sub.push(bulk(geo_format_distance(dist)));
                        }
                        if withhash {
                            sub.push(RespFrame::integer(gp.score as i64));
                        }
                        if withcoords {
                            sub.push(RespFrame::array(vec![
                                bulk(format_human_long_double(gp.longitude)),
                                bulk(format_human_long_double(gp.latitude)),
                            ]));
                        }
                        out.push(RespFrame::array(sub));
                    } else {
                        out.push(bulk(&gp.member));
                    }
                }
                RespFrame::array(out)
            }
            Some(dest) => {
                if returned_items > 0 {
                    let mut stored: HashMap<Vec<u8>, f64> = HashMap::new();
                    for gp in points.iter().take(returned_items) {
                        let dist = gp.dist / shape.conversion;
                        let score = if storedist { dist } else { gp.score as f64 };
                        stored.insert(gp.member.clone(), normalize_zero(score));
                    }
                    self.db.insert(
                        dest.clone(),
                        Entry {
                            value: StoredValue::ZSet(stored),
                            expire_at_ms: None,
                        },
                    );
                    self.note_write(&dest);
                } else {
                    let existed = self.db.remove(&dest).is_some();
                    if existed {
                        self.note_write(&dest);
                    }
                }
                RespFrame::integer(returned_items as i64)
            }
        }
    }

    fn get_value(&mut self, key: &[u8]) -> Option<&Entry> {
        self.purge_expired_fields(key);
        self.db.get(key)
    }

    fn purge_if_expired(&mut self, key: &[u8]) {
        let expired = self
            .db
            .get(key)
            .and_then(|entry| entry.expire_at_ms)
            .is_some_and(|deadline| deadline <= self.host.now_millis());
        if expired {
            self.db.remove(key);
        }
    }

    /// Lazy per-field expiry for hashes, mirroring how the reference server
    /// treats expired hash fields as already-deleted on access (active expiry
    /// is the same effect). First purges a whole-key TTL via `purge_if_expired`;
    /// then, if `key` holds a hash, drops every field whose `expire_at_ms` has
    /// passed relative to the host clock. If that empties the hash, the key is
    /// removed entirely (an empty hash is a deleted key in Valkey). An expired
    /// field is therefore invisible to every subsequent read or write within
    /// the same command. This is passive expiry: like `purge_if_expired`, it
    /// does not bump the mutation epoch or mark the key dirty — absolute
    /// per-field deadlines make a stale persisted copy harmless, and a real
    /// mutation in the same command path will call `note_write` itself.
    fn purge_expired_fields(&mut self, key: &[u8]) {
        self.purge_if_expired(key);
        let now = self.host.now_millis();
        let emptied = match self.db.get_mut(key) {
            Some(Entry {
                value: StoredValue::Hash(fields),
                ..
            }) => {
                fields.retain(|_, field| {
                    !field.expire_at_ms.is_some_and(|deadline| deadline <= now)
                });
                fields.is_empty()
            }
            _ => false,
        };
        if emptied {
            self.db.remove(key);
        }
    }

    fn purge_expired_keys(&mut self) {
        let now = self.host.now_millis();
        self.db
            .retain(|_, entry| !entry.expire_at_ms.is_some_and(|deadline| deadline <= now));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestMethod {
    Get,
    Post,
    Put,
    Head,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestResponseFormat {
    Json,
    Resp2,
}

#[derive(Debug, Clone, Copy)]
pub struct RestRequest<'a> {
    pub method: RestMethod,
    pub path: &'a str,
    pub body: &'a [u8],
    pub response_format: RestResponseFormat,
}

impl<'a> RestRequest<'a> {
    pub fn get(path: &'a str) -> Self {
        Self {
            method: RestMethod::Get,
            path,
            body: &[],
            response_format: RestResponseFormat::Json,
        }
    }

    pub fn post(path: &'a str, body: &'a [u8]) -> Self {
        Self {
            method: RestMethod::Post,
            path,
            body,
            response_format: RestResponseFormat::Json,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

impl RestResponse {
    fn json_frame(frame: RespFrame) -> Self {
        let status = rest_status_for_frame(&frame);
        match serde_json::to_vec(&rest_frame_json(frame)) {
            Ok(body) => Self {
                status,
                content_type: APPLICATION_JSON,
                body,
            },
            Err(_) => Self::json_error(500, b"ERR JSON encode failed"),
        }
    }

    fn resp2_frame(frame: RespFrame) -> Self {
        let status = rest_status_for_frame(&frame);
        let mut body = Vec::new();
        encode_resp2(&frame, &mut body);
        Self {
            status,
            content_type: APPLICATION_OCTET_STREAM,
            body,
        }
    }

    fn json_error(status: u16, message: &[u8]) -> Self {
        let body = serde_json::to_vec(&json!({
            "error": String::from_utf8_lossy(message)
        }))
        .unwrap_or_else(|_| b"{\"error\":\"ERR JSON encode failed\"}".to_vec());
        Self {
            status,
            content_type: APPLICATION_JSON,
            body,
        }
    }
}

const APPLICATION_JSON: &str = "application/json";
const APPLICATION_OCTET_STREAM: &str = "application/octet-stream";

enum RestCommand {
    Single(Vec<Vec<u8>>),
    Pipeline(Vec<Vec<Vec<u8>>>),
}

fn rest_command_from_request(request: RestRequest<'_>) -> Result<RestCommand, Vec<u8>> {
    let (path, query) = split_path_query(request.path);
    let segments = parse_path_segments(path)?;
    let is_pipeline = segments
        .first()
        .is_some_and(|segment| ascii_eq(segment, b"pipeline"));

    if is_pipeline {
        if !matches!(request.method, RestMethod::Post | RestMethod::Put) {
            return Err(b"ERR pipeline requires POST or PUT".to_vec());
        }
        return parse_pipeline_body(request.body);
    }

    if segments.is_empty() {
        if !matches!(request.method, RestMethod::Post | RestMethod::Put) {
            return Err(b"ERR missing command".to_vec());
        }
        return parse_single_command_body(request.body).map(RestCommand::Single);
    }

    if matches!(request.method, RestMethod::Post | RestMethod::Put)
        && body_looks_like_json_array(request.body)
    {
        let argv = parse_single_command_body(request.body)?;
        if argv
            .first()
            .is_some_and(|command| ascii_eq(command, &segments[0]))
        {
            return Ok(RestCommand::Single(argv));
        }
    }

    let mut argv = segments;
    if matches!(request.method, RestMethod::Post | RestMethod::Put) && !request.body.is_empty() {
        argv.push(request.body.to_vec());
    }
    append_query_args(query, &mut argv)?;
    Ok(RestCommand::Single(argv))
}

/// Resolve the [`KeyAccess`] a REST request touches, reusing the exact same
/// `RestRequest` -> argv parsing [`Engine::execute_rest`] runs so a lazy
/// per-key loader sees byte-identical commands to what will execute. A single
/// command yields its `command_keys`; a `/pipeline` batch yields the
/// [`KeyAccess::merge`] union of every command in the batch (the request as a
/// whole touches every key any of its commands touches, and degrades to
/// `FullKeyspace` if any one does). A malformed request that cannot be parsed
/// into commands touches no keys, so the loader fetches nothing and the request
/// fails the same way it would have eagerly.
pub fn rest_command_keys(request: RestRequest<'_>) -> KeyAccess {
    match rest_command_from_request(request) {
        Ok(RestCommand::Single(argv)) => command_keys(&argv),
        Ok(RestCommand::Pipeline(commands)) => {
            let mut access = KeyAccess::Keys(Vec::new());
            for argv in &commands {
                access = access.merge(command_keys(argv));
            }
            access
        }
        Err(_) => KeyAccess::Keys(Vec::new()),
    }
}

fn split_path_query(path: &str) -> (&str, &str) {
    match path.split_once('?') {
        Some((path, query)) => (path, query),
        None => (path, ""),
    }
}

fn parse_path_segments(path: &str) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| percent_decode(segment.as_bytes(), false))
        .collect()
}

fn append_query_args(query: &str, argv: &mut Vec<Vec<u8>>) -> Result<(), Vec<u8>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == "_token" {
            continue;
        }
        argv.push(percent_decode(key.as_bytes(), true)?);
        if !value.is_empty() {
            argv.push(percent_decode(value.as_bytes(), true)?);
        }
    }
    Ok(())
}

fn percent_decode(input: &[u8], plus_as_space: bool) -> Result<Vec<u8>, Vec<u8>> {
    let mut out = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        match input[index] {
            b'%' => {
                if index + 2 >= input.len() {
                    return Err(b"ERR invalid URL escape".to_vec());
                }
                let Some(high) = hex_nibble(input[index + 1]) else {
                    return Err(b"ERR invalid URL escape".to_vec());
                };
                let Some(low) = hex_nibble(input[index + 2]) else {
                    return Err(b"ERR invalid URL escape".to_vec());
                };
                out.push((high << 4) | low);
                index += 3;
            }
            b'+' if plus_as_space => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn body_looks_like_json_array(body: &[u8]) -> bool {
    body.iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'[')
}

fn parse_single_command_body(body: &[u8]) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let json: JsonValue =
        serde_json::from_slice(body).map_err(|_| b"ERR invalid JSON command body".to_vec())?;
    json_array_to_argv(&json)
}

fn parse_pipeline_body(body: &[u8]) -> Result<RestCommand, Vec<u8>> {
    let json: JsonValue =
        serde_json::from_slice(body).map_err(|_| b"ERR invalid JSON pipeline body".to_vec())?;
    let Some(rows) = json.as_array() else {
        return Err(b"ERR pipeline body must be an array".to_vec());
    };
    let mut commands = Vec::with_capacity(rows.len());
    for row in rows {
        commands.push(json_array_to_argv(row)?);
    }
    Ok(RestCommand::Pipeline(commands))
}

fn json_array_to_argv(value: &JsonValue) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let Some(items) = value.as_array() else {
        return Err(b"ERR command body must be an array".to_vec());
    };
    if items.is_empty() {
        return Err(b"ERR command body must not be empty".to_vec());
    }
    let mut argv = Vec::with_capacity(items.len());
    for item in items {
        argv.push(json_arg_to_bytes(item)?);
    }
    Ok(argv)
}

fn json_arg_to_bytes(value: &JsonValue) -> Result<Vec<u8>, Vec<u8>> {
    match value {
        JsonValue::String(value) => Ok(value.as_bytes().to_vec()),
        JsonValue::Number(value) => Ok(value.to_string().into_bytes()),
        JsonValue::Bool(true) => Ok(b"true".to_vec()),
        JsonValue::Bool(false) => Ok(b"false".to_vec()),
        JsonValue::Null => Ok(b"null".to_vec()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            Err(b"ERR command arguments must be scalar JSON values".to_vec())
        }
    }
}

fn rest_status_for_frame(frame: &RespFrame) -> u16 {
    match frame {
        RespFrame::Error(_) | RespFrame::BulkError(_) => 400,
        _ => 200,
    }
}

fn rest_content_type(format: RestResponseFormat) -> &'static str {
    match format {
        RestResponseFormat::Json => APPLICATION_JSON,
        RestResponseFormat::Resp2 => APPLICATION_OCTET_STREAM,
    }
}

/// Serialize one key's entry into the canonical JSON object used both inside a
/// full snapshot and as a standalone per-key value. Keys, hash fields, and
/// zset members are hex-encoded; zset members are sorted by member and scores
/// use the lossless snapshot string form so a round-trip is exact.
fn encode_entry(key: &[u8], entry: &Entry) -> JsonMap<String, JsonValue> {
    let mut object = JsonMap::new();
    object.insert("key".to_owned(), JsonValue::String(hex_encode(key)));
    if let Some(expire_at_ms) = entry.expire_at_ms {
        object.insert("expire_at_ms".to_owned(), json!(expire_at_ms));
    }
    match &entry.value {
        StoredValue::String(value) => {
            object.insert("type".to_owned(), JsonValue::String("string".to_owned()));
            object.insert("value".to_owned(), JsonValue::String(hex_encode(value)));
        }
        StoredValue::Hash(fields) => {
            // Serialize fields in insertion order (the `IndexMap` order),
            // matching valkey's stored hash order so a DUMP listpack and a
            // snapshot round-trip both preserve it.
            object.insert("type".to_owned(), JsonValue::String("hash".to_owned()));
            object.insert(
                "fields".to_owned(),
                JsonValue::Array(
                    fields
                        .iter()
                        .map(|(field, field_value)| {
                            // Two-element `[field, value]` for a field with no
                            // TTL (round-trips with old snapshots); a third
                            // element carrying the absolute deadline is added
                            // only when the field has a per-field expiry.
                            let mut entry = vec![
                                JsonValue::String(hex_encode(field)),
                                JsonValue::String(hex_encode(&field_value.value)),
                            ];
                            if let Some(expire_at_ms) = field_value.expire_at_ms {
                                entry.push(json!(expire_at_ms));
                            }
                            JsonValue::Array(entry)
                        })
                        .collect(),
                ),
            );
        }
        StoredValue::ZSet(members) => {
            let mut member_items: Vec<_> = members.iter().collect();
            member_items.sort_by(|(left, _), (right, _)| left.cmp(right));
            object.insert("type".to_owned(), JsonValue::String("zset".to_owned()));
            object.insert(
                "members".to_owned(),
                JsonValue::Array(
                    member_items
                        .into_iter()
                        .map(|(member, score)| {
                            JsonValue::Array(vec![
                                JsonValue::String(hex_encode(member)),
                                JsonValue::String(score_snapshot_string(*score)),
                            ])
                        })
                        .collect(),
                ),
            );
        }
        StoredValue::List(items) => {
            object.insert("type".to_owned(), JsonValue::String("list".to_owned()));
            object.insert(
                "items".to_owned(),
                JsonValue::Array(
                    items
                        .iter()
                        .map(|item| JsonValue::String(hex_encode(item)))
                        .collect(),
                ),
            );
        }
        StoredValue::Set(members) => {
            // Serialize members in insertion order (the `IndexSet` order),
            // matching valkey's stored listpack order so a DUMP and a snapshot
            // round-trip both preserve it.
            object.insert("type".to_owned(), JsonValue::String("set".to_owned()));
            object.insert(
                "members".to_owned(),
                JsonValue::Array(
                    members
                        .iter()
                        .map(|member| JsonValue::String(hex_encode(member)))
                        .collect(),
                ),
            );
        }
        StoredValue::Stream(stream) => {
            object.insert("type".to_owned(), JsonValue::String("stream".to_owned()));
            object.insert(
                "entries".to_owned(),
                JsonValue::Array(
                    stream
                        .entries
                        .iter()
                        .map(|(id, fields)| {
                            JsonValue::Array(vec![
                                JsonValue::String(format!("{}-{}", id.ms, id.seq)),
                                JsonValue::Array(
                                    fields
                                        .iter()
                                        .flat_map(|(field, value)| {
                                            [
                                                JsonValue::String(hex_encode(field)),
                                                JsonValue::String(hex_encode(value)),
                                            ]
                                        })
                                        .collect(),
                                ),
                            ])
                        })
                        .collect(),
                ),
            );
            object.insert(
                "last_id".to_owned(),
                JsonValue::String(format!("{}-{}", stream.last_id.ms, stream.last_id.seq)),
            );
            object.insert(
                "max_deleted_id".to_owned(),
                JsonValue::String(format!(
                    "{}-{}",
                    stream.max_deleted_id.ms, stream.max_deleted_id.seq
                )),
            );
            object.insert("entries_added".to_owned(), json!(stream.entries_added));
            object.insert(
                "first_id".to_owned(),
                JsonValue::String(format!("{}-{}", stream.first_id.ms, stream.first_id.seq)),
            );
            let mut group_names: Vec<&Vec<u8>> = stream.groups.keys().collect();
            group_names.sort();
            object.insert(
                "groups".to_owned(),
                JsonValue::Array(
                    group_names
                        .into_iter()
                        .map(|name| {
                            let group = &stream.groups[name];
                            let mut g = JsonMap::new();
                            g.insert("name".to_owned(), JsonValue::String(hex_encode(name)));
                            g.insert(
                                "last_delivered_id".to_owned(),
                                JsonValue::String(format!(
                                    "{}-{}",
                                    group.last_delivered_id.ms, group.last_delivered_id.seq
                                )),
                            );
                            g.insert("entries_read".to_owned(), json!(group.entries_read));
                            g.insert(
                                "pending".to_owned(),
                                JsonValue::Array(
                                    group
                                        .pending
                                        .iter()
                                        .map(|(id, nack)| {
                                            let mut p = JsonMap::new();
                                            p.insert(
                                                "id".to_owned(),
                                                JsonValue::String(format!("{}-{}", id.ms, id.seq)),
                                            );
                                            p.insert(
                                                "consumer".to_owned(),
                                                JsonValue::String(hex_encode(&nack.consumer)),
                                            );
                                            p.insert(
                                                "delivery_time_ms".to_owned(),
                                                json!(nack.delivery_time_ms),
                                            );
                                            p.insert(
                                                "delivery_count".to_owned(),
                                                json!(nack.delivery_count),
                                            );
                                            JsonValue::Object(p)
                                        })
                                        .collect(),
                                ),
                            );
                            let mut consumer_names: Vec<&Vec<u8>> = group.consumers.keys().collect();
                            consumer_names.sort();
                            g.insert(
                                "consumers".to_owned(),
                                JsonValue::Array(
                                    consumer_names
                                        .into_iter()
                                        .map(|cname| {
                                            let consumer = &group.consumers[cname];
                                            let mut cobj = JsonMap::new();
                                            cobj.insert(
                                                "name".to_owned(),
                                                JsonValue::String(hex_encode(cname)),
                                            );
                                            cobj.insert(
                                                "seen_time_ms".to_owned(),
                                                json!(consumer.seen_time_ms),
                                            );
                                            cobj.insert(
                                                "active_time_ms".to_owned(),
                                                json!(consumer.active_time_ms),
                                            );
                                            cobj.insert(
                                                "pending".to_owned(),
                                                JsonValue::Array(
                                                    consumer
                                                        .pending
                                                        .iter()
                                                        .map(|id| {
                                                            JsonValue::String(format!(
                                                                "{}-{}",
                                                                id.ms, id.seq
                                                            ))
                                                        })
                                                        .collect(),
                                                ),
                                            );
                                            JsonValue::Object(cobj)
                                        })
                                        .collect(),
                                ),
                            );
                            JsonValue::Object(g)
                        })
                        .collect(),
                ),
            );
        }
    }
    object
}

/// Inverse of `encode_entry`: decode one JSON key object into `(key, entry)`.
fn decode_entry(object: &JsonMap<String, JsonValue>) -> Result<(Vec<u8>, Entry), SnapshotError> {
    let key = hex_decode(
        object
            .get("key")
            .and_then(JsonValue::as_str)
            .ok_or(SnapshotError::MissingField("key"))?,
    )?;
    let expire_at_ms = match object.get("expire_at_ms") {
        Some(value) => Some(
            value
                .as_u64()
                .ok_or(SnapshotError::InvalidField("expire_at_ms"))?,
        ),
        None => None,
    };
    let value = match object
        .get("type")
        .and_then(JsonValue::as_str)
        .ok_or(SnapshotError::MissingField("type"))?
    {
        "string" => StoredValue::String(hex_decode(
            object
                .get("value")
                .and_then(JsonValue::as_str)
                .ok_or(SnapshotError::MissingField("value"))?,
        )?),
        "hash" => {
            let fields = object
                .get("fields")
                .and_then(JsonValue::as_array)
                .ok_or(SnapshotError::MissingField("fields"))?;
            let mut decoded_fields = IndexMap::new();
            for pair in fields {
                let pair = pair
                    .as_array()
                    .ok_or(SnapshotError::InvalidField("fields"))?;
                // `[field, value]` (legacy / no-TTL) or `[field, value, ttl]`.
                if pair.len() != 2 && pair.len() != 3 {
                    return Err(SnapshotError::InvalidField("fields"));
                }
                let field = hex_decode(
                    pair[0]
                        .as_str()
                        .ok_or(SnapshotError::InvalidField("fields"))?,
                )?;
                let value = hex_decode(
                    pair[1]
                        .as_str()
                        .ok_or(SnapshotError::InvalidField("fields"))?,
                )?;
                let expire_at_ms = match pair.get(2) {
                    Some(value) => Some(
                        value
                            .as_u64()
                            .ok_or(SnapshotError::InvalidField("fields"))?,
                    ),
                    None => None,
                };
                decoded_fields.insert(field, HashField { value, expire_at_ms });
            }
            StoredValue::Hash(decoded_fields)
        }
        "zset" => {
            let members = object
                .get("members")
                .and_then(JsonValue::as_array)
                .ok_or(SnapshotError::MissingField("members"))?;
            let mut decoded_members = HashMap::new();
            for pair in members {
                let pair = pair
                    .as_array()
                    .ok_or(SnapshotError::InvalidField("members"))?;
                if pair.len() != 2 {
                    return Err(SnapshotError::InvalidField("members"));
                }
                let member = hex_decode(
                    pair[0]
                        .as_str()
                        .ok_or(SnapshotError::InvalidField("members"))?,
                )?;
                let score = pair[1]
                    .as_str()
                    .and_then(|text| parse_score(text.as_bytes()))
                    .ok_or(SnapshotError::InvalidField("members"))?;
                decoded_members.insert(member, score);
            }
            StoredValue::ZSet(decoded_members)
        }
        "list" => {
            let items = object
                .get("items")
                .and_then(JsonValue::as_array)
                .ok_or(SnapshotError::MissingField("items"))?;
            let mut decoded_items = VecDeque::with_capacity(items.len());
            for item in items {
                let item = hex_decode(item.as_str().ok_or(SnapshotError::InvalidField("items"))?)?;
                decoded_items.push_back(item);
            }
            StoredValue::List(decoded_items)
        }
        "set" => {
            let members = object
                .get("members")
                .and_then(JsonValue::as_array)
                .ok_or(SnapshotError::MissingField("members"))?;
            let mut decoded_members = IndexSet::with_capacity(members.len());
            for member in members {
                let member =
                    hex_decode(member.as_str().ok_or(SnapshotError::InvalidField("members"))?)?;
                decoded_members.insert(member);
            }
            StoredValue::Set(decoded_members)
        }
        "stream" => {
            let entries = object
                .get("entries")
                .and_then(JsonValue::as_array)
                .ok_or(SnapshotError::MissingField("entries"))?;
            let mut decoded_entries = std::collections::BTreeMap::new();
            for entry in entries {
                let entry = entry
                    .as_array()
                    .ok_or(SnapshotError::InvalidField("entries"))?;
                if entry.len() != 2 {
                    return Err(SnapshotError::InvalidField("entries"));
                }
                let id = decode_stream_id(
                    entry[0]
                        .as_str()
                        .ok_or(SnapshotError::InvalidField("entries"))?,
                )?;
                let fields_raw = entry[1]
                    .as_array()
                    .ok_or(SnapshotError::InvalidField("entries"))?;
                if fields_raw.len() % 2 != 0 {
                    return Err(SnapshotError::InvalidField("entries"));
                }
                let mut fields = Vec::with_capacity(fields_raw.len() / 2);
                for pair in fields_raw.chunks_exact(2) {
                    let field = hex_decode(
                        pair[0]
                            .as_str()
                            .ok_or(SnapshotError::InvalidField("entries"))?,
                    )?;
                    let value = hex_decode(
                        pair[1]
                            .as_str()
                            .ok_or(SnapshotError::InvalidField("entries"))?,
                    )?;
                    fields.push((field, value));
                }
                decoded_entries.insert(id, fields);
            }
            let last_id = decode_stream_id(
                object
                    .get("last_id")
                    .and_then(JsonValue::as_str)
                    .ok_or(SnapshotError::MissingField("last_id"))?,
            )?;
            let max_deleted_id = decode_stream_id(
                object
                    .get("max_deleted_id")
                    .and_then(JsonValue::as_str)
                    .ok_or(SnapshotError::MissingField("max_deleted_id"))?,
            )?;
            let entries_added = object
                .get("entries_added")
                .and_then(JsonValue::as_u64)
                .ok_or(SnapshotError::MissingField("entries_added"))?;
            let first_id = decode_stream_id(
                object
                    .get("first_id")
                    .and_then(JsonValue::as_str)
                    .ok_or(SnapshotError::MissingField("first_id"))?,
            )?;
            let groups_raw = object
                .get("groups")
                .and_then(JsonValue::as_array)
                .ok_or(SnapshotError::MissingField("groups"))?;
            let mut groups = std::collections::HashMap::with_capacity(groups_raw.len());
            for group_raw in groups_raw {
                let g = group_raw
                    .as_object()
                    .ok_or(SnapshotError::InvalidField("groups"))?;
                let name = hex_decode(
                    g.get("name")
                        .and_then(JsonValue::as_str)
                        .ok_or(SnapshotError::InvalidField("groups"))?,
                )?;
                let last_delivered_id = decode_stream_id(
                    g.get("last_delivered_id")
                        .and_then(JsonValue::as_str)
                        .ok_or(SnapshotError::InvalidField("groups"))?,
                )?;
                let entries_read = g
                    .get("entries_read")
                    .and_then(JsonValue::as_i64)
                    .ok_or(SnapshotError::InvalidField("groups"))?;
                let pending_raw = g
                    .get("pending")
                    .and_then(JsonValue::as_array)
                    .ok_or(SnapshotError::InvalidField("groups"))?;
                let mut pending = std::collections::BTreeMap::new();
                for p in pending_raw {
                    let p = p.as_object().ok_or(SnapshotError::InvalidField("groups"))?;
                    let id = decode_stream_id(
                        p.get("id")
                            .and_then(JsonValue::as_str)
                            .ok_or(SnapshotError::InvalidField("groups"))?,
                    )?;
                    let consumer = hex_decode(
                        p.get("consumer")
                            .and_then(JsonValue::as_str)
                            .ok_or(SnapshotError::InvalidField("groups"))?,
                    )?;
                    let delivery_time_ms = p
                        .get("delivery_time_ms")
                        .and_then(JsonValue::as_u64)
                        .ok_or(SnapshotError::InvalidField("groups"))?;
                    let delivery_count = p
                        .get("delivery_count")
                        .and_then(JsonValue::as_u64)
                        .ok_or(SnapshotError::InvalidField("groups"))?;
                    pending.insert(
                        id,
                        PendingEntry {
                            consumer,
                            delivery_time_ms,
                            delivery_count,
                        },
                    );
                }
                let consumers_raw = g
                    .get("consumers")
                    .and_then(JsonValue::as_array)
                    .ok_or(SnapshotError::InvalidField("groups"))?;
                let mut consumers = std::collections::HashMap::with_capacity(consumers_raw.len());
                for c in consumers_raw {
                    let c = c.as_object().ok_or(SnapshotError::InvalidField("groups"))?;
                    let cname = hex_decode(
                        c.get("name")
                            .and_then(JsonValue::as_str)
                            .ok_or(SnapshotError::InvalidField("groups"))?,
                    )?;
                    let seen_time_ms = c
                        .get("seen_time_ms")
                        .and_then(JsonValue::as_u64)
                        .ok_or(SnapshotError::InvalidField("groups"))?;
                    let active_time_ms = c
                        .get("active_time_ms")
                        .and_then(JsonValue::as_u64)
                        .ok_or(SnapshotError::InvalidField("groups"))?;
                    let cpending_raw = c
                        .get("pending")
                        .and_then(JsonValue::as_array)
                        .ok_or(SnapshotError::InvalidField("groups"))?;
                    let mut cpending = std::collections::BTreeSet::new();
                    for id in cpending_raw {
                        cpending.insert(decode_stream_id(
                            id.as_str().ok_or(SnapshotError::InvalidField("groups"))?,
                        )?);
                    }
                    consumers.insert(
                        cname,
                        Consumer {
                            pending: cpending,
                            seen_time_ms,
                            active_time_ms,
                        },
                    );
                }
                groups.insert(
                    name,
                    Group {
                        last_delivered_id,
                        pending,
                        consumers,
                        entries_read,
                    },
                );
            }
            StoredValue::Stream(StreamValue {
                entries: decoded_entries,
                last_id,
                max_deleted_id,
                entries_added,
                first_id,
                groups,
            })
        }
        _ => return Err(SnapshotError::InvalidField("type")),
    };
    Ok((
        key,
        Entry {
            value,
            expire_at_ms,
        },
    ))
}

/// Normalise a signed list index into `[0, len)` for read/write access,
/// mirroring the index handling in `lindexCommand` / `lsetCommand`
/// (`t_list.c`). Negative indices count from the tail. Returns `None` when
/// the resolved index is out of range.
fn resolve_list_index(index: i64, len: usize) -> Option<usize> {
    let len_i = len as i64;
    let resolved = if index < 0 { index + len_i } else { index };
    if resolved < 0 || resolved >= len_i {
        return None;
    }
    Some(resolved as usize)
}

/// Resolve `start` / `stop` for `LRANGE` / `LTRIM` (`addListRangeReply` /
/// `ltrimCommand`, `t_list.c`). Negative indices count from the tail; the
/// result is clamped to `0 <= s <= e < len`. Returns `None` for an empty or
/// inverted range.
fn resolve_list_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
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

/// A parsed `MAXLEN`/`MINID` trim request (`streamAddTrimArgs`, `t_stream.c`).
/// `approx` records whether `~` was given; in our flat (non-listpack-node)
/// model trimming is always exact, so `approx` only affects the LIMIT
/// syntax-check, never the number of entries removed.
#[derive(Debug, Clone)]
struct StreamTrim {
    threshold: StreamTrimThreshold,
    approx: bool,
    limit_given: bool,
}

#[derive(Debug, Clone, Copy)]
enum StreamTrimThreshold {
    MaxLen(u64),
    MinId(StreamId),
}

/// Apply a trim to a stream in place, returning the number of entries removed
/// (`streamTrim`, `t_stream.c`). MAXLEN removes the oldest entries until the
/// length is at most the threshold; MINID removes every entry with an ID
/// strictly less than the threshold. Always trims exactly (see `StreamTrim`).
/// Trimming does NOT touch `max_deleted_id` (only XDEL does, per the C); it
/// updates `first_id` (0-0 when empty, else the new smallest entry).
fn apply_stream_trim(stream: &mut StreamValue, trim: &StreamTrim) -> u64 {
    let mut removed = 0u64;
    match trim.threshold {
        StreamTrimThreshold::MaxLen(maxlen) => {
            while stream.entries.len() as u64 > maxlen {
                let Some((&id, _)) = stream.entries.iter().next() else {
                    break;
                };
                stream.entries.remove(&id);
                removed += 1;
            }
        }
        StreamTrimThreshold::MinId(minid) => {
            let to_remove: Vec<StreamId> = stream
                .entries
                .range(..minid)
                .map(|(id, _)| *id)
                .collect();
            for id in to_remove {
                stream.entries.remove(&id);
                removed += 1;
            }
        }
    }
    if removed > 0 {
        stream.first_id = match stream.entries.keys().next() {
            Some(id) => *id,
            None => StreamId::MIN,
        };
    }
    removed
}

/// Compute the ID for a new XADD entry given the stream's current `last_id`
/// (`streamAppendItem` / `streamNextID`, `t_stream.c`). `use_id` is the
/// explicit ID (if any); `seq_given` is false for the `ms-*` auto-seq form.
/// Returns `None` when the resulting ID would not be strictly greater than
/// `last_id` (the C `EDOM` case).
fn stream_next_append_id(
    last_id: StreamId,
    use_id: Option<StreamId>,
    seq_given: bool,
    now_ms: u64,
) -> Option<StreamId> {
    let id = match use_id {
        Some(use_id) => {
            if seq_given {
                use_id
            } else if last_id.ms == use_id.ms {
                if last_id.seq == u64::MAX {
                    return None;
                }
                StreamId {
                    ms: last_id.ms,
                    seq: last_id.seq + 1,
                }
            } else {
                use_id
            }
        }
        None => {
            if now_ms > last_id.ms {
                StreamId {
                    ms: now_ms,
                    seq: 0,
                }
            } else {
                last_id.incr()?
            }
        }
    };
    if id <= last_id {
        return None;
    }
    Some(id)
}

/// Render one stream entry as the RESP `[id-string, [field, value, ...]]`
/// two-element array (`streamReplyWithRange`, `t_stream.c`).
fn render_stream_entry(id: StreamId, fields: &[(Vec<u8>, Vec<u8>)]) -> RespFrame {
    let mut flat = Vec::with_capacity(fields.len() * 2);
    for (field, value) in fields {
        flat.push(bulk(field));
        flat.push(bulk(value));
    }
    RespFrame::array(vec![bulk(id.to_string_bytes()), RespFrame::array(flat)])
}

/// The `-ERR unknown subcommand '<sub>'. Try <CMD> HELP.` reply
/// (`addReplySubcommandSyntaxError`, `server.c`). The subcommand name is echoed
/// in its original case.
fn subcommand_syntax_error(command: &[u8], subcommand: &[u8]) -> RespFrame {
    let mut msg = b"ERR unknown subcommand '".to_vec();
    msg.extend_from_slice(subcommand);
    msg.extend_from_slice(b"'. Try ");
    msg.extend_from_slice(command);
    msg.extend_from_slice(b" HELP.");
    err(&msg)
}

/// Parse a (non-strict) stream ID for XGROUP SETID (`streamParseIDOrReply`,
/// `t_stream.c`): a bare `ms` fills seq 0; `-`/`+` are not special here.
fn parse_stream_range_id_or_normal(raw: &[u8]) -> Result<StreamId, RespFrame> {
    parse_stream_id_strict(raw, 0).map(|(id, _)| id)
}

/// `streamEstimateDistanceFromFirstEverEntry` (`t_stream.c`): the ID's logical
/// read counter, or `SCG_INVALID_ENTRIES_READ` when unobtainable. Operates on
/// the scalar stream fields rather than the whole struct so callers can compute
/// it while holding a mutable borrow of a group.
fn estimate_distance_from_first(
    entries_added: u64,
    length: u64,
    first_id: StreamId,
    last_id: StreamId,
    max_deleted_id: StreamId,
    id: StreamId,
) -> i64 {
    if entries_added == 0 {
        return 0;
    }
    if length == 0 && id <= last_id {
        return entries_added as i64;
    }
    if id != StreamId::MIN && id < max_deleted_id {
        return SCG_INVALID_ENTRIES_READ;
    }
    if id == last_id {
        return entries_added as i64;
    } else if id > last_id {
        return SCG_INVALID_ENTRIES_READ;
    }
    if max_deleted_id == StreamId::MIN || max_deleted_id < first_id {
        if id < first_id {
            return (entries_added - length) as i64;
        } else if id == first_id {
            return (entries_added - length + 1) as i64;
        }
    }
    SCG_INVALID_ENTRIES_READ
}

/// `streamReplyWithCGLag` (`t_stream.c`): the group's lag as a RESP integer, or
/// a RESP null when not computable. Our flat model never has tombstones beyond
/// `max_deleted_id`, so `streamRangeHasTombstones` reduces to a max_deleted check.
fn stream_cg_lag(stream: &StreamValue, group: &Group) -> RespFrame {
    let entries_added = stream.entries_added;
    if entries_added == 0 {
        return RespFrame::integer(0);
    }
    let no_tombstones_ahead = group.last_delivered_id >= stream.max_deleted_id;
    if group.entries_read != SCG_INVALID_ENTRIES_READ && no_tombstones_ahead {
        return RespFrame::integer(entries_added as i64 - group.entries_read);
    }
    let estimate = estimate_distance_from_first(
        entries_added,
        stream.entries.len() as u64,
        stream.first_id,
        stream.last_id,
        stream.max_deleted_id,
        group.last_delivered_id,
    );
    if estimate != SCG_INVALID_ENTRIES_READ {
        RespFrame::integer(entries_added as i64 - estimate)
    } else {
        RespFrame::null_bulk()
    }
}

/// Parse a strict stream ID (`streamParseStrictIDOrReply`, `t_stream.c`):
/// `-`/`+` are rejected. Forms: `ms` (seq = `missing_seq`), `ms-seq`, `ms-*`
/// (auto-seq; the returned bool `seq_given` is false). Returns a WRONGTYPE-free
/// "Invalid stream ID" error frame on a malformed ID.
fn parse_stream_id_strict(raw: &[u8], missing_seq: u64) -> Result<(StreamId, bool), RespFrame> {
    if (raw == b"-" || raw == b"+") || raw.is_empty() {
        return Err(err(
            b"ERR Invalid stream ID specified as stream command argument",
        ));
    }
    parse_stream_id_inner(raw, missing_seq, true)
}

/// Parse a range-interval stream ID (`streamParseIntervalIDOrReply`,
/// `t_stream.c`). A leading `(` makes the bound exclusive (and the remainder is
/// parsed strictly). `-`/`+` map to the min/max IDs. A partial `ms` uses
/// `missing_seq` for the seq part (0 for start, UINT64_MAX for end). Returns
/// `(id, exclusive)`.
fn parse_stream_range_id(raw: &[u8], missing_seq: u64) -> Result<(StreamId, bool), RespFrame> {
    if raw.len() > 1 && raw[0] == b'(' {
        let (id, _) = parse_stream_id_strict(&raw[1..], missing_seq)?;
        return Ok((id, true));
    }
    if raw == b"-" {
        return Ok((StreamId::MIN, false));
    }
    if raw == b"+" {
        return Ok((StreamId::MAX, false));
    }
    let (id, _) = parse_stream_id_inner(raw, missing_seq, false)?;
    Ok((id, false))
}

/// Shared `<ms>[-<seq>|-*]` body parser (`streamGenericParseIDOrReply`,
/// `t_stream.c`). `allow_auto_seq` enables the `ms-*` form (only valid where a
/// `seq_given` out-param is threaded in C). Returns `(id, seq_given)`.
fn parse_stream_id_inner(
    raw: &[u8],
    missing_seq: u64,
    allow_auto_seq: bool,
) -> Result<(StreamId, bool), RespFrame> {
    let invalid = || err(b"ERR Invalid stream ID specified as stream command argument");
    let text = match std::str::from_utf8(raw) {
        Ok(t) => t,
        Err(_) => return Err(invalid()),
    };
    match text.split_once('-') {
        Some((ms_str, seq_str)) => {
            let Ok(ms) = ms_str.parse::<u64>() else {
                return Err(invalid());
            };
            if allow_auto_seq && seq_str == "*" {
                Ok((StreamId { ms, seq: 0 }, false))
            } else {
                let Ok(seq) = seq_str.parse::<u64>() else {
                    return Err(invalid());
                };
                Ok((StreamId { ms, seq }, true))
            }
        }
        None => {
            let Ok(ms) = text.parse::<u64>() else {
                return Err(invalid());
            };
            Ok((
                StreamId {
                    ms,
                    seq: missing_seq,
                },
                true,
            ))
        }
    }
}

/// Decode a snapshot `<ms>-<seq>` stream ID string. Both parts must be present
/// (the snapshot format always writes the canonical two-part form).
fn decode_stream_id(text: &str) -> Result<StreamId, SnapshotError> {
    let (ms, seq) = text
        .split_once('-')
        .ok_or(SnapshotError::InvalidField("stream_id"))?;
    let ms = ms
        .parse::<u64>()
        .map_err(|_| SnapshotError::InvalidField("stream_id"))?;
    let seq = seq
        .parse::<u64>()
        .map_err(|_| SnapshotError::InvalidField("stream_id"))?;
    Ok(StreamId { ms, seq })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, SnapshotError> {
    let bytes = text.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(SnapshotError::InvalidHex);
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = hex_nibble(pair[0]).ok_or(SnapshotError::InvalidHex)?;
        let low = hex_nibble(pair[1]).ok_or(SnapshotError::InvalidHex)?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn rest_frame_json(frame: RespFrame) -> JsonValue {
    match frame {
        RespFrame::Error(message) | RespFrame::BulkError(message) => json!({
            "error": String::from_utf8_lossy(message.as_bytes())
        }),
        other => json!({
            "result": resp_frame_result_json(other)
        }),
    }
}

fn resp_frame_result_json(frame: RespFrame) -> JsonValue {
    match frame {
        RespFrame::Simple(value)
        | RespFrame::Bulk(Some(value))
        | RespFrame::BigNumber(value)
        | RespFrame::VerbatimString { data: value, .. } => {
            JsonValue::String(String::from_utf8_lossy(value.as_bytes()).into_owned())
        }
        RespFrame::Integer(value) => json!(value),
        RespFrame::Bulk(None) | RespFrame::Array(None) | RespFrame::Null => JsonValue::Null,
        RespFrame::Array(Some(items)) | RespFrame::Push(items) | RespFrame::Set(items) => {
            JsonValue::Array(items.into_iter().map(resp_frame_result_json).collect())
        }
        RespFrame::Boolean(value) => JsonValue::Bool(value),
        RespFrame::Double(value) => json!(value),
        RespFrame::Map(pairs) | RespFrame::Attribute(pairs) => JsonValue::Array(
            pairs
                .into_iter()
                .flat_map(|(key, value)| {
                    [resp_frame_result_json(key), resp_frame_result_json(value)]
                })
                .collect(),
        ),
        RespFrame::Error(message) | RespFrame::BulkError(message) => {
            JsonValue::String(String::from_utf8_lossy(message.as_bytes()).into_owned())
        }
    }
}

fn install_sandbox(lua: &Lua) -> lua_rs_runtime::Result<()> {
    let globals = lua.globals();
    for name in [
        "io",
        "debug",
        "package",
        "require",
        "load",
        "loadfile",
        "dofile",
        "loadstring",
        "print",
        "getfenv",
        "setfenv",
    ] {
        globals.set(name, LuaValue::Nil)?;
    }
    Ok(())
}

fn install_keys_argv(lua: &Lua, keys: &[Vec<u8>], args: &[Vec<u8>]) -> lua_rs_runtime::Result<()> {
    let keys_table = lua.create_table()?;
    for (index, key) in keys.iter().enumerate() {
        keys_table.set(index as i64 + 1, lua.create_string(key)?)?;
    }
    lua.globals().set("KEYS", keys_table)?;

    let argv_table = lua.create_table()?;
    for (index, arg) in args.iter().enumerate() {
        argv_table.set(index as i64 + 1, lua.create_string(arg)?)?;
    }
    lua.globals().set("ARGV", argv_table)?;
    Ok(())
}

/// Host side of `redis.call`/`redis.pcall`. Always returns errors as
/// `{err=...}` tables; the script harness prelude turns them into raised
/// Lua errors for `redis.call`. Argument-conversion failures use the
/// reference server's wording.
fn host_call<H: Host>(
    cell: &RefCell<&mut Engine<H>>,
    lua: &Lua,
    call_args: Variadic<LuaValue>,
) -> lua_rs_runtime::Result<LuaValue> {
    let argv = match collect_lua_args(call_args) {
        Ok(argv) => argv,
        Err(_) => return error_table(lua, b"ERR Command arguments must be strings or integers"),
    };
    let mut engine = cell.borrow_mut();
    match engine.execute_inner(&argv, true) {
        RespFrame::Error(message) => error_table(lua, message.as_bytes()),
        frame => resp_to_lua(lua, &frame),
    }
}

/// Single-line prelude (so user code starts on chunk line 2) that rebinds
/// `redis.call` to raise `{err=...}` tables, then opens the function the
/// user script becomes the body of. See `run_lua_script` for why.
const SCRIPT_HARNESS_PRELUDE: &[u8] = b"local __edge_raw_pcall = redis.pcall redis.call = function(...) local __edge_reply = __edge_raw_pcall(...) if type(__edge_reply) == 'table' and __edge_reply.err ~= nil then error(__edge_reply) end return __edge_reply end local __edge_fn = function()\n";

const SCRIPT_HARNESS_SUFFIX: &[u8] = b"\nend local __edge_ok, __edge_res = pcall(__edge_fn) if __edge_ok then return __edge_res end if type(__edge_res) == 'table' and __edge_res.err ~= nil then return __edge_res end return {err='ERR ' .. tostring(__edge_res)}\n";

fn wrap_script_in_pcall_harness(script: &[u8]) -> Vec<u8> {
    let mut wrapped = Vec::with_capacity(
        SCRIPT_HARNESS_PRELUDE.len() + script.len() + SCRIPT_HARNESS_SUFFIX.len(),
    );
    wrapped.extend_from_slice(SCRIPT_HARNESS_PRELUDE);
    wrapped.extend_from_slice(script);
    wrapped.extend_from_slice(SCRIPT_HARNESS_SUFFIX);
    wrapped
}

/// Compiles `script` in a throwaway interpreter without executing any of it
/// (the probe chunk only defines a local function), so SCRIPT LOAD and EVAL
/// can reject syntax errors up front like the reference server does.
fn compile_error_message(script: &[u8]) -> Option<String> {
    let lua = Lua::new_versioned(LuaVersion::V51);
    let mut probe = Vec::with_capacity(script.len() + 40);
    probe.extend_from_slice(b"local __edge_fn = function()\n");
    probe.extend_from_slice(script);
    probe.extend_from_slice(b"\nend");
    match lua.load(probe).set_name("user_script").exec() {
        Ok(()) => None,
        Err(error) => Some(error.message_lossy()),
    }
}

fn compile_error_reply(message: &str) -> RespFrame {
    let mut msg = b"ERR Error compiling script (new function): ".to_vec();
    msg.extend_from_slice(message.as_bytes());
    err(&msg)
}

/// Numkeys validation in the reference server's check order: integer parse,
/// then the greater-than-args check, then the negative check.
fn validate_numkeys(rest: &[Vec<u8>]) -> Result<usize, RespFrame> {
    let Some(numkeys) = parse_i64(&rest[0]) else {
        return Err(err(b"ERR value is not an integer or out of range"));
    };
    let available = (rest.len() - 1) as i64;
    if numkeys > available {
        return Err(err(
            b"ERR Number of keys can't be greater than number of args",
        ));
    }
    if numkeys < 0 {
        return Err(err(b"ERR Number of keys can't be negative"));
    }
    Ok(numkeys as usize)
}

/// Formats the unknown-command error exactly like the reference server:
/// each argument rendered as 'arg' plus a trailing space, stopping once the
/// rendered argument text reaches 128 bytes.
/// Arity of every command the engine dispatches plus the transaction-control
/// verbs, mirroring the `arity` field of `reference/valkey/src/commands/*.json`
/// (via `harness/command-registry.json`). A positive value requires exactly
/// that many arguments (command name included); a negative value requires at
/// least its absolute value. `None` means the command is unknown to the engine,
/// which the queue-time validator reports as an unknown-command error — matching
/// that these are precisely the commands `execute_inner` would accept. Match is
/// case-insensitive, mirroring `lookupCommand`'s case-folding.
fn command_arity(command: &[u8]) -> Option<i64> {
    let upper = command.to_ascii_uppercase();
    let arity = match upper.as_slice() {
        b"APPEND" => 3,
        b"BITCOUNT" => -2,
        b"BITFIELD" => -2,
        b"BITFIELD_RO" => -2,
        b"BITOP" => -4,
        b"BITPOS" => -3,
        b"COPY" => -3,
        b"DECR" => 2,
        b"DECRBY" => 3,
        b"DEL" => -2,
        b"DELIFEQ" => 3,
        b"DISCARD" => 1,
        b"ECHO" => 2,
        b"EVAL" => -3,
        b"EVAL_RO" => -3,
        b"EVALSHA" => -3,
        b"EVALSHA_RO" => -3,
        b"EXEC" => 1,
        b"EXISTS" => -3,
        b"EXPIRE" => -3,
        b"EXPIREAT" => -3,
        b"EXPIRETIME" => 2,
        b"FLUSHALL" => -1,
        b"GEOADD" => -5,
        b"GEODIST" => -4,
        b"GEOHASH" => -2,
        b"GEOPOS" => -2,
        b"GEORADIUS" => -6,
        b"GEORADIUSBYMEMBER" => -5,
        b"GEORADIUSBYMEMBER_RO" => -5,
        b"GEORADIUS_RO" => -6,
        b"GEOSEARCH" => -7,
        b"GEOSEARCHSTORE" => -8,
        b"GET" => -2,
        b"GETBIT" => 3,
        b"GETDEL" => 2,
        b"GETEX" => -2,
        b"GETRANGE" => 4,
        b"GETSET" => 3,
        b"HDEL" => -3,
        b"HEXISTS" => 3,
        b"HEXPIRE" => -6,
        b"HEXPIREAT" => -6,
        b"HEXPIRETIME" => -5,
        b"HGET" => 3,
        b"HGETALL" => 2,
        b"HGETDEL" => -5,
        b"HGETEX" => -5,
        b"HINCRBY" => 4,
        b"HINCRBYFLOAT" => 4,
        b"HKEYS" => 2,
        b"HLEN" => 2,
        b"HMGET" => -3,
        b"HMSET" => -4,
        b"HPERSIST" => -5,
        b"HPEXPIRE" => -6,
        b"HPEXPIREAT" => -6,
        b"HPEXPIRETIME" => -5,
        b"HPTTL" => -5,
        b"HSCAN" => -3,
        b"HSET" => -4,
        b"HSETEX" => -6,
        b"HSETNX" => 4,
        b"HSTRLEN" => 3,
        b"HTTL" => -5,
        b"HVALS" => 2,
        b"INCR" => 2,
        b"INCRBY" => 3,
        b"LINDEX" => 3,
        b"LINSERT" => 5,
        b"LLEN" => 2,
        b"LMOVE" => 5,
        b"LMPOP" => -4,
        b"LPOP" => -2,
        b"LPOS" => -3,
        b"LPUSH" => -3,
        b"LPUSHX" => -3,
        b"LRANGE" => 4,
        b"LREM" => 4,
        b"LSET" => 4,
        b"LTRIM" => 4,
        b"MGET" => -2,
        b"MSET" => -3,
        b"MSETEX" => -4,
        b"MSETNX" => -3,
        b"MULTI" => 1,
        b"PERSIST" => 2,
        b"PEXPIRE" => -3,
        b"PEXPIREAT" => -3,
        b"PEXPIRETIME" => 2,
        b"PING" => -1,
        b"PSETEX" => 4,
        b"PTTL" => 2,
        b"RENAME" => 3,
        b"RENAMENX" => 3,
        b"RPOP" => -2,
        b"RPOPLPUSH" => 3,
        b"RPUSH" => -3,
        b"RPUSHX" => -3,
        b"SADD" => -3,
        b"SCAN" => -2,
        b"SCARD" => 2,
        b"SCRIPT" => -2,
        b"SDIFF" => -2,
        b"SDIFFSTORE" => -3,
        b"SET" => -3,
        b"SETBIT" => 4,
        b"SETEX" => 4,
        b"SETNX" => 3,
        b"SETRANGE" => 4,
        b"SINTER" => -2,
        b"SINTERCARD" => -3,
        b"SINTERSTORE" => -3,
        b"SISMEMBER" => 3,
        b"SMEMBERS" => 2,
        b"SMISMEMBER" => -3,
        b"SMOVE" => 4,
        b"SREM" => -3,
        b"SSCAN" => -3,
        b"STRLEN" => 2,
        b"SUBSTR" => 4,
        b"SUNION" => -2,
        b"SUNIONSTORE" => -3,
        b"TOUCH" => -2,
        b"TTL" => 2,
        b"TYPE" => 2,
        b"UNLINK" => -2,
        b"UNWATCH" => 1,
        b"WATCH" => -2,
        b"ZADD" => -4,
        b"ZCARD" => 2,
        b"ZCOUNT" => 4,
        b"ZDIFF" => -3,
        b"ZDIFFSTORE" => -4,
        b"ZINCRBY" => 4,
        b"ZINTER" => -3,
        b"ZINTERCARD" => -3,
        b"ZINTERSTORE" => -4,
        b"ZLEXCOUNT" => 4,
        b"ZMPOP" => -4,
        b"ZMSCORE" => -3,
        b"ZPOPMAX" => -2,
        b"ZPOPMIN" => -2,
        b"ZRANGE" => -4,
        b"ZRANGEBYLEX" => -4,
        b"ZRANGEBYSCORE" => -4,
        b"ZRANGESTORE" => -5,
        b"ZRANK" => -3,
        b"ZREM" => -3,
        b"ZSCAN" => -3,
        b"ZREMRANGEBYLEX" => 4,
        b"ZREMRANGEBYRANK" => 4,
        b"ZREMRANGEBYSCORE" => 4,
        b"ZREVRANGE" => -4,
        b"ZREVRANGEBYLEX" => -4,
        b"ZREVRANGEBYSCORE" => -4,
        b"ZREVRANK" => -3,
        b"ZSCORE" => 3,
        b"ZUNION" => -3,
        b"ZUNIONSTORE" => -4,
        _ => return None,
    };
    Some(arity)
}

/// Which keys a command touches, used to drive lazy per-key loading. A lazy
/// backing store fetches exactly the keys a command needs before executing it,
/// so a Durable Object's cold-start cost becomes O(touched) instead of
/// O(total state).
///
/// `Keys(...)` lists the exact data keys the command reads or writes, in the
/// order they appear in `argv` (duplicates preserved — the loader dedups).
/// `FullKeyspace` means the command needs the whole keyspace resident before it
/// can run: either because it enumerates keys (`SCAN`/`KEYS`/`FLUSHALL`/…) or
/// because its key set is not statically knowable from `argv`
/// (`SORT` with `BY`/`GET` patterns, `EVAL`/`FCALL` scripts that may
/// `redis.call` any key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyAccess {
    Keys(Vec<Vec<u8>>),
    FullKeyspace,
}

impl KeyAccess {
    /// Fold another command's access into this one. Used to compute the union
    /// of the commands queued inside a `MULTI`/`EXEC` transaction: the
    /// transaction touches every key any queued command touches, and degrades
    /// to `FullKeyspace` if any queued command needs the whole keyspace.
    pub fn merge(self, other: KeyAccess) -> KeyAccess {
        match (self, other) {
            (KeyAccess::FullKeyspace, _) | (_, KeyAccess::FullKeyspace) => KeyAccess::FullKeyspace,
            (KeyAccess::Keys(mut a), KeyAccess::Keys(b)) => {
                a.extend(b);
                KeyAccess::Keys(a)
            }
        }
    }
}

/// Resolve the keys a single command touches, faithful to valkey's per-command
/// `key_specs` (`reference/valkey/src/commands/*.json`). Derived from the
/// `begin_search` + `find_keys` of each spec:
///
/// - `index{pos:N}` + `range{lastkey:0}` → one key at `argv[N]`.
/// - `range{lastkey:-1, step:S}` → every `S`th key from the start position to
///   the end (`MSET`/`MSETNX` step 2; `DEL`/`MGET`/`PFCOUNT`/`WATCH` step 1).
/// - `keynum{keynumidx, firstkey, step}` → read the numkeys count then take
///   that many keys (`LMPOP`/`ZMPOP`/`SINTERCARD`/`ZINTERCARD`/`ZUNION`/… and
///   the `dst numkeys key…` store variants).
/// - `keyword{STORE|STOREDIST}` → an optional destination key after a keyword
///   (`GEORADIUS`/`GEORADIUSBYMEMBER`).
/// - the `STREAMS` keyword split for `XREAD`/`XREADGROUP`.
///
/// `FullKeyspace` is returned for keyspace-enumerating commands and,
/// conservatively, for the dynamic-key commands whose key set is not statically
/// knowable: `SORT`/`SORT_RO` (the `BY`/`GET` patterns dereference arbitrary
/// keys) and `EVAL`/`EVALSHA`/`EVAL_RO`/`EVALSHA_RO`/`FCALL`/`FCALL_RO` (a
/// script may `redis.call` any key, beyond its declared `KEYS`).
///
/// The transaction-control verbs (`MULTI`/`DISCARD`/`UNWATCH`/`EXEC`) touch no
/// data keys themselves and return `Keys(vec![])`; a lazy loader computes the
/// union of the queued commands at `EXEC` time via [`KeyAccess::merge`].
/// Connection / non-data commands (`PING`/`ECHO`/`SCRIPT`/…) also return
/// `Keys(vec![])`.
pub fn command_keys(argv: &[Vec<u8>]) -> KeyAccess {
    let Some(command) = argv.first() else {
        return KeyAccess::Keys(Vec::new());
    };
    let upper = command.to_ascii_uppercase();

    let one_key_at = |pos: usize| -> KeyAccess {
        match argv.get(pos) {
            Some(key) => KeyAccess::Keys(vec![key.clone()]),
            None => KeyAccess::Keys(Vec::new()),
        }
    };

    match upper.as_slice() {
        // ── keyspace-enumerating: need every key ────────────────────────────
        b"SCAN" | b"KEYS" | b"DBSIZE" | b"RANDOMKEY" | b"FLUSHALL" | b"FLUSHDB"
        | b"SWAPDB" => KeyAccess::FullKeyspace,

        // ── dynamic-key (key set not statically knowable) ───────────────────
        // SORT dereferences arbitrary keys via BY/GET patterns; scripts may
        // redis.call any key. Conservatively load the whole keyspace.
        b"SORT" | b"SORT_RO" | b"EVAL" | b"EVALSHA" | b"EVAL_RO" | b"EVALSHA_RO"
        | b"FCALL" | b"FCALL_RO" => KeyAccess::FullKeyspace,

        // ── transaction control: no data keys (EXEC unions the queue) ───────
        b"MULTI" | b"EXEC" | b"DISCARD" | b"UNWATCH" => KeyAccess::Keys(Vec::new()),

        // ── connection / non-data ───────────────────────────────────────────
        b"PING" | b"ECHO" | b"SCRIPT" | b"TIME" | b"OBJECT" => KeyAccess::Keys(Vec::new()),

        // ── all key arguments from pos 1, step 1 (range lastkey:-1) ──────────
        b"DEL" | b"UNLINK" | b"EXISTS" | b"TOUCH" | b"WATCH" | b"MGET" | b"PFCOUNT" => {
            KeyAccess::Keys(argv[1..].to_vec())
        }

        // ── alternating key/value from pos 1, step 2 (range lastkey:-1 step 2)
        b"MSET" | b"MSETNX" => {
            let mut keys = Vec::new();
            let mut i = 1;
            while i < argv.len() {
                keys.push(argv[i].clone());
                i += 2;
            }
            KeyAccess::Keys(keys)
        }

        // ── MSETEX numkeys key val key val … (keynum, firstkey 1, step 2) ────
        b"MSETEX" => keynum_keys(argv, 1, 2),

        // ── two distinct keys (pos 1, pos 2) ────────────────────────────────
        b"RENAME" | b"RENAMENX" | b"SMOVE" | b"LMOVE" | b"RPOPLPUSH"
        | b"GEOSEARCHSTORE" | b"ZRANGESTORE" | b"LCS" => {
            let mut keys = Vec::new();
            if let Some(k) = argv.get(1) {
                keys.push(k.clone());
            }
            if let Some(k) = argv.get(2) {
                keys.push(k.clone());
            }
            KeyAccess::Keys(keys)
        }

        // ── COPY src dst [DB n] [REPLACE] — both keys at pos 1, 2 ────────────
        b"COPY" => {
            let mut keys = Vec::new();
            if let Some(k) = argv.get(1) {
                keys.push(k.clone());
            }
            if let Some(k) = argv.get(2) {
                keys.push(k.clone());
            }
            KeyAccess::Keys(keys)
        }

        // ── dest at pos 1, then all source keys from pos 2 (range lastkey:-1)
        b"SINTERSTORE" | b"SUNIONSTORE" | b"SDIFFSTORE" | b"PFMERGE" => {
            KeyAccess::Keys(argv[1..].to_vec())
        }

        // ── BITOP op dest src… — dest at pos 2, sources from pos 3 ───────────
        b"BITOP" => {
            if argv.len() <= 2 {
                KeyAccess::Keys(Vec::new())
            } else {
                KeyAccess::Keys(argv[2..].to_vec())
            }
        }

        // ── dest at pos 1, then numkeys-counted source keys at pos 2 ─────────
        b"ZUNIONSTORE" | b"ZINTERSTORE" | b"ZDIFFSTORE" => {
            let mut keys = Vec::new();
            if let Some(dst) = argv.get(1) {
                keys.push(dst.clone());
            }
            if let KeyAccess::Keys(src) = keynum_keys(argv, 2, 1) {
                keys.extend(src);
            }
            KeyAccess::Keys(keys)
        }

        // ── numkeys-counted keys from pos 1 (keynum, firstkey 1, step 1) ─────
        b"LMPOP" | b"ZMPOP" | b"SINTERCARD" | b"ZINTERCARD" | b"ZUNION" | b"ZINTER"
        | b"ZDIFF" => keynum_keys(argv, 1, 1),

        // ── GEORADIUS / GEORADIUSBYMEMBER: source key + optional STORE dest ──
        b"GEORADIUS" | b"GEORADIUSBYMEMBER" => {
            let mut keys = Vec::new();
            if let Some(k) = argv.get(1) {
                keys.push(k.clone());
            }
            keys.extend(geo_store_dest(argv));
            KeyAccess::Keys(keys)
        }
        b"GEORADIUS_RO" | b"GEORADIUSBYMEMBER_RO" | b"GEOSEARCH" => one_key_at(1),

        // ── XREAD / XREADGROUP: keys follow the STREAMS keyword ──────────────
        b"XREAD" | b"XREADGROUP" => xread_keys(argv),

        // ── everything else: single key at pos 1, when present ──────────────
        _ => {
            if argv.len() >= 2 {
                one_key_at(1)
            } else {
                KeyAccess::Keys(Vec::new())
            }
        }
    }
}

/// Read a numkeys count at `argv[count_pos]` then collect that many keys
/// starting at `count_pos + 1` with the given step. Mirrors valkey's
/// `keynum` find-keys spec. A non-numeric or out-of-range count yields no
/// keys (the command will error at execution; loading nothing is safe).
fn keynum_keys(argv: &[Vec<u8>], count_pos: usize, step: usize) -> KeyAccess {
    let Some(raw) = argv.get(count_pos) else {
        return KeyAccess::Keys(Vec::new());
    };
    let Ok(text) = std::str::from_utf8(raw) else {
        return KeyAccess::Keys(Vec::new());
    };
    let Ok(numkeys) = text.parse::<usize>() else {
        return KeyAccess::Keys(Vec::new());
    };
    let mut keys = Vec::with_capacity(numkeys);
    let mut idx = count_pos + 1;
    for _ in 0..numkeys {
        match argv.get(idx) {
            Some(key) => keys.push(key.clone()),
            None => break,
        }
        idx += step;
    }
    KeyAccess::Keys(keys)
}

/// Find the optional `STORE`/`STOREDIST` destination key of a
/// `GEORADIUS`/`GEORADIUSBYMEMBER` command. The destination is the argument
/// immediately following the keyword.
fn geo_store_dest(argv: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let mut i = 2;
    while i < argv.len() {
        if ascii_eq(&argv[i], b"STORE") || ascii_eq(&argv[i], b"STOREDIST") {
            if let Some(dest) = argv.get(i + 1) {
                return vec![dest.clone()];
            }
        }
        i += 1;
    }
    Vec::new()
}

/// Resolve the key list of an `XREAD`/`XREADGROUP`: the arguments between the
/// `STREAMS` keyword and the trailing ids. After `STREAMS` the remaining tokens
/// are `key… id…` split exactly in half, mirroring valkey's range spec
/// (`lastkey:-1, step:1, limit:2`).
fn xread_keys(argv: &[Vec<u8>]) -> KeyAccess {
    let mut streams_at = None;
    for (i, arg) in argv.iter().enumerate().skip(1) {
        if ascii_eq(arg, b"STREAMS") {
            streams_at = Some(i);
            break;
        }
    }
    let Some(start) = streams_at else {
        return KeyAccess::Keys(Vec::new());
    };
    let rest = &argv[start + 1..];
    let key_count = rest.len() / 2;
    KeyAccess::Keys(rest[..key_count].to_vec())
}

fn unknown_command_error(command: &[u8], args: &[Vec<u8>]) -> RespFrame {
    let mut msg = b"ERR unknown command '".to_vec();
    msg.extend_from_slice(command);
    msg.extend_from_slice(b"', with args beginning with: ");
    let mut rendered = Vec::new();
    for arg in args {
        if rendered.len() >= 128 {
            break;
        }
        let budget = 128 - rendered.len();
        rendered.push(b'\'');
        rendered.extend_from_slice(&arg[..arg.len().min(budget)]);
        rendered.extend_from_slice(b"' ");
    }
    msg.extend_from_slice(&rendered);
    err(&msg)
}

fn invalid_expire_time(command: &[u8]) -> RespFrame {
    let mut msg = b"ERR invalid expire time in '".to_vec();
    msg.extend_from_slice(command);
    msg.extend_from_slice(b"' command");
    err(&msg)
}

/// The pending expiry option captured while parsing MSETEX's trailing
/// arguments, deferring the integer parse and overflow checks until after the
/// whole option list has been validated (matching `msetexCommand`'s order:
/// parse all options first, then run `getExpireMillisecondsOrReply`). `EX`/`PX`
/// are relative to now; `EXAT`/`PXAT` are absolute. `unit_ms` is the
/// multiplier that converts the raw value to milliseconds.
enum MsetexExpire {
    Relative { raw: Vec<u8>, unit_ms: u64 },
    Absolute { raw: Vec<u8>, unit_ms: u64 },
}

/// Whether `argv` is a write command for read-only-script gating. Resolves the
/// container command `XGROUP` to its subcommand exactly as valkey does
/// (`XGROUP HELP` is read-only; `CREATE`/`DESTROY`/etc. write), then defers to
/// `is_write_command` for the flat command set.
fn argv_is_write(argv: &[Vec<u8>]) -> bool {
    let command = &argv[0];
    if ascii_eq(command, b"XGROUP") {
        return match argv.get(1) {
            Some(sub) => !ascii_eq(sub, b"HELP"),
            None => false,
        };
    }
    is_write_command(command)
}

/// True for commands carrying valkey's WRITE or MAY_REPLICATE command flag,
/// among the set `execute_inner` dispatches. EVAL_RO/EVALSHA_RO reject any such
/// command issued via `redis.call`/`redis.pcall`, mirroring `scriptIsReadOnly()`
/// gating on `cmd_flags & (CMD_WRITE | CMD_MAY_REPLICATE)` (`module.c`,
/// `scriptCall` path). The check is on the static command flag, not the runtime
/// arguments, so e.g. `SORT ... ` (no STORE) and `GETEX key` (no TTL change) are
/// still rejected, exactly as valkey rejects them, while their `*_RO` siblings
/// are allowed. EVAL/EVALSHA/SCRIPT are not listed here because
/// `script_blocked_command` already rejects them from scripts first. `XGROUP` is
/// resolved per-subcommand by `argv_is_write`, not here. Derived from
/// `reference/valkey/src/commands/*.json` `command_flags`.
fn is_write_command(command: &[u8]) -> bool {
    let upper = command.to_ascii_uppercase();
    matches!(
        upper.as_slice(),
        b"APPEND"
            | b"BITFIELD"
            | b"BITOP"
            | b"COPY"
            | b"DECR"
            | b"DECRBY"
            | b"DEL"
            | b"DELIFEQ"
            | b"EXPIRE"
            | b"EXPIREAT"
            | b"FLUSHALL"
            | b"GEOADD"
            | b"GEORADIUS"
            | b"GEORADIUSBYMEMBER"
            | b"GEOSEARCHSTORE"
            | b"GETDEL"
            | b"GETEX"
            | b"GETSET"
            | b"HDEL"
            | b"HEXPIRE"
            | b"HEXPIREAT"
            | b"HGETDEL"
            | b"HGETEX"
            | b"HINCRBY"
            | b"HINCRBYFLOAT"
            | b"HMSET"
            | b"HPERSIST"
            | b"HPEXPIRE"
            | b"HPEXPIREAT"
            | b"HSET"
            | b"HSETEX"
            | b"HSETNX"
            | b"INCR"
            | b"INCRBY"
            | b"INCRBYFLOAT"
            | b"LINSERT"
            | b"LMOVE"
            | b"LMPOP"
            | b"LPOP"
            | b"LPUSH"
            | b"LPUSHX"
            | b"LREM"
            | b"LSET"
            | b"LTRIM"
            | b"MSET"
            | b"MSETEX"
            | b"MSETNX"
            | b"PERSIST"
            | b"PEXPIRE"
            | b"PEXPIREAT"
            | b"PFADD"
            | b"PFCOUNT"
            | b"PFMERGE"
            | b"PSETEX"
            | b"RENAME"
            | b"RENAMENX"
            | b"RPOP"
            | b"RPOPLPUSH"
            | b"RPUSH"
            | b"RPUSHX"
            | b"SADD"
            | b"SDIFFSTORE"
            | b"SET"
            | b"SETBIT"
            | b"SETEX"
            | b"SETNX"
            | b"SETRANGE"
            | b"SINTERSTORE"
            | b"SMOVE"
            | b"SORT"
            | b"SREM"
            | b"SUNIONSTORE"
            | b"UNLINK"
            | b"XACK"
            | b"XADD"
            | b"XDEL"
            | b"XREADGROUP"
            | b"XSETID"
            | b"XTRIM"
            | b"ZADD"
            | b"ZDIFFSTORE"
            | b"ZINCRBY"
            | b"ZINTERSTORE"
            | b"ZMPOP"
            | b"ZPOPMAX"
            | b"ZPOPMIN"
            | b"ZRANGESTORE"
            | b"ZREM"
            | b"ZREMRANGEBYLEX"
            | b"ZREMRANGEBYRANK"
            | b"ZREMRANGEBYSCORE"
            | b"ZUNIONSTORE"
    )
}

/// Which extended command grammar `parse_hash_set_options` is parsing.
/// `HSet` (HSETEX) accepts NX/XX/FNX/FXX/KEEPTTL plus EX/PX/EXAT/PXAT; `HGet`
/// (HGETEX) accepts PERSIST plus EX/PX/EXAT/PXAT. Mirrors `COMMAND_HSET` /
/// `COMMAND_HGET` in `parseExtendedCommandArgumentsOrReply` (`server.c`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashExtMode {
    HSet,
    HGet,
}

/// Parsed extended options for HSETEX / HGETEX, mirroring the flag set produced
/// by `parseExtendedCommandArgumentsOrReply`. `expire` carries the chosen
/// EX/PX/EXAT/PXAT unit, raw value, and whether it is an absolute timestamp.
#[derive(Debug, Default)]
struct HashSetOptions {
    key_nx: bool,
    key_xx: bool,
    fnx: bool,
    fxx: bool,
    keepttl: bool,
    persist: bool,
    expire: Option<(ExpireUnit, i64, bool)>,
}

/// Locate the mandatory `FIELDS` keyword in HGETEX/HSETEX, scanning the option
/// region. Mirrors the C scan loop: the keyword cannot be the last argument
/// (it must be followed by numfields), so the scan stops at `argc - 1`.
fn find_fields_keyword(argv: &[Vec<u8>]) -> Option<usize> {
    (2..argv.len().saturating_sub(1)).find(|&index| ascii_eq(&argv[index], b"FIELDS"))
}

/// Parse the EX/PX/EXAT/PXAT/KEEPTTL/PERSIST/NX/XX/FNX/FXX option region of
/// HSETEX/HGETEX (the args between the key and the FIELDS keyword), mirroring
/// `parseExtendedCommandArgumentsOrReply` for `COMMAND_HSET`/`COMMAND_HGET`.
/// Every conflict (two expiry sources, EX without a value, NX with XX, a flag
/// not valid for the mode, an unknown token) is the single `ERR syntax error`,
/// matching the reference's `shared.syntaxerr`.
fn parse_hash_set_options(
    opts: &[Vec<u8>],
    mode: HashExtMode,
) -> Result<HashSetOptions, RespFrame> {
    let mut parsed = HashSetOptions::default();
    let syntax = || Err(err(b"ERR syntax error"));
    let mut index = 0;
    while index < opts.len() {
        let opt = &opts[index];
        let has_next = index + 1 < opts.len();
        let expiry_chosen = parsed.expire.is_some();
        if ascii_eq(opt, b"NX")
            && mode == HashExtMode::HSet
            && !parsed.key_xx
        {
            parsed.key_nx = true;
        } else if ascii_eq(opt, b"XX") && mode == HashExtMode::HSet && !parsed.key_nx {
            parsed.key_xx = true;
        } else if ascii_eq(opt, b"FNX") && mode == HashExtMode::HSet && !parsed.fxx {
            parsed.fnx = true;
        } else if ascii_eq(opt, b"FXX") && mode == HashExtMode::HSet && !parsed.fnx {
            parsed.fxx = true;
        } else if ascii_eq(opt, b"KEEPTTL")
            && mode == HashExtMode::HSet
            && !parsed.persist
            && !expiry_chosen
        {
            parsed.keepttl = true;
        } else if ascii_eq(opt, b"PERSIST")
            && mode == HashExtMode::HGet
            && !parsed.keepttl
            && !expiry_chosen
        {
            parsed.persist = true;
        } else if ascii_eq(opt, b"EX")
            && !parsed.keepttl
            && !parsed.persist
            && !expiry_chosen
            && has_next
        {
            parsed.expire = parse_field_expire_value(&opts[index + 1], ExpireUnit::Seconds, false)?;
            index += 1;
        } else if ascii_eq(opt, b"PX")
            && !parsed.keepttl
            && !parsed.persist
            && !expiry_chosen
            && has_next
        {
            parsed.expire =
                parse_field_expire_value(&opts[index + 1], ExpireUnit::Milliseconds, false)?;
            index += 1;
        } else if ascii_eq(opt, b"EXAT")
            && !parsed.keepttl
            && !parsed.persist
            && !expiry_chosen
            && has_next
        {
            parsed.expire = parse_field_expire_value(&opts[index + 1], ExpireUnit::Seconds, true)?;
            index += 1;
        } else if ascii_eq(opt, b"PXAT")
            && !parsed.keepttl
            && !parsed.persist
            && !expiry_chosen
            && has_next
        {
            parsed.expire =
                parse_field_expire_value(&opts[index + 1], ExpireUnit::Milliseconds, true)?;
            index += 1;
        } else {
            return syntax();
        }
        index += 1;
    }
    Ok(parsed)
}

/// Validate the integer accompanying an EX/PX/EXAT/PXAT flag. The C parser
/// stores the raw object and only converts it later, but a non-integer here is
/// ultimately a "value is not an integer or out of range" error; we parse
/// eagerly and surface that exact message.
fn parse_field_expire_value(
    bytes: &[u8],
    unit: ExpireUnit,
    absolute: bool,
) -> Result<Option<(ExpireUnit, i64, bool)>, RespFrame> {
    match parse_i64(bytes) {
        Some(value) => Ok(Some((unit, value, absolute))),
        None => Err(err(b"ERR value is not an integer or out of range")),
    }
}

/// `proto-max-bulk-len` default (512 MiB): the configured ceiling on a string
/// value's byte length, used by SETRANGE/APPEND/SETBIT growth checks.
const PROTO_MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

/// Mirror of `checkStringLength` (`t_string.c`): returns an error frame when
/// growing a string to `size + append` bytes would exceed `proto-max-bulk-len`
/// or overflow, otherwise `None`.
fn check_string_length(size: i64, append: i64) -> Option<RespFrame> {
    let total = (size as i128) + (append as i128);
    if total > PROTO_MAX_BULK_LEN as i128 || total < size as i128 || total < append as i128 {
        return Some(err(
            b"ERR string exceeds maximum allowed size (proto-max-bulk-len)",
        ));
    }
    None
}

/// Parse a GETBIT/SETBIT bit offset (`getBitOffsetFromArgument`, `bitops.c`).
/// Rejects non-integers, negatives, and offsets whose byte index reaches the
/// proto byte ceiling, all with the same error text.
fn get_bit_offset_from_arg(arg: &[u8]) -> Result<u64, RespFrame> {
    const ERR: &[u8] = b"ERR bit offset is not an integer or out of range";
    let Some(loffset) = parse_i64(arg) else {
        return Err(err(ERR));
    };
    if loffset < 0 || (loffset >> 3) >= PROTO_MAX_BULK_LEN {
        return Err(err(ERR));
    }
    Ok(loffset as u64)
}

/// Count set bits across the whole slice (`serverPopcount`, `bitops.c`).
fn server_popcount(data: &[u8]) -> i64 {
    data.iter().map(|b| b.count_ones() as i64).sum()
}

/// Position of the first bit equal to `bit` within `data[..count]`
/// (`serverBitpos`, `bitops.c`). Returns `count * 8` when searching for a clear
/// bit and none is found (the string is treated as zero-padded to the right),
/// or `-1` when searching for a set bit and none is found.
fn server_bitpos(data: &[u8], count: usize, bit: i32) -> i64 {
    let target = bit != 0;
    let skip_byte: u8 = if target { 0x00 } else { 0xFF };
    let count = count.min(data.len());
    let mut pos: i64 = 0;
    let mut i = 0usize;
    while i < count && data[i] == skip_byte {
        pos += 8;
        i += 1;
    }
    if i >= count {
        return if target { -1 } else { pos };
    }
    let byte = data[i];
    for shift in (0u32..8).rev() {
        let is_set = (byte >> shift) & 1 != 0;
        if is_set == target {
            return pos;
        }
        pos += 1;
    }
    pos
}

/// BITFIELD overflow mode (`BFOVERFLOW_*`, `bitops.c`). Sets how SET/INCRBY
/// behave when the result exceeds the field's signed/unsigned range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BfOverflow {
    Wrap,
    Sat,
    Fail,
}

/// A parsed BITFIELD subcommand (`struct bitfieldOp`, `bitops.c`).
struct BitfieldOp {
    offset: u64,
    i64: i64,
    opcode: BitfieldOpcode,
    owtype: BfOverflow,
    bits: u32,
    sign: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitfieldOpcode {
    Get,
    Set,
    IncrBy,
}

/// BITOP operation kind (`BITOP_*`, `bitops.c`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitopKind {
    And,
    Or,
    Xor,
    Not,
}

/// Write `value`'s low `bits` bits big-endian-msb-first starting at bit
/// `offset` (`setUnsignedBitfield`, `bitops.c`). `p` must be large enough.
fn set_unsigned_bitfield(p: &mut [u8], mut offset: u64, bits: u32, value: u64) {
    for j in 0..bits {
        let bitval = ((value & (1u64 << (bits - 1 - j))) != 0) as u8;
        let byte = (offset >> 3) as usize;
        let bit = 7 - (offset & 0x7);
        let mut byteval = p[byte];
        byteval &= !(1u8 << bit);
        byteval |= bitval << bit;
        p[byte] = byteval;
        offset += 1;
    }
}

/// Signed counterpart of `set_unsigned_bitfield` (`setSignedBitfield`).
fn set_signed_bitfield(p: &mut [u8], offset: u64, bits: u32, value: i64) {
    set_unsigned_bitfield(p, offset, bits, value as u64);
}

/// Read `bits` bits big-endian-msb-first at bit `offset`
/// (`getUnsignedBitfield`, `bitops.c`). `p` must be large enough.
fn get_unsigned_bitfield(p: &[u8], mut offset: u64, bits: u32) -> u64 {
    let mut value: u64 = 0;
    for _ in 0..bits {
        let byte = (offset >> 3) as usize;
        let bit = 7 - (offset & 0x7);
        let bitval = (p[byte] >> bit) & 1;
        value = (value << 1) | bitval as u64;
        offset += 1;
    }
    value
}

/// Sign-extending counterpart of `get_unsigned_bitfield`
/// (`getSignedBitfield`, `bitops.c`).
fn get_signed_bitfield(p: &[u8], offset: u64, bits: u32) -> i64 {
    let mut value = get_unsigned_bitfield(p, offset, bits) as i64;
    if bits < 64 && (value & (1i64 << (bits - 1))) != 0 {
        value |= (-1i64) << bits;
    }
    value
}

/// `checkUnsignedBitfieldOverflow` (`bitops.c`). Returns `(overflow, limit)`
/// where `overflow` is true on over/underflow and `limit` is the value to
/// store under the requested overflow mode (only meaningful when `overflow`).
fn check_unsigned_bitfield_overflow(
    value: u64,
    incr: i64,
    bits: u32,
    owtype: BfOverflow,
) -> (bool, u64) {
    let max = if bits == 64 { u64::MAX } else { (1u64 << bits) - 1 };
    let maxincr = max.wrapping_sub(value) as i64;
    let minincr = (0u64.wrapping_sub(value)) as i64;
    let handle_wrap = || {
        let mask = (-1i64 as u64) << bits;
        (value.wrapping_add(incr as u64)) & !mask
    };
    if value > max || (incr > 0 && incr > maxincr) {
        let limit = match owtype {
            BfOverflow::Wrap => handle_wrap(),
            BfOverflow::Sat => max,
            BfOverflow::Fail => 0,
        };
        return (true, limit);
    } else if incr < 0 && incr < minincr {
        let limit = match owtype {
            BfOverflow::Wrap => handle_wrap(),
            BfOverflow::Sat => 0,
            BfOverflow::Fail => 0,
        };
        return (true, limit);
    }
    (false, 0)
}

/// `checkSignedBitfieldOverflow` (`bitops.c`). Returns `(overflow, limit)`.
fn check_signed_bitfield_overflow(
    value: i64,
    incr: i64,
    bits: u32,
    owtype: BfOverflow,
) -> (bool, i64) {
    let max = if bits == 64 {
        i64::MAX
    } else {
        (1i64 << (bits - 1)) - 1
    };
    let min = (-max) - 1;
    let maxincr = (max as u64).wrapping_sub(value as u64) as i64;
    let minincr = (min as u64).wrapping_sub(value as u64) as i64;
    let handle_wrap = || {
        let msb = 1u64 << (bits - 1);
        let mut c = (value as u64).wrapping_add(incr as u64);
        if bits < 64 {
            let mask = (-1i64 as u64) << bits;
            if c & msb != 0 {
                c |= mask;
            } else {
                c &= !mask;
            }
        }
        c as i64
    };
    if value > max
        || (bits != 64 && incr > maxincr)
        || (value >= 0 && incr > 0 && incr > maxincr)
    {
        let limit = match owtype {
            BfOverflow::Wrap => handle_wrap(),
            BfOverflow::Sat => max,
            BfOverflow::Fail => 0,
        };
        return (true, limit);
    } else if value < min
        || (bits != 64 && incr < minincr)
        || (value < 0 && incr < 0 && incr < minincr)
    {
        let limit = match owtype {
            BfOverflow::Wrap => handle_wrap(),
            BfOverflow::Sat => min,
            BfOverflow::Fail => 0,
        };
        return (true, limit);
    }
    (false, 0)
}

/// Parse a BITFIELD `<sign><bits>` type argument (`getBitfieldTypeFromArgument`,
/// `bitops.c`). Returns `(sign, bits)` where `sign` is true for signed. Unsigned
/// is capped at 63 bits, signed at 64, both ≥ 1.
fn get_bitfield_type_from_argument(arg: &[u8]) -> Result<(bool, u32), RespFrame> {
    const ERR: &[u8] = b"ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.";
    let sign = match arg.first() {
        Some(b'i') => true,
        Some(b'u') => false,
        _ => return Err(err(ERR)),
    };
    let Some(llbits) = parse_i64(&arg[1..]) else {
        return Err(err(ERR));
    };
    if llbits < 1 || (sign && llbits > 64) || (!sign && llbits > 63) {
        return Err(err(ERR));
    }
    Ok((sign, llbits as u32))
}

/// Parse a BITFIELD bit-offset argument supporting the `#<n>` form
/// (`getBitOffsetFromArgument` with `hash=1`, `bitops.c`). `#<n>` multiplies
/// the parsed value by `bits`.
fn get_bitfield_offset_from_argument(arg: &[u8], bits: u32) -> Result<u64, RespFrame> {
    const ERR: &[u8] = b"ERR bit offset is not an integer or out of range";
    let usehash = matches!(arg.first(), Some(b'#')) && bits > 0;
    let body = if usehash { &arg[1..] } else { arg };
    let Some(mut loffset) = parse_i64(body) else {
        return Err(err(ERR));
    };
    if usehash {
        loffset = loffset.wrapping_mul(bits as i64);
    }
    if loffset < 0 || (loffset >> 3) >= PROTO_MAX_BULK_LEN {
        return Err(err(ERR));
    }
    Ok(loffset as u64)
}

/// Parse the trailing NX|XX|GT|LT flags of the EXPIRE family, mirroring
/// `parseExtendedExpireArgumentsOrReply` (`expire.c`). Returns the resolved
/// condition (the edge engine accepts at most one), or an error frame for an
/// unsupported option or an incompatible flag combination. Empty `opts` yields
/// `None` (no condition).
fn parse_expire_condition(opts: &[Vec<u8>]) -> Result<Option<ExpireCondition>, RespFrame> {
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut chosen = None;
    for opt in opts {
        if ascii_eq(opt, b"NX") {
            nx = true;
            chosen = Some(ExpireCondition::Nx);
        } else if ascii_eq(opt, b"XX") {
            xx = true;
            chosen = Some(ExpireCondition::Xx);
        } else if ascii_eq(opt, b"GT") {
            gt = true;
            chosen = Some(ExpireCondition::Gt);
        } else if ascii_eq(opt, b"LT") {
            lt = true;
            chosen = Some(ExpireCondition::Lt);
        } else {
            let mut msg = b"ERR Unsupported option ".to_vec();
            msg.extend_from_slice(opt);
            return Err(err(&msg));
        }
    }
    if (nx && xx) || (nx && gt) || (nx && lt) {
        return Err(err(
            b"ERR NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return Err(err(
            b"ERR GT and LT options at the same time are not compatible",
        ));
    }
    Ok(chosen)
}

fn old_string_reply(old_string: Option<Vec<u8>>) -> RespFrame {
    match old_string {
        Some(value) => bulk(value),
        None => RespFrame::null_bulk(),
    }
}

fn apply_zadd(
    members: &mut HashMap<Vec<u8>, f64>,
    scored: Vec<(f64, Vec<u8>)>,
    nx: bool,
    xx: bool,
) -> (i64, i64) {
    let mut added = 0;
    let mut updated = 0;
    for (score, member) in scored {
        match members.get_mut(&member) {
            Some(existing) => {
                if !nx && *existing != score {
                    *existing = score;
                    updated += 1;
                }
            }
            None => {
                if !xx {
                    members.insert(member, score);
                    added += 1;
                }
            }
        }
    }
    (added, updated)
}

/// Combine one member's running `target` score with a new contribution `val`
/// under `aggregate`, mirroring `zunionInterAggregate` (`t_zset.c`). SUM keeps
/// the valkey rule that `+inf + -inf` collapses to `0` instead of NaN.
fn zunion_inter_aggregate(target: &mut f64, val: f64, aggregate: ZAggregate) {
    match aggregate {
        ZAggregate::Sum => {
            *target += val;
            if target.is_nan() {
                *target = 0.0;
            }
        }
        ZAggregate::Min => {
            if val < *target {
                *target = val;
            }
        }
        ZAggregate::Max => {
            if val > *target {
                *target = val;
            }
        }
    }
}

/// Computes a ZUNION/ZINTER/ZDIFF result map from the already-materialised
/// `sources` (missing keys are empty maps, sets already expanded to score
/// `1.0`), applying per-source `weights` and the `aggregate` rule. Mirrors the
/// score math of `zunionInterDiffGenericCommand`: each source score is
/// multiplied by its weight before aggregation, a weighted score of NaN becomes
/// `0`, INTER keeps only members present in every source, and DIFF keeps the
/// first source's members (and unweighted scores) absent from all later
/// sources. The result is unordered; callers sort it for replies.
fn compute_zset_op(
    op: ZSetOp,
    sources: &[HashMap<Vec<u8>, f64>],
    weights: &[f64],
    aggregate: ZAggregate,
) -> HashMap<Vec<u8>, f64> {
    match op {
        ZSetOp::Union => {
            let mut result: HashMap<Vec<u8>, f64> = HashMap::new();
            for (index, source) in sources.iter().enumerate() {
                for (member, raw) in source {
                    let mut score = weights[index] * raw;
                    if score.is_nan() {
                        score = 0.0;
                    }
                    match result.get_mut(member) {
                        Some(existing) => zunion_inter_aggregate(existing, score, aggregate),
                        None => {
                            result.insert(member.clone(), score);
                        }
                    }
                }
            }
            result
        }
        ZSetOp::Inter => {
            let mut result: HashMap<Vec<u8>, f64> = HashMap::new();
            let Some(first_index) = (0..sources.len()).min_by_key(|index| sources[*index].len())
            else {
                return result;
            };
            if sources[first_index].is_empty() {
                return result;
            }
            for (member, raw) in &sources[first_index] {
                let mut score = weights[first_index] * raw;
                if score.is_nan() {
                    score = 0.0;
                }
                let mut present_in_all = true;
                for (index, source) in sources.iter().enumerate() {
                    if index == first_index {
                        continue;
                    }
                    match source.get(member) {
                        Some(other) => {
                            let mut value = weights[index] * other;
                            if value.is_nan() {
                                value = 0.0;
                            }
                            zunion_inter_aggregate(&mut score, value, aggregate);
                        }
                        None => {
                            present_in_all = false;
                            break;
                        }
                    }
                }
                if present_in_all {
                    result.insert(member.clone(), score);
                }
            }
            result
        }
        ZSetOp::Diff => {
            let mut result: HashMap<Vec<u8>, f64> = HashMap::new();
            let Some(first) = sources.first() else {
                return result;
            };
            for (member, raw) in first {
                if sources[1..].iter().any(|source| source.contains_key(member)) {
                    continue;
                }
                result.insert(member.clone(), *raw);
            }
            result
        }
    }
}

/// Ascending (score, then member-lexicographic) order, the canonical zset
/// ordering used by ZRANK and ZRANGE.
fn sorted_zset_entries(members: &HashMap<Vec<u8>, f64>) -> Vec<(Vec<u8>, f64)> {
    let mut entries: Vec<(Vec<u8>, f64)> = members
        .iter()
        .map(|(member, score)| (member.clone(), *score))
        .collect();
    entries.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .expect("zset scores are never NaN")
            .then_with(|| left.0.cmp(&right.0))
    });
    entries
}

/// One side of a `ZRANGEBYSCORE` score interval: a finite-or-infinite bound
/// value plus whether the bound itself is excluded (the leading `(` form).
/// Mirrors the reference `zrangespec` min/max + minex/maxex fields.
struct ScoreBound {
    value: f64,
    exclusive: bool,
}

impl ScoreBound {
    /// Whether `score` satisfies this bound used as the interval minimum,
    /// matching the reference `zslValueGteMin` (`>` when exclusive, else `>=`).
    fn gte_min(&self, score: f64) -> bool {
        if self.exclusive {
            score > self.value
        } else {
            score >= self.value
        }
    }

    /// Whether `score` satisfies this bound used as the interval maximum,
    /// matching the reference `zslValueLteMax` (`<` when exclusive, else `<=`).
    fn lte_max(&self, score: f64) -> bool {
        if self.exclusive {
            score < self.value
        } else {
            score <= self.value
        }
    }
}

/// Parses one `ZRANGEBYSCORE` bound exactly like the reference `zslParseRange`
/// over `valkey_strtod_n`: a leading `(` marks the bound exclusive, the rest is
/// a float where the inf/infinity spellings (any case, optional sign) are
/// accepted, an empty body parses as `0.0`, and a `NaN` or trailing-garbage
/// body is rejected.
fn parse_score_bound(bytes: &[u8]) -> Option<ScoreBound> {
    let (exclusive, body) = match bytes.split_first() {
        Some((b'(', rest)) => (true, rest),
        _ => (false, bytes),
    };
    if body.is_empty() {
        return Some(ScoreBound {
            value: 0.0,
            exclusive,
        });
    }
    let text = std::str::from_utf8(body).ok()?;
    let value: f64 = text.parse().ok()?;
    if value.is_nan() {
        return None;
    }
    Some(ScoreBound { value, exclusive })
}

/// One side of a `ZRANGEBYLEX`/`ZLEXCOUNT`/`ZREMRANGEBYLEX` lex interval,
/// mirroring the reference `zlexrangespec` after `zslParseLexRangeItem`: the
/// bare `-`/`+` sentinels stand for "less than every member" / "greater than
/// every member", a `[member` bound is inclusive and a `(member` bound is
/// exclusive. Comparison is byte-wise (`sdscmp`).
enum LexBound {
    Min,
    Max,
    Inclusive(Vec<u8>),
    Exclusive(Vec<u8>),
}

impl LexBound {
    /// Whether `member` satisfies this bound used as the interval minimum,
    /// matching `zslLexValueGteMin` (`>` when exclusive, else `>=`).
    fn gte_min(&self, member: &[u8]) -> bool {
        match self {
            LexBound::Min => true,
            LexBound::Max => false,
            LexBound::Inclusive(bound) => member >= bound.as_slice(),
            LexBound::Exclusive(bound) => member > bound.as_slice(),
        }
    }

    /// Whether `member` satisfies this bound used as the interval maximum,
    /// matching `zslLexValueLteMax` (`<` when exclusive, else `<=`).
    fn lte_max(&self, member: &[u8]) -> bool {
        match self {
            LexBound::Min => false,
            LexBound::Max => true,
            LexBound::Inclusive(bound) => member <= bound.as_slice(),
            LexBound::Exclusive(bound) => member < bound.as_slice(),
        }
    }
}

/// Parses one lex bound exactly like the reference `zslParseLexRangeItem`: a
/// bare `+`/`-` is the max/min sentinel (any trailing byte is an error), a
/// leading `[`/`(` marks the rest inclusive/exclusive, and anything else
/// (including an empty argument) is rejected with the string-range error.
fn parse_lex_bound(bytes: &[u8]) -> Option<LexBound> {
    match bytes.split_first() {
        Some((b'+', rest)) if rest.is_empty() => Some(LexBound::Max),
        Some((b'-', rest)) if rest.is_empty() => Some(LexBound::Min),
        Some((b'[', rest)) => Some(LexBound::Inclusive(rest.to_vec())),
        Some((b'(', rest)) => Some(LexBound::Exclusive(rest.to_vec())),
        _ => None,
    }
}

/// Parses a `LIMIT` offset/count argument the way `getLongFromObjectOrReply`
/// does for these commands: a base-10 signed integer with no surrounding
/// whitespace or trailing characters. Anything else yields the integer error.
fn parse_limit_arg(bytes: &[u8]) -> Option<i64> {
    parse_i64(bytes)
}

/// Applies the `LIMIT offset count` window to an already in-range, ascending
/// slice, mirroring the reference forward-direction loop: a negative offset
/// (or one at/after the end) yields nothing, and a negative count returns all
/// remaining elements after the offset.
fn apply_score_limit(
    in_range: &[(Vec<u8>, f64)],
    limit: Option<(i64, i64)>,
) -> Vec<&(Vec<u8>, f64)> {
    let Some((offset, count)) = limit else {
        return in_range.iter().collect();
    };
    if offset < 0 {
        return Vec::new();
    }
    let offset = offset as usize;
    if offset >= in_range.len() {
        return Vec::new();
    }
    let tail = &in_range[offset..];
    if count < 0 {
        tail.iter().collect()
    } else {
        tail.iter().take(count as usize).collect()
    }
}

/// The reference server keeps small zsets listpack-encoded; listpack scores
/// round-trip through their decimal string, where "-0" int-encodes to 0, so
/// a stored negative zero loses its sign (only the transient ZINCRBY reply
/// keeps it). Stored scores are normalized the same way here.
fn normalize_zero(score: f64) -> f64 {
    if score == 0.0 {
        0.0
    } else {
        score
    }
}

/// Score parser matching the reference server: integer-looking input goes
/// through the long-long path first (the server int-encodes such arguments,
/// which turns "-0" into +0.0), everything else through a string2d-style
/// float parse that rejects garbage and NaN but accepts the inf/infinity
/// spellings in any case.
fn parse_score(bytes: &[u8]) -> Option<f64> {
    if let Some(integer) = parse_i64(bytes) {
        return Some(integer as f64);
    }
    let text = std::str::from_utf8(bytes).ok()?;
    let value: f64 = text.parse().ok()?;
    if value.is_nan() {
        return None;
    }
    Some(value)
}

/// Renders a score exactly like the reference server's d2string: "inf" and
/// "-inf" for infinities, "0"/"-0" for zeroes, plain integers via the
/// double2ll fast path, and otherwise shortest round-trip digits laid out
/// with the plain/decimal/scientific thresholds of fpconv's emit_digits.
fn format_score(value: f64) -> Vec<u8> {
    if value.is_nan() {
        return b"nan".to_vec();
    }
    if value.is_infinite() {
        return if value < 0.0 {
            b"-inf".to_vec()
        } else {
            b"inf".to_vec()
        };
    }
    if value == 0.0 {
        return if value.is_sign_negative() {
            b"-0".to_vec()
        } else {
            b"0".to_vec()
        };
    }
    let integer_bound = (i64::MAX / 2) as f64;
    if (-integer_bound..=integer_bound).contains(&value) && value == (value as i64) as f64 {
        return (value as i64).to_string().into_bytes();
    }
    let exponential = format!("{:e}", value.abs());
    let (mantissa, exponent) = exponential
        .split_once('e')
        .expect("LowerExp always emits an exponent");
    let exp10: i32 = exponent.parse().expect("LowerExp exponent is an integer");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let ndigits = digits.len() as i32;
    let k = exp10 - (ndigits - 1);
    let mut out = String::new();
    if value < 0.0 {
        out.push('-');
    }
    if k >= 0 && exp10 < ndigits + 7 {
        out.push_str(&digits);
        for _ in 0..k {
            out.push('0');
        }
    } else if k < 0 && (k > -7 || exp10.abs() < 4) {
        let offset = ndigits + k;
        if offset <= 0 {
            out.push_str("0.");
            for _ in 0..(-offset) {
                out.push('0');
            }
            out.push_str(&digits);
        } else {
            out.push_str(&digits[..offset as usize]);
            out.push('.');
            out.push_str(&digits[offset as usize..]);
        }
    } else {
        out.push_str(&digits[..1]);
        if ndigits > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp10 < 0 { '-' } else { '+' });
        out.push_str(&exp10.abs().to_string());
    }
    out.into_bytes()
}

/// Snapshots serialize scores as the same canonical string used in replies;
/// both the integer fast path and the shortest-digit path round-trip f64
/// exactly through `parse_score`.
fn score_snapshot_string(score: f64) -> String {
    String::from_utf8(format_score(score)).expect("score strings are ASCII")
}

fn resp_to_lua(lua: &Lua, frame: &RespFrame) -> lua_rs_runtime::Result<LuaValue> {
    match frame {
        RespFrame::Simple(value) => {
            let table = lua.create_table()?;
            table.set("ok", lua.create_string(value.as_bytes())?)?;
            Ok(LuaValue::Table(table))
        }
        RespFrame::Error(value) | RespFrame::BulkError(value) => {
            let table = lua.create_table()?;
            table.set("err", lua.create_string(value.as_bytes())?)?;
            Ok(LuaValue::Table(table))
        }
        RespFrame::Integer(value) => Ok(LuaValue::Integer(*value)),
        RespFrame::Bulk(Some(value)) => Ok(LuaValue::String(lua.create_string(value.as_bytes())?)),
        RespFrame::Bulk(None) | RespFrame::Null => Ok(LuaValue::Boolean(false)),
        RespFrame::Array(Some(items)) | RespFrame::Push(items) | RespFrame::Set(items) => {
            let table = lua.create_table()?;
            for (index, item) in items.iter().enumerate() {
                table.set(index as i64 + 1, resp_to_lua(lua, item)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        RespFrame::Array(None) => Ok(LuaValue::Boolean(false)),
        RespFrame::Boolean(value) => Ok(LuaValue::Boolean(*value)),
        RespFrame::Double(value) => Ok(LuaValue::Number(*value)),
        RespFrame::BigNumber(value) => Ok(LuaValue::String(lua.create_string(value.as_bytes())?)),
        RespFrame::VerbatimString { data, .. } => {
            Ok(LuaValue::String(lua.create_string(data.as_bytes())?))
        }
        RespFrame::Map(pairs) | RespFrame::Attribute(pairs) => {
            let table = lua.create_table()?;
            let mut index = 1_i64;
            for (key, value) in pairs {
                table.set(index, resp_to_lua(lua, key)?)?;
                table.set(index + 1, resp_to_lua(lua, value)?)?;
                index += 2;
            }
            Ok(LuaValue::Table(table))
        }
    }
}

fn lua_to_resp(value: &LuaValue) -> lua_rs_runtime::Result<RespFrame> {
    match value {
        LuaValue::Nil => Ok(RespFrame::null_bulk()),
        LuaValue::Boolean(true) => Ok(RespFrame::integer(1)),
        LuaValue::Boolean(false) => Ok(RespFrame::null_bulk()),
        LuaValue::Integer(value) => Ok(RespFrame::integer(*value)),
        LuaValue::Number(value) => Ok(RespFrame::integer(*value as i64)),
        LuaValue::String(value) => Ok(bulk(value.as_bytes()?)),
        LuaValue::Table(table) => {
            if let Some(message) = table_string_bytes(table, "err")? {
                return Ok(err(&message));
            }
            if let Some(message) = table_string_bytes(table, "ok")? {
                return Ok(simple(&message));
            }

            let mut items = Vec::new();
            let mut index = 1_i64;
            loop {
                let item = table.get::<_, LuaValue>(index)?;
                if matches!(item, LuaValue::Nil) {
                    break;
                }
                items.push(lua_to_resp(&item)?);
                index += 1;
            }
            Ok(RespFrame::array(items))
        }
        _ => Ok(RespFrame::null_bulk()),
    }
}

fn table_string_bytes(table: &LuaTable, key: &str) -> lua_rs_runtime::Result<Option<Vec<u8>>> {
    match table.get::<_, Option<LuaString>>(key)? {
        Some(value) => Ok(Some(value.as_bytes()?)),
        None => Ok(None),
    }
}

fn collect_lua_args(args: Variadic<LuaValue>) -> lua_rs_runtime::Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(args.len());
    for value in args {
        out.push(lua_arg_to_bytes(&value)?);
    }
    Ok(out)
}

fn lua_arg_to_bytes(value: &LuaValue) -> lua_rs_runtime::Result<Vec<u8>> {
    match value {
        LuaValue::String(value) => value.as_bytes(),
        LuaValue::Integer(value) => Ok(value.to_string().into_bytes()),
        LuaValue::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok((*value as i64).to_string().into_bytes())
        }
        LuaValue::Number(value) if value.is_finite() => Ok(value.to_string().into_bytes()),
        _ => Err(lua_runtime_error(
            "command arguments must be strings or integers",
        )
        .into()),
    }
}

fn error_table(lua: &Lua, message: &[u8]) -> lua_rs_runtime::Result<LuaValue> {
    let table = lua.create_table()?;
    table.set("err", lua.create_string(message)?)?;
    Ok(LuaValue::Table(table))
}

fn lua_runtime_error(message: &str) -> LuaError {
    LuaError::runtime(format_args!("{message}"))
}

fn script_blocked_command(command: &[u8]) -> bool {
    ascii_eq(command, b"EVAL") || ascii_eq(command, b"EVALSHA") || ascii_eq(command, b"SCRIPT")
}

fn simple(value: &[u8]) -> RespFrame {
    RespFrame::simple(RedisString::from_bytes(value))
}

// ── HyperLogLog (PFADD / PFCOUNT / PFMERGE) ──────────────────────────────────
//
// A faithful port of valkey's `hyperloglog.c` cardinality machinery, reduced to
// the dense-only path the edge engine needs. An HLL is "just a string": these
// functions operate on the raw `Vec<u8>` held in `StoredValue::String`, in the
// exact `HYLL`-header + 16384×6-bit-register byte format valkey uses. Storing
// dense-only is observationally identical to valkey's sparse form for
// PFADD/PFCOUNT/PFMERGE: the registers (hence the estimate) are the same, and
// valkey auto-promotes sparse→dense anyway. We never expose the raw bytes via
// GET on an HLL key, so the on-disk encoding difference is invisible.
//
// Hash: `MurmurHash64A`, seed `0xadc83b19` (`MurmurHash64A`, `hyperloglog.c`).
// Estimator: Ertl loglog-beta sigma/tau (`hllSigma`/`hllTau`/`hllCount`,
// arXiv:1702.01284) — the exact estimator the pinned valkey uses, so the
// integer estimate is byte-for-byte identical at every cardinality.

/// Precision: bits used to index a register (`HLL_P`, `hyperloglog.c`).
const HLL_P: u32 = 14;
/// Bits for the leading-zero count (`HLL_Q` = 64 - P = 50).
const HLL_Q: u32 = 64 - HLL_P;
/// Number of registers: 2^14 = 16384 (`HLL_REGISTERS`).
const HLL_REGISTERS: usize = 1 << HLL_P;
/// Mask to extract the register index from a hash (`HLL_P_MASK`).
const HLL_P_MASK: u64 = (HLL_REGISTERS as u64) - 1;
/// Bits stored per register (`HLL_BITS`).
const HLL_BITS: u32 = 6;
/// Largest value a 6-bit register can hold (`HLL_REGISTER_MAX` = 63).
const HLL_REGISTER_MAX: u8 = ((1u32 << HLL_BITS) - 1) as u8;
/// Size of the fixed 16-byte HLL header (`HLL_HDR_SIZE`).
const HLL_HDR_SIZE: usize = 16;
/// Total bytes of a dense HLL: header + ceil(16384*6/8) = 16 + 12288 = 12304
/// (`HLL_DENSE_SIZE`).
const HLL_DENSE_SIZE: usize = HLL_HDR_SIZE + (HLL_REGISTERS * HLL_BITS as usize).div_ceil(8);
/// Dense encoding discriminant stored in header byte [4] (`HLL_DENSE`).
const HLL_DENSE: u8 = 0;
/// 0.5/ln(2) bias constant (`HLL_ALPHA_INF`).
const HLL_ALPHA_INF: f64 = 0.721_347_520_444_481_7;
/// Byte offset of the cached-cardinality field within the header.
const HLL_HDR_CARD_OFF: usize = 8;
/// The HLL-specific WRONGTYPE payload (`hyperloglog.c`, `isHLLObjectOrReply`).
const HLL_WRONG_TYPE_ERR: &[u8] = b"WRONGTYPE Key is not a valid HyperLogLog string value.";

/// Read dense register `regnum` (6-bit packed) (`HLL_DENSE_GET_REGISTER`).
#[inline]
fn hll_dense_get_register(registers: &[u8], regnum: usize) -> u8 {
    let byte_idx = regnum * HLL_BITS as usize / 8;
    let fb = (regnum * HLL_BITS as usize) & 7;
    let fb8 = 8 - fb;
    let b0 = registers[byte_idx] as u32;
    let b1 = registers.get(byte_idx + 1).copied().unwrap_or(0) as u32;
    ((b0 >> fb | b1 << fb8) & HLL_REGISTER_MAX as u32) as u8
}

/// Write dense register `regnum` to `val` (`HLL_DENSE_SET_REGISTER`).
#[inline]
fn hll_dense_set_register(registers: &mut [u8], regnum: usize, val: u8) {
    let byte_idx = regnum * HLL_BITS as usize / 8;
    let fb = (regnum * HLL_BITS as usize) & 7;
    let fb8 = 8 - fb;
    let v = val as u32;
    registers[byte_idx] &= !((HLL_REGISTER_MAX as u32) << fb) as u8;
    registers[byte_idx] |= (v << fb) as u8;
    if let Some(next) = registers.get_mut(byte_idx + 1) {
        *next &= !((HLL_REGISTER_MAX as u32) >> fb8) as u8;
        *next |= (v >> fb8) as u8;
    }
}

/// `MurmurHash64A` — the endian-neutral 64-bit Murmur2 variant valkey hashes
/// HLL elements with (`hyperloglog.c`).
fn hll_murmur64a(key: &[u8], seed: u32) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let len = key.len();
    let mut h: u64 = (seed as u64) ^ (len as u64).wrapping_mul(M);
    let chunks = len / 8;
    for i in 0..chunks {
        let base = i * 8;
        let mut k = key[base] as u64;
        k |= (key[base + 1] as u64) << 8;
        k |= (key[base + 2] as u64) << 16;
        k |= (key[base + 3] as u64) << 24;
        k |= (key[base + 4] as u64) << 32;
        k |= (key[base + 5] as u64) << 40;
        k |= (key[base + 6] as u64) << 48;
        k |= (key[base + 7] as u64) << 56;
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    let tail = &key[chunks * 8..];
    let mut i = tail.len();
    while i > 0 {
        i -= 1;
        h ^= (tail[i] as u64) << (8 * i);
    }
    if !tail.is_empty() {
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// Hash `ele` to (register-index, pattern-length) (`hllPatLen`). The pattern
/// length is the position of the first set bit in the upper `HLL_Q` bits of the
/// hash, 1-indexed and capped via the sentinel `1 << HLL_Q`.
fn hll_pat_len(ele: &[u8]) -> (usize, u8) {
    let hash = hll_murmur64a(ele, 0xadc83b19);
    let index = (hash & HLL_P_MASK) as usize;
    let mut bits = hash >> HLL_P;
    bits |= 1u64 << HLL_Q;
    (index, (bits.trailing_zeros() + 1) as u8)
}

/// Add `ele` to the dense register array, keeping the max pattern length per
/// index (`hllDenseSet` via `hllAdd`). Returns true if a register grew.
fn hll_dense_add(registers: &mut [u8], ele: &[u8]) -> bool {
    let (index, count) = hll_pat_len(ele);
    if count > hll_dense_get_register(registers, index) {
        hll_dense_set_register(registers, index, count);
        true
    } else {
        false
    }
}

/// Merge a dense register array `src` into a 1-byte-per-register `max` array,
/// taking the register-wise maximum (the dense branch of `hllMerge`).
fn hll_merge_dense(max: &mut [u8], src: &[u8]) {
    for (i, slot) in max.iter_mut().enumerate() {
        let v = hll_dense_get_register(src, i);
        if v > *slot {
            *slot = v;
        }
    }
}

/// Ertl sigma correction (`hllSigma`, arXiv:1702.01284).
fn hll_sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0f64;
    let mut z = x;
    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            break;
        }
    }
    z
}

/// Ertl tau correction (`hllTau`, arXiv:1702.01284).
fn hll_tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0f64;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z_prime == z {
            break;
        }
    }
    z / 3.0
}

/// Estimate cardinality from a 1-byte-per-register array (`hllCount` over the
/// `HLL_RAW` form: the histogram is the same as the dense form, so a single
/// estimator serves both PFCOUNT-single and PFCOUNT-union/PFMERGE paths). The
/// arithmetic matches `hllCount` exactly: `llround(HLL_ALPHA_INF * m*m / z)`.
fn hll_count_from_histo(reghisto: &[i32; 64]) -> u64 {
    let m = HLL_REGISTERS as f64;
    let mut z = m * hll_tau((m - reghisto[HLL_Q as usize + 1] as f64) / m);
    let mut j = HLL_Q as usize;
    while j >= 1 {
        z += reghisto[j] as f64;
        z *= 0.5;
        j -= 1;
    }
    z += m * hll_sigma(reghisto[0] as f64 / m);
    (HLL_ALPHA_INF * m * m / z).round() as u64
}

/// Estimate cardinality from a 1-byte-per-register `max` array — the form
/// produced by merging HLLs in PFCOUNT-union / PFMERGE (`hllRawRegHisto` +
/// `hllCount`). Each byte directly holds one register value (0..=63).
fn hll_count_raw(registers: &[u8]) -> u64 {
    let mut reghisto = [0i32; 64];
    for &r in registers.iter() {
        reghisto[r as usize] += 1;
    }
    hll_count_from_histo(&reghisto)
}

/// Estimate cardinality from a dense key's 6-bit-packed register data
/// (`hllDenseRegHisto` + `hllCount`). `registers` is the slice *after* the
/// header.
fn hll_count_dense(registers: &[u8]) -> u64 {
    let mut reghisto = [0i32; 64];
    for j in 0..HLL_REGISTERS {
        reghisto[hll_dense_get_register(registers, j) as usize] += 1;
    }
    hll_count_from_histo(&reghisto)
}

/// Build a fresh dense, all-zero HLL byte buffer with a valid `HYLL` header
/// (the `createHLLObject` result, already promoted to dense). All registers are
/// zero and the cached-cardinality field is left zeroed with a valid cache bit.
fn hll_create_dense() -> Vec<u8> {
    let mut buf = vec![0u8; HLL_DENSE_SIZE];
    buf[0..4].copy_from_slice(b"HYLL");
    buf[4] = HLL_DENSE;
    buf
}

/// Set the cardinality-cache-invalid bit (MSB of the last card byte)
/// (`HLL_INVALIDATE_CACHE`). The dense-only PFCOUNT recomputes every time, so
/// this is kept only to preserve a faithful, valid header.
fn hll_invalidate_cache(buf: &mut [u8]) {
    buf[HLL_HDR_CARD_OFF + 7] |= 1 << 7;
}

/// Validate `buf` as a well-formed HLL string: `HYLL` magic, dense encoding,
/// exact dense length. The edge engine only ever writes dense HLLs, so a valid
/// HLL here is always dense; a plain string (or any non-`HYLL` value) fails,
/// yielding the HLL-specific WRONGTYPE (`isHLLObjectOrReply`).
fn hll_is_valid(buf: &[u8]) -> bool {
    buf.len() == HLL_DENSE_SIZE && &buf[0..4] == b"HYLL" && buf[4] == HLL_DENSE
}

// ──────────────────────────────────────────────────────────────────────────
// RDB DUMP/RESTORE framing — faithful port of cluster.c (createDumpPayload /
// verifyDumpPayload), rdb.c (rdbSaveRawString / string load), crc64.c, and
// lzf_c.c / lzf_d.c. Confined to STRING values + the DUMP framing this wave.
// Confirmed against valkey-server 9.1.0: RDB_VERSION = 80 (footer bytes
// `0x50 0x00`, little-endian), CRC64 = Jones variant over the payload+version
// bytes (memrev64ifbe is a no-op on LE hosts, so CRC is stored LE).
// ──────────────────────────────────────────────────────────────────────────

/// `RDB_VERSION` from `rdb.h` (valkey 9.1.0). Written into the DUMP footer and
/// the maximum accepted RESTORE version under the default (strict) policy.
const RDB_DUMP_VERSION: u16 = 80;

/// Top-2-bit length-encoding selectors (`rdbSaveLen` / `rdbLoadLen`).
const RDB_6BITLEN: u8 = 0;
const RDB_14BITLEN: u8 = 1;
const RDB_32BITLEN: u8 = 0x80;
const RDB_64BITLEN: u8 = 0x81;
const RDB_ENCVAL: u8 = 3;
const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const RDB_ENC_LZF: u8 = 3;

/// RDB object-type bytes (`rdb.h` `enum RdbType`). The engine emits and
/// accepts the compact small-collection encodings only; the larger
/// hashtable/skiplist encodings stay deferred. A plain (no-field-TTL) hash
/// preserves insertion order (`IndexMap`), so it DUMPs to the compact
/// `RDB_TYPE_HASH_LISTPACK`; a hash carrying any per-field TTL is `RDB_TYPE_HASH_2`
/// (hashtable+expiry, type byte 22) in the reference and stays deferred. A SET
/// DUMPs to `RDB_TYPE_SET_INTSET` (all-integer, within intset limits) or to
/// `RDB_TYPE_SET_LISTPACK` (a small non-integer set, members in the listpack
/// order valkey would store — reproduced by replaying its encoding state
/// machine); a set valkey would keep as a hashtable stays deferred.
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_SET_INTSET: u8 = 11;
const RDB_TYPE_HASH_LISTPACK: u8 = 16;
const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
const RDB_TYPE_SET_LISTPACK: u8 = 20;
/// `RDB_TYPE_HASH_2` (type byte 22, `rdb.h`): a hashtable-encoded hash carrying
/// per-field expiration, added in RDB 80 (valkey 9.0). The body is a field
/// count followed by `<field><value><8-byte-LE-i64-expiry-ms>` triples, where
/// the expiry is the C `EXPIRY_NONE` sentinel (`-1`, all-`0xff`) for a field
/// with no TTL. Any hash with at least one field TTL is hashtable-encoded in
/// valkey 9.1.0 (there is no listpack-with-expiry encoding in this version),
/// so this is the only DUMP type the edge engine emits for a TTL-carrying hash.
const RDB_TYPE_HASH_2: u8 = 22;

/// The C `EXPIRY_NONE` sentinel (`expire.h`): the per-field expiry value meaning
/// "no TTL". Serialized in `RDB_TYPE_HASH_2` as the 8-byte little-endian `-1`.
const HASH_EXPIRY_NONE: i64 = -1;

/// The largest hash field count for which valkey's hashtable iterates in
/// **insertion order** for a clean (append-only, no delete-then-readd) hash, so
/// the edge engine's `IndexMap` order reproduces valkey's `RDB_TYPE_HASH_2`
/// field order byte-for-byte. Confirmed against valkey-server 9.1.0: at 1..=6
/// fields the small hashtable preserves insertion order across server restarts;
/// at 7 fields the table resizes and iteration becomes hash-seed bucket order
/// the engine cannot reproduce. A TTL-carrying hash with more than this many
/// fields is therefore deferred (the caller emits the aggregate-deferral error).
const HASH_RDB2_INSERTION_ORDER_MAX: usize = 6;

/// `QUICKLIST_NODE_CONTAINER_PACKED` (`quicklist.h`): a quicklist node whose
/// payload is a listpack (vs `_PLAIN` = 1, a single oversized raw element).
const QUICKLIST_NODE_CONTAINER_PACKED: u64 = 2;

/// Default object-encoding thresholds (`config.c`): the engine only emits the
/// compact encodings when the collection stays within valkey's defaults, so a
/// DUMP byte-matches what the reference (running with the same defaults)
/// produces. A collection past these limits is deferred — valkey would switch
/// to hashtable/skiplist/quicklist-with-multiple-nodes, which the engine
/// cannot reproduce byte-for-byte. Confirmed against valkey-server 9.1.0.
const ZSET_MAX_LISTPACK_ENTRIES: usize = 128;
const ZSET_MAX_LISTPACK_VALUE: usize = 64;
const SET_MAX_INTSET_ENTRIES: usize = 512;

/// `set-max-listpack-entries` / `set-max-listpack-value` defaults (`config.c`,
/// confirmed against valkey-server 9.1.0: 128 entries, 64-byte member). A set
/// with any non-integer member stays `OBJ_ENCODING_LISTPACK` only while it has
/// `< set-max-listpack-entries` members and every member is `<= set-max-listpack-value`
/// bytes; past either it is a hashtable (`RDB_TYPE_SET`), deferred.
const SET_MAX_LISTPACK_ENTRIES: usize = 128;
const SET_MAX_LISTPACK_VALUE: usize = 64;

/// `hash-max-listpack-entries` / `hash-max-listpack-value` defaults (`config.c`,
/// confirmed against valkey-server 9.1.0: 512 entries, 64-byte field/value).
/// A hash within both limits and with no per-field TTL stays
/// `OBJ_ENCODING_LISTPACK` and DUMPs to `RDB_TYPE_HASH_LISTPACK`; past either
/// limit it is a hashtable (`RDB_TYPE_HASH`), which the engine cannot reproduce
/// byte-for-byte, so it is deferred.
const HASH_MAX_LISTPACK_ENTRIES: usize = 512;
const HASH_MAX_LISTPACK_VALUE: usize = 64;

/// `list-max-listpack-size` default of `-2` → an 8 KiB per-node byte budget
/// (`optimization_level[1]`, `quicklist.c`). A small list whose single
/// listpack fits this budget is stored as a one-node quicklist; a larger list
/// would spill into multiple nodes (and `OBJ_ENCODING_QUICKLIST`), which the
/// engine's flat `VecDeque` cannot reproduce, so it is deferred.
const LIST_MAX_LISTPACK_BYTES: usize = 8192;

const CRC64_REFLECTED_POLY: u64 = 0x95ac9329ac4bc9b5;

const fn crc64_make_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u64;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ CRC64_REFLECTED_POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC64_TABLE: [u64; 256] = crc64_make_table();

/// CRC-64 (Jones variant), matching valkey's `crc64(crc, data, len)`. Pass
/// `crc = 0` for a one-shot checksum.
fn crc64(crc: u64, data: &[u8]) -> u64 {
    let mut state = crc;
    for &byte in data {
        let index = ((state ^ (byte as u64)) & 0xff) as usize;
        state = (state >> 8) ^ CRC64_TABLE[index];
    }
    state
}

/// `string2ll` (`util.c`) — parse `s` as a canonical decimal integer fitting in
/// an `i64`. Canonical means: no leading zeros (except a lone "0"), an optional
/// single leading `-` (but never "-0"), and no other characters. Returns `None`
/// when `s` is not such a value — exactly the cases where the reference falls
/// through to a raw string in `rdbSaveRawString`.
fn string2ll(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.len() == 1 && s[0].is_ascii_digit() {
        return Some((s[0] - b'0') as i64);
    }
    let mut idx = 0usize;
    let negative = s[0] == b'-';
    if negative {
        idx = 1;
        if idx == s.len() {
            return None;
        }
    }
    if !(s[idx] >= b'1' && s[idx] <= b'9') {
        return None;
    }
    let mut v: u64 = (s[idx] - b'0') as u64;
    idx += 1;
    while idx < s.len() {
        let c = s[idx];
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?;
        v = v.checked_add((c - b'0') as u64)?;
        idx += 1;
    }
    if negative {
        let limit = (i64::MAX as u64) + 1;
        if v > limit {
            return None;
        }
        Some((v as i128 * -1) as i64)
    } else {
        if v > i64::MAX as u64 {
            return None;
        }
        Some(v as i64)
    }
}

/// `rdbEncodeInteger` (`rdb.c`): if `value` fits in i8/i16/i32, emit the
/// `RDB_ENCVAL`-prefixed INT8/16/32 encoding (LE payload). Returns `None` for
/// values outside i32 (the reference then stores them as a raw decimal string).
fn rdb_encode_integer(value: i64) -> Option<Vec<u8>> {
    if (-(1 << 7)..=(1 << 7) - 1).contains(&value) {
        Some(vec![(RDB_ENCVAL << 6) | RDB_ENC_INT8, value as u8])
    } else if (-(1 << 15)..=(1 << 15) - 1).contains(&value) {
        let mut out = vec![(RDB_ENCVAL << 6) | RDB_ENC_INT16];
        out.extend_from_slice(&(value as i16).to_le_bytes());
        Some(out)
    } else if (-(1_i64 << 31)..=(1_i64 << 31) - 1).contains(&value) {
        let mut out = vec![(RDB_ENCVAL << 6) | RDB_ENC_INT32];
        out.extend_from_slice(&(value as i32).to_le_bytes());
        Some(out)
    } else {
        None
    }
}

/// `rdbSaveLen` (`rdb.c`): RDB variable-length integer. 6-bit, 14-bit (BE),
/// 32-bit (BE) or 64-bit (BE) per the top two bits of the first byte.
fn rdb_save_len(out: &mut Vec<u8>, len: u64) {
    if len <= 63 {
        out.push((RDB_6BITLEN << 6) | (len as u8));
    } else if len <= 16383 {
        out.push((RDB_14BITLEN << 6) | ((len >> 8) as u8 & 0x3f));
        out.push((len & 0xff) as u8);
    } else if len <= u32::MAX as u64 {
        out.push(RDB_32BITLEN);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.push(RDB_64BITLEN);
        out.extend_from_slice(&len.to_be_bytes());
    }
}

/// Read one `rdbLoadLen` value from `data` at `*pos`. Returns `(value,
/// is_encoded)`; when `is_encoded` the value holds the low-6-bit `RDB_ENC_*`
/// discriminant. Advances `*pos`. `None` on truncation.
fn rdb_load_len(data: &[u8], pos: &mut usize) -> Option<(u64, bool)> {
    let first = *data.get(*pos)?;
    *pos += 1;
    match (first & 0xc0) >> 6 {
        0 => Some(((first & 0x3f) as u64, false)),
        1 => {
            let second = *data.get(*pos)?;
            *pos += 1;
            Some(((((first & 0x3f) as u64) << 8) | second as u64, false))
        }
        2 => {
            let sub = first & 0x3f;
            if sub == 0 {
                let bytes = data.get(*pos..*pos + 4)?;
                *pos += 4;
                Some((u32::from_be_bytes(bytes.try_into().ok()?) as u64, false))
            } else if sub == 1 {
                let bytes = data.get(*pos..*pos + 8)?;
                *pos += 8;
                Some((u64::from_be_bytes(bytes.try_into().ok()?), false))
            } else {
                None
            }
        }
        3 => Some(((first & 0x3f) as u64, true)),
        _ => unreachable!(),
    }
}

/// `rdbSaveRawString` (`rdb.c`) for a STRING value. Tries integer encoding
/// (only when `len <= 11` and the bytes are a canonical i32-fitting integer),
/// then LZF compression (only when `len > 20` and the result actually shrinks),
/// otherwise stores `<len><bytes>` verbatim. `rdb_compression` is on by default
/// in the reference, so this always attempts LZF for long strings.
fn rdb_save_raw_string(out: &mut Vec<u8>, s: &[u8]) {
    if s.len() <= 11 {
        if let Some(value) = string2ll(s) {
            if let Some(enc) = rdb_encode_integer(value) {
                out.extend_from_slice(&enc);
                return;
            }
        }
    }
    if s.len() > 20 {
        if let Some(compressed) = lzf_compress(s) {
            // rdbSaveLzfStringObject only writes the blob when compression
            // saved space (lzf_compress returns None here when it didn't).
            out.push((RDB_ENCVAL << 6) | RDB_ENC_LZF);
            rdb_save_len(out, compressed.len() as u64);
            rdb_save_len(out, s.len() as u64);
            out.extend_from_slice(&compressed);
            return;
        }
    }
    rdb_save_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// `createDumpPayload` (`cluster.c`) for any supported value type. Selects the
/// RDB object-type byte the reference would (given valkey's default encoding
/// thresholds), serializes the body byte-identically, then appends the version
/// footer and CRC64. Returns `None` when the value cannot be reproduced
/// byte-for-byte — either an unsupported type (Stream), a TTL-carrying hash
/// past `HASH_RDB2_INSERTION_ORDER_MAX` fields (valkey's hashtable then iterates
/// in hash-seed bucket order), a SET valkey would
/// keep as a hashtable, or a collection past the compact-encoding thresholds
/// (valkey would have
/// converted it to hashtable/skiplist/multi-node-quicklist, whose iteration
/// order the engine's flat containers cannot match). The caller turns `None`
/// into the Wave-21 aggregate-deferral error.
fn rdb_create_dump_payload(value: &StoredValue) -> Option<Vec<u8>> {
    let mut payload = Vec::new();
    match value {
        StoredValue::String(bytes) => {
            payload.push(RDB_TYPE_STRING);
            rdb_save_raw_string(&mut payload, bytes);
        }
        StoredValue::List(items) => {
            let lp = rdb_list_to_listpack(items)?;
            payload.push(RDB_TYPE_LIST_QUICKLIST_2);
            // A list listpack is saved as a single-node quicklist: node count,
            // the node's container type, then the listpack as a raw string.
            rdb_save_len(&mut payload, 1);
            rdb_save_len(&mut payload, QUICKLIST_NODE_CONTAINER_PACKED);
            rdb_save_raw_string(&mut payload, &lp);
        }
        StoredValue::ZSet(members) => {
            let lp = rdb_zset_to_listpack(members)?;
            payload.push(RDB_TYPE_ZSET_LISTPACK);
            rdb_save_raw_string(&mut payload, &lp);
        }
        StoredValue::Set(members) => {
            let (type_byte, blob) = rdb_set_to_dump(members)?;
            payload.push(type_byte);
            rdb_save_raw_string(&mut payload, &blob);
        }
        StoredValue::Hash(fields) => {
            if fields.values().any(|f| f.expire_at_ms.is_some()) {
                let body = rdb_hash_to_hash2(fields)?;
                payload.push(RDB_TYPE_HASH_2);
                payload.extend_from_slice(&body);
            } else {
                let lp = rdb_hash_to_listpack(fields)?;
                payload.push(RDB_TYPE_HASH_LISTPACK);
                rdb_save_raw_string(&mut payload, &lp);
            }
        }
        StoredValue::Stream(_) => return None,
    }
    payload.push((RDB_DUMP_VERSION & 0xff) as u8);
    payload.push(((RDB_DUMP_VERSION >> 8) & 0xff) as u8);
    let crc = crc64(0, &payload);
    payload.extend_from_slice(&crc.to_le_bytes());
    Some(payload)
}

/// Build the listpack body for a LIST, in element order (the engine's
/// `VecDeque` already holds valkey's order). Returns `None` when the list
/// would not be a single-node listpack quicklist under valkey's default
/// `list-max-listpack-size = -2` (8 KiB per node) — i.e. the listpack body
/// would exceed the byte budget — so such lists are deferred.
fn rdb_list_to_listpack(items: &VecDeque<Vec<u8>>) -> Option<Vec<u8>> {
    let mut lp = ListpackWriter::new();
    for item in items {
        lp.append_auto(item);
    }
    if lp.byte_len() > LIST_MAX_LISTPACK_BYTES {
        return None;
    }
    Some(lp.into_bytes())
}

/// Build the listpack body for a ZSET. Members are emitted in valkey's stored
/// order — (score ascending, member lexicographic ascending) — with each
/// member followed by its score as a separate listpack entry (`zzlInsertAt`,
/// `t_zset.c`): an integer entry when the score round-trips through
/// `double2ll`, else the `d2string` text. Returns `None` when the zset is past
/// valkey's `zset-max-listpack-{entries,value}` defaults (it would be a
/// skiplist, whose DUMP the engine cannot reproduce).
fn rdb_zset_to_listpack(members: &HashMap<Vec<u8>, f64>) -> Option<Vec<u8>> {
    if members.len() > ZSET_MAX_LISTPACK_ENTRIES {
        return None;
    }
    let ordered = sorted_zset_entries(members);
    let mut lp = ListpackWriter::new();
    for (member, score) in &ordered {
        if member.len() > ZSET_MAX_LISTPACK_VALUE {
            return None;
        }
        lp.append_auto(member);
        match double2ll(*score) {
            Some(lscore) => lp.append_integer(lscore),
            None => lp.append_auto(&format_score(*score)),
        }
    }
    Some(lp.into_bytes())
}

/// Build the listpack body for a HASH, in field insertion order (the engine's
/// `IndexMap` already holds valkey's stored order). Each field is followed by
/// its value as the next listpack entry (`hashTypeSet`/`lpAppend`), with the
/// same canonical-integer auto-detection valkey applies, so the bytes match a
/// reference `RDB_TYPE_HASH_LISTPACK` DUMP. Returns `None` (deferred) when:
/// - any field carries a per-field TTL — valkey then keeps the hash as a
///   hashtable and DUMPs `RDB_TYPE_HASH_2` (type byte 22, a different format the
///   engine cannot reproduce byte-for-byte this wave); or
/// - the hash is past `hash-max-listpack-{entries,value}` (it would be a
///   hashtable in valkey, `RDB_TYPE_HASH`).
fn rdb_hash_to_listpack(fields: &IndexMap<Vec<u8>, HashField>) -> Option<Vec<u8>> {
    if fields.len() > HASH_MAX_LISTPACK_ENTRIES {
        return None;
    }
    let mut lp = ListpackWriter::new();
    for (field, stored) in fields {
        if stored.expire_at_ms.is_some() {
            return None;
        }
        if field.len() > HASH_MAX_LISTPACK_VALUE || stored.value.len() > HASH_MAX_LISTPACK_VALUE {
            return None;
        }
        lp.append_auto(field);
        lp.append_auto(&stored.value);
    }
    Some(lp.into_bytes())
}

/// Build the `RDB_TYPE_HASH_2` body for a hash carrying at least one per-field
/// TTL — the `rdbSaveObject` `OBJ_ENCODING_HASHTABLE` path (`rdb.c`): a field
/// count (`rdbSaveLen`), then for each field a `field` raw string, a `value`
/// raw string, and the field's expiry as an 8-byte little-endian `i64`
/// millisecond time (`rdbSaveMillisecondTime`) — `EXPIRY_NONE` (`-1`,
/// all-`0xff`) for a field with no TTL. Fields are emitted in the engine's
/// `IndexMap` insertion order, which matches valkey's small-hashtable iteration
/// order. Returns `None` (deferred) when:
/// - the hash has more than `HASH_RDB2_INSERTION_ORDER_MAX` fields (valkey's
///   hashtable resizes and iterates in hash-seed bucket order the engine cannot
///   reproduce); or
/// - any field or value exceeds the listpack value cap — not a real constraint
///   for a hashtable-encoded hash, but kept symmetric with the listpack path so
///   only genuinely small, byte-reproducible hashes are emitted.
fn rdb_hash_to_hash2(fields: &IndexMap<Vec<u8>, HashField>) -> Option<Vec<u8>> {
    if fields.len() > HASH_RDB2_INSERTION_ORDER_MAX {
        return None;
    }
    let mut body = Vec::new();
    rdb_save_len(&mut body, fields.len() as u64);
    for (field, stored) in fields {
        rdb_save_raw_string(&mut body, field);
        rdb_save_raw_string(&mut body, &stored.value);
        let expiry: i64 = match stored.expire_at_ms {
            Some(ms) => ms as i64,
            None => HASH_EXPIRY_NONE,
        };
        body.extend_from_slice(&expiry.to_le_bytes());
    }
    Some(body)
}

/// The encoding a SET ends up in, with the contents valkey would store. The
/// engine reconstructs this by replaying `setTypeAddAux` over the set's
/// insertion order (the `IndexSet` order), so the resulting `Intset`/`Listpack`
/// holds members in exactly valkey's stored order — sorted ascending for an
/// intset, append-order for a listpack (which, for a set that was first an
/// intset and then converted, means the original integers in sorted order
/// followed by the later non-integer members).
enum SetEncoding {
    Intset(Vec<i64>),
    Listpack(Vec<Vec<u8>>),
    Hashtable,
}

/// Replay valkey's `setTypeAddAux` / `setTypeCreate` encoding state machine
/// over `members` in insertion order, returning the encoding and stored
/// contents the reference would hold. `set-max-intset-entries` (512),
/// `set-max-listpack-entries` (128) and `set-max-listpack-value` (64) gate the
/// transitions, confirmed against valkey-server 9.1.0:
/// - The first member chooses the initial encoding (`setTypeCreate`): intset
///   when it parses as an integer, else listpack.
/// - Adding an integer to an intset keeps it sorted; crossing 512 entries
///   converts to hashtable (`maybeConvertIntset`).
/// - Adding a non-integer to an intset converts to **listpack** (the intset
///   members iterated in sorted order, then the new member appended) when the
///   intset length is `< set-max-listpack-entries` and both the new member and
///   the widest existing integer are `<= set-max-listpack-value`; otherwise to
///   hashtable.
/// - Adding to a listpack appends (preserving order) while `lpLength <
///   set-max-listpack-entries` and the member is `<= set-max-listpack-value`;
///   otherwise it converts to hashtable.
///
/// The `lpSafeToAdd` 1 GiB-listpack guards in the C are omitted: they cannot
/// trip within these entry/value caps.
fn simulate_set_encoding(members: &IndexSet<Vec<u8>>) -> SetEncoding {
    let mut intset: Option<Vec<i64>> = None;
    let mut listpack: Option<Vec<Vec<u8>>> = None;
    let mut hashtable = false;

    for member in members {
        if hashtable {
            break;
        }
        let as_int = string2ll(member);
        match (&mut intset, &mut listpack) {
            (None, None) => {
                // First member: `setTypeCreate` chooses the initial encoding.
                match as_int {
                    Some(v) => intset = Some(vec![v]),
                    None => listpack = Some(vec![member.clone()]),
                }
            }
            (Some(ints), None) => match as_int {
                Some(v) => {
                    // `intsetAdd` keeps the set sorted and unique; the engine's
                    // `IndexSet` already deduped, so just insert in order.
                    if let Err(pos) = ints.binary_search(&v) {
                        ints.insert(pos, v);
                    }
                    if ints.len() > SET_MAX_INTSET_ENTRIES {
                        hashtable = true;
                    }
                }
                None => {
                    // Non-integer added to an intset: convert to listpack when
                    // within the listpack thresholds, else hashtable.
                    let widest_int = ints
                        .iter()
                        .map(|v| v.to_string().len())
                        .max()
                        .unwrap_or(0);
                    if ints.len() < SET_MAX_LISTPACK_ENTRIES
                        && member.len() <= SET_MAX_LISTPACK_VALUE
                        && widest_int <= SET_MAX_LISTPACK_VALUE
                    {
                        let mut lp: Vec<Vec<u8>> = ints
                            .iter()
                            .map(|v| v.to_string().into_bytes())
                            .collect();
                        lp.push(member.clone());
                        listpack = Some(lp);
                        intset = None;
                    } else {
                        hashtable = true;
                    }
                }
            },
            (None, Some(lp)) => {
                // Listpack: append while within entry/value limits, else convert.
                if lp.len() < SET_MAX_LISTPACK_ENTRIES && member.len() <= SET_MAX_LISTPACK_VALUE {
                    lp.push(member.clone());
                } else {
                    hashtable = true;
                }
            }
            (Some(_), Some(_)) => unreachable!("a set is in exactly one encoding"),
        }
    }

    if hashtable {
        SetEncoding::Hashtable
    } else if let Some(ints) = intset {
        SetEncoding::Intset(ints)
    } else if let Some(lp) = listpack {
        SetEncoding::Listpack(lp)
    } else {
        // An empty set never reaches DUMP (the key is deleted when emptied), but
        // valkey would create an intset for it.
        SetEncoding::Intset(Vec::new())
    }
}

/// Build the DUMP body for a SET, returning `(type_byte, body)`. Replays
/// valkey's encoding state machine (`simulate_set_encoding`) so the bytes match
/// a reference DUMP: an all-integer set within `set-max-intset-entries` is an
/// `RDB_TYPE_SET_INTSET` blob (sorted); a small non-integer set is an
/// `RDB_TYPE_SET_LISTPACK` (members in valkey's listpack order). Returns `None`
/// when the set would be a hashtable in valkey (`RDB_TYPE_SET`), which the
/// engine cannot reproduce byte-for-byte.
fn rdb_set_to_dump(members: &IndexSet<Vec<u8>>) -> Option<(u8, Vec<u8>)> {
    match simulate_set_encoding(members) {
        SetEncoding::Intset(mut ints) => Some((RDB_TYPE_SET_INTSET, intset_encode(&mut ints))),
        SetEncoding::Listpack(items) => {
            let mut lp = ListpackWriter::new();
            for item in &items {
                lp.append_auto(item);
            }
            Some((RDB_TYPE_SET_LISTPACK, lp.into_bytes()))
        }
        SetEncoding::Hashtable => None,
    }
}

/// The members of a SET in valkey's stored iteration order, used by SMEMBERS so
/// the reply matches the reference byte-for-byte. Replays the encoding state
/// machine: an all-integer intset is iterated **sorted ascending**; a listpack
/// in its stored append order (the `IndexSet` insertion order, except integers
/// that were once an intset come out sorted ahead of later non-integers — the
/// listpack vector already encodes that). A hashtable-encoded set's true
/// iteration order is dict-bucket order the engine cannot reproduce, so it
/// falls back to insertion order (still membership-correct).
fn set_member_order(members: &IndexSet<Vec<u8>>) -> Vec<Vec<u8>> {
    match simulate_set_encoding(members) {
        SetEncoding::Intset(ints) => ints.into_iter().map(|v| v.to_string().into_bytes()).collect(),
        SetEncoding::Listpack(items) => items,
        SetEncoding::Hashtable => members.iter().cloned().collect(),
    }
}

/// Encode a set of `i64` values as an intset blob
/// (`[encoding:u32-le][length:u32-le][sorted contents]`, `intset.c`). The
/// single encoding width is the widest needed for any member (2/4/8 bytes);
/// the contents are sorted ascending and written little-endian. `ints` is
/// sorted in place. Byte-identical to `intsetBlobLen` output.
fn intset_encode(ints: &mut [i64]) -> Vec<u8> {
    ints.sort_unstable();
    let width: usize = ints
        .iter()
        .map(|&v| {
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                8
            } else if v < i16::MIN as i64 || v > i16::MAX as i64 {
                4
            } else {
                2
            }
        })
        .max()
        .unwrap_or(2);
    let mut out = Vec::with_capacity(8 + ints.len() * width);
    out.extend_from_slice(&(width as u32).to_le_bytes());
    out.extend_from_slice(&(ints.len() as u32).to_le_bytes());
    for &v in ints.iter() {
        match width {
            2 => out.extend_from_slice(&(v as i16).to_le_bytes()),
            4 => out.extend_from_slice(&(v as i32).to_le_bytes()),
            _ => out.extend_from_slice(&v.to_le_bytes()),
        }
    }
    out
}

/// `double2ll` (`util.c`): when `d` is finite, inside the safe `±LLONG_MAX/2`
/// casting range, and has no fractional part, return the exact `i64` — the
/// condition under which `zzlInsertAt` stores a score as an integer listpack
/// entry. Otherwise `None` (the score is then stored via `d2string`).
fn double2ll(d: f64) -> Option<i64> {
    let bound = (i64::MAX / 2) as f64;
    if !d.is_finite() || d < -bound || d > bound {
        return None;
    }
    let ll = d as i64;
    if ll as f64 == d {
        Some(ll)
    } else {
        None
    }
}

/// A minimal append-only listpack builder, byte-compatible with `listpack.c`
/// (`lpAppend` / `lpAppendInteger`). Header is `[total-bytes:u32-le]
/// [num-elements:u16-le]`; each entry is `<encoding+data><backlen>` where
/// `backlen` is the reverse varint length of the encoding+data; the buffer
/// ends with the `0xff` terminator. This is the same wire format the engine
/// must emit for LIST/ZSET DUMP byte-parity — the backlen varint is the
/// finicky part (see `lp_encode_backlen`).
struct ListpackWriter {
    /// Entry bytes only (encoding+data+backlen, no header, no terminator).
    body: Vec<u8>,
    count: u16,
}

impl ListpackWriter {
    const HDR_SIZE: usize = 6;

    fn new() -> Self {
        ListpackWriter {
            body: Vec::new(),
            count: 0,
        }
    }

    /// Append a value, auto-detecting a canonical integer exactly as `lpAppend`
    /// does (the listpack stores `"22"` as a 7-bit-uint entry, not a string).
    fn append_auto(&mut self, value: &[u8]) {
        match string2ll(value) {
            Some(int) => self.append_integer(int),
            None => self.append_string(value),
        }
    }

    fn append_string(&mut self, value: &[u8]) {
        let mut entry = lp_encode_string(value);
        let backlen = lp_encode_backlen(entry.len());
        entry.extend_from_slice(&backlen);
        self.body.extend_from_slice(&entry);
        self.count = self.count.saturating_add(1);
    }

    fn append_integer(&mut self, value: i64) {
        let mut entry = lp_encode_integer(value);
        let backlen = lp_encode_backlen(entry.len());
        entry.extend_from_slice(&backlen);
        self.body.extend_from_slice(&entry);
        self.count = self.count.saturating_add(1);
    }

    /// Total byte length of the finished listpack (header + entries + `0xff`).
    fn byte_len(&self) -> usize {
        Self::HDR_SIZE + self.body.len() + 1
    }

    fn into_bytes(self) -> Vec<u8> {
        let total = self.byte_len();
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&(total as u32).to_le_bytes());
        // num-elements caps at LP_HDR_NUMELE_UNKNOWN (0xffff); the engine never
        // emits more than the threshold-bounded element counts, so the cap is
        // unreachable here, but mirror the field width regardless.
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.body);
        out.push(0xff);
        out
    }
}

/// `lpEncodeGetType` string path (`listpack.c`): the 6-bit / 12-bit / 32-bit
/// string encodings, prefix + raw bytes. (The integer auto-detection is done
/// by the caller via `append_auto`; this path is reached only for genuine
/// strings.)
fn lp_encode_string(value: &[u8]) -> Vec<u8> {
    let len = value.len();
    let mut out;
    if len < 64 {
        out = Vec::with_capacity(1 + len);
        out.push(0x80 | len as u8);
    } else if len < 4096 {
        out = Vec::with_capacity(2 + len);
        out.push(0xe0 | ((len >> 8) as u8 & 0x0f));
        out.push((len & 0xff) as u8);
    } else {
        out = Vec::with_capacity(5 + len);
        out.push(0xf0);
        out.extend_from_slice(&(len as u32).to_le_bytes());
    }
    out.extend_from_slice(value);
    out
}

/// `lpEncodeGetType` integer path (`listpack.c`): 7-bit uint, 13-bit, 16/24/32/
/// 64-bit signed encodings. Byte-identical to `lpEncodeIntegerGetType`.
fn lp_encode_integer(value: i64) -> Vec<u8> {
    if (0..=127).contains(&value) {
        vec![value as u8]
    } else if (-4096..=4095).contains(&value) {
        let unsigned = if value < 0 {
            ((1i64 << 13) + value) as u16
        } else {
            value as u16
        };
        vec![((unsigned >> 8) as u8) | 0xc0, (unsigned & 0xff) as u8]
    } else if (i16::MIN as i64..=i16::MAX as i64).contains(&value) {
        let unsigned = if value < 0 {
            ((1i64 << 16) + value) as u32
        } else {
            value as u32
        };
        vec![0xf1, (unsigned & 0xff) as u8, (unsigned >> 8) as u8]
    } else if (-8_388_608..=8_388_607).contains(&value) {
        let unsigned = if value < 0 {
            ((1i64 << 24) + value) as u32
        } else {
            value as u32
        };
        vec![
            0xf2,
            (unsigned & 0xff) as u8,
            ((unsigned >> 8) & 0xff) as u8,
            (unsigned >> 16) as u8,
        ]
    } else if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
        let unsigned = if value < 0 {
            ((1i64 << 32) + value) as u64
        } else {
            value as u64
        };
        vec![
            0xf3,
            (unsigned & 0xff) as u8,
            ((unsigned >> 8) & 0xff) as u8,
            ((unsigned >> 16) & 0xff) as u8,
            (unsigned >> 24) as u8,
        ]
    } else {
        let unsigned = value as u64;
        vec![
            0xf4,
            (unsigned & 0xff) as u8,
            ((unsigned >> 8) & 0xff) as u8,
            ((unsigned >> 16) & 0xff) as u8,
            ((unsigned >> 24) & 0xff) as u8,
            ((unsigned >> 32) & 0xff) as u8,
            ((unsigned >> 40) & 0xff) as u8,
            ((unsigned >> 48) & 0xff) as u8,
            (unsigned >> 56) as u8,
        ]
    }
}

/// `lpEncodeBacklen` (`listpack.c`): the reverse-varint length of the
/// `encoding+data` run, 1–5 bytes. Each byte carries 7 bits; the continuation
/// bit (`0x80`) is set on every byte except the first written (so a backward
/// reader stops at the byte without it). Byte-identical to the reference — this
/// is the boundary that makes listpack DUMP parity finicky.
fn lp_encode_backlen(len: usize) -> Vec<u8> {
    if len <= 127 {
        vec![len as u8]
    } else if len < 16384 {
        vec![(len >> 7) as u8, (len & 127) as u8 | 128]
    } else if len < 2_097_152 {
        vec![
            (len >> 14) as u8,
            ((len >> 7) & 127) as u8 | 128,
            (len & 127) as u8 | 128,
        ]
    } else if len < 268_435_456 {
        vec![
            (len >> 21) as u8,
            ((len >> 14) & 127) as u8 | 128,
            ((len >> 7) & 127) as u8 | 128,
            (len & 127) as u8 | 128,
        ]
    } else {
        vec![
            (len >> 28) as u8,
            ((len >> 21) & 127) as u8 | 128,
            ((len >> 14) & 127) as u8 | 128,
            ((len >> 7) & 127) as u8 | 128,
            (len & 127) as u8 | 128,
        ]
    }
}

/// `verifyDumpPayload` (`cluster.c`): check the 2-byte RDB version (must be
/// accepted by the strict policy: `1 <= ver <= RDB_VERSION` and not a foreign
/// version) and the trailing CRC64 over `data[..len-8]`. Returns the parsed
/// version on success, `None` (→ "DUMP payload version or checksum are wrong")
/// otherwise.
fn rdb_verify_dump_payload(data: &[u8]) -> Option<u16> {
    if data.len() < 10 {
        return None;
    }
    let footer = &data[data.len() - 10..];
    let rdbver = (footer[1] as u16) << 8 | footer[0] as u16;
    if !rdb_is_version_accepted(rdbver) {
        return None;
    }
    let expected = crc64(0, &data[..data.len() - 8]);
    let stored = u64::from_le_bytes(data[data.len() - 8..].try_into().ok()?);
    if expected != stored {
        return None;
    }
    Some(rdbver)
}

/// `rdbIsVersionAccepted` (`rdb.c`) under the default strict policy: reject
/// versions below 1, foreign versions (12..=79), and any version above the
/// engine's own `RDB_VERSION`.
fn rdb_is_version_accepted(rdbver: u16) -> bool {
    if rdbver < 1 {
        return false;
    }
    if rdbver > RDB_DUMP_VERSION {
        return false;
    }
    // RDB_FOREIGN_VERSION_MIN..=RDB_FOREIGN_VERSION_MAX (12..=79, rdb.h).
    if (12..=79).contains(&rdbver) {
        return false;
    }
    true
}

/// Decode the value carried by a verified DUMP payload. Handles the compact
/// encodings the engine emits (STRING, LIST_QUICKLIST_2, ZSET_LISTPACK,
/// SET_INTSET) plus their listpack/ziplist siblings that real valkey dumps may
/// carry. Any other type byte yields `None` (→ "Bad data format"). Mirrors
/// `rdbLoadObjectType` + the relevant arms of `rdbLoadObject`.
fn rdb_load_dump_value(data: &[u8]) -> Option<StoredValue> {
    if data.len() < 10 {
        return None;
    }
    let body = &data[..data.len() - 10];
    let mut pos = 0usize;
    let type_byte = *body.get(pos)?;
    pos += 1;
    match type_byte {
        RDB_TYPE_STRING => {
            let bytes = rdb_load_string(body, &mut pos)?;
            Some(StoredValue::String(bytes))
        }
        RDB_TYPE_LIST_QUICKLIST_2 => {
            // A quicklist_2: a node count, then each node's container type and
            // its payload (a listpack, raw-string-encoded). Concatenate every
            // node's entries in order.
            let (nodes, _) = rdb_load_len(body, &mut pos)?;
            let mut items: VecDeque<Vec<u8>> = VecDeque::new();
            for _ in 0..nodes {
                let (container, _) = rdb_load_len(body, &mut pos)?;
                let blob = rdb_load_string(body, &mut pos)?;
                if container == QUICKLIST_NODE_CONTAINER_PACKED {
                    for entry in listpack_iter(&blob)? {
                        items.push_back(entry);
                    }
                } else {
                    // PLAIN node: the blob is a single oversized element.
                    items.push_back(blob);
                }
            }
            Some(StoredValue::List(items))
        }
        RDB_TYPE_ZSET_LISTPACK => {
            let blob = rdb_load_string(body, &mut pos)?;
            let entries = listpack_iter(&blob)?;
            if entries.len() % 2 != 0 {
                return None;
            }
            let mut members: HashMap<Vec<u8>, f64> = HashMap::new();
            let mut it = entries.into_iter();
            while let (Some(member), Some(score_bytes)) = (it.next(), it.next()) {
                let score = parse_score(&score_bytes)?;
                members.insert(member, score);
            }
            Some(StoredValue::ZSet(members))
        }
        RDB_TYPE_SET_INTSET => {
            let blob = rdb_load_string(body, &mut pos)?;
            let ints = intset_decode(&blob)?;
            let mut members: IndexSet<Vec<u8>> = IndexSet::new();
            for v in ints {
                members.insert(v.to_string().into_bytes());
            }
            Some(StoredValue::Set(members))
        }
        RDB_TYPE_SET_LISTPACK => {
            // A listpack of set members in valkey's stored (insertion) order.
            let blob = rdb_load_string(body, &mut pos)?;
            let entries = listpack_iter(&blob)?;
            let mut members: IndexSet<Vec<u8>> = IndexSet::with_capacity(entries.len());
            for member in entries {
                members.insert(member);
            }
            Some(StoredValue::Set(members))
        }
        RDB_TYPE_HASH_LISTPACK => {
            // A listpack of alternating field, value entries in insertion order.
            let blob = rdb_load_string(body, &mut pos)?;
            let entries = listpack_iter(&blob)?;
            if entries.len() % 2 != 0 {
                return None;
            }
            let mut fields: IndexMap<Vec<u8>, HashField> = IndexMap::new();
            let mut it = entries.into_iter();
            while let (Some(field), Some(value)) = (it.next(), it.next()) {
                fields.insert(field, HashField::new(value));
            }
            Some(StoredValue::Hash(fields))
        }
        RDB_TYPE_HASH_2 => {
            // A hashtable-encoded hash with per-field TTLs: a field count, then
            // for each field a `field`/`value` raw string and an 8-byte LE i64
            // expiry-ms (`EXPIRY_NONE` = -1 means no TTL). Mirrors the
            // `RDB_TYPE_HASH_2` arm of `rdbLoadObject`.
            let (count, _) = rdb_load_len(body, &mut pos)?;
            let mut fields: IndexMap<Vec<u8>, HashField> = IndexMap::new();
            for _ in 0..count {
                let field = rdb_load_string(body, &mut pos)?;
                let value = rdb_load_string(body, &mut pos)?;
                let expiry_bytes = body.get(pos..pos + 8)?;
                pos += 8;
                let expiry = i64::from_le_bytes(expiry_bytes.try_into().ok()?);
                if expiry < HASH_EXPIRY_NONE {
                    return None;
                }
                let expire_at_ms = if expiry == HASH_EXPIRY_NONE {
                    None
                } else {
                    Some(expiry as u64)
                };
                fields.insert(
                    field,
                    HashField {
                        value,
                        expire_at_ms,
                    },
                );
            }
            Some(StoredValue::Hash(fields))
        }
        _ => None,
    }
}

/// Parse an intset blob (`[encoding:u32-le][length:u32-le][contents]`,
/// `intset.c`) into its `i64` members. `None` on a bad encoding width or a
/// truncated body.
fn intset_decode(blob: &[u8]) -> Option<Vec<i64>> {
    if blob.len() < 8 {
        return None;
    }
    let width = u32::from_le_bytes(blob[0..4].try_into().ok()?) as usize;
    let length = u32::from_le_bytes(blob[4..8].try_into().ok()?) as usize;
    if !matches!(width, 2 | 4 | 8) {
        return None;
    }
    if blob.len() < 8 + length * width {
        return None;
    }
    let mut out = Vec::with_capacity(length);
    let mut pos = 8usize;
    for _ in 0..length {
        let v = match width {
            2 => i16::from_le_bytes(blob[pos..pos + 2].try_into().ok()?) as i64,
            4 => i32::from_le_bytes(blob[pos..pos + 4].try_into().ok()?) as i64,
            _ => i64::from_le_bytes(blob[pos..pos + 8].try_into().ok()?),
        };
        out.push(v);
        pos += width;
    }
    Some(out)
}

/// Iterate a listpack body (`[total-bytes:u32-le][num-elements:u16-le]
/// <entries><0xff>`, `listpack.c`), returning each entry decoded to its bytes
/// (integer entries become their decimal string, matching how the engine
/// stores them). `None` on malformed input.
fn listpack_iter(blob: &[u8]) -> Option<Vec<Vec<u8>>> {
    if blob.len() < 7 {
        return None;
    }
    let total = u32::from_le_bytes(blob[0..4].try_into().ok()?) as usize;
    if total != blob.len() || blob[blob.len() - 1] != 0xff {
        return None;
    }
    let mut out = Vec::new();
    let mut pos = 6usize; // skip 6-byte header
    while pos < blob.len() {
        if blob[pos] == 0xff {
            return Some(out);
        }
        let (value, encoded_len) = lp_decode_entry(blob, pos)?;
        out.push(value);
        // Advance past encoding+data and the trailing backlen.
        let backlen_size = lp_backlen_size(encoded_len);
        pos += encoded_len + backlen_size;
    }
    None
}

/// Decode one listpack entry at `pos`, returning `(bytes, encoded_len)` where
/// `encoded_len` is the byte length of the encoding+data (excluding the
/// trailing backlen). Mirrors `lpGet` (`listpack.c`).
fn lp_decode_entry(data: &[u8], pos: usize) -> Option<(Vec<u8>, usize)> {
    let byte = *data.get(pos)?;
    if byte & 0x80 == 0 {
        // 7-bit uint
        Some(((byte & 0x7f).to_string().into_bytes(), 1))
    } else if byte & 0xc0 == 0x80 {
        // 6-bit string
        let len = (byte & 0x3f) as usize;
        let s = data.get(pos + 1..pos + 1 + len)?;
        Some((s.to_vec(), 1 + len))
    } else if byte & 0xe0 == 0xc0 {
        // 13-bit int
        let b1 = *data.get(pos + 1)? as u16;
        let uval = (((byte & 0x1f) as u16) << 8) | b1;
        let val = if uval >= (1 << 12) {
            uval as i64 - (1 << 13)
        } else {
            uval as i64
        };
        Some((val.to_string().into_bytes(), 2))
    } else if byte & 0xff == 0xf1 {
        // 16-bit int
        let raw = u16::from_le_bytes(data.get(pos + 1..pos + 3)?.try_into().ok()?);
        Some(((raw as i16 as i64).to_string().into_bytes(), 3))
    } else if byte & 0xff == 0xf2 {
        // 24-bit int
        let b = data.get(pos + 1..pos + 4)?;
        let mut raw = (b[0] as u32) | ((b[1] as u32) << 8) | ((b[2] as u32) << 16);
        if raw & (1 << 23) != 0 {
            raw |= 0xff00_0000;
        }
        Some(((raw as i32 as i64).to_string().into_bytes(), 4))
    } else if byte & 0xff == 0xf3 {
        // 32-bit int
        let raw = u32::from_le_bytes(data.get(pos + 1..pos + 5)?.try_into().ok()?);
        Some(((raw as i32 as i64).to_string().into_bytes(), 5))
    } else if byte & 0xff == 0xf4 {
        // 64-bit int
        let raw = i64::from_le_bytes(data.get(pos + 1..pos + 9)?.try_into().ok()?);
        Some((raw.to_string().into_bytes(), 9))
    } else if byte & 0xf0 == 0xe0 {
        // 12-bit string
        let b1 = *data.get(pos + 1)? as usize;
        let len = (((byte & 0x0f) as usize) << 8) | b1;
        let s = data.get(pos + 2..pos + 2 + len)?;
        Some((s.to_vec(), 2 + len))
    } else if byte == 0xf0 {
        // 32-bit string
        let len = u32::from_le_bytes(data.get(pos + 1..pos + 5)?.try_into().ok()?) as usize;
        let s = data.get(pos + 5..pos + 5 + len)?;
        Some((s.to_vec(), 5 + len))
    } else {
        None
    }
}

/// Byte length of the reverse-varint backlen encoding the entry length `len`
/// (`lpEncodeBacklen` inverse). Matches `lp_encode_backlen`'s output width.
fn lp_backlen_size(len: usize) -> usize {
    if len <= 127 {
        1
    } else if len < 16384 {
        2
    } else if len < 2_097_152 {
        3
    } else if len < 268_435_456 {
        4
    } else {
        5
    }
}

/// `rdbGenericLoadStringObject` (`rdb.c`) for a STRING: dispatch on the
/// length-encoding header into an INT8/16/32 decimal string, an LZF blob, or a
/// verbatim raw run. Advances `*pos`. `None` on malformed input.
fn rdb_load_string(data: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let (len, is_encoded) = rdb_load_len(data, pos)?;
    if is_encoded {
        match len as u8 {
            RDB_ENC_INT8 => {
                let b = *data.get(*pos)?;
                *pos += 1;
                Some((b as i8 as i64).to_string().into_bytes())
            }
            RDB_ENC_INT16 => {
                let bytes = data.get(*pos..*pos + 2)?;
                *pos += 2;
                Some((i16::from_le_bytes(bytes.try_into().ok()?) as i64).to_string().into_bytes())
            }
            RDB_ENC_INT32 => {
                let bytes = data.get(*pos..*pos + 4)?;
                *pos += 4;
                Some((i32::from_le_bytes(bytes.try_into().ok()?) as i64).to_string().into_bytes())
            }
            RDB_ENC_LZF => {
                let (clen, _) = rdb_load_len(data, pos)?;
                let (ulen, _) = rdb_load_len(data, pos)?;
                let compressed = data.get(*pos..*pos + clen as usize)?;
                *pos += clen as usize;
                lzf_decompress(compressed, ulen as usize)
            }
            _ => None,
        }
    } else {
        let bytes = data.get(*pos..*pos + len as usize)?;
        *pos += len as usize;
        Some(bytes.to_vec())
    }
}

/// `lzf_compress` (`lzf_c.c`) with valkey's compile config (`HLOG=16`,
/// `VERY_FAST=1`, `ULTRA_FAST=0`, `INIT_HTAB=0`, `STRICT_ALIGN`-safe). Returns
/// the compressed bytes when they fit in `in_len - 1` (the buffer bound the
/// reference passes via `outlen = len - 4`, but we mirror the in-place behavior
/// by allocating `len` and rejecting non-shrinking output), or `None` when the
/// data is incompressible — exactly when `rdbSaveLzfStringObject` returns 0 and
/// the caller stores the string verbatim. Byte-identical to the reference for
/// inputs short enough that the uninitialized C hash table cannot diverge
/// (validated against captured valkey-server output in the unit tests).
fn lzf_compress(in_data: &[u8]) -> Option<Vec<u8>> {
    const HLOG: usize = 16;
    const HSIZE: usize = 1 << HLOG;
    const MAX_LIT: usize = 1 << 5;
    const MAX_OFF: usize = 1 << 13;
    const MAX_REF: usize = (1 << 8) + (1 << 3);

    let in_len = in_data.len();
    // rdbSaveLzfStringObject requires len > 4 and outlen = len - 4.
    if in_len <= 4 {
        return None;
    }
    let out_len = in_len - 4;
    // The C code passes a buffer of exactly `out_len` bytes and bounds every
    // write against `out_end = out + out_len`. Replicating that boundary
    // EXACTLY is what makes the output byte-identical: it is precisely the
    // out-of-space test (`op + 4 >= out_end`) that makes the reference bail to
    // a verbatim store on inputs that compress only marginally (e.g. the
    // 21-byte "012...0"). We bound-check against `out_end` while over-allocating
    // the backing buffer so a benign write never panics in Rust.
    let out_end = out_len;

    // htab stores 1-based input indices (0 == empty), matching the C code's
    // `*hslot ? (*hslot + BIAS) : NULL` with BIAS = in_data (slot 0 = NULL).
    let mut htab = vec![0usize; HSIZE];
    let mut out = vec![0u8; out_len + 8];

    let frst = |p: usize| -> u32 { ((in_data[p] as u32) << 8) | in_data[p + 1] as u32 };
    let next = |v: u32, p: usize| -> u32 { (v << 8) | in_data[p + 2] as u32 };
    // VERY_FAST IDX.
    let idx = |h: u32| -> usize {
        (((h >> (3 * 8 - HLOG as u32)).wrapping_sub(h.wrapping_mul(5))) as usize) & (HSIZE - 1)
    };

    let mut ip: usize = 0;
    let mut op: usize = 1; // op++ start run
    let mut lit: i64 = 0;

    if in_len < 2 {
        return None;
    }
    let mut hval = frst(ip);

    while ip < in_len.saturating_sub(2) {
        hval = next(hval, ip);
        let hslot = idx(hval);
        let reference = htab[hslot]; // 0 == NULL, else (idx+1)
        htab[hslot] = ip + 1;

        let mut matched = false;
        if reference != 0 {
            let ref_idx = reference - 1;
            let off = ip.wrapping_sub(ref_idx).wrapping_sub(1);
            if off < MAX_OFF
                && ref_idx > 0
                && in_data[ref_idx + 2] == in_data[ip + 2]
                && in_data[ref_idx] == in_data[ip]
                && in_data[ref_idx + 1] == in_data[ip + 1]
            {
                matched = true;
                let mut len: usize = 2;
                let mut maxlen = in_len - ip - len;
                if maxlen > MAX_REF {
                    maxlen = MAX_REF;
                }

                // Conservative + exact out-of-space test (against out_end).
                if op + 3 + 1 >= out_end && op - (lit == 0) as usize + 3 + 1 >= out_end {
                    return None;
                }

                out[op - lit as usize - 1] = (lit - 1) as u8; // stop run
                if lit == 0 {
                    op -= 1; // undo run if zero
                }

                // Extend match.
                loop {
                    if maxlen > 16 {
                        let mut broke = false;
                        for _ in 0..16 {
                            len += 1;
                            if in_data[ref_idx + len] != in_data[ip + len] {
                                broke = true;
                                break;
                            }
                        }
                        if broke {
                            break;
                        }
                    }
                    len += 1;
                    while len < maxlen && in_data[ref_idx + len] == in_data[ip + len] {
                        len += 1;
                    }
                    break;
                }

                len -= 2; // #octets - 1
                ip += 1;

                if len < 7 {
                    out[op] = ((off >> 8) + (len << 5)) as u8;
                    op += 1;
                } else {
                    out[op] = ((off >> 8) + (7 << 5)) as u8;
                    op += 1;
                    out[op] = (len - 7) as u8;
                    op += 1;
                }
                out[op] = off as u8;
                op += 1;

                lit = 0;
                op += 1; // start run

                ip += len + 1;

                if ip >= in_len.saturating_sub(2) {
                    break;
                }

                // VERY_FAST && !ULTRA_FAST rehash path.
                ip -= 1;
                ip -= 1;
                hval = frst(ip);
                hval = next(hval, ip);
                htab[idx(hval)] = ip + 1;
                ip += 1;
                hval = next(hval, ip);
                htab[idx(hval)] = ip + 1;
                ip += 1;
            }
        }

        if !matched {
            // one literal byte
            if op >= out_end {
                return None;
            }
            out[op] = in_data[ip];
            op += 1;
            ip += 1;
            lit += 1;
            if lit as usize == MAX_LIT {
                out[op - lit as usize - 1] = (lit - 1) as u8; // stop run
                lit = 0;
                op += 1; // start run
            }
        }
    }

    if op + 3 > out_end {
        return None;
    }

    while ip < in_len {
        lit += 1;
        out[op] = in_data[ip];
        op += 1;
        ip += 1;
        if lit as usize == MAX_LIT {
            out[op - lit as usize - 1] = (lit - 1) as u8;
            lit = 0;
            op += 1;
        }
    }

    out[op - lit as usize - 1] = (lit - 1) as u8; // end run
    if lit == 0 {
        op -= 1; // undo run if zero
    }

    out.truncate(op);
    Some(out)
}

/// `lzf_decompress` (`lzf_d.c`): expand `input` into exactly `output_len`
/// bytes. `None` on malformed input or length mismatch.
fn lzf_decompress(input: &[u8], output_len: usize) -> Option<Vec<u8>> {
    let mut out = vec![0u8; output_len];
    let mut ip = 0usize;
    let mut op = 0usize;

    while ip < input.len() {
        let ctrl = input[ip] as usize;
        ip += 1;

        if ctrl < 32 {
            let run = ctrl + 1;
            if op + run > output_len || ip + run > input.len() {
                return None;
            }
            out[op..op + run].copy_from_slice(&input[ip..ip + run]);
            ip += run;
            op += run;
        } else {
            let mut len = ctrl >> 5;
            let mut back = ((ctrl & 0x1f) << 8) + 1;
            if ip >= input.len() {
                return None;
            }
            if len == 7 {
                len += input[ip] as usize;
                ip += 1;
                if ip >= input.len() {
                    return None;
                }
            }
            back += input[ip] as usize;
            ip += 1;
            len += 2;
            if op + len > output_len || back > op {
                return None;
            }
            let mut src = op - back;
            for _ in 0..len {
                out[op] = out[src];
                op += 1;
                src += 1;
            }
        }
    }

    if op != output_len {
        return None;
    }
    Some(out)
}

fn bulk(value: impl AsRef<[u8]>) -> RespFrame {
    RespFrame::bulk(RedisString::from_bytes(value))
}

fn err(value: &[u8]) -> RespFrame {
    RespFrame::error(RedisString::from_bytes(value))
}

fn wrong_type() -> RespFrame {
    err(b"WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn wrong_arity(command: &[u8]) -> RespFrame {
    let mut msg = b"ERR wrong number of arguments for '".to_vec();
    msg.extend_from_slice(command);
    msg.extend_from_slice(b"' command");
    err(&msg)
}

fn checked_ttl_ms(raw: i64, unit_ms: u64) -> Option<u64> {
    u64::try_from(raw).ok()?.checked_mul(unit_ms)
}

/// `parseScanCursorOrReply` (`db.c`): a SCAN cursor must parse via `string2ull`
/// — a non-negative integer that fits in an unsigned 64-bit value. Returns the
/// parsed cursor, or `None` (→ "ERR invalid cursor") on anything else. The
/// engine ignores the cursor's value (single-pass) but still validates it so a
/// malformed cursor errors identically to the reference.
fn parse_scan_cursor(bytes: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(bytes).ok()?;
    text.parse::<u64>().ok()
}

/// `parseScanOptionsOrReply` (`db.c`), restricted to the options the edge
/// engine honours. Walks `[MATCH pat] [COUNT n] [TYPE t] [NOVALUES|NOSCORES]`
/// in order, matching the C error text and check-ordering: `COUNT < 1` →
/// `shared.syntaxerr`; `TYPE` is keyspace-only and unknown names →
/// "unknown type name '...'"; `NOVALUES` is HSCAN-only and `NOSCORES` is
/// ZSCAN-only (each with its specific error); any other token → `syntaxerr`. A
/// `MATCH *` is treated as "no pattern" (the C `use_pattern` fast-path).
fn parse_scan_options(
    args: &[Vec<u8>],
    kind: CollectionScan,
) -> Result<ScanOptions, RespFrame> {
    let mut opts = ScanOptions {
        pattern: None,
        type_filter: None,
        only_keys: false,
    };
    let mut i = 0;
    while i < args.len() {
        let opt = &args[i];
        let remaining = args.len() - i;
        if ascii_eq(opt, b"COUNT") && remaining >= 2 {
            let Some(count) = parse_i64(&args[i + 1]) else {
                return Err(err(b"ERR value is not an integer or out of range"));
            };
            if count < 1 {
                return Err(err(b"ERR syntax error"));
            }
            i += 2;
        } else if ascii_eq(opt, b"MATCH") && remaining >= 2 {
            let pattern = &args[i + 1];
            opts.pattern = if pattern.as_slice() == b"*" {
                None
            } else {
                Some(pattern.clone())
            };
            i += 2;
        } else if ascii_eq(opt, b"TYPE")
            && matches!(kind, CollectionScan::Keyspace)
            && remaining >= 2
        {
            let typename = args[i + 1].to_ascii_lowercase();
            let known: &[&[u8]] = &[b"string", b"list", b"set", b"zset", b"hash", b"stream"];
            if !known.contains(&typename.as_slice()) {
                let mut msg = b"ERR unknown type name '".to_vec();
                msg.extend_from_slice(&args[i + 1]);
                msg.push(b'\'');
                return Err(err(&msg));
            }
            opts.type_filter = Some(typename);
            i += 2;
        } else if ascii_eq(opt, b"NOVALUES") {
            if !matches!(kind, CollectionScan::Hash) {
                return Err(err(b"ERR NOVALUES option can only be used in HSCAN"));
            }
            opts.only_keys = true;
            i += 1;
        } else if ascii_eq(opt, b"NOSCORES") {
            if !matches!(kind, CollectionScan::ZSet) {
                return Err(err(b"ERR NOSCORES option can only be used in ZSCAN"));
            }
            opts.only_keys = true;
            i += 1;
        } else {
            return Err(err(b"ERR syntax error"));
        }
    }
    Ok(opts)
}

/// Build a SCAN-family reply: the 2-element `[cursor, [elements]]` array with
/// the cursor a bulk string `"0"` (mirroring `addReplyBulkLongLong(c, 0)` in
/// `scanGenericCommand`, e.g. the shared `emptyscan` frame). The engine always
/// completes in a single pass, so the cursor is unconditionally `"0"`.
fn scan_reply(items: Vec<RespFrame>) -> RespFrame {
    RespFrame::array(vec![bulk(b"0"), RespFrame::array(items)])
}

fn parse_i64(bytes: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(bytes).ok()?;
    if text.is_empty()
        || text.as_bytes().iter().any(|b| !b.is_ascii_digit())
            && !matches!(text.as_bytes(), [b'-', rest @ ..] if !rest.is_empty() && rest.iter().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    text.parse::<i64>().ok()
}

fn ascii_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(l, r)| l.eq_ignore_ascii_case(r))
}

/// Parse an INCRBYFLOAT/HINCRBYFLOAT increment (`getLongDoubleFromObject` →
/// `string2ld`, `util.c`). Strict: no surrounding whitespace, the whole slice
/// must parse, NaN is rejected. `inf`/`+inf`/`-inf` are accepted (the caller's
/// result guard then rejects them); leading `+`, leading `.`, and trailing `.`
/// are all valid, matching `strtold`. Returns `None` for any unparsable input.
fn parse_incr_float(bytes: &[u8]) -> Option<f64> {
    if bytes.is_empty() {
        return None;
    }
    let text = std::str::from_utf8(bytes).ok()?;
    if text.starts_with(char::is_whitespace) || text.ends_with(char::is_whitespace) {
        return None;
    }
    let value: f64 = text.parse().ok()?;
    if value.is_nan() {
        return None;
    }
    Some(value)
}

/// Parse a stored string value as a float for INCRBYFLOAT/HINCRBYFLOAT. Same
/// rules as `parse_incr_float` but also rejects Inf — a stored Inf can never be
/// produced by these commands, so it is treated as an invalid float.
fn parse_stored_float(bytes: &[u8]) -> Option<f64> {
    let value = parse_incr_float(bytes)?;
    if value.is_infinite() {
        return None;
    }
    Some(value)
}

/// Format a float the way valkey's `ld2string(buf, len, value, LD_STR_HUMAN)`
/// (`util.c`) does for INCRBYFLOAT/HINCRBYFLOAT replies: `%.17Lf` (fixed
/// notation, never exponential), then trailing zeros after the `.` are stripped,
/// then a bare trailing `.`, then `-0` is normalized to `0`. `inf`/`nan` are
/// guarded by the caller before this is ever reached. f64 arithmetic plus
/// `%.17f` reproduces valkey's `long double` `%.17Lf` byte-for-byte across the
/// representable-decimal range these commands operate on (verified against
/// `valkey-server` over 1000+ random and adversarial sums).
fn format_human_long_double(value: f64) -> Vec<u8> {
    let mut text = format!("{:.17}", value);
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if text == "-0" {
        text = "0".to_string();
    }
    text.into_bytes()
}

// ──────────────────────────────────────────────────────────────────────────
// GEO subsystem — a faithful port of geohash.c / geohash_helper.c / geo.c.
// Members are stored in a ZSET whose score is the 52-bit interleaved geohash
// (step 26) of (lon, lat). All float ops, constants and formatting mirror the
// reference so GEOPOS/GEODIST/GEOHASH/GEOSEARCH outputs are byte-identical.
// ──────────────────────────────────────────────────────────────────────────

/// Coordinate-range constraints (EPSG:900913); we cannot geocode at the poles.
const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LONG_MIN: f64 = -180.0;
const GEO_LONG_MAX: f64 = 180.0;
/// `26*2 = 52` bits of precision (`GEO_STEP_MAX`).
const GEO_STEP_MAX: u8 = 26;
/// Earth's quadratic mean radius for WGS-84, used by the haversine.
const GEO_EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;
const GEO_MERCATOR_MAX: f64 = 20037726.37;
const GEO_DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;

const GEO_CIRCULAR: i32 = 1;
const GEO_RECTANGLE: i32 = 2;

const GEO_SORT_NONE: u8 = 0;
const GEO_SORT_ASC: u8 = 1;
const GEO_SORT_DESC: u8 = 2;

const GEO_FLAG_COORDS: u32 = 1 << 0;
const GEO_FLAG_MEMBER: u32 = 1 << 1;
const GEO_FLAG_NOSTORE: u32 = 1 << 2;
const GEO_FLAG_SEARCH: u32 = 1 << 3;
const GEO_FLAG_SEARCHSTORE: u32 = 1 << 4;

/// Interleave the low bits of `xlo` and `ylo` (x in even positions, y in odd),
/// the bit-twiddle from `geohash.c interleave64`.
fn geo_interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
    ];
    const S: [u32; 5] = [1, 2, 4, 8, 16];
    let mut x = xlo as u64;
    let mut y = ylo as u64;
    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];
    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];
    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];
    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];
    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];
    x | (y << 1)
}

/// Reverse `geo_interleave64` (`geohash.c deinterleave64`).
fn geo_deinterleave64(interleaved: u64) -> u64 {
    const B: [u64; 6] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
        0x00000000FFFFFFFF,
    ];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];
    let mut x = interleaved;
    let mut y = interleaved >> 1;
    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];
    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];
    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];
    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];
    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];
    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];
    x | (y << 32)
}

#[derive(Clone, Copy, Default)]
struct GeoHashBits {
    bits: u64,
    step: u8,
}

#[derive(Clone, Copy, Default)]
struct GeoHashArea {
    lon_min: f64,
    lon_max: f64,
    lat_min: f64,
    lat_max: f64,
}

/// Encode `(lon, lat)` to a `GeoHashBits` at the given step over the WGS-84
/// coordinate range (`geohashEncode` with the standard ranges).
fn geo_encode_step(lon: f64, lat: f64, step: u8) -> GeoHashBits {
    let lat_offset = (lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);
    let long_offset = (lon - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);
    let lat_offset = lat_offset * ((1u64 << step) as f64);
    let long_offset = long_offset * ((1u64 << step) as f64);
    GeoHashBits {
        bits: geo_interleave64(lat_offset as u32, long_offset as u32),
        step,
    }
}

/// The 52-bit-aligned score for a member at `(lon, lat)` (`geohashEncodeWGS84`
/// at `GEO_STEP_MAX` then `geohashAlign52Bits` — at step 26 that shift is zero).
fn geo_encode_score(lon: f64, lat: f64) -> u64 {
    geo_align_52_bits(geo_encode_step(lon, lat, GEO_STEP_MAX))
}

/// Left-shift a hash's bits up to the fixed 52-bit field (`geohashAlign52Bits`).
fn geo_align_52_bits(hash: GeoHashBits) -> u64 {
    hash.bits << (52 - hash.step * 2)
}

/// Decode a `GeoHashBits` into its bounding box (`geohashDecode`). A zeroed hash
/// (bits == 0 && step == 0) decodes to the default empty area.
fn geo_decode_area(hash: GeoHashBits) -> GeoHashArea {
    if hash.bits == 0 && hash.step == 0 {
        return GeoHashArea::default();
    }
    let step = hash.step;
    let hash_sep = geo_deinterleave64(hash.bits);
    let lat_scale = GEO_LAT_MAX - GEO_LAT_MIN;
    let long_scale = GEO_LONG_MAX - GEO_LONG_MIN;
    let ilato = (hash_sep & 0xFFFF_FFFF) as u32;
    let ilono = (hash_sep >> 32) as u32;
    let scale = (1u64 << step) as f64;
    GeoHashArea {
        lat_min: GEO_LAT_MIN + (ilato as f64 / scale) * lat_scale,
        lat_max: GEO_LAT_MIN + ((ilato as f64 + 1.0) / scale) * lat_scale,
        lon_min: GEO_LONG_MIN + (ilono as f64 / scale) * long_scale,
        lon_max: GEO_LONG_MIN + ((ilono as f64 + 1.0) / scale) * long_scale,
    }
}

/// Decode a 52-bit score to the center `(lon, lat)`, clamped to the valid range
/// (`decodeGeohash` → `geohashDecodeToLongLatWGS84` → `geohashDecodeAreaToLongLat`).
fn geo_decode_score(score: u64) -> (f64, f64) {
    let area = geo_decode_area(GeoHashBits {
        bits: score,
        step: GEO_STEP_MAX,
    });
    let mut x = (area.lon_min + area.lon_max) / 2.0;
    if x > GEO_LONG_MAX {
        x = GEO_LONG_MAX;
    }
    if x < GEO_LONG_MIN {
        x = GEO_LONG_MIN;
    }
    let mut y = (area.lat_min + area.lat_max) / 2.0;
    if y > GEO_LAT_MAX {
        y = GEO_LAT_MAX;
    }
    if y < GEO_LAT_MIN {
        y = GEO_LAT_MIN;
    }
    (x, y)
}

/// Shift a hash one cell east/west in interleaved space (`geohash_move_x`).
fn geo_move_x(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }
    let mut x = hash.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let y = hash.bits & 0x5555_5555_5555_5555;
    let zz = 0x5555_5555_5555_5555u64 >> (64 - hash.step as u32 * 2);
    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x |= zz;
        x = x.wrapping_sub(zz + 1);
    }
    x &= 0xaaaa_aaaa_aaaa_aaaau64 >> (64 - hash.step as u32 * 2);
    hash.bits = x | y;
}

/// Shift a hash one cell north/south in interleaved space (`geohash_move_y`).
fn geo_move_y(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }
    let x = hash.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let mut y = hash.bits & 0x5555_5555_5555_5555;
    let zz = 0xaaaa_aaaa_aaaa_aaaau64 >> (64 - hash.step as u32 * 2);
    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y |= zz;
        y = y.wrapping_sub(zz + 1);
    }
    y &= 0x5555_5555_5555_5555u64 >> (64 - hash.step as u32 * 2);
    hash.bits = x | y;
}

#[derive(Clone, Copy, Default)]
struct GeoNeighbors {
    north: GeoHashBits,
    east: GeoHashBits,
    west: GeoHashBits,
    south: GeoHashBits,
    north_east: GeoHashBits,
    south_east: GeoHashBits,
    north_west: GeoHashBits,
    south_west: GeoHashBits,
}

/// Compute the 8 neighbor boxes of `hash` (`geohashNeighbors`).
fn geo_neighbors(hash: GeoHashBits) -> GeoNeighbors {
    let mk = |dx: i8, dy: i8| {
        let mut t = hash;
        geo_move_x(&mut t, dx);
        geo_move_y(&mut t, dy);
        t
    };
    GeoNeighbors {
        east: mk(1, 0),
        west: mk(-1, 0),
        south: mk(0, -1),
        north: mk(0, 1),
        north_west: mk(-1, 1),
        north_east: mk(1, 1),
        south_east: mk(1, -1),
        south_west: mk(-1, -1),
    }
}

fn geo_deg_rad(ang: f64) -> f64 {
    ang * GEO_DEG_TO_RAD
}

fn geo_rad_deg(ang: f64) -> f64 {
    ang / GEO_DEG_TO_RAD
}

/// Simplified latitude-only great-circle distance (`geohashGetLatDistance`).
fn geo_lat_distance(lat1d: f64, lat2d: f64) -> f64 {
    GEO_EARTH_RADIUS_IN_METERS * (geo_deg_rad(lat2d) - geo_deg_rad(lat1d)).abs()
}

/// Haversine great-circle distance in meters (`geohashGetDistance`), including
/// the same-longitude fast path guarded by `GEO_EPSILON`.
fn geo_haversine(lon1d: f64, lat1d: f64, lon2d: f64, lat2d: f64) -> f64 {
    let lon1r = geo_deg_rad(lon1d);
    let lon2r = geo_deg_rad(lon2d);
    let v = ((lon2r - lon1r) / 2.0).sin();
    const GEO_EPSILON: f64 = 1e-15;
    if v.abs() <= GEO_EPSILON {
        return geo_lat_distance(lat1d, lat2d);
    }
    let lat1r = geo_deg_rad(lat1d);
    let lat2r = geo_deg_rad(lat2d);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * GEO_EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

/// Estimate the geohash step (precision) covering `range_meters` at `lat`
/// (`geohashEstimateStepsByRadius`).
fn geo_estimate_steps(range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return 26;
    }
    let mut step: i32 = 1;
    let mut rm = range_meters;
    while rm < GEO_MERCATOR_MAX {
        rm *= 2.0;
        step += 1;
    }
    step -= 2;
    if lat > 66.0 || lat < -66.0 {
        step -= 1;
        if lat > 80.0 || lat < -80.0 {
            step -= 1;
        }
    }
    if step < 1 {
        step = 1;
    }
    if step > 26 {
        step = 26;
    }
    step as u8
}

#[derive(Clone, Copy, Default)]
struct GeoShape {
    kind: i32,
    xy: [f64; 2],
    conversion: f64,
    radius: f64,
    width: f64,
    height: f64,
    bounds: [f64; 4],
}

/// Compute the lon/lat bounding box of the search shape into `shape.bounds`
/// (`geohashBoundingBox`, circular/rectangle cases).
fn geo_bounding_box(shape: &mut GeoShape) {
    let (height, width) = if shape.kind == GEO_CIRCULAR {
        (shape.conversion * shape.radius, shape.conversion * shape.radius)
    } else {
        (
            shape.conversion * shape.height / 2.0,
            shape.conversion * shape.width / 2.0,
        )
    };
    let lon = shape.xy[0];
    let lat = shape.xy[1];
    let lat_delta = geo_rad_deg(height / GEO_EARTH_RADIUS_IN_METERS);
    let long_delta_top =
        geo_rad_deg(width / GEO_EARTH_RADIUS_IN_METERS / geo_deg_rad(lat + lat_delta).cos());
    let long_delta_bottom =
        geo_rad_deg(width / GEO_EARTH_RADIUS_IN_METERS / geo_deg_rad(lat - lat_delta).cos());
    let southern = lat < 0.0;
    shape.bounds[0] = if southern {
        lon - long_delta_bottom
    } else {
        lon - long_delta_top
    };
    shape.bounds[2] = if southern {
        lon + long_delta_bottom
    } else {
        lon + long_delta_top
    };
    shape.bounds[1] = lat - lat_delta;
    shape.bounds[3] = lat + lat_delta;
}

struct GeoRadius {
    hash: GeoHashBits,
    neighbors: GeoNeighbors,
}

/// Calculate the center hash + 8 neighbor boxes covering the search shape
/// (`geohashCalculateAreasByShapeWGS84`), including the step-decrease correction
/// near edges and the useless-neighbor pruning.
fn geo_calculate_areas(shape: &mut GeoShape) -> GeoRadius {
    geo_bounding_box(shape);
    let min_lon = shape.bounds[0];
    let min_lat = shape.bounds[1];
    let max_lon = shape.bounds[2];
    let max_lat = shape.bounds[3];
    let lon = shape.xy[0];
    let lat = shape.xy[1];
    let mut radius_meters = if shape.kind == GEO_CIRCULAR {
        shape.radius
    } else {
        ((shape.width / 2.0).powi(2) + (shape.height / 2.0).powi(2)).sqrt()
    };
    radius_meters *= shape.conversion;
    let mut steps = geo_estimate_steps(radius_meters, lat);
    let mut hash = geo_encode_step(lon, lat, steps);
    let mut neighbors = geo_neighbors(hash);
    let mut area = geo_decode_area(hash);
    let mut decrease = false;
    {
        let north = geo_decode_area(neighbors.north);
        let south = geo_decode_area(neighbors.south);
        let east = geo_decode_area(neighbors.east);
        let west = geo_decode_area(neighbors.west);
        if north.lat_max < max_lat {
            decrease = true;
        }
        if south.lat_min > min_lat {
            decrease = true;
        }
        if east.lon_max < max_lon {
            decrease = true;
        }
        if west.lon_min > min_lon {
            decrease = true;
        }
    }
    if steps > 1 && decrease {
        steps -= 1;
        hash = geo_encode_step(lon, lat, steps);
        neighbors = geo_neighbors(hash);
        area = geo_decode_area(hash);
    }
    if steps >= 2 {
        if area.lat_min < min_lat {
            neighbors.south = GeoHashBits::default();
            neighbors.south_west = GeoHashBits::default();
            neighbors.south_east = GeoHashBits::default();
        }
        if area.lat_max > max_lat {
            neighbors.north = GeoHashBits::default();
            neighbors.north_east = GeoHashBits::default();
            neighbors.north_west = GeoHashBits::default();
        }
        if area.lon_min < min_lon {
            neighbors.west = GeoHashBits::default();
            neighbors.south_west = GeoHashBits::default();
            neighbors.north_west = GeoHashBits::default();
        }
        if area.lon_max > max_lon {
            neighbors.east = GeoHashBits::default();
            neighbors.south_east = GeoHashBits::default();
            neighbors.north_east = GeoHashBits::default();
        }
    }
    GeoRadius { hash, neighbors }
}

/// Check whether a decoded point is inside the search shape, returning its
/// distance to the center (`geoWithinShape` for circular/rectangle).
fn geo_within_shape(shape: &GeoShape, x: f64, y: f64) -> Option<f64> {
    if shape.kind == GEO_CIRCULAR {
        let d = geo_haversine(shape.xy[0], shape.xy[1], x, y);
        if d > shape.radius * shape.conversion {
            return None;
        }
        Some(d)
    } else {
        let lat_distance = geo_lat_distance(y, shape.xy[1]);
        if lat_distance > shape.height * shape.conversion / 2.0 {
            return None;
        }
        let lon_distance = geo_haversine(x, y, shape.xy[0], y);
        if lon_distance > shape.width * shape.conversion / 2.0 {
            return None;
        }
        Some(geo_haversine(shape.xy[0], shape.xy[1], x, y))
    }
}

struct GeoPoint {
    member: Vec<u8>,
    dist: f64,
    score: u64,
    longitude: f64,
    latitude: f64,
}

/// Search `members` for all points inside `shape`, mirroring
/// `membersOfAllNeighbors`/`membersOfGeoHashBox`/`geoGetPointsInRange`. We don't
/// have a skiplist, so we filter the whole ZSET by each box's `[min, max)` score
/// window, dedup across overlapping boxes, and stop early once `limit` (>0) hits.
fn geo_search_points(
    members: &HashMap<Vec<u8>, f64>,
    shape: &GeoShape,
    limit: u64,
) -> Vec<GeoPoint> {
    let mut work = *shape;
    let radius = geo_calculate_areas(&mut work);
    let boxes = [
        radius.hash,
        radius.neighbors.north,
        radius.neighbors.south,
        radius.neighbors.east,
        radius.neighbors.west,
        radius.neighbors.north_east,
        radius.neighbors.north_west,
        radius.neighbors.south_east,
        radius.neighbors.south_west,
    ];
    let sorted = sorted_zset_entries(members);
    let mut result: Vec<GeoPoint> = Vec::new();
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut last: Option<GeoHashBits> = None;
    for (idx, b) in boxes.iter().enumerate() {
        if b.bits == 0 && b.step == 0 {
            continue;
        }
        if idx > 0 {
            if let Some(prev) = last {
                if b.bits == prev.bits && b.step == prev.step {
                    continue;
                }
            }
        }
        if limit != 0 && result.len() as u64 >= limit {
            break;
        }
        let min = geo_align_52_bits(*b);
        let mut bplus = *b;
        bplus.bits += 1;
        let max = geo_align_52_bits(bplus);
        for (member, score) in &sorted {
            let s = *score as u64;
            if s < min {
                continue;
            }
            if s >= max {
                continue;
            }
            let (x, y) = geo_decode_score(s);
            if let Some(dist) = geo_within_shape(shape, x, y) {
                if seen.insert(member.clone()) {
                    result.push(GeoPoint {
                        member: member.clone(),
                        dist,
                        score: s,
                        longitude: x,
                        latitude: y,
                    });
                }
            }
            if limit != 0 && result.len() as u64 >= limit {
                break;
            }
        }
        last = Some(*b);
    }
    result
}

/// Render a distance with `fixedpoint_d2string(_, 4)`: 4 fractional digits,
/// `llrint` (round-half-to-even) scaling.
fn geo_format_distance(distance: f64) -> Vec<u8> {
    let scaled = distance * 10000.0;
    let rounded = scaled.round_ties_even() as i64;
    let negative = rounded < 0;
    let value = rounded.unsigned_abs();
    let int_part = value / 10000;
    let frac = value % 10000;
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&int_part.to_string());
    out.push('.');
    out.push_str(&format!("{:04}", frac));
    out.into_bytes()
}

/// Re-encode a member's internal score as the 11-character standard geohash
/// string valkey emits (`geohashCommand`): decode to `(lon, lat)`, re-encode
/// over the standard `-180..180`/`-90..90` ranges at step 26, then read 5-bit
/// groups through valkey's base32 alphabet (the 11th char is always `0`).
fn geo_hash_string(score: u64) -> Vec<u8> {
    const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let (x, y) = geo_decode_score(score);
    let lat_offset = (y - (-90.0)) / 180.0 * ((1u64 << 26) as f64);
    let long_offset = (x - (-180.0)) / 360.0 * ((1u64 << 26) as f64);
    let bits = geo_interleave64(lat_offset as u32, long_offset as u32);
    let mut buf = Vec::with_capacity(11);
    for i in 0..11u32 {
        let idx = if i == 10 {
            0
        } else {
            ((bits >> (52 - ((i + 1) * 5))) & 0x1f) as usize
        };
        buf.push(ALPHABET[idx]);
    }
    buf
}

/// Parse a longitude/latitude/radius argument exactly like
/// `getDoubleFromObjectOrReply` (`strtod` over the whole string, rejecting NaN
/// and trailing garbage; the inf spellings are accepted by strtod but lie
/// outside the coordinate range so the caller rejects them).
fn parse_geo_double(bytes: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(bytes).ok()?.trim();
    if text.is_empty() {
        return None;
    }
    let value: f64 = text.parse().ok()?;
    if value.is_nan() {
        return None;
    }
    Some(value)
}

/// Format a double the way C `printf("%f", v)` does (6 fractional digits), used
/// only inside the `invalid longitude,latitude pair %f,%f` error.
fn format_c_double(value: f64) -> String {
    format!("{:.6}", value)
}

/// Returns the conversion factor to meters for a GEO unit (`extractUnitOrReply`),
/// or `None` for an unknown unit.
fn geo_unit_to_meters(unit: &[u8]) -> Option<f64> {
    if ascii_eq(unit, b"m") {
        Some(1.0)
    } else if ascii_eq(unit, b"km") {
        Some(1000.0)
    } else if ascii_eq(unit, b"ft") {
        Some(0.3048)
    } else if ascii_eq(unit, b"mi") {
        Some(1609.34)
    } else {
        None
    }
}

/// Validate a `(lon, lat)` pair, returning the reference error frame if out of
/// range (`extractLongLatOrReply`'s range check).
fn geo_check_lonlat(lon: f64, lat: f64) -> Option<RespFrame> {
    if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        let mut msg = b"ERR invalid longitude,latitude pair ".to_vec();
        msg.extend_from_slice(format_c_double(lon).as_bytes());
        msg.push(b',');
        msg.extend_from_slice(format_c_double(lat).as_bytes());
        return Some(err(&msg));
    }
    None
}

/// Parse `<radius> <unit>` into `(conversion_to_meters, radius)`
/// (`extractDistanceOrReply`), rejecting a non-numeric radius, a negative
/// radius, or a bad unit with the reference error strings.
fn geo_extract_distance(radius_arg: &[u8], unit_arg: &[u8]) -> Result<(f64, f64), RespFrame> {
    let Some(distance) = parse_geo_double(radius_arg) else {
        return Err(err(b"ERR need numeric radius"));
    };
    if distance < 0.0 {
        return Err(err(b"ERR radius cannot be negative"));
    }
    let Some(to_meters) = geo_unit_to_meters(unit_arg) else {
        return Err(err(b"ERR unsupported unit provided. please use M, KM, FT, MI"));
    };
    Ok((to_meters, distance))
}

/// Parse `<width> <height> <unit>` into `(conversion, width, height)`
/// (`extractBoxOrReply`).
fn geo_extract_box(
    width_arg: &[u8],
    height_arg: &[u8],
    unit_arg: &[u8],
) -> Result<(f64, f64, f64), RespFrame> {
    let Some(width) = parse_geo_double(width_arg) else {
        return Err(err(b"ERR need numeric width"));
    };
    let Some(height) = parse_geo_double(height_arg) else {
        return Err(err(b"ERR need numeric height"));
    };
    if height < 0.0 || width < 0.0 {
        return Err(err(b"ERR height or width cannot be negative"));
    }
    let Some(to_meters) = geo_unit_to_meters(unit_arg) else {
        return Err(err(b"ERR unsupported unit provided. please use M, KM, FT, MI"));
    };
    Ok((to_meters, width, height))
}

/// Glob match following valkey's `stringmatchlen` (`util.c`, case-sensitive):
/// `*` (any run), `?` (any single byte), `[...]` classes with `^` negation,
/// `a-z` ranges, and `\` escape. Faithful recursive port of `stringmatchlen_impl`
/// so KEYS pattern semantics match byte-for-byte.
fn string_match_len(pattern: &[u8], string: &[u8]) -> bool {
    string_match_len_impl(pattern, string, 0)
}

fn string_match_len_impl(pattern: &[u8], string: &[u8], nesting: u32) -> bool {
    if nesting > 1000 {
        return false;
    }
    let mut p = 0usize;
    let mut s = 0usize;
    let plen = pattern.len();
    let slen = string.len();
    while p < plen && s < slen {
        match pattern[p] {
            b'*' => {
                while p + 1 < plen && pattern[p + 1] == b'*' {
                    p += 1;
                }
                if p + 1 == plen {
                    return true;
                }
                let mut t = s;
                loop {
                    if string_match_len_impl(&pattern[p + 1..], &string[t..], nesting + 1) {
                        return true;
                    }
                    if t >= slen {
                        break;
                    }
                    t += 1;
                }
                return false;
            }
            b'?' => {
                s += 1;
            }
            b'[' => {
                p += 1;
                let not_op = p < plen && pattern[p] == b'^';
                if not_op {
                    p += 1;
                }
                let mut matched = false;
                loop {
                    if p + 1 < plen && pattern[p] == b'\\' {
                        p += 1;
                        if pattern[p] == string[s] {
                            matched = true;
                        }
                    } else if p >= plen {
                        p -= 1;
                        break;
                    } else if pattern[p] == b']' {
                        break;
                    } else if p + 2 < plen && pattern[p + 1] == b'-' {
                        let mut start = pattern[p];
                        let mut end = pattern[p + 2];
                        if start > end {
                            std::mem::swap(&mut start, &mut end);
                        }
                        let c = string[s];
                        p += 2;
                        if c >= start && c <= end {
                            matched = true;
                        }
                    } else if pattern[p] == string[s] {
                        matched = true;
                    }
                    p += 1;
                }
                let matched = if not_op { !matched } else { matched };
                if !matched {
                    return false;
                }
                s += 1;
            }
            b'\\' if p + 1 < plen => {
                p += 1;
                if pattern[p] != string[s] {
                    return false;
                }
                s += 1;
            }
            other => {
                if other != string[s] {
                    return false;
                }
                s += 1;
            }
        }
        p += 1;
        if s == slen {
            while p < plen && pattern[p] == b'*' {
                p += 1;
            }
            break;
        }
    }
    p == plen && s == slen
}

/// Parse a `LEFT`/`RIGHT` direction argument (`getListPositionFromObjectOrReply`,
/// `t_list.c`). `LEFT` is the head, `RIGHT` is the tail. Returns `None` for any
/// other token so the caller can emit a syntax error.
fn parse_list_position(bytes: &[u8]) -> Option<ListEnd> {
    if ascii_eq(bytes, b"LEFT") {
        Some(ListEnd::Head)
    } else if ascii_eq(bytes, b"RIGHT") {
        Some(ListEnd::Tail)
    } else {
        None
    }
}

fn normalise_sha(bytes: &[u8]) -> Option<[u8; 40]> {
    if bytes.len() != 40 {
        return None;
    }
    let mut out = [0u8; 40];
    for (index, byte) in bytes.iter().enumerate() {
        out[index] = match *byte {
            b'0'..=b'9' | b'a'..=b'f' => *byte,
            b'A'..=b'F' => *byte + 32,
            _ => return None,
        };
    }
    Some(out)
}

fn sha1_hex(data: &[u8]) -> [u8; 40] {
    let digest = sha1_digest(data);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 40];
    for (index, byte) in digest.iter().enumerate() {
        out[index * 2] = HEX[(byte >> 4) as usize];
        out[index * 2 + 1] = HEX[(byte & 0x0f) as usize];
    }
    out
}

fn sha1_digest(data: &[u8]) -> [u8; 20] {
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;
    let bit_len = (data.len() as u64) * 8;

    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks(64) {
        let mut w = [0u32; 80];
        for index in 0..16 {
            w[index] = u32::from_be_bytes([
                chunk[index * 4],
                chunk[index * 4 + 1],
                chunk[index * 4 + 2],
                chunk[index * 4 + 3],
            ]);
        }
        for index in 16..80 {
            w[index] = (w[index - 3] ^ w[index - 8] ^ w[index - 14] ^ w[index - 16]).rotate_left(1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;

        for index in 0..80 {
            let (f, k) = if index < 20 {
                ((b & c) | ((!b) & d), 0x5A827999u32)
            } else if index < 40 {
                (b ^ c ^ d, 0x6ED9EBA1u32)
            } else if index < 60 {
                ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32)
            } else {
                (b ^ c ^ d, 0xCA62C1D6)
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[index]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use redis_protocol::encode_resp2;

    use super::*;

    #[test]
    fn geo_encode_decode_distance_match_valkey_sicily() {
        // Ground truth captured from the pinned reference valkey-server for the
        // canonical Sicily example. Locks the 52-bit score, the %.17Lf-trimmed
        // GEOPOS coordinate strings, the 4-digit GEODIST output across units,
        // and the 11-char GEOHASH re-encoding so a refactor can't silently
        // break byte-for-byte float parity without this fast test catching it.
        let palermo = geo_encode_score(13.361389, 38.115556);
        let catania = geo_encode_score(15.087269, 37.502669);
        assert_eq!(palermo, 3479099956230698);
        assert_eq!(catania, 3479447370796909);

        let (px, py) = geo_decode_score(palermo);
        assert_eq!(format_human_long_double(px), b"13.36138933897018433".to_vec());
        assert_eq!(format_human_long_double(py), b"38.11555639549629859".to_vec());

        let dist = geo_haversine(px, py, geo_decode_score(catania).0, geo_decode_score(catania).1);
        assert_eq!(geo_format_distance(dist), b"166274.1516".to_vec());
        assert_eq!(geo_format_distance(dist / 1000.0), b"166.2742".to_vec());
        assert_eq!(geo_format_distance(dist / 1609.34), b"103.3182".to_vec());
        assert_eq!(geo_format_distance(dist / 0.3048), b"545518.8700".to_vec());

        assert_eq!(geo_hash_string(palermo), b"sqc8b49rny0".to_vec());
        assert_eq!(geo_hash_string(catania), b"sqdtr74hyu0".to_vec());
    }

    #[test]
    fn mutation_epoch_tracks_state_changes_not_reads() {
        let mut engine = Engine::new_in_memory();
        let start = engine.mutation_epoch();

        engine.execute(&argv(&[b"GET", b"missing"]));
        engine.execute(&argv(&[b"EXISTS", b"missing"]));
        engine.execute(&argv(&[b"TTL", b"missing"]));
        engine.execute(&argv(&[b"DEL", b"missing"]));
        engine.execute(&argv(&[b"EXPIRE", b"missing", b"10"]));
        assert_eq!(engine.mutation_epoch(), start);

        engine.execute(&argv(&[b"SET", b"k", b"v"]));
        let after_set = engine.mutation_epoch();
        assert_ne!(after_set, start);

        engine.execute(&argv(&[b"GET", b"k"]));
        assert_eq!(engine.mutation_epoch(), after_set);

        engine.execute(&argv(&[b"EXPIRE", b"k", b"10"]));
        let after_expire = engine.mutation_epoch();
        assert_ne!(after_expire, after_set);

        engine.execute(&argv(&[b"DEL", b"k"]));
        assert_ne!(engine.mutation_epoch(), after_expire);
    }

    fn script_load(engine: &mut Engine<NoopHost>, body: &[u8]) -> [u8; 40] {
        let frame = engine.execute(&argv(&[b"SCRIPT", b"LOAD", body]));
        match frame {
            RespFrame::Bulk(Some(sha)) => {
                let mut out = [0u8; 40];
                out.copy_from_slice(&sha);
                out
            }
            other => panic!("SCRIPT LOAD did not return a sha: {other:?}"),
        }
    }

    fn script_exists(engine: &mut Engine<NoopHost>, sha: &[u8; 40]) -> bool {
        let frame = engine.execute(&argv(&[b"SCRIPT", b"EXISTS", sha]));
        matches!(frame, RespFrame::Array(Some(items))
            if matches!(items.first(), Some(RespFrame::Integer(1))))
    }

    #[test]
    fn script_cache_evicts_least_recently_used_past_the_count_cap() {
        let mut engine = Engine::new_in_memory();
        let first = script_load(&mut engine, b"return 0");
        let mut shas = vec![first];
        for index in 1..=MAX_CACHED_SCRIPTS {
            let body = format!("return {index}");
            shas.push(script_load(&mut engine, body.as_bytes()));
        }

        assert!(
            !script_exists(&mut engine, &shas[0]),
            "the oldest untouched script must be evicted once the cap is exceeded"
        );
        assert!(
            script_exists(&mut engine, shas.last().unwrap()),
            "the most recent script must survive"
        );
        assert_eq!(
            engine.execute(&argv(&[b"EVALSHA", &shas[0], b"0"])),
            err(b"NOSCRIPT No matching script."),
            "an evicted script answers EVALSHA with NOSCRIPT"
        );
    }

    #[test]
    fn evalsha_use_protects_a_hot_script_from_eviction() {
        let mut engine = Engine::new_in_memory();
        let hot = script_load(&mut engine, b"return 'hot'");
        for index in 0..MAX_CACHED_SCRIPTS {
            let body = format!("return {index}");
            script_load(&mut engine, body.as_bytes());
            engine.execute(&argv(&[b"EVALSHA", &hot, b"0"]));
        }
        assert!(
            script_exists(&mut engine, &hot),
            "a script kept warm by EVALSHA must not be evicted by one-off loads"
        );
    }

    #[test]
    fn script_flush_resets_the_bounded_cache() {
        let mut engine = Engine::new_in_memory();
        let sha = script_load(&mut engine, b"return 1");
        assert!(script_exists(&mut engine, &sha));
        engine.execute(&argv(&[b"SCRIPT", b"FLUSH"]));
        assert!(!script_exists(&mut engine, &sha));
        let reloaded = script_load(&mut engine, b"return 1");
        assert_eq!(reloaded, sha, "the same body re-hashes to the same sha");
        assert!(script_exists(&mut engine, &sha));
    }

    #[test]
    fn mutation_epoch_ignores_script_cache_but_sees_script_writes() {
        let mut engine = Engine::new_in_memory();
        let start = engine.mutation_epoch();

        engine.execute(&argv(&[b"SCRIPT", b"LOAD", b"return 1"]));
        assert_eq!(engine.mutation_epoch(), start);

        engine.execute(&argv(&[b"EVAL", b"return 1", b"0"]));
        assert_eq!(engine.mutation_epoch(), start);

        engine.execute(&argv(&[
            b"EVAL",
            b"return redis.call('SET', KEYS[1], ARGV[1])",
            b"1",
            b"k",
            b"v",
        ]));
        assert_ne!(engine.mutation_epoch(), start);
    }

    /// Run `seed` (unmeasured), then assert that executing `measured` bumps the
    /// mutation epoch whenever it changes snapshot-visible state. `export_snapshot`
    /// is the content fingerprint: it purges already-expired keys before
    /// serializing, so passive expiry never registers as a change and the clock
    /// is held fixed across the before/after pair. The guarded direction is
    /// `content changed ⇒ epoch bumped`: under-marking (a write the persistence
    /// layer would silently drop) fails here; conservative over-marking (a bump
    /// without a content change, e.g. HSET writing an identical value) is safe
    /// and allowed.
    fn assert_write_bumps_epoch(seed: &[&[&[u8]]], measured: &[&[u8]]) {
        let label = || {
            measured
                .iter()
                .map(|part| String::from_utf8_lossy(part).into_owned())
                .collect::<Vec<_>>()
        };
        let mut engine = Engine::new_in_memory();
        engine.host_mut().set_now_millis(1_000);
        for command in seed {
            engine.execute(&argv(command));
        }
        // A per-key storage map standing in for the host. Seed it with the
        // pre-command state so the dirty-key flush has a baseline to mutate.
        let mut storage: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        for key in engine.take_dirty() {
            if let Some(bytes) = engine.export_key(&key) {
                storage.insert(key, bytes);
            }
        }
        let before = engine.export_snapshot();
        let epoch_before = engine.mutation_epoch();
        engine.execute(&argv(measured));
        let after = engine.export_snapshot();
        let epoch_after = engine.mutation_epoch();
        let content_changed = before != after;
        let epoch_bumped = epoch_after != epoch_before;
        assert!(
            !content_changed || epoch_bumped,
            "command {:?} changed snapshot state without bumping the mutation epoch — the persistence layer would silently drop this write",
            label(),
        );
        // Flush only the dirty keys into storage, then rebuild a fresh engine
        // from storage alone. It must reproduce the full post-command state —
        // proving the dirty set captured every change a host needs to persist.
        for key in engine.take_dirty() {
            match engine.export_key(&key) {
                Some(bytes) => {
                    storage.insert(key, bytes);
                }
                None => {
                    storage.remove(&key);
                }
            }
        }
        let mut restored = Engine::new_in_memory();
        restored.host_mut().set_now_millis(1_000);
        let mut storage_keys: Vec<_> = storage.keys().cloned().collect();
        storage_keys.sort();
        for key in storage_keys {
            restored.import_key(&storage[&key]).unwrap();
        }
        assert_eq!(
            restored.export_snapshot(),
            after,
            "command {:?}: per-key dirty flush did not reproduce the full state",
            label(),
        );
    }

    #[test]
    fn every_command_that_writes_visible_state_bumps_the_epoch() {
        let seed_k: &[&[u8]] = &[b"SET", b"k", b"v"];
        let seed_n: &[&[u8]] = &[b"SET", b"n", b"5"];
        let seed_h: &[&[u8]] = &[b"HSET", b"h", b"f", b"v"];
        let seed_z: &[&[u8]] = &[b"ZADD", b"z", b"1", b"a"];
        let seed_l: &[&[u8]] = &[b"RPUSH", b"l", b"a", b"b", b"c"];
        let seed_s: &[&[u8]] = &[b"SADD", b"s", b"a", b"b", b"c"];

        let cases: &[(&[&[&[u8]]], &[&[u8]])] = &[
            (&[], &[b"SET", b"k", b"v"]),
            (&[seed_k], &[b"GET", b"k"]),
            (&[seed_k], &[b"EXISTS", b"k"]),
            (&[seed_k], &[b"SET", b"k", b"v2"]),
            (&[seed_k], &[b"SET", b"k", b"v2", b"NX"]),
            (&[], &[b"SET", b"fresh", b"v", b"NX"]),
            (&[seed_k], &[b"SET", b"k", b"v2", b"XX"]),
            (&[], &[b"SET", b"missing", b"v", b"XX"]),
            (&[seed_k], &[b"SET", b"k", b"v2", b"GET"]),
            (&[seed_k], &[b"SETEX", b"k", b"100", b"v2"]),
            (&[seed_k], &[b"DEL", b"k"]),
            (&[], &[b"DEL", b"missing"]),
            (&[seed_n], &[b"INCR", b"n"]),
            (&[], &[b"INCR", b"new"]),
            (&[seed_n], &[b"INCRBY", b"n", b"3"]),
            (&[seed_k], &[b"EXPIRE", b"k", b"100"]),
            (&[], &[b"EXPIRE", b"missing", b"100"]),
            (&[seed_k], &[b"PEXPIRE", b"k", b"100000"]),
            (&[seed_k], &[b"EXPIRE", b"k", b"-1"]),
            (&[seed_k], &[b"EXPIREAT", b"k", b"99999999999"]),
            (&[seed_k], &[b"PEXPIREAT", b"k", b"99999999999000"]),
            (&[seed_k], &[b"EXPIREAT", b"k", b"1"]),
            (
                &[&[b"SET", b"k", b"v", b"EX", b"100"]],
                &[b"PERSIST", b"k"],
            ),
            (&[seed_k], &[b"PERSIST", b"k"]),
            (&[seed_k], &[b"TTL", b"k"]),
            (&[seed_k], &[b"PTTL", b"k"]),
            (&[seed_k], &[b"EXPIRETIME", b"k"]),
            (&[seed_k], &[b"PEXPIRETIME", b"k"]),
            (&[seed_k], &[b"TYPE", b"k"]),
            (&[], &[b"TYPE", b"missing"]),
            (&[seed_k], &[b"TOUCH", b"k"]),
            (&[seed_k], &[b"UNLINK", b"k"]),
            (&[seed_k], &[b"RENAME", b"k", b"k2"]),
            (
                &[&[b"SET", b"k", b"v", b"EX", b"100"]],
                &[b"RENAME", b"k", b"k2"],
            ),
            (&[seed_k], &[b"RENAMENX", b"k", b"k2"]),
            (&[seed_k], &[b"COPY", b"k", b"k2"]),
            (
                &[&[b"SET", b"k", b"v", b"EX", b"100"]],
                &[b"COPY", b"k", b"k2"],
            ),
            (&[seed_k], &[b"FLUSHALL"]),
            (&[seed_k, seed_h, seed_z], &[b"FLUSHALL"]),
            (&[], &[b"PING"]),
            (&[], &[b"ECHO", b"hi"]),
            (&[seed_h], &[b"HSET", b"h", b"f2", b"v2"]),
            (&[seed_h], &[b"HGET", b"h", b"f"]),
            (&[seed_h], &[b"HGETALL", b"h"]),
            (&[seed_h], &[b"HDEL", b"h", b"f"]),
            (&[seed_h], &[b"HDEL", b"h", b"missing"]),
            (&[seed_h], &[b"HEXPIRE", b"h", b"100", b"FIELDS", b"1", b"f"]),
            (&[seed_h], &[b"HPEXPIRE", b"h", b"100000", b"FIELDS", b"1", b"f"]),
            (
                &[seed_h],
                &[b"HEXPIREAT", b"h", b"99999999999", b"FIELDS", b"1", b"f"],
            ),
            (&[seed_h], &[b"HEXPIRE", b"h", b"0", b"FIELDS", b"1", b"f"]),
            (&[seed_h], &[b"HEXPIRE", b"h", b"100", b"FIELDS", b"1", b"nope"]),
            (&[seed_h], &[b"HTTL", b"h", b"FIELDS", b"1", b"f"]),
            (&[seed_h], &[b"HPTTL", b"h", b"FIELDS", b"1", b"f"]),
            (&[seed_h], &[b"HEXPIRETIME", b"h", b"FIELDS", b"1", b"f"]),
            (
                &[seed_h, &[b"HEXPIRE", b"h", b"100", b"FIELDS", b"1", b"f"]],
                &[b"HPERSIST", b"h", b"FIELDS", b"1", b"f"],
            ),
            (&[seed_h], &[b"HPERSIST", b"h", b"FIELDS", b"1", b"f"]),
            (&[seed_h], &[b"HGETEX", b"h", b"FIELDS", b"1", b"f"]),
            (&[seed_h], &[b"HGETEX", b"h", b"EX", b"100", b"FIELDS", b"1", b"f"]),
            (
                &[seed_h, &[b"HEXPIRE", b"h", b"100", b"FIELDS", b"1", b"f"]],
                &[b"HGETEX", b"h", b"PERSIST", b"FIELDS", b"1", b"f"],
            ),
            (&[seed_h], &[b"HGETEX", b"h", b"EX", b"0", b"FIELDS", b"1", b"f"]),
            (&[seed_h], &[b"HGETDEL", b"h", b"FIELDS", b"1", b"f"]),
            (&[seed_h], &[b"HGETDEL", b"h", b"FIELDS", b"1", b"missing"]),
            (&[], &[b"HSETEX", b"hx", b"EX", b"100", b"FIELDS", b"1", b"a", b"1"]),
            (&[seed_h], &[b"HSETEX", b"h", b"FIELDS", b"1", b"f", b"z"]),
            (
                &[seed_h, &[b"HEXPIRE", b"h", b"100", b"FIELDS", b"1", b"f"]],
                &[b"HSETEX", b"h", b"KEEPTTL", b"FIELDS", b"1", b"f", b"z"],
            ),
            (&[seed_h], &[b"HSETEX", b"h", b"EX", b"0", b"FIELDS", b"1", b"f", b"z"]),
            (&[seed_h], &[b"HSETEX", b"h", b"FNX", b"FIELDS", b"1", b"f", b"z"]),
            (&[seed_h], &[b"HSETEX", b"h", b"FXX", b"FIELDS", b"1", b"miss", b"z"]),
            (&[seed_h], &[b"HINCRBY", b"h", b"n", b"3"]),
            (&[seed_h], &[b"HINCRBYFLOAT", b"h", b"n", b"1.5"]),
            (&[], &[b"ZADD", b"z", b"1", b"a"]),
            (&[seed_z], &[b"ZADD", b"z", b"1", b"a", b"NX"]),
            (&[seed_z], &[b"ZADD", b"z", b"2", b"a"]),
            (&[seed_z], &[b"ZADD", b"z", b"9", b"b", b"XX"]),
            (&[seed_z], &[b"ZSCORE", b"z", b"a"]),
            (&[seed_z], &[b"ZINCRBY", b"z", b"5", b"a"]),
            (&[seed_z], &[b"ZREM", b"z", b"a"]),
            (&[seed_z], &[b"ZREM", b"z", b"missing"]),
            (&[seed_z], &[b"ZCARD", b"z"]),
            (&[seed_z], &[b"ZRANGE", b"z", b"0", b"-1"]),
            (&[seed_z], &[b"ZRANGEBYSCORE", b"z", b"-inf", b"+inf"]),
            (&[], &[b"LPUSH", b"l", b"a", b"b", b"c"]),
            (&[], &[b"RPUSH", b"l", b"a", b"b", b"c"]),
            (&[seed_l], &[b"LPUSHX", b"l", b"z"]),
            (&[], &[b"LPUSHX", b"missing", b"z"]),
            (&[seed_l], &[b"RPUSHX", b"l", b"z"]),
            (&[seed_l], &[b"LPOP", b"l"]),
            (&[seed_l], &[b"RPOP", b"l"]),
            (&[seed_l], &[b"LPOP", b"l", b"2"]),
            (&[seed_l], &[b"LPOP", b"l", b"9"]),
            (&[&[b"RPUSH", b"l", b"x"]], &[b"LPOP", b"l"]),
            (&[], &[b"LPOP", b"missing"]),
            (&[seed_l], &[b"LLEN", b"l"]),
            (&[seed_l], &[b"LRANGE", b"l", b"0", b"-1"]),
            (&[seed_l], &[b"LINDEX", b"l", b"0"]),
            (&[seed_l], &[b"LSET", b"l", b"0", b"z"]),
            (&[seed_l], &[b"LINSERT", b"l", b"BEFORE", b"b", b"x"]),
            (&[seed_l], &[b"LINSERT", b"l", b"AFTER", b"missing", b"x"]),
            (&[seed_l], &[b"LREM", b"l", b"0", b"a"]),
            (&[&[b"RPUSH", b"l", b"a", b"a", b"a"]], &[b"LREM", b"l", b"0", b"a"]),
            (&[seed_l], &[b"LTRIM", b"l", b"1", b"1"]),
            (&[seed_l], &[b"LTRIM", b"l", b"5", b"9"]),
            (&[seed_l], &[b"RPOPLPUSH", b"l", b"dst"]),
            (&[&[b"RPUSH", b"l", b"x"]], &[b"RPOPLPUSH", b"l", b"dst"]),
            (&[], &[b"RPOPLPUSH", b"missing", b"dst"]),
            (&[seed_l], &[b"RPOPLPUSH", b"l", b"l"]),
            (&[seed_l], &[b"LMOVE", b"l", b"dst", b"LEFT", b"RIGHT"]),
            (&[seed_l], &[b"LMOVE", b"l", b"dst", b"RIGHT", b"LEFT"]),
            (&[seed_l], &[b"LMPOP", b"1", b"l", b"LEFT"]),
            (&[seed_l], &[b"LMPOP", b"2", b"missing", b"l", b"RIGHT", b"COUNT", b"2"]),
            (&[], &[b"LMPOP", b"1", b"missing", b"LEFT"]),
            (&[&[b"RPUSH", b"l", b"x"]], &[b"LMPOP", b"1", b"l", b"LEFT"]),
            (&[seed_l], &[b"LPOS", b"l", b"a"]),
            (&[seed_l], &[b"LPOS", b"l", b"missing", b"COUNT", b"0"]),
            (&[], &[b"SADD", b"s", b"a", b"b", b"c"]),
            (&[seed_s], &[b"SADD", b"s", b"a"]),
            (&[seed_s], &[b"SADD", b"s", b"d"]),
            (&[seed_s], &[b"SREM", b"s", b"a"]),
            (&[seed_s], &[b"SREM", b"s", b"missing"]),
            (&[&[b"SADD", b"s", b"only"]], &[b"SREM", b"s", b"only"]),
            (&[seed_s], &[b"SCARD", b"s"]),
            (&[seed_s], &[b"SISMEMBER", b"s", b"a"]),
            (&[seed_s], &[b"SMISMEMBER", b"s", b"a", b"x"]),
            (&[seed_s], &[b"SMEMBERS", b"s"]),
            (&[seed_s], &[b"SMOVE", b"s", b"dst", b"a"]),
            (&[seed_s], &[b"SMOVE", b"s", b"dst", b"missing"]),
            (&[&[b"SADD", b"s", b"x"]], &[b"SMOVE", b"s", b"dst", b"x"]),
            (&[seed_s], &[b"SINTER", b"s"]),
            (&[seed_s], &[b"SUNION", b"s"]),
            (&[seed_s], &[b"SDIFF", b"s"]),
            (&[seed_s], &[b"SINTERCARD", b"1", b"s"]),
            (&[seed_s], &[b"SINTERCARD", b"1", b"s", b"LIMIT", b"2"]),
            (&[seed_s], &[b"SINTERSTORE", b"dst", b"s"]),
            (&[seed_s], &[b"SUNIONSTORE", b"dst", b"s"]),
            (&[seed_s], &[b"SDIFFSTORE", b"dst", b"s"]),
            (&[seed_s], &[b"SDIFFSTORE", b"dst", b"s", b"s"]),
            (&[], &[b"SCRIPT", b"LOAD", b"return 1"]),
            (&[], &[b"EVAL", b"return 1", b"0"]),
            (
                &[],
                &[
                    b"EVAL",
                    b"return redis.call('SET', KEYS[1], '1')",
                    b"1",
                    b"ek",
                ],
            ),
            (
                &[seed_k],
                &[b"EVAL", b"return redis.call('GET', KEYS[1])", b"1", b"k"],
            ),
        ];

        for (seed, measured) in cases {
            assert_write_bumps_epoch(seed, measured);
        }
    }

    const TOKEN_BUCKET_SCRIPT: &[u8] = br#"
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local capacity = tonumber(ARGV[2])
        local refill_tokens = tonumber(ARGV[3])
        local refill_ms = tonumber(ARGV[4])
        local cost = tonumber(ARGV[5])
        local ttl_ms = tonumber(ARGV[6])

        local function ceil_div(num, denom)
            return math.floor((num + denom - 1) / denom)
        end

        local tokens = capacity
        local updated_at = now
        local raw = redis.call('GET', key)
        if raw then
            local sep = string.find(raw, ':', 1, true)
            if sep then
                tokens = tonumber(string.sub(raw, 1, sep - 1))
                updated_at = tonumber(string.sub(raw, sep + 1))
            end
        end
        if tokens == nil then tokens = capacity end
        if updated_at == nil then updated_at = now end
        if now < updated_at then updated_at = now end

        local elapsed = now - updated_at
        local refill = math.floor(elapsed * refill_tokens / refill_ms)
        if refill > 0 then
            tokens = tokens + refill
            if tokens > capacity then tokens = capacity end
            updated_at = updated_at + math.floor(refill * refill_ms / refill_tokens)
        end

        local allowed = 0
        local retry_after = 0
        if tokens >= cost then
            tokens = tokens - cost
            allowed = 1
        else
            local missing = cost - tokens
            retry_after = updated_at + ceil_div(missing * refill_ms, refill_tokens) - now
            if retry_after < 0 then retry_after = 0 end
        end

        local reset_ms = updated_at + ceil_div((capacity - tokens) * refill_ms, refill_tokens)
        redis.call('SET', key, tostring(tokens) .. ':' .. tostring(updated_at), 'PX', ttl_ms)
        return {'allowed', allowed, 'remaining', tokens, 'reset_ms', reset_ms, 'retry_after_ms', retry_after}
    "#;

    const HASH_POLICY_TOKEN_BUCKET_SCRIPT: &[u8] = br#"
        local bucket_key = KEYS[1]
        local policy_key = KEYS[2]
        local now = tonumber(ARGV[1])
        local cost = tonumber(ARGV[2])

        local capacity = tonumber(redis.call('HGET', policy_key, 'capacity') or '10')
        local refill_tokens = tonumber(redis.call('HGET', policy_key, 'refill_tokens') or '5')
        local refill_ms = tonumber(redis.call('HGET', policy_key, 'refill_ms') or '1000')
        local ttl_ms = tonumber(redis.call('HGET', policy_key, 'ttl_ms') or '60000')

        local function ceil_div(num, denom)
            return math.floor((num + denom - 1) / denom)
        end

        local tokens = capacity
        local updated_at = now
        local raw = redis.call('GET', bucket_key)
        if raw then
            local sep = string.find(raw, ':', 1, true)
            if sep then
                tokens = tonumber(string.sub(raw, 1, sep - 1))
                updated_at = tonumber(string.sub(raw, sep + 1))
            end
        end
        if tokens == nil then tokens = capacity end
        if updated_at == nil then updated_at = now end
        if now < updated_at then updated_at = now end

        local elapsed = now - updated_at
        local refill = math.floor(elapsed * refill_tokens / refill_ms)
        if refill > 0 then
            tokens = tokens + refill
            if tokens > capacity then tokens = capacity end
            updated_at = updated_at + math.floor(refill * refill_ms / refill_tokens)
        end

        local allowed = 0
        local retry_after = 0
        if tokens >= cost then
            tokens = tokens - cost
            allowed = 1
        else
            local missing = cost - tokens
            retry_after = updated_at + ceil_div(missing * refill_ms, refill_tokens) - now
            if retry_after < 0 then retry_after = 0 end
        end

        local reset_ms = updated_at + ceil_div((capacity - tokens) * refill_ms, refill_tokens)
        redis.call('SET', bucket_key, tostring(tokens) .. ':' .. tostring(updated_at), 'PX', ttl_ms)
        return {'allowed', allowed, 'remaining', tokens, 'reset_ms', reset_ms, 'retry_after_ms', retry_after, 'capacity', capacity}
    "#;

    fn argv(items: &[&[u8]]) -> Vec<Vec<u8>> {
        items.iter().map(|item| item.to_vec()).collect()
    }

    fn resp2(frame: &RespFrame) -> Vec<u8> {
        let mut out = Vec::new();
        encode_resp2(frame, &mut out);
        out
    }

    fn response_json(response: RestResponse) -> JsonValue {
        assert_eq!(response.content_type, APPLICATION_JSON);
        serde_json::from_slice(&response.body).unwrap()
    }

    fn load_token_bucket(engine: &mut Engine<NoopHost>) -> Vec<u8> {
        let reply = engine.execute(&argv(&[b"SCRIPT", b"LOAD", TOKEN_BUCKET_SCRIPT]));
        match reply {
            RespFrame::Bulk(Some(sha)) => sha.into_bytes(),
            other => panic!("unexpected script load reply: {other:?}"),
        }
    }

    fn evalsha_token_bucket(engine: &mut Engine<NoopHost>, sha: &[u8], now: &[u8]) -> Vec<u8> {
        let request = vec![
            b"EVALSHA".to_vec(),
            sha.to_vec(),
            b"1".to_vec(),
            b"edge:tenant:42:tokens".to_vec(),
            now.to_vec(),
            b"10".to_vec(),
            b"5".to_vec(),
            b"1000".to_vec(),
            b"7".to_vec(),
            b"60000".to_vec(),
        ];
        resp2(&engine.execute(&request))
    }

    fn rest_load_token_bucket(engine: &mut Engine<NoopHost>) -> Vec<u8> {
        let script = String::from_utf8_lossy(TOKEN_BUCKET_SCRIPT);
        let body = serde_json::to_vec(&json!(["SCRIPT", "LOAD", script])).unwrap();
        let response = engine.execute_rest(RestRequest::post("/", &body));
        assert_eq!(response.status, 200);
        let value = response_json(response);
        value["result"].as_str().unwrap().as_bytes().to_vec()
    }

    fn rest_evalsha_token_bucket(
        engine: &mut Engine<NoopHost>,
        sha: &[u8],
        now: &str,
    ) -> JsonValue {
        let sha = String::from_utf8_lossy(sha);
        let body = serde_json::to_vec(&json!([
            "EVALSHA",
            sha,
            1,
            "edge:tenant:42:tokens",
            now,
            10,
            5,
            1000,
            7,
            60000
        ]))
        .unwrap();
        let response = engine.execute_rest(RestRequest::post("/EVALSHA", &body));
        assert_eq!(response.status, 200);
        response_json(response)
    }

    fn rest_evalsha_hash_policy_bucket(
        engine: &mut Engine<NoopHost>,
        sha: &[u8],
        now: &str,
    ) -> JsonValue {
        let sha = String::from_utf8_lossy(sha);
        let body = serde_json::to_vec(&json!([
            "EVALSHA",
            sha,
            2,
            "edge:tenant:42:tokens",
            "edge:tenant:42:policy",
            now,
            7
        ]))
        .unwrap();
        let response = engine.execute_rest(RestRequest::post("/EVALSHA", &body));
        assert_eq!(response.status, 200);
        response_json(response)
    }

    #[test]
    fn basic_mvp_commands_work_without_redis_core() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"41"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            engine.execute(&argv(&[b"INCR", b"k"])),
            RespFrame::integer(42)
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"k"]))),
            b"$2\r\n42\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"EXISTS", b"k"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"PEXPIRE", b"k", b"2500"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"TTL", b"k"])),
            RespFrame::integer(3)
        );

        engine.host_mut().set_now_millis(3_501);
        assert_eq!(
            engine.execute(&argv(&[b"GET", b"k"])),
            RespFrame::null_bulk()
        );
    }

    #[test]
    fn hash_commands_cover_policy_storage_shape() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        assert_eq!(
            engine.execute(&argv(&[
                b"HSET",
                b"policy",
                b"capacity",
                b"10",
                b"refill_tokens",
                b"5",
                b"refill_ms",
                b"1000",
                b"ttl_ms",
                b"60000"
            ])),
            RespFrame::integer(4)
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGET", b"policy", b"capacity"]))),
            b"$2\r\n10\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"HGET", b"policy", b"missing"])),
            RespFrame::null_bulk()
        );
        // HGETALL returns fields in INSERTION order (matching valkey's
        // listpack/hashtable ordering), i.e. the order the HSET supplied them:
        // capacity, refill_tokens, refill_ms, ttl_ms.
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGETALL", b"policy"]))),
            b"*8\r\n$8\r\ncapacity\r\n$2\r\n10\r\n$13\r\nrefill_tokens\r\n$1\r\n5\r\n$9\r\nrefill_ms\r\n$4\r\n1000\r\n$6\r\nttl_ms\r\n$5\r\n60000\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"HDEL", b"policy", b"ttl_ms", b"missing"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"PEXPIRE", b"policy", b"500"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            response_json(engine.execute_rest(RestRequest::get("/HGET/policy/refill_ms"))),
            json!({"result": "1000"})
        );

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"plain", b"value"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGET", b"plain", b"field"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"policy"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn snapshot_round_trip_preserves_strings_hashes_and_absolute_expiry() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"plain", b"value"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"HSET",
                b"policy",
                b"capacity",
                b"10",
                b"refill_ms",
                b"1000"
            ])),
            RespFrame::integer(2)
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"volatile", b"soon", b"PX", b"500"])),
            RespFrame::simple("OK")
        );
        // A hash with a per-field TTL: `capacity` carries a deadline, `refill_ms`
        // does not. Both must round-trip through the snapshot codec.
        assert_eq!(
            engine.execute(&argv(&[
                b"HPEXPIRE",
                b"policy",
                b"500",
                b"FIELDS",
                b"1",
                b"capacity"
            ])),
            RespFrame::array(vec![RespFrame::integer(1)])
        );

        let snapshot = engine.export_snapshot();
        let mut restored = Engine::new(NoopHost::new(1_200));
        restored.import_snapshot(&snapshot).unwrap();

        assert_eq!(
            resp2(&restored.execute(&argv(&[b"GET", b"plain"]))),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"HGET", b"policy", b"capacity"]))),
            b"$2\r\n10\r\n"
        );
        assert_eq!(
            restored.execute(&argv(&[b"PTTL", b"volatile"])),
            RespFrame::integer(300)
        );
        // The per-field deadline survived: `capacity` has ~300ms left at now=1200
        // (deadline 1500), `refill_ms` has no TTL.
        assert_eq!(
            restored.execute(&argv(&[b"HPTTL", b"policy", b"FIELDS", b"2", b"capacity", b"refill_ms"])),
            RespFrame::array(vec![RespFrame::integer(300), RespFrame::integer(-1)])
        );

        restored.host_mut().set_now_millis(1_500);
        assert_eq!(
            restored.execute(&argv(&[b"GET", b"volatile"])),
            RespFrame::null_bulk()
        );
        // The field expired at its absolute deadline (1500) after the clock moved.
        assert_eq!(
            restored.execute(&argv(&[b"HGET", b"policy", b"capacity"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"HGET", b"policy", b"refill_ms"]))),
            b"$4\r\n1000\r\n"
        );
    }

    #[test]
    fn snapshot_import_rejects_malformed_hex() {
        let mut engine = Engine::new_in_memory();
        let malformed = br#"{
            "format": "valdr-engine-snapshot",
            "version": 1,
            "keys": [{"key": "0", "type": "string", "value": "00"}]
        }"#;

        assert_eq!(
            engine.import_snapshot(malformed),
            Err(SnapshotError::InvalidHex)
        );
    }

    #[test]
    fn script_load_exists_flush_round_trip() {
        let mut engine = Engine::new_in_memory();
        let sha = load_token_bucket(&mut engine);

        let exists = engine.execute(&vec![
            b"SCRIPT".to_vec(),
            b"EXISTS".to_vec(),
            sha.clone(),
            b"ffffffffffffffffffffffffffffffffffffffff".to_vec(),
        ]);
        assert_eq!(resp2(&exists), b"*2\r\n:1\r\n:0\r\n");

        assert_eq!(
            engine.execute(&argv(&[b"SCRIPT", b"FLUSH"])),
            RespFrame::simple("OK")
        );
        let missing = engine.execute(&vec![b"EVALSHA".to_vec(), sha, b"0".to_vec()]);
        assert_eq!(missing, RespFrame::error("NOSCRIPT No matching script."));
    }

    #[test]
    fn evalsha_runs_stateful_token_bucket_fixture() {
        let mut engine = Engine::new_in_memory();
        let sha = load_token_bucket(&mut engine);

        assert_eq!(
            evalsha_token_bucket(&mut engine, &sha, b"1000"),
            b"*8\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:0\r\n"
        );
        assert_eq!(
            evalsha_token_bucket(&mut engine, &sha, b"1100"),
            b"*8\r\n$7\r\nallowed\r\n:0\r\n$9\r\nremaining\r\n:3\r\n$8\r\nreset_ms\r\n:2400\r\n$14\r\nretry_after_ms\r\n:700\r\n"
        );
        assert_eq!(
            evalsha_token_bucket(&mut engine, &sha, b"1800"),
            b"*8\r\n$7\r\nallowed\r\n:1\r\n$9\r\nremaining\r\n:0\r\n$8\r\nreset_ms\r\n:3800\r\n$14\r\nretry_after_ms\r\n:0\r\n"
        );
    }

    #[test]
    fn rest_path_post_body_and_resp2_modes_match_edge_adapter_shape() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        let set = engine.execute_rest(RestRequest::get("/SET/foo/bar"));
        assert_eq!(set.status, 200);
        assert_eq!(response_json(set), json!({"result": "OK"}));

        let get = engine.execute_rest(RestRequest::get("/GET/foo"));
        assert_eq!(get.status, 200);
        assert_eq!(response_json(get), json!({"result": "bar"}));

        let post = engine.execute_rest(RestRequest::post("/SET/post-key?PX=2500", b"post-value"));
        assert_eq!(post.status, 200);
        assert_eq!(response_json(post), json!({"result": "OK"}));
        assert_eq!(
            response_json(engine.execute_rest(RestRequest::get("/PTTL/post-key"))),
            json!({"result": 2500})
        );

        let resp2_get = engine.execute_rest(RestRequest {
            method: RestMethod::Get,
            path: "/GET/post-key",
            body: &[],
            response_format: RestResponseFormat::Resp2,
        });
        assert_eq!(resp2_get.status, 200);
        assert_eq!(resp2_get.content_type, APPLICATION_OCTET_STREAM);
        assert_eq!(resp2_get.body, b"$10\r\npost-value\r\n");
    }

    #[test]
    fn rest_pipeline_is_ordered_and_non_atomic() {
        let mut engine = Engine::new_in_memory();
        let body = br#"[
            ["SET", "key1", "1"],
            ["INCR", "key1"],
            ["GET", "key1"],
            ["NOPE"]
        ]"#;

        let response = engine.execute_rest(RestRequest::post("/pipeline", body));

        assert_eq!(response.status, 200);
        assert_eq!(
            response_json(response),
            json!([
                {"result": "OK"},
                {"result": 2},
                {"result": "2"},
                {"error": "ERR unknown command 'NOPE', with args beginning with: "}
            ])
        );
    }

    #[test]
    fn rest_adapter_runs_token_bucket_fixture() {
        let mut engine = Engine::new_in_memory();
        let sha = rest_load_token_bucket(&mut engine);

        assert_eq!(
            rest_evalsha_token_bucket(&mut engine, &sha, "1000"),
            json!({"result": ["allowed", 1, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 0]})
        );
        assert_eq!(
            rest_evalsha_token_bucket(&mut engine, &sha, "1100"),
            json!({"result": ["allowed", 0, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 700]})
        );
        assert_eq!(
            rest_evalsha_token_bucket(&mut engine, &sha, "1800"),
            json!({"result": ["allowed", 1, "remaining", 0, "reset_ms", 3800, "retry_after_ms", 0]})
        );
    }

    #[test]
    fn rest_adapter_runs_hash_policy_token_bucket_fixture() {
        let mut engine = Engine::new_in_memory();
        let policy = engine.execute_rest(RestRequest::post(
            "/HSET/edge%3Atenant%3A42%3Apolicy",
            b"[\"HSET\",\"edge:tenant:42:policy\",\"capacity\",\"10\",\"refill_tokens\",\"5\",\"refill_ms\",\"1000\",\"ttl_ms\",\"60000\"]",
        ));
        assert_eq!(response_json(policy), json!({"result": 4}));

        let script = String::from_utf8_lossy(HASH_POLICY_TOKEN_BUCKET_SCRIPT);
        let body = serde_json::to_vec(&json!(["SCRIPT", "LOAD", script])).unwrap();
        let response = engine.execute_rest(RestRequest::post("/", &body));
        assert_eq!(response.status, 200);
        let value = response_json(response);
        let sha = value["result"].as_str().unwrap().as_bytes().to_vec();

        assert_eq!(
            rest_evalsha_hash_policy_bucket(&mut engine, &sha, "1000"),
            json!({"result": ["allowed", 1, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 0, "capacity", 10]})
        );
        assert_eq!(
            rest_evalsha_hash_policy_bucket(&mut engine, &sha, "1100"),
            json!({"result": ["allowed", 0, "remaining", 3, "reset_ms", 2400, "retry_after_ms", 700, "capacity", 10]})
        );

        let upgrade = engine.execute_rest(RestRequest::get(
            "/HSET/edge%3Atenant%3A42%3Apolicy/capacity/20",
        ));
        assert_eq!(response_json(upgrade), json!({"result": 0}));
        assert_eq!(
            rest_evalsha_hash_policy_bucket(&mut engine, &sha, "1800"),
            json!({"result": ["allowed", 1, "remaining", 0, "reset_ms", 5800, "retry_after_ms", 0, "capacity", 20]})
        );
    }

    #[test]
    fn sha1_hex_known_vectors() {
        assert_eq!(&sha1_hex(b""), b"da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            &sha1_hex(b"abc"),
            b"a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }

    #[test]
    fn set_options_nx_xx_get_follow_reference_semantics() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"v1", b"NX"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"v2", b"NX"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"k"]))),
            b"$2\r\nv1\r\n"
        );

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"missing", b"v", b"XX"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            engine.execute(&argv(&[b"EXISTS", b"missing"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"v2", b"XX"])),
            RespFrame::simple("OK")
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SET", b"k", b"v3", b"GET"]))),
            b"$2\r\nv2\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"fresh", b"v1", b"GET"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SET", b"k", b"v9", b"NX", b"GET"]))),
            b"$2\r\nv3\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"k"]))),
            b"$2\r\nv3\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"missing", b"v", b"XX", b"GET"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            engine.execute(&argv(&[b"EXISTS", b"missing"])),
            RespFrame::integer(0)
        );

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"v", b"NX", b"XX"])),
            RespFrame::error("ERR syntax error")
        );
        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k", b"v", b"EX", b"10", b"PX", b"10000"])),
            RespFrame::error("ERR syntax error")
        );

        assert_eq!(
            engine.execute(&argv(&[b"SET", b"k2", b"v", b"NX", b"PX", b"2500"])),
            RespFrame::simple("OK")
        );
        assert_eq!(
            engine.execute(&argv(&[b"PTTL", b"k2"])),
            RespFrame::integer(2500)
        );

        engine.execute(&argv(&[b"HSET", b"h", b"f", b"v"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SET", b"h", b"v", b"GET"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGET", b"h", b"f"]))),
            b"$1\r\nv\r\n"
        );
    }

    #[test]
    fn zset_commands_cover_leaderboard_shape() {
        let mut engine = Engine::new(NoopHost::new(1_000));

        assert_eq!(
            engine.execute(&argv(&[
                b"ZADD", b"board", b"1", b"a", b"2", b"b", b"2", b"bb", b"3", b"c"
            ])),
            RespFrame::integer(4)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZCARD", b"board"])),
            RespFrame::integer(4)
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZSCORE", b"board", b"b"]))),
            b"$1\r\n2\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZSCORE", b"board", b"nope"])),
            RespFrame::null_bulk()
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"5", b"a"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"CH", b"6", b"a", b"4", b"d"])),
            RespFrame::integer(2)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"NX", b"9", b"a"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZSCORE", b"board", b"a"]))),
            b"$1\r\n6\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"XX", b"8", b"nope2"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZSCORE", b"board", b"nope2"])),
            RespFrame::null_bulk()
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"NX", b"XX", b"1", b"m"])),
            RespFrame::error("ERR XX and NX options at the same time are not compatible")
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"oops", b"m"])),
            RespFrame::error("ERR value is not a valid float")
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZADD", b"board", b"1", b"m", b"2"])),
            RespFrame::error("ERR syntax error")
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZRANK", b"board", b"b"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANK", b"board", b"bb"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZREVRANK", b"board", b"a"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZREVRANK", b"board", b"b"])),
            RespFrame::integer(4)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANK", b"board", b"zzz"])),
            RespFrame::null_bulk()
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGE", b"board", b"0", b"2"]))),
            b"*3\r\n$1\r\nb\r\n$2\r\nbb\r\n$1\r\nc\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGE", b"board", b"-2", b"-1", b"WITHSCORES"]))),
            b"*4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\na\r\n$1\r\n6\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGE", b"board", b"0", b"1", b"REV"]))),
            b"*2\r\n$1\r\na\r\n$1\r\nd\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGE", b"board", b"5", b"9"])),
            RespFrame::array(Vec::new())
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGE", b"board", b"0", b"-1", b"BOGUS"])),
            RespFrame::error("ERR syntax error")
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZINCRBY", b"board", b"1.5", b"b"]))),
            b"$3\r\n3.5\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZINCRBY", b"board", b"oops", b"b"])),
            RespFrame::error("ERR value is not a valid float")
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZINCRBY", b"znan", b"inf", b"m"]))),
            b"$3\r\ninf\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZINCRBY", b"znan", b"-inf", b"m"])),
            RespFrame::error("ERR resulting score is not a number (NaN)")
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZINCRBY", b"zneg", b"-0.0", b"m"]))),
            b"$2\r\n-0\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZSCORE", b"zneg", b"m"]))),
            b"$1\r\n0\r\n"
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZREM", b"board", b"a", b"missing"])),
            RespFrame::integer(1)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZREM", b"board", b"b", b"bb", b"c", b"d"])),
            RespFrame::integer(4)
        );
        assert_eq!(
            engine.execute(&argv(&[b"EXISTS", b"board"])),
            RespFrame::integer(0)
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZREM", b"board", b"a"])),
            RespFrame::integer(0)
        );

        engine.execute(&argv(&[b"SET", b"s", b"v"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZADD", b"s", b"1", b"m"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"znan"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn zrangebyscore_matches_reference_ranges_options_and_errors() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[
            b"ZADD", b"z", b"1", b"a", b"2", b"b", b"2", b"bb", b"3", b"c", b"4", b"d",
        ]));
        let epoch = engine.mutation_epoch();

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"2", b"3"]))),
            b"*3\r\n$1\r\nb\r\n$2\r\nbb\r\n$1\r\nc\r\n"
        );
        assert_eq!(engine.mutation_epoch(), epoch, "ZRANGEBYSCORE is read-only");

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"(2", b"3"]))),
            b"*1\r\n$1\r\nc\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"2", b"(3"]))),
            b"*2\r\n$1\r\nb\r\n$2\r\nbb\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"WITHSCORES"
            ]))),
            b"*10\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$2\r\nbb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$1\r\n4\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"-inf", b"inf"]))),
            b"*5\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\nbb\r\n$1\r\nc\r\n$1\r\nd\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"1",
                b"2"
            ]))),
            b"*2\r\n$1\r\nb\r\n$2\r\nbb\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"1",
                b"-1"
            ]))),
            b"*4\r\n$1\r\nb\r\n$2\r\nbb\r\n$1\r\nc\r\n$1\r\nd\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"3",
                b"100"
            ]))),
            b"*2\r\n$1\r\nc\r\n$1\r\nd\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"100",
                b"5"
            ])),
            RespFrame::array(Vec::new())
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"-1",
                b"2"
            ])),
            RespFrame::array(Vec::new())
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"100", b"200"])),
            RespFrame::array(Vec::new())
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"nokey", b"0", b"10"])),
            RespFrame::array(Vec::new())
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"foo", b"10"])),
            RespFrame::error("ERR min or max is not a float")
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"(foo", b"10"])),
            RespFrame::error("ERR min or max is not a float")
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"nan", b"10"])),
            RespFrame::error("ERR min or max is not a float")
        );

        assert_eq!(
            engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"x",
                b"2"
            ])),
            RespFrame::error("ERR value is not an integer or out of range")
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"LIMIT",
                b"1"
            ])),
            RespFrame::error("ERR syntax error")
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"-inf", b"+inf", b"BOGUS"])),
            RespFrame::error("ERR syntax error")
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"foo", b"10", b"BOGUS"])),
            RespFrame::error("ERR syntax error"),
            "option parse precedes bound parse"
        );
        assert_eq!(
            engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"foo",
                b"10",
                b"LIMIT",
                b"x",
                b"y"
            ])),
            RespFrame::error("ERR value is not an integer or out of range"),
            "LIMIT integer error precedes bound parse"
        );

        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"0"])),
            RespFrame::error("ERR wrong number of arguments for 'zrangebyscore' command")
        );

        engine.execute(&argv(&[b"SET", b"zstr", b"v"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"zstr", b"0", b"10"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn zrangebyscore_handles_lex_tiebreak_inf_members_and_empty_bound() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[
            b"ZADD", b"z", b"1.5", b"a", b"-2.5", b"neg", b"0", b"zero", b"3.0e15", b"big", b"0.1",
            b"tenth", b"inf", b"top", b"-inf", b"bottom",
        ]));

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"+inf",
                b"WITHSCORES"
            ]))),
            b"*14\r\n$6\r\nbottom\r\n$4\r\n-inf\r\n$3\r\nneg\r\n$4\r\n-2.5\r\n$4\r\nzero\r\n$1\r\n0\r\n$5\r\ntenth\r\n$3\r\n0.1\r\n$1\r\na\r\n$3\r\n1.5\r\n$3\r\nbig\r\n$16\r\n3000000000000000\r\n$3\r\ntop\r\n$3\r\ninf\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"100",
                b"(inf",
                b"WITHSCORES"
            ]))),
            b"*2\r\n$3\r\nbig\r\n$16\r\n3000000000000000\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"(-inf", b"-100"])),
            RespFrame::array(Vec::new())
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"ZRANGEBYSCORE",
                b"z",
                b"-inf",
                b"-100",
                b"WITHSCORES"
            ]))),
            b"*2\r\n$6\r\nbottom\r\n$4\r\n-inf\r\n"
        );

        engine.execute(&argv(&[
            b"ZADD", b"lex", b"5", b"banana", b"5", b"apple", b"5", b"cherry",
        ]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"lex", b"5", b"5"]))),
            b"*3\r\n$5\r\napple\r\n$6\r\nbanana\r\n$6\r\ncherry\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZRANGEBYSCORE", b"z", b"", b"0.1"]))),
            b"*2\r\n$4\r\nzero\r\n$5\r\ntenth\r\n",
            "an empty bound body parses as 0.0, inclusive"
        );
    }

    #[test]
    fn zset_score_replies_match_reference_formatting() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"0", b"0"),
            (b"1", b"1"),
            (b"-1", b"-1"),
            (b"1.5", b"1.5"),
            (b"3.0e15", b"3000000000000000"),
            (b"0.1", b"0.1"),
            (b"-0", b"0"),
            (b"-0.0", b"0"),
            (b"0.001", b"0.001"),
            (b"100.25", b"100.25"),
            (b"1e300", b"1e+300"),
            (b"1e-9", b"1e-9"),
            (b"inf", b"inf"),
            (b"-Infinity", b"-inf"),
        ];
        let mut engine = Engine::new_in_memory();
        for (index, (input, expected)) in cases.iter().enumerate() {
            let member = format!("m{index}").into_bytes();
            engine.execute(&vec![
                b"ZADD".to_vec(),
                b"fmt".to_vec(),
                input.to_vec(),
                member.clone(),
            ]);
            let reply = engine.execute(&vec![b"ZSCORE".to_vec(), b"fmt".to_vec(), member]);
            match reply {
                RespFrame::Bulk(Some(value)) => assert_eq!(
                    value.as_bytes(),
                    *expected,
                    "score input {:?}",
                    String::from_utf8_lossy(input)
                ),
                other => panic!("unexpected ZSCORE reply for {input:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn snapshot_round_trip_preserves_zsets() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[
            b"ZADD",
            b"board",
            b"1.5",
            b"alice",
            b"0.1",
            b"bob",
            b"3000000000000000",
            b"carol",
            b"-1",
            b"dave",
        ]));
        engine.execute(&argv(&[
            b"ZADD", b"edges", b"inf", b"top", b"-inf", b"bottom",
        ]));

        let snapshot = engine.export_snapshot();
        let mut restored = Engine::new(NoopHost::new(1_000));
        restored.import_snapshot(&snapshot).unwrap();

        for key in [&b"board"[..], b"edges"] {
            assert_eq!(
                resp2(&engine.execute(&argv(&[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"]))),
                resp2(&restored.execute(&argv(&[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"])))
            );
        }
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"ZSCORE", b"board", b"bob"]))),
            b"$3\r\n0.1\r\n"
        );
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"ZSCORE", b"edges", b"top"]))),
            b"$3\r\ninf\r\n"
        );
    }

    #[test]
    fn snapshot_round_trip_preserves_list_order() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[b"RPUSH", b"queue", b"a", b"b", b"c", b"d"]));
        engine.execute(&argv(&[b"LPUSH", b"stack", b"x", b"y", b"z"]));

        let snapshot = engine.export_snapshot();
        let mut restored = Engine::new(NoopHost::new(1_000));
        restored.import_snapshot(&snapshot).unwrap();

        for key in [&b"queue"[..], b"stack"] {
            assert_eq!(
                resp2(&engine.execute(&argv(&[b"LRANGE", key, b"0", b"-1"]))),
                resp2(&restored.execute(&argv(&[b"LRANGE", key, b"0", b"-1"])))
            );
        }
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"LRANGE", b"stack", b"0", b"-1"]))),
            b"*3\r\n$1\r\nz\r\n$1\r\ny\r\n$1\r\nx\r\n"
        );
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"TYPE", b"queue"]))),
            b"+list\r\n"
        );
    }

    #[test]
    fn snapshot_round_trip_preserves_set() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[b"SADD", b"tags", b"a", b"b", b"c", b"d"]));
        engine.execute(&argv(&[b"SADD", b"solo", b"only"]));

        let snapshot = engine.export_snapshot();
        let mut restored = Engine::new(NoopHost::new(1_000));
        restored.import_snapshot(&snapshot).unwrap();

        assert_eq!(
            resp2(&restored.execute(&argv(&[b"SCARD", b"tags"]))),
            b":4\r\n"
        );
        for member in [&b"a"[..], b"b", b"c", b"d"] {
            assert_eq!(
                resp2(&restored.execute(&argv(&[b"SISMEMBER", b"tags", member]))),
                b":1\r\n",
                "member {member:?} should survive the snapshot round-trip"
            );
        }
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"SISMEMBER", b"tags", b"missing"]))),
            b":0\r\n"
        );
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"TYPE", b"tags"]))),
            b"+set\r\n"
        );
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"SISMEMBER", b"solo", b"only"]))),
            b":1\r\n"
        );
    }

    #[test]
    fn snapshot_round_trip_preserves_stream() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[b"XADD", b"s", b"1-1", b"f1", b"v1"]));
        engine.execute(&argv(&[b"XADD", b"s", b"1-2", b"f2", b"v2", b"f3", b"v3"]));
        engine.execute(&argv(&[b"XADD", b"s", b"2-1", b"a", b"b"]));
        engine.execute(&argv(&[b"XDEL", b"s", b"1-1"]));
        engine.execute(&argv(&[b"XSETID", b"s", b"5-5"]));

        let snapshot = engine.export_snapshot();
        let mut restored = Engine::new(NoopHost::new(1_000));
        restored.import_snapshot(&snapshot).unwrap();

        for cmd in [
            &[b"XLEN".as_ref(), b"s"][..],
            &[b"XRANGE".as_ref(), b"s", b"-", b"+"][..],
            &[b"XREVRANGE".as_ref(), b"s", b"+", b"-"][..],
            &[b"TYPE".as_ref(), b"s"][..],
        ] {
            assert_eq!(
                resp2(&engine.execute(&argv(cmd))),
                resp2(&restored.execute(&argv(cmd))),
                "stream round-trip mismatch for {cmd:?}"
            );
        }
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"TYPE", b"s"]))),
            b"+stream\r\n"
        );
        assert_eq!(resp2(&restored.execute(&argv(&[b"XLEN", b"s"]))), b":2\r\n");
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"XADD", b"s", b"6-1", b"x", b"y"]))),
            b"$3\r\n6-1\r\n",
            "last_id (5-5 via XSETID) must survive so 6-1 is accepted"
        );
    }

    #[test]
    fn snapshot_round_trip_preserves_stream_groups_and_pel() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[b"XADD", b"s", b"1-1", b"a", b"1"]));
        engine.execute(&argv(&[b"XADD", b"s", b"2-2", b"b", b"2"]));
        engine.execute(&argv(&[b"XADD", b"s", b"3-3", b"c", b"3"]));
        engine.execute(&argv(&[b"XGROUP", b"CREATE", b"s", b"g1", b"0"]));
        engine.execute(&argv(&[
            b"XREADGROUP", b"GROUP", b"g1", b"alice", b"COUNT", b"2", b"STREAMS", b"s", b">",
        ]));
        engine.execute(&argv(&[b"XACK", b"s", b"g1", b"1-1"]));

        let snapshot = engine.export_snapshot();
        let mut restored = Engine::new(NoopHost::new(1_000));
        restored.import_snapshot(&snapshot).unwrap();

        for cmd in [
            &[b"XPENDING".as_ref(), b"s", b"g1"][..],
            &[b"XINFO".as_ref(), b"GROUPS", b"s"][..],
            &[b"XINFO".as_ref(), b"STREAM", b"s"][..],
            &[b"XLEN".as_ref(), b"s"][..],
        ] {
            assert_eq!(
                resp2(&engine.execute(&argv(cmd))),
                resp2(&restored.execute(&argv(cmd))),
                "stream-group round-trip mismatch for {cmd:?}"
            );
        }
        assert_eq!(
            resp2(&restored.execute(&argv(&[b"XPENDING", b"s", b"g1"]))),
            b"*4\r\n:1\r\n$3\r\n2-2\r\n$3\r\n2-2\r\n*1\r\n*2\r\n$5\r\nalice\r\n$1\r\n1\r\n",
            "restored group PEL must contain only 2-2 owned by alice after ack of 1-1"
        );
        assert_eq!(
            resp2(&restored.execute(&argv(&[
                b"XREADGROUP", b"GROUP", b"g1", b"alice", b"STREAMS", b"s", b">",
            ]))),
            b"*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n3-3\r\n*2\r\n$1\r\nc\r\n$1\r\n3\r\n",
            "restored last_delivered_id (2-2) must survive so > delivers only 3-3"
        );
    }

    #[test]
    fn mutation_epoch_zset_and_set_options_contract() {
        let mut engine = Engine::new_in_memory();
        let start = engine.mutation_epoch();

        engine.execute(&argv(&[b"ZADD", b"z", b"1", b"a"]));
        let after_zadd = engine.mutation_epoch();
        assert_ne!(after_zadd, start);

        engine.execute(&argv(&[b"ZSCORE", b"z", b"a"]));
        engine.execute(&argv(&[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"]));
        engine.execute(&argv(&[b"ZRANK", b"z", b"a"]));
        engine.execute(&argv(&[b"ZREVRANK", b"z", b"a"]));
        engine.execute(&argv(&[b"ZCARD", b"z"]));
        assert_eq!(engine.mutation_epoch(), after_zadd);

        engine.execute(&argv(&[b"ZADD", b"z", b"1", b"a"]));
        assert_eq!(engine.mutation_epoch(), after_zadd);
        engine.execute(&argv(&[b"ZADD", b"z", b"XX", b"5", b"missing"]));
        assert_eq!(engine.mutation_epoch(), after_zadd);

        engine.execute(&argv(&[b"ZINCRBY", b"z", b"1", b"a"]));
        let after_zincrby = engine.mutation_epoch();
        assert_ne!(after_zincrby, after_zadd);

        engine.execute(&argv(&[b"ZREM", b"z", b"missing"]));
        assert_eq!(engine.mutation_epoch(), after_zincrby);
        engine.execute(&argv(&[b"ZREM", b"z", b"a"]));
        let after_zrem = engine.mutation_epoch();
        assert_ne!(after_zrem, after_zincrby);

        engine.execute(&argv(&[b"SET", b"k", b"v"]));
        let after_set = engine.mutation_epoch();
        engine.execute(&argv(&[b"SET", b"k", b"v2", b"NX"]));
        assert_eq!(engine.mutation_epoch(), after_set);
        engine.execute(&argv(&[b"SET", b"other", b"v", b"XX"]));
        assert_eq!(engine.mutation_epoch(), after_set);
        engine.execute(&argv(&[b"SET", b"k", b"v2", b"XX", b"GET"]));
        assert_ne!(engine.mutation_epoch(), after_set);
    }

    #[test]
    fn eval_registers_script_in_cache_for_evalsha() {
        let mut engine = Engine::new_in_memory();
        assert_eq!(
            engine.execute(&argv(&[b"EVAL", b"return 2", b"0"])),
            RespFrame::integer(2)
        );
        let sha = sha1_hex(b"return 2");
        let exists = engine.execute(&vec![b"SCRIPT".to_vec(), b"EXISTS".to_vec(), sha.to_vec()]);
        assert_eq!(resp2(&exists), b"*1\r\n:1\r\n");
        assert_eq!(
            engine.execute(&vec![b"EVALSHA".to_vec(), sha.to_vec(), b"0".to_vec()]),
            RespFrame::integer(2)
        );
    }

    #[test]
    fn eval_uncaught_script_errors_return_resp_errors() {
        let mut engine = Engine::new_in_memory();

        let reply = engine.execute(&argv(&[b"EVAL", b"error('boom')", b"0"]));
        match &reply {
            RespFrame::Error(message) => {
                let text = String::from_utf8_lossy(message.as_bytes()).into_owned();
                assert!(text.starts_with("ERR "), "unexpected error shape: {text}");
                assert!(text.contains("boom"), "error message lost: {text}");
            }
            other => panic!("expected error frame, got {other:?}"),
        }

        engine.execute(&argv(&[b"SET", b"s", b"v"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL",
                b"return redis.call('INCR', KEYS[1])",
                b"1",
                b"s"
            ]))),
            b"-ERR value is not an integer or out of range\r\n"
        );

        engine.execute(&argv(&[b"HSET", b"h", b"f", b"v"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL",
                b"return redis.call('GET', KEYS[1])",
                b"1",
                b"h"
            ]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL",
                b"return redis.call('SET', KEYS[1], true)",
                b"1",
                b"b"
            ]))),
            b"-ERR Command arguments must be strings or integers\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL",
                b"return type(redis.call('GET', KEYS[1]))",
                b"1",
                b"missing"
            ]))),
            b"$7\r\nboolean\r\n"
        );
    }

    #[test]
    fn eval_ro_rejects_writes_but_allows_reads() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SET", b"ro:k", b"hello"]));

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL_RO",
                b"return redis.call('get', KEYS[1])",
                b"1",
                b"ro:k"
            ]))),
            b"$5\r\nhello\r\n",
            "read-only script that only reads matches EVAL"
        );

        let epoch_before = engine.mutation_epoch();
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL_RO",
                b"return redis.call('set', KEYS[1], 'v')",
                b"1",
                b"ro:k"
            ]))),
            b"-ERR Write commands are not allowed from read-only scripts.\r\n",
            "uncaught write via redis.call surfaces valkey's exact read-only error"
        );
        assert_eq!(
            engine.mutation_epoch(),
            epoch_before,
            "rejected write must not mutate state"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"ro:k"]))),
            b"$5\r\nhello\r\n",
            "value is unchanged after the rejected write"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL_RO",
                b"local r = redis.pcall('set', KEYS[1], 'v') return r.err",
                b"1",
                b"ro:k"
            ]))),
            b"$58\r\nERR Write commands are not allowed from read-only scripts.\r\n",
            "pcall returns the exact error string (with code) in its err field, matching valkey on the wire"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL",
                b"return redis.call('set', KEYS[1], 'world')",
                b"1",
                b"ro:k"
            ]))),
            b"+OK\r\n",
            "ordinary EVAL after EVAL_RO is never gated"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"ro:k"]))),
            b"$5\r\nworld\r\n"
        );
    }

    #[test]
    fn evalsha_ro_gates_writes_via_static_command_flag() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"RPUSH", b"ro:lst", b"c", b"a", b"b"]));

        let load = engine.execute(&argv(&[
            b"SCRIPT",
            b"LOAD",
            b"return redis.call('sort', KEYS[1], 'ALPHA')",
        ]));
        let sha = match &load {
            RespFrame::Bulk(Some(bytes)) => bytes.as_bytes().to_vec(),
            other => panic!("expected sha bulk, got {other:?}"),
        };

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVALSHA_RO",
                sha.as_slice(),
                b"1",
                b"ro:lst"
            ]))),
            b"-ERR Write commands are not allowed from read-only scripts.\r\n",
            "SORT carries the WRITE flag, so it is rejected even without STORE"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"EVAL_RO",
                b"return redis.call('sort_ro', KEYS[1], 'ALPHA')",
                b"1",
                b"ro:lst"
            ]))),
            b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
            "SORT_RO is a read-only sibling and is allowed"
        );
    }

    #[test]
    fn delifeq_deletes_only_on_exact_string_match() {
        let mut engine = Engine::new_in_memory();

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"DELIFEQ", b"de:missing", b"x"]))),
            b":0\r\n",
            "missing key replies :0"
        );

        engine.execute(&argv(&[b"SET", b"de:k", b"val1"]));
        let epoch_mismatch = engine.mutation_epoch();
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"DELIFEQ", b"de:k", b"other"]))),
            b":0\r\n",
            "value mismatch keeps the key and replies :0"
        );
        assert_eq!(
            engine.mutation_epoch(),
            epoch_mismatch,
            "a non-deleting DELIFEQ is a read: no epoch bump"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"de:k"]))),
            b"$4\r\nval1\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"DELIFEQ", b"de:k", b"val1"]))),
            b":1\r\n",
            "exact match deletes and replies :1"
        );
        assert_eq!(resp2(&engine.execute(&argv(&[b"EXISTS", b"de:k"]))), b":0\r\n");

        engine.execute(&argv(&[b"RPUSH", b"de:lst", b"x"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"DELIFEQ", b"de:lst", b"x"]))),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            "a non-string key replies WRONGTYPE"
        );
    }

    #[test]
    fn msetex_sets_all_keys_with_optional_expiry_and_conditions() {
        let mut engine = Engine::new_in_memory();

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"MSETEX", b"2", b"mx:a", b"1", b"mx:b", b"2"
            ]))),
            b":1\r\n"
        );
        assert_eq!(resp2(&engine.execute(&argv(&[b"GET", b"mx:a"]))), b"$1\r\n1\r\n");
        assert_eq!(resp2(&engine.execute(&argv(&[b"GET", b"mx:b"]))), b"$1\r\n2\r\n");
        assert_eq!(resp2(&engine.execute(&argv(&[b"TTL", b"mx:a"]))), b":-1\r\n");

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"MSETEX",
                b"1",
                b"mx:c",
                b"cv",
                b"PXAT",
                b"99999999999999"
            ]))),
            b":1\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"PEXPIRETIME", b"mx:c"]))),
            b":99999999999999\r\n",
            "absolute PXAT is stored verbatim"
        );

        engine.execute(&argv(&[b"SET", b"mx:nx", b"old"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"MSETEX", b"1", b"mx:nx", b"new", b"NX"
            ]))),
            b":0\r\n",
            "NX on an existing key rejects the whole batch"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"mx:nx"]))),
            b"$3\r\nold\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"MSETEX", b"1", b"mx:nx", b"xv", b"XX"
            ]))),
            b":1\r\n",
            "XX on an existing key applies"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"mx:nx"]))),
            b"$2\r\nxv\r\n"
        );

        assert_eq!(
            resp2(&engine.execute(&argv(&[b"MSETEX", b"0", b"x", b"y"]))),
            b"-ERR invalid numkeys value or out of range\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"MSETEX", b"2", b"mx:a", b"1"]))),
            b"-ERR syntax error\r\n",
            "too few key/value pairs is a syntax error"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"MSETEX", b"1", b"mx:a", b"1", b"EX", b"0"
            ]))),
            b"-ERR invalid expire time in 'msetex' command\r\n"
        );
    }

    #[test]
    fn multi_exec_runs_queued_commands_in_order() {
        let mut engine = Engine::new_in_memory();
        assert_eq!(resp2(&engine.execute(&argv(&[b"MULTI"]))), b"+OK\r\n");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SET", b"mx:k", b"1"]))),
            b"+QUEUED\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"INCR", b"mx:k"]))),
            b"+QUEUED\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"EXEC"]))),
            b"*2\r\n+OK\r\n:2\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"mx:k"]))),
            b"$1\r\n2\r\n"
        );
    }

    #[test]
    fn exec_without_multi_and_discard_without_multi_error() {
        let mut engine = Engine::new_in_memory();
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"EXEC"]))),
            b"-ERR EXEC without MULTI\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"DISCARD"]))),
            b"-ERR DISCARD without MULTI\r\n"
        );
    }

    #[test]
    fn nested_multi_aborts_the_transaction() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"MULTI"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"MULTI"]))),
            b"-ERR Command 'multi' not allowed inside a transaction\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"EXEC"]))),
            b"-EXECABORT Transaction discarded because of previous errors.\r\n"
        );
    }

    #[test]
    fn discard_rolls_back_the_queue() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"MULTI"]));
        engine.execute(&argv(&[b"SET", b"mx:d", b"9"]));
        assert_eq!(resp2(&engine.execute(&argv(&[b"DISCARD"]))), b"+OK\r\n");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"mx:d"]))),
            b"$-1\r\n"
        );
    }

    #[test]
    fn queue_time_error_makes_exec_abort_and_applies_nothing() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"MULTI"]));
        assert!(resp2(&engine.execute(&argv(&[b"NOTACOMMAND", b"a", b"b"])))
            .starts_with(b"-ERR unknown command 'NOTACOMMAND'"));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SET", b"mx:q", b"1"]))),
            b"+QUEUED\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"EXEC"]))),
            b"-EXECABORT Transaction discarded because of previous errors.\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"mx:q"]))),
            b"$-1\r\n"
        );
    }

    #[test]
    fn queue_time_arity_error_aborts() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"MULTI"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET"]))),
            b"-ERR wrong number of arguments for 'get' command\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"EXEC"]))),
            b"-EXECABORT Transaction discarded because of previous errors.\r\n"
        );
    }

    #[test]
    fn runtime_error_inside_exec_does_not_abort_the_batch() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"MULTI"]));
        engine.execute(&argv(&[b"SET", b"mx:r", b"v"]));
        engine.execute(&argv(&[b"INCR", b"mx:r"]));
        engine.execute(&argv(&[b"LPUSH", b"mx:r", b"x"]));
        let frame = resp2(&engine.execute(&argv(&[b"EXEC"])));
        assert_eq!(
            frame,
            b"*3\r\n+OK\r\n-ERR value is not an integer or out of range\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"mx:r"]))),
            b"$1\r\nv\r\n"
        );
    }

    #[test]
    fn watch_cas_returns_null_array_when_a_watched_key_changes() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SET", b"mx:w", b"1"]));
        engine.execute(&argv(&[b"WATCH", b"mx:w"]));
        engine.execute(&argv(&[b"SET", b"mx:w", b"2"]));
        engine.execute(&argv(&[b"MULTI"]));
        engine.execute(&argv(&[b"GET", b"mx:w"]));
        assert_eq!(resp2(&engine.execute(&argv(&[b"EXEC"]))), b"*-1\r\n");
    }

    #[test]
    fn watch_happy_path_runs_when_no_watched_key_changes() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SET", b"mx:w", b"1"]));
        engine.execute(&argv(&[b"WATCH", b"mx:w"]));
        engine.execute(&argv(&[b"MULTI"]));
        engine.execute(&argv(&[b"GET", b"mx:w"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"EXEC"]))),
            b"*1\r\n$1\r\n1\r\n"
        );
    }

    #[test]
    fn unwatch_clears_dirty_cas() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SET", b"mx:w", b"1"]));
        engine.execute(&argv(&[b"WATCH", b"mx:w"]));
        engine.execute(&argv(&[b"SET", b"mx:w", b"2"]));
        assert_eq!(resp2(&engine.execute(&argv(&[b"UNWATCH"]))), b"+OK\r\n");
        engine.execute(&argv(&[b"MULTI"]));
        engine.execute(&argv(&[b"GET", b"mx:w"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"EXEC"]))),
            b"*1\r\n$1\r\n2\r\n"
        );
    }

    #[test]
    fn transaction_state_does_not_leak_into_snapshots_or_epoch() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SET", b"mx:s", b"v"]));
        let _ = engine.take_dirty();
        let snapshot_before = engine.export_snapshot();
        let epoch_before = engine.mutation_epoch();

        engine.execute(&argv(&[b"WATCH", b"mx:s"]));
        engine.execute(&argv(&[b"MULTI"]));
        engine.execute(&argv(&[b"GET", b"mx:s"]));
        engine.execute(&argv(&[b"EXEC"]));

        assert_eq!(engine.export_snapshot(), snapshot_before);
        assert_eq!(engine.mutation_epoch(), epoch_before);
        assert!(engine.take_dirty().is_empty());
    }

    #[test]
    fn script_internal_calls_run_atomically_when_a_queued_eval_executes() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"MULTI"]));
        let reply = resp2(&engine.execute(&argv(&[
            b"EVAL",
            b"redis.call('SET', KEYS[1], '1'); return redis.call('INCR', KEYS[1])",
            b"1",
            b"mx:script",
        ])));
        assert_eq!(reply, b"+QUEUED\r\n");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"EXEC"]))),
            b"*1\r\n:2\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"GET", b"mx:script"]))),
            b"$1\r\n2\r\n"
        );
    }

    fn pfcount(engine: &mut Engine<NoopHost>, key: &[u8]) -> i64 {
        match engine.execute(&argv(&[b"PFCOUNT", key])) {
            RespFrame::Integer(n) => n,
            other => panic!("expected integer, got {:?}", other),
        }
    }

    #[test]
    fn pfadd_creates_key_and_reports_changes() {
        let mut engine = Engine::new_in_memory();
        // New key with elements -> :1.
        assert_eq!(resp2(&engine.execute(&argv(&[b"PFADD", b"pf:a", b"x", b"y", b"z"]))), b":1\r\n");
        // Adding an already-seen element -> :0 (no register changed).
        assert_eq!(resp2(&engine.execute(&argv(&[b"PFADD", b"pf:a", b"x"]))), b":0\r\n");
        // Small cardinalities are exact.
        assert_eq!(pfcount(&mut engine, b"pf:a"), 3);
    }

    #[test]
    fn pfadd_empty_creates_empty_hll() {
        let mut engine = Engine::new_in_memory();
        assert_eq!(resp2(&engine.execute(&argv(&[b"PFADD", b"pf:e"]))), b":1\r\n");
        assert_eq!(pfcount(&mut engine, b"pf:e"), 0);
        // Re-running bare PFADD on an existing empty HLL changes nothing.
        assert_eq!(resp2(&engine.execute(&argv(&[b"PFADD", b"pf:e"]))), b":0\r\n");
    }

    #[test]
    fn pfcount_missing_key_is_zero() {
        let mut engine = Engine::new_in_memory();
        assert_eq!(pfcount(&mut engine, b"pf:missing"), 0);
    }

    #[test]
    fn pfcount_union_of_multiple_keys() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"PFADD", b"pf:u1", b"a", b"b", b"c"]));
        engine.execute(&argv(&[b"PFADD", b"pf:u2", b"c", b"d", b"e"]));
        // Union {a,b,c,d,e} = 5; PFCOUNT must not mutate either key.
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"PFCOUNT", b"pf:u1", b"pf:u2"]))),
            b":5\r\n"
        );
        assert_eq!(pfcount(&mut engine, b"pf:u1"), 3);
        assert_eq!(pfcount(&mut engine, b"pf:u2"), 3);
    }

    #[test]
    fn pfmerge_then_count() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"PFADD", b"pf:s1", b"a", b"b", b"c"]));
        engine.execute(&argv(&[b"PFADD", b"pf:s2", b"c", b"d", b"e"]));
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"PFMERGE", b"pf:dst", b"pf:s1", b"pf:s2"]))),
            b"+OK\r\n"
        );
        assert_eq!(pfcount(&mut engine, b"pf:dst"), 5);
        // Merging into an existing dest unions in the dest's own registers too.
        engine.execute(&argv(&[b"PFADD", b"pf:s3", b"f", b"g"]));
        engine.execute(&argv(&[b"PFMERGE", b"pf:dst", b"pf:s3"]));
        assert_eq!(pfcount(&mut engine, b"pf:dst"), 7);
    }

    #[test]
    fn pf_commands_reject_plain_strings() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SET", b"pf:str", b"hello"]));
        let wrong = b"-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n";
        assert_eq!(resp2(&engine.execute(&argv(&[b"PFADD", b"pf:str", b"x"]))), wrong);
        assert_eq!(resp2(&engine.execute(&argv(&[b"PFCOUNT", b"pf:str"]))), wrong);
        assert_eq!(resp2(&engine.execute(&argv(&[b"PFMERGE", b"pf:dst2", b"pf:str"]))), wrong);
    }

    #[test]
    fn pfcount_estimation_matches_valkey_at_scale() {
        let mut engine = Engine::new_in_memory();
        // Probed against reference valkey-server: these exact integers.
        for i in 1..=100 {
            engine.execute(&argv(&[b"PFADD", b"pf:h100", format!("elem-{}", i).as_bytes()]));
        }
        assert_eq!(pfcount(&mut engine, b"pf:h100"), 100);
        for i in 1..=1000 {
            engine.execute(&argv(&[b"PFADD", b"pf:h1000", format!("item:{}", i).as_bytes()]));
        }
        assert_eq!(pfcount(&mut engine, b"pf:h1000"), 1002);
    }

    #[test]
    fn pfadd_only_dirties_on_change() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"PFADD", b"pf:d", b"a"]));
        let epoch = engine.mutation_epoch();
        let _ = engine.take_dirty();
        // A no-op PFADD (duplicate) must not bump the epoch or dirty the key.
        engine.execute(&argv(&[b"PFADD", b"pf:d", b"a"]));
        assert_eq!(engine.mutation_epoch(), epoch);
        assert!(engine.take_dirty().is_empty());
        // PFCOUNT is read-only: no epoch bump, no dirty.
        let _ = pfcount(&mut engine, b"pf:d");
        assert_eq!(engine.mutation_epoch(), epoch);
        assert!(engine.take_dirty().is_empty());
    }

    #[test]
    fn hll_dense_register_roundtrips() {
        // Exercise the 6-bit packed get/set across the byte-straddling cases.
        let mut regs = vec![0u8; (HLL_REGISTERS * HLL_BITS as usize).div_ceil(8)];
        for i in 0..HLL_REGISTERS {
            let v = (i % (HLL_REGISTER_MAX as usize + 1)) as u8;
            hll_dense_set_register(&mut regs, i, v);
        }
        for i in 0..HLL_REGISTERS {
            let v = (i % (HLL_REGISTER_MAX as usize + 1)) as u8;
            assert_eq!(hll_dense_get_register(&regs, i), v);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // DUMP / RESTORE — RDB framing, LZF, CRC64
    // ──────────────────────────────────────────────────────────────────────

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn dump_bytes(engine: &mut Engine<NoopHost>, key: &[u8]) -> Vec<u8> {
        match engine.execute(&argv(&[b"DUMP", key])) {
            RespFrame::Bulk(Some(bytes)) => bytes.as_bytes().to_vec(),
            other => panic!("expected bulk DUMP reply, got {other:?}"),
        }
    }

    #[test]
    fn crc64_jones_known_vector() {
        assert_eq!(crc64(0, b"123456789"), 0xe9c6d914c4b8d9ca);
    }

    /// DUMP must be byte-identical to valkey-server 9.1.0. These payloads were
    /// captured by running `SET k v; DUMP k` against the reference binary.
    #[test]
    fn dump_byte_parity_integers_and_short_strings() {
        // (value, expected DUMP hex) — captured from valkey-server 9.1.0.
        let cases: &[(&[u8], &str)] = &[
            (b"7", "00c0075000bbe781331df9a966"),
            (b"-7", "00c0f95000910c18a149f0255f"),
            (b"12345", "00c13930500052be23b60dae6f4d"),
            (b"1000", "00c1e803500046fb8e1f2496d090"),
            (b"100000", "00c2a086010050003e0ecac1f17dc180"),
            (b"99999999999", "000b39393939393939393939395000ec9a816a9089888b"),
            (b"hello", "000568656c6c6f5000ac5816e7fb6647fe"),
            (b"", "0000500092b195d1912dc52e"),
            (b"01234567890123456789", "00143031323334353637383930313233343536373839500019bc68b4636080b8"),
            (b"012345678901234567890", "001530313233343536373839303132333435363738393050002b61bc1664a3947b"),
        ];
        for (value, expected) in cases {
            let mut engine = Engine::new_in_memory();
            engine.execute(&argv(&[b"SET", b"k", value]));
            let got = dump_bytes(&mut engine, b"k");
            assert_eq!(
                got,
                hex(expected),
                "DUMP parity failed for value {:?}: got {} want {}",
                String::from_utf8_lossy(value),
                hex_encode(&got),
                expected
            );
        }
    }

    /// Long, compressible strings: DUMP must reproduce valkey's exact
    /// LZF-compressed payload byte-for-byte. Incompressible long strings must
    /// store verbatim (the `rand_noncompress` case). Captured from
    /// valkey-server 9.1.0.
    #[test]
    fn dump_byte_parity_long_strings_lzf() {
        let a64: Vec<u8> = vec![b'a'; 64];
        let a200: Vec<u8> = vec![b'a'; 200];
        let ab_rep: Vec<u8> = b"ab".iter().cycle().take(80).copied().collect();
        let lorem: Vec<u8> = b"the quick brown fox jumps over the lazy dog "
            .iter()
            .cycle()
            .take(44 * 5)
            .copied()
            .collect();
        let mixed: Vec<u8> = b"hello world hello world hello world hello world!!!".to_vec();
        let json: Vec<u8> = {
            let unit = br#"{"name":"valdr","type":"engine","wave":21,"name":"valdr"}"#;
            unit.iter().chain(unit.iter()).copied().collect()
        };
        let spaces: Vec<u8> = vec![b' '; 50];
        let rand_noncompress: Vec<u8> = (0u8..64).collect();

        let cases: &[(&[u8], &str)] = &[
            (&a64, "00c3094040016161e0330001616150000c4f5f83504ea398"),
            (&a200, "00c30940c8016161e0bb000161615000d0b33ebf9fe06a17"),
            (&ab_rep, "00c30a405002616261e0420101616250005bb7969f2f8cc3b1"),
            (&lorem, "00c33440dc1f74686520717569636b2062726f776e20666f78206a756d7073206f7665722074201e076c617a7920646f67600ce0a12b01672050000d01c00229d0be05"),
            (&mixed, "00c315320c68656c6c6f20776f726c642068e0190b022121215000eb013a6f466048d7"),
            (&json, "00c3394072137b226e616d65223a2276616c6472222c22747970400e05656e67696e65200f02776176200f0232312ce00528017d7be0050fe01f3801227d5000e4085c453d3ab387"),
            (&spaces, "00c30932012020e025000120205000019e274029dcfea9"),
            (&rand_noncompress, "004040000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f50004326ce3d6b5ad3b1"),
        ];
        for (value, expected) in cases {
            let mut engine = Engine::new_in_memory();
            engine.execute(&argv(&[b"SET", b"k", value]));
            let got = dump_bytes(&mut engine, b"k");
            assert_eq!(
                got,
                hex(expected),
                "LZF DUMP parity failed (len {}): got {}",
                value.len(),
                hex_encode(&got)
            );
        }
    }

    #[test]
    fn dump_missing_key_is_nil() {
        let mut engine = Engine::new_in_memory();
        assert!(matches!(
            engine.execute(&argv(&[b"DUMP", b"nope"])),
            RespFrame::Bulk(None)
        ));
    }

    /// DUMP of a still-deferred type (a Stream, whose RDB encoding the engine
    /// does not reproduce) yields the aggregate-deferral error. Wave 22 added
    /// LIST/ZSET/integer-SET and Wave 23 added plain HASH, so those no longer
    /// error here; a non-integer SET and a hash-with-field-TTL are also
    /// deferred (asserted in their own tests).
    #[test]
    fn dump_wrong_type_is_deferral_error() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"XADD", b"st", b"1-1", b"f", b"v"]));
        let reply = engine.execute(&argv(&[b"DUMP", b"st"]));
        assert!(matches!(reply, RespFrame::Error(_)));
    }

    /// In-process DUMP→RESTORE round-trip: RESTORE the engine's own DUMP into a
    /// new key and assert the restored value equals the original.
    #[test]
    fn dump_restore_round_trip_strings() {
        let values: &[&[u8]] = &[
            b"7",
            b"-7",
            b"12345",
            b"100000",
            b"99999999999",
            b"hello",
            b"",
            b"01234567890123456789",
            &[b'a'; 200],
            b"the quick brown fox jumps over the lazy dog repeated repeated repeated",
        ];
        for value in values {
            let mut engine = Engine::new_in_memory();
            engine.execute(&argv(&[b"SET", b"src", value]));
            let payload = dump_bytes(&mut engine, b"src");
            let reply = engine.execute(&argv(&[b"RESTORE", b"dst", b"0", &payload]));
            assert_eq!(resp2(&reply), b"+OK\r\n", "RESTORE failed for {value:?}");
            let restored = engine.execute(&argv(&[b"GET", b"dst"]));
            match restored {
                RespFrame::Bulk(Some(bytes)) => assert_eq!(bytes.as_bytes(), *value),
                RespFrame::Bulk(None) if value.is_empty() => {
                    // empty-string GET still returns a bulk of length 0
                    panic!("empty string should round-trip to an empty bulk, got nil");
                }
                other => panic!("unexpected GET after RESTORE: {other:?}"),
            }
        }
    }

    /// RESTORE a hardcoded real valkey integer-string dump (`SET k 12345; DUMP
    /// k` on valkey-server 9.1.0) and assert the value decodes correctly.
    #[test]
    fn restore_hardcoded_valkey_integer_dump() {
        let mut engine = Engine::new_in_memory();
        let payload = hex("00c13930500052be23b60dae6f4d");
        let reply = engine.execute(&argv(&[b"RESTORE", b"k", b"0", &payload]));
        assert_eq!(resp2(&reply), b"+OK\r\n");
        match engine.execute(&argv(&[b"GET", b"k"])) {
            RespFrame::Bulk(Some(bytes)) => assert_eq!(bytes.as_bytes(), b"12345"),
            other => panic!("unexpected GET: {other:?}"),
        }
        // TYPE must be string.
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"TYPE", b"k"]))),
            b"+string\r\n"
        );
    }

    /// RESTORE a hardcoded real valkey short-string dump (`SET k hello; DUMP k`).
    #[test]
    fn restore_hardcoded_valkey_short_string_dump() {
        let mut engine = Engine::new_in_memory();
        let payload = hex("000568656c6c6f5000ac5816e7fb6647fe");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"k", b"0", &payload]))),
            b"+OK\r\n"
        );
        match engine.execute(&argv(&[b"GET", b"k"])) {
            RespFrame::Bulk(Some(bytes)) => assert_eq!(bytes.as_bytes(), b"hello"),
            other => panic!("unexpected GET: {other:?}"),
        }
    }

    /// RESTORE a hardcoded real valkey LZF-compressed long-string dump
    /// (`SET k aaaa...(64); DUMP k`) — exercises the `lzf_decompress` path.
    #[test]
    fn restore_hardcoded_valkey_lzf_dump() {
        let mut engine = Engine::new_in_memory();
        let payload = hex("00c3094040016161e0330001616150000c4f5f83504ea398");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"k", b"0", &payload]))),
            b"+OK\r\n"
        );
        match engine.execute(&argv(&[b"GET", b"k"])) {
            RespFrame::Bulk(Some(bytes)) => assert_eq!(bytes.as_bytes(), vec![b'a'; 64].as_slice()),
            other => panic!("unexpected GET: {other:?}"),
        }
    }

    #[test]
    fn restore_busykey_without_replace() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SET", b"k", b"existing"]));
        let payload = hex("000568656c6c6f5000ac5816e7fb6647fe");
        let reply = engine.execute(&argv(&[b"RESTORE", b"k", b"0", &payload]));
        assert_eq!(
            resp2(&reply),
            b"-BUSYKEY Target key name already exists.\r\n"
        );
        // value unchanged
        match engine.execute(&argv(&[b"GET", b"k"])) {
            RespFrame::Bulk(Some(bytes)) => assert_eq!(bytes.as_bytes(), b"existing"),
            other => panic!("unexpected GET: {other:?}"),
        }
    }

    #[test]
    fn restore_replace_overwrites() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SET", b"k", b"existing"]));
        let payload = hex("000568656c6c6f5000ac5816e7fb6647fe");
        let reply = engine.execute(&argv(&[b"RESTORE", b"k", b"0", &payload, b"REPLACE"]));
        assert_eq!(resp2(&reply), b"+OK\r\n");
        match engine.execute(&argv(&[b"GET", b"k"])) {
            RespFrame::Bulk(Some(bytes)) => assert_eq!(bytes.as_bytes(), b"hello"),
            other => panic!("unexpected GET: {other:?}"),
        }
    }

    #[test]
    fn restore_bad_checksum_errors() {
        let mut engine = Engine::new_in_memory();
        let mut payload = hex("000568656c6c6f5000ac5816e7fb6647fe");
        let last = payload.len() - 1;
        payload[last] ^= 0xff; // corrupt the CRC
        let reply = engine.execute(&argv(&[b"RESTORE", b"k", b"0", &payload]));
        assert_eq!(
            resp2(&reply),
            b"-ERR DUMP payload version or checksum are wrong\r\n"
        );
    }

    #[test]
    fn restore_future_version_errors() {
        let mut engine = Engine::new_in_memory();
        // Re-frame "hello" with a bumped RDB version (81 > 80) so the CRC is
        // valid but the version is rejected.
        let mut payload = Vec::new();
        payload.push(RDB_TYPE_STRING);
        rdb_save_raw_string(&mut payload, b"hello");
        payload.push(81u8); // version low byte
        payload.push(0u8); // version high byte
        let crc = crc64(0, &payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        let reply = engine.execute(&argv(&[b"RESTORE", b"k", b"0", &payload]));
        assert_eq!(
            resp2(&reply),
            b"-ERR DUMP payload version or checksum are wrong\r\n"
        );
    }

    #[test]
    fn restore_truncated_payload_errors() {
        let mut engine = Engine::new_in_memory();
        let reply = engine.execute(&argv(&[b"RESTORE", b"k", b"0", b"short"]));
        assert_eq!(
            resp2(&reply),
            b"-ERR DUMP payload version or checksum are wrong\r\n"
        );
    }

    #[test]
    fn restore_negative_ttl_errors() {
        let mut engine = Engine::new_in_memory();
        let payload = hex("000568656c6c6f5000ac5816e7fb6647fe");
        let reply = engine.execute(&argv(&[b"RESTORE", b"k", b"-1", &payload]));
        assert_eq!(resp2(&reply), b"-ERR Invalid TTL value, must be >= 0\r\n");
    }

    #[test]
    fn restore_relative_ttl_applied() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        let payload = hex("000568656c6c6f5000ac5816e7fb6647fe");
        // ttl = 5000ms relative → expire_at = 1000 + 5000 = 6000.
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"k", b"5000", &payload]))),
            b"+OK\r\n"
        );
        // PTTL should be ~5000.
        match engine.execute(&argv(&[b"PTTL", b"k"])) {
            RespFrame::Integer(ms) => assert_eq!(ms, 5000),
            other => panic!("unexpected PTTL: {other:?}"),
        }
    }

    #[test]
    fn restore_absttl_applied() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        let payload = hex("000568656c6c6f5000ac5816e7fb6647fe");
        // absolute deadline 9000.
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"RESTORE", b"k", b"9000", &payload, b"ABSTTL"
            ]))),
            b"+OK\r\n"
        );
        match engine.execute(&argv(&[b"PTTL", b"k"])) {
            RespFrame::Integer(ms) => assert_eq!(ms, 8000),
            other => panic!("unexpected PTTL: {other:?}"),
        }
    }

    #[test]
    fn restore_already_expired_absttl_does_not_store() {
        let mut engine = Engine::new(NoopHost::new(10_000));
        let payload = hex("000568656c6c6f5000ac5816e7fb6647fe");
        // absolute deadline 5000 < now 10000 → key is not created, reply +OK.
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"RESTORE", b"k", b"5000", &payload, b"ABSTTL"
            ]))),
            b"+OK\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"EXISTS", b"k"]))),
            b":0\r\n"
        );
    }

    #[test]
    fn restore_idletime_and_freq_accepted_and_validated() {
        let mut engine = Engine::new_in_memory();
        let payload = hex("000568656c6c6f5000ac5816e7fb6647fe");
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"RESTORE", b"k", b"0", &payload, b"IDLETIME", b"100"
            ]))),
            b"+OK\r\n"
        );
        let payload2 = hex("000568656c6c6f5000ac5816e7fb6647fe");
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"RESTORE", b"k2", b"0", &payload2, b"FREQ", b"5"
            ]))),
            b"+OK\r\n"
        );
        // FREQ out of range.
        let payload3 = hex("000568656c6c6f5000ac5816e7fb6647fe");
        assert_eq!(
            resp2(&engine.execute(&argv(&[
                b"RESTORE", b"k3", b"0", &payload3, b"FREQ", b"300"
            ]))),
            b"-ERR Invalid FREQ value, must be >= 0 and <= 255\r\n"
        );
    }

    #[test]
    fn restore_bad_data_format_for_unsupported_type() {
        let mut engine = Engine::new_in_memory();
        // Type byte 0x0e (a quicklist) with a valid CRC but unsupported type.
        let mut payload = vec![0x0eu8, 0x00];
        payload.push(80u8);
        payload.push(0u8);
        let crc = crc64(0, &payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        let reply = engine.execute(&argv(&[b"RESTORE", b"k", b"0", &payload]));
        assert_eq!(resp2(&reply), b"-ERR Bad data format\r\n");
    }

    /// One-off brute-force parity check against a captured valkey corpus.
    /// Ignored by default; run with the corpus path in VALDR_DUMP_CORPUS:
    /// `VALDR_DUMP_CORPUS=/path/corpus.tsv cargo test -p valdr-engine \
    ///   dump_corpus_byte_parity -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn dump_corpus_byte_parity() {
        let path = std::env::var("VALDR_DUMP_CORPUS").expect("set VALDR_DUMP_CORPUS");
        let text = std::fs::read_to_string(path).unwrap();
        let mut total = 0usize;
        let mut diverge = 0usize;
        for line in text.lines() {
            let mut parts = line.split('\t');
            let value = hex(parts.next().unwrap());
            let expected = hex(parts.next().unwrap());
            let mut engine = Engine::new_in_memory();
            engine.execute(&argv(&[b"SET", b"k", &value]));
            let got = dump_bytes(&mut engine, b"k");
            total += 1;
            if got != expected {
                diverge += 1;
                println!(
                    "DIVERGE len={} value={:?}\n  got  {}\n  want {}",
                    value.len(),
                    String::from_utf8_lossy(&value),
                    hex_encode(&got),
                    hex_encode(&expected)
                );
            }
        }
        println!("corpus parity: {}/{} match, {} diverge", total - diverge, total, diverge);
        assert_eq!(diverge, 0, "{diverge} DUMP divergences vs valkey");
    }

    #[test]
    fn lzf_compress_decompress_internal_round_trip() {
        let input: Vec<u8> = b"abcabcabcabcabcabcabcabcabcabcabcabc".to_vec();
        if let Some(compressed) = lzf_compress(&input) {
            let back = lzf_decompress(&compressed, input.len()).unwrap();
            assert_eq!(back, input);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // DUMP / RESTORE — aggregate types (LIST, ZSET, integer SET)
    //
    // The expected hex blobs below were captured from valkey-server 9.1.0:
    // `RPUSH/ZADD/SADD ...; DUMP key` via `valkey-cli --no-raw`. DUMP must be
    // byte-identical to these.
    // ──────────────────────────────────────────────────────────────────────

    /// LIST [a, b, c] → RDB_TYPE_LIST_QUICKLIST_2, single PACKED listpack node.
    #[test]
    fn dump_byte_parity_list_strings() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"RPUSH", b"l", b"a", b"b", b"c"]));
        let got = dump_bytes(&mut engine, b"l");
        assert_eq!(
            got,
            hex("12010210100000000300816102816202816302ff50000732709d0b61356a"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// LIST [1, 22, 333] → members auto-detected as listpack integers.
    #[test]
    fn dump_byte_parity_list_integers() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"RPUSH", b"l", b"1", b"22", b"333"]));
        let got = dump_bytes(&mut engine, b"l");
        assert_eq!(
            got,
            hex("1201020e0e000000030001011601c14d02ff5000457ccc4539bcbb15"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// ZSET {1:a, 2:b, 3:c} → RDB_TYPE_ZSET_LISTPACK, member+integer-score pairs.
    #[test]
    fn dump_byte_parity_zset_integer_scores() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]));
        let got = dump_bytes(&mut engine, b"z");
        assert_eq!(
            got,
            hex("1116160000000600816102010181620202018163020301ff5000e131b5549ef85262"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// ZSET with a fractional score → score stored as the d2string text "1.5".
    #[test]
    fn dump_byte_parity_zset_float_score() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"ZADD", b"z", b"1.5", b"x", b"2", b"y"]));
        let got = dump_bytes(&mut engine, b"z");
        assert_eq!(
            got,
            hex("111414000000040081780283312e35048179020201ff50000f5498e0353675d1"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// ZSET equal scores → emitted in member-lexicographic order (a, b, c).
    #[test]
    fn dump_byte_parity_zset_tiebreak() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"ZADD", b"z", b"5", b"b", b"5", b"a", b"5", b"c"]));
        let got = dump_bytes(&mut engine, b"z");
        assert_eq!(
            got,
            hex("1116160000000600816102050181620205018163020501ff5000ffa936ad2960f69e"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// SET of all-integers {3,1,2,100} → RDB_TYPE_SET_INTSET, sorted int16.
    #[test]
    fn dump_byte_parity_intset_int16() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SADD", b"s", b"3", b"1", b"2", b"100"]));
        let got = dump_bytes(&mut engine, b"s");
        assert_eq!(
            got,
            hex("0b1002000000040000000100020003006400500035ac5286d49d3bbd"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// Wider members force int32 width for the whole intset.
    #[test]
    fn dump_byte_parity_intset_int32() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SADD", b"s", b"-5", b"70000", b"3"]));
        let got = dump_bytes(&mut engine, b"s");
        assert_eq!(
            got,
            hex("0b140400000003000000fbffffff03000000701101005000ba94e7d1e8b2e6a5"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// A 64-bit member forces int64 width.
    #[test]
    fn dump_byte_parity_intset_int64() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SADD", b"s", b"5000000000", b"1"]));
        let got = dump_bytes(&mut engine, b"s");
        assert_eq!(
            got,
            hex("0b180800000002000000010000000000000000f2052a0100000050009bdf6bfb7955d208"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// A small non-integer SET → RDB_TYPE_SET_LISTPACK (type byte 20), members
    /// in insertion order. Both string members survive verbatim. Captured from
    /// valkey-server 9.1.0 (`SADD s foo bar`).
    #[test]
    fn dump_byte_parity_set_listpack_strings() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SADD", b"s", b"foo", b"bar"]));
        let got = dump_bytes(&mut engine, b"s");
        assert_eq!(
            got,
            hex("141111000000020083666f6f048362617204ff5000d21f22b90176bebc"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// A non-integer SET with single-byte string members (`a b c d`), insertion
    /// order preserved in the listpack.
    #[test]
    fn dump_byte_parity_set_listpack_short_strings() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SADD", b"s", b"a", b"b", b"c", b"d"]));
        let got = dump_bytes(&mut engine, b"s");
        assert_eq!(
            got,
            hex("1413130000000400816102816202816302816402ff5000fcd288811fffc033"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// A single non-integer member SET (`only`).
    #[test]
    fn dump_byte_parity_set_listpack_single() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SADD", b"s", b"only"]));
        let got = dump_bytes(&mut engine, b"s");
        assert_eq!(
            got,
            hex("140d0d0000000100846f6e6c7905ff5000221b634e081f0d13"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// A listpack set whose first member is a non-integer, then integers, then a
    /// string (`hello 1 2 world`): created as a listpack from the start, so each
    /// member is appended in insertion order — the integers `1`/`2` stored as
    /// listpack integer entries.
    #[test]
    fn dump_byte_parity_set_listpack_mixed_order() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SADD", b"s", b"hello", b"1", b"2", b"world"]));
        let got = dump_bytes(&mut engine, b"s");
        assert_eq!(
            got,
            hex("14191900000004008568656c6c6f060101020185776f726c6406ff50005c01682b4367a5f8"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// A set that begins as an intset (`3 1 2`) and then gains a non-integer
    /// (`foo`): valkey converts the intset to a listpack, iterating the intset
    /// in **sorted** order (`1 2 3`) and appending `foo`. The DUMP must
    /// reproduce that sorted-prefix order, not the engine's raw insertion order.
    #[test]
    fn dump_byte_parity_set_intset_to_listpack_sorted_prefix() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SADD", b"s", b"3", b"1", b"2", b"foo"]));
        let got = dump_bytes(&mut engine, b"s");
        assert_eq!(
            got,
            hex("141212000000040001010201030183666f6f04ff5000e3f9d88bc095253e"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// A listpack set containing the empty string member alongside a string
    /// (`"" foo`).
    #[test]
    fn dump_byte_parity_set_listpack_empty_member() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SADD", b"s", b"", b"foo"]));
        let got = dump_bytes(&mut engine, b"s");
        assert_eq!(
            got,
            hex("140e0e0000000200800183666f6f04ff500069364761c6b1cbc0"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// A non-integer SET past `set-max-listpack-entries` (129 short members) is a
    /// hashtable in valkey (`RDB_TYPE_SET`), which the engine cannot reproduce
    /// byte-for-byte, so its DUMP is deferred with the aggregate-deferral error.
    #[test]
    fn dump_large_non_integer_set_is_deferred() {
        let mut engine = Engine::new_in_memory();
        let mut cmd: Vec<Vec<u8>> = vec![b"SADD".to_vec(), b"s".to_vec()];
        for i in 0..129 {
            cmd.push(format!("m{i}").into_bytes());
        }
        let cmd_refs: Vec<&[u8]> = cmd.iter().map(|v| v.as_slice()).collect();
        engine.execute(&argv(&cmd_refs));
        assert!(matches!(
            engine.execute(&argv(&[b"DUMP", b"s"])),
            RespFrame::Error(_)
        ));
    }

    /// HASH {a:1, b:2, c:3} → RDB_TYPE_HASH_LISTPACK, field/value pairs in
    /// insertion order, both members integer-encoded where canonical. Captured
    /// from valkey-server 9.1.0.
    #[test]
    fn dump_byte_parity_hash_int_values() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3"]));
        let got = dump_bytes(&mut engine, b"h");
        assert_eq!(
            got,
            hex("1016160000000600816102010181620202018163020301ff500077b14e27930cd9aa"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// HASH with string field and string value (`only` → `val`).
    #[test]
    fn dump_byte_parity_hash_single_string() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"HSET", b"h", b"only", b"val"]));
        let got = dump_bytes(&mut engine, b"h");
        assert_eq!(
            got,
            hex("1012120000000200846f6e6c79058376616c04ff500032e703df7d285a7c"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// HASH with multiple string fields/values, insertion order preserved
    /// (`name`→`alice`, `city`→`paris`).
    #[test]
    fn dump_byte_parity_hash_string_pairs() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[
            b"HSET", b"h", b"name", b"alice", b"city", b"paris",
        ]));
        let got = dump_bytes(&mut engine, b"h");
        assert_eq!(
            got,
            hex("1021210000000400846e616d650585616c6963650684636974790585706172697306ff50008bc625e47b16652b"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// HASH whose values span the listpack integer widths (8/16/32-bit):
    /// `f1`→`100`, `f2`→`-128`, `f3`→`70000`. Confirms each integer value is
    /// encoded with the exact width valkey uses.
    #[test]
    fn dump_byte_parity_hash_int_widths() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[
            b"HSET", b"h", b"f1", b"100", b"f2", b"-128", b"f3", b"70000",
        ]));
        let got = dump_bytes(&mut engine, b"h");
        assert_eq!(
            got,
            hex("101d1d000000060082663103640182663203df800282663303f270110104ff5000a4ada730493e09a9"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// A small HASH carrying per-field TTLs is hashtable-encoded in valkey and
    /// DUMPs as `RDB_TYPE_HASH_2` (type byte 22): a field count, then
    /// `<field><value><8-byte-LE-i64-expiry-ms>` triples in insertion order
    /// (`EXPIRY_NONE` = -1 = all-`0xff` for an untracked field). Captured from
    /// valkey-server 9.1.0 with `HEXPIREAT key 99999999999 FIELDS ...` (the
    /// deadline stored is `99999999999 * 1000` ms = `183c7a10f35a0000` LE).
    #[test]
    fn dump_byte_parity_hash2_single_field_ttl() {
        // f -> v, TTL on f.
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[b"HSET", b"h", b"f", b"v"]));
        engine.execute(&argv(&[
            b"HEXPIREAT", b"h", b"99999999999", b"FIELDS", b"1", b"f",
        ]));
        let got = dump_bytes(&mut engine, b"h");
        assert_eq!(
            got,
            hex("160101660176183c7a10f35a0000500034d044f1e78b2d4e"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// HASH_2 with a tracked first field and two untracked fields, integer
    /// values: `a`→1 (TTL), `b`→2, `c`→3. The untracked fields carry the
    /// `EXPIRY_NONE` (all-`0xff`) 8-byte sentinel. Captured from valkey 9.1.0.
    #[test]
    fn dump_byte_parity_hash2_mixed_ttl() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3"]));
        engine.execute(&argv(&[
            b"HEXPIREAT", b"h", b"99999999999", b"FIELDS", b"1", b"a",
        ]));
        let got = dump_bytes(&mut engine, b"h");
        assert_eq!(
            got,
            hex("16030161c001183c7a10f35a00000162c002ffffffffffffffff0163c003ffffffffffffffff5000492f6728cfd335ce"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// HASH_2 where both fields carry a TTL: `x`→10, `y`→20, both tracked with
    /// the same absolute deadline. Captured from valkey 9.1.0.
    #[test]
    fn dump_byte_parity_hash2_all_ttl() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[b"HSET", b"h", b"x", b"10", b"y", b"20"]));
        engine.execute(&argv(&[
            b"HEXPIREAT", b"h", b"99999999999", b"FIELDS", b"2", b"x", b"y",
        ]));
        let got = dump_bytes(&mut engine, b"h");
        assert_eq!(
            got,
            hex("16020178c00a183c7a10f35a00000179c014183c7a10f35a000050005aed679b940bbdbc"),
            "got {}",
            hex_encode(&got)
        );
    }

    /// RESTORE of a real valkey 9.1.0 `RDB_TYPE_HASH_2` payload reconstructs the
    /// fields, values, and per-field TTLs. Uses the `hb` capture (a TTL, b/c
    /// untracked); after RESTORE, `HGET` returns the values and `HPEXPIRETIME`
    /// returns the tracked deadline for `a` and -1 for the untracked fields.
    #[test]
    fn restore_real_valkey_hash2_payload() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        let payload = hex(
            "16030161c001183c7a10f35a00000162c002ffffffffffffffff0163c003ffffffffffffffff5000492f6728cfd335ce",
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"h", b"0", &payload]))),
            b"+OK\r\n"
        );
        for (f, v) in [(&b"a"[..], &b"1"[..]), (b"b", b"2"), (b"c", b"3")] {
            let got = engine.execute(&argv(&[b"HGET", b"h", f]));
            assert_eq!(
                got,
                RespFrame::Bulk(Some(RedisString::from_bytes(v.to_vec()))),
                "field {:?}",
                f
            );
        }
        // HPEXPIRETIME a -> the tracked absolute ms deadline (99999999999000).
        let got = engine.execute(&argv(&[
            b"HPEXPIRETIME", b"h", b"FIELDS", b"3", b"a", b"b", b"c",
        ]));
        assert_eq!(
            got,
            RespFrame::array(vec![
                RespFrame::integer(99999999999000),
                RespFrame::integer(-1),
                RespFrame::integer(-1),
            ])
        );
    }

    /// In-process DUMP→RESTORE round-trip for a TTL-carrying hash: the
    /// reconstructed value carries the same fields, values, and the tracked
    /// per-field deadline.
    #[test]
    fn dump_restore_round_trip_hash2() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[b"HSET", b"src", b"a", b"1", b"b", b"2"]));
        engine.execute(&argv(&[
            b"HEXPIREAT", b"src", b"99999999999", b"FIELDS", b"1", b"a",
        ]));
        let payload = dump_bytes(&mut engine, b"src");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"dst", b"0", &payload]))),
            b"+OK\r\n"
        );
        assert_eq!(
            engine.execute(&argv(&[b"HGET", b"dst", b"a"])),
            RespFrame::Bulk(Some(RedisString::from_bytes(b"1".to_vec())))
        );
        assert_eq!(
            engine.execute(&argv(&[b"HPEXPIRETIME", b"dst", b"FIELDS", b"2", b"a", b"b"])),
            RespFrame::array(vec![
                RespFrame::integer(99999999999000),
                RespFrame::integer(-1),
            ])
        );
    }

    /// A TTL-carrying HASH past `HASH_RDB2_INSERTION_ORDER_MAX` fields is still
    /// deferred: valkey's hashtable iterates in hash-seed bucket order the
    /// engine cannot reproduce, so DUMP returns the aggregate-deferral error.
    #[test]
    fn dump_hash2_large_is_deferred() {
        let mut engine = Engine::new(NoopHost::new(1_000));
        engine.execute(&argv(&[
            b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3", b"d", b"4", b"e", b"5", b"f", b"6",
            b"g", b"7",
        ]));
        engine.execute(&argv(&[
            b"HEXPIREAT", b"h", b"99999999999", b"FIELDS", b"1", b"a",
        ]));
        assert!(matches!(
            engine.execute(&argv(&[b"DUMP", b"h"])),
            RespFrame::Error(_)
        ));
    }

    /// In-process DUMP→RESTORE round-trip for each aggregate, asserting the
    /// reconstructed value matches via the read commands.
    #[test]
    fn dump_restore_round_trip_list() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"RPUSH", b"src", b"a", b"22", b"333", b"hello world"]));
        let payload = dump_bytes(&mut engine, b"src");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"dst", b"0", &payload]))),
            b"+OK\r\n"
        );
        let got = engine.execute(&argv(&[b"LRANGE", b"dst", b"0", b"-1"]));
        let RespFrame::Array(Some(items)) = got else {
            panic!("expected array, got {got:?}");
        };
        let vals: Vec<Vec<u8>> = items
            .iter()
            .map(|f| match f {
                RespFrame::Bulk(Some(b)) => b.as_bytes().to_vec(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            vals,
            vec![
                b"a".to_vec(),
                b"22".to_vec(),
                b"333".to_vec(),
                b"hello world".to_vec()
            ]
        );
    }

    #[test]
    fn dump_restore_round_trip_zset() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"ZADD", b"src", b"1", b"a", b"2.5", b"b", b"3", b"c"]));
        let payload = dump_bytes(&mut engine, b"src");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"dst", b"0", &payload]))),
            b"+OK\r\n"
        );
        let got = engine.execute(&argv(&[b"ZRANGE", b"dst", b"0", b"-1", b"WITHSCORES"]));
        let RespFrame::Array(Some(items)) = got else {
            panic!("expected array, got {got:?}");
        };
        let vals: Vec<Vec<u8>> = items
            .iter()
            .map(|f| match f {
                RespFrame::Bulk(Some(b)) => b.as_bytes().to_vec(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            vals,
            vec![
                b"a".to_vec(),
                b"1".to_vec(),
                b"b".to_vec(),
                b"2.5".to_vec(),
                b"c".to_vec(),
                b"3".to_vec()
            ]
        );
    }

    #[test]
    fn dump_restore_round_trip_intset() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[b"SADD", b"src", b"3", b"1", b"2", b"100", b"-7"]));
        let payload = dump_bytes(&mut engine, b"src");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"dst", b"0", &payload]))),
            b"+OK\r\n"
        );
        let mut got: Vec<i64> = match engine.execute(&argv(&[b"SMEMBERS", b"dst"])) {
            RespFrame::Array(Some(items)) => items
                .iter()
                .map(|f| match f {
                    RespFrame::Bulk(Some(b)) => {
                        std::str::from_utf8(b.as_bytes()).unwrap().parse().unwrap()
                    }
                    other => panic!("unexpected {other:?}"),
                })
                .collect(),
            other => panic!("expected array, got {other:?}"),
        };
        got.sort_unstable();
        assert_eq!(got, vec![-7, 1, 2, 3, 100]);
    }

    /// RESTORE a hardcoded real valkey LIST dump and assert the reconstruction.
    #[test]
    fn restore_hardcoded_valkey_list_dump() {
        let mut engine = Engine::new_in_memory();
        let payload = hex("12010210100000000300816102816202816302ff50000732709d0b61356a");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"l", b"0", &payload]))),
            b"+OK\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"TYPE", b"l"]))),
            b"+list\r\n"
        );
        let got = engine.execute(&argv(&[b"LRANGE", b"l", b"0", b"-1"]));
        let RespFrame::Array(Some(items)) = got else {
            panic!("expected array, got {got:?}");
        };
        assert_eq!(items.len(), 3);
    }

    /// RESTORE a hardcoded real valkey ZSET listpack dump.
    #[test]
    fn restore_hardcoded_valkey_zset_dump() {
        let mut engine = Engine::new_in_memory();
        let payload =
            hex("1116160000000600816102010181620202018163020301ff5000e131b5549ef85262");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"z", b"0", &payload]))),
            b"+OK\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"ZSCORE", b"z", b"b"]))),
            b"$1\r\n2\r\n"
        );
    }

    /// RESTORE a hardcoded real valkey SET_INTSET dump.
    #[test]
    fn restore_hardcoded_valkey_intset_dump() {
        let mut engine = Engine::new_in_memory();
        let payload = hex("0b1002000000040000000100020003006400500035ac5286d49d3bbd");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"s", b"0", &payload]))),
            b"+OK\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SCARD", b"s"]))),
            b":4\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SISMEMBER", b"s", b"100"]))),
            b":1\r\n"
        );
    }

    /// In-process DUMP→RESTORE round-trip for a non-integer SET (listpack),
    /// asserting the reconstructed value preserves insertion order via SMEMBERS.
    #[test]
    fn dump_restore_round_trip_set_listpack() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[
            b"SADD", b"src", b"delta", b"alpha", b"charlie", b"bravo",
        ]));
        let payload = dump_bytes(&mut engine, b"src");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"dst", b"0", &payload]))),
            b"+OK\r\n"
        );
        assert_eq!(resp2(&engine.execute(&argv(&[b"TYPE", b"dst"]))), b"+set\r\n");
        // SMEMBERS preserves the original insertion order for a listpack set.
        let got = engine.execute(&argv(&[b"SMEMBERS", b"dst"]));
        let RespFrame::Array(Some(items)) = got else {
            panic!("expected array, got {got:?}");
        };
        let members: Vec<Vec<u8>> = items
            .iter()
            .map(|f| match f {
                RespFrame::Bulk(Some(b)) => b.as_bytes().to_vec(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            members,
            vec![
                b"delta".to_vec(),
                b"alpha".to_vec(),
                b"charlie".to_vec(),
                b"bravo".to_vec(),
            ]
        );
    }

    /// RESTORE a hardcoded real valkey SET_LISTPACK dump (captured from
    /// valkey-server 9.1.0: `SADD s foo bar`), then verify membership and the
    /// listpack insertion order.
    #[test]
    fn restore_hardcoded_valkey_set_listpack_dump() {
        let mut engine = Engine::new_in_memory();
        let payload = hex("141111000000020083666f6f048362617204ff5000d21f22b90176bebc");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"s", b"0", &payload]))),
            b"+OK\r\n"
        );
        assert_eq!(resp2(&engine.execute(&argv(&[b"TYPE", b"s"]))), b"+set\r\n");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SISMEMBER", b"s", b"foo"]))),
            b":1\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SISMEMBER", b"s", b"bar"]))),
            b":1\r\n"
        );
        // Insertion order is reconstructed (foo, bar).
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"SMEMBERS", b"s"]))),
            b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        );
    }

    /// In-process DUMP→RESTORE round-trip for a HASH, asserting the
    /// reconstructed value matches in insertion order via HGETALL.
    #[test]
    fn dump_restore_round_trip_hash() {
        let mut engine = Engine::new_in_memory();
        engine.execute(&argv(&[
            b"HSET", b"src", b"z", b"26", b"a", b"1", b"m", b"hello world",
        ]));
        let payload = dump_bytes(&mut engine, b"src");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"dst", b"0", &payload]))),
            b"+OK\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"TYPE", b"dst"]))),
            b"+hash\r\n"
        );
        // HGETALL preserves the original insertion order (z, a, m).
        let got = engine.execute(&argv(&[b"HGETALL", b"dst"]));
        let RespFrame::Array(Some(items)) = got else {
            panic!("expected array, got {got:?}");
        };
        let vals: Vec<Vec<u8>> = items
            .iter()
            .map(|f| match f {
                RespFrame::Bulk(Some(b)) => b.as_bytes().to_vec(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            vals,
            vec![
                b"z".to_vec(),
                b"26".to_vec(),
                b"a".to_vec(),
                b"1".to_vec(),
                b"m".to_vec(),
                b"hello world".to_vec(),
            ]
        );
    }

    /// RESTORE a hardcoded real valkey HASH_LISTPACK dump (captured from
    /// valkey-server 9.1.0: `HSET h a 1 b 2 c 3`).
    #[test]
    fn restore_hardcoded_valkey_hash_dump() {
        let mut engine = Engine::new_in_memory();
        let payload =
            hex("1016160000000600816102010181620202018163020301ff500077b14e27930cd9aa");
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"RESTORE", b"h", b"0", &payload]))),
            b"+OK\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"TYPE", b"h"]))),
            b"+hash\r\n"
        );
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HGET", b"h", b"b"]))),
            b"$1\r\n2\r\n"
        );
        // Insertion order is reconstructed exactly (a, b, c).
        assert_eq!(
            resp2(&engine.execute(&argv(&[b"HKEYS", b"h"]))),
            b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
        );
    }

    /// The intset/listpack encoders are exercised directly for the finicky
    /// bits: the backlen boundary at 64-byte string members and int widths.
    #[test]
    fn listpack_backlen_boundary() {
        // A 63-byte string: encoding is 1 prefix byte + 63 data = 64, which
        // still fits a single backlen byte (<= 127). A 64-byte string: same.
        let mut lp = ListpackWriter::new();
        let s63 = vec![b'x'; 63];
        lp.append_string(&s63);
        let bytes = lp.into_bytes();
        // header(6) + [0xbf | 63 data | backlen=64] + 0xff
        assert_eq!(bytes[6], 0x80 | 63);
        // backlen byte is the entry's encoding+data length (1 + 63 = 64).
        assert_eq!(bytes[6 + 1 + 63], 64);
    }

    #[test]
    fn intset_encode_decode_round_trip() {
        let mut ints = vec![100i64, -5, 70000, 3, i64::MAX, i64::MIN];
        let blob = intset_encode(&mut ints);
        let back = intset_decode(&blob).unwrap();
        let mut expected = vec![100i64, -5, 70000, 3, i64::MAX, i64::MIN];
        expected.sort_unstable();
        assert_eq!(back, expected);
    }

    /// Build the `Keys(...)` variant from a list of byte-string literals, so the
    /// expectation table below reads close to the command argv.
    fn keys(items: &[&[u8]]) -> KeyAccess {
        KeyAccess::Keys(items.iter().map(|k| k.to_vec()).collect())
    }

    #[test]
    fn command_keys_faithful_to_valkey_key_specs() {
        let full = KeyAccess::FullKeyspace;
        let none = KeyAccess::Keys(Vec::new());

        let cases: Vec<(Vec<Vec<u8>>, KeyAccess)> = vec![
            // single key at pos 1
            (argv(&[b"GET", b"k"]), keys(&[b"k"])),
            (argv(&[b"SET", b"k", b"v"]), keys(&[b"k"])),
            (argv(&[b"INCR", b"k"]), keys(&[b"k"])),
            (argv(&[b"ZADD", b"z", b"1", b"a"]), keys(&[b"z"])),
            (argv(&[b"HSET", b"h", b"f", b"v"]), keys(&[b"h"])),
            (argv(&[b"XADD", b"s", b"*", b"f", b"v"]), keys(&[b"s"])),
            (argv(&[b"EXPIRE", b"k", b"10"]), keys(&[b"k"])),
            (argv(&[b"PFADD", b"hll", b"a"]), keys(&[b"hll"])),
            (argv(&[b"GETEX", b"k"]), keys(&[b"k"])),
            (argv(&[b"DUMP", b"k"]), keys(&[b"k"])),
            // all key args (range lastkey:-1 step 1)
            (argv(&[b"MGET", b"k1", b"k2", b"k3"]), keys(&[b"k1", b"k2", b"k3"])),
            (argv(&[b"DEL", b"k1", b"k2"]), keys(&[b"k1", b"k2"])),
            (argv(&[b"EXISTS", b"a", b"b"]), keys(&[b"a", b"b"])),
            (argv(&[b"TOUCH", b"a", b"b"]), keys(&[b"a", b"b"])),
            (argv(&[b"UNLINK", b"a"]), keys(&[b"a"])),
            (argv(&[b"WATCH", b"a", b"b"]), keys(&[b"a", b"b"])),
            (argv(&[b"PFCOUNT", b"h1", b"h2"]), keys(&[b"h1", b"h2"])),
            // alternating key/value (range step 2)
            (argv(&[b"MSET", b"k1", b"v1", b"k2", b"v2"]), keys(&[b"k1", b"k2"])),
            (argv(&[b"MSETNX", b"k1", b"v1", b"k2", b"v2"]), keys(&[b"k1", b"k2"])),
            // MSETEX numkeys key val key val
            (
                argv(&[b"MSETEX", b"2", b"a", b"1", b"b", b"2"]),
                keys(&[b"a", b"b"]),
            ),
            // two keys
            (argv(&[b"RENAME", b"a", b"b"]), keys(&[b"a", b"b"])),
            (argv(&[b"RENAMENX", b"a", b"b"]), keys(&[b"a", b"b"])),
            (argv(&[b"SMOVE", b"a", b"b", b"m"]), keys(&[b"a", b"b"])),
            (argv(&[b"LMOVE", b"a", b"b", b"LEFT", b"RIGHT"]), keys(&[b"a", b"b"])),
            (argv(&[b"RPOPLPUSH", b"a", b"b"]), keys(&[b"a", b"b"])),
            (argv(&[b"COPY", b"a", b"b"]), keys(&[b"a", b"b"])),
            (argv(&[b"LCS", b"a", b"b"]), keys(&[b"a", b"b"])),
            (
                argv(&[b"ZRANGESTORE", b"dst", b"src", b"0", b"-1"]),
                keys(&[b"dst", b"src"]),
            ),
            (
                argv(&[b"GEOSEARCHSTORE", b"dst", b"src", b"FROMMEMBER", b"m", b"BYRADIUS", b"1", b"m", b"ASC"]),
                keys(&[b"dst", b"src"]),
            ),
            // dest + all source keys (SINTERSTORE-class)
            (
                argv(&[b"SINTERSTORE", b"dst", b"k1", b"k2"]),
                keys(&[b"dst", b"k1", b"k2"]),
            ),
            (
                argv(&[b"SUNIONSTORE", b"dst", b"k1"]),
                keys(&[b"dst", b"k1"]),
            ),
            (
                argv(&[b"PFMERGE", b"dst", b"a", b"b"]),
                keys(&[b"dst", b"a", b"b"]),
            ),
            // BITOP op dest src...
            (
                argv(&[b"BITOP", b"AND", b"dst", b"a", b"b"]),
                keys(&[b"dst", b"a", b"b"]),
            ),
            // dest + numkeys-counted sources (ZUNIONSTORE-class)
            (
                argv(&[b"ZUNIONSTORE", b"dst", b"2", b"k1", b"k2"]),
                keys(&[b"dst", b"k1", b"k2"]),
            ),
            (
                argv(&[b"ZINTERSTORE", b"dst", b"1", b"k1", b"WEIGHTS", b"3"]),
                keys(&[b"dst", b"k1"]),
            ),
            // numkeys-counted keys (LMPOP/ZMPOP/ZUNION-class)
            (
                argv(&[b"LMPOP", b"2", b"k1", b"k2", b"LEFT"]),
                keys(&[b"k1", b"k2"]),
            ),
            (
                argv(&[b"ZMPOP", b"1", b"z", b"MIN"]),
                keys(&[b"z"]),
            ),
            (
                argv(&[b"SINTERCARD", b"2", b"k1", b"k2"]),
                keys(&[b"k1", b"k2"]),
            ),
            (
                argv(&[b"ZINTERCARD", b"2", b"z1", b"z2"]),
                keys(&[b"z1", b"z2"]),
            ),
            (
                argv(&[b"ZUNION", b"2", b"z1", b"z2", b"WITHSCORES"]),
                keys(&[b"z1", b"z2"]),
            ),
            (
                argv(&[b"ZDIFF", b"2", b"z1", b"z2"]),
                keys(&[b"z1", b"z2"]),
            ),
            // GEORADIUS source + optional STORE destination
            (
                argv(&[b"GEORADIUS", b"src", b"15", b"37", b"200", b"km"]),
                keys(&[b"src"]),
            ),
            (
                argv(&[b"GEORADIUS", b"src", b"15", b"37", b"200", b"km", b"STORE", b"dst"]),
                keys(&[b"src", b"dst"]),
            ),
            (
                argv(&[b"GEORADIUSBYMEMBER", b"src", b"m", b"200", b"km", b"STOREDIST", b"dd"]),
                keys(&[b"src", b"dd"]),
            ),
            (
                argv(&[b"GEOSEARCH", b"src", b"FROMMEMBER", b"m", b"BYRADIUS", b"1", b"km", b"ASC"]),
                keys(&[b"src"]),
            ),
            // XREAD / XREADGROUP split on STREAMS
            (
                argv(&[b"XREAD", b"COUNT", b"2", b"STREAMS", b"s1", b"s2", b"0", b"0"]),
                keys(&[b"s1", b"s2"]),
            ),
            (
                argv(&[b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s1", b">"]),
                keys(&[b"s1"]),
            ),
            // FullKeyspace: enumeration
            (argv(&[b"SCAN", b"0"]), full.clone()),
            (argv(&[b"KEYS", b"*"]), full.clone()),
            (argv(&[b"FLUSHALL"]), full.clone()),
            (argv(&[b"RANDOMKEY"]), full.clone()),
            (argv(&[b"DBSIZE"]), full.clone()),
            // FullKeyspace: dynamic keys
            (argv(&[b"SORT", b"l", b"BY", b"w_*", b"GET", b"d_*"]), full.clone()),
            (argv(&[b"SORT_RO", b"l"]), full.clone()),
            (argv(&[b"EVAL", b"return 1", b"1", b"k"]), full.clone()),
            (argv(&[b"EVALSHA", b"abc", b"1", b"k"]), full.clone()),
            (argv(&[b"EVAL_RO", b"return 1", b"0"]), full.clone()),
            (argv(&[b"EVALSHA_RO", b"abc", b"0"]), full.clone()),
            // transaction control verbs touch no data keys
            (argv(&[b"MULTI"]), none.clone()),
            (argv(&[b"EXEC"]), none.clone()),
            (argv(&[b"DISCARD"]), none.clone()),
            (argv(&[b"UNWATCH"]), none.clone()),
            // connection / non-data
            (argv(&[b"PING"]), none.clone()),
            (argv(&[b"ECHO", b"hi"]), none.clone()),
            (argv(&[b"SCRIPT", b"LOAD", b"return 1"]), none.clone()),
        ];

        for (cmd, expected) in cases {
            let got = command_keys(&cmd);
            assert_eq!(
                got,
                expected,
                "command_keys mismatch for {:?}",
                cmd.iter()
                    .map(|a| String::from_utf8_lossy(a).into_owned())
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn command_keys_merge_unions_and_degrades() {
        // Union of Keys accumulates (duplicates preserved for the loader to dedup).
        let merged = command_keys(&argv(&[b"GET", b"a"]))
            .merge(command_keys(&argv(&[b"SET", b"b", b"1"])));
        assert_eq!(merged, keys(&[b"a", b"b"]));
        // A FullKeyspace member degrades the whole union.
        let degraded = command_keys(&argv(&[b"GET", b"a"]))
            .merge(command_keys(&argv(&[b"KEYS", b"*"])));
        assert_eq!(degraded, KeyAccess::FullKeyspace);
    }

    #[test]
    fn command_keys_empty_argv_is_no_keys() {
        assert_eq!(command_keys(&[]), KeyAccess::Keys(Vec::new()));
    }
}
