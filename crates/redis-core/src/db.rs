//! `RedisDb` — one logical database (keyspace) and C-level DB API.
//!
//! Translation of `src/db.c` (~2850 lines, ~80 functions).
//!
//! Phase-A model:
//!   - `dict`    → `HashMap<RedisString, RedisObject>`.
//!   - Expiry    → stored in `RedisObject.expire` per `object.rs`; mirrors C robj embed.
//!     A secondary `db->expires` index for active-expiry scanning is deferred to Phase 4.
//!   - kvstore / cluster-slot routing → TODO(port): deferred to Phase 4.
//!   - `blocking_keys`, `ready_keys`, `watched_keys` → stub HashMaps (Phase 3 / Phase 5).
//!   - Lazy-free (async delete) → synchronous in Phase A; TODO(port) marked.
//!   - `signalModifiedKey`, `notifyKeyspaceEvent`, `trackingInvalidateKey` → no-ops in Phase A.
//!
//! PORT NOTE: C embeds the key inside the robj value (hasembkey). Rust uses only the
//! HashMap key — the object does not carry a copy of its own key.
//!
//! PORT NOTE: `ctx.db()` / `ctx.db_mut()` are expected to be added to `CommandContext`
//! in Phase 3 when `&mut RedisServer` is wired in. All commands that reach db state are
//! annotated TODO(port) at those call sites; the name-resolution errors are expected
//! Phase-A failures.
//!
//! PORT NOTE: C stringmatchlen (util.c) is replaced by the local `glob_match` helper,
//! which handles `*` and `?` but not `[...]` character classes yet.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_types::{RedisError, RedisString};

use crate::client::ClientId;
use crate::command_context::CommandContext;
use crate::object::{ObjectKind, RedisObject, EXPIRY_NONE};

/// Global cross-connection MULTI/WATCH support.
///
/// `watched`: every WATCH-registered key maps to the set of client ids that
/// asked to be notified. `dirty`: every client id whose watched set has been
/// touched since the last EXEC. WATCH adds to `watched`; UNWATCH/EXEC clears
/// the client from `watched`; `set_key`/`sync_delete` adds to `dirty`; EXEC
/// reads-and-clears `dirty` for its own client id.
///
/// PORT NOTE: deliberate architectural shortcut for Phase B. Real Redis stores
/// the per-key watcher list on `serverDb.watched_keys` and mutates each
/// watching `client` directly via the global client list. Until `RedisServer`
/// owns the client list and is reachable from `db.rs::set_key`, this global
/// `OnceLock` carries the same information. Initialise from `main.rs` startup.
#[derive(Debug, Default)]
pub struct WatchedKeysIndex {
    pub watched: HashMap<RedisString, HashSet<ClientId>>,
    pub dirty: HashSet<ClientId>,
}

static WATCHED_KEYS_INDEX: OnceLock<Arc<Mutex<WatchedKeysIndex>>> = OnceLock::new();

/// Install or fetch the global watched-keys index.
///
/// First caller installs an empty index; subsequent callers receive the same
/// `Arc`. Safe to call from the binary entry point and from per-command
/// handlers without synchronisation concerns beyond the inner `Mutex`.
pub fn watched_keys_index() -> &'static Arc<Mutex<WatchedKeysIndex>> {
    WATCHED_KEYS_INDEX.get_or_init(|| Arc::new(Mutex::new(WatchedKeysIndex::default())))
}

/// Register `client_id` as a watcher of `key` in the global index.
pub fn watched_keys_index_add(key: &RedisString, client_id: ClientId) {
    let idx = watched_keys_index();
    let mut guard = match idx.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.watched.entry(key.clone()).or_default().insert(client_id);
}

/// Remove `client_id` from every watch list.
pub fn watched_keys_index_remove_client(client_id: ClientId) {
    let idx = watched_keys_index();
    let mut guard = match idx.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.watched.retain(|_, watchers| {
        watchers.remove(&client_id);
        !watchers.is_empty()
    });
}

/// Mark every client watching `key` as dirty.
///
/// C: db.c → multi.c::touchWatchedKey. Called after every write to `key`.
pub fn watched_keys_touch(key: &RedisString) {
    let idx = watched_keys_index();
    let mut guard = match idx.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let watchers = match guard.watched.get(key) {
        Some(s) if !s.is_empty() => s.clone(),
        _ => return,
    };
    for cid in watchers {
        guard.dirty.insert(cid);
    }
}

/// Return `true` and clear the dirty flag if `client_id` was marked dirty.
pub fn watched_keys_take_dirty(client_id: ClientId) -> bool {
    let idx = watched_keys_index();
    let mut guard = match idx.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.dirty.remove(&client_id)
}

// ─────────────────────────────────────────────────────────────────────────────
// Lookup flags  (C: server.h LOOKUP_*)
// ─────────────────────────────────────────────────────────────────────────────

pub const LOOKUP_NONE: u32     = 0;
pub const LOOKUP_NOTOUCH: u32  = 1 << 0;
pub const LOOKUP_NONOTIFY: u32 = 1 << 1;
pub const LOOKUP_NOSTATS: u32  = 1 << 2;
pub const LOOKUP_WRITE: u32    = 1 << 3;
pub const LOOKUP_NOEXPIRE: u32 = 1 << 4;

pub const EXPIRE_FORCE_DELETE_EXPIRED: u32 = 1 << 0;
pub const EXPIRE_AVOID_DELETE_EXPIRED: u32 = 1 << 1;

pub const SETKEY_KEEPTTL: u32       = 1 << 0;
pub const SETKEY_NO_SIGNAL: u32     = 1 << 1;
pub const SETKEY_ALREADY_EXIST: u32 = 1 << 2;
pub const SETKEY_DOESNT_EXIST: u32  = 1 << 3;
pub const SETKEY_ADD_OR_UPDATE: u32 = 1 << 4;

pub const EMPTYDB_NO_FLAGS: u32    = 0;
pub const EMPTYDB_ASYNC: u32       = 1 << 0;
pub const EMPTYDB_NOFUNCTIONS: u32 = 1 << 1;

pub const DB_FLAG_KEY_DELETED: u32   = 1 << 0;
pub const DB_FLAG_KEY_EXPIRED: u32   = 1 << 1;
pub const DB_FLAG_KEY_OVERWRITE: u32 = 1 << 2;

const DEFAULT_SCAN_COUNT: i64 = 10;

// ─────────────────────────────────────────────────────────────────────────────
// KeyStatus  (C: keyStatus in server.h)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum KeyStatus {
    Valid,
    Expired,
    Deleted,
}

// ─────────────────────────────────────────────────────────────────────────────
// ScanOptions  (C: scanOptions struct, db.c:1151)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub count: i64,
    /// Pattern bytes, or `None` for '*' (match all).
    pub pat: Option<Vec<u8>>,
    pub use_pattern: bool,
    /// Slot derived from pattern hashtag in cluster mode; -1 if none.
    pub match_slot: i32,
    /// Object type filter; `i64::MAX` means no filter.
    pub type_filter: i64,
    /// Explicit slot from SLOT option (CLUSTERSCAN only); -1 if none.
    pub input_slot: i32,
    /// Return only keys, not field+value pairs (NOVALUES / NOSCORES).
    pub only_keys: bool,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            count: DEFAULT_SCAN_COUNT,
            pat: None,
            use_pattern: false,
            match_slot: -1,
            type_filter: i64::MAX,
            input_slot: -1,
            only_keys: false,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// KeyReference + GetKeysResult  (C: keyReference / getKeysResult, server.h)
// ─────────────────────────────────────────────────────────────────────────────

/// Position + access-flags for one key argument inside a command's argv[].
#[derive(Debug, Clone, Copy, Default)]
pub struct KeyReference {
    pub pos: i32,
    pub flags: i64,
}

/// Heap-backed result of `get_keys_from_command` and family.
///
/// PORT NOTE: C uses a 16-slot on-stack buffer to avoid heap allocation for
/// most commands. Phase A uses `Vec`; revisit in Phase B if profiling shows cost.
#[derive(Debug, Default, Clone)]
pub struct GetKeysResult {
    pub keys: Vec<KeyReference>,
}

impl GetKeysResult {
    pub fn new() -> Self {
        Self { keys: Vec::with_capacity(16) }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ScanData  (C: scanData typedef, db.c:983)
// ─────────────────────────────────────────────────────────────────────────────

/// Accumulator passed to hashtable scan callbacks during SCAN / HSCAN / SSCAN / ZSCAN.
///
/// TODO(port): full implementation requires kvstore / vector / listpack / skiplist
/// integration — deferred to Phase 4.
#[derive(Debug, Default)]
pub struct ScanData {
    /// Collected (key, optional_value) byte pairs.
    pub result: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub db_id: u32,
    /// `i64::MAX` = no filter.
    pub type_filter: i64,
    pub pattern: Option<Vec<u8>>,
    pub sampled: i64,
    pub only_keys: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// RedisDb struct  (C: serverDb in server.h)
// ─────────────────────────────────────────────────────────────────────────────

/// One logical Redis database (keyspace).
///
/// Phase-A implementation: HashMap-backed. kvstore, cluster-slot addressing,
/// and secondary expires dict land in Phase 4.
#[derive(Debug, Default)]
pub struct RedisDb {
    /// Database index.
    pub id: u32,

    /// Main keyspace. C: serverDb.keys (kvstore).
    dict: HashMap<RedisString, RedisObject>,

    /// Keys with blocking clients (BLPOP / BRPOP / XREADGROUP).
    /// C: serverDb.blocking_keys — TODO(port): deferred to Phase 3.
    blocking_keys: HashMap<RedisString, ()>,

    /// Keys signalled as ready to unblock a waiting client.
    /// C: serverDb.ready_keys — TODO(port): deferred to Phase 3.
    ready_keys: HashSet<RedisString>,

    /// Keys being WATCHed by MULTI/EXEC clients.
    /// C: serverDb.watched_keys — TODO(port): deferred to Phase 5.
    watched_keys: HashMap<RedisString, ()>,

    /// Average TTL for INFO keyspace stats.
    /// C: serverDb.avg_ttl — TODO(port): active-expiry cycle (Phase 3).
    pub avg_ttl: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// impl RedisDb
// ─────────────────────────────────────────────────────────────────────────────

impl RedisDb {
    pub fn new(id: u32) -> Self {
        Self { id, ..Default::default() }
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    /// Wall-clock time in milliseconds since the Unix epoch.
    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    /// True if the key exists and its TTL has elapsed.
    ///
    /// C: db.c:2122 objectIsExpired / keyIsExpiredWithDictIndexImpl
    pub fn is_expired(&self, key: &RedisString) -> bool {
        // TODO(port): return false when server.loading is true
        match self.dict.get(key) {
            None => false,
            Some(obj) => obj.expire != EXPIRY_NONE && obj.expire < Self::now_ms(),
        }
    }

    /// Check and optionally delete an expired key. Returns the new `KeyStatus`.
    ///
    /// C: db.c:2157 expireIfNeededWithDictIndex
    pub fn expire_if_needed(&mut self, key: &RedisString, flags: u32) -> KeyStatus {
        if !self.is_expired(key) {
            return KeyStatus::Valid;
        }
        if flags & EXPIRE_AVOID_DELETE_EXPIRED != 0 {
            return KeyStatus::Expired;
        }
        // TODO(port): EXPIRE_FORCE_DELETE_EXPIRED — check replica mode before deleting
        // TODO(port): notifyKeyspaceEvent(NOTIFY_EXPIRED, "expired", keyobj, db->id)
        // TODO(port): signalModifiedKey(NULL, db, keyobj)
        // TODO(port): propagateDeletion to AOF + replicas
        // TODO(port): server.stat_expiredkeys++
        self.dict.remove(key);
        KeyStatus::Deleted
    }

    // ── Lookup API ──────────────────────────────────────────────────────────

    /// General-purpose key lookup with flags.
    ///
    /// C: db.c:80 lookupKey
    pub fn lookup_key(&mut self, key: &RedisString, _flags: u32) -> Option<&RedisObject> {
        // TODO(port): LRU touch (unless LOOKUP_NOTOUCH or active child process)
        // TODO(port): keyspace hit / miss counters (unless LOOKUP_NOSTATS | LOOKUP_WRITE)
        // TODO(port): keymiss keyspace notification (unless LOOKUP_NONOTIFY | LOOKUP_WRITE)
        if self.expire_if_needed(key, 0) != KeyStatus::Valid {
            return None;
        }
        self.dict.get(key)
    }

    /// Read-oriented lookup. Asserts no LOOKUP_WRITE flag.
    ///
    /// C: db.c:136 lookupKeyReadWithFlags
    pub fn lookup_key_read_with_flags(&mut self, key: &RedisString, flags: u32) -> Option<&RedisObject> {
        debug_assert!(flags & LOOKUP_WRITE == 0);
        self.lookup_key(key, flags)
    }

    /// Convenience read lookup with no flags.
    ///
    /// C: db.c:143 lookupKeyRead
    ///
    /// MIGRATION SHIM: the original C function (and the full-port version)
    /// takes `&mut self` because reads touch LRU and may lazy-delete expired
    /// keys. The architect-stub variant accepted `impl AsRef<[u8]>` and was
    /// `&self`. To keep all 30+ call sites compiling, the back-compat layer
    /// makes this `&self` over `impl AsRef<[u8]>` and skips both the LRU
    /// touch and the expire-if-needed cycle. Callers who want either should
    /// use `lookup_key_read_with_flags` (mutable, LRU-touching).
    pub fn lookup_key_read(&self, key: impl AsRef<[u8]>) -> Option<&RedisObject> {
        let k = RedisString::from_bytes(key.as_ref());
        self.find(&k)
    }

    /// Write-oriented lookup — may force-delete an expired key.
    ///
    /// C: db.c:153 lookupKeyWriteWithFlags
    pub fn lookup_key_write_with_flags(&mut self, key: &RedisString, flags: u32) -> Option<&mut RedisObject> {
        if self.expire_if_needed(key, EXPIRE_FORCE_DELETE_EXPIRED | flags) != KeyStatus::Valid {
            return None;
        }
        self.dict.get_mut(key)
    }

    /// Write-oriented lookup with no extra flags.
    ///
    /// C: db.c:157 lookupKeyWrite
    pub fn lookup_key_write(&mut self, key: &RedisString) -> Option<&mut RedisObject> {
        self.lookup_key_write_with_flags(key, LOOKUP_NONE)
    }

    // ── Key add / set / replace ─────────────────────────────────────────────

    /// Insert a key that must not already exist (debug-asserts in dev builds).
    ///
    /// C: db.c:227 dbAdd
    pub fn add(&mut self, key: RedisString, value: RedisObject) {
        debug_assert!(!self.dict.contains_key(&key), "dbAdd: key already exists");
        // TODO(port): signalKeyAsReady(db, key, val->type)
        // TODO(port): notifyKeyspaceEvent(NOTIFY_NEW, "new", key, db->id)
        self.dict.insert(key, value);
    }

    /// Insert-or-overwrite with no preconditions. Returns the previous value if any.
    ///
    /// PORT NOTE: covers both insert (dbAdd path) and overwrite (dbSetValue path)
    /// depending on caller intent. Use `set_key` for the high-level interface.
    pub fn insert(&mut self, key: RedisString, value: RedisObject) -> Option<RedisObject> {
        // TODO(port): moduleNotifyKeyUnlink / signalDeletedKeyAsReady on overwrite
        // TODO(port): initObjectLRUOrLFU on insert
        self.dict.insert(key, value)
    }

    /// High-level key setter: insert or overwrite, handle TTL + signals.
    ///
    /// C: db.c:417 setKey
    pub fn set_key(&mut self, key: RedisString, mut value: RedisObject, flags: u32) {
        let preserved_expire = if flags & SETKEY_KEEPTTL != 0 {
            self.dict.get(&key).map(|o| o.expire).unwrap_or(EXPIRY_NONE)
        } else {
            EXPIRY_NONE
        };
        value.expire = preserved_expire;
        self.dict.insert(key.clone(), value);
        if flags & SETKEY_NO_SIGNAL == 0 {
            self.signal_modified(&key);
        }
    }

    /// Replace a key's value without touching its expiry or LRU.
    ///
    /// C: db.c:397 dbReplaceValue (→ dbSetValue with overwrite=false)
    pub fn replace_value(&mut self, key: &RedisString, mut value: RedisObject) {
        // TODO(port): lazy-free of old object via tryOffloadFreeObjToIOThreads
        if let Some(old) = self.dict.get(key) {
            value.expire = old.expire;
            value.lru = old.lru;
        }
        self.dict.insert(key.clone(), value);
    }

    /// Ensure the string at `key` is mutable (raw encoding, not shared).
    ///
    /// C: db.c:583 dbUnshareStringValue
    pub fn unshare_string_value(&mut self, key: &RedisString) -> Option<&mut RedisObject> {
        // TODO(port): getDecodedObject / createRawStringObject / dbReplaceValue
        //             if encoding == EMBSTR or refcount > 1 (Phase 4 encoding work)
        self.dict.get_mut(key)
    }

    // ── Delete ──────────────────────────────────────────────────────────────

    /// Synchronous delete; returns true if the key existed.
    ///
    /// C: db.c:540 dbSyncDelete
    pub fn sync_delete(&mut self, key: &RedisString) -> bool {
        // TODO(port): moduleNotifyKeyUnlink(key, val, db->id, flags)
        // TODO(port): signalDeletedKeyAsReady(db, key, val->type)
        // TODO(port): dbUntrackKeyWithVolatileItems if OBJ_HASH with volatile fields
        let existed = self.dict.remove(key).is_some();
        if existed {
            watched_keys_touch(key);
        }
        existed
    }

    /// Async delete (lazy-free). Phase A falls through to synchronous delete.
    ///
    /// C: db.c:546 dbAsyncDelete
    /// PERF(port): freeObjAsync — background deallocation deferred to Phase 3.
    pub fn async_delete(&mut self, key: &RedisString) -> bool {
        // TODO(port): freeObjAsync when server.lazyfree_lazy_server_del is set
        self.sync_delete(key)
    }

    /// Delete using the server's lazyfree setting.
    ///
    /// C: db.c:552 dbDelete
    pub fn delete(&mut self, key: &RedisString) -> bool {
        // TODO(port): choose sync vs async via server.lazyfree_lazy_server_del
        self.sync_delete(key)
    }

    // ── Expiry ──────────────────────────────────────────────────────────────

    /// Remove the TTL from an existing key (make it persistent).
    ///
    /// C: db.c:1891 removeExpire
    pub fn remove_expire(&mut self, key: &RedisString) -> bool {
        if let Some(obj) = self.dict.get_mut(key) {
            if obj.expire != EXPIRY_NONE {
                obj.expire = EXPIRY_NONE;
                return true;
            }
        }
        false
    }

    /// Set the absolute expiry timestamp (ms since epoch) for an existing key.
    ///
    /// C: db.c:1911 setExpire
    pub fn set_expire(&mut self, key: &RedisString, when: i64) {
        // TODO(port): rememberReplicaKeyWithExpire on writable replica
        if let Some(obj) = self.dict.get_mut(key) {
            obj.expire = when;
        }
    }

    /// Return the expiry timestamp (ms) or `EXPIRY_NONE` (-1) if no TTL.
    ///
    /// C: db.c:1964 getExpire
    pub fn get_expire(&self, key: &RedisString) -> i64 {
        self.dict.get(key).map(|o| o.expire).unwrap_or(EXPIRY_NONE)
    }

    /// True if the key has an elapsed TTL.
    ///
    /// C: db.c:2151 keyIsExpired
    pub fn key_is_expired(&self, key: &RedisString) -> bool {
        self.is_expired(key)
    }

    // ── Misc DB operations ──────────────────────────────────────────────────

    /// Remove all keys.
    ///
    /// C: db.c:601 emptyDbStructure (single-db path)
    pub fn clear(&mut self) {
        // TODO(port): emptyDbAsync, kvstoreEmpty callback, resetDbExpiryState
        let watched_keys: Vec<RedisString> = self.dict.keys().cloned().collect();
        self.dict.clear();
        self.avg_ttl = 0;
        for k in &watched_keys {
            watched_keys_touch(k);
        }
    }

    /// Raw (no expiry check) key lookup. Used by internal scans.
    ///
    /// C: db.c:2271 dbFind
    pub fn find(&self, key: &RedisString) -> Option<&RedisObject> {
        self.dict.get(key)
    }

    /// True if the key is in the dict regardless of TTL.
    pub fn exists_raw(&self, key: &RedisString) -> bool {
        self.dict.contains_key(key)
    }

    /// True if the key is present and not expired.
    pub fn exists(&mut self, key: &RedisString) -> bool {
        self.lookup_key_read_with_flags(key, LOOKUP_NOTOUCH).is_some()
    }

    /// Number of keys including logically-expired ones not yet lazily removed.
    ///
    /// C: db.c:2287 dbSize
    pub fn size(&self) -> u64 {
        self.dict.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }

    /// Return keys matching `pattern` that are not expired (immutable, no TTL removal).
    ///
    /// TODO(port): cluster slot filtering (patternHashSlot); active-expiry during scan.
    pub fn matching_keys(&self, pattern: &[u8]) -> Vec<RedisString> {
        let all = pattern == b"*";
        let now = Self::now_ms();
        self.dict
            .iter()
            .filter(|(_, obj)| obj.expire == EXPIRY_NONE || obj.expire >= now)
            .filter(|(k, _)| all || glob_match(pattern, k.as_bytes()))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Return a stable snapshot of every live (non-expired) key paired with
    /// its `TYPE` byte-string name (`string`, `list`, `hash`, `set`, `zset`,
    /// `stream`, `none`).
    ///
    /// Used by the linear-cursor SCAN implementation in `scan_command`; the
    /// iteration order is whatever the underlying `HashMap` yields and is
    /// only stable within a single mutation-free window. Real Redis hashes
    /// the cursor for resize safety — see the TODO in `scan_command` for
    /// the deferred parity work.
    pub fn keys_snapshot_with_types(&self) -> Vec<(RedisString, &'static [u8])> {
        let now = Self::now_ms();
        self.dict
            .iter()
            .filter(|(_, obj)| obj.expire == EXPIRY_NONE || obj.expire >= now)
            .map(|(k, obj)| (k.clone(), object_kind_name(&obj.kind)))
            .collect()
    }

    /// Return a random key that is not expired.
    ///
    /// C: db.c:442 dbRandomKey
    /// PERF(port): O(n) HashMap walk — replace with fair random kvstore entry in Phase 4.
    pub fn random_key(&self) -> Option<RedisString> {
        // TODO(port): kvstoreGetFairRandomHashtableIndex / kvstoreHashtableFairRandomEntry
        // TODO(port): use a proper random source (server.random or `rand` crate)
        let now = Self::now_ms();
        self.dict
            .iter()
            .find(|(_, obj)| obj.expire == EXPIRY_NONE || obj.expire >= now)
            .map(|(k, _)| k.clone())
    }

    /// Swap keyspace contents with `other`. blocking/ready/watched stay in place.
    ///
    /// C: db.c:1769 dbSwapDatabases (inner per-db swap)
    pub fn swap_contents_with(&mut self, other: &mut RedisDb) {
        // TODO(port): touchAllWatchedKeysInDb(self, other) before swap
        // TODO(port): scanDatabaseForDeletedKeys(self, other) — XREADGROUP unblocking
        // TODO(port): scanDatabaseForReadyKeys(self) after swap — BLPOP/BRPOP unblocking
        std::mem::swap(&mut self.dict, &mut other.dict);
        std::mem::swap(&mut self.avg_ttl, &mut other.avg_ttl);
    }

    // ── Signal hooks (Phase A stubs) ────────────────────────────────────────

    /// Invalidate WATCH state and client-tracking for a modified key.
    ///
    /// C: db.c:754 signalModifiedKey
    ///
    /// MIGRATION SHIM: accepts anything that views as bytes (the architect
    /// stub took `impl AsRef<[u8]>`) so callers passing `&RedisString`,
    /// `&Vec<u8>`, or `&[u8]` all compile. Notifies every WATCH watcher of
    /// `key` via the global watched-keys index (see [`watched_keys_index`]).
    pub fn signal_modified(&self, key: impl AsRef<[u8]>) {
        // TODO(port): trackingInvalidateKey(c, key, 1)
        let k = RedisString::from_bytes(key.as_ref());
        watched_keys_touch(&k);
    }

    // ── Migration shims for the architect stub ──────────────────────────────

    /// Database id as `i32` (matches the C `redisDb.id` type used by callers).
    pub fn id(&self) -> i32 {
        self.id as i32
    }

    /// Number of keys (alias of [`size`] as `usize`).
    ///
    /// MIGRATION SHIM: the architect-stub `len()` returned `usize`; we keep it
    /// here for callers that haven't switched to `size()` yet.
    pub fn len(&self) -> usize {
        self.size() as usize
    }

    /// Byte-keyed delete shim that accepts `impl AsRef<[u8]>`.
    ///
    /// MIGRATION SHIM: the architect stub had `delete(impl AsRef<[u8]>)`;
    /// some callers still pass `&Vec<u8>`. The full-port `delete(&RedisString)`
    /// already covers `&RedisString`. This sibling method handles the byte
    /// path via a temporary `RedisString::from_bytes`.
    pub fn delete_by_bytes(&mut self, key: impl AsRef<[u8]>) -> bool {
        let k = RedisString::from_bytes(key.as_ref());
        self.delete(&k)
    }

    /// Whether `key` has an active expiry that is already in the past.
    ///
    /// MIGRATION SHIM: the architect-stub variant took `&RedisObject` (the
    /// caller hadn't extracted a key string yet). The new helper extracts the
    /// string-payload bytes (if any) and reuses [`key_is_expired`]; returns
    /// `false` for non-string `key` arguments.
    pub fn key_is_expired_obj(&self, key: &RedisObject) -> bool {
        match key.as_string_bytes() {
            Some(bytes) => self.key_is_expired(&RedisString::from_bytes(bytes)),
            None => false,
        }
    }

    /// True if no key in this db is currently WATCHed by any client.
    ///
    /// MIGRATION SHIM: the architect stub kept the watched-keys map on
    /// `RedisDb`. Phase 3 will route this through MULTI/EXEC state; until
    /// then this returns `self.watched_keys.is_empty()`.
    pub fn watched_keys_is_empty(&self) -> bool {
        self.watched_keys.is_empty()
    }

    /// Register `client_id` as a watcher of `key` in this db.
    ///
    /// MIGRATION SHIM: the architect stub stored watcher lists on the db
    /// itself. The full port defers this to Phase 3; we record presence
    /// only so [`watched_keys_is_empty`] returns the expected answer.
    pub fn watched_keys_add_client(&mut self, key: &RedisObject, _client_id: crate::client::ClientId) {
        if let Some(bytes) = key.as_string_bytes() {
            self.watched_keys
                .entry(RedisString::from_bytes(bytes))
                .or_insert(());
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Free-standing DB helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Signal that a DB was flushed (invalidates WATCHes and client tracking).
///
/// C: db.c:759 signalFlushedDb
pub fn signal_flushed_db(_db_id: i32, _async_mode: bool) {
    // TODO(port): scanDatabaseForDeletedKeys(db, NULL)
    // TODO(port): touchAllWatchedKeysInDb(db, NULL)
    // TODO(port): trackingInvalidateKeysOnFlush(async)
}

/// Swap two databases by index (keyspace swap; blocking/watched state stays).
///
/// C: db.c:1769 dbSwapDatabases
pub fn db_swap_databases(
    server_dbs: &mut Vec<RedisDb>,
    id1: usize,
    id2: usize,
) -> Result<(), RedisError> {
    if id1 >= server_dbs.len() || id2 >= server_dbs.len() {
        return Err(RedisError::out_of_range());
    }
    if id1 == id2 {
        return Ok(());
    }
    let (a, b) = if id1 < id2 {
        let (lo, hi) = server_dbs.split_at_mut(id2);
        (&mut lo[id1], &mut hi[0])
    } else {
        let (lo, hi) = server_dbs.split_at_mut(id1);
        (&mut hi[0], &mut lo[id2])
    };
    a.swap_contents_with(b);
    Ok(())
}

/// Propagate a key deletion to replicas and AOF.
///
/// C: db.c:2021 propagateDeletion
pub fn propagate_deletion(_db_id: u32, _key: &RedisString, _lazy: bool, _slot: i32) {
    // TODO(port): alsoPropagate(db->id, argv[DEL/UNLINK + key], PROPAGATE_AOF|PROPAGATE_REPL, slot)
    // Replication + AOF: Phase 4+
}

/// Delete an expired key and propagate the implicit deletion.
///
/// C: db.c:1969 deleteExpiredKeyAndPropagate
pub fn delete_expired_key_and_propagate(db: &mut RedisDb, key: &RedisString) {
    // TODO(port): latencyStartMonitor / latencyEndMonitor for "expire-del"
    db.sync_delete(key);
    // TODO(port): notifyKeyspaceEvent(NOTIFY_EXPIRED, "expired", keyobj, db->id)
    // TODO(port): signalModifiedKey(NULL, db, keyobj)
    // TODO(port): propagateDeletion(db, keyobj, server.lazyfree_lazy_expire, dict_index)
    // TODO(port): server.stat_expiredkeys++
}

// ─────────────────────────────────────────────────────────────────────────────
// Key-space commands
// ─────────────────────────────────────────────────────────────────────────────

/// C: db.c:831 flushdbCommand — FLUSHDB [ASYNC|SYNC]
pub fn flushdb_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): parse ASYNC/SYNC flag from argv[1]
    // TODO(port): forceCommandPropagation(c, PROPAGATE_REPL|PROPAGATE_AOF)
    // TODO(port): server.dirty += emptyData(c->db->id, flags|EMPTYDB_NOFUNCTIONS, NULL)
    ctx.db_mut().clear();
    ctx.reply_simple_string(b"OK")
}

/// C: db.c:856 flushallCommand — FLUSHALL [ASYNC|SYNC]
pub fn flushall_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): emptyData(-1, flags, NULL) — flush all databases
    // TODO(port): killRDBChild / killSlotMigrationChild / rdbSave if saveparamslen > 0
    // TODO(port): forceCommandPropagation, moduleFireServerEvent FLUSHDB_START/END
    ctx.db_mut().clear();
    ctx.reply_simple_string(b"OK")
}

/// Common body for DEL and UNLINK.
///
/// C: db.c:871 delGenericCommand
fn del_generic_command(ctx: &mut CommandContext, lazy: bool) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    let mut num_deleted: i64 = 0;
    for j in 1..argc {
        let key = ctx.arg(j)?.clone();
        // TODO(port): expireIfNeeded(c->db, c->argv[j], NULL, 0) before delete
        let deleted = if lazy {
            ctx.db_mut().async_delete(&key)
        } else {
            ctx.db_mut().sync_delete(&key)
        };
        if deleted {
            // TODO(port): signalModifiedKey(c, c->db, c->argv[j])
            // TODO(port): notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, db->id)
            // TODO(port): server.dirty++
            num_deleted += 1;
        }
    }
    ctx.reply_integer(num_deleted)
}

/// C: db.c:887 delCommand — DEL key [key ...]
pub fn del_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): use server.lazyfree_lazy_user_del to pick lazy vs sync
    del_generic_command(ctx, false)
}

/// C: db.c:891 unlinkCommand — UNLINK key [key ...]
pub fn unlink_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    del_generic_command(ctx, true)
}

/// C: db.c:896 existsCommand — EXISTS key [key ...]
pub fn exists_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    let mut count: i64 = 0;
    for j in 1..argc {
        let key = ctx.arg(j)?.clone();
        if ctx.db_mut().lookup_key_read_with_flags(&key, LOOKUP_NOTOUCH).is_some() {
            count += 1;
        }
    }
    ctx.reply_integer(count)
}

/// C: db.c:907 selectCommand — SELECT index
pub fn select_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): getIntFromObjectOrReply(c, c->argv[1], &id, NULL)
    // TODO(port): selectDb(c, id) — multi-db support (server.dbnum, createDatabaseIfNeeded)
    let _ = ctx.arg(1)?;
    Err(RedisError::runtime(b"SELECT: TODO(port): multi-db not implemented in Phase A"))
}

/// C: db.c:924 randomkeyCommand — RANDOMKEY
pub fn randomkey_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    match ctx.db().random_key() {
        None => ctx.reply_null_bulk(),
        Some(key) => ctx.reply_bulk(key.as_bytes()),
    }
}

/// C: db.c:936 keysCommand — KEYS pattern
pub fn keys_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let pattern = ctx.arg(1)?.clone();
    // TODO(port): cluster slot filtering (patternHashSlot, clusterIsSlotImporting check)
    let matching = ctx.db().matching_keys(pattern.as_bytes());
    ctx.reply_array_header(matching.len())?;
    for key in &matching {
        ctx.reply_bulk(key.as_bytes())?;
    }
    Ok(())
}

/// Return the canonical `TYPE` reply byte-string for an `ObjectKind`.
///
/// Mirrors the dispatch table in `t_string.c`'s `typeCommand`; used by
/// SCAN's `TYPE` filter and by `keys_snapshot_with_types`.
pub fn object_kind_name(kind: &ObjectKind) -> &'static [u8] {
    match kind {
        ObjectKind::String(_) => b"string",
        ObjectKind::List(_) => b"list",
        ObjectKind::Hash(_) => b"hash",
        ObjectKind::Set(_) => b"set",
        ObjectKind::ZSet(_) => b"zset",
        ObjectKind::Stream(_) => b"stream",
        ObjectKind::Module => b"none",
    }
}

/// C: db.c:1402 scanCommand — SCAN cursor [MATCH pat] [COUNT n] [TYPE type]
///
/// Phase-B linear cursor: the cursor is a `u64` byte-offset into the
/// snapshot returned by `keys_snapshot_with_types`. Each call walks up to
/// `COUNT` entries (default `10`), applies any `MATCH` glob and `TYPE`
/// filter, and replies with the next cursor (or `0` on completion) plus
/// the matched key array. Pattern matching reuses `glob_match`.
///
/// TODO(port): resize-safe reverse-binary cursor mixing (db.c hashCursor)
/// once kvstore lands in Phase 4.
pub fn scan_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let cursor_str = ctx.arg(1)?.clone();
    let cursor = parse_u64_from_bytes(cursor_str.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR invalid cursor"))?;

    let argc = ctx.arg_count();
    let mut pattern: Option<Vec<u8>> = None;
    let mut count: i64 = DEFAULT_SCAN_COUNT;
    let mut type_filter: Option<Vec<u8>> = None;

    let mut j: usize = 2;
    while j < argc {
        let opt = ctx.arg(j)?.clone();
        let bytes = opt.as_bytes();
        if eq_ignore_ascii_case(bytes, b"MATCH") {
            if j + 1 >= argc {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            pattern = Some(ctx.arg(j + 1)?.as_bytes().to_vec());
            j += 2;
        } else if eq_ignore_ascii_case(bytes, b"COUNT") {
            if j + 1 >= argc {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            let n = parse_i64_from_bytes(ctx.arg(j + 1)?.as_bytes())
                .ok_or_else(|| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
            if n < 1 {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            count = n;
            j += 2;
        } else if eq_ignore_ascii_case(bytes, b"TYPE") {
            if j + 1 >= argc {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            type_filter = Some(ctx.arg(j + 1)?.as_bytes().to_vec());
            j += 2;
        } else {
            return Err(RedisError::runtime(b"ERR syntax error"));
        }
    }

    let snapshot = ctx.db().keys_snapshot_with_types();
    let total = snapshot.len() as u64;
    let start = cursor as usize;
    let stop = (start + count as usize).min(snapshot.len());
    let next_cursor: u64 = if stop as u64 >= total { 0 } else { stop as u64 };

    let mut matched: Vec<RedisString> = Vec::new();
    for (key, kind_name) in snapshot.into_iter().skip(start).take(count as usize) {
        if let Some(ref pat) = pattern {
            if !glob_match(pat, key.as_bytes()) {
                continue;
            }
        }
        if let Some(ref tf) = type_filter {
            if tf.as_slice() != kind_name {
                continue;
            }
        }
        matched.push(key);
    }

    ctx.reply_array_header(2usize)?;
    ctx.reply_bulk(next_cursor.to_string().as_bytes())?;
    ctx.reply_array_header(matched.len())?;
    for key in matched {
        ctx.reply_bulk_string(key)?;
    }
    Ok(())
}

/// Parse an unsigned decimal cursor from a byte slice.
fn parse_u64_from_bytes(b: &[u8]) -> Option<u64> {
    if b.is_empty() {
        return None;
    }
    let mut n: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((c - b'0') as u64)?;
    }
    Some(n)
}

/// C: db.c:1408 dbsizeCommand — DBSIZE
pub fn dbsize_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    ctx.reply_integer(ctx.db().size() as i64)
}

/// C: db.c:1412 lastsaveCommand — LASTSAVE
pub fn lastsave_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): server.lastsave
    ctx.reply_integer(0)
}

/// C: db.c:1416 typeCommand — TYPE key
pub fn type_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let key = ctx.arg(1)?.clone();
    let type_name: &[u8] = match ctx.db_mut().lookup_key_read_with_flags(&key, LOOKUP_NOTOUCH) {
        None => b"none",
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => b"string",
            ObjectKind::List(_)   => b"list",
            ObjectKind::Hash(_)   => b"hash",
            ObjectKind::Set(_)    => b"set",
            ObjectKind::ZSet(_)   => b"zset",
            ObjectKind::Stream(_) => b"stream",
            ObjectKind::Module    => b"none", // TODO(port): return module type name
        },
    };
    ctx.reply_simple_string(type_name)
}

/// C: db.c:1422 shutdownCommand — SHUTDOWN [[NOSAVE|SAVE] [NOW] [FORCE] [SAFE] | ABORT]
pub fn shutdown_command(_ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): full flag parsing (NOSAVE / SAVE / NOW / FORCE / ABORT / SAFE / FAILOVER)
    // TODO(port): blockClientShutdown, prepareForShutdown, abortShutdown, exit(0)
    Err(RedisError::runtime(b"SHUTDOWN: TODO(port): not implemented in Phase A"))
}

/// Common body for RENAME and RENAMENX.
///
/// C: db.c:1491 renameGenericCommand
fn rename_generic_command(ctx: &mut CommandContext, nx: bool) -> Result<(), RedisError> {
    let src_key = ctx.arg(1)?.clone();
    let dst_key = ctx.arg(2)?.clone();
    let same_key = src_key == dst_key;

    if ctx.db_mut().lookup_key_write(&src_key).is_none() {
        return Err(RedisError::runtime(b"ERR no such key"));
    }
    if same_key {
        return if nx { ctx.reply_integer(0) } else { ctx.reply_simple_string(b"OK") };
    }

    let dst_exists = ctx.db_mut().exists_raw(&dst_key);
    if dst_exists && nx {
        return ctx.reply_integer(0);
    }
    if dst_exists {
        ctx.db_mut().sync_delete(&dst_key);
    }

    let expire = ctx.db().get_expire(&src_key);
    let value = ctx.db_mut().dict.remove(&src_key);
    match value {
        None => return Err(RedisError::runtime(b"ERR no such key")),
        Some(mut obj) => {
            obj.expire = expire;
            ctx.db_mut().dict.insert(dst_key.clone(), obj);
        }
    }

    // TODO(port): signalModifiedKey(c, c->db, c->argv[1]) and c->argv[2]
    // TODO(port): notifyKeyspaceEvent "rename_from" and "rename_to"
    // TODO(port): server.dirty++

    if nx { ctx.reply_integer(1) } else { ctx.reply_simple_string(b"OK") }
}

/// C: db.c:1530 renameCommand — RENAME key newkey
pub fn rename_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    rename_generic_command(ctx, false)
}

/// C: db.c:1534 renamenxCommand — RENAMENX key newkey
pub fn renamenx_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    rename_generic_command(ctx, true)
}

/// C: db.c:1538 moveCommand — MOVE key db [REPLACE]
pub fn move_command(_ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): multi-db required (selectDb, server.dbnum, createDatabaseIfNeeded)
    Err(RedisError::runtime(b"MOVE: TODO(port): multi-db not implemented in Phase A"))
}

/// C: db.c:1611 copyCommand — COPY source destination [DB n] [REPLACE]
///
/// Phase-A semantics: a single logical DB is supported. The optional `DB n`
/// modifier is parsed and validated (must equal the current db id) but the
/// destination always lands in the active keyspace. The optional `REPLACE`
/// modifier overrides the "destination must not exist" check. Source TTL is
/// preserved on the duplicated value. Returns `:1` on success, `:0` if the
/// destination already exists and `REPLACE` was not supplied.
pub fn copy_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    let src_key = ctx.arg(1)?.clone();
    let dst_key = ctx.arg(2)?.clone();

    let mut replace = false;
    let mut target_db: Option<i64> = None;
    let mut j: usize = 3;
    while j < argc {
        let opt = ctx.arg(j)?.clone();
        let bytes = opt.as_bytes();
        if eq_ignore_ascii_case(bytes, b"REPLACE") {
            replace = true;
            j += 1;
        } else if eq_ignore_ascii_case(bytes, b"DB") {
            if j + 1 >= argc {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            let val_bytes = ctx.arg(j + 1)?.clone();
            let parsed = parse_i64_from_bytes(val_bytes.as_bytes())
                .ok_or_else(|| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
            target_db = Some(parsed);
            j += 2;
        } else {
            return Err(RedisError::runtime(b"ERR syntax error"));
        }
    }

    let current_db_id = ctx.db().id() as i64;
    if let Some(target) = target_db {
        if target != current_db_id {
            return Err(RedisError::runtime(b"ERR DB index is out of range"));
        }
    }

    if src_key == dst_key {
        return ctx.reply_integer(0);
    }

    let src_clone = match ctx.db_mut().lookup_key_read_with_flags(&src_key, LOOKUP_NOTOUCH) {
        None => return ctx.reply_integer(0),
        Some(obj) => obj.clone(),
    };

    if ctx.db_mut().exists_raw(&dst_key) && !replace {
        return ctx.reply_integer(0);
    }

    let expire = ctx.db().get_expire(&src_key);
    let mut new_obj = src_clone;
    new_obj.expire = expire;
    ctx.db_mut().insert(dst_key.clone(), new_obj);

    ctx.reply_integer(1)
}

/// C: db.c:1408 touchCommand — TOUCH key [key ...]
///
/// Returns the number of supplied keys that currently exist. Matches the
/// semantics of EXISTS for Phase A; the C implementation also bumps LRU/LFU
/// access info, which is deferred.
pub fn touch_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    let mut count: i64 = 0;
    for j in 1..argc {
        let key = ctx.arg(j)?.clone();
        if ctx.db_mut().lookup_key_read_with_flags(&key, LOOKUP_NONE).is_some() {
            count += 1;
        }
    }
    ctx.reply_integer(count)
}

/// Case-insensitive ASCII equality for command option keywords.
fn eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| x.to_ascii_lowercase() == y.to_ascii_lowercase())
}

/// C: db.c:1861 swapdbCommand — SWAPDB index index
pub fn swapdb_command(_ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): cluster mode check (SWAPDB disallowed in cluster mode)
    // TODO(port): getIntFromObjectOrReply for id1/id2; call db_swap_databases
    Err(RedisError::runtime(b"SWAPDB: TODO(port): multi-db not implemented in Phase A"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Key-extraction helpers  (C: db.c:2295 getKeysPrepareResult and family)
// ─────────────────────────────────────────────────────────────────────────────

/// Generic key-extraction for commands with a numkeys counter argument.
///
/// Used by ZUNION, ZUNIONSTORE, ZINTER, LMPOP, BLMPOP, etc.
///
/// C: db.c:2681 genericGetKeys
pub fn generic_get_keys(
    store_key_ofs: Option<i32>,
    key_count_ofs: usize,
    first_key_ofs: usize,
    key_step: usize,
    argv: &[RedisString],
    result: &mut GetKeysResult,
) -> Result<usize, RedisError> {
    // TODO(port): validate numkeys against argc; handle modules / negative arity
    let numkeys_bytes = argv.get(key_count_ofs).ok_or_else(RedisError::not_integer)?;
    let numkeys = parse_i64_from_bytes(numkeys_bytes.as_bytes())
        .filter(|&n| n >= 1)
        .ok_or_else(RedisError::not_integer)? as usize;

    result.keys.clear();
    for i in 0..numkeys {
        result.keys.push(KeyReference {
            pos: (first_key_ofs + i * key_step) as i32,
            flags: 0,
        });
    }
    if let Some(ofs) = store_key_ofs {
        result.keys.push(KeyReference { pos: ofs, flags: 0 });
    }
    Ok(result.keys.len())
}

/// C: db.c:2597 getKeysUsingLegacyRangeSpec — firstkey/lastkey/step extraction.
pub fn get_keys_using_legacy_range_spec(
    first: i32,
    last: i32,
    step: i32,
    argc: i32,
    result: &mut GetKeysResult,
) -> i32 {
    // TODO(port): KSPEC_BS_INVALID check (return 0 immediately if spec is invalid)
    if first < 0 || step < 1 {
        result.keys.clear();
        return 0;
    }
    let actual_last = if last >= 0 { last } else { argc + last };
    let mut count = 0_i32;
    let mut j = first;
    while j <= actual_last && j < argc {
        result.keys.push(KeyReference { pos: j, flags: 0 });
        count += 1;
        j += step;
    }
    count
}

// ─────────────────────────────────────────────────────────────────────────────
// Byte-level integer parser (avoids banned from_utf8 / String conversions)
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a decimal integer from a byte slice without UTF-8 conversion.
///
/// Returns `None` if the slice is empty, contains non-ASCII digits, or overflows.
fn parse_i64_from_bytes(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let (neg, digits) = if b[0] == b'-' { (true, &b[1..]) } else { (false, b) };
    if digits.is_empty() {
        return None;
    }
    let mut n: i64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((c - b'0') as i64)?;
    }
    if neg { Some(-n) } else { Some(n) }
}

// ─────────────────────────────────────────────────────────────────────────────
// Glob-style pattern matcher  (C: util.c stringmatchlen)
// ─────────────────────────────────────────────────────────────────────────────

/// Case-sensitive glob match over byte slices.
///
/// Supports `*` (any sequence of bytes) and `?` (any single byte).
/// TODO(port): implement `[abc]` / `[a-z]` character-class matching
///             once util.c (redis-core/src/util.rs) is ported.
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0usize;
    let mut ti = 0usize;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0usize;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            star_ti += 1;
            ti = star_ti;
            pi = star_pi + 1;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{ObjectKind, StringEncoding, EXPIRY_NONE};

    fn make_str_obj(s: &[u8]) -> RedisObject {
        RedisObject {
            lru: Default::default(),
            expire: EXPIRY_NONE,
            kind: ObjectKind::String(StringEncoding::Raw(RedisString::from_bytes(s))),
        }
    }

    fn k(s: &[u8]) -> RedisString {
        RedisString::from_bytes(s)
    }

    #[test]
    fn add_lookup_delete_round_trip() {
        let mut db = RedisDb::new(0);
        let key = k(b"foo");
        assert!(!db.exists_raw(&key));
        db.add(key.clone(), make_str_obj(b"bar"));
        assert!(db.exists_raw(&key));
        assert!(db.find(&key).is_some());
        assert!(db.sync_delete(&key));
        assert!(!db.exists_raw(&key));
    }

    #[test]
    fn expired_key_invisible_to_lookup() {
        let mut db = RedisDb::new(0);
        let key = k(b"expiring");
        let mut obj = make_str_obj(b"v");
        obj.expire = 1; // 1 ms since epoch — always in the past
        db.add(key.clone(), obj);
        assert!(db.lookup_key_read_with_flags(&key, LOOKUP_NONE).is_none());
        assert!(!db.exists_raw(&key), "expired key should be lazily removed");
    }

    #[test]
    fn non_expired_key_visible() {
        let mut db = RedisDb::new(0);
        let key = k(b"future");
        let mut obj = make_str_obj(b"v");
        obj.expire = i64::MAX; // far future
        db.add(key.clone(), obj);
        assert!(db.lookup_key_read_with_flags(&key, LOOKUP_NONE).is_some());
    }

    #[test]
    fn remove_expire_makes_key_persistent() {
        let mut db = RedisDb::new(0);
        let key = k(b"k");
        let mut obj = make_str_obj(b"v");
        obj.expire = i64::MAX;
        db.add(key.clone(), obj);
        assert_eq!(db.get_expire(&key), i64::MAX);
        assert!(db.remove_expire(&key));
        assert_eq!(db.get_expire(&key), EXPIRY_NONE);
    }

    #[test]
    fn clear_removes_all_keys() {
        let mut db = RedisDb::new(0);
        db.add(k(b"a"), make_str_obj(b"1"));
        db.add(k(b"b"), make_str_obj(b"2"));
        db.clear();
        assert!(db.is_empty());
        assert_eq!(db.size(), 0);
    }

    #[test]
    fn swap_contents_exchanges_keys() {
        let mut db1 = RedisDb::new(0);
        let mut db2 = RedisDb::new(1);
        db1.add(k(b"x"), make_str_obj(b"from-db1"));
        db2.add(k(b"y"), make_str_obj(b"from-db2"));
        db1.swap_contents_with(&mut db2);
        assert!(db1.exists_raw(&k(b"y")));
        assert!(!db1.exists_raw(&k(b"x")));
        assert!(db2.exists_raw(&k(b"x")));
        assert!(!db2.exists_raw(&k(b"y")));
    }

    #[test]
    fn glob_match_basic() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"foo*", b"foobar"));
        assert!(!glob_match(b"foo*", b"barfoo"));
        assert!(glob_match(b"f?o", b"foo"));
        assert!(!glob_match(b"f?o", b"fo"));
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"a", b""));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h*llo", b"hllo"));
        assert!(glob_match(b"h*llo", b"heeeello"));
    }

    #[test]
    fn parse_i64_from_bytes_cases() {
        assert_eq!(parse_i64_from_bytes(b"42"), Some(42));
        assert_eq!(parse_i64_from_bytes(b"-7"), Some(-7));
        assert_eq!(parse_i64_from_bytes(b"0"), Some(0));
        assert_eq!(parse_i64_from_bytes(b""), None);
        assert_eq!(parse_i64_from_bytes(b"abc"), None);
        assert_eq!(parse_i64_from_bytes(b"-"), None);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/db.c  (~2850 lines, ~80 functions)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         86
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         Core lookup/add/delete/expiry/set_key/swap translated faithfully.
//                  Commands stubbed with ctx.db_mut() calls (expected name-resolution
//                  errors — Phase 3 wires CommandContext to RedisServer).
//                  kvstore/cluster slots, lazy-free, SCAN cursor, multi-db
//                  (SELECT/MOVE/COPY/SWAPDB), keyspace notifications, and
//                  replication propagation all carry TODO(port) and are deferred
//                  to Phase 3 (keyspace events, blocking) and Phase 4 (kvstore).
//                  Validator shows only expected E0432/E0282 name-resolution errors;
//                  zero real syntax errors.
// ──────────────────────────────────────────────────────────────────────────
