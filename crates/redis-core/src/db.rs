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
//! PORT NOTE: `ctx.db()` / `ctx.db_mut()` now expose the selected DB, and
//! `CommandContext` carries a DB-list route for staged cross-DB migration.
//! Cross-DB commands use that route instead of naming the transitional global
//! database store directly.
//!
//! PORT NOTE: C stringmatchlen (util.c) is exposed through the local
//! `glob_match` wrapper so db.c call sites stay source-shaped while sharing
//! the faithful util.c implementation.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_types::{RedisError, RedisString};

use crate::client::ClientId;
use crate::command_context::CommandContext;
use crate::live_config::LiveConfig;
use crate::metrics::server_metrics;
use crate::notify::{NOTIFY_EXPIRED, NOTIFY_GENERIC, NOTIFY_KEYEVENT, NOTIFY_KEYSPACE};
use crate::object::{ObjectKind, RedisObject, EXPIRY_NONE};
use crate::pubsub_registry::PubSubRegistry;

/// Global cross-connection MULTI/WATCH support.
///
/// `watched`: every WATCH-registered `(db_id, key)` maps to the set of client
/// ids that asked to be notified. `dirty`: every client id whose watched set
/// has been touched since the last EXEC. WATCH adds to `watched`; UNWATCH/EXEC
/// clears the client from `watched`; `set_key`/`sync_delete` adds to `dirty`;
/// EXEC reads-and-clears `dirty` for its own client id.
///
/// PORT NOTE: deliberate architectural shortcut for Phase B. Real Redis stores
/// the per-key watcher list on `serverDb.watched_keys` and mutates each
/// watching `client` directly via the global client list. Until `RedisServer`
/// owns the client list and is reachable from `db.rs::set_key`, this global
/// `OnceLock` carries the same information. Initialise from `main.rs` startup.
#[derive(Debug, Default)]
pub struct WatchedKeysIndex {
    pub watched: HashMap<(u32, RedisString), HashSet<ClientId>>,
    pub dirty: HashSet<ClientId>,
}

static WATCHED_KEYS_INDEX: OnceLock<Arc<Mutex<WatchedKeysIndex>>> = OnceLock::new();
static WATCHED_KEYS_REGISTRATIONS: AtomicUsize = AtomicUsize::new(0);

type SwapDbWakeFn = dyn Fn(u32) + Send + Sync;
static SWAPDB_WAKE_HOOK: OnceLock<Box<SwapDbWakeFn>> = OnceLock::new();

/// Install the SWAPDB wake hook.
///
/// The hook receives a single database index and wakes any clients blocked on
/// keys that exist in that database. It acquires the database lock internally.
/// Installed once at startup from `redis-commands`; subsequent calls are
/// no-ops (OnceLock semantics).
pub fn install_swapdb_wake_hook(f: Box<SwapDbWakeFn>) {
    let _ = SWAPDB_WAKE_HOOK.set(f);
}

type StreamKeyDeletedFn = dyn Fn(&RedisString) + Send + Sync;
static STREAM_KEY_DELETED_HOOK: OnceLock<Box<StreamKeyDeletedFn>> = OnceLock::new();

/// Install the hook called when a stream key is deleted (DEL / FLUSHDB-side).
///
/// The hook receives the key that was deleted and wakes any XREADGROUP BLOCK
/// clients waiting on that key with a NOGROUP error. Installed once from
/// `redis-commands`; subsequent calls are no-ops.
pub fn install_stream_key_deleted_hook(f: Box<StreamKeyDeletedFn>) {
    let _ = STREAM_KEY_DELETED_HOOK.set(f);
}

fn fire_stream_key_deleted_hook(key: &RedisString) {
    if let Some(hook) = STREAM_KEY_DELETED_HOOK.get() {
        hook(key);
    }
}

type StreamDbFlushedFn = dyn Fn() + Send + Sync;
static STREAM_DB_FLUSHED_HOOK: OnceLock<Box<StreamDbFlushedFn>> = OnceLock::new();

/// Install the hook called when a database is flushed (FLUSHDB / FLUSHALL).
///
/// Wakes all XREADGROUP BLOCK clients with NOGROUP errors. Installed once
/// from `redis-commands`; subsequent calls are no-ops.
pub fn install_stream_db_flushed_hook(f: Box<StreamDbFlushedFn>) {
    let _ = STREAM_DB_FLUSHED_HOOK.set(f);
}

fn fire_stream_db_flushed_hook() {
    if let Some(hook) = STREAM_DB_FLUSHED_HOOK.get() {
        hook();
    }
}

type StreamRenameHookFn = dyn Fn(&RedisString, u32) + Send + Sync;
static STREAM_RENAME_HOOK: OnceLock<Box<StreamRenameHookFn>> = OnceLock::new();

/// Install the hook called after RENAME/RENAMENX completes.
///
/// The hook receives the destination key name and the database index. The
/// `redis-commands` layer wakes any XREADGROUP BLOCK clients parked on that
/// key: if the new value has the expected group, entries are delivered;
/// otherwise NOGROUP is sent. Installed once from `redis-commands`; subsequent
/// calls are no-ops.
pub fn install_stream_rename_hook(f: Box<StreamRenameHookFn>) {
    let _ = STREAM_RENAME_HOOK.set(f);
}

fn fire_stream_rename_hook(dst_key: &RedisString, db_id: u32) {
    if let Some(hook) = STREAM_RENAME_HOOK.get() {
        hook(dst_key, db_id);
    }
}

type StreamKeyOverwrittenFn = dyn Fn(&RedisString) + Send + Sync;
static STREAM_KEY_OVERWRITTEN_HOOK: OnceLock<Box<StreamKeyOverwrittenFn>> = OnceLock::new();

/// Install the hook called when a stream key is overwritten with a non-stream
/// value (e.g. SET mystream val). Wakes blocked XREADGROUP clients with
/// WRONGTYPE error. Installed once from `redis-commands`; subsequent calls
/// are no-ops.
pub fn install_stream_key_overwritten_hook(f: Box<StreamKeyOverwrittenFn>) {
    let _ = STREAM_KEY_OVERWRITTEN_HOOK.set(f);
}

fn fire_stream_key_overwritten_hook(key: &RedisString) {
    if let Some(hook) = STREAM_KEY_OVERWRITTEN_HOOK.get() {
        hook(key);
    }
}

/// Carry-all for the components needed to fire keyspace notifications from
/// code paths that do not have a `CommandContext` (lazy expiry, active expiry).
///
/// Installed once at server startup via `install_global_notify_handle`.
pub struct GlobalNotifyHandle {
    pub pubsub: Arc<Mutex<PubSubRegistry>>,
    pub live_config: Arc<LiveConfig>,
}

static GLOBAL_NOTIFY_HANDLE: OnceLock<Arc<GlobalNotifyHandle>> = OnceLock::new();

/// Install the global notification handle used by lazy/active expiry paths.
///
/// Should be called once during server initialisation, before any connection
/// is accepted. Subsequent calls are no-ops (OnceLock semantics).
pub fn install_global_notify_handle(
    pubsub: Arc<Mutex<PubSubRegistry>>,
    live_config: Arc<LiveConfig>,
) {
    let _ = GLOBAL_NOTIFY_HANDLE.set(Arc::new(GlobalNotifyHandle {
        pubsub,
        live_config,
    }));
}

/// Publish a keyspace notification from a code path that has no `CommandContext`.
///
/// `event_type` is a `NOTIFY_*` flag from `crate::notify`. `event` is the
/// raw event-name bytes. `key` is the affected key. `dbid` is the database
/// index. Returns immediately when no handle is installed (unit tests) or
/// when the configured flags do not include `event_type`.
pub fn notify_keyspace_event_global(event_type: i32, event: &[u8], key: &RedisString, dbid: u32) {
    let handle = match GLOBAL_NOTIFY_HANDLE.get() {
        Some(h) => h,
        None => return,
    };
    let flags = handle.live_config.notify_keyspace_events_flags() as i32;
    if flags & event_type == 0 {
        return;
    }
    let dbid_bytes = format!("{}", dbid).into_bytes();
    if flags & NOTIFY_KEYSPACE != 0 {
        let mut chan: Vec<u8> = Vec::with_capacity(
            b"__keyspace@".len() + dbid_bytes.len() + b"__:".len() + key.as_bytes().len(),
        );
        chan.extend_from_slice(b"__keyspace@");
        chan.extend_from_slice(&dbid_bytes);
        chan.extend_from_slice(b"__:");
        chan.extend_from_slice(key.as_bytes());
        let chan_str = RedisString::from_vec(chan);
        let event_str = RedisString::from_bytes(event);
        publish_to_registry(&handle.pubsub, &chan_str, &event_str);
    }
    if flags & NOTIFY_KEYEVENT != 0 {
        let mut chan: Vec<u8> = Vec::with_capacity(
            b"__keyevent@".len() + dbid_bytes.len() + b"__:".len() + event.len(),
        );
        chan.extend_from_slice(b"__keyevent@");
        chan.extend_from_slice(&dbid_bytes);
        chan.extend_from_slice(b"__:");
        chan.extend_from_slice(event);
        let chan_str = RedisString::from_vec(chan);
        publish_to_registry(&handle.pubsub, &chan_str, key);
    }
}

fn publish_to_registry(
    registry: &Arc<Mutex<PubSubRegistry>>,
    channel: &RedisString,
    message: &RedisString,
) {
    use redis_protocol::frame::encode_resp2;
    use redis_protocol::RespFrame;
    let frame_bytes = {
        let mut buf = Vec::with_capacity(32 + channel.as_bytes().len() + message.as_bytes().len());
        encode_resp2(
            &RespFrame::array(vec![
                RespFrame::bulk(RedisString::from_static(b"message")),
                RespFrame::bulk(channel.clone()),
                RespFrame::bulk(message.clone()),
            ]),
            &mut buf,
        );
        buf
    };
    let (channel_subs, pattern_pairs) = {
        let guard = match registry.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let subs = guard.channel_subscribers(channel);
        let pats = guard.pattern_matches(channel, glob_match_ascii_ci_db);
        (subs, pats)
    };
    let guard = match registry.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    for sub in channel_subs {
        guard.send_to(sub, frame_bytes.clone());
    }
    for (pattern, subs) in pattern_pairs {
        let pmessage_bytes = {
            use redis_protocol::frame::encode_resp2;
            use redis_protocol::RespFrame;
            let mut buf =
                Vec::with_capacity(64 + channel.as_bytes().len() + message.as_bytes().len());
            encode_resp2(
                &RespFrame::array(vec![
                    RespFrame::bulk(RedisString::from_static(b"pmessage")),
                    RespFrame::bulk(pattern.clone()),
                    RespFrame::bulk(channel.clone()),
                    RespFrame::bulk(message.clone()),
                ]),
                &mut buf,
            );
            buf
        };
        for sub in subs {
            guard.send_to(sub, pmessage_bytes.clone());
        }
    }
}

fn glob_match_ascii_ci_db(pattern: &[u8], text: &[u8]) -> bool {
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_p, mut star_t) = (usize::MAX, 0usize);
    let lower = |b: u8| if b.is_ascii_uppercase() { b + 32 } else { b };
    while ti < text.len() {
        if pi < pattern.len() && pattern[pi] == b'?' {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && lower(pattern[pi]) == lower(text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_p = pi;
            star_t = ti;
            pi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// Install or fetch the global watched-keys index.
///
/// First caller installs an empty index; subsequent callers receive the same
/// `Arc`. Safe to call from the binary entry point and from per-command
/// handlers without synchronisation concerns beyond the inner `Mutex`.
pub fn watched_keys_index() -> &'static Arc<Mutex<WatchedKeysIndex>> {
    WATCHED_KEYS_INDEX.get_or_init(|| Arc::new(Mutex::new(WatchedKeysIndex::default())))
}

/// True when any client has at least one WATCH registration.
///
/// This is a fast-path mirror of the global watched-key index. The index
/// mutex remains authoritative for exact key/client membership.
pub fn watched_keys_any() -> bool {
    WATCHED_KEYS_REGISTRATIONS.load(Ordering::Acquire) != 0
}

fn watched_keys_sub_registrations(n: usize) {
    if n == 0 {
        return;
    }
    let _ =
        WATCHED_KEYS_REGISTRATIONS.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(n))
        });
}

/// Register `client_id` as a watcher of `key` in database `db_id`.
pub fn watched_keys_index_add(db_id: u32, key: &RedisString, client_id: ClientId) {
    WATCHED_KEYS_REGISTRATIONS.fetch_add(1, Ordering::AcqRel);
    let idx = watched_keys_index();
    let mut guard = match idx.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let inserted = guard
        .watched
        .entry((db_id, key.clone()))
        .or_default()
        .insert(client_id);
    if !inserted {
        watched_keys_sub_registrations(1);
    }
}

/// Remove `client_id` from every watch list.
pub fn watched_keys_index_remove_client(client_id: ClientId) {
    let idx = watched_keys_index();
    let mut guard = match idx.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let mut removed = 0usize;
    guard.watched.retain(|_, watchers| {
        if watchers.remove(&client_id) {
            removed += 1;
        }
        !watchers.is_empty()
    });
    drop(guard);
    watched_keys_sub_registrations(removed);
}

/// Mark every client watching `key` in database `db_id` as dirty.
///
/// C: db.c → multi.c::touchWatchedKey. Called after every write to `key`.
pub fn watched_keys_touch(db_id: u32, key: &RedisString) {
    if !watched_keys_any() {
        return;
    }
    let idx = watched_keys_index();
    let mut guard = match idx.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let watched_key = (db_id, key.clone());
    let watchers = match guard.watched.get(&watched_key) {
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

/// Mark WATCH clients dirty for every watched key in `emptied` that exists in
/// either the old keyspace or the replacement keyspace.
///
/// C: multi.c::touchAllWatchedKeysInDb. Used by FLUSH/SWAP-style operations
/// where the watched-key ownership remains attached to the logical DB id while
/// the keyspace contents are replaced.
fn watched_keys_touch_all_in_db(emptied: &RedisDb, replaced_with: Option<&RedisDb>) {
    if !watched_keys_any() {
        return;
    }
    let idx = watched_keys_index();
    let mut guard = match idx.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let mut dirty_clients: Vec<ClientId> = Vec::new();
    for ((db_id, key), watchers) in guard.watched.iter() {
        if *db_id != emptied.id {
            continue;
        }
        let exists_in_emptied = emptied.find(key).is_some();
        let exists_in_replacement = replaced_with.is_some_and(|db| db.find(key).is_some());
        if exists_in_emptied || exists_in_replacement {
            dirty_clients.extend(watchers.iter().copied());
        }
    }
    for cid in dirty_clients {
        guard.dirty.insert(cid);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Lookup flags  (C: server.h LOOKUP_*)
// ─────────────────────────────────────────────────────────────────────────────

pub const LOOKUP_NONE: u32 = 0;
pub const LOOKUP_NOTOUCH: u32 = 1 << 0;
pub const LOOKUP_NONOTIFY: u32 = 1 << 1;
pub const LOOKUP_NOSTATS: u32 = 1 << 2;
pub const LOOKUP_WRITE: u32 = 1 << 3;
pub const LOOKUP_NOEXPIRE: u32 = 1 << 4;

pub const EXPIRE_FORCE_DELETE_EXPIRED: u32 = 1 << 0;
pub const EXPIRE_AVOID_DELETE_EXPIRED: u32 = 1 << 1;

pub const SETKEY_KEEPTTL: u32 = 1 << 0;
pub const SETKEY_NO_SIGNAL: u32 = 1 << 1;
pub const SETKEY_ALREADY_EXIST: u32 = 1 << 2;
pub const SETKEY_DOESNT_EXIST: u32 = 1 << 3;
pub const SETKEY_ADD_OR_UPDATE: u32 = 1 << 4;

pub const EMPTYDB_NO_FLAGS: u32 = 0;
pub const EMPTYDB_ASYNC: u32 = 1 << 0;
pub const EMPTYDB_NOFUNCTIONS: u32 = 1 << 1;

pub const DB_FLAG_KEY_DELETED: u32 = 1 << 0;
pub const DB_FLAG_KEY_EXPIRED: u32 = 1 << 1;
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
        Self {
            keys: Vec::with_capacity(16),
        }
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

    /// When true, lazy expiration is suppressed and expired keys stay visible
    /// to lookups and key iteration. Set per-command by the dispatcher when a
    /// primary is in `import-mode` and the calling client is in import-source
    /// state. C: `server.current_client->flag.import_source` consulted by
    /// `keyIsExpired` / `objectIsExpired` (db.c:2126/2144).
    import_source_active: bool,

    /// When true (a primary in `import-mode`), an expired key is still reported
    /// as expired to non-import clients but is NOT lazily deleted — the server
    /// waits for an explicit DEL from the import source. C: the `KEEP_EXPIRED`
    /// branch of `getExpirationPolicyWithFlags` (expire.c:995-1019).
    import_mode_keep: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// impl RedisDb
// ─────────────────────────────────────────────────────────────────────────────

impl RedisDb {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    /// Construct a `RedisDb` from a snapshot of (key, object) pairs.
    ///
    /// Used by BGSAVE to build a throwaway DB containing only the entries
    /// captured at snapshot time. The id is set to 0.
    pub fn from_snapshot(entries: Vec<(RedisString, crate::object::RedisObject)>) -> Self {
        let mut db = Self::new(0);
        for (k, v) in entries {
            db.dict.insert(k, v);
        }
        db
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
        // C: db.c:2126/2144 — a primary in import-mode keeps expired keys
        // visible to an import-source client.
        if self.import_source_active {
            return false;
        }
        match self.dict.get(key) {
            None => false,
            Some(obj) => obj.expire != EXPIRY_NONE && obj.expire < Self::now_ms(),
        }
    }

    /// Sets the per-command import-expiry state. `import_source_active` keeps
    /// expired keys fully visible to the current client; `import_mode_keep`
    /// stops lazy expiration from deleting expired keys (they stay, reported as
    /// expired). The dispatcher refreshes these before every command so they
    /// reflect the current client and server state.
    pub fn set_import_expire_state(&mut self, import_source_active: bool, import_mode_keep: bool) {
        self.import_source_active = import_source_active;
        self.import_mode_keep = import_mode_keep;
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
        // C: getExpirationPolicyWithFlags KEEP_EXPIRED — a primary in import-mode
        // reports the key as expired but waits for the import source to delete
        // it, so non-force lookups must not lazily remove it.
        if self.import_mode_keep && flags & EXPIRE_FORCE_DELETE_EXPIRED == 0 {
            return KeyStatus::Expired;
        }
        // TODO(port): EXPIRE_FORCE_DELETE_EXPIRED — check replica mode before deleting
        // TODO(port): signalModifiedKey(NULL, db, keyobj)
        // TODO(port): propagateDeletion to AOF + replicas
        self.dict.remove(key);
        notify_keyspace_event_global(NOTIFY_EXPIRED, b"expired", key, self.id);
        server_metrics()
            .expired_keys
            .fetch_add(1, Ordering::Relaxed);
        KeyStatus::Deleted
    }

    // ── Lookup API ──────────────────────────────────────────────────────────

    /// General-purpose key lookup with flags.
    ///
    /// C: db.c:80 lookupKey
    pub fn lookup_key(&mut self, key: &RedisString, flags: u32) -> Option<&RedisObject> {
        if self.expire_if_needed(key, 0) != KeyStatus::Valid {
            if flags & (LOOKUP_NOSTATS | LOOKUP_WRITE) == 0 {
                server_metrics()
                    .keyspace_misses
                    .fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        if flags & LOOKUP_NOTOUCH == 0 {
            let now = crate::lru_clock::current_lru_clock();
            if let Some(obj) = self.dict.get_mut(key) {
                obj.lru = now;
            }
        }
        let result = self.dict.get(key);
        if flags & (LOOKUP_NOSTATS | LOOKUP_WRITE) == 0 {
            match result {
                Some(_) => server_metrics()
                    .keyspace_hits
                    .fetch_add(1, Ordering::Relaxed),
                None => server_metrics()
                    .keyspace_misses
                    .fetch_add(1, Ordering::Relaxed),
            };
        }
        result
    }

    /// Read-oriented lookup. Asserts no LOOKUP_WRITE flag.
    ///
    /// C: db.c:136 lookupKeyReadWithFlags
    pub fn lookup_key_read_with_flags(
        &mut self,
        key: &RedisString,
        flags: u32,
    ) -> Option<&RedisObject> {
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
    pub fn lookup_key_write_with_flags(
        &mut self,
        key: &RedisString,
        flags: u32,
    ) -> Option<&mut RedisObject> {
        if self.expire_if_needed(key, EXPIRE_FORCE_DELETE_EXPIRED | flags) != KeyStatus::Valid {
            return None;
        }
        let touch = flags & LOOKUP_NOTOUCH == 0;
        let now = crate::lru_clock::current_lru_clock();
        let obj = self.dict.get_mut(key)?;
        if touch {
            obj.lru = now;
        }
        Some(obj)
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
        self.set_key_prepared(key, value, flags);
    }

    /// High-level setter when the caller already observed the previous
    /// expiry during its command-specific lookup.
    pub fn set_key_with_known_expire(
        &mut self,
        key: RedisString,
        mut value: RedisObject,
        expire: i64,
        flags: u32,
    ) {
        value.expire = expire;
        self.set_key_prepared(key, value, flags);
    }

    fn set_key_prepared(&mut self, key: RedisString, value: RedisObject, flags: u32) {
        let may_have_blocked_stream_waiters =
            STREAM_KEY_OVERWRITTEN_HOOK.get().is_some() && crate::blocked_keys::blocked_keys_any();
        let old_was_stream =
            may_have_blocked_stream_waiters && self.dict.get(&key).is_some_and(|o| o.is_stream());
        let needs_watch_signal = flags & SETKEY_NO_SIGNAL == 0 && watched_keys_any();
        let needs_stream_hook = old_was_stream;
        if needs_watch_signal || needs_stream_hook {
            let hook_key = key.clone();
            self.dict.insert(key, value);
            if needs_watch_signal {
                watched_keys_touch(self.id, &hook_key);
            }
            if needs_stream_hook {
                fire_stream_key_overwritten_hook(&hook_key);
            }
        } else {
            self.dict.insert(key, value);
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
            watched_keys_touch(self.id, key);
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
        let watched_keys: Vec<RedisString> = if watched_keys_any() {
            self.dict.keys().cloned().collect()
        } else {
            Vec::new()
        };
        self.dict.clear();
        self.avg_ttl = 0;
        for k in &watched_keys {
            watched_keys_touch(self.id, k);
        }
    }

    /// Raw (no expiry check) key lookup. Used by internal scans.
    ///
    /// C: db.c:2271 dbFind
    pub fn find(&self, key: &RedisString) -> Option<&RedisObject> {
        self.dict.get(key)
    }

    /// Borrow the main dict for eviction sampling.
    ///
    /// Exposed here rather than going through `keys_snapshot_with_types` so
    /// the eviction loop in `eviction.rs` can peek at `RedisObject.lru` for
    /// each sample without allocating a snapshot of the entire keyspace.
    pub fn iter_for_eviction(&self) -> impl Iterator<Item = (&RedisString, &RedisObject)> {
        self.dict.iter()
    }

    /// True if the key is in the dict regardless of TTL.
    pub fn exists_raw(&self, key: &RedisString) -> bool {
        self.dict.contains_key(key)
    }

    /// True if the key is present and not expired.
    pub fn exists(&mut self, key: &RedisString) -> bool {
        self.lookup_key_read_with_flags(key, LOOKUP_NOTOUCH)
            .is_some()
    }

    /// Number of keys including logically-expired ones not yet lazily removed.
    ///
    /// C: db.c:2287 dbSize
    pub fn size(&self) -> u64 {
        self.dict.len() as u64
    }

    /// Number of keys that carry an active TTL (expire != EXPIRY_NONE).
    ///
    /// Used by `INFO keyspace` to populate the `expires=N` field.
    pub fn expires_count(&self) -> u64 {
        self.dict
            .values()
            .filter(|o| o.expire != EXPIRY_NONE)
            .count() as u64
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
            .filter(|(_, obj)| {
                self.import_source_active || obj.expire == EXPIRY_NONE || obj.expire >= now
            })
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
            .filter(|(_, obj)| {
                self.import_source_active || obj.expire == EXPIRY_NONE || obj.expire >= now
            })
            .map(|(k, obj)| (k.clone(), object_kind_name(&obj.kind)))
            .collect()
    }

    /// Return a random key that is not expired.
    ///
    /// C: db.c:442 dbRandomKey
    /// PERF(port): O(n) HashMap walk — replace with fair random kvstore entry in Phase 4.
    pub fn random_key(&self) -> Option<RedisString> {
        // TODO(port): kvstoreGetFairRandomHashtableIndex / kvstoreHashtableFairRandomEntry
        let now = Self::now_ms();
        if self.dict.is_empty() {
            return None;
        }
        let mut seed = [0u8; 8];
        crate::util::get_random_bytes(&mut seed);
        let start = (u64::from_le_bytes(seed) as usize) % self.dict.len();
        self.dict
            .iter()
            .cycle()
            .skip(start)
            .take(self.dict.len())
            .find(|(_, obj)| {
                self.import_source_active || obj.expire == EXPIRY_NONE || obj.expire >= now
            })
            .map(|(k, _)| k.clone())
    }

    /// Sample up to `max` (key, expire_ms) pairs from this db's keyspace for the
    /// active-expiration cycle. Only keys that carry a TTL are returned;
    /// untagged (persistent) keys are skipped. The starting offset is pseudo-
    /// randomised from `offset_seed` to avoid biased deletion from HashMap
    /// iteration order.
    ///
    /// PERF(port): O(n) walk over the main dict because Phase B has no
    /// secondary `expires` index yet. Phase 4 kvstore work replaces this with
    /// a direct sample over `db->expires`.
    pub fn sample_expiring_keys(&self, max: usize, offset_seed: u64) -> Vec<(RedisString, i64)> {
        if max == 0 || self.dict.is_empty() {
            return Vec::new();
        }
        let len = self.dict.len();
        let start = (offset_seed as usize) % len;
        let mut out: Vec<(RedisString, i64)> = Vec::with_capacity(max);
        for (k, obj) in self.dict.iter().cycle().skip(start).take(len) {
            if obj.expire != EXPIRY_NONE {
                out.push((k.clone(), obj.expire));
                if out.len() >= max {
                    break;
                }
            }
        }
        out
    }

    /// Number of keys that currently carry a TTL.
    ///
    /// PERF(port): O(n) walk — phase-4 kvstore exposes `db->expires` size in O(1).
    pub fn expiring_key_count(&self) -> usize {
        self.dict
            .iter()
            .filter(|(_, obj)| obj.expire != EXPIRY_NONE)
            .count()
    }

    /// Swap keyspace contents with `other`. blocking/ready/watched stay in place.
    ///
    /// C: db.c:1769 dbSwapDatabases (inner per-db swap)
    pub fn swap_contents_with(&mut self, other: &mut RedisDb) {
        watched_keys_touch_all_in_db(self, Some(other));
        watched_keys_touch_all_in_db(other, Some(self));
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
        if !watched_keys_any() {
            return;
        }
        let k = RedisString::from_bytes(key.as_ref());
        watched_keys_touch(self.id, &k);
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
    pub fn watched_keys_add_client(
        &mut self,
        key: &RedisObject,
        _client_id: crate::client::ClientId,
    ) {
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
    let db_id = db.id;
    db.sync_delete(key);
    notify_keyspace_event_global(NOTIFY_EXPIRED, b"expired", key, db_id);
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
    fire_stream_db_flushed_hook();
    ctx.db_mut().clear();
    ctx.reply_simple_string(b"OK")
}

/// C: db.c:856 flushallCommand — FLUSHALL [ASYNC|SYNC]
///
/// Clears every logical database. The current client's DB is cleared via the
/// already-held `ctx.db_mut()` reference; all other DBs are locked and cleared
/// individually to avoid re-acquiring the mutex held by the accept loop.
pub fn flushall_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    fire_stream_db_flushed_hook();
    ctx.for_each_db_mut(|db| db.clear())?;
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
        let is_stream = ctx
            .db_mut()
            .lookup_key_read_with_flags(&key, LOOKUP_NOTOUCH)
            .is_some_and(|o| o.is_stream());
        // TODO(port): expireIfNeeded(c->db, c->argv[j], NULL, 0) before delete
        let deleted = if lazy {
            ctx.db_mut().async_delete(&key)
        } else {
            ctx.db_mut().sync_delete(&key)
        };
        if deleted {
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
            // TODO(port): signalModifiedKey(c, c->db, c->argv[j])
            // TODO(port): server.dirty++
            num_deleted += 1;
            if is_stream {
                if ctx.client_ref().flag_deny_blocking() {
                    ctx.client_mut().pending_wakes.push(key.clone());
                } else {
                    fire_stream_key_deleted_hook(&key);
                }
            }
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
        if ctx
            .db_mut()
            .lookup_key_read_with_flags(&key, LOOKUP_NOTOUCH)
            .is_some()
        {
            count += 1;
        }
    }
    ctx.reply_integer(count)
}

/// C: db.c:907 selectCommand — SELECT index
pub fn select_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let id_arg = ctx.arg(1)?.clone();
    let id = parse_i64_from_bytes(id_arg.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
    let id = ctx.validate_db_index(id)?;
    ctx.client_mut().db_index = id;
    ctx.reply_simple_string(b"OK")
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
        ObjectKind::Json(_) => b"ReJSON-RL",
        ObjectKind::Bloom(_) => b"MBbloom--",
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
            let n = parse_i64_from_bytes(ctx.arg(j + 1)?.as_bytes()).ok_or_else(|| {
                RedisError::runtime(b"ERR value is not an integer or out of range")
            })?;
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
        } else if eq_ignore_ascii_case(bytes, b"NOSCORES") {
            return Err(RedisError::runtime(
                b"ERR NOSCORES option can only be used in ZSCAN",
            ));
        } else if eq_ignore_ascii_case(bytes, b"NOVALUES") {
            return Err(RedisError::runtime(
                b"ERR NOVALUES option can only be used in HSCAN",
            ));
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
    let type_name: &[u8] = match ctx
        .db_mut()
        .lookup_key_read_with_flags(&key, LOOKUP_NOTOUCH)
    {
        None => b"none",
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => b"string",
            ObjectKind::List(_) => b"list",
            ObjectKind::Hash(_) => b"hash",
            ObjectKind::Set(_) => b"set",
            ObjectKind::ZSet(_) => b"zset",
            ObjectKind::Stream(_) => b"stream",
            ObjectKind::Module => b"none",
            ObjectKind::Json(_) => b"ReJSON-RL",
            ObjectKind::Bloom(_) => b"MBbloom--",
        },
    };
    ctx.reply_simple_string(type_name)
}

/// C: db.c:1422 shutdownCommand — SHUTDOWN [[NOSAVE|SAVE] [NOW] [FORCE] [SAFE] | ABORT]
pub fn shutdown_command(_ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): full flag parsing (NOSAVE / SAVE / NOW / FORCE / ABORT / SAFE / FAILOVER)
    // TODO(port): blockClientShutdown, prepareForShutdown, abortShutdown, exit(0)
    Err(RedisError::runtime(
        b"SHUTDOWN: TODO(port): not implemented in Phase A",
    ))
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
        return if nx {
            ctx.reply_integer(0)
        } else {
            ctx.reply_simple_string(b"OK")
        };
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

    ctx.notify_keyspace_event(NOTIFY_GENERIC, b"rename_from", &src_key);
    ctx.notify_keyspace_event(NOTIFY_GENERIC, b"rename_to", &dst_key);
    // TODO(port): signalModifiedKey(c, c->db, c->argv[1]) and c->argv[2]
    // TODO(port): server.dirty++
    fire_stream_rename_hook(&dst_key, ctx.db().id() as u32);

    if nx {
        ctx.reply_integer(1)
    } else {
        ctx.reply_simple_string(b"OK")
    }
}

/// C: db.c:1530 renameCommand — RENAME key newkey
pub fn rename_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    rename_generic_command(ctx, false)
}

/// C: db.c:1534 renamenxCommand — RENAMENX key newkey
pub fn renamenx_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    rename_generic_command(ctx, true)
}

/// C: db.c:1538 moveGenericCommand — MOVE key db [REPLACE]
///
/// Atomically moves `key` from the client's current database to `target_db`.
/// With `REPLACE`, an existing key in the destination is overwritten.
/// Returns 1 on success, 0 when the key does not exist in the source or
/// already exists in the destination (and REPLACE was not supplied).
pub fn move_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"move"));
    }
    let key = ctx.arg(1)?.clone();
    let db_arg = ctx.arg(2)?.clone();
    let target_db = parse_i64_from_bytes(db_arg.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR value is not an integer or out of range"))?;

    let replace = if argc == 4 {
        let opt = ctx.arg(3)?.clone();
        if eq_ignore_ascii_case(opt.as_bytes(), b"REPLACE") {
            true
        } else {
            return Err(RedisError::runtime(b"ERR syntax error"));
        }
    } else if argc > 4 {
        return Err(RedisError::runtime(b"ERR syntax error"));
    } else {
        false
    };

    let target_db = ctx.validate_db_index(target_db)?;
    let current_db_id = ctx.selected_db_id();
    if target_db == current_db_id {
        return Err(RedisError::runtime(
            b"ERR source and destination objects are the same",
        ));
    }
    let src_obj = match ctx
        .db_mut()
        .lookup_key_read_with_flags(&key, LOOKUP_NOTOUCH)
    {
        None => return ctx.reply_integer(0),
        Some(obj) => obj.clone(),
    };
    let expire = ctx.db().get_expire(&key);
    let inserted = ctx.with_db_index(target_db, |dest| {
        if dest.exists_raw(&key) && !replace {
            return false;
        }
        let mut new_obj = src_obj;
        new_obj.expire = expire;
        dest.insert(key.clone(), new_obj);
        dest.signal_modified(&key);
        true
    })?;
    if !inserted {
        return ctx.reply_integer(0);
    }
    ctx.db_mut().sync_delete(&key);
    ctx.notify_keyspace_event(NOTIFY_GENERIC, b"move_from", &key);
    notify_keyspace_event_global(NOTIFY_GENERIC, b"move_to", &key, target_db);
    ctx.reply_integer(1)
}

/// C: db.c:1611 copyCommand — COPY source destination [DB n] [REPLACE]
///
/// Copies `source` to `destination`. When `DB n` is supplied and `n` differs
/// from the client's current database, the destination key lands in that
/// logical database. The `REPLACE` flag allows overwriting an existing
/// destination key. Returns `:1` on success, `:0` otherwise.
pub fn copy_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    let src_key = ctx.arg(1)?.clone();
    let dst_key = ctx.arg(2)?.clone();

    let mut replace = false;
    let mut explicit_target_db: Option<u32> = None;
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
            let parsed = parse_i64_from_bytes(val_bytes.as_bytes()).ok_or_else(|| {
                RedisError::runtime(b"ERR value is not an integer or out of range")
            })?;
            explicit_target_db = Some(ctx.validate_db_index(parsed)?);
            j += 2;
        } else {
            return Err(RedisError::runtime(b"ERR syntax error"));
        }
    }

    let current_db_id = ctx.selected_db_id();
    let resolved_target_db = explicit_target_db.unwrap_or(current_db_id);

    if src_key == dst_key && resolved_target_db == current_db_id {
        return ctx.reply_integer(0);
    }

    let src_clone = match ctx
        .db_mut()
        .lookup_key_read_with_flags(&src_key, LOOKUP_NOTOUCH)
    {
        None => return ctx.reply_integer(0),
        Some(obj) => obj.clone(),
    };
    let expire = ctx.db().get_expire(&src_key);
    let mut new_obj = src_clone;
    new_obj.expire = expire;

    let inserted = ctx.with_db_index(resolved_target_db, |dest| {
        if dest.exists_raw(&dst_key) && !replace {
            return false;
        }
        dest.insert(dst_key.clone(), new_obj);
        dest.signal_modified(&dst_key);
        true
    })?;
    if !inserted {
        return ctx.reply_integer(0);
    }

    if resolved_target_db == current_db_id {
        ctx.notify_keyspace_event(NOTIFY_GENERIC, b"copy_to", &dst_key);
    } else {
        notify_keyspace_event_global(NOTIFY_GENERIC, b"copy_to", &dst_key, resolved_target_db);
    }
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
        if ctx
            .db_mut()
            .lookup_key_read_with_flags(&key, LOOKUP_NONE)
            .is_some()
        {
            count += 1;
        }
    }
    ctx.reply_integer(count)
}

/// Wake any clients blocked on keys in `other_db_id` (the db not currently
/// held by the command dispatch).
///
/// Delegates to the hook installed by `install_swapdb_wake_hook`. No-op when
/// no hook is installed (unit tests).
fn wake_blocked_in_other_db(other_db_id: u32) {
    if let Some(hook) = SWAPDB_WAKE_HOOK.get() {
        hook(other_db_id);
    }
}

/// Case-insensitive ASCII equality for command option keywords.
fn eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| x.to_ascii_lowercase() == y.to_ascii_lowercase())
}

/// C: db.c:1861 swapdbCommand — SWAPDB index index
///
/// Atomically swaps the contents of two logical databases. When the client's
/// current database is one of the two being swapped, the swap uses the already-
/// held `ctx.db_mut()` lock plus a second lock on the other DB to avoid
/// deadlock against the accept loop's lock ordering. When neither swapped DB
/// is the current client DB both locks are acquired fresh in ascending index
/// order (matching `GlobalDatabases::swap`). After the swap, blocked clients
/// waiting on keys in either DB are woken.
pub fn swapdb_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"swapdb"));
    }
    let id1_arg = ctx.arg(1)?.clone();
    let id2_arg = ctx.arg(2)?.clone();
    let db_count = ctx.database_count() as i64;
    let id1 = parse_i64_from_bytes(id1_arg.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR invalid first DB index"))?;
    let id2 = parse_i64_from_bytes(id2_arg.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR invalid second DB index"))?;
    if id1 < 0 || id1 >= db_count || id2 < 0 || id2 >= db_count {
        return Err(RedisError::runtime(b"ERR DB index is out of range"));
    }
    let current_db_id = ctx.selected_db_id();
    let id1u = id1 as u32;
    let id2u = id2 as u32;

    if id1u == id2u {
        return ctx.reply_simple_string(b"OK");
    }

    ctx.with_two_db_indices(id1u, id2u, |left, right| {
        left.swap_contents_with(right);
    })?;

    let blocked_keys: Vec<RedisString> = {
        let idx = match crate::blocked_keys::blocked_keys_index().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        idx.all_blocked_keys()
    };
    let wake_order = if current_db_id == id1u {
        [id1u, id2u]
    } else if current_db_id == id2u {
        [id2u, id1u]
    } else {
        [id1u, id2u]
    };
    for db_id in wake_order {
        if db_id == current_db_id {
            for key in &blocked_keys {
                if let Some(obj) = ctx.db().find(key) {
                    if !obj.is_stream() {
                        fire_stream_key_overwritten_hook(key);
                    }
                    ctx.client_mut().pending_wakes.push(key.clone());
                }
            }
        } else {
            wake_blocked_in_other_db(db_id);
        }
    }

    ctx.reply_simple_string(b"OK")
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
    let numkeys_bytes = argv
        .get(key_count_ofs)
        .ok_or_else(RedisError::not_integer)?;
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
    let (neg, digits) = if b[0] == b'-' {
        (true, &b[1..])
    } else {
        (false, b)
    };
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
    if neg {
        Some(-n)
    } else {
        Some(n)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Glob-style pattern matcher  (C: util.c stringmatchlen)
// ─────────────────────────────────────────────────────────────────────────────

/// Case-sensitive glob match over byte slices.
///
/// C: util.c `stringmatchlen(pattern, plen, string, slen, 0)`.
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    crate::util::string_match_len(pattern, text, false)
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
    fn import_source_active_keeps_expired_keys_visible() {
        let mut db = RedisDb::new(0);
        let key = k(b"foo1");
        let mut obj = make_str_obj(b"1");
        obj.expire = 1; // 1 ms since epoch — always in the past
        db.add(key.clone(), obj);

        // Normal client: the expired key is invisible to lookup and iteration.
        assert!(db.is_expired(&key));
        assert!(db.random_key().is_none());
        assert!(db.matching_keys(b"*").is_empty());

        // import-source state: the expired key stays visible everywhere, and
        // is NOT lazily deleted on lookup.
        db.set_import_expire_state(true, true);
        assert!(!db.is_expired(&key));
        assert!(db.lookup_key_read_with_flags(&key, LOOKUP_NONE).is_some());
        assert!(
            db.exists_raw(&key),
            "import-source lookup must not delete the key"
        );
        assert_eq!(db.random_key(), Some(key.clone()));
        assert_eq!(db.matching_keys(b"*"), vec![key.clone()]);

        // import-mode, normal client: the key is reported expired (invisible)
        // but a non-force read must NOT lazily delete it.
        db.set_import_expire_state(false, true);
        assert!(db.is_expired(&key));
        assert!(db.lookup_key_read_with_flags(&key, LOOKUP_NONE).is_none());
        assert!(db.exists_raw(&key), "import-mode must keep the expired key");

        // No import mode: normal lazy expiration deletes on read.
        db.set_import_expire_state(false, false);
        assert!(db.is_expired(&key));
        assert!(db.lookup_key_read_with_flags(&key, LOOKUP_NONE).is_none());
        assert!(
            !db.exists_raw(&key),
            "without import mode the key is lazily deleted"
        );
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
    fn watched_key_fast_path_preserves_dirty_marking() {
        let key = k(b"watched-fast-path");
        let cid = 9_101_001;
        watched_keys_index_remove_client(cid);

        watched_keys_touch(0, &key);
        assert!(!watched_keys_take_dirty(cid));

        watched_keys_index_add(0, &key, cid);
        watched_keys_touch(1, &key);
        assert!(!watched_keys_take_dirty(cid));

        watched_keys_touch(0, &key);
        assert!(watched_keys_take_dirty(cid));

        watched_keys_index_remove_client(cid);
        watched_keys_touch(0, &key);
        assert!(!watched_keys_take_dirty(cid));
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
    fn swap_contents_marks_watchers_on_logical_db() {
        let key = k(b"swap-watch-key");
        let cid = 9_101_003;
        watched_keys_index_remove_client(cid);

        let mut db0 = RedisDb::new(0);
        let mut db1 = RedisDb::new(1);
        db0.add(key.clone(), make_str_obj(b"from-db0"));
        watched_keys_index_add(1, &key, cid);

        db1.swap_contents_with(&mut db0);

        assert!(watched_keys_take_dirty(cid));
        watched_keys_index_remove_client(cid);
    }

    #[test]
    fn glob_match_basic() {
        assert!(glob_match(b"*", b"anything"));
        assert!(!glob_match(b"*", b""));
        assert!(glob_match(b"foo*", b"foobar"));
        assert!(!glob_match(b"foo*", b"barfoo"));
        assert!(glob_match(b"f?o", b"foo"));
        assert!(!glob_match(b"f?o", b"fo"));
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"a", b""));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h*llo", b"hllo"));
        assert!(glob_match(b"h*llo", b"heeeello"));
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[a-z]llo", b"hello"));
        assert!(!glob_match(b"h[^e]llo", b"hello"));
        assert!(!glob_match(
            b"a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
    }

    #[test]
    fn glob_match_rejects_abusive_nested_patterns_like_valkey() {
        let mut pattern = Vec::with_capacity(100_000);
        for _ in 0..50_000 {
            pattern.extend_from_slice(b"*?");
        }
        let text = vec![b'a'; 50_000];
        assert!(!glob_match(&pattern, &text));
    }

    #[test]
    fn random_key_does_not_pin_to_first_hashmap_entry() {
        let mut db = RedisDb::new(0);
        db.add(k(b"foo"), make_str_obj(b"x"));
        db.add(k(b"bar"), make_str_obj(b"y"));

        let mut seen = HashSet::new();
        for _ in 0..100 {
            if let Some(key) = db.random_key() {
                seen.insert(key);
            }
        }

        assert!(seen.contains(&k(b"foo")));
        assert!(seen.contains(&k(b"bar")));
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
//   todos:         59
//   port_notes:    6
//   unsafe_blocks: 0
//   notes:         Core lookup/add/delete/expiry/set_key/swap translated faithfully.
//                  Commands stubbed with ctx.db_mut() calls (expected name-resolution
//                  errors — Phase 3 wires CommandContext to RedisServer).
//                  kvstore/cluster slots, lazy-free, SCAN cursor, full
//                  keyspace notifications, and replication propagation all
//                  carry TODO(port) and are deferred to Phase 3 (keyspace
//                  events, blocking) and Phase 4 (kvstore). SELECT, DB-index
//                  validation, MOVE/COPY/SWAPDB, and WATCH dirtying now read
//                  CommandContext DB routing / logical DB ids.
//                  Validator shows only expected E0432/E0282 name-resolution errors;
//                  zero real syntax errors.
// ──────────────────────────────────────────────────────────────────────────
