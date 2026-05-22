//! `CommandContext` — the contract every command implementation works against.
//!
//! Per PORTING.md §2 #5: bundles `&mut Client`, parsed args, and reply
//! writer helpers. Returns `Result<(), RedisError>`. NOT the C `client *c`
//! parameter — commands never touch the raw connection or buffer-list.
//!
//! `RedisServer` reference comes via the orchestrator (Phase 3 architect
//! packet adds it).

use std::sync::{Arc, Mutex, MutexGuard};

use crate::client::Client;
use crate::databases::{global_databases, GlobalDatabases};
use crate::db::RedisDb;
use crate::live_config::LiveConfig;
use crate::notify::{NOTIFY_KEYEVENT, NOTIFY_KEYSPACE};
use crate::object::RedisObject;
use crate::pubsub_registry::PubSubRegistry;
use crate::server::RedisServer;
use redis_protocol::frame::encode_resp2;
use redis_protocol::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};

/// Storage for the database reachable from a `CommandContext`.
///
/// In production the server hands every command a shared `&mut RedisDb` so
/// SET/GET state persists across commands and connections. Unit tests still
/// want an isolated scratch db per context, so this enum supports both.
///
/// Runtime-owner packets add the routed variant so command handlers can use a
/// selected-DB/cross-DB boundary without naming the global database store.
pub enum DbStorage<'a> {
    Owned(RedisDb),
    Borrowed(&'a mut RedisDb),
    Routed {
        selected: &'a mut RedisDb,
        route: DbListRoute,
    },
    OwnerList {
        selected_index: u32,
        dbs: &'a mut [RedisDb],
    },
}

impl<'a> DbStorage<'a> {
    fn as_ref(&self) -> &RedisDb {
        match self {
            DbStorage::Owned(db) => db,
            DbStorage::Borrowed(db) => db,
            DbStorage::Routed { selected, .. } => selected,
            DbStorage::OwnerList {
                selected_index,
                dbs,
            } => {
                let index = selected_position(dbs.len(), *selected_index);
                &dbs[index]
            }
        }
    }

    fn as_mut(&mut self) -> &mut RedisDb {
        match self {
            DbStorage::Owned(db) => db,
            DbStorage::Borrowed(db) => db,
            DbStorage::Routed { selected, .. } => selected,
            DbStorage::OwnerList {
                selected_index,
                dbs,
            } => {
                let index = selected_position(dbs.len(), *selected_index);
                &mut dbs[index]
            }
        }
    }

    fn route(&self) -> Option<DbListRoute> {
        match self {
            DbStorage::Routed { route, .. } => Some(*route),
            DbStorage::Owned(_) | DbStorage::Borrowed(_) | DbStorage::OwnerList { .. } => None,
        }
    }

    fn is_owner_list(&self) -> bool {
        matches!(self, DbStorage::OwnerList { .. })
    }
}

fn selected_position(len: usize, selected_index: u32) -> usize {
    if len == 0 {
        0
    } else {
        (selected_index as usize).min(len - 1)
    }
}

/// Route to the current live DB list.
///
/// This is deliberately a route to the existing `global_databases()` storage,
/// not a second keyspace. Later owner-owned DB packets can replace the route
/// internals without changing command handlers that ask `CommandContext` for
/// selected-DB or cross-DB access.
#[derive(Clone, Copy)]
pub struct DbListRoute {
    dbs: &'static GlobalDatabases,
}

impl DbListRoute {
    pub fn global() -> Self {
        Self {
            dbs: global_databases(),
        }
    }

    pub fn count(self) -> usize {
        self.dbs.count()
    }

    pub fn get(self, index: u32) -> Arc<Mutex<RedisDb>> {
        self.dbs.get(index)
    }
}

/// Bundle of context every command receives. Wraps a mutable Client and
/// exposes argument access + reply-writer methods.
///
/// PORT NOTE: `db` is held in a [`DbStorage`] enum. Production callers use
/// [`CommandContext::with_server`] to share the selected live `RedisDb` plus a
/// DB-list route; tests use [`CommandContext::new`] for an isolated owned
/// scratch db. The owner-owned DB packet will replace the route internals with
/// the owner-held DB list.
pub struct CommandContext<'a> {
    pub client: &'a mut Client,
    db: DbStorage<'a>,
    /// Shared handle to the live server state. Wrapped in `Arc` so the same
    /// instance is reachable from every command-dispatch thread without giving
    /// out a `&mut` borrow.
    server: Arc<RedisServer>,
    /// Optional shared pub/sub registry handle.
    ///
    /// `None` in unit tests; `Some` for the live server, where every
    /// connection's accept-loop wraps the same registry in `Arc<Mutex<>>`.
    pub pubsub: Option<Arc<Mutex<PubSubRegistry>>>,
}

/// Argument type accepted by `CommandContext::reply_error`.
///
/// STUB — Phase B placeholder. Implemented for `&RedisError` (the canonical
/// case) and `&[u8]` (used by translated code that builds raw error message
/// bytes inline). Once all translated code switches to `RedisError`, the
/// `&[u8]` impl can be removed.
pub trait ReplyErrorArg {
    fn into_reply_error_payload(self) -> RedisString;
}

impl ReplyErrorArg for &RedisError {
    fn into_reply_error_payload(self) -> RedisString {
        self.to_resp_payload()
    }
}

impl ReplyErrorArg for &[u8] {
    fn into_reply_error_payload(self) -> RedisString {
        RedisString::from_bytes(self)
    }
}

impl<const N: usize> ReplyErrorArg for &[u8; N] {
    fn into_reply_error_payload(self) -> RedisString {
        RedisString::from_bytes(self)
    }
}

/// Flexible reply-array length argument.
///
/// Translated callers pass `usize`, `i64`, and `i32` interchangeably; this
/// trait normalises them to `i64` for the underlying writer. Phase 3 may
/// tighten this once we settle on a single int type for protocol sizes.
pub trait ReplyArrayLen {
    fn into_reply_len(self) -> i64;
}

impl ReplyArrayLen for i64 {
    fn into_reply_len(self) -> i64 {
        self
    }
}
impl ReplyArrayLen for usize {
    fn into_reply_len(self) -> i64 {
        self as i64
    }
}
impl ReplyArrayLen for i32 {
    fn into_reply_len(self) -> i64 {
        self as i64
    }
}

/// Flexible argv-index trait. Translated code mixes `usize`, `i32`, and
/// arithmetic on `i64` for indexing into `client.argv`.
pub trait ArgIndex {
    fn into_arg_index(self) -> RedisResult<usize>;
}

impl ArgIndex for usize {
    fn into_arg_index(self) -> RedisResult<usize> {
        Ok(self)
    }
}
impl ArgIndex for i64 {
    fn into_arg_index(self) -> RedisResult<usize> {
        usize::try_from(self).map_err(|_| RedisError::runtime(b"argv index out of range"))
    }
}
impl ArgIndex for i32 {
    fn into_arg_index(self) -> RedisResult<usize> {
        usize::try_from(self).map_err(|_| RedisError::runtime(b"argv index out of range"))
    }
}

impl<'a> CommandContext<'a> {
    /// Construct a context with an isolated owned scratch database.
    ///
    /// Intended for unit tests; production code paths should use
    /// [`Self::with_server`] so the live server's config and pubsub registry
    /// thread through every dispatch.
    pub fn new(client: &'a mut Client) -> Self {
        Self {
            client,
            db: DbStorage::Owned(RedisDb::new(0)),
            server: Arc::new(RedisServer::default()),
            pubsub: None,
        }
    }

    /// Construct a context sharing the caller-supplied database.
    ///
    /// Test-only helper; production callers go through [`Self::with_server`].
    pub fn with_db(client: &'a mut Client, db: &'a mut RedisDb) -> Self {
        Self {
            client,
            db: DbStorage::Borrowed(db),
            server: Arc::new(RedisServer::default()),
            pubsub: None,
        }
    }

    /// Construct a context with both a shared database and a shared pub/sub
    /// registry. Used by the live server accept loop.
    pub fn with_db_and_pubsub(
        client: &'a mut Client,
        db: &'a mut RedisDb,
        pubsub: Arc<Mutex<PubSubRegistry>>,
    ) -> Self {
        Self {
            client,
            db: DbStorage::Borrowed(db),
            server: Arc::new(RedisServer::default()),
            pubsub: Some(pubsub),
        }
    }

    /// Construct a fully-wired context: live database, shared pub/sub
    /// registry, and the actual `Arc<RedisServer>` carrying the live config.
    /// This is the production accept-loop constructor.
    pub fn with_server(
        client: &'a mut Client,
        db: &'a mut RedisDb,
        server: Arc<RedisServer>,
        pubsub: Arc<Mutex<PubSubRegistry>>,
    ) -> Self {
        Self::with_server_and_db_route(client, db, DbListRoute::global(), server, pubsub)
    }

    /// Construct a fully-wired context with an explicit DB-list route.
    ///
    /// `db` is the already-selected database for this dispatch. `route` names
    /// the full live DB list that cross-DB commands can consult without
    /// reaching back to `global_databases()` directly. For this packet the
    /// route points at the existing global handles; it does not move storage
    /// ownership or create a second live keyspace.
    pub fn with_server_and_db_route(
        client: &'a mut Client,
        db: &'a mut RedisDb,
        route: DbListRoute,
        server: Arc<RedisServer>,
        pubsub: Arc<Mutex<PubSubRegistry>>,
    ) -> Self {
        Self {
            client,
            db: DbStorage::Routed {
                selected: db,
                route,
            },
            server,
            pubsub: Some(pubsub),
        }
    }

    /// Construct a production context backed by the RuntimeOwner-owned DB
    /// slice.
    ///
    /// The selected DB is derived from `client.db_index`, matching Valkey's
    /// per-client selected database semantics. Cross-DB commands route through
    /// closure helpers on this context instead of taking global DB mutexes.
    pub fn with_server_and_db_list(
        client: &'a mut Client,
        dbs: &'a mut [RedisDb],
        server: Arc<RedisServer>,
        pubsub: Arc<Mutex<PubSubRegistry>>,
    ) -> Self {
        let selected_index = client.db_index;
        Self {
            client,
            db: DbStorage::OwnerList {
                selected_index,
                dbs,
            },
            server,
            pubsub: Some(pubsub),
        }
    }

    // ── Args ──────────────────────────────────────────────────────

    pub fn arg(&self, i: usize) -> RedisResult<&RedisString> {
        self.client
            .arg(i)
            .ok_or_else(|| RedisError::wrong_number_of_args(self.command_name()))
    }

    pub fn arg_count(&self) -> usize {
        self.client.arg_count()
    }

    /// Arg 0 is the command name (uppercase by Redis convention).
    pub fn command_name(&self) -> &[u8] {
        self.client
            .arg(0)
            .map(|s| s.as_bytes())
            .unwrap_or(b"<unknown>")
    }

    // ── Reply writers ─────────────────────────────────────────────

    pub fn reply_simple_string(&mut self, bytes: &[u8]) -> RedisResult<()> {
        self.client.write_simple_string(bytes);
        Ok(())
    }

    pub fn reply_bulk(&mut self, bytes: &[u8]) -> RedisResult<()> {
        self.client.write_bulk(bytes);
        Ok(())
    }

    pub fn reply_bulk_string(&mut self, s: RedisString) -> RedisResult<()> {
        self.client.write_bulk_string(&s);
        Ok(())
    }

    pub fn reply_null_bulk(&mut self) -> RedisResult<()> {
        self.client.write_null_bulk();
        Ok(())
    }

    pub fn reply_integer(&mut self, n: i64) -> RedisResult<()> {
        self.client.write_integer(n);
        Ok(())
    }

    pub fn reply_array_header<L: ReplyArrayLen>(&mut self, len: L) -> RedisResult<()> {
        self.reply_array_header_i64(len.into_reply_len())
    }

    pub fn reply_null_array(&mut self) -> RedisResult<()> {
        self.client.write_null_array();
        Ok(())
    }

    pub fn reply_frame(&mut self, frame: &RespFrame) -> RedisResult<()> {
        self.client.write_frame(frame);
        Ok(())
    }

    /// Reply with an error. Equivalent of C's addReplyError* family.
    ///
    /// Accepts either a `&RedisError` (preferred) or raw `&[u8]` bytes; both
    /// are dispatched through [`ReplyErrorArg`]. The error becomes a `-...`
    /// RESP error line; this does not return `Err`.
    pub fn reply_error<E: ReplyErrorArg>(&mut self, err: E) -> RedisResult<()> {
        let payload = err.into_reply_error_payload();
        self.client.write_error(payload.as_bytes());
        Ok(())
    }

    // ── Phase-B stubs needed by translated command code ────────────

    /// Argument count, C-style (alias of `arg_count`).
    pub fn argc(&self) -> usize {
        self.client.arg_count()
    }

    /// Owned-copy argv accessor.
    ///
    /// Returns a cloned `RedisString` for the given index. Translated code
    /// uses this where it wants to retain a copy across borrows of `ctx`.
    pub fn arg_owned<I: ArgIndex>(&self, i: I) -> RedisResult<RedisString> {
        let idx = i.into_arg_index()?;
        self.client
            .arg(idx)
            .cloned()
            .ok_or_else(|| RedisError::wrong_number_of_args(self.command_name()))
    }

    /// Argv accessor returning a `RedisObject::String` wrapper.
    ///
    /// STUB — Phase B placeholder mapping a raw argv `RedisString` into the
    /// `RedisObject::String` variant. Eventually arguments will already be
    /// `RedisObject`-typed once shared-object interning lands.
    pub fn arg_as_object<I: ArgIndex>(&self, i: I) -> RedisResult<RedisObject> {
        let s = self.arg_owned(i)?;
        Ok(RedisObject::from_string(s))
    }

    /// Null bulk reply (alias of `reply_null_bulk`).
    pub fn reply_null(&mut self) -> RedisResult<()> {
        self.reply_null_bulk()
    }

    /// Push or array header — RESP3 push frame in client RESP3 mode,
    /// fall back to RESP2 array header otherwise.
    pub fn reply_push_or_array_header<L: ReplyArrayLen>(&mut self, len: L) -> RedisResult<()> {
        let n = len.into_reply_len();
        self.client.write_push_or_array_header(n);
        Ok(())
    }

    fn reply_array_header_i64(&mut self, len: i64) -> RedisResult<()> {
        self.client.write_array_header(len);
        Ok(())
    }

    /// Reply with a map header for `n_pairs` key/value pairs.
    ///
    /// RESP3 clients receive `%N\r\n`; RESP2 clients receive an equivalent
    /// flat alternating array header (`*2N\r\n`). Caller must then emit
    /// exactly `2 * n_pairs` frames in alternating key/value order; both
    /// wire shapes are well-formed RESP under either protocol.
    pub fn reply_map_header<L: ReplyArrayLen>(&mut self, n_pairs: L) -> RedisResult<()> {
        let n = n_pairs.into_reply_len();
        self.client.write_map_header(n);
        Ok(())
    }

    /// Reply with a set header for `n` items.
    ///
    /// RESP3 clients receive `~N\r\n`; RESP2 clients receive `*N\r\n` (the
    /// semantic distinction between array and set is RESP3-only).
    pub fn reply_set_header<L: ReplyArrayLen>(&mut self, n: L) -> RedisResult<()> {
        let n = n.into_reply_len();
        self.client.write_set_header(n);
        Ok(())
    }

    /// Reply with a RESP3 double. RESP2 clients receive a bulk string of the
    /// canonical textual representation (matches `format_score` in the zset
    /// command surface).
    pub fn reply_double(&mut self, d: f64) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Double(d));
        Ok(())
    }

    /// Reply with a RESP3 boolean. RESP2 clients receive an integer reply of
    /// `:1\r\n` (true) or `:0\r\n` (false).
    pub fn reply_boolean(&mut self, b: bool) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Boolean(b));
        Ok(())
    }

    /// Reply with a RESP3 null. RESP2 clients receive `$-1\r\n`.
    pub fn reply_resp3_null(&mut self) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Null);
        Ok(())
    }

    /// Reply with a RESP3 big number. RESP2 clients receive a bulk string of
    /// the same digits.
    pub fn reply_big_number(&mut self, digits: &[u8]) -> RedisResult<()> {
        self.client
            .write_frame(&RespFrame::BigNumber(RedisString::from_bytes(digits)));
        Ok(())
    }

    /// Reply with a RESP3 verbatim string. RESP2 clients receive a bulk
    /// string of the payload bytes (no format tag).
    pub fn reply_verbatim_string(&mut self, format: &[u8; 3], bytes: &[u8]) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::VerbatimString {
            format: *format,
            data: RedisString::from_bytes(bytes),
        });
        Ok(())
    }

    /// Per-context database. STUB — Phase 3 routes through the server.
    pub fn db(&self) -> &RedisDb {
        self.db.as_ref()
    }

    /// Mutable view of the per-context database. STUB — Phase 3 routes through
    /// the server keyed by `client.db_index`.
    pub fn db_mut(&mut self) -> &mut RedisDb {
        self.db.as_mut()
    }

    /// Number of logical databases visible to this command context.
    ///
    /// Production contexts carry a `DbListRoute` and report the configured
    /// route length. Legacy test contexts fall back to the current global DB
    /// count so SELECT validation keeps matching the product envelope until
    /// tests opt into an explicit route.
    pub fn database_count(&self) -> usize {
        match &self.db {
            DbStorage::OwnerList { dbs, .. } => dbs.len(),
            _ => self
                .db
                .route()
                .map(DbListRoute::count)
                .unwrap_or_else(|| global_databases().count()),
        }
    }

    /// Route to the logical DB list visible from this context.
    ///
    /// Production contexts receive an explicit route from the dispatch owner.
    /// Legacy borrowed/owned contexts fall back to the current global route so
    /// tests and the TLS startup path preserve today's storage model.
    pub fn db_list_route(&self) -> DbListRoute {
        self.db.route().unwrap_or_else(DbListRoute::global)
    }

    /// Validate a database index parsed from argv.
    pub fn validate_db_index(&self, index: i64) -> RedisResult<u32> {
        if index < 0 || index >= self.database_count() as i64 {
            return Err(RedisError::runtime(b"ERR DB index is out of range"));
        }
        Ok(index as u32)
    }

    /// Currently selected database index as carried by the client.
    pub fn selected_db_index(&self) -> u32 {
        self.client.db_index
    }

    /// Return the selected DB id for the borrowed DB currently installed in
    /// this context.
    pub fn selected_db_id(&self) -> u32 {
        self.db.as_ref().id
    }

    /// Return a handle to a non-selected DB from the live DB list.
    ///
    /// `Ok(None)` means the requested index is the currently borrowed DB and
    /// callers should use `ctx.db()` / `ctx.db_mut()` instead. This avoids
    /// taking the same `Arc<Mutex<RedisDb>>` twice on the transitional storage
    /// model. Future owner-owned DB packets can keep this contract while
    /// changing the route internals.
    pub fn other_db_handle(&self, index: u32) -> RedisResult<Option<Arc<Mutex<RedisDb>>>> {
        self.validate_db_index(index as i64)?;
        if self.db.is_owner_list() {
            return Err(RedisError::runtime(
                b"ERR owner DB route does not expose mutex handles",
            ));
        }
        if index == self.selected_db_id() {
            return Ok(None);
        }
        Ok(Some(self.db_list_route().get(index)))
    }

    /// Run `f` with mutable access to a logical database selected by index.
    ///
    /// For the selected DB this reuses the already-borrowed `&mut RedisDb`.
    /// For any other DB it locks the current DB-list route. This is the
    /// transitional API that lets commands stop naming `global_databases()`
    /// directly before the owner-owned DB flip.
    pub fn with_db_index<R>(
        &mut self,
        index: u32,
        f: impl FnOnce(&mut RedisDb) -> R,
    ) -> RedisResult<R> {
        self.validate_db_index(index as i64)?;
        if let DbStorage::OwnerList { dbs, .. } = &mut self.db {
            let pos = index as usize;
            return Ok(f(&mut dbs[pos]));
        }
        if index == self.selected_db_id() {
            return Ok(f(self.db.as_mut()));
        }
        let handle = self.db_list_route().get(index);
        let mut guard = lock_db_handle(&handle);
        Ok(f(&mut guard))
    }

    /// Run `f` with this context temporarily routed to `index` as its selected
    /// DB.
    ///
    /// Queued EXEC dispatch uses this to preserve commands that changed DB via
    /// an earlier queued SELECT. On owner-owned storage this simply changes the
    /// selected index inside the DB slice route; on the transitional global
    /// route it builds a short-lived context around the requested DB handle.
    pub fn with_selected_db_index<R>(
        &mut self,
        index: u32,
        f: impl FnOnce(&mut CommandContext<'_>) -> R,
    ) -> RedisResult<R> {
        self.validate_db_index(index as i64)?;
        if let DbStorage::OwnerList { selected_index, .. } = &mut self.db {
            let old = *selected_index;
            *selected_index = index;
            let result = f(self);
            if let DbStorage::OwnerList { selected_index, .. } = &mut self.db {
                *selected_index = old;
            }
            return Ok(result);
        }

        if index == self.selected_db_id() {
            return Ok(f(self));
        }

        let route = self.db_list_route();
        let handle = route.get(index);
        let mut guard = lock_db_handle(&handle);
        let server = self.server_arc();
        match self.pubsub.as_ref().cloned() {
            Some(pubsub) => {
                let mut selected_ctx = CommandContext::with_server_and_db_route(
                    self.client_mut(),
                    &mut guard,
                    route,
                    server,
                    pubsub,
                );
                Ok(f(&mut selected_ctx))
            }
            None => {
                let mut selected_ctx = CommandContext::with_db(self.client_mut(), &mut guard);
                Ok(f(&mut selected_ctx))
            }
        }
    }

    /// Run `f` once for every logical DB in the context route.
    pub fn for_each_db_mut(
        &mut self,
        mut f: impl FnMut(&mut RedisDb),
    ) -> RedisResult<()> {
        if let DbStorage::OwnerList { dbs, .. } = &mut self.db {
            for db in dbs.iter_mut() {
                f(db);
            }
            return Ok(());
        }

        let current = self.selected_db_id();
        f(self.db.as_mut());
        let count = self.database_count();
        for i in 0..count {
            let db_id = i as u32;
            if db_id == current {
                continue;
            }
            let handle = self.db_list_route().get(db_id);
            let mut guard = lock_db_handle(&handle);
            f(&mut guard);
        }
        Ok(())
    }

    /// Run `f` with mutable access to two distinct logical DBs.
    pub fn with_two_db_indices<R>(
        &mut self,
        first: u32,
        second: u32,
        f: impl FnOnce(&mut RedisDb, &mut RedisDb) -> R,
    ) -> RedisResult<R> {
        self.validate_db_index(first as i64)?;
        self.validate_db_index(second as i64)?;
        if first == second {
            return Err(RedisError::runtime(
                b"ERR source and destination objects are the same",
            ));
        }

        if let DbStorage::OwnerList { dbs, .. } = &mut self.db {
            let a = first as usize;
            let b = second as usize;
            if a < b {
                let (lo, hi) = dbs.split_at_mut(b);
                return Ok(f(&mut lo[a], &mut hi[0]));
            }
            let (lo, hi) = dbs.split_at_mut(a);
            return Ok(f(&mut hi[0], &mut lo[b]));
        }

        let current = self.selected_db_id();
        if first == current {
            let handle = self.db_list_route().get(second);
            let mut guard = lock_db_handle(&handle);
            return Ok(f(self.db.as_mut(), &mut guard));
        }
        if second == current {
            let handle = self.db_list_route().get(first);
            let mut guard = lock_db_handle(&handle);
            return Ok(f(&mut guard, self.db.as_mut()));
        }

        let (lo, hi, flip) = if first < second {
            (first, second, false)
        } else {
            (second, first, true)
        };
        let lo_handle = self.db_list_route().get(lo);
        let hi_handle = self.db_list_route().get(hi);
        let mut lo_guard = lock_db_handle(&lo_handle);
        let mut hi_guard = lock_db_handle(&hi_handle);
        if flip {
            Ok(f(&mut hi_guard, &mut lo_guard))
        } else {
            Ok(f(&mut lo_guard, &mut hi_guard))
        }
    }

    /// Snapshot every logical DB reachable through this context.
    pub fn snapshot_all_dbs(
        &mut self,
    ) -> RedisResult<Vec<(u32, Vec<(RedisString, RedisObject)>)>> {
        let mut out = Vec::new();
        self.for_each_db_mut(|db| {
            let entries = db
                .iter_for_eviction()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            out.push((db.id, entries));
        })?;
        Ok(out)
    }

    /// Mutable borrow of the underlying `Client`.
    pub fn client_mut(&mut self) -> &mut Client {
        self.client
    }

    /// Shared borrow of the underlying `Client`.
    pub fn client_ref(&self) -> &Client {
        self.client
    }

    /// Shared borrow of the live `RedisServer`. Returns the actual server
    /// that the accept loop built at startup — not a per-context scratch
    /// copy — so CONFIG SET writes and other live mutations are visible.
    pub fn server(&self) -> &RedisServer {
        &self.server
    }

    /// Clone the `Arc<RedisServer>` for handlers that need to outlive the
    /// current command (background threads, deferred callbacks).
    pub fn server_arc(&self) -> Arc<RedisServer> {
        Arc::clone(&self.server)
    }

    /// Shared borrow of the live config. Convenience for
    /// `ctx.server().live_config.as_ref()`.
    pub fn live_config(&self) -> &LiveConfig {
        &self.server.live_config
    }

    /// Fire a keyspace-notification for one database operation.
    ///
    /// `event_type` is one or more `NOTIFY_*` flags OR'd together (the class
    /// of the triggering command, e.g. `NOTIFY_STRING` for `SET`). `event` is
    /// the raw event-name bytes (e.g. `b"set"`). `key` is the key the
    /// operation touched. The current database id comes from `client.db_index`.
    ///
    /// The helper consults `live_config.notify_keyspace_events_flags` and
    /// returns early when the configured mask does not intersect `event_type`
    /// (matches the C semantics in `notifyKeyspaceEvent`).
    ///
    /// Publishes to `__keyspace@<db>__:<key>` and/or `__keyevent@<db>__:<event>`
    /// channels via the shared pub/sub registry; ignores callers that have no
    /// pubsub handle attached (unit tests).
    pub fn notify_keyspace_event(&self, event_type: i32, event: &[u8], key: &RedisString) {
        let flags = self.live_config().notify_keyspace_events_flags() as i32;
        if flags & event_type == 0 {
            return;
        }
        let pubsub = match &self.pubsub {
            Some(p) => p,
            None => return,
        };
        let dbid = self.client.db_index;
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
            publish_keyspace_message(pubsub, &chan_str, &event_str);
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
            publish_keyspace_message(pubsub, &chan_str, key);
        }
    }

    /// Fast preflight for command hot paths that otherwise have to keep an
    /// owned key alive only to call `notify_keyspace_event`, which is usually
    /// disabled by config.
    pub fn keyspace_notifications_enabled(&self, event_type: i32) -> bool {
        let flags = self.live_config().notify_keyspace_events_flags() as i32;
        flags & event_type != 0 && self.pubsub.is_some()
    }

    /// Empty-array reply (RESP `*0\r\n`).
    pub fn reply_empty_array(&mut self) -> RedisResult<()> {
        self.reply_array_header_i64(0)
    }

    /// Argv accessor returning a `RedisObject` (alias of `arg_as_object`).
    pub fn arg_object<I: ArgIndex>(&self, i: I) -> RedisResult<RedisObject> {
        self.arg_as_object(i)
    }

    /// Begin watching `key` for MULTI/EXEC CAS semantics.
    ///
    /// STUB — Phase B placeholder. The full implementation in
    /// `redis-commands::multi::watch_for_key` needs both `&mut Client` and
    /// `&mut RedisDb`; this thin wrapper bridges via the per-context db
    /// scratch until Phase 3 wires real db routing.
    pub fn watch_for_key(&mut self, _key: &RedisObject) -> RedisResult<()> {
        // TODO(port): call multi::watch_for_key once cross-crate dispatch
        // resolves the borrow conflict between &mut Client and &mut RedisDb.
        Ok(())
    }

    /// Dispatch the currently-installed queued command with the given flags.
    ///
    /// STUB — Phase B placeholder; real implementation depends on the Phase 3
    /// command-dispatch table.
    pub fn call_queued(&mut self, _flags: u32) -> RedisResult<()> {
        // TODO(port): wire when command dispatch lands.
        Ok(())
    }
}

/// Encode a `message channel payload` push frame and ship it to every
/// subscriber that matches `channel` (exact or pattern). Used by the
/// `notify_keyspace_event` helper.
///
/// RESP3 subscribers receive a `>` push frame prefixed with the `pubsub`
/// discriminator. RESP2 subscribers receive the legacy `*3` / `*4` array
/// emission unchanged.
fn publish_keyspace_message(
    registry: &Arc<Mutex<PubSubRegistry>>,
    channel: &RedisString,
    message: &RedisString,
) {
    let resp2_message = encode_pubsub_message_resp2(channel, message);
    let resp3_message = encode_pubsub_message_resp3(channel, message);
    let (channel_subs, pattern_pairs) = {
        let guard = match registry.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let subs = guard.channel_subscribers(channel);
        let pats = guard.pattern_matches(channel, glob_match_ascii_ci);
        (subs, pats)
    };
    let guard = match registry.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    for sub in channel_subs {
        let bytes = if guard.resp_proto(sub) == 3 {
            resp3_message.clone()
        } else {
            resp2_message.clone()
        };
        guard.send_to(sub, bytes);
    }
    for (pattern, subs) in pattern_pairs {
        let resp2_pmessage = encode_pubsub_pmessage_resp2(&pattern, channel, message);
        let resp3_pmessage = encode_pubsub_pmessage_resp3(&pattern, channel, message);
        for sub in subs {
            let bytes = if guard.resp_proto(sub) == 3 {
                resp3_pmessage.clone()
            } else {
                resp2_pmessage.clone()
            };
            guard.send_to(sub, bytes);
        }
    }
}

fn lock_db_handle(db: &Arc<Mutex<RedisDb>>) -> MutexGuard<'_, RedisDb> {
    match db.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// Encode a RESP2 `*3 message channel payload` array.
pub fn encode_pubsub_message_resp2(channel: &RedisString, message: &RedisString) -> Vec<u8> {
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
}

/// Encode a RESP3 `>4 pubsub message channel payload` push frame.
pub fn encode_pubsub_message_resp3(channel: &RedisString, message: &RedisString) -> Vec<u8> {
    let mut buf = Vec::with_capacity(48 + channel.as_bytes().len() + message.as_bytes().len());
    redis_protocol::encode_resp3(
        &RespFrame::Push(vec![
            RespFrame::bulk(RedisString::from_static(b"pubsub")),
            RespFrame::bulk(RedisString::from_static(b"message")),
            RespFrame::bulk(channel.clone()),
            RespFrame::bulk(message.clone()),
        ]),
        &mut buf,
    );
    buf
}

/// Encode a RESP2 `*4 pmessage pattern channel payload` array.
pub fn encode_pubsub_pmessage_resp2(
    pattern: &RedisString,
    channel: &RedisString,
    message: &RedisString,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(
        48 + pattern.as_bytes().len() + channel.as_bytes().len() + message.as_bytes().len(),
    );
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
}

/// Encode a RESP3 `>5 pubsub pmessage pattern channel payload` push frame.
pub fn encode_pubsub_pmessage_resp3(
    pattern: &RedisString,
    channel: &RedisString,
    message: &RedisString,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(
        64 + pattern.as_bytes().len() + channel.as_bytes().len() + message.as_bytes().len(),
    );
    redis_protocol::encode_resp3(
        &RespFrame::Push(vec![
            RespFrame::bulk(RedisString::from_static(b"pubsub")),
            RespFrame::bulk(RedisString::from_static(b"pmessage")),
            RespFrame::bulk(pattern.clone()),
            RespFrame::bulk(channel.clone()),
            RespFrame::bulk(message.clone()),
        ]),
        &mut buf,
    );
    buf
}

fn glob_match_ascii_ci(pattern: &[u8], text: &[u8]) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_with_args(args: &[&[u8]]) -> (Client,) {
        let mut c = Client::new(1);
        c.set_args(args.iter().map(|s| RedisString::from_bytes(s)).collect());
        (c,)
    }

    #[test]
    fn arg_access_returns_err_when_oob() {
        let (mut c,) = ctx_with_args(&[b"SET", b"foo"]);
        let ctx = CommandContext::new(&mut c);
        assert!(ctx.arg(0).is_ok());
        assert!(ctx.arg(1).is_ok());
        let err = ctx.arg(2).unwrap_err();
        assert!(matches!(err, RedisError::WrongNumberOfArgs(_)));
    }

    #[test]
    fn reply_simple_string_writes_resp() {
        let (mut c,) = ctx_with_args(&[b"PING"]);
        let mut ctx = CommandContext::new(&mut c);
        ctx.reply_simple_string(b"PONG").unwrap();
        assert_eq!(c.drain_reply(), b"+PONG\r\n");
    }

    #[test]
    fn reply_array_header_emits_correct_prefix() {
        let (mut c,) = ctx_with_args(&[]);
        let mut ctx = CommandContext::new(&mut c);
        ctx.reply_array_header(3).unwrap();
        ctx.reply_integer(1).unwrap();
        ctx.reply_integer(2).unwrap();
        ctx.reply_integer(3).unwrap();
        assert_eq!(c.drain_reply(), b"*3\r\n:1\r\n:2\r\n:3\r\n");
    }

    #[test]
    fn reply_error_emits_error_line() {
        let (mut c,) = ctx_with_args(&[]);
        let mut ctx = CommandContext::new(&mut c);
        ctx.reply_error(&RedisError::wrong_type()).unwrap();
        assert_eq!(
            c.drain_reply(),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn routed_context_validates_against_db_list_count() {
        let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
        let server = Arc::new(RedisServer::default());
        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"SELECT")]);
        let mut db = RedisDb::new(0);
        let ctx = CommandContext::with_server(
            &mut c,
            &mut db,
            Arc::clone(&server),
            Arc::clone(&registry),
        );

        assert_eq!(ctx.database_count(), global_databases().count());
        assert!(ctx.validate_db_index(0).is_ok());
        assert!(ctx
            .validate_db_index(global_databases().count() as i64)
            .is_err());
    }

    #[test]
    fn notify_keyspace_event_publishes_to_both_channel_families() {
        use crate::notify::{NOTIFY_KEYEVENT, NOTIFY_KEYSPACE, NOTIFY_STRING};
        use std::sync::mpsc;

        let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        {
            let mut guard = registry.lock().unwrap();
            guard.register_sender(99, tx);
            guard.subscribe_channel(RedisString::from_bytes(b"__keyspace@0__:foo"), 99);
            guard.subscribe_channel(RedisString::from_bytes(b"__keyevent@0__:set"), 99);
        }

        let server = Arc::new(RedisServer::default());
        server.live_config.set_notify_keyspace_events_flags(
            (NOTIFY_KEYSPACE | NOTIFY_KEYEVENT | NOTIFY_STRING) as u32,
        );

        let mut c = Client::new(1);
        c.set_args(vec![RedisString::from_bytes(b"SET")]);
        let mut db = RedisDb::new(0);
        let ctx = CommandContext::with_server(
            &mut c,
            &mut db,
            Arc::clone(&server),
            Arc::clone(&registry),
        );

        let key = RedisString::from_bytes(b"foo");
        ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &key);

        let mut received: Vec<Vec<u8>> = Vec::new();
        while let Ok(bytes) = rx.try_recv() {
            received.push(bytes);
        }
        assert_eq!(
            received.len(),
            2,
            "expected one keyspace and one keyevent frame"
        );
        let joined: Vec<u8> = received.concat();
        assert!(joined
            .windows(b"__keyspace@0__:foo".len())
            .any(|w| w == b"__keyspace@0__:foo"));
        assert!(joined
            .windows(b"__keyevent@0__:set".len())
            .any(|w| w == b"__keyevent@0__:set"));
    }

    #[test]
    fn notify_keyspace_event_respects_disabled_flags() {
        use crate::notify::NOTIFY_STRING;
        use std::sync::mpsc;

        let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        {
            let mut guard = registry.lock().unwrap();
            guard.register_sender(100, tx);
            guard.subscribe_channel(RedisString::from_bytes(b"__keyspace@0__:foo"), 100);
        }
        let server = Arc::new(RedisServer::default());
        let mut c = Client::new(2);
        c.set_args(vec![RedisString::from_bytes(b"SET")]);
        let mut db = RedisDb::new(0);
        let ctx = CommandContext::with_server(
            &mut c,
            &mut db,
            Arc::clone(&server),
            Arc::clone(&registry),
        );
        let key = RedisString::from_bytes(b"foo");
        ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &key);
        assert!(
            rx.try_recv().is_err(),
            "no notification should fire when flags are 0"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (PORTING.md §2 #5 + §4.5 reply mapping)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         2
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Reply writer, arg access, and transitional DB-list routing.
// ──────────────────────────────────────────────────────────────────────────
