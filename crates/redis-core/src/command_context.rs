//! `CommandContext` — the contract every command implementation works against.
//!
//! Per PORTING.md §2 #5: bundles `&mut Client`, parsed args, and reply
//! writer helpers. Returns `Result<(), RedisError>`. NOT the C `client *c`
//! parameter — commands never touch the raw connection or buffer-list.
//!
//! `RedisServer` reference comes via the orchestrator (Phase 3 architect
//! packet adds it).

use std::sync::{Arc, Mutex};

use crate::client::Client;
use crate::db::RedisDb;
use crate::live_config::LiveConfig;
use crate::notify::{
    NOTIFY_KEYEVENT, NOTIFY_KEYSPACE,
};
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
/// Phase 3 will collapse this back to `&'a mut RedisServer` once the server
/// reference threads through every dispatch site.
pub enum DbStorage<'a> {
    Owned(RedisDb),
    Borrowed(&'a mut RedisDb),
}

impl<'a> DbStorage<'a> {
    fn as_ref(&self) -> &RedisDb {
        match self {
            DbStorage::Owned(db) => db,
            DbStorage::Borrowed(db) => db,
        }
    }

    fn as_mut(&mut self) -> &mut RedisDb {
        match self {
            DbStorage::Owned(db) => db,
            DbStorage::Borrowed(db) => db,
        }
    }
}

/// Bundle of context every command receives. Wraps a mutable Client and
/// exposes argument access + reply-writer methods.
///
/// PORT NOTE: `db` is held in a [`DbStorage`] enum. Production callers use
/// [`CommandContext::with_db`] to share one `RedisDb` across commands; tests
/// use [`CommandContext::new`] for an isolated owned scratch db. Phase 3 will
/// replace this with `&'a mut RedisServer` (and `db()` will route through the
/// server's db list keyed by `client.db_index`).
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
    fn into_reply_len(self) -> i64 { self }
}
impl ReplyArrayLen for usize {
    fn into_reply_len(self) -> i64 { self as i64 }
}
impl ReplyArrayLen for i32 {
    fn into_reply_len(self) -> i64 { self as i64 }
}

/// Flexible argv-index trait. Translated code mixes `usize`, `i32`, and
/// arithmetic on `i64` for indexing into `client.argv`.
pub trait ArgIndex {
    fn into_arg_index(self) -> RedisResult<usize>;
}

impl ArgIndex for usize {
    fn into_arg_index(self) -> RedisResult<usize> { Ok(self) }
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
        Self {
            client,
            db: DbStorage::Borrowed(db),
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
        self.client
            .write_frame(&RespFrame::Simple(RedisString::from_bytes(bytes)));
        Ok(())
    }

    pub fn reply_bulk(&mut self, bytes: &[u8]) -> RedisResult<()> {
        self.client
            .write_frame(&RespFrame::Bulk(Some(RedisString::from_bytes(bytes))));
        Ok(())
    }

    pub fn reply_bulk_string(&mut self, s: RedisString) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Bulk(Some(s)));
        Ok(())
    }

    pub fn reply_null_bulk(&mut self) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Bulk(None));
        Ok(())
    }

    pub fn reply_integer(&mut self, n: i64) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Integer(n));
        Ok(())
    }

    pub fn reply_array_header<L: ReplyArrayLen>(&mut self, len: L) -> RedisResult<()> {
        self.reply_array_header_i64(len.into_reply_len())
    }

    pub fn reply_null_array(&mut self) -> RedisResult<()> {
        self.client.write_frame(&RespFrame::Array(None));
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
        self.client
            .write_frame(&RespFrame::Error(err.into_reply_error_payload()));
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
    ///
    /// STUB — Phase B emits an array header regardless of protocol mode.
    /// Full RESP3 push-frame support lands when networking is ported.
    pub fn reply_push_or_array_header<L: ReplyArrayLen>(
        &mut self,
        len: L,
    ) -> RedisResult<()> {
        self.reply_array_header_i64(len.into_reply_len())
    }

    fn reply_array_header_i64(&mut self, len: i64) -> RedisResult<()> {
        let mut buf = Vec::new();
        buf.push(b'*');
        use std::io::Write;
        let _ = write!(buf, "{}", len);
        buf.extend_from_slice(b"\r\n");
        self.client.reply_buf.extend_from_slice(&buf);
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
    pub fn notify_keyspace_event(
        &self,
        event_type: i32,
        event: &[u8],
        key: &RedisString,
    ) {
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

/// Encode a `*3 message channel payload` push frame and ship it to every
/// subscriber that matches `channel` (exact or pattern). Used by the
/// `notify_keyspace_event` helper.
fn publish_keyspace_message(
    registry: &Arc<Mutex<PubSubRegistry>>,
    channel: &RedisString,
    message: &RedisString,
) {
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
        let pats = guard.pattern_matches(channel, glob_match_ascii_ci);
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

    fn ctx_with_args(args: &[&[u8]]) -> (Client, ) {
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
        server
            .live_config
            .set_notify_keyspace_events_flags((NOTIFY_KEYSPACE | NOTIFY_KEYEVENT | NOTIFY_STRING) as u32);

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
        assert_eq!(received.len(), 2, "expected one keyspace and one keyevent frame");
        let joined: Vec<u8> = received.concat();
        assert!(joined.windows(b"__keyspace@0__:foo".len()).any(|w| w == b"__keyspace@0__:foo"));
        assert!(joined.windows(b"__keyevent@0__:set".len()).any(|w| w == b"__keyevent@0__:set"));
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
        assert!(rx.try_recv().is_err(), "no notification should fire when flags are 0");
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (PORTING.md §2 #5 + §4.5 reply mapping)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         1
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Reply writer + arg access. RedisServer reference deferred to Phase 3.
// ──────────────────────────────────────────────────────────────────────────
