//! Fast in-memory iteration harness for the Replication + AOF subsystems.
//! Proves replication-stream / backlog correctness *deterministically* with no
//! sockets, no tclsh, and no spawned server process — the rung-2 inner loop
//! team develops repl fixes against (per the parent `CLAUDE.md` doctrine
//! the `conn_transport_kit.rs` exemplar). The slow `assert_replication_stream`
//! tclsh oracle is the wrong loop for these bugs; here they reproduce 100%
//! the time in milliseconds.
//! Run just this loop:
//! cargo test -p redis-commands --test repl_correctness_kit
//! ── Kit mechanism ───────────────────────────────────────────────────────────
//! The replication fan-out path the live server uses is:
//! dispatch tail (dispatch.rs:661-758)
//! → propagate_write_to_replicas / propagate_command_from_wake
//! → repl.append_to_backlog(...) (the backlog)
//! → repl.send_to_replica(bytes) (per-replica mpsc + output accounting)
//! `ReplCapture` registers a real `ReplicaConn` in the *live global*
//! `ReplicationState` whose `outbound_sender` is an mpsc whose receiver we own.
//! After driving the live `dispatch`, draining that receiver yields the
//! bytes that would have gone out on the replica socket — the in-memory analog
//! of Tcl's `assert_replication_stream`. The backlog is process-global
//! (OnceLock), so a `REPL_GUARD` mutex serializes the repl-touching tests
//! each test reads bytes only from *its own* channel, making capture
//! deterministic regardless of accumulated backlog.
//! The AOF round-trip harness writes through the real `AofWriter` to a unique
//! temp file, then replays it into a fresh DB and asserts key-for-key equality
//! — the append→bytes→replay→assert-dbs-equal loop, on the live codec.

use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use redis_core::replication::{
    global_replication_state, ReplicaConn, ReplicaState, ReplicationState,
};
use redis_core::{Client, PubSubRegistry, RedisDb, RedisServer};
use redis_types::RedisString;

use redis_commands::dispatch::dispatch;

// ─── shared-global serialization ─────────────────────────────────────────────

/// The replication state is a process-wide `OnceLock`. Tests that drive
/// live fan-out path must not interleave their backlog appends, so they take
/// this guard. Capture is still per-channel, but serializing keeps the global
/// `selected_db` / offset progression legible when a test inspects them.
fn repl_guard() -> MutexGuard<'static, ()> {
    static REPL_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    match REPL_GUARD.get_or_init(|| Mutex::new(())).lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

// ─── argv / RESP helpers ─────────────────────────────────────────────────────

fn argv(parts: &[&[u8]]) -> Vec<RedisString> {
    parts.iter().map(|p| RedisString::from_bytes(p)).collect()
}

/// RESP multibulk encoding of a command, matching `aof::encode_resp_command`
/// (the exact bytes the backlog/fan-out path emits). Used to assert verbatim
/// propagation.
fn resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

// ─── ReplCapture: the assert_replication_stream analog ───────────────────────

/// A registered replica whose outbound bytes we capture in memory.
/// Registering a `ReplicaConn` in the live global state makes
/// `should_propagate_writes` return true and makes the live fan-out loop send
/// this replica every propagated command — exactly as a real attached replica
/// would receive them. `drain` collects all bytes sent so far.
struct ReplCapture {
    rx: Receiver<Vec<u8>>,
    repl: Arc<ReplicationState>,
    client_id: u64,
}

impl ReplCapture {
    /// Register a fresh online replica in the global state at `start_offset`.
    fn attach(client_id: u64, start_offset: i64) -> Self {
        let repl = global_replication_state();
        let (tx, rx) = mpsc::channel();
        let conn = ReplicaConn::new(client_id, ReplicaState::Online, start_offset, tx);
        repl.add_replica(conn);
        Self {
            rx,
            repl,
            client_id,
        }
    }

    /// All bytes the fan-out path has sent to this replica, concatenated
    /// send order — the in-memory replication stream for this connection.
    fn drain(&self) -> Vec<u8> {
        let mut out = Vec::new();
        while let Ok(chunk) = self.rx.try_recv() {
            out.extend_from_slice(&chunk);
        }
        out
    }
}

impl Drop for ReplCapture {
    fn drop(&mut self) {
        self.repl.remove_replica(self.client_id);
    }
}

/// Drive the live `dispatch` for a single command on a non-replica client whose
/// context shares the same pubsub registry (so the writer-thread sender exists)
/// and the live `RedisServer`. Returns the reply bytes.
fn dispatch_as_primary(client_id: u64, db: &mut RedisDb, cmd: &[&[u8]]) -> Vec<u8> {
    dispatch_as_primary_argv(client_id, db, argv(cmd))
}

fn dispatch_as_primary_argv(client_id: u64, db: &mut RedisDb, args: Vec<RedisString>) -> Vec<u8> {
    let mut c = Client::new(client_id);
    c.set_args(args);
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    {
        let mut ctx = redis_core::CommandContext::with_server(&mut c, db, server, pubsub);
        let _ = dispatch(&mut ctx);
    }
    c.drain_reply()
}

fn count_subsequence(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .filter(|w| *w == needle)
        .count()
}

fn dispatch_as_primary_on_db(
    client_id: u64,
    db_id: u32,
    db: &mut RedisDb,
    cmd: &[&[u8]],
) -> Vec<u8> {
    let mut c = Client::new(client_id);
    c.db_index = db_id;
    c.set_args(argv(cmd));
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    {
        let mut ctx = redis_core::CommandContext::with_server(&mut c, db, server, pubsub);
        let _ = dispatch(&mut ctx);
    }
    c.drain_reply()
}

/// Drive one command through the same dispatch entrypoint used by EXEC while
/// draining queued commands. `flag_deny_blocking` is the current in-EXEC marker
/// in production code; it bypasses processCommand-only gates such as the
/// read-only-replica check without relaxing ordinary top-level dispatch.
fn dispatch_as_exec_drain(client_id: u64, db: &mut RedisDb, cmd: &[&[u8]]) -> Vec<u8> {
    let mut c = Client::new(client_id);
    c.set_flag_deny_blocking(true);
    c.set_args(argv(cmd));
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    {
        let mut ctx = redis_core::CommandContext::with_server(&mut c, db, server, pubsub);
        let _ = dispatch(&mut ctx);
    }
    c.drain_reply()
}

// ─── GREEN ANCHOR ────────────────────────────────────────────────────────────

/// GREEN ANCHOR — proves the capture mechanism is faithful before any red test
/// is trusted. A plain `SET k v` from a primary client must land in
/// replication stream *verbatim* (this is known-correct: single-node-repl
/// proves string 108/0). If the captured bytes equal the exact RESP encoding
/// the SET, the `ReplCapture` mechanism reflects the real fan-out path.
#[test]
fn anchor_plain_set_propagates_verbatim() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_001, 0);
    let mut db = RedisDb::new(0);

    let reply = dispatch_as_primary(1, &mut db, &[b"SET", b"k", b"v"]);
    assert_eq!(reply, b"+OK\r\n", "SET should succeed");

    let stream = cap.drain();
    let set_frame = resp(&[b"SET", b"k", b"v"]);
    // The fan-out prepends SELECT when the replica's last-seen DB differs;
    // SET frame must appear verbatim as a suffix of the captured stream.
    assert!(
        stream
            .windows(set_frame.len())
            .any(|w| w == set_frame.as_slice()),
        "captured replication stream must contain the verbatim SET frame.\n\
         captured: {:?}\n  expected frame: {:?}",
        String::from_utf8_lossy(&stream),
        String::from_utf8_lossy(&set_frame),
    );
}

#[test]
fn r2_replica_fanout_updates_pending_output_accounting() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_002, global_replication_state().master_offset());
    let mut db = RedisDb::new(0);

    let reply = dispatch_as_primary(2, &mut db, &[b"SET", b"acct", b"value"]);
    assert_eq!(reply, b"+OK\r\n", "SET should succeed");

    let stream = cap.drain();
    let pending = {
        let guard = cap.repl.replicas.lock().unwrap();
        guard
            .get(&cap.client_id)
            .expect("capture replica still registered")
            .pending_output_bytes
            .load(Ordering::Relaxed)
    };
    assert_eq!(
        pending,
        stream.len(),
        "fan-out accounting should track queued replica bytes"
    );
}

// ─── R1-NOOP-DIRTY: no-op write propagation guards ─────────────────────────

/// R1-NOOP-DIRTY regression guard: no-op DEL inside MULTI/EXEC must not appear
/// in the replication stream. The implementation uses command-local
/// `prevent_propagation` as the mutation signal for commands that return 0.
#[test]
fn finding1_noop_del_in_multi_must_not_propagate() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_011, 0);
    let mut db = RedisDb::new(0);

    let mut c = Client::new(11);
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));

    // MULTI; DEL missing; EXEC — DEL deletes nothing (key never existed).
    for cmd in [
        &[b"MULTI".as_slice()][..],
        &[b"DEL".as_slice(), b"missing-key".as_slice()][..],
        &[b"EXEC".as_slice()][..],
    ] {
        c.set_args(cmd.iter().map(|p| RedisString::from_bytes(p)).collect());
        let mut ctx = redis_core::CommandContext::with_server(
            &mut c,
            &mut db,
            server.clone(),
            pubsub.clone(),
        );
        let _ = dispatch(&mut ctx);
    }

    let stream = cap.drain();
    let del_frame = resp(&[b"DEL", b"missing-key"]);
    assert!(
        !stream
            .windows(del_frame.len())
            .any(|w| w == del_frame.as_slice()),
        "no-op DEL of a missing key inside MULTI/EXEC must NOT be propagated, \
         but it appears in the replication stream.\n  captured: {:?}",
        String::from_utf8_lossy(&stream),
    );
}

/// R1-NOOP-DIRTY companion guard for the top-level dispatch path.
#[test]
fn finding1b_noop_del_top_level_must_not_propagate() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_012, 0);
    let mut db = RedisDb::new(0);

    let reply = dispatch_as_primary(12, &mut db, &[b"DEL", b"missing-key"]);
    assert_eq!(reply, b":0\r\n", "DEL of a missing key returns 0");

    let stream = cap.drain();
    let del_frame = resp(&[b"DEL", b"missing-key"]);
    assert!(
        !stream
            .windows(del_frame.len())
            .any(|w| w == del_frame.as_slice()),
        "top-level no-op DEL must NOT be propagated, but it appears in the \
         replication stream.\n  captured: {:?}",
        String::from_utf8_lossy(&stream),
    );
}

#[test]
fn r1_noop_delete_style_writes_must_not_propagate() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_013, 0);
    let mut db = RedisDb::new(0);

    let srem_missing = resp(&[b"SREM", b"missing-set", b"member"]);
    let reply = dispatch_as_primary(13, &mut db, &[b"SREM", b"missing-set", b"member"]);
    assert_eq!(reply, b":0\r\n");
    let stream = cap.drain();
    assert!(
        !stream
            .windows(srem_missing.len())
            .any(|w| w == srem_missing.as_slice()),
        "no-op SREM against a missing key must not propagate"
    );

    let hdel_missing = resp(&[b"HDEL", b"missing-hash", b"field"]);
    let reply = dispatch_as_primary(14, &mut db, &[b"HDEL", b"missing-hash", b"field"]);
    assert_eq!(reply, b":0\r\n");
    let stream = cap.drain();
    assert!(
        !stream
            .windows(hdel_missing.len())
            .any(|w| w == hdel_missing.as_slice()),
        "no-op HDEL against a missing key must not propagate"
    );

    let zrem_missing = resp(&[b"ZREM", b"missing-zset", b"member"]);
    let reply = dispatch_as_primary(15, &mut db, &[b"ZREM", b"missing-zset", b"member"]);
    assert_eq!(reply, b":0\r\n");
    let stream = cap.drain();
    assert!(
        !stream
            .windows(zrem_missing.len())
            .any(|w| w == zrem_missing.as_slice()),
        "no-op ZREM against a missing key must not propagate"
    );

    assert_eq!(
        dispatch_as_primary(16, &mut db, &[b"SADD", b"set", b"present"]),
        b":1\r\n"
    );
    assert_eq!(
        dispatch_as_primary(17, &mut db, &[b"HSET", b"hash", b"field", b"value"]),
        b":1\r\n"
    );
    assert_eq!(
        dispatch_as_primary(18, &mut db, &[b"ZADD", b"zset", b"1", b"member"]),
        b":1\r\n"
    );
    let _ = cap.drain();

    let srem_absent = resp(&[b"SREM", b"set", b"absent"]);
    let reply = dispatch_as_primary(19, &mut db, &[b"SREM", b"set", b"absent"]);
    assert_eq!(reply, b":0\r\n");
    let stream = cap.drain();
    assert!(
        !stream
            .windows(srem_absent.len())
            .any(|w| w == srem_absent.as_slice()),
        "no-op SREM against an existing set must not propagate"
    );

    let hdel_absent = resp(&[b"HDEL", b"hash", b"absent"]);
    let reply = dispatch_as_primary(20, &mut db, &[b"HDEL", b"hash", b"absent"]);
    assert_eq!(reply, b":0\r\n");
    let stream = cap.drain();
    assert!(
        !stream
            .windows(hdel_absent.len())
            .any(|w| w == hdel_absent.as_slice()),
        "no-op HDEL against an existing hash must not propagate"
    );

    let zrem_absent = resp(&[b"ZREM", b"zset", b"absent"]);
    let reply = dispatch_as_primary(21, &mut db, &[b"ZREM", b"zset", b"absent"]);
    assert_eq!(reply, b":0\r\n");
    let stream = cap.drain();
    assert!(
        !stream
            .windows(zrem_absent.len())
            .any(|w| w == zrem_absent.as_slice()),
        "no-op ZREM against an existing sorted set must not propagate"
    );
}

#[test]
fn r1_spop_rewrites_replication_to_deterministic_commands() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_014, 0);
    let mut db = RedisDb::new(0);

    assert_eq!(
        dispatch_as_primary(22, &mut db, &[b"SADD", b"single", b"only"]),
        b":1\r\n"
    );
    let _ = cap.drain();

    let reply = dispatch_as_primary(23, &mut db, &[b"SPOP", b"single"]);
    assert_eq!(reply, b"$4\r\nonly\r\n");
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"SREM", b"single", b"only"]).len())
            .any(|w| w == resp(&[b"SREM", b"single", b"only"]).as_slice()),
        "single-element SPOP must propagate as SREM, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream
            .windows(resp(&[b"SPOP", b"single"]).len())
            .any(|w| w == resp(&[b"SPOP", b"single"]).as_slice()),
        "SPOP itself must not be propagated"
    );

    assert_eq!(
        dispatch_as_primary(24, &mut db, &[b"SADD", b"partial", b"a", b"b", b"c"]),
        b":3\r\n"
    );
    let _ = cap.drain();

    let reply = dispatch_as_primary(25, &mut db, &[b"SPOP", b"partial", b"2"]);
    assert!(
        reply.starts_with(b"*2\r\n"),
        "SPOP count reply should contain two elements, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    let stream = cap.drain();
    assert!(
        stream.windows(b"SREM".len()).any(|w| w == b"SREM"),
        "partial SPOP count must propagate as SREM, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream
            .windows(resp(&[b"SPOP", b"partial", b"2"]).len())
            .any(|w| w == resp(&[b"SPOP", b"partial", b"2"]).as_slice()),
        "SPOP count itself must not be propagated"
    );

    assert_eq!(
        dispatch_as_primary(26, &mut db, &[b"SADD", b"full", b"x", b"y"]),
        b":2\r\n"
    );
    let _ = cap.drain();

    let reply = dispatch_as_primary(27, &mut db, &[b"SPOP", b"full", b"2"]);
    assert!(
        reply.starts_with(b"*2\r\n"),
        "full SPOP count reply should contain two elements, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"DEL", b"full"]).len())
            .any(|w| w == resp(&[b"DEL", b"full"]).as_slice()),
        "full SPOP count must propagate as DEL by default, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream
            .windows(resp(&[b"SPOP", b"full", b"2"]).len())
            .any(|w| w == resp(&[b"SPOP", b"full", b"2"]).as_slice()),
        "full SPOP count itself must not be propagated"
    );

    let mut sadd = vec![
        RedisString::from_bytes(b"SADD"),
        RedisString::from_bytes(b"batch"),
    ];
    for i in 0..1026 {
        sadd.push(RedisString::from_vec(i.to_string().into_bytes()));
    }
    assert_eq!(dispatch_as_primary_argv(28, &mut db, sadd), b":1026\r\n");
    let _ = cap.drain();

    let reply = dispatch_as_primary(29, &mut db, &[b"SPOP", b"batch", b"1025"]);
    assert!(
        reply.starts_with(b"*1025\r\n"),
        "large SPOP count reply should contain 1025 elements, got prefix {:?}",
        String::from_utf8_lossy(&reply[..reply.len().min(32)])
    );
    let stream = cap.drain();
    assert_eq!(
        count_subsequence(&stream, b"$4\r\nSREM\r\n"),
        2,
        "SPOP count above 1024 must propagate in two SREM batches, got {:?}",
        String::from_utf8_lossy(&stream[..stream.len().min(256)])
    );
    assert!(
        !stream
            .windows(resp(&[b"SPOP", b"batch", b"1025"]).len())
            .any(|w| w == resp(&[b"SPOP", b"batch", b"1025"]).as_slice()),
        "large SPOP count itself must not be propagated"
    );
}

#[test]
fn r1_ttl_relative_writes_rewrite_to_absolute_propagation() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_015, 0);
    let mut db = RedisDb::new(0);

    assert_eq!(
        dispatch_as_primary(28, &mut db, &[b"SET", b"ttl-key", b"value"]),
        b"+OK\r\n"
    );
    let _ = cap.drain();

    let reply = dispatch_as_primary(29, &mut db, &[b"EXPIRE", b"ttl-key", b"60"]);
    assert_eq!(reply, b":1\r\n");
    let stream = cap.drain();
    assert!(
        stream
            .windows(b"PEXPIREAT".len())
            .any(|w| w == b"PEXPIREAT"),
        "relative EXPIRE must propagate as PEXPIREAT, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream
            .windows(resp(&[b"EXPIRE", b"ttl-key", b"60"]).len())
            .any(|w| w == resp(&[b"EXPIRE", b"ttl-key", b"60"]).as_slice()),
        "relative EXPIRE itself must not be propagated"
    );

    let reply = dispatch_as_primary(30, &mut db, &[b"SET", b"set-rel", b"value", b"EX", b"60"]);
    assert_eq!(reply, b"+OK\r\n");
    let stream = cap.drain();
    assert!(
        stream.windows(b"PXAT".len()).any(|w| w == b"PXAT"),
        "SET EX must propagate as SET PXAT, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream
            .windows(resp(&[b"SET", b"set-rel", b"value", b"EX", b"60"]).len())
            .any(|w| w == resp(&[b"SET", b"set-rel", b"value", b"EX", b"60"]).as_slice()),
        "relative SET EX form must not be propagated"
    );

    assert_eq!(
        dispatch_as_primary(31, &mut db, &[b"SET", b"getex-rel", b"value"]),
        b"+OK\r\n"
    );
    let _ = cap.drain();

    let reply = dispatch_as_primary(32, &mut db, &[b"GETEX", b"getex-rel", b"EX", b"60"]);
    assert_eq!(reply, b"$5\r\nvalue\r\n");
    let stream = cap.drain();
    assert!(
        stream
            .windows(b"PEXPIREAT".len())
            .any(|w| w == b"PEXPIREAT"),
        "GETEX EX must propagate as PEXPIREAT, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream
            .windows(resp(&[b"GETEX", b"getex-rel", b"EX", b"60"]).len())
            .any(|w| w == resp(&[b"GETEX", b"getex-rel", b"EX", b"60"]).as_slice()),
        "relative GETEX EX form must not be propagated"
    );
}

#[test]
fn r1_db_select_precedes_db_switching_replication_stream() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.selected_db
        .store(-1, std::sync::atomic::Ordering::SeqCst);
    let cap = ReplCapture::attach(900_016, 0);
    let mut db5 = RedisDb::new(5);

    let reply = dispatch_as_primary_on_db(33, 5, &mut db5, &[b"SET", b"k5", b"v"]);
    assert_eq!(reply, b"+OK\r\n");
    let stream = cap.drain();
    let select5 = resp(&[b"SELECT", b"5"]);
    let set5 = resp(&[b"SET", b"k5", b"v"]);
    let select_pos = stream
        .windows(select5.len())
        .position(|w| w == select5.as_slice())
        .expect("first DB 5 write should emit SELECT 5");
    let set_pos = stream
        .windows(set5.len())
        .position(|w| w == set5.as_slice())
        .expect("first DB 5 write should emit SET");
    assert!(
        select_pos < set_pos,
        "SELECT 5 must precede the DB 5 write, got {:?}",
        String::from_utf8_lossy(&stream)
    );

    let reply = dispatch_as_primary_on_db(34, 5, &mut db5, &[b"SET", b"k5b", b"v"]);
    assert_eq!(reply, b"+OK\r\n");
    let stream = cap.drain();
    assert!(
        !stream
            .windows(select5.len())
            .any(|w| w == select5.as_slice()),
        "consecutive writes in DB 5 should not resend SELECT 5, got {:?}",
        String::from_utf8_lossy(&stream)
    );

    let mut db0 = RedisDb::new(0);
    let reply = dispatch_as_primary_on_db(35, 0, &mut db0, &[b"SET", b"k0", b"v"]);
    assert_eq!(reply, b"+OK\r\n");
    let stream = cap.drain();
    let select0 = resp(&[b"SELECT", b"0"]);
    let set0 = resp(&[b"SET", b"k0", b"v"]);
    let select_pos = stream
        .windows(select0.len())
        .position(|w| w == select0.as_slice())
        .expect("switching back to DB 0 should emit SELECT 0");
    let set_pos = stream
        .windows(set0.len())
        .position(|w| w == set0.as_slice())
        .expect("DB 0 write should emit SET");
    assert!(
        select_pos < set_pos,
        "SELECT 0 must precede the DB 0 write, got {:?}",
        String::from_utf8_lossy(&stream)
    );
}

// ─── Finding #2: IN-MULTI REPLICAOF ──────────────────────────────────────────

/// AUDIT FINDING #2 (regression guard): queuing a write together with a role
/// change inside MULTI then EXEC must not abort the later queued write with
/// READONLY.
///
/// A top-level write should fail the instant the global replication state is a
/// replica. A queued command already inside EXEC is different: Valkey drains the
/// queued argv through `call` rather than re-entering processCommand, so the
/// processCommand-only read-only-replica gate must not fire. We model the
/// mid-EXEC "now a replica" condition by flipping the global repl state, then
/// compare ordinary top-level dispatch with the production in-EXEC marker.
#[test]
fn finding2_write_after_inmulti_role_change_should_not_readonly() {
    let _g = repl_guard();
    let repl = global_replication_state();

    // Snapshot + flip the live global into replica mode (the state REPLICAOF
    // would establish mid-EXEC), then restore afterwards.
    let was_replica = repl.is_replica();
    repl.become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 1);

    let mut top_level_db = RedisDb::new(0);
    let top_level_reply = dispatch_as_primary(21, &mut top_level_db, &[b"SET", b"k", b"v"]);
    let mut exec_db = RedisDb::new(0);
    let exec_reply = dispatch_as_exec_drain(22, &mut exec_db, &[b"SET", b"k", b"v"]);

    // restore global state for sibling tests
    repl.become_master();
    if was_replica {
        repl.become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 1);
    }

    assert!(
        top_level_reply.starts_with(b"-READONLY"),
        "ordinary top-level writes on a read-only replica must still be \
         rejected, but got: {:?}",
        String::from_utf8_lossy(&top_level_reply),
    );
    assert!(
        !exec_reply.starts_with(b"-READONLY"),
        "a write issued in the role-change-in-MULTI scenario must not be \
         rejected READONLY, but got: {:?}",
        String::from_utf8_lossy(&exec_reply),
    );
}

// ─── R2 FULL-SYNC RDB LOADER ANCHOR ──────────────────────────────────────────

/// Historical audit finding #3 found that the live replica dialer read the
/// master's FULLRESYNC RDB bytes without applying them. The dialer now routes
/// those bytes through the runtime-owner `LoadRdb` queue, so this test remains
/// as the deterministic inner-loop anchor for the lower-level RDB
/// save→load round-trip (`save_rdb` → `load_into`) that the live handoff
/// depends on.
#[test]
fn r2_rdb_roundtrip_reconstructs_keyspace_loader() {
    let dir = unique_temp_dir("repl-kit-rdb");
    std::fs::create_dir_all(&dir).unwrap();
    let rdb_path = dir.join("dump.rdb");

    // Master keyspace.
    let mut master = RedisDb::new(0);
    master.add(
        RedisString::from_bytes(b"a"),
        redis_core::RedisObject::new_string(b"1"),
    );
    master.add(
        RedisString::from_bytes(b"b"),
        redis_core::RedisObject::new_string(b"22"),
    );

    redis_core::rdb::save_rdb(&master, &rdb_path).expect("master RDB save");

    // What `ingest_rdb` does: load into a fresh DB.
    let mut replica = RedisDb::new(0);
    redis_core::rdb::load_into(&mut replica, &rdb_path).expect("replica RDB load");

    let a = replica
        .lookup_key_read(b"a")
        .map(|o| o.string_bytes().to_vec());
    let b = replica
        .lookup_key_read(b"b")
        .map(|o| o.string_bytes().to_vec());

    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(a.as_deref(), Some(b"1".as_slice()));
    assert_eq!(b.as_deref(), Some(b"22".as_slice()));
}

// ─── Finding #4: PARTIAL RESYNC +CONTINUE ────────────────────────────────────

/// AUDIT FINDING #4 (gap): `handle_psync` (replication.rs:969-983) marks
/// replica Online on a `+CONTINUE` partial resync and registers it, but never
/// replays `backlog[provided..master]` to the replica's outbound sender.
/// Upstream Valkey calls `addReplyReplicationBacklog` to ship `(provided..master]`.
/// Expected: after `+CONTINUE`, the bytes in `(provided_offset, master_offset]`
/// are sent to the new replica. This drives the live `psync_command` with an
/// in-window offset and a captured outbound sender, then asserts catch-up bytes
/// arrive. The test reproduces the gap (no catch-up bytes are sent).
#[test]
fn finding4_partial_resync_continue_must_replay_backlog_window() {
    let _g = repl_guard();
    let repl = global_replication_state();

    // Seed backlog so there is a definite (provided..master] window. We pick
    // `provided` = current master offset, append a known frame, and expect
    // replica to receive exactly that frame on +CONTINUE.
    let provided_offset = repl.master_offset();
    let catchup = resp(&[b"SET", b"late", b"x"]);
    repl.append_to_backlog(&catchup);
    let master_offset = repl.master_offset();
    assert!(
        master_offset > provided_offset,
        "backlog must have advanced"
    );

    // The replica connection: register a pubsub sender so `psync_command` can
    // steal it, and capture the receiver.
    let client_id: u64 = 940_001;
    let (tx, rx) = mpsc::channel();
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    pubsub.lock().unwrap().register_sender(client_id, tx);

    let mut c = Client::new(client_id);
    // PSYNC ? <provided_offset> → runid "?" matches, offset in window → +CONTINUE
    c.set_args(argv(&[
        b"PSYNC",
        b"?",
        provided_offset.to_string().as_bytes(),
    ]));
    let mut db = RedisDb::new(0);
    let server = Arc::new(RedisServer::default());
    {
        let mut ctx =
            redis_core::CommandContext::with_server(&mut c, &mut db, server, pubsub.clone());
        redis_commands::replication::psync_command(&mut ctx).expect("psync");
    }
    let reply = c.drain_reply();
    assert!(
        reply.starts_with(b"+CONTINUE"),
        "expected a partial-resync +CONTINUE, got: {:?}",
        String::from_utf8_lossy(&reply)
    );

    // Drain whatever the master pushed to the replica's outbound channel.
    let mut sent = Vec::new();
    while let Ok(chunk) = rx.try_recv() {
        sent.extend_from_slice(&chunk);
    }

    repl.remove_replica(client_id);

    assert!(
        sent.windows(catchup.len()).any(|w| w == catchup.as_slice()),
        "after +CONTINUE the master must replay backlog[{}..{}] to the replica, \
         but no catch-up bytes were sent.\n  sent: {:?}\n  expected frame: {:?}",
        provided_offset,
        master_offset,
        String::from_utf8_lossy(&sent),
        String::from_utf8_lossy(&catchup),
    );
}

// ─── P3: DEBUG REPLICATE feeds the replication stream ────────────────────────

/// P3 — `DEBUG REPLICATE <cmd> [args...]` must inject the command verbatim into
/// the replication stream (mirrors C `replicationFeedReplicas(-1, ...)`), so an
/// attached replica receives it. replication-4 uses this to force divergence;
/// the un-implemented subcommand previously aborted the whole file.
#[test]
fn p3_debug_replicate_feeds_replication_stream() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(970_001, global_replication_state().master_offset());
    let mut db = RedisDb::new(0);
    let reply = dispatch_as_primary(
        970_002,
        &mut db,
        &[b"DEBUG", b"REPLICATE", b"fake-command-1", b"xyz"],
    );
    assert!(
        reply.starts_with(b"+OK"),
        "DEBUG REPLICATE should reply +OK, got {:?}",
        String::from_utf8_lossy(&reply),
    );
    let sent = cap.drain();
    let needle = resp(&[b"fake-command-1", b"xyz"]);
    assert!(
        sent.windows(needle.len()).any(|w| w == needle.as_slice()),
        "DEBUG REPLICATE must feed the verbatim command to replicas.\n  sent: {:?}",
        String::from_utf8_lossy(&sent),
    );
}

// ─── P2: partial-resync counters (sync_full / sync_partial_ok / err) ─────────

/// P2 — the master-side `handle_psync` decision must bump the three sync
/// counters the dual-server harness asserts on (`INFO sync_full /
/// sync_partial_ok / sync_partial_err`), mirroring C `syncCommand` /
/// `masterTryPartialResynchronization`:
/// * in-window partial PSYNC → `+CONTINUE`, `sync_partial_ok += 1`
/// * out-of-window partial PSYNC (concrete replid+offset) → `+FULLRESYNC`,
///   `sync_partial_err += 1` AND `sync_full += 1`
/// * fresh `PSYNC ? -1` → `+FULLRESYNC`, `sync_full += 1`, partial counters flat
#[test]
fn p2_psync_bumps_sync_counters() {
    let _g = repl_guard();
    let repl = global_replication_state();

    let drive_psync = |client_id: u64, args: &[&[u8]]| -> Vec<u8> {
        let (tx, _rx) = mpsc::channel();
        let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
        pubsub.lock().unwrap().register_sender(client_id, tx);
        let mut c = Client::new(client_id);
        c.set_args(argv(args));
        let mut db = RedisDb::new(0);
        let server = Arc::new(RedisServer::default());
        {
            let mut ctx =
                redis_core::CommandContext::with_server(&mut c, &mut db, server, pubsub.clone());
            redis_commands::replication::psync_command(&mut ctx).expect("psync");
        }
        let reply = c.drain_reply();
        repl.remove_replica(client_id);
        reply
    };

    // Seed the backlog so there is a definite in-window offset.
    let in_window_offset = repl.master_offset();
    repl.append_to_backlog(&resp(&[b"SET", b"a", b"1"]));

    // (1) in-window partial → +CONTINUE, sync_partial_ok += 1.
    let (f0, ok0, err0) = repl.sync_counters();
    let reply = drive_psync(
        960_101,
        &[b"PSYNC", b"?", in_window_offset.to_string().as_bytes()],
    );
    let (f1, ok1, err1) = repl.sync_counters();
    assert!(
        reply.starts_with(b"+CONTINUE"),
        "want +CONTINUE, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    assert_eq!(
        (ok1, err1, f1),
        (ok0 + 1, err0, f0),
        "in-window partial must bump only sync_partial_ok"
    );

    // (2) out-of-window partial (concrete replid + impossible future offset)
    //     → +FULLRESYNC, sync_partial_err += 1 AND sync_full += 1.
    let runid = String::from_utf8(repl.runid().to_vec()).unwrap();
    let beyond = (repl.master_offset() + 1_000_000).to_string();
    let (f2, ok2, err2) = repl.sync_counters();
    let reply = drive_psync(960_102, &[b"PSYNC", runid.as_bytes(), beyond.as_bytes()]);
    let (f3, ok3, err3) = repl.sync_counters();
    assert!(
        reply.starts_with(b"+FULLRESYNC"),
        "want +FULLRESYNC, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    assert_eq!(
        (ok3, err3, f3),
        (ok2, err2 + 1, f2 + 1),
        "out-of-window partial must bump sync_partial_err and sync_full"
    );

    // (3) fresh full sync (PSYNC ? -1) → sync_full += 1, partials flat.
    let (f4, ok4, err4) = repl.sync_counters();
    let reply = drive_psync(960_103, &[b"PSYNC", b"?", b"-1"]);
    let (f5, ok5, err5) = repl.sync_counters();
    assert!(
        reply.starts_with(b"+FULLRESYNC"),
        "want +FULLRESYNC, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    assert_eq!(
        (ok5, err5, f5),
        (ok4, err4, f4 + 1),
        "fresh full sync must bump only sync_full"
    );
}

// ─── P1: replica link-state observability ────────────────────────────────────

/// P1 — the `ROLE` reply's replica state field must reflect the fine-grained
/// link phase the dialer publishes (`connect`/`connecting`/`handshake`/`sync`/
/// `connected`), not a hardcoded `connected`. This is the observability the
/// dual-server harness polls via `[lindex [$replica role] 3]`. Pure mapping
/// plus a live `role_command` drive in replica mode, restoring MASTER after.
#[test]
fn p1_role_reports_replica_link_state() {
    use redis_core::replication::replica_link_code as link;
    assert_eq!(link::as_role_str(link::CONNECT), "connect");
    assert_eq!(link::as_role_str(link::CONNECTING), "connecting");
    assert_eq!(link::as_role_str(link::HANDSHAKE), "handshake");
    assert_eq!(link::as_role_str(link::TRANSFER), "sync");
    assert_eq!(link::as_role_str(link::CONNECTED), "connected");

    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 6379);

    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    for (code, want) in [
        (link::HANDSHAKE, "handshake"),
        (link::TRANSFER, "sync"),
        (link::CONNECTED, "connected"),
    ] {
        repl.set_replica_link(code);
        let mut c = Client::new(951_001);
        c.set_args(argv(&[b"ROLE"]));
        let mut db = RedisDb::new(0);
        {
            let mut ctx = redis_core::CommandContext::with_server(
                &mut c,
                &mut db,
                server.clone(),
                pubsub.clone(),
            );
            redis_commands::replication::role_command(&mut ctx).expect("role");
        }
        let reply = c.drain_reply();
        assert!(
            reply.windows(want.len()).any(|w| w == want.as_bytes()),
            "ROLE state field should be {:?}, reply was {:?}",
            want,
            String::from_utf8_lossy(&reply),
        );
    }

    // Restore the process-global state to MASTER so later repl tests that
    // assume primary mode (should_propagate_writes) are unaffected.
    repl.become_master();
}

// ─── WAITAOF local durability guards ────────────────────────────────────────

#[test]
fn p4_waitaof_local_fsync_progress_pair() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();

    let dir = unique_temp_dir("repl-kit-waitaof-local");
    std::fs::create_dir_all(&dir).unwrap();
    let aof_path = dir.join("appendonly.aof");
    let writer = Arc::new(
        redis_commands::aof::AofWriter::open_truncate(&aof_path, redis_commands::aof::FSYNC_ALWAYS)
            .expect("open aof"),
    );
    writer.force_fsynced_repl_offset(42);
    redis_commands::aof::install_aof_writer(writer);

    let server = Arc::new(RedisServer::default());
    server.live_config.set_appendonly(true);
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let mut c = Client::new(980_001);
    c.last_write_repl_offset = 42;
    c.set_args(argv(&[b"WAITAOF", b"1", b"0", b"0"]));
    let mut db = RedisDb::new(0);
    {
        let mut ctx = redis_core::CommandContext::with_server(&mut c, &mut db, server, pubsub);
        redis_commands::replication::waitaof_command(&mut ctx).expect("waitaof");
    }

    redis_commands::aof::remove_aof_writer();
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(c.drain_reply(), b"*2\r\n:1\r\n:0\r\n");
}

#[test]
fn p4_waitaof_local_requires_appendonly() {
    let _g = repl_guard();
    global_replication_state().become_master();
    redis_commands::aof::remove_aof_writer();

    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let mut c = Client::new(980_002);
    c.last_write_repl_offset = 42;
    c.set_args(argv(&[b"WAITAOF", b"1", b"0", b"0"]));
    let mut db = RedisDb::new(0);
    let err = {
        let mut ctx = redis_core::CommandContext::with_server(&mut c, &mut db, server, pubsub);
        redis_commands::replication::waitaof_command(&mut ctx).unwrap_err()
    };
    assert!(
        err.to_resp_payload()
            .as_bytes()
            .windows(b"appendonly is disabled".len())
            .any(|w| w == b"appendonly is disabled"),
        "WAITAOF numlocal>0 with appendonly off must reject, got {:?}",
        String::from_utf8_lossy(err.to_resp_payload().as_bytes()),
    );
}

#[test]
fn p4_waitaof_local_waiter_unblocks_when_appendonly_disabled() {
    let _g = repl_guard();
    global_replication_state().become_master();
    redis_commands::aof::remove_aof_writer();

    let server = Arc::new(RedisServer::default());
    server.live_config.set_appendonly(true);
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let (tx, rx) = mpsc::channel();
    let client_id = 980_003;
    pubsub.lock().unwrap().register_sender(client_id, tx);

    let mut c = Client::new(client_id);
    c.last_write_repl_offset = 42;
    c.set_args(argv(&[b"WAITAOF", b"1", b"0", b"5000"]));
    let mut db = RedisDb::new(0);
    {
        let mut ctx = redis_core::CommandContext::with_server(&mut c, &mut db, server, pubsub);
        redis_commands::replication::waitaof_command(&mut ctx).expect("waitaof");
    }
    assert!(c.blocked_on_keys, "WAITAOF should block before local fsync");

    redis_commands::replication::unblock_waitaof_local_disabled();
    let reply = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("WAITAOF waiter should be unblocked");
    assert!(
        reply.starts_with(
            b"-ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled."
        ),
        "unexpected unblock reply: {:?}",
        String::from_utf8_lossy(&reply),
    );
}

#[test]
fn p4_wait_and_waitaof_waiters_unblock_on_role_change() {
    let _g = repl_guard();
    global_replication_state().become_master();
    redis_commands::aof::remove_aof_writer();

    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let (wait_tx, wait_rx) = mpsc::channel();
    let (waitaof_tx, waitaof_rx) = mpsc::channel();
    let wait_client_id = 980_004;
    let waitaof_client_id = 980_005;
    {
        let mut guard = pubsub.lock().unwrap();
        guard.register_sender(wait_client_id, wait_tx);
        guard.register_sender(waitaof_client_id, waitaof_tx);
    }

    let mut wait_client = Client::new(wait_client_id);
    wait_client.last_write_repl_offset = 42;
    wait_client.set_args(argv(&[b"WAIT", b"1", b"0"]));
    let mut wait_db = RedisDb::new(0);
    {
        let mut ctx = redis_core::CommandContext::with_server(
            &mut wait_client,
            &mut wait_db,
            server.clone(),
            pubsub.clone(),
        );
        redis_commands::replication::wait_command(&mut ctx).expect("wait");
    }
    assert!(
        wait_client.blocked_on_keys,
        "WAIT should block before role-change unblock"
    );

    let mut waitaof_client = Client::new(waitaof_client_id);
    waitaof_client.last_write_repl_offset = 42;
    waitaof_client.set_args(argv(&[b"WAITAOF", b"0", b"1", b"0"]));
    let mut waitaof_db = RedisDb::new(0);
    {
        let mut ctx = redis_core::CommandContext::with_server(
            &mut waitaof_client,
            &mut waitaof_db,
            server,
            pubsub,
        );
        redis_commands::replication::waitaof_command(&mut ctx).expect("waitaof");
    }
    assert!(
        waitaof_client.blocked_on_keys,
        "WAITAOF should block before role-change unblock"
    );

    global_replication_state().become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 6379);
    let mut role_change_client = Client::new(980_006);
    role_change_client.set_args(argv(&[b"REPLICAOF", b"NO", b"ONE"]));
    let mut role_change_db = RedisDb::new(0);
    {
        let mut ctx = redis_core::CommandContext::with_server(
            &mut role_change_client,
            &mut role_change_db,
            Arc::new(RedisServer::default()),
            Arc::new(Mutex::new(PubSubRegistry::new())),
        );
        redis_commands::replication::replicaof_command(&mut ctx).expect("replicaof no one");
    }
    assert_eq!(role_change_client.drain_reply(), b"+OK\r\n");

    for (name, rx) in [("WAIT", wait_rx), ("WAITAOF", waitaof_rx)] {
        let reply = rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or_else(|_| panic!("{name} waiter should be unblocked"));
        assert!(
            reply.starts_with(
                b"-UNBLOCKED force unblock from blocking operation, instance state changed"
            ),
            "{name} role-change unblock reply was {:?}",
            String::from_utf8_lossy(&reply),
        );
    }
}

// ─── AOF round-trip (green capability) ───────────────────────────────────────

/// GREEN — AOF append→bytes→replay→assert-dbs-equal on the live codec.
/// Appends two SETs through the real `AofWriter` to a temp file, replays
/// file into a fresh DB, and asserts key-for-key equality. Proves the AOF
/// encode/replay seam is faithful (the inner loop for AOF correctness work).
#[test]
fn aof_append_replay_roundtrip_reconstructs_keyspace() {
    use redis_commands::aof::{replay_aof, AofWriter};

    let dir = unique_temp_dir("repl-kit-aof");
    std::fs::create_dir_all(&dir).unwrap();
    let aof_path = dir.join("appendonly.aof");

    {
        let writer = AofWriter::open_truncate(&aof_path, 0).expect("open aof");
        writer
            .append_selected(0, &argv(&[b"SET", b"x", b"1"]))
            .unwrap();
        writer
            .append_selected(0, &argv(&[b"SET", b"y", b"2"]))
            .unwrap();
        writer.flush().unwrap();
    }

    let mut db = RedisDb::new(0);
    replay_aof(&aof_path, &mut db).expect("replay aof");

    let x = db.lookup_key_read(b"x").map(|o| o.string_bytes().to_vec());
    let y = db.lookup_key_read(b"y").map(|o| o.string_bytes().to_vec());

    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(x.as_deref(), Some(b"1".as_slice()));
    assert_eq!(y.as_deref(), Some(b"2".as_slice()));
}

// ─── temp dir helper ─────────────────────────────────────────────────────────

fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("{}-{}-{}-{}", tag, std::process::id(), nanos, n))
}
