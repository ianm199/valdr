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
//! → conn.outbound_sender.send(bytes) (per-replica mpsc)
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

use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

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
    let mut c = Client::new(client_id);
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

// ─── Finding #1: NO-OP-IN-MULTI PROPAGATION ─────────────────────────────────

/// AUDIT FINDING #1 (active bug): dispatch.rs has no `server.dirty`-delta gate
/// around the handler call (~L616/L661), and `multi.rs::run_one_queued`
/// (L293) decides propagation purely from `command_is_write_or_may_replicate`
/// + `prevent_propagation`. `db.rs::del_generic_command` (L1278) never calls
/// `set_prevent_propagation()` on a zero-delete no-op (see the TODO at L1296:
/// "server.dirty++"). So a no-op `DEL missing` inside MULTI/EXEC is wrongly
/// propagated.
/// Expected:
/// no-op DEL must NOT appear in the replication stream.
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

// ─── Finding #1b: NO-OP-AT-TOP-LEVEL PROPAGATION (companion) ─────────────────

/// AUDIT FINDING #1 (companion, top-level path): the same missing
/// `server.dirty` gate at the dispatch tail (dispatch.rs:661 only checks
/// `should_propagate_write_command`, which checks `prevent_propagation` —
/// never set by the no-op DEL). A top-level no-op `DEL missing` is wrongly
/// propagated too. Pinned separately so a fix that only patches the MULTI path
/// still flags the top-level leak.
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

// ─── Finding #2: IN-MULTI REPLICAOF ──────────────────────────────────────────

/// AUDIT FINDING #2 (active bug): queuing a write together with a role change
/// inside MULTI then EXEC aborts the transaction with READONLY.
/// The faithful, fully in-memory reproduction of the gate that causes it:
/// the readonly gate (dispatch.rs:1950 `enforce_replica_readonly_gate`) fires
/// for an ordinary write the instant `global_replication_state.is_replica`
/// is true. Inside EXEC, a role change earlier in the same transaction flips
/// that global mid-run, so a following queued write hits READONLY. We model
/// "now a replica" condition deterministically by flipping the global repl
/// state to replica (no TCP, no dialer thread — the real `REPLICAOF` handler
/// replication.rs:43 does a blocking TCP connect + thread spawn, which is not
/// in-memory-safe; see notes), then dispatching a SET. Expected: the write must
/// not be rejected with READONLY in this transaction-internal scenario.
#[test]
fn finding2_write_after_inmulti_role_change_should_not_readonly() {
    let _g = repl_guard();
    let repl = global_replication_state();

 // Snapshot + flip the live global into replica mode (the state REPLICAOF
 // would establish mid-EXEC), then restore afterwards.
    let was_replica = repl.is_replica();
    repl.become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 1);

    let mut db = RedisDb::new(0);
    let reply = dispatch_as_primary(21, &mut db, &[b"SET", b"k", b"v"]);

 // restore global state for sibling tests
    repl.become_master();
    if was_replica {
        repl.become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 1);
    }

    assert!(
        !reply.starts_with(b"-READONLY"),
        "a write issued in the role-change-in-MULTI scenario must not be \
         rejected READONLY, but got: {:?}",
        String::from_utf8_lossy(&reply),
    );
}

// ─── Finding #3: REPLICA DISCARDS RDB ────────────────────────────────────────

/// AUDIT FINDING #3 (#1 gap): the live replica dialer
/// `handshake_sink_loop` (replica_dialer.rs:102, spawned by
/// `spawn_replica_dialer` at L92) reads the master's FULLRESYNC RDB via
/// `read_fullresync_rdb` (L135) and **discards** the returned `Vec<u8>` — only
/// its `Err` is inspected. The one function that actually loads an RDB,
/// `ingest_rdb` (L470), is called only from `dialer_loop` (L233), which has
/// **zero callers** (verified: `grep -n dialer_loop` shows only its
/// definition). So after a full sync the replica keyspace is empty rather than
/// equal to the master's.
/// `read_fullresync_rdb` and `ingest_rdb` are private, so this test pins
/// reachable, deterministic seam: the RDB save→load round-trip (`save_rdb` →
/// `load_into`) that `ingest_rdb` *would* drive faithfully reconstructs
/// master keyspace. The test therefore PASSES (the loader works), and its
/// doc-comment + the notes record that the bug is that the live sink loop never
/// calls this loader. Classified green-already-correct for the loader;
/// dead-code wiring gap is documented as inconclusive-at-this-level.
#[test]
fn finding3_rdb_roundtrip_reconstructs_keyspace_loader_is_dead_in_sink_loop() {
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
    let reply = drive_psync(960_101, &[b"PSYNC", b"?", in_window_offset.to_string().as_bytes()]);
    let (f1, ok1, err1) = repl.sync_counters();
    assert!(reply.starts_with(b"+CONTINUE"), "want +CONTINUE, got {:?}", String::from_utf8_lossy(&reply));
    assert_eq!((ok1, err1, f1), (ok0 + 1, err0, f0), "in-window partial must bump only sync_partial_ok");

 // (2) out-of-window partial (concrete replid + impossible future offset)
 //     → +FULLRESYNC, sync_partial_err += 1 AND sync_full += 1.
    let runid = String::from_utf8(repl.runid().to_vec()).unwrap();
    let beyond = (repl.master_offset() + 1_000_000).to_string();
    let (f2, ok2, err2) = repl.sync_counters();
    let reply = drive_psync(960_102, &[b"PSYNC", runid.as_bytes(), beyond.as_bytes()]);
    let (f3, ok3, err3) = repl.sync_counters();
    assert!(reply.starts_with(b"+FULLRESYNC"), "want +FULLRESYNC, got {:?}", String::from_utf8_lossy(&reply));
    assert_eq!((ok3, err3, f3), (ok2, err2 + 1, f2 + 1), "out-of-window partial must bump sync_partial_err and sync_full");

 // (3) fresh full sync (PSYNC ? -1) → sync_full += 1, partials flat.
    let (f4, ok4, err4) = repl.sync_counters();
    let reply = drive_psync(960_103, &[b"PSYNC", b"?", b"-1"]);
    let (f5, ok5, err5) = repl.sync_counters();
    assert!(reply.starts_with(b"+FULLRESYNC"), "want +FULLRESYNC, got {:?}", String::from_utf8_lossy(&reply));
    assert_eq!((ok5, err5, f5), (ok4, err4, f4 + 1), "fresh full sync must bump only sync_full");
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
