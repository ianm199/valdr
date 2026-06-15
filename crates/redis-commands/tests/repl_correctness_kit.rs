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
use std::time::{Duration, Instant};

use redis_core::blocked_keys::{blocked_keys_index, BlockedAction, BlockedSide, BlockedWaiter};
use redis_core::db::LOOKUP_NOTOUCH;
use redis_core::metrics::{command_stats_snapshot, reset_command_stats};
use redis_core::replication::{
    global_replication_state, ReplBgsaveJob, ReplicaConn, ReplicaState, ReplicationState,
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

fn dispatch_result_as_primary(client_id: u64, db: &mut RedisDb, cmd: &[&[u8]]) -> Vec<u8> {
    let mut c = Client::new(client_id);
    c.set_args(argv(cmd));
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let result = {
        let mut ctx = redis_core::CommandContext::with_server(&mut c, db, server, pubsub);
        dispatch(&mut ctx)
    };
    match result {
        Ok(()) => c.drain_reply(),
        Err(err) => err.to_resp_payload().as_bytes().to_vec(),
    }
}

fn block_blpop(client_id: u64, db: &mut RedisDb, key: &[u8]) -> Receiver<Vec<u8>> {
    {
        let mut idx = blocked_keys_index()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let _ = idx.remove_client(client_id);
    }

    let (tx, rx) = mpsc::channel();
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    {
        let mut guard = pubsub.lock().unwrap();
        guard.register_sender(client_id, tx);
    }
    let mut c = Client::new(client_id);
    c.set_args(argv(&[b"BLPOP".as_slice(), key, b"0".as_slice()]));
    let server = Arc::new(RedisServer::default());
    {
        let mut ctx = redis_core::CommandContext::with_server(&mut c, db, server, pubsub);
        dispatch(&mut ctx).expect("BLPOP dispatch should park");
    }
    assert!(c.blocked_on_keys, "BLPOP should park on an empty key");
    assert_eq!(
        c.drain_reply(),
        b"",
        "blocked BLPOP must not synchronously reply"
    );
    rx
}

fn count_subsequence(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .filter(|w| *w == needle)
        .count()
}

fn assert_reply_contains(reply: &[u8], needle: &[u8]) {
    assert!(
        reply.windows(needle.len()).any(|w| w == needle),
        "reply {:?} did not contain {:?}",
        String::from_utf8_lossy(reply),
        String::from_utf8_lossy(needle),
    );
}

fn wait_until_logically_expired(db: &RedisDb, key: &RedisString) {
    let deadline = Instant::now() + Duration::from_millis(250);
    while !db.is_expired(key) {
        assert!(
            Instant::now() < deadline,
            "key {:?} did not become logically expired before deadline",
            String::from_utf8_lossy(key.as_ref()),
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn info_field_value(reply: &[u8], name: &str) -> Option<String> {
    let first_line = reply.windows(2).position(|w| w == b"\r\n")?;
    let payload = &reply[first_line + 2..reply.len().saturating_sub(2)];
    let text = String::from_utf8_lossy(payload);
    text.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(':'))
        .map(|value| value.trim_end_matches('\r').to_string())
}

fn command_stat_calls(name: &[u8]) -> u64 {
    command_stats_snapshot()
        .into_iter()
        .find(|stat| stat.name.eq_ignore_ascii_case(name))
        .map(|stat| stat.calls)
        .unwrap_or(0)
}

fn command_stat_counts(name: &[u8]) -> (u64, u64, u64) {
    command_stats_snapshot()
        .into_iter()
        .find(|stat| stat.name.eq_ignore_ascii_case(name))
        .map(|stat| (stat.calls, stat.rejected_calls, stat.failed_calls))
        .unwrap_or((0, 0, 0))
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

fn dispatch_as_replica_apply(client_id: u64, db: &mut RedisDb, cmd: &[&[u8]]) -> Vec<u8> {
    let mut c = Client::new(client_id);
    c.replication_apply = true;
    c.db_index = db.id;
    c.set_authenticated_user(Some(RedisString::from_static(b"default")));
    c.set_args(argv(cmd));
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    {
        let mut ctx = redis_core::CommandContext::with_server(&mut c, db, server, pubsub);
        let _ = dispatch(&mut ctx);
    }
    c.drain_reply()
}

fn dispatch_as_replica_apply_on_dbs(
    client_id: u64,
    dbs: &mut [RedisDb],
    selected_db: &mut u32,
    cmd: &[&[u8]],
) -> Vec<u8> {
    let mut c = Client::new(client_id);
    c.replication_apply = true;
    c.db_index = *selected_db;
    c.set_authenticated_user(Some(RedisString::from_static(b"default")));
    c.set_args(argv(cmd));
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    {
        let mut ctx =
            redis_core::CommandContext::with_server_and_db_list(&mut c, dbs, server, pubsub);
        let _ = dispatch(&mut ctx);
    }
    *selected_db = c.db_index;
    c.drain_reply()
}

fn dispatch_as_primary_on_dbs(
    client_id: u64,
    dbs: &mut [RedisDb],
    selected_db: &mut u32,
    cmd: &[&[u8]],
) -> Vec<u8> {
    let mut c = Client::new(client_id);
    c.db_index = *selected_db;
    c.set_args(argv(cmd));
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    {
        let mut ctx =
            redis_core::CommandContext::with_server_and_db_list(&mut c, dbs, server, pubsub);
        let _ = dispatch(&mut ctx);
    }
    *selected_db = c.db_index;
    c.drain_reply()
}

fn debug_digest(client_id: u64, dbs: &mut [RedisDb], selected_db: &mut u32) -> Vec<u8> {
    let reply = dispatch_as_primary_on_dbs(client_id, dbs, selected_db, &[b"DEBUG", b"DIGEST"]);
    assert!(
        reply.starts_with(b"$40\r\n") && reply.ends_with(b"\r\n"),
        "DEBUG DIGEST should return a 40-byte bulk string, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    reply[5..45].to_vec()
}

fn apply_resp_stream_as_replica_on_dbs(
    client_id: u64,
    dbs: &mut [RedisDb],
    selected_db: &mut u32,
    stream: &[u8],
) -> Vec<Vec<u8>> {
    let mut c = Client::new(client_id);
    c.replication_apply = true;
    c.suppress_monitor = true;
    c.db_index = *selected_db;
    c.set_authenticated_user(Some(RedisString::from_static(b"default")));
    c.query_buf.extend_from_slice(stream);

    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let mut consumed_total = 0usize;
    let mut replies = Vec::new();

    loop {
        match c.parse_query_buffer_into_argv(consumed_total) {
            Ok(Some(consumed)) => {
                consumed_total += consumed;
                if c.argv.is_empty() {
                    continue;
                }
                let result = {
                    let mut ctx = redis_core::CommandContext::with_server_and_db_list(
                        &mut c,
                        dbs,
                        Arc::clone(&server),
                        Arc::clone(&pubsub),
                    );
                    dispatch(&mut ctx)
                };
                assert!(
                    result.is_ok(),
                    "replica stream command failed for argv {:?}: {:?}",
                    c.argv
                        .iter()
                        .map(|arg| String::from_utf8_lossy(arg.as_bytes()).to_string())
                        .collect::<Vec<_>>(),
                    result.err()
                );
                replies.push(c.drain_reply());
                c.reset_args();
            }
            Ok(None) => break,
            Err(err) => panic!("replica stream parse failed: {:?}", err),
        }
    }

    *selected_db = c.db_index;
    replies
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

#[test]
fn r2_info_stats_counts_replication_output_bytes() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let cap = ReplCapture::attach(900_019, repl.master_offset());
    let mut db = RedisDb::new(0);

    assert_eq!(
        dispatch_as_primary(19, &mut db, &[b"CONFIG", b"RESETSTAT"]),
        b"+OK\r\n"
    );
    let before = dispatch_as_primary(20, &mut db, &[b"INFO", b"stats"]);
    assert_eq!(
        info_field_value(&before, "total_net_repl_output_bytes").as_deref(),
        Some("0"),
        "INFO stats should expose the replication output byte counter after RESETSTAT"
    );

    assert_eq!(
        dispatch_as_primary(21, &mut db, &[b"SET", b"metric-key", b"value"]),
        b"+OK\r\n"
    );
    let stream = cap.drain();
    assert!(
        !stream.is_empty(),
        "SET should fan out bytes to the capture replica"
    );
    let after = dispatch_as_primary(22, &mut db, &[b"INFO", b"stats"]);
    let count = info_field_value(&after, "total_net_repl_output_bytes")
        .expect("INFO stats should include total_net_repl_output_bytes")
        .parse::<u64>()
        .expect("replication output byte counter should be numeric");
    assert!(
        count >= stream.len() as u64,
        "replication output byte counter should include queued stream bytes; count={count}, stream_len={}",
        stream.len()
    );
}

#[test]
fn replica_apply_relays_empty_flushes_to_downstream_replicas() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let cap = ReplCapture::attach(900_031, 0);
    repl.become_replica_of(RedisString::from_static(b"127.0.0.1"), 6379);

    let mut db = RedisDb::new(0);
    assert_eq!(
        dispatch_as_replica_apply(31, &mut db, &[b"FLUSHDB"]),
        b"+OK\r\n"
    );
    assert_eq!(
        dispatch_as_replica_apply(32, &mut db, &[b"FLUSHALL"]),
        b"+OK\r\n"
    );
    assert_eq!(
        dispatch_as_replica_apply(33, &mut db, &[b"EVAL", b"redis.call('flushdb')", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        dispatch_as_replica_apply(34, &mut db, &[b"EVAL", b"redis.call('flushall')", b"0"]),
        b"+OK\r\n"
    );

    let stream = cap.drain();
    let flushdb = resp(&[b"FLUSHDB"]);
    let flushall = resp(&[b"FLUSHALL"]);
    let flushdb_script = resp(&[b"flushdb"]);
    let flushall_script = resp(&[b"flushall"]);
    assert_eq!(
        count_subsequence(&stream, &flushdb),
        1,
        "replica apply must relay direct FLUSHDB to downstream replicas, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert_eq!(
        count_subsequence(&stream, &flushdb_script),
        1,
        "replica apply must relay script FLUSHDB to downstream replicas, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert_eq!(
        count_subsequence(&stream, &flushall),
        1,
        "replica apply must relay direct FLUSHALL to downstream replicas, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert_eq!(
        count_subsequence(&stream, &flushall_script),
        1,
        "replica apply must relay script FLUSHALL to downstream replicas, got {:?}",
        String::from_utf8_lossy(&stream)
    );

    repl.become_master();
}

#[test]
fn replica_fullsync_stream_starts_at_upstream_selected_db() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    repl.become_replica_of(RedisString::from_static(b"127.0.0.1"), 6379);

    let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut selected_db = 0;
    assert_eq!(
        dispatch_as_replica_apply_on_dbs(35, &mut dbs, &mut selected_db, &[b"SELECT", b"9"]),
        b"+OK\r\n"
    );
    assert_eq!(selected_db, 9);

    repl.reset_selected_db_for_full_resync();
    let cap = ReplCapture::attach(900_032, 0);
    assert_eq!(
        dispatch_as_replica_apply_on_dbs(
            36,
            &mut dbs,
            &mut selected_db,
            &[b"SET", b"key", b"value"]
        ),
        b"+OK\r\n"
    );

    let stream = cap.drain();
    let select9 = resp(&[b"SELECT", b"9"]);
    assert!(
        !stream
            .windows(select9.len())
            .any(|w| w == select9.as_slice()),
        "full-sync from a replica should treat the upstream stream DB as already selected, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        stream
            .windows(resp(&[b"SET", b"key", b"value"]).len())
            .any(|w| w == resp(&[b"SET", b"key", b"value"]).as_slice()),
        "replica-applied DB 9 write should still relay downstream, got {:?}",
        String::from_utf8_lossy(&stream)
    );

    repl.become_master();
}

#[test]
fn replica_apply_partial_catchup_preserves_db9_for_minus_zero_overwrite() {
    let _g = repl_guard();
    let mut dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut selected_db = 0;
    let key = b"316637927";

    let mut first_stream = Vec::new();
    first_stream.extend(resp(&[b"SELECT", b"9"]));
    first_stream.extend(resp(&[b"SET", key, b"-0"]));
    let replies =
        apply_resp_stream_as_replica_on_dbs(37, &mut dbs, &mut selected_db, &first_stream);
    assert_eq!(selected_db, 9);
    assert_eq!(replies, vec![b"+OK\r\n".to_vec(), b"+OK\r\n".to_vec()]);

    let catchup = resp(&[b"SET", key, b"0"]);
    let replies = apply_resp_stream_as_replica_on_dbs(38, &mut dbs, &mut selected_db, &catchup);
    assert_eq!(selected_db, 9);
    assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);

    let db9 = dbs[9]
        .lookup_key_read(key)
        .map(|obj| obj.string_bytes().to_vec());
    let db0 = dbs[0]
        .lookup_key_read(key)
        .map(|obj| obj.string_bytes().to_vec());
    assert_eq!(db9.as_deref(), Some(b"0".as_slice()));
    assert_eq!(db0, None, "partial catch-up must not fall back to DB 0");
}

#[test]
fn primary_db9_overwrite_stream_replays_to_replica_db9() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    repl.reset_selected_db_for_full_resync();
    let cap = ReplCapture::attach(900_033, repl.master_offset());

    let key = b"316637927";
    let mut master_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut master_selected_db = 0;
    assert_eq!(
        dispatch_as_primary_on_dbs(
            39,
            &mut master_dbs,
            &mut master_selected_db,
            &[b"SELECT", b"9"]
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        dispatch_as_primary_on_dbs(
            40,
            &mut master_dbs,
            &mut master_selected_db,
            &[b"SET", key, b"-0"]
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        dispatch_as_primary_on_dbs(
            41,
            &mut master_dbs,
            &mut master_selected_db,
            &[b"SET", key, b"0"]
        ),
        b"+OK\r\n"
    );

    let stream = cap.drain();
    let select9 = resp(&[b"SELECT", b"9"]);
    assert!(
        stream
            .windows(select9.len())
            .any(|window| window == select9.as_slice()),
        "DB 9 primary stream must include SELECT before writes, got {:?}",
        String::from_utf8_lossy(&stream)
    );

    let mut replica_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut replica_selected_db = 0;
    apply_resp_stream_as_replica_on_dbs(42, &mut replica_dbs, &mut replica_selected_db, &stream);

    let value = replica_dbs[9]
        .lookup_key_read(key)
        .map(|obj| obj.string_bytes().to_vec());
    assert_eq!(replica_selected_db, 9);
    assert_eq!(value.as_deref(), Some(b"0".as_slice()));
    assert!(
        replica_dbs[0].lookup_key_read(key).is_none(),
        "DB 0 must not receive the DB 9 overwrite"
    );

    repl.become_master();
}

#[test]
fn fullsync_rdb_minus_zero_then_db9_catchup_zero_reconstructs_latest_value() {
    let _g = repl_guard();
    let key = b"316637927";
    let dir = unique_temp_dir("repl-kit-fullsync-db9");
    std::fs::create_dir_all(&dir).unwrap();
    let rdb_path = dir.join("dump.rdb");

    let mut master_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut master_selected_db = 0;
    assert_eq!(
        dispatch_as_primary_on_dbs(
            43,
            &mut master_dbs,
            &mut master_selected_db,
            &[b"SELECT", b"9"]
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        dispatch_as_primary_on_dbs(
            44,
            &mut master_dbs,
            &mut master_selected_db,
            &[b"SET", key, b"-0"]
        ),
        b"+OK\r\n"
    );

    redis_core::rdb::save_rdb_databases(&master_dbs, &rdb_path).expect("save full-sync RDB");

    let mut replica_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    redis_core::rdb::load_into_dbs_replacing(&mut replica_dbs, &rdb_path)
        .expect("load full-sync RDB");
    assert_eq!(
        replica_dbs[9]
            .lookup_key_read(key)
            .map(|obj| obj.string_bytes().to_vec())
            .as_deref(),
        Some(b"-0".as_slice()),
        "RDB load should preserve the pre-catch-up DB 9 value exactly"
    );

    let mut catchup = Vec::new();
    catchup.extend(resp(&[b"SELECT", b"9"]));
    catchup.extend(resp(&[b"SET", key, b"0"]));
    let mut replica_selected_db = 0;
    apply_resp_stream_as_replica_on_dbs(45, &mut replica_dbs, &mut replica_selected_db, &catchup);
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(replica_selected_db, 9);
    assert_eq!(
        replica_dbs[9]
            .lookup_key_read(key)
            .map(|obj| obj.string_bytes().to_vec())
            .as_deref(),
        Some(b"0".as_slice())
    );
    assert!(replica_dbs[0].lookup_key_read(key).is_none());
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
fn r1_set_store_commands_rewrite_to_deterministic_destination_updates() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_016, 0);
    let mut db = RedisDb::new(0);

    assert_eq!(
        dispatch_as_primary(30, &mut db, &[b"SADD", b"src", b"a"]),
        b":1\r\n"
    );
    assert_eq!(
        dispatch_as_primary(31, &mut db, &[b"SADD", b"other", b"b"]),
        b":1\r\n"
    );
    let _ = cap.drain();

    let reply = dispatch_as_primary(32, &mut db, &[b"SUNIONSTORE", b"dst", b"src", b"other"]);
    assert_eq!(reply, b":2\r\n");
    let stream = cap.drain();

    assert!(
        !stream
            .windows(b"SUNIONSTORE".len())
            .any(|w| w == b"SUNIONSTORE"),
        "source-dependent SUNIONSTORE must not be propagated verbatim: {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        stream
            .windows(resp(&[b"DEL", b"dst"]).len())
            .any(|w| w == resp(&[b"DEL", b"dst"]).as_slice()),
        "store rewrite must clear the destination before replaying members: {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        stream.windows(b"SADD".len()).any(|w| w == b"SADD")
            && stream.windows(b"dst".len()).any(|w| w == b"dst")
            && stream.windows(b"a".len()).any(|w| w == b"a")
            && stream.windows(b"b".len()).any(|w| w == b"b"),
        "store rewrite must replay the concrete destination members: {:?}",
        String::from_utf8_lossy(&stream)
    );

    let reply = dispatch_as_primary(33, &mut db, &[b"SINTERSTORE", b"empty", b"src", b"missing"]);
    assert_eq!(reply, b":0\r\n");
    let stream = cap.drain();
    assert!(
        !stream
            .windows(b"SINTERSTORE".len())
            .any(|w| w == b"SINTERSTORE"),
        "empty SINTERSTORE must not be propagated verbatim: {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        stream
            .windows(resp(&[b"DEL", b"empty"]).len())
            .any(|w| w == resp(&[b"DEL", b"empty"]).as_slice()),
        "empty store rewrite must delete the destination: {:?}",
        String::from_utf8_lossy(&stream)
    );
}

#[test]
fn r1_zset_store_commands_rewrite_to_deterministic_destination_updates() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_017, 0);
    let mut db = RedisDb::new(0);

    assert_eq!(
        dispatch_as_primary(34, &mut db, &[b"ZADD", b"za", b"1", b"a"]),
        b":1\r\n"
    );
    assert_eq!(
        dispatch_as_primary(35, &mut db, &[b"ZADD", b"zb", b"2", b"b"]),
        b":1\r\n"
    );
    let _ = cap.drain();

    let reply = dispatch_as_primary(36, &mut db, &[b"ZUNIONSTORE", b"zdst", b"2", b"za", b"zb"]);
    assert_eq!(reply, b":2\r\n");
    let stream = cap.drain();

    assert!(
        !stream
            .windows(b"ZUNIONSTORE".len())
            .any(|w| w == b"ZUNIONSTORE"),
        "source-dependent ZUNIONSTORE must not be propagated verbatim: {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        stream
            .windows(resp(&[b"DEL", b"zdst"]).len())
            .any(|w| w == resp(&[b"DEL", b"zdst"]).as_slice()),
        "zset store rewrite must clear the destination first: {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        stream
            .windows(resp(&[b"ZADD", b"zdst", b"1", b"a", b"2", b"b"]).len())
            .any(|w| w == resp(&[b"ZADD", b"zdst", b"1", b"a", b"2", b"b"]).as_slice()),
        "zset store rewrite must replay concrete score/member pairs: {:?}",
        String::from_utf8_lossy(&stream)
    );

    let reply = dispatch_as_primary(
        37,
        &mut db,
        &[b"ZINTERSTORE", b"empty-zdst", b"2", b"za", b"missing"],
    );
    assert_eq!(reply, b":0\r\n");
    let stream = cap.drain();
    assert!(
        !stream
            .windows(b"ZINTERSTORE".len())
            .any(|w| w == b"ZINTERSTORE"),
        "empty ZINTERSTORE must not be propagated verbatim: {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        stream
            .windows(resp(&[b"DEL", b"empty-zdst"]).len())
            .any(|w| w == resp(&[b"DEL", b"empty-zdst"]).as_slice()),
        "empty zset store rewrite must delete the destination: {:?}",
        String::from_utf8_lossy(&stream)
    );
}

#[test]
fn r1_set_store_first_fullsync_catchup_rewrite_selects_db9() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let _ = repl.take_repl_bgsave_job();
    repl.set_repl_child_pid(0);

    let mut primary_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut primary_selected_db = 9;
    assert_eq!(
        dispatch_as_primary_on_dbs(
            34,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"SADD", b"src", b"a"]
        ),
        b":1\r\n"
    );
    assert_eq!(
        dispatch_as_primary_on_dbs(
            35,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"SADD", b"other", b"b"]
        ),
        b":1\r\n"
    );

    repl.selected_db.store(-1, Ordering::Release);
    let snapshot_offset = repl.master_offset();
    let dir = unique_temp_dir("repl-kit-set-store-catchup");
    let temp_path = dir.join("temp-repl-set-store.rdb");
    repl.install_repl_bgsave_job(ReplBgsaveJob {
        child_pid: 0,
        temp_path: temp_path.clone(),
        waiting_replicas: vec![900_017],
        snapshot_offset,
        catch_up_bytes: Vec::new(),
        needs_getack_on_completion: false,
    });

    assert_eq!(
        dispatch_as_primary_on_dbs(
            36,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"SUNIONSTORE", b"dst", b"src", b"other"]
        ),
        b":2\r\n"
    );
    let job = repl
        .take_repl_bgsave_job()
        .expect("active full-sync job should capture rewritten set-store bytes");
    let _ = std::fs::remove_file(&temp_path);
    let _ = std::fs::remove_dir_all(&dir);

    let catchup = job.catch_up_bytes;
    assert!(
        catchup.starts_with(&resp(&[b"SELECT", b"9"])),
        "first active full-sync set-store rewrite must select DB 9, got {:?}",
        String::from_utf8_lossy(&catchup)
    );
    assert!(
        !catchup
            .windows(b"SUNIONSTORE".len())
            .any(|w| w == b"SUNIONSTORE"),
        "set-store catch-up must be concrete destination writes, got {:?}",
        String::from_utf8_lossy(&catchup)
    );

    let mut replica_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut replica_selected_db = 0;
    apply_resp_stream_as_replica_on_dbs(39, &mut replica_dbs, &mut replica_selected_db, &catchup);
    assert_eq!(replica_selected_db, 9);
    assert!(
        replica_dbs[0].lookup_key_read(b"dst").is_none(),
        "DB 0 must not receive the DB 9 set-store destination"
    );
    let dst = replica_dbs[9]
        .lookup_key_read(b"dst")
        .and_then(|obj| obj.set().cloned())
        .expect("DB 9 should receive the concrete set-store destination");
    assert!(dst.contains(&RedisString::from_static(b"a")));
    assert!(dst.contains(&RedisString::from_static(b"b")));

    repl.selected_db.store(-1, Ordering::Release);
}

#[test]
fn r1_zset_store_first_fullsync_catchup_rewrite_selects_db12() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let _ = repl.take_repl_bgsave_job();
    repl.set_repl_child_pid(0);

    let mut primary_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut primary_selected_db = 12;
    assert_eq!(
        dispatch_as_primary_on_dbs(
            40,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"ZADD", b"za", b"1", b"a"]
        ),
        b":1\r\n"
    );
    assert_eq!(
        dispatch_as_primary_on_dbs(
            41,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"ZADD", b"zdst", b"7", b"stale"]
        ),
        b":1\r\n"
    );

    repl.selected_db.store(-1, Ordering::Release);
    let snapshot_offset = repl.master_offset();
    let dir = unique_temp_dir("repl-kit-zset-store-catchup");
    let temp_path = dir.join("temp-repl-zset-store.rdb");
    repl.install_repl_bgsave_job(ReplBgsaveJob {
        child_pid: 0,
        temp_path: temp_path.clone(),
        waiting_replicas: vec![900_018],
        snapshot_offset,
        catch_up_bytes: Vec::new(),
        needs_getack_on_completion: false,
    });

    assert_eq!(
        dispatch_as_primary_on_dbs(
            42,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"ZINTERSTORE", b"zdst", b"2", b"za", b"missing"]
        ),
        b":0\r\n"
    );
    let job = repl
        .take_repl_bgsave_job()
        .expect("active full-sync job should capture rewritten zset-store bytes");
    let _ = std::fs::remove_file(&temp_path);
    let _ = std::fs::remove_dir_all(&dir);

    let catchup = job.catch_up_bytes;
    assert!(
        catchup.starts_with(&resp(&[b"SELECT", b"12"])),
        "first active full-sync zset-store rewrite must select DB 12, got {:?}",
        String::from_utf8_lossy(&catchup)
    );
    assert!(
        !catchup
            .windows(b"ZINTERSTORE".len())
            .any(|w| w == b"ZINTERSTORE"),
        "zset-store catch-up must be concrete destination writes, got {:?}",
        String::from_utf8_lossy(&catchup)
    );
    assert!(
        catchup
            .windows(resp(&[b"DEL", b"zdst"]).len())
            .any(|w| w == resp(&[b"DEL", b"zdst"]).as_slice()),
        "empty zset-store catch-up must delete the destination, got {:?}",
        String::from_utf8_lossy(&catchup)
    );

    let mut replica_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut replica_selected_db = 12;
    dispatch_as_primary_on_dbs(
        43,
        &mut replica_dbs,
        &mut replica_selected_db,
        &[b"ZADD", b"za", b"1", b"a"],
    );
    dispatch_as_primary_on_dbs(
        44,
        &mut replica_dbs,
        &mut replica_selected_db,
        &[b"ZADD", b"missing", b"1", b"a"],
    );
    dispatch_as_primary_on_dbs(
        45,
        &mut replica_dbs,
        &mut replica_selected_db,
        &[b"ZADD", b"zdst", b"7", b"stale"],
    );
    apply_resp_stream_as_replica_on_dbs(46, &mut replica_dbs, &mut replica_selected_db, &catchup);
    assert_eq!(replica_selected_db, 12);
    assert!(
        replica_dbs[12].lookup_key_read(b"zdst").is_none(),
        "concrete zset-store catch-up must delete the destination even when stale replica sources would make verbatim ZINTERSTORE non-empty"
    );

    repl.selected_db.store(-1, Ordering::Release);
}

#[test]
fn r1_zset_store_rewrite_then_hset_fullsync_catchup_switches_db() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let _ = repl.take_repl_bgsave_job();
    repl.set_repl_child_pid(0);

    let mut primary_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut primary_selected_db = 12;
    assert_eq!(
        dispatch_as_primary_on_dbs(
            47,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"ZADD", b"za", b"1", b"a"]
        ),
        b":1\r\n"
    );
    assert_eq!(
        dispatch_as_primary_on_dbs(
            48,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"ZADD", b"zdst", b"7", b"stale"]
        ),
        b":1\r\n"
    );

    repl.selected_db.store(-1, Ordering::Release);
    let snapshot_offset = repl.master_offset();
    let dir = unique_temp_dir("repl-kit-zset-store-hset-catchup");
    let temp_path = dir.join("temp-repl-zset-store-hset.rdb");
    repl.install_repl_bgsave_job(ReplBgsaveJob {
        child_pid: 0,
        temp_path: temp_path.clone(),
        waiting_replicas: vec![900_020],
        snapshot_offset,
        catch_up_bytes: Vec::new(),
        needs_getack_on_completion: false,
    });

    assert_eq!(
        dispatch_as_primary_on_dbs(
            49,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"ZINTERSTORE", b"zdst", b"2", b"za", b"missing"]
        ),
        b":0\r\n"
    );
    assert_eq!(
        dispatch_as_primary_on_dbs(
            50,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"SELECT", b"11"]
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        dispatch_as_primary_on_dbs(
            51,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"HSET", b"967", b"512775170389", b"20010100220140"]
        ),
        b":1\r\n"
    );

    let job = repl
        .take_repl_bgsave_job()
        .expect("active full-sync job should capture zset rewrite and following HSET");
    let _ = std::fs::remove_file(&temp_path);
    let _ = std::fs::remove_dir_all(&dir);

    let catchup = job.catch_up_bytes;
    assert!(
        catchup.starts_with(&resp(&[b"SELECT", b"12"])),
        "zset rewrite must first select DB 12, got {:?}",
        String::from_utf8_lossy(&catchup)
    );
    assert!(
        catchup
            .windows(resp(&[b"SELECT", b"11"]).len())
            .any(|w| w == resp(&[b"SELECT", b"11"]).as_slice()),
        "ordinary HSET after zset rewrite must switch catch-up to DB 11, got {:?}",
        String::from_utf8_lossy(&catchup)
    );
    assert!(
        catchup
            .windows(resp(&[b"HSET", b"967", b"512775170389", b"20010100220140"]).len())
            .any(|w| {
                w == resp(&[b"HSET", b"967", b"512775170389", b"20010100220140"]).as_slice()
            }),
        "catch-up must include the ordinary HSET frame, got {:?}",
        String::from_utf8_lossy(&catchup)
    );

    let mut replica_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut replica_selected_db = 0;
    apply_resp_stream_as_replica_on_dbs(52, &mut replica_dbs, &mut replica_selected_db, &catchup);
    assert_eq!(replica_selected_db, 11);
    assert!(
        replica_dbs[12].lookup_key_read(b"967").is_none(),
        "DB 12 must not receive the DB 11 hash key"
    );
    let hash = replica_dbs[11]
        .lookup_key_read(b"967")
        .and_then(|obj| obj.hash().cloned())
        .expect("DB 11 should receive the ordinary HSET after zset-store rewrite");
    assert_eq!(
        hash.get(&RedisString::from_static(b"512775170389")),
        Some(&RedisString::from_static(b"20010100220140"))
    );

    repl.selected_db.store(-1, Ordering::Release);
}

#[test]
fn r1_active_fullsync_catchup_replays_db9_sadd_set_creation() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let _ = repl.take_repl_bgsave_job();
    repl.set_repl_child_pid(0);
    repl.selected_db.store(-1, Ordering::Release);

    let mut primary_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut primary_selected_db = 9;
    let snapshot_offset = repl.master_offset();
    let dir = unique_temp_dir("repl-kit-active-fullsync-sadd");
    let temp_path = dir.join("temp-repl-active-fullsync-sadd.rdb");
    repl.install_repl_bgsave_job(ReplBgsaveJob {
        child_pid: 0,
        temp_path: temp_path.clone(),
        waiting_replicas: vec![900_019],
        snapshot_offset,
        catch_up_bytes: Vec::new(),
        needs_getack_on_completion: false,
    });

    assert_eq!(
        dispatch_as_primary_on_dbs(
            42,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[
                b"SADD",
                b"238641124329",
                b"-438323278649",
                b"2172725227",
                b"397",
                b"817822073"
            ]
        ),
        b":4\r\n"
    );
    let job = repl
        .take_repl_bgsave_job()
        .expect("active full-sync job should capture ordinary SADD bytes");
    let _ = std::fs::remove_file(&temp_path);
    let _ = std::fs::remove_dir_all(&dir);

    let catchup = job.catch_up_bytes;
    assert!(
        catchup.starts_with(&resp(&[b"SELECT", b"9"])),
        "first active full-sync SADD must select DB 9, got {:?}",
        String::from_utf8_lossy(&catchup)
    );
    assert!(
        catchup.windows(b"SADD".len()).any(|w| w == b"SADD")
            && catchup
                .windows(b"238641124329".len())
                .any(|w| w == b"238641124329"),
        "active full-sync catch-up must include the ordinary SADD frame, got {:?}",
        String::from_utf8_lossy(&catchup)
    );

    let mut replica_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut replica_selected_db = 0;
    apply_resp_stream_as_replica_on_dbs(43, &mut replica_dbs, &mut replica_selected_db, &catchup);
    assert_eq!(replica_selected_db, 9);
    assert!(
        replica_dbs[0].lookup_key_read(b"238641124329").is_none(),
        "DB 0 must not receive the active full-sync DB 9 set"
    );
    let db9_set = replica_dbs[9]
        .lookup_key_read(b"238641124329")
        .and_then(|obj| obj.set().cloned())
        .expect("DB 9 should receive the active full-sync set");
    assert!(db9_set.contains(&RedisString::from_static(b"-438323278649")));
    assert!(db9_set.contains(&RedisString::from_static(b"2172725227")));
    assert!(db9_set.contains(&RedisString::from_static(b"397")));
    assert!(db9_set.contains(&RedisString::from_static(b"817822073")));

    repl.selected_db.store(-1, Ordering::Release);
}

#[test]
fn r1_live_write_after_fullsync_forces_select_for_new_send_bulk_replica() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let _ = repl.take_repl_bgsave_job();
    repl.set_repl_child_pid(0);
    repl.selected_db.store(9, Ordering::Release);

    let client_id = 900_018;
    let snapshot_offset = repl.master_offset();
    let (tx, rx) = mpsc::channel();
    repl.add_replica(ReplicaConn::new(
        client_id,
        ReplicaState::WaitingBgsave,
        snapshot_offset,
        tx,
    ));
    let dir = unique_temp_dir("repl-kit-post-fullsync-select");
    let temp_path = dir.join("temp-repl-post-fullsync-select.rdb");
    let outcome = repl.complete_repl_bgsave_transfer(
        ReplBgsaveJob {
            child_pid: 0,
            temp_path: temp_path.clone(),
            waiting_replicas: vec![client_id],
            snapshot_offset,
            catch_up_bytes: Vec::new(),
            needs_getack_on_completion: false,
        },
        b"RDB".to_vec(),
    );
    let _ = std::fs::remove_file(&temp_path);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(outcome.delivered_replicas, vec![client_id]);
    assert_eq!(rx.try_recv().unwrap(), b"$3\r\nRDB".to_vec());

    let mut primary_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut primary_selected_db = 9;
    assert_eq!(
        dispatch_as_primary_on_dbs(
            40,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"SADD", b"db9-set", b"597971278521"]
        ),
        b":1\r\n"
    );

    let mut stream = Vec::new();
    while let Ok(chunk) = rx.try_recv() {
        stream.extend_from_slice(&chunk);
    }
    assert!(
        stream.starts_with(&resp(&[b"SELECT", b"9"])),
        "a send_bulk replica after fullsync may be at DB 0, so the first live DB 9 set write must force SELECT; got {:?}",
        String::from_utf8_lossy(&stream)
    );

    let mut replica_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut replica_selected_db = 0;
    apply_resp_stream_as_replica_on_dbs(41, &mut replica_dbs, &mut replica_selected_db, &stream);
    assert_eq!(replica_selected_db, 9);
    assert!(
        replica_dbs[0].lookup_key_read(b"db9-set").is_none(),
        "DB 0 must not receive the post-fullsync DB 9 set write"
    );
    let db9_set = replica_dbs[9]
        .lookup_key_read(b"db9-set")
        .and_then(|obj| obj.set().cloned())
        .expect("DB 9 should receive the post-fullsync set write");
    assert!(db9_set.contains(&RedisString::from_static(b"597971278521")));

    repl.remove_replica(client_id);
    repl.selected_db.store(-1, Ordering::Release);
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
fn r1_lazy_expire_recreate_propagates_delete_before_write() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    repl.reset_selected_db_for_full_resync();
    let cap = ReplCapture::attach(900_034, repl.master_offset());
    let mut primary_db = RedisDb::new(0);
    let mut replica_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut replica_selected_db = 0;

    assert_eq!(
        dispatch_as_primary(34, &mut primary_db, &[b"SADD", b"s", b"foo"]),
        b":1\r\n"
    );
    assert_eq!(
        dispatch_as_primary(35, &mut primary_db, &[b"PEXPIRE", b"s", b"1"]),
        b":1\r\n"
    );
    let initial_stream = cap.drain();
    apply_resp_stream_as_replica_on_dbs(
        36,
        &mut replica_dbs,
        &mut replica_selected_db,
        &initial_stream,
    );
    let _ = cap.drain();
    let key = RedisString::from_static(b"s");
    wait_until_logically_expired(&primary_db, &key);

    assert_eq!(
        dispatch_as_primary(37, &mut primary_db, &[b"SADD", b"s", b"foo"]),
        b":1\r\n"
    );
    let stream = cap.drain();
    let del_frame = resp(&[b"DEL", b"s"]);
    let sadd_frame = resp(&[b"SADD", b"s", b"foo"]);
    let del_pos = stream
        .windows(del_frame.len())
        .position(|w| w == del_frame.as_slice())
        .expect("lazy expiry before recreate must propagate DEL");
    let sadd_pos = stream
        .windows(sadd_frame.len())
        .position(|w| w == sadd_frame.as_slice())
        .expect("recreating SADD must still propagate");
    assert!(
        del_pos < sadd_pos,
        "replica must delete the expired value before applying the recreating write, got {:?}",
        String::from_utf8_lossy(&stream)
    );

    apply_resp_stream_as_replica_on_dbs(38, &mut replica_dbs, &mut replica_selected_db, &stream);
    replica_dbs[0].set_replica_keep_expired(true);
    assert!(
        replica_dbs[0]
            .lookup_key_read_with_flags(&key, LOOKUP_NOTOUCH)
            .is_some(),
        "without the propagated DEL, replica apply mutates the expired set and normal replica reads still report it missing"
    );
}

#[test]
fn r1_legacy_write_forms_rewrite_to_replica_command_names() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let cap = ReplCapture::attach(900_033, 0);
    let mut db = RedisDb::new(0);

    assert_eq!(
        dispatch_as_primary(41, &mut db, &[b"SET", b"test", b"foo"]),
        b"+OK\r\n"
    );
    let _ = cap.drain();
    assert_eq!(
        dispatch_as_primary(42, &mut db, &[b"GETSET", b"test", b"bar"]),
        b"$3\r\nfoo\r\n"
    );
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"SET", b"test", b"bar"]).len())
            .any(|w| w == resp(&[b"SET", b"test", b"bar"]).as_slice()),
        "GETSET must propagate as SET, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream
            .windows(resp(&[b"GETSET", b"test", b"bar"]).len())
            .any(|w| w == resp(&[b"GETSET", b"test", b"bar"]).as_slice()),
        "GETSET itself must not be propagated"
    );

    assert_eq!(
        dispatch_as_primary(43, &mut db, &[b"LPUSH", b"src", b"one"]),
        b":1\r\n"
    );
    let _ = cap.drain();
    assert_eq!(
        dispatch_as_primary(44, &mut db, &[b"BRPOPLPUSH", b"src", b"dst", b"5"]),
        b"$3\r\none\r\n"
    );
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"RPOPLPUSH", b"src", b"dst"]).len())
            .any(|w| w == resp(&[b"RPOPLPUSH", b"src", b"dst"]).as_slice()),
        "BRPOPLPUSH with data must propagate as RPOPLPUSH, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream
            .windows(b"BRPOPLPUSH".len())
            .any(|w| w == b"BRPOPLPUSH"),
        "blocking BRPOPLPUSH form must not be propagated"
    );
    assert!(
        !stream.windows(b"LMOVE".len()).any(|w| w == b"LMOVE"),
        "legacy BRPOPLPUSH must not be rewritten to LMOVE"
    );

    assert_eq!(
        dispatch_as_primary(45, &mut db, &[b"LPUSH", b"src2", b"two"]),
        b":1\r\n"
    );
    let _ = cap.drain();
    assert_eq!(
        dispatch_as_primary(
            46,
            &mut db,
            &[b"BLMOVE", b"src2", b"dst2", b"LEFT", b"RIGHT", b"5"]
        ),
        b"$3\r\ntwo\r\n"
    );
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"LMOVE", b"src2", b"dst2", b"left", b"right"]).len())
            .any(|w| w == resp(&[b"LMOVE", b"src2", b"dst2", b"left", b"right"]).as_slice()),
        "BLMOVE with data must propagate as LMOVE, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream.windows(b"BLMOVE".len()).any(|w| w == b"BLMOVE"),
        "blocking BLMOVE form must not be propagated"
    );
}

#[test]
fn r1_blocked_move_wake_rewrites_to_nonblocking_names() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let cap = ReplCapture::attach(900_034, 0);
    let mut db = RedisDb::new(0);

    let src = RedisString::from_static(b"wake-src");
    let dst = RedisString::from_static(b"wake-dst");
    let (tx, rx) = mpsc::channel();
    {
        let mut idx = blocked_keys_index()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        idx.add(BlockedWaiter {
            client_id: 910_034,
            sender: tx,
            keys: vec![src.clone()],
            action: BlockedAction::Move {
                side: BlockedSide::Tail,
                dst_key: dst.clone(),
                dst_side: BlockedSide::Head,
                legacy_rpoplpush: true,
            },
            deadline_ms: i64::MAX,
            resp_proto: 2,
            username: None,
            redirect_on_role_change: false,
        });
    }
    assert_eq!(
        dispatch_as_primary(47, &mut db, &[b"LPUSH", b"wake-src", b"one"]),
        b":1\r\n"
    );
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(100)).unwrap(),
        b"$3\r\none\r\n"
    );
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"RPOPLPUSH", b"wake-src", b"wake-dst"]).len())
            .any(|w| w == resp(&[b"RPOPLPUSH", b"wake-src", b"wake-dst"]).as_slice()),
        "woken BRPOPLPUSH must propagate as RPOPLPUSH, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream.windows(b"LMOVE".len()).any(|w| w == b"LMOVE"),
        "woken BRPOPLPUSH must not propagate as LMOVE"
    );

    let src = RedisString::from_static(b"wake-src2");
    let dst = RedisString::from_static(b"wake-dst2");
    let (tx, rx) = mpsc::channel();
    {
        let mut idx = blocked_keys_index()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        idx.add(BlockedWaiter {
            client_id: 910_035,
            sender: tx,
            keys: vec![src.clone()],
            action: BlockedAction::Move {
                side: BlockedSide::Head,
                dst_key: dst.clone(),
                dst_side: BlockedSide::Tail,
                legacy_rpoplpush: false,
            },
            deadline_ms: i64::MAX,
            resp_proto: 2,
            username: None,
            redirect_on_role_change: false,
        });
    }
    assert_eq!(
        dispatch_as_primary(48, &mut db, &[b"LPUSH", b"wake-src2", b"two"]),
        b":1\r\n"
    );
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(100)).unwrap(),
        b"$3\r\ntwo\r\n"
    );
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"LMOVE", b"wake-src2", b"wake-dst2", b"left", b"right"]).len())
            .any(|w| {
                w == resp(&[b"LMOVE", b"wake-src2", b"wake-dst2", b"left", b"right"]).as_slice()
            }),
        "woken BLMOVE must propagate as LMOVE, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream.windows(b"BLMOVE".len()).any(|w| w == b"BLMOVE"),
        "woken BLMOVE must not propagate blocking form"
    );
}

#[test]
fn r1_blocked_single_list_pop_wake_rewrites_to_nonblocking_names() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_036, 0);
    let mut db = RedisDb::new(0);

    let key = RedisString::from_static(b"wake-blpop");
    let (tx, rx) = mpsc::channel();
    {
        let mut idx = blocked_keys_index()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        idx.add(BlockedWaiter {
            client_id: 910_036,
            sender: tx,
            keys: vec![key.clone()],
            action: BlockedAction::Pop {
                side: BlockedSide::Head,
                count: 0,
            },
            deadline_ms: i64::MAX,
            resp_proto: 2,
            username: None,
            redirect_on_role_change: false,
        });
    }
    assert_eq!(
        dispatch_as_primary(65, &mut db, &[b"LPUSH", b"wake-blpop", b"left"]),
        b":1\r\n"
    );
    let reply = rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_reply_contains(&reply, b"left");
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"LPOP", b"wake-blpop"]).len())
            .any(|w| w == resp(&[b"LPOP", b"wake-blpop"]).as_slice()),
        "woken BLPOP must propagate as LPOP, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream.windows(b"BLPOP".len()).any(|w| w == b"BLPOP"),
        "woken BLPOP must not propagate the blocking form"
    );

    let key = RedisString::from_static(b"wake-brpop");
    let (tx, rx) = mpsc::channel();
    {
        let mut idx = blocked_keys_index()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        idx.add(BlockedWaiter {
            client_id: 910_037,
            sender: tx,
            keys: vec![key.clone()],
            action: BlockedAction::Pop {
                side: BlockedSide::Tail,
                count: 0,
            },
            deadline_ms: i64::MAX,
            resp_proto: 2,
            username: None,
            redirect_on_role_change: false,
        });
    }
    assert_eq!(
        dispatch_as_primary(66, &mut db, &[b"RPUSH", b"wake-brpop", b"right"]),
        b":1\r\n"
    );
    let reply = rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_reply_contains(&reply, b"right");
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"RPOP", b"wake-brpop"]).len())
            .any(|w| w == resp(&[b"RPOP", b"wake-brpop"]).as_slice()),
        "woken BRPOP must propagate as RPOP, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream.windows(b"BRPOP".len()).any(|w| w == b"BRPOP"),
        "woken BRPOP must not propagate the blocking form"
    );
}

#[test]
fn r1_blocked_single_zset_pop_wake_rewrites_to_nonblocking_names() {
    let _g = repl_guard();
    let cap = ReplCapture::attach(900_037, 0);
    let mut db = RedisDb::new(0);

    let key = RedisString::from_static(b"wake-bzmin");
    let (tx, rx) = mpsc::channel();
    {
        let mut idx = blocked_keys_index()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        idx.add(BlockedWaiter {
            client_id: 910_038,
            sender: tx,
            keys: vec![key.clone()],
            action: BlockedAction::ZSetPop {
                reverse: false,
                count: 0,
            },
            deadline_ms: i64::MAX,
            resp_proto: 2,
            username: None,
            redirect_on_role_change: false,
        });
    }
    assert_eq!(
        dispatch_as_primary(67, &mut db, &[b"ZADD", b"wake-bzmin", b"1", b"min"]),
        b":1\r\n"
    );
    let reply = rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_reply_contains(&reply, b"min");
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"ZPOPMIN", b"wake-bzmin"]).len())
            .any(|w| w == resp(&[b"ZPOPMIN", b"wake-bzmin"]).as_slice()),
        "woken BZPOPMIN must propagate as ZPOPMIN, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream.windows(b"BZPOPMIN".len()).any(|w| w == b"BZPOPMIN"),
        "woken BZPOPMIN must not propagate the blocking form"
    );

    let key = RedisString::from_static(b"wake-bzmax");
    let (tx, rx) = mpsc::channel();
    {
        let mut idx = blocked_keys_index()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        idx.add(BlockedWaiter {
            client_id: 910_039,
            sender: tx,
            keys: vec![key.clone()],
            action: BlockedAction::ZSetPop {
                reverse: true,
                count: 0,
            },
            deadline_ms: i64::MAX,
            resp_proto: 2,
            username: None,
            redirect_on_role_change: false,
        });
    }
    assert_eq!(
        dispatch_as_primary(68, &mut db, &[b"ZADD", b"wake-bzmax", b"9", b"max"]),
        b":1\r\n"
    );
    let reply = rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_reply_contains(&reply, b"max");
    let stream = cap.drain();
    assert!(
        stream
            .windows(resp(&[b"ZPOPMAX", b"wake-bzmax"]).len())
            .any(|w| w == resp(&[b"ZPOPMAX", b"wake-bzmax"]).as_slice()),
        "woken BZPOPMAX must propagate as ZPOPMAX, got {:?}",
        String::from_utf8_lossy(&stream)
    );
    assert!(
        !stream.windows(b"BZPOPMAX".len()).any(|w| w == b"BZPOPMAX"),
        "woken BZPOPMAX must not propagate the blocking form"
    );
}

#[test]
fn r1_replica_apply_counts_wake_rewritten_move_commands() {
    let _g = repl_guard();
    reset_command_stats();

    let mut db = RedisDb::new(0);
    assert_eq!(
        dispatch_as_replica_apply(49, &mut db, &[b"LPUSH", b"a", b"foo"]),
        b":1\r\n"
    );
    assert_eq!(
        dispatch_as_replica_apply(50, &mut db, &[b"RPOPLPUSH", b"a", b"b"]),
        b"$3\r\nfoo\r\n"
    );
    assert_eq!(command_stat_calls(b"rpoplpush"), 1);
    assert_eq!(command_stat_calls(b"lmove"), 0);

    reset_command_stats();
    let mut db = RedisDb::new(0);
    assert_eq!(
        dispatch_as_replica_apply(51, &mut db, &[b"LPUSH", b"c", b"bar"]),
        b":1\r\n"
    );
    assert_eq!(
        dispatch_as_replica_apply(52, &mut db, &[b"LMOVE", b"c", b"d", b"left", b"right"]),
        b"$3\r\nbar\r\n"
    );
    assert_eq!(command_stat_calls(b"lmove"), 1);
    assert_eq!(command_stat_calls(b"rpoplpush"), 0);

    reset_command_stats();
}

#[test]
fn r1_debug_digest_tracks_keyspace_mutations_for_replication_waits() {
    let _g = repl_guard();
    let mut primary_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut replica_dbs: Vec<RedisDb> = (0..16).map(RedisDb::new).collect();
    let mut primary_selected_db = 0;
    let mut replica_selected_db = 0;

    let empty_primary = debug_digest(53, &mut primary_dbs, &mut primary_selected_db);
    let empty_replica = debug_digest(54, &mut replica_dbs, &mut replica_selected_db);
    assert_eq!(
        empty_primary, empty_replica,
        "empty keyspaces should start with the same digest"
    );
    assert_ne!(
        empty_primary, b"0000000000000000000000000000000000000000",
        "DEBUG DIGEST must not be the old all-zero convergence stub"
    );

    assert_eq!(
        dispatch_as_primary_on_dbs(
            55,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"LPUSH", b"src", b"value"]
        ),
        b":1\r\n"
    );
    assert_ne!(
        debug_digest(56, &mut primary_dbs, &mut primary_selected_db),
        debug_digest(57, &mut replica_dbs, &mut replica_selected_db),
        "digest must remain unequal until the replica applies the write"
    );

    assert_eq!(
        dispatch_as_primary_on_dbs(
            58,
            &mut replica_dbs,
            &mut replica_selected_db,
            &[b"LPUSH", b"src", b"value"]
        ),
        b":1\r\n"
    );
    assert_eq!(
        debug_digest(59, &mut primary_dbs, &mut primary_selected_db),
        debug_digest(60, &mut replica_dbs, &mut replica_selected_db),
        "matching keyspaces should converge once the same write is applied"
    );

    assert_eq!(
        dispatch_as_primary_on_dbs(
            61,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"SELECT", b"2"]
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        dispatch_as_primary_on_dbs(
            62,
            &mut primary_dbs,
            &mut primary_selected_db,
            &[b"SET", b"src", b"value"]
        ),
        b"+OK\r\n"
    );
    assert_ne!(
        debug_digest(63, &mut primary_dbs, &mut primary_selected_db),
        debug_digest(64, &mut replica_dbs, &mut replica_selected_db),
        "the digest must include the DB id, not just key/value bytes"
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

#[test]
fn p4_blocked_blpop_unblocks_before_replica_apply_after_role_change() {
    let _g = repl_guard();
    global_replication_state().become_master();
    reset_command_stats();

    let mut db = RedisDb::new(0);
    let rx = block_blpop(980_007, &mut db, b"foo");
    assert_eq!(command_stat_counts(b"blpop"), (1, 0, 0));

    redis_commands::replication::unblock_replication_role_change();
    let reply = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("BLPOP waiter should be force-unblocked on role change");
    assert!(
        reply.starts_with(
            b"-UNBLOCKED force unblock from blocking operation, instance state changed"
        ),
        "unexpected BLPOP role-change reply: {:?}",
        String::from_utf8_lossy(&reply),
    );
    assert_eq!(command_stat_counts(b"blpop"), (1, 1, 0));

    assert_eq!(
        dispatch_as_replica_apply(980_008, &mut db, &[b"RPUSH", b"foo", b"a", b"b", b"c"]),
        b":3\r\n"
    );
    assert_eq!(
        dispatch_as_primary(980_009, &mut db, &[b"LRANGE", b"foo", b"0", b"-1"]),
        b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
    assert!(
        rx.try_recv().is_err(),
        "replica-applied RPUSH must not wake the stale BLPOP waiter"
    );

    reset_command_stats();
}

// ─── R5 FAILOVER parser-only groundwork ─────────────────────────────────────

#[test]
fn r5_failover_parser_registered_and_rejects_no_replica_state() {
    let _g = repl_guard();
    global_replication_state().become_master();

    let mut db = RedisDb::new(0);
    let reply = dispatch_result_as_primary(990_001, &mut db, &[b"FAILOVER"]);
    assert_reply_contains(&reply, b"FAILOVER requires connected replicas.");
    assert!(
        !reply
            .windows(b"unknown command".len())
            .any(|w| w.eq_ignore_ascii_case(b"unknown command")),
        "FAILOVER should be registered, got {:?}",
        String::from_utf8_lossy(&reply),
    );

    let abort = dispatch_result_as_primary(990_002, &mut db, &[b"FAILOVER", b"ABORT"]);
    assert_reply_contains(&abort, b"No failover in progress.");

    let bad_timeout =
        dispatch_result_as_primary(990_003, &mut db, &[b"FAILOVER", b"TIMEOUT", b"0"]);
    assert_reply_contains(&bad_timeout, b"FAILOVER timeout must be greater than 0");

    let bad_target = dispatch_result_as_primary(990_004, &mut db, &[b"FAILOVER", b"TO", b"host"]);
    assert_reply_contains(&bad_target, b"syntax error");
}

#[test]
fn r5_failover_parser_keeps_state_and_capability_boundaries() {
    let _g = repl_guard();
    let repl = global_replication_state();
    repl.become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 6379);

    let mut db = RedisDb::new(0);
    let as_replica = dispatch_result_as_primary(990_005, &mut db, &[b"FAILOVER"]);
    assert_reply_contains(
        &as_replica,
        b"FAILOVER is not valid when server is a replica.",
    );

    repl.become_master();
    let _capture = ReplCapture::attach(990_006, repl.master_offset());

    let force_without_timeout = dispatch_result_as_primary(
        990_007,
        &mut db,
        &[b"FAILOVER", b"TO", b"127.0.0.1", b"6379", b"FORCE"],
    );
    assert_reply_contains(
        &force_without_timeout,
        b"FAILOVER with force option requires both a timeout and target HOST and IP.",
    );

    let would_start_real_failover = dispatch_result_as_primary(990_008, &mut db, &[b"FAILOVER"]);
    assert_eq!(would_start_real_failover, b"+OK\r\n");
    repl.abort_manual_failover();
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
