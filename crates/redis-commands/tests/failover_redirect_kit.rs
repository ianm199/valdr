//! Deterministic inner loop for replica redirect semantics.
//!
//! The Tcl `integration/replica-redirect` gate starts with client-visible
//! REDIRECT behavior before it reaches real coordinated failover. This kit
//! drives the production dispatch path directly so redirect-capable clients,
//! READONLY clients, and MULTI/EXEC transitions are covered without spawning a
//! two-server topology.

use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use redis_core::blocked_keys::{blocked_keys_index, BlockedAction, BlockedSide, BlockedWaiter};
use redis_core::replication::{
    global_replication_state, ManualFailoverAdvance, ReplicaConn, ReplicaState,
};
use redis_core::ClientId;
use redis_core::{Client, PubSubRegistry, RedisDb, RedisServer};
use redis_ds::stream::StreamId;
use redis_protocol::frame::{encode_resp2, RespFrame};
use redis_types::RedisString;

use redis_commands::dispatch::dispatch;

fn repl_guard() -> MutexGuard<'static, ()> {
    static REPL_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    match REPL_GUARD.get_or_init(|| Mutex::new(())).lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

struct ReplicaMode;

impl ReplicaMode {
    fn enter(host: &[u8], port: u16) -> Self {
        let repl = global_replication_state();
        repl.become_master();
        repl.become_replica_of(RedisString::from_bytes(host), port);
        Self
    }
}

impl Drop for ReplicaMode {
    fn drop(&mut self) {
        global_replication_state().become_master();
    }
}

fn argv(parts: &[&[u8]]) -> Vec<RedisString> {
    parts.iter().map(|p| RedisString::from_bytes(p)).collect()
}

fn run(client: &mut Client, db: &mut RedisDb, cmd: &[&[u8]]) -> Vec<u8> {
    run_with_server(client, db, Arc::new(RedisServer::default()), cmd)
}

fn run_with_server(
    client: &mut Client,
    db: &mut RedisDb,
    server: Arc<RedisServer>,
    cmd: &[&[u8]],
) -> Vec<u8> {
    client.set_args(argv(cmd));
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let result = {
        let mut ctx = redis_core::CommandContext::with_server(client, db, server, pubsub);
        dispatch(&mut ctx)
    };
    let mut reply = client.drain_reply();
    if let Err(err) = result {
        if reply.is_empty() {
            encode_resp2(&RespFrame::Error(err.to_resp_payload()), &mut reply);
        }
    }
    reply
}

struct AttachedReplica {
    client_id: ClientId,
}

impl AttachedReplica {
    fn online(client_id: ClientId) -> Self {
        Self::online_with_port_and_offset(client_id, 0, global_replication_state().master_offset())
    }

    fn online_with_port_and_offset(client_id: ClientId, port: u16, offset: i64) -> Self {
        let (tx, _rx) = mpsc::channel();
        let conn = ReplicaConn::new(client_id, ReplicaState::Online, offset, tx);
        conn.listening_port.store(port, Ordering::Relaxed);
        global_replication_state().add_replica(conn);
        Self { client_id }
    }
}

impl Drop for AttachedReplica {
    fn drop(&mut self) {
        global_replication_state().remove_replica(self.client_id);
    }
}

fn assert_reply(reply: &[u8], expected: &[u8]) {
    assert_eq!(
        reply,
        expected,
        "reply mismatch\n  got: {:?}\n want: {:?}",
        String::from_utf8_lossy(reply),
        String::from_utf8_lossy(expected)
    );
}

fn assert_starts_with(reply: &[u8], expected: &[u8]) {
    assert!(
        reply.starts_with(expected),
        "reply {:?} did not start with {:?}",
        String::from_utf8_lossy(reply),
        String::from_utf8_lossy(expected)
    );
}

fn assert_contains(reply: &[u8], expected: &[u8]) {
    assert!(
        reply.windows(expected.len()).any(|w| w == expected),
        "reply {:?} did not contain {:?}",
        String::from_utf8_lossy(reply),
        String::from_utf8_lossy(expected)
    );
}

#[test]
fn redirect_capability_redirects_replica_data_commands_to_primary() {
    let _guard = repl_guard();
    let _replica = ReplicaMode::enter(b"127.0.0.1", 6381);
    let mut client = Client::new(1_010_001);
    let mut db = RedisDb::new(0);

    assert_reply(
        &run(&mut client, &mut db, &[b"CLIENT", b"CAPA", b"REDIRECT"]),
        b"+OK\r\n",
    );
    assert_reply(
        &run(&mut client, &mut db, &[b"GET", b"foo"]),
        b"-REDIRECT 127.0.0.1:6381\r\n",
    );
    assert_reply(
        &run(&mut client, &mut db, &[b"SET", b"foo", b"bar"]),
        b"-REDIRECT 127.0.0.1:6381\r\n",
    );
    assert_reply(&run(&mut client, &mut db, &[b"PING"]), b"+PONG\r\n");
}

#[test]
fn readonly_redirect_client_keeps_allowed_reads_but_redirects_writes() {
    let _guard = repl_guard();
    let _replica = ReplicaMode::enter(b"primary.example", 6390);
    let mut client = Client::new(1_010_002);
    let mut db = RedisDb::new(0);

    assert_reply(
        &run(&mut client, &mut db, &[b"CLIENT", b"CAPA", b"REDIRECT"]),
        b"+OK\r\n",
    );
    assert_reply(&run(&mut client, &mut db, &[b"READONLY"]), b"+OK\r\n");
    assert_reply(&run(&mut client, &mut db, &[b"GET", b"foo"]), b"$-1\r\n");
    assert_reply(
        &run(&mut client, &mut db, &[b"SET", b"foo", b"bar"]),
        b"-REDIRECT primary.example:6390\r\n",
    );
}

#[test]
fn ordinary_replica_clients_keep_readonly_contract_without_redirect() {
    let _guard = repl_guard();
    let _replica = ReplicaMode::enter(b"127.0.0.1", 6382);
    let mut client = Client::new(1_010_003);
    let mut db = RedisDb::new(0);

    assert_reply(&run(&mut client, &mut db, &[b"GET", b"foo"]), b"$-1\r\n");
    assert_reply(
        &run(&mut client, &mut db, &[b"SET", b"foo", b"bar"]),
        b"-READONLY You can't write against a read only replica.\r\n",
    );
}

#[test]
fn pending_replconf_metadata_is_applied_when_replica_registers() {
    let _guard = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let client_id = 1_010_006;
    let mut client = Client::new(client_id);
    let mut db = RedisDb::new(0);

    assert_reply(
        &run(
            &mut client,
            &mut db,
            &[b"REPLCONF", b"listening-port", b"6386"],
        ),
        b"+OK\r\n",
    );
    assert_reply(
        &run(&mut client, &mut db, &[b"REPLCONF", b"capa", b"psync2"]),
        b"+OK\r\n",
    );

    let (tx, _rx) = mpsc::channel();
    repl.add_replica(ReplicaConn::new(
        client_id,
        ReplicaState::Online,
        repl.master_offset(),
        tx,
    ));
    assert!(repl.manual_failover_target_online(&(RedisString::from_bytes(b"127.0.0.1"), 6386,)));
    let guard = repl.replicas.lock().unwrap_or_else(|p| p.into_inner());
    let conn = guard.get(&client_id).expect("registered replica");
    assert_eq!(conn.listening_port(), 6386);
    assert_ne!(conn.capa_flags(), 0);
    drop(guard);
    repl.remove_replica(client_id);
}

#[test]
fn failover_completion_redirects_blocked_data_clients_but_keeps_readonly_stream_waiters() {
    let _guard = repl_guard();
    let _replica = ReplicaMode::enter(b"127.0.0.1", 6385);
    let (pop_tx, pop_rx) = mpsc::channel();
    let (stream_tx, stream_rx) = mpsc::channel();
    let pop_key = RedisString::from_bytes(b"pop-key");
    let stream_key = RedisString::from_bytes(b"stream-key");
    let pop_client_id = 1_010_013;
    let stream_client_id = 1_010_014;

    {
        let mut idx = blocked_keys_index()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let _ = idx.remove_client(pop_client_id);
        let _ = idx.remove_client(stream_client_id);
        idx.add(BlockedWaiter {
            client_id: pop_client_id,
            sender: pop_tx,
            keys: vec![pop_key.clone()],
            action: BlockedAction::Pop {
                side: BlockedSide::Tail,
                count: 0,
            },
            deadline_ms: i64::MAX,
            resp_proto: 2,
            username: None,
            redirect_on_role_change: true,
        });
        idx.add(BlockedWaiter {
            client_id: stream_client_id,
            sender: stream_tx,
            keys: vec![stream_key.clone()],
            action: BlockedAction::Stream {
                id_after: StreamId::ZERO,
            },
            deadline_ms: i64::MAX,
            resp_proto: 2,
            username: None,
            redirect_on_role_change: false,
        });
    }

    redis_commands::replication::redirect_blocked_clients_after_failover();

    assert_eq!(
        pop_rx.try_recv().expect("blocked pop should redirect"),
        b"-REDIRECT 127.0.0.1:6385\r\n"
    );
    assert!(
        stream_rx.try_recv().is_err(),
        "readonly stream waiter should remain blocked"
    );
    let mut idx = blocked_keys_index()
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    assert!(!idx.has_waiters_for(&pop_key));
    assert!(idx.has_waiters_for(&stream_key));
    let _ = idx.remove_client(stream_client_id);
}

#[test]
fn queued_write_redirects_at_exec_after_role_change() {
    let _guard = repl_guard();
    global_replication_state().become_master();
    let mut client = Client::new(1_010_004);
    let mut db = RedisDb::new(0);

    assert_reply(
        &run(&mut client, &mut db, &[b"CLIENT", b"CAPA", b"REDIRECT"]),
        b"+OK\r\n",
    );
    assert_reply(&run(&mut client, &mut db, &[b"MULTI"]), b"+OK\r\n");
    assert_reply(
        &run(&mut client, &mut db, &[b"SET", b"foo", b"bar"]),
        b"+QUEUED\r\n",
    );

    let _replica = ReplicaMode::enter(b"127.0.0.1", 6383);
    assert_reply(
        &run(&mut client, &mut db, &[b"EXEC"]),
        b"-REDIRECT 127.0.0.1:6383\r\n",
    );
}

#[test]
fn queue_time_redirect_marks_transaction_dirty_for_execabort() {
    let _guard = repl_guard();
    let _replica = ReplicaMode::enter(b"127.0.0.1", 6384);
    let mut client = Client::new(1_010_005);
    let mut db = RedisDb::new(0);

    assert_reply(
        &run(&mut client, &mut db, &[b"CLIENT", b"CAPA", b"REDIRECT"]),
        b"+OK\r\n",
    );
    assert_reply(&run(&mut client, &mut db, &[b"MULTI"]), b"+OK\r\n");
    assert_reply(
        &run(&mut client, &mut db, &[b"SET", b"foo", b"bar"]),
        b"-REDIRECT 127.0.0.1:6384\r\n",
    );

    let exec = run(&mut client, &mut db, &[b"EXEC"]);
    assert_starts_with(
        &exec,
        b"-EXECABORT Transaction discarded because of previous errors.",
    );
}

#[test]
fn failover_without_force_enters_waiting_for_sync_and_abort_clears_pause() {
    let _guard = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let _replica = AttachedReplica::online(1_010_007);
    let server = Arc::new(RedisServer::default());
    let mut client = Client::new(1_010_008);
    let mut db = RedisDb::new(0);

    assert_reply(
        &run_with_server(&mut client, &mut db, Arc::clone(&server), &[b"FAILOVER"]),
        b"+OK\r\n",
    );
    assert_contains(
        &run_with_server(
            &mut client,
            &mut db,
            Arc::clone(&server),
            &[b"INFO", b"replication"],
        ),
        b"master_failover_state:waiting-for-sync",
    );
    let clients = run_with_server(
        &mut client,
        &mut db,
        Arc::clone(&server),
        &[b"INFO", b"clients"],
    );
    assert_contains(&clients, b"paused_reason:failover");
    assert_contains(&clients, b"paused_actions:write");

    assert_reply(
        &run_with_server(
            &mut client,
            &mut db,
            Arc::clone(&server),
            &[b"FAILOVER", b"ABORT"],
        ),
        b"+OK\r\n",
    );
    assert_contains(
        &run_with_server(
            &mut client,
            &mut db,
            Arc::clone(&server),
            &[b"INFO", b"replication"],
        ),
        b"master_failover_state:no-failover",
    );
    assert_contains(
        &run_with_server(&mut client, &mut db, server, &[b"INFO", b"clients"]),
        b"paused_reason:none",
    );
}

#[test]
fn force_failover_with_target_waits_then_demotes_on_timeout() {
    let _guard = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    repl.master_repl_offset.store(42, Ordering::Relaxed);
    let _replica = AttachedReplica::online_with_port_and_offset(1_010_009, 6385, 0);
    let server = Arc::new(RedisServer::default());
    let mut client = Client::new(1_010_010);
    let mut db = RedisDb::new(0);

    assert_reply(
        &run_with_server(
            &mut client,
            &mut db,
            Arc::clone(&server),
            &[
                b"FAILOVER",
                b"TO",
                b"127.0.0.1",
                b"6385",
                b"TIMEOUT",
                b"500",
                b"FORCE",
            ],
        ),
        b"+OK\r\n",
    );
    assert_contains(
        &run_with_server(
            &mut client,
            &mut db,
            Arc::clone(&server),
            &[b"INFO", b"replication"],
        ),
        b"master_failover_state:waiting-for-sync",
    );

    let outcome = repl.advance_manual_failover(redis_core::util::mstime().saturating_add(1_000));
    match outcome {
        ManualFailoverAdvance::Started { host, port, .. } => {
            assert_eq!(host.as_bytes(), b"127.0.0.1");
            assert_eq!(port, 6385);
        }
        other => panic!("expected failover handoff, got {other:?}"),
    }
    assert_contains(
        &run_with_server(&mut client, &mut db, server, &[b"INFO", b"replication"]),
        b"master_failover_state:failover-in-progress",
    );
    assert_eq!(
        repl.replica_of_target()
            .map(|(host, port)| (host.as_bytes().to_vec(), port)),
        Some((b"127.0.0.1".to_vec(), 6385))
    );

    let _ = repl.abort_manual_failover();
    repl.become_master();
}

#[test]
fn psync_failover_promotes_replica_with_matching_cached_replid() {
    let _guard = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    repl.become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 6379);
    let old_primary_replid = [b'a'; 40];
    repl.set_cached_primary_replid(old_primary_replid);
    repl.append_to_backlog(b"abcdefg");

    let mut client = Client::new(1_010_011);
    let mut db = RedisDb::new(0);
    let reply = run(
        &mut client,
        &mut db,
        &[b"PSYNC", old_primary_replid.as_ref(), b"7", b"FAILOVER"],
    );

    assert_starts_with(&reply, b"+CONTINUE ");
    assert!(
        !repl.is_replica(),
        "PSYNC FAILOVER should promote the target"
    );
    assert!(
        client.is_replica,
        "old primary should now sync from promoted target"
    );
    repl.become_master();
}

#[test]
fn psync_failover_rejects_master_and_wrong_replid() {
    let _guard = repl_guard();
    let repl = global_replication_state();
    repl.become_master();
    let mut client = Client::new(1_010_012);
    let mut db = RedisDb::new(0);
    let old_primary_replid = [b'a'; 40];

    let reply = run(
        &mut client,
        &mut db,
        &[b"PSYNC", old_primary_replid.as_ref(), b"0", b"FAILOVER"],
    );
    assert_contains(&reply, b"ERR PSYNC FAILOVER can't be sent to a master.");

    repl.become_replica_of(RedisString::from_bytes(b"127.0.0.1"), 6379);
    repl.set_cached_primary_replid(old_primary_replid);
    client.is_replica = false;
    let wrong_replid = [b'b'; 40];
    let reply = run(
        &mut client,
        &mut db,
        &[b"PSYNC", wrong_replid.as_ref(), b"0", b"FAILOVER"],
    );
    assert_contains(&reply, b"ERR PSYNC FAILOVER replid must match my replid.");
    assert!(
        repl.is_replica(),
        "wrong replid must not promote the target"
    );
    repl.become_master();
}
