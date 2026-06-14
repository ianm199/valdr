//! Deterministic inner loop for full-sync lifecycle cleanup.
//!
//! The slow Tcl `integration/replication` frontier includes child-failure and
//! killed-child cases. This kit pins the Rust state transition that must hold
//! before those socket-level cases can be made reliable: a failed replication
//! BGSAVE must not leave stale waiters or temp files behind.

use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_commands::dispatch::dispatch;
use redis_core::db::RedisDb;
use redis_core::live_config::ReplDisklessLoadMode;
use redis_core::object::RedisObject;
use redis_core::replication::global_replication_state;
use redis_core::replication::{
    generate_runid, ReplBgsaveJob, ReplicaConn, ReplicaState, ReplicationState,
};
use redis_core::{Client, ClientId, PubSubRegistry, RedisServer};
use redis_protocol::frame::{encode_resp2, RespFrame};
use redis_types::RedisString;

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("valdr-{name}-{}-{nanos}", std::process::id()))
}

fn attach_waiting_replica(
    st: &ReplicationState,
    client_id: ClientId,
    offset: i64,
) -> mpsc::Receiver<Vec<u8>> {
    let (tx, rx) = mpsc::channel();
    st.add_replica(ReplicaConn::new(
        client_id,
        ReplicaState::WaitingBgsave,
        offset,
        tx,
    ));
    rx
}

fn install_job(st: &ReplicationState, temp_path: PathBuf, waiters: Vec<ClientId>) {
    install_job_with_pid(st, 99, temp_path, waiters);
}

fn install_job_with_pid(
    st: &ReplicationState,
    child_pid: i32,
    temp_path: PathBuf,
    waiters: Vec<ClientId>,
) {
    st.install_repl_bgsave_job(ReplBgsaveJob {
        child_pid,
        temp_path,
        waiting_replicas: waiters,
        snapshot_offset: st.master_offset(),
        catch_up_bytes: Vec::new(),
        needs_getack_on_completion: false,
    });
    st.set_repl_child_pid(child_pid);
}

fn assert_string_value(db: &RedisDb, key: &[u8], expected: &[u8]) {
    let key = RedisString::from_bytes(key);
    let obj = db.find(&key).expect("key should exist");
    assert_eq!(obj.as_string_bytes(), Some(expected));
}

fn assert_key_missing(db: &RedisDb, key: &[u8]) {
    assert!(
        db.find(&RedisString::from_bytes(key)).is_none(),
        "key should be absent: {:?}",
        String::from_utf8_lossy(key)
    );
}

fn global_repl_guard() -> MutexGuard<'static, ()> {
    static REPL_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    match REPL_GUARD.get_or_init(|| Mutex::new(())).lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn run_dispatch(client: &mut Client, db: &mut RedisDb, parts: &[&[u8]]) -> Vec<u8> {
    let server = Arc::new(RedisServer::default());
    run_dispatch_with_server(client, db, server, parts)
}

fn run_dispatch_with_server(
    client: &mut Client,
    db: &mut RedisDb,
    server: Arc<RedisServer>,
    parts: &[&[u8]],
) -> Vec<u8> {
    client.set_args(parts.iter().map(|p| RedisString::from_bytes(p)).collect());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let result = {
        let mut ctx = redis_core::CommandContext::with_server(client, db, server, pubsub);
        dispatch(&mut ctx)
    };
    if let Err(err) = result {
        let payload = err.to_resp_payload();
        client.reply_buf.push(b'-');
        client.reply_buf.extend_from_slice(payload.as_bytes());
        client.reply_buf.extend_from_slice(b"\r\n");
    }
    client.drain_reply()
}

fn load_library(
    server: Arc<RedisServer>,
    db: &mut RedisDb,
    client_id: ClientId,
    code: &[u8],
) -> Vec<u8> {
    let mut client = Client::new(client_id);
    client.authenticated_user = Some(RedisString::from_static(b"default"));
    run_dispatch_with_server(
        &mut client,
        db,
        server,
        &[b"FUNCTION", b"LOAD", b"REPLACE", code],
    )
}

fn flush_functions(server: Arc<RedisServer>, db: &mut RedisDb, client_id: ClientId) -> Vec<u8> {
    let mut client = Client::new(client_id);
    client.authenticated_user = Some(RedisString::from_static(b"default"));
    run_dispatch_with_server(&mut client, db, server, &[b"FUNCTION", b"FLUSH", b"SYNC"])
}

fn fcall_reply(server: Arc<RedisServer>, db: &mut RedisDb, client_id: ClientId) -> Vec<u8> {
    let mut client = Client::new(client_id);
    client.authenticated_user = Some(RedisString::from_static(b"default"));
    run_dispatch_with_server(&mut client, db, server, &[b"FCALL", b"fullsync_swap", b"0"])
}

fn dispatch_replica_link_command(client: &mut Client, db: &mut RedisDb, parts: &[&[u8]]) {
    let reply_start = client.reply_buf.len();
    client.is_replica = true;
    client.set_args(parts.iter().map(|p| RedisString::from_bytes(p)).collect());
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let result = {
        let mut ctx = redis_core::CommandContext::with_server(client, db, server, pubsub);
        dispatch(&mut ctx)
    };
    if let Err(err) = result {
        let payload = err.to_resp_payload();
        encode_resp2(&RespFrame::Error(payload), &mut client.reply_buf);
    }
    client.finish_command_reply(reply_start);
}

#[test]
fn replica_link_replies_are_protocol_violations() {
    let _guard = global_repl_guard();
    let mut db = RedisDb::new(0);

    let mut ping = Client::new(9_301);
    dispatch_replica_link_command(&mut ping, &mut db, &[b"PING"]);
    let violation = ping
        .take_replica_reply_violation_since(0)
        .expect("replica PING reply should be a link violation");
    assert_eq!(violation.command, b"ping");
    assert_eq!(violation.reply, b"+PONG\r\n");
    assert!(!violation.is_error);
    assert!(ping.reply_buf.is_empty());

    let mut get = Client::new(9_302);
    dispatch_replica_link_command(&mut get, &mut db, &[b"GET", b"k"]);
    let violation = get
        .take_replica_reply_violation_since(0)
        .expect("replica keyspace error should be a link violation");
    assert_eq!(violation.command, b"get");
    assert!(violation.is_error);
    assert!(
        violation
            .reply
            .windows(b"Replica can't interact with the keyspace".len())
            .any(|w| w == b"Replica can't interact with the keyspace"),
        "unexpected reply: {:?}",
        String::from_utf8_lossy(&violation.reply)
    );

    let mut slowlog = Client::new(9_303);
    dispatch_replica_link_command(&mut slowlog, &mut db, &[b"SLOWLOG", b"GET"]);
    let violation = slowlog
        .take_replica_reply_violation_since(0)
        .expect("replica slowlog error should be a link violation");
    assert_eq!(violation.command, b"slowlog|get");
    assert!(violation.is_error);
}

#[test]
fn failed_fullsync_job_cleans_waiters_temp_files_and_child_state() {
    let st = ReplicationState::new(generate_runid(), 64);
    let dir = unique_temp_dir("fullsync-lifecycle");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let temp_path = dir.join("temp-repl-kit.rdb");
    std::fs::write(&temp_path, b"partial rdb").expect("write temp rdb");
    std::fs::write(temp_path.with_extension("rdb.tmp"), b"partial tmp").expect("write tmp rdb");

    attach_waiting_replica(&st, 71, 0);
    attach_waiting_replica(&st, 72, 0);
    install_job(&st, temp_path.clone(), vec![71]);
    assert!(
        st.enqueue_repl_waiter(72),
        "second full-sync waiter should join the in-flight job"
    );
    assert_eq!(st.connected_replicas(), 2);

    let snapshot = st
        .repl_bgsave_job_snapshot()
        .expect("job should be installed");
    assert_eq!(snapshot.1, vec![71, 72]);

    let aborted = st.abort_repl_bgsave_job().expect("job should abort");
    assert_eq!(aborted.waiting_replicas, vec![71, 72]);
    assert_eq!(st.repl_child_pid(), 0);
    assert_eq!(
        st.connected_replicas(),
        0,
        "failed full-sync waiters must not stay registered"
    );
    assert!(st.repl_bgsave_job_snapshot().is_none());
    assert!(!temp_path.exists(), "failed job temp RDB should be removed");
    assert!(
        !temp_path.with_extension("rdb.tmp").exists(),
        "failed job side temp file should be removed"
    );

    attach_waiting_replica(&st, 73, st.master_offset());
    let next_path = dir.join("temp-repl-kit-next.rdb");
    install_job(&st, next_path, vec![73]);
    let next = st
        .repl_bgsave_job_snapshot()
        .expect("later full-sync job should install cleanly");
    assert_eq!(next.1, vec![73]);

    let _ = st.abort_repl_bgsave_job();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn killed_repl_child_is_collected_and_later_fullsync_can_deliver() {
    let st = ReplicationState::new(generate_runid(), 4);
    let dir = unique_temp_dir("fullsync-killed-child");
    std::fs::create_dir_all(&dir).expect("create temp dir");

    let dead_path = dir.join("temp-repl-dead.rdb");
    std::fs::write(&dead_path, b"partial rdb").expect("write temp rdb");
    std::fs::write(dead_path.with_extension("rdb.tmp"), b"partial tmp").expect("write tmp rdb");
    let _dead_rx = attach_waiting_replica(&st, 81, 0);
    install_job_with_pid(&st, 111, dead_path.clone(), vec![81]);

    let collected = st
        .collect_failed_repl_bgsave_child_exit(111)
        .expect("matching killed child should be collected");
    assert_eq!(collected.waiting_replicas, vec![81]);
    assert_eq!(st.repl_child_pid(), 0);
    assert_eq!(
        st.connected_replicas(),
        0,
        "wait_bgsave replica from killed child must be dropped"
    );
    assert!(st.repl_bgsave_job_snapshot().is_none());
    assert!(!dead_path.exists());
    assert!(!dead_path.with_extension("rdb.tmp").exists());
    assert!(
        st.collect_failed_repl_bgsave_child_exit(111).is_none(),
        "stale child-exit observations must not tear down later jobs"
    );

    let rx = attach_waiting_replica(&st, 82, st.master_offset());
    let next_path = dir.join("temp-repl-next.rdb");
    install_job_with_pid(&st, 112, next_path, vec![82]);
    st.append_to_backlog(b"abc");
    let job = st.take_repl_bgsave_job().expect("later job should exist");
    let outcome = st.complete_repl_bgsave_transfer(job, b"RDB".to_vec());

    assert_eq!(outcome.delivered_replicas, vec![82]);
    assert!(outcome.failed_replicas.is_empty());
    assert_eq!(outcome.retained_catchup_len, 3);
    assert_eq!(st.repl_child_pid(), 0);
    assert_eq!(st.connected_replicas(), 1);
    {
        let guard = st.replicas.lock().unwrap();
        assert_eq!(guard[&82].state(), ReplicaState::SendingRdb);
    }
    assert_eq!(rx.recv().unwrap(), b"$3\r\nRDB".to_vec());
    assert_eq!(rx.recv().unwrap(), b"abc".to_vec());
    assert_eq!(st.account_replica_output_drained(82, 10), 0);
    {
        let guard = st.replicas.lock().unwrap();
        assert_eq!(guard[&82].state(), ReplicaState::SendingRdb);
    }
    assert!(st.acknowledge_replica(82, st.master_offset(), None, 1_000));
    {
        let guard = st.replicas.lock().unwrap();
        assert_eq!(guard[&82].state(), ReplicaState::Online);
    }
    assert_eq!(st.read_history_at(0, 3).as_deref(), Some(b"abc".as_slice()));

    let _ = st.abort_repl_bgsave_job();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fullsync_completion_includes_backlog_tail_after_job_detaches() {
    let st = ReplicationState::new(generate_runid(), 64);
    let rx = attach_waiting_replica(&st, 85, 0);

    install_job(&st, PathBuf::from("detached-tail.rdb"), vec![85]);
    st.append_to_backlog(b"a");
    let job = st
        .take_repl_bgsave_job()
        .expect("reaper should detach the job before transfer");
    st.append_to_backlog(b"b");

    let outcome = st.complete_repl_bgsave_transfer(job, b"RDB".to_vec());

    assert_eq!(outcome.delivered_replicas, vec![85]);
    assert!(outcome.failed_replicas.is_empty());
    assert_eq!(
        outcome.retained_catchup_len, 2,
        "catch-up must include bytes appended after the job left active state"
    );
    assert_eq!(st.replicas_snapshot()[0].1, "send_bulk");
    assert!(st.fullsync_transfer_in_progress());
    assert_eq!(rx.recv().unwrap(), b"$3\r\nRDB".to_vec());
    assert_eq!(rx.recv().unwrap(), b"ab".to_vec());
    assert_eq!(st.account_replica_output_drained(85, 9), 0);
    assert_eq!(st.replicas_snapshot()[0].1, "send_bulk");
    assert!(st.fullsync_transfer_in_progress());
    assert!(st.acknowledge_replica(85, st.master_offset(), None, 1_000));
    assert_eq!(st.replicas_snapshot()[0].1, "online");
    assert!(!st.fullsync_transfer_in_progress());
    assert_eq!(st.read_history_at(0, 2).as_deref(), Some(b"ab".as_slice()));
}

#[test]
fn waiting_fullsync_job_stays_in_progress_without_visible_child_pid() {
    let st = ReplicationState::new(generate_runid(), 64);
    let _rx = attach_waiting_replica(&st, 86, 0);

    install_job_with_pid(&st, 0, PathBuf::from("waiting-no-pid.rdb"), vec![86]);

    assert!(
        st.fullsync_transfer_in_progress(),
        "INFO persistence must still report replication BGSAVE in progress while a full-sync waiter is waiting"
    );

    let removed = st.remove_replica(86);
    assert!(removed.was_repl_bgsave_waiter);
    assert_eq!(removed.remaining_repl_bgsave_waiters, 0);
    assert!(
        !st.fullsync_transfer_in_progress(),
        "after the last waiter disconnects, a zero-pid placeholder job no longer represents active full sync"
    );
    let _ = st.abort_repl_bgsave_job();
}

#[test]
fn last_fullsync_waiter_disconnect_marks_repl_child_useless() {
    let st = ReplicationState::new(generate_runid(), 4);
    let dir = unique_temp_dir("fullsync-useless-child");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let temp_path = dir.join("temp-repl-useless.rdb");
    std::fs::write(&temp_path, b"partial rdb").expect("write temp rdb");

    let _rx1 = attach_waiting_replica(&st, 83, 0);
    let _rx2 = attach_waiting_replica(&st, 84, 0);
    install_job_with_pid(&st, 221, temp_path.clone(), vec![83, 84]);

    let first = st.remove_replica(83);
    assert!(first.removed);
    assert!(first.was_repl_bgsave_waiter);
    assert_eq!(first.remaining_repl_bgsave_waiters, 1);
    assert_eq!(first.useless_repl_child_pid, None);
    assert_eq!(st.repl_child_pid(), 221);
    let snapshot = st
        .repl_bgsave_job_snapshot()
        .expect("job should remain while one waiter still needs it");
    assert_eq!(snapshot.1, vec![84]);

    let last = st.remove_replica(84);
    assert!(last.removed);
    assert!(last.was_repl_bgsave_waiter);
    assert_eq!(last.remaining_repl_bgsave_waiters, 0);
    assert_eq!(last.useless_repl_child_pid, Some(221));
    assert_eq!(
        st.repl_child_pid(),
        221,
        "core reports the useless child but leaves OS reaping to the server"
    );
    let empty_job = st
        .repl_bgsave_job_snapshot()
        .expect("server still needs the job until waitpid collects the child");
    assert!(empty_job.1.is_empty());

    let collected = st
        .collect_failed_repl_bgsave_child_exit(221)
        .expect("signaled useless child should be collected");
    assert!(collected.waiting_replicas.is_empty());
    assert_eq!(st.repl_child_pid(), 0);
    assert!(st.repl_bgsave_job_snapshot().is_none());
    assert!(!temp_path.exists());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn replica_rdb_replacement_is_atomic_on_failed_incoming_fullsync() {
    let dir = unique_temp_dir("fullsync-atomic-rdb");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let bad_path = dir.join("bad-incoming.rdb");
    let good_path = dir.join("good-incoming.rdb");
    std::fs::write(&bad_path, b"REDIS0011short").expect("write corrupt RDB");

    let mut current = vec![RedisDb::new(0), RedisDb::new(1)];
    current[0].add(
        RedisString::from_static(b"stable"),
        RedisObject::new_string(b"old"),
    );
    current[1].add(
        RedisString::from_static(b"other-db"),
        RedisObject::new_string(b"old-db1"),
    );

    let err = redis_core::rdb::load_into_dbs_replacing(&mut current, &bad_path)
        .expect_err("corrupt incoming RDB must fail");
    assert!(
        err.to_string().contains("short") || err.to_string().contains("EOF"),
        "unexpected corrupt RDB error: {err}"
    );
    assert_string_value(&current[0], b"stable", b"old");
    assert_string_value(&current[1], b"other-db", b"old-db1");

    let mut incoming = vec![RedisDb::new(0), RedisDb::new(1)];
    incoming[0].add(
        RedisString::from_static(b"stable"),
        RedisObject::new_string(b"new"),
    );
    redis_core::rdb::save_rdb_databases(&incoming, &good_path).expect("save valid incoming RDB");

    let msg = redis_core::rdb::load_into_dbs_replacing(&mut current, &good_path)
        .expect("valid incoming RDB should replace current data");
    assert!(msg.contains("1 keys"));
    assert_string_value(&current[0], b"stable", b"new");
    assert!(
        current[1]
            .find(&RedisString::from_static(b"other-db"))
            .is_none(),
        "successful replacement should drop keys absent from the incoming RDB"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn primary_link_script_write_applies_on_readonly_replica() {
    let _guard = global_repl_guard();
    let repl = global_replication_state();
    let was_replica = repl.is_replica();
    repl.become_replica_of(RedisString::from_static(b"127.0.0.1"), 6379);

    let no_write_script = b"return 'replica-local-read'";
    let script = b"return redis.call('SET','from-primary','applied')";

    let mut readonly_db = RedisDb::new(0);
    let mut readonly = Client::new(90);
    let readonly_reply = run_dispatch(
        &mut readonly,
        &mut readonly_db,
        &[b"EVAL", no_write_script, b"0"],
    );

    let mut ordinary_db = RedisDb::new(0);
    let mut ordinary = Client::new(91);
    let ordinary_reply = run_dispatch(&mut ordinary, &mut ordinary_db, &[b"EVAL", script, b"0"]);

    let mut apply_db = RedisDb::new(0);
    let mut apply = Client::new(92);
    apply.replication_apply = true;
    apply.authenticated_user = Some(RedisString::from_static(b"default"));
    let apply_reply = run_dispatch(&mut apply, &mut apply_db, &[b"EVAL", script, b"0"]);
    let applied_value = apply_db
        .find(&RedisString::from_static(b"from-primary"))
        .and_then(|obj| obj.as_string_bytes().map(|bytes| bytes.to_vec()));

    repl.become_master();
    if was_replica {
        repl.become_replica_of(RedisString::from_static(b"127.0.0.1"), 6379);
    }

    assert!(
        !readonly_reply.starts_with(b"-READONLY"),
        "ordinary no-write scripts are allowed on read-only replicas, got {:?}",
        String::from_utf8_lossy(&readonly_reply)
    );
    assert!(
        ordinary_reply.starts_with(b"-READONLY"),
        "ordinary read-only replica clients must still reject script writes, got {:?}",
        String::from_utf8_lossy(&ordinary_reply)
    );
    assert!(
        !apply_reply.starts_with(b"-READONLY"),
        "primary-link script application must bypass read-only client guards, got {:?}",
        String::from_utf8_lossy(&apply_reply)
    );
    assert_eq!(applied_value.as_deref(), Some(b"applied".as_slice()));
}

#[test]
fn writable_replica_fcall_bypasses_script_readonly_preflight() {
    let _guard = global_repl_guard();
    let repl = global_replication_state();
    let was_replica = repl.is_replica();
    repl.become_master();

    let server = Arc::new(RedisServer::default());
    let mut db = RedisDb::new(0);
    let library = b"#!lua name=fullsync_fcall\nserver.register_function('fullsync_fcall_read', function() return 'hello-from-function' end)";

    let mut loader = Client::new(93);
    loader.authenticated_user = Some(RedisString::from_static(b"default"));
    let load_reply = run_dispatch_with_server(
        &mut loader,
        &mut db,
        Arc::clone(&server),
        &[b"FUNCTION", b"LOAD", b"REPLACE", library],
    );

    repl.become_replica_of(RedisString::from_static(b"127.0.0.1"), 6379);

    let mut readonly = Client::new(94);
    let readonly_reply = run_dispatch_with_server(
        &mut readonly,
        &mut db,
        Arc::clone(&server),
        &[b"FCALL", b"fullsync_fcall_read", b"0"],
    );

    server.live_config.set_slave_read_only(false);
    let mut writable = Client::new(95);
    let writable_reply = run_dispatch_with_server(
        &mut writable,
        &mut db,
        Arc::clone(&server),
        &[b"FCALL", b"fullsync_fcall_read", b"0"],
    );

    repl.become_master();
    if was_replica {
        repl.become_replica_of(RedisString::from_static(b"127.0.0.1"), 6379);
    }

    assert!(
        !load_reply.starts_with(b"-"),
        "function load should succeed before role flip, got {:?}",
        String::from_utf8_lossy(&load_reply)
    );
    assert!(
        readonly_reply.starts_with(b"-READONLY"),
        "write-capable FCALL remains blocked while replica-read-only is yes, got {:?}",
        String::from_utf8_lossy(&readonly_reply)
    );
    assert_eq!(
        writable_reply, b"$19\r\nhello-from-function\r\n",
        "replica-read-only no should allow FCALL through the script preflight"
    );
}

#[test]
fn async_loading_serves_old_db_but_blocks_no_async_loading_commands() {
    let server = Arc::new(RedisServer::default());
    server.persistence.set_async_loading(true);

    let mut db = RedisDb::new(0);
    db.add(
        RedisString::from_static(b"old-key"),
        RedisObject::new_string(b"old-value"),
    );

    let mut reader = Client::new(96);
    let get_reply = run_dispatch_with_server(
        &mut reader,
        &mut db,
        Arc::clone(&server),
        &[b"GET", b"old-key"],
    );

    let mut info = Client::new(97);
    let info_reply = run_dispatch_with_server(
        &mut info,
        &mut db,
        Arc::clone(&server),
        &[b"INFO", b"persistence"],
    );

    let mut config = Client::new(98);
    let config_reply = run_dispatch_with_server(
        &mut config,
        &mut db,
        Arc::clone(&server),
        &[b"CONFIG", b"SET", b"appendonly", b"no"],
    );
    let mut lua_limit = Client::new(99);
    let lua_limit_reply = run_dispatch_with_server(
        &mut lua_limit,
        &mut db,
        Arc::clone(&server),
        &[b"CONFIG", b"SET", b"lua-time-limit", b"10"],
    );

    assert_eq!(get_reply, b"$9\r\nold-value\r\n");
    assert!(
        info_reply
            .windows(b"loading:0".len())
            .any(|w| w == b"loading:0"),
        "INFO should hide ordinary loading during async loading: {:?}",
        String::from_utf8_lossy(&info_reply)
    );
    assert!(
        info_reply
            .windows(b"async_loading:1".len())
            .any(|w| w == b"async_loading:1"),
        "INFO should expose async_loading: {:?}",
        String::from_utf8_lossy(&info_reply)
    );
    assert!(
        config_reply.starts_with(b"-LOADING"),
        "NO_ASYNC_LOADING commands should be blocked during async loading, got {:?}",
        String::from_utf8_lossy(&config_reply)
    );
    assert_eq!(
        lua_limit_reply, b"+OK\r\n",
        "safe script timeout tuning should remain available during async loading"
    );
    assert_eq!(server.live_config.lua_time_limit_ms(), 10);

    server.persistence.set_loading(false);
    assert!(!server.persistence.loading());
    assert!(!server.persistence.async_loading());

    server.persistence.set_loading(true);
    let mut key_delay = Client::new(100);
    let key_delay_reply = run_dispatch_with_server(
        &mut key_delay,
        &mut db,
        Arc::clone(&server),
        &[b"CONFIG", b"SET", b"key-load-delay", b"0"],
    );
    assert_eq!(
        key_delay_reply, b"+OK\r\n",
        "test load-delay tuning should remain available during ordinary loading"
    );
    server.persistence.set_loading(false);
}

#[test]
fn repl_diskless_load_config_updates_live_mode() {
    let server = Arc::new(RedisServer::default());
    let mut db = RedisDb::new(0);

    let mut setter = Client::new(119);
    let set_reply = run_dispatch_with_server(
        &mut setter,
        &mut db,
        Arc::clone(&server),
        &[b"CONFIG", b"SET", b"repl-diskless-load", b"swapdb"],
    );
    let mut getter = Client::new(120);
    let get_reply = run_dispatch_with_server(
        &mut getter,
        &mut db,
        Arc::clone(&server),
        &[b"CONFIG", b"GET", b"repl-diskless-load"],
    );
    assert_eq!(set_reply, b"+OK\r\n");
    assert_eq!(
        server.live_config.repl_diskless_load(),
        ReplDisklessLoadMode::Swapdb
    );
    assert!(
        get_reply
            .windows(b"swapdb".len())
            .any(|window| window == b"swapdb"),
        "CONFIG GET should expose the live diskless-load mode: {:?}",
        String::from_utf8_lossy(&get_reply)
    );

    let mut on_empty = Client::new(122);
    let on_empty_reply = run_dispatch_with_server(
        &mut on_empty,
        &mut db,
        Arc::clone(&server),
        &[b"CONFIG", b"SET", b"repl-diskless-load", b"on-empty-db"],
    );
    assert_eq!(on_empty_reply, b"+OK\r\n");
    assert_eq!(
        server.live_config.repl_diskless_load(),
        ReplDisklessLoadMode::OnEmptyDb
    );

    let mut invalid = Client::new(121);
    let invalid_reply = run_dispatch_with_server(
        &mut invalid,
        &mut db,
        Arc::clone(&server),
        &[b"CONFIG", b"SET", b"repl-diskless-load", b"bogus"],
    );
    assert!(
        invalid_reply.starts_with(b"-ERR"),
        "invalid diskless-load mode should be rejected, got {:?}",
        String::from_utf8_lossy(&invalid_reply)
    );
    assert_eq!(
        server.live_config.repl_diskless_load(),
        ReplDisklessLoadMode::OnEmptyDb
    );
}

#[test]
fn successful_swapdb_fullsync_replaces_dataset_and_functions_together() {
    let _guard = global_repl_guard();
    let repl = global_replication_state();
    let was_replica = repl.is_replica();
    repl.become_master();

    let server = Arc::new(RedisServer::default());
    let old_library = b"#!lua name=fullsync_swap_lib\nserver.register_function('fullsync_swap', function() return 'hello1' end)";
    let new_library = b"#!lua name=fullsync_swap_lib\nserver.register_function('fullsync_swap', function() return 'hello2' end)";

    let mut live_db = RedisDb::new(0);
    let _ = flush_functions(Arc::clone(&server), &mut live_db, 110);
    assert!(
        !load_library(Arc::clone(&server), &mut live_db, 111, old_library).starts_with(b"-"),
        "old function library should load"
    );
    assert_eq!(
        fcall_reply(Arc::clone(&server), &mut live_db, 112),
        b"$6\r\nhello1\r\n"
    );

    assert!(
        !load_library(Arc::clone(&server), &mut live_db, 113, new_library).starts_with(b"-"),
        "new function library should load for incoming snapshot encoding"
    );
    let incoming_function_payloads = redis_commands::eval::function_rdb_payloads();
    assert!(!incoming_function_payloads.is_empty());

    assert!(
        !load_library(Arc::clone(&server), &mut live_db, 114, old_library).starts_with(b"-"),
        "live function library should be restored before incoming full sync"
    );
    assert_eq!(
        fcall_reply(Arc::clone(&server), &mut live_db, 115),
        b"$6\r\nhello1\r\n"
    );

    let dir = unique_temp_dir("fullsync-swapdb-functions");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let bad_function_path = dir.join("bad-functions.rdb");
    let good_path = dir.join("good-functions.rdb");

    let mut current = vec![RedisDb::new(0)];
    current[0].add(
        RedisString::from_static(b"old-key"),
        RedisObject::new_string(b"old-value"),
    );
    let mut incoming = vec![RedisDb::new(0)];
    incoming[0].add(
        RedisString::from_static(b"new-key"),
        RedisObject::new_string(b"new-value"),
    );

    redis_core::rdb::save_rdb_databases_with_functions(
        &incoming,
        &[b"not-a-function-dump".to_vec()],
        &bad_function_path,
    )
    .expect("bad-function RDB should still serialize as an opaque payload");
    let bad_plan = redis_core::rdb::load_replacement_plan(current.len(), &bad_function_path)
        .expect("DB plan should load before function payload validation");
    assert!(
        redis_commands::eval::prepare_rdb_function_replacement(
            &bad_plan.outcome.function_payloads,
        )
        .is_err(),
        "invalid incoming function payload should reject the whole replacement"
    );
    assert_string_value(&current[0], b"old-key", b"old-value");
    assert_eq!(
        fcall_reply(Arc::clone(&server), &mut live_db, 116),
        b"$6\r\nhello1\r\n",
        "old function must remain live after rejected function payload"
    );

    redis_core::rdb::save_rdb_databases_with_functions(
        &incoming,
        &incoming_function_payloads,
        &good_path,
    )
    .expect("good incoming RDB should serialize");
    let good_plan = redis_core::rdb::load_replacement_plan(current.len(), &good_path)
        .expect("good incoming RDB should stage");
    let prepared = redis_commands::eval::prepare_rdb_function_replacement(
        &good_plan.outcome.function_payloads,
    )
    .expect("incoming functions should prepare");
    current = good_plan.dbs;
    redis_commands::eval::install_rdb_function_replacement(prepared);

    assert_key_missing(&current[0], b"old-key");
    assert_string_value(&current[0], b"new-key", b"new-value");
    assert_eq!(
        fcall_reply(Arc::clone(&server), &mut live_db, 117),
        b"$6\r\nhello2\r\n",
        "successful replacement should expose incoming functions"
    );

    let _ = flush_functions(Arc::clone(&server), &mut live_db, 118);
    let _ = std::fs::remove_dir_all(&dir);
    repl.become_master();
    if was_replica {
        repl.become_replica_of(RedisString::from_static(b"127.0.0.1"), 6379);
    }
}
