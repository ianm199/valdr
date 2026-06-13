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
use redis_core::object::RedisObject;
use redis_core::replication::global_replication_state;
use redis_core::replication::{
    generate_runid, ReplBgsaveJob, ReplicaConn, ReplicaState, ReplicationState,
};
use redis_core::{Client, ClientId, PubSubRegistry, RedisServer};
use redis_types::RedisString;

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("valdr-{name}-{}-{nanos}", std::process::id()))
}

fn attach_waiting_replica(st: &ReplicationState, client_id: ClientId, offset: i64) {
    let (tx, _rx) = mpsc::channel();
    st.add_replica(ReplicaConn::new(
        client_id,
        ReplicaState::WaitingBgsave,
        offset,
        tx,
    ));
}

fn install_job(st: &ReplicationState, temp_path: PathBuf, waiters: Vec<ClientId>) {
    st.install_repl_bgsave_job(ReplBgsaveJob {
        child_pid: 99,
        temp_path,
        waiting_replicas: waiters,
        snapshot_offset: st.master_offset(),
        catch_up_bytes: Vec::new(),
        needs_getack_on_completion: false,
    });
    st.set_repl_child_pid(99);
}

fn assert_string_value(db: &RedisDb, key: &[u8], expected: &[u8]) {
    let key = RedisString::from_bytes(key);
    let obj = db.find(&key).expect("key should exist");
    assert_eq!(obj.as_string_bytes(), Some(expected));
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
