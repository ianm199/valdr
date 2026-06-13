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
    let mut invalid = Client::new(121);
    let invalid_reply = run_dispatch_with_server(
        &mut invalid,
        &mut db,
        Arc::clone(&server),
        &[b"CONFIG", b"SET", b"repl-diskless-load", b"bogus"],
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
    assert!(
        invalid_reply.starts_with(b"-ERR"),
        "invalid diskless-load mode should be rejected, got {:?}",
        String::from_utf8_lossy(&invalid_reply)
    );
    assert_eq!(
        server.live_config.repl_diskless_load(),
        ReplDisklessLoadMode::Swapdb
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
