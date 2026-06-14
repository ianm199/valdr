//! Deterministic inner loop for PSYNC reconnect decisions.
//!
//! This kit drives the real `psync_command` entrypoint where possible, then
//! uses local `ReplicationState` values for replica-side reconnect cache cases
//! that should not depend on the process-global replication state.

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use redis_core::client_info::client_info_registry;
use redis_core::replication::{
    global_replication_state, replica_link_code, ReplicaConn, ReplicaState, ReplicationState,
    DEFAULT_REPL_BACKLOG_SIZE, REPLICA_CAPA_DUAL_CHANNEL,
};
use redis_core::{Client, PubSubRegistry, RedisDb, RedisServer};
use redis_protocol::frame::{encode_resp2, RespFrame};
use redis_types::RedisString;

use redis_commands::dispatch::dispatch;

fn psync_guard() -> MutexGuard<'static, ()> {
    static PSYNC_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    match PSYNC_GUARD.get_or_init(|| Mutex::new(())).lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn arg(bytes: &[u8]) -> RedisString {
    RedisString::from_bytes(bytes)
}

fn runid_string(st: &ReplicationState) -> Vec<u8> {
    st.runid().to_vec()
}

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

fn fullresync_offset(reply: &[u8]) -> i64 {
    let text = std::str::from_utf8(reply).expect("FULLRESYNC reply should be UTF-8");
    let mut fields = text.split_whitespace();
    assert_eq!(fields.next(), Some("+FULLRESYNC"));
    let _runid = fields.next().expect("FULLRESYNC runid");
    fields
        .next()
        .expect("FULLRESYNC offset")
        .parse::<i64>()
        .expect("FULLRESYNC offset should parse")
}

struct PsyncDrive {
    reply: Vec<u8>,
    sent: Vec<u8>,
    counters_before: (u64, u64, u64),
    counters_after: (u64, u64, u64),
}

fn drive_psync_with_server(
    client_id: u64,
    args: Vec<Vec<u8>>,
    server: Arc<RedisServer>,
) -> PsyncDrive {
    let repl = global_replication_state();
    let counters_before = repl.sync_counters();
    let (tx, rx) = mpsc::channel();
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    pubsub.lock().unwrap().register_sender(client_id, tx);

    let mut c = Client::new(client_id);
    c.set_args(args.into_iter().map(RedisString::from_vec).collect());
    let mut db = RedisDb::new(0);
    {
        let mut ctx =
            redis_core::CommandContext::with_server(&mut c, &mut db, server, pubsub.clone());
        redis_commands::replication::psync_command(&mut ctx).expect("psync command");
    }

    let mut sent = Vec::new();
    while let Ok(chunk) = rx.try_recv() {
        sent.extend_from_slice(&chunk);
    }
    let reply = c.drain_reply();
    let counters_after = repl.sync_counters();
    repl.remove_replica(client_id);
    cleanup_repl_bgsave(&repl);
    PsyncDrive {
        reply,
        sent,
        counters_before,
        counters_after,
    }
}

fn drive_psync_after_replconf_with_server(
    client_id: u64,
    replconf_args: Vec<Vec<u8>>,
    psync_args: Vec<Vec<u8>>,
    server: Arc<RedisServer>,
) -> PsyncDrive {
    let repl = global_replication_state();
    let counters_before = repl.sync_counters();
    let (tx, rx) = mpsc::channel();
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    pubsub.lock().unwrap().register_sender(client_id, tx);

    let mut c = Client::new(client_id);
    let mut db = RedisDb::new(0);
    let advertises_dual_channel = replconf_args
        .iter()
        .any(|arg| arg.eq_ignore_ascii_case(b"dual-channel"));
    c.set_args(
        replconf_args
            .into_iter()
            .map(RedisString::from_vec)
            .collect(),
    );
    {
        let mut ctx = redis_core::CommandContext::with_server(
            &mut c,
            &mut db,
            server.clone(),
            pubsub.clone(),
        );
        redis_commands::replication::replconf_command(&mut ctx).expect("replconf command");
    }
    assert_eq!(c.drain_reply(), b"+OK\r\n");
    if advertises_dual_channel {
        assert_ne!(
            repl.replica_capa_flags_for_client(client_id) & REPLICA_CAPA_DUAL_CHANNEL,
            0,
            "REPLCONF capa dual-channel should be available to the following PSYNC"
        );
    }

    c.set_args(psync_args.into_iter().map(RedisString::from_vec).collect());
    {
        let mut ctx =
            redis_core::CommandContext::with_server(&mut c, &mut db, server, pubsub.clone());
        redis_commands::replication::psync_command(&mut ctx).expect("psync command");
    }

    let mut sent = Vec::new();
    while let Ok(chunk) = rx.try_recv() {
        sent.extend_from_slice(&chunk);
    }
    let reply = c.drain_reply();
    let counters_after = repl.sync_counters();
    repl.remove_replica(client_id);
    cleanup_repl_bgsave(&repl);
    PsyncDrive {
        reply,
        sent,
        counters_before,
        counters_after,
    }
}

fn drive_psync(client_id: u64, args: Vec<Vec<u8>>) -> PsyncDrive {
    drive_psync_with_server(client_id, args, Arc::new(RedisServer::default()))
}

fn run_dispatch(client: &mut Client, db: &mut RedisDb, cmd: &[&[u8]]) -> Vec<u8> {
    client.set_args(cmd.iter().map(|p| RedisString::from_bytes(p)).collect());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let server = Arc::new(RedisServer::default());
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

fn reap_repl_child_pid(pid: i32) {
    if pid <= 0 {
        return;
    }
    #[cfg(unix)]
    unsafe {
        let _ = libc::kill(pid as libc::pid_t, libc::SIGTERM);
        let mut status: libc::c_int = 0;
        let _ = libc::waitpid(pid as libc::pid_t, &mut status, 0);
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
    }
}

fn cleanup_repl_bgsave(repl: &Arc<ReplicationState>) {
    if let Some(job) = repl.take_repl_bgsave_job() {
        let _ = std::fs::remove_file(&job.temp_path);
        let _ = std::fs::remove_file(Path::new(&job.temp_path).with_extension("rdb.tmp"));
    }
    let pid = repl.repl_child_pid();
    reap_repl_child_pid(pid);
    repl.set_repl_child_pid(0);
}

fn reset_global_primary(repl: &Arc<ReplicationState>, backlog_size: usize) {
    cleanup_repl_bgsave(repl);
    repl.resize_backlog_preserving_history(backlog_size);
    let runid = *repl.runid();
    repl.adopt_fullresync_primary(runid, 0);
    repl.selected_db.store(-1, Ordering::Release);
    repl.set_zero_offset_partial_resync_allowed(false);
    repl.become_master();
}

struct GlobalReplReset {
    repl: Arc<ReplicationState>,
}

impl GlobalReplReset {
    fn new(repl: &Arc<ReplicationState>) -> Self {
        reset_global_primary(repl, DEFAULT_REPL_BACKLOG_SIZE);
        Self {
            repl: Arc::clone(repl),
        }
    }
}

impl Drop for GlobalReplReset {
    fn drop(&mut self) {
        reset_global_primary(&self.repl, DEFAULT_REPL_BACKLOG_SIZE);
    }
}

#[test]
fn same_primary_reconnect_gets_continue_and_replays_catchup() {
    let _g = psync_guard();
    let repl = global_replication_state();
    repl.become_master();
    cleanup_repl_bgsave(&repl);

    let provided = repl.master_offset();
    let catchup = resp(&[b"SET", b"psync-kit", b"v"]);
    repl.append_to_backlog(&catchup);
    let runid = runid_string(&repl);
    let drive = drive_psync(
        1_100_001,
        vec![b"PSYNC".to_vec(), runid, provided.to_string().into_bytes()],
    );

    assert!(
        drive.reply.starts_with(b"+CONTINUE"),
        "same-primary in-window reconnect should continue, got {:?}",
        String::from_utf8_lossy(&drive.reply)
    );
    assert!(
        drive
            .sent
            .windows(catchup.len())
            .any(|w| w == catchup.as_slice()),
        "+CONTINUE must replay the missing backlog bytes, sent {:?}",
        String::from_utf8_lossy(&drive.sent)
    );
    assert_eq!(
        drive.counters_after.1,
        drive.counters_before.1 + 1,
        "sync_partial_ok should increment"
    );
    assert_eq!(drive.counters_after.0, drive.counters_before.0);
    assert_eq!(drive.counters_after.2, drive.counters_before.2);
}

#[test]
fn same_primary_reconnect_replays_retained_fullsync_history_beyond_backlog() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);
    repl.resize_backlog_preserving_history(16);

    let fullsync_owner = 1_100_018;
    let (owner_tx, _owner_rx) = mpsc::channel();
    repl.add_replica(ReplicaConn::new(
        fullsync_owner,
        ReplicaState::SendingRdb,
        0,
        owner_tx,
    ));

    let already_applied = resp(&[b"SET", b"retained-a", b"1"]);
    repl.append_to_backlog(&already_applied);
    repl.retain_fullsync_history(0, already_applied, &[fullsync_owner]);
    let provided = repl.master_offset();
    let catchup = resp(&[b"SET", b"retained-b", b"2"]);
    repl.append_to_backlog(&catchup);
    assert!(
        repl.backlog_snapshot().0 < provided,
        "retained full-sync history should keep the reconnect offset readable beyond the circular backlog"
    );

    let runid = runid_string(&repl);
    let drive = drive_psync(
        1_100_019,
        vec![b"PSYNC".to_vec(), runid, provided.to_string().into_bytes()],
    );

    assert!(
        drive.reply.starts_with(b"+CONTINUE"),
        "same-primary reconnect should continue from retained full-sync history, got {:?}",
        String::from_utf8_lossy(&drive.reply)
    );
    assert_eq!(
        drive.sent, catchup,
        "+CONTINUE should replay exactly the missing retained-history tail"
    );
    assert_eq!(drive.counters_after.1, drive.counters_before.1 + 1);
    assert_eq!(drive.counters_after.0, drive.counters_before.0);
    assert_eq!(drive.counters_after.2, drive.counters_before.2);

    repl.remove_replica(fullsync_owner);
}

#[test]
fn same_primary_zero_offset_reconnect_gets_continue_and_replays_catchup() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);

    assert_eq!(repl.master_offset(), 0);
    let catchup = resp(&[b"SET", b"zero-offset", b"v"]);
    repl.append_to_backlog(&catchup);
    let runid = runid_string(&repl);
    let drive = drive_psync(1_100_011, vec![b"PSYNC".to_vec(), runid, b"0".to_vec()]);

    assert!(
        drive.reply.starts_with(b"+CONTINUE"),
        "same-primary zero-offset reconnect should continue, got {:?}",
        String::from_utf8_lossy(&drive.reply)
    );
    assert!(
        drive
            .sent
            .windows(catchup.len())
            .any(|w| w == catchup.as_slice()),
        "+CONTINUE from offset zero must replay retained backlog bytes, sent {:?}",
        String::from_utf8_lossy(&drive.sent)
    );
    assert_eq!(drive.counters_after.1, drive.counters_before.1 + 1);
    assert_eq!(drive.counters_after.0, drive.counters_before.0);
    assert_eq!(drive.counters_after.2, drive.counters_before.2);
}

#[test]
fn killed_last_replica_at_zero_offset_keeps_backlog_window_for_partial_reconnect() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);

    let old_client_id = 1_100_013;
    let (tx, _rx) = mpsc::channel();
    repl.add_replica(ReplicaConn::new(old_client_id, ReplicaState::Online, 0, tx));
    {
        let mut guard = client_info_registry().lock().unwrap();
        guard.deregister(old_client_id);
        guard.register(old_client_id, "127.0.0.1:0".to_string());
        guard.mark_killed(old_client_id);
    }
    assert_eq!(repl.connected_replicas(), 0);
    assert!(
        repl.should_propagate_writes(),
        "the backlog TTL window should stay active after the last replica disconnects even when histlen is still zero"
    );

    let mut writer = Client::new(1_100_014);
    let mut db = RedisDb::new(0);
    let reply = run_dispatch(&mut writer, &mut db, &[b"SET", b"after-kill", b"v"]);
    assert_eq!(reply, b"+OK\r\n");
    let (first, master, histlen, _) = repl.backlog_snapshot();
    assert_eq!(first, 0);
    assert!(master > 0);
    assert!(histlen > 0);

    let runid = runid_string(&repl);
    let drive = drive_psync(1_100_015, vec![b"PSYNC".to_vec(), runid, b"0".to_vec()]);
    assert!(
        drive.reply.starts_with(b"+CONTINUE"),
        "writes after a killed zero-offset replica must remain available for partial reconnect, got {:?}",
        String::from_utf8_lossy(&drive.reply)
    );
    let expected_catchup = resp(&[b"SET", b"after-kill", b"v"]);
    assert!(
        drive
            .sent
            .windows(expected_catchup.len())
            .any(|w| w == expected_catchup.as_slice()),
        "+CONTINUE should replay the write that happened while no replica was connected"
    );
    assert_eq!(drive.counters_after.1, drive.counters_before.1 + 1);
    assert_eq!(drive.counters_after.0, drive.counters_before.0);
    assert_eq!(drive.counters_after.2, drive.counters_before.2);

    client_info_registry()
        .lock()
        .unwrap()
        .deregister(old_client_id);
}

#[test]
fn dual_channel_capable_fullsync_counts_logical_main_channel_psync() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);
    let server = Arc::new(RedisServer::default());
    server
        .live_config
        .set_dual_channel_replication_enabled(true);

    let client_id = 1_100_016;
    let drive = drive_psync_after_replconf_with_server(
        client_id,
        vec![
            b"REPLCONF".to_vec(),
            b"capa".to_vec(),
            b"psync2".to_vec(),
            b"dual-channel".to_vec(),
        ],
        vec![b"PSYNC".to_vec(), b"?".to_vec(), b"-1".to_vec()],
        server,
    );

    assert!(
        drive.reply.starts_with(b"+FULLRESYNC"),
        "the port still uses the ordinary full-sync transport for now, got {:?}",
        String::from_utf8_lossy(&drive.reply)
    );
    assert_eq!(drive.counters_after.0, drive.counters_before.0 + 1);
    assert_eq!(
        drive.counters_after.1,
        drive.counters_before.1 + 1,
        "dual-channel full sync should account the logical main-channel +CONTINUE"
    );
    assert_eq!(drive.counters_after.2, drive.counters_before.2);
}

#[test]
fn dual_channel_capability_is_ignored_when_master_config_disables_it() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);
    let server = Arc::new(RedisServer::default());
    server
        .live_config
        .set_dual_channel_replication_enabled(false);

    let drive = drive_psync_after_replconf_with_server(
        1_100_017,
        vec![
            b"REPLCONF".to_vec(),
            b"capa".to_vec(),
            b"psync2".to_vec(),
            b"dual-channel".to_vec(),
        ],
        vec![b"PSYNC".to_vec(), b"?".to_vec(), b"-1".to_vec()],
        server,
    );

    assert!(drive.reply.starts_with(b"+FULLRESYNC"));
    assert_eq!(drive.counters_after.0, drive.counters_before.0 + 1);
    assert_eq!(
        drive.counters_after.1, drive.counters_before.1,
        "master-side dual-channel config must gate the provisional accounting"
    );
    assert_eq!(drive.counters_after.2, drive.counters_before.2);
}

#[test]
fn same_primary_reconnect_after_db9_minus_zero_replays_later_zero_only() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);

    let key = b"316637927";
    let mut already_applied = Vec::new();
    already_applied.extend(resp(&[b"SELECT", b"9"]));
    already_applied.extend(resp(&[b"SET", key, b"-0"]));
    let provided = repl.append_to_backlog(&already_applied);
    let catchup = resp(&[b"SET", key, b"0"]);
    repl.append_to_backlog(&catchup);

    let runid = runid_string(&repl);
    let drive = drive_psync(
        1_100_012,
        vec![b"PSYNC".to_vec(), runid, provided.to_string().into_bytes()],
    );

    assert!(
        drive.reply.starts_with(b"+CONTINUE"),
        "same-primary reconnect after DB 9 -0 frame should continue, got {:?}",
        String::from_utf8_lossy(&drive.reply)
    );
    assert_eq!(
        drive.sent, catchup,
        "+CONTINUE should replay only the later DB 9 overwrite"
    );
    assert_eq!(drive.counters_after.1, drive.counters_before.1 + 1);
    assert_eq!(drive.counters_after.0, drive.counters_before.0);
    assert_eq!(drive.counters_after.2, drive.counters_before.2);
}

#[test]
fn fresh_fullsync_catchup_prefixes_selected_db_before_first_active_write() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);
    let server = Arc::new(RedisServer::default());
    let client_id = 1_100_020;
    let snapshot_offset = repl.master_offset();

    repl.selected_db.store(9, Ordering::Release);

    let (tx, rx) = mpsc::channel();
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    pubsub.lock().unwrap().register_sender(client_id, tx);

    let mut client = Client::new(client_id);
    client.set_args(vec![
        RedisString::from_static(b"PSYNC"),
        RedisString::from_static(b"?"),
        RedisString::from_static(b"-1"),
    ]);
    let mut db = RedisDb::new(0);
    {
        let mut ctx = redis_core::CommandContext::with_server(&mut client, &mut db, server, pubsub);
        redis_commands::replication::psync_command(&mut ctx).expect("psync command");
    }
    let reply = client.drain_reply();
    assert!(
        reply.starts_with(b"+FULLRESYNC"),
        "fresh PSYNC should start a full sync, got {:?}",
        String::from_utf8_lossy(&reply)
    );

    let select9 = resp(&[b"SELECT", b"9"]);
    let zadd = resp(&[
        b"ZADD",
        b"200185508560",
        b"0.3282887667083595",
        b"-785098819",
    ]);
    repl.append_to_backlog(&zadd);

    let job = repl
        .take_repl_bgsave_job()
        .expect("fresh full sync should install a replication BGSAVE job");
    assert_eq!(job.snapshot_offset, snapshot_offset);
    let mut expected_catchup = select9;
    expected_catchup.extend_from_slice(&zadd);
    assert_eq!(
        job.catch_up_bytes, expected_catchup,
        "active full-sync catch-up must include the selected-DB prefix before later writes"
    );

    let temp_path = job.temp_path.clone();
    let child_pid = job.child_pid;
    reap_repl_child_pid(child_pid);
    let outcome = repl.complete_repl_bgsave_transfer(job, b"RDB".to_vec());
    let _ = std::fs::remove_file(&temp_path);
    let _ = std::fs::remove_file(Path::new(&temp_path).with_extension("rdb.tmp"));

    assert_eq!(outcome.delivered_replicas, vec![client_id]);
    assert!(outcome.failed_replicas.is_empty());
    assert_eq!(outcome.retained_catchup_len, expected_catchup.len());
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        b"$3\r\nRDB".to_vec()
    );
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        expected_catchup
    );
    repl.remove_replica(client_id);
}

#[test]
fn in_flight_fullsync_waiter_reuses_existing_snapshot_offset() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);
    let server = Arc::new(RedisServer::default());

    let first_client_id = 1_100_021;
    let (first_tx, _first_rx) = mpsc::channel();
    let first_pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    first_pubsub
        .lock()
        .unwrap()
        .register_sender(first_client_id, first_tx);
    let mut first = Client::new(first_client_id);
    first.set_args(vec![
        RedisString::from_static(b"PSYNC"),
        RedisString::from_static(b"?"),
        RedisString::from_static(b"-1"),
    ]);
    let mut db = RedisDb::new(0);
    {
        let mut ctx = redis_core::CommandContext::with_server(
            &mut first,
            &mut db,
            Arc::clone(&server),
            first_pubsub,
        );
        redis_commands::replication::psync_command(&mut ctx).expect("first psync command");
    }
    let first_reply = first.drain_reply();
    let first_offset = fullresync_offset(&first_reply);

    let catchup = resp(&[b"SET", b"while-bgsave", b"1"]);
    repl.append_to_backlog(&catchup);
    assert!(
        repl.master_offset() > first_offset,
        "test setup should advance the master stream after the first full sync"
    );

    let second_client_id = 1_100_022;
    let (second_tx, _second_rx) = mpsc::channel();
    let second_pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    second_pubsub
        .lock()
        .unwrap()
        .register_sender(second_client_id, second_tx);
    let mut second = Client::new(second_client_id);
    second.set_args(vec![
        RedisString::from_static(b"PSYNC"),
        RedisString::from_static(b"?"),
        RedisString::from_static(b"-1"),
    ]);
    {
        let mut ctx = redis_core::CommandContext::with_server(
            &mut second,
            &mut db,
            Arc::clone(&server),
            second_pubsub,
        );
        redis_commands::replication::psync_command(&mut ctx).expect("second psync command");
    }
    let second_reply = second.drain_reply();
    assert_eq!(
        fullresync_offset(&second_reply),
        first_offset,
        "a waiter joining an in-flight full sync must be told the snapshot offset of the shared RDB"
    );

    let job = repl
        .take_repl_bgsave_job()
        .expect("in-flight full sync job should remain installed");
    assert_eq!(job.snapshot_offset, first_offset);
    assert_eq!(
        job.waiting_replicas,
        vec![first_client_id, second_client_id]
    );
    let temp_path = job.temp_path.clone();
    reap_repl_child_pid(job.child_pid);
    let _ = std::fs::remove_file(&temp_path);
    let _ = std::fs::remove_file(Path::new(&temp_path).with_extension("rdb.tmp"));
    repl.remove_replica(first_client_id);
    repl.remove_replica(second_client_id);
}

#[test]
fn config_set_backlog_size_expands_live_psync_window() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);

    let expanded = DEFAULT_REPL_BACKLOG_SIZE + 2048;
    let expanded_arg = expanded.to_string();
    let mut client = Client::new(1_100_007);
    let mut db = RedisDb::new(0);
    let reply = run_dispatch(
        &mut client,
        &mut db,
        &[
            b"CONFIG",
            b"SET",
            b"repl-backlog-size",
            expanded_arg.as_bytes(),
        ],
    );

    assert_eq!(reply, b"+OK\r\n");
    assert_eq!(repl.backlog_snapshot().3, expanded);

    let provided = repl.master_offset();
    let bytes = vec![b'z'; DEFAULT_REPL_BACKLOG_SIZE + 1024];
    repl.append_to_backlog(&bytes);
    assert!(
        repl.backlog_snapshot().0 <= provided,
        "expanded backlog should retain offset {provided}; snapshot {:?}",
        repl.backlog_snapshot()
    );

    let runid = runid_string(&repl);
    let drive = drive_psync(
        1_100_008,
        vec![b"PSYNC".to_vec(), runid, provided.to_string().into_bytes()],
    );

    assert!(
        drive.reply.starts_with(b"+CONTINUE"),
        "expanded live backlog should allow partial resync, got {:?}",
        String::from_utf8_lossy(&drive.reply)
    );
    assert_eq!(
        drive.sent.len(),
        bytes.len(),
        "+CONTINUE should replay the retained bytes"
    );
    assert_eq!(drive.counters_after.1, drive.counters_before.1 + 1);
    assert_eq!(drive.counters_after.0, drive.counters_before.0);
}

#[test]
fn idle_backlog_ttl_expiry_falls_back_to_fullresync_and_counts_partial_error() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);

    let provided = repl.master_offset();
    repl.append_to_backlog(&resp(&[b"SET", b"ttl-expiry", b"1"]));
    repl.backlog_last_replica_disconnect_ms
        .store(redis_core::util::mstime() - 2_000, Ordering::Relaxed);

    let server = Arc::new(RedisServer::default());
    server.live_config.set_repl_backlog_ttl(1);
    let runid = runid_string(&repl);
    let drive = drive_psync_with_server(
        1_100_009,
        vec![b"PSYNC".to_vec(), runid, provided.to_string().into_bytes()],
        server,
    );

    assert!(
        drive.reply.starts_with(b"+FULLRESYNC"),
        "idle backlog TTL expiry should force full resync, got {:?}",
        String::from_utf8_lossy(&drive.reply)
    );
    assert_eq!(drive.counters_after.0, drive.counters_before.0 + 1);
    assert_eq!(drive.counters_after.2, drive.counters_before.2 + 1);
}

#[test]
fn backlog_expired_reconnect_gets_fullresync_and_counts_partial_error() {
    let _g = psync_guard();
    let repl = global_replication_state();
    repl.become_master();
    cleanup_repl_bgsave(&repl);

    let expired_offset = repl.master_offset();
    let bytes = vec![b'x'; DEFAULT_REPL_BACKLOG_SIZE + 16];
    repl.append_to_backlog(&bytes);
    let runid = runid_string(&repl);
    let drive = drive_psync(
        1_100_002,
        vec![
            b"PSYNC".to_vec(),
            runid,
            expired_offset.to_string().into_bytes(),
        ],
    );

    assert!(
        drive.reply.starts_with(b"+FULLRESYNC"),
        "expired history should full-resync, got {:?}",
        String::from_utf8_lossy(&drive.reply)
    );
    assert_eq!(
        drive.counters_after.0,
        drive.counters_before.0 + 1,
        "sync_full should increment"
    );
    assert_eq!(
        drive.counters_after.2,
        drive.counters_before.2 + 1,
        "sync_partial_err should increment for concrete expired replid"
    );
}

#[test]
fn wrong_replid_future_offset_and_fresh_sync_have_distinct_metrics() {
    let _g = psync_guard();
    let repl = global_replication_state();
    repl.become_master();
    cleanup_repl_bgsave(&repl);
    repl.append_to_backlog(&resp(&[b"SET", b"metric-anchor", b"1"]));

    let master = repl.master_offset();
    let wrong = drive_psync(
        1_100_003,
        vec![
            b"PSYNC".to_vec(),
            vec![b'b'; 40],
            master.to_string().into_bytes(),
        ],
    );
    assert!(wrong.reply.starts_with(b"+FULLRESYNC"));
    assert_eq!(wrong.counters_after.0, wrong.counters_before.0 + 1);
    assert_eq!(wrong.counters_after.2, wrong.counters_before.2 + 1);

    let runid = runid_string(&repl);
    let future = drive_psync(
        1_100_004,
        vec![
            b"PSYNC".to_vec(),
            runid,
            (repl.master_offset() + 1).to_string().into_bytes(),
        ],
    );
    assert!(future.reply.starts_with(b"+FULLRESYNC"));
    assert_eq!(future.counters_after.0, future.counters_before.0 + 1);
    assert_eq!(future.counters_after.2, future.counters_before.2 + 1);

    let fresh = drive_psync(
        1_100_005,
        vec![b"PSYNC".to_vec(), b"?".to_vec(), b"-1".to_vec()],
    );
    assert!(fresh.reply.starts_with(b"+FULLRESYNC"));
    assert_eq!(fresh.counters_after.0, fresh.counters_before.0 + 1);
    assert_eq!(
        fresh.counters_after.2, fresh.counters_before.2,
        "fresh PSYNC ? -1 is a requested full sync, not a partial error"
    );
}

#[test]
fn malformed_psync_offset_errors_without_fullsync_side_effects() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);
    let counters_before = repl.sync_counters();
    let replicas_before = repl.connected_replicas();

    let mut client = Client::new(1_100_010);
    let mut db = RedisDb::new(0);
    let reply = run_dispatch(
        &mut client,
        &mut db,
        &[b"PSYNC", b"replicationid", b"offset_str"],
    );

    assert!(
        reply
            .windows(b"ERR value is not an integer or out of range".len())
            .any(|w| w == b"ERR value is not an integer or out of range"),
        "malformed PSYNC offset should return the parser error, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    assert!(!client.is_replica);
    assert_eq!(repl.sync_counters(), counters_before);
    assert_eq!(repl.connected_replicas(), replicas_before);
}

#[test]
fn syncing_replica_refuses_downstream_psync_until_upstream_connected() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);
    repl.become_replica_of(arg(b"127.0.0.1"), 6379);
    repl.set_replica_link(replica_link_code::TRANSFER);
    let counters_before = repl.sync_counters();
    let replicas_before = repl.connected_replicas();

    let mut client = Client::new(1_100_011);
    let mut db = RedisDb::new(0);
    let reply = run_dispatch(&mut client, &mut db, &[b"PSYNC", b"?", b"-1"]);

    assert!(
        reply.starts_with(b"-NOMASTERLINK Can't SYNC while not connected with my master"),
        "syncing replica should refuse chained PSYNC, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    assert!(!client.is_replica);
    assert_eq!(repl.sync_counters(), counters_before);
    assert_eq!(repl.connected_replicas(), replicas_before);
}

#[test]
fn connected_replica_can_serve_chained_psync() {
    let _g = psync_guard();
    let repl = global_replication_state();
    let _reset = GlobalReplReset::new(&repl);
    repl.become_replica_of(arg(b"127.0.0.1"), 6379);
    repl.set_replica_link(replica_link_code::CONNECTED);
    repl.append_to_backlog(&resp(&[b"SET", b"from-upstream", b"1"]));

    let reply = drive_psync(
        1_100_012,
        vec![
            b"PSYNC".to_vec(),
            runid_string(&repl),
            repl.master_offset().to_string().into_bytes(),
        ],
    );

    assert!(
        reply.reply.starts_with(b"+CONTINUE"),
        "connected chained replica should serve PSYNC, got {:?}",
        String::from_utf8_lossy(&reply.reply)
    );
    assert_eq!(reply.counters_after.1, reply.counters_before.1 + 1);
}

#[test]
fn target_detour_preserves_cached_reconnect_state_until_fullsync_adopts_new_primary() {
    let st = ReplicationState::new([b'c'; 40], 64);
    st.become_replica_of(arg(b"127.0.0.1"), 6379);

    let cached = [b'd'; 40];
    let adopted = [b'e'; 40];
    st.set_cached_primary_replid(cached);
    st.append_to_backlog(&resp(&[b"SET", b"old-primary", b"1"]));
    let preserved_offset = st.master_offset();

    st.become_replica_of(arg(b"127.0.0.1"), 6379);
    assert_eq!(st.cached_primary_replid(), Some(cached));
    assert_eq!(st.master_offset(), preserved_offset);

    st.become_replica_of(arg(b"127.0.0.2"), 6379);
    assert_eq!(st.cached_primary_replid(), Some(cached));
    assert_eq!(st.master_offset(), preserved_offset);

    st.become_replica_of(arg(b"127.0.0.1"), 6379);
    assert_eq!(st.cached_primary_replid(), Some(cached));
    assert_eq!(
        st.master_offset(),
        preserved_offset,
        "a temporary REPLICAOF detour must not destroy the original master's PSYNC cache"
    );

    st.adopt_fullresync_primary(adopted, 9000);
    assert_eq!(st.cached_primary_replid(), Some(adopted));
    assert_eq!(st.master_offset(), 9000);
    assert_eq!(
        st.backlog_snapshot().2,
        0,
        "adopting a new FULLRESYNC stream must discard old backlog bytes"
    );
}

#[test]
fn promoted_master_reconfigured_as_replica_drops_cached_reconnect_state() {
    let st = ReplicationState::new([b'c'; 40], 64);
    st.become_replica_of(arg(b"127.0.0.1"), 6379);

    let cached = [b'd'; 40];
    st.set_cached_primary_replid(cached);
    st.set_zero_offset_partial_resync_allowed(true);
    st.append_to_backlog(&resp(&[b"XADD", b"k", b"*", b"foo", b"bar"]));

    st.become_master();
    assert_eq!(
        st.cached_primary_replid(),
        Some(cached),
        "promotion preserves history until the operator chooses a new replica relationship"
    );

    st.become_replica_of(arg(b"127.0.0.1"), 6379);

    assert!(
        st.cached_primary_replid().is_none(),
        "a promoted/standalone master reconfigured with REPLICAOF must start with PSYNC ? -1"
    );
    assert!(
        !st.zero_offset_partial_resync_allowed(),
        "empty-RDB zero-offset permission belongs to the old upstream relationship"
    );
}

#[test]
fn client_kill_primary_addr_requests_replica_dialer_reconnect() {
    let _g = psync_guard();
    let repl = global_replication_state();
    repl.become_master();
    repl.become_replica_of(arg(b"127.0.0.1"), 6399);
    assert!(!repl.take_replica_link_drop_request());

    let mut client = Client::new(1_100_006);
    client.addr = Some("127.0.0.1:55000".to_string());
    let mut db = RedisDb::new(0);
    let reply = run_dispatch(
        &mut client,
        &mut db,
        &[b"CLIENT", b"KILL", b"127.0.0.1:6399"],
    );

    assert_eq!(reply, b"+OK\r\n");
    assert!(
        repl.take_replica_link_drop_request(),
        "CLIENT KILL primary address should ask the dialer to reconnect"
    );

    let reply = run_dispatch(
        &mut client,
        &mut db,
        &[b"CLIENT", b"KILL", b"127.0.0.1:6400"],
    );
    assert!(
        String::from_utf8_lossy(&reply).contains("ERR No such client"),
        "unmatched old-style CLIENT KILL should still report no client, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    assert!(!repl.take_replica_link_drop_request());
    repl.become_master();
}

// PORT STATUS
//   source:        Valkey integration/replication-psync behavior
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Deterministic PSYNC reconnect kit for cached primary,
//                  backlog, retained history, target detours, and counters.
