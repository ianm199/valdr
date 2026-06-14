//! Deterministic inner loop for replication-buffer lifetime semantics.
//!
//! This kit targets the `integration/replication-buffer` frontier without
//! sockets or Tcl timing. It exercises the shared history that sits outside the
//! configured circular backlog while full-sync replicas are still consuming
//! catch-up bytes.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use redis_commands::config_cmd::{
    apply_config_set, config_value_for_key, validate_config_set_pair,
};
use redis_commands::dispatch::propagate_command_raw;
use redis_core::client_info::client_info_registry;
use redis_core::live_config::LiveConfig;
use redis_core::metrics::server_metrics;
use redis_core::replication::{
    generate_runid, global_replication_state, ReplBgsaveJob, ReplicaConn, ReplicaState,
    ReplicationState,
};
use redis_core::ClientId;
use redis_types::RedisString;

fn global_repl_guard() -> MutexGuard<'static, ()> {
    static REPL_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    match REPL_GUARD.get_or_init(|| Mutex::new(())).lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn argv(parts: &[&[u8]]) -> Vec<RedisString> {
    parts
        .iter()
        .map(|part| RedisString::from_bytes(part))
        .collect()
}

fn attach_replica(st: &ReplicationState, client_id: ClientId, offset: i64) -> Receiver<Vec<u8>> {
    let (tx, rx) = mpsc::channel();
    st.add_replica(ReplicaConn::new(
        client_id,
        ReplicaState::Online,
        offset,
        tx,
    ));
    rx
}

fn install_job(st: &ReplicationState, waiters: Vec<ClientId>, snapshot_offset: i64) {
    st.install_repl_bgsave_job(ReplBgsaveJob {
        child_pid: 1,
        temp_path: PathBuf::from("temp-repl-buffer-kit.rdb"),
        waiting_replicas: waiters,
        snapshot_offset,
        catch_up_bytes: Vec::new(),
        needs_getack_on_completion: false,
    });
}

#[test]
fn killed_replica_is_not_a_live_fanout_target_during_async_teardown() {
    let _guard = global_repl_guard();
    let repl = global_replication_state();
    repl.become_master();

    let client_id = 1_990_001;
    repl.remove_replica(client_id);
    {
        let mut guard = client_info_registry().lock().unwrap();
        guard.deregister(client_id);
        guard.register(client_id, "127.0.0.1:0".to_string());
        guard.mark_killed(client_id);
    }

    let (tx, rx) = mpsc::channel();
    repl.add_replica(ReplicaConn::new(
        client_id,
        ReplicaState::Online,
        repl.master_offset(),
        tx,
    ));

    let before = repl.master_offset();
    let offset = propagate_command_raw(&argv(&[b"SET", b"killed-replica", b"v"]));
    assert!(
        offset > before,
        "the write still belongs in replication history for future PSYNC"
    );
    assert!(
        rx.try_recv().is_err(),
        "CLIENT KILL removes the client snapshot immediately, so same-transaction fan-out must not keep writing to that replica id"
    );

    repl.remove_replica(client_id);
    client_info_registry().lock().unwrap().deregister(client_id);
}

#[test]
fn send_bulk_replica_receives_new_stream_writes_after_rdb_is_queued() {
    let _guard = global_repl_guard();
    let repl = global_replication_state();
    repl.become_master();

    let client_id = 1_990_002;
    repl.remove_replica(client_id);
    client_info_registry().lock().unwrap().deregister(client_id);

    let (tx, rx) = mpsc::channel();
    repl.add_replica(ReplicaConn::new(
        client_id,
        ReplicaState::SendingRdb,
        repl.master_offset(),
        tx,
    ));

    let cmd = argv(&[b"SET", b"send-bulk-replica", b"v"]);
    let expected = redis_commands::aof::encode_resp_command(&cmd);
    let before = repl.master_offset();
    let offset = propagate_command_raw(&cmd);

    assert_eq!(offset, before + expected.len() as i64);
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(250))
            .expect("send_bulk replica should receive command stream bytes"),
        expected
    );

    repl.remove_replica(client_id);
    client_info_registry().lock().unwrap().deregister(client_id);
}

#[test]
fn retained_fullsync_history_extends_partial_resync_after_job_completion() {
    let st = ReplicationState::new(generate_runid(), 4);
    let _rx = attach_replica(&st, 41, 0);
    install_job(&st, vec![41], 0);

    st.append_to_backlog(b"abcdef");
    let job = st.take_repl_bgsave_job().expect("active full-sync job");
    assert!(
        st.read_history_at(0, 6).is_none(),
        "before retention, taking the job exposes only the circular backlog"
    );

    st.retain_fullsync_history(
        job.snapshot_offset,
        job.catch_up_bytes,
        &job.waiting_replicas,
    );
    assert_eq!(st.backlog_snapshot(), (0, 6, 6, 4));
    assert_eq!(
        st.read_history_at(0, 6).as_deref(),
        Some(b"abcdef".as_slice())
    );

    st.append_to_backlog(b"ghij");
    assert_eq!(st.backlog_snapshot(), (0, 10, 10, 4));
    assert_eq!(
        st.read_history_at(0, 10).as_deref(),
        Some(b"abcdefghij".as_slice())
    );
    assert!(
        st.can_read_history_range(0, 10),
        "PSYNC should be allowed when retained history and backlog are contiguous"
    );
}

#[test]
fn retained_fullsync_history_keeps_growing_for_send_bulk_owner() {
    let st = ReplicationState::new(generate_runid(), 4);
    let _rx = attach_replica(&st, 42, 0);

    st.append_to_backlog(b"abcdef");
    st.retain_fullsync_history(0, b"abcdef".to_vec(), &[42]);
    assert_eq!(st.backlog_snapshot(), (0, 6, 6, 4));

    st.append_to_backlog(b"ghij");
    st.append_to_backlog(b"klmn");

    assert_eq!(
        st.backlog_snapshot(),
        (0, 14, 14, 4),
        "a send_bulk owner should keep the shared full-sync stream readable beyond the circular backlog"
    );
    assert_eq!(
        st.read_history_at(0, 14).as_deref(),
        Some(b"abcdefghijklmn".as_slice())
    );

    st.remove_replica(42);
    assert_eq!(
        st.backlog_snapshot(),
        (10, 14, 4, 4),
        "disconnecting the last owner releases the grown shared history"
    );
    assert!(!st.can_read_history_range(0, 14));
}

#[test]
fn retained_history_is_counted_once_and_released_by_ack_or_disconnect() {
    let st = ReplicationState::new(generate_runid(), 4);
    let _rx1 = attach_replica(&st, 51, 0);
    let _rx2 = attach_replica(&st, 52, 0);

    st.append_to_backlog(b"abcdef");
    st.retain_fullsync_history(0, b"abcdef".to_vec(), &[52, 51, 51]);
    assert_eq!(
        st.retained_repl_history_len(),
        6,
        "shared retained bytes must be counted once, not once per owner"
    );
    {
        let guard = st.retained_history.lock().unwrap();
        assert_eq!(guard[0].owners, vec![51, 52]);
    }

    st.release_retained_history_ack(51, 5);
    assert_eq!(
        st.retained_repl_history_len(),
        6,
        "ACK before the segment end must not release the owner"
    );

    st.release_retained_history_ack(51, 6);
    assert_eq!(
        st.retained_repl_history_len(),
        6,
        "one remaining owner still pins the shared segment"
    );
    {
        let guard = st.retained_history.lock().unwrap();
        assert_eq!(guard[0].owners, vec![52]);
    }

    st.remove_replica(52);
    assert_eq!(st.retained_repl_history_len(), 0);
    assert!(
        st.read_history_at(0, 1).is_none(),
        "after the last owner disconnects, old retained bytes must disappear"
    );
}

#[test]
fn active_fullsync_catchup_releases_when_last_waiter_disconnects() {
    let st = ReplicationState::new(generate_runid(), 4);
    let _rx1 = attach_replica(&st, 91, 0);
    let _rx2 = attach_replica(&st, 92, 0);
    install_job(&st, vec![91, 92], 0);

    st.append_to_backlog(b"abcdef");
    assert_eq!(st.replication_history_extra_len(), 6);
    assert_eq!(
        st.read_history_at(0, 6).as_deref(),
        Some(b"abcdef".as_slice())
    );

    let first = st.remove_replica(91);
    assert!(first.was_repl_bgsave_waiter);
    assert_eq!(first.remaining_repl_bgsave_waiters, 1);
    assert_eq!(st.replication_history_extra_len(), 6);
    assert_eq!(
        st.read_history_at(0, 6).as_deref(),
        Some(b"abcdef".as_slice()),
        "one remaining waiter still pins active catch-up history"
    );

    let last = st.remove_replica(92);
    assert!(last.was_repl_bgsave_waiter);
    assert_eq!(last.remaining_repl_bgsave_waiters, 0);
    assert_eq!(last.useless_repl_child_pid, Some(1));
    assert_eq!(
        st.replication_history_extra_len(),
        0,
        "no replica can use the active catch-up buffer after the last waiter disconnects"
    );
    assert!(
        st.read_history_at(0, 6).is_none(),
        "only the circular backlog remains after active catch-up release"
    );
    let job = st
        .take_repl_bgsave_job()
        .expect("job remains for reaper cleanup");
    assert!(job.waiting_replicas.is_empty());
    assert!(job.catch_up_bytes.is_empty());
}

#[test]
fn active_fullsync_catchup_extends_readable_history_beyond_backlog() {
    let st = ReplicationState::new(generate_runid(), 4);
    let _rx = attach_replica(&st, 93, 0);
    install_job(&st, vec![93], 0);

    st.append_to_backlog(b"abcdefghijkl");
    assert_eq!(
        st.backlog_snapshot(),
        (0, 12, 12, 4),
        "active full-sync catch-up should make INFO-style history outgrow the circular backlog"
    );
    assert_eq!(
        st.read_history_at(0, 12).as_deref(),
        Some(b"abcdefghijkl".as_slice())
    );
    assert!(
        st.can_read_history_range(0, 12),
        "PSYNC should be allowed across active catch-up bytes beyond repl-backlog-size"
    );

    let removed = st.remove_replica(93);
    assert!(removed.was_repl_bgsave_waiter);
    assert_eq!(removed.remaining_repl_bgsave_waiters, 0);
    assert_eq!(
        st.backlog_snapshot(),
        (8, 12, 4, 4),
        "disconnecting the last full-sync waiter should shrink history back to the circular backlog"
    );
    assert!(st.read_history_at(0, 1).is_none());
    assert!(!st.can_read_history_range(0, 12));
}

#[test]
fn online_replica_reconnect_can_consume_active_history_pinned_by_other_waiter() {
    let st = ReplicationState::new(generate_runid(), 4);
    let online_rx = attach_replica(&st, 94, 0);
    let _waiter_rx = attach_replica(&st, 95, 0);
    install_job(&st, vec![95], 0);

    st.append_to_backlog(b"abcdef");
    st.remove_replica(94);
    st.append_to_backlog(b"ghijkl");

    assert_eq!(
        st.backlog_snapshot(),
        (0, 12, 12, 4),
        "the waiting full-sync replica should pin all catch-up bytes for PSYNC"
    );
    assert!(st.can_read_history_range(0, st.master_offset()));
    assert_eq!(
        st.read_history_at(0, st.master_offset() as usize)
            .as_deref(),
        Some(b"abcdefghijkl".as_slice())
    );

    let reconnected_rx = attach_replica(&st, 96, 0);
    let catch_up = st
        .read_history_at(0, st.master_offset() as usize)
        .expect("active catch-up should satisfy the reconnect");
    assert!(st.send_to_replica(96, catch_up));
    assert_eq!(reconnected_rx.recv().unwrap(), b"abcdefghijkl".to_vec());
    assert!(
        online_rx.try_recv().is_err(),
        "the old disconnected replica channel must not receive reconnect catch-up"
    );
    assert_eq!(
        st.repl_bgsave_catchup_len(),
        12,
        "the full-sync waiter still pins active history after the online reconnect"
    );
}

#[test]
fn dual_channel_memory_accounting_excludes_active_fullsync_catchup() {
    let st = ReplicationState::new(generate_runid(), 4);
    let _rx = attach_replica(&st, 101, 0);
    install_job(&st, vec![101], 0);

    st.append_to_backlog(b"abcdef");
    assert_eq!(st.repl_bgsave_catchup_len(), 6);
    assert_eq!(st.replication_history_extra_len(), 6);
    assert_eq!(
        st.replication_history_extra_len_for_memory(false),
        6,
        "single-channel accounting charges active catch-up to replication memory"
    );
    assert_eq!(
        st.replication_history_extra_len_for_memory(true),
        0,
        "dual-channel accounting must not inflate the normal replication buffer while RDB sync is active"
    );

    let job = st.take_repl_bgsave_job().expect("active full-sync job");
    st.retain_fullsync_history(
        job.snapshot_offset,
        job.catch_up_bytes,
        &job.waiting_replicas,
    );
    assert_eq!(st.retained_repl_history_len(), 6);
    assert_eq!(
        st.replication_history_extra_len_for_memory(true),
        6,
        "post-transfer retained history still counts because it can satisfy PSYNC"
    );
}

#[test]
fn dual_channel_replication_config_is_live() {
    let cfg = Arc::new(LiveConfig::new());

    assert!(cfg.dual_channel_replication_enabled());
    assert_eq!(
        config_value_for_key(&cfg, b"dual-channel-replication-enabled").as_deref(),
        Some("yes")
    );

    validate_config_set_pair(b"dual-channel-replication-enabled", b"no")
        .expect("valid yes/no value");
    apply_config_set(&cfg, b"dual-channel-replication-enabled", b"no");
    assert!(!cfg.dual_channel_replication_enabled());
    assert_eq!(
        config_value_for_key(&cfg, b"dual-channel-replication-enabled").as_deref(),
        Some("no")
    );

    apply_config_set(&cfg, b"dual-channel-replication-enabled", b"YeS");
    assert!(cfg.dual_channel_replication_enabled());
    assert!(
        validate_config_set_pair(b"dual-channel-replication-enabled", b"maybe").is_err(),
        "CONFIG SET should reject non yes/no dual-channel values"
    );
}

#[test]
fn retained_history_does_not_overclaim_after_backlog_wrap_creates_gap() {
    let st = ReplicationState::new(generate_runid(), 4);
    let _rx = attach_replica(&st, 61, 0);

    st.append_to_backlog(b"abcdef");
    st.retain_fullsync_history(0, b"abcdef".to_vec(), &[61]);
    {
        let mut guard = st.retained_history.lock().unwrap();
        guard[0].open = false;
    }

    st.append_to_backlog(b"ghij");
    assert_eq!(
        st.read_history_at(0, 10).as_deref(),
        Some(b"abcdefghij".as_slice())
    );

    st.append_to_backlog(b"klmn");
    assert_eq!(
        st.backlog_snapshot(),
        (10, 14, 4, 4),
        "INFO-style history must report the contiguous readable range, not a stale retained island"
    );
    assert!(
        !st.can_read_history_range(0, 14),
        "PSYNC must not be allowed across a gap between retained bytes and current backlog"
    );
    assert_eq!(
        st.read_history_at(0, 6).as_deref(),
        Some(b"abcdef".as_slice()),
        "the retained island is still directly readable for its own range"
    );
}

#[test]
fn shared_replica_output_memory_is_counted_once_and_drains_by_slowest_replica() {
    let st = ReplicationState::new(generate_runid(), 64);
    let _rx1 = attach_replica(&st, 81, 0);
    let _rx2 = attach_replica(&st, 82, 0);

    assert!(st.send_to_replica(81, b"shared-block".to_vec()));
    assert!(st.send_to_replica(82, b"shared-block".to_vec()));

    let mem = st.replica_output_memory_snapshot();
    assert_eq!(mem.shared_output_bytes, 12);
    assert_eq!(mem.private_output_bytes, 0);
    assert_eq!(
        mem.total_output_bytes, 24,
        "CLIENT-style output memory remains per replica"
    );
    assert_eq!(
        mem.replication_buffer_bytes(),
        12,
        "shared replication-stream bytes are one logical buffer"
    );

    assert_eq!(st.account_replica_output_drained(81, 12), 0);
    let mem = st.replica_output_memory_snapshot();
    assert_eq!(
        mem.shared_output_bytes, 12,
        "one slow replica still pins the shared stream bytes"
    );
    assert_eq!(mem.total_output_bytes, 12);

    assert_eq!(st.account_replica_output_drained(82, 12), 0);
    let mem = st.replica_output_memory_snapshot();
    assert_eq!(mem.shared_output_bytes, 0);
    assert_eq!(mem.total_output_bytes, 0);

    assert!(st.send_private_to_replica(81, b"private-a".to_vec()));
    assert!(st.send_private_to_replica(82, b"private-bb".to_vec()));
    let mem = st.replica_output_memory_snapshot();
    assert_eq!(mem.shared_output_bytes, 0);
    assert_eq!(mem.private_output_bytes, 19);
    assert_eq!(mem.total_output_bytes, 19);
    assert_eq!(
        mem.replication_buffer_bytes(),
        19,
        "private replica output is still counted per waiting replica"
    );
}

#[test]
fn shared_stream_can_exceed_hard_limit_but_private_output_disconnects_offender() {
    let st = ReplicationState::new(generate_runid(), 64);
    st.set_replica_output_buffer_hard_limit(10);
    let slow_rx = attach_replica(&st, 71, 0);
    let healthy_rx = attach_replica(&st, 72, 0);
    let before_disconnects = server_metrics()
        .client_output_buffer_limit_disconnections
        .load(Ordering::Relaxed);

    assert!(
        st.send_to_replica(71, b"abcdefghijkl".to_vec()),
        "shared replication history may exceed the private hard limit"
    );
    assert!(st.send_to_replica(72, b"ABCDEFGH".to_vec()));
    assert_eq!(st.connected_replicas(), 2);
    {
        let guard = st.replicas.lock().unwrap();
        assert_eq!(guard[&71].pending_output_bytes.load(Ordering::Relaxed), 12);
        assert_eq!(guard[&72].pending_output_bytes.load(Ordering::Relaxed), 8);
    }
    assert_eq!(
        st.account_replica_output_drained(71, 12),
        0,
        "writer-side drain should clear reported replica output memory"
    );

    assert!(st.send_private_to_replica(71, b"abcdefgh".to_vec()));
    assert!(
        !st.send_private_to_replica(71, b"ijkl".to_vec()),
        "private queued output crossing the hard limit should disconnect the offender"
    );
    assert_eq!(st.connected_replicas(), 1);
    {
        let guard = st.replicas.lock().unwrap();
        assert!(!guard.contains_key(&71));
        assert!(guard.contains_key(&72));
        assert_eq!(guard[&72].pending_output_bytes.load(Ordering::Relaxed), 8);
    }
    assert_eq!(
        server_metrics()
            .client_output_buffer_limit_disconnections
            .load(Ordering::Relaxed),
        before_disconnects + 1
    );

    assert!(st.send_to_replica(72, b"Z".to_vec()));
    assert_eq!(healthy_rx.recv().unwrap(), b"ABCDEFGH".to_vec());
    assert_eq!(healthy_rx.recv().unwrap(), b"Z".to_vec());
    assert_eq!(slow_rx.recv().unwrap(), b"abcdefghijkl".to_vec());
    assert_eq!(slow_rx.recv().unwrap(), b"abcdefgh".to_vec());
    assert_eq!(slow_rx.recv().unwrap(), b"ijkl".to_vec());
}
