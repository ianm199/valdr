//! Deterministic inner loop for replication-buffer lifetime semantics.
//!
//! This kit targets the `integration/replication-buffer` frontier without
//! sockets or Tcl timing. It exercises the shared history that sits outside the
//! configured circular backlog while full-sync replicas are still consuming
//! catch-up bytes.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver};

use redis_core::metrics::server_metrics;
use redis_core::replication::{
    generate_runid, ReplBgsaveJob, ReplicaConn, ReplicaState, ReplicationState,
};
use redis_core::ClientId;

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
fn retained_history_does_not_overclaim_after_backlog_wrap_creates_gap() {
    let st = ReplicationState::new(generate_runid(), 4);
    let _rx = attach_replica(&st, 61, 0);

    st.append_to_backlog(b"abcdef");
    st.retain_fullsync_history(0, b"abcdef".to_vec(), &[61]);

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
