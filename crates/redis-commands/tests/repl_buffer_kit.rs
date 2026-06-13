//! Deterministic inner loop for replication-buffer lifetime semantics.
//!
//! This kit targets the `integration/replication-buffer` frontier without
//! sockets or Tcl timing. It exercises the shared history that sits outside the
//! configured circular backlog while full-sync replicas are still consuming
//! catch-up bytes.

use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};

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
