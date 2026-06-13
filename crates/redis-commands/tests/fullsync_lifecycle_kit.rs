//! Deterministic inner loop for full-sync lifecycle cleanup.
//!
//! The slow Tcl `integration/replication` frontier includes child-failure and
//! killed-child cases. This kit pins the Rust state transition that must hold
//! before those socket-level cases can be made reliable: a failed replication
//! BGSAVE must not leave stale waiters or temp files behind.

use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::replication::{
    generate_runid, ReplBgsaveJob, ReplicaConn, ReplicaState, ReplicationState,
};
use redis_core::ClientId;

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
