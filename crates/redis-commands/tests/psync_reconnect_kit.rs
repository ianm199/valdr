//! Deterministic inner loop for PSYNC reconnect decisions.
//!
//! This kit drives the real `psync_command` entrypoint where possible, then
//! uses local `ReplicationState` values for replica-side reconnect cache cases
//! that should not depend on the process-global replication state.

use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use redis_core::replication::{
    global_replication_state, ReplicationState, DEFAULT_REPL_BACKLOG_SIZE,
};
use redis_core::{Client, PubSubRegistry, RedisDb, RedisServer};
use redis_types::RedisString;

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

struct PsyncDrive {
    reply: Vec<u8>,
    sent: Vec<u8>,
    counters_before: (u64, u64, u64),
    counters_after: (u64, u64, u64),
}

fn drive_psync(client_id: u64, args: Vec<Vec<u8>>) -> PsyncDrive {
    let repl = global_replication_state();
    let counters_before = repl.sync_counters();
    let (tx, rx) = mpsc::channel();
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    pubsub.lock().unwrap().register_sender(client_id, tx);

    let mut c = Client::new(client_id);
    c.set_args(args.into_iter().map(RedisString::from_vec).collect());
    let mut db = RedisDb::new(0);
    let server = Arc::new(RedisServer::default());
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

fn cleanup_repl_bgsave(repl: &Arc<ReplicationState>) {
    if let Some(job) = repl.take_repl_bgsave_job() {
        let _ = std::fs::remove_file(&job.temp_path);
        let _ = std::fs::remove_file(Path::new(&job.temp_path).with_extension("rdb.tmp"));
    }
    let pid = repl.repl_child_pid();
    if pid > 0 {
        #[cfg(unix)]
        unsafe {
            let _ = libc::kill(pid as libc::pid_t, libc::SIGTERM);
            let mut status: libc::c_int = 0;
            let _ = libc::waitpid(pid as libc::pid_t, &mut status, 0);
        }
    }
    repl.set_repl_child_pid(0);
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
fn target_change_clears_cached_reconnect_state_but_same_target_preserves_it() {
    let st = ReplicationState::new([b'c'; 40], 64);
    st.become_replica_of(arg(b"127.0.0.1"), 6379);

    let cached = [b'd'; 40];
    st.set_cached_primary_replid(cached);
    st.master_repl_offset
        .store(1234, std::sync::atomic::Ordering::Relaxed);

    st.become_replica_of(arg(b"127.0.0.1"), 6379);
    assert_eq!(st.cached_primary_replid(), Some(cached));
    assert_eq!(st.master_offset(), 1234);

    st.become_replica_of(arg(b"127.0.0.2"), 6379);
    assert_eq!(st.cached_primary_replid(), None);
    assert_eq!(st.master_offset(), 0);
}
