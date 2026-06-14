//! Replica-side connection state machine — Wave C.
//! When `REPLICAOF host port` is issued, `spawn_replica_dialer` launches a
//! dedicated background thread that:
//! 1. Connects to the master's TCP port.
//! 2. Runs the PING / REPLCONF / PSYNC handshake.
//! 3. Reads the `$<size>\r\n<rdb-bytes>` full-resync payload.
//! 4. Drains the full-resync RDB payload. RuntimeOwner-backed loading is a
//! separate follow-up; the current frontier starts from empty replicas.
//! 5. Enters the command-apply loop: reads one RESP frame per iteration,
//! applies it through the RuntimeOwner-owned DB queue, and responds
//! `REPLCONF GETACK *` with `REPLCONF ACK <offset>`.
//! 6. On any I/O error or EOF: sleeps briefly and restarts from step 1.
//! 7. If `ReplicationState::dialer_stop_flag` is set (by `REPLICAOF NO ONE`),
//! exits immediately.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use redis_core::live_config::{ReplDisklessLoadMode, DEFAULT_REPL_TIMEOUT};
use redis_core::replication::{
    failover_state_code, global_replication_state, repl_state_code, replica_link_code,
    ReplicationState,
};
use redis_core::server::RedisServer;
use redis_core::util::mstime;
use redis_types::RedisString;

static GLOBAL_SERVER: OnceLock<Arc<RedisServer>> = OnceLock::new();
static GLOBAL_OUR_PORT: OnceLock<u16> = OnceLock::new();
static GLOBAL_RDB_DIR: OnceLock<String> = OnceLock::new();
static RUNTIME_APPLY_TX: OnceLock<Sender<ReplicaApplyRequest>> = OnceLock::new();
const RUNTIME_APPLY_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICA_STREAM_READ_BUFFER_SIZE: usize = 4 * 1024 * 1024;
const HANDSHAKE_TIMEOUT_LOG: &str = "redis-server: Timeout connecting to the PRIMARY";

/// Work the dialer thread hands to the runtime owner loop because the owner —
/// not the dialer — owns the live DB slice.
/// `Command` is an ordinary replicated write parsed off the primary stream.
/// `LoadRdb` is the full-resync snapshot the primary streams after `FULLRESYNC`;
/// it must be loaded into the owned databases, replacing their contents. Routing
/// both through the same queue keeps every keyspace mutation on the owner's
/// ownership boundary.
pub enum ReplicaApplyKind {
    Command(Vec<RedisString>),
    CommandBatch(Vec<Vec<RedisString>>),
    LoadRdb(Vec<u8>),
}

/// A unit of replica-side work for the runtime owner loop, with the post-apply
/// master offset and a completion channel the dialer blocks on.
pub struct ReplicaApplyRequest {
    pub kind: ReplicaApplyKind,
    pub offset_after: i64,
    pub done: Sender<bool>,
}

/// Register the shared resources the dialer thread needs.
/// Called once from the binary's main before any `REPLICAOF` command can be
/// issued. Subsequent calls are no-ops (OnceLock semantics).
pub fn install_dialer_resources(server: Arc<RedisServer>, our_port: u16, rdb_dir: String) {
    let _ = GLOBAL_SERVER.set(server);
    let _ = GLOBAL_OUR_PORT.set(our_port);
    let _ = GLOBAL_RDB_DIR.set(rdb_dir);
}

/// Install the RuntimeOwner-owned DB apply queue used by the replica dialer.
/// The dialer owns the master TCP stream and parses replication frames, but it
/// cannot mutate the live keyspace directly because RuntimeOwner owns the DB
/// slice. Applying through this queue keeps replica writes on the same thread
/// and ownership boundary as ordinary client writes.
pub fn install_runtime_apply_sender(tx: Sender<ReplicaApplyRequest>) {
    let _ = RUNTIME_APPLY_TX.set(tx);
}

/// Spawn a background dialer thread that implements the full replica state
/// machine described in the module doc.
/// The function returns immediately; the spawned thread runs until
/// `ReplicationState::dialer_stop_flag` is set to `true`. Returns an error
/// when the dialer resources have not been installed.
pub fn spawn_replica_dialer(
    host: RedisString,
    port: u16,
    dialer_epoch: u64,
) -> Result<(), &'static str> {
    let _ = GLOBAL_SERVER
        .get()
        .ok_or("dialer resources not installed")?;
    let our_port = *GLOBAL_OUR_PORT
        .get()
        .ok_or("dialer resources not installed")?;
    let _ = GLOBAL_RDB_DIR
        .get()
        .ok_or("dialer resources not installed")?;

    let host_for_thread = host.clone();
    thread::Builder::new()
        .name("replica-dialer".to_string())
        .spawn(move || handshake_sink_loop(host_for_thread, port, our_port, dialer_epoch))
        .map(|_| ())
        .map_err(|_| "replica dialer thread spawn failed")
}

/// RuntimeOwner-compatible replica dialer.
/// This performs the real TCP handshake so primaries observe an attached
/// `flags=S` replica and stream bytes to it. Streamed write commands are routed
/// back into RuntimeOwner so they mutate the live owner-owned DB list.
fn handshake_sink_loop(host: RedisString, port: u16, our_port: u16, dialer_epoch: u64) {
    let repl = global_replication_state();
    loop {
        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return;
        }
        if !wait_for_replica_dialer_pause(&repl, dialer_epoch) {
            return;
        }

        repl.set_replica_link(replica_link_code::CONNECTING);
        let host_str = String::from_utf8_lossy(host.as_bytes()).to_string();
        let stream = match TcpStream::connect(format!("{}:{}", host_str, port)) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "redis-server: replica: connect {}:{} failed: {}",
                    host_str, port, e
                );
                repl.set_replica_link(replica_link_code::CONNECT);
                thread::sleep(Duration::from_millis(200));
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let _ = set_handshake_read_timeout(&stream, configured_handshake_timeout());

        repl.set_replica_link(replica_link_code::HANDSHAKE);
        let outcome = match run_handshake(&stream, &repl, our_port) {
            Ok(o) => o,
            Err(e) => {
                log_handshake_failure(&e);
                repl.set_replica_link(replica_link_code::CONNECT);
                thread::sleep(Duration::from_millis(200));
                continue;
            }
        };

        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return;
        }

        let (online_offset, defer_fullsync_ack_until_idle, defer_partial_online_until_idle) =
            match outcome {
                PsyncOutcome::FullResync { offset, replid } => {
                    let async_loading = repl.cached_primary_replid().is_some_and(|id| id == replid);
                    repl.set_replica_link(replica_link_code::TRANSFER);
                    publish_fullsync_loading_state(async_loading);
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
                    let rdb_bytes = match read_fullresync_rdb(&stream, &repl, dialer_epoch) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            if is_handshake_timeout_error(&e) {
                                log_handshake_failure(&e);
                            } else {
                                eprintln!("redis-server: replica: RDB sink failed: {}", e);
                            }
                            clear_fullsync_loading_state();
                            if !repl.dialer_epoch_is_current(dialer_epoch) {
                                return;
                            }
                            repl.repl_state
                                .store(repl_state_code::REPLICA_CONNECTING, Ordering::SeqCst);
                            repl.set_replica_link(replica_link_code::CONNECT);
                            thread::sleep(Duration::from_millis(200));
                            continue;
                        }
                    };

                    if !repl.dialer_epoch_is_current(dialer_epoch) {
                        clear_fullsync_loading_state();
                        return;
                    }
                    if !load_rdb_via_runtime_owner(rdb_bytes, offset) {
                        eprintln!("redis-server: replica: RDB load failed");
                        clear_fullsync_loading_state();
                        repl.repl_state
                            .store(repl_state_code::REPLICA_CONNECTING, Ordering::SeqCst);
                        repl.set_replica_link(replica_link_code::CONNECT);
                        thread::sleep(Duration::from_millis(200));
                        continue;
                    }
                    clear_fullsync_loading_state();
                    repl.adopt_fullresync_primary(replid, offset);
                    (offset, true, false)
                }
                PsyncOutcome::Continue { offset } => {
                    // Partial resync: no RDB. The backlog catch-up bytes arrive inline
                    // on the same stream and are consumed by the sink loop below.
                    // Do not publish `master_link_status:up` until that stream has
                    // gone idle once; otherwise clients can observe stale keyspace
                    // before catch-up commands such as FLUSHALL have applied.
                    eprintln!(
                        "redis-server: replica: +CONTINUE partial resync from offset {}",
                        offset
                    );
                    (offset, false, true)
                }
            };

        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
        crate::aof::force_current_writer_fsynced_repl_offset(online_offset);
        let mut stream_offset = online_offset;
        if !defer_partial_online_until_idle {
            publish_replica_online_after_psync(&repl);
        }

        let defer_fullsync_ack_until_idle =
            Arc::new(AtomicBool::new(defer_fullsync_ack_until_idle));
        let defer_partial_online_until_idle =
            Arc::new(AtomicBool::new(defer_partial_online_until_idle));
        let periodic_ack_stream = stream.try_clone().ok();
        if let Some(ack_stream) = periodic_ack_stream {
            let repl_for_ack = Arc::clone(&repl);
            let ack_deferred = Arc::clone(&defer_fullsync_ack_until_idle);
            let _ = thread::Builder::new()
                .name("replica-ack".to_string())
                .spawn(move || {
                    periodic_ack_loop(ack_stream, repl_for_ack, dialer_epoch, ack_deferred)
                });
        }
        run_replica_sink_loop(
            &stream,
            &repl,
            dialer_epoch,
            &mut stream_offset,
            &defer_fullsync_ack_until_idle,
            &defer_partial_online_until_idle,
        );
        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return;
        }
        repl.repl_state
            .store(repl_state_code::REPLICA_CONNECTING, Ordering::SeqCst);
        repl.set_replica_link(replica_link_code::CONNECT);
        thread::sleep(Duration::from_millis(200));
    }
}

fn publish_fullsync_loading_state(async_loading: bool) {
    if let Some(server) = GLOBAL_SERVER.get() {
        publish_fullsync_loading_state_for_server(server, async_loading);
    }
}

fn publish_fullsync_loading_state_for_server(server: &RedisServer, async_loading: bool) {
    match server.live_config.repl_diskless_load() {
        ReplDisklessLoadMode::Disabled => {}
        ReplDisklessLoadMode::Swapdb if async_loading => {
            println!("redis-server: Loading DB in memory");
            server.persistence.set_async_loading(true);
        }
        ReplDisklessLoadMode::Swapdb
        | ReplDisklessLoadMode::FlushBeforeLoad
        | ReplDisklessLoadMode::OnEmptyDb => {
            println!("redis-server: Loading DB in memory");
            server.persistence.set_loading(true);
        }
    }
}

fn clear_fullsync_loading_state() {
    if let Some(server) = GLOBAL_SERVER.get() {
        server.persistence.set_loading(false);
    }
}

fn wait_for_replica_dialer_pause(repl: &Arc<ReplicationState>, dialer_epoch: u64) -> bool {
    loop {
        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return false;
        }
        let remaining = repl.replica_dialer_pause_remaining_ms(mstime());
        if remaining <= 0 {
            return true;
        }
        thread::sleep(Duration::from_millis((remaining as u64).min(50)));
    }
}

fn run_replica_sink_loop(
    stream: &TcpStream,
    repl: &ReplicationState,
    dialer_epoch: u64,
    stream_offset: &mut i64,
    defer_fullsync_ack_until_idle: &AtomicBool,
    defer_partial_online_until_idle: &AtomicBool,
) {
    let mut read_buf = Vec::new();
    let mut tmp = vec![0u8; REPLICA_STREAM_READ_BUFFER_SIZE];
    let mut command_batch: Vec<(Vec<RedisString>, i64)> = Vec::new();
    loop {
        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return;
        }
        if repl.take_replica_link_drop_request() {
            let _ = stream.shutdown(Shutdown::Both);
            return;
        }
        let n = match stream_read(stream, &mut tmp) {
            Ok(0) => return,
            Ok(n) => n,
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if !release_idle_state_and_ack(
                    stream,
                    repl,
                    defer_fullsync_ack_until_idle,
                    defer_partial_online_until_idle,
                ) {
                    return;
                }
                continue;
            }
            Err(_) => return,
        };
        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return;
        }
        read_buf.extend_from_slice(&tmp[..n]);
        let frames = match parse_replica_frames(&mut read_buf, stream_offset) {
            Ok(frames) => frames,
            Err(_) => return,
        };
        for frame in frames {
            if !repl.dialer_epoch_is_current(dialer_epoch) {
                return;
            }
            match frame {
                ReplicaStreamFrame::Command { argv, offset_after } => {
                    command_batch.push((argv, offset_after));
                }
                ReplicaStreamFrame::GetAck { offset_after } => {
                    if replica_command_batch_has_open_transaction(&command_batch) {
                        continue;
                    }
                    if !flush_replica_command_batch(
                        stream,
                        repl,
                        dialer_epoch,
                        defer_fullsync_ack_until_idle,
                        &mut command_batch,
                    ) {
                        return;
                    }
                    if let Some(ack) = build_replconf_ack_if_ready(
                        repl,
                        defer_fullsync_ack_until_idle,
                        offset_after,
                    ) {
                        if stream_write(stream, &ack).is_err() {
                            return;
                        }
                    }
                }
            }
        }
        if !replica_command_batch_has_open_transaction(&command_batch)
            && !flush_replica_command_batch(
                stream,
                repl,
                dialer_epoch,
                defer_fullsync_ack_until_idle,
                &mut command_batch,
            )
        {
            return;
        }
    }
}

fn release_idle_state_and_ack(
    stream: &TcpStream,
    repl: &ReplicationState,
    defer_fullsync_ack_until_idle: &AtomicBool,
    defer_partial_online_until_idle: &AtomicBool,
) -> bool {
    let released_partial = release_partial_online_after_idle(repl, defer_partial_online_until_idle);
    let released_fullsync = release_fullsync_ack_after_idle(repl, defer_fullsync_ack_until_idle);
    if !released_partial && !released_fullsync {
        return true;
    }

    let offset = repl.master_repl_offset.load(Ordering::SeqCst);
    match build_replconf_ack_if_ready(repl, defer_fullsync_ack_until_idle, offset) {
        Some(ack) => stream_write(stream, &ack).is_ok(),
        None => true,
    }
}

fn release_fullsync_ack_after_idle(repl: &ReplicationState, pending: &AtomicBool) -> bool {
    if !pending.swap(false, Ordering::SeqCst) {
        return false;
    }
    repl.set_replica_link(replica_link_code::CONNECTED);
    true
}

fn publish_replica_online_after_psync(repl: &ReplicationState) {
    repl.repl_state
        .store(repl_state_code::REPLICA_ONLINE, Ordering::SeqCst);
    repl.set_replica_link(replica_link_code::CONNECTED);
    complete_manual_failover_after_psync(repl);
}

fn release_partial_online_after_idle(repl: &ReplicationState, pending: &AtomicBool) -> bool {
    if !pending.swap(false, Ordering::SeqCst) {
        return false;
    }
    repl.repl_state
        .store(repl_state_code::REPLICA_ONLINE, Ordering::SeqCst);
    repl.set_replica_link(replica_link_code::CONNECTED);
    complete_manual_failover_after_psync(repl);
    true
}

enum ReplicaStreamFrame {
    Command {
        argv: Vec<RedisString>,
        offset_after: i64,
    },
    GetAck {
        offset_after: i64,
    },
}

fn parse_replica_frames(
    read_buf: &mut Vec<u8>,
    stream_offset: &mut i64,
) -> io::Result<Vec<ReplicaStreamFrame>> {
    let mut frames = Vec::new();
    loop {
        match redis_protocol::parse_inline_or_multibulk(read_buf) {
            Ok(Some((argv, consumed))) => {
                *stream_offset = stream_offset.saturating_add(consumed as i64);
                let offset_after = *stream_offset;
                read_buf.drain(..consumed);
                if is_getack(&argv) {
                    frames.push(ReplicaStreamFrame::GetAck { offset_after });
                } else {
                    frames.push(ReplicaStreamFrame::Command { argv, offset_after });
                }
            }
            Ok(None) => return Ok(frames),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid replica stream frame",
                ))
            }
        }
    }
}

fn flush_replica_command_batch(
    stream: &TcpStream,
    repl: &ReplicationState,
    dialer_epoch: u64,
    defer_fullsync_ack_until_idle: &AtomicBool,
    command_batch: &mut Vec<(Vec<RedisString>, i64)>,
) -> bool {
    let Some((_, offset_after)) = command_batch.last() else {
        return true;
    };
    let offset_after = *offset_after;
    let commands: Vec<Vec<RedisString>> = std::mem::take(command_batch)
        .into_iter()
        .map(|(argv, _)| argv)
        .collect();
    if !apply_command_batch_via_runtime_owner(commands, offset_after) {
        return false;
    }
    if !repl.dialer_epoch_is_current(dialer_epoch) {
        return false;
    }
    // C Valkey replicas periodically ACK their processed offset even when the
    // primary did not send GETACK. This eager ACK keeps script WAIT's
    // non-blocking path accurate without adding a second timer thread to the
    // RuntimeOwner-compatible dialer.
    match build_replconf_ack_if_ready(repl, defer_fullsync_ack_until_idle, offset_after) {
        Some(ack) => stream_write(stream, &ack).is_ok(),
        None => true,
    }
}

fn replica_command_batch_has_open_transaction(batch: &[(Vec<RedisString>, i64)]) -> bool {
    let mut open = false;
    for (argv, _) in batch {
        let Some(name) = argv.first() else {
            continue;
        };
        if name.as_bytes().eq_ignore_ascii_case(b"MULTI") {
            open = true;
        } else if name.as_bytes().eq_ignore_ascii_case(b"EXEC")
            || name.as_bytes().eq_ignore_ascii_case(b"DISCARD")
        {
            open = false;
        }
    }
    open
}

/// Execute the PING / REPLCONF / PSYNC handshake over `stream`.
/// Returns the PSYNC outcome: `FullResync` (an RDB bulk follows) or `Continue`
/// (backlog catch-up streams inline, no RDB).
fn run_handshake(
    stream: &TcpStream,
    repl: &ReplicationState,
    our_port: u16,
) -> io::Result<PsyncOutcome> {
    if let Some(primaryauth) = GLOBAL_SERVER
        .get()
        .and_then(|server| server.live_config.primaryauth())
    {
        send_multibulk(stream, &[b"AUTH", primaryauth.as_bytes()])?;
        let auth_reply = read_line(stream)?;
        if !auth_reply.starts_with(b"+OK") {
            let msg = format!(
                "redis-server: Unable to AUTH to PRIMARY: {}",
                String::from_utf8_lossy(&auth_reply)
            );
            eprintln!("{}", msg);
            println!("{}", msg);
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "AUTH to primary failed: {}",
                    String::from_utf8_lossy(&auth_reply)
                ),
            ));
        }
    }

    send_multibulk(stream, &[b"PING"])?;
    let pong = read_line(stream)?;
    if !pong.starts_with(b"+PONG") && !pong.starts_with(b"+pong") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected +PONG, got: {:?}", String::from_utf8_lossy(&pong)),
        ));
    }

    let port_str = our_port.to_string();
    send_multibulk(
        stream,
        &[b"REPLCONF", b"listening-port", port_str.as_bytes()],
    )?;
    let ok1 = read_line(stream)?;
    if !ok1.starts_with(b"+OK") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "REPLCONF listening-port: expected +OK, got: {:?}",
                String::from_utf8_lossy(&ok1)
            ),
        ));
    }

    let mut capa_args: Vec<&[u8]> = vec![b"REPLCONF", b"capa", b"psync2"];
    if GLOBAL_SERVER
        .get()
        .map(|server| server.live_config.dual_channel_replication_enabled())
        .unwrap_or(false)
    {
        capa_args.push(b"dual-channel");
    }
    send_multibulk(stream, &capa_args)?;
    let ok2 = read_line(stream)?;
    if !ok2.starts_with(b"+OK") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "REPLCONF capa: expected +OK, got: {:?}",
                String::from_utf8_lossy(&ok2)
            ),
        ));
    }

    // Attempt a partial resync when we have already completed a full sync with
    // this primary (we cached its replid) and hold a concrete offset. The
    // primary grants `+CONTINUE` if our offset is still inside its backlog
    // window; otherwise it falls back to `+FULLRESYNC`. This is independent of
    // the live `repl_state`, which the dialer resets to CONNECTING on every
    // disconnect — so partial resync survives a dropped link.
    let psync_args = select_psync_args(repl);
    send_multibulk_vec(stream, &psync_args)?;

    let reply = read_line(stream)?;
    parse_psync_reply(&reply)
}

fn select_psync_args(repl: &ReplicationState) -> Vec<Vec<u8>> {
    let our_offset = repl.master_repl_offset.load(Ordering::SeqCst);
    let cached_replid = repl.cached_primary_replid();
    let failover = repl.manual_failover_state(0) == failover_state_code::FAILOVER_IN_PROGRESS;
    if let Some(replid) = cached_replid
        .filter(|_| failover || our_offset > 0 || repl.zero_offset_partial_resync_allowed())
    {
        let mut args = vec![
            b"PSYNC".to_vec(),
            replid.to_vec(),
            our_offset.to_string().into_bytes(),
        ];
        if failover {
            args.push(b"FAILOVER".to_vec());
        }
        return args;
    }
    vec![b"PSYNC".to_vec(), b"?".to_vec(), b"-1".to_vec()]
}

fn complete_manual_failover_after_psync(repl: &ReplicationState) {
    if !repl.complete_manual_failover() {
        return;
    }
    crate::replication::redirect_blocked_clients_after_failover();
    if let Some(server) = GLOBAL_SERVER.get() {
        redis_core::networking::clear_failover_pause(server);
    }
}

fn configured_handshake_timeout() -> Duration {
    let secs = GLOBAL_SERVER
        .get()
        .map(|server| server.live_config.repl_timeout())
        .unwrap_or(DEFAULT_REPL_TIMEOUT)
        .max(1);
    Duration::from_secs(secs)
}

fn set_handshake_read_timeout(stream: &TcpStream, timeout: Duration) -> io::Result<()> {
    stream.set_read_timeout(Some(timeout))
}

fn is_handshake_timeout_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

fn log_handshake_failure(err: &io::Error) {
    if is_handshake_timeout_error(err) {
        let msg = HANDSHAKE_TIMEOUT_LOG;
        eprintln!("{}", msg);
        println!("{}", msg);
        let _ = io::stdout().flush();
    } else {
        eprintln!("redis-server: replica: handshake failed: {}", err);
    }
}

/// The two PSYNC outcomes the replica must handle differently: a `+FULLRESYNC`
/// is followed by an RDB bulk payload (replace the keyspace); a `+CONTINUE`
/// streams backlog catch-up bytes inline with no RDB (keep the keyspace).
enum PsyncOutcome {
    /// `+FULLRESYNC <replid> <offset>` — adopt `replid`, read the RDB, reset
    /// the offset to `offset`.
    FullResync { offset: i64, replid: [u8; 40] },
    /// `+CONTINUE [<replid>]` — partial resync accepted; resume from `offset`
    /// (the replica's current offset). No RDB follows.
    Continue { offset: i64 },
}

/// Parse the `+FULLRESYNC <runid> <offset>` or `+CONTINUE [<runid>]` line.
fn parse_psync_reply(line: &[u8]) -> io::Result<PsyncOutcome> {
    let s = std::str::from_utf8(line)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 PSYNC reply"))?
        .trim();

    if let Some(rest) = s.strip_prefix("+FULLRESYNC ") {
        let mut parts = rest.splitn(2, ' ');
        let runid = parts.next().unwrap_or("");
        let offset_str = parts.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing offset in +FULLRESYNC")
        })?;
        let offset = offset_str.trim().parse::<i64>().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "cannot parse FULLRESYNC offset")
        })?;
        let replid = parse_replid(runid).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "bad replid in +FULLRESYNC")
        })?;
        return Ok(PsyncOutcome::FullResync { offset, replid });
    }

    if s.starts_with("+CONTINUE") {
        return Ok(PsyncOutcome::Continue {
            offset: global_replication_state()
                .master_repl_offset
                .load(Ordering::SeqCst),
        });
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unexpected PSYNC reply: {}", s),
    ))
}

/// Parse a 40-char hex replid into the fixed-width byte array the partial-resync
/// `PSYNC` echoes back. Returns `None` unless it is exactly 40 ASCII bytes.
fn parse_replid(runid: &str) -> Option<[u8; 40]> {
    let bytes = runid.as_bytes();
    if bytes.len() != 40 {
        return None;
    }
    let mut out = [0u8; 40];
    out.copy_from_slice(bytes);
    Some(out)
}

/// Read the `$<size>\r\n<rdb-bytes>` bulk payload that follows `+FULLRESYNC`.
fn read_fullresync_rdb(
    stream: &TcpStream,
    repl: &ReplicationState,
    dialer_epoch: u64,
) -> io::Result<Vec<u8>> {
    read_fullresync_rdb_with_timeout(stream, repl, dialer_epoch, configured_handshake_timeout())
}

fn read_fullresync_rdb_with_timeout(
    stream: &TcpStream,
    repl: &ReplicationState,
    dialer_epoch: u64,
    idle_timeout: Duration,
) -> io::Result<Vec<u8>> {
    let header = read_line_checked(stream, repl, dialer_epoch, idle_timeout)?;
    let header_str = std::str::from_utf8(&header)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 RDB header"))?
        .trim();

    let size_str = header_str.strip_prefix('$').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected $<size>, got: {}", header_str),
        )
    })?;
    let size: usize = size_str
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "cannot parse RDB size"))?;

    let mut buf = vec![0u8; size];
    read_exact_from_stream_checked(stream, &mut buf, repl, dialer_epoch, idle_timeout)?;
    Ok(buf)
}

fn read_line_checked(
    stream: &TcpStream,
    repl: &ReplicationState,
    dialer_epoch: u64,
    idle_timeout: Duration,
) -> io::Result<Vec<u8>> {
    let mut byte = [0u8; 1];
    loop {
        let mut line = Vec::new();
        loop {
            read_exact_from_stream_checked(stream, &mut byte, repl, dialer_epoch, idle_timeout)?;
            if byte[0] == b'\n' {
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                break;
            }
            line.push(byte[0]);
        }
        if !line.is_empty() {
            return Ok(line);
        }
    }
}

fn read_exact_from_stream_checked(
    stream: &TcpStream,
    buf: &mut [u8],
    repl: &ReplicationState,
    dialer_epoch: u64,
    idle_timeout: Duration,
) -> io::Result<()> {
    let mut filled = 0;
    let mut last_progress = Instant::now();
    while filled < buf.len() {
        if !repl.dialer_epoch_is_current(dialer_epoch) || repl.take_replica_link_drop_request() {
            let _ = stream.shutdown(Shutdown::Both);
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "replica full-sync read interrupted by role change",
            ));
        }
        match stream_read_slice(stream, &mut buf[filled..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF reading RDB",
                ));
            }
            Ok(n) => {
                filled += n;
                last_progress = Instant::now();
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if last_progress.elapsed() >= idle_timeout {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Timeout connecting to the PRIMARY",
                    ));
                }
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn apply_command_batch_via_runtime_owner(
    commands: Vec<Vec<RedisString>>,
    offset_after: i64,
) -> bool {
    if commands.is_empty() {
        return true;
    }
    send_to_runtime_owner(ReplicaApplyKind::CommandBatch(commands), offset_after)
}

/// Hand the freshly-received full-resync RDB snapshot to the runtime owner so it
/// loads into the owned databases. The previous implementation read these bytes
/// off the wire and discarded them, leaving a replica with an empty keyspace
/// after a full sync; this routes them through the same ownership boundary as
/// replicated commands.
fn load_rdb_via_runtime_owner(rdb_bytes: Vec<u8>, offset_after: i64) -> bool {
    send_to_runtime_owner(ReplicaApplyKind::LoadRdb(rdb_bytes), offset_after)
}

fn send_to_runtime_owner(kind: ReplicaApplyKind, offset_after: i64) -> bool {
    let tx = match RUNTIME_APPLY_TX.get() {
        Some(tx) => tx.clone(),
        None => return false,
    };
    let (done_tx, done_rx) = mpsc::channel();
    if tx
        .send(ReplicaApplyRequest {
            kind,
            offset_after,
            done: done_tx,
        })
        .is_err()
    {
        return false;
    }
    done_rx.recv_timeout(RUNTIME_APPLY_TIMEOUT).unwrap_or(false)
}

/// Returns true when the argv represents `REPLCONF GETACK *`.
fn is_getack(argv: &[RedisString]) -> bool {
    argv.len() >= 2
        && argv[0].as_bytes().eq_ignore_ascii_case(b"REPLCONF")
        && argv[1].as_bytes().eq_ignore_ascii_case(b"GETACK")
}

/// Periodically send `REPLCONF ACK <offset>` to the master every second.
/// Exits when the stop flag is set or the write fails (master disconnected).
fn periodic_ack_loop(
    mut stream: TcpStream,
    repl: Arc<ReplicationState>,
    dialer_epoch: u64,
    defer_fullsync_ack_until_idle: Arc<AtomicBool>,
) {
    loop {
        thread::sleep(Duration::from_secs(1));

        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return;
        }
        if repl.repl_state.load(Ordering::SeqCst) != repl_state_code::REPLICA_ONLINE {
            return;
        }

        let Some(msg) = build_replconf_ack_if_ready(
            &repl,
            &defer_fullsync_ack_until_idle,
            repl.master_repl_offset.load(Ordering::SeqCst),
        ) else {
            continue;
        };
        if stream.write_all(&msg).is_err() {
            return;
        }
    }
}

/// Build a `REPLCONF ACK <offset>` multibulk frame.
fn build_replconf_ack(offset: i64) -> Vec<u8> {
    let offset_str = offset.to_string();
    let fack = crate::aof::current_fsynced_repl_offset();
    if fack >= 0 {
        let fack_str = fack.to_string();
        build_multibulk(&[
            b"REPLCONF",
            b"ACK",
            offset_str.as_bytes(),
            b"FACK",
            fack_str.as_bytes(),
        ])
    } else {
        build_multibulk(&[b"REPLCONF", b"ACK", offset_str.as_bytes()])
    }
}

fn build_replconf_ack_if_ready(
    repl: &ReplicationState,
    defer_fullsync_ack_until_idle: &AtomicBool,
    offset: i64,
) -> Option<Vec<u8>> {
    if repl.replica_link.load(Ordering::SeqCst) == replica_link_code::CONNECTED
        && !defer_fullsync_ack_until_idle.load(Ordering::SeqCst)
    {
        Some(build_replconf_ack(offset))
    } else {
        None
    }
}

/// Encode `parts` as a RESP multibulk array and write it to `stream`.
fn send_multibulk(stream: &TcpStream, parts: &[&[u8]]) -> io::Result<()> {
    let msg = build_multibulk(parts);
    stream_write(stream, &msg)
}

fn send_multibulk_vec(stream: &TcpStream, parts: &[Vec<u8>]) -> io::Result<()> {
    let borrowed: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    send_multibulk(stream, &borrowed)
}

/// Build a RESP multibulk byte string from `parts`.
fn build_multibulk(parts: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        buf.extend_from_slice(part);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

/// Read a `\r\n`-terminated line from the stream.
/// Accumulates bytes until a CRLF is found. Returns the line bytes without
/// the trailing `\r\n`. Standalone `\n` bytes (keepalive pings that Valkey
/// masters send periodically) are skipped until a non-empty line arrives.
fn read_line(stream: &TcpStream) -> io::Result<Vec<u8>> {
    let mut byte = [0u8; 1];
    loop {
        let mut line = Vec::new();
        loop {
            stream_read_exact(stream, &mut byte)?;
            if byte[0] == b'\n' {
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                break;
            }
            line.push(byte[0]);
        }
        if !line.is_empty() {
            return Ok(line);
        }
    }
}

fn stream_write(stream: &TcpStream, data: &[u8]) -> io::Result<()> {
    let mut s = stream
        .try_clone()
        .map_err(|e| io::Error::new(e.kind(), format!("stream clone: {}", e)))?;
    s.write_all(data)
}

fn stream_read(stream: &TcpStream, buf: &mut [u8]) -> io::Result<usize> {
    let mut s = stream
        .try_clone()
        .map_err(|e| io::Error::new(e.kind(), format!("stream clone: {}", e)))?;
    s.read(buf)
}

fn stream_read_exact(stream: &TcpStream, buf: &mut [u8]) -> io::Result<()> {
    let mut s = stream
        .try_clone()
        .map_err(|e| io::Error::new(e.kind(), format!("stream clone: {}", e)))?;
    s.read_exact(buf)
}

fn stream_read_slice(stream: &TcpStream, buf: &mut [u8]) -> io::Result<usize> {
    let mut s = stream
        .try_clone()
        .map_err(|e| io::Error::new(e.kind(), format!("stream clone: {}", e)))?;
    s.read(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn local_state() -> ReplicationState {
        ReplicationState::new([b'1'; 40], 1024)
    }

    fn argv_bytes(argv: &[RedisString]) -> Vec<Vec<u8>> {
        argv.iter().map(|arg| arg.as_bytes().to_vec()).collect()
    }

    fn command(parts: &[&[u8]]) -> (Vec<RedisString>, i64) {
        (
            parts
                .iter()
                .map(|part| RedisString::from_bytes(part))
                .collect(),
            0,
        )
    }

    fn batch_sizes_for_chunked_replica_stream(stream: &[u8], chunk_size: usize) -> Vec<usize> {
        let mut read_buf = Vec::new();
        let mut stream_offset = 0;
        let mut command_batch: Vec<(Vec<RedisString>, i64)> = Vec::new();
        let mut batch_sizes = Vec::new();
        for chunk in stream.chunks(chunk_size) {
            read_buf.extend_from_slice(chunk);
            let frames = parse_replica_frames(&mut read_buf, &mut stream_offset).unwrap();
            for frame in frames {
                match frame {
                    ReplicaStreamFrame::Command { argv, offset_after } => {
                        command_batch.push((argv, offset_after));
                    }
                    ReplicaStreamFrame::GetAck { .. } => {
                        if !replica_command_batch_has_open_transaction(&command_batch)
                            && !command_batch.is_empty()
                        {
                            batch_sizes.push(command_batch.len());
                            command_batch.clear();
                        }
                    }
                }
            }
            if !replica_command_batch_has_open_transaction(&command_batch)
                && !command_batch.is_empty()
            {
                batch_sizes.push(command_batch.len());
                command_batch.clear();
            }
        }
        assert!(
            read_buf.is_empty(),
            "test stream should end on a complete RESP frame"
        );
        batch_sizes
    }

    fn connected_stream_pair() -> (TcpStream, TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let replica = TcpStream::connect(addr).expect("connect peer");
        let (primary, _) = listener.accept().expect("accept peer");
        (replica, primary)
    }

    fn assert_ack_frame_contains_offset(frame: &[u8], offset: i64) {
        let offset = offset.to_string();
        assert!(
            frame.windows(b"REPLCONF".len()).any(|w| w == b"REPLCONF")
                && frame.windows(b"ACK".len()).any(|w| w == b"ACK")
                && frame
                    .windows(offset.as_bytes().len())
                    .any(|w| w == offset.as_bytes()),
            "ACK frame should encode the processed offset {offset}: {:?}",
            String::from_utf8_lossy(frame)
        );
    }

    #[test]
    fn replica_command_batch_waits_for_transaction_exec() {
        let mut batch = vec![command(&[b"MULTI"]), command(&[b"SET", b"k", b"v"])];
        assert!(
            replica_command_batch_has_open_transaction(&batch),
            "a split catch-up stream must not flush after MULTI before EXEC arrives"
        );

        batch.push(command(&[b"EXEC"]));
        assert!(
            !replica_command_batch_has_open_transaction(&batch),
            "the transaction envelope is complete once EXEC has been parsed"
        );
    }

    #[test]
    fn large_partial_resync_commands_batch_by_read_window() {
        let value = vec![b'x'; 10_000];
        let mut stream = Vec::new();
        for i in 0..512 {
            let key = format!("k:{i}");
            stream.extend_from_slice(&build_multibulk(&[b"SET", key.as_bytes(), &value]));
        }

        let batches =
            batch_sizes_for_chunked_replica_stream(&stream, REPLICA_STREAM_READ_BUFFER_SIZE);

        assert!(
            batches.len() <= 2,
            "10KB catch-up commands should not degrade into one RuntimeOwner apply request per command: {batches:?}"
        );
        assert!(
            batches.iter().any(|count| *count >= 400),
            "the first full read window should amortize RuntimeOwner apply roundtrips: {batches:?}"
        );
    }

    #[test]
    fn handshake_read_timeout_detects_primary_stall_before_pong() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let primary = thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("accept replica");
            thread::sleep(Duration::from_millis(150));
        });

        let stream = TcpStream::connect(addr).expect("connect test primary");
        set_handshake_read_timeout(&stream, Duration::from_millis(20))
            .expect("set handshake read timeout");
        let repl = local_state();
        let err = match run_handshake(&stream, &repl, 0) {
            Ok(_) => panic!("stalled primary should time out during handshake"),
            Err(err) => err,
        };

        assert!(
            is_handshake_timeout_error(&err),
            "expected handshake timeout classification, got {err:?}"
        );
        assert_eq!(
            HANDSHAKE_TIMEOUT_LOG,
            "redis-server: Timeout connecting to the PRIMARY"
        );

        let _ = primary.join();
    }

    #[test]
    fn fullsync_rdb_header_read_times_out_when_primary_stalls() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let primary = thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("accept replica");
            thread::sleep(Duration::from_millis(150));
        });

        let stream = TcpStream::connect(addr).expect("connect test primary");
        stream
            .set_read_timeout(Some(Duration::from_millis(10)))
            .expect("set read timeout");
        let repl = local_state();
        let err = match read_fullresync_rdb_with_timeout(
            &stream,
            &repl,
            repl.dialer_epoch.load(Ordering::SeqCst),
            Duration::from_millis(50),
        ) {
            Ok(_) => panic!("stalled primary should time out before RDB bulk header"),
            Err(err) => err,
        };

        assert!(
            is_handshake_timeout_error(&err),
            "expected full-sync header timeout classification, got {err:?}"
        );

        let _ = primary.join();
    }

    #[test]
    fn fullsync_role_connects_before_ack_is_released_after_stream_idle() {
        let repl = local_state();
        repl.set_replica_link(replica_link_code::TRANSFER);
        let pending = AtomicBool::new(true);

        assert_eq!(repl.replica_link_str(), "sync");
        repl.set_replica_link(replica_link_code::CONNECTED);
        assert_eq!(
            repl.replica_link_str(),
            "connected",
            "after the fullsync RDB is loaded, ROLE can report connected even while ACK is deferred"
        );
        assert!(
            build_replconf_ack_if_ready(&repl, &pending, 10).is_none(),
            "loaded fullsync links must not ACK until the post-RDB stream goes idle"
        );

        release_fullsync_ack_after_idle(&repl, &pending);
        assert!(!pending.load(Ordering::SeqCst));
        assert!(build_replconf_ack_if_ready(&repl, &pending, 10).is_some());

        repl.set_replica_link(replica_link_code::TRANSFER);
        release_fullsync_ack_after_idle(&repl, &pending);
        assert_eq!(
            repl.replica_link_str(),
            "sync",
            "once the deferred ACK flag is consumed, later idle checks are no-ops"
        );
    }

    #[test]
    fn fullsync_idle_release_immediately_acks_applied_catchup_offset() {
        let (replica_stream, mut primary_stream) = connected_stream_pair();
        primary_stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set read timeout");

        let repl = local_state();
        repl.repl_state
            .store(repl_state_code::REPLICA_ONLINE, Ordering::SeqCst);
        repl.set_replica_link(replica_link_code::CONNECTED);
        repl.master_repl_offset.store(147, Ordering::SeqCst);
        let fullsync_pending = AtomicBool::new(true);
        let partial_pending = AtomicBool::new(false);

        assert!(
            release_idle_state_and_ack(&replica_stream, &repl, &fullsync_pending, &partial_pending,),
            "idle full-sync release should write the ACK synchronously"
        );
        assert!(!fullsync_pending.load(Ordering::SeqCst));

        let mut buf = [0u8; 128];
        let n = primary_stream.read(&mut buf).expect("read ACK");
        assert_ack_frame_contains_offset(&buf[..n], 147);
    }

    #[test]
    fn partial_resync_link_waits_for_catchup_idle_before_online() {
        let repl = local_state();
        repl.repl_state
            .store(repl_state_code::REPLICA_CONNECTING, Ordering::SeqCst);
        repl.set_replica_link(replica_link_code::HANDSHAKE);
        let pending_online = AtomicBool::new(true);
        let ack_ready = AtomicBool::new(false);

        assert_eq!(repl.replica_link_str(), "handshake");
        assert!(
            build_replconf_ack_if_ready(&repl, &ack_ready, 10).is_none(),
            "a partial-resync replica must not ACK or report online before catch-up idle"
        );

        release_partial_online_after_idle(&repl, &pending_online);

        assert!(!pending_online.load(Ordering::SeqCst));
        assert_eq!(repl.replica_link_str(), "connected");
        assert_eq!(
            repl.repl_state.load(Ordering::SeqCst),
            repl_state_code::REPLICA_ONLINE
        );
        assert!(
            build_replconf_ack_if_ready(&repl, &ack_ready, 10).is_some(),
            "after catch-up stream idle, the replica can publish online and ACK"
        );

        repl.set_replica_link(replica_link_code::HANDSHAKE);
        release_partial_online_after_idle(&repl, &pending_online);
        assert_eq!(
            repl.replica_link_str(),
            "handshake",
            "once the partial-online flag is consumed, later idle checks are no-ops"
        );
    }

    #[test]
    fn partial_resync_idle_release_immediately_acks_applied_catchup_offset() {
        let (replica_stream, mut primary_stream) = connected_stream_pair();
        primary_stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set read timeout");

        let repl = local_state();
        repl.repl_state
            .store(repl_state_code::REPLICA_CONNECTING, Ordering::SeqCst);
        repl.set_replica_link(replica_link_code::HANDSHAKE);
        repl.master_repl_offset.store(211, Ordering::SeqCst);
        let fullsync_pending = AtomicBool::new(false);
        let partial_pending = AtomicBool::new(true);

        assert!(
            release_idle_state_and_ack(&replica_stream, &repl, &fullsync_pending, &partial_pending,),
            "idle partial-resync release should publish online and write the ACK synchronously"
        );
        assert!(!partial_pending.load(Ordering::SeqCst));
        assert_eq!(repl.replica_link_str(), "connected");
        assert_eq!(
            repl.repl_state.load(Ordering::SeqCst),
            repl_state_code::REPLICA_ONLINE
        );

        let mut buf = [0u8; 128];
        let n = primary_stream.read(&mut buf).expect("read ACK");
        assert_ack_frame_contains_offset(&buf[..n], 211);
    }

    #[test]
    fn fullsync_link_suppresses_ack_until_connected() {
        let repl = local_state();
        let ready = AtomicBool::new(false);
        repl.set_replica_link(replica_link_code::TRANSFER);

        assert!(
            build_replconf_ack_if_ready(&repl, &ready, 10).is_none(),
            "a replica still reporting ROLE sync must not ACK and make the master mark it online"
        );

        repl.set_replica_link(replica_link_code::CONNECTED);
        let ack = build_replconf_ack_if_ready(&repl, &ready, 10).expect("connected link can ACK");
        assert!(
            ack.windows(b"REPLCONF".len()).any(|w| w == b"REPLCONF")
                && ack.windows(b"ACK".len()).any(|w| w == b"ACK")
                && ack.windows(b"10".len()).any(|w| w == b"10"),
            "ACK frame should encode the processed offset: {:?}",
            String::from_utf8_lossy(&ack)
        );
    }

    #[test]
    fn replica_stream_parser_collects_complete_frames_with_offsets() {
        let repl = local_state();
        let set_frame = build_multibulk(&[b"SET", b"k", b"v"]);
        let incr_frame = build_multibulk(&[b"INCR", b"n"]);
        let mut buf = Vec::new();
        buf.extend_from_slice(&set_frame);
        buf.extend_from_slice(&incr_frame);

        let mut stream_offset = 0;
        let frames = parse_replica_frames(&mut buf, &mut stream_offset).unwrap();

        assert!(buf.is_empty());
        assert_eq!(stream_offset, (set_frame.len() + incr_frame.len()) as i64);
        assert_eq!(frames.len(), 2);
        match &frames[0] {
            ReplicaStreamFrame::Command { argv, offset_after } => {
                assert_eq!(
                    argv_bytes(argv),
                    vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]
                );
                assert_eq!(*offset_after, set_frame.len() as i64);
            }
            ReplicaStreamFrame::GetAck { .. } => panic!("SET parsed as GETACK"),
        }
        match &frames[1] {
            ReplicaStreamFrame::Command { argv, offset_after } => {
                assert_eq!(argv_bytes(argv), vec![b"INCR".to_vec(), b"n".to_vec()]);
                assert_eq!(*offset_after, (set_frame.len() + incr_frame.len()) as i64);
            }
            ReplicaStreamFrame::GetAck { .. } => panic!("INCR parsed as GETACK"),
        }
        assert_eq!(
            repl.master_repl_offset.load(Ordering::SeqCst),
            0,
            "parsing may compute stream offsets, but applied/ACK offset must only advance after RuntimeOwner apply succeeds"
        );
    }

    #[test]
    fn replica_stream_parser_keeps_partial_tail_for_next_read() {
        let set_frame = build_multibulk(&[b"SET", b"k", b"v"]);
        let incr_frame = build_multibulk(&[b"INCR", b"n"]);
        let partial_len = incr_frame.len() - 2;
        let mut buf = Vec::new();
        buf.extend_from_slice(&set_frame);
        buf.extend_from_slice(&incr_frame[..partial_len]);

        let mut stream_offset = 0;
        let frames = parse_replica_frames(&mut buf, &mut stream_offset).unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(buf, incr_frame[..partial_len]);
        assert_eq!(stream_offset, set_frame.len() as i64);
    }

    #[test]
    fn replica_stream_parser_marks_getack_without_dropping_surrounding_commands() {
        let set_frame = build_multibulk(&[b"SET", b"k", b"v"]);
        let getack_frame = build_multibulk(&[b"REPLCONF", b"GETACK", b"*"]);
        let incr_frame = build_multibulk(&[b"INCR", b"n"]);
        let mut buf = Vec::new();
        buf.extend_from_slice(&set_frame);
        buf.extend_from_slice(&getack_frame);
        buf.extend_from_slice(&incr_frame);

        let mut stream_offset = 0;
        let frames = parse_replica_frames(&mut buf, &mut stream_offset).unwrap();

        assert!(buf.is_empty());
        assert_eq!(frames.len(), 3);
        match &frames[0] {
            ReplicaStreamFrame::Command { argv, offset_after } => {
                assert_eq!(
                    argv_bytes(argv),
                    vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]
                );
                assert_eq!(*offset_after, set_frame.len() as i64);
            }
            ReplicaStreamFrame::GetAck { .. } => panic!("SET parsed as GETACK"),
        }
        match &frames[1] {
            ReplicaStreamFrame::GetAck { offset_after } => {
                assert_eq!(*offset_after, (set_frame.len() + getack_frame.len()) as i64);
            }
            ReplicaStreamFrame::Command { .. } => panic!("GETACK parsed as command"),
        }
        match &frames[2] {
            ReplicaStreamFrame::Command { argv, offset_after } => {
                assert_eq!(argv_bytes(argv), vec![b"INCR".to_vec(), b"n".to_vec()]);
                assert_eq!(
                    *offset_after,
                    (set_frame.len() + getack_frame.len() + incr_frame.len()) as i64
                );
            }
            ReplicaStreamFrame::GetAck { .. } => panic!("INCR parsed as GETACK"),
        }
    }

    #[test]
    fn psync_args_use_fresh_sync_without_cached_offset() {
        let repl = local_state();
        assert_eq!(
            select_psync_args(&repl),
            vec![b"PSYNC".to_vec(), b"?".to_vec(), b"-1".to_vec()]
        );
    }

    #[test]
    fn psync_args_do_not_use_cached_replid_at_zero_offset_by_default() {
        let repl = local_state();
        let cached = [b'b'; 40];
        repl.set_cached_primary_replid(cached);

        assert_eq!(
            select_psync_args(&repl),
            vec![b"PSYNC".to_vec(), b"?".to_vec(), b"-1".to_vec()]
        );
    }

    #[test]
    fn psync_args_use_cached_replid_at_zero_offset_after_empty_fullsync() {
        let repl = local_state();
        let cached = [b'b'; 40];
        repl.set_cached_primary_replid(cached);
        repl.set_zero_offset_partial_resync_allowed(true);

        assert_eq!(
            select_psync_args(&repl),
            vec![b"PSYNC".to_vec(), cached.to_vec(), b"0".to_vec()]
        );
    }

    #[test]
    fn psync_args_include_failover_even_at_zero_offset() {
        let repl = local_state();
        let cached = [b'a'; 40];
        repl.set_cached_primary_replid(cached);
        repl.failover_state
            .store(failover_state_code::FAILOVER_IN_PROGRESS, Ordering::Relaxed);

        assert_eq!(
            select_psync_args(&repl),
            vec![
                b"PSYNC".to_vec(),
                cached.to_vec(),
                b"0".to_vec(),
                b"FAILOVER".to_vec()
            ]
        );
    }

    #[test]
    fn diskless_load_mode_controls_fullsync_loading_state() {
        let server = RedisServer::default();

        publish_fullsync_loading_state_for_server(&server, true);
        assert!(!server.persistence.loading());
        assert!(!server.persistence.async_loading());

        server
            .live_config
            .set_repl_diskless_load(ReplDisklessLoadMode::Swapdb);
        publish_fullsync_loading_state_for_server(&server, true);
        assert!(server.persistence.loading());
        assert!(server.persistence.async_loading());

        server.persistence.set_loading(false);
        publish_fullsync_loading_state_for_server(&server, false);
        assert!(server.persistence.loading());
        assert!(!server.persistence.async_loading());

        server.persistence.set_loading(false);
        server
            .live_config
            .set_repl_diskless_load(ReplDisklessLoadMode::FlushBeforeLoad);
        publish_fullsync_loading_state_for_server(&server, false);
        assert!(server.persistence.loading());
        assert!(!server.persistence.async_loading());

        server.persistence.set_loading(false);
        server
            .live_config
            .set_repl_diskless_load(ReplDisklessLoadMode::OnEmptyDb);
        publish_fullsync_loading_state_for_server(&server, false);
        assert!(server.persistence.loading());
        assert!(!server.persistence.async_loading());
    }

    #[test]
    fn fullsync_rdb_read_exits_when_dialer_epoch_changes() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let writer = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept client");
            stream_write(&stream, b"$5\r\n").expect("write RDB bulk header");
            thread::sleep(Duration::from_millis(250));
        });

        let stream = TcpStream::connect(addr).expect("connect test stream");
        stream
            .set_read_timeout(Some(Duration::from_millis(20)))
            .expect("set read timeout");
        let repl = Arc::new(local_state());
        let dialer_epoch = repl.dialer_epoch.load(Ordering::SeqCst);
        let repl_for_interrupt = Arc::clone(&repl);
        let interrupter = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            repl_for_interrupt
                .dialer_epoch
                .fetch_add(1, Ordering::SeqCst);
        });

        let err = read_fullresync_rdb(&stream, &repl, dialer_epoch)
            .expect_err("epoch change should interrupt full-sync read");
        assert_eq!(err.kind(), io::ErrorKind::Interrupted);

        let _ = interrupter.join();
        let _ = writer.join();
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         1
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Deleted superseded dialer_loop/ingest_rdb/run_command_apply_loop/
//                  apply_command_locally/lock_db (alternate apply loop, no caller).
//                  Replica dialer is explicitly blocked after the owner-owned
//                  DB flip until replication apply can route through
//                  RuntimeOwner instead of a divergent global DB. ACKs are
//                  suppressed while a full-sync link is still ROLE sync so the
//                  primary keeps the RDB-channel state visible until idle.
// ──────────────────────────────────────────────────────────────────────────
