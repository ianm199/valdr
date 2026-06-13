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
use std::net::TcpStream;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use redis_core::replication::{
    failover_state_code, global_replication_state, repl_state_code, replica_link_code,
    ReplicationState,
};
use redis_core::server::RedisServer;
use redis_types::RedisString;

static GLOBAL_SERVER: OnceLock<Arc<RedisServer>> = OnceLock::new();
static GLOBAL_OUR_PORT: OnceLock<u16> = OnceLock::new();
static GLOBAL_RDB_DIR: OnceLock<String> = OnceLock::new();
static RUNTIME_APPLY_TX: OnceLock<Sender<ReplicaApplyRequest>> = OnceLock::new();

/// Work the dialer thread hands to the runtime owner loop because the owner —
/// not the dialer — owns the live DB slice.
/// `Command` is an ordinary replicated write parsed off the primary stream.
/// `LoadRdb` is the full-resync snapshot the primary streams after `FULLRESYNC`;
/// it must be loaded into the owned databases, replacing their contents. Routing
/// both through the same queue keeps every keyspace mutation on the owner's
/// ownership boundary.
pub enum ReplicaApplyKind {
    Command(Vec<RedisString>),
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

        repl.set_replica_link(replica_link_code::HANDSHAKE);
        let outcome = match run_handshake(&stream, &repl, our_port) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("redis-server: replica: handshake failed: {}", e);
                thread::sleep(Duration::from_millis(200));
                continue;
            }
        };

        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return;
        }

        let online_offset = match outcome {
            PsyncOutcome::FullResync { offset, replid } => {
                // Adopt the primary's replid so the next reconnect can request a
                // partial resync against it.
                repl.set_cached_primary_replid(replid);
                repl.set_replica_link(replica_link_code::TRANSFER);
                let rdb_bytes = match read_fullresync_rdb(&stream) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        eprintln!("redis-server: replica: RDB sink failed: {}", e);
                        thread::sleep(Duration::from_millis(200));
                        continue;
                    }
                };

                if !repl.dialer_epoch_is_current(dialer_epoch) {
                    return;
                }
                if !load_rdb_via_runtime_owner(rdb_bytes, offset) {
                    eprintln!("redis-server: replica: RDB load failed");
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }
                repl.master_repl_offset.store(offset, Ordering::SeqCst);
                offset
            }
            PsyncOutcome::Continue { offset } => {
                // Partial resync: no RDB. The backlog catch-up bytes arrive inline
                // on the same stream and are consumed by the sink loop below, which
                // advances the offset as it applies them. The keyspace is preserved.
                eprintln!(
                    "redis-server: replica: +CONTINUE partial resync from offset {}",
                    offset
                );
                offset
            }
        };

        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
        crate::aof::force_current_writer_fsynced_repl_offset(online_offset);
        repl.repl_state
            .store(repl_state_code::REPLICA_ONLINE, Ordering::SeqCst);
        repl.set_replica_link(replica_link_code::CONNECTED);
        complete_manual_failover_after_psync(&repl);

        let periodic_ack_stream = stream.try_clone().ok();
        if let Some(ack_stream) = periodic_ack_stream {
            let repl_for_ack = Arc::clone(&repl);
            let _ = thread::Builder::new()
                .name("replica-ack".to_string())
                .spawn(move || periodic_ack_loop(ack_stream, repl_for_ack, dialer_epoch));
        }
        run_replica_sink_loop(&stream, &repl, dialer_epoch);
        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return;
        }
        repl.repl_state
            .store(repl_state_code::REPLICA_CONNECTING, Ordering::SeqCst);
        repl.set_replica_link(replica_link_code::CONNECT);
        thread::sleep(Duration::from_millis(200));
    }
}

fn run_replica_sink_loop(stream: &TcpStream, repl: &ReplicationState, dialer_epoch: u64) {
    let mut read_buf = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        if !repl.dialer_epoch_is_current(dialer_epoch) {
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
                continue;
            }
            Err(_) => return,
        };
        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return;
        }
        read_buf.extend_from_slice(&tmp[..n]);
        loop {
            match redis_protocol::parse_inline_or_multibulk(&read_buf) {
                Ok(Some((argv, consumed))) => {
                    if !repl.dialer_epoch_is_current(dialer_epoch) {
                        return;
                    }
                    let offset_after = repl
                        .master_repl_offset
                        .fetch_add(consumed as i64, Ordering::SeqCst)
                        .saturating_add(consumed as i64);
                    read_buf.drain(..consumed);
                    if is_getack(&argv) {
                        if stream_write(stream, &build_replconf_ack(offset_after)).is_err() {
                            return;
                        }
                    } else {
                        if !apply_command_via_runtime_owner(argv, offset_after) {
                            return;
                        }
                        if !repl.dialer_epoch_is_current(dialer_epoch) {
                            return;
                        }
                        // C Valkey replicas periodically ACK their processed
                        // offset even when the primary did not send GETACK.
                        // This eager ACK keeps script WAIT's non-blocking
                        // path accurate without adding a second timer thread
                        // to the RuntimeOwner-compatible dialer.
                        if stream_write(stream, &build_replconf_ack(offset_after)).is_err() {
                            return;
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
    }
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

    send_multibulk(stream, &[b"REPLCONF", b"capa", b"psync2"])?;
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
    if let Some(replid) = cached_replid.filter(|_| failover || our_offset > 0) {
        let mut args = vec![b"PSYNC".to_vec(), replid.to_vec(), our_offset.to_string().into_bytes()];
        if failover {
            args.push(b"FAILOVER".to_vec());
        }
        return args;
    }
    vec![b"PSYNC".to_vec(), b"?".to_vec(), b"-1".to_vec()]
}

fn complete_manual_failover_after_psync(repl: &Arc<ReplicationState>) {
    if !repl.complete_manual_failover() {
        return;
    }
    crate::replication::redirect_blocked_clients_after_failover();
    if let Some(server) = GLOBAL_SERVER.get() {
        redis_core::networking::clear_failover_pause(server);
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
fn read_fullresync_rdb(stream: &TcpStream) -> io::Result<Vec<u8>> {
    let header = read_line(stream)?;
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
    read_exact_from_stream(stream, &mut buf)?;
    Ok(buf)
}

fn apply_command_via_runtime_owner(argv: Vec<RedisString>, offset_after: i64) -> bool {
    send_to_runtime_owner(ReplicaApplyKind::Command(argv), offset_after)
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
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap_or(false)
}

/// Returns true when the argv represents `REPLCONF GETACK *`.
fn is_getack(argv: &[RedisString]) -> bool {
    argv.len() >= 2
        && argv[0].as_bytes().eq_ignore_ascii_case(b"REPLCONF")
        && argv[1].as_bytes().eq_ignore_ascii_case(b"GETACK")
}

/// Periodically send `REPLCONF ACK <offset>` to the master every second.
/// Exits when the stop flag is set or the write fails (master disconnected).
fn periodic_ack_loop(mut stream: TcpStream, repl: Arc<ReplicationState>, dialer_epoch: u64) {
    loop {
        thread::sleep(Duration::from_secs(1));

        if !repl.dialer_epoch_is_current(dialer_epoch) {
            return;
        }
        if repl.repl_state.load(Ordering::SeqCst) != repl_state_code::REPLICA_ONLINE {
            return;
        }

        let offset = repl.master_repl_offset.load(Ordering::SeqCst);
        let msg = build_replconf_ack(offset);
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

/// Read exactly `buf.len` bytes from the stream.
fn read_exact_from_stream(stream: &TcpStream, buf: &mut [u8]) -> io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = stream_read_slice(stream, &mut buf[filled..])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF reading RDB",
            ));
        }
        filled += n;
    }
    Ok(())
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

    fn local_state() -> ReplicationState {
        ReplicationState::new([b'1'; 40], 1024)
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
//                  RuntimeOwner instead of a divergent global DB.
// ──────────────────────────────────────────────────────────────────────────
