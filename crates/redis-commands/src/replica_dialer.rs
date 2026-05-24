//! Replica-side connection state machine — Wave C.
//!
//! When `REPLICAOF host port` is issued, `spawn_replica_dialer` launches a
//! dedicated background thread that:
//!
//!  1. Connects to the master's TCP port.
//!  2. Runs the PING / REPLCONF / PSYNC handshake.
//!  3. Reads the `$<size>\r\n<rdb-bytes>` full-resync payload.
//!  4. Writes the blob to a temp file and calls `rdb::load_into` to populate the DB.
//!  5. Enters the command-apply loop: reads one RESP frame per iteration,
//!     applies it locally (through a discarding `CommandContext`), and
//!     responds to `REPLCONF GETACK *` with `REPLCONF ACK <offset>`.
//!  6. Sends a periodic `REPLCONF ACK <offset>` every second so the master
//!     can power WAIT and detect lagging replicas.
//!  7. On any I/O error or EOF: sleeps one second and restarts from step 1.
//!  8. If `ReplicationState::dialer_stop_flag` is set (by `REPLICAOF NO ONE`),
//!     exits immediately.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use redis_core::client::Client;
use redis_core::command_context::CommandContext;
use redis_core::db::RedisDb;
use redis_core::pubsub_registry::PubSubRegistry;
use redis_core::replication::{global_replication_state, repl_state_code, ReplicationState};
use redis_core::server::RedisServer;
use redis_types::RedisString;

static GLOBAL_SERVER: OnceLock<Arc<RedisServer>> = OnceLock::new();
static GLOBAL_OUR_PORT: OnceLock<u16> = OnceLock::new();
static GLOBAL_RDB_DIR: OnceLock<String> = OnceLock::new();

/// Register the shared resources the dialer thread needs.
///
/// Called once from the binary's main before any `REPLICAOF` command can be
/// issued. Subsequent calls are no-ops (OnceLock semantics).
pub fn install_dialer_resources(server: Arc<RedisServer>, our_port: u16, rdb_dir: String) {
    let _ = GLOBAL_SERVER.set(server);
    let _ = GLOBAL_OUR_PORT.set(our_port);
    let _ = GLOBAL_RDB_DIR.set(rdb_dir);
}

/// Spawn a background dialer thread that implements the full replica state
/// machine described in the module doc.
///
/// The function returns immediately; the spawned thread runs until
/// `ReplicationState::dialer_stop_flag` is set to `true`. Returns an error
/// when the dialer resources have not been installed.
pub fn spawn_replica_dialer(host: RedisString, port: u16) -> Result<(), &'static str> {
    let _ = (host, port);
    let _ = GLOBAL_SERVER
        .get()
        .ok_or("dialer resources not installed")?;
    let _ = GLOBAL_OUR_PORT
        .get()
        .ok_or("dialer resources not installed")?;
    let _ = GLOBAL_RDB_DIR
        .get()
        .ok_or("dialer resources not installed")?;

    // TODO(architect): replica apply must become a RuntimeOwner event/channel
    // before REPLICAOF can start after the owner-owned DB flip. The previous
    // dialer mutated a global Arc<Mutex<RedisDb>>, which would now be a
    // divergent keyspace.
    Err("replica dialer blocked until RuntimeOwner-owned DB apply channel exists")
}

/// The outer reconnect loop. Connects to the master, runs the handshake,
/// applies commands, and restarts on any error.
fn dialer_loop(
    host: RedisString,
    port: u16,
    our_port: u16,
    rdb_dir: String,
    db: Arc<Mutex<RedisDb>>,
    server: Arc<RedisServer>,
) {
    let repl = global_replication_state();

    loop {
        if repl.dialer_stop_flag.load(Ordering::SeqCst) {
            return;
        }

        let host_str = String::from_utf8_lossy(host.as_bytes()).to_string();
        let stream = match TcpStream::connect(format!("{}:{}", host_str, port)) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "redis-server: replica: connect {}:{} failed: {}",
                    host_str, port, e
                );
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        if let Err(e) = stream.set_nodelay(true) {
            eprintln!("redis-server: replica: set_nodelay failed: {}", e);
        }

        let initial_offset = match run_handshake(&stream, &repl, our_port) {
            Ok(off) => off,
            Err(e) => {
                eprintln!("redis-server: replica: handshake failed: {}", e);
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        if repl.dialer_stop_flag.load(Ordering::SeqCst) {
            return;
        }

        let rdb_bytes = match read_fullresync_rdb(&stream) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("redis-server: replica: RDB read failed: {}", e);
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        if let Err(e) = ingest_rdb(&rdb_bytes, &rdb_dir, &db) {
            eprintln!("redis-server: replica: RDB ingest failed: {}", e);
            thread::sleep(Duration::from_secs(1));
            continue;
        }

        repl.master_repl_offset
            .store(initial_offset, Ordering::SeqCst);
        repl.repl_state
            .store(repl_state_code::REPLICA_ONLINE, Ordering::SeqCst);
        eprintln!("redis-server: replica: ONLINE at offset {}", initial_offset);

        let ack_stream = match stream.try_clone() {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "redis-server: replica: try_clone for ACK thread failed: {}",
                    e
                );
                repl.repl_state
                    .store(repl_state_code::REPLICA_CONNECTING, Ordering::SeqCst);
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        let repl_for_ack = Arc::clone(&repl);
        let _ = thread::Builder::new()
            .name("replica-ack".to_string())
            .spawn(move || {
                periodic_ack_loop(ack_stream, repl_for_ack);
            });

        run_command_apply_loop(&stream, &repl, &db, &server);

        repl.repl_state
            .store(repl_state_code::REPLICA_CONNECTING, Ordering::SeqCst);
        eprintln!("redis-server: replica: disconnected, will reconnect");
        thread::sleep(Duration::from_secs(1));
    }
}

/// Execute the PING / REPLCONF / PSYNC handshake over `stream`.
///
/// Returns the initial replication offset from the `+FULLRESYNC` reply.
/// On `+CONTINUE` we return the current master offset (partial resync).
fn run_handshake(stream: &TcpStream, repl: &ReplicationState, our_port: u16) -> io::Result<i64> {
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

    let our_runid = repl.runid();
    let our_offset = repl.master_repl_offset.load(Ordering::SeqCst);
    let is_reconnect =
        repl.repl_state.load(Ordering::SeqCst) == repl_state_code::REPLICA_ONLINE && our_offset > 0;

    if is_reconnect {
        let offset_str = our_offset.to_string();
        let runid_str = std::str::from_utf8(our_runid)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid runid"))?;
        send_multibulk(
            stream,
            &[b"PSYNC", runid_str.as_bytes(), offset_str.as_bytes()],
        )?;
    } else {
        send_multibulk(stream, &[b"PSYNC", b"?", b"-1"])?;
    }

    let reply = read_line(stream)?;
    parse_psync_reply(&reply)
}

/// Parse the `+FULLRESYNC <runid> <offset>` or `+CONTINUE <runid>` line.
///
/// Returns the initial offset the replica should track from.
fn parse_psync_reply(line: &[u8]) -> io::Result<i64> {
    let s = std::str::from_utf8(line)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 PSYNC reply"))?
        .trim();

    if let Some(rest) = s.strip_prefix("+FULLRESYNC ") {
        let mut parts = rest.splitn(2, ' ');
        let _runid = parts.next().unwrap_or("");
        let offset_str = parts.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing offset in +FULLRESYNC")
        })?;
        let offset = offset_str.trim().parse::<i64>().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "cannot parse FULLRESYNC offset")
        })?;
        return Ok(offset);
    }

    if s.starts_with("+CONTINUE") {
        return Ok(global_replication_state()
            .master_repl_offset
            .load(Ordering::SeqCst));
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unexpected PSYNC reply: {}", s),
    ))
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

/// Write the RDB bytes to a temp file, then call `rdb::load_into`.
fn ingest_rdb(rdb_bytes: &[u8], rdb_dir: &str, db: &Arc<Mutex<RedisDb>>) -> io::Result<()> {
    let temp_path = PathBuf::from(rdb_dir).join("temp-incoming.rdb");
    {
        let mut f = std::fs::File::create(&temp_path)?;
        f.write_all(rdb_bytes)?;
        f.flush()?;
    }

    let result = {
        let mut guard = lock_db(db)?;
        *guard = RedisDb::new(0);
        redis_core::rdb::load_into(&mut guard, &temp_path)
    };

    let _ = std::fs::remove_file(&temp_path);

    match result {
        Ok(msg) => {
            eprintln!("redis-server: replica: RDB loaded: {}", msg);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// The main command-apply loop. Reads one RESP frame per iteration, applies
/// it to the local DB, and handles `REPLCONF GETACK *` by replying with
/// our current offset. Exits on any read error or when the stop flag is set.
fn run_command_apply_loop(
    stream: &TcpStream,
    repl: &ReplicationState,
    db: &Arc<Mutex<RedisDb>>,
    server: &Arc<RedisServer>,
) {
    let mut read_buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 8192];

    loop {
        if repl.dialer_stop_flag.load(Ordering::SeqCst) {
            return;
        }

        let n = match stream_read(stream, &mut tmp) {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };
        read_buf.extend_from_slice(&tmp[..n]);

        loop {
            match redis_protocol::parse_inline_or_multibulk(&read_buf) {
                Ok(Some((argv, consumed))) => {
                    repl.master_repl_offset
                        .fetch_add(consumed as i64, Ordering::SeqCst);
                    read_buf.drain(..consumed);

                    if argv.is_empty() {
                        continue;
                    }

                    if is_getack(&argv) {
                        let offset = repl.master_repl_offset.load(Ordering::SeqCst);
                        let ack_msg = build_replconf_ack(offset);
                        if stream_write(stream, &ack_msg).is_err() {
                            return;
                        }
                    } else {
                        apply_command_locally(&argv, db, server);
                    }
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
    }
}

/// Returns true when the argv represents `REPLCONF GETACK *`.
fn is_getack(argv: &[RedisString]) -> bool {
    argv.len() >= 2
        && argv[0].as_bytes().eq_ignore_ascii_case(b"REPLCONF")
        && argv[1].as_bytes().eq_ignore_ascii_case(b"GETACK")
}

/// Apply a command received from the master to our local DB.
///
/// Uses a discarding `CommandContext` (replies written into a `Client` whose
/// `reply_buf` is never flushed to a socket). The `is_replica` flag on the
/// client prevents re-propagation of the write to our own downstream replicas.
fn apply_command_locally(
    argv: &[RedisString],
    db: &Arc<Mutex<RedisDb>>,
    server: &Arc<RedisServer>,
) {
    if argv.is_empty() {
        return;
    }
    let name = argv[0].clone();
    let mut client = Client::new(0);
    client.is_replica = true;
    client.authenticated_user = Some(RedisString::from_bytes(b"default"));
    client.set_args(argv.to_vec());

    let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
    let result = match db.lock() {
        Ok(mut guard) => {
            let mut ctx = CommandContext::with_server(
                &mut client,
                &mut guard,
                Arc::clone(server),
                Arc::clone(&registry),
            );
            crate::dispatch::dispatch_command_name(&mut ctx, name.as_bytes())
        }
        Err(p) => {
            let mut guard = p.into_inner();
            let mut ctx = CommandContext::with_server(
                &mut client,
                &mut guard,
                Arc::clone(server),
                Arc::clone(&registry),
            );
            crate::dispatch::dispatch_command_name(&mut ctx, name.as_bytes())
        }
    };
    if let Err(e) = result {
        eprintln!(
            "redis-server: replica: apply_command_locally({}) error: {:?}",
            String::from_utf8_lossy(name.as_bytes()),
            e
        );
    }
}

/// Periodically send `REPLCONF ACK <offset>` to the master every second.
///
/// Exits when the stop flag is set or the write fails (master disconnected).
fn periodic_ack_loop(mut stream: TcpStream, repl: Arc<ReplicationState>) {
    loop {
        thread::sleep(Duration::from_secs(1));

        if repl.dialer_stop_flag.load(Ordering::SeqCst) {
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
    build_multibulk(&[b"REPLCONF", b"ACK", offset_str.as_bytes()])
}

/// Encode `parts` as a RESP multibulk array and write it to `stream`.
fn send_multibulk(stream: &TcpStream, parts: &[&[u8]]) -> io::Result<()> {
    let msg = build_multibulk(parts);
    stream_write(stream, &msg)
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
///
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

/// Read exactly `buf.len()` bytes from the stream.
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

fn lock_db(db: &Arc<Mutex<RedisDb>>) -> io::Result<std::sync::MutexGuard<'_, RedisDb>> {
    db.lock()
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "DB mutex poisoned"))
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

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        valkey/src/replication.c (replica-side state machine)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         1
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Replica dialer is explicitly blocked after the owner-owned
//                  DB flip until replication apply can route through
//                  RuntimeOwner instead of a divergent global DB.
// ──────────────────────────────────────────────────────────────────────────
