//! `redis-server` binary entry point — Wave A scaffolding.
//!
//! Minimal TCP accept loop: binds a port, spawns a thread per accepted
//! connection, reads RESP requests, dispatches through `redis-commands`,
//! and writes the reply back to the socket.
//!
//! Round 8a adds a per-connection writer thread (driven by an `mpsc::Sender`)
//! so PUBLISH running on a foreign connection can deliver bytes to subscriber
//! sockets without re-acquiring the subscriber's transport from a foreign
//! thread.
//!
//! Out of scope for Wave A:
//!   * Event-loop based I/O (no `mio` / `tokio`); blocking thread-per-conn.
//!   * Multi-DB routing (every command sees DB 0).
//!   * Replication, cluster, persistence, modules.

use std::fs;
use std::io::{self, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_commands::{dispatch, pubsub};
use redis_core::command_context::CommandContext;
use redis_core::db::RedisDb;
use redis_core::pubsub_registry::PubSubRegistry;
use redis_core::{Client, Connection};
use redis_protocol::frame::{encode_resp2, RespFrame};
use redis_protocol::parse_inline_or_multibulk;
use redis_types::{RedisError, RedisString};

const DEFAULT_PORT: u16 = 6379;
const DEFAULT_BIND: &str = "127.0.0.1";

/// Parsed command-line arguments.
struct CliArgs {
    port: u16,
    bind: String,
}

impl Default for CliArgs {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            bind: DEFAULT_BIND.to_string(),
        }
    }
}

/// Parse CLI flags and (optionally) a Valkey-style config file path.
///
/// The Valkey TCL harness invokes the server as `valkey-server /path/to/conf`,
/// so when `argv[1]` does not start with `--` we treat it as a config-file
/// path and parse `key value` lines from it. Recognised directives are `port`
/// and `bind`; everything else is silently skipped so the unknown directives
/// the harness writes (`enable-protected-configs`, `unixsocket`, `loglevel`,
/// `notify-keyspace-events`, etc.) do not abort startup.
fn parse_args(argv: Vec<String>) -> Result<CliArgs, String> {
    let mut out = CliArgs::default();
    let mut it = argv.into_iter().skip(1).peekable();
    if let Some(first) = it.peek() {
        if !first.starts_with("--") {
            let path = it.next().expect("peek then next");
            apply_config_file(&mut out, Path::new(&path))?;
        }
    }
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--port" | "-p" => {
                let v = it.next().ok_or_else(|| "--port requires a value".to_string())?;
                out.port = v.parse().map_err(|_| format!("invalid port: {}", v))?;
            }
            "--bind" => {
                let v = it.next().ok_or_else(|| "--bind requires a value".to_string())?;
                out.bind = v;
            }
            "--help" | "-h" => {
                println!("Usage: redis-server [<config-file>] [--port N] [--bind addr]");
                std::process::exit(0);
            }
            other => {
                eprintln!("redis-server: ignoring unknown flag '{}'", other);
            }
        }
    }
    Ok(out)
}

/// Read a Valkey-style config file and update `args` with the directives we
/// understand. Lines are split on the first run of whitespace; blank lines and
/// `#`-prefixed comments are skipped; unknown directives are ignored.
fn apply_config_file(args: &mut CliArgs, path: &Path) -> Result<(), String> {
    let contents = fs::read_to_string(path)
        .map_err(|e| format!("cannot read config file '{}': {}", path.display(), e))?;
    for raw_line in contents.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let key = match parts.next() {
            Some(k) if !k.is_empty() => k,
            _ => continue,
        };
        let value = parts.next().unwrap_or("").trim();
        match key {
            "port" => {
                let v: u16 = value.parse().map_err(|_| format!("invalid port: {}", value))?;
                args.port = v;
            }
            "bind" => {
                let first_addr = value.split_whitespace().next().unwrap_or("");
                if !first_addr.is_empty() {
                    args.bind = first_addr.to_string();
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Emit the startup-log sentinels the Valkey TCL harness greps for.
///
/// `wait_server_started` in `tests/support/server.tcl` scans the server's
/// stdout for ` PID: <pid>` followed by `Server initialized`. Once those two
/// tokens appear in the same stream the harness considers the server alive
/// and proceeds to dial the configured port. We emit the conventional
/// `<pid>:M <ts> * …` triplet so the regex matches without further tweaks.
fn emit_startup_log() {
    let pid = std::process::id();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("{}:M {} * PID: {}", pid, ts, pid);
    println!("{}:M {} * Server initialized", pid, ts);
    println!("{}:M {} * Ready to accept connections tcp", pid, ts);
    let _ = io::stdout().flush();
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let args = match parse_args(argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("redis-server: {}", e);
            std::process::exit(1);
        }
    };

    let bind_ip: IpAddr = match args.bind.parse() {
        Ok(ip) => ip,
        Err(_) => {
            eprintln!(
                "redis-server: --bind expects an IP literal (got '{}'); hostnames not yet supported",
                args.bind
            );
            std::process::exit(1);
        }
    };
    let addr = SocketAddr::new(bind_ip, args.port);

    let listener = match TcpListener::bind(addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("redis-server: bind {} failed: {}", addr, e);
            std::process::exit(1);
        }
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    install_shutdown_handler(Arc::clone(&shutdown));

    if let Err(e) = listener.set_nonblocking(false) {
        eprintln!("redis-server: set_nonblocking(false) failed: {}", e);
    }
    eprintln!("redis-server: listening on {}", addr);
    emit_startup_log();

    let db = Arc::new(Mutex::new(RedisDb::new(0)));
    let next_client_id = Arc::new(AtomicU64::new(1));
    let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
    serve(listener, shutdown, db, next_client_id, registry);
}

/// Best-effort SIGINT/SIGTERM handler stub.
fn install_shutdown_handler(_shutdown: Arc<AtomicBool>) {}

/// Accept loop. One std::thread per accepted connection.
fn serve(
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    next_client_id: Arc<AtomicU64>,
    registry: Arc<Mutex<PubSubRegistry>>,
) {
    for incoming in listener.incoming() {
        if shutdown.load(Ordering::SeqCst) {
            eprintln!("redis-server: shutdown requested, exiting accept loop");
            return;
        }
        match incoming {
            Ok(stream) => {
                if let Err(e) = stream.set_nodelay(true) {
                    eprintln!("redis-server: set_nodelay failed: {}", e);
                }
                let peer = stream
                    .peer_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                let shutdown = Arc::clone(&shutdown);
                let db = Arc::clone(&db);
                let registry = Arc::clone(&registry);
                let id = next_client_id.fetch_add(1, Ordering::Relaxed);
                let _ = thread::Builder::new()
                    .name(format!("client-{}", peer))
                    .spawn(move || handle_connection(stream, shutdown, db, id, peer, registry));
            }
            Err(e) => {
                eprintln!("redis-server: accept failed: {}", e);
                if shutdown.load(Ordering::SeqCst) {
                    return;
                }
            }
        }
    }
}

/// Spawn a writer thread that drains an `mpsc::Receiver<Vec<u8>>` and writes
/// each payload to the connection. Returns the matching sender that the read
/// loop and the pub/sub registry both hold.
fn spawn_writer(
    mut writer: TcpStream,
    peer: String,
) -> Sender<Vec<u8>> {
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let _ = thread::Builder::new()
        .name(format!("writer-{}", peer))
        .spawn(move || {
            for payload in rx {
                if writer.write_all(&payload).is_err() {
                    break;
                }
            }
            let _ = writer.shutdown(std::net::Shutdown::Both);
        });
    tx
}

/// Per-connection event loop. Reads from the socket, feeds the incremental
/// parser, dispatches each completed command, then ships replies through the
/// outbound mpsc so the writer thread owns all socket writes.
fn handle_connection(
    stream: TcpStream,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    id: u64,
    peer_addr: String,
    registry: Arc<Mutex<PubSubRegistry>>,
) {
    let writer_clone = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("redis-server: try_clone failed for {}: {}", peer_addr, e);
            return;
        }
    };
    let outbound = spawn_writer(writer_clone, peer_addr.clone());

    if let Ok(mut guard) = registry.lock() {
        guard.register_sender(id, outbound.clone());
    }

    let mut client = Client::with_connection(Connection::Tcp(stream));
    client.id = id;
    client.addr = Some(peer_addr);
    let mut read_buf = [0u8; 16 * 1024];

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        let conn = match client.conn.as_mut() {
            Some(c) => c,
            None => break,
        };

        let n = match conn.read(&mut read_buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };

        client.query_buf.extend_from_slice(&read_buf[..n]);

        let mut disconnect = false;
        loop {
            let parsed = parse_inline_or_multibulk(&client.query_buf);
            match parsed {
                Ok(Some((argv, consumed))) => {
                    client.query_buf.drain(..consumed);
                    process_command(&mut client, argv, &db, &registry);
                }
                Ok(None) => break,
                Err(err) => {
                    queue_error_reply(&mut client, &err);
                    let _ = flush_reply(&mut client, &outbound);
                    disconnect = true;
                    break;
                }
            }

            if !flush_reply(&mut client, &outbound) {
                disconnect = true;
                break;
            }

            if client.should_close {
                disconnect = true;
                break;
            }
        }

        if disconnect {
            break;
        }

        if !flush_reply(&mut client, &outbound) {
            break;
        }

        if client.should_close {
            break;
        }
    }

    let _ = pubsub::drop_client_from_registry(&registry, id);
    drop(outbound);
}

/// Install `argv` as the current command and route through the dispatcher.
fn process_command(
    client: &mut Client,
    argv: Vec<RedisString>,
    db: &Arc<Mutex<RedisDb>>,
    registry: &Arc<Mutex<PubSubRegistry>>,
) {
    client.set_args(argv);
    let result = {
        let mut guard = match db.lock() {
            Ok(g) => g,
            Err(poison) => poison.into_inner(),
        };
        let mut ctx = CommandContext::with_db_and_pubsub(client, &mut guard, Arc::clone(registry));
        dispatch(&mut ctx)
    };
    if let Err(err) = result {
        let payload = err.to_resp_payload();
        encode_resp2(&RespFrame::Error(payload), &mut client.reply_buf);
    }
    client.reset_args();
}

/// Drain `client.reply_buf` through the outbound sender. Returns `false` if
/// the writer thread has already exited (connection should tear down).
fn flush_reply(client: &mut Client, outbound: &Sender<Vec<u8>>) -> bool {
    if client.reply_buf.is_empty() {
        return true;
    }
    let bytes = std::mem::take(&mut client.reply_buf);
    outbound.send(bytes).is_ok()
}

/// Append a RESP error line to the pending reply buffer for later flushing.
fn queue_error_reply(client: &mut Client, err: &RedisError) {
    let payload = err.to_resp_payload();
    encode_resp2(&RespFrame::Error(payload), &mut client.reply_buf);
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        architect packet (Wave A main + Round 8a pub/sub wiring)
//   target_crate:  redis-server
//   confidence:    high
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Blocking thread-per-conn TCP server with a per-conn
//                  writer thread driven by mpsc. Pub/sub registry is shared
//                  via Arc<Mutex<>>. SIGINT handler is a no-op stub.
// ──────────────────────────────────────────────────────────────────────────
