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
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustls::StreamOwned;

#[cfg(unix)]
use libc;
use redis_commands::connection::get_max_clients;
use redis_commands::{dispatch, pubsub};
use redis_core::blocked_keys::{blocked_keys_index, current_time_ms};
use redis_core::client_info::client_info_registry;
use redis_core::command_context::CommandContext;
use redis_core::databases::global_databases;
use redis_core::db::RedisDb;
use redis_core::expire::{active_expire_config, spawn_active_expire_thread};
use redis_core::lru_clock::spawn_lru_clock_thread;
use redis_core::metrics::server_metrics;
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
    maxclients: u64,
    rdb_disabled: bool,
    dir: String,
    dbfilename: String,
    appendonly: bool,
    appendfilename: String,
    appendfsync: u8,
    set_max_intset_entries: usize,
    set_max_listpack_entries: usize,
    set_max_listpack_value: usize,
}

impl Default for CliArgs {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            bind: DEFAULT_BIND.to_string(),
            maxclients: get_max_clients(),
            rdb_disabled: false,
            dir: redis_core::live_config::DEFAULT_RDB_DIR.to_string(),
            dbfilename: redis_core::live_config::DEFAULT_RDB_FILENAME.to_string(),
            appendonly: false,
            appendfilename: redis_core::live_config::DEFAULT_AOF_FILENAME.to_string(),
            appendfsync: redis_commands::aof::FSYNC_EVERYSEC,
            set_max_intset_entries: redis_core::live_config::DEFAULT_SET_MAX_INTSET_ENTRIES,
            set_max_listpack_entries: redis_core::live_config::DEFAULT_SET_MAX_LISTPACK_ENTRIES,
            set_max_listpack_value: redis_core::live_config::DEFAULT_SET_MAX_LISTPACK_VALUE,
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
            "--rdb-disabled" => {
                out.rdb_disabled = true;
            }
            "--appendonly" => {
                let v = it.next().ok_or_else(|| "--appendonly requires yes/no".to_string())?;
                out.appendonly = v == "yes";
            }
            "--appendfilename" => {
                let v = it.next().ok_or_else(|| "--appendfilename requires a value".to_string())?;
                out.appendfilename = v;
            }
            "--appendfsync" => {
                let v = it.next().ok_or_else(|| "--appendfsync requires always/everysec/no".to_string())?;
                if let Some(p) = redis_commands::aof::parse_fsync_policy(v.as_bytes()) {
                    out.appendfsync = p;
                }
            }
            "--help" | "-h" => {
                println!("Usage: redis-server [<config-file>] [--port N] [--bind addr] [--rdb-disabled]");
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
            "maxclients" => {
                if let Ok(v) = value.parse::<u64>() {
                    args.maxclients = v;
                }
            }
            "dir" => {
                if !value.is_empty() {
                    args.dir = value.to_string();
                }
            }
            "dbfilename" => {
                if !value.is_empty() {
                    args.dbfilename = value.to_string();
                }
            }
            "appendonly" => {
                args.appendonly = value == "yes";
            }
            "appendfilename" => {
                if !value.is_empty() {
                    args.appendfilename = value.to_string();
                }
            }
            "appendfsync" => {
                if let Some(p) = redis_commands::aof::parse_fsync_policy(value.as_bytes()) {
                    args.appendfsync = p;
                }
            }
            "set-max-intset-entries" => {
                if let Ok(v) = value.parse::<usize>() {
                    args.set_max_intset_entries = v;
                }
            }
            "set-max-listpack-entries" => {
                if let Ok(v) = value.parse::<usize>() {
                    args.set_max_listpack_entries = v;
                }
            }
            "set-max-listpack-value" => {
                if let Ok(v) = value.parse::<usize>() {
                    args.set_max_listpack_value = v;
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

    server_metrics().set_tcp_port(args.port);

    let live_config = Arc::new(redis_core::live_config::LiveConfig::new());
    let live_config_for_hook = Arc::clone(&live_config);
    live_config.set_maxclients(args.maxclients);
    live_config.set_rdb_dir(args.dir.clone());
    live_config.set_rdb_filename(args.dbfilename.clone());
    live_config.set_appendonly(args.appendonly);
    live_config.set_appendfilename(args.appendfilename.clone());
    live_config.set_appendfsync(args.appendfsync);
    live_config.store_set_max_intset_entries(args.set_max_intset_entries);
    live_config.store_set_max_listpack_entries(args.set_max_listpack_entries);
    live_config.store_set_max_listpack_value(args.set_max_listpack_value);
    redis_core::object::install_live_config(Arc::clone(&live_config));
    redis_commands::install_live_config_handle(Arc::clone(&live_config));
    redis_core::acl::install_acl_state();
    let repl_state = Arc::new(redis_core::replication::ReplicationState::new(
        redis_core::replication::generate_runid(),
        live_config.repl_backlog_size() as usize,
    ));
    redis_core::replication::install_replication_state(Arc::clone(&repl_state));

    let server = Arc::new(redis_core::RedisServer::with_live_config(
        args.port,
        Arc::clone(&live_config),
    ));

    let db_zero = global_databases().get(0);

    redis_commands::replica_dialer::install_dialer_resources(
        Arc::clone(&db_zero),
        Arc::clone(&server),
        args.port,
        args.dir.clone(),
    );

    if !args.rdb_disabled {
        let rdb_path = redis_core::rdb::rdb_path(
            &live_config.rdb_dir(),
            &live_config.rdb_filename(),
        );
        if rdb_path.exists() {
            match db_zero.lock() {
                Ok(mut guard) => {
                    match redis_core::rdb::load_into(&mut guard, &rdb_path) {
                        Ok(msg) => eprintln!("redis-server: {}", msg),
                        Err(e) => eprintln!("redis-server: RDB load failed ({}): {}", rdb_path.display(), e),
                    }
                }
                Err(p) => {
                    let mut guard = p.into_inner();
                    match redis_core::rdb::load_into(&mut guard, &rdb_path) {
                        Ok(msg) => eprintln!("redis-server: {}", msg),
                        Err(e) => eprintln!("redis-server: RDB load failed ({}): {}", rdb_path.display(), e),
                    }
                }
            }
        }
    }

    if args.appendonly {
        let aof_path = std::path::Path::new(&args.dir).join(&args.appendfilename);
        if aof_path.exists() {
            match db_zero.lock() {
                Ok(mut guard) => {
                    match redis_commands::aof::replay_aof(&aof_path, &mut guard) {
                        Ok(n) => eprintln!("redis-server: AOF replay: {} commands", n),
                        Err(e) => eprintln!("redis-server: AOF replay failed ({}): {}", aof_path.display(), e),
                    }
                }
                Err(p) => {
                    let mut guard = p.into_inner();
                    match redis_commands::aof::replay_aof(&aof_path, &mut guard) {
                        Ok(n) => eprintln!("redis-server: AOF replay: {} commands", n),
                        Err(e) => eprintln!("redis-server: AOF replay failed ({}): {}", aof_path.display(), e),
                    }
                }
            }
        }
        match redis_commands::aof::AofWriter::open(&aof_path, args.appendfsync) {
            Ok(w) => redis_commands::aof::install_aof_writer(Arc::new(w)),
            Err(e) => eprintln!("redis-server: failed to open AOF {}: {}", aof_path.display(), e),
        }
        redis_commands::aof::spawn_fsync_thread();
    }

    let next_client_id = Arc::new(AtomicU64::new(1));
    let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
    redis_core::db::install_global_notify_handle(
        Arc::clone(&registry),
        Arc::clone(&live_config),
    );
    redis_core::db::install_swapdb_wake_hook(Box::new(|other_db_id| {
        redis_commands::wake_blocked_after_swapdb(other_db_id, other_db_id);
    }));
    spawn_blocked_timeout_thread(Arc::clone(&shutdown));
    let active_expire_cfg = Arc::clone(active_expire_config());
    let metrics_arc = Arc::clone(server_metrics());
    let _ = spawn_active_expire_thread(global_databases().get(0), active_expire_cfg, Some(metrics_arc));
    let _ = spawn_lru_clock_thread();
    spawn_bgsave_reaper(Arc::clone(&server), Arc::clone(&live_config));
    spawn_repl_bgsave_reaper();

    let db_for_tls = global_databases().get(0);
    let registry_for_tls = Arc::clone(&registry);
    let server_for_tls = Arc::clone(&server);
    let next_id_for_tls = Arc::clone(&next_client_id);
    let shutdown_for_tls = Arc::clone(&shutdown);
    let bind_ip_for_hook = bind_ip;
    redis_core::tls::install_tls_start_hook(Box::new(move |port| {
        if port == 0 {
            return;
        }
        let cert = match live_config_for_hook.tls_cert_file() {
            Some(p) => p,
            None => {
                eprintln!("redis-server: CONFIG SET tls-port requires tls-cert-file to be set first");
                live_config_for_hook.set_tls_port(0);
                return;
            }
        };
        let key = match live_config_for_hook.tls_key_file() {
            Some(p) => p,
            None => {
                eprintln!("redis-server: CONFIG SET tls-port requires tls-key-file to be set first");
                live_config_for_hook.set_tls_port(0);
                return;
            }
        };
        let ca = live_config_for_hook.tls_ca_cert_file();
        let require_client_cert = live_config_for_hook.tls_auth_clients() == 1;
        let tls_cfg = match redis_core::tls::TlsConfig::from_paths(
            &cert,
            &key,
            ca.as_deref(),
            require_client_cert,
        ) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("redis-server: failed to load TLS config: {}", e);
                live_config_for_hook.set_tls_port(0);
                return;
            }
        };
        let tls_addr = SocketAddr::new(bind_ip_for_hook, port);
        let tls_listener = match TcpListener::bind(tls_addr) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("redis-server: TLS bind {} failed: {}", tls_addr, e);
                live_config_for_hook.set_tls_port(0);
                return;
            }
        };
        eprintln!("redis-server: TLS listener on {}", tls_addr);
        let db2 = Arc::clone(&db_for_tls);
        let reg2 = Arc::clone(&registry_for_tls);
        let srv2 = Arc::clone(&server_for_tls);
        let id2 = Arc::clone(&next_id_for_tls);
        let shut2 = Arc::clone(&shutdown_for_tls);
        let _ = thread::Builder::new()
            .name("tls-accept".to_string())
            .spawn(move || {
                serve_tls(tls_listener, tls_cfg, shut2, db2, id2, reg2, srv2);
            });
    }));

    serve(listener, shutdown, db_zero, next_client_id, registry, server, args.port);
}

/// Reaper thread for BGSAVE child processes.
///
/// Polls `server.rdb_child_pid` every 500 ms. When a non-zero PID is
/// recorded, calls `waitpid` with `WNOHANG` to check if the child has exited.
/// On success: updates `last_save_unix` and clears the PID. On failure
/// (non-zero exit status): logs an error and clears the PID.
///
/// Only compiled on Unix — the thread-snapshot BGSAVE fallback on non-Unix
/// platforms does not produce child processes and needs no reaping.
#[cfg(unix)]
fn spawn_bgsave_reaper(
    server: Arc<redis_core::RedisServer>,
    live_config: Arc<redis_core::live_config::LiveConfig>,
) {
    use std::sync::atomic::Ordering;
    let _ = thread::Builder::new()
        .name("bgsave-reaper".to_string())
        .spawn(move || loop {
            thread::sleep(Duration::from_millis(500));
            let child_pid = server.rdb_child_pid();
            if child_pid == 0 {
                continue;
            }
            let mut status: libc::c_int = 0;
            let ret = unsafe { libc::waitpid(child_pid as libc::pid_t, &mut status, libc::WNOHANG) };
            if ret == 0 {
                continue;
            }
            if ret < 0 {
                eprintln!("redis-server: waitpid({}) failed: errno={}", child_pid, ret);
                server.set_rdb_child_pid(0);
                server_metrics()
                    .rdb_saves_failed
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let exited_ok = libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0;
            if exited_ok {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                live_config.set_last_save_unix(now);
                server_metrics()
                    .rdb_saves_succeeded
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                eprintln!(
                    "redis-server: BGSAVE child {} exited with status {}",
                    child_pid, status
                );
                server_metrics()
                    .rdb_saves_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
            server.set_rdb_child_pid(0);
        });
}

#[cfg(not(unix))]
fn spawn_bgsave_reaper(
    _server: Arc<redis_core::RedisServer>,
    _live_config: Arc<redis_core::live_config::LiveConfig>,
) {
}

/// Reaper for BGSAVE-for-replication child processes.
///
/// Tracked separately from the user-`BGSAVE` reaper because the two can run
/// concurrently: a user invoking `BGSAVE` while a replica is mid-handshake
/// keeps both children alive at once. On successful child exit this thread
/// reads the temp RDB file into memory and ships it through each waiting
/// replica's outbound channel, then sends the catch-up backlog window (from
/// `snapshot_offset` to the current master offset) and flips the replica to
/// `Online`.
///
/// On non-Unix the BGSAVE-for-replication path uses a thread fallback that
/// drops the job onto `ReplicationState` after the save completes — no
/// `waitpid` is needed there. For now the non-Unix path will leave the temp
/// file in place; full disposition of the fallback is a future TODO.
#[cfg(unix)]
fn spawn_repl_bgsave_reaper() {
    let _ = thread::Builder::new()
        .name("repl-bgsave-reaper".to_string())
        .spawn(move || loop {
            thread::sleep(Duration::from_millis(200));
            let repl = redis_core::replication::global_replication_state();
            let child_pid = repl.repl_child_pid();
            if child_pid == 0 {
                continue;
            }
            let mut status: libc::c_int = 0;
            let ret = unsafe { libc::waitpid(child_pid as libc::pid_t, &mut status, libc::WNOHANG) };
            if ret == 0 {
                continue;
            }
            if ret < 0 {
                eprintln!(
                    "redis-server: repl-bgsave waitpid({}) failed: ret={}",
                    child_pid, ret
                );
                let _ = repl.take_repl_bgsave_job();
                repl.set_repl_child_pid(0);
                continue;
            }
            let exited_ok = libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0;
            if !exited_ok {
                eprintln!(
                    "redis-server: BGSAVE-for-replication child {} exited with status {}",
                    child_pid, status
                );
                if let Some(job) = repl.take_repl_bgsave_job() {
                    let _ = std::fs::remove_file(&job.temp_path);
                }
                repl.set_repl_child_pid(0);
                continue;
            }
            dispatch_full_sync_transfer();
            repl.set_repl_child_pid(0);
        });
}

#[cfg(not(unix))]
fn spawn_repl_bgsave_reaper() {}

/// Stream the freshly-baked RDB plus the catch-up backlog window to every
/// replica registered on the current `ReplBgsaveJob`, then mark each one
/// `Online`. Called by the repl-bgsave reaper after `waitpid` confirms the
/// child exited cleanly.
fn dispatch_full_sync_transfer() {
    let repl = redis_core::replication::global_replication_state();
    let job = match repl.take_repl_bgsave_job() {
        Some(j) => j,
        None => return,
    };
    let rdb_bytes = match fs::read(&job.temp_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!(
                "redis-server: failed to read RDB temp file {}: {}",
                job.temp_path.display(),
                e
            );
            let _ = std::fs::remove_file(&job.temp_path);
            return;
        }
    };
    let mut header = format!("${}\r\n", rdb_bytes.len()).into_bytes();
    header.extend_from_slice(&rdb_bytes);

    let snapshot_offset = job.snapshot_offset;
    for client_id in &job.waiting_replicas {
        repl.set_replica_state(*client_id, redis_core::replication::ReplicaState::SendingRdb);
        if !repl.send_to_replica(*client_id, header.clone()) {
            eprintln!(
                "redis-server: full-sync RDB send failed for replica client_id={}",
                client_id
            );
            continue;
        }
        let current_offset = repl.master_offset();
        if current_offset > snapshot_offset {
            let catch_up = {
                let guard = match repl.backlog.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.read_at(snapshot_offset, (current_offset - snapshot_offset) as usize)
            };
            if let Some(bytes) = catch_up {
                if !bytes.is_empty() {
                    let _ = repl.send_to_replica(*client_id, bytes);
                }
            }
        }
        repl.set_replica_state(*client_id, redis_core::replication::ReplicaState::Online);
        eprintln!(
            "redis-server: full-sync RDB delivered to replica client_id={} ({} bytes, snapshot_offset={})",
            client_id,
            rdb_bytes.len(),
            snapshot_offset
        );
    }
    let _ = std::fs::remove_file(&job.temp_path);
}

/// Background scanner that wakes blocked BLPOP/BRPOP/BLMOVE waiters once
/// their deadline elapses.
///
/// Polls the global `BlockedKeysIndex` every 100 ms, drains entries whose
/// `deadline_ms` is in the past, and ships either `*-1\r\n` (null array,
/// for BLPOP / BRPOP / BLMPOP) or `$-1\r\n` (null bulk, for BLMOVE /
/// BRPOPLPUSH) through each waiter's outbound mpsc.
fn spawn_blocked_timeout_thread(shutdown: Arc<AtomicBool>) {
    let _ = thread::Builder::new()
        .name("blocked-timeout".to_string())
        .spawn(move || {
            while !shutdown.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(100));
                let expired = {
                    let mut idx = match blocked_keys_index().lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                    idx.take_expired(current_time_ms())
                };
                for waiter in expired {
                    let reply = match &waiter.action {
                        redis_core::blocked_keys::BlockedAction::Wait { target_offset, .. } => {
                            let repl = redis_core::replication::global_replication_state();
                            let count = {
                                let guard = match repl.replicas.lock() {
                                    Ok(g) => g,
                                    Err(p) => p.into_inner(),
                                };
                                guard
                                    .values()
                                    .filter(|r| {
                                        r.offset.load(std::sync::atomic::Ordering::Relaxed)
                                            >= *target_offset
                                    })
                                    .count()
                            };
                            waiter.action.timeout_reply_bytes_with_count(count)
                        }
                        other => other.timeout_reply_bytes().to_vec(),
                    };
                    let _ = waiter.sender.send(reply);
                }
            }
        });
}

/// Best-effort SIGINT/SIGTERM handler stub.
fn install_shutdown_handler(_shutdown: Arc<AtomicBool>) {}

/// Accept loop. One std::thread per accepted connection.
///
/// Before spawning a handler thread, checks the live `maxclients` limit against
/// the `connected_clients` counter in `ServerMetrics`. When the limit is
/// reached, writes the canonical error reply and closes the socket.
fn serve(
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    next_client_id: Arc<AtomicU64>,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
    tcp_port: u16,
) {
    for incoming in listener.incoming() {
        if shutdown.load(Ordering::SeqCst) {
            eprintln!("redis-server: shutdown requested, exiting accept loop");
            return;
        }
        match incoming {
            Ok(mut stream) => {
                let metrics = server_metrics();
                let current = metrics.connected_clients.load(Ordering::Relaxed);
                let limit = get_max_clients();
                if current >= limit {
                    metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
                    let _ = stream.write_all(b"-ERR max number of clients reached\r\n");
                    drop(stream);
                    continue;
                }

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
                let server_clone = Arc::clone(&server);
                let id = next_client_id.fetch_add(1, Ordering::Relaxed);
                metrics.on_connect();
                metrics.total_connections_received.fetch_add(1, Ordering::Relaxed);
                let _ = thread::Builder::new()
                    .name(format!("client-{}", peer))
                    .spawn(move || {
                        handle_connection(
                            stream, shutdown, db, id, peer, registry, server_clone, tcp_port,
                        )
                    });
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

/// Accept loop for the TLS listener.
///
/// Mirrors `serve` but wraps each accepted `TcpStream` in a rustls
/// `ServerConnection` before handing off to `handle_connection_tls`. The
/// plain TCP accept loop is unaffected by this code path.
fn serve_tls(
    listener: TcpListener,
    tls_cfg: redis_core::tls::TlsConfig,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    next_client_id: Arc<AtomicU64>,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
) {
    for incoming in listener.incoming() {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        match incoming {
            Ok(stream) => {
                let metrics = server_metrics();
                let current = metrics.connected_clients.load(Ordering::Relaxed);
                let limit = redis_commands::connection::get_max_clients();
                if current >= limit {
                    metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
                    drop(stream);
                    continue;
                }
                if let Err(e) = stream.set_nodelay(true) {
                    eprintln!("redis-server: tls set_nodelay failed: {}", e);
                }
                let peer = stream
                    .peer_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                let tls_conn = match rustls::ServerConnection::new(
                    Arc::clone(&tls_cfg.server_config),
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("redis-server: tls ServerConnection::new failed for {}: {}", peer, e);
                        continue;
                    }
                };
                let tls_stream = Box::new(StreamOwned::new(tls_conn, stream));
                let conn = Connection::Tls(tls_stream);
                let shutdown2 = Arc::clone(&shutdown);
                let db2 = Arc::clone(&db);
                let registry2 = Arc::clone(&registry);
                let server2 = Arc::clone(&server);
                let id = next_client_id.fetch_add(1, Ordering::Relaxed);
                metrics.on_connect();
                metrics.total_connections_received.fetch_add(1, Ordering::Relaxed);
                let _ = thread::Builder::new()
                    .name(format!("tls-client-{}", peer))
                    .spawn(move || {
                        handle_connection_tls(conn, shutdown2, db2, id, peer, registry2, server2);
                    });
            }
            Err(e) => {
                eprintln!("redis-server: tls accept failed: {}", e);
                if shutdown.load(Ordering::SeqCst) {
                    return;
                }
            }
        }
    }
}

/// Spawn a writer thread that drains an `mpsc::Receiver<Vec<u8>>` and writes
/// each payload to the TCP stream. Returns the matching sender that the read
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

/// Per-connection event loop for plain TCP connections.
///
/// Reads from the socket, feeds the incremental parser, dispatches each
/// completed command, then ships replies through the outbound mpsc so the
/// dedicated writer thread owns all socket writes.
fn handle_connection(
    stream: TcpStream,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    id: u64,
    peer_addr: String,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
    tcp_port: u16,
) {
    let _ = tcp_port;
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
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.register(id, peer_addr.clone());
    }

    let mut client = Client::with_connection(Connection::Tcp(stream));
    client.id = id;
    client.addr = Some(peer_addr);
    client.authenticated_user = determine_initial_user();

    run_client_loop(&mut client, &outbound, shutdown, db, registry, server);
}

/// Per-connection event loop for TLS connections.
///
/// Unlike the plain TCP path, TLS state is owned by a single `StreamOwned`
/// and cannot be cloned. Replies are written synchronously from the read loop
/// thread; pub/sub payloads delivered via the outbound channel are drained
/// inline via `try_recv` between commands.
fn handle_connection_tls(
    conn: Connection,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    id: u64,
    peer_addr: String,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
) {
    let (tx, rx) = mpsc::channel::<Vec<u8>>();

    if let Ok(mut guard) = registry.lock() {
        guard.register_sender(id, tx.clone());
    }
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.register(id, peer_addr.clone());
    }

    let mut client = Client::with_connection(conn);
    client.id = id;
    client.addr = Some(peer_addr.clone());
    client.authenticated_user = determine_initial_user();

    run_client_loop_tls(&mut client, tx, rx, peer_addr, shutdown, db, registry, server);
}

/// Shared read-dispatch-write loop for plain TCP connections.
///
/// Parameterised over the outbound sender so both `handle_connection` (plain
/// TCP) can share the same loop body without code duplication.
fn run_client_loop(
    client: &mut Client,
    outbound: &Sender<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
) {
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
                    process_command(client, argv, &db, &registry, &server);
                }
                Ok(None) => break,
                Err(err) => {
                    queue_error_reply(client, &err);
                    let _ = flush_reply(client, outbound);
                    disconnect = true;
                    break;
                }
            }

            if !flush_reply(client, outbound) {
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

        if !flush_reply(client, outbound) {
            break;
        }

        if client.should_close {
            break;
        }
    }

    let id = client.id;
    let _ = pubsub::drop_client_from_registry(&registry, id);
    redis_core::replication::global_replication_state().remove_replica(id);
    client.clear_blocked_on_keys();
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.deregister(id);
    }
    server_metrics().on_disconnect();
}

/// Read-dispatch-write loop for TLS connections.
///
/// Because `rustls::StreamOwned` is not `Clone`, writes go through
/// `conn.write_all` on the same thread. The `rx` channel carries pub/sub
/// payloads from foreign threads; they are drained inline via `try_recv`
/// after each command so subscribers connected over TLS still receive
/// published messages.
fn run_client_loop_tls(
    client: &mut Client,
    outbound_tx: Sender<Vec<u8>>,
    outbound_rx: Receiver<Vec<u8>>,
    peer_addr: String,
    shutdown: Arc<AtomicBool>,
    db: Arc<Mutex<RedisDb>>,
    registry: Arc<Mutex<PubSubRegistry>>,
    server: Arc<redis_core::RedisServer>,
) {
    let mut read_buf = [0u8; 16 * 1024];

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        while let Ok(payload) = outbound_rx.try_recv() {
            let conn = match client.conn.as_mut() {
                Some(c) => c,
                None => break,
            };
            if conn.write_all(&payload).is_err() {
                break;
            }
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
                    process_command(client, argv, &db, &registry, &server);
                }
                Ok(None) => break,
                Err(err) => {
                    queue_error_reply(client, &err);
                    let reply = std::mem::take(&mut client.reply_buf);
                    if !reply.is_empty() {
                        if let Some(c) = client.conn.as_mut() {
                            let _ = c.write_all(&reply);
                        }
                    }
                    disconnect = true;
                    break;
                }
            }

            let reply = std::mem::take(&mut client.reply_buf);
            if !reply.is_empty() {
                match client.conn.as_mut() {
                    Some(c) => {
                        if c.write_all(&reply).is_err() {
                            disconnect = true;
                            break;
                        }
                    }
                    None => {
                        disconnect = true;
                        break;
                    }
                }
            }

            while let Ok(payload) = outbound_rx.try_recv() {
                match client.conn.as_mut() {
                    Some(c) => {
                        if c.write_all(&payload).is_err() {
                            disconnect = true;
                            break;
                        }
                    }
                    None => {
                        disconnect = true;
                        break;
                    }
                }
            }

            if disconnect || client.should_close {
                disconnect = true;
                break;
            }
        }

        if disconnect || client.should_close {
            break;
        }
    }

    let _ = peer_addr;
    let id = client.id;
    let _ = pubsub::drop_client_from_registry(&registry, id);
    redis_core::replication::global_replication_state().remove_replica(id);
    client.clear_blocked_on_keys();
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.deregister(id);
    }
    drop(outbound_tx);
    server_metrics().on_disconnect();
}

/// Install `argv` as the current command and route through the dispatcher.
///
/// If the previous command parked the client on the global blocked-keys
/// index, the wake/timeout reply has already gone out via the writer thread
/// before this fresh read returned bytes — clear the residual flag and any
/// surviving registry entry before dispatching the new command.
fn process_command(
    client: &mut Client,
    argv: Vec<RedisString>,
    _db: &Arc<Mutex<RedisDb>>,
    registry: &Arc<Mutex<PubSubRegistry>>,
    server: &Arc<redis_core::RedisServer>,
) {
    client.clear_blocked_on_keys();
    client.set_args(argv);

    let cmd_name = client
        .arg(0)
        .map(|a| {
            core::str::from_utf8(a.as_bytes())
                .unwrap_or("")
                .to_ascii_lowercase()
        })
        .unwrap_or_default();
    if let Ok(mut guard) = client_info_registry().lock() {
        guard.set_cmd(client.id, &cmd_name);
        guard.set_db(client.id, client.db_index);
    }

    let metrics = server_metrics();
    metrics.total_commands_processed.fetch_add(1, Ordering::Relaxed);
    let t0 = SystemTime::now();
    let result = {
        let selected_db = global_databases().get(client.db_index);
        let mut guard = match selected_db.lock() {
            Ok(g) => g,
            Err(poison) => poison.into_inner(),
        };
        let mut ctx = CommandContext::with_server(
            client,
            &mut guard,
            Arc::clone(server),
            Arc::clone(registry),
        );
        let r = dispatch(&mut ctx);
        let deferred: Vec<RedisString> = std::mem::take(&mut ctx.client_mut().pending_wakes);
        for key in &deferred {
            redis_commands::list::wake_blocked_for_key(&mut guard, key);
        }
        r
    };
    let elapsed_us = t0
        .elapsed()
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    metrics
        .active_time_main_thread_us
        .fetch_add(elapsed_us, Ordering::Relaxed);
    if client.blocked_on_keys {
        if let Ok(mut guard) = client_info_registry().lock() {
            guard.set_blocked(client.id, true);
        }
    }
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

/// Determine the initial authenticated username for a newly accepted connection.
///
/// If the global default ACL user is enabled and has `nopass`, the client
/// starts pre-authenticated as `default`. Otherwise the client must AUTH before
/// running commands.
fn determine_initial_user() -> Option<RedisString> {
    let acl = redis_core::acl::global_acl_state();
    let default_key = RedisString::from_bytes(b"default");
    let guard = match acl.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if let Some(user) = guard.users.get(&default_key) {
        if user.flags.enabled && user.flags.nopass {
            return Some(default_key);
        }
    }
    None
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
