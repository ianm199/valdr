//! `redis-server` binary entry point — Wave A scaffolding.
//! Plain TCP binds a port and runs through the `mio` readiness-backed
//! `RuntimeOwner` loop: one owner accepts sockets, parses RESP requests,
//! dispatches through `redis-commands`, and flushes replies.
//! TLS transport migration is still human-gated. Once the owner loop owns
//! live DB vector, this binary refuses to start the old TLS command path rather
//! than letting TLS commands mutate a divergent global DB.
//! Out of scope for Wave A:
//! * Tokio/raw pollers; plain TCP uses `mio`, TLS keeps the older path.
//! * Cluster, modules, and full TLS socket migration.

/// Use jemalloc as the process global allocator. Valkey ships jemalloc; this
/// reference build falls back to libc malloc, and a default Rust binary uses
/// system allocator. jemalloc's thread-local arenas and size classes cut
/// per-element heap-allocation cost that dominates collection-write commands
/// (SADD/HSET/ZADD/SPOP/ZPOPMIN), where every member/field is its own
/// `RedisString` allocation. The `unsafe impl GlobalAlloc` lives in
/// allocator crate, not here, so this stays within the crate unsafe budget.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::io::{self, Write};
use std::net::{IpAddr, SocketAddr, TcpListener};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::mpsc::{self};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use redis_core::db::RedisDb;
use redis_core::expire::active_expire_config;
use redis_core::lru_clock::spawn_lru_clock_thread;
use redis_core::metrics::server_metrics;
use redis_core::pubsub_registry::PubSubRegistry;
use redis_types::RedisString;

mod runtime_owner;

mod check_aof;
mod cli;
mod startup;

// ── Re-exports from extracted modules (refactor/file-structure-splits) ──────
// runtime_owner uses `super::<fn>` for these; they now live in sibling modules.
pub(crate) use cli::*;
pub(crate) use startup::*;

pub(crate) const DEFAULT_PORT: u16 = 6379;
pub(crate) const DEFAULT_BIND: &str = "127.0.0.1";
pub(crate) const ACTIVE_TIME_SAMPLE_INTERVAL: u64 = 1024;
pub(crate) const MAX_UNAUTHENTICATED_MULTIBULK_LEN: i64 = 10;
pub(crate) const MAX_UNAUTHENTICATED_BULK_LEN: i64 = 16 * 1024;

pub(crate) static RENAMED_READY_KEYS: OnceLock<Mutex<Vec<(u32, RedisString)>>> = OnceLock::new();
pub(crate) static RENAMED_READY_KEYS_PENDING: AtomicBool = AtomicBool::new(false);

fn main() {
    let _clock = redis_core::monotonic::monotonic_init();
    let argv: Vec<String> = std::env::args().collect();

    let prog = argv
        .first()
        .map(|p| {
            Path::new(p)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string()
        })
        .unwrap_or_default();
    if prog.contains("check-rdb") {
        std::process::exit(run_check_rdb(&argv[1..]));
    }
    if prog.contains("check-aof") {
        std::process::exit(check_aof::run_check_aof(&argv[1..]));
    }

    let args = match parse_args(argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("redis-server: {}", e);
            std::process::exit(1);
        }
    };

    let mut listeners: Vec<TcpListener> = Vec::with_capacity(args.bind.len());
    for bind in &args.bind {
        let bind_ip: IpAddr = match bind.parse() {
            Ok(ip) => ip,
            Err(_) => {
                eprintln!(
                    "redis-server: --bind expects IP literals (got '{}'); hostnames not yet supported",
                    bind
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
        if let Err(e) = listener.set_nonblocking(true) {
            eprintln!("redis-server: set_nonblocking(true) failed: {}", e);
        }
        eprintln!("redis-server: listening on {}", addr);
        listeners.push(listener);
    }
    if listeners.is_empty() {
        eprintln!("redis-server: no TCP bind addresses configured");
        if args.unixsocket.is_none() {
            std::process::exit(1);
        }
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    install_shutdown_handler(Arc::clone(&shutdown));
    #[cfg(unix)]
    if let Some(path) = args.unixsocket.clone() {
        spawn_unix_control_listener(
            path,
            args.unixsocketperm,
            args.unixsocketgroup.clone(),
            Arc::clone(&shutdown),
        );
    }
    emit_startup_log();

    server_metrics().set_tcp_port(args.port);
    redis_commands::connection::set_tcp_port_config(args.port);

    let live_config = Arc::new(redis_core::live_config::LiveConfig::new());
    let live_config_for_hook = Arc::clone(&live_config);
    live_config.set_maxclients(args.maxclients);
    live_config.set_maxmemory(args.maxmemory);
    live_config.set_maxmemory_policy(args.maxmemory_policy);
    live_config.set_rdb_dir(args.dir.clone());
    live_config.set_rdb_filename(args.dbfilename.clone());
    live_config.set_appendonly(args.appendonly);
    live_config.set_appendfilename(args.appendfilename.clone());
    live_config.set_appenddirname(args.appenddirname.clone());
    live_config.set_appendfsync(args.appendfsync);
    live_config.set_aof_load_truncated(args.aof_load_truncated);
    live_config.set_aof_use_rdb_preamble(args.aof_use_rdb_preamble);
    live_config.set_rdb_version_check_relaxed(args.rdb_version_check_relaxed);
    live_config.set_auto_aof_rewrite_percentage(args.auto_aof_rewrite_percentage);
    live_config.set_auto_aof_rewrite_min_size(args.auto_aof_rewrite_min_size);
    live_config.set_hash_max_listpack_entries(args.hash_max_listpack_entries);
    live_config.set_hash_max_listpack_value(args.hash_max_listpack_value);
    live_config.set_list_max_listpack_size(args.list_max_listpack_size);
    live_config.store_set_max_intset_entries(args.set_max_intset_entries);
    live_config.store_set_max_listpack_entries(args.set_max_listpack_entries);
    live_config.store_set_max_listpack_value(args.set_max_listpack_value);
    live_config.set_zset_max_listpack_entries(args.zset_max_listpack_entries);
    live_config.set_zset_max_listpack_value(args.zset_max_listpack_value);
    live_config.set_lua_enable_insecure_api(args.lua_enable_insecure_api);
    live_config.set_lua_time_limit_ms(args.lua_time_limit_ms);
    if let Some(secret) = &args.requirepass {
        live_config.set_requirepass(Some(redis_types::RedisString::from_bytes(
            secret.as_bytes(),
        )));
    }
    if args.acl_pubsub_default_allchannels {
        redis_core::acl::set_acl_pubsub_default(b"allchannels");
    } else {
        redis_core::acl::set_acl_pubsub_default(b"resetchannels");
    }
    redis_core::object::install_live_config(Arc::clone(&live_config));
    redis_commands::install_live_config_handle(Arc::clone(&live_config));
    redis_commands::connection::set_config_file_path(args.config_path.clone());
    for (key, value) in &args.startup_config_overrides {
        redis_commands::connection::set_startup_config_override(key, value);
        if key.eq_ignore_ascii_case("save") {
            live_config.set_save_enabled(value.split_whitespace().next().is_some());
        } else if key.eq_ignore_ascii_case("repl-diskless-sync") {
            live_config.set_repl_diskless_sync(value.eq_ignore_ascii_case("yes"));
        }
    }
    redis_core::acl::install_acl_state();
    for (from, to) in &args.command_renames {
        if let Err(e) =
            redis_commands::dispatch::apply_command_rename(from.as_bytes(), to.as_bytes())
        {
            eprintln!("{}", String::from_utf8_lossy(&e));
            std::process::exit(1);
        }
    }
    if let Err(e) = redis_commands::connection::load_acl_startup_config(
        &args.acl_user_lines,
        &args.dir,
        args.aclfile.as_deref(),
    ) {
        eprintln!("{}", String::from_utf8_lossy(&e));
        std::process::exit(1);
    }
    if let Some(secret) = args.requirepass.as_deref() {
        redis_commands::connection::apply_requirepass_to_acl(Some(secret.as_bytes()));
    }
    let repl_state = Arc::new(redis_core::replication::ReplicationState::new(
        redis_core::replication::generate_runid(),
        live_config.repl_backlog_size() as usize,
    ));
    redis_core::replication::install_replication_state(Arc::clone(&repl_state));

    let server = Arc::new(redis_core::RedisServer::with_live_config(
        args.port,
        Arc::clone(&live_config),
    ));
    spawn_signal_shutdown_watcher(Arc::clone(&server), Arc::clone(&live_config));

    let (replica_apply_tx, replica_apply_rx) =
        mpsc::channel::<redis_commands::replica_dialer::ReplicaApplyRequest>();
    redis_commands::replica_dialer::install_runtime_apply_sender(replica_apply_tx);
    redis_commands::replica_dialer::install_dialer_resources(
        Arc::clone(&server),
        args.port,
        args.dir.clone(),
    );

    let mut owner_dbs: Vec<RedisDb> = (0..runtime_owner::DEFAULT_DATABASE_COUNT)
        .map(RedisDb::new)
        .collect();

    server.persistence.set_loading(true);
    let mut loaded_persistence = false;
    if args.appendonly {
        let load_options = redis_commands::aof::AofLoadOptions {
            load_truncated: args.aof_load_truncated,
            allow_rdb_preamble: args.aof_use_rdb_preamble,
            lua_time_limit_ms: args.lua_time_limit_ms,
        };
        match redis_commands::aof::load_append_only_files(
            Path::new(&args.dir),
            &args.appendfilename,
            &args.appenddirname,
            &mut owner_dbs,
            load_options,
        ) {
            Ok(Some((n, _size))) => {
                loaded_persistence = true;
                eprintln!("redis-server: AOF replay: {} commands", n);
            }
            Ok(None) => {}
            Err(e) => {
                log_aof_replay_error(&e, &args.appendfilename);
                std::process::exit(1);
            }
        };
        match redis_commands::aof::open_manifest_current_incr_writer(
            Path::new(&args.dir),
            &args.appendfilename,
            &args.appenddirname,
            &owner_dbs,
            args.appendfsync,
        ) {
            Ok((w, base_size, current_size)) => {
                server.persistence.set_aof_base_size(base_size);
                server.persistence.set_aof_current_size(current_size);
                server.set_aof_state(redis_core::AofState::On);
                redis_commands::aof::install_aof_writer(Arc::new(w));
                let cleanup = redis_commands::aof::cleanup_aof_appenddir(
                    Path::new(&args.dir),
                    &args.appendfilename,
                    &args.appenddirname,
                );
                if cleanup.did_work() {
                    eprintln!(
                        "redis-server: AOF cleanup inspected {} files, preserved {} referenced files, removed {} temp files and {} orphaned AOF files",
                        cleanup.inspected_files,
                        cleanup.preserved_referenced_files,
                        cleanup.removed_temp_files,
                        cleanup.removed_orphaned_aof_files
                    );
                    for err in cleanup.errors {
                        eprintln!("redis-server: {}", err);
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "redis-server: failed to open AOF manifest layout {}: {}",
                    Path::new(&args.dir).join(&args.appenddirname).display(),
                    e
                );
                std::process::exit(1);
            }
        }
        redis_commands::aof::spawn_fsync_thread();
    } else if !args.rdb_disabled {
        let rdb_path =
            redis_core::rdb::rdb_path(&live_config.rdb_dir(), &live_config.rdb_filename());
        if rdb_path.exists() {
            loaded_persistence = true;
            let rdb_options = redis_core::rdb::RdbLoadOptions {
                relaxed_version_check: live_config.rdb_version_check_relaxed(),
                ..Default::default()
            };
            match redis_core::rdb::load_replacement_plan_with_options(
                owner_dbs.len(),
                &rdb_path,
                rdb_options,
            ) {
                Ok(plan) => {
                    let functions = match redis_commands::eval::prepare_rdb_function_replacement(
                        &plan.outcome.function_payloads,
                    ) {
                        Ok(functions) => functions,
                        Err(e) => {
                            println!(
                                "redis-server: RDB function load failed ({}): {}",
                                rdb_path.display(),
                                e
                            );
                            println!(
                                "redis-server: Fatal error loading the DB, check server logs. Exiting."
                            );
                            let _ = io::stdout().flush();
                            std::process::exit(1);
                        }
                    };
                    let stats = redis_core::rdb::last_load_stats();
                    server
                        .persistence
                        .set_rdb_last_load_stats(stats.keys_expired, stats.keys_loaded);
                    let msg = plan.outcome.message;
                    owner_dbs = plan.dbs;
                    redis_commands::eval::install_rdb_function_replacement(functions);
                    println!("redis-server: {}", msg);
                }
                Err(e) => {
                    println!(
                        "redis-server: RDB load failed ({}): {}",
                        rdb_path.display(),
                        e
                    );
                    println!(
                        "redis-server: Fatal error loading the DB, check server logs. Exiting."
                    );
                    let _ = io::stdout().flush();
                    std::process::exit(1);
                }
            }
        }
    }
    if args.key_load_delay > 0 && loaded_persistence {
        let server_for_loading = Arc::clone(&server);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(30));
            server_for_loading.persistence.set_loading(false);
        });
    } else {
        server.persistence.set_loading(false);
    }

    let next_client_id = Arc::new(AtomicU64::new(1));
    let registry = Arc::new(Mutex::new(PubSubRegistry::new()));
    redis_core::db::install_global_notify_handle(Arc::clone(&registry), Arc::clone(&live_config));
    redis_core::db::install_swapdb_wake_hook(Box::new(|other_db_id| {
        redis_commands::wake_blocked_after_swapdb(other_db_id, other_db_id);
    }));
    install_deferred_rename_ready_hook();
    redis_commands::stream::install_stream_hooks();
    spawn_blocked_timeout_thread(Arc::clone(&shutdown));
    let active_expire_cfg = Arc::clone(active_expire_config());
    active_expire_cfg.set_effort(live_config.active_expire_effort());
    active_expire_cfg.set_hz(live_config.hz());
    let _ = spawn_lru_clock_thread();
    spawn_bgsave_reaper(Arc::clone(&server), Arc::clone(&live_config));
    spawn_repl_bgsave_reaper();

    let bind_addrs_for_port_hook = args.bind.clone();
    redis_commands::connection::install_tcp_port_set_hook(Box::new(move |port| {
        if port == 0 {
            return Ok(Vec::new());
        }
        let mut listeners = Vec::with_capacity(bind_addrs_for_port_hook.len());
        for bind in &bind_addrs_for_port_hook {
            let bind_ip: IpAddr = bind
                .parse()
                .map_err(|_| b"ERR Failed to bind to specified addresses".to_vec())?;
            let addr = SocketAddr::new(bind_ip, port);
            let listener = TcpListener::bind(addr)
                .map_err(|_| b"ERR Unable to listen on this port".to_vec())?;
            listener
                .set_nonblocking(true)
                .map_err(|_| b"ERR Unable to listen on this port".to_vec())?;
            eprintln!("redis-server: queued dynamic listener on {}", addr);
            listeners.push(listener);
        }
        Ok(listeners)
    }));

    redis_commands::connection::install_tcp_bind_set_hook(Box::new(move |value, port| {
        let text = std::str::from_utf8(value)
            .map_err(|_| b"ERR Failed to bind to specified addresses".to_vec())?;
        let trimmed = text.trim();
        if trimmed.is_empty() || port == 0 {
            return Ok(Vec::new());
        }
        let mut listeners = Vec::new();
        for raw in trimmed.split_whitespace() {
            if raw == "-::*" {
                continue;
            }
            let bind_text = if raw == "*" { "0.0.0.0" } else { raw };
            let bind_ip: IpAddr = bind_text
                .parse()
                .map_err(|_| b"ERR Failed to bind to specified addresses".to_vec())?;
            let addr = SocketAddr::new(bind_ip, port);
            let listener = TcpListener::bind(addr)
                .map_err(|_| b"ERR Failed to bind to specified addresses".to_vec())?;
            listener
                .set_nonblocking(true)
                .map_err(|_| b"ERR Failed to bind to specified addresses".to_vec())?;
            eprintln!("redis-server: queued bind listener on {}", addr);
            listeners.push(listener);
        }
        Ok(listeners)
    }));

    redis_core::tls::install_tls_start_hook(Box::new(move |_port| {
        match redis_core::tls::rebuild_from_live(&live_config_for_hook) {
            Ok(Some(cfg)) => {
                redis_core::tls::set_current_server_config(Some(cfg.server_config));
            }
            Ok(None) => {
                redis_core::tls::set_current_server_config(None);
            }
            Err(e) => {
                eprintln!(
                    "redis-server: TLS reconfiguration failed: {}; previous config retained",
                    e
                );
            }
        }
    }));

    // Build the TLS listener(s) + rustls config from startup config (the same
    // tls-* keys the upstream test harness passes). TLS connections are served
    // by the same RuntimeOwner / DB as plain TCP (the divergent-DB path is gone).
    let (tls_listeners, tls_config) = build_tls_startup(&args, &live_config);

    runtime_owner::RuntimeOwner::run_plain_tcp(
        listeners,
        shutdown,
        next_client_id,
        registry,
        server,
        args.port,
        tls_listeners,
        tls_config,
        owner_dbs,
        replica_apply_rx,
    );
}

fn log_aof_replay_error(err: &io::Error, appendfilename: &str) {
    let message = err.to_string();
    let lower = message.to_ascii_lowercase();
    if lower.contains("unknown command") {
        println!("{}", message);
    }
    match err.kind() {
        io::ErrorKind::UnexpectedEof => {
            println!(
                "Unexpected end of file reading the append only file {}. You can: 1) Make a backup of your AOF file, then use ./valkey-check-aof --fix <filename.manifest>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.",
                appendfilename
            );
        }
        io::ErrorKind::InvalidData => {
            println!(
                "Bad file format reading the append only file {}: make a backup of your AOF file, then use ./valkey-check-aof --fix <filename.manifest>",
                appendfilename
            );
        }
        _ => {}
    }
    eprintln!("redis-server: AOF replay failed: {}", message);
}
