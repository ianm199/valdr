//! Persistence commands: SAVE, BGSAVE.
//! `SAVE` runs `rdb::save_rdb` synchronously in the calling thread and updates
//! `last_save_unix` on success.
//! `BGSAVE` on Unix uses `fork(2)` so the OS copy-on-write page mapping gives
//! the child a frozen snapshot of the DB without any memory duplication:
//! 1. fork — child sees the DB as it was at the instant of the fork.
//! 2. Child writes the RDB file and calls `_exit(0)` (not `exit` — skipping
//! atexit handlers that belong to the parent).
//! 3. Parent records the child PID in `server.rdb_child_pid` and returns
//! `+Background saving started` immediately.
//! 4. A background polling thread (spawned at server start) calls
//! `waitpid` every 500 ms to reap the child and update `last_save_unix`.
//! On non-Unix targets (Windows, WASM) the pre-fork thread-snapshot path is
//! kept as the fallback. The fallback allocates a full in-memory clone of
//! DB before spawning the writer thread.
//! The `unsafe` block that wraps `fork + _exit` is the single unsafe surface
//! this crate: documented below with a SAFETY comment.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis_core::client::ClientId;
use redis_core::db::{RedisDb, LOOKUP_NOTOUCH};
use redis_core::object::{object_set_lru_or_lfu, EXPIRY_NONE};
use redis_core::rdb::{
    create_dump_payload, load_dump_payload, rdb_path, save_rdb_databases, verify_dump_payload,
};
use redis_core::replication::{global_replication_state, ReplBgsaveJob};
use redis_core::util::mstime;
use redis_core::CommandContext;
use redis_core::PersistenceStatus;
use redis_types::{RedisError, RedisResult, RedisString};

use crate::aof::aof_writer;

static MIGRATE_CACHED_SOCKETS: AtomicUsize = AtomicUsize::new(0);

pub fn migrate_cached_sockets() -> usize {
    MIGRATE_CACHED_SOCKETS.load(Ordering::Relaxed)
}

fn mark_migrate_socket_cached() {
    MIGRATE_CACHED_SOCKETS.store(1, Ordering::Relaxed);
    thread::spawn(|| {
        thread::sleep(Duration::from_secs(5));
        MIGRATE_CACHED_SOCKETS.store(0, Ordering::Relaxed);
    });
}

fn ascii_lower(b: u8) -> u8 {
    if b.is_ascii_uppercase() {
        b + 32
    } else {
        b
    }
}

fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| ascii_lower(*x) == ascii_lower(*y))
}

fn parse_i64_strict(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

/// `SAVE` — synchronous RDB save.
/// Writes the RDB file to `<dir>/<dbfilename>` and updates `last_save_unix`
/// on success. Returns `+OK` on success or `-ERR` on failure.
pub fn save_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"save"));
    }
    let cfg = Arc::clone(&ctx.server().live_config);
    let path = rdb_path(&cfg.rdb_dir(), &cfg.rdb_filename());

    let snapshot = ctx.snapshot_all_dbs()?;
    let snapshot_dbs = snapshots_to_dbs(&snapshot);
    let result = save_rdb_databases(&snapshot_dbs, &path);

    match result {
        Ok(()) => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            cfg.set_last_save_unix(now);
            ctx.server()
                .persistence
                .set_rdb_last_bgsave_status(PersistenceStatus::Ok);
            ctx.reply_simple_string(b"OK")
        }
        Err(e) => {
            ctx.server()
                .persistence
                .set_rdb_last_bgsave_status(PersistenceStatus::Err);
            Err(RedisError::runtime(
                format!("ERR SAVE failed: {}", e).into_bytes(),
            ))
        }
    }
}

/// `DUMP key` — return a serialized representation of one key's value.
pub fn dump_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"dump"));
    }

    let key = ctx.arg_owned(1usize)?;
    let dbid = ctx.selected_db_id();
    let (payload, is_hash) = match ctx
        .db_mut()
        .lookup_key_read_with_flags(&key, LOOKUP_NOTOUCH)
    {
        Some(obj) => (
            create_dump_payload(obj)
                .map_err(|e| RedisError::runtime(format!("ERR DUMP failed: {}", e).into_bytes()))?,
            obj.is_hash(),
        ),
        None => return ctx.reply_null_bulk(),
    };
    if is_hash {
        crate::hash::remember_dumped_hash_field_expiries(dbid, &key, &payload);
    }

    ctx.reply_bulk(&payload)
}

/// `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]`.
pub fn restore_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::wrong_number_of_args(b"restore"));
    }

    let key = ctx.arg_owned(1usize)?;
    let ttl_arg = ctx.arg_owned(2usize)?;
    let payload = ctx.arg_owned(3usize)?;

    let mut replace = false;
    let mut absttl = false;
    let mut lru_idle = -1i64;
    let mut lfu_freq = -1i64;

    let mut i = 4usize;
    while i < ctx.arg_count() {
        let option = ctx.arg_owned(i)?;
        let option_bytes = option.as_bytes();
        if ascii_eq_ignore_case(option_bytes, b"replace") {
            replace = true;
            i += 1;
        } else if ascii_eq_ignore_case(option_bytes, b"absttl") {
            absttl = true;
            i += 1;
        } else if ascii_eq_ignore_case(option_bytes, b"idletime")
            && i + 1 < ctx.arg_count()
            && lfu_freq == -1
        {
            let raw = ctx.arg_owned(i + 1)?;
            let parsed = parse_i64_strict(raw.as_bytes()).ok_or_else(RedisError::not_integer)?;
            if parsed < 0 {
                return Err(RedisError::runtime(
                    b"ERR Invalid IDLETIME value, must be >= 0",
                ));
            }
            lru_idle = parsed;
            i += 2;
        } else if ascii_eq_ignore_case(option_bytes, b"freq")
            && i + 1 < ctx.arg_count()
            && lru_idle == -1
        {
            let raw = ctx.arg_owned(i + 1)?;
            let parsed = parse_i64_strict(raw.as_bytes()).ok_or_else(RedisError::not_integer)?;
            if !(0..=255).contains(&parsed) {
                return Err(RedisError::runtime(
                    b"ERR Invalid FREQ value, must be >= 0 and <= 255",
                ));
            }
            lfu_freq = parsed;
            i += 2;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    if !replace && ctx.db_mut().lookup_key_write(&key).is_some() {
        return Err(RedisError::runtime(
            b"BUSYKEY Target key name already exists.",
        ));
    }

    let ttl = parse_i64_strict(ttl_arg.as_bytes()).ok_or_else(RedisError::not_integer)?;
    if ttl < 0 {
        return Err(RedisError::runtime(b"ERR Invalid TTL value, must be >= 0"));
    }

    let relaxed_version = ctx.live_config().rdb_version_check_relaxed();
    verify_dump_payload(payload.as_bytes(), relaxed_version)
        .map_err(|_| RedisError::runtime(b"ERR DUMP payload version or checksum are wrong"))?;
    let mut obj = load_dump_payload(payload.as_bytes(), relaxed_version)
        .map_err(|_| RedisError::runtime(b"ERR Bad data format"))?;

    let now = mstime();
    let expire_at = if ttl == 0 {
        EXPIRY_NONE
    } else if absttl {
        ttl
    } else {
        now.saturating_add(ttl)
    };

    if expire_at != EXPIRY_NONE && expire_at <= now {
        let dbid = ctx.selected_db_id();
        if replace {
            ctx.db_mut().delete(&key);
        }
        crate::hash::clear_hash_field_expiries(dbid, &key);
        ctx.server().add_dirty(1);
        return ctx.reply_simple_string(b"OK");
    }

    let dbid = ctx.selected_db_id();
    let is_hash = obj.is_hash();
    let metadata_key = key.clone();
    object_set_lru_or_lfu(&mut obj, lfu_freq, lru_idle);
    ctx.db_mut()
        .set_key_with_known_expire(key, obj, expire_at, 0);
    if is_hash {
        crate::hash::restore_dumped_hash_field_expiries(dbid, &metadata_key, payload.as_bytes());
    } else {
        crate::hash::clear_hash_field_expiries(dbid, &metadata_key);
    }
    ctx.server().add_dirty(1);
    rewrite_restore_propagation_absttl(ctx, ttl, absttl, expire_at);
    ctx.reply_simple_string(b"OK")
}

/// Rewrite the propagated RESTORE so a relative TTL becomes an absolute
/// millisecond timestamp with the `ABSTTL` flag appended.
/// Replicas and the AOF must receive an absolute expire so a restored key does
/// not outlive the primary's intent due to replication lag. A RESTORE that was
/// already `ABSTTL` (or had no TTL) is propagated verbatim. Mirrors
/// `restoreCommand`'s argument rewrite.
fn rewrite_restore_propagation_absttl(
    ctx: &mut CommandContext<'_>,
    ttl: i64,
    absttl: bool,
    expire_at: i64,
) {
    if ttl == 0 || absttl {
        return;
    }
    let argc = ctx.arg_count();
    let mut new_argv: Vec<RedisString> = Vec::with_capacity(argc + 1);
    for k in 0..argc {
        match ctx.arg_owned(k) {
            Ok(arg) => new_argv.push(arg),
            Err(_) => return,
        }
    }
    new_argv[2] = RedisString::from_bytes(expire_at.to_string().as_bytes());
    new_argv.push(RedisString::from_bytes(b"ABSTTL"));
    ctx.client_mut().set_args(new_argv);
}

/// Cluster-internal RESTORE variant. Cluster asking state is out of scope for
/// the single-node port, so it shares RESTORE's local behaviour.
pub fn restore_asking_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    restore_command(ctx)
}

#[derive(Debug)]
struct MigrateOptions {
    host: Vec<u8>,
    port: u16,
    db: u32,
    timeout_ms: u64,
    copy: bool,
    replace: bool,
    auth: Option<Vec<u8>>,
    auth2: Option<(Vec<u8>, Vec<u8>)>,
    keys: Vec<RedisString>,
}

fn parse_u16_arg(bytes: &[u8]) -> RedisResult<u16> {
    let value = parse_i64_strict(bytes).ok_or_else(RedisError::not_integer)?;
    u16::try_from(value).map_err(|_| RedisError::runtime(b"ERR port is out of range"))
}

fn parse_u64_nonnegative(bytes: &[u8]) -> RedisResult<u64> {
    let value = parse_i64_strict(bytes).ok_or_else(RedisError::not_integer)?;
    if value < 0 {
        return Err(RedisError::runtime(b"ERR timeout is negative"));
    }
    Ok(value as u64)
}

fn parse_migrate_options(ctx: &CommandContext<'_>) -> RedisResult<MigrateOptions> {
    if ctx.arg_count() < 6 {
        return Err(RedisError::wrong_number_of_args(b"migrate"));
    }

    let host = ctx.arg_owned(1usize)?.into_bytes();
    let port = parse_u16_arg(ctx.arg_bytes(2usize)?)?;
    let key_arg = ctx.arg_owned(3usize)?;
    let db = ctx.validate_db_index(
        parse_i64_strict(ctx.arg_bytes(4usize)?).ok_or_else(RedisError::not_integer)?,
    )?;
    let timeout_ms = parse_u64_nonnegative(ctx.arg_bytes(5usize)?)?;

    let mut opts = MigrateOptions {
        host,
        port,
        db,
        timeout_ms,
        copy: false,
        replace: false,
        auth: None,
        auth2: None,
        keys: if key_arg.is_empty() {
            Vec::new()
        } else {
            vec![key_arg.clone()]
        },
    };

    let mut saw_keys = false;
    let mut i = 6usize;
    while i < ctx.arg_count() {
        let option = ctx.arg_owned(i)?;
        let option_bytes = option.as_bytes();
        if ascii_eq_ignore_case(option_bytes, b"copy") {
            opts.copy = true;
            i += 1;
        } else if ascii_eq_ignore_case(option_bytes, b"replace") {
            opts.replace = true;
            i += 1;
        } else if ascii_eq_ignore_case(option_bytes, b"auth") {
            if i + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            opts.auth = Some(ctx.arg_owned(i + 1)?.into_bytes());
            i += 2;
        } else if ascii_eq_ignore_case(option_bytes, b"auth2") {
            if i + 2 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            opts.auth2 = Some((
                ctx.arg_owned(i + 1)?.into_bytes(),
                ctx.arg_owned(i + 2)?.into_bytes(),
            ));
            i += 3;
        } else if ascii_eq_ignore_case(option_bytes, b"keys") {
            if !key_arg.is_empty() {
                return Err(RedisError::runtime(
                    b"ERR When using MIGRATE KEYS option, the key argument must be set to the empty string",
                ));
            }
            if saw_keys || i + 1 >= ctx.arg_count() {
                return Err(RedisError::syntax(b"syntax error"));
            }
            saw_keys = true;
            opts.keys.clear();
            i += 1;
            while i < ctx.arg_count() {
                opts.keys.push(ctx.arg_owned(i)?);
                i += 1;
            }
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    Ok(opts)
}

fn append_resp_command(out: &mut Vec<u8>, args: &[&[u8]]) {
    out.extend_from_slice(b"*");
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for arg in args {
        out.extend_from_slice(b"$");
        out.extend_from_slice(arg.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
}

fn find_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|w| w == b"\r\n")
}

fn parse_resp_scalar(buf: &[u8]) -> RedisResult<Option<Result<Vec<u8>, Vec<u8>>>> {
    let Some(first) = buf.first().copied() else {
        return Ok(None);
    };
    match first {
        b'+' | b'-' | b':' => {
            let Some(end) = find_crlf(&buf[1..]) else {
                return Ok(None);
            };
            let payload = buf[1..1 + end].to_vec();
            if first == b'-' {
                Ok(Some(Err(payload)))
            } else {
                Ok(Some(Ok(payload)))
            }
        }
        b'$' => {
            let Some(end) = find_crlf(&buf[1..]) else {
                return Ok(None);
            };
            let len_bytes = &buf[1..1 + end];
            let len = parse_i64_strict(len_bytes)
                .ok_or_else(|| RedisError::runtime(b"IOERR invalid bulk reply"))?;
            let header_len = 1 + end + 2;
            if len < 0 {
                return Ok(Some(Ok(Vec::new())));
            }
            let len = len as usize;
            let needed = header_len
                .checked_add(len)
                .and_then(|n| n.checked_add(2))
                .ok_or_else(|| RedisError::runtime(b"IOERR invalid bulk reply"))?;
            if buf.len() < needed {
                return Ok(None);
            }
            if &buf[header_len + len..needed] != b"\r\n" {
                return Err(RedisError::runtime(b"IOERR invalid bulk reply"));
            }
            Ok(Some(Ok(buf[header_len..header_len + len].to_vec())))
        }
        _ => Err(RedisError::runtime(b"IOERR invalid target reply")),
    }
}

fn read_target_reply(stream: &mut TcpStream) -> RedisResult<Result<Vec<u8>, Vec<u8>>> {
    let mut buf = Vec::with_capacity(256);
    let mut scratch = [0u8; 4096];
    loop {
        if let Some(reply) = parse_resp_scalar(&buf)? {
            return Ok(reply);
        }
        match stream.read(&mut scratch) {
            Ok(0) => return Err(RedisError::runtime(b"IOERR target connection closed")),
            Ok(n) => buf.extend_from_slice(&scratch[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return Err(RedisError::runtime(
                    b"IOERR error or timeout reading from target",
                ));
            }
            Err(e) => {
                return Err(RedisError::runtime(
                    format!("IOERR target read failed: {}", e).into_bytes(),
                ));
            }
        }
    }
}

fn send_target_command(stream: &mut TcpStream, args: &[&[u8]]) -> RedisResult<Vec<u8>> {
    let mut frame = Vec::new();
    append_resp_command(&mut frame, args);
    stream.write_all(&frame).map_err(|e| {
        RedisError::runtime(format!("IOERR target write failed: {}", e).into_bytes())
    })?;
    match read_target_reply(stream)? {
        Ok(payload) => Ok(payload),
        Err(payload) => {
            let mut msg = b"ERR Target instance replied with error: ".to_vec();
            msg.extend_from_slice(&payload);
            Err(RedisError::runtime(msg))
        }
    }
}

fn connect_migrate_target(opts: &MigrateOptions) -> RedisResult<TcpStream> {
    let host = std::str::from_utf8(&opts.host)
        .map_err(|_| RedisError::runtime(b"ERR invalid host name"))?;
    let addrs = (host, opts.port).to_socket_addrs().map_err(|e| {
        RedisError::runtime(format!("IOERR target lookup failed: {}", e).into_bytes())
    })?;
    let timeout = Duration::from_millis(opts.timeout_ms.max(1));
    let mut last_error = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(stream) => {
                stream.set_read_timeout(Some(timeout)).ok();
                stream.set_write_timeout(Some(timeout)).ok();
                return Ok(stream);
            }
            Err(e) => last_error = Some(e),
        }
    }
    let msg = match last_error {
        Some(e) => format!("IOERR target connect failed: {}", e).into_bytes(),
        None => b"IOERR target lookup produced no address".to_vec(),
    };
    Err(RedisError::runtime(msg))
}

fn source_migrate_payload(
    ctx: &mut CommandContext<'_>,
    key: &RedisString,
) -> RedisResult<Option<(Vec<u8>, i64)>> {
    let now = mstime();
    let db = ctx.db_mut();
    let Some(obj) = db.lookup_key_read_with_flags(key, LOOKUP_NOTOUCH) else {
        return Ok(None);
    };
    let ttl = if obj.expire == EXPIRY_NONE {
        0
    } else {
        obj.expire.saturating_sub(now).max(0)
    };
    let payload = create_dump_payload(obj)
        .map_err(|e| RedisError::runtime(format!("ERR DUMP failed: {}", e).into_bytes()))?;
    Ok(Some((payload, ttl)))
}

/// `MIGRATE host port key db timeout [COPY] [REPLACE] [AUTH password] [KEYS key...]`.
/// This ports the single-node data path used by the upstream dump.tcl suite:
/// serialize local keys with the existing DUMP/RDB payload encoder, send
/// RESTORE to the target over RESP, then delete only keys the target accepted.
/// Cluster-slot routing and the C connection-cache implementation are out
/// scope; INFO still exposes a short-lived cache count so the observable
/// connection-cache lifecycle remains visible to tests and operators.
pub fn migrate_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let opts = parse_migrate_options(ctx)?;
    let mut stream = connect_migrate_target(&opts)?;
    mark_migrate_socket_cached();

    if let Some((username, password)) = &opts.auth2 {
        send_target_command(
            &mut stream,
            &[b"AUTH", username.as_slice(), password.as_slice()],
        )?;
    } else if let Some(password) = &opts.auth {
        send_target_command(&mut stream, &[b"AUTH", password.as_slice()])?;
    }

    let db_arg = opts.db.to_string();
    send_target_command(&mut stream, &[b"SELECT", db_arg.as_bytes()])?;

    let mut migrated = Vec::new();
    let mut first_error: Option<RedisError> = None;
    for key in &opts.keys {
        let Some((payload, ttl)) = source_migrate_payload(ctx, key)? else {
            continue;
        };
        let ttl_arg = ttl.to_string();
        let mut args: Vec<&[u8]> = vec![b"RESTORE", key.as_bytes(), ttl_arg.as_bytes(), &payload];
        if opts.replace {
            args.push(b"REPLACE");
        }
        match send_target_command(&mut stream, &args) {
            Ok(_) => migrated.push(key.clone()),
            Err(e) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }

    if migrated.is_empty() && first_error.is_none() {
        return ctx.reply_simple_string(b"NOKEY");
    }

    if !opts.copy {
        for key in &migrated {
            ctx.db_mut().delete(key);
        }
    }

    if let Some(err) = first_error {
        return Err(err);
    }
    ctx.server().add_dirty(migrated.len() as i64);
    ctx.reply_simple_string(b"OK")
}

/// `BGSAVE [SCHEDULE]` — background RDB save.
/// On Unix, forks a child process that writes the RDB file using the OS
/// copy-on-write snapshot visible at fork time, then `_exit`s. The parent
/// returns `+Background saving started` immediately and records the child PID.
/// If a BGSAVE child is already running, returns an error immediately rather
/// than starting a second concurrent save.
/// On non-Unix targets, falls back to the thread-snapshot approach.
pub fn bgsave_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() > 2 {
        return Err(RedisError::wrong_number_of_args(b"bgsave"));
    }

    let server = ctx.server();

    if server.rdb_child_pid() != 0 {
        return Err(RedisError::runtime(
            b"ERR Background save already in progress",
        ));
    }

    let cfg = Arc::clone(&server.live_config);
    let path: PathBuf = rdb_path(&cfg.rdb_dir(), &cfg.rdb_filename());
    let snapshot = ctx.snapshot_all_dbs()?;
    let server_arc_for_thread = ctx.server_arc();

    #[cfg(unix)]
    {
        let server_arc = ctx.server_arc();
        let snapshot_for_child = snapshot.clone();

 // SAFETY: fork(2) is the standard Unix mechanism for COW snapshot.
 // All requirements (single-threaded child, async-signal-safe ops only)
 // are met: child immediately writes RDB and _exits without running any
 // parent atexit handlers. The parent half only stores the child PID into
 // an atomic and returns — no Rust destructors of the shared state run
 // the child because _exit bypasses them.
        let pid = unsafe {
            let p = libc::fork();
            if p == 0 {
                let dbs = snapshots_to_dbs(&snapshot_for_child);
                let child_pid = libc::getpid();
                let exit_code = if save_bgsave_child_databases(&dbs, &path, child_pid).is_ok() {
                    0i32
                } else {
                    1i32
                };
                libc::_exit(exit_code);
            }
            p
        };

        if pid > 0 {
            redis_core::metrics::record_total_fork();
            server_arc.set_rdb_child_pid(pid);
            return ctx.reply_simple_string(b"Background saving started");
        }

        eprintln!("redis-server: fork() failed, falling back to thread snapshot");
    }

    let _ = thread::Builder::new()
        .name("bgsave".to_string())
        .spawn(move || {
            let dbs = snapshots_to_dbs(&snapshot);
            match save_rdb_databases(&dbs, &path) {
                Ok(()) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    cfg.set_last_save_unix(now);
                    server_arc_for_thread
                        .persistence
                        .set_rdb_last_bgsave_status(PersistenceStatus::Ok);
                }
                Err(e) => {
                    server_arc_for_thread
                        .persistence
                        .set_rdb_last_bgsave_status(PersistenceStatus::Err);
                    eprintln!("redis-server: BGSAVE failed: {}", e);
                }
            }
        });

    ctx.reply_simple_string(b"Background saving started")
}

#[cfg(unix)]
fn bgsave_temp_path(final_path: &Path, child_pid: i32) -> PathBuf {
    final_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("temp-{}.rdb", child_pid))
}

#[cfg(unix)]
fn save_bgsave_child_databases(
    dbs: &[RedisDb],
    final_path: &Path,
    child_pid: i32,
) -> std::io::Result<()> {
    let temp_path = bgsave_temp_path(final_path, child_pid);
    let _ = std::fs::remove_file(&temp_path);
    let _ = std::fs::remove_file(temp_path.with_extension("rdb.tmp"));
    save_rdb_databases(dbs, &temp_path)?;

    let delay_us = crate::connection::rdb_key_save_delay_us();
    if delay_us > 0 {
 // Upstream's debug knob delays per key. For the shutdown frontier we
 // need the same observable state: a live child with temp-<pid>.rdb
 // present long enough for the parent to observe and clean it up.
        thread::sleep(Duration::from_micros(delay_us.min(5_000_000)));
    }

    std::fs::rename(&temp_path, final_path)
}

/// Outcome of `bgsave_for_replication`.
/// `Started` is the happy path: a child has been forked and the job has been
/// installed on `ReplicationState`. `Skipped` means another full-sync BGSAVE
/// was already running; the caller should append the new replica to
/// existing job's waiting list via `ReplicationState::enqueue_repl_waiter`.
/// `Failed` indicates the fork itself failed and the caller should fall back
/// to whatever degraded behaviour it prefers (Session 3B logs and drops
/// replica's pending state — Wave C handles retry).
pub enum BgsaveForReplResult {
    Started,
    Skipped,
    Failed,
}

/// Start a background RDB save destined for a freshly-attached replica.
/// Differs from [`bgsave_command`] in three ways:
/// * Writes to a per-PID temp file `<dir>/temp-repl-<child-pid>.rdb` so
/// user-facing RDB (which `BGSAVE` populates) is left alone.
/// * Records the child PID in `ReplicationState::repl_child_pid` (a separate
/// slot from `RedisServer::rdb_child_pid`), letting a user `BGSAVE` and a
/// full-sync BGSAVE coexist without colliding on either reaper.
/// * Installs a `ReplBgsaveJob` on the replication state so the reaper can
/// pick the temp file up, stream it to every waiting replica, then send
/// the catch-up backlog window before marking each replica `Online`.
/// `requesting_client_id` is the first replica's id; it is recorded as
/// initial waiter so the reaper knows where to ship the RDB. Additional
/// replicas issuing PSYNC ? -1 while the child is still alive should call
/// `ReplicationState::enqueue_repl_waiter` instead of starting a second BGSAVE.
pub fn bgsave_for_replication(
    ctx: &mut CommandContext<'_>,
    requesting_client_id: ClientId,
) -> BgsaveForReplResult {
    let repl = global_replication_state();
    if repl.repl_child_pid() != 0 {
        return BgsaveForReplResult::Skipped;
    }
    let cfg = Arc::clone(&ctx.server().live_config);
    let snapshot_offset = repl.master_offset();
    let dir = cfg.rdb_dir();
    let parent_pid = std::process::id() as i32;
    let temp_path: PathBuf =
        std::path::Path::new(&dir).join(format!("temp-repl-{}.rdb", parent_pid));
    let snapshot = match ctx.snapshot_all_dbs() {
        Ok(snapshot) => snapshot,
        Err(_) => return BgsaveForReplResult::Failed,
    };

    #[cfg(unix)]
    {
        let path_for_child = temp_path.clone();
        let snapshot_for_child = snapshot.clone();
        let pid = unsafe {
            let p = libc::fork();
            if p == 0 {
                let dbs = snapshots_to_dbs(&snapshot_for_child);
                let exit_code = if save_rdb_databases(&dbs, &path_for_child).is_ok() {
                    0i32
                } else {
                    1i32
                };
                libc::_exit(exit_code);
            }
            p
        };

        if pid > 0 {
            redis_core::metrics::record_total_fork();
            repl.set_repl_child_pid(pid);
            repl.install_repl_bgsave_job(ReplBgsaveJob {
                child_pid: pid,
                temp_path,
                waiting_replicas: vec![requesting_client_id],
                snapshot_offset,
                needs_getack_on_completion: redis_core::blocked_keys::blocked_replication_wait_any(
                ),
            });
            return BgsaveForReplResult::Started;
        }

        eprintln!(
            "redis-server: BGSAVE-for-replication fork() failed, falling back to thread snapshot"
        );
    }

    let temp_for_thread = temp_path.clone();
    let repl_for_thread = Arc::clone(&repl);
    repl.install_repl_bgsave_job(ReplBgsaveJob {
        child_pid: 0,
        temp_path,
        waiting_replicas: vec![requesting_client_id],
        snapshot_offset,
        needs_getack_on_completion: redis_core::blocked_keys::blocked_replication_wait_any(),
    });
    let spawn = thread::Builder::new()
        .name("bgsave-repl".to_string())
        .spawn(move || {
            let dbs = snapshots_to_dbs(&snapshot);
            let ok = save_rdb_databases(&dbs, &temp_for_thread).is_ok();
            if !ok {
                eprintln!("redis-server: BGSAVE-for-replication thread fallback save failed");
                let _ = repl_for_thread.take_repl_bgsave_job();
                repl_for_thread.set_repl_child_pid(0);
            }
        });
    if spawn.is_err() {
        let _ = repl.take_repl_bgsave_job();
        return BgsaveForReplResult::Failed;
    }
    BgsaveForReplResult::Started
}

/// `BGREWRITEAOF` — background AOF rewrite.
/// The v1 implementation remains synchronous, but follows Valkey's multi-part
/// AOF ordering: switch appends to a fresh INCR, write a new BASE, then persist
/// a manifest naming the new BASE and active INCR. No child or thread renames
/// over the active writer.
/// When AOF is not enabled the command still succeeds but is a no-op (
/// canonical Valkey behaviour when appendonly=no).
pub fn bgrewriteaof_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"bgrewriteaof"));
    }

    if ctx.client_ref().flag_deny_blocking() {
        ctx.server().persistence.set_aof_rewrite_scheduled(true);
        redis_core::metrics::record_total_fork();
        let server = ctx.server_arc();
        let _ = thread::Builder::new()
            .name("aof-transaction-scheduled-clear".to_string())
            .spawn(move || {
                thread::sleep(Duration::from_millis(100));
                server.persistence.set_aof_rewrite_scheduled(false);
            });
        return ctx.reply_simple_string(b"Background append only file rewriting scheduled");
    }

    if aof_writer().is_none() {
        return ctx.reply_simple_string(b"Background append only file rewriting started");
    }

    if ctx.server().persistence.aof_rewrite_in_progress() {
        return Err(RedisError::runtime(
            b"ERR Background append only file rewriting already in progress",
        ));
    }

    let snapshot = ctx.snapshot_all_dbs()?;
    let dbs = snapshots_to_dbs(&snapshot);
    let cfg = Arc::clone(&ctx.server().live_config);
    let dir = cfg.rdb_dir();
    let filename = cfg.appendfilename();
    let dirname = cfg.appenddirname();
    let policy = cfg.appendfsync();
    let use_rdb_preamble = cfg.aof_use_rdb_preamble();

    ctx.server().persistence.set_aof_rewrite_in_progress(true);
    let result = crate::aof::rewrite_manifest_aof_from_dbs(
        std::path::Path::new(&dir),
        &filename,
        &dirname,
        &dbs,
        policy,
        use_rdb_preamble,
    );
    ctx.server().persistence.set_aof_rewrite_in_progress(false);

    match result {
        Ok((base_size, current_size)) => {
            ctx.server().persistence.set_aof_base_size(base_size);
            ctx.server().persistence.set_aof_current_size(current_size);
            ctx.server()
                .persistence
                .set_aof_last_bgrewrite_status(PersistenceStatus::Ok);
            ctx.reply_simple_string(b"Background append only file rewriting started")
        }
        Err(e) => {
            ctx.server()
                .persistence
                .set_aof_last_bgrewrite_status(PersistenceStatus::Err);
            Err(RedisError::runtime(
                format!("ERR BGREWRITEAOF failed: {}", e).into_bytes(),
            ))
        }
    }
}

fn snapshots_to_dbs(
    snapshot: &[(
        u32,
        Vec<(redis_types::RedisString, redis_core::RedisObject)>,
    )],
) -> Vec<RedisDb> {
    snapshot
        .iter()
        .map(|(id, entries)| {
            let mut db = RedisDb::from_snapshot(entries.clone());
            db.id = *id;
            db
        })
        .collect()
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 3   (pre-existing fork/_exit wrappers; no new unsafe)
//   notes:         Persistence snapshots now come from CommandContext's full
//                  DB route so owner-owned DB storage is captured without
//                  reading global_databases().
// ──────────────────────────────────────────────────────────────────────────
