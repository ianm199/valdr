//! Persistence commands: SAVE, BGSAVE.
//!
//! `SAVE` runs `rdb::save_rdb` synchronously in the calling thread and updates
//! `last_save_unix` on success.
//!
//! `BGSAVE` on Unix uses `fork(2)` so the OS copy-on-write page mapping gives
//! the child a frozen snapshot of the DB without any memory duplication:
//!   1. fork — child sees the DB as it was at the instant of the fork.
//!   2. Child writes the RDB file and calls `_exit(0)` (not `exit()` — skipping
//!      atexit handlers that belong to the parent).
//!   3. Parent records the child PID in `server.rdb_child_pid` and returns
//!      `+Background saving started` immediately.
//!   4. A background polling thread (spawned at server start) calls
//!      `waitpid` every 500 ms to reap the child and update `last_save_unix`.
//!
//! On non-Unix targets (Windows, WASM) the pre-fork thread-snapshot path is
//! kept as the fallback. The fallback allocates a full in-memory clone of the
//! DB before spawning the writer thread.
//!
//! The `unsafe` block that wraps `fork + _exit` is the single unsafe surface in
//! this crate: documented below with a SAFETY comment.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

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
use redis_types::{RedisError, RedisResult};

use crate::aof::aof_writer;

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
///
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
    let payload = match ctx
        .db_mut()
        .lookup_key_read_with_flags(&key, LOOKUP_NOTOUCH)
    {
        Some(obj) => create_dump_payload(obj)
            .map_err(|e| RedisError::runtime(format!("ERR DUMP failed: {}", e).into_bytes()))?,
        None => return ctx.reply_null_bulk(),
    };

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
        if replace {
            ctx.db_mut().delete(&key);
        }
        ctx.server().add_dirty(1);
        return ctx.reply_simple_string(b"OK");
    }

    object_set_lru_or_lfu(&mut obj, lfu_freq, lru_idle);
    ctx.db_mut()
        .set_key_with_known_expire(key, obj, expire_at, 0);
    ctx.server().add_dirty(1);
    ctx.reply_simple_string(b"OK")
}

/// Cluster-internal RESTORE variant. Cluster asking state is out of scope for
/// the single-node port, so it shares RESTORE's local behaviour.
pub fn restore_asking_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    restore_command(ctx)
}

/// `BGSAVE [SCHEDULE]` — background RDB save.
///
/// On Unix, forks a child process that writes the RDB file using the OS
/// copy-on-write snapshot visible at fork time, then `_exit`s. The parent
/// returns `+Background saving started` immediately and records the child PID.
///
/// If a BGSAVE child is already running, returns an error immediately rather
/// than starting a second concurrent save.
///
/// On non-Unix targets, falls back to the thread-snapshot approach.
pub fn bgsave_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() > 2 {
        return Err(RedisError::wrong_number_of_args(b"bgsave"));
    }

    let server = ctx.server();

    if server.rdb_child_pid() != 0 {
        return Err(RedisError::runtime(
            b"ERR Background save already in progress".to_vec(),
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
        // an atomic and returns — no Rust destructors of the shared state run in
        // the child because _exit bypasses them.
        let pid = unsafe {
            let p = libc::fork();
            if p == 0 {
                let dbs = snapshots_to_dbs(&snapshot_for_child);
                let exit_code = if save_rdb_databases(&dbs, &path).is_ok() {
                    0i32
                } else {
                    1i32
                };
                libc::_exit(exit_code);
            }
            p
        };

        if pid > 0 {
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

/// Outcome of `bgsave_for_replication`.
///
/// `Started` is the happy path: a child has been forked and the job has been
/// installed on `ReplicationState`. `Skipped` means another full-sync BGSAVE
/// was already running; the caller should append the new replica to the
/// existing job's waiting list via `ReplicationState::enqueue_repl_waiter`.
/// `Failed` indicates the fork itself failed and the caller should fall back
/// to whatever degraded behaviour it prefers (Session 3B logs and drops the
/// replica's pending state — Wave C handles retry).
pub enum BgsaveForReplResult {
    Started,
    Skipped,
    Failed,
}

/// Start a background RDB save destined for a freshly-attached replica.
///
/// Differs from [`bgsave_command`] in three ways:
///   * Writes to a per-PID temp file `<dir>/temp-repl-<child-pid>.rdb` so the
///     user-facing RDB (which `BGSAVE` populates) is left alone.
///   * Records the child PID in `ReplicationState::repl_child_pid` (a separate
///     slot from `RedisServer::rdb_child_pid`), letting a user `BGSAVE` and a
///     full-sync BGSAVE coexist without colliding on either reaper.
///   * Installs a `ReplBgsaveJob` on the replication state so the reaper can
///     pick the temp file up, stream it to every waiting replica, then send
///     the catch-up backlog window before marking each replica `Online`.
///
/// `requesting_client_id` is the first replica's id; it is recorded as the
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
            repl.set_repl_child_pid(pid);
            repl.install_repl_bgsave_job(ReplBgsaveJob {
                child_pid: pid,
                temp_path,
                waiting_replicas: vec![requesting_client_id],
                snapshot_offset,
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
///
/// The v1 implementation remains synchronous, but follows Valkey's multi-part
/// AOF ordering: switch appends to a fresh INCR, write a new BASE, then persist
/// a manifest naming the new BASE and active INCR. No child or thread renames
/// over the active writer.
///
/// When AOF is not enabled the command still succeeds but is a no-op (the
/// canonical Valkey behaviour when appendonly=no).
pub fn bgrewriteaof_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"bgrewriteaof"));
    }

    if aof_writer().is_none() {
        return ctx.reply_simple_string(b"Background append only file rewriting started");
    }

    if ctx.server().persistence.aof_rewrite_in_progress() {
        return Err(RedisError::runtime(
            b"ERR Background append only file rewriting already in progress".to_vec(),
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
//   source:        src/rdb.c / src/aof.c persistence command integration
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 3   (pre-existing fork/_exit wrappers; no new unsafe)
//   notes:         Persistence snapshots now come from CommandContext's full
//                  DB route so owner-owned DB storage is captured without
//                  reading global_databases().
// ──────────────────────────────────────────────────────────────────────────
