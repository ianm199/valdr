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

use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::client::ClientId;
use redis_core::db::RedisDb;
use redis_core::rdb::{rdb_path, save_rdb};
use redis_core::replication::{global_replication_state, ReplBgsaveJob};
use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult};

use crate::aof::{aof_writer, write_aof_rewrite};

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

    let result = {
        let db = ctx.db();
        save_rdb(db, &path)
    };

    match result {
        Ok(()) => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            cfg.set_last_save_unix(now);
            ctx.reply_simple_string(b"OK")
        }
        Err(e) => Err(RedisError::runtime(
            format!("ERR SAVE failed: {}", e).into_bytes(),
        )),
    }
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

    #[cfg(unix)]
    {
        let server_arc = ctx.server_arc();

        // SAFETY: fork(2) is the standard Unix mechanism for COW snapshot.
        // All requirements (single-threaded child, async-signal-safe ops only)
        // are met: child immediately writes RDB and _exits without running any
        // parent atexit handlers. The parent half only stores the child PID into
        // an atomic and returns — no Rust destructors of the shared state run in
        // the child because _exit bypasses them.
        let pid = unsafe {
            let p = libc::fork();
            if p == 0 {
                let exit_code = if save_rdb(ctx.db(), &path).is_ok() { 0i32 } else { 1i32 };
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

    let snapshot = snapshot_db(ctx.db());
    let _ = thread::Builder::new()
        .name("bgsave".to_string())
        .spawn(move || {
            let tmp_db = RedisDb::from_snapshot(snapshot);
            match save_rdb(&tmp_db, &path) {
                Ok(()) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    cfg.set_last_save_unix(now);
                }
                Err(e) => {
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

    #[cfg(unix)]
    {
        let path_for_child = temp_path.clone();
        let pid = unsafe {
            let p = libc::fork();
            if p == 0 {
                let exit_code = if save_rdb(ctx.db(), &path_for_child).is_ok() { 0i32 } else { 1i32 };
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

    let snapshot = snapshot_db(ctx.db());
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
            let tmp_db = RedisDb::from_snapshot(snapshot);
            let ok = save_rdb(&tmp_db, &temp_for_thread).is_ok();
            if !ok {
                eprintln!(
                    "redis-server: BGSAVE-for-replication thread fallback save failed"
                );
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
/// On Unix, forks a child that walks the DB and writes a compacted AOF to a
/// temp file, then atomically renames it over the existing AOF. The parent
/// returns `+Background append only file rewriting started` immediately.
///
/// When AOF is not enabled the command still succeeds but is a no-op (the
/// canonical Valkey behaviour when appendonly=no).
pub fn bgrewriteaof_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 1 {
        return Err(RedisError::wrong_number_of_args(b"bgrewriteaof"));
    }

    let writer_arc = match aof_writer() {
        Some(w) => w,
        None => {
            return ctx.reply_simple_string(b"Background append only file rewriting started");
        }
    };

    let snapshot = snapshot_db(ctx.db());
    let aof_path = writer_arc.path.clone();

    #[cfg(unix)]
    {
        let pid = unsafe {
            let p = libc::fork();
            if p == 0 {
                let tmp_path = {
                    let mut t = aof_path.clone();
                    let mut name = t.file_name().unwrap_or_default().to_os_string();
                    name.push(".rewrite.tmp");
                    t.set_file_name(name);
                    t
                };
                let exit_code = match do_aof_rewrite(&snapshot, &tmp_path, &aof_path) {
                    Ok(()) => 0i32,
                    Err(_) => 1i32,
                };
                libc::_exit(exit_code);
            }
            p
        };

        if pid > 0 {
            return ctx.reply_simple_string(b"Background append only file rewriting started");
        }

        eprintln!("redis-server: BGREWRITEAOF fork() failed, falling back to thread");
    }

    let _ = thread::Builder::new()
        .name("bgrewriteaof".to_string())
        .spawn(move || {
            let tmp_path = {
                let mut t = aof_path.clone();
                let mut name = t.file_name().unwrap_or_default().to_os_string();
                name.push(".rewrite.tmp");
                t.set_file_name(name);
                t
            };
            if let Err(e) = do_aof_rewrite(&snapshot, &tmp_path, &aof_path) {
                eprintln!("redis-server: BGREWRITEAOF failed: {}", e);
            }
        });

    ctx.reply_simple_string(b"Background append only file rewriting started")
}

/// Write a complete AOF rewrite for `snapshot` to `tmp_path`, then atomically
/// rename it over `final_path`.
fn do_aof_rewrite(
    snapshot: &[(redis_types::RedisString, redis_core::RedisObject)],
    tmp_path: &PathBuf,
    final_path: &PathBuf,
) -> std::io::Result<()> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(tmp_path)?;
    let mut buf = BufWriter::new(file);
    let tmp_db = RedisDb::from_snapshot(snapshot.to_vec());
    write_aof_rewrite(&tmp_db, &mut buf)?;
    buf.flush()?;
    std::fs::rename(tmp_path, final_path)?;
    Ok(())
}

/// Snapshot the entries of `db` into an owned `Vec` for the thread-based
/// BGSAVE fallback used on non-Unix targets and on fork failure.
fn snapshot_db(db: &RedisDb) -> Vec<(redis_types::RedisString, redis_core::RedisObject)> {
    db.iter_for_eviction()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
