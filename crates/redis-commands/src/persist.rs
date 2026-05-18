//! Persistence commands: SAVE, BGSAVE.
//!
//! `SAVE` runs `rdb::save_rdb` synchronously and updates `last_save_unix`
//! on the live config.
//!
//! `BGSAVE` spawns a background thread that takes a snapshot of the DB
//! and writes the RDB file. The main thread returns immediately with
//! `+Background saving started`. No fork is used — this is a deliberate
//! Phase-1 simplification.
//!
//! TODO: real BGSAVE semantics require fork(2) so the parent keeps serving
//! requests while the child writes the file without seeing concurrent writes.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::db::RedisDb;
use redis_core::rdb::{rdb_path, save_rdb};
use redis_core::CommandContext;
use redis_types::{RedisError, RedisResult};

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
/// Spawns a thread that holds a snapshot of the key-value pairs and writes
/// the RDB file. The command returns immediately with
/// `+Background saving started`.
///
/// TODO: replace the thread snapshot with a real fork-based approach so the
/// parent does not block while the child writes the file.
pub fn bgsave_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() > 2 {
        return Err(RedisError::wrong_number_of_args(b"bgsave"));
    }

    let cfg = Arc::clone(&ctx.server().live_config);
    let path: PathBuf = rdb_path(&cfg.rdb_dir(), &cfg.rdb_filename());

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

/// Snapshot the entries of `db` into an owned `Vec` for BGSAVE.
fn snapshot_db(db: &RedisDb) -> Vec<(redis_types::RedisString, redis_core::RedisObject)> {
    db.iter_for_eviction()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
