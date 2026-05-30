//! Stream-side reactive hooks installed by `redis-commands::stream`.
//! Extracted from `db.rs` by refactor/file-structure-splits. These four
//! hooks let stream-blocking-clients (`XREADGROUP BLOCK`) react to keyspace
//! events that originate outside the stream codepath itself:
//! * key-deleted (DEL / FLUSHDB)
//! * db-flushed (FLUSHDB / FLUSHALL)
//! * key-renamed (RENAME / RENAMENX)
//! * key-overwritten (SET on a stream key with a non-stream value)
//! Each hook is a single `OnceLock<Box<dyn Fn>>` installed once at startup
//! from `redis-commands::stream` and fired by db-side mutation code.

use redis_types::RedisString;
use std::sync::OnceLock;

type StreamKeyDeletedFn = dyn Fn(&RedisString) + Send + Sync;
static STREAM_KEY_DELETED_HOOK: OnceLock<Box<StreamKeyDeletedFn>> = OnceLock::new();

/// Install the hook called when a stream key is deleted (DEL / FLUSHDB-side).
/// The hook receives the key that was deleted and wakes any XREADGROUP BLOCK
/// clients waiting on that key with a NOGROUP error. Installed once
/// `redis-commands`; subsequent calls are no-ops.
pub fn install_stream_key_deleted_hook(f: Box<StreamKeyDeletedFn>) {
    let _ = STREAM_KEY_DELETED_HOOK.set(f);
}

pub fn fire_stream_key_deleted_hook(key: &RedisString) {
    if let Some(hook) = STREAM_KEY_DELETED_HOOK.get() {
        hook(key);
    }
}

type StreamDbFlushedFn = dyn Fn() + Send + Sync;
static STREAM_DB_FLUSHED_HOOK: OnceLock<Box<StreamDbFlushedFn>> = OnceLock::new();

/// Install the hook called when a database is flushed (FLUSHDB / FLUSHALL).
/// Wakes all XREADGROUP BLOCK clients with NOGROUP errors. Installed once
/// from `redis-commands`; subsequent calls are no-ops.
pub fn install_stream_db_flushed_hook(f: Box<StreamDbFlushedFn>) {
    let _ = STREAM_DB_FLUSHED_HOOK.set(f);
}

pub fn fire_stream_db_flushed_hook() {
    if let Some(hook) = STREAM_DB_FLUSHED_HOOK.get() {
        hook();
    }
}

type StreamRenameHookFn = dyn Fn(&RedisString, u32) + Send + Sync;
static STREAM_RENAME_HOOK: OnceLock<Box<StreamRenameHookFn>> = OnceLock::new();

/// Install the hook called after RENAME/RENAMENX completes.
/// The hook receives the destination key name and the database index. The
/// `redis-commands` layer wakes any XREADGROUP BLOCK clients parked on that
/// key: if the new value has the expected group, entries are delivered;
/// otherwise NOGROUP is sent. Installed once from `redis-commands`; subsequent
/// calls are no-ops.
pub fn install_stream_rename_hook(f: Box<StreamRenameHookFn>) {
    let _ = STREAM_RENAME_HOOK.set(f);
}

pub fn fire_stream_rename_hook(dst_key: &RedisString, db_id: u32) {
    if let Some(hook) = STREAM_RENAME_HOOK.get() {
        hook(dst_key, db_id);
    }
}

type StreamKeyOverwrittenFn = dyn Fn(&RedisString) + Send + Sync;
static STREAM_KEY_OVERWRITTEN_HOOK: OnceLock<Box<StreamKeyOverwrittenFn>> = OnceLock::new();

/// Install the hook called when a stream key is overwritten with a non-stream
/// value (e.g. SET mystream val). Wakes blocked XREADGROUP clients with
/// WRONGTYPE error. Installed once from `redis-commands`; subsequent calls
/// are no-ops.
pub fn install_stream_key_overwritten_hook(f: Box<StreamKeyOverwrittenFn>) {
    let _ = STREAM_KEY_OVERWRITTEN_HOOK.set(f);
}

pub fn fire_stream_key_overwritten_hook(key: &RedisString) {
    if let Some(hook) = STREAM_KEY_OVERWRITTEN_HOOK.get() {
        hook(key);
    }
}

pub fn has_stream_key_overwritten_hook() -> bool {
    STREAM_KEY_OVERWRITTEN_HOOK.get().is_some()
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        extracted from db.rs (refactor/file-structure-splits)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         4 stream-reactive hooks: key_deleted, db_flushed,
//                  rename, key_overwritten. Installed by redis-commands::stream;
//                  fired by db-side mutation code via crate::stream_hooks::*.
// ──────────────────────────────────────────────────────────────────────────
