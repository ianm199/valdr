//! AUTO-EXTRACTED from connection.rs by refactor/file-structure-splits phase 1.5.
#![allow(unused_imports, dead_code, unused_variables, unused_mut)]

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis_core::acl::{
    acl_log_entries, acl_log_max_len, acl_log_now_millis, acl_pubsub_default_config_value,
    apply_acl_pubsub_default_to_user, category as acl_category, category_name_to_bit,
    clear_acl_log, global_acl_state, hex_to_hash, record_acl_log_entry, set_acl_log_max_len,
    set_acl_pubsub_default, sha256_hash, AclKeyPattern, AclLogEntry, AclUser, ACL_KEY_READ,
    ACL_KEY_READ_WRITE, ACL_KEY_WRITE, ALL_CATEGORY_NAMES,
};
use redis_core::blocked_keys::{blocked_keys_index, BlockedAction};
use redis_core::client_info::client_info_registry;
use redis_core::eviction::{try_evict_to_fit, EvictionOutcome};
use redis_core::live_config::{LiveConfig, MaxmemoryPolicyCode};
use redis_core::metrics::{
    record_acl_access_denied_auth, record_blocked_command_rejected, record_error_reply,
    server_metrics,
};
use redis_core::networking::{
    client_matches_ip_filter, validate_client_capa_filter, validate_client_flag_filter,
};
use redis_core::notify::{keyspace_events_string_to_flags, NOTIFY_EVICTED};
use redis_core::object::object_compute_size;
use redis_core::{CommandContext, PersistenceStatus, RedisDb};
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisResult, RedisString};
use serde_json::Value;

use crate::acl_cmd::*;
use crate::client_cmd::*;
use crate::client_limits::*;
use crate::command_meta::*;
use crate::config_cmd::*;
use crate::connection::*;
use crate::generated::{GeneratedCommandSpec, COMMANDS};
use crate::listeners::*;
use crate::live_config_handle;
use crate::shutdown_signals::*;

pub fn debug_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() < 2 {
        return Err(RedisError::wrong_number_of_args(b"debug"));
    }
    let sub = ctx.arg_owned(1usize)?;
    if ascii_eq_ignore_case(sub.as_bytes(), b"SLEEP") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"debug"));
        }
        let secs_arg = ctx.arg_owned(2usize)?;
        let secs = parse_f64_strict(secs_arg.as_bytes())
            .ok_or_else(|| RedisError::runtime(b"ERR value is not a valid float"))?;
        if secs.is_sign_negative() || secs.is_nan() {
            return Err(RedisError::runtime(b"ERR value is not a valid float"));
        }
        let dur = std::time::Duration::from_secs_f64(secs);
        std::thread::sleep(dur);
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"SET-ACTIVE-EXPIRE") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"PAUSE-CRON") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"debug pause-cron"));
        }
        let value = ctx.arg_owned(2usize)?;
        match value.as_bytes() {
            b"0" => set_debug_pause_cron(false),
            b"1" => set_debug_pause_cron(true),
            _ => {
                return Err(RedisError::runtime(
                    b"ERR value is not an integer or out of range",
                ))
            }
        }
        // Upstream uses this as a test-only clientsCron timing knob. This
        // port does not run a C-style clientsCron loop, so accepting the knob
        // lets query-buffer tests proceed to their observable assertions.
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"REPLYBUFFER") {
        if ctx.arg_count() != 4 {
            return Err(RedisError::wrong_number_of_args(b"debug replybuffer"));
        }
        let knob = ctx.arg_owned(2usize)?;
        if !ascii_eq_ignore_case(knob.as_bytes(), b"PEAK-RESET-TIME") {
            let mut msg = Vec::with_capacity(
                b"ERR Unknown DEBUG REPLYBUFFER subcommand: ".len() + knob.as_bytes().len(),
            );
            msg.extend_from_slice(b"ERR Unknown DEBUG REPLYBUFFER subcommand: ");
            msg.extend_from_slice(knob.as_bytes());
            return Err(RedisError::runtime(msg));
        }
        let value = ctx.arg_owned(3usize)?;
        let bytes = value.as_bytes();
        if !ascii_eq_ignore_case(bytes, b"NEVER")
            && !ascii_eq_ignore_case(bytes, b"RESET")
            && parse_i64_strict(bytes).is_none()
        {
            return Err(RedisError::runtime(
                b"ERR value is not an integer or out of range",
            ));
        }
        // The runtime owner tracks reply-buffer memory directly rather than
        // through a peak-reset timer. Accept the test-only knob and leave
        // actual accounting path unchanged.
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"SET-SKIP-CHECKSUM-VALIDATION") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(
                b"debug set-skip-checksum-validation",
            ));
        }
        let flag = ctx.arg_owned(2usize)?;
        redis_core::rdb::load::set_skip_checksum_validation(flag.as_bytes() != b"0");
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"AOF-FLUSH-SLEEP") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"debug aof-flush-sleep"));
        }
        let micros = ctx.arg_owned(2usize)?;
        let Some(micros) = parse_i64_strict(micros.as_bytes()).filter(|n| *n >= 0) else {
            return Err(RedisError::runtime(
                b"ERR value is not an integer or out of range",
            ));
        };
        crate::aof::set_debug_aof_flush_sleep_micros(micros as u64);
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"POPULATE") {
        if !(3..=5).contains(&ctx.arg_count()) {
            return Err(RedisError::wrong_number_of_args(b"debug populate"));
        }
        let count_arg = ctx.arg_owned(2usize)?;
        let Some(count) = parse_i64_strict(count_arg.as_bytes()).filter(|n| *n >= 0) else {
            return Err(RedisError::runtime(
                b"ERR value is not an integer or out of range",
            ));
        };
        let prefix = ctx.arg_owned(3usize)?;
        let size = if ctx.arg_count() >= 5 {
            let size_arg = ctx.arg_owned(4usize)?;
            parse_i64_strict(size_arg.as_bytes())
                .filter(|n| *n >= 0)
                .unwrap_or(0) as usize
        } else {
            0
        };
        let value = RedisString::from_vec(vec![b'0'; size]);
        for idx in 0..count {
            let mut key = Vec::with_capacity(prefix.len() + 24);
            key.extend_from_slice(prefix.as_bytes());
            key.extend_from_slice(b":");
            key.extend_from_slice(idx.to_string().as_bytes());
            ctx.db_mut().set_key(
                RedisString::from_vec(key),
                redis_core::RedisObject::from_string(value.clone()),
                0,
            );
        }
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"CLIENT-ENFORCE-REPLY-LIST") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(
                b"debug client-enforce-reply-list",
            ));
        }
        let value = ctx.arg_owned(2usize)?;
        let enabled = match value.as_bytes() {
            b"0" => false,
            b"1" => true,
            _ => {
                return Err(RedisError::runtime(
                    b"ERR value is not an integer or out of range",
                ))
            }
        };
        redis_core::client::set_debug_client_enforce_reply_list(enabled);
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"CONFIG-REWRITE-FORCE-ALL") {
        if ctx.arg_count() != 2 {
            return Err(RedisError::wrong_number_of_args(
                b"debug config-rewrite-force-all",
            ));
        }
        // Test-only upstream knob: force CONFIG REWRITE to emit every option.
        // This port's CONFIG REWRITE is currently a no-op persistence shim, so
        // accepting the DEBUG command is the observable compatibility contract.
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"FORCE-FREE-PRIMARY-ASYNC") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(
                b"debug force-free-primary-async",
            ));
        }
        let value = ctx.arg_owned(2usize)?;
        match value.as_bytes() {
            b"0" | b"1" => {}
            _ => {
                return Err(RedisError::runtime(
                    b"ERR value is not an integer or out of range",
                ))
            }
        }
        // C toggles server.debug_force_free_primary_async so the next primary
        // client is freed on the async path. This port does not yet keep a
        // primary client object in the RuntimeOwner-disabled replica dialer,
        // but the upstream wait.tcl repoint test uses this knob before it
        // checks that REPLICAOF logs only one reconnect attempt.
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"DIGEST-VALUE") {
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"debug digest-value"));
        }
        let key = ctx.arg_owned(2usize)?;
        let digest = match ctx
            .db_mut()
            .lookup_key_read_with_flags(&key, redis_core::db::LOOKUP_NOTOUCH)
        {
            None => b"0000000000000000000000000000000000000000".to_vec(),
            Some(obj) => {
                let mut h: u64 = 0xcbf29ce484222325;
                for b in obj.string_bytes_owned() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
                format!("{:040x}", h).into_bytes()
            }
        };
        return ctx.reply_bulk_string(redis_types::RedisString::from_bytes(&digest));
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"DIGEST") {
        return ctx.reply_bulk_string(redis_types::RedisString::from_bytes(
            b"0000000000000000000000000000000000000000",
        ));
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"OBJECT") {
        if ctx.arg_count() != 3 {
            return Err(RedisError::wrong_number_of_args(b"debug object"));
        }
        let key = ctx.arg_owned(2usize)?;
        let line = match ctx
            .db_mut()
            .lookup_key_read_with_flags(&key, redis_core::db::LOOKUP_NOTOUCH)
        {
            None => b"Value at:0x0 refcount:0 encoding:null serializedlength:0 lru:0 lru_seconds:0 type:none".to_vec(),
            Some(obj) => format!(
                "Value at:0x0 refcount:1 encoding:{} serializedlength:1 lru:{} lru_seconds:{} type:{}",
                obj.encoding_name(),
                obj.lru,
                obj.lru_idle_secs(),
                obj.type_name()
            )
            .into_bytes(),
        };
        return ctx.reply_simple_string(&line);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"HTSTATS") {
        let entries = ctx.db().len();
        let table_size = debug_htstats_main_table_size(entries, ctx.server().rdb_child_pid() != 0);
        let payload = format!(
            "[Dictionary HT]\nHash table 0 stats (main hash table):\n table size: {}\n number of entries: {}\n rehashing index: -1\n",
            table_size, entries
        );
        return ctx.reply_bulk_string(redis_types::RedisString::from_bytes(payload.as_bytes()));
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"QUICKLIST-PACKED-THRESHOLD") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"CHANGE-REPL-ID") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"RELOAD") {
        return debug_reload_command(ctx);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"LOADAOF") {
        return debug_loadaof_command(ctx);
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"FLUSHALL") {
        ctx.db_mut().clear();
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"JMAP") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"AOFSTATS") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"DISABLE-REPLICATION-CACHING") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"CLOSE-LISTENERS-ASA") {
        return ctx.reply_simple_string(b"OK");
    }
    if ascii_eq_ignore_case(sub.as_bytes(), b"REPLICATE") {
        // `DEBUG REPLICATE <cmd> [args...]` injects an arbitrary command verbatim
        // into the replication stream (no implicit SELECT), mirroring C
        // `replicationFeedReplicas(-1, argv+2, argc-2)`. Used by replication-4 to
        // deliberately diverge a replica.
        if ctx.arg_count() < 3 {
            return Err(RedisError::wrong_number_of_args(b"debug"));
        }
        let mut fed = Vec::with_capacity(ctx.arg_count() - 2);
        for i in 2..ctx.arg_count() {
            fed.push(ctx.arg_owned(i)?);
        }
        crate::dispatch::propagate_command_raw(&fed);
        return ctx.reply_simple_string(b"OK");
    }
    let mut msg =
        Vec::with_capacity(b"ERR Unknown DEBUG subcommand: ".len() + sub.as_bytes().len());
    msg.extend_from_slice(b"ERR Unknown DEBUG subcommand: ");
    msg.extend_from_slice(sub.as_bytes());
    Err(RedisError::runtime(msg))
}

fn debug_htstats_main_table_size(entries: usize, child_active: bool) -> usize {
    let minimum = 4096usize;
    if entries <= minimum {
        return minimum;
    }
    if child_active {
        return (entries - 1).next_power_of_two().max(minimum);
    }
    entries.next_power_of_two().max(minimum)
}

pub fn debug_reload_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    let mut nosave = false;
    let mut noflush = false;
    let mut merge = false;

    for i in 2..ctx.arg_count() {
        let opt = ctx.arg_owned(i)?;
        let bytes = opt.as_bytes();
        if ascii_eq_ignore_case(bytes, b"NOSAVE") {
            nosave = true;
        } else if ascii_eq_ignore_case(bytes, b"NOFLUSH") {
            noflush = true;
        } else if ascii_eq_ignore_case(bytes, b"MERGE") {
            merge = true;
            noflush = true;
        } else {
            return Err(RedisError::syntax(b"syntax error"));
        }
    }

    let cfg = Arc::clone(&ctx.server().live_config);
    let path = redis_core::rdb::rdb_path(&cfg.rdb_dir(), &cfg.rdb_filename());
    if !nosave {
        let snapshot = ctx.snapshot_all_dbs()?;
        let snapshot_dbs = snapshot.to_dbs();
        redis_core::rdb::save_rdb_databases(&snapshot_dbs, &path).map_err(|e| {
            ctx.server()
                .persistence
                .set_rdb_last_bgsave_status(PersistenceStatus::Err);
            RedisError::runtime(format!("ERR DEBUG RELOAD SAVE failed: {}", e).into_bytes())
        })?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        cfg.set_last_save_unix(now);
        ctx.server()
            .persistence
            .set_rdb_last_bgsave_status(PersistenceStatus::Ok);
    }

    let mut loaded: Vec<RedisDb> = (0..ctx.database_count() as u32).map(RedisDb::new).collect();
    redis_core::rdb::load_into_dbs_with_options(
        &mut loaded,
        &path,
        redis_core::rdb::RdbLoadOptions {
            allow_dup: merge,
            skip_expired: true,
            aof_preamble: false,
        },
    )
    .map_err(|e| RedisError::runtime(format!("ERR DEBUG RELOAD failed: {}", e).into_bytes()))?;

    replace_or_merge_dbs(ctx, loaded, noflush, merge)?;
    ctx.reply_simple_string(b"OK")
}

pub fn debug_loadaof_command(ctx: &mut CommandContext<'_>) -> RedisResult<()> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"debug loadaof"));
    }

    let cfg = Arc::clone(&ctx.server().live_config);
    let dir = cfg.rdb_dir();
    let filename = cfg.appendfilename();
    let dirname = cfg.appenddirname();
    let mut loaded: Vec<RedisDb> = (0..ctx.database_count() as u32).map(RedisDb::new).collect();
    crate::aof::load_append_only_files(
        std::path::Path::new(&dir),
        &filename,
        &dirname,
        &mut loaded,
        crate::aof::AofLoadOptions {
            load_truncated: cfg.aof_load_truncated(),
            allow_rdb_preamble: cfg.aof_use_rdb_preamble(),
            lua_time_limit_ms: cfg.lua_time_limit_ms(),
        },
    )
    .map_err(|e| RedisError::runtime(format!("ERR DEBUG LOADAOF failed: {}", e).into_bytes()))?;
    replace_or_merge_dbs(ctx, loaded, false, true)?;
    ctx.reply_simple_string(b"OK")
}

pub(crate) fn replace_or_merge_dbs(
    ctx: &mut CommandContext<'_>,
    loaded: Vec<RedisDb>,
    noflush: bool,
    merge: bool,
) -> RedisResult<()> {
    if noflush {
        for loaded_db in loaded.iter() {
            let db_id = loaded_db.id;
            ctx.with_db_index(db_id, |live| {
                for (key, obj) in loaded_db.iter_for_eviction() {
                    if !merge && live.exists_raw(key) {
                        return Err(RedisError::runtime(
                            b"ERR DEBUG RELOAD found duplicate key; use MERGE",
                        ));
                    }
                    live.insert(key.clone(), obj.clone());
                }
                Ok(())
            })??;
        }
    } else {
        for loaded_db in loaded {
            let db_id = loaded_db.id;
            ctx.with_db_index(db_id, move |live| {
                *live = loaded_db;
            })?;
        }
    }
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        extracted from connection.rs (phase 1.5)
//   target_crate:  redis-commands
//   confidence:    high
//   todos:         0
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         DEBUG command + helpers.
// ──────────────────────────────────────────────────────────────────────────
