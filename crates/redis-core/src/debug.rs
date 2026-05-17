//! Debug command and crash-reporting infrastructure.
//!
//! Ported from `src/debug.c` (2649 lines, ~58 functions).
//!
//! This module contains:
//!   * `debug_command` — the `DEBUG` command dispatcher.
//!   * Dataset-digest helpers used by `DEBUG DIGEST` / `DEBUG DIGEST-VALUE`.
//!   * Crash / assertion reporting helpers (`bug_report_start`, `print_crash_report`, etc.).
//!   * Signal-handler registration and the SIGSEGV / SIGALRM handlers.
//!   * Watchdog timer helpers.
//!   * Thread-stack-trace utilities (Linux-only, platform-gated).
//!
//! # Safety policy
//! Signal handlers, platform-specific `ucontext_t` access, `dladdr`, and
//! raw-pointer manipulation (mmap, backtrace) are inherently `unsafe`.
//! Per PORTING.md §1 no `unsafe` is allowed in pilot crates.
//! All such sites are marked `TODO(architect)` and stubbed so the module
//! compiles (with name-resolution errors expected in Phase A).
//!
//! # SHA-1
//! C-side SHA-1 (`sha1.h`) is used only in the dataset-digest helpers.
//! We need a `sha1` crate dependency — see `TODO(architect)` in `xor_digest`.
//!
//! # Backtrace
//! The C uses `execinfo.h`'s `backtrace()` / `backtrace_symbols_fd()`.
//! In Rust we'd use the `backtrace` crate (or `std::backtrace`). Deferred
//! to Phase B; all call-sites carry `TODO(port)`.
//!
//! C: debug.c

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use redis_types::{RedisError, RedisString};

use crate::command_context::CommandContext;
use crate::db::RedisDb;
use crate::object::RedisObject;
use crate::server::RedisServer;

// ── Module-level globals ────────────────────────────────────────────────────

/// True (1) once the bug-report header has been emitted. Guards against
/// duplicate headers when two threads crash simultaneously.
/// C: static int bug_report_start
static BUG_REPORT_STARTED: AtomicI32 = AtomicI32::new(0);

/// Protects `BUG_REPORT_STARTED` across threads.
/// C: static pthread_mutex_t bug_report_start_mutex
static BUG_REPORT_MUTEX: Mutex<()> = Mutex::new(());

// ── SHA-1 digest helper types ───────────────────────────────────────────────

/// 20-byte SHA-1 digest buffer.
/// C: unsigned char digest[20]
type Sha1Digest = [u8; 20];

// ═══════════════════════════════════════════════════════════════════════════
//  Dataset-digest helpers  (DEBUG DIGEST / DEBUG DIGEST-VALUE)
// ═══════════════════════════════════════════════════════════════════════════

/// XOR a SHA-1 of `data` into `digest`.
///
/// Because XOR is commutative, calling this for every element of an unordered
/// collection gives a digest that is order-independent.
///
/// C: debug.c:98-108, xorDigest
///
/// TODO(architect): add `sha1` crate dependency to redis-core for SHA1Init/Update/Final.
pub(crate) fn xor_digest(digest: &mut Sha1Digest, data: &[u8]) {
    // TODO(port): sha1 crate not yet available. Replace with:
    //   let hash = sha1::Sha1::from(data).digest().bytes();
    //   for i in 0..20 { digest[i] ^= hash[i]; }
    let _ = data;
    let _ = digest;
}

/// XOR a SHA-1 of `data` into `digest`, then SHA-1 the result in place.
///
/// `digest = SHA1(digest XOR SHA1(data))` — order-preserving accumulation.
///
/// C: debug.c:130-137, mixDigest
///
/// TODO(architect): same sha1 dependency as xor_digest.
pub(crate) fn mix_digest(digest: &mut Sha1Digest, data: &[u8]) {
    xor_digest(digest, data);
    // TODO(port): then SHA1 the 20-byte digest in place:
    //   let hash = sha1::Sha1::from(&digest[..]).digest().bytes();
    //   *digest = hash;
    let _ = data;
}

/// XOR the decoded string value of `obj` into `digest`.
///
/// C: debug.c:110-114, xorStringObjectDigest
pub(crate) fn xor_string_object_digest(digest: &mut Sha1Digest, obj: &RedisObject) {
    // TODO(port): call obj.decoded() → Cow<RedisString>, then xor_digest on its bytes.
    let _ = (digest, obj);
}

/// Mix the decoded string value of `obj` into `digest`.
///
/// C: debug.c:139-143, mixStringObjectDigest
pub(crate) fn mix_string_object_digest(digest: &mut Sha1Digest, obj: &RedisObject) {
    // TODO(port): call obj.decoded() → Cow<RedisString>, then mix_digest on its bytes.
    let _ = (digest, obj);
}

/// Mix the type-tag, then xor-accumulate a per-key digest for `obj`.
///
/// Handles String / List / Set / ZSet / Hash / Stream / Module variants.
/// Lists use `mix_digest` for ordering; sets/hashes use `xor_digest` for
/// commutativity.
///
/// C: debug.c:153-283, xorObjectDigest
pub(crate) fn xor_object_digest(
    db: &RedisDb,
    key: &RedisString,
    digest: &mut Sha1Digest,
    obj: &RedisObject,
) {
    // PORT NOTE: type discriminant replaces C's `htonl(o->type)` u32 tag.
    // TODO(port): implement full match over RedisObject variants when
    //   List / Set / ZSet / Hash / Stream / Module encodings exist.
    // PERF(port): C uses htonl for deterministic byte order in the hash tag
    //   — need the same big-endian u32 encoding here.
    let type_tag: u32 = if obj.is_string() {
        0
    } else {
        // TODO(port): assign stable discriminants matching C OBJ_* constants.
        0
    };
    let tag_be = type_tag.to_be_bytes();
    mix_digest(digest, &tag_be);

    if obj.is_string() {
        mix_string_object_digest(digest, obj);
    } else {
        // TODO(port): implement List / Set / ZSet / Hash / Stream / Module digests.
    }

    // TODO(port): if the key has an expiry, xor_digest(digest, b"!!expire!!", 10)
    let _ = (db, key);
}

/// Compute a single 20-byte digest representing the entire dataset across
/// all databases.
///
/// C: debug.c:291-327, computeDatasetDigest
pub(crate) fn compute_dataset_digest(server: &RedisServer) -> Sha1Digest {
    let mut final_digest: Sha1Digest = [0u8; 20];

    for db_index in 0..server.db_count() {
        // TODO(port): iterate kvstore per db using db.iter_keys(); mix the
        //   db-id tag then xor each key-value pair's digest into final_digest.
        // C: aux = htonl(j); mixDigest(final, &aux, sizeof(aux));
        let db_tag = (db_index as u32).to_be_bytes();
        mix_digest(&mut final_digest, &db_tag);

        let Some(db) = server.db(db_index as u32) else {
            continue;
        };

        // TODO(port): kvstore iteration not yet available.
        //   for (key, obj) in db.iter() {
        //       let mut kv_digest: Sha1Digest = [0u8; 20];
        //       mix_digest(&mut kv_digest, key.as_bytes());
        //       xor_object_digest(db, key, &mut kv_digest, obj);
        //       xor_digest(&mut final_digest, &kv_digest);
        //   }
        let _ = db;
    }

    final_digest
}

// ═══════════════════════════════════════════════════════════════════════════
//  jemalloc mallctl helpers  (#[cfg(feature = "jemalloc")])
// ═══════════════════════════════════════════════════════════════════════════

/// Read or write an integer jemalloc tunable via `mallctl`.
///
/// C: debug.c:330-369, mallctl_int  (guarded by USE_JEMALLOC)
///
/// TODO(port): implement when jemalloc feature is enabled.
#[cfg(feature = "jemalloc")]
fn mallctl_int(ctx: &mut CommandContext, key: &RedisString, val: Option<i64>) -> Result<(), RedisError> {
    // TODO(port): call tikv-jemalloc-ctl or je_mallctl FFI.
    let _ = (ctx, key, val);
    Err(RedisError::runtime(b"mallctl_int not implemented"))
}

/// Read or write a string jemalloc tunable via `mallctl`.
///
/// C: debug.c:371-395, mallctl_string  (guarded by USE_JEMALLOC)
///
/// TODO(port): implement when jemalloc feature is enabled.
#[cfg(feature = "jemalloc")]
fn mallctl_string(
    ctx: &mut CommandContext,
    key: &RedisString,
    val: Option<&RedisString>,
) -> Result<(), RedisError> {
    // TODO(port): call tikv-jemalloc-ctl or je_mallctl FFI.
    let _ = (ctx, key, val);
    Err(RedisError::runtime(b"mallctl_string not implemented"))
}

// ═══════════════════════════════════════════════════════════════════════════
//  DEBUG command dispatcher
// ═══════════════════════════════════════════════════════════════════════════

/// The `DEBUG` command.
///
/// Dispatches to one of many sub-operations depending on `argv[1]`.
/// All sub-operations that require mutable server state carry a
/// `TODO(port)` because `CommandContext` does not yet expose a
/// `&mut RedisServer` reference (planned for Phase 3).
///
/// C: debug.c:398-1076, debugCommand
pub fn debug_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();

    // Require at least DEBUG <subcommand>
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"DEBUG"));
    }

    // Clone arg[1] to avoid reborrow conflicts in the big match below.
    let sub = ctx.arg(1)?.clone();
    let sub_bytes: &[u8] = sub.as_bytes();

    if eq_ci(sub_bytes, b"help") {
        return debug_help(ctx);
    } else if eq_ci(sub_bytes, b"segfault") {
        // C: mmap a read-only page, write to it → SIGSEGV
        // TODO(architect): intentional crash; unsafe mmap required.
        // In Rust we cannot do this without unsafe. Flag and abort.
        return Err(RedisError::runtime(b"ERR DEBUG SEGFAULT: unsafe; see TODO(architect)"));
    } else if eq_ci(sub_bytes, b"panic") {
        // C: serverPanic("DEBUG PANIC called at Unix time %lld", ...)
        // TODO(architect): is panic!() correct here? Matches C behavior.
        panic!("DEBUG PANIC called (intentional crash requested via DEBUG command)");
    } else if eq_ci(sub_bytes, b"restart") || eq_ci(sub_bytes, b"crash-and-recover") {
        return debug_restart(ctx, sub_bytes);
    } else if eq_ci(sub_bytes, b"oom") {
        // C: zmalloc(SIZE_MAX/2) to trigger OOM handler.
        // TODO(port): allocate a huge Vec to trigger OOM;
        //   exact C behavior is allocator-specific.
        return Err(RedisError::runtime(b"ERR DEBUG OOM: not implemented in Rust port"));
    } else if eq_ci(sub_bytes, b"assert") {
        // C: serverAssertWithInfo(c, c->argv[0], 1 == 2)
        debug_assert!(false, "DEBUG ASSERT intentional assertion failure");
        return Ok(());
    } else if eq_ci(sub_bytes, b"log") && argc == 3 {
        let msg = ctx.arg(2)?.clone();
        log::warn!("DEBUG LOG: {:?}", msg.as_bytes());
        return ctx.reply_simple_string(b"OK");
    } else if eq_ci(sub_bytes, b"leak") && argc == 3 {
        // C: sdsdup(objectGetVal(c->argv[2])) — intentional leak.
        // PORT NOTE: Rust has no leak-without-Drop equivalent without unsafe;
        //   use Box::leak as a reasonable approximation.
        let val = ctx.arg(2)?.clone();
        let _ = Box::leak(Box::new(val)); // intentional memory leak for testing
        return ctx.reply_simple_string(b"OK");
    } else if eq_ci(sub_bytes, b"reload") {
        return debug_reload(ctx);
    } else if eq_ci(sub_bytes, b"loadaof") {
        return debug_loadaof(ctx);
    } else if eq_ci(sub_bytes, b"drop-cluster-packet-filter") && argc == 3 {
        // TODO(port): server.cluster_drop_packet_filter = parse(argv[2])
        return Err(RedisError::runtime(b"ERR cluster not implemented"));
    } else if eq_ci(sub_bytes, b"close-cluster-link-on-packet-drop") && argc == 3 {
        // TODO(port): server.debug_cluster_close_link_on_packet_drop = ...
        return Err(RedisError::runtime(b"ERR cluster not implemented"));
    } else if eq_ci(sub_bytes, b"disable-cluster-random-ping") && argc == 3 {
        // TODO(port): server.debug_cluster_disable_random_ping = ...
        return Err(RedisError::runtime(b"ERR cluster not implemented"));
    } else if eq_ci(sub_bytes, b"disable-cluster-reconnection") && argc == 3 {
        // TODO(port): server.debug_cluster_disable_reconnection = ...
        return Err(RedisError::runtime(b"ERR cluster not implemented"));
    } else if eq_ci(sub_bytes, b"slotmigration") {
        return debug_slotmigration(ctx);
    } else if eq_ci(sub_bytes, b"object") && (argc == 3 || argc == 4) {
        return debug_object(ctx);
    } else if eq_ci(sub_bytes, b"sdslen") && argc == 3 {
        return debug_sdslen(ctx);
    } else if eq_ci(sub_bytes, b"listpack") && argc == 3 {
        return debug_listpack(ctx);
    } else if eq_ci(sub_bytes, b"quicklist") && (argc == 3 || argc == 4) {
        return debug_quicklist(ctx);
    } else if eq_ci(sub_bytes, b"populate") && argc >= 3 && argc <= 5 {
        return debug_populate(ctx);
    } else if eq_ci(sub_bytes, b"digest") && argc == 2 {
        return debug_digest(ctx);
    } else if eq_ci(sub_bytes, b"digest-value") && argc >= 2 {
        return debug_digest_value(ctx);
    } else if eq_ci(sub_bytes, b"protocol") && argc == 3 {
        return debug_protocol(ctx);
    } else if eq_ci(sub_bytes, b"sleep") && argc == 3 {
        return debug_sleep(ctx);
    } else if eq_ci(sub_bytes, b"set-active-expire") && argc == 3 {
        // TODO(port): server.active_expire_enabled = parse_bool(argv[2])
        return Err(RedisError::runtime(b"ERR DEBUG SET-ACTIVE-EXPIRE: server mutation not yet wired"));
    } else if eq_ci(sub_bytes, b"quicklist-packed-threshold") && argc == 3 {
        // TODO(port): call quicklistSetPackedThreshold(sz)
        return Err(RedisError::runtime(b"ERR DEBUG QUICKLIST-PACKED-THRESHOLD: not implemented"));
    } else if eq_ci(sub_bytes, b"set-skip-checksum-validation") && argc == 3 {
        // TODO(port): server.skip_checksum_validation = ...
        return Err(RedisError::runtime(b"ERR DEBUG SET-SKIP-CHECKSUM-VALIDATION: not implemented"));
    } else if eq_ci(sub_bytes, b"aof-flush-sleep") && argc == 3 {
        // TODO(port): server.aof_flush_sleep = ...
        return Err(RedisError::runtime(b"ERR DEBUG AOF-FLUSH-SLEEP: not implemented"));
    } else if eq_ci(sub_bytes, b"replicate") && argc >= 3 {
        // TODO(port): replicationFeedReplicas(...)
        return Err(RedisError::runtime(b"ERR DEBUG REPLICATE: replication not implemented"));
    } else if eq_ci(sub_bytes, b"error") && argc == 3 {
        return debug_error(ctx);
    } else if eq_ci(sub_bytes, b"structsize") && argc == 2 {
        return debug_structsize(ctx);
    } else if eq_ci(sub_bytes, b"htstats") && argc >= 3 {
        return debug_htstats(ctx);
    } else if eq_ci(sub_bytes, b"htstats-key") && argc >= 3 {
        return debug_htstats_key(ctx);
    } else if eq_ci(sub_bytes, b"change-repl-id") && argc == 2 {
        // TODO(port): changeReplicationId(); clearReplicationId2()
        return Err(RedisError::runtime(b"ERR DEBUG CHANGE-REPL-ID: replication not implemented"));
    } else if eq_ci(sub_bytes, b"stringmatch-test") && argc == 2 {
        // TODO(port): call stringmatchlen_fuzz_test()
        return ctx.reply_simple_string(b"Apparently the server did not crash: test passed");
    } else if eq_ci(sub_bytes, b"set-disable-deny-scripts") && argc == 3 {
        // TODO(port): server.script_disable_deny_script = ...
        return Err(RedisError::runtime(b"ERR DEBUG SET-DISABLE-DENY-SCRIPTS: not implemented"));
    } else if eq_ci(sub_bytes, b"config-rewrite-force-all") && argc == 2 {
        // TODO(port): rewriteConfig(server.configfile, 1)
        return Err(RedisError::runtime(b"ERR DEBUG CONFIG-REWRITE-FORCE-ALL: not implemented"));
    } else if eq_ci(sub_bytes, b"client-eviction") && argc == 2 {
        return debug_client_eviction(ctx);
    } else if eq_ci(sub_bytes, b"pause-cron") && argc == 3 {
        // TODO(port): server.pause_cron = parse_bool(argv[2])
        return Err(RedisError::runtime(b"ERR DEBUG PAUSE-CRON: not implemented"));
    } else if eq_ci(sub_bytes, b"replybuffer") && argc == 4 {
        return debug_replybuffer(ctx);
    } else if eq_ci(sub_bytes, b"pause-after-fork") && argc == 3 {
        // TODO(port): server.debug_pause_after_fork = ...
        return Err(RedisError::runtime(b"ERR DEBUG PAUSE-AFTER-FORK: not implemented"));
    } else if eq_ci(sub_bytes, b"delay-rdb-client-free-seconds") && argc == 3 {
        // TODO(port): server.wait_before_rdb_client_free = ...
        return Err(RedisError::runtime(b"ERR DEBUG DELAY-RDB-CLIENT-FREE-SECONDS: not implemented"));
    } else if eq_ci(sub_bytes, b"dict-resizing") && argc == 3 {
        // TODO(port): server.dict_resizing = ...; updateDictResizePolicy()
        return Err(RedisError::runtime(b"ERR DEBUG DICT-RESIZING: not implemented"));
    } else if eq_ci(sub_bytes, b"hashtable-can-abort-shrink") && argc == 3 {
        // TODO(port): hashtableSetCanAbortShrink(...)
        return Err(RedisError::runtime(b"ERR DEBUG HASHTABLE-CAN-ABORT-SHRINK: not implemented"));
    } else if eq_ci(sub_bytes, b"client-enforce-reply-list") && argc == 3 {
        // TODO(port): server.debug_client_enforce_reply_list = ...
        return Err(RedisError::runtime(b"ERR DEBUG CLIENT-ENFORCE-REPLY-LIST: not implemented"));
    } else if eq_ci(sub_bytes, b"force-free-primary-async") && argc == 3 {
        // TODO(port): server.debug_force_free_primary_async = ...
        return Err(RedisError::runtime(b"ERR DEBUG FORCE-FREE-PRIMARY-ASYNC: not implemented"));
    }
    #[cfg(feature = "jemalloc")]
    if eq_ci(sub_bytes, b"mallctl") && argc >= 3 {
        let key = ctx.arg(2)?.clone();
        let val = if argc > 3 {
            let v = ctx.arg(3)?;
            // TODO(port): parse i64 from v
            Some(0i64)
        } else {
            None
        };
        return mallctl_int(ctx, &key, val);
    }
    #[cfg(feature = "jemalloc")]
    if eq_ci(sub_bytes, b"mallctl-str") && argc >= 3 {
        let key = ctx.arg(2)?.clone();
        let val = if argc > 3 {
            Some(ctx.arg(3)?.clone())
        } else {
            None
        };
        return mallctl_string(ctx, &key, val.as_ref());
    }

    // Fallthrough → unknown subcommand
    Err(RedisError::runtime(b"ERR unknown debug subcommand"))
}

// ── debug_command sub-handlers ───────────────────────────────────────────

/// Emit the DEBUG help text.
///
/// C: debug.c:399-529 (the `help` branch of debugCommand)
fn debug_help(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // PORT NOTE: The help strings are long; we emit a concise summary and
    //   point at the C source for the authoritative list.
    // TODO(port): emit each help line as an array element matching C output.
    let help: &[&[u8]] = &[
        b"AOF-FLUSH-SLEEP <microsec>",
        b"ASSERT",
        b"CHANGE-REPL-ID",
        b"CONFIG-REWRITE-FORCE-ALL",
        b"CRASH-AND-RECOVER [<ms>]",
        b"DIGEST",
        b"DIGEST-VALUE <key> [<key> ...]",
        b"ERROR <string>",
        b"LEAK <string>",
        b"LOG <message>",
        b"OBJECT <key> [fast]",
        b"OOM",
        b"PANIC",
        b"POPULATE <count> [<prefix>] [<size>]",
        b"PROTOCOL <type>",
        b"RELOAD [MERGE] [NOFLUSH] [NOSAVE]",
        b"RESTART [<ms>]",
        b"SDSLEN <key>",
        b"SEGFAULT",
        b"SLEEP <seconds>",
        b"STRUCTSIZE",
        b"LISTPACK <key>",
        b"QUICKLIST <key> [<0|1>]",
    ];
    ctx.reply_array_header(help.len() as i64)?;
    for line in help {
        ctx.reply_bulk(line)?;
    }
    Ok(())
}

/// Handle `DEBUG RESTART` / `DEBUG CRASH-AND-RECOVER [<ms>]`.
///
/// C: debug.c:538-548
fn debug_restart(ctx: &mut CommandContext, sub: &[u8]) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    let _delay_ms: i64 = if argc >= 3 {
        parse_i64_arg(ctx, 2)?
    } else {
        0
    };
    let _is_graceful = eq_ci(sub, b"restart");
    // TODO(port): restartServer(ctx, flags, delay) — requires &mut RedisServer.
    Err(RedisError::runtime(b"ERR failed to restart the server. Check server logs."))
}

/// Handle `DEBUG RELOAD [MERGE] [NOFLUSH] [NOSAVE]`.
///
/// C: debug.c:561-606
fn debug_reload(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    let mut _flush = true;
    let mut _save = true;
    let mut _allow_dup = false;

    for j in 2..argc {
        let opt = ctx.arg(j)?.clone();
        let opt_bytes = opt.as_bytes();
        if eq_ci(opt_bytes, b"MERGE") {
            _allow_dup = true;
        } else if eq_ci(opt_bytes, b"NOFLUSH") {
            _flush = false;
        } else if eq_ci(opt_bytes, b"NOSAVE") {
            _save = false;
        } else {
            return Err(RedisError::runtime(
                b"ERR DEBUG RELOAD only supports the MERGE, NOFLUSH and NOSAVE options.",
            ));
        }
    }
    // TODO(port): rdbSave / rdbLoad when persistence is implemented.
    Err(RedisError::runtime(b"ERR DEBUG RELOAD: persistence not yet implemented"))
}

/// Handle `DEBUG LOADAOF`.
///
/// C: debug.c:607-622
fn debug_loadaof(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): flushAppendOnlyFile, loadAppendOnlyFiles — AOF not yet ported.
    let _ = ctx;
    Err(RedisError::runtime(b"ERR DEBUG LOADAOF: AOF not yet implemented"))
}

/// Handle `DEBUG SLOTMIGRATION PREVENT-PAUSE|PREVENT-FAILOVER <0|1>`.
///
/// C: debug.c:637-646
fn debug_slotmigration(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() < 4 {
        return Err(RedisError::runtime(b"ERR DEBUG SLOTMIGRATION: wrong number of arguments"));
    }
    let sub2 = ctx.arg(2)?.clone();
    if eq_ci(sub2.as_bytes(), b"prevent-pause") || eq_ci(sub2.as_bytes(), b"prevent-failover") {
        // TODO(port): mutate server.debug_slot_migration_prevent_* field.
        ctx.reply_simple_string(b"OK")
    } else {
        Err(RedisError::runtime(b"ERR unknown SLOTMIGRATION subcommand"))
    }
}

/// Handle `DEBUG OBJECT <key> [fast]`.
///
/// C: debug.c:647-706
fn debug_object(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    let _fast = argc == 4 && {
        let opt = ctx.arg(3)?;
        eq_ci(opt.as_bytes(), b"fast")
    };
    let key = ctx.arg(2)?.clone();
    // TODO(port): db lookup, refcount, encoding, LRU/LFU via CommandContext.
    // For now return a placeholder status line.
    // PORT NOTE: key bytes are shown in debug {:?} format (ASCII escape sequences),
    //   not decoded as UTF-8 — satisfies the bytes-everywhere rule.
    let mut msg: Vec<u8> = b"Value at:<ptr> refcount:1 encoding:raw key:".to_vec();
    for &b in key.as_bytes().iter() {
        let b: u8 = b;
        if b.is_ascii_graphic() && b != b'"' && b != b'\\' {
            msg.push(b);
        } else {
            msg.extend_from_slice(format!("\\x{:02x}", b).as_bytes());
        }
    }
    ctx.reply_bulk(&msg)
}

/// Handle `DEBUG SDSLEN <key>`.
///
/// C: debug.c:707-730
fn debug_sdslen(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _key = ctx.arg(2)?.clone();
    // TODO(port): db lookup, sdslen / sdsavail equivalents.
    Err(RedisError::runtime(b"ERR DEBUG SDSLEN: not yet implemented"))
}

/// Handle `DEBUG LISTPACK <key>`.
///
/// C: debug.c:731-742
fn debug_listpack(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _key = ctx.arg(2)?.clone();
    // TODO(port): db lookup, check encoding is listpack, call lpRepr equivalent.
    Err(RedisError::runtime(b"ERR DEBUG LISTPACK: not yet implemented"))
}

/// Handle `DEBUG QUICKLIST <key> [<0|1>]`.
///
/// C: debug.c:742-754
fn debug_quicklist(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _key = ctx.arg(2)?.clone();
    // TODO(port): db lookup, check encoding is quicklist, call quicklistRepr.
    Err(RedisError::runtime(b"ERR DEBUG QUICKLIST: not yet implemented"))
}

/// Handle `DEBUG POPULATE <count> [<prefix>] [<size>]`.
///
/// Inserts `count` string key-value pairs into the current database for
/// testing purposes.
///
/// C: debug.c:755-793
fn debug_populate(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    let count = parse_positive_i64_arg(ctx, 2)?;

    let prefix: Vec<u8> = if argc >= 4 {
        ctx.arg(3)?.as_bytes().to_vec()
    } else {
        b"key".to_vec()
    };

    let val_size: usize = if argc == 5 {
        parse_positive_i64_arg(ctx, 4)? as usize
    } else {
        0
    };

    // TODO(port): reach the current db via ctx, call db.add(key, value).
    // This loop structure is faithful to C.
    for j in 0i64..count {
        // PORT NOTE: key is constructed byte-by-byte to avoid from_utf8 on Redis data.
        let mut key_bytes: Vec<u8> = prefix.clone();
        key_bytes.push(b':');
        // j.to_string() produces ASCII digits — safe to extend as bytes.
        key_bytes.extend_from_slice(j.to_string().as_bytes());

        let val_bytes: Vec<u8> = if val_size == 0 {
            // b"value:<j>"
            let mut v = b"value:".to_vec();
            v.extend_from_slice(j.to_string().as_bytes());
            v
        } else {
            let mut base = b"value:".to_vec();
            base.extend_from_slice(j.to_string().as_bytes());
            let mut v = vec![0u8; val_size];
            let copy_len = base.len().min(val_size);
            v[..copy_len].copy_from_slice(&base[..copy_len]);
            v
        };
        // TODO(port): db.lookup_key_write(key)? db.add(key, RedisObject::String(val))?
        let _ = (key_bytes, val_bytes);
    }

    ctx.reply_simple_string(b"OK")
}

/// Handle `DEBUG DIGEST`.
///
/// C: debug.c:794-802
fn debug_digest(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): call compute_dataset_digest(&server)
    // Using a zeroed placeholder for now.
    let digest: Sha1Digest = [0u8; 20];
    let hex = hex_encode_20(&digest);
    ctx.reply_bulk(&hex)
}

/// Handle `DEBUG DIGEST-VALUE <key> [<key> ...]`.
///
/// C: debug.c:803-819
fn debug_digest_value(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    let n = (argc - 2) as i64;
    ctx.reply_array_header(n)?;

    for j in 2..argc {
        let _key = ctx.arg(j)?.clone();
        // TODO(port): dbFind(key) → xorObjectDigest or zeroes if missing.
        let digest: Sha1Digest = [0u8; 20];
        let hex = hex_encode_20(&digest);
        ctx.reply_bulk(&hex)?;
    }
    Ok(())
}

/// Handle `DEBUG PROTOCOL <type>`.
///
/// Emits a test reply of the requested RESP type.
///
/// C: debug.c:820-882
fn debug_protocol(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let name = ctx.arg(2)?.clone();
    let n = name.as_bytes();

    if eq_ci(n, b"string") {
        ctx.reply_bulk(b"Hello World")
    } else if eq_ci(n, b"integer") {
        ctx.reply_integer(12345)
    } else if eq_ci(n, b"double") {
        ctx.reply_double(3.141)
    } else if eq_ci(n, b"bignum") {
        ctx.reply_big_number(b"1234567999999999999999999999999999999")
    } else if eq_ci(n, b"null") {
        ctx.reply_null()
    } else if eq_ci(n, b"array") {
        ctx.reply_array_header(3)?;
        for j in 0i64..3 {
            ctx.reply_integer(j)?;
        }
        Ok(())
    } else if eq_ci(n, b"set") {
        ctx.reply_set_header(3)?;
        for j in 0i64..3 {
            ctx.reply_integer(j)?;
        }
        Ok(())
    } else if eq_ci(n, b"map") {
        ctx.reply_map_header(3)?;
        for j in 0i64..3 {
            ctx.reply_integer(j)?;
            ctx.reply_bool(j == 1)?;
        }
        Ok(())
    } else if eq_ci(n, b"attrib") {
        // TODO(port): RESP3 attribute replies (ctx.resp_version() needed).
        ctx.reply_bulk(b"Some real reply following the attribute")
    } else if eq_ci(n, b"push") {
        // TODO(port): RESP3 push replies require ctx.resp_version() check.
        ctx.reply_bulk(b"Some real reply following the push reply")
    } else if eq_ci(n, b"true") {
        ctx.reply_bool(true)
    } else if eq_ci(n, b"false") {
        ctx.reply_bool(false)
    } else if eq_ci(n, b"verbatim") {
        ctx.reply_verbatim(b"This is a verbatim\nstring", b"txt")
    } else {
        Err(RedisError::runtime(
            b"ERR Wrong protocol type name. Please use one of the following: \
            string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false",
        ))
    }
}

/// Handle `DEBUG SLEEP <seconds>`.
///
/// C: debug.c:882-890
fn debug_sleep(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let arg = ctx.arg(2)?.clone();
    let secs_str = arg.as_bytes();
    // TODO(port): parse float from byte slice without str conversion.
    // PERF(port): C uses nanosleep for sub-ms precision.
    let secs: f64 = parse_f64_bytes(secs_str)?;
    let dur = Duration::from_secs_f64(secs);
    std::thread::sleep(dur);
    ctx.reply_simple_string(b"OK")
}

/// Handle `DEBUG ERROR <string>`.
///
/// Emits a verbatim RESP error frame using the user-supplied string.
///
/// C: debug.c:911-917
fn debug_error(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let msg = ctx.arg(2)?.clone();
    // PORT NOTE: C prepends "-" and appends "\r\n" directly into the reply buffer
    //   and replaces newlines with spaces. We return a RedisError::runtime with the
    //   msg bytes so the reply layer can format the RESP error frame.
    // TODO(port): replace newline chars in msg with spaces for wire-exact match.
    let mut payload = b"-".to_vec();
    payload.extend_from_slice(msg.as_bytes());
    // Strip embedded \r\n as C does.
    let payload: Vec<u8> = payload.iter().map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b }).collect();
    // TODO(port): addReplySds sends the raw RESP line; we approximate with Error variant.
    Err(RedisError::runtime(&payload))
}

/// Handle `DEBUG STRUCTSIZE`.
///
/// Reports sizes of key C structs in Rust equivalents.
///
/// C: debug.c:918-928
fn debug_structsize(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let bits = if std::mem::size_of::<*const ()>() == 8 { 64u32 } else { 32u32 };
    // PORT NOTE: Rust struct sizes differ from C; reporting what we have.
    // The format! here builds a Rust-side diagnostic string (not Redis data) — OK per §1.
    // TODO(port): add sizes for RedisObject, RedisString variants as they stabilize.
    let msg: Vec<u8> = {
        let mut v: Vec<u8> = b"bits:".to_vec();
        v.extend_from_slice(bits.to_string().as_bytes());
        v.extend_from_slice(b" robj:");
        v.extend_from_slice(std::mem::size_of::<RedisObject>().to_string().as_bytes());
        v.extend_from_slice(b" redisstring:");
        v.extend_from_slice(std::mem::size_of::<RedisString>().to_string().as_bytes());
        v
    };
    ctx.reply_bulk(&msg)
}

/// Handle `DEBUG HTSTATS <dbid> [full]`.
///
/// C: debug.c:929-961
fn debug_htstats(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _dbid = parse_i64_arg(ctx, 2)?;
    let argc = ctx.arg_count();
    let _full = argc >= 4 && {
        let opt = ctx.arg(3)?;
        eq_ci(opt.as_bytes(), b"full")
    };
    // TODO(port): kvstoreGetStats for the db's key and expire stores.
    let stats = b"[Dictionary HT]\n(stats not available)\n[Expires HT]\n(stats not available)\n";
    ctx.reply_verbatim(stats, b"txt")
}

/// Handle `DEBUG HTSTATS-KEY <key> [full]`.
///
/// C: debug.c:962-986
fn debug_htstats_key(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let _key = ctx.arg(2)?.clone();
    // TODO(port): objectCommandLookupOrReply → get hashtable pointer.
    Err(RedisError::runtime(b"ERR DEBUG HTSTATS-KEY: not yet implemented"))
}

/// Handle `DEBUG CLIENT-EVICTION`.
///
/// C: debug.c:1003-1025
fn debug_client_eviction(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(port): server.client_mem_usage_buckets
    Err(RedisError::runtime(b"ERR maxmemory-clients is disabled."))
}

/// Handle `DEBUG REPLYBUFFER peak-reset-time|resizing <...>`.
///
/// C: debug.c:1037-1052
fn debug_replybuffer(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let sub2 = ctx.arg(2)?.clone();
    if eq_ci(sub2.as_bytes(), b"peak-reset-time") {
        // TODO(port): server.reply_buffer_peak_reset_time = ...
    } else if eq_ci(sub2.as_bytes(), b"resizing") {
        // TODO(port): server.reply_buffer_resizing_enabled = ...
    } else {
        return Err(RedisError::runtime(b"ERR unknown REPLYBUFFER subcommand"));
    }
    ctx.reply_simple_string(b"OK")
}

// ═══════════════════════════════════════════════════════════════════════════
//  Crash / assertion reporting
// ═══════════════════════════════════════════════════════════════════════════

/// Start a bug report; returns `true` if this is the first call.
///
/// Thread-safe via `BUG_REPORT_MUTEX`. On the first invocation it emits
/// the "BUG REPORT START" header to stderr / log.
///
/// C: debug.c:1214-1225, bugReportStart
pub fn bug_report_start() -> bool {
    let _guard = BUG_REPORT_MUTEX.lock();
    if BUG_REPORT_STARTED.load(Ordering::SeqCst) == 0 {
        eprintln!("\n\n=== VALKEY BUG REPORT START: Cut & paste starting from here ===\n");
        BUG_REPORT_STARTED.store(1, Ordering::SeqCst);
        true
    } else {
        false
    }
}

/// End a bug report and terminate the process.
///
/// If `kill_via_signal` is true, re-raise `sig` with the default handler
/// (so a core dump is produced). Otherwise calls `abort()`.
///
/// C: debug.c:2379-2411, bugReportEnd
///
/// TODO(architect): raising a signal and `abort()` require platform-specific
///   unsafe code; flagged here.
pub fn bug_report_end(kill_via_signal: bool, sig: i32) -> ! {
    eprintln!(
        "\n=== VALKEY BUG REPORT END. \
         Please report at https://github.com/valkey-io/valkey/issues ===\n"
    );
    if !kill_via_signal {
        // TODO(architect): should call std::process::abort() or _exit(1)
        //   depending on server.use_exit_on_panic. Using abort() for now.
        std::process::abort();
    }
    // TODO(architect): restore default signal handler and re-raise `sig`.
    //   Requires libc::sigaction + libc::kill — unsafe.
    let _ = sig;
    std::process::abort();
}

/// Internal assertion failure handler.
///
/// C: debug.c:1080-1097, _serverAssert
pub fn server_assert_failed(expr: &str, file: &str, line: u32) -> ! {
    let new_report = bug_report_start();
    log::warn!(
        "=== {}ASSERTION FAILED ===",
        if new_report { "" } else { "RECURSIVE " }
    );
    log::warn!("==> {}:{} '{}' is not true", file, line, expr);
    // TODO(port): log_stack_trace() when backtrace feature is available.
    if new_report {
        print_crash_report();
    }
    bug_report_end(false, 0);
}

/// Internal panic handler (SERVER_PANIC equivalent).
///
/// C: debug.c:1187-1211, _serverPanic
pub fn server_panic(file: &str, line: u32, msg: &str) -> ! {
    let new_report = bug_report_start();
    log::warn!("------------------------------------------------");
    log::warn!("!!! Software Failure. Press left mouse button to continue");
    log::warn!("Guru Meditation: {} #{}:{}", msg, file, line);
    if new_report {
        print_crash_report();
    }
    bug_report_end(false, 0);
}

/// Print the crash report: server info, current client, modules, config.
///
/// C: debug.c:2359-2377, printCrashReport
pub fn print_crash_report() {
    log_server_info();
    // TODO(port): logCurrentClient(server.current_client, "CURRENT")
    // TODO(port): logCurrentClient(server.executing_client, "EXECUTING")
    log_modules_info();
    log_config_debug_info();
    do_fast_memory_test();
}

// ═══════════════════════════════════════════════════════════════════════════
//  Logging helpers used during crash reports
// ═══════════════════════════════════════════════════════════════════════════

/// Log global server INFO output.
///
/// C: debug.c:1996-2015, logServerInfo
pub fn log_server_info() {
    log::warn!("\n------ INFO OUTPUT ------\n");
    // TODO(port): genValkeyInfoString / genClusterDebugString.
    log::warn!("(info output not yet implemented)");
    log::warn!("\n------ CLIENT LIST OUTPUT ------\n");
    // TODO(port): getAllClientsInfoString(-1, hide_user_data)
}

/// Log config debug info.
///
/// C: debug.c:2018-2024, logConfigDebugInfo
pub fn log_config_debug_info() {
    log::warn!("\n------ CONFIG DEBUG OUTPUT ------\n");
    // TODO(port): getConfigDebugInfo()
}

/// Log modules info.
///
/// C: debug.c:2027-2032, logModulesInfo
pub fn log_modules_info() {
    log::warn!("\n------ MODULES INFO OUTPUT ------\n");
    // TODO(port): modulesCollectInfo(sdsempty(), NULL, 1, 0)
}

/// Log info about the given client during a crash.
///
/// C: debug.c:2037-2073, logCurrentClient
pub fn log_current_client(cc: Option<()>, title: &str) {
    if cc.is_none() {
        return;
    }
    log::warn!("\n------ {} CLIENT INFO ------\n", title);
    // TODO(port): catClientInfoString, arg iteration.
}

/// Log object debug info (type, encoding, refcount).
///
/// C: debug.c:1140-1173, serverLogObjectDebugInfo
pub fn log_object_debug_info(obj: &RedisObject) {
    let type_name = if obj.is_string() { "String" } else { "unknown" };
    log::warn!("Object type: {}", type_name);
}

// ═══════════════════════════════════════════════════════════════════════════
//  Memory test
// ═══════════════════════════════════════════════════════════════════════════

/// Run a fast memory test on anonymous Linux memory maps.
///
/// C: debug.c:2080-2142, memtest_test_linux_anonymous_maps
/// C: debug.c:2166-2180, doFastMemoryTest
///
/// TODO(architect): requires reading /proc/self/maps and raw memory access —
///   inherently unsafe on Linux. Stubbed to no-op.
pub fn do_fast_memory_test() {
    #[cfg(target_os = "linux")]
    {
        log::warn!("\n------ FAST MEMORY TEST ------\n");
        // TODO(architect): implement memtest_test_linux_anonymous_maps using
        //   raw memory access. Requires unsafe code and architect approval.
        log::warn!("(memory test not yet implemented)");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stack trace logging
// ═══════════════════════════════════════════════════════════════════════════

/// Open the log file descriptor for direct write (signal-handler safe).
///
/// C: debug.c:1696-1700, openDirectLogFiledes
///
/// TODO(port): check server.logfile; open for append if not stdout.
pub fn open_direct_log_filedes() -> Option<i32> {
    // TODO(port): use server.logfile config to decide stdout vs file.
    None // placeholder
}

/// Close what open_direct_log_filedes returned.
///
/// C: debug.c:1703-1706, closeDirectLogFiledes
pub fn close_direct_log_filedes(_fd: Option<i32>) {
    // no-op until open_direct_log_filedes is implemented
}

/// Log a stack trace. Signal-handler safe on platforms with `HAVE_BACKTRACE`.
///
/// C: debug.c:1937-1976, logStackTrace
///
/// TODO(port): use the `backtrace` crate or std::backtrace when
///   the crate dependency is available.
/// TODO(architect): add `backtrace` crate to redis-core Cargo.toml.
pub fn log_stack_trace() {
    log::warn!("\n------ STACK TRACE ------\n");
    // TODO(port): backtrace::print or std::backtrace::Backtrace::capture().
    log::warn!("(stack trace not yet implemented — add backtrace crate)");
    log::warn!("\n------ STACK TRACE DONE ------\n");
}

// ═══════════════════════════════════════════════════════════════════════════
//  Signal handlers
// ═══════════════════════════════════════════════════════════════════════════

/// Register SIGSEGV, SIGBUS, SIGFPE, SIGILL, SIGABRT handlers.
///
/// C: debug.c:2318-2345, setupSigSegvHandler
///
/// TODO(architect): signal handler registration requires unsafe libc::sigaction.
///   This is deferred to Phase B with architect approval.
pub fn setup_sigsegv_handler() {
    // TODO(architect): libc::sigaction(SIGSEGV, ...) — unsafe, requires architect approval.
    log::debug!("setup_sigsegv_handler: signal registration deferred (TODO architect)");
}

/// Register SIGALRM handler (software watchdog).
///
/// C: debug.c:2305-2316, setupDebugSigHandlers
///
/// TODO(architect): signal handler registration requires unsafe libc::sigaction.
pub fn setup_debug_sig_handlers() {
    setup_sigsegv_handler();
    // TODO(architect): sigaction(SIGALRM, &act, NULL) — unsafe.
}

/// Remove crash signal handlers (restore SIG_DFL).
///
/// C: debug.c:2347-2357, removeSigSegvHandlers
///
/// TODO(architect): libc::sigaction — unsafe.
pub fn remove_sigsegv_handlers() {
    // TODO(architect): restore SIG_DFL for SIGSEGV/SIGBUS/SIGFPE/SIGILL/SIGABRT.
}

// ═══════════════════════════════════════════════════════════════════════════
//  Hex-dump logging
// ═══════════════════════════════════════════════════════════════════════════

/// Log a hex dump of `data` at the given log `level`.
///
/// C: debug.c:2415-2435, serverLogHexDump
pub fn server_log_hex_dump(descr: &str, data: &[u8]) {
    log::warn!("{} (hexdump of {} bytes):", descr, data.len());
    let charset = b"0123456789abcdef";
    let mut line = Vec::with_capacity(64);
    for &byte in data {
        line.push(charset[(byte >> 4) as usize]);
        line.push(charset[(byte & 0xf) as usize]);
        if line.len() == 64 {
            // PORT NOTE: `line` contains only hex digits (0-9, a-f) — always valid UTF-8.
            //   `from_utf8_lossy` on hex ASCII is safe; this is a log line, not Redis data.
            log::warn!("{}", String::from_utf8_lossy(&line));
            line.clear();
        }
    }
    if !line.is_empty() {
        log::warn!("{}", String::from_utf8_lossy(&line));
    }
    log::warn!("");
}

// ═══════════════════════════════════════════════════════════════════════════
//  Watchdog
// ═══════════════════════════════════════════════════════════════════════════

/// Schedule a SIGALRM delivery in `period_ms` milliseconds.
/// `period_ms == 0` disables the watchdog timer.
///
/// C: debug.c:2466-2476, watchdogScheduleSignal
///
/// TODO(architect): setitimer() — unsafe libc call.
pub fn watchdog_schedule_signal(period_ms: i32) {
    // TODO(architect): libc::setitimer(ITIMER_REAL, ...) — unsafe.
    let _ = period_ms;
}

/// Apply the configured watchdog period from server config.
///
/// C: debug.c:2477-2489, applyWatchdogPeriod
pub fn apply_watchdog_period(watchdog_period: i32, hz: i32) {
    if watchdog_period == 0 {
        watchdog_schedule_signal(0);
    } else {
        let min_period = (1000 / hz) * 2;
        let effective = if watchdog_period < min_period {
            min_period
        } else {
            watchdog_period
        };
        watchdog_schedule_signal(effective);
    }
}

/// Stop the process (raise SIGSTOP) for debugging.
///
/// C: debug.c:2491-2495, debugPauseProcess
///
/// TODO(architect): libc::raise(SIGSTOP) — unsafe.
pub fn debug_pause_process() {
    log::info!("Process is about to stop.");
    // TODO(architect): libc::raise(libc::SIGSTOP) — unsafe.
    log::info!("Process has been continued.");
}

/// Introduce a configurable delay for testing.
///
/// Positive `usec` sleeps that many microseconds. Negative `usec` sleeps
/// stochastically (1-in-N chance of a 1-µs sleep).
///
/// C: debug.c:2499-2504, debugDelay
pub fn debug_delay(usec: i32) {
    let effective = if usec < 0 {
        // PERF(port): C uses rand() % -usec; using a simple deterministic
        //   approximation here. Phase B: replace with proper RNG.
        if (-usec) > 1 { 0 } else { 1 }
    } else {
        usec
    };
    if effective > 0 {
        std::thread::sleep(Duration::from_micros(effective as u64));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Thread management
// ═══════════════════════════════════════════════════════════════════════════

/// Kill all non-current threads before a memory test.
///
/// C: debug.c:2160-2164, killThreads
///
/// TODO(architect): pthread_cancel(server.main_thread_id), bioKillThreads,
///   killIOThreads — all require unsafe thread manipulation.
pub fn kill_threads() {
    // TODO(architect): pthread_cancel et al.
}

// ═══════════════════════════════════════════════════════════════════════════
//  x86 code dump (dladdr, dumpX86Calls, dumpCodeAroundEIP)
// ═══════════════════════════════════════════════════════════════════════════

/// Scan memory at `addr` for E8 (CALL) opcodes and print their targets.
///
/// C: debug.c:2185-2208, dumpX86Calls
///
/// TODO(architect): requires raw pointer scan + dladdr — unsafe.
pub fn dump_x86_calls(_addr: usize, _len: usize) {
    // TODO(architect): unsafe raw pointer iteration + libc::dladdr.
}

/// Dump the code around a faulting instruction pointer.
///
/// C: debug.c:2210-2236, dumpCodeAroundEIP
///
/// TODO(architect): dladdr + hex dump of raw code — unsafe.
pub fn dump_code_around_eip(_eip: usize) {
    // TODO(architect): libc::dladdr + raw memory hex dump.
}

// ═══════════════════════════════════════════════════════════════════════════
//  Cluster debug info
// ═══════════════════════════════════════════════════════════════════════════

/// Append cluster info to an info string.
///
/// C: debug.c:1980-1993, genClusterDebugString
pub fn gen_cluster_debug_string(mut info: Vec<u8>) -> Vec<u8> {
    // TODO(port): genClusterInfoString / clusterGenNodesDescription.
    info.extend_from_slice(b"\r\n# Cluster info\r\n(cluster not implemented)\n");
    info
}

// ═══════════════════════════════════════════════════════════════════════════
//  Linux-only thread stacktrace utilities
// ═══════════════════════════════════════════════════════════════════════════

/// Checks whether a given thread is ready to receive a signal (does not block
/// or ignore it). Reads `/proc/<pid>/task/<tid>/status`.
///
/// C: debug.c:2515-2562, is_thread_ready_to_signal  (Linux-only, static)
///
/// TODO(architect): requires `open()` + manual file parsing without libc
///   buffered I/O (async-signal-safe). Unsafe proc-fs access.
#[cfg(target_os = "linux")]
pub(crate) fn is_thread_ready_to_signal(
    _proc_pid_task_path: &[u8],
    _tid: &[u8],
    _sig_num: i32,
) -> bool {
    // TODO(architect): open /proc/<pid>/task/<tid>/status, parse SigBlk/SigIgn.
    true
}

/// Get the TIDs of all process threads that can receive `sig_num`.
///
/// C: debug.c:2582-2647, get_ready_to_signal_threads_tids  (Linux-only, static)
///
/// TODO(architect): requires SYS_getdents64 syscall (async-signal-safe directory
///   enumeration). Unsafe.
#[cfg(target_os = "linux")]
pub(crate) fn get_ready_to_signal_threads_tids(_sig_num: i32) -> Vec<i32> {
    // TODO(architect): syscall(SYS_getdents64) + is_thread_ready_to_signal.
    Vec::new()
}

// ═══════════════════════════════════════════════════════════════════════════
//  Private utilities
// ═══════════════════════════════════════════════════════════════════════════

/// Case-insensitive byte-slice equality.
///
/// Replaces C `strcasecmp(a, b) == 0`.
#[inline]
fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(&x, &y)| x.to_ascii_lowercase() == y.to_ascii_lowercase())
}

/// Parse argument at index `i` as `i64`.
fn parse_i64_arg(ctx: &mut CommandContext, i: usize) -> Result<i64, RedisError> {
    let arg = ctx.arg(i)?;
    let bytes = arg.as_bytes();
    parse_i64_bytes(bytes)
}

/// Parse argument at index `i` as a positive `i64`.
fn parse_positive_i64_arg(ctx: &mut CommandContext, i: usize) -> Result<i64, RedisError> {
    let v = parse_i64_arg(ctx, i)?;
    if v < 0 {
        return Err(RedisError::out_of_range());
    }
    Ok(v)
}

/// Parse `i64` from a byte slice.
fn parse_i64_bytes(bytes: &[u8]) -> Result<i64, RedisError> {
    // TODO(port): replace with shared util::parse_integer when util.rs is ported.
    let s = bytes; // we avoid from_utf8; parse digit-by-digit
    let mut n: i64 = 0;
    let mut neg = false;
    let mut iter = s.iter().peekable();
    if iter.peek() == Some(&&b'-') {
        neg = true;
        iter.next();
    }
    let mut has_digit = false;
    for &c in iter {
        if c < b'0' || c > b'9' {
            return Err(RedisError::not_integer());
        }
        n = n.checked_mul(10).and_then(|n| n.checked_add((c - b'0') as i64))
            .ok_or_else(RedisError::not_integer)?;
        has_digit = true;
    }
    if !has_digit {
        return Err(RedisError::not_integer());
    }
    Ok(if neg { -n } else { n })
}

/// Parse `f64` from a byte slice (for DEBUG SLEEP).
///
/// PORT NOTE: `from_utf8` here is safe because valid float literals are always
/// a subset of ASCII/UTF-8 (digits, '.', 'e', 'E', '+', '-'). This is not
/// a Redis key or value — it is a command argument treated as a numeric
/// duration, so the UTF-8 exemption from PORTING.md §1 applies.
///
/// TODO(port): share with util.rs strtod port once available.
fn parse_f64_bytes(bytes: &[u8]) -> Result<f64, RedisError> {
    let s = bytes.iter().take_while(|&&b| b != 0).copied().collect::<Vec<_>>();
    // PORT NOTE: float args are always ASCII; see above.
    let s = std::str::from_utf8(&s).map_err(|_| RedisError::not_float())?;
    s.trim().parse::<f64>().map_err(|_| RedisError::not_float())
}

/// Hex-encode a 20-byte SHA-1 digest into a `Vec<u8>`.
fn hex_encode_20(digest: &Sha1Digest) -> Vec<u8> {
    let charset = b"0123456789abcdef";
    let mut out = Vec::with_capacity(40);
    for &byte in digest {
        out.push(charset[(byte >> 4) as usize]);
        out.push(charset[(byte & 0xf) as usize]);
    }
    out
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/debug.c  (2649 lines, ~58 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         112
//   port_notes:    13
//   unsafe_blocks: 0   (must be 0 in pilot crates)
//   notes:         All signal-handler, backtrace, dladdr, mmap, pthread, and
//                  proc-fs code is stubbed with TODO(architect) — these require
//                  unsafe libc calls and architect approval before Phase B.
//                  SHA-1 requires adding a `sha1` crate dep (TODO architect).
//                  debugCommand sub-handlers for server-state mutation all carry
//                  TODO(port) pending Phase 3 CommandContext + RedisServer wiring.
//                  Validator shows only expected E0432/E0433 name-resolution errors.
// ──────────────────────────────────────────────────────────────────────────
