//! MULTI/EXEC transaction block and WATCH/UNWATCH CAS implementation.
//!
//! C source: `reference/valkey/src/multi.c` (585 lines, 18 functions)
//! Crate: `redis-commands`
//!
//! Implements:
//! - MULTI / EXEC / DISCARD command handlers
//! - WATCH / UNWATCH command handlers
//! - Per-client multi-state lifecycle (init, reset, free)
//! - Server-side key-touching that marks watching clients as dirty
//!   (called from `db.c` / `t_*.c` on every write operation)
//!
//! ## Structural differences from C
//!
//! The C implementation uses intrusive linked lists (`listNode` embedded
//! directly inside `watchedKey`) so that a single node can live in both
//! the client's per-transaction list and the db's per-key client list.
//! This lets removals from both structures happen in O(1).
//!
//! In this Phase-A port the intrusive trick is replaced with:
//! - `MultiState::watched_keys: Vec<WatchedKey>` — client-side list
//! - `MultiState::watched_keys_by_db: HashMap<i32, HashSet<RedisString>>` —
//!   per-db O(1) membership check
//!
//! The db-side index (`db.watched_keys`) is accessed via
//! `RedisDb::watched_keys_lookup` / `_insert` / `_remove` stubs here;
//! the concrete field must be added to `RedisDb` before Phase B.
//!
//! ## Architect items
//!
//! TODO(architect): `MultiState` and `MultiCmd` belong in
//! `redis-core/src/client.rs` (or `redis-core/src/multi_state.rs`) because
//! `Client` owns them.  They are defined here for Phase A.  Before Phase B
//! they must migrate: `redis-core` cannot depend on `redis-commands`.
//!
//! TODO(architect): `CommandRef` — the Rust equivalent of `serverCommand *`.
//! Phase A uses a function-pointer alias `CommandFn`; the actual shape (static
//! dispatch table entry, trait object, enum variant) is an architect decision.
//!
//! TODO(architect): `CommandContext::client_mut()` — mutable borrow of the
//! `Client` from within `CommandContext`.  Several functions here need it.
//!
//! TODO(architect): `CommandContext::call(flags)` — executes a queued command
//! during EXEC.  Depends on Phase 3 dispatch table.
//!
//! TODO(architect): `RedisDb::watched_keys` field — per-db dict mapping a key
//! to the list of watching clients.  Must be added to `RedisDb` in
//! `redis-core/src/db.rs` before Phase B.
//!
//! TODO(architect): `RedisServer::watching_clients` counter — global count of
//! clients with at least one active WATCH; needed by `watch_for_key` and
//! `unwatch_all_keys`.

use std::collections::{HashMap, HashSet};

use redis_core::client::{Client, MultiCmd, MultiState, WatchedKey};
use redis_core::command_context::CommandContext;
use redis_core::db::RedisDb;
use redis_core::object::RedisObject;
use redis_core::server::RedisServer;
use redis_types::{RedisError, RedisString};

// ── CMD_CALL flags (C: server.h:574-579) ────────────────────────────────────

/// Suppress all propagation side-effects (used for AOF client replay).
pub const CMD_CALL_NONE: u32 = 0;
/// Propagate to AOF.
pub const CMD_CALL_PROPAGATE_AOF: u32 = 1 << 0;
/// Propagate to replicas.
pub const CMD_CALL_PROPAGATE_REPL: u32 = 1 << 1;
/// Full propagation (AOF + repl) — default for normal command execution.
pub const CMD_CALL_FULL: u32 = CMD_CALL_PROPAGATE_AOF | CMD_CALL_PROPAGATE_REPL;

// ── Local types ──────────────────────────────────────────────────────────────

/// Placeholder type for a reference into the server command table.
///
/// C: `struct serverCommand *` field in `multiCmd`.
///
/// TODO(architect): Replace with the canonical command-dispatch type once the
/// Phase 3 command-table architecture is settled.
pub type CommandFn = fn(&mut CommandContext) -> Result<(), RedisError>;

// PORT NOTE: `MultiCmd`, `MultiState`, and `WatchedKey` moved to
// `redis-core::client` to live alongside `Client` (which owns the state).
// Architect TODO from the original Phase A salvage has been honoured.

// ── Client multi-state lifecycle ─────────────────────────────────────────────

/// Lazily initialise `client.mstate` for the given db.
///
/// C: `initClientMultiState` in `multi.c:35-39`.
pub fn init_client_multi_state(client: &mut Client) {
    if client.mstate.is_some() {
        return;
    }
    let db_id = client.db_id(); // TODO(port): Client::db_id() accessor
    client.mstate = Some(Box::new(MultiState::new(db_id)));
}

/// Free queued commands from `mstate`, releasing argument storage.
///
/// C: `freeClientMultiStateCmds` in `multi.c:41-52`.
///
/// In C this loops over `multiCmd.argv[i]` and calls `decrRefCount` + `zfree`.
/// In Rust, dropping `MultiState::commands` drops each `MultiCmd` and its
/// `argv: Vec<RedisObject>` automatically.
fn free_client_multi_state_cmds(mstate: &mut MultiState) {
    mstate.commands.clear();
    mstate.alloc_count = 0;
}

/// Release all per-db watched-keys hashtables from `mstate`.
///
/// C: `freeClientMultiWatchedKeysByDB` in `multi.c:54-65`.
fn free_client_multi_watched_keys_by_db(mstate: &mut MultiState) {
    mstate.watched_keys_by_db.clear();
}

/// Release all resources associated with `client.mstate`.
///
/// C: `freeClientMultiState` in `multi.c:68-76`.
pub fn free_client_multi_state(client: &mut Client) {
    if client.mstate.is_none() {
        return;
    }
    if let Some(mstate) = client.mstate.as_mut() {
        free_client_multi_state_cmds(mstate);
        free_client_multi_watched_keys_by_db(mstate);
    }
    // unwatchAllKeys must be called before we drop mstate so the db-side index
    // is cleaned up first.  Logically equivalent to C's call order.
    unwatch_all_keys(client); // TODO(port): needs &mut RedisDb access
    client.mstate = None;
}

/// Reset queued commands and flag counters without destroying watch state.
///
/// C: `resetClientMultiState` in `multi.c:78-88`.
pub fn reset_client_multi_state(client: &mut Client) {
    let db_id = client.db_id();
    let mstate = match client.mstate.as_mut() {
        Some(m) if !m.commands.is_empty() => m,
        _ => return,
    };
    free_client_multi_state_cmds(mstate);
    mstate.cmd_flags = 0;
    mstate.cmd_inv_flags = 0;
    mstate.argv_len_sums = 0;
    mstate.transaction_db_id = db_id;
}

// ── Command queueing ─────────────────────────────────────────────────────────

/// Append the current client command to the MULTI queue.
///
/// C: `queueMultiCommand` in `multi.c:91-137`.
///
/// Called by the command dispatcher when the client is inside a MULTI block.
pub fn queue_multi_command(client: &mut Client, cmd_flags: u64) {
    // No-op if the transaction is already doomed: saves memory in pipeline
    // scenarios where the client keeps sending after a dirty_cas/dirty_exec.
    // C: multi.c:98
    if client.flag_dirty_cas() || client.flag_dirty_exec() {
        // TODO(port): Client::flag_dirty_cas() / flag_dirty_exec() accessors
        return;
    }

    init_client_multi_state(client);

    let argv_len_snapshot = client.argv_len();
    let argc_snapshot = client.argc();
    let cmd_snapshot = client.current_cmd_fn();
    let slot_snapshot = client.slot();
    let argv_snapshot = client.take_argv();

    let mstate = client.mstate.as_mut().expect("mstate just initialised");

    // Lazily allocate with initial capacity 2 (matching C comment at line 103).
    if mstate.commands.is_empty() {
        mstate.commands.reserve(2);
        mstate.alloc_count = 2;
    }

    // TODO(port): The C code copies c->argv / c->argc / c->cmd / c->argv_len /
    // c->slot into the new multiCmd and then NULLs out c->argv.  In Rust,
    // CommandContext (or Client) must expose a `take_argv()` method that moves
    // the argv Vec into the queued MultiCmd without cloning.  The exact API
    // depends on how CommandContext owns its argument list (Phase B decision).
    let mc = MultiCmd {
        argv: argv_snapshot,
        argv_len: argv_len_snapshot,
        argc: argc_snapshot,
        cmd: cmd_snapshot,
        slot: slot_snapshot,
    };

    // If the queued command is SELECT, track the new transaction db.
    // C: multi.c:117-124 — calls mc->cmd->get_dbid_args if available.
    // TODO(port): This requires introspection of the command descriptor (which
    // db ids the SELECT argument targets).  Defer to Phase B when the command
    // table is fully wired.
    //
    // Placeholder: if we could identify SELECT here we would do:
    //   mstate.transaction_db_id = select_target_db_id;

    let argv_len_sum = mc.argv_len as usize;
    let argc = mc.argc as usize;
    mstate.commands.push(mc);
    mstate.cmd_flags |= cmd_flags;
    mstate.cmd_inv_flags |= !cmd_flags;
    // C: c->mstate->argv_len_sums += c->argv_len_sum + sizeof(robj*) * c->argc
    // PERF(port): sizeof(robj*) was pointer-size overhead in C heap; use
    // std::mem::size_of::<usize>() as an approximation.
    mstate.argv_len_sums += argv_len_sum + std::mem::size_of::<usize>() * argc;
}

// ── Transaction control ──────────────────────────────────────────────────────

/// Abort the current MULTI transaction and clear all flags.
///
/// C: `discardTransaction` in `multi.c:139-145`.
pub fn discard_transaction(client: &mut Client) {
    reset_client_multi_state(client);
    client.set_flag_multi(false);     // TODO(port): Client::set_flag_multi()
    client.set_flag_dirty_cas(false); // TODO(port): Client::set_flag_dirty_cas()
    client.set_flag_dirty_exec(false);// TODO(port): Client::set_flag_dirty_exec()
    unwatch_all_keys(client);         // TODO(port): needs &mut RedisDb
}

/// Mark the current MULTI transaction as doomed due to a command-queueing error.
///
/// C: `flagTransaction` in `multi.c:149-154`.
///
/// EXEC will return `-EXECABORT` after this is set.
pub fn flag_transaction(client: &mut Client) {
    if client.flag_multi() { // TODO(port): Client::flag_multi()
        client.set_flag_dirty_exec(true);
        reset_client_multi_state(client);
    }
}

// ── Command handlers ─────────────────────────────────────────────────────────

/// MULTI — begin a transaction block.
///
/// C: `multiCommand` in `multi.c:156-161`.
pub fn multi_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let client = ctx.client_mut(); // TODO(port): CommandContext::client_mut()
    init_client_multi_state(client);
    client.set_flag_multi(true);
    let db_id = client.db_id();
    if let Some(mstate) = client.mstate.as_mut() {
        mstate.transaction_db_id = db_id;
    }
    ctx.reply_simple_string(b"OK")
}

/// DISCARD — abort a transaction block.
///
/// C: `discardCommand` in `multi.c:163-170`.
pub fn discard_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let client = ctx.client_mut(); // TODO(port): CommandContext::client_mut()
    if !client.flag_multi() {
        return Err(RedisError::runtime(b"DISCARD without MULTI"));
    }
    discard_transaction(client);
    ctx.reply_simple_string(b"OK")
}

/// Abort a transaction, sending an EXECABORT error to the client.
///
/// C: `execCommandAbort` in `multi.c:177-187`.
///
/// `error` must be the raw error message bytes (may or may not start with `-`).
pub fn exec_command_abort(ctx: &mut CommandContext, mut error: &[u8]) -> Result<(), RedisError> {
    let client = ctx.client_mut(); // TODO(port): CommandContext::client_mut()
    discard_transaction(client);

    // Strip a leading `-` if present; the format string adds it back.
    // C: multi.c:180 — `if (error[0] == '-') error++;`
    if error.first() == Some(&b'-') {
        error = &error[1..];
    }

    // Build the EXECABORT message: "-EXECABORT Transaction discarded because of: <reason>"
    // C: addReplyErrorFormat(c, "-EXECABORT Transaction discarded because of: %s", error)
    let mut msg = Vec::with_capacity(64 + error.len());
    msg.extend_from_slice(b"-EXECABORT Transaction discarded because of: ");
    msg.extend_from_slice(error);

    // TODO(port): replicationFeedMonitors — propagate EXEC to MONITOR clients.
    // C: multi.c:186 — replicationFeedMonitors(c, server.monitors, c->db->id, c->argv, c->argc)
    // Blocked on Phase 3 replication layer.

    Err(RedisError::runtime(&msg))
}

/// EXEC — execute all queued commands in the transaction block.
///
/// C: `execCommand` in `multi.c:189-296`.
///
/// Returns `Ok(())` after writing all replies; errors inside individual queued
/// commands are written as error frames in the array, not propagated as `Err`.
pub fn exec_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // C: multi.c:195-198
    {
        let client = ctx.client_mut(); // TODO(port): CommandContext::client_mut()
        if !client.flag_multi() {
            return Err(RedisError::runtime(b"EXEC without MULTI"));
        }
    }

    // EXEC with an expired watched key is treated as a CAS failure.
    // C: multi.c:201-203
    {
        let client = ctx.client_mut();
        if is_watched_key_expired(client) {
            client.set_flag_dirty_cas(true);
        }
    }

    // Handle abort conditions.
    // C: multi.c:211-220
    {
        let client = ctx.client_mut();
        if client.flag_dirty_exec() {
            // A command-queueing error occurred: send EXECABORT.
            // C: addReplyErrorObject(c, shared.execaborterr)
            // TODO(port): ctx.reply_error_object(shared::execaborterr()) — needs
            // shared-object registry wired up.  For now emit the canonical message.
            drop(client); // release borrow before calling ctx method
            let _ = ctx.reply_error(b"EXECABORT Transaction discarded because of previous errors.");
            // TODO(architect): discard_transaction needs re-borrow of ctx.client_mut()
            // here; blocked until CommandContext exposes a discard_exec_transaction()
            // helper that avoids the double-borrow.  Tracked in needs_architect.txt.
            return Ok(());
        }
        if client.flag_dirty_cas() {
            // A watched key was touched: return null array (not an error).
            // C: addReply(c, shared.nullarray[c->resp])
            ctx.reply_null_array()?; // TODO(port): CommandContext::reply_null_array()
            discard_transaction(ctx.client_mut());
            return Ok(());
        }
    }

    // Save client flags; we are about to mutate them temporarily.
    // C: multi.c:222 — `struct ClientFlags old_flags = c->flag;`
    let old_deny_blocking = ctx.client_ref().flag_deny_blocking();
    // TODO(port): Client::flag_deny_blocking() / Client::client_ref()

    // Disallow blocking commands inside MULTI/EXEC.
    // C: multi.c:225
    ctx.client_mut().set_flag_deny_blocking(true);
    // TODO(port): Client::set_flag_deny_blocking()

    // Unwatch keys before executing (saves CPU on subsequent iterations).
    // C: multi.c:228
    unwatch_all_keys(ctx.client_mut()); // TODO(port): needs db access

    // Mark server as inside exec.
    // C: multi.c:230 — server.in_exec = 1
    ctx.server_mut().set_in_exec(true);
    // TODO(port): CommandContext::server_mut() -> &mut RedisServer; RedisServer::set_in_exec()

    let count = ctx.client_ref()
        .mstate
        .as_ref()
        .map_or(0, |m| m.commands.len());

    ctx.reply_array_header(count)?;
    // TODO(port): CommandContext::reply_array_header(n) — emits `*n\r\n`

    // Execute each queued command.
    // C: multi.c:238-284
    for j in 0..count {
        // Swap the client's current argv/cmd with the queued command's.
        // C: multi.c:239-242
        ctx.client_mut().set_current_queued_command(j);
        // TODO(port): Client::set_current_queued_command(index) — installs
        // commands[j].argv/argc/argv_len/cmd as the client's current command.

        // ACL check at execution time (ACL rules may have changed since queue).
        // C: multi.c:246-270
        // TODO(port): ACLCheckAllPerm / addACLLogEntry — blocked on Phase 2 ACL
        // implementation.  For now we skip the ACL gate and always call.
        //
        // When ACL is wired, the pattern is:
        //   let acl_result = ctx.acl_check_all_perm(&mut acl_errpos);
        //   if acl_result != AclResult::Ok { ... emit NOPERM error frame ... } else { ... }

        // Call the command.
        // C: multi.c:267-270
        //   if (c->id == CLIENT_ID_AOF)  call(c, CMD_CALL_NONE);
        //   else                          call(c, CMD_CALL_FULL);
        //
        // TODO(port): ctx.call(CMD_CALL_FULL) — dispatch table call; blocked
        // on Phase 3.  Also depends on client ID check for AOF replay.
        let _call_flags = if ctx.client_ref().is_aof_client() {
            CMD_CALL_NONE
        } else {
            CMD_CALL_FULL
        };
        // TODO(port): Client::is_aof_client()
        ctx.call_queued(_call_flags)?;
        // TODO(port): CommandContext::call_queued(flags) — executes the command
        // installed by set_current_queued_command() with the given call flags.

        // C: serverAssert(c->flag.blocked == 0)
        debug_assert!(
            !ctx.client_ref().flag_blocked(),
            "blocking command inside MULTI is forbidden"
        );
        // TODO(port): Client::flag_blocked()

        // Save back any mutations the called command made to argv/cmd.
        // C: multi.c:276-279
        ctx.client_mut().save_queued_command_state(j);
        // TODO(port): Client::save_queued_command_state(index) — writes
        // current argc/argv/argv_len/cmd back into commands[j].

        // Free the original argv (already recorded for commandlog/monitor).
        // C: multi.c:283 — freeClientOriginalArgv(c)
        ctx.client_mut().free_original_argv();
        // TODO(port): Client::free_original_argv()
    }

    // Restore deny_blocking to its pre-EXEC value.
    // C: multi.c:287
    if !old_deny_blocking {
        ctx.client_mut().set_flag_deny_blocking(false);
    }

    // Restore original argv/cmd.
    // C: multi.c:289-292
    ctx.client_mut().restore_orig_argv();
    // TODO(port): Client::restore_orig_argv() — restores the orig_argv saved
    // before the loop, matching C's: c->argv = orig_argv; c->argc = orig_argc; etc.

    discard_transaction(ctx.client_mut());

    // Clear in_exec flag.
    // C: multi.c:295
    ctx.server_mut().set_in_exec(false);

    Ok(())
}

// ── WATCH / UNWATCH ──────────────────────────────────────────────────────────

/// Begin watching `key` in the current db for CAS semantics.
///
/// C: `watchForKey` in `multi.c:356-397`.
pub fn watch_for_key(client: &mut Client, db: &mut RedisDb, key: &RedisObject) {
    // C: multi.c:360 — increment watching_clients when first key is added
    if client
        .mstate
        .as_ref()
        .map_or(true, |m| m.watched_keys.is_empty())
    {
        // TODO(port): server.watching_clients += 1; needs &mut RedisServer access.
    }

    init_client_multi_state(client);

    let db_id = db.id(); // TODO(port): RedisDb::id() -> i32
    let client_id = client.id();

    let mstate = client.mstate.as_mut().expect("just initialised");

    // Deduplicate: if already watching this key in this db, skip.
    // C: multi.c:373-374 — hashtableFind on the per-db hashtable
    let key_bytes = key.as_bytes(); // TODO(port): RedisObject::as_bytes() -> &[u8] or &RedisString
    let key_string = RedisString::from_bytes(key_bytes); // TODO(port): RedisString::from_bytes()
    if mstate
        .watched_keys_by_db
        .get(&db_id)
        .map_or(false, |s: &HashSet<RedisString>| s.contains(&key_string))
    {
        return;
    }

    // Register in the db-side dict so that key modifications reach us.
    // C: multi.c:378-382 — dictFetchValue / listCreate / dictAdd
    db.watched_keys_add_client(key, client_id);
    // TODO(port): RedisDb::watched_keys_add_client(key, client_id) — adds the
    // client id to the per-key watcher list in db.watched_keys.

    // Record the watch on the client side.
    let expired = db.key_is_expired(key); // TODO(port): RedisDb::key_is_expired()
    mstate.watched_keys.push(WatchedKey {
        key: key.clone(), // PERF(port): clone of RedisObject — may be expensive for large vals; profile Phase B
        db_id,
        expired,
    });

    // Update per-db membership set for O(1) deduplication.
    mstate
        .watched_keys_by_db
        .entry(db_id)
        .or_default()
        .insert(key_string);
}

/// Remove all watches for this client and clean up db-side references.
///
/// C: `unwatchAllKeys` in `multi.c:401-434`.
pub fn unwatch_all_keys(client: &mut Client) {
    let has_watches = client
        .mstate
        .as_ref()
        .map_or(false, |m| !m.watched_keys.is_empty());
    if !has_watches {
        return;
    }

    // We need mutable access to the db(s) referenced in watched_keys.  The
    // WatchedKey holds a db_id which we use to look up the db from the server.
    // TODO(port): To remove the client from the db-side watcher list we need
    // &mut RedisDb.  In Phase A we note what must happen; the actual removal
    // requires passing &mut RedisServer or a similar accessor.
    //
    // For each watched_key wk:
    //   1. Remove client.id() from db.watched_keys[wk.key].
    //   2. If the per-key client list is now empty, remove the key entry.
    //   3. Drop wk.
    //
    // C: multi.c:407-431
    //
    // TODO(port): iterate mstate.watched_keys, call
    //   db_for(wk.db_id).watched_keys_remove_client(&wk.key, client.id())
    // for each entry.  Blocked on &mut RedisServer access from here.

    if let Some(mstate) = client.mstate.as_mut() {
        mstate.watched_keys.clear();
        // Clear all per-db membership sets.
        for set in mstate.watched_keys_by_db.values_mut() {
            let set: &mut HashSet<RedisString> = set;
            set.clear();
        }
    }

    // Decrement global watching_clients counter.
    // C: multi.c:433 — server.watching_clients--
    // TODO(port): server.watching_clients -= 1; needs &mut RedisServer.
}

/// Return `true` if any non-initially-expired watched key has now expired.
///
/// C: `isWatchedKeyExpired` in `multi.c:438-451`.
pub fn is_watched_key_expired(client: &Client) -> bool {
    let mstate = match client.mstate.as_ref() {
        Some(m) if !m.watched_keys.is_empty() => m,
        _ => return false,
    };

    for wk in &mstate.watched_keys {
        if wk.expired {
            // Key was already expired when WATCH was called — ignore.
            continue;
        }
        // TODO(port): Need &RedisDb to call RedisDb::key_is_expired(&wk.key).
        // Placeholder: return false conservatively until db access is wired.
        // When wired, replace with: if db_for(wk.db_id).key_is_expired(&wk.key) { return true; }
        let _ = wk; // suppress unused warning
    }

    false
}

/// Mark all clients watching `key` in `db` as dirty (CAS failure).
///
/// C: `touchWatchedKey` in `multi.c:455-492`.
///
/// Called by write operations in `db.c` and `t_*.c` after they modify a key.
pub fn touch_watched_key(db: &mut RedisDb, key: &RedisObject) {
    // C: multi.c:460 — early return if no one is watching anything
    if db.watched_keys_is_empty() {
        // TODO(port): RedisDb::watched_keys_is_empty() -> bool
        return;
    }

    // C: multi.c:461 — clients = dictFetchValue(db->watched_keys, key)
    // TODO(port): RedisDb::watched_keys_get_clients(key) -> Option<Vec<ClientId>>
    // For Phase A, the actual per-client mutation is described in comments.

    // For each client watching this key:
    // C: multi.c:467-491
    //
    //   for wk in clients_watching(key):
    //       if wk.expired:
    //           if same db AND same key AND key no longer exists:
    //               // Expired key deleted — logically unchanged. Clear flag.
    //               wk.expired = false
    //               continue  // goto skip_client
    //           break         // stop processing (remaining clients not dirtied)
    //       client.flag_dirty_cas = true
    //       reset_client_multi_state(client)
    //       unwatch_all_keys(client)  // client removes itself from all watches
    //       // continue to next watching client
    //
    // TODO(port): Full implementation requires:
    //   - RedisDb::watched_keys_get_clients_mut(key) -> &mut Vec<WatchedKeyRef>
    //   - Access to each Client by ID to call set_flag_dirty_cas / reset / unwatch
    //   Blocked on Phase 3 server-state access pattern.
    let _ = (db, key); // suppress unused-variable warnings
}

/// Mark all watching clients dirty when an entire db is flushed or swapped.
///
/// C: `touchAllWatchedKeysInDb` in `multi.c:501-543`.
///
/// `replaced_with` is `Some` for SWAPDB (the db that is swapping in); `None`
/// for FLUSHDB/FLUSHALL/diskless-replication.
pub fn touch_all_watched_keys_in_db(emptied: &mut RedisDb, replaced_with: Option<&mut RedisDb>) {
    // C: multi.c:506 — early return if nothing watched
    if emptied.watched_keys_is_empty() {
        // TODO(port): RedisDb::watched_keys_is_empty()
        return;
    }

    // Iterate over every key that has watchers in `emptied`.
    // C: multi.c:508 — dictGetSafeIterator / dictNext loop
    //
    // For each (key, clients) in emptied.watched_keys:
    //   exists_in_emptied = dbFind(emptied, key) != NULL
    //   if exists_in_emptied OR (replaced_with AND dbFind(replaced_with, key) != NULL):
    //     for each wk in clients:
    //       if wk.expired:
    //         if !replaced_with OR !dbFind(replaced_with, key):
    //           wk.expired = false; continue  // expired key deleted
    //         elif keyIsExpired(replaced_with, key):
    //           continue                       // expired key remains expired
    //       elif !exists_in_emptied AND keyIsExpired(replaced_with, key):
    //         wk.expired = true; continue      // non-existing replaced by expired
    //       // Normal dirty case:
    //       client.flag_dirty_cas = true
    //       reset_client_multi_state(client)
    //       // Note: do NOT call unwatchAllKeys here — it would invalidate the
    //       // iterator via use-after-free.  C comment at multi.c:537-539.
    //
    // TODO(port): Full implementation requires:
    //   - RedisDb::watched_keys_iter() — iterating (key, client-ids) pairs
    //   - &mut Client access by id within the loop
    //   - Careful iterator-invalidation avoidance (matches C comment)
    //   Blocked on Phase 3 server-state access pattern.
    let _ = (emptied, replaced_with); // suppress unused-variable warnings
}

/// WATCH — register keys for CAS monitoring.
///
/// C: `watchCommand` in `multi.c:545-558`.
pub fn watch_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // If already dirty, WATCH is a no-op (CAS already failed).
    // C: multi.c:549-552
    if ctx.client_ref().flag_dirty_cas() {
        return ctx.reply_simple_string(b"OK");
    }

    let client = ctx.client_mut();
    init_client_multi_state(client);

    // argv[1..] are the keys to watch.
    // C: multi.c:556 — for (j = 1; j < c->argc; j++) watchForKey(c, c->argv[j])
    // TODO(port): Need &mut RedisDb alongside &mut Client.  watchForKey requires
    // both.  CommandContext must provide a way to access both simultaneously.
    // For now, call a ctx-level method that handles the borrow internally.
    let argc = ctx.argc(); // TODO(port): CommandContext::argc() -> usize
    for j in 1..argc {
        let key = ctx.arg_object(j)?; // TODO(port): CommandContext::arg_object(i) -> Result<RedisObject, RedisError>
        ctx.watch_for_key(&key)?;
        // TODO(port): CommandContext::watch_for_key(key) — calls watch_for_key
        // with the appropriate client + db references extracted from ctx.
    }

    ctx.reply_simple_string(b"OK")
}

/// UNWATCH — remove all CAS monitors for this client.
///
/// C: `unwatchCommand` in `multi.c:560-564`.
pub fn unwatch_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    unwatch_all_keys(ctx.client_mut()); // TODO(port): needs db access via ctx
    ctx.client_mut().set_flag_dirty_cas(false);
    ctx.reply_simple_string(b"OK")
}

// ── Memory accounting ────────────────────────────────────────────────────────

/// Return the approximate memory overhead of `client.mstate` in bytes.
///
/// C: `multiStateMemOverhead` in `multi.c:566-584`.
pub fn multi_state_mem_overhead(client: &Client) -> usize {
    let mstate = match client.mstate.as_ref() {
        Some(m) => m,
        None => return 0,
    };

    let mut mem = mstate.argv_len_sums;

    // Watched-keys list overhead.
    // C: listLength(&c->mstate->watched_keys) * (sizeof(listNode) + sizeof(watchedKey))
    // In Rust: Vec<WatchedKey> — approximate with size_of::<WatchedKey>().
    mem += mstate.watched_keys.len()
        * (std::mem::size_of::<WatchedKey>() + std::mem::size_of::<usize>());

    // Per-db hashtable overhead.
    // C: sizeof(hashtable *) * server.dbnum + hashtableMemUsage per non-null table
    // TODO(port): server.dbnum is a runtime value; we approximate with the
    // number of db entries actually present in the HashMap.
    mem += mstate.watched_keys_by_db.len()
        * (std::mem::size_of::<i32>() + std::mem::size_of::<HashSet<RedisString>>());
    for set in mstate.watched_keys_by_db.values() {
        // PERF(port): std::mem::size_of is only the stack frame; heap allocation
        // of the HashSet is not captured here.  Phase B can use a proper allocator
        // instrumented allocator or jemalloc stats.
        let set: &HashSet<RedisString> = set;
        mem += set.len() * std::mem::size_of::<RedisString>();
    }

    // Reserved command-slot overhead.
    // C: c->mstate->alloc_count * sizeof(multiCmd)
    mem += mstate.alloc_count as usize * std::mem::size_of::<MultiCmd>();

    mem
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/multi.c  (585 lines, 18 functions)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         33
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:         Logic faithful to C; all cross-crate wiring (Client accessors,
//                  RedisDb::watched_keys, RedisServer::watching_clients,
//                  CommandContext::call_queued) needs Phase B resolution.
//                  MultiState/MultiCmd must migrate to redis-core before Phase B.
//                  discard_transaction call in exec_command stubbed out — the
//                  double-borrow (client borrow + ctx reply borrow) cannot be
//                  resolved without a CommandContext::discard_exec_transaction()
//                  helper.  Tracked in needs_architect.txt.
// ──────────────────────────────────────────────────────────────────────────
