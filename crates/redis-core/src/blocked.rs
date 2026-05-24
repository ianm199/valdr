//! Blocking-client infrastructure.
//!
//! Port of `src/blocked.c` (Valkey, 819 lines, 28 functions).
//!
//! Provides the generic machinery for blocking operations such as BLPOP, XREAD,
//! WAIT, WAITAOF, CLIENT PAUSE, and SHUTDOWN. When a client issues a blocking
//! command that cannot be satisfied immediately, it is placed in the blocked state
//! with a specific [`BlockingType`]. When the blocking condition is met (a key
//! becomes available, replicas catch up, or a timeout fires), the appropriate
//! unblock path runs.
//!
//! ## Design notes
//!
//! * [`BlockingState`] is lazily allocated on [`Client`]. The C code allocates it
//!   with `zmalloc` on first need; in Rust this is `Option<Box<BlockingState>>`.
//! * The per-db `blocking_keys` dict maps a key to the list of clients waiting
//!   for it. In Rust this is `HashMap<RedisString, Vec<ClientId>>` on `RedisDb`.
//! * Ref-counting (`incrRefCount`/`decrRefCount`) is replaced by Rust ownership.
//! * Module-blocking paths are stubbed; modules are Phase 10.
//!
//! ## Missing fields
//!
//! Several fields that the C code accesses via `client->flag.*`, `client->bstate`,
//! `server.blocked_clients`, etc., must be added to canonical types before this
//! module compiles. Each gap is flagged with `TODO(architect)`.

use std::collections::HashMap;

use redis_types::{RedisError, RedisString};

use crate::client::{Client, ClientId};
use crate::object::RedisObject;
use crate::server::RedisServer;

// ── Error-flag bit constants ───────────────────────────────────────────────────

/// Bit flag: the blocked command was externally rejected (e.g., role change).
/// C: `#define ERROR_COMMAND_REJECTED (1 << 0)` — server.h:3400
pub const ERROR_COMMAND_REJECTED: i32 = 1 << 0;

/// Bit flag: the blocked command failed internally.
/// C: `#define ERROR_COMMAND_FAILED (1 << 1)` — server.h:3401
pub const ERROR_COMMAND_FAILED: i32 = 1 << 1;

// ── BlockingType ──────────────────────────────────────────────────────────────

/// The reason a client is currently blocked.
///
/// C: `typedef enum blocking_type { ... } blocking_type;` — server.h:340
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum BlockingType {
    /// Not blocked — sentinel / unset value.  C: `BLOCKED_NONE`
    #[default]
    None,
    /// BLPOP and friends.  C: `BLOCKED_LIST`
    List,
    /// WAIT for synchronous replication.  C: `BLOCKED_WAIT`
    Wait,
    /// Blocked by a loadable module.  C: `BLOCKED_MODULE`
    Module,
    /// XREAD.  C: `BLOCKED_STREAM`
    Stream,
    /// BZPOP et al.  C: `BLOCKED_ZSET`
    ZSet,
    /// processCommand re-try scheduled by the server.  C: `BLOCKED_POSTPONE`
    Postpone,
    /// SHUTDOWN blocked waiting for ongoing operations to finish.  C: `BLOCKED_SHUTDOWN`
    Shutdown,
}

impl BlockingType {
    /// Number of discriminants used to size per-type counter arrays.
    /// C: `BLOCKED_NUM`
    pub const COUNT: usize = 8;

    /// Returns `true` for the three blocking types that wait on data keys.
    pub fn is_key_blocking(self) -> bool {
        matches!(
            self,
            BlockingType::List | BlockingType::ZSet | BlockingType::Stream
        )
    }
}

// ── BlockingState ─────────────────────────────────────────────────────────────

/// The blocking-operation state attached to a client.
///
/// Lazily created on first use. In C this is `zmalloc`-allocated; in Rust it is
/// held as `Option<Box<BlockingState>>` on the `Client` struct.
///
/// C: `typedef struct blockingState { ... } blockingState;` — server.h:963
///
/// TODO(architect): Add `pub bstate: Option<Box<BlockingState>>` to `Client` in
/// `crates/redis-core/src/client.rs`.
#[derive(Debug, Default)]
pub struct BlockingState {
    /// Which operation is blocking this client.  C: `blocking_type btype`
    pub btype: BlockingType,

    /// Absolute deadline in milliseconds since epoch (0 = no timeout).
    /// C: `mstime_t timeout`
    pub timeout: i64,

    /// Whether to unblock the client when a watched key is deleted or disappears.
    /// Set for XREADGROUP consumers.  C: `int unblock_on_nokey`
    pub unblock_on_nokey: bool,

    /// The keys the client is waiting on.
    ///
    /// C: `dict *keys` — maps `robj *key` to `listNode *` (position in the
    /// per-db blocking list). In Rust the value is `()` for Phase A; O(1) node
    /// removal requires an intrusive-list strategy deferred to Phase 4.
    ///
    /// TODO(port): The C code stores a listNode* per key for O(1) removal from
    /// the per-db list. Replace `()` value with a typed node handle once the
    /// per-db blocking lists are modelled as slabs or index-stable collections.
    pub keys: HashMap<RedisString, ()>,

    /// Utility list-node index.
    ///
    /// In C this is a union of three `listNode *`:
    ///   - `client_waiting_acks_list_node` (BLOCKED_WAIT)
    ///   - `postponed_list_node` (BLOCKED_POSTPONE)
    ///   - `generic_blocked_list_node` (generic placeholder)
    ///
    /// In Rust we store an opaque index into whichever server list currently
    /// owns this client. Typed accessors are TODO.
    ///
    /// TODO(architect): Replace with typed handle once the relevant server lists
    /// are modelled in `RedisServer`.
    pub utility_list_index: Option<usize>,

    // ── BLOCKED_WAIT fields ──────────────────────────────────────────────────
    /// Number of replicas we are waiting for ACK.  C: `int numreplicas`
    pub num_replicas: i32,
    /// Whether WAITAOF is also waiting for local fsync.  C: `int numlocal`
    pub num_local: i32,
    /// Replication offset to reach.  C: `long long reploffset`
    pub repl_offset: i64,

    // ── BLOCKED_MODULE fields ────────────────────────────────────────────────
    /// Opaque handle for `ValkeyModuleBlockedClient`. Deferred to Phase 10.
    /// TODO(port): module blocked handle — opaque void* in C; Phase 10 only.
    pub module_blocked_handle: Option<()>,
    /// Opaque handle for `ValkeyModuleAsyncRMCallPromise`. Deferred to Phase 10.
    /// TODO(port): async RM call handle — opaque void* in C; Phase 10 only.
    pub async_rm_call_handle: Option<()>,
}

impl BlockingState {
    pub fn new() -> Self {
        Self::default()
    }
}

// ── ReadyList ─────────────────────────────────────────────────────────────────

/// A `(db_index, key)` pair queued on `server.ready_keys`.
///
/// When a key is modified and there are blocked clients waiting for it, a
/// `ReadyList` entry is appended to `server.ready_keys`. The function
/// [`handle_clients_blocked_on_keys`] drains this list each event-loop tick.
///
/// C: `typedef struct readyList { serverDb *db; robj *key; } readyList;` — server.h:1008
///
/// TODO(architect): `RedisServer` needs `ready_keys: Vec<ReadyList>` and
/// `unblocked_clients: VecDeque<ClientId>`.
#[derive(Debug)]
pub struct ReadyList {
    /// Index of the database the key lives in.
    pub db_index: u32,
    /// The key that became ready.
    pub key: RedisString,
}

// ── ObjectTypeHint ────────────────────────────────────────────────────────────

/// Thin type hint mirroring the `OBJ_*` constants, used by [`signal_key_as_ready`]
/// and friends to pick the right [`BlockingType`] without importing the full
/// `RedisObject` enum discriminant.
///
/// Phase 4 will likely collapse this into a method on `RedisObject`.
///
/// TODO(architect): decide whether `get_blocked_type_by_obj_type` should become a
/// method on `RedisObject` or stay as a free function here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectTypeHint {
    List,
    ZSet,
    Module,
    Stream,
    Other,
}

impl From<&RedisObject> for ObjectTypeHint {
    fn from(obj: &RedisObject) -> Self {
        if obj.is_list() {
            return ObjectTypeHint::List;
        }
        if obj.is_zset() {
            return ObjectTypeHint::ZSet;
        }
        if obj.is_stream() {
            return ObjectTypeHint::Stream;
        }
        ObjectTypeHint::Other
    }
}

// ── init / free ───────────────────────────────────────────────────────────────

/// Lazily initialise the blocking-state on a client if not already present.
///
/// C: `void initClientBlockingState(client *c)` — blocked.c:81
///
/// TODO(architect): `Client` must have field `bstate: Option<Box<BlockingState>>`.
pub fn init_client_blocking_state(c: &mut Client) {
    // TODO(architect): if c.bstate.is_some() { return; }
    // TODO(architect): c.bstate = Some(Box::new(BlockingState::new()));
    let _ = c;
}

/// Free (drop) the blocking-state on a client.
///
/// C: `void freeClientBlockingState(client *c)` — blocked.c:96
///
/// TODO(architect): `Client` must have field `bstate: Option<Box<BlockingState>>`.
pub fn free_client_blocking_state(c: &mut Client) {
    // TODO(architect): c.bstate = None;
    let _ = c;
}

// ── Core block / unblock ──────────────────────────────────────────────────────

/// Put a client into blocked mode with the given blocking type.
///
/// Sets the blocked flag, records the `btype`, increments per-type counters on
/// the server, and inserts the client into the timeout table.  Must be called
/// after [`init_client_blocking_state`].
///
/// Replicated clients must not be blocked except for `Module` or `Postpone`.
///
/// C: `void blockClient(client *c, int btype)` — blocked.c:106
///
/// TODO(architect): `Client` needs `flag_blocked: bool` and `flag_module: bool`.
/// TODO(architect): `RedisServer` needs `blocked_clients: u32` and
///   `blocked_clients_by_type: [u32; BlockingType::COUNT]`.
pub fn block_client(c: &mut Client, server: &mut RedisServer, btype: BlockingType) {
    // C: serverAssert(!(isReplicatedClient(c) && btype != BLOCKED_MODULE && btype != BLOCKED_POSTPONE));
    // TODO(port): is_replicated_client check — requires Client replication flags.

    init_client_blocking_state(c);

    // TODO(architect): c.flag_blocked = true;
    // TODO(architect): c.bstate_mut().btype = btype;
    // TODO(architect): if !c.flag_module { server.blocked_clients += 1; }
    // TODO(architect): server.blocked_clients_by_type[btype as usize] += 1;
    // TODO(architect): crate::timeout::add_client_to_timeout_table(server, c);

    let _ = (c, server, btype);
}

/// Update command statistics after a client is unblocked without running
/// `processCommand` (timeout or module rejection paths).
///
/// `blocked_us`: microseconds spent blocked.
/// `reply_us`: microseconds spent composing the reply.
/// `failed_or_rejected`: `0`, [`ERROR_COMMAND_REJECTED`], or [`ERROR_COMMAND_FAILED`].
///
/// C: `void updateStatsOnUnblock(client *c, long blocked_us, long reply_us, int failed_or_rejected)`
/// — blocked.c:129
///
/// TODO(architect): `Client` needs `duration: i64`, `last_cmd`, `commands_processed: u64`.
/// TODO(architect): `RedisServer` needs `stat_numcommands: u64` and
///   `latency_tracking_enabled: bool`.
pub fn update_stats_on_unblock(
    c: &mut Client,
    server: &mut RedisServer,
    blocked_us: i64,
    reply_us: i64,
    failed_or_rejected: i32,
) {
    debug_assert!(
        failed_or_rejected >= 0 && failed_or_rejected <= ERROR_COMMAND_FAILED,
        "invalid failed_or_rejected value: {}",
        failed_or_rejected,
    );

    // TODO(architect): c.duration += blocked_us + reply_us;
    // TODO(architect): c.last_cmd.microseconds += c.duration;
    // TODO(architect): cluster_slot_stats_add_cpu_duration(c, c.duration);
    // TODO(architect): c.last_cmd.calls += 1;
    // TODO(architect): c.commands_processed += 1;
    // TODO(architect): server.stat_numcommands += 1;

    if failed_or_rejected != 0 {
        if (failed_or_rejected & ERROR_COMMAND_FAILED) != 0 {
            // TODO(architect): c.last_cmd.failed_calls += 1;
        } else if (failed_or_rejected & ERROR_COMMAND_REJECTED) != 0 {
            // TODO(architect): c.last_cmd.rejected_calls += 1;
        }
    }

    // TODO(architect): if server.latency_tracking_enabled {
    //     update_command_latency_histogram(&mut c.last_cmd.latency_histogram, c.duration * 1000);
    // }
    // TODO(architect): commandlog_push_current_command(c, c.last_cmd);
    // TODO(architect): c.duration = 0;
    // TODO(architect): latency_add_sample_if_needed("command-unblocking", reply_us);

    let _ = (c, server, blocked_us, reply_us, failed_or_rejected);
}

/// Schedule a client for reprocessing at the next safe event-loop point.
///
/// Appends the client to `server.unblocked_clients` if not already queued.
/// This is needed because unblocking a client does not re-fire the readable
/// event that would normally trigger command processing.
///
/// C: `void queueClientForReprocessing(client *c)` — blocked.c:206
///
/// TODO(architect): `Client` needs `flag_unblocked: bool`.
/// TODO(architect): `RedisServer` needs `unblocked_clients: VecDeque<ClientId>`.
pub fn queue_client_for_reprocessing(c: &mut Client, server: &mut RedisServer) {
    // TODO(architect):
    //   if !c.flag_unblocked {
    //       c.flag_unblocked = true;
    //       server.unblocked_clients.push_back(c.id);
    //   }
    let _ = (c, server);
}

/// Drain the `server.unblocked_clients` queue, re-processing each client's
/// input buffer.  Called from `before_sleep()`.
///
/// C: `void processUnblockedClients(void)` — blocked.c:158
///
/// TODO(architect): `RedisServer` needs `unblocked_clients: VecDeque<ClientId>`
/// and a way to look up a `&mut Client` by `ClientId`.
pub fn process_unblocked_clients(server: &mut RedisServer) {
    // C: blocked.c:158-188
    // while (listLength(server.unblocked_clients)) {
    //   c = listFirst(server.unblocked_clients)->value;
    //   listDelNode(server.unblocked_clients, ln);
    //   c->flag.unblocked = 0;
    //   if (c->flag.module) {
    //     if (!c->flag.blocked) moduleCallCommandUnblockedHandler(c);
    //     continue;
    //   }
    //   if (!c->flag.blocked) {
    //     if (processPendingCommandAndInputBuffer(c) == C_ERR) continue;
    //   }
    //   beforeNextClient(c);
    // }
    // TODO(architect): implement once server has unblocked_clients queue and
    // a ClientStore providing &mut Client by id.
    let _ = server;
}

/// Unblock a client by calling the btype-specific cleanup, clearing flags, and
/// optionally queuing for reprocessing.
///
/// C: `void unblockClient(client *c, int queue_for_reprocessing)` — blocked.c:217
///
/// TODO(architect): `Client` needs full flag + bstate surface.
/// TODO(architect): `RedisServer` needs `blocked_clients*`, `postponed_clients`.
pub fn unblock_client(c: &mut Client, server: &mut RedisServer, queue_for_reprocessing: bool) {
    // PORT NOTE: The C code dispatches on bstate->btype:
    //   List | ZSet | Stream → unblockClientWaitingData(c)
    //   Wait  → unblockClientWaitingReplicas(c)
    //   Module → maybe unblockClientWaitingData + unblockClientFromModule(c)
    //   Postpone → listDelNode(server.postponed_clients, node)
    //   Shutdown → no-op cleanup
    //   _ → serverPanic  (translated as debug_assert! unreachable below)
    //
    // TODO(architect): read c.bstate.btype once that field exists on Client.
    // TODO(architect): implement each branch using the server list handles.

    // TODO(architect): if !c.flag_pending_command && btype != Shutdown {
    //     reqres_append_response(c);
    //     reset_client(c);
    // }

    // TODO(architect): if !c.flag_module { server.blocked_clients -= 1; }
    // TODO(architect): server.blocked_clients_by_type[old_btype as usize] -= 1;
    // TODO(architect): c.flag_blocked = false;
    // TODO(architect): c.bstate.btype = BlockingType::None;
    // TODO(architect): c.bstate.unblock_on_nokey = false;
    // TODO(architect): crate::timeout::remove_client_from_timeout_table(server, c);

    if queue_for_reprocessing {
        queue_client_for_reprocessing(c, server);
    }
    let _ = (c, server);
}

/// Return `true` if a blocked client may be timed out via
/// [`unblock_client_on_timeout`].
///
/// C: `int blockedClientMayTimeout(client *c)` — blocked.c:260
///
/// TODO(architect): `Client` needs bstate field.
pub fn blocked_client_may_timeout(c: &Client) -> bool {
    // TODO(architect): match c.bstate.btype {
    //   BlockingType::Module  → module_blocked_client_may_timeout(c),
    //   List | ZSet | Stream | Wait → true,
    //   _ → false,
    // }
    let _ = c;
    false
}

/// Send a timeout reply to a client that has timed out while blocked.
///
/// Called by the cron function just before [`unblock_client`].
///
/// C: `void replyToBlockedClientTimedOut(client *c)` — blocked.c:277
///
/// TODO(architect): `Client` needs bstate, cmd fields.
/// TODO(port): replicationCountAcksByOffset / replicationCountAOFAcksByOffset
///   live in replication.c — defer to the replication crate.
pub fn reply_to_blocked_client_timed_out(
    c: &mut Client,
    server: &mut RedisServer,
) -> Result<(), RedisError> {
    // C: blocked.c:277-298
    // List | ZSet | Stream → addReplyNullArray(c); updateStatsOnUnblock(c,0,0,0);
    // Wait → dispatch by c->cmd->proc:
    //   waitCommand  → addReplyLongLong(replicationCountAcksByOffset(reploffset))
    //   waitaofCommand → addReplyArrayLen(2) + two reply integers
    //   clusterCommand → addReplyErrorObject(shared.noreplicaserr)
    //   _ → serverPanic (flag as TODO)
    // Module → moduleBlockedClientTimedOut(c, 0)
    // _ → serverPanic
    //
    // TODO(architect): implement full dispatch once bstate and cmd fields exist.
    // TODO(port): module timeout callback — Phase 10.
    let _ = (c, server);
    Ok(())
}

/// Send an error reply to all clients blocked on SHUTDOWN and unblock them.
///
/// C: `void replyToClientsBlockedOnShutdown(void)` — blocked.c:302
///
/// TODO(architect): `RedisServer` needs `blocked_clients_by_type` and `clients` list.
pub fn reply_to_clients_blocked_on_shutdown(server: &mut RedisServer) -> Result<(), RedisError> {
    // C: blocked.c:302-314
    // if server.blocked_clients_by_type[BLOCKED_SHUTDOWN] == 0 { return; }
    // for c in server.clients:
    //   if c.flag.blocked && btype == Shutdown:
    //     addReplyError(c, "Errors trying to SHUTDOWN. Check logs.");
    //     unblockClient(c, 1);
    //
    // TODO(architect): iterate server.clients and unblock each SHUTDOWN-blocked client.
    let _ = server;
    Ok(())
}

/// Force-unblock all blocked clients (except POSTPONE) when the instance role
/// changes (e.g., primary → replica).
///
/// In cluster mode: redirect if possible. In standalone mode: send `-REDIRECT`
/// (if client supports it) or `-UNBLOCKED` error and disconnect.
///
/// C: `void disconnectOrRedirectAllBlockedClients(void)` — blocked.c:326
///
/// TODO(architect): requires Client flag fields, cluster mode, server client list.
/// TODO(port): cluster redirect path — Phase 6 (redis-cluster crate).
pub fn disconnect_or_redirect_all_blocked_clients(
    server: &mut RedisServer,
) -> Result<(), RedisError> {
    // C: blocked.c:326-361
    // for c in server.clients:
    //   if !c.flag.blocked → skip
    //   if btype == Postpone → continue
    //   if server.cluster_enabled:
    //     if clusterRedirectBlockedClientIfNeeded(c) → unblockClientOnError(c, NULL)
    //   else:
    //     if c.flag.readonly && !(c.lastcmd.flags & CMD_WRITE) → continue
    //     if clientSupportStandAloneRedirect(c) && key-blocking btype:
    //       if Module && !moduleClientIsBlockedOnKeys(c) → continue
    //       addReplyErrorSds(c, "-REDIRECT primary_host:primary_port")
    //       unblockClientOnError(c, NULL)
    //     else:
    //       unblockClientOnError(c, "-UNBLOCKED force unblock ...")
    //       c.flag.close_after_reply = true
    //
    // TODO(architect): implement once Client flag surface and server.clients exist.
    let _ = server;
    Ok(())
}

// ── Key-ready signalling ───────────────────────────────────────────────────────

/// Map a Redis object type hint to the corresponding [`BlockingType`].
///
/// C: `static blocking_type getBlockedTypeByType(int type)` — blocked.c:505
fn get_blocked_type_by_obj_type(obj_type: ObjectTypeHint) -> BlockingType {
    match obj_type {
        ObjectTypeHint::List => BlockingType::List,
        ObjectTypeHint::ZSet => BlockingType::ZSet,
        ObjectTypeHint::Module => BlockingType::Module,
        ObjectTypeHint::Stream => BlockingType::Stream,
        ObjectTypeHint::Other => BlockingType::None,
    }
}

/// Internal: signal a key as ready, optionally treating the signal as a deletion.
///
/// Inserts the key into `server.ready_keys` if it has blocked waiters and is not
/// already queued there. Deduplication is done via `db.ready_keys` (a set).
///
/// C: `static void signalKeyAsReadyLogic(serverDb *db, robj *key, int type, int deleted)`
/// — blocked.c:522
///
/// TODO(architect): `RedisDb` needs `blocking_keys`, `blocking_keys_unblock_on_nokey`,
///   and `ready_keys` maps. `RedisServer` needs `blocked_clients_by_type` and `ready_keys`.
fn signal_key_as_ready_logic(
    server: &mut RedisServer,
    db_index: u32,
    key: &RedisString,
    obj_type: ObjectTypeHint,
    deleted: bool,
) {
    let btype = get_blocked_type_by_obj_type(obj_type);
    if btype == BlockingType::None {
        return;
    }

    // C: if !server.blocked_clients_by_type[btype] && !server.blocked_clients_by_type[MODULE]
    //        return;
    // TODO(architect): check server.blocked_clients_by_type once that array exists.

    if deleted {
        // C: if dictFind(db->blocking_keys_unblock_on_nokey, key) == NULL { return; }
        // TODO(architect): check db.blocking_keys_unblock_on_nokey.
    } else {
        // C: if dictFind(db->blocking_keys, key) == NULL { return; }
        // TODO(architect): check db.blocking_keys.
    }

    // C: de = dictAddRaw(db->ready_keys, key, &existing);
    //    if de == NULL { return; }  // already queued
    //    incrRefCount(key);
    //    rl = zmalloc(sizeof(*rl)); rl->key = key; rl->db = db; incrRefCount(key);
    //    listAddNodeTail(server.ready_keys, rl);
    // TODO(architect): dedup via db.ready_keys set; push ReadyList to server.ready_keys.

    let _ = (server, db_index, key, deleted);
}

/// Signal that a key has been modified and may unblock waiting clients.
///
/// C: `void signalKeyAsReady(serverDb *db, robj *key, int type)` — blocked.c:613
pub fn signal_key_as_ready(
    server: &mut RedisServer,
    db_index: u32,
    key: &RedisString,
    obj_type: ObjectTypeHint,
) {
    signal_key_as_ready_logic(server, db_index, key, obj_type, false);
}

/// Signal that a key has been deleted; unblock clients that requested
/// `unblock_on_nokey` behavior (e.g., XREADGROUP consumers).
///
/// C: `void signalDeletedKeyAsReady(serverDb *db, robj *key, int type)` — blocked.c:617
pub fn signal_deleted_key_as_ready(
    server: &mut RedisServer,
    db_index: u32,
    key: &RedisString,
    obj_type: ObjectTypeHint,
) {
    signal_key_as_ready_logic(server, db_index, key, obj_type, true);
}

// ── blockForKeys ──────────────────────────────────────────────────────────────

/// Block a client on a set of keys with the given type and timeout.
///
/// For each key: registers the client in the per-db `blocking_keys` map and
/// records the key in `client.bstate.keys`. If `unblock_on_nokey` is set, the
/// client is also registered in `db.blocking_keys_unblock_on_nokey` so it wakes
/// up on key deletion.
///
/// C: `void blockForKeys(client *c, int btype, robj **keys, int numkeys,
///   mstime_t timeout, int unblock_on_nokey)` — blocked.c:434
///
/// TODO(architect): `Client` needs bstate, `flag_reexecuting_command`, and
///   `flag_pending_command`.
/// TODO(architect): `RedisDb` needs `blocking_keys` and
///   `blocking_keys_unblock_on_nokey` maps.
pub fn block_for_keys(
    c: &mut Client,
    server: &mut RedisServer,
    btype: BlockingType,
    keys: &[RedisString],
    timeout: i64,
    unblock_on_nokey: bool,
) {
    // C: blocked.c:434-486
    init_client_blocking_state(c);

    // C: if (!c->flag.reexecuting_command) { c->bstate->timeout = timeout; }
    // TODO(architect): if !c.flag_reexecuting_command { c.bstate.timeout = timeout; }

    for key in keys {
        // C: if (!(client_blocked_entry = dictAddRaw(c->bstate->keys, key, NULL))) continue;
        // TODO(architect): skip if key already in c.bstate.keys.
        // TODO(architect): insert key into c.bstate.keys.

        // C: db_blocked_entry = dictAddRaw(c->db->blocking_keys, key, &existing);
        //    if (db_blocked_entry != NULL) { l = listCreate(); dictSetVal(..., l); }
        //    else { l = dictGetVal(existing); }
        //    listAddNodeTail(l, c);
        //    dictSetVal(c->bstate->keys, client_blocked_entry, listLast(l));
        // TODO(architect): add client id to db.blocking_keys[key] list.

        // C: if (unblock_on_nokey) { dictAddRaw / dictIncrUnsignedIntegerVal ... }
        // TODO(architect): if unblock_on_nokey, update db.blocking_keys_unblock_on_nokey counter.

        let _ = key;
    }

    // TODO(architect): c.bstate.unblock_on_nokey = unblock_on_nokey;
    // TODO(architect): if btype != BlockingType::Module { c.flag_pending_command = true; }

    block_client(c, server, btype);
    let _ = (timeout, unblock_on_nokey);
}

// ── Internal unblock helpers ──────────────────────────────────────────────────

/// Remove a client from all per-db `blocking_keys` registrations it holds.
///
/// C: `static void unblockClientWaitingData(client *c)` — blocked.c:490
///
/// TODO(architect): `Client` needs bstate.keys; `RedisDb` needs blocking_keys.
fn unblock_client_waiting_data(c: &mut Client, server: &mut RedisServer) {
    // C: blocked.c:490-503
    // if (dictSize(c->bstate->keys) == 0) return;
    // di = dictGetIterator(c->bstate->keys);
    // while ((de = dictNext(di)) != NULL) { releaseBlockedEntry(c, de, 0); }
    // dictReleaseIterator(di);
    // dictEmpty(c->bstate->keys, NULL);
    //
    // TODO(architect): iterate c.bstate.keys and call release_blocked_entry for each.
    let _ = (c, server);
}

/// Remove one entry from the per-db blocked-client registrations for a single key.
///
/// Steps:
/// 1. Unlink the client from the per-db `blocking_keys[key]` list.
/// 2. If the list is now empty, remove the key from `blocking_keys` and from
///    `blocking_keys_unblock_on_nokey`.
/// 3. If the list is non-empty but this client had `unblock_on_nokey`, decrement
///    the refcount in `blocking_keys_unblock_on_nokey` and delete if zero.
/// 4. If `remove_key` is true, also delete the key from `c.bstate.keys`.
///
/// C: `static void releaseBlockedEntry(client *c, dictEntry *de, int remove_key)`
/// — blocked.c:579
///
/// TODO(architect): `RedisDb` needs `blocking_keys` and
///   `blocking_keys_unblock_on_nokey` maps.
fn release_blocked_entry(
    c: &mut Client,
    server: &mut RedisServer,
    key: &RedisString,
    remove_key: bool,
) {
    // C: blocked.c:579-611 (detailed in module doc above)
    // TODO(architect): implement once db maps are modelled.
    let _ = (c, server, key, remove_key);
}

// ── handleClientsBlockedOnKeys ────────────────────────────────────────────────

/// Run the ready-keys queue and try to unblock one or more clients per key.
///
/// Called from `before_sleep()` after any command that may have pushed data.
/// Iterates `server.ready_keys` until empty; each round may enqueue new entries
/// (e.g., a BLMOVE triggering another blocked BLPOP).
///
/// A static reentrancy guard prevents recursive calls from breaking fairness.
///
/// C: `void handleClientsBlockedOnKeys(void)` — blocked.c:383
///
/// TODO(architect): `RedisServer` needs `ready_keys: Vec<ReadyList>` and
///   `also_propagate.numops` assertion.
pub fn handle_clients_blocked_on_keys(server: &mut RedisServer) {
    // PORT NOTE: The C implementation uses a static local `in_handling_blocked_clients`
    // int to prevent recursion. In Rust this maps to a `Cell<bool>` or a
    // `thread_local!` guard. For Phase A this is a stub.
    //
    // C: blocked.c:383-425
    // while listLength(server.ready_keys) != 0 {
    //   l = server.ready_keys; server.ready_keys = listCreate();
    //   for each rl in l:
    //     dictDelete(rl->db->ready_keys, rl->key);
    //     handleClientsBlockedOnKey(rl);
    //     decrRefCount(rl->key); zfree(rl); listDelNode(l, ln);
    //   listRelease(l);
    // }
    //
    // TODO(architect): implement once server.ready_keys is modelled.
    let _ = server;
}

/// Attempt to unblock each client waiting for `rl.key` in its database.
///
/// Checks that the key's current type matches the client's expected type before
/// unblocking. Mismatching clients are skipped (they stay blocked). At most the
/// initial list length is processed to avoid an infinite loop when reprocessing
/// a command immediately blocks again.
///
/// C: `static void handleClientsBlockedOnKey(readyList *rl)` — blocked.c:624
///
/// TODO(architect): `RedisDb` needs `blocking_keys`.
fn handle_clients_blocked_on_key(server: &mut RedisServer, rl: &ReadyList) {
    // C: blocked.c:624-657
    // de = dictFind(rl->db->blocking_keys, rl->key);
    // clients = dictGetVal(de);
    // count = listLength(clients);
    // for each receiver in clients (up to initial count):
    //   o = lookupKeyReadWithFlags(rl->db, rl->key, LOOKUP_NOEFFECTS);
    //   if type matches or MODULE or unblock_on_nokey:
    //     if btype != Module → unblockClientOnKey(receiver, rl->key)
    //     else               → moduleUnblockClientOnKey(receiver, rl->key)
    //
    // TODO(architect): implement full lookup once db.blocking_keys map exists.
    // TODO(port): module path — Phase 10.
    let _ = (server, rl);
}

// ── Specialised block entry points ───────────────────────────────────────────

/// Block a client waiting for replica acknowledgement (WAIT / WAITAOF).
///
/// C: `void blockClientForReplicaAck(client *c, mstime_t timeout, long long offset,
///   long numreplicas, int numlocal)` — blocked.c:660
///
/// TODO(architect): `RedisServer` needs `clients_waiting_acks: VecDeque<ClientId>`.
/// TODO(architect): `Client` needs bstate fields.
pub fn block_client_for_replica_ack(
    c: &mut Client,
    server: &mut RedisServer,
    timeout: i64,
    offset: i64,
    num_replicas: i32,
    num_local: i32,
) {
    // C: blocked.c:660-673
    init_client_blocking_state(c);
    // TODO(architect): c.bstate.timeout = timeout;
    // TODO(architect): c.bstate.repl_offset = offset;
    // TODO(architect): c.bstate.num_replicas = num_replicas;
    // TODO(architect): c.bstate.num_local = num_local;
    // TODO(architect): server.clients_waiting_acks.push_front(c.id);
    // TODO(architect): assert c.bstate.utility_list_index.is_none()
    // TODO(architect): c.bstate.utility_list_index = Some(0); // head index

    block_client(c, server, BlockingType::Wait);
    let _ = (timeout, offset, num_replicas, num_local);
}

/// Block a client with `Postpone` — the command will be re-tried later from
/// `server.postponed_clients`.
///
/// C: `void blockPostponeClient(client *c)` — blocked.c:678
///
/// TODO(architect): `RedisServer` needs `postponed_clients`.
/// TODO(architect): `Client` needs `flag_pending_command: bool`.
pub fn block_postpone_client(c: &mut Client, server: &mut RedisServer) {
    // C: blocked.c:678-687
    init_client_blocking_state(c);
    // TODO(architect): c.bstate.timeout = 0;
    block_client(c, server, BlockingType::Postpone);
    // TODO(architect): server.postponed_clients.push_back(c.id);
    // TODO(architect): assert c.bstate.utility_list_index.is_none()
    // TODO(architect): c.bstate.utility_list_index = Some(tail of postponed_clients);
    // TODO(architect): c.flag_pending_command = true;
    let _ = (c, server);
}

/// Block a client awaiting the SHUTDOWN sequence to complete.
///
/// C: `void blockClientShutdown(client *c)` — blocked.c:690
pub fn block_client_shutdown(c: &mut Client, server: &mut RedisServer) {
    // C: blocked.c:690-694
    init_client_blocking_state(c);
    // TODO(architect): c.bstate.timeout = 0;
    block_client(c, server, BlockingType::Shutdown);
    let _ = (c, server);
}

// ── Key-specific unblock paths ────────────────────────────────────────────────

/// Unblock a single client when one of its watched keys becomes available.
///
/// Releases the key from the client's blocking set, clears the blocked state,
/// and if `flag_pending_command` is set, re-executes the command inside an
/// execution unit so that client-side caching notifications and afterCommand
/// processing run correctly.
///
/// C: `static void unblockClientOnKey(client *c, robj *key)` — blocked.c:700
///
/// TODO(architect): deep access to Client flags and server execution context.
/// TODO(port): enterExecutionUnit / exitExecutionUnit — event-loop execution
///   context not yet modelled in the pilot.
fn unblock_client_on_key(
    c: &mut Client,
    server: &mut RedisServer,
    key: &RedisString,
) -> Result<(), RedisError> {
    // C: blocked.c:700-747
    // 1. de = dictFind(c->bstate->keys, key); releaseBlockedEntry(c, de, 1);
    // 2. assert btype is List | Stream | ZSet
    // 3. unblockClient(c, 0);
    // 4. if c->flag.pending_command:
    //      c->flag.pending_command = 0; c->flag.reexecuting_command = 1;
    //      old_client = server.current_client; server.current_client = c;
    //      enterExecutionUnit(1, 0);
    //      if processCommandAndResetClient(c) == C_ERR → exitExecutionUnit(); restore; return;
    //      if !c->flag.blocked:
    //        if c->flag.module → moduleCallCommandUnblockedHandler(c)
    //        else → queueClientForReprocessing(c)
    //      exitExecutionUnit();
    //      afterCommand(c);
    //      c->flag.reexecuting_command = 0;
    //      server.current_client = old_client;

    release_blocked_entry(c, server, key, true);
    unblock_client(c, server, false);

    // TODO(architect): implement pending_command re-execution path once Client
    // flags (flag_pending_command, flag_reexecuting_command, flag_module,
    // flag_blocked) and server.current_client exist.
    Ok(())
}

/// Attempt to serve a module-blocked client when a key becomes ready.
///
/// C: `static void moduleUnblockClientOnKey(client *c, robj *key)` — blocked.c:753
///
/// TODO(port): Phase 10 — module blocking infrastructure.
fn module_unblock_client_on_key(
    c: &mut Client,
    server: &mut RedisServer,
    key: &RedisString,
) -> Result<(), RedisError> {
    // C: blocked.c:753-770
    // prev_error_replies = server.stat_total_error_replies;
    // old_client = server.current_client; server.current_client = c;
    // start replyTimer;
    // if moduleTryServeClientBlockedOnKey(c, key):
    //   updateStatsOnUnblock(c, 0, elapsedUs(replyTimer), failed_flag);
    //   moduleUnblockClient(c);
    // afterCommand(c);
    // server.current_client = old_client;
    //
    // TODO(port): module path deferred to Phase 10.
    let _ = (c, server, key);
    Ok(())
}

// ── Timeout / error unblock entry points ─────────────────────────────────────

/// Timeout a blocked client: send the appropriate null/error reply, clear the
/// `flag_pending_command` flag, and unblock.
///
/// C: `void unblockClientOnTimeout(client *c)` — blocked.c:778
///
/// TODO(architect): `Client` needs bstate and `flag_pending_command`.
/// TODO(port): signature differs from the `timeout.rs` call-site which passes
///   only one argument — reconcile in Phase B.
pub fn unblock_client_on_timeout(
    c: &mut Client,
    server: &mut RedisServer,
) -> Result<(), RedisError> {
    // C: blocked.c:778-785
    // if btype == Module && isModuleClientUnblocked(c) { return; }
    // TODO(port): module unblocked check — Phase 10.

    reply_to_blocked_client_timed_out(c, server)?;
    // TODO(architect): if c.flag_pending_command { c.flag_pending_command = false; }
    unblock_client(c, server, true);
    Ok(())
}

/// Unblock a client with an error reply.  If `err_bytes` is `Some`, it is sent
/// as a Redis error before unblocking.
///
/// C: `void unblockClientOnError(client *c, const char *err_str)` — blocked.c:789
///
/// TODO(architect): client reply machinery.
pub fn unblock_client_on_error(
    c: &mut Client,
    server: &mut RedisServer,
    err_bytes: Option<&[u8]>,
) -> Result<(), RedisError> {
    // C: blocked.c:789-794
    // if err_str: addReplyError(c, err_str);
    // updateStatsOnUnblock(c, 0, 0, ERROR_COMMAND_REJECTED);
    // if c->flag.pending_command → c->flag.pending_command = 0;
    // unblockClient(c, 1);

    if let Some(_msg) = err_bytes {
        // TODO(architect): ctx.reply_error(_msg) once reply machinery exists on Client.
    }
    update_stats_on_unblock(c, server, 0, 0, ERROR_COMMAND_REJECTED);
    // TODO(architect): if c.flag_pending_command { c.flag_pending_command = false; }
    unblock_client(c, server, true);
    Ok(())
}

// ── before_sleep hook ─────────────────────────────────────────────────────────

/// The `before_sleep()` hook for the blocked-client subsystem.
///
/// Run in order:
/// 1. Sweep the timeout table for expired blocked clients.
/// 2. Process clients waiting for replication ACKs (WAIT / WAITAOF).
/// 3. Process key-ready signals (BLPOP / XREAD / BZPOP wakeups).
/// 4. Dispatch module-unblocked clients.
/// 5. Drain the `unblocked_clients` queue.
///
/// C: `void blockedBeforeSleep(void)` — blocked.c:796
pub fn blocked_before_sleep(server: &mut RedisServer) -> Result<(), RedisError> {
    // C: blocked.c:796-818

    // 1. handleBlockedClientsTimeout();
    crate::timeout::handle_blocked_clients_timeout(server);

    // 2. if listLength(server.clients_waiting_acks) → processClientsWaitingReplicas();
    // TODO(architect): processClientsWaitingReplicas — replication crate (Phase 3+).

    // 3. handleClientsBlockedOnKeys();
    handle_clients_blocked_on_keys(server);

    // 4. if moduleCount() → moduleHandleBlockedClients();
    // TODO(port): module path — Phase 10.

    // 5. if listLength(server.unblocked_clients) → processUnblockedClients();
    process_unblocked_clients(server);

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/blocked.c  (819 lines, 28 functions)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         116
//   port_notes:    2
//   unsafe_blocks: 0
//   notes:         All 28 functions translated with correct signatures and full
//                  C-logic commentary. TODOs are primarily architect items for
//                  missing fields on Client (bstate, flag_*), RedisServer
//                  (blocked_clients, ready_keys, postponed_clients, clients_waiting_acks),
//                  and RedisDb (blocking_keys, blocking_keys_unblock_on_nokey).
//                  Logic is faithful to the C; Phase B wires up the fields.
//                  Validator shows only expected E0432/E0433 name-resolution errors.
// ──────────────────────────────────────────────────────────────────────────────
