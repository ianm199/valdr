//! Memory-prefetch optimization for batched command processing.
// Deferred feature: Valkey-8 batch dict-prefetch optimization; wired when
// redis-ds hashtable and Client prefetch fields are ported (Phase B+).
#![allow(dead_code)]
//! Prefetching keys and data for multiple commands in a batch amortizes
//! memory-access latency across multiple operations. The batch accumulates
//! clients until it is full, then prefetches the relevant hashtable entries
//! before executing all commands together.
//! # Design overview
//! 1. `prefetch_commands_batch_init` allocates the singleton batch
//! `server.prefetch_batch_max_size`.
//! 2. `add_command_to_batch_and_process_if_full` is called once per incoming
//! client on the I/O-thread path; it adds the client and its keys to
//! batch, then fires `process_clients_commands_batch` when the batch is full.
//! 3. `process_clients_commands_batch` calls `prefetch_commands` (which issues
//! hardware prefetch hints into L1 cache) then dispatches each client's
//! command via `process_pending_command_and_input_buffer`.
//! # Phase-A limitations
//! - **Hardware prefetch**: `valkey_prefetch` wraps `__builtin_prefetch`; Rust
//! equivalents (`core::intrinsics::prefetch_read_data`) are `unsafe`
//!   nightly-only.  All call sites are stubbed — see `TODO(architect)` below.
//! - **`HashTable` / `HashtableIncrementalFindState`**: from redis-ds
//! (not yet ported); represented as opaque placeholder types.
//! - **Several `Client` fields** (`parsed_cmd`, `cmd_queue`, `read_flags`,
//!   `slot`) are not yet on the Rust `Client` stub; see `TODO(architect)`.
//! - **Global singleton**: translated as `thread_local!` for the Phase-2
//! single-threaded model. Phase 3+ multi-thread strategy is deferred.


use std::cell::RefCell;

use crate::client::{Client, ClientId};
use redis_types::RedisError;

// ── Opaque placeholder types (redis-ds not yet ported) ───────────────────────

/// Placeholder for `hashtableIncrementalFindState`.
/// TODO(architect): replace with `redis_ds::hashtable::IncrementalFindState`
/// once the redis-ds crate is ported. The C type drives a multi-step
/// incremental lookup that issues cache-line prefetches at each step.
/// Requires a dependency edge: redis-core → redis-ds.
pub struct HashtableIncrementalFindState {
    _opaque: (),
}

/// Placeholder for `hashtable *`.
/// TODO(architect): replace with a concrete `redis_ds::hashtable::Hashtable`
/// reference once the redis-ds crate is ported. Represents the per-slot
/// keyspace hashtable obtained via `kvstoreGetHashtable`.
pub struct Hashtable {
    _opaque: (),
}

// ── Internal types ────────────────────────────────────────────────────────────

/// Phase of prefetch for a single key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefetchState {
 /// Prefetch hashtable entries associated with this key's hash bucket.
    Entry,
 /// Prefetch the value object found in the previous step.
    Value,
 /// Prefetching is complete for this key.
    Done,
}

/// Per-key prefetch bookkeeping: current phase + incremental find state.
struct KeyPrefetchInfo {
    state: PrefetchState,
 /// Incremental hashtable find state machine.
    /// TODO(architect): replace with redis_ds::hashtable::IncrementalFindState.
    hashtab_state: HashtableIncrementalFindState,
}

/// All state for one batch of client commands being prefetched and processed.
/// PORT NOTE: `clients` holds `ClientId` values rather than raw `*mut Client`
/// pointers. Any function that needs to drive a client's command execution
/// must receive a mutable reference to the server's client table so it can
/// resolve ids to live references.
/// PORT NOTE: `keys` in C is `void **` that starts as `robj *` argv pointers
/// and is mutated in-place to `sds` data pointers by `prefetchCommands`. In
/// Rust we store `Vec<u8>` clones (already resolved byte data) so no in-place
/// pointer swap is needed. This loses the zero-copy property; the whole module
/// is a performance optimization so Phase B should profile this.
/// PERF(port): C uses a raw pointer array with no per-key heap allocation.
struct PrefetchCommandsBatch {
 /// Round-robin cursor through `prefetch_info`.
    cur_idx: usize,
 /// Number of keys whose prefetch is complete.
    keys_done: usize,
 /// Number of keys currently in the batch.
    key_count: usize,
 /// Number of clients currently in the batch.
    client_count: usize,
 /// Maximum batch capacity; set from `server.prefetch_batch_max_size`.
    max_prefetch_size: usize,
 /// Number of commands executed so far in this batch invocation.
 /// A non-zero value signals a recursive call from `ProcessingEventsWhileBlocked`.
    executed_commands: usize,
 /// Cluster slot for each key (index-parallel to `keys`; 0 when clustering off).
    slots: Vec<i32>,
 /// Key byte data to prefetch (one per batch slot).
 /// raw argv object pointers, later recast to sds data.
 /// PORT NOTE: stored as owned byte vec rather than raw pointer; see struct note.
    keys: Vec<Vec<u8>>,
 /// Clients in the batch. `None` means the slot was cleared mid-batch
 /// (e.g., by `remove_client_from_pending_commands_batch`).
    clients: Vec<Option<ClientId>>,
 /// Hashtable for each key's db slot.
    /// TODO(architect): change element type to a redis-ds Hashtable reference.
    keys_tables: Vec<Option<Box<Hashtable>>>,
 /// Prefetch state machine for each key.
    prefetch_info: Vec<KeyPrefetchInfo>,
}

// ── Global singleton ──────────────────────────────────────────────────────────

// Per-thread batch singleton, mirroring C `static PrefetchCommandsBatch *batch`.
// PORT NOTE: The C global is process-wide. In the Phase-2 single-threaded
// model, `thread_local!` is behaviorally equivalent. Phase 3+ multi-threaded
// I/O may need a different strategy.
// TODO(architect): Decide ownership model for multi-threaded Phase 3 (shared
// Mutex vs per-thread batches vs actor-style dispatch).
thread_local! {
    static BATCH: RefCell<Option<Box<PrefetchCommandsBatch>>> = const { RefCell::new(None) };
}

// ── PrefetchCommandsBatch helpers ─────────────────────────────────────────────

impl PrefetchCommandsBatch {
 /// Allocate and zero-initialise a new batch with `max_prefetch_size` slots.
    fn new(max_prefetch_size: usize) -> Box<Self> {
        let prefetch_info: Vec<KeyPrefetchInfo> = (0..max_prefetch_size)
            .map(|_| KeyPrefetchInfo {
                state: PrefetchState::Done,
                hashtab_state: HashtableIncrementalFindState { _opaque: () },
            })
            .collect();

        Box::new(Self {
            cur_idx: 0,
            keys_done: 0,
            key_count: 0,
            client_count: 0,
            max_prefetch_size,
            executed_commands: 0,
            slots: vec![0i32; max_prefetch_size],
            keys: vec![Vec::new(); max_prefetch_size],
            clients: vec![None; max_prefetch_size],
            keys_tables: (0..max_prefetch_size).map(|_| None).collect(),
            prefetch_info,
        })
    }

 /// Advance the round-robin cursor by one, wrapping at `key_count`.
    fn move_to_next_key(&mut self) {
        self.cur_idx = (self.cur_idx + 1) % self.key_count;
    }

 /// Return the index of the next `KeyPrefetchInfo` that is not yet `Done`,
 /// or `None` if all keys are done.
 /// PORT NOTE: returns an index rather than a raw pointer so the caller can
 /// borrow `prefetch_info` mutably after the call.
    fn next_pending_idx(&mut self) -> Option<usize> {
        if self.key_count == 0 {
            return None;
        }
        let start_idx = self.cur_idx;
        loop {
            let idx = self.cur_idx;
            if self.prefetch_info[idx].state != PrefetchState::Done {
                return Some(idx);
            }
            self.cur_idx = (self.cur_idx + 1) % self.key_count;
            if self.cur_idx == start_idx {
                return None;
            }
        }
    }

 /// Reset all batch counters; keeps allocated storage for reuse.
    fn reset(&mut self) {
        self.cur_idx = 0;
        self.keys_done = 0;
        self.key_count = 0;
        self.client_count = 0;
        self.executed_commands = 0;
    }
}

// ── Module-level private functions ────────────────────────────────────────────

/// Mark key at `idx` as done, incrementing the batch-level counter.
/// PORT NOTE: the C version also increments `server.stat_total_prefetch_entries`.
/// That stat field does not yet exist on the Rust `RedisServer` stub.
/// TODO(architect): add `stat_total_prefetch_entries: u64` to `RedisServer`.
fn mark_key_done(batch: &mut PrefetchCommandsBatch, idx: usize) {
    batch.prefetch_info[idx].state = PrefetchState::Done;
    // TODO(port): server.stat_total_prefetch_entries += 1
    batch.keys_done += 1;
}

/// Initialise per-key prefetch info before the prefetch loop runs.
/// Keys whose hashtable is absent or empty are immediately marked done.
/// The rest have their incremental find state initialised.
fn init_batch_info(batch: &mut PrefetchCommandsBatch) {
    for i in 0..batch.key_count {
        let table_present_and_non_empty = match &batch.keys_tables[i] {
            None => false,
            Some(_t) => {
                // TODO(port): call hashtableSize(t) != 0 once redis-ds is available
                false // stub: treat all as empty until redis-ds lands
            }
        };

        if !table_present_and_non_empty {
            batch.prefetch_info[i].state = PrefetchState::Done;
            batch.keys_done += 1;
            continue;
        }

        batch.prefetch_info[i].state = PrefetchState::Entry;
        // TODO(port): hashtableIncrementalFindInit(
 // &info.hashtab_state, tables[i], batch.keys[i]
 // Needs: redis_ds::hashtable Hashtable + IncrementalFindState API.
    }
}

/// Perform one incremental step of hashtable lookup for the key at `idx`.
/// If the step is not yet complete, advance to the next key and come back.
/// If the step finished and copy-avoidance is active, mark done without
/// prefetching the value. Otherwise, transition to `PrefetchState::Value`.
/// TODO(architect): `server.io_threads_num` and `server.min_io_threads_copy_avoid`
/// are not yet on the Rust `RedisServer` stub. Add them as `i32` fields.
fn prefetch_entry(batch: &mut PrefetchCommandsBatch, idx: usize) {
    // TODO(port): let still_searching = batch.prefetch_info[idx].hashtab_state.step();
    let still_searching = false; // stub

    if still_searching {
 // Not done yet — defer and let another key run.
        batch.move_to_next_key();
        return;
    }

    // TODO(port): check server.io_threads_num >= server.min_io_threads_copy_avoid
    let copy_avoid_active = false; // stub

    if copy_avoid_active {
        mark_key_done(batch, idx);
    } else {
        batch.prefetch_info[idx].state = PrefetchState::Value;
    }
}

/// Prefetch the value object for the entry found in `prefetch_entry`.
/// If the entry lookup succeeded and the value is a raw-encoding string,
/// issue a hardware prefetch hint for the value's data pointer. Then mark
/// the key done regardless.
/// TODO(architect): `valkey_prefetch(ptr)` wraps `__builtin_prefetch`.  Rust
/// equivalents (`core::intrinsics::prefetch_read_data`) require `unsafe`
/// nightly. This call site must remain a no-op until the architect decides
/// whether to gate this behind a feature flag or use a stable substitute
/// (e.g., a manual loop that touches the cache line).
fn prefetch_value(batch: &mut PrefetchCommandsBatch, idx: usize) {
    // TODO(port): let entry = batch.prefetch_info[idx].hashtab_state.get_result();
 // If entry is Some and val.encoding == OBJ_ENCODING_RAW && val.type == OBJ_STRING:
    //   TODO(architect): valkey_prefetch(objectGetVal(val))

    mark_key_done(batch, idx);
}

/// Drive the prefetch state machine for every key in `batch`.
fn hashtable_prefetch(batch: &mut PrefetchCommandsBatch) {
    init_batch_info(batch);

    loop {
        let Some(idx) = batch.next_pending_idx() else {
            break;
        };
        let state = batch.prefetch_info[idx].state;
        match state {
            PrefetchState::Entry => prefetch_entry(batch, idx),
            PrefetchState::Value => prefetch_value(batch, idx),
            PrefetchState::Done => {
 // Unreachable: `next_pending_idx` never returns a Done index.
                // TODO(architect): confirm whether panic! is appropriate here, or
 // whether a logged error and break is preferred.
                debug_assert!(
                    false,
                    "next_pending_idx returned a Done slot — invariant broken"
                );
                break;
            }
        }
    }
}

/// Orchestrate all prefetch operations for the current batch.
/// 1. Prefetch each client's argv pointers (brought in by the I/O thread).
/// 2. Prefetch the raw data pointer inside each argv object (if RAW encoding).
/// 3. Resolve key objects to their byte-data pointers.
/// 4. Run the hashtable prefetch loop (only when > 1 key).
/// TODO(architect): Steps 1-3 require `Client.argv: Vec<RedisObject>` and
/// `RedisObject.encoding` field (raw/embedded/int), neither of which exists
/// on the current Rust stubs. The function body is a skeleton until those
/// fields are defined.
/// TODO(architect): `valkey_prefetch(c->argv[j])` — hardware prefetch; see
/// `prefetch_value` for the full constraint note.
/// TODO(architect): `server.stat_total_prefetch_batches` field missing from
/// `RedisServer`. Add as `u64`.
fn prefetch_commands(batch: &mut PrefetchCommandsBatch) {
 // Step 1: prefetch argv object headers for each client (skipping argv[0] —
 // command name — which the I/O thread already looked up).
 // for i in 0..batch.client_count {
 // let Some(client_id) = batch.clients[i] else { continue };
    //     // TODO(port): resolve client_id → &Client via server client table
 // // if client.argc <= 1 { continue }
 // // for j in 1..client.argc {
    //     //     TODO(architect): valkey_prefetch(client.argv[j])
 // // }
 // }

 // Step 2: prefetch the raw data pointer inside each OBJ_ENCODING_RAW argv.
 // for i in 0..batch.client_count {
    //     // TODO(port): similar loop; check argv[j].encoding == RAW
    //     //     TODO(architect): valkey_prefetch(objectGetVal(argv[j]))
 // }

 // Step 3: resolve key objects to raw byte data.
 // In C: batch->keys[i] = objectGetVal((robj *)batch->keys[i])
 // In Rust: keys are already Vec<u8> clones, so this is a no-op.
 // PORT NOTE: The C mutation is a pointer-swap within a `void *` array;
 // Rust avoids this by storing resolved bytes from the start.

 // Step 4: hashtable prefetch (only beneficial with more than one key).
    if batch.key_count > 1 {
        // TODO(port): server.stat_total_prefetch_batches += 1
        hashtable_prefetch(batch);
    }
}

/// Extract keys from `cmd`/`argv` and append them to the batch.
/// TODO(port): `getKeysFromCommand` / `GetKeysResult` are not yet ported
///. The function is a skeleton.
/// TODO(architect): The parameter `cmd` maps to `CommandSpec` (owner:
/// `redis-commands::spec`). Adding a dep edge redis-core → redis-commands
/// creates a cycle (redis-commands already depends on redis-core). Either
/// CommandSpec must move to redis-types, or key-extraction must be
/// callback-based. Flag for architect to resolve.
/// C parameter mapping:
/// `struct serverCommand *cmd` → would be `&CommandSpec` (audit type)
/// `robj **argv` → would be `&[RedisObject]`
/// `argc: i32` → `argc: i32`
/// `serverDb *db` → `db_index: usize` (no direct db ref here)
/// `slot: i32` → `slot: i32`
fn add_command_to_batch(_batch: &mut PrefetchCommandsBatch, _argc: i32, slot: i32) {
    // TODO(port): call getKeysFromCommand(cmd, argv, argc, &result) and iterate
 // over result.keys[i].pos to identify argv positions that are keys.
 // for i in 0..num_keys {
 // if batch.key_count >= batch.max_prefetch_size { break }
 // batch.keys[batch.key_count] = argv[result.keys[i].pos].clone_bytes;
 // batch.slots[batch.key_count] = if slot >= 0 { slot } else { 0 };
    //     // TODO(port): batch.keys_tables[batch.key_count] = kvstoreGetHashtable(db.keys, slot)
 // batch.key_count += 1;
 // }
    let _ = slot;
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Allocate the global prefetch batch according to server configuration.
/// No-op if the batch already exists or if `prefetch_batch_max_size == 0`.
/// TODO(architect): `server.prefetch_batch_max_size` is not yet on the Rust
/// `RedisServer` stub. Add as `usize`. Passed here as a parameter until
/// the stub is updated.
pub fn prefetch_commands_batch_init(max_prefetch_size: usize) {
    BATCH.with(|cell| {
        let mut guard = cell.borrow_mut();
        if guard.is_some() {
            return;
        }
        if max_prefetch_size == 0 {
            return;
        }
        *guard = Some(PrefetchCommandsBatch::new(max_prefetch_size));
    });
}

/// Drop the global prefetch batch and free all associated memory.
pub fn free_prefetch_commands_batch() {
    BATCH.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Config-change callback: reinitialise the batch when `prefetch_batch_max_size` changes.
/// Returns `true` (C_OK) unconditionally. If a batch with clients is
/// progress, the resize is deferred until the batch drains.
/// TODO(architect): The C signature takes `const char **err` (config callback
/// convention). A clean Rust equivalent would be a closure registered with
/// the config subsystem; the current signature is a minimal stub.
/// TODO(architect): `server.prefetch_batch_max_size` needed to re-read the new
/// value after the change. Passed as a parameter here.
pub fn on_max_batch_size_change(new_max: usize) -> bool {
    let has_clients = BATCH.with(|cell| cell.borrow().as_ref().is_some_and(|b| b.client_count > 0));

    if has_clients {
 // Batch in progress — defer the resize.
        return true;
    }

    free_prefetch_commands_batch();
    prefetch_commands_batch_init(new_max);
    true
}

/// Process all prefetched commands in the current batch.
/// 1. On the first (non-recursive) invocation: runs `prefetch_commands`
/// issue cache prefetch hints for all keys.
/// 2. Executes each client's pending command in order, nulling each client
/// slot before calling into it to prevent re-entry.
/// 3. Resets the batch for the next round.
/// 4. If `max_prefetch_size` changed while the batch was live, reinitialises.
/// TODO(port): `process_pending_command_and_input_buffer(client)` and
/// `before_next_client(client)` — these live in /
/// and are not yet ported. The dispatch loop is a skeleton.
/// TODO(architect): The function needs a mutable reference to the server's
/// client table to resolve `ClientId` → `&mut Client`. Current signature
/// cannot implement the dispatch loop without that reference.
pub fn process_clients_commands_batch() -> Result<(), RedisError> {
    let has_batch_with_clients =
        BATCH.with(|cell| cell.borrow().as_ref().is_some_and(|b| b.client_count > 0));

    if !has_batch_with_clients {
        return Ok(());
    }

    let is_first_invocation = BATCH.with(|cell| {
        cell.borrow()
            .as_ref()
            .is_some_and(|b| b.executed_commands == 0)
    });

    if is_first_invocation {
        BATCH.with(|cell| {
            if let Some(batch) = cell.borrow_mut().as_mut() {
                prefetch_commands(batch);
            }
        });
    }

 // for i in 0..batch.client_count {
 // let Some(client_id) = batch.clients[i] else { continue };
 // Lients[i] = None; // null out before recursive call
 // batch.executed_commands += 1;
    //     // TODO(port): process_pending_command_and_input_buffer(client_id)?;
    //     // TODO(port): before_next_client(client_id);
 // }

    BATCH.with(|cell| {
        if let Some(batch) = cell.borrow_mut().as_mut() {
            batch.reset();
        }
    });

 // handle deferred resize.
    // TODO(port): compare batch.max_prefetch_size vs server.prefetch_batch_max_size
 // and call on_max_batch_size_change if they differ.
 // Requires server.prefetch_batch_max_size field.

    Ok(())
}

/// Add the client's command (and any queued commands) to the batch.
/// Triggers `process_clients_commands_batch` if the batch fills up.
/// Returns `Ok(` (C_OK) when the client was added successfully, or
/// `Err(RedisError::runtime(...))` (C_ERR) if no batch exists.
/// TODO(architect): Requires the following `Client` fields that are not yet
/// on the Rust stub:
/// - `client.parsed_cmd: Option<CommandSpec>` — next command to run
/// - `client.read_flags: u32` — bitmask; needs READ_FLAGS_BAD_ARITY /
/// READ_FLAGS_PREFETCHED constants
/// - `client.slot: i32` — cluster slot (−1 if disabled)
/// - `client.cmd_queue: CmdQueue` — queued commands from pipelining
/// - `client.db: &RedisDb` or db index
/// TODO(architect): `READ_FLAGS_BAD_ARITY` and `READ_FLAGS_PREFETCHED` bit
/// constants — define in `crates/redis-core/src/client.rs`.
pub fn add_command_to_batch_and_process_if_full(client: &mut Client) -> Result<(), RedisError> {
    let batch_exists = BATCH.with(|cell| cell.borrow().is_some());
    if !batch_exists {
        return Err(RedisError::runtime(b"no prefetch batch initialised"));
    }

    let client_id = client.id();

    BATCH.with(|cell| {
        let mut guard = cell.borrow_mut();
        let batch = guard.as_mut().expect("batch was Some above");

        let slot = batch.client_count;
        batch.clients[slot] = Some(client_id);
        batch.client_count += 1;

 // if c->parsed_cmd && !(c->read_flags & READ_FLAGS_BAD_ARITY):
 // c->read_flags |= READ_FLAGS_PREFETCHED
 // add_command_to_batch(batch, c->parsed_cmd, c->argv, c->argc, c->db, c->slot)
        // TODO(port): read client.parsed_cmd, read_flags, argv, argc, db, slot
        add_command_to_batch(batch, 0, -1); // stub

 // queued pipeline commands.
 // for p in client.cmd_queue[off..len]:
 // if !p.cmd: continue
 // p.read_flags |= READ_FLAGS_PREFETCHED
 // add_command_to_batch(batch, p.cmd, p.argv, p.argc, c.db, p.slot)
        // TODO(port): iterate client.cmd_queue when it exists on Client
    });

 // Fire if batch is full by client count or by key count.
    let should_process = BATCH.with(|cell| {
        cell.borrow().as_ref().is_some_and(|b| {
            b.client_count == b.max_prefetch_size || b.key_count == b.max_prefetch_size
        })
    });

    if should_process {
        process_clients_commands_batch()?;
    }

    Ok(())
}

/// Remove the given client from the batch if it is present, without executing it.
/// Called when a client is freed or disconnected before its batch fires.
pub fn remove_client_from_pending_commands_batch(client_id: ClientId) {
    BATCH.with(|cell| {
        let mut guard = cell.borrow_mut();
        let Some(batch) = guard.as_mut() else { return };
        for slot in batch.clients[..batch.client_count].iter_mut() {
            if *slot == Some(client_id) {
                *slot = None;
                return;
            }
        }
    });
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//              +   src/memory_prefetch.h  (12 lines, 5 declarations)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         14
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:         All function skeletons are present; logic correct for the
//                  parts that do not depend on redis-ds or unported Client
//                  fields.  Hardware prefetch calls are no-ops.  Phase B
//                  must wire up redis-ds::hashtable, the Client stub fields
//                  (parsed_cmd / read_flags / slot / cmd_queue), and the
//                  server stats fields before this module can function.
// ──────────────────────────────────────────────────────────────────────────────
