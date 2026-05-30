//! Timeout handling for blocked and idle clients.
//! Two main responsibilities:
//! 1. **Idle-client timeout** — `clients_cron_handle_timeout` is called once
//! per client per server-cron tick and terminates clients that have been
//! idle longer than `server.maxidletime`.
//! 2. **Blocked-client timeout table** — clients that block with a non-zero
//! deadline are inserted into a sorted 16-byte-keyed structure so that
//! `handle_blocked_clients_timeout` can sweep them cheaply in `before_sleep`.
//! Keys encode `[be(timeout_ms) | client_id]`; lexicographic order equals
//! chronological order, enabling an O(expired) early-exit sweep.
//! ## Phase notes
//! The radix-tree timeout table (`server.clients_timeout_table` in C) maps
//! `RadixTree` from `redis-ds` (Phase 4/5, `audit` tier in type-vocabulary).
//! Until that crate lands, `add_client_to_timeout_table`,
//! `remove_client_from_timeout_table`, and `handle_blocked_clients_timeout`
//! are stubs guarded by `TODO(architect)` markers.
//! Key-encoding helpers (`encode_timeout_key` / `decode_timeout_key`) are
//! fully translated and ready for use once the table type is wired.

use redis_types::{RedisError, RedisResult};

use crate::client::{Client, ClientId};
use crate::object::RedisObject;
use crate::server::RedisServer;

/// Length in bytes of one blocked-client timeout-table key:
/// 8 bytes big-endian deadline (ms) followed by 8 bytes client ID.
pub const CLIENT_ST_KEY_LEN: usize = 16;

/// Time unit for a timeout value supplied as a command argument.
/// TODO(architect): `TimeUnit` is consumed by SET, EXPIRE, BLPOP, WAIT, and
/// other commands across multiple crates. Relocate to `redis-types` or a
/// shared `redis-core::util` module once those commands are ported and
/// duplication becomes unavoidable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
 /// Timeout supplied in seconds (parsed as float; fractional values allowed).
    Seconds,
 /// Timeout supplied in whole milliseconds (parsed as integer).
    Milliseconds,
}

// ── Blocked-client timeout check ─────────────────────────────────────────────

/// Check whether a blocked client has reached its timeout deadline.
/// If the client is blocked, has a non-zero deadline, and that deadline is
/// strictly before `now` (milliseconds), calls `unblock_client_on_timeout`
/// and returns `true`. Returns `false` and performs no operation otherwise.
/// PORT NOTE(Phase B): added `server: &mut RedisServer` to match the canonical
/// `unblock_client_on_timeout` signature in `blocked.rs`, which needs server
/// state to drive `reply_to_blocked_client_timed_out` and `unblock_client`.
/// The original C `checkBlockedClientTimeout(client *c, mstime_t now)` got
/// the server implicitly via the global; Rust passes it explicitly.
pub fn check_blocked_client_timeout(
    client: &mut Client,
    _server: &mut RedisServer,
    now: i64,
) -> bool {
    // TODO(port): Client needs `is_blocked()` and `blocking_timeout() -> i64`
 // accessors once blocking state (c->flag.blocked, c->bstate->timeout) is
 // added to the Client struct.
    if client.is_blocked() {
        let timeout = client.blocking_timeout();
        if timeout != 0 && timeout < now {
            return true;
        }
    }
    false
}

/// Per-client cron check: handle idle timeout and cluster-blocked redirect.
/// Called once per client per cron tick. `now_ms` is the current wall-clock
/// time in milliseconds; it is passed in to avoid repeated syscalls across
/// cron loop's client sweep.
/// Returns `true` if the client was terminated and **must not be accessed
/// again** by the caller.
/// PORT NOTE: The C implementation calls `freeClient(c)` which removes
/// client from the server's linked list and closes the connection in-place.
/// The Rust equivalent signals termination by returning `true`; the cron-loop
/// owner is responsible for removal and connection teardown (Phase 3).
pub fn clients_cron_handle_timeout(
    server: &mut RedisServer,
    client: &mut Client,
    now_ms: i64,
) -> bool {
    let now: i64 = now_ms / 1000;

    // TODO(port): RedisServer needs `max_idle_time() -> i64` (seconds; maps to
 // server.maxidletime). Client needs `is_replica`, `must_obey`,
 // `is_blocked`, `is_pubsub`, and `last_interaction -> i64` (seconds)
 // accessors before this block can be wired up.
    if server.max_idle_time() > 0
        && !client.is_replica()
        && !client.must_obey()
        && !client.is_blocked()
        && !client.is_pubsub()
        && (now - client.last_interaction()) > server.max_idle_time()
    {
        log::debug!("Closing idle client id={}", client.id);
 // Caller must free (remove from client list, close connection).
        return true;
    } else if client.is_blocked() {
 // Cluster: unblock and redirect clients blocked on keys that are no
 // longer served by this node.
        // TODO(architect): cluster_enabled() and clusterRedirectBlockedClientIfNeeded
 // live in the redis-cluster crate (Phase 3+). Wire up when available.
 // if (clusterRedirectBlockedClientIfNeeded(c))
 // unblockClientOnError(c, NULL);
 // }
        if server.cluster_enabled() {
            // TODO(port): call cluster::redirect_blocked_client_if_needed(client);
 // if redirected, unblock the client with a cluster-redirect error.
        }
    }
    false
}

// ── Timeout-table key encoding ────────────────────────────────────────────────

/// Encode a 16-byte blocked-client radix-tree key from a deadline and a client ID.
/// Key layout:
/// ```text
/// bytes 0..8 — deadline_ms encoded as big-endian u64
/// (lexicographic order == chronological order)
/// bytes 8..16 — client_id encoded as little-endian u64
/// (disambiguation within the same millisecond)
/// ```
/// PORT NOTE: The C implementation stores a raw `client *` pointer in bytes
/// 8..16 via `writePointerWithPadding(buf + 8, c)`. Rust stores the `ClientId`
/// instead so that decoding is safe and no raw pointer is persisted across
/// ownership boundaries. After decoding, callers look up the client through
/// `RedisServer::find_client(client_id)`.
pub fn encode_timeout_key(client_id: ClientId, deadline_ms: u64) -> [u8; CLIENT_ST_KEY_LEN] {
    let mut buf = [0u8; CLIENT_ST_KEY_LEN];
 // Big-endian so that byte-lexicographic order equals chronological order.
    buf[..8].copy_from_slice(&deadline_ms.to_be_bytes());
 // Little-endian client ID for disambiguation; ordering within this field
 // is irrelevant because all keys with the same deadline are treated equally.
    buf[8..].copy_from_slice(&client_id.to_le_bytes());
    buf
}

/// Decode a 16-byte radix-tree key into `(deadline_ms, client_id)`.
/// PORT NOTE: C decodes a raw `client *` in bytes 8..16. Rust decodes a
/// `ClientId`; the caller must look up the live client and handle the case
/// where it has already been freed or unblocked.
pub fn decode_timeout_key(buf: &[u8; CLIENT_ST_KEY_LEN]) -> (u64, ClientId) {
    let mut deadline_bytes = [0u8; 8];
    deadline_bytes.copy_from_slice(&buf[..8]);

    let mut id_bytes = [0u8; 8];
    id_bytes.copy_from_slice(&buf[8..]);

    (
        u64::from_be_bytes(deadline_bytes),
        u64::from_le_bytes(id_bytes),
    )
}

// ── Timeout-table mutation ────────────────────────────────────────────────────

/// Register a blocked client in the server's timeout table.
/// No-op when the client's blocking deadline is zero (block forever).
/// On successful insertion the client's `in_to_table` flag is set so that
/// `remove_client_from_timeout_table` can skip the lookup when the flag is clear.
pub fn add_client_to_timeout_table(server: &mut RedisServer, client: &mut Client) {
    // TODO(port): Client needs `blocking_timeout() -> i64`.
    let timeout = client.blocking_timeout();
    if timeout == 0 {
        return;
    }
    let key = encode_timeout_key(client.id, timeout as u64);
    // TODO(architect): RedisServer needs `clients_timeout_table: RadixTree`
 // (redis-ds, Phase 4/5).
 // c->flag.in_to_table = 1;
    // TODO(port): server.clients_timeout_table.try_insert(&key)
 // → client.set_in_timeout_table(true)
    let _ = (server, key);
}

/// Remove a blocked client from the server's timeout table.
/// No-op when the client's `in_to_table` flag is clear.
pub fn remove_client_from_timeout_table(server: &mut RedisServer, client: &mut Client) {
    // TODO(port): Client needs `in_timeout_table() -> bool` and
 // `set_in_timeout_table(bool)`, and `blocking_timeout -> i64`.
    if !client.in_timeout_table() {
        return;
    }
    client.set_in_timeout_table(false);
    let timeout = client.blocking_timeout();
    let key = encode_timeout_key(client.id, timeout as u64);
    // TODO(architect): server.clients_timeout_table.remove(&key)
    let _ = (server, key);
}

// ── Blocked-client timeout sweep ─────────────────────────────────────────────

/// Sweep the blocked-client timeout table and unblock every client whose
/// deadline has passed.
/// Called from `before_sleep` on each event-loop iteration. Because keys
/// are stored in chronological order (big-endian deadline prefix), the loop
/// stops at the first key whose deadline is still in the future — O(expired)
/// work per call in the common case.
/// After each removal the iterator is re-seeked to the beginning, matching
/// the C implementation's `raxSeek(&ri, "^", NULL, 0)` after `raxRemove`.
/// This handles the case where removal shifts tree structure.
pub fn handle_blocked_clients_timeout(server: &mut RedisServer) {
    // TODO(architect): Full implementation requires `RadixTree` from redis-ds
 // (Phase 4/5). Stubbed until that crate is available.
 // Pseudocode matching the C structure:
 // let table = &mut server.clients_timeout_table;
 // if table.is_empty { return; }
    //   let now: u64 = ms_time();  // TODO(port): real millisecond clock
 // loop {
 // let Some(raw_key) = table.first_key else { break };
 // let key: [u8; CLIENT_ST_KEY_LEN] = …;
 // let (deadline_ms, client_id) = decode_timeout_key(&key);
 // if deadline_ms >= now { break; } // all remaining are future
 // // C: c->flag.in_to_table = 0;
 // if let Some(c) = server.find_client_mut(client_id) {
 // c.set_in_timeout_table(false);
 // check_blocked_client_timeout(c, now as i64);
 // }
 // table.remove(&key);
 // // Re-seek to start) because
 // // removal may shift the tree layout.
 // }
    let _ = server;
}

// ── Timeout argument parsing ──────────────────────────────────────────────────

/// Parse a timeout argument from a `RedisObject` and return an absolute
/// deadline in milliseconds.
/// Interpretation is controlled by `unit`:
/// - `TimeUnit::Seconds` — the object is decoded as a floating-point number
/// of seconds and ceiled to the next whole millisecond.
/// - `TimeUnit::Milliseconds` — the object is decoded as a whole-number count
/// of milliseconds.
/// A raw value of `0` means "block forever" and is returned as `Ok(0)`
/// unchanged. Negative values and overflow are returned as `Err`.
/// PORT NOTE: The C signature is
/// `int getTimeoutFromObjectOrReply(client *c, robj *object, mstime_t *timeout, int unit)`
/// where `client *c` is used to call `addReplyError` inline on parse failure.
/// In Rust the function returns `Result`; the caller converts errors to client
/// replies via `ctx.reply_error(&err)` or the `?` operator. Error message
/// bytes are verbatim matches to the C strings for wire-diff compatibility.
pub fn get_timeout_from_object_or_reply(
    object: &RedisObject,
    unit: TimeUnit,
    now_ms: i64,
) -> RedisResult<i64> {
    let tval: i64 = match unit {
        TimeUnit::Seconds => {
 // "timeout is not a float or out of range")
            // TODO(port): replace stub with RedisObject::get_long_double() once
 // Is ported (redis-core::object).
 // PERF(port): C uses `long double` (80-bit extended on x86); Rust
 // uses f64 (64-bit). Sub-millisecond fractional-second precision
 // may differ slightly — profile in Phase B if SET PX Tcl tests diverge.
            let ftval: f64 = object_get_long_double(object)
                .map_err(|_| RedisError::runtime(b"timeout is not a float or out of range"))?;

            let ftval_ms = ftval * 1000.0_f64;

            if ftval_ms > i64::MAX as f64 {
                return Err(RedisError::runtime(b"timeout is out of range"));
            }

            ftval_ms.ceil() as i64
        }

        TimeUnit::Milliseconds => {
 // "timeout is not an integer or out of range")
            // TODO(port): replace stub with RedisObject::get_long_long() once
 // Is ported (redis-core::object).
            object_get_long_long(object)
                .map_err(|_| RedisError::runtime(b"timeout is not an integer or out of range"))?
        }
    };

    if tval < 0 {
        return Err(RedisError::runtime(b"timeout is negative"));
    }

 // now) → overflow; tval += now; }
    if tval > 0 {
        if tval > i64::MAX - now_ms {
 // C comment: 'tval+now' would overflow
            return Err(RedisError::runtime(b"timeout is out of range"));
        }
        return Ok(tval + now_ms);
    }

 // tval == 0: block forever — return 0 unchanged.
    Ok(0)
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Decode a `RedisObject` as a 64-bit floating-point number.
/// Stub — the real implementation belongs in `redis-core::object` as a method
/// on `RedisObject`.
/// TODO(port): replace with `object.get_long_double() -> RedisResult<f64>` once
/// object.rs is ported.
fn object_get_long_double(obj: &RedisObject) -> RedisResult<f64> {
    let _ = obj;
    Err(RedisError::runtime(
        b"object_get_long_double: not yet implemented",
    ))
}

/// Decode a `RedisObject` as a signed 64-bit integer.
/// Stub — the real implementation belongs in `redis-core::object` as a method
/// on `RedisObject`.
/// TODO(port): replace with `object.get_long_long() -> RedisResult<i64>` once
/// object.rs is ported.
fn object_get_long_long(obj: &RedisObject) -> RedisResult<i64> {
    let _ = obj;
    Err(RedisError::runtime(
        b"object_get_long_long: not yet implemented",
    ))
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         13
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         Key-encoding helpers fully translated; blocking-state
//                  accessors (Client) and RadixTree (RedisServer) are Phase 4/5
//                  stubs.  get_timeout_from_object_or_reply drops the client *c
//                  parameter in favour of Result return (see PORT NOTE).
// ──────────────────────────────────────────────────────────────────────────────
