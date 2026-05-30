//! Keyspace event notifications.
//! Translates database operations into Pub/Sub messages on two channel families:
//! - `__keyspace@<db>__:<key>` with the event name as the message payload
//! - `__keyevent@<db>__:<event>` with the key name as the message payload
//! Which families are active is controlled by `server.notify_keyspace_events`,
//! a bitmask of the `NOTIFY_*` constants defined here.
//! Reference: <https://valkey.io/topics/notifications>

use crate::client::Client;
use crate::object::RedisObject;
use crate::server::RedisServer;
use redis_types::error::RedisError;
use redis_types::string::RedisString;

// ── Notification-class flag constants ─────────────────────────────────────────

/// Keyspace-channel prefix enabled (`K` in config string).
pub const NOTIFY_KEYSPACE: i32 = 1 << 0;
/// Keyevent-channel prefix enabled (`E` in config string).
pub const NOTIFY_KEYEVENT: i32 = 1 << 1;
/// Generic command events: `DEL`, `EXPIRE`, `RENAME`, … (`g`).
pub const NOTIFY_GENERIC: i32 = 1 << 2;
/// String-command events (`$`).
pub const NOTIFY_STRING: i32 = 1 << 3;
/// List-command events (`l`).
pub const NOTIFY_LIST: i32 = 1 << 4;
/// Set-command events (`s`).
pub const NOTIFY_SET: i32 = 1 << 5;
/// Hash-command events (`h`).
pub const NOTIFY_HASH: i32 = 1 << 6;
/// Sorted-set-command events (`z`).
pub const NOTIFY_ZSET: i32 = 1 << 7;
/// Key-expiry events (`x`).
pub const NOTIFY_EXPIRED: i32 = 1 << 8;
/// Key-eviction events (`e`).
pub const NOTIFY_EVICTED: i32 = 1 << 9;
/// Stream-command events (`t`).
pub const NOTIFY_STREAM: i32 = 1 << 10;
/// Key-miss events — intentionally excluded from `NOTIFY_ALL` (`m`).
pub const NOTIFY_KEY_MISS: i32 = 1 << 11;
/// Module-only: key loaded from RDB snapshot (not user-configurable).
pub const NOTIFY_LOADED: i32 = 1 << 12;
/// Module keyspace-notification events (`d`).
pub const NOTIFY_MODULE: i32 = 1 << 13;
/// New-key notification events (`n`).
pub const NOTIFY_NEW: i32 = 1 << 14;

/// All standard notification classes combined.
/// The `A` shorthand in the config string maps to this mask. Intentionally
/// excludes `NOTIFY_KEY_MISS`, `NOTIFY_NEW`, and `NOTIFY_LOADED`.
pub const NOTIFY_ALL: i32 = NOTIFY_GENERIC
    | NOTIFY_STRING
    | NOTIFY_LIST
    | NOTIFY_SET
    | NOTIFY_HASH
    | NOTIFY_ZSET
    | NOTIFY_EXPIRED
    | NOTIFY_EVICTED
    | NOTIFY_STREAM
    | NOTIFY_MODULE;

// ── Public API ────────────────────────────────────────────────────────────────

/// Converts a notification-class config string (e.g. `b"KEA"`) to a bitmask
/// of `NOTIFY_*` flags.
/// Returns `Err` on the first byte that does not correspond to a known
/// notification-class character. The C implementation returned `-1` in that
/// case; this translation uses `Result` instead.
pub fn keyspace_events_string_to_flags(classes: &[u8]) -> Result<i32, RedisError> {
    let mut flags: i32 = 0;
    for &c in classes {
        match c {
            b'A' => flags |= NOTIFY_ALL,
            b'g' => flags |= NOTIFY_GENERIC,
            b'$' => flags |= NOTIFY_STRING,
            b'l' => flags |= NOTIFY_LIST,
            b's' => flags |= NOTIFY_SET,
            b'h' => flags |= NOTIFY_HASH,
            b'z' => flags |= NOTIFY_ZSET,
            b'x' => flags |= NOTIFY_EXPIRED,
            b'e' => flags |= NOTIFY_EVICTED,
            b'K' => flags |= NOTIFY_KEYSPACE,
            b'E' => flags |= NOTIFY_KEYEVENT,
            b't' => flags |= NOTIFY_STREAM,
            b'm' => flags |= NOTIFY_KEY_MISS,
            b'd' => flags |= NOTIFY_MODULE,
            b'n' => flags |= NOTIFY_NEW,
            _ => return Err(RedisError::syntax(b"invalid notification class character")),
        }
    }
    Ok(flags)
}

/// Converts a bitmask of `NOTIFY_*` flags back to the canonical config-string
/// representation.
/// The output is suitable for storage in `server.notify_keyspace_events`
/// for returning from `CONFIG GET notify-keyspace-events`. The returned
/// `RedisString` is owned by the caller.
pub fn keyspace_events_flags_to_string(flags: i32) -> RedisString {
    let mut res: Vec<u8> = Vec::with_capacity(16);

    if (flags & NOTIFY_ALL) == NOTIFY_ALL {
        res.push(b'A');
    } else {
        if flags & NOTIFY_GENERIC != 0 {
            res.push(b'g');
        }
        if flags & NOTIFY_STRING != 0 {
            res.push(b'$');
        }
        if flags & NOTIFY_LIST != 0 {
            res.push(b'l');
        }
        if flags & NOTIFY_SET != 0 {
            res.push(b's');
        }
        if flags & NOTIFY_HASH != 0 {
            res.push(b'h');
        }
        if flags & NOTIFY_ZSET != 0 {
            res.push(b'z');
        }
        if flags & NOTIFY_EXPIRED != 0 {
            res.push(b'x');
        }
        if flags & NOTIFY_EVICTED != 0 {
            res.push(b'e');
        }
        if flags & NOTIFY_STREAM != 0 {
            res.push(b't');
        }
        if flags & NOTIFY_MODULE != 0 {
            res.push(b'd');
        }
        if flags & NOTIFY_NEW != 0 {
            res.push(b'n');
        }
    }

    if flags & NOTIFY_KEYSPACE != 0 {
        res.push(b'K');
    }
    if flags & NOTIFY_KEYEVENT != 0 {
        res.push(b'E');
    }
    if flags & NOTIFY_KEY_MISS != 0 {
        res.push(b'm');
    }

    RedisString::from_bytes(&res)
}

/// Fires keyspace and/or keyevent Pub/Sub notifications for a single database
/// operation.
/// `event_type` is one or more `NOTIFY_*` flags OR'd together representing
/// class of the triggering command (e.g. `NOTIFY_STRING` for `SET`). `event`
/// is the raw event-name bytes (e.g. `b"set"`, `b"del"`). `key` is
/// `RedisObject` for the affected key. `dbid` is the zero-based database
/// index.
/// The module notification system is called first, before
/// `notify_keyspace_events` config gate, so modules always receive events they
/// subscribed to regardless of the server config.
/// PORT NOTE: The C implementation reads `server.executing_client` from a
/// process-global. This translation takes `executing_client` as an explicit
/// `Option<&mut Client>` parameter to avoid global mutable state.
/// TODO(architect): `notify_keyspace_events` must be added as a field on
/// `RedisServer` (or `ServerConfig`) and exposed via an accessor. The current
/// placeholder hard-codes `0` (no notifications), which causes the function
/// always return early after the module hook.
/// TODO(architect): `pubsub_publish_message` lives in `redis-commands/pubsub`;
/// a direct dep from `redis-core` → `redis-commands` would be circular.
/// Options: move the publish primitive into `redis-core`, or accept a
/// `fn(&RedisObject, &RedisObject) -> Result<, RedisError>` callback.
/// Escalate before wiring the actual publish calls.
/// TODO(architect): `module_notify_keyspace_event` is a Phase-10 stub; wire
/// it once `redis-modules` has a dep-edge into `redis-core`.
pub fn notify_keyspace_event(
    server: &mut RedisServer,
    executing_client: Option<&mut Client>,
    event_type: i32,
    event: &[u8],
    key: &RedisObject,
    dbid: i32,
) -> Result<(), RedisError> {
 // debugServerAssert(moduleNotifyKeyspaceSubscribersCnt == 0 ||...)
 // This assertion guards that keyspace notifications from buffered-reply
 // write commands are never sent before the reply is committed. The Rust
 // equivalent requires the module subscriber count and Client flag access;
 // deferred to Phase B.
    // TODO(port): restore the debugServerAssert equivalent once Client flags
 // (keyspace_notified, buffered_reply) and the module subscriber count are
 // accessible.

 // Notify module subscribers before the config gate. The module engine
 // filters by subscribed event types internally.
    // TODO(port): module_notify_keyspace_event — Phase 10 stub; no-op for now.

    if let Some(client) = executing_client {
 // c->flag.keyspace_notified = 1
        // TODO(port): expose `keyspace_notified` flag on `Client`.
        let _ = client;

 // commitDeferredReplyBuffer(c, 1)
        // TODO(port): commit_deferred_reply_buffer — networking call into
 // `redis-core/networking.rs`; wire once the signature is stable.
    }

 // Early exit if this event class is not enabled.
 // if (!(server.notify_keyspace_events & type)) return;
    // TODO(port): replace `notify_flags` placeholder with
 // `server.notify_keyspace_events` once that accessor exists.
    let notify_flags: i32 = 0; // placeholder — see TODO(architect) above
    let _ = server;
    if notify_flags & event_type == 0 {
        return Ok(());
    }

 // Render dbid as decimal ASCII bytes once; reused for both channel names.
 // PORT NOTE: `format!` here converts an integer to ASCII digits, not Redis
 // byte data; the UTF-8 ban on Redis data does not apply.
    let dbid_bytes = format!("{}", dbid).into_bytes();

 // ── __keyspace@<db>__:<key> <event> ──────────────────────────────────────
    if notify_flags & NOTIFY_KEYSPACE != 0 {
        let key_bytes = object_to_bytes(key)?;
        let mut chan: Vec<u8> = Vec::with_capacity(
            b"__keyspace@".len() + dbid_bytes.len() + b"__:".len() + key_bytes.len(),
        );
        chan.extend_from_slice(b"__keyspace@");
        chan.extend_from_slice(&dbid_bytes);
        chan.extend_from_slice(b"__:");
        chan.extend_from_slice(key_bytes);

        let chan_obj = RedisObject::new_string(&chan);
        let event_obj = RedisObject::new_string(event);

        // TODO(port): pubsub_publish_message(&chan_obj, &event_obj, false)?;
        // Blocked on TODO(architect) dep-edge decision above.
        let _ = (chan_obj, event_obj);
    }

 // ── __keyevent@<db>__:<event> <key> ──────────────────────────────────────
    if notify_flags & NOTIFY_KEYEVENT != 0 {
        let mut chan: Vec<u8> = Vec::with_capacity(
            b"__keyevent@".len() + dbid_bytes.len() + b"__:".len() + event.len(),
        );
        chan.extend_from_slice(b"__keyevent@");
        chan.extend_from_slice(&dbid_bytes);
        chan.extend_from_slice(b"__:");
        chan.extend_from_slice(event);

        let chan_obj = RedisObject::new_string(&chan);

 // message payload is the key.
        // TODO(port): pubsub_publish_message(&chan_obj, key, false)?;
        // Blocked on TODO(architect) dep-edge decision above.
        let _ = chan_obj;
    }

    Ok(())
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Extracts the underlying byte slice from a `RedisObject::String`.
/// Returns `Err(RedisError::wrong_type` if `obj` is not a string variant.
/// In C, `objectGetVal(obj)` performs an unchecked cast of `obj->ptr`
/// `sds`; this helper adds an explicit type check.
fn object_to_bytes(obj: &RedisObject) -> Result<&[u8], RedisError> {
    obj.as_string_bytes().ok_or_else(RedisError::wrong_type)
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         10
//   port_notes:    2
//   unsafe_blocks: 0
//   notes:         All three functions ported faithfully. The pubsub publish
//                  calls and module-notify hook are stubbed pending architect
//                  decisions on the circular dep-edge (redis-core →
//                  redis-commands) and Phase-10 module API.  The
//                  notify_keyspace_events config accessor is a placeholder
//                  (hard-coded 0) until RedisServer gains that field.
// ──────────────────────────────────────────────────────────────────────────────
