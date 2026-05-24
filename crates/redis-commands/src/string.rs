//! String command implementations: SET, GET, INCR, APPEND, LCS, and friends.
//!
//! C source: `reference/valkey/src/t_string.c`  (1 056 lines, 29 functions)
//! Crate: `redis-commands`  (pilot phase)
//!
//! All Redis data — keys, values, RESP payloads — uses `RedisString` /
//! `&[u8]`.  `String` / `&str` / `from_utf8` are banned for stored Redis data
//! per PORTING.md §1.  Transient number-parsing is the sole usage exception;
//! see `parse_float_from_object` and the `PORT NOTE` there.
//!
//! Commands follow PORTING.md §4.1:
//!   `void fooCommand(client *c)` →
//!   `pub fn foo_command(ctx: &mut CommandContext) -> Result<(), RedisError>`
//!
//! ## Architect items (Phase 3 / Phase 4)
//!
//! TODO(architect): `CommandContext::db()` / `db_mut()` — needs `&mut RedisServer`
//! added to `CommandContext` in Phase 3 (redis-core architect packet).
//!
//! TODO(architect): `CommandContext::server_dirty_incr()` — increment server dirty
//! counter; blocked on Phase 3 RedisServer access.
//!
//! TODO(architect): `CommandContext::notify_keyspace_event(event_type, event, key)`
//! — keyspace event dispatch; blocked on Phase 3.
//!
//! TODO(architect): `CommandContext::signal_modified_key(key)` — WATCH and
//! client-tracking invalidation; blocked on Phase 3.
//!
//! TODO(architect): TTL management (`set_expire`, `remove_expire`,
//! `check_already_expired`) — blocked on Phase 3 expiry layer.
//!
//! TODO(architect): Replication command rewriting (`rewrite_client_command_vector`,
//! `rewrite_client_command_argument`, `replace_client_command_vector`) — blocked
//! on Phase 3+ replication layer.
//!
//! TODO(architect): `CommandContext::command_time_snapshot() -> i64` — cached
//! timestamp set at command-dispatch time; currently falls back to SystemTime.
//!
//! TODO(architect): `CommandContext::proto_max_bulk_len() -> i64` — server config
//! accessor; currently hard-coded to Valkey default 512 MiB.
//!
//! TODO(architect): `CommandContext::reply_map_header(n)` — RESP3 map header;
//! needed by LCS IDX mode.
//!
//! TODO(architect): `CommandContext::reply_deferred_len()` /
//! `set_deferred_len()` — deferred array-length protocol; needed by LCS IDX mode.
//!
//! TODO(architect): `CommandContext::must_obey_client() -> bool` — master-client
//! bypass for `check_string_length`.

use redis_core::command_context::CommandContext;
use redis_core::db::LOOKUP_NONE;
use redis_core::notify::{NOTIFY_GENERIC, NOTIFY_STRING};
use redis_core::object::{ObjectKind, RedisObject};
use redis_protocol::frame::RespFrame;
use redis_types::{RedisError, RedisString};
use std::io::Write;

// ── SET / GETEX / MSETEX flag bits  (C: ARGS_* in server.h) ─────────────
// PORT NOTE: bit positions are local to this port; the C constants live in
// server.h with their own values.  Only the bit semantics need to match.
pub const SET_FLAG_NONE: u32 = 0;
pub const SET_FLAG_NX: u32 = 1 << 0; // NX — only set if key absent
pub const SET_FLAG_XX: u32 = 1 << 1; // XX — only set if key present
pub const SET_FLAG_GET: u32 = 1 << 2; // GET — return old value
pub const SET_FLAG_EX: u32 = 1 << 3; // EX  — relative seconds
pub const SET_FLAG_PX: u32 = 1 << 4; // PX  — relative milliseconds
pub const SET_FLAG_EXAT: u32 = 1 << 5; // EXAT — absolute Unix seconds
pub const SET_FLAG_PXAT: u32 = 1 << 6; // PXAT — absolute Unix milliseconds
pub const SET_FLAG_KEEPTTL: u32 = 1 << 7; // KEEPTTL — preserve existing TTL
pub const SET_FLAG_ARGV3: u32 = 1 << 8; // internal: value sits at argv[3]
pub const SET_FLAG_IFEQ: u32 = 1 << 9; // IFEQ — only set if current == comparison
pub const SET_FLAG_PERSIST: u32 = 1 << 10; // PERSIST — remove TTL (GETEX only)

// ── setKey() hint bits  (C: SETKEY_* in server.h) ────────────────────────
pub const SETKEY_KEEPTTL: u32 = 1 << 0;
pub const SETKEY_DOESNT_EXIST: u32 = 1 << 1;
pub const SETKEY_ALREADY_EXIST: u32 = 1 << 2;
pub const SETKEY_ADD_OR_UPDATE: u32 = 1 << 3;

/// Default maximum length of a single bulk string in bytes (512 MiB).
///
/// Matches Valkey's `PROTO_MAX_BULK_LEN_DEFAULT` (server.h). Commands that
/// would grow a stored string beyond this limit must reject with the
/// canonical `ERR string exceeds maximum allowed size` reply. Until the
/// `CommandContext` exposes a server-config accessor for the runtime value,
/// this hard-coded constant gates SETRANGE/APPEND.
pub const PROTO_MAX_BULK_LEN_DEFAULT: usize = 512 * 1024 * 1024;

/// Expiry-time unit for SET / GETEX / MSETEX.
///
/// C: `UNIT_SECONDS` = 0, `UNIT_MILLISECONDS` = 1 (server.h).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Seconds,
    Milliseconds,
}

/// Discriminator for `parse_extended_command_args` — controls which optional
/// flags are legal.
///
/// C: `COMMAND_SET` / `COMMAND_GET` / `COMMAND_MSET` enum values passed to
/// `parseExtendedCommandArgumentsOrReply` in server.c.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    /// SET: accepts NX, XX, IFEQ, GET, EX, PX, EXAT, PXAT, KEEPTTL.
    Set,
    /// GETEX: accepts PERSIST, EX, PX, EXAT, PXAT.
    Get,
    /// MSETEX: accepts NX, XX, EX, PX, EXAT, PXAT, KEEPTTL.
    Mset,
}

// ──────────────────────────────────────────────────────────────────────────
// Private helpers
// ──────────────────────────────────────────────────────────────────────────

/// Reject if `size + append` would exceed the server's proto-max-bulk-len.
///
/// C: `checkStringLength` (t_string.c:45).
fn check_string_length(
    _ctx: &mut CommandContext,
    size: i64,
    append: i64,
) -> Result<(), RedisError> {
    todo!("C: t_string.c:45, checkStringLength")
}

/// Convert a raw expire argument (string of digits) to absolute milliseconds
/// since epoch, applying unit scaling and optional timestamp addition.
///
/// C: `getExpireMillisecondsOrReply` (t_string.c:221).
fn get_expire_milliseconds(
    expire_val: &RedisString,
    flags: u32,
    unit: Unit,
) -> Result<i64, RedisError> {
    todo!("C: t_string.c:221, getExpireMillisecondsOrReply")
}

/// Parse optional command arguments for SET / GETEX / MSETEX.
///
/// Iterates `ctx.arg(start_idx..argc)`, setting bits in `flags`, updating
/// `unit`, and capturing the expire and comparison values.  Unknown tokens
/// yield `RedisError::syntax`.
///
/// C: `parseExtendedCommandArgumentsOrReply` (server.c — not visible here).
fn parse_extended_command_args(
    ctx: &CommandContext,
    kind: CommandKind,
    start_idx: usize,
    flags: &mut u32,
    unit: &mut Unit,
    expire: &mut Option<RedisString>,
    comparison: &mut Option<RedisString>,
) -> Result<(), RedisError> {
    todo!("C: parseExtendedCommandArgumentsOrReply (server.c)")
}

/// Parse a decimal integer from the raw bytes of a `RedisString`.
///
/// C: `getLongLongFromObjectOrReply` (partial) — error-reply path is
/// replaced by returning `Err(RedisError::not_integer())`.
fn parse_integer(s: &RedisString) -> Result<i64, RedisError> {
    todo!("C: getLongLongFromObjectOrReply — integer parse")
}

/// Parse an integer from a `RedisObject::String`.
///
/// C: `getLongLongFromObjectOrReply`.
fn parse_integer_from_object(obj: &RedisObject) -> Result<i64, RedisError> {
    todo!("C: getLongLongFromObjectOrReply")
}

/// Parse a float from a `RedisObject::String`.
///
/// C: `getLongDoubleFromObjectOrReply`.
/// PERF(port): C uses 80-bit `long double` on x86; Rust uses `f64` (64-bit).
/// Results may diverge at the precision boundary.
fn parse_float_from_object(obj: &RedisObject) -> Result<f64, RedisError> {
    todo!("C: getLongDoubleFromObjectOrReply")
}

/// Return the byte slice of a `RedisObject::String`, or `None` if wrong type.
///
/// C: `objectGetVal(o)` when encoding is raw/embstr.
fn object_as_bytes(obj: &RedisObject) -> Option<&[u8]> {
    obj.as_string_bytes()
}

/// Return the length of a string object's value.
///
/// C: `stringObjectLen(o)`.
fn string_object_len(obj: &RedisObject) -> usize {
    object_as_bytes(obj).map_or(0, |b| b.len())
}

/// Format an `i64` as its decimal ASCII bytes.
///
/// C: `ll2string` / `createStringObjectFromLongLong`.
fn long_long_to_bytes(n: i64) -> Vec<u8> {
    let mut buf = Vec::new();
    write!(buf, "{}", n).ok();
    buf
}

/// Format an `f64` as decimal ASCII bytes.
///
/// TODO(port): C `ld2string` with `humanfriendly=1` trims trailing zeros
/// and uses 17 significant digits for round-trip fidelity.  The Rust
/// `{}` formatter may produce different representations for some values;
/// verify against wire-diff oracle in Phase C.
fn double_to_bytes(v: f64) -> Vec<u8> {
    let mut buf = Vec::new();
    write!(buf, "{}", v).ok();
    buf
}

// ──────────────────────────────────────────────────────────────────────────
// Generic / shared command logic
// ──────────────────────────────────────────────────────────────────────────

/// Core implementation shared by SET, SETNX, SETEX, PSETEX, GETSET.
///
/// C: `setGenericCommand` (t_string.c:76).
pub fn set_generic_command(
    ctx: &mut CommandContext,
    flags: u32,
    key: &RedisString,
    val: RedisString,
    expire_val: Option<&RedisString>,
    unit: Unit,
    ok_reply: Option<&[u8]>,
    abort_reply: Option<&[u8]>,
    comparison: Option<&RedisString>,
) -> Result<(), RedisError> {
    todo!("C: t_string.c:76, setGenericCommand")
}

/// Shared MSET / MSETNX logic.
///
/// C: `msetGenericCommand` (t_string.c:548).
fn mset_generic_command(ctx: &mut CommandContext, nx: bool) -> Result<(), RedisError> {
    todo!("C: t_string.c:548, msetGenericCommand")
}

/// Shared INCR / DECR / INCRBY / DECRBY logic.
///
/// C: `incrDecrCommand` (t_string.c:697).
fn incr_decr_command(ctx: &mut CommandContext, incr: i64) -> Result<(), RedisError> {
    todo!("C: t_string.c:697, incrDecrCommand")
}

/// Read-path GET used by SET+GET, GETDEL, GETSET.
///
/// Replies with the current string value (or null) and returns
/// `Ok(true)` if the key existed and was a string, `Ok(false)` if absent,
/// `Err(WrongType)` on type mismatch.
///
/// C: `getGenericCommand` (t_string.c:302) — returns C_OK / C_ERR.
pub(crate) fn get_generic_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    todo!("C: t_string.c:302, getGenericCommand")
}

// ──────────────────────────────────────────────────────────────────────────
// Public command entry points
// ──────────────────────────────────────────────────────────────────────────

/// SET key value [NX|XX] [GET]
///     [EX s | PX ms | EXAT ts | PXAT ms-ts | KEEPTTL]
///
/// C: `setCommand` (t_string.c:251).
pub fn set_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"set"));
    }
    let key = ctx.arg_owned(1usize)?;
    let value = ctx.arg_owned(2usize)?;

    let mut flags: u32 = 0;
    let mut expire_at_ms: Option<i64> = None;
    let mut comparison: Option<RedisString> = None;
    let mut j = 3usize;
    while j < argc {
        let opt = ctx.arg_owned(j)?;
        let opt_bytes = opt.as_bytes();
        if opt_bytes.eq_ignore_ascii_case(b"NX") {
            if flags & (SET_FLAG_XX | SET_FLAG_IFEQ) != 0 {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            flags |= SET_FLAG_NX;
            j += 1;
        } else if opt_bytes.eq_ignore_ascii_case(b"XX") {
            if flags & (SET_FLAG_NX | SET_FLAG_IFEQ) != 0 {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            flags |= SET_FLAG_XX;
            j += 1;
        } else if opt_bytes.eq_ignore_ascii_case(b"IFEQ") {
            if flags & (SET_FLAG_NX | SET_FLAG_XX | SET_FLAG_IFEQ) != 0 || j + 1 >= argc {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            flags |= SET_FLAG_IFEQ;
            comparison = Some(ctx.arg_owned(j + 1)?);
            j += 2;
        } else if opt_bytes.eq_ignore_ascii_case(b"GET") {
            flags |= SET_FLAG_GET;
            j += 1;
        } else if opt_bytes.eq_ignore_ascii_case(b"KEEPTTL") {
            if flags & (SET_FLAG_EX | SET_FLAG_PX | SET_FLAG_EXAT | SET_FLAG_PXAT) != 0 {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            flags |= SET_FLAG_KEEPTTL;
            j += 1;
        } else if opt_bytes.eq_ignore_ascii_case(b"EX")
            || opt_bytes.eq_ignore_ascii_case(b"PX")
            || opt_bytes.eq_ignore_ascii_case(b"EXAT")
            || opt_bytes.eq_ignore_ascii_case(b"PXAT")
        {
            if flags
                & (SET_FLAG_EX | SET_FLAG_PX | SET_FLAG_EXAT | SET_FLAG_PXAT | SET_FLAG_KEEPTTL)
                != 0
            {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            if j + 1 >= argc {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            let bit = if opt_bytes.eq_ignore_ascii_case(b"EX") {
                SET_FLAG_EX
            } else if opt_bytes.eq_ignore_ascii_case(b"PX") {
                SET_FLAG_PX
            } else if opt_bytes.eq_ignore_ascii_case(b"EXAT") {
                SET_FLAG_EXAT
            } else {
                SET_FLAG_PXAT
            };
            flags |= bit;
            let value_arg = ctx.arg_owned(j + 1)?;
            let raw = parse_strict_i64(value_arg.as_bytes()).ok_or_else(|| {
                RedisError::runtime(b"ERR value is not an integer or out of range")
            })?;
            if raw <= 0 && (bit == SET_FLAG_EX || bit == SET_FLAG_PX) {
                return Err(RedisError::runtime(
                    b"ERR invalid expire time in 'set' command",
                ));
            }
            if raw < 0 {
                return Err(RedisError::runtime(
                    b"ERR invalid expire time in 'set' command",
                ));
            }
            let now_ms: i64 = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let abs_ms: i64 = match bit {
                b if b == SET_FLAG_EX => raw
                    .checked_mul(1000)
                    .and_then(|v| v.checked_add(now_ms))
                    .ok_or_else(|| {
                        RedisError::runtime(b"ERR invalid expire time in 'set' command")
                    })?,
                b if b == SET_FLAG_PX => raw.checked_add(now_ms).ok_or_else(|| {
                    RedisError::runtime(b"ERR invalid expire time in 'set' command")
                })?,
                b if b == SET_FLAG_EXAT => raw.checked_mul(1000).ok_or_else(|| {
                    RedisError::runtime(b"ERR invalid expire time in 'set' command")
                })?,
                _ => raw,
            };
            expire_at_ms = Some(abs_ms);
            j += 2;
        } else {
            return Err(RedisError::runtime(b"ERR syntax error"));
        }
    }

    let needs_current_value = flags & (SET_FLAG_GET | SET_FLAG_IFEQ) != 0;
    let (key_exists, prev_bytes): (bool, Option<Vec<u8>>) =
        match ctx.db_mut().lookup_key_write(&key) {
            None => (false, None),
            Some(obj) => {
                if needs_current_value {
                    match &obj.kind {
                        ObjectKind::String(_) => (true, Some(obj.string_bytes_owned())),
                        _ => return Err(RedisError::wrong_type()),
                    }
                } else {
                    (true, None)
                }
            }
        };
    if flags & SET_FLAG_IFEQ != 0 {
        let Some(compare) = comparison.as_ref() else {
            return Err(RedisError::runtime(b"ERR syntax error"));
        };
        if !prev_bytes
            .as_ref()
            .is_some_and(|bytes| bytes.as_slice() == compare.as_bytes())
        {
            if flags & SET_FLAG_GET != 0 {
                return reply_optional_bulk(ctx, prev_bytes);
            }
            return ctx.reply_null_bulk();
        }
    }
    if flags & SET_FLAG_NX != 0 && key_exists {
        if flags & SET_FLAG_GET != 0 {
            return reply_optional_bulk(ctx, prev_bytes);
        }
        return ctx.reply_null_bulk();
    }
    if flags & SET_FLAG_XX != 0 && !key_exists {
        if flags & SET_FLAG_GET != 0 {
            return reply_optional_bulk(ctx, prev_bytes);
        }
        return ctx.reply_null_bulk();
    }

    let setkey_flags = if flags & SET_FLAG_KEEPTTL != 0 {
        SETKEY_KEEPTTL
    } else {
        0
    };
    let obj = RedisObject::new_string_try_encoded(value.as_bytes());
    let notify = ctx.keyspace_notifications_enabled(NOTIFY_STRING);
    match expire_at_ms {
        Some(abs_ms) => {
            ctx.db_mut().set_key(key.clone(), obj, setkey_flags);
            if notify {
                ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &key);
            }
            let now_ms: i64 = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            if abs_ms <= now_ms {
                ctx.db_mut().sync_delete(&key);
            } else {
                ctx.db_mut().set_expire(&key, abs_ms);
            }
        }
        None if notify => {
            ctx.db_mut().set_key(key.clone(), obj, setkey_flags);
            ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &key);
        }
        None => {
            ctx.db_mut().set_key(key, obj, setkey_flags);
        }
    }

    if flags & SET_FLAG_GET != 0 {
        reply_optional_bulk(ctx, prev_bytes)
    } else {
        ctx.reply_simple_string(b"OK")
    }
}

fn reply_optional_bulk(ctx: &mut CommandContext, bytes: Option<Vec<u8>>) -> Result<(), RedisError> {
    match bytes {
        None => ctx.reply_null_bulk(),
        Some(bytes) => ctx.reply_bulk_string(RedisString::from_bytes(&bytes)),
    }
}

/// SETNX key value
///
/// Sets `key` to `value` only if `key` is not yet present. Replies `:1`
/// when applied, `:0` when the key already existed.
///
/// C: `setnxCommand` (t_string.c:267).
pub fn setnx_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"setnx"));
    }
    let key = ctx.arg_owned(1usize)?;
    let value = ctx.arg_owned(2usize)?;
    if ctx.db_mut().lookup_key_write(&key).is_some() {
        return ctx.reply_integer(0);
    }
    let obj = RedisObject::new_string_try_encoded(value.as_bytes());
    if ctx.keyspace_notifications_enabled(NOTIFY_STRING) {
        ctx.db_mut().set_key(key.clone(), obj, 0);
        ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &key);
    } else {
        ctx.db_mut().set_key(key, obj, 0);
    }
    ctx.reply_integer(1)
}

/// SETEX key seconds value
///
/// Atomically set `key` to `value` with an absolute expire of `seconds`
/// seconds from now. Equivalent to `SET key value EX seconds`. Replies
/// `+OK\r\n`.
///
/// C: `setexCommand` (t_string.c:272).
pub fn setex_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    setex_generic(ctx, b"setex", 1000)
}

/// PSETEX key milliseconds value
///
/// Same as SETEX but with millisecond resolution. Replies `+OK\r\n`.
///
/// C: `psetexCommand` (t_string.c:277).
pub fn psetex_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    setex_generic(ctx, b"psetex", 1)
}

/// Shared SETEX / PSETEX logic. `multiplier` converts the user-supplied
/// expire amount into milliseconds.
fn setex_generic(ctx: &mut CommandContext, name: &[u8], multiplier: i64) -> Result<(), RedisError> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(name));
    }
    let key = ctx.arg_owned(1usize)?;
    let secs_arg = ctx.arg_owned(2usize)?;
    let value = ctx.arg_owned(3usize)?;
    let raw = parse_strict_i64(secs_arg.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
    if raw <= 0 {
        let mut buf = Vec::with_capacity(b"ERR invalid expire time in '".len() + name.len() + 2);
        buf.extend_from_slice(b"ERR invalid expire time in '");
        buf.extend_from_slice(name);
        buf.extend_from_slice(b"' command");
        return Err(RedisError::runtime(buf));
    }
    let now_ms: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let abs_ms = raw
        .checked_mul(multiplier)
        .and_then(|v| v.checked_add(now_ms))
        .ok_or_else(|| {
            let mut buf =
                Vec::with_capacity(b"ERR invalid expire time in '".len() + name.len() + 2);
            buf.extend_from_slice(b"ERR invalid expire time in '");
            buf.extend_from_slice(name);
            buf.extend_from_slice(b"' command");
            RedisError::runtime(buf)
        })?;
    let obj = RedisObject::new_string_try_encoded(value.as_bytes());
    ctx.db_mut().set_key(key.clone(), obj, 0);
    ctx.db_mut().set_expire(&key, abs_ms);
    ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &key);
    ctx.reply_simple_string(b"OK")
}

/// DELIFEQ key value — delete `key` only if its current value equals `value`.
///
/// Replies `:1` when the key existed, matched, and was deleted; `:0` when the
/// key was absent or its current value differs. WRONGTYPE if the key holds a
/// non-string value.
///
/// C: `delifeqCommand` (t_string.c:283).
pub fn delifeq_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"delifeq"));
    }
    let key = ctx.arg_owned(1usize)?;
    let cmp = ctx.arg_owned(2usize)?;
    let matched = match ctx.db_mut().lookup_key_write(&key) {
        None => false,
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => obj.string_bytes_owned() == cmp.as_bytes(),
            _ => return Err(RedisError::wrong_type()),
        },
    };
    if matched {
        ctx.db_mut().sync_delete(&key);
        ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
        ctx.reply_integer(1)
    } else {
        ctx.reply_integer(0)
    }
}

/// GET key
///
/// Replies with the key's bulk-string value, the null bulk `$-1\r\n` if the
/// key is absent, or `WRONGTYPE` if the key is not a string.
///
/// C: `getCommand` (t_string.c:316).
pub fn get_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"get"));
    }
    let key = ctx.arg_owned(1usize)?;
    let bytes: Option<Vec<u8>> = match ctx.db_mut().lookup_key_read_with_flags(&key, LOOKUP_NONE) {
        None => None,
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => Some(obj.string_bytes_owned()),
            _ => return Err(RedisError::wrong_type()),
        },
    };
    match bytes {
        None => ctx.reply_null_bulk(),
        Some(b) => ctx.reply_bulk_string(RedisString::from_bytes(&b)),
    }
}

/// GETEX key [PERSIST|EX s|PX ms|EXAT ts|PXAT ms-ts]
///
/// Returns the current bulk-string value of `key` and optionally updates its
/// TTL. With no extra option behaves like GET. With `PERSIST` removes any
/// expire on the key. `EX|PX|EXAT|PXAT` set an absolute expire in seconds or
/// milliseconds (relative or unix epoch).
///
/// C: `getexCommand` (t_string.c:340).
pub fn getex_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    if argc < 2 || argc > 4 {
        return Err(RedisError::wrong_number_of_args(b"getex"));
    }
    let key = ctx.arg_owned(1usize)?;
    let mut persist = false;
    let mut expire_at_ms: Option<i64> = None;
    let mut remove_expire = false;
    let now_ms: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    if argc >= 3 {
        let opt = ctx.arg_owned(2usize)?;
        let ob = opt.as_bytes();
        if ob.eq_ignore_ascii_case(b"PERSIST") {
            if argc != 3 {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            persist = true;
            remove_expire = true;
        } else if ob.eq_ignore_ascii_case(b"EX")
            || ob.eq_ignore_ascii_case(b"PX")
            || ob.eq_ignore_ascii_case(b"EXAT")
            || ob.eq_ignore_ascii_case(b"PXAT")
        {
            if argc != 4 {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            let val_arg = ctx.arg_owned(3usize)?;
            let raw = parse_strict_i64(val_arg.as_bytes()).ok_or_else(|| {
                RedisError::runtime(b"ERR value is not an integer or out of range")
            })?;
            let abs_ms = if ob.eq_ignore_ascii_case(b"EX") {
                if raw <= 0 {
                    return Err(RedisError::runtime(
                        b"ERR invalid expire time in 'getex' command",
                    ));
                }
                raw.checked_mul(1000).and_then(|v| v.checked_add(now_ms))
            } else if ob.eq_ignore_ascii_case(b"PX") {
                if raw <= 0 {
                    return Err(RedisError::runtime(
                        b"ERR invalid expire time in 'getex' command",
                    ));
                }
                raw.checked_add(now_ms)
            } else if ob.eq_ignore_ascii_case(b"EXAT") {
                raw.checked_mul(1000)
            } else {
                Some(raw)
            }
            .ok_or_else(|| RedisError::runtime(b"ERR invalid expire time in 'getex' command"))?;
            expire_at_ms = Some(abs_ms);
        } else {
            return Err(RedisError::runtime(b"ERR syntax error"));
        }
    }
    let bytes: Option<Vec<u8>> = match ctx.db_mut().lookup_key_write(&key) {
        None => None,
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => Some(obj.string_bytes_owned()),
            _ => return Err(RedisError::wrong_type()),
        },
    };
    let _ = persist;
    if let Some(b) = bytes {
        if remove_expire {
            if ctx.db_mut().remove_expire(&key) {
                ctx.client_mut()
                    .set_args(vec![RedisString::from_bytes(b"PERSIST"), key.clone()]);
            }
        } else if let Some(abs_ms) = expire_at_ms {
            if abs_ms <= now_ms {
                if ctx.db_mut().sync_delete(&key) {
                    ctx.client_mut()
                        .set_args(vec![RedisString::from_bytes(b"DEL"), key.clone()]);
                }
            } else {
                ctx.db_mut().set_expire(&key, abs_ms);
                ctx.client_mut().set_args(vec![
                    RedisString::from_bytes(b"PEXPIREAT"),
                    key.clone(),
                    RedisString::from_vec(abs_ms.to_string().into_bytes()),
                ]);
            }
        }
        ctx.reply_bulk_string(RedisString::from_bytes(&b))
    } else {
        ctx.reply_null_bulk()
    }
}

/// GETDEL key — atomic get-then-delete.
///
/// Replies with the bulk-string value held by `key` and deletes the key.
/// A missing key returns a null bulk; a non-string key yields WRONGTYPE.
///
/// C: `getdelCommand` (t_string.c:395).
pub fn getdel_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"getdel"));
    }
    let key = ctx.arg_owned(1usize)?;
    let bytes: Option<Vec<u8>> = match ctx.db_mut().lookup_key_write(&key) {
        None => None,
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => Some(obj.string_bytes_owned()),
            _ => return Err(RedisError::wrong_type()),
        },
    };
    match bytes {
        None => ctx.reply_null_bulk(),
        Some(b) => {
            ctx.db_mut().sync_delete(&key);
            ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &key);
            ctx.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
            ctx.reply_bulk_string(RedisString::from_bytes(&b))
        }
    }
}

/// GETSET key value — atomic get-and-set (deprecated; use SET … GET instead).
///
/// Returns the previous bulk-string value at `key` and stores `value`. A
/// missing key returns a null bulk and is created. A non-string previous
/// value yields WRONGTYPE without modifying the keyspace.
///
/// C: `getsetCommand` (t_string.c:408).
pub fn getset_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"getset"));
    }
    let key = ctx.arg_owned(1usize)?;
    let value = ctx.arg_owned(2usize)?;
    let prev: Option<Vec<u8>> = match ctx.db_mut().lookup_key_write(&key) {
        None => None,
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => Some(obj.string_bytes_owned()),
            _ => return Err(RedisError::wrong_type()),
        },
    };
    let obj = RedisObject::new_string_try_encoded(value.as_bytes());
    ctx.db_mut().set_key(key.clone(), obj, 0);
    ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &key);
    match prev {
        None => ctx.reply_null_bulk(),
        Some(b) => ctx.reply_bulk_string(RedisString::from_bytes(&b)),
    }
}

/// SETRANGE key offset value
///
/// Overwrites the string at `key` starting at `offset`, zero-padding when
/// the offset extends past the current length. Replies with the resulting
/// string length. If the key is missing and `value` is empty, the key is
/// not created and `:0` is returned. A non-string `key` yields WRONGTYPE.
/// Rejects with the `proto-max-bulk-len` size error when the resulting
/// length would exceed `PROTO_MAX_BULK_LEN_DEFAULT` (512 MiB).
///
/// C: `setrangeCommand` (t_string.c:432).
pub fn setrange_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"setrange"));
    }
    let key = ctx.arg_owned(1usize)?;
    let offset_raw = ctx.arg_owned(2usize)?;
    let value = ctx.arg_owned(3usize)?;
    let offset = parse_strict_i64(offset_raw.as_bytes()).ok_or_else(RedisError::not_integer)?;
    if offset < 0 {
        return Err(RedisError::runtime(b"ERR offset is out of range"));
    }
    let offset = offset as usize;
    let value_bytes = value.as_bytes();
    if !value_bytes.is_empty() {
        let needed = offset.saturating_add(value_bytes.len());
        if needed > PROTO_MAX_BULK_LEN_DEFAULT {
            return Err(RedisError::runtime(
                b"ERR string exceeds maximum allowed size (proto-max-bulk-len)",
            ));
        }
    }
    let existing: Option<Vec<u8>> = match ctx.db_mut().lookup_key_write(&key) {
        None => None,
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => Some(obj.string_bytes_owned()),
            _ => return Err(RedisError::wrong_type()),
        },
    };
    match existing {
        None => {
            if value_bytes.is_empty() {
                return ctx.reply_integer(0);
            }
            let total = offset + value_bytes.len();
            let mut buf = vec![0u8; total];
            buf[offset..offset + value_bytes.len()].copy_from_slice(value_bytes);
            let obj = RedisObject::new_raw_string(&buf);
            ctx.db_mut().set_key(key.clone(), obj, 0);
            ctx.notify_keyspace_event(NOTIFY_STRING, b"setrange", &key);
            ctx.reply_integer(total as i64)
        }
        Some(mut buf) => {
            if value_bytes.is_empty() {
                return ctx.reply_integer(buf.len() as i64);
            }
            let needed = offset + value_bytes.len();
            if buf.len() < needed {
                buf.resize(needed, 0);
            }
            buf[offset..offset + value_bytes.len()].copy_from_slice(value_bytes);
            let new_len = buf.len() as i64;
            let stored = RedisObject::new_raw_string(&buf);
            ctx.db_mut().set_key(key.clone(), stored, SETKEY_KEEPTTL);
            ctx.notify_keyspace_event(NOTIFY_STRING, b"setrange", &key);
            ctx.reply_integer(new_len)
        }
    }
}

/// GETRANGE key start end  (also aliased as SUBSTR)
///
/// Returns the substring of the string value at `key` bounded by the
/// inclusive byte indices `start` and `end`. Negative indices count from
/// the end of the string. Missing keys reply with an empty bulk string;
/// non-string values yield WRONGTYPE.
///
/// C: `getrangeCommand` (t_string.c:489).
pub fn getrange_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 4 {
        return Err(RedisError::wrong_number_of_args(b"getrange"));
    }
    let key = ctx.arg_owned(1usize)?;
    let start_raw = ctx.arg_owned(2usize)?;
    let end_raw = ctx.arg_owned(3usize)?;
    let start = parse_strict_i64(start_raw.as_bytes()).ok_or_else(RedisError::not_integer)?;
    let end = parse_strict_i64(end_raw.as_bytes()).ok_or_else(RedisError::not_integer)?;
    let bytes: Vec<u8> = match ctx.db_mut().lookup_key_read_with_flags(&key, LOOKUP_NONE) {
        None => return ctx.reply_bulk_string(RedisString::from_bytes(b"")),
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => obj.string_bytes_owned(),
            _ => return Err(RedisError::wrong_type()),
        },
    };
    let len = bytes.len() as i64;
    if len == 0 {
        return ctx.reply_bulk_string(RedisString::from_bytes(b""));
    }
    let mut s = if start < 0 { start + len } else { start };
    let mut e = if end < 0 { end + len } else { end };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        e = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || s >= len {
        return ctx.reply_bulk_string(RedisString::from_bytes(b""));
    }
    let slice = &bytes[s as usize..=e as usize];
    ctx.reply_bulk_string(RedisString::from_bytes(slice))
}

/// MGET key [key …]
///
/// Replies with an array whose elements are each either the string value
/// at the corresponding key or null when the key is missing or holds a
/// non-string value.
///
/// C: `mgetCommand` (t_string.c:530).
pub fn mget_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"mget"));
    }
    let mut keys: Vec<RedisString> = Vec::with_capacity(argc - 1);
    for j in 1..argc {
        keys.push(ctx.arg_owned(j)?);
    }
    let mut values: Vec<Option<Vec<u8>>> = Vec::with_capacity(keys.len());
    for key in &keys {
        let bytes: Option<Vec<u8>> = match ctx.db_mut().lookup_key_read_with_flags(key, LOOKUP_NONE)
        {
            None => None,
            Some(obj) => match &obj.kind {
                ObjectKind::String(_) => Some(obj.string_bytes_owned()),
                _ => None,
            },
        };
        values.push(bytes);
    }
    ctx.reply_array_header(values.len())?;
    for v in values {
        match v {
            None => ctx.reply_null_bulk()?,
            Some(b) => ctx.reply_bulk_string(RedisString::from_bytes(&b))?,
        }
    }
    Ok(())
}

/// MSET key value [key value …]
///
/// Atomically sets every key-value pair. Always replies `+OK`. An odd
/// number of key/value tokens yields a syntax error.
///
/// C: `msetCommand` (t_string.c:592).
pub fn mset_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    if argc < 3 || argc % 2 == 0 {
        return Err(RedisError::wrong_number_of_args(b"mset"));
    }
    let mut j = 1;
    while j < argc {
        let key = ctx.arg_owned(j)?;
        let value = ctx.arg_owned(j + 1)?;
        let obj = RedisObject::new_string_try_encoded(value.as_bytes());
        ctx.db_mut().set_key(key.clone(), obj, 0);
        ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &key);
        j += 2;
    }
    ctx.reply_simple_string(b"OK")
}

/// MSETNX key value [key value …]
///
/// Sets every supplied pair only when none of the named keys already
/// exists. Replies `:1` if applied, `:0` if any key existed.
///
/// C: `msetnxCommand` (t_string.c:597).
pub fn msetnx_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    if argc < 3 || argc % 2 == 0 {
        return Err(RedisError::wrong_number_of_args(b"msetnx"));
    }
    let mut pairs: Vec<(RedisString, RedisString)> = Vec::with_capacity((argc - 1) / 2);
    let mut j = 1;
    while j < argc {
        let key = ctx.arg_owned(j)?;
        let value = ctx.arg_owned(j + 1)?;
        pairs.push((key, value));
        j += 2;
    }
    for (key, _) in &pairs {
        if ctx.db_mut().lookup_key_write(key).is_some() {
            return ctx.reply_integer(0);
        }
    }
    for (key, value) in pairs {
        let obj = RedisObject::new_string_try_encoded(value.as_bytes());
        ctx.db_mut().set_key(key.clone(), obj, 0);
        ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &key);
    }
    ctx.reply_integer(1)
}

/// MSETEX numkeys key value [key value …] [NX|XX] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL]
///
/// C: `msetexCommand` (t_string.c:604).
pub fn msetex_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.arg_count();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(b"msetex"));
    }
    let numkeys_arg = ctx.arg_owned(1usize)?;
    let numkeys_signed = parse_strict_i64(numkeys_arg.as_bytes())
        .ok_or_else(|| RedisError::runtime(b"ERR invalid numkeys value or out of range"))?;
    if !(1..=i64::from(i32::MAX)).contains(&numkeys_signed) {
        return Err(RedisError::runtime(
            b"ERR invalid numkeys value or out of range",
        ));
    }
    let numkeys = numkeys_signed as usize;
    let pairs_end = match numkeys.checked_mul(2).and_then(|p| 2usize.checked_add(p)) {
        Some(v) => v,
        None => return Err(RedisError::runtime(b"ERR syntax error")),
    };
    if pairs_end > argc {
        return Err(RedisError::runtime(b"ERR syntax error"));
    }
    let mut nx = false;
    let mut xx = false;
    let mut keepttl = false;
    let mut expire_at_ms: Option<i64> = None;
    let mut got_expire_flag = false;
    let now_ms: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let mut j = pairs_end;
    while j < argc {
        let opt = ctx.arg_owned(j)?;
        let ob = opt.as_bytes();
        if ob.eq_ignore_ascii_case(b"NX") {
            if xx {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            nx = true;
            j += 1;
        } else if ob.eq_ignore_ascii_case(b"XX") {
            if nx {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            xx = true;
            j += 1;
        } else if ob.eq_ignore_ascii_case(b"KEEPTTL") {
            if got_expire_flag {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            keepttl = true;
            got_expire_flag = true;
            j += 1;
        } else if ob.eq_ignore_ascii_case(b"EX")
            || ob.eq_ignore_ascii_case(b"PX")
            || ob.eq_ignore_ascii_case(b"EXAT")
            || ob.eq_ignore_ascii_case(b"PXAT")
        {
            if got_expire_flag || j + 1 >= argc {
                return Err(RedisError::runtime(b"ERR syntax error"));
            }
            let val_arg = ctx.arg_owned(j + 1)?;
            let raw = parse_strict_i64(val_arg.as_bytes()).ok_or_else(|| {
                RedisError::runtime(b"ERR value is not an integer or out of range")
            })?;
            if raw <= 0 {
                return Err(RedisError::runtime(
                    b"ERR invalid expire time in 'msetex' command",
                ));
            }
            let abs = if ob.eq_ignore_ascii_case(b"EX") {
                raw.checked_mul(1000).and_then(|v| v.checked_add(now_ms))
            } else if ob.eq_ignore_ascii_case(b"PX") {
                raw.checked_add(now_ms)
            } else if ob.eq_ignore_ascii_case(b"EXAT") {
                raw.checked_mul(1000)
            } else {
                Some(raw)
            }
            .ok_or_else(|| RedisError::runtime(b"ERR invalid expire time in 'msetex' command"))?;
            expire_at_ms = Some(abs);
            got_expire_flag = true;
            j += 2;
        } else {
            return Err(RedisError::runtime(b"ERR syntax error"));
        }
    }
    if nx {
        for p in 0..numkeys {
            let k = ctx.arg_owned(2 + 2 * p)?;
            if ctx.db().find(&k).is_some() {
                return ctx.reply_integer(0);
            }
        }
    }
    if xx {
        for p in 0..numkeys {
            let k = ctx.arg_owned(2 + 2 * p)?;
            if ctx.db().find(&k).is_none() {
                return ctx.reply_integer(0);
            }
        }
    }
    for p in 0..numkeys {
        let k = ctx.arg_owned(2 + 2 * p)?;
        let v = ctx.arg_owned(3 + 2 * p)?;
        let obj = RedisObject::new_string_try_encoded(v.as_bytes());
        let flags = if keepttl { SETKEY_KEEPTTL } else { 0 };
        ctx.db_mut().set_key(k.clone(), obj, flags);
        ctx.notify_keyspace_event(NOTIFY_STRING, b"set", &k);
        if let Some(abs_ms) = expire_at_ms {
            if abs_ms <= now_ms {
                ctx.db_mut().sync_delete(&k);
            } else {
                ctx.db_mut().set_expire(&k, abs_ms);
                ctx.notify_keyspace_event(NOTIFY_GENERIC, b"expire", &k);
            }
        } else if !keepttl {
            ctx.db_mut().remove_expire(&k);
        }
    }
    ctx.reply_integer(1)
}

/// Parse a `RedisString` as an `i64` using Redis' strict semantics.
///
/// Real Redis rejects leading or trailing whitespace, embedded NUL, empty
/// strings, and any non-decimal characters except an optional leading sign.
/// This mirrors `string2ll` in util.c.
fn parse_strict_i64(bytes: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<i64>().ok()
}

/// Shared implementation for INCR / DECR / INCRBY / DECRBY.
///
/// Looks up the key, parses its current bytes as a strict `i64` (default 0
/// if the key is absent), applies the signed delta with overflow checking,
/// stores the resulting integer as ASCII decimal bytes under the key, and
/// replies with the new value. Returns the canonical Redis errors when the
/// existing value is not a parseable integer, when the key is the wrong
/// type, or when the arithmetic would overflow `i64`.
fn incr_decr_apply(
    ctx: &mut CommandContext,
    key: RedisString,
    delta: i64,
) -> Result<(), RedisError> {
    let mut current_expire = redis_core::object::EXPIRY_NONE;
    let current: i64 = match ctx.db_mut().lookup_key_write(&key) {
        None => 0,
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => {
                current_expire = obj.expire;
                match obj.get_long_long() {
                    Ok(n) => n,
                    Err(_) => return Err(RedisError::not_integer()),
                }
            }
            _ => return Err(RedisError::wrong_type()),
        },
    };
    let next = match current.checked_add(delta) {
        Some(v) => v,
        None => {
            return Err(RedisError::runtime(
                b"ERR increment or decrement would overflow",
            ))
        }
    };
    let stored = RedisObject::new_int_string(next);
    ctx.db_mut()
        .set_key_with_known_expire(key, stored, current_expire, SETKEY_KEEPTTL);
    ctx.reply_integer(next)
}

/// INCR key
///
/// C: `incrCommand` (t_string.c:731).
pub fn incr_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"incr"));
    }
    let key = ctx.arg_owned(1usize)?;
    if ctx.keyspace_notifications_enabled(NOTIFY_STRING) {
        incr_decr_apply(ctx, key.clone(), 1)?;
        ctx.notify_keyspace_event(NOTIFY_STRING, b"incrby", &key);
    } else {
        incr_decr_apply(ctx, key, 1)?;
    }
    Ok(())
}

/// DECR key
///
/// C: `decrCommand` (t_string.c:735).
pub fn decr_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"decr"));
    }
    let key = ctx.arg_owned(1usize)?;
    if ctx.keyspace_notifications_enabled(NOTIFY_STRING) {
        incr_decr_apply(ctx, key.clone(), -1)?;
        ctx.notify_keyspace_event(NOTIFY_STRING, b"decrby", &key);
    } else {
        incr_decr_apply(ctx, key, -1)?;
    }
    Ok(())
}

/// INCRBY key increment
///
/// C: `incrbyCommand` (t_string.c:739).
pub fn incrby_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"incrby"));
    }
    let key = ctx.arg_owned(1usize)?;
    let delta_raw = ctx.arg_owned(2usize)?;
    let delta = parse_strict_i64(delta_raw.as_bytes()).ok_or_else(RedisError::not_integer)?;
    if ctx.keyspace_notifications_enabled(NOTIFY_STRING) {
        incr_decr_apply(ctx, key.clone(), delta)?;
        ctx.notify_keyspace_event(NOTIFY_STRING, b"incrby", &key);
    } else {
        incr_decr_apply(ctx, key, delta)?;
    }
    Ok(())
}

/// DECRBY key decrement
///
/// C: `decrbyCommand` (t_string.c:746).
pub fn decrby_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"decrby"));
    }
    let key = ctx.arg_owned(1usize)?;
    let delta_raw = ctx.arg_owned(2usize)?;
    let delta = parse_strict_i64(delta_raw.as_bytes()).ok_or_else(RedisError::not_integer)?;
    let negated = match delta.checked_neg() {
        Some(v) => v,
        None => {
            return Err(RedisError::runtime(
                b"ERR increment or decrement would overflow",
            ))
        }
    };
    if ctx.keyspace_notifications_enabled(NOTIFY_STRING) {
        incr_decr_apply(ctx, key.clone(), negated)?;
        ctx.notify_keyspace_event(NOTIFY_STRING, b"decrby", &key);
    } else {
        incr_decr_apply(ctx, key, negated)?;
    }
    Ok(())
}

/// Format an `f64` value as ASCII decimal bytes matching Redis wire output.
///
/// Redis uses `ld2string` with `LD_STR_HUMAN` flag, which prints 17 significant
/// digits and strips trailing zeros. Rust's `Display` for `f64` already uses
/// Grisu/Dragon4 to produce the shortest decimal that round-trips, which
/// matches Redis output for most values. For integer-valued floats the Display
/// formatter omits the decimal point, so we append `.0` to match Redis
/// (e.g. `10.0` not `10`). Scientific notation is rejected by Redis; values
/// that Rust would format as `1e10` are formatted as `10000000000` via `{:.0}`.
fn format_float_redis(v: f64) -> Vec<u8> {
    let s = format!("{}", v);
    if s.contains('e') || s.contains('E') {
        let precise = format!("{:.17}", v);
        let trimmed = precise.trim_end_matches('0').trim_end_matches('.');
        if trimmed.is_empty() {
            return b"0".to_vec();
        }
        return trimmed.as_bytes().to_vec();
    }
    s.as_bytes().to_vec()
}

/// Parse a byte slice as a `f64` for use as an INCRBYFLOAT increment.
///
/// Allows Inf so that `+inf` triggers the "would produce" error after the
/// arithmetic rather than a generic "not a valid float" error. NaN literals
/// (`nan`, `-nan`) are rejected immediately because no arithmetic on a
/// finite value produces NaN via +inf.
fn parse_float_strict(bytes: &[u8]) -> Result<f64, RedisError> {
    if bytes.is_empty() {
        return Err(RedisError::not_float());
    }
    let s = core::str::from_utf8(bytes).map_err(|_| RedisError::not_float())?;
    if s.starts_with(char::is_whitespace) || s.ends_with(char::is_whitespace) {
        return Err(RedisError::not_float());
    }
    let v: f64 = s.parse().map_err(|_| RedisError::not_float())?;
    if v.is_nan() {
        return Err(RedisError::not_float());
    }
    Ok(v)
}

/// Parse a byte slice as a stored `f64` value (rejects Inf and NaN).
fn parse_stored_float(bytes: &[u8]) -> Result<f64, RedisError> {
    let v = parse_float_strict(bytes)?;
    if v.is_infinite() {
        return Err(RedisError::not_float());
    }
    Ok(v)
}

/// INCRBYFLOAT key increment
///
/// Parses the stored value as `f64` (defaulting to 0.0 for a missing key),
/// adds the `increment` (also parsed as `f64`), stores the result as its
/// canonical ASCII decimal representation, and replies with that string.
/// Returns `WRONGTYPE` when the key is not a string, and
/// `-ERR value is not a valid float` when either the stored value or the
/// increment cannot be parsed.
///
/// C: `incrbyfloatCommand` (t_string.c:758).
pub fn incrbyfloat_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"incrbyfloat"));
    }
    let key = ctx.arg_owned(1usize)?;
    let incr_arg = ctx.arg_owned(2usize)?;
    let incr = parse_float_strict(incr_arg.as_bytes())?;

    let current: f64 = match ctx.db_mut().lookup_key_write(&key) {
        None => 0.0,
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => {
                let bytes = obj.string_bytes_owned();
                parse_stored_float(&bytes)?
            }
            _ => return Err(RedisError::wrong_type()),
        },
    };

    let next = current + incr;
    if next.is_nan() || next.is_infinite() {
        return Err(RedisError::runtime(
            b"ERR increment would produce NaN or Infinity",
        ));
    }
    let result_bytes = format_float_redis(next);
    let stored = RedisObject::new_raw_string(&result_bytes);
    ctx.db_mut().set_key(key.clone(), stored, SETKEY_KEEPTTL);
    ctx.notify_keyspace_event(NOTIFY_STRING, b"incrbyfloat", &key);
    ctx.reply_bulk_string(RedisString::from_bytes(&result_bytes))
}

/// APPEND key value
///
/// If `key` does not exist the command behaves like SET. If `key` exists
/// and is a string, `value` is concatenated and the new length is returned.
/// A non-string `key` yields `WRONGTYPE`.
///
/// C: `appendCommand` (t_string.c:791).
pub fn append_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 3 {
        return Err(RedisError::wrong_number_of_args(b"append"));
    }
    let key = ctx.arg_owned(1usize)?;
    let value = ctx.arg_owned(2usize)?;
    let new_len: i64 = match ctx.db_mut().lookup_key_write(&key) {
        None => {
            let obj = RedisObject::new_string_try_encoded(value.as_bytes());
            let len = value.as_bytes().len() as i64;
            ctx.db_mut().set_key(key.clone(), obj, 0);
            len
        }
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => {
                let mut combined = obj.string_bytes_owned();
                combined.extend_from_slice(value.as_bytes());
                if combined.len() > PROTO_MAX_BULK_LEN_DEFAULT {
                    return Err(RedisError::runtime(
                        b"ERR string exceeds maximum allowed size (proto-max-bulk-len)",
                    ));
                }
                let len = combined.len() as i64;
                let stored = RedisObject::new_raw_string(&combined);
                ctx.db_mut().set_key(key.clone(), stored, SETKEY_KEEPTTL);
                len
            }
            _ => return Err(RedisError::wrong_type()),
        },
    };
    ctx.notify_keyspace_event(NOTIFY_STRING, b"append", &key);
    ctx.reply_integer(new_len)
}

/// STRLEN key
///
/// Returns the byte length of the string value at `key`, or `0` when the
/// key is missing. WRONGTYPE if the existing value is not a string.
///
/// C: `strlenCommand` (t_string.c:834).
pub fn strlen_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() != 2 {
        return Err(RedisError::wrong_number_of_args(b"strlen"));
    }
    let key = ctx.arg_owned(1usize)?;
    let len: i64 = match ctx.db_mut().lookup_key_read_with_flags(&key, LOOKUP_NONE) {
        None => 0,
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => obj.string_len()? as i64,
            _ => return Err(RedisError::wrong_type()),
        },
    };
    ctx.reply_integer(len)
}

/// One emitted LCS match range, in the order the backward walk produces it.
struct LcsMatch {
    a_start: u32,
    a_end: u32,
    b_start: u32,
    b_end: u32,
    match_len: u32,
}

/// Reads a key for LCS: a missing key is the empty string; a non-string value
/// is the upstream `The specified keys must contain string values` error
/// (note: not `WRONGTYPE`, matching `lcsCommand`).
fn lcs_lookup(ctx: &mut CommandContext, key: &RedisString) -> Result<Vec<u8>, RedisError> {
    match ctx.db_mut().lookup_key_read_with_flags(key, LOOKUP_NONE) {
        None => Ok(Vec::new()),
        Some(obj) => match &obj.kind {
            ObjectKind::String(_) => Ok(obj.string_bytes_owned()),
            _ => Err(RedisError::runtime(
                b"The specified keys must contain string values",
            )),
        },
    }
}

/// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
///
/// Implements the longest-common-subsequence algorithm via vanilla
/// O(n·m) dynamic programming, then walks the table backward to recover the
/// LCS string and (for `IDX`) the matching index ranges.
///
/// C: `lcsCommand` (t_string.c:842).
pub fn lcs_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    if ctx.arg_count() < 3 {
        return Err(RedisError::wrong_number_of_args(b"lcs"));
    }
    let key_a = ctx.arg_owned(1usize)?;
    let key_b = ctx.arg_owned(2usize)?;

    let mut getlen = false;
    let mut getidx = false;
    let mut withmatchlen = false;
    let mut minmatchlen: i64 = 0;

    let mut j = 3usize;
    let argc = ctx.arg_count();
    while j < argc {
        let opt = ctx.arg_owned(j)?;
        let opt = opt.as_bytes();
        let moreargs = argc - 1 - j;
        if opt.eq_ignore_ascii_case(b"IDX") {
            getidx = true;
        } else if opt.eq_ignore_ascii_case(b"LEN") {
            getlen = true;
        } else if opt.eq_ignore_ascii_case(b"WITHMATCHLEN") {
            withmatchlen = true;
        } else if opt.eq_ignore_ascii_case(b"MINMATCHLEN") && moreargs > 0 {
            let raw = ctx.arg_owned(j + 1)?;
            minmatchlen = parse_strict_i64(raw.as_bytes())
                .ok_or_else(|| RedisError::runtime(b"ERR value is not an integer or out of range"))?;
            if minmatchlen < 0 {
                minmatchlen = 0;
            }
            j += 1;
        } else {
            return Err(RedisError::runtime(b"ERR syntax error"));
        }
        j += 1;
    }

    if getlen && getidx {
        return Err(RedisError::runtime(
            b"If you want both the length and indexes, please just use IDX.",
        ));
    }

    let a = lcs_lookup(ctx, &key_a)?;
    let b = lcs_lookup(ctx, &key_b)?;
    let alen = a.len();
    let blen = b.len();

    // LCS[i][j] = length of the LCS of a[0..i] and b[0..j], stored row-major
    // in a flat (alen+1)*(blen+1) table indexed as lcs[i*(blen+1) + j].
    let width = blen + 1;
    let mut lcs = vec![0u32; (alen + 1) * width];
    for i in 1..=alen {
        for k in 1..=blen {
            lcs[i * width + k] = if a[i - 1] == b[k - 1] {
                lcs[(i - 1) * width + (k - 1)] + 1
            } else {
                lcs[(i - 1) * width + k].max(lcs[i * width + (k - 1)])
            };
        }
    }

    let total_len = lcs[alen * width + blen];
    let computelcs = getidx || !getlen;
    let mut result = vec![0u8; total_len as usize];
    let mut idx = total_len;
    let mut matches: Vec<LcsMatch> = Vec::new();

    // Sentinel: arange_start == alen means "no range currently open".
    let mut i = alen;
    let mut k = blen;
    let mut arange_start = alen;
    let mut arange_end = 0usize;
    let mut brange_start = 0usize;
    let mut brange_end = 0usize;

    while computelcs && i > 0 && k > 0 {
        let mut emit_range = false;
        if a[i - 1] == b[k - 1] {
            result[(idx - 1) as usize] = a[i - 1];
            if arange_start == alen {
                arange_start = i - 1;
                arange_end = i - 1;
                brange_start = k - 1;
                brange_end = k - 1;
            } else if arange_start == i && brange_start == k {
                arange_start -= 1;
                brange_start -= 1;
            } else {
                emit_range = true;
            }
            if arange_start == 0 || brange_start == 0 {
                emit_range = true;
            }
            idx -= 1;
            i -= 1;
            k -= 1;
        } else {
            if lcs[(i - 1) * width + k] > lcs[i * width + (k - 1)] {
                i -= 1;
            } else {
                k -= 1;
            }
            if arange_start != alen {
                emit_range = true;
            }
        }

        if emit_range {
            let match_len = (arange_end - arange_start + 1) as u32;
            if (minmatchlen == 0 || match_len as i64 >= minmatchlen) && getidx {
                matches.push(LcsMatch {
                    a_start: arange_start as u32,
                    a_end: arange_end as u32,
                    b_start: brange_start as u32,
                    b_end: brange_end as u32,
                    match_len,
                });
            }
            arange_start = alen;
        }
    }

    if getidx {
        let match_frames: Vec<RespFrame> = matches
            .iter()
            .map(|m| {
                let mut parts = vec![
                    RespFrame::Array(Some(vec![
                        RespFrame::Integer(m.a_start as i64),
                        RespFrame::Integer(m.a_end as i64),
                    ])),
                    RespFrame::Array(Some(vec![
                        RespFrame::Integer(m.b_start as i64),
                        RespFrame::Integer(m.b_end as i64),
                    ])),
                ];
                if withmatchlen {
                    parts.push(RespFrame::Integer(m.match_len as i64));
                }
                RespFrame::Array(Some(parts))
            })
            .collect();
        let reply = RespFrame::Map(vec![
            (
                RespFrame::bulk(RedisString::from_static(b"matches")),
                RespFrame::Array(Some(match_frames)),
            ),
            (
                RespFrame::bulk(RedisString::from_static(b"len")),
                RespFrame::Integer(total_len as i64),
            ),
        ]);
        ctx.reply_frame(&reply)
    } else if getlen {
        ctx.reply_integer(total_len as i64)
    } else {
        ctx.reply_bulk_string(RedisString::from_vec(result))
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/t_string.c  (1 056 lines, 29 functions)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         15
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         Phase A skeleton — all stubs; bodies added in subsequent edits.
//                  db/server access blocked on Phase 3 architect packet.
// ──────────────────────────────────────────────────────────────────────────
