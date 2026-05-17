//! Stream type and command implementations: XADD, XRANGE, XREVRANGE, XLEN,
//! XREAD, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XDEL, XTRIM,
//! XSETID, XGROUP, and XINFO.
//!
//! C source: `reference/valkey/src/t_stream.c` (4 029 lines, ~54 functions).
//! Also absorbs type declarations from `reference/valkey/src/stream.h`.
//! Crate: `redis-commands`  (phase: later)
//!
//! ## Architecture / design notes
//!
//! The C implementation stores stream data as a radix tree (`rax`) whose values
//! are listpack byte arrays.  Both `rax` and `listpack` are Phase 4/5 components
//! owned by `crates/redis-ds`.  This Phase A translation replaces:
//!   - `rax *`        → `std::collections::BTreeMap<[u8; 16], Vec<u8>>` (key:
//!                       16-byte big-endian stream ID; value: listpack bytes)
//!   - listpack ops   → `TODO(port)` stubs with `Vec<u8>` placeholders
//!
//! The sorted-by-key property of BTreeMap faithfully models the rax's
//! lexicographic ordering of 128-bit big-endian IDs.
//!
//! Consumer-group NACKs are shared between two PELs in C (cg->pel and
//! consumer->pel point to the same `streamNACK *`).  Phase A models this as:
//!   - `StreamCG.pel`:       `BTreeMap<[u8;16], StreamNACK>` (owned)
//!   - `StreamConsumer.pel`: `BTreeSet<[u8;16]>` (keys only; owned values live
//!                            in the CG PEL)
//! See `TODO(architect)` below for the Phase B ownership decision.
//!
//! ## TODO(architect) items
//!
//! TODO(architect): `StreamId`, `Stream`, `StreamCG`, `StreamConsumer`,
//!   `StreamNACK` should eventually live in `crates/redis-ds/src/stream.rs`
//!   (Phase 5 audit type in type-vocabulary.tsv).  Defined here as a Phase A
//!   placeholder; migrate when Phase 5 begins.
//!
//! TODO(architect): Decide shared-ownership model for NACK ↔ consumer back-ref
//!   (C uses raw-ptr aliasing; Rust needs Arc<Mutex<…>> or index-map approach).
//!
//! TODO(architect): `CommandContext` needs
//!   `command_time_snapshot() -> i64`,
//!   `server_stream_node_max_bytes() -> usize`,
//!   `server_stream_node_max_entries() -> i64`,
//!   `server_dirty_incr()`,
//!   `notify_keyspace_event(kind, event, key)`,
//!   `signal_modified_key(key)`,
//!   `signal_key_as_ready(db, key, obj_type)`,
//!   `block_for_keys(keys, n, timeout, is_xreadgroup)`,
//!   `also_propagate(db_id, argv, argc, flags, slot)`,
//!   `prevent_command_propagation()`,
//!   `must_obey_client() -> bool`,
//!   `rewrite_client_command_argument(idx, arg)`,
//!   `reply_deferred_len() -> DeferredLen`,
//!   `set_deferred_array_len(ptr, len)`,
//!   `set_deferred_map_len(ptr, len)`,
//!   `reply_null_array()`,
//!   `reply_help(lines)`,
//!   `reply_subcommand_syntax_error()`,
//!   `reply_bulk_sds(s)`,
//!   `reply_add_reply_error_arity()`.
//!   All blocked on Phase 3 architect packet.

use redis_core::command_context::CommandContext;
use redis_core::object::RedisObject;
use redis_types::{RedisError, RedisString};
use std::collections::{BTreeMap, BTreeSet};

// ── Item-flag constants (C: STREAM_ITEM_FLAG_*) ──────────────────────────────

pub const STREAM_ITEM_FLAG_NONE: i64 = 0;
pub const STREAM_ITEM_FLAG_DELETED: i64 = 1 << 0;
pub const STREAM_ITEM_FLAG_SAMEFIELDS: i64 = 1 << 1;

// ── Misc constants ────────────────────────────────────────────────────────────

pub const STREAMID_STATIC_VECTOR_LEN: usize = 8;
pub const STREAM_LISTPACK_MAX_PRE_ALLOCATE: usize = 4096;
pub const STREAM_LISTPACK_MAX_SIZE: usize = 1 << 30;
pub const STREAM_ID_STR_LEN: usize = 44;
pub const XREAD_BLOCKED_DEFAULT_COUNT: i64 = 1000;

// ── streamReplyWithRange flags ────────────────────────────────────────────────

pub const STREAM_RWR_NOACK: i32 = 1 << 0;
pub const STREAM_RWR_RAWENTRIES: i32 = 1 << 1;
pub const STREAM_RWR_HISTORY: i32 = 1 << 2;

// ── Trim strategy constants ───────────────────────────────────────────────────

pub const TRIM_STRATEGY_NONE: i32 = 0;
pub const TRIM_STRATEGY_MAXLEN: i32 = 1;
pub const TRIM_STRATEGY_MINID: i32 = 2;

// ── streamCreateConsumer flags ────────────────────────────────────────────────

pub const SCC_DEFAULT: i32 = 0;
pub const SCC_NO_NOTIFY: i32 = 1 << 0;
pub const SCC_NO_DIRTIFY: i32 = 1 << 1;

// ── Consumer-group sentinel ───────────────────────────────────────────────────

pub const SCG_INVALID_ENTRIES_READ: i64 = -1;

// ── StreamId ──────────────────────────────────────────────────────────────────
// PORT NOTE: canonical home is crates/redis-ds/src/stream.rs (audit, Phase 5).
// Defined here as a Phase A placeholder.
// TODO(architect): migrate StreamId to redis-ds when Phase 5 starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

// ── Stream ────────────────────────────────────────────────────────────────────
// PORT NOTE: canonical home is crates/redis-ds/src/stream.rs (Phase 5).
// TODO(architect): migrate to redis-ds.
//
// The `data` field replaces `rax *rax` in the C struct.  Keys are 16-byte
// big-endian encoded StreamId values (via `stream_encode_id`); values are
// raw listpack bytes.
// TODO(port): replace `Vec<u8>` listpack placeholder with `ListPack` from
// redis-ds once Phase 4 lands.
pub struct Stream {
    pub data: BTreeMap<[u8; 16], Vec<u8>>,
    pub length: u64,
    pub last_id: StreamId,
    pub first_id: StreamId,
    pub max_deleted_entry_id: StreamId,
    pub entries_added: u64,
    pub cgroups: Option<BTreeMap<Vec<u8>, StreamCG>>,
}

// ── StreamCG ──────────────────────────────────────────────────────────────────
// PORT NOTE: canonical home is crates/redis-ds/src/stream.rs (Phase 5).
// TODO(architect): migrate to redis-ds.
pub struct StreamCG {
    pub last_id: StreamId,
    pub entries_read: i64,
    /// Owned NACK entries keyed by encoded StreamId (16 bytes big-endian).
    pub pel: BTreeMap<[u8; 16], StreamNACK>,
    /// Consumers keyed by name bytes.
    pub consumers: BTreeMap<Vec<u8>, StreamConsumer>,
}

// ── StreamConsumer ────────────────────────────────────────────────────────────
// PORT NOTE: canonical home is crates/redis-ds/src/stream.rs (Phase 5).
// TODO(architect): migrate to redis-ds.
pub struct StreamConsumer {
    pub seen_time: i64,
    /// -1 when never active (C: initialised to -1).
    pub active_time: i64,
    pub name: RedisString,
    /// Keys into the parent `StreamCG.pel`.  Ownership lives in the CG PEL.
    /// TODO(architect): decide whether to use Arc<Mutex<StreamNACK>> for true
    /// shared ownership, or keep this index-based approach.
    pub pel: BTreeSet<[u8; 16]>,
}

// ── StreamNACK ────────────────────────────────────────────────────────────────
// PORT NOTE: canonical home is crates/redis-ds/src/stream.rs (Phase 5).
// TODO(architect): migrate to redis-ds.
pub struct StreamNACK {
    pub delivery_time: i64,
    pub delivery_count: u64,
    /// Consumer name (key into StreamCG.consumers).
    /// C uses a raw back-pointer; we store the name for Phase A safety.
    /// TODO(architect): decide final back-ref representation.
    pub consumer_name: Option<RedisString>,
}

// ── StreamPropInfo ────────────────────────────────────────────────────────────
/// Carries keyname and groupname for AOF/replica propagation.
/// C source: `stream.h:101-104`, `streamPropInfo`.
pub struct StreamPropInfo {
    pub keyname: RedisString,
    pub groupname: RedisString,
}

// ── StreamIterator ────────────────────────────────────────────────────────────
// PORT NOTE: The C iterator stores raw pointers into a mutable listpack byte
// array.  That pattern is incompatible with safe Rust.  This struct captures
// the iterator's *logical* state; the pointer-arithmetic inside
// `stream_iterator_get_id` / `stream_iterator_get_field` is TODO(port) pending
// Phase 4 ListPack API.
// TODO(architect): redesign StreamIterator for safe Rust once ListPack lands.
pub struct StreamIterator {
    pub primary_id: StreamId,
    pub primary_fields_count: u64,
    pub entry_flags: i64,
    pub rev: bool,
    pub skip_tombstones: bool,
    pub start_id: StreamId,
    pub end_id: StreamId,
    /// Current position in `stream.data` (the "rax cursor").
    pub rax_key: Option<[u8; 16]>,
    /// Raw listpack bytes for the current node.
    /// TODO(port): replace with ListPack reference once Phase 4 lands.
    pub lp: Vec<u8>,
    /// Byte cursor within the current listpack.
    pub lp_cursor: usize,
    /// Cursor for the flags field of the current entry.
    pub lp_flags_cursor: usize,
    /// Cursor for the primary-entry field list (SAMEFIELDS compression).
    pub primary_fields_start: usize,
    pub primary_fields_cursor: usize,
    /// Scratch buffers for integer-encoded listpack elements (mirrors C).
    pub field_buf: [u8; 21],
    pub value_buf: [u8; 21],
}

// ── StreamAddTrimArgs ─────────────────────────────────────────────────────────
/// Parsed arguments for XADD / XTRIM.
/// C source: `t_stream.c:663-681`.
#[derive(Default)]
pub struct StreamAddTrimArgs {
    pub id: StreamId,
    pub id_given: bool,
    pub seq_given: bool,
    pub no_mkstream: bool,
    pub trim_strategy: i32,
    pub trim_strategy_arg_idx: i32,
    pub approx_trim: bool,
    pub limit: i64,
    pub maxlen: i64,
    pub minid: StreamId,
}

// ─────────────────────────────────────────────────────────────────────────────
// StreamId operations
// ─────────────────────────────────────────────────────────────────────────────

impl StreamId {
    /// Return the zero ID.
    pub fn zero() -> Self {
        StreamId { ms: 0, seq: 0 }
    }

    /// Return the maximum ID.
    pub fn max() -> Self {
        StreamId { ms: u64::MAX, seq: u64::MAX }
    }

    /// Return true if this is the 0-0 ID.
    /// C source: `t_stream.c:1398-1400`, `streamIDEqZero`.
    pub fn is_zero(&self) -> bool {
        self.ms == 0 && self.seq == 0
    }

    /// Encode as 16-byte big-endian buffer (for rax key ordering).
    /// C source: `t_stream.c:358-363`, `streamEncodeID`.
    pub fn encode(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&self.ms.to_be_bytes());
        buf[8..].copy_from_slice(&self.seq.to_be_bytes());
        buf
    }

    /// Decode from a 16-byte big-endian buffer.
    /// C source: `t_stream.c:368-373`, `streamDecodeID`.
    pub fn decode(buf: &[u8; 16]) -> Self {
        let ms = u64::from_be_bytes(buf[..8].try_into().unwrap_or([0u8; 8]));
        let seq = u64::from_be_bytes(buf[8..].try_into().unwrap_or([0u8; 8]));
        StreamId { ms, seq }
    }

    /// Compare two stream IDs. Returns -1/0/1.
    /// C source: `t_stream.c:376-388`, `streamCompareID`.
    pub fn compare(&self, other: &StreamId) -> i32 {
        if self.ms > other.ms { return 1; }
        if self.ms < other.ms { return -1; }
        if self.seq > other.seq { return 1; }
        if self.seq < other.seq { return -1; }
        0
    }

    /// Increment the ID in-place. Returns `Err` if the ID wraps (was MAX).
    /// C source: `t_stream.c:104-119`, `streamIncrID`.
    pub fn incr(&mut self) -> Result<(), RedisError> {
        if self.seq == u64::MAX {
            if self.ms == u64::MAX {
                self.ms = 0;
                self.seq = 0;
                return Err(RedisError::runtime(b"stream ID overflow"));
            }
            self.ms += 1;
            self.seq = 0;
        } else {
            self.seq += 1;
        }
        Ok(())
    }

    /// Decrement the ID in-place. Returns `Err` if already 0-0.
    /// C source: `t_stream.c:124-139`, `streamDecrID`.
    pub fn decr(&mut self) -> Result<(), RedisError> {
        if self.seq == 0 {
            if self.ms == 0 {
                self.ms = u64::MAX;
                self.seq = u64::MAX;
                return Err(RedisError::runtime(b"stream ID underflow"));
            }
            self.ms -= 1;
            self.seq = u64::MAX;
        } else {
            self.seq -= 1;
        }
        Ok(())
    }

    /// Format the stream ID as `<ms>-<seq>` into a byte buffer.
    /// Returns the number of bytes written.
    /// C source: `t_stream.c:1366-1372`, `streamID2string`.
    pub fn to_bytes(&self, buf: &mut [u8; STREAM_ID_STR_LEN]) -> usize {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(buf.as_mut());
        // PERF(port): itoa would be faster; using format! here for simplicity.
        let _ = write!(cursor, "{}-{}", self.ms, self.seq);
        cursor.position() as usize
    }

    /// Allocate and return the `<ms>-<seq>` representation.
    /// C source: `t_stream.c:1374-1378`, `createStreamIDString`.
    pub fn to_redis_string(&self) -> RedisString {
        let mut buf = [0u8; STREAM_ID_STR_LEN];
        let len = self.to_bytes(&mut buf);
        RedisString::from_bytes(&buf[..len])
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stream construction / destruction
// ─────────────────────────────────────────────────────────────────────────────

/// Create a new empty stream.
/// C source: `t_stream.c:73-86`, `streamNew`.
pub fn stream_new() -> Stream {
    Stream {
        data: BTreeMap::new(),
        length: 0,
        first_id: StreamId::zero(),
        last_id: StreamId::zero(),
        max_deleted_entry_id: StreamId::zero(),
        entries_added: 0,
        cgroups: None,
    }
}

/// Return the number of entries in a stream.
/// C source: `t_stream.c:96-99`, `streamLength`.
pub fn stream_length(s: &Stream) -> u64 {
    s.length
}

/// Generate the next stream ID given the previous one and the current wall
/// clock time.
/// C source: `t_stream.c:145-154`, `streamNextID`.
/// TODO(architect): replace `current_ms` param with `ctx.command_time_snapshot()`.
pub fn stream_next_id(last_id: &StreamId, current_ms: u64) -> StreamId {
    if current_ms > last_id.ms {
        StreamId { ms: current_ms, seq: 0 }
    } else {
        let mut new_id = *last_id;
        let _ = new_id.incr(); // ignore overflow; already checked at call site
        new_id
    }
}

/// Deep-duplicate a stream including all consumer groups and PELs.
/// C source: `t_stream.c:161-259`, `streamDup`.
/// TODO(port): listpack deep-copy requires Phase 4 ListPack API; currently
/// copies raw bytes which is correct for the data nodes but consumer
/// cross-references need the shared-ownership design from the architect.
pub fn stream_dup(s: &Stream) -> Stream {
    // Copy data nodes (each is a raw listpack byte blob).
    let data: BTreeMap<[u8; 16], Vec<u8>> = s.data
        .iter()
        .map(|(k, v)| (*k, v.clone()))
        .collect();

    let cgroups = s.cgroups.as_ref().map(|cgs| {
        cgs.iter().map(|(name, cg)| {
            let new_pel: BTreeMap<[u8; 16], StreamNACK> = cg.pel.iter()
                .map(|(k, nack)| {
                    (*k, StreamNACK {
                        delivery_time: nack.delivery_time,
                        delivery_count: nack.delivery_count,
                        consumer_name: nack.consumer_name.clone(),
                    })
                })
                .collect();
            let new_consumers: BTreeMap<Vec<u8>, StreamConsumer> = cg.consumers.iter()
                .map(|(cname, consumer)| {
                    (cname.clone(), StreamConsumer {
                        name: consumer.name.clone(),
                        seen_time: consumer.seen_time,
                        active_time: consumer.active_time,
                        pel: consumer.pel.clone(),
                    })
                })
                .collect();
            (name.clone(), StreamCG {
                last_id: cg.last_id,
                entries_read: cg.entries_read,
                pel: new_pel,
                consumers: new_consumers,
            })
        }).collect()
    });

    Stream {
        data,
        length: s.length,
        first_id: s.first_id,
        last_id: s.last_id,
        max_deleted_entry_id: s.max_deleted_entry_id,
        entries_added: s.entries_added,
        cgroups,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Low-level listpack helpers (stubs — Phase 4)
// ─────────────────────────────────────────────────────────────────────────────

// PORT NOTE: All `lp*` operations in C work on a raw byte array with a custom
// binary format.  Until the Phase 4 `ListPack` type is available in redis-ds,
// these are stubbed.  The signatures capture the intent; the bodies carry
// TODO(port) markers.

/// Get the edge stream ID (first or last) from a listpack node.
/// C source: `t_stream.c:291-341`, `lpGetEdgeStreamID`.
/// TODO(port): requires listpack traversal; stub pending Phase 4.
fn lp_get_edge_stream_id(lp: &[u8], first: bool, primary_id: &StreamId) -> Option<StreamId> {
    if lp.is_empty() {
        return None;
    }
    // TODO(port): parse listpack header to extract first/last entry ID delta.
    // For now return None to indicate "not implemented".
    let _ = (first, primary_id);
    None
}

// ─────────────────────────────────────────────────────────────────────────────
// Stream ID encoding / comparison (standalone helpers)
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a stream ID into a 16-byte big-endian buffer.
/// C source: `t_stream.c:358-363`, `streamEncodeID`.
pub fn stream_encode_id(id: &StreamId) -> [u8; 16] {
    id.encode()
}

/// Decode a 16-byte big-endian buffer into a stream ID.
/// C source: `t_stream.c:368-373`, `streamDecodeID`.
pub fn stream_decode_id(buf: &[u8; 16]) -> StreamId {
    StreamId::decode(buf)
}

/// Compare two stream IDs: -1 / 0 / 1.
/// C source: `t_stream.c:376-388`, `streamCompareID`.
pub fn stream_compare_id(a: &StreamId, b: &StreamId) -> i32 {
    a.compare(b)
}

/// Return true if `id` is the 0-0 ID.
/// C source: `t_stream.c:1398-1400`, `streamIDEqZero`.
pub fn stream_id_eq_zero(id: &StreamId) -> bool {
    id.is_zero()
}

// ─────────────────────────────────────────────────────────────────────────────
// Stream edge / tombstone helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Find the first or last non-tombstone (or including tombstone) entry ID.
/// C source: `t_stream.c:393-404`, `streamGetEdgeID`.
/// TODO(port): relies on the stream iterator which needs Phase 4 listpack.
pub fn stream_get_edge_id(s: &mut Stream, first: bool, skip_tombstones: bool) -> StreamId {
    let mut si = stream_iterator_start(s, None, None, !first);
    si.skip_tombstones = skip_tombstones;
    // TODO(port): stream_iterator_get_id stub; returns sentinel on failure.
    let _ = stream_iterator_stop(&si);
    if first { StreamId::max() } else { StreamId::zero() }
}

/// Return true if the range [start, end] contains any tombstone entry.
/// C source: `t_stream.c:1407-1437`, `streamRangeHasTombstones`.
pub fn stream_range_has_tombstones(
    s: &Stream,
    start: Option<&StreamId>,
    end: Option<&StreamId>,
) -> bool {
    if s.length == 0 || s.max_deleted_entry_id.is_zero() {
        return false;
    }
    let start_id = start.copied().unwrap_or(StreamId::zero());
    let end_id = end.copied().unwrap_or(StreamId::max());
    start_id.compare(&s.max_deleted_entry_id) <= 0
        && s.max_deleted_entry_id.compare(&end_id) <= 0
}

/// Estimate how many entries were added before `id` (distance from origin).
/// C source: `t_stream.c:1494-1536`, `streamEstimateDistanceFromFirstEverEntry`.
pub fn stream_estimate_distance_from_first_ever_entry(s: &Stream, id: &StreamId) -> i64 {
    if s.entries_added == 0 {
        return 0;
    }
    if s.length == 0 && id.compare(&s.last_id) < 1 {
        return s.entries_added as i64;
    }
    if !id.is_zero() && id.compare(&s.max_deleted_entry_id) < 0 {
        return SCG_INVALID_ENTRIES_READ;
    }
    let cmp_last = id.compare(&s.last_id);
    if cmp_last == 0 {
        return s.entries_added as i64;
    }
    if cmp_last > 0 {
        return SCG_INVALID_ENTRIES_READ;
    }
    let cmp_id_first = id.compare(&s.first_id);
    let cmp_xdel_first = s.max_deleted_entry_id.compare(&s.first_id);
    if s.max_deleted_entry_id.is_zero() || cmp_xdel_first < 0 {
        if cmp_id_first < 0 {
            return (s.entries_added - s.length) as i64;
        } else if cmp_id_first == 0 {
            return (s.entries_added - s.length + 1) as i64;
        }
    }
    SCG_INVALID_ENTRIES_READ
}

// ─────────────────────────────────────────────────────────────────────────────
// Stream append / trim
// ─────────────────────────────────────────────────────────────────────────────

/// Append a new entry to the stream.
/// C source: `t_stream.c:425-661`, `streamAppendItem`.
///
/// `argv` holds alternating field/value pairs.  `use_id` is `None` for
/// auto-generated IDs.  Returns the new entry's `StreamId`.
///
/// TODO(port): the listpack encoding logic (primary-entry, delta-ID, SAMEFIELDS
/// flag, lp-count trailing field) requires Phase 4 ListPack API.  Currently
/// only the ID generation and length bookkeeping are faithfully translated.
/// TODO(architect): pass `current_ms` via `ctx.command_time_snapshot()`.
pub fn stream_append_item(
    s: &mut Stream,
    argv: &[RedisString],
    use_id: Option<StreamId>,
    seq_given: bool,
    current_ms: u64,
) -> Result<StreamId, RedisError> {
    // C: t_stream.c:427-449 — generate the new entry ID.
    let id = if let Some(mut uid) = use_id {
        if seq_given {
            uid
        } else {
            if s.last_id.ms == uid.ms {
                if s.last_id.seq == u64::MAX {
                    return Err(RedisError::runtime(b"ERR stream ID sequence overflow"));
                }
                let mut new_id = s.last_id;
                new_id.seq += 1;
                new_id
            } else {
                uid
            }
        }
    } else {
        stream_next_id(&s.last_id, current_ms)
    };

    // C: t_stream.c:455-458 — check monotonicity.
    if id.compare(&s.last_id) <= 0 {
        return Err(RedisError::runtime(
            b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
        ));
    }

    // C: t_stream.c:463-471 — check total element size.
    let total_len: usize = argv.iter().map(|s: &RedisString| s.len()).sum();
    if total_len > STREAM_LISTPACK_MAX_SIZE {
        return Err(RedisError::runtime(b"ERR Elements are too large to be stored"));
    }

    // TODO(port): Listpack encoding (primary entry, delta IDs, SAMEFIELDS
    // compression, lp-count field) is not yet implemented.
    // Inserting a raw placeholder entry so that ID bookkeeping is correct.
    let key = id.encode();
    // Placeholder: store the raw field/value pairs as a flat byte sequence.
    let mut raw: Vec<u8> = Vec::with_capacity(total_len + 32);
    for field_or_val in argv {
        raw.extend_from_slice(field_or_val.as_bytes());
        raw.push(b'\0'); // separator
    }
    s.data.insert(key, raw);

    // C: t_stream.c:655-659 — update bookkeeping.
    s.length += 1;
    s.entries_added += 1;
    s.last_id = id;
    if s.length == 1 {
        s.first_id = id;
    }

    Ok(id)
}

/// Trim the stream per the provided `args`.  Returns the number of deleted entries.
/// C source: `t_stream.c:710-865`, `streamTrim`.
/// TODO(port): approximate trimming of whole radix-tree nodes requires the
/// Phase 4 listpack API to read node entry-counts and last IDs.  Currently
/// performs exact trimming by iterating `data` (BTreeMap) which does not
/// distinguish between nodes.
pub fn stream_trim(s: &mut Stream, args: &StreamAddTrimArgs) -> i64 {
    if args.trim_strategy == TRIM_STRATEGY_NONE {
        return 0;
    }
    if args.trim_strategy == TRIM_STRATEGY_MAXLEN && s.length <= args.maxlen as u64 {
        return 0;
    }

    let mut deleted: i64 = 0;
    // TODO(port): approximate trimming (args.approx_trim) should only remove
    // whole listpack nodes; implementing exact trimming here as a safe fallback.
    loop {
        if args.trim_strategy == TRIM_STRATEGY_MAXLEN {
            if s.length <= args.maxlen as u64 {
                break;
            }
        }
        if args.limit != 0 && deleted >= args.limit {
            break;
        }
        // Pop the smallest key (oldest entry).
        let Some((key, _)) = s.data.pop_first() else { break };
        let entry_id = StreamId::decode(&key);
        if args.trim_strategy == TRIM_STRATEGY_MINID {
            if entry_id.compare(&args.minid) >= 0 {
                // Re-insert and stop — we've gone past the trim point.
                // TODO(port): this is wrong for the node-level model; nodes
                // may span multiple IDs.  Placeholder for Phase B.
                s.data.insert(key, Vec::new());
                break;
            }
        }
        s.length = s.length.saturating_sub(1);
        deleted += 1;
    }

    // Update first_id after trimming.
    if s.length == 0 {
        s.first_id = StreamId::zero();
    } else if deleted > 0 {
        // PORT NOTE: stream_get_edge_id is a TODO(port) stub; best effort here.
        if let Some((&first_key, _)) = s.data.iter().next() {
            s.first_id = StreamId::decode(&first_key);
        }
    }

    deleted
}

/// Trim a stream by maximum length.
/// C source: `t_stream.c:868-874`, `streamTrimByLength`.
/// TODO(architect): `server_stream_node_max_entries` needed for limit calculation.
pub fn stream_trim_by_length(s: &mut Stream, maxlen: i64, approx: bool) -> i64 {
    let args = StreamAddTrimArgs {
        trim_strategy: TRIM_STRATEGY_MAXLEN,
        approx_trim: approx,
        limit: if approx { 10000 } else { 0 }, // TODO(architect): 100 * stream_node_max_entries
        maxlen,
        ..Default::default()
    };
    stream_trim(s, &args)
}

/// Trim a stream by minimum ID.
/// C source: `t_stream.c:877-883`, `streamTrimByID`.
/// TODO(architect): `server_stream_node_max_entries` needed for limit calculation.
pub fn stream_trim_by_id(s: &mut Stream, minid: StreamId, approx: bool) -> i64 {
    let args = StreamAddTrimArgs {
        trim_strategy: TRIM_STRATEGY_MINID,
        approx_trim: approx,
        limit: if approx { 10000 } else { 0 }, // TODO(architect): 100 * stream_node_max_entries
        minid,
        ..Default::default()
    };
    stream_trim(s, &args)
}

// ─────────────────────────────────────────────────────────────────────────────
// Stream iterator
// ─────────────────────────────────────────────────────────────────────────────
// PORT NOTE: The C iterator walks a rax radix tree node by node, and within
// each node walks the listpack using raw pointer arithmetic.  The Rust version
// captures iterator state but the internal traversal methods are TODO(port)
// stubs until Phase 4 ListPack is available.

/// Initialise a stream iterator over [start, end] in the given direction.
/// C source: `t_stream.c:1042-1088`, `streamIteratorStart`.
pub fn stream_iterator_start(
    s: &Stream,
    start: Option<StreamId>,
    end: Option<StreamId>,
    rev: bool,
) -> StreamIterator {
    let start_id = start.unwrap_or(StreamId::zero());
    let end_id = end.unwrap_or(StreamId::max());

    // Find the first rax key to visit.
    // C: seeks "<=start" or "^" for forward; "<= end" or "$" for reverse.
    let rax_key: Option<[u8; 16]> = if !rev {
        let start_key = start_id.encode();
        // Find the node whose key is <= start_key, or the first node.
        s.data.range(..=start_key).next_back().map(|(k, _)| *k)
            .or_else(|| s.data.keys().next().copied())
    } else {
        let end_key = end_id.encode();
        s.data.range(..=end_key).next_back().map(|(k, _)| *k)
            .or_else(|| s.data.keys().next_back().copied())
    };

    let lp = rax_key
        .and_then(|k| s.data.get(&k).cloned())
        .unwrap_or_default();

    StreamIterator {
        primary_id: StreamId::zero(),
        primary_fields_count: 0,
        entry_flags: 0,
        rev,
        skip_tombstones: true,
        start_id,
        end_id,
        rax_key,
        lp,
        lp_cursor: 0,
        lp_flags_cursor: 0,
        primary_fields_start: 0,
        primary_fields_cursor: 0,
        field_buf: [0u8; 21],
        value_buf: [0u8; 21],
    }
}

/// Advance the iterator.  Returns `Some((id, numfields))` or `None` at end.
/// C source: `t_stream.c:1093-1222`, `streamIteratorGetID`.
/// TODO(port): listpack pointer traversal not yet implemented; returns None.
pub fn stream_iterator_get_id(
    _si: &mut StreamIterator,
    _s: &Stream,
) -> Option<(StreamId, i64)> {
    // TODO(port): implement listpack traversal once Phase 4 ListPack lands.
    None
}

/// Get the current field/value pair from the iterator.
/// C source: `t_stream.c:1230-1244`, `streamIteratorGetField`.
/// TODO(port): listpack pointer traversal not yet implemented.
pub fn stream_iterator_get_field(
    _si: &mut StreamIterator,
) -> Option<(Vec<u8>, Vec<u8>)> {
    // TODO(port): implement once Phase 4 ListPack lands.
    None
}

/// Mark the current entry as deleted and re-seek the iterator.
/// C source: `t_stream.c:1256-1306`, `streamIteratorRemoveEntry`.
/// TODO(port): requires in-place listpack mutation; stub pending Phase 4.
pub fn stream_iterator_remove_entry(
    si: &mut StreamIterator,
    current: &StreamId,
    s: &mut Stream,
) {
    // TODO(port): mark STREAM_ITEM_FLAG_DELETED in the listpack and update
    // entry/deleted counters in the primary entry.
    // As a Phase A placeholder: remove the data node entirely.
    let key = current.encode();
    if s.data.remove(&key).is_some() {
        s.length = s.length.saturating_sub(1);
    }
    // Re-seek the iterator to the next position.
    let new_start = if si.rev {
        si.start_id
    } else {
        *current
    };
    let new_end = if si.rev {
        *current
    } else {
        si.end_id
    };
    *si = stream_iterator_start(s, Some(new_start), Some(new_end), si.rev);
}

/// Stop the iterator (no-op in Rust; C needed to free rax iterator).
/// C source: `t_stream.c:1311-1313`, `streamIteratorStop`.
pub fn stream_iterator_stop(_si: &StreamIterator) {}

/// Return true if the entry with given ID exists in the stream (not deleted).
/// C source: `t_stream.c:1316-1326`, `streamEntryExists`.
pub fn stream_entry_exists(s: &Stream, id: &StreamId) -> bool {
    // TODO(port): should use stream iterator to check the DELETED flag inside
    // the listpack.  Using data key presence as a Phase A approximation.
    s.data.contains_key(&id.encode())
}

/// Delete the entry with the given ID from the stream.
/// Returns 1 if deleted, 0 if not found.
/// C source: `t_stream.c:1330-1342`, `streamDeleteItem`.
pub fn stream_delete_item(s: &mut Stream, id: &StreamId) -> i32 {
    let mut si = stream_iterator_start(s, Some(*id), Some(*id), false);
    // TODO(port): stream_iterator_get_id stub returns None; using direct delete
    // as placeholder.
    let key = id.encode();
    if s.data.remove(&key).is_some() {
        s.length = s.length.saturating_sub(1);
        stream_iterator_stop(&si);
        1
    } else {
        stream_iterator_stop(&si);
        0
    }
}

/// Get the last valid (non-tombstone) stream ID.
/// C source: `t_stream.c:1345-1352`, `streamLastValidID`.
/// TODO(port): should use the iterator to skip tombstones; using last BTreeMap
/// key as a Phase A approximation (correct when no tombstones present).
pub fn stream_last_valid_id(s: &Stream) -> Option<StreamId> {
    s.data.keys().next_back().map(StreamId::decode)
}

// ─────────────────────────────────────────────────────────────────────────────
// Reply helpers (depend on CommandContext which is Phase 3+)
// ─────────────────────────────────────────────────────────────────────────────

/// Reply with a stream ID as a bulk string `<ms>-<seq>`.
/// C source: `t_stream.c:1383-1385`, `addReplyStreamID`.
pub fn add_reply_stream_id(ctx: &mut CommandContext, id: &StreamId) -> Result<(), RedisError> {
    let s = id.to_redis_string();
    ctx.reply_bulk(s.as_bytes())
}

/// Create a `RedisObject` wrapping the string form of a stream ID.
/// C source: `t_stream.c:1393-1395`, `createObjectFromStreamID`.
pub fn create_object_from_stream_id(id: &StreamId) -> RedisObject {
    RedisObject::String(id.to_redis_string())
}

/// Reply with the consumer group lag.
/// C source: `t_stream.c:1442-1470`, `streamReplyWithCGLag`.
pub fn stream_reply_with_cg_lag(
    ctx: &mut CommandContext,
    s: &Stream,
    cg: &StreamCG,
) -> Result<(), RedisError> {
    let valid;
    let lag;

    if s.entries_added == 0 {
        lag = 0i64;
        valid = true;
    } else if cg.entries_read != SCG_INVALID_ENTRIES_READ
        && !stream_range_has_tombstones(s, Some(&cg.last_id), None)
    {
        lag = s.entries_added as i64 - cg.entries_read;
        valid = true;
    } else {
        let entries_read =
            stream_estimate_distance_from_first_ever_entry(s, &cg.last_id);
        if entries_read != SCG_INVALID_ENTRIES_READ {
            lag = s.entries_added as i64 - entries_read;
            valid = true;
        } else {
            lag = 0;
            valid = false;
        }
    }

    if valid {
        ctx.reply_integer(lag)
    } else {
        ctx.reply_null()
    }
}

/// Send stream entries in [start, end] to the client.
/// C source: `t_stream.c:1667-1795`, `streamReplyWithRange`.
/// TODO(port): iterator not yet implemented; returns 0.
/// TODO(architect): propagation via `also_propagate` blocked on Phase 3.
pub fn stream_reply_with_range(
    ctx: &mut CommandContext,
    s: &mut Stream,
    start: Option<StreamId>,
    end: Option<StreamId>,
    count: usize,
    rev: bool,
    group: Option<&mut StreamCG>,
    consumer: Option<&mut StreamConsumer>,
    flags: i32,
    spi: Option<&StreamPropInfo>,
) -> Result<usize, RedisError> {
    // TODO(port): implement full range iteration once Phase 4 iterator lands.
    if flags & STREAM_RWR_HISTORY != 0 {
        if let (Some(g), Some(c)) = (group, consumer) {
            return stream_reply_with_range_from_consumer_pel(ctx, s, start, end, count, c, g);
        }
    }
    let _ = (start, end, count, rev, spi);
    if flags & STREAM_RWR_RAWENTRIES == 0 {
        // TODO(architect): ctx.reply_deferred_len() not yet available.
    }
    Ok(0)
}

/// Reply with range from a consumer's pending entries list.
/// C source: `t_stream.c:1810-1848`, `streamReplyWithRangeFromConsumerPEL`.
/// TODO(port): requires working `stream_reply_with_range`; stub pending Phase 4.
pub fn stream_reply_with_range_from_consumer_pel(
    ctx: &mut CommandContext,
    s: &mut Stream,
    start: Option<StreamId>,
    end: Option<StreamId>,
    count: usize,
    consumer: &mut StreamConsumer,
    cg: &StreamCG,
) -> Result<usize, RedisError> {
    let start_key = start.map(|id| id.encode()).unwrap_or([0u8; 16]);
    let end_key = end.map(|id| id.encode());

    let mut arraylen = 0usize;
    // TODO(architect): ctx.reply_deferred_len() not yet available.
    for key in consumer.pel.range(start_key..) {
        if let Some(ref ek) = end_key {
            if key > ek {
                break;
            }
        }
        if count != 0 && arraylen >= count {
            break;
        }
        let id = StreamId::decode(key);
        // TODO(port): call stream_reply_with_range for the individual entry.
        arraylen += 1;
        // Update delivery time if NACK exists.
        if let Some(nack) = cg.pel.get(key) {
            // TODO(port): nack.delivery_time update requires mutable access
            // through the current shared-ownership model.
            let _ = nack;
        }
    }
    // TODO(architect): set_deferred_array_len
    Ok(arraylen)
}

// ─────────────────────────────────────────────────────────────────────────────
// ID parsing
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a stream ID from a byte slice.
/// The format is `<ms>-<seq>`, `<ms>-*`, `<ms>`, `-`, or `+`.
/// C source: `t_stream.c:1887-1940`, `streamGenericParseIDOrReply`.
pub fn stream_generic_parse_id(
    raw: &[u8],
    missing_seq: u64,
    strict: bool,
    seq_given_out: Option<&mut bool>,
) -> Result<StreamId, RedisError> {
    const ERR: &[u8] = b"ERR Invalid stream ID specified as stream command argument";

    if raw.len() >= 128 {
        return Err(RedisError::runtime(ERR));
    }

    if strict && (raw == b"-" || raw == b"+") {
        return Err(RedisError::runtime(ERR));
    }

    // Handle special IDs.
    if raw == b"-" {
        if let Some(sg) = seq_given_out { *sg = true; }
        return Ok(StreamId::zero());
    }
    if raw == b"+" {
        if let Some(sg) = seq_given_out { *sg = true; }
        return Ok(StreamId::max());
    }

    // Parse <ms>[-<seq>].
    let dash_pos = raw.iter().position(|&b| b == b'-');
    let ms_bytes = dash_pos.map(|p| &raw[..p]).unwrap_or(raw);

    let ms = parse_u64_bytes(ms_bytes).ok_or_else(|| RedisError::runtime(ERR))?;

    // Determine seq value and whether it was explicitly given.
    let (seq, seq_was_given): (u64, bool) = if let Some(pos) = dash_pos {
        let seq_bytes = &raw[pos + 1..];
        if seq_bytes == b"*" {
            // C: <ms>-* form — seq is auto-generated, seq_given = 0.
            (0u64, false)
        } else {
            let v = parse_u64_bytes(seq_bytes).ok_or_else(|| RedisError::runtime(ERR))?;
            (v, true)
        }
    } else {
        // No dash: use missing_seq and consider seq not explicitly given.
        (missing_seq, false)
    };

    // Write out the seq_given flag if the caller requested it.
    if let Some(sg) = seq_given_out {
        *sg = seq_was_given;
    }

    Ok(StreamId { ms, seq })
}

/// Parse a stream ID from a `RedisObject` argument (module API wrapper).
/// C source: `t_stream.c:1943-1945`, `streamParseID`.
pub fn stream_parse_id(o: &RedisObject) -> Result<StreamId, RedisError> {
    let bytes = object_get_bytes(o)?;
    stream_generic_parse_id(bytes, 0, false, None)
}

/// Parse a stream ID, accepting `-` and `+`.
/// C source: `t_stream.c:1949-1951`, `streamParseIDOrReply`.
pub fn stream_parse_id_or_reply(
    ctx: &mut CommandContext,
    o: &RedisObject,
    missing_seq: u64,
) -> Result<StreamId, RedisError> {
    let bytes = object_get_bytes(o)?;
    stream_generic_parse_id(bytes, missing_seq, false, None).map_err(|e| {
        // TODO(port): in C, addReplyError is called on the client.
        // The error is returned so the caller can propagate it.
        e
    })
}

/// Parse a stream ID, rejecting `-` and `+` (strict mode).
/// C source: `t_stream.c:1956-1958`, `streamParseStrictIDOrReply`.
pub fn stream_parse_strict_id_or_reply(
    ctx: &mut CommandContext,
    o: &RedisObject,
    missing_seq: u64,
    seq_given: Option<&mut bool>,
) -> Result<StreamId, RedisError> {
    let bytes = object_get_bytes(o)?;
    stream_generic_parse_id(bytes, missing_seq, true, seq_given)
}

/// Parse an interval endpoint, handling the `(` exclusive prefix.
/// C source: `t_stream.c:1966-1980`, `streamParseIntervalIDOrReply`.
pub fn stream_parse_interval_id_or_reply(
    ctx: &mut CommandContext,
    o: &RedisObject,
    missing_seq: u64,
) -> Result<(StreamId, bool), RedisError> {
    let bytes = object_get_bytes(o)?;
    if bytes.len() > 1 && bytes[0] == b'(' {
        let id = stream_generic_parse_id(&bytes[1..], missing_seq, true, None)?;
        Ok((id, true))
    } else {
        let id = stream_generic_parse_id(bytes, missing_seq, false, None)?;
        Ok((id, false))
    }
}

/// Parse XADD / XTRIM arguments.
/// C source: `t_stream.c:891-1019`, `streamParseAddOrTrimArgsOrReply`.
/// Returns the 0-based position of the ID argument (XADD), or 0 for XTRIM.
/// TODO(architect): ctx.arg(i) signature — blocked on CommandContext design.
/// TODO(architect): ctx.must_obey_client() — blocked on Phase 3.
pub fn stream_parse_add_or_trim_args_or_reply(
    ctx: &mut CommandContext,
    args: &mut StreamAddTrimArgs,
    xadd: bool,
) -> Result<i32, RedisError> {
    *args = StreamAddTrimArgs::default();
    let argc = ctx.argc();
    let mut limit_given = false;
    let mut i = 2i32;

    while i < argc as i32 {
        let moreargs = (argc as i32 - 1) - i;
        let opt = ctx.arg(i as usize)?;

        // Fast path for auto-ID in XADD.
        if xadd && opt == b"*" {
            break;
        }

        let opt_upper = to_upper_bytes(opt);
        if opt_upper == b"MAXLEN" && moreargs > 0 {
            if args.trim_strategy != TRIM_STRATEGY_NONE {
                return Err(RedisError::runtime(
                    b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible",
                ));
            }
            args.approx_trim = false;
            let next = ctx.arg((i + 1) as usize)?;
            if moreargs >= 2 && next == b"~" {
                args.approx_trim = true;
                i += 1;
            } else if moreargs >= 2 && next == b"=" {
                i += 1;
            }
            let count_bytes = ctx.arg((i + 1) as usize)?;
            args.maxlen = parse_i64_bytes(count_bytes)
                .ok_or_else(|| RedisError::not_integer())?;
            if args.maxlen < 0 {
                return Err(RedisError::runtime(b"ERR The MAXLEN argument must be >= 0."));
            }
            i += 1;
            args.trim_strategy = TRIM_STRATEGY_MAXLEN;
            args.trim_strategy_arg_idx = i;
        } else if opt_upper == b"MINID" && moreargs > 0 {
            if args.trim_strategy != TRIM_STRATEGY_NONE {
                return Err(RedisError::runtime(
                    b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible",
                ));
            }
            args.approx_trim = false;
            let next = ctx.arg((i + 1) as usize)?;
            if moreargs >= 2 && next == b"~" {
                args.approx_trim = true;
                i += 1;
            } else if moreargs >= 2 && next == b"=" {
                i += 1;
            }
            let id_arg_obj = ctx.arg_as_object((i + 1) as usize)?;
            args.minid = stream_parse_strict_id_or_reply(ctx, &id_arg_obj, 0, None)?;
            i += 1;
            args.trim_strategy = TRIM_STRATEGY_MINID;
            args.trim_strategy_arg_idx = i;
        } else if opt_upper == b"LIMIT" && moreargs > 0 {
            let limit_bytes = ctx.arg((i + 1) as usize)?;
            args.limit = parse_i64_bytes(limit_bytes)
                .ok_or_else(|| RedisError::not_integer())?;
            if args.limit < 0 {
                return Err(RedisError::runtime(b"ERR The LIMIT argument must be >= 0."));
            }
            limit_given = true;
            i += 1;
        } else if xadd && opt_upper == b"NOMKSTREAM" {
            args.no_mkstream = true;
        } else if xadd {
            let id_arg_obj = ctx.arg_as_object(i as usize)?;
            let mut seq_given = false;
            args.id = stream_parse_strict_id_or_reply(ctx, &id_arg_obj, 0, Some(&mut seq_given))?;
            args.id_given = true;
            args.seq_given = seq_given;
            break;
        } else {
            return Err(RedisError::syntax(b"ERR syntax error"));
        }
        i += 1;
    }

    if args.limit != 0 && args.trim_strategy == TRIM_STRATEGY_NONE {
        return Err(RedisError::runtime(
            b"ERR syntax error, LIMIT cannot be used without specifying a trimming strategy",
        ));
    }
    if !xadd && args.trim_strategy == TRIM_STRATEGY_NONE {
        return Err(RedisError::runtime(
            b"ERR syntax error, XTRIM must be called with a trimming strategy",
        ));
    }

    // TODO(architect): ctx.must_obey_client() check (AOF/replica bypass).
    if !limit_given {
        if args.approx_trim {
            // TODO(architect): 100 * stream_node_max_entries from server config.
            args.limit = 10000;
            if args.limit > 1_000_000 {
                args.limit = 1_000_000;
            }
        } else {
            args.limit = 0;
        }
    } else if !args.approx_trim {
        return Err(RedisError::runtime(
            b"ERR syntax error, LIMIT cannot be used without the special ~ option",
        ));
    }

    Ok(i)
}

// ─────────────────────────────────────────────────────────────────────────────
// Propagation helpers (Phase 3+ — all TODO(port))
// ─────────────────────────────────────────────────────────────────────────────

/// Propagate an XCLAIM command to AOF / replicas.
/// C source: `t_stream.c:1541-1571`, `streamPropagateXCLAIM`.
/// TODO(port): requires `also_propagate` + shared-object infrastructure.
pub fn stream_propagate_xclaim(
    ctx: &mut CommandContext,
    key: &RedisString,
    group: &StreamCG,
    groupname: &RedisString,
    id: &StreamId,
    nack: &StreamNACK,
) {
    // TODO(port): build XCLAIM argv and call ctx.also_propagate(...).
    let _ = (key, group, groupname, id, nack);
}

/// Propagate XGROUP SETID to AOF / replicas.
/// C source: `t_stream.c:1579-1593`, `streamPropagateGroupID`.
/// TODO(port): requires `also_propagate`.
pub fn stream_propagate_group_id(
    ctx: &mut CommandContext,
    key: &RedisString,
    group: &StreamCG,
    groupname: &RedisString,
) {
    // TODO(port): build XGROUP SETID argv and call ctx.also_propagate(...).
    let _ = (key, group, groupname);
}

/// Propagate XGROUP CREATECONSUMER to AOF / replicas.
/// C source: `t_stream.c:1601-1612`, `streamPropagateConsumerCreation`.
/// TODO(port): requires `also_propagate`.
pub fn stream_propagate_consumer_creation(
    ctx: &mut CommandContext,
    key: &RedisString,
    groupname: &RedisString,
    consumername: &RedisString,
) {
    // TODO(port): build XGROUP CREATECONSUMER argv and call ctx.also_propagate(...).
    let _ = (key, groupname, consumername);
}

// ─────────────────────────────────────────────────────────────────────────────
// DB lookup helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Look up the stream at `key`, creating it if missing (unless `no_create`).
/// Returns the mutable stream, or None if the key does not exist and no_create.
/// C source: `t_stream.c:1856-1868`, `streamTypeLookupWriteOrCreate`.
/// TODO(architect): RedisDb::lookup_key_write / db::add not yet integrated.
pub fn stream_type_lookup_write_or_create<'a>(
    ctx: &'a mut CommandContext,
    key: &[u8],
    no_create: bool,
) -> Result<Option<&'a mut Stream>, RedisError> {
    // TODO(architect): replace with ctx.db_mut().lookup_key_write(key) and
    // type-check for OBJ_STREAM.
    Err(RedisError::runtime(b"TODO stream_type_lookup_write_or_create not implemented"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Consumer group operations
// ─────────────────────────────────────────────────────────────────────────────

/// Create a new NACK with delivery count 1 and the current time.
/// C source: `t_stream.c:2455-2461`, `streamCreateNACK`.
/// TODO(architect): replace `now_ms` with `ctx.command_time_snapshot()`.
pub fn stream_create_nack(consumer_name: Option<RedisString>, now_ms: i64) -> StreamNACK {
    StreamNACK {
        delivery_time: now_ms,
        delivery_count: 1,
        consumer_name,
    }
}

/// Create a new consumer group in stream `s`.
/// Returns the new group or `None` if a group with that name already exists.
/// C source: `t_stream.c:2489-2499`, `streamCreateCG`.
pub fn stream_create_cg<'a>(
    s: &'a mut Stream,
    name: &[u8],
    id: StreamId,
    entries_read: i64,
) -> Option<&'a mut StreamCG> {
    let cgroups = s.cgroups.get_or_insert_with(BTreeMap::new);
    let key = name.to_vec();
    if cgroups.contains_key(&key) {
        return None;
    }
    cgroups.insert(key.clone(), StreamCG {
        last_id: id,
        entries_read,
        pel: BTreeMap::new(),
        consumers: BTreeMap::new(),
    });
    cgroups.get_mut(&key)
}

/// Free a consumer group (no-op in Rust; ownership handles cleanup).
/// C source: `t_stream.c:2503-2507`, `streamFreeCG`.
pub fn stream_free_cg(_cg: StreamCG) {}

/// Look up a consumer group by name.
/// C source: `t_stream.c:2516-2521`, `streamLookupCG`.
pub fn stream_lookup_cg<'a>(s: &'a Stream, groupname: &[u8]) -> Option<&'a StreamCG> {
    s.cgroups.as_ref()?.get(groupname)
}

/// Look up a consumer group by name (mutable).
pub fn stream_lookup_cg_mut<'a>(s: &'a mut Stream, groupname: &[u8]) -> Option<&'a mut StreamCG> {
    s.cgroups.as_mut()?.get_mut(groupname)
}

/// Create a consumer in the given group.
/// C source: `t_stream.c:2527-2543`, `streamCreateConsumer`.
/// TODO(architect): keyspace notification and dirty++ require ctx access.
pub fn stream_create_consumer<'a>(
    cg: &'a mut StreamCG,
    name: &[u8],
    flags: i32,
    now_ms: i64,
) -> Option<&'a mut StreamConsumer> {
    let key = name.to_vec();
    if cg.consumers.contains_key(&key) {
        return None;
    }
    cg.consumers.insert(key.clone(), StreamConsumer {
        name: RedisString::from_bytes(name),
        seen_time: now_ms,
        active_time: -1,
        pel: BTreeSet::new(),
    });
    // TODO(architect): if !(flags & SCC_NO_NOTIFY) → notify keyspace event.
    // TODO(architect): if !(flags & SCC_NO_DIRTIFY) → server.dirty++.
    cg.consumers.get_mut(&key)
}

/// Look up a consumer by name.
/// C source: `t_stream.c:2547-2552`, `streamLookupConsumer`.
pub fn stream_lookup_consumer<'a>(cg: &'a StreamCG, name: &[u8]) -> Option<&'a StreamConsumer> {
    cg.consumers.get(name)
}

/// Look up a consumer by name (mutable).
pub fn stream_lookup_consumer_mut<'a>(
    cg: &'a mut StreamCG,
    name: &[u8],
) -> Option<&'a mut StreamConsumer> {
    cg.consumers.get_mut(name)
}

/// Delete a consumer: remove its PEL entries from the group PEL and drop it.
/// C source: `t_stream.c:2555-2571`, `streamDelConsumer`.
pub fn stream_del_consumer(cg: &mut StreamCG, consumer_name: &[u8]) {
    if let Some(consumer) = cg.consumers.remove(consumer_name) {
        // Remove all consumer PEL entries from the group PEL.
        for key in &consumer.pel {
            cg.pel.remove(key);
        }
        // consumer drops here (and its pel BTreeSet).
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stream commands
// ─────────────────────────────────────────────────────────────────────────────

/// XADD key [(MAXLEN|MINID) [~|=] count] [NOMKSTREAM] <id|*> field value ...
/// C source: `t_stream.c:2004-2088`, `xaddCommand`.
/// TODO(architect): ctx.db_mut(), ctx.server_dirty_incr(), ctx.notify_keyspace_event(),
///   ctx.signal_modified_key(), ctx.signal_key_as_ready() — all Phase 3.
/// TODO(architect): ctx.command_time_snapshot() for current_ms.
pub fn xadd_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut parsed_args = StreamAddTrimArgs::default();
    let idpos = stream_parse_add_or_trim_args_or_reply(ctx, &mut parsed_args, true)?;

    let field_pos = idpos + 1;
    let argc = ctx.argc() as i32;

    if (argc - field_pos) < 2 || ((argc - field_pos) % 2) == 1 {
        return Err(RedisError::wrong_number_of_args(b"XADD"));
    }

    // Guard against 0-0 explicit ID.
    if parsed_args.id_given && parsed_args.seq_given
        && parsed_args.id.ms == 0 && parsed_args.id.seq == 0
    {
        return Err(RedisError::runtime(
            b"ERR The ID specified in XADD must be greater than 0-0",
        ));
    }

    let key = ctx.arg(1)?.clone();
    let _s = stream_type_lookup_write_or_create(ctx, key.as_slice(), parsed_args.no_mkstream)?;

    // TODO(port): stream_append_item call once DB integration is available.
    // TODO(architect): reply with the generated ID.
    Err(RedisError::runtime(b"TODO xadd_command not fully implemented"))
}

/// XRANGE/XREVRANGE shared implementation.
/// C source: `t_stream.c:2097-2144`, `xrangeGenericCommand`.
/// TODO(architect): DB lookup (lookupKeyReadOrReply) — Phase 3.
pub fn xrange_generic_command(ctx: &mut CommandContext, rev: bool) -> Result<(), RedisError> {
    let start_arg_idx = if rev { 3 } else { 2 };
    let end_arg_idx = if rev { 2 } else { 3 };

    let start_obj = ctx.arg_as_object(start_arg_idx)?;
    let (mut start_id, start_ex) =
        stream_parse_interval_id_or_reply(ctx, &start_obj, 0)?;
    if start_ex {
        start_id.incr().map_err(|_| {
            RedisError::runtime(b"ERR invalid start ID for the interval")
        })?;
    }

    let end_obj = ctx.arg_as_object(end_arg_idx)?;
    let (mut end_id, end_ex) =
        stream_parse_interval_id_or_reply(ctx, &end_obj, u64::MAX)?;
    if end_ex {
        end_id.decr().map_err(|_| {
            RedisError::runtime(b"ERR invalid end ID for the interval")
        })?;
    }

    let argc = ctx.argc() as i32;
    let mut count: i64 = -1;
    if argc > 4 {
        let mut j = 4i32;
        while j < argc {
            let additional = argc - j - 1;
            let opt = ctx.arg(j as usize)?;
            if to_upper_bytes(opt) == b"COUNT" && additional >= 1 {
                let cnt_bytes = ctx.arg((j + 1) as usize)?;
                count = parse_i64_bytes(cnt_bytes).ok_or_else(|| RedisError::not_integer())?;
                if count < 0 { count = 0; }
                j += 1;
            } else {
                return Err(RedisError::syntax(b"ERR syntax error"));
            }
            j += 1;
        }
    }

    // TODO(architect): DB lookup (lookupKeyReadOrReply) — Phase 3.
    // TODO(port): call stream_reply_with_range once DB integration is ready.
    let _ = (start_id, end_id, count, rev);
    Err(RedisError::runtime(b"TODO xrange_generic_command not fully implemented"))
}

/// XRANGE key start end [COUNT n]
/// C source: `t_stream.c:2147-2149`, `xrangeCommand`.
pub fn xrange_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    xrange_generic_command(ctx, false)
}

/// XREVRANGE key end start [COUNT n]
/// C source: `t_stream.c:2152-2154`, `xrevrangeCommand`.
pub fn xrevrange_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    xrange_generic_command(ctx, true)
}

/// XLEN key
/// C source: `t_stream.c:2157-2162`, `xlenCommand`.
/// TODO(architect): DB lookup — Phase 3.
pub fn xlen_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    // TODO(architect): lookup stream from ctx.db() and reply with s.length.
    Err(RedisError::runtime(b"TODO xlen_command not fully implemented"))
}

/// XREAD [BLOCK ms] [COUNT n] STREAMS key... id...
/// Also implements XREADGROUP when GROUP option is present.
/// C source: `t_stream.c:2172-2446`, `xreadCommand`.
/// TODO(architect): blocking I/O (blockForKeys), DB lookups, consumer group
///   management — all Phase 3+.
pub fn xread_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    let mut timeout: i64 = -1;
    let mut count: i64 = 0;
    let mut streams_arg: usize = 0;
    let mut streams_count: usize = 0;
    let mut noack = false;
    let mut groupname: Option<RedisString> = None;
    let mut consumername: Option<RedisString> = None;

    // Detect XREADGROUP by checking command name length == 10 ("XREADGROUP").
    let is_xreadgroup = ctx.arg(0).map(|b| b.len() == 10).unwrap_or(false);

    let mut i = 1usize;
    while i < argc {
        let moreargs = argc - i - 1;
        let opt = ctx.arg(i)?;
        let opt_up = to_upper_bytes(opt);
        if opt_up == b"BLOCK" && moreargs >= 1 {
            i += 1;
            let t = ctx.arg(i)?;
            timeout = parse_i64_bytes(t).ok_or_else(|| RedisError::not_integer())?;
            if timeout < 0 { timeout = 0; }
        } else if opt_up == b"COUNT" && moreargs >= 1 {
            i += 1;
            let c = ctx.arg(i)?;
            count = parse_i64_bytes(c).ok_or_else(|| RedisError::not_integer())?;
            if count < 0 { count = 0; }
        } else if opt_up == b"STREAMS" && moreargs >= 1 {
            streams_arg = i + 1;
            streams_count = argc - streams_arg;
            if streams_count % 2 != 0 {
                return Err(RedisError::runtime(
                    b"ERR Unbalanced STREAMS list of streams",
                ));
            }
            streams_count /= 2;
            break;
        } else if opt_up == b"GROUP" && moreargs >= 2 {
            if !is_xreadgroup {
                return Err(RedisError::runtime(
                    b"ERR The GROUP option is only supported by XREADGROUP",
                ));
            }
            let gn = ctx.arg(i + 1)?;
            let cn = ctx.arg(i + 2)?;
            groupname = Some(RedisString::from_bytes(gn));
            consumername = Some(RedisString::from_bytes(cn));
            i += 2;
        } else if opt_up == b"NOACK" {
            if !is_xreadgroup {
                return Err(RedisError::runtime(
                    b"ERR The NOACK option is only supported by XREADGROUP",
                ));
            }
            noack = true;
        } else {
            return Err(RedisError::syntax(b"ERR syntax error"));
        }
        i += 1;
    }

    if streams_arg == 0 {
        return Err(RedisError::syntax(b"ERR syntax error"));
    }
    if is_xreadgroup && groupname.is_none() {
        return Err(RedisError::runtime(b"ERR Missing GROUP option for XREADGROUP"));
    }

    // TODO(architect): DB lookups, consumer group resolution, blocking I/O —
    // all require Phase 3 infrastructure.
    let _ = (timeout, count, noack, groupname, consumername, streams_arg, streams_count);
    Err(RedisError::runtime(b"TODO xread_command not fully implemented"))
}

/// XGROUP CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER
/// C source: `t_stream.c:2582-2736`, `xgroupCommand`.
/// TODO(architect): DB lookup, dirty++, keyspace notification — Phase 3.
pub fn xgroup_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let opt = ctx.arg(1)?;
    let opt_up = to_upper_bytes(opt);

    if ctx.argc() == 2 && opt_up == b"HELP" {
        // TODO(architect): ctx.reply_help(lines) — Phase 3.
        return Err(RedisError::runtime(b"TODO XGROUP HELP not implemented"));
    }

    if ctx.argc() < 4 {
        return Err(RedisError::wrong_number_of_args(b"XGROUP"));
    }

    // Parse optional MKSTREAM / ENTRIESREAD for CREATE/SETID.
    let mut mkstream = false;
    let mut entries_read: i64 = SCG_INVALID_ENTRIES_READ;
    let is_create = opt_up == b"CREATE";
    let is_setid = opt_up == b"SETID";
    let mut j = 5usize;
    while j < ctx.argc() {
        let subopt = ctx.arg(j)?;
        let subopt_up = to_upper_bytes(subopt);
        if is_create && subopt_up == b"MKSTREAM" {
            mkstream = true;
            j += 1;
        } else if (is_create || is_setid) && subopt_up == b"ENTRIESREAD" && j + 1 < ctx.argc() {
            let er = ctx.arg(j + 1)?;
            entries_read = parse_i64_bytes(er).ok_or_else(|| RedisError::not_integer())?;
            if entries_read < 0 && entries_read != SCG_INVALID_ENTRIES_READ {
                return Err(RedisError::runtime(
                    b"ERR value for ENTRIESREAD must be positive or -1",
                ));
            }
            j += 2;
        } else {
            return Err(RedisError::syntax(b"ERR syntax error"));
        }
    }

    // TODO(architect): DB lookup, stream/group access, notifications — Phase 3.
    let _ = (mkstream, entries_read);
    Err(RedisError::runtime(b"TODO xgroup_command not fully implemented"))
}

/// XSETID stream id [ENTRIESADDED n] [MAXDELETEDID id]
/// C source: `t_stream.c:2742-2808`, `xsetidCommand`.
/// TODO(architect): DB lookup — Phase 3.
pub fn xsetid_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let id_obj = ctx.arg_as_object(2)?;
    let mut id = stream_parse_strict_id_or_reply(ctx, &id_obj, 0, None)?;
    let mut max_xdel_id = StreamId::zero();
    let mut entries_added: i64 = -1;

    let mut i = 3usize;
    while i < ctx.argc() {
        let moreargs = ctx.argc() - i - 1;
        let opt = ctx.arg(i)?;
        let opt_up = to_upper_bytes(opt);
        if opt_up == b"ENTRIESADDED" && moreargs >= 1 {
            let ea = ctx.arg(i + 1)?;
            entries_added = parse_i64_bytes(ea).ok_or_else(|| RedisError::not_integer())?;
            if entries_added < 0 {
                return Err(RedisError::runtime(b"ERR entries_added must be positive"));
            }
            i += 2;
        } else if opt_up == b"MAXDELETEDID" && moreargs >= 1 {
            let mdid_obj = ctx.arg_as_object(i + 1)?;
            max_xdel_id = stream_parse_strict_id_or_reply(ctx, &mdid_obj, 0, None)?;
            if id.compare(&max_xdel_id) < 0 {
                return Err(RedisError::runtime(
                    b"ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
                ));
            }
            i += 2;
        } else {
            return Err(RedisError::syntax(b"ERR syntax error"));
        }
    }

    // TODO(architect): DB lookup (lookupKeyWriteOrReply), update s.last_id,
    // s.entries_added, s.max_deleted_entry_id, server.dirty++, notify. Phase 3.
    let _ = (id, max_xdel_id, entries_added);
    Err(RedisError::runtime(b"TODO xsetid_command not fully implemented"))
}

/// XACK key group id [id ...]
/// C source: `t_stream.c:2818-2865`, `xackCommand`.
/// TODO(architect): DB lookup, dirty++ — Phase 3.
pub fn xack_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    if argc < 4 {
        return Err(RedisError::wrong_number_of_args(b"XACK"));
    }

    // Parse all IDs upfront (all-or-nothing).
    let id_count = argc - 3;
    let mut ids: Vec<StreamId> = Vec::with_capacity(id_count);
    for j in 3..argc {
        let id_obj = ctx.arg_as_object(j)?;
        ids.push(stream_parse_strict_id_or_reply(ctx, &id_obj, 0, None)?);
    }

    // TODO(architect): DB lookup → group → remove from group.pel and
    // consumer.pel → server.dirty++ → reply with acknowledged count. Phase 3.
    let _ = ids;
    Err(RedisError::runtime(b"TODO xack_command not fully implemented"))
}

/// XPENDING key group [[IDLE idle] start stop count [consumer]]
/// C source: `t_stream.c:2876-3043`, `xpendingCommand`.
/// TODO(architect): DB lookup — Phase 3.
pub fn xpending_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    let just_info = argc == 3;

    if argc != 3 && (argc < 6 || argc > 9) {
        return Err(RedisError::syntax(b"ERR syntax error"));
    }

    let mut start_id = StreamId::zero();
    let mut end_id = StreamId::max();
    let mut count: i64 = 0;
    let mut minidle: i64 = 0;
    let mut consumername: Option<RedisString> = None;

    if argc >= 6 {
        let mut start_idx = 3usize;
        if to_upper_bytes(ctx.arg(3)?) == b"IDLE" {
            let idle = ctx.arg(4)?;
            minidle = parse_i64_bytes(idle).ok_or_else(|| RedisError::not_integer())?;
            if argc < 8 {
                return Err(RedisError::syntax(b"ERR syntax error"));
            }
            start_idx += 2;
        }

        // count
        let cnt = ctx.arg(start_idx + 2)?;
        count = parse_i64_bytes(cnt).ok_or_else(|| RedisError::not_integer())?;
        if count < 0 { count = 0; }

        // start / end
        let start_obj = ctx.arg_as_object(start_idx)?;
        let (mut sid, sex) = stream_parse_interval_id_or_reply(ctx, &start_obj, 0)?;
        if sex {
            sid.incr().map_err(|_| RedisError::runtime(b"ERR invalid start ID"))?;
        }
        start_id = sid;

        let end_obj = ctx.arg_as_object(start_idx + 1)?;
        let (mut eid, eex) = stream_parse_interval_id_or_reply(ctx, &end_obj, u64::MAX)?;
        if eex {
            eid.decr().map_err(|_| RedisError::runtime(b"ERR invalid end ID"))?;
        }
        end_id = eid;

        if start_idx + 3 < argc {
            let cn = ctx.arg(start_idx + 3)?;
            consumername = Some(RedisString::from_bytes(cn));
        }
    }

    // TODO(architect): DB lookup → group PEL iteration → reply. Phase 3.
    let _ = (just_info, start_id, end_id, count, minidle, consumername);
    Err(RedisError::runtime(b"TODO xpending_command not fully implemented"))
}

/// XCLAIM key group consumer min-idle-time id... [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID]
/// C source: `t_stream.c:3111-3313`, `xclaimCommand`.
/// TODO(architect): DB lookup, also_propagate — Phase 3.
pub fn xclaim_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    if argc < 6 {
        return Err(RedisError::wrong_number_of_args(b"XCLAIM"));
    }

    let minidle_arg = ctx.arg(4)?;
    let mut minidle = parse_i64_bytes(minidle_arg).ok_or_else(|| RedisError::not_integer())?;
    if minidle < 0 { minidle = 0; }

    // Parse IDs (stopping at options).
    let mut ids: Vec<StreamId> = Vec::new();
    let mut j = 5usize;
    while j < argc {
        let id_obj = ctx.arg_as_object(j)?;
        match stream_parse_strict_id_or_reply(ctx, &id_obj, 0, None) {
            Ok(id) => { ids.push(id); j += 1; }
            Err(_) => break,
        }
    }
    let last_id_arg = j.saturating_sub(1);

    // Parse options.
    let mut retrycount: i64 = -1;
    let mut deliverytime: i64 = -1;
    let mut force = false;
    let mut justid = false;
    let mut last_id = StreamId::zero();

    // TODO(architect): current_ms from ctx.command_time_snapshot().
    let now: i64 = 0; // placeholder
    while j < argc {
        let moreargs = argc - j - 1;
        let opt = ctx.arg(j)?;
        let opt_up = to_upper_bytes(opt);
        if opt_up == b"FORCE" {
            force = true;
        } else if opt_up == b"JUSTID" {
            justid = true;
        } else if opt_up == b"IDLE" && moreargs >= 1 {
            let v = ctx.arg(j + 1)?;
            deliverytime = now - parse_i64_bytes(v).ok_or_else(|| RedisError::not_integer())?;
            j += 1;
        } else if opt_up == b"TIME" && moreargs >= 1 {
            let v = ctx.arg(j + 1)?;
            deliverytime = parse_i64_bytes(v).ok_or_else(|| RedisError::not_integer())?;
            j += 1;
        } else if opt_up == b"RETRYCOUNT" && moreargs >= 1 {
            let v = ctx.arg(j + 1)?;
            retrycount = parse_i64_bytes(v).ok_or_else(|| RedisError::not_integer())?;
            j += 1;
        } else if opt_up == b"LASTID" && moreargs >= 1 {
            let lid_obj = ctx.arg_as_object(j + 1)?;
            last_id = stream_parse_strict_id_or_reply(ctx, &lid_obj, 0, None)?;
            j += 1;
        } else {
            return Err(RedisError::runtime(b"ERR Unrecognized XCLAIM option"));
        }
        j += 1;
    }

    // TODO(architect): DB lookup, claim loop, propagation. Phase 3.
    let _ = (minidle, ids, last_id_arg, retrycount, deliverytime, force, justid, last_id);
    Err(RedisError::runtime(b"TODO xclaim_command not fully implemented"))
}

/// XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID]
/// C source: `t_stream.c:3331-3497`, `xautoclaimCommand`.
/// TODO(architect): DB lookup, claim loop, propagation. Phase 3.
pub fn xautoclaim_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    if argc < 6 {
        return Err(RedisError::wrong_number_of_args(b"XAUTOCLAIM"));
    }

    let minidle_arg = ctx.arg(4)?;
    let mut minidle = parse_i64_bytes(minidle_arg).ok_or_else(|| RedisError::not_integer())?;
    if minidle < 0 { minidle = 0; }

    let start_obj = ctx.arg_as_object(5)?;
    let (mut start_id, startex) = stream_parse_interval_id_or_reply(ctx, &start_obj, 0)?;
    if startex {
        start_id.incr().map_err(|_| RedisError::runtime(b"ERR invalid start ID"))?;
    }

    let mut count: i64 = 100;
    let mut justid = false;
    let mut j = 6usize;
    while j < argc {
        let moreargs = argc - j - 1;
        let opt = ctx.arg(j)?;
        let opt_up = to_upper_bytes(opt);
        if opt_up == b"COUNT" && moreargs >= 1 {
            let v = ctx.arg(j + 1)?;
            count = parse_i64_bytes(v).ok_or_else(|| RedisError::not_integer())?;
            if count <= 0 {
                return Err(RedisError::runtime(b"ERR COUNT must be > 0"));
            }
            j += 1;
        } else if opt_up == b"JUSTID" {
            justid = true;
        } else {
            return Err(RedisError::syntax(b"ERR syntax error"));
        }
        j += 1;
    }

    // TODO(architect): DB lookup, autoclaim loop, reply[0..2]. Phase 3.
    let _ = (minidle, start_id, count, justid);
    Err(RedisError::runtime(b"TODO xautoclaim_command not fully implemented"))
}

/// XDEL key id [id ...]
/// C source: `t_stream.c:3504-3559`, `xdelCommand`.
/// TODO(architect): DB lookup, dirty++, notify. Phase 3.
pub fn xdel_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"XDEL"));
    }

    // Parse all IDs upfront (all-or-nothing).
    let id_count = argc - 2;
    let mut ids: Vec<StreamId> = Vec::with_capacity(id_count);
    for j in 2..argc {
        let id_obj = ctx.arg_as_object(j)?;
        ids.push(stream_parse_strict_id_or_reply(ctx, &id_obj, 0, None)?);
    }

    // TODO(architect): DB lookup → stream_delete_item for each ID →
    // update s.max_deleted_entry_id, s.first_id → signal/notify. Phase 3.
    let _ = ids;
    Err(RedisError::runtime(b"TODO xdel_command not fully implemented"))
}

/// XTRIM key MAXLEN|MINID [~|=] count [LIMIT n]
/// C source: `t_stream.c:3584-3616`, `xtrimCommand`.
/// TODO(architect): DB lookup, dirty++, notify. Phase 3.
pub fn xtrim_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let mut parsed_args = StreamAddTrimArgs::default();
    let ret = stream_parse_add_or_trim_args_or_reply(ctx, &mut parsed_args, false);
    if ret.is_err() {
        return ret.map(|_| ());
    }

    // TODO(architect): DB lookup → stream_trim(s, &parsed_args) →
    // rewrite approx specifier → signal/notify → dirty++. Phase 3.
    let _ = parsed_args;
    Err(RedisError::runtime(b"TODO xtrim_command not fully implemented"))
}

/// XINFO CONSUMERS|GROUPS|STREAM|HELP
/// C source: `t_stream.c:3825-3927`, `xinfoCommand`.
/// TODO(architect): DB lookup, map/array reply helpers. Phase 3.
pub fn xinfo_command(ctx: &mut CommandContext) -> Result<(), RedisError> {
    let argc = ctx.argc();
    if argc < 2 {
        return Err(RedisError::wrong_number_of_args(b"XINFO"));
    }
    let opt = ctx.arg(1)?;
    let opt_up = to_upper_bytes(opt);

    if opt_up == b"HELP" {
        // TODO(architect): ctx.reply_help(lines). Phase 3.
        return Err(RedisError::runtime(b"TODO XINFO HELP not implemented"));
    }
    if argc < 3 {
        return Err(RedisError::wrong_number_of_args(b"XINFO"));
    }
    // TODO(architect): DB lookup, dispatch CONSUMERS/GROUPS/STREAM. Phase 3.
    Err(RedisError::runtime(b"TODO xinfo_command not fully implemented"))
}

/// Validate the integrity of a stream listpack.
/// C source: `t_stream.c:3932-4029`, `streamValidateListpackIntegrity`.
/// TODO(port): requires Phase 4 listpack validation API.
pub fn stream_validate_listpack_integrity(lp: &[u8], deep: bool) -> bool {
    // TODO(port): implement full structural validation of the listpack binary
    // format once Phase 4 ListPack is available.
    if lp.is_empty() {
        return false;
    }
    if !deep {
        return true; // shallow: just check non-empty
    }
    false // conservative: always fail deep validation until implemented
}

// ─────────────────────────────────────────────────────────────────────────────
// Private byte-parsing helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a `u64` from an ASCII decimal byte slice. Returns `None` on error.
fn parse_u64_bytes(b: &[u8]) -> Option<u64> {
    if b.is_empty() || b.len() > 20 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        if c < b'0' || c > b'9' {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
    }
    Some(v)
}

/// Parse an `i64` from an ASCII decimal byte slice (with optional `-` prefix).
fn parse_i64_bytes(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let (neg, digits) = if b[0] == b'-' { (true, &b[1..]) } else { (false, b) };
    let u = parse_u64_bytes(digits)?;
    if neg {
        if u > i64::MAX as u64 + 1 { return None; }
        Some(-(u as i64))
    } else {
        if u > i64::MAX as u64 { return None; }
        Some(u as i64)
    }
}

/// Return an ASCII-uppercased copy of a byte slice (ASCII only, heap-allocated).
/// PERF(port): allocates; consider stack buffer for short option names.
fn to_upper_bytes(b: &[u8]) -> Vec<u8> {
    b.iter().map(|c| c.to_ascii_uppercase()).collect()
}

/// Extract the raw bytes from a `RedisObject::String`.
/// TODO(architect): RedisObject::String(RedisString) byte accessor.
fn object_get_bytes(o: &RedisObject) -> Result<&[u8], RedisError> {
    match o {
        RedisObject::String(s) => Ok(s.as_bytes()),
        _ => Err(RedisError::wrong_type()),
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/t_stream.c  (4029 lines, ~54 functions)
//                  + src/stream.h  (161 lines, type declarations)
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         112  (45 TODO(port) + 67 TODO(architect))
//   port_notes:    9
//   unsafe_blocks: 0
//   notes:         Phase A skeleton.  StreamId/compare/encode/decode/parse
//                  fully translated.  stream_append_item bookkeeping translated;
//                  listpack encoding stubbed (TODO(port)).  stream_trim exact
//                  logic translated; approximate (node-level) trim is a
//                  placeholder.  All command arg-parsing faithfully translated;
//                  DB lookups, dirty counters, keyspace notifications, and
//                  propagation stubs require Phase 3 CommandContext extensions
//                  (TODO(architect)).  StreamIterator traversal stubs pending
//                  Phase 4 ListPack API (TODO(port)).  Consumer-group NACKs use
//                  index-based model instead of C's pointer aliasing; ownership
//                  model needs architect decision (TODO(architect)).
// ──────────────────────────────────────────────────────────────────────────
