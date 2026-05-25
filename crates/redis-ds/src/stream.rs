//! `StreamId` + Phase-B inline stream storage.
//!
//! Source: `reference/valkey/src/stream.h` (paired with `t_stream.c`).
//!
//! A stream ID is a 128-bit composite: the upper 64 bits are a
//! millisecond unix timestamp, the lower 64 bits a per-ms sequence
//! counter. IDs are totally ordered and printed as `<ms>-<seq>` on the
//! wire.
//!
//! Round 9 introduces a pragmatic `InlineStream` storage that mirrors
//! the LIST/HASH/SET/ZSET inline encodings: entries kept in a sorted
//! `Vec<StreamEntry>` for binary-searchable range queries. Phase 5
//! replaces this with a real radix-tree + listpack representation.

use redis_types::RedisString;
use std::collections::HashMap;

/// 128-bit composite stream identifier `(ms, seq)`.
///
/// Ordering is lexicographic on `(ms, seq)` and the wire format is
/// `<ms>-<seq>`. `parse` accepts `<ms>` (seq defaults), `<ms>-<seq>`,
/// `-` (alias for the minimum sentinel used by XRANGE), and `+` (alias
/// for the maximum sentinel).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    pub const fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// Format as `<ms>-<seq>` into a freshly allocated `String`.
    pub fn to_display_string(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }

    /// Return the bytes of `<ms>-<seq>` for direct use in RESP bulk replies.
    pub fn to_display_bytes(&self) -> Vec<u8> {
        self.to_display_string().into_bytes()
    }

    /// Increment by one with wrap-around to the next `(ms, 0)` slot.
    /// Returns `None` if the id is already `MAX`.
    pub fn checked_succ(&self) -> Option<StreamId> {
        if self.seq == u64::MAX {
            if self.ms == u64::MAX {
                return None;
            }
            Some(StreamId {
                ms: self.ms + 1,
                seq: 0,
            })
        } else {
            Some(StreamId {
                ms: self.ms,
                seq: self.seq + 1,
            })
        }
    }

    /// Decrement by one with wrap-around to the previous `(ms-1, MAX)` slot.
    /// Returns `None` if the id is already `ZERO`.
    pub fn checked_pred(&self) -> Option<StreamId> {
        if self.seq == 0 {
            if self.ms == 0 {
                return None;
            }
            Some(StreamId {
                ms: self.ms - 1,
                seq: u64::MAX,
            })
        } else {
            Some(StreamId {
                ms: self.ms,
                seq: self.seq - 1,
            })
        }
    }
}

/// Errors returned by `StreamId::parse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamIdParseError {
    /// Input was empty or contained non-ASCII-digit characters in the
    /// `<ms>` or `<seq>` slots.
    Malformed,
}

/// Parse an ID literal `<ms>[-<seq>]` where missing seq is treated as
/// the `default_seq` value (typically 0 for start-of-range and
/// `u64::MAX` for end-of-range).
///
/// This is the building block; the higher-level XRANGE parser handles
/// the `-` / `+` / `(` prefixes on top.
pub fn parse_stream_id(input: &[u8], default_seq: u64) -> Result<StreamId, StreamIdParseError> {
    if input.is_empty() {
        return Err(StreamIdParseError::Malformed);
    }
    let s = core::str::from_utf8(input).map_err(|_| StreamIdParseError::Malformed)?;
    match s.find('-') {
        None => {
            let ms = s
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::Malformed)?;
            Ok(StreamId {
                ms,
                seq: default_seq,
            })
        }
        Some(idx) => {
            let (ms_part, dash_seq) = s.split_at(idx);
            let seq_part = &dash_seq[1..];
            if ms_part.is_empty() || seq_part.is_empty() {
                return Err(StreamIdParseError::Malformed);
            }
            let ms = ms_part
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::Malformed)?;
            let seq = seq_part
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::Malformed)?;
            Ok(StreamId { ms, seq })
        }
    }
}

/// A single stream entry: id plus an ordered list of `(field, value)`
/// pairs. Field order is preserved so that `XRANGE` output is
/// deterministic.
#[derive(Clone, Debug)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: Vec<(RedisString, RedisString)>,
}

/// One entry in a Pending Entry List (PEL).
///
/// Records the entry id, when it was last delivered (unix ms) and how
/// many times it has been delivered. Group-level and per-consumer PELs
/// hold separate `PelEntry` copies that are kept in lock-step.
#[derive(Clone, Debug)]
pub struct PelEntry {
    pub entry_id: StreamId,
    pub delivery_time_ms: i64,
    pub delivery_count: u64,
}

/// One consumer inside a consumer group.
///
/// `pel` is sorted by `entry_id` ascending. `seen_time_ms` updates on any
/// XREADGROUP/XCLAIM touch (even with no entries delivered); `active_time_ms`
/// only updates when at least one entry was delivered or claimed.
#[derive(Clone, Debug)]
pub struct Consumer {
    pub name: RedisString,
    pub seen_time_ms: i64,
    pub active_time_ms: i64,
    pub pel: Vec<PelEntry>,
}

/// One consumer group.
///
/// `pel` is the group-wide PEL: it is the union of the per-consumer PELs
/// and is also sorted by `entry_id`. Helpers on `InlineStream` keep the
/// two views consistent so callers can scan either side cheaply.
/// Sentinel for a consumer group whose logical read counter is unknown, e.g.
/// a group created behind existing entries without `ENTRIESREAD`, or one whose
/// position is fragmented by a tombstone. Mirrors `SCG_INVALID_ENTRIES_READ`
/// (stream.h:114). Reported to clients as a null `entries-read`/`lag`.
pub const SCG_INVALID_ENTRIES_READ: i64 = -1;

#[derive(Clone, Debug)]
pub struct ConsumerGroup {
    pub name: RedisString,
    pub last_delivered_id: StreamId,
    pub consumers: HashMap<RedisString, Consumer>,
    pub pel: Vec<PelEntry>,
    pub entries_read: i64,
}

/// Phase-B inline stream storage.
///
/// `entries` is kept sorted by `id` ascending and is therefore
/// binary-searchable. `last_id` is the largest id ever inserted; this
/// is what auto-id generation (`*`) compares against and is preserved
/// across delete/trim operations to match real Redis semantics.
///
/// Approximate trim behavior is implemented above this storage layer by the
/// command crate. The inline store deliberately exposes only ordered entries
/// plus stream counters; it does not pretend to model upstream radix-tree
/// listpack node boundaries.
#[derive(Clone, Debug, Default)]
pub struct InlineStream {
    pub entries: Vec<StreamEntry>,
    pub last_id: StreamId,
    pub max_deleted_id: StreamId,
    pub entries_added: u64,
    pub groups: HashMap<RedisString, ConsumerGroup>,
}

impl InlineStream {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Index of the first entry with `id >= target`.
    pub fn lower_bound(&self, target: &StreamId) -> usize {
        self.entries.partition_point(|e| e.id < *target)
    }

    /// Index of the first entry with `id > target`.
    pub fn upper_bound(&self, target: &StreamId) -> usize {
        self.entries.partition_point(|e| e.id <= *target)
    }

    /// Append `entry` whose id must be strictly greater than `last_id`.
    /// Updates `last_id` and `entries_added` on success.
    pub fn append(&mut self, entry: StreamEntry) {
        self.last_id = entry.id;
        self.entries_added += 1;
        self.entries.push(entry);
    }

    /// Delete the entry with id `target`. Returns `true` if removed.
    pub fn delete(&mut self, target: &StreamId) -> bool {
        let idx = self.lower_bound(target);
        if idx < self.entries.len() && self.entries[idx].id == *target {
            self.entries.remove(idx);
            if *target > self.max_deleted_id {
                self.max_deleted_id = *target;
            }
            return true;
        }
        false
    }

    /// ID of the first surviving entry, or `0-0` for an empty stream.
    /// Mirrors `stream->first_id` after `streamGetEdgeID(s, 1, 1, ...)`.
    pub fn first_id(&self) -> StreamId {
        self.entries.first().map(|e| e.id).unwrap_or(StreamId::ZERO)
    }

    /// Snapshot the stream-level scalars consumer-group lag math depends on.
    ///
    /// Taken before mutating a group so the read counter can be advanced in
    /// place without aliasing the stream (the snapshot is `Copy`).
    pub fn lag_view(&self) -> StreamLagView {
        StreamLagView {
            entries_added: self.entries_added,
            length: self.entries.len() as u64,
            last_id: self.last_id,
            first_id: self.first_id(),
            max_deleted_id: self.max_deleted_id,
        }
    }
}

/// A `Copy` snapshot of the stream-level scalars used to compute a consumer
/// group's `entries-read` and `lag`. Mirrors the inputs of
/// `streamEstimateDistanceFromFirstEverEntry`, `streamRangeHasTombstones`, and
/// `streamReplyWithCGLag` (t_stream.c).
#[derive(Clone, Copy, Debug)]
pub struct StreamLagView {
    pub entries_added: u64,
    pub length: u64,
    pub last_id: StreamId,
    pub first_id: StreamId,
    pub max_deleted_id: StreamId,
}

impl StreamLagView {
    /// Port of `streamEstimateDistanceFromFirstEverEntry` (t_stream.c:1494):
    /// the logical read counter of `id`, or `SCG_INVALID_ENTRIES_READ` when it
    /// cannot be determined.
    pub fn estimate_entries_read(&self, id: StreamId) -> i64 {
        if self.entries_added == 0 {
            return 0;
        }

        let entries_added = self.entries_added as i64;
        let length = self.length as i64;

        if self.length == 0 && id <= self.last_id {
            return entries_added;
        }

        if id != StreamId::ZERO && id < self.max_deleted_id {
            return SCG_INVALID_ENTRIES_READ;
        }

        if id == self.last_id {
            return entries_added;
        } else if id > self.last_id {
            return SCG_INVALID_ENTRIES_READ;
        }

        if self.max_deleted_id == StreamId::ZERO || self.max_deleted_id < self.first_id {
            if id < self.first_id {
                return entries_added - length;
            } else if id == self.first_id {
                return entries_added - length + 1;
            }
        }

        SCG_INVALID_ENTRIES_READ
    }

    /// Port of `streamRangeHasTombstones` (t_stream.c:1407). `end == None`
    /// means the open-ended upper bound (`UINT64_MAX`).
    pub fn range_has_tombstones(&self, start: StreamId, end: Option<StreamId>) -> bool {
        if self.length == 0 || self.max_deleted_id == StreamId::ZERO {
            return false;
        }
        let end_id = end.unwrap_or(StreamId::new(u64::MAX, u64::MAX));
        start <= self.max_deleted_id && self.max_deleted_id <= end_id
    }

    /// Port of `streamReplyWithCGLag` (t_stream.c:1442): the group's lag, or
    /// `None` when it cannot be determined (reported to clients as null).
    pub fn group_lag(&self, group_entries_read: i64, group_last_id: StreamId) -> Option<i64> {
        if self.entries_added == 0 {
            return Some(0);
        }
        if group_entries_read != SCG_INVALID_ENTRIES_READ
            && !self.range_has_tombstones(group_last_id, None)
        {
            return Some(self.entries_added as i64 - group_entries_read);
        }
        let estimate = self.estimate_entries_read(group_last_id);
        if estimate != SCG_INVALID_ENTRIES_READ {
            return Some(self.entries_added as i64 - estimate);
        }
        None
    }

    /// Advance a group's read counter over a run of newly delivered entry IDs,
    /// returning the updated `(entries_read, last_delivered_id)`. Port of the
    /// per-entry maintenance in `streamReplyWithRange` (t_stream.c:1700): for
    /// each ID past the group's last-delivered, increment the counter when it
    /// is valid and unfragmented, otherwise re-estimate it.
    pub fn advance_read_counter(
        &self,
        entries_read: i64,
        last_id: StreamId,
        delivered_ids: &[StreamId],
    ) -> (i64, StreamId) {
        let mut read = entries_read;
        let mut last = last_id;
        for &id in delivered_ids {
            if id > last {
                if read != SCG_INVALID_ENTRIES_READ
                    && last >= self.first_id
                    && !self.range_has_tombstones(last, None)
                {
                    read += 1;
                } else if self.entries_added != 0 {
                    read = self.estimate_entries_read(id);
                }
                last = id;
            }
        }
        (read, last)
    }
}

impl Consumer {
    pub fn new(name: RedisString, now_ms: i64) -> Self {
        Self {
            name,
            seen_time_ms: now_ms,
            active_time_ms: now_ms,
            pel: Vec::new(),
        }
    }

    /// Index of the first PEL entry with `entry_id >= target`.
    pub fn pel_lower_bound(&self, target: &StreamId) -> usize {
        self.pel.partition_point(|e| e.entry_id < *target)
    }

    /// Find a PEL entry by id; returns the slot index if present.
    pub fn pel_find(&self, target: &StreamId) -> Option<usize> {
        let idx = self.pel_lower_bound(target);
        if idx < self.pel.len() && self.pel[idx].entry_id == *target {
            Some(idx)
        } else {
            None
        }
    }
}

impl ConsumerGroup {
    pub fn new(name: RedisString, last_delivered_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id,
            consumers: HashMap::new(),
            pel: Vec::new(),
            entries_read: SCG_INVALID_ENTRIES_READ,
        }
    }

    /// Index of the first PEL entry with `entry_id >= target`.
    pub fn pel_lower_bound(&self, target: &StreamId) -> usize {
        self.pel.partition_point(|e| e.entry_id < *target)
    }

    /// Find a PEL entry by id; returns the slot index if present.
    pub fn pel_find(&self, target: &StreamId) -> Option<usize> {
        let idx = self.pel_lower_bound(target);
        if idx < self.pel.len() && self.pel[idx].entry_id == *target {
            Some(idx)
        } else {
            None
        }
    }

    /// Insert or update a group PEL entry for `entry_id`, keeping the
    /// sorted order. Used by `pel_add` and `pel_claim`.
    pub fn pel_upsert(&mut self, entry: PelEntry) {
        let idx = self.pel_lower_bound(&entry.entry_id);
        if idx < self.pel.len() && self.pel[idx].entry_id == entry.entry_id {
            self.pel[idx] = entry;
        } else {
            self.pel.insert(idx, entry);
        }
    }

    /// Remove a group PEL entry by id. Returns the removed entry.
    pub fn pel_remove(&mut self, target: &StreamId) -> Option<PelEntry> {
        let idx = self.pel_lower_bound(target);
        if idx < self.pel.len() && self.pel[idx].entry_id == *target {
            Some(self.pel.remove(idx))
        } else {
            None
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/stream.h + t_stream.c
//   target_crate:  redis-ds
//   confidence:    pragmatic Phase-B (inline encoding)
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Inline encoding for Round-9 stream commands; Phase 5
//                  swaps to rax + listpack once those modules ship.
// ──────────────────────────────────────────────────────────────────────────
