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
    pub const MAX: StreamId = StreamId { ms: u64::MAX, seq: u64::MAX };

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
            Some(StreamId { ms: self.ms + 1, seq: 0 })
        } else {
            Some(StreamId { ms: self.ms, seq: self.seq + 1 })
        }
    }

    /// Decrement by one with wrap-around to the previous `(ms-1, MAX)` slot.
    /// Returns `None` if the id is already `ZERO`.
    pub fn checked_pred(&self) -> Option<StreamId> {
        if self.seq == 0 {
            if self.ms == 0 {
                return None;
            }
            Some(StreamId { ms: self.ms - 1, seq: u64::MAX })
        } else {
            Some(StreamId { ms: self.ms, seq: self.seq - 1 })
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
            let ms = s.parse::<u64>().map_err(|_| StreamIdParseError::Malformed)?;
            Ok(StreamId { ms, seq: default_seq })
        }
        Some(idx) => {
            let (ms_part, dash_seq) = s.split_at(idx);
            let seq_part = &dash_seq[1..];
            if ms_part.is_empty() || seq_part.is_empty() {
                return Err(StreamIdParseError::Malformed);
            }
            let ms = ms_part.parse::<u64>().map_err(|_| StreamIdParseError::Malformed)?;
            let seq = seq_part.parse::<u64>().map_err(|_| StreamIdParseError::Malformed)?;
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

/// Phase-B inline stream storage.
///
/// `entries` is kept sorted by `id` ascending and is therefore
/// binary-searchable. `last_id` is the largest id ever inserted; this
/// is what auto-id generation (`*`) compares against and is preserved
/// across delete/trim operations to match real Redis semantics.
#[derive(Clone, Debug, Default)]
pub struct InlineStream {
    pub entries: Vec<StreamEntry>,
    pub last_id: StreamId,
    pub max_deleted_id: StreamId,
    pub entries_added: u64,
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
