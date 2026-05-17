//! `StreamId` — the (ms, seq) identifier used to address entries in a
//! Redis stream.
//!
//! Source: `reference/valkey/src/stream.h` (paired with
//! `t_stream.c`). A stream ID is a 128-bit composite: the upper 64
//! bits are a millisecond unix timestamp, the lower 64 bits a per-ms
//! sequence counter. IDs are totally ordered and printed as
//! `<ms>-<seq>` on the wire.

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    // TODO(port): wire to t_stream.c streamID layout (ms, seq)
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const fn new() -> Self {
        Self { ms: 0, seq: 0 }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/stream.h
//   target_crate:  redis-ds
//   confidence:    skeleton
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         stub awaiting Phase 5 translation (streams)
// ──────────────────────────────────────────────────────────────────────────
