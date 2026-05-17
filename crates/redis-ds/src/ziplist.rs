//! `Ziplist` — legacy compact list/hash encoding superseded by listpack.
//!
//! Source: `reference/valkey/src/ziplist.c` (and `ziplist.h`). Still
//! required for backward-compatible RDB loading: older RDB files
//! encode small lists/hashes as ziplists, and the server must decode
//! them on load. New writes use listpack; ziplist is read-mostly.

#[derive(Debug, Clone, Default)]
pub struct Ziplist {
    // TODO(port): bring over the zlbytes/zltail/zllen header + entry buffer
}

impl Ziplist {
    pub fn new() -> Self {
        Self::default()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/ziplist.c
//   target_crate:  redis-ds
//   confidence:    skeleton
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         stub awaiting Phase 4 translation (read-only legacy encoding)
// ──────────────────────────────────────────────────────────────────────────
