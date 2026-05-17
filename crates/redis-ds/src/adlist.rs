//! `LinkedList` — Redis's generic doubly-linked list (`list` /
//! `listNode` in C).
//!
//! Source: `reference/valkey/src/adlist.c` (and `adlist.h`). The
//! "A simple generic doubly linked list" used throughout the server
//! for client queues, pubsub subscribers, blocked clients, and many
//! other small ordered collections.

use std::marker::PhantomData;

#[derive(Debug, Clone, Default)]
pub struct LinkedList<T> {
    // TODO(port): bring over head/tail/len + dup/free/match callbacks
    _t: PhantomData<T>,
}

impl<T> LinkedList<T> {
    pub fn new() -> Self {
        Self { _t: PhantomData }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/adlist.c
//   target_crate:  redis-ds
//   confidence:    skeleton
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         stub awaiting Phase 4 translation
// ──────────────────────────────────────────────────────────────────────────
