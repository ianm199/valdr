//! `HashTable` — the newer SIMD-friendly open-addressed hash table.
//!
//! Source: `reference/valkey/src/hashtable.c` (and `hashtable.h`).
//! Introduced as a faster alternative to `dict` for some keyspace
//! workloads: open addressing with metadata bytes (a la swiss / F14).
//! Coexists with the legacy `dict` during the transition.

use std::marker::PhantomData;

#[derive(Debug, Clone, Default)]
pub struct HashTable<K, V> {
    // TODO(port): bring over the bucket array + metadata + resize state
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V> HashTable<K, V> {
    pub fn new() -> Self {
        Self {
            _k: PhantomData,
            _v: PhantomData,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/hashtable.c
//   target_crate:  redis-ds
//   confidence:    skeleton
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         stub awaiting Phase 4 translation
// ──────────────────────────────────────────────────────────────────────────
