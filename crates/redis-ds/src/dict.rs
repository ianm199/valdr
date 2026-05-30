//! `Dict` — incrementally-rehashing hash table.
//! Separate-chaining hash table with two underlying tables during incremental
//! rehash, sized in powers of two, with per-type key/value callbacks.

use std::marker::PhantomData;

#[derive(Debug, Clone, Default)]
pub struct Dict<K, V> {
    // TODO(port): bring over the two ht[2] tables + rehashidx + iter count
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V> Dict<K, V> {
    pub fn new() -> Self {
        Self {
            _k: PhantomData,
            _v: PhantomData,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        (incrementally-rehashing dict, Redis stdlib)
//   target_crate:  redis-ds
//   confidence:    skeleton
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         stub awaiting translation
// ──────────────────────────────────────────────────────────────────────────
