//! `redis-ds` — data-structure encodings.
//! Owners:
//! - `ListPack` — `src/listpack.rs`
//! - `QuickList` — `src/quicklist.rs`
//! - `IntSet` — `src/intset.rs`
//! - `RadixTree` — `src/rax.rs`
//! - `StreamId` — `src/stream.rs`
//! - `Dict` — `src/dict.rs`
//! - `LinkedList` — `src/adlist.rs`
//! - `ZSkiplist` — `src/zskiplist.rs`
//! - `HashTable` — `src/hashtable.rs`
//! - `Kvstore` — `src/kvstore.rs`
//! - `Ziplist` — `src/ziplist.rs`
//! All modules in this crate are skeleton stubs awaiting translation.

pub mod adlist;
pub mod dict;
pub mod hashtable;
pub mod intset;
pub mod kvstore;
pub mod listpack;
pub mod pqsort;
pub mod quicklist;
pub mod rax;
pub mod stream;
pub mod ziplist;
pub mod zipmap;
pub mod zskiplist;

pub use adlist::LinkedList;
pub use dict::Dict;
pub use hashtable::HashTable;
pub use intset::IntSet;
pub use kvstore::Kvstore;
pub use listpack::ListPack;
pub use quicklist::QuickList;
pub use rax::RadixTree;
pub use stream::StreamId;
pub use ziplist::Ziplist;
pub use zskiplist::ZSkiplist;

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        (data-structure module scaffolding)
//   target_crate:  redis-ds
//   confidence:    skeleton
//   todos:         11
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Crate skeleton; all module types are stubs awaiting translation.
// ──────────────────────────────────────────────────────────────────────────
