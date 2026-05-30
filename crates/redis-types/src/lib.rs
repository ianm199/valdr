//! Cross-cutting type vocabulary owned by `redis-types`.
//! Owners (per `harness/type-vocabulary.tsv`):
//! - `RedisString` — `src/string.rs`
//! - `RedisError` — `src/error.rs`
//! Other vocabulary types live in other crates by design; see
//! registry. This crate is the foundation: no dependencies on other
//! port crates.

pub mod error;
pub mod string;

pub use error::{RedisError, RedisResult};
pub use string::RedisString;

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        (none — scaffolding placeholder)
//   target_crate:  redis-types
//   confidence:    skeleton
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         scaffolding; awaiting first translation packet
// ──────────────────────────────────────────────────────────────────────────
