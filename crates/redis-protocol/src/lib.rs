//! RESP2/RESP3 parser and serializer.
//!
//! Owners (per `harness/type-vocabulary.tsv`):
//!   - `RespFrame` — `src/frame.rs`
//!
//! Phase 2 of the pilot lives here.

pub mod frame;
pub mod parser;

pub use frame::{RespFrame, encode_resp2};
pub use parser::{ParserCallbacks, ParserCursor};

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        (none — scaffolding placeholder)
//   target_crate:  redis-protocol
//   confidence:    skeleton
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         scaffolding; Phase 2 of pilot translates RESP frame model here
// ──────────────────────────────────────────────────────────────────────────
