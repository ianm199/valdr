//! RESP2/RESP3 parser and serializer.
//!
//! Owners (per `harness/type-vocabulary.tsv`):
//!   - `RespFrame` — `src/frame.rs`
//!
//! Phase 2 of the pilot lives here.

pub mod frame;
pub mod parser;
pub mod request;

pub use frame::{
    encode_big_number, encode_boolean, encode_double, encode_for_proto, encode_map_header,
    encode_null_resp3, encode_push_header, encode_resp2, encode_resp3, encode_set_header,
    encode_verbatim_string, format_double_text, RespFrame,
};
pub use parser::{ParserCallbacks, ParserCursor};
pub use request::{parse_inline_or_multibulk, parse_inline_or_multibulk_into};

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
