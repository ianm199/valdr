//! Command registry and command implementations.
//!
//! Owners (per `harness/type-vocabulary.tsv`):
//!   - `CommandSpec` (spec.rs)
//!
//! The command table is generated from `reference/valkey/src/commands/*.json`
//! by `harness/gen-command-registry.py` (TODO). The generator's output
//! lives at `src/generated.rs` and must not be hand-edited.
//!
//! Pilot commands: PING, ECHO, HELLO, COMMAND (Phase 2); SET, GET, DEL,
//! EXISTS, INCR (Phase 3).

pub mod bitops;
pub mod connection;
pub mod dispatch;
pub mod generated;
pub mod hash;
pub mod hyperloglog;
pub mod info;
pub mod list;
pub mod multi;
pub mod pubsub;
pub mod set;
pub mod stream;
pub mod string;
pub mod zset;

pub use dispatch::{dispatch, lookup_command, DispatchEntry, Handler};

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        (none — scaffolding placeholder)
//   target_crate:  redis-commands
//   confidence:    skeleton
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         scaffolding; gen-command-registry.py is the first deliverable
// ──────────────────────────────────────────────────────────────────────────
