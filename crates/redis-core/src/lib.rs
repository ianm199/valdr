//! Core server state.
//!
//! Owners (per `harness/type-vocabulary.tsv`):
//!   - `Client`          — `src/client.rs`
//!   - `CommandContext`  — `src/command_context.rs`
//!   - `RedisServer`     — `src/server.rs`     (STUB; expand in Phase 3)
//!   - `RedisDb`         — `src/db.rs`         (STUB; HashMap-backed; kvstore in Phase 4)
//!   - `RedisObject`     — `src/object.rs`     (STUB; encoding sub-variants in Phase 4)
//!
//! Phases 2-3 of the pilot land here.

pub mod blocked;
pub mod defrag;
pub mod client;
pub mod expire;
pub mod command_context;
pub mod commandlog;
pub mod connection;
pub mod db;
pub mod latency;
pub mod lazyfree;
pub mod object;
pub mod server;
pub mod strtod;
pub mod timeout;
pub mod unix;
pub mod util;

pub use client::{Client, ClientId};
pub use command_context::CommandContext;
pub use db::RedisDb;
pub use object::RedisObject;
pub use server::{RedisServer, ServerConfig};

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        (none — scaffolding placeholder)
//   target_crate:  redis-core
//   confidence:    skeleton
//   todos:         5
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         scaffolding; awaiting first translation packet
// ──────────────────────────────────────────────────────────────────────────
