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

pub mod acl;
pub mod client_info;
pub mod databases;
pub mod evict;
pub mod eviction;
pub mod live_config;
pub mod rdb;
pub mod lru_clock;
pub mod memory;
pub mod metrics;
pub mod monotonic;
pub mod blocked;
pub mod blocked_keys;
pub mod memory_prefetch;
pub mod localtime;
pub mod defrag;
pub mod logreqres;
pub mod client;
pub mod expire;
pub mod command_context;
pub mod commandlog;
pub mod connection;
pub mod db;
pub mod latency;
pub mod lazyfree;
pub mod notify;
pub mod object;
pub mod pubsub_registry;
pub mod replication;
pub mod server;
pub mod strtod;
pub mod timeout;
pub mod tls;
pub mod transport;
pub mod unix;
pub mod util;

pub use client::{Client, ClientId};
pub use command_context::CommandContext;
pub use db::RedisDb;
pub use object::{ObjectKind, RedisObject};
pub use pubsub_registry::PubSubRegistry;
pub use server::{RedisServer, ServerConfig};
pub use transport::Connection;

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
