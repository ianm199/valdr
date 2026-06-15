//! Core Valdr server state and shared runtime primitives.
//!
//! This crate owns the types that command handlers and the server runtime
//! share: clients, command contexts, server config/state, database storage,
//! objects, persistence state, replication state, metrics, and networking
//! helpers. Some internals still carry explicit TODO(port)/TODO(architect)
//! markers, but the crate is no longer just scaffolding: the live command
//! server, replication kits, AOF/RDB paths, and scripting code all execute
//! through these APIs.

pub mod acl;
pub mod bio;
pub mod blocked_keys;
pub mod childinfo;
pub mod client;
pub mod client_info;
pub mod command_context;
pub mod commandlog;
pub mod conn_socket;
pub mod conn_tls;
pub mod connection;
pub mod cpu_affinity;
pub mod databases;
pub mod db;
pub mod defrag;
pub mod entry;
pub mod eviction;
pub mod expire;
pub mod fifo;
pub mod keyspace_cow;
pub mod keyspace_map;
pub mod keyspace_snapshot;
pub mod latency;
pub mod lazyfree;
pub mod live_config;
pub mod localtime;
pub mod logreqres;
pub mod lru_clock;
pub mod lrulfu;
pub mod memory;
pub mod memory_prefetch;
pub mod metrics;
pub mod monotonic;
pub mod mt19937;
pub mod mutexqueue;
pub mod networking;
pub mod notify;
pub mod object;
pub mod persistence;
pub mod pubsub_registry;
pub mod queues;
pub mod rand;
pub mod rdb;
pub mod replication;
pub mod reply_traits;
pub mod server;
pub mod setproctitle;
pub mod siphash;
pub mod stream_hooks;
pub mod strtod;
pub mod syscheck;
pub mod threads_mngr;
pub mod timeout;
pub mod tls;
pub mod tracking;
pub mod transport;
pub mod unix;
pub mod util;

pub use client::{Client, ClientId};
pub use command_context::CommandContext;
pub use db::RedisDb;
pub use keyspace_cow::{stats_snapshot as keyspace_cow_stats_snapshot, KeyspaceCowStats};
pub use keyspace_map::{KeyspaceMap, KeyspaceMapSnapshot};
pub use keyspace_snapshot::{KeyspaceSnapshot, KeyspaceSnapshotDb, KeyspaceSnapshotStats};
pub use object::{ObjectKind, RedisObject};
pub use persistence::{AofState, PersistenceState, PersistenceStatus};
pub use pubsub_registry::PubSubRegistry;
pub use server::{RedisServer, ServerConfig};
pub use transport::Connection;

// PORT STATUS: active compatibility core. Keep unresolved work local to the
// modules that own it with TODO(port)/TODO(architect) markers instead of
// applying a crate-level placeholder label.
