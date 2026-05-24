//! RDB persistence framework — Round 18 scaffold.
//!
//! Submodules:
//!   - `crc`    — CRC-64 (Jones polynomial) checksum
//!   - `varint` — RDB variable-length integer encoding/decoding
//!   - `header` — magic header, AUX fields, opcode constants
//!   - `save`   — `save_rdb` writes a complete RDB file
//!   - `load`   — `load_into` reads an RDB file into a `RedisDb`
//!
//! Re-exported entry points: `save_rdb`, `save_rdb_databases`, `load_into`,
//! `load_into_dbs`.

pub mod crc;
pub mod hash;
pub mod header;
pub mod list;
pub mod listpack;
pub mod load;
pub mod lzf;
pub mod save;
pub mod set;
pub mod stream;
pub mod string;
pub mod varint;
pub mod zset;

pub use load::{
    load_dump_payload, load_into, load_into_dbs, load_into_dbs_with_options, load_value_payload,
    verify_dump_payload, RdbLoadOptions,
};
pub use save::{
    create_dump_payload, rdb_type_for_object, save_object_payload, save_rdb, save_rdb_databases,
};

use std::path::PathBuf;

/// Construct the full RDB file path from `dir` and `filename` config values.
pub fn rdb_path(dir: &str, filename: &str) -> PathBuf {
    PathBuf::from(dir).join(filename)
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        RDB module surface
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         Re-exports include caller-owned multi-DB load/save helpers
//                  used by the RuntimeOwner-owned DB startup path.
// ──────────────────────────────────────────────────────────────────────────
