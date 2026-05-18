//! RDB persistence framework — Round 18 scaffold.
//!
//! Submodules:
//!   - `crc`    — CRC-64 (Jones polynomial) checksum
//!   - `varint` — RDB variable-length integer encoding/decoding
//!   - `header` — magic header, AUX fields, opcode constants
//!   - `save`   — `save_rdb` writes a complete RDB file
//!   - `load`   — `load_into` reads an RDB file into a `RedisDb`
//!
//! Re-exported entry points: `save_rdb`, `load_into`.

pub mod crc;
pub mod hash;
pub mod header;
pub mod list;
pub mod listpack;
pub mod load;
pub mod save;
pub mod set;
pub mod stream;
pub mod string;
pub mod varint;
pub mod zset;

pub use load::load_into;
pub use save::save_rdb;

use std::path::PathBuf;

/// Construct the full RDB file path from `dir` and `filename` config values.
pub fn rdb_path(dir: &str, filename: &str) -> PathBuf {
    PathBuf::from(dir).join(filename)
}
