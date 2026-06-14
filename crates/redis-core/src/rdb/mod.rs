//! RDB persistence framework — Round 18 scaffold.
//! Submodules:
//! - `crc` — CRC-64 (Jones polynomial) checksum
//! - `varint` — RDB variable-length integer encoding/decoding
//! - `header` — magic header, AUX fields, opcode constants
//! - `save` — `save_rdb` writes a complete RDB file
//! - `load` — `load_into` reads an RDB file into a `RedisDb`
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
pub mod ziplist;
pub mod zset;

pub use load::{
    last_load_stats, load_dump_payload, load_into, load_into_dbs,
    load_into_dbs_collecting_functions, load_into_dbs_replacing,
    load_into_dbs_replacing_with_options, load_into_dbs_with_options, load_replacement_plan,
    load_replacement_plan_with_options, load_value_payload, verify_dump_payload, RdbLoadOptions,
    RdbLoadOutcome, RdbLoadStats, RdbReplacementPlan,
};
pub use save::{
    create_dump_payload, rdb_type_for_object, save_object_payload, save_rdb, save_rdb_databases,
    save_rdb_databases_with_functions,
};

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Construct the full RDB file path from `dir` and `filename` config values.
pub fn rdb_path(dir: &str, filename: &str) -> PathBuf {
    PathBuf::from(dir).join(filename)
}

/// Stage a replica full-sync RDB through the configured on-disk RDB file.
///
/// Valkey's `repl-diskless-load disabled` path writes the incoming snapshot to
/// a temp file, fsyncs it, and renames it over `dbfilename` before loading. A
/// failed rename must abort the full sync before the in-memory keyspace is
/// replaced, so callers can keep serving the old data and retry later.
pub fn stage_replica_fullsync_rdb_to_disk(
    dir: impl AsRef<Path>,
    filename: &str,
    bytes: &[u8],
) -> io::Result<PathBuf> {
    let dir = dir.as_ref();
    fs::create_dir_all(dir)?;
    let final_path = dir.join(filename);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let temp_path = dir.join(format!("temp-repl-{}-{}.rdb", std::process::id(), nanos));

    let write_result = (|| -> io::Result<()> {
        let mut file = File::create(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    })();
    if let Err(err) = write_result {
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }

    if let Err(err) = fs::rename(&temp_path, &final_path) {
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }

    Ok(final_path)
}

/// Upper bound on how many elements an untrusted RDB/RESTORE length prefix may
/// pre-allocate.
/// A malformed or hostile payload (a crafted `RESTORE`, or a corrupt RDB on
/// disk) can declare an enormous element count in a length field. Passing that
/// count straight to `with_capacity` makes the loader attempt a multi-gigabyte
/// allocation and abort the process before the element-read loop reaches
/// (absent) data. Clamping the pre-allocation removes the abort: collections
/// still grow naturally past this bound for genuinely large, well-formed
/// payloads, and a short hostile payload fails cleanly on the next element read.
pub(crate) const RDB_PREALLOC_CAP: usize = 1 << 16;

/// Clamp an untrusted element count to [`RDB_PREALLOC_CAP`] for pre-allocation.
pub(crate) fn prealloc_capacity(declared: u64) -> usize {
    (declared as usize).min(RDB_PREALLOC_CAP)
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
