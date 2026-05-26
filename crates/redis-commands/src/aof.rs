//! Append-Only File (AOF) persistence.
//!
//! `AofWriter` encodes every write command as a RESP multibulk array and
//! appends it to the AOF file. The fsync policy is governed by the
//! `appendfsync` config key: `always` (sync after every append), `everysec`
//! (background sync thread fsyncs once per second), or `no` (OS decides).
//!
//! `BGREWRITEAOF` walks the current DB and emits the minimal command sequence
//! needed to reconstruct each key. In the manifest layout it switches appends
//! to a new INCR file before writing the new BASE, then persists a manifest
//! naming the new BASE plus active INCR.
//!
//! Replay on startup: `replay_aof` reads RESP commands using the same parser as
//! the network path. Malformed input and unsupported commands are fatal unless
//! the caller explicitly allows a truncated tail.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use redis_core::db::RedisDb;
use redis_core::object::{HashEncoding, InlineHash, ObjectKind, EXPIRY_NONE};
use redis_core::{Client, CommandContext, PubSubRegistry, RedisServer};
use redis_types::RedisString;

/// fsync policy discriminant stored inside `AtomicU8`.
pub const FSYNC_NO: u8 = 0;
pub const FSYNC_EVERYSEC: u8 = 1;
pub const FSYNC_ALWAYS: u8 = 2;

pub fn parse_fsync_policy(s: &[u8]) -> Option<u8> {
    let lower: Vec<u8> = s.iter().map(|b| b.to_ascii_lowercase()).collect();
    match lower.as_slice() {
        b"no" => Some(FSYNC_NO),
        b"everysec" => Some(FSYNC_EVERYSEC),
        b"always" => Some(FSYNC_ALWAYS),
        _ => None,
    }
}

pub fn fsync_policy_str(code: u8) -> &'static str {
    match code {
        FSYNC_NO => "no",
        FSYNC_ALWAYS => "always",
        _ => "everysec",
    }
}

/// Options controlling AOF replay strictness.
#[derive(Clone, Debug)]
pub struct AofLoadOptions {
    /// Accept an incomplete final command and replay the valid prefix.
    pub load_truncated: bool,
    /// Accept an RDB preamble before RESP commands. Placeholder for the
    /// manifest/RDB-preamble packet; legacy single-file AOF currently rejects
    /// preambles even when this is true.
    pub allow_rdb_preamble: bool,
}

impl Default for AofLoadOptions {
    fn default() -> Self {
        Self {
            load_truncated: false,
            allow_rdb_preamble: false,
        }
    }
}

const AOF_MANIFEST_SUFFIX: &str = ".manifest";
const BASE_AOF_SUFFIX: &str = ".base.aof";
const INCR_AOF_SUFFIX: &str = ".incr.aof";
const MANIFEST_MAX_LINE: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AofManifestFileType {
    Base,
    Incr,
    History,
}

#[derive(Clone, Debug)]
struct AofManifestFile {
    name: Vec<u8>,
    seq: i64,
    file_type: AofManifestFileType,
}

#[derive(Default, Debug)]
struct AofManifest {
    base: Option<AofManifestFile>,
    incr: Vec<AofManifestFile>,
}

impl AofManifest {
    fn is_empty(&self) -> bool {
        self.base.is_none() && self.incr.is_empty()
    }

    fn load_sequence(&self) -> Vec<&AofManifestFile> {
        let mut out = Vec::with_capacity(usize::from(self.base.is_some()) + self.incr.len());
        if let Some(base) = &self.base {
            out.push(base);
        }
        out.extend(self.incr.iter());
        out
    }

    fn max_incr_seq(&self) -> i64 {
        self.incr.iter().map(|file| file.seq).max().unwrap_or(0)
    }

    fn next_base_seq(&self) -> i64 {
        self.base.as_ref().map(|file| file.seq).unwrap_or(0) + 1
    }

    fn next_incr_seq(&self) -> i64 {
        self.max_incr_seq() + 1
    }
}

/// The global AOF writer. `None` when `appendonly` is disabled.
static AOF_WRITER: OnceLock<Arc<Mutex<Option<Arc<AofWriter>>>>> = OnceLock::new();

fn aof_writer_cell() -> &'static Arc<Mutex<Option<Arc<AofWriter>>>> {
    AOF_WRITER.get_or_init(|| Arc::new(Mutex::new(None)))
}

/// Return the active `AofWriter`, if any.
pub fn aof_writer() -> Option<Arc<AofWriter>> {
    let cell = aof_writer_cell();
    let guard = match cell.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.clone()
}

/// Install (or replace) the active `AofWriter`.
pub fn install_aof_writer(writer: Arc<AofWriter>) {
    let cell = aof_writer_cell();
    let mut guard = match cell.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = Some(writer);
}

/// Clear the active `AofWriter` (called when `appendonly` goes false→false).
pub fn remove_aof_writer() {
    let cell = aof_writer_cell();
    let mut guard = match cell.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = None;
}

fn take_aof_writer() -> Option<Arc<AofWriter>> {
    let cell = aof_writer_cell();
    let mut guard = match cell.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.take()
}

fn restore_aof_writer(writer: Option<Arc<AofWriter>>) {
    let cell = aof_writer_cell();
    let mut guard = match cell.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard = writer;
}

/// Append-only file writer.
pub struct AofWriter {
    pub path: PathBuf,
    file: Mutex<BufWriter<File>>,
    selected_db: Mutex<Option<u32>>,
    pub pending_bytes: AtomicUsize,
    pub fsync_policy: AtomicU8,
    pending_repl_offset: AtomicI64,
    fsynced_repl_offset: AtomicI64,
}

impl AofWriter {
    /// Open (or create) the AOF file at `path`.
    pub fn open(path: &Path, fsync_policy: u8) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file: Mutex::new(BufWriter::new(file)),
            selected_db: Mutex::new(None),
            pending_bytes: AtomicUsize::new(0),
            fsync_policy: AtomicU8::new(fsync_policy),
            pending_repl_offset: AtomicI64::new(-1),
            fsynced_repl_offset: AtomicI64::new(0),
        })
    }

    /// Create or truncate an AOF file, then reopen it in append mode.
    pub fn open_truncate(path: &Path, fsync_policy: u8) -> io::Result<Self> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Self::open(path, fsync_policy)
    }

    /// Encode `argv` as a RESP multibulk command and append it to the file.
    ///
    /// When the fsync policy is `FSYNC_ALWAYS`, flushes and fsyncs before
    /// returning. Otherwise the everysec background thread or the OS handles
    /// durability.
    pub fn append(&self, argv: &[RedisString]) -> io::Result<()> {
        self.append_selected(0, argv)
    }

    /// Append a command without inserting an implicit SELECT record.
    ///
    /// MULTI/EXEC transaction envelopes use this; commands inside the envelope
    /// still call `append_selected` so DB selection is represented inside the
    /// transaction.
    pub fn append_raw(&self, argv: &[RedisString]) -> io::Result<()> {
        let encoded = encode_resp_command(argv);
        self.append_encoded(&encoded)
    }

    /// Append a command that was executed against logical DB `db_id`.
    ///
    /// Mirrors Valkey `feedAppendOnlyFile`: a SELECT record is inserted when
    /// the target DB differs from the previous AOF record, then the command is
    /// appended in the same RESP multibulk format used by replication.
    pub fn append_selected(&self, db_id: u32, argv: &[RedisString]) -> io::Result<()> {
        let mut encoded = Vec::with_capacity(64);
        {
            let mut selected = match self.selected_db.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if *selected != Some(db_id) {
                let db_arg = RedisString::from_vec(db_id.to_string().into_bytes());
                let select = [RedisString::from_bytes(b"SELECT"), db_arg];
                encoded.extend_from_slice(&encode_resp_command(&select));
                *selected = Some(db_id);
            }
        }
        encoded.extend_from_slice(&encode_resp_command(argv));
        self.append_encoded(&encoded)
    }

    fn append_encoded(&self, encoded: &[u8]) -> io::Result<()> {
        let len = encoded.len();
        {
            let mut guard = match self.file.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.write_all(encoded)?;
            if self.fsync_policy.load(Ordering::Relaxed) == FSYNC_ALWAYS {
                guard.flush()?;
                guard.get_ref().sync_data()?;
                self.pending_bytes.store(0, Ordering::Relaxed);
                let pending = self.pending_repl_offset.load(Ordering::Relaxed);
                if pending >= 0 {
                    self.fsynced_repl_offset.store(pending, Ordering::Release);
                }
                return Ok(());
            }
        }
        self.pending_bytes.fetch_add(len, Ordering::Relaxed);
        Ok(())
    }

    /// Flush the BufWriter and fsync to disk if there are pending bytes.
    ///
    /// Called by the everysec background thread.
    pub fn fsync_if_due(&self) -> io::Result<()> {
        if self.pending_bytes.load(Ordering::Relaxed) == 0 {
            return Ok(());
        }
        let mut guard = match self.file.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.flush()?;
        guard.get_ref().sync_data()?;
        self.pending_bytes.store(0, Ordering::Relaxed);
        let pending = self.pending_repl_offset.load(Ordering::Relaxed);
        if pending >= 0 {
            self.fsynced_repl_offset.store(pending, Ordering::Release);
        }
        Ok(())
    }

    /// Record the replication offset covered by the most recent AOF append.
    ///
    /// Upstream Valkey tracks `server.fsynced_reploff` rather than raw file
    /// byte offsets. WAITAOF waits on that replication offset, so the Rust AOF
    /// writer remembers the highest replication offset whose command bytes
    /// have been appended, then publishes it after a successful fsync.
    pub fn note_repl_offset(&self, offset: i64) {
        if offset < 0 {
            return;
        }
        self.pending_repl_offset.store(offset, Ordering::Release);
        if self.fsync_policy.load(Ordering::Relaxed) == FSYNC_ALWAYS {
            self.fsynced_repl_offset.store(offset, Ordering::Release);
        }
    }

    pub fn fsynced_repl_offset(&self) -> i64 {
        self.fsynced_repl_offset.load(Ordering::Acquire)
    }

    pub fn force_fsynced_repl_offset(&self, offset: i64) {
        if offset < 0 {
            return;
        }
        self.pending_repl_offset.store(offset, Ordering::Release);
        self.fsynced_repl_offset.store(offset, Ordering::Release);
    }

    /// Flush the buffer without fsyncing. Used during clean shutdown.
    pub fn flush(&self) -> io::Result<()> {
        let mut guard = match self.file.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.flush()
    }

    /// Atomically replace the AOF file with a freshly-rewritten version at
    /// `new_path` by renaming `new_path` over `self.path`.
    pub fn rewrite_swap(&self, new_path: &Path) -> io::Result<()> {
        std::fs::rename(new_path, &self.path)
    }

    /// Blocking, single-file AOF rewrite used until manifest-style rewrite
    /// finalization lands.
    ///
    /// The writer mutex is held for the full rewrite so no acknowledged command
    /// can append to the old file and then be hidden by the final rename. This
    /// is deliberately conservative: `BGREWRITEAOF` is not truly background
    /// while this path is active, but it preserves the no-write-loss invariant.
    pub fn rewrite_from_dbs_blocking(&self, dbs: &[RedisDb], tmp_path: &Path) -> io::Result<()> {
        let mut active = match self.file.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        active.flush()?;

        let tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(tmp_path)?;
        let mut tmp = BufWriter::new(tmp_file);
        write_aof_rewrite_for_dbs(dbs, &mut tmp)?;
        tmp.flush()?;
        tmp.get_ref().sync_data()?;
        drop(tmp);

        std::fs::rename(tmp_path, &self.path)?;
        let reopened = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        *active = BufWriter::new(reopened);
        self.pending_bytes.store(0, Ordering::Relaxed);
        match self.selected_db.lock() {
            Ok(mut selected) => *selected = None,
            Err(poisoned) => *poisoned.into_inner() = None,
        }
        Ok(())
    }
}

pub fn note_current_writer_repl_offset(offset: i64) {
    if let Some(writer) = aof_writer() {
        writer.note_repl_offset(offset);
    }
}

pub fn current_fsynced_repl_offset() -> i64 {
    aof_writer()
        .map(|writer| writer.fsynced_repl_offset())
        .unwrap_or(-1)
}

pub fn force_current_writer_fsynced_repl_offset(offset: i64) {
    if let Some(writer) = aof_writer() {
        writer.force_fsynced_repl_offset(offset);
    }
}

/// Encode a command argv slice as a RESP multibulk array.
pub fn encode_resp_command(argv: &[RedisString]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(format!("*{}\r\n", argv.len()).as_bytes());
    for arg in argv {
        let bytes = arg.as_bytes();
        out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
        out.extend_from_slice(bytes);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Spawn the everysec fsync background thread. The thread runs until the
/// process exits. When `appendonly` is disabled the thread simply sleeps.
pub fn spawn_fsync_thread() {
    std::thread::Builder::new()
        .name("aof-fsync".to_string())
        .spawn(move || {
            let interval = Duration::from_secs(1);
            let mut last = Instant::now();
            loop {
                std::thread::sleep(Duration::from_millis(100));
                if last.elapsed() >= interval {
                    last = Instant::now();
                    if let Some(writer) = aof_writer() {
                        if writer.fsync_policy.load(Ordering::Relaxed) == FSYNC_EVERYSEC {
                            if let Err(e) = writer.fsync_if_due() {
                                eprintln!("redis-server: AOF fsync failed: {}", e);
                            } else {
                                crate::replication::maybe_wake_wait_clients();
                            }
                        }
                    }
                }
            }
        })
        .expect("failed to spawn aof-fsync thread");
}

/// Write the minimum command sequence to reconstruct `db` into `file`.
///
/// String → SET / SETEX
/// List   → RPUSH (batched ≤ 64 elements)
/// Hash   → HMSET (batched ≤ 64 field/value pairs)
/// Set    → SADD (batched ≤ 64 members)
/// ZSet   → ZADD (batched ≤ 64 score/member pairs)
/// Stream → XADD per entry, XSETID to lock last_id / max_deleted_id /
///          entries_added, XGROUP CREATE per group, XGROUP CREATECONSUMER
///          per consumer, and XCLAIM JUSTID FORCE per PEL entry. Streams
///          with no entries but at least one group emit a placeholder
///          XADD + XDEL pair followed by XSETID to recreate the key.
///          Truly-empty streams (no entries and no groups) are skipped —
///          replay won't recreate the key. XCLAIM is emitted with FORCE
///          so the PEL entry is created during replay even though the
///          consumer's PEL is empty at that point. JUSTID preserves the
///          previous `delivery_count` slot in our codebase (zero), so
///          original per-PEL `delivery_count` and `delivery_time_ms` are
///          not restored exactly — they reset to zero / replay-time.
///
/// After the data command each key with a TTL gets PEXPIREAT.
pub fn write_aof_rewrite<W: Write>(db: &RedisDb, writer: &mut W) -> io::Result<()> {
    write_aof_rewrite_for_dbs(std::slice::from_ref(db), writer)
}

/// Write a compact AOF rewrite for every non-empty logical DB in order.
pub fn write_aof_rewrite_for_dbs<W: Write>(dbs: &[RedisDb], writer: &mut W) -> io::Result<()> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let mut selected_db: Option<u32> = None;
    for db in dbs {
        if db.size() == 0 {
            continue;
        }
        if selected_db != Some(db.id) {
            let db_id = RedisString::from_vec(db.id.to_string().into_bytes());
            let cmd = [RedisString::from_bytes(b"SELECT"), db_id];
            writer.write_all(&encode_resp_command(&cmd))?;
            selected_db = Some(db.id);
        }
        write_aof_rewrite_db_contents(db, writer, now_ms)?;
    }
    Ok(())
}

fn write_aof_rewrite_db_contents<W: Write>(
    db: &RedisDb,
    writer: &mut W,
    now_ms: i64,
) -> io::Result<()> {
    for (key, obj) in db.iter_for_eviction() {
        if obj.expire != EXPIRY_NONE && obj.expire <= now_ms {
            continue;
        }
        match &obj.kind {
            ObjectKind::String(enc) => {
                let value_bytes = match enc {
                    redis_core::object::StringEncoding::Raw(s) => s.as_bytes().to_vec(),
                    redis_core::object::StringEncoding::Embstr(s) => s.as_bytes().to_vec(),
                    redis_core::object::StringEncoding::Int(n) => n.to_string().into_bytes(),
                };
                if obj.expire != EXPIRY_NONE {
                    let ttl_ms = obj.expire - now_ms;
                    let ttl_sec = (ttl_ms / 1000).max(1);
                    let cmd = [
                        RedisString::from_bytes(b"SETEX"),
                        key.clone(),
                        RedisString::from_vec(ttl_sec.to_string().into_bytes()),
                        RedisString::from_vec(value_bytes),
                    ];
                    writer.write_all(&encode_resp_command(&cmd))?;
                } else {
                    let cmd = [
                        RedisString::from_bytes(b"SET"),
                        key.clone(),
                        RedisString::from_vec(value_bytes),
                    ];
                    writer.write_all(&encode_resp_command(&cmd))?;
                }
            }
            ObjectKind::List(enc) => {
                let elements: Vec<RedisString> = match enc {
                    redis_core::object::ListEncoding::Inline(dq) => dq.iter().cloned().collect(),
                    redis_core::object::ListEncoding::QuickList(dq) => dq.iter().cloned().collect(),
                    redis_core::object::ListEncoding::ListPack(_) => Vec::new(),
                };
                for chunk in elements.chunks(64) {
                    let mut cmd = Vec::with_capacity(chunk.len() + 2);
                    cmd.push(RedisString::from_bytes(b"RPUSH"));
                    cmd.push(key.clone());
                    cmd.extend_from_slice(chunk);
                    writer.write_all(&encode_resp_command(&cmd))?;
                }
                write_pexpireat(writer, key, obj.expire)?;
            }
            ObjectKind::Hash(enc) => {
                let pairs: Vec<(RedisString, RedisString)> = match enc {
                    redis_core::object::HashEncoding::Inline(map) => {
                        map.iter().map(|(f, v)| (f.clone(), v.clone())).collect()
                    }
                    redis_core::object::HashEncoding::HashTable(map) => {
                        map.iter().map(|(f, v)| (f.clone(), v.clone())).collect()
                    }
                    redis_core::object::HashEncoding::ListPack(_) => Vec::new(),
                };
                for chunk in pairs.chunks(64) {
                    let mut cmd = Vec::with_capacity(chunk.len() * 2 + 2);
                    cmd.push(RedisString::from_bytes(b"HMSET"));
                    cmd.push(key.clone());
                    for (f, v) in chunk {
                        cmd.push(f.clone());
                        cmd.push(v.clone());
                    }
                    writer.write_all(&encode_resp_command(&cmd))?;
                }
                write_pexpireat(writer, key, obj.expire)?;
            }
            ObjectKind::Set(enc) => {
                let members: Vec<RedisString> = match enc {
                    redis_core::object::SetEncoding::Inline(s) => s.data.iter().cloned().collect(),
                    redis_core::object::SetEncoding::HashTable(hs) => hs.iter().cloned().collect(),
                    redis_core::object::SetEncoding::IntSet(v) => v
                        .iter()
                        .map(|n| RedisString::from_vec(n.to_string().into_bytes()))
                        .collect(),
                    redis_core::object::SetEncoding::ListPack(_) => Vec::new(),
                };
                for chunk in members.chunks(64) {
                    let mut cmd = Vec::with_capacity(chunk.len() + 2);
                    cmd.push(RedisString::from_bytes(b"SADD"));
                    cmd.push(key.clone());
                    cmd.extend_from_slice(chunk);
                    writer.write_all(&encode_resp_command(&cmd))?;
                }
                write_pexpireat(writer, key, obj.expire)?;
            }
            ObjectKind::ZSet(enc) => {
                let pairs: Vec<(f64, RedisString)> = match enc {
                    redis_core::object::ZSetEncoding::Inline(zs) => {
                        zs.iter_ascending().map(|(s, m)| (s, m.clone())).collect()
                    }
                    redis_core::object::ZSetEncoding::SkipList(v) => {
                        v.iter().map(|(m, s)| (*s, m.clone())).collect()
                    }
                    redis_core::object::ZSetEncoding::ListPack(_) => Vec::new(),
                };
                for chunk in pairs.chunks(64) {
                    let mut cmd = Vec::with_capacity(chunk.len() * 2 + 2);
                    cmd.push(RedisString::from_bytes(b"ZADD"));
                    cmd.push(key.clone());
                    for (score, member) in chunk {
                        cmd.push(RedisString::from_vec(format_score(*score).into_bytes()));
                        cmd.push(member.clone());
                    }
                    writer.write_all(&encode_resp_command(&cmd))?;
                }
                write_pexpireat(writer, key, obj.expire)?;
            }
            ObjectKind::Stream(redis_core::object::StreamEncoding::Inline(stream)) => {
                if stream.entries.is_empty() && stream.groups.is_empty() {
                    continue;
                }
                write_stream_rewrite(writer, key, stream)?;
                write_pexpireat(writer, key, obj.expire)?;
            }
            ObjectKind::Module => {}
            ObjectKind::Json(_) => {}
            ObjectKind::Bloom(_) => {}
        }
    }
    Ok(())
}

fn write_pexpireat<W: Write>(writer: &mut W, key: &RedisString, expire: i64) -> io::Result<()> {
    if expire == EXPIRY_NONE {
        return Ok(());
    }
    let cmd = [
        RedisString::from_bytes(b"PEXPIREAT"),
        key.clone(),
        RedisString::from_vec(expire.to_string().into_bytes()),
    ];
    writer.write_all(&encode_resp_command(&cmd))
}

/// Emit the per-stream command sequence: XADD per entry, XSETID, then per
/// group XGROUP CREATE + XGROUP CREATECONSUMER + XCLAIM JUSTID FORCE.
///
/// For empty-but-existing streams (no entries, has at least one group), a
/// placeholder `XADD <key> 1-1 _ _` followed by `XDEL <key> 1-1` is emitted
/// so the key exists before the XGROUP commands run; the trailing XSETID
/// then restores the original `last_id`, `entries_added`, and
/// `max_deleted_id`.
fn write_stream_rewrite<W: Write>(
    writer: &mut W,
    key: &RedisString,
    stream: &redis_ds::stream::InlineStream,
) -> io::Result<()> {
    if stream.entries.is_empty() {
        let placeholder_id = RedisString::from_bytes(b"1-1");
        let xadd = [
            RedisString::from_bytes(b"XADD"),
            key.clone(),
            placeholder_id.clone(),
            RedisString::from_bytes(b"_"),
            RedisString::from_bytes(b"_"),
        ];
        writer.write_all(&encode_resp_command(&xadd))?;
        let xdel = [
            RedisString::from_bytes(b"XDEL"),
            key.clone(),
            placeholder_id,
        ];
        writer.write_all(&encode_resp_command(&xdel))?;
    } else {
        for entry in &stream.entries {
            let mut cmd: Vec<RedisString> = Vec::with_capacity(3 + entry.fields.len() * 2);
            cmd.push(RedisString::from_bytes(b"XADD"));
            cmd.push(key.clone());
            cmd.push(RedisString::from_vec(entry.id.to_display_bytes()));
            for (field, value) in &entry.fields {
                cmd.push(field.clone());
                cmd.push(value.clone());
            }
            writer.write_all(&encode_resp_command(&cmd))?;
        }
    }

    let mut xsetid: Vec<RedisString> = Vec::with_capacity(7);
    xsetid.push(RedisString::from_bytes(b"XSETID"));
    xsetid.push(key.clone());
    xsetid.push(RedisString::from_vec(stream.last_id.to_display_bytes()));
    xsetid.push(RedisString::from_bytes(b"ENTRIESADDED"));
    xsetid.push(RedisString::from_vec(
        stream.entries_added.to_string().into_bytes(),
    ));
    if stream.max_deleted_id != redis_ds::stream::StreamId::ZERO {
        xsetid.push(RedisString::from_bytes(b"MAXDELETEDID"));
        xsetid.push(RedisString::from_vec(
            stream.max_deleted_id.to_display_bytes(),
        ));
    }
    writer.write_all(&encode_resp_command(&xsetid))?;

    let mut group_names: Vec<&RedisString> = stream.groups.keys().collect();
    group_names.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    for gname in group_names {
        let group = match stream.groups.get(gname) {
            Some(g) => g,
            None => continue,
        };
        let create = [
            RedisString::from_bytes(b"XGROUP"),
            RedisString::from_bytes(b"CREATE"),
            key.clone(),
            gname.clone(),
            RedisString::from_vec(group.last_delivered_id.to_display_bytes()),
        ];
        writer.write_all(&encode_resp_command(&create))?;

        let mut consumer_names: Vec<&RedisString> = group.consumers.keys().collect();
        consumer_names.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        for cname in consumer_names {
            let consumer = match group.consumers.get(cname) {
                Some(c) => c,
                None => continue,
            };
            let createcons = [
                RedisString::from_bytes(b"XGROUP"),
                RedisString::from_bytes(b"CREATECONSUMER"),
                key.clone(),
                gname.clone(),
                cname.clone(),
            ];
            writer.write_all(&encode_resp_command(&createcons))?;

            for pel_entry in &consumer.pel {
                let xclaim = [
                    RedisString::from_bytes(b"XCLAIM"),
                    key.clone(),
                    gname.clone(),
                    cname.clone(),
                    RedisString::from_bytes(b"0"),
                    RedisString::from_vec(pel_entry.entry_id.to_display_bytes()),
                    RedisString::from_bytes(b"JUSTID"),
                    RedisString::from_bytes(b"FORCE"),
                ];
                writer.write_all(&encode_resp_command(&xclaim))?;
            }
        }
    }

    Ok(())
}

fn format_score(score: f64) -> String {
    if score == f64::INFINITY {
        "+inf".to_string()
    } else if score == f64::NEG_INFINITY {
        "-inf".to_string()
    } else {
        format!("{}", score)
    }
}

/// Replay an AOF file into `db` by parsing each RESP command and dispatching
/// it through the handler table.
///
/// Commands that fail to parse are skipped with a warning. Commands that
/// return errors are also skipped (e.g. SETEX with already-expired TTL).
///
/// Returns the number of commands successfully replayed.
pub fn replay_aof(path: &Path, db: &mut RedisDb) -> io::Result<usize> {
    replay_aof_databases(path, std::slice::from_mut(db))
}

/// Replay an AOF file into an owner-provided logical DB vector.
///
/// SELECT commands update the replay target. This is used during startup
/// before `RuntimeOwner` begins polling sockets, so it mutates only the DB
/// vector that will become the owner-owned live keyspace.
pub fn replay_aof_databases(path: &Path, dbs: &mut [RedisDb]) -> io::Result<usize> {
    replay_aof_databases_with_options(path, dbs, AofLoadOptions::default())
}

pub fn replay_aof_databases_with_options(
    path: &Path,
    dbs: &mut [RedisDb],
    options: AofLoadOptions,
) -> io::Result<usize> {
    use redis_protocol::parse_inline_or_multibulk;

    if dbs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AOF replay requires at least one database",
        ));
    }

    let data = std::fs::read(path)?;
    if data.starts_with(b"REDIS") || data.starts_with(b"VALKEY") {
        let mode = if options.allow_rdb_preamble {
            "RDB preamble loading is not implemented for legacy single-file AOF"
        } else {
            "RDB preamble is disabled for this AOF load"
        };
        return Err(io::Error::new(io::ErrorKind::InvalidData, mode));
    }
    let mut buf: &[u8] = &data;
    let mut replayed = 0usize;
    let mut selected_db: usize = 0;

    loop {
        while buf.starts_with(b"#") {
            if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                buf = &buf[pos + 1..];
            } else {
                break;
            }
        }
        if buf.is_empty() {
            break;
        }

        match parse_inline_or_multibulk(buf) {
            Ok(Some((argv, consumed))) => {
                buf = &buf[consumed..];
                if argv.is_empty() {
                    continue;
                }
                let name_lower: Vec<u8> = argv[0]
                    .as_bytes()
                    .iter()
                    .map(|b| b.to_ascii_lowercase())
                    .collect();
                if name_lower.as_slice() == b"select" && argv.len() >= 2 {
                    let index = parse_usize_ascii(argv[1].as_bytes()).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "AOF SELECT has invalid DB id")
                    })?;
                    if index >= dbs.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("AOF SELECT DB {} exceeds configured DB count", index),
                        ));
                    }
                    selected_db = index;
                    replayed += 1;
                    continue;
                }
                if name_lower.as_slice() == b"flushall" {
                    for db in dbs.iter_mut() {
                        db.clear();
                    }
                    replayed += 1;
                    continue;
                }
                dispatch_replay_command(&argv, &mut dbs[selected_db])?;
                replayed += 1;
            }
            Ok(None) => {
                if options.load_truncated {
                    break;
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "AOF ended with an incomplete command",
                ));
            }
            Err(e) => {
                if options.load_truncated && !buf.contains(&b'\n') {
                    break;
                }
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("AOF parse error after {} commands: {}", replayed, e),
                ));
            }
        }
    }

    Ok(replayed)
}

/// Load Valkey multi-part AOF files or fall back to the legacy single AOF.
///
/// When `<dir>/<appenddirname>/<appendfilename>.manifest` exists, the manifest
/// is parsed with Valkey's strict startup rules and the BASE plus ordered INCR
/// files it names are loaded from `appenddirname`. When no manifest exists, the
/// old `<dir>/<appendfilename>` file remains the compatibility input.
pub fn load_append_only_files(
    dir: &Path,
    appendfilename: &str,
    appenddirname: &str,
    dbs: &mut [RedisDb],
    options: AofLoadOptions,
) -> io::Result<Option<(usize, u64)>> {
    let aof_dir = dir.join(appenddirname);
    let manifest_path = aof_dir.join(format!("{}{}", appendfilename, AOF_MANIFEST_SUFFIX));

    if manifest_path.exists() {
        let manifest = load_aof_manifest(&manifest_path)?;
        if manifest.is_empty() {
            return Ok(None);
        }
        return load_manifest_files(&aof_dir, &manifest, dbs, options).map(Some);
    }

    let legacy_path = dir.join(appendfilename);
    if !legacy_path.exists() {
        return Ok(None);
    }

    let replayed = replay_aof_databases_with_options(&legacy_path, dbs, options)?;
    let size = legacy_path.metadata().map(|m| m.len()).unwrap_or(0);
    Ok(Some((replayed, size)))
}

/// Open the active writer using Valkey's multi-part manifest layout.
///
/// Startup and `CONFIG SET appendonly yes` both call this after any existing
/// AOF state has been loaded into `dbs`. It creates `appenddirname`, creates a
/// BASE file plus current INCR when the manifest has no writable entries, and
/// opens the last/current INCR with append semantics.
pub fn open_manifest_current_incr_writer(
    dir: &Path,
    appendfilename: &str,
    appenddirname: &str,
    dbs: &[RedisDb],
    fsync_policy: u8,
) -> io::Result<(AofWriter, u64)> {
    let aof_dir = dir.join(appenddirname);
    std::fs::create_dir_all(&aof_dir)?;
    let manifest_path = aof_manifest_path(dir, appendfilename, appenddirname);
    let mut manifest = if manifest_path.exists() {
        load_aof_manifest(&manifest_path)?
    } else {
        AofManifest::default()
    };
    let mut dirty = false;

    if manifest.base.is_none() && manifest.incr.is_empty() {
        let base_name = format!("{}.1{}", appendfilename, BASE_AOF_SUFFIX);
        let base_path = aof_dir.join(&base_name);
        write_base_aof_file(&base_path, dbs)?;
        manifest.base = Some(AofManifestFile {
            name: base_name.into_bytes(),
            seq: 1,
            file_type: AofManifestFileType::Base,
        });
        dirty = true;
    }

    let incr_name = match manifest.incr.last() {
        Some(file) => file.name.clone(),
        None => {
            let seq = manifest.max_incr_seq() + 1;
            let name = format!("{}.{}{}", appendfilename, seq.max(1), INCR_AOF_SUFFIX);
            let bytes = name.into_bytes();
            manifest.incr.push(AofManifestFile {
                name: bytes.clone(),
                seq: seq.max(1),
                file_type: AofManifestFileType::Incr,
            });
            dirty = true;
            bytes
        }
    };

    let incr_path = manifest_file_path(&aof_dir, &incr_name);
    let writer = AofWriter::open(&incr_path, fsync_policy)?;
    if dirty {
        persist_aof_manifest(&manifest_path, &manifest)?;
    }
    let size = incr_path.metadata().map(|m| m.len()).unwrap_or(0);
    Ok((writer, size))
}

/// Rewrite a Valkey multi-part AOF manifest while keeping appends on an INCR.
///
/// This is synchronous for now, but follows the parent-side ordering that
/// matters for correctness: publish a new active INCR first, write and rename a
/// durable BASE file, then persist a manifest naming only the new BASE and the
/// current INCR. History deletion and failed-rewrite INCR accumulation are
/// intentionally left out of this v1 path.
pub fn rewrite_manifest_aof_from_dbs(
    dir: &Path,
    appendfilename: &str,
    appenddirname: &str,
    dbs: &[RedisDb],
    fsync_policy: u8,
    use_rdb_preamble: bool,
) -> io::Result<(u64, u64)> {
    let aof_dir = dir.join(appenddirname);
    std::fs::create_dir_all(&aof_dir)?;
    let manifest_path = aof_manifest_path(dir, appendfilename, appenddirname);
    let manifest = if manifest_path.exists() {
        load_aof_manifest(&manifest_path)?
    } else {
        AofManifest::default()
    };

    let base_seq = manifest.next_base_seq();
    let incr_seq = manifest.next_incr_seq();
    let base_suffix = if use_rdb_preamble {
        ".base.rdb"
    } else {
        BASE_AOF_SUFFIX
    };
    let base_name = format!("{}.{}{}", appendfilename, base_seq, base_suffix);
    let incr_name = format!("{}.{}{}", appendfilename, incr_seq, INCR_AOF_SUFFIX);
    let base_path = aof_dir.join(&base_name);
    let incr_path = aof_dir.join(&incr_name);
    let temp_base_path = aof_dir.join(format!(
        "temp-rewriteaof-bg-{}{}",
        std::process::id(),
        if use_rdb_preamble { ".rdb" } else { ".aof" }
    ));

    let new_writer = Arc::new(AofWriter::open_truncate(&incr_path, fsync_policy)?);
    let previous_writer = take_aof_writer();
    if let Some(writer) = &previous_writer {
        if let Err(e) = writer.flush() {
            restore_aof_writer(previous_writer);
            return Err(e);
        }
    }
    install_aof_writer(Arc::clone(&new_writer));

    let result = (|| {
        if use_rdb_preamble {
            redis_core::rdb::save_rdb_databases(dbs, &temp_base_path)?;
        } else {
            write_base_aof_file(&temp_base_path, dbs)?;
        }
        std::fs::rename(&temp_base_path, &base_path)?;

        let final_manifest = AofManifest {
            base: Some(AofManifestFile {
                name: base_name.into_bytes(),
                seq: base_seq,
                file_type: AofManifestFileType::Base,
            }),
            incr: vec![AofManifestFile {
                name: incr_name.into_bytes(),
                seq: incr_seq,
                file_type: AofManifestFileType::Incr,
            }],
        };
        persist_aof_manifest(&manifest_path, &final_manifest)?;

        let base_size = base_path.metadata().map(|m| m.len()).unwrap_or(0);
        new_writer.flush()?;
        let incr_size = incr_path.metadata().map(|m| m.len()).unwrap_or(0);
        Ok((base_size, base_size.saturating_add(incr_size)))
    })();

    if result.is_err() {
        restore_aof_writer(previous_writer);
        let _ = std::fs::remove_file(&temp_base_path);
        let _ = std::fs::remove_file(&base_path);
        let _ = std::fs::remove_file(&incr_path);
    }

    result
}

fn aof_manifest_path(dir: &Path, appendfilename: &str, appenddirname: &str) -> PathBuf {
    dir.join(appenddirname)
        .join(format!("{}{}", appendfilename, AOF_MANIFEST_SUFFIX))
}

fn write_base_aof_file(path: &Path, dbs: &[RedisDb]) -> io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    let mut writer = BufWriter::new(file);
    write_aof_rewrite_for_dbs(dbs, &mut writer)?;
    writer.flush()?;
    writer.get_ref().sync_data()
}

fn persist_aof_manifest(path: &Path, manifest: &AofManifest) -> io::Result<()> {
    let tmp_path = path.with_extension("manifest.tmp");
    let data = encode_aof_manifest(manifest);
    std::fs::write(&tmp_path, data)?;
    std::fs::rename(tmp_path, path)
}

fn encode_aof_manifest(manifest: &AofManifest) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(base) = &manifest.base {
        encode_manifest_line(&mut out, base);
    }
    for incr in &manifest.incr {
        encode_manifest_line(&mut out, incr);
    }
    out
}

/// Whether a manifest filename must be written in quoted/escaped (`sdscatrepr`)
/// form. Mirrors `sdsneedsrepr`: any whitespace, quote, backslash, or
/// non-printable byte (and the empty string) forces quoting so the round-trip
/// survives `split_manifest_args`.
fn manifest_name_needs_repr(name: &[u8]) -> bool {
    name.is_empty()
        || name.iter().any(|&b| {
            b == b'\\'
                || b == b'"'
                || b == b'\''
                || b.is_ascii_whitespace()
                || !(0x20..=0x7e).contains(&b)
        })
}

/// Append a manifest filename, quoting and escaping it like `sdscatrepr` when
/// it contains characters that would otherwise break the whitespace-delimited
/// manifest line (spaces, quotes, backslashes, control bytes).
fn append_manifest_name(out: &mut Vec<u8>, name: &[u8]) {
    if !manifest_name_needs_repr(name) {
        out.extend_from_slice(name);
        return;
    }
    out.push(b'"');
    for &b in name {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'"' => out.extend_from_slice(b"\\\""),
            0x0a => out.extend_from_slice(b"\\n"),
            0x0d => out.extend_from_slice(b"\\r"),
            0x09 => out.extend_from_slice(b"\\t"),
            0x07 => out.extend_from_slice(b"\\a"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x20..=0x7e => out.push(b),
            other => out.extend_from_slice(format!("\\x{:02x}", other).as_bytes()),
        }
    }
    out.push(b'"');
}

fn encode_manifest_line(out: &mut Vec<u8>, file: &AofManifestFile) {
    out.extend_from_slice(b"file ");
    append_manifest_name(out, &file.name);
    out.extend_from_slice(b" seq ");
    out.extend_from_slice(file.seq.to_string().as_bytes());
    out.extend_from_slice(b" type ");
    let ty = match file.file_type {
        AofManifestFileType::Base => b'b',
        AofManifestFileType::Incr => b'i',
        AofManifestFileType::History => b'h',
    };
    out.push(ty);
    out.push(b'\n');
}

fn load_manifest_files(
    aof_dir: &Path,
    manifest: &AofManifest,
    dbs: &mut [RedisDb],
    options: AofLoadOptions,
) -> io::Result<(usize, u64)> {
    let files = manifest.load_sequence();
    if files.is_empty() {
        return Ok((0, 0));
    }

    let mut total_size = 0u64;
    for file in &files {
        let path = manifest_file_path(aof_dir, &file.name);
        let meta = path.metadata().map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "can't open the append log file {} for reading: {}",
                    manifest_name_for_log(&file.name),
                    e
                ),
            )
        })?;
        if !meta.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "append log file {} is not a regular file",
                    manifest_name_for_log(&file.name)
                ),
            ));
        }
        total_size = total_size.saturating_add(meta.len());
    }

    let mut replayed = 0usize;
    for (index, file) in files.iter().enumerate() {
        let path = manifest_file_path(aof_dir, &file.name);
        let is_last = index + 1 == files.len();
        match file.file_type {
            AofManifestFileType::Base if file.name.ends_with(b".rdb") => {
                let rdb_options = redis_core::rdb::RdbLoadOptions {
                    allow_dup: true,
                    skip_expired: true,
                    aof_preamble: true,
                };
                redis_core::rdb::load_into_dbs_with_options(dbs, &path, rdb_options)?;
            }
            AofManifestFileType::Base | AofManifestFileType::Incr => {
                let mut file_options = options.clone();
                file_options.load_truncated = options.load_truncated && is_last;
                file_options.allow_rdb_preamble = false;
                replayed += replay_aof_databases_with_options(&path, dbs, file_options)?;
            }
            AofManifestFileType::History => {}
        }
    }

    Ok((replayed, total_size))
}

fn load_aof_manifest(path: &Path) -> io::Result<AofManifest> {
    let data = std::fs::read(path)?;
    if data.is_empty() {
        return invalid_manifest("Found an empty AOF manifest");
    }

    let mut manifest = AofManifest::default();
    let mut max_incr_seq = 0i64;
    let mut pos = 0usize;
    let mut line_num = 0usize;

    while pos < data.len() {
        let Some(rel_end) = data[pos..].iter().position(|&b| b == b'\n') else {
            return invalid_manifest("The AOF manifest file contains too long line");
        };
        let end = pos + rel_end;
        let raw_line = &data[pos..=end];
        line_num += 1;
        pos = end + 1;

        if raw_line.len() > MANIFEST_MAX_LINE {
            return invalid_manifest("The AOF manifest file contains too long line");
        }
        if raw_line.first() == Some(&b'#') {
            continue;
        }

        let line = trim_manifest_line(raw_line);
        if line.is_empty() {
            return invalid_manifest_at("Invalid AOF manifest file format", line_num, line);
        }

        let argv = split_manifest_args(line)
            .map_err(|_| manifest_error_at("Invalid AOF manifest file format", line_num, line))?;
        if argv.len() < 6 || argv.len() % 2 != 0 {
            return invalid_manifest_at("Invalid AOF manifest file format", line_num, line);
        }

        let mut name: Option<Vec<u8>> = None;
        let mut seq: Option<i64> = None;
        let mut file_type: Option<u8> = None;

        for pair in argv.chunks_exact(2) {
            let key = &pair[0];
            let value = &pair[1];
            if key.eq_ignore_ascii_case(b"file") {
                if !path_is_base_name(value) {
                    return invalid_manifest_at(
                        "File can't be a path, just a filename",
                        line_num,
                        line,
                    );
                }
                name = Some(value.clone());
            } else if key.eq_ignore_ascii_case(b"seq") {
                seq = parse_i64_ascii(value);
            } else if key.eq_ignore_ascii_case(b"type") {
                file_type = value.first().copied();
            }
        }

        let Some(name) = name else {
            return invalid_manifest_at("Invalid AOF manifest file format", line_num, line);
        };
        let Some(seq) = seq.filter(|n| *n != 0) else {
            return invalid_manifest_at("Invalid AOF manifest file format", line_num, line);
        };
        let Some(raw_type) = file_type else {
            return invalid_manifest_at("Invalid AOF manifest file format", line_num, line);
        };

        let file_type = match raw_type {
            b'b' => AofManifestFileType::Base,
            b'i' => AofManifestFileType::Incr,
            b'h' => AofManifestFileType::History,
            _ => return invalid_manifest_at("Unknown AOF file type", line_num, line),
        };
        let item = AofManifestFile {
            name,
            seq,
            file_type,
        };

        match file_type {
            AofManifestFileType::Base => {
                if manifest.base.is_some() {
                    return invalid_manifest_at(
                        "Found duplicate base file information",
                        line_num,
                        line,
                    );
                }
                manifest.base = Some(item);
            }
            AofManifestFileType::Incr => {
                if item.seq <= max_incr_seq {
                    return invalid_manifest_at(
                        "Found a non-monotonic sequence number",
                        line_num,
                        line,
                    );
                }
                max_incr_seq = item.seq;
                manifest.incr.push(item);
            }
            AofManifestFileType::History => {}
        }
    }

    Ok(manifest)
}

fn split_manifest_args(line: &[u8]) -> io::Result<Vec<Vec<u8>>> {
    let mut args = Vec::new();
    let mut i = 0usize;
    while i < line.len() {
        while i < line.len() && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= line.len() {
            break;
        }

        let quote = match line[i] {
            b'\'' | b'"' => {
                let q = line[i];
                i += 1;
                Some(q)
            }
            _ => None,
        };
        let mut arg = Vec::new();
        if let Some(q) = quote {
            while i < line.len() && line[i] != q {
                if q == b'"' && line[i] == b'\\' && i + 1 < line.len() {
                    let c = line[i + 1];
                    match c {
                        b'x' if i + 3 < line.len()
                            && line[i + 2].is_ascii_hexdigit()
                            && line[i + 3].is_ascii_hexdigit() =>
                        {
                            let hi = (line[i + 2] as char).to_digit(16).unwrap() as u8;
                            let lo = (line[i + 3] as char).to_digit(16).unwrap() as u8;
                            arg.push(hi * 16 + lo);
                            i += 4;
                        }
                        b'n' => {
                            arg.push(b'\n');
                            i += 2;
                        }
                        b'r' => {
                            arg.push(b'\r');
                            i += 2;
                        }
                        b't' => {
                            arg.push(b'\t');
                            i += 2;
                        }
                        b'b' => {
                            arg.push(0x08);
                            i += 2;
                        }
                        b'a' => {
                            arg.push(0x07);
                            i += 2;
                        }
                        _ => {
                            arg.push(c);
                            i += 2;
                        }
                    }
                    continue;
                }
                if q == b'\'' && line[i] == b'\\' && i + 1 < line.len() && line[i + 1] == b'\'' {
                    arg.push(b'\'');
                    i += 2;
                    continue;
                }
                arg.push(line[i]);
                i += 1;
            }
            if i >= line.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unterminated quoted manifest argument",
                ));
            }
            i += 1;
            if i < line.len() && !line[i].is_ascii_whitespace() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest quote must end an argument",
                ));
            }
        } else {
            while i < line.len() && !line[i].is_ascii_whitespace() {
                arg.push(line[i]);
                i += 1;
            }
        }
        args.push(arg);
    }
    Ok(args)
}

fn trim_manifest_line(line: &[u8]) -> &[u8] {
    let mut start = 0usize;
    let mut end = line.len();
    while start < end && matches!(line[start], b' ' | b'\t' | b'\r' | b'\n') {
        start += 1;
    }
    while end > start && matches!(line[end - 1], b' ' | b'\t' | b'\r' | b'\n') {
        end -= 1;
    }
    &line[start..end]
}

/// Whether `path` is a bare filename with no directory component. Upstream
/// `pathIsBaseName` also rejects `\` for Windows, but on our POSIX targets a
/// backslash is an ordinary filename byte (not a path separator), and AOF
/// filenames legitimately containing one must round-trip — so only `/` is
/// treated as a separator here.
fn path_is_base_name(path: &[u8]) -> bool {
    !path.contains(&b'/')
}

fn parse_i64_ascii(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut i = 0usize;
    let mut sign = 1i64;
    if bytes[0] == b'-' {
        sign = -1;
        i = 1;
    } else if bytes[0] == b'+' {
        i = 1;
    }
    if i == bytes.len() {
        return None;
    }
    let mut out = 0i64;
    while i < bytes.len() {
        let b = bytes[i];
        if !b.is_ascii_digit() {
            return None;
        }
        out = out.checked_mul(10)?.checked_add((b - b'0') as i64)?;
        i += 1;
    }
    out.checked_mul(sign)
}

fn manifest_file_path(aof_dir: &Path, name: &[u8]) -> PathBuf {
    aof_dir.join(manifest_name_pathbuf(name))
}

#[cfg(unix)]
fn manifest_name_pathbuf(name: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    PathBuf::from(std::ffi::OsString::from_vec(name.to_vec()))
}

#[cfg(not(unix))]
fn manifest_name_pathbuf(name: &[u8]) -> PathBuf {
    PathBuf::from(String::from_utf8_lossy(name).into_owned())
}

fn manifest_name_for_log(name: &[u8]) -> String {
    String::from_utf8_lossy(name).into_owned()
}

fn invalid_manifest<T>(msg: &'static str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidData, msg))
}

fn invalid_manifest_at<T>(msg: &'static str, line_num: usize, line: &[u8]) -> io::Result<T> {
    Err(manifest_error_at(msg, line_num, line))
}

fn manifest_error_at(msg: &'static str, line_num: usize, line: &[u8]) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{} at line {}: {}",
            msg,
            line_num,
            String::from_utf8_lossy(line)
        ),
    )
}

/// Route `argv` through the full command-dispatch machinery against `db`.
///
/// Constructs a minimal synthetic client (no live transport, authenticated as
/// the default user) and calls [`crate::dispatch::dispatch_command_name`].
/// Errors and unknown commands are returned as replay failures.
fn dispatch_via_handler(argv: &[RedisString], db: &mut RedisDb) -> io::Result<()> {
    if argv.is_empty() {
        return Ok(());
    }
    let name = argv[0].clone();
    let mut client = Client::new(0);
    client.authenticated_user = Some(RedisString::from_bytes(b"default"));
    client.set_args(argv.to_vec());
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let mut ctx = CommandContext::with_server(&mut client, db, server, pubsub);
    let saved_writer = take_aof_writer();
    let result = crate::dispatch::dispatch_command_name(&mut ctx, name.as_bytes());
    restore_aof_writer(saved_writer);
    result.map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "AOF command {:?} failed: {}",
                String::from_utf8_lossy(name.as_bytes()),
                e
            ),
        )
    })?;
    if ctx.client_ref().reply_buf.starts_with(b"-") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "AOF command {:?} returned error: {}",
                String::from_utf8_lossy(name.as_bytes()),
                String::from_utf8_lossy(&ctx.client_ref().reply_buf)
            ),
        ));
    }
    Ok(())
}

/// Dispatch a single replayed command against `db` without a real client
/// context. Implements a minimal subset covering the commands emitted by
/// `write_aof_rewrite` and normal write operations.
///
/// Unknown or unsupported commands during replay are fatal. The small direct
/// cases keep common replay hot paths simple; other commands fall through to
/// the real command handler with AOF propagation suppressed.
fn dispatch_replay_command(argv: &[RedisString], db: &mut RedisDb) -> io::Result<()> {
    use redis_core::object::{ObjectKind, RedisObject, EXPIRY_NONE};

    if argv.is_empty() {
        return Ok(());
    }

    let name_lower: Vec<u8> = argv[0]
        .as_bytes()
        .iter()
        .map(|b| b.to_ascii_lowercase())
        .collect();

    match name_lower.as_slice() {
        b"set" if argv.len() >= 3 => {
            let key = argv[1].clone();
            let val = RedisObject::new_string(argv[2].as_bytes());
            db.insert(key, val);
        }
        b"setex" if argv.len() >= 4 => {
            let key = argv[1].clone();
            let ttl_sec: i64 = match std::str::from_utf8(argv[2].as_bytes())
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(n) => n,
                None => return invalid_aof_command("SETEX has invalid TTL"),
            };
            let now_ms = current_ms();
            let expire_ms = now_ms + ttl_sec * 1000;
            if expire_ms <= now_ms {
                return Ok(());
            }
            let mut val = RedisObject::new_string(argv[3].as_bytes());
            val.expire = expire_ms;
            db.insert(key, val);
        }
        b"rpush" if argv.len() >= 3 => {
            use std::collections::VecDeque;
            let key = argv[1].clone();
            let mut dq = match db.lookup_key_read(&key) {
                Some(obj) => obj.list().cloned().unwrap_or_default(),
                None => VecDeque::new(),
            };
            for elem in &argv[2..] {
                dq.push_back(elem.clone());
            }
            let obj = RedisObject::new_list_from_vec(dq);
            db.insert(key, obj);
        }
        b"hmset" if argv.len() >= 4 && (argv.len() - 2) % 2 == 0 => {
            let key = argv[1].clone();
            let mut map: InlineHash = match db.lookup_key_read(&key) {
                Some(obj) => match &obj.kind {
                    ObjectKind::Hash(HashEncoding::Inline(m) | HashEncoding::HashTable(m)) => {
                        m.clone()
                    }
                    _ => InlineHash::new(),
                },
                None => InlineHash::new(),
            };
            let mut i = 2;
            while i + 1 < argv.len() {
                map.insert(argv[i].clone(), argv[i + 1].clone());
                i += 2;
            }
            let obj = RedisObject {
                lru: 0,
                expire: EXPIRY_NONE,
                kind: ObjectKind::Hash(HashEncoding::Inline(map)),
            };
            db.insert(key, obj);
        }
        b"sadd" if argv.len() >= 3 => {
            use std::collections::HashSet;
            let key = argv[1].clone();
            let mut hs: HashSet<RedisString> = match db.lookup_key_read(&key) {
                Some(obj) => obj.set().cloned().unwrap_or_default(),
                None => HashSet::new(),
            };
            for m in &argv[2..] {
                hs.insert(m.clone());
            }
            let obj = RedisObject::new_set_from_set(hs);
            db.insert(key, obj);
        }
        b"zadd" if argv.len() >= 4 && (argv.len() - 2) % 2 == 0 => {
            use redis_core::object::{InlineZSet, ZSetEncoding};
            let key = argv[1].clone();
            let mut zs = match db.lookup_key_read(&key) {
                Some(obj) => match &obj.kind {
                    ObjectKind::ZSet(ZSetEncoding::Inline(z)) => z.clone(),
                    _ => InlineZSet::new(),
                },
                None => InlineZSet::new(),
            };
            let mut i = 2;
            while i + 1 < argv.len() {
                let score_str = std::str::from_utf8(argv[i].as_bytes()).unwrap_or("0");
                let score: f64 = match score_str {
                    "+inf" => f64::INFINITY,
                    "-inf" => f64::NEG_INFINITY,
                    s => s.parse().unwrap_or(0.0),
                };
                zs.upsert(argv[i + 1].clone(), score);
                i += 2;
            }
            let obj = RedisObject {
                lru: 0,
                expire: EXPIRY_NONE,
                kind: ObjectKind::ZSet(ZSetEncoding::Inline(zs)),
            };
            db.insert(key, obj);
        }
        b"lpush" if argv.len() >= 3 => {
            use std::collections::VecDeque;
            let key = argv[1].clone();
            let mut dq = match db.lookup_key_read(&key) {
                Some(obj) => obj.list().cloned().unwrap_or_default(),
                None => VecDeque::new(),
            };
            for elem in argv[2..].iter().rev() {
                dq.push_front(elem.clone());
            }
            let obj = RedisObject::new_list_from_vec(dq);
            db.insert(key, obj);
        }
        b"del" if argv.len() >= 2 => {
            for key in &argv[1..] {
                db.sync_delete(key);
            }
        }
        b"flushdb" | b"flushall" => {
            db.clear();
        }
        b"pexpireat" if argv.len() >= 3 => {
            let key = &argv[1];
            let expire_ms: i64 = match std::str::from_utf8(argv[2].as_bytes())
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(n) => n,
                None => return invalid_aof_command("PEXPIREAT has invalid timestamp"),
            };
            let now_ms = current_ms();
            if expire_ms <= now_ms {
                db.sync_delete(key);
            } else {
                db.set_expire(key, expire_ms);
            }
        }
        b"expire" if argv.len() >= 3 => {
            let key = &argv[1];
            let ttl_sec: i64 = match std::str::from_utf8(argv[2].as_bytes())
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(n) => n,
                None => return invalid_aof_command("EXPIRE has invalid TTL"),
            };
            let expire_ms = current_ms() + ttl_sec * 1000;
            db.set_expire(key, expire_ms);
        }
        b"hset" if argv.len() >= 4 && (argv.len() - 2) % 2 == 0 => {
            let key = argv[1].clone();
            let mut map: InlineHash = match db.lookup_key_read(&key) {
                Some(obj) => match &obj.kind {
                    ObjectKind::Hash(HashEncoding::Inline(m) | HashEncoding::HashTable(m)) => {
                        m.clone()
                    }
                    _ => InlineHash::new(),
                },
                None => InlineHash::new(),
            };
            let mut i = 2;
            while i + 1 < argv.len() {
                map.insert(argv[i].clone(), argv[i + 1].clone());
                i += 2;
            }
            let obj = RedisObject {
                lru: 0,
                expire: EXPIRY_NONE,
                kind: ObjectKind::Hash(HashEncoding::Inline(map)),
            };
            db.insert(key, obj);
        }
        b"xadd" if argv.len() >= 5 => {
            dispatch_via_handler(argv, db)?;
        }
        b"xdel" if argv.len() >= 3 => {
            dispatch_via_handler(argv, db)?;
        }
        b"xsetid" if argv.len() >= 3 => {
            dispatch_via_handler(argv, db)?;
        }
        b"xgroup" if argv.len() >= 4 => {
            dispatch_via_handler(argv, db)?;
        }
        b"xclaim" if argv.len() >= 6 => {
            dispatch_via_handler(argv, db)?;
        }
        _ => {
            dispatch_via_handler(argv, db)?;
        }
    }
    Ok(())
}

fn invalid_aof_command<T>(msg: &'static str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidData, msg))
}

fn current_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn parse_usize_ascii(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    let mut out: usize = 0;
    for b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        out = out.checked_mul(10)?.checked_add((b - b'0') as usize)?;
    }
    Some(out)
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/aof.c feedAppendOnlyFile/replay path
//   target_crate:  redis-commands
//   confidence:    medium
//   todos:         0
//   port_notes:    1
//   unsafe_blocks: 0
//   notes:         AOF append/rewrite/replay now preserves logical DB
//                  selection for RuntimeOwner-owned DB slices.
// ──────────────────────────────────────────────────────────────────────────
