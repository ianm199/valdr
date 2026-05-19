//! Append-Only File (AOF) persistence.
//!
//! `AofWriter` encodes every write command as a RESP multibulk array and
//! appends it to the AOF file. The fsync policy is governed by the
//! `appendfsync` config key: `always` (sync after every append), `everysec`
//! (background sync thread fsyncs once per second), or `no` (OS decides).
//!
//! `BGREWRITEAOF` walks the current DB and emits the minimal command sequence
//! needed to reconstruct each key, then atomically swaps the new file over the
//! old one. The child process pattern mirrors `BGSAVE`.
//!
//! Replay on startup: `replay_aof` reads the file line-by-line using the same
//! RESP parser as the network path and dispatches each command through the
//! normal handler machinery.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use redis_core::db::RedisDb;
use redis_core::object::{ObjectKind, EXPIRY_NONE};
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

/// Append-only file writer.
pub struct AofWriter {
    pub path: PathBuf,
    file: Mutex<BufWriter<File>>,
    pub pending_bytes: AtomicUsize,
    pub fsync_policy: AtomicU8,
}

impl AofWriter {
    /// Open (or create) the AOF file at `path`.
    pub fn open(path: &Path, fsync_policy: u8) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file: Mutex::new(BufWriter::new(file)),
            pending_bytes: AtomicUsize::new(0),
            fsync_policy: AtomicU8::new(fsync_policy),
        })
    }

    /// Encode `argv` as a RESP multibulk command and append it to the file.
    ///
    /// When the fsync policy is `FSYNC_ALWAYS`, flushes and fsyncs before
    /// returning. Otherwise the everysec background thread or the OS handles
    /// durability.
    pub fn append(&self, argv: &[RedisString]) -> io::Result<()> {
        let encoded = encode_resp_command(argv);
        let len = encoded.len();
        {
            let mut guard = match self.file.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.write_all(&encoded)?;
            if self.fsync_policy.load(Ordering::Relaxed) == FSYNC_ALWAYS {
                guard.flush()?;
                guard.get_ref().sync_data()?;
                self.pending_bytes.store(0, Ordering::Relaxed);
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
        Ok(())
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
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

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
                    redis_core::object::ListEncoding::Inline(dq) => {
                        dq.iter().cloned().collect()
                    }
                    redis_core::object::ListEncoding::QuickList(v) => v.clone(),
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
                    redis_core::object::SetEncoding::Inline(hs) => {
                        hs.iter().cloned().collect()
                    }
                    redis_core::object::SetEncoding::HashTable(hs) => {
                        hs.iter().cloned().collect()
                    }
                    redis_core::object::SetEncoding::IntSet(v) => {
                        v.iter().map(|n| RedisString::from_vec(n.to_string().into_bytes())).collect()
                    }
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
        xsetid.push(RedisString::from_vec(stream.max_deleted_id.to_display_bytes()));
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
    use redis_core::object::EXPIRY_NONE;
    use redis_protocol::parse_inline_or_multibulk;

    let data = std::fs::read(path)?;
    let mut buf: &[u8] = &data;
    let mut replayed = 0usize;

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
                dispatch_replay_command(&argv, db);
                replayed += 1;
            }
            Ok(None) => {
                break;
            }
            Err(_) => {
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    eprintln!(
                        "redis-server: AOF parse error, skipping line: {:?}",
                        std::str::from_utf8(&buf[..pos]).unwrap_or("<binary>")
                    );
                    buf = &buf[pos + 1..];
                } else {
                    break;
                }
            }
        }
    }

    Ok(replayed)
}

/// Route `argv` through the full command-dispatch machinery against `db`.
///
/// Constructs a minimal synthetic client (no live transport, authenticated as
/// the default user) and calls [`crate::dispatch::dispatch_command_name`].
/// Errors and unknown commands are silently dropped — replay is best-effort.
fn dispatch_via_handler(argv: &[RedisString], db: &mut RedisDb) {
    if argv.is_empty() {
        return;
    }
    let name = argv[0].clone();
    let mut client = Client::new(0);
    client.authenticated_user = Some(RedisString::from_bytes(b"default"));
    client.set_args(argv.to_vec());
    let server = Arc::new(RedisServer::default());
    let pubsub = Arc::new(Mutex::new(PubSubRegistry::new()));
    let mut ctx = CommandContext::with_server(&mut client, db, server, pubsub);
    let _ = crate::dispatch::dispatch_command_name(&mut ctx, name.as_bytes());
}

/// Dispatch a single replayed command against `db` without a real client
/// context. Implements a minimal subset covering the commands emitted by
/// `write_aof_rewrite` and normal write operations.
///
/// Unknown or unsupported commands during replay are silently skipped.
fn dispatch_replay_command(argv: &[RedisString], db: &mut RedisDb) {
    use redis_core::object::{RedisObject, StringEncoding, ObjectKind, EXPIRY_NONE};

    if argv.is_empty() {
        return;
    }

    let name_lower: Vec<u8> = argv[0].as_bytes().iter().map(|b| b.to_ascii_lowercase()).collect();

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
                None => return,
            };
            let now_ms = current_ms();
            let expire_ms = now_ms + ttl_sec * 1000;
            if expire_ms <= now_ms {
                return;
            }
            let mut val = RedisObject::new_string(argv[3].as_bytes());
            val.expire = expire_ms;
            db.insert(key, val);
        }
        b"rpush" if argv.len() >= 3 => {
            use redis_core::object::ListEncoding;
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
            use std::collections::HashMap;
            let key = argv[1].clone();
            let mut map: HashMap<RedisString, RedisString> = match db.lookup_key_read(&key) {
                Some(obj) => match &obj.kind {
                    ObjectKind::Hash(redis_core::object::HashEncoding::Inline(m)) => m.clone(),
                    _ => HashMap::new(),
                },
                None => HashMap::new(),
            };
            let mut i = 2;
            while i + 1 < argv.len() {
                map.insert(argv[i].clone(), argv[i + 1].clone());
                i += 2;
            }
            let obj = RedisObject {
                lru: 0,
                expire: EXPIRY_NONE,
                kind: ObjectKind::Hash(redis_core::object::HashEncoding::Inline(map)),
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
                None => return,
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
                None => return,
            };
            let expire_ms = current_ms() + ttl_sec * 1000;
            db.set_expire(key, expire_ms);
        }
        b"hset" if argv.len() >= 4 && (argv.len() - 2) % 2 == 0 => {
            use std::collections::HashMap;
            let key = argv[1].clone();
            let mut map: HashMap<RedisString, RedisString> = match db.lookup_key_read(&key) {
                Some(obj) => match &obj.kind {
                    ObjectKind::Hash(redis_core::object::HashEncoding::Inline(m)) => m.clone(),
                    _ => HashMap::new(),
                },
                None => HashMap::new(),
            };
            let mut i = 2;
            while i + 1 < argv.len() {
                map.insert(argv[i].clone(), argv[i + 1].clone());
                i += 2;
            }
            let obj = RedisObject {
                lru: 0,
                expire: EXPIRY_NONE,
                kind: ObjectKind::Hash(redis_core::object::HashEncoding::Inline(map)),
            };
            db.insert(key, obj);
        }
        b"xadd" if argv.len() >= 5 => {
            dispatch_via_handler(argv, db);
        }
        b"xdel" if argv.len() >= 3 => {
            dispatch_via_handler(argv, db);
        }
        b"xsetid" if argv.len() >= 3 => {
            dispatch_via_handler(argv, db);
        }
        b"xgroup" if argv.len() >= 4 => {
            dispatch_via_handler(argv, db);
        }
        b"xclaim" if argv.len() >= 6 => {
            dispatch_via_handler(argv, db);
        }
        _ => {}
    }
}

fn current_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
