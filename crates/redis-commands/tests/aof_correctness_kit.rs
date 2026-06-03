//! Fast in-memory iteration harness for the AOF persistence subsystem.
//! This is the rung-2 inner loop for AOF work: append a write through the LIVE
//! `AofWriter` encoder, replay the produced bytes back through the LIVE
//! `replay_aof_databases_with_options` parser/dispatcher, and assert
//! reconstructed keyspace equals the original — all in a tmpdir,
//! milliseconds, with no sockets, no server process, and no tclsh. It mirrors
//! `harness/oracle/persistence-cycle.py` but runs deterministically as a unit
//! test.
//! House style is borrowed from `crates/redis-core/tests/conn_transport_kit.rs`:
//! a small scriptable in-memory mechanism (here an append->replay cycle plus a
//! `FailingSink` that returns ENOSPC on demand) that makes a durability /
//! replay bug reproduce 100% of the time instead of "sometimes".
//! Run just this loop:
//! cargo test -p redis-commands --test aof_correctness_kit
//! Test taxonomy:
//! * GREEN ANCHOR — `anchor_six_types_roundtrip_under_always` proves
//! append->replay capture is faithful before any red test is trusted.
//! * RED — reproduces a real audit bug on its documented assertion.
//! * GREEN-LOCK — behavior is already correct; kept as a regression lock.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use redis_commands::aof::{
    aof_timestamp_enabled, append_raw_for_dispatch, append_selected_for_dispatch,
    begin_thread_aof_batch, debug_aof_flush_sleep_micros, encode_resp_command,
    finish_thread_aof_batch, flush_thread_aof_batch_for_lifecycle, load_append_only_files,
    record_aof_append_result, replay_aof_databases_with_options,
    rewrite_manifest_aof_disabled_from_dbs, set_aof_timestamp_enabled,
    set_debug_aof_flush_sleep_micros, write_aof_rewrite_for_dbs, AofLoadOptions, AofWriter,
    FSYNC_ALWAYS, FSYNC_EVERYSEC, FSYNC_NO,
};
use redis_core::db::{RedisDb, LOOKUP_NONE};
use redis_core::object::{ObjectKind, RedisObject, EXPIRY_NONE};
use redis_core::persistence::{PersistenceState, PersistenceStatus};
use redis_types::RedisString;
use std::sync::{Arc, Mutex};

// ─── tmpdir plumbing (no tempfile dev-dep available) ─────────────────────────

static COUNTER: AtomicU64 = AtomicU64::new(0);
static AOF_GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

/// A unique scratch directory under the OS temp dir, removed on drop. No
/// external crate; we only have `std` here because the crate has no
/// `[dev-dependencies]`.
struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("aof_kit_{tag}_{pid}_{n}"));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        Scratch { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct ResetAofFlushSleep;

impl Drop for ResetAofFlushSleep {
    fn drop(&mut self) {
        set_debug_aof_flush_sleep_micros(0);
    }
}

struct ResetAofTimestamp;

impl Drop for ResetAofTimestamp {
    fn drop(&mut self) {
        set_aof_timestamp_enabled(false);
    }
}

// ─── the append -> replay cycle (the core kit mechanism) ─────────────────────

/// A serializable snapshot of one logical DB's keyspace, comparable for
/// equality. Captures kind, value, and TTL-presence so the round-trip assertion
/// is meaningful for every plain type.
#[derive(Debug, PartialEq, Eq)]
struct DbSnapshot {
    entries: Vec<KeySnapshot>,
}

#[derive(Debug, PartialEq, Eq)]
struct KeySnapshot {
    key: Vec<u8>,
    value: String,
    has_ttl: bool,
}

fn snapshot(db: &RedisDb) -> DbSnapshot {
    let mut entries: Vec<KeySnapshot> = db
        .iter_for_eviction()
        .map(|(k, obj)| KeySnapshot {
            key: k.as_bytes().to_vec(),
            value: render_value(obj),
            has_ttl: obj.expire != EXPIRY_NONE,
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    DbSnapshot { entries }
}

/// A canonical, order-independent string rendering of an object's value so two
/// objects holding the same logical content compare equal regardless
/// encoding or internal ordering.
fn render_value(obj: &RedisObject) -> String {
    use redis_core::object::{
        HashEncoding, ListEncoding, SetEncoding, StringEncoding, ZSetEncoding,
    };
    match &obj.kind {
        ObjectKind::String(enc) => {
            let bytes = match enc {
                StringEncoding::Raw(s) | StringEncoding::Embstr(s) => s.as_bytes().to_vec(),
                StringEncoding::Int(n) => n.to_string().into_bytes(),
            };
            format!("str:{}", String::from_utf8_lossy(&bytes))
        }
        ObjectKind::List(enc) => {
            let items: Vec<String> = match enc {
                ListEncoding::Inline(d) | ListEncoding::QuickList(d) => {
                    d.iter().map(|x| lossy(x)).collect()
                }
                ListEncoding::ListPack(_) => Vec::new(),
            };
            format!("list:[{}]", items.join(","))
        }
        ObjectKind::Hash(enc) => {
            let mut pairs: Vec<String> = match enc {
                HashEncoding::Inline(m) | HashEncoding::HashTable(m) => m
                    .iter()
                    .map(|(f, v)| format!("{}={}", lossy(f), lossy(v)))
                    .collect(),
                HashEncoding::ListPack(_) => Vec::new(),
            };
            pairs.sort();
            format!("hash:{{{}}}", pairs.join(","))
        }
        ObjectKind::Set(enc) => {
            let mut members: Vec<String> = match enc {
                SetEncoding::Inline(s) => s.data.iter().map(|x| lossy(x)).collect(),
                SetEncoding::HashTable(hs) => hs.iter().map(|x| lossy(x)).collect(),
                SetEncoding::IntSet(v) => v.iter().map(|n| n.to_string()).collect(),
                SetEncoding::ListPack(_) => Vec::new(),
            };
            members.sort();
            format!("set:{{{}}}", members.join(","))
        }
        ObjectKind::ZSet(enc) => {
            let mut pairs: Vec<String> = match enc {
                ZSetEncoding::Inline(z) => z
                    .iter_ascending()
                    .map(|(s, m)| format!("{}:{}", lossy(m), s))
                    .collect(),
                ZSetEncoding::SkipList(v) => v
                    .iter()
                    .map(|(m, s)| format!("{}:{}", lossy(m), s))
                    .collect(),
                ZSetEncoding::ListPack(_) => Vec::new(),
            };
            pairs.sort();
            format!("zset:{{{}}}", pairs.join(","))
        }
        other => format!("other:{:?}", std::mem::discriminant(other)),
    }
}

fn lossy(s: &RedisString) -> String {
    String::from_utf8_lossy(s.as_bytes()).into_owned()
}

fn rs(b: &[u8]) -> RedisString {
    RedisString::from_bytes(b)
}

/// Replay an AOF file at `path` into a single fresh DB and snapshot it.
fn replay_into_fresh(path: &Path, options: AofLoadOptions) -> io::Result<DbSnapshot> {
    let mut dbs = vec![RedisDb::new(0)];
    replay_aof_databases_with_options(path, &mut dbs, options)?;
    Ok(snapshot(&dbs[0]))
}

// ─── GREEN ANCHOR ────────────────────────────────────────────────────────────

/// Build a source DB holding all six plain key types (one of them volatile),
/// append the reconstruction commands through the LIVE `AofWriter`, replay
/// produced bytes into a fresh DB, and assert the keyspaces are equal.
/// This is the faithfulness proof: a non-trivial assert that exercises the real
/// encode -> file -> parse -> dispatch cycle for every type the kit later
/// stresses. If this passes, the kit's capture mechanism is trustworthy.
#[test]
fn anchor_six_types_roundtrip_under_always() {
    let scratch = Scratch::new("anchor");
    let aof_path = scratch.path("appendonly.aof");
    let writer = AofWriter::open(&aof_path, FSYNC_ALWAYS).expect("open aof");

    // string
    writer
        .append(&[rs(b"SET"), rs(b"k:str"), rs(b"hello world")])
        .unwrap();
    // list
    writer
        .append(&[rs(b"RPUSH"), rs(b"k:list"), rs(b"a"), rs(b"b"), rs(b"c")])
        .unwrap();
    // hash
    writer
        .append(&[
            rs(b"HMSET"),
            rs(b"k:hash"),
            rs(b"f1"),
            rs(b"v1"),
            rs(b"f2"),
            rs(b"v2"),
        ])
        .unwrap();
    // set
    writer
        .append(&[rs(b"SADD"), rs(b"k:set"), rs(b"x"), rs(b"y"), rs(b"z")])
        .unwrap();
    // zset
    writer
        .append(&[
            rs(b"ZADD"),
            rs(b"k:zset"),
            rs(b"1"),
            rs(b"one"),
            rs(b"2"),
            rs(b"two"),
        ])
        .unwrap();
    // volatile string (TTL ~1000s in the future)
    let expire_at = current_ms() + 1_000_000;
    writer
        .append(&[rs(b"SET"), rs(b"k:vol"), rs(b"ephemeral")])
        .unwrap();
    writer
        .append(&[
            rs(b"PEXPIREAT"),
            rs(b"k:vol"),
            rs(expire_at.to_string().as_bytes()),
        ])
        .unwrap();
    writer.flush().unwrap();

    // Build the expected DB independently (not from the AOF).
    let mut expected = RedisDb::new(0);
    expected.insert(rs(b"k:str"), RedisObject::new_string(b"hello world"));
    let mut list = std::collections::VecDeque::new();
    list.push_back(rs(b"a"));
    list.push_back(rs(b"b"));
    list.push_back(rs(b"c"));
    expected.insert(rs(b"k:list"), RedisObject::new_list_from_vec(list));
    {
        let mut m = std::collections::HashMap::new();
        m.insert(rs(b"f1"), rs(b"v1"));
        m.insert(rs(b"f2"), rs(b"v2"));
        expected.insert(rs(b"k:hash"), RedisObject::new_hash_from_map(m));
    }
    {
        let mut hs = std::collections::HashSet::new();
        hs.insert(rs(b"x"));
        hs.insert(rs(b"y"));
        hs.insert(rs(b"z"));
        expected.insert(rs(b"k:set"), RedisObject::new_set_from_set(hs));
    }
    {
        let mut z = redis_core::object::InlineZSet::new();
        z.upsert(rs(b"one"), 1.0);
        z.upsert(rs(b"two"), 2.0);
        expected.insert(rs(b"k:zset"), RedisObject::new_zset_from_inline(z));
    }
    {
        let mut vol = RedisObject::new_string(b"ephemeral");
        vol.expire = expire_at;
        expected.insert(rs(b"k:vol"), vol);
    }

    let replayed = replay_into_fresh(&aof_path, AofLoadOptions::default()).unwrap();
    assert_eq!(
        replayed,
        snapshot(&expected),
        "append->replay must reconstruct all six plain types exactly"
    );
}

// ─── append failure status ───────────────────────────────────────────────────

/// A sink that fails every write with ENOSPC, used to drive the durability
/// decision the dispatch tail makes.
struct FailingSink;

impl Write for FailingSink {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "ENOSPC: No space left on device",
        ))
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "ENOSPC: No space left on device",
        ))
    }
}

#[test]
fn disk_full_append_failure_sets_operator_status_err() {
    let persistence = PersistenceState::new();
    assert_eq!(persistence.aof_last_write_status(), PersistenceStatus::Ok);

    // The write genuinely fails ENOSPC through the failing sink.
    let mut sink = FailingSink;
    let encoded = encode_resp_command(&[rs(b"SET"), rs(b"k"), rs(b"v")]);
    let append_result = sink.write_all(&encoded);
    assert!(
        append_result.is_err(),
        "FailingSink must produce the ENOSPC error"
    );

    // Run the same production status helper used by dispatch.rs and multi.rs.
    assert!(!record_aof_append_result(
        &persistence,
        "AOF append failed",
        append_result
    ));

    // A durability failure must be visible through INFO persistence.
    assert_eq!(
        persistence.aof_last_write_status(),
        PersistenceStatus::Err,
        "ENOSPC on AOF append must set aof_last_write_status=ERR"
    );

    assert!(record_aof_append_result(
        &persistence,
        "AOF append failed",
        Ok(())
    ));
    assert_eq!(
        persistence.aof_last_write_status(),
        PersistenceStatus::Ok,
        "a later successful AOF append should restore aof_last_write_status=OK"
    );
}

// ─── audit finding 2: FSYNC policies + replay (GREEN-LOCK / no oracle) ───────

/// Under each fsync policy, the appended bytes must replay into an identical
/// keyspace. An injectable clock drives the everysec window deterministically:
/// instead of waiting one real second, we call `fsync_if_due` (the
/// drain the everysec background thread performs) to force the pending bytes
/// disk, then replay. This proves the policy choice does not change replayed
/// content.
#[test]
fn fsync_policies_all_replay_identically() {
    let payload: Vec<(RedisString, &[u8])> =
        vec![(rs(b"a"), b"1"), (rs(b"b"), b"2"), (rs(b"c"), b"3")];

    let mut snapshots = Vec::new();
    for (label, policy) in [
        ("no", FSYNC_NO),
        ("everysec", FSYNC_EVERYSEC),
        ("always", FSYNC_ALWAYS),
    ] {
        let scratch = Scratch::new(&format!("fsync_{label}"));
        let aof_path = scratch.path("appendonly.aof");
        let writer = AofWriter::open(&aof_path, policy).expect("open aof");
        for (k, v) in &payload {
            writer.append(&[rs(b"SET"), k.clone(), rs(v)]).unwrap();
        }
        // Deterministic everysec window: force the once-per-second drain now,
        // rather than sleeping. For NO/ALWAYS this is a harmless flush.
        writer.fsync_if_due().expect("fsync_if_due drain");
        writer.flush().unwrap();

        let snap = replay_into_fresh(&aof_path, AofLoadOptions::default()).unwrap();
        snapshots.push((label, snap));
    }

    let (_, ref reference) = snapshots[0];
    for (label, snap) in &snapshots[1..] {
        assert_eq!(
            snap, reference,
            "policy {label} produced a divergent replay"
        );
    }
    // Sanity: the reference actually has the three keys.
    assert_eq!(reference.entries.len(), 3);
}

#[test]
fn debug_aof_flush_sleep_delays_pre_write_flush_path() {
    let _reset = ResetAofFlushSleep;
    let scratch = Scratch::new("debug_aof_flush_sleep");
    let aof_path = scratch.path("appendonly.aof");
    let writer = AofWriter::open(&aof_path, FSYNC_ALWAYS).expect("open aof");

    set_debug_aof_flush_sleep_micros(75_000);
    assert_eq!(debug_aof_flush_sleep_micros(), 75_000);

    let start = Instant::now();
    writer
        .append(&[rs(b"SET"), rs(b"sleep:probe"), rs(b"1")])
        .expect("append with debug sleep");
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(50),
        "DEBUG AOF-FLUSH-SLEEP must delay the pre-write flush path; elapsed={elapsed:?}"
    );
    assert!(std::fs::metadata(&aof_path).unwrap().len() > 0);
}

#[test]
fn aof_timestamp_annotations_prefix_append_and_rewrite_when_enabled() {
    let _global = AOF_GLOBAL_TEST_LOCK.lock().expect("AOF global test lock");
    let _reset = ResetAofTimestamp;
    let scratch = Scratch::new("aof_timestamp_annotations");
    let aof_path = scratch.path("appendonly.aof");
    let writer = AofWriter::open(&aof_path, FSYNC_EVERYSEC).expect("open aof");

    set_aof_timestamp_enabled(true);
    assert!(aof_timestamp_enabled());
    writer
        .append(&[rs(b"SET"), rs(b"timestamp:append"), rs(b"1")])
        .expect("append timestamped command");
    let bytes = std::fs::read(&aof_path).expect("read timestamped append");
    assert!(
        bytes.starts_with(b"#TS:"),
        "enabled AOF timestamp annotations must prefix the next append"
    );

    let mut db = RedisDb::new(0);
    db.insert(rs(b"timestamp:rewrite"), RedisObject::new_string(b"2"));
    let mut rewrite = Vec::new();
    write_aof_rewrite_for_dbs(&[db], &mut rewrite).expect("rewrite timestamped aof");
    assert!(
        rewrite.starts_with(b"#TS:"),
        "enabled AOF timestamp annotations must prefix rewrite output"
    );
}

#[test]
fn rewrite_snapshots_loaded_functions_before_keyspace() {
    let _global = AOF_GLOBAL_TEST_LOCK.lock().expect("AOF global test lock");
    let _reset_timestamp = ResetAofTimestamp;
    set_aof_timestamp_enabled(false);
    let scratch = Scratch::new("rewrite_functions");
    let seed_path = scratch.path("seed.aof");
    let rewrite_path = scratch.path("rewrite.aof");
    let verify_path = scratch.path("verify.aof");
    let library = b"#!lua name=test\nserver.register_function('test', function() return 1 end)\n";

    std::fs::write(
        &seed_path,
        [
            encode_resp_command(&[rs(b"FUNCTION"), rs(b"FLUSH")]),
            encode_resp_command(&[rs(b"FUNCTION"), rs(b"LOAD"), rs(library)]),
        ]
        .concat(),
    )
    .unwrap();
    let mut dbs = vec![RedisDb::new(0)];
    replay_aof_databases_with_options(&seed_path, &mut dbs, AofLoadOptions::default())
        .expect("seed FUNCTION LOAD must replay");

    let mut rewrite = Vec::new();
    write_aof_rewrite_for_dbs(&dbs, &mut rewrite).expect("rewrite with functions");
    assert!(
        rewrite.starts_with(b"*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n"),
        "rewrite must emit FUNCTION LOAD before DB SELECT/key records"
    );
    assert!(
        rewrite
            .windows(library.len())
            .any(|window| window == library),
        "rewrite must preserve original library source"
    );

    std::fs::write(&rewrite_path, &rewrite).unwrap();
    std::fs::write(
        &verify_path,
        [
            encode_resp_command(&[rs(b"FUNCTION"), rs(b"FLUSH")]),
            std::fs::read(&rewrite_path).unwrap(),
            encode_resp_command(&[rs(b"FCALL"), rs(b"test"), rs(b"0")]),
        ]
        .concat(),
    )
    .unwrap();
    let mut verify_dbs = vec![RedisDb::new(0)];
    replay_aof_databases_with_options(&verify_path, &mut verify_dbs, AofLoadOptions::default())
        .expect("rewritten FUNCTION LOAD must make FCALL replayable after flush");

    std::fs::write(
        scratch.path("cleanup.aof"),
        encode_resp_command(&[rs(b"FUNCTION"), rs(b"FLUSH")]),
    )
    .unwrap();
    let mut cleanup_dbs = vec![RedisDb::new(0)];
    replay_aof_databases_with_options(
        &scratch.path("cleanup.aof"),
        &mut cleanup_dbs,
        AofLoadOptions::default(),
    )
    .expect("cleanup function registry");
}

#[test]
fn disabled_bgrewriteaof_replaces_stale_manifest_with_function_snapshot() {
    let _global = AOF_GLOBAL_TEST_LOCK.lock().expect("AOF global test lock");
    let _reset_timestamp = ResetAofTimestamp;
    set_aof_timestamp_enabled(false);
    let scratch = Scratch::new("disabled_rewrite_functions");
    let appendfile = "appendonly.aof";
    let appenddir = "appendonlydir";
    let aof_dir = scratch.path(appenddir);
    std::fs::create_dir_all(&aof_dir).unwrap();
    std::fs::write(
        aof_dir.join("appendonly.aof.1.base.aof"),
        encode_resp_command(&[rs(b"SET"), rs(b"stale"), rs(b"old")]),
    )
    .unwrap();
    std::fs::write(aof_dir.join("appendonly.aof.1.incr.aof"), b"").unwrap();
    write_manifest(
        &scratch,
        appenddir,
        appendfile,
        "file appendonly.aof.1.base.aof seq 1 type b\n\
         file appendonly.aof.1.incr.aof seq 1 type i\n",
    );

    let library = b"#!lua name=test\nserver.register_function('test', function() return 1 end)\n";
    let seed_path = scratch.path("disabled-seed.aof");
    std::fs::write(
        &seed_path,
        [
            encode_resp_command(&[rs(b"FUNCTION"), rs(b"FLUSH")]),
            encode_resp_command(&[rs(b"FUNCTION"), rs(b"LOAD"), rs(library)]),
        ]
        .concat(),
    )
    .unwrap();
    let mut dbs = vec![RedisDb::new(0)];
    replay_aof_databases_with_options(&seed_path, &mut dbs, AofLoadOptions::default())
        .expect("seed FUNCTION LOAD must replay");

    let (base_size, current_size) =
        rewrite_manifest_aof_disabled_from_dbs(&scratch.dir, appendfile, appenddir, &dbs, false)
            .expect("disabled AOF rewrite");
    assert_eq!(base_size, current_size);

    let manifest = std::fs::read_to_string(aof_dir.join("appendonly.aof.manifest")).unwrap();
    assert!(manifest.contains("file appendonly.aof.2.base.aof seq 2 type b"));
    assert!(
        !manifest.contains("type i"),
        "disabled rewrite must not publish an active INCR"
    );
    let base = std::fs::read(aof_dir.join("appendonly.aof.2.base.aof")).unwrap();
    assert!(base.windows(library.len()).any(|window| window == library));
    assert!(
        !base
            .windows(b"stale".len())
            .any(|window| window == b"stale"),
        "disabled rewrite must replace the stale on-disk BASE"
    );

    std::fs::write(
        scratch.path("disabled-flush.aof"),
        encode_resp_command(&[rs(b"FUNCTION"), rs(b"FLUSH")]),
    )
    .unwrap();
    let mut flush_dbs = vec![RedisDb::new(0)];
    replay_aof_databases_with_options(
        &scratch.path("disabled-flush.aof"),
        &mut flush_dbs,
        AofLoadOptions::default(),
    )
    .expect("flush before manifest load");

    let mut loaded = vec![RedisDb::new(0)];
    load_append_only_files(
        &scratch.dir,
        appendfile,
        appenddir,
        &mut loaded,
        AofLoadOptions::default(),
    )
    .expect("disabled rewrite manifest must load");
    assert_eq!(snapshot(&loaded[0]).entries.len(), 0);

    let fcall_path = scratch.path("disabled-fcall.aof");
    std::fs::write(
        &fcall_path,
        encode_resp_command(&[rs(b"FCALL"), rs(b"test"), rs(b"0")]),
    )
    .unwrap();
    replay_aof_databases_with_options(&fcall_path, &mut loaded, AofLoadOptions::default())
        .expect("loaded disabled rewrite function must be callable");
}

#[test]
fn appendfsync_always_thread_batch_syncs_once_for_many_commands() {
    let scratch = Scratch::new("always_batch_once");
    let aof_path = scratch.path("appendonly.aof");
    let writer = Arc::new(AofWriter::open(&aof_path, FSYNC_ALWAYS).expect("open aof"));
    let persistence = PersistenceState::new();

    let _ = finish_thread_aof_batch(&persistence);
    begin_thread_aof_batch();
    assert!(append_selected_for_dispatch(
        &persistence,
        "AOF append failed",
        Arc::clone(&writer),
        0,
        &[rs(b"SET"), rs(b"k1"), rs(b"v1")],
        11,
    ));
    assert!(append_selected_for_dispatch(
        &persistence,
        "AOF append failed",
        Arc::clone(&writer),
        0,
        &[rs(b"SET"), rs(b"k2"), rs(b"v2")],
        22,
    ));

    assert_eq!(writer.fsync_count(), 0, "staged appends must not fsync");
    assert_eq!(
        writer.current_size(),
        0,
        "staged bytes are not visible until flush"
    );
    assert!(finish_thread_aof_batch(&persistence));

    assert_eq!(writer.fsync_count(), 1, "one batch flush should fsync once");
    assert_eq!(writer.fsynced_repl_offset(), 22);
    assert_eq!(persistence.aof_last_write_status(), PersistenceStatus::Ok);
    let snap = replay_into_fresh(&aof_path, AofLoadOptions::default()).unwrap();
    assert_eq!(snap.entries.len(), 2);
}

#[test]
fn appendfsync_always_transaction_envelope_batches_as_one_flush() {
    let scratch = Scratch::new("always_batch_tx");
    let aof_path = scratch.path("appendonly.aof");
    let writer = Arc::new(AofWriter::open(&aof_path, FSYNC_ALWAYS).expect("open aof"));
    let persistence = PersistenceState::new();

    let _ = finish_thread_aof_batch(&persistence);
    begin_thread_aof_batch();
    assert!(append_raw_for_dispatch(
        &persistence,
        "transaction AOF append failed",
        Arc::clone(&writer),
        &[rs(b"MULTI")],
        -1,
    ));
    assert!(append_selected_for_dispatch(
        &persistence,
        "transaction AOF append failed",
        Arc::clone(&writer),
        0,
        &[rs(b"SET"), rs(b"tx:k1"), rs(b"v1")],
        -1,
    ));
    assert!(append_selected_for_dispatch(
        &persistence,
        "transaction AOF append failed",
        Arc::clone(&writer),
        0,
        &[rs(b"INCR"), rs(b"tx:n")],
        -1,
    ));
    assert!(append_raw_for_dispatch(
        &persistence,
        "transaction AOF append failed",
        Arc::clone(&writer),
        &[rs(b"EXEC")],
        77,
    ));
    assert!(finish_thread_aof_batch(&persistence));

    assert_eq!(writer.fsync_count(), 1);
    assert_eq!(writer.fsynced_repl_offset(), 77);
    let bytes = std::fs::read(&aof_path).unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(text.contains("MULTI"));
    assert!(text.contains("EXEC"));
    let snap = replay_into_fresh(&aof_path, AofLoadOptions::default()).unwrap();
    assert_eq!(snap.entries.len(), 2);
}

#[test]
fn appendfsync_always_lifecycle_barrier_flushes_current_batch() {
    let scratch = Scratch::new("always_batch_barrier");
    let aof_path = scratch.path("appendonly.aof");
    let writer = Arc::new(AofWriter::open(&aof_path, FSYNC_ALWAYS).expect("open aof"));
    let persistence = PersistenceState::new();

    let _ = finish_thread_aof_batch(&persistence);
    begin_thread_aof_batch();
    assert!(append_selected_for_dispatch(
        &persistence,
        "AOF append failed",
        Arc::clone(&writer),
        0,
        &[rs(b"SET"), rs(b"before"), rs(b"1")],
        1,
    ));
    assert!(flush_thread_aof_batch_for_lifecycle(
        &persistence,
        "lifecycle barrier flush failed",
    ));
    assert_eq!(writer.fsync_count(), 1);
    assert_eq!(writer.fsynced_repl_offset(), 1);

    assert!(append_selected_for_dispatch(
        &persistence,
        "AOF append failed",
        Arc::clone(&writer),
        0,
        &[rs(b"SET"), rs(b"after"), rs(b"2")],
        2,
    ));
    assert_eq!(
        writer.fsync_count(),
        1,
        "barrier leaves a fresh active batch"
    );
    assert!(finish_thread_aof_batch(&persistence));
    assert_eq!(writer.fsync_count(), 2);
    assert_eq!(writer.fsynced_repl_offset(), 2);

    let snap = replay_into_fresh(&aof_path, AofLoadOptions::default()).unwrap();
    assert_eq!(snap.entries.len(), 2);
}

// ─── audit finding 3: MULTI/EXEC AOF replay (RED — replayer rejects EXEC) ────

/// A transaction appended as a MULTI..EXEC envelope (the `multi.rs:330-350`
/// shape: `append_raw(MULTI)`, `append_selected` per command, `append_raw(EXEC)`)
/// must replay into the exact keyspace. MULTI/EXEC records bracket the inner
/// commands; the replayer must consume them (Valkey loads MULTI/EXEC as a
/// no-op transaction frame) and apply the inner commands.
/// RED FINDING: the replayer (`replay_aof_databases_with_options` →
/// `dispatch_replay_command`, aof.rs) has NO special case for `MULTI`/`EXEC`.
/// They fall through to `dispatch_via_handler`, where `EXEC` returns
/// "ERR EXEC without MULTI" (no transaction state exists during replay) and
/// whole load aborts with an `io::Error`. So any AOF written by
/// `append_transaction_commands_to_aof` for a multi-command transaction is
/// unreplayable — a durability/replay divergence from Valkey.
#[test]
fn multi_exec_envelope_replays_keyspace_exactly() {
    let scratch = Scratch::new("multi");
    let aof_path = scratch.path("appendonly.aof");
    let writer = AofWriter::open(&aof_path, FSYNC_ALWAYS).expect("open aof");

    // Faithful copy of append_transaction_commands_to_aof's multi-command arm.
    writer.append_raw(&[rs(b"MULTI")]).unwrap();
    writer
        .append_selected(0, &[rs(b"SET"), rs(b"tx:a"), rs(b"1")])
        .unwrap();
    writer
        .append_selected(0, &[rs(b"RPUSH"), rs(b"tx:l"), rs(b"x"), rs(b"y")])
        .unwrap();
    writer
        .append_selected(0, &[rs(b"SADD"), rs(b"tx:s"), rs(b"m")])
        .unwrap();
    writer.append_raw(&[rs(b"EXEC")]).unwrap();
    writer.flush().unwrap();

    let mut expected = RedisDb::new(0);
    expected.insert(rs(b"tx:a"), RedisObject::new_string(b"1"));
    let mut l = std::collections::VecDeque::new();
    l.push_back(rs(b"x"));
    l.push_back(rs(b"y"));
    expected.insert(rs(b"tx:l"), RedisObject::new_list_from_vec(l));
    let mut hs = std::collections::HashSet::new();
    hs.insert(rs(b"m"));
    expected.insert(rs(b"tx:s"), RedisObject::new_set_from_set(hs));

    // Capture the replay outcome explicitly so the failure lands on
    // documented assertion (replay must succeed and reconstruct the keyspace),
    // not on an unwrap panic.
    let replayed = replay_into_fresh(&aof_path, AofLoadOptions::default());
    assert_eq!(
        replayed.map_err(|e| e.to_string()),
        Ok(snapshot(&expected)),
        "MULTI/EXEC envelope must replay to the same keyspace as the inner \
         commands; instead the replayer rejects EXEC (no MULTI/EXEC handling in \
         dispatch_replay_command)"
    );
}

#[test]
fn unfinished_multi_load_truncated_reverts_to_before_multi() {
    let scratch = Scratch::new("unfinished_multi");
    let aof_path = scratch.path("appendonly.aof");
    let reject_path = scratch.path("appendonly-reject.aof");

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&encode_resp_command(&[
        rs(b"SET"),
        rs(b"foo"),
        rs(b"hello"),
    ]));
    let valid_before_multi = bytes.len();
    bytes.extend_from_slice(&encode_resp_command(&[rs(b"MULTI")]));
    bytes.extend_from_slice(&encode_resp_command(&[
        rs(b"SET"),
        rs(b"bar"),
        rs(b"world"),
    ]));
    std::fs::write(&aof_path, &bytes).unwrap();
    std::fs::write(&reject_path, &bytes).unwrap();

    let mut opts_yes = AofLoadOptions::default();
    opts_yes.load_truncated = true;
    let replayed = replay_into_fresh(&aof_path, opts_yes).expect("unfinished MULTI tolerated");
    let mut expected = RedisDb::new(0);
    expected.insert(rs(b"foo"), RedisObject::new_string(b"hello"));
    assert_eq!(
        replayed,
        snapshot(&expected),
        "load_truncated=yes must roll back an unfinished MULTI body"
    );
    assert_eq!(
        std::fs::metadata(&aof_path).unwrap().len(),
        valid_before_multi as u64,
        "unfinished MULTI truncation must cut back to before MULTI"
    );

    let mut dbs = vec![RedisDb::new(0)];
    let err = replay_aof_databases_with_options(&reject_path, &mut dbs, AofLoadOptions::default())
        .expect_err("load_truncated=no must reject unfinished MULTI");
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}

// ─── audit finding 4: TRUNCATED-TAIL (GREEN-LOCK / no oracle) ────────────────

/// A truncated final command: the valid prefix must replay under
/// `load_truncated=yes`, and the whole load must error under
/// `load_truncated=no`.
#[test]
fn truncated_tail_loads_prefix_only_when_allowed() {
    let scratch = Scratch::new("trunc");
    let aof_path = scratch.path("appendonly.aof");
    let reject_path = scratch.path("appendonly-reject.aof");

    // Two complete commands, then a deliberately truncated third (header claims
    // a 3-element array but the bytes stop mid-command).
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&encode_resp_command(&[rs(b"SET"), rs(b"good1"), rs(b"v1")]));
    bytes.extend_from_slice(&encode_resp_command(&[rs(b"SET"), rs(b"good2"), rs(b"v2")]));
    let valid_prefix_len = bytes.len();
    bytes.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$4\r\ngo"); // truncated tail
    std::fs::write(&aof_path, &bytes).unwrap();
    std::fs::write(&reject_path, &bytes).unwrap();

    // load_truncated=yes -> valid prefix replays and the torn tail is removed
    // so future appends land after a valid RESP boundary.
    let mut opts_yes = AofLoadOptions::default();
    opts_yes.load_truncated = true;
    let replayed = replay_into_fresh(&aof_path, opts_yes).expect("truncated tail tolerated");
    let mut expected = RedisDb::new(0);
    expected.insert(rs(b"good1"), RedisObject::new_string(b"v1"));
    expected.insert(rs(b"good2"), RedisObject::new_string(b"v2"));
    assert_eq!(
        replayed,
        snapshot(&expected),
        "load_truncated=yes must replay the valid prefix and drop the torn tail"
    );
    assert_eq!(
        std::fs::metadata(&aof_path).unwrap().len(),
        valid_prefix_len as u64,
        "load_truncated=yes must physically truncate the torn tail before new appends"
    );

    let writer = AofWriter::open(&aof_path, FSYNC_ALWAYS).expect("reopen repaired aof");
    writer
        .append(&[rs(b"SET"), rs(b"after"), rs(b"repair")])
        .expect("append after truncation repair");
    let replayed = replay_into_fresh(&aof_path, AofLoadOptions::default())
        .expect("repaired file must replay without load_truncated");
    expected.insert(rs(b"after"), RedisObject::new_string(b"repair"));
    assert_eq!(
        replayed,
        snapshot(&expected),
        "new commands appended after truncation repair must survive restart"
    );

    // load_truncated=no -> error on a fresh copy of the same torn input.
    let mut dbs = vec![RedisDb::new(0)];
    let err = replay_aof_databases_with_options(&reject_path, &mut dbs, AofLoadOptions::default())
        .expect_err("load_truncated=no must reject a torn tail");
    assert!(
        err.kind() == io::ErrorKind::UnexpectedEof || err.kind() == io::ErrorKind::InvalidData,
        "expected EOF/InvalidData on torn tail, got {err:?}"
    );
}

#[test]
fn replay_preserves_past_expire_across_later_collection_mutation() {
    let scratch = Scratch::new("expire_replay");
    let aof_path = scratch.path("appendonly.aof");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&encode_resp_command(&[
        rs(b"RPUSH"),
        rs(b"list"),
        rs(b"foo"),
    ]));
    bytes.extend_from_slice(&encode_resp_command(&[
        rs(b"PEXPIREAT"),
        rs(b"list"),
        rs(b"1000"),
    ]));
    bytes.extend_from_slice(&encode_resp_command(&[
        rs(b"RPUSH"),
        rs(b"list"),
        rs(b"bar"),
    ]));
    std::fs::write(&aof_path, &bytes).unwrap();

    let key = rs(b"list");
    let mut dbs = vec![RedisDb::new(0)];
    replay_aof_databases_with_options(&aof_path, &mut dbs, AofLoadOptions::default())
        .expect("AOF with past PEXPIREAT should replay");

    let obj = dbs[0]
        .find(&key)
        .expect("replay should keep the expired key until normal lookup");
    assert_eq!(obj.expire, 1000);
    assert_eq!(obj.list().map(|list| list.len()), Some(2));
    assert!(
        dbs[0]
            .lookup_key_read_with_flags(&key, LOOKUP_NONE)
            .is_none(),
        "normal post-load lookup should expire the key"
    );
    assert!(!dbs[0].exists_raw(&key));
}

#[test]
fn replay_set_pxat_preserves_absolute_expire_from_rewrite_form() {
    let scratch = Scratch::new("set_pxat_replay");
    let aof_path = scratch.path("appendonly.aof");
    let future_ms = current_ms() + 2_000_000;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&encode_resp_command(&[
        rs(b"SET"),
        rs(b"y"),
        rs(b"somevalue"),
        rs(b"PXAT"),
        rs(future_ms.to_string().as_bytes()),
    ]));
    bytes.extend_from_slice(&encode_resp_command(&[
        rs(b"SET"),
        rs(b"py"),
        rs(b"somevalue"),
        rs(b"PXAT"),
        rs(future_ms.to_string().as_bytes()),
    ]));
    std::fs::write(&aof_path, &bytes).unwrap();

    let mut dbs = vec![RedisDb::new(0)];
    replay_aof_databases_with_options(&aof_path, &mut dbs, AofLoadOptions::default())
        .expect("SET PXAT rewrite form should replay");

    for key in [rs(b"y"), rs(b"py")] {
        let obj = dbs[0]
            .find(&key)
            .expect("replay must create key from SET PXAT");
        assert_eq!(obj.expire, future_ms, "SET PXAT must preserve expiry");
    }
}

// ─── audit finding 5: MANIFEST round-trip + validation (GREEN-LOCK) ──────────
// `encode_aof_manifest` / `load_aof_manifest` are private to aof.rs, so this
// kit cannot call them directly without a production visibility change. Instead
// it drives the public `load_append_only_files` entry point, which parses
// manifest with the same strict rules. A well-formed manifest must load its
// BASE+INCR; a malformed manifest (duplicate base / non-monotonic incr seq)
// must error with the exact Valkey message. See kit notes for the visibility
// limitation.

fn write_manifest(scratch: &Scratch, appenddir: &str, appendfile: &str, body: &str) -> PathBuf {
    let dir = scratch.path(appenddir);
    std::fs::create_dir_all(&dir).unwrap();
    let manifest_path = dir.join(format!("{appendfile}.manifest"));
    std::fs::write(&manifest_path, body).unwrap();
    manifest_path
}

#[test]
fn manifest_well_formed_round_trips_and_loads() {
    let scratch = Scratch::new("manifest_ok");
    let appendfile = "appendonly.aof";
    let appenddir = "appendonlydir";

    // BASE holds a SET; INCR holds another SET.
    let dir = scratch.path(appenddir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("appendonly.aof.1.base.aof"),
        encode_resp_command(&[rs(b"SET"), rs(b"base_key"), rs(b"bv")]),
    )
    .unwrap();
    std::fs::write(
        dir.join("appendonly.aof.1.incr.aof"),
        encode_resp_command(&[rs(b"SET"), rs(b"incr_key"), rs(b"iv")]),
    )
    .unwrap();
    write_manifest(
        &scratch,
        appenddir,
        appendfile,
        "file appendonly.aof.1.base.aof seq 1 type b\n\
         file appendonly.aof.1.incr.aof seq 1 type i\n",
    );

    let mut dbs = vec![RedisDb::new(0)];
    let result = load_append_only_files(
        &scratch.dir,
        appendfile,
        appenddir,
        &mut dbs,
        AofLoadOptions::default(),
    )
    .expect("well-formed manifest must load");
    assert!(
        result.is_some(),
        "manifest with BASE+INCR must report a load"
    );
    let snap = snapshot(&dbs[0]);
    assert_eq!(snap.entries.len(), 2, "BASE+INCR keys must both be present");
}

#[test]
fn manifest_duplicate_base_errors_with_valkey_message() {
    let scratch = Scratch::new("manifest_dupbase");
    write_manifest(
        &scratch,
        "appendonlydir",
        "appendonly.aof",
        "file appendonly.aof.1.base.aof seq 1 type b\n\
         file appendonly.aof.2.base.aof seq 2 type b\n",
    );
    let mut dbs = vec![RedisDb::new(0)];
    let err = load_append_only_files(
        &scratch.dir,
        "appendonly.aof",
        "appendonlydir",
        &mut dbs,
        AofLoadOptions::default(),
    )
    .expect_err("duplicate base must error");
    let msg = err.to_string();
    assert!(
        msg.contains("Found duplicate base file information"),
        "expected Valkey duplicate-base message, got: {msg}"
    );
}

#[test]
fn manifest_non_monotonic_incr_seq_errors_with_valkey_message() {
    let scratch = Scratch::new("manifest_nonmono");
    write_manifest(
        &scratch,
        "appendonlydir",
        "appendonly.aof",
        "file appendonly.aof.1.base.aof seq 1 type b\n\
         file appendonly.aof.5.incr.aof seq 5 type i\n\
         file appendonly.aof.3.incr.aof seq 3 type i\n",
    );
    let mut dbs = vec![RedisDb::new(0)];
    let err = load_append_only_files(
        &scratch.dir,
        "appendonly.aof",
        "appendonlydir",
        &mut dbs,
        AofLoadOptions::default(),
    )
    .expect_err("non-monotonic incr seq must error");
    let msg = err.to_string();
    assert!(
        msg.contains("Found a non-monotonic sequence number"),
        "expected Valkey non-monotonic-seq message, got: {msg}"
    );
}

// ─── helpers ─────────────────────────────────────────────────────────────────

fn current_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
