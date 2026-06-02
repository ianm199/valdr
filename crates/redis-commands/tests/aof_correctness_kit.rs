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

use redis_commands::aof::{
    append_raw_for_dispatch, append_selected_for_dispatch, begin_thread_aof_batch,
    encode_resp_command, finish_thread_aof_batch, flush_thread_aof_batch_for_lifecycle,
    record_aof_append_result, replay_aof_databases_with_options, AofLoadOptions, AofWriter,
    FSYNC_ALWAYS, FSYNC_EVERYSEC, FSYNC_NO,
};
use redis_core::db::RedisDb;
use redis_core::object::{ObjectKind, RedisObject, EXPIRY_NONE};
use redis_core::persistence::{PersistenceState, PersistenceStatus};
use redis_types::RedisString;
use std::sync::Arc;

// ─── tmpdir plumbing (no tempfile dev-dep available) ─────────────────────────

static COUNTER: AtomicU64 = AtomicU64::new(0);

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

// ─── audit finding 4: TRUNCATED-TAIL (GREEN-LOCK / no oracle) ────────────────

/// A truncated final command: the valid prefix must replay under
/// `load_truncated=yes`, and the whole load must error under
/// `load_truncated=no`.
#[test]
fn truncated_tail_loads_prefix_only_when_allowed() {
    let scratch = Scratch::new("trunc");
    let aof_path = scratch.path("appendonly.aof");

    // Two complete commands, then a deliberately truncated third (header claims
    // a 3-element array but the bytes stop mid-command).
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&encode_resp_command(&[rs(b"SET"), rs(b"good1"), rs(b"v1")]));
    bytes.extend_from_slice(&encode_resp_command(&[rs(b"SET"), rs(b"good2"), rs(b"v2")]));
    bytes.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$4\r\ngo"); // truncated tail
    std::fs::write(&aof_path, &bytes).unwrap();

    // load_truncated=yes → valid prefix replays, no error.
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

    // load_truncated=no → error.
    let mut dbs = vec![RedisDb::new(0)];
    let err = replay_aof_databases_with_options(&aof_path, &mut dbs, AofLoadOptions::default())
        .expect_err("load_truncated=no must reject a torn tail");
    assert!(
        err.kind() == io::ErrorKind::UnexpectedEof || err.kind() == io::ErrorKind::InvalidData,
        "expected EOF/InvalidData on torn tail, got {err:?}"
    );
}

// ─── audit finding 5: MANIFEST round-trip + validation (GREEN-LOCK) ──────────
// `encode_aof_manifest` / `load_aof_manifest` are private to aof.rs, so this
// kit cannot call them directly without a production visibility change. Instead
// it drives the public `load_append_only_files` entry point, which parses
// manifest with the same strict rules. A well-formed manifest must load its
// BASE+INCR; a malformed manifest (duplicate base / non-monotonic incr seq)
// must error with the exact Valkey message. See kit notes for the visibility
// limitation.

use redis_commands::aof::load_append_only_files;

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
