//! In-memory lazy per-key loading kit for the EdgeStash cold-start work.
//!
//! This is the `conn_transport_kit`-style fast inner loop for Phase 2a of the
//! cold-start optimization (`docs/EDGESTASH_COLDSTART_PREP.md`, option A). It
//! proves, with no sockets / no server / no `storage.list()`, that the engine's
//! per-key persistence API (`export_key`/`import_key`/`take_dirty`) plus
//! `command_keys` is sufficient to serve a request by loading only the keys it
//! touches — and that doing so is byte-for-byte identical to the eager
//! "load the whole keyspace first" adapter the production worker uses today
//! (`crates/edgestash-cloudflare/src/lib.rs` `load_entries`).
//!
//! Three things are asserted:
//!   1. Parity — the same command sequence run through an EAGER engine
//!      (every key preloaded) and a LAZY engine (keys fetched on demand from a
//!      `MockStore`) produces identical RESP2 reply bytes for every command,
//!      over the entire differential fixture corpus. An under-fetch in
//!      `command_keys` would make the lazy reply diverge — the test catches it.
//!   2. The cold-cost win — a single-key command against a 1000-key store
//!      fetches `<= touched` keys and NEVER lists the whole keyspace.
//!   3. The enumeration fallback — a `SCAN`/`KEYS` triggers exactly one
//!      `full_list()` and never re-lists on a repeat enumeration.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;

use redis_protocol::{encode_resp2, RespFrame};
use serde_json::Value as JsonValue;
use valdr_engine::{command_keys, Engine, KeyAccess};

/// A deterministic, instrumented stand-in for Durable Object storage. Holds the
/// per-key exported bytes (exactly what `Engine::export_key` produces) keyed by
/// the raw redis key, and records every access so the kit can assert the
/// cold-cost shape: how many single-key `get`s and how many whole-keyspace
/// `full_list`s a sequence performed.
#[derive(Default)]
struct MockStore {
    entries: HashMap<Vec<u8>, Vec<u8>>,
    get_count: usize,
    list_count: usize,
    fetched_keys: Vec<Vec<u8>>,
}

impl MockStore {
    fn new() -> Self {
        MockStore::default()
    }

    /// Seed a key's exported bytes directly (used to build a large cold store
    /// without paying a flush per key).
    fn seed(&mut self, key: Vec<u8>, bytes: Vec<u8>) {
        self.entries.insert(key, bytes);
    }

    /// Fetch one key's exported bytes, recording the access. `None` means the
    /// key is absent in storage (so the engine simply has no entry for it).
    fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_count += 1;
        self.fetched_keys.push(key.to_vec());
        self.entries.get(key).cloned()
    }

    /// Enumerate every stored entry, recording the whole-keyspace access. This
    /// is the expensive `storage.list()` the lazy path exists to avoid.
    fn full_list(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.list_count += 1;
        self.entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Persist a key's new exported bytes (a flush write).
    fn put(&mut self, key: Vec<u8>, bytes: Vec<u8>) {
        self.entries.insert(key, bytes);
    }

    /// Remove a key (a flush delete — the engine reported it dirty with no
    /// exportable value).
    fn delete(&mut self, key: &[u8]) {
        self.entries.remove(key);
    }
}

/// A `MockStore` plus the resident/loaded bookkeeping a lazy adapter needs.
/// Shared (`Rc<RefCell>`) only so the test body can read the access counters
/// after driving the engine; the lazy logic itself is single-owner.
struct LazyEngine {
    engine: Engine<valdr_engine::NoopHost>,
    store: Rc<RefCell<MockStore>>,
    /// Keys already imported into the engine this session — never re-fetched.
    resident: HashSet<Vec<u8>>,
    /// Set once a `full_list()` has imported the whole keyspace; further
    /// enumeration commands then serve from memory without re-listing.
    fully_loaded: bool,
    /// Mirror of the engine's MULTI queue so a lazy `EXEC` can preload the
    /// union of every queued command's keys before the transaction replays.
    multi_queue: Vec<Vec<Vec<u8>>>,
    in_multi: bool,
}

impl LazyEngine {
    fn new(store: Rc<RefCell<MockStore>>) -> Self {
        LazyEngine {
            engine: Engine::new_in_memory(),
            store,
            resident: HashSet::new(),
            fully_loaded: false,
            multi_queue: Vec::new(),
            in_multi: false,
        }
    }

    /// Import one key from the store if it is not already resident. A `None`
    /// from the store means the key does not exist; we still mark it resident so
    /// a later command for the same absent key does not re-fetch ("absent in
    /// memory" now correctly equals "absent in storage").
    fn ensure_resident(&mut self, key: &[u8]) {
        if self.fully_loaded || self.resident.contains(key) {
            return;
        }
        if let Some(bytes) = self.store.borrow_mut().get(key) {
            self.engine
                .import_key(&bytes)
                .expect("MockStore held bytes export_key did not produce");
        }
        self.resident.insert(key.to_vec());
    }

    /// Import the whole keyspace exactly once, the lazy fallback for an
    /// enumeration / dynamic-key command.
    fn ensure_fully_loaded(&mut self) {
        if self.fully_loaded {
            return;
        }
        for (key, bytes) in self.store.borrow_mut().full_list() {
            if !self.resident.contains(&key) {
                self.engine
                    .import_key(&bytes)
                    .expect("MockStore held bytes export_key did not produce");
            }
            self.resident.insert(key);
        }
        self.fully_loaded = true;
    }

    /// Load whatever a command requires before it runs.
    fn load_for(&mut self, access: &KeyAccess) {
        match access {
            KeyAccess::FullKeyspace => self.ensure_fully_loaded(),
            KeyAccess::Keys(keys) => {
                for key in keys {
                    self.ensure_resident(key);
                }
            }
        }
    }

    /// Execute one command through the lazy adapter, mirroring exactly what the
    /// production worker would do: resolve the keys, fetch the missing ones,
    /// run the command, then flush every dirty key back to the store.
    fn execute(&mut self, argv: &[Vec<u8>]) -> RespFrame {
        let upper = argv
            .first()
            .map(|c| c.to_ascii_uppercase())
            .unwrap_or_default();

        // Track the MULTI queue so EXEC can preload the union of queued keys.
        // The transaction-control verbs themselves touch no data keys; the
        // queued data commands' keys are what must be resident at EXEC time.
        if upper == b"MULTI" {
            self.in_multi = true;
            self.multi_queue.clear();
        } else if upper == b"EXEC" {
            let mut union = KeyAccess::Keys(Vec::new());
            for queued in &self.multi_queue {
                union = union.merge(command_keys(queued));
            }
            self.load_for(&union);
            self.in_multi = false;
            self.multi_queue.clear();
        } else if upper == b"DISCARD" {
            self.in_multi = false;
            self.multi_queue.clear();
        } else if self.in_multi {
            // A queued command is only validated at queue time, not run; record
            // it for the EXEC union but do not load yet.
            self.multi_queue.push(argv.to_vec());
        } else {
            self.load_for(&command_keys(argv));
        }

        let frame = self.engine.execute(argv);
        self.flush();
        frame
    }

    /// Write every key the engine marked dirty back to the store: the exported
    /// bytes when the key still holds a value, or a delete when it does not.
    /// Mirrors `drain_flush` in the production adapter.
    fn flush(&mut self) {
        for key in self.engine.take_dirty() {
            match self.engine.export_key(&key) {
                Some(bytes) => self.store.borrow_mut().put(key.clone(), bytes),
                None => self.store.borrow_mut().delete(&key),
            }
            // A freshly written/deleted key is now authoritative in memory.
            self.resident.insert(key);
        }
    }
}

/// One fixture line: a command plus the optional engine clock controls and
/// comparison `mode` the differential corpus carries. `sleep_ms` advances the
/// deterministic clock (the oracle sleeps wall-clock; for an eager-vs-lazy
/// comparison both engines only need the *same* clock progression, so we
/// advance a shared counter). `mode` selects how eager and lazy replies are
/// compared: `set_equal`/`scan_reply` are order-insensitive because both
/// engines hold the same keys but iterate their `db` HashMaps in different
/// orders (eager in SET-insertion order, lazy in `full_list()` import order) —
/// exactly the reason the differential oracle uses these modes against valkey.
struct Fixture {
    cmd: Vec<Vec<u8>>,
    now_millis: Option<u64>,
    sleep_ms: Option<u64>,
    mode: String,
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../harness/oracle/valdr-fixtures")
}

/// Parse one JSONL fixture file into its ordered command sequence. Lines
/// without a `cmd` array (none in the corpus, but defensively) are skipped.
fn parse_fixture_file(path: &std::path::Path) -> Vec<Fixture> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read fixture file {}: {e}", path.display()));
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: JsonValue = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("invalid fixture JSON in {}: {e}", path.display()));
        let Some(items) = value.get("cmd").and_then(JsonValue::as_array) else {
            continue;
        };
        let mut cmd = Vec::with_capacity(items.len());
        for item in items {
            let text = item
                .as_str()
                .unwrap_or_else(|| panic!("non-string cmd entry in {}", path.display()));
            cmd.push(text.as_bytes().to_vec());
        }
        let now_millis = value.get("now_millis").and_then(JsonValue::as_u64);
        let sleep_ms = value.get("sleep_ms").and_then(JsonValue::as_u64);
        let mode = value
            .get("mode")
            .and_then(JsonValue::as_str)
            .unwrap_or("exact")
            .to_owned();
        out.push(Fixture {
            cmd,
            now_millis,
            sleep_ms,
            mode,
        });
    }
    out
}

/// Render a frame to a stable byte key for multiset comparison. Used to compare
/// the *elements* of an order-insensitive reply (`set_equal`/`scan_reply`)
/// without caring about iteration order.
fn frame_key(frame: &RespFrame) -> Vec<u8> {
    resp_bytes(frame)
}

/// Compare two top-level array replies element-by-element as multisets (order
/// insensitive). Returns true when both are arrays holding the same elements
/// regardless of order. Non-array replies fall back to exact byte equality.
fn frames_equal_unordered(a: &RespFrame, b: &RespFrame) -> bool {
    match (a, b) {
        (RespFrame::Array(Some(xs)), RespFrame::Array(Some(ys))) => {
            if xs.len() != ys.len() {
                return false;
            }
            let mut left: Vec<Vec<u8>> = xs.iter().map(frame_key).collect();
            let mut right: Vec<Vec<u8>> = ys.iter().map(frame_key).collect();
            left.sort();
            right.sort();
            left == right
        }
        _ => resp_bytes(a) == resp_bytes(b),
    }
}

/// SCAN-family reply is `[cursor, [elements]]`. The cursor must match exactly
/// (both engines fully enumerate, so it is "0"); the element array is compared
/// as a multiset.
fn scan_replies_equal(a: &RespFrame, b: &RespFrame) -> bool {
    match (a, b) {
        (RespFrame::Array(Some(xs)), RespFrame::Array(Some(ys))) if xs.len() == 2 && ys.len() == 2 => {
            resp_bytes(&xs[0]) == resp_bytes(&ys[0]) && frames_equal_unordered(&xs[1], &ys[1])
        }
        _ => resp_bytes(a) == resp_bytes(b),
    }
}

/// Apply the fixture's comparison mode between the eager and lazy replies.
/// `set_equal`/`scan_reply` are order-insensitive (HashMap iteration order
/// differs between the two engines); every other mode is byte-exact because,
/// running on a shared deterministic clock with per-key round-trips, the two
/// engines must agree to the byte.
fn replies_match(mode: &str, eager: &RespFrame, lazy: &RespFrame) -> bool {
    match mode {
        "set_equal" => frames_equal_unordered(eager, lazy),
        "scan_reply" => scan_replies_equal(eager, lazy),
        _ => resp_bytes(eager) == resp_bytes(lazy),
    }
}

fn resp_bytes(frame: &RespFrame) -> Vec<u8> {
    let mut out = Vec::new();
    encode_resp2(frame, &mut out);
    out
}

/// Drive one fixture file through an eager engine and a lazy engine in lockstep
/// and assert byte-identical replies. The eager engine never sees the store
/// (its keyspace is whatever the sequence itself wrote — the corpus files are
/// self-seeding isolated sequences); the lazy engine flushes to and reloads
/// from a `MockStore`, so for the SAME sequence its keyspace is identical at
/// every step *iff* `command_keys` never under-fetches. Returns the number of
/// commands compared.
fn assert_parity_for_file(path: &std::path::Path) -> usize {
    let fixtures = parse_fixture_file(path);

    let mut eager = Engine::new_in_memory();
    let lazy_store = Rc::new(RefCell::new(MockStore::new()));
    let mut lazy = LazyEngine::new(Rc::clone(&lazy_store));

    // A fixed base clock so the eager and lazy engines see identical time; the
    // absolute value is irrelevant, only that both engines get the same input.
    let base_clock: u64 = 1_700_000_000_000;
    let mut clock = base_clock;

    for (index, fixture) in fixtures.iter().enumerate() {
        if let Some(extra) = fixture.sleep_ms {
            clock = clock.wrapping_add(extra);
        }
        let now = fixture.now_millis.unwrap_or(clock);
        eager.host_mut().set_now_millis(now);
        lazy.engine.host_mut().set_now_millis(now);

        let eager_reply = eager.execute(&fixture.cmd);
        let lazy_reply = lazy.execute(&fixture.cmd);

        assert!(
            replies_match(&fixture.mode, &eager_reply, &lazy_reply),
            "lazy/eager divergence in {} at command #{index} ({:?}, mode={}) — \
             command_keys likely under-fetched its keys\n  eager: {:02x?}\n  lazy:  {:02x?}",
            path.display(),
            fixture
                .cmd
                .iter()
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .collect::<Vec<_>>(),
            fixture.mode,
            resp_bytes(&eager_reply),
            resp_bytes(&lazy_reply),
        );
    }

    fixtures.len()
}

/// THE PARITY PROOF. Every differential fixture file is an isolated, self-seeding
/// command sequence; running each through both engines and diffing every reply
/// proves `command_keys` loads everything each command needs. A single
/// under-fetch anywhere in the corpus fails this test.
#[test]
fn lazy_matches_eager_over_the_whole_corpus() {
    let dir = fixtures_dir();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read fixtures dir {}: {e}", dir.display()))
        .map(|entry| entry.expect("dir entry").path())
        .filter(|p| p.extension().map(|e| e == "jsonl").unwrap_or(false))
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "no fixture files found under {}",
        dir.display()
    );

    let mut total_files = 0usize;
    let mut total_commands = 0usize;
    for file in &files {
        let compared = assert_parity_for_file(file);
        total_files += 1;
        total_commands += compared;
    }

    // Lock in that the corpus is non-trivial so a future fixture deletion that
    // silently empties the proof is caught.
    assert!(
        total_files >= 30,
        "expected the full fixture corpus (>=30 files), saw {total_files}"
    );
    assert!(
        total_commands >= 1000,
        "expected a substantial corpus (>=1000 commands), saw {total_commands}"
    );
    eprintln!(
        "lazy/eager parity: {total_commands} commands across {total_files} files, 0 mismatches"
    );
}

/// THE COLD-COST WIN. Seed a store with 1000 keys (as if a tenant cold-started
/// holding 1000 keys), then issue a single GET. The lazy engine must fetch only
/// the one touched key and must NEVER list the whole keyspace — cold cost is
/// O(touched), not O(state).
#[test]
fn single_key_get_is_o_touched_not_o_state() {
    let store = Rc::new(RefCell::new(MockStore::new()));

    // Build the 1000-key store by exporting from a throwaway engine.
    {
        let mut seeder = Engine::new_in_memory();
        let mut s = store.borrow_mut();
        for i in 0..1000u32 {
            let key = format!("key:{i:04}").into_bytes();
            seeder.execute(&[b"SET".to_vec(), key.clone(), format!("v{i}").into_bytes()]);
            let bytes = seeder
                .export_key(&key)
                .expect("seeded key must export");
            s.seed(key, bytes);
        }
    }

    let mut lazy = LazyEngine::new(Rc::clone(&store));
    lazy.engine.host_mut().set_now_millis(1_700_000_000_000);

    let reply = resp_bytes(&lazy.execute(&[b"GET".to_vec(), b"key:0500".to_vec()]));

    let s = store.borrow();
    assert_eq!(
        s.list_count, 0,
        "a single-key GET must never list the whole keyspace"
    );
    assert!(
        s.get_count <= 1,
        "a single-key GET fetched {} keys, expected <= 1 (touched-key count)",
        s.get_count
    );
    assert_eq!(
        s.fetched_keys,
        vec![b"key:0500".to_vec()],
        "the only key fetched must be the one GET touched"
    );
    assert_eq!(
        reply,
        b"$4\r\nv500\r\n".to_vec(),
        "lazy GET returned the wrong value"
    );
    eprintln!(
        "cold-cost win: GET over a 1000-key store -> {} get(s), {} list(s)",
        s.get_count, s.list_count
    );
}

/// A second touched key on the same lazy engine fetches once more and never
/// lists — and re-reading an already-resident key fetches zero additional times.
#[test]
fn resident_keys_are_not_refetched() {
    let store = Rc::new(RefCell::new(MockStore::new()));
    {
        let mut seeder = Engine::new_in_memory();
        let mut s = store.borrow_mut();
        for k in [b"a".as_slice(), b"b", b"c"] {
            seeder.execute(&[b"SET".to_vec(), k.to_vec(), b"1".to_vec()]);
            s.seed(k.to_vec(), seeder.export_key(k).unwrap());
        }
    }
    let mut lazy = LazyEngine::new(Rc::clone(&store));
    lazy.engine.host_mut().set_now_millis(1_700_000_000_000);

    lazy.execute(&[b"GET".to_vec(), b"a".to_vec()]);
    lazy.execute(&[b"GET".to_vec(), b"a".to_vec()]); // already resident
    lazy.execute(&[b"GET".to_vec(), b"b".to_vec()]);

    let s = store.borrow();
    assert_eq!(s.list_count, 0, "point reads must never list");
    assert_eq!(
        s.get_count, 2,
        "expected 2 fetches (a, then b); the repeat GET a must not refetch"
    );
}

/// THE ENUMERATION FALLBACK. A `SCAN`/`KEYS` is a `FullKeyspace` command: it
/// must trigger exactly one `full_list()`, and a second enumeration must not
/// re-list (the store is marked fully loaded).
#[test]
fn scan_and_keys_trigger_exactly_one_full_list() {
    let store = Rc::new(RefCell::new(MockStore::new()));
    {
        let mut seeder = Engine::new_in_memory();
        let mut s = store.borrow_mut();
        for i in 0..10u32 {
            let key = format!("k:{i}").into_bytes();
            seeder.execute(&[b"SET".to_vec(), key.clone(), b"v".to_vec()]);
            s.seed(key.clone(), seeder.export_key(&key).unwrap());
        }
    }
    let mut lazy = LazyEngine::new(Rc::clone(&store));
    lazy.engine.host_mut().set_now_millis(1_700_000_000_000);

    let scan = resp_bytes(&lazy.execute(&[b"SCAN".to_vec(), b"0".to_vec(), b"COUNT".to_vec(), b"1000".to_vec()]));
    assert_eq!(store.borrow().list_count, 1, "SCAN must list exactly once");

    // A second enumeration: still exactly one list total (already fully loaded).
    lazy.execute(&[b"KEYS".to_vec(), b"*".to_vec()]);
    assert_eq!(
        store.borrow().list_count,
        1,
        "a repeat enumeration must not re-list once fully loaded"
    );

    // Sanity: the SCAN actually saw the seeded keyspace (10 keys present).
    assert!(
        scan.windows(3).filter(|w| *w == b"k:0").count() >= 1 || scan.len() > 20,
        "SCAN reply should contain the loaded keys"
    );
    eprintln!(
        "enumeration fallback: SCAN+KEYS over a 10-key store -> {} full_list(s)",
        store.borrow().list_count
    );
}
