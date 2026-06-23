//! Lazy per-key cold-load proof for the provider-neutral EdgeStash layer.
//!
//! Phase 2b wires `valdr_engine::command_keys` / `rest_command_keys` into the
//! real request path: a lazily-opened `EdgeObject` (`EdgeObject::open_lazy`)
//! imports only the keys a request touches from provider storage before running
//! the command, instead of the eager `EdgeObject::open` whole-keyspace
//! `storage.list()`. This is the `lazy_loader_kit` doctrine
//! (`crates/valdr-engine/tests/lazy_loader_kit.rs`) applied one layer up, at the
//! `ObjectStorage` boundary the real Cloudflare Durable Object implements.
//!
//! Three things are asserted, with no sockets / no server / no real Durable
//! Object — a deterministic in-memory `RecordingStorage` that counts every
//! `get`/`list`/`put`/`delete`:
//!   1. The cold-cost win — a single-key command against a store holding 1000
//!      keys does exactly one `get` (the touched key) and NEVER a `list`. Cold
//!      cost is O(touched), not O(total tenant state).
//!   2. The enumeration fallback — a `SCAN` / `KEYS` triggers exactly one
//!      `list()`, and a repeat enumeration does not re-list (fully loaded).
//!   3. Parity — a representative raw-command sequence and a limiter sequence
//!      produce byte-identical HTTP responses through the eager and the lazy
//!      object. A `command_keys` under-fetch in the lazy path would diverge.

use std::collections::{HashMap, HashSet};

use edgestash_demo::{
    EdgeHttpRequest, EdgeHttpResponse, EdgeObject, EdgeError, MemoryObjectStorage, ObjectStorage,
    Policy,
};

/// An `ObjectStorage` that records every access so a test can assert the
/// cold-load shape: how many single-key `get`s and how many whole-keyspace
/// `list`s a request performed. The values themselves are exactly the per-key
/// exported bytes `EdgeObject` writes, so the lazy object reconstructs the same
/// state the eager object would have listed.
#[derive(Debug, Default)]
struct RecordingStorage {
    values: HashMap<String, Vec<u8>>,
    dirty: HashSet<String>,
    get_count: usize,
    list_count: usize,
    put_count: usize,
    delete_count: usize,
    got_keys: Vec<String>,
}

impl RecordingStorage {
    /// Seed one already-persisted storage entry without recording it as an
    /// access or a dirty write — used to build a large cold store cheaply.
    fn seed(&mut self, key: String, value: Vec<u8>) {
        self.values.insert(key, value);
    }
}

impl ObjectStorage for RecordingStorage {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, EdgeError> {
        self.get_count += 1;
        self.got_keys.push(key.to_owned());
        Ok(self.values.get(key).cloned())
    }

    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), EdgeError> {
        self.put_count += 1;
        self.values.insert(key.to_owned(), value.to_vec());
        self.dirty.insert(key.to_owned());
        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<(), EdgeError> {
        self.delete_count += 1;
        self.values.remove(key);
        self.dirty.insert(key.to_owned());
        Ok(())
    }

    fn list(&mut self) -> Result<Vec<(String, Vec<u8>)>, EdgeError> {
        self.list_count += 1;
        Ok(self
            .values
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}

/// Build a `RecordingStorage` already holding `count` distinct string keys, by
/// running the SETs through a throwaway eager object so every stored entry is in
/// the exact `k:<hex>` layout and per-key export format the lazy object expects.
/// The returned storage's access counters start at zero (seeding does not record).
fn seeded_store(count: u32) -> RecordingStorage {
    let mut seed_object = EdgeObject::open(MemoryObjectStorage::default()).unwrap();
    for i in 0..count {
        let path = format!("/v1/valdr/seed/SET/key%3A{i:04}/v{i}");
        let response = seed_object.handle_http(EdgeHttpRequest::get(&path, 1_000));
        assert_eq!(response.status, 200, "seed SET failed for key {i}");
    }
    let mut store = RecordingStorage::default();
    for (skey, bytes) in seed_object.into_storage().list().unwrap() {
        store.seed(skey, bytes);
    }
    store
}

/// THE COLD-COST WIN. A store holding 1000 keys; a single-key GET must fetch
/// exactly the one touched key and never list the whole keyspace.
#[test]
fn single_key_get_is_o_touched_not_o_state() {
    let store = seeded_store(1000);
    let mut object = EdgeObject::open_lazy(store).unwrap();

    let response =
        object.handle_http(EdgeHttpRequest::get("/v1/valdr/seed/GET/key%3A0500", 2_000));
    assert_eq!(response.status, 200);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({ "result": "v500" }),
        "lazy GET returned the wrong value"
    );

    let store = object.into_storage();
    assert_eq!(
        store.list_count, 0,
        "a single-key GET must never list the whole keyspace"
    );
    assert_eq!(
        store.get_count, 1,
        "a single-key GET must fetch exactly the one touched key (touched-key count)"
    );
    assert_eq!(
        store.got_keys,
        vec![key_storage_key(b"key:0500")],
        "the only key fetched must be the one GET touched"
    );
    eprintln!(
        "cold-cost win: GET over a 1000-key store -> {} get(s), {} list(s)",
        store.get_count, store.list_count
    );
}

/// A second touched key fetches once more and never lists; re-reading an
/// already-resident key fetches zero additional times.
#[test]
fn resident_keys_are_not_refetched() {
    let store = seeded_store(50);
    let mut object = EdgeObject::open_lazy(store).unwrap();

    object.handle_http(EdgeHttpRequest::get("/v1/valdr/seed/GET/key%3A0000", 2_000));
    object.handle_http(EdgeHttpRequest::get("/v1/valdr/seed/GET/key%3A0000", 2_000)); // resident
    object.handle_http(EdgeHttpRequest::get("/v1/valdr/seed/GET/key%3A0001", 2_000));

    let store = object.into_storage();
    assert_eq!(store.list_count, 0, "point reads must never list");
    assert_eq!(
        store.get_count, 2,
        "expected 2 fetches (key:0000, then key:0001); the repeat GET must not refetch"
    );
}

/// A key written this session is authoritative in memory and is not re-fetched
/// from storage on a later read.
#[test]
fn written_key_is_not_refetched() {
    let store = seeded_store(10);
    let mut object = EdgeObject::open_lazy(store).unwrap();

    // Write a brand-new key the store has never held. SET's `command_keys` is
    // `Keys([fresh])`, so the lazy path fetches `fresh` once (a miss) before the
    // write — that is the only fetch of `fresh` the whole test should ever do.
    let set = object.handle_http(EdgeHttpRequest::get("/v1/valdr/seed/SET/fresh/hello", 2_000));
    assert_eq!(set.status, 200);
    // Read it back — must serve from memory (the SET flushed it and marked it
    // resident), so the GET adds no further fetch of `fresh`.
    let get = object.handle_http(EdgeHttpRequest::get("/v1/valdr/seed/GET/fresh", 2_000));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&get.body).unwrap(),
        serde_json::json!({ "result": "hello" })
    );

    let store = object.into_storage();
    assert_eq!(store.list_count, 0, "writing + reading one key must not list");
    assert_eq!(
        store
            .got_keys
            .iter()
            .filter(|k| **k == key_storage_key(b"fresh"))
            .count(),
        1,
        "a key written this session must be fetched at most once (the SET miss), \
         never re-fetched on the later GET"
    );
}

/// THE ENUMERATION FALLBACK. A `SCAN` is a `FullKeyspace` command: exactly one
/// `list()`, and a repeat enumeration (`KEYS`) does not re-list.
#[test]
fn scan_and_keys_trigger_exactly_one_list() {
    let store = seeded_store(10);
    let mut object = EdgeObject::open_lazy(store).unwrap();

    let scan = object.handle_http(EdgeHttpRequest::get(
        "/v1/valdr/seed/SCAN/0/COUNT/1000",
        2_000,
    ));
    assert_eq!(scan.status, 200);
    assert_eq!(
        object.storage().list_count, 1,
        "SCAN must list exactly once"
    );

    let keys = object.handle_http(EdgeHttpRequest::get("/v1/valdr/seed/KEYS/*", 2_000));
    assert_eq!(keys.status, 200);
    assert_eq!(
        object.storage().list_count,
        1,
        "a repeat enumeration must not re-list once fully loaded"
    );

    // The SCAN actually enumerated the seeded keyspace.
    let scan_json: serde_json::Value = serde_json::from_slice(&scan.body).unwrap();
    let cursor_and_keys = scan_json
        .get("result")
        .and_then(|v| v.as_array())
        .expect("SCAN reply is [cursor, [keys]]");
    let enumerated = cursor_and_keys[1].as_array().expect("SCAN key array");
    assert_eq!(enumerated.len(), 10, "SCAN should see all 10 seeded keys");

    eprintln!(
        "enumeration fallback: SCAN+KEYS over a 10-key store -> {} list(s)",
        object.storage().list_count
    );
}

/// PARITY — a representative raw-command sequence produces byte-identical HTTP
/// responses through the eager object and the lazy object. The eager object
/// holds the seeded keyspace from `open`; the lazy object fetches each key on
/// demand. Identical responses prove `command_keys` / `rest_command_keys` never
/// under-fetch for this command set.
#[test]
fn lazy_matches_eager_on_a_representative_raw_command_sequence() {
    // Seed both objects from the same starting keyspace (a few preexisting keys).
    let mut base = MemoryObjectStorage::default();
    {
        let mut seed = EdgeObject::open(MemoryObjectStorage::default()).unwrap();
        for (k, v) in [("alpha", "1"), ("beta", "2"), ("gamma", "3")] {
            seed.handle_http(EdgeHttpRequest::get(
                &format!("/v1/valdr/t/SET/{k}/{v}"),
                1_000,
            ));
        }
        for (skey, bytes) in seed.into_storage().list().unwrap() {
            base.put(&skey, &bytes).unwrap();
        }
        base.drain_dirty();
    }

    let mut eager = EdgeObject::open(base.clone()).unwrap();
    let mut lazy = EdgeObject::open_lazy(base).unwrap();

    // A spread of single-key, multi-key, write, expiry, hash, and enumerating
    // commands — exactly the operations the dogfood scenarios drive.
    let ops: &[&str] = &[
        "/v1/valdr/t/GET/alpha",
        "/v1/valdr/t/SET/delta/4",
        "/v1/valdr/t/INCR/counter",
        "/v1/valdr/t/INCRBY/counter/41",
        "/v1/valdr/t/APPEND/alpha/-tail",
        "/v1/valdr/t/MGET/alpha/beta/gamma/delta",
        "/v1/valdr/t/EXISTS/alpha/missing",
        "/v1/valdr/t/HSET/h/f1/v1/f2/v2",
        "/v1/valdr/t/HGET/h/f1",
        "/v1/valdr/t/SET/ttlkey/x?PX=5000",
        "/v1/valdr/t/PTTL/ttlkey",
        "/v1/valdr/t/DEL/beta",
        "/v1/valdr/t/GET/beta",
        "/v1/valdr/t/SCAN/0/COUNT/1000",
        "/v1/valdr/t/DBSIZE",
        "/v1/valdr/t/GET/gamma",
    ];

    for (i, path) in ops.iter().enumerate() {
        let now = 2_000 + i as u64;
        let eager_resp = eager.handle_http(EdgeHttpRequest::get(path, now));
        let lazy_resp = lazy.handle_http(EdgeHttpRequest::get(path, now));
        // SCAN enumerates the engine's `db` HashMap, whose iteration order
        // differs between an eager object (keys inserted in SET order) and a
        // lazy one (keys inserted in fetch / `list()` order). The element set is
        // identical — exactly the `scan_reply` order-insensitivity the
        // differential oracle and the lazy_loader_kit both account for — so SCAN
        // is compared as a multiset; every other reply is byte-exact.
        let order_insensitive = path.contains("/SCAN/") || path.contains("/KEYS/");
        assert_responses_match(path, &eager_resp, &lazy_resp, order_insensitive);
    }
}

/// PARITY — the limiter (policy + EVALSHA token bucket) path is byte-identical
/// through eager and lazy, and the lazy limiter never lists the keyspace. This
/// is the demo's primary workload, and exercises the `check_key_access`
/// precise-keys path (bucket + policy) on the lazy side: the limiter script's
/// key set is fully known, so the lazy object loads exactly those two keys
/// instead of conservatively listing for the `EVALSHA`.
#[test]
fn lazy_matches_eager_on_the_limiter_path() {
    let policy = Policy::token_bucket(10, 5, 1_000, 60_000);

    let mut eager = EdgeObject::open(MemoryObjectStorage::default()).unwrap();
    let mut lazy = EdgeObject::open_lazy(RecordingStorage::default()).unwrap();

    eager.install_policy("acme", policy).unwrap();
    lazy.install_policy("acme", policy).unwrap();

    for (i, now) in [1_000u64, 1_050, 1_100, 1_500, 2_000].into_iter().enumerate() {
        let request = edgestash_demo::LimitRequest {
            tenant_id: "acme",
            now_millis: now,
            cost: 3,
        };
        let eager_decision = eager.check(request).unwrap();
        let lazy_decision = lazy.check(request).unwrap();
        assert_eq!(
            eager_decision, lazy_decision,
            "limiter decision diverged at step {i} (now={now})"
        );
    }

    // The lazy limiter path stays O(touched): it loads bucket+policy per check
    // via `check_key_access`, never the whole keyspace.
    assert_eq!(
        lazy.storage().list_count, 0,
        "the limiter path must never list the keyspace"
    );
}

fn assert_responses_match(
    path: &str,
    eager: &EdgeHttpResponse,
    lazy: &EdgeHttpResponse,
    order_insensitive: bool,
) {
    assert_eq!(
        eager.status, lazy.status,
        "status diverged for {path}: eager={} lazy={}",
        eager.status, lazy.status
    );
    if order_insensitive && scan_replies_equal_unordered(&eager.body, &lazy.body) {
        return;
    }
    assert_eq!(
        eager.body, lazy.body,
        "body diverged for {path}:\n  eager: {}\n  lazy:  {}",
        String::from_utf8_lossy(&eager.body),
        String::from_utf8_lossy(&lazy.body)
    );
}

/// Compare two enumeration replies (`{"result":[cursor,[keys]]}` for SCAN, or
/// `{"result":[keys]}` for KEYS) ignoring the order of the key list — the
/// cursor (always `"0"` here, both engines fully enumerate) and the key multiset
/// must match.
fn scan_replies_equal_unordered(a: &[u8], b: &[u8]) -> bool {
    let (Ok(av), Ok(bv)) = (
        serde_json::from_slice::<serde_json::Value>(a),
        serde_json::from_slice::<serde_json::Value>(b),
    ) else {
        return false;
    };
    sorted_keys(&av) == sorted_keys(&bv)
}

/// Pull the key array out of a SCAN or KEYS `result` and return it sorted, so two
/// replies holding the same keys in different orders compare equal.
fn sorted_keys(value: &serde_json::Value) -> Option<Vec<String>> {
    let result = value.get("result")?;
    let array = match result.as_array()? {
        // SCAN: [cursor, [keys]] — only the cursor "0" is meaningful here, and it
        // is identical for both engines (full enumeration), so compare the keys.
        items if items.len() == 2 && items[1].is_array() => items[1].as_array()?,
        // KEYS: [keys]
        items => items,
    };
    let mut keys: Vec<String> = array
        .iter()
        .map(|v| v.as_str().map(str::to_owned))
        .collect::<Option<Vec<String>>>()?;
    keys.sort();
    Some(keys)
}

/// Mirror of the crate-internal `k:<hex>` storage-key layout so the test can
/// name the exact storage-key a touched Redis key resolves to.
fn key_storage_key(redis_key: &[u8]) -> String {
    let mut out = String::with_capacity(2 + redis_key.len() * 2);
    out.push_str("k:");
    for byte in redis_key {
        out.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        out.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
    }
    out
}
