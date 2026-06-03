# Keyspace COW Payload Sharing Spec

**Status:** Packet M landing record and next-packet recommendation. Written
after `221da48 feat: add segmented snapshots and AOF fault gates`, implemented
on top of `787f63c docs: specify keyspace COW payload sharing packet`.
**Date:** 2026-06-03.
**Scope:** `redis-core` keyspace/object layout, held-snapshot observability,
snapshot-window write cost, AOF/RDB rewrite gates, and benchmark tooling.

---

## 1. Why This Is Next

The previous packet proved the first forkless snapshot step:

- `RedisDb` stores keys in segmented copy-on-write `KeyspaceMap`.
- `snapshot_all_dbs()` captures shared segment roots through the
  `KeyspaceSnapshot` facade.
- AOF/RDB consumers did not learn a new contract.
- AOF rewrite-start snapshot capture at 100k keys dropped from Packet J's
  19319 us to 97 us.

That removes the worst command-path deep clone, but it does not finish the
forkless COW story. The cost moved into the snapshot window:

- The first live write to a segment that a snapshot still holds clones that
  segment's `HashMap`.
- Segment clone currently clones `RedisObject` values.
- `RedisObject` still contains metadata (`lru`, `expire`) and payload
  (`kind`) in one owned value.
- Large mutable payloads can still be copied when the index segment clones or
  when a payload mutation happens while a snapshot is live.

The next high-leverage work is therefore:

1. Make held-snapshot COW pressure visible in production.
2. Extend the toy model to separate metadata-only mutations from payload
   mutations.
3. Implement the smallest payload-sharing step that the evidence justifies.

This packet is deliberately not "rewrite the object system because it feels
right." It is "instrument first, model first, then cut payload copying where the
numbers say the current design pays too much."

2026-06-03 conclusion:

- Held-snapshot COW counters and `INFO persistence` visibility are in.
- `aof-rewrite-latency.py` now samples those counters before/during/after the
  rewrite window.
- The standalone model now has production-shaped deep segmented COW plus
  metadata/payload split variants.
- The model strongly supports payload sharing for medium/large values held
  across snapshots, but does not justify a broad production `RedisObject`
  layout migration in this packet because small-value live hot paths showed
  real risk.
- The production landing is therefore telemetry plus model evidence. The next
  payload-sharing packet should be narrower than "wrap every ObjectKind in
  Arc" unless fresh evidence proves the broad shape is safe.

---

## 2. Current Concrete Shape

Key code after `221da48`:

- `crates/redis-core/src/keyspace_map.rs`
  - `KeyspaceMap { segments: Vec<Arc<HashMap<RedisString, RedisObject>>>, len }`
  - `snapshot()` clones segment roots.
  - `insert`, `get_mut`, and `remove` use `Arc::make_mut` on one segment.
- `crates/redis-core/src/keyspace_snapshot.rs`
  - `KeyspaceSnapshotDb` stores either owned entries or a shared
    `KeyspaceMapSnapshot`.
  - Saver-side materialization still clones entries behind the facade.
- `crates/redis-core/src/object.rs`
  - `RedisObject { lru, expire, kind }`.
  - `ObjectKind` owns strings, lists, hashes, sets, zsets, streams, JSON, bloom.
- `crates/redis-core/src/persistence.rs`
  - Tracks rewrite snapshot key count and capture micros.
- `crates/redis-commands/src/info.rs`
  - Renders `aof_last_rewrite_snapshot_keys` and
    `aof_last_rewrite_snapshot_us`.

Current blind spots:

- No counter for active keyspace snapshots.
- No counter for segment clones caused by held snapshots.
- No counter for how many keys/estimated payload bytes are cloned by those
  segment copies.
- No way to correlate a `BGREWRITEAOF` window with keyspace COW pressure from
  `INFO`.
- No model variant that represents "metadata cloned, payload shared."

---

## 3. End State

The ambitious end state is a forkless snapshot keyspace that has:

- Root/segment clone capture at save start.
- Segment clone accounting during held snapshots.
- Metadata updates that do not clone large payload bytes.
- Payload mutations that clone only the payload being mutated, only when a
  snapshot or another owner still references that payload.
- AOF/RDB snapshot consumers kept behind `KeyspaceSnapshot`.
- Normal command throughput guarded by repeated profile/focused probes.

Conceptual target:

```rust
pub struct KeyspaceEntry {
    pub lru: LruClock,
    pub expire: i64,
    pub payload: Arc<ObjectPayload>,
}

pub struct ObjectPayload {
    pub kind: ObjectKind,
}
```

Important distinction:

- Metadata COW should copy `lru`/`expire` and an `Arc` pointer.
- Payload COW should clone payload bytes/collections only when mutating a
  payload with more than one owner.

This is closer to Valkey's object refcounting than `Arc<RedisObject>` would be,
because `Arc<RedisObject>` would put metadata and payload behind the same
reference count. LRU or TTL changes could then force whole-object COW.

---

## 4. Non-Goals

- Do not replace `RuntimeOwner` or DB ownership architecture.
- Do not switch the live keyspace to a generic HAMT by default.
- Do not add benchmark-only command fast paths.
- Do not make public performance claims from one local run.
- Do not change AOF/RDB consumer contracts beyond `KeyspaceSnapshot`.
- Do not port persistent inner encodings for every large collection in the
  first implementation step.
- Do not hide regressions by disabling LRU, expiration, WATCH, MULTI, AOF,
  replication, pub/sub, blocking, or scripting behavior.

---

## 5. Packet A: Held-Snapshot COW Counters

This packet should land before payload sharing. It creates the instrumentation
needed to know whether deeper work helped.

### 5.1 Counters

Add keyspace COW counters in `redis-core`, probably next to metrics or
persistence state:

- `keyspace_cow_active_snapshots`
- `keyspace_cow_snapshot_starts_total`
- `keyspace_cow_snapshot_drops_total`
- `keyspace_cow_segment_clone_total`
- `keyspace_cow_segment_clone_keys_total`
- `keyspace_cow_segment_clone_estimated_bytes_total`
- `keyspace_cow_segment_clone_max_keys`
- `keyspace_cow_segment_clone_max_estimated_bytes`
- `keyspace_cow_segment_clone_micros_total`
- `keyspace_cow_segment_clone_max_micros`

Use atomics. These counters are telemetry, not exact allocator accounting.

### 5.2 Where To Count

In `KeyspaceMap::snapshot()`:

- Increment snapshot starts.
- Increment active snapshots.
- Return a snapshot guard object whose `Drop` decrements active snapshots and
  increments snapshot drops.

In write paths that may call `Arc::make_mut`:

- Before `make_mut`, check `Arc::strong_count(&segment) > 1`.
- If shared, measure:
  - segment key count;
  - estimated clone bytes;
  - elapsed clone micros.
- Then `Arc::make_mut`.

Avoid expensive per-object deep size walks in the hot path by default. Estimated
bytes can start as:

- `segment.len() * size_of::<(RedisString, RedisObject)>()`
- plus cheap payload size only for obvious strings if already available.

The exact allocator truth can come from a later memory profiler. The first
counter only needs to show relative pressure by workload and segment count.

### 5.3 INFO Surface

Expose counters under `INFO persistence` or `INFO stats`.

Preferred names:

- `keyspace_cow_active_snapshots`
- `keyspace_cow_segment_clones`
- `keyspace_cow_segment_clone_keys`
- `keyspace_cow_segment_clone_estimated_bytes`
- `keyspace_cow_segment_clone_max_keys`
- `keyspace_cow_segment_clone_max_estimated_bytes`
- `keyspace_cow_segment_clone_max_us`

Also consider preserving last-rewrite-specific fields:

- `aof_last_rewrite_cow_segment_clones`
- `aof_last_rewrite_cow_clone_estimated_bytes`
- `aof_last_rewrite_cow_clone_max_us`

That makes `BGREWRITEAOF` windows inspectable without external profilers.

### 5.4 Tests

Add focused `redis-core` tests:

- Snapshot increments active count and drops it on `Drop`.
- Insert into a shared segment increments clone counters once.
- Repeated writes to the same already-unshared segment do not keep counting.
- Misses do not call `make_mut` and do not count clones.
- Remove on missing key does not count clones.
- Snapshot isolation still holds after insert/update/delete.

Add an `INFO` test if there is an existing info kit pattern that can be used
without large harness work.

---

## 6. Packet B: Model Metadata/Payload Split

Extend `harness/models/keyspace-cow-model` before production payload sharing.

New variants:

- `entry`: full index clone, metadata by value, payload in `Arc`.
- `seg_entry`: segmented COW index with metadata by value and `Arc` payload.
- Optional `seg_entry_mut_payload`: same as `seg_entry`, but write phase mutates
  payload contents through `Arc::make_mut`.

New phases:

- `metadata_touch`: mutate only LRU-like metadata.
- `ttl_touch`: mutate expire-like metadata.
- `replace_payload`: replace value payload.
- `mutate_payload`: in-place payload mutation, like APPEND or collection update.
- `held_metadata_touch`: same as above while a snapshot is held.
- `held_mutate_payload`: mutate payload while a snapshot is held.

New payload sizes:

- 64 bytes: normal hot-path baseline.
- 1 KiB: medium string/listpack-like value.
- 64 KiB or 1 MiB: large value stress.

The model should report:

- snapshot latency;
- GET ns/op;
- metadata update ns/op;
- INCR-like update ns/op;
- held metadata update ns/op;
- held payload mutation ns/op;
- key clone bytes;
- payload clone bytes;
- RSS samples.

The model decides whether the production packet should:

- only add counters;
- share string payloads first;
- share all `ObjectKind` payloads;
- wait and tune segment count instead.

---

## 7. Packet C: Production Payload Sharing

Only start this after Packet A counters and Packet B model evidence exist.

### 7.1 Smallest Viable Production Step

The safest production path is not to rewrite every command at once. Use staged
types and preserve public methods:

1. Introduce `ObjectPayload`.
2. Move `ObjectKind` into `ObjectPayload`.
3. Change `RedisObject` to hold metadata plus `Arc<ObjectPayload>`.
4. Keep method names like `as_string_bytes`, `kind`, `kind_mut`, constructors,
   and clone behavior stable enough that command crates do not all churn at
   once.
5. Add explicit payload mutation helper:

```rust
impl RedisObject {
    pub fn payload_mut(&mut self) -> &mut ObjectPayload {
        Arc::make_mut(&mut self.payload)
    }
}
```

The key invariant: metadata-only methods must not call `payload_mut`.

### 7.2 Migration Strategy

Stage 1: compatibility wrapper.

- Add `ObjectPayload`.
- Add `RedisObject::payload()` and `payload_mut()`.
- Keep existing constructors.
- Update object methods internally.

Stage 2: command path migration.

- Convert direct `obj.kind` matches to helper methods where needed.
- Payload-mutating commands call `payload_mut`.
- Metadata-only calls (`lru`, `expire`) stay direct.

Stage 3: snapshot and persistence gates.

- `KeyspaceMap` segment clone should clone metadata plus `Arc` payload.
- Saver materialization can still clone `RedisObject`; clone should become cheap
  for payloads.
- AOF/RDB serialization can read payload through immutable refs.

Stage 4: optional payload-specific refinement.

- If broad `Arc<ObjectPayload>` hurts hot-path throughput, back down to
  string-only or large-payload-only sharing.
- If collection mutation clones too much, defer persistent inner encodings to a
  separate packet.

---

## 8. Tool Iteration Plan

### 8.1 Start Clean

Use the committed packet as the baseline:

```bash
git status --short
git log --oneline -4
```

Before modifying production code, capture the current signal:

```bash
cargo check -p redis-core -p redis-commands
cargo test --manifest-path harness/models/keyspace-cow-model/Cargo.toml
```

### 8.2 Model First

For model-only changes:

```bash
cargo test --manifest-path harness/models/keyspace-cow-model/Cargo.toml
cargo run --release --manifest-path harness/models/keyspace-cow-model/Cargo.toml -- \
  --keys 100000 --value-bytes 64 --read-ops 200000 --write-ops 10000 --segments 1024
cargo run --release --manifest-path harness/models/keyspace-cow-model/Cargo.toml -- \
  --keys 100000 --value-bytes 1024 --read-ops 200000 --write-ops 10000 --segments 1024
cargo run --release --manifest-path harness/models/keyspace-cow-model/Cargo.toml -- \
  --keys 100000 --value-bytes 65536 --read-ops 100000 --write-ops 5000 --segments 1024
```

Store TSV results under `harness/models/keyspace-cow-model/results/` and update
`RESULTS.md`.

### 8.3 Production Correctness Gates

After counter or payload-sharing code changes:

```bash
cargo check -p redis-core -p redis-commands
cargo test -p redis-core
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-server
python3 harness/oracle/persistence-cycle.py --mode rdb --skip-build
python3 harness/oracle/persistence-cycle.py --mode aof --skip-build
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite --skip-build
python3 harness/oracle/persistence-frontier.py --skip-build --fail-on-failure
```

Correctness stays ahead of performance. A payload-sharing bug can look like a
performance win by dropping state, so the restart/frontier gates are mandatory.

### 8.4 Performance Gates

Build release once:

```bash
cargo build --release -p redis-server
```

Snapshot/rewrite gate:

```bash
python3 harness/bench/aof-rewrite-latency.py \
  --targets rust --skip-build --dataset-sizes 5000,25000,100000
```

Normal hot-path gate:

```bash
VALKEY_BENCH_SKIP_BUILD=1 bash harness/bench/run-profile-matrix.sh
python3 harness/bench/default-suite-parts.py run \
  --mode ordered --target both --tests set,get,incr \
  --requests 50000 --clients 50 --pipeline 1 --payload 64 --no-build
python3 harness/bench/default-suite-parts.py run \
  --mode ordered --target both --tests set,get,incr \
  --requests 200000 --clients 50 --pipeline 16 --payload 64 --no-build
```

Held-snapshot stress gate to add or extend:

```bash
python3 harness/bench/keyspace-cow-window.py \
  --payload-bytes 64,1024,65536 \
  --dataset-size 100000 \
  --write-mode metadata,payload \
  --targets rust --skip-build
```

If adding a new runner is too much for the first packet, extend
`aof-rewrite-latency.py` to sample new COW `INFO` fields before/during/after
rewrite.

### 8.5 Profile Attribution

Only after repeated matrix/focused probes show a real gap:

```bash
python3 harness/bench/profile-hotspots.py --suite smoke --sample-seconds 4
python3 harness/bench/profile-calltree.py --suite big --profile-seconds 8
```

Do not run profile or benchmark agents concurrently. The benchmark host,
results directory, profiler, and ports are shared resources.

---

## 9. Acceptance Criteria

Packet A, counters-only, is accepted when:

- Active snapshot count returns to zero after rewrite completion.
- Segment clone counters move only during held-snapshot writes.
- Miss paths do not clone segments.
- Existing AOF/RDB/frontier gates pass.
- Rewrite-latency 5k/25k/100k remains in the same shape as `221da48`.
- Docs explain counter semantics and limits.

Packet B, model, is accepted when:

- Model tests prove snapshot isolation for metadata and payload variants.
- `RESULTS.md` compares deep, seg_hash, im, entry, and seg_entry.
- Results include 64 B, 1 KiB, and large payload cases.
- The model gives a clear production recommendation.

Packet C, production payload sharing, is accepted when:

- Snapshot isolation still holds across update/delete/metadata/payload mutation.
- Normal command throughput does not regress materially versus the committed
  baseline. Use repeated medians, not one run.
- Held-snapshot clone estimated bytes drop materially for medium/large payloads.
  Target: at least 50 percent lower in a 1 KiB or larger payload stress case.
- AOF/RDB restart cycles and full persistence frontier pass.
- Rewrite-latency capture stays root-clone sized.
- `INFO` exposes enough counters to debug future segment/payload tuning.

---

## 10. Risks

- **Hot-path Arc tax.** `Arc<ObjectPayload>` may add refcount cost or layout
  pressure even when no snapshot is active.
- **Command churn.** Many commands match or mutate `ObjectKind`; a broad
  migration can sprawl.
- **False precision.** Estimated clone bytes are telemetry, not allocator truth.
- **Large collection mutation.** Payload sharing does not make hash/list/zset
  inner encodings persistent. Mutating a huge collection with a shared payload
  can still clone the huge collection.
- **LRU behavior.** Reads touch LRU unless `LOOKUP_NOTOUCH`; metadata-only
  updates must stay cheap and correct.
- **Expiration semantics.** TTL metadata has import-mode, replica, and
  primary-link exceptions. Do not simplify these while moving metadata.
- **Benchmark noise.** The final matrix after `221da48` was mixed. Repeated
  focused probes are mandatory before calling a small ratio a regression.

---

## 11. Recommended First Goal

Start with counters plus model expansion, not broad object refactor. That gives
us production observability and first-principles evidence before touching the
widest object APIs.

If counters show low COW pressure under realistic rewrite windows, the next
move may be segment tuning or cleanup instead of payload sharing. If counters
show large segment/payload clone pressure, the model will tell us whether to
share all payloads, only strings, or only large payloads.

---

## 12. Packet M Landing Record

### 12.1 Implemented

Production code:

- `redis-core::keyspace_cow` owns lightweight atomic telemetry:
  active snapshots, snapshot starts/drops, segment clone count, total cloned
  keys, total estimated clone bytes, max cloned keys/bytes, and clone timing
  fields.
- `KeyspaceMapSnapshot` holds a snapshot guard, so active snapshot counters
  reflect live snapshot lifetime instead of only capture calls.
- `KeyspaceMap` records a segment clone when `Arc::make_mut` returns a new
  segment pointer.
- `INFO persistence` exposes:
  `keyspace_cow_active_snapshots`, `keyspace_cow_snapshot_starts`,
  `keyspace_cow_snapshot_drops`, `keyspace_cow_segment_clones`,
  `keyspace_cow_segment_clone_keys`,
  `keyspace_cow_segment_clone_estimated_bytes`,
  `keyspace_cow_segment_clone_max_keys`,
  `keyspace_cow_segment_clone_max_estimated_bytes`,
  `keyspace_cow_segment_clone_us`, and
  `keyspace_cow_segment_clone_max_us`.
- `aof-rewrite-latency.py` samples COW counters around `BGREWRITEAOF`, stores
  deltas and peaks, and waits through `LOADING` after restart.

Model/tooling:

- `harness/models/keyspace-cow-model` now separates:
  - `seg_deep_hash`: production-shaped segmented COW with owned payloads;
  - `seg_entry_hash`: segmented COW with metadata by value and `Arc<Payload>`;
  - full-index `entry` variants for comparison.
- Model output includes `entry_clone_mb` in addition to key and payload clone
  bytes.
- New model phases cover live metadata updates, held-snapshot metadata updates,
  held-snapshot replacement, held-snapshot payload mutation, and held-snapshot
  INCR-like mutation.

### 12.2 Counter Semantics

These counters are relative pressure telemetry, not allocator truth:

- Estimated bytes are entry size plus cheap string payload byte counts.
- Non-string object deep sizes are intentionally not walked in the command
  path.
- `keyspace_cow_segment_clone_us` and max-us are currently zero by design. The
  implementation records clones with one `Arc::make_mut` and a pointer compare;
  it does not add `Instant::now` or a pre-`strong_count` branch to every write.
  If clone timing becomes necessary, add it behind a debug/bench flag or a
  dedicated benchmark runner rather than unconditionally taxing normal writes.

### 12.3 Final Gates

Correctness:

```bash
cargo check -p redis-core -p redis-commands
cargo test -p redis-core
cargo test -p redis-commands info_persistence_exposes_keyspace_cow_fields -- --nocapture
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-server
python3 harness/oracle/persistence-cycle.py --mode rdb
python3 harness/oracle/persistence-cycle.py --mode aof
python3 harness/oracle/persistence-cycle.py --mode aof-rewrite
python3 harness/oracle/persistence-frontier.py --skip-build
```

Observed result:

- `redis-core`: 183 passed, 1 ignored; conn transport and doc-test gates passed
  or ignored as expected.
- AOF kit: 11/11.
- replication kit: 13/13.
- `redis-server`: 8/8.
- RDB/AOF/AOF-rewrite restart cycles: pass.
- full persistence frontier: 40/40 pass (`20260603T015103Z`).

Performance/model:

```bash
cargo test --manifest-path harness/models/keyspace-cow-model/Cargo.toml
bash harness/bench/run-profile-matrix.sh
python3 harness/bench/p1-parity-probe.py --commands set,get,incr --pipeline 1 --trials 10 --warmups 2 --requests 50000
python3 harness/bench/p1-parity-probe.py --commands set,get,incr --pipeline 16 --trials 10 --warmups 2 --requests 200000
python3 harness/bench/aof-rewrite-latency.py --dataset-sizes 5000,25000,100000
```

Artifacts:

- Model:
  - `harness/models/keyspace-cow-model/results/keys100k-v64-entry-split.tsv`
  - `harness/models/keyspace-cow-model/results/keys100k-v1k-entry-split.tsv`
  - `harness/models/keyspace-cow-model/results/keys4k-v64k-entry-split.tsv`
- Profile matrix:
  `harness/bench/results/20260603T014731Z-787f63c-profile-matrix.tsv`.
- Focused p1 probe:
  `harness/bench/results/20260603T014745Z-787f63c-p1-parity.json`.
- Focused p16 probe:
  `harness/bench/results/20260603T014809Z-787f63c-p1-parity.json`.
- Rewrite latency:
  `harness/bench/results/20260603T014824Z-787f63c-aof-rewrite-latency.*`.

### 12.4 Measured Readout

Normal throughput remains mixed but bounded:

- Profile matrix: median 0.98x, min 0.84x, max 1.33x.
- Focused p1 `SET`/`GET`/`INCR`: 1.020x / 1.051x / 1.008x.
- Focused p16 `SET`/`GET`/`INCR`: 1.211x / 1.006x / 0.910x.
- The p16 `INCR` miss is not a new COW cliff: an isolated baseline worktree at
  `787f63c` measured p16 `INCR` around 0.934x before this packet.

Rewrite-start capture stays root-clone sized:

| Dataset | Snapshot Keys | Capture | Command Wall | COW Clones | COW Keys | COW Est Bytes |
|---:|---:|---:|---:|---:|---:|---:|
| 5k | 7,819 | 57 us | 8.682 ms | 20 | 126 | 19,690 |
| 25k | 27,952 | 60 us | 14.270 ms | 117 | 3,038 | 479,682 |
| 100k | 102,937 | 111 us | 8.372 ms | 439 | 43,076 | 6,835,864 |

The 100k row is the main sanity check: snapshot capture is still roughly
100 us, while the held-window counters show real COW pressure that was
previously invisible.

### 12.5 Model Readout

100k keys, 64-byte values, 1024 segments:

- `seg_deep_hash` held payload mutation cloned about 1.53 MiB of keys plus
  6.10 MiB of payload and ran at 751 ns/op.
- `seg_entry_hash` held metadata mutation cloned about 1.53 MiB of keys plus
  2.29 MiB of entry metadata and zero payload bytes, at 277 ns/op.
- `seg_entry_hash` held payload mutation cloned only the touched payloads
  (about 0.58 MiB), at 426 ns/op.
- Risk: small-value live `replace_live` was much slower in the entry split
  variant in this run, 1131 ns/op versus 174 ns/op for production-shaped
  `seg_deep_hash`.

100k keys, 1 KiB values:

- `seg_deep_hash` held mutation cloned about 97.62 MiB of payload and ran
  around 2.36-2.39 us/op.
- `seg_entry_hash` held mutation cloned about 9.31 MiB of touched payload and
  ran around 0.90-1.11 us/op.

4k keys, 64 KiB values:

- `seg_deep_hash` held mutation cloned about 255.75 MiB of payload and ran
  around 28.9-35.4 us/op.
- `seg_entry_hash` held mutation cloned about 55.94 MiB of touched payload and
  ran around 4.9-5.0 us/op.

### 12.6 Decision

Do not land broad production payload sharing in this packet.

The evidence says payload sharing is very likely the right direction for
large values held across rewrite snapshots. It does not say that changing every
`RedisObject` into metadata plus `Arc<ObjectPayload>` is safe for the normal
small-value command path. The next packet should narrow the experiment:

- large string payload handle first, or
- collection-specific payload handle where mutation cost is worst, or
- a production feature flag/alternate object layout benchmark before migration.

Keep the new counters in production now. They are cheap enough for default use
and give the next packet a real measurement surface.
