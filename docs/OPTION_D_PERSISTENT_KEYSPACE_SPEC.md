# Option D — Forkless point-in-time snapshots via a persistent keyspace

**Status:** phase-one implementation checkpoint, updated with toy-model and
production AOF rewrite evidence. Segmented COW `KeyspaceMap` is landed as the
current live keyspace; value-payload sharing and persistent inner encodings are
still future work.
**Author:** repl-observability follow-up, 2026-06-02.
**One-line:** replace `fork()`'s page-level copy-on-write with safe Rust
snapshotting. The goal is data-structure-level structural sharing, but the
evidence below says HAMT is only one candidate, not the destination by default.

All C citations are verbatim from the pinned upstream `reference/valkey/src/`
(Valkey **9.1.0**, `VALKEY_VERSION` `src/version.h`; tree `c9e8005e9`). Line
numbers are exact; re-verify against that pin before editing.

---

## 1. What we are actually replacing — fork/COW in C

Redis/Valkey takes a consistent point-in-time snapshot of the entire dataset
*without locking and without blocking the main loop* by forking. The child
inherits the parent's address space via kernel copy-on-write; it serializes a
frozen instant while the parent keeps serving, and the kernel duplicates only
the pages written during the save.

The single fork entry point, used by RDB save, AOF rewrite, and replication:

- `server.c:7060` — `int serverFork(int purpose) {` (purpose ∈ `CHILD_TYPE_*`,
  `server.h:1734-1739`).
- `server.c:7072` — `if ((childpid = valkey_fork()) == 0) {` — the child branch.
- `server.c:7099-7109` — fork is **measured as a rate**, which is the tell that
  its cost scales with heap size:
  ```c
  server.stat_fork_time = ustime() - start;
  server.stat_fork_rate =
      (double)zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024 * 1024 * 1024); /* GB per second. */
  latencyAddSampleIfNeeded("fork", server.stat_fork_time);
  ```

BGSAVE and the diskless-replication child both go through it:
- `rdb.c:1673` — `if ((childpid = serverFork(CHILD_TYPE_RDB)) == 0) {` then
  `rdb.c:1684` child `retval = rdbSave(...)`, `rdb.c:1697`
  `server.rdb_child_type = RDB_CHILD_TYPE_DISK;`.
- `rdb.c:3800` — diskless: child runs `rdbSaveRioWithEOFMark(...)` writing the
  RDB **straight to the replica sockets**; `rdb.c:3881`
  `server.rdb_child_type = RDB_CHILD_TYPE_SOCKET;` (`RDB_CHILD_TYPE_*`,
  `server.h:648-650`).

### The fork tax C itself pays (and instruments)

fork is not free; the upstream code is littered with mitigations that *are the
evidence* of its cost:

- **COW amplification.** The child measures how much memory the save actually
  cost by reading dirtied pages: `childinfo.c:84` `cow = zmalloc_get_private_dirty(-1);`
  → `zmalloc.c:940` `zmalloc_get_private_dirty` → `zmalloc.c:878`
  `zmalloc_get_smap_bytes_by_field("Private_Dirty:", ...)` (reads
  `/proc/self/smaps`). Under write load every touched page is duplicated; this
  is the well-known "BGSAVE can ~2× memory" hazard. Reported as
  `server.stat_rdb_cow_bytes` (`childinfo.c:133`).
- **Active COW avoidance.** The child immediately hands pages back to the OS to
  *avoid* COW: `server.h:103` `#define dismissMemory zmadvise_dontneed`;
  `server.c:7175` `dismissMemoryInChild()` walks the repl buffer + every client
  and `madvise(MADV_DONTNEED)`s them (`zmalloc.c:536`, comment: *"to avoid CoW
  when the parent modifies those shared pages"*).
- **THP is a landmine.** `server.c:7177` `if (server.thp_enabled) return;` —
  `MADV_DONTNEED` doesn't work under Transparent Huge Pages, so Valkey detects
  THP and tries to disable it process-wide (`server.c:6823` `THPDisable()` via
  `prctl(PR_SET_THP_DISABLE)`).

**The point:** fork buys an O(1)-ish consistent snapshot, but pays a
heap-size-proportional latency spike at fork time plus up-to-2× transient memory
under writes, and drags a tail of OS-specific hazards (THP, overcommit,
`MADV_DONTNEED`). In **safe, multi-threaded Rust** it is additionally unsound:
`fork()` copies only the calling thread, orphaning every lock other threads
held (allocator, logger, async runtime) — the child may legally call only
async-signal-safe functions, which excludes essentially all allocating Rust.

---

## 2. The behavior we MUST preserve (so the oracle still passes)

We do not have to reproduce *fork*; we have to reproduce its observable
consequences, because that is what the test suite asserts. The full-sync state
machine the snapshot drives:

- Primary-side replica states (`server.h:440-446`):
  ```c
  #define REPLICA_STATE_WAIT_BGSAVE_START 6 /* We need to produce a new RDB file. */
  #define REPLICA_STATE_WAIT_BGSAVE_END 7   /* Waiting RDB file creation to finish. */
  #define REPLICA_STATE_SEND_BULK 8         /* Sending RDB file to replica. */
  #define REPLICA_STATE_ONLINE 9            /* RDB file transmitted, sending just updates. */
  ```
- Replica-side link states (`server.h:389-406`): `REPL_STATE_CONNECT …
  REPL_STATE_TRANSFER … REPL_STATE_CONNECTED`.
- `syncCommand` puts a replica into `WAIT_BGSAVE_START` and either piggybacks an
  in-flight save or starts one (`replication.c:1090`
  `c->repl_data->repl_state = REPLICA_STATE_WAIT_BGSAVE_START;`).
- The **diskless-sync-delay window** — the thing the hanging `replication.tcl`
  block-1 tests poll for — lives in `shouldStartChildReplication`
  (`replication.c:5442`): a BGSAVE starts only when
  `max_idle >= server.repl_diskless_sync_delay` **or**
  `replicas_waiting >= server.repl_diskless_sync_max_replicas`. That deliberate
  hold is what makes `handshake`/`wait_bgsave` observable.
- `replicationSetupReplicaForFullResync` (`replication.c:826`) → `WAIT_BGSAVE_END`
  + sends `+FULLRESYNC replid offset`; `replicaPutOnline` (`replication.c:1586`)
  → `ONLINE`.
- Partial resync needs the backlog: `replBacklog` (`server.h:1067-1077`) with a
  `rax *blocks_index` for offset lookup; `feedReplicationBuffer`
  (`replication.c:449`) appends and advances `primary_repl_offset`.

A forkless snapshot satisfies all of these as long as: (a) the snapshot is taken
at a well-defined instant, (b) the save runs concurrently with the main loop so
the in-progress window is real, and (c) the post-snapshot command stream is
buffered and flushed on completion. Structural sharing gives (a) cheaply, the
saver thread gives (b), and (c) is the existing backlog, already wired in the
port.

---

## 3. The Rust port today (what changed in phase one)

- Keyspace: `crates/redis-core/src/db.rs` now stores `dict: KeyspaceMap`.
  `KeyspaceMap` is a segmented copy-on-write table:
  `Vec<Arc<HashMap<RedisString, RedisObject>>>`. A snapshot clones segment
  roots in O(segment count) instead of walking every key.
- Snapshot facade: `crates/redis-core/src/keyspace_snapshot.rs` remains the only
  consumer contract. AOF/RDB callers receive `KeyspaceSnapshot`; they do not
  know whether a DB snapshot is owned/deep or shared/segmented.
- Command capture: `command_context.rs` `snapshot_all_dbs` now calls
  `db.snapshot_keyspace()` and wraps the result in `KeyspaceSnapshotDb`. The
  owner-thread capture is therefore root cloning, not full key/value cloning.
- Serialization/materialization: the current RDB/AOF writer still eventually
  materializes cloned `(RedisString, RedisObject)` entries through the
  `KeyspaceSnapshot` facade. The big win is moving that O(N) work off the
  command path; this phase does not yet introduce shared object payloads.
- Values: `RedisObject` is still owned by value. `object.rs` still names the
  deeper target: `makeObjectShared` maps to `Arc<RedisObject>` or, better, to a
  metadata/payload split. That work is intentionally not bundled into this
  packet.

So the current port is no longer the worst case on both axes. It still has
owned values and background materialization, but `BGREWRITEAOF` start no longer
deep-clones the dataset on the owner thread. The remaining forkless/value-COW
work is real core data-structure work, not a precondition for the first
rewrite-start latency win.

---

## 4. The Option D architecture

Option D should now mean: a forkless point-in-time snapshot contract. It should
not mean: commit the live keyspace to a generic HAMT before the real Valdr path
has numbers.

Two layers can become structurally shared, but they are separable. Value sharing
is faithful to C's `robj` refcounting and likely worth pursuing. Index sharing
is the risky part and must stay evidence-gated.

### 4.1 Values → shared payloads, not `Arc<RedisObject>`

C already refcounts every value and shares immutable ones:
- `server.h:820-830` `struct serverObject { … unsigned refcount : OBJ_REFCOUNT_BITS; … }`
- `object.c:615` `incrRefCount`, `object.c:627` `decrRefCount` (frees at
  refcount 1), `object.c:131` `makeObjectShared` (`refcount = OBJ_SHARED_REFCOUNT`,
  `server.h:779`).

The naive Rust mapping is `HashMap<Key, Arc<RedisObject>>`. That is too coarse
for this port as written. `RedisObject` currently carries mutable metadata
(`lru`, `expire`) and the value payload (`kind`) together
(`object.rs:428-434`). Reads can touch LRU (`db.rs:667/722`), TTL commands touch
`expire` (`db.rs:849/860`), and value commands mutate `kind`.

If the whole object is inside one `Arc`, a metadata update during a live snapshot
can clone the whole payload. That is the wrong shape for large strings and
collections, and it would make ordinary read/LRU behavior look like value COW.

Better Rust shape, conceptually:

```rust
struct KeyspaceEntry {
    lru: LruClock,
    expire: i64,
    payload: Arc<ObjectPayload>,
}
```

`ObjectPayload` is the old `RedisObject.kind` content. Snapshotting clones the
payload `Arc`, not the bytes. Metadata-only changes produce a new live entry
that reuses the same payload `Arc`. Payload mutation uses `Arc::make_mut`
semantics: if a snapshot also holds the payload, clone the payload, mutate the
clone, and install it. With no live snapshot, `make_mut` is a no-op.

This preserves the C refcounting idea while avoiding an avoidable performance
trap in the current Rust object layout.

### 4.2 Keyspace index → structurally shared snapshot view

The index from key → entry needs a cheap, stable snapshot view. A HAMT is one
way to get that, but it is not the only way and it should not be the default
conclusion.

The target contract:

- **Snapshot:** capture a consistent root without walking every key.
- **GET:** stay close to the current `std::HashMap` path; GET/SET margin is not
  large enough to spend casually.
- **Insert/update/delete:** preserve snapshot isolation while bounding write
  amplification during the save window.
- **Iteration:** let the saver walk an immutable view without locks or rehash
  hazards.

Candidate shapes:

- **Full clone baseline:** keep `std::HashMap`; snapshot clones every key and
  entry. Lowest GET risk, worst snapshot latency.
- **`Arc` values + full index clone:** clone keys/index, share payloads. This is
  a smaller copy but still O(N) in key count.
- **Segmented COW table:** route keys into many small map segments; snapshot
  clones segment roots, and writes clone only the first touched segment while a
  snapshot is live. This keeps lookup closer to hash-table behavior at the cost
  of segment tuning and first-write-per-segment clone spikes.
- **HAMT / persistent map:** snapshot is root clone; writes path-copy trie nodes.
  This is clean and general, but lookup adds pointer chasing and generic crate
  overhead that the toy model makes visible.

This is "software COW": the in-process, allocator-safe equivalent of what fork
does in the kernel. The open question is the map shape that gives enough COW
benefit without losing too much steady-state command throughput.

C analogues for the "iterate a mutating table consistently" problem this solves
for free: today C must use the stateless reverse-binary-cursor `dictScan`
(`dict.c:1093` — *"the hash table may be resized between iteration calls"*) and
the new `hashtableScan` which **pauses rehashing during the scan**
(`hashtable.c:2046`). With a persistent map the saver holds an immutable root and
needs none of that machinery — iteration consistency is structural.

### 4.3 The forkless saver

```
BGSAVE / full-sync RDB:
  let snap: KeyspaceSnapshot = db.snapshot();   // cheap: clone DB snapshot roots
  spawn_saver_thread(move || {
      for (k, v) in snap.iter() {                // reads a frozen tree; no locks
          rdb::write_entry(&mut out, k, &v);     // payloads shared, not deep-copied
      }
      // disk:     write to temp file, fsync, rename     (replaces rdb.c:1684)
      // diskless: write to replica sockets / pipe       (replaces rdb.c:3800)
  });
  // owner thread keeps serving; writes path-copy the live root + make_mut values.
```

This maps onto the existing `ReplBgsaveJob` state machine (`persist.rs:782`),
but the lifecycle changes materially: "child PID" state becomes job completion
state, and Unix `waitpid` reapers need to stop owning the success path. State
reporting (`rdb_bgsave_in_progress`, the `WAIT_BGSAVE_*` transitions, P1's
ROLE/INFO work) becomes trivially truthful because the save genuinely runs for a
measurable window.

### 4.4 The honest weakness: large mutable values

Structural sharing is node/segment-granular on the *index*, but a payload can
still be one `Arc`. If, during a save, the owner mutates a **large** collection
a snapshot holds (e.g. `HSET` on a million-field hash, `APPEND` to a 1 MB
string), `make_mut` copies the **whole payload**. fork avoids this because
page-COW only duplicates the touched pages of that value's bytes.

Mitigations, in increasing cost:
1. **Most values are small** (listpack-encoded — `OBJ_ENCODING_LISTPACK`,
   `server.h:776`). Copy-on-write of a small value is cheap; this covers the
   common case.
2. **Bound the exposure window.** Saves are infrequent and time-bounded; only
   values mutated *during* a save pay, so the amortized cost is low. Track and
   `log()` worst-case copies (no silent spikes).
3. **Persistent inner encodings** for the big-collection types (persistent
   `im::Vector`/skiplist/HAMT inside zset/hash/list) — full fidelity to fork's
   incremental-COW behavior, but this is the deep end (it touches the entire
   `redis-ds` encoding layer) and should be deferred until evidence demands it.

This weakness is the main reason full structural sharing is a long-term
data-structure effort, not a quick persistence patch.

---

## 5. Performance estimates

Grounded, but intentionally not final. The toy model in
`harness/models/keyspace-cow-model` is a decision aid, not a substitute for
Valdr benchmarks. It is enough to reject "generic HAMT by default" and enough to
justify a first-principles `KeyspaceSnapshot` plan.

### 5.1 Steady state, no save running

| Op | Today (`std::HashMap`, owned) | Valkey 9.1 (`hashtable`, cache-line) | HAMT candidate + shared payloads |
|---|---|---|---|
| GET (index lookup) | ~1 cache miss | ~1 cache miss (`hashtable.c:224`, 64 B buckets) | ~log₃₂ N pointer chases ≈ 3–5 cache misses at 1 M keys |
| SET (insert/update) | amortized O(1) in place | amortized O(1) in place | path-copy O(log₃₂ N): ~4–6 small node allocs |
| Value access | move/borrow | refcount touch | `Arc` deref (1 indirection) + clone = 1 atomic |

Estimated HAMT command-throughput impact vs an in-place table, **index portion
only** (parsing/reply/IO are unchanged and dominate many commands):
- **Read-heavy (GET/HGET):** index lookup ~1.5–2.5× slower → net per-command
  hit likely **~10–30%** once non-index work is included.
- **Write-heavy (SET/DEL):** path-copy + alloc → net **~20–50%**, plus higher
  allocator pressure (more, smaller, short-lived allocations).

These are real and they cut *against* where Valkey just optimized: 9.1 moved the
keyspace **to** cache-line buckets (`hashtable.c`) precisely for lookup speed; a
generic HAMT moves the other way. Do not undersell this.

### 5.2 Memory

- **Permanent:** a HAMT pays bitmap + child-array node overhead, plausibly
  **1.3–2×** the index structure vs a packed open-addressing table. Segmented
  COW pays root/segment overhead instead. In both cases, the index is only a
  fraction of total RSS when values dominate, but tiny-value workloads make this
  ratio worse.
- **Transient during save:** only dirtied spine nodes + values mutated in the
  window are duplicated. For sparse writes this can be **well below** fork's
  page-COW; for big-value churn it can spike (§4.4). Net: D trades fork's
  *transient up-to-2×* for permanent index overhead and snapshot-window write
  amplification. The tail gets better only if the steady-state tax is contained.

### 5.3 Save-time latency (where forkless structural sharing wins)

- **fork today:** a heap-size-proportional stall on the event loop at fork time
  (page-table copy; tens to hundreds of µs per GB, worse under THP — the very
  thing `server.c:6823` fights), plus process-creation cost. This is a
  p99/p999 latency event on every BGSAVE.
- **Option D:** snapshot = clone roots/segments rather than values. **No fork,
  no page-table copy, no second process, no THP interaction.** The event-loop
  stall at save-start drops from "copy the dataset / fork the heap" to "clone
  snapshot handles." For the latency-sensitive positioning (cf. the
  cold-start-tail work) this is a genuine, demoable selling point if steady-state
  throughput stays acceptable.

### 5.4 vs the port *today*

Against the current `snapshot_all_dbs` (full O(N) deep copy on the owner thread,
then fork), a forkless snapshot path is clearly better at save start. The
nuanced part is the live keyspace representation: replacing the current
`std::HashMap` with a generic persistent map can spend too much on the hot path.
That is why the near-term plan should separate "delete fork" from "replace the
live map."

### 5.5 Toy-model evidence update

The reusable model lives at `harness/models/keyspace-cow-model`. It compares:

- `deep`: `HashMap<Key, Payload>` with full snapshot clone.
- `arc`: `HashMap<Key, Arc<Payload>>` with full index clone and shared values.
- `seg`: segmented copy-on-write `HashMap` roots with `id % segments` routing.
- `seg_hash`: segmented copy-on-write with key-byte hash routing, matching
  production `KeyspaceMap` more closely.
- `im`: persistent HAMT using the `im` crate.

Fresh model artifacts:

- `harness/models/keyspace-cow-model/results/keys100k-v64-fnv-incr-rss.tsv`
- `harness/models/keyspace-cow-model/results/keys1m-v64-fnv-incr-rss.tsv`

100k keys, 64-byte values, 1024 segments:

| Variant | Snapshot | GET ns/op | INCR ns/op | Held Replace ns/op | Held INCR ns/op | Snapshot Clone Bytes |
|---|---:|---:|---:|---:|---:|---:|
| deep | 9.420 ms | 57.8 | 104.1 | 206.2 | 97.5 | 1.53 MiB keys + 6.10 MiB payload |
| arc | 3.344 ms | 59.2 | 130.6 | 154.6 | 194.2 | 1.53 MiB keys |
| seg 1024 | 0.003 ms | 84.9 | 133.2 | 397.0 | 493.1 | none at snapshot |
| seg_hash 1024 | 0.004 ms | 97.5 | 176.2 | 464.1 | 444.8 | none at snapshot |
| im | ~0 ms | 112.4 | 258.5 | 945.8 | 912.7 | none at snapshot |

1M keys, 64-byte values, 16384 segments:

| Variant | Snapshot | GET ns/op | INCR ns/op | Held Replace ns/op | Held INCR ns/op | Held Clone Bytes |
|---|---:|---:|---:|---:|---:|---:|
| deep | 131.180 ms | 160.7 | 416.0 | 397.2 | 261.4 | none after snapshot |
| seg_hash 16384 | 0.112 ms | 300.6 | 674.1 | 4202.8 | 4331.8 | 13.98 MiB keys + 0.61 MiB payload |
| im | ~0 ms | 271.9 | 513.6 | 2104.6 | 2050.5 | 1.78 MiB keys + 0.61 MiB payload |

Current read:

- Generic HAMT still delivers the snapshot property, but it remains too costly
  for this packet as the default live keyspace. It improves 1M held-write clone
  bytes versus segmented COW, but it has slower snapshot iteration and visible
  live-operation overhead.
- `Arc` payloads help snapshot memory but still leave an O(N) key/index clone.
- Segmented COW is the best phase-one production step because it keeps
  hash-table-like lookup and makes capture root-clone sized. It is not free:
  first writes to shared segments during a held snapshot clone segment maps.
- Mutating large shared values copies the full payload in all shared-payload
  variants; splitting metadata from payload is prerequisite work, not polish.

### 5.6 Production packet evidence

Phase one landed the smallest production step justified by the model:
`RedisDb` now uses `KeyspaceMap`, while `KeyspaceSnapshot` remains the only
AOF/RDB consumer facade. This means the structural-sharing feature is active in
the live keyspace, so normal command throughput must be measured. AOF itself
remains off by default.

Final production artifacts on 2026-06-02:

- Rewrite-latency gate:
  `harness/bench/results/20260602T210203Z-1a9d679-aof-rewrite-latency.*`.
- Full profile matrix:
  `harness/bench/results/20260602T210228Z-1a9d679-profile-matrix.tsv`.
- Focused ordered hit-path probes:
  `harness/bench/results/20260602T210252Z-1a9d679-default-suite-parts.*` and
  `harness/bench/results/20260602T210259Z-1a9d679-default-suite-parts.*`.
- Correctness gates: `persistence-cycle --mode rdb` pass
  (`20260602T210155Z`), `persistence-cycle --mode aof` pass
  (`20260602T205929Z`), `persistence-cycle --mode aof-rewrite` pass
  (`20260602T205934Z`), and full persistence frontier 40/40 pass
  (`20260602T205938Z`).

Rewrite-start capture at required dataset sizes:

| Dataset | Snapshot Keys | Snapshot Capture | Command Wall | Post-Reply Rewrite | Restart |
|---:|---:|---:|---:|---:|---|
| 5k | 7942 | 55 us | 10.058 ms | 50.442 ms | passed |
| 25k | 27794 | 99 us | 9.114 ms | 69.123 ms | passed |
| 100k | 102916 | 97 us | 9.778 ms | 127.611 ms | passed |

Packet J's deep-snapshot baseline at 100k was 19319 us snapshot capture and
27.943 ms command wall. Phase one brings that row to 97 us and 9.778 ms,
respectively. The BASE writer still walks and serializes the snapshot in the
background, which is why post-reply rewrite wall remains proportional to data.

Throughput read:

- Full profile matrix summary: median 1.02x, min 0.76x, max 1.35x. The soft
  rows are p1 `GET` at 0.86x, p16 `PING_MBULK` at 0.76x, p16 `INCR` at 0.88x,
  and range-prep `LPUSH` at 0.89x. p100 `GET` is 1.17x and p100 `SET` is
  1.30x.
- Focused p1 ordered hit-path probe: `SET` 1.011x, `GET` 0.950x, `INCR`
  1.032x.
- Focused p16 ordered hit-path probe: `SET` 1.323x, `GET` 1.022x, `INCR`
  0.950x.
- Read this as bounded mixed telemetry, not a 99% throughput cliff. The
  segmented keyspace is active even when AOF is off, so these rows remain the
  main guardrail for future tuning.

---

## 6. Migration plan (phased, oracle-anchored, each rung independently shippable)

0. **Evidence package: done.** Keep this spec and
   `harness/models/keyspace-cow-model` in the repo so future keyspace work has a
   reproducible first-principles model instead of only prose.
1. **KeyspaceSnapshot facade: done.** AOF/RDB consumers use
   `KeyspaceSnapshot`, not ad hoc key/value vectors.
2. **Segmented-COW index phase one: done.** `RedisDb` uses `KeyspaceMap`, and
   `snapshot_all_dbs` captures shared segment roots on the owner thread.
3. **Keep materialization behind the facade.** Saver-side materialization can be
   replaced later without changing AOF/RDB call sites. The current packet
   deliberately preserves that boundary.
4. **Add completion/lifetime accounting for held snapshots.** Today segment COW
   naturally releases when the background snapshot drops. Future work should
   expose enough counters to report held-snapshot segment clone pressure.
5. **Fix saver memory shape.** The current RDB/AOF path still serializes from a
   snapshot into file output after capture. Streaming writer plumbing is
   separate from keyspace COW and should be measured separately.
6. **Split metadata from payload.** Move toward `Entry { lru, expire, payload:
   Arc<ObjectPayload> }` before introducing broad value sharing. This avoids
   cloning large values for LRU/TTL changes while a snapshot is live.
7. **Tune or replace segment routing only with evidence.** Segment count,
   routing hash, and possible prehashed-key storage are performance knobs.
   Current telemetry does not justify a bigger map rewrite inside this packet.
8. **Defer persistent inner encodings** for large collections (§4.4) until
   oracle/bench evidence shows the big-value COW spike matters.

---

## 7. Risks & open questions

- **Read-path regression remains the main guardrail** (§5.1/§5.6). Current
  telemetry clears the phase-one bar, but every future segment/value-sharing
  change needs the same GET/SET/INCR evidence.
- **Whole-object `Arc` is the wrong first step.** The current `RedisObject`
  layout mixes metadata and payload; split it first or metadata churn can clone
  large values.
- **Allocator pressure** from HAMT path-copying or segmented first-write clones
  can move latency even when average throughput looks acceptable.
- **Segment tuning is workload-sensitive.** More segments reduce held-snapshot
  clone bytes but add root/read overhead. The toy model and profile matrix are
  both required before changing `DEFAULT_KEYSPACE_SEGMENTS`.
- **Large-value COW spike** (§4.4) is the main place software COW is worse than
  fork until persistent inner encodings exist.
- **RDB streaming is a separate bottleneck.** Keyspace COW does not fix output
  buffering by itself.
- **Lifecycle is bigger than deleting `libc::fork`.** PID, reaper, metrics, and
  replication job state all need a thread/job-completion shape.
- **Scope.** Phase one changes the index but not value payload ownership. Full
  structural sharing is still core data-structure work touching `redis-core`
  db/object layers and eventually `redis-ds`.

## 8. Recommendation

Forkless/structurally shared snapshots are still the right direction, and the
phase-one segmented-COW implementation is justified by the evidence. Generic
HAMT as the default live keyspace is still not justified.

Move next in this order:

1. Merge this phase-one structural-sharing plus AOF-fault packet once review
   accepts the evidence.
2. Add held-snapshot clone counters so future tuning can report real segment
   COW pressure, not only benchmark side effects.
3. Split key metadata from value payload before introducing broad payload
   sharing.
4. Add background/startup cleanup for orphaned rewrite temp/history files after
   the synchronous successful-cleanup path remains stable.
5. Re-run the toy model and profile matrix for any future segment-count,
   routing-hash, prehashed-key, or HAMT experiment.

That path keeps the Valkey port honest: take the measured snapshot-start win,
keep hot-path throughput guarded, and reserve deeper persistent data-structure
work for evidence that demands it.
