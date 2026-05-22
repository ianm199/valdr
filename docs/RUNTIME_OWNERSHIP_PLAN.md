# Runtime Ownership Plan

Status: refined by `runtime-owner-0-faithful-map` on 2026-05-21 after the
baseline oracle (`81fd4ff`), profile-matrix (`6da1962`), and hotspot
(`15060d7`) runs. The binding decisions live in
`harness/architecture/decisions/runtime-ownership.md` and
`harness/architecture/lifecycle-map.toml`; this doc is the prose explanation
behind them.

## Refinement 2026-05-21

The hotspot run finally tells us where the wall is. On smoke-suite p100 GET,
`__psynch_mutexwait` accounted for 22,512 samples vs 118 samples for all
Rust user code combined; INCR was 18,728 vs 227. Lookup, parsing, allocator,
and dispatch-table cost are no longer measurable next to the DB mutex. The
production direction is the faithful event-loop owner (option A below). The
following section-5 questions are now answered for the unattended run; the
rest are explicit `TODO(human)` and block the real owner-loop migration:

- sharding: out of scope for this milestone
- public claim: alpha telemetry, not speed parity
- background events channel: single ordered `RuntimeEvent` enum into the owner
- TLS migration sequencing: out of scope for this milestone
- inert scaffold rule: no new dep, no product-path change, no concrete poller
- poller dependency: **TODO(human)** — `mio` recommended, not yet approved
- I/O thread parity: **TODO(human)** — default recommendation is "later packet"
- soak runner: **TODO(human)** — `harness/bench/run-soak.sh` does not exist yet

The subsystem ownership boundary (accept, socket-read, parse, dispatch, db,
socket-write, cron, active-expire, pub/sub, blocked, AOF, replication, RDB,
TLS) is enumerated with upstream Valkey C references in
`harness/architecture/lifecycle-map.toml [[subsystem]]` and in the decision
doc's "Subsystem Ownership Boundary" table. Translator packets must work to
those tables; widening the boundary requires re-entering the architect
packet.

The original architecture write-up below is preserved for context.

## Refinement 2026-05-22 (re-attempt)

The first refinement landed the architect decisions but left two declared
targets — `harness/runners.toml` and `harness/completion.toml` — out of sync
with the locked decisions. This re-attempt closes that gap:

- `harness/runners.toml` `wire-smoke` runner now declares
  `runtime-owner-canaries` and `runtime-owner-scaffold` so the
  `runtime-owner-post-canary-oracle` and `runtime-owner-post-scaffold-oracle`
  packets actually produce capability evidence against the runner manifest.
- `harness/runners.toml` adds a `[[planned_runner]]` row for
  `runtime-owner-soak` that points back at the TODO(human) gate. Promoting
  it to a real `[[runner]]` requires the missing script and human-chosen
  scope.
- `harness/completion.toml` `[project].non_goals` now mirrors the locked
  TLS, poller-crate, public-claim, and soak-runner-authoring decisions.
  `binding_architecture_artifacts` and `decision_evidence_root` give the
  completion contract a structured pointer at the binding architect files
  and the hotspot evidence that drove them.
- `harness/architecture/object-vocabulary.tsv` adds `SlotId` as a newtype.
  The previous map referenced it in the dispatch signature without
  declaring it.
- `harness/work-packets.jsonl` `runtime-owner-2-scaffold-types` note no
  longer re-enumerates the allowed types; the object vocabulary is the
  single source of truth. (Two-source-of-truth violation removed.)
- `harness/architecture/lifecycle-map.toml` gains an `[invariants]` table
  encoding the five cross-artifact rules so a future translator/runner/fixer
  packet cannot quietly relax them.
- `harness/architecture/decisions/runtime-ownership.md` gains a "Cross-Artifact
  Synchronization (locked 2026-05-22)" section that records the same five
  rules in prose, with a single-architect escalation requirement to change
  them.

These are consistency edits. They do not change the production direction,
do not add or remove a packet, and do not relax any TODO(human) gate.

---

Status: architecture decision after the first Redis performance loop, 2026-05-21.

This doc captures the "option 5" performance question: whether to move beyond
local hot-path patches and change who owns sockets, clients, and databases.

## Decision

Do not implement the runtime-ownership rewrite as a same-day Redis patch.

The no-regret optimizations already landed:

- batch replies per socket read;
- drain the query buffer once per read batch;
- direct-write ordinary request/reply traffic;
- batch client-info snapshots, reuse argv storage, use monotonic timing, and
  hold the DB0 lock across safe read batches;
- cache generated command metadata in dispatch and avoid argv snapshots unless
  slowlog, AOF, or replication need them;
- skip standalone write-propagation work when AOF is off, no replicas are
  connected, and no replication backlog is active;
- fold handler and metadata lookup into one runtime dispatch table;
- bucket runtime dispatch lookup by the command's first ASCII byte.

Those moved deep-pipeline GET from roughly 221k req/s to roughly 2.17M req/s,
and moved deep-pipeline SET from roughly 190k req/s in the alpha baseline to
roughly 1.67M req/s.
That is a real improvement, and it also exposes the remaining architecture
gap: valkey-rs still has blocking per-client threads sharing
`Arc<Mutex<RedisDb>>`, while upstream Valkey drains many sockets and executes
commands from a tight event loop.

The next step is not another small hot-path edit. It is a runtime ownership
rewrite. Doing that casually would either fork the semantics or turn the
benchmark into a lie.

## Current Runtime Shape

```text
TcpListener::incoming()
  accept one socket
  spawn read/dispatch thread
    Client owns query_buf, argv, reply_buf, selected db, pub/sub state
    parse one read batch
    lock Arc<Mutex<RedisDb>>
    build CommandContext { &mut Client, &mut RedisDb, Arc<RedisServer>, pubsub }
    dispatch command
    write ordinary replies directly
  spawn writer thread
    used by pub/sub, blocked-list wakeups, replica pushes

background threads:
  active expire
  blocked-key timeout
  replication/AOF/BGSAVE helpers
  client-info, pub/sub, ACL, slowlog globals behind Arc<Mutex<_>>
```

This shape was good for compatibility bring-up. It lets agents port commands
without understanding an event loop. It is not the production Valkey shape.

The main blocking points are visible in the code:

- `redis-core/src/databases.rs` stores each DB as `Arc<Mutex<RedisDb>>`.
- `redis-core/src/command_context.rs` gives every command a mutable client and
  mutable DB.
- `redis-server/src/main.rs` accepts a socket, spawns a client thread, spawns a
  writer thread, and locks the DB around dispatch.
- pub/sub, blocked keys, replication, and client metadata assume cross-thread
  communication through registries and channels.

## Why Not Patch It Quickly

### A command fast path is dishonest

We could special-case `PING`, `GET`, `SET`, and `INCR` in the server loop and
bypass `dispatch`, ACL checks, slowlog, AOF, replication, maxmemory, scripts,
and transaction semantics. The benchmark would improve. The port would be
worse.

That kind of patch is exactly what the harness should prevent: it optimizes a
scoreboard by stepping around the compatibility surface.

### `RwLock` is not the real fix

Changing `Arc<Mutex<RedisDb>>` to `Arc<RwLock<RedisDb>>` sounds attractive for
GET-heavy workloads, but command execution today is typed as `&mut RedisDb`.
Read-only command dispatch would need a real read-only command context,
generated command metadata wired into the dispatcher, and careful handling for
commands that look read-only but expire keys, touch LRU/LFU metadata, wake
blocked clients, or update client/server statistics.

That can be a useful subproject, but it is not the runtime ownership rewrite.

### Sharding is not automatically Redis-compatible

Key-range sharding removes one global DB lock, but Redis semantics make it
expensive:

- multi-key commands can cross shards;
- `MULTI` / `WATCH` / `EXEC` want atomic behavior across selected keys;
- `SELECT` changes per-client DB, not a shard;
- blocking list commands park clients and wake them from write paths;
- Lua scripts and replication want ordered, single-threaded command effects.

Sharding is a product decision, not a translation cleanup.

## Viable Designs

### A. Faithful event-loop runtime

One runtime thread owns normal clients and the selected DBs. Sockets are
nonblocking. The loop polls readiness, drains request bytes, dispatches command
batches, and flushes replies. Background systems send events into the owner
loop rather than taking DB locks directly.

```text
poller/kqueue/epoll/mio
  ready client sockets
  timer events
  background events
        |
        v
RuntimeOwner
  Vec<Client>
  Vec<RedisDb>
  PubSubRegistry
  BlockedKeysIndex
  Slowlog/metrics
        |
        v
CommandContext { &mut RuntimeOwner, client_id }
```

Pros:

- closest to C Valkey's `ae.c` model;
- removes the DB mutex from the hot path;
- makes pipelined tiny commands much more competitive;
- gives one coherent place for timers, active expire, blocked wakeups, pub/sub,
  and replication events.

Cons:

- forces a real `CommandContext` redesign;
- background helpers must become event senders, not direct DB mutators;
- TLS, pub/sub, blocking commands, replication, and persistence must be
  rechecked under the new owner model;
- this is a milestone, not a hotfix.

Recommendation: this is the production direction if valkey-rs keeps going.

### B. Shard-owned workers

Network threads parse requests and route command batches to shard workers. Each
worker owns a DB partition. Replies flow back to the client writer.

Pros:

- can scale beyond upstream's single command thread for independent keys;
- maps to modern multicore cache-server designs.

Cons:

- Redis compatibility gets hard around transactions, scripts, multi-key ops,
  blocking commands, and replication ordering;
- requires a command-effect protocol instead of direct `&mut Client` mutation;
- not a faithful port of upstream Valkey.

Recommendation: not the first production rewrite. Revisit after a faithful
owner loop exists and after the compatibility envelope is stable.

### C. Tokio with shared DB locks

Move socket handling to async tasks but keep `Arc<Mutex<RedisDb>>`.

Pros:

- reduces thread count;
- can handle many idle clients well;
- ecosystem support is strong.

Cons:

- does not remove the command-path DB lock;
- async locks around CPU-bound command execution can worsen tail latency;
- adds a runtime dependency without solving the benchmark cliff.

Recommendation: useful for connection scalability, not the core #5 fix.

### D. Benchmark-only owned-DB mode

Add an environment flag that runs only `PING` / `GET` / `SET` / `INCR` through a
small single-thread owner loop.

Pros:

- quickly estimates the event-loop ceiling.

Cons:

- creates a second server with a smaller semantic surface;
- risks publishing numbers from a mode that is not the product;
- teaches agents that benchmark-specific shortcuts are acceptable.

Recommendation: reject for public numbers. A private scratch experiment is fine,
but it should not land as the default benchmark path.

## How To Reopen This With The Harness

If we choose to spend on #5 later, reopen it as an architect-led packet family,
not as one broad "make Redis faster" prompt.

### 1. Add architecture-stage artifacts

Create these files first:

```text
harness/architecture/decisions/runtime-ownership.md
harness/architecture/lifecycle-map.toml
harness/architecture/object-vocabulary.tsv
```

Minimum `runtime-ownership.md` contents:

```text
Decision:
- Normal command execution is owned by RuntimeOwner.
- RuntimeOwner owns ClientTable, RedisDb list, PubSubRegistry,
  BlockedKeysIndex, timers, slowlog, and ordinary reply flushing.
- Background helpers send RuntimeEvent messages into RuntimeOwner.

Non-goals:
- No benchmark-only GET/SET/PING/INCR bypass.
- No sharded command execution in this milestone.
- TLS may stay on the old path until runtime-owner-5 unless explicitly moved.

Gates:
- smoke oracle 21/21
- RDB bidirectional oracle unchanged
- official surveyed TCL files unchanged
- profile matrix updated after every packet
```

Minimum object vocabulary:

```tsv
name	kind	owner	notes
RuntimeOwner	struct	crates/redis-server/src/runtime.rs	Owns normal command execution.
ClientTable	struct	crates/redis-server/src/runtime.rs	Owns live client slots and socket state.
RuntimeEvent	enum	crates/redis-server/src/runtime.rs	Background-to-owner event channel.
CommandContext	struct	crates/redis-core/src/command_context.rs	Will point at owner/client id, not raw shared lock.
RedisDb	struct	crates/redis-core/src/db.rs	Moves from Arc<Mutex<_>> hot path to owner-held state.
```

### 2. Add explicit packet rows

The packet graph should be materialized in `harness/work-packets.jsonl`.
Sketch:

```json
{"schema_version":1,"id":"runtime-owner-0-design","phase":6,"role":"architect","selector":"manual","targets":["harness/architecture/decisions/runtime-ownership.md","harness/work-packets.jsonl","harness/runners.toml"],"capabilities":["runtime-owner"],"resources":["runtime-architecture"],"exclusive":true,"cost_hint":"medium"}
{"schema_version":1,"id":"runtime-owner-1-client-table","phase":6,"role":"translator","selector":"manual","depends_on":["runtime-owner-0-design"],"targets":["crates/redis-server/src/runtime.rs","crates/redis-server/src/main.rs"],"capabilities":["runtime-owner"],"resources":["runtime-owner"],"cost_hint":"large"}
{"schema_version":1,"id":"runtime-owner-2-nonblocking-poller","phase":6,"role":"translator","selector":"manual","depends_on":["runtime-owner-1-client-table"],"targets":["crates/redis-server/src/runtime.rs","crates/redis-server/Cargo.toml","Cargo.lock"],"capabilities":["runtime-owner"],"resources":["runtime-owner","dependency-policy"],"cost_hint":"large"}
{"schema_version":1,"id":"runtime-owner-3-command-context","phase":6,"role":"translator","selector":"manual","depends_on":["runtime-owner-2-nonblocking-poller"],"targets":["crates/redis-core/src/command_context.rs","crates/redis-commands/src/dispatch.rs","crates/redis-server/src/runtime.rs"],"capabilities":["runtime-owner"],"resources":["command-context","runtime-owner"],"cost_hint":"large"}
{"schema_version":1,"id":"runtime-owner-4-background-events","phase":6,"role":"translator","selector":"manual","depends_on":["runtime-owner-3-command-context"],"targets":["crates/redis-server/src/runtime.rs","crates/redis-core/src/expire.rs","crates/redis-core/src/blocked_keys.rs","crates/redis-commands/src/replica_dialer.rs"],"capabilities":["runtime-owner"],"resources":["runtime-owner","background-events"],"cost_hint":"large"}
{"schema_version":1,"id":"runtime-owner-5-pubsub-blocking-replication","phase":6,"role":"translator","selector":"manual","depends_on":["runtime-owner-4-background-events"],"targets":["crates/redis-server/src/runtime.rs","crates/redis-commands/src/pubsub.rs","crates/redis-commands/src/list.rs","crates/redis-core/src/replication.rs"],"capabilities":["runtime-owner"],"resources":["runtime-owner","pubsub","blocking","replication"],"cost_hint":"large"}
{"schema_version":1,"id":"runtime-owner-6-bench-and-soak","phase":6,"role":"runner","selector":"nightly","depends_on":["runtime-owner-5-pubsub-blocking-replication"],"runner":"profile-matrix-and-soak","capabilities":["runtime-owner"],"cost_hint":"small"}
```

The scheduler should treat these as mostly serial because they all lock the
same `runtime-owner` resource. Parallelism can still happen around runners,
docs, or independent command metadata cleanup.

### 3. Add runner gates

Add or extend `harness/runners.toml` with:

```toml
[[runner]]
id = "runtime-owner-smoke"
kind = "ExternalSuite"
surface = "correctness"
method = "wire-oracle"
command = ["bash", "harness/oracle/smoke.sh", "--skip-build"]

[[runner]]
id = "runtime-owner-profile-matrix"
kind = "ExternalSuite"
surface = "performance"
method = "bench-load"
command = ["bash", "harness/bench/run-profile-matrix.sh"]

[[runner]]
id = "runtime-owner-soak-30m"
kind = "ExternalSuite"
surface = "robustness"
method = "soak"
command = ["bash", "harness/bench/run-soak.sh", "--duration", "1800"]
```

If `run-soak.sh` does not exist yet, create that as its own runner packet
before claiming production performance.

### 4. Packet acceptance criteria

Every implementation packet must pass:

- `cargo test -p redis-core -p redis-protocol -p redis-server`;
- `bash harness/oracle/smoke.sh --skip-build`;
- no RDB bidirectional oracle regression if persistence code is touched;
- no command-specific benchmark bypass;
- no public benchmark table update unless the normal product path produced
  the number.

The performance runner should append a profile-matrix evidence blob after each
packet. The docs table should be updated only after the correctness gates pass.

### 5. Human decisions before dispatch

The first architect packet should force these decisions into
`runtime-ownership.md`:

1. Poller dependency: use `mio`, raw platform polling, or another runtime?
2. TLS migration: move TLS into the owner loop now, or keep the old TLS path
   temporarily and mark it as a separate non-optimized path?
3. Background events: use a single `RuntimeEvent` channel, per-subsystem
   channels, or direct timer integration?
4. Sharding: explicitly out of scope for this milestone?
5. Public claim: are performance numbers alpha telemetry or a speed-parity
   claim?

Do not dispatch translators until these are answered.

### 6. Stop conditions

Abort or quarantine the chain if any of these happen:

- a packet special-cases benchmark commands outside normal dispatch;
- smoke oracle regresses and cannot be restored in the same packet;
- the implementation introduces a second live DB model without a migration
  plan;
- pub/sub, blocking wakeups, or replication become "temporarily disabled" in
  the default product path;
- benchmark rows improve but conformance evidence is missing.

The packet should not be "make Redis faster." It should be "move command
execution ownership to one runtime owner while preserving the command surface."

## What This Means For nginx

The Redis experiment is directly useful for nginx, but mostly as a warning.

For nginx, runtime ownership is not a late performance cleanup. It is the
architecture:

- which event loop owns sockets;
- which worker owns request state;
- where timers live;
- how sendfile, TLS, keepalive, upstream proxying, and graceful shutdown feed
  events back into the loop;
- which global/shared structures are intentionally shared vs owned.

The nginx port should not begin with a blocking thread-per-connection skeleton
and then try to optimize backward. It should start with an explicit runtime
owner model, then generate packets around that model:

```text
runtime owner
  -> accept/listen sockets
  -> connection table
  -> request parser state
  -> timer wheel
  -> file/sendfile path
  -> upstream/proxy path
  -> graceful reload/shutdown path
```

Benchmarks should be present from the first useful server loop, not after
conformance is already green. The lesson from valkey-rs is that conformance can
look excellent while the runtime shape still caps throughput.

## Stop Condition For This Redis Loop

Call the Redis performance loop complete for now.

What we learned:

- the harness can track conformance and performance in the same repo;
- performance packets work when they name a subsystem boundary;
- small no-regret patches can produce large wins;
- the remaining gap is architectural, not incidental Rust overhead;
- the nginx run should make runtime ownership a first-class architect packet
  before translator agents start filling in large surfaces.

The honest next Redis milestone is not another micro-iteration. It is an
explicit runtime-owner project with its own packet graph and budget.
