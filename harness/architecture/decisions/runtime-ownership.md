# Runtime Ownership Decision

Status: refined by `runtime-owner-0-faithful-map`, 2026-05-21, after baseline
oracle + profile-matrix + smoke-suite hotspot evidence (commits
`81fd4ff`, `6da1962`, `15060d7`).

## Decision

The faithful event-loop runtime owner (option A in
`docs/RUNTIME_OWNERSHIP_PLAN.md`) is the production direction for valkey-rs.
Subsystem ownership is named explicitly below so translator packets cannot
quietly redraw the boundary.

The runtime owner will eventually own:

- accept loop and connection table
- per-client socket read/parse state and write buffers
- the selected-DB list (`Vec<RedisDb>`) currently behind `Arc<Mutex<_>>`
- ordinary reply flushing in a `beforeSleep`-style step
- timers, `serverCron`, and active-expire scheduling
- pub/sub delivery routing
- blocked-client wakeups (`blockedBeforeSleep`-equivalent)
- slowlog / latency / metrics increments on the hot path
- ordered AOF and replication propagation after each dispatch batch

The default product path does not migrate to the owner loop until:

1. the wire-diff oracle stays green on the surveyed corpus;
2. the runtime-owner canary corpus (`runtime-owner-1-canary-corpus`) is green;
3. profile-matrix and hotspot evidence confirms the lock-wait shape collapsed;
4. AOF, replication, RDB, scripting, blocking, and pub/sub behavior remains
   byte-compatible.

## Evidence Backing The Decision

Per `harness/evidence/runs/20260522T010206Z-15060d7-runner-runtime-owner-baseline-hotspots.json`
(commit `15060d7`, Apple M3 Max, smoke suite, 4s wall-clock sampling):

| workload  | lock samples            | rust_user samples | socket_read samples | idle_or_wait samples |
|-----------|-------------------------|-------------------|---------------------|----------------------|
| GET p100  | 22,512 (`__psynch_mutexwait`) | 118               | 986                 | 41,002               |
| INCR p100 | 18,728 (`__psynch_mutexwait`) | 227               | 803                 | 38,020               |

Lock-wait time outweighs in-process Rust user time by ~190x on GET and ~82x on
INCR. The profile-matrix median ratio at commit `6da1962` was 0.69x with the
minimum at 0.60x on pipeline depth p1/INCR and p100/INCR — and yet
`redis_commands::dispatch::lookup_runtime_command` is at 40 samples on GET.
That means the remaining gap is not dispatch-table cost, parsing cost, or
allocator cost. It is the `Arc<Mutex<RedisDb>>` taken on every command on every
per-client thread.

This is the architectural floor. No additional micro-optimization on the
existing thread-per-client path will close the lock-wait stack. The next
correctness-preserving step is to move ownership of the DB list off the
mutex and into a runtime owner.

## Subsystem Ownership Boundary

Authoritative for translator packets. New code that contradicts this table
must escalate back to the architect packet, not silently widen.

| subsystem        | upstream Valkey reference                                              | current Rust owner                                       | target owner (post-migration)                             |
|------------------|-------------------------------------------------------------------------|----------------------------------------------------------|-----------------------------------------------------------|
| accept loop      | `ae.c` aeMain + `networking.c` acceptCommonHandler                      | `TcpListener::incoming` in `crates/redis-server/src/main.rs` | `RuntimeOwner::accept_step`                              |
| socket read      | `networking.c:4284` readQueryFromClient                                 | per-thread blocking `Client::read_from_socket`           | `RuntimeOwner::poll_readable` -> `ClientSlot::ingest`     |
| RESP parse       | `networking.c:4146` processInputBuffer                                  | `redis_protocol::request::parse_*` (pure, per thread)    | unchanged (pure parser, called by owner)                  |
| command dispatch | `server.c:4303` processCommand                                          | `redis_commands::dispatch::run_command` against `CommandContext { &mut Client, &mut RedisDb, .. }` | `RuntimeOwner::dispatch(slot_id, parsed)` with `&mut RuntimeOwner` |
| db storage       | `db.c` selectDb / dictFind                                              | `Arc<Mutex<RedisDb>>` per index in `redis-core/src/databases.rs` | `Vec<RedisDb>` owned by `RuntimeOwner`                    |
| socket write     | `networking.c:3059` writeToClient + `:3271` handleClientsWithPendingWrites | direct `Client::reply_buf` flush per thread             | `RuntimeOwner::flush_pending_writes` (beforeSleep step)   |
| serverCron       | `server.c` serverCron                                                   | background thread(s) acquiring DB locks                  | `RuntimeOwner::cron_step`                                 |
| active expire    | `expire.c` activeExpireCycle                                            | background thread + DB lock                              | `RuntimeOwner::active_expire_step`                        |
| pub/sub          | `pubsub.c` pubsubPublishMessage                                         | `PubSubRegistry` global behind locks (`pubsub_registry.rs`) | `RuntimeEvent::Publish { channel, payload }` -> owner   |
| blocked clients  | `blocked.c` blockedBeforeSleep / unblockClient                          | global blocked-keys index + writer thread                | `RuntimeEvent::WakeBlocked { slot_id, reason }` -> owner  |
| AOF              | `aof.c` flushAppendOnlyFile (called from beforeSleep)                   | background thread, lock-protected                        | `RuntimeOwner::flush_aof_step` after dispatch batch       |
| replication      | `replication.c` replicationFeedReplicas                                 | `replica_dialer` invoked under DB lock                   | `RuntimeOwner::propagate_step` (ordered post-dispatch)    |
| RDB              | `rdb.c` rdbSaveBackground                                               | background helper + DB lock during fork prep             | unchanged storage path; owner gates entry/exit            |
| TLS              | `connection.c` connTypeProcessPendingData                               | per-thread TLS connection (existing path)                | UNCHANGED in this milestone (see TODO(human) below)       |

## Non-Goals (binding)

- No special fast path for `PING`, `GET`, `SET`, or `INCR` that bypasses
  normal command dispatch, ACL, slowlog, AOF, replication, maxmemory, scripts,
  or transaction semantics. (Already encoded; restated with evidence: even at
  0.56x median ratio, fast-pathing four commands is the wrong fix — the lock
  wait is what dominates.)
- No sharded DB ownership in this milestone. `MULTI`/`WATCH`/`EXEC`, `SELECT`,
  Lua scripts, blocking lists, and replication ordering all assume
  single-threaded command effects across the selected DB.
- No disabling of TLS, ACL, scripting, transactions, pub/sub, blocking
  commands, expiration, AOF, replication, or RDB for a benchmark.
- No public speed-parity claim. Performance numbers from this run and the
  follow-up scaffold packets are **alpha telemetry**, not a product claim.
  The docs benchmark table is only updated when the normal product path
  produced the number.

## Decisions Locked By This Architect Packet

These were listed as "decisions before dispatch" in
`docs/RUNTIME_OWNERSHIP_PLAN.md` §5. They are now answered for the
unattended run:

1. **Sharding scope** — explicitly out of scope for the runtime-owner
   milestone. Reopen only with a separate architect packet after the
   owner loop is the default product path.
2. **Public claim** — alpha telemetry. The capability
   `faithful-runtime-ownership` is described as "moves toward a Valkey-like
   runtime owner while preserving drop-in command semantics". It is not a
   speed-parity claim and must not be marketed as one.
3. **Background events channel shape** — single `RuntimeEvent` enum, single
   channel into the owner. Per-subsystem channels are rejected because
   AOF/replication ordering and pub/sub-vs-blocked-keys interleaving require
   one totally-ordered event stream. Variants enumerated in
   `harness/architecture/object-vocabulary.tsv`.
4. **Inert scaffold rule** — `runtime-owner-2-scaffold-types` must add types
   and tests only. It must not add a poller crate, must not flip the default
   product path, and must not change `main.rs` beyond a module declaration
   plus clearly disabled wiring.
5. **TLS migration scope** — out of scope for this milestone. TLS stays on
   the existing per-thread path until a separate `runtime-owner-tls-migration`
   architect packet exists. The scaffold may not assume TLS lives in the
   owner.

## TODO(human): Decisions Blocking The Real Owner-Loop Migration

These are deliberately NOT decided by this architect packet. They are dependency
or policy choices that require human review. The runtime-owner-3 (and later)
implementation packets are blocked on them and must not be dispatched until each
is answered in this section.

### Overnight owner-loop experiment decision, 2026-05-22

The operator approved an overnight attempt to move performance toward parity.
For that run only, `docs/RUNTIME_OWNER_OVERNIGHT_ARCHITECTURE.md` answers the
poller question by deliberately **not** adding `mio` yet. The first owner-loop
implementation uses standard-library nonblocking plain-TCP sockets and a
single owner loop. TLS remains on the existing thread-per-client path, sharding
remains out of scope, I/O threads remain out of scope, and no benchmark-only
command bypass is allowed.

This does not supersede the production recommendation that a real poller such
as `mio` is the likely long-term shape. It creates a lower-blast-radius
evidence step: prove or disprove that removing the thread-per-client/mutex
runtime shape moves the current p100 benchmark wall while preserving
wire-smoke.

- **TODO(human): poller dependency.** Options:
  (a) `mio` (cross-platform, std-shaped readiness API, mature),
  (b) `polling` crate (smaller surface),
  (c) `tokio` (forces async-everywhere; conflicts with `&mut RuntimeOwner` model),
  (d) raw `epoll`/`kqueue` via `libc` (no new dep, more unsafe code).
  Recommendation pending: `mio`. Adds one workspace dep, keeps the
  `&mut RuntimeOwner` synchronous command model intact, mirrors what
  upstream Valkey expresses with `ae_kqueue.c`/`ae_epoll.c`. Requires
  architect approval to add to `crates/redis-server/Cargo.toml`.
- **TODO(human): TLS migration sequencing.** When does TLS join the owner
  loop? Options: (a) never in this port (keep dual path indefinitely),
  (b) one follow-up packet after the owner loop is default,
  (c) before the owner loop becomes default. Choice gates whether the
  `connection` abstraction in `redis-core/src/connection.rs` is allowed to
  change in the owner-loop packet family.
- **TODO(human): I/O thread parity.** Upstream Valkey has optional I/O
  threads (`server.c` `trySendPollJobToIOThreads`). Do we (a) skip them
  entirely, (b) build a single-thread owner first and add I/O threads as a
  later packet, or (c) match upstream's threaded readiness model? Default
  recommendation: (b).
- **TODO(human): soak runner.** `harness/bench/run-soak.sh` does not yet
  exist. Before any public performance claim, an explicit `runtime-owner-soak`
  runner must land. Not in this unattended run.

Until these are answered, the only runtime-owner work the harness will
dispatch is: this architect map (now), `runtime-owner-1-canary-corpus`,
`runtime-owner-2-scaffold-types` (inert), and their oracle/bench post-runs.

## Required Gates (every implementation packet in this family)

- `bash harness/oracle/smoke.sh --skip-build` green
- `cargo check --workspace` green
- `cargo test -p redis-core -p redis-protocol -p redis-server` green
- runtime-owner canary corpus green once it lands
- profile-matrix evidence updated after every packet
- no RDB bidirectional oracle regression if persistence code is touched
- no benchmark-specific command bypass introduced anywhere

## Stop Conditions

Quarantine the chain if any of these happen:

- a packet special-cases benchmark commands outside normal dispatch
- smoke oracle regresses and is not restored in the same packet
- a second live DB model is introduced without a migration plan
- pub/sub, blocking wakeups, or replication get "temporarily disabled" in the
  default product path
- benchmark rows improve but conformance evidence is missing
- the scaffold packet changes the default product path before the canary
  oracle is green

## Cross-Artifact Synchronization (locked 2026-05-22)

These rules align the typed artifacts under this packet's authority. A future
architect packet may relax them, but a translator/runner/fixer packet may not.

1. **Runner capability manifest is the single source of truth for "what
   evidence this runner produces."** `harness/runners.toml` wire-smoke runner
   declares `wire-compatibility`, `runtime-owner-canaries`, and
   `runtime-owner-scaffold`. Any packet using `wire-smoke` may only declare
   capabilities that are a subset of that list.
2. **Object vocabulary is the single source of truth for scaffold types.**
   `harness/architecture/object-vocabulary.tsv` rows owned by
   `crates/redis-server/src/runtime_owner.rs` define the entire allowed
   surface of `runtime-owner-2-scaffold-types`. `work-packets.jsonl` does
   not re-enumerate them. Adding a new scaffold type requires this architect
   doc to be updated first.
3. **`SlotId` is a newtype**, not a bare integer. The dispatch signature
   `RuntimeOwner::dispatch(slot_id, parsed)` and the variants
   `RuntimeEvent::WakeBlocked { slot_id, .. }` and
   `OwnerCommandResult::Closed { slot_id }` all share that type; using `u32`
   directly would let `db_index` or `replica_id` cross paths at the type
   level.
4. **Completion non-goals mirror the locked decisions.**
   `harness/completion.toml` `[project].non_goals` includes TLS owner-loop
   migration, adding a poller workspace dep, public speed-parity claim from
   non-default mode, and authoring a soak runner before its script and human
   thresholds exist. Removing any of these requires a follow-up architect
   packet.
5. **Planned, human-blocked runners do not become real runners by
   accident.** `harness/runners.toml [[planned_runner]]` is a placeholder
   shape. Promoting a row to `[[runner]]` requires the script to exist and
   the matching TODO(human) item in this doc to be answered.
