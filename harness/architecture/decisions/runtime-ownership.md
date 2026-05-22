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

The full production owner loop with an owned DB list does not become the final
runtime shape until:

1. the wire-diff oracle stays green on the surveyed corpus;
2. the runtime-owner canary corpus (`runtime-owner-1-canary-corpus`) is green;
3. profile-matrix and hotspot evidence confirms the lock-wait shape collapsed;
4. AOF, replication, RDB, scripting, blocking, and pub/sub behavior remains
   byte-compatible.

The narrower `runtime-owner-4-std-nonblocking-owner-loop` experiment may replace
the default plain-TCP path earlier, but only under the std experiment contract
below and only as alpha telemetry until its post-owner-loop oracle passes.

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

## Std Nonblocking Owner-Loop Experiment (locked 2026-05-22)

`runtime-owner-4-std-nonblocking-owner-loop` is safe to dispatch after
`runtime-owner-3-overnight-architecture`. It is not blocked by the long-term
poller dependency question because this experiment deliberately uses only
standard-library nonblocking sockets and a linear scan over live plain-TCP
clients.

This packet is an ownership-transition experiment, not the final production
event-loop port. Its implementation contract is:

1. **Plain TCP only.** The normal TCP listener may move to a single owner loop.
   TLS remains on the existing thread-per-client path and must keep using the
   same command dispatch behavior.
2. **No new readiness dependency.** Do not add `mio`, `polling`, `tokio`, raw
   `epoll`/`kqueue`, or a new unsafe poller surface in this packet.
3. **Existing dispatch path.** Every command still enters
   `redis_commands::dispatch` through `CommandContext::with_server`. No
   `PING`/`GET`/`SET`/`INCR` fast path is allowed.
4. **Transitional DB model.** Use the existing `global_databases()`
   `Arc<Mutex<RedisDb>>` handles as the live database source during this
   packet. The owner loop is the sole plain-TCP command executor, so it may
   hold the selected DB guard across a bounded parse/dispatch batch, but it
   must not create a second live `Vec<RedisDb>` that diverges from TLS,
   active-expire, AOF replay, replication, or RDB helpers. Moving the live DB
   list fully into `RuntimeOwner` remains a later architect/translator packet.
5. **Socket and client ownership.** Accepted plain-TCP `TcpStream`s,
   `Client` state, query buffers, parsed argv staging, and ordinary reply
   flushing are owned by `RuntimeOwner`/`ClientSlot`, not by per-client read or
   writer threads.
6. **Foreign payload delivery.** Pub/sub, blocked wakeups, WAIT/replication
   replies, and other out-of-band payloads must enter the owner loop through a
   per-slot `mpsc::Sender<Vec<u8>>` registered in the existing
   `PubSubRegistry`/blocked-key machinery. The owner loop drains the matching
   receivers and appends bytes to the slot write buffer; no foreign thread may
   write a plain-TCP owner-loop socket directly.
7. **Loop shape.** Each tick accepts until `WouldBlock`, drains foreign
   payload channels, reads ready client sockets until `WouldBlock`, parses with
   `parse_inline_or_multibulk_into`, dispatches completed commands, flushes
   pending writes, and cleans up closed clients. If no progress was made, the
   loop must sleep or yield briefly; a busy spin is not acceptable.
8. **Lifecycle hooks stay live.** `maxclients`, client-info registry,
   connected-client metrics, selected DB state, `RESET`, `QUIT`, pub/sub
   cleanup, replica cleanup, and blocked-key cleanup must still run on the
   owner-loop path.

The post-owner-loop oracle is the gate. Benchmark evidence after this packet is
only meaningful if `runtime-owner-4-post-owner-loop-oracle` passes.

## Mio Readiness Poller Step (locked 2026-05-22)

`runtime-owner-7-poller-architecture` approves
`runtime-owner-8-mio-poller-owner-loop` as the next automatic implementation
packet.

Evidence:

- The current wire-smoke oracle passed at
  `harness/evidence/runs/20260522T135522Z-d884855-runner-runtime-owner-6-current-oracle.json`,
  including `21-runtime-owner-canaries`.
- The current profile matrix at
  `harness/evidence/runs/20260522T135524Z-5f159bc-runner-runtime-owner-6-current-profile-matrix.json`
  shows the p1 path is no longer the main concern, while core-p100 remains
  below parity: GET 0.78x, SET 0.79x, INCR 0.63x, PING 0.76x.
- The current hotspot run at
  `harness/evidence/runs/20260522T135537Z-1edcb59-runner-runtime-owner-6-current-hotspots.json`
  reports median 0.75x, min 0.59x, max 0.86x.
- The current call-tree run at
  `harness/evidence/runs/20260522T140909Z-0022bc7-runner-runtime-owner-6-current-calltree.json`
  reports median 0.74x, min 0.56x, max 0.84x and stores raw call-tree
  artifacts under `harness/bench/profiles/20260522T140914Z-0022bc7-calltree/`.
  Those artifacts show `RuntimeOwner::run_plain_tcp` spending visible samples
  in repeated `accept`, `yield_now`, socket read/write, parser, dispatch
  lookup, hashing, and allocation work.

Decision:

1. **Poller dependency for this packet: `mio`.** Add `mio` as a workspace
   dependency with `os-poll` and `net` features. `tokio`, `polling`, and raw
   `epoll`/`kqueue` remain unapproved for this milestone.
2. **No unsafe budget increase.** The poller packet must use `mio`'s safe
   readiness API. Raw platform poller code is out of scope.
3. **Plain TCP only.** TLS stays on the existing thread-per-connection path.
4. **Command path unchanged.** Every command still goes through
   `parse_inline_or_multibulk_into`, `CommandContext::with_server`, and
   `redis_commands::dispatch`.
5. **Readiness model.** The owner loop registers the listener and each
   plain-TCP slot with stable tokens. Listener readiness accepts until
   `WouldBlock`; client readability drains socket reads; writability is
   registered only while a slot has pending writes and removed when empty.
6. **Dispatch continuation.** If a slot reaches the per-tick command cap while
   complete commands remain buffered, the owner loop must reschedule that slot;
   it must not wait for another socket readiness edge.
7. **Foreign payloads.** Pub/sub, blocked wakeups, WAIT/replication replies,
   and similar payloads still enter via the existing per-slot mpsc receivers.
   The owner drains those receivers after poll returns and on a short bounded
   timeout, queues bytes into the owner write buffer, and owns socket writes.
   Foreign threads still may not write owner-loop sockets directly.
8. **DB storage unchanged.** Continue using `global_databases()` handles as
   transitional storage. An owner-owned live `Vec<RedisDb>` remains a later
   architect packet.
9. **Compatibility gate.** `runtime-owner-8-post-poller-oracle` must pass
   before any post-poller benchmark evidence is trusted.

## Post-Poller Hotpath Wave (locked 2026-05-22)

`runtime-owner-8` completed cleanly enough to make the next performance wall
local and measurable rather than structural. The owner-owned DB migration is
still the larger manual architecture decision, but it is not the first next
step. The current macOS call-tree artifacts show three smaller faithful-port
cost centers that can be tested directly with the existing harness runners:

1. **Command timing / slowlog predicate overhead.** `dispatch_command_name()`
   currently pays `Instant::now()` plus `elapsed()` on every command before
   deciding whether slowlog should record anything. Upstream Valkey also times
   command execution in `server.c::call()`, so this packet may reduce avoidable
   work but may not disable timing semantics, slowlog, latency stats, AOF, or
   replication.
2. **RESP reply buffer encoding.** `CommandContext` common reply helpers build
   `RespFrame` / `RedisString` values and enter the generic encoder even for
   simple RESP2 legacy replies. Upstream has direct reply helpers in
   `networking.c` and attempts copy avoidance for bulk replies. This packet may
   add direct client-buffer encoders for hot RESP2 shapes while preserving
   generic RESP3 frames.
3. **WATCH dirty-key fast path.** `RedisDb::signal_modified()` currently enters
   the global watched-key mutex path and constructs a `RedisString` for every
   write. Upstream `multi.c::touchWatchedKey()` returns immediately if the DB
   has no watched keys. This packet may add an equivalent no-watch fast path,
   but WATCH/MULTI/EXEC invalidation remains binding behavior.

Binding constraints for this wave:

- macOS/current-runner evidence only; no Linux-specific packet is approved here;
- no command-specific benchmark bypass;
- no owner-owned live `Vec<RedisDb>`;
- no disabled ACL, maxmemory, readonly-replica, slowlog, transactions,
  scripting, expiration, WATCH, AOF, replication, pub/sub, blocking, RDB, or
  RESP3 semantics;
- every implementation packet must be followed by wire-smoke and fresh
  profile/call-tree evidence before the next packet is trusted.

## TODO(human): Long-Term Runtime Decisions

These are remaining long-term choices, plus the now-resolved poller dependency
record for this milestone. Items still marked `TODO(human)` require human
review before the final production event-loop shape, but they do not block the
bounded plain-TCP `mio` poller packet above.

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

- **Resolved for runtime-owner-8: poller dependency.** `mio` is approved for
  the bounded plain-TCP readiness packet because it keeps the synchronous
  `&mut RuntimeOwner` command model intact and mirrors the readiness role that
  upstream Valkey expresses through `ae_kqueue.c`/`ae_epoll.c`. `polling`,
  `tokio`, and raw `epoll`/`kqueue` remain unapproved without a later
  architect packet.
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

After runtime-owner-7, the only automatic runtime-owner implementation work
beyond the std experiment and its polish packet is the bounded `mio` plain-TCP
poller. Moving TLS into the owner loop, adding I/O threads, choosing a poller
other than `mio`, adding raw poller unsafe code, or making a public performance
claim still requires a follow-up architect decision.

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
6. **The std owner-loop packet uses transitional DB storage.**
   `runtime-owner-4-std-nonblocking-owner-loop` must use existing
   `global_databases()` handles. A fully owner-owned `Vec<RedisDb>` is still
   the target model, but creating it while TLS/background helpers still use
   `global_databases()` would create two live keyspaces.
7. **Foreign bytes for owner-loop sockets enter through the owner.**
   Pub/sub, blocked wakeups, WAIT/replication replies, and similar payloads
   use per-slot `mpsc` senders. The owner loop drains receivers and writes
   plain-TCP sockets, preserving the socket ownership boundary.
