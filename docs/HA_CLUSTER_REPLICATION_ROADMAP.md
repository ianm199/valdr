# HA, Replication, and Cluster Roadmap

**Status:** long-term execution plan (2026-06-13).
**Audience:** unattended coding-agent lanes and humans reviewing what Valdr
should tackle after the single-node alpha.
**Related:** [`roadmap.md`](roadmap.md),
[`TEST_AND_FEATURE_COVERAGE.md`](TEST_AND_FEATURE_COVERAGE.md),
[`REPL_OBSERVABILITY_OVERNIGHT_PLAN.md`](REPL_OBSERVABILITY_OVERNIGHT_PLAN.md),
[`REPLICATION_INTEGRATION_DASHBOARD.md`](REPLICATION_INTEGRATION_DASHBOARD.md),
[`SENTINEL_INVENTORY.md`](SENTINEL_INVENTORY.md),
[`AOF_ENDGAME_SPEC.md`](AOF_ENDGAME_SPEC.md), and
[`EDGE_WASM_COMMAND_ENGINE.md`](EDGE_WASM_COMMAND_ENGINE.md).

## North Star

Valdr should grow from "single-node Valkey-compatible server in mostly safe
Rust" into three related but separate product shapes:

1. **Reliable single-node server:** release-grade persistence, observability,
   memory safety, and client compatibility.
2. **Replicated HA server:** primary-replica replication that can survive
   reconnects, promote a replica, and support Sentinel-style orchestration.
3. **Clustered deployment:** hash-slot sharding with client-visible redirection,
   replicas, failover, and eventually online slot migration.

Do not blur those claims. Single-node alpha is already strong. Replication,
Sentinel, and Cluster need their own evidence gates before the README can claim
them.

## Upstream Model Anchors

These are the compatibility targets to read before changing behavior:

- Valkey replication: <https://valkey.io/topics/replication/>
- Valkey `WAIT`: <https://valkey.io/commands/wait/>
- Valkey `WAITAOF`: <https://valkey.io/commands/waitaof/>
- Valkey Sentinel: <https://valkey.io/topics/sentinel/>
- Valkey Sentinel client spec: <https://valkey.io/topics/sentinel-clients/>
- Valkey Cluster tutorial: <https://valkey.io/topics/cluster-tutorial/>
- Valkey Cluster specification: <https://valkey.io/topics/cluster-spec/>
- Valkey atomic slot migration: <https://valkey.io/topics/atomic-slot-migration/>

The important design constraint from upstream is that base replication is
asynchronous primary-replica replication. Sentinel and Cluster are additional HA
layers, not replacements for the replication stream. Cluster also accepts
asynchronous replication and "last failover wins" behavior; write loss windows
are part of the model and must be documented rather than hidden.

## Current Baseline

Public claims today:

- Single-node core TCL gate is green.
- AOF is single-node alpha and correctness-gated.
- Replication is alpha.
- Production HA / Sentinel is not claimed.
- Cluster mode is not implemented.

Existing replication work already landed:

- Fine-grained replica link-state observability for `ROLE` / `INFO`.
- Partial-resync counters and a `+CONTINUE` path.
- Backlog replay for partial resync in the in-memory correctness kit.
- `DEBUG REPLICATE` propagation path.
- Local `WAIT` / `WAITAOF` guards and wakeup mechanics.
- RDB load/apply path for replica full sync exists in the runtime owner.

Current red/unfinished areas from the 2026-06-13 R0 dashboard in
[`REPLICATION_INTEGRATION_DASHBOARD.md`](REPLICATION_INTEGRATION_DASHBOARD.md):

- Dual-server `integration/replication.tcl` and `replication-buffer.tcl` are
  still blocked by full-sync / diskless behavior and replication-buffer
  accounting semantics. The focused `replication-buffer` gate is now 8/7 after
  shared output ownership, active catch-up release, and empty-RDB zero-offset
  reconnect handling. A fast follow-up kit wired the live
  `dual-channel-replication-enabled` flag and fixed dual-channel INFO-memory
  accounting for active full-sync catch-up. A later INFO split kept ordinary
  replica client output out of `mem_total_replication_buffers`, moving the
  focused Tcl gate from 7/8 to 8/7. A dual-channel INFO topology shim then
  exposed provisional `type=rdb-channel` entries for waiting full-sync
  replicas, moving the focused gate to 9/6. The transaction catch-up slice then
  made RuntimeOwner apply split replicated MULTI/EXEC catch-up with one
  pseudo-client and taught the replica dialer not to flush partial transactions
  mid-stream. The focused gate stayed at 9/6, but the low-output-buffer PSYNC
  counter failure moved from non-dual-channel duplicate reconnects to the
  narrower dual-channel fake/main PSYNC accounting edge. The dual-channel
  accounting slice then taught the dialer to advertise `dual-channel` and
  counted the logical main-channel PSYNC for dual-capable full syncs, moving the
  focused gate to 12/4. Follow-up kits then kept retained full-sync history
  open while a `send_bulk` owner pins it and included `send_bulk` replicas in
  command-stream fan-out; Tcl is intentionally deferred until the next
  scoreboard run. The next visible buffer slice is backlog-memory shrink after
  slow-replica disconnect, broader shared-history ownership, and the later true
  dual-channel RDB transport.
- A rebuilt R1 gate now shows `replication-3` at 3/4 and `replication-4` at
  15/2. The command-propagation rewrite cases are cleared, but
  expiration/PFCOUNT semantics and divergence/writable-replica cases still need
  work.
- `block-repl` is green at 2/0 with real `DEBUG DIGEST` after blocked list/zset
  single-pop wakes began propagating their canonical nonblocking forms.
- `replication-2` is green at 7/0 with real `DEBUG DIGEST` after the replica
  dialer began batching already-read command frames through RuntimeOwner.
- `replication-psync` had a historical focused 90/0 gate after live backlog
  resize, backlog TTL expiry, and delayed reconnect semantics landed. Current
  full-file reruns time out with master/replica inconsistency lines even under
  the old conservative offset-zero selector, so PSYNC is a reopened R3
  frontier. A detached full-sync catch-up tail kit removed the earliest broad
  no-reconnect mismatch; the next visible data divergence is a single string
  value `0` on the master vs `-0` on the replica. A follow-up Rust kit found
  and fixed an RDB raw numeric-string fidelity bug in that family; full Tcl has
  not yet been rerun after that fix.
- `replication-aof-sync` is green as of 2026-06-13 after full-sync RDB loads
  refresh appendonly manifests correctly.
- `replica-redirect.tcl` needs real `FAILOVER` plus client redirect semantics.

## Execution Rules

1. **Preserve current green gates.** `replication-2`, `block-repl`, and
   `replication-aof-sync` are no-regression tripwires for replication work.
   Treat `replication-psync` as a reopened red gate until the current timeout
   is explained or fixed.
2. **Use the fast kit first.** Build deterministic tests in
   `crates/redis-commands/tests/repl_correctness_kit.rs` before grinding slow
   TCL files.
3. **Run dual-server TCL sequentially.** Never run replication TCL with `-j4` or
   two integration-repl runs at once; suite contention creates false failures.
4. **Do not claim HA because a command exists.** A `FAILOVER` parser is not a
   failover feature until role transitions, data convergence, and client
   behavior are gated.
5. **Keep Cluster separate from Sentinel.** Sentinel is HA for non-clustered
   replication groups. Cluster is sharding plus its own failover model.
6. **Document every loosened divergence.** If Valdr intentionally differs from
   Valkey due to safety, Rust runtime ownership, or no-fork architecture, record
   it next to the gate that proves the replacement behavior.

## Validation Ladder

Use the cheapest rung that can prove the change.

```bash
# Rung 1: compile
cargo check -p redis-core -p redis-commands -p redis-server

# Rung 2: deterministic replication/AOF harness
cargo test -p redis-commands --test repl_correctness_kit

# Rung 3: broader command regression
cargo test -p redis-commands
cargo test --workspace

# Rung 4: focused dual-server replication TCL, sequential only
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-focused-<packet> \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/<file> \
  --isolated-tests-copy

# Rung 5: single-node gate after touching shared dispatch/persistence paths
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build \
  --timeout-s 220 \
  --baseport 30000 \
  --portcount 8000
```

## Track R: Replication Out Of Alpha

Goal: primary-replica replication can recover from reconnects, full sync,
partial sync, multi-DB writes, expiration, and propagation rewrites with
evidence strong enough to move "Replication" from alpha to beta.

### R0: Baseline And Dashboard Hygiene

**Why:** agents need a current scoreboard before changing behavior.

**Status:** completed on 2026-06-13. See
[`REPLICATION_INTEGRATION_DASHBOARD.md`](REPLICATION_INTEGRATION_DASHBOARD.md).

Work:

- Add a `tcl-integration-repl-current` runner entry or script wrapper that
  runs the in-scope replication files sequentially.
- Refresh a per-file table for:
  `replication-2`, `block-repl`, `replication-3`, `replication-4`,
  `replication-buffer`, `replication`, `replication-psync`,
  `replication-aof-sync`, and `replica-redirect`.
- Record pass/fail/timeout/no-summary status in a new results section or a
  generated artifact.
- Make `crates/redis-commands/temp-repl-*.rdb` cleanup explicit in the harness
  or test teardown so long runs do not pollute the working tree.

Gate:

```bash
cargo test -p redis-commands --test repl_correctness_kit
python3 harness/oracle/tcl-survey.py --runner-id repl-baseline \
  --profile integration-repl --timeout-s 240 --baseport 47000 \
  --portcount 4000 --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy
```

### R1: Command Propagation Correctness

**Why:** many upstream replication tests fail because the primary emits the
wrong command form, not because sockets are broken.

Work packets:

- **R1-TTL-REWRITE:** propagate relative-expiry writes as absolute `PEXPIREAT`
  / equivalent absolute forms so paused replicas do not re-anchor TTL at apply
  time. Completed on 2026-06-13; see
  [`REPLICATION_INTEGRATION_DASHBOARD.md`](REPLICATION_INTEGRATION_DASHBOARD.md).
- **R1-SPOP-REWRITE:** rewrite `SPOP key count` into deterministic `SREM`
  frames for the exact elements removed, split removals above 1024 members into
  multiple `SREM` batches, and rewrite full-count removals to `DEL`/`UNLINK`.
  Completed on 2026-06-13; see
  [`REPLICATION_INTEGRATION_DASHBOARD.md`](REPLICATION_INTEGRATION_DASHBOARD.md).
- **R1-DB-SELECT:** ensure multi-DB replication delivery emits and applies
  `SELECT` consistently, including replica apply state after reconnect.
  Dispatch-time fan-out coverage completed on 2026-06-13. Chained
  replica-apply selected-DB state also landed on 2026-06-13: a downstream
  full-sync from a replica now starts from the upstream stream DB instead of
  emitting an extra first `SELECT 9`. Broader reconnect/apply-state coverage
  remains part of `R3-RECONNECT-MATRIX`.
- **R1-NOOP-DIRTY:** keep no-op writes out of the replication stream by using a
  dirty-delta or equivalent mutation signal, not command metadata alone.
  Completed for `DEL`, `UNLINK`, `SREM`, `HDEL`, and `ZREM` on 2026-06-13; see
  [`REPLICATION_INTEGRATION_DASHBOARD.md`](REPLICATION_INTEGRATION_DASHBOARD.md).
- **R1-LEGACY-COMMAND-REWRITE:** keep deprecated/blocking command forms out of
  the replication stream when Valkey rewrites them to canonical writes. The
  2026-06-13 packet covers `GETSET` to `SET`, immediate `BRPOPLPUSH` to
  `RPOPLPUSH`, immediate `BLMOVE` to `LMOVE`, and blocked wake
  `BRPOPLPUSH` / `BLMOVE` bytes. A later real-`DEBUG DIGEST` packet cleared
  the live empty-blocking commandstats assertions by making Tcl wait for actual
  replica application.
- **R1-BLOCKING-WAKE-REWRITE:** keep blocked-client pop effects in the
  replication stream. Completed for single `BLPOP` / `BRPOP` wakes as
  `LPOP` / `RPOP` and single `BZPOPMIN` / `BZPOPMAX` wakes as `ZPOPMIN` /
  `ZPOPMAX` on 2026-06-13. Extend this lane to counted/multi-key fairness
  cases before changing more blocked-client scheduling.
- **R1-REAL-DEBUG-DIGEST:** `DEBUG DIGEST` is now a deterministic keyspace hash
  instead of an all-zero stub. This made `integration/replication` convergence
  waits meaningful and exposed `replication-2` replica catch-up lag that needs
  a throughput kit.
- **R1-REPLICA-APPLY-BATCH:** reduce replica catch-up latency by batching all
  complete command frames already read by the dialer into one RuntimeOwner apply
  request. Completed on 2026-06-13 and restored `replication-2` to 7/0 under
  real digest. Follow-ups: bounded batch size, queue-depth telemetry, and
  fairness under slow commands.
- **R1-SCRIPT-FUNCTION-PROP:** verify script/function propagation semantics
  under `EVAL`, `EVALSHA`, `FCALL`, and write/no-write shebang flags. The
  Lua-originated empty `FLUSHDB` / `FLUSHALL` chained-replica case is now
  covered; broader script/function propagation remains open.

Gate:

```bash
cargo test -p redis-commands --test repl_correctness_kit
python3 harness/oracle/tcl-survey.py --runner-id repl-command-prop \
  --profile single-node-repl --timeout-s 180 --baseport 45000 \
  --portcount 3000 --clients 1 \
  --files unit/expire,unit/type/set,unit/scripting,unit/functions \
  --isolated-tests-copy
python3 harness/oracle/tcl-survey.py --runner-id repl-r1-integration \
  --profile integration-repl --timeout-s 240 --baseport 47000 \
  --portcount 4000 --clients 1 \
  --files integration/replication-3,integration/replication-4 \
  --isolated-tests-copy
```

Parallelism:

- R1-TTL-REWRITE and R1-SPOP-REWRITE both touch dispatch/command propagation;
  do not run them in parallel.
- R1-DB-SELECT can run in parallel with docs/harness work, but not with
  replica apply state changes.

### R2: Full Sync And RDB Handoff

**Why:** replication cannot leave alpha while full sync relies on shortcuts or
hangs in diskless/window cases.

Work packets:

- **R2-RDB-BULK-FAITHFUL:** make the replica consume the primary's full-sync
  RDB payload as the source of truth. Retire or quarantine the current
  `KEYS`/`DUMP` seed path so it cannot mask full-sync bugs. Shortcut removal
  completed on 2026-06-13; the PSYNC dialer / runtime-owner RDB apply path is
  now the only bootstrap path, but the broader full-sync integration gate is
  still red until the diskless / BGSAVE-window cases are fixed.
- **R2-BGSAVE-WINDOW:** implement the observable `wait_bgsave` / diskless
  sync-delay window without violating the safe-Rust architecture. If fork is
  still used on Unix, provide a non-Unix thread/job fallback with the same state
  transitions. The 2026-06-13 frontier packet now reports replication BGSAVE in
  `INFO persistence` and honors the bounded `rdb-key-save-delay` debug window
  for replication BGSAVE jobs, moving `integration/replication-buffer` from a
  setup exception to a counted 3/15 result. The failed full-sync cleanup slice
  now aborts failed replication BGSAVE jobs through one path that drops stale
  waiters, removes temp RDB files, clears `repl_child_pid`, and allows later
  jobs to start; live `integration/replication` now reaches the later
  script-busy `READONLY` frontier instead of aborting at killed-child
  collection. A later per-key `rdb-key-save-delay` experiment showed that
  keeping BGSAVE alive longer can move the `repl_backlog_histlen` outgrowth
  assertions, but timing-only changes either time out or no-summary abort the
  file. The next packet must solve BGSAVE state lifetime, catch-up history, and
  replica offset convergence together through `fullsync_lifecycle_kit` /
  `repl_buffer_kit`.
- **R2-PIGGYBACK:** replicas arriving during an in-flight replication BGSAVE
  join the same job and receive the same snapshot plus catch-up backlog.
  Initial active-job catch-up buffering landed on 2026-06-13: writes appended
  while a replication BGSAVE is active are retained in the job and shipped
  after the RDB payload even if the circular backlog wrapped. Completed
  full-sync catch-up history is now retained while dependent replicas still pin
  it, and released on ACK/disconnect. Replica-applied commands now relay to
  downstream replicas when stream consumers exist, including empty direct and
  Lua-originated flushes. Failure cleanup is now explicit, but script-busy
  full-sync apply and diskless short-read state transitions still need
  dedicated kit coverage. The incoming replica RDB replacement boundary is
  now atomic: corrupt or short full-sync RDB bytes are staged and rejected
  without clearing the previous replica dataset, while a valid snapshot replaces
  the old keyspace. The script-busy full-sync frontier also moved: no-write
  EVAL can run on read-only replicas, script writes remain rejected for
  ordinary clients, and primary-link script writes apply locally. The live
  `integration/replication` gate then reached diskless swapdb async-loading
  state. A follow-up FCALL preflight slice made writable replicas honor
  `replica-read-only no` for write-capable functions, moving the Tcl frontier
  to async-loading abort/disconnect cleanup (`Replica didn't disconnect`).
  The next async-loading slice added a first-class `PersistenceState`
  `async_loading` bit, `INFO persistence` exposure, dispatch handling for
  `NO_ASYNC_LOADING`, safe script-timeout CONFIG exceptions, and full-sync
  success/failure cleanup. That moved the focused `integration/replication`
  run past the LOADING exceptions but still timed out in later diskless swapdb
  and pipe-drop assertions. A follow-up native RDB/function payload slice made
  full-sync replacement carry function libraries with the DB replacement plan,
  removing the successful-swap `hello1`/`hello2` mismatch from the focused Tcl
  failures. Async failure rollback, old-dataset exposure, DB-size drift, and
  diskless short-read/drop state transitions remain unfinished. A later
  diskless-load mode slice added typed `repl-diskless-load` live config and
  mode-aware loading publication, clearing the different-replid
  `replica enter loading` assertion but not the broader aborted-load rollback
  and pipe-drop log/state failures. A follow-up diskless loading slice moved
  the compatibility `Loading DB in memory` message onto the stdout log stream
  watched by the Tcl harness and allowed `CONFIG SET key-load-delay` through
  ordinary loading, moving the focused gate to a later
  `replication child dies when parent is killed` abort. True child/pipe
  lifecycle semantics remain unfinished. The next child-lifecycle slice moved
  successful transfer side effects into a deterministic `ReplicationState`
  helper, made failed child collection ignore stale PIDs, made fork children
  exit promptly when their parent dies during the bounded debug save-delay
  window, and accepted `repl-diskless-load on-empty-db`. The focused
  `integration/replication` gate now has no abort/exception point and times
  out later in replication-link/cache-master territory; async rollback,
  diskless pipe logs, and no-longer-useful RDB child cancellation remained
  open. The next child-lifetime slice pruned active full-sync waiter lists when
  replicas disconnect, made owner-loop and socket-loop cleanup signal useless
  replication BGSAVE children when `save` is disabled, and made replica-side
  full-sync RDB reads interruptible on `REPLICAOF NO ONE`. The focused Tcl
  gate dropped the no-longer-useful child failure. The next replica-link guard
  slice made replica-link replies an explicit `Client` invariant, logs and
  closes links that generate replies after `SYNC` / `PSYNC`, and turns
  disallowed keyspace interaction into the expected critical error path. The
  focused `integration/replication` gate now completes without timeout at
  28/39, and all four `replica do not write the reply to the replication link`
  assertions are gone. A follow-up PSYNC parser slice also added the
  compatibility log for malformed PSYNC offsets while keeping
  `integration/replication-psync` green at 90/0. Remaining R2 work is still
  full-sync/diskless correctness: old-data rollback, async-load exposure,
  DB-size drift, diskless pipe/drop logs, cache-master replacement, and offset
  convergence.
- **R2-BUFFER-LIMITS:** implement replica output-buffer accounting and
  disconnection policy well enough for `replication-buffer` to count tests
  instead of dying in setup. Partial accounting surface landed on 2026-06-13:
  command fan-out now routes through the replica send/accounting helper and
  `INFO memory` exposes the Valkey-compatible replication-buffer field names.
  The retained full-sync history slice moved `integration/replication-buffer`
  to 4/15. Broader online-replica shared-buffer ownership, backlog histlen
  outgrowth under slow replicas, typed writer-side drain accounting, and the
  full `replication-buffer` Tcl gate remain unfinished. The 2026-06-13
  shared/private output slice pins the key semantic distinction in
  `repl_buffer_kit`: shared replication-stream bytes may exceed the replica
  hard output-buffer limit while explicitly private output disconnects only the
  offending replica. Full-sync RDB bulk now uses the private path; normal
  command fan-out and post-RDB catch-up remain shared. A later
  shared-output/drain slice split replica pending output into shared and
  private ownership, made INFO count shared stream memory once, wired
  successful writer sends to drain pending replica output, and moved focused
  `integration/replication-buffer` from 4/11 to 5/10. The two
  backlog-histlen outgrowth assertions are now green. The next reclaim slice
  drops active BGSAVE catch-up bytes as soon as the last waiting replica
  disconnects while leaving the job installed for child/temp-file cleanup,
  moving focused `integration/replication-buffer` to 6/9. The empty-RDB
  zero-offset reconnect slice then made `PSYNC <cached-replid> 0` legal only
  after a full-sync snapshot loaded no keys, moving focused
  `integration/replication-buffer` to 7/8 by clearing the dual-channel
  low-output-buffer partial-resync assertion. The next kit-first pass then
  made `dual-channel-replication-enabled` a real live config value and changed
  INFO memory to exclude active RDB full-sync catch-up from normal
  replication-buffer accounting while still charging retained post-transfer
  PSYNC history; Tcl was intentionally deferred as a scoreboard. The INFO
  output split then stopped counting ordinary replica client output as
  `mem_total_replication_buffers`, added active-catch-up outgrowth coverage to
  `repl_buffer_kit`, and moved focused `integration/replication-buffer` to
  8/7. The dual-channel INFO topology slice then added provisional
  `type=rdb-channel` lines for waiting full-sync replicas and moved the gate to
  9/6. The transaction catch-up slice then kept one replication-apply
  pseudo-client alive for a parsed RuntimeOwner batch, held socket-read batches
  open while a replicated MULTI is pending, suppressed downstream transaction
  fanout during replica apply, and extended the runtime apply timeout for
  upstream `DEBUG SLEEP` catch-up. Rust units and kits stayed green, and the
  focused `integration/replication-buffer` artifact
  `harness/oracle/results/tcl-survey/20260614T043523717635Z/result.json`
  stayed at 9/6 while proving the non-dual low-output-buffer duplicate-PSYNC
  loop is gone. The dual-channel PSYNC accounting slice then mapped
  `REPLCONF capa dual-channel`, advertised it from dual-enabled replicas, and
  counted the logical main-channel successful PSYNC for dual-capable full
  syncs while still using the ordinary RDB transport. Focused
  `integration/replication-buffer` artifact
  `harness/oracle/results/tcl-survey/20260614T044906327151Z/result.json`
  moved the gate to 12/4. The next kit-first drain-visibility slice keeps
  primary-side full-sync replicas in `send_bulk` until their queued RDB/catch-up
  bytes leave the RuntimeOwner write buffer, moves replica-side ROLE to
  `connected` only after full-sync stream idle, accounts owner-loop replica
  writes through the same pending-output drain path, and proves the active
  full-sync history can satisfy an online replica reconnect while another waiter
  pins it. Short Rust kits plus a two-server live probe are now the debugger for
  this surface; the focused Tcl file remains the scoreboard gate. Remaining
  buffer work is later slow-replica output-buffer disconnect trimming, broader
  partial-resync history ownership, and real dual-channel RDB transport beyond
  the compatibility accounting shim. A later debug-delayed BGSAVE hold
  experiment was rejected after it regressed the focused gate to a no-summary
  replica-offset catch-up abort; the next BGSAVE-window attempt must prove
  online replica catch-up throughput while a full-sync waiter pins large shared
  history, not just keep the job state alive longer. The follow-up dialer
  throughput slice raised the primary-stream read window to 1 MiB and added a
  10 KiB-command batching kit, keeping the focused `replication-buffer` gate
  counted at 12/4 while removing the one-command-per-large-frame apply
  bottleneck as a prerequisite for the next state-window fix. The current
  kit-first shared-stream slice keeps retained full-sync history open while a
  `send_bulk` owner pins it and treats `send_bulk` replicas as command-stream
  fan-out targets; `repl_buffer_kit`, `psync_reconnect_kit`,
  `replica_dialer::tests`, and `cargo check` are green, with the long Tcl gate
  deferred as an outer scoreboard.

Gate:

```bash
cargo test -p redis-commands --test repl_correctness_kit
python3 harness/oracle/tcl-survey.py --runner-id repl-fullsync \
  --profile integration-repl --timeout-s 300 --baseport 47000 \
  --portcount 4000 --clients 1 \
  --files integration/replication,integration/replication-buffer \
  --isolated-tests-copy
```

### R3: PSYNC Hardening

**Why:** partial resync is the line between "replication demo" and a usable
replicated server.

Work packets:

- **R3-DUAL-REPLID:** implement primary and secondary replication IDs plus
  failover-history windows where Valkey expects them.
- **R3-BACKLOG-RESIZE:** make `repl-backlog-size` changes safe and observable.
  Completed for the live PSYNC path on 2026-06-13: `CONFIG SET` now resizes the
  live circular backlog while preserving readable bytes, and the focused
  `integration/replication-psync` gate is green with the upstream 100 MB
  backlog matrix. The broader `replication-buffer` shared-buffer/output-buffer
  memory model remains separate R2/R6 work.
- **R3-OFFSET-CONVERGENCE:** make `wait_for_ofs_sync` converge in high-volume
  tests; audit ACK timing, backlog offsets, and replica apply progress.
- **R3-RECONNECT-MATRIX:** deterministic tests for reconnect within backlog,
  reconnect outside backlog, wrong replid, future offset, empty backlog, and
  backlog wraparound. Master-side PSYNC decision coverage completed on
  2026-06-13. Replica-side target-change hardening now clears cached partial
  resync metadata only when `REPLICAOF` points at a different primary, preserving
  same-primary reconnect state while preventing stale PSYNC attempts against a
  new target. `psync_reconnect_kit.rs` now drives the live `psync_command`
  entrypoint for same-primary continue, backlog-expired fallback, wrong replid,
  future offset, and fresh full-sync metrics. A follow-up 2026-06-13 slice made
  `CLIENT KILL <primary-addr>` on replicas request a dialer reconnect, moving
  `integration/replication-psync` from timeout to a counted 86/4 result. A later
  2026-06-13 slice added live backlog resize, `repl-backlog-ttl` expiry, stale
  replica-entry cleanup by listening port, and `DEBUG SLEEP` pause support for
  the background dialer; the focused `integration/replication-psync` gate now
  passes 90/90 at
  `harness/oracle/results/tcl-survey/20260613T162716653643Z/result.json`.
  Current reruns later on 2026-06-13 timed out with master/replica
  inconsistency lines both with the scoped empty-RDB zero-offset selector and
  with the old conservative selector. Treat this as reopened R3 work:
  a follow-up kit on 2026-06-14 fixed the detached full-sync catch-up tail
  window where writes appended after the reaper took the BGSAVE job were not
  included in the RDB catch-up stream. The full Tcl matrix still timed out, but
  its visible data diff narrowed to `0` vs `-0`. A follow-up small Rust kit
  covered DB 9 partial catch-up, primary stream replay, PSYNC replay from the
  offset after the `-0` frame, and full-sync RDB reconstruction. That kit found
  the RDB raw loader promoting `-0` to integer `0`; the loader now shares the
  runtime string encoder and canonical integer round-trip rule. Keep using
  these kits as the debugger and rerun full Tcl only as a scoreboard.
- **R3-METRICS:** keep `sync_full`, `sync_partial_ok`, `sync_partial_err`,
  master/replica offsets, lag, and backlog histlen faithful in `INFO`.

Gate:

```bash
cargo test -p redis-commands --test repl_correctness_kit
python3 harness/oracle/tcl-survey.py --runner-id repl-psync \
  --profile integration-repl --timeout-s 300 --baseport 47000 \
  --portcount 4000 --clients 1 \
  --files integration/replication-psync \
  --isolated-tests-copy
```

### R4: WAIT / WAITAOF / Durability Acknowledgement

**Why:** HA users rely on acknowledgement semantics even though replication is
asynchronous.

Work packets:

- **R4-WAIT-ACK:** make `REPLCONF GETACK` / `ACK` timing faithful under online,
  reconnecting, and full-syncing replicas.
- **R4-WAITAOF-REPLICA:** finish replica AOF fsync acknowledgement semantics
  across `FACK`, `appendonly` state changes, and role changes.
- **R4-ROLE-CHANGE-UNBLOCK:** unblock or error waiters correctly on
  `REPLICAOF`, `FAILOVER`, disconnect, and `appendonly no`. The first
  2026-06-13 packet drains both `WAIT` and `WAITAOF` waiters on `REPLICAOF`
  topology changes, while preserving the existing `appendonly no` WAITAOF
  error path; `FAILOVER` and disconnect coverage remain future packets.
- **R4-PERSISTENCE-MATRIX:** cross-check AOF/RDB/replication interactions from
  [`AOF_ENDGAME_SPEC.md`](AOF_ENDGAME_SPEC.md). The 2026-06-13 AOF full-sync
  packet made `integration/replication-aof-sync` green by replacing stale
  replica keyspace on full-sync RDB load, reusing disk-based RDB payloads as
  `.base.rdb` files when safe, and falling back to manifest rewrite for
  diskless sync.

Gate:

```bash
cargo test -p redis-commands --test repl_correctness_kit wait
python3 harness/oracle/tcl-survey.py --runner-id repl-wait \
  --profile integration-repl --timeout-s 240 --baseport 47000 \
  --portcount 4000 --clients 1 \
  --files unit/wait,integration/replication-aof-sync \
  --isolated-tests-copy
```

### R5: Server-Side Failover Primitive

**Why:** Sentinel and Cluster both need safe server role transitions. Implement
the server primitive before implementing an orchestrator.

Work packets:

- **R5-FAILOVER-PARSER:** implement `FAILOVER` syntax and errors without
  claiming failover yet. The first 2026-06-13 packet registers server
  `FAILOVER`, validates `TO`, `TIMEOUT`, `FORCE`, and `ABORT` syntax, returns
  faithful replica/no-replica early errors, and stops with an explicit
  unimplemented error before any coordinated failover state machine begins.
- **R5-MANUAL-FAILOVER:** primary can coordinate a manual failover to a chosen
  replica: pause writes, wait for offset, promote replica, demote old primary.
  The first state slice landed on 2026-06-13: valid `FAILOVER` requests with
  connected replicas now enter visible `waiting-for-sync` or
  `failover-in-progress` state, expose `master_failover_state` in
  `INFO replication`, and `FAILOVER ABORT` clears state and failover pause.
  Follow-up 2026-06-13 slices added timeout-driven handoff, `PSYNC ... FAILOVER`
  target promotion, old-primary demotion, failover pause clearing, and
  zero-offset partial-sync handoff. A live two-process probe now proves a forced
  failover from `127.0.0.1:27379` to `127.0.0.1:27380` can redirect both a
  blocked `BRPOP` client and a `GET` client paused during failover-in-progress.
- **R5-REPLICA-PROMOTION:** `REPLICAOF NO ONE` promotion preserves data,
  replid/offset history, client-visible role, and write policy.
- **R5-CLIENT-REDIRECT:** implement the client capability and redirect behavior
  needed by `replica-redirect.tcl`. The first redirect-contract slice landed
  on 2026-06-13: redirect-capable clients now get primary-target `REDIRECT`
  replies for replica data commands, `READONLY` clients can keep local reads,
  queue-time redirects dirty MULTI for `EXECABORT`, and queued writes redirect
  at `EXEC` if the node became a replica after queueing. Remaining work is
  the official Tcl file's no-summary timeout, not basic redirect formatting.
  Evidence from `repl-failover-observe-hang` shows the old primary reporting
  `blocked_clients:2`, `paused_actions:all`, and `role:slave` while the Tcl
  harness still timed out before resuming the paused target process.
- **R5-ABORT-ROLLBACK:** timeout and abort paths leave the topology in a
  coherent state.

Gate:

```bash
cargo test -p redis-commands --test repl_correctness_kit failover
python3 harness/oracle/tcl-survey.py --runner-id repl-failover \
  --profile integration-repl --timeout-s 300 --baseport 47000 \
  --portcount 4000 --clients 1 \
  --files integration/replica-redirect \
  --isolated-tests-copy
```

### Replication Beta Exit Criteria

Replication can move beyond alpha only when:

- `replication-2` and `block-repl` stay green under the real `DEBUG DIGEST`.
- `replication-3` and `replication-4` have no known non-diskless failures, and
  `replication-psync` stays green.
- Full sync works through the real RDB handoff path, not a seed shortcut.
- `WAIT` and `WAITAOF` behavior is documented and gated.
- README wording lists exact unsupported variants, if any.

## Track H: Sentinel / Non-Cluster HA

Goal: provide a credible HA story for a small replicated deployment without
hash-slot sharding.

Do not start Sentinel implementation before R5 server-side failover has a
working gate. Sentinel without a faithful role-change primitive becomes a
monitoring facade that cannot safely move writes.

### H0: Product Decision

Decide whether Valdr wants:

- full Valkey Sentinel compatibility,
- a minimal Sentinel-compatible client discovery subset,
- or a Valdr-native HA controller with separate branding.

Default recommendation: implement a Sentinel-compatible subset only after the
server failover primitive is green, because existing clients understand
Sentinel discovery.

### H1: Sentinel Command Inventory

**Status:** completed on 2026-06-13. See
[`SENTINEL_INVENTORY.md`](SENTINEL_INVENTORY.md).

Work:

- Inventory upstream `sentinel/*.tcl` and bucket tests into:
  discovery, monitoring, quorum, failover, config rewrite, auth/TLS,
  notifications, and edge cases.
- Add `sentinel_later` sub-buckets to coverage docs so progress is visible.
- Implement parser stubs only when they return faithful errors or read-only
  discovery data.

Candidate command subset:

- `SENTINEL get-master-addr-by-name`
- `SENTINEL masters`
- `SENTINEL replicas` / `SENTINEL slaves`
- `SENTINEL sentinels`
- `SENTINEL ckquorum`
- `SENTINEL failover`

### H2: Monitor And Quorum Core

Work:

- Sentinel process or mode with its own config/runtime ownership.
- Periodic PING/INFO/ROLE probes.
- Subjective down / objective down tracking.
- Quorum and epoch bookkeeping.
- Pub/Sub notification channel compatibility where clients expect it.

### H3: Automated Failover

Work:

- Select best replica by offset, priority, and availability.
- Trigger server-side `FAILOVER` / promotion path.
- Reconfigure remaining replicas to follow new primary.
- Preserve client discovery correctness during and after failover.

Gate:

```bash
python3 harness/oracle/tcl-survey.py --runner-id sentinel-focused \
  --profile default --timeout-s 300 --baseport 51000 \
  --portcount 5000 --clients 1 \
  --files sentinel/<focused-files> \
  --isolated-tests-copy
```

Sentinel can be public beta only when a three-server topology can survive a
primary kill, promote a replica, redirect clients to the new primary, and keep
the old primary from accepting divergent writes after it returns.

## Track C: Cluster

Goal: implement Valkey Cluster in staged slices: first client-visible static
slot routing, then multi-node topology, then replicas/failover, then online
migration.

Cluster should not be mixed into the single-node or Sentinel claims. It has a
different consistency model and its own client contract.

### C0: Cluster Foundations

Work packets that can run early and mostly independently:

- **C0-HASHSLOT:** implement CRC16 slot calculation and hashtag extraction in a
  focused module with tests against official vectors. Completed on
  2026-06-13 for `CLUSTER KEYSLOT`; this does not enable cluster mode.
- **C0-KEYSPECS:** audit command key extraction for cluster routing. Multi-key
  commands need `CROSSSLOT` behavior when keys span slots. Audit helpers and
  tests completed on 2026-06-13; runtime `CROSSSLOT` enforcement remains part
  of cluster-mode enablement.
- **C0-CONFIG:** add config surface for cluster-enabled, node id, announced
  host/ports, and local slot ownership.
- **C0-COVERAGE:** split `cluster_later` coverage into sub-buckets: keyslot,
  command redirection, cluster bus/gossip, failover, resharding, and sharded
  pub/sub.

Gate:

```bash
cargo test -p redis-core cluster
cargo test -p redis-commands cluster
```

Evidence:

- `crates/redis-commands/src/cluster.rs` implements CRC16/XMODEM, Valkey-style
  hashtag extraction, and `CLUSTER KEYSLOT`.
- Tests cover the standard CRC16 vector, known key-slot vectors, hashtag edge
  cases, direct handler execution, and dispatch through the parent `CLUSTER`
  command.
- `command_meta.rs` now uses generated subcommand `container` metadata for
  `COMMAND GETKEYS` lookup and adds audit tests for range specs, keynum specs,
  generated no-key subcommands, and multi-key slot grouping.
- Gate on 2026-06-13:

  ```bash
  cargo test -p redis-commands cluster -- --nocapture
  cargo test -p redis-commands command_keyspec -- --nocapture
  ```

  Results: cluster filter 7 passed, 0 failed; command-keyspec filter 3 passed,
  0 failed.

### C1: Static Single-Node Cluster Compatibility

Work:

- `CLUSTER KEYSLOT`
- `CLUSTER INFO`
- `CLUSTER NODES`
- `CLUSTER SLOTS`
- `CLUSTER SHARDS` if current upstream clients expect it.
- Return `MOVED` for slots not owned by the local node in a static table.
- Return `CROSSSLOT` for unsupported multi-slot commands.

This stage makes cluster-aware clients able to bootstrap and route, even before
Valdr runs a real cluster bus.

Gate:

```bash
python3 harness/oracle/tcl-survey.py --runner-id cluster-static \
  --profile default --timeout-s 180 --baseport 52000 \
  --portcount 5000 --clients 1 \
  --files unit/cluster/<static-files> \
  --isolated-tests-copy
```

### C2: Multi-Node Static Sharding

Work:

- Start N Valdr nodes with fixed slot ownership.
- Clients receive stable `MOVED` redirections.
- No automatic failover yet.
- No slot migration yet.
- Add a harness helper to launch a deterministic 3-primary cluster.

Gate:

- Rust process harness for slot map routing.
- Focused upstream cluster TCL files that do not require failover/migration.

### C3: Cluster Bus And Gossip

Work:

- Cluster bus connection lifecycle.
- PING/PONG/MEET/FORGET gossip.
- Node flags and link state.
- Config epochs and slot ownership convergence.
- `CLUSTER MEET`, `CLUSTER FORGET`, `CLUSTER RESET`, `CLUSTER REPLICATE`
  minimal faithful behavior.

Gate:

- Deterministic cluster bus kit before TCL.
- Focused cluster TCL for node discovery and convergence.

### C4: Cluster Replicas And Failover

Work:

- Replica assignment to primaries.
- Manual `CLUSTER FAILOVER` paths.
- Election / epoch handling.
- Promotion and old-primary demotion using Track R/R5 primitives.
- Document asynchronous write-loss windows.

Gate:

- Multi-node kill/restart harness.
- Focused cluster failover TCL.

### C5: Resharding And Migration

Work:

- Slot states: importing, migrating, stable.
- `ASK` redirection and `ASKING`.
- `MIGRATE` / key transfer behavior.
- Atomic slot migration APIs if targeting newer Valkey behavior.
- Sharded pub/sub routing.

Gate:

- Focused cluster migration TCL.
- Long-running chaos loop with resharding under writes.

### Cluster Beta Exit Criteria

Cluster can move beyond "experimental" only when:

- Cluster-aware clients can bootstrap from `CLUSTER SLOTS` / `CLUSTER SHARDS`.
- Static sharding routes reads/writes correctly across at least 3 primaries.
- Multi-key commands either work within one slot or return faithful errors.
- Replica failover behavior is either implemented and gated, or clearly marked
  unsupported.
- Slot migration behavior is either implemented and gated, or clearly marked
  unsupported.

## Track P: Persistence / Replication / HA Interlock

Goal: make durability semantics coherent when persistence and replication are
both enabled.

Work packets:

- **P1-AOF-REPL-OFFSET:** ensure AOF records and fsync state publish the correct
  replication offsets for `WAITAOF`.
- **P2-RESTART-REPLID:** define and test replid/offset behavior after restart
  from RDB/AOF.
- **P3-AOF-BASE-FOR-REPL:** finish RDB-reuse-as-AOF-base or document the safe
  replacement.
- **P4-FAILOVER-PERSISTENCE:** after promotion, new primary AOF/RDB state must
  be coherent and old primary must not fork divergent history without explicit
  reconfiguration.

Gate:

```bash
cargo test -p redis-commands --test aof_correctness_kit
cargo test -p redis-commands --test repl_correctness_kit
python3 harness/bench/aof-matrix.py --quick --targets rust --skip-build
python3 harness/oracle/tcl-survey.py --runner-id repl-persistence \
  --profile integration-repl --timeout-s 360 --baseport 47000 \
  --portcount 4000 --clients 1 \
  --files integration/replication-aof-sync,unit/wait \
  --isolated-tests-copy
```

## Track E: Portable Engine And Storage Backends

Goal: keep the new Valdr engine / EdgeStash work aligned with the server HA
work without forcing the edge runtime to inherit full server complexity.

Work packets:

- **E1-SNAPSHOT-TRAIT:** split snapshot load/save from hot authoritative state.
- **E2-JOURNAL-TRAIT:** add append/replay journal hooks for hosts that can
  provide durable logs.
- **E3-R2-SNAPSHOT:** implement R2/S3-style object storage as snapshot/archive
  backends, not as hot atomic state.
- **E4-DO-AUTHORITY:** keep Durable Objects as the first authoritative edge
  state backend because they serialize requests per object.
- **E5-PORTABLE-REPLICA:** experiment with exporting Valdr-engine snapshots /
  journals into the server replication model only after server Track R is more
  stable.

This track should not block server HA. It is a parallel product path with a
shared principle: make host capabilities explicit.

## Agent Work Queue

These are packet-sized tasks an unattended agent can pick up. Each packet should
end with a short evidence note in this document or a linked packet doc.

### Safe Near-Term Packets

| Packet | Goal | Touches | Gate |
|---|---|---|---|
| `R0-REPL-DASHBOARD` | Fresh integration-repl scoreboard and cleanup temp RDB files | harness/docs | replication tripwires |
| `R1-NOOP-DIRTY` | Stop no-op writes from propagating | dispatch/db/multi tests | repl kit + single-node-repl |
| `R1-SPOP-REWRITE` | Propagate `SPOP count` as deterministic removals | set command + repl kit | repl kit + replication-3/4 |
| `R1-TTL-REWRITE` | Propagate relative TTL writes as absolute expiry | string/db/expire propagation | repl kit + replication-3/4 |
| `R1-DB-SELECT` | Fix multi-DB replica apply and stream `SELECT` coverage | replication apply/runtime owner | repl kit |
| `R3-RECONNECT-MATRIX` | Add deterministic PSYNC edge-case tests | repl kit only first | repl kit |
| `R5-REPLICA-REDIRECT-HARNESS` | Convert the remaining `replica-redirect.tcl` no-summary timeout into a parsed failure or pass | harness observation + failover kit | failover_redirect_kit + focused Tcl |
| `C0-HASHSLOT` | Implement hash slot calculation and vectors | new cluster module | cargo tests |
| `C0-KEYSPECS` | Audit key extraction / CROSSSLOT requirements | command metadata docs/tests | cargo tests |
| `H1-SENTINEL-INVENTORY` | Bucket Sentinel TCL and command surface | docs/harness only | no code gate |
| `E1-SNAPSHOT-TRAIT` | Split snapshot store abstraction for engine hosts | valdr-engine/edgestash | valdr wasm checks |

### Higher-Risk Packets

| Packet | Risk |
|---|---|
| `R2-RDB-BULK-FAITHFUL` | Changes full-sync data path; can regress all dual-server replication. |
| `R2-BGSAVE-WINDOW` | Touches process/job/fork/thread fallback behavior. |
| `R5-MANUAL-FAILOVER` | Requires role-change state machine and write freeze semantics. |
| `H2-MONITOR-QUORUM` | New long-running runtime mode; needs careful architecture. |
| `C3-CLUSTER-BUS` | New network protocol and distributed state machine. |
| `C4-CLUSTER-FAILOVER` | Combines cluster epochs with replication promotion. |
| `C5-RESHARDING` | Key movement under writes; high blast radius. |

## Parallelization Matrix

Safe in parallel:

- Cluster C0 hash-slot work with replication R1 work.
- Sentinel H1 inventory with any code work.
- Edge storage E1/E2 with server replication, if files do not overlap.
- Docs/coverage bucket work with implementation work.

Do not parallelize:

- Two dual-server TCL replication runs.
- Two edits to `crates/redis-commands/src/replication.rs`.
- Full-sync R2 work with failover R5 work.
- Cluster failover C4 with server failover R5.
- AOF/WAITAOF Track P with low-level replication offset changes unless one lane
  owns the shared offset model.

## Claim Milestones

### M1: Replication Beta

README can change "Replication: Alpha" to "Replication: Beta" when R1-R4 gates
are green and remaining failures are explicitly limited to diskless/failover
variants.

### M2: HA Preview

README can add "Manual failover preview" when R5 has a green process harness and
`replica-redirect` has meaningful passing coverage.

### M3: Sentinel Experimental

README can add "Sentinel experimental" when a three-node primary/replica setup
plus three Sentinel processes can promote a replica and clients can discover the
new primary.

### M4: Cluster Static Preview

README can add "Cluster static routing preview" when C1/C2 are green and
cluster-aware clients can route by `CLUSTER SLOTS` across fixed primaries.

### M5: Cluster Experimental

README can add "Cluster experimental" when C3/C4 are green enough to survive a
primary kill in a sharded topology, with documented async write-loss behavior.

## Recommended First Run

If you want to let agents run while doing other work, start with this queue:

1. `R0-REPL-DASHBOARD`
2. `R1-NOOP-DIRTY`
3. `R1-SPOP-REWRITE`
4. `R1-TTL-REWRITE`
5. `R3-RECONNECT-MATRIX`
6. `C0-HASHSLOT`
7. `H1-SENTINEL-INVENTORY`
8. `E1-SNAPSHOT-TRAIT`

This queue maximizes independent progress while avoiding the deepest
distributed-state-machine work until the command propagation and evidence
baseline are stronger.
