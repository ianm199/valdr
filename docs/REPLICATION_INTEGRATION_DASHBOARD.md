# Replication Integration Dashboard

**Status:** R0 baseline refreshed on 2026-06-13.

This dashboard tracks the current `integration-repl` TCL frontier for Valdr
replication work. It is telemetry, not a production HA claim.

## Commands

Fast deterministic gate:

```bash
cargo test -p redis-commands --test repl_correctness_kit
```

Latest result on 2026-06-13 after the real digest / blocked-pop wake packet:
29 passed, 0 failed.

R0 full integration dashboard:

```bash
harness/oracle/run-integration-repl-current.sh \
  --runner-id repl-r0-current-20260613 \
  --skip-build
```

Result on 2026-06-13:

```text
TCL survey: 9 files, 36 passed tests, 18 failed tests, 1 timed out,
3 without summary, 53 parsed failure lines, 2 abort/exception points
```

Artifacts:

- Per-file logs:
  `harness/oracle/results/tcl-survey/20260613T002752482668Z/`
- Tripwire/result-writer verification:
  `harness/oracle/results/tcl-survey/20260613T003911251513Z/result.json`

The full R0 dashboard run completed before `tcl-survey.py` started writing the
aggregate `result.json` file. Future runs write that file automatically. Rows
for `replication-3` and `replication-4` below use the newer rebuilt R1 gate:

```bash
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r1-integration-3-4-current \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-3,integration/replication-4 \
  --isolated-tests-copy \
  --skip-build
```

R1 rebuilt result on 2026-06-13:

```text
TCL survey: 2 files, 18 passed tests, 6 failed tests, 0 timed out,
0 without summary, 12 parsed failure lines, 0 abort/exception points
```

Artifact:
`harness/oracle/results/tcl-survey/20260613T005958508926Z/result.json`.

## Current Table

| File | Current result | Status | Frontier |
|---|---:|---|---|
| `integration/replication-2` | 7/0 | Green | Real `DEBUG DIGEST` remains active; replica apply batching removed the complex-dataset catch-up lag that had exposed the primary/replica key-count gap. |
| `integration/block-repl` | 2/0 | Green | Real `DEBUG DIGEST` plus single blocked-pop wake propagation now validates the list/zset blocking workload. |
| `integration/replication-3` | 3/4 | Red | Expiry consistency, writable-replica expired-key behavior, and PFCOUNT expired-key/cache semantics. |
| `integration/replication-4` | 15/2 | Red | SPOP rewrite cases now pass; remaining failures are divergence/default writable-replica cases. |
| `integration/replication-buffer` | 16/0 | Green | The replication-buffer kit line now covers active full-sync catch-up beyond the circular backlog, partial resync from retained shared history, retained-history release after the last dependent replica disconnects, shared output memory charged once, and hard-limit disconnect isolation. Follow-up Tcl scoreboards moved the file through 13/3, 15/1, and finally 16/0 at artifact `20260614T071942726290Z`; keep `repl_buffer_kit` as the inner loop and rerun this Tcl file only as a regression scoreboard. |
| `integration/replication` | 40/27 | Red | Full-sync lifecycle work moved past killed-child cleanup, script-busy READONLY, FCALL READONLY, the first async-loading CONFIG exception, successful swapdb function-context mismatch, parent-killed child discovery, `repl-diskless-load on-empty-db`, no-longer-useful RDB child cancellation, all four replica-link reply-violation assertions, malformed-PSYNC-offset logging, chained replica `FLUSHDB` / `FLUSHALL` stream relay, `GETSET` rewrite, nonblocking `BRPOPLPUSH` / `BLMOVE` rewrite stats, and empty-blocking `BRPOPLPUSH` / `BLMOVE` commandstats via real digest waits. Remaining failures are counted full-sync, diskless pipe/drop, blocked-list role-change, cache-master, lazy-expire, and old-data rollback cases. |
| `integration/replication-psync` | timeout | Red | Historical focused gate was 90/0 after live backlog resize, `repl-backlog-ttl` expiry, stale replica entry cleanup, and `DEBUG SLEEP` pause support. Current full-file reruns remain red, but the kit-first loop has moved the visible frontier: raw `-0` RDB fidelity was fixed, set-store commands now rewrite to deterministic `DEL` plus `SADD`, fresh full-sync catch-up now injects and retains a selected-DB prefix before active writes, and the latest replica-only DB 0 set residue is covered by a post-fullsync live-stream SELECT kit. The latest single Tcl scoreboard remains `20260614T091542767472Z`; do not use the six-minute file as the debugger. Rerun it as a scoreboard after the next kit batch or nightly pass. |
| `integration/replication-aof-sync` | 6/0 | Green | Full-sync AOF base refresh, disk-based RDB reuse, diskless BGREWRITEAOF fallback, and stale local RDB restart coverage now pass. |
| `integration/replica-redirect` | 11/0 | Green | `CLIENT CAPA REDIRECT`, MULTI/EXEC replica redirects, failover pause, waiting-for-sync responses, and blocked-client behavior during failover now pass in the direct Tcl file. The final 2026-06-14 kit-first pass moved the file from timeout/no-summary to parsed 10/1, reduced the stale DB 9 stream return to partial-resync/role-change invariants, then cleared the full file at 11/0 in 6 seconds. |
| `unit/wait` | 39/0 | Green | WAIT command suite passed after the R4 role-change unblock packet; WAITAOF/FACK edge cases still need separate coverage. |

## Temp RDB Cleanup

R0 added explicit cleanup for stale replication full-sync temp files matching
`crates/redis-commands/temp-repl-*.rdb` in `tcl-survey.py`.

Evidence from the full dashboard run:

- `before_setup` removed 176 stale temp RDB files.
- All cleanup calls reported 0 errors.
- After the run, this returned 0:

  ```bash
  find crates/redis-commands -maxdepth 1 -name 'temp-repl-*.rdb' -print | wc -l
  ```

## Next Useful Packets

The R1 propagation packets cleared the known command-rewrite regressions, but
the rebuilt `replication-3` / `replication-4` gate is not green. The largest
visible integration frontiers are now:

- Expiry-on-replica semantics: `replication-3` still fails master/replica
  consistency with expire, writable replica expired-key behavior, and PFCOUNT
  expired-key/cache cases.
- `R1-BLOCKING-WAKE-REWRITE`: empty-blocking `BRPOPLPUSH` / `BLMOVE` live
  stats now pass, and the list/zset single-pop blocking workload is covered by
  `block-repl`. Keep extending this family for `BLMPOP` / `BZMPOP` and
  multi-key fairness before touching more blocked-client code.
- `R1-REPLICA-APPLY-THROUGHPUT`: the first batching slice is complete and
  restores `replication-2` to green under real digest. Keep this lane open for
  bounded queue depth, batch-size metrics, and owner-loop fairness under slow
  commands.
- `R2-RDB-BULK-FAITHFUL`: the old `REPLICAOF` pre-PSYNC `KEYS`/`DUMP` seed
  shortcut is removed, so remaining full-sync work must pass through the
  streamed RDB handoff path.
- `R2-BGSAVE-WINDOW`: replication BGSAVE now reports through `INFO persistence`
  and honors the bounded per-key debug save-delay window; keep extending this
  into the diskless/full-sync windows behind `integration/replication`. Failed
  full-sync BGSAVE jobs now clean up waiters, temp files, and replication-child
  state instead of poisoning later sync attempts. Async-loading state is now
  explicit in `INFO persistence` and dispatch. Successful full-sync RDB
  replacement now carries function payloads too, and replica-link replies are
  now detected, logged, and disconnected instead of being flushed to the link.
  Chained replica apply now relays empty `FLUSHDB` / `FLUSHALL`, including
  Lua-originated flushes, and initializes downstream stream DB state from the
  upstream selected DB. Async failure rollback and diskless pipe cleanup remain
  open.
- `R2-BGSAVE-CATCHUP`: active replication BGSAVE jobs now retain appended
  replication bytes outside the circular backlog and use that buffer for
  post-RDB catch-up. Completed full-sync catch-up bytes are now also retained
  while dependent replicas still pin them. The kit surface now also proves an
  online replica reconnect can consume active full-sync history while another
  waiter keeps that history pinned.
- `R3-RECONNECT-MATRIX`: extend the new master-side PSYNC decision matrix into
  live replica-dialer reconnect coverage before grinding `replication-psync`.
  Current full-file PSYNC reruns time out again with master/replica
  inconsistency lines, including a conservative-selector comparison. The
  detached full-sync catch-up tail slice removes the earliest broad
  no-reconnect mismatch. The narrowed `0` vs `-0` family now has Rust kit
  coverage, including an RDB raw-string load bug where `-0` was promoted to
  integer `0`. The later DB 0 set residue is also covered by a kit that drives
  RDB delivery through `complete_repl_bgsave_transfer` and proves the first
  post-fullsync DB 9 live write forces `SELECT 9`. Keep using these kits as
  the debugger and reserve the full Tcl matrix for a scoreboard rerun.
- `R2-BUFFER-LIMITS`: accounting aliases, fan-out accounting, retained
  full-sync history, owner-loop replica drain, and full-sync `send_bulk`
  visibility are covered; implement broader shared-buffer memory accounting,
  backlog outgrowth under slow online replicas, and replica output-buffer
  disconnection semantics behind `replication-buffer`.
- `R4-WAIT/WAITAOF`: role-change unblock now covers both WAIT and WAITAOF for
  `REPLICAOF` topology changes; replica FACK/disconnect semantics remain open.
- `R4-AOF-FULLSYNC`: `replication-aof-sync` is now green after full-sync RDB
  loads refresh appendonly manifests correctly.
- `R5-MANUAL-FAILOVER`: server `FAILOVER` now has parser coverage and visible
  state; the next useful work is real write pause, offset wait,
  promotion/demotion, and blocked-client handling needed by
  `replica-redirect`. The basic replica REDIRECT contract for redirect-capable
  clients is now covered, and `FAILOVER` exposes `waiting-for-sync` /
  `failover-in-progress`. Pause accounting, timeout handling, blocked-client
  REDIRECT unblocking, and promotion/demotion remain open.

## Packet Evidence

### R1-REPLICA-APPLY-BATCH

Status: completed on 2026-06-13.

Implementation:

- The replica dialer now parses all complete frames already read from the
  primary socket and submits them to RuntimeOwner as one ordered batch instead
  of blocking on a completion channel for every single write.
- `REPLCONF GETACK *` still flushes any pending command batch before replying,
  preserving ACK ordering.
- RuntimeOwner applies each batch in order while preserving the replica's
  selected DB across commands and records the final applied replication offset.

Evidence:

```bash
cargo test -p redis-commands replica_dialer -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-apply-batch-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `redis-commands replica_dialer`: 7 passed, 0 failed.
- `repl_correctness_kit`: 29 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused single-file probe:
  `harness/oracle/results/tcl-survey/20260613T220012228979Z/result.json`
  reported `integration/replication-2` 7/0.
- Paired tripwire:
  `harness/oracle/results/tcl-survey/20260613T220025361161Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

### R1-REAL-DIGEST-BLOCKING-WAKE

Status: integration advance completed on 2026-06-13.

Implementation:

- `DEBUG DIGEST` now hashes the live keyspace instead of returning the old
  all-zero stub, so Tcl convergence checks wait for actual replica application.
  The digest is deterministic across databases and object types; it omits TTL
  timing metadata for now to avoid turning expiry jitter into false mismatches.
- Empty-blocking `BRPOPLPUSH` / `BLMOVE` commandstats assertions now pass
  because Tcl no longer exits the digest wait before the replica applies the
  propagated `RPOPLPUSH` / `LMOVE`.
- Single-element `BLPOP` / `BRPOP` wakes now propagate `LPOP` / `RPOP`.
- Single-element `BZPOPMIN` / `BZPOPMAX` wakes now propagate `ZPOPMIN` /
  `ZPOPMAX`.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo test -p redis-core blocked_keys::tests -- --nocapture
cargo test -p redis-core replication::tests -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-debug-digest-blocking-pop \
  --profile integration-repl \
  --timeout-s 420 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-blocking-pop-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `repl_correctness_kit`: 29 passed, 0 failed.
- `redis-core blocked_keys::tests`: 3 passed, 0 failed.
- `redis-core replication::tests`: 15 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Narrow Tcl probes showed:
  `BRPOPLPUSH replication, when blocking against empty list` and
  `BLMOVE (left, left) replication, when blocking against empty list` both
  passing.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T214905151897Z/result.json`
  reported 40 passed / 27 failed. This clears the five empty-blocking
  `BRPOPLPUSH` / `BLMOVE` stats cases over the previous 35/32 result.
- Focused tripwire:
  `harness/oracle/results/tcl-survey/20260613T214753728213Z/result.json`
  reported `integration/block-repl` 2/0. It also reported
  `integration/replication-2` 6/1; the real digest exposed a separate
  complex-dataset catch-up lag that was previously hidden by the zero digest.

### R1-LEGACY-COMMAND-REWRITE

Status: partial integration advance completed on 2026-06-13.

Implementation:

- `GETSET key value` now propagates as `SET key value`, so replicas count and
  apply the canonical write form.
- Immediate `BRPOPLPUSH` with data now propagates as `RPOPLPUSH`, and immediate
  `BLMOVE` with data now propagates as `LMOVE`.
- Blocked move waiters now remember whether they came from `BRPOPLPUSH` or
  `BLMOVE`; the deterministic kit proves wake propagation emits
  `RPOPLPUSH` for legacy waiters and `LMOVE` for BLMOVE waiters. The official
  Tcl empty-blocking commandstats assertions still need a live-server follow-up.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo test -p redis-core blocked_keys::tests -- --nocapture
cargo test -p redis-core replication::tests -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-command-rewrite-stats \
  --profile integration-repl \
  --timeout-s 420 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-command-rewrite-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `repl_correctness_kit`: 25 passed, 0 failed.
- `redis-core blocked_keys::tests`: 3 passed, 0 failed.
- `redis-core replication::tests`: 15 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T205627898507Z/result.json`
  reported 35 passed / 32 failed. `GETSET replication`, nonblocking
  `BRPOPLPUSH replication, list exists`, and the four nonblocking
  `BLMOVE ..., list exists` cases cleared.
- Focused dual-server tripwire:
  `harness/oracle/results/tcl-survey/20260613T210230529049Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

### R2-CHAINED-REPLICA-FLUSH-RELAY

Status: completed on 2026-06-13.

Implementation:

- `replication_apply` commands may now fan out to downstream replicas when a
  replica has stream consumers, rather than being suppressed as ordinary
  replica writes.
- Lua inner writes no longer inherit the EXEC drain suppression that blocked
  script-originated flush propagation.
- Replica-applied `SELECT` updates the remembered upstream stream DB, and a
  downstream full-sync from a replica starts the command stream at that DB
  instead of emitting a spurious first `SELECT 9`.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo test -p redis-core replication::tests -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-replica-apply-flush-select-state \
  --profile integration-repl \
  --timeout-s 420 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-replica-apply-flush-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `repl_correctness_kit`: 23 passed, 0 failed.
- `redis-core replication::tests`: 15 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T204411632115Z/result.json`
  reported 29 passed / 38 failed; `FLUSHDB / FLUSHALL should replicate`
  cleared.
- Focused dual-server tripwire:
  `harness/oracle/results/tcl-survey/20260613T204905111041Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

### R2-RDB-BULK-FAITHFUL

Status: shortcut removal completed on 2026-06-13; full-sync integration remains
red until the diskless / BGSAVE-window frontier is fixed.

Implementation:

- `REPLICAOF <host> <port>` no longer opens a separate client connection to the
  primary to copy keyspace through `KEYS`, `PTTL`, and `DUMP`.
- The replica bootstrap source of truth is now the PSYNC dialer reading the
  `FULLRESYNC` RDB bulk and applying it through the runtime-owner `LoadRdb`
  queue.
- `replicaof_does_not_preseed_from_primary` binds a fake primary socket and
  proves `REPLICAOF` does not open the old seed connection before the dialer
  owns full sync.

Evidence:

```bash
cargo test -p redis-commands \
  replication::tests::replicaof_does_not_preseed_from_primary \
  -- --nocapture
cargo test -p redis-commands replication::tests -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit
cargo check -p redis-commands
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r2-rdb-bulk-faithful-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- Focused no-preseed unit test: passed.
- Full replication unit module: 11 passed, 0 failed.
- `repl_correctness_kit`: 17 passed, 0 failed.
- `cargo check -p redis-commands`: passed.
- `cargo build --bin redis-server`: passed.
- Focused dual-server tripwire:
  `harness/oracle/results/tcl-survey/20260613T042054084579Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.
- Dual-server `integration/replication` and `integration/replication-buffer`
  remain the current red full-sync / buffer frontier from the R0 dashboard.

### R2-BUFFER-LIMITS

Status: partial accounting surface completed on 2026-06-13; slow Tcl remains
red. It now reaches counted buffer assertions after the R2-BGSAVE-WINDOW
packet.

Implementation:

- Ordinary command fan-out and raw synthesized propagation now route through
  `ReplicationState::send_to_replica`, the same helper used by RDB/catch-up
  sends, so `ReplicaConn::pending_output_bytes` and client output-memory
  snapshots are updated consistently.
- `INFO memory` now exposes Valkey-compatible
  `mem_replication_backlog`, `mem_total_replication_buffers`, and
  `mem_replicas_repl_buffer` fields. In Valdr's current model these are derived
  from the active backlog allocation plus replica client/output memory; a true
  shared pending-replication buffer is still future work.
- `repl_correctness_kit.rs` covers the fan-out accounting path, and
  `info.rs` covers the INFO memory field surface.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit \
  r2_replica_fanout_updates_pending_output_accounting \
  -- --nocapture
cargo test -p redis-commands \
  info::tests::info_memory_exposes_replication_buffer_fields \
  -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit
cargo check -p redis-commands
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r2-buffer-accounting \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-buffer \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r2-buffer-accounting-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- Focused fan-out accounting unit test: passed.
- Focused INFO memory field unit test: passed.
- `repl_correctness_kit`: 18 passed, 0 failed.
- `cargo check -p redis-commands`: passed.
- `cargo build --bin redis-server`: passed.
- Earlier `integration/replication-buffer` run:
  `harness/oracle/results/tcl-survey/20260613T042757054194Z/result.json`
  reported no summary and an exception at `fail to sync with replicas`, before
  the buffer assertions. The later R2-BGSAVE-WINDOW packet moved this frontier
  to counted failures; this earlier result remains evidence only for the
  fan-out accounting packet's original scope.
- Focused dual-server tripwire:
  `harness/oracle/results/tcl-survey/20260613T042825912490Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

### R2-BGSAVE-WINDOW

Status: partial full-sync frontier movement on 2026-06-13.

Implementation:

- `INFO persistence` now reports `rdb_bgsave_in_progress:1` when either the
  ordinary user BGSAVE child or the replication BGSAVE child is active.
- Replication BGSAVE jobs now honor the same bounded `rdb-key-save-delay`
  debug window as ordinary BGSAVE. This makes the `wait_bgsave` / `sync`
  state observable to upstream Tcl tests without relying on the RDB writer
  racing slowly enough.
- `info.rs` has a focused unit test for the replication-BGSAVE INFO flag.

Evidence:

```bash
cargo test -p redis-commands \
  info::tests::info_persistence_counts_replication_bgsave_child \
  -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo check -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-frontier-buffer-bgsave-window \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-buffer \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-frontier-buffer-window-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- Focused INFO persistence test: passed.
- `repl_correctness_kit`: 21 passed, 0 failed.
- `cargo check -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication-buffer`:
  `harness/oracle/results/tcl-survey/20260613T050038752302Z/result.json`
  moved from no-summary setup exception to a counted `3/15` result. Remaining
  failures are now buffer/backlog/partial-resync semantics, not the initial
  BGSAVE sync-window assertion.
- Focused dual-server tripwire:
  `harness/oracle/results/tcl-survey/20260613T050307837025Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

2026-06-13 follow-up finding:

- A per-key `rdb-key-save-delay` experiment confirmed that keeping replication
  BGSAVE alive longer removes the `repl_backlog_histlen` outgrowth assertion
  class, but it is not safe to land as a simple delay change. Uncapped and
  capped variants either timed out the file or converted it to a no-summary
  abort while exposing the deeper partial catch-up / offset-convergence
  frontier.
- Rejected experiment artifacts:
  `harness/oracle/results/tcl-survey/20260613T170027355956Z/result.json`,
  `harness/oracle/results/tcl-survey/20260613T170559847568Z/result.json`, and
  `harness/oracle/results/tcl-survey/20260613T171218725380Z/result.json`.
  Do not reintroduce this as a timing-only fix; the next useful packet needs a
  deterministic full-sync lifecycle kit that can keep the BGSAVE state,
  catch-up history, and replica offset convergence coherent together.

### R2-BUFFER-SHARED-PRIVATE

Status: deterministic kit slice completed on 2026-06-13; Tcl remains at the
stable 4/11 `replication-buffer` baseline.

Implementation:

- `ReplicationState::send_to_replica` now explicitly represents shared
  replication-stream output: bytes are still queued and reported as pending
  replica output, but this path does not enforce the hard output-buffer limit.
- `ReplicationState::send_private_to_replica` is the hard-limit-enforced path
  for explicitly private replica output. Full-sync RDB bulk transfer now uses
  this private path; post-RDB catch-up and normal command fan-out remain on the
  shared path.
- `CONFIG SET client-output-buffer-limit` mirrors the replica hard limit into
  `ReplicationState`, and the connection config test now pins that hot update.
- `repl_buffer_kit` now covers the upstream distinction that shared
  replication history may exceed the private hard limit, while private queued
  output disconnects only the offending replica and leaves healthy replicas
  usable. The kit also includes an explicit drain API guard for future typed
  writer integration.

Evidence:

```bash
rustfmt --check \
  crates/redis-core/src/replication.rs \
  crates/redis-commands/src/client_limits.rs \
  crates/redis-commands/src/config_cmd.rs \
  crates/redis-commands/src/connection.rs \
  crates/redis-commands/tests/repl_buffer_kit.rs \
  crates/redis-server/src/startup.rs
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands \
  connection::tests::client_output_buffer_limit_updates_hot_snapshot \
  -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-buffer \
  --profile integration-repl \
  --runner-id repl-buffer-private-output-stability-rerun \
  --timeout-s 300 \
  --baseport 30179 \
  --portcount 100 \
  --skip-build
```

Results:

- `repl_buffer_kit`: 4 passed, 0 failed.
- Focused config test: passed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication-buffer`:
  `harness/oracle/results/tcl-survey/20260613T172143202124Z/result.json`
  reported 4 passed, 11 failed, 0 timed out, 0 without summary. This is a
  no-regression result against the post-PSYNC 4/11 baseline, not a pass-count
  improvement.

### R2-BUFFER-SHARED-OUTPUT-DRAIN

Status: shared output-memory accounting slice completed on 2026-06-13;
`integration/replication-buffer` moved from 4/11 to 5/10.

Implementation:

- `ReplicaConn` now tracks shared replication-stream output separately from
  explicitly private replica output while preserving the existing per-replica
  pending-output total for client visibility.
- `ReplicationState::replica_output_memory_snapshot` counts shared stream
  bytes once, pinned by the slowest dependent replica, while private output is
  still summed per replica.
- `INFO memory` now derives `mem_replicas_repl_buffer` from the replication
  state's logical shared/private snapshot instead of blindly reusing
  `mem_clients_slaves`.
- Plain TCP and TLS writer loops now call
  `account_replica_output_drained` after successful outbound writes, so healthy
  replicas stop pinning old output memory once the writer drains it.
- `repl_buffer_kit` adds a deterministic case proving shared output is counted
  once, remains pinned while one replica is slow, drains after the last
  dependent replica catches up, and still counts private output per replica.

Evidence:

```bash
rustfmt --check \
  crates/redis-core/src/replication.rs \
  crates/redis-commands/src/info.rs \
  crates/redis-commands/tests/repl_buffer_kit.rs \
  crates/redis-server/src/startup.rs
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands \
  info::tests::info_memory_exposes_replication_buffer_fields \
  -- --nocapture
cargo test -p redis-core replication::tests -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo test -p redis-server -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-shared-output-drain \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-buffer \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-shared-output-drain-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `repl_buffer_kit`: 5 passed, 0 failed.
- Focused INFO memory test: passed.
- Core replication tests: 15 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `redis-server` unit tests: 11 passed, 0 failed.
- `repl_correctness_kit`: 29 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication-buffer`:
  `harness/oracle/results/tcl-survey/20260613T221012325316Z/result.json`
  reported 5 passed, 10 failed, 0 timed out, 0 without summary. The two
  `Replication backlog size can outgrow the backlog limit config` assertions
  are now absent from the failure list. One `Replication buffer will become
  smaller when no replica uses dualchannel no` assertion is now exposed as a
  remaining reclaim/shrink failure.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T221254578531Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- The live server now has enough shared-output accounting and writer-side drain
  behavior for the slow-replica backlog outgrowth class to pass. The next
  `repl_buffer_kit` slice should model reclaim policy after the last dependent
  replica disconnects or catches up, including the distinction between retained
  full-sync history, active catch-up bytes, and per-replica output memory.

### R2-BUFFER-ACTIVE-CATCHUP-RELEASE

Status: active full-sync catch-up reclaim slice completed on 2026-06-13;
`integration/replication-buffer` moved from 5/10 to 6/9.

Implementation:

- When the last replica waiting on an active replication BGSAVE disconnects,
  `remove_repl_bgsave_waiter` now clears the job's `catch_up_bytes`
  immediately.
- The replication BGSAVE job itself remains installed so the reaper can still
  collect the child, clean temp files, and report the useless-child signal
  through the existing lifecycle path.
- `repl_buffer_kit` now covers the active-job variant of the reclaim invariant:
  one remaining waiter keeps active catch-up history readable, while removing
  the last waiter releases the extra history bytes and leaves only the
  circular backlog.

Evidence:

```bash
rustfmt --check \
  crates/redis-core/src/replication.rs \
  crates/redis-commands/tests/repl_buffer_kit.rs
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-core replication::tests -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-active-catchup-release \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-buffer \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-active-catchup-release-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `repl_buffer_kit`: 6 passed, 0 failed.
- Core replication tests: 15 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- `repl_correctness_kit`: 29 passed, 0 failed.
- Focused `integration/replication-buffer`:
  `harness/oracle/results/tcl-survey/20260613T221649651682Z/result.json`
  reported 6 passed, 9 failed, 0 timed out, 0 without summary. The
  `Replication buffer will become smaller when no replica uses dualchannel no`
  assertion is now absent from the failure list.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T221917433378Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- Active catch-up memory no longer waits for child collection once no replica
  can consume it. The remaining shrink failures are in the dual-channel setup
  and in the later slow-replica/output-buffer disconnect path, not this
  active-job no-waiter case.

### R2-BUFFER-ZERO-OFFSET-PSYNC

Status: empty-RDB zero-offset reconnect slice completed on 2026-06-13;
`integration/replication-buffer` moved from 6/9 to 7/8.

Implementation:

- `RdbLoadOutcome` now exposes `keys_loaded` / `keys_expired` to callers that
  need to make post-load replication decisions without parsing the log string.
- Successful replica full-sync RDB replacement marks `PSYNC <cached-replid> 0`
  safe only when the incoming snapshot loaded zero keys.
- The replica dialer keeps the old conservative behavior for normal offset-zero
  cached replids: it sends `PSYNC ? -1` unless the reconnect is manual
  failover, the processed offset is greater than zero, or the last full-sync
  snapshot was empty.
- Target changes and promotion back to primary clear the zero-offset permission
  bit with the rest of the cached partial-resync state.
- `psync_reconnect_kit` adds a master-side regression case proving
  `PSYNC <runid> 0` can return `+CONTINUE` and replay retained backlog bytes
  when the primary really has history after offset zero.
- A 2026-06-14 follow-up made the command-level PSYNC decision matrix explicit:
  `PSYNC <runid> 0` with no readable history and no empty-RDB permission must
  fall back to full resync, while the empty-history permission allows the
  caught-up offset-zero case.

Evidence:

```bash
rustfmt \
  crates/redis-core/src/rdb/load.rs \
  crates/redis-core/src/replication.rs \
  crates/redis-commands/src/replica_dialer.rs \
  crates/redis-commands/tests/psync_reconnect_kit.rs \
  crates/redis-server/src/runtime_owner.rs
cargo test -p redis-commands replica_dialer -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-zero-offset-psync-narrow2 \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-buffer \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-zero-offset-psync-psync360-narrow2 \
  --profile integration-repl \
  --timeout-s 360 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-psync \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-zero-offset-psync-psync360-conservative-compare \
  --profile integration-repl \
  --timeout-s 360 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-psync \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-zero-offset-psync-tripwire2 \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `replica_dialer` tests: 9 passed, 0 failed.
- `psync_reconnect_kit`: 9 passed, 0 failed.
- `repl_buffer_kit`: 6 passed, 0 failed.
- `repl_correctness_kit`: 29 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication-buffer`:
  `harness/oracle/results/tcl-survey/20260613T224648237767Z/result.json`
  reported 7 passed, 8 failed, 0 timed out, 0 without summary. The
  `Partial resynchronization is successful even client-output-buffer-limit is
  less than repl-backlog-size. dualchannel yes` assertion is now absent from
  the failure list; the `dualchannel no` variant remains red.
- Focused `integration/replication-psync` with the scoped zero-offset selector:
  `harness/oracle/results/tcl-survey/20260613T224913822348Z/result.json`
  timed out at 360 seconds with master/replica inconsistency lines before a
  summary.
- Conservative-selector comparison for `integration/replication-psync`:
  `harness/oracle/results/tcl-survey/20260613T225613931557Z/result.json`
  also timed out at 360 seconds with the same class of inconsistency lines.
  That makes PSYNC a reopened current R3 frontier, but it does not implicate
  this packet's offset-zero selector.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T230647306243Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- Offset zero is not one state. A cached replid at offset zero is unsafe after a
  non-empty full-sync snapshot because snapshot state is not represented in the
  replication stream. It is safe after an empty full-sync snapshot because the
  primary can replay retained bytes after offset zero without duplicating
  preexisting keyspace. The remaining non-dual-channel buffer failure likely
  needs a separate output-buffer/disconnect or counter slice, not a broader
  PSYNC selector.

### R3-FULLSYNC-DETACHED-CATCHUP-TAIL

Status: deterministic full-sync tail gap fixed on 2026-06-14; use kits as the
inner loop before rerunning the slow PSYNC Tcl matrix.

Implementation:

- `fullsync_lifecycle_kit` now covers the reaper detach window: the server
  takes the active replication BGSAVE job before reading and shipping the temp
  RDB, but client writes can still append to the backlog before transfer
  completion.
- `complete_repl_bgsave_transfer` now builds catch-up from both sources: the
  detached job's `catch_up_bytes` plus the live backlog tail from the point
  where the detached buffer ends through the current master offset.
- The completion path requires the whole catch-up range to be readable before
  delivering it, instead of silently preferring a stale detached buffer.

Evidence:

```bash
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
```

Results:

- `fullsync_lifecycle_kit`: 11 passed, 0 failed. The new
  `fullsync_completion_includes_backlog_tail_after_job_detaches` case failed
  before the fix with `retained_catchup_len == 1` instead of `2`.
- `psync_reconnect_kit`: 9 passed, 0 failed.
- `repl_buffer_kit`: 6 passed, 0 failed.
- `repl_correctness_kit`: 29 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- Prior oracle rerun, not part of the normal inner loop:
  `harness/oracle/results/tcl-survey/20260614T025006301487Z/result.json`
  still timed out at 540 seconds, but the first earlier failure
  `no reconnection, just sync (diskless: no, disabled, dual-channel: no,
  reconnect: 0)` disappeared. The latest `/tmp/repldump*.txt` diff from that
  run showed one remaining data difference: DB 9 key `316637927` was string
  `0` on the master and `-0` on the replica.

Takeaway:

- This packet fixes a real full-sync lifecycle race and narrows the PSYNC
  frontier. The next packet did build the small `0` vs `-0` family kit and
  found an RDB raw numeric-string fidelity bug, reinforcing that the slow Tcl
  matrix should stay a scoreboard rather than the debugger loop.

### R3-RDB-NUMERIC-STRING-FIDELITY

Status: deterministic `0` / `-0` family kit fixed on 2026-06-14; no slow Tcl
rerun in this packet.

Implementation:

- `repl_correctness_kit` now has a RESP stream apply helper that feeds raw
  replication bytes through the same parser and replica-apply dispatch shape as
  the runtime owner.
- New DB 9 probes cover:
  - partial catch-up after `SELECT 9; SET key -0`, proving a bare later
    `SET key 0` applies to the preserved selected DB;
  - primary propagation capture for `SELECT 9; SET key -0; SET key 0`,
    proving the emitted stream replays to DB 9;
  - full-sync RDB reconstruction where the RDB contains `-0` and catch-up
    later sets `0`.
- `psync_reconnect_kit` now covers reconnecting from the offset after the DB 9
  `-0` frame and replaying only the later `SET key 0` frame.
- RDB raw string loading now uses the runtime string encoder, and
  `is_canonical_i64_ascii` now requires byte-for-byte integer round-trip
  formatting. This keeps raw `-0` as bytes while still promoting canonical `0`
  to integer encoding.

Evidence:

```bash
cargo test -p redis-core rdb::string -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
```

Results:

- `rdb::string`: 10 passed, 0 failed. The new `raw_minus_zero_roundtrip_stays_raw_bytes`
  case guards against `-0` becoming integer `0`.
- `repl_correctness_kit`: 32 passed, 0 failed. The new full-sync reconstruction
  case failed before the fix because the loaded RDB value was `0` instead of
  `-0`.
- `psync_reconnect_kit`: 10 passed, 0 failed.
- `fullsync_lifecycle_kit`: 11 passed, 0 failed.
- `repl_buffer_kit`: 6 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.

Takeaway:

- The random complex-data generator can produce both `0` and `-0`; persistence,
  full sync, and catch-up must preserve those bytes exactly until a later
  command overwrites them. The kit found a real RDB loader mismatch without
  another long Tcl run.

### R2-BUFFER-DUAL-CHANNEL-ACCOUNTING-KIT

Status: deterministic dual-channel INFO-memory kit completed on 2026-06-14;
the long `integration/replication-buffer` Tcl file was intentionally deferred
as a scoreboard.

Implementation:

- `LiveConfig` now stores `dual-channel-replication-enabled`, defaulting to
  `yes` to match the existing config default.
- `CONFIG SET` / `CONFIG GET` now mutate and expose the live dual-channel
  value, and invalid non yes/no values are rejected.
- `ReplicationState` now distinguishes raw full-sync history retained in
  memory from the bytes that INFO should charge to normal replication buffers.
- With dual-channel enabled, active RDB full-sync catch-up bytes are no longer
  charged to `mem_replication_backlog`; retained post-transfer history still
  counts because it can satisfy PSYNC. With dual-channel disabled, the previous
  conservative accounting remains.
- `repl_buffer_kit` now covers the exact distinction that caused the first
  focused `replication-buffer` failure: active full-sync catch-up exists, but
  dual-channel INFO accounting must not inflate the normal replication-buffer
  total.

Evidence:

```bash
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands info:: -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
```

Results:

- `repl_buffer_kit`: 8 passed, 0 failed.
- `redis-commands info::`: 3 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.

Takeaway:

- This is the desired kit-first replacement for using the 100+ second Tcl file
  as the debugger. The next scoreboard run should use the focused
  `integration/replication-buffer` gate only after the next buffer slice is
  also kit-green.

### R2-BUFFER-INFO-OUTPUT-SPLIT

Status: focused `integration/replication-buffer` moved from 7/8 to 8/7 on
2026-06-14.

Implementation:

- `INFO memory` now keeps ordinary replica client output out of
  `mem_total_replication_buffers`.
- `mem_replicas_repl_buffer` is currently `0` because the Rust port does not
  yet model Valkey's dual-channel replica-side `pending_repl_data` buffer as a
  separate structure.
- Replica output remains visible under `mem_clients_slaves` / client memory;
  it no longer inflates Valkey-style replication-buffer fields.
- `repl_buffer_kit` now explicitly covers active full-sync catch-up making
  readable history outgrow the circular backlog, then shrinking back to the
  circular backlog after the last waiting replica disconnects.

Evidence:

```bash
cargo test -p redis-commands info:: -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-info-field-split \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --skip-build \
  --files integration/replication-buffer
```

Results:

- `redis-commands info::`: 3 passed, 0 failed.
- `repl_buffer_kit`: 9 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- Focused Tcl artifact:
  `harness/oracle/results/tcl-survey/20260614T033242068928Z/result.json`.
- `integration/replication-buffer`: 8 passed, 7 failed, no timeout.

Takeaway:

- The prior `All replicas share one global replication buffer dualchannel yes`
  memory assertion is gone, and the following shrink test is now green. The
  remaining failure in that first test is topology/counting: dual-channel
  expects the syncing replica to expose an extra `type=rdb-channel` connection
  in `connected_slaves`.

### R2-BUFFER-DUAL-CHANNEL-INFO-TOPOLOGY

Status: focused `integration/replication-buffer` moved from 8/7 to 9/6 on
2026-06-14.

Implementation:

- `INFO replication` now includes a `type=replica` field on ordinary replica
  lines, matching the shape Valkey uses for replication client type.
- When `dual-channel-replication-enabled yes` and a replica is waiting for
  BGSAVE full sync, INFO adds one provisional `type=rdb-channel` line for that
  waiting replica and includes it in `connected_slaves`.
- This is an observability shim only: the Rust port still sends the actual RDB
  through the ordinary full-sync owner, and real dual-channel transport remains
  future work.
- The INFO unit tests now serialize global replication-state mutations and
  cover the provisional `rdb-channel` count/line explicitly.

Evidence:

```bash
cargo test -p redis-commands info:: -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-rdb-channel-info \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --skip-build \
  --files integration/replication-buffer
```

Results:

- `redis-commands info::`: 4 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- Focused Tcl artifact:
  `harness/oracle/results/tcl-survey/20260614T033956189569Z/result.json`.
- `integration/replication-buffer`: 9 passed, 6 failed, no timeout.

Takeaway:

- The first dual-channel global-buffer group is now green. Remaining
  `replication-buffer` failures are in the later partial-resync beyond backlog
  and low-output-buffer PSYNC counter sections.

### R2-BUFFER-TX-CATCHUP-APPLY

Status: focused `integration/replication-buffer` stayed at 9/6 on 2026-06-14,
but the failing low-output-buffer PSYNC counter moved from non-dual-channel to
dual-channel.

Implementation:

- RuntimeOwner replica apply now uses one pseudo-client for a parsed
  `CommandBatch`, so split `MULTI ... EXEC` catch-up preserves transaction
  state instead of applying `EXEC` with a fresh client.
- The replica dialer keeps parsed command frames buffered across socket-read
  boundaries while a transaction envelope is open; this prevents a large
  backlog catch-up from flushing after `MULTI` but before `EXEC`.
- Replication-apply transaction propagation now suppresses downstream fan-out,
  matching the existing top-level replication-apply write path.
- The RuntimeOwner apply wait budget is now named and long enough for upstream
  `DEBUG SLEEP 2` catch-up scenarios.
- `psync_reconnect_kit` adds a zero-histlen killed-last-replica case proving
  the backlog TTL window remains active after immediate CLIENT KILL cleanup.

Evidence:

```bash
cargo test -p redis-server runtime_owner::tests::replica_apply_batch_preserves_multi_state_until_exec -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo check -p redis-commands
cargo build -p redis-server --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-tx-catchup-batch-retry \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --skip-build \
  --files integration/replication-buffer
```

Results:

- RuntimeOwner transaction batch unit: 1 passed, 0 failed.
- `replica_dialer::tests`: 10 passed, 0 failed.
- `psync_reconnect_kit`: 11 passed, 0 failed.
- `repl_buffer_kit`: 10 passed, 0 failed.
- `cargo check -p redis-commands`: passed.
- `cargo build -p redis-server --bin redis-server`: passed.
- Focused Tcl artifact:
  `harness/oracle/results/tcl-survey/20260614T043523717635Z/result.json`.
- `integration/replication-buffer`: 9 passed, 6 failed, no timeout.

Takeaway:

- A two-server live probe for the upstream low-output-buffer command ordering
  now ends at `sync_full=1`, `sync_partial_ok=1`, `sync_partial_err=0` in
  non-dual-channel mode; before the packet it reproduced the Tcl failure with
  `sync_partial_ok=2`.
- The same Tcl section now fails only in dual-channel mode with actual
  `sync_partial_ok=1` vs expected `2`. The next kit should model Valkey's
  dual-channel fake/main PSYNC accounting explicitly instead of relying on a
  reconnect loop to create the second counter increment.

### R2-BUFFER-DUAL-PSYNC-ACCOUNTING

Status: focused `integration/replication-buffer` moved from 9/6 to 12/4 on
2026-06-14.

Implementation:

- `REPLCONF capa dual-channel` now maps to the Valkey-compatible capability bit
  and remains visible to the following `PSYNC` before the replica is registered.
- The replica dialer advertises `dual-channel` only when its live config has
  `dual-channel-replication-enabled yes`.
- A dual-capable full-sync request on a dual-enabled master now accounts the
  logical main-channel successful PSYNC that Valkey performs after the separate
  RDB channel loads. The Rust port still transfers the RDB through the ordinary
  full-sync path; this is explicit compatibility accounting, not full
  dual-channel transport.

Evidence:

```bash
cargo test -p redis-commands --test psync_reconnect_kit dual_channel -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo check -p redis-commands
cargo build -p redis-server --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-dual-psync-accounting \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --skip-build \
  --files integration/replication-buffer
```

Results:

- `psync_reconnect_kit dual_channel`: 2 passed, 0 failed.
- `psync_reconnect_kit`: 13 passed, 0 failed.
- `replica_dialer::tests`: 10 passed, 0 failed.
- `repl_buffer_kit`: 10 passed, 0 failed.
- `cargo check -p redis-commands`: passed.
- `cargo build -p redis-server --bin redis-server`: passed.
- Two-server live probe: dual initial sync ended at `sync_full=1`,
  `sync_partial_ok=1`, `sync_partial_err=0`; after `CLIENT KILL TYPE REPLICA`
  and partial reconnect it ended at `sync_full=1`, `sync_partial_ok=2`,
  `sync_partial_err=0`.
- Focused Tcl artifact:
  `harness/oracle/results/tcl-survey/20260614T044906327151Z/result.json`.
- `integration/replication-buffer`: 12 passed, 4 failed, no timeout.

Takeaway:

- Both low-output-buffer dual-channel PSYNC counter failures cleared. The
  remaining four failures are the dual/non-dual pair for partial resync beyond
  configured backlog and the dual/non-dual pair for backlog-memory shrink after
  slow-replica disconnect.

### R2-BUFFER-FULLSYNC-DRAIN-VISIBILITY

Status: kit-first slice completed on 2026-06-14; focused Tcl scoreboard stayed
at 12/4 after this packet.

Implementation:

- Master-side full-sync waiters remain in `send_bulk` after the RDB/catch-up
  payload is queued and move to `online` only when pending replica output drains
  to zero.
- RuntimeOwner plain-TCP and TLS write paths now report replica bytes consumed
  from the slot write buffer to `ReplicationState::account_replica_output_drained`.
- Replica-side ROLE stays `sync` after a full resync until the primary stream
  reaches an idle read, avoiding an early `connected` state while catch-up bytes
  are still being applied.
- INFO persistence treats queued full-sync output as an in-progress full-sync
  transfer, and INFO replication exposes provisional dual-channel `rdb-channel`
  lines for both `wait_bgsave` and `send_bulk`.
- `repl_buffer_kit` now proves active full-sync catch-up history can satisfy an
  online replica reconnect while another full-sync waiter still pins that
  history.

Evidence:

```bash
cargo test -p redis-server \
  runtime_owner::tests::replica_slot_write_drain_updates_replication_pending_output \
  -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands info::tests -- --nocapture
cargo check -p redis-commands -p redis-server
cargo build -p redis-server --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-fullsync-drain-visibility \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --skip-build \
  --files integration/replication-buffer
```

Results:

- RuntimeOwner replica drain kit: 1 passed, 0 failed.
- `replica_dialer::tests`: 11 passed, 0 failed.
- `fullsync_lifecycle_kit`: 11 passed, 0 failed.
- `repl_buffer_kit`: 11 passed, 0 failed.
- `info::tests`: 4 passed, 0 failed.
- `cargo check -p redis-commands -p redis-server`: passed.
- `cargo build -p redis-server --bin redis-server`: passed.
- Two-server live probe with `CONFIG SET rdb-key-save-delay 1000000` and a
  large post-BGSAVE catch-up tail: 179 samples, saw replica ROLE `sync`, and
  settled at master `rdb_bgsave_in_progress:0`, `connected_slaves:1`, replica
  ROLE `connected`, and no lingering `state=send_bulk` line.
- Focused Tcl artifact:
  `harness/oracle/results/tcl-survey/20260614T052444531017Z/result.json`.
- `integration/replication-buffer`: 12 passed, 4 failed, no timeout.

Takeaway:

- The useful debugger here is the kit ladder, not the full Tcl file. The
  remaining frontier is the shared-history / backlog-shrink pair in the slow
  replica scenario.
- A follow-up experiment that kept the debug-delayed BGSAVE job installed until
  active catch-up went idle was rejected and reverted. Focused artifact
  `harness/oracle/results/tcl-survey/20260614T053600956113Z/result.json`
  regressed to no-summary after 196s with `Replica offset didn't catch up with
  the master after too long time.` The next attempt needs a kit that proves
  online replica catch-up throughput while the full-sync waiter pins large
  history; a state-lifetime hold alone is not acceptable.

### R2-BUFFER-LARGE-CATCHUP-BATCHING

Status: throughput prerequisite completed on 2026-06-14; focused Tcl scoreboard
stayed at 12/4.

Implementation:

- The replica dialer now reads primary stream catch-up with a 1 MiB buffer
  instead of 8 KiB.
- `replica_dialer::tests::large_partial_resync_commands_batch_by_read_window`
  models the `replication-buffer` workload shape: many 10 KiB `SET` frames
  should be applied in a small number of RuntimeOwner batches instead of one
  apply roundtrip per command.

Evidence:

```bash
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo check -p redis-commands -p redis-server
cargo build -p redis-server --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-large-catchup-batch \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --skip-build \
  --files integration/replication-buffer
```

Results:

- `replica_dialer::tests`: 12 passed, 0 failed.
- `repl_buffer_kit`: 11 passed, 0 failed.
- `psync_reconnect_kit`: 13 passed, 0 failed.
- `cargo check -p redis-commands -p redis-server`: passed.
- `cargo build -p redis-server --bin redis-server`: passed.
- Focused Tcl artifact:
  `harness/oracle/results/tcl-survey/20260614T054522469720Z/result.json`.
- `integration/replication-buffer`: 12 passed, 4 failed, no timeout.

Takeaway:

- This does not solve the remaining state-lifetime assertions by itself, but it
  removes the one-command-per-large-frame apply bottleneck that made the rejected
  BGSAVE-window hold regress to a replica catch-up abort. The next window fix
  should use this batching kit as a prerequisite.

### R2-BUFFER-OPEN-RETAINED-STREAM

Status: kit-first shared-stream slice completed on 2026-06-14; focused Tcl was
deferred intentionally.

Implementation:

- Retained full-sync history segments are now marked open while an owner still
  pins them, so later replication stream bytes extend the same shared readable
  range instead of leaving only the circular backlog after the RDB/catch-up
  payload is queued.
- Command propagation now fans out to replicas in `SendingRdb` as well as
  `Online`; a full-sync replica with RDB bytes queued is still consuming the
  command stream.
- `repl_buffer_kit` now covers both sides directly: the retained range grows
  past the configured backlog until the owner disconnects, and a synthetic
  `SendingRdb` replica receives newly propagated RESP command bytes.

Evidence:

```bash
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
```

Results:

- `repl_buffer_kit`: 13 passed, 0 failed.
- `psync_reconnect_kit`: 13 passed, 0 failed.
- `replica_dialer::tests`: 12 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.

Takeaway:

- We do not need the long `integration/replication-buffer` Tcl command for the
  inner loop. These kits are the fast proof for the remaining shared-history
  semantics; the Tcl file should be rerun only as an outer scoreboard after a
  coherent slice changes the expected count.

### R2-BUFFER-SHARED-LAG-LIMIT-CLOSE

Status: prerequisite correctness slice completed on 2026-06-14; focused Tcl
scoreboard stayed at 12/4.

Implementation:

- Replica output-buffer hard-limit checks now use Valkey's effective replica
  limit: a nonzero configured hard limit below `repl-backlog-size` is floored
  at the backlog size.
- Shared replication-stream bytes now participate in that effective limit. A
  slow replica whose shared lag grows beyond the effective limit is removed
  and releases any retained full-sync history it pinned.
- Output-limit disconnects send the empty outbound payload used by both live
  server paths as the writer close sentinel before removing replica metadata.
  A no-sentinel variant was rejected because it only removed bookkeeping and
  left live replica reconnect behavior broken.
- `psync_reconnect_kit` now proves that `PSYNC +CONTINUE` can replay catch-up
  bytes from retained full-sync history beyond the circular backlog, separating
  range/decision coverage from live 100 MB apply timing.

Evidence:

```bash
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build -p redis-server --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-shared-lag-limit-close \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --skip-build \
  --files integration/replication-buffer
```

Results:

- `repl_buffer_kit`: 14 passed, 0 failed.
- `psync_reconnect_kit`: 14 passed, 0 failed.
- `replica_dialer::tests`: 12 passed, 0 failed.
- `fullsync_lifecycle_kit`: 11 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build -p redis-server --bin redis-server`: passed.
- Focused Tcl artifact:
  `harness/oracle/results/tcl-survey/20260614T060739867825Z/result.json`.
- `integration/replication-buffer`: 12 passed, 4 failed, no timeout.

Rejected experiment:

- `harness/oracle/results/tcl-survey/20260614T060303623049Z/result.json`
  regressed the same focused file to 8/6 because output-limit disconnects did
  not signal the live writer to close. The accepted implementation keeps the
  close sentinel in the kit.

Takeaway:

- The remaining four failures are no longer explained by PSYNC retained-range
  availability or by the output-limit close hook. The next useful kit should
  model live replica catch-up/offset advancement for the 100 MB reconnect while
  another `send_bulk` owner pins the shared retained history.

### R2-BGSAVE-CATCHUP

Status: active-job catch-up foundation completed on 2026-06-13; slow Tcl
remains red at the same counted `3/15` frontier.

Implementation:

- `ReplBgsaveJob` now owns a shared `catch_up_bytes` buffer.
- Every replication backlog append also copies bytes into the active
  replication-BGSAVE job, if one exists.
- Full-sync transfer sends the job's catch-up bytes after the RDB payload
  instead of relying only on the circular backlog.
- Partial resync catch-up now reads through `ReplicationState::read_history_at`,
  which can serve bytes from either the circular backlog or the active BGSAVE
  catch-up buffer.
- `INFO memory` accounts the active BGSAVE catch-up buffer as replication
  backlog memory.

Evidence:

```bash
cargo test -p redis-core replication::tests -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-frontier-buffer-catchup-no-resize \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-buffer \
  --isolated-tests-copy \
  --skip-build
```

Results:

- Core replication tests: 9 passed, 0 failed, including
  `bgsave_catchup_extends_history_beyond_circular_backlog`.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `repl_correctness_kit`: 21 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication-buffer`:
  `harness/oracle/results/tcl-survey/20260613T052351201016Z/result.json`
  reported the same counted `3/15` result with 0 timeouts, 0 no-summary files,
  and 0 abort/exception points.

Takeaway:

- A runtime `CONFIG SET repl-backlog-size` resize probe was intentionally not
  kept in this packet. With the current short-lived shared buffer, the probe
  regressed `integration/replication-buffer` back to a no-summary
  `Replica offset didn't catch up with the master after too long time`
  exception:
  `harness/oracle/results/tcl-survey/20260613T051700796223Z/result.json`.
  R3-BACKLOG-RESIZE needs longer-lived shared-buffer retention and
  offset-convergence work before it is safe to expose.

### R2-RETAINED-FULLSYNC-HISTORY

Status: first retained-history slice completed on 2026-06-13;
`integration/replication-buffer` improved from 3/15 to 4/15 but remains red.

Implementation:

- Added `RetainedReplHistory`, an immutable shared replication-history segment
  retained after a full-sync BGSAVE job has been consumed.
- Full-sync transfer retains the completed catch-up bytes for replicas that
  successfully had the RDB plus catch-up stream queued.
- `REPLCONF ACK` releases a replica's retained-history pin once it has consumed
  through the end of a retained segment; replica disconnect releases all of
  that replica's pins.
- `ReplicationState::read_history_at` can stitch retained full-sync history,
  active BGSAVE catch-up bytes, and the circular backlog without cloning large
  buffers for PSYNC decisions.
- `INFO memory` counts retained full-sync history once as replication history
  rather than once per dependent replica.
- New deterministic `repl_buffer_kit.rs` covers retained history surviving job
  completion, partial-resync reads from retained history plus backlog, release
  on ACK/disconnect, one-copy accounting, and gap-aware PSYNC range coverage.

Evidence:

```bash
cargo test -p redis-core replication::tests -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-retained-history-v2 \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-buffer \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-buffer-retained-history-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- Core replication tests: 9 passed, 0 failed.
- `repl_buffer_kit`: 3 passed, 0 failed.
- `repl_correctness_kit`: 21 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication-buffer`:
  `harness/oracle/results/tcl-survey/20260613T135020603220Z/result.json`
  reported 4 passed, 11 failed, 0 timed out, 0 without summary, and 0
  abort/exception points.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T135331346121Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- A first implementation used the right retained-history state but cloned large
  catch-up buffers while deciding whether PSYNC history was available. The
  focused Tcl run exposed that as a no-summary offset-convergence regression:
  `harness/oracle/results/tcl-survey/20260613T134424827847Z/result.json`.
  The kept implementation checks interval coverage without copying payload
  bytes, then materializes bytes only for the actual replay send.
- The next useful `repl_buffer_kit` slice is broader online-replica shared
  buffer ownership: retained full-sync history is now real, but slow online
  replica output still does not make `repl_backlog_histlen` outgrow the
  configured circular backlog the way Valkey's global replication buffer does.

### R2-FULLSYNC-LIFECYCLE-CLEANUP

Status: first failed-job cleanup slice completed on 2026-06-13;
`integration/replication` moves past the killed-child collection frontier but
remains red at the later script-busy full-sync apply case.

Implementation:

- Added `ReplicationState::cleanup_failed_repl_bgsave_job` to drop waiting
  replica records and remove both final and side temp RDB paths for failed
  replication BGSAVE jobs.
- Added `ReplicationState::abort_repl_bgsave_job` to consume the installed
  job, run failed-job cleanup, and clear `repl_child_pid`.
- The replication BGSAVE reaper now aborts failed child jobs through that shared
  cleanup path for `waitpid` errors and nonzero child exits.
- Full-sync transfer read failures now drop stale waiters and temp files instead
  of only removing the temp RDB.
- The non-Unix/thread fallback path also aborts the replication job on save or
  thread-spawn failure.
- New deterministic `fullsync_lifecycle_kit.rs` proves a failed full-sync job
  cleans waiters, removes temp files, clears child state, and allows a later
  job to install cleanly.

Evidence:

```bash
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo test -p redis-core replication::tests -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-fullsync-lifecycle-cleanup \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-fullsync-lifecycle-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `fullsync_lifecycle_kit`: 1 passed, 0 failed.
- Core replication tests: 9 passed, 0 failed.
- `repl_correctness_kit`: 21 passed, 0 failed.
- `repl_buffer_kit`: 3 passed, 0 failed.
- `psync_reconnect_kit`: 4 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T140356504725Z/result.json`
  still produced no parsed summary, but it moved from
  `diskless replication child being killed is collected` /
  `child process exited abnormally` to
  `Master stream is correctly processed while the replica has a script in -BUSY state`
  with `READONLY You can't write against a read only replica..`.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T140731144046Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- Failed replication BGSAVE jobs now have a single cleanup path that removes
  stale full-sync waiters and child state. The next useful
  `fullsync_lifecycle_kit` slice is script-busy full-sync application: the
  replica must process the primary stream around `-BUSY` without issuing writes
  through the normal read-only command path.

### 2026-06-13 R2 follow-up: atomic incoming replica RDB replacement

Scope:

- Added `redis_core::rdb::load_into_dbs_replacing`, an all-or-nothing load
  helper that stages a full incoming RDB into fresh logical DBs and swaps it
  into the caller only after the entire file validates and loads.
- Changed the replica runtime-owner full-sync apply path to use that helper
  instead of clearing the live replica keyspace before calling the incremental
  RDB loader.
- Extended `fullsync_lifecycle_kit.rs` with a deterministic case proving a
  corrupt incoming full-sync RDB leaves the existing replica data intact, while
  a valid incoming RDB replaces the old dataset and drops keys absent from the
  snapshot.

Evidence:

```bash
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
rustfmt --check \
  crates/redis-core/src/rdb/load.rs \
  crates/redis-core/src/rdb/mod.rs \
  crates/redis-server/src/runtime_owner.rs \
  crates/redis-commands/tests/fullsync_lifecycle_kit.rs
cargo test -p redis-core rdb::load -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication \
  --profile integration-repl \
  --runner-id fullsync-atomic-rdb \
  --timeout-s 300 \
  --baseport 30279 \
  --portcount 100 \
  --skip-build
```

Results:

- `fullsync_lifecycle_kit`: 2 passed, 0 failed.
- Core RDB load tests: 5 passed, 0 failed.
- Targeted `rustfmt --check`: passed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T173112222872Z/result.json`
  completed without timing out but produced no parsed summary. It still aborts
  at `Master stream is correctly processed while the replica has a script in
  -BUSY state` with
  `READONLY You can't write against a read only replica..`, plus 19 parsed
  failure lines before the abort.

Takeaway:

- Replica full-sync application now has an atomic keyspace replacement boundary
  for corrupt or short incoming RDBs. This completes the
  `fullsync_lifecycle_kit` case "replica full-sync failure does not replace
  good old data unless the incoming RDB is valid." The follow-up
  script-readonly packet below moved the broader `integration/replication`
  gate past the script-busy stream apply abort; the next full-sync lifecycle
  slice is now diskless swapdb / async-loading state.

### 2026-06-13 R2 follow-up: replica script readonly boundaries

Scope:

- Relaxed the non-shebang EVAL preflight on read-only replicas so ordinary
  no-write scripts can execute locally. Actual script writes are still rejected
  at `redis.call` / `redis.pcall` command re-entry.
- Extended the same script-readonly predicate to exempt the primary-link
  `replication_apply` pseudo-client, matching the generic dispatch read-only
  guard. This lets commands received from the upstream primary apply locally
  without tripping client-facing read-only replica errors.
- Extended `fullsync_lifecycle_kit.rs` to cover all three boundaries:
  ordinary no-write EVAL is allowed on a read-only replica, ordinary script
  writes are rejected, and primary-link script writes apply to the replica DB.

Evidence:

```bash
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
rustfmt --check \
  crates/redis-commands/src/eval.rs \
  crates/redis-commands/tests/fullsync_lifecycle_kit.rs
cargo test -p redis-commands eval::tests -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication \
  --profile integration-repl \
  --runner-id fullsync-script-readonly \
  --timeout-s 300 \
  --baseport 30479 \
  --portcount 100 \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-2,integration/block-repl \
  --profile integration-repl \
  --runner-id fullsync-script-readonly-tripwire \
  --timeout-s 240 \
  --baseport 30579 \
  --portcount 100 \
  --skip-build
```

Results:

- `fullsync_lifecycle_kit`: 3 passed, 0 failed.
- `eval::tests`: 28 passed, 0 failed.
- Targeted `rustfmt --check`: passed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T174350907420Z/result.json`
  completed without timing out but produced no parsed summary. It moved from
  the previous `Master stream is correctly processed while the replica has a
  script in -BUSY state` READONLY abort to
  `Diskless load swapdb (async_loading): new database is exposed after
  swapping`, still with a READONLY exception and 21 parsed failure lines.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T174744082067Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- The script-busy replication frontier was a client-facing EVAL preflight
  problem, not only a primary-link apply problem. Non-writing scripts on
  read-only replicas now run, script writes are still blocked for ordinary
  clients, and primary-link script writes apply locally. The next
  `fullsync_lifecycle_kit` slice should model diskless swapdb / async-loading
  role state, especially the FCALL/read-only exception in the new abort test.

### 2026-06-13 R2 follow-up: writable-replica FCALL preflight

Scope:

- Made FCALL and shebang-EVAL script read-only preflight honor
  `replica-read-only no`, matching the generic write-command gate.
- Applied the same shebang-EVAL condition in the `lua-rs` backend path so the
  future Lua engine swap keeps the same replica-writable semantics.
- Extended `fullsync_lifecycle_kit.rs` with a function case matching the Tcl
  diskless swapdb frontier: a function loaded before demotion remains blocked
  while `replica-read-only yes`, then runs once live config flips to
  writable-replica mode.

Evidence:

```bash
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
rustfmt --check \
  crates/redis-commands/src/eval.rs \
  crates/redis-commands/src/eval/lua_rs_backend.rs \
  crates/redis-commands/tests/fullsync_lifecycle_kit.rs
cargo test -p redis-commands eval::tests -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication \
  --profile integration-repl \
  --runner-id fullsync-writable-fcall \
  --timeout-s 300 \
  --baseport 30679 \
  --portcount 100 \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-2,integration/block-repl \
  --profile integration-repl \
  --runner-id fullsync-writable-fcall-tripwire \
  --timeout-s 240 \
  --baseport 30779 \
  --portcount 100 \
  --skip-build
```

Results:

- `fullsync_lifecycle_kit`: 4 passed, 0 failed.
- `eval::tests`: 28 passed, 0 failed.
- Targeted `rustfmt --check`: passed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T175145745365Z/result.json`
  completed without timing out but produced no parsed summary. It moved from
  the previous `$replica fcall test 0` READONLY exception in
  `Diskless load swapdb (async_loading): new database is exposed after
  swapping` to a later async-loading aborted-branch exception:
  `Replica didn't disconnect`, with 25 parsed failure lines.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T175600712511Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- Writable replicas now consistently bypass script/function preflight
  READONLY checks while ordinary read-only replicas remain protected. The next
  `fullsync_lifecycle_kit` slice should focus on explicit async-loading
  abort/disconnect state: when the master kills the replica connection during
  swapdb loading, Valdr must clear async-loading/loading state and expose the
  old dataset.

### 2026-06-13 R2 follow-up: async-loading state surface

Scope:

- Added an explicit `async_loading` bit to `PersistenceState`. Setting it also
  marks the server as loading internally; clearing ordinary loading clears both
  bits.
- `INFO persistence` now reports `async_loading:1` while hiding ordinary
  `loading:1` during async loading, matching the swapdb model where the old
  dataset remains visible.
- Dispatch now honors the command table's `NO_ASYNC_LOADING` flag separately
  from ordinary loading. Normal reads can continue during async loading, while
  unsafe commands still receive `-LOADING`.
- Replica full-sync now publishes async-loading state for same-primary
  replacement sync attempts and clears it on success, short read, load failure,
  or canceled epoch.
- `CONFIG SET lua-time-limit` and `busy-reply-threshold` remain available
  during async loading so script-busy replication tests can tune the server,
  while dangerous config such as `appendonly` stays blocked.
- Extended `fullsync_lifecycle_kit.rs` with a deterministic async-loading case
  proving old data remains readable, INFO exposes the right bits,
  `NO_ASYNC_LOADING` commands are blocked, safe script timeout config works,
  and clearing loading clears async-loading too.

Evidence:

```bash
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
rustfmt --check \
  crates/redis-core/src/persistence.rs \
  crates/redis-commands/src/info.rs \
  crates/redis-commands/src/dispatch.rs \
  crates/redis-commands/src/replica_dialer.rs \
  crates/redis-commands/src/connection.rs \
  crates/redis-commands/tests/fullsync_lifecycle_kit.rs
cargo test -p redis-commands eval::tests -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication \
  --profile integration-repl \
  --runner-id fullsync-async-loading-config \
  --timeout-s 300 \
  --baseport 31079 \
  --portcount 100 \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-2,integration/block-repl \
  --profile integration-repl \
  --runner-id fullsync-async-loading-tripwire \
  --timeout-s 240 \
  --baseport 31179 \
  --portcount 100 \
  --skip-build
```

Results:

- `fullsync_lifecycle_kit`: 5 passed, 0 failed.
- `eval::tests`: 28 passed, 0 failed.
- Targeted `rustfmt --check`: passed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T180644028410Z/result.json`
  timed out at 300 seconds with no parsed summary. It produced 26 parsed
  failure lines, no exception, and no `abort_test`. This is not a pass, but it
  moved past the earlier LOADING exceptions around async-loading script/config
  handling.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T181317455732Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- Async-loading is now represented as a first-class state rather than an
  accidental ordinary loading mode. The next full-sync lifecycle slice should
  attack true diskless swapdb replacement: stage the incoming dataset away from
  the live old DB, swap atomically on success, and make short-read/drop paths
  clear replica link/loading state without leaving Tcl waiters hanging.

### 2026-06-13 R2 follow-up: full-sync function payload replacement

Scope:

- Added optional opaque `FUNCTION2` payload support to the native RDB writer.
  `redis-core` still treats the payload as bytes; `redis-commands::eval` owns
  function-library encoding and decoding.
- Added RDB load replacement plans that stage DBs and collect function payloads
  before mutating the caller's live DB slice.
- RuntimeOwner full-sync load now stages the incoming DBs, prepares the
  incoming function registry, then swaps both into live state only after both
  phases succeed. Bad function payloads reject the full replacement and leave
  old data/functions live.
- Native `SAVE`, `BGSAVE`, replication BGSAVE, signal-shutdown RDB saves, and
  AOF RDB-preamble bases now include current function payloads.
- Startup RDB load and AOF RDB-base replay now install the function registry
  carried by native RDB files.
- Extended `fullsync_lifecycle_kit.rs` with a deterministic swapdb/function
  case proving invalid function payloads do not replace old state, while a
  valid incoming snapshot replaces old keys and old functions together.

Evidence:

```bash
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
rustfmt \
  crates/redis-core/src/rdb/save.rs \
  crates/redis-core/src/rdb/load.rs \
  crates/redis-core/src/rdb/mod.rs \
  crates/redis-commands/src/eval.rs \
  crates/redis-commands/src/persist.rs \
  crates/redis-commands/src/aof.rs \
  crates/redis-commands/src/debug_cmd.rs \
  crates/redis-server/src/runtime_owner.rs \
  crates/redis-server/src/main.rs \
  crates/redis-server/src/startup.rs \
  crates/redis-commands/tests/fullsync_lifecycle_kit.rs
cargo check -p redis-core -p redis-commands -p redis-server
cargo test -p redis-core rdb:: -- --nocapture
cargo test -p redis-commands eval::tests -- --nocapture
cargo test -p redis-commands aof::tests -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication \
  --profile integration-repl \
  --runner-id fullsync-function-swap \
  --timeout-s 300 \
  --baseport 31279 \
  --portcount 100 \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-2,integration/block-repl \
  --profile integration-repl \
  --runner-id fullsync-function-swap-tripwire \
  --timeout-s 240 \
  --baseport 31379 \
  --portcount 100 \
  --skip-build
```

Results:

- `fullsync_lifecycle_kit`: 6 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- Core RDB tests: 60 passed, 0 failed.
- `eval::tests`: 28 passed, 0 failed.
- `aof::tests`: 1 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T182229376236Z/result.json`
  timed out at 300 seconds with no parsed summary, 26 parsed failure lines,
  no exception, and no `abort_test`. The previous successful-swap assertion
  `Diskless load swapdb (async_loading): new database is exposed after
  swapping` and its `hello1`/`hello2` mismatch are gone from the parsed
  failures.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T182747183795Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- Native RDB/full-sync now treats functions as part of the replacement state.
  The remaining swapdb work is not successful function replacement; it is
  failure isolation. The next kit slice should model the current failures:
  `dbsize` drift while async loading is in progress, old key exposure after
  async replication fails, and explicit diskless short-read/drop loading logs
  plus replica-link cleanup.

### 2026-06-13 R2 follow-up: diskless-load mode surface

Scope:

- Added a typed `repl-diskless-load` live config with `disabled`, `swapdb`, and
  `flush-before-load` modes.
- `CONFIG GET/SET repl-diskless-load` now reads and updates that live mode, and
  startup config overrides preserve it.
- Replica full-sync loading publication now consults the mode instead of
  guessing only from matching replids:
  `disabled` stays quiet, `swapdb` publishes `async_loading` when the replid
  matches and ordinary `loading` otherwise, and `flush-before-load` publishes
  ordinary `loading`.
- Added focused Rust coverage for CONFIG mode updates and for the dialer
  loading-state decision.

Evidence:

```bash
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication \
  --profile integration-repl \
  --runner-id fullsync-diskless-load-mode \
  --timeout-s 300 \
  --baseport 31479 \
  --portcount 100 \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-2,integration/block-repl \
  --profile integration-repl \
  --runner-id fullsync-diskless-load-mode-tripwire \
  --timeout-s 240 \
  --baseport 31579 \
  --portcount 100 \
  --skip-build
```

Results:

- `fullsync_lifecycle_kit`: 7 passed, 0 failed.
- `replica_dialer::tests`: 3 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T183500712105Z/result.json`
  timed out at 300 seconds with no parsed summary, 26 parsed failure lines,
  no exception, and no `abort_test`. The prior
  `Diskless load swapdb (different replid): replica enter loading` failure is
  gone. The run still fails old-dataset exposure after aborted loads and still
  misses some `Loading DB in memory` log waits; it also reaches the next
  `diskless fast replicas drop during rdb pipe` assertion.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T184041873115Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- The server now has the right live knob for diskless-load semantics, and the
  dialer no longer publishes ordinary loading for default full sync. The next
  useful slice should make the loading window observable before the primary has
  finished generating the RDB, then keep the old DB/function view pinned until
  the incoming RDB is known to have completed successfully.

### 2026-06-13 R2 follow-up: diskless loading log and config exceptions

Scope:

- Moved the compatibility `Loading DB in memory` message from stderr to stdout,
  matching the Tcl harness log file it watches during diskless short-read and
  replica-drop tests.
- Allowed `CONFIG SET key-load-delay 0` through the loading gate. The upstream
  diskless child-death test uses this debug/test knob while the replica is in
  ordinary loading state.
- Extended `fullsync_lifecycle_kit.rs` so `key-load-delay` remains available
  during ordinary loading.

Evidence:

```bash
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication \
  --profile integration-repl \
  --runner-id fullsync-loading-log-keydelay \
  --timeout-s 300 \
  --baseport 31779 \
  --portcount 100 \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-2,integration/block-repl \
  --profile integration-repl \
  --runner-id fullsync-loading-log-tripwire \
  --timeout-s 240 \
  --baseport 31879 \
  --portcount 100 \
  --skip-build
```

Results:

- `fullsync_lifecycle_kit`: 7 passed, 0 failed.
- `replica_dialer::tests`: 3 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T184830106513Z/result.json`
  completed before the 300 second timeout, still without a parsed summary. It
  reported 28 parsed failure lines and an abort/exception at
  `replication child dies when parent is killed - diskless...` with
  `child process exited abnormally.` This moves past the prior `key-load-delay`
  LOADING exception and exposes the next child-death frontier.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T185242660894Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- Diskless loading observability is now closer to the upstream harness shape,
  but the implementation still lacks the real child/pipe lifecycle semantics
  those later tests assert. The next `fullsync_lifecycle_kit` slice should
  model replication-child death and parent-death cleanup explicitly before
  another full Tcl grind.

### 2026-06-13 R2 follow-up: parent-death child cleanup

Scope:

- Factored successful full-sync transfer side effects into
  `ReplicationState::complete_repl_bgsave_transfer`, giving the Rust kits a
  deterministic way to prove RDB bulk delivery, catch-up delivery, online
  transition, and retained catch-up history after a prior failed child.
- Added `ReplicationState::collect_failed_repl_bgsave_child_exit` so stale
  child-exit observations cannot tear down a later full-sync job.
- Made forked BGSAVE and BGSAVE-for-replication children notice parent death
  while sleeping in the debug save-delay window. On Linux this also arms a
  parent-death signal; on Unix generally the child polls for parent PID
  changes.
- Changed `rdb-key-save-delay` from a single post-save sleep into a bounded
  per-key-equivalent delay based on snapshot key count, capped at five seconds.
  This keeps upstream child-observability tests meaningful without making the
  suite unbounded.
- Accepted `repl-diskless-load on-empty-db` as a live config value. Valdr
  currently treats it conservatively like ordinary loading until the dialer has
  a true empty-DB predicate.
- Extended `fullsync_lifecycle_kit.rs` with a killed-child collection case and
  `on-empty-db` config coverage.

Evidence:

```bash
cargo test -p redis-core replication::tests -- --nocapture
cargo test -p redis-commands persist::tests -- --nocapture
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication \
  --profile integration-repl \
  --runner-id fullsync-child-exit-onempty \
  --timeout-s 360 \
  --baseport 32179 \
  --portcount 100 \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-2,integration/block-repl \
  --profile integration-repl \
  --runner-id fullsync-child-exit-tripwire \
  --timeout-s 240 \
  --baseport 32279 \
  --portcount 100 \
  --skip-build
```

Results:

- `redis-core replication::tests`: 15 passed, 0 failed.
- `persist::tests`: 4 passed, 0 failed.
- `fullsync_lifecycle_kit`: 8 passed, 0 failed.
- `replica_dialer::tests`: 3 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T191404820272Z/result.json`
  timed out at 360 seconds, still without a parsed summary, but had no abort
  test and no exception. It reported 37 parsed failure lines and reached later
  replication-link assertions after the previous parent-killed child and
  `repl-diskless-load on-empty-db` abort points.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T192012094878Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- The upstream-style process-observability frontier moved: `get_child_pid`
  can now observe the delayed replication child, parent death no longer aborts
  the file, and `on-empty-db` is accepted. Remaining `integration/replication`
  work is now beyond child collection: async rollback/DB-size drift, diskless
  pipe logs, cache-master handoff, and later replication-link behavior.

### 2026-06-13 R2 follow-up: cancel useless full-sync children

Scope:

- `ReplicationState::remove_replica` now prunes active full-sync waiter lists
  and returns a typed `ReplicaRemovalOutcome`.
- Runtime-owner and thread-per-connection cleanup paths now request
  replication BGSAVE child cancellation when the last full-sync waiter leaves
  and normal `save` rules are disabled. If `save` remains configured, Valdr
  preserves the child to match the upstream test's "still useful for save"
  expectation.
- Replica-side full-sync RDB reads now use bounded read timeouts and check the
  dialer epoch/drop flag. This lets `REPLICAOF NO ONE` interrupt an in-flight
  RDB receive and close the primary socket promptly instead of waiting for the
  old primary to finish sending the RDB.
- Extended `fullsync_lifecycle_kit.rs` with a two-waiter case that proves only
  the final waiter disconnect marks the replication child useless.
- Added a `replica_dialer::tests` socket-pair case proving a full-sync RDB read
  exits with `Interrupted` when the dialer epoch changes mid-read.

Evidence:

```bash
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo test -p redis-core replication::tests -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication \
  --profile integration-repl \
  --runner-id fullsync-useless-child-runtime \
  --timeout-s 420 \
  --baseport 32479 \
  --portcount 100 \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-2,integration/block-repl \
  --profile integration-repl \
  --runner-id fullsync-useless-child-tripwire \
  --timeout-s 240 \
  --baseport 32579 \
  --portcount 100 \
  --skip-build
```

Results:

- `replica_dialer::tests`: 4 passed, 0 failed.
- `fullsync_lifecycle_kit`: 9 passed, 0 failed.
- `redis-core replication::tests`: 15 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`:
  `harness/oracle/results/tcl-survey/20260613T193718529072Z/result.json`
  timed out at 420 seconds, still without a parsed summary, but had no abort
  test and no exception. Parsed failure lines dropped from 37 to 36; the
  `Kill rdb child process if its dumping RDB is not useful` failure is gone.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T194426703275Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- Full-sync child lifetime is now tied to active waiters rather than stale job
  membership. The remaining `integration/replication` failures are concentrated
  in async rollback/DB-size drift, diskless pipe observability, cache-master
  handoff, and replication-link reply validation.

### R4-AOF-FULLSYNC

Status: `integration/replication-aof-sync` green on 2026-06-13.

Implementation:

- Replica full-sync RDB loading now replaces the existing replica keyspace
  before applying the incoming RDB, so stale local keys do not survive a full
  sync or later restart.
- Appendonly replicas refresh their manifest after a successful full-sync RDB
  load. Disk-based, RDB-preamble-enabled sync publishes the received RDB bytes
  as a fresh `.base.rdb` plus active `.incr.aof`; other full-sync modes run the
  existing manifest rewrite fallback from the loaded DBs.
- Startup config exposure now carries `repl-diskless-sync` into `LiveConfig`,
  allowing the AOF refresh path to distinguish the disk-based reuse case from
  diskless fallback in the Tcl topology.
- `aof.rs` has a focused unit test for publishing a full-sync RDB manifest
  base plus active incr file.

Evidence:

```bash
cargo test -p redis-commands \
  aof::tests::fullsync_rdb_manifest_base_installs_base_and_incr \
  -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo test -p redis-commands --test aof_correctness_kit -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-aof-fullsync-base-refresh-3 \
  --profile integration-repl \
  --timeout-s 180 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-aof-sync \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-aof-fullsync-base-refresh-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- Focused AOF manifest unit test: passed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `aof_correctness_kit`: 18 passed, 0 failed.
- `repl_correctness_kit`: 21 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication-aof-sync`:
  `harness/oracle/results/tcl-survey/20260613T053525324235Z/result.json`
  reported 6 passed, 0 failed, 0 timed out, 0 no-summary files, and 0
  abort/exception points.
- Focused tripwire:
  `harness/oracle/results/tcl-survey/20260613T053539524508Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

### R1-NOOP-DIRTY

Status: completed on 2026-06-13 for the covered deletion-style no-op writes.

Implementation:

- `DEL` / `UNLINK` already suppressed no-op propagation.
- `SREM`, `HDEL`, and `ZREM` now call `prevent_propagation` when the key is
  missing or no requested member/field is removed.
- `repl_correctness_kit.rs` covers top-level no-op `DEL`, no-op `DEL` inside
  `MULTI` / `EXEC`, and missing/existing-container no-op `SREM`, `HDEL`, and
  `ZREM`.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit
cargo check -p redis-commands
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r1-noop-single-node-repl \
  --profile single-node-repl \
  --timeout-s 180 \
  --baseport 45000 \
  --portcount 3000 \
  --clients 1 \
  --files unit/type/set,unit/type/hash,unit/type/zset \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `repl_correctness_kit`: 14 passed, 0 failed.
- `cargo check -p redis-commands`: passed.
- `cargo build --bin redis-server`: passed.
- Focused TCL:
  `harness/oracle/results/tcl-survey/20260613T004416973596Z/result.json`
  reported `unit/type/hash` 83/0, `unit/type/zset` 320/0, and
  `unit/type/set` 114/1.

The remaining `unit/type/set` failure is
`SPOP new implementation: code path #1 propagate as DEL or UNLINK`, which is
the next `R1-SPOP-REWRITE` packet rather than a no-op dirty failure.

### R1-SPOP-REWRITE

Status: completed on 2026-06-13.

Implementation:

- `SPOP key` now rewrites propagation to `SREM key <removed-member>`.
- `SPOP key count` now suppresses no-op propagation for missing keys and
  `count == 0`.
- Partial `SPOP key count` rewrites propagation to `SREM key <removed...>`.
- Partial `SPOP key count` above 1024 removed elements propagates as multiple
  `SREM` batches, matching the upstream command-stat expectation.
- Full `SPOP key count` rewrites propagation to `DEL key` by default, or
  `UNLINK key` when `lazyfree-lazy-server-del` is configured as `yes`.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit
cargo check -p redis-commands
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r1-spop-three-type-recheck \
  --profile single-node-repl \
  --timeout-s 180 \
  --baseport 45000 \
  --portcount 3000 \
  --clients 1 \
  --files unit/type/set,unit/type/hash,unit/type/zset \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `repl_correctness_kit`: 17 passed, 0 failed after adding the 1024-member
  batching guard and the DB SELECT guard.
- `cargo check -p redis-commands`: passed.
- `cargo build --bin redis-server`: passed.
- Focused TCL:
  `harness/oracle/results/tcl-survey/20260613T004936241400Z/result.json`
  reported `unit/type/set` 115/0, `unit/type/hash` 83/0, and
  `unit/type/zset` 320/0.
- Rebuilt `integration/replication-4` focused recheck:
  `harness/oracle/results/tcl-survey/20260613T005920803425Z/result.json`
  reported 14/3 and no longer reported the `spopwithcount rewrite srem
  command` failure.
- Rebuilt `integration/replication-3,replication-4` R1 gate:
  `harness/oracle/results/tcl-survey/20260613T005958508926Z/result.json`
  reported `replication-3` 3/4 and `replication-4` 15/2. The SPOP and debug
  propagation cases passed; remaining failures are expiry/PFCOUNT semantics and
  divergence/writable-replica cases.

### R1-TTL-REWRITE

Status: completed on 2026-06-13.

Implementation:

- EXPIRE-family commands propagate as `PEXPIREAT key <absolute-ms>`.
- Expiry timestamps already in the past propagate as `UNLINK key`.
- `SET` / `SETEX` / `PSETEX` relative expiry forms propagate as `SET ... PXAT
  <absolute-ms>`.
- `GETEX EX|PX` propagates as `PEXPIREAT key <absolute-ms>`.
- `MSETEX EX|PX` propagates as `MSETEX ... PXAT <absolute-ms>`.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r1-ttl-expire-baseline \
  --profile single-node-repl \
  --timeout-s 240 \
  --baseport 45000 \
  --portcount 3000 \
  --clients 1 \
  --files unit/expire \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `repl_correctness_kit`: 16 passed, 0 failed.
- Focused TCL:
  `harness/oracle/results/tcl-survey/20260613T005055896320Z/result.json`
  reported `unit/expire` 67/0.

### R1-DB-SELECT

Status: completed on 2026-06-13 for dispatch-time fan-out coverage.

Implementation:

- The shared replication fan-out path already prefixes a `SELECT <db>` frame
  before the first write in a different logical DB.
- `repl_correctness_kit.rs` now proves that the first DB 5 write emits
  `SELECT 5` before the write, that consecutive DB 5 writes do not resend the
  selector, and that switching back to DB 0 emits `SELECT 0` before the write.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit
cargo check -p redis-commands
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r1-integration-3-4-current \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-3,integration/replication-4 \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `repl_correctness_kit`: 17 passed, 0 failed.
- `cargo check -p redis-commands`: passed.
- `cargo build --bin redis-server`: passed.
- Focused TCL:
  `harness/oracle/results/tcl-survey/20260613T005958508926Z/result.json`
  reported `replication-3` 3/4 and `replication-4` 15/2. This is not an HA
  claim; it is evidence that the R1 command propagation regressions are cleared
  while expiration and divergence semantics remain.

### R3-RECONNECT-MATRIX

Status: completed on 2026-06-13 for master-side PSYNC decision coverage;
extended with replica target-change state hardening and a standalone
`psync_reconnect_kit` on 2026-06-13.

Implementation:

- `handle_psync` now routes through a small decision helper that preserves the
  existing behavior while making reconnect cases directly testable.
- Unit coverage now exercises fresh full sync, caught-up empty-backlog
  reconnect, in-window reconnect, wrong replid, future offset, old offset after
  backlog wraparound, and the first retained offset after wraparound.
- Existing `repl_correctness_kit.rs` coverage still proves that a granted
  `+CONTINUE` replays the backlog catch-up bytes and that PSYNC counters move
  correctly for in-window, future-offset fallback, and fresh full sync.
- Replica-side state now preserves cached primary replid/offset across
  same-target reconnects, but clears them when `REPLICAOF` changes host or port.
  This prevents a new primary from receiving stale PSYNC metadata while keeping
  the live dialer eligible for partial resync after ordinary disconnects.
- `psync_reconnect_kit.rs` now drives the real `psync_command` entrypoint for
  same-primary `+CONTINUE`, backlog-expired `+FULLRESYNC`, wrong replid, future
  offset, and fresh `PSYNC ? -1` metric behavior. It also keeps the
  target-change cache rule in a deterministic standalone kit.

Evidence:

```bash
cargo test -p redis-commands \
  replication::tests::psync_decision_matrix_covers_reconnect_edges \
  -- --nocapture
cargo test -p redis-commands replication::tests::psync -- --nocapture
cargo test -p redis-core \
  replication::tests::target_change_resets_cached_partial_resync_state \
  -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
```

Results:

- Focused decision-matrix unit test: passed.
- Focused PSYNC unit filter: 2 passed, 0 failed.
- Focused target-change state unit test: passed.
- `psync_reconnect_kit`: 4 passed, 0 failed.
- `repl_correctness_kit`: 21 passed, 0 failed.
- `repl_buffer_kit`: 3 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `integration/replication-psync` was not rerun in this packet; the R0
  dashboard timeout remains the current slow-suite frontier.

### R4-ROLE-CHANGE-UNBLOCK

Status: partial R4 progress on 2026-06-13.

Implementation:

- `BlockedKeysIndex` can now drain every replication-progress waiter, covering
  both `WAIT` and `WAITAOF`.
- `REPLICAOF` topology changes now unblock those waiters with the existing
  `UNBLOCKED force unblock from blocking operation, instance state changed`
  error payload.
- `appendonly no` still uses the narrower WAITAOF-local error path, so plain
  WAIT clients are not disturbed by AOF-only configuration changes.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit \
  p4_wait_and_waitaof_waiters_unblock_on_role_change \
  -- --nocapture
cargo test -p redis-core blocked_keys -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo test -p redis-commands replication::tests::wait -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r4-wait-role-change \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files unit/wait \
  --isolated-tests-copy \
  --skip-build
```

Results:

- Focused repl-kit role-change test: passed.
- Core blocked-key unit filter: 3 passed, 0 failed.
- `repl_correctness_kit`: 19 passed, 0 failed.
- WAIT/WAITAOF unit filter: 6 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused TCL:
  `harness/oracle/results/tcl-survey/20260613T043825663450Z/result.json`
  reported `unit/wait` 39/0.
- `integration/replication-aof-sync` was not rerun in this packet and remains
  at the current 1/5 frontier.

### R5-FAILOVER-PARSER

Status: parser-only progress on 2026-06-13; no HA/failover claim.

Implementation:

- Server `FAILOVER` is registered in the runtime dispatch table.
- The parser accepts the Valkey server syntax:
  `FAILOVER [TO <HOST> <PORT> [FORCE]] [ABORT] [TIMEOUT <timeout>]`.
- `ABORT`, invalid `TIMEOUT`, incomplete `TO`, replica-mode, no-replica, and
  `FORCE` precondition errors are covered.
- A syntactically valid command that would need the real coordinated failover
  state machine returns an explicit unimplemented error instead of starting any
  fake role transition.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit failover -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r5-failover-parser \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replica-redirect \
  --isolated-tests-copy \
  --skip-build
```

Results:

- Focused failover parser filter: 2 passed, 0 failed.
- `repl_correctness_kit`: 21 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused TCL:
  `harness/oracle/results/tcl-survey/20260613T044419486652Z/result.json`
  still has no summary for `integration/replica-redirect`; it now aborts with
  `ERR FAILOVER is parsed but coordinated failover is not implemented yet.`
  rather than `unknown command`.

### R5-REDIRECT-CONTRACT

Status: first client-visible redirect slice completed on 2026-06-13;
`integration/replica-redirect` still aborts at the coordinated failover
placeholder, but the earlier REDIRECT assertions now pass.

Implementation:

- Added `failover_redirect_kit.rs` as the deterministic inner loop for
  redirect-capable clients on replicas.
- Top-level dispatch now returns `-REDIRECT <host>:<port>` for data-access
  commands from clients that declared `CLIENT CAPA REDIRECT` when this node is
  a replica with a known primary target.
- `READONLY` clients with redirect capability keep allowed read commands local
  while write-like commands still redirect to the primary.
- Non-data commands such as `PING`, and ordinary non-redirect clients, preserve
  the existing replica behavior.
- MULTI queue-time redirect now marks the transaction dirty so `EXEC` returns
  `EXECABORT`, matching the Tcl case where a write is issued while already on a
  replica.
- If a write was queued while the node was primary and the node becomes a
  replica before `EXEC`, `EXEC` returns `REDIRECT` for redirect-capable clients
  instead of running the queued write locally.

Evidence:

```bash
cargo test -p redis-commands --test failover_redirect_kit -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo test -p redis-commands multi -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r5-redirect-contract \
  --profile integration-repl \
  --timeout-s 300 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replica-redirect \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r5-redirect-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `failover_redirect_kit`: 5 passed, 0 failed.
- `repl_correctness_kit`: 21 passed, 0 failed.
- MULTI filter: 8 passed across unit/integration kit filters, 0 failed.
- `repl_buffer_kit`: 3 passed, 0 failed.
- `psync_reconnect_kit`: 4 passed, 0 failed.
- `fullsync_lifecycle_kit`: 1 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replica-redirect`:
  `harness/oracle/results/tcl-survey/20260613T141710606290Z/result.json`
  still produced no parsed summary and aborts at
  `client paused before and during failover-in-progress` with
  `ERR FAILOVER is parsed but coordinated failover is not implemented yet..`.
  It reports 0 parsed failure lines before that abort.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T141719342946Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- The next useful `failover_redirect_kit` slice is not more top-level
  REDIRECT formatting. It is explicit primary-side failover state:
  write/client pause, waiting-for-sync, unblocking blocked clients with
  REDIRECT, and eventual promotion/demotion handoff.

### R5-FAILOVER-VISIBLE-STATE

Status: first visible primary-side failover state slice completed on
2026-06-13. This is still not manual failover: it starts and aborts explicit
state and pause, but does not complete timeout, promotion, demotion, or blocked
client REDIRECT unblocking.

Implementation:

- Added primary-side manual failover state to `ReplicationState`:
  `no-failover`, `waiting-for-sync`, and `failover-in-progress`.
- `FAILOVER` with connected replicas now returns `OK` and enters visible state
  instead of returning the old coordinated-failover-unimplemented error.
- `FAILOVER TO <host> <port> TIMEOUT <ms> FORCE` enters
  `failover-in-progress`; non-force `FAILOVER` enters `waiting-for-sync`.
- `FAILOVER ABORT` now clears the manual failover state and failover pause.
- `INFO replication` exposes `master_failover_state`.
- Added failover pause helpers in `redis-core::networking` so the state can
  use the existing runtime pause machinery.
- `failover_redirect_kit.rs` now covers visible state, failover pause
  observability, and ABORT cleanup in addition to redirect-capable client
  behavior.

Evidence:

```bash
cargo test -p redis-commands --test failover_redirect_kit -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo test -p redis-core replication::tests -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-r5-failover-state-freshbase \
  --profile integration-repl \
  --timeout-s 90 \
  --baseport 52000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replica-redirect \
  --isolated-tests-copy \
  --skip-build
```

Results:

- `failover_redirect_kit`: 7 passed, 0 failed.
- `repl_correctness_kit`: 21 passed, 0 failed.
- Core replication tests: 9 passed, 0 failed.
- `repl_buffer_kit`: 3 passed, 0 failed.
- `psync_reconnect_kit`: 4 passed, 0 failed.
- `fullsync_lifecycle_kit`: 1 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replica-redirect`:
  `harness/oracle/results/tcl-survey/20260613T144457545119Z/result.json`
  timed out in `client paused before and during failover-in-progress` with
  `Timeout waiting for blocked clients`. This is a later semantic frontier
  than the earlier explicit unimplemented FAILOVER error.
- Runtime-owner pause follow-up:
  `cargo test -p redis-server failover_pause_exempts_client_capa -- --nocapture`
  passed, proving `CLIENT CAPA REDIRECT` can complete during failover pause
  while data reads are still paused.
- Focused `integration/replica-redirect` after the `CLIENT CAPA` pause
  exemption:
  `harness/oracle/results/tcl-survey/20260613T145117971383Z/result.json`
  timed out with 0 parsed failure lines and no stdout failure text. The next
  frontier is likely failover completion/unblock, not the earlier
  blocked-client count assertion.
- An earlier focused run before making state persistent,
  `harness/oracle/results/tcl-survey/20260613T142430221015Z/result.json`,
  reached the same test but timed out at blocked-client wait because the
  failover pause expired too quickly for `TIMEOUT 100`.
- Follow-up tripwire attempts
  `harness/oracle/results/tcl-survey/20260613T143600515176Z/result.json`,
  `harness/oracle/results/tcl-survey/20260613T144733225968Z/result.json`, and
  `harness/oracle/results/tcl-survey/20260613T144747353291Z/result.json` were
  inconclusive harness runs: startup/port-selection failures occurred before
  useful replication assertions were parsed.

Takeaway:

- Superseded by the follow-up handoff packet below. The live-runtime
  blocked-client kit/probe now exists and proves the production redirect path;
  the remaining `replica-redirect.tcl` issue is converting the no-summary Tcl
  timeout into a parsed assertion or pass.

### 2026-06-13 R5 follow-up: manual failover handoff and blocked REDIRECT

Scope:

- Added pending `REPLCONF listening-port` / `capa` metadata so `FAILOVER TO`
  can target replicas whose metadata arrives before `PSYNC` registers
  `ReplicaConn`.
- Added timeout-driven manual failover advancement in the runtime owner:
  `waiting-for-sync` starts with write pause, `failover-in-progress` uses
  all-client pause, and the old primary demotes to the selected replica.
- Added `PSYNC <cached-replid> <offset> FAILOVER` selection, including the
  zero-offset case, so the target promotes and grants `+CONTINUE` during manual
  failover.
- Added blocked waiter metadata for failover role changes. Redirect-capable
  blocking pop/zset clients and non-readonly stream readers drain with
  `-REDIRECT host:port`; readonly `XREAD` waiters remain blocked.

Evidence:

```bash
cargo test -p redis-commands --test failover_redirect_kit -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo test -p redis-core replication::tests -- --nocapture
cargo test -p redis-core blocked_keys::tests -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-failover-blocked-redirect \
  --profile integration-repl \
  --files integration/replica-redirect \
  --timeout-s 180 \
  --baseport 27479 \
  --portcount 100 \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-failover-observe-hang \
  --profile integration-repl \
  --files integration/replica-redirect \
  --timeout-s 90 \
  --baseport 27779 \
  --portcount 100 \
  --skip-build
```

Results:

- `failover_redirect_kit`: 11 passed, 0 failed.
- `replica_dialer::tests`: 2 passed, 0 failed.
- Core replication tests: 9 passed, 0 failed.
- Core blocked-key tests: 3 passed, 0 failed.
- `repl_correctness_kit`: 21 passed, 0 failed.
- `psync_reconnect_kit`: 4 passed, 0 failed.
- `repl_buffer_kit`: 3 passed, 0 failed.
- `fullsync_lifecycle_kit`: 1 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Live two-process probe on ports `27379/27380`: passed. A blocked
  redirect-capable `BRPOP` and a redirect-capable `GET` issued during
  `failover-in-progress` both received `-REDIRECT 127.0.0.1:27380`; old primary
  reported `role:slave`, `master_failover_state:no-failover`, and
  `master_port:27380`; target reported `role:master`.
- Focused `integration/replica-redirect`
  `harness/oracle/results/tcl-survey/20260613T152950464324Z/result.json`:
  timed out after 180s, no parsed failures, no summary.
- Side-observed focused run
  `harness/oracle/results/tcl-survey/20260613T153506959193Z/result.json`:
  timed out after 90s, no parsed failures. During the run, external `INFO`
  showed the old primary at `blocked_clients:2`, `paused_actions:all`,
  `role:slave`, and `master_failover_state:failover-in-progress` while the
  target process was still SIGSTOP'd.

Takeaway:

- The server-side handoff and blocked-client redirect semantics now have Rust
  and live-process evidence. The remaining Tcl frontier is harness-facing or a
  narrower client-lifecycle edge: explain why the official script does not
  execute `resume_process` despite externally visible `blocked_clients:2`, then
  turn that no-summary timeout into a parsed assertion or pass.

### 2026-06-14 R5 follow-up: failover pause setup SELECT

Scope:

- Converted the remaining first `replica-redirect.tcl` no-summary hang into a
  kit-sized runtime invariant: failover all-client pause must park
  redirect-capable data commands, but still allow connection/setup commands.
- Added `SELECT` to the owner-loop failover pause exemption set. The Tcl
  `valkey_deferring_client` helper sends an implicit `SELECT 9` before the test
  can issue `CLIENT CAPA REDIRECT`; parking that `SELECT` made the script hang
  before it could create the second paused client.
- Extended the runtime-owner kit so `CLIENT CAPA`, `SELECT`, and `INFO clients`
  stay available while a data `GET` remains pause-postponed and counted.
- Updated the redirect kit's PSYNC failover setup to seed real backlog history
  with `append_to_backlog` instead of writing `master_repl_offset` directly.
  The newer PSYNC decision code requires a readable history window, not just an
  offset number.

Evidence:

```bash
cargo test -p redis-server failover_pause_exempts_client_capa_but_pauses_data_reads -- --nocapture
cargo test -p redis-server failover_all_pause_counts_postponed_data_but_allows_info -- --nocapture
cargo test -p redis-commands --test failover_redirect_kit -- --nocapture --test-threads=1
cargo build -p redis-server --bin redis-server
VALKEY_BIN_DIR=/Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/target/debug \
  timeout 80 tclsh tests/test_helper.tcl \
  --single integration/replica-redirect \
  --clients 1 \
  --skip-leaks \
  --baseport 45400 \
  --portcount 4000 \
  --tags '-needs:debug -cluster -needs:cluster' \
  --verbose \
  --only 'write command inside MULTI is QUEUED, EXEC should be REDIRECT' \
  --only 'client paused before and during failover-in-progress'
```

Results:

- Runtime-owner failover pause predicate kit: passed after first failing on the
  new `SELECT` assertion.
- Runtime-owner dispatch/counting kit: 1 passed, 0 failed.
- `failover_redirect_kit` serialized: 11 passed, 0 failed.
- `cargo build -p redis-server --bin redis-server`: passed.
- Focused direct Tcl `integration/replica-redirect` slice: 2 passed, 0 failed
  in 3 seconds. The previously hanging `client paused before and during
  failover-in-progress` case now passes.

Takeaway:

- This is the preferred shape for the remaining red replication work: narrow
  the Tcl timeout with observation, encode the blocking invariant as a fast
  Rust kit, then run only the smallest Tcl selector that proves the script
  advanced. A full `replica-redirect` scoreboard is still needed before the
  whole file's dashboard row can change status.

### 2026-06-14 R5 follow-up: replica-redirect scoreboard green

Scope:

- Converted the next `replica-redirect.tcl` failure from a broad Tcl symptom
  into three kit-sized invariants:
  partial resync must not publish `master_link_status:up` before inline catch-up
  has gone idle, a promoted master reconfigured with `REPLICAOF` must drop
  cached upstream PSYNC state, and primary-side replica rows must be cleared
  when a master demotes.
- The concrete Tcl failure was `blocked clients behavior during failover` after
  `responses in waiting-for-sync state`: the replica-side readonly `XREAD`
  returned stale DB 9 stream data instead of blocking. The root cause was a
  premature `wait_replica_online` on the old primary's stale `slave0` row.
- Added fast coverage in `replica_dialer::tests`, `psync_reconnect_kit`, and
  `failover_redirect_kit` before rerunning Tcl.
- Kept ordinary `REPLICAOF` chained-replica rows intact; stale primary-side
  row cleanup is limited to failover demotion, where those rows would otherwise
  make `wait_replica_online` observe an old downstream.

Evidence:

```bash
cargo test -p redis-commands --test failover_redirect_kit -- --nocapture --test-threads=1
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture --test-threads=1
cargo test -p redis-commands replica_dialer::tests -- --nocapture --test-threads=1
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture --test-threads=1
cargo test -p redis-core replication::tests -- --nocapture
cargo build -p redis-server --bin redis-server
VALKEY_BIN_DIR=/Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/target/debug \
  timeout 90 tclsh tests/test_helper.tcl \
  --single integration/replica-redirect \
  --clients 1 \
  --skip-leaks \
  --baseport 47500 \
  --portcount 4000 \
  --tags '-needs:debug -cluster -needs:cluster' \
  --verbose \
  --only 'responses in waiting-for-sync state' \
  --only 'blocked clients behavior during failover'
VALKEY_BIN_DIR=/Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port/target/debug \
  timeout 120 tclsh tests/test_helper.tcl \
  --single integration/replica-redirect \
  --clients 1 \
  --skip-leaks \
  --baseport 47700 \
  --portcount 4000 \
  --tags '-needs:debug -cluster -needs:cluster' \
  --verbose
```

Results:

- `failover_redirect_kit`: 11 passed, 0 failed.
- `psync_reconnect_kit`: 17 passed, 0 failed.
- `replica_dialer::tests`: 14 passed, 0 failed.
- `repl_correctness_kit`: 32 passed, 0 failed.
- Core replication tests: 15 passed, 0 failed.
- `cargo build -p redis-server --bin redis-server`: passed.
- Focused direct Tcl selector: 2 passed, 0 failed in 3 seconds.
- Full direct `integration/replica-redirect`: 11 passed, 0 failed in 6 seconds.

Takeaway:

- We do not need the long Tcl command as the development loop. The productive
  loop is: observe enough Tcl to name the failure, encode the invariant as a
  fast Rust kit, then use the smallest Tcl selector as a scoreboard. Full Tcl
  runs are still valuable, but only after the kits are green.

### 2026-06-13 R3 follow-up: replica-side CLIENT KILL reconnect

Scope:

- `CLIENT KILL <master_host>:<master_port>` on a replica now recognizes the
  outbound primary link owned by the replica dialer, even though that TCP stream
  is not a normal runtime client slot.
- The command sets a one-shot dialer drop request; the dialer explicitly shuts
  down the current stream, returns to its reconnect loop, and issues PSYNC with
  cached replid/offset.
- Ordinary `REPLICAOF` target changes now clear both cached replid/offset and
  stale backlog bytes, preventing impossible `master_offset=0` plus old backlog
  state from poisoning later partial-resync decisions. The failover demotion path
  still preserves old-primary history for `PSYNC ... FAILOVER`.

Evidence:

```bash
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo test -p redis-core replication::tests -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-psync-client-kill-dialer \
  --profile integration-repl \
  --files integration/replication-psync \
  --timeout-s 240 \
  --baseport 28079 \
  --portcount 100 \
  --skip-build
```

Results:

- `psync_reconnect_kit`: 5 passed, 0 failed.
- `replica_dialer::tests`: 2 passed, 0 failed.
- Core replication tests: 9 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Live two-process probe on ports `27979/27980`: passed. `CLIENT KILL
  127.0.0.1:27979` on the replica returned `+OK`, the primary's
  `sync_partial_ok` moved `0 -> 1`, and the primary settled at
  `connected_slaves:1` with the replica online.
- Focused `integration/replication-psync`
  `harness/oracle/results/tcl-survey/20260613T154546265871Z/result.json`:
  completed in 210s with 86 passed, 4 failed, 0 timeouts, and 0 no-summary
  files. All four failures are `ok after delay` variants expecting
  `sync_partial_ok > 0`.

Takeaway:

- The major PSYNC timeout was the replica-side link-kill visibility gap. The
  remaining R3 slice should focus on delayed reconnects: preserve enough
  history, offset, and cached replid state through `DEBUG SLEEP` / delayed
  reconnect windows so the `ok after delay` variants get `+CONTINUE` instead of
  full resync.

### 2026-06-13 R3 follow-up: live backlog resize and TTL expiry

Scope:

- `CONFIG SET repl-backlog-size` now resizes the live circular backlog while
  preserving readable history, so delayed reconnect windows can actually use the
  configured 100 MB backlog in the upstream PSYNC matrix.
- `repl-backlog-ttl` is now represented in live config and enforced
  opportunistically before PSYNC decisions. Expiry clears readable history while
  preserving the master offset, so concrete old-offset PSYNC attempts fall back
  to full resync and increment `sync_partial_err`.
- Replica reconnect cleanup now removes stale master-side `ReplicaConn` entries
  that advertise the same listening port, and normal disconnect expiry is armed
  from the removed replica's last ACK time.
- `DEBUG SLEEP` on a replica pauses the background replica dialer, matching the
  upstream single-threaded delay that the Tcl `backlog expired` cases depend on.

Evidence:

```bash
cargo test -p redis-core replication::tests -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo test -p redis-commands replica_dialer::tests -- --nocapture
cargo check -p redis-core -p redis-commands -p redis-server
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-psync \
  --profile integration-repl \
  --runner-id repl-psync-backlog-resize-ttl-pause \
  --timeout-s 240 \
  --baseport 29379 \
  --portcount 100 \
  --skip-build
```

Results:

- Core replication tests: 15 passed, 0 failed.
- `psync_reconnect_kit`: 7 passed, 0 failed.
- `replica_dialer::tests`: 2 passed, 0 failed.
- `cargo check -p redis-core -p redis-commands -p redis-server`: passed.
- `cargo build --bin redis-server`: passed.
- Live two-process probe on ports `29279/29280`: during the delayed reconnect,
  `connected_slaves` dropped to 0 and the reconnect moved `sync_partial_err` to
  1 before full resync.
- Focused `integration/replication-psync`
  `harness/oracle/results/tcl-survey/20260613T162716653643Z/result.json`:
  completed in 211s with 90 passed, 0 failed, 0 timeouts, and 0 no-summary
  files.

Takeaway:

- `integration/replication-psync` is now a green tripwire. Future PSYNC work can
  move from reconnect basics to dual replication IDs and failover-history
  windows instead of repeatedly rediscovering backlog sizing, TTL, and delayed
  reconnect mechanics.

### 2026-06-13 R2 baseline after PSYNC green

Evidence:

```bash
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-buffer \
  --profile integration-repl \
  --runner-id repl-buffer-post-psync-green-baseline \
  --timeout-s 300 \
  --baseport 29479 \
  --portcount 100 \
  --skip-build
```

Result:

- `harness/oracle/results/tcl-survey/20260613T163420334540Z/result.json`:
  completed in 131s with 4 passed, 11 failed, 0 timeouts, and 0 no-summary
  files.

Takeaway:

- The next replication-buffer packet should be designed as a real shared-buffer
  and output-buffer-lifetime slice. The failure shape is no longer a harness
  timeout; it is counted assertions around shared memory, slow-replica backlog
  outgrowth, retained-history partial resync, and output-buffer limit policy.

### 2026-06-13 R2 replica-link reply guard

Scope:

- `Client` now exposes a deterministic replica-link reply violation boundary:
  after `SYNC` / `PSYNC` has turned a connection into a replica, any ordinary
  reply generated by later replica-link input is captured as a protocol
  violation, removed from the reply buffer, and summarized with the command
  name used by the upstream log assertions.
- Server dispatch boundaries now apply that guard in both the legacy socket
  loop and the RuntimeOwner DB-list path. Violations are logged to stdout, then
  the connection is closed without flushing the generated reply to the replica
  link.
- Disallowed keyspace commands from replica links now generate
  `ERR Replica can't interact with the keyspace`, which feeds the expected
  critical-log path instead of being silently ignored.
- `fullsync_lifecycle_kit` covers `PING`, `GET`, and `SLOWLOG GET` replica-link
  cases; core `client` tests cover command naming, reply clearing, error
  classification, and handshake exemption.

Evidence:

```bash
cargo test -p redis-core client::tests::replica -- --nocapture
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-replica-link-stdout \
  --profile integration-repl \
  --timeout-s 420 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-replica-link-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- Core replica-reply client tests: 3 passed, 0 failed.
- `fullsync_lifecycle_kit`: 10 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`
  `harness/oracle/results/tcl-survey/20260613T195949781851Z/result.json`:
  completed in 296s with 28 passed, 39 failed, 0 timed out, 0 without summary,
  and 0 abort/exception points. The four `replica do not write the reply to the
  replication link` failures are gone.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T200518959430Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- The replica-link protocol guard is no longer the `integration/replication`
  frontier. The remaining counted failures are broader replication behavior:
  diskless pipe/drop logs, full-sync rollback, command stats for rewritten
  blocking-list commands, cache-master handling, lazy expire, and old-data
  exposure during failed/async loads.

### 2026-06-13 R3 malformed PSYNC offset log

Scope:

- Malformed `PSYNC <replid> <offset>` offsets still return
  `ERR value is not an integer or out of range`, but now also emit the
  Valkey-compatible stdout diagnostic watched by `integration/replication`:
  `Replica <id> asks for synchronization but with a wrong offset`.
- `psync_reconnect_kit` now proves malformed offsets do not turn the client
  into a replica, do not register full-sync waiters, and do not perturb PSYNC
  counters.

Evidence:

```bash
cargo test -p redis-commands \
  replication::tests::wrong_psync_offset_log_line_matches_upstream_pattern \
  -- --nocapture
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-psync-wrong-offset-log \
  --profile integration-repl \
  --timeout-s 420 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-psync-wrong-offset-psync-gate \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-psync \
  --isolated-tests-copy \
  --skip-build
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-psync-wrong-offset-tripwire \
  --profile integration-repl \
  --timeout-s 240 \
  --baseport 47000 \
  --portcount 4000 \
  --clients 1 \
  --files integration/replication-2,integration/block-repl \
  --isolated-tests-copy \
  --skip-build
```

Results:

- Focused wrong-offset unit test: passed.
- `psync_reconnect_kit`: 8 passed, 0 failed.
- `cargo build --bin redis-server`: passed.
- Focused `integration/replication`
  `harness/oracle/results/tcl-survey/20260613T201024847711Z/result.json`:
  completed with 28 passed, 39 failed, 0 timed out, 0 without summary, and 0
  abort/exception points. The `PSYNC with wrong offset should throw error`
  failure is gone from the failure list; the aggregate pass count is unchanged
  because a count-sensitive BLMOVE assertion reappeared in this run.
- Focused `integration/replication-psync`
  `harness/oracle/results/tcl-survey/20260613T201550387200Z/result.json`:
  stayed green at 90 passed, 0 failed.
- Focused no-regression tripwire:
  `harness/oracle/results/tcl-survey/20260613T201925262632Z/result.json`
  reported `integration/replication-2` 7/0 and `integration/block-repl` 2/0.

Takeaway:

- The malformed-offset parser path is now faithful enough for the
  `integration/replication` log assertion, while the broader PSYNC reconnect
  gate remains green. The remaining red `integration/replication` work is not
  PSYNC parser behavior; it is the full-sync, diskless, blocked-list, and
  rollback surface already tracked above.

### 2026-06-14 R3 follow-up: PSYNC set-store and fullsync DB prefix kits

Scope:

- The PSYNC no-reconnect Tcl dump first exposed source-dependent set-store
  replay: a replica could replay `SUNIONSTORE` / `SINTERSTORE` /
  `SDIFFSTORE` against later source-set state and produce a different
  destination. The set commands now suppress verbatim propagation and emit
  deterministic destination updates: `DEL dst`, followed by sorted `SADD dst`
  batches when the result is non-empty.
- The next Tcl dump exposed full-sync catch-up DB drift: catch-up bytes after
  an RDB load can begin while the master stream is logically on DB 9, but the
  new replica starts applying post-RDB commands from DB 0. Fresh full sync now
  appends a real `SELECT <db>` frame after the advertised snapshot offset when
  the replication stream already has a nonzero selected DB.
- The BGSAVE-for-replication job now seeds its active catch-up buffer from
  backlog bytes already appended between the advertised snapshot offset and
  job installation. That keeps the catch-up window ordered as
  `SELECT <db>` followed by writes that arrive while the child is active.
- New fast coverage:
  `psync_reconnect_kit::fresh_fullsync_catchup_prefixes_selected_db_before_first_active_write`,
  `repl_correctness_kit::r1_set_store_commands_rewrite_to_deterministic_destination_updates`,
  and
  `repl_correctness_kit::r1_set_store_first_fullsync_catchup_rewrite_selects_db9`.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture --test-threads=1
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture --test-threads=1
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture --test-threads=1
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture --test-threads=1
cargo test -p redis-commands --test aof_correctness_kit -- --nocapture --test-threads=1
cargo test -p redis-commands replication::tests -- --nocapture --test-threads=1
cargo build -p redis-server --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id repl-psync-selected-db-kit-fix \
  --profile integration-repl \
  --timeout-s 360 \
  --baseport 32000 \
  --portcount 4000 \
  --clients 1 \
  --isolated-tests-copy \
  --skip-build \
  --files integration/replication-psync
```

Results:

- `repl_correctness_kit`: 34 passed, 0 failed.
- `psync_reconnect_kit`: 18 passed, 0 failed.
- `fullsync_lifecycle_kit`: 12 passed, 0 failed.
- `repl_buffer_kit`: 15 passed, 0 failed.
- `aof_correctness_kit`: 18 passed, 0 failed.
- `redis-commands` replication unit filter: 13 passed, 0 failed.
- `cargo build -p redis-server --bin redis-server`: passed.
- Focused `integration/replication-psync`
  `harness/oracle/results/tcl-survey/20260614T091542767472Z/result.json`:
  timed out in
  `Test replication partial resync: no reconnection, just sync (diskless: no, disabled, dual-channel: no, reconnect: 0)`.
  The visible dump changed again: the earlier DB 0/DB 9 duplicated zset is
  gone, and the remaining diff is one replica-only DB 0 set row with member
  `597971278521`.

Takeaway:

- The long Tcl command was useful as a scoreboard only. The productive loop was
  to turn each dump signature into a Rust kit, fix that invariant, and rerun
  the broad Tcl file once. The next PSYNC slice should target the remaining
  complex-data DB 0 residue directly, likely by replaying active full-sync
  catch-up for the set/list/zset/hash command mix rather than rerunning the
  six-minute Tcl file as the debugger.

### 2026-06-14 R3 follow-up: post-fullsync live-stream SELECT reset

Scope:

- The latest `integration/replication-psync` scoreboard showed one
  replica-only DB 0 set row after earlier DB 9 catch-up fixes. That shape can
  happen when a replica has just received an RDB and starts applying later live
  stream bytes from DB 0, while the primary's replication stream cache still
  believes DB 9 is already selected for older stream consumers.
- `complete_repl_bgsave_transfer` now invalidates the primary replication
  stream selected-DB cache after delivering an RDB to at least one replica. The
  next live write emits a real `SELECT <db>` even if older online consumers had
  already selected that DB.
- New fast coverage
  `repl_correctness_kit::r1_live_write_after_fullsync_forces_select_for_new_send_bulk_replica`
  drives the real full-sync transfer completion path, drains the private RDB
  bulk, dispatches a DB 9 `SADD`, replays the captured live stream on a fresh
  replica starting at DB 0, and proves the write lands only in DB 9.
- This packet intentionally did not rerun the six-minute
  `integration/replication-psync` file. The reduced kit is the debugger; the
  broad Tcl file should be the next scoreboard or nightly confirmation, not the
  inner loop.

Evidence:

```bash
cargo test -p redis-commands --test repl_correctness_kit -- --nocapture --test-threads=1
cargo test -p redis-commands --test psync_reconnect_kit -- --nocapture --test-threads=1
cargo test -p redis-commands --test fullsync_lifecycle_kit -- --nocapture --test-threads=1
cargo test -p redis-commands --test repl_buffer_kit -- --nocapture --test-threads=1
cargo test -p redis-commands --test aof_correctness_kit -- --nocapture --test-threads=1
cargo test -p redis-commands replication::tests -- --nocapture --test-threads=1
cargo test -p redis-core replication::tests -- --nocapture --test-threads=1
cargo build -p redis-server --bin redis-server
```

Results:

- `repl_correctness_kit`: 35 passed, 0 failed.
- `psync_reconnect_kit`: 18 passed, 0 failed.
- `fullsync_lifecycle_kit`: 12 passed, 0 failed.
- `repl_buffer_kit`: 15 passed, 0 failed.
- `aof_correctness_kit`: 18 passed, 0 failed.
- `redis-commands` replication unit filter: 13 passed, 0 failed.
- `redis-core` replication unit filter: 15 passed, 0 failed.
- `cargo build -p redis-server --bin redis-server`: passed.

Takeaway:

- This is the faster path we want for lagging HA/replication fronts: use Tcl
  artifacts to name a failure signature, reduce the signature to a kit, and
  keep coding against the kit until the invariant is fixed. Only rerun the
  long Tcl file when it can answer, "did the visible frontier move?"
