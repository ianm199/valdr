# Replication Integration Dashboard

**Status:** R0 baseline refreshed on 2026-06-13.

This dashboard tracks the current `integration-repl` TCL frontier for Valdr
replication work. It is telemetry, not a production HA claim.

## Commands

Fast deterministic gate:

```bash
cargo test -p redis-commands --test repl_correctness_kit
```

Latest result on 2026-06-13 after the R5 parser packet: 21 passed, 0 failed.

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
| `integration/replication-2` | 7/0 | Green | No-regression tripwire. |
| `integration/block-repl` | 2/0 | Green | No-regression tripwire. |
| `integration/replication-3` | 3/4 | Red | Expiry consistency, writable-replica expired-key behavior, and PFCOUNT expired-key/cache semantics. |
| `integration/replication-4` | 15/2 | Red | SPOP rewrite cases now pass; remaining failures are divergence/default writable-replica cases. |
| `integration/replication-buffer` | 4/11 | Red | Fresh post-PSYNC baseline still reaches counted assertions without timeout. Shared vs private output-buffer semantics are now pinned in `repl_buffer_kit`; remaining failures are Valkey-style global replication-buffer memory, BGSAVE/slow-replica backlog outgrowth, partial resync across broader retained history, and typed live output-buffer disconnect/drain policy. |
| `integration/replication` | no summary | Red | The failed full-sync cleanup packet moves past `diskless replication child being killed is collected`; the current abort is `Master stream is correctly processed while the replica has a script in -BUSY state` with `READONLY`. Diskless/script-busy full-sync apply remains the frontier. |
| `integration/replication-psync` | 90/0 | Green | Focused gate is green after live backlog resize, `repl-backlog-ttl` expiry, stale replica entry cleanup, and `DEBUG SLEEP` pause support for the replica dialer. |
| `integration/replication-aof-sync` | 6/0 | Green | Full-sync AOF base refresh, disk-based RDB reuse, diskless BGREWRITEAOF fallback, and stale local RDB restart coverage now pass. |
| `integration/replica-redirect` | timeout | Red | `CLIENT CAPA REDIRECT` top-level and MULTI/EXEC replica redirect semantics now pass the early file assertions. Manual `FAILOVER` now reaches timeout-driven handoff in Rust kits and a live two-process probe, including blocked `BRPOP` and paused `GET` REDIRECT after promotion. The official Tcl file still no-summary times out in the first failover test; side observation showed old primary `blocked_clients:2`, `paused_actions:all`, and `role:slave` while the target remained SIGSTOP'd. |
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
- `R2-RDB-BULK-FAITHFUL`: the old `REPLICAOF` pre-PSYNC `KEYS`/`DUMP` seed
  shortcut is removed, so remaining full-sync work must pass through the
  streamed RDB handoff path.
- `R2-BGSAVE-WINDOW`: replication BGSAVE now reports through `INFO persistence`
  and honors the bounded debug save delay; keep extending this into the
  diskless/full-sync windows behind `integration/replication`. Failed
  full-sync BGSAVE jobs now clean up waiters, temp files, and replication-child
  state instead of poisoning later sync attempts.
- `R2-BGSAVE-CATCHUP`: active replication BGSAVE jobs now retain appended
  replication bytes outside the circular backlog and use that buffer for
  post-RDB catch-up. Completed full-sync catch-up bytes are now also retained
  while dependent replicas still pin them.
- `R3-RECONNECT-MATRIX`: extend the new master-side PSYNC decision matrix into
  live replica-dialer reconnect coverage before grinding `replication-psync`.
- `R2-BUFFER-LIMITS`: accounting aliases, fan-out accounting, and retained
  full-sync history are covered; implement broader shared-buffer memory
  accounting, backlog outgrowth under slow online replicas, and replica
  output-buffer disconnection semantics behind `replication-buffer`.
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
  good old data unless the incoming RDB is valid." The broader
  `integration/replication` gate remains blocked at script-busy stream apply,
  so the next full-sync lifecycle slice should model primary-link command
  application around BUSY/script state instead of touching RDB replacement.

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
