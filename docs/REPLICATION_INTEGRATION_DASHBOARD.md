# Replication Integration Dashboard

**Status:** R0 baseline refreshed on 2026-06-13.

This dashboard tracks the current `integration-repl` TCL frontier for Valdr
replication work. It is telemetry, not a production HA claim.

## Commands

Fast deterministic gate:

```bash
cargo test -p redis-commands --test repl_correctness_kit
```

Latest result on 2026-06-13 after the R4 role-change unblock packet: 19
passed, 0 failed.

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
| `integration/replication-buffer` | no summary after R2 shortcut removal; prior R0 was 2/13 | Red | Full-sync/BGSAVE setup now aborts before buffer assertions; global replication buffer, backlog growth/shrink, and replica output-buffer limit semantics remain unfinished. |
| `integration/replication` | no summary | Red | Aborts at `diskless replication child being killed is collected` with `child process exited abnormally`; diskless/full-sync behavior remains the frontier. |
| `integration/replication-psync` | timeout | Red | Timed out at 300s; no-backlog/backlog-expired and diskless variants remain frontier. |
| `integration/replication-aof-sync` | 1/5 | Red | RDB-reuse-as-AOF-base and diskless AOF fallback behavior. |
| `integration/replica-redirect` | no summary | Red | Aborts at `client paused before and during failover-in-progress`; `FAILOVER` is still unknown. |
| `unit/wait` | 39/0 | Green | WAIT command suite passed after the R4 role-change unblock packet; WAITAOF/AOF integration still tracked by `replication-aof-sync`. |

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
- `R3-RECONNECT-MATRIX`: extend the new master-side PSYNC decision matrix into
  live replica-dialer reconnect coverage before grinding `replication-psync`.
- `R2-BUFFER-LIMITS`: accounting aliases and fan-out accounting are covered;
  implement shared-buffer trimming and replica output-buffer disconnection
  semantics behind `replication-buffer`.
- `R4-WAIT/WAITAOF`: role-change unblock now covers both WAIT and WAITAOF for
  `REPLICAOF` topology changes; replica FACK/disconnect semantics and
  `replication-aof-sync` remain open.
- `R5-FAILOVER-PARSER`: start failover syntax and faithful errors before any
  HA claim.

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
red and currently aborts before counted buffer assertions.

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
- `integration/replication-buffer`:
  `harness/oracle/results/tcl-survey/20260613T042757054194Z/result.json`
  reported no summary and an exception at `fail to sync with replicas`, before
  the buffer assertions. This is blocked by the full-sync/BGSAVE frontier, not
  evidence that buffer limits are correct.
- Focused dual-server tripwire:
  `harness/oracle/results/tcl-survey/20260613T042825912490Z/result.json`
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
extended with replica target-change state hardening on 2026-06-13.

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

Evidence:

```bash
cargo test -p redis-commands \
  replication::tests::psync_decision_matrix_covers_reconnect_edges \
  -- --nocapture
cargo test -p redis-commands replication::tests::psync -- --nocapture
cargo test -p redis-core \
  replication::tests::target_change_resets_cached_partial_resync_state \
  -- --nocapture
cargo test -p redis-commands --test repl_correctness_kit
cargo check -p redis-core -p redis-commands -p redis-server
```

Results:

- Focused decision-matrix unit test: passed.
- Focused PSYNC unit filter: 2 passed, 0 failed.
- Focused target-change state unit test: passed.
- `repl_correctness_kit`: 18 passed, 0 failed.
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
