# Replication Integration Dashboard

**Status:** R0 baseline refreshed on 2026-06-13.

This dashboard tracks the current `integration-repl` TCL frontier for Valdr
replication work. It is telemetry, not a production HA claim.

## Loop Policy

Use full Tcl files as scoreboards, not as the debugger. The normal loop for a
red replication surface is:

1. Read the latest Tcl artifact and name one concrete failure signature.
2. Reduce that signature into a Rust kit or narrow unit test.
3. Fix against the kit and run file-scoped gates.
4. Run a focused Tcl selector only when the kit predicts useful movement.
5. Save the long full-file Tcl command for batch scoreboards or nightly runs.

## Commands

Fast deterministic replication/HA gate:

```bash
make repl-kits
```

For a single focused kit:

```bash
make repl-kits REPL_KITS=psync_reconnect_kit
```

Use this kit lane as the normal debugger before reaching for a full Tcl
scoreboard. It runs the replication correctness, backlog/buffer,
full-sync-lifecycle, PSYNC reconnect, failover redirect, and replica dialer
Rust tests without starting the upstream Tcl matrix.

Latest result on 2026-06-14: `make repl-kits` passed 142/142 tests.
The adjacent runtime-owner unit lane also passed 13/13 tests after reducing a
`repl-diskless-load swapdb` PSYNC DB-selection mismatch:

```bash
cargo test -p redis-server runtime_owner::tests
```

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
| `integration/replication-buffer` | 16/0 | Green | The replication-buffer kit line now covers active full-sync catch-up beyond the circular backlog, selected-DB full-sync prefixes appended into the active job before backlog wrap, partial resync from retained shared history, retained-history release after the last dependent replica disconnects, shared output memory charged once, and hard-limit disconnect isolation. Follow-up Tcl scoreboards moved the file through 13/3, 15/1, and finally 16/0 at artifact `20260614T071942726290Z`; keep `repl_buffer_kit` as the inner loop and rerun this Tcl file only as a regression scoreboard. |
| `integration/replication` | 54/13 | Red | Full-sync lifecycle work moved past killed-child cleanup, script-busy READONLY, FCALL READONLY, async-loading CONFIG exceptions, successful swapdb function payloads, parent-killed child discovery, `repl-diskless-load on-empty-db`, no-longer-useful RDB child cancellation, replica-link reply violations, malformed-PSYNC-offset logging, chained replica `FLUSHDB` / `FLUSHALL` stream relay, `GETSET` rewrite, nonblocking `BRPOPLPUSH` / `BLMOVE` rewrite stats, empty-blocking commandstats, replica output-byte stats, BLPOP role-change divergence, `replicas_waiting_psync` visibility, handshake-timeout detection, line-224 `MULTI`/`SLAVEOF`/`INFO`/`EXEC`, three-replica full-sync/write-load offset convergence, killed swapdb full-sync sockets, `repl-diskless-load flush-before-load` owner-DB clearing, lazy-expire recreate propagation, and disk-based RDB rename-failure rollback. The current clean-port full 2026-06-14 scoreboard `20260614T154218963678Z` completes with 54 passed, 13 failed, 0 timed out, 0 without summary, and 26 parsed failure lines. Remaining scoreboard failures cluster around diskless pipe/log observability, `replicaof` immediately after disconnection, cache-master/fullsync load behavior, and EINTR watchdog. The latest checkpoint adds replica stdout markers for short-read/error, fullsync success, and partial-resync success paths; `make repl-kits` is green, but the long Tcl re-score after the completion-marker addition is intentionally deferred. Focused Tcl `--only` remains unreliable for this file because earlier top-level setup can still abort; use process kits as reducers and full-file Tcl as the scoreboard. |
| `integration/replication-psync` | 90/0 | Green | Historical focused gate was 90/0 after live backlog resize, `repl-backlog-ttl` expiry, stale replica entry cleanup, and `DEBUG SLEEP` pause support. Later full-file reruns regressed to timeouts and digest mismatches, but the kit-first loop moved the visible frontier through raw `-0` RDB fidelity, deterministic set/zset store rewrites, selected-DB full-sync catch-up, post-fullsync live-stream DB selection, in-flight BGSAVE waiter offset reuse, fresh full-sync snapshot barriers, no-reconnect `repl-diskless-load swapdb`, same-primary socket-drop partial reconnect, delayed replica-side `CLIENT KILL`/`DEBUG SLEEP` reconnect, DB 0 final-list-pop catch-up, DB 11 final-`HDEL` catch-up, DB 11 binary-field final-`HDEL` catch-up, DB 0 final-list-pop partial reconnect, and Tcl-style stop-after-online bg_complex writers. The no-quiet artifact `20260614T133752958562Z` captured a swapdb digest mismatch, and `20260614T135259082789Z` later timed out with four parsed PSYNC failures. The final stop-timed reducer exposed the real transfer-window bug: the reaper took the active BGSAVE job before reading/queuing the RDB, so writes during that window could miss both job catch-up and live fan-out or interleave ahead of catch-up. Production now keeps the job installed while reading the temp RDB and uses the full-sync snapshot barrier while taking the job, computing catch-up, queuing RDB/catch-up, and only then exposing replicas to live fan-out. Full `integration/replication-psync` is green at artifact `20260614T143228939871Z`: 90 passed, 0 failed, 0 timed out, 0 parsed failure lines. Keep the new process kit as the inner loop and rerun this Tcl file only as a regression scoreboard. |
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

Keep this list short enough to scan before a coding run. Long-form planning
lives in [`HA_CLUSTER_REPLICATION_ROADMAP.md`](HA_CLUSTER_REPLICATION_ROADMAP.md);
the previous dashboard detail is preserved in
[`docs/history/REPLICATION_NEXT_PACKET_DETAIL_20260615.md`](history/REPLICATION_NEXT_PACKET_DETAIL_20260615.md).

| Priority | Packet | Why now | First gate |
|---:|---|---|---|
| 1 | Expiry-on-replica semantics | `replication-3` is still red on expire consistency, writable-replica expired-key behavior, and PFCOUNT expired-key/cache cases. | Extract a Rust reducer before rerunning `integration/replication-3`. |
| 2 | `R2-BGSAVE-WINDOW` | `integration/replication` remains the broadest red file; open work clusters around diskless/full-sync windows, rollback, and pipe cleanup. | `fullsync_lifecycle_kit`, then full `integration/replication` as scoreboard. |
| 3 | `R2-BUFFER-LIMITS` | `replication-buffer` is green but still needs broader shared-buffer and slow-replica output-limit accounting before beta claims. | `repl_buffer_kit`. |
| 4 | `R4-WAIT/WAITAOF` | Role-change unblock is covered; replica FACK/disconnect semantics remain open. | `repl_correctness_kit` plus focused WAITAOF reducers. |
| 5 | `R5-MANUAL-FAILOVER` | Parser/state visibility exists; real pause, offset wait, promotion/demotion, and blocked-client redirect behavior remain the HA gap. | `failover_redirect_kit` and `replica_dialer::tests`. |

## Packet Evidence Archive

Historical packet writeups moved to
[`docs/history/REPLICATION_INTEGRATION_PACKET_EVIDENCE.md`](history/REPLICATION_INTEGRATION_PACKET_EVIDENCE.md)
on 2026-06-15. Keep this dashboard focused on current results, fast gates,
and next useful packets; add long-form packet evidence to the archive.
