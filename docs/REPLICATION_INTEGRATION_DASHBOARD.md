# Replication Integration Dashboard

**Status:** R0 baseline refreshed on 2026-06-13.

This dashboard tracks the current `integration-repl` TCL frontier for Valdr
replication work. It is telemetry, not a production HA claim.

## Commands

Fast deterministic gate:

```bash
cargo test -p redis-commands --test repl_correctness_kit
```

Result on 2026-06-13: 13 passed, 0 failed.

Full current integration dashboard:

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

The full dashboard run completed before `tcl-survey.py` started writing the
aggregate `result.json` file. Future runs write that file automatically.

## Current Table

| File | Current result | Status | Frontier |
|---|---:|---|---|
| `integration/replication-2` | 7/0 | Green | No-regression tripwire. |
| `integration/block-repl` | 2/0 | Green | No-regression tripwire. |
| `integration/replication-3` | 7/0 | Green | Earlier command-propagation failures are not present on this tree. |
| `integration/replication-4` | 17/0 | Green | `DEBUG REPLICATE` path is no longer an abort frontier. |
| `integration/replication-buffer` | 2/13 | Red | Global replication buffer, backlog growth/shrink, and replica output-buffer limit semantics. |
| `integration/replication` | no summary | Red | Aborts at `diskless replication child being killed is collected` with `child process exited abnormally`; diskless/full-sync behavior remains the frontier. |
| `integration/replication-psync` | timeout | Red | Timed out at 300s; no-backlog/backlog-expired and diskless variants remain frontier. |
| `integration/replication-aof-sync` | 1/5 | Red | RDB-reuse-as-AOF-base and diskless AOF fallback behavior. |
| `integration/replica-redirect` | no summary | Red | Aborts at `client paused before and during failover-in-progress`; `FAILOVER` is still unknown. |

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

The current dashboard changes the emphasis from the older overnight notes:
`replication-3` and `replication-4` are green now. The next replication packets
should still keep R1 command-propagation tests as regression coverage, but the
largest visible integration frontiers are now:

- `R3-RECONNECT-MATRIX`: clarify the remaining PSYNC no-backlog/backlog-expired
  cases in the deterministic kit before grinding `replication-psync`.
- `R2-BUFFER-LIMITS`: implement/account for replication buffer and output-buffer
  semantics behind `replication-buffer`.
- `R5-FAILOVER-PARSER`: start failover syntax and faithful errors before any
  HA claim.
