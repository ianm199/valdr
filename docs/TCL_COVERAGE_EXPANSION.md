# TCL Coverage Expansion

Status: telemetry runner added, 2026-05-22. This document is about widening
official-suite visibility, not changing the public compatibility claim.

## Why This Exists

`docs/CONFORMANCE.md` reports strong coverage on the core surveyed unit files,
but it also names several unswept upstream TCL files. Many of those files are
not empty product gaps: the command modules already exist, but the upstream file
was never put behind a safe harness runner. That makes the coverage question too
manual and too easy to misread.

The new `tcl-survey-unswept` runner gives the harness a bounded way to ask:

- which unswept files run to a summary;
- which abort immediately on one missing command or edge case;
- which need a split runner because the first abort hides a larger body;
- which failures are cheap edge semantics versus large subsystem work.

The runner is telemetry-only. It emits `claim_level = "telemetry"` rows and raw
logs under `harness/oracle/results/tcl-survey/`. A green or red result here does
not automatically become a README conformance claim.

## Runner

```bash
python3 harness/oracle/tcl-survey.py --skip-build --timeout-s 90 \
  --files unit/bitops,unit/bitfield,unit/geo,unit/hyperloglog,unit/scripting,unit/scan,unit/sort,unit/dump,unit/info,unit/slowlog
```

Harness entry:

- runner: `tcl-survey-unswept`
- kind: `json_command`
- method: `official-suite`
- work packet: `tcl-survey-unswept`
- selector: `manual`

The runner executes each TCL file independently with a per-file timeout. It
captures:

- pass/fail counts when upstream emits `Test Summary`;
- timeout/no-summary status;
- the first aborting test where parseable;
- the exception text;
- raw stdout/stderr JSON for audit.

## First Survey Snapshot

Command:

```bash
python3 harness/oracle/tcl-survey.py --skip-build --timeout-s 90 \
  --files unit/bitops,unit/bitfield,unit/geo,unit/hyperloglog,unit/scripting,unit/scan,unit/sort,unit/dump,unit/info,unit/slowlog
```

Summary:

```text
10 files surveyed
62 passed tests counted
9 failed tests counted
0 timed out
8 files aborted before Test Summary
```

| File | Counted result | First abort / key failure | Packet read |
|---|---:|---|---|
| `unit/geo` | 62 pass / 9 fail | Error wording and option-combination semantics | Small, high ROI |
| `unit/info` | 0 pass / 0 fail | Tag-filtered / no meaningful tests under this policy | Runner-scope issue |
| `unit/bitops` | no summary | `BITCOUNT with just start`: `ERR syntax error` | Small, edge semantics |
| `unit/bitfield` | no summary | `BITFIELD overflow detection fuzzing`: I/O error | Stabilization / crash packet |
| `unit/hyperloglog` | no summary | `PFDEBUG` missing during sparse/dense test | Medium, debug + sparse/dense semantics |
| `unit/scripting` | no summary | `FUNCTION LOAD` missing before basic EVAL block | Large, split functions vs EVAL |
| `unit/scan` | no summary | `ZSCAN ... NOSCORES`: `ERR syntax error` | Small/medium |
| `unit/sort` | no summary | `SORT BY`: unknown command | Medium if dispatch gap, large if semantics absent |
| `unit/dump` | no summary | `DUMP`: unknown command | Medium/large, product valuable |
| `unit/slowlog` | no summary | `FUNCTION LOAD` missing in scripting slowlog block | Split from core slowlog edges |

## What This Says About "Spark Skeleton" Work

Do not use cheap agents to mass-author trusted command behavior. The useful
fast path is narrower:

1. Run this survey to find aborting frontiers.
2. Use cheap agents for file/test bucketing and source mapping.
3. Convert the bucket into typed packets with one upstream file frontier each.
4. Use the normal harness loop for implementation, oracle evidence, and commit.

Broad skeleton generation is still useful for leaf discovery or fixture
generation, but it should not be wired into dispatch unless a typed runner proves
the behavior.

## Packet Candidates

Recommended next packet order:

1. `tcl-geo-edge-semantics`
   - target: `crates/redis-commands/src/geo.rs`
   - expected lift: the 9 counted `unit/geo` failures
   - why first: file runs to summary, failures are explicit and bounded

2. `tcl-bitops-bitcount-bitpos-edges`
   - target: `crates/redis-commands/src/bitops.rs`
   - first frontier: `BITCOUNT key start` accepted by upstream
   - why: likely a small parser/arity semantics fix, then more of the file opens

3. `tcl-scan-zscan-noscores`
   - target: scan/zset command handling
   - first frontier: `ZSCAN zset 0 COUNT 1000 NOSCORES`
   - why: option parsing edge; good conformance win if contained

4. `tcl-slowlog-core-edges`
   - target: `crates/redis-commands/src/slowlog_cmd.rs` and slowlog recording
   - caveat: split around function/scripting-dependent tests
   - why: core slowlog failures are visible before the scripting abort

5. `tcl-dump-restore-minimal`
   - target: DUMP/RESTORE dispatch and RDB single-key payload compatibility
   - why: product-useful, but larger than error wording fixes

6. `tcl-bitfield-overflow-stability`
   - target: `crates/redis-commands/src/bitops.rs`
   - first frontier: overflow fuzzing I/O error
   - why: crash/lost-connection behavior is higher severity than ordinary mismatch

7. `tcl-hyperloglog-sparse-dense`
   - target: `crates/redis-commands/src/hyperloglog.rs`
   - caveat: `PFDEBUG` may need a test-only/official-suite-compatible subset

8. `tcl-sort-wire`
   - target: sort command registration and semantics
   - caveat: likely large if implementation is not wired to current context

9. `tcl-scripting-function-split`
   - target: scripting/functions
   - caveat: split Valkey `FUNCTION` support from baseline `EVAL` compatibility

## Design Notes

- A file aborting before `Test Summary` is not a runner failure. It is a packet
  generation signal.
- A counted pass/fail result is more actionable than an abort because it exposes
  the whole file's frontier.
- Keep the deny tags explicit. The current survey denies `needs:repl`,
  `needs:debug`, and `external:skip`; changing that policy changes the meaning
  of the numbers.
- Use this as an architecture input before widening `docs/CONFORMANCE.md`.
  Public conformance should stay tied to stable, repeatable gates.
