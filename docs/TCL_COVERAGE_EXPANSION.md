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
Latest focused survey, 2026-05-23:
10 files surveyed
173 passed tests counted
0 failed tests counted
0 timed out
4 files aborted before Test Summary
```

| File | Counted result | First abort / key failure | Packet read |
|---|---:|---|---|
| `unit/bitops` | 50 pass / 0 fail | Complete under current deny-tag policy | Done |
| `unit/bitfield` | 18 pass / 0 fail | Complete under current deny-tag policy | Done |
| `unit/geo` | 71 pass / 0 fail | Complete under current deny-tag policy | Done |
| `unit/scan` | 21 pass / 0 fail | Complete under current deny-tag policy | Done |
| `unit/dump` | 13 pass / 0 fail | Complete under current deny-tag policy | Done |
| `unit/info` | 0 pass / 0 fail | Tag-filtered / no meaningful tests under this policy | Runner-scope issue |
| `unit/hyperloglog` | no summary | `PFDEBUG` missing during sparse/dense test | Medium, debug + sparse/dense semantics |
| `unit/scripting` | no summary | `FUNCTION LOAD` missing before basic EVAL block | Large, split functions vs EVAL |
| `unit/sort` | no summary | After `tcl-sort-wire-minimal`: reaches `COMMAND GETKEYS`; two earlier STORE encoding assertions are blocked on the legacy `list-max-ziplist-size` CONFIG alias | Sort wired; connection metadata remains |
| `unit/slowlog` | no summary | `FUNCTION LOAD` missing in scripting slowlog block | Split from core slowlog edges |

Previous baseline before this wave was 62 counted passes, 9 counted failures,
and 8 no-summary abort files. The wave closed GEO, BITOPS, SCAN, BITFIELD, and
DUMP/RESTORE; remaining no-summary files are now true missing-subsystem
frontiers rather than parser/dispatch slips.

## GEO Edge Packet

`tcl-geo-edge-semantics` is scoped to `crates/redis-commands/src/geo.rs` and
the `unit/geo` counted failures from
`harness/evidence/runs/20260523T022906Z-edb96a4-runner-tcl-survey-unswept.json`.
The implementation mirrors `reference/valkey/src/geo.c:533-864` error payload
semantics for the reported edge cases: missing members, missing
`FROMMEMBER`/`FROMLONLAT`, missing `BYRADIUS`/`BYBOX`/`BYPOLYGON`, `ANY`
without `COUNT`, non-positive `COUNT`, store-option incompatibility, and
`BYPOLYGON` vertex-count validation.

This is still telemetry, not a public conformance claim. The local sandbox used
for the implementation could not bind the upstream TCL helper ports, so
`tcl-post-geo-survey` remains the typed evidence gate that must refresh the
post-fix `unit/geo` count on a runner host with loopback bind permission.

## BITOPS Edge Packet

`tcl-bitops-bitcount-bitpos-edges` is scoped to
`crates/redis-commands/src/bitops.rs` and the first `unit/bitops` abort from
the post-GEO survey. The implementation mirrors
`reference/valkey/src/bitops.c:943-1125` for `BITCOUNT key [start [end
[BIT|BYTE]]]` and `BITPOS key bit [start [end [BIT|BYTE]]]`: optional
start-only `BITCOUNT`, argument parse order before key lookup/type checks, unit
validation order, and missing-key `BITPOS` handling.

This remains telemetry. The follow-up `tcl-post-bitops-survey` runner must
refresh whether `unit/bitops` now reaches a counted summary or exposes the next
frontier, likely in the deferred BITFIELD surface.

## SCAN NOSCORES Packet

`tcl-scan-zscan-noscores` is scoped to ZSCAN option parsing and plain-SCAN
option rejection. The implementation mirrors
`reference/valkey/src/db.c:1150-1405` and
`reference/valkey/src/t_zset.c:3828-3836` for `NOSCORES`: ZSCAN accepts the
option and emits members without score bulks, while plain SCAN rejects
`NOSCORES` with the upstream option-specific error.

This remains telemetry. The follow-up `tcl-post-scan-survey` runner must
refresh whether `unit/scan` now reaches a counted summary or exposes the next
frontier in scan cursor fidelity or score formatting.

## BITFIELD Overflow Packet

`tcl-bitfield-overflow-stability` is scoped to BITFIELD overflow parsing and
arithmetic stability in `crates/redis-commands/src/bitops.rs`. The
implementation mirrors `reference/valkey/src/bitops.c:350-640` and
`:1210-1418` for signed minimum integer parsing, unsigned overflow pre-checks
using wrapping arithmetic, and `OVERFLOW FAIL` returning null without writing
when a SET value is outside the declared bitfield range.

This remains telemetry. The follow-up `tcl-post-bitfield-survey` runner must
refresh whether `unit/bitfield` now reaches a counted summary or exposes the
next BITFIELD frontier.

## DUMP / RESTORE Packet

`tcl-dump-restore-minimal` is implemented in `crates/redis-commands/src/persist.rs`
with shared single-object RDB helpers in `redis-core::rdb`. DUMP now emits the
Valkey-compatible payload shape:

```text
<type byte><object payload><u16 RDB version LE><u64 CRC64 LE>
```

RESTORE validates checksum/version, honors strict vs relaxed
`CONFIG SET rdb-version-check`, supports `REPLACE`, `ABSTTL`, `IDLETIME`, and
`FREQ`, and uses the same RDB object serializers as SAVE/LOAD to avoid a second
serialization path. The `unit/dump` telemetry is now 13/13 under the current
deny-tag policy.

## HLL PFDEBUG Dispatch Packet

`tcl-hll-pfdebug-dispatch` is scoped to normal command dispatch and the
TCL-facing PFDEBUG surface in `crates/redis-commands/src/hyperloglog.rs`. The
packet wires `PFDEBUG` into the shared dispatcher and implements
`GETREG`, `DECODE`, `ENCODING`, and `TODENSE` against the existing stored HLL
bytes. It also matches Valkey's packed dense-register sentinel behavior without
adding a second HyperLogLog representation.

This remains telemetry. The focused `unit/hyperloglog` survey now reaches a
summary instead of aborting on unknown `PFDEBUG`:

```text
run: harness/oracle/results/tcl-survey/20260523T052712Z/unit__hyperloglog.json
23 passed
3 failed
0 timed out
0 without summary
```

The remaining focused failures are `PFSELFTEST` dispatch/implementation and the
`hll-sparse-max-bytes` config-driven sparse-to-dense frontier. Those need
separate packets; this packet does not rewrite PFADD, PFCOUNT, or PFMERGE.

## SORT Wire Packet

`tcl-sort-wire-minimal` is scoped to wiring the already-translated SORT surface
through normal command dispatch. The packet adds `sort.rs` to the command crate,
registers `SORT` and `SORT_RO`, and fills the minimal `CommandContext` helpers
needed by the translation for raw argv parsing, read lookup, hash field lookup,
bulk-object replies, and STORE writes.

The focused `unit/sort` survey now advances past the prior unknown-command
wall:

```text
run: harness/oracle/results/tcl-survey/20260523T054433Z/unit__sort.json
0 counted passed tests
0 counted failed tests
0 timed out
1 without summary
```

The file now stops at `COMMAND GETKEYS sort ...`, which is owned by
`crates/redis-commands/src/connection.rs` and was outside this packet's writable
targets. The two earlier reported STORE assertions are not SORT data failures:
the stored list values and lengths pass, but the test expects quicklist because
`CONFIG GET list-max-ziplist-size` does not expose the legacy alias in the Rust
connection surface. That alias is also in `connection.rs` and needs a separate
metadata/config packet.

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

1. `tcl-slowlog-core-edges`
   - target: `crates/redis-commands/src/slowlog_cmd.rs` and slowlog recording
   - caveat: split around function/scripting-dependent tests
   - why: core slowlog failures are visible before the scripting abort

2. `tcl-hyperloglog-config-selftest`
   - target: `crates/redis-commands/src/hyperloglog.rs`
   - caveat: split `PFSELFTEST` from `hll-sparse-max-bytes` config promotion

3. `tcl-sort-connection-metadata`
   - target: `crates/redis-commands/src/connection.rs`
   - caveat: split from SORT semantics; needed for `COMMAND GETKEYS` and the
     legacy `list-max-ziplist-size` CONFIG alias used by `unit/sort`

4. `tcl-functions-load-minimal`
   - target: function registry / FUNCTION LOAD enough for scripting and slowlog
   - caveat: likely architectural, because it touches scripting command shape

5. `tcl-scripting-function-split`
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
