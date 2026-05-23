# TCL Coverage Long Run

Status: queued 2026-05-22. This is a harness-driven conformance expansion wave.

## Goal

Turn the new `tcl-survey-unswept` telemetry into a longer implementation run
that widens official upstream TCL coverage without changing the public
conformance claim prematurely.

The run is deliberately not "port everything missing." It targets the first
frontiers where the survey gives objective, bounded feedback:

1. `unit/geo` counted failures.
2. `unit/bitops` BITCOUNT/BITPOS parsing and error semantics.
3. `unit/scan` ZSCAN `NOSCORES`.
4. `unit/bitfield` overflow stability / connection-loss abort.

After each implementation packet the same survey runner runs again, so progress
is visible as ledger rows instead of prose.

## Packet Graph

```text
tcl-survey-unswept
  |
  v
tcl-geo-edge-semantics
  |
tcl-post-geo-survey
  |
  v
tcl-bitops-bitcount-bitpos-edges
  |
tcl-post-bitops-survey
  |
  v
tcl-scan-zscan-noscores
  |
tcl-post-scan-survey
  |
  v
tcl-bitfield-overflow-stability
  |
tcl-post-bitfield-survey
```

## GEO Packet Note

`tcl-geo-edge-semantics` targets only GEO option/error edge semantics in
`crates/redis-commands/src/geo.rs`. It does not broaden GEO performance work or
zset behavior. The follow-up `tcl-post-geo-survey` runner remains required to
refresh the official `unit/geo` count because local implementation verification
was limited by loopback bind denial in the Codex sandbox.

## BITOPS Packet Note

`tcl-bitops-bitcount-bitpos-edges` targets only BITCOUNT/BITPOS parser and
range-edge semantics in `crates/redis-commands/src/bitops.rs`, matching
`reference/valkey/src/bitops.c:943-1125`. It accepts Valkey's start-only
`BITCOUNT` form and preserves upstream error precedence by parsing numeric
range arguments and optional `BIT|BYTE` units before key lookup/type checks.

It deliberately does not touch BITFIELD. The follow-up
`tcl-post-bitops-survey` runner remains the telemetry gate for the refreshed
`unit/bitops` frontier.

## Why These Four

`unit/geo` is first because it reaches `Test Summary`, so the pass/fail count is
not hidden behind an abort. The failure class appears to be option validation
and error behavior, not a missing subsystem.

`unit/bitops` is next because it aborts on a small upstream-anchored parser
difference: `BITCOUNT key start` is legal in Valkey. Fixing that should expose
more of the file and may recover several simple edge tests.

`unit/scan` is next because `NOSCORES` is a recent but bounded ZSCAN option.
This is a good test of whether the harness can drive one upstream-test frontier
without broad sorted-set churn.

`unit/bitfield` is last in this wave because the current failure is a lost
connection during overflow fuzzing. That is higher severity than a text
mismatch, but the arithmetic surface is more delicate than the first three
packets.

## Deliberately Deferred

- `unit/dump`: product-useful, but likely needs DUMP/RESTORE payload format and
  checksum work. Do this as its own architecture packet, not as a tail-end fix.
- `unit/sort`: `sort.rs` exists but is not wired into `lib.rs`/dispatch. It may
  be a compile/API catch-up packet before semantics. Keep it separate.
- `unit/scripting` and slowlog's function-dependent tail: `FUNCTION LOAD`
  support is its own subsystem.
- `unit/hyperloglog`: `PFDEBUG` may need either debug-command policy or a
  test-compatible subset; not a first-wave packet.

## Run Command

Recommended autonomous Codex run:

```bash
python3 ../port-harness/loop/run-loop.py \
  --project . \
  --reset \
  --selector manual \
  --packet tcl-geo-edge-semantics \
  --auto-dispatch \
  --dispatch-runtime codex \
  --dispatch-timeout-s 2400 \
  --max-iterations 9 \
  --max-failures 2 \
  --max-same-packet-failures 1
```

This starts at the first implementation packet, then continues through manual
runner and implementation packets in phase order.

## Tracking

Completion/queue:

```bash
python3 ../port-harness/loop/check-completion.py --project . --json | jq '.done, .results[-8:]'
python3 ../port-harness/loop/parallel-plan.py --project . --selector manual --json | jq '.selected'
```

Latest TCL evidence:

```bash
python3 - <<'PY'
import json
from pathlib import Path

for line in Path("harness/evidence/ledger.jsonl").read_text().splitlines()[-120:]:
    row = json.loads(line)
    if row.get("packet", "").startswith("tcl-"):
        print(row.get("ts"), row.get("kind"), row.get("packet"), row.get("metric"), row.get("test"), row.get("value"), row.get("summary"))
PY
```

Latest transcripts:

```bash
ls -lat harness/loop/state/transcripts | head
```

## Interpretation

The primary metric is not total counted passes alone. It is:

- fewer `tcl_file_no_summary` rows;
- `unit/geo` moving toward 71/0;
- `unit/bitops` and `unit/scan` moving from abort to counted summaries;
- no regressions in `wire-smoke`.

If a packet fails, treat the transcript and focused survey log as architecture
evidence for the next cut. Do not widen the packet mid-run.
