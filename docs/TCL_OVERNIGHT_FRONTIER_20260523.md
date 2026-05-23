# TCL Overnight Frontier — 2026-05-23

Status: HLL PFDEBUG and SORT wire packets applied; remaining frontier updated.

## Goal

Use the current post-DUMP baseline to push the next official TCL frontier
without drifting away from a faithful single-node Valkey-compatible server.
The loop should prefer bounded conformance packets with objective survey gates
over local one-off fixes.

Current focused frontier baseline after `tcl-frontier-baseline-after-dump`:

```text
10 files surveyed
173 passed tests counted
0 failed tests counted
0 timed out
4 files aborted before Test Summary
```

Counted-green files: `unit/bitops`, `unit/bitfield`, `unit/geo`,
`unit/scan`, `unit/dump`.

Remaining no-summary files are true missing-subsystem frontiers:

- `unit/sort` — `SORT` / `SORT_RO` are now wired through normal dispatch.
  The focused file advances to connection metadata gaps: `COMMAND GETKEYS`
  and the legacy `list-max-ziplist-size` CONFIG alias.
- `unit/slowlog` — core SLOWLOG exists, but edge semantics still diverge; the
  tail is blocked on `FUNCTION LOAD` / `FCALL`.
- `unit/scripting` — the test alternates EVAL and FUNCTION modes, so a minimal
  function registry is needed before the EVAL body can be surveyed deeply.

Post-`tcl-hll-pfdebug-dispatch` focused update:

```text
unit/hyperloglog
23 passed tests counted
3 failed tests counted
0 timed out
0 files aborted before Test Summary
```

Evidence:
`harness/oracle/results/tcl-survey/20260523T052712Z/unit__hyperloglog.json`.
The remaining HLL failures are `PFSELFTEST` and config-driven
`hll-sparse-max-bytes` sparse-to-dense behavior. Treat the combined frontier as
196 counted passes, 3 counted failures, and 3 no-summary files only as a
telemetry projection from the prior 10-file baseline plus this focused HLL run.

Post-`tcl-sort-wire-minimal` focused update:

```text
unit/sort
0 counted tests
0 timed out
1 without summary
first abort: SORT extracts STORE correctly
exception: ERR Unknown COMMAND subcommand: getkeys.
```

Evidence:
`harness/oracle/results/tcl-survey/20260523T054433Z/unit__sort.json`.
This proves the packet cleared the unknown `SORT` command wall. The two STORE
assertions immediately before the abort are connection/config metadata drift:
the stored values and lengths pass, while expected quicklist/listpack encoding
uses the legacy `list-max-ziplist-size` alias that `connection.rs` does not yet
expose.

## Packet Strategy

1. `tcl-frontier-baseline-after-dump`
   - Runner-only snapshot. This proves the run starts from the expected
     173/0/4 frontier.

2. `tcl-hll-pfdebug-dispatch`
   - Applied. `PFDEBUG` now uses the shared dispatcher and the existing HLL
     bytes for `GETREG`, `DECODE`, `ENCODING`, and `TODENSE`. The focused
     `unit/hyperloglog` survey reaches a counted summary at 23 pass / 3 fail.

3. `tcl-hll-config-selftest`
   - Split follow-up for `PFSELFTEST` and `hll-sparse-max-bytes` promotion.
     Keep PFADD, PFCOUNT, and PFMERGE semantics faithful to the stored HLL
     representation.

4. `tcl-sort-wire-minimal`
   - Applied. `sort.rs` is compiled into the crate, `SORT` and `SORT_RO` are
     registered in the shared dispatcher, and minimal context helpers support
     BY/GET/LIMIT/ASC/DESC/ALPHA/STORE/SORT_RO STORE rejection.
   - Follow-up: split `tcl-sort-connection-metadata` for `COMMAND GETKEYS` and
     legacy `list-max-ziplist-size` CONFIG alias behavior in `connection.rs`.

5. `tcl-slowlog-core-edges`
   - Fix core SLOWLOG semantics that are independent of scripting functions:
     count argument validation, original-argv logging where possible, and
     blocking-command logging if the existing blocked-command path exposes the
     right hook.

6. `tcl-functions-load-fcall-minimal`
   - Minimal Lua function bridge for `FUNCTION LOAD [REPLACE]` and
     `FCALL`/`FCALL_RO`, backed by the existing EVAL machinery. Scope is
     deliberately smaller than full Valkey functions: enough for the TCL
     scripting and slowlog frontiers to start executing function-mode tests.

Every implementation packet is followed by the same `tcl-survey-unswept`
runner. The loop is allowed to stop on repeated packet failure; repeated
failure should produce a blocker row instead of burning the night.

## Non-Goals

- No cluster/module/Sentinel/TLS expansion.
- No benchmark-only shortcuts.
- No weakening of wire-diff or RDB oracles.
- No broad workspace formatting churn.
- No fake function API that returns OK but cannot execute the loaded body.
