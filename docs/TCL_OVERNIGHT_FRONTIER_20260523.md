# TCL Overnight Frontier — 2026-05-23

Status: queued for an unattended Codex-backed harness run.

## Goal

Use the current post-DUMP baseline to push the next official TCL frontier
without drifting away from a faithful single-node Valkey-compatible server.
The loop should prefer bounded conformance packets with objective survey gates
over local one-off fixes.

Current focused frontier baseline:

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

- `unit/hyperloglog` — `PFDEBUG` is implemented as a function but not wired
  through dispatch.
- `unit/sort` — `SORT` / `SORT_RO` are translated in `sort.rs` but the module
  is not wired and likely needs compile/API catch-up.
- `unit/slowlog` — core SLOWLOG exists, but edge semantics still diverge; the
  tail is blocked on `FUNCTION LOAD` / `FCALL`.
- `unit/scripting` — the test alternates EVAL and FUNCTION modes, so a minimal
  function registry is needed before the EVAL body can be surveyed deeply.

## Packet Strategy

1. `tcl-frontier-baseline-after-dump`
   - Runner-only snapshot. This proves the run starts from the expected
     173/0/4 frontier.

2. `tcl-hll-pfdebug-dispatch`
   - Smallest likely win. `pfdebug_command` already exists in
     `hyperloglog.rs`; the packet wires command dispatch and fixes only the
     immediate TCL-facing PFDEBUG surface.

3. `tcl-sort-wire-minimal`
   - Wire `sort.rs` into the crate and dispatch table, then repair compile/API
     gaps until `unit/sort` advances past the unknown-command/compile wall.
     This is a larger packet but it uses already-ported source rather than
     greenfield behavior.

4. `tcl-slowlog-core-edges`
   - Fix core SLOWLOG semantics that are independent of scripting functions:
     count argument validation, original-argv logging where possible, and
     blocking-command logging if the existing blocked-command path exposes the
     right hook.

5. `tcl-functions-load-fcall-minimal`
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

