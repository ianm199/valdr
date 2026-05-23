# TCL Next Frontier — 2026-05-23

Status: packet plan for the next conformance push after commit `80f7dd5`.

## Evidence Baseline

Latest focused survey evidence:

```text
harness/evidence/runs/20260523T154306Z-ae8f997-runner-tcl-post-functions-minimal-survey-v2.json

10 files surveyed
261 counted passes
5 counted failures
0 timed out
1 file without Test Summary
```

Green in this focused frontier:

- `unit/bitops`: 50/50
- `unit/bitfield`: 18/18
- `unit/geo`: 71/71
- `unit/hyperloglog`: 26/26
- `unit/scan`: 21/21
- `unit/dump`: 13/13
- `unit/slowlog`: 13/13

Remaining red surface:

- `unit/sort`: 49/54, with five concrete failures.
- `unit/scripting`: aborts before Test Summary after three reported failures and
  missing `WAITAOF`.
- `unit/info`: no counted tests in this focused file set; not a blocker for the
  next SORT/scripting frontier.

## Why V3 Exists

The v2 loop made real progress but hit two harness-shape issues:

1. `tcl-slowlog-core-edges-v2` was semantically fixed but the packet remained
   blocked because record-completion was run from a dirty tree. The later
   functions survey proves `unit/slowlog` is green.
2. Old blocked packet names still control completion and scheduler reasoning.
   The next wave should not try to revive stale rows. It should start from a
   clean commit and cut packets from current red evidence.

The v3 graph is therefore a clean frontier graph, not a rewrite of history.

## V3 Packet Graph

```text
tcl-frontier-v3-baseline
  ├─ tcl-sort-final-five-v3
  │    └─ tcl-post-sort-final-survey-v3
  └─ tcl-scripting-conversion-select-v3
       └─ tcl-post-scripting-conversion-survey-v3
            └─ tcl-scripting-wait-waitaof-v3
                 └─ tcl-post-scripting-wait-survey-v3
                      └─ tcl-core-expanded-survey-v2
```

## SORT Frontier

Current failing tests in `reference/valkey/tests/unit/sort.tcl`:

- `SORT BY key STORE`
- `SORT BY hash field STORE`
- `SORT sorted set BY nosort works as expected from scripts`
- `SORT will complain with numerical sorting and bad doubles (1)`
- `SORT will complain with numerical sorting and bad doubles (2)`

Failure classes:

- Stored SORT list reports `OBJECT ENCODING` as `listpack`, while upstream
  expects `quicklist`.
- SORT from scripts returns empty arrays for the sorted-set `BY nosort` case.
- Numeric sort errors say `One or more scores can't be converted into double`
  but the TCL pattern expects an `ERR` prefix containing `double`.

Packet target: fix only these five failures through normal SORT, object
encoding, and Lua reply-conversion paths. Do not special-case test names.

## Scripting Frontier

Current failures in `reference/valkey/tests/unit/scripting.tcl`:

- `EVAL - Scripts do not block on wait`
- abort at `WAITAOF` unknown command during script/function mode

Failure classes:

- `WAIT`/`WAITAOF` need script-safe nonblocking behavior. For the current
  single-node non-replication envelope, returning zero is faithful enough for
  these tests if the command path remains normal and explicit.

Bronze status:

- `47d7fbf` fixed the Lua reply conversion / selected-DB restoration packet.
- The focused scripting survey now reaches the planned Silver frontier:
  `EVAL - Scripts do not block on wait`, then aborts at unknown `WAITAOF`.

Next packet target: fix script-safe WAIT/WAITAOF behavior without broad
replication work or dispatcher bypasses.

## Run Discipline

- Every implementation packet has a focused TCL runner gate.
- Runner evidence is telemetry until `docs/CONFORMANCE.md` is updated with the
  covered file list and exclusions.
- Agents must not write authoritative ledger rows or evidence blobs directly;
  `record-completion.py` owns those.
- If a packet repeatedly fails without target-file edits, stop and regenerate
  packet scope from the latest survey evidence instead of retrying blindly.
