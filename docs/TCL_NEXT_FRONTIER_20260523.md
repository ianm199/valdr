# TCL Next Frontier — 2026-05-23

Status: packet plan for the next conformance push after commit `80f7dd5`.

Scope note: this file is about a focused frontier runner. It is not the full
upstream-suite count. Full-suite accounting is tracked in
[`TCL_FULL_SUITE_GOAL_20260523.md`](TCL_FULL_SUITE_GOAL_20260523.md).

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
                      └─ tcl-scripting-cjson-v3
                           └─ tcl-post-scripting-cjson-survey-v3
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

Current status in `reference/valkey/tests/unit/scripting.tcl` after the
WAIT/WAITAOF packet:

- the script/runtime error boundary is RESP-safe again: `redis.call(...)`
  runtime errors no longer leak Lua stack-trace newlines into the outer error
  reply.
- script-safe `WAIT` and `WAITAOF` now run through normal dispatch without
  blocking in the current single-node envelope.
- the focused scripting run now reaches `EVAL - JSON numeric decoding` and
  aborts because the sandbox has no `cjson` global.

Failure classes fixed in this wave:

- `WAIT` is no longer blocked inside redis.call/pcall; script-safe
  nonblocking behavior uses existing dispatch + normal handler path.
- `WAITAOF` is now known and executed through normal dispatch with script-safe
  nonblocking replies in this single-node envelope.
- Lua runtime errors from `redis.call` are converted into one-line RESP errors
  before leaving EVAL/FUNCTION, preserving Tcl protocol framing.

Packet-scope correction: `tcl-scripting-wait-waitaof-v3` originally named only
`dispatch.rs`, `eval.rs`, and this doc as targets even though `replication.rs`
is the canonical owner for `WAIT`/`WAITAOF`. That made the packet guide the
Builder toward a wrong ownership move. The packet now explicitly includes
`replication.rs`; `eval.rs` only owns the inner-script `deny_blocking` dispatch
flag.

General packet-scope rule from this failure: packet `targets` are the primary
work surface, not the whole semantic boundary. A worker must identify the
canonical owner before editing. If the owner is outside `targets` and outside
declared collateral, the correct output is a packet-scope miss, not a fake
implementation in the wrong file.

Bronze status:

- `47d7fbf` fixed the Lua reply conversion / selected-DB restoration packet.
- This packet carried the focused scripting survey past the planned Silver
  frontier: `EVAL - Scripts do not block on wait` and the following `WAITAOF`
  dispatch point now pass under the current single-node envelope.

Latest packet target done: script-safe WAIT/WAITAOF handled.

Latest focused proof command, using a custom port range because another
long-running TCL survey held the default range:

```bash
cargo build --bin redis-server
cd reference/valkey
VALKEY_BIN_DIR=$PWD/../../target/debug \
  tclsh tests/test_helper.tcl --single unit/scripting --clients 1 \
  --skip-leaks --tags "-needs:repl -needs:debug -external:skip" \
  --quiet --baseport 31111 --portcount 8000
```

Observed next abort:

```text
EVAL - JSON numeric decoding
ERR [string "function_library"]:4: attempt to index global 'cjson' (a nil value).
```

Current next frontier after this wave:

- `unit/scripting`: embedded Redis Lua libraries, starting with a minimal
  Redis-compatible `cjson` table in the Lua sandbox.
- `unit/sort`: still the five concrete SORT failures above unless that sibling
  packet has already landed.

## Run Discipline

- Every implementation packet has a focused TCL runner gate.
- Runner evidence is telemetry until `docs/CONFORMANCE.md` is updated with the
  covered file list and exclusions.
- Agents must not write authoritative ledger rows or evidence blobs directly;
  `record-completion.py` owns those.
- If a packet repeatedly fails without target-file edits, stop and regenerate
  packet scope from the latest survey evidence instead of retrying blindly.
