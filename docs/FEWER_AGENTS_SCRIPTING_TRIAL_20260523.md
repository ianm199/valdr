# Fewer-Agents Scripting Trial - 2026-05-23

Status: trial plan for adapting the nginx fewer-agents throughput experiment to
the Redis/Valkey TCL conformance frontier.

## Why This Trial Exists

The nginx trial in
`../subagentHarnessTests/nginx-fewer-agents-throughput-trial.md` tests whether
fewer, broader roles can move compatibility faster than a planner ->
translator -> compiler-fixer -> test-fixer -> verifier chain. The useful part
for Redis is not the exact nginx runtime target. It is the artifact contract:

```text
Planner  -> WAVE BRIEF
Builder  -> BUILD REPORT
Auditor  -> AUDIT REPORT
```

Redis already has strong harness gates and current TCL evidence. The experiment
is therefore a wrapper around existing packets, not a replacement for the
packet graph.

## Current Frontier

After `8975381 sort: close TCL final-five frontier`:

- `unit/sort` is green on the focused runner: 54/54.
- `unit/scripting` remains the highest-leverage red surface.
- `unit/info` is still a no-summary watch item, but it is not blocking the next
  mechanical lane.

The active v3 graph is still:

```text
tcl-post-sort-final-survey-v3
tcl-scripting-conversion-select-v3
tcl-post-scripting-conversion-survey-v3
tcl-scripting-wait-waitaof-v3
tcl-post-scripting-wait-survey-v3
tcl-core-expanded-survey-v2
```

## Trial Hypothesis

A Builder that owns a bounded scripting slice end-to-end can reduce handoff
overhead without weakening evidence if:

- Planner output names source anchors and Rust seams before code changes.
- Builder changes only the declared packet surface plus explicit collateral.
- Auditor checks evidence, scope, and anti-gaming invariants.
- The harness runner remains authoritative.

Primary metric:

```text
promoted TCL scripting tests per wall-clock hour
```

Secondary metrics:

```text
model invocations per promoted test
runner cycles per promoted test
manual rescue count
scope drift count
regression count
```

## Strategy Labels

### Bronze: `mechanical-bridge`

Target packet:

```text
tcl-scripting-conversion-select-v3
```

Goal:

- Redis RESP arrays returned by `redis.call` convert to Lua tables with
  upstream-compatible shape.
- `SELECT` inside Lua does not leak the selected DB back to the caller.

Upstream anchors:

- `reference/valkey/src/eval.c`
- `reference/valkey/src/script.c`
- `reference/valkey/src/db.c`
- `reference/valkey/tests/unit/scripting.tcl:193-244`

Rust anchors:

- `crates/redis-commands/src/eval.rs`
- `crates/redis-commands/src/connection.rs`
- `crates/redis-commands/src/dispatch.rs`
- `crates/redis-core/src/command_context.rs`

Why mechanical helps:

The upstream behavior is a lifecycle bridge: enter script context, run an inner
command, convert the inner reply to Lua, and restore caller state. Translating
the lifecycle order is more important than inventing a new high-level shape.

Gate:

```bash
python3 harness/oracle/tcl-survey.py --skip-build --timeout-s 150 --files unit/scripting
```

### Silver: `script-safe-blocking`

Target packet:

```text
tcl-scripting-wait-waitaof-v3
```

Goal:

- `WAIT` and `WAITAOF` inside scripts/functions return through the normal
  dispatch path without blocking the script.
- In the current single-node non-replication envelope, returning zero is
  acceptable only if command semantics remain explicit and local.

Upstream anchors:

- `reference/valkey/src/replication.c`
- `reference/valkey/src/eval.c`
- `reference/valkey/tests/unit/scripting.tcl:292-300`

Rust anchors:

- `crates/redis-commands/src/replication.rs`
- `crates/redis-commands/src/dispatch.rs`
- `crates/redis-commands/src/eval.rs`

Gate:

```bash
python3 harness/oracle/tcl-survey.py --skip-build --timeout-s 150 --files unit/scripting
```

### Gold: `scripting-hardening`

Deferred until Bronze and Silver land.

Candidate scope:

- `EVAL_RO` / read-only command enforcement.
- `SCRIPT SHOW`, `SCRIPT KILL`, tracebacks, richer function metadata.
- Full `unit/functions` follow-up.

Gold should be cut from fresh runner evidence, not guessed upfront.

## Role Contracts For This Trial

### Planner

Planner is read-only. Output must include:

```text
WAVE BRIEF
- goal
- upstream source anchors
- Rust seams and current helper APIs
- strategy label
- implementation scope
- allowed collateral
- non-goals
- required gates
- risks
```

### Builder

Builder owns one bounded packet. Output must include:

```text
BUILD REPORT
- packet id
- strategy label
- source behavior matched
- files changed
- commands run
- evidence paths
- remaining misses or blocker
```

Builder must not write `harness/evidence/ledger.jsonl` or the driver-reserved
evidence blob. Builder must not bypass dispatch, ACL, transactions, scripting,
expiration, pub/sub, blocking wakeups, AOF, RDB, or replication to make a local
test pass.

### Auditor

Auditor is read-only. Output must include:

```text
AUDIT REPORT
- evidence checked
- runner artifacts checked
- target drift: yes/no
- normalizer or reference edits: yes/no
- anti-gaming invariant status
- result: pass | fail | blocked
```

## Success Criteria

Adopt this lane for future Redis work only if it improves throughput without
increasing rescue or regression count:

```text
Bronze success:
  - tcl-scripting-conversion-select-v3 completed
  - paired runner completed
  - no broad smoke regression
  - no undeclared collateral

Silver success:
  - tcl-scripting-wait-waitaof-v3 completed
  - paired runner completed
  - scripting no longer aborts at WAITAOF
  - no normalizer/reference weakening
```

## First Execution Shape

Use a Spark scout/worker for the Builder only after this brief exists.
Keep final runner execution, ledger acceptance, and commits under the main
supervisor.

```text
1. Run tcl-post-sort-final-survey-v3 on main.
2. Dispatch one Spark Builder for tcl-scripting-conversion-select-v3.
3. Main supervisor reviews diff and runs cargo + focused TCL.
4. Dispatch/read-only Auditor if the Builder claims green.
5. Commit only after objective evidence lands.
```
