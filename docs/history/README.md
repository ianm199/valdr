# history/

Planning, triage, and retrospective documents from the port's
development. Preserved here for narrative context — none of this is
load-bearing reference material.

For *current* user-facing docs, see the top-level [`docs/`](../) folder:

- [`docs/TEST_AND_FEATURE_COVERAGE.md`](../TEST_AND_FEATURE_COVERAGE.md) —
  current coverage source of truth (commands, oracles, divergences)
- [`docs/DOCKER.md`](../DOCKER.md) — container deployment
- [`docs/ADR_001_LUA_RUNTIME.md`](../ADR_001_LUA_RUNTIME.md) — Lua runtime
  decision record
- [`docs/VALKEY_SYSTEM_DEEP_DIVE.md`](../VALKEY_SYSTEM_DEEP_DIVE.md) —
  codebase tour for contributors

## What's in here

| File | What it captured |
|---|---|
| `PATH_TO_RUNNABLE.md` | Pre-port execution plan: how we'd get a runnable `redis-server` listening on TCP. Shipped. |
| `PATH_TO_DEF3.md` | Plan for "Definition 3" — prod-safe single-node cache (persistence, auth, eviction). Shipped. |
| `RDB_PLAN.md` | 10-round implementation plan for RDB v11 persistence + bidirectional oracle. Shipped (378/378 PASS). |
| `DOCKER_PLAN.md` | Original Docker packaging spec. Superseded by `docs/DOCKER.md`. |
| `TCL_ORACLE_PLAN.md` | Plan for adopting the upstream Valkey TCL test suite as our primary conformance oracle. Adopted. |
| `TCL_TRIAGE.md` | First-run triage of `unit/type/string` against our binary (Round 9). |
| `TCL_TRIAGE_DATATYPES.md` | First-run triage of `unit/type/{hash,set,zset}` (Round 10b). |
| `TCL_TRIAGE_KEYOPS.md` | First-run triage of `unit/expire`, `unit/incr`, `unit/keyspace`, and friends (Round 10c). |
| `TCL_DASHBOARD.md` | Mid-port TCL pass-rate snapshot. Numbers here are stale; see `docs/TEST_AND_FEATURE_COVERAGE.md` for current. |
| `HARNESS_LEARNINGS.md` | Retrospective on running the AI porting harness at scale on Valkey for the first time. |
| `HARNESS_MODE_A_VS_B.md` | Strategy doc distinguishing translation tasks (Mode A) from greenfield Rust subsystem tasks (Mode B). |
