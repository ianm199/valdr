# Overnight lane: replication observability (P1+P2+P3)

**Branch:** `lane/repl-observability` (in-place, reuses existing `target/` — disk is ~90% full, do NOT create worktrees).
**Runtime:** single sequential unattended `claude -p` lane. Launched 2026-06-01.
**Mandate:** move the dual-server `integration/replication*.tcl` oracle from its
current partial/hanging state toward green, in **safe Rust only**, by fixing the
observability and discrete-command gaps that currently hide whole files. The
fork-gated feature tail is explicitly OUT of scope (see below).

## The bar (read this first — it overrides any build-success instinct)

The dual-server oracle is the ONLY truth-teller. `cargo build` green is not
signal. A clean build with a regressed oracle file is a regression. Never trust
a self-reported pass count — re-read the run JSON / stdout `Test Summary` line.

**Oracle runs MUST be sequential (`--clients 1`). NEVER run two suites
concurrently and NEVER use `-j4`** — concurrent dual-server suites fabricate
mass false regressions (see the `oracle-suite-contention` lesson). One file at a
time.

## Honest baseline (latest `integration-repl` runs, 2026-05-27/29)

| File | Baseline | What's wrong |
|---|---|---|
| `replication-2` | **7/0 ✓ GREEN** | no-regression tripwire — must stay green |
| `block-repl` | **2/0 ✓ GREEN** | no-regression tripwire — must stay green |
| `replication-3` | 4/3 | replica expired-key delete; spop→srem cmdstat propagation |
| `replication-4` | aborts | `ERR Unknown DEBUG subcommand: replicate` kills the file mid-run (+2 real fails) |
| `replication-buffer` | **aborts** | `[$replica role]` never reports `sync` → "fail to sync with replicas" → whole file dies in setup |
| `replication` | **TIMES OUT 150s** | "Replica does not enter handshake state" / "...wait_bgsave state" — intermediate link states never reported; test polls forever |
| `replication-psync` | **TIMES OUT 120s** | `sync_partial_ok`/`sync_partial_err` stay 0 — counters not wired; assertions never satisfied |
| `replication-aof-sync` | 1/5 | RDB-reuse-as-AOF-base + diskless — **fork-gated, OUT OF SCOPE** |
| `replica-redirect` | aborts | `ERR unknown command 'failover'` — FAILOVER unimplemented |
| `dual-channel`, `cross-version` | — | big feature / cross-version — **OUT OF SCOPE** |

Confirmed in code: `master_link_status`/`master_sync_in_progress` ARE emitted
(`redis-commands/src/info.rs:531/536`); `sync_partial_ok/err/full` are NOT;
FAILOVER is absent from `dispatch.rs`; `DEBUG REPLICATE` is unimplemented.

## The leverage insight

Two whole files (`replication`, `replication-buffer`) and most of a third
(`replication-psync`) contribute ~zero counted tests because a single missing
INFO/ROLE state transition makes the harness's `wait_for_sync` /
`wait_for_condition` loops hang or abort *before any assertion runs*. Reporting
the state un-hides dozens of sub-tests at once. This is pure observability in
safe Rust — not new replication mechanics.

## Packets (sequential — they share `replication.rs` + `info.rs`, do NOT parallelize)

### P1 — Replica link-state observability (highest leverage, do first)
Report the transient link states the harness polls: `handshake`, `wait_bgsave`,
`sync`, `connected`, in INFO replication and the `ROLE` reply's replica state
field. The replica state machine likely jumps terminal-to-terminal today;
expose the intermediate states the C server publishes during a sync.
- **Owns:** `crates/redis-core/src/replication.rs`, `crates/redis-commands/src/info.rs`, the ROLE handler (`grep b"ROLE"` in `dispatch.rs`).
- **Targets:** un-hangs `replication.tcl`, un-aborts `replication-buffer.tcl`.
- **Gate:** `--files integration/replication,integration/replication-buffer` (sequential, separate invocations or `--files` list run one-at-a-time).

### P2 — Partial-resync counters + path (after P1 — shares info.rs/replication.rs)
Wire `sync_partial_ok` / `sync_partial_err` / `sync_full` increments on the
`+CONTINUE` / `+FULLRESYNC` paths. The `+CONTINUE` path already exists; the
counters just don't fire. Ensure a reconnect within the backlog window actually
takes the partial path and bumps `sync_partial_ok`; a miss bumps
`sync_partial_err`.
- **Owns:** `replication.rs` + `info.rs` (shared with P1).
- **Targets:** the non-diskless variants of `replication-psync.tcl`.

### P3 — Discrete gaps
- `DEBUG REPLICATE` subcommand → un-aborts `replication-4`.
- `FAILOVER` command (at minimum a faithful stub that the redirect test accepts) → un-aborts `replica-redirect`.
- The two real `replication-3` fails: replica expired-key delete semantics, spop→srem propagation cmdstat.
- **Owns:** `dispatch.rs` + the relevant command files; `expire.rs` for the expired-key path.

## OUT OF SCOPE (do not attempt — leave for a human product decision)
- Diskless sync, dual-channel replication, RDB-reuse-as-AOF-base
  (`replication-aof-sync` failures) — all bottom out on real `fork()`-based
  bgsave, which conflicts with the zero-unsafe budget and is an explicit
  architecture decision for the user.
- `cross-version-replication` (needs a different-version reference binary).

## Iteration discipline (climb the cheapest rung first)
- **Rung 1:** `cargo check -p redis-core -p redis-commands`.
- **Rung 2 (inner loop):** extend `crates/redis-commands/tests/repl_correctness_kit.rs`
  with deterministic in-memory cases for the link-state transitions and
  partial-resync counters where feasible. Build the kit case BEFORE grinding the
  30–150s oracle repeatedly.
- **Rung 4 (gate):** single-file oracle, sequential:
  ```bash
  python3 harness/oracle/tcl-survey.py \
    --runner-id repl-obs --profile integration-repl \
    --timeout-s 240 --baseport 47000 --portcount 4000 --clients 1 \
    --files integration/<file> --isolated-tests-copy
  ```
  (Omit `--skip-build` on the first run of each packet so it builds your changes.)
- **End of lane:** one sequential sweep over all in-scope files; confirm
  `replication-2` and `block-repl` are still GREEN (the no-regression tripwire).

## Safety
- `commit-on-stop.sh` auto-commits — nothing is lost between Stop events.
- Commit per packet with the oracle delta in the message.
- If a change regresses a currently-green file, REVERT it — green stays green.
- Record the final per-file before/after table at the bottom of this doc.

## Results log (append as you go)
_(to be filled in by the lane)_
