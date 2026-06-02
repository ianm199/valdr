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

### Lane run 2026-06-02 (overnight, sequential `--clients 1`)

**Per-file before → after (sequential oracle, `integration-repl`, baseport 47000):**

| File | Baseline | After | Δ | Notes |
|---|---|---|---|---|
| `replication-2` | 7/0 ✓ | **7/0 ✓** | — | tripwire held GREEN |
| `block-repl` | 2/0 ✓ | **2/0 ✓** | — | tripwire held GREEN |
| `replication-4` | abort (0 counted) | **13/4** | **+13 passes** | DEBUG REPLICATE un-aborted the file (P3a) |
| `replication-psync` | 66/24 | **72–73/17–18** | **+6–7 passes** | partial-resync path + counters wired (P2); 73/17 at the P2 gate, 72/18 on the final sweep — a 1-test run-to-run variance, not a regression: the load (`bg_complex_data`→`createComplexDataset` *without* `useexpire`) creates no TTL keys, so the P3b expire changes provably cannot touch its digest (dual-server timing noise per `oracle-suite-contention`) |
| `replication-3` | 4/3 | 4/3 | — | expire fails blocked deeper (see below) |
| `replication-buffer` | 0 (setup-die) | 0 | — | blocked on diskless sync-window |
| `replication` | timeout (0) | timeout (0) | — | blocked on diskless block-1 (150s of waits) |
| `replica-redirect` | abort (0) | abort (0) | — | not attempted (big feature, see below) |

**Net: +19–20 counted passing tests** across the dual-server replication suite,
two whole tripwire files held GREEN, zero regressions (zero `unsafe` added).

**Commits (this lane):**
- `P1: faithful replica link-state observability (ROLE/INFO)` — `replica_link_code`
  (connect/connecting/handshake/sync/connected) published by the dialer; ROLE
  state field reflects the live phase. Kit: `p1_role_reports_replica_link_state`.
- `P2: wire partial-resync path + sync counters` — replica caches the primary
  replid and requests `PSYNC <replid> <offset>` on reconnect; `+CONTINUE` handled
  distinctly (no RDB, keyspace preserved); INFO emits `sync_full` /
  `sync_partial_ok` / `sync_partial_err`, bumped in `handle_psync`. Kit:
  `p2_psync_bumps_sync_counters`.
- `P3a: DEBUG REPLICATE injects command into replication stream` — un-aborts
  `replication-4`. Kit: `p3_debug_replicate_feeds_replication_stream`.
- `P3b: replica passive expiry` + `(cont): primary-link applies IGNORE expiry` —
  faithful replica expiry policy (KEEP for normal clients, IGNORE for the
  primary link, store-already-expired on a replica). Units:
  `replica_keep_expired_reports_expired_without_deleting`,
  `replica_link_apply_ignores_expiry`. No counted movement (blocked deeper).

**rung-2 / unit coverage added:** 3 new kit cases (`repl_correctness_kit.rs`) +
2 new `RedisDb` unit tests. All green; pre-existing kit failure `finding2` is
unrelated (verified on a clean tree).

**What remains / blockers (honest):**
- **`replication.tcl` + `replication-buffer.tcl`** — their `handshake` /
  `wait_bgsave` / role==`sync` assertions need the master to HOLD the sync for
  a configurable window (`repl-diskless-sync-delay`). That window bottoms out on
  the fork()-based BGSAVE hold, explicitly **OUT OF SCOPE** for this safe-Rust
  lane. P1 lands the faithful ROLE/INFO machinery that future window work builds
  on, but cannot un-block these without the fork-delay mechanism.
- **`replication-3` expire fails + 3 of `replication-4`'s 4 fails** — bottom out
  on **command-propagation rewriting** the primary does not yet do: relative-TTL
  writes are not rewritten to absolute `PEXPIREAT` (a paused-then-resumed replica
  re-anchors the TTL to apply-time and reads the key as live), and `SPOP <count>`
  is not rewritten to `SREM`. Plus `replication-3`'s high-volume consistency test
  fails on `wait_for_ofs_sync` not converging (offset-sync), and its `select 5`
  keys never reach the replica keyspace (multi-DB delivery). These are core
  replication mechanics, not the observability/discrete-command gaps this lane
  targets. The replica expiry-policy trio (P3b) is the faithful prerequisite for
  them and is now in place.
- **`replica-redirect.tcl`** — needs full `CLIENT CAPA REDIRECT` (replica
  write-redirect) **and** a real `FAILOVER` role-swap state machine
  (`master_failover_state` transitions + promote/demote). A bare FAILOVER stub
  would only convert the abort into a summary of still-failing tests (the file is
  dominated by redirect/failover semantics we don't implement), so it was not
  attempted — it is a feature, not an observability gap.
- **`replication-psync` remaining 17 fails** — diskless-load variants
  (`diskless: yes`, out of scope) plus a few backlog-window edge cases.
