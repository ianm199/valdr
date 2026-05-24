# Loop Observations 2026-05-23

Purpose: literal notes on how the port harness loop is moving Redis/Valkey
conformance forward during the persistence-focused run.

## Current Run

- Selector: `nightly`
- Runtime: Codex through `port-harness/loop/run-loop.py --auto-dispatch`
- Focus: persistence conformance signal, especially AOF/RDB restart and replay
  behavior.

## What Worked

- Runner preflight produced a useful frontier instead of vague "persistence is
  bad" signal: 10/12 persistence scenarios passed, with two named failures.
- Four runner packets completed inline before agent dispatch:
  - `persistence-frontier-baseline-v1`: 10/12
  - `persistence-current-rdb-cycle-v1`: pass
  - `persistence-current-aof-cycle-v1`: pass
  - `persistence-current-aof-rewrite-cycle-v1`: pass
- The next implementation packet selected by the scheduler was appropriate:
  `persistence-aof-propagation-v1`, targeting the GETEX/AOF propagation failure.
- Codex reproduced the focused GETEX failure with the same runner command before
  editing.
- Codex localized the issue to command dirty/AOF propagation, not to the file
  writer itself.
- Codex ran the packet's focused gate after editing:
  `getex-does-not-append-to-aof`, `aof-spop-count-replay`,
  `aof-lmpop-zmpop-replay` all passed.
- The post-fix frontier runner completed inline and improved the persistence
  frontier from 10/12 to 11/12.

## Friction / Bugs

- `test-fixer` prompt generation only understood `oracle`/`rust-vs-reference`
  rows. It failed on typed JSON runner evidence. Fixed in global harness
  `pick-next.py` by allowing numerator/denominator failure rows from runners.
- The first generalized typed-evidence prompt included broad
  `official-tcl-coverage` timeout/no-summary noise. Fixed by preferring specific
  capability failures over broad telemetry capabilities like
  `official-tcl-coverage`.
- `cargo fmt -p redis-commands` caused broad formatting churn outside packet
  scope. Codex noticed and attempted to trim it, but this needs inspection before
  merge.
- Auto-commit is still expected to skip because the tree contains broad existing
  dirty state and several packet changes outside declared targets.
- `record-completion.py` rejected the successful GETEX fix because
  `no_out_of_scope_changes` compared against the whole pre-existing dirty
  worktree. That is the wrong baseline for a dirty multi-packet run: the packet
  should be judged against changes introduced since its own dispatch attempt,
  while still recording that the tree was dirty.
- Because record-completion failed, `run-loop.py` immediately selected the same
  packet for retry. This would have wasted a second Codex turn on an
  already-fixed implementation. We stopped that retry manually.
- The generic test-fixer completion profile forced the old diff-smoke oracle
  (`harness/oracle/smoke.sh --skip-build`). That gate hung and was not the
  right proof for this packet. The packet's real proof is the persistence
  frontier JSON command. We recorded completion with `--oracle-mode
  reference-only` after confirming the transcript contained the focused
  persistence-frontier pass.
- The global harness now supports packet-level `oracle_mode`, and the current
  persistence test-fixer packets are marked `reference-only`. This prevents
  future JSON-runner-driven fixes from inheriting the stale wire-diff gate.

## Efficiency Notes

- Time from loop start to useful runner signal: good. The runner-only pass took
  seconds and yielded a concrete 10/12 frontier.
- Time from clean Codex prompt to focused gate green: roughly minutes, acceptable
  for a first persistence test-fixer packet.
- Human intervention was still required twice:
  - once to fix the test-fixer/typed-runner evidence abstraction;
  - once to stop and regenerate an over-broad prompt.
- The loop is moving the project forward, but it is not yet unattended-optimal.
  Prompt construction quality is still a first-order throughput limiter.

## Design Takeaways

- Packet capabilities need to distinguish broad scoreboard telemetry from
  actionable fault ownership. A fixer packet should not inherit every broad
  failing dashboard row.
- Runner results are first-class oracle evidence. The harness should treat JSON
  command runners, official-suite runners, fuzzers, and differential oracles
  uniformly when selecting fixer prompts.
- The loop should enforce a narrow formatting policy. Project-wide `cargo fmt`
  from a subagent can create unrelated dirty state that obscures the packet diff.
- Runner preflights are valuable before overnight work because they convert weak
  test signal into named, scoped red cases.
- Agent completion needs packet-specific gate selection. `test-fixer` should not
  imply "run the wire diff oracle" when the packet is driven by a JSON runner,
  TCL runner, fuzz runner, or benchmark runner.
- Architect packets that create runner-driven fixers should set `oracle_mode`
  explicitly on the generated packet.

## Open Checks Before Calling This Run Good

- Codex dispatch exited cleanly for the first GETEX attempt.
- `record-completion.py` accepted and ledgered `persistence-aof-propagation-v1`
  after the completion baseline fix and explicit `reference-only` oracle mode.
- Inspect and remove any unrelated formatting churn from files not in the packet.
- Full `persistence-frontier` runner after the GETEX fix improved to 11/12.
- Confirm the next selected nightly packet is the AOF manifest architecture item,
  not a stale/manual fallback.

## Later Events

### AOF manifest architecture packet

- Packet: `persistence-aof-manifest-architecture-v1`
- Role/runtime: `architect` via Codex
- Result: completed and ledgered.
- Evidence blob:
  `harness/evidence/runs/20260523T195353Z-a93a2ad-architect-persistence-aof-manifest-architecture-v1.json`
- Useful output:
  - Added a manifest-lite wave instead of one huge AOF rewrite.
  - Added implementation/fixer packets for manifest load, current INCR layout,
    rewrite finalization, and post-step frontier runners.
  - Extended `docs/PERSISTENCE_BORING_SPEC_20260523.md` with source-shaped
    AOF manifest contracts.

Loop assessment:

- This was a good use of the architect role. It read the latest frontier row,
  avoided broad implementation, and converted one red scenario into a small
  graph of runner/fixer pairs.
- The role still needed better prompt rendering: source ranges were partially
  degraded in the prompt, so the agent had to recover by reading
  `harness/work-packets.jsonl` directly.

### AOF manifest frontier scenario packet

- Packet: `persistence-aof-manifest-frontier-scenarios-v1`
- Role/runtime: `translator` via Codex
- Result: completed and ledgered.
- Evidence blob:
  `harness/evidence/runs/20260523T201037Z-a93a2ad-translator-persistence-aof-manifest-frontier-scenarios-v1.json`
- Direct runner output before ledgering:
  `harness/oracle/results/persistence-frontier/20260523T200914Z/result.json`
- Frontier after scenario expansion:
  - `12/23` passing overall.
  - Old persistence rows stayed green.
  - Manifest rows are red except `multipart-aof-empty-dir-startup`.

What the packet added:

- Expanded `harness/oracle/persistence-frontier.py` from one manifest row to a
  real manifest frontier:
  - basic manifest load
  - missing referenced file fails
  - non-monotonic INCR fails
  - blank-line manifest fails
  - empty manifest fails
  - duplicate BASE fails
  - unknown manifest type fails
  - empty appendonly dir starts cleanly
  - discontinuous INCR load
  - empty INCR load
  - `CONFIG SET appendonly yes` creates manifest layout
  - `BGREWRITEAOF` advances BASE/INCR sequence numbers
- Documented the scenario purpose in
  `docs/PERSISTENCE_BORING_SPEC_20260523.md`.

Loop assessment:

- The work moved conformance forward in the important harness sense: it did not
  make the implementation pass more rows, but it turned one vague red manifest
  issue into eleven specific, source-shaped red rows. That gives the next
  fixer a much better target.
- The role choice was wrong. This is oracle-authoring, not C-to-Rust
  translation. The translator prompt made the agent read irrelevant Rust module
  surfaces and PORT STATUS rules before it got to the actual job. We need a
  first-class `oracle-writer` or `runner-author` role.
- The source-range renderer is buggy for multi-file source ranges. The prompt
  showed `server.c` plus bare numeric ranges; the agent had to inspect the
  packet row to discover the actual upstream Tcl/support files. Fixing this is
  a harness-level throughput improvement.
- Fixed immediately after the packet: global `port-harness/loop/pick-next.py`
  now renders full `source_ranges` strings instead of stripping everything
  before the first colon.
- The agent found and fixed a fixture bug in its own first scenario expansion:
  AOF files were written before creating `appendonlydir`. It adjusted the helper
  to create parent directories, which is exactly the kind of self-correction we
  want from oracle-authoring agents.

### Current queue position

- The run stopped cleanly at `max-iterations=8`.
- Next selected nightly item is:
  `persistence-aof-manifest-frontier-baseline-v1`.
- That runner should ledger the expanded `12/23` frontier, then select the first
  real manifest implementation fixer: `persistence-aof-manifest-load-v1`.

### Manifest load fixer packet

- Packet: `persistence-aof-manifest-load-v1`
- Role/runtime: `test-fixer` via Codex
- Baseline runner immediately before dispatch:
  `persistence-aof-manifest-frontier-baseline-v1`, `12/23` passing.
- The prompt was materially better after the source-range renderer fix: it
  included full upstream anchors:
  - `reference/valkey/src/aof.c:57-420`
  - `reference/valkey/src/aof.c:1522-1918`
  - `reference/valkey/src/server.c:7248-7260`
  - `reference/valkey/tests/integration/aof-multi-part.tcl:1-430`

Implementation signal:

- The agent found the correct first divergence: startup only loaded the legacy
  `<dir>/<appendfilename>` AOF path and ignored
  `<dir>/<appenddirname>/<appendfilename>.manifest`.
- It added a private manifest parser/loader under
  `crates/redis-commands/src/aof.rs` and wired startup through target files.
- It built successfully and produced a frontier run at
  `harness/oracle/results/persistence-frontier/20260523T201845Z/result.json`.
- That run improved the frontier from `12/23` to `21/23`.
- Remaining red rows after this implementation:
  - `multipart-aof-appendonly-enable-layout`
  - `multipart-aof-rewrite-sequence-advance`

Loop/process issue:

- The agent ran `cargo fmt` and reformatted a broad set of files outside the
  packet target. This is a serious loop hygiene problem in a dirty worktree.
- I interrupted the loop before automated cleanup could accidentally revert
  unrelated user/prior-agent changes.
- Cleanup performed manually:
  - Restored only 60 Rust files that were clean before this packet and became
    dirty solely from formatter churn.
  - Preserved every file that was already dirty before the packet.
  - Preserved the new baseline evidence blob.
- Validation after cleanup:
  - `cargo check --workspace` passed.
  - `cargo build -p redis-server` passed.
  - `python3 harness/oracle/persistence-frontier.py --skip-build` produced
    `harness/oracle/results/persistence-frontier/20260523T202137Z/result.json`
    with `21/23` passing.
- Ledger status:
  - Initial `record-completion.py` correctly rejected the interrupted transcript
    under the old mtime-based dirty heuristic.
  - I changed global `record-completion.py` to ignore paths already listed in
    `next-packet.json`'s `git_status_short` by path, not by mtime. This is an
    explicit dirty-worktree compromise: it prevents false failures from
    pre-existing dirty files touched by formatters, while the proper long-term
    fix is content snapshots at dispatch.
  - After that harness fix, `record-completion.py` accepted and ledgered
    `persistence-aof-manifest-load-v1`.
- Design takeaway: completion gates should fail fast on project-wide formatter
  churn, and packet prompts should explicitly prefer `cargo fmt --check` or
  file-scoped formatting over workspace `cargo fmt`.
- Fixed immediately after the incident: global
  `port-harness/loop/prompt-template-test-fixer.md` now bans
  workspace-wide formatters during dirty packet runs.

Packet-quality issue:

- The packet's focused gate text referenced
  `multipart-aof-manifest-invalid-format-fails`, but the actual oracle split
  that concept into specific rows such as blank-line, empty-file, duplicate-base,
  and unknown-type failures. The agent noticed the mismatch and proceeded, but
  packet generation needs to derive focused gates from registered scenario ids,
  not from stale prose.
- Fixed two follow-up packet notes immediately after this check:
  - `multipart-aof-manifest-enable-creates-layout` ->
    `multipart-aof-appendonly-enable-layout`
  - `multipart-aof-manifest-rewrite-finalization` ->
    `multipart-aof-rewrite-sequence-advance`

### Current-INCR manifest layout packet in flight

- Active loop command:
  `python3 ../port-harness/loop/run-loop.py --project . --selector nightly --reset --auto-dispatch --dispatch-runtime codex --dispatch-sandbox danger-full-access --dispatch-approval never --dispatch-timeout-s 2400 --max-iterations 8 --max-failures 3`
- Active packet:
  `persistence-aof-manifest-current-incr-v1`
- Prompt/evidence shape is good:
  - latest evidence blob is
    `harness/evidence/runs/20260523T202439Z-a93a2ad-runner-persistence-post-manifest-load-frontier-v1.json`
  - the prompt names the two failing rows:
    `multipart-aof-appendonly-enable-layout` and
    `multipart-aof-rewrite-sequence-advance`
  - packet instruction explicitly scopes this packet to current-INCR writer
    layout and says not to implement BGREWRITEAOF manifest finalization here
  - hard rule now bans workspace-wide `cargo fmt`
- Transcript signal so far:
  - agent reproduced the appendonly-enable failure artifact
  - agent read the corresponding Valkey AOF manifest/naming functions:
    `getNewIncrAofName`, `getLastIncrAofName`, and `persistAofManifest`
  - agent is inspecting Rust `aof.rs`, `connection.rs`, `main.rs`, and
    `live_config.rs`
- Loop observation:
  - this is the best-shaped packet in the manifest wave so far: tight failing
    rows, source anchors, focused gates, and a clear "do not cross into rewrite"
    line
  - still no strong evidence yet that the packet has begun editing; continue
    polling transcript before judging efficiency

Current-INCR result:

- `persistence-aof-manifest-current-incr-v1` completed and ledgered.
- Gates:
  - `cargo check -p redis-commands -p redis-server` passed
  - `cargo build -p redis-server` passed
  - focused `multipart-aof-appendonly-enable-layout` passed
  - `python3 harness/oracle/persistence-cycle.py --mode aof` passed
  - focused pair `multipart-aof-empty-dir-startup,multipart-aof-appendonly-enable-layout`
    passed
  - extra replay check `aof-debug-loadaof-complex-dataset,multipart-aof-manifest-basic-load`
    passed
- The packet did not run workspace `cargo fmt`; the new prompt rule worked.
- The following runner still reported `21/23`, not `22/23`, because:
  - `multipart-aof-appendonly-enable-layout` is now green
  - `multipart-aof-rewrite-sequence-advance` remains red as expected
  - `getex-does-not-append-to-aof` is red again, but this appears to be an
    oracle expectation issue after manifest layout: the scenario still looks
    for root `appendonly.aof`, while the writer now correctly uses
    `appenddirname/appendfilename.N.incr.aof` with a manifest
- Loop observation:
  - the loop's packet-to-failure filtering selected the correct next manifest
    packet, `persistence-aof-manifest-rewrite-finalize-v1`
  - however, the frontier runner can still show stale red rows from old
    filesystem assumptions; after rewrite finalization, we need an oracle-fixer
    packet for `getex-does-not-append-to-aof` under manifest layout

### Rewrite-finalize packet and final persistence frontier

- `persistence-aof-manifest-rewrite-finalize-v1` completed and ledgered.
- Implementation signal:
  - the agent read Valkey `backgroundRewriteDoneHandler` and the
    `aof-multi-part.tcl` sequence-number cases
  - it replaced the old "rewrite over active writer" path with a synchronous
    manifest finalizer that:
    1. opens a fresh current INCR writer first
    2. writes the BASE in the configured preamble format
    3. renames the BASE into `appenddirname`
    4. persists a manifest containing the new BASE and current INCR
  - it preserved the important safety property for this v1: no child/thread
    renames over the active writer
- Gates:
  - `cargo build -p redis-server` passed
  - focused `multipart-aof-rewrite-sequence-advance` passed
  - `python3 harness/oracle/persistence-cycle.py --mode aof-rewrite` passed
  - focused `aof-rewrite-collections-digest,multipart-aof-rewrite-sequence-advance`
    passed
- The loop then ran:
  - `persistence-post-manifest-rewrite-cycle-v1`: pass
  - `persistence-post-manifest-rewrite-frontier-v1`: `22/23`
  - `persistence-focused-tcl-v1`: pass status, but weak signal:
    `4 files, 0 passed tests, 0 failed tests, 0 timed out, 4 without summary`

Oracle repair:

- The remaining `22/23` red row was `getex-does-not-append-to-aof`.
- Investigation showed this was not a command regression. The scenario still
  inspected root `appendonly.aof`, but after the manifest wave the active file
  is the manifest's current INCR under `appendonlydir`.
- I updated `harness/oracle/persistence-frontier.py` to parse the current INCR
  from `appendonlydir/appendonly.aof.manifest`, falling back to legacy
  `appendonly.aof` only if that exists.
- Verification:
  - focused `getex-does-not-append-to-aof` passed with path
    `appendonlydir/appendonly.aof.1.incr.aof`
  - full direct frontier run:
    `harness/oracle/results/persistence-frontier/20260523T204142Z/result.json`
    reports `23/23` scenarios passing

Loop observation:

- The implementation loop worked well for the manifest wave: the sequence
  architecture -> oracle expansion -> baseline -> fixers -> refresh runners
  drove the feature from `12/23` to green.
- The loop still needs a first-class "oracle-fixer" role. The final green came
  from a manual oracle correction because the queue had no ready packet left.
- The focused TCL persistence runner is not yet a useful completion signal: a
  pass with `0 passed / 0 failed / 4 without summary` should probably be a
  warning or fail until the parser can extract real upstream counts.
- The latest automated ledger row says `22/23`; the latest direct result says
  `23/23`. Before publishing this as a dashboard claim, add a ledgered
  post-oracle-fix runner packet or teach the dashboard to surface direct
  frontier result artifacts explicitly.

Ledger closeout:

- Added `persistence-post-oracle-fix-frontier-v1` as a small runner packet.
- Ran it through the chassis with `--selector nightly --reset --max-iterations 1`.
- Ledgered evidence:
  `harness/evidence/runs/20260523T204256Z-a93a2ad-runner-persistence-post-oracle-fix-frontier-v1.json`
- Completion now reports:
  - `persistence-overnight-frontier-final`: pass,
    `persistence frontier: 23/23 scenarios passing`
  - `persistence-aof-manifest-rewrite-cycle-final`: pass,
    `persistence aof-rewrite cycle: pass`
  - `persistence-focused-tcl-final`: warning; this is currently a runner-id /
    weak-summary issue, not a persistence-frontier failure
- Verification after the loop:
  - `cargo check --workspace` passed with existing warnings
  - latest ledgered persistence frontier is `23/23`
