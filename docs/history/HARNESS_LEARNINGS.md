# Harness pattern learnings — 2026-05-16/17 redis-rs-port sessions

Findings from running the AI-driven C-to-Rust port harness on Valkey for the
first time at scale (65 files translated, ~50k Rust LoC, ~$77 cost).
Companion to `RETROSPECTIVE_AND_PRODUCTIZATION.md` in the lua-rs-port
sibling repo.

## Session shape

- **2026-05-16 evening**: chassis extraction (port-harness), redis-rs-port
  scaffolding, first translator packet (resp_parser.c → parser.rs),
  unattended overnight loop produced 36 files / $36 cost.
- **2026-05-17**: parallel sub-agent waves (compile-fix → re-merge →
  cleanup → translator wave on redis-ds files), reaching 65 done /
  workspace compiles clean.

## What worked beyond expectations

### Parallel sub-agents are a real multiplier for INDEPENDENT tasks
4 parallel Wave-1 sub-agents (compile-fix, merge-planner, defer-curator,
quarantine-triage) did ~$15 / ~15 min of work that would have been 60+
min sequentially. Same shape repeated through the session.

The discipline that made this work: each agent's prompt was
*self-contained* (held all context, didn't reference my conversation)
and its scope was *disjoint* from sibling agents (different file sets,
different goals).

### Opus on architect-style tasks earns its premium
The merge-executor sub-agent (Opus, $25 budget) re-merged 2000 LoC of
quarantined full ports across 13 caller files with the shim-layer
approach the merge-planner designed. Cargo check at 0 errors, 8/8 db
tests pass. This is the kind of work where Sonnet would have either
asked for clarification or made the wrong shape decision.

### Cost-per-line is remarkably stable
$0.0019-$0.005 per Rust output line, regardless of file complexity.
- resp_parser.c (sophisticated, agent restructured architecture): $0.92 / 513 LoC = $0.0018/line
- adlist-class mechanical files: similar rate
Difficulty manifests in *success rate*, not cost-per-output-line. Hard
files fail and produce no output → no cost. **Total spend formula:**
`(target LoC) × (line cost) × (1 / success rate)`.

### Phase A is genuinely cheap
65 files translated, ~50k Rust LoC, **~$77 total cost over ~24 hours**.
The agent is rarely the bottleneck — harness design is.

### The chassis safety net catches real bugs
- Type-vocabulary hook in enforce mode prevented duplicate `Client`
  definitions when the salvaged multi.rs introduced them locally.
- Unsafe-budget hook (post-fix) prevented agents from sneaking in raw
  pointers.
- commit-on-stop's gating-hook re-run blocked the bad multi.rs commit
  even though syntax checks passed.

## Bugs / friction encountered

### The stub-then-full-port API-drift cycle
Hit three times this session. Architect defines a 50-line stub of
`RedisObject`. Translator does a faithful 1180-line port that changes
`enum` → `struct + ObjectKind`. **36 callers break.**

**Mitigation that emerged:** the merge-planner produces a back-compat
shim layer (`Flat<'_>` views, predicates, `From` impls). New shape
exposes old API. Callers migrate at their own pace.

**Open chassis gap:** no automatic detection of shape-changing edits to
vocabulary types. A v2 hook should require a `// SHIM:` comment block
when a registered type's shape changes.

### Hook regex bug silently killed 20 translations
`unsafe-budget.sh` used `grep -rhn` (line numbers) + `grep -vE '^\s*//'`
(comment filter). The `-n` flag prefixed line numbers, so the comment
filter never matched, so commented-out C reference (`// unsafe { ... }`)
counted as real unsafe. Parallel fanout produced 20 valid translations
that all got hook-blocked.

Fixed in chassis commit `144e130`. **The diff-test suite didn't catch
it** because the fixture didn't include the realistic mix of real code
+ adjacent C-as-comments that translators actually produce.

**Implication:** chassis smoke tests need fixtures mirroring translator
output patterns, not just simple unit cases.

### Parallel commit-on-stop is structurally wrong
When 4 fanout workers commit simultaneously, each runs the gating-hook
chain. The chain enumerates ALL modified files in the worktree (across
workers!) and runs `unsafe-budget` per file. The hook scans the whole
crate dir. So worker A's commit fails because worker B's in-flight file
just landed something in the crate.

Per-worker `CLAUDE_TARGET_RS_FILE` scoping is honored by
forbidden-pattern, trailer-required, type-vocabulary — but NOT by
commit-on-stop's re-run loop.

**Open chassis gap:** either go full Carlini (one git worktree per
worker, merge later) OR scope commit-on-stop to worker's direct file
only. v1 fix is the second; v2 ideal is the first.

### Cumulative drift slips past per-commit safety net
The translate_loop's `cargo check + revert last agent commit` works
when ONE commit causes breakage. But object.rs/db.rs full-ports landed
fine individually; the breakage manifested only AFTER more callers
committed against the new API. Single-commit revert can't unwind
cumulative drift.

**Open chassis gap:** track a "shape signature" of vocabulary types
between checks and alarm on changes. Or do multi-commit revert windows.

### Translator agents will introduce unsafe under pressure
multi.c's first attempt had 2 unsafe blocks (raw-pointer reborrow to
work around a double-borrow on CommandContext). Hook caught it,
quarantine kicked in. The right behavior was `TODO(architect): need
re-borrow split` — but the agent reached for `unsafe` instead.

This is a prompt-engineering issue: the translator agent's instructions
need to be more emphatic about TODO(architect) being preferable.

### Agent-written tests can be wrong
localtime.rs test had a wrong timestamp baked in (1705318496 = 11:34:56
UTC, but test asserted hour==12). Impl matches C correctly. The
"NEVER edit the test" rule from PORTING.md conflicts with the reality
that agent-written tests can have bugs.

**Implication for chassis:** distinguish agent-authored tests (editable)
from human/reference tests (canonical). Maybe a `# generated` marker
on agent-authored test files.

### In-flight kill loses work cheaply
Killing the loop mid-translation lost ~$2 of nearly-complete work
(t_string.c). The chassis salvage pattern worked: file stayed in
worktree as untracked, manual commit recovered it. Good failure mode.

## Patterns that emerged

### Wave-based work is the natural rhythm
1. Stub types (architect, ~$5)
2. Translator on files (parallel, $1-3/file)
3. Compile-fix (Opus, ~$5-10)
4. Cleanup planning (Opus, ~$5)
5. Targeted fixes (varies)

Repeat. Each wave commits cleanly before the next starts.

### Planner agents produce documents; executor agents consume documents
Best handoff observed:
1. Merge-planner sub-agent → `MERGE_PLAN.md`
2. Merge-executor sub-agent reads the plan and executes
3. Phase-B planner sub-agent → `PHASE_B_CLEANUP.md`
4. (Future) Phase-B executor consumes it

When two editor-style sub-agents try to touch the same files, they
conflict. When one plans and another executes, clean.

### Audit-vs-enforce is a powerful gradient on vocabulary
Ship types in `audit` mode while the architect hasn't committed to a
shape. Flip to `enforce` once the shape is real. Lets the chassis ship
incrementally without big-bang cleanups.

This session: started with 14 vocabulary types (5 enforce, 9 audit).
Ended with 23 entries, mostly enforce, after the merge added the
ObjectKind/StringEncoding/etc. family and redis-ds crate creation
flipped its 4 + added 7 new types.

### "Quarantine instead of fail" preserves agent work
Hook-blocked files go to `harness/loop/quarantine/` instead of being
deleted. Triage agent later decides salvage / retranslate / skip. This
session: 5 files quarantined; 4 salvaged with mechanical edits (1
small unsafe fix + 3 verbatim placements).

### Phase A produces a shape; Phase B makes it run
Cargo check clean ≠ functionally complete. We have 50k LoC of valid
Rust but:
- 4 `todo!()` panics
- 1378 `TODO(port)` markers
- 723 `TODO(architect)` markers
- 43 `"not yet implemented"` sentinels
- No event loop, no TCP listener, no `main.rs` body, no command-dispatch
  lookup function

**Phase A is ~6 hours / $77 of work. Phase B (one working command
end-to-end) will be 5-10x that, almost all architect time.** This is
important for honest productization framing.

## Chassis v2 priorities (actionable)

In order of impact:

### Tier 1 — fix the parallel race
- **Git worktree per worker** (Carlini-style). Each fanout worker gets
  its own clone of the project worktree. Translations happen in
  isolation. Merge to mainline on success. Eliminates commit-on-stop
  races structurally.
- Alternative: properly scope commit-on-stop's gating-hook re-run to
  `CLAUDE_TARGET_RS_FILE`'s file only, not the whole tree.

### Tier 2 — shape-drift detection
- New chassis hook: on every edit to a vocabulary type's owner file,
  compute a "shape signature" (variants, fields, public method set).
  Alarm if it changed without an accompanying `// SHIM:` comment block
  documenting the back-compat plan.
- Multi-commit revert window for cumulative drift detection.

### Tier 3 — realistic hook fixtures
- `port-harness/test/fixtures/` with translator-realistic .rs files
  (mix of real code + adjacent C-as-comments + PORT STATUS trailers +
  TODO markers). Each hook tested against these.
- Catches the class of bug that killed 20 translations (regex falsely
  matching commented content).

### Tier 4 — better translator prompts
- More emphatic TODO(architect)-over-unsafe guidance in the translator
  template. Maybe a small banned-patterns reminder in the per-file
  prompt that fanout assembles.
- Distinguish agent-authored tests from canonical tests. `# generated`
  marker; agent-authored tests are editable by test-fixer.

### Tier 5 — Phase B orchestration primitives
- Phase B is structurally different from Phase A: it's about
  *integration*, not *translation*. The chassis should have a separate
  loop type for it. Inputs: TODO inventory + dependency graph. Outputs:
  per-iteration "next-X-TODOs to fix" packets.

### Tier 6 — PR-per-file workflow
- Per retro §10.4: auto-commit erodes the rollback story. A v2 chassis
  should optionally produce a git branch + PR per agent commit.
  Auto-merge OK with policy, but the audit trail exists.

## Numerical reference (this session)

| Metric | Value |
|---|---|
| Files translated | 65 |
| Rust output LoC | ~50,000 |
| Total cost | ~$77 |
| Cost per file (avg) | $1.18 |
| Cost per output line (avg) | $0.0015 |
| Wall clock (active translation) | ~6 hours |
| Wall clock (with sub-agent waves) | ~12 hours |
| Parallel worker peak | 6 concurrent claude processes |
| Sub-agents launched | 9 |
| Workspace cargo check final | clean |
| Tests passing | 37/38 |
| Hook bugs found and fixed | 1 (chassis-side) |
| Chassis commits this session | 1 |
| Project commits this session | ~30 agent + ~10 manual |

## The biggest meta-lesson

**The chassis is the IP, and the chassis has internal feedback loops we
keep discovering.** Every misbehavior in this session was an
architectural gap in the chassis (hook scoping, shape-drift detection,
parallel commit), not an agent failure. Agents did exactly what the
prompts directed them to do. The interesting bugs were in how the
harness composed their work across files / time / workers.

If you productize this: the harness *is* the product, agents are
commodity inputs, and most engineering investment goes into the
chassis's feedback loops.
