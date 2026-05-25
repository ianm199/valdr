# Goal 2 ("Big Semantic Failure Burn-Down") — evidence-based re-baseline

**Date:** 2026-05-25. **Author:** Claude (Opus 4.7), propagation lane.

## Why this exists

Goal 2 asks to "reduce known-fail by ≥250 source tests" across five families,
listed with these chunk sizes:

| Family | Goal-2 listed size |
|---|---|
| unit/scripting | 186 |
| unit/functions | 112 |
| unit/introspection | 117 |
| unit/introspection-2 | 33 |
| unit/pause | 23 |
| **sum** | **471** |

Those numbers are the **original dark/uncounted source-test counts** — what was
failing *before* the suite was illuminated. They are no longer the live state.

## Current state (from the coordination board's own evidence)

| Family | Current counted | Remaining fails | Owner / commit |
|---|---|---|---|
| unit/scripting | 349/53 | 53 | Codex `5f03e39` |
| unit/functions | 83/10 | ~10 | Codex `b01947e`/later |
| unit/introspection | 53/40 | 40 | Codex `bff8c00` |
| unit/introspection-2 | 46/3 | 3 | Codex `c581e79` |
| unit/pause | 6/14 | 14 | Claude (Phase 1 merged; gate reverted) |
| **sum of remaining fails** | | **~120** | |

**Whole-suite dashboard (Codex):** 2584 pass / **226 fail** / 2810 counted.

## The conclusion

- The ≥250 reduction in these five families **already happened**, driven mostly
  by Codex: scripting/functions/introspection went from dark to 349/83/53
  passing (~485 tests now green that previously weren't). By the
  "reduction-from-original-baseline" reading, **Goal 2 is already met by the
  team.**
- By the "reduce the *current* fail count by 250" reading, it is **arithmetically
  impossible for anyone**: only ~120 fails remain across the five families, and
  only **226** in the entire suite.

## Realistic re-based targets (pick per owner)

The remaining ~120 family fails are concentrated where they're hard, not where
they're cheap:

- **scripting (53) / functions (10) / introspection (40):** Codex's active lane;
  `eval.rs` dirty in `main`. Not safely takeable by another agent without a
  deliberate split.
- **pause (14):** 12 need the command-loop postpone gate in `runtime_owner.rs`
  (Codex's file) + the deferred-client timing quirk; 2 "skip-during-pause" tests
  are correct-in-isolation but masked by intra-file cascade. See
  `[[pause-tcl-is-all-or-nothing]]` / `PAUSE_GATE_DESIGN_20260525.md`.
- **introspection-2 (3):** nearly done.

A truthful next goal is not "≥250 across these five" but e.g. "drive the suite's
**226** remaining fails toward zero," which requires the contended/infra lanes
(replication real-replica, fork-based persistence, the command-loop pause gate,
blocking-wake propagation) — each a deliberate, coordinated assignment, not a
solo sweep.

## What this lane contributed instead (non-racing, adjacent)

Effect-based replication propagation (`claude/repl-propagation-20260525`, 5
commits, not merged): expire 65/2→67/0, type/string 105/3→108/0, type/list
254/3→255/2, type/zset 318/2→319/1 (+7) under a new `single-node-repl` survey
profile. Foundational for the ~17 `assert_replication_stream` files.
