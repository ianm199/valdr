# TCL Core Visibility Wave - 2026-05-25

Purpose: drive the Agent 1 overnight lane by maximizing counted upstream TCL
coverage. This is an illumination run first: files moving from timeout,
no-summary, or zero-count into counted pass/fail are wins even when they are not
yet green.

## Goal

Starting snapshot from the coordination board:

```text
Full upstream TCL denominator: 4299 source test blocks
Counted runner result:        2038 pass / 116 fail / 2154 counted
Conservative pass proof:      47.4%
Counted coverage:             50.1%
Hidden timeout/no-summary:    ~409 source tests
```

Stretch target for this wave: push counted coverage above 2500. Moonshot:
2650+ counted.

## Scout

Command:

```bash
python3 harness/oracle/tcl-survey.py \
  --runner-id tcl-core-visibility-wave-agent1-baseline \
  --skip-build \
  --timeout-s 120 \
  --baseport 53111 \
  --portcount 8000 \
  --files unit/pubsub,unit/introspection-2,unit/tracking,unit/wait,unit/maxmemory,unit/auth,unit/pubsubshard,unit/pause,unit/commandlog,unit/latency-monitor,unit/networking,unit/shutdown,unit/obuf-limits,unit/bitops,unit/dump,unit/sort
```

Evidence:

`harness/oracle/results/tcl-survey/20260525T041354Z/`

Result:

```text
16 files, 141 passed tests, 15 failed tests, 2 timed out, 4 without summary
```

| File | Source tests | Scout result | Interpretation |
|---|---:|---|---|
| `unit/pubsub` | 34 | timeout/no-summary | Real hang at keyspace stream notification ordering. |
| `unit/introspection-2` | 33 source lines / 49 counted tests | no-summary at `COMMAND LIST` | Best immediate non-overlapping unlock. |
| `unit/tracking` | 61 | 59/0 | Existing dirty tracking work is valuable; preserve and commit from its owner lane. |
| `unit/wait` | 37 | 0/0 | Current deny-tag policy selects no tests. |
| `unit/maxmemory` | 13 | 0/0 | Current deny-tag policy selects no tests. |
| `unit/auth` | 13 | 0/0 | Current deny-tag policy selects no tests. |
| `unit/pubsubshard` | 11 | 0/0 | Current deny-tag policy selects no tests. |
| `unit/pause` | 23 | 5/15 | Counted-red; not an illumination target unless we want pause semantics. |
| `unit/commandlog` | 20 | 14/0 | Counted-green subset under current tags. |
| `unit/latency-monitor` | 17 | timeout/no-summary | Real timeout; lower denominator but likely related to commandlog/latency globals. |
| `unit/networking` | 9 | 0/0 | Current deny-tag policy selects no tests. |
| `unit/shutdown` | 9 | 0/0 | Current deny-tag policy selects no tests. |
| `unit/obuf-limits` | 12 | 0/0 | Current deny-tag policy selects no tests. |
| `unit/bitops` | 46 | 50/0 | Counted-green under current test file. |
| `unit/dump` | 30 | 13/0 | Counted-green subset under current tags. |
| `unit/sort` | 43 | no-summary | Aborts on `assert_encoding` listpack vs quicklist. Likely object/list encoding interaction. |

## First Pull: `unit/introspection-2`

Patch: add bounded `COMMAND LIST` and compact `COMMAND INFO` handling in
`crates/redis-commands/src/connection.rs`.

Why this was first:

- No overlap with active stream blocking or ACL worktrees.
- The abort was exact and local: `ERR Unknown COMMAND subcommand: list`.
- Upstream `unit/introspection-2` only needs `COMMAND LIST` filtering and the
  flags list at index 2 of `COMMAND INFO` to keep running.

Verification:

```bash
cargo build --bin redis-server
python3 harness/oracle/tcl-survey.py \
  --runner-id tcl-introspection2-command-list-info-v1-final \
  --skip-build \
  --timeout-s 120 \
  --baseport 55111 \
  --portcount 3000 \
  --files unit/introspection-2
```

Evidence:

`harness/oracle/results/tcl-survey/20260525T042310Z/unit__introspection-2.json`

Result:

```text
unit/introspection-2: 46 pass / 3 fail / 49 counted
```

Remaining failures:

- `TTL`, `TYPE`, and `EXISTS` should not alter last access time.
- `TOUCH` should alter last access time.
- `TOUCH` should alter last access time in no-touch mode.

That is an object idle-time/LRU metadata lane, not a COMMAND introspection lane.

## Next Overnight Targets

1. `unit/pubsub`: 34 source tests, currently timeout. First blocker is stream
   keyspace notification ordering (`xgroup-create` arrives where upstream
   expects `xadd`). This is likely `notify.rs` / stream command notification
   ordering, but avoid active stream blocking files unless coordinated.
2. `unit/sort`: 43 source tests, currently no-summary. First blocker is
   listpack vs quicklist encoding assertion. This may overlap object/list
   storage changes already dirty in the main worktree; inspect before editing.
3. `unit/latency-monitor`: 17 source tests, timeout. Smaller denominator, but
   likely a contained latency/commandlog global-state issue.
4. `unit/pause`: 20 counted tests with 15 failures. This is a product-semantic
   packet, not illumination; useful after the bigger dark files are counted.
5. `unit/introspection-2` cleanup: 3 known failures around object idle-time
   mutation. Good small follow-up if no larger dark file is safe to touch.

## Operating Rules For Continuation

- Keep using isolated `--baseport` and `--portcount`.
- One hidden-to-counted file per commit.
- Do not touch active ACL or stream-blocking files without updating
  `AGENT_COORDINATION_BOARD.md`.
- If a target times out after two implementation attempts, record the first
  blocker and move to the next target; the wave is about breadth.
