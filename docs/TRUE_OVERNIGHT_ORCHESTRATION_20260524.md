# True Overnight Orchestration - 2026-05-24

Status: planning document for a high-throughput Redis/Valkey conformance run.
This file is intentionally separate from the root coordination task list so the
worktree can carry its own execution notes.

## Current Reality

There are two different coordination surfaces right now:

- Coordination tree:
  `/Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port`
  on `main`, dirty, ahead of origin, and containing the new `tcl-breadth-*`
  nightly packet graph, breadth runners, evidence, and dashboard scripts.
- Implementation worktree:
  `/Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port-worktree`
  on `codex/tcl-breadth-wave-a-20260524`, clean at `a93a2ad`.

Important mismatch: this worktree does not yet contain
`docs/TCL_BREADTH_OVERNIGHT_20260524.md`, the breadth launcher/watch scripts,
or the `tcl-breadth-*` packet IDs in `harness/work-packets.jsonl`.
`parallel-plan --selector nightly` selects nothing in this worktree. The dirty
main tree currently selects `tcl-breadth-current-red-baseline-v1`.

The run should therefore choose one mode before launching:

1. Use dirty `redis-rs-port` as the coordination/run tree and use this worktree
   for isolated implementation branches.
2. Promote/sync the breadth harness artifacts into this worktree, then run the
   overnight loop here.

Mode 1 is safer immediately because it preserves the current evidence ledger
and packet graph. Mode 2 is cleaner for code review after the harness edits are
made explicit and committed or mechanically copied.

## Goal

Move upstream TCL coverage from invisible buckets into counted telemetry:

- reduce `skipped-by-policy`, `no-summary`, and `timeout`;
- unblock large single-node TCL files with narrow command/protocol fixes;
- preserve runner evidence in `harness/evidence/ledger.jsonl`;
- avoid spending the night perfecting one file while thousands of source tests
  remain unmeasured.

The overnight success metric is not LOC. It is movement across the full
4,299-test denominator: more files with real Test Summary output and fewer
abort gates.

## Preflight Checklist

- [ ] Pick the run tree: dirty main as coordinator, or synced worktree.
- [ ] Confirm the selected queue:
  `python3 ../port-harness/loop/parallel-plan.py --project . --selector nightly --json`.
- [ ] Confirm completion status:
  `python3 ../port-harness/loop/check-completion.py --project . --json`.
- [ ] Confirm no stale active loop owns `oracle-results`, `cargo-target`, or
  `port-range:21111-29111`.
- [ ] Preserve current dirty main state before broad code edits:
  `git status --short --branch`.
- [ ] If using this worktree as the run tree, sync the breadth artifacts first:
  `harness/work-packets.jsonl`, `harness/runners.toml`,
  `harness/completion.toml`, `harness/oracle/tcl-survey.py`,
  `harness/oracle/tcl-suite-inventory.py`,
  `docs/TCL_BREADTH_OVERNIGHT_20260524.md`,
  `harness/run-tcl-breadth-overnight.sh`, and
  `harness/watch-tcl-breadth.sh`.

Do not sync unrelated dirty persistence implementation files into the Wave A
worktree unless the run intentionally moves onto that full main state.

## Queue Shape

The intended breadth queue from the coordination tree is:

```text
tcl-breadth-current-red-baseline-v1
tcl-breadth-unswept-scout-v1
  -> Wave A:
     tcl-zset-unified-range-bylex-v1
     tcl-hash-hgetdel-v1
     tcl-scripting-cjson-lua-libs-v1
     tcl-client-protocol-info-v1
     tcl-list-timeout-frontier-v1
  -> tcl-breadth-current-red-after-wave-a-v1
  -> Wave B:
     tcl-functions-library-breadth-v1
     tcl-pubsub-reply-notify-v1
     tcl-stream-trim-cgroup-wake-v1
     tcl-hash-field-expiry-basic-v1
  -> tcl-breadth-expanded-core-v1
  -> tcl-suite-inventory-post-breadth-v1
```

In the clean worktree before sync, the actual `auto` queue is the older
scripting frontier:

```text
tcl-scripting-wait-waitaof-v3
-> tcl-post-scripting-wait-survey-v3
-> tcl-scripting-cjson-v3
-> tcl-post-scripting-cjson-survey-v3
-> tcl-core-expanded-survey-v2
```

Do not confuse these queues. If the plan is the true breadth overnight, run
from the tree that contains the `tcl-breadth-*` packets.

## Coordinator Rules

The parent coordinator owns these decisions:

- runner execution and evidence attachment;
- packet ordering and resource locks;
- final integration of subagent patches;
- any architecture/data-model choice;
- stopping a packet when it grows beyond scope.

Runner packets should normally run locally under the coordinator. They lock
`oracle-results`, `cargo-target`, and often `port-range:21111-29111`; letting
multiple agents run them independently will create noisy evidence and port
conflicts.

Workers can edit implementation files, but they are not alone in the codebase.
Every worker prompt must say:

- do not revert edits made by others;
- keep to the assigned files and packet scope;
- stop with `TODO(architect)` or a blocker note instead of inventing a
  cross-cutting design;
- report changed files and exact gates run.

## Model And Subagent Strategy

Use `gpt-5.3-codex-spark` for narrow source-shaped implementation slices when
all of these are true:

- one subsystem owner and a small target file set;
- source anchors are explicit;
- no new canonical type, dependency edge, or runtime ownership question;
- no unsafe and no byte/String ambiguity;
- objective local gate exists, such as a crate test plus one focused TCL file.

Use the parent or a stronger/deeper worker when any of these are true:

- multiple ownership domains are involved;
- the packet touches client state, live config, runtime-owner, blocked wakeups,
  pub/sub delivery, scripting runtime, replication, AOF/RDB, or persistence;
- the packet requires a data model rather than a command shim;
- latest runner logs must be interpreted before the target is known;
- the packet note says to isolate the frontier first.

Use explorer agents for read-only source/log questions. Use worker agents for
bounded code changes with disjoint write sets. Avoid multiple workers in
`eval.rs`, `connection.rs`, `client.rs`, or `dispatch.rs` at the same time.

## Wave A Dispatch Matrix

### `tcl-hash-hgetdel-v1`

Recommended agent: Spark worker.

Why: small command, clear behavior, mostly `hash.rs` plus dispatch. It should
not implement hash field expiry.

Owned files:

- `crates/redis-commands/src/hash.rs`
- `crates/redis-commands/src/dispatch.rs`

Required proof:

```bash
cargo test -p redis-commands hash
python3 harness/oracle/tcl-survey.py --skip-build --timeout-s 120 --files unit/type/hash
```

### `tcl-zset-unified-range-bylex-v1`

Recommended agent: Spark worker if the existing lex helpers are as described;
otherwise escalate after source read.

Why: expected to route `ZRANGE ... BYLEX` through existing lex range code.

Owned files:

- `crates/redis-commands/src/zset.rs`

Required proof:

```bash
cargo test -p redis-commands zset
python3 harness/oracle/tcl-survey.py --skip-build --timeout-s 180 --files unit/type/zset
```

### `tcl-scripting-cjson-lua-libs-v1`

Recommended agent: parent or strong worker, not Spark by default.

Why: Lua sandbox semantics, `cjson.null`, error conversion, and table/JSON
shape can spill across scripting behavior.

Owned files:

- `crates/redis-commands/src/eval.rs`

Required proof:

```bash
cargo test -p redis-commands eval
python3 harness/oracle/tcl-survey.py --skip-build --timeout-s 180 --files unit/scripting
```

Scope discipline:

- implement `cjson` only;
- do not add `cmsgpack`, `bitop`, or a larger function engine unless a fresh
  runner proves that is the next blocker;
- use `serde_json` already in the workspace.

### `tcl-client-protocol-info-v1`

Recommended agent: parent or strong worker with explicit file ownership.

Why: touches protocol shape, client state, config state, and current-client
metadata. This is not a blind C port.

Owned files:

- `crates/redis-commands/src/connection.rs`
- `crates/redis-core/src/client.rs`
- `crates/redis-core/src/live_config.rs`

Required proof:

```bash
cargo test -p redis-commands connection
python3 harness/oracle/tcl-survey.py --skip-build --timeout-s 180 --files unit/protocol,unit/multi,unit/expire
```

Scope discipline:

- `CLIENT TRACKING`, `CLIENT CACHING`, and `CLIENT GETREDIR` can be honest
  single-node state;
- do not implement full invalidation routing in this packet;
- `CLIENT import-source` may be a constrained no-op only if the TCL expire
  frontier needs it.

### `tcl-list-timeout-frontier-v1`

Recommended agent: explorer first, then parent or focused worker.

Why: this is a timeout frontier. The first job is to identify the exact
blocking/wake edge from the latest log and upstream test, not to rewrite lists.

Owned files after diagnosis:

- `crates/redis-commands/src/list.rs`
- possibly `crates/redis-core/src/client.rs`
- possibly `crates/redis-server/src/main.rs`

Required proof:

```bash
python3 harness/oracle/tcl-survey.py --skip-build --timeout-s 240 --files unit/type/list
```

Stop if the timeout cannot be reproduced or mapped to one concrete edge.
Record the log path and mark the packet blocked rather than guessing.

## Wave B Dispatch Matrix

Wave B should only start after `tcl-breadth-current-red-after-wave-a-v1`
captures the new frontier.

### `tcl-functions-library-breadth-v1`

Use parent or deep worker. It shares `eval.rs` and `connection.rs` with other
packets and needs registry semantics, metadata, and FCALL behavior. Do not let
Spark expand this opportunistically.

### `tcl-pubsub-reply-notify-v1`

Use parent or deep worker. It touches `CLIENT REPLY`, flush behavior, pub/sub
envelopes, notification delivery, and possibly runtime-owner paths. This needs
architecture-level care.

### `tcl-stream-trim-cgroup-wake-v1`

Use explorer first. If the log shows a precise trim or SETID behavior, a
focused worker can patch `stream.rs` and `redis-ds/src/stream.rs`. Do not port
the whole Valkey stream storage model in this packet.

### `tcl-hash-field-expiry-basic-v1`

Use parent or deep architecture first. This introduces per-field TTL metadata
and read/write hiding semantics. A Spark worker can help with command parsing
only after the data model is chosen.

## Integration Order

After the baseline and scout runners finish:

1. Dispatch Spark workers for `HGETDEL` and `ZRANGE BYLEX` in parallel if their
   write sets are disjoint.
2. Keep `cjson`, `CLIENT/HELLO`, and list timeout diagnosis separate.
3. Integrate the smallest isolated patch first: `HGETDEL`.
4. Integrate `ZRANGE BYLEX`.
5. Integrate `CLIENT/HELLO` only after its focused protocol/multi/expire gates
   are clean enough to preserve counted output.
6. Integrate `cjson` after scripting tests reach the JSON block and the patch
   does not worsen earlier scripting output.
7. Integrate the list fix only if it is concrete and bounded.
8. Run `tcl-breadth-current-red-after-wave-a-v1`.
9. Select Wave B based on the refreshed logs, not the pre-run guess.

If any patch turns a counted file into `timeout` or `no-summary`, stop and
preserve the logs before continuing.

## Worker Prompt Templates

Spark implementation worker:

```text
You are working in <worktree>. You are not alone in the codebase; do not
revert edits made by others. Own only <files>. Implement packet <id> using
PORTING.md and these Valkey source anchors: <anchors>. Keep scope to <scope>.
If you need a cross-cutting type, new dependency edge, runtime ownership
decision, unsafe, or broader behavior than the packet says, stop with
TODO(architect) and report the blocker. Run <gates>. Final answer: changed
files, tests run, remaining risks.
```

Explorer:

```text
Read only. In <worktree>, inspect latest runner logs for <packet/file> plus
the upstream Valkey test/source anchors. Identify the first concrete abort,
timeout, or semantic mismatch and propose the smallest implementation target.
Do not edit files.
```

Deep worker:

```text
You are working in <worktree>. You are not alone in the codebase; do not
revert edits made by others. Own only <files>. This packet crosses subsystem
state, so read the upstream source and local owners before editing. Preserve
existing dispatch, ACL, MULTI/EXEC, scripting, expiration, pub/sub, blocking,
AOF, RDB, and replication semantics unless the packet explicitly scopes them.
Run <gates>. Final answer: changed files, tests run, architecture assumptions,
remaining risks.
```

## Launch Commands

From the coordination tree that contains the breadth packet graph:

```bash
cd /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port
python3 ../port-harness/loop/parallel-plan.py --project . --selector nightly --json | python3 -m json.tool
python3 ../port-harness/loop/run-loop.py \
  --project . \
  --selector nightly \
  --auto-dispatch \
  --dispatch-runtime codex \
  --dispatch-sandbox workspace-write \
  --dispatch-approval never \
  --dispatch-timeout-s 2400 \
  --max-iterations 18 \
  --max-failures 4 \
  --max-same-packet-failures 2 \
  --reset
```

Watch:

```bash
python3 ../port-harness/loop/watch.py --project .
tail -f harness/loop/state/loop-state.json
tail -n 40 harness/evidence/ledger.jsonl | jq -r '[.ts,.kind,(.packet // .runner // ""),(.summary // .runner_status // "")] | @tsv'
```

Post-run accounting:

```bash
python3 harness/oracle/tcl-suite-inventory.py
python3 ../port-harness/loop/parallel-plan.py --project . --selector nightly --json | python3 -m json.tool
python3 ../port-harness/loop/check-completion.py --project . --json | python3 -m json.tool
```

## Stop Rules

- If a packet fails twice without target-file edits, mark it blocked and move
  to another subsystem.
- If a change turns counted output into timeout/no-summary, stop and preserve
  logs.
- If a packet grows into cluster, module ABI, Sentinel, TLS, or full
  replication, stop and cut a product-scope decision.
- If a Spark worker reports `TODO(architect)`, do not ask it to push through.
  Bring the work back to parent/deep architecture.
- If the run reaches `tcl-breadth-expanded-core-v1`, let it finish even with
  counted failures. Counted failures are useful evidence.

## Morning Closeout

- [ ] Save latest inventory path and source-test bucket counts.
- [ ] List files moved from `no-summary` or `timeout` to counted output.
- [ ] List files that regressed into `timeout` or `no-summary`.
- [ ] Attach latest evidence rows and runner artifact paths.
- [ ] Update the root task list claims: complete, blocked with log path, or
  next packet.
- [ ] Decide the next wave from evidence, not from stale packet expectations.
