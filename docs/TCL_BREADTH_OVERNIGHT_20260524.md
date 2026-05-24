# TCL Breadth Overnight - 2026-05-24

Status: queued breadth-oriented conformance wave. This replaces the stale
single-critical-path queue with a runner-first packet graph cut from the latest
full-suite inventory.

## Goal

The goal for the next multi-hour run is breadth, not polish:

- increase the number of upstream TCL files that run to a real Test Summary;
- unblock large single-node files that currently abort early;
- scout high-value skipped single-node files instead of hiding them behind the
  historical scoped denominator;
- keep full-suite accounting honest after each wave.

Optimization and exact parity can come later. Tonight's useful win is moving
more source-test blocks from `skipped-by-policy`, `no-summary`, and `timeout`
into counted pass/fail telemetry.

## Current Inventory

Latest generated inventory:

```text
harness/oracle/results/tcl-suite-inventory/20260524T033248Z.json
```

Full upstream denominator:

```text
245 TCL files
4,299 source test blocks
```

Current status:

```text
counted pass/fail       ###........................... 489 / 4,299
pass file buckets       ###........................... 492 / 4,299 source tests
no-summary files        #######....................... 1,026 / 4,299
timeout files           ##............................ 264 / 4,299
skipped-by-policy       ##################............ 2,517 / 4,299
```

Interpretation: we are not failing 3,800 tests. We mostly have not run them
through a useful harness yet, or they abort before Valkey's TCL harness emits a
summary. The overnight run should therefore widen the measured surface and fix
abort gates.

## Red Surface By Source-Test Count

| File | Status | Tests | Current blocker | First packet |
|---|---:|---:|---|---|
| `unit/scripting` | no-summary | 186 | Lua sandbox lacks `cjson` global | `tcl-scripting-cjson-lua-libs-v1` |
| `unit/type/zset` | no-summary | 170 | `ZRANGE ... BYLEX` stub aborts file | `tcl-zset-unified-range-bylex-v1` |
| `unit/type/list` | timeout | 148 | list/blocking edge not isolated yet | `tcl-list-timeout-frontier-v1` |
| `unit/type/string` | no-summary | 116 | runner/server stderr artifact issue; likely downstream abort | covered by breadth runner |
| `unit/functions` | no-summary | 112 | function registry case/list/load edges | `tcl-functions-library-breadth-v1` |
| `unit/type/stream` | timeout | 82 | XTRIM/XREAD wake edges | `tcl-stream-trim-cgroup-wake-v1` |
| `unit/expire` | no-summary | 81 | `CLIENT import-source` unsupported | `tcl-client-protocol-info-v1` |
| `unit/type/hash` | no-summary | 81 | `HGETDEL` unknown | `tcl-hash-hgetdel-v1` |
| `unit/multi` | no-summary | 70 | `CLIENT INFO`, WATCH duplicate/queue errors | `tcl-client-protocol-info-v1` |
| `unit/type/stream-cgroups` | no-summary | 65 | PEL/XGROUP SETID ID handling | `tcl-stream-trim-cgroup-wake-v1` |
| `unit/pubsub` | timeout | 34 | `CLIENT REPLY`, keyspace notification behavior | `tcl-pubsub-reply-notify-v1` |
| `unit/protocol` | no-summary | 31 | HELLO `availability_zone` | `tcl-client-protocol-info-v1` |

## Scout Surface

These files are currently `skipped-by-policy` only because we have not swept
them with a generated runner. They are high-value because they are single-node
product surface, not cluster/module/Sentinel product decisions.

| File | Tests | Why scout it now |
|---|---:|---|
| `unit/hashexpire` | 226 | Largest single-node hole. Needs field-level hash TTL model. |
| `unit/introspection` | 117 | Mostly CLIENT/COMMAND/INFO/CONFIG behavior. Likely many cheap wins. |
| `unit/acl` | 114 | ACL engine exists; runner evidence is stale or missing. |
| `unit/acl-v2` | 72 | Same family; may reveal Valkey 9 deltas. |
| `unit/tracking` | 61 | CLIENT TRACKING is mostly a protocol/state feature. |
| `unit/wait` | 37 | WAIT/WAITAOF recently changed; should be measured. |
| `unit/maxmemory` | 13 | Eviction exists; should be measured. |
| `unit/auth` | 13 | ACL/auth exists; likely high ROI. |

The first scout runner should not try to fix all of these. It should produce
typed per-file telemetry so packet generation stops guessing.

## Source Anchors

Use these before touching each subsystem:

- Scripting libraries: `reference/valkey/deps/lua/src/lua_cjson.c`,
  `reference/valkey/src/eval.c`, `reference/valkey/tests/unit/scripting.tcl`.
  Local owner: `crates/redis-commands/src/eval.rs`.
- Functions: `reference/valkey/src/functions.c`,
  `reference/valkey/src/modules/lua/function_lua.c`,
  `reference/valkey/tests/unit/functions.tcl`.
  Local owner: `crates/redis-commands/src/eval.rs` plus
  `crates/redis-commands/src/connection.rs`.
- ZSET unified range: `reference/valkey/src/t_zset.c:2906-3730`,
  especially `zrangeGenericCommand`, `genericZrangebylexCommand`, and the
  result-handler methods. Local owner: `crates/redis-commands/src/zset.rs`.
- Hash delete/field TTL: `reference/valkey/src/t_hash.c:64-145`,
  `471-615`, `1155-1198`, `1359-1752`, `1977-2140`, and
  `2380-2416`. Local owners: `crates/redis-commands/src/hash.rs`,
  `crates/redis-core/src/object.rs`, RDB/AOF only after behavior is green.
- CLIENT/HELLO/tracking/reply: `reference/valkey/src/networking.c:5262-5851`
  and `reference/valkey/src/server.c:6172-6505`. Local owners:
  `crates/redis-commands/src/connection.rs`,
  `crates/redis-core/src/client.rs`, `crates/redis-core/src/live_config.rs`.
- Pub/Sub/keyspace notifications: `reference/valkey/src/pubsub.c`,
  `reference/valkey/src/notify.c`, and `reference/valkey/tests/unit/pubsub.tcl`.
  Local owners: `crates/redis-commands/src/pubsub.rs`,
  `crates/redis-core/src/command_context.rs`.
- Streams: `reference/valkey/src/t_stream.c` and
  `reference/valkey/tests/unit/type/stream*.tcl`. Local owners:
  `crates/redis-commands/src/stream.rs`, `crates/redis-ds/src/stream.rs`.

## Packet Graph

```text
tcl-breadth-current-red-baseline-v1
tcl-breadth-unswept-scout-v1
  ├─ tcl-zset-unified-range-bylex-v1
  ├─ tcl-hash-hgetdel-v1
  ├─ tcl-scripting-cjson-lua-libs-v1
  ├─ tcl-client-protocol-info-v1
  └─ tcl-list-timeout-frontier-v1
       └─ tcl-breadth-current-red-after-wave-a-v1
            ├─ tcl-functions-library-breadth-v1
            ├─ tcl-pubsub-reply-notify-v1
            ├─ tcl-stream-trim-cgroup-wake-v1
            └─ tcl-hash-field-expiry-basic-v1
                 └─ tcl-breadth-expanded-core-v1
                      └─ tcl-suite-inventory-post-breadth-v1
```

The graph is intentionally wide. If one packet blocks, the operator should
record a blocker and move on to another subsystem rather than burning the whole
run on one abort.

Phase-141 hash/zset packets now share batch `tcl-core-wave-a` with
`opportunistic_scope.mode = "same_batch"`. This lets a conformance agent keep a
real adjacent command-family fix in the same wave without failing the current
packet as out-of-scope churn. The sibling work is still not claimable until its
own packet proof runs; the relaxation is only about keeping the loop moving.

## Packet Notes

### `tcl-zset-unified-range-bylex-v1`

Local code already has `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZLEXCOUNT`,
`ZREMRANGEBYLEX`, and `ZRANGESTORE ... BYLEX`. The known abort is the explicit
stub in `zrange_command` for `BYLEX`. The conservative implementation is to
route `ZRANGE key min max BYLEX [REV] [LIMIT ...]` through the existing lex
range helper and preserve the C syntax restrictions:

- `WITHSCORES` with `BYLEX` is a syntax error;
- `REV` means reverse traversal with min/max interpreted like upstream;
- `LIMIT` is valid only with `BYSCORE` or `BYLEX`;
- missing source key returns an empty array/store count zero.

This should turn `unit/type/zset` from abort to counted summary.

### `tcl-hash-hgetdel-v1`

This is the fastest high-confidence breadth win. Implement `HGETDEL key FIELDS
num field ...` using existing hash map helpers:

- reply array of old values or nils, one per requested field;
- delete fields that existed;
- delete the key when the hash becomes empty;
- preserve wrong-type and syntax behavior enough for `unit/type/hash.tcl`;
- add dispatch entry for `HGETDEL`.

Do not implement field TTLs in this packet.

Closeout note 2026-05-24: `HGETDEL` is implemented in the Rust hash command
module and wired through dispatch. Local Rust unit coverage verifies value/nil
array replies, field deletion, empty-key deletion, missing-key nil arrays,
wrong-type rejection, and `numfields` mismatch errors. In this sandbox the TCL
runner cannot allocate its listener range, so focused and broader TCL surveys
stop before running source tests with `Can't find a non busy port`.

### `tcl-scripting-cjson-lua-libs-v1`

Install a Redis-compatible `cjson` table into the `mlua` sandbox. The first
target is the JSON block in `unit/scripting.tcl:378-445`, not the entire Lua
extension ecosystem.

Minimum behavior:

- `cjson.decode` for JSON objects, arrays, strings, numbers, booleans, null;
- `cjson.encode` for Lua tables/numbers/strings/booleans/nil-compatible shape;
- `cjson.null` sentinel;
- no-op or stateful enough `encode_keep_buffer`, `encode_max_depth`,
  `decode_max_depth`, and `encode_invalid_numbers` to match the tests;
- produce Lua errors through normal `redis.call` error conversion, not raw
  multiline Lua stack traces.

Use `serde_json` already present in `redis-commands`.

Closeout note 2026-05-24: the `mlua` sandbox now installs a `cjson` table for
EVAL/functions with `encode`, `decode`, `null`, `new`, and the config setters
used by `unit/scripting`. Focused scripting survey advances through the JSON
block; the next observed abort is the separate missing `cmsgpack` global at
`EVAL - cmsgpack can pack double?`.

### `tcl-client-protocol-info-v1`

This packet should collect cheap protocol/introspection unblocks:

- `HELLO` includes `availability_zone` when `CONFIG SET availability-zone v`
  stores a non-empty value, and omits it when the value is empty.
- `CLIENT INFO` returns the same current-client line shape as `CLIENT LIST`
  for the active connection.
- `CLIENT TRACKING`, `CLIENT CACHING`, and `CLIENT GETREDIR` should be honest
  single-node state, not real invalidation routing yet.
- `CLIENT import-source` may be an accepted no-op state for the import-mode
  expire tests, but fail closed if the command shape is unknown.

This is meant to convert `unit/protocol`, `unit/multi`, `unit/expire`, and
parts of `unit/other`/`unit/introspection` from aborts into counted output.

### `tcl-list-timeout-frontier-v1`

Do not guess. Start by reading the latest `unit/type/list` TCL log and source
test around the timeout. Fix the first real blocking/wake edge only. If the
timeout is just a missing test skip or a runner tag issue, update the runner
policy and record why.

2026-05-24 update: the first isolated blocking edge was `BLPOP/BLMPOP_LEFT
when new key is moved into place`, where `RENAME` created a ready list key
without waking blocked waiters. The focused rename and `SORT ... STORE` wake
subset now passes; the full list file still reaches later non-blocking list
failures/timeouts and should be handled by follow-up packets.

### `tcl-functions-library-breadth-v1`

Improve the existing minimal function registry enough for early
`unit/functions`:

- function names case-insensitive where Valkey expects it;
- library name validation and unknown engine errors match the test patterns;
- `FUNCTION LIST` returns loaded library/function metadata;
- `FUNCTION STATS` reports library/function counts;
- `FCALL` and `FCALL_RO` continue to execute through the existing Lua runtime.

Do not implement replication, long-running function kill, or full dump/restore
unless reached by the same file after the early summary becomes counted.

Closeout note 2026-05-24: function load validation now rejects bad library
names with the Valkey pattern, unknown engines report `Engine '<name>' not
found`, and function lookup/registration collision checks are case-insensitive.
`FUNCTION LIST`, `FUNCTION STATS`, `FUNCTION DUMP/RESTORE`, and `FCALL_RO`
write-command rejection have minimal registry-backed behavior. Focused
`unit/functions` moved past the original load/list/case frontier; full file now
reports 28 passes and stops at the explicitly deferred long-running
`FUNCTION KILL` timeout.

### `tcl-pubsub-reply-notify-v1`

Focus on timeout-causing behavior:

- `CLIENT REPLY OFF|ON|SKIP` suppresses replies at the client flush boundary;
- Pub/Sub `PING` in RESP2 mode has the upstream envelope;
- keyspace/keyevent notifications are delivered for existing call sites when
  `notify-keyspace-events` is configured.

This packet may touch both `Client` state and the server flush path. It should
not rewrite the pub/sub registry.

Closeout note 2026-05-24: `CLIENT REPLY OFF|ON|SKIP` now tracks per-client
reply state and drops ordinary command replies at command completion while
preserving Pub/Sub push frames. RESP2 Pub/Sub `PING` now replies with the
upstream `{pong <payload>}` array, and RESP3 Pub/Sub pushes no longer include
the extra `pubsub` discriminator. Focused `unit/pubsub` coverage for PING,
CLIENT REPLY, and first keyspace/keyevent notification cases passes. Full
`unit/pubsub` moves to 27 passes, then still exposes separate stream
notification and expired-event gaps outside this packet's target files.

### `tcl-stream-trim-cgroup-wake-v1`

Focus on the measured aborts:

- `XADD MAXLEN ~` and `LIMIT` trim semantics;
- XREAD wake suppression for `XADD + DEL` cases;
- PEL reassignment after `XGROUP SETID`.

Do not port the entire stream rax/listpack storage. Current inline stream
storage is acceptable if observable behavior matches this TCL frontier.

Closeout: implemented approximate stream trimming for the measured `MAXLEN ~`
and `LIMIT` cases, including the inline-storage heuristic needed for the
`stream-node-max-entries 100` TCL expectation. `XGROUP SETID ... -` now parses
as the zero cursor, and pending entries are reassigned when a group is rewound
and replayed through a different consumer. `XADD` suppresses transient stream
wakes during EXEC, and list wake drain no longer consumes stream waiters when a
transaction temporarily turns the watched key into a list. Focused TCL passes
for `XADD with MAXLEN option and the '~' argument`, `XADD with LIMIT delete
entries no more than limit`, both `XREAD: XADD + DEL` wake-suppression tests,
and `PEL NACK reassignment after XGROUP SETID event`.

Residual: `XREAD + multiple XADD inside transaction` still needs a separate
EXEC deferred stream-wake hook in `multi.rs` so post-EXEC delivery can batch
the committed stream entries rather than suppressing them entirely.

### `tcl-hash-field-expiry-basic-v1`

This is the largest breadth packet. It should run only after the unswept scout
confirms `unit/hashexpire` is still the biggest red single-node hole.

Minimum behavior:

- add per-field expiry metadata to hash objects;
- lazily hide/delete expired fields on hash reads and writes;
- implement basic `HGETEX`, `HSETEX`, `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`,
  `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`, `HPERSIST`;
- increment `expired_fields` info metric if the code already has a natural
  metric location;
- emit keyspace notifications only where the existing notification path makes
  it cheap.

Non-goals for this packet:

- byte-perfect RDB `RDB_TYPE_HASH_2`;
- replication propagation of field TTLs;
- active field expiration effort matching upstream;
- optimizing the field-expiry index.

If this packet becomes too large, split after the data model plus `HGETEX` and
`HSETEX`; do not burn the night on full field TTL parity.

Closeout note 2026-05-24: implemented the pragmatic side-table field-expiry
model plus `HGETEX`, `HSETEX`, `HEXPIRE`/`HPEXPIRE`, `HEXPIREAT`/`HPEXPIREAT`,
`HTTL`/`HPTTL`, `HEXPIRETIME`/`HPEXPIRETIME`, and `HPERSIST`, including lazy
purge on hash reads/writes, `expired_fields`, `keys_with_volatile_items`, and
basic keyspace notifications. Focused `unit/hashexpire` now clears all local
hash-expiry assertion failures before the first remaining abort:
`HSETEX is not replicating validation arguments`. That abort is in replication
stream/AOF rewrite behavior, explicitly outside this packet's non-goals, and
should be split to a replication/propagation packet before claiming the whole
TCL file green.

## Run Command

Codex autonomous loop, breadth selector only:

```bash
cd /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port
bash harness/run-tcl-breadth-overnight.sh
```

Detached overnight form:

```bash
bash harness/launch-tcl-breadth-overnight.sh
```

Expanded equivalent:

```bash
python3 ../port-harness/loop/run-loop.py \
  --project . \
  --selector nightly \
  --auto-dispatch \
  --dispatch-runtime codex \
  --dispatch-sandbox danger-full-access \
  --dispatch-approval never \
  --dispatch-timeout-s 2400 \
  --max-iterations 18 \
  --max-failures 4 \
  --max-same-packet-failures 2 \
  --reset
```

If Codex runtime is unstable, same queue through Claude:

```bash
python3 ../port-harness/loop/run-loop.py \
  --project . \
  --selector nightly \
  --auto-dispatch \
  --dispatch-runtime claude \
  --dispatch-budget-usd 12 \
  --dispatch-timeout-s 2400 \
  --max-iterations 18 \
  --max-failures 4 \
  --max-same-packet-failures 2 \
  --reset
```

Watch:

```bash
bash harness/watch-tcl-breadth.sh
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

- If a packet fails twice without target-file edits, record it blocked and move
  on. That means packet scope was wrong.
- If an implementation makes a file worse from counted summary to timeout,
  stop the packet and preserve logs for test-fixer work.
- If the run reaches `tcl-breadth-expanded-core-v1`, let it finish even if the
  summary has failures; failures are useful breadth telemetry.
- Do not start cluster, moduleapi, Sentinel, TLS, or multi-node integration
  implementation packets from this queue. Those need separate product/runtime
  decisions.
