# TCL Hidden Frontier - 2026-05-24

Generated: `2026-05-25T04:05:30.667340+00:00`

This is an illumination artifact, not a conformance claim. It maps the
timeout/no-summary bucket into concrete subsystem packets so broad
implementation work can start from evidence instead of guessing.

## Accounting Snapshot

- Full upstream TCL denominator: **4299** source test blocks
- Counted runner result: **1741 pass / 52 fail / 1793 counted**
- Conservative full-suite proof: **40.5%** counted-pass / full denominator
- Non-skipped denominator: **2568** source test blocks
- Hidden timeout/no-summary bucket: **647** source tests (**25.2%** of non-skipped)

| Status | Source tests |
|---|---:|
| `fail` | 229 |
| `no-summary` | 389 |
| `pass` | 1414 |
| `skipped-by-policy` | 1731 |
| `timeout` | 258 |
| `zero-count` | 278 |

## Focus Files

| File | Tests | Status | Failure mode | First visible failing test / exception | Likely root | Next packet |
|---|---:|---|---|---|---|---|
| `unit/scripting.tcl` | 186 | `no-summary` | no-summary abort at named test | EVAL - is Lua able to call Redis API? | Lua scripting sandbox, redis.call reply conversion, ACL/category exposure | `tcl-scripting-acl-globals-frontier-v1` |
| `unit/functions.tcl` | 112 | `fail` | counted failures | FUNCTION - test function flush will re-create the lua engine | Function library engine, Lua function registry, async/blocking test interaction | `tcl-functions-timeout-scout-then-library-v1` |
| `unit/multi.tcl` | 70 | `pass` | passes |  | Transaction state machine: WATCH-in-MULTI, dirty flag, queueing errors, DISCARD/UNWATCH | `none-currently-passing-regression-guard` |
| `unit/pubsub.tcl` | 34 | `timeout` | timeout after visible failures | Keyspace notifications: stream events test | Pub/Sub keyspace notification ordering and client reply behavior | `tcl-pubsub-keyspace-notify-order-v1` |
| `unit/type/stream.tcl` | 82 | `timeout` | timeout after visible failures | XREAD + multiple XADD inside transaction | Stream XREAD/XADD transaction wake behavior | `tcl-stream-transaction-xread-wake-v1` |
| `unit/type/stream-cgroups.tcl` | 65 | `no-summary` | no-summary abort at named test | Consumer seen-time and active-time | Consumer group PEL metadata and XREADGROUP blocking edge cases | `tcl-stream-cgroups-pel-idle-seen-time-v1` |
| `unit/introspection.tcl` | 117 | `fail` | counted failures | CLIENT KILL with IP filter | Harness tmp-dir/server lifecycle first; then CLIENT/COMMAND/CONFIG/INFO introspection | `tcl-introspection-runner-isolation-v1` |
| `unit/keyspace.tcl` | 65 | `pass` | passes |  | Harness tmp-dir/server lifecycle first; then keyspace/expire/SCAN semantics | `none-currently-passing-regression-guard` |
| `unit/geo.tcl` | 70 | `pass` | passes |  | Harness tmp-dir/server lifecycle first; then GEO command edge semantics | `none-currently-passing-regression-guard` |
| `integration/aof.tcl` | 45 | `zero-count` | 0/0 summary; runner selected no tests under current tag policy |  | AOF durability, check utility compatibility, truncation/corruption repair semantics | `tcl-aof-check-utility-and-corruption-frontier-v1` |
| `integration/rdb.tcl` | 24 | `zero-count` | 0/0 summary; runner selected no tests under current tag policy |  | RDB integration utility/server launch behavior and bgsave cancel/future-version semantics | `tcl-rdb-integration-launch-bgsave-cancel-v1` |

## Ranked Packet Candidates

| Rank | Packet | File | Tests | Value | Risk | Why next |
|---:|---|---|---:|---|---|---|
| 1 | `tcl-scripting-acl-globals-frontier-v1` | `unit/scripting.tcl` | 186 | high | high | Split into two passes: first make the no-summary ACL/FUNCTION abort diagnostic and correct, then address the revealed global-protection and Redis namespace failures. Keep all work inside the scripting/ACL lane. |
| 2 | `tcl-introspection-runner-isolation-v1` | `unit/introspection.tcl` | 117 | medium | medium | Treat the current cat/stdout exception as runner isolation until reproduced otherwise. Give this file a dedicated tmp dir and only then cut CLIENT/COMMAND/INFO implementation packets. |
| 3 | `tcl-functions-timeout-scout-then-library-v1` | `unit/functions.tcl` | 112 | high | high | Do not start with a broad function rewrite. First add a single-test bisect/scout runner for the timeout, then port only the first library lifecycle semantic that blocks summary output. |
| 4 | `tcl-stream-transaction-xread-wake-v1` | `unit/type/stream.tcl` | 82 | high | high | Port the upstream blocked stream client wake semantics around XADD inside MULTI before touching consumer-group metadata. |
| 5 | `tcl-stream-cgroups-pel-idle-seen-time-v1` | `unit/type/stream-cgroups.tcl` | 65 | high | high | Implement the missing `idle`/seen-time dictionary shape and keep the blocking XREADGROUP failures as separate follow-up packets. |
| 6 | `tcl-sort-runner-launch-then-by-get-v1` | `unit/sort.tcl` | 43 | medium | medium | The current timeout says the harness cannot start the server. Fix that visibility issue before changing SORT internals. |
| 7 | `tcl-pubsub-keyspace-notify-order-v1` | `unit/pubsub.tcl` | 34 | medium | medium | Start from the stream event notification mismatch. Verify exact xgroup/xadd ordering and CLIENT REPLY behavior, then rerun the file with a short timeout to see if the hang collapses. |
| 8 | `tcl-bitops-runner-isolation-then-edge-fails-v1` | `unit/bitops.tcl` | 46 | medium | low | Current evidence is a tmp/stdout runner artifact. Isolate the run before spending implementation time. |
| 9 | `tcl-command-list-filterby-v1` | `unit/introspection-2.tcl` | 33 | medium | medium | Implement the missing COMMAND LIST/FILTERBY subcommand path from the generated registry before broader introspection polish. |
| 10 | `tcl-dump-runner-launch-then-restore-edges-v1` | `unit/dump.tcl` | 30 | medium | medium | RDB object oracles are strong; first remove the test server launch failure, then focus on DUMP/RESTORE edge semantics. |

## Per-File Notes

### `unit/scripting.tcl`

- Source tests hidden/covered by this file: **186**
- Latest status: `no-summary` (no-summary abort at named test)
- First visible failing test: `EVAL - is Lua able to call Redis API?`
- Recommended packet: `tcl-scripting-acl-globals-frontier-v1`
- Recommended action: Split into two passes: first make the no-summary ACL/FUNCTION abort diagnostic and correct, then address the revealed global-protection and Redis namespace failures. Keep all work inside the scripting/ACL lane.
- Likely root subsystem: Lua scripting sandbox, redis.call reply conversion, ACL/category exposure
- Latest log: `harness/oracle/results/tcl-survey/20260525T032652Z/unit__scripting.json`
- Local source files:
  - `crates/redis-commands/src/eval.rs`
  - `crates/redis-commands/src/dispatch.rs`
  - `crates/redis-core/src/acl.rs`
- Upstream source anchors:
  - `reference/valkey/src/eval.c`
  - `reference/valkey/src/modules/lua/script_lua.c`
  - `reference/valkey/tests/unit/scripting.tcl`
- First parsed failures:
  - Globals protection reading an undeclared global variable in tests/unit/scripting.tcl
  - Globals protection setting an undeclared global* in tests/unit/scripting.tcl
  - Scripts can handle commands with incorrect arity in tests/unit/scripting.tcl
  - Functions in the Redis namespace are able to report errors in tests/unit/scripting.tcl
  - CLUSTER RESET can not be invoke from within a script in tests/unit/scripting.tcl
- Parsed exception: `OOM command not allowed when used memory > 'maxmemory'..`

### `unit/functions.tcl`

- Source tests hidden/covered by this file: **112**
- Latest status: `fail` (counted failures)
- First visible failing test: `FUNCTION - test function flush will re-create the lua engine`
- Recommended packet: `tcl-functions-timeout-scout-then-library-v1`
- Recommended action: Do not start with a broad function rewrite. First add a single-test bisect/scout runner for the timeout, then port only the first library lifecycle semantic that blocks summary output.
- Likely root subsystem: Function library engine, Lua function registry, async/blocking test interaction
- Latest log: `harness/oracle/results/tcl-survey/20260525T030959Z/unit__functions.json`
- Local source files:
  - `crates/redis-commands/src/eval.rs`
  - `crates/redis-commands/src/connection.rs`
  - `crates/redis-core/src/acl.rs`
- Upstream source anchors:
  - `reference/valkey/src/functions.c`
  - `reference/valkey/src/modules/lua/function_lua.c`
  - `reference/valkey/tests/unit/functions.tcl`
- First parsed failures:
  - FUNCTION - test function flush will re-create the lua engine in tests/unit/functions.tcl
  - LIBRARIES - math.random from function load in tests/unit/functions.tcl
  - LIBRARIES - redis.call from function load in tests/unit/functions.tcl
  - LIBRARIES - redis.setresp from function load in tests/unit/functions.tcl
  - LIBRARIES - redis.set_repl from function load in tests/unit/functions.tcl

### `unit/multi.tcl`

- Source tests hidden/covered by this file: **70**
- Latest status: `pass` (passes)
- First visible failing test: `None`
- Recommended packet: `none-currently-passing-regression-guard`
- Recommended action: Fresh focused scout reaches a clean summary for this file. Do not spend implementation time here now; keep it in the regression inventory.
- Likely root subsystem: Transaction state machine: WATCH-in-MULTI, dirty flag, queueing errors, DISCARD/UNWATCH
- Latest log: `harness/oracle/results/tcl-survey/20260525T024710Z/unit__multi.json`
- Local source files:
  - `crates/redis-commands/src/multi.rs`
  - `crates/redis-core/src/client.rs`
  - `crates/redis-core/src/db.rs`
- Upstream source anchors:
  - `reference/valkey/src/multi.c`
  - `reference/valkey/tests/unit/multi.tcl`

### `unit/pubsub.tcl`

- Source tests hidden/covered by this file: **34**
- Latest status: `timeout` (timeout after visible failures)
- First visible failing test: `Keyspace notifications: stream events test`
- Recommended packet: `tcl-pubsub-keyspace-notify-order-v1`
- Recommended action: Start from the stream event notification mismatch. Verify exact xgroup/xadd ordering and CLIENT REPLY behavior, then rerun the file with a short timeout to see if the hang collapses.
- Likely root subsystem: Pub/Sub keyspace notification ordering and client reply behavior
- Latest log: `harness/oracle/results/tcl-survey/20260524T233238Z/unit__pubsub.json`
- Local source files:
  - `crates/redis-commands/src/pubsub.rs`
  - `crates/redis-core/src/notify.rs`
  - `crates/redis-core/src/pubsub_registry.rs`
  - `crates/redis-commands/src/connection.rs`
- Upstream source anchors:
  - `reference/valkey/src/pubsub.c`
  - `reference/valkey/src/notify.c`
  - `reference/valkey/tests/unit/pubsub.tcl`
- First parsed failures:
  - Keyspace notifications: stream events test in tests/unit/pubsub.tcl

### `unit/type/stream.tcl`

- Source tests hidden/covered by this file: **82**
- Latest status: `timeout` (timeout after visible failures)
- First visible failing test: `XREAD + multiple XADD inside transaction`
- Recommended packet: `tcl-stream-transaction-xread-wake-v1`
- Recommended action: Port the upstream blocked stream client wake semantics around XADD inside MULTI before touching consumer-group metadata.
- Likely root subsystem: Stream XREAD/XADD transaction wake behavior
- Latest log: `harness/oracle/results/tcl-survey/20260524T233238Z/unit__type__stream.json`
- Local source files:
  - `crates/redis-commands/src/stream.rs`
  - `crates/redis-ds/src/stream.rs`
  - `crates/redis-commands/src/multi.rs`
- Upstream source anchors:
  - `reference/valkey/src/t_stream.c`
  - `reference/valkey/tests/unit/type/stream.tcl`
- First parsed failures:
  - XREAD + multiple XADD inside transaction in tests/unit/type/stream.tcl

### `unit/type/stream-cgroups.tcl`

- Source tests hidden/covered by this file: **65**
- Latest status: `no-summary` (no-summary abort at named test)
- First visible failing test: `Consumer seen-time and active-time`
- Recommended packet: `tcl-stream-cgroups-pel-idle-seen-time-v1`
- Recommended action: Implement the missing `idle`/seen-time dictionary shape and keep the blocking XREADGROUP failures as separate follow-up packets.
- Likely root subsystem: Consumer group PEL metadata and XREADGROUP blocking edge cases
- Latest log: `harness/oracle/results/tcl-survey/20260524T233238Z/unit__type__stream-cgroups.json`
- Local source files:
  - `crates/redis-commands/src/stream.rs`
  - `crates/redis-ds/src/stream.rs`
- Upstream source anchors:
  - `reference/valkey/src/t_stream.c`
  - `reference/valkey/tests/unit/type/stream-cgroups.tcl`
- First parsed failures:
  - Blocking XREADGROUP: key type changed with transaction in tests/unit/type/stream-cgroups.tcl
  - Blocking XREADGROUP: swapped DB, key is not a stream in tests/unit/type/stream-cgroups.tcl
  - Blocking XREADGROUP will ignore BLOCK if ID is not > in tests/unit/type/stream-cgroups.tcl
  - Blocking XREADGROUP for stream key that has clients blocked on list in tests/unit/type/stream-cgroups.tcl
  - Blocking XREADGROUP for stream key that has clients blocked on stream - avoid endless loop in tests/unit/type/stream-cgroups.tcl
- Parsed exception: `key "idle" not known in dictionary.`

### `unit/introspection.tcl`

- Source tests hidden/covered by this file: **117**
- Latest status: `fail` (counted failures)
- First visible failing test: `CLIENT KILL with IP filter`
- Recommended packet: `tcl-introspection-runner-isolation-v1`
- Recommended action: Treat the current cat/stdout exception as runner isolation until reproduced otherwise. Give this file a dedicated tmp dir and only then cut CLIENT/COMMAND/INFO implementation packets.
- Likely root subsystem: Harness tmp-dir/server lifecycle first; then CLIENT/COMMAND/CONFIG/INFO introspection
- Latest log: `harness/oracle/results/tcl-survey/20260525T020615Z/unit__introspection.json`
- Local source files:
  - `harness/oracle/tcl-survey.py`
  - `crates/redis-commands/src/connection.rs`
  - `crates/redis-commands/src/info.rs`
  - `crates/redis-commands/src/generated.rs`
- Upstream source anchors:
  - `reference/valkey/src/networking.c`
  - `reference/valkey/src/server.c`
  - `reference/valkey/tests/unit/introspection.tcl`
- First parsed failures:
  - CLIENT KILL with IP filter in tests/unit/introspection.tcl
  - CLIENT KILL with IPv6 filter in tests/unit/introspection.tcl
  - CLIENT KILL with CAPA filter in tests/unit/introspection.tcl
  - CLIENT KILL with NAME filter in tests/unit/introspection.tcl
  - CLIENT KILL with FLAGS filter in tests/unit/introspection.tcl

### `unit/keyspace.tcl`

- Source tests hidden/covered by this file: **65**
- Latest status: `pass` (passes)
- First visible failing test: `None`
- Recommended packet: `none-currently-passing-regression-guard`
- Recommended action: Fresh focused scout reaches a clean summary for this file. Do not spend implementation time here now; keep it in the regression inventory.
- Likely root subsystem: Harness tmp-dir/server lifecycle first; then keyspace/expire/SCAN semantics
- Latest log: `harness/oracle/results/tcl-survey/20260524T233238Z/unit__keyspace.json`
- Local source files:
  - `harness/oracle/tcl-survey.py`
  - `crates/redis-core/src/db.rs`
  - `crates/redis-core/src/expire.rs`
  - `crates/redis-commands/src/dispatch.rs`
- Upstream source anchors:
  - `reference/valkey/src/db.c`
  - `reference/valkey/src/expire.c`
  - `reference/valkey/tests/unit/keyspace.tcl`

### `unit/geo.tcl`

- Source tests hidden/covered by this file: **70**
- Latest status: `pass` (passes)
- First visible failing test: `None`
- Recommended packet: `none-currently-passing-regression-guard`
- Recommended action: Fresh focused scout reaches a clean summary for this file. Do not spend implementation time here now; keep it in the regression inventory.
- Likely root subsystem: Harness tmp-dir/server lifecycle first; then GEO command edge semantics
- Latest log: `harness/oracle/results/tcl-survey/20260524T233238Z/unit__geo.json`
- Local source files:
  - `harness/oracle/tcl-survey.py`
  - `crates/redis-commands/src/geo.rs`
  - `crates/redis-commands/src/geohash_geohash.rs`
- Upstream source anchors:
  - `reference/valkey/src/geo.c`
  - `reference/valkey/tests/unit/geo.tcl`

### `integration/aof.tcl`

- Source tests hidden/covered by this file: **45**
- Latest status: `zero-count` (0/0 summary; runner selected no tests under current tag policy)
- First visible failing test: `None`
- Recommended packet: `tcl-aof-check-utility-and-corruption-frontier-v1`
- Recommended action: Add or alias the `valkey-check-aof` utility first; the current abort is not the server AOF path alone. Then target truncated/unfinished MULTI repair and logged-error parity.
- Likely root subsystem: AOF durability, check utility compatibility, truncation/corruption repair semantics
- Latest log: `harness/oracle/results/tcl-survey/20260524T233238Z/integration__aof.json`
- Local source files:
  - `crates/redis-commands/src/aof.rs`
  - `crates/redis-core/src/persistence.rs`
  - `harness/oracle/persistence-frontier.py`
  - `crates/redis-server/src/bin/valkey-check-aof.rs (new utility target)`
- Upstream source anchors:
  - `reference/valkey/src/aof.c`
  - `reference/valkey/src/valkey-check-aof.c`
  - `reference/valkey/tests/integration/aof.tcl`

### `integration/rdb.tcl`

- Source tests hidden/covered by this file: **24**
- Latest status: `zero-count` (0/0 summary; runner selected no tests under current tag policy)
- First visible failing test: `None`
- Recommended packet: `tcl-rdb-integration-launch-bgsave-cancel-v1`
- Recommended action: RDB object oracles are strong; this frontier is process/integration behavior. Start by making the integration runner launch the right server binary, then implement bgsave-cancel/future-version edges.
- Likely root subsystem: RDB integration utility/server launch behavior and bgsave cancel/future-version semantics
- Latest log: `harness/oracle/results/tcl-survey/20260524T233238Z/integration__rdb.json`
- Local source files:
  - `crates/redis-core/src/rdb/load.rs`
  - `crates/redis-core/src/rdb/save.rs`
  - `crates/redis-commands/src/persist.rs`
  - `harness/oracle/persistence-frontier.py`
- Upstream source anchors:
  - `reference/valkey/src/rdb.c`
  - `reference/valkey/tests/integration/rdb.tcl`

## Operating Guidance

1. Do not spend the next long run on per-test wording fixes. The hidden bucket
   is dominated by subsystem aborts and timeouts.
2. Run one large subsystem packet at a time for scripting/functions/streams.
   Those overlap enough that parallel edits will corrupt interpretation.
3. Runner-artifact frontiers (`cat .../stdout`) should be fixed as harness
   isolation first, not treated as command failures.
4. Persistence integration frontiers are product-critical even though the
   source-test count is smaller; they should run in parallel with scripting
   only if the worktree is isolated.

## Reproduction

```bash
python3 harness/oracle/tcl-suite-inventory.py
python3 harness/oracle/tcl-survey.py --runner-id tcl-hidden-frontier-20260524 \
  --skip-build --timeout-s 75 \
  --files unit/scripting,unit/functions,unit/multi,unit/pubsub,unit/type/stream,unit/type/stream-cgroups,unit/introspection,unit/keyspace,unit/geo,integration/aof,integration/rdb
python3 harness/oracle/tcl-frontier-map.py
```
