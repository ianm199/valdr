# TCL unlock overnight - 2026-05-24

Status: packet plan for the next long conformance push after the Redis-DS
source-shaped wave landed at `cd97832`.

## Current evidence

Generated single-node dashboard at 2026-05-24T17:43Z:

```text
single_node_core_v1: 52 files / 2531 source test blocks

proved             982 / 2531   38.8%
known-fail         162 / 2531    6.4%
abort/no-summary  1012 / 2531   40.0%
timeout            334 / 2531   13.2%
not-swept           41 / 2531    1.6%
```

The useful target is not the five counted failures. It is the abort and
timeout surface. Each early abort hides many later upstream tests from the
denominator. The overnight run should therefore prefer subsystem unlocks over
small wording fixes.

## Packet order

### 1. Hash field expiry family

Existing packet: `tcl-hash-field-expiry-basic-v1`.

Why first: `unit/hashexpire.tcl` has 226 source tests and currently aborts at
missing `HGETEX`. This is the largest direct single-node unlock.

Source anchors:

- `reference/valkey/src/t_hash.c` around `hgetexCommand`, `hsetex` handling,
  and `hexpireCommand`.
- `reference/valkey/tests/unit/hashexpire.tcl`.

Scope cap: pragmatic per-field expiry semantics are enough. Full active-expire
parity, replication propagation, and byte-perfect RDB HASH_2 persistence are
not required for this wave.

### 2. Lua `cmsgpack`

Packet: `tcl-scripting-cmsgpack-unlock-v1`.

Why: `unit/scripting.tcl` has 186 source tests and currently aborts on
`cmsgpack` being nil. We already have `cjson`; the next embedded Lua library is
the natural next unlock.

Source anchors:

- `reference/valkey/deps/lua/src/lua_cmsgpack.c`.
- `reference/valkey/src/modules/lua/script_lua.c`.
- `reference/valkey/tests/unit/scripting.tcl`.

Scope cap: implement the pack/unpack subset needed by upstream scripting tests
without adding a crate. Keep payloads byte-oriented.

Result 2026-05-24: installed byte-oriented `cmsgpack` and `cmsgpack_safe`
tables in the mlua scripting sandbox from `crates/redis-commands/src/eval.rs`.
Focused proof `harness/oracle/results/tcl-survey/20260524T191412Z/unit__scripting.json`
advances `unit/scripting` past the cmsgpack double, negative-int64, smoke, and
circular-reference tests; the next frontier is the separate missing `bit`
global.

#### Side packet `tcl-scripting-bit-lib-v1`

Result 2026-05-24: installed a Redis-compatible `bit` global (LuaBitOp 1.0.2
surface: `tobit`/`bnot`/`band`/`bor`/`bxor`/`lshift`/`rshift`/`arshift`/`rol`/
`ror`/`bswap`/`tohex`) as a readonly table in the mlua scripting sandbox from
`crates/redis-commands/src/eval.rs`. Semantics are a byte-faithful port of
`reference/valkey/deps/lua/src/lua_bit.c`: 32-bit wrapping math, the
magic-number (`2^52+2^51`) `barg` reduction over the `lua51` double
`lua_Number`, and the `INT32_MIN` guard in `tohex` so
`bit.tohex(65535, -2147483648)` resolves to `0000FFFF`. Focused proof
`harness/oracle/results/tcl-survey/20260524T205655Z/unit__scripting.json`
advances `unit/scripting` past `EVAL - Verify minimal bitop functionality`
(line 574); the abort now lands at the unrelated `os.clock()` test
(`Measures elapsed time os.clock()`, line 784 — missing `os` global), which is
the next frontier. The later `lua bit.tohex bug` test (line 840) is gated
behind that `os` frontier but is already covered by a focused `redis-commands`
unit test (`bit_tohex_int32_min_width_matches_upstream`). Four `bit_*` unit
tests pass; `cargo check -p redis-commands` and `cargo check --workspace` are
clean. No new crate, no `unsafe`, scripting/functions engine unchanged.

#### Side packet `tcl-scripting-os-clock-v1`

Result 2026-05-24: replaced the blanket `os = nil` sandbox strip in
`crates/redis-commands/src/eval.rs` with a faithful minimal `os` table holding
only `os.clock` (process-relative monotonic seconds via `std::time::Instant` +
`OnceLock` epoch), matching Valkey's `script_lua.c` sandbox. Kept as a plain
(non-proxy) table so the sandbox test's `pairs(os)` sees exactly
`{clock = function}` and every other `os.*` stays absent, so `os.execute()`
etc. raise the asserted `attempt to call field 'X' (a nil value)`. Three
`os_*` unit tests pass; `cargo check --workspace` clean. Focused TCL proof
`harness/oracle/results/tcl-survey/20260524T211817Z/unit__scripting.json`
advances `unit/scripting` past `os.clock()` (line 784, which now busy-loops a
real ~1s) and the dangerous-method/global-protection block; abort now lands at
`Script with RESP3 map` (line 1058) — **22** more test blocks crossed.

Next detonator: `Script with RESP3 map` aborts with `Bad protocol, 't' as
reply type byte`. A raw-socket probe (2026-05-24) ruled out the core protocol:
the *non-script* path is correct — `HELLO 3` returns a valid `%7` map and
`HGETALL` returns `%1\r\n$5\r\nfield\r\n$5\r\nvalue\r\n`. The fault is therefore
in the **scripting reply round-trip** in `eval.rs`, not `redis-protocol`:
`run_script {redis.setresp(3); return redis.call('hgetall', KEYS[1])}` exercises
(a) `redis.setresp(3)` making `redis.call` hand the script a RESP3 map-shaped
Lua table, and (b) re-encoding that returned table to the client. The `'t'`
desync points at the Lua-table -> reply conversion for the map case. This is a
contained scripting-lane fix (next packet `tcl-scripting-resp3-map-v1`), still
in `crates/redis-commands/src/eval.rs`.

#### Side packet `tcl-scripting-resp3-map-v1`

Result 2026-05-24: implemented `redis.setresp(n)` (validates 2/3, stored in the
Lua registry, default 2 as upstream) and made the script reply round-trip
RESP-aware in `crates/redis-commands/src/eval.rs`, both the EVAL and FUNCTION
(fcall) paths:

- `reply_to_lua` now takes the script's resp view: a Map/Set reply reaches the
  script as a RESP3 `{map=...}`/`{set=...}` table under `setresp(3)` but as a
  flat RESP2 array under `setresp(2)`.
- `lua_to_resp` now encodes a returned `{map=...}` as a `%` map and `{set=...}`
  as a `~` set **only when the client negotiated RESP3** (`client.resp_proto`),
  otherwise as a flat `*` array — matching upstream's client-vs-script resp
  matrix.
- Lua execution errors are normalized to a single RESP-safe line across
  runtime, callback, and syntax errors. This removed the hidden newline stack
  trace that desynchronized the Tcl client after `redis.sha1hex()`.
- Recursive Lua table replies are bounded with an upstream-shaped
  `-ERR reached lua stack limit` guard instead of recursing until Rust stack
  overflow.

Wire-proven for all four cases in both paths (raw-socket probe):
RESP3-client+`setresp(3)` -> `%1`; the other three (RESP3+2, RESP2+3, RESP2+2)
-> flat `*2`. Focused `redis-commands` unit tests cover map reply view,
map-table encoding, and recursive-table stack limiting; `cargo check
--workspace` clean. Focused oracle:
`harness/oracle/results/tcl-survey/20260524T214220Z/unit__scripting.json`.
`unit/scripting` now runs past `os.clock`, dangerous-method,
`lua bit.tohex bug`, RESP3-map, recursive object, and massive-unpack coverage,
versus aborting at line 1058 before.

Newly *revealed* downstream frontiers (separate packets, not regressions —
they were hidden behind the 1058 abort): counted `[err]`s for Globals
protection (`a=10` not erroring), command arity handling, Redis-namespace error
reporting, and `CLUSTER RESET` inside a script. The current no-summary abort is
later and different: `Script ACL check` authenticates as a restricted user, then
the helper attempts `FUNCTION LOAD` and receives `NOPERM This user has no
permissions to run the 'function' command`. That should be the next contained
scripting/ACL packet, likely by matching upstream ACL categories for FUNCTION
or by making the test helper's load path execute under the expected user
context.

Frontier progression on `unit/scripting.tcl`: cmsgpack (~line 540s) -> bit
(574) -> os (784) -> RESP3 map (1058, cleared) -> recursive reply stack guard
-> scripting ACL/FUNCTION permission abort. File still no-summary until that
ACL detonator clears, but the scripting-globals/reply chain through 1058 is
done.

### 3. `SET IFEQ`

Packet: `tcl-string-ifeq-unlock-v1`.

Why: `unit/type/string.tcl` has 116 source tests and aborts at `SET IFEQ`.
The local parser already has `SET_FLAG_IFEQ`; this is likely a contained
semantic/parser fix with high test-unlock value.

Source anchors:

- `reference/valkey/src/t_string.c:setGenericCommand`.
- `reference/valkey/src/server.c` argument parsing for `ARGS_SET_IFEQ`.
- `reference/valkey/tests/unit/type/string.tcl`.

Result 2026-05-24: implemented SET `IFEQ` parser and conditional write
semantics in `crates/redis-commands/src/string.rs`. Focused TCL proof
`harness/oracle/results/tcl-survey/20260524T180220Z/unit__type__string.json`
now passes the IFEQ block and advances the file frontier to the existing
`LCS basic` gap.

#### Side packet `tcl-string-lcs-v1` — FILE FLIPPED TO SUMMARY

Result 2026-05-24: implemented `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN n]
[WITHMATCHLEN]` in `crates/redis-commands/src/string.rs` (the registered
handler was a stub). Faithful port of `lcsCommand` (t_string.c:842): vanilla
O(n·m) DP table, backward walk to recover both the LCS string and the IDX match
ranges, with the upstream contiguous-range extend/emit logic and `MINMATCHLEN`
filter. A missing key reads as the empty string; a non-string value gives the
upstream `The specified keys must contain string values` error (not WRONGTYPE).
The `IDX` reply is built as a `RespFrame::Map` so `reply_frame` adapts it to the
client's RESP version (RESP3 `%` map / RESP2 flat array) automatically.

Wire-proven against the suite's RNA vectors: basic LCS == `rnalcs` (len 227),
`LEN` == 227, and `IDX` / `IDX WITHMATCHLEN` / `IDX ... MINMATCHLEN 5` match the
expected index structures byte-for-byte. `cargo check --workspace` clean.

LCS was the **last** detonator in `unit/type/string.tcl`: clearing it flipped
the whole file from `abort/no-summary` to a clean summary. Focused oracle:
`harness/oracle/results/tcl-survey/20260524T214220Z/unit__type__string.json`.
Result: **104 passed / 0 failed / 0 aborts, `\o/ All tests passed without
errors!`**. This is the Phase-1 jackpot: ~104 source blocks move straight from
the hidden `abort/no-summary` bucket into `proved`, with no new `known-fail`
revealed.

#### Side packet `tcl-expire-import-mode-v1`

Result 2026-05-24: fixed the two counted `unit/expire.tcl` failures around
import mode (the file already ran to summary at 63/2). Two pieces, faithful to
upstream:

- **Import-source visibility + import-mode keep** (test `Client can visit
  expired key in import-source state`). Added per-command DB state set by the
  dispatcher (`crates/redis-commands/src/dispatch.rs`) from
  `client.import_source` + `import-mode`: an import-source client sees expired
  keys as live (`RedisDb::is_expired` returns false, and the
  `random_key`/`matching_keys`/`keys_snapshot_with_types` filters keep them), so
  `TTL`->0/`GET`/`INCR`/`RANDOMKEY`/`SCAN`/`KEYS` all observe the key; and a
  primary in import-mode reports expired keys as expired to normal clients but
  does **not** lazily delete them (`expire_if_needed` KEEP_EXPIRED branch). C:
  db.c:2126/2144 + `getExpirationPolicyWithFlags` (expire.c:995-1019).
- **EXPIREAT past + import-mode** (test `Negative ttl will not cause server to
  crash when import mode is on`). Wired `check_already_expired` (expire.rs) to
  return false under import-mode and used it in `expire_generic_command`, so a
  past `EXPIREAT` stores the (past) expire instead of deleting; no crash, keys
  stay (dbsize holds) until import-mode is turned off and active expiry resumes.

Verified by a clean RESP-framed wire probe (9/9 behaviors for both tests:
normal `GET`->nil/`TTL`->-2 with `dbsize` held at 1, then import-source
`TTL`->0/`GET`->1/`INCR`->2/`RANDOMKEY`/`SCAN`/`KEYS`->foo1; and EXPIREAT-past
-> dbsize 2 + `PING` alive) plus a `redis-core` unit test covering all four
expiry states. `cargo check --workspace` clean. Oracle confirmed: running
`unit/expire` from an isolated copy of `tests/` (so `::tmproot` does not share
`reference/valkey/tests/tmp/` with the concurrent breadth runner) passes
**65 / 0**, up from 63 / 2 — a `fail` -> `pass` flip.

Operational note: the intermittent `cat/head .../stdout: No such file` aborts
seen across scripting/tracking/pause this session were a **tmp-dir collision**,
not a port or code issue — a concurrent `test_helper` shares
`reference/valkey/tests/tmp/` and its per-file cleanup races other runs. Side
agents should verify single files from an isolated `tests/` copy (cwd with its
own `./tests/tmp`).

### 4. Functions metadata / early library behavior

Packet: `tcl-functions-library-metadata-v1`.

Why: `unit/functions.tcl` has 112 source tests. We already have a minimal
`FUNCTION LOAD` / `FCALL` bridge, but the file still aborts on metadata and
early library semantics.

Source anchors:

- `reference/valkey/src/functions.c`.
- `reference/valkey/src/modules/lua/function_lua.c`.
- `reference/valkey/tests/unit/functions.tcl`.

Scope cap: early single-node library semantics only. Replication, long-running
kill, and complete engine ABI are out of scope.

Result 2026-05-24: implemented source-shaped FUNCTION metadata parsing,
named-argument descriptions, case-insensitive library/function lookup, Valkey
metadata error strings, and FUNCTION STATS/LIST compatibility in
`crates/redis-commands/src/eval.rs`. Focused TCL proof selected the metadata,
FCALL/FCALL_RO, LIST, and STATS rows from `unit/functions.tcl`; full-file
coverage still reaches the known long-running kill non-goal.

### 5. CLIENT LIST filters and command introspection

Packet: `tcl-client-introspection-filters-v1`.

Why: `unit/introspection.tcl` has 117 source tests and aborts at `CLIENT LIST`
IP/filter syntax. The repo already has `redis-core::networking` and
`client_info` scaffolding; this is a good source-shaped integration packet.

Source anchors:

- `reference/valkey/src/networking.c:clientCommand`.
- `reference/valkey/tests/unit/introspection.tcl`.

Scope cap: CLIENT LIST / INFO / FILTERS and COMMAND GETKEYS compatibility
needed by the first frontier. Full MONITOR and tracking invalidation are
separate packets.

Result 2026-05-24: implemented byte-oriented CLIENT LIST/INFO fields,
stateful CLIENT CAPA/SETINFO metadata, common positive and negative LIST
filters, and early COMMAND GETKEYS/GETKEYSANDFLAGS key extraction in
`crates/redis-commands/src/connection.rs` with supporting client snapshot
state in `redis-core`. Focused TCL proof passed 32 CLIENT LIST filter rows from
`unit/introspection.tcl` and 11 COMMAND GETKEYS/GETKEYSANDFLAGS rows from
`unit/introspection-2.tcl`.

### 6. Observability admin commands

Packet: `tcl-observability-admin-unlock-v1`.

Why: `unit/other.tcl`, `unit/commandlog.tcl`, and `unit/latency-monitor.tcl`
are small but currently abort on missing `MONITOR`, `COMMANDLOG`, and
`LATENCY HISTOGRAM` behavior. This can convert three files from no-summary to
counted outcomes.

Source anchors:

- `reference/valkey/src/commandlog.c`.
- `reference/valkey/src/latency.c`.
- `reference/valkey/src/server.c:monitorCommand`.
- `reference/valkey/tests/unit/other.tcl`.
- `reference/valkey/tests/unit/commandlog.tcl`.
- `reference/valkey/tests/unit/latency-monitor.tcl`.

Scope cap: enough single-node observability behavior to avoid early aborts.
Streaming MONITOR perfection is not required if the test frontier only needs
command availability and basic output shape.

### 7. Client tracking minimal state

Packet: `tcl-client-tracking-minimal-v1`.

Why: `unit/tracking.tcl` has 61 source tests and aborts on CLIENT TRACKING.
Full invalidation routing is large, but the local repo already has a tracking
module and client state fields. A minimal stateful implementation may unlock
counted failures.

Source anchors:

- `reference/valkey/src/tracking.c`.
- `reference/valkey/src/networking.c:clientCommand`.
- `reference/valkey/tests/unit/tracking.tcl`.

Scope cap: parse ON/OFF/TRACKINGINFO and maintain per-client visible state.
Do not claim full cache-invalidation semantics unless tests prove it.

Packet result note, 2026-05-24:

- Implemented stateful `CLIENT TRACKING ON/OFF`, `CLIENT CACHING`,
  `CLIENT GETREDIR`, `CLIENT TRACKINGINFO`, prefix collision handling, and a
  minimal live invalidation runtime for common read/write key shapes.
- Focused proof now reaches `Tracking info is correct` without the earlier
  tracking timeout. The remaining abort is outside this packet boundary:
  `INFO` does not emit `tracking_total_items`, `tracking_total_keys`,
  `tracking_total_prefixes`, or `tracking_clients`.
- Required boundary widening: add `crates/redis-commands/src/info.rs` to a
  follow-up packet so INFO can report the runtime tracking counters.
- Non-goals intentionally left for a later tracking fidelity packet:
  complete script read-key introspection, exact self-push ordering for every
  command family, eviction-before-response ordering, and full server-owned
  tracking state integration.

### 8. List timeout after Redis-DS wave

Packet: `tcl-list-timeout-post-ds-v2`.

Why: `unit/type/list.tcl` still times out. The Redis-DS wave just added
ListPack and QuickList structures, so this is the first reasonable moment to
ask an agent whether list behavior can be moved closer to upstream without a
full storage rewrite.

Scope cap: fix the first measured timeout/hang or convert the file to counted
failures. A full object-storage migration is too large for this packet unless
it falls out naturally.

Packet result note, 2026-05-24:

- Converted `unit/type/list` from timeout/no-summary to counted TCL output:
  **251 passed / 3 failed / 0 timed out / 0 without summary**.
- Required boundary widening beyond the original list/object/quicklist target:
  `CLIENT UNBLOCK`/`CLIENT UNPAUSE` behavior lives in `connection.rs`, and
  cross-owner blocked-client wake cleanup lives in `runtime_owner.rs`.
- The three remaining counted failures are now concrete follow-ups:
  listpack-to-quicklist encoding threshold fidelity and two `BLPOP`
  commandstats accounting rows.

## Final evidence

The morning scoreboard should be:

1. `tcl-breadth-expanded-core-v1` - existing broad single-node core runner.
2. `tcl-breadth-expanded-core-v2` - same runner after this unlock wave.
3. `tcl-suite-inventory-post-breadth-v2` - full TCL inventory refresh.
4. `single-node-core-dashboard-post-unlock-v1` - generated dashboard against
   the 2531-test single-node-core denominator.

The goal is not "everything green by morning." The goal is to reduce
`abort/no-summary` and `timeout`, and turn hidden tests into either `proved` or
concrete `known-fail` rows.

## Wave C queue, 2026-05-24

The first post-tracking inventory exposed one harness visibility bug: colored
TCL `Test Summary` lines were not parsed, so green files like `unit/type/zset`
were misclassified as `no-summary`. After stripping ANSI before summary parse,
the current counted result is:

```text
full upstream TCL:      245 files / 4299 source test blocks
counted TCL results:   1014 pass / 5 fail / 1019 counted
single_node_core_v1:   1038 proved / 162 known-fail / 677 abort / 613 timeout / 41 not-swept
```

Wave C is intentionally broader than another tiny frontier fix:

- `tcl-list-timeout-post-ds-v2` remains the next mainline packet. It targets
  the 148-test `unit/type/list` timeout after the Redis-DS quicklist/listpack
  wave.
- `tcl-tracking-info-counters-v1` follows the minimal tracking packet and adds
  real INFO counters so `unit/tracking` can move beyond `Tracking info is
  correct`.
- `tcl-string-lcs-v1` ports Valkey's LCS command shape for the current
  `unit/type/string` abort.
- `tcl-hashexpire-repl-stream-v1` targets the current 226-test
  `unit/hashexpire` abort where `attach_to_replication_stream` sees
  `+FULLRESYNC` instead of a counted command stream.
- `tcl-scripting-bit-lib-v1` is marked `manual` for a side worktree: it should
  install the Redis Lua `bit` library without conflicting with the main
  list/quicklist path.

Side packet result, 2026-05-24:

- `tcl-tracking-info-counters-v1` exposed live client-tracking counters in
  `INFO` from the packet-level runtime (`redis-core/src/tracking.rs`), matching
  the upstream `tracking.c`/`server.c` fields:
  `tracking_clients`, `tracking_total_items`, `tracking_total_keys`, and
  `tracking_total_prefixes`. A Rust unit test models the upstream scenario
  directly: one normal tracking client reads `key1`/`key2`, one BCAST client
  registers `prefix:`, and the snapshot reports `2 / 2 / 1 / 2`.
  Manual RESP probing against the server confirms the exact INFO text expected
  by `unit/tracking.tcl`. The full file still aborts before it can claim a
  clean summary because earlier invalidation-order tests leave the Tcl client
  stream desynchronized; this is a source-shaped sub-frontier fix, not a whole
  file unlock.
- `tcl-scripting-bit-lib-v1` installed the Valkey-compatible Lua `bit` global.
- `tcl-scripting-os-clock-v1` installed the sandboxed Lua `os` table with only
  `os.clock`, plus the Lua `{double = n}` reply bridge required by the elapsed
  time test.
- `unit/scripting` now advances past the bit and `os.clock` frontiers to
  `Script with RESP3 map`; it is still no-summary until RESP3 map/protocol and
  globals-protection errors are addressed.
- `tcl-hashexpire-repl-stream-v1` flipped `unit/hashexpire` to a counted
  clean file: 207/207. The fix set legacy `SYNC` apart from `PSYNC` during
  full resync, emits replication `SELECT` frames when DB selection changes,
  rewrites `HSETEX` propagation to canonical `PXAT` without validation flags,
  preserves hash-field expiry metadata across `COPY`, handles import-mode
  zero-TTL hash fields without eager deletion, and moves `expired_fields`
  reset ownership from opportunistic empty-DB `HSET` cleanup to
  `CONFIG RESETSTAT`. Evidence:
  `harness/oracle/results/tcl-survey/20260524T225345Z/unit__hashexpire.json`.
