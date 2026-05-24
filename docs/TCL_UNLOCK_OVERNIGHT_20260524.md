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
