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

### 3. `SET IFEQ`

Packet: `tcl-string-ifeq-unlock-v1`.

Why: `unit/type/string.tcl` has 116 source tests and aborts at `SET IFEQ`.
The local parser already has `SET_FLAG_IFEQ`; this is likely a contained
semantic/parser fix with high test-unlock value.

Source anchors:

- `reference/valkey/src/t_string.c:setGenericCommand`.
- `reference/valkey/src/server.c` argument parsing for `ARGS_SET_IFEQ`.
- `reference/valkey/tests/unit/type/string.tcl`.

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

### 8. List timeout after Redis-DS wave

Packet: `tcl-list-timeout-post-ds-v2`.

Why: `unit/type/list.tcl` still times out. The Redis-DS wave just added
ListPack and QuickList structures, so this is the first reasonable moment to
ask an agent whether list behavior can be moved closer to upstream without a
full storage rewrite.

Scope cap: fix the first measured timeout/hang or convert the file to counted
failures. A full object-storage migration is too large for this packet unless
it falls out naturally.

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
