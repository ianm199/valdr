# mlua → lua-rs scripting migration — empirical conformance map

Measured 2026-06-23 against the real Tcl oracle (companion to `MLUA_EXIT_PLAN.md`,
which is the intent; this is the measured delta + phased plan). The omnilua #189
GC blocker that older notes cite is CLOSED (fixed omnilua 0.2.0) — it is NOT a
blocker. Base Lua 5.1 is NOT the bottleneck.

## PROGRESS (2026-06-23) — EVAL scripting essentially ported

- **omnilua API unblock** (`lua-rs-port` `e61a179`): added `Table::raw_get/raw_set/
  raw_pairs/set_metatable/get_metatable` (additive glue over existing `RawLuaTable`;
  omnilua suite + official Lua 44/44 still green). Path dep → instantly available to valdr.
- **Phase A** (`08ebbc6`): cjson/cmsgpack/bit injected into the lua-rs backend (byte-parity vs mlua).
- **Phase B** (`60e4150`): RESP converter raw-iterates via `raw_pairs` (no metamethod aborts),
  readonly `_G`/redis via `set_metatable`, RESP3 map/set/double, insecure-api reset.
- **Result: lua-rs `unit/scripting` 356 → 423/424 (99.76%)** vs mlua 424/424. The file now
  completes (no abort). The lone failure is `random numbers are random now` (bucket F).
- **Remaining for full mlua-exit:** bucket F (RNG determinism, 1 test) · `struct` lib (net-new)
  · **port FUNCTION/FCALL to lua-rs** (the big chunk — `unit/functions` is still 100% mlua) ·
  then flip the default + drop mlua/lua-src.

## Measured delta (default mlua vs `--features redis-commands/lua-rs-engine`)

| Suite | mlua | lua-rs | Note |
|---|---|---|---|
| unit/scripting | 424/424 | **356/424 (84%)**, −68 instances / 21 test-defs | the real gap |
| unit/functions | 94/94 | 94/94 | **misleading** — FUNCTION/FCALL is never routed through lua-rs (stays mlua even with the feature); 0% ported, invisibly |

The `lua-rs-engine` feature only swaps the EVAL `run_script` seam
(`eval/script_runtime.rs`). All `eval/function_*.rs` are unconditionally mlua.

## Gap buckets (the "all work to do")

| # | Bucket | ~Instances | Effort | Kind |
|---|---|---|---|---|
| A | cjson/cmsgpack/bit injected libs missing on lua-rs | ~30 | M | **REBIND** — logic exists (`eval/lua_{cjson,cmsgpack,bit}.rs`, ~1165 LOC) but is mlua-typed; swap glue to `lua_rs_runtime`. Highest ROI. |
| C | global-protection / readonly `_G` not enforced on lua-rs | ~16 | M–L | net-new semantics (LUA-RS-REDIS-001); writes to `_G`/redis table aren't blocked |
| F | RNG determinism + `lua-enable-insecure-api` reset path | ~9 | M | net-new adapter plumbing (per-script deterministic seeding + config-reset wiring) |
| D | RESP-conversion trips metamethods (use raw-get) | ~8 | S–M | net-new (small); converter must raw-access tables (LUA-RS-REDIS-005) |
| B | RESP3 map/set return conversion | ~2 | M | net-new; `lua_rs_to_resp_inner` has a `// TODO` / `let _ = resp3` |
| E | error wording (`loadstring` nil → wrong message) | ~2 | S | falls out of fixing C |
| — | FUNCTION/FCALL port to lua-rs | all of functions | **L** | net-new, NOT STARTED; biggest single chunk; invisible in the functions numbers |
| — | `struct` injected lib | 0 today | M | net-new for BOTH backends (mlua doesn't inject it either) |

Base Lua 5.1 carried 356/424 cleanly — 0 generic string/table/number/pcall failures.

## Phased plan (recommended order)

1. **Phase A — rebind cjson/cmsgpack/bit for lua-rs** (this slice). ~30 instances,
   logic already correct (passes on mlua), only the Lua-binding glue changes.
   Gate: lua-rs scripting Tcl 356 → ~386, no mlua regression.
2. **Phase B — Redis sandbox/runtime semantics on lua-rs** (buckets C+D+E+B together;
   they share the readonly-table + raw-access + RESP3 converter surface). ~28 instances.
   Some sub-items need lua-rs-port API growth (readonly `_G`, raw table iteration) —
   file/track the LUA-RS-REDIS-00x tickets upstream.
3. **Phase C — RNG determinism + insecure-API reset** (bucket F). ~9 instances.
4. **Phase D — port FUNCTION/FCALL to lua-rs** (the L chunk). Put the `function_*.rs`
   runtime behind the same cfg seam; drive unit/functions on lua-rs to 94/94.
5. **Phase E — `struct` lib + final parity sweep**; then flip the default and drop mlua.

Done-criteria: `unit/scripting` + `unit/functions` reach mlua's counts under
`--features lua-rs-engine`, then the feature becomes the default and `mlua` +
`lua-src`/`luajit-src` leave the dependency tree (the last C in the request path).

## How to re-measure
Build `cargo build --bin redis-server --features redis-commands/lua-rs-engine`,
re-link `target/debug/valkey-server`, run `tcl-survey.py --files unit/scripting`
(and `unit/functions`) `--skip-build`. lua-rs binary sanity check: a `cjson` script
errors "nonexistent global" on lua-rs, works on mlua.
