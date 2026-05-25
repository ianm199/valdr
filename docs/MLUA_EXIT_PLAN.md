# mlua Exit Plan — replace vendored C Lua with `lua-rs-port`

**Status:** Intended / not started (2026-05-25).
**Owner lane:** scripting (`crates/redis-commands/src/eval.rs`).
**Companion:** [`ADR_001_LUA_RUNTIME.md`](ADR_001_LUA_RUNTIME.md) records *why*
we use mlua today and the reversal seam. This doc records the standing
*intent* to remove it, what blocks that, and the done criteria.

## The goal in one line

Scripting (`EVAL` / `EVALSHA` / `EVAL_RO` / `EVALSHA_RO` / `SCRIPT` /
`FCALL` / `FUNCTION`) is currently backed by **`mlua = "0.10"` with
`features = ["lua51", "vendored"]`** — i.e. bundled, FFI-linked **C Lua
5.1**. That is the **last place this "Valkey in safe Rust" port still
compiles and runs C**. The goal is to swap it for the sibling
**`lua-rs-port`** (safe-Rust Lua) so the scripting path becomes safe Rust
end-to-end.

## Why it matters (not just purity)

The product thesis (see top-level `CLAUDE.md`) is that the *harness* is the
real artifact and the headline is eventually "**nginx, in safe Rust**." The
Valkey port is a proof point. "Valkey in safe Rust" is materially weaker if
`EVAL` still drops into a vendored C interpreter — the one-line pitch has an
asterisk. Closing this gap:

- removes the only C/`unsafe`-via-FFI dependency in the request path
  (`grep -rn '\bunsafe\b' crates/` is already clean in *our* code; mlua's
  unsafe lives in the dependency — this removes that dependency entirely);
- makes "safe Rust" true for the whole server, including scripts;
- exercises `lua-rs-port` under a second real workload (Redis scripts in
  the wild), which is itself harness muscle-building;
- ties the three sibling projects (`lua-rs-port`, `redis-rs-port`,
  eventual `nginx-rs-port`) into one coherent safe-Rust story.

## Why mlua first (the pragmatic call — do not undo lightly)

Conformance before purity. Real Redis ships **C Lua 5.1** with exact
`string.byte` / `table.maxn` / `tostring` / number-formatting semantics that
scripts depend on, plus `cjson` / `cmsgpack` / `struct` / `bit`. mlua gives
wire-diff parity against the upstream Tcl oracle *now*. We get scripting
**conformant** first, then make it **safe** — not the other way around.
`unit/functions.tcl` is already counted (`81/12`, `b01947e`) and
`unit/scripting.tcl` is being unlocked on top of mlua. That progress is the
baseline the swap must not regress.

## The seam is already clean (low structural risk)

Per ADR_001's "Reversal path": the Lua runtime is reachable **only** through
`eval_command`, `evalsha_command`, `script_command` (and the `FUNCTION` /
`FCALL` entrypoints) in `eval.rs`. Each constructs a fresh `Lua` per call;
nothing about mlua leaks into `dispatch.rs`, the command context, the client,
or the protocol layer. The swap replaces the bodies of those entrypoints +
the script-cache helper — **no change to dispatch, client, or wire layer.**

## Blockers — what `lua-rs-port` must reach first

This is gated on the sibling port, not on Redis. Before the swap is viable,
`lua-rs-port` needs feature parity for the subset Redis scripts actually use:

- [ ] `string.*` incl. `string.format`, `string.byte`/`char`, patterns
- [ ] number → string formatting parity (Lua 5.1 `%.14g` semantics)
- [ ] `table.*` incl. `table.maxn`, `table.sort` with comparators
- [ ] `os` subset actually exposed to scripts (Redis removes most of it)
- [ ] coroutine state machine (some libs/scripts use it)
- [ ] bytecode loader / `string.dump` behaviour (or a documented divergence)
- [ ] `pcall` / `error` / traceback formatting matching Redis 7.x messages
- [ ] the Redis-injected libs that live *outside* base Lua and would need
      Rust implementations regardless of engine: **`cjson`, `cmsgpack`,
      `struct`, `bit`** (today these ride on the C ecosystem via mlua glue)

Track `lua-rs-port`'s own test-suite percentage as the readiness signal
(33/44 official tests / ~75% as of 2026-05-19 per top-level `CLAUDE.md`).

## Migration steps (when unblocked)

1. Add a thin `ScriptEngine` trait behind the three+ entrypoints so mlua and
   `lua-rs-port` can be selected at compile time (feature flag) during
   bring-up. Keeps a fallback while parity is proven.
2. Implement the trait against `lua-rs-port`; port the sandbox global-removal
   and the injected `redis` table (see ADR_001 tables) to the new engine.
3. Re-implement / wire `cjson`, `cmsgpack`, `struct`, `bit` in safe Rust.
4. Run the oracle: `unit/scripting.tcl` + `unit/functions.tcl` must hold at
   or above the mlua baseline (no regressions, ideally same counts).
5. Flip the default feature to `lua-rs-port`; keep mlua behind an opt-in
   feature for one release as an escape hatch.
6. Remove the mlua dependency and the escape hatch once the safe-Rust engine
   is green across the scripting + functions suites.

## Done criteria

- `grep -rn 'mlua' crates/ Cargo.toml` returns nothing.
- No `vendored`/C-compile step in the build for scripting.
- `unit/scripting.tcl` and `unit/functions.tcl` are counted at ≥ their
  mlua-era pass counts.
- The one-line pitch "Valkey, in safe Rust — including EVAL" is literally
  true.

## Cross-links

- [`ADR_001_LUA_RUNTIME.md`](ADR_001_LUA_RUNTIME.md) — the accepted decision +
  sandbox/`redis`-table reference.
- `../../lua-rs-port/` — the sibling safe-Rust Lua port (readiness gate).
- Top-level `../../CLAUDE.md` — the three-project / harness-as-product thesis.
