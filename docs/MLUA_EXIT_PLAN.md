# mlua Exit Plan — replace vendored C Lua with `lua-rs-port`

**Status:** Bring-up started (2026-06-11): `redis-commands` has an opt-in
`lua-rs-engine` Cargo feature for the `EVAL` / `EVALSHA` execution path.
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

## Current bring-up slice

`crates/redis-commands/src/eval/lua_rs_backend.rs` is the first
`lua-rs-port` adapter. It is intentionally opt-in and narrower than the mlua
backend:

- covered now: fresh Lua 5.1-selected runtime per `EVAL`, Redis script
  preflight gates, `KEYS` / `ARGV`, basic Lua-to-RESP conversion,
  `redis.call`, `redis.pcall`, `redis.sha1hex`, `redis.setresp`,
  `redis.status_reply`, `redis.error_reply`, `redis.log`, and
  `redis.acl_check_cmd`;
- first product-shaped script covered now:
  `lua_rs_evalsha_runs_stateful_token_bucket_fixture` exercises `SCRIPT LOAD`
  plus repeated `EVALSHA` calls for a token-bucket limiter using `GET`,
  `SET PX`, `tonumber`, `tostring`, string parsing, integer arithmetic, and
  deterministic allow/deny/remaining/reset output;
- still mlua-backed: Redis Functions (`FUNCTION` / `FCALL`) and the unit tests
  that exercise direct `cjson`, `cmsgpack`, and `bit` helpers;
- deliberately missing in the lua-rs backend: Redis-injected `cjson`,
  `cmsgpack`, `struct`, and full `bit` libraries, RESP3 map/set return
  iteration, cached function runtimes, and full Redis Lua sandbox parity.

The first useful stress finding: Redis global-protection install exposed a
`lua-rs-port` semantic/API edge around assigning globals after `_G` receives a
`__newindex` metamethod. The adapter installs wrapper globals before locking
`_G` for now; lua-rs should grow a conformance test for the exact 5.1 behavior.
The hash-backed policy limiter did not uncover a new Lua runtime gap; it used
the existing command re-entry, bulk-string/nil conversion, `tonumber`, string
parsing, table returns, and integer arithmetic paths.

Concrete lua-rs-port follow-up tickets from this spike:

- **LUA-RS-REDIS-001:** Add a Lua 5.1 conformance test for writes to `_G`
  after `_G` receives a `__newindex` metamethod. Redis script setup wants to
  lock globals after installing injected tables; the current adapter installs
  wrappers first as a workaround.
- **LUA-RS-REDIS-002:** Implement Redis-compatible `cjson` injection in safe
  Rust, including encode/decode error shapes and number/string behavior used by
  Redis scripts.
- **LUA-RS-REDIS-003:** Implement Redis-compatible `cmsgpack` injection in safe
  Rust or define an explicit unsupported-script error for the lua-rs backend.
- **LUA-RS-REDIS-004:** Implement Redis-compatible `struct` and `bit` injected
  libraries. Existing mlua-backed tests cover important bit semantics that the
  lua-rs path must eventually own.
- **LUA-RS-REDIS-005:** Expose enough table iteration/runtime metadata for the
  lua-rs backend to convert RESP3 map/set-style Lua tables without relying on
  mlua-specific traversal behavior.

Validation at this point:

- `cargo check -p redis-commands`
- `cargo check -p redis-commands --features lua-rs-engine`
- `cargo test -p redis-commands --features lua-rs-engine`
- `cargo test -p redis-commands --features lua-rs-engine
  lua_rs_evalsha_runs_stateful_token_bucket_fixture`
- `cargo test -p redis-commands --features lua-rs-engine
  lua_rs_evalsha_reads_hash_policy_for_token_bucket_fixture`
- `cargo check --target wasm32-unknown-unknown -p lua-wasm` in
  `lua-rs-port`
- `cargo check --target wasm32-unknown-unknown -p redis-types -p
  redis-protocol`
- `cargo check --target wasm32-unknown-unknown -p redis-ds`
- `cargo test -p valdr-engine`
- `cargo check --target wasm32-unknown-unknown -p valdr-engine`
- `cargo tree -p valdr-engine --target wasm32-unknown-unknown | rg
  'mlua|mlua-sys|ring|rustls|mio|tikv|jemalloc|libc|getrandom'` returns no
  matches
- `cargo test -p edgestash-demo`
- `cargo check --target wasm32-unknown-unknown -p edgestash-demo`
- `cargo test -p edgestash-cloudflare`
- `cargo check --target wasm32-unknown-unknown -p edgestash-cloudflare`
- `npx wrangler deploy --dry-run --outdir /tmp/edgestash-cloudflare-build`
  from `crates/edgestash-cloudflare`
- `npx wrangler dev --ip 127.0.0.1 --port 8787` with
  `sh fixtures/smoke.sh` from `crates/edgestash-cloudflare`

Wasm status: the Lua runtime is not the first blocker. The new
`crates/valdr-engine` boundary compiles for `wasm32-unknown-unknown` with
`lua-rs-port` scripting, the token-bucket fixture, basic hash commands, a
hash-backed tenant-policy limiter fixture, and the pure REST adapter.
`crates/edgestash-demo` also compiles for `wasm32-unknown-unknown` and proves a
Worker-shaped tenant-shard wrapper can call the same Lua limiter without
pulling the legacy server stack. It also proves cold-start continuation after a
wasm-safe `valdr-engine` snapshot/restore by reloading the Lua script and making
the next limiter decision from persisted bucket state. The provider-neutral
`ObjectStorage` trait proves that state can be bound to a Durable-Object-shaped
storage capability without pulling in native dependencies. The provider-neutral
HTTP route layer also proves the policy install, limiter check, and raw
Upstash-style command paths can sit above the same Lua-backed engine without
adding native dependencies. `crates/edgestash-cloudflare` now maps that shape to
the real `worker` crate: Worker `fetch` routes tenants to Durable Objects, the
Durable Object restores/persists the Valdr snapshot through Cloudflare storage,
and the crate checks for `wasm32-unknown-unknown`. The new Wrangler config also
passes `wrangler deploy --dry-run`, which exercises `worker-build`, emits a
Workers bundle, and confirms the `EDGESTASH` Durable Object binding. The
local `wrangler dev` smoke fixture also passes, including the Lua limiter path
through Worker -> Durable Object -> Valdr -> lua-rs-port and the tenant-scoped
`/v1/valdr` command pass-through. The remaining edge proof is an actual
deployment and latency measurement, not the Rust Wasm compile boundary. The
older full `redis-commands` stack still fails before command logic because it
pulls in native/non-browser dependencies such as `ring` and `getrandom` through
`redis-core`; it also still compiles `mlua-sys` because Redis Functions remain
mlua-backed in that crate.

## Migration steps

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
- [`EDGE_WASM_COMMAND_ENGINE.md`](EDGE_WASM_COMMAND_ENGINE.md) — a product-shaped
  Wasm/edge command-engine spike that motivates the `lua-rs-port` migration.
- `../../lua-rs-port/` — the sibling safe-Rust Lua port (readiness gate).
- Top-level `../../CLAUDE.md` — the three-project / harness-as-product thesis.
