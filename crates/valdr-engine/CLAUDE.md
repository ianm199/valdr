# valdr-engine — working in this crate

The portable, **wasm-safe** Valdr command engine: Redis/Valkey command semantics
with no server / transport / TLS / OS dependencies, so it runs inside an edge
runtime. This crate is the durable artifact behind **EdgeStash** — for the
product, the request path, and how to run/deploy it, see
[`../edgestash-cloudflare/CLAUDE.md`](../edgestash-cloudflare/CLAUDE.md).
Authoritative design + status log:
[`../../docs/EDGE_WASM_COMMAND_ENGINE.md`](../../docs/EDGE_WASM_COMMAND_ENGINE.md).

## The one invariant: wasm-safety

`valdr-engine` must compile to `wasm32-unknown-unknown` and must never depend on
`mlua`, `ring`, `rustls`, `mio`, `getrandom`, `libc`, jemalloc, OS networking,
threads, the native clock, or the filesystem. Time / randomness / persistence
enter only through the `Host` trait. Check after any dependency change:

```bash
cargo check --target wasm32-unknown-unknown -p valdr-engine
cargo tree -p valdr-engine --target wasm32-unknown-unknown | rg 'mlua|ring|rustls|mio|getrandom|libc|jemalloc'   # must be empty
```

Deps are intentionally tiny: `lua-rs-runtime` (omnilua, the pure-Rust Lua),
`redis-protocol`, `redis-types`, `serde_json`, `indexmap`.

## Public API (`src/lib.rs`)

- `Host` trait — the host capabilities: `now_millis`, `random_bytes`,
  `persist_append`. The engine never reads a clock or RNG directly.
- `Engine<H>` — `execute(argv) -> RespFrame`, `execute_rest(req) -> RestResponse`
  (Upstash-style), `export_snapshot`/`import_snapshot`,
  `export_key`/`import_key` (per-key persistence), `mutation_epoch()`,
  `take_dirty()` (drain changed keys, sorted).
- `command_keys(argv) -> KeyAccess` — static touched-key analysis;
  `KeyAccess` is `FullKeyspace` or `Keys(Vec<Vec<u8>>)`.

## Where things are

- **Command dispatch:** `execute_inner()` in `src/lib.rs` — a cascading
  case-insensitive match on the command name (~19k LOC, one file). To add a
  command: implement it there and add a fixture (below).
- **`command_keys` / lazy load:** returns `FullKeyspace` for SCAN/KEYS/EVAL/etc.
  and `Keys(..)` for point / multi-key commands. This drives the host's
  O(touched) cold-load. **If you add a command you MUST teach `command_keys`
  exactly which keys it touches** — under-fetching is a correctness bug, caught
  by the parity kit, not a perf nit.
- **Lazy parity proof:** `tests/lazy_loader_kit.rs` asserts (1) lazy ≡ eager
  byte-for-byte over the whole fixture corpus, (2) single-key GET against 1000
  keys does ≤1 fetch / 0 lists, (3) SCAN/KEYS triggers exactly one full list.
  Run it after touching `command_keys`.
- **Snapshot:** per-key JSON, absolute `expire_at_ms`. `take_dirty` /
  `export_key` / `import_key` are the per-key surface the Cloudflare adapter
  flushes; HGETALL/SMEMBERS/KEYS replies are **sorted** for snapshot determinism.

## Inner loop

```bash
cargo test -p valdr-engine                                            # all engine unit tests
cargo test -p valdr-engine lazy_matches_eager_over_the_whole_corpus   # after command_keys changes
# the bar — differential vs real valkey-server:
cargo build -p valdr-fixture-runner --release \
  && (cd ../../harness/oracle && python3 valdr-engine-differential.py --strict)
```

Add a fixture line in the matching `harness/oracle/valdr-fixtures/<family>.jsonl`
for every new command / edge case, and pick the tolerance `mode` deliberately:
`set_equal` for unordered replies (the engine sorts), `error_prefix` for Lua
wording, `ttl_band` for clock-dependent values, else `exact`.

## Gotchas

- **HGETALL / SMEMBERS / KEYS** are sorted by the engine, so the oracle compares
  them with `set_equal`, not `exact`. Don't "fix" the sort to match valkey order.
- **Known-unsupported (recorded, not failures):** TIME / RANDOMKEY / SPOP /
  SRANDMEMBER / ZRANDMEMBER (nondeterministic — cannot differential),
  BITFIELD / BITOP, blocking commands (BLPOP etc. — wrong shape for a
  request/response Durable Object). See `known-unsupported.jsonl`.
- **Scripts run through omnilua;** uncaught Lua errors are wrapped in a `pcall`
  harness to dodge an upstream omnilua GC bug (`lua-gc/src/heap.rs:842`) — keep
  the wrapper until the lua-rs-port fix lands.

(No PORT STATUS trailer here — that convention is for `.rs` files. Repo guide:
[`../../CLAUDE.md`](../../CLAUDE.md).)
