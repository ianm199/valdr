# EdgeStash cold-start — Phase 2 prep (analysis only; needs live measurement)

Overnight-safe analysis of the ~0.5s cold Durable Object start (the open
optimization target per the `cloudflare-deploy-blocker` memory). **No deploy or
measurement was run here** — cold-start numbers require a live `wrangler deploy`
+ latency run, which is an interactive step. This doc stages the work.

## Where cold-start cost comes from (read from `crates/edgestash-cloudflare/src/lib.rs`)

On a DO's first request (`self.hot.borrow().is_none()` in `fetch`):
1. **Wasm module instantiation** — the DO cold-boots and instantiates the
   `wasm32-unknown-unknown` engine. Platform-bound; scales with **wasm binary size**.
2. **`load_entries()` → `storage.list()`** (`lib.rs:128`) — a single bulk read of
   **every** `k:`-prefixed storage entry, then `MemoryObjectStorage::from_entries`
   + `EdgeObject::open` rebuilds the whole engine in memory. Cost is **O(total
   tenant state)** — every key is read and decoded on the first request even if
   the request only touches one key.

So cold-start ≈ `wasm_instantiate(size)` + `storage.list()` + `O(state)` rebuild.

## Two candidate optimizations (hypotheses — validate with live measurement)

### A. Lazy per-key loading — make cold-start O(1) instead of O(state)  [biggest structural win]
Today the engine is fully reconstructed from `storage.list()` before serving the
first request. The persistence model is already **per-key** (`export_key`/
`import_key`, `take_dirty`), so the eager full scan is not required by the data
model — it's an adapter choice.
- Change: give the engine a **lazy backing store**. On a command, for each key it
  touches that isn't resident, the adapter does a single `storage.get("k:"+hex)`
  and `import_key`s it; the engine serves from memory thereafter. Drop the
  upfront `storage.list()`.
- Win: first-request latency becomes independent of total tenant state — a DO
  holding 10k keys cold-starts as fast as one holding 1. The common edge workload
  (a request touches 1–3 keys: a rate-limit counter, a quota, an idempotency key)
  pays 1–3 `storage.get`s instead of one whole-state `storage.list()` + rebuild.
- Cost/caveats: commands that enumerate the keyspace (`KEYS`, `SCAN`, `DBSIZE`,
  `FLUSHALL`, multi-key aggregates over unknown keys) still need the full set —
  keep a `storage.list()` fallback that runs only when such a command is issued,
  and mark the store "fully loaded" afterward. Needs a resident/absent bit per key
  and care that "key absent in memory" ≠ "key absent in storage" until loaded.
- Effort: medium. Engine gains a `LazyStore` notion + a host fetch callback; the
  adapter wires `storage.get`. Gate on the differential oracle (behavior must be
  identical) plus a new in-memory lazy-store test kit (TestPipe-style).

### B. Shrink the wasm to speed instantiation  [cheap, independent]
Instantiation time scales with module size.
- Measure the current `.wasm` size (built artifact under the worker build dir).
- Levers: `opt-level = "z"`/`"s"` + `lto = true` + `codegen-units = 1` +
  `panic = "abort"` + `strip = true` for the wasm profile; run `wasm-opt -Oz`
  (worker-build pins `worker-build@=0.8.4` per the deploy memory — confirm it
  invokes wasm-opt, else add a post-step); audit for pulled-in-but-unused deps
  (serde_json is the heavy one — the snapshot codec uses it).
- Win: smaller module → faster cold instantiate, on every cold start, for free.
  Independent of A.

## Measurement plan (the interactive step for the user)
1. Deploy: `cd crates/edgestash-cloudflare && env -u CLOUDFLARE_API_TOKEN npx wrangler deploy`
   (per `cloudflare-deploy-blocker` memory).
2. Force cold starts: hit a **fresh** DO id (new key/namespace) repeatedly with a
   gap long enough to evict the hot instance; record first-request vs warm-request
   latency. Vary tenant state size (10 / 1k / 10k keys) to expose the O(state) term.
3. Establish the baseline split (wasm-instantiate vs storage.list vs rebuild) by
   timing inside `fetch` (log `Date.now()` deltas around `load_entries`/`open`).
4. Then implement B (cheap) and re-measure; implement A and re-measure across the
   state-size sweep — A should flatten the curve.

## Status
Analysis complete. Implementation + measurement deferred to an interactive session
(cold-start numbers are not reproducible unattended). Tracked in `CAMPAIGN_BACKLOG.md` Phase 2.
