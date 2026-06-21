# fit-bench — a product-fit comparison harness for EdgeStash

The differential oracle (`harness/oracle/valdr-engine-differential.py`) settles
**correctness**: does valdr-engine behave like `valkey-server`. fit-bench settles
**product fit**: for a concrete use case, how does EdgeStash compare to the
alternatives a buyer would otherwise reach for — measured, not asserted.

It mirrors the oracle's discipline. The same use case is driven through every
backend by one harness; some backends *fail* the correctness run, and that failure
is the artifact.

## Phase 1 scope (this directory)

Everything reachable on **one Cloudflare account, no third-party service**:

| Backend | What it is | Expected on inventory |
|---|---|---|
| `edgestash` | valdr-engine (wasm) in a Durable Object — the system under test | correct (atomic Lua + DO serialization) |
| `raw-do` | a Durable Object whose reserve logic is hand-written TypeScript | correct (DO serialization) — the honest "why a Redis engine?" baseline |
| `kv` | a Worker backing the same logic on Workers KV (eventual, non-atomic) | **oversells** — the counter-example |

The lead use case is **flash-sale inventory**, because its invariant — never reserve
more units than exist — is binary and unarguable. `edgestash` and `raw-do` are
*expected to tie* on correctness; including `raw-do` is what keeps the comparison
honest. EdgeStash's edge over a raw Durable Object is **not** correctness here — it
is the Redis API + portability, Lua policy you change without redeploying, and an
oracle. fit-bench measures `raw-do` on every axis *except* correctness, where they
are level.

Out of scope for Phase 1 (needs accounts / a later phase): Upstash, Momento, the
Cloudflare native rate-limit binding.

## Two modes

### `--mode local` — the deterministic inner loop

In-process simulation of each backend's *documented* consistency model: serialized
atomic RMW (Durable Objects, EdgeStash) versus a non-atomic KV get/set. The oversell
**emerges from the model**, not a hardcoded verdict — but the magnitude is the
worst-case lost update and is labelled `MODELED`. Use it to develop the harness and
to show the *direction* of the contrast. It does not produce a publishable number.

```sh
python3 harness/fit-bench/fit_bench.py --mode local --buyers 50 --stock 10
```

### `--mode http` — the oracle

Fire real concurrent HTTP load at deployed backends and measure what actually
happens: reserved/oversold counts plus an interleaved warm-latency probe. This is
the number you cite.

```sh
python3 harness/fit-bench/fit_bench.py --mode http \
  --edgestash-base https://edgestash-valdr.<acct>.workers.dev \
  --kv-base       https://fitbench-kv.<acct>.workers.dev \
  --raw-do-base   https://fitbench-raw-do.<acct>.workers.dev \
  --buyers 50 --stock 10 --latency-samples 40
```

Any subset of `--*-base` flags works; pass only the backends you have deployed.

## Backend contract

`kv` and `raw-do` implement the identical contract so the harness drives them the
same way (`backends/*/src/index.ts`):

```
PUT  /seed?sku=<sku>&stock=<n>          reset a SKU to n units
POST /reserve?sku=<sku>&buyer=<id>      200 {"reserved":k} on a win, 409 {"soldout":true}
GET  /stock?sku=<sku>                   200 {"stock":n}
```

`edgestash` is driven through its existing tenant route layer (SCRIPT LOAD reserve →
SET stock → EVALSHA per buyer), the same flow as
`crates/edgestash-cloudflare/fixtures/demos/inventory.sh`.

## Deploying the Phase-1 backends (your Cloudflare account)

EdgeStash is already deployed (`crates/edgestash-cloudflare`). The two TypeScript
backends here deploy with Wrangler. Run each from its own directory; both unset
`CLOUDFLARE_API_TOKEN` so Wrangler falls back to your `wrangler login` OAuth session
(see `memory/cloudflare-deploy-blocker`).

```sh
# kv backend — create a namespace, paste its id into wrangler.jsonc, deploy
cd harness/fit-bench/backends/kv-worker
npm install
env -u CLOUDFLARE_API_TOKEN npx wrangler kv namespace create STOCK_KV   # prints the id
#   -> set kv_namespaces[0].id in wrangler.jsonc to that id
env -u CLOUDFLARE_API_TOKEN npx wrangler deploy

# raw-do backend — no namespace needed (storage is the DO itself)
cd ../raw-do-worker
npm install
env -u CLOUDFLARE_API_TOKEN npx wrangler deploy
```

Then run `--mode http` with the three deployed URLs.

> The KV race is real but timing-dependent: a low-concurrency burst that all lands in
> one colo with read-your-writes caching can *under*-show the oversell. Push
> `--buyers` up (100+) and re-run a few times; the published number should be the
> observed oversell range, not a single lucky run. This is exactly why the magnitude
> is measured, not modeled.

## What this proves (and doesn't)

- **Proves:** Workers KV oversells under concurrency where EdgeStash (and a raw DO)
  do not — the atomic-edge thesis, as a measured number rather than a claim. And the
  cost shape: a KV reserve pays an expensive KV write per decision.
- **Does not prove:** anything about EdgeStash vs Upstash/Momento (Phase 2), or that
  EdgeStash beats a raw Durable Object on correctness (it ties — by design).

## Files

```
fit_bench.py                     the harness driver (stdlib only; local + http modes)
backends/kv-worker/              Workers KV backend (oversells — the experiment)
backends/raw-do-worker/          raw Durable Object backend (correct — the honest baseline)
```
