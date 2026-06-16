# EdgeStash Cloudflare Adapter

This crate is the Cloudflare host boundary for the EdgeStash Valdr demo. The
engine is still `valdr-engine`; this crate only wires Worker requests,
Durable Object routing, and Durable Object storage.

Each Durable Object keeps one hot engine instance across requests: the
snapshot is restored from storage once, requests execute against the hot
engine, and the snapshot is written back only when the engine's mutation
epoch changed. A failed write discards the hot instance so storage stays
authoritative. One exported snapshot is capped at
`edgestash_demo::MAX_SNAPSHOT_BYTES` (120 KiB, under the 128 KiB Durable
Object value limit); a mutating request that would push past the cap gets
`507` and is rolled back to the last persisted snapshot.

## Time authority

The Workers runtime clock (`Date::now()`) is authoritative for every route,
including raw `/v1/valdr` command expiries. Client-supplied `now_millis` in
limit/AI bodies is rejected with `400` unless the dev-only
`EDGESTASH_ALLOW_CLIENT_TIME` var is `"true"`. Never set that var on a real
deployment — a client that controls the clock can refill its own rate-limit
buckets.

## Local Run

```sh
rustup target add wasm32-unknown-unknown
cd crates/edgestash-cloudflare
npx wrangler dev                                          # production time mode
npx wrangler dev --var EDGESTASH_ALLOW_CLIENT_TIME:true   # deterministic fixtures
```

The Wrangler config binds `EDGESTASH` to the `EdgeStashObject` Durable Object
class and uses the SQLite-backed Durable Object migration path.

To validate the build and binding metadata without deploying:

```sh
npx wrangler deploy --dry-run --outdir /tmp/edgestash-cloudflare-build
```

## Smoke Fixtures

Two checked fixtures, one per time mode:

```sh
sh fixtures/smoke.sh           # needs --var EDGESTASH_ALLOW_CLIENT_TIME:true
sh fixtures/smoke-secure.sh    # needs plain `npx wrangler dev`
```

`smoke.sh` drives the limiter with client-supplied `now_millis` and asserts
exact decisions. `smoke-secure.sh` asserts the production default: body time
is rejected, and the limiter drains, refills, and expires sessions on the
real Worker clock. Both use a fresh tenant by default so persisted local
Durable Object state from earlier runs cannot affect decisions; set
`TENANT=...` to target a specific object.

With `wrangler dev --var EDGESTASH_ALLOW_CLIENT_TIME:true` running, the
manual version of the deterministic flow:

```sh
BASE=http://127.0.0.1:8787

curl -sS -X PUT "$BASE/v1/policy/tenant-42" \
  -H "content-type: application/json" \
  --data '{"capacity":10,"refill_tokens":5,"refill_ms":1000,"ttl_ms":60000}'

curl -sS -X POST "$BASE/v1/limit/tenant-42" \
  -H "content-type: application/json" \
  --data '{"now_millis":1000,"cost":7}'

curl -sS -X POST "$BASE/v1/limit/tenant-42" \
  -H "content-type: application/json" \
  --data '{"now_millis":1100,"cost":7}'
```

Expected limiter decisions:

```json
{"allowed":true,"capacity":10,"remaining":3,"reset_ms":2400,"retry_after_ms":0}
{"allowed":false,"capacity":10,"remaining":3,"reset_ms":2400,"retry_after_ms":700}
```

In production time mode the same routes take `{"cost":7}` with no
`now_millis` and decide on the Worker clock.

The toy AI route uses the same Lua limiter as an API spend guard:

```sh
curl -sS -X POST "$BASE/v1/ai/tenant-42" \
  -H "content-type: application/json" \
  --data '{"now_millis":2000,"tokens":3,"prompt":"summarize invoices"}'
```

When allowed, it returns a deterministic fake completion plus the updated
remaining-token decision. When denied, it returns `429` with the limiter state.

Raw Valdr command pass-through is tenant-scoped:

```sh
curl -sS "$BASE/v1/valdr/tenant-42/SET/raw%2Fkey/hello%20edge"
curl -sS "$BASE/v1/valdr/tenant-42/GET/raw%2Fkey"
```
