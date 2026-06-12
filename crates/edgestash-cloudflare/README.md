# EdgeStash Cloudflare Adapter

This crate is the Cloudflare host boundary for the EdgeStash Valdr demo. The
engine is still `valdr-engine`; this crate only wires Worker requests,
Durable Object routing, and Durable Object storage.

## Local Run

```sh
rustup target add wasm32-unknown-unknown
cd crates/edgestash-cloudflare
npx wrangler dev
```

The Wrangler config binds `EDGESTASH` to the `EdgeStashObject` Durable Object
class and uses the SQLite-backed Durable Object migration path.

To validate the build and binding metadata without deploying:

```sh
npx wrangler deploy --dry-run --outdir /tmp/edgestash-cloudflare-build
```

## Smoke Fixture

With `wrangler dev` running on the default port:

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

Or run the checked smoke fixture:

```sh
sh fixtures/smoke.sh
```

The fixture uses a fresh tenant by default so persisted local Durable Object
state from earlier runs cannot affect the limiter decision. Set `TENANT=...` to
target a specific object.
