# EdgeStash — working in this subsystem

EdgeStash is **Valdr's command engine (`valdr-engine`) compiled to
`wasm32-unknown-unknown` and run inside a Cloudflare Durable Object** — Redis-style
atomic state at the edge, with Lua scripts, no external Redis service. It is *not* a
port of the Redis TCP server to Workers; it is the proof that `valdr-engine` is a
reusable embeddable engine, not just the guts of `redis-server`.

> The narrow product: **drop-in edge rate limiting with Lua scripts, no external
> Redis.** An Upstash-REST-compatible surface; the engine artifact is Valdr.

This file is the operational "how to work here" guide for the whole EdgeStash lane.
Authoritative design + dated status log:
[`../../docs/EDGE_WASM_COMMAND_ENGINE.md`](../../docs/EDGE_WASM_COMMAND_ENGINE.md).
Engine internals + the wasm-safety invariant:
[`../valdr-engine/CLAUDE.md`](../valdr-engine/CLAUDE.md).

## Status (live as of 2026-06-13)

- **Deployed:** `https://edgestash-valdr.ianmclaughlin1398.workers.dev`. Worker
  startup ~2 ms; bundle ~1.65 MiB / ~591 KiB gzip.
- **Measured (single off-edge client):** cold Durable Object start **~0.5 s**
  (new DO + per-key restore + wasm init); warm round-trip **~66 ms p50 / ~103 ms
  p90** — of which the engine is ~2 ms, the rest is client→edge RTT.
- **Oracle:** differential vs pinned `valkey-server` — **352 fixtures, 333 pass,
  0 divergences, 19 known-unsupported** (`--strict` exits 0).
- **Phase:** spike hardening. Engine boundary, Cloudflare adapter, per-key
  persistence, host-time authority, deploy + measurement all landed. **The open
  target is cold-start cost** (wasm size, lazy script reload, restore cost).

## The four crates (all workspace members of valdr)

| Crate | LOC | Role |
|---|---|---|
| `valdr-engine` | ~19k | The portable, wasm-safe command engine. 175+ commands, Lua via omnilua, JSON snapshot + per-key export, `command_keys` static key analysis. **wasm-safety is an invariant — see its CLAUDE.md.** |
| `edgestash-demo` | ~1.8k | Provider-neutral HTTP layer (`/v1/...`), the Lua token-bucket limiter, the `ObjectStorage` trait, and the four demo workflows. No Cloudflare SDK — compiles to wasm on its own. |
| `edgestash-cloudflare` | ~400 | **This crate.** The real Cloudflare host: Worker `fetch` → tenant→Durable Object routing → hot engine → per-key lazy load + flush. `cdylib`. |
| `valdr-fixture-runner` | ~115 | JSONL stdin/stdout driver exposing the engine to the differential oracle. |

## Architecture (the request path)

```
RESP/REST client
  → Worker fetch (src/lib.rs)             tenant parsed from /v1/.../{tenant}
  → EdgeStashObject (Durable Object)      one hot EdgeObject<Storage> per tenant
      → prefetch_for: command_keys(req) → load ONLY touched keys from DO storage
      → EdgeObject HTTP layer (edgestash-demo)
          → valdr-engine Engine::execute / execute_rest
              → Lua limiter via EVALSHA (omnilua)
      → drain_flush: flush ONLY dirty keys back to DO storage (iff mutation_epoch moved)
```

**Per-key persistence (superseded snapshot-per-mutation):** each Redis key lives
under its own SQLite-backed DO value `k:<hex(key)>`. Cold start loads lazily —
`command_keys(argv)` is O(touched), not O(state), so a single-key GET against a
50k-key tenant fetches one value, not the whole keyspace. A read-only request does
**zero** storage writes. **120 KiB per-value cap** (`edgestash_demo::MAX_VALUE_BYTES`,
under CF's 128 KiB DO-value limit): a mutating request whose changed value would
exceed it returns **507** and the hot instance is rebuilt from storage (rollback),
so a tenant cannot wedge its object unpersistable. Total tenant state is bounded
only by DO storage.

**Time authority:** the Worker clock (`Date::now()`) is authoritative for every
route, including raw `/v1/valdr` expiries. Client-supplied `now_millis` is rejected
with **400** unless the dev-only `EDGESTASH_ALLOW_CLIENT_TIME=true` is set —
**never set that in production** (a client that controls the clock refills its own
rate-limit buckets).

## Route table (`src/lib.rs`)

| Path | Method | Does |
|---|---|---|
| `/` , `/dashboard` | GET | Static `assets/dashboard.html` — live quota/reset/script viewer |
| `/script` | GET | The Lua limiter source (`edgestash_demo::LIMITER_SCRIPT`) |
| `/v1/policy/{tenant}` | PUT/POST | Install/update tenant limit policy (capacity / refill_tokens / refill_ms / ttl_ms) |
| `/v1/limit/{tenant}` | POST | Token-bucket decision via Lua `EVALSHA` |
| `/v1/ai/{tenant}` | POST | Toy AI spend-guard (same limiter); `429` + state when denied |
| `/v1/valdr/{tenant}/CMD/arg…` | GET/POST/PUT | Raw Upstash-style command pass-through, tenant-scoped |
| `/v1/_debug/{tenant}` | GET | **dev-only** keyspace dump (engine snapshot JSON, `FullKeyspace`); 403 unless `EDGESTASH_ALLOW_DEBUG=true`. Read-only — writes nothing. |

## The iteration ladder (EdgeStash-specific)

The parent CLAUDE.md "climb the cheapest rung" doctrine, applied to EdgeStash.
**`wrangler dev` is the integration gate, not the inner loop** — most development
happens three rungs below it, because `edgestash-demo` deliberately models the
Worker/DO shape (the `ObjectStorage` trait + in-memory mocks) so routing, lazy-load,
and persistence semantics are testable with zero Cloudflare. Push behavior *into*
that provider-neutral core; the more that lives there, the less wrangler's fidelity
gaps cost you.

| Tier | What | Cost | When |
|---|---|---|---|
| 1 | `cargo check --target wasm32-unknown-unknown -p edgestash-cloudflare` | <2s | does the wasm boundary still hold? (the load-bearing invariant) |
| 2 | `cargo test -p valdr-engine` (+ `lazy_loader_kit`) | ms | engine behavior; run the parity kit after touching `command_keys` |
| 3 | `cargo test -p edgestash-demo` | ms | **the inner loop you develop against** — the Worker/DO shape (routing, per-key lazy load, persistence) in-memory and deterministic via `RecordingStorage`, no wrangler. A new `/v1/*` route and its `http_request_key_access` get proven here. |
| 4 | `python3 harness/oracle/valdr-engine-differential.py --strict` | tens of s | correctness bar: engine vs real `valkey-server`. The truth-teller for command semantics. |
| 5 | `npx wrangler dev …` + `sh fixtures/smoke*.sh` | ~15–40s | **integration gate**: does the real Cloudflare wiring work — workerd, DO, SQLite, time mode, bindings? NOT where you iterate logic. |
| 6 | `npx wrangler deploy --dry-run --outdir …` | ~20s | bundle + binding validation without deploying |
| 7 | `npx wrangler deploy` + measure | minutes | **the only place the physics is real** — cold-start, hibernation/sleep durability, placement, network latency, quota. wrangler dev reproduces none of these (no real sleep, R2 = local disk, no RTT). |

The trap: rungs 1–5 all go green while the things that actually make EdgeStash *hard*
— cold-start cost (~0.5 s, wasm-size-bound) and durability-on-sleep — live only at
rung 7. When you work that frontier the loop is **deploy-and-measure** (or a
purpose-built measurement harness), not `wrangler dev`. Per-binding `remote: true` is
the one way to buy a slice of realism locally (one binding → real R2/D1 with real
latency) without going fully remote.

## Build & run (the commands for each rung)

```bash
# rung 1 — does the wasm boundary still hold? (fastest real signal)
cargo check --target wasm32-unknown-unknown -p edgestash-cloudflare

# rung 2/3 — engine + demo unit tests (in-memory, deterministic, ms)
cargo test -p valdr-engine
cargo test -p edgestash-demo

# build the Worker bundle (worker-build already installed; pinned =0.8.4)
cd crates/edgestash-cloudflare && worker-build --release        # -> build/index.js

# run it locally (npx downloads wrangler; node already present)
npx wrangler dev --ip 127.0.0.1 --port 8787 --var EDGESTASH_ALLOW_DEBUG:true        # USE THIS for the dashboard: production time + live Keyspace panel
npx wrangler dev --ip 127.0.0.1 --port 8787                                         # production time, no debug panel
npx wrangler dev --ip 127.0.0.1 --port 8787 --var EDGESTASH_ALLOW_CLIENT_TIME:true  # deterministic fixtures ONLY (smoke.sh)

# prove it serves
sh fixtures/smoke.sh          # deterministic decisions (needs the var)
sh fixtures/smoke-secure.sh   # production default: body time rejected, real-clock drain/refill
sh fixtures/dogfood.sh        # the clock-independent dogfood scenarios over real Worker HTTP

# deploy / validate
npx wrangler deploy --dry-run --outdir /tmp/edgestash-cloudflare-build
npx wrangler deploy
```

### Inspecting local state (and the Local Explorer caveat)

**The Local Explorer cannot introspect this DO.** Its Durable Object browser talks
to the DO over JS-native RPC, which requires the class to `extends DurableObject`
(`cloudflare:workers`). The Rust `worker` crate (0.8.4) emits a **fetch-style** DO,
so the Explorer fails with *"receiving Durable Object does not support RPC …"*. Its
KV/R2/D1 views still work, but EdgeStash binds **only** a DO, so the Explorer shows
nothing useful here. The Worker→DO data path (`stub.fetch`) is unaffected — this is
purely an Explorer↔workers-rs interop gap, not a bug.

To actually see the keyspace, read the local SQLite — one file per tenant DO under
`.wrangler/state/v3/do/edgestash-valdr-EdgeStashObject/<id>.sqlite`, table `_cf_KV`,
keys `k:<hex(redis_key)>` (the per-key model; any `valdr-engine-snapshot-v1` rows
are stale pre-Phase-2 leftovers — local state accumulates across `dev` runs):
```bash
f=.wrangler/state/v3/do/edgestash-valdr-EdgeStashObject/<id>.sqlite
sqlite3 "$f" "SELECT name FROM __miniflare_do_name"      # which tenant this DO is
sqlite3 "$f" "SELECT key, length(value) FROM _cf_KV"     # its keyspace (hex-decode k:…)
```
The cleanest view is the built-in debug route: `GET /v1/_debug/<tenant>` returns the
engine's whole-DB snapshot JSON (`{"format":"valdr-engine-snapshot","version":1,
"keys":[{key,type,value|fields,expire_at_ms}]}`, hex-encoded keys/values). It is
gated behind `EDGESTASH_ALLOW_DEBUG=true` (off by default, so a deploy never exposes
tenant keyspaces) and is read-only (writes nothing). The dashboard at `/` renders it
as a live **Keyspace** panel. Enable it locally with
`npx wrangler dev --var EDGESTASH_ALLOW_DEBUG:true`. Route + gate live in
`edgestash-demo` (`handle_http` / `http_request_key_access`) and the adapter
(`debug_allowed`); covered by `tests/debug_keyspace.rs`. Reset local state with
`rm -rf .wrangler/state`.

## The oracle (the bar — build success is NOT the bar)

`../../harness/oracle/valdr-engine-differential.py` diffs `valdr-engine` (via
`valdr-fixture-runner`) against the pinned reference `valkey-server` over RESP2,
byte-for-byte, with per-fixture tolerance modes (`exact`, `ttl_band`,
`error_prefix`, `set_equal` for the engine's deterministic hash-field sort,
`scan_reply`). Fixtures: `../../harness/oracle/valdr-fixtures/*.jsonl`.

```bash
cargo build -p valdr-fixture-runner --release
cd harness/oracle && python3 valdr-engine-differential.py --strict   # needs valkey-server on PATH
```

This is **independent of** the native-server Tcl suite (`harness/oracle/tcl-survey.py`).
EdgeStash has its own bar because it ships a different artifact.

## Gotchas

- **wasm-safety is load-bearing.** `valdr-engine` must never pull
  `mlua`/`ring`/`rustls`/`mio`/`getrandom`/OS-net/threads/fs. A dep that drags one
  in breaks the whole product. Guard:
  `cargo tree -p valdr-engine --target wasm32-unknown-unknown | rg 'mlua|ring|rustls|mio|getrandom|libc|jemalloc'`
  must print nothing.
- **`worker-build` pinned `=0.8.4`** — 0.8.5 passes `--force-enable-abort-handler`
  to the wasm-bindgen CLI our lockfile pins (0.2.123), which rejects it.
- **Multi-key atomicity exists only inside one Durable Object.** No cross-tenant /
  cross-shard atomic ops; the API requires a tenant/shard key.
- **Lua parity rides on the sibling `lua-rs-port` (omnilua).** A differential run
  once surfaced an omnilua GC use-after-sweep (`lua-gc/src/heap.rs:842`) on errors
  raised through `lua.scope`; valdr-engine wraps user scripts in a Lua `pcall`
  harness as mitigation — keep it until the lua-rs-port fix lands.

(No PORT STATUS trailer here — that convention is for `.rs` files. Back to the repo
guide: [`../../CLAUDE.md`](../../CLAUDE.md).)
