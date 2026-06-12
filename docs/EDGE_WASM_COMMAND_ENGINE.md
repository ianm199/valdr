# Edge Wasm Valdr Engine

**Status:** Spike in progress (2026-06-11).
**Goal:** justify a `wasm32-unknown-unknown` Valdr command-engine build with a
useful edge product shape, not a novelty port. The API can be Redis-compatible;
the engine artifact is Valdr.
**Related:** [`MLUA_EXIT_PLAN.md`](MLUA_EXIT_PLAN.md) and the sibling
`../../lua-rs-port/` project.

## The idea

Build **EdgeStash**: an Upstash REST-compatible Valdr command engine that runs
inside an edge runtime, first Cloudflare Workers + Durable Objects, backed by
Valdr compiled to `wasm32-unknown-unknown`.

The narrow demo:

> Drop-in edge rate limiting with Lua scripts, no external Redis service.

This is not a claim that a full Redis TCP server runs unchanged in Workers. The
useful artifact is a portable Redis-compatible Valdr engine that can be embedded
into an edge state container.

## Why this is worth testing

Upstash already proves the demand for connectionless Redis at the edge. Its REST
API accepts Redis commands over HTTP in Redis argument order:

```text
GET /COMMAND/arg1/arg2
POST / ["COMMAND", "arg1", "arg2"]
```

Upstash also has a rate-limit product aimed directly at serverless and edge
runtimes, including Cloudflare Workers, Vercel Edge, Fastly Compute@Edge, and
WebAssembly environments. It supports Lua scripting through `EVAL` / `EVALSHA`.

Sources:

- Upstash REST API: <https://upstash.com/docs/redis/features/restapi>
- Upstash Rate Limit overview:
  <https://upstash.com/docs/redis/sdks/ratelimit-ts/overview>
- Upstash EVAL docs:
  <https://upstash.com/docs/redis/sdks/py/commands/scripts/eval>
- Cloudflare Durable Objects overview:
  <https://developers.cloudflare.com/durable-objects/>

The product wedge is different from hosted Redis:

- Upstash: external serverless Redis endpoint accessed from edge functions.
- EdgeStash: Valdr command engine embedded inside the edge app/state container.

That means the first question is not "can this replace Upstash?" It is:

> Can a useful Upstash-style workflow run without leaving the edge runtime?

## Long-Term Vision

The long-term goal is to make Valdr more than a server port. The durable
artifact should be a portable command engine with Redis-compatible semantics
where they are useful, safe-Rust Lua programmability, and explicit host
capabilities for time, randomness, persistence, logging, and request limits.

The deployment model should stay layered:

```text
valdr-engine
  -> portable command execution, Lua, state, snapshots / journal hooks

edge/native adapters
  -> Worker + Durable Object, local process, browser, or other hosts

storage backends
  -> authoritative hot state where atomicity exists
  -> snapshots / archives / replication logs where object storage fits
```

That keeps the product path open without pretending every backend has the same
consistency model. Durable Objects are a good first authority for tenant-local
atomic state. Object stores such as R2/S3 are better as snapshot, archive, or
replication-log targets unless paired with a real serialization or lease layer.

The important thesis is:

> Valdr should be usable as a server when that is the right shape, but also as
> an embeddable programmable state engine when the request is already executing
> in a constrained host such as an edge runtime.

Near-term work should harden this direction incrementally: split snapshot and
journal storage traits, deploy latency/cold-start measurements, Lua library
parity for `lua-rs-port`, and a broader but still intentional command subset.

## Target user

The likely user is an infra/platform engineer already running application logic
in Cloudflare Workers who needs small, atomic, low-latency state close to the
request.

Concrete workflows:

- API and AI spend rate limiting.
- Per-user / per-tenant quota checks.
- Bot, signup, login, and webhook abuse counters.
- Idempotency keys for edge-handled requests.
- Session revocation and short-lived auth state.
- Room or tenant-local coordination where one Durable Object naturally owns the
  shard.

The differentiator is Lua:

```text
request -> Worker -> Durable Object -> Valdr Wasm engine -> Lua token bucket
```

Scriptability matters because real rate limit and quota policies are not just
`INCR + EXPIRE`. They often need plan-aware limits, burst tokens, remaining
quota, retry-after calculation, idempotency checks, or multi-counter updates in
one atomic decision.

## MVP

### Crate shape

`crates/valdr-engine` is the first portable engine crate with no server/runtime
dependencies:

```rust
pub trait Host {
    fn now_millis(&self) -> u64;
    fn random_bytes(&mut self, out: &mut [u8]) -> Result<(), HostError>;
    fn persist_append(&mut self, record: &[u8]) -> Result<(), HostError>;
}

pub struct Engine<H> {
    host: H,
    // DB, expiries, script cache, config subset.
}

impl<H: Host> Engine<H> {
    pub fn execute(&mut self, argv: &[Vec<u8>]) -> RespFrame;
    pub fn execute_rest(&mut self, request: RestRequest<'_>) -> RestResponse;
}
```

This crate must not depend on:

- `mio`
- `rustls` / `ring`
- `tikv-jemallocator`
- OS networking
- process/thread APIs
- native filesystem APIs

The current Redis workspace fails `wasm32-unknown-unknown` before command logic
because native/default dependencies such as `ring` and `getrandom` are pulled in.
The Wasm project therefore needs a smaller crate boundary, not just feature flags
on the existing server.

### Current boundary evidence

Checkpoint from 2026-06-11:

- `cargo test -p valdr-engine` passes, including the token-bucket fixture.
- `cargo check --target wasm32-unknown-unknown -p valdr-engine` passes.
- `cargo tree -p valdr-engine --target wasm32-unknown-unknown | rg
  'mlua|mlua-sys|ring|rustls|mio|tikv|jemalloc|libc|getrandom'` returns no
  matches, so the portable engine boundary is not accidentally pulling in the
  known native/server dependency chain.
- `valdr-engine` now includes a pure Upstash-style REST adapter: path commands,
  JSON command bodies, JSON `{result}` / `{error}` responses, RESP2 response
  mode, and non-atomic `/pipeline` JSON arrays.
- `valdr-engine` now carries the edge MVP command subset for strings,
  expiries, scripts, and basic hashes: `GET`, `SET`, `SETEX`, `DEL`, `EXISTS`,
  `INCR`, `INCRBY`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `HGET`, `HSET`,
  `HGETALL`, `HDEL`, `EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, and
  `SCRIPT FLUSH`.
- `valdr-engine::tests::rest_adapter_runs_hash_policy_token_bucket_fixture`
  proves the product-shaped limiter case: tenant policy is stored in a hash,
  Lua reads it with `HGET`, state mutates through `GET` / `SET PX`, and a
  policy upgrade changes later decisions without changing the script.
- `cargo test -p edgestash-demo` passes. This crate models the Worker/Durable
  Object shape without a provider SDK: stable tenant-to-shard routing, one hot
  `valdr-engine` instance per shard, policy bootstrap through the REST adapter,
  and a Lua `EVALSHA` limiter decision.
- `cargo check --target wasm32-unknown-unknown -p edgestash-demo` passes, so the
  Worker-shaped wrapper does not reintroduce native runtime dependencies.
- `valdr-engine` exposes a wasm-safe JSON snapshot/import API for string and
  hash state with absolute key expiries. `edgestash-demo::EdgeShard` uses it to
  model Durable Object cold-start restore: state survives, the script cache is
  hot-only, and the limiter reloads its script before continuing decisions.
- `edgestash-demo::EdgeObject<S>` binds `EdgeShard` to an explicit
  `ObjectStorage` trait. The in-memory test storage models Durable Object
  `storage.get` / `storage.put` without introducing a provider SDK or native
  dependencies.
- `edgestash-demo` now includes a provider-neutral HTTP route layer:
  `PUT /v1/policy/{tenant}`, `POST /v1/limit/{tenant}`,
  `POST /v1/ai/{tenant}`, and `/v1/valdr/{tenant}/...` raw Upstash-style
  command pass-through. It compiles to `wasm32-unknown-unknown` with the rest of
  the demo.
- `crates/edgestash-cloudflare` now adds the first real Cloudflare host
  adapter. The Worker `fetch` handler routes each tenant to a Durable Object,
  the Durable Object restores the `valdr-engine` snapshot from
  `worker::durable::Storage`, runs the provider-neutral `EdgeObject` HTTP
  layer, and writes the updated snapshot back to storage.
- `crates/edgestash-cloudflare/wrangler.jsonc` is now a runnable Workers
  config: `main = build/index.js`, a `worker-build` release build command, an
  `EDGESTASH` Durable Object binding, and a SQLite-backed
  `new_sqlite_classes` migration for `EdgeStashObject`.
- `cargo test -p edgestash-cloudflare` passes for the adapter's route parsing
  coverage.
- `cargo check --target wasm32-unknown-unknown -p edgestash-cloudflare` passes.
- `npx wrangler deploy --dry-run --outdir /tmp/edgestash-cloudflare-build`
  passes from `crates/edgestash-cloudflare`, producing a 1600.99 KiB upload
  bundle / 572.58 KiB gzip bundle and confirming the `env.EDGESTASH
  (EdgeStashObject)` Durable Object binding.
- `npx wrangler dev --ip 127.0.0.1 --port 8787` plus
  `sh fixtures/smoke.sh` passes from `crates/edgestash-cloudflare`. The smoke
  installs tenant policy, runs two Lua limiter decisions through the Worker and
  Durable Object, exercises the `/v1/ai/{tenant}` toy API-spend route, and
  verifies tenant-scoped `/v1/valdr` command pass-through. The fixture creates a
  fresh tenant by default because local Durable Object storage persists across
  dev runs.
- `cargo check --target wasm32-unknown-unknown -p redis-types -p redis-protocol`
  passes.
- `cargo check --target wasm32-unknown-unknown -p redis-ds` passes after making
  the listpack backlen ceiling comparison width-explicit.
- `cargo check --target wasm32-unknown-unknown -p lua-wasm` passes in
  `../../lua-rs-port/`.
- `cargo check --target wasm32-unknown-unknown -p redis-commands --features
  lua-rs-engine` still fails before command logic; that legacy crate also still
  compiles `mlua-sys` because Redis Functions remain mlua-backed there.

The remaining native chain in the old full command stack is:

```text
redis-commands
  -> redis-core
    -> rustls
      -> ring
        -> getrandom
```

So the first extraction line is not "make `redis-commands` Wasm." It is:

> split the data/protocol/command-execution pieces from server transport, TLS,
> persistence workers, and process/thread modules.

The lower crates and `valdr-engine` now support the Wasm story. The wrong part
is that the current `redis-commands` crate inherits the full server core and
still carries mlua for the Function path.

`crates/edgestash-demo` is the first local Worker-shaped consumer. It is not a
Cloudflare deployment; it is a provider-neutral harness that keeps the important
runtime shape visible in code:

```text
EdgeWorker
  -> stable tenant shard index
  -> EdgeShard
  -> valdr-engine REST adapter
  -> SCRIPT LOAD / EVALSHA limiter script
```

### Cloudflare shape

One Durable Object owns one engine instance:

```text
Worker request
  -> route to Durable Object id
  -> parse Upstash-style REST command
  -> Engine::execute(argv)
  -> encode JSON / RESP-like response
```

Initial routes:

```text
POST /                 ["SET", "foo", "bar"]
GET  /GET/foo
POST /pipeline         [["INCR", "k"], ["EXPIRE", "k", "60"]]
POST /                 ["EVAL", "...lua...", "1", "key", "arg"]
POST /                 ["SCRIPT", "LOAD", "...lua..."]
```

Pipeline support should be explicit but non-atomic by default, matching common
REST pipeline expectations. Atomicity should come from one command, `MULTI` in a
later phase, or Lua.

Current adapter status:

- `GET /COMMAND/arg1/...` is parsed into command argv.
- `POST /` accepts a JSON command array.
- `POST /pipeline` accepts a two-dimensional JSON command array and returns one
  JSON result/error object per command.
- `POST /COMMAND/...` appends the raw body as the final command argument, with
  query arguments appended after it, matching the common `SET key <body> ?EX=...`
  shape.
- `RestResponseFormat::Resp2` returns raw RESP2 bytes for single commands.

### Command subset

Implemented in `valdr-engine` for the rate-limit/idempotency/session demo:

```text
GET SET SETEX DEL EXISTS
INCR INCRBY
EXPIRE PEXPIRE TTL PTTL
HGET HSET HGETALL HDEL
EVAL EVALSHA SCRIPT LOAD SCRIPT EXISTS SCRIPT FLUSH
```

Nice-to-have next:

```text
ZADD ZREM ZCARD ZCOUNT ZRANGE ZREMRANGEBYSCORE
SADD SREM SISMEMBER SCARD
```

Do not start with streams, pub/sub, blocking commands, replication, cluster, RDB,
AOF, or native RESP/TCP. Those do not prove the edge command-engine thesis.

## Demo application

Build an "AI API spend limiter at the edge."

Inputs:

- API key / user id.
- Plan: free, pro, enterprise.
- Token estimate for this request.
- Current time.

Lua script output:

```json
{
  "allowed": true,
  "remaining": 8421,
  "reset_ms": 1710000000000,
  "retry_after_ms": 0
}
```

Workflow:

```text
1. Request hits Worker.
2. Worker hashes API key to a Durable Object.
3. Durable Object calls EVAL against local Wasm Valdr engine.
4. Script atomically updates token bucket and monthly/day counters.
5. Worker allows, rejects, or annotates request before forwarding.
```

Success criteria:

- The same policy can be implemented against Upstash Redis and EdgeStash by
  changing only the Redis client endpoint/adapter.
- The Lua script returns the same allow/deny/remaining/reset decisions for a
  deterministic fixture set.
- The engine builds for `wasm32-unknown-unknown`.
- No C Lua, no native networking, no host filesystem, no process APIs.
- The demo can run locally in a Workers-like test harness and in a real Worker.

Native proof already started:

- `lua_rs_evalsha_runs_stateful_token_bucket_fixture` loads a Lua token-bucket
  script with `SCRIPT LOAD`, executes it repeatedly with `EVALSHA`, mutates
  state through `GET` / `SET PX`, and verifies deterministic
  `allowed` / `remaining` / `reset_ms` / `retry_after_ms` decisions through the
  `lua-rs-engine` backend.
- `lua_rs_evalsha_reads_hash_policy_for_token_bucket_fixture` runs the same
  class of workload on the native `redis-commands` lua-rs backend, but reads
  tenant policy from a Redis hash through `redis.call('HGET', ...)`.
- `valdr-engine::tests::evalsha_runs_stateful_token_bucket_fixture` runs the
  same policy shape against the Wasm-safe engine boundary.
- `valdr-engine::tests::rest_adapter_runs_token_bucket_fixture` runs the same
  policy through the REST adapter using `SCRIPT LOAD` and `EVALSHA` JSON command
  bodies.
- `valdr-engine::tests::rest_adapter_runs_hash_policy_token_bucket_fixture`
  runs the hash-backed tenant-policy variant through the REST adapter.
- `valdr-engine::tests::rest_pipeline_is_ordered_and_non_atomic` verifies the
  `/pipeline` response shape.
- `edgestash-demo::tests::worker_routes_tenant_to_stable_shard_and_runs_limiter`
  runs the same tenant-policy limiter through a Worker-shaped router and shard.
- `edgestash-demo::tests::tenants_are_isolated_even_when_sharing_a_worker`
  verifies tenant-local state isolation across shared Worker routing.
- `edgestash-demo::tests::worker_can_still_expose_raw_upstash_style_rest_on_the_tenant_shard`
  verifies that the Worker-shaped wrapper can still expose raw REST commands on
  the selected shard.
- `edgestash-demo::tests::shard_snapshot_restore_preserves_limiter_state_after_cold_start`
  snapshots one shard, restores it into a fresh shard, reloads the Lua script,
  and verifies the next limiter decision continues from persisted bucket state.
- `edgestash-demo::tests::edge_object_storage_binding_persists_limiter_state_across_reopen`
  persists snapshots through the `ObjectStorage` trait and reopens the object
  before making the next limiter decision.
- `edgestash-demo::tests::edge_object_rest_commands_persist_across_reopen`
  verifies raw REST commands also survive object reopen through the same storage
  binding.
- `edgestash-demo::tests::http_policy_and_limit_routes_persist_across_reopen`
  exercises the Worker-facing policy and limiter HTTP routes through storage
  reopen.
- `edgestash-demo::tests::http_raw_valdr_route_uses_upstash_shape_and_storage_binding`
  verifies tenant-scoped raw Upstash-style HTTP command routing and persistence.
- `edgestash-demo::tests::http_routes_return_explicit_errors_for_bad_requests`
  checks route-level method/body validation errors.

## What this proves

If the demo works, we have shown:

- Valdr can be split into a portable command engine independent of the TCP
  server.
- `lua-rs-port` can run meaningful Redis scripts in Wasm.
- Durable Objects can act as Redis shard containers for edge-local atomic state.
- Upstash-style workflows can run without an external Redis service for a useful
  subset.
- The pure-Rust Lua migration has a concrete product reason beyond "remove C."

## Risks and open questions

- **Cloudflare deployment:** the `edgestash-cloudflare` adapter now compiles to
  `wasm32-unknown-unknown`, maps `worker::Request` / `worker::Response`, and
  uses Durable Object `storage.put` / `storage.get`. Wrangler dry-run build and
  binding validation pass, and local `wrangler dev` traffic passes the limiter
  smoke fixture. It still needs a deployed latency/cold-start measurement.
- **Persistence policy:** the demo snapshots after each mutating route. A
  production version still needs a decision on snapshot-every-command versus an
  append-log durability scheme.
- **Sharding:** Multi-key atomicity only exists inside one Durable Object. The
  API should require hash tags or an explicit shard key.
- **Latency:** Durable Object placement and cold starts may erase the benefit for
  some users. The demo must measure this.
- **Compatibility:** Upstash REST compatibility is a convenience target, not a
  license to claim complete Upstash behavior.
- **Lua parity:** `lua-rs-port` still needs Redis Lua 5.1 conformance and the
  Redis-injected libraries (`cjson`, `cmsgpack`, `struct`, `bit`).
- **Wasm host APIs:** time, random, persistence, logging, and request limits must
  be explicit host capabilities.

## Implementation milestones

1. **Engine boundary audit**
   Identify the smallest set of crates needed for DB, expiry, protocol values,
   command dispatch, and scripting. Record every non-Wasm dependency that leaks
   in.

2. **`valdr-engine` crate**
   Move or wrap command execution behind an `Engine::execute(argv)` interface.
   Native tests should execute the same command fixtures without the TCP server.

3. **Wasm compile target**
   Make `cargo check --target wasm32-unknown-unknown -p valdr-engine` pass with
   scripting enabled through `lua-rs-engine`.

4. **Upstash-style REST adapter**
   Add path/body parsing and JSON response encoding in a small Worker-facing
   layer.

5. **Worker-shaped prototype**
   `crates/edgestash-demo` keeps hot state in memory, routes tenants to stable
   shards, installs tenant policy, and runs the Lua limiter through the Valdr
   REST adapter.

6. **Durable Object persistence**
   `valdr-engine` can export/import a JSON snapshot and `edgestash-demo` proves
   cold-start restore of limiter state. `EdgeObject<S>` now binds those bytes to
   an explicit `ObjectStorage` trait. `edgestash-cloudflare` maps that shape to
   Cloudflare Durable Object storage for the first wasm-checked adapter. Next:
   decide whether to snapshot every command or append log records.

7. **Worker HTTP adapter**
   `edgestash-demo` exposes provider-neutral HTTP-shaped routes for policy
   install, limiter checks, and tenant-scoped raw Upstash-style command
   pass-through. `edgestash-cloudflare` translates actual Cloudflare Worker
   requests/responses to this route layer. `wrangler deploy --dry-run` builds
   the Workers bundle and validates the Durable Object binding, and the smoke
   fixture passes under local `wrangler dev`. Next: deploy and measure latency
   / cold-start behavior.

8. **Rate limiter fixture suite**
   Run deterministic fixtures against both Upstash Redis and EdgeStash. Compare
   decisions and response shapes.

9. **Public demo**
   Deploy a Worker that protects a fake AI endpoint and exposes a dashboard with
   remaining quota, reset time, and script source.

## Non-goals

- Full Redis server in Wasm.
- Inbound TCP compatibility.
- Redis Cluster compatibility.
- General-purpose globally replicated Redis.
- Exact Upstash product clone.
- Replacing managed Redis for existing backend apps.

The useful first artifact is smaller and sharper: Redis-style atomic edge state
with Lua scripts, embedded where the request is already executing.
