# EdgeStash

*Redis-compatible atomic state, embedded in the edge.*

EdgeStash runs the Valdr engine, a safe-Rust port of Valkey, compiled to WebAssembly and embedded inside a Cloudflare Worker and Durable Object. You get Redis commands and Lua scripting with atomic, edge-local state, and nothing to call out to. It started as a way to prove a point: Valdr doesn't have to be a server. The same engine can run where the request already is.

[Try the live demo →](https://edgestash-valdr.ianmclaughlin1398.workers.dev)

## The demo

An AI-spend rate limiter. A Lua token bucket decides allow-or-deny for every request, inside the Worker, next to the request. Drain it faster than it refills and you get a 429. The policy (capacity, refill rate) lives in a Valdr hash you can change without touching the script.

## What makes it different

Edge state today is usually one of two things: a remote Redis you call over HTTP, or bespoke code written against a Durable Object. EdgeStash is neither. It embeds a Redis-compatible engine in the runtime, so you keep the Redis and Lua programming model with no network hop and no rewrite.

| Approach | State lives | Programming model |
|---|---|---|
| Hosted Redis SaaS (Upstash) | remote, over HTTP | Redis, via REST |
| Durable Object, hand-rolled | in the worker | custom, per app |
| EdgeStash | in the worker | Redis + Lua, drop-in |

The detail that makes it work: the engine is memory-safe Rust, and the Lua is also Rust (omniLua, no C Lua). That matters because C Lua can't compile to `wasm32-unknown-unknown` (it needs `setjmp`), so it falls back to the heavier emscripten target. A pure-Rust stack hits `wasm32-unknown-unknown` cleanly, which is the target edge runtimes want. The wasm artifact is memory-safe top to bottom:

| Layer | Implementation |
|---|---|
| Command engine | Valdr, safe Rust |
| Lua scripting | omniLua, pure Rust, no C Lua |
| Isolation | WebAssembly sandbox |

## How it works

```
request → Worker → Durable Object → valdr-engine (wasm) → Lua
```

One Durable Object owns one tenant's state and handles requests one at a time, so a multi-step decision (read the policy, refill, spend, persist) runs as a single atomic step. State is written per-key to Durable Object storage and survives cold starts.

## Status

A working prototype, not a product. The single-node Valdr core underneath it passes Valkey's [test suite](coverage.html); EdgeStash itself is differential-tested against a real `valkey-server` (400+ fixtures, zero divergences) and deployed live. Open source under BSD-3.

[Live demo](https://edgestash-valdr.ianmclaughlin1398.workers.dev) · [Source](https://github.com/ianm199/valdr)
