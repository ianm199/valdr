# EdgeStash

*Redis-compatible atomic state, inside your Cloudflare Worker.*

EdgeStash runs the Valdr engine, a safe-Rust port of Valkey, compiled to WebAssembly and embedded inside a Cloudflare Worker and Durable Object. You get Redis commands and Lua scripting with atomic, edge-local state, and nothing to call out to. It started as a way to prove a point: Valdr doesn't have to be a server. The same engine runs where the request already is.

If you already run on Cloudflare, there's nothing new to adopt. EdgeStash is a Worker and a Durable Object in your own account: the state never leaves Cloudflare, and there's no separate service to provision, secure, or pay for.

[Try the live demo →](https://edgestash-valdr.ianmclaughlin1398.workers.dev)

## The demo

An AI-spend rate limiter. A Lua token bucket decides allow-or-deny for every request, in the Worker, next to the request. Drain it faster than it refills and you get a 429. The policy (capacity, refill rate) lives in a Valdr hash you can change without touching the script.

## What makes it different

Edge state usually means one of two things: a cache like Workers KV, or a remote Redis you call over HTTP. EdgeStash is a third option, a Redis-compatible engine that runs in the Worker itself.

| Option | Consistency | Programmable | Where it runs |
|---|---|---|---|
| CDN / Cache API | HTTP cache | no | the edge |
| Workers KV | eventual | get/set only | the edge, replicated |
| Upstash / Momento | strong | Lua (Upstash) | a separate service |
| Durable Object (raw) | strong, per shard | yes, hand-written | the Worker |
| **EdgeStash** | strong, per shard | Redis + Lua, portable | the Worker |

Caches are eventually consistent. That is fine for content, and wrong for a counter or a limit that has to hold under concurrent writes. A remote Redis like Upstash is atomic and scriptable too, but it is a separate service behind an HTTP call. EdgeStash keeps the same state in the Worker. A raw Durable Object gives you that same atomic state; EdgeStash adds the Redis API and Lua on top, so the policy is data you change without a redeploy, and the same scripts run against any real Redis.

Both layers are Rust: the engine (Valdr) and the Lua (omniLua, not C Lua). That matters for WebAssembly. C Lua can't target `wasm32-unknown-unknown` because it needs `setjmp`, so it falls back to the heavier emscripten target. A pure-Rust stack compiles to `wasm32-unknown-unknown` directly, which is the target edge runtimes want, and it stays memory-safe top to bottom:

| Layer | Implementation |
|---|---|
| Command engine | Valdr, safe Rust |
| Lua scripting | omniLua, pure Rust, no C Lua |
| Isolation | WebAssembly sandbox |

## What it's for

Atomic state next to the request. Good fits:

- Rate limiting and quotas, per user, tenant, or API key; AI-spend caps.
- Idempotency keys, so a webhook or payment runs exactly once.
- Counters and budgets: usage meters, credits, votes.
- Short-lived auth state: OTP attempt limits, session revocation, one-time tokens.
- Locks and coordination, where one shard owns the resource.
- Small per-room or per-tenant state: leaderboards, matchmaking, game rooms.

## How it works

```
   request
   │
   ▼
   edge worker  →  Durable Object  (one per tenant)
                   │
                   ▼
                   valdr-engine (wasm) + Lua
                   read → decide → persist    ← one atomic step
                   │
                   ▼
   allow → forward to origin   ·   deny → 429
```

Cloudflare runs one Durable Object per tenant and delivers its requests one at a time, so a multi-step decision (read the policy, refill, spend, persist) runs as a single atomic step, and concurrent callers can't race it. That serialization and the durable storage are the Durable Object's; EdgeStash is the Redis engine and Lua that run inside it. State is written per key and survives restarts.

A warm object answers in single-digit milliseconds. A tenant with no recent traffic wakes its object in about half a second, then stays warm.

## Run it yourself

A Worker and one Durable Object, deployed to your own Cloudflare account — no Rust toolchain to install. The [edgestash repo](https://github.com/ianm199/edgestash) ships the prebuilt WebAssembly; the button forks it to your GitHub and deploys it.

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/ianm199/edgestash)

Or clone it and run `npx wrangler deploy`.

## Status

A working prototype, not a product. The single-node Valdr core underneath it passes Valkey's [test suite](coverage.html); EdgeStash itself is differential-tested against a real `valkey-server` (400+ fixtures, zero divergences) and deployed live. Open source under BSD-3.

[Live demo](https://edgestash-valdr.ianmclaughlin1398.workers.dev) · [Source](https://github.com/ianm199/valdr)
