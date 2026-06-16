[← Valdr blog](blog.html)

# Valdr 1.0: a memory-safe Valkey, now running in WebAssembly

*2026-06-16*

Valdr is a reimplementation of Valkey in Rust. The single-node core now passes Valkey's own test suite, so we're calling it stable. This post covers that, plus the first thing we've built on it that isn't a server.

## What 1.0 covers

Single-node only, and we mean only:

- Passes Valkey's own suite: 3,035 of 3,035 counted assertions. We run it as an oracle, so a one-byte disagreement is a bug.
- Real traffic over TLS, on rustls. No OpenSSL, no `fork()`.
- At parity with or faster than Valkey on most common commands. [Numbers.](index.html)
- Mostly safe Rust, with a small `unsafe` budget capped in CI.

No replication, Sentinel, or Cluster yet. That's the [roadmap](roadmap.html), and it gets its own test gates before we claim it.

## The engine in WebAssembly

We compiled the command engine to `wasm32` and ran it inside a Cloudflare Worker, with a Durable Object holding each tenant's state. The demo is an AI-spend limiter: a Lua token bucket that decides allow-or-deny inside the Worker, with no external Redis to call. [Try it.](https://edgestash-valdr.ianmclaughlin1398.workers.dev)

It's Lua because a rate limiter is an atomic read-decide-write, which no single command and no `MULTI` can express. A script runs it as one step, and the limits live in data you can retune without redeploying.

The Lua is also safe Rust (omniLua, no C Lua), so the stack is memory-safe end to end:

| Layer | Implementation |
|---|---|
| Command engine | Valdr, safe Rust |
| TLS | rustls, no OpenSSL |
| Lua | omniLua, no C Lua |
| Isolation | WebAssembly |

Every command gets diffed against a real `valkey-server` (400+ fixtures, zero divergences). A warm decision is about 2ms of engine time.

## What's next

Replication and failover (the current focus), more of the Lua stdlib in omniLua (`cjson`, `cmsgpack`, `struct`, `bit`), and cutting edge cold-start.

Valdr is BSD-3 licensed and on [GitHub](https://github.com/ianm199/valdr).
