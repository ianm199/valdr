[← Valdr blog](blog.html)

# Valdr 1.0 — a memory-safe Valkey, and its first life beyond the server

*2026-06-16*

We started Valdr with one question: can the core infrastructure the web runs on be rebuilt in memory-safe Rust, and *proven* to behave exactly like the thing it replaces? Two milestones now say yes. The single-node core has reached the bar we set for calling it stable. And we have our first use case that shows Valdr is more than a server port — the same engine, compiled to WebAssembly, running Lua at the edge.

## What "1.0" means

Valdr 1.0 is the **single-node** milestone, and we're deliberate about that scope.

- **It passes Valkey's own test suite.** The single-node core suite is green: **3,035 of 3,035** counted assertions across the upstream 54-file suite, and 2,525 of 2,526 source-test blocks. We treat the upstream suite as the oracle — if Valdr and Valkey disagree on a single byte, that is a bug, not a footnote. [Full coverage →](coverage.html)
- **It runs real workloads over TLS.** Memory-safe TLS via **rustls** — no OpenSSL, no `fork()`, no C in the hot path.
- **It is fast.** At parity with, or faster than, Valkey on most common commands. [Benchmarks →](index.html)
- **It is mostly memory-safe by construction** — safe Rust with a small, audited `unsafe` budget, enforced in CI.

What 1.0 does **not** claim yet: multi-node replication, Sentinel, or Cluster. Those are the active frontier, and we will not blur a single-node claim into a high-availability one. [Roadmap →](roadmap.html)

## The first real use case: Valkey semantics in WebAssembly, with Lua

Passing the suite proves correctness. The more interesting question was whether the engine is *clean* enough to live somewhere an ordinary Redis can't go. So we took the Valdr command engine, compiled it to `wasm32`, and embedded it inside a Cloudflare Worker — with a Durable Object as the per-tenant shard. We call it **EdgeStash**.

The demo is an AI-spend rate limiter: a fake AI endpoint protected by a token bucket written in Lua, with the entire allow/deny decision running **inside the Worker**, next to the request — no round trip to an external Redis service.

[Try the live demo →](https://edgestash-valdr.ianmclaughlin1398.workers.dev)

Why Lua? A real rate limiter — or quota, or idempotency check — is an atomic *read-decide-write*: read the bucket, compute the refill, decide, persist. Several steps, with arithmetic and a branch. No single command does that, and a transaction cannot decide mid-flight. A Lua script can: it runs as one atomic unit, where the data is. Change the policy (which is stored as data) and the same script honors the new limits with no redeploy.

And here is the part we are proudest of: **the Lua is also safe Rust.** EdgeStash runs on **omniLua**, a pure-Rust Lua 5.1 — no C Lua. So the whole stack is memory-safe by default, end to end:

| Layer | Implementation |
|---|---|
| Command engine | Valdr — safe Rust |
| TLS | rustls — safe Rust, no OpenSSL |
| Lua scripting | omniLua — safe Rust, no C Lua |
| Isolation | WebAssembly sandbox |

We hold the engine to the same bar as the server: every command it runs is **differentially tested** against a real `valkey-server` — more than 400 fixtures, **zero divergences**. A warm limiter decision is about **2 ms** of engine time.

## Why this shape matters

The durable result is not the demo — it is the boundary that made it possible. Because Valdr is safe Rust with a clean split between the command engine and the transport, it can be a **server** when that is the right shape, and an **embeddable, programmable state engine** when the request is already running somewhere constrained — an edge worker, a browser, another process. Redis-compatible atomic state, with real Lua logic, embedded where you need it.

## What is next

- **Replication and high availability** — the current frontier, held to its own evidence gates.
- **omniLua library parity** — `cjson`, `cmsgpack`, `struct`, and `bit`, so more existing Redis scripts run unchanged.
- **Edge cold-start** — trimming the first-request latency when a tenant shard wakes.

Valdr is BSD-3-licensed and developed in the open. [GitHub →](https://github.com/ianm199/valdr)
