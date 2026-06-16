[← Valdr blog](blog.html)

# Valdr 1.0: a memory-safe Valkey, now running in WebAssembly

*2026-06-16*

Valdr is a reimplementation of Valkey in Rust. We've been building it to answer a fairly blunt question: can you rebuild infrastructure this critical in a memory-safe language and prove it behaves exactly like the original?

The single-node core now passes Valkey's own test suite, so we're calling it stable. That's what 1.0 means here. The rest of this post is about the first thing we've run on top of it that isn't a server: the Valdr engine, compiled to WebAssembly, executing Lua at the edge.

## What 1.0 covers

1.0 is the single-node milestone, and only that.

- It passes Valkey's own test suite: 3,035 of 3,035 counted assertions across the upstream 54-file suite, plus 2,525 of 2,526 source-test blocks. We run that suite as an oracle. If Valdr and Valkey disagree on a single byte, we treat it as a bug.
- It serves real traffic over TLS, on rustls instead of OpenSSL. No C in the TLS path, and no `fork()` anywhere in the server.
- It's competitive on speed: at parity with or faster than Valkey on most of the common commands. The [benchmarks](index.html) have the per-command numbers.
- It's mostly safe Rust. There's a small `unsafe` budget that's audited and capped in CI, with no sprawl.

What 1.0 deliberately leaves out: replication, Sentinel, and Cluster. Multi-node is the work in progress, and we'd rather ship an honest single-node claim than a vague "production-ready" one. The [roadmap](roadmap.html) tracks the rest.

## Running the engine in WebAssembly

Passing a test suite tells you the engine is correct. It doesn't tell you it's clean. What I actually wanted to know was whether we could lift the command engine out of the server and run it somewhere an ordinary Redis can't go.

So we compiled it to `wasm32` and dropped it inside a Cloudflare Worker, with a Durable Object holding each tenant's state. We're calling that EdgeStash.

The demo is a spend limiter for an AI endpoint. A token bucket, written in Lua, decides allow-or-deny on every request, and it runs inside the Worker right next to the request. Nothing calls out to an external Redis. [Try it live.](https://edgestash-valdr.ianmclaughlin1398.workers.dev)

Why Lua, and not just a couple of commands? A rate limiter is an atomic read-decide-write. You read the bucket, work out how much it has refilled, decide, and write the new value back. That's several steps with arithmetic and a branch in the middle. No single Redis command does it, and a `MULTI` transaction can't either, because a transaction can't read a value and then decide what to do about it. A script can. It runs as one atomic unit, on the data itself, and because the limits live in their own key you can retune them without touching the script.

The Lua is also safe Rust, and that's the part I find most interesting. EdgeStash runs on omniLua, a Lua 5.1 interpreter written in Rust, so there's no C Lua anywhere in the request path. The whole stack ends up memory-safe by default:

| Layer | Implementation |
|---|---|
| Command engine | Valdr, safe Rust |
| TLS | rustls, no OpenSSL |
| Lua scripting | omniLua, no C Lua |
| Isolation | WebAssembly sandbox |

We hold the engine to the same standard as the server. Every command it runs gets diffed against a real `valkey-server` across more than 400 fixtures, and there are currently zero divergences. A warm limiter decision is about 2ms of engine time; the rest of a request is network.

## Why bother with the WebAssembly version

The demo is a toy. The boundary underneath it isn't. Once the command engine is cleanly split from networking and storage, it stops needing to be a server at all. It can be a library you embed wherever the request already runs: an edge worker now, a browser or another process later. You get Redis-style atomic state with real scripting, without standing up a Redis to talk to.

## What's next

Roughly in order:

- Replication and failover. This is the current focus, and it earns its own test gates before we claim it works.
- More of the Lua standard library in omniLua (`cjson`, `cmsgpack`, `struct`, `bit`), so existing Redis scripts run without edits.
- Edge cold starts. The first request to a sleeping tenant takes about half a second today, and most of that is ours to cut.

Valdr is BSD-3 licensed and built in the open. The code is on [GitHub](https://github.com/ianm199/valdr).
