# Runtime Owner Overnight Architecture

Status: operator-approved overnight experiment, 2026-05-22.

## Goal

Use the harness to push the Redis/Valkey port toward performance parity without
breaking the drop-in compatibility story.

This is not a benchmark-only fork. The implementation path must keep normal
command dispatch, ACL, transactions, scripting, expiration, pub/sub, blocking
wakeups, AOF, replication, RDB, and wire compatibility intact. Benchmarks only
count when they come from the default product path after the oracle is green.

## Current Evidence

The latest runtime-owner baseline shows the remaining simple-command gap is not
mostly parser, allocator, or dispatch-table overhead. The hotspot evidence in
`harness/evidence/runs/20260522T010206Z-15060d7-runner-runtime-owner-baseline-hotspots.json`
shows `__psynch_mutexwait` dominating GET and INCR at pipeline depth 100.

That means the next serious performance move is ownership, not another local
micro-optimization.

Post-scaffold evidence keeps that conclusion intact:

- `runtime-owner-post-scaffold-oracle` passed at
  `harness/evidence/runs/20260522T041306Z-2e2ae35-runner-runtime-owner-post-scaffold-oracle.json`.
- `runtime-owner-post-scaffold-profile-matrix` reported median 0.68x, GET p1
  0.68x, and GET p100 0.64x at
  `harness/evidence/runs/20260522T041648Z-1a4086e-runner-runtime-owner-post-scaffold-profile-matrix.json`.
- `runtime-owner-post-scaffold-hotspots` reported median 0.56x, min 0.48x,
  max 0.60x at
  `harness/evidence/runs/20260522T041658Z-ab1d468-runner-runtime-owner-post-scaffold-hotspots.json`.

Post-owner evidence moved the diagnosis from architecture-scale mutex waiting
to local owner-loop work:

- `runtime-owner-4-post-owner-loop-oracle` passed at
  `harness/evidence/runs/20260522T043942Z-7c190b1-runner-runtime-owner-4-post-owner-loop-oracle.json`.
- `runtime-owner-5-owner-loop-polish` profile matrix reported median 0.80x,
  min 0.57x, max 1.18x, GET p1 1.10x, and GET p100 0.72x at
  `harness/bench/results/20260522T044745Z-803918c-profile-matrix.tsv`.
- `runtime-owner-5-owner-loop-polish` hotspots reported median 0.74x,
  min 0.61x, max 0.80x at
  `harness/bench/results/20260522T044801Z-803918c-hotspots.json`.

Before `runtime-owner-4-std-nonblocking-owner-loop`, the Rust surface still
matched the architectural diagnosis:

- `crates/redis-server/src/main.rs` accepts plain TCP with
  `TcpListener::incoming`, spawns one client read/dispatch thread, and spawns a
  writer thread for plain TCP.
- `process_current_command_with_db` still constructs
  `CommandContext::with_server` and calls `redis_commands::dispatch`.
- DB state still enters dispatch through `Arc<Mutex<RedisDb>>` from
  `global_databases()`.
- `crates/redis-server/src/runtime_owner.rs` is an inert scaffold; it does not
  yet own sockets, dispatch, or the live DB list.

## Implementation Update: runtime-owner-4

The implementation packet moves the default plain-TCP listener into
`RuntimeOwner::run_plain_tcp`, a single std-library nonblocking owner loop with
a linear scan over live slots. The loop accepts until `WouldBlock`, drains each
slot's foreign-payload receiver, reads sockets until `WouldBlock`, parses with
`parse_inline_or_multibulk_into`, dispatches through
`CommandContext::with_server` and `redis_commands::dispatch`, flushes pending
slot writes, and removes closed clients.

The DB storage rule remains transitional: the owner loop uses
`global_databases()` `Arc<Mutex<RedisDb>>` handles and may hold DB0's guard
across a bounded per-slot dispatch batch. It does not create an owner-owned
live `Vec<RedisDb>`, so TLS, active-expire, AOF replay, replication, RDB, and
other helpers still observe the same keyspace.

TLS remains on the existing thread-per-connection path. Foreign bytes for
plain-TCP clients still enter through mpsc senders registered in
`PubSubRegistry`; the owner loop drains the matching receivers and writes the
socket itself.

## Implementation Update: runtime-owner-5

The perf-polish packet keeps the same subsystem boundary as runtime-owner-4:
plain-TCP owner-loop internals only. It does not change command execution,
RESP wire parsing, ACL, transactions, scripting, expiration, pub/sub, blocking
wakeups, AOF, replication, RDB, TLS, or the DB ownership model.

The local change is reply staging:

- `ClientWriteBuffer` now tracks a consumed offset, so partial socket writes do
  not immediately `drain(..n)` and memmove the remaining reply bytes.
- Foreign payload receivers queue owned `Vec<u8>` payloads into the slot write
  buffer instead of collecting and copying them through borrowed slices.
- The dispatch batch lets `client.reply_buf` accumulate across parsed commands
  and transfers it to the slot write buffer once per batch, matching the
  upstream `beforeSleep`/pending-write shape more closely.
- Fully consumed query buffers clear directly; partial consumption keeps the
  existing drain behavior.

The post-polish matrix and hotspot runs are alpha telemetry, not a public
performance guarantee. They do show the architectural packet did its job:
sampled `__psynch_mutexwait` is no longer the dominant remaining leaf, and the
next packet should focus on owner-loop readiness/write batching, command
lookup, parser/integer parsing, allocator traffic, and command hasher work.

## Poller Architecture Update: runtime-owner-7

The runtime-owner-6 evidence approves the next bounded step: replace the std
nonblocking linear readiness scan with a `mio` readiness poller for plain TCP.

Evidence used:

- `runtime-owner-6-current-oracle` passed full wire-smoke, including the
  runtime-owner canary corpus, at
  `harness/evidence/runs/20260522T135522Z-d884855-runner-runtime-owner-6-current-oracle.json`.
- `runtime-owner-6-current-profile-matrix` reported core-p100 GET 0.78x, SET
  0.79x, INCR 0.63x, and PING 0.76x at
  `harness/evidence/runs/20260522T135524Z-5f159bc-runner-runtime-owner-6-current-profile-matrix.json`.
- `runtime-owner-6-current-hotspots` reported median 0.75x, min 0.59x, max
  0.86x at
  `harness/evidence/runs/20260522T135537Z-1edcb59-runner-runtime-owner-6-current-hotspots.json`.
- `runtime-owner-6-current-calltree` reported median 0.74x, min 0.56x, max
  0.84x and stored raw `sample` artifacts under
  `harness/bench/profiles/20260522T140914Z-0022bc7-calltree/`.

The call-tree artifacts show `RuntimeOwner::run_plain_tcp` still paying for
repeated nonblocking `accept`, idle `yield_now`, socket read/write, parser,
dispatch lookup, hashing, and allocation. This is enough evidence to answer
the earlier poller dependency question for the next packet: use `mio`, preserve
the synchronous owner-loop command model, and keep the product path faithful.

This does not widen the milestone. TLS remains outside the owner loop; the live
DB list remains behind `global_databases()`; sharding and I/O threads remain
out of scope; background semantics stay live; no benchmark-only command path is
allowed.

## Overnight Strategy

Do the lowest-blast-radius owner-loop step first:

```text
plain TCP listener
  -> single nonblocking owner loop
      owns accepted plain-TCP client slots
      drains readable sockets
      parses RESP into each slot's Client
      dispatches through the existing redis_commands::dispatch path
      writes pending replies from the same owner loop
      keeps TLS on the existing thread-per-client path
```

This deliberately uses standard-library nonblocking sockets and a linear scan
over live clients for the overnight run. It does not add `mio`, `tokio`, or raw
platform poller bindings yet.

Why this is the right first experiment:

- It removes the thread-per-client scheduling shape from the hot path.
- It avoids adding a dependency while the correctness surface is still moving.
- It tests the most important architectural hypothesis with limited code.
- It keeps the normal dispatcher and command semantics in the path.
- It is easy to replace with a real `mio` poller later if the evidence says the
  owner model is right.

The expected benchmark improvement is on pipelined tiny commands where the
current design pays thread/mutex overhead on each read batch. This may not reach
full upstream parity; if it does not, the post-run profile should tell us
whether the next wall is linear scanning, socket writes, parser cost, reply
encoding, or something else.

## Binding Decisions For This Run

- **Sharding:** out of scope.
- **Benchmark-only GET/SET/PING/INCR bypass:** rejected.
- **Poller dependency:** defer `mio`; use std nonblocking linear scan for this
  run only.
- **TLS:** keep existing TLS thread-per-client path; do not migrate TLS into
  the owner loop tonight.
- **I/O threads:** out of scope; owner loop first, I/O threads later if needed.
- **Soak:** not a public claim gate tonight. Use profile matrix + hotspot
  evidence as alpha telemetry.
- **Default product path:** the implementation packet may replace the default
  plain-TCP path, but benchmark numbers only count after post-owner-loop
  `wire-smoke` is green.

## Runtime-Owner-4 Contract

`runtime-owner-4-std-nonblocking-owner-loop` is approved for dispatch with this
bounded contract:

- Plain TCP moves first; TLS remains on the existing thread-per-client path.
- Use standard-library nonblocking `TcpListener`/`TcpStream` and a linear scan.
  Do not add `mio`, `polling`, `tokio`, raw platform pollers, or unsafe poller
  code.
- Keep `redis_commands::dispatch` as the only command execution path.
- Use `parse_inline_or_multibulk_into` for request parsing.
- Keep the existing `global_databases()` `Arc<Mutex<RedisDb>>` handles as the
  live DB source for this packet. The owner loop may hold the selected DB guard
  across a bounded parse/dispatch batch, but must not create a second live
  `Vec<RedisDb>` that diverges from TLS, active-expire, AOF replay,
  replication, or RDB helpers.
- Accepted plain-TCP sockets, `Client` values, query buffers, parsed argv
  staging, and ordinary reply flushing are owned by owner-loop client slots.
- Pub/sub, blocked wakeups, WAIT/replication replies, and other foreign bytes
  for owner-loop clients enter through per-slot `mpsc::Sender<Vec<u8>>`
  handles. The owner loop drains matching receivers and writes the socket; no
  foreign thread writes an owner-loop plain-TCP socket directly.
- Preserve `maxclients`, client-info registry updates, connected-client
  metrics, `RESET`, `QUIT`, selected DB state, pub/sub cleanup, replica
  cleanup, and blocked-key cleanup.
- Each owner tick accepts until `WouldBlock`, drains foreign payload channels,
  reads clients until `WouldBlock`, parses and dispatches completed commands,
  flushes pending writes, cleans closed clients, and sleeps or yields briefly
  if no progress occurred.

This contract intentionally keeps the long-term `Vec<RedisDb>` owner model out
of the first implementation packet. It removes the per-client command threads
from the plain-TCP hot path without inventing a second database model.

## Required Order

1. Fix runtime-owner canary divergences.
2. Re-run the full wire-smoke oracle.
3. Land the inert RuntimeOwner scaffold.
4. Re-run oracle and benchmarks.
5. Run one architect packet to sanity-check this overnight design against the
   current code and evidence.
6. Implement the std nonblocking owner loop for plain TCP.
7. Re-run oracle, profile matrix, and hotspots.
8. If the first owner loop is green but still slow, run one bounded perf-polish
   packet using the new hotspot evidence.

## Implementation Constraints

- Keep `redis_commands::dispatch` as the command execution path.
- Do not create a second semantic DB model. If the owner loop uses the existing
  global DB handle as an intermediate step, document that it is an ownership
  transition and ensure shared access remains serialized.
- Do not disable pub/sub, blocking commands, replication, AOF, RDB, scripting,
  or ACL to improve numbers.
- Use existing `Client`, `RedisDb`, `CommandContext`, `PubSubRegistry`, and
  reply-buffer primitives unless a packet explicitly updates the vocabulary.
- Plain TCP and TLS may have different runtime implementations during this
  milestone, but their command behavior must remain byte-compatible.
- The owner loop cannot count as a performance win until
  `runtime-owner-4-post-owner-loop-oracle` passes.

## Stop Conditions

Quarantine the chain if:

- wire-smoke regresses and cannot be restored in the same packet;
- a benchmark improves by bypassing normal dispatch or command semantics;
- the implementation introduces command-specific fast paths;
- the owner loop becomes default before canaries are green;
- background features are silently disabled;
- benchmark docs are updated without matching oracle evidence.

## What Success Looks Like

Minimum useful success:

- canaries green;
- owner-loop packet lands;
- wire-smoke green;
- fresh profile matrix and hotspot evidence committed.

High success:

- simple pipelined commands move materially toward upstream parity;
- hotspot evidence no longer points primarily at `__psynch_mutexwait`;
- remaining gap is local and packetizable.

Full speed parity by morning is possible but not promised. The real deliverable
is either a faster faithful runtime path or evidence precise enough to choose
the next architecture packet.
