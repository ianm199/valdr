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
- **Default product path:** if the owner loop lands, plain TCP may become the
  default path only after `wire-smoke` is green.

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
  transition and ensure no two command owners mutate it concurrently.
- Do not disable pub/sub, blocking commands, replication, AOF, RDB, scripting,
  or ACL to improve numbers.
- Use existing `Client`, `RedisDb`, `CommandContext`, `PubSubRegistry`, and
  reply-buffer primitives unless a packet explicitly updates the vocabulary.
- Plain TCP and TLS may have different runtime implementations during this
  milestone, but their command behavior must remain byte-compatible.

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
