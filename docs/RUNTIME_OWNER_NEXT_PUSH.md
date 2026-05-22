# Runtime Owner Next Push

Status: `runtime-owner-8-mio-poller-owner-loop` implementation landed and its
post-poller oracle, profile, call-tree, and p100 regression gates passed. The
next auto wave is the smaller macOS hot-path packet family in
`docs/RUNTIME_OWNER_HOTPATH_PUSH.md`; the owner-owned DB migration remains a
manual architecture packet.

This document scopes the next large harness run after the std nonblocking
`RuntimeOwner` loop. The previous run proved the big architecture move was
real: default plain TCP no longer uses the old per-client command-thread path,
wire smoke stayed green, and the final post-polish benchmark matrix reached:

```text
profile matrix median 0.79x, min 0.66x, max 1.63x
GET p1 1.08x
GET p100 0.76x
hotspot median 0.78x, min 0.58x, max 0.84x
```

The current runtime-owner-6 evidence keeps that diagnosis intact:

```text
runtime-owner-6-current-oracle: wire-smoke pass, including 21-runtime-owner-canaries
runtime-owner-6-current-profile-matrix: core-p100 GET 0.78x, SET 0.79x, INCR 0.63x, PING 0.76x
runtime-owner-6-current-hotspots: median 0.75x, min 0.59x, max 0.86x
runtime-owner-6-current-calltree: median 0.74x, min 0.56x, max 0.84x
```

The call-tree artifacts at
`harness/bench/profiles/20260522T140914Z-0022bc7-calltree/` show the std owner
loop still spending measurable samples in repeated nonblocking `accept`,
`yield_now`, socket read/write, parser, dispatch lookup, hashing, and allocation
work. For example, `ping-p100` records `__accept` and `cthread_yield` under
`RuntimeOwner::run_plain_tcp`, while `get-p100` and `incr-p100` show the
expected command-path costs after the loop reaches dispatch. The remaining gap
is no longer the old DB-mutex wall; it is local runtime owner-loop readiness and
per-command work.

The next run should use the harness performance loop:

```text
oracle
  -> profile matrix
  -> hotspot profile
  -> call-tree profile
  -> architect diagnosis
  -> bounded implementation
  -> oracle
  -> profile matrix
  -> hotspot profile
  -> call-tree profile
  -> regression comparators
```

## Chosen next architecture bet

The next auto run should attempt a real readiness poller for the plain-TCP
owner loop.

Approved direction:

- add `mio` as a workspace dependency with `os-poll` and `net` features;
- keep the synchronous `&mut RuntimeOwner` command execution model;
- keep `redis_commands::dispatch` as the only command path;
- replace std nonblocking linear accept/read/write scans with readiness events;
- keep TLS on the existing per-connection path;
- keep the transitional `global_databases()` DB model for this run.

Why this first:

- It is the smallest production-shaped step after the std owner loop.
- It targets the repeated accept/yield/readiness shape now visible in call-tree
  evidence without inventing a second DB model.
- It is much safer than moving the live DB list into `RuntimeOwner`.
- If it fails to move numbers, the post-poller call-tree evidence should tell
  us whether the next wall is command execution, allocation/copying, or the
  transitional DB lock.

## Runtime-Owner-8 Contract

`runtime-owner-8-mio-poller-owner-loop` is approved with this contract:

- Plain TCP only. TLS remains on the existing thread-per-connection path.
- Use `mio::Poll`, `mio::Events`, and stable slot tokens for readiness. Do not
  add `tokio`, `polling`, raw `epoll`/`kqueue`, or any unsafe poller surface.
- The listener token accepts until `WouldBlock`; client readable tokens drain
  socket reads; writable interest is enabled only while the slot write buffer
  has pending bytes and disabled again when it is empty.
- Slots that still have complete parsed commands after the per-tick command cap
  must be rescheduled by the owner loop; they must not wait for another socket
  readiness edge before dispatch continues.
- Foreign pub/sub, blocked, WAIT, and replication payloads still enter through
  per-slot mpsc receivers. The owner loop drains receivers after poll returns
  and on a short bounded timeout, queues bytes into the slot write buffer, and
  owns the socket write. Foreign threads must not write owner-loop sockets.
- Preserve `redis_commands::dispatch` through `CommandContext::with_server`,
  `parse_inline_or_multibulk_into`, selected DB state, `RESET`, `QUIT`,
  maxclients, client-info registry updates, connected-client metrics, pub/sub
  cleanup, replica cleanup, and blocked-key cleanup.
- Keep the transitional `global_databases()` storage model. Do not create an
  owner-owned live `Vec<RedisDb>` in this packet.
- If `cargo check --workspace`, focused tests, or full wire-smoke cannot be
  restored, stop with `TODO(architect)` instead of weakening compatibility.

Implementation update:

- `mio` is now a workspace dependency with `os-poll` and `net`.
- The default plain-TCP `RuntimeOwner` loop uses `mio::Poll`, `mio::Events`,
  listener token `0`, and stable slot-derived client tokens.
- Listener readiness accepts until `WouldBlock`; readable client tokens drain
  socket reads and dispatch through `parse_inline_or_multibulk_into`,
  `CommandContext::with_server`, and `redis_commands::dispatch`.
- Writable interest is registered only while a slot write buffer has pending
  bytes, and is removed again once the buffer drains.
- Slots that hit `MAX_COMMANDS_PER_SLOT_TICK` with a complete command still in
  `query_buf` are queued for owner-loop continuation without waiting for a new
  socket readiness edge.
- Foreign payload receivers are still per-slot mpsc channels; the owner drains
  them after poll returns on a short bounded timeout and owns all socket writes.

## Explicit non-goals for the next run

- No benchmark-only GET/SET/PING/INCR fast paths.
- No sharding.
- No owner-owned `Vec<RedisDb>` migration in the poller packet.
- No TLS migration into `RuntimeOwner`.
- No disabling ACL, transactions, scripting, expiration, pub/sub, blocking,
  AOF, RDB, or replication for speed.
- No public benchmark claim. The next run remains alpha telemetry.

## Queued packet family already completed

The new queue begins after `runtime-owner-5-post-polish-hotspots`.

1. `runtime-owner-6-current-oracle`
   - Reprove wire compatibility before spending optimization tokens.

2. `runtime-owner-6-current-profile-matrix`
   - Fresh matrix at current HEAD.

3. `runtime-owner-6-current-hotspots`
   - Long p100 sampled profile at current HEAD.

4. `runtime-owner-6-profile-artifact-runner`
   - Done in `harness/bench/profile-calltree.py`.
   - Runner id: `bench-profile-calltree`.
   - Emits typed `RunnerResult` JSON, a TSV summary, and raw profiler
     artifacts under `harness/bench/profiles/<UTC>-<commit>-calltree/`.
   - Uses `/usr/bin/sample` on macOS, or `perf` / `cargo flamegraph` on Linux
     when available.
   - Profiles attach to the Rust server PID only; the benchmark commands and
     server flags stay in the normal harness envelope.

5. `runtime-owner-6-current-calltree`
   - Run the new artifact profiler before implementation.

6. `runtime-owner-7-poller-architecture`
   - Architect pass that reads all fresh evidence and either:
     - approves the `mio` poller packet with a concrete contract, or
     - blocks with a better packet graph.

7. `runtime-owner-8-mio-poller-owner-loop`
   - Done in this implementation packet.
   - Adds the `mio` poller dependency and rewires only plain-TCP
     readiness/writeback.

8. Post-poller gates:
   - `runtime-owner-8-post-poller-oracle`
   - `runtime-owner-8-post-poller-profile-matrix`
   - `runtime-owner-8-post-poller-hotspots`
   - `runtime-owner-8-post-poller-calltree`
   - p100 performance-regression comparators for PING/GET/SET/INCR.

9. `runtime-owner-9-owner-owned-db-architecture`
   - Manual follow-up only.
   - This is the real high-risk migration; do not auto-dispatch it until the
     poller evidence is reviewed.

## Next queued packet family

The next auto selector wave is intentionally smaller than owner-owned DB. It is
documented in `docs/RUNTIME_OWNER_HOTPATH_PUSH.md` and targets three measured
post-poller costs:

1. `runtime-owner-10-hotpath-timing-gate`
   - reduce avoidable command timing / slowlog predicate overhead without
     disabling slowlog or command duration semantics;
   - gate with wire-smoke, profile matrix, and calltree.

2. `runtime-owner-11-reply-buffer-hotpath`
   - add direct RESP2 reply-buffer helpers for common legacy replies while
     preserving generic RESP3 frame handling;
   - gate with wire-smoke, profile matrix, and calltree.

3. `runtime-owner-12-watch-dirty-fastpath`
   - skip global WATCH dirty-key lock/allocation work when no clients are
     watching keys, while preserving WATCH/MULTI/EXEC invalidation;
   - gate with wire-smoke, profile matrix, calltree, and final p100 regression
     comparators.

This wave explicitly ignores Linux-only optimizations. No io_uring, epoll,
Linux perf dependency, sharding, command-specific benchmark bypass, or
owner-owned live DB migration is in scope.

## Kickoff command

The wrapper defaults to Codex with local-network-capable sandboxing because the
runner packets start local Valkey/valkey-rs servers and benchmark clients.

```bash
bash harness/run-runtime-owner-loop.sh --reset
```

Equivalent explicit form:

```bash
python3 ../port-harness/loop/run-loop.py \
  --project . \
  --reset \
  --selector auto \
  --auto-dispatch \
  --dispatch-runtime codex \
  --dispatch-sandbox danger-full-access \
  --dispatch-approval never \
  --dispatch-timeout-s 5400 \
  --max-iterations 24 \
  --max-failures 3 \
  --max-same-packet-failures 2
```

After this implementation packet, the next selected packet should be
`runtime-owner-8-post-poller-oracle`. If the loop selects anything else,
inspect `harness/work-packets.jsonl` and `harness/evidence/ledger.jsonl` before
dispatching.

## Success criteria

Minimum useful success:

- post-poller wire smoke passes;
- benchmark artifacts are captured with dirty/tree metadata from the updated
  harness;
- no p100 simple-command regression worse than the configured tolerance;
- call-tree artifacts point to the next wall.

High success:

- p100 GET/SET/PING move closer to 0.9x+;
- p99 tails narrow or stay neutral;
- the profiler shows readiness/writeback cost reduced;
- the next architecture decision is obviously owner-owned DB, allocation, or
  parser/serializer work.

## Decision boundary

If the `mio` poller does not improve p100 throughput or tails, do not keep
patching readiness blindly. Stop at the post-poller architecture decision and
use the call-tree evidence to pick the next packet family.
