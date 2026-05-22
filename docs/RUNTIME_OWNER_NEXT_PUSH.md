# Runtime Owner Next Push

Status: queued packet plan, not yet executed.

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

The remaining gap is no longer the old DB-mutex wall. It is local runtime
owner-loop work: readiness, writeback, parser/integer parsing, dispatch lookup,
hashing/allocation, and socket I/O. The next run should use the new harness
performance loop:

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

Recommended direction:

- add `mio` as a workspace dependency;
- keep the synchronous `&mut RuntimeOwner` command execution model;
- keep `redis_commands::dispatch` as the only command path;
- replace std nonblocking linear scanning with poll readiness;
- keep TLS on the existing per-connection path;
- keep the transitional `global_databases()` DB model for this run.

Why this first:

- It is the smallest production-shaped step after the std owner loop.
- It targets the remaining p100 shape without inventing a second DB model.
- It is much safer than moving the live DB list into `RuntimeOwner`.
- If it fails to move numbers, the post-poller call-tree evidence should tell
  us whether the next wall is command execution, allocation/copying, or the
  transitional DB lock.

## Explicit non-goals for the next run

- No benchmark-only GET/SET/PING/INCR fast paths.
- No sharding.
- No owner-owned `Vec<RedisDb>` migration in the poller packet.
- No TLS migration into `RuntimeOwner`.
- No disabling ACL, transactions, scripting, expiration, pub/sub, blocking,
  AOF, RDB, or replication for speed.
- No public benchmark claim. The next run remains alpha telemetry.

## Queued packet family

The new queue begins after `runtime-owner-5-post-polish-hotspots`.

1. `runtime-owner-6-current-oracle`
   - Reprove wire compatibility before spending optimization tokens.

2. `runtime-owner-6-current-profile-matrix`
   - Fresh matrix at current HEAD.

3. `runtime-owner-6-current-hotspots`
   - Long p100 sampled profile at current HEAD.

4. `runtime-owner-6-profile-artifact-runner`
   - Add a runner that preserves call-tree/flamegraph-style artifacts.
   - On macOS it may use `/usr/bin/sample`.
   - On Linux it may use `perf` / `cargo flamegraph` if available.
   - The runner must emit typed `RunnerResult` JSON and attach raw artifacts.

5. `runtime-owner-6-current-calltree`
   - Run the new artifact profiler before implementation.

6. `runtime-owner-7-poller-architecture`
   - Architect pass that reads all fresh evidence and either:
     - approves the `mio` poller packet with a concrete contract, or
     - blocks with a better packet graph.

7. `runtime-owner-8-mio-poller-owner-loop`
   - Bounded implementation packet.
   - Adds the poller dependency and rewires only plain-TCP readiness/writeback.

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

The next selected packet should be `runtime-owner-6-current-oracle`. If the
first selected packet is anything else, stop and inspect `harness/work-packets.jsonl`
plus `harness/evidence/ledger.jsonl` before dispatching.

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
