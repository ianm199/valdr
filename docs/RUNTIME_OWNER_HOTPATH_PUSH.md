# Runtime Owner Hotpath Push

Status: architecture plan queued after `runtime-owner-8` post-poller evidence,
2026-05-22. This wave deliberately ignores Linux-specific work and uses the
current macOS benchmark/profile runners.

## Why This Wave Exists

The runtime-owner work removed the old catastrophic DB mutex wall. The first
working end-to-end benchmark was roughly 0.07-0.09x upstream on simple
pipelined commands. The owner-loop/poller line moved the default product path
to roughly 0.7-0.8x on the core p100 workloads while preserving the wire-smoke
oracle.

The post-poller evidence says the next wall is no longer one obvious system
architecture mistake. It is local hot-path work inside faithful command
execution:

```text
post-poller profile matrix: median 0.75x, min 0.60x, max 1.49x; GET p100 0.73x
post-poller hotspot:        median 0.74x, min 0.60x, max 0.83x
post-poller calltree:       median 0.74x, min 0.62x, max 0.81x
```

The manual owner-owned DB migration remains the larger architectural packet.
This document creates a smaller evidence-backed lane first, because the current
call trees show three fixable costs that do not require changing the DB
ownership boundary.

## Current Evidence

Latest artifacts:

- Matrix: `harness/evidence/runs/20260522T144541Z-aa09851-runner-runtime-owner-8-post-poller-profile-matrix.json`
- Hotspot: `harness/evidence/runs/20260522T144623Z-651c67e-runner-runtime-owner-8-post-poller-hotspots.json`
- Call tree: `harness/evidence/runs/20260522T144733Z-d3b7602-runner-runtime-owner-8-post-poller-calltree.json`
- Raw call-tree directory: `harness/bench/profiles/20260522T144738Z-d3b7602-calltree/`

Important call-tree shapes:

- `get-p100` spends visible samples in
  `redis_commands::dispatch::dispatch_command_name`, `Instant::now`,
  `Instant::elapsed`, `clock_gettime`, `mach_absolute_time`,
  `lookup_runtime_command`, `redis_protocol::frame::encode_resp2`,
  `RawVec::reserve`, malloc/free, and `RedisDb::lookup_key`.
- `ping-p100` has the same command-dispatch timing and reply-encoding shape
  without DB lookup, which makes timing/reply overhead especially suspect.
- `set-p100` and `incr-p100` show `RedisDb::set_key`,
  `RedisDb::signal_modified`, `watched_keys_touch`, a global mutex lock, hash
  table insertion, and allocation/free.

These are telemetry signals, not proof. Each implementation packet below must
be followed by wire-smoke plus fresh benchmark/profile evidence.

## Source Anchors

Upstream Valkey:

- `reference/valkey/src/server.c:3863-4044` - `call()` times command execution,
  updates command stats, latency samples, commandlog/slowlog-adjacent state,
  and propagation after normal command execution.
- `reference/valkey/src/networking.c:596-824` - reply buffer/list append path.
- `reference/valkey/src/networking.c:1340-1505` - integer, length-prefix, and
  bulk reply helpers; bulk replies attempt copy avoidance before copying.
- `reference/valkey/src/multi.c:453-486` - `touchWatchedKey()` returns
  immediately when the DB has no watched keys and only marks clients when the
  key is actually watched.
- `reference/valkey/src/db.c:752-758` - `signalModifiedKey()` delegates to
  WATCH invalidation and client tracking.

Current Rust:

- `crates/redis-commands/src/dispatch.rs` - `dispatch_command_name()` times
  every command with `Instant::now()`/`elapsed()` before checking whether a
  slowlog entry is needed.
- `crates/redis-core/src/command_context.rs` - common reply helpers construct
  `RespFrame` and often `RedisString` objects before encoding into
  `Client::reply_buf`.
- `crates/redis-core/src/client.rs` - `Client::write_frame()` calls the generic
  RESP encoder for every reply.
- `crates/redis-core/src/db.rs` - `signal_modified()` builds a new
  `RedisString` and calls the global watched-key mutex path on every modified
  key, even when no clients are watching anything.

## Binding Non-Goals

- No command-specific fast paths for `PING`, `GET`, `SET`, or `INCR`.
- No skipped ACL, maxmemory, readonly-replica, slowlog, transactions, scripting,
  expiration, WATCH, AOF, replication, pub/sub, blocking, or RDB behavior.
- No owner-owned live `Vec<RedisDb>` in this wave.
- No Linux-specific work: no io_uring, epoll, perf-only gating, or Linux-only
  benchmarking requirement.
- No public performance claim. All new numbers remain alpha telemetry.

## Packet Families

### 10. Command Timing / Slowlog Gate

Hypothesis: on simple p100 workloads, the current unconditional command timer
cost is a measurable fraction of the remaining gap. Upstream also times command
execution, so the correct fix is not "turn off slowlog." The packet should
reduce avoidable overhead while preserving command duration semantics.

Allowed ideas:

- cache the slowlog/config predicate in a cheap form;
- avoid work that is only needed when slowlog/latency recording can actually
  consume it;
- consolidate duration capture so dispatch does not pay duplicate timing costs;
- keep `CONFIG SET slowlog-log-slower-than` and `slowlog-max-len` live.

Forbidden:

- disabling slowlog by default;
- lying about command durations;
- special-casing benchmark commands;
- bypassing AOF/replication argv snapshots.

Gate:

```text
implementation -> wire-smoke -> profile matrix -> calltree
```

### 11. RESP Reply Buffer Hot Path

Hypothesis: hot replies pay too much allocation and generic frame-encoding cost.
The profile shows `encode_resp2`, `RawVec::reserve`, malloc/free, and temporary
reply objects on GET/SET/PING paths. Upstream's reply path has direct helpers
for integer/simple/bulk bytes and attempts copy avoidance for bulk strings.

Allowed ideas:

- add direct `Client` helpers for RESP2 legacy replies:
  simple string, integer, null bulk, bulk bytes, array header;
- have `CommandContext` hot reply helpers call those direct encoders;
- pre-reserve exact or near-exact reply bytes for small simple replies;
- preserve `reply_frame()` and generic RESP3-only frame handling through the
  existing encoder.

Forbidden:

- changing command APIs to return raw bytes;
- bypassing `CommandContext`;
- breaking RESP3 native frames;
- removing tests that assert exact wire bytes.

Gate:

```text
implementation -> wire-smoke -> profile matrix -> calltree
```

### 12. WATCH Dirty-Key Fast Path

Hypothesis: SET/INCR still pay global WATCH bookkeeping even when no client has
registered any watch. Upstream checks whether the DB has watched keys before
doing deeper work. The Rust port currently enters a global mutex path and
allocates a `RedisString` in `signal_modified()` on every write.

Allowed ideas:

- add a cheap global or per-index "watchers present" fast path before locking;
- avoid constructing a new `RedisString` when no watchers exist;
- preserve exact `WATCH` / `MULTI` / `EXEC` invalidation when watchers do exist;
- extend or reuse the runtime-owner canary corpus for WATCH invalidation.

Forbidden:

- disabling WATCH;
- making WATCH only work on the old per-thread path;
- changing EXEC dirty-CAS behavior;
- suppressing dirty-key tracking for expired watched keys.

Gate:

```text
implementation -> wire-smoke -> profile matrix -> calltree -> final p100 regression gates
```

## Larger Packet Still Deferred

`runtime-owner-9-owner-owned-db-architecture` remains the manual high-risk
migration. That packet should move selected DB ownership into `RuntimeOwner`
only after this smaller wave shows which residual costs remain. If these three
packets do not move the median or p100 ratios, the next architecture session
should spend compute on owner-owned DB rather than more local hot-path patches.

## Kickoff

Run the queued auto wave:

```bash
python3 ../port-harness/loop/run-loop.py \
  --project . \
  --selector auto \
  --auto-dispatch \
  --dispatch-runtime codex \
  --dispatch-sandbox danger-full-access \
  --dispatch-approval never \
  --dispatch-timeout-s 5400 \
  --max-iterations 32 \
  --max-failures 3 \
  --max-same-packet-failures 2
```

Useful checks:

```bash
python3 ../port-harness/loop/parallel-plan.py --project . --selector auto --json
python3 ../port-harness/loop/check-completion.py --project . --json
python3 harness/bench/history.py --serve --port 8022
```
