# Runtime Ownership Decision

Status: seeded for harness run, 2026-05-21. The architect packet
`runtime-owner-0-faithful-map` is expected to refine this file after reading
fresh benchmark and stack-sampling evidence.

## Decision

The Redis performance work must preserve Valkey compatibility. The target
architecture is a faithful runtime-owner model, not sharding and not a
benchmark-only shortcut.

The runtime owner may eventually own:

- the normal-client table;
- selected database state;
- socket readiness and ordinary reply flushing;
- timers and active expire scheduling;
- pub/sub delivery routing;
- blocking wakeups;
- slowlog/latency/metrics updates;
- AOF and replication propagation ordering.

The default product path must not move to this owner loop until correctness
evidence proves the migration preserves the drop-in envelope.

## Non-Goals

- No special fast path for `PING`, `GET`, `SET`, or `INCR` that bypasses normal
  command dispatch.
- No sharded DB ownership in this milestone.
- No disabling TLS, ACL, scripting, transactions, pub/sub, blocking commands,
  expiration, AOF, replication, or RDB semantics for a benchmark.
- No public speed-parity claim from a private experimental mode.

## Required Gates

Every implementation packet in this family must keep:

- `bash harness/oracle/smoke.sh --skip-build` green;
- `cargo check --workspace` green;
- relevant unit tests green;
- profile-matrix evidence updated after correctness passes.

The canary corpus should be expanded before the first real owner-loop
migration so compatibility risks are visible in the oracle, not just prose.

## Current Hypothesis

The latest optimization log moved deep-pipeline `GET` from roughly 221k req/s
to roughly 2.17M req/s, but the profile-matrix median barely moved on the
last dispatch-table cleanup. That suggests local command-lookup overhead is no
longer the main wall. The remaining gap is likely ownership and scheduling:
per-client threads, blocking reads, writer-thread handoff for special clients,
and shared state coordination.

## Packet Boundary

The unattended run is intentionally conservative:

1. record baseline oracle and benchmark evidence;
2. refine this architecture map;
3. add runtime-owner canaries;
4. add an inert typed scaffold;
5. rerun oracle and benchmarks.

The real owner-loop migration is a follow-up human-reviewed packet family.
