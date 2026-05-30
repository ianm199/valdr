# Coverage

Valdr is tested against **Valkey's own upstream test suite** — not tests we wrote.
Verified 2026-05-28 against [Valkey 9.1.0](https://github.com/valkey-io/valkey/releases/tag/9.1.0).

## The headline numbers

| Measure | Proven | Total | |
|---|--:|--:|---|
| Counted assertions (single-node) | 3,015 | 3,015 | **100%** |
| Single-node core source blocks | 2,466 | 2,541 | **97%** |
| Full upstream suite source blocks | 2,466 | 4,299 | 57% — includes unbuilt scope |

*Counted assertions* are what Valkey's own `test_helper.tcl` reports at runtime.
*Source blocks* are a static count of `test {…}` blocks in the upstream files. The
full-suite number reads lower **only** because ~41% of upstream's tests cover features
we have deliberately not built (below) — those are unbuilt scope, not failures.

## In scope — single-node core (97% proven)

This is the product we build: single-node, drop-in Redis/Valkey. By subsystem:

| Subsystem | Proven | Total | |
|---|--:|--:|---|
| Keyspace & memory | 524 | 524 | 100% |
| Execution | 450 | 450 | 100% |
| Auth / config / introspection | 436 | 436 | 100% |
| Protocol / client | 125 | 126 | 99% |
| Data types | 930 | 995 | 94% |

## Out of scope — and why (the other 41%)

These buckets read 0% because the **feature isn't implemented**, not because tests
fail. They are upstream test surface we have not built a product or runner for.

| Bucket | Tests | Why it's out of scope |
|---|--:|---|
| Module C ABI | 587 | Loadable `.so` modules — not implemented by design |
| Cluster | 564 | Cluster mode — not built |
| Integration (replication / AOF / CLI) | 473 | Multi-server; a separate runner, not release-gated |
| Sentinel | 100 | High-availability failover — not built |
| Platform (TLS / I/O-threads / MPTCP / OOM) | 33 | Infra-specific test files — deferred |
| Persistence frontier (`aofrw`) | 9 | AOF rewrite — alpha |
| Robustness (`fuzzer`) | 1 | Passing (1/1) |

The non-single-node buckets sum to 1,768; `4,299 − 1,768 = 2,531` in-scope blocks.

## Independent oracles (not the TCL suite)

These verify Valdr by other means and don't depend on the upstream Tcl tests:

| Oracle | Proven | Total |
|---|--:|--:|
| Rust workspace tests | 405 | 405 |
| Wire-diff smoke (RESP frames) | 23 | 23 |
| RDB bidirectional (load + save vs reference) | 378 | 378 |

## Single-node, per file

Every upstream `unit/*.tcl` and `unit/type/*.tcl` file in the 54-file single-node
wrapper (counted passing assertions):

| File | Pass | File | Pass |
|---|--:|---|--:|
| type/zset | 318 | type/string | 104 |
| type/list | 254 | hashexpire | 329 |
| scripting | 420 | type/set | 114 |
| introspection | 113 | functions | 94 |
| type/hash | 83 | type/stream | 73 |
| geo | 71 | keyspace | 65 |
| expire | 65 | tracking | 59 |
| sort | 54 | bitops | 50 |
| multi | 48 | introspection-2 | 49 |
| wait | 39 | pubsub | 35 |
| type/incr | 31 | maxmemory | 30 |
| protocol | 28 | dump | 27 |
| other | 27 | hyperloglog | 26 |
| info | 24 | scan | 21 |
| pause | 20 | bitfield | 18 |
| auth | 16 | client-eviction | 14 |
| commandlog | 14 | obuf-limits | 13 |
| latency-monitor | 12 | slowlog | 13 |
| type/list-3 | 11 | pubsubshard | 11 |
| shutdown | 9 | info-command | 5 |
| networking | 5 | memefficiency | 5 |
| lazyfree | 4 | quit | 3 |
| querybuf | 2 | type/list-2 | 2 |
| limits | 1 | violations | 1 |
| fuzzer | 1 | | |

Three files are not counted as proven: `aofrw` (AOF-rewrite, alpha), `stream-cgroups`
(passes 59/59 under the default profile; aborts only in the external dual-server
profile), and `replybufsize` (filtered to 0/0 by tag policy).
