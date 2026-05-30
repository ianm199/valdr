# Coverage

Valkey's own test suite, not ours. Verified against [Valkey 9.1.0](https://github.com/valkey-io/valkey/releases/tag/9.1.0), 2026-05-28.

## Numbers

| Measure | Proven | Total | |
|---|--:|--:|---|
| Counted assertions, single-node | 3,015 | 3,015 | **100%** |
| Single-node core blocks | 2,466 | 2,541 | **97%** |
| Full upstream suite | 2,466 | 4,299 | 57% |

*Counted assertions*: what upstream `test_helper.tcl` reports at runtime. *Source blocks*: a static count of `test {…}` blocks. The full-suite number is lower because ~41% of upstream tests cover features we don't build — unbuilt, not failing.

## In scope — single-node core, 97%

| Subsystem | Proven | Total | |
|---|--:|--:|---|
| Keyspace & memory | 524 | 524 | 100% |
| Execution | 450 | 450 | 100% |
| Auth / config / introspection | 436 | 436 | 100% |
| Protocol / client | 125 | 126 | 99% |
| Data types | 930 | 995 | 94% |

## Out of scope

0% means not built, not failing.

| Bucket | Tests | |
|---|--:|---|
| Module C ABI | 587 | loadable `.so` modules, by design |
| Cluster | 564 | not built |
| Integration — replication / AOF / CLI | 473 | separate runner, not gated |
| Sentinel | 100 | not built |
| Platform — TLS / I/O-threads / MPTCP / OOM | 33 | deferred |
| Persistence frontier — `aofrw` | 9 | alpha |
| Robustness — `fuzzer` | 1 | passing |

## Independent oracles

Not the Tcl suite.

| Oracle | Proven | Total |
|---|--:|--:|
| Rust workspace tests | 405 | 405 |
| Wire-diff smoke | 23 | 23 |
| RDB bidirectional | 378 | 378 |

## Per file

54 single-node files, counted passing assertions.

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
| introspection-2 | 49 | multi | 48 |
| wait | 39 | pubsub | 35 |
| type/incr | 31 | maxmemory | 30 |
| protocol | 28 | dump | 27 |
| other | 27 | hyperloglog | 26 |
| info | 24 | scan | 21 |
| pause | 20 | bitfield | 18 |
| auth | 16 | client-eviction | 14 |
| commandlog | 14 | obuf-limits | 13 |
| slowlog | 13 | latency-monitor | 12 |
| type/list-3 | 11 | pubsubshard | 11 |
| shutdown | 9 | info-command | 5 |
| networking | 5 | memefficiency | 5 |
| lazyfree | 4 | quit | 3 |
| querybuf | 2 | type/list-2 | 2 |
| limits | 1 | violations | 1 |
| fuzzer | 1 | | |

Not counted: `aofrw` (AOF rewrite, alpha), `stream-cgroups` (59/59 on the default profile; aborts only under the external dual-server profile), `replybufsize` (filtered to 0/0 by tag policy).
