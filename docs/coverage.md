# Coverage

Valkey's own test suite, not ours. Verified against [Valkey 9.1.0](https://github.com/valkey-io/valkey/releases/tag/9.1.0), 2026-06-03.

## Numbers

| Measure | Proven | Total | |
|---|--:|--:|---|
| Counted assertions, 54-file wrapper | 3,035 | 3,035 | **100%** |
| Single-node core blocks | 2,525 | 2,526 | **99.96%** |
| Built single-node + AOF blocks | 2,534 | 2,535 | **99.96%** |
| Full upstream suite | 2,534 | 4,281 | 59% |

*Counted assertions*: what upstream `test_helper.tcl` reports at runtime. *Source blocks*: a static count of `test {…}` blocks. The full-suite number is lower because ~41% of upstream tests cover features we do not claim yet: cluster, modules, Sentinel, HA replication, and platform-specific integration.

## In scope — single-node core, 99.96%

| Subsystem | Proven | Total | |
|---|--:|--:|---|
| Keyspace & memory | 518 | 518 | 100% |
| Execution | 451 | 451 | 100% |
| Auth / config / introspection | 436 | 436 | 100% |
| Protocol / client | 125 | 126 | 99% |
| Data types | 995 | 995 | 100% |

`stream-cgroups` (consumer groups) passes 59/59 under the default profile; a few of its subtests exercise *replication* (NOGROUP-to-replica) and are counted in the replication bucket, not here. The only unproven single-node core source block is `replybufsize`, which reaches a 0/0 summary under the current tag policy. `unit/aofrw` is now green in the wrapper run and counted separately as single-node AOF coverage.

## Out of scope

0% means not built, not failing.

| Bucket | Tests | |
|---|--:|---|
| Module C ABI | 587 | loadable `.so` modules, by design |
| Cluster | 562 | not built |
| Integration — replication / CLI | 464 | separate runner, not gated |
| Sentinel | 100 | not built |
| Platform — TLS / I/O-threads / MPTCP / OOM | 31 | deferred |
| Persistence frontier — `aofrw` | 9 | green in wrapper; still tracked outside `single_node_core_v1` |
| Robustness — `fuzzer` | 1 | passing |

## Independent oracles

Not the Tcl suite.

| Oracle | Proven | Total |
|---|--:|--:|
| Rust workspace tests | 476 | 476 |
| Wire-diff smoke | 23 | 23 |
| RDB bidirectional | 378 | 378 |

## Latest evidence

| Artifact | Result |
|---|---|
| `harness/oracle/results/tcl-survey/20260603T160316334341Z/result.json` | 54 files, 3,035 counted passes, 0 counted failures, 0 timeouts, 1 no-summary |
| `harness/oracle/results/tcl-survey/20260603T161606450481Z/unit__type__stream-cgroups.json` | `unit/type/stream-cgroups` default profile: 59/59 |
| `harness/oracle/results/single-node-core-v1/latest.json` | 2,525 / 2,526 source blocks proved in `single_node_core_v1` |

Not counted in `single_node_core_v1`: `aofrw` (AOF rewrite, 22/22 in the wrapper) and `fuzzer` (1/1). `replybufsize` remains the only unproven core source block because it is filtered to 0/0 by tag policy. `stream-cgroups` is counted in the core dashboard because it passes 59/59 under the default profile; the no-summary path in the 54-file external wrapper is a promoted-replica subtest and belongs to replication/HA scope.

## Reproduce

The tests are Valkey's own, under `reference/valkey/tests/`. Run them against Valdr with the [test harness](https://github.com/ianm199/valdr/tree/main/harness/oracle):

```
# build Valdr + run the full single-node suite (54 files)
bash harness/oracle/run-single-node-tcl-suite.sh

# one file
make oracle FILES=unit/type/zset

# consumer groups, under the default profile
python3 harness/oracle/tcl-survey.py --profile default \
  --files unit/type/stream-cgroups --isolated-tests-copy
```
