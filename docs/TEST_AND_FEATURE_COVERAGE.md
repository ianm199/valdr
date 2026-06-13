# Test And Feature Coverage

Last regenerated: 2026-06-03 from the current single-node TCL wrapper,
dashboard, and AOF telemetry artifacts listed below.

This is the correctness coverage source of truth. It distinguishes runtime
counted assertions from static upstream `test { ... }` source blocks, and it
keeps single-node alpha claims separate from replication, cluster, Sentinel, and
module-C-ABI surfaces.

## Current True Numbers

| Scope | Proven | Total | Status |
|---|---:|---:|---|
| Rust workspace tests | 476 | 476 | Pass; 5 ignored |
| Wire smoke | 23 | 23 | 100% |
| RDB bidirectional oracle | 378 | 378 | 100% |
| Single-node TCL counted assertions | 3,035 | 3,035 | 100% |
| `single_node_core_v1` source blocks | 2,525 | 2,526 | 99.96% |
| Built single-node + AOF source blocks | 2,534 | 2,535 | 99.96% |
| Full upstream TCL source blocks | 2,534 | 4,281 | 59.2%, scoped accounting |

Three denominators are intentionally in use:

- **Counted assertions (3,035 / 3,035):** what upstream
  `test_helper.tcl` reported at runtime in the 54-file wrapper. The one
  no-summary file is a promoted-replica `stream-cgroups` subtest and is not a
  counted single-node failure.
- **Source blocks (2,525 / 2,526):** static upstream `test { ... }` blocks in
  the `single_node_core_v1` dashboard. The only unproven source block is
  `unit/replybufsize.tcl`, which runs to a 0/0 summary under the current tag
  policy.
- **Built single-node + AOF (2,534 / 2,535):** the core source blocks plus
  `unit/aofrw.tcl`'s 9 source blocks. AOF is not part of the core dashboard
  bucket yet, but it is green in the wrapper and correctness-gated as
  single-node alpha.

The full upstream denominator remains lower because Valdr does not claim
cluster, loadable C modules, Sentinel, production HA, or platform-specific
integration in the single-node alpha.

## Current Artifacts

| Artifact | Result |
|---|---|
| `harness/oracle/results/tcl-survey/20260603T160316334341Z/result.json` | 54 files, 3,035 counted passes, 0 counted failures, 0 timeouts, 1 no-summary |
| `harness/oracle/results/tcl-survey/20260603T161606450481Z/unit__type__stream-cgroups.json` | `unit/type/stream-cgroups` default profile: 59 passed, 0 failed |
| `harness/oracle/results/single-node-core-v1/latest.json` | `single_node_core_v1`: 2,525 / 2,526 source blocks proved |
| `harness/oracle/results/tcl-suite-inventory/latest.json` | Full upstream inventory: 247 files / 4,281 source blocks |
| `harness/bench/results/20260603T162047Z-a580d88-aof-matrix.json` | AOF quick matrix: 16 / 16 pass, telemetry only |
| `harness/bench/results/20260603T162115Z-a580d88-aof-rewrite-latency.json` | AOF rewrite latency: 1 / 1 pass, restart verified, telemetry only |

## Conformance At A Glance

```
FULL UPSTREAM TCL SUITE - 4,281 source test blocks
  proven built single-node + AOF      2,534 / 4,281  59.2%
  out-of-scope or later buckets       1,747 / 4,281  40.8%

SINGLE-NODE CORE - 2,526 source blocks
  proved                              2,525 / 2,526  99.96%
  zero-count frontier                     1 / 2,526   0.04%

COUNTED TCL ASSERTIONS IN 54-FILE WRAPPER
  passed                              3,035 / 3,035  100.0%

INDEPENDENT ORACLES
  Rust workspace tests                  476 / 476    pass
  wire-diff smoke                        23 / 23     pass
  RDB bidirectional                     378 / 378    pass
```

## Full Upstream Buckets

The dashboard buckets the full upstream suite as follows. These are source-test
denominators, not runtime assertion counts.

| Bucket | Files | Source tests | Product status |
|---|---:|---:|---|
| `single_node_core_v1` | 52 | 2,526 | Built scope; 2,525 proved |
| `persistence_next` | 1 | 9 | `unit/aofrw`; green in wrapper, still bucketed outside core |
| `robustness_later` | 1 | 1 | `unit/fuzzer`; green |
| `module_strategy_later` | 50 | 587 | Loadable C module ABI not implemented |
| `cluster_later` | 61 | 562 | Cluster mode not implemented |
| `integration_next` | 33 | 464 | Replication / CLI / integration runner, separate from single-node |
| `sentinel_later` | 24 | 100 | Sentinel HA not implemented |
| `platform_later` | 4 | 31 | TLS/I/O-thread/MPTCP/OOM platform tests deferred |
| `harness_files` | 19 | 1 | Upstream harness/support files |
| `unclassified` | 2 | 0 | No product source blocks |
| **Total** | **247** | **4,281** | |

### Sentinel Sub-Buckets

`sentinel_later` is split in
[`SENTINEL_INVENTORY.md`](SENTINEL_INVENTORY.md) so future HA work can move
one lane at a time without implying full Sentinel support.

| Sentinel lane | Files | Source tests |
|---|---:|---:|
| Discovery and read-only introspection | 2 | 23 |
| Deprecated command aliases | 1 | 4 |
| Config and rewrite | 4 | 20 |
| Replica topology reconfiguration | 2 | 10 |
| Quorum and down detection | 4 | 12 |
| Manual failover and selection | 4 | 19 |
| Auth, ACL, and debug | 2 | 4 |
| Harness/includes | 5 | 8 |
| **Total** | **24** | **100** |

## Known Non-Blockers

| Surface | Classification | Evidence |
|---|---|---|
| `unit/replybufsize.tcl` | Harness/tag-policy frontier | Reaches a 0/0 summary; this is the only unproved `single_node_core_v1` source block. |
| `unit/type/stream-cgroups.tcl` no-summary in the 54-file wrapper | Replication/HA scope, not single-node | Abort is `Consumer group last ID propagation to slave`; default single-node profile passes 59/59. |
| Replication / PSYNC / Sentinel HA | Alpha, outside single-node alpha promise | `unit/wait` is green, but production HA and partial resync are not release claims. |
| Cluster and loadable C modules | Not implemented by design for this alpha | Bucketed out of the full upstream denominator. |

## AOF Status

AOF is now **single-node alpha, correctness-gated**. It should not be described
as production HA or universal filesystem-durability proof.

Current evidence:

- `unit/aofrw`: 22 / 22 in the 54-file wrapper.
- AOF correctness kit: 18 / 18 in `cargo test --workspace`.
- Quick AOF matrix: `20260603T162047Z-a580d88-aof-matrix.json`, 16 / 16 pass.
  Worst quick-run overhead is `appendfsync always`, `incr`, pipeline 1:
  99.452% throughput overhead; pipeline 16 records 3,889.5 rps for `incr` and
  3,667.5 rps for `set`.
- Rewrite latency: `20260603T162115Z-a580d88-aof-rewrite-latency.json`, 1 / 1
  pass. Dataset 250, 400 acknowledged writes, restart passed, manifest found,
  snapshot 822 keys in 46 us, rewrite command/start block 8.750 ms, post-reply
  wall 43.051 ms, rewrite wall 51.801 ms, during p99 3.913 ms, during p100
  8.740 ms.

Those performance numbers are telemetry from an Apple M3 Max development host
on a dirty tree after a release rebuild. Repeat on an isolated benchmark host
before publishing stronger performance or durability claims.

## Feature Coverage Matrix

"Implemented" means the single-node runner or an independent oracle covers the
row with zero counted failures in the current artifacts. It does not imply
full-suite integration, cluster, Sentinel, platform, or C module coverage.

| Feature area | State | Evidence |
|---|---|---|
| RESP2 / RESP3 protocol | Implemented | `unit/protocol`, `unit/networking`, wire smoke 23/23. |
| Strings and numerics | Implemented | `unit/type/string`, `unit/type/incr`. |
| Lists | Implemented | `unit/type/list`, `list-2`, `list-3`. |
| Hashes + hash field expiry | Implemented | `unit/type/hash`, `unit/hashexpire`. |
| Sets | Implemented | `unit/type/set`; RDB set corpus both directions. |
| Sorted sets | Implemented | `unit/type/zset`; RDB zset corpus both directions. |
| Streams + consumer groups | Implemented single-node | `unit/type/stream`; `stream-cgroups` default profile 59/59. |
| Bitmaps / bitfield | Implemented | `unit/bitops`, `unit/bitfield`. |
| HyperLogLog | Implemented | `unit/hyperloglog`. |
| Geo | Implemented | `unit/geo`. |
| Transactions | Implemented | `unit/multi`. |
| Lua scripting | Implemented | `unit/scripting`. |
| Functions / `FCALL` correctness | Implemented | `unit/functions`; performance tracked separately. |
| Pub/Sub + sharded Pub/Sub | Implemented single-node | `unit/pubsub`, `unit/pubsubshard`. |
| Auth / ACL | Implemented | `unit/auth`, `unit/acl`, `unit/acl-v2`. |
| Introspection / COMMAND / INFO / SLOWLOG | Implemented | `unit/introspection*`, `unit/info*`, `unit/commandlog`, `unit/slowlog`. |
| Expiration / TTL | Implemented | `unit/expire`. |
| Maxmemory / eviction | Implemented | `unit/maxmemory`, `unit/client-eviction`. |
| Lazy freeing | Implemented | `unit/lazyfree`. |
| RDB persistence | Oracle-gated | 378 / 378 bidirectional checks. |
| AOF | Single-node alpha, correctness-gated | `unit/aofrw` 22/22, AOF kit 18/18, matrix/rewrite telemetry current. |
| Replication / HA | Alpha, not single-node release claim | `unit/wait` is green; PSYNC/full-resync behavior remains roadmap. |
| Cluster | Not implemented | `cluster_later`: 562 source tests. |
| Sentinel | Not implemented | `sentinel_later`: 100 source tests. |
| TLS / I/O threads / MPTCP / platform | Deferred / separate platform surface | `platform_later`: 31 source tests. |
| Loadable C modules | Not implemented by design | `module_strategy_later`: 587 source tests. |

## Authoritative Commands

```bash
cargo build --release -p redis-server
cargo test --workspace
bash harness/oracle/smoke.sh --skip-build
python3 harness/oracle/rdb-diff --direction=all
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build \
  --timeout-s 220 --baseport 30000 --portcount 8000
python3 harness/oracle/tcl-suite-inventory.py
python3 harness/oracle/single-node-core-dashboard.py
python3 harness/bench/aof-matrix.py --quick --targets rust --skip-build
python3 harness/bench/aof-rewrite-latency.py --quick --targets rust --skip-build
```

Focused stream consumer-group proof:

```bash
python3 harness/oracle/tcl-survey.py \
  --runner-id stream-cgroups-single-node-subset \
  --profile default \
  --timeout-s 220 \
  --baseport 31000 \
  --portcount 1000 \
  --files unit/type/stream-cgroups \
  --isolated-tests-copy \
  --skip-build
```

## Source-Of-Truth Order

When numbers disagree, trust in this order:

1. A fresh `run-single-node-tcl-suite.sh` wrapper artifact.
2. Focused artifacts for explicitly classified files, such as
   `unit/type/stream-cgroups` under `--profile default`.
3. `single-node-core-dashboard.py` for static source-block accounting.
4. `tcl-suite-inventory.py` for full-suite discovery and bucket sizes.
5. This document.
6. README/site summaries.
7. Historical logs.

`harness/oracle/results/tcl-suite-inventory/latest.json` is not a fresh
pass/fail claim by itself. It merges the latest per-file logs across runs and
is used here only for suite discovery and bucket denominators.
