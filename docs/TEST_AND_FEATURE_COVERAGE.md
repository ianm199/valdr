# Test And Feature Coverage

Last regenerated: 2026-05-27 from canonical single-node TCL run
`20260527T201327757399Z`.

This is the single source-of-truth document for correctness coverage. The repo
used to carry several overlapping summaries that mixed different denominators
and quoted numbers without a run id; those are gone. Use this doc to decide
what we can claim, which command generates each number, and which fresh
artifact backs it.

## Current True Numbers

Every row is reproduced by a documented command below.

| Scope | Proven | Total | Status |
|---|---:|---:|---|
| Rust workspace tests | 405 | 405 | 100% (5 ignored) |
| Wire smoke | 23 | 23 | 100% |
| RDB bidirectional oracle | 378 | 378 | 100% |
| Single-node TCL counted assertions | 3015 | 3015 | 100% |
| Single-node core source blocks | 2466 | 2541 | 97.0% |
| Full upstream TCL source blocks | 2466 | 4299 | 57.4%, includes out-of-scope buckets |

Three numbers, three different denominators, all true at once:

- **Counted assertions (3015 / 3015):** what upstream `test_helper.tcl` reported
  at runtime across the 54-file single-node wrapper. Zero failures.
- **Single-node source blocks (2466 / 2541):** static `test {` blocks in the
  54 discovered files that ran to a proven pass. The 75 unproven blocks are
  three known files: `unit/aofrw` (9, no-summary), `unit/type/stream-cgroups`
  (65, no-summary under the external profile), and `unit/replybufsize` (1,
  zero-count). Nothing else is unaccounted for.
- **Full upstream (2466 / 4299):** the same proven single-node blocks measured
  against the entire upstream suite, whose remaining surface is bucketed below.

## Conformance At A Glance

Bars visualize the exact numbers tabulated in this document (single-node run
`20260527T201327757399Z` + the `single-node-core-dashboard.py` projection).
Within the scope we build (single-node core) we are at 97% proven / 100% of
counted assertions; the full-suite bar reads lower only because ~41% of the
4,299-block denominator is cluster/modules/sentinel/integration we have
deliberately not built.

```
FULL UPSTREAM TCL SUITE — 4,299 source test blocks
  ███████████████████▓░░░░░░░░░░░░░░  2,466 proven of 4,299
  █ proven 2,465   ▓ in-scope pending 66   ░ not built (out of scope) 1,768

COVERAGE WITHIN EACH BUCKET (proven of that bucket's own tests)
  single-node core (built)    █████████████████████████████████░  2465 / 2531   97%
  modules / C ABI             ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░     0 / 587     0%   not built (by design)
  cluster                     ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░     0 / 564     0%   not built
  integration (repl/AOF/CLI)  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░     0 / 473     0%   separate runner; not gated
  sentinel                    ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░     0 / 100     0%   not built
  platform (TLS/iothr/MPTCP)  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░     0 / 33      0%   deferred
  persistence frontier        ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░     0 / 9       0%   unit/aofrw, alpha
  robustness (fuzzer)         ██████████████████████████████████     1 / 1     100%

SINGLE-NODE CORE — 2,541 source blocks (the scope we build)
  proven                      █████████████████████████████████░ 2,466 / 2,541  97.0%
  counted assertions          ██████████████████████████████████ 3,015 / 3,015  100.0%

SINGLE-NODE CORE BY SUBSYSTEM (proven source blocks)
  auth/config/introspection   ██████████████████████████████████ 436 / 436  100.0%
  execution                   ██████████████████████████████████ 450 / 450  100.0%
  keyspace/memory             ██████████████████████████████████ 524 / 524  100.0%
  protocol/client             ██████████████████████████████████ 125 / 126   99.2%
  data types                  ████████████████████████████████░░ 930 / 995   93.5%

INDEPENDENT ORACLES
  Rust workspace tests        ██████████████████████████████████ 405 / 405  100.0%
  wire-diff smoke             ██████████████████████████████████  23 / 23   100.0%
  RDB bidirectional           ██████████████████████████████████ 378 / 378  100.0%
```

## Authoritative Commands

```bash
cargo test --workspace
bash harness/oracle/smoke.sh --skip-build
python3 harness/oracle/rdb-diff --direction=all
bash harness/oracle/run-single-node-tcl-suite.sh --timeout-s 180 --baseport 30000 --portcount 8000
```

Integration tests are **separate** and must be run explicitly, e.g.:

```bash
python3 harness/oracle/tcl-survey.py \
  --files integration/replication-2 \
  --no-default-deny-tags \
  --skip-build \
  --isolated-tests-copy \
  --timeout-s 180 \
  --baseport 43000 \
  --portcount 8000 \
  --quiet
```

## Source-Of-Truth Order

When numbers disagree, trust in this order:

1. A fresh `run-single-node-tcl-suite.sh` artifact under
   `harness/oracle/results/tcl-survey/<run-id>/result.json`.
2. The wrapper command that produced that artifact.
3. The `single-node-core-dashboard.py` projection of the *latest* per-file
   evidence.
4. This document.
5. Static suite config and inventory scripts.
6. Historical logs and README tables.

### `tcl-suite-inventory/latest.*` is NOT fresh truth

`harness/oracle/results/tcl-suite-inventory/latest.json` merges the *latest log
per file across all runs* — including stale logs left behind by focused or
contained (`safe-survey.py`) runs. It therefore reports phantom failures and
timeouts that a clean wrapper run does not have (e.g. a regenerate immediately
after the clean `20260527T201327757399Z` run still showed `3023 / 3028` with
5 fails and 2 timeouts that did not occur in the wrapper). Use it only as a
file-discovery / bucketing index, never as a pass/fail claim. The pass/fail
claim is the wrapper `result.json`.

## Definitions

| Term | Meaning |
|---|---|
| Full upstream TCL suite | Every `.tcl` file under `reference/valkey/tests`. Inventory: ~245 product test files, **4,299** literal source `test` blocks. |
| Single-node wrapper | `run-single-node-tcl-suite.sh`. Discovers 54 `unit/*.tcl` and `unit/type/*.tcl` files, excluding TLS, MPTCP, I/O-thread, and OOM-score infra files. |
| Source test block | A literal upstream line beginning with `test {` or `test "`. A static denominator. |
| Counted assertion | What upstream `test_helper.tcl` reports in `Test Summary` at runtime. After tags, loops, and generated subtests it can be higher or lower than source blocks. |
| `no-summary` | The file did not reach a final `Test Summary` (uncaught runtime error, connection break, or nested-suite abort). Not a pass and not a counted fail. |
| `zero-count` | The file reached `0 passed, 0 failed` (filtered to zero by tag policy). Not proven. |
| `single_node_core_v1` | Product envelope used by `single-node-core-dashboard.py`: single-node behavior, excluding persistence rewrite, fuzzer, replication, cluster, Sentinel, TLS/I/O-thread platform tests, and loadable-module C ABI. |
| `safe-survey.py` | Conservative contained runner. Useful for disk-risky work; **not** the official single-node number. |

## The Official Single-Node TCL Number

Regenerate:

```bash
cargo build --bin redis-server
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build \
  --timeout-s 180 --baseport 30000 --portcount 8000
```

Latest run:

| Field | Value |
|---|---:|
| Run id | `20260527T201327757399Z` |
| Files discovered | 54 |
| Counted assertions | 3,015 |
| Counted passes | 3,015 |
| Counted failures | 0 |
| Timed-out files | 0 |
| No-summary files | 2 |
| Zero-count files | 1 |

The single-node surface is green except for three explicitly listed files:

| File | Classification | Why |
|---|---|---|
| `unit/aofrw.tcl` | no-summary | AOF rewrite of functions aborts with `ERR Function not found`. Persistence frontier (`persistence_next` bucket); included in the 54-file wrapper but outside `single_node_core_v1`. |
| `unit/type/stream-cgroups.tcl` | no-summary (external profile) | Under `single-node-external` the file enters nested dual-server replication blocks (`NOGROUP` / last-ID-propagation-to-slave) and aborts. Under `--profile default` it passes 59/59. Those blocks belong to the replication suite, not single-node. |
| `unit/replybufsize.tcl` | zero-count | Filtered to `0 / 0` by tag policy; not proven. |

### Fast TCL Iteration Loop

Do not run the full 54-file wrapper while debugging one failure. Build only
after Rust changes, then reuse the binary:

```bash
cargo build --bin redis-server
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build --files unit/lazyfree
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build --files unit/type/stream-cgroups --profile default
```

For the tightest cycle, run one upstream test by name from the Valkey checkout:

```bash
cd reference/valkey
VALKEY_BIN_DIR="$PWD/../../target/debug" \
  tclsh tests/test_helper.tcl \
  --single unit/lazyfree \
  --only "UNLINK can reclaim memory in background" \
  --clients 1 --skip-leaks \
  --baseport 33000 --portcount 4000 \
  --tags "-needs:repl -repl -needs:debug -cluster -needs:cluster" \
  --quiet
```

Use the exact-test command for code iteration, the one-file wrapper for a
checked artifact, and the full wrapper only when producing the number of record.

## Single-Node Per-File Matrix

From the fresh `20260527T201327757399Z` run. `counted` is what the upstream
helper reported at runtime; it differs from source blocks because of tag
filtering, loops, and generated subtests.

| File | Counted pass | Counted fail | Status |
|---|---:|---:|---|
| `unit/acl-v2` | 72 | 0 | pass |
| `unit/acl` | 112 | 0 | pass |
| `unit/aofrw` | - | - | no-summary |
| `unit/auth` | 16 | 0 | pass |
| `unit/bitfield` | 18 | 0 | pass |
| `unit/bitops` | 50 | 0 | pass |
| `unit/client-eviction` | 14 | 0 | pass |
| `unit/commandlog` | 14 | 0 | pass |
| `unit/dump` | 27 | 0 | pass |
| `unit/expire` | 65 | 0 | pass |
| `unit/functions` | 94 | 0 | pass |
| `unit/fuzzer` | 1 | 0 | pass |
| `unit/geo` | 71 | 0 | pass |
| `unit/hashexpire` | 329 | 0 | pass |
| `unit/hyperloglog` | 26 | 0 | pass |
| `unit/info-command` | 5 | 0 | pass |
| `unit/info` | 24 | 0 | pass |
| `unit/introspection-2` | 49 | 0 | pass |
| `unit/introspection` | 113 | 0 | pass |
| `unit/keyspace` | 65 | 0 | pass |
| `unit/latency-monitor` | 12 | 0 | pass |
| `unit/lazyfree` | 4 | 0 | pass |
| `unit/limits` | 1 | 0 | pass |
| `unit/maxmemory` | 30 | 0 | pass |
| `unit/memefficiency` | 5 | 0 | pass |
| `unit/multi` | 48 | 0 | pass |
| `unit/networking` | 5 | 0 | pass |
| `unit/obuf-limits` | 13 | 0 | pass |
| `unit/other` | 27 | 0 | pass |
| `unit/pause` | 20 | 0 | pass |
| `unit/protocol` | 28 | 0 | pass |
| `unit/pubsub` | 35 | 0 | pass |
| `unit/pubsubshard` | 11 | 0 | pass |
| `unit/querybuf` | 2 | 0 | pass |
| `unit/quit` | 3 | 0 | pass |
| `unit/replybufsize` | 0 | 0 | zero-count |
| `unit/scan` | 21 | 0 | pass |
| `unit/scripting` | 420 | 0 | pass |
| `unit/shutdown` | 9 | 0 | pass |
| `unit/slowlog` | 13 | 0 | pass |
| `unit/sort` | 54 | 0 | pass |
| `unit/tracking` | 59 | 0 | pass |
| `unit/violations` | 1 | 0 | pass |
| `unit/wait` | 39 | 0 | pass |
| `unit/type/hash` | 83 | 0 | pass |
| `unit/type/incr` | 31 | 0 | pass |
| `unit/type/list-2` | 2 | 0 | pass |
| `unit/type/list-3` | 11 | 0 | pass |
| `unit/type/list` | 254 | 0 | pass |
| `unit/type/set` | 114 | 0 | pass |
| `unit/type/stream-cgroups` | - | - | no-summary (external profile; 59/59 under `--profile default`) |
| `unit/type/stream` | 73 | 0 | pass |
| `unit/type/string` | 104 | 0 | pass |
| `unit/type/zset` | 318 | 0 | pass |

## Source-Block Coverage

Regenerate after the official TCL run:

```bash
python3 harness/oracle/tcl-suite-inventory.py
python3 harness/oracle/single-node-core-dashboard.py
```

Two source-block accountings, both valid:

| Accounting | Proven | Total | Notes |
|---|---:|---:|---|
| 54-file wrapper | 2,466 | 2,541 | Headline single-node-core number. Unproven 75 = `aofrw` (9) + `stream-cgroups` (65) + `replybufsize` (1). |
| `single_node_core_v1` envelope | 2,465 | 2,531 | Excludes `aofrw` and `fuzzer`. The +1 in the wrapper number is `unit/fuzzer` (1, passes). |

`single_node_core_v1` by subsystem (latest dashboard):

| Subsystem | Proved | Pending | Total |
|---|---:|---:|---:|
| Auth/config/introspection | 436 | 0 | 436 |
| Data types | 930 | 65 | 995 |
| Execution | 450 | 0 | 450 |
| Keyspace/memory | 524 | 0 | 524 |
| Protocol/client | 125 | 1 | 126 |

The 65 pending data-type blocks are the `stream-cgroups` external-replication
body; the 1 pending protocol block is `replybufsize` zero-count.

## Full Upstream TCL Suite

The 4,299-block denominator stays honest by bucketing everything the
single-node runner does not cover. These are not failures; they are surface we
have not built a runner or product claim for.

| Bucket | Source tests | Meaning |
|---|---:|---|
| `single_node_core_v1` | 2,531 | Single-node Redis/Valkey behavior (built scope). |
| Module C ABI (`module_strategy_later`) | 587 | Loadable `.so` modules. Not implemented by design. |
| Cluster (`cluster_later`) | 564 | Cluster mode. |
| Integration (`integration_next`) | 473 | Replication / AOF / RDB / CLI / benchmark integration. |
| Sentinel (`sentinel_later`) | 100 | Sentinel HA. |
| Platform (`platform_later`) | 33 | TLS / I/O-threads / MPTCP / OOM. |
| Persistence frontier (`persistence_next`) | 9 | `unit/aofrw`. |
| Robustness frontier (`robustness_later`) | 1 | `unit/fuzzer`. |
| Harness/support (`harness_files`) | 1 | Non-product harness test. |
| Total | 4,299 | |

Non-single-node buckets sum to 1,768; `4,299 − 1,768 = 2,531`.

## Independent Oracles

These do not depend on the TCL suite.

### Rust Workspace Tests

```bash
cargo test --workspace
```

405 passed, 0 failed, 5 ignored across the workspace crates.

### Wire-Diff Smoke

```bash
bash harness/oracle/smoke.sh --skip-build
```

23 / 23 byte-exact RESP scripts. Sends fixed RESP corpora to upstream Valkey and
to this server and compares raw replies byte-for-byte.

### RDB Bidirectional Oracle

```bash
python3 harness/oracle/rdb-diff --direction=all
```

| Corpus | Checks |
|---|---:|
| `01-strings-basic` | 28 |
| `02-strings-edge` | 34 |
| `03-hashes` | 72 |
| `04-sets` | 56 |
| `05-lists` | 62 |
| `06-zsets` | 72 |
| `07-streams` | 54 |
| Total | 378 / 378 pass |

Directions A (we save → C loads) and B (C saves → we load) are gating.
Direction C compares raw bytes and is informational — compatible RDB encodings
can differ while loading to the same logical keyspace.

## Feature Coverage Matrix

"Green" means the single-node TCL runner or an independent oracle covers the
row with zero counted failures in the latest run. It does not mean every
full-suite integration, cluster, Sentinel, platform, or module scenario exists.

| Feature area | State | Evidence |
|---|---|---|
| RESP2 / RESP3 protocol | Implemented | `unit/protocol` 28/0, `unit/networking` 5/0, wire smoke 23/23. |
| Strings and numerics | Implemented | `unit/type/string` 104/0, `unit/type/incr` 31/0. |
| Lists | Implemented | `unit/type/list` 254/0, `list-2` 2/0, `list-3` 11/0. |
| Hashes + hash field expiry | Implemented | `unit/type/hash` 83/0, `unit/hashexpire` 329/0. |
| Sets | Implemented | `unit/type/set` 114/0; RDB set corpus both directions. |
| Sorted sets | Implemented | `unit/type/zset` 318/0; RDB zset corpus both directions. |
| Streams | Implemented single-node | `unit/type/stream` 73/0; `stream-cgroups` 59/0 under `--profile default`. Cross-server replication of streams is in the replication suite. |
| Bitmaps / bitfield | Implemented | `unit/bitops` 50/0, `unit/bitfield` 18/0. |
| HyperLogLog | Implemented | `unit/hyperloglog` 26/0. |
| Geo | Implemented | `unit/geo` 71/0. |
| Transactions | Implemented | `unit/multi` 48/0. |
| Lua scripting | Implemented | `unit/scripting` 420/0. |
| Functions / `FCALL` correctness | Implemented | `unit/functions` 94/0. Performance tracked separately. |
| Pub/Sub + sharded Pub/Sub | Implemented single-node | `unit/pubsub` 35/0, `unit/pubsubshard` 11/0. |
| Auth / ACL | Implemented | `unit/auth` 16/0, `unit/acl` 112/0, `unit/acl-v2` 72/0. |
| Introspection / COMMAND / INFO / SLOWLOG | Implemented | `unit/introspection*`, `unit/info*`, `unit/commandlog` 14/0, `unit/slowlog` 13/0. |
| Expiration / TTL | Implemented | `unit/expire` 65/0. |
| Maxmemory / eviction | Implemented | `unit/maxmemory` 30/0, `unit/client-eviction` 14/0. |
| Lazy freeing | Implemented | `unit/lazyfree` 4/0. |
| RDB persistence | Oracle-gated | 378/378 bidirectional checks. |
| AOF | Alpha, not release-gated | `unit/aofrw` no-summary; `persistence_next` bucket. |
| Replication / HA | Alpha, not conformance-gated | `unit/wait` 39/0; full replication/Sentinel suites are out of single-node scope. |
| Cluster | Not implemented | `cluster_later`: 564 source tests. |
| Sentinel | Not implemented | `sentinel_later`: 100 source tests. |
| TLS / I/O threads / MPTCP / platform | Deferred | `platform_later`: 33 source tests. |
| Loadable C modules | Not implemented by design | `module_strategy_later`: 587 source tests. Native RedisJSON/RedisBloom subsets are separate Rust impls, not C ABI. |

## Reporting Rules

Every TCL number published anywhere in this repo must state:

- the file list or runner id;
- whether it is a fresh run or historical;
- the tag deny policy (the single-node profile denies `needs:repl`,
  `needs:debug`, cluster, and external-replication tags, resolved from
  `tcl-survey.py`'s `DENY_TAG_PROFILES`);
- counted passes and failures;
- timeout / no-summary / zero-count files;
- whether it is a scoped claim, telemetry, or full-suite accounting.

A number that says only "TCL: N pass" without those qualifiers is ambiguous —
fix the document. Do not publish a counted total (e.g. `3010 / 3010`) unless a
fresh `run-single-node-tcl-suite.sh` artifact shows it; the wrapper run id is
the proof.

## Reproduce Everything

```bash
cargo build --bin redis-server
cargo test --workspace

bash harness/oracle/run-single-node-tcl-suite.sh --skip-build \
  --timeout-s 180 --baseport 30000 --portcount 8000
python3 harness/oracle/tcl-suite-inventory.py
python3 harness/oracle/single-node-core-dashboard.py

bash harness/oracle/smoke.sh --skip-build
python3 harness/oracle/rdb-diff --direction=all
```

Inspect the latest wrapper artifact (the authoritative pass/fail):

```bash
jq '.evidence.summary' harness/oracle/results/tcl-survey/*/result.json
jq '.core' harness/oracle/results/single-node-core-v1/latest.json
```

List the official file set:

```bash
bash harness/oracle/run-single-node-tcl-suite.sh --list-files | tr ',' '\n'
```
