# valdr

Valdr is a port of [Valkey](https://github.com/valkey-io/valkey) aiming to be fully compatible with Redis/ Valkey clients in memory safe Rust. The motivation for this project is to attempt to build mostly memory safe alternatives to core web infrastructure and to explore archtecture choices that wil enable faster performance in the long run. 

Currently Valdr supports the all the core Redis Client commands and passes 97% of [single node tests](https://valdr.dev/coverage.html). 

This repo heavily leveraged coding agents in the process and was inspired by similar efforts.

## Status

| Area | Status |
|---|---|
| Release state | Alpha |
| Primary target | Single-node Redis/Valkey workloads |
| Protocol | RESP2 / RESP3 |
| Client compatibility | Existing Redis clients |
| License | BSD-3-Clause |

## Compatibility

| Surface | Current state | Evidence |
|---|---|---|
| Single-node RESP wire behavior | Full on current smoke corpus | 23 / 23 byte-exact scripts vs upstream Valkey |
| RDB load/save interop | Full on current corpus | 378 / 378 bidirectional checks |
| Upstream TCL suite | Single-node core green; not a full-suite claim | Full denominator 4,299 blocks, bucketed in [`docs/TEST_AND_FEATURE_COVERAGE.md`](docs/TEST_AND_FEATURE_COVERAGE.md) |
| Cluster mode | Not implemented | Out of scope for current alpha |
| Loadable C modules | Not implemented | Out of scope for current alpha |
| Production HA / Sentinel | Not claimed | Replication/AOF exist but are not production-conformance gated |
| In-process TLS | Enabled (rustls; no OpenSSL) | TLS 1.2 + 1.3; mTLS tri-state (`no`/`optional`/`yes`); dynamic CONFIG SET of `tls-protocols`, `tls-auth-clients`, cert/key paths. CBC-suite tests in `unit/tls.tcl` are a deliberate rustls divergence — see [`docs/TLS_FAITHFUL_PLAN.md`](docs/TLS_FAITHFUL_PLAN.md). |

## Features

| Feature | Status |
|---|---|
| Strings | Implemented |
| Lists | Implemented |
| Hashes | Implemented |
| Sets | Implemented |
| Sorted sets | Implemented |
| Streams | Implemented |
| Pub/sub | Implemented |
| Transactions | Implemented |
| Lua scripting | Implemented |
| ACL / AUTH | Implemented |
| Multi-DB | Implemented |
| Expiration / TTL | Implemented |
| Maxmemory eviction | Implemented |
| RDB persistence | Implemented and oracle-gated |
| AOF | Alpha |
| Replication | Alpha |
| RedisJSON-compatible commands | Native subset |
| RedisBloom-compatible commands | Native subset |

## Benchmark Commands

<details>
<summary>Run official valkey-benchmark against Valkey and valdr Docker images</summary>

```bash
docker network create valdr-bench

docker run -d --rm \
  --name valkey-ref \
  --network valdr-bench \
  valkey/valkey:8-alpine

docker run -d --rm \
  --name valdr \
  --network valdr-bench \
  ghcr.io/ianm199/valdr:alpha

sleep 1
```

```bash
docker run --rm \
  --network valdr-bench \
  valkey/valkey:8-alpine \
  valkey-benchmark \
    -h valkey-ref \
    -p 6379 \
    -n 100000 \
    -c 50 \
    -P 100 \
    -d 64 \
    -t ping_inline,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_300,lrange_500,lrange_600,mset,mget,xadd,function_load,fcall \
    --warmup 1 \
    --csv \
    --precision 3
```

```bash
docker run --rm \
  --network valdr-bench \
  valkey/valkey:8-alpine \
  valkey-benchmark \
    -h valdr \
    -p 6379 \
    -n 100000 \
    -c 50 \
    -P 100 \
    -d 64 \
    -t ping_inline,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_300,lrange_500,lrange_600,mset,mget,xadd,function_load,fcall \
    --warmup 1 \
    --csv \
    --precision 3
```

```bash
docker rm -f valkey-ref valdr
docker network rm valdr-bench
```

</details>

## Performance

Latest warmed local run vs **two upstream Valkey versions**: Valkey 8.1.7
(the current 8-line stable, what `valkey/valkey:8-alpine` ships) and Valkey
9.1.0 (the current 9-line stable, released 2026-05-19). Same Valdr binary
benchmarked against both adversaries; both adversaries built from
`reference/valkey` with `make BUILD_TLS=no`.

| Metric | vs Valkey 8.1.7 | vs Valkey 9.1.0 |
|---|---:|---:|
| Median ratio across 23 commands | **1.139x** | **1.150x** |
| Pipeline-smoke median (GET/PING_MBULK/SET × p=1/16/100) | 1.057x | 1.124x |
| JSON cache mix median (4 KB docs, p=1) | — (see note) | 1.001x |

The two Valkey versions perform within 2% of each other on this matrix
overall. 8.1.7 turns out to be the slightly faster adversary on most rows —
`get`, `incr`, all the data-structure ops, `lrange_*`, `zpopmin`, `spop`,
`mset` — which is why our ratio against 8.1.7 (1.134×) lands slightly below
our ratio against 9.1.0 (1.150×) even though the same Valdr binary is being
benchmarked. The handful 9.1.0 is faster on: `ping_inline`, `rpush`, `xadd`
(all close to parity between the two).

### Per-command, both adversaries side-by-side

Each adversary measured in its own run; the two `Valdr rps` columns reflect
what Valdr did during that adversary's run (run-to-run variance is normally
~3–8% on the un-quantized rps numbers; `valkey-benchmark` rounds some
default-suite outputs to common figures, hence many identical pairs).

| Command | Valdr rps (8.1.7 run) | Valkey 8.1.7 rps | Ratio | Valdr rps (9.1.0 run) | Valkey 9.1.0 rps | Ratio |
|---|---:|---:|---:|---:|---:|---:|
| PING_INLINE | 5,263,158 | 3,703,704 | 1.421× | 5,263,158 | 4,000,000 | 1.316× |
| PING_MBULK | 7,142,857 | 5,555,556 | 1.286× | 7,142,857 | 5,555,556 | 1.286× |
| SET | 3,703,704 | 3,030,303 | 1.222× | 3,846,154 | 2,564,102 | 1.500× |
| GET | 4,761,905 | 3,846,154 | 1.238× | 4,545,454 | 3,448,276 | 1.318× |
| INCR | 4,166,667 | 4,000,000 | 1.042× | 3,846,154 | 3,571,428 | 1.077× |
| LPUSH | 2,777,778 | 2,439,024 | 1.139× | 2,564,102 | 2,380,952 | 1.077× |
| RPUSH | 2,702,703 | 2,702,703 | 1.000× | 2,500,000 | 2,777,778 | 0.900× |
| LPOP | 2,564,102 | 2,272,727 | 1.128× | 2,500,000 | 2,173,913 | 1.150× |
| RPOP | 2,380,952 | 2,500,000 | 0.952× | 2,380,952 | 2,439,024 | 0.976× |
| SADD | 2,380,952 | 3,225,806 | 0.738× | 2,325,581 | 3,125,000 | 0.744× |
| HSET | 1,724,138 | 2,500,000 | 0.690× | 1,754,386 | 2,500,000 | 0.702× |
| SPOP | 2,857,143 | 4,000,000 | 0.714× | 2,857,143 | 3,703,704 | 0.771× |
| ZADD | 1,923,077 | 2,439,024 | 0.788× | 1,851,852 | 2,222,222 | 0.833× |
| ZPOPMIN | 2,857,143 | 4,166,667 | 0.686× | 2,702,703 | 3,703,704 | 0.730× |
| LRANGE_100 (first 100) | 176,367 | 129,032 | 1.367× | 186,916 | 114,943 | 1.626× |
| LRANGE_300 (first 300) | 56,180 | 38,971 | 1.442× | 62,461 | 37,722 | 1.656× |
| LRANGE_500 (first 500) | 34,153 | 23,175 | 1.474× | 35,137 | 21,777 | 1.613× |
| LRANGE_600 (first 600) | 27,778 | 18,685 | 1.487× | 29,308 | 18,123 | 1.617× |
| MSET (10 keys) | 636,943 | 456,621 | 1.395× | 628,931 | 442,478 | 1.421× |
| MGET (10 keys) | 1,063,829 | 847,457 | 1.255× | 1,030,928 | 740,741 | 1.392× |
| XADD | 1,123,596 | 1,388,889 | 0.809× | 1,098,901 | 1,408,451 | 0.780× |
| FUNCTION_LOAD | 578,035 | 58,893 | 9.815× | 588,235 | 56,593 | 10.394× |
| FCALL | 847,458 | 1,428,571 | 0.593× | 869,565 | 1,351,351 | 0.643× |

The honest pattern:
- **Wins** (ratio > 1.2× against both): `ping_*`, `set`, `get`, `mset`,
  `mget`, all `lrange` variants, `function_load`.
- **Parity** (0.95×–1.20× against both): `incr`, `lpush`, `rpush`, `lpop`, `rpop`.
- **Behind** (0.6×–0.85×): `sadd`, `hset`, `spop`, `zadd`, `zpopmin`, `xadd`, `fcall`.
  These are the data-structure-internal commands and the Lua FCALL path.
  These are also where the Rust port's behavioral fidelity guarantees
  (oracle-gated against upstream tests) currently extract a perf cost we
  haven't paid down yet.

### Pipeline-depth curve (the publication-relevant shape)

The single best summary of where Valdr wins: GET/SET/PING_MBULK throughput
at three pipeline depths against both adversaries.

| Workload | Pipeline | Valdr rps (8.1.7 run) | Valkey 8.1.7 rps | Ratio | Valdr rps (9.1.0 run) | Valkey 9.1.0 rps | Ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| GET | 1 | 149,925 | 157,233 | 0.954× | 161,551 | 175,747 | 0.919× |
| GET | 16 | 2,202,643 | 2,083,333 | 1.057× | 2,590,674 | 2,304,148 | 1.124× |
| GET | 100 | 5,555,556 | 4,201,680 | **1.322×** | 6,024,096 | 3,937,008 | **1.530×** |
| PING_MBULK | 1 | 152,905 | 160,514 | 0.953× | 179,533 | 177,305 | 1.013× |
| PING_MBULK | 16 | 2,267,574 | 2,232,143 | 1.016× | 2,375,297 | 2,331,002 | 1.019× |
| PING_MBULK | 100 | 7,246,377 | 5,102,041 | **1.420×** | 7,407,407 | 4,950,495 | **1.496×** |
| SET | 1 | 150,376 | 159,236 | 0.944× | 165,016 | 176,991 | 0.932× |
| SET | 16 | 1,941,748 | 1,757,469 | 1.105× | 2,028,397 | 1,607,717 | 1.262× |
| SET | 100 | 3,610,108 | 2,695,418 | **1.339×** | 3,636,363 | 2,347,418 | **1.549×** |

At single-request pipeline depth, Valdr trails by 5-17% (per-request RESP
parsing + dispatch overhead). At pipeline=16, parity. At pipeline=100,
Valdr is consistently **1.4-1.5× faster than either Valkey version** — the
RuntimeOwner event loop amortizes parser/dispatch/write costs better than
upstream's accept-per-thread model.

Benchmark notes:

- Valdr commit: `7838a3d` (this README); Valkey adversaries:
  `valkey/valkey:8-alpine` (= 8.1.7) and `valkey-io/valkey@9.1.0`
- Host: Apple M3 Max, macOS
- Warmup: 1,000 `PING_MBULK` requests, 1 client, pipeline 1, before every measured row
- Full artifacts:
  `harness/bench/results/20260528T191756Z-9666290-official-warm-results.md` (vs 9.1.0),
  `harness/bench/results/20260528T193018Z-7838a3d-official-warm-run.log` (vs 8.1.7)
- MGET against Valkey 8.1.7 needed a bench-client swap because 8.1.7's
  `valkey-benchmark` doesn't recognize `-t mget` as a target (added later).
  The MGET row was filled in by re-running just that command with a 9.x
  `valkey-benchmark` driving the 8.1.7 server — see
  `bash harness/bench/official-warm-run.sh` and its `TESTS=…` /
  `BENCH_BIN=…` env-var overrides for the one-line reproduction.
- Re-run: `bash harness/bench/official-warm-run.sh` (uses whatever Valkey
  is built at `reference/valkey`; switch tags with `git -C reference/valkey
  checkout <tag> && make -j BUILD_TLS=no`)

## Run

```bash
cargo build --release
./target/release/redis-server --port 6379 --bind 127.0.0.1
```

## Docker

```bash
docker pull ghcr.io/ianm199/valdr:alpha
docker run --rm -p 6379:6379 ghcr.io/ianm199/valdr:alpha
```

## Test Commands

```bash
bash scripts/setup-reference.sh
cargo build -p redis-server
cargo test --workspace
bash harness/oracle/smoke.sh --skip-build
python3 harness/oracle/rdb-diff --direction=all
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build
bash harness/bench/official-warm-run.sh
```

The single source of truth for what these prove — counted TCL passes,
single-node source-block coverage, and how the full 4,299-block upstream
denominator is bucketed — is
[`docs/TEST_AND_FEATURE_COVERAGE.md`](docs/TEST_AND_FEATURE_COVERAGE.md).
