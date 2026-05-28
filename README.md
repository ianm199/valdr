# valdr

Single-node Valkey-compatible server: a Rust port of the upstream Valkey C implementation.

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
  ghcr.io/flightdecksystems/valdr:alpha

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

Latest warmed local run vs upstream Valkey:

Official `valkey-benchmark` suite:

| Metric | Result |
|---|---:|
| Command rows completed | 23 / 23 |
| Median throughput ratio vs Valkey | 1.250x |
| PING_MBULK ratio | 1.429x |
| SET ratio | 1.538x |
| GET ratio | 1.409x |
| INCR ratio | 1.250x |
| MGET ratio | 1.525x |
| Slowest known row | `FCALL` at 0.593x |

Focused comparison:

| Workload | Pipeline | Valkey rps | valdr rps | Ratio | Valkey p99 ms | valdr p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| GET | 1 | 149,031 | 147,275 | 0.988x | 0.407 | 0.399 |
| SET | 1 | 136,426 | 143,678 | 1.053x | 0.439 | 0.399 |
| GET | 100 | 3,802,281 | 5,988,024 | 1.575x | 1.527 | 0.935 |
| SET | 100 | 2,331,003 | 3,610,108 | 1.549x | 2.383 | 1.559 |
| JSON GET, 4KB docs | 1 | 34,288 | 33,832 | 0.987x | 4.167 | 4.173 |
| JSON SET, 4KB docs | 1 | 32,503 | 32,889 | 1.012x | 4.249 | 4.387 |
| JSON mixed, 4KB docs | 1 | 33,090 | 34,438 | 1.041x | 4.284 | 4.131 |

Per-command `valkey-benchmark` breakdown:

| Command | Valkey rps | valdr rps | Ratio | Valkey p99 ms | valdr p99 ms |
|---|---:|---:|---:|---:|---:|
| PING_INLINE | 3,846,154 | 5,263,158 | 1.368x | 1.559 | 1.063 |
| PING_MBULK | 5,000,000 | 7,142,857 | 1.429x | 1.231 | 0.703 |
| SET | 2,500,000 | 3,846,154 | 1.538x | 2.303 | 1.631 |
| GET | 3,225,806 | 4,545,454 | 1.409x | 1.823 | 1.143 |
| INCR | 3,333,334 | 4,166,667 | 1.250x | 1.663 | 1.311 |
| LPUSH | 2,325,581 | 2,631,579 | 1.132x | 2.799 | 2.095 |
| RPUSH | 2,631,579 | 2,564,102 | 0.974x | 2.247 | 2.231 |
| LPOP | 2,173,913 | 2,500,000 | 1.150x | 2.775 | 2.223 |
| RPOP | 2,380,952 | 2,380,952 | 1.000x | 2.455 | 4.367 |
| SADD | 3,030,303 | 2,380,952 | 0.786x | 1.935 | 2.271 |
| HSET | 2,325,581 | 1,785,714 | 0.768x | 2.999 | 3.111 |
| SPOP | 3,703,704 | 3,125,000 | 0.844x | 1.599 | 1.823 |
| ZADD | 2,222,222 | 1,724,138 | 0.776x | 3.575 | 4.943 |
| ZPOPMIN | 3,846,154 | 2,777,778 | 0.722x | 1.511 | 2.007 |
| LRANGE_100 | 110,132 | 182,815 | 1.660x | 24.959 | 21.855 |
| LRANGE_300 | 35,613 | 58,207 | 1.634x | 61.375 | 94.079 |
| LRANGE_500 | 20,458 | 32,765 | 1.602x | 103.487 | 106.559 |
| LRANGE_600 | 17,015 | 26,874 | 1.579x | 119.423 | 146.687 |
| MSET | 440,529 | 609,756 | 1.384x | 3.799 | 8.935 |
| MGET | 662,252 | 1,010,101 | 1.525x | 10.351 | 5.167 |
| XADD | 1,351,351 | 1,098,901 | 0.813x | 6.391 | 4.735 |
| FUNCTION LOAD | 55,432 | 588,235 | 10.612x | 76.351 | 8.727 |
| FCALL | 1,369,863 | 813,008 | 0.593x | 4.975 | 12.191 |

Benchmark note:

- Commit: `b31c324`
- Host: Apple M3 Max
- Warmup: 1,000 `PING_MBULK` requests, 1 client, pipeline 1
- Full artifact: `harness/bench/results/20260527T145526Z-b31c324-official-warm-results.md`
- Re-run: `bash harness/bench/official-warm-run.sh`

## Run

```bash
cargo build --release
./target/release/redis-server --port 6379 --bind 127.0.0.1
```

## Docker

```bash
docker pull ghcr.io/flightdecksystems/valdr:alpha
docker run --rm -p 6379:6379 ghcr.io/flightdecksystems/valdr:alpha
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
