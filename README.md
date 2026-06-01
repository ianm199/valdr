# valdr

Valdr is a port of [Valkey](https://github.com/valkey-io/valkey) aiming to be fully compatible with Redis/ Valkey clients in memory safe Rust. The motivation for this project is to attempt to build mostly memory safe alternatives to core web infrastructure and to explore archtecture choices that wil enable faster performance in the long run. 

Currently Valdr supports the all the core Redis Client commands and passes 99.6% of [single node tests](https://valdr.dev/coverage.html). 

This repo heavily leveraged coding agents in the process. This was largely inspired by the changing landscape of [memory safety attacks](https://labs.cloudsecurityalliance.org/research/csa-research-note-claude-mythos-autonomous-offensive-thresho/) as agentic cyber capabilities increase.

## Roadmap

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

<!-- PERF:START — auto-generated from docs/perf-data.json by `make site-data`; do not hand-edit between these markers -->

Latest warmed local run: Valdr (`8c8678f`) vs **Valkey 9.1.0**, measured 2026-05-29T20:32:39+00:00 on Apple M3 Max. These tables and the [valdr.dev](https://valdr.dev) landing page both render `docs/perf-data.json` — one source of truth, no hand-typed numbers. Ratio = valdr_rps / valkey_rps; >1.00 = Valdr is faster. `function_load` is excluded (its ratio is a reload-fast-path artifact, not throughput).

**Server config:** no `.conf` file — both servers are launched from explicit flags, persistence off, everything else stock defaults. Valkey: `--save "" --appendonly no --daemonize no --loglevel warning`. Valdr: `--rdb-disabled --appendonly no`. Both bound to `127.0.0.1`.

### Per-command (default `valkey-benchmark` suite)

| Command | Valdr rps | Valkey 9.1.0 rps | Ratio |
|---|---:|---:|---:|
| PING_INLINE | 6,250,000 | 4,000,000 | **1.562×** |
| PING_MBULK | 6,666,667 | 5,555,556 | 1.200× |
| SET | 3,846,154 | 2,564,102 | **1.500×** |
| GET | 4,545,454 | 3,448,276 | **1.318×** |
| INCR | 4,166,667 | 3,571,428 | 1.167× |
| LPUSH | 2,702,703 | 2,325,581 | 1.162× |
| RPUSH | 2,702,703 | 2,702,703 | 1.000× |
| LPOP | 2,777,778 | 2,325,581 | 1.194× |
| RPOP | 2,564,102 | 2,631,579 | 0.974× |
| SADD | 3,448,276 | 3,225,806 | 1.069× |
| HSET | 2,777,778 | 2,564,102 | 1.083× |
| SPOP | 4,545,454 | 4,000,000 | 1.136× |
| ZADD | 2,564,102 | 2,325,581 | 1.103× |
| ZPOPMIN | 4,166,667 | 3,846,154 | 1.083× |
| LRANGE_100 | 184,502 | 120,337 | **1.533×** |
| LRANGE_300 | 62,266 | 37,994 | **1.639×** |
| LRANGE_500 | 36,805 | 22,589 | **1.629×** |
| LRANGE_600 | 30,460 | 18,498 | **1.647×** |
| MSET | 680,272 | 476,190 | **1.429×** |
| MGET | 1,020,408 | 751,880 | **1.357×** |
| XADD | 1,265,823 | 1,449,275 | 0.873× |
| FCALL | 990,099 | 1,408,451 | 0.703× |

- **Wins** (ratio ≥ 1.2×): `ping_inline`, `ping_mbulk`, `set`, `get`, `lrange_100`, `lrange_300`, `lrange_500`, `lrange_600`, `mset`, `mget`.
- **Parity** (0.95×–1.2×): `incr`, `lpush`, `rpush`, `lpop`, `rpop`, `sadd`, `hset`, `spop`, `zadd`, `zpopmin`.
- **Behind** (< 0.95×): `xadd`, `fcall` — where the port's oracle-gated behavioral fidelity currently extracts a perf cost not yet paid down.

### Pipeline-depth curve (GET/SET/PING/INCR at p=1/16/100)

| Workload | Valdr rps | Valkey 9.1.0 rps | Ratio |
|---|---:|---:|---:|
| GET p=1 | 160,256 | 150,602 | 1.064× |
| GET p=16 | 2,000,000 | 1,739,130 | 1.150× |
| GET p=100 | 5,128,205 | 3,174,603 | **1.615×** |
| PING p=1 | 167,785 | 154,799 | 1.084× |
| PING p=16 | 2,222,222 | 2,105,263 | 1.056× |
| PING p=100 | 8,000,000 | 5,000,000 | **1.600×** |
| SET p=1 | 162,338 | 151,515 | 1.071× |
| SET p=16 | 2,061,856 | 1,562,500 | **1.320×** |
| SET p=100 | 4,166,667 | 2,247,191 | **1.854×** |
| INCR p=1 | 168,350 | 163,934 | 1.027× |
| INCR p=16 | 1,801,802 | 1,739,130 | 1.036× |
| INCR p=100 | 4,255,320 | 3,389,830 | 1.255× |

<!-- PERF:END -->

### How the numbers are produced

- Warmup: 1,000 `PING_MBULK` requests (1 client, pipeline 1) before every measured row.
- Refresh: `make bench-release` (fresh local artifacts) then `make site-data`,
  which regenerates `docs/perf-data.json` **and** rewrites the table above from
  it. The [valdr.dev](https://valdr.dev) landing page fetches the same JSON, so
  the site and this README can never disagree — they share one source of truth.
- Switch the Valkey adversary: `git -C reference/valkey checkout <tag> && make -j BUILD_TLS=no`,
  then re-run the refresh.

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
