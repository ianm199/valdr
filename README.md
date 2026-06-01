# valdr
Single node Valkey in mostly memory safe Rust. Verified against Valkey's test suite, uses memory safe TLS with rustls.

Valdr is a port of [Valkey](https://github.com/valkey-io/valkey) aiming to be fully compatible with Redis/Valkey clients in memory safe Rust. The motivation for this project is to attempt to build mostly memory safe alternatives to core web infrastructure and to explore architecture choices that will enable faster performance in the long run. 

Currently Valdr supports all the core Redis client commands and passes 99.6% of [single node tests](https://valdr.dev/coverage.html). 

This repo heavily leveraged coding agents in the process. This was largely inspired by the changing landscape of [memory safety attacks](https://labs.cloudsecurityalliance.org/research/csa-research-note-claude-mythos-autonomous-offensive-thresho/) as agentic cyber capabilities increase.

## Roadmap
[Full roadmap here](https://valdr.dev/roadmap.html)
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
| Upstream TCL suite | Single-node core green; not a full-suite claim | Full denominator 4,299 blocks, bucketed at [valdr.dev/coverage.html](https://valdr.dev/coverage.html) |
| Cluster mode | Not implemented | Out of scope for current alpha |
| Loadable C modules | Not implemented | Out of scope for current alpha |
| Production HA / Sentinel | Not claimed | Replication/AOF exist but are not production-conformance gated |
| In-process TLS | Enabled (rustls; no OpenSSL) | TLS 1.2 + 1.3; mTLS tri-state (`no`/`optional`/`yes`); dynamic CONFIG SET of `tls-protocols`, `tls-auth-clients`, cert/key paths. CBC-suite tests in `unit/tls.tcl` are a deliberate rustls divergence. |

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

Latest warmed local run: Valdr (`9104a19`) vs **Valkey 9.1.0 (jemalloc)**, measured 2026-06-01T14:44:54+00:00 on Apple M3 Max. These tables and the [valdr.dev](https://valdr.dev) landing page both render `docs/perf-data.json` — one source of truth, no hand-typed numbers. Ratio = valdr_rps / valkey_rps; >1.00 = Valdr is faster. `function_load` is excluded (its ratio is a reload-fast-path artifact, not throughput).

**Server config:** no `.conf` file — both servers are launched from explicit flags, persistence off, everything else stock defaults. Valkey: `--save "" --appendonly no --daemonize no --loglevel warning`. Valdr: `--rdb-disabled --appendonly no`. Both bound to `127.0.0.1`.

### Per-command (default `valkey-benchmark` suite)

| Command | Valdr rps | Valkey 9.1.0 (jemalloc) rps | Ratio |
|---|---:|---:|---:|
| PING_INLINE | 6,666,667 | 4,761,905 | **1.400×** |
| PING_MBULK | 9,090,909 | 6,250,000 | **1.455×** |
| SET | 4,166,667 | 2,941,176 | **1.417×** |
| GET | 5,000,000 | 3,846,154 | **1.300×** |
| INCR | 4,545,454 | 4,000,000 | 1.136× |
| LPUSH | 2,702,703 | 2,702,703 | 1.000× |
| RPUSH | 2,702,703 | 3,030,303 | 0.892× |
| LPOP | 2,777,778 | 2,500,000 | 1.111× |
| RPOP | 2,564,102 | 2,702,703 | 0.949× |
| SADD | 3,448,276 | 3,571,428 | 0.966× |
| HSET | 2,777,778 | 2,777,778 | 1.000× |
| SPOP | 4,761,905 | 4,347,826 | 1.095× |
| ZADD | 2,631,579 | 2,380,952 | 1.105× |
| ZPOPMIN | 4,347,826 | 4,000,000 | 1.087× |
| LRANGE_100 | 185,529 | 132,979 | **1.395×** |
| LRANGE_300 | 59,277 | 39,620 | **1.496×** |
| LRANGE_500 | 35,398 | 22,878 | **1.547×** |
| LRANGE_600 | 30,340 | 18,713 | **1.621×** |
| MSET | 699,301 | 515,464 | **1.357×** |
| MGET | 1,075,269 | 793,651 | **1.355×** |
| XADD | 1,388,889 | 1,724,138 | 0.806× |
| FCALL | 1,030,928 | 1,449,275 | 0.711× |

- **Wins** (ratio ≥ 1.2×): `ping_inline`, `ping_mbulk`, `set`, `get`, `lrange_100`, `lrange_300`, `lrange_500`, `lrange_600`, `mset`, `mget`.
- **Parity** (0.95×–1.2×): `incr`, `lpush`, `lpop`, `sadd`, `hset`, `spop`, `zadd`, `zpopmin`.
- **Behind** (< 0.95×): `rpush`, `rpop`, `xadd`, `fcall` 

### Pipeline-depth curve (GET/SET/PING/INCR at p=1/16/100)

| Workload | Valdr rps | Valkey 9.1.0 (jemalloc) rps | Ratio |
|---|---:|---:|---:|
| GET p=1 | 162,866 | 147,059 | 1.107× |
| GET p=16 | 2,597,402 | 2,040,816 | 1.273× |
| GET p=100 | 4,761,905 | 3,571,428 | **1.333×** |
| PING p=1 | 179,211 | 159,236 | 1.125× |
| PING p=16 | 2,816,901 | 2,469,136 | 1.141× |
| PING p=100 | 8,000,000 | 5,882,352 | **1.360×** |
| SET p=1 | 177,936 | 154,799 | 1.149× |
| SET p=16 | 1,980,198 | 1,785,714 | 1.109× |
| SET p=100 | 4,081,633 | 2,816,901 | **1.449×** |
| INCR p=1 | 207,469 | 174,825 | 1.187× |
| INCR p=16 | 2,173,913 | 2,325,581 | 0.935× |
| INCR p=100 | 4,166,667 | 3,448,276 | 1.208× |

<!-- PERF:END -->

### How the numbers are produced

- Warmup: 1,000 `PING_MBULK` requests (1 client, pipeline 1) before every measured row.
- Refresh: `make bench-release` (fresh local artifacts) then `make site-data`,
  which regenerates `docs/perf-data.json` **and** rewrites the table above from
  it. The [valdr.dev](https://valdr.dev) landing page fetches the same JSON, so
  the site and this README can never disagree — they share one source of truth.
- Switch the Valkey adversary: `git -C reference/valkey checkout <tag> && make -j MALLOC=jemalloc BUILD_TLS=no`,
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
[valdr.dev/coverage.html](https://valdr.dev/coverage.html).
