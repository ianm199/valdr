# valkey-rs

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
| Upstream TCL suite | Scoped evidence, not full-suite pass claim | Full denominator: 4,299 test blocks |
| Cluster mode | Not implemented | Out of scope for current alpha |
| Loadable C modules | Not implemented | Out of scope for current alpha |
| Production HA / Sentinel | Not claimed | Replication/AOF exist but are not production-conformance gated |
| In-process TLS | Not enabled | rustls scaffold exists; listener is not wired to runtime owner |

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

## Performance

Latest warmed local run:

| Suite | Result |
|---|---:|
| Default suite, ordered, P100 | 23 / 23 pass |
| Default suite median ratio vs Valkey | 1.250x |
| Default suite min ratio vs Valkey | 0.593x |
| Pipeline smoke | 9 / 9 pass |
| Pipeline smoke median ratio vs Valkey | 1.108x |
| JSON document mix, 4KB docs, P1 | 3 / 3 pass |
| JSON document mix median ratio vs Valkey | 1.012x |

Representative rows:

| Workload | Pipeline | Valkey rps | valkey-rs rps | Ratio | Valkey p99 ms | valkey-rs p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| GET | 1 | 149,031 | 147,275 | 0.988x | 0.407 | 0.399 |
| SET | 1 | 136,426 | 143,678 | 1.053x | 0.439 | 0.399 |
| GET | 100 | 3,802,281 | 5,988,024 | 1.575x | 1.527 | 0.935 |
| SET | 100 | 2,331,003 | 3,610,108 | 1.549x | 2.383 | 1.559 |
| JSON GET, 4KB docs | 1 | 34,288 | 33,832 | 0.987x | 4.167 | 4.173 |
| JSON mixed, 4KB docs | 1 | 33,090 | 34,438 | 1.041x | 4.284 | 4.131 |

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
docker pull ghcr.io/ianm199/valkey-rs:alpha
docker run --rm -p 6379:6379 ghcr.io/ianm199/valkey-rs:alpha
```

## Test Commands

```bash
bash scripts/setup-reference.sh
cargo build -p redis-server
bash harness/oracle/smoke.sh --skip-build
python3 harness/oracle/rdb-diff --direction=all
bash harness/bench/official-warm-run.sh
```
