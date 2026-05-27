# Release Checklist

Target release: `v0.1.0-alpha.1`, Docker-first alpha.

## Local Gates

Run from the repository root:

```bash
git status --short --branch
cargo build --release --locked -p redis-server
cargo build -p redis-server
cargo test --workspace
bash harness/oracle/smoke.sh --skip-build
python3 harness/oracle/rdb-diff --direction=all
bash .claude/hooks/unsafe-budget.sh </dev/null
bash harness/docker/smoke.sh
```

Optional performance smoke:

```bash
VALKEY_BENCH_SKIP_BUILD=1 python3 harness/bench/default-suite-parts.py run \
  --mode ordered \
  --target both \
  --tests ping_inline,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_300,lrange_500,lrange_600,mset,mget,xadd \
  --requests 100000 \
  --clients 50 \
  --pipeline 1 \
  --payload 64 \
  --timeout-s 60 \
  --no-build

python3 harness/bench/pipeline-smoke.py --commands get,ping_mbulk,set,incr --pipelines 1,16,100
```

## Docker Pull And Try

```bash
docker pull ghcr.io/ianm199/valkey-rs:alpha &&
docker run --rm -p 6379:6379 -v valkey-rs-data:/data ghcr.io/ianm199/valkey-rs:alpha
```

Docker-only one-copy smoke:

```bash
docker network create valkey-rs-try >/dev/null 2>&1 || true
docker rm -f valkey-rs-try >/dev/null 2>&1 || true
docker pull ghcr.io/ianm199/valkey-rs:alpha
docker run -d --name valkey-rs-try --network valkey-rs-try -v valkey-rs-data:/data ghcr.io/ianm199/valkey-rs:alpha
docker run --rm --network valkey-rs-try redis:7-alpine redis-cli -h valkey-rs-try PING
docker run --rm --network valkey-rs-try redis:7-alpine redis-cli -h valkey-rs-try SET hello world
docker run --rm --network valkey-rs-try redis:7-alpine redis-cli -h valkey-rs-try GET hello
docker rm -f valkey-rs-try
docker network rm valkey-rs-try
```

## Docker Benchmark

```bash
IMAGE=ghcr.io/ianm199/valkey-rs:alpha \
REQUESTS=100000 \
CLIENTS=50 \
PIPELINE=16 \
TESTS=ping_inline,ping_mbulk,set,get,incr,lrange_100,lrange_300 \
bash harness/docker/bench.sh
```

Deep-pipeline smoke:

```bash
PIPELINE=100 REQUESTS=200000 TESTS=get,set,incr,ping_mbulk bash harness/docker/bench.sh
```

## Publish

```bash
git tag -a v0.1.0-alpha.1 -m "v0.1.0-alpha.1"
git push origin main
git push origin v0.1.0-alpha.1
```

After workflows complete:

```bash
gh run list --branch main --limit 10
docker manifest inspect ghcr.io/ianm199/valkey-rs:alpha
SKIP_BUILD=1 IMAGE=ghcr.io/ianm199/valkey-rs:alpha bash harness/docker/smoke.sh
```

Manual checks:

- GHCR package visibility is public.
- GitHub Pages is enabled for Actions, or the Pages workflow is expected to
  enable it on first successful run.
- README, Docker docs, site pages, and changelog all describe alpha limits
  consistently.
