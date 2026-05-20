# Docker packaging plan

Goal: a Dockerfile that produces a small image which a developer can
`docker run` and point any RESP2 client at — and have it "just work"
as a Valkey replacement for typical app use cases.

Not in scope: image-registry publishing, multi-arch builds, ARM
optimization, kubernetes manifests, helm chart.

## Deliverables

1. `Dockerfile` at repo root (multi-stage build).
2. `docker-compose.yml` for the easy-path "I just want to run it
   with persistence."
3. `harness/docker/smoke.sh` — Docker smoke test (build image, run,
   exercise a small command set via raw RESP, verify persistence
   across `docker stop`/`docker start`).
4. `harness/docker/client-compat.py` — runs `redis-py` and `ioredis`
   against the container, exercises the common app patterns (cache,
   session, pub/sub, streams, pipelining, AUTH). Reports per-pattern
   PASS/FAIL.
5. Add a "Try in Docker" section to the repo README (if a README
   exists; otherwise a new `docs/DOCKER.md`).

## Dockerfile design

Multi-stage:

```dockerfile
FROM rust:1.78-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --bin redis-server --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 1000 --user-group --shell /bin/false \
        --home-dir /data redis
COPY --from=build /src/target/release/redis-server /usr/local/bin/
RUN install -d -o redis -g redis -m 0755 /data
WORKDIR /data
USER redis
EXPOSE 6379
HEALTHCHECK --interval=10s --timeout=2s --retries=3 \
        CMD redis-cli -p 6379 PING || exit 1
ENTRYPOINT ["/usr/local/bin/redis-server"]
CMD ["--port", "6379", "--bind", "0.0.0.0", "--dir", "/data"]
```

Notes:
- Multi-stage: build under `rust:slim`, runtime under `debian:slim`.
  Image should be ~30 MB after release-strip.
- Run as non-root `redis` user. Owns `/data`.
- HEALTHCHECK uses real `redis-cli` — image needs it. Either install
  via apt or copy the bundled `reference/valkey/src/valkey-cli` from
  the build stage. The latter avoids the apt dependency.

## docker-compose.yml

```yaml
services:
  redis:
    build: .
    ports: ["6379:6379"]
    volumes: ["./data:/data"]
    restart: unless-stopped
```

One command: `docker compose up -d`. Persists to `./data/dump.rdb`
on the host.

## Smoke test

`harness/docker/smoke.sh` should:
1. `docker build` the image.
2. `docker run -d` with a tempfile volume.
3. Wait for the healthcheck to pass.
4. Send the hand-corpus subset via raw RESP (PING, SET, GET, LPUSH,
   HSET, ZADD, PUBLISH/SUBSCRIBE cross-conn, MULTI/EXEC, BLPOP with
   timeout).
5. SAVE → `docker stop` → `docker start` → verify SET persisted.
6. Tear down.

## Client-compat test

`harness/docker/client-compat.py` exercises the patterns app
developers actually hit:

| Pattern | Library | Expected |
|---|---|---|
| Basic GET/SET/EXPIRE | redis-py, ioredis | PASS |
| Session store | redis-py / Flask-Session adapter | PASS |
| Pub/Sub subscribe + publish | redis-py.PubSub | PASS |
| Pipeline 100 commands | redis-py.pipeline | PASS |
| XADD/XREADGROUP queue | redis-py streams | PASS |
| MULTI/EXEC | redis-py transaction | PASS |
| BLPOP with timeout | redis-py blocking-pop | PASS |
| INCR rate limit | redis-py + counter | PASS |
| EVAL script | redis-py eval() | EXPECTED FAIL (not implemented) |
| HELLO 3 (RESP3) | redis-py 5+ proto=3 | EXPECTED FAIL (we reject; client must fallback) |
| Redlock | python-redlock-py | LIKELY FAIL (uses EVAL) |

Report PASS / EXPECTED-FAIL / UNEXPECTED-FAIL.

## Known unfixable-in-Docker gaps

These are protocol-level, not Docker-fixable:
- EVAL / EVALSHA / SCRIPT subfamily
- Cluster discovery (single-node always)
- Replication (no replicas)
- RESP3 push frames (some pub/sub edge cases)

Document them in `docs/DOCKER.md` "Compatibility" section so users
know up-front.

## Budget estimate

- Dockerfile + compose: ~$3 (Sonnet)
- Smoke test script: ~$4 (Sonnet)
- Client-compat with two real libraries: ~$8 (Sonnet — needs to
  pip/npm install, exercise patterns, capture results)
- README/docs: ~$2 (Sonnet)
- **Total: ~$15-20** in one parallel-or-sequential round.

## When to run

After:
1. The chassis static-gen upgrade (Option B), so we don't sink
   effort into something we'd redo
2. OR right now if the operator wants a concrete demo artifact
   independently — Docker doesn't depend on anything else and ships
   a real "you can use this" deliverable

The Docker work is orthogonal to nginx/lua/chassis — it's a
packaging milestone the project crosses once and then maintains
incrementally.
