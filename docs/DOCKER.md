# Docker

`valkey-rs` ships as a single `redis-server` binary inside a small Debian
runtime image. The container listens on port 6379 and stores persistence files
under `/data`.

## Pull

Published images are intended to live at GitHub Container Registry:

```bash
docker pull ghcr.io/ianm199/valkey-rs:alpha
docker run --rm -p 6379:6379 -v valkey-rs-data:/data ghcr.io/ianm199/valkey-rs:alpha
```

Useful tags:

- `alpha` — latest alpha image from `main`.
- `main` — latest image from the default branch.
- `sha-<git-sha>` — immutable image for a specific commit.

Published images target `linux/amd64` and `linux/arm64`.

If the package is not visible yet, make the GHCR package public from the
repository package settings after the first workflow publish.

## Build locally

```bash
docker build -t valkey-rs:local .
docker run --rm -p 6379:6379 -v valkey-rs-data:/data valkey-rs:local
```

Or with Compose:

```bash
docker compose up --build
```

## Smoke test

The Docker smoke builds the image, starts a container with a named volume, uses
`redis-py` to exercise `PING`, `SET`, `GET`, `HSET`, pipelining, and `SAVE`,
then restarts the container and verifies the data was reloaded from RDB:

```bash
bash harness/docker/smoke.sh
```

Set `IMAGE=...` to test a different image name:

```bash
docker pull ghcr.io/ianm199/valkey-rs:alpha
SKIP_BUILD=1 IMAGE=ghcr.io/ianm199/valkey-rs:alpha bash harness/docker/smoke.sh
```

## Runtime config

The image runs:

```bash
redis-server /etc/valkey-rs/redis.conf
```

The bundled config is:

```conf
bind 0.0.0.0
port 6379
dir /data
dbfilename dump.rdb
appendonly no
```

For persistence, mount `/data` as either a named volume or a writable host
directory.

## Publish

Publish from a machine logged into GHCR with package-write permission:

```bash
SHA="$(git rev-parse --short HEAD)"
IMAGE="ghcr.io/ianm199/valkey-rs"

docker build \
  -t "$IMAGE:alpha" \
  -t "$IMAGE:main" \
  -t "$IMAGE:sha-$SHA" \
  .

echo "$GITHUB_TOKEN" | docker login ghcr.io -u ianm199 --password-stdin
docker push "$IMAGE:alpha"
docker push "$IMAGE:main"
docker push "$IMAGE:sha-$SHA"
```

## Current limits

The image has the same limits as the binary:

- single-node only, no cluster mode;
- no loadable C-ABI modules;
- alpha status until sustained-load and performance testing are published.
