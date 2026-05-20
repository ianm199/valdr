# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS build

WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release --locked --bin redis-server \
    && strip target/release/redis-server

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home-dir /data --shell /usr/sbin/nologin valkey \
    && install -d -o valkey -g valkey -m 0755 /data /etc/valkey-rs

COPY --from=build /src/target/release/redis-server /usr/local/bin/redis-server
COPY docker/redis.conf /etc/valkey-rs/redis.conf

RUN chown valkey:valkey /etc/valkey-rs/redis.conf

USER valkey
WORKDIR /data
EXPOSE 6379

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/redis-server"]
CMD ["/etc/valkey-rs/redis.conf"]
