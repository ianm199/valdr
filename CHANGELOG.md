# Changelog

## 0.1.0-alpha.1 - 2026-05-28

Initial public alpha release candidate.

- Runs as a `redis-server`-compatible binary and container image.
- Supports RESP2/RESP3, core Redis data types, streams, pub/sub, transactions,
  Lua scripting, ACL, eviction, RDB persistence, and native JSON/Bloom command
  subsets.
- Gates the current single-node claim with wire-diff smoke, RDB bidirectional
  tests, and scoped upstream TCL survey evidence.
- Publishes Docker images at `ghcr.io/ianm199/valkey-rs:alpha`, `:main`, and
  `:sha-<short-sha>`.
- Ships alpha benchmark tooling for local and Docker-only runs.

Known alpha limits:

- No cluster mode, Sentinel, loadable C-ABI modules, or release-supported
  in-process TLS.
- AOF and replication basics exist, but are not production HA gates.
- Full upstream Valkey suite accounting is still in progress.
- Sustained-load and broader workload benchmark evidence remain post-alpha
  release work.
