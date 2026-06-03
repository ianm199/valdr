# Roadmap

## Near term

- Keep AOF release wording narrow and evidence-backed
  - AOF is single-node alpha, correctness-gated on the current tree
  - Repeat AOF matrix/rewrite-latency telemetry on isolated release hosts before stronger public performance or durability claims
- Take replication out of alpha
  - Prove partial-resync (PSYNC) instead of always doing a full re-sync
  - Define and gate the production HA story separately from the single-node alpha
- Add Clustering and HA to get closer to feature parity

## Performance
- FCALL is the last sub-parity command — Lua-VM per-call overhead, not data structures
- Optional: I/O threads (Redis-style) for a throughput bump at high concurrency
- Support thread pooling 
- Start to explore ways to increase performance, offering more users incentives to try this. Maybe more aggressive hotpaths or sharding than Valkey

## Safety

- Migrate scripting from mlua (C Lua) to [**lua-rs**](https://github.com/ianm199/lua-rs/tree/main) (pure-Rust Lua)
  - Removes the last embedded C dependency in the data path
  - Eliminates the 3 `unsafe` pointer blocks in `eval.rs`
  - Gated on Lua 5.1 compatibility — the number model and `setfenv`/`getfenv`
  - This project is not yet at maturity, that will take some time. None of the other Lua in Rust projects are either really 
- Add `#![forbid(unsafe_code)]` to the zero-unsafe data crates
- Make decision on C ABI - either support unsafe C or support full Rust alternatives

## Bigger bets

- Multi-threaded execution (Dragonfly-style shared-nothing sharding)
  - Tier 1: I/O threads first (lower risk)
  - Tier 2: sharded execution + a cross-shard transaction framework
- Compact data-structure encodings (intset / listpack / skiplist)
