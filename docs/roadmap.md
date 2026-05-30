# Roadmap

Edit `docs/roadmap.md` and commit. `-` for a bullet, two-space indent for a
sub-bullet, `- [x]` for done.

## Near term

- Take AOF and replication out of alpha
  - Prove partial-resync (PSYNC) instead of always doing a full re-sync
  - AOF multi-part + truncated-tail recovery edge cases
- Finish the release checklist and ship the alpha tag
  - Write the landing-page copy
  - Confirm `unit/introspection` is green on a clean environment

## Performance

- [x] Collection-write commands beat upstream (jemalloc + hot-table dispatch)
- [x] p=1 GET/SET at or above parity
- FCALL is the last sub-parity command — Lua-VM per-call overhead, not data structures
- Optional: I/O threads (Redis-style) for a throughput bump at high concurrency

## Safety

- Migrate scripting from mlua (C Lua) to **lua-rs** (pure-Rust Lua)
  - Removes the last embedded C dependency in the data path
  - Eliminates the 3 `unsafe` pointer blocks in `eval.rs`
  - Gated on Lua 5.1 compatibility — the number model and `setfenv`/`getfenv`
- Add `#![forbid(unsafe_code)]` to the zero-unsafe data crates

## Bigger bets

- Multi-threaded execution (Dragonfly-style shared-nothing sharding)
  - Tier 1: I/O threads first (lower risk)
  - Tier 2: sharded execution + a cross-shard transaction framework
- Compact data-structure encodings (intset / listpack / skiplist)

## Out of scope (for now)

- Cluster mode
- Sentinel / high availability
- Loadable C module ABI
