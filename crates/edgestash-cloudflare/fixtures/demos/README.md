# EdgeStash demo workflows

Four production-shaped workflows that each prevent a concrete bug the
"atomic Lua at the edge, one Durable Object per entity" design exists to solve.
Each demo has two layers:

- a deterministic Rust scenario test in `crates/edgestash-demo/tests/demo_*.rs`
  (the correctness gate — time-controlled, runs in CI, no server), and
- a self-checking shell script here that replays the same flow over real
  Worker HTTP.

Run the shell demos against a deployed Worker (default) or local `wrangler dev`:

```sh
# against the deployed Worker (default BASE)
sh fixtures/demos/inventory.sh
sh fixtures/demos/otp.sh
sh fixtures/demos/feature_flags.sh
sh fixtures/demos/matchmaking.sh

# against a local dev server
BASE=http://127.0.0.1:8787 sh fixtures/demos/inventory.sh
```

Each script uses a fresh tenant per run, is `set -eu`, and exits nonzero on any
invariant violation.

| Demo | App / pain it prevents | EdgeStash mechanism | The invariant proven |
|---|---|---|---|
| `inventory.sh` | Flash sale / limited drop — **oversell** | DO-per-SKU; atomic Lua `RESERVE`/`CONFIRM`/`CANCEL`, `INCRBY -qty`, hold key with PX TTL | 50 concurrent buyers vs stock=10 → exactly 10 reserved, 40 sold-out, stock never negative (DO serialization + atomic Lua) |
| `otp.sh` | 2FA / login code — **brute force** | DO-per-user; code+attempts in a hash with PEXPIRE; atomic `VERIFY` decrements then compares | a correct code submitted after the attempt budget is exhausted is still rejected |
| `feature_flags.sh` | Gradual rollout — **flag eval latency / drift** | DO-per-app; `EVALUATE` buckets via `redis.sha1hex(flag:user) % 100`, kill > deny > allow > rollout | deterministic stable buckets, correct precedence, boundary flips at the user's own bucket — evaluated at the edge, no round-trip |
| `matchmaking.sh` | Multiplayer queue — **double-match** | DO-per-pool; ZSet by rating; `FIND_MATCH` uses `ZRANGEBYSCORE` band query + atomic dual `ZREM` | every pair within band, ZCARD drops by exactly 2 per match, a matched player is never matched again |

The Lua for each lives as a string constant inside both the matching
`demo_*.rs` and the `.sh` file, so the script the Rust test verifies is
byte-identical to the one the live demo loads.
