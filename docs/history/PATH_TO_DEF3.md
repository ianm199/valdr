# Path to Definition 3 — prod-safe Redis cache

Target: a Rust Valkey impl safe to run as a read-mostly cache in CI/dev,
with persistence, auth, and memory bounds. Not full Redis parity — see scope.

## TL;DR

Eight feature areas, ~25-35 agent rounds, ~$220-300 in API tokens at current
mix (Sonnet-heavy translator + Opus reviewer). Headline risk is RDB binary-format
fidelity: every encoding has its own serialize path and a single mismatch is
silent until a TCL `restart_server` test fails on reload. The work splits
cleanly into three phases — observability (rounds 14-17), bounded memory
(rounds 18-23), persistence (rounds 24-32) — each shippable as its own
"definition 3.{1,2,3}" milestone. Static generation from `config.c` and
`rdb.h` saves 2-4 rounds and validates the pattern before nginx.

## Scope of Def 3

**In:**
1. RDB save + load (SAVE, BGSAVE, --dbfilename, load on startup)
2. AUTH + `requirepass` (single-user, no per-user ACLs)
3. `maxmemory` enforcement + `noeviction` and `allkeys-lru` policies
4. Active expiration cycle wired into the event loop
5. Keyspace event notifications (`__keyspace@N__:key` + `__keyevent@N__:evt`)
6. CONFIG SET with behavioural hooks for the 12 configs that matter operationally
7. SLOWLOG GET/LEN/RESET + LATENCY HISTORY with real measurements
8. `maxclients` enforcement at accept time

**Out (defer to Def 4+ or never):**
- AOF (rdb-only is enough for cache use)
- Full ACL (users, categories, key patterns, channel patterns)
- LFU, volatile-* eviction policies, allkeys-lfu
- Cluster, replication, sentinel, modules, TLS, EVAL/scripting
- RESP3, CLIENT TRACKING (invalidations)
- Persistence-aware shutdown (NOSAVE / SAVE / FAILOVER flags beyond NOSAVE)

## Codebase audit — per feature

### Persistence: RDB save + load

- **Valkey source**: `reference/valkey/src/rdb.c` (4022 LoC), `rdb.h`,
  plus `rio.c` (~400 LoC buffered I/O). Headline API:
  `rdbSaveType`, `rdbSaveLen`, `rdbSaveStringObject`, `rdbSaveObject`,
  `rdbSaveRio`, `rdbSaveBackground`, `rdbLoadType`, `rdbLoadObject`,
  `rdbLoadRioWithLoadingCtx`. Per-encoding serializers (intset, listpack,
  ziplist-legacy, quicklist, skiplist, stream radix) live inline.
- **What it does**: writes a length-prefixed, type-tagged binary stream
  with an opcode-based framing (`RDB_OPCODE_AUX`, `RDB_OPCODE_EXPIRETIME_MS`,
  `RDB_OPCODE_SELECTDB`, `RDB_OPCODE_EOF`) plus a trailing CRC64. Load
  parses the same stream back into the keyspace.
- **Translation strategy**: **hybrid — static-gen the type ID table
  (`RDB_TYPE_*` constants from `rdb.h`); hand-port everything else.**
  RDB version is currently `RDB_VERSION = 12`. Skip pre-v9 backward-compat
  branches (legacy ziplist, list-zipmap) — we never wrote them, never read
  them. Implement v12-only encode + decode for the six types we have
  (string, list-quicklist, set-listpack+intset+hashtable, hash-listpack+
  hashtable, zset-listpack+skiplist, stream-listpack-radix). Stream is
  optional for Def 3 since `XADD` isn't a cache-use primitive — gate it
  behind a feature flag.
- **Dependencies in our codebase**: extends `RedisObject` (`object.rs`) with
  `serialize_rdb / deserialize_rdb`. Needs a new `crates/redis-persist`
  crate for `rio` buffered reader/writer + CRC64 (`reference/valkey/src/crc64.c`,
  fully tabular — port verbatim, ~180 LoC). Touches `redis-server` startup
  to add a load-on-boot path before the listener binds.
- **Estimated effort**: 5-7 agent rounds. ~2200 LoC Rust (rio 250, crc64 200,
  rdb-core 800, per-type encoders 600, per-type decoders 600). Sonnet for
  encoders (mechanical), Opus for the framing + version dispatch (subtle).
- **Risk**: Highest risk surface in Def 3. Mitigation:
  - Round one is "round-trip with a C-generated `dump.rdb`" — load only.
    Use Valkey's own `redis-cli --rdb` against C-Valkey to produce a
    reference file, succeed iff our loader reproduces the keyspace.
  - Round two is "save + reload through ourselves." Then "C-saves, we load"
    and "we save, C loads" as wire-diff oracle modes.

### Persistence: AOF

- **Recommendation: do not implement in Def 3.** AOF is 2920 LoC of
  rewrite-during-write state machine plus `aof-use-rdb-preamble` plus
  fsync policy. The marginal value over RDB for a read-mostly cache is
  zero. Stub `BGREWRITEAOF` / `WAITAOF` to `+OK` no-ops; reject
  `appendonly yes` at config time with `ERR AOF not supported`. Record
  in `docs/DEFINITION_4.md` as a future milestone if a user demands it.

### AUTH / minimal ACL

- **Valkey source**: `reference/valkey/src/acl.c` (3504 LoC). We need
  ~5% of it. Headline functions actually required:
  `ACLCheckAuthenticatedUser`, `ACLAuthenticateUser`,
  `time_independent_strcmp`, `ACLHashPassword` (SHA256-of-cleartext),
  the `requirepass` config plumbing in `config.c:2148-2165`.
- **What it does**: client connections start in `c->user = DefaultUser`,
  which has `nopass` unless `requirepass` is set. On `AUTH <pw>` the
  server SHA256s the input and time-independent-compares against the
  stored hash list; on match flips `c->authenticated = 1`. All commands
  except `AUTH`, `HELLO`, `RESET`, `QUIT` reject with `NOAUTH` while
  unauthenticated.
- **Translation strategy**: **minimal hand-port, no ACL framework.**
  Add `Client::authenticated: bool` and `RedisServer::requirepass:
  Option<Vec<u8>>` (storing the SHA256 hash, not cleartext). Implement
  `AUTH password`, `AUTH user password` (reject any user != "default"),
  and gate the dispatcher with a 6-LoC `if !ctx.client.authenticated &&
  !is_auth_exempt(cmd_name) { return Err(NoAuth) }`. Implement `HELLO ...
  AUTH ...` (already partly parsed — see
  `crates/redis-commands/src/connection.rs` HELLO handler).
- **Dependencies in our codebase**: `Client` (`crates/redis-core/src/client.rs`),
  `RedisServer` (one new field), dispatch (`crates/redis-commands/src/dispatch.rs`),
  HELLO handler (`connection.rs`). One new dep: `sha2` crate (already
  whitelisted in the Rust ecosystem; pure-Rust, no openssl).
- **Estimated effort**: 2 rounds. ~250 LoC. Sonnet.
- **Risk**: Low. The watch-out is the dispatcher gate — adding it
  without breaking the 19/19 hand-corpus is the only fiddly part.
  Mitigation: gate is `if let Some(_) = server.requirepass { ... }`, so
  default behaviour is unchanged.

### Eviction policies — `noeviction` + `allkeys-lru`

- **Valkey source**: `reference/valkey/src/evict.c` (647 LoC, 10 functions).
  Critical: `getMaxmemoryState`, `performEvictions`, `evictionPoolPopulate`,
  `freeMemoryGetNotCountedMemory`. We already have a stub
  `crates/redis-core/src/evict.rs` (869 LoC, 71 TODOs).
- **What it does**: before each command that may add memory, call
  `getMaxmemoryState`; if over the limit, sample N candidates from
  random hash buckets, populate the EvictionPool ordered by LRU idle
  time, evict the worst until under the limit or the time budget expires.
- **Translation strategy**: **wire the existing stub to a real memory
  estimator + the LRU clock.** Real memory accounting is a rathole —
  short-circuit it with `RedisDb::approximate_memory()` that returns
  `dict.len() * 80 + sum_of_string_bytes`. Close enough for cache.
  For `allkeys-lru`, add `RedisObject::lru_clock: u32` (we already have
  the field), bump it on every `lookup_key_read` non-NOTOUCH path.
  Sample 5 keys (`maxmemory-samples` default), keep the oldest.
  Reject `volatile-*`, `allkeys-lfu`, `volatile-lfu`,
  `allkeys-random` at config-set time with `ERR ...not supported`.
- **Dependencies in our codebase**: `evict.rs` (already exists),
  `object.rs` (lru field), `db.rs` (memory estimator), `command_context.rs`
  (call `perform_evictions` on the write path).
- **Estimated effort**: 3 rounds. ~600 LoC net change. Sonnet for the
  sampling loop, Opus for the integration into `CommandContext::dispatch`
  (subtle: must run before the command, but skip on read-only commands —
  the existing command spec has the `write` flag).
- **Risk**: Medium. Real Redis's memory accounting is exact (zmalloc
  tracks every alloc); ours is approximate. If `maxmemory 100mb` and we
  evict at 100mb of our-estimator-bytes but real RSS is 250mb, operators
  will be confused. **Recommendation**: document the approximation in
  `INFO memory` output (`used_memory_estimated: true`). Don't try to
  match RSS.

### Active expiration cycle

- **Valkey source**: `reference/valkey/src/expire.c` (1031 LoC). We have
  a half-port at `crates/redis-core/src/expire.rs` (1020 LoC, 43 TODOs).
  The hard half is done; the missing half is **calling it from a cron
  driver**.
- **What it does**: `serverCron` fires at `server.hz` (default 10 Hz)
  and calls `activeExpireCycle(ACTIVE_EXPIRE_CYCLE_SLOW)`. Each call
  samples up to `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP=20` keys from
  `db->expires` per DB, deletes the expired ones, repeats if >25%
  were expired.
- **Translation strategy**: **build the cron, fix the existing port's
  blockers, defer kvstore.** Two unblocks: (1) `RedisDb::expires`
  secondary index — add a `HashSet<RedisString>` of "keys with a TTL"
  alongside the main dict; populated by `set_expire`, drained by
  `remove_expire` and `sync_delete`. Iterate this set in the cron
  instead of scanning the whole dict. (2) Wire a 100 ms timer thread
  (single-threaded server) or a tokio interval (if we adopt tokio in
  this milestone) that calls `active_expire_cycle(SLOW)`.
- **Dependencies in our codebase**: `db.rs` (new expires index),
  `expire.rs` (already 90% there), `redis-server/main.rs` (timer thread).
- **Estimated effort**: 2 rounds. ~250 LoC net change. Sonnet.
- **Risk**: Medium. Active expiration changes TCL test timing. The
  TCL `unit/expire` file is already race-flaky (per dashboard:
  "33% of runs see only 3 passes"); making expiration more aggressive
  may stabilise or destabilise. Mitigation: gate behind a config flag
  (`active-expire-effort`, default 1) so we can dial it down if needed.

### Keyspace event notifications

- **Valkey source**: `reference/valkey/src/notify.c` (159 LoC, 3
  functions). We have a port at `crates/redis-core/src/notify.rs`
  (320 LoC) — the constants and flag-parsing are done.
- **What it does**: when a key is modified, if `notify-keyspace-events`
  config bitmask matches the event class, publish a `__keyspace@<db>__:<key>`
  channel message (payload = event name) and/or `__keyevent@<db>__:<event>`
  (payload = key name).
- **Translation strategy**: **wire the existing `notify_keyspace_event`
  function into the ~30 call sites that need it.** The function exists;
  the call sites (`set`, `del`, `expire`, `lpush`, `sadd`, `zadd`, …)
  have TODO(port) markers. Use PUBSUB infrastructure — we already have
  `crates/redis-core/src/pubsub_registry.rs`.
- **Dependencies in our codebase**: notify.rs (exists), every write
  command site (many small edits), pubsub_registry.rs (exists). The
  config string `notify-keyspace-events` needs CONFIG SET wiring.
- **Estimated effort**: 2 rounds. ~30 mechanical call-site edits +
  PUBSUB integration. Sonnet — parallel-friendly (split call sites
  across 3 agents).
- **Risk**: Low. Mostly mechanical. Watch-out: TCL tests for
  `keyspace notifications` are currently in the `--skiptest` list
  because they hang (per dashboard); unblocking them is a Def 3 win.

### CONFIG SET semantics — behavioural hooks

- **Valkey source**: `reference/valkey/src/config.c` (3708 LoC). The
  core is the `standardConfig static_configs[]` static table (205 entries
  per grep). We have a 2811-LoC half-port at
  `crates/redis-core/src/config.rs` (121 TODOs).
- **What it does**: declarative table mapping config name → field on
  `redisServer` + getter/setter/applier callbacks + validator + default.
  CONFIG SET parses the name, looks up the entry, validates the value,
  writes it to the field, calls the `apply` callback if defined.
- **Translation strategy**: **static-gen the schema table from `config.c`,
  hand-port the 12 configs that need behavioural apply()s.** Build a
  Python extractor (~150 LoC) that parses the `createIntConfig(...)` /
  `createStringConfig(...)` / `createEnumConfig(...)` macros via regex,
  emits `crates/redis-core/src/generated/config_table.rs` with a
  `pub const CONFIG_SCHEMA: &[ConfigEntry] = &[ ... ];`. The 12 that
  need real apply() hooks:
  `maxmemory`, `maxmemory-policy`, `maxmemory-samples`,
  `requirepass`, `maxclients`, `databases`, `hz`,
  `notify-keyspace-events`, `slowlog-log-slower-than`,
  `slowlog-max-len`, `latency-monitor-threshold`, `save` (RDB save
  points). Everything else: storage-only — accept and return the value
  but don't act on it.
- **Dependencies in our codebase**: replaces most of the
  hand-port in `config.rs`. Touches every behavioural config consumer.
- **Estimated effort**: 4-5 rounds. ~400 LoC for the extractor + the
  12 apply()s + dispatch. Opus for the extractor (one-shot, high
  leverage), Sonnet for the apply()s.
- **Risk**: Low. The static table is a one-way export from C; we can
  re-extract any time Valkey adds configs. Watch-out: enum value
  strings (e.g. `maxmemory-policy noeviction|allkeys-lru|...`) must
  match the Valkey strings exactly because the TCL tests compare.

### SLOWLOG + LATENCY

- **Valkey source**: `reference/valkey/src/latency.c` (776 LoC),
  `commandlog.c` (not in `reference/valkey/src/` per grep — appears
  consolidated; we have a port at `crates/redis-core/src/commandlog.rs`
  at 786 LoC and `latency.rs` at 1044 LoC).
- **What it does**: SLOWLOG = ring buffer of (id, timestamp, duration,
  argv) for commands over `slowlog-log-slower-than` microseconds.
  LATENCY = ring buffer of (event-name, timestamp, latency) samples
  for tagged events (`expire-cycle`, `fork`, `aof-write`, etc.) over
  `latency-monitor-threshold` ms.
- **Translation strategy**: **wire the existing ports to real measurements.**
  Both ports are already structurally complete. Missing: (a) the dispatch
  hook in `CommandContext::dispatch` that times the command and calls
  `commandlog_push` if over threshold, (b) the `latency_add_sample_if_needed`
  call sites in `expire.rs`, `evict.rs`, fork paths (no fork yet but
  RDB save will introduce one).
- **Dependencies in our codebase**: dispatch.rs, expire.rs, evict.rs,
  the new rdb code.
- **Estimated effort**: 2 rounds. ~150 LoC net. Sonnet.
- **Risk**: Low.

### Connection limits (`maxclients`)

- **Valkey source**: `reference/valkey/src/networking.c` —
  `acceptCommonHandler:2851`, `clientsCron:5512`.
- **What it does**: on accept, if `listLength(server.clients) >=
  server.maxclients`, write `-ERR max number of clients reached\r\n`
  and close the FD.
- **Translation strategy**: **add a check in the accept loop.** Trivial.
- **Dependencies in our codebase**: `crates/redis-server` accept loop
  (search for `TcpListener::accept`), `RedisServer::maxclients` (already
  has `bind_addrs`, just add this).
- **Estimated effort**: 0.5 round (folded into round 14). ~30 LoC.
- **Risk**: None.

## Round-by-round plan

Three phases. Each row is one round; "parallel" = agents launched in the same
fanout. Cost estimates use Sonnet @ $3/Mtok input / $15/Mtok output, Opus @
$15/$75. Per-round token estimate ~25-50k I + 15-30k O.

### Phase 1 — observability (rounds 14-17, ~$30)

| # | Focus | Model | Parallel | Est tokens | Blocks on |
|---|---|---|---|---|---|
| 14 | `maxclients` enforcement + dispatch timing hook | Sonnet | 1 | 30k/15k | nothing |
| 15 | Wire SLOWLOG + LATENCY measurements to dispatch | Sonnet | 1 | 40k/20k | 14 |
| 16 | Behavioural CONFIG SET for the 12 hot configs | Opus | 1 | 60k/35k | 15 |
| 17 | Config static-gen extractor (`harness/extract_config_schema.py`) | Opus | 1 | 50k/30k | 16 |

Ship as **Def 3.1: observable cache.** Operators can now see slow queries,
adjust runtime configs that actually do something, and the connection
floodgate has a ceiling.

### Phase 2 — bounded memory (rounds 18-23, ~$50)

| # | Focus | Model | Parallel | Est tokens | Blocks on |
|---|---|---|---|---|---|
| 18 | `RedisDb::expires` secondary index + drain hooks | Sonnet | 1 | 30k/15k | nothing |
| 19 | Cron driver (100ms timer thread) + wire `active_expire_cycle` | Sonnet | 1 | 35k/20k | 18 |
| 20a | Notify call-sites: string + list + hash (parallel) | Sonnet | 3 | 25k/15k ea | 18 |
| 20b | Notify call-sites: set + zset + generic (parallel) | Sonnet | 3 | 25k/15k ea | 18 |
| 21 | AUTH + requirepass + dispatcher gate | Sonnet | 1 | 30k/15k | nothing |
| 22 | Memory estimator (`RedisDb::approximate_memory`) + LRU touch | Sonnet | 1 | 35k/20k | 18 |
| 23 | Eviction integration into `CommandContext::dispatch` + INFO memory | Opus | 1 | 55k/30k | 22 |

Ship as **Def 3.2: bounded cache.** Won't OOM under load. Active expiration.
AUTH. Real keyspace events for clients that depend on them.

### Phase 3 — persistence (rounds 24-32, ~$140)

| # | Focus | Model | Parallel | Est tokens | Blocks on |
|---|---|---|---|---|---|
| 24 | `crates/redis-persist` crate scaffold + `rio` buffered I/O | Sonnet | 1 | 35k/20k | nothing |
| 25 | CRC64 port (tabular, mechanical) + `rdb.h` type constants static-gen | Sonnet | 1 | 30k/15k | 24 |
| 26 | RDB framing: header, opcodes, AUX, SELECTDB, EOF, CRC | Opus | 1 | 60k/35k | 25 |
| 27a | String encode/decode (int/embstr/raw) | Sonnet | 1 | 30k/15k | 26 |
| 27b | List encode/decode (quicklist + listpack) | Sonnet | 1 | 35k/20k | 26 |
| 27c | Hash encode/decode (listpack + hashtable) | Sonnet | 1 | 35k/20k | 26 |
| 27d | Set encode/decode (listpack + intset + hashtable) | Sonnet | 1 | 35k/20k | 26 |
| 27e | ZSet encode/decode (listpack + skiplist) | Sonnet | 1 | 40k/22k | 26 |
| 28 | SAVE + BGSAVE commands (fork-based for BGSAVE) | Opus | 1 | 55k/30k | 27a-e |
| 29 | Startup RDB load path (in `redis-server/main.rs`) | Sonnet | 1 | 30k/15k | 27a-e |
| 30 | Wire-diff oracle round-trip: C-saves → we-load | Opus | 1 | 40k/20k | 29 |
| 31 | Wire-diff oracle round-trip: we-save → C-loads | Opus | 1 | 40k/20k | 28 |
| 32 | Stub AOF commands (BGREWRITEAOF / WAITAOF as +OK no-ops; reject `appendonly yes`) | Sonnet | 1 | 20k/10k | nothing |

Ship as **Def 3.3: persistent cache.** Survives restart with the keyspace intact.

**Totals**: 26 rounds (14-32, with 27a-e + 20a/20b counted individually);
~$220-300 in tokens; ~30-40 hours of session wall-clock at current cadence
(1.5 rounds/session average).

## Chassis upgrades that would speed this up

### 1. Static generation (the user's open suggestion) — recommended, build first

Concretely, four registry-shaped surfaces in Def 3 benefit:

| Surface | Source | Output | Saves |
|---|---|---|---|
| CONFIG schema | `config.c` `static_configs[]` (205 entries) | `crates/redis-core/src/generated/config_table.rs` | ~1.5 rounds |
| RDB type IDs | `rdb.h` `RDB_TYPE_*` #defines (~30 entries) | `crates/redis-persist/src/generated/rdb_types.rs` | ~0.3 rounds |
| Eviction policy names | `evict.c` MAXMEMORY_FLAG_* + name table | `crates/redis-core/src/generated/eviction_policies.rs` | ~0.2 rounds |
| Keyspace event flags | `notify.c` `NOTIFY_*` + char-map | already in `notify.rs` — no extract | 0 |

**Cost to build**: ~$15-20 for `port-harness/lib/extractor.py` (a generic
"regex out C macros into structured Rust" tool) plus per-surface adapter
scripts. Builds on the existing `harness/gen-command-registry.py` pattern.

**Payoff**: shaves ~2 rounds on Def 3 directly, but the real win is
**proving the pattern** before nginx (which has many more static tables
like `ngx_modules[]` and `ngx_http_variables[]`). Recommendation: **build
it in round 17 as the CONFIG-schema extractor**, then reuse for RDB
in round 25.

### 2. Worktree-per-worker isolation (V2 Tier 1) — defer

Round-by-round plan shows ~9 parallel-agent rounds (20a, 20b, 27a-e plus
a few small ones), maybe 18-20 parallel agent invocations total. V2_PRIORITIES
notes "20 of 24 valid translations got orphaned" in a single fanout — that's
a ~17% loss rate. At ~20 invocations, the expected loss is ~3-4 rounds
worth of work re-run, ~$15-25.

The chassis fix per V2 Tier 1 is "$20 of chassis work." **The math is a
wash.** But: worktrees also pay off for nginx and any future port.
**Recommendation: build it once at the start of Phase 2** (between rounds
17 and 18), absorb the $20 cost, and harvest the savings across rounds
20, 27, and all future ports.

### 3. Tighter "always-rebuild-before-measure" discipline — yes, trivial

Per the prompt: "Round 9/10 baseline ran against stale binary." Cost is
trivial. **Recommendation: add a `harness/loop/pre-measure-hook.sh`** that
runs `cargo build --release -p redis-server` before any TCL run and aborts
if the binary's mtime is older than any source file in `crates/*/src/`. 30
LoC of bash. Include in the chassis Tier-3 fixture for regression coverage.

### 4. Pre-bundled context packets to shrink prompts — defer, measure first

Per-agent prompts are 3-5k tokens of which 2-3k is the same five files
re-read each round (PORTING.md, type-vocabulary.tsv, file-deps.tsv, plus
the one C source being translated and its target Rust file). Over 26
rounds × 3-5 parallel agents = ~100 agent invocations × 2.5k redundant
tokens = ~250k tokens × $3/Mtok = ~$0.75 saved. **Not worth a chassis
investment yet.** Revisit at nginx if total agent counts rise above ~200.

### Priority order for chassis investment

1. **Always-rebuild-before-measure** (round 14 prep, ~$1). Trivial,
   prevents the next "stale-binary baseline" loss.
2. **Static-gen extractor** (round 17, ~$15-20). Validates the pattern,
   shaves Def 3 rounds, sets up nginx.
3. **Worktree-per-worker** (round 18 prep, ~$20). Pays for itself within
   Def 3 via parallel fanouts.

Skip context-packets and PR-per-file (V2 Tier 6) for now.

## Risk register

1. **RDB format fidelity** (highest). Each object encoding has a separate
   serialize path. A 1-byte mismatch is silent until reload. Mitigation:
   wire-diff oracle in rounds 30-31 runs both directions (C↔us). Encode
   each type with adjacent `// C: rdb.c:NNNN-NNNN` comments so reviewers
   can diff visually.
2. **Active expiration timing changes TCL `unit/expire` results.** The
   file is already race-flaky (33% of runs see only 3 passes). Adding
   aggressive expiration may stabilise or destabilise. Mitigation: gate
   behind `active-expire-effort` config (default 1, minimum). If TCL
   regresses, dial to 0 = lazy-only.
3. **ACL scope-creep.** Easy to drift into "well, while we're here, let's
   add ACL SETUSER." Hard cap: AUTH + requirepass only. Users != "default"
   get rejected. Document in `DEFINITION_4.md` as a future milestone.
4. **Memory estimator drift from RSS.** Our estimate is dict-len ×
   approximate-bytes; real Valkey uses zmalloc tracking. Operators
   setting `maxmemory 100mb` may see RSS hit 250mb. Mitigation: emit
   `used_memory_estimated: true` in INFO memory; document the
   approximation in DEFINITION_3.md.
5. **CONFIG SET behavioural drift.** Our 12 apply()s vs Valkey's ~60 —
   tests for the 48 we don't behaviorally implement may pass (CONFIG GET
   returns the stored value) but the system behaviour won't change. The
   TCL coverage of CONFIG behaviour is light, so the risk of being caught
   is low; the risk of an operator being surprised is medium. Mitigation:
   add a `CONFIG GET --apply-status` extension that reports
   "set/stored-only" per config. (Or punt — most operators won't notice.)

## Estimates

- **Total agent rounds**: 26 (range: 23-32 depending on retries)
- **Estimated total tokens**: ~3.5M input, ~1.8M output across all rounds.
- **Estimated wall-clock**: 25-35 session-hours at current cadence
  (1.5 rounds/session avg).
- **Estimated dollar cost** (API-direct): $220-300 best case, $400 if
  RDB rounds 26-28 need re-runs (likely — RDB is the surface that always
  needs a second pass).

## Decision points — operator input required

1. **AOF: confirm out of scope for Def 3?** The current plan stubs
   BGREWRITEAOF as a no-op and rejects `appendonly yes`. If you want
   real AOF, add ~4 rounds and ~$50.
2. **AUTH: single-password requirepass enough, or do you need
   AUTH user password too?** Single-user is 2 rounds; multi-user with
   even a stub ACLUser struct is 4-5 rounds because the dispatcher gate
   has to consult a per-user permission set.
3. **Memory estimator: approximate-ok, or invest in real allocator
   tracking?** Real tracking would require a custom global allocator
   (`#[global_allocator]`) with per-allocation accounting. That's a
   separate ~5-round chassis-style effort and might be worth its own
   milestone (Def 4 candidate).
4. **Active expiration: introduce a real timer thread, or take the
   tokio dependency now?** Single-threaded server with a `std::thread::spawn`
   100ms loop is 30 LoC. Tokio rework is a separate ~3-round adventure
   that touches every command path. **Recommendation: spawn-thread for
   Def 3, tokio in a separate Def 4 milestone.**
5. **`save` config behaviour: support multi-tuple "save 3600 1 300 100"
   syntax, or single-snapshot only?** Multi-tuple is faithful to Valkey
   but adds a ~50-LoC parser. Single-tuple is enough for CI/dev caches.
6. **Static-gen and worktree chassis investments: green-light $40 of
   chassis work before Phase 2?** Recommendation: yes, payoff is across
   the whole port.
