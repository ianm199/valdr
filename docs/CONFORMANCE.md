# Conformance

Current coverage source of truth:
[`TEST_AND_FEATURE_COVERAGE.md`](TEST_AND_FEATURE_COVERAGE.md). Older
numbers in this file are historical unless they match a fresh artifact.

This document is the load-bearing answer to "does it actually work like
Valkey?" It captures the current state across three independent oracles
that verify every commit.

For each oracle, the numbers below are the current alpha-release state.
They are reproducible; see
[Reproducing](#reproducing) at the bottom.

## At a glance

| Oracle | Status |
|---|---|
| **Wire-diff smoke** (23 RESP corpus scripts vs upstream Valkey, byte-exact) | **23 / 23 PASS** ✅ |
| **RDB bidirectional oracle** (we save → C loads; C saves → we load) | **378 / 378 PASS** ✅ |
| **Upstream Valkey TCL suite** | Single-node core green; full denominator 4,299 blocks, bucketed in [`TEST_AND_FEATURE_COVERAGE.md`](TEST_AND_FEATURE_COVERAGE.md) |
| **`unsafe` budget** | **14 first-party blocks**, tracked by `harness/unsafe-budgets.toml` |

## TCL Suite Accounting

All TCL numbers — counted passes, source-block coverage, the full-suite
denominator, and how the non-single-node surface is bucketed — live in one
place: [`TEST_AND_FEATURE_COVERAGE.md`](TEST_AND_FEATURE_COVERAGE.md). That
document is regenerated from fresh `run-single-node-tcl-suite.sh` artifacts.
This file does not duplicate those counts; the explanation below is about the
*mechanism* behind no-summary files, which the numbers alone do not convey.

Do not read any single TCL number as "the whole upstream suite" unless it uses
the full denominator. On this checkout the full upstream suite is 245 `.tcl`
files and 4,299 `test` blocks under `reference/valkey/tests/`; the single-node
core runner covers a documented subset and the rest is bucketed (cluster,
modules, integration, Sentinel, platform, persistence/robustness frontier) in
the source-of-truth doc.

### Why "abort / no-summary" exists, and what unlocking one means

A no-summary file is not *failing* — it is *hidden* behind an early file abort.
The single-node core surface is now almost entirely past this stage (see the
source-of-truth doc for the current no-summary list), but the mechanism still
governs the remaining frontier files (integration, replication, and the
persistence/robustness frontier), so understanding it is the difference between
reading the scoreboard correctly and chasing the wrong fixes.

Each `.tcl` file runs its `test {name} {body} {expected}` blocks sequentially.
The framework's `test` proc (`reference/valkey/tests/support/test.tcl:262`)
runs each body inside a `catch`, but only swallows two error classes
gracefully:

- `assertion:*` — a failed `assert_*` → records `[err]`, **continues** to the
  next test.
- anything at all, **but only if `--durable` is set** → caught, **continues**.

Every other error hits the `else` branch (`test.tcl:294`,
`error $error $::errorInfo`) and is **re-raised**. It propagates out of `test`,
out of the file, and `tclsh` exits non-zero **before printing the summary
line**. `tcl-survey.py` then classifies the whole file as `no-summary`
(`passed: null`, `total: 0`) and records the last-running test as `abort_test`.

The errors that trigger this are *raw runtime errors*, not assertions — most
commonly a script reaching for a global our sandbox does not yet provide
(`attempt to index global 'bit' / 'os' (a nil value)`), or a missing command.
The Tcl `r` client surfaces a server `-ERR` reply as a Tcl error, and since it
does not start with `assertion:`, it detonates the file.

Consequence: **the first non-assertion error in a file hides every test after
it.** So the high-leverage move is unlocking the *blocker*, not fixing wording
in an already-running test.

What "unlocking" actually buys, stated honestly (historical worked example —
the `tcl-scripting-bit-lib-v1` packet, 2026-05-24; `unit/scripting.tcl` has
since been fully unlocked and now reaches its summary, see the source-of-truth
doc):

- `unit/scripting.tcl` has **186** `test` blocks. It was aborting at the first
  `bit.*` use (line 574). Installing the `bit` global moved the abort to the
  next missing global, `os.clock()` (line 784).
- That advanced the frontier past **20** `test` blocks (575–784) that now
  *execute* instead of being skipped-by-abort. Because a passing run simply
  proceeds while a failed assertion would be caught-and-continued, reaching 784
  is positive evidence those 20 ran clean.
- **But the file still aborts** (now at `os`), still emits no summary, and is
  **still counted as `no-summary` with `total: 0`.** Moving an abort frontier
  is *not* the same as adding counted passes. A file only converts to counted
  numbers when execution reaches its end-of-file summary — i.e. when every
  non-assertion-error detonator between here and EOF is cleared. It is a chain
  of detonators; each unlock defuses one.

Operational lever: a survey pass *with* `--durable` records each non-assertion
error as a failure and keeps going to the summary, so it reveals the *counted*
ceiling a file would reach "if it didn't abort." The default survey runs
**without** `--durable` on the conservative assumption that a non-assertion
error may leave the server/connection wedged and produce garbage downstream.
For clean Lua "nil global" errors that assumption is usually false (the
connection is intact), so a one-off `--durable` run is a cheap way to quantify
how much real signal sits behind a given abort — but it is a diagnostic, not
the conformance number of record.

### Attack strategy: chase the detonator chain, file by file

Because ~40% of the single-node-core denominator is hidden behind aborts (and
~13% behind timeouts), the highest-leverage work is *unhiding* whole files, not
fixing wording in tests that already run. The loop:

```
Phase 1 — UNHIDE:  find the abort → implement the missing global/command →
                   re-run → find the next abort → repeat UNTIL the file reaches
                   its summary line.
Phase 2 — CONFORM: the file's tests are now visible and counted → grind err->ok.
```

Three rules keep this honest:

1. **A frontier advance pays nothing until the file flips.** Killing one
   detonator in a file that has several moves the dashboard by zero — every
   block stays in the abort lump until the file runs to its summary. The unit
   of payout is the *file flipping to summary*, not the individual unlock. So
   commit to clearing a file's whole chain, or don't start it.

2. **Scout with `--durable` before committing.** A one-off durable pass (a
   diagnostic, never the number of record) reveals, in one shot: how many
   detonators are in the chain, the pass-ceiling (how many blocks would land in
   `proved` vs `known-fail` once the file runs), and which "fails" are real vs
   reply-desync noise. That decides whether a file is worth it before investing:
   a 2-detonator chain guarding 150 easy passes is a jackpot; a 6-detonator
   chain guarding 10 passes is not.

3. **"Unhide" is not "proved".** When a file flips, its blocks split into
   `proved` (the ones that pass) and `known-fail` (the ones that fail). Chasing
   aborts buys *visibility*; only the passing subset buys *proved*. Estimate the
   proved-gain from the durable `ok` ceiling, not from the file's block count.

Prioritize across files by `(blocks hidden) x (chain is short / ceiling is
high)`. This is why the unlock waves open with files like `unit/hashexpire`
(226 blocks aborting on one missing `HGETEX`) rather than a one-line wording
fix. Timeouts are the parallel hidden bucket — same leverage, different failure
mode (a hang or busy-loop instead of a raised error).

Harness-product direction: an agent should not have to *rediscover* the next
abort by re-running. The durable scout should be promoted to a pre-computed
**per-file detonator map** (ordered chain + ok/fail ceiling), so each unlock
packet is handed its exact position in the chain instead of groping forward.

## Wire-diff smoke

The 23 scripts in `harness/oracle/corpus/` are sent to both
`reference/valkey/src/valkey-server` and `target/debug/redis-server` on
parallel sockets, and the raw RESP replies are compared byte-for-byte.
Any single-byte divergence fails the script.

| Script | Coverage |
|---|---|
| `01-ping` | PING with no arg + with echo |
| `02-echo` | ECHO basic |
| `03-set-get` | SET / GET / SET EX / NX / XX |
| `04-del-exists` | DEL / EXISTS multi-key |
| `05-incr` | INCR / DECR / non-numeric error path |
| `06-string-ext` | APPEND / STRLEN / SETRANGE / GETRANGE / SUBSTR |
| `07-keyops` | TYPE / RENAME / SCAN / KEYS / OBJECT ENCODING |
| `08-server` | INFO / DBSIZE / TIME / CONFIG GET/SET / SELECT |
| `08b-list` | LPUSH / RPUSH / LPOP / RPOP / LRANGE / LLEN / LINDEX / LSET / LREM / LTRIM / LINSERT / LMOVE |
| `09-hash` | HSET / HGET / HDEL / HMGET / HKEYS / HVALS / HINCRBY |
| `10-set` | SADD / SREM / SMEMBERS / SINTER / SUNION / SDIFF / SCARD / SMOVE |
| `11-zset` | ZADD / ZRANGE / ZSCORE / ZINCRBY / ZRANGEBYSCORE / ZRANGEBYLEX / ZREM / ZCOUNT |
| `12-ttl` | EXPIRE / EXPIREAT / TTL / PTTL / PERSIST / SET EX / KEEPTTL / PEXPIRE |
| `13-scan-zset-extras` | SCAN cursor / ZRANGEBYSCORE LIMIT / ZSCAN / HSCAN |
| `14-pubsub` | SUBSCRIBE / PUBLISH / PSUBSCRIBE / UNSUBSCRIBE |
| `15-tx` | MULTI / EXEC / DISCARD / WATCH / DISCARD-after-error |
| `16-bitmap` | SETBIT / GETBIT / BITCOUNT / BITOP / BITPOS |
| `17-streams` | XADD / XRANGE / XLEN / XREAD / XDEL / XINFO |
| `18-hll` | PFADD / PFCOUNT / PFMERGE |
| `19-geo` | GEOADD / GEODIST / GEOPOS / GEOSEARCH / GEORADIUS |
| `20-edge-cases` | 52 commands covering historical regressions (`i64::MIN` arg crashes, CONFIG RESETSTAT, MSETEX, set encoding stickiness, EXPIRE-already-expired, ziplist config aliases, etc.) |
| `21-runtime-owner-canaries` | Runtime-owner compatibility canaries |
| `22-dump-restore` | DUMP payload byte-exactness and missing-key nil behavior |

Run: `bash harness/oracle/smoke.sh --skip-build`

## RDB bidirectional oracle

The 7 corpora in `harness/oracle/rdb-corpus/` exercise each encoding:

- `01-strings-basic` — raw, embstr, int encodings
- `02-strings-edge` — boundary lengths, integer-encoded numbers, embstr ↔ raw transitions
- `03-hashes` — listpack + hashtable, mixed encodings, large values
- `04-sets` — intset + listpack + hashtable encodings
- `05-lists` — listpack + quicklist
- `06-zsets` — listpack + skiplist
- `07-streams` — listpack-encoded entries with consumer groups

Three directions are tested per corpus:

- **A: we save → C loads.** Our `SAVE` writes an RDB; we load it into
  the C binary; we diff the resulting keyspace using DEBUG OBJECT.
- **B: C saves → we load.** Real Valkey writes an RDB; we load it; we
  diff. Catches loader compatibility bugs.
- **C: byte-exact informational.** Not gating, but tracked.

54 checks × 7 corpora = 378 total. **All passing.**

Run: `python3 harness/oracle/rdb-diff --direction=all`

## Upstream Valkey TCL suite

The full upstream test infrastructure at `reference/valkey/tests/` runs
against our binary via a symlink trick — `harness/oracle/setup_tcl_runner.sh`
creates `target/debug/valkey-server` as a symlink to our binary, so the
unmodified TCL harness launches our server without modification.

The per-file matrix — every discovered single-node file, its source-block
count, and its counted pass/fail/no-summary status — is maintained in
[`TEST_AND_FEATURE_COVERAGE.md`](TEST_AND_FEATURE_COVERAGE.md) and regenerated
from fresh `run-single-node-tcl-suite.sh` runs. It is not duplicated here so
the two cannot drift apart.

The survey records abort/no-summary cases separately from counted pass/fail
cases, so coverage is never hidden behind a single aggregate number. The deny
policy is part of the meaning of any number: the single-node profile denies
`needs:repl`, `needs:debug`, cluster, and external-replication tags, resolved
from one place (`tcl-survey.py`'s `DENY_TAG_PROFILES`).

Outside the single-node core claim, bucketed in the source-of-truth doc:

- `unit/cluster` — needs cluster/product decision and runner support
- `unit/moduleapi` — needs module-ABI product decision
- `unit/replication` requires multi-node infrastructure (Session 3
  established our backbone but full replication conformance isn't yet
  swept)
- `unit/tls`, `unit/io-threads`, `unit/mptcp`, etc. — infrastructure edges
  deferred from the current product claim

## Command coverage

A non-exhaustive list of confirmed-working command families. Each one
either has wire-diff coverage in the smoke corpus, TCL coverage in the
matrix above, or both.

### Strings & numerics
GET • SET • SETNX • SETEX • PSETEX • GETSET • GETDEL • GETEX • MGET • MSET •
MSETNX • MSETEX • APPEND • STRLEN • GETRANGE • SETRANGE • SUBSTR •
INCR • INCRBY • INCRBYFLOAT • DECR • DECRBY

### Bitmap
SETBIT • GETBIT • BITCOUNT • BITOP (AND/OR/XOR/NOT) • BITPOS • BITFIELD

### Lists
LPUSH • RPUSH • LPUSHX • RPUSHX • LPOP • RPOP • LLEN • LRANGE • LINDEX •
LSET • LREM • LTRIM • LINSERT • LMOVE • LMPOP • RPOPLPUSH • LPOS •
BLPOP • BRPOP • BLMOVE • BRPOPLPUSH • BLMPOP

### Hashes
HSET • HGET • HGETALL • HDEL • HEXISTS • HKEYS • HVALS • HLEN • HMGET •
HMSET • HSETNX • HRANDFIELD • HSCAN • HINCRBY • HINCRBYFLOAT • HSTRLEN

### Sets
SADD • SREM • SMEMBERS • SISMEMBER • SMISMEMBER • SCARD • SDIFF •
SDIFFSTORE • SINTER • SINTERSTORE • SINTERCARD • SMOVE • SPOP •
SRANDMEMBER • SUNION • SUNIONSTORE • SSCAN

### Sorted sets
ZADD • ZRANGE • ZRANGEBYSCORE • ZRANGEBYLEX • ZRANGESTORE • ZREVRANGE •
ZREVRANGEBYSCORE • ZREVRANGEBYLEX • ZSCORE • ZMSCORE • ZRANK • ZREVRANK •
ZINCRBY • ZADD INCR/XX/NX/GT/LT/CH • ZCARD • ZCOUNT • ZLEXCOUNT •
ZREM • ZREMRANGEBYRANK • ZREMRANGEBYSCORE • ZREMRANGEBYLEX • ZINTER •
ZUNION • ZDIFF • ZINTERSTORE • ZUNIONSTORE • ZDIFFSTORE • ZINTERCARD •
ZPOPMIN • ZPOPMAX • ZMPOP • BZPOPMIN • BZPOPMAX • BZMPOP • ZSCAN •
ZRANDMEMBER

### Streams
XADD • XDEL • XLEN • XRANGE • XREVRANGE • XREAD • XREAD BLOCK • XTRIM •
XLEN • XSETID • XGROUP CREATE • XGROUP DESTROY • XGROUP CREATECONSUMER •
XGROUP DELCONSUMER • XGROUP SETID • XREADGROUP • XREADGROUP BLOCK •
XACK • XCLAIM • XAUTOCLAIM • XPENDING • XINFO

### HyperLogLog
PFADD • PFCOUNT • PFMERGE

### Geo
GEOADD • GEODIST • GEOPOS • GEOHASH • GEOSEARCH • GEOSEARCHSTORE •
GEORADIUS • GEORADIUSBYMEMBER

### Pub/Sub
SUBSCRIBE • UNSUBSCRIBE • PSUBSCRIBE • PUNSUBSCRIBE • PUBLISH •
PUBSUB CHANNELS/NUMSUB/NUMPAT • SSUBSCRIBE / SPUBLISH (sharded — single-node)

### Transactions
MULTI • EXEC • DISCARD • WATCH • UNWATCH

### Scripting
EVAL • EVALSHA • SCRIPT LOAD • SCRIPT EXISTS • SCRIPT FLUSH •
redis.call / redis.pcall / KEYS[] / ARGV[] / SHA1HEX inside Lua

### Keyspace
DEL • EXISTS • TYPE • RENAME • RENAMENX • KEYS • SCAN • RANDOMKEY •
TOUCH • UNLINK • COPY • COPY DB • MOVE • DUMP • RESTORE • RESTORE-ASKING

### Server
PING • ECHO • SELECT • DBSIZE • TIME • INFO • INFO keyspace • CONFIG GET •
CONFIG SET • CONFIG RESETSTAT • CONFIG REWRITE • FLUSHDB • FLUSHALL •
SWAPDB • SHUTDOWN • CLIENT • COMMAND • COMMAND INFO • COMMAND LIST •
LASTSAVE • BGSAVE • BGREWRITEAOF • SAVE • DEBUG SLEEP • DEBUG OBJECT •
DEBUG SET-ACTIVE-EXPIRE • DEBUG CHANGE-REPL-ID • DEBUG JMAP

### Persistence
RDB v11 — `SAVE` (synchronous), `BGSAVE` (fork-based on Unix; thread-based
on non-Unix), `--rdb-disabled` flag, dump.rdb load on startup, DUMP/RESTORE
single-key payloads with CRC/version footer validation.

AOF — write per command, `appendfsync always` / `everysec` / `no`,
fsync thread, startup replay including the stream commands (`XADD`,
`XSETID`, `XGROUP CREATE`/`CREATECONSUMER`, `XCLAIM JUSTID FORCE`,
`XDEL`).

### Replication
PSYNC handshake • full-sync RDB transfer • REPLCONF • WAIT •
backlog circular buffer • replica state machine • READONLY enforcement •
replica command-apply loop • periodic ACK thread.

### Eviction
maxmemory + maxmemory-policy: noeviction • allkeys-lru • allkeys-lfu •
allkeys-random • volatile-lru • volatile-lfu • volatile-random •
volatile-ttl. Sample-based evictor. LRU 1-Hz clock thread. LFU 8-bit
log-counter with time-decay.

### ACL
Multi-user with SHA-256 password hashes. Category bitmasks
(`+@admin -@dangerous`, etc.). User commands (ACL GETUSER / SETUSER /
LIST / WHOAMI / CAT). Channel-pattern allow-lists.

### TLS
rustls scaffolding is present, including PEM cert/key loading code, but
in-process TLS is not release-supported on the runtime-owner path. Put a TLS
terminator in front of the container if you need encrypted client connections.

### Native module commands
RedisJSON-compatible: `JSON.SET / GET / DEL / TYPE / NUMINCRBY /
NUMMULTBY / STRAPPEND / STRLEN / OBJKEYS / OBJLEN / ARRAPPEND /
ARRLEN / ARRINSERT / ARRPOP / CLEAR / MGET / FORGET`. JSONPath subset
(`$`, `$.foo`, `$[0]`, `$[-1]`, `$.foo[*]`, `$..foo`).

RedisBloom-compatible: `BF.RESERVE / ADD / EXISTS / MADD / MEXISTS /
INSERT / INFO`.

We do **not** implement the C ABI for loadable `.so` modules. The
above are native Rust implementations of the popular commands.

## Known gaps

### Deliberate (not on the 1.0 roadmap)

- **Clustering** — single-node by design. Use existing Valkey for
  clustered deployments.
- **Loadable C-ABI modules** — security/safety trade-off. We provide
  native implementations of RedisJSON + RedisBloom; others would require
  separate native ports.
- **import-mode / import-source** — Valkey Enterprise feature.

### Valkey 9.0 extensions (planned for 1.0)

- HGETDEL family (HEXPIRE, HEXPIREAT, HPERSIST, HPEXPIRE, HPEXPIREAT,
  HEXPIRETIME, HPEXPIRETIME, HTTL, HPTTL)
- LCS (longest-common-subsequence)
- SET … IFEQ conditional
- HELLO without protover argument
- HELLO availability-zone

### Open bugs (small / known)

The earlier per-test failure list here (stream-cgroups XREADGROUP edges,
`RANDOMKEY` distribution, long-glob matching, `HINCRBYFLOAT` NaN text) is no
longer accurate: those files now pass under the single-node runner. Any current
counted failure or no-summary file is listed in
[`TEST_AND_FEATURE_COVERAGE.md`](TEST_AND_FEATURE_COVERAGE.md), which is the
only place that tracks the live per-file status.

One architectural caveat is independent of the TCL count and still holds:

- BlockedKeysIndex is keyed by `RedisString` only, not `(db_index,
  RedisString)`. A blocked `BLPOP` on key `"x"` in db 0 could be
  spuriously woken by `LPUSH "x"` in db 5. Rare in practice; deferred.

### Performance

The release-candidate benchmark matrix is published as alpha telemetry, not as
production soak evidence. Latest local artifacts:

- `harness/bench/results/20260527T025338Z-2ff3fcc-default-suite-parts.json`:
  21/21 non-function default-suite rows pass, median `1.060x` vs upstream,
  weakest row `0.986x`.
- `harness/bench/results/20260527T025426Z-2ff3fcc-pipeline-smoke.json`:
  12/12 pipeline-smoke rows pass, median `1.133x`; no P100 cliff or timeout.
- `harness/bench/results/20260527T025203Z-2ff3fcc-json-doc-mix.json`:
  3/3 JSON document scenarios pass, median `0.994x`.

See [`BENCHMARKS.md`](BENCHMARKS.md) and
[`RUST_PERFORMANCE_IMPROVEMENT_PLAYBOOK_20260526.md`](RUST_PERFORMANCE_IMPROVEMENT_PLAYBOOK_20260526.md)
for methodology and the exact command lines.

## Reproducing

Wire-diff smoke:

```bash
cargo build --bin redis-server
bash harness/oracle/smoke.sh --skip-build
```

RDB oracle:

```bash
python3 harness/oracle/rdb-diff --direction=all
```

TCL suite against our binary (one unit file):

```bash
cargo build --bin redis-server
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build --files unit/type/zset
```

Canonical single-node unit/type survey:

```bash
bash harness/oracle/run-single-node-tcl-suite.sh
```

See [`TCL_TEST_SUITE_RUNBOOK.md`](TCL_TEST_SUITE_RUNBOOK.md) for the exact
profile, file selection, port rules, and conservative contained survey variant.

The TCL infrastructure requires `tclsh` (`brew install tcl-tk` on
macOS, `apt-get install tcl` on Debian/Ubuntu).
