# TCL baseline — expire / incr / keyspace / other unit/

First runs of the canonical Valkey TCL suite against the Rust server for
key-operations files. Captured 2026-05-17T20:34Z by Round-10c (TCL oracle
triage agent) against fix-state `59bbe91` (Round 9 head; no string.rs /
object.rs fixes from Round 10a were applied at run-time — pre-build only).

## Reproducer

```sh
cd /Users/ianmclaughlin/PycharmProjects/rustExperiments/redis-rs-port
bash harness/oracle/setup_tcl_runner.sh          # builds + symlinks valkey-server
cd reference/valkey
VALKEY_BIN_DIR=$(pwd)/../../target/debug \
  tclsh tests/test_helper.tcl \
  --single unit/<file>                            # e.g. unit/expire
  --clients 1 --skip-leaks \
  --tags "-needs:repl -needs:debug" \
  --timeout 90
```

No `--skiptest` exclusions were added in this round — the only files we
truncated were those where a top-level Tcl exception aborts the test client
before subsequent tests can be enumerated. (See "True hangs" sections; in
practice there are zero true hangs across the five files below.)

## TL;DR

Five new files baselined (plus two cross-check probes). **expire.tcl**
reaches **52/72** scorable; **incr.tcl** reaches **14/15** before
INCRBYFLOAT aborts; **keyspace.tcl** reaches **27/28** before `COPY ... DB
10` aborts. Smaller files: **quit.tcl 3/3 green**, **list-2.tcl 2/2
green**, **limits.tcl 0/1**, **info-command.tcl 1/5**, **auth.tcl 0/4**,
**protocol.tcl 0/0** (aborts on first test), **sort.tcl 0/0** (aborts in
test fixture setup), **scan.tcl 6/10** before a syntax-error abort.

Across all nine files the dominant blockers, in order of leverage:

1. List/Set/Hash/ZSet **OBJECT ENCODING** does not report
   `listpack` / `intset` for small collections (always reports the
   "large" encoding). Aborts the test-fixture setup of `sort.tcl` and
   blocks 5+ asserts in `scan.tcl`, `expire.tcl`, `incr.tcl`'s post-
   `INCRBYFLOAT` body, and the second `start_server` block of
   `expire.tcl` (after `restart_server` race).
2. **INCRBYFLOAT** is unimplemented — blocks 14 tests in `incr.tcl`.
3. Error-message **format** is `ERR invalid expire time in command`; the
   canonical wording is `ERR invalid expire time in 'expire' command`
   (per-command suffix). Blocks 3 EXPIRE big-integer tests.

## expire.tcl
- Pass: **52 / 72** in best run (race-dependent — see notes below)
- Failures (genuine): 9
- Tag-denied skips: 11 (8 needs:repl, 3 needs:debug)
- True hangs (--skiptest needed): **none**
- Tcl-level abort: 1 (CLIENT IMPORT-SOURCE — exception kills the test
  client, ~10 trailing tests in this start_server block become
  unenumerable; the file's second top-level start_server (~15 more
  tests, mostly `needs:repl`) is never reached)

The file contains 81 source-level `test` directives; ~9 are inside the
unreachable second `start_server` block and ~10 are after the
import-source abort. The 72 in the denominator is the count of tests
the harness actually advances to in the best of three runs.

Race note: this file contains a `restart_server`-style test ("Server
should actively expire keys incrementally") that exercises a
`kill_server` cleanup path. On ~33% of runs (1 of 3 in my sampling) the
cleanup races with our server shutdown and the TCL framework throws a
`cat: ./tests/tmp/server.NNN/stderr: No such file or directory`
exception, killing the test client after only 3 tests have run. The
**52/72** number is from the lucky 2/3 of runs.

### Top failure categories
- (a) **Genuine bugs**: 7 tests
  - 3× `EXPIRE/PEXPIRE` big-integer overflow error wording — we say
    `ERR invalid expire time in command`, canonical says
    `ERR invalid expire time in 'expire' command`. (`crates/redis-commands/src/generic.rs`
    or wherever the expire validators live.)
  - 1× `EXPIRE / EXPIREAT / PEXPIRE / PEXPIREAT Expiration time is
    already expired` — asserts `s expired_keys == 1`; our INFO stats
    `expired_keys` does not increment on synchronous-expire-on-set.
  - 1× `Server should actively expire keys incrementally` — active-
    expire job (the `serverCron` 100ms sweep) is not implemented; keys
    only die on access.
  - 2× `EXPIRE with GT/LT on key without TTL` — semantics mismatch:
    Valkey treats "no TTL" as `LT` true / `GT` false; we treat both
    as false. One-liner in `expire_command`.
  - 1× `EXPIRE with negative expiry on a non-volatile key` — variant of
    the above LT-vs-no-TTL semantics.
- (b) **Commands not yet implemented**: 1 family
  - `CLIENT IMPORT-SOURCE` — Valkey-only import-mode feature.
    Unimplemented; throws unknown-subcommand and aborts the test
    client.
- (c) **Infrastructure / framework**: 1 race
  - `restart_server` triggers a `check_sanitizer_errors` that `cat`s a
    stderr file our server may not have flushed yet, killing the test
    client. Not a server bug per se but blocks deterministic baseline.

## incr.tcl
- Pass: **14 / 15** scorable
- Failures: 0
- Tag-denied skips: 1 (needs:debug — "INCR can modify objects in-place")
- True hangs (--skiptest needed): **none**
- Tcl-level abort: 1 (INCRBYFLOAT unimplemented — kills the test client
  on first call; 14 trailing INCRBYFLOAT-family tests unenumerable)

29 source-level tests, 14 of which were reached before the abort. All 14
reached tests passed. The remaining 14 unreachable tests are all
INCRBYFLOAT variants except `string to double with null terminator`,
`No negative zero`, and `INCRBY INCRBYFLOAT DECRBY against unhappy path`
(also INCRBYFLOAT-dependent).

### Top failure categories
- (b) **Commands not yet implemented**: `INCRBYFLOAT` family (14 tests
  blocked by single abort).

## keyspace.tcl
- Pass: **27 / 28** scorable
- Failures: 0
- Tag-denied skips: 1 (needs:debug — "DEL against expired key")
- True hangs (--skiptest needed): **none**
- Tcl-level abort: 1 (`COPY ... DB 10` — our COPY validates the
  destination DB index against a smaller `dbnum`; throws
  `ERR DB index is out of range` and kills the test client)

65 source-level tests; the file makes heavy use of multi-DB operations
(SELECT, COPY DB, MOVE) and immediately aborts on first `COPY ... DB 10`,
so 38 trailing tests are unenumerable. Likely fix: bump default
`databases` config from whatever we use today to 16 (Valkey default), or
fix COPY's DB-validation message to be `ERR DB index is out of range`
without aborting — but the test calls `r copy ... DB 10` followed by
`r select 10`, both of which need the higher DB count.

### Top failure categories
- (a) **Genuine bugs / config gap**: 1
  - Default `databases` < 11. Trivial config fix to unblock ~30
    multi-DB tests.

## Other small TCL files attempted

| file | pass | total | notes |
|------|------|-------|-------|
| quit.tcl              | **3** | 3  | green |
| type/list-2.tcl       | **2** | 2  | green |
| limits.tcl            | 0     | 1  | maxclients accepts 50 conns when limit is 10 — `maxclients` config not enforced |
| info-command.tcl      | 1     | 5  | INFO commandstats omits per-command `rejected_calls`/`calls`/`usec` fields |
| auth.tcl              | 0     | 4  | `AUTH` not implemented (`ERR unknown command 'auth'`); aborts test client after 4 setup errors |
| protocol.tcl          | 0     | 5+ | first test "Handle an empty query" aborts test client — server returns `ERR empty command` on `\r\n` instead of silently ignoring |
| sort.tcl              | 0     | 2+ | aborts in `start_server` body during fixture setup — `assert_encoding listpack` fails because LPUSH of 16 elements reports `quicklist` instead of `listpack` |
| scan.tcl              | 6     | 10  | 4× encoding-assertion failures (SSCAN/HSCAN/ZSCAN — same listpack/intset gap), then test client aborts on `ERR syntax error` (likely an unimplemented SCAN option) |

`total` for protocol/sort/scan reflects what the harness reached; full
source counts are 31, 43, 20 respectively but most are gated by the
encoding fixture setup.

## Recommended fixes (highest-leverage first)

1. **Implement collection `OBJECT ENCODING` aliases**
   (`listpack`, `intset`, `embstr`, `int`). Currently
   `redis-types::object::encoding_name` only returns the "large"
   encoding (`hashtable`, `quicklist`, `skiplist`, `raw`). Adding the
   small-collection promotion logic (or even just reporting the small
   name when `OBJ_ENCODING_LISTPACK`/`OBJ_ENCODING_INTSET` would apply
   in canonical Valkey) unlocks:
   - the `sort.tcl` fixture (43 source tests gated on this single
     assertion at file load time);
   - all 4 `scan.tcl` SSCAN/HSCAN/ZSCAN encoding asserts;
   - the 3 `unit/type/string.tcl` int-encoding asserts already noted
     in `TCL_TRIAGE.md`;
   - propagates into hash/set/zset TCL once Round 10b lands.
   Easily the single highest-leverage fix in the suite.

2. **Bump default `databases` from current value to 16**
   (Valkey default) **OR** implement the `databases <N>` config
   override the TCL framework already passes via `--databases`. Unblocks
   ~30 tests in `keyspace.tcl` plus an unknown but large number in
   `dump.tcl` / `other.tcl`.

3. **Fix the EXPIRE error-wording format**
   from `ERR invalid expire time in command` to
   `ERR invalid expire time in '<cmd>' command`. Touches
   `expire_command`, `pexpire_command`, `expireat_command`,
   `pexpireat_command`, `setex_command`, `psetex_command`,
   `set_command` (EX/PX subarg). Unblocks 3 `expire.tcl` tests and
   likely 3-5 more across `incr.tcl`, `type/string.tcl`, `type/hash.tcl`.

4. **Fix `EXPIRE ... GT/LT` semantics for keys without TTL**:
   per Valkey, `LT` returns 1 (treats absent TTL as +inf), `GT` returns 0
   (treats absent TTL as -inf). Unblocks 3 `expire.tcl` tests; ~5 LOC
   change in `expire_generic`.

5. **Implement `INCRBYFLOAT` / `HINCRBYFLOAT`** — straightforward port
   of the Valkey impl (long double + format with %.17Lg + strip trailing
   zeros). Unblocks 14 `incr.tcl` tests.

6. **Stop emitting `ERR empty command` for empty-line RESP frames**.
   When the parser sees a bare `\r\n` (or `*0\r\n` / negative bulk
   count) we should drop it silently. ~3 LOC in `redis-protocol::parse`.
   Unblocks `protocol.tcl` entirely (31 tests).

7. **Fix the `kill_server` race**. The TCL framework requires a stderr
   file at `tests/tmp/server.<pid>.<n>/stderr` to survive shutdown. We
   should either (a) ensure our server flushes/closes stderr cleanly
   before exit, or (b) the test harness can be invoked with
   `--stderr-file=/dev/null`. Without this, the 33% race in
   `expire.tcl` will keep flipping the baseline between 3 and 52
   passes.

## --skiptest exclusions

**None** added in this round. The deliverable is a clean count of what
the canonical suite says today, including aborts. The next agent (or a
follow-on Round 10d) can add per-test `--skiptest` filters once the
high-leverage fixes above land.
