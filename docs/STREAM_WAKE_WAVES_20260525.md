# Stream blocking-wake lane — map + waves (2026-05-25)

Branch `claude/stream-blocking-wake-20260525` off main `ca56a89`. Methodology:
port-harness v2 (Source → Map → Wave → Prove). Builds on the deferred-wake
command-execution-unit merged in `ca56a89`.

## Map (oracle ground truth, default + single-node-repl profiles)

- `unit/type/stream.tcl` **71/2** (82 tests; 9 are `needs:debug`, uncounted)
- `unit/type/stream-cgroups.tcl` **56/3** (65 tests; ~6 `needs:debug`, uncounted)
- Neither file has `needs:repl` / `assert_replication_stream` → stream
  propagation is NOT tested here (no propagation wave needed for these files).

### The 5 counted fails

| # | Test | File | Root cause |
|---|---|---|---|
| 1 | XREAD + multiple XADD inside transaction | stream | wake must defer to end-of-EXEC, then the woken XREAD must re-read ALL entries added (we currently broadcast a single entry per wake) |
| 2 | XTRIM with MAXLEN option basic test | stream | `MAXLEN ~ N` approx-trim must keep to listpack-macro-node boundaries (`~444`→500, `~400`→400), not trim exactly |
| 3 | Blocking XREADGROUP: key type changed with transaction | cgroups | `SADD` inside MULTI/EXEC must wake the stream waiter, which re-evaluates type → WRONGTYPE |
| 4 | Blocking XREADGROUP: swapped DB, key not a stream | cgroups | `SWAPDB` must wake stream waiters in both DBs → re-eval → WRONGTYPE |
| 5 | XGROUP DESTROY should unblock XREADGROUP with -NOGROUP | cgroups | wake fires (infra exists) but also needs errorstats: `failed_calls`/`total_error_replies` for the failed blocked command |

### Uncounted `needs:debug` (~15) — separate wave
DEBUG RELOAD / DEBUG JMAP / DEBUG OBJECT on streams. Countable once those
subcommands round-trip streams (we already have stream RDB load/save).

## Waves (each proven by oracle: stream/stream-cgroups default + no-regression list/zset)

- **Wave A — XGROUP DESTROY errorstats (#5).** Smallest; wake already fires.
  Wire failed-blocked-command stats (errorstats now on main post-scripting-merge).
- **Wave B — type-reevaluating wake (#3, #4).** Route SADD/SWAPDB (any key-ready
  or key-replace event) through a wake that re-checks blocked stream/group
  waiters and replies WRONGTYPE/NOGROUP. Reuse the deferred `pending_wakes`
  drain from the CEU.
- **Wave C — re-read-all XREAD wake (#1).** Deferred stream wake that re-runs
  the read (returns all entries since the blocked id), not single-entry
  broadcast. Pairs with the CEU's end-of-EXEC drain.
- **Wave D — XTRIM `~` approx-trim (#2).** Independent of blocking; macro-node
  granularity in `streamTrim`.
- **Wave E — needs:debug stream tests.** DEBUG RELOAD/JMAP/OBJECT for streams.

Ordering: A (cheapest, counted) → B → C → D → E.
