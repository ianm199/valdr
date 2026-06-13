# wire-diff oracle

Compares RESP byte streams from C Valkey (`reference/valkey/src/valkey-server`)
and the Rust `redis-server` binary on the same input scripts. The
load-bearing infrastructure piece for **autonomy** — the test-fixer agent
calls `./wire-diff` iteratively as it fixes code, getting real behavioral
feedback instead of compile-only feedback.

## Contract

```sh
./wire-diff [--suite NAME] [--c-port N] [--rust-port N] [--diff-only]
```

- Default: spawn fresh C and Rust servers, run all corpus scripts under
  the suite, compare frame-by-frame after normalization.
- `--c-port N` / `--rust-port N`: use already-running servers on those
  ports instead of spawning fresh ones.
- `--suite NAME`: run only the named subset (`smoke`, `protocol`,
  `strings`, etc.). Default: all.
- `--diff-only`: print diffs without failing. Useful for the test-fixer
  iteration when you want to see what's changing without blocking on
  every minor difference.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | All corpus cases match between C and Rust. |
| 1 | At least one case diverges. |
| 2 | Infrastructure error (server didn't start, port in use, missing binary). |
| 3 | `--diff-only` mode — diffs were shown, no judgment. |

## Comparison classes

Per `REDIS_PORT_HARNESS_SPEC.md §Oracle 2`:

- `byte_exact` — replies must match byte-for-byte (PING, ECHO, SET, GET,
  DEL, EXISTS, INCR with deterministic args).
- `frame_exact` — replies must parse to the same RESP frame after RESP
  decode (handles trivial byte-level whitespace differences).
- `normalized` — replies are filtered through a per-command normalizer
  before compare (INFO, TIME, RANDOMKEY, CLIENT, SCAN cursor).
- `state_digest` — at end of script, `DEBUG DIGEST` is compared.
  Used for command sequences where individual replies may be racy but
  end-state should match.

Each corpus script declares its class in the header.

## Corpus format

A corpus script is a `.txt` file with one command per line. Lines
starting with `#` are comments. Lines starting with `[`...`]` are
control directives:

```
# 01-ping.txt
[class: byte_exact]
[description: PING with and without message]

PING
PING hello
ECHO "world"
ECHO "with spaces and special chars: !@#$"
```

The driver parses each command via inline RESP encoding, sends to both
servers, captures responses, and runs the appropriate compare.

## Where this fits in the agent loop

The `test-fixer` agent has `Bash(./harness/oracle/wire-diff*)` in its
`allowedTools`. After every iteration of a fix attempt:

```
# Agent runs:
./harness/oracle/wire-diff --suite strings

# Output:
=== suite: strings ===
01-set-get             PASS
02-set-with-ex         FAIL
  expected: +OK\r\n
  actual:   -ERR unknown option EX\r\n
03-incr                PASS
```

The agent reads the failure, edits the impl, re-runs. Iterates until
PASS. This is the missing piece from `RETROSPECTIVE_AND_PRODUCTIZATION.md §9.2`
that's non-optional at Redis scale.

## Current state

The Rust `redis-server` is live. The smoke wrapper builds or reuses
`target/debug/redis-server`, starts it next to the pinned upstream Valkey
binary, sends every corpus script to both servers, and compares the RESP
frames.

Current release gate:

```sh
bash harness/oracle/smoke.sh --skip-build
```

The corpus is intentionally small and byte-exact. It is the fast compatibility
floor for local iteration; the TCL suite and RDB oracle provide broader
coverage.

## Replication TCL dashboard

The current dual-server replication frontier is tracked by:

```sh
harness/oracle/run-integration-repl-current.sh --skip-build
```

That wrapper runs the in-scope `integration/replication*.tcl` files
sequentially with `--clients 1` through `tcl-survey.py`. Do not run two
dual-server replication TCL surveys concurrently; port and test-dir contention
can create false failures.
