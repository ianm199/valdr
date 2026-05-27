# TCL Test Suite Runbook

This is the canonical way to *run* the upstream Valkey TCL suite against the
Rust server. For what the resulting numbers *mean* — the current counts, the
source-block denominators, and how the full upstream suite is bucketed — see
the source of truth: [`TEST_AND_FEATURE_COVERAGE.md`](TEST_AND_FEATURE_COVERAGE.md).

## Official Single-Node Run

```bash
bash harness/oracle/run-single-node-tcl-suite.sh
```

The wrapper runs `harness/oracle/tcl-survey.py` with the settings that define
the current official local number:

- profile: `single-node-external`
- files: every `unit/*.tcl` and `unit/type/*.tcl`, excluding
  `unit/tls`, `unit/mptcp`, `unit/io-threads`, and `unit/oom-score-adj`
- tags denied by the profile: `needs:repl`, `repl`, `needs:debug`,
  `cluster`, `needs:cluster`
- `external:skip` is allowed, so local-server tests such as `unit/maxmemory`
  are counted
- `--isolated-tests-copy`, so runs do not share `reference/valkey/tests/tmp`
- `--timeout-s 180`
- `--baseport 30000 --portcount 8000`

The wrapper prints a concise summary and stores raw artifacts under:

```text
harness/oracle/results/tcl-survey/<run-id>/
```

It also writes the full RunnerResult JSON to:

```text
harness/oracle/results/tcl-survey/<run-id>/result.json
```

## Fast Feedback

Build once, then run focused files through the same profile:

```bash
cargo build --bin redis-server
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build --files unit/maxmemory
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build --files unit/multi
```

Multiple files are comma-separated:

```bash
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build \
  --files unit/maxmemory,unit/multi,unit/type/stream
```

Use focused runs for iteration. Use the full wrapper command before claiming a
new official single-node number.

For a single upstream test body, run `test_helper.tcl` directly from the Valkey
checkout. This is the fastest edit/debug loop and still exercises the upstream
harness:

```bash
cd reference/valkey

VALKEY_BIN_DIR="$PWD/../../target/debug" \
  tclsh tests/test_helper.tcl \
  --single unit/lazyfree \
  --only "UNLINK can reclaim memory in background" \
  --clients 1 --skip-leaks \
  --baseport 33000 --portcount 4000 \
  --tags "-needs:repl -repl -needs:debug -cluster -needs:cluster" \
  --quiet
```

Some files contain both single-node tests and nested external replication
tests. For example, the core consumer-group portion of
`unit/type/stream-cgroups` is checked with the conservative profile:

```bash
bash harness/oracle/run-single-node-tcl-suite.sh --skip-build \
  --profile default \
  --files unit/type/stream-cgroups
```

## Port Rule

Do not hand-pick high baseports for the upstream TCL helper.

The helper checks both each candidate port and `port + 10000`. A baseport such
as `56111` with a normal portcount is invalid because the companion port exceeds
`65535`, which makes the whole run fail before tests start. The wrapper
validates this and defaults to the known-good range:

```text
--baseport 30000 --portcount 8000
```

## Conservative Contained Survey

`harness/oracle/safe-survey.py` is a different profile. It runs inside an 8 GB
disk image with file-size and free-space guards, and it denies `external:skip`.
That makes it useful after disk-risky work, but it does not count local-server
external tests such as `unit/maxmemory`.

```bash
python3 harness/oracle/safe-survey.py \
  --timeout 180 \
  --image-size 8g \
  --file-cap-mb 1024 \
  --floor-drop-gb 15 \
  --baseport-start 31000
```

Report it as the conservative contained/default number, not as the
`single-node-external` number.

## Direct `tclsh` Runs

Direct `tclsh tests/test_helper.tcl ...` runs are for debugging one test body
inside the upstream harness. They are not the number of record unless the same
file is rerun through `run-single-node-tcl-suite.sh`.
