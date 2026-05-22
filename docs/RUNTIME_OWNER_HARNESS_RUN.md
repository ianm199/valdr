# Runtime Owner Harness Run

Status: configured 2026-05-21.

This is the several-hour harness run for Redis performance architecture. It is
designed to keep valkey-rs faithful to upstream Valkey while using the harness
to drive evidence, architecture, canaries, and safe scaffolding.

## Goal

Use the harness to answer: "what is the next architecture move after the
small hot-path wins, and can we prepare it without losing drop-in behavior?"

This run should not complete the full runtime-owner rewrite. It should produce
auditable evidence and a safe starting point:

- fresh wire-diff pass/fail evidence;
- fresh profile matrix;
- fresh sampled hotspot evidence;
- a refined runtime ownership decision doc;
- compatibility canaries for owner-loop risks;
- an inert `RuntimeOwner` scaffold that does not alter the default server path;
- post-scaffold oracle and benchmark rows.

## Faithfulness Constraints

The run must not create a faster but non-Valkey server. The following are
explicitly forbidden:

- benchmark-only fast paths for `PING`, `GET`, `SET`, or `INCR`;
- bypassing ACL, transactions, scripting, expiration, pub/sub, blocking
  wakeups, AOF, replication, or RDB behavior;
- sharding as a hidden implementation detail;
- publishing numbers from a non-default product path;
- broad normalizers that hide wire incompatibility.

## Packet Chain

The seeded `harness/work-packets.jsonl` chain is:

| Packet | Role | Purpose |
|---|---|---|
| `runtime-owner-baseline-oracle` | runner | Prove current wire compatibility before architecture work. |
| `runtime-owner-baseline-profile-matrix` | runner | Record pipeline-depth performance baseline. |
| `runtime-owner-baseline-hotspots` | runner | Record short stack-sampling hotspot evidence. |
| `runtime-owner-0-faithful-map` | architect | Refine the runtime owner decision and packet graph. |
| `runtime-owner-1-canary-corpus` | translator | Add oracle canaries for semantics likely to break. |
| `runtime-owner-post-canary-oracle` | runner | Prove the new canaries pass against C and Rust. |
| `runtime-owner-2-scaffold-types` | translator | Add inert owner-loop vocabulary without changing default behavior. |
| `runtime-owner-post-scaffold-oracle` | runner | Prove the scaffold did not alter behavior. |
| `runtime-owner-post-scaffold-profile-matrix` | runner | Refresh performance matrix. |
| `runtime-owner-post-scaffold-hotspots` | runner | Run the larger sampled profile for the next decision. |

## Launch

From the repo root:

```sh
bash harness/run-runtime-owner-loop.sh
```

The script uses the productized chassis:

```sh
python3 ../port-harness/loop/run-loop.py \
  --project "$PWD" \
  --selector auto \
  --auto-dispatch \
  --dispatch-runtime claude \
  --dispatch-budget-usd "${RUNTIME_OWNER_BUDGET_USD:-35}" \
  --dispatch-timeout-s "${RUNTIME_OWNER_TIMEOUT_S:-3600}" \
  --dispatch-model "${RUNTIME_OWNER_MODEL:-opus}" \
  --max-iterations "${RUNTIME_OWNER_MAX_ITERATIONS:-10}" \
  --max-failures "${RUNTIME_OWNER_MAX_FAILURES:-2}"
```

Set environment variables to tune the run:

```sh
RUNTIME_OWNER_BUDGET_USD=60 \
RUNTIME_OWNER_TIMEOUT_S=5400 \
RUNTIME_OWNER_MAX_ITERATIONS=10 \
bash harness/run-runtime-owner-loop.sh
```

## What To Check When Returning

Run:

```sh
python3 ../port-harness/loop/check-completion.py --project "$PWD"
git log --oneline -10
git status --short
tail -40 harness/evidence/ledger.jsonl
```

Interpretation:

- If the run stops after the architect packet, read
  `harness/architecture/decisions/runtime-ownership.md` for human questions.
- If canary oracle fails, treat that as a compatibility finding, not a bad
  benchmark result.
- If the scaffold lands and the oracle stays green, the next decision is
  whether to spend on the real owner loop.

## Expected Result

Success is not "speed parity tonight." Success is a clean, evidence-backed
handoff:

- conformance still green;
- performance evidence refreshed;
- no benchmark gaming;
- runtime ownership decomposed into faithful packets;
- the next high-risk migration is explicit enough for human review.
