# TCL ACL visibility wave - 2026-05-25

## Goal

Move the ACL/auth lane from hidden or aborting TCL files into counted coverage
for the Single-Node Core Visibility Wave.

## Result

`unit/acl-v2.tcl` now reaches a normal test summary instead of aborting on the
first selector command:

```text
before: no-summary abort
        ERR Error in ACL SETUSER modifier '(+@write':
        Unrecognized parameter '(+@write'

after:  47 passed / 25 failed / 72 counted
```

This adds 72 counted TCL blocks to the visibility wave while preserving the
existing green ACL file:

```text
unit/acl     114/0
unit/acl-v2   47/25
```

## Evidence

```bash
cargo build --bin redis-server

python3 harness/oracle/tcl-survey.py \
  --runner-id tcl-aclv2-final-v1 \
  --skip-build \
  --timeout-s 240 \
  --baseport 49111 \
  --portcount 5000 \
  --no-default-deny-tags \
  --deny-tag needs:repl \
  --deny-tag needs:debug \
  --deny-tag cluster \
  --deny-tag needs:cluster \
  --files unit/acl-v2

python3 harness/oracle/tcl-survey.py \
  --runner-id tcl-acl-final-v2 \
  --skip-build \
  --timeout-s 180 \
  --baseport 51111 \
  --portcount 5000 \
  --no-default-deny-tags \
  --deny-tag needs:repl \
  --deny-tag needs:debug \
  --deny-tag cluster \
  --deny-tag needs:cluster \
  --files unit/acl
```

Artifacts:

- `harness/oracle/results/tcl-survey/20260525T075323Z/unit__acl-v2.json`
- `harness/oracle/results/tcl-survey/20260525T075334Z/unit__acl.json`

## What landed

- ACL selectors are parsed from both single bulk strings and multi-token Tcl
  forms such as `(+@write ~write::*)`.
- `clearselectors` is wired.
- `ACL GETUSER` renders selector maps.
- ACL key rules now track read/write permission bits for `%R~`, `%W~`, and
  `%RW~` patterns.
- Command/key/db checks evaluate root permissions and each selector as separate,
  non-additive permission routes.
- `ACL DRYRUN` uses the real runtime command spec instead of the first generated
  duplicate command name, so command families like `GET` and `SET` pick the data
  command instead of CONFIG/COMMANDLOG variants.

## Remaining red in acl-v2

The 25 remaining failures are semantic follow-ups, not visibility blockers:

- Detailed key-spec access classification for odd commands such as MIGRATE,
  SORT, SINTERCARD, MEMORY USAGE, and related DRYRUN cases.
- Database ACL semantics for selectors, transactions, WATCH, scripts, and ACL
  invalid-database stats.
- Exact `ACL LIST` selector string ordering.
- SORT option-specific ACL errors.

These are good counted-red follow-ups after larger hidden/no-summary lanes have
been illuminated.
