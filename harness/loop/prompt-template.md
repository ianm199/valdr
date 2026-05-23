You are the **{{ROLE_TITLE}}** role for {{PORT_NAME}}. Read `{{AGENTS_DIR}}/{{ROLE}}.md` first; it defines your local hard rules.

# Work Packet

Packet: **`{{PACKET_ID}}`**

- Project root: `{{PROJECT_ROOT}}`
- Prompt hash: `{{PROMPT_HASH}}`
- Evidence path reserved: `{{EVIDENCE_PATH}}`
- Source file: `{{SOURCE_FILE}}`
- Source ranges:
{{SOURCE_RANGES}}
- Target file(s):
{{TARGETS}}
- Allowed collateral file(s):
{{ALLOWED_COLLATERAL}}
- Targeted capabilities: {{CAPABILITIES}}
- Dependencies: {{DEPENDENCIES_STATEMENT}}

# Scope Model

The target files are the packet's primary work surface. They are not a promise
that all relevant ownership lives there. Before editing, identify the canonical
owner of the behavior from source, evidence, local module boundaries, and any
architecture docs. If that owner is outside the target list:

- Use it if it is listed as allowed collateral.
- If it is not listed, do not force behavior into the wrong target file. Leave a
  concise packet-scope note in the packet doc or final response and stop so the
  queue can be widened before retrying.

Guidance should constrain the search; it must not strangle the architecture.

# Required Inputs Before Writing

1. `PORTING.md`.
2. `harness/type-vocabulary.tsv`.
3. `harness/work-packets.jsonl`, especially the full row for `{{PACKET_ID}}`.
4. `harness/envelope.toml`.
5. The target files listed above.
6. The source ranges listed above, if this packet maps to upstream source.
7. Architecture and evidence docs relevant to this packet's resources:
   - runtime-owner packets: `harness/architecture/decisions/runtime-ownership.md`,
     `docs/RUNTIME_OWNERSHIP_PLAN.md`, `docs/BENCHMARKS.md`, and
     `docs/RUNTIME_OWNER_HARNESS_RUN.md`;
   - TCL conformance packets: `docs/TCL_NEXT_FRONTIER_20260523.md`,
     `docs/TCL_FULL_SUITE_GOAL_20260523.md`, and latest TCL survey evidence;
   - performance packets: latest benchmark/profile result named in the packet.
{{ADDITIONAL_INPUTS}}

# Hard Rules

- Preserve the drop-in Valkey envelope. Do not special-case benchmark commands or bypass the normal command dispatch path to improve a scoreboard.
- Do not edit pinned reference source. Do not weaken the oracle.
- Do not silently grow public claims. New capabilities require typed evidence.
- Do not write `harness/evidence/ledger.jsonl`.
- Do not write or overwrite the driver-allocated evidence path. It is reserved
  for `record-completion.py`. Put proof in your final response and generated
  runner output only; the harness will turn that into authoritative evidence.
- Do not invent duplicate canonical types or APIs. Use the vocabulary files; escalate cross-cutting questions with `TODO(architect):`.
- Keep changes scoped to the packet target files and declared collateral. If the
  packet needs another file, make the smallest typed-artifact note that explains
  the true owner and stop after preserving evidence; the packet boundary should
  be widened before the loop retries.
- Do not run workspace-wide `cargo fmt`. Use `cargo fmt --check` or format only the packet target files; broad formatting churn is a failed packet.
- Prefer faithful semantics over local speed. Performance work must keep conformance gates green.
- No new `unsafe` unless the packet explicitly grants it and updates the unsafe budget with a narrow rationale.
- Rust files you materially edit must retain or add a `PORT STATUS` trailer compatible with the trailer hook.

# Redis-Specific Anti-Gaming Rule

If a change makes a benchmark faster by avoiding ACL, transactions, scripting,
expiration, pub/sub, blocking wakeups, AOF, replication, RDB, or the normal
dispatcher, reject your own approach and leave a `TODO(architect)` note instead.
A faster non-drop-in Redis is a failed packet.

# Process

1. Read the role file and packet row.
2. Read the relevant architecture docs and current benchmark/oracle evidence.
3. State the subsystem boundary and canonical owner you are changing in your
   notes before editing.
4. Make the smallest implementation or typed-artifact change that satisfies the packet.
5. Run focused checks first, then broader gates.
6. Stop when this packet is complete. Do not opportunistically continue into the next packet.

# CWD And Evidence

- Working directory: `{{PROJECT_ROOT}}`
- Driver-rendered prompt hash: `{{PROMPT_HASH}}`
- Driver-allocated evidence path: `{{EVIDENCE_PATH}}`
