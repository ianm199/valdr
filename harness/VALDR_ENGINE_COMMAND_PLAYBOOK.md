# valdr-engine command-add playbook

Pre-computed contract for adding a command (or a value type) to the wasm-safe
edge engine. One source file: `crates/valdr-engine/src/lib.rs`. One oracle:
`harness/oracle/valdr-engine-differential.py`. Read this before touching the
engine; do not re-derive these contracts per command.

## The gate (run after EVERY change — the only truth-teller)

```bash
python3 harness/oracle/valdr-engine-differential.py \
  --server-bin "$(pwd)/reference/valkey/src/valkey-server" --strict
```

- Diffs every fixture's RESP frame against the **real `valkey-server`** reference.
- Must print `0 diverge` and exit 0. Baseline: `382 pass / 0 diverge / 19 known-unsupported`.
- Every command you add must move **pass up** and/or **known-unsupported down**,
  and **never** increase diverge.
- Also run `cargo test -p valdr-engine`. Its per-key dirty-flush round-trip test
  (`lib.rs` ~line 2577) rebuilds the db from `take_dirty`/`export_key` and
  catches snapshot-codec bugs the differential oracle cannot see.

Build success is **not** signal. A clean build with a divergence is a regression.

## Where behavior comes from (never guess a reply shape or error string)

- Pinned C: `reference/valkey/src/{t_string,t_hash,t_zset,t_list,t_set,db,expire,server}.c`.
- The already-correct Rust port: `crates/redis-commands/src/{string,hash,zset,list,set}.rs`
  and `crates/redis-core` — mirror its reply shapes, error messages, and check order.
- Copy exact integer/bulk/array/error replies and exact error text + argument-check
  ordering from the reference. Edge engine behavior must be byte-identical.

## Fixture model (each file is a clean room)

- The oracle issues `FLUSHALL` + `SCRIPT FLUSH` to valkey before each `*.jsonl`,
  and the engine is fresh per file. **Fixtures within a file share state, in order.**
- Use a **key namespace unique to the file** so nothing leaks across files.
- Line format: `{"id": "...", "cmd": ["NAME", "arg", ...]}`.
  Optional: `"mode": "ttl_band", "band": N` (TTL fuzz), `"now_millis": N`, `"sleep_ms": N`.
- **`ttl_band` mode is SCALAR-ONLY** — it only band-compares a single `:integer`
  reply (oracle `compare()`). Array-returning TTL reads (HTTL/HPTTL/HEXPIRETIME
  FIELDS...) CANNOT be band-checked; they pass only when ms/seconds align by luck
  and silently flake. To assert a relative TTL on an array reply, instead set it
  with an ABSOLUTE command (HEXPIREAT/HPEXPIREAT) and read it back with the
  absolute reader (HEXPIRETIME/HPEXPIRETIME) under `exact` mode — the returned
  timestamp is deterministic (it's the value you set), no clock drift.
- A `"known_unsupported": true` line is **record-only** (never a verdict). To
  "close" a gap: implement the command, then **remove the flag** (or rehome the
  fixture into its type file with a proper setup sequence) so it becomes a real
  PASS/DIVERGE assertion. `known-unsupported.jsonl` should only ever hold things
  we deliberately do not support.

## Adding a contained command (no new value type)

1. **Dispatch** — in `execute_inner` (`lib.rs` ~line 347), add a branch before the
   final `else { unknown_command_error(...) }`:
   ```rust
   } else if ascii_eq(command, b"NAME") {
       self.name_command(argv)
   ```
   Multi-word commands (e.g. `SCRIPT EXISTS`) match `argv[0]` then dispatch on the
   subcommand inside the handler — see `script_command`.
2. **Handler** `fn name_command(&mut self, argv: &[Vec<u8>]) -> RespFrame`:
   - Arity: `if argv.len() != N { return wrong_arity(b"name"); }` (or `< N`).
   - Lazy expiry before key access: `self.purge_if_expired(&argv[1]);`.
   - Read path: `self.get_value(&key).map(|e| &e.value)`. Write path: `self.db.get_mut(&key)`.
   - Type check: `match` the `StoredValue` variant; mismatched variants → `wrong_type()`.
   - **Mutation accounting**: call `self.note_write(&key)` EXACTLY when
     snapshot-visible state changed (write / delete / expiry). Reads must NOT call
     it. When a command empties an aggregate, `self.db.remove(&key)` then `note_write`.
   - Reply builders: `bulk(&[u8])`, `RespFrame::integer(i64)`, `RespFrame::null_bulk()`,
     `RespFrame::array(vec)`, `err(b"ERR ...")`. Copy the exact `+OK` reply from
     `set_command`. Grep neighbouring handlers for the precise builder.
3. **Fixtures** — add to the type's `.jsonl` under `harness/oracle/valdr-fixtures/`.

## Adding a NEW value type (List, Set) — CROSS-CUTTING, every site or it breaks

The single-file enum is woven through five places. Touch all of them:

1. `enum StoredValue` (`lib.rs` ~line 72): add `List(VecDeque<Vec<u8>>)` /
   `Set(HashSet<Vec<u8>>)`.
2. `encode_entry` (~line 1583): add a `StoredValue::List(..) =>` /
   `StoredValue::Set(..) =>` arm. Write `"type":"list"`/`"set"` plus a hex-encoded
   array of items — **preserve order for lists**, **sort for sets** (determinism).
3. `decode_entry` (~line 1646): add the matching `"list" => ...` / `"set" => ...` arm.
   `encode`/`decode` must round-trip exactly (the dirty-flush unit test enforces it).
4. Explicit wrong_type guards: handlers with
   `Some(StoredValue::String(_) | StoredValue::ZSet(_)) => wrong_type()`
   (e.g. lines 664, 684) must keep rejecting the new type. Match existing style —
   prefer a `Some(_) => wrong_type()` catch-all where the real arm is already present.
5. `TYPE` (once implemented) must name the new type (`+list`/`+set`).

Then add the type's commands as contained commands and a fresh `<type>.jsonl`.

## Helpers already in the file (grep before reinventing)

`ascii_eq`, `wrong_arity`, `wrong_type`, `err`, `bulk`, `get_value`,
`purge_if_expired`, `note_write`, `unknown_command_error`, `hex_encode`,
`hex_decode`, `score_snapshot_string`, `parse_int`-style helpers.

## Discipline

- One file, cross-cutting: **sequential**, oracle-gate every landing, commit green.
- Reads never call `note_write`. Mutations always do, with the exact key(s) touched.
- When in doubt about a reply, run the command against `reference/valkey/src/valkey-server`
  by hand and copy what it does.
