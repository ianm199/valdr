# CLIENT PAUSE — subsystem status + gate design (2026-05-25)

Branch `claude/pause-semantics-20260525` off main `61a29d9`. Goal-2 slice
(`unit/pause.tcl`, 23 source tests). Note: nearly all of Goal 2
(scripting/functions/introspection/ACL/CLIENT/COMMAND/INFO) is Codex Agent-1's
active wave; pause was the one system not in their done/pipeline.

## LANDED (Phase 1, committed `d7abd65`) — clean, mergeable

CLIENT PAUSE/UNPAUSE state machine + INFO reporting. `unit/pause.tcl` 5/15 → **6/14**.
- `server.rs`: `pause_events: Mutex<[PauseEvent; 4]>` server-global.
- `networking.rs`: real `client_pause_command` (`<timeout> [WRITE|ALL]`, default
  ALL; `pause_clients_by_client` keeps most-restrictive action + longest end),
  `client_unpause_command`, and `pause_info` → `(reason, actions, timeout)`.
- `connection.rs`: wired the live CLIENT dispatch (was an OK stub) to the above.
- `info.rs`: `paused_reason` / `paused_actions` / `paused_timeout_milliseconds`.

## BUILT + REVERTED — the command-loop gate (the keystone for the other 14)

A full postpone+re-dispatch gate was implemented and **verified working in 5
isolated manual tests** (write held under PAUSE WRITE; reads not held;
`blocked_clients:1` while held; resume on UNPAUSE; resume on pause timeout). It
was **reverted** because it regressed `unit/pause.tcl` from a clean 6/14 to a
**hang** (`exit=124`) via a test cascade (below). The design, which works
mechanically, for whoever resumes it:

- `dispatch.rs::command_pause_class(name) -> (is_write, is_may_replicate)` — OR
  the WRITE / MAY_REPLICATE flags across **all** same-named specs in `COMMANDS`
  (the table lists subcommands under colliding names, e.g. "SET" is both string
  SET and CONFIG SET; `registered_command_spec` returns the wrong first match —
  this was a real bug to avoid). `is_may_replicate = WRITE | MAY_REPLICATE`.
- `client.rs`: `blocked_on_pause: bool`.
- `networking.rs`: a `PAUSED_CLIENTS` AtomicU64 (+ paused_clients_count/incr/decr),
  `pause_exempt_command` (CLIENT/AUTH/HELLO/RESET/QUIT/SUBSCRIBE-family), and
  `command_postponed_by_pause(paused_actions, is_may_replicate, cmd)` mirroring
  server.c:4656 (ALL → postpone all non-exempt; WRITE → postpone may-replicate).
- `runtime_owner.rs::dispatch_slot_commands`: before dispatch, if
  `!deny_blocking` and the command is gated → rewind `consumed_total -= consumed`
  (leave bytes in `query_buf`), set `blocked_on_pause`, incr counter, `break`.
- `runtime_owner.rs::resume_paused_slots` (called each loop iteration before
  `dispatch_scheduled_commands`): for each `blocked_on_pause` slot whose held
  command is no longer gated, clear the flag, decr counter, re-run
  `dispatch_slot_commands` (re-parses the buffered command), and flush via
  `ensure_writable_interest` (the same seam scheduled continuations use).
- `cleanup_slot`: decr counter if a held client disconnects (leak guard).
- `info.rs`: `blocked_clients += paused_clients_count()`.

## THE BLOCKER — `unit/pause.tcl` cascade → hang

`wait_for_blocked_clients_count N` asserts `blocked_clients == N` **exactly**, so
any lingering held client breaks every later wait. The first failure is test
"old pause-all takes precedence over new pause-write": it uses a **deferring**
client (`valkey $host $port 1` = send-only, replies read later) and asserts
`elapsed > 200` immediately after `$rd get FOO` — which is send-only and returns
in ~54ms. I could not reconcile how this passes upstream (the send doesn't
block), so the assert fails on our side, the test ends without cleaning up its
held GET, and that held client cascades into the next tests' exact-count waits
→ accumulation → file hang.

**Resume points for next time:**
1. Understand the deferring-client `elapsed > 200` semantics (does upstream rely
   on server-side read-postponement creating TCP backpressure on the next send?
   does the harness `$rd` method actually block?). That determines whether the
   gate's resume timing needs to make `$rd get` observably take ~200ms.
2. Ensure held clients can't linger across test boundaries (the cascade) — the
   `cleanup_slot` decr is there, but a test that fails its assert may leave a
   held deferring client connected; resume-on-timeout must fire promptly.
3. The gate itself is correct (manual proof); the work is the test-harness
   interaction, not the mechanism.

The gate touches `runtime_owner.rs` (the command loop) which is Codex Agent-1's
committed file — coordinate at merge.
