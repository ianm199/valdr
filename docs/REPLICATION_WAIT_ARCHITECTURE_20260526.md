# Replication WAIT/ACK Architecture Note

Date: 2026-05-26

This note captures the current Rust design for the Valkey replication frontier
that made `unit/wait` claimable and explains why the remaining `unit/maxmemory`
timeout is not a WAIT/ACK lifecycle failure.

## Current Evidence

Focused TCL evidence from the resumed replication run:

| File | Latest result | Evidence |
|---|---:|---|
| `unit/wait` | 39 passed / 0 failed | `harness/oracle/results/tcl-survey/20260526T010311347419Z/unit__wait.json` |
| `unit/bitfield` | 22 passed / 0 failed | `harness/oracle/results/tcl-survey/20260526T010311347419Z/unit__bitfield.json` |
| `unit/auth` | 16 passed / 0 failed | `harness/oracle/results/tcl-survey/20260526T010311347419Z/unit__auth.json` |
| `unit/maxmemory` | timeout/no-summary | `harness/oracle/results/tcl-survey/20260525T233141143791Z/unit__maxmemory.json` |

Before this replication push, `unit/wait` was at 13 passed / 26 failed. The
current proof point is 39/0. `unit/auth` was 14/2 on the resumed branch; the two
remaining failures were the binary-password `primaryauth` cases and are now 16/0.

## Upstream Valkey Flow

The relevant upstream machinery is split across three places:

- `reference/valkey/src/replication.c`
  - `WAIT` and `WAITAOF` count replicas by acknowledged replication offsets.
  - `REPLCONF ACK <offset>` updates each replica client's acknowledged stream
    offset.
  - `REPLCONF FACK <offset>` updates the AOF fsync acknowledgement path used by
    `WAITAOF`.
  - `REPLCONF GETACK *` forces replicas to report their current offset so a
    client blocked in `WAIT` does not sit idle waiting for a periodic ACK.
- `reference/valkey/src/blocked.c`
  - `BLOCKED_WAIT` clients are kept on `clients_waiting_acks`.
  - `processClientsWaitingReplicas()` wakes clients whose required ACK count is
    now satisfied or whose timeout expired.
- `reference/valkey/src/networking.c`
  - Each client tracks the write offset it must wait for after a mutating
    command. `WAIT` is scoped to that client's last write, not to a global
    "latest" value.

The important invariant is that a write command establishes a target replication
offset, replicas independently report offsets, and blocked clients wake when
enough online replicas have acknowledged at least that target offset.

## Rust Flow

The Rust implementation mirrors the invariant without mirroring every C data
structure.

### Master Offset And Backlog

The shared replication state lives in `crates/redis-core/src/replication.rs`.
`ReplicationState::append_to_backlog()` appends serialized propagated commands
to the backlog and advances `master_repl_offset`.

The command execution path records the offset reached by a client's last
propagated write on `Client::last_write_repl_offset`. `WAIT` and `WAITAOF` read
that field to decide what offset the caller is waiting for.

### Replica ACK And FACK

`crates/redis-commands/src/replication.rs` handles `REPLCONF`.

- `ACK <offset>` updates the replica's in-memory ACK offset.
- `FACK <offset>` updates the replica's AOF fsync ACK offset.
- Both paths can wake blocked waiters when their conditions become true.

The wake logic is owned by the replication command module because it knows how
to count current replica ACKs, count AOF ACKs, serialize the final integer reply,
and remove the waiter from the core blocked-client registry.

### WAIT And WAITAOF Blocking

`WAIT` and `WAITAOF` create `BlockedAction::Wait` or
`BlockedAction::WaitAof` entries in `crates/redis-core/src/blocked_keys.rs`.
Those actions carry the target offset, requested replica count, local AOF fsync
requirement, timeout, and client id.

When a client blocks, the implementation also broadcasts `REPLCONF GETACK *`.
This is load-bearing: a replica may already be caught up, but Valkey still
prompts a fresh ACK so the blocked client can wake promptly instead of waiting
for the next periodic report.

### Runtime-Owner Replica Apply Path

The replica dialer runs outside the runtime-owner loop, but DB mutation must not.
`crates/redis-commands/src/replica_dialer.rs` turns commands received from the
primary into `ReplicaApplyRequest`s and sends them through the runtime apply
channel installed by `install_runtime_apply_sender()`.

`crates/redis-server/src/runtime_owner.rs` drains that channel and applies the
command with `client.replication_apply = true`. That flag prevents re-propagating
the command while still letting the normal command implementation mutate the
owner-owned DB. This is what makes replica-side command application line up with
the runtime-owner architecture instead of bypassing it.

### Dialer Epoch And REPLICAOF Retargeting

`REPLICAOF host port` calls `ReplicationState::become_replica_of()`, increments
`dialer_epoch`, marks the server as connecting, and starts a dialer for that
epoch. `REPLICAOF NO ONE` increments the same epoch and returns the server to
master role.

The dialer checks `dialer_epoch_is_current()` before applying data or sending
ACKs. A stale connection that survives a retarget cannot keep mutating the DB or
reporting offsets after a newer `REPLICAOF` decision superseded it.

## Primaryauth Fix

The remaining `unit/auth` failures were binary-password `primaryauth` cases.
Two separate compatibility details mattered:

1. The replica must send `AUTH <primaryauth>` before `PING` when the primary
   requires authentication. Sending `PING` first produced `-NOAUTH`, so the TCL
   test never saw Valkey's expected "Unable to AUTH to PRIMARY" path.
2. `INFO replication` must not report `master_link_status:up` just because a
   `replicaof` target exists. The link is only up when `repl_state` is
   `REPLICA_ONLINE`; otherwise it should report `down` and sync in progress.

The current implementation fixes both:

- `crates/redis-commands/src/replica_dialer.rs` authenticates before `PING`.
- `crates/redis-commands/src/info.rs` derives `master_link_status` from
  `ReplicationState::repl_state`.

That changes `unit/auth` from 14/2 to 16/0.

## Maxmemory Classification

`unit/maxmemory` is still not a replication WAIT/ACK failure.

The failing upstream test is `test_slave_buffers` in
`reference/valkey/tests/unit/maxmemory.tcl`. It:

1. Starts a master and replica.
2. Pauses the replica process.
3. Writes enough commands to build a large master-side output buffer for the
   replica client.
4. Expects `INFO memory` on the master to report that memory as
   `mem_clients_slaves`.
5. Expects replica output-buffer memory to be counted in
   `mem_not_counted_for_evict`, so maxmemory eviction does not delete user keys
   just because a replica is paused.

The latest failure shows exactly that gap:

```text
slave buffer are counted correctly
Expected 0 > 2*1024*1024

replica buffer don't induce eviction
Expected [dbsize] == 100
```

The Rust implementation currently has the tell:

```rust
fn client_memory_info_totals() -> (usize, usize) {
    ...
    (normal, 0)
}
```

So `mem_clients_slaves` is always zero. The next packet should be a
client/replica output-buffer memory accounting packet, not another WAIT/ACK
packet. It needs to track master-side replica output buffer bytes, expose them
through `INFO memory`, and exclude them from maxmemory eviction calculations.

## Claim Boundary

What is now claimable:

- Basic single-node primary/replica `WAIT` behavior.
- Replica-side application through the runtime-owner path.
- `REPLCONF ACK` and `REPLCONF FACK` enough for the current `unit/wait` proof.
- Binary `primaryauth` handshake behavior covered by `unit/auth`.
- `INFO replication` link-state truthfulness for connecting vs online replicas.

What is not yet claimable from this work alone:

- The full replication integration suite.
- Diskless sync, child-process replication, or long-duration replica churn.
- Replica client output-buffer memory accounting.
- Maxmemory eviction exclusion for paused replicas.
- Cluster, Sentinel, or multi-primary failover semantics.

## Smoke Check Caveat

The focused replication files passed after the primaryauth fix, but the first
broader smoke check that included replication-tagged subtests in otherwise core
files exposed a separate regression:

```text
harness/oracle/results/tcl-survey/20260526T004953034243Z/
  unit/type/string:   104 passed / 4 failed
  unit/type/zset:     318 passed / 2 failed
  unit/expire:         63 passed / 4 failed
  unit/hashexpire:    206 passed / 1 failed
  unit/introspection: abort/no-summary at MONITOR redaction
```

Those failures are mostly propagation-shape tests such as `GETDEL`/`GETEX`,
`ZMPOP`/`BZMPOP`, TTL absolute-time propagation, and hash-field TTL
propagation. The common root was not the command rewrite code; it was an
unconditional post-full-sync `REPLCONF GETACK *` inserted by the WAIT work. That
extra command polluted every replication stream and, because it advanced the
replication offset, made some assertions miss their final command.

The fix is to send the post-full-sync GETACK only when the full-sync job was
armed while a `WAIT`/`WAITAOF` waiter was present, or when such a waiter is
still present at completion. Recording the condition on `ReplBgsaveJob` matters:
the waiter can be transient by the time the child exits, but the retarget case
still needs the post-sync ACK prompt. This preserves the "client entered WAIT
while a replica was still full-syncing" behavior without changing normal
replication streams. The narrower WAIT/ACK/auth proof remains valid:

```text
harness/oracle/results/tcl-survey/20260526T010311347419Z/
  unit/wait:     39 passed / 0 failed
  unit/bitfield: 22 passed / 0 failed
  unit/auth:     16 passed / 0 failed
```

Final smoke after that gate:

```text
harness/oracle/results/tcl-survey/20260526T010357438677Z/
  unit/type/string: 108 passed / 0 failed
  unit/type/zset:   320 passed / 0 failed
  unit/expire:       67 passed / 0 failed
  unit/hashexpire:  207 passed / 0 failed

harness/oracle/results/tcl-survey/20260526T011653359945Z/
  unit/introspection: 113 passed / 0 failed
```

## Next Packet

Recommended next packet:

```text
client-replica-output-buffer-accounting

Goal:
  Make replica output buffers visible to INFO memory and excluded from
  maxmemory eviction pressure.

Evidence target:
  unit/maxmemory should stop failing:
    - "slave buffer are counted correctly"
    - "replica buffer don't induce eviction"

Likely files:
  crates/redis-core/src/client_info.rs
  crates/redis-core/src/replication.rs
  crates/redis-commands/src/info.rs
  maxmemory accounting/enforcement code in redis-core/redis-commands
```
