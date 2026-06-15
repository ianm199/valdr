# Replication Next Packet Detail Archive

This file preserves the long-form "Next Useful Packets" detail that previously
lived in [`../REPLICATION_INTEGRATION_DASHBOARD.md`](../REPLICATION_INTEGRATION_DASHBOARD.md).
The dashboard now keeps a short current-priority table; long-form roadmap
context belongs in [`../HA_CLUSTER_REPLICATION_ROADMAP.md`](../HA_CLUSTER_REPLICATION_ROADMAP.md)
or archived history files like this one.

## 2026-06-15 Dashboard Detail

The R1 propagation packets cleared the known command-rewrite regressions, but
the rebuilt `replication-3` / `replication-4` gate is not green. The largest
visible integration frontiers are now:

- Expiry-on-replica semantics: `replication-3` still fails master/replica
  consistency with expire, writable replica expired-key behavior, and PFCOUNT
  expired-key/cache cases.
- `R1-BLOCKING-WAKE-REWRITE`: empty-blocking `BRPOPLPUSH` / `BLMOVE` live
  stats now pass, and the list/zset single-pop blocking workload is covered by
  `block-repl`. Keep extending this family for `BLMPOP` / `BZMPOP` and
  multi-key fairness before touching more blocked-client code.
- `R1-REPLICA-APPLY-THROUGHPUT`: the first batching slice is complete and
  restores `replication-2` to green under real digest. Keep this lane open for
  bounded queue depth, batch-size metrics, and owner-loop fairness under slow
  commands.
- `R2-RDB-BULK-FAITHFUL`: the old `REPLICAOF` pre-PSYNC `KEYS`/`DUMP` seed
  shortcut is removed, so remaining full-sync work must pass through the
  streamed RDB handoff path.
- `R2-BGSAVE-WINDOW`: replication BGSAVE now reports through `INFO persistence`
  and honors the bounded per-key debug save-delay window; keep extending this
  into the diskless/full-sync windows behind `integration/replication`. Failed
  full-sync BGSAVE jobs now clean up waiters, temp files, and replication-child
  state instead of poisoning later sync attempts. Async-loading state is now
  explicit in `INFO persistence` and dispatch. Successful full-sync RDB
  replacement now carries function payloads too, and replica-link replies are
  now detected, logged, and disconnected instead of being flushed to the link.
  Chained replica apply now relays empty `FLUSHDB` / `FLUSHALL`, including
  Lua-originated flushes, and initializes downstream stream DB state from the
  upstream selected DB. Chained full sync now also treats the upstream stream
  DB as already represented by the downstream RDB, avoiding redundant `SELECT`
  frames before the first live write. Replica-side handshake/full-sync reads
  now honor `repl-timeout` while waiting on a stalled primary. Async failure
  rollback, deeper multi-replica offset convergence, and diskless pipe cleanup
  remain open.
- `R2-BGSAVE-CATCHUP`: active replication BGSAVE jobs now retain appended
  replication bytes outside the circular backlog and use that buffer for
  post-RDB catch-up. Completed full-sync catch-up bytes are now also retained
  while dependent replicas still pin them. The kit surface now also proves an
  online replica reconnect can consume active full-sync history while another
  waiter keeps that history pinned, and that a selected-DB prefix appended
  after job installation survives circular backlog wrap.
- `R3-RECONNECT-MATRIX`: extend the new master-side PSYNC decision matrix into
  live replica-dialer reconnect coverage before grinding `replication-psync`.
  Current full-file PSYNC reruns time out again with master/replica
  inconsistency lines, including a conservative-selector comparison. The
  detached full-sync catch-up tail slice removes the earliest broad
  no-reconnect mismatch. The narrowed `0` vs `-0` family now has Rust kit
  coverage, including an RDB raw-string load bug where `-0` was promoted to
  integer `0`. The later DB 0 set residue is also covered by a kit that drives
  RDB delivery through `complete_repl_bgsave_transfer` and proves the first
  post-fullsync DB 9 live write forces `SELECT 9`. Zset store propagation is
  now deterministic, and the first no-reconnect Tcl body has a passing
  extracted reducer. Keep using these kits as the debugger and reserve the full
  Tcl matrix for a scoreboard rerun.
- `R2-BUFFER-LIMITS`: accounting aliases, fan-out accounting, retained
  full-sync history, owner-loop replica drain, and full-sync `send_bulk`
  visibility are covered; implement broader shared-buffer memory accounting,
  backlog outgrowth under slow online replicas, and replica output-buffer
  disconnection semantics behind `replication-buffer`.
- `R4-WAIT/WAITAOF`: role-change unblock now covers WAIT, WAITAOF, and
  write-sensitive list/zset blocking waiters for `REPLICAOF` topology changes;
  replica FACK/disconnect semantics remain open.
- `R4-AOF-FULLSYNC`: `replication-aof-sync` is now green after full-sync RDB
  loads refresh appendonly manifests correctly.
- `R5-MANUAL-FAILOVER`: server `FAILOVER` now has parser coverage and visible
  state; the next useful work is real write pause, offset wait,
  promotion/demotion, and blocked-client handling needed by
  `replica-redirect`. The basic replica REDIRECT contract for redirect-capable
  clients is now covered, and `FAILOVER` exposes `waiting-for-sync` /
  `failover-in-progress`. Pause accounting, timeout handling, blocked-client
  REDIRECT unblocking, and promotion/demotion remain open.
