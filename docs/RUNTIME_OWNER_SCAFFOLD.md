# Runtime Owner Scaffold

Status: added by `runtime-owner-2-scaffold-types`.

## Boundary

This scaffold only defines the typed runtime-owner vocabulary owned by
`crates/redis-server/src/runtime_owner.rs` in
`harness/architecture/object-vocabulary.tsv`.

The default product path is unchanged:

- `main.rs` still accepts plain TCP connections with `TcpListener::incoming`.
- Each client still runs on the existing per-connection thread path.
- Commands still parse through `redis-protocol` and dispatch through
  `redis_commands::dispatch`.
- The selected database still comes from the existing `Arc<Mutex<RedisDb>>`
  path.
- TLS, pub/sub, blocking wakeups, AOF, replication, RDB, ACL, and transactions
  are not rerouted through the scaffold.

## Types

The scaffold defines exactly the runtime-owner rows from the object vocabulary:

- `RuntimeOwner`
- `ClientSlot`
- `ClientWriteBuffer`
- `RuntimeEvent`
- `RuntimeOwnerConfig`
- `OwnerCommandResult`
- `PollDriverHandle`
- `SlotId`

`SlotId` is a newtype around `u32`, not a type alias. `PollDriverHandle` is an
abstract placeholder and imports no poller crate. The poller choice remains a
`TODO(human)` decision in
`harness/architecture/decisions/runtime-ownership.md`.

## Semantics Covered

The unit tests in `runtime_owner.rs` cover the scaffold behavior that is safe
to prove before the owner loop is wired:

- construction defaults are disabled and inert;
- `ClientWriteBuffer` preserves byte order and drains atomically;
- `ClientSlot` owns query bytes, staged argv, and reply bytes;
- `RuntimeEvent` queueing is FIFO and capacity-limited without silent drops;
- slot allocation reuses removed slot ids through the `SlotId` newtype;
- `OwnerCommandResult` carries the slot id for every outcome.

No benchmark-only command path is added. No socket readiness backend is chosen.
No second live database model is connected to the server.
