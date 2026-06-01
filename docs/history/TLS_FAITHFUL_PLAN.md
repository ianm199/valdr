# TLS — Faithful Implementation Plan

Status: PLAN (no code yet). Drafted 2026-05-27.

Goal: give Valdr client-facing TLS by finishing the upstream connection-type
abstraction and adding a TLS backend, staying as close to the upstream Valkey
design as the rustls/OpenSSL boundary allows.

This is a **transport-layer subproject**, not a one-file change. Read the whole
plan before starting; the phasing exists to avoid a live collision in
`runtime_owner.rs`.

---

## 1. The one place "just translate it" breaks

Everywhere else in this port (data structures, commands) we translate C → Rust
line-faithfully because there is no external-library boundary. **TLS is the
exception.** Upstream `reference/valkey/src/tls.c` is **2,028 lines of
OpenSSL** (171 `SSL_*`/`BIO_*` calls). Valdr uses **rustls**. The two libraries
have no 1:1 correspondence:

| Upstream OpenSSL | rustls equivalent |
|---|---|
| `SSL_accept` (handshake) | drive `read_tls`/`process_new_packets`/`write_tls` while `is_handshaking()` |
| `SSL_read` | `read_tls` → `process_new_packets` → `reader().read()` |
| `SSL_write` | `writer().write()` → `write_tls()` |
| `SSL_get_error` → `WANT_READ`/`WANT_WRITE` | `wants_read()` / `wants_write()` |
| `BIO` buffers, `last_failed_write_data_len` | rustls owns its plaintext/ciphertext buffers (not needed) |
| `SSL_CTX` + cert/cipher config | `rustls::ServerConfig` (already built in `redis-core/src/tls.rs`) |

So `tls.c` is a **structural translation with library substitution**: we
reproduce its control flow and connection lifecycle, but reimplement the crypto
calls against rustls.

### Why not the `openssl` crate (which *would* be a literal translation)?
Because it links the memory-unsafe OpenSSL C library back into a project whose
entire thesis — and launch story — is "safe Rust, no OpenSSL." rustls is the
right call, and accepting "structural not literal" here is the price. See
historical Redis CVEs: the safety value is in the data engine we already cover;
reintroducing OpenSSL would put a memory-unsafe surface back in.

---

## 2. Upstream architecture (what we translate)

`connection.h` (530 lines) defines `struct ConnectionType` — a vtable of
function pointers. `socket.c` (495 lines) and `tls.c` (2,028 lines) are two
backends behind it. The event loop (`ae.c`) calls each connection's
`ae_handler` on readiness, which runs the registered read/write handler.

The **hard kernel** is `tls.c`'s own opening comment:

> With TLS connections we need to handle cases where during a logical read or
> write operation, the SSL library asks to block for the opposite socket
> operation. … if we notify the caller of a write operation that it blocks, and
> SSL asks for a read, we need to trigger the write handler again on the next
> read event.

Upstream tracks this with `TLS_CONN_FLAG_READ_WANT_WRITE` /
`TLS_CONN_FLAG_WRITE_WANT_READ` and `updateSSLEvent()`/`registerSSLEvent()`.
**rustls collapses this bookkeeping into `wants_read()`/`wants_write()`** — after
every pump you recompute the mio `Interest` set from those two booleans. So the
rustls backend is structurally *simpler* than `tls.c` here, but must reproduce
the same *semantics* (handshake completes before app data; interest re-armed on
the opposite direction; `close_notify` handled).

---

## 3. Current port state

`crates/redis-core/src/connection.rs` (986 lines) already **faithfully ports the
vtable** as `trait ConnectionTypeTrait` (`read`/`write`/`writev`/`accept`/
`connect`/`listen`/`set_read_handler`/`set_write_handler`/`shutdown`/`close`/
`addr`/`sync_*`), plus a `Connection` struct and a global registry
(`conn_type_register`). Gaps (its own TODOs):

- **`ae_handler` / `accept_handler` not ported** — "depends on the event-loop
  design." This is the conceptual core: bridge C's `ae` handler-toggle to mio.
- **Zero backends registered** — no socket type, no TLS type.
- **`Mutex` registry** with a noted single-thread re-entrancy/deadlock risk to
  resolve (the dispatch closure calls back into registry functions).
- `get_peer_user` omitted (needs the ACL `User` type).

The running server does **not** use `connection.rs` yet — it uses the
`transport.rs` enum (`Tcp` / `Tls(StreamOwned)`), explicitly labelled a
temporary "Wave A pilot" to be collapsed into `connection.rs` once backends
land. The blocking `Tls(StreamOwned)` path in `main.rs` (`serve_tls` →
`handle_connection_tls`) works but is **fenced off** (`main.rs:1190` refuses the
TLS listener) because it predates the owner-loop DB-ownership flip and would
mutate a divergent DB. We are replacing that shortcut, not extending it.

---

## 4. socket.c → SocketConnectionType checklist

Translate `socket.c`'s `CT_Socket` vtable into a `SocketConnectionType:
ConnectionTypeTrait` (new module, e.g. `redis-core/src/conn_socket.rs`). Prove
the vtable on plain TCP *before* adding TLS.

| `CT_Socket` member | C fn | Rust target | Notes |
|---|---|---|---|
| `listen` | `connSocketListen` | bind via `mio::net::TcpListener` | mirrors current owner-loop listener |
| `accept` | `connSocketAccept` | accept → `conn_create_accepted` | calls accept_handler |
| `read` | `connSocketRead` | `MioTcpStream::read` | `WouldBlock` → 0 bytes / Pending |
| `write` | `connSocketWrite` | `MioTcpStream::write` | short writes ok |
| `writev` | `connSocketWritev` | `write_vectored` | |
| `set_read_handler` | `connSocketSetReadHandler` | store handler + arm `Interest::READABLE` | |
| `set_write_handler` | `connSocketSetWriteHandler` | store handler + arm `Interest::WRITABLE` | `barrier` flag |
| `ae_handler` | `connSocketEventHandler` | mio readiness → invoke handlers | the event-loop bridge |
| `shutdown`/`close` | `connSocketShutdown/Close` | `Shutdown`/drop | |
| `addr` | `connSocketAddr` | `peer_addr`/`local_addr` | |
| `connect` | `connSocketConnect` | outbound (replication client) | **defer** — server-inbound first |
| `sync_read/write/readline` | `connSocketSync*` | blocking helpers | **defer** — used by RDB/repl transfer |

Most of this is near-literal translation. The genuinely new design work is the
`ae_handler` ↔ mio bridge in `connection.rs` (§6).

---

## 5. tls.c → TlsConnectionType (rustls) method map

New module (e.g. `redis-core/src/conn_tls.rs`) implementing `TlsConnectionType:
ConnectionTypeTrait`. Per-connection state holds a `rustls::ServerConnection`
plus the `MioTcpStream`. `configure()` wraps the existing
`redis-core::tls::TlsConfig` (`Arc<rustls::ServerConfig>`, cert/key/CA, mTLS via
`WebPkiClientVerifier` — already done).

| `CT_TLS` member | C fn (OpenSSL) | rustls implementation |
|---|---|---|
| `conn_create*` | `createTLSConnection*` | `ServerConnection::new(Arc<ServerConfig>)` |
| `accept` | `connTLSAccept` (`SSL_accept`) | pump `read_tls`/`process_new_packets`/`write_tls` until `!is_handshaking()`, then fire accept_handler |
| `read` | `connTLSRead` (`SSL_read`) | `read_tls` → `process_new_packets` → `reader().read(buf)` |
| `write` | `connTLSWrite` (`SSL_write`) | `writer().write(data)` → `write_tls(sock)` |
| `writev` | `connTLSWritev` | `writer().write_vectored` → `write_tls` |
| `set_read_handler` | `connTLSSetReadHandler` | store handler + `update_event()` |
| `set_write_handler` | `connTLSSetWriteHandler` | store handler + `update_event()` |
| `ae_handler` | `tlsEventHandler` | on readiness: pump TLS, run handshake or app handler, then `update_event()` |
| `updateSSLEvent`/`registerSSLEvent` | flag bookkeeping | **replace** with: `Interest = (wants_read?READABLE) \| (wants_write?WRITABLE)`; reregister |
| error mapping | `handleSSLReturnCode`/`SSL_get_error` | map `rustls::Error` / `IoState` to connection error + `last_errno` |
| `connect` | `connTLSConnect` | outbound TLS (replication) — **defer** |
| `sync_*` | `connTLS Sync*` | blocking — **defer** |

Things that **disappear** vs `tls.c` (rustls owns them): the
`READ_WANT_WRITE`/`WRITE_WANT_READ` flags, `last_failed_write_data_len`, manual
`BIO` buffering. Things we **must** still get right: handshake-before-app-data,
re-arming the opposite-direction interest, `close_notify`, mTLS client-cert
verification result surfacing.

---

## 6. connection.rs gap closure (the conceptual core)

1. Decide the handler representation: `ConnectionCallbackFunc` over mio. Likely
   a `Token`-keyed slot the owner loop drives, with read/write handler closures
   stored on the `Connection`.
2. Port `ae_handler`/`accept_handler` semantics: on a mio readiness event for a
   token, look up the connection and invoke the stored read/write handler;
   honor the `barrier` flag (suppress write when read fired this tick).
3. Resolve the registry concurrency TODO — for the single-threaded owner this
   can be a non-`Mutex` (`RefCell`/owner-local) registry to avoid the re-entrant
   deadlock the current code warns about.

---

## 7. Phasing (collision-aware)

**Phase 1 — foundation, in `redis-core`, ZERO collision (do now):**
1. Close the `connection.rs` gaps (§6): `ae_handler`/`accept_handler` over mio.
2. `SocketConnectionType` ← translate `socket.c` (§4); prove the vtable on
   plain TCP with unit tests.
3. `TlsConnectionType` ← `tls.c` structure + rustls (§5); unit-test handshake +
   echo in isolation (loopback, self-signed cert).

**Phase 2 — rewire, touches `runtime_owner.rs` (GATED, see §8):**
4. Point the owner loop at the registry/vtable instead of the `transport.rs`
   enum; create accepted connections via `conn_create_accepted`.
5. Remove the TLS refusal guard (`main.rs:1190`); wire `CONFIG SET tls-port`,
   `tls-cert-file`, `tls-key-file`, `tls-ca-cert*`, `tls-auth-clients`, etc.
6. Retire `transport.rs` (the "Wave A pilot").

**Phase 3 — validate:**
7. Run the TLS suite via the oracle (note: the default profile denies `tls`; run
   it explicitly):
   `bash harness/oracle/run-single-node-tcl-suite.sh --skip-build --files unit/tls`
8. Stretch: run the full 54-file suite in upstream `--tls` mode — a brutal proof
   that the transport handles every command pattern, not just `unit/tls.tcl`.

---

## 8. Collision warning — `runtime_owner.rs`

As of 2026-05-27 `runtime_owner.rs` has ~156 lines of **uncommitted WIP** on the
`pause_postponed` / command-continuation path — the `pause.tcl` command-loop
gate (another agent's lane; see `AGENT_COORDINATION_BOARD.md` and the
`pause-tcl-is-all-or-nothing` note). **Phase 2 must not start until that lands**,
or it will clobber in-flight work. Phase 1 lives entirely in `redis-core` and is
safe to start immediately. Claim a row on the coordination board before Phase 2.

---

## 9. Effort & risk

- **Phase 1**: large but mostly mechanical, except the `ae_handler`↔mio bridge
  (design) and the rustls non-blocking handshake (the fixed-cost kernel — the
  same difficulty in *any* TLS approach, so paying it inside the faithful vtable
  is the better investment).
- **Phase 2**: medium; the risk is the owner-loop rewire interacting with client
  state (selected DB, MULTI queue) and the pause work.
- **Top risk**: non-blocking TLS handshake edge cases (WANT_WRITE during read,
  partial records across readiness events, `close_notify`) — pass in dev, flake
  under load. Mitigation: the full `--tls` suite run in Phase 3.

## 10. Open decisions (TODO human/architect)
- Handler representation in `connection.rs` (closures vs token-dispatch).
- Whether replication/cluster TLS (`connect`, `sync_*`) is in scope now (plan:
  defer — single-node client TLS first).
- Registry concurrency model for the single-threaded owner.

---

## 11. Fast-iteration tooling (build this FIRST)

The naïve loop — build redis-server → spawn process → spawn tclsh + test_helper
→ generate certs → real sockets → diff — is seconds per iteration and gives a
coarse signal. For a non-blocking state machine that is the worst feedback loop.
Two simplifications make a microsecond loop possible:

1. **TLS testing = transport-*transparency* testing.** Command semantics are
   already oracle-verified over plain TCP (2,734 tests). The TLS layer only has
   to prove "bytes in = bytes out, with correct handshake/error behavior."
2. **rustls is built for in-memory testing.** `ServerConnection`/
   `ClientConnection` talk over any `Read`/`Write`; the canonical rustls test
   shuttles ciphertext between two buffers with no sockets.

### Tooling, by leverage

1. **`TestPipe` — scriptable in-memory non-blocking duplex (foundation).** A
   `Read + Write` fake you control byte-for-byte: deliver N bytes then
   `WouldBlock`, split a record across "readiness events," force short writes.
   The classic TLS-over-event-loop bugs (partial record across events,
   WANT_WRITE-during-read) reproduce here *deterministically and instantly*
   where a real socket reproduces them unreliably.
2. **In-memory peer + `drive()` pump.** Pair a `rustls::ClientConnection` (peer)
   with the server-side connection over the `TestPipe`; `drive()` pumps both to
   quiescence. Full handshake in <1 ms, zero network.
3. **Generic connection-type conformance battery.** One battery any backend must
   pass (accept → echo → large write → half-close → error-on-garbage). Run it
   against the socket backend first (proves the `ae_handler`↔mio bridge with no
   crypto), then TLS. Reusable for future unix/RDMA backends — harness IP, not a
   one-off.
4. **Scenario micro-suite (the "extrapolate to passing" set).** One micro-test
   per transport behavior `unit/tls.tcl` depends on: clean handshake, split
   handshake (1 byte/event), echo, large reply (multiple `write_tls`), mTLS
   (cert / no-cert / bad-cert), `close_notify`, malformed ClientHello. Keep a
   map: each micro-scenario → the `tls.tcl` test it predicts.
5. **Randomized chunking/WouldBlock invariant.** For random byte-chunkings and
   WouldBlock points, handshake+echo must always complete with identical
   plaintext. Kills the "passes in dev, flakes under load" class deterministically.
   (Hand-rolled seed sweep first; upgrade to `proptest` later.)
6. **Keep it in `redis-core`; loop = `cargo test -p redis-core`.** Phase 1 is
   redis-core-internal, so downstream crates never rebuild and the binary never
   links. Incremental edit→test is sub-second.

### The iteration ladder ("extrapolate to passing")

```
micro-suite (in-memory, µs–ms)   ← iterate here ~99% of the time
   ↓ green ⇒ predicts ↓
CLI smoke (loopback, ~1s)        ← optional backstop: real socket, rustls/openssl peer
   ↓ green ⇒ predicts ↓
unit/tls.tcl via oracle          ← run rarely, as a gate (not a loop)
   ↓ green ⇒ ↓
full suite over --tls            ← final proof, once
```

When the oracle eventually disagrees with the micro-suite, that gap becomes a
new micro-test — the suite gets sharper over time.

### Status
`TestPipe` + the drive pump + an in-memory rustls handshake/echo proof + the
randomized chunking invariant live in
`crates/redis-core/tests/conn_transport_kit.rs` (test cert fixtures under
`crates/redis-core/tests/fixtures/`). This is collision-free (test code only)
and proves the fast loop before any `runtime_owner.rs` work.
