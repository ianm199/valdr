//! Unix-domain socket connection backend.
//! Implements `ConnectionTypeTrait`
//! for Unix-domain sockets and registers the backend with the global connection
//! registry.
//! # Design
//! In C, `CT_Unix` is a static `ConnectionType` struct whose function-pointer
//! fields mostly forward to `connectionTypeTcp->method(conn,...)`. The
//! only truly Unix-specific behaviour is:
//! - `addr` — always returns the socket path + port 0 (never a real IP).
//! - `is_local` — always returns `true`.
//! - `listen` — creates and binds a Unix-domain socket fd via `anetUnixServer`.
//! - `accept_handler` — accepts new clients and calls `acceptCommonHandler` with
//! the `unix_socket` client flag set.
//! - `get_last_error` — uses `strerror(conn->last_errno)`.
//! All I/O methods (`write`, `read`, `writev`, `shutdown`, `close`, `accept`,
//! `set_write_handler`, `set_read_handler`, and the three `sync_*` variants)
//! delegate unchanged to the TCP/socket backend because the underlying fd
//! behaves identically after accept.
//! # PORT NOTE: TCP delegation
//! The registry's `with_conn_type` helper in `connection.rs` is currently
//! `fn` (private). Delegating methods therefore carry `TODO(port)` stubs.
//! TODO(architect): expose `pub(crate) fn with_conn_type` (or a
//! `pub(crate) fn delegate_to_tcp(conn, closure)` wrapper) in `connection.rs`
//! so that `unix.rs` can forward I/O calls to `ConnectionTypeId::Socket`
//! without re-acquiring the mutex or holding a raw `ConnectionType *`.

use std::io;
use std::io::IoSlice;

use crate::connection::{
    conn_type_register, ConnListener, Connection, ConnectionCallbackFunc, ConnectionState,
    ConnectionTypeId, ConnectionTypeTrait,
};
use redis_types::RedisError;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Maximum IOV count for scatter-gather writes on this platform.
/// Mirrors the value written to `conn->iovcnt` in `connCreateUnix`.
/// TODO(port): replace with `libc::IOV_MAX` once `libc` is added to
/// `redis-core`'s `Cargo.toml` (architect decision).
pub const IOV_MAX: u16 = 1024;

// ─── UnixConnectionType ───────────────────────────────────────────────────────

/// Unix-domain socket connection backend.
/// Implements `ConnectionTypeTrait`. Stateless except for the cached socket
/// path, which is set during `listen` so that `addr` can return it without
/// needing access to global server state.
pub struct UnixConnectionType {
 /// Path of the bound Unix socket. Set by `listen` on the first bind.
 /// PORT NOTE: C read this from `server.unixsocket` (a process global).
 /// Storing it here makes `addr` testable without global server state.
    socket_path: Vec<u8>,
}

impl UnixConnectionType {
 /// Create a new, un-bound Unix connection type backend.
    pub fn new() -> Self {
        Self {
            socket_path: Vec::new(),
        }
    }
}

impl Default for UnixConnectionType {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionTypeTrait for UnixConnectionType {
 /// Return the Unix connection type discriminant.
    fn get_type_id(&self) -> ConnectionTypeId {
        ConnectionTypeId::Unix
    }

 /// No per-type global configuration for Unix sockets.
    fn configure(&mut self, _reconfigure: bool) -> Result<(), RedisError> {
        Ok(())
    }

 /// Return `(socket_path_bytes, 0)` as the connection address.
 /// Unix sockets have no IP address or port; the path is returned in place
 /// of the IP field and port is always 0. Both `remote` and `conn` are
 /// ignored — the path is the same for every end of the connection.
 /// C equivalent: `snprintf(ip, ip_len, "%s:0", server.unixsocket); *port = 0;`
    fn addr(&self, _conn: &Connection, _remote: bool) -> Result<(Vec<u8>, u16), RedisError> {
        Ok((self.socket_path.clone(), 0))
    }

 /// Unix-domain sockets are always local connections.
 /// always returns `1`.
    fn is_local(&self, _conn: &Connection) -> Option<bool> {
        Some(true)
    }

 /// Bind and listen on all Unix socket paths in `listener.bindaddr`.
 /// The C implementation calls `unlink(addr)` to remove any stale socket
 /// file, then `anetUnixServer(neterr, addr, perm, backlog, group)`
 /// bind and listen. It also reads `perm`/`group` from a per-listener
 /// private config struct (`serverUnixContextConfig *ctx_cfg = listener->priv`).
    /// TODO(port): replace with `std::os::unix::net::UnixListener::bind(path)`
 /// + `set_nonblocking(true)` + `IntoRawFd::into_raw_fd`.
 /// Also call `std::fs::remove_file(path)` beforehand (mirrors `unlink`).
    /// TODO(port): `listener.priv` (carrying `perm` and `group`) is omitted from
 /// `ConnListener` in connection.rs. Phase B should add a typed extension
 /// field (e.g. `unix_config: Option<UnixListenerConfig>`) or pass these
 /// through a separate config argument.
    /// TODO(port): `server.tcp_backlog` is needed for the listen backlog depth
 /// but is not accessible here without a reference to `RedisServer`.
    fn listen(&self, listener: &mut ConnListener) -> Result<(), RedisError> {
        if listener.bindaddr_count == 0 {
            return Ok(());
        }

 // PORT NOTE: currently one bind address is always supplied
 // (`bindaddr_count == 1`), but the loop mirrors the C source
 // support potential future multi-socket configurations.
        if let Some(j) = (0..(listener.bindaddr_count as usize)).next() {
            let addr = listener.bindaddr[j].clone();

            // TODO(port): std::fs::remove_file(&addr).ok();  — mirrors unlink(addr)
            // TODO(port): let raw_fd = anet::unix_server(&addr, perm, backlog, group)?;
            // TODO(port): listener.fd[listener.count as usize] = raw_fd;
            // TODO(port): listener.count += 1;

            let _ = addr;

            return Err(RedisError::runtime(
                b"connUnixListen: anet::unix_server not yet ported (Phase B)",
            ));
        }

        Ok(())
    }

 /// Close all fds opened by `listen`; delegates to the TCP backend.
    /// TODO(port): call the TCP backend's `close_listener` via crate-internal
 /// dispatch once `with_conn_type` is `pub(crate)` in `connection.rs`.
    fn close_listener(&self, listener: &mut ConnListener) {
        // TODO(port): delegate to ConnectionTypeId::Socket backend close_listener.
 // Fallback: close all fds held by this listener directly.
        for i in 0..(listener.count as usize) {
            // TODO(port): call libc::close(listener.fd[i]) here once `libc` is
 // available. For Phase A this is a no-op stub.
            let _ = listener.fd[i];
        }
        listener.count = 0;
    }

 /// Allocate a new outbound Unix-socket connection (fd = -1).
    fn conn_create(&self) -> Connection {
        let mut conn = Connection::new(ConnectionTypeId::Unix, -1);
        conn.iovcnt = IOV_MAX;
        conn
    }

 /// Allocate a `Connection` wrapping an already-accepted Unix fd.
    fn conn_create_accepted(&self, fd: i32) -> Connection {
        let mut conn = self.conn_create();
        conn.fd = fd;
        conn.state = ConnectionState::Accepting;
        conn
    }

 /// Graceful shutdown; delegates to the TCP backend.
    /// TODO(port): dispatch to `ConnectionTypeId::Socket` backend `shutdown`
 /// via `with_conn_type` once that function is `pub(crate)`.
    fn shutdown(&self, conn: &mut Connection) {
        let _ = conn;
        // TODO(port): delegate to TCP backend shutdown.
    }

 /// Hard close; delegates to the TCP backend.
    /// TODO(port): dispatch to `ConnectionTypeId::Socket` backend `close`
 /// via `with_conn_type` once that function is `pub(crate)`.
    fn close(&self, conn: &mut Connection) {
        let _ = conn;
        // TODO(port): delegate to TCP backend close.
    }

 /// Outbound connect — not available for the Unix backend (server-side only).
    fn connect(
        &self,
        _conn: &mut Connection,
        _addr: &[u8],
        _port: u16,
        _source_addr: Option<&[u8]>,
        _multipath: bool,
        _connect_handler: ConnectionCallbackFunc,
    ) -> Result<(), RedisError> {
        Err(RedisError::runtime(
            b"connect not supported for Unix connection type",
        ))
    }

 /// Blocking outbound connect — not available for the Unix backend.
    fn blocking_connect(
        &self,
        _conn: &mut Connection,
        _addr: &[u8],
        _port: u16,
        _timeout_ms: i64,
    ) -> Result<(), RedisError> {
        Err(RedisError::runtime(
            b"blocking_connect not supported for Unix connection type",
        ))
    }

 /// Accept an inbound connection; delegates to the TCP backend.
    /// TODO(port): dispatch to `ConnectionTypeId::Socket` backend `accept`
 /// via `with_conn_type` once that function is `pub(crate)`.
    fn accept(
        &self,
        conn: &mut Connection,
        accept_handler: ConnectionCallbackFunc,
    ) -> Result<(), RedisError> {
        let _ = (conn, accept_handler);
        // TODO(port): delegate to TCP backend accept.
        Err(RedisError::runtime(
            b"connUnixAccept: TCP delegation not yet wired (Phase B)",
        ))
    }

 /// Write bytes to the connection; delegates to the TCP backend.
    /// TODO(port): dispatch to `ConnectionTypeId::Socket` backend `write`
 /// via `with_conn_type` once that function is `pub(crate)`.
    fn write(&self, conn: &mut Connection, data: &[u8]) -> Result<usize, RedisError> {
        let _ = (conn, data);
        // TODO(port): delegate to TCP backend write.
        Err(RedisError::runtime(
            b"connUnixWrite: TCP delegation not yet wired (Phase B)",
        ))
    }

 /// Scatter-gather write; delegates to the TCP backend.
    /// TODO(port): dispatch to `ConnectionTypeId::Socket` backend `writev`
 /// via `with_conn_type` once that function is `pub(crate)`.
    fn writev(&self, conn: &mut Connection, iov: &[IoSlice<'_>]) -> Result<usize, RedisError> {
        let _ = (conn, iov);
        // TODO(port): delegate to TCP backend writev.
        Err(RedisError::runtime(
            b"connUnixWritev: TCP delegation not yet wired (Phase B)",
        ))
    }

 /// Read bytes from the connection; delegates to the TCP backend.
    /// TODO(port): dispatch to `ConnectionTypeId::Socket` backend `read`
 /// via `with_conn_type` once that function is `pub(crate)`.
    fn read(&self, conn: &mut Connection, buf: &mut [u8]) -> Result<usize, RedisError> {
        let _ = (conn, buf);
        // TODO(port): delegate to TCP backend read.
        Err(RedisError::runtime(
            b"connUnixRead: TCP delegation not yet wired (Phase B)",
        ))
    }

 /// Register the write-ready handler; delegates to the TCP backend.
    /// TODO(port): dispatch to `ConnectionTypeId::Socket` backend `set_write_handler`
 /// via `with_conn_type` once that function is `pub(crate)`.
    fn set_write_handler(
        &self,
        conn: &mut Connection,
        handler: Option<ConnectionCallbackFunc>,
        barrier: bool,
    ) -> Result<(), RedisError> {
        let _ = (conn, handler, barrier);
        // TODO(port): delegate to TCP backend set_write_handler.
        Err(RedisError::runtime(
            b"connUnixSetWriteHandler: TCP delegation not yet wired (Phase B)",
        ))
    }

 /// Register the read-ready handler; delegates to the TCP backend.
    /// TODO(port): dispatch to `ConnectionTypeId::Socket` backend `set_read_handler`
 /// via `with_conn_type` once that function is `pub(crate)`.
    fn set_read_handler(
        &self,
        conn: &mut Connection,
        handler: Option<ConnectionCallbackFunc>,
    ) -> Result<(), RedisError> {
        let _ = (conn, handler);
        // TODO(port): delegate to TCP backend set_read_handler.
        Err(RedisError::runtime(
            b"connUnixSetReadHandler: TCP delegation not yet wired (Phase B)",
        ))
    }

 /// Return the last transport-level error as bytes.
 /// returns `strerror(conn->last_errno)`.
 /// PORT NOTE: C returned a static `const char *` from `strerror`. Rust maps
 /// `last_errno` to an `std::io::Error` description and converts to `Vec<u8>`.
 /// The result is an OS error description string (not Redis data), so
 /// use of `String` via `to_string` is permitted here.
    fn get_last_error(&self, conn: &Connection) -> Option<Vec<u8>> {
        if conn.last_errno == 0 {
            return None;
        }
        let msg = io::Error::from_raw_os_error(conn.last_errno).to_string();
        Some(msg.into_bytes())
    }

 /// Blocking write with millisecond timeout; delegates to `syncWrite`.
    /// TODO(port): call `syncio::sync_write(conn.fd, data, timeout_ms)` once
 /// `syncio.rs` is ported (phase: defer in file-deps.tsv).
    fn sync_write(
        &self,
        conn: &mut Connection,
        data: &[u8],
        timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        let _ = (conn, data, timeout_ms);
        // TODO(port): delegate to syncio::sync_write(conn.fd, data, timeout_ms).
        Err(RedisError::runtime(
            b"connUnixSyncWrite: syncio not yet ported (Phase B)",
        ))
    }

 /// Blocking read with millisecond timeout; delegates to `syncRead`.
    /// TODO(port): call `syncio::sync_read(conn.fd, buf, timeout_ms)` once
 /// `syncio.rs` is ported (phase: defer in file-deps.tsv).
    fn sync_read(
        &self,
        conn: &mut Connection,
        buf: &mut [u8],
        timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        let _ = (conn, buf, timeout_ms);
        // TODO(port): delegate to syncio::sync_read(conn.fd, buf, timeout_ms).
        Err(RedisError::runtime(
            b"connUnixSyncRead: syncio not yet ported (Phase B)",
        ))
    }

 /// Blocking line-read with millisecond timeout; delegates to `syncReadLine`.
    /// TODO(port): call `syncio::sync_readline(conn.fd, buf, timeout_ms)` once
 /// `syncio.rs` is ported (phase: defer in file-deps.tsv).
    fn sync_readline(
        &self,
        conn: &mut Connection,
        buf: &mut [u8],
        timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        let _ = (conn, buf, timeout_ms);
        // TODO(port): delegate to syncio::sync_readline(conn.fd, buf, timeout_ms).
        Err(RedisError::runtime(
            b"connUnixSyncReadLine: syncio not yet ported (Phase B)",
        ))
    }
}

// ─── Event-loop accept handler ────────────────────────────────────────────────

/// Accept new Unix-domain socket connections in a bounded loop.
/// Called by the event loop when the listening Unix socket fd becomes readable.
/// Accepts up to `max_new_conns` new clients per invocation, then returns.
/// PORT NOTE: In C this is a bare `aeFileProc` callback registered directly
/// with the ae event loop. In Rust it becomes a free function. How and when
/// it is registered depends on the Phase 2 event-loop design (architect decision).
/// C flow (condensed):
/// ```c
/// while (max--) {
/// cfd = anetUnixAccept(server.neterr, fd);
/// if (cfd == ANET_ERR) { handle_error; return; }
/// flags.unix_socket = 1;
/// acceptCommonHandler(connCreateAcceptedUnix(cfd, NULL), flags, NULL);
/// }
/// ```
/// TODO(port): call `anet::unix_accept(server_neterr_buf, listen_fd)` for
/// each accept iteration once `anet.rs` is ported (phase: defer).
/// TODO(port): call `networking::accept_common_handler(conn, flags, None)` once
/// `networking.rs` exposes that entry point publicly. It is currently phase:
/// pilot in file-deps.tsv.
/// TODO(port): the `ClientFlags` struct (with `unix_socket: bool`) must be
/// defined; it lives in `client.rs` once ported.
pub fn unix_accept_handler(
    listen_fd: i32,
    max_new_conns: i32,
    backend: &UnixConnectionType,
) -> Result<(), RedisError> {
    let remaining = max_new_conns;

    while remaining > 0 {
 // PORT NOTE: remaining -= 1 removed — dead assignment before break (placeholder loop).
        // TODO(port): cfd = anet::unix_accept(neterr_buf, listen_fd)?;
 // On EAGAIN/EWOULDBLOCK → break (no more pending connections).
 // On EINTR → continue (interrupted; retry).
 // On other errors → log warning and return.
 // if (cfd == ANET_ERR) {
 // if (anetRetryAcceptOnError(errno)) continue;
 // if (errno != EWOULDBLOCK)
 // serverLog(LL_WARNING, "Accepting client connection: %s", server.neterr);
 // return;
 // }

        // TODO(port): build the accepted connection:
 // let conn = backend.conn_create_accepted(cfd);

        // TODO(port): set the unix_socket flag and call accept_common_handler:
 // let mut flags = ClientFlags::default;
 // flags.unix_socket = true;
 // networking::accept_common_handler(conn, flags, None)?;
 // serverLog(LL_VERBOSE, "Accepted connection to %s", server.unixsocket);
 // acceptCommonHandler(connCreateAcceptedUnix(cfd, NULL), flags, NULL);

 // Suppress unused-variable warning on `backend` in Phase A stub.
        let _ = (listen_fd, backend);

        break;
    }

    Ok(())
}

// ─── Registration ─────────────────────────────────────────────────────────────

/// Register the Unix-domain socket connection type with the global registry.
/// Calls `conn_type_register`, which invokes `init` on the backend
/// immediately (matching C behaviour where `connTypeRegister` calls `init`).
/// PORT NOTE: The C function name is `RedisRegisterConnectionTypeUnix`.
/// Rust uses `register_connection_type_unix` (snake_case, no `Redis` prefix
///.
pub fn register_connection_type_unix() -> Result<(), RedisError> {
    conn_type_register(Box::new(UnixConnectionType::new()))
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         18
//   port_notes:    3
//   unsafe_blocks: 0
//   notes:         UnixConnectionType implements ConnectionTypeTrait; all I/O
//                  methods are stubbed with TODO(port) pending TCP-backend
//                  delegation via pub(crate) with_conn_type in connection.rs;
//                  listen() and accept_handler() are stubbed pending anet.rs
//                  and networking.rs phase-B wiring; get_last_error uses
//                  std::io::Error::from_raw_os_error for strerror translation.
// ──────────────────────────────────────────────────────────────────────────
