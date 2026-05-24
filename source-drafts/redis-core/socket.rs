//! TCP socket connection backend.
//!
//! Port of `socket.c` (496 lines, 23 functions).
//! Implements [`ConnectionTypeTrait`] for plain TCP sockets — the Rust
//! equivalent of C's `static ConnectionType CT_Socket` vtable.
//!
//! All raw POSIX I/O syscalls (`read`, `write`, `writev`, `shutdown`, `close`,
//! socket-option setters) require either `unsafe` or the `libc` crate, neither
//! of which is permitted in pilot crates.  Those call sites are marked
//! `TODO(architect)` and return `Err` stubs.  Phase B will add the `libc`
//! dependency and wire up the real syscalls (or replace them with
//! `std::net::TcpStream`-based wrappers).
//!
//! Event-loop integration (`aeCreateFileEvent`, `aeDeleteFileEvent`, `aeWait`)
//! is stubbed; `ae.c` is phase: defer.  When `event_loop.rs` is ported the
//! local `AE_READABLE`/`AE_WRITABLE` aliases should become imports from
//! `crate::event_loop`.
//!
//! PORT NOTE: `socket.rs` must be declared as `pub mod socket;` in `lib.rs`
//! for Phase B compilation.  It is omitted here to keep the Phase A translator
//! scope to a single file.
//!
//! # C source reference
//! `reference/valkey/src/socket.c`

use std::io::IoSlice;

use redis_types::RedisError;

use crate::connection::{
    conn_type_register, ConnListener, Connection, ConnectionCallbackFunc,
    ConnectionState, ConnectionTypeId, ConnectionTypeTrait, CONN_FLAG_CLOSE_SCHEDULED,
    CONN_FLAG_WRITE_BARRIER,
};

// ─── AE event-mask constants ──────────────────────────────────────────────────
//
// Re-stated from ae.h (phase: defer) so the event-handler logic can be
// expressed faithfully in Phase A.
//
// TODO(port): replace with `use crate::event_loop::{AE_READABLE, AE_WRITABLE}`
// once event_loop.rs is ported.

const AE_READABLE: i32 = 1;
const AE_WRITABLE: i32 = 2;

// ─── errno stub ───────────────────────────────────────────────────────────────
//
// TODO(architect): replace with `libc::ETIMEDOUT` (and `libc::EAGAIN`,
// `libc::EINTR`, `libc::EWOULDBLOCK` used in commented-out logical stubs)
// once the `libc` crate is added to redis-core's Cargo.toml.

const ETIMEDOUT: i32 = 110;

// ─── IOV_MAX ──────────────────────────────────────────────────────────────────

/// Maximum scatter-gather vector count passed to `writev(2)`.
///
/// C: `IOV_MAX` from `<sys/uio.h>`.
///
/// TODO(architect): replace with `libc::IOV_MAX as u16` once libc is available.
const IOV_MAX: u16 = 1024;

// ─── Internal helpers ─────────────────────────────────────────────────────────

/// Increment the connection's reference count, invoke `handler`, then decrement.
///
/// Returns `false` if the connection was scheduled for close and must not be
/// accessed again by the caller.
///
/// C: `callHandler` macro from `connhelpers.h`.
fn call_handler(conn: &mut Connection, handler: ConnectionCallbackFunc) -> bool {
    conn.refs += 1;
    handler(conn);
    conn.refs -= 1;
    if conn.refs == 0 && (conn.flags & CONN_FLAG_CLOSE_SCHEDULED) != 0 {
        // TODO(port): call SocketConnectionType::close(conn) here to complete the
        // deferred close.  Phase B should wire this up through the global registry
        // or by threading a backend reference through the call sites.
        conn.state = ConnectionState::Closed;
        return false;
    }
    true
}

/// Return `true` if at least one handler call is currently executing on this
/// connection (i.e. `refs > 0`).
///
/// C: `connHasRefs` macro from `connhelpers.h`.
fn conn_has_refs(conn: &Connection) -> bool {
    conn.refs > 0
}

// ─── SocketConnectionType ────────────────────────────────────────────────────

/// Backend implementing [`ConnectionTypeTrait`] for plain TCP sockets.
///
/// Zero-sized; all mutable runtime state lives in [`Connection`].
///
/// C: `static ConnectionType CT_Socket` in `socket.c`.
pub struct SocketConnectionType;

impl ConnectionTypeTrait for SocketConnectionType {
    fn get_type_id(&self) -> ConnectionTypeId {
        ConnectionTypeId::Socket
    }

    fn configure(&mut self, _reconfigure: bool) -> Result<(), RedisError> {
        // CT_Socket.configure = NULL in C — no-op.
        Ok(())
    }

    // ── Connection creation ───────────────────────────────────────────────────

    /// C: `connCreateSocket` (socket.c:78-85)
    fn conn_create(&self) -> Connection {
        let mut conn = Connection::new(ConnectionTypeId::Socket, -1);
        conn.iovcnt = IOV_MAX;
        conn
    }

    /// C: `connCreateAcceptedSocket` (socket.c:97-103)
    fn conn_create_accepted(&self, fd: i32) -> Connection {
        let mut conn = self.conn_create();
        conn.fd = fd;
        conn.state = ConnectionState::Accepting;
        conn
    }

    // ── Shutdown / close ──────────────────────────────────────────────────────

    /// C: `connSocketShutdown` (socket.c:133-137)
    fn shutdown(&self, conn: &mut Connection) {
        if conn.fd == -1 {
            return;
        }
        // TODO(architect): libc::shutdown(conn.fd, libc::SHUT_RDWR) — unsafe raw-fd op
        // not permitted in pilot crates.
    }

    /// C: `connSocketClose` (socket.c:140-156)
    fn close(&self, conn: &mut Connection) {
        if conn.fd != -1 {
            // TODO(architect): aeDeleteFileEvent(server.el, conn.fd, AE_READABLE | AE_WRITABLE)
            // — deferred (ae.c: defer).
            // TODO(architect): libc::close(conn.fd) — unsafe raw-fd op.
            conn.fd = -1;
        }
        if conn_has_refs(conn) {
            conn.flags |= CONN_FLAG_CLOSE_SCHEDULED;
            return;
        }
        // C: zfree(conn) — memory is owned by Rust; freed by Drop when the owning
        // Box is released.
    }

    // ── Connect ───────────────────────────────────────────────────────────────

    /// C: `connSocketConnect` (socket.c:105-125)
    fn connect(
        &self,
        conn: &mut Connection,
        _addr: &[u8],
        _port: u16,
        _source_addr: Option<&[u8]>,
        _multipath: bool,
        _connect_handler: ConnectionCallbackFunc,
    ) -> Result<(), RedisError> {
        // TODO(architect): needs anetTcpNonBlockBestEffortBindConnect (anet.c: defer).
        //
        // Logical translation preserved for Phase B:
        //   let fd = anet_tcp_non_block_best_effort_bind_connect(addr, port, src_addr, multipath)?;
        //   conn.fd = fd;
        //   conn.state = ConnectionState::Connecting;
        //   conn.conn_handler = Some(connect_handler);
        //   aeCreateFileEvent(server.el, conn.fd, AE_WRITABLE, socket_event_handler_ptr, conn);
        //   return Ok(());
        conn.state = ConnectionState::Error;
        Err(RedisError::runtime(
            b"connSocketConnect: anet not yet ported (Phase B)",
        ))
    }

    /// C: `connSocketBlockingConnect` (socket.c:369-387)
    fn blocking_connect(
        &self,
        conn: &mut Connection,
        _addr: &[u8],
        _port: u16,
        _timeout_ms: i64,
    ) -> Result<(), RedisError> {
        // TODO(architect): needs anetTcpNonBlockConnect + aeWait (both deferred).
        //
        // Logical translation preserved for Phase B:
        //   let fd = anet_tcp_non_block_connect(addr, port)?;
        //   conn.fd = fd;
        //   if (aeWait(fd, AE_WRITABLE, timeout_ms) & AE_WRITABLE) == 0 {
        //       conn.state = ConnectionState::Error;
        //       conn.last_errno = ETIMEDOUT;
        //       return Err(RedisError::runtime(b"connection timed out"));
        //   }
        //   conn.state = ConnectionState::Connected;
        //   return Ok(());
        conn.state = ConnectionState::Error;
        conn.last_errno = ETIMEDOUT;
        Err(RedisError::runtime(
            b"connSocketBlockingConnect: anet/aeWait not yet ported (Phase B)",
        ))
    }

    // ── Accept ────────────────────────────────────────────────────────────────

    /// C: `connSocketAccept` (socket.c:211-220)
    fn accept(
        &self,
        conn: &mut Connection,
        accept_handler: ConnectionCallbackFunc,
    ) -> Result<(), RedisError> {
        if conn.state != ConnectionState::Accepting {
            return Err(RedisError::runtime(
                b"connSocketAccept: connection not in ACCEPTING state",
            ));
        }
        conn.state = ConnectionState::Connected;
        if !call_handler(conn, accept_handler) {
            return Err(RedisError::Closed);
        }
        Ok(())
    }

    // ── I/O ───────────────────────────────────────────────────────────────────

    /// C: `connSocketWrite` (socket.c:158-174)
    fn write(&self, _conn: &mut Connection, _data: &[u8]) -> Result<usize, RedisError> {
        // TODO(port): C debug-asserts that io_write_state != CLIENT_PENDING_IO via
        // connGetPrivateData().  Omitted: Connection.private_data not yet defined.
        //
        // TODO(architect): libc::write(conn.fd, data.as_ptr() as *const _, data.len())
        // — unsafe raw-fd op not permitted in pilot crates.
        //
        // Logical equivalent (errno handling preserved):
        //   let ret = libc::write(conn.fd, data.as_ptr() as *const _, data.len()) as isize;
        //   if ret < 0 && last_errno != EAGAIN {
        //       conn.last_errno = last_errno;
        //       if last_errno != EINTR && conn.state == ConnectionState::Connected {
        //           conn.state = ConnectionState::Error;
        //       }
        //   }
        //   Ok(ret as usize)
        Err(RedisError::runtime(
            b"connSocketWrite: libc::write not yet available (Phase B)",
        ))
    }

    /// C: `connSocketWritev` (socket.c:176-188)
    fn writev(&self, _conn: &mut Connection, _iov: &[IoSlice<'_>]) -> Result<usize, RedisError> {
        // TODO(architect): libc::writev(conn.fd, iov.as_ptr() as *const _, iov.len() as i32)
        // — unsafe raw-fd op.
        //
        // Logical equivalent mirrors connSocketWrite errno handling above.
        Err(RedisError::runtime(
            b"connSocketWritev: libc::writev not yet available (Phase B)",
        ))
    }

    /// C: `connSocketRead` (socket.c:190-209)
    fn read(&self, _conn: &mut Connection, _buf: &mut [u8]) -> Result<usize, RedisError> {
        // TODO(port): same io_read_state debug assertion as write — omitted.
        //
        // TODO(architect): libc::read(conn.fd, buf.as_mut_ptr() as *mut _, buf.len())
        // — unsafe raw-fd op.
        //
        // Logical equivalent:
        //   let ret = libc::read(conn.fd, buf.as_mut_ptr() as *mut _, buf.len()) as isize;
        //   if ret == 0 { conn.state = ConnectionState::Closed; return Ok(0); }
        //   if ret < 0 && last_errno != EAGAIN {
        //       conn.last_errno = last_errno;
        //       if last_errno != EINTR && conn.state == ConnectionState::Connected {
        //           conn.state = ConnectionState::Error;
        //       }
        //   }
        //   Ok(ret as usize)
        Err(RedisError::runtime(
            b"connSocketRead: libc::read not yet available (Phase B)",
        ))
    }

    // ── Handler registration ──────────────────────────────────────────────────

    /// C: `connSocketSetWriteHandler` (socket.c:230-243)
    fn set_write_handler(
        &self,
        conn: &mut Connection,
        handler: Option<ConnectionCallbackFunc>,
        barrier: bool,
    ) -> Result<(), RedisError> {
        if handler == conn.write_handler {
            return Ok(());
        }
        conn.write_handler = handler;
        if barrier {
            conn.flags |= CONN_FLAG_WRITE_BARRIER;
        } else {
            conn.flags &= !CONN_FLAG_WRITE_BARRIER;
        }
        if conn.write_handler.is_none() {
            // TODO(architect): aeDeleteFileEvent(server.el, conn.fd, AE_WRITABLE)
        } else {
            // TODO(architect): aeCreateFileEvent(server.el, conn.fd, AE_WRITABLE,
            //                                    socket_event_handler_ptr, conn)
            //   if result == AE_ERR: return Err(RedisError::runtime(b"aeCreateFileEvent failed"))
        }
        Ok(())
    }

    /// C: `connSocketSetReadHandler` (socket.c:248-257)
    fn set_read_handler(
        &self,
        conn: &mut Connection,
        handler: Option<ConnectionCallbackFunc>,
    ) -> Result<(), RedisError> {
        if handler == conn.read_handler {
            return Ok(());
        }
        conn.read_handler = handler;
        if conn.read_handler.is_none() {
            // TODO(architect): aeDeleteFileEvent(server.el, conn.fd, AE_READABLE)
        } else {
            // TODO(architect): aeCreateFileEvent(server.el, conn.fd, AE_READABLE,
            //                                    socket_event_handler_ptr, conn)
            //   if result == AE_ERR: return Err(RedisError::runtime(b"aeCreateFileEvent failed"))
        }
        Ok(())
    }

    // ── Error reporting ───────────────────────────────────────────────────────

    /// C: `connSocketGetLastError` (socket.c:259-261)
    fn get_last_error(&self, conn: &Connection) -> Option<Vec<u8>> {
        if conn.last_errno == 0 {
            return None;
        }
        // TODO(port): replace with libc::strerror(conn.last_errno) once libc is
        // available; for now emit a placeholder that preserves the errno value.
        Some(format!("errno={}", conn.last_errno).into_bytes())
    }

    // ── Address queries ───────────────────────────────────────────────────────

    /// C: `connSocketAddr` (socket.c:337-342)
    fn addr(&self, _conn: &Connection, _remote: bool) -> Result<(Vec<u8>, u16), RedisError> {
        // TODO(architect): anetFdToString(conn.fd, ip, ip_len, port, remote as i32)
        // calls getpeername / getsockname — unsafe raw-fd ops.
        Err(RedisError::runtime(
            b"connSocketAddr: anetFdToString not yet ported (Phase B)",
        ))
    }

    /// C: `connSocketIsLocal` (socket.c:344-350)
    ///
    /// Logic: call `addr(remote=true)` then check whether the IP string starts
    /// with `"127."` or equals `"::1"`.
    fn is_local(&self, _conn: &Connection) -> Option<bool> {
        // TODO(architect): depends on addr() via anetFdToString — both deferred.
        None
    }

    // ── Listener management ───────────────────────────────────────────────────

    /// C: `connSocketListen` (socket.c:352-354)
    fn listen(&self, _listener: &mut ConnListener) -> Result<(), RedisError> {
        // TODO(architect): listenToPort(listener) — bind/listen unsafe fd ops.
        Err(RedisError::runtime(
            b"connSocketListen: listenToPort not yet ported (Phase B)",
        ))
    }

    /// C: `connSocketCloseListener` (socket.c:356-367)
    fn close_listener(&self, listener: &mut ConnListener) {
        for j in 0..listener.count as usize {
            if listener.fd[j] == -1 {
                continue;
            }
            // TODO(architect): aeDeleteFileEvent(server.el, listener.fd[j], AE_READABLE)
            // TODO(architect): libc::close(listener.fd[j]) — unsafe raw-fd op.
            listener.fd[j] = -1;
        }
        listener.count = 0;
    }

    // ── Blocking sync I/O ─────────────────────────────────────────────────────
    //
    // PORT NOTE: The C source comments that these wrappers "should ideally be
    // refactored out in favor of pure async work."  They are retained here for
    // fidelity; syncio.c is phase: defer.

    /// C: `connSocketSyncWrite` (socket.c:393-399)
    fn sync_write(
        &self,
        _conn: &mut Connection,
        _data: &[u8],
        _timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        // TODO(architect): syncWrite(conn.fd, data.as_ptr(), data.len(), timeout_ms)
        // from syncio.c (phase: defer).
        Err(RedisError::runtime(
            b"connSocketSyncWrite: syncio not yet ported (Phase B)",
        ))
    }

    /// C: `connSocketSyncRead` (socket.c:401-407)
    fn sync_read(
        &self,
        _conn: &mut Connection,
        _buf: &mut [u8],
        _timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        // TODO(architect): syncRead(conn.fd, buf.as_mut_ptr(), buf.len(), timeout_ms)
        // from syncio.c (phase: defer).
        Err(RedisError::runtime(
            b"connSocketSyncRead: syncio not yet ported (Phase B)",
        ))
    }

    /// C: `connSocketSyncReadLine` (socket.c:409-415)
    fn sync_readline(
        &self,
        _conn: &mut Connection,
        _buf: &mut [u8],
        _timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        // TODO(architect): syncReadLine(conn.fd, buf.as_mut_ptr(), buf.len(), timeout_ms)
        // from syncio.c (phase: defer).
        Err(RedisError::runtime(
            b"connSocketSyncReadLine: syncio not yet ported (Phase B)",
        ))
    }
}

// ─── Event handler ────────────────────────────────────────────────────────────

/// AE event handler dispatched when the fd of a connected socket is readable
/// or writable.
///
/// Handles the `Connecting → Connected` state transition on the first writable
/// event, then dispatches to the registered read/write handlers respecting the
/// write-barrier flag.
///
/// C: `connSocketEventHandler` (socket.c:263-312)
///
/// TODO(port): the actual signature must match `aeFileProc` from ae.h (defer):
/// `fn(el: *mut AeEventLoop, fd: i32, client_data: *mut Connection, mask: i32)`.
/// The `el` and `fd` parameters are elided here; Phase B will add them when
/// event_loop.rs is integrated.
///
/// PORT NOTE: C computes `call_write` and `call_read` booleans before dispatching,
/// then passes `conn->write_handler` / `conn->read_handler` directly to
/// `callHandler`.  If the read handler clears the write handler in C, the pre-
/// computed `call_write` is still `true` and a NULL function pointer is called —
/// a potential bug.  The Rust translation re-checks the handler via `if let
/// Some(handler)` at dispatch time, which is strictly safer.
pub(crate) fn socket_event_handler(conn: &mut Connection, mask: i32) {
    // C: socket.c:268-281 — handle connection completion on first AE_WRITABLE.
    if conn.state == ConnectionState::Connecting
        && (mask & AE_WRITABLE) != 0
        && conn.conn_handler.is_some()
    {
        // TODO(architect): conn_error = anetGetError(conn.fd)
        //   → getsockopt(SOL_SOCKET, SO_ERROR) — unsafe raw-fd op.
        //
        // Logical translation (Phase B):
        //   let conn_error = anet_get_error(conn.fd);
        //   if conn_error != 0 {
        //       conn.last_errno = conn_error;
        //       conn.state = ConnectionState::Error;
        //   } else {
        //       conn.state = ConnectionState::Connected;
        //   }
        //
        // Phase A assumption: connect succeeded (no real fd to inspect).
        conn.state = ConnectionState::Connected;

        if conn.write_handler.is_none() {
            // TODO(architect): aeDeleteFileEvent(server.el, conn.fd, AE_WRITABLE)
        }

        // Take the connect-completion handler (sets conn.conn_handler = None, matching C's
        // `conn->conn_handler = NULL` after the call).
        if let Some(handler) = conn.conn_handler.take() {
            if !call_handler(conn, handler) {
                return;
            }
        }
    }

    // C: socket.c:294-311 — dispatch read/write handlers, respecting write barrier.
    let invert = (conn.flags & CONN_FLAG_WRITE_BARRIER) != 0;
    let call_write = (mask & AE_WRITABLE) != 0 && conn.write_handler.is_some();
    let call_read = (mask & AE_READABLE) != 0 && conn.read_handler.is_some();

    if !invert && call_read {
        if let Some(handler) = conn.read_handler {
            if !call_handler(conn, handler) {
                return;
            }
        }
    }
    if call_write {
        if let Some(handler) = conn.write_handler {
            if !call_handler(conn, handler) {
                return;
            }
        }
    }
    if invert && call_read {
        if let Some(handler) = conn.read_handler {
            if !call_handler(conn, handler) {
                return;
            }
        }
    }
}

// ─── Accept handler ───────────────────────────────────────────────────────────

/// AE event handler called when the TCP listening socket becomes readable.
///
/// Accepts up to `max_new_conns` new connections per event-loop iteration,
/// optionally enables TCP keepalive, and forwards each accepted fd to
/// `accept_common_handler` (to be supplied by `crate::networking`).
///
/// C: `connSocketAcceptHandler` (socket.c:314-335)
///
/// TODO(port): the actual signature must match `aeFileProc` from ae.h (defer):
/// `fn(el: *mut AeEventLoop, fd: i32, privdata: *mut (), mask: i32)`.
/// Phase B should also thread `RedisServer` by reference rather than relying
/// on a process global.
///
/// TODO(architect): needs anetTcpAccept, anetRetryAcceptOnError, anetKeepAlive
/// (all from anet.c: defer) and `crate::networking::accept_common_handler`
/// (networking.c: pilot but not yet integrated).
pub(crate) fn socket_accept_handler(listen_fd: i32, max_new_conns: i32, tcpkeepalive: i32) {
    let _ = (listen_fd, tcpkeepalive);
    let mut remaining = max_new_conns;
    while remaining > 0 {
        remaining -= 1;

        // TODO(architect): cfd = anetTcpAccept(neterr, listen_fd, cip, NET_IP_STR_LEN, &cport)
        //   if cfd == ANET_ERR (-1):
        //     if anetRetryAcceptOnError(errno): continue
        //     if errno != EWOULDBLOCK: log warning (serverLog(LL_WARNING, ...))
        //     return
        //   if tcpkeepalive != 0: anetKeepAlive(NULL, cfd, tcpkeepalive)
        //   let conn = SocketConnectionType.conn_create_accepted(cfd);
        //   crate::networking::accept_common_handler(conn, flags, cip)
        break;
    }
}

// ─── anet-style socket-option public wrappers ─────────────────────────────────
//
// C: `socket.c:470-495`.  These are the only non-static functions in the file
// besides `RedisRegisterConnectionTypeSocket`.  They wrap `anet*` helpers that
// set `fcntl`/`setsockopt` options on the raw file descriptor.

/// Set the socket to blocking mode.
///
/// C: `connBlock` (socket.c:470-473)
pub fn conn_block(conn: &mut Connection) -> Result<(), RedisError> {
    if conn.fd == -1 {
        return Err(RedisError::runtime(b"connBlock: fd not open"));
    }
    // TODO(architect): anetBlock(NULL, conn.fd) — anet.c is phase: defer.
    Err(RedisError::runtime(
        b"connBlock: anet not yet ported (Phase B)",
    ))
}

/// Set the socket to non-blocking mode.
///
/// C: `connNonBlock` (socket.c:475-478)
pub fn conn_non_block(conn: &mut Connection) -> Result<(), RedisError> {
    if conn.fd == -1 {
        return Err(RedisError::runtime(b"connNonBlock: fd not open"));
    }
    // TODO(architect): anetNonBlock(NULL, conn.fd) — anet.c is phase: defer.
    Err(RedisError::runtime(
        b"connNonBlock: anet not yet ported (Phase B)",
    ))
}

/// Enable TCP keepalive with the given idle interval in seconds.
///
/// C: `connKeepAlive` (socket.c:480-483)
pub fn conn_keep_alive(conn: &mut Connection, _interval: i32) -> Result<(), RedisError> {
    if conn.fd == -1 {
        return Err(RedisError::runtime(b"connKeepAlive: fd not open"));
    }
    // TODO(architect): anetKeepAlive(NULL, conn.fd, interval) — anet.c is phase: defer.
    Err(RedisError::runtime(
        b"connKeepAlive: anet not yet ported (Phase B)",
    ))
}

/// Set the send timeout via `SO_SNDTIMEO`.
///
/// C: `connSendTimeout` (socket.c:485-487)
pub fn conn_send_timeout(_conn: &mut Connection, _ms: i64) -> Result<(), RedisError> {
    // TODO(architect): anetSendTimeout(NULL, conn.fd, ms) — anet.c is phase: defer.
    Err(RedisError::runtime(
        b"connSendTimeout: anet not yet ported (Phase B)",
    ))
}

/// Set the receive timeout via `SO_RCVTIMEO`.
///
/// C: `connRecvTimeout` (socket.c:489-491)
pub fn conn_recv_timeout(_conn: &mut Connection, _ms: i64) -> Result<(), RedisError> {
    // TODO(architect): anetRecvTimeout(NULL, conn.fd, ms) — anet.c is phase: defer.
    Err(RedisError::runtime(
        b"connRecvTimeout: anet not yet ported (Phase B)",
    ))
}

// ─── Registration ─────────────────────────────────────────────────────────────

/// Register the TCP socket backend in the global connection-type registry.
///
/// Calls `SocketConnectionType::init()` (a no-op, matching `CT_Socket.init = NULL`)
/// via `conn_type_register`.  Must be called during server startup before any
/// connection is created.
///
/// C: `RedisRegisterConnectionTypeSocket` (socket.c:493-495)
pub fn register_connection_type_socket() -> Result<(), RedisError> {
    conn_type_register(Box::new(SocketConnectionType))
}

// ──────────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/socket.c  (496 lines, 23 functions)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         36
//   port_notes:    4
//   unsafe_blocks: 0
//   notes:         CT_Socket vtable → SocketConnectionType implementing
//                  ConnectionTypeTrait; all POSIX raw-fd syscalls and anet*
//                  helpers stubbed with TODO(architect) pending libc addition;
//                  event-loop integration (aeCreateFileEvent etc.) stubbed
//                  pending ae.c port (phase: defer); call_handler ref-counting
//                  faithfully translated; write-barrier and read/write handler
//                  dispatch logic fully preserved.
// ──────────────────────────────────────────────────────────────────────────────
