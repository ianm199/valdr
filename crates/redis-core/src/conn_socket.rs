//! Plain-TCP connection-type backend.
//!
//! Port of `socket.c`'s `CT_Socket` vtable to `ConnectionTypeTrait`. Translates
//! the structure faithfully; the one representation change is that I/O goes
//! through the connection's owned stream (`Connection::io`, a `dyn ConnIo`)
//! instead of a raw `fd` + libc `read`/`write` — the safe-Rust handle model
//! decided in `connection.rs`. The event-loop registration that C did via
//! `aeCreateFileEvent`/`aeDeleteFileEvent` becomes interest derived from
//! handler presence (`connHasReadHandler`/`connHasWriteHandler`), which the
//! owner loop reads to drive `mio` — so this backend stays event-loop-agnostic.
//!
//! Out of scope here (deferred per docs/TLS_FAITHFUL_PLAN.md): outbound
//! `connect`/`blocking_connect`, `listen`/`close_listener` (the owner loop owns
//! binding/accepting), and the `sync_*` blocking helpers (RDB/repl transfer).

use std::io::{IoSlice, Read, Write};

use redis_types::RedisError;

use crate::connection::{
    ConnListener, Connection, ConnectionCallbackFunc, ConnectionState, ConnectionTypeId,
    ConnectionTypeTrait, CONN_FLAG_WRITE_BARRIER,
};

/// The plain-TCP backend. Stateless: per-connection state lives on `Connection`.
///
/// C: `static ConnectionType CT_Socket` in socket.c.
#[derive(Debug, Default)]
pub struct SocketConnectionType;

impl SocketConnectionType {
    pub fn new() -> Self {
        SocketConnectionType
    }
}

/// Should a transport error transition a CONNECTED connection to ERROR?
///
/// C: `if (errno != EINTR && conn->state == CONN_STATE_CONNECTED) state = ERROR`.
/// `WouldBlock` (EAGAIN) and `Interrupted` (EINTR) are transient — never fatal.
fn is_transient(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
    )
}

impl ConnectionTypeTrait for SocketConnectionType {
    fn get_type_id(&self) -> ConnectionTypeId {
        ConnectionTypeId::Socket
    }

    fn configure(&mut self, _reconfigure: bool) -> Result<(), RedisError> {
        Ok(())
    }

    /// C: `connSocketRead`. EOF (`0`) → state CLOSED; transient errors are
    /// returned as `Io(WouldBlock/Interrupted)` without changing state.
    fn read(&self, conn: &mut Connection, buf: &mut [u8]) -> Result<usize, RedisError> {
        let io = conn
            .io
            .as_io_mut()
            .ok_or_else(|| RedisError::runtime(b"connSocketRead: no stream attached"))?;
        match io.read(buf) {
            Ok(0) => {
                conn.state = ConnectionState::Closed;
                Ok(0)
            }
            Ok(n) => Ok(n),
            Err(e) => {
                let kind = e.kind();
                if !is_transient(kind) && conn.state == ConnectionState::Connected {
                    conn.state = ConnectionState::Error;
                }
                Err(RedisError::io(kind))
            }
        }
    }

    /// C: `connSocketWrite`.
    fn write(&self, conn: &mut Connection, data: &[u8]) -> Result<usize, RedisError> {
        let io = conn
            .io
            .as_io_mut()
            .ok_or_else(|| RedisError::runtime(b"connSocketWrite: no stream attached"))?;
        match io.write(data) {
            Ok(n) => Ok(n),
            Err(e) => {
                let kind = e.kind();
                if !is_transient(kind) && conn.state == ConnectionState::Connected {
                    conn.state = ConnectionState::Error;
                }
                Err(RedisError::io(kind))
            }
        }
    }

    /// C: `connSocketWritev`.
    fn writev(&self, conn: &mut Connection, iov: &[IoSlice<'_>]) -> Result<usize, RedisError> {
        let io = conn
            .io
            .as_io_mut()
            .ok_or_else(|| RedisError::runtime(b"connSocketWritev: no stream attached"))?;
        match io.write_vectored(iov) {
            Ok(n) => Ok(n),
            Err(e) => {
                let kind = e.kind();
                if !is_transient(kind) && conn.state == ConnectionState::Connected {
                    conn.state = ConnectionState::Error;
                }
                Err(RedisError::io(kind))
            }
        }
    }

    /// C: `connSocketAccept`. Transitions ACCEPTING → CONNECTED and fires the
    /// accept handler.
    fn accept(
        &self,
        conn: &mut Connection,
        accept_handler: ConnectionCallbackFunc,
    ) -> Result<(), RedisError> {
        if conn.state != ConnectionState::Accepting {
            return Err(RedisError::runtime(b"connSocketAccept: not in accepting state"));
        }
        conn.state = ConnectionState::Connected;
        accept_handler(conn);
        Ok(())
    }

    /// C: `connSocketSetReadHandler`. The `aeCreateFileEvent`/`aeDeleteFileEvent`
    /// step is the owner loop's job — it derives interest from handler presence.
    fn set_read_handler(
        &self,
        conn: &mut Connection,
        handler: Option<ConnectionCallbackFunc>,
    ) -> Result<(), RedisError> {
        conn.read_handler = handler;
        Ok(())
    }

    /// C: `connSocketSetWriteHandler` — sets/clears the write barrier flag.
    fn set_write_handler(
        &self,
        conn: &mut Connection,
        handler: Option<ConnectionCallbackFunc>,
        barrier: bool,
    ) -> Result<(), RedisError> {
        conn.write_handler = handler;
        if barrier {
            conn.flags |= CONN_FLAG_WRITE_BARRIER;
        } else {
            conn.flags &= !CONN_FLAG_WRITE_BARRIER;
        }
        Ok(())
    }

    /// C: `connSocketGetLastError` (`strerror(conn->last_errno)`).
    fn get_last_error(&self, conn: &Connection) -> Option<Vec<u8>> {
        if conn.last_errno == 0 {
            None
        } else {
            Some(format!("errno={}", conn.last_errno).into_bytes())
        }
    }

    fn conn_create(&self) -> Connection {
        Connection::new(ConnectionTypeId::Socket, -1)
    }

    fn conn_create_accepted(&self, fd: i32) -> Connection {
        let mut conn = Connection::new(ConnectionTypeId::Socket, fd);
        conn.state = ConnectionState::Accepting;
        conn
    }

    /// C: `connSocketClose` — drop the owned stream and mark closed.
    fn close(&self, conn: &mut Connection) {
        conn.io = crate::connection::ConnIoSlot::None;
        conn.state = ConnectionState::Closed;
    }

    /// C: `connSocketShutdown` (`shutdown(fd, SHUT_RDWR)`). A half-shutdown on an
    /// owned stream is not separable here; treat as close for now.
    fn shutdown(&self, conn: &mut Connection) {
        self.close(conn);
    }

    fn addr(&self, _conn: &Connection, _remote: bool) -> Result<(Vec<u8>, u16), RedisError> {
        Err(RedisError::runtime(
            b"connSocketAddr: peer address is tracked by the owner loop, not this backend",
        ))
    }

    fn is_local(&self, _conn: &Connection) -> Option<bool> {
        None
    }

    fn listen(&self, _listener: &mut ConnListener) -> Result<(), RedisError> {
        Err(RedisError::runtime(
            b"connSocketListen: binding is owned by the runtime owner loop (Phase 2)",
        ))
    }

    fn close_listener(&self, _listener: &mut ConnListener) {}

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
            b"connSocketConnect: outbound connect deferred (server-inbound only)",
        ))
    }

    fn blocking_connect(
        &self,
        _conn: &mut Connection,
        _addr: &[u8],
        _port: u16,
        _timeout_ms: i64,
    ) -> Result<(), RedisError> {
        Err(RedisError::runtime(
            b"connSocketBlockingConnect: outbound connect deferred",
        ))
    }

    fn sync_write(
        &self,
        _conn: &mut Connection,
        _data: &[u8],
        _timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        Err(RedisError::runtime(b"connSocketSyncWrite: deferred"))
    }

    fn sync_read(
        &self,
        _conn: &mut Connection,
        _buf: &mut [u8],
        _timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        Err(RedisError::runtime(b"connSocketSyncRead: deferred"))
    }

    fn sync_readline(
        &self,
        _conn: &mut Connection,
        _buf: &mut [u8],
        _timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        Err(RedisError::runtime(b"connSocketSyncReadLine: deferred"))
    }
}

/// Port of `connSocketEventHandler` — the `ae_handler`. Given a readiness
/// notification (the owner loop translates a `mio` event into `readable` /
/// `writable`), fire the connect/read/write handlers in the right order.
///
/// Mirrors the upstream ordering: read before write normally, inverted under
/// `CONN_FLAG_WRITE_BARRIER`.
pub fn socket_event_handler(conn: &mut Connection, readable: bool, writable: bool) {
    // Outbound connect completion. We only do inbound today, but the shape is
    // faithful so the path exists when `connect` lands.
    if conn.state == ConnectionState::Connecting && writable {
        if let Some(handler) = conn.conn_handler.take() {
            conn.state = ConnectionState::Connected;
            handler(conn);
            return;
        }
    }

    let invert = conn.flags & CONN_FLAG_WRITE_BARRIER != 0;
    let call_write = writable && conn.write_handler.is_some();
    let call_read = readable && conn.read_handler.is_some();

    if !invert && call_read {
        if let Some(handler) = conn.read_handler {
            handler(conn);
        }
    }
    if call_write {
        if let Some(handler) = conn.write_handler {
            handler(conn);
        }
    }
    if invert && call_read {
        if let Some(handler) = conn.read_handler {
            handler(conn);
        }
    }
}

/// Register the socket backend in the global connection-type registry.
///
/// C: `RedisRegisterConnectionTypeSocket` in socket.c.
pub fn register_socket_connection_type() -> Result<(), RedisError> {
    crate::connection::conn_type_register(Box::new(SocketConnectionType::new()))
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        reference/valkey/src/socket.c (CT_Socket vtable)
//   target_crate:  redis-core
//   confidence:    high (I/O methods); stubs for outbound/listen/sync (deferred)
//   todos:         outbound connect, listen/close_listener, sync_* (Phase 2+)
//   port_notes:    fd+libc I/O → owned `dyn ConnIo` stream; ae event registration
//                  → owner-loop interest derived from handler presence
//   unsafe_blocks: 0
//   notes:         read/write/writev/accept/set_*_handler + socket_event_handler
//                  faithfully translated; proven via tests/conn_transport_kit.rs
// ──────────────────────────────────────────────────────────────────────────
