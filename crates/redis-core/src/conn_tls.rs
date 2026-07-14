//! TLS connection-type backend (rustls).
//! Structural port of 's `CT_TLS` vtable to `ConnectionTypeTrait`, with
//! crypto reimplemented on rustls (no OpenSSL). The mapping:
//! | (OpenSSL) | here (rustls) |
//! |---------------------------|----------------------------------------|
//! | `SSL_accept` | drive `read_tls`/`process_new_packets`/`write_tls` while `is_handshaking` |
//! | `SSL_read` | `read_tls` → `process_new_packets` → `reader.read` |
//! | `SSL_write` | `writer.write` → `write_tls` |
//! | `WANT_READ`/`WANT_WRITE` flags + `updateSSLEvent` | `wants_read` / `wants_write` recomputed each pump |
//! OpenSSL flag bookkeeping disappears: rustls owns its plaintext/ciphertext
//! buffers and exposes the desired interest directly.

use std::io::{self, IoSlice, Read};
use std::sync::Arc;

use redis_types::RedisError;

use crate::connection::{
    ConnIo, ConnIoSlot, ConnListener, Connection, ConnectionCallbackFunc, ConnectionState,
    ConnectionTypeId, ConnectionTypeTrait, TlsIo,
};
use crate::tls::TlsConfig;

/// The TLS backend. Holds the shared `rustls::ServerConfig` (backend-global,
/// like C's `valkey_tls_ctx`); per-connection session state lives on
/// `Connection`'s `ConnIoSlot::Tls`.
pub struct TlsConnectionType {
    server_config: Arc<rustls::ServerConfig>,
}

impl TlsConnectionType {
 /// Build from the project's `TlsConfig` (PEM-loaded cert/key/CA).
    pub fn new(config: TlsConfig) -> Self {
        Self {
            server_config: config.server_config,
        }
    }

 /// Build directly from a rustls server config (used by tests and the owner
 /// loop once a config is in hand).
    pub fn from_server_config(server_config: Arc<rustls::ServerConfig>) -> Self {
        Self { server_config }
    }

 /// Create a server-side accepted TLS connection over `io`.
 /// (or a test) supplies the freshly-accepted ciphertext transport.
    pub fn accept_connection(&self, io: Box<dyn ConnIo>) -> Result<Connection, RedisError> {
        let mut session =
            rustls::ServerConnection::new(Arc::clone(&self.server_config)).map_err(|e| {
                RedisError::runtime(format!("tls ServerConnection::new: {e}").into_bytes())
            })?;
 // Unlimit rustls' internal plaintext buffer — the application layer
 // already bounds incoming data via `client-query-buffer-limit` (1 GB by
 // default). With rustls' default ~64 KB ceiling, `read_tls` errors with
 // "received plaintext buffer full" on any client write large enough
 // produce more plaintext than the session can hold between drains.
        session.set_buffer_limit(None);
        let mut conn = Connection::new(ConnectionTypeId::Tls, -1);
        conn.state = ConnectionState::Accepting;
        conn.io = ConnIoSlot::Tls(Box::new(TlsIo {
            io,
            session: Box::new(session),
        }));
        Ok(conn)
    }
}

fn would_block(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::WouldBlock
}

/// Pull available ciphertext from `stream` into `session` and process it.
/// Returns `Ok(true)` if the transport hit EOF. Generic over the byte stream so
/// the server's owner loop (which owns the `mio::TcpStream`) reuses the
/// same, harness-tested pump as the backend.
pub fn session_read_pump<S: io::Read>(
    session: &mut rustls::ServerConnection,
    stream: &mut S,
) -> Result<bool, RedisError> {
    loop {
        match session.read_tls(stream) {
            Ok(0) => return Ok(true),
            Ok(_) => {
                session
                    .process_new_packets()
                    .map_err(|_| RedisError::io(io::ErrorKind::InvalidData))?;
            }
            Err(e) if would_block(&e) => return Ok(false),
            Err(e) => return Err(RedisError::io(e.kind())),
        }
    }
}

/// Flush pending ciphertext from `session` to `stream`.
pub fn session_write_pump<S: io::Write>(
    session: &mut rustls::ServerConnection,
    stream: &mut S,
) -> Result<(), RedisError> {
    while session.wants_write() {
        match session.write_tls(stream) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) if would_block(&e) => break,
            Err(e) => return Err(RedisError::io(e.kind())),
        }
    }
    Ok(())
}

fn pump_incoming(tls: &mut TlsIo) -> Result<bool, RedisError> {
    session_read_pump(&mut tls.session, &mut tls.io)
}

fn pump_outgoing(tls: &mut TlsIo) -> Result<(), RedisError> {
    session_write_pump(&mut tls.session, &mut tls.io)
}

impl ConnectionTypeTrait for TlsConnectionType {
    fn get_type_id(&self) -> ConnectionTypeId {
        ConnectionTypeId::Tls
    }

    fn configure(&mut self, _reconfigure: bool) -> Result<(), RedisError> {
        Ok(())
    }

 /// advance the handshake as far as the currently
 /// available ciphertext allows. If complete, fire the accept handler;
 /// otherwise return (the event loop re-drives on the next readiness).
    fn accept(
        &self,
        conn: &mut Connection,
        accept_handler: ConnectionCallbackFunc,
    ) -> Result<(), RedisError> {
        if conn.state != ConnectionState::Accepting {
            return Err(RedisError::runtime(
                b"connTLSAccept: not in accepting state",
            ));
        }
        let tls = conn
            .io
            .as_tls_mut()
            .ok_or_else(|| RedisError::runtime(b"connTLSAccept: no tls session"))?;

        pump_outgoing(tls)?;
        let eof = pump_incoming(tls)?;
        pump_outgoing(tls)?;

        if tls.session.is_handshaking() {
            if eof {
                conn.state = ConnectionState::Error;
                return Err(RedisError::io(io::ErrorKind::UnexpectedEof));
            }
 // Still negotiating; caller re-drives.
            return Ok(());
        }

        conn.state = ConnectionState::Connected;
        accept_handler(conn);
        Ok(())
    }

    fn read(&self, conn: &mut Connection, buf: &mut [u8]) -> Result<usize, RedisError> {
        let tls = conn
            .io
            .as_tls_mut()
            .ok_or_else(|| RedisError::runtime(b"connTLSRead: no tls session"))?;
        let eof = pump_incoming(tls)?;
        pump_outgoing(tls)?;

        match tls.session.reader().read(buf) {
            Ok(0) => {
                conn.state = ConnectionState::Closed;
                Ok(0)
            }
            Ok(n) => Ok(n),
            Err(e) if would_block(&e) => {
                if eof {
                    conn.state = ConnectionState::Error;
                    Err(RedisError::io(io::ErrorKind::UnexpectedEof))
                } else {
                    Err(RedisError::io(io::ErrorKind::WouldBlock))
                }
            }
            Err(e) => {
                conn.state = ConnectionState::Error;
                Err(RedisError::io(e.kind()))
            }
        }
    }

    fn write(&self, conn: &mut Connection, data: &[u8]) -> Result<usize, RedisError> {
        use std::io::Write;
        let tls = conn
            .io
            .as_tls_mut()
            .ok_or_else(|| RedisError::runtime(b"connTLSWrite: no tls session"))?;
        let n = match tls.session.writer().write(data) {
            Ok(n) => n,
            Err(e) if would_block(&e) => 0,
            Err(e) => return Err(RedisError::io(e.kind())),
        };
        pump_outgoing(tls)?;
        if n == 0 {
            return Err(RedisError::io(io::ErrorKind::WouldBlock));
        }
        Ok(n)
    }

    fn writev(&self, conn: &mut Connection, iov: &[IoSlice<'_>]) -> Result<usize, RedisError> {
        use std::io::Write;
        let tls = conn
            .io
            .as_tls_mut()
            .ok_or_else(|| RedisError::runtime(b"connTLSWritev: no tls session"))?;
        let n = match tls.session.writer().write_vectored(iov) {
            Ok(n) => n,
            Err(e) if would_block(&e) => 0,
            Err(e) => return Err(RedisError::io(e.kind())),
        };
        pump_outgoing(tls)?;
        if n == 0 {
            return Err(RedisError::io(io::ErrorKind::WouldBlock));
        }
        Ok(n)
    }

    fn set_read_handler(
        &self,
        conn: &mut Connection,
        handler: Option<ConnectionCallbackFunc>,
    ) -> Result<(), RedisError> {
        conn.read_handler = handler;
        Ok(())
    }

    fn set_write_handler(
        &self,
        conn: &mut Connection,
        handler: Option<ConnectionCallbackFunc>,
        barrier: bool,
    ) -> Result<(), RedisError> {
        conn.write_handler = handler;
        if barrier {
            conn.flags |= crate::connection::CONN_FLAG_WRITE_BARRIER;
        } else {
            conn.flags &= !crate::connection::CONN_FLAG_WRITE_BARRIER;
        }
        Ok(())
    }

    fn get_last_error(&self, conn: &Connection) -> Option<Vec<u8>> {
        if conn.last_errno == 0 {
            None
        } else {
            Some(format!("tls errno={}", conn.last_errno).into_bytes())
        }
    }

    fn conn_create_accepted(&self, fd: i32) -> Connection {
        let mut conn = Connection::new(ConnectionTypeId::Tls, fd);
        conn.state = ConnectionState::Accepting;
        conn
    }

 /// send `close_notify`, flush, drop.
    fn close(&self, conn: &mut Connection) {
        if let Some(tls) = conn.io.as_tls_mut() {
            tls.session.send_close_notify();
            let _ = pump_outgoing(tls);
        }
        conn.io = ConnIoSlot::None;
        conn.state = ConnectionState::Closed;
    }

    fn shutdown(&self, conn: &mut Connection) {
        self.close(conn);
    }

 // ── deferred / owner-loop-owned (per docs/TLS_FAITHFUL_PLAN.md) ──

    fn conn_create(&self) -> Connection {
        Connection::new(ConnectionTypeId::Tls, -1)
    }

    fn addr(&self, _conn: &Connection, _remote: bool) -> Result<(Vec<u8>, u16), RedisError> {
        Err(RedisError::runtime(
            b"connTLSAddr: tracked by the owner loop",
        ))
    }

    fn is_local(&self, _conn: &Connection) -> Option<bool> {
        None
    }

    fn listen(&self, _listener: &mut ConnListener) -> Result<(), RedisError> {
        Err(RedisError::runtime(
            b"connTLSListen: owned by the runtime owner loop (Phase 2)",
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
            b"connTLSConnect: outbound TLS deferred",
        ))
    }

    fn blocking_connect(
        &self,
        _conn: &mut Connection,
        _addr: &[u8],
        _port: u16,
        _timeout_ms: i64,
    ) -> Result<(), RedisError> {
        Err(RedisError::runtime(b"connTLSBlockingConnect: deferred"))
    }

    fn sync_write(
        &self,
        _conn: &mut Connection,
        _data: &[u8],
        _timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        Err(RedisError::runtime(b"connTLSSyncWrite: deferred"))
    }

    fn sync_read(
        &self,
        _conn: &mut Connection,
        _buf: &mut [u8],
        _timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        Err(RedisError::runtime(b"connTLSSyncRead: deferred"))
    }

    fn sync_readline(
        &self,
        _conn: &mut Connection,
        _buf: &mut [u8],
        _timeout_ms: i64,
    ) -> Result<isize, RedisError> {
        Err(RedisError::runtime(b"connTLSSyncReadLine: deferred"))
    }
}

/// Port of `tlsEventHandler` — the `ae_handler` for TLS. Advance the record
/// layer with whatever the transport delivered, then run the read/write
/// handlers with the same ordering/barrier semantics as the socket backend.
pub fn tls_event_handler(conn: &mut Connection, readable: bool, writable: bool) {
    if let Some(tls) = conn.io.as_tls_mut() {
        let _ = pump_incoming(tls);
        let _ = pump_outgoing(tls);
    }
    crate::conn_socket::socket_event_handler(conn, readable, writable);
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Valkey
//   target_crate:  redis-core
//   confidence:    high (handshake/read/write/close); stubs for outbound/listen/sync
//   todos:         outbound connect, mTLS peer-cert accessor, listen, sync_* (Phase 2+)
//   port_notes:    OpenSSL → rustls; WANT_READ/WANT_WRITE flag bookkeeping →
//                  wants_read()/wants_write(); fd → owned ConnIo transport
//   unsafe_blocks: 0
//   notes:         non-blocking handshake + record I/O over ConnIoSlot::Tls;
//                  proven against a raw rustls client in tests/conn_transport_kit.rs
// ──────────────────────────────────────────────────────────────────────────
