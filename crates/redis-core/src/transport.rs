//! Runtime transport: the live `Connection` enum used by the redis-server binary.
//! Wave A pilot abstraction. Owns a real OS handle (currently `TcpStream` for
//! plain TCP and `rustls::StreamOwned<ServerConnection, TcpStream>` for TLS)
//! and presents a small read/write/close API to the event loop
//! `redis-server::main`.
//! # Why this is separate from `connection.rs`
//! `connection.rs` is the C-faithful port of /
//! (vtable-based registry, `ConnectionTypeTrait`, file descriptors). It is
//! intended to eventually back this module's variants but is not yet wired
//! to a real backend (the registry has no implementations registered).
//! The Wave A pilot needs a synchronous, working TCP transport *now* so
//! binary can accept connections. This module provides that minimal surface
//! and is the type referenced by `Client::with_connection`.
//! TODO(architect): collapse this with `connection.rs` once concrete
//! `ConnectionTypeTrait` backends (socket / unix / tls) land in Phase 5.
//! Until then the two coexist: `connection::Connection` is the C-ported
//! struct with `fd: i32`; `transport::Connection` is the live runtime enum.

use std::io;
use std::net::{SocketAddr, TcpStream};

use rustls::StreamOwned;

/// Live connection used by the running redis-server binary.
/// `Tcp` is the plain blocking TCP variant used by default. `Tls` wraps a
/// `rustls::StreamOwned<ServerConnection, TcpStream>` behind a `Box` to keep
/// the enum size constant regardless of the rustls state machine size.
pub enum Connection {
 /// Plain blocking TCP connection.
    Tcp(TcpStream),
 /// TLS connection backed by rustls. The inner stream owns both the rustls
 /// `ServerConnection` state machine and the underlying `TcpStream`.
    Tls(Box<StreamOwned<rustls::ServerConnection, TcpStream>>),
}

impl Connection {
 /// Read up to `buf.len` bytes from the connection.
 /// Returns the number of bytes read; `0` indicates EOF.
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        use io::Read;
        match self {
            Connection::Tcp(s) => s.read(buf),
            Connection::Tls(s) => s.read(buf),
        }
    }

 /// Write the entire buffer to the connection, retrying on short writes.
    pub fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        use io::Write;
        match self {
            Connection::Tcp(s) => s.write_all(buf),
            Connection::Tls(s) => s.write_all(buf),
        }
    }

 /// Close the connection by dropping the underlying handle.
 /// Equivalent to `std::mem::drop(self)`; provided so call sites can be
 /// explicit about intent.
    pub fn close(self) {
        drop(self);
    }

 /// Return the peer address (remote end) of the connection.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Connection::Tcp(s) => s.peer_addr(),
            Connection::Tls(s) => s.get_ref().peer_addr(),
        }
    }

 /// Attempt to clone the underlying `TcpStream` for use by the writer thread.
 /// For `Tls` connections the writer thread cannot share the rustls state
 /// machine, so `None` is returned — the caller must use a different
 /// write-path arrangement (e.g., channel-only writes through the read loop).
    pub fn try_clone_tcp(&self) -> Option<TcpStream> {
        match self {
            Connection::Tcp(s) => s.try_clone().ok(),
            Connection::Tls(_) => None,
        }
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Connection::Tcp(s) => match s.peer_addr() {
                Ok(addr) => write!(f, "Connection::Tcp({})", addr),
                Err(_) => write!(f, "Connection::Tcp(<closed>)"),
            },
            Connection::Tls(s) => match s.get_ref().peer_addr() {
                Ok(addr) => write!(f, "Connection::Tls({})", addr),
                Err(_) => write!(f, "Connection::Tls(<closed>)"),
            },
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Session 2B (TLS support) — extends Wave A TCP transport
//   target_crate:  redis-core
//   confidence:    high
//   todos:         1
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Adds Tls variant backed by rustls::StreamOwned. The Box
//                  keeps the enum size from ballooning. try_clone_tcp() lets
//                  main.rs decide the write-path per connection type.
// ──────────────────────────────────────────────────────────────────────────
