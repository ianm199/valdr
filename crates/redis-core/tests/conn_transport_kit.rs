//! Fast in-memory iteration harness for the connection / TLS layer.
//! Proves a non-blocking TLS state machine *deterministically* with no sockets,
//! no tclsh, and no server process — the fast loop described
//! `docs/TLS_FAITHFUL_PLAN.md` §11.
//! The key tool is `TestPipe`: a scriptable in-memory non-blocking duplex you
//! control byte-for-byte (deliver N bytes then `WouldBlock`, force short
//! writes). The classic TLS-over-event-loop bugs — a record split across
//! readiness events, WANT_WRITE during a read — reproduce here instantly
//! deterministically, where a real socket reproduces them unreliably.
//! Run just this loop:
//! cargo test -p redis-core --test conn_transport_kit

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConnection, ServerConnection};

use redis_core::conn_socket::{socket_event_handler, SocketConnectionType};
use redis_core::conn_tls::TlsConnectionType;
use redis_core::connection::{Connection, ConnectionState, ConnectionTypeId, ConnectionTypeTrait};
use redis_types::RedisError;

// ─── TestPipe ────────────────────────────────────────────────────────────────

type SharedBuf = Arc<Mutex<VecDeque<u8>>>;

/// One end of an in-memory non-blocking duplex. Reads from `inbound`, writes
/// `outbound`. `read_chunk` caps bytes returned per `read` (simulating record
/// fragmentation); `write_chunk` caps bytes accepted per `write` (simulating
/// short/partial socket writes). An empty inbound yields `WouldBlock`, exactly
/// like a non-blocking socket with no data ready.
struct PipeEnd {
    inbound: SharedBuf,
    outbound: SharedBuf,
    read_chunk: usize,
    write_chunk: usize,
}

impl Read for PipeEnd {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut q = self.inbound.lock().unwrap();
        if q.is_empty() {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "no data ready"));
        }
        let n = buf.len().min(self.read_chunk).min(q.len());
        for slot in buf.iter_mut().take(n) {
            *slot = q.pop_front().unwrap();
        }
        Ok(n)
    }
}

impl Write for PipeEnd {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = buf.len().min(self.write_chunk);
        self.outbound.lock().unwrap().extend(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Create a cross-connected `(server_end, client_end)` duplex pair. `read_chunk`
/// and `write_chunk` (>= 1) shape the fragmentation both ends.
fn pipe_pair(read_chunk: usize, write_chunk: usize) -> (PipeEnd, PipeEnd) {
    assert!(read_chunk >= 1 && write_chunk >= 1);
    let s2c: SharedBuf = Arc::new(Mutex::new(VecDeque::new()));
    let c2s: SharedBuf = Arc::new(Mutex::new(VecDeque::new()));
    let server = PipeEnd {
        inbound: c2s.clone(),
        outbound: s2c.clone(),
        read_chunk,
        write_chunk,
    };
    let client = PipeEnd {
        inbound: s2c,
        outbound: c2s,
        read_chunk,
        write_chunk,
    };
    (server, client)
}

// ─── rustls in-memory setup ──────────────────────────────────────────────────

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn load_certs() -> Vec<CertificateDer<'static>> {
    let pem: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/test-cert.pem"
    ));
    rustls_pemfile::certs(&mut &pem[..])
        .collect::<Result<Vec<_>, _>>()
        .expect("parse test cert")
}

fn load_key() -> PrivateKeyDer<'static> {
    let pem: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/test-key.pem"
    ));
    rustls_pemfile::private_key(&mut &pem[..])
        .expect("read test key")
        .expect("test key present")
}

/// The test CA cert the client trusts (the server leaf chains to it).
fn load_ca() -> Vec<CertificateDer<'static>> {
    let pem: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/ca-cert.pem"
    ));
    rustls_pemfile::certs(&mut &pem[..])
        .collect::<Result<Vec<_>, _>>()
        .expect("parse test CA cert")
}

/// Server config (cert/key) the TLS backend and a raw test server share.
fn test_server_config() -> Arc<rustls::ServerConfig> {
    Arc::new(
        rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(load_certs(), load_key())
            .unwrap(),
    )
}

/// A raw rustls client that trusts the test CA — the peer in every TLS test.
fn test_client() -> ClientConnection {
    let mut roots = rustls::RootCertStore::empty();
    for cert in load_ca() {
        roots.add(cert).unwrap();
    }
    let client_cfg = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = ServerName::try_from("localhost").unwrap().to_owned();
    ClientConnection::new(Arc::new(client_cfg), name).unwrap()
}

/// A fresh, fully-independent (server, client) raw rustls connection pair.
fn fresh() -> (ServerConnection, ClientConnection) {
    let server = ServerConnection::new(test_server_config()).unwrap();
    (server, test_client())
}

// ─── the drive pump ──────────────────────────────────────────────────────────

fn would_block(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::WouldBlock
}

fn to_io(e: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// Flush all pending ciphertext out of `conn` onto `end`. Returns whether any
/// bytes moved.
fn flush_tls(conn: &mut impl Pump, end: &mut PipeEnd) -> io::Result<bool> {
    let mut moved = false;
    while conn.p_wants_write() {
        match conn.p_write_tls(end) {
            Ok(0) => break,
            Ok(_) => moved = true,
            Err(e) if would_block(&e) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(moved)
}

/// Read all available ciphertext from `end` into `conn` and process it. Returns
/// whether any bytes moved.
fn feed_tls(conn: &mut impl Pump, end: &mut PipeEnd) -> io::Result<bool> {
    let mut moved = false;
    loop {
        match conn.p_read_tls(end) {
            Ok(0) => break,
            Ok(_) => moved = true,
            Err(e) if would_block(&e) => break,
            Err(e) => return Err(e),
        }
    }
    if moved {
        conn.p_process().map_err(to_io)?;
    }
    Ok(moved)
}

/// Pump both connections to quiescence: flush ciphertext each way, feed it,
/// process, repeat until no progress. Bounded for safety.
fn drive(
    server: &mut ServerConnection,
    server_end: &mut PipeEnd,
    client: &mut ClientConnection,
    client_end: &mut PipeEnd,
) -> io::Result<()> {
    for _ in 0..100_000 {
        let mut progress = false;
        progress |= flush_tls(client, client_end)?;
        progress |= flush_tls(server, server_end)?;
        progress |= feed_tls(server, server_end)?;
        progress |= feed_tls(client, client_end)?;
        if !progress {
            return Ok(());
        }
    }
    panic!("drive() did not reach quiescence — likely a stuck handshake");
}

/// Minimal trait so the pump is generic over server/client connections without
/// caring which side it is. The `p_`-prefixed names deliberately differ
/// rustls's inherent methods so the impls call the real rustls methods rather
/// than recursing into themselves.
trait Pump {
    fn p_wants_write(&self) -> bool;
    fn p_write_tls(&mut self, wr: &mut dyn Write) -> io::Result<usize>;
    fn p_read_tls(&mut self, rd: &mut dyn Read) -> io::Result<usize>;
    fn p_process(&mut self) -> Result<rustls::IoState, rustls::Error>;
}

macro_rules! impl_pump {
    ($t:ty) => {
        impl Pump for $t {
            fn p_wants_write(&self) -> bool {
                self.wants_write()
            }
            fn p_write_tls(&mut self, wr: &mut dyn Write) -> io::Result<usize> {
                self.write_tls(wr)
            }
            fn p_read_tls(&mut self, rd: &mut dyn Read) -> io::Result<usize> {
                self.read_tls(rd)
            }
            fn p_process(&mut self) -> Result<rustls::IoState, rustls::Error> {
                self.process_new_packets()
            }
        }
    };
}
impl_pump!(ServerConnection);
impl_pump!(ClientConnection);

/// Read all plaintext currently available from a rustls connection's reader.
fn drain_plaintext(mut reader: impl Read) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) if would_block(&e) => break,
            Err(_) => break,
        }
    }
    out
}

// ─── tests: the TestPipe itself ──────────────────────────────────────────────

#[test]
fn testpipe_fragments_reads_and_signals_wouldblock() {
    let (mut server, mut client) = pipe_pair(/* read_chunk */ 2, /* write_chunk */ 100);
    let mut buf = [0u8; 8];

 // Empty inbound reads as WouldBlock, never as EOF.
    assert_eq!(
        server.read(&mut buf).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );

 // Client writes 5 bytes; server sees them in 2-byte fragments.
    client.write_all(b"hello").unwrap();
    assert_eq!(server.read(&mut buf).unwrap(), 2);
    assert_eq!(&buf[..2], b"he");
    assert_eq!(server.read(&mut buf).unwrap(), 2);
    assert_eq!(&buf[..2], b"ll");
    assert_eq!(server.read(&mut buf).unwrap(), 1);
    assert_eq!(&buf[..1], b"o");
    assert_eq!(
        server.read(&mut buf).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn testpipe_caps_short_writes() {
    let (mut server, mut _client) = pipe_pair(64, /* write_chunk */ 3);
 // A single write accepts at most write_chunk bytes.
    assert_eq!(server.write(b"abcdef").unwrap(), 3);
}

/// The "trivial backend": a no-crypto echo loop driven over the pipe under
/// 1-byte fragmentation. Proves the harness drives a non-blocking
/// request/response cycle deterministically, independent of TLS.
#[test]
fn trivial_nonblocking_echo_roundtrips() {
    let (mut server, mut client) = pipe_pair(1, 1);

    client.write_all(b"PING\r\n").unwrap();

    let req = read_line(&mut server);
    assert_eq!(req, b"PING\r\n");
    server.write_all(&req).unwrap();

    let resp = read_line(&mut client);
    assert_eq!(resp, b"PING\r\n");
}

// ─── tests: the real SocketConnectionType backend over the vtable ────────────

/// Prove the vtable on plain TCP first (no crypto): a request read and an echo
/// write, both through `SocketConnectionType`'s `ConnectionTypeTrait` methods
/// over an owned `PipeEnd` stream.
#[test]
fn socket_backend_echoes_through_the_vtable() {
    let ct = SocketConnectionType::new();
    let (server_end, mut peer) = pipe_pair(64, 64);
    let mut conn = Connection::new(ConnectionTypeId::Socket, 7).with_stream(Box::new(server_end));
    conn.state = ConnectionState::Connected;

    peer.write_all(b"PING\r\n").unwrap();
    let mut buf = [0u8; 64];
    let n = ct.read(&mut conn, &mut buf).unwrap();
    assert_eq!(&buf[..n], b"PING\r\n");

    assert_eq!(ct.write(&mut conn, b"PING\r\n").unwrap(), 6);
    let mut out = [0u8; 64];
    let m = peer.read(&mut out).unwrap();
    assert_eq!(&out[..m], b"PING\r\n");
}

/// An empty stream surfaces as `Io(WouldBlock)`, never a spurious EOF.
#[test]
fn socket_backend_read_would_block_when_empty() {
    let ct = SocketConnectionType::new();
    let (server_end, _peer) = pipe_pair(64, 64);
    let mut conn = Connection::new(ConnectionTypeId::Socket, 7).with_stream(Box::new(server_end));
    conn.state = ConnectionState::Connected;

    let mut buf = [0u8; 16];
    match ct.read(&mut conn, &mut buf) {
        Err(RedisError::Io(kind)) => assert_eq!(kind, io::ErrorKind::WouldBlock),
        other => panic!("expected Io(WouldBlock), got {other:?}"),
    }
 // A would-block must NOT transition a connected socket to error state.
    assert_eq!(conn.state, ConnectionState::Connected);
}

/// EOF (peer closed → empty + closed) surfaces as `Ok(0)` and marks CLOSED.
#[test]
fn socket_backend_eof_marks_closed() {
    let ct = SocketConnectionType::new();
 // A reader over an empty, permanently-closed source: a cursor at end.
    let mut conn = Connection::new(ConnectionTypeId::Socket, 7)
        .with_stream(Box::new(io::Cursor::new(Vec::<u8>::new())));
    conn.state = ConnectionState::Connected;

    let mut buf = [0u8; 16];
    assert_eq!(ct.read(&mut conn, &mut buf).unwrap(), 0);
    assert_eq!(conn.state, ConnectionState::Closed);
}

static EVENT_LOG: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
fn rec_read(_c: &mut Connection) {
    EVENT_LOG.lock().unwrap().push("read");
}
fn rec_write(_c: &mut Connection) {
    EVENT_LOG.lock().unwrap().push("write");
}

/// Port of `connSocketEventHandler`: read fires before write normally, and
/// order inverts under the write barrier.
#[test]
fn socket_event_handler_orders_handlers_and_inverts_on_barrier() {
    let ct = SocketConnectionType::new();
    let (server_end, _peer) = pipe_pair(64, 64);
    let mut conn = Connection::new(ConnectionTypeId::Socket, 7).with_stream(Box::new(server_end));
    conn.state = ConnectionState::Connected;
    ct.set_read_handler(&mut conn, Some(rec_read)).unwrap();
    ct.set_write_handler(&mut conn, Some(rec_write), false)
        .unwrap();

    EVENT_LOG.lock().unwrap().clear();
    socket_event_handler(&mut conn, true, true);
    assert_eq!(*EVENT_LOG.lock().unwrap(), vec!["read", "write"]);

    ct.set_write_handler(&mut conn, Some(rec_write), true)
        .unwrap();
    EVENT_LOG.lock().unwrap().clear();
    socket_event_handler(&mut conn, true, true);
    assert_eq!(*EVENT_LOG.lock().unwrap(), vec!["write", "read"]);
}

fn read_line(end: &mut PipeEnd) -> Vec<u8> {
    let mut acc = Vec::new();
    let mut buf = [0u8; 4];
    loop {
        match end.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                acc.extend_from_slice(&buf[..n]);
                if acc.ends_with(b"\r\n") {
                    break;
                }
            }
            Err(e) if would_block(&e) => break,
            Err(e) => panic!("unexpected read error: {e}"),
        }
    }
    acc
}

// ─── tests: real rustls over the pipe ────────────────────────────────────────

#[test]
fn rustls_handshake_and_echo_clean() {
    let (mut server, mut client) = fresh();
    let (mut s_end, mut c_end) = pipe_pair(16384, 16384);

    drive(&mut server, &mut s_end, &mut client, &mut c_end).unwrap();
    assert!(!server.is_handshaking(), "server handshake incomplete");
    assert!(!client.is_handshaking(), "client handshake incomplete");

    let msg = b"*1\r\n$4\r\nPING\r\n";
    client.writer().write_all(msg).unwrap();
    drive(&mut server, &mut s_end, &mut client, &mut c_end).unwrap();
    assert_eq!(
        drain_plaintext(server.reader()),
        msg,
        "server did not see request"
    );

    server.writer().write_all(msg).unwrap();
    drive(&mut server, &mut s_end, &mut client, &mut c_end).unwrap();
    assert_eq!(
        drain_plaintext(client.reader()),
        msg,
        "client did not see reply"
    );
}

/// The killer case: the entire handshake is delivered one byte per event with
/// short writes. A correct non-blocking state machine must still complete it.
#[test]
fn rustls_handshake_completes_one_byte_at_a_time() {
    let (mut server, mut client) = fresh();
    let (mut s_end, mut c_end) = pipe_pair(1, 1);

    drive(&mut server, &mut s_end, &mut client, &mut c_end).unwrap();
    assert!(!server.is_handshaking());
    assert!(!client.is_handshaking());

    client.writer().write_all(b"+OK\r\n").unwrap();
    drive(&mut server, &mut s_end, &mut client, &mut c_end).unwrap();
    assert_eq!(drain_plaintext(server.reader()), b"+OK\r\n");
}

/// A large reply forces many `write_tls` records and many fragmented reads.
/// rustls's `writer` has a bounded plaintext buffer, so a real server must
/// interleave queueing plaintext with draining it via `write_tls` — it cannot
/// dump an arbitrarily large reply in one shot. This test mirrors that
/// streaming/backpressure pattern, which the eventual owner-loop integration
/// must also honor.
#[test]
fn rustls_large_reply_streams_intact() {
    let (mut server, mut client) = fresh();
    let (mut s_end, mut c_end) = pipe_pair(37, 37);

    drive(&mut server, &mut s_end, &mut client, &mut c_end).unwrap();

    let payload = vec![b'x'; 100_000];
    let mut got = Vec::new();
    let mut sent = 0;
    while sent < payload.len() {
        let end = (sent + 8192).min(payload.len());
        server.writer().write_all(&payload[sent..end]).unwrap();
        sent = end;
        drive(&mut server, &mut s_end, &mut client, &mut c_end).unwrap();
        got.extend_from_slice(&drain_plaintext(client.reader()));
    }

    assert_eq!(got.len(), payload.len());
    assert_eq!(got, payload);
}

/// Invariance under arbitrary fragmentation: for every chunk size,
/// handshake completes and an echo round-trips byte-identically. This is
/// "passes in dev, flakes under load" class, made deterministic.
#[test]
fn rustls_roundtrips_under_all_chunkings() {
    let msg = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n";
    for &chunk in &[1usize, 2, 3, 5, 8, 13, 64, 1024, 16384] {
        let (mut server, mut client) = fresh();
        let (mut s_end, mut c_end) = pipe_pair(chunk, chunk);

        drive(&mut server, &mut s_end, &mut client, &mut c_end).unwrap();
        assert!(!server.is_handshaking(), "handshake stuck at chunk={chunk}");

        client.writer().write_all(msg).unwrap();
        drive(&mut server, &mut s_end, &mut client, &mut c_end).unwrap();
        assert_eq!(
            drain_plaintext(server.reader()),
            msg,
            "request mismatch at chunk={chunk}"
        );

        server.writer().write_all(msg).unwrap();
        drive(&mut server, &mut s_end, &mut client, &mut c_end).unwrap();
        assert_eq!(
            drain_plaintext(client.reader()),
            msg,
            "reply mismatch at chunk={chunk}"
        );
    }
}

// ─── tests: the real TlsConnectionType backend over the vtable ───────────────

fn noop_accept(_conn: &mut Connection) {}

/// Drive a TLS handshake between the backend-managed server `conn` and a raw
/// rustls `client`, shuttling ciphertext over `peer` (the client's pipe end;
/// the server's end lives inside `conn.io`).
fn drive_tls_handshake(
    ct: &TlsConnectionType,
    conn: &mut Connection,
    client: &mut ClientConnection,
    peer: &mut PipeEnd,
) {
    for _ in 0..10_000 {
        flush_tls(client, peer).unwrap();
        feed_tls(client, peer).unwrap();
        if conn.state == ConnectionState::Accepting {
            ct.accept(conn, noop_accept).unwrap();
        }
        if conn.state == ConnectionState::Connected && !client.is_handshaking() {
            return;
        }
    }
    panic!("TLS handshake did not converge");
}

/// The headline dogfood: a real TLS handshake + request/echo entirely through
/// the `TlsConnectionType` vtable (server) against a raw rustls client.
#[test]
fn tls_backend_handshake_and_echo_through_vtable() {
    let ct = TlsConnectionType::from_server_config(test_server_config());
    let mut client = test_client();
    let (server_end, mut peer) = pipe_pair(16384, 16384);
    let mut conn = ct.accept_connection(Box::new(server_end)).unwrap();

    drive_tls_handshake(&ct, &mut conn, &mut client, &mut peer);
    assert_eq!(conn.state, ConnectionState::Connected);

    let msg = b"*1\r\n$4\r\nPING\r\n";
    client.writer().write_all(msg).unwrap();
    flush_tls(&mut client, &mut peer).unwrap();

    let mut buf = [0u8; 256];
    let n = ct.read(&mut conn, &mut buf).unwrap();
    assert_eq!(&buf[..n], msg, "server did not decrypt the request");

    assert_eq!(ct.write(&mut conn, msg).unwrap(), msg.len());
    feed_tls(&mut client, &mut peer).unwrap();
    assert_eq!(
        drain_plaintext(client.reader()),
        msg,
        "client did not receive the echo"
    );
}

/// The killer non-blocking case, now through the real backend: the handshake
/// delivered one byte per event must still complete and carry app data.
#[test]
fn tls_backend_handshake_one_byte_at_a_time() {
    let ct = TlsConnectionType::from_server_config(test_server_config());
    let mut client = test_client();
    let (server_end, mut peer) = pipe_pair(1, 1);
    let mut conn = ct.accept_connection(Box::new(server_end)).unwrap();

    drive_tls_handshake(&ct, &mut conn, &mut client, &mut peer);
    assert_eq!(conn.state, ConnectionState::Connected);

    client.writer().write_all(b"+OK\r\n").unwrap();
    flush_tls(&mut client, &mut peer).unwrap();
    let mut buf = [0u8; 64];
    let n = ct.read(&mut conn, &mut buf).unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n");
}

/// All-chunkings invariant through the real TLS backend: for every
/// fragmentation, handshake completes and an echo round-trips byte-identically.
#[test]
fn tls_backend_roundtrips_under_all_chunkings() {
    let msg = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n";
    for &chunk in &[1usize, 2, 3, 5, 8, 13, 64, 1024, 16384] {
        let ct = TlsConnectionType::from_server_config(test_server_config());
        let mut client = test_client();
        let (server_end, mut peer) = pipe_pair(chunk, chunk);
        let mut conn = ct.accept_connection(Box::new(server_end)).unwrap();

        drive_tls_handshake(&ct, &mut conn, &mut client, &mut peer);
        assert_eq!(
            conn.state,
            ConnectionState::Connected,
            "handshake stuck at chunk={chunk}"
        );

        client.writer().write_all(msg).unwrap();
        flush_tls(&mut client, &mut peer).unwrap();
        let mut buf = [0u8; 256];
        let n = ct.read(&mut conn, &mut buf).unwrap();
        assert_eq!(&buf[..n], msg, "request mismatch at chunk={chunk}");

        assert_eq!(ct.write(&mut conn, msg).unwrap(), msg.len());
        feed_tls(&mut client, &mut peer).unwrap();
        assert_eq!(
            drain_plaintext(client.reader()),
            msg,
            "reply mismatch at chunk={chunk}"
        );
    }
}
