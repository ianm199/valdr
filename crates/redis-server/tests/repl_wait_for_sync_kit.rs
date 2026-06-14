use std::fmt;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Frame {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Frame>),
}

impl Frame {
    fn text_lossy(&self) -> String {
        match self {
            Frame::Simple(bytes) | Frame::Error(bytes) => String::from_utf8_lossy(bytes).into(),
            Frame::Integer(n) => n.to_string(),
            Frame::Bulk(Some(bytes)) => String::from_utf8_lossy(bytes).into(),
            Frame::Bulk(None) => "(nil)".to_string(),
            Frame::Array(items) => items
                .iter()
                .map(Frame::text_lossy)
                .collect::<Vec<_>>()
                .join("|"),
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.text_lossy())
    }
}

struct RespConn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl RespConn {
    fn connect(port: u16) -> io::Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        Ok(Self {
            stream,
            buf: Vec::new(),
        })
    }

    fn command(&mut self, parts: &[&[u8]]) -> io::Result<Frame> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            encoded.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            encoded.extend_from_slice(part);
            encoded.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&encoded)?;
        self.read_frame()
    }

    fn read_frame(&mut self) -> io::Result<Frame> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some((frame, consumed)) = parse_frame(&self.buf)? {
                self.buf.drain(..consumed);
                return Ok(frame);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out reading RESP frame",
                ));
            }
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "server closed connection",
                    ));
                }
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }
}

fn parse_frame(buf: &[u8]) -> io::Result<Option<(Frame, usize)>> {
    if buf.is_empty() {
        return Ok(None);
    }
    match buf[0] {
        b'+' => parse_line_payload(buf).map(|opt| opt.map(|(bytes, n)| (Frame::Simple(bytes), n))),
        b'-' => parse_line_payload(buf).map(|opt| opt.map(|(bytes, n)| (Frame::Error(bytes), n))),
        b':' => match parse_line_payload(buf)? {
            Some((bytes, n)) => {
                let text = std::str::from_utf8(&bytes)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 integer"))?;
                let value = text
                    .parse()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad integer"))?;
                Ok(Some((Frame::Integer(value), n)))
            }
            None => Ok(None),
        },
        b'$' => parse_bulk(buf),
        b'*' => parse_array(buf),
        kind => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown RESP frame byte: {kind:#x}"),
        )),
    }
}

fn parse_line_payload(buf: &[u8]) -> io::Result<Option<(Vec<u8>, usize)>> {
    let Some(end) = find_crlf(buf, 1) else {
        return Ok(None);
    };
    Ok(Some((buf[1..end].to_vec(), end + 2)))
}

fn parse_bulk(buf: &[u8]) -> io::Result<Option<(Frame, usize)>> {
    let Some(line_end) = find_crlf(buf, 1) else {
        return Ok(None);
    };
    let len_text = std::str::from_utf8(&buf[1..line_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 bulk length"))?;
    let len: isize = len_text
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad bulk length"))?;
    if len < 0 {
        return Ok(Some((Frame::Bulk(None), line_end + 2)));
    }
    let body_start = line_end + 2;
    let body_end = body_start + len as usize;
    let frame_end = body_end + 2;
    if buf.len() < frame_end {
        return Ok(None);
    }
    if &buf[body_end..frame_end] != b"\r\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bulk string missing trailing CRLF",
        ));
    }
    Ok(Some((
        Frame::Bulk(Some(buf[body_start..body_end].to_vec())),
        frame_end,
    )))
}

fn parse_array(buf: &[u8]) -> io::Result<Option<(Frame, usize)>> {
    let Some(line_end) = find_crlf(buf, 1) else {
        return Ok(None);
    };
    let len_text = std::str::from_utf8(&buf[1..line_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 array length"))?;
    let len: isize = len_text
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad array length"))?;
    if len < 0 {
        return Ok(Some((Frame::Array(Vec::new()), line_end + 2)));
    }
    let mut consumed = line_end + 2;
    let mut frames = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let Some((frame, n)) = parse_frame(&buf[consumed..])? else {
            return Ok(None);
        };
        consumed += n;
        frames.push(frame);
    }
    Ok(Some((Frame::Array(frames), consumed)))
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    buf.get(from..)?
        .windows(2)
        .position(|pair| pair == b"\r\n")
        .map(|idx| from + idx)
}

struct TestServer {
    child: Child,
    dir: PathBuf,
    port: u16,
}

impl TestServer {
    fn start(name: &str) -> io::Result<Self> {
        let port = free_port()?;
        let dir = unique_temp_dir(name);
        std::fs::create_dir_all(&dir)?;
        let stdout = std::fs::File::create(dir.join("stdout"))?;
        let stderr = std::fs::File::create(dir.join("stderr"))?;
        let child = Command::new(env!("CARGO_BIN_EXE_redis-server"))
            .arg("--port")
            .arg(port.to_string())
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--dir")
            .arg(&dir)
            .arg("--save")
            .arg("")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;
        let mut server = Self { child, dir, port };
        server.wait_ready()?;
        Ok(server)
    }

    fn wait_ready(&mut self) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(mut conn) = RespConn::connect(self.port) {
                if matches!(conn.command(&[b"PING"]), Ok(Frame::Simple(bytes)) if bytes == b"PONG")
                {
                    return Ok(());
                }
            }
            if let Some(status) = self.child.try_wait()? {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("server exited before ready: {status}"),
                ));
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("server on port {} did not become ready", self.port),
                ));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn stderr_tail(&self) -> String {
        std::fs::read_to_string(self.dir.join("stderr"))
            .unwrap_or_default()
            .chars()
            .rev()
            .take(4000)
            .collect::<String>()
            .chars()
            .rev()
            .collect()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("valdr-{name}-{}-{nanos}", std::process::id()))
}

fn expect_simple(frame: Frame, expected: &[u8]) {
    match frame {
        Frame::Simple(bytes) if bytes == expected => {}
        other => panic!(
            "expected simple {:?}, got {:?}",
            String::from_utf8_lossy(expected),
            other
        ),
    }
}

fn expect_success(frame: Frame) {
    match frame {
        Frame::Simple(bytes) if bytes == b"OK" => {}
        Frame::Integer(n) if n >= 0 => {}
        other => panic!("expected successful write reply, got {other:?}"),
    }
}

fn info_field(frame: &Frame, field: &str) -> Option<String> {
    let Frame::Bulk(Some(bytes)) = frame else {
        return None;
    };
    let text = String::from_utf8_lossy(bytes);
    text.lines()
        .find_map(|line| line.strip_prefix(field)?.strip_prefix(':'))
        .map(|value| value.trim_end_matches('\r').to_string())
}

fn info_u64(frame: &Frame, field: &str) -> Option<u64> {
    info_field(frame, field)?.parse().ok()
}

fn wait_for_replica_up(replica: &TestServer, master: &TestServer) -> io::Result<Frame> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = None;
    while Instant::now() < deadline {
        let mut conn = RespConn::connect(replica.port)?;
        let info = conn.command(&[b"INFO", b"replication"])?;
        if info_field(&info, "master_link_status").as_deref() == Some("up") {
            return Ok(info);
        }
        last = Some(info);
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "replica did not sync in time; last INFO={:?}; replica stderr={}; master stderr={}",
            last,
            replica.stderr_tail(),
            master.stderr_tail()
        ),
    ))
}

fn info(server: &TestServer, section: &[u8]) -> io::Result<Frame> {
    let mut conn = RespConn::connect(server.port)?;
    conn.command(&[b"INFO", section])
}

fn wait_for_info_field(
    server: &TestServer,
    section: &[u8],
    field: &str,
    expected: &str,
) -> io::Result<Frame> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = None;
    while Instant::now() < deadline {
        let frame = info(server, section)?;
        if info_field(&frame, field).as_deref() == Some(expected) {
            return Ok(frame);
        }
        last = Some(frame);
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "INFO {section:?} field {field} did not reach {expected}; last={last:?}; stderr={}",
            server.stderr_tail()
        ),
    ))
}

fn wait_for_info_counter_at_least(
    server: &TestServer,
    section: &[u8],
    field: &str,
    minimum: u64,
) -> io::Result<Frame> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = None;
    while Instant::now() < deadline {
        let frame = info(server, section)?;
        if info_u64(&frame, field).is_some_and(|value| value >= minimum) {
            return Ok(frame);
        }
        last = Some(frame);
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "INFO {section:?} counter {field} did not reach {minimum}; last={last:?}; stderr={}",
            server.stderr_tail()
        ),
    ))
}

fn configure_psync_surface(master: &TestServer, replica: &TestServer) {
    let mut master_conn = RespConn::connect(master.port).expect("connect master for config");
    for command in [
        [
            b"CONFIG".as_slice(),
            b"SET",
            b"repl-backlog-size",
            b"1000000",
        ]
        .as_slice(),
        [b"CONFIG".as_slice(), b"SET", b"repl-backlog-ttl", b"3600"].as_slice(),
        [b"CONFIG".as_slice(), b"SET", b"repl-diskless-sync", b"no"].as_slice(),
        [
            b"CONFIG".as_slice(),
            b"SET",
            b"repl-diskless-sync-delay",
            b"1",
        ]
        .as_slice(),
        [
            b"CONFIG".as_slice(),
            b"SET",
            b"dual-channel-replication-enabled",
            b"no",
        ]
        .as_slice(),
    ] {
        expect_simple(
            master_conn.command(command).expect("master CONFIG SET"),
            b"OK",
        );
    }

    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica for config");
    for command in [
        [
            b"CONFIG".as_slice(),
            b"SET",
            b"repl-diskless-load",
            b"disabled",
        ]
        .as_slice(),
        [
            b"CONFIG".as_slice(),
            b"SET",
            b"dual-channel-replication-enabled",
            b"no",
        ]
        .as_slice(),
    ] {
        expect_simple(
            replica_conn.command(command).expect("replica CONFIG SET"),
            b"OK",
        );
    }
}

fn write_complex_data(port: u16, start: usize, count: usize) -> io::Result<()> {
    let mut conn = RespConn::connect(port)?;
    let mut selected_db = None;
    for i in start..start + count {
        let db = match i % 3 {
            0 => 9,
            1 => 11,
            _ => 12,
        };
        if selected_db != Some(db) {
            expect_simple(
                conn.command(&[b"SELECT", db.to_string().as_bytes()])?,
                b"OK",
            );
            selected_db = Some(db);
        }
        let suffix = i.to_string();
        let key = format!("key:{suffix}");
        let value = format!("value:{suffix}");
        let hash = format!("hash:{}", i % 17);
        let set = format!("set:{}", i % 19);
        let zset = format!("zset:{}", i % 23);
        let list = format!("list:{}", i % 29);
        let score = (i % 97).to_string();

        expect_success(conn.command(&[b"SET", key.as_bytes(), value.as_bytes()])?);
        expect_success(conn.command(&[
            b"HSET",
            hash.as_bytes(),
            key.as_bytes(),
            value.as_bytes(),
        ])?);
        expect_success(conn.command(&[b"SADD", set.as_bytes(), key.as_bytes()])?);
        expect_success(conn.command(&[
            b"ZADD",
            zset.as_bytes(),
            score.as_bytes(),
            key.as_bytes(),
        ])?);
        expect_success(conn.command(&[b"LPUSH", list.as_bytes(), value.as_bytes()])?);
    }
    Ok(())
}

fn debug_digest(server: &TestServer) -> io::Result<String> {
    let mut conn = RespConn::connect(server.port)?;
    match conn.command(&[b"DEBUG", b"DIGEST"])? {
        Frame::Bulk(Some(bytes)) | Frame::Simple(bytes) => {
            Ok(String::from_utf8_lossy(&bytes).into())
        }
        frame => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected DEBUG DIGEST reply: {frame:?}"),
        )),
    }
}

fn wait_for_digest_match(master: &TestServer, replica: &TestServer) -> io::Result<String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_master = String::new();
    let mut last_replica = String::new();
    while Instant::now() < deadline {
        last_master = debug_digest(master)?;
        last_replica = debug_digest(replica)?;
        if last_master == last_replica {
            return Ok(last_master);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "digest did not converge; master={last_master}, replica={last_replica}; replica stderr={}; master stderr={}",
            replica.stderr_tail(),
            master.stderr_tail()
        ),
    ))
}

#[test]
fn multi_slaveof_fullsync_reaches_up_and_transfers_master_key() {
    let master = TestServer::start("repl-wait-master").expect("start master");
    let replica = TestServer::start("repl-wait-replica").expect("start replica");

    let mut master_conn = RespConn::connect(master.port).expect("connect master");
    expect_simple(
        master_conn
            .command(&[b"SET", b"mykey", b"foo"])
            .expect("SET should reply"),
        b"OK",
    );

    let mut txn = RespConn::connect(replica.port).expect("connect replica");
    expect_simple(txn.command(&[b"MULTI"]).expect("MULTI should reply"), b"OK");
    expect_simple(
        txn.command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
            .expect("SLAVEOF should queue"),
        b"QUEUED",
    );
    expect_simple(
        txn.command(&[b"INFO", b"replication"])
            .expect("INFO should queue"),
        b"QUEUED",
    );
    let exec = txn.command(&[b"EXEC"]).expect("EXEC should reply");
    assert!(
        exec.text_lossy().contains("master_link_status:down"),
        "EXEC should include the immediate down link status observed by Tcl, got {exec:?}"
    );

    let mut role_conn = RespConn::connect(replica.port).expect("connect replica for ROLE");
    let role = role_conn.command(&[b"ROLE"]).expect("ROLE should reply");
    assert!(
        matches!(&role, Frame::Array(items) if matches!(items.first(), Some(Frame::Bulk(Some(role))) if role == b"slave")),
        "ROLE should switch to slave immediately after EXEC, got {role:?}"
    );

    let info = wait_for_replica_up(&replica, &master).expect("replica should sync");
    assert_eq!(
        info_field(&info, "master_sync_in_progress").as_deref(),
        Some("0"),
        "synced replica should clear master_sync_in_progress: {info:?}"
    );

    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica for GET");
    let value = replica_conn
        .command(&[b"GET", b"mykey"])
        .expect("GET should reply");
    assert_eq!(
        value,
        Frame::Bulk(Some(b"foo".to_vec())),
        "full sync should transfer the master's existing key"
    );
}

#[test]
fn psync_no_reconnect_fullsync_converges_under_cross_db_write_load() {
    let master = TestServer::start("repl-psync-master").expect("start master");
    let replica = TestServer::start("repl-psync-replica").expect("start replica");
    configure_psync_surface(&master, &replica);

    write_complex_data(master.port, 0, 60).expect("seed master data");
    let master_port = master.port;
    let writer = thread::spawn(move || write_complex_data(master_port, 60, 120));

    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica");
    expect_simple(
        replica_conn
            .command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
            .expect("SLAVEOF should reply"),
        b"OK",
    );

    wait_for_replica_up(&replica, &master).expect("replica should reach online state");
    writer
        .join()
        .expect("writer thread should not panic")
        .expect("writer commands should succeed");
    let digest = wait_for_digest_match(&master, &replica).expect("digests should converge");
    assert!(
        !digest.is_empty(),
        "DEBUG DIGEST should return a non-empty digest after write load"
    );
}

#[test]
fn psync_same_primary_socket_drop_reconnects_with_continue() {
    let master = TestServer::start("repl-reconnect-master").expect("start master");
    let replica = TestServer::start("repl-reconnect-replica").expect("start replica");
    configure_psync_surface(&master, &replica);

    write_complex_data(master.port, 400, 25).expect("seed master data");
    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica");
    expect_simple(
        replica_conn
            .command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
            .expect("SLAVEOF should reply"),
        b"OK",
    );
    wait_for_replica_up(&replica, &master).expect("initial full sync should finish");
    write_complex_data(master.port, 450, 10)
        .expect("online writes before reconnect should advance the PSYNC offset");
    wait_for_digest_match(&master, &replica)
        .expect("replica should apply online writes before the forced socket drop");

    let stats_before = wait_for_info_counter_at_least(&master, b"stats", "sync_full", 1)
        .expect("master should count initial full sync");
    let sync_full_before = info_u64(&stats_before, "sync_full").expect("sync_full field");
    let partial_ok_before = info_u64(&stats_before, "sync_partial_ok").unwrap_or(0);

    let mut master_conn = RespConn::connect(master.port).expect("connect master");
    match master_conn
        .command(&[b"CLIENT", b"KILL", b"TYPE", b"replica"])
        .expect("CLIENT KILL TYPE replica should reply")
    {
        Frame::Integer(killed) if killed >= 1 => {}
        frame => panic!("expected to kill at least one replica client, got {frame:?}"),
    }
    wait_for_info_field(&master, b"replication", "connected_slaves", "0")
        .expect("master should observe the dropped replica connection");

    write_complex_data(master.port, 500, 40)
        .expect("master writes while replica link is reconnecting should succeed");
    let stats_after =
        wait_for_info_counter_at_least(&master, b"stats", "sync_partial_ok", partial_ok_before + 1)
            .expect("same-primary reconnect should be accepted as partial resync");
    assert_eq!(
        info_u64(&stats_after, "sync_full"),
        Some(sync_full_before),
        "same-primary socket drop with retained backlog must not require another full sync"
    );

    wait_for_replica_up(&replica, &master).expect("replica should return online");
    let digest = wait_for_digest_match(&master, &replica)
        .expect("replica should receive writes made during reconnect");
    assert!(
        !digest.is_empty(),
        "DEBUG DIGEST should return a non-empty digest after partial reconnect"
    );
}
