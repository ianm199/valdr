use std::fmt;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
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
    read_deadline: Duration,
}

impl RespConn {
    fn connect(port: u16) -> io::Result<Self> {
        Self::connect_with_deadline(port, Duration::from_secs(2))
    }

    fn connect_with_deadline(port: u16, read_deadline: Duration) -> io::Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(Duration::from_millis(100)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        Ok(Self {
            stream,
            buf: Vec::new(),
            read_deadline,
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
        let deadline = Instant::now() + self.read_deadline;
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

fn expect_non_error(frame: Frame) {
    if let Frame::Error(err) = frame {
        panic!(
            "expected non-error reply, got {:?}",
            String::from_utf8_lossy(&err)
        );
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

fn wait_for_replicas_offset_match(
    master: &TestServer,
    replicas: &[&TestServer],
) -> io::Result<u64> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = String::new();
    while Instant::now() < deadline {
        let master_info = info(master, b"replication")?;
        let Some(master_offset) = info_u64(&master_info, "master_repl_offset") else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("master INFO has no master_repl_offset: {master_info:?}"),
            ));
        };
        let mut offsets = Vec::with_capacity(replicas.len());
        let mut all_match = true;
        for replica in replicas {
            let replica_info = info(replica, b"replication")?;
            let link_up = info_field(&replica_info, "master_link_status").as_deref() == Some("up");
            let offset = info_u64(&replica_info, "master_repl_offset");
            offsets.push((replica.port, link_up, offset));
            if !link_up || offset != Some(master_offset) {
                all_match = false;
            }
        }
        if all_match {
            return Ok(master_offset);
        }
        last = format!("master_offset={master_offset}, replica_offsets={offsets:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "replica offsets did not converge; last={last}; master stderr={}; replica stderrs={:?}",
            master.stderr_tail(),
            replicas
                .iter()
                .map(|replica| replica.stderr_tail())
                .collect::<Vec<_>>()
        ),
    ))
}

fn configure_psync_surface(master: &TestServer, replica: &TestServer) {
    configure_psync_surface_with_diskless_load(master, replica, b"disabled");
}

fn configure_psync_surface_with_diskless_load(
    master: &TestServer,
    replica: &TestServer,
    repl_diskless_load: &[u8],
) {
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
            repl_diskless_load,
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

fn write_mutating_complex_data(port: u16, start: usize, count: usize) -> io::Result<()> {
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
        let str_key = format!("mut:str:{}", i % 37);
        let hash = format!("mut:hash:{}", i % 11);
        let set_a = format!("mut:set:a:{}", i % 13);
        let set_b = format!("mut:set:b:{}", i % 13);
        let set_dst = format!("mut:set:dst:{}", i % 7);
        let zset_a = format!("mut:zset:a:{}", i % 17);
        let zset_b = format!("mut:zset:b:{}", i % 17);
        let zset_dst = format!("mut:zset:dst:{}", i % 9);
        let list = format!("mut:list:{}", i % 5);
        let field = format!("field:{}", i % 23);
        let member = format!("member:{suffix}");
        let value = format!("value:{suffix}");
        let score = (i % 101).to_string();

        expect_success(conn.command(&[b"SET", str_key.as_bytes(), value.as_bytes()])?);
        expect_success(conn.command(&[
            b"HSET",
            hash.as_bytes(),
            field.as_bytes(),
            value.as_bytes(),
        ])?);
        if i % 4 == 0 {
            expect_success(conn.command(&[b"HDEL", hash.as_bytes(), field.as_bytes()])?);
        }
        expect_success(conn.command(&[b"SADD", set_a.as_bytes(), member.as_bytes()])?);
        expect_success(conn.command(&[b"SADD", set_b.as_bytes(), value.as_bytes()])?);
        if i % 5 == 0 {
            expect_success(conn.command(&[b"SREM", set_a.as_bytes(), member.as_bytes()])?);
        }
        expect_success(conn.command(&[
            b"SUNIONSTORE",
            set_dst.as_bytes(),
            set_a.as_bytes(),
            set_b.as_bytes(),
        ])?);
        expect_success(conn.command(&[
            b"ZADD",
            zset_a.as_bytes(),
            score.as_bytes(),
            member.as_bytes(),
        ])?);
        expect_success(conn.command(&[
            b"ZADD",
            zset_b.as_bytes(),
            score.as_bytes(),
            value.as_bytes(),
        ])?);
        if i % 6 == 0 {
            expect_success(conn.command(&[b"ZREM", zset_a.as_bytes(), member.as_bytes()])?);
        }
        expect_success(conn.command(&[
            b"ZUNIONSTORE",
            zset_dst.as_bytes(),
            b"2",
            zset_a.as_bytes(),
            zset_b.as_bytes(),
        ])?);
        expect_success(conn.command(&[b"LPUSH", list.as_bytes(), value.as_bytes()])?);
        if i % 7 == 0 {
            expect_success(conn.command(&[b"LREM", list.as_bytes(), b"0", value.as_bytes()])?);
        }
    }
    Ok(())
}

#[derive(Clone)]
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.0
    }

    fn range(&mut self, upper: usize) -> usize {
        (self.next() as usize) % upper
    }
}

fn bg_value(rng: &mut Lcg) -> Vec<u8> {
    match rng.range(6) {
        0 => {
            let value = rng.range(2_000) as i64 - 1_000;
            value.to_string().into_bytes()
        }
        1 => {
            let value = rng.next() as i64;
            value.to_string().into_bytes()
        }
        2 => {
            let len = rng.range(80);
            (0..len)
                .map(|_| b'0' + rng.range((b'z' - b'0' + 1) as usize) as u8)
                .filter(|byte| *byte != b'\\')
                .collect()
        }
        3 => {
            let len = rng.range(160);
            (0..len)
                .map(|_| b'0' + rng.range((b'4' - b'0' + 1) as usize) as u8)
                .collect()
        }
        _ => {
            let len = rng.range(160);
            (0..len)
                .map(|idx| match idx % 13 {
                    0 => 0,
                    1 => b'\r',
                    2 => b'\n',
                    3 => b'"',
                    4 => b'\\',
                    _ => rng.range(256) as u8,
                })
                .collect()
        }
    }
}

fn find_bg_key_with_type(
    conn: &mut RespConn,
    rng: &mut Lcg,
    wanted: &str,
) -> io::Result<Option<String>> {
    for _ in 0..20 {
        let candidate = format!("bg:{}", rng.range(64));
        if conn.command(&[b"TYPE", candidate.as_bytes()])?.text_lossy() == wanted {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn write_bg_complex_style_op(conn: &mut RespConn, rng: &mut Lcg) -> io::Result<()> {
    let key_id = rng.range(64);
    let key = format!("bg:{key_id}");
    let dst_key = format!("bg:dst:{}", rng.range(16));
    let field = bg_value(rng);
    let value = bg_value(rng);
    let score = match rng.range(16) {
        0 => "+inf".to_string(),
        1 => "-inf".to_string(),
        n => format!("{}.{}", n, rng.range(1_000_000)),
    };

    let mut kind = conn.command(&[b"TYPE", key.as_bytes()])?.text_lossy();
    if kind == "none" {
        match rng.range(6) {
            0 => expect_success(conn.command(&[b"SET", key.as_bytes(), value.as_slice()])?),
            1 => expect_success(conn.command(&[b"LPUSH", key.as_bytes(), value.as_slice()])?),
            2 => expect_success(conn.command(&[b"SADD", key.as_bytes(), value.as_slice()])?),
            3 => expect_success(conn.command(&[
                b"ZADD",
                key.as_bytes(),
                score.as_bytes(),
                value.as_slice(),
            ])?),
            4 => expect_success(conn.command(&[
                b"HSET",
                key.as_bytes(),
                field.as_slice(),
                value.as_slice(),
            ])?),
            _ => expect_success(conn.command(&[b"DEL", key.as_bytes()])?),
        }
        kind = conn.command(&[b"TYPE", key.as_bytes()])?.text_lossy();
    }

    match kind.as_str() {
        "string" | "none" => {}
        "list" => match rng.range(5) {
            0 => expect_success(conn.command(&[b"LPUSH", key.as_bytes(), value.as_slice()])?),
            1 => expect_success(conn.command(&[b"RPUSH", key.as_bytes(), value.as_slice()])?),
            2 => {
                expect_success(conn.command(&[b"LREM", key.as_bytes(), b"0", value.as_slice()])?)
            }
            3 => expect_non_error(conn.command(&[b"RPOP", key.as_bytes()])?),
            _ => expect_non_error(conn.command(&[b"LPOP", key.as_bytes()])?),
        },
        "set" => match rng.range(4) {
            0 => expect_success(conn.command(&[b"SADD", key.as_bytes(), value.as_slice()])?),
            1 => expect_success(conn.command(&[b"SREM", key.as_bytes(), value.as_slice()])?),
            n => {
                if let Some(other_key) = find_bg_key_with_type(conn, rng, "set")? {
                    let op: &[u8] = match n {
                        2 => b"SUNIONSTORE",
                        3 => b"SINTERSTORE",
                        _ => b"SDIFFSTORE",
                    };
                    expect_success(conn.command(&[
                        op,
                        dst_key.as_bytes(),
                        key.as_bytes(),
                        other_key.as_bytes(),
                    ])?);
                }
            }
        },
        "zset" => match rng.range(4) {
            0 => expect_success(conn.command(&[
                b"ZADD",
                key.as_bytes(),
                score.as_bytes(),
                value.as_slice(),
            ])?),
            1 => expect_success(conn.command(&[b"ZREM", key.as_bytes(), value.as_slice()])?),
            n => {
                if let Some(other_key) = find_bg_key_with_type(conn, rng, "zset")? {
                    let op = if n == 2 {
                        b"ZUNIONSTORE"
                    } else {
                        b"ZINTERSTORE"
                    };
                    expect_success(conn.command(&[
                        op,
                        dst_key.as_bytes(),
                        b"2",
                        key.as_bytes(),
                        other_key.as_bytes(),
                    ])?);
                }
            }
        },
        "hash" => {
            if rng.range(2) == 0 {
                expect_success(conn.command(&[
                    b"HSET",
                    key.as_bytes(),
                    field.as_slice(),
                    value.as_slice(),
                ])?);
            } else {
                expect_success(conn.command(&[b"HDEL", key.as_bytes(), field.as_slice()])?);
            }
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected TYPE reply in bg complex writer: {other}"),
            ));
        }
    }
    Ok(())
}

fn write_bg_complex_style_data(port: u16, db: u32, seed: u64, count: usize) -> io::Result<()> {
    let mut conn = RespConn::connect(port)?;
    expect_simple(
        conn.command(&[b"SELECT", db.to_string().as_bytes()])?,
        b"OK",
    );
    let mut rng = Lcg::new(seed);

    for _ in 0..count {
        write_bg_complex_style_op(&mut conn, &mut rng)?;
    }

    Ok(())
}

fn write_bg_complex_style_data_until(
    port: u16,
    db: u32,
    seed: u64,
    stop: Arc<AtomicBool>,
    max_ops: usize,
) -> io::Result<usize> {
    let mut conn = RespConn::connect(port)?;
    expect_simple(
        conn.command(&[b"SELECT", db.to_string().as_bytes()])?,
        b"OK",
    );
    let mut rng = Lcg::new(seed);
    let mut ops = 0;

    while ops < max_ops && !stop.load(Ordering::Relaxed) {
        write_bg_complex_style_op(&mut conn, &mut rng)?;
        ops += 1;
    }

    Ok(ops)
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
fn psync_no_reconnect_swapdb_fullsync_converges_under_mutating_write_load() {
    let master = TestServer::start("repl-psync-swapdb-master").expect("start master");
    let replica = TestServer::start("repl-psync-swapdb-replica").expect("start replica");
    configure_psync_surface_with_diskless_load(&master, &replica, b"swapdb");

    write_mutating_complex_data(master.port, 2000, 60).expect("seed master data");
    let mut writers = Vec::new();
    for worker in 0..3 {
        let master_port = master.port;
        writers.push(thread::spawn(move || {
            write_mutating_complex_data(master_port, 2100 + worker * 200, 80)
        }));
    }

    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica");
    expect_simple(
        replica_conn
            .command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
            .expect("SLAVEOF should reply"),
        b"OK",
    );

    wait_for_replica_up(&replica, &master).expect("replica should reach online state");
    for writer in writers {
        writer
            .join()
            .expect("writer thread should not panic")
            .expect("writer commands should succeed");
    }
    let digest = wait_for_digest_match(&master, &replica).expect("digests should converge");
    assert!(
        !digest.is_empty(),
        "DEBUG DIGEST should return a non-empty digest after swapdb full sync"
    );
}

#[test]
fn psync_no_reconnect_fullsync_converges_under_bg_complex_style_load() {
    let master = TestServer::start("repl-psync-bg-master").expect("start master");
    let replica = TestServer::start("repl-psync-bg-replica").expect("start replica");
    configure_psync_surface(&master, &replica);

    let mut master_conn = RespConn::connect(master.port).expect("connect master");
    expect_simple(
        master_conn
            .command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"2000"])
            .expect("CONFIG SET rdb-key-save-delay"),
        b"OK",
    );

    let mut writers = Vec::new();
    for (db, seed) in [(9, 0x9_u64), (11, 0x11_u64), (12, 0x12_u64)] {
        let master_port = master.port;
        writers.push(thread::spawn(move || {
            write_bg_complex_style_data(master_port, db, seed, 1_600)
        }));
    }
    thread::sleep(Duration::from_millis(50));

    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica");
    expect_simple(
        replica_conn
            .command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
            .expect("SLAVEOF should reply"),
        b"OK",
    );

    wait_for_replica_up(&replica, &master).expect("replica should reach online state");
    for writer in writers {
        writer
            .join()
            .expect("writer thread should not panic")
            .expect("bg complex-style writer commands should succeed");
    }
    let digest =
        wait_for_digest_match(&master, &replica).expect("bg complex-style digests should converge");
    assert!(
        !digest.is_empty(),
        "DEBUG DIGEST should return a non-empty digest after bg complex-style load"
    );
}

#[test]
fn psync_no_reconnect_fullsync_converges_when_bg_complex_writers_stop_after_online() {
    let master = TestServer::start("repl-psync-bg-stop-master").expect("start master");
    let replica = TestServer::start("repl-psync-bg-stop-replica").expect("start replica");
    configure_psync_surface(&master, &replica);

    let mut master_conn = RespConn::connect(master.port).expect("connect master");
    expect_simple(
        master_conn
            .command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"2000"])
            .expect("CONFIG SET rdb-key-save-delay"),
        b"OK",
    );

    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for (db, seed) in [(9, 0x90_u64), (11, 0x110_u64), (12, 0x120_u64)] {
        let master_port = master.port;
        let stop = Arc::clone(&stop);
        writers.push(thread::spawn(move || {
            write_bg_complex_style_data_until(master_port, db, seed, stop, 100_000)
        }));
    }
    thread::sleep(Duration::from_millis(50));

    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica");
    expect_simple(
        replica_conn
            .command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
            .expect("SLAVEOF should reply"),
        b"OK",
    );

    wait_for_replica_up(&replica, &master).expect("replica should reach online state");
    stop.store(true, Ordering::Relaxed);

    let mut total_ops = 0;
    for writer in writers {
        total_ops += writer
            .join()
            .expect("writer thread should not panic")
            .expect("stop-timed bg complex-style writer commands should succeed");
    }
    assert!(
        total_ops > 1_000,
        "stop-timed bg_complex workload should run long enough to stress full-sync catch-up; ops={total_ops}"
    );

    let digest = wait_for_digest_match(&master, &replica)
        .expect("stop-timed bg complex-style digests should converge");
    assert!(
        !digest.is_empty(),
        "DEBUG DIGEST should return a non-empty digest after stop-timed bg_complex load"
    );
}

#[test]
fn psync_no_reconnect_fullsync_replays_db0_final_list_pop() {
    let master = TestServer::start("repl-psync-list-pop-master").expect("start master");
    let replica = TestServer::start("repl-psync-list-pop-replica").expect("start replica");
    configure_psync_surface(&master, &replica);

    let mut master_conn = RespConn::connect(master.port).expect("connect master");
    expect_simple(
        master_conn
            .command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"500000"])
            .expect("CONFIG SET rdb-key-save-delay"),
        b"OK",
    );
    expect_success(
        master_conn
            .command(&[b"LPUSH", b"733", b"982874811618"])
            .expect("seed one-element DB 0 list"),
    );

    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica");
    expect_simple(
        replica_conn
            .command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
            .expect("SLAVEOF should reply"),
        b"OK",
    );
    wait_for_info_field(&master, b"persistence", "rdb_bgsave_in_progress", "1")
        .expect("master should keep replication BGSAVE open long enough for catch-up write");

    let popped = master_conn
        .command(&[b"LPOP", b"733"])
        .expect("final LPOP should reply");
    assert_eq!(
        popped,
        Frame::Bulk(Some(b"982874811618".to_vec())),
        "master should remove the exact one-element list value"
    );
    assert_eq!(
        master_conn
            .command(&[b"EXISTS", b"733"])
            .expect("EXISTS after final LPOP"),
        Frame::Integer(0),
        "master should delete the list key after final pop"
    );

    wait_for_replica_up(&replica, &master).expect("replica should reach online state");
    wait_for_digest_match(&master, &replica)
        .expect("replica should replay the catch-up final LPOP and converge");

    let mut replica_check = RespConn::connect(replica.port).expect("connect replica for EXISTS");
    assert_eq!(
        replica_check
            .command(&[b"EXISTS", b"733"])
            .expect("replica EXISTS after full sync"),
        Frame::Integer(0),
        "replica must not retain the DB 0 list after full-sync catch-up"
    );
}

fn assert_db11_final_hdel_catchup(prefix: &str, key: &[u8], field: &[u8], value: &[u8]) {
    let master_name = format!("repl-psync-{prefix}-master");
    let replica_name = format!("repl-psync-{prefix}-replica");
    let master = TestServer::start(&master_name).expect("start master");
    let replica = TestServer::start(&replica_name).expect("start replica");
    configure_psync_surface(&master, &replica);

    let mut master_conn = RespConn::connect(master.port).expect("connect master");
    expect_simple(
        master_conn
            .command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"500000"])
            .expect("CONFIG SET rdb-key-save-delay"),
        b"OK",
    );
    expect_simple(
        master_conn
            .command(&[b"SELECT", b"11"])
            .expect("SELECT DB 11"),
        b"OK",
    );
    expect_success(
        master_conn
            .command(&[b"HSET", key, field, value])
            .expect("seed one-field DB 11 hash"),
    );

    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica");
    expect_simple(
        replica_conn
            .command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
            .expect("SLAVEOF should reply"),
        b"OK",
    );
    wait_for_info_field(&master, b"persistence", "rdb_bgsave_in_progress", "1")
        .expect("master should keep replication BGSAVE open long enough for catch-up write");

    assert_eq!(
        master_conn
            .command(&[b"HDEL", key, field])
            .expect("final HDEL should reply"),
        Frame::Integer(1),
        "master should remove the exact one-field hash entry"
    );
    assert_eq!(
        master_conn
            .command(&[b"EXISTS", key])
            .expect("EXISTS after final HDEL"),
        Frame::Integer(0),
        "master should delete the hash key after final HDEL"
    );

    wait_for_replica_up(&replica, &master).expect("replica should reach online state");
    wait_for_digest_match(&master, &replica)
        .expect("replica should replay the catch-up final HDEL and converge");

    let mut replica_check = RespConn::connect(replica.port).expect("connect replica for EXISTS");
    expect_simple(
        replica_check
            .command(&[b"SELECT", b"11"])
            .expect("replica SELECT DB 11"),
        b"OK",
    );
    assert_eq!(
        replica_check
            .command(&[b"EXISTS", key])
            .expect("replica EXISTS after full sync"),
        Frame::Integer(0),
        "replica must not retain the DB 11 hash after full-sync catch-up"
    );
}

#[test]
fn psync_no_reconnect_fullsync_replays_db11_final_hdel() {
    assert_db11_final_hdel_catchup("numeric-hdel", b"700", b"980330930547", b"-784434765942");
}

#[test]
fn psync_no_reconnect_fullsync_replays_db11_binary_final_hdel() {
    assert_db11_final_hdel_catchup(
        "binary-hdel",
        b"bin-hash",
        b"\0field\r\n\"\\:980330930547",
        b"\0value\r\n-784434765942",
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

#[test]
fn psync_same_primary_reconnect_replays_db0_final_list_pop() {
    let master = TestServer::start("repl-reconnect-list-pop-master").expect("start master");
    let replica = TestServer::start("repl-reconnect-list-pop-replica").expect("start replica");
    configure_psync_surface(&master, &replica);

    let mut master_conn = RespConn::connect(master.port).expect("connect master");
    expect_success(
        master_conn
            .command(&[b"LPUSH", b"733", b"982874811618"])
            .expect("seed one-element DB 0 list"),
    );

    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica");
    expect_simple(
        replica_conn
            .command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
            .expect("SLAVEOF should reply"),
        b"OK",
    );
    wait_for_replica_up(&replica, &master).expect("initial full sync should finish");
    wait_for_digest_match(&master, &replica).expect("initial list should converge");
    expect_success(
        master_conn
            .command(&[b"SET", b"offset-anchor", b"1"])
            .expect("online anchor write should advance PSYNC offset"),
    );
    wait_for_digest_match(&master, &replica)
        .expect("replica should apply the online anchor before reconnect");

    let stats_before = wait_for_info_counter_at_least(&master, b"stats", "sync_full", 1)
        .expect("master should count initial full sync");
    let sync_full_before = info_u64(&stats_before, "sync_full").expect("sync_full field");
    let partial_ok_before = info_u64(&stats_before, "sync_partial_ok").unwrap_or(0);

    match master_conn
        .command(&[b"CLIENT", b"KILL", b"TYPE", b"replica"])
        .expect("CLIENT KILL TYPE replica should reply")
    {
        Frame::Integer(killed) if killed >= 1 => {}
        frame => panic!("expected to kill at least one replica client, got {frame:?}"),
    }
    wait_for_info_field(&master, b"replication", "connected_slaves", "0")
        .expect("master should observe the dropped replica connection");

    assert_eq!(
        master_conn
            .command(&[b"LPOP", b"733"])
            .expect("final LPOP while replica is reconnecting"),
        Frame::Bulk(Some(b"982874811618".to_vec())),
        "master should remove the exact one-element list value"
    );
    assert_eq!(
        master_conn
            .command(&[b"EXISTS", b"733"])
            .expect("EXISTS after final LPOP"),
        Frame::Integer(0),
        "master should delete the list key after final pop"
    );

    let stats_after =
        wait_for_info_counter_at_least(&master, b"stats", "sync_partial_ok", partial_ok_before + 1)
            .expect("same-primary reconnect should be accepted as partial resync");
    assert_eq!(
        info_u64(&stats_after, "sync_full"),
        Some(sync_full_before),
        "same-primary reconnect with retained final LPOP must not require another full sync"
    );

    wait_for_replica_up(&replica, &master).expect("replica should return online");
    wait_for_digest_match(&master, &replica)
        .expect("replica should replay final LPOP made during reconnect");
    let mut replica_check = RespConn::connect(replica.port).expect("connect replica for EXISTS");
    assert_eq!(
        replica_check
            .command(&[b"EXISTS", b"733"])
            .expect("replica EXISTS after reconnect"),
        Frame::Integer(0),
        "replica must not retain the DB 0 list after partial reconnect"
    );
}

#[test]
fn psync_replica_delayed_reconnect_after_client_kill_gets_continue() {
    let master = TestServer::start("repl-delay-master").expect("start master");
    let replica = TestServer::start("repl-delay-replica").expect("start replica");
    configure_psync_surface(&master, &replica);

    write_complex_data(master.port, 900, 25).expect("seed master data");
    let mut replica_conn = RespConn::connect(replica.port).expect("connect replica");
    expect_simple(
        replica_conn
            .command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
            .expect("SLAVEOF should reply"),
        b"OK",
    );
    wait_for_replica_up(&replica, &master).expect("initial full sync should finish");
    write_complex_data(master.port, 930, 10)
        .expect("online writes before delayed reconnect should advance PSYNC offset");
    wait_for_digest_match(&master, &replica)
        .expect("replica should apply online writes before delayed reconnect");

    let stats_before = wait_for_info_counter_at_least(&master, b"stats", "sync_full", 1)
        .expect("master should count initial full sync");
    let sync_full_before = info_u64(&stats_before, "sync_full").expect("sync_full field");
    let partial_ok_before = info_u64(&stats_before, "sync_partial_ok").unwrap_or(0);

    let primary_addr = format!("127.0.0.1:{}", master.port);
    for cycle in 0..2 {
        let master_port = master.port;
        let writer =
            thread::spawn(move || write_mutating_complex_data(master_port, 950 + cycle * 150, 80));
        let mut delayed = RespConn::connect_with_deadline(replica.port, Duration::from_secs(8))
            .expect("connect replica for delayed reconnect transaction");
        expect_simple(
            delayed.command(&[b"MULTI"]).expect("MULTI should reply"),
            b"OK",
        );
        expect_simple(
            delayed
                .command(&[b"CLIENT", b"KILL", primary_addr.as_bytes()])
                .expect("CLIENT KILL should queue"),
            b"QUEUED",
        );
        expect_simple(
            delayed
                .command(&[b"DEBUG", b"SLEEP", b"3"])
                .expect("DEBUG SLEEP should queue"),
            b"QUEUED",
        );
        let exec = delayed
            .command(&[b"EXEC"])
            .expect("EXEC should finish after DEBUG SLEEP");
        assert!(
            exec.text_lossy().contains("OK"),
            "delayed reconnect transaction should succeed, got {exec:?}"
        );
        writer
            .join()
            .expect("writer thread should not panic")
            .expect("writer commands should succeed");
        wait_for_info_counter_at_least(
            &master,
            b"stats",
            "sync_partial_ok",
            partial_ok_before + cycle as u64 + 1,
        )
        .expect("each delayed reconnect should be accepted as partial resync");
    }

    let stats_after =
        wait_for_info_counter_at_least(&master, b"stats", "sync_partial_ok", partial_ok_before + 2)
            .expect("delayed same-primary reconnect should be accepted as partial resync");
    assert_eq!(
        info_u64(&stats_after, "sync_full"),
        Some(sync_full_before),
        "delayed same-primary reconnect with retained backlog must not require another full sync"
    );

    wait_for_replica_up(&replica, &master).expect("replica should return online after delay");
    let digest = wait_for_digest_match(&master, &replica)
        .expect("replica should receive writes made during delayed reconnect");
    assert!(
        !digest.is_empty(),
        "DEBUG DIGEST should return a non-empty digest after delayed partial reconnect"
    );
}

#[test]
fn multi_replica_fullsync_under_write_load_converges_offsets_and_digests() {
    let master = TestServer::start("repl-multi-master").expect("start master");
    let replica_a = TestServer::start("repl-multi-a").expect("start replica a");
    let replica_b = TestServer::start("repl-multi-b").expect("start replica b");
    let replica_c = TestServer::start("repl-multi-c").expect("start replica c");
    let replicas: [&TestServer; 3] = [&replica_a, &replica_b, &replica_c];

    for replica in replicas {
        configure_psync_surface(&master, replica);
    }

    write_complex_data(master.port, 700, 80).expect("seed master data");
    let master_port = master.port;
    let writer = thread::spawn(move || write_complex_data(master_port, 780, 160));

    for replica in replicas {
        let mut conn = RespConn::connect(replica.port).expect("connect replica");
        expect_simple(
            conn.command(&[b"SLAVEOF", b"127.0.0.1", master.port.to_string().as_bytes()])
                .expect("SLAVEOF should reply"),
            b"OK",
        );
    }

    for replica in replicas {
        wait_for_replica_up(replica, &master).expect("replica should reach online state");
    }
    writer
        .join()
        .expect("writer thread should not panic")
        .expect("writer commands should succeed");

    wait_for_info_field(&master, b"replication", "replicas_waiting_psync", "0")
        .expect("master should not retain full-sync waiters after replicas are online");
    let offset = wait_for_replicas_offset_match(&master, &replicas)
        .expect("all replicas should acknowledge the final master offset");
    assert!(
        offset > 0,
        "write load should advance the replication offset"
    );

    let master_digest = debug_digest(&master).expect("master DEBUG DIGEST");
    assert!(
        !master_digest.is_empty(),
        "DEBUG DIGEST should return a non-empty digest after multi-replica load"
    );
    for replica in replicas {
        let replica_digest = wait_for_digest_match(&master, replica)
            .expect("replica digest should converge with master");
        assert_eq!(replica_digest, master_digest);
    }
}
