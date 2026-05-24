//! Connection layer framework.
//!
//! Port of `connection.c` (171 lines, 9 functions) and `connection.h`
//! (530 lines, many inline helpers). Translates Valkey's vtable-based
//! connection abstraction — a C `struct ConnectionType` of function pointers —
//! to the `ConnectionTypeTrait` trait and a `Connection` struct holding
//! per-connection runtime state.
//!
//! Concrete backends (TCP, Unix, TLS, RDMA) live in `socket.rs`, `unix.rs`,
//! `tls.rs`, and `rdma.rs` respectively and register themselves via
//! `conn_type_register`.
//!
//! # Architecture (PORT NOTE)
//!
//! C keeps a process-global `static ConnectionType *connTypes[CONN_TYPE_MAX]`
//! and dispatches every I/O operation through `conn->type->method(conn, ...)`.
//! Rust replaces this with a lazily-initialised
//! `OnceLock<Mutex<Vec<Option<Box<dyn ConnectionTypeTrait>>>>>` registry.
//! Dispatch wrappers (`conn_write`, `conn_read`, …) look up the correct
//! backend by `conn.type_id`.
//!
//! TODO(architect): the registry uses `Mutex` (single-threaded Phase 2).
//! Phase 3+ should measure whether `RwLock` or an immutable-after-init
//! `OnceLock` is more appropriate. A re-entrancy hazard exists if any
//! dispatch closure itself calls back into registry functions — the same
//! thread would deadlock on the `Mutex`. Consider `Arc<dyn ConnectionTypeTrait>`
//! per-slot as a lock-free hot path for Phase 3+.

use std::io::IoSlice;
use std::sync::{Mutex, OnceLock};

use redis_types::{RedisError, RedisString};

// ─── Constants ────────────────────────────────────────────────────────────────

pub const CONN_INFO_LEN: usize = 32;
pub const CONN_ADDR_STR_LEN: usize = 128;
/// Longest valid hostname (C: `NET_HOST_STR_LEN`).
pub const NET_HOST_STR_LEN: usize = 256;
/// Enough for an IPv6 address string (C: `NET_IP_STR_LEN`).
pub const NET_IP_STR_LEN: usize = 46;
/// Must accommodate `hostname:port` (C: `NET_HOST_PORT_STR_LEN`).
pub const NET_HOST_PORT_STR_LEN: usize = NET_HOST_STR_LEN + 32;
/// Maximum number of bind-address entries per listener (C: `CONFIG_BINDADDR_MAX`).
pub const CONFIG_BINDADDR_MAX: usize = 16;
/// Total connection-type registry slots. Mirrors C `CONN_TYPE_MAX`.
pub const CONN_TYPE_MAX: usize = 4;

// ─── Connection flags ─────────────────────────────────────────────────────────

/// Close scheduled by a handler; physical close deferred.
pub const CONN_FLAG_CLOSE_SCHEDULED: u16 = 1 << 0;
/// Write barrier active: suppress write event if a read fired in the same loop tick.
pub const CONN_FLAG_WRITE_BARRIER: u16 = 1 << 1;
/// Accept may be offloaded to an IO thread.
pub const CONN_FLAG_ALLOW_ACCEPT_OFFLOAD: u16 = 1 << 2;

// ─── ConnectionState ─────────────────────────────────────────────────────────

/// Per-connection lifecycle state. Mirrors C `ConnectionState` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionState {
    #[default]
    None = 0,
    Connecting,
    Accepting,
    Connected,
    Closed,
    Error,
}

// ─── ConnectionTypeId ─────────────────────────────────────────────────────────

/// Discriminant for connection-type backends.
///
/// C: `ConnectionTypeId` enum + `CONN_TYPE_*` integer constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ConnectionTypeId {
    Invalid = -1,
    /// Plain TCP socket (`CONN_TYPE_SOCKET`).
    Socket = 0,
    /// Unix-domain socket (`CONN_TYPE_UNIX`).
    Unix = 1,
    /// TLS over TCP (`CONN_TYPE_TLS`). May be absent when built without TLS.
    Tls = 2,
    /// RDMA (`CONN_TYPE_RDMA`). May be absent when built without RDMA support.
    Rdma = 3,
}

impl ConnectionTypeId {
    /// Convert to the registry slot index. Returns `None` for `Invalid`.
    pub fn as_slot(self) -> Option<usize> {
        match self {
            Self::Invalid => None,
            Self::Socket => Some(0),
            Self::Unix => Some(1),
            Self::Tls => Some(2),
            Self::Rdma => Some(3),
        }
    }

    /// Human-readable name matching the C `getConnectionTypeName` inline function.
    pub fn name(self) -> &'static str {
        match self {
            Self::Socket => "tcp",
            Self::Unix => "unix",
            Self::Tls => "tls",
            Self::Rdma => "rdma",
            Self::Invalid => "invalid type",
        }
    }
}

// ─── Callback type alias ──────────────────────────────────────────────────────

/// Function-pointer callback for connection events.
///
/// C: `typedef void (*ConnectionCallbackFunc)(struct connection *conn)`
pub type ConnectionCallbackFunc = fn(&mut Connection);

// ─── ConnectionTypeTrait ─────────────────────────────────────────────────────

/// Vtable trait for connection-type backends.
///
/// PORT NOTE: C expressed the vtable as `struct ConnectionType` containing raw
/// function pointers. Rust expresses it as a trait with dynamic dispatch.
///
/// Methods that modify backend-global state (`init`, `cleanup`, `configure`)
/// take `&mut self`. All per-connection I/O methods take `&self` because the
/// mutable state lives in `Connection`, not in the backend.
///
/// TODO(port): `ae_handler` and `accept_handler` (event-loop callbacks) are
/// omitted. Their Rust type depends on the event-loop design, which is a
/// Phase 2 architect decision (`mio` vs `tokio`).
///
/// TODO(port): `get_peer_user` is omitted. It returns a `*user` ACL record;
/// the `User` type does not yet exist (`acl.rs` is phase: later).
pub trait ConnectionTypeTrait: Send + Sync {
    /// Return this backend's type discriminant.
    fn get_type_id(&self) -> ConnectionTypeId;

    /// Per-backend global initialisation, called automatically on registration.
    fn init(&mut self) {}

    /// Per-backend global cleanup, called on server shutdown.
    fn cleanup(&mut self) {}

    /// (Re)configure backend-level options (e.g. TLS certificates).
    /// `reconfigure = true` means overwrite existing configuration.
    fn configure(&mut self, reconfigure: bool) -> Result<(), RedisError>;

    /// Fill in the local (`remote = false`) or remote (`remote = true`) address.
    /// Returns `(ip_bytes, port)`.
    fn addr(&self, conn: &Connection, remote: bool) -> Result<(Vec<u8>, u16), RedisError>;

    /// Return `Some(true)` for local/loopback, `Some(false)` for remote,
    /// or `None` on failure.
    fn is_local(&self, conn: &Connection) -> Option<bool>;

    /// Start listening on the addresses bound in `listener`.
    fn listen(&self, listener: &mut ConnListener) -> Result<(), RedisError>;

    /// Close all file descriptors opened by `listen`.
    fn close_listener(&self, listener: &mut ConnListener);

    /// Allocate a new outbound `Connection` of this type.
    fn conn_create(&self) -> Connection;

    /// Allocate a `Connection` wrapping an already-accepted fd.
    fn conn_create_accepted(&self, fd: i32) -> Connection;

    /// Graceful shutdown (half-close / drain).
    fn shutdown(&self, conn: &mut Connection);

    /// Hard close.
    fn close(&self, conn: &mut Connection);

    /// Start an async outbound connect. `connect_handler` is called on completion or error.
    fn connect(
        &self,
        conn: &mut Connection,
        addr: &[u8],
        port: u16,
        source_addr: Option<&[u8]>,
        multipath: bool,
        connect_handler: ConnectionCallbackFunc,
    ) -> Result<(), RedisError>;

    /// Blocking outbound connect with a millisecond timeout.
    fn blocking_connect(
        &self,
        conn: &mut Connection,
        addr: &[u8],
        port: u16,
        timeout_ms: i64,
    ) -> Result<(), RedisError>;

    /// Accept an inbound connection; calls `accept_handler` when the handshake completes.
    fn accept(
        &self,
        conn: &mut Connection,
        accept_handler: ConnectionCallbackFunc,
    ) -> Result<(), RedisError>;

    /// Write `data` to the connection. Short writes are possible.
    /// Returns bytes written.
    fn write(&self, conn: &mut Connection, data: &[u8]) -> Result<usize, RedisError>;

    /// Scatter-gather write. Short writes are possible. Returns bytes written.
    fn writev(&self, conn: &mut Connection, iov: &[IoSlice<'_>]) -> Result<usize, RedisError>;

    /// Read from the connection into `buf`. Short reads are possible.
    /// Returns bytes read; `0` indicates EOF.
    fn read(&self, conn: &mut Connection, buf: &mut [u8]) -> Result<usize, RedisError>;

    /// Register (or clear with `None`) a write-ready handler.
    /// `barrier = true` suppresses write events when a read already fired this tick.
    fn set_write_handler(
        &self,
        conn: &mut Connection,
        handler: Option<ConnectionCallbackFunc>,
        barrier: bool,
    ) -> Result<(), RedisError>;

    /// Register (or clear with `None`) a read-ready handler.
    fn set_read_handler(
        &self,
        conn: &mut Connection,
        handler: Option<ConnectionCallbackFunc>,
    ) -> Result<(), RedisError>;

    /// Return the last transport-level error bytes, if any.
    fn get_last_error(&self, conn: &Connection) -> Option<Vec<u8>>;

    /// Blocking write with millisecond timeout.
    fn sync_write(
        &self,
        conn: &mut Connection,
        data: &[u8],
        timeout_ms: i64,
    ) -> Result<isize, RedisError>;

    /// Blocking read with millisecond timeout.
    fn sync_read(
        &self,
        conn: &mut Connection,
        buf: &mut [u8],
        timeout_ms: i64,
    ) -> Result<isize, RedisError>;

    /// Blocking line-read with millisecond timeout.
    fn sync_readline(
        &self,
        conn: &mut Connection,
        buf: &mut [u8],
        timeout_ms: i64,
    ) -> Result<isize, RedisError>;

    /// Return `true` if the backend has data buffered and not yet delivered.
    fn has_pending_data(&self) -> bool {
        false
    }

    /// Process buffered pending data. Returns items processed.
    ///
    /// TODO(architect): if the backend holds a mutable pending-data queue this
    /// will need `&mut self`; change `with_conn_type` call sites accordingly.
    fn process_pending_data(&self) -> i32 {
        0
    }

    /// Defer event-loop state updates to the main thread (IO threads + TLS).
    fn postpone_update_state(&self, _conn: &mut Connection, _on: bool) {}

    /// Apply deferred event-loop state updates (called from the main thread).
    fn update_state(&self, _conn: &mut Connection) {}

    /// Return PEM-encoded peer certificate bytes (TLS only).
    fn get_peer_cert(&self, _conn: &Connection) -> Option<RedisString> {
        None
    }

    /// Return `true` if this backend provides built-in integrity checks (e.g. RDMA CRC).
    fn integrity_checked(&self) -> bool {
        false
    }
}

// ─── Connection struct ────────────────────────────────────────────────────────

/// Per-connection runtime state.
///
/// PORT NOTE: The C `struct connection` carried a raw `ConnectionType *type`
/// pointer into the vtable. Here the type pointer is replaced by
/// `type_id: ConnectionTypeId`; all dispatch goes through the global registry.
///
/// TODO(port): C `void *private_data` is omitted. Phase B should add a typed
/// per-connection extension (e.g. `Option<Box<TlsState>>`) once concrete
/// backends are ported.
#[derive(Debug)]
pub struct Connection {
    /// Which registered backend manages this connection.
    pub type_id: ConnectionTypeId,
    pub state: ConnectionState,
    /// Last OS errno-equivalent from the transport layer.
    pub last_errno: i32,
    /// Underlying file descriptor.
    pub fd: i32,
    /// Bitmask of `CONN_FLAG_*` values.
    pub flags: u16,
    /// Reference count (mirrors C `short int refs`).
    pub refs: i16,
    /// Pending iovec count (mirrors C `unsigned short int iovcnt`).
    pub iovcnt: u16,
    /// General lifecycle callback (set by the accept/connect path).
    pub conn_handler: Option<ConnectionCallbackFunc>,
    /// Write-ready callback.
    pub write_handler: Option<ConnectionCallbackFunc>,
    /// Read-ready callback.
    pub read_handler: Option<ConnectionCallbackFunc>,
}

impl Connection {
    /// Construct a new connection with the given type discriminant and fd.
    pub fn new(type_id: ConnectionTypeId, fd: i32) -> Self {
        Self {
            type_id,
            state: ConnectionState::None,
            last_errno: 0,
            fd,
            flags: 0,
            refs: 0,
            iovcnt: 0,
            conn_handler: None,
            write_handler: None,
            read_handler: None,
        }
    }

    /// C: `connGetState` — return the current lifecycle state.
    pub fn get_state(&self) -> ConnectionState {
        self.state
    }

    /// C: `connHasWriteHandler` — true when a write-ready handler is installed.
    pub fn has_write_handler(&self) -> bool {
        self.write_handler.is_some()
    }

    /// C: `connHasReadHandler` — true when a read-ready handler is installed.
    pub fn has_read_handler(&self) -> bool {
        self.read_handler.is_some()
    }

    /// C: `connLastErrorRetryable` — true when the last error was EINTR.
    ///
    /// TODO(port): hard-codes EINTR = 4. Replace with `libc::EINTR` once the
    /// `libc` crate is added to redis-core's Cargo.toml.
    pub fn last_error_retryable(&self) -> bool {
        const EINTR: i32 = 4;
        self.last_errno == EINTR
    }

    /// C: `connGetType` — return the connection type discriminant.
    pub fn get_type(&self) -> ConnectionTypeId {
        self.type_id
    }

    /// C: `connIsTLS` — true when the connection is managed by the TLS backend.
    pub fn is_tls(&self) -> bool {
        self.type_id == ConnectionTypeId::Tls
    }

    /// C: `connGetInfo` — write a short descriptor (e.g. `fd=5`) into `buf`.
    ///
    /// PORT NOTE: C used `snprintf(buf, buf_len-1, "fd=%i", …)` into a stack
    /// char buffer. Rust writes into a `Vec<u8>` passed by the caller.
    pub fn get_info(&self, buf: &mut Vec<u8>) {
        buf.clear();
        let s = format!("fd={}", self.fd);
        buf.extend_from_slice(s.as_bytes());
    }
}

// ─── ConnListener struct ──────────────────────────────────────────────────────

/// Per-type listener binding: addresses, file descriptors, and port.
///
/// PORT NOTE: C `ConnectionType *ct` replaced by `conn_type_id: ConnectionTypeId`.
/// C `char **bindaddr` replaced by `Vec<Vec<u8>>` (byte strings, one per address).
/// C `void *priv` omitted — see TODO below.
#[derive(Debug)]
pub struct ConnListener {
    /// Up to `CONFIG_BINDADDR_MAX` open listening file descriptors.
    pub fd: [i32; CONFIG_BINDADDR_MAX],
    /// Number of active file descriptors in `fd`.
    pub count: i32,
    /// Bind-address strings as raw bytes (e.g. `b"127.0.0.1"`, `b"::"`).
    pub bindaddr: Vec<Vec<u8>>,
    pub bindaddr_count: i32,
    pub port: i32,
    /// Which connection-type backend manages this listener.
    pub conn_type_id: ConnectionTypeId,
    // TODO(port): C `void *priv` carried per-type extension data (e.g. a TLS
    // configuration pointer). Omit for Phase A; Phase B should add a typed
    // extension field for each backend.
}

// ─── Global connection-type registry ─────────────────────────────────────────

/// Lazily-initialised global registry of `ConnectionTypeTrait` backends.
///
/// C: `static ConnectionType *connTypes[CONN_TYPE_MAX]` in connection.c.
static CONN_REGISTRY: OnceLock<Mutex<Vec<Option<Box<dyn ConnectionTypeTrait>>>>> = OnceLock::new();

fn registry() -> &'static Mutex<Vec<Option<Box<dyn ConnectionTypeTrait>>>> {
    CONN_REGISTRY.get_or_init(|| {
        let mut slots: Vec<Option<Box<dyn ConnectionTypeTrait>>> =
            Vec::with_capacity(CONN_TYPE_MAX);
        for _ in 0..CONN_TYPE_MAX {
            slots.push(None);
        }
        Mutex::new(slots)
    })
}

// ─── Registry management ──────────────────────────────────────────────────────

/// Register a connection-type backend in the global registry.
///
/// Calls `ct.init()` immediately after registration, matching C behaviour.
///
/// C: `connTypeRegister` in connection.c:32-44
pub fn conn_type_register(mut ct: Box<dyn ConnectionTypeTrait>) -> Result<(), RedisError> {
    let type_id = ct.get_type_id();
    let slot = type_id
        .as_slot()
        .ok_or_else(|| RedisError::runtime(b"connTypeRegister: invalid connection type id"))?;

    ct.init();

    let mut reg = registry()
        .lock()
        .map_err(|_| RedisError::runtime(b"connTypeRegister: registry mutex poisoned"))?;

    if reg[slot].is_some() {
        return Err(RedisError::runtime(
            b"connTypeRegister: type already registered",
        ));
    }

    // PORT NOTE: C called serverLog(LL_VERBOSE, "Connection type %s registering", …).
    // Phase B should replace this with log::debug!.

    reg[slot] = Some(ct);
    Ok(())
}

/// Initialise all required connection-type backends.
///
/// C: `connTypeInitialize` in connection.c:46-60 — calls
/// `RedisRegisterConnectionTypeSocket`, `RedisRegisterConnectionTypeUnix`,
/// `RedisRegisterConnectionTypeTLS`, and `RegisterConnectionTypeRdma` defined
/// in `socket.c`, `unix.c`, `tls.c`, and `rdma.c` respectively.
///
/// TODO(port): registration calls are deferred until `socket.rs`, `unix.rs`,
/// `tls.rs`, and `rdma.rs` are ported (phase: later). Returns an error
/// placeholder so callers know this is not yet wired up.
pub fn conn_type_initialize() -> Result<(), RedisError> {
    Err(RedisError::runtime(
        b"conn_type_initialize: concrete backends not yet implemented",
    ))
}

/// Look up whether a backend for `type_id` is registered.
/// Returns `Ok(type_id)` if present, or `Err` with a warning if not.
///
/// C: `connectionByType` in connection.c:62-71
pub fn connection_by_type(type_id: ConnectionTypeId) -> Result<ConnectionTypeId, RedisError> {
    let slot = type_id
        .as_slot()
        .ok_or_else(|| RedisError::runtime(b"connectionByType: invalid type"))?;

    let reg = registry()
        .lock()
        .map_err(|_| RedisError::runtime(b"connectionByType: registry mutex poisoned"))?;

    if reg[slot].is_none() {
        // PORT NOTE: C called serverLog(LL_WARNING, "Missing implement of connection type %s", …).
        return Err(RedisError::runtime(
            b"connectionByType: type not registered",
        ));
    }

    Ok(type_id)
}

/// Dispatch `f` with a shared reference to the registered backend for `type_id`.
///
/// PORT NOTE: C returned a raw `ConnectionType *` and callers used it freely.
/// Rust uses a callback to scope the registry lock to the dispatch call.
///
/// TODO(architect): if `f` calls back into registry functions (e.g. to create
/// another connection), this will deadlock on the `Mutex`. Phase 3+ should
/// consider `Arc<dyn ConnectionTypeTrait>` per slot to allow lock-free reads.
fn with_conn_type<F, R>(type_id: ConnectionTypeId, f: F) -> Result<R, RedisError>
where
    F: FnOnce(&dyn ConnectionTypeTrait) -> R,
{
    let slot = type_id
        .as_slot()
        .ok_or_else(|| RedisError::runtime(b"with_conn_type: invalid type id"))?;

    let reg = registry()
        .lock()
        .map_err(|_| RedisError::runtime(b"with_conn_type: registry mutex poisoned"))?;

    match reg[slot].as_deref() {
        Some(ct) => Ok(f(ct)),
        None => Err(RedisError::runtime(
            b"with_conn_type: connection type not registered",
        )),
    }
}

/// Dispatch `f` with a mutable reference to the registered backend for `type_id`.
/// Used for operations that modify backend-global state (`configure`, cleanup).
fn with_conn_type_mut<F, R>(type_id: ConnectionTypeId, f: F) -> Result<R, RedisError>
where
    F: FnOnce(&mut dyn ConnectionTypeTrait) -> R,
{
    let slot = type_id
        .as_slot()
        .ok_or_else(|| RedisError::runtime(b"with_conn_type_mut: invalid type id"))?;

    let mut reg = registry()
        .lock()
        .map_err(|_| RedisError::runtime(b"with_conn_type_mut: registry mutex poisoned"))?;

    match reg[slot].as_deref_mut() {
        Some(ct) => Ok(f(ct)),
        None => Err(RedisError::runtime(
            b"with_conn_type_mut: connection type not registered",
        )),
    }
}

// ─── Cached type-handle accessors ─────────────────────────────────────────────

/// Return the TCP (socket) connection type discriminant.
///
/// C: `connectionTypeTcp` used a static local variable cache.
/// Rust: the global registry is already effectively cached after init.
///
/// C: connection.c:74-83
pub fn connection_type_tcp() -> Result<ConnectionTypeId, RedisError> {
    connection_by_type(ConnectionTypeId::Socket)
}

/// Return the TLS connection type discriminant, or `None` if TLS is absent.
///
/// Unlike TCP and Unix, TLS may legitimately be missing (built without TLS).
///
/// C: connection.c:85-98
pub fn connection_type_tls() -> Option<ConnectionTypeId> {
    connection_by_type(ConnectionTypeId::Tls).ok()
}

/// Return the Unix-socket connection type discriminant.
///
/// C: connection.c:100-108
pub fn connection_type_unix() -> Result<ConnectionTypeId, RedisError> {
    connection_by_type(ConnectionTypeId::Unix)
}

// ─── Cleanup and pending-data traversal ───────────────────────────────────────

/// Call `cleanup()` on every registered backend.
///
/// C: `connTypeCleanupAll` in connection.c:110-120
pub fn conn_type_cleanup_all() -> Result<(), RedisError> {
    let mut reg = registry()
        .lock()
        .map_err(|_| RedisError::runtime(b"conn_type_cleanup_all: registry mutex poisoned"))?;

    for slot in reg.iter_mut() {
        if let Some(ct) = slot.as_mut() {
            ct.cleanup();
        }
    }
    Ok(())
}

/// Return `true` if any registered backend has buffered pending data.
///
/// C: `connTypeHasPendingData` in connection.c:123-136
pub fn conn_type_has_pending_data() -> Result<bool, RedisError> {
    let reg = registry()
        .lock()
        .map_err(|_| RedisError::runtime(b"conn_type_has_pending_data: registry mutex poisoned"))?;

    for slot in reg.iter() {
        if let Some(ct) = slot.as_ref() {
            if ct.has_pending_data() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Process buffered pending data for all registered backends.
/// Returns the total items processed across all types.
///
/// C: `connTypeProcessPendingData` in connection.c:138-152
pub fn conn_type_process_pending_data() -> Result<i32, RedisError> {
    let reg = registry().lock().map_err(|_| {
        RedisError::runtime(b"conn_type_process_pending_data: registry mutex poisoned")
    })?;

    let mut total: i32 = 0;
    for slot in reg.iter() {
        if let Some(ct) = slot.as_ref() {
            total = total.saturating_add(ct.process_pending_data());
        }
    }
    Ok(total)
}

// ─── Listener info string ──────────────────────────────────────────────────────

/// Append listener information to `info` for all active listeners.
///
/// PORT NOTE: C read `server.listeners[j]` (a process global). Rust makes the
/// dependency explicit: callers pass the server's listener slice. This avoids
/// a hidden dependency on `RedisServer` and makes the function testable in
/// isolation.
///
/// C: `getListensInfoString` in connection.c:154-170
pub fn get_listens_info_string(info: &mut Vec<u8>, listeners: &[ConnListener]) {
    for (j, listener) in listeners.iter().enumerate() {
        if listener.count == 0 {
            continue;
        }

        let type_name = listener.conn_type_id.name();
        let header = format!("listener{}:name={}", j, type_name);
        info.extend_from_slice(header.as_bytes());

        for addr in &listener.bindaddr {
            info.extend_from_slice(b",bind=");
            info.extend_from_slice(addr);
        }

        if listener.port != 0 {
            let port_field = format!(",port={}", listener.port);
            info.extend_from_slice(port_field.as_bytes());
        }

        info.extend_from_slice(b"\r\n");
    }
}

// ─── Dispatch wrappers for inline helpers (from connection.h) ─────────────────

/// Accept an inbound connection, calling `accept_handler` when the handshake is done.
///
/// C: `connAccept` (inline) in connection.h:204-206
pub fn conn_accept(
    conn: &mut Connection,
    accept_handler: ConnectionCallbackFunc,
) -> Result<(), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.accept(conn, accept_handler))?
}

/// Initiate an async outbound connect; `connect_handler` is called on completion or error.
///
/// C: `connConnect` (inline) in connection.h:217-224
pub fn conn_connect(
    conn: &mut Connection,
    addr: &[u8],
    port: u16,
    src_addr: Option<&[u8]>,
    multipath: bool,
    connect_handler: ConnectionCallbackFunc,
) -> Result<(), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| {
        ct.connect(conn, addr, port, src_addr, multipath, connect_handler)
    })?
}

/// Blocking outbound connect with a millisecond timeout.
///
/// C: `connBlockingConnect` (inline) in connection.h:232-234
pub fn conn_blocking_connect(
    conn: &mut Connection,
    addr: &[u8],
    port: u16,
    timeout_ms: i64,
) -> Result<(), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| {
        ct.blocking_connect(conn, addr, port, timeout_ms)
    })?
}

/// Write bytes to the connection. Short write possible; see `ConnectionState` on error.
///
/// C: `connWrite` (inline) in connection.h:243-245
pub fn conn_write(conn: &mut Connection, data: &[u8]) -> Result<usize, RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.write(conn, data))?
}

/// Scatter-gather write. Short write possible.
///
/// C: `connWritev` (inline) in connection.h:255-257
pub fn conn_writev(conn: &mut Connection, iov: &[IoSlice<'_>]) -> Result<usize, RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.writev(conn, iov))?
}

/// Read bytes from the connection. Short read possible; `0` = EOF.
///
/// C: `connRead` (inline) in connection.h:267-270
pub fn conn_read(conn: &mut Connection, buf: &mut [u8]) -> Result<usize, RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.read(conn, buf))?
}

/// Register (or clear) the write-ready handler, no barrier.
///
/// C: `connSetWriteHandler` (inline) in connection.h:275-277
pub fn conn_set_write_handler(
    conn: &mut Connection,
    handler: Option<ConnectionCallbackFunc>,
) -> Result<(), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.set_write_handler(conn, handler, false))?
}

/// Register (or clear) the read-ready handler.
///
/// C: `connSetReadHandler` (inline) in connection.h:283-285
pub fn conn_set_read_handler(
    conn: &mut Connection,
    handler: Option<ConnectionCallbackFunc>,
) -> Result<(), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.set_read_handler(conn, handler))?
}

/// Register (or clear) the write-ready handler with an optional write barrier.
///
/// C: `connSetWriteHandlerWithBarrier` (inline) in connection.h:291-293
pub fn conn_set_write_handler_with_barrier(
    conn: &mut Connection,
    handler: Option<ConnectionCallbackFunc>,
    barrier: bool,
) -> Result<(), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.set_write_handler(conn, handler, barrier))?
}

/// Shutdown (graceful half-close) the connection.
///
/// C: `connShutdown` (inline) in connection.h:295-297
pub fn conn_shutdown(conn: &mut Connection) -> Result<(), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.shutdown(conn))?;
    Ok(())
}

/// Fully close the connection.
///
/// C: `connClose` (inline) in connection.h:299-301
pub fn conn_close(conn: &mut Connection) -> Result<(), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.close(conn))?;
    Ok(())
}

/// Return the last transport-level error, if any.
///
/// C: `connGetLastError` (inline) in connection.h:306-308
pub fn conn_get_last_error(conn: &Connection) -> Result<Option<Vec<u8>>, RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.get_last_error(conn))
}

/// Blocking write with millisecond timeout.
///
/// C: `connSyncWrite` (inline) in connection.h:310-312
pub fn conn_sync_write(
    conn: &mut Connection,
    data: &[u8],
    timeout_ms: i64,
) -> Result<isize, RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.sync_write(conn, data, timeout_ms))?
}

/// Blocking read with millisecond timeout.
///
/// C: `connSyncRead` (inline) in connection.h:314-316
pub fn conn_sync_read(
    conn: &mut Connection,
    buf: &mut [u8],
    timeout_ms: i64,
) -> Result<isize, RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.sync_read(conn, buf, timeout_ms))?
}

/// Blocking line-read with millisecond timeout.
///
/// C: `connSyncReadLine` (inline) in connection.h:318-320
pub fn conn_sync_readline(
    conn: &mut Connection,
    buf: &mut [u8],
    timeout_ms: i64,
) -> Result<isize, RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.sync_readline(conn, buf, timeout_ms))?
}

/// Get the peer (remote) address of the connection.
///
/// C: `connAddrPeerName` (inline) in connection.h:361-363
pub fn conn_addr_peer_name(conn: &Connection) -> Result<(Vec<u8>, u16), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.addr(conn, true))?
}

/// Get the local (socket) address of the connection.
///
/// C: `connAddrSockName` (inline) in connection.h:365-367
pub fn conn_addr_sock_name(conn: &Connection) -> Result<(Vec<u8>, u16), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.addr(conn, false))?
}

/// Test whether the connection is local/loopback.
///
/// C: `connIsLocal` (inline) in connection.h:371-377
pub fn conn_is_local(conn: &Connection) -> Result<Option<bool>, RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.is_local(conn))
}

/// Format `ip` and `port` into a printable address string.
///
/// IPv6 addresses are wrapped in `[…]`; e.g. `[::1]:6379` vs `127.0.0.1:6379`.
///
/// C: `formatAddr` (inline) in connection.h:346-348
pub fn format_addr(ip: &[u8], port: u16) -> Vec<u8> {
    let mut out = Vec::new();
    let is_ipv6 = ip.contains(&b':');
    if is_ipv6 {
        out.push(b'[');
        out.extend_from_slice(ip);
        out.push(b']');
    } else {
        out.extend_from_slice(ip);
    }
    let suffix = format!(":{}", port);
    out.extend_from_slice(suffix.as_bytes());
    out
}

/// Format the connection's local or remote address as a printable byte string.
///
/// PORT NOTE: C wrote into a caller-supplied `char buf[buf_len]`. Rust returns
/// an owned `Vec<u8>` since the output length is not known at the call site.
///
/// C: `connFormatAddr` (inline) in connection.h:350-358
pub fn conn_format_addr(conn: &Connection, remote: bool) -> Result<Vec<u8>, RedisError> {
    let type_id = conn.type_id;
    let addr_result = with_conn_type(type_id, |ct| ct.addr(conn, remote))?;
    let (ip, port) = addr_result?;
    Ok(format_addr(&ip, port))
}

/// Get the TLS peer certificate for this connection, if available.
///
/// C: `connGetPeerCert` (inline) in connection.h:421-427
pub fn conn_get_peer_cert(conn: &Connection) -> Result<Option<RedisString>, RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.get_peer_cert(conn))
}

/// Defer event-loop state updates to the main thread (IO threads + TLS path).
///
/// C: `connSetPostponeUpdateState` (inline) in connection.h:520-524
pub fn conn_set_postpone_update_state(conn: &mut Connection, on: bool) -> Result<(), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.postpone_update_state(conn, on))?;
    Ok(())
}

/// Apply deferred event-loop state updates (called from the main thread).
///
/// C: `connUpdateState` (inline) in connection.h:514-518
pub fn conn_update_state(conn: &mut Connection) -> Result<(), RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.update_state(conn))?;
    Ok(())
}

/// Return true if this connection's backend performs built-in integrity checks.
///
/// C: `connIsIntegrityChecked` (inline) in connection.h:526-528
pub fn conn_is_integrity_checked(conn: &Connection) -> Result<bool, RedisError> {
    let type_id = conn.type_id;
    with_conn_type(type_id, |ct| ct.integrity_checked())
}

/// Allocate a new outbound connection of the given backend type.
///
/// C: `connCreate` (inline) in connection.h:457-459
pub fn conn_create(type_id: ConnectionTypeId) -> Result<Connection, RedisError> {
    with_conn_type(type_id, |ct| ct.conn_create())
}

/// Allocate a connection wrapping an already-accepted fd.
///
/// C: `connCreateAccepted` (inline) in connection.h:462-465
pub fn conn_create_accepted(type_id: ConnectionTypeId, fd: i32) -> Result<Connection, RedisError> {
    with_conn_type(type_id, |ct| ct.conn_create_accepted(fd))
}

/// Configure a connection-type backend (e.g. load TLS certificates).
///
/// C: `connTypeConfigure` (inline) in connection.h:469-472
pub fn conn_type_configure(type_id: ConnectionTypeId, reconfigure: bool) -> Result<(), RedisError> {
    with_conn_type_mut(type_id, |ct| ct.configure(reconfigure))?
}

/// Start listening on the bound addresses in `listener`.
///
/// C: `connListen` (inline) in connection.h:484-486
pub fn conn_listen(listener: &mut ConnListener) -> Result<(), RedisError> {
    let type_id = listener.conn_type_id;
    with_conn_type(type_id, |ct| ct.listen(listener))?
}

/// Close all file descriptors opened by `conn_listen`, if any are active.
///
/// C: `connCloseListener` (inline) in connection.h:489-493
pub fn conn_close_listener(listener: &mut ConnListener) -> Result<(), RedisError> {
    if listener.count > 0 {
        let type_id = listener.conn_type_id;
        with_conn_type(type_id, |ct| ct.close_listener(listener))?;
    }
    Ok(())
}

/// Return the event-loop accept-handler function pointer for the given backend.
///
/// C: `connAcceptHandler` (inline) in connection.h:496-499
///
/// TODO(port): C returned `aeFileProc *` (a void fn pointer for the ae event
/// loop). The return type depends on the event-loop design (Phase 2 architect
/// decision). This stub returns the unit type as a placeholder.
pub fn conn_accept_handler(_type_id: ConnectionTypeId) {
    // TODO(port): return the backend's accept_handler fn pointer once the
    // event-loop type is decided (Phase 2).
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        src/connection.c  (171 lines, 9 functions)
//                  src/connection.h  (530 lines, merged)
//   target_crate:  redis-core
//   confidence:    medium
//   todos:         10
//   port_notes:    5
//   unsafe_blocks: 0
//   notes:         ConnectionType vtable → ConnectionTypeTrait; Connection
//                  carries type_id instead of raw vtable pointer; registry
//                  uses OnceLock<Mutex<...>>; concrete backend registration
//                  (socket/unix/tls/rdma) deferred to later phase.
// ──────────────────────────────────────────────────────────────────────────
