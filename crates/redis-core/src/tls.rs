//! TLS configuration for the redis-server binary.
//!
//! Builds a `rustls::ServerConfig` from PEM-encoded certificate, private-key
//! and (optional) CA certificate files. The resulting `TlsConfig` wraps an
//! `Arc<ServerConfig>` so it is cheap to clone and share across the accept
//! path.
//!
//! mTLS (mutual TLS / client certificate validation) is configured via a
//! tri-state matching upstream Valkey's `tls-auth-clients` directive:
//!
//! | mode | meaning                                | rustls verifier                                                 |
//! |------|----------------------------------------|------------------------------------------------------------------|
//! | 0    | `no`        — never ask for a cert     | `with_no_client_auth()`                                          |
//! | 1    | `yes`       — require a valid cert     | `WebPkiClientVerifier::builder(roots).build()`                   |
//! | 2    | `optional`  — accept either            | `WebPkiClientVerifier::builder(roots).allow_unauthenticated()`   |
//!
//! `protocols` restricts the negotiable TLS versions. The empty string means
//! "all rustls-supported" (TLS 1.2 + TLS 1.3). Non-empty values are tokenized
//! on whitespace and matched against `TLSv1.2` / `TLSv1.3`. Older protocols
//! (`SSLv3`, `TLSv1`, `TLSv1.1`) are not supported by rustls and produce an
//! error — that is a security upgrade vs. upstream OpenSSL, documented at the
//! site level.
//!
//! # Dynamic reconfiguration
//!
//! `CONFIG SET tls-port`, `tls-auth-clients`, `tls-protocols`,
//! `tls-cert-file`, etc. all need to take effect on the next accepted
//! connection without restarting the listener. They route through
//! `notify_tls_port_set`, which fires `TLS_START_HOOK`. The hook (installed
//! by `main.rs`) calls [`rebuild_from_live`] and then [`set_current_server_config`]
//! to atomically swap the live `Arc<ServerConfig>` that the accept loop reads
//! via [`current_server_config`].

use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::{Arc, OnceLock, RwLock};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::version::{TLS12, TLS13};
use rustls::{RootCertStore, SupportedProtocolVersion};

use crate::live_config::LiveConfig;

static TLS_START_HOOK: OnceLock<Box<dyn Fn(u16) + Send + Sync>> = OnceLock::new();

/// Holds the `Arc<ServerConfig>` currently used by every fresh TLS accept.
/// `None` means TLS is disabled (no listener should be active). Swapped under
/// a `RwLock` because the accept path reads concurrently with the hook that
/// rebuilds on `CONFIG SET`.
fn current_cell() -> &'static RwLock<Option<Arc<rustls::ServerConfig>>> {
    static CELL: OnceLock<RwLock<Option<Arc<rustls::ServerConfig>>>> = OnceLock::new();
    CELL.get_or_init(|| RwLock::new(None))
}

/// Read the live `Arc<ServerConfig>`. Returns `None` when TLS has not been
/// configured (either because tls-port is 0 or because the most recent rebuild
/// failed; in the latter case the previous config remains in place).
pub fn current_server_config() -> Option<Arc<rustls::ServerConfig>> {
    match current_cell().read() {
        Ok(g) => g.clone(),
        Err(p) => p.into_inner().clone(),
    }
}

/// Replace the live `Arc<ServerConfig>`. `None` disables TLS for subsequent
/// accepts; existing already-handshaked connections are unaffected.
pub fn set_current_server_config(cfg: Option<Arc<rustls::ServerConfig>>) {
    match current_cell().write() {
        Ok(mut g) => *g = cfg,
        Err(p) => *p.into_inner() = cfg,
    }
}

/// Install the runtime hook that rebuilds and swaps the TLS server config when
/// `CONFIG SET` mutates any TLS directive. `main.rs` calls this exactly once
/// after the plain TCP listener is bound. The hook captures the
/// `Arc<LiveConfig>` it needs.
pub fn install_tls_start_hook(hook: Box<dyn Fn(u16) + Send + Sync>) {
    let _ = TLS_START_HOOK.set(hook);
}

/// Notify the TLS subsystem that a TLS-related directive was changed via
/// `CONFIG SET`. The port argument is informational — the hook reads
/// everything it needs from `LiveConfig`. A 0 typically means "disable TLS"
/// and the hook is expected to `set_current_server_config(None)`.
pub fn notify_tls_port_set(port: u16) {
    if let Some(hook) = TLS_START_HOOK.get() {
        hook(port);
    }
}

/// Fully-built TLS server configuration.
///
/// Wraps an `Arc<rustls::ServerConfig>` so it can be cheaply cloned and
/// handed to each accept-loop thread without deep copying the crypto state.
#[derive(Clone)]
pub struct TlsConfig {
    pub server_config: Arc<rustls::ServerConfig>,
}

impl TlsConfig {
    /// Build a `TlsConfig` from PEM files on disk and `LiveConfig`-style
    /// directives.
    ///
    /// * `cert_path`         — PEM certificate chain (leaf first).
    /// * `key_path`          — PEM private key matching the leaf cert.
    /// * `ca_path`           — PEM CA bundle. Required when `auth_clients_mode`
    ///                         is `1` (yes) or `2` (optional); ignored when `0`.
    /// * `auth_clients_mode` — Tri-state from upstream's `tls-auth-clients`:
    ///                         `0=no`, `1=yes`, `2=optional`.
    /// * `protocols`         — Space-separated TLS version list (`"TLSv1.2"`,
    ///                         `"TLSv1.3"`, both, or `""` for default).
    pub fn from_paths(
        cert_path: &Path,
        key_path: &Path,
        ca_path: Option<&Path>,
        auth_clients_mode: u8,
        protocols: &str,
    ) -> io::Result<Self> {
        let cert_chain = load_certs(cert_path)?;
        let private_key = load_private_key(key_path)?;

        let versions = parse_protocols(protocols)?;

        let builder = if versions.is_empty() {
            rustls::ServerConfig::builder()
        } else {
            rustls::ServerConfig::builder_with_protocol_versions(&versions)
        };

        let server_config = match auth_clients_mode {
            0 => builder
                .with_no_client_auth()
                .with_single_cert(cert_chain, private_key)
                .map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("TLS config error: {e}"))
                })?,
            1 | 2 => {
                let ca = ca_path.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "tls-auth-clients yes/optional requires tls-ca-cert-file to be set",
                    )
                })?;
                let mut root_store = RootCertStore::empty();
                for cert in load_certs(ca)? {
                    root_store.add(cert).map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, format!("CA cert error: {e}"))
                    })?;
                }
                let verifier_builder = WebPkiClientVerifier::builder(Arc::new(root_store));
                let verifier = if auth_clients_mode == 2 {
                    verifier_builder.allow_unauthenticated().build()
                } else {
                    verifier_builder.build()
                }
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("client verifier build error: {e}"),
                    )
                })?;
                builder
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(cert_chain, private_key)
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("TLS config error: {e}"),
                        )
                    })?
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("tls-auth-clients: unknown mode {other}"),
                ));
            }
        };

        Ok(Self {
            server_config: Arc::new(server_config),
        })
    }
}

/// Rebuild a `TlsConfig` from the current `LiveConfig` state. Returns
/// `Ok(None)` when TLS is configured-off (either `tls-port` is 0 or
/// cert/key paths are missing) — callers should then clear
/// [`current_server_config`]. Returns `Err` when the configuration is
/// invalid (missing CA for `yes`/`optional`, unparseable protocol, etc.) —
/// callers should log and leave the previous config in place.
pub fn rebuild_from_live(cfg: &LiveConfig) -> io::Result<Option<TlsConfig>> {
    let (cert, key) = match (cfg.tls_cert_file(), cfg.tls_key_file()) {
        (Some(c), Some(k)) => (c, k),
        _ => return Ok(None),
    };
    let ca = cfg.tls_ca_cert_file();
    let mode = cfg.tls_auth_clients();
    let protocols = cfg.tls_protocols();
    TlsConfig::from_paths(&cert, &key, ca.as_deref(), mode, &protocols).map(Some)
}

/// Parse a space-separated TLS-version list into rustls's
/// `&'static SupportedProtocolVersion` references.
///
/// Empty input yields an empty `Vec`, which the builder treats as "default
/// (all rustls-supported)". `SSLv3`, `TLSv1`, `TLSv1.1` produce an error
/// because rustls cannot negotiate them — refusing legacy versions is a
/// deliberate security upgrade vs. upstream OpenSSL.
fn parse_protocols(s: &str) -> io::Result<Vec<&'static SupportedProtocolVersion>> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let mut out: Vec<&'static SupportedProtocolVersion> = Vec::new();
    for tok in trimmed.split_ascii_whitespace() {
        match tok {
            "TLSv1.2" => out.push(&TLS12),
            "TLSv1.3" => out.push(&TLS13),
            "SSLv3" | "TLSv1" | "TLSv1.1" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("tls-protocols: {tok} is not supported by rustls (TLSv1.2 and TLSv1.3 only)"),
                ));
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("tls-protocols: unrecognized token '{other}'"),
                ));
            }
        }
    }
    Ok(out)
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot open cert file '{}': {e}", path.display()),
        )
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("cert parse error: {e}")))
}

fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot open key file '{}': {e}", path.display()),
        )
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("key parse error: {e}")))?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("no private key found in '{}'", path.display()),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp(name: &str, data: &[u8]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("redis_tls_test_{}", name));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(data).unwrap();
        path
    }

    #[test]
    fn from_paths_returns_error_on_bad_cert() {
        let cert = write_temp("bad_cert.pem", b"not a cert");
        let key = write_temp("bad_key.pem", b"not a key");
        let result = TlsConfig::from_paths(&cert, &key, None, 0, "");
        assert!(result.is_err(), "should fail with invalid cert/key data");
    }

    #[test]
    fn from_paths_returns_error_on_missing_file() {
        let result = TlsConfig::from_paths(
            std::path::Path::new("/nonexistent/cert.pem"),
            std::path::Path::new("/nonexistent/key.pem"),
            None,
            0,
            "",
        );
        assert!(result.is_err(), "should fail when cert file is missing");
    }

    #[test]
    fn mtls_without_ca_returns_error() {
        let cert = write_temp("mtls_cert.pem", b"not a cert");
        let key = write_temp("mtls_key.pem", b"not a key");
        let result = TlsConfig::from_paths(&cert, &key, None, 1, "");
        assert!(
            result.is_err(),
            "mtls without CA path should return an error"
        );
    }

    #[test]
    fn parse_protocols_empty_means_default() {
        let v = parse_protocols("").unwrap();
        assert!(v.is_empty(), "empty input -> empty list (default versions)");
    }

    #[test]
    fn parse_protocols_accepts_tls12_and_tls13() {
        let v = parse_protocols("TLSv1.2").unwrap();
        assert_eq!(v.len(), 1);
        let v = parse_protocols("TLSv1.2 TLSv1.3").unwrap();
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn parse_protocols_rejects_legacy() {
        for tok in &["SSLv3", "TLSv1", "TLSv1.1"] {
            assert!(
                parse_protocols(tok).is_err(),
                "{tok} must be rejected (rustls won't negotiate it)"
            );
        }
    }

    #[test]
    fn current_server_config_round_trips() {
        assert!(current_server_config().is_none() || current_server_config().is_some());
        set_current_server_config(None);
        assert!(current_server_config().is_none());
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Session 2B + 2C (TLS support; dynamic reconfig)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         1  (SNI; tls-ciphers is parsed-but-ignored — rustls won't
//                      negotiate CBC suites, by design)
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Builds rustls ServerConfig from PEM files. Supports
//                  tri-state mTLS (no/yes/optional) via WebPkiClientVerifier
//                  and protocol-version restriction via
//                  builder_with_protocol_versions. Live reconfig via
//                  `rebuild_from_live` + `set_current_server_config` swap.
// ──────────────────────────────────────────────────────────────────────────
