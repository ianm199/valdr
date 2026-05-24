//! TLS configuration for the redis-server binary.
//!
//! Builds a `rustls::ServerConfig` from PEM-encoded certificate, private-key
//! and optional CA certificate files. The resulting `TlsConfig` is cheap to
//! clone (the inner `ServerConfig` lives behind `Arc`) and is passed to the
//! per-listener accept thread in `redis-server::main`.
//!
//! mTLS (mutual TLS / client certificate validation) is enabled when
//! `require_client_cert` is `true`. In that mode the server demands a valid
//! certificate chain from every connecting client; connections that do not
//! present one are rejected at the TLS handshake.
//!
//! # Dynamic TLS listener startup
//!
//! `CONFIG SET tls-port <N>` must be able to start a new TLS listener at
//! runtime. Because `apply_config_set` in `redis-commands` has no direct
//! access to main's socket machinery, `main.rs` installs a callback via
//! `install_tls_start_hook`. When `notify_tls_port_set` is called (from
//! `apply_config_set`), it invokes that callback with the new port number.
//! The callback is responsible for loading the TLS config from `LiveConfig`
//! and spawning the accept thread.

use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::{Arc, OnceLock};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::RootCertStore;

static TLS_START_HOOK: OnceLock<Box<dyn Fn(u16) + Send + Sync>> = OnceLock::new();

/// Install the runtime hook that starts (or attempts to start) a TLS listener
/// on the given port. `main.rs` calls this exactly once after the plain TCP
/// listener is bound. The hook captures everything it needs (live config, DB,
/// registry, etc.) via `Arc` clones from within the closure.
pub fn install_tls_start_hook(hook: Box<dyn Fn(u16) + Send + Sync>) {
    let _ = TLS_START_HOOK.set(hook);
}

/// Notify the TLS subsystem that `tls-port` was changed via CONFIG SET.
///
/// If `main.rs` has installed a hook (via `install_tls_start_hook`), it is
/// invoked with the new port number. When `port` is 0 the hook is still
/// called; the hook implementation treats 0 as "disable" and takes no action.
/// If no hook is installed yet (startup race), this is a silent no-op — the
/// correct port will be read from `LiveConfig` at listener-bind time.
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
    /// Build a `TlsConfig` from PEM files on disk.
    ///
    /// `cert_path`  — PEM certificate chain (leaf first).
    /// `key_path`   — PEM private key corresponding to the leaf certificate.
    /// `ca_path`    — Optional PEM CA bundle used to verify client certs when
    ///                `require_client_cert` is `true`. Ignored otherwise.
    /// `require_client_cert` — When `true`, the server performs mTLS: every
    ///                client must present a valid certificate signed by the CA.
    pub fn from_paths(
        cert_path: &Path,
        key_path: &Path,
        ca_path: Option<&Path>,
        require_client_cert: bool,
    ) -> io::Result<Self> {
        let cert_chain = load_certs(cert_path)?;
        let private_key = load_private_key(key_path)?;

        let builder = rustls::ServerConfig::builder();

        let server_config = if require_client_cert {
            let ca = ca_path.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "tls-auth-clients yes requires tls-ca-cert-file to be set",
                )
            })?;
            let mut root_store = RootCertStore::empty();
            for cert in load_certs(ca)? {
                root_store.add(cert).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("CA cert error: {e}"))
                })?;
            }
            let verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
                .build()
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
                    io::Error::new(io::ErrorKind::InvalidData, format!("TLS config error: {e}"))
                })?
        } else {
            builder
                .with_no_client_auth()
                .with_single_cert(cert_chain, private_key)
                .map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("TLS config error: {e}"))
                })?
        };

        Ok(Self {
            server_config: Arc::new(server_config),
        })
    }
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
        let result = TlsConfig::from_paths(&cert, &key, None, false);
        assert!(result.is_err(), "should fail with invalid cert/key data");
    }

    #[test]
    fn from_paths_returns_error_on_missing_file() {
        let result = TlsConfig::from_paths(
            std::path::Path::new("/nonexistent/cert.pem"),
            std::path::Path::new("/nonexistent/key.pem"),
            None,
            false,
        );
        assert!(result.is_err(), "should fail when cert file is missing");
    }

    #[test]
    fn mtls_without_ca_returns_error() {
        let cert = write_temp("mtls_cert.pem", b"not a cert");
        let key = write_temp("mtls_key.pem", b"not a key");
        let result = TlsConfig::from_paths(&cert, &key, None, true);
        assert!(
            result.is_err(),
            "mtls without CA path should return an error"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:        Session 2B (TLS support)
//   target_crate:  redis-core
//   confidence:    high
//   todos:         1  (SNI; cipher-suite selection from live config)
//   port_notes:    0
//   unsafe_blocks: 0
//   notes:         Builds rustls ServerConfig from PEM files. Supports
//                  optional mTLS client verification via WebPkiClientVerifier.
// ──────────────────────────────────────────────────────────────────────────
