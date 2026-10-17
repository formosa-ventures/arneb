//! TLS for the client-facing listeners (pgwire and the Trino client protocol).
//!
//! One PEM certificate chain and private key (the `[tls]` config section) is
//! loaded once at startup into two rustls server configs that differ only in
//! ALPN: `postgresql` for pgwire (PostgreSQL 17 direct TLS) and
//! `h2` / `http/1.1` for the Trino listener. rustls runs on the aws-lc-rs
//! provider, the same one pgwire and the SCRAM code already use.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// ALPN protocol id for PostgreSQL direct TLS.
pub const PGWIRE_ALPN: &[u8] = b"postgresql";

/// Errors raised while loading the `[tls]` certificate and key.
#[derive(Debug, thiserror::Error)]
pub enum TlsConfigError {
    /// A PEM file could not be read or parsed.
    #[error("[tls] cannot read {kind} file {path}: {reason}")]
    Read {
        kind: &'static str,
        path: PathBuf,
        reason: String,
    },
    /// The certificate file holds no certificates.
    #[error("[tls] certificate file {0} contains no PEM certificates")]
    EmptyChain(PathBuf),
    /// rustls rejected the key material (e.g. the key does not match the
    /// certificate).
    #[error("[tls] invalid certificate/key pair ({cert} / {key}): {reason}")]
    Invalid {
        cert: PathBuf,
        key: PathBuf,
        reason: String,
    },
}

/// How the pgwire listener treats clients that do not negotiate TLS.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PgwireTlsMode {
    /// Reject plaintext connections with `FATAL 28000` (default).
    #[default]
    Required,
    /// Accept both TLS and plaintext connections.
    Optional,
}

impl PgwireTlsMode {
    /// Config spelling of the mode.
    pub fn name(self) -> &'static str {
        match self {
            PgwireTlsMode::Required => "required",
            PgwireTlsMode::Optional => "optional",
        }
    }
}

impl std::str::FromStr for PgwireTlsMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "required" => Ok(PgwireTlsMode::Required),
            "optional" => Ok(PgwireTlsMode::Optional),
            other => Err(format!(
                "invalid pgwire TLS mode '{other}' (expected \"required\" or \"optional\")"
            )),
        }
    }
}

/// pgwire TLS settings handed to [`crate::ProtocolServer::with_tls`].
#[derive(Clone)]
pub struct PgwireTls {
    pub acceptor: TlsAcceptor,
    pub mode: PgwireTlsMode,
}

impl std::fmt::Debug for PgwireTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgwireTls")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

/// TLS acceptors for both client-facing listeners, built from one
/// certificate chain and key.
#[derive(Clone)]
pub struct ServerTls {
    pgwire: TlsAcceptor,
    trino: TlsAcceptor,
}

impl std::fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTls").finish_non_exhaustive()
    }
}

impl ServerTls {
    /// Load a PEM certificate chain (leaf first) and private key (PKCS#8,
    /// PKCS#1 or SEC1) and build the pgwire and Trino acceptors.
    pub fn load(cert_path: &Path, key_path: &Path) -> Result<Self, TlsConfigError> {
        let certs = load_certs(cert_path)?;
        let key = load_key(key_path)?;
        let build = |alpn: Vec<Vec<u8>>| {
            server_config(certs.clone(), key.clone_key(), alpn).map_err(|reason| {
                TlsConfigError::Invalid {
                    cert: cert_path.to_path_buf(),
                    key: key_path.to_path_buf(),
                    reason,
                }
            })
        };
        let pgwire = build(vec![PGWIRE_ALPN.to_vec()])?;
        let trino = build(vec![b"h2".to_vec(), b"http/1.1".to_vec()])?;
        Ok(Self {
            pgwire: TlsAcceptor::from(pgwire),
            trino: TlsAcceptor::from(trino),
        })
    }

    /// Acceptor for the pgwire listener (ALPN `postgresql`).
    pub fn pgwire_acceptor(&self) -> TlsAcceptor {
        self.pgwire.clone()
    }

    /// Acceptor for the Trino listener (ALPN `h2`, `http/1.1`).
    pub fn trino_acceptor(&self) -> TlsAcceptor {
        self.trino.clone()
    }
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let read_err = |reason: String| TlsConfigError::Read {
        kind: "certificate",
        path: path.to_path_buf(),
        reason,
    };
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| read_err(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| read_err(e.to_string()))?;
    if certs.is_empty() {
        return Err(TlsConfigError::EmptyChain(path.to_path_buf()));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| TlsConfigError::Read {
        kind: "private key",
        path: path.to_path_buf(),
        reason: e.to_string(),
    })
}

fn server_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    alpn: Vec<Vec<u8>>,
) -> Result<Arc<ServerConfig>, String> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| e.to_string())?;
    config.alpn_protocols = alpn;
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Pair {
        cert_pem: String,
        key_pem: String,
    }

    fn self_signed() -> Pair {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        Pair {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
        }
    }

    fn write(dir: &Path, name: &str, contents: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("arneb-tls-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn valid_pair_loads() {
        let dir = tempdir();
        let pair = self_signed();
        let cert = write(&dir, "c.pem", &pair.cert_pem);
        let key = write(&dir, "k.pem", &pair.key_pem);
        ServerTls::load(&cert, &key).unwrap();
    }

    #[test]
    fn missing_file_names_the_path() {
        let dir = tempdir();
        let pair = self_signed();
        let key = write(&dir, "k.pem", &pair.key_pem);
        let missing = dir.join("nope.pem");
        let err = ServerTls::load(&missing, &key).unwrap_err();
        assert!(matches!(
            err,
            TlsConfigError::Read {
                kind: "certificate",
                ..
            }
        ));
        assert!(err.to_string().contains("nope.pem"), "{err}");
    }

    #[test]
    fn empty_pem_is_rejected() {
        let dir = tempdir();
        let pair = self_signed();
        let cert = write(&dir, "c.pem", "");
        let key = write(&dir, "k.pem", &pair.key_pem);
        assert!(matches!(
            ServerTls::load(&cert, &key).unwrap_err(),
            TlsConfigError::EmptyChain(_)
        ));
        let cert = write(&dir, "c2.pem", &pair.cert_pem);
        let key = write(&dir, "k2.pem", "");
        assert!(matches!(
            ServerTls::load(&cert, &key).unwrap_err(),
            TlsConfigError::Read {
                kind: "private key",
                ..
            }
        ));
    }

    #[test]
    fn mismatched_key_is_rejected() {
        let dir = tempdir();
        let a = self_signed();
        let b = self_signed();
        let cert = write(&dir, "c.pem", &a.cert_pem);
        let key = write(&dir, "k.pem", &b.key_pem);
        assert!(matches!(
            ServerTls::load(&cert, &key).unwrap_err(),
            TlsConfigError::Invalid { .. }
        ));
    }

    #[test]
    fn pgwire_mode_parses() {
        assert_eq!(
            "required".parse::<PgwireTlsMode>().unwrap(),
            PgwireTlsMode::Required
        );
        assert_eq!(
            " Optional ".parse::<PgwireTlsMode>().unwrap(),
            PgwireTlsMode::Optional
        );
        let err = "prefer".parse::<PgwireTlsMode>().unwrap_err();
        assert!(
            err.contains("required") && err.contains("optional"),
            "{err}"
        );
    }
}
