//! Test certificates shared by the TLS integration tests: a throwaway CA and
//! a `localhost` / `127.0.0.1` leaf it signed, written to a temp directory.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};

pub struct TestPki {
    pub dir: PathBuf,
    pub ca_pem: String,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

impl TestPki {
    /// A fresh CA plus a leaf for `localhost` and `127.0.0.1`.
    pub fn new() -> Self {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "arneb test CA");
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::new(ca_params, ca_key);

        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf =
            rcgen::CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
                .unwrap()
                .signed_by(&leaf_key, &issuer)
                .unwrap();

        let dir = std::env::temp_dir().join(format!("arneb-pki-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("server.crt");
        let key_path = dir.join("server.key");
        std::fs::write(&cert_path, leaf.pem()).unwrap();
        std::fs::write(&key_path, leaf_key.serialize_pem()).unwrap();
        Self {
            dir,
            ca_pem: ca_cert.pem(),
            cert_path,
            key_path,
        }
    }

    /// The CA certificate in DER form.
    pub fn ca_der(&self) -> CertificateDer<'static> {
        CertificateDer::from_pem_slice(self.ca_pem.as_bytes()).unwrap()
    }

    /// A rustls client config that trusts only this CA.
    pub fn client_config(&self) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        roots.add(self.ca_der()).unwrap();
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth()
    }
}
