//! Cluster RPC security: a shared bearer secret on every Flight call and
//! optional TLS between nodes (`[cluster] secret`, `[cluster.tls]`).
//!
//! The server binary installs one [`ClusterSecurity`] at startup with
//! [`install_cluster_security`]; every outbound Flight connection
//! ([`crate::submit_task`], [`crate::send_heartbeat`],
//! [`crate::ExchangeClient`], the dynamic-filter RPCs) goes through
//! [`connect`], which adds `authorization: Bearer <secret>` and, with TLS,
//! switches `http://` peer addresses to `https://` and verifies the peer
//! against the cluster CA. [`crate::start_flight_server`] checks the secret
//! in an interceptor that runs before any handler, so heartbeat
//! registration, task submission, exchange and status calls are all
//! covered. With nothing installed, behavior is unchanged.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arrow_flight::flight_service_client::FlightServiceClient;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig,
};
use tonic::{Request, Status};

/// Minimum accepted length of `[cluster] secret`.
pub const MIN_CLUSTER_SECRET_LEN: usize = 32;

/// Shared secret, redacted from `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct ClusterSecret(String);

impl ClusterSecret {
    /// Wrap a secret, enforcing [`MIN_CLUSTER_SECRET_LEN`].
    pub fn new(secret: impl Into<String>) -> Result<Self, String> {
        let secret = secret.into();
        if secret.chars().count() < MIN_CLUSTER_SECRET_LEN {
            return Err(format!(
                "[cluster] secret must be at least {MIN_CLUSTER_SECRET_LEN} characters"
            ));
        }
        if MetadataValue::<Ascii>::try_from(format!("Bearer {secret}")).is_err() {
            return Err("[cluster] secret must be printable ASCII".to_owned());
        }
        Ok(Self(secret))
    }

    fn bearer(&self) -> MetadataValue<Ascii> {
        MetadataValue::try_from(format!("Bearer {}", self.0)).expect("validated in new")
    }
}

impl std::fmt::Debug for ClusterSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterSecret(<redacted>)")
    }
}

/// PEM material for `[cluster.tls]`.
#[derive(Clone)]
pub struct ClusterTls {
    /// This node's certificate chain (PEM).
    pub cert_pem: Vec<u8>,
    /// This node's private key (PEM).
    pub key_pem: Vec<u8>,
    /// CA that signed every node's certificate (PEM).
    pub ca_pem: Vec<u8>,
}

impl std::fmt::Debug for ClusterTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterTls").finish_non_exhaustive()
    }
}

/// Security settings for both directions of cluster RPC.
#[derive(Debug, Clone, Default)]
pub struct ClusterSecurity {
    pub secret: Option<ClusterSecret>,
    pub tls: Option<ClusterTls>,
}

impl ClusterSecurity {
    /// Server-side TLS config for [`crate::start_flight_server`].
    pub(crate) fn server_tls(&self) -> Option<ServerTlsConfig> {
        self.tls
            .as_ref()
            .map(|t| ServerTlsConfig::new().identity(Identity::from_pem(&t.cert_pem, &t.key_pem)))
    }
}

static INSTALLED: OnceLock<ClusterSecurity> = OnceLock::new();
static NONE: ClusterSecurity = ClusterSecurity {
    secret: None,
    tls: None,
};

/// Install the process-wide security used by outbound Flight calls. Call
/// once at startup, before any RPC; later calls are ignored (returns
/// `false`).
pub fn install_cluster_security(security: ClusterSecurity) -> bool {
    INSTALLED.set(security).is_ok()
}

/// The installed security, or none.
pub fn cluster_security() -> &'static ClusterSecurity {
    INSTALLED.get().unwrap_or(&NONE)
}

/// Adds the bearer secret to every outbound request.
#[derive(Clone)]
pub struct BearerInterceptor(Option<MetadataValue<Ascii>>);

impl Interceptor for BearerInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = &self.0 {
            req.metadata_mut().insert("authorization", value.clone());
        }
        Ok(req)
    }
}

/// Flight client carrying the cluster credentials.
pub type FlightClient = FlightServiceClient<InterceptedService<Channel, BearerInterceptor>>;

/// Open a Flight connection to `address` with `security`. With TLS, an
/// `http://` address is upgraded to `https://` and the peer must present a
/// certificate signed by the cluster CA.
pub async fn connect(address: &str, security: &ClusterSecurity) -> Result<FlightClient, String> {
    FlightPeer::new(address, security)?.connect().await
}

/// A Flight peer prepared once (address resolved for TLS, CA parsed,
/// bearer built), for a caller that connects to it more than once.
#[derive(Clone)]
pub(crate) struct FlightPeer {
    address: String,
    endpoint: Endpoint,
    interceptor: BearerInterceptor,
}

impl FlightPeer {
    pub(crate) fn new(address: &str, security: &ClusterSecurity) -> Result<Self, String> {
        let address = match (&security.tls, address.strip_prefix("http://")) {
            (Some(_), Some(rest)) => format!("https://{rest}"),
            _ => address.to_owned(),
        };
        let mut endpoint = Channel::from_shared(address.clone())
            .map_err(|e| format!("invalid flight address {address}: {e}"))?;
        if let Some(tls) = &security.tls {
            endpoint = endpoint
                .tls_config(
                    ClientTlsConfig::new().ca_certificate(Certificate::from_pem(&tls.ca_pem)),
                )
                .map_err(|e| format!("invalid [cluster.tls] client config: {e}"))?;
        }
        let interceptor = BearerInterceptor(security.secret.as_ref().map(ClusterSecret::bearer));
        Ok(Self {
            address,
            endpoint,
            interceptor,
        })
    }

    pub(crate) async fn connect(&self) -> Result<FlightClient, String> {
        let channel = self
            .endpoint
            .connect()
            .await
            .map_err(|e| format!("failed to connect to {}: {e}", self.address))?;
        Ok(FlightServiceClient::with_interceptor(
            channel,
            self.interceptor.clone(),
        ))
    }
}

/// Constant-time byte comparison (length is not secret).
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

const REJECT_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Server-side check of the bearer secret; runs before every handler.
/// Without a secret it passes everything through.
#[derive(Clone)]
pub struct SecretCheck {
    expected: Option<Arc<MetadataValue<Ascii>>>,
    last_logged: Arc<Mutex<HashMap<Option<IpAddr>, Instant>>>,
}

impl SecretCheck {
    pub(crate) fn new(secret: Option<&ClusterSecret>) -> Self {
        Self {
            expected: secret.map(|s| Arc::new(s.bearer())),
            last_logged: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// WARN on `arneb::cluster`, at most once per peer per minute.
    fn log_rejection(&self, peer: Option<std::net::SocketAddr>) {
        let ip = peer.map(|p| p.ip());
        let now = Instant::now();
        let mut last = self.last_logged.lock().unwrap_or_else(|e| e.into_inner());
        if last
            .get(&ip)
            .is_some_and(|at| now.duration_since(*at) < REJECT_LOG_INTERVAL)
        {
            return;
        }
        last.insert(ip, now);
        tracing::warn!(
            target: "arneb::cluster",
            peer = ?peer,
            "rejected cluster RPC with a missing or wrong [cluster] secret"
        );
    }
}

impl Interceptor for SecretCheck {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let Some(expected) = &self.expected else {
            return Ok(req);
        };
        let ok = req
            .metadata()
            .get("authorization")
            .is_some_and(|got| ct_eq(got.as_bytes(), expected.as_bytes()));
        if ok {
            Ok(req)
        } else {
            self.log_rejection(req.remote_addr());
            Err(Status::unauthenticated("missing or invalid cluster secret"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_length_and_redaction() {
        assert!(ClusterSecret::new("short").is_err());
        let s = ClusterSecret::new("x".repeat(32)).unwrap();
        assert_eq!(format!("{s:?}"), "ClusterSecret(<redacted>)");
        assert!(ClusterSecret::new(format!("{}\n", "x".repeat(32))).is_err());
    }

    #[test]
    fn ct_eq_matches_eq() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
    }

    #[test]
    fn secret_check_requires_the_bearer() {
        let secret = ClusterSecret::new("s".repeat(40)).unwrap();
        let mut check = SecretCheck::new(Some(&secret));
        assert_eq!(
            check.call(Request::new(())).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("authorization", "Bearer wrong".parse().unwrap());
        assert!(check.call(req).is_err());
        let mut req = Request::new(());
        req.metadata_mut().insert("authorization", secret.bearer());
        assert!(check.call(req).is_ok());

        let mut open = SecretCheck::new(None);
        assert!(open.call(Request::new(())).is_ok());
    }
}
