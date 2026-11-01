//! OAuth 2.0 resource-server side of `/mcp`: validates JWT access tokens
//! against the issuer's JWKS.
//!
//! [`OAuthValidator`] holds a cache of the IdP's signing keys (by `kid`),
//! refetches it when it expires or when a token names an unknown `kid`
//! (at most once per `jwks_min_refresh`, single-flight), and checks
//! signature, algorithm, `iss`, `aud`, `exp`, `nbf`, the user claim and the
//! required scopes. Only asymmetric algorithms are accepted (RS256/384/512,
//! PS256, ES256/384); `none` and HS* are always refused.
//!
//! Tokens are never logged; a rejection is at most its reason code, on the
//! `arneb::oauth` target at DEBUG.

use std::sync::Mutex;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, Jwk, KeyAlgorithm, PublicKeyUse};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tokio::time::Instant;

use crate::config::OAuthConfig;

/// Timeout of each request to the IdP (discovery document and JWKS).
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);
/// Largest IdP response body read.
const MAX_BODY_BYTES: usize = 1024 * 1024;
/// Longest token validated; anything bigger is refused before parsing.
const MAX_TOKEN_BYTES: usize = 16 * 1024;
/// The accepted signature algorithms (asymmetric only).
const ALLOWED_ALGS: [Algorithm; 6] = [
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::ES256,
    Algorithm::ES384,
];

/// A `[oauth]` section reduced to what the validator needs.
#[derive(Debug, Clone)]
pub struct OAuthSettings {
    /// Required `iss`; also the base of the discovery URL.
    pub issuer: String,
    /// Public URL of the protected resource (`/mcp`).
    pub resource: String,
    /// Required `aud` value.
    pub audience: String,
    /// Claim carrying the Arneb user name.
    pub user_claim: String,
    /// JWKS URL; `None` = taken from the discovery document.
    pub jwks_uri: Option<String>,
    /// Scopes a token must carry.
    pub required_scopes: Vec<String>,
    /// Clock-skew allowance for `exp` / `nbf`.
    pub leeway: Duration,
    /// How long fetched keys are used.
    pub jwks_cache: Duration,
    /// Minimum gap between JWKS fetch attempts.
    pub jwks_min_refresh: Duration,
}

/// Why an [`OAuthConfig`] cannot become [`OAuthSettings`].
#[derive(Debug, thiserror::Error)]
pub enum OAuthSettingsError {
    /// No `issuer`: OAuth is off.
    #[error("[oauth] is not enabled (no issuer)")]
    NotEnabled,
    /// A required field is absent.
    #[error("[oauth] {0} is required")]
    Missing(&'static str),
}

impl TryFrom<&OAuthConfig> for OAuthSettings {
    type Error = OAuthSettingsError;

    fn try_from(c: &OAuthConfig) -> Result<Self, Self::Error> {
        let issuer = c.issuer.clone().ok_or(OAuthSettingsError::NotEnabled)?;
        let resource = c
            .resource
            .clone()
            .ok_or(OAuthSettingsError::Missing("resource"))?;
        let audience = c
            .effective_audience()
            .ok_or(OAuthSettingsError::Missing("audience"))?
            .to_owned();
        Ok(Self {
            issuer,
            resource,
            audience,
            user_claim: c.user_claim.clone(),
            jwks_uri: c.jwks_uri.clone(),
            required_scopes: c.required_scopes.clone(),
            leeway: Duration::from_secs(c.leeway_secs),
            jwks_cache: Duration::from_secs(c.jwks_cache_secs),
            jwks_min_refresh: Duration::from_secs(c.jwks_min_refresh_secs),
        })
    }
}

/// The identity a valid token stands for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthPrincipal {
    /// Value of the configured user claim.
    pub user: String,
    /// Scopes from `scope` (space separated) and/or `scp` (array).
    pub scopes: Vec<String>,
}

/// Why a bearer token was not accepted.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum OAuthReject {
    /// The token is not acceptable; the payload is a reason code
    /// (`expired`, `bad_audience`, ...).
    #[error("invalid token: {0}")]
    Invalid(&'static str),
    /// A valid token that lacks a required scope.
    #[error("insufficient scope")]
    InsufficientScope,
    /// The signing keys could not be fetched, so no verdict is possible.
    #[error("identity provider unavailable")]
    Unavailable,
}

/// Cheap shape check: three non-empty dot-separated segments, not an Arneb
/// token (`arneb_` prefix). Decides whether a bearer is tried as a JWT.
pub fn looks_like_jwt(token: &str) -> bool {
    if token.starts_with("arneb_") {
        return false;
    }
    let mut parts = token.split('.');
    let three = (0..3).all(|_| parts.next().is_some_and(|p| !p.is_empty()));
    three && parts.next().is_none()
}

/// One usable signing key.
struct CachedKey {
    kid: Option<String>,
    /// `alg` declared on the JWK, when any.
    alg: Option<KeyAlgorithm>,
    params: AlgorithmParameters,
    key: DecodingKey,
}

#[derive(Default)]
struct KeyCache {
    keys: Vec<std::sync::Arc<CachedKey>>,
    /// When `keys` were fetched.
    fetched_at: Option<Instant>,
    /// Start of the latest fetch attempt (successful or not).
    last_attempt: Option<Instant>,
    /// Bumped by every completed attempt; lets waiters of a single-flight
    /// refresh reuse its outcome.
    generation: u64,
    last_ok: bool,
    /// Discovered (or configured) JWKS URL.
    jwks_uri: Option<String>,
}

enum Refresh {
    /// Keys are fresh (this call or a concurrent one fetched them).
    Fetched,
    /// The fetch failed.
    Failed,
    /// Too soon after the previous attempt; nothing was done.
    Throttled,
}

/// Validates OAuth access tokens; share as `Arc<OAuthValidator>`.
pub struct OAuthValidator {
    settings: OAuthSettings,
    http: Option<reqwest::Client>,
    cache: Mutex<KeyCache>,
    /// Serialises fetches (single-flight).
    refresh_lock: tokio::sync::Mutex<()>,
}

impl OAuthValidator {
    /// A validator with an empty key cache; keys are fetched on demand (or
    /// by [`warm`](Self::warm)).
    pub fn new(settings: OAuthSettings) -> Self {
        Self::with_timeout(settings, HTTP_TIMEOUT)
    }

    fn with_timeout(settings: OAuthSettings, timeout: Duration) -> Self {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            // A JWKS/discovery URL that redirects is a misconfiguration.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(
                |e| tracing::error!(target: "arneb::oauth", "cannot build the HTTP client: {e}"),
            )
            .ok();
        Self {
            settings,
            http,
            cache: Mutex::new(KeyCache::default()),
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// The settings in effect.
    pub fn settings(&self) -> &OAuthSettings {
        &self.settings
    }

    /// Fetch the signing keys once, so the first request does not pay for
    /// it. Logs a WARN on failure; never errors.
    pub async fn warm(&self) {
        let seen = self.cache.lock().unwrap().generation;
        if matches!(self.refresh(seen).await, Refresh::Failed) {
            tracing::warn!(
                target: "arneb::oauth",
                "could not fetch the signing keys at startup; retrying on the first token"
            );
        }
    }

    /// Validate a bearer token.
    pub async fn validate(&self, token: &str) -> Result<OAuthPrincipal, OAuthReject> {
        let result = self.validate_inner(token).await;
        if let Err(e) = &result {
            tracing::debug!(target: "arneb::oauth", reason = %e, "token rejected");
        }
        result
    }

    async fn validate_inner(&self, token: &str) -> Result<OAuthPrincipal, OAuthReject> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(OAuthReject::Invalid("too_large"));
        }
        let header = parse_header(token)?;
        let alg = ALLOWED_ALGS
            .iter()
            .copied()
            .find(|a| alg_name(*a) == header.alg)
            .ok_or(OAuthReject::Invalid("bad_algorithm"))?;
        let key = self.key_for(header.kid.as_deref()).await?;
        check_key_matches(&key, alg)?;

        let mut v = Validation::new(alg);
        v.algorithms = vec![alg];
        v.set_issuer(&[&self.settings.issuer]);
        v.set_audience(&[&self.settings.audience]);
        v.leeway = self.settings.leeway.as_secs();
        v.validate_exp = true;
        v.validate_nbf = true;
        v.set_required_spec_claims(&["exp", "iss", "aud"]);
        let data = jsonwebtoken::decode::<serde_json::Value>(token, &key.key, &v)
            .map_err(|e| OAuthReject::Invalid(reason_code(e.kind())))?;
        self.principal(&data.claims)
    }

    fn principal(&self, claims: &serde_json::Value) -> Result<OAuthPrincipal, OAuthReject> {
        let user = claims
            .get(&self.settings.user_claim)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or(OAuthReject::Invalid("no_user_claim"))?
            .to_owned();
        let mut scopes: Vec<String> = Vec::new();
        if let Some(s) = claims.get("scope").and_then(|v| v.as_str()) {
            scopes.extend(s.split_whitespace().map(str::to_owned));
        }
        match claims.get("scp") {
            Some(serde_json::Value::Array(a)) => {
                scopes.extend(a.iter().filter_map(|v| v.as_str()).map(str::to_owned));
            }
            Some(serde_json::Value::String(s)) => {
                scopes.extend(s.split_whitespace().map(str::to_owned));
            }
            _ => {}
        }
        if !self
            .settings
            .required_scopes
            .iter()
            .all(|r| scopes.iter().any(|s| s == r))
        {
            return Err(OAuthReject::InsufficientScope);
        }
        Ok(OAuthPrincipal { user, scopes })
    }

    /// The cached key for `kid`, refetching the set when it is stale or the
    /// key is unknown (rate limited).
    async fn key_for(&self, kid: Option<&str>) -> Result<std::sync::Arc<CachedKey>, OAuthReject> {
        // At most one refresh per call: a second miss after it is final.
        let mut refreshed = false;
        loop {
            let (found, expired, generation, last_ok) = {
                let c = self.cache.lock().unwrap();
                let expired = c
                    .fetched_at
                    .is_none_or(|t| t.elapsed() >= self.settings.jwks_cache);
                let found = if expired {
                    Lookup::Stale
                } else {
                    lookup(&c.keys, kid)
                };
                (found, expired, c.generation, c.last_ok)
            };
            match found {
                Lookup::Found(k) => return Ok(k),
                Lookup::Ambiguous | Lookup::Unknown | Lookup::Stale if refreshed => {
                    // Fresh keys were fetched and still none fits.
                    return Err(if expired {
                        OAuthReject::Unavailable
                    } else {
                        OAuthReject::Invalid("unknown_key")
                    });
                }
                _ => {}
            }
            match self.refresh(generation).await {
                Refresh::Fetched => refreshed = true,
                Refresh::Failed => return Err(OAuthReject::Unavailable),
                Refresh::Throttled => {
                    // Cannot refetch yet: with no usable keys, or when the
                    // last attempt failed (so nothing ruled the kid out),
                    // that means unavailable; otherwise the kid is unknown.
                    return Err(if expired || !last_ok {
                        OAuthReject::Unavailable
                    } else {
                        OAuthReject::Invalid("unknown_key")
                    });
                }
            }
        }
    }

    /// Fetch the key set unless a concurrent call just did (`seen` is the
    /// generation the caller observed) or the previous attempt was too
    /// recent.
    async fn refresh(&self, seen: u64) -> Refresh {
        let _flight = self.refresh_lock.lock().await;
        let jwks_uri = {
            let c = self.cache.lock().unwrap();
            if c.generation != seen {
                // Someone else refreshed while we waited: share the outcome.
                return if c.last_ok {
                    Refresh::Fetched
                } else {
                    Refresh::Failed
                };
            }
            if c.last_attempt
                .is_some_and(|t| t.elapsed() < self.settings.jwks_min_refresh)
            {
                return Refresh::Throttled;
            }
            c.jwks_uri.clone()
        };
        // Open the rate-limit window before the await: if this future is
        // dropped mid-fetch (client disconnect), the window still counts and
        // is treated as a failed attempt, so a flood of cancelled requests
        // cannot drive one IdP fetch each.
        let started = Instant::now();
        {
            let mut c = self.cache.lock().unwrap();
            c.last_attempt = Some(started);
            c.last_ok = false;
        }
        let outcome = self.fetch(jwks_uri).await;
        let mut c = self.cache.lock().unwrap();
        c.last_attempt = Some(started);
        c.generation += 1;
        match outcome {
            Ok((keys, uri)) => {
                c.keys = keys;
                c.fetched_at = Some(Instant::now());
                c.jwks_uri = Some(uri);
                c.last_ok = true;
                Refresh::Fetched
            }
            Err(msg) => {
                // The old keys stay usable until `jwks_cache` elapses.
                tracing::warn!(target: "arneb::oauth", "fetching the signing keys failed: {msg}");
                c.last_ok = false;
                Refresh::Failed
            }
        }
    }

    async fn fetch(
        &self,
        known_uri: Option<String>,
    ) -> Result<(Vec<std::sync::Arc<CachedKey>>, String), String> {
        let http = self.http.as_ref().ok_or("no HTTP client")?;
        let uri = match (&self.settings.jwks_uri, known_uri) {
            (Some(u), _) => u.clone(),
            (None, Some(u)) => u,
            (None, None) => self.discover(http).await?,
        };
        let body = get_limited(http, &uri).await?;
        let set: JwksDoc =
            serde_json::from_slice(&body).map_err(|e| format!("bad JWKS document: {e}"))?;
        // Parse leniently: an IdP may list keys of kinds we do not know
        // (e.g. RSA-OAEP encryption keys), which must not hide the rest.
        let keys: Vec<_> = set
            .keys
            .into_iter()
            .filter_map(|v| serde_json::from_value::<Jwk>(v).ok())
            .filter(|j| !matches!(j.common.public_key_use, Some(PublicKeyUse::Encryption)))
            .filter_map(|j| {
                let key = DecodingKey::from_jwk(&j).ok()?;
                Some(std::sync::Arc::new(CachedKey {
                    kid: j.common.key_id.clone(),
                    alg: j.common.key_algorithm,
                    params: j.algorithm,
                    key,
                }))
            })
            .collect();
        if keys.is_empty() {
            return Err("the JWKS has no usable signing key".into());
        }
        Ok((keys, uri))
    }

    /// Resolve `jwks_uri` through the OIDC discovery document, checking its
    /// `issuer` against the configured one.
    async fn discover(&self, http: &reqwest::Client) -> Result<String, String> {
        let url = format!(
            "{}/.well-known/openid-configuration",
            self.settings.issuer.trim_end_matches('/')
        );
        let body = get_limited(http, &url).await?;
        let doc: Discovery =
            serde_json::from_slice(&body).map_err(|e| format!("bad discovery document: {e}"))?;
        if doc.issuer != self.settings.issuer {
            tracing::warn!(
                target: "arneb::oauth",
                configured = %self.settings.issuer,
                discovered = %doc.issuer,
                "the discovery document's issuer differs from [oauth] issuer"
            );
            return Err("discovery issuer mismatch".into());
        }
        doc.jwks_uri
            .ok_or_else(|| "discovery has no jwks_uri".into())
    }
}

#[derive(Deserialize)]
struct Discovery {
    issuer: String,
    jwks_uri: Option<String>,
}

#[derive(Deserialize)]
struct JwksDoc {
    keys: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct RawHeader {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

fn parse_header(token: &str) -> Result<RawHeader, OAuthReject> {
    let seg = token.split('.').next().unwrap_or_default();
    let bytes = URL_SAFE_NO_PAD
        .decode(seg)
        .map_err(|_| OAuthReject::Invalid("malformed"))?;
    serde_json::from_slice(&bytes).map_err(|_| OAuthReject::Invalid("malformed"))
}

fn alg_name(a: Algorithm) -> &'static str {
    match a {
        Algorithm::RS256 => "RS256",
        Algorithm::RS384 => "RS384",
        Algorithm::RS512 => "RS512",
        Algorithm::PS256 => "PS256",
        Algorithm::ES256 => "ES256",
        Algorithm::ES384 => "ES384",
        _ => "",
    }
}

enum Lookup {
    Found(std::sync::Arc<CachedKey>),
    /// No `kid` in the token and several keys to choose from.
    Ambiguous,
    Unknown,
    /// The cache is empty or expired.
    Stale,
}

fn lookup(keys: &[std::sync::Arc<CachedKey>], kid: Option<&str>) -> Lookup {
    match kid {
        Some(k) => keys
            .iter()
            .find(|c| c.kid.as_deref() == Some(k))
            .map_or(Lookup::Unknown, |c| Lookup::Found(c.clone())),
        None if keys.len() == 1 => Lookup::Found(keys[0].clone()),
        None => Lookup::Ambiguous,
    }
}

/// The JWK must be able to verify `alg`: same `alg` when declared, else the
/// same key family and curve.
fn check_key_matches(key: &CachedKey, alg: Algorithm) -> Result<(), OAuthReject> {
    let bad = Err(OAuthReject::Invalid("bad_algorithm"));
    if let Some(declared) = key.alg {
        return if key_alg(declared) == Some(alg) {
            Ok(())
        } else {
            bad
        };
    }
    let ok = match (&key.params, alg) {
        (
            AlgorithmParameters::RSA(_),
            Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512 | Algorithm::PS256,
        ) => true,
        (AlgorithmParameters::EllipticCurve(ec), Algorithm::ES256) => {
            ec.curve == EllipticCurve::P256
        }
        (AlgorithmParameters::EllipticCurve(ec), Algorithm::ES384) => {
            ec.curve == EllipticCurve::P384
        }
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        bad
    }
}

fn key_alg(a: KeyAlgorithm) -> Option<Algorithm> {
    Some(match a {
        KeyAlgorithm::RS256 => Algorithm::RS256,
        KeyAlgorithm::RS384 => Algorithm::RS384,
        KeyAlgorithm::RS512 => Algorithm::RS512,
        KeyAlgorithm::PS256 => Algorithm::PS256,
        KeyAlgorithm::ES256 => Algorithm::ES256,
        KeyAlgorithm::ES384 => Algorithm::ES384,
        _ => return None,
    })
}

fn reason_code(kind: &ErrorKind) -> &'static str {
    match kind {
        ErrorKind::ExpiredSignature => "expired",
        ErrorKind::ImmatureSignature => "not_yet_valid",
        ErrorKind::InvalidAudience => "bad_audience",
        ErrorKind::InvalidIssuer => "bad_issuer",
        ErrorKind::InvalidSignature => "bad_signature",
        ErrorKind::InvalidAlgorithm | ErrorKind::InvalidAlgorithmName => "bad_algorithm",
        ErrorKind::MissingRequiredClaim(c) => match c.as_str() {
            "exp" => "expired",
            "iss" => "bad_issuer",
            "aud" => "bad_audience",
            _ => "malformed",
        },
        _ => "malformed",
    }
}

/// GET `url`, reading at most [`MAX_BODY_BYTES`] of a 2xx response.
async fn get_limited(http: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
    let mut resp = http
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url}: HTTP {}", resp.status()));
    }
    if resp
        .content_length()
        .is_some_and(|n| n > MAX_BODY_BYTES as u64)
    {
        return Err(format!("GET {url}: response too large"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| format!("GET {url}: {e}"))? {
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(format!("GET {url}: response too large"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// A mock identity provider for tests (also used by the Web-layer tests).
#[cfg(test)]
pub(crate) mod test_idp {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};

    use aws_lc_rs::encoding::AsDer;
    use aws_lc_rs::rsa::{KeyPair as RsaKeyPair, KeySize};
    use aws_lc_rs::signature::{EcdsaKeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
    use axum::extract::State;
    use axum::routing::get;
    use axum::{Json, Router};
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    use jsonwebtoken::jwk::Jwk;
    use jsonwebtoken::{Algorithm, EncodingKey, Header};

    /// A signing key pair with its public JWK.
    pub(crate) struct TestKey {
        pub kid: String,
        pub alg: Algorithm,
        enc: EncodingKey,
        pub jwk: Jwk,
    }

    /// PKCS#8 DER plus the public modulus and exponent of the shared RSA key.
    fn rsa_material() -> &'static (Vec<u8>, Vec<u8>, Vec<u8>) {
        static M: OnceLock<(Vec<u8>, Vec<u8>, Vec<u8>)> = OnceLock::new();
        M.get_or_init(|| {
            use aws_lc_rs::signature::KeyPair as _;
            let kp = RsaKeyPair::generate(KeySize::Rsa2048).unwrap();
            let der = AsDer::<aws_lc_rs::encoding::Pkcs8V1Der>::as_der(&kp).unwrap();
            let public = kp.public_key();
            (
                der.as_ref().to_vec(),
                public.modulus().big_endian_without_leading_zero().to_vec(),
                public.exponent().big_endian_without_leading_zero().to_vec(),
            )
        })
    }

    impl TestKey {
        fn build(kid: &str, alg: Algorithm, enc: EncodingKey) -> Self {
            let mut jwk = Jwk::from_encoding_key(&enc, alg).unwrap();
            jwk.common.key_id = Some(kid.into());
            Self {
                kid: kid.into(),
                alg,
                enc,
                jwk,
            }
        }

        /// A fresh P-256 key (ES256).
        pub fn ec(kid: &str) -> Self {
            let doc = EcdsaKeyPair::generate_pkcs8(
                &ECDSA_P256_SHA256_FIXED_SIGNING,
                &aws_lc_rs::rand::SystemRandom::new(),
            )
            .unwrap();
            Self::build(
                kid,
                Algorithm::ES256,
                EncodingKey::from_ec_der(doc.as_ref()),
            )
        }

        /// An RSA-2048 key (RS256); the key material is generated once per
        /// test process and shared by all `rsa` keys.
        pub fn rsa(kid: &str) -> Self {
            // jsonwebtoken derives RSA JWKs from PKCS#1 only; ours is PKCS#8,
            // so the JWK is assembled from the public components.
            let (der, n, e) = rsa_material();
            // ... and its signer wants PKCS#1, which the PEM loader unwraps.
            let b64 = base64::engine::general_purpose::STANDARD.encode(der);
            let lines: Vec<&str> = b64
                .as_bytes()
                .chunks(64)
                .map(|c| std::str::from_utf8(c).unwrap())
                .collect();
            let pem = format!(
                "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
                lines.join("\n")
            );
            let jwk: Jwk = serde_json::from_value(serde_json::json!({
                "kty": "RSA", "kid": kid, "alg": "RS256", "use": "sig",
                "n": URL_SAFE_NO_PAD.encode(n), "e": URL_SAFE_NO_PAD.encode(e),
            }))
            .unwrap();
            Self {
                kid: kid.into(),
                alg: Algorithm::RS256,
                enc: EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap(),
                jwk,
            }
        }

        /// Sign `claims` with this key and the standard header (`kid`, `alg`).
        pub fn sign(&self, claims: &serde_json::Value) -> String {
            let mut h = Header::new(self.alg);
            h.kid = Some(self.kid.clone());
            jsonwebtoken::encode(&h, claims, &self.enc).unwrap()
        }
    }

    /// What the mock serves on `/jwks`.
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Mode {
        Ok,
        Fail500,
        Hang,
    }

    struct Shared {
        keys: Mutex<Vec<serde_json::Value>>,
        mode: Mutex<Mode>,
        hits: AtomicUsize,
        discovery_issuer: Mutex<String>,
        base: String,
    }

    /// A running mock IdP; the server stops when it is dropped.
    pub(crate) struct TestIdp {
        shared: Arc<Shared>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for TestIdp {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn discovery(State(s): State<Arc<Shared>>) -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "issuer": s.discovery_issuer.lock().unwrap().clone(),
            "jwks_uri": format!("{}/jwks", s.base),
        }))
    }

    async fn jwks(
        State(s): State<Arc<Shared>>,
    ) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
        s.hits.fetch_add(1, Ordering::SeqCst);
        let mode = *s.mode.lock().unwrap();
        match mode {
            Mode::Ok => Ok(Json(
                serde_json::json!({ "keys": s.keys.lock().unwrap().clone() }),
            )),
            Mode::Fail500 => Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
            Mode::Hang => std::future::pending().await,
        }
    }

    impl TestIdp {
        /// Start on `127.0.0.1:0` serving `keys`.
        pub async fn start(keys: &[&TestKey]) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let shared = Arc::new(Shared {
                keys: Mutex::new(keys.iter().map(|k| jwk_json(k)).collect()),
                mode: Mutex::new(Mode::Ok),
                hits: AtomicUsize::new(0),
                discovery_issuer: Mutex::new(base.clone()),
                base,
            });
            let app = Router::new()
                .route("/.well-known/openid-configuration", get(discovery))
                .route("/jwks", get(jwks))
                .with_state(shared.clone());
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
            Self { shared, task }
        }

        /// The issuer (and base URL) of this IdP.
        pub fn issuer(&self) -> String {
            self.shared.base.clone()
        }

        /// Replace the served key set (key rotation).
        pub fn set_keys(&self, keys: &[&TestKey]) {
            *self.shared.keys.lock().unwrap() = keys.iter().map(|k| jwk_json(k)).collect();
        }

        /// Switch between normal, `500` and hanging `/jwks` responses.
        pub fn set_mode(&self, mode: Mode) {
            *self.shared.mode.lock().unwrap() = mode;
        }

        /// Make the discovery document claim another issuer.
        pub fn set_discovery_issuer(&self, issuer: &str) {
            *self.shared.discovery_issuer.lock().unwrap() = issuer.into();
        }

        /// Number of `/jwks` requests so far.
        pub fn jwks_hits(&self) -> usize {
            self.shared.hits.load(Ordering::SeqCst)
        }
    }

    fn jwk_json(k: &TestKey) -> serde_json::Value {
        serde_json::to_value(&k.jwk).unwrap()
    }

    /// Seconds since the epoch.
    pub(crate) fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Claims of a valid token for `issuer`/`aud`: `sub = "alice"`,
    /// `preferred_username = "alice"`, `exp` in five minutes.
    pub(crate) fn claims(issuer: &str, aud: &str) -> serde_json::Value {
        serde_json::json!({
            "iss": issuer, "aud": aud, "sub": "alice",
            "preferred_username": "alice",
            "iat": now(), "exp": now() + 300,
        })
    }
}

/// Well-known path prefix of RFC 9728 protected-resource metadata.
pub const METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

/// The metadata URL of `resource` (RFC 9728 section 3.1): the well-known
/// segment inserted between the authority and the resource's path.
pub fn metadata_url(resource: &str) -> String {
    match url::Url::parse(resource) {
        Ok(u) => {
            let path = if u.path() == "/" { "" } else { u.path() };
            let authority = &u[..url::Position::BeforePath];
            format!("{authority}{METADATA_PATH}{path}")
        }
        // Settings are validated at startup; this only guards a bad literal.
        Err(_) => format!("{}{METADATA_PATH}", resource.trim_end_matches('/')),
    }
}

/// `WWW-Authenticate` value for a `/mcp` request without valid credentials.
pub fn challenge(settings: &OAuthSettings) -> String {
    let mut v = format!(
        r#"Bearer realm="arneb", resource_metadata="{}""#,
        metadata_url(&settings.resource)
    );
    if !settings.required_scopes.is_empty() {
        v.push_str(&format!(
            r#", scope="{}""#,
            settings.required_scopes.join(" ")
        ));
    }
    v
}

/// `WWW-Authenticate` value for a valid token that lacks a required scope.
pub fn insufficient_scope_challenge(settings: &OAuthSettings) -> String {
    format!(
        r#"Bearer realm="arneb", error="insufficient_scope", scope="{}", resource_metadata="{}""#,
        settings.required_scopes.join(" "),
        metadata_url(&settings.resource)
    )
}

#[cfg(test)]
mod tests {
    use super::test_idp::{claims, now, Mode, TestIdp, TestKey};
    use super::*;
    use jsonwebtoken::Header;
    use serde_json::json;

    const AUD: &str = "https://arneb.test/mcp";

    fn settings(issuer: &str) -> OAuthSettings {
        OAuthSettings {
            issuer: issuer.into(),
            resource: AUD.into(),
            audience: AUD.into(),
            user_claim: "sub".into(),
            jwks_uri: None,
            required_scopes: vec![],
            leeway: Duration::from_secs(0),
            jwks_cache: Duration::from_secs(3600),
            jwks_min_refresh: Duration::from_secs(30),
        }
    }

    fn validator(idp: &TestIdp, f: impl FnOnce(&mut OAuthSettings)) -> OAuthValidator {
        let mut s = settings(&idp.issuer());
        f(&mut s);
        OAuthValidator::new(s)
    }

    fn b64(v: &serde_json::Value) -> String {
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(v).unwrap())
    }

    #[test]
    fn metadata_url_for_resource_shapes() {
        assert_eq!(
            metadata_url("https://h/mcp"),
            "https://h/.well-known/oauth-protected-resource/mcp"
        );
        assert_eq!(
            metadata_url("https://h:8443/mcp"),
            "https://h:8443/.well-known/oauth-protected-resource/mcp"
        );
        assert_eq!(
            metadata_url("https://h/arneb/mcp"),
            "https://h/.well-known/oauth-protected-resource/arneb/mcp"
        );
        assert_eq!(
            metadata_url("https://h/mcp/"),
            "https://h/.well-known/oauth-protected-resource/mcp/"
        );
    }

    #[test]
    fn challenges_carry_metadata_and_scopes() {
        let mut s = settings("https://idp.test");
        assert_eq!(
            challenge(&s),
            r#"Bearer realm="arneb", resource_metadata="https://arneb.test/.well-known/oauth-protected-resource/mcp""#
        );
        s.required_scopes = vec!["a".into(), "b".into()];
        assert!(challenge(&s).ends_with(r#"", scope="a b""#));
        assert_eq!(
            insufficient_scope_challenge(&s),
            r#"Bearer realm="arneb", error="insufficient_scope", scope="a b", resource_metadata="https://arneb.test/.well-known/oauth-protected-resource/mcp""#
        );
    }

    #[tokio::test]
    async fn accepts_a_valid_token_and_maps_the_claim() {
        for key in [TestKey::ec("k1"), TestKey::rsa("k1")] {
            let idp = TestIdp::start(&[&key]).await;
            let v = validator(&idp, |s| s.user_claim = "preferred_username".into());
            let mut c = claims(&idp.issuer(), AUD);
            c["sub"] = json!("ignored");
            c["scope"] = json!("openid arneb");
            let p = v.validate(&key.sign(&c)).await.unwrap();
            assert_eq!(p.user, "alice");
            assert_eq!(p.scopes, ["openid", "arneb"]);

            let mut c = claims(&idp.issuer(), AUD);
            c["scp"] = json!(["a", "b"]);
            let p = v.validate(&key.sign(&c)).await.unwrap();
            assert_eq!(p.scopes, ["a", "b"]);
        }
    }

    #[tokio::test]
    async fn rejects_every_bad_token_class() {
        let key = TestKey::ec("k1");
        let other = TestKey::ec("k1"); // same kid, different key material
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |_| {});
        let iss = idp.issuer();
        let reject = |t: String| {
            let v = &v;
            async move { v.validate(&t).await.unwrap_err() }
        };
        let invalid = |r: &'static str| OAuthReject::Invalid(r);

        let mut c = claims(&iss, "someone-else");
        assert_eq!(reject(key.sign(&c)).await, invalid("bad_audience"));
        c = claims("https://evil.test", AUD);
        assert_eq!(reject(key.sign(&c)).await, invalid("bad_issuer"));
        c = claims(&iss, AUD);
        c["exp"] = json!(now() - 30);
        assert_eq!(reject(key.sign(&c)).await, invalid("expired"));
        c = claims(&iss, AUD);
        c["nbf"] = json!(now() + 600);
        assert_eq!(reject(key.sign(&c)).await, invalid("not_yet_valid"));
        c = claims(&iss, AUD);
        c.as_object_mut().unwrap().remove("sub");
        assert_eq!(reject(key.sign(&c)).await, invalid("no_user_claim"));
        c = claims(&iss, AUD);
        c["sub"] = json!("");
        assert_eq!(reject(key.sign(&c)).await, invalid("no_user_claim"));
        assert_eq!(
            reject(other.sign(&claims(&iss, AUD))).await,
            invalid("bad_signature")
        );

        // alg=none and HS256 keyed with the public key are never accepted.
        let none = format!(
            "{}.{}.",
            b64(&json!({"alg": "none", "kid": "k1"})),
            b64(&claims(&iss, AUD))
        );
        assert_eq!(reject(none).await, invalid("bad_algorithm"));
        let hs = jsonwebtoken::encode(
            &{
                let mut h = Header::new(Algorithm::HS256);
                h.kid = Some("k1".into());
                h
            },
            &claims(&iss, AUD),
            &EncodingKey::from_secret(b"public-key-bytes"),
        )
        .unwrap();
        assert_eq!(reject(hs).await, invalid("bad_algorithm"));
        // RS256 header against an EC JWK (declared ES256) is a mismatch.
        let rsa = TestKey::rsa("k1");
        assert_eq!(
            reject(rsa.sign(&claims(&iss, AUD))).await,
            invalid("bad_algorithm")
        );

        let unknown = TestKey::ec("nope");
        assert_eq!(
            reject(unknown.sign(&claims(&iss, AUD))).await,
            invalid("unknown_key")
        );
        assert_eq!(
            reject("x".repeat(MAX_TOKEN_BYTES + 1)).await,
            invalid("too_large")
        );
        assert_eq!(reject("###.b.c".into()).await, invalid("malformed"));

        // 30 s past expiry passes with 60 s of leeway.
        let lenient = validator(&idp, |s| s.leeway = Duration::from_secs(60));
        let mut c = claims(&iss, AUD);
        c["exp"] = json!(now() - 30);
        lenient.validate(&key.sign(&c)).await.unwrap();
    }

    use jsonwebtoken::EncodingKey;

    #[tokio::test]
    async fn insufficient_scope() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| s.required_scopes = vec!["arneb".into()]);
        let mut c = claims(&idp.issuer(), AUD);
        c["scope"] = json!("openid");
        assert_eq!(
            v.validate(&key.sign(&c)).await.unwrap_err(),
            OAuthReject::InsufficientScope
        );
        c["scope"] = json!("openid arneb");
        v.validate(&key.sign(&c)).await.unwrap();
    }

    #[tokio::test]
    async fn rotation_refetches_once_and_floods_are_rate_limited() {
        let old = TestKey::ec("old");
        let new = TestKey::ec("new");
        let idp = TestIdp::start(&[&old]).await;
        let v = validator(&idp, |s| s.jwks_min_refresh = Duration::from_millis(50));
        v.validate(&old.sign(&claims(&idp.issuer(), AUD)))
            .await
            .unwrap();
        assert_eq!(idp.jwks_hits(), 1);
        tokio::time::sleep(Duration::from_millis(80)).await;

        idp.set_keys(&[&old, &new]);
        v.validate(&new.sign(&claims(&idp.issuer(), AUD)))
            .await
            .unwrap();
        assert_eq!(idp.jwks_hits(), 2, "exactly one extra JWKS hit");

        // A flood of random kids inside the window refetches at most once.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let before = idp.jwks_hits();
        let mut tasks = Vec::new();
        let v = std::sync::Arc::new(v);
        for i in 0..100 {
            let v = v.clone();
            let mut k = TestKey::ec("x");
            k.kid = format!("rand-{i}");
            let t = k.sign(&claims(&idp.issuer(), AUD));
            tasks.push(tokio::spawn(async move { v.validate(&t).await }));
        }
        for t in tasks {
            assert_eq!(
                t.await.unwrap().unwrap_err(),
                OAuthReject::Invalid("unknown_key")
            );
        }
        assert!(idp.jwks_hits() <= before + 1, "hits: {}", idp.jwks_hits());
    }

    #[tokio::test]
    async fn idp_down_is_unavailable_until_it_returns() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        idp.set_mode(Mode::Fail500);
        let v = validator(&idp, |s| s.jwks_min_refresh = Duration::from_millis(50));
        let t = key.sign(&claims(&idp.issuer(), AUD));
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        // Within the window nothing is refetched and there are no keys.
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        assert_eq!(idp.jwks_hits(), 1);

        idp.set_mode(Mode::Ok);
        tokio::time::sleep(Duration::from_millis(80)).await;
        v.validate(&t).await.unwrap();
    }

    #[tokio::test]
    async fn a_hanging_idp_is_unavailable_within_the_timeout() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        idp.set_mode(Mode::Hang);
        let mut s = settings(&idp.issuer());
        s.jwks_min_refresh = Duration::from_millis(10);
        let v = OAuthValidator::with_timeout(s, Duration::from_millis(300));
        let t = key.sign(&claims(&idp.issuer(), AUD));
        let started = std::time::Instant::now();
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[tokio::test]
    async fn failed_refresh_keeps_old_keys_until_the_cache_expires() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| {
            s.jwks_cache = Duration::from_millis(200);
            s.jwks_min_refresh = Duration::from_millis(10);
        });
        let t = key.sign(&claims(&idp.issuer(), AUD));
        v.validate(&t).await.unwrap();
        idp.set_mode(Mode::Fail500);
        let unknown = TestKey::ec("zzz").sign(&claims(&idp.issuer(), AUD));
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Known key still decides while the fetch fails...
        v.validate(&t).await.unwrap();
        // ...an unknown kid cannot be decided...
        assert_eq!(
            v.validate(&unknown).await.unwrap_err(),
            OAuthReject::Unavailable
        );
        // ...and once the cache expires nothing can.
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
    }

    #[tokio::test]
    async fn a_cancelled_fetch_still_opens_the_rate_limit_window() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        idp.set_mode(Mode::Hang);
        let v = validator(&idp, |_| {});
        let t = key.sign(&claims(&idp.issuer(), AUD));
        // Drop the validate future mid-fetch.
        let r = tokio::time::timeout(Duration::from_millis(200), v.validate(&t)).await;
        assert!(r.is_err());
        assert_eq!(idp.jwks_hits(), 1);
        idp.set_mode(Mode::Ok);
        for _ in 0..5 {
            assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        }
        assert_eq!(idp.jwks_hits(), 1, "no refetch inside the window");
    }

    #[tokio::test]
    async fn throttled_after_a_failed_attempt_is_unavailable_not_unknown_key() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| s.jwks_min_refresh = Duration::from_millis(100));
        v.validate(&key.sign(&claims(&idp.issuer(), AUD)))
            .await
            .unwrap();
        // Valid keys cached, IdP goes down, a rotated kid shows up.
        idp.set_mode(Mode::Fail500);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let rotated = TestKey::ec("rotated").sign(&claims(&idp.issuer(), AUD));
        for _ in 0..3 {
            assert_eq!(
                v.validate(&rotated).await.unwrap_err(),
                OAuthReject::Unavailable
            );
        }
        assert_eq!(idp.jwks_hits(), 2, "one failed fetch, then throttled");
    }

    #[tokio::test]
    async fn discovered_issuer_must_match() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        idp.set_discovery_issuer("https://other.test");
        let v = validator(&idp, |_| {});
        let t = key.sign(&claims(&idp.issuer(), AUD));
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        assert_eq!(idp.jwks_hits(), 0);
    }

    #[tokio::test]
    async fn configured_jwks_uri_skips_discovery() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        idp.set_discovery_issuer("https://other.test");
        let uri = format!("{}/jwks", idp.issuer());
        let v = validator(&idp, |s| s.jwks_uri = Some(uri));
        v.validate(&key.sign(&claims(&idp.issuer(), AUD)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn warm_never_errors() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        idp.set_mode(Mode::Fail500);
        let v = validator(&idp, |_| {});
        v.warm().await;
        assert_eq!(idp.jwks_hits(), 1);
    }

    #[test]
    fn settings_require_an_enabled_config() {
        assert!(OAuthSettings::try_from(&OAuthConfig::default()).is_err());
        let c = OAuthConfig {
            issuer: Some("https://idp.test".into()),
            resource: Some(AUD.into()),
            ..Default::default()
        };
        let s = OAuthSettings::try_from(&c).unwrap();
        assert_eq!(s.audience, AUD);
        assert_eq!(s.leeway, Duration::from_secs(60));
    }

    #[test]
    fn looks_like_jwt_cases() {
        assert!(looks_like_jwt("a.b.c"));
        for no in ["arneb_x.y.z", "arneb_s_abc", "a.b", "a..c", "a.b.c.d", ""] {
            assert!(!looks_like_jwt(no), "{no}");
        }
    }
}
