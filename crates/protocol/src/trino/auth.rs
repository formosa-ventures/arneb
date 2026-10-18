//! HTTP Basic authentication for the Trino listener (`[auth] type =
//! "password"`), verified against the same `[[auth.users]]` SCRAM verifiers
//! as pgwire.
//!
//! - `/v1/info`, `/v1/info/state` and `/v1/status` stay open for health
//!   checks; everything else needs `Authorization: Basic`.
//! - Failure is `401` with `WWW-Authenticate: Basic realm="arneb"`.
//! - The authenticated user is the query's user: any non-blank
//!   `X-Trino-User` / `X-Presto-User` naming someone else is `403`, and the
//!   session takes its user from the [`AuthUser`] extension, never from the
//!   headers.
//! - Trino clients resend credentials on every `nextUri` poll, so successful
//!   verifications are cached (bounded, TTL) under an HMAC of the password
//!   with a per-process key; failures are never cached.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde_json::json;

use super::session::claimed_users;
use crate::auth::{hmac_sha256, AuthConfigError, UserCredentials};

const CACHE_TTL: Duration = Duration::from_secs(300);
const CACHE_CAPACITY: usize = 1024;

/// Paths reachable without credentials (load-balancer health checks).
const PUBLIC_PATHS: &[&str] = &["/v1/info", "/v1/info/state", "/v1/status"];

/// Successful verifications: `(user, HMAC(cache_key, password))` → when.
type VerifyCache = HashMap<(String, [u8; 32]), Instant>;

/// The user a request authenticated as, stored in request extensions.
#[derive(Debug, Clone)]
pub(crate) struct AuthUser(pub(crate) String);

/// Password verifier plus the success cache.
#[derive(Clone)]
pub(crate) struct Authenticator {
    creds: Arc<UserCredentials>,
    cache_key: [u8; 32],
    cache: Arc<Mutex<VerifyCache>>,
    /// Number of full (PBKDF2) verifications; read by tests.
    verifications: Arc<AtomicU64>,
}

impl Authenticator {
    pub(crate) fn new(
        creds: Arc<UserCredentials>,
        verifications: Arc<AtomicU64>,
    ) -> Result<Self, AuthConfigError> {
        let mut key = [0u8; 32];
        aws_lc_rs::rand::fill(&mut key).map_err(|_| AuthConfigError::Random)?;
        Ok(Self {
            creds,
            cache_key: key,
            cache: Arc::new(Mutex::new(HashMap::new())),
            verifications,
        })
    }

    fn verify(&self, user: &str, password: &str) -> bool {
        let digest = hmac_sha256(&self.cache_key, password.as_bytes());
        let key = (user.to_owned(), digest);
        let now = Instant::now();
        {
            let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(at) = cache.get(&key) {
                if now.duration_since(*at) < CACHE_TTL {
                    return true;
                }
            }
        }
        self.verifications.fetch_add(1, Ordering::Relaxed);
        if !self.creds.verify_password(user, password) {
            return false;
        }
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if cache.len() >= CACHE_CAPACITY {
            cache.retain(|_, at| now.duration_since(*at) < CACHE_TTL);
            if cache.len() >= CACHE_CAPACITY {
                if let Some(oldest) = cache
                    .iter()
                    .min_by_key(|(_, at)| **at)
                    .map(|(k, _)| k.clone())
                {
                    cache.remove(&oldest);
                }
            }
        }
        cache.insert(key, now);
        true
    }
}

fn unauthorized(message: &str) -> Response {
    let mut resp = (
        StatusCode::UNAUTHORIZED,
        axum::Json(json!({ "error": { "message": message, "errorName": "UNAUTHORIZED" } })),
    )
        .into_response();
    resp.headers_mut().insert(
        "WWW-Authenticate",
        HeaderValue::from_static("Basic realm=\"arneb\""),
    );
    resp
}

pub(crate) fn forbidden(message: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(json!({ "error": { "message": message, "errorName": "PERMISSION_DENIED" } })),
    )
        .into_response()
}

/// `user:password` from an `Authorization: Basic` header.
fn basic_credentials(req: &Request) -> Option<(String, String)> {
    let value = req.headers().get("Authorization")?.to_str().ok()?;
    let (scheme, encoded) = value.trim().split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = STANDARD.decode(encoded.trim()).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (user, password) = decoded.split_once(':')?;
    Some((user.to_owned(), password.to_owned()))
}

/// Axum middleware enforcing Basic auth when the server has an
/// [`Authenticator`].
pub(crate) async fn require_auth(
    State(auth): State<Option<Authenticator>>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(auth) = auth else {
        return next.run(req).await;
    };
    if PUBLIC_PATHS.contains(&req.uri().path()) {
        return next.run(req).await;
    }
    let Some((user, password)) = basic_credentials(&req) else {
        return unauthorized("Authentication required");
    };
    if user.is_empty() || !auth.verify(&user, &password) {
        tracing::warn!(user = %user, "Trino password authentication failed");
        return unauthorized("Invalid credentials");
    }

    // The authenticated user is the session user (`ClientSession` reads it
    // from the extension): any user header naming someone else is
    // impersonation.
    if let Some(claimed) = claimed_users(req.headers()).find(|c| *c != user) {
        return forbidden(&format!(
            "User '{user}' cannot impersonate user '{claimed}'"
        ));
    }
    req.extensions_mut().insert(AuthUser(user));
    next.run(req).await
}
