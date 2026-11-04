//! OAuth 2.0 resource-server side of `/mcp`: validates JWT access tokens
//! against the issuer's JWKS.
//!
//! [`OAuthValidator`] holds a cache of the IdP's signing keys (by `kid`) and
//! checks signature, algorithm, `iss`, `aud`, `exp`, `nbf`, that the token
//! is a plain bearer (no DPoP or mTLS binding) and an access token (not an
//! OIDC ID token), the user claim (with
//! `email_verified` for an email address) and the required scopes. Only asymmetric algorithms are accepted (RS256/384/512,
//! PS256, ES256/384); `none` and HS* are always refused.
//!
//! Key refreshes run in a detached task (single-flight, at most one start
//! per `jwks_min_refresh`), so a request that is dropped mid-fetch never
//! aborts the fetch. The set is refetched in the background from 80% of
//! `jwks_cache` on (stale-while-revalidate); while that fails, the last good
//! keys keep deciding for up to `jwks_max_stale` after they were fetched. A
//! token naming an unknown `kid` waits for a refetch (throttled likewise).
//!
//! Tokens are never logged; a rejection is at most its reason code, on the
//! `arneb::oauth` target at DEBUG.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures::future::{BoxFuture, FutureExt as _, Shared};
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
    /// How long fetched keys are fresh; a background refetch starts at 80%.
    pub jwks_cache: Duration,
    /// Minimum gap between JWKS fetch attempts.
    pub jwks_min_refresh: Duration,
    /// How long after a successful fetch its keys may still decide while
    /// refetches fail (at least `jwks_cache`).
    pub jwks_max_stale: Duration,
    /// Require the `at+jwt` header `typ` (RFC 9068); off by default because
    /// many IdPs send `typ: JWT` on access tokens.
    pub require_access_token_typ: bool,
    /// Accept an email-shaped user (other than from `user_claim = "email"`)
    /// from a token without `email_verified`; an explicit non-`true` value
    /// is refused regardless.
    pub allow_missing_email_verified: bool,
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
            jwks_max_stale: Duration::from_secs(c.jwks_max_stale_secs),
            require_access_token_typ: c.require_access_token_typ,
            allow_missing_email_verified: c.allow_missing_email_verified,
        })
    }
}

impl OAuthSettings {
    /// Age of the key set from which a background refetch starts.
    fn refresh_ahead(&self) -> Duration {
        self.jwks_cache * 4 / 5
    }
}

/// The identity a valid token stands for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthPrincipal {
    /// Value of the configured user claim.
    pub user: String,
    /// Scopes from `scope` (space separated) and/or `scp` (array).
    pub scopes: Vec<String>,
    /// The token's `exp` in epoch milliseconds.
    pub expires_at_ms: u64,
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
    /// The user is an email address (or `user_claim = "email"`) and the
    /// token's `email_verified` is not true.
    #[error("email not verified")]
    UnverifiedEmail,
    /// The user (an email address under a claim other than `email`) is not
    /// the token's `email`, so `email_verified` does not vouch for it.
    #[error("user claim is not the verified email")]
    EmailMismatch,
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

/// A JWKS fetch running in its own task; resolves to whether it succeeded.
type Flight = Shared<BoxFuture<'static, bool>>;

#[derive(Default)]
struct KeyCache {
    keys: Vec<Arc<CachedKey>>,
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
    /// The fetch in flight, if any. It runs detached, so dropping the
    /// request that started it (a client disconnect) never aborts it.
    inflight: Option<Flight>,
}

enum Refresh {
    /// Keys are fresh (this call or a concurrent one fetched them).
    Fetched,
    /// The fetch failed.
    Failed,
    /// Too soon after the previous attempt; nothing was done.
    Throttled,
}

/// What the detached fetch task needs.
struct Inner {
    settings: OAuthSettings,
    http: Option<reqwest::Client>,
    cache: Mutex<KeyCache>,
    /// Test hook: make the next fetch panic.
    #[cfg(test)]
    panic_next_fetch: std::sync::atomic::AtomicBool,
}

/// Armed for the life of a fetch task; still armed when dropped only when
/// the task unwinds or is cancelled (a finished task disarms it with
/// `take`).
struct FetchGuard(Option<Arc<Inner>>);

impl Drop for FetchGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            inner.record_lost_fetch();
        }
    }
}

/// Validates OAuth access tokens; share as `Arc<OAuthValidator>`.
pub struct OAuthValidator {
    inner: Arc<Inner>,
    /// `WWW-Authenticate` values, computed once.
    challenge: String,
    invalid_token_challenge: String,
    insufficient_scope_challenge: String,
    /// The path derived from `resource` that the metadata document is
    /// served at (besides the bare well-known path).
    metadata_path: String,
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
            challenge: challenge(&settings),
            invalid_token_challenge: invalid_token_challenge(&settings),
            insufficient_scope_challenge: insufficient_scope_challenge(&settings),
            metadata_path: metadata_path(&settings.resource),
            inner: Arc::new(Inner {
                settings,
                http,
                cache: Mutex::new(KeyCache::default()),
                #[cfg(test)]
                panic_next_fetch: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// The settings in effect.
    pub fn settings(&self) -> &OAuthSettings {
        &self.inner.settings
    }

    /// `WWW-Authenticate` for a `/mcp` request without credentials.
    pub fn challenge(&self) -> &str {
        &self.challenge
    }

    /// `WWW-Authenticate` for a presented access token that is not valid
    /// (RFC 6750 section 3.1 `invalid_token`), so a client can refresh it
    /// instead of starting over.
    pub fn invalid_token_challenge(&self) -> &str {
        &self.invalid_token_challenge
    }

    /// `WWW-Authenticate` for a valid token that lacks a required scope.
    pub fn insufficient_scope_challenge(&self) -> &str {
        &self.insufficient_scope_challenge
    }

    /// The path of the protected-resource metadata document (RFC 9728
    /// section 3.1), derived from `resource`.
    pub fn metadata_path(&self) -> &str {
        &self.metadata_path
    }

    /// Fetch the signing keys once, so the first request does not pay for
    /// it. Logs a WARN on failure; never errors.
    pub async fn warm(&self) {
        let seen = self.inner.lock().generation;
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
        let settings = &self.inner.settings;
        if token.len() > MAX_TOKEN_BYTES {
            return Err(OAuthReject::Invalid("too_large"));
        }
        // An unknown `alg` (`none` included) does not parse.
        let header =
            jsonwebtoken::decode_header(token).map_err(|_| OAuthReject::Invalid("malformed"))?;
        let alg = header.alg;
        if !ALLOWED_ALGS.contains(&alg) {
            return Err(OAuthReject::Invalid("bad_algorithm"));
        }
        if settings.require_access_token_typ
            && !header.typ.as_deref().is_some_and(is_access_token_typ)
        {
            return Err(OAuthReject::Invalid("not_at_jwt"));
        }
        let keys = self.keys_for(header.kid.as_deref(), alg).await?;

        let mut v = Validation::new(alg);
        v.algorithms = vec![alg];
        v.set_issuer(&[&settings.issuer]);
        v.set_audience(&[&settings.audience]);
        v.leeway = settings.leeway.as_secs();
        v.validate_exp = true;
        v.validate_nbf = true;
        v.set_required_spec_claims(&["exp", "iss", "aud"]);
        // Every key under the `kid` that fits `alg` (an IdP migrating
        // algorithms may publish several); the first that verifies decides.
        let mut claims = None;
        for key in &keys {
            match jsonwebtoken::decode::<serde_json::Value>(token, &key.key, &v) {
                Ok(data) => {
                    claims = Some(data.claims);
                    break;
                }
                Err(e) if matches!(e.kind(), ErrorKind::InvalidSignature) => continue,
                // The signature verified; a claim is wrong.
                Err(e) => return Err(OAuthReject::Invalid(reason_code(e.kind()))),
            }
        }
        let claims = claims.ok_or(OAuthReject::Invalid("bad_signature"))?;
        // A sender-constrained token (DPoP, mTLS) is only valid with a proof
        // of possession, which Arneb does not verify: accepting it as a
        // plain bearer would let a stolen one be replayed.
        if is_sender_constrained(header.typ.as_deref(), &claims) {
            return Err(OAuthReject::Invalid("sender_constrained"));
        }
        // One parsed scope list decides both what the token is and what it
        // may do.
        let scopes = token_scopes(&claims);
        // The IdP also signs OIDC ID tokens, for the same issuer and
        // possibly the same audience: refuse what may be one. This comes
        // before every 403 (scope, email), so an ID token is always a 401
        // `invalid_token` that the client discards, never a step-up.
        match token_kind(header.typ.as_deref(), &claims) {
            TokenKind::Access => {}
            TokenKind::NotAccess => return Err(OAuthReject::Invalid("id_token")),
            // Unmarked and ID-token-like: only every required scope, which
            // an ID token never carries, vouches for it.
            TokenKind::Unmarked if may_be_id_token(&claims, &scopes) => {
                if settings.required_scopes.is_empty()
                    || !has_scopes(&scopes, &settings.required_scopes)
                {
                    return Err(OAuthReject::Invalid("id_token"));
                }
            }
            TokenKind::Unmarked => {}
        }
        self.principal(&claims, scopes)
    }

    fn principal(
        &self,
        claims: &serde_json::Value,
        scopes: Vec<String>,
    ) -> Result<OAuthPrincipal, OAuthReject> {
        let settings = &self.inner.settings;
        let user = claims
            .get(&settings.user_claim)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or(OAuthReject::Invalid("no_user_claim"))?
            .to_owned();
        // The scope first: an email refusal is about the identity, and
        // should not mask a token the client can fix by stepping up.
        if !has_scopes(&scopes, &settings.required_scopes) {
            return Err(OAuthReject::InsufficientScope);
        }
        check_email(settings, claims, &user)?;
        let expires_at_ms = claims
            .get("exp")
            .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
            .ok_or(OAuthReject::Invalid("no_exp"))?
            .saturating_mul(1000);
        Ok(OAuthPrincipal {
            user,
            scopes,
            expires_at_ms,
        })
    }

    /// The cached keys for `kid` that can verify `alg`, refetching the set
    /// when it is unusable or the key is unknown (rate limited).
    async fn keys_for(
        &self,
        kid: Option<&str>,
        alg: Algorithm,
    ) -> Result<Vec<Arc<CachedKey>>, OAuthReject> {
        let settings = &self.inner.settings;
        // At most one refresh per call: a second miss after it is final.
        let mut refreshed = false;
        loop {
            let (found, usable, generation, last_ok) = {
                let mut c = self.inner.lock();
                let age = c.fetched_at.map(|t| t.elapsed());
                let usable = age.is_some_and(|a| a < settings.jwks_max_stale);
                if usable && age.is_some_and(|a| a >= settings.refresh_ahead()) {
                    // Stale-while-revalidate: answer from these keys and
                    // refetch in the background (throttled, single-flight).
                    let _ = self.inner.start_fetch(&mut c);
                }
                let found = if usable {
                    lookup(&c.keys, kid, alg)
                } else {
                    Lookup::Stale
                };
                (found, usable, c.generation, c.last_ok)
            };
            match found {
                Lookup::Found(k) => return Ok(k),
                Lookup::Incompatible => return Err(OAuthReject::Invalid("bad_algorithm")),
                Lookup::Ambiguous | Lookup::Unknown | Lookup::Stale if refreshed => {
                    // Fresh keys were fetched and still none fits.
                    return Err(if usable {
                        OAuthReject::Invalid("unknown_key")
                    } else {
                        OAuthReject::Unavailable
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
                    return Err(if !usable || !last_ok {
                        OAuthReject::Unavailable
                    } else {
                        OAuthReject::Invalid("unknown_key")
                    });
                }
            }
        }
    }

    /// Wait for a fetch of the key set: the one in flight, or a new one
    /// unless a concurrent call just finished one (`seen` is the generation
    /// the caller observed) or the previous attempt was too recent.
    async fn refresh(&self, seen: u64) -> Refresh {
        let flight = {
            let mut c = self.inner.lock();
            if c.generation != seen {
                // Someone else refreshed meanwhile: share the outcome.
                return if c.last_ok {
                    Refresh::Fetched
                } else {
                    Refresh::Failed
                };
            }
            match self.inner.start_fetch(&mut c) {
                Some(flight) => flight,
                None => return Refresh::Throttled,
            }
        };
        if flight.await {
            Refresh::Fetched
        } else {
            Refresh::Failed
        }
    }
}

impl Inner {
    /// The key cache, also after a panic while it was held: every update
    /// leaves it consistent (at worst an attempt recorded twice), so a
    /// poisoned lock must not turn every later request into a panic.
    fn lock(&self) -> MutexGuard<'_, KeyCache> {
        self.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The fetch in flight, else a new one in a detached task; `None` when
    /// the previous attempt started less than `jwks_min_refresh` ago. Called
    /// with the cache locked.
    fn start_fetch(self: &Arc<Self>, c: &mut KeyCache) -> Option<Flight> {
        if let Some(flight) = &c.inflight {
            return Some(flight.clone());
        }
        if c.last_attempt
            .is_some_and(|t| t.elapsed() < self.settings.jwks_min_refresh)
        {
            return None;
        }
        c.last_attempt = Some(Instant::now());
        let inner = Arc::clone(self);
        let known_uri = c.jwks_uri.clone();
        let task = tokio::spawn(async move {
            // Records a failed attempt if the task ends without recording
            // one (a panic, or the runtime shutting down), so the flight is
            // never left listed and refreshes resume.
            let mut guard = FetchGuard(Some(Arc::clone(&inner)));
            let ok = inner.run_fetch(known_uri).await;
            // Disarm: the outcome is recorded (and the Arc released).
            guard.0.take();
            ok
        });
        let flight = task.map(|r| r.unwrap_or(false)).boxed().shared();
        c.inflight = Some(flight.clone());
        Some(flight)
    }

    /// Record an attempt that ended without an outcome as failed.
    fn record_lost_fetch(&self) {
        let mut c = self.lock();
        c.generation += 1;
        c.inflight = None;
        c.last_ok = false;
        tracing::warn!(
            target: "arneb::oauth",
            "fetching the signing keys ended without a result; recorded as a failed attempt"
        );
    }

    /// One fetch attempt, recorded in the cache; `true` on success.
    async fn run_fetch(&self, known_uri: Option<String>) -> bool {
        let outcome = self.fetch(known_uri).await;
        let mut c = self.lock();
        c.generation += 1;
        c.inflight = None;
        match outcome {
            Ok((keys, uri)) => {
                c.keys = keys;
                c.fetched_at = Some(Instant::now());
                c.jwks_uri = Some(uri);
                c.last_ok = true;
                true
            }
            Err(msg) => {
                match c.fetched_at {
                    Some(t) if t.elapsed() < self.settings.jwks_max_stale => tracing::warn!(
                        target: "arneb::oauth",
                        keys_age_secs = t.elapsed().as_secs(),
                        max_stale_secs = self.settings.jwks_max_stale.as_secs(),
                        "fetching the signing keys failed: {msg}; the previous keys stay in use \
                         until they are jwks_max_stale_secs old"
                    ),
                    _ => tracing::warn!(
                        target: "arneb::oauth",
                        "fetching the signing keys failed: {msg}"
                    ),
                }
                c.last_ok = false;
                // A discovered URL may be stale (the IdP moved it): forget
                // it so the next attempt rediscovers.
                if self.settings.jwks_uri.is_none() {
                    c.jwks_uri = None;
                }
                false
            }
        }
    }

    async fn fetch(
        &self,
        known_uri: Option<String>,
    ) -> Result<(Vec<Arc<CachedKey>>, String), String> {
        #[cfg(test)]
        if self
            .panic_next_fetch
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("test: the JWKS fetch panicked");
        }
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
                Some(Arc::new(CachedKey {
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
        let uri = doc
            .jwks_uri
            .ok_or_else(|| "discovery has no jwks_uri".to_owned())?;
        check_discovered_jwks_uri(&self.settings.issuer, &uri)?;
        Ok(uri)
    }
}

/// A `jwks_uri` from a discovery document: the configured-URL rule (D9),
/// and `https` whenever the issuer itself is not on loopback, so a remote
/// document cannot point Arneb at plain-http services on its own host.
fn check_discovered_jwks_uri(issuer: &str, uri: &str) -> Result<(), String> {
    crate::config::check_oauth_url("the discovered jwks_uri", uri, true)?;
    let issuer_on_loopback =
        url::Url::parse(issuer).is_ok_and(|u| crate::config::is_loopback_url(&u));
    let https = url::Url::parse(uri).is_ok_and(|u| u.scheme() == "https");
    if !issuer_on_loopback && !https {
        return Err(format!(
            "the discovered jwks_uri '{uri}' must use https: the issuer is not on loopback"
        ));
    }
    Ok(())
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

/// The RFC 9068 media type of a JWT access token, as a header `typ`.
fn is_access_token_typ(typ: &str) -> bool {
    typ.eq_ignore_ascii_case("at+jwt") || typ.eq_ignore_ascii_case("application/at+jwt")
}

/// An unverified address can be anyone's (self-registration, social login),
/// so it must not name an Arneb user, whichever claim carries it.
///
/// - `user_claim = "email"`: `email_verified` must be true.
/// - Any other claim whose value contains `@` (`sub` included: Okta puts
///   the login there, which a self-registering user picks; also
///   `preferred_username`, `upn`, a namespaced claim): `email_verified`
///   speaks only for the token's `email`, so that must be the same address
///   (ASCII case-insensitive) and verified; a different verified `email` is
///   refused (a user can pick a username that is someone else's address).
///   An explicit non-true `email_verified` is always refused. Otherwise
///   nothing vouches for the address (no `email_verified`, or no `email`
///   for it to speak for): only `allow_missing_email_verified` trusts it,
///   and then an unverified `email` vouches for nothing either way, so a
///   different one is ignored (Entra's `upn` beside its `email`).
fn check_email(
    settings: &OAuthSettings,
    claims: &serde_json::Value,
    user: &str,
) -> Result<(), OAuthReject> {
    let verified = || match claims.get("email_verified") {
        Some(serde_json::Value::Bool(b)) => Some(*b),
        Some(serde_json::Value::String(s)) => Some(s == "true"),
        Some(_) => Some(false),
        None => None,
    };
    if settings.user_claim == "email" {
        return match verified() {
            Some(true) => Ok(()),
            _ => Err(OAuthReject::UnverifiedEmail),
        };
    }
    if !user.contains('@') {
        return Ok(());
    }
    let email = claims.get("email").and_then(|v| v.as_str());
    match (email, verified()) {
        (_, Some(false)) => Err(OAuthReject::UnverifiedEmail),
        (Some(e), Some(true)) if e.eq_ignore_ascii_case(user) => Ok(()),
        (Some(_), Some(true)) => Err(OAuthReject::EmailMismatch),
        // Nothing vouches for the address: only the opt-in trusts it.
        _ if settings.allow_missing_email_verified => Ok(()),
        _ => Err(OAuthReject::UnverifiedEmail),
    }
}

/// A token bound to a key: a `cnf` claim of any shape (RFC 7800; DPoP's
/// `jkt`, mTLS's `x5t#S256`), or a `DPoP` type in the payload `typ`
/// (Keycloak's DPoP-bound access tokens) or the header `typ`.
fn is_sender_constrained(header_typ: Option<&str>, claims: &serde_json::Value) -> bool {
    let dpop = |t: &str| t.eq_ignore_ascii_case("DPoP") || t.eq_ignore_ascii_case("dpop+jwt");
    claims.get("cnf").is_some()
        || header_typ.is_some_and(dpop)
        || claims.get("typ").and_then(|v| v.as_str()).is_some_and(dpop)
}

/// Keycloak's payload `typ` of signed tokens that are not access tokens
/// (its access tokens say `Bearer`).
const NON_ACCESS_TOKEN_TYPS: [&str; 4] = ["ID", "Refresh", "Offline", "Logout"];

/// What a token's type markers say it is.
#[derive(Debug, PartialEq, Eq)]
enum TokenKind {
    /// Header `typ` `at+jwt` (RFC 9068), or payload `typ` `Bearer`
    /// (Keycloak, whose access tokens may carry `nonce`).
    Access,
    /// Payload `typ` `ID`, `Refresh`, `Offline` or `Logout` (Keycloak).
    NotAccess,
    /// No marker (Entra ID, Okta and most others send header `typ: JWT`).
    Unmarked,
}

/// The token's kind by its markers, strongest first: the header's `at+jwt`,
/// then the payload `typ` (case-insensitive).
fn token_kind(header_typ: Option<&str>, claims: &serde_json::Value) -> TokenKind {
    if header_typ.is_some_and(is_access_token_typ) {
        return TokenKind::Access;
    }
    match claims.get("typ").and_then(|v| v.as_str()) {
        Some(t) if t.eq_ignore_ascii_case("Bearer") => TokenKind::Access,
        Some(t)
            if NON_ACCESS_TOKEN_TYPS
                .iter()
                .any(|n| t.eq_ignore_ascii_case(n)) =>
        {
            TokenKind::NotAccess
        }
        _ => TokenKind::Unmarked,
    }
}

/// An unmarked token that may be an OIDC ID token: it has `nonce` or
/// `at_hash` (OIDC Core 2 and 3.1.3.6), or no scope at all (`scopes` as
/// [`token_scopes`] parses them, so a `null`, empty or non-string scope
/// counts as none). ID tokens carry no scope, and one from a refresh grant
/// has no `nonce` either, so a missing scope is the only sign it gives.
fn may_be_id_token(claims: &serde_json::Value, scopes: &[String]) -> bool {
    claims.get("nonce").is_some() || claims.get("at_hash").is_some() || scopes.is_empty()
}

/// The token's scopes: `scope` (a space-separated string) and `scp` (an
/// array of strings, or a space-separated string). Anything else is no
/// scope.
fn token_scopes(claims: &serde_json::Value) -> Vec<String> {
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
    scopes
}

/// `scopes` holds every one of `required`.
fn has_scopes(scopes: &[String], required: &[String]) -> bool {
    required.iter().all(|r| scopes.iter().any(|s| s == r))
}

enum Lookup {
    /// The keys that may have signed the token.
    Found(Vec<Arc<CachedKey>>),
    /// Keys exist for the `kid`, but none can verify the token's `alg`.
    Incompatible,
    /// No `kid` in the token and several keys to choose from.
    Ambiguous,
    Unknown,
    /// The cache is empty or too old to use.
    Stale,
}

fn lookup(keys: &[Arc<CachedKey>], kid: Option<&str>, alg: Algorithm) -> Lookup {
    let named: Vec<_> = match kid {
        Some(k) => keys
            .iter()
            .filter(|c| c.kid.as_deref() == Some(k))
            .cloned()
            .collect(),
        None if keys.len() == 1 => vec![keys[0].clone()],
        None => return Lookup::Ambiguous,
    };
    if named.is_empty() {
        return Lookup::Unknown;
    }
    let fitting: Vec<_> = named.into_iter().filter(|k| key_fits(k, alg)).collect();
    if fitting.is_empty() {
        Lookup::Incompatible
    } else {
        Lookup::Found(fitting)
    }
}

/// The JWK can verify `alg`: same `alg` when declared, else the same key
/// family and curve.
fn key_fits(key: &CachedKey, alg: Algorithm) -> bool {
    if let Some(declared) = key.alg {
        return key_alg(declared) == Some(alg);
    }
    match (&key.params, alg) {
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
            Self::rsa_with(kid, Algorithm::RS256)
        }

        /// The shared RSA key for `alg` (RS* or PS*), its JWK declaring it.
        pub fn rsa_with(kid: &str, alg: Algorithm) -> Self {
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
                "kty": "RSA", "kid": kid, "alg": alg, "use": "sig",
                "n": URL_SAFE_NO_PAD.encode(n), "e": URL_SAFE_NO_PAD.encode(e),
            }))
            .unwrap();
            Self {
                kid: kid.into(),
                alg,
                enc: EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap(),
                jwk,
            }
        }

        /// Sign `claims` with this key and a header without `kid`.
        pub fn sign_kidless(&self, claims: &serde_json::Value) -> String {
            jsonwebtoken::encode(&Header::new(self.alg), claims, &self.enc).unwrap()
        }

        /// Sign `claims` with this key and the standard header (`kid`, `alg`,
        /// `typ: JWT`).
        pub fn sign(&self, claims: &serde_json::Value) -> String {
            self.sign_typ(claims, Some("JWT"))
        }

        /// Sign `claims` with a header carrying `kid`, `alg` and `typ`.
        pub fn sign_typ(&self, claims: &serde_json::Value, typ: Option<&str>) -> String {
            let mut h = Header::new(self.alg);
            h.kid = Some(self.kid.clone());
            h.typ = typ.map(str::to_owned);
            jsonwebtoken::encode(&h, claims, &self.enc).unwrap()
        }
    }

    /// What the mock serves on `/jwks`.
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Mode {
        Ok,
        Fail500,
        Hang,
        /// Answer normally after this many milliseconds.
        Slow(u64),
    }

    struct Shared {
        keys: Mutex<Vec<serde_json::Value>>,
        mode: Mutex<Mode>,
        hits: AtomicUsize,
        discovery_issuer: Mutex<String>,
        discovery_jwks_uri: Mutex<Option<String>>,
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
            "jwks_uri": s.discovery_jwks_uri.lock().unwrap().clone()
                .unwrap_or_else(|| format!("{}/jwks", s.base)),
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
            Mode::Slow(ms) => {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                Ok(Json(
                    serde_json::json!({ "keys": s.keys.lock().unwrap().clone() }),
                ))
            }
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
                discovery_jwks_uri: Mutex::new(None),
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

        /// Make the discovery document point at another `jwks_uri`.
        pub fn set_discovery_jwks_uri(&self, uri: &str) {
            *self.shared.discovery_jwks_uri.lock().unwrap() = Some(uri.into());
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

    /// Claims of a valid access token for `issuer`/`aud`: `sub = "alice"`,
    /// `preferred_username = "alice"`, `scope = "openid"`, `exp` in five
    /// minutes.
    pub(crate) fn claims(issuer: &str, aud: &str) -> serde_json::Value {
        serde_json::json!({
            "iss": issuer, "aud": aud, "sub": "alice",
            "preferred_username": "alice", "scope": "openid",
            "iat": now(), "exp": now() + 300,
        })
    }
}

/// Well-known path prefix of RFC 9728 protected-resource metadata.
pub const METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

/// The path of `resource`'s metadata document (RFC 9728 section 3.1): the
/// well-known segment followed by the resource's path, or the bare segment
/// for a resource without a path.
pub fn metadata_path(resource: &str) -> String {
    match url::Url::parse(resource) {
        Ok(u) if u.path() != "/" => format!("{METADATA_PATH}{}", u.path()),
        _ => METADATA_PATH.to_owned(),
    }
}

/// The metadata URL of `resource` (RFC 9728 section 3.1): the well-known
/// segment inserted between the authority and the resource's path.
pub fn metadata_url(resource: &str) -> String {
    match url::Url::parse(resource) {
        Ok(u) => {
            let authority = &u[..url::Position::BeforePath];
            format!("{authority}{}", metadata_path(resource))
        }
        // Settings are validated at startup; this only guards a bad literal.
        Err(_) => format!("{}{METADATA_PATH}", resource.trim_end_matches('/')),
    }
}

/// `WWW-Authenticate` value for a `/mcp` request without credentials.
fn challenge(settings: &OAuthSettings) -> String {
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

/// `WWW-Authenticate` value for a presented token that is not valid. The
/// description is fixed: the reason stays in the DEBUG log (D10).
fn invalid_token_challenge(settings: &OAuthSettings) -> String {
    let mut v = String::from(
        r#"Bearer realm="arneb", error="invalid_token", error_description="the access token is not valid""#,
    );
    // The scope to re-authorize with, as in the no-credentials challenge.
    if !settings.required_scopes.is_empty() {
        v.push_str(&format!(
            r#", scope="{}""#,
            settings.required_scopes.join(" ")
        ));
    }
    v.push_str(&format!(
        r#", resource_metadata="{}""#,
        metadata_url(&settings.resource)
    ));
    v
}

/// `WWW-Authenticate` value for a valid token that lacks a required scope.
fn insufficient_scope_challenge(settings: &OAuthSettings) -> String {
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
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
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
            jwks_max_stale: Duration::from_secs(86400),
            require_access_token_typ: false,
            allow_missing_email_verified: false,
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
        assert_eq!(
            metadata_url("https://h"),
            "https://h/.well-known/oauth-protected-resource"
        );
        assert_eq!(
            metadata_path("https://h/arneb/mcp"),
            "/.well-known/oauth-protected-resource/arneb/mcp"
        );
        assert_eq!(
            metadata_path("https://h/"),
            "/.well-known/oauth-protected-resource"
        );
    }

    #[test]
    fn challenges_are_computed_once_and_carry_metadata_and_scopes() {
        let meta = "https://arneb.test/.well-known/oauth-protected-resource/mcp";
        let mut s = settings("https://idp.test");
        let v = OAuthValidator::new(s.clone());
        assert_eq!(
            v.challenge(),
            format!(r#"Bearer realm="arneb", resource_metadata="{meta}""#)
        );
        assert_eq!(
            v.invalid_token_challenge(),
            format!(
                r#"Bearer realm="arneb", error="invalid_token", error_description="the access token is not valid", resource_metadata="{meta}""#
            )
        );
        assert_eq!(
            v.metadata_path(),
            "/.well-known/oauth-protected-resource/mcp"
        );
        s.required_scopes = vec!["a".into(), "b".into()];
        let v = OAuthValidator::new(s);
        assert!(v.challenge().ends_with(r#"", scope="a b""#));
        // A refused token is told which scope to re-authorize with, too.
        assert_eq!(
            v.invalid_token_challenge(),
            format!(
                r#"Bearer realm="arneb", error="invalid_token", error_description="the access token is not valid", scope="a b", resource_metadata="{meta}""#
            )
        );
        assert_eq!(
            v.insufficient_scope_challenge(),
            format!(
                r#"Bearer realm="arneb", error="insufficient_scope", scope="a b", resource_metadata="{meta}""#
            )
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
            c.as_object_mut().unwrap().remove("scope");
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

        // alg=none (which does not even parse as a header) and HS256 keyed
        // with the public key are never accepted.
        let none = format!(
            "{}.{}.",
            b64(&json!({"alg": "none", "kid": "k1"})),
            b64(&claims(&iss, AUD))
        );
        assert_eq!(reject(none).await, invalid("malformed"));
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
        // The scope is checked before the address: an unverified one does
        // not mask the missing scope the client can fix.
        c["scope"] = json!("openid");
        c["sub"] = json!("alice@corp.example");
        assert_eq!(
            v.validate(&key.sign(&c)).await.unwrap_err(),
            OAuthReject::InsufficientScope
        );
    }

    #[tokio::test]
    async fn rotation_refetches_once_and_floods_are_rate_limited() {
        let old = TestKey::ec("old");
        let new = TestKey::ec("new");
        let idp = TestIdp::start(&[&old]).await;
        let v = validator(&idp, |s| s.jwks_min_refresh = Duration::from_millis(1000));
        v.validate(&old.sign(&claims(&idp.issuer(), AUD)))
            .await
            .unwrap();
        assert_eq!(idp.jwks_hits(), 1);
        tokio::time::sleep(Duration::from_millis(1200)).await;

        idp.set_keys(&[&old, &new]);
        v.validate(&new.sign(&claims(&idp.issuer(), AUD)))
            .await
            .unwrap();
        assert_eq!(idp.jwks_hits(), 2, "exactly one extra JWKS hit");

        // A flood of random kids inside the window refetches at most once.
        tokio::time::sleep(Duration::from_millis(1200)).await;
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

        // Right after the flood, inside the window, an unknown kid with keys
        // present is `unknown_key` and costs no further JWKS hit.
        let hits = idp.jwks_hits();
        let mut k = TestKey::ec("x");
        k.kid = "after-flood".into();
        assert_eq!(
            v.validate(&k.sign(&claims(&idp.issuer(), AUD)))
                .await
                .unwrap_err(),
            OAuthReject::Invalid("unknown_key")
        );
        assert_eq!(idp.jwks_hits(), hits, "throttled: no extra JWKS hit");
    }

    #[tokio::test]
    async fn idp_down_is_unavailable_until_it_returns() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        idp.set_mode(Mode::Fail500);
        let v = validator(&idp, |s| s.jwks_min_refresh = Duration::from_millis(1000));
        let t = key.sign(&claims(&idp.issuer(), AUD));
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        // Within the window nothing is refetched and there are no keys.
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        assert_eq!(idp.jwks_hits(), 1);

        idp.set_mode(Mode::Ok);
        tokio::time::sleep(Duration::from_millis(1200)).await;
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
    async fn failed_refreshes_keep_old_keys_until_max_stale() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| {
            s.jwks_cache = Duration::from_millis(200);
            s.jwks_min_refresh = Duration::from_millis(10);
            s.jwks_max_stale = Duration::from_millis(500);
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
        // ...past jwks_cache the old keys still decide (refetches fail)...
        tokio::time::sleep(Duration::from_millis(230)).await;
        v.validate(&t).await.unwrap();
        // ...and past jwks_max_stale nothing can.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        // The IdP returns: the next fetch serves again.
        idp.set_mode(Mode::Ok);
        tokio::time::sleep(Duration::from_millis(20)).await;
        v.validate(&t).await.unwrap();
    }

    #[tokio::test]
    async fn keys_are_refetched_in_the_background_before_they_expire() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| {
            s.jwks_cache = Duration::from_millis(500);
            s.jwks_min_refresh = Duration::from_millis(10);
        });
        let t = key.sign(&claims(&idp.issuer(), AUD));
        v.validate(&t).await.unwrap();
        assert_eq!(idp.jwks_hits(), 1);
        // Below 80% of jwks_cache: no refetch.
        tokio::time::sleep(Duration::from_millis(100)).await;
        v.validate(&t).await.unwrap();
        assert_eq!(idp.jwks_hits(), 1);
        // From 80% on, a slow IdP does not hold the request: it answers from
        // the cached keys and the refetch runs in the background.
        idp.set_mode(Mode::Slow(300));
        tokio::time::sleep(Duration::from_millis(320)).await;
        let started = std::time::Instant::now();
        v.validate(&t).await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(200));
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(idp.jwks_hits(), 2);
        // The refetch made the keys fresh again: no further hit right away.
        v.validate(&t).await.unwrap();
        assert_eq!(idp.jwks_hits(), 2);
    }

    #[tokio::test]
    async fn a_cancelled_request_does_not_cancel_the_fetch() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        idp.set_mode(Mode::Slow(300));
        // The default 30 s jwks_min_refresh: a lost fetch would mean 503s.
        let v = validator(&idp, |_| {});
        let t = key.sign(&claims(&idp.issuer(), AUD));
        // Clients that disconnect mid-fetch (the futures are dropped)...
        for _ in 0..5 {
            let r = tokio::time::timeout(Duration::from_millis(50), v.validate(&t)).await;
            assert!(r.is_err());
        }
        // ...start one fetch between them, which completes on its own.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(idp.jwks_hits(), 1);
        v.validate(&t).await.unwrap();
        assert_eq!(idp.jwks_hits(), 1);
    }

    #[tokio::test]
    async fn a_panicking_fetch_is_a_failed_attempt_and_refreshes_resume() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| s.jwks_min_refresh = Duration::from_millis(10));
        let t = key.sign(&claims(&idp.issuer(), AUD));
        // A waiter of the panicking fetch gets a verdict, not a hang...
        v.inner
            .panic_next_fetch
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        {
            let c = v.inner.lock();
            assert!(c.inflight.is_none(), "the dead flight is not left listed");
            assert!(!c.last_ok);
            assert_eq!(c.generation, 1);
        }
        // ...and the next attempt fetches normally.
        tokio::time::sleep(Duration::from_millis(20)).await;
        v.validate(&t).await.unwrap();
        assert_eq!(idp.jwks_hits(), 1);
    }

    #[tokio::test]
    async fn a_panicking_background_refresh_does_not_stall_later_ones() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| {
            s.jwks_cache = Duration::from_millis(200);
            s.jwks_min_refresh = Duration::from_millis(10);
        });
        let t = key.sign(&claims(&idp.issuer(), AUD));
        v.validate(&t).await.unwrap();
        assert_eq!(idp.jwks_hits(), 1);
        // Past 80% of jwks_cache the background refresh (nobody awaits it)
        // panics; the request is still answered from the cached keys.
        v.inner
            .panic_next_fetch
            .store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(170)).await;
        v.validate(&t).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        {
            let c = v.inner.lock();
            assert!(c.inflight.is_none(), "the dead flight is not left listed");
            assert!(!c.last_ok);
        }
        // The next request starts a fresh background refresh, which lands.
        v.validate(&t).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(idp.jwks_hits(), 2);
        assert!(v.inner.lock().last_ok);
    }

    #[tokio::test]
    async fn a_request_joins_the_fetch_in_flight() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        idp.set_mode(Mode::Slow(300));
        let v = validator(&idp, |_| {});
        let t = key.sign(&claims(&idp.issuer(), AUD));
        let r = tokio::time::timeout(Duration::from_millis(50), v.validate(&t)).await;
        assert!(r.is_err());
        // Inside the throttle window, a legitimate request waits for that
        // fetch instead of being refused.
        v.validate(&t).await.unwrap();
        assert_eq!(idp.jwks_hits(), 1);
    }

    #[tokio::test]
    async fn throttled_after_a_failed_attempt_is_unavailable_not_unknown_key() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| s.jwks_min_refresh = Duration::from_millis(1000));
        v.validate(&key.sign(&claims(&idp.issuer(), AUD)))
            .await
            .unwrap();
        // Valid keys cached, IdP goes down, a rotated kid shows up.
        idp.set_mode(Mode::Fail500);
        tokio::time::sleep(Duration::from_millis(1200)).await;
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
    async fn a_failed_fetch_forgets_the_discovered_jwks_uri() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| s.jwks_min_refresh = Duration::from_millis(1));
        let t = key.sign(&claims(&idp.issuer(), AUD));
        v.validate(&t).await.unwrap();
        assert!(v.inner.lock().jwks_uri.is_some());
        // The cached URL starts failing: it is forgotten, so the next
        // attempt goes back through discovery.
        idp.set_mode(Mode::Fail500);
        let unknown = TestKey::ec("zzz").sign(&claims(&idp.issuer(), AUD));
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            v.validate(&unknown).await.unwrap_err(),
            OAuthReject::Unavailable
        );
        assert!(v.inner.lock().jwks_uri.is_none());
        idp.set_mode(Mode::Ok);
        let rotated = TestKey::ec("k2");
        idp.set_keys(&[&key, &rotated]);
        tokio::time::sleep(Duration::from_millis(5)).await;
        v.validate(&rotated.sign(&claims(&idp.issuer(), AUD)))
            .await
            .unwrap();
        assert!(v.inner.lock().jwks_uri.is_some());
    }

    #[tokio::test]
    async fn a_configured_jwks_uri_is_kept_after_a_failure() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let uri = format!("{}/jwks", idp.issuer());
        let v = validator(&idp, |s| {
            s.jwks_uri = Some(uri);
            s.jwks_min_refresh = Duration::from_millis(1);
        });
        idp.set_mode(Mode::Fail500);
        let t = key.sign(&claims(&idp.issuer(), AUD));
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        idp.set_mode(Mode::Ok);
        tokio::time::sleep(Duration::from_millis(5)).await;
        v.validate(&t).await.unwrap();
    }

    #[tokio::test]
    async fn a_discovered_jwks_uri_must_be_https_or_loopback() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        idp.set_discovery_jwks_uri("http://idp.example.com/jwks");
        let v = validator(&idp, |_| {});
        let t = key.sign(&claims(&idp.issuer(), AUD));
        assert_eq!(v.validate(&t).await.unwrap_err(), OAuthReject::Unavailable);
        assert_eq!(idp.jwks_hits(), 0, "the insecure URL was never fetched");
    }

    #[tokio::test]
    async fn kidless_tokens_need_exactly_one_key() {
        let key = TestKey::ec("k1");
        let other = TestKey::ec("k2");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |_| {});
        let c = claims(&idp.issuer(), AUD);
        v.validate(&key.sign_kidless(&c)).await.unwrap();

        let idp2 = TestIdp::start(&[&key, &other]).await;
        let v2 = validator(&idp2, |_| {});
        let c2 = claims(&idp2.issuer(), AUD);
        assert_eq!(
            v2.validate(&key.sign_kidless(&c2)).await.unwrap_err(),
            OAuthReject::Invalid("unknown_key")
        );
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
        assert_eq!(s.jwks_max_stale, Duration::from_secs(86400));
        assert!(!s.require_access_token_typ);
    }

    #[tokio::test]
    async fn id_tokens_are_not_access_tokens() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |_| {});
        let iss = idp.issuer();
        let id_token = OAuthReject::Invalid("id_token");
        // Unmarked (`typ: JWT` or none: Entra, Okta) with OIDC-only claims,
        // or a Keycloak payload `typ` of a non-access token.
        for (claim, value) in [
            ("nonce", json!("n-0S6")),
            ("at_hash", json!("77QmUPtjPfzWtF2AnpK9RQ")),
            ("typ", json!("ID")),
            ("typ", json!("Refresh")),
            ("typ", json!("Offline")),
            ("typ", json!("Logout")),
            ("typ", json!("logout")),
        ] {
            let mut c = claims(&iss, AUD);
            c[claim] = value;
            assert_eq!(
                v.validate(&key.sign(&c)).await.unwrap_err(),
                id_token,
                "{claim}"
            );
            assert_eq!(
                v.validate(&key.sign_typ(&c, None)).await.unwrap_err(),
                id_token,
                "{claim} without typ"
            );
        }
        // An ID token from a refresh grant: no nonce, no at_hash, no scope.
        let mut refreshed = claims(&iss, AUD);
        refreshed.as_object_mut().unwrap().remove("scope");
        assert_eq!(
            v.validate(&key.sign(&refreshed)).await.unwrap_err(),
            id_token
        );
        // Keycloak's access tokens: `typ: JWT` header, payload `typ: Bearer`
        // (any case), with a `nonce` before Keycloak 24 or with its
        // "nonce backwards compatible" mapper, even without a scope.
        for typ in ["Bearer", "bearer"] {
            let mut c = claims(&iss, AUD);
            c["typ"] = json!(typ);
            v.validate(&key.sign(&c)).await.unwrap();
            v.validate(&key.sign_typ(&c, None)).await.unwrap();
            c["nonce"] = json!("n-0S6");
            c["at_hash"] = json!("77QmUPtjPfzWtF2AnpK9RQ");
            v.validate(&key.sign(&c)).await.unwrap();
            c.as_object_mut().unwrap().remove("scope");
            v.validate(&key.sign(&c)).await.unwrap();
        }
        // An `at+jwt` header is an access token by definition (RFC 9068),
        // and outranks a payload `typ`.
        let mut c = claims(&iss, AUD);
        c["nonce"] = json!("n");
        c["typ"] = json!("ID");
        v.validate(&key.sign_typ(&c, Some("at+jwt"))).await.unwrap();
        // Unmarked with a scope and neither nonce nor at_hash: an access token.
        v.validate(&key.sign(&claims(&iss, AUD))).await.unwrap();
    }

    #[tokio::test]
    async fn sender_constrained_tokens_are_refused() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let iss = idp.issuer();
        let bound = OAuthReject::Invalid("sender_constrained");
        let cnf = json!({"jkt": "0ZcOCORZNYy-DWpqq30jZyJGHTN0d2HglBV3uiguA4I"});
        // Keycloak 23: payload `typ: DPoP`, the request's nonce, a scope.
        let mut kc23 = claims(&iss, AUD);
        kc23["typ"] = json!("DPoP");
        kc23["nonce"] = json!("n-0S6");
        kc23["scope"] = json!("openid arneb");
        kc23["cnf"] = cnf.clone();
        // Keycloak 24+: no nonce. Without the refusal it would be accepted
        // as a plain bearer, `cnf.jkt` ignored.
        let mut kc24 = kc23.clone();
        kc24.as_object_mut().unwrap().remove("nonce");
        // A `cnf` alone (mTLS-bound, or a marked access token), and the
        // DPoP type alone.
        let mut mtls = claims(&iss, AUD);
        mtls["cnf"] = json!({"x5t#S256": "bwcK0esc3ACC3DB2Y5_lESsXE8o9ltc05O89jdN-dg2"});
        let mut bearer_bound = claims(&iss, AUD);
        bearer_bound["typ"] = json!("Bearer");
        bearer_bound["cnf"] = cnf;
        let mut typ_only = claims(&iss, AUD);
        typ_only["typ"] = json!("dpop");
        for (what, c) in [
            ("kc23", &kc23),
            ("kc24", &kc24),
            ("mtls", &mtls),
            ("bearer_bound", &bearer_bound),
            ("typ_only", &typ_only),
        ] {
            for required in [vec![], vec!["arneb".to_owned()]] {
                let v = validator(&idp, |s| s.required_scopes = required.clone());
                assert_eq!(
                    v.validate(&key.sign(c)).await.unwrap_err(),
                    bound,
                    "{what} {required:?}"
                );
                let at = key.sign_typ(c, Some("at+jwt"));
                assert_eq!(v.validate(&at).await.unwrap_err(), bound, "{what} at+jwt");
            }
        }
        // A `DPoP` header type too.
        let v = validator(&idp, |_| {});
        assert_eq!(
            v.validate(&key.sign_typ(&claims(&iss, AUD), Some("dpop+jwt")))
                .await
                .unwrap_err(),
            bound
        );
    }

    #[tokio::test]
    async fn required_scopes_vouch_for_unmarked_id_token_like_tokens() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| s.required_scopes = vec!["arneb".into()]);
        let iss = idp.issuer();
        // An ID token (no scope), with or without nonce: never accepted,
        // and a 401 `invalid_token` (not a 403 step-up), even when its
        // address is unverified or another `email` (checks that would be
        // a 403).
        let mut refreshed = claims(&iss, AUD);
        refreshed.as_object_mut().unwrap().remove("scope");
        let mut code_flow = refreshed.clone();
        code_flow["nonce"] = json!("n-0S6");
        let mut unverified = code_flow.clone();
        unverified["sub"] = json!("alice@corp.example");
        unverified["email"] = json!("other@corp.example");
        unverified["email_verified"] = json!(true);
        // A scope that does not parse is no scope; a nonce-bearing token
        // with only other scopes is no better.
        let mut null_scope = refreshed.clone();
        null_scope["scope"] = json!(null);
        let mut array_scope = refreshed.clone();
        array_scope["scope"] = json!(["arneb"]);
        let mut empty_scope = refreshed.clone();
        empty_scope["scope"] = json!("");
        let mut other_scope = code_flow.clone();
        other_scope["scope"] = json!("openid profile");
        for (what, c) in [
            ("refreshed", &refreshed),
            ("code_flow", &code_flow),
            ("unverified", &unverified),
            ("null_scope", &null_scope),
            ("array_scope", &array_scope),
            ("empty_scope", &empty_scope),
            ("other_scope", &other_scope),
        ] {
            assert_eq!(
                v.validate(&key.sign(c)).await.unwrap_err(),
                OAuthReject::Invalid("id_token"),
                "{what}"
            );
        }
        // An unmarked access token carrying a nonce passes once it carries
        // the required scope, which no ID token does.
        let mut c = code_flow.clone();
        c["scope"] = json!("openid arneb");
        assert_eq!(v.validate(&key.sign(&c)).await.unwrap().user, "alice");
        // Keycloak's payload `typ: ID` stays refused, scope or not.
        c["typ"] = json!("ID");
        assert_eq!(
            v.validate(&key.sign(&c)).await.unwrap_err(),
            OAuthReject::Invalid("id_token")
        );
    }

    #[tokio::test]
    async fn require_access_token_typ_wants_at_jwt() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| s.require_access_token_typ = true);
        let c = claims(&idp.issuer(), AUD);
        for typ in [Some("at+jwt"), Some("application/at+jwt"), Some("AT+JWT")] {
            v.validate(&key.sign_typ(&c, typ)).await.unwrap();
        }
        for typ in [Some("JWT"), None] {
            assert_eq!(
                v.validate(&key.sign_typ(&c, typ)).await.unwrap_err(),
                OAuthReject::Invalid("not_at_jwt"),
                "{typ:?}"
            );
        }
    }

    #[tokio::test]
    async fn an_email_user_claim_needs_a_verified_email() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| s.user_claim = "email".into());
        let mut c = claims(&idp.issuer(), AUD);
        c["email"] = json!("alice@corp.example");
        assert_eq!(
            v.validate(&key.sign(&c)).await.unwrap_err(),
            OAuthReject::UnverifiedEmail,
            "no email_verified"
        );
        for no in [json!(false), json!("false"), json!(1), json!(null)] {
            c["email_verified"] = no.clone();
            assert_eq!(
                v.validate(&key.sign(&c)).await.unwrap_err(),
                OAuthReject::UnverifiedEmail,
                "{no}"
            );
        }
        for yes in [json!(true), json!("true")] {
            c["email_verified"] = yes;
            assert_eq!(
                v.validate(&key.sign(&c)).await.unwrap().user,
                "alice@corp.example"
            );
        }
        // The opt-in for a missing email_verified never covers `email`.
        let v = validator(&idp, |s| {
            s.user_claim = "email".into();
            s.allow_missing_email_verified = true;
        });
        c.as_object_mut().unwrap().remove("email_verified");
        assert_eq!(
            v.validate(&key.sign(&c)).await.unwrap_err(),
            OAuthReject::UnverifiedEmail
        );
        // A user name that is not an address does not look at it.
        let v = validator(&idp, |s| s.user_claim = "preferred_username".into());
        c["email_verified"] = json!(false);
        assert_eq!(v.validate(&key.sign(&c)).await.unwrap().user, "alice");
    }

    #[tokio::test]
    async fn an_email_address_under_any_claim_needs_a_verified_email() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let addr = "alice@corp.example";
        // Keycloak "email as username", Entra `upn`, an Auth0 namespaced
        // claim: the address must be the token's verified `email`.
        for claim in ["preferred_username", "upn", "https://corp.example/email"] {
            let v = validator(&idp, |s| s.user_claim = claim.into());
            let lax = validator(&idp, |s| {
                s.user_claim = claim.into();
                s.allow_missing_email_verified = true;
            });
            // (email, email_verified, accepted strictly, accepted with the opt-in)
            for (email, verified, strict, opt_in) in [
                (Some(addr), Some(json!(true)), true, true),
                (Some("Alice@Corp.Example"), Some(json!("true")), true, true),
                (Some(addr), Some(json!(false)), false, false),
                (Some(addr), Some(json!("false")), false, false),
                (Some(addr), None, false, true),
                (None, None, false, true),
                // No `email` for `email_verified` to speak for.
                (None, Some(json!(true)), false, true),
                (None, Some(json!(false)), false, false),
            ] {
                let mut c = claims(&idp.issuer(), AUD);
                c[claim] = json!(addr);
                if let Some(e) = email {
                    c["email"] = json!(e);
                }
                if let Some(b) = &verified {
                    c["email_verified"] = b.clone();
                }
                let t = key.sign(&c);
                for (v, ok) in [(&v, strict), (&lax, opt_in)] {
                    let got = v.validate(&t).await.map(|p| p.user);
                    let want = if ok {
                        Ok(addr.to_owned())
                    } else {
                        Err(OAuthReject::UnverifiedEmail)
                    };
                    assert_eq!(got, want, "{claim} {email:?} {verified:?}");
                }
            }
        }
    }

    #[tokio::test]
    async fn a_username_that_is_someone_elses_address_is_refused() {
        // Self-registration: the attacker picks the username
        // admin@corp.example and verifies their own inbox.
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let mut c = claims(&idp.issuer(), AUD);
        c["preferred_username"] = json!("admin@corp.example");
        c["email"] = json!("attacker@evil.example");
        c["email_verified"] = json!(true);
        for lax in [false, true] {
            let v = validator(&idp, |s| {
                s.user_claim = "preferred_username".into();
                s.allow_missing_email_verified = lax;
            });
            assert_eq!(
                v.validate(&key.sign(&c)).await.unwrap_err(),
                OAuthReject::EmailMismatch,
                "opt-in {lax}"
            );
        }
    }

    #[tokio::test]
    async fn the_opt_in_ignores_an_unverified_other_email() {
        // Entra: `upn` (or a guest `#EXT#` UPN) beside an `email` that is a
        // different address, and never an `email_verified`.
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        for upn in [
            "alice@corp.onmicrosoft.com",
            "alice_corp.com#EXT#@tenant.onmicrosoft.com",
        ] {
            let mut c = claims(&idp.issuer(), AUD);
            c["upn"] = json!(upn);
            c["email"] = json!("alice@corp.com");
            let t = key.sign(&c);
            let lax = validator(&idp, |s| {
                s.user_claim = "upn".into();
                s.allow_missing_email_verified = true;
            });
            assert_eq!(lax.validate(&t).await.unwrap().user, upn);
            // Without the opt-in the address is simply unverified.
            let strict = validator(&idp, |s| s.user_claim = "upn".into());
            assert_eq!(
                strict.validate(&t).await.unwrap_err(),
                OAuthReject::UnverifiedEmail
            );
        }
    }

    #[tokio::test]
    async fn an_email_shaped_sub_is_checked_like_any_claim() {
        // Okta custom authorization server, `sub` = the login, self-service
        // registration without email verification: the attacker registers
        // the login admin@corp.example (an Arneb user with no Okta account
        // yet) and gets a token with that `sub` and no `email_verified`.
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let strict = validator(&idp, |_| {});
        let mut c = claims(&idp.issuer(), AUD);
        c["sub"] = json!("admin@corp.example");
        assert_eq!(
            strict.validate(&key.sign(&c)).await.unwrap_err(),
            OAuthReject::UnverifiedEmail
        );
        // Only the explicit opt-in trusts it.
        let lax = validator(&idp, |s| s.allow_missing_email_verified = true);
        assert_eq!(
            lax.validate(&key.sign(&c)).await.unwrap().user,
            "admin@corp.example"
        );
        // A verified `email` that is the same address vouches for it.
        c["email"] = json!("Admin@corp.example");
        c["email_verified"] = json!(true);
        assert_eq!(
            strict.validate(&key.sign(&c)).await.unwrap().user,
            "admin@corp.example"
        );
        // A verified other address, or `email_verified: false`, never does.
        c["email"] = json!("attacker@evil.example");
        for v in [&strict, &lax] {
            assert_eq!(
                v.validate(&key.sign(&c)).await.unwrap_err(),
                OAuthReject::EmailMismatch
            );
        }
        c["email"] = json!("admin@corp.example");
        c["email_verified"] = json!(false);
        for v in [&strict, &lax] {
            assert_eq!(
                v.validate(&key.sign(&c)).await.unwrap_err(),
                OAuthReject::UnverifiedEmail
            );
        }
        // An opaque `sub` is not an address and is not checked.
        c["sub"] = json!("00u1a2b3c4");
        assert_eq!(
            strict.validate(&key.sign(&c)).await.unwrap().user,
            "00u1a2b3c4"
        );
    }

    #[test]
    fn a_discovered_jwks_uri_of_a_remote_issuer_must_be_https() {
        let remote = "https://idp.example/realms/arneb";
        check_discovered_jwks_uri(remote, "https://idp.example/certs").unwrap();
        // Any https host: IdPs serve keys from a CDN or another domain.
        check_discovered_jwks_uri(remote, "https://keys.example.net/jwks").unwrap();
        for bad in [
            "http://127.0.0.1:8500/v1/agent/self",
            "http://localhost/jwks",
            "http://[::1]/jwks",
            "http://idp.example/certs",
        ] {
            let err = check_discovered_jwks_uri(remote, bad).unwrap_err();
            assert!(err.contains("https"), "{bad}: {err}");
        }
        // A loopback issuer (local testing) may keep plain http on loopback.
        let local = "http://127.0.0.1:8180/realms/arneb";
        check_discovered_jwks_uri(local, "http://127.0.0.1:8180/certs").unwrap();
        check_discovered_jwks_uri(local, "http://idp.example/certs").unwrap_err();
    }

    #[tokio::test]
    async fn every_key_under_a_kid_is_tried() {
        // An algorithm migration: RS256 and PS256 under one kid, RS256 first.
        let rs = TestKey::rsa_with("k1", Algorithm::RS256);
        let ps = TestKey::rsa_with("k1", Algorithm::PS256);
        let idp = TestIdp::start(&[&rs, &ps]).await;
        let v = validator(&idp, |_| {});
        let c = claims(&idp.issuer(), AUD);
        v.validate(&ps.sign(&c)).await.unwrap();
        v.validate(&rs.sign(&c)).await.unwrap();
        // A rotation reusing a kid: two EC keys, the token signed by the second.
        let first = TestKey::ec("k2");
        let second = TestKey::ec("k2");
        let idp = TestIdp::start(&[&first, &second]).await;
        let v = validator(&idp, |_| {});
        let c = claims(&idp.issuer(), AUD);
        v.validate(&second.sign(&c)).await.unwrap();
        v.validate(&first.sign(&c)).await.unwrap();
        // A key under the kid that verifies nothing is still bad_signature.
        assert_eq!(
            v.validate(&TestKey::ec("k2").sign(&c)).await.unwrap_err(),
            OAuthReject::Invalid("bad_signature")
        );
        // ...and a claim error from the key that verified is reported as is.
        let mut expired = claims(&idp.issuer(), AUD);
        expired["exp"] = json!(now() - 600);
        assert_eq!(
            v.validate(&second.sign(&expired)).await.unwrap_err(),
            OAuthReject::Invalid("expired")
        );
    }

    #[tokio::test]
    async fn a_finished_fetch_releases_its_reference() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| s.jwks_min_refresh = Duration::from_millis(10));
        let t = key.sign(&claims(&idp.issuer(), AUD));
        for _ in 0..3 {
            v.validate(&t).await.unwrap();
            v.warm().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(idp.jwks_hits() >= 2);
        // Only the validator holds `Inner`: no fetch task leaked its Arc.
        assert_eq!(Arc::strong_count(&v.inner), 1);
    }

    #[tokio::test]
    async fn a_poisoned_key_cache_keeps_serving() {
        let key = TestKey::ec("k1");
        let idp = TestIdp::start(&[&key]).await;
        let v = validator(&idp, |s| s.jwks_min_refresh = Duration::from_millis(10));
        let t = key.sign(&claims(&idp.issuer(), AUD));
        v.validate(&t).await.unwrap();
        // A panic while the cache is locked poisons the mutex...
        let inner = Arc::clone(&v.inner);
        std::thread::spawn(move || {
            let _held = inner.cache.lock().unwrap();
            panic!("test: panic while holding the key cache");
        })
        .join()
        .unwrap_err();
        assert!(v.inner.cache.is_poisoned());
        // ...and requests, refreshes and a lost fetch still work.
        v.validate(&t).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        v.warm().await;
        v.inner.record_lost_fetch();
        tokio::time::sleep(Duration::from_millis(20)).await;
        v.validate(&t).await.unwrap();
    }

    #[test]
    fn looks_like_jwt_cases() {
        assert!(looks_like_jwt("a.b.c"));
        for no in ["arneb_x.y.z", "arneb_s_abc", "a.b", "a..c", "a.b.c.d", ""] {
            assert!(!looks_like_jwt(no), "{no}");
        }
    }
}
