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
//! - A cache miss runs PBKDF2 on the blocking pool behind a small limiter,
//!   so unauthenticated requests cannot tie up the async workers.
//! - Identical concurrent misses share one verification (single-flight),
//!   and at most [`MAX_QUEUED_VERIFICATIONS`] distinct misses wait for a
//!   slot: past that a miss is `503` with `Retry-After` (Trino clients
//!   retry 503), so a flood of bad credentials cannot build an unbounded
//!   queue in front of a legitimate client whose cache entry expired. The
//!   TTL is deliberately not refreshed on a hit, so a revoked or changed
//!   password still stops working within [`CACHE_TTL`].
//! - A [token-shaped](crate::auth::is_token_shaped) password is an API
//!   token: it is checked with a SHA-256 compare on every request, bypassing
//!   the cache, the limiter and the queue, so a revoked token is refused at
//!   once.

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
use tokio::sync::{OnceCell, Semaphore};

use super::session::claimed_users;
use crate::auth::{hmac_sha256, is_token_shaped, AuthConfigError, CredentialSource};

const CACHE_TTL: Duration = Duration::from_secs(300);
const CACHE_CAPACITY: usize = 1024;
/// PBKDF2 verifications (cache misses) allowed to run at once; further
/// misses wait for a slot. Cache hits never wait.
const MAX_CONCURRENT_VERIFICATIONS: usize = 4;
/// Distinct cache misses allowed to wait for a verification slot on top of
/// the [`MAX_CONCURRENT_VERIFICATIONS`] running ones. At 4 slots and tens of
/// milliseconds per PBKDF2 derivation, 64 waiters is well under a second of
/// queue: enough to absorb a burst of legitimate first polls, small enough
/// that a flood is rejected (`503`) instead of delaying everyone.
const MAX_QUEUED_VERIFICATIONS: usize = 64;
/// `Retry-After` (seconds) on the `503` returned when the queue is full.
const OVERLOADED_RETRY_AFTER_SECS: &str = "1";

/// Paths reachable without credentials (load-balancer health checks).
const PUBLIC_PATHS: &[&str] = &["/v1/info", "/v1/info/state", "/v1/status"];

/// `(user, HMAC(cache_key, password))`: never holds the password itself.
type CredKey = (String, [u8; 32]);
/// Successful verifications → when they were verified.
type VerifyCache = HashMap<CredKey, Instant>;
/// Verifications queued or running, one shared result cell per credential.
type InFlight = HashMap<CredKey, Arc<OnceCell<bool>>>;

/// Outcome of [`BasicAuthenticator::check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasicVerdict {
    /// The credentials are valid.
    Valid,
    /// Wrong password, unknown user, or a dead token.
    Invalid,
    /// The verification queue is full; the client should retry.
    Overloaded,
}

/// The user a request authenticated as, stored in request extensions.
#[derive(Debug, Clone)]
pub(crate) struct AuthUser(pub(crate) String);

/// HTTP Basic credential checker: a success cache, a PBKDF2 limiter and a
/// single-flight map in front of a [`CredentialSource`]. Cheap to clone.
#[derive(Clone)]
pub struct BasicAuthenticator {
    creds: Arc<dyn CredentialSource>,
    cache_key: [u8; 32],
    cache: Arc<Mutex<VerifyCache>>,
    /// Number of full (PBKDF2) verifications; read by tests.
    verifications: Arc<AtomicU64>,
    /// Slots for concurrent PBKDF2 verifications.
    limiter: Arc<Semaphore>,
    /// Single-flight map; its size bounds the verification queue.
    in_flight: Arc<Mutex<InFlight>>,
}

/// One request's membership in an in-flight verification. On drop it
/// removes the map entry once the result is known, or when it is the last
/// participant (a cancelled request must not leave a slot behind).
struct FlightGuard<'a> {
    in_flight: &'a Mutex<InFlight>,
    key: &'a CredKey,
    cell: Arc<OnceCell<bool>>,
}

impl Drop for FlightGuard<'_> {
    fn drop(&mut self) {
        let mut map = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        // Participants join only under this lock, so a strong count of 2
        // (map + this guard) means nobody else is waiting on the cell.
        if map
            .get(self.key)
            .is_some_and(|c| Arc::ptr_eq(c, &self.cell))
            && (self.cell.initialized() || Arc::strong_count(&self.cell) == 2)
        {
            map.remove(self.key);
        }
    }
}

impl BasicAuthenticator {
    /// Build an authenticator over `creds`.
    pub fn new(creds: Arc<dyn CredentialSource>) -> Result<Self, AuthConfigError> {
        Self::with_counter(creds, Arc::new(AtomicU64::new(0)))
    }

    /// Like [`BasicAuthenticator::new`], counting PBKDF2 verifications in
    /// `verifications`.
    pub(crate) fn with_counter(
        creds: Arc<dyn CredentialSource>,
        verifications: Arc<AtomicU64>,
    ) -> Result<Self, AuthConfigError> {
        let mut key = [0u8; 32];
        aws_lc_rs::rand::fill(&mut key).map_err(|_| AuthConfigError::Random)?;
        Ok(Self {
            creds,
            cache_key: key,
            cache: Arc::new(Mutex::new(HashMap::new())),
            verifications,
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_VERIFICATIONS)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn is_cached(&self, key: &CredKey, now: Instant) -> bool {
        let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache
            .get(key)
            .is_some_and(|at| now.duration_since(*at) < CACHE_TTL)
    }

    /// Check `user:password`. A cache hit answers at once and never queues;
    /// otherwise identical concurrent misses share one PBKDF2 derivation on
    /// the blocking pool, at most [`MAX_CONCURRENT_VERIFICATIONS`] at a time
    /// with at most [`MAX_QUEUED_VERIFICATIONS`] more waiting, so a flood of
    /// bad credentials can neither stall the async workers, use more than a
    /// few cores, nor build an unbounded queue.
    ///
    /// A token-shaped password skips all of that: it is checked directly
    /// (one SHA-256 compare) and never cached, so revocation is immediate.
    pub async fn check(&self, user: &str, password: &str) -> BasicVerdict {
        if is_token_shaped(password) {
            return if self.creds.verify_token(password).as_deref() == Some(user) {
                BasicVerdict::Valid
            } else {
                BasicVerdict::Invalid
            };
        }
        let digest = hmac_sha256(&self.cache_key, password.as_bytes());
        let key = (user.to_owned(), digest);
        if self.is_cached(&key, Instant::now()) {
            return BasicVerdict::Valid;
        }
        let cell = {
            let mut map = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            match map.get(&key) {
                Some(cell) => Arc::clone(cell),
                None => {
                    if map.len() >= MAX_CONCURRENT_VERIFICATIONS + MAX_QUEUED_VERIFICATIONS {
                        return BasicVerdict::Overloaded;
                    }
                    let cell = Arc::new(OnceCell::new());
                    map.insert(key.clone(), Arc::clone(&cell));
                    cell
                }
            }
        };
        let guard = FlightGuard {
            in_flight: &self.in_flight,
            key: &key,
            cell,
        };
        // If the request running the verification is dropped, another
        // participant's `get_or_init` takes over.
        let ok = *guard
            .cell
            .get_or_init(|| self.verify_uncached(&key, user, password))
            .await;
        drop(guard);
        if ok {
            BasicVerdict::Valid
        } else {
            BasicVerdict::Invalid
        }
    }

    /// Run (or, if a slot frees up after another flight cached the same
    /// credentials, skip) the PBKDF2 verification and cache a success.
    async fn verify_uncached(&self, key: &CredKey, user: &str, password: &str) -> bool {
        let Ok(permit) = Arc::clone(&self.limiter).acquire_owned().await else {
            return false;
        };
        if self.is_cached(key, Instant::now()) {
            return true;
        }
        self.verifications.fetch_add(1, Ordering::Relaxed);
        let creds = Arc::clone(&self.creds);
        let (u, p) = (user.to_owned(), password.to_owned());
        // The permit moves into the blocking task, so a request dropped
        // mid-verification still holds its slot until PBKDF2 finishes.
        let ok = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            creds.verify_password(&u, &p)
        })
        .await
        .unwrap_or(false);
        if !ok {
            return false;
        }
        let now = Instant::now();
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
        cache.insert(key.clone(), now);
        true
    }
}

fn overloaded() -> Response {
    let mut resp = (
        StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(json!({ "error": {
            "message": "Too many pending authentication requests; retry later",
            "errorName": "SERVICE_UNAVAILABLE",
        } })),
    )
        .into_response();
    resp.headers_mut().insert(
        "Retry-After",
        HeaderValue::from_static(OVERLOADED_RETRY_AFTER_SECS),
    );
    resp
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
/// [`BasicAuthenticator`].
pub(crate) async fn require_auth(
    State(auth): State<Option<BasicAuthenticator>>,
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
    if user.is_empty() {
        return unauthorized("Invalid credentials");
    }
    match auth.check(&user, &password).await {
        BasicVerdict::Valid => {}
        BasicVerdict::Invalid => {
            tracing::warn!(user = %user, "Trino password authentication failed");
            return unauthorized("Invalid credentials");
        }
        BasicVerdict::Overloaded => {
            tracing::warn!(user = %user, "Trino authentication queue full; returning 503");
            return overloaded();
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{ScramVerifier, UserCredentials};

    fn authenticator() -> (BasicAuthenticator, Arc<AtomicU64>) {
        let creds = UserCredentials::new(vec![(
            "alice".to_owned(),
            ScramVerifier::generate("alice-pw").unwrap(),
        )])
        .unwrap();
        let counter = Arc::new(AtomicU64::new(0));
        let auth = BasicAuthenticator::with_counter(Arc::new(creds), Arc::clone(&counter)).unwrap();
        (auth, counter)
    }

    #[tokio::test]
    async fn cache_misses_wait_for_a_verification_slot() {
        let (auth, counter) = authenticator();
        let held = Arc::clone(&auth.limiter)
            .acquire_many_owned(MAX_CONCURRENT_VERIFICATIONS as u32)
            .await
            .unwrap();
        let pending = tokio::spawn({
            let auth = auth.clone();
            async move { auth.check("alice", "wrong").await == BasicVerdict::Valid }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!pending.is_finished(), "verification must wait for a slot");
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        drop(held);
        assert!(!pending.await.unwrap());
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert_eq!(
            auth.limiter.available_permits(),
            MAX_CONCURRENT_VERIFICATIONS
        );
    }

    #[tokio::test]
    async fn cache_hits_skip_the_limiter() {
        let (auth, counter) = authenticator();
        assert_eq!(auth.check("alice", "alice-pw").await, BasicVerdict::Valid);
        let _held = Arc::clone(&auth.limiter)
            .acquire_many_owned(MAX_CONCURRENT_VERIFICATIONS as u32)
            .await
            .unwrap();
        let hit =
            tokio::time::timeout(Duration::from_secs(1), auth.check("alice", "alice-pw")).await;
        assert_eq!(hit.ok(), Some(BasicVerdict::Valid));
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    fn hold_all_slots(auth: &BasicAuthenticator) -> tokio::sync::OwnedSemaphorePermit {
        Arc::clone(&auth.limiter)
            .try_acquire_many_owned(MAX_CONCURRENT_VERIFICATIONS as u32)
            .unwrap()
    }

    fn in_flight_len(auth: &BasicAuthenticator) -> usize {
        auth.in_flight.lock().unwrap().len()
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn identical_concurrent_misses_verify_once() {
        for (password, expected) in [
            ("alice-pw", BasicVerdict::Valid),
            ("wrong", BasicVerdict::Invalid),
        ] {
            let (auth, counter) = authenticator();
            // Hold every slot so all N requests are in flight together,
            // more than the slot count (each would otherwise run PBKDF2).
            let held = hold_all_slots(&auth);
            let tasks: Vec<_> = (0..MAX_CONCURRENT_VERIFICATIONS * 4)
                .map(|_| {
                    let auth = auth.clone();
                    tokio::spawn(async move { auth.check("alice", password).await })
                })
                .collect();
            settle().await;
            assert_eq!(in_flight_len(&auth), 1, "one shared flight per credential");
            drop(held);
            for t in tasks {
                assert_eq!(t.await.unwrap(), expected);
            }
            assert_eq!(counter.load(Ordering::Relaxed), 1, "PBKDF2 ran once");
            assert_eq!(in_flight_len(&auth), 0);
        }
    }

    #[tokio::test]
    async fn cancelled_flight_does_not_leak_a_queue_slot() {
        let (auth, _) = authenticator();
        let held = hold_all_slots(&auth);
        let task = tokio::spawn({
            let auth = auth.clone();
            async move { auth.check("alice", "wrong").await }
        });
        settle().await;
        assert_eq!(in_flight_len(&auth), 1);
        task.abort();
        let _ = task.await;
        assert_eq!(in_flight_len(&auth), 0);
        drop(held);
    }

    #[tokio::test]
    async fn full_queue_rejects_new_misses_but_not_hits_or_joins() {
        let (auth, counter) = authenticator();
        assert_eq!(auth.check("alice", "alice-pw").await, BasicVerdict::Valid);
        let held = hold_all_slots(&auth);
        let bound = MAX_CONCURRENT_VERIFICATIONS + MAX_QUEUED_VERIFICATIONS;
        let flood: Vec<_> = (0..bound)
            .map(|i| {
                let auth = auth.clone();
                tokio::spawn(async move { auth.check("alice", &format!("bad-{i}")).await })
            })
            .collect();
        settle().await;
        assert_eq!(in_flight_len(&auth), bound);

        // A new distinct miss is refused at once instead of queueing.
        let rejected =
            tokio::time::timeout(Duration::from_secs(1), auth.check("alice", "bad-new")).await;
        assert_eq!(rejected.ok(), Some(BasicVerdict::Overloaded));
        // A cached credential never touches the queue.
        let hit =
            tokio::time::timeout(Duration::from_secs(1), auth.check("alice", "alice-pw")).await;
        assert_eq!(hit.ok(), Some(BasicVerdict::Valid));
        // An identical miss joins its existing flight rather than being refused.
        let join = tokio::spawn({
            let auth = auth.clone();
            async move { auth.check("alice", "bad-0").await }
        });
        settle().await;
        assert!(!join.is_finished());

        drop(held);
        assert_eq!(join.await.unwrap(), BasicVerdict::Invalid);
        for t in flood {
            assert_eq!(t.await.unwrap(), BasicVerdict::Invalid);
        }
        assert_eq!(counter.load(Ordering::Relaxed), 1 + bound as u64);
        assert_eq!(in_flight_len(&auth), 0);
    }

    #[test]
    fn overloaded_response_is_503_with_retry_after() {
        let resp = overloaded();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers().get("Retry-After").unwrap(),
            OVERLOADED_RETRY_AFTER_SECS
        );
    }
}
