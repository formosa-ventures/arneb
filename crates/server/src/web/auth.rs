//! Authentication for the Web port (`/api/*`, `/mcp` and, with `[web]
//! metrics_auth`, `/metrics`).
//!
//! Under `[auth] type = "password"`:
//! - `Authorization: Bearer <token>` is checked first: a session token
//!   (`arneb_s_…`) against the [`SessionStore`](super::session::SessionStore),
//!   anything else as an API token against the credential source.
//! - Otherwise `Authorization: Basic` goes through the shared
//!   [`BasicAuthenticator`](arneb_protocol::BasicAuthenticator), so PBKDF2
//!   never runs on an async worker.
//! - Anything else, or a bad credential of any kind, is the same `401`
//!   `unauthenticated` envelope: unknown token id, wrong secret, unknown
//!   user and wrong password cannot be told apart.
//! - [`PUBLIC_ROUTES`] and everything outside `/api/` (the static frontend,
//!   and `/metrics` unless `metrics_auth`) stay open.
//!
//! Under `none` every request passes, as `X-Arneb-User` (or `anonymous`)
//! with [`AuthVia::Unauthenticated`].
//!
//! The authenticated [`Identity`] is stored in the request extensions.

use arneb_protocol::BasicVerdict;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, Method};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;

use super::error::{ApiError, ErrorCode};
use super::session::SESSION_PREFIX;
use super::WebState;

/// How a request authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthVia {
    /// `Authorization: Bearer` with an API token.
    Token,
    /// `Authorization: Bearer` with a Web session token.
    Session,
    /// `Authorization: Basic` (a password or an API token as the password).
    Basic,
    /// `[auth] type = "none"`: the user is only claimed.
    Unauthenticated,
}

impl AuthVia {
    /// `token`, `session`, `basic` or `unauthenticated`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Token => "token",
            Self::Session => "session",
            Self::Basic => "basic",
            Self::Unauthenticated => "unauthenticated",
        }
    }
}

/// Who a request runs as; a request extension set by [`authenticate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub user: String,
    pub via: AuthVia,
}

/// Routes under `/api/` reachable without credentials: `(method, path)`,
/// `None` for any method. Everything outside `/api/` (the static frontend)
/// is public as well, and so is `/metrics` unless `metrics_auth`.
pub const PUBLIC_ROUTES: &[(Option<Method>, &str)] = &[
    (None, "/api/v1/info"),
    (None, "/api/v2/health"),
    (Some(Method::POST), "/api/v2/sessions"),
];

/// Whether `method path` needs credentials under password auth: every
/// `/api/` path except [`PUBLIC_ROUTES`], all of `/mcp` when it is served
/// (`mcp_served`), and `/metrics` when `metrics_auth`. The single place
/// that decides what is public. An unserved `/mcp` is public so that it
/// answers its documented `404`, not a `401` that would start a client's
/// credential flow against an endpoint that does not exist.
pub fn requires_auth(method: &Method, path: &str, metrics_auth: bool, mcp_served: bool) -> bool {
    if path == "/metrics" {
        return metrics_auth;
    }
    if path == "/mcp" || path.starts_with("/mcp/") {
        return mcp_served;
    }
    path.starts_with("/api/")
        && !PUBLIC_ROUTES
            .iter()
            .any(|(m, p)| *p == path && m.as_ref().is_none_or(|m| m == method))
}

/// The one rejection for every missing or bad credential.
fn unauthenticated() -> Response {
    ApiError::new(ErrorCode::Unauthenticated, "authentication required")
        .with_hint("send `Authorization: Bearer <token>` or `Authorization: Basic`")
        .into_response()
}

/// Middleware: authenticate the request (see the module docs).
pub async fn authenticate(State(state): State<WebState>, mut req: Request, next: Next) -> Response {
    let Some(creds) = &state.credentials else {
        let user = claimed_user(req.headers()).unwrap_or_else(|| "anonymous".to_owned());
        req.extensions_mut().insert(Identity {
            user,
            via: AuthVia::Unauthenticated,
        });
        return next.run(req).await;
    };
    // Password auth is on, so a missing checker is a wiring bug: refuse,
    // never fall back to `none`.
    let Some(basic) = &state.basic else {
        tracing::error!("password auth is configured without a Basic authenticator");
        return ApiError::new(ErrorCode::Internal, "authentication is misconfigured")
            .into_response();
    };
    if !requires_auth(
        req.method(),
        req.uri().path(),
        state.metrics_auth,
        state.mcp.is_some(),
    ) {
        return next.run(req).await;
    }
    let Some(authorization) = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return unauthenticated();
    };
    let (scheme, value) = authorization.split_once(' ').unwrap_or((authorization, ""));
    let value = value.trim();
    let identity = if scheme.eq_ignore_ascii_case("bearer") {
        if value.starts_with(SESSION_PREFIX) {
            state.sessions.resolve(value).map(|user| Identity {
                user,
                via: AuthVia::Session,
            })
        } else {
            creds.verify_token(value).map(|user| Identity {
                user,
                via: AuthVia::Token,
            })
        }
    } else if scheme.eq_ignore_ascii_case("basic") {
        let Some((user, password)) = decode_basic(value) else {
            return unauthenticated();
        };
        match basic.check(&user, &password).await {
            BasicVerdict::Valid => Some(Identity {
                user,
                via: AuthVia::Basic,
            }),
            BasicVerdict::Invalid => None,
            BasicVerdict::Overloaded => {
                return ApiError::new(
                    ErrorCode::Overloaded,
                    "too many pending authentication requests",
                )
                .with_hint("retry shortly, or authenticate with a bearer token")
                .into_response();
            }
        }
    } else {
        None
    };
    match identity {
        Some(identity) => {
            req.extensions_mut().insert(identity);
            next.run(req).await
        }
        None => unauthenticated(),
    }
}

/// `user:password` from a Basic credential.
pub(super) fn decode_basic(value: &str) -> Option<(String, String)> {
    let decoded = String::from_utf8(STANDARD.decode(value).ok()?).ok()?;
    let (user, password) = decoded.split_once(':')?;
    Some((user.to_owned(), password.to_owned()))
}

/// The non-blank `X-Arneb-User` (only trusted under `[auth] type = "none"`).
fn claimed_user(headers: &HeaderMap) -> Option<String> {
    let user = headers.get("x-arneb-user")?.to_str().ok()?.trim();
    (!user.is_empty()).then(|| user.to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arneb_protocol::{CredentialSource, ScramVerifier};
    use axum::body::Body;
    use axum::http::{HeaderValue, StatusCode};
    use tower::ServiceExt as _;

    use super::*;
    use crate::web::{build_router, SessionStore};

    const ALICE_TOKEN: &str = "arneb_0123456789abcdef_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    /// Same secret, an id no token has.
    const UNKNOWN_ID: &str = "arneb_fedcba9876543210_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    /// Alice's id, another secret.
    const WRONG_SECRET: &str = "arneb_0123456789abcdef_BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    #[derive(Debug)]
    struct TestCreds;

    impl CredentialSource for TestCreds {
        fn scram_verifiers(&self, _user: &str) -> Vec<ScramVerifier> {
            Vec::new()
        }
        fn verify_password(&self, user: &str, password: &str) -> bool {
            matches!((user, password), ("alice", "secret") | ("bob", "hunter2"))
        }
        fn verify_token(&self, token: &str) -> Option<String> {
            (token == ALICE_TOKEN).then(|| "alice".to_owned())
        }
        fn user_count(&self) -> usize {
            2
        }
    }

    fn web_state(password: bool, rules: Option<&str>, metrics_auth: bool) -> WebState {
        let credentials: Option<Arc<dyn CredentialSource>> =
            password.then(|| Arc::new(TestCreds) as _);
        let access_rules =
            rules.map(|r| Arc::new(arneb_catalog::AccessRules::from_json(r).unwrap()));
        WebState {
            authorizer: crate::web::privilege::Authorizer::new(
                access_rules.clone(),
                Vec::new(),
                password,
            ),
            query_tracker: Arc::new(arneb_scheduler::QueryTracker::new()),
            node_registry: arneb_scheduler::NodeRegistry::new(30),
            start_time: std::time::Instant::now(),
            role: "standalone".into(),
            admission: None,
            admission_changes: Default::default(),
            max_queued_queries: 8,
            memory_pool: Arc::new(arneb_common::memory_pool::GreedyMemoryPool::new(1 << 20)),
            memory_limit_bytes: None,
            basic: credentials
                .clone()
                .map(|c| arneb_protocol::BasicAuthenticator::new(c).unwrap()),
            credentials,
            state: None,
            audit: crate::web::test_audit_log(),
            access_rules,
            metrics_auth,
            sessions: SessionStore::new(),
            agent: None,
            mcp: None,
        }
    }

    fn basic(user: &str, password: &str) -> String {
        format!("Basic {}", STANDARD.encode(format!("{user}:{password}")))
    }

    struct Reply {
        status: StatusCode,
        www_authenticate: Option<HeaderValue>,
        body: Vec<u8>,
    }

    impl Reply {
        fn json(&self) -> serde_json::Value {
            serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
        }
    }

    async fn send(state: &WebState, method: Method, path: &str, headers: &[(&str, &str)]) -> Reply {
        let mut req = axum::http::Request::builder().method(method).uri(path);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let resp = build_router(state.clone())
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        Reply {
            status: resp.status(),
            www_authenticate: resp.headers().get(header::WWW_AUTHENTICATE).cloned(),
            body: axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        }
    }

    async fn get(state: &WebState, path: &str, auth: Option<&str>) -> Reply {
        let headers: Vec<(&str, &str)> = auth.map(|a| ("authorization", a)).into_iter().collect();
        send(state, Method::GET, path, &headers).await
    }

    #[tokio::test]
    async fn api_v1_requires_credentials_under_password_auth() {
        let state = web_state(true, None, false);
        for path in [
            "/api/v1/queries",
            "/api/v1/cluster",
            "/api/v1/cluster/workers",
            "/api/v2/tokens",
        ] {
            let reply = get(&state, path, None).await;
            assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{path}");
            assert_eq!(reply.json()["error"]["code"], "unauthenticated", "{path}");
            assert_eq!(
                reply.www_authenticate.as_ref().map(|v| v.to_str().unwrap()),
                Some(r#"Bearer realm="arneb""#)
            );
        }
        let reply = get(&state, "/api/v1/queries", Some("Digest whatever")).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn info_and_health_stay_public() {
        let state = web_state(true, None, false);
        let reply = get(&state, "/api/v1/info", None).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.json()["version"].is_string());
        // The static frontend (and the SPA fallback) never asks for credentials.
        for path in ["/", "/index.html", "/queries/abc"] {
            assert_ne!(
                get(&state, path, None).await.status,
                StatusCode::UNAUTHORIZED
            );
        }

        // `/api/v2`'s public routes are decided here.
        assert!(!requires_auth(&Method::GET, "/api/v2/health", false, true));
        assert!(!requires_auth(
            &Method::POST,
            "/api/v2/sessions",
            false,
            true
        ));
        assert!(requires_auth(&Method::GET, "/api/v2/sessions", false, true));
        assert!(requires_auth(
            &Method::DELETE,
            "/api/v2/sessions",
            false,
            true
        ));
        assert!(requires_auth(&Method::GET, "/api/v2/tokens", false, true));
        assert!(requires_auth(&Method::GET, "/api/v1/info/", false, true));
        assert!(requires_auth(&Method::GET, "/api/v1/queries", false, true));
        assert!(!requires_auth(&Method::GET, "/", false, true));
        assert!(!requires_auth(&Method::GET, "/metrics", false, true));
        assert!(requires_auth(&Method::GET, "/metrics", true, true));
        // `/mcp` needs credentials exactly when it is served.
        for path in ["/mcp", "/mcp/", "/mcp/x"] {
            assert!(requires_auth(&Method::POST, path, false, true), "{path}");
            assert!(!requires_auth(&Method::POST, path, false, false), "{path}");
        }
        assert!(requires_auth(&Method::GET, "/api/v2/tokens", false, false));
    }

    #[tokio::test]
    async fn bearer_token_and_basic_both_authenticate() {
        let state = web_state(true, None, false);
        let session = state.sessions.create("bob").unwrap();
        for auth in [
            format!("Bearer {ALICE_TOKEN}"),
            format!("bearer {ALICE_TOKEN}"),
            format!("Bearer {session}"),
            basic("alice", "secret"),
            basic("bob", "hunter2"),
            basic("alice", ALICE_TOKEN),
        ] {
            let reply = get(&state, "/api/v1/queries", Some(&auth)).await;
            assert_eq!(reply.status, StatusCode::OK, "{auth}");
        }

        // The identity each scheme yields, seen by a handler.
        let probe = axum::Router::new()
            .route(
                "/api/v1/whoami",
                axum::routing::get(
                    |axum::Extension(id): axum::Extension<Identity>| async move {
                        format!("{}:{:?}", id.user, id.via)
                    },
                ),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                authenticate,
            ))
            .with_state(state.clone());
        for (auth, expect) in [
            (format!("Bearer {ALICE_TOKEN}"), "alice:Token"),
            (format!("Bearer {session}"), "bob:Session"),
            (basic("bob", "hunter2"), "bob:Basic"),
        ] {
            let resp = probe
                .clone()
                .oneshot(
                    axum::http::Request::get("/api/v1/whoami")
                        .header("authorization", &auth)
                        // Ignored under password auth.
                        .header("x-arneb-user", "mallory")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(&body[..], expect.as_bytes());
        }

        // A session token is only a bearer: not a Basic password.
        let reply = get(&state, "/api/v1/queries", Some(&basic("bob", &session))).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn unknown_token_id_and_wrong_secret_are_indistinguishable() {
        let state = web_state(true, None, false);
        let revoked = state.sessions.create("alice").unwrap();
        state.sessions.revoke(&revoked);
        let attempts = [
            format!("Bearer {UNKNOWN_ID}"),
            format!("Bearer {WRONG_SECRET}"),
            format!("Bearer {revoked}"),
            "Bearer arneb_s_nope".to_owned(),
            "Bearer not-a-token".to_owned(),
            "Bearer".to_owned(),
            basic("alice", "wrong"),
            basic("nobody", "secret"),
            basic("alice", WRONG_SECRET),
            basic("bob", ALICE_TOKEN),
            "Basic !!!not-base64".to_owned(),
        ];
        let baseline = get(&state, "/api/v1/queries", None).await;
        assert_eq!(baseline.status, StatusCode::UNAUTHORIZED);
        for auth in attempts {
            let reply = get(&state, "/api/v1/queries", Some(&auth)).await;
            assert_eq!(reply.status, baseline.status, "{auth}");
            assert_eq!(reply.www_authenticate, baseline.www_authenticate, "{auth}");
            assert_eq!(reply.body, baseline.body, "{auth}");
        }
    }

    #[tokio::test]
    async fn credentials_without_a_basic_checker_fail_closed() {
        let mut state = web_state(true, None, false);
        state.basic = None;
        for path in ["/api/v1/queries", "/api/v1/info", "/api/v2/health"] {
            let reply = get(&state, path, None).await;
            assert_eq!(reply.status, StatusCode::INTERNAL_SERVER_ERROR, "{path}");
            assert_eq!(reply.json()["error"]["code"], "internal", "{path}");
        }
    }

    #[tokio::test]
    async fn metrics_is_public_unless_metrics_auth() {
        let open = web_state(true, None, false);
        assert_eq!(get(&open, "/metrics", None).await.status, StatusCode::OK);

        let closed = web_state(true, None, true);
        let reply = get(&closed, "/metrics", None).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
        assert_eq!(reply.json()["error"]["code"], "unauthenticated");
        let bearer = format!("Bearer {ALICE_TOKEN}");
        assert_eq!(
            get(&closed, "/metrics", Some(&bearer)).await.status,
            StatusCode::OK
        );
    }

    const VIEW_BOB_ONLY: &str = r#"{"queries": [
        {"user": "alice", "queryOwner": "bob", "allow": ["view"]},
        {"user": "alice|bob|carol", "allow": ["execute"]}
    ]}"#;

    #[tokio::test]
    async fn hidden_query_is_not_found_and_unkillable_query_is_forbidden() {
        let state = web_state(true, Some(VIEW_BOB_ONLY), false);
        let tracker = &state.query_tracker;
        let bobs = tracker.create_query_from("SELECT 1".into(), "pgwire", Some("bob".into()));
        let carols = tracker.create_query_from("SELECT 2".into(), "trino", Some("carol".into()));
        let alice = format!("Bearer {ALICE_TOKEN}");

        let listed = get(&state, "/api/v1/queries", Some(&alice)).await.json();
        let ids: Vec<&str> = listed["queries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|q| q["query_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec![bobs.to_string()]);

        let visible = get(&state, &format!("/api/v1/queries/{bobs}"), Some(&alice)).await;
        assert_eq!(visible.status, StatusCode::OK);

        // Hidden looks exactly like missing.
        let missing = get(
            &state,
            &format!("/api/v1/queries/{}", uuid::Uuid::new_v4()),
            Some(&alice),
        )
        .await;
        let hidden = get(&state, &format!("/api/v1/queries/{carols}"), Some(&alice)).await;
        assert_eq!(missing.status, StatusCode::NOT_FOUND);
        assert_eq!(
            (hidden.status, &hidden.body),
            (missing.status, &missing.body)
        );

        let auth = [("authorization", alice.as_str())];
        let kill_hidden = send(
            &state,
            Method::DELETE,
            &format!("/api/v1/queries/{carols}"),
            &auth,
        )
        .await;
        assert_eq!(kill_hidden.status, StatusCode::NOT_FOUND);
        let kill_visible = send(
            &state,
            Method::DELETE,
            &format!("/api/v1/queries/{bobs}"),
            &auth,
        )
        .await;
        assert_eq!(kill_visible.status, StatusCode::FORBIDDEN);
        assert_eq!(kill_visible.json()["error"]["code"], "forbidden");
        assert_eq!(
            tracker.get_query(&bobs).unwrap().state,
            arneb_scheduler::QueryState::Queued,
            "a refused kill leaves the query alone"
        );

        // The owner may always kill their own query.
        let bob = basic("bob", "hunter2");
        let killed = send(
            &state,
            Method::DELETE,
            &format!("/api/v1/queries/{bobs}"),
            &[("authorization", bob.as_str())],
        )
        .await;
        assert_eq!(killed.status, StatusCode::NO_CONTENT);

        // The v1 400 body is unchanged (empty), not the envelope.
        let bad = get(&state, "/api/v1/queries/not-a-uuid", Some(&alice)).await;
        assert_eq!((bad.status, bad.body.len()), (StatusCode::BAD_REQUEST, 0));
    }

    #[tokio::test]
    async fn none_mode_keeps_the_api_open() {
        let state = web_state(false, Some(VIEW_BOB_ONLY), true);
        let carols =
            state
                .query_tracker
                .create_query_from("SELECT 2".into(), "trino", Some("carol".into()));
        let as_alice = [("x-arneb-user", "alice")];
        let listed = send(&state, Method::GET, "/api/v1/queries", &as_alice).await;
        assert_eq!(listed.status, StatusCode::OK);
        assert_eq!(listed.json()["queries"].as_array().unwrap().len(), 1);
        assert_eq!(
            get(&state, "/api/v1/cluster", None).await.status,
            StatusCode::OK
        );
        assert_eq!(get(&state, "/metrics", None).await.status, StatusCode::OK);
        let killed = send(
            &state,
            Method::DELETE,
            &format!("/api/v1/queries/{carols}"),
            &as_alice,
        )
        .await;
        assert_eq!(killed.status, StatusCode::NO_CONTENT);

        let probe = axum::Router::new()
            .route(
                "/api/v1/whoami",
                axum::routing::get(
                    |axum::Extension(id): axum::Extension<Identity>| async move {
                        format!("{}:{:?}", id.user, id.via)
                    },
                ),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                authenticate,
            ))
            .with_state(state.clone());
        for (user, expect) in [
            (Some("alice"), "alice:Unauthenticated"),
            (Some("  "), "anonymous:Unauthenticated"),
            (None, "anonymous:Unauthenticated"),
        ] {
            let mut req = axum::http::Request::get("/api/v1/whoami");
            if let Some(user) = user {
                req = req.header("x-arneb-user", user);
            }
            let resp = probe
                .clone()
                .oneshot(req.body(Body::empty()).unwrap())
                .await
                .unwrap();
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(&body[..], expect.as_bytes());
        }
    }
}
