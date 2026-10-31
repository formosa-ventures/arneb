//! `/api/v2`: the headless API. Health, sessions, `whoami`, API-token
//! management, and the `/api/v1` read endpoints mirrored with the error
//! envelope (see [`ApiError`]).
//!
//! The password check, token issuing and token revocation are blocking
//! (PBKDF2, fsync), so they run off the async workers: the check through
//! the shared [`BasicAuthenticator`](arneb_protocol::BasicAuthenticator),
//! the state-store writes under `spawn_blocking`. Plaintext tokens appear in
//! response bodies only, never in logs.

use std::sync::Arc;

use arneb_state::{StateError, StateStore};
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{delete, get, post};
use axum::{Extension, Router};
use serde::{Deserialize, Serialize};

use super::api::{
    self, ClusterResponse, InfoResponse, ListQueryParams, QueriesResponse, QueryResponse,
    WorkerResponse,
};
use super::auth::{decode_basic, AuthVia, Identity};
use super::error::{ApiError, ErrorCode};
use super::session::{SESSION_PREFIX, SESSION_TTL};
use super::WebState;

/// Longest accepted token name, in characters.
const MAX_TOKEN_NAME_CHARS: usize = 128;
/// The accepted `expires_in_days` range.
const EXPIRES_IN_DAYS: std::ops::RangeInclusive<i64> = 1..=3650;
const DAY_MS: u64 = 24 * 60 * 60 * 1000;

/// Token issuances running PBKDF2 at once; the rest wait, so a burst cannot
/// saturate the blocking pool.
const MAX_CONCURRENT_ISSUES: usize = 4;
static ISSUE_PERMITS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(MAX_CONCURRENT_ISSUES);

/// The `/api/v2` routes, nested at `/api/v2` by [`super::build_router`].
pub fn routes() -> Router<WebState> {
    Router::new()
        .route("/health", get(health))
        .route("/sessions", post(create_session))
        .route("/sessions/current", delete(end_session))
        .route("/whoami", get(whoami))
        .route("/tokens", post(issue_token).get(list_tokens))
        .route("/tokens/{id}", delete(revoke_token))
        .route("/queries", get(list_queries))
        .route("/queries/{id}", get(get_query).delete(cancel_query))
        .route("/cluster", get(cluster))
        .route("/cluster/workers", get(workers))
        .route("/info", get(info))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
}

async fn not_found() -> ApiError {
    ApiError::new(ErrorCode::NotFound, "no such endpoint")
}

async fn method_not_allowed() -> Response {
    let mut resp = ApiError::new(ErrorCode::InvalidArgument, "method not allowed")
        .with_hint("check the API reference for the methods this path accepts")
        .into_response();
    *resp.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
    resp
}

fn bad_body(e: BytesRejection) -> ApiError {
    ApiError::new(ErrorCode::InvalidArgument, e.body_text())
}

fn bad_query(e: QueryRejection) -> ApiError {
    ApiError::new(ErrorCode::InvalidArgument, e.body_text())
        .with_hint("check the query-string parameters")
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    version: &'static str,
    /// `none` or `password`.
    auth: &'static str,
}

async fn health(State(state): State<WebState>) -> Json<Health> {
    Json(Health {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        auth: if state.credentials.is_some() {
            "password"
        } else {
            "none"
        },
    })
}

fn auth_disabled() -> ApiError {
    ApiError::new(
        ErrorCode::AuthDisabled,
        "this server runs with [auth] type = \"none\"",
    )
    .with_hint("set [auth] type = \"password\" to use sessions and API tokens")
}

/// Parse a JSON request body; an empty body is `T::default()` when
/// `allow_empty`.
fn parse_body<T: serde::de::DeserializeOwned + Default>(
    body: &Bytes,
    allow_empty: bool,
) -> Result<T, ApiError> {
    if body.is_empty() && allow_empty {
        return Ok(T::default());
    }
    serde_json::from_slice(body).map_err(|e| {
        ApiError::new(
            ErrorCode::InvalidArgument,
            format!("invalid request body: {e}"),
        )
        .with_hint("send a JSON object, with Content-Type: application/json")
    })
}

#[derive(Deserialize, Default)]
struct LoginRequest {
    user: String,
    password: String,
}

#[derive(Serialize)]
struct SessionResponse {
    token: String,
    /// Milliseconds since the Unix epoch.
    expires_ms: u64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn create_session(
    State(state): State<WebState>,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<SessionResponse>), ApiError> {
    let Some(basic) = &state.basic else {
        return Err(auth_disabled());
    };
    let login: LoginRequest = parse_body(&body.map_err(bad_body)?, false)?;
    // Sessions are for password logins: an API token as the password is
    // refused exactly like a wrong password.
    if arneb_protocol::is_token_shaped(&login.password) {
        return Err(invalid_login());
    }
    match basic.check(&login.user, &login.password).await {
        arneb_protocol::BasicVerdict::Valid => {}
        arneb_protocol::BasicVerdict::Invalid => return Err(invalid_login()),
        arneb_protocol::BasicVerdict::Overloaded => return Err(overloaded()),
    }
    let token = state.sessions.create(&login.user).map_err(|_| {
        ApiError::new(ErrorCode::Overloaded, "too many active sessions")
            .with_hint("retry later, or authenticate with an API token")
    })?;
    Ok((
        StatusCode::CREATED,
        Json(SessionResponse {
            token,
            expires_ms: now_ms() + SESSION_TTL.as_millis() as u64,
        }),
    ))
}

fn invalid_login() -> ApiError {
    ApiError::new(ErrorCode::Unauthenticated, "invalid user or password")
}

fn overloaded() -> ApiError {
    ApiError::new(
        ErrorCode::Overloaded,
        "too many pending authentication requests",
    )
    .with_hint("retry shortly, or authenticate with a bearer token")
}

async fn end_session(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    if state.credentials.is_none() {
        return Err(auth_disabled());
    }
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split_once(' '))
        .map(|(_, t)| t.trim())
        .filter(|t| who.via == AuthVia::Session && t.starts_with(SESSION_PREFIX));
    let Some(token) = token else {
        return Err(ApiError::new(
            ErrorCode::InvalidArgument,
            "only a session token can end a session",
        )
        .with_hint("send the session token as `Authorization: Bearer <token>`"));
    };
    state.sessions.revoke(token);
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
struct WhoAmI {
    user: String,
    /// `token`, `session`, `basic` or `unauthenticated`.
    via: &'static str,
}

async fn whoami(Extension(who): Extension<Identity>) -> Json<WhoAmI> {
    Json(WhoAmI {
        via: match who.via {
            AuthVia::Token => "token",
            AuthVia::Session => "session",
            AuthVia::Basic => "basic",
            AuthVia::Unauthenticated => "unauthenticated",
        },
        user: who.user,
    })
}

/// The state store, for the token routes: `auth_disabled` under `none`.
fn token_store(state: &WebState) -> Result<Arc<StateStore>, ApiError> {
    if state.credentials.is_none() {
        return Err(auth_disabled());
    }
    state.state.clone().ok_or_else(|| {
        tracing::error!("password auth is configured without a state store");
        ApiError::new(ErrorCode::Internal, "the state store is unavailable")
    })
}

fn state_error(e: StateError) -> ApiError {
    match e {
        StateError::TokenNotFound(_) => ApiError::new(ErrorCode::NotFound, "no such token"),
        StateError::UnknownUser(_) => {
            ApiError::new(ErrorCode::Unauthenticated, "the user no longer exists")
        }
        StateError::TooManyTokens { limit, .. } => ApiError::new(
            ErrorCode::InvalidArgument,
            format!("you already have {limit} live tokens"),
        )
        .with_hint("revoke unused tokens with DELETE /api/v2/tokens/{id}"),
        other => {
            tracing::error!(error = %other, "state store operation failed");
            ApiError::new(ErrorCode::Internal, "the state store operation failed")
        }
    }
}

fn blocking_failed(e: tokio::task::JoinError) -> ApiError {
    tracing::error!(error = %e, "blocking state store task failed");
    ApiError::new(ErrorCode::Internal, "the state store operation failed")
}

#[derive(Deserialize, Default)]
struct IssueTokenRequest {
    name: Option<String>,
    /// Kept untyped so a non-integer gets the range hint too.
    expires_in_days: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct IssuedTokenResponse {
    /// Shown once; the server keeps only a verifier.
    token: String,
    id: String,
    name: Option<String>,
    created_ms: u64,
    expires_ms: Option<u64>,
    /// Present (`false`) only when the state is in memory.
    #[serde(skip_serializing_if = "Option::is_none")]
    persistent: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
}

async fn issue_token(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<IssuedTokenResponse>), ApiError> {
    let store = token_store(&state)?;
    let req: IssueTokenRequest = parse_body(&body.map_err(bad_body)?, true)?;
    let name = req
        .name
        .map(|n| n.trim().to_owned())
        .filter(|n| !n.is_empty());
    if name
        .as_ref()
        .is_some_and(|n| n.chars().count() > MAX_TOKEN_NAME_CHARS)
    {
        return Err(
            ApiError::new(ErrorCode::InvalidArgument, "name is too long")
                .with_hint(format!("use at most {MAX_TOKEN_NAME_CHARS} characters")),
        );
    }
    let expires_at_ms = match req.expires_in_days {
        None => None,
        Some(days) => match days.as_i64().filter(|d| EXPIRES_IN_DAYS.contains(d)) {
            Some(days) => Some(now_ms() + days as u64 * DAY_MS),
            None => {
                return Err(ApiError::new(
                    ErrorCode::InvalidArgument,
                    format!("expires_in_days {days} is not valid"),
                )
                .with_hint(format!(
                    "expires_in_days must be an integer between {} and {}",
                    EXPIRES_IN_DAYS.start(),
                    EXPIRES_IN_DAYS.end()
                )));
            }
        },
    };
    // A token cannot mint a child that outlives it.
    let mut hints = Vec::new();
    let mut expires_at_ms = expires_at_ms;
    if let Some(parent_expiry) = authenticating_token_expiry(&store, &who, &headers)? {
        if expires_at_ms.is_none_or(|e| e > parent_expiry) {
            expires_at_ms = Some(parent_expiry);
            hints.push("expires_ms was clamped to the expiry of the token that made this request");
        }
    }
    let persistent = store.is_persistent();
    if !persistent {
        hints.push("the server keeps state in memory only: this token is lost when it restarts");
    }
    let _permit = ISSUE_PERMITS
        .acquire()
        .await
        .map_err(|_| ApiError::new(ErrorCode::Internal, "token issuance is unavailable"))?;
    let issued =
        tokio::task::spawn_blocking(move || store.issue_token(&who.user, name, expires_at_ms))
            .await
            .map_err(blocking_failed)?
            .map_err(state_error)?;
    Ok((
        StatusCode::CREATED,
        Json(IssuedTokenResponse {
            token: issued.token,
            id: issued.info.id,
            name: issued.info.name,
            created_ms: issued.info.created_ms,
            expires_ms: issued.info.expires_ms,
            persistent: (!persistent).then_some(false),
            hint: (!hints.is_empty()).then(|| hints.join("; ")),
        }),
    ))
}

/// The expiry of the API token that authenticated this request. `Ok(None)`
/// means the request was not made with an API token, or the token never
/// expires. The middleware already verified the credential, so only its id
/// is read; if that token is gone or expired by now (revoked between the
/// middleware and this handler), the request fails as unauthenticated
/// rather than minting an unclamped child.
fn authenticating_token_expiry(
    store: &StateStore,
    who: &Identity,
    headers: &HeaderMap,
) -> Result<Option<u64>, ApiError> {
    let gone = || ApiError::new(ErrorCode::Unauthenticated, "authentication required");
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return Ok(None);
    };
    let Some((_, credential)) = value.split_once(' ') else {
        return Ok(None);
    };
    let token = match who.via {
        AuthVia::Token => credential.trim().to_owned(),
        AuthVia::Basic => match decode_basic(credential.trim()) {
            Some((_, password)) => password,
            None => return Ok(None),
        },
        _ => return Ok(None),
    };
    if !arneb_protocol::is_token_shaped(&token) {
        return Ok(None);
    }
    let id = &token["arneb_".len()..][..16];
    let now = now_ms();
    let record = store
        .list_tokens(&who.user)
        .into_iter()
        .find(|t| t.id == id)
        .filter(|t| t.expires_ms.is_none_or(|e| now < e))
        .ok_or_else(gone)?;
    Ok(record.expires_ms)
}

#[derive(Serialize)]
struct TokenView {
    id: String,
    name: Option<String>,
    created_ms: u64,
    expires_ms: Option<u64>,
}

#[derive(Serialize)]
struct TokensResponse {
    tokens: Vec<TokenView>,
}

async fn list_tokens(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
) -> Result<Json<TokensResponse>, ApiError> {
    let store = token_store(&state)?;
    let tokens = store
        .list_tokens(&who.user)
        .into_iter()
        .map(|t| TokenView {
            id: t.id,
            name: t.name,
            created_ms: t.created_ms,
            expires_ms: t.expires_ms,
        })
        .collect();
    Ok(Json(TokensResponse { tokens }))
}

async fn revoke_token(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let store = token_store(&state)?;
    tokio::task::spawn_blocking(move || store.revoke_token(&who.user, &id))
        .await
        .map_err(blocking_failed)?
        .map_err(state_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_queries(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<ListQueryParams>, QueryRejection>,
) -> Result<Json<QueriesResponse>, ApiError> {
    let Query(params) = params.map_err(bad_query)?;
    Ok(Json(api::queries_response(&state, &who, params)))
}

/// The v1 lookup's status as an envelope: a malformed id is
/// `invalid_argument`; a missing or hidden query is `not_found`.
fn lookup_error(status: StatusCode) -> ApiError {
    if status == StatusCode::BAD_REQUEST {
        ApiError::new(ErrorCode::InvalidArgument, "the query id is not a UUID")
    } else {
        ApiError::new(ErrorCode::NotFound, "no such query")
    }
}

async fn get_query(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<Json<QueryResponse>, ApiError> {
    let info = api::visible_query(&state, &who, &id).map_err(lookup_error)?;
    Ok(Json(QueryResponse::from(info)))
}

async fn cancel_query(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let info = api::visible_query(&state, &who, &id).map_err(lookup_error)?;
    if !api::may_kill(&state, &who, info.user.as_deref()) {
        return Err(ApiError::new(
            ErrorCode::Forbidden,
            "the access rules do not allow you to kill this query",
        ));
    }
    state
        .query_tracker
        .cancel_query(&info.query_id)
        .map_err(|_| lookup_error(StatusCode::NOT_FOUND))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn cluster(State(state): State<WebState>) -> Json<ClusterResponse> {
    Json(api::cluster_response(&state))
}

async fn workers(State(state): State<WebState>) -> Json<Vec<WorkerResponse>> {
    Json(api::workers_response(&state))
}

async fn info(State(state): State<WebState>) -> Json<InfoResponse> {
    Json(api::info_response(&state))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arneb_protocol::{CredentialSource, ScramVerifier};
    use axum::body::Body;
    use axum::http::{header, Method, StatusCode};
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use serde_json::{json, Value};
    use tower::ServiceExt as _;

    use crate::web::{build_router, SessionStore, WebState};

    fn web_state(password: bool, persistent_path: Option<&std::path::Path>) -> WebState {
        let store = Arc::new(arneb_state::StateStore::open(persistent_path).unwrap());
        let users: Vec<(String, ScramVerifier)> = [("alice", "secret"), ("bob", "hunter2")]
            .into_iter()
            .map(|(user, pw)| {
                let v = ScramVerifier::from_password(pw, b"0123456789abcdef", 4096);
                (user.to_string(), v)
            })
            .collect();
        store.seed_users(&users, false).unwrap();
        let credentials: Option<Arc<dyn CredentialSource>> =
            password.then(|| store.clone() as Arc<dyn CredentialSource>);
        WebState {
            query_tracker: Arc::new(arneb_scheduler::QueryTracker::new()),
            node_registry: arneb_scheduler::NodeRegistry::new(30),
            start_time: std::time::Instant::now(),
            role: "standalone".into(),
            admission: None,
            max_queued_queries: 8,
            memory_pool: Arc::new(arneb_common::memory_pool::GreedyMemoryPool::new(1 << 20)),
            memory_limit_bytes: None,
            basic: credentials
                .clone()
                .map(|c| arneb_protocol::BasicAuthenticator::new(c).unwrap()),
            credentials,
            state: password.then_some(store),
            access_rules: None,
            metrics_auth: false,
            sessions: SessionStore::new(),
        }
    }

    fn basic(user: &str, password: &str) -> String {
        format!("Basic {}", STANDARD.encode(format!("{user}:{password}")))
    }

    async fn call(
        state: &WebState,
        method: Method,
        path: &str,
        auth: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut req = axum::http::Request::builder().method(method).uri(path);
        if let Some(a) = auth {
            req = req.header(header::AUTHORIZATION, a);
        }
        let body = match body {
            Some(b) => {
                req = req.header(header::CONTENT_TYPE, "application/json");
                Body::from(b.to_string())
            }
            None => Body::empty(),
        };
        let resp = build_router(state.clone())
            .oneshot(req.body(body).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn drop_elapsed(v: &mut Value) {
        match v {
            Value::Object(m) => {
                m.remove("elapsed_ms");
                m.values_mut().for_each(drop_elapsed);
            }
            Value::Array(a) => a.iter_mut().for_each(drop_elapsed),
            _ => {}
        }
    }

    async fn login(state: &WebState, user: &str, password: &str) -> String {
        let (status, body) = call(
            state,
            Method::POST,
            "/api/v2/sessions",
            None,
            Some(json!({"user": user, "password": password})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert!(body["expires_ms"].is_u64());
        format!("Bearer {}", body["token"].as_str().unwrap())
    }

    #[test]
    fn parent_token_lookup_fails_closed_when_the_token_is_gone() {
        let state = web_state(true, None);
        let store = state.state.clone().unwrap();
        let issued = store.issue_token("alice", None, None).unwrap();
        let who = super::Identity {
            user: "alice".into(),
            via: super::AuthVia::Token,
        };
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {}", issued.token).parse().unwrap(),
        );
        assert_eq!(
            super::authenticating_token_expiry(&store, &who, &headers).unwrap(),
            None
        );
        store.revoke_token("alice", &issued.info.id).unwrap();
        let err = super::authenticating_token_expiry(&store, &who, &headers).unwrap_err();
        assert!(
            format!("{err:?}")
                .to_lowercase()
                .contains("unauthenticated"),
            "{err:?}"
        );
        // Same through Basic with the token as the password.
        let who = super::Identity {
            user: "alice".into(),
            via: super::AuthVia::Basic,
        };
        headers.insert(
            header::AUTHORIZATION,
            basic("alice", &issued.token).parse().unwrap(),
        );
        assert!(super::authenticating_token_expiry(&store, &who, &headers).is_err());
    }

    #[tokio::test]
    async fn health_reports_auth_mode() {
        for (password, mode) in [(true, "password"), (false, "none")] {
            let state = web_state(password, None);
            let (status, body) = call(&state, Method::GET, "/api/v2/health", None, None).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                body,
                json!({"status": "ok", "version": env!("CARGO_PKG_VERSION"), "auth": mode})
            );
        }
    }

    #[tokio::test]
    async fn login_then_issue_list_and_revoke_a_token() {
        let state = web_state(true, None);

        // Bad logins are the one `unauthenticated` envelope.
        for (u, p) in [("alice", "wrong"), ("nobody", "secret")] {
            let (status, body) = call(
                &state,
                Method::POST,
                "/api/v2/sessions",
                None,
                Some(json!({"user": u, "password": p})),
            )
            .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert_eq!(body["error"]["code"], "unauthenticated");
        }
        let (status, body) = call(&state, Method::POST, "/api/v2/sessions", None, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");

        let session = login(&state, "alice", "secret").await;
        let (_, me) = call(&state, Method::GET, "/api/v2/whoami", Some(&session), None).await;
        assert_eq!(me, json!({"user": "alice", "via": "session"}));

        let (status, issued) = call(
            &state,
            Method::POST,
            "/api/v2/tokens",
            Some(&session),
            Some(json!({"name": "ci", "expires_in_days": 30})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert!(issued["token"].as_str().unwrap().starts_with("arneb_"));
        assert_eq!(issued["name"], "ci");
        assert!(issued["expires_ms"].as_u64().unwrap() > issued["created_ms"].as_u64().unwrap());
        // In-memory state: the token dies with the process.
        assert_eq!(issued["persistent"], false);
        assert!(issued["hint"].as_str().unwrap().contains("restart"));
        let id = issued["id"].as_str().unwrap().to_owned();

        let (status, listed) =
            call(&state, Method::GET, "/api/v2/tokens", Some(&session), None).await;
        assert_eq!(status, StatusCode::OK);
        let tokens = listed["tokens"].as_array().unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0]["id"], id.as_str());
        assert!(
            tokens[0].get("token").is_none(),
            "the secret is never listed"
        );

        let path = format!("/api/v2/tokens/{id}");
        let (status, _) = call(&state, Method::DELETE, &path, Some(&session), None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, body) = call(&state, Method::DELETE, &path, Some(&session), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "not_found");
        let (_, listed) = call(&state, Method::GET, "/api/v2/tokens", Some(&session), None).await;
        assert!(listed["tokens"].as_array().unwrap().is_empty());

        // Ending the session ends it.
        let (status, _) = call(
            &state,
            Method::DELETE,
            "/api/v2/sessions/current",
            Some(&session),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _) = call(&state, Method::GET, "/api/v2/whoami", Some(&session), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn persistent_state_does_not_warn() {
        let dir = std::env::temp_dir().join(format!("arneb-v2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let state = web_state(true, Some(&dir.join("state.json")));
        let (status, issued) = call(
            &state,
            Method::POST,
            "/api/v2/tokens",
            Some(&basic("alice", "secret")),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert!(issued.get("persistent").is_none() && issued.get("hint").is_none());
        assert!(issued["expires_ms"].is_null(), "no expiry unless asked");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn issued_token_authenticates_the_next_request() {
        let state = web_state(true, None);
        let (_, issued) = call(
            &state,
            Method::POST,
            "/api/v2/tokens",
            Some(&basic("alice", "secret")),
            Some(json!({})),
        )
        .await;
        let bearer = format!("Bearer {}", issued["token"].as_str().unwrap());
        let (status, me) = call(&state, Method::GET, "/api/v2/whoami", Some(&bearer), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(me, json!({"user": "alice", "via": "token"}));
        // A token may manage its owner's tokens, but is not a session.
        let (status, _) = call(&state, Method::GET, "/api/v2/tokens", Some(&bearer), None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = call(
            &state,
            Method::DELETE,
            "/api/v2/sessions/current",
            Some(&bearer),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");

        let (_, me) = call(
            &state,
            Method::GET,
            "/api/v2/whoami",
            Some(&basic("bob", "hunter2")),
            None,
        )
        .await;
        assert_eq!(me, json!({"user": "bob", "via": "basic"}));
    }

    #[tokio::test]
    async fn tokens_are_auth_disabled_under_none() {
        let state = web_state(false, None);
        for (method, path) in [
            (Method::POST, "/api/v2/tokens"),
            (Method::GET, "/api/v2/tokens"),
            (Method::DELETE, "/api/v2/tokens/0123456789abcdef"),
            (Method::POST, "/api/v2/sessions"),
        ] {
            let (status, body) = call(&state, method, path, None, Some(json!({}))).await;
            assert_eq!(status, StatusCode::CONFLICT, "{path}");
            assert_eq!(body["error"]["code"], "auth_disabled", "{path}");
        }
        let (_, me) = call(&state, Method::GET, "/api/v2/whoami", None, None).await;
        assert_eq!(me, json!({"user": "anonymous", "via": "unauthenticated"}));
    }

    #[tokio::test]
    async fn expires_in_days_out_of_range_is_invalid_argument_with_hint() {
        let state = web_state(true, None);
        let auth = basic("alice", "secret");
        for days in [json!(0), json!(-1), json!(3651), json!("ten"), json!(1.5)] {
            let (status, body) = call(
                &state,
                Method::POST,
                "/api/v2/tokens",
                Some(&auth),
                Some(json!({"expires_in_days": days})),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{days}");
            assert_eq!(body["error"]["code"], "invalid_argument", "{days}");
            assert!(
                body["error"]["hint"]
                    .as_str()
                    .unwrap()
                    .contains("1 and 3650"),
                "{days}: {body}"
            );
        }
        for days in [1, 3650] {
            let (status, _) = call(
                &state,
                Method::POST,
                "/api/v2/tokens",
                Some(&auth),
                Some(json!({"expires_in_days": days})),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "{days}");
        }
    }

    #[tokio::test]
    async fn cannot_list_or_revoke_another_users_tokens() {
        let state = web_state(true, None);
        let alice = basic("alice", "secret");
        let bob = basic("bob", "hunter2");
        let (_, issued) = call(&state, Method::POST, "/api/v2/tokens", Some(&alice), None).await;
        let id = issued["id"].as_str().unwrap();

        let (_, listed) = call(&state, Method::GET, "/api/v2/tokens", Some(&bob), None).await;
        assert!(listed["tokens"].as_array().unwrap().is_empty());
        let path = format!("/api/v2/tokens/{id}");
        let (status, body) = call(&state, Method::DELETE, &path, Some(&bob), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "not_found");
        let (_, listed) = call(&state, Method::GET, "/api/v2/tokens", Some(&alice), None).await;
        assert_eq!(listed["tokens"].as_array().unwrap().len(), 1, "still alive");
    }

    #[tokio::test]
    async fn the_global_session_cap_is_overloaded() {
        let mut state = web_state(true, None);
        state.sessions = SessionStore::with_max_sessions(1);
        login(&state, "alice", "secret").await;
        let (status, body) = call(
            &state,
            Method::POST,
            "/api/v2/sessions",
            None,
            Some(json!({"user": "bob", "password": "hunter2"})),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["code"], "overloaded");
    }

    #[tokio::test]
    async fn v2_queries_matches_v1_shape() {
        let state = web_state(true, None);
        let auth = basic("alice", "secret");
        let id = state.query_tracker.create_query_from(
            "SELECT 1".into(),
            "pgwire",
            Some("alice".into()),
        );
        let other =
            state
                .query_tracker
                .create_query_from("SELECT 2".into(), "pgwire", Some("bob".into()));
        for (v1, v2) in [
            ("/api/v1/queries", "/api/v2/queries"),
            ("/api/v1/cluster", "/api/v2/cluster"),
            ("/api/v1/cluster/workers", "/api/v2/cluster/workers"),
            (
                &format!("/api/v1/queries/{id}"),
                &format!("/api/v2/queries/{id}"),
            ),
        ] {
            let (s1, mut b1) = call(&state, Method::GET, v1, Some(&auth), None).await;
            let (s2, mut b2) = call(&state, Method::GET, v2, Some(&auth), None).await;
            // Elapsed time moves between the two calls.
            drop_elapsed(&mut b1);
            drop_elapsed(&mut b2);
            assert_eq!((s1, &b1), (s2, &b2), "{v2}");
            assert_eq!(s2, StatusCode::OK);
        }
        let (_, info) = call(&state, Method::GET, "/api/v2/info", Some(&auth), None).await;
        assert_eq!(info["version"], env!("CARGO_PKG_VERSION"));

        // Errors are the envelope.
        let (status, body) = call(
            &state,
            Method::GET,
            "/api/v2/queries/not-a-uuid",
            Some(&auth),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
        let missing = format!("/api/v2/queries/{}", uuid::Uuid::nil());
        let (status, body) = call(&state, Method::GET, &missing, Some(&auth), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "not_found");
        let (status, _) = call(
            &state,
            Method::DELETE,
            &format!("/api/v2/queries/{other}"),
            Some(&auth),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "no rules: allowed");
    }

    async fn issue(state: &WebState, auth: &str, body: Value) -> (StatusCode, Value) {
        call(
            state,
            Method::POST,
            "/api/v2/tokens",
            Some(auth),
            Some(body),
        )
        .await
    }

    #[tokio::test]
    async fn child_of_an_expiring_token_is_clamped() {
        let state = web_state(true, None);
        let (_, parent) = issue(
            &state,
            &basic("alice", "secret"),
            json!({"expires_in_days": 1}),
        )
        .await;
        let parent_expiry = parent["expires_ms"].as_u64().unwrap();
        let ptoken = parent["token"].as_str().unwrap();
        for auth in [format!("Bearer {ptoken}"), basic("alice", ptoken)] {
            for body in [json!({"expires_in_days": 30}), json!({})] {
                let (status, child) = issue(&state, &auth, body).await;
                assert_eq!(status, StatusCode::CREATED);
                assert_eq!(child["expires_ms"].as_u64(), Some(parent_expiry));
                assert!(child["hint"].as_str().unwrap().contains("clamped"));
            }
            // A shorter request is kept.
            let (_, child) = issue(&state, &auth, json!({"expires_in_days": 1})).await;
            assert!(child["expires_ms"].as_u64().unwrap() <= parent_expiry);
        }
    }

    #[tokio::test]
    async fn child_of_a_non_expiring_token_is_unrestricted() {
        let state = web_state(true, None);
        let (_, parent) = issue(&state, &basic("alice", "secret"), json!({})).await;
        let bearer = format!("Bearer {}", parent["token"].as_str().unwrap());
        let (_, child) = issue(&state, &bearer, json!({"expires_in_days": 30})).await;
        let ttl = child["expires_ms"].as_u64().unwrap() - child["created_ms"].as_u64().unwrap();
        assert!(ttl >= 29 * 24 * 3600 * 1000);
        assert!(!child["hint"].as_str().unwrap().contains("clamped"));
        let (_, child) = issue(&state, &bearer, json!({})).await;
        assert!(child["expires_ms"].is_null());
    }

    #[tokio::test]
    async fn login_with_a_token_as_password_looks_like_a_wrong_password() {
        let state = web_state(true, None);
        let (_, parent) = issue(&state, &basic("alice", "secret"), json!({})).await;
        let attempt = |password: String| {
            let state = state.clone();
            async move {
                call(
                    &state,
                    Method::POST,
                    "/api/v2/sessions",
                    None,
                    Some(json!({"user": "alice", "password": password})),
                )
                .await
            }
        };
        let wrong = attempt("wrong".into()).await;
        let with_token = attempt(parent["token"].as_str().unwrap().to_owned()).await;
        assert_eq!(wrong.0, StatusCode::UNAUTHORIZED);
        assert_eq!(wrong, with_token);
    }

    #[tokio::test]
    async fn v2_errors_are_always_the_envelope() {
        let state = web_state(true, None);
        let auth = basic("alice", "secret");
        let (status, body) = call(&state, Method::GET, "/api/v2/nope", None, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "auth comes first");
        assert_eq!(body["error"]["code"], "unauthenticated");
        let (status, body) = call(&state, Method::GET, "/api/v2/nope", Some(&auth), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "not_found");
        let (status, body) = call(&state, Method::PUT, "/api/v2/tokens", Some(&auth), None).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(body["error"]["code"], "invalid_argument");
        let (status, body) = call(&state, Method::PUT, "/api/v2/tokens", None, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        let (status, body) = call(
            &state,
            Method::GET,
            "/api/v2/queries?state=a&state=b",
            Some(&auth),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn the_hundred_and_first_token_is_refused() {
        let state = web_state(true, None);
        let auth = basic("alice", "secret");
        for _ in 0..arneb_state::MAX_TOKENS_PER_USER {
            assert_eq!(issue(&state, &auth, json!({})).await.0, StatusCode::CREATED);
        }
        let (status, body) = issue(&state, &auth, json!({})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert!(body["error"]["hint"].as_str().unwrap().contains("revoke"));
        // Another user is unaffected.
        let bob = basic("bob", "hunter2");
        assert_eq!(issue(&state, &bob, json!({})).await.0, StatusCode::CREATED);
    }
}
