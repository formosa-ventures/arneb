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

use arneb_scheduler::KillOutcome;
use arneb_state::{now_ms, StateError, StateStore};
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
use super::audit::{audited, AuditSubject};
use super::auth::{decode_basic, AuthVia, Identity};
use super::error::{ApiError, ErrorCode};
use super::manage::{run_manage, Applied, DryRun, ManageAction, ManageOutcome, Preview};
use super::privilege::{Authorizer, Layer};
use super::session::{SESSION_PREFIX, SESSION_TTL};
use super::WebState;

/// Longest accepted token name, in characters.
const MAX_TOKEN_NAME_CHARS: usize = 128;
/// The accepted `expires_in_days` range.
const EXPIRES_IN_DAYS: std::ops::RangeInclusive<i64> = 1..=3650;
const DAY_MS: u64 = 24 * 60 * 60 * 1000;
/// [`SESSION_TTL`] in milliseconds (8 hours: no truncation).
const SESSION_TTL_MS: u64 = SESSION_TTL.as_secs() * 1000;

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
        .merge(super::agent::routes())
        .merge(super::audit::routes())
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

pub(super) fn bad_body(e: BytesRejection) -> ApiError {
    ApiError::new(ErrorCode::InvalidArgument, e.body_text())
}

pub(super) fn bad_query(e: QueryRejection) -> ApiError {
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
            expires_ms: now_ms().saturating_add(SESSION_TTL_MS),
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
    privileges: Privileges,
}

/// What the caller may do beyond querying (manage-api D11).
#[derive(Serialize)]
pub(super) struct Privileges {
    observe: bool,
    manage: bool,
}

impl Privileges {
    /// The layers `who` holds.
    pub(super) fn of(authorizer: &Authorizer, who: &Identity) -> Self {
        Self {
            observe: authorizer.allows(who, Layer::Observe),
            manage: authorizer.allows(who, Layer::Manage),
        }
    }
}

async fn whoami(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
) -> Json<WhoAmI> {
    Json(WhoAmI {
        privileges: Privileges::of(&state.authorizer, &who),
        via: who.via.as_str(),
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

/// A token issuing request: the `POST /api/v2/tokens` body, or the
/// `create_token` tool's arguments.
#[derive(Deserialize, Default)]
pub(super) struct IssueTokenRequest {
    pub(super) name: Option<String>,
    /// Kept untyped so a non-integer gets the range hint too.
    pub(super) expires_in_days: Option<serde_json::Value>,
}

/// A freshly issued token; the plaintext is shown once.
#[derive(Serialize)]
pub(super) struct IssuedTokenResponse {
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
    token_store(&state)?;
    let req: IssueTokenRequest = parse_body(&body.map_err(bad_body)?, true)?;
    let issued = issue_token_for(&state, &who, &headers, req).await?;
    Ok((StatusCode::CREATED, Json(issued)))
}

/// Issues a token for `who`. `headers` are the request's, to find the
/// credential that authenticated it (a token cannot mint a longer-lived
/// child). Shared with the MCP `create_token` tool.
pub(super) async fn issue_token_for(
    state: &WebState,
    who: &Identity,
    headers: &HeaderMap,
    req: IssueTokenRequest,
) -> Result<IssuedTokenResponse, ApiError> {
    let store = token_store(state)?;
    let user = who.user.clone();
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
            Some(days) => Some(now_ms().saturating_add(days.unsigned_abs() * DAY_MS)),
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
    if let Some(parent_expiry) = authenticating_token_expiry(&store, who, headers)? {
        if expires_at_ms.is_none_or(|e| e > parent_expiry) {
            expires_at_ms = Some(parent_expiry);
            hints.push("expires_ms was clamped to the expiry of the token that made this request");
        }
    }
    let persistent = store.is_persistent();
    if !persistent {
        hints.push("the server keeps state in memory only: this token is lost when it restarts");
    }
    let permit = ISSUE_PERMITS
        .acquire()
        .await
        .map_err(|_| ApiError::new(ErrorCode::Internal, "token issuance is unavailable"))?;
    // The permit moves into the blocking task, so it is held for as long as
    // PBKDF2 runs even if the client disconnects and this future is dropped.
    let (state, who) = (state.clone(), who.clone());
    let issued = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let parameters = serde_json::json!({"name": name, "expires_ms": expires_at_ms});
        audited(
            &state,
            &who,
            "token.issue",
            || {
                store
                    .issue_token(&user, name, expires_at_ms)
                    .map_err(state_error)
            },
            |out| AuditSubject {
                // The id only: never the token itself.
                target: out.map(|t| t.info.id.clone()).unwrap_or_default(),
                parameters,
            },
        )
    })
    .await
    .map_err(blocking_failed)??;
    Ok(IssuedTokenResponse {
        token: issued.token,
        id: issued.info.id,
        name: issued.info.name,
        created_ms: issued.info.created_ms,
        expires_ms: issued.info.expires_ms,
        persistent: (!persistent).then_some(false),
        hint: (!hints.is_empty()).then(|| hints.join("; ")),
    })
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
pub(super) struct TokenView {
    id: String,
    name: Option<String>,
    created_ms: u64,
    expires_ms: Option<u64>,
}

/// The caller's live tokens.
#[derive(Serialize)]
pub(super) struct TokensResponse {
    tokens: Vec<TokenView>,
}

async fn list_tokens(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
) -> Result<Json<TokensResponse>, ApiError> {
    Ok(Json(list_tokens_for(&state, &who)?))
}

/// `who`'s live tokens. Shared with the MCP `list_tokens` tool.
pub(super) fn list_tokens_for(
    state: &WebState,
    who: &Identity,
) -> Result<TokensResponse, ApiError> {
    let store = token_store(state)?;
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
    Ok(TokensResponse { tokens })
}

async fn revoke_token(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    revoke_token_for(&state, &who, id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Revokes `who`'s token `id`; another user's id is `not_found`. Shared
/// with the MCP `revoke_token` tool.
pub(super) async fn revoke_token_for(
    state: &WebState,
    who: &Identity,
    id: String,
) -> Result<(), ApiError> {
    let store = token_store(state)?;
    // A malformed id is refused before it is audited: the id is the audit
    // target, and a pasted token must never land in the audit log.
    if !arneb_protocol::is_token_id(&id) {
        return Err(
            ApiError::new(ErrorCode::InvalidArgument, "the token id is not valid").with_hint(
                "a token id is 16 lowercase hex characters, as GET /api/v2/tokens lists",
            ),
        );
    }
    let (state, who) = (state.clone(), who.clone());
    tokio::task::spawn_blocking(move || {
        audited(
            &state,
            &who,
            "token.revoke",
            || store.revoke_token(&who.user, &id).map_err(state_error),
            |_| AuditSubject {
                target: id.clone(),
                parameters: serde_json::json!({}),
            },
        )
    })
    .await
    .map_err(blocking_failed)?
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
pub(super) fn lookup_error(status: StatusCode) -> ApiError {
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
    params: Result<Query<DryRun>, QueryRejection>,
) -> Result<Json<ManageOutcome<KillResult>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    Ok(Json(cancel_query_for(&state, &who, &id, dry_run).await?))
}

/// The `result` of a `query.cancel`.
#[derive(Serialize)]
pub(super) struct KillResult {
    query_id: String,
    owner: Option<String>,
    /// Lowercase, as the query list reports states.
    state_before: String,
    state: String,
}

/// Cancels query `id` for `who` under the kill rule (manage-api D2, D7,
/// ruling R6): a query `who` may neither view nor kill is `not_found` (a
/// manager, or a `queries` rule allowing `kill`, finds it by id even
/// without `view`); then [`Authorizer::check_kill`]. Audited unless `who` owns the query. A query
/// that already ended is a no-op (`changed: false`). Shared with
/// `/api/v1` and the MCP `cancel_query` tool.
pub(super) async fn cancel_query_for(
    state: &WebState,
    who: &Identity,
    id: &str,
    dry_run: bool,
) -> Result<ManageOutcome<KillResult>, ApiError> {
    let info = api::killable_query(state, who, id).map_err(lookup_error)?;
    let kill = state.authorizer.check_kill(who, info.user.as_deref())?;
    let action = CancelQuery {
        id: info.query_id,
        owner: info.user,
        by: who.user.clone(),
        by_owner: kill.is_owner(),
    };
    run_manage(state, who, kill.grant(), dry_run, action).await
}

struct CancelQuery {
    id: arneb_common::identifiers::QueryId,
    owner: Option<String>,
    by: String,
    by_owner: bool,
}

impl CancelQuery {
    fn owned_by(&self) -> String {
        match &self.owner {
            Some(owner) => format!("owned by {owner}"),
            None => "with no owner".to_owned(),
        }
    }

    fn ended(&self, state: arneb_scheduler::QueryState) -> Preview {
        Preview {
            summary: format!(
                "query {} {} already {}; nothing to cancel",
                self.id,
                self.owned_by(),
                api::state_name(state)
            ),
            changed: false,
            warning: None,
        }
    }

    fn gone() -> ApiError {
        lookup_error(StatusCode::NOT_FOUND)
    }
}

impl ManageAction for CancelQuery {
    type Output = KillResult;

    fn action(&self) -> &'static str {
        "query.cancel"
    }

    fn target(&self) -> String {
        self.id.to_string()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"owner": self.owner})
    }

    fn audited(&self) -> bool {
        !self.by_owner
    }

    fn unchanged_effect(&self) -> &'static str {
        "already_ended"
    }

    fn preview(&self, state: &WebState) -> Result<Preview, ApiError> {
        let now = state
            .query_tracker
            .get_query(&self.id)
            .ok_or_else(Self::gone)?
            .state;
        if now.is_terminal() {
            return Ok(self.ended(now));
        }
        Ok(Preview {
            summary: format!(
                "cancels 1 query ({}) {}; it is {}",
                self.id,
                self.owned_by(),
                api::state_name(now)
            ),
            changed: true,
            warning: None,
        })
    }

    fn apply(self, state: &WebState) -> Result<Applied<KillResult>, ApiError> {
        let tracker = &state.query_tracker;
        let outcome = tracker
            .kill_query(&self.id, &self.by)
            .map_err(|_| Self::gone())?;
        let (before, preview) = match outcome {
            KillOutcome::Killed { was } => (
                was,
                Preview {
                    summary: format!(
                        "cancelled query {} {}; it was {}",
                        self.id,
                        self.owned_by(),
                        api::state_name(was)
                    ),
                    changed: true,
                    warning: None,
                },
            ),
            KillOutcome::AlreadyEnded(now) => (now, self.ended(now)),
        };
        let after = tracker
            .get_query(&self.id)
            .map_or(arneb_scheduler::QueryState::Cancelled, |q| q.state);
        Ok(Applied {
            preview,
            result: KillResult {
                query_id: self.id.to_string(),
                owner: self.owner,
                state_before: api::state_name(before).to_owned(),
                state: api::state_name(after).to_owned(),
            },
        })
    }
}

async fn cluster(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
) -> Result<Json<ClusterResponse>, ApiError> {
    state.authorizer.check(&who, Layer::Observe)?;
    Ok(Json(api::cluster_response(&state)))
}

async fn workers(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
) -> Result<Json<Vec<WorkerResponse>>, ApiError> {
    state.authorizer.check(&who, Layer::Observe)?;
    Ok(Json(api::workers_response(&state)))
}

async fn info(State(state): State<WebState>) -> Json<InfoResponse> {
    Json(api::info_response(&state))
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Arc;

    use arneb_protocol::{CredentialSource, ScramVerifier};
    use axum::body::Body;
    use axum::http::{header, Method, StatusCode};
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use serde_json::{json, Value};
    use tower::ServiceExt as _;

    use crate::web::{build_router, SessionStore, WebState};

    pub(in crate::web) fn web_state(
        password: bool,
        persistent_path: Option<&std::path::Path>,
    ) -> WebState {
        let store = Arc::new(arneb_state::StateStore::open(persistent_path).unwrap());
        let users: Vec<(String, ScramVerifier)> = [
            ("alice", "secret"),
            ("bob", "hunter2"),
            ("root", "rootpw"),
            ("ops", "opspw"),
        ]
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
            audit: crate::web::test_audit_log(),
            access_rules: None,
            authorizer: crate::web::privilege::Authorizer::new(None, Vec::new(), password),
            metrics_auth: false,
            sessions: SessionStore::new(),
            agent: None,
            mcp: None,
        }
    }

    /// `state` with `rules` and `admin_users` behind its authorizer.
    pub(in crate::web) fn with_authz(
        mut state: WebState,
        rules: Option<&str>,
        admins: &[&str],
    ) -> WebState {
        let rules = rules.map(|r| Arc::new(arneb_catalog::AccessRules::from_json(r).unwrap()));
        state.access_rules = rules.clone();
        state.authorizer = crate::web::privilege::Authorizer::new(
            rules,
            admins.iter().map(|a| a.to_string()).collect(),
            state.credentials.is_some(),
        );
        state
    }

    pub(in crate::web) fn basic(user: &str, password: &str) -> String {
        format!("Basic {}", STANDARD.encode(format!("{user}:{password}")))
    }

    pub(in crate::web) async fn call(
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
        assert_eq!(
            me,
            json!({"user": "alice", "via": "session", "privileges": {"observe": true, "manage": false}})
        );

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
        assert_eq!(
            me,
            json!({"user": "alice", "via": "token", "privileges": {"observe": true, "manage": false}})
        );
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
        assert_eq!(
            me,
            json!({"user": "bob", "via": "basic", "privileges": {"observe": true, "manage": false}})
        );
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
        assert_eq!(
            me,
            json!({"user": "anonymous", "via": "unauthenticated", "privileges": {"observe": true, "manage": false}})
        );
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
        let (status, body) = call(
            &state,
            Method::DELETE,
            &format!("/api/v2/queries/{other}"),
            Some(&auth),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "no rules: allowed; {body}");
        assert_eq!(body["action"], "query.cancel");
        assert_eq!(body["result"]["state"], "cancelled");
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

    #[tokio::test]
    async fn whoami_reports_privileges() {
        let state = with_authz(web_state(true, None), None, &["root"]);
        for (user, pw, observe, manage) in [
            ("alice", "secret", true, false),
            ("root", "rootpw", true, true),
        ] {
            let (_, me) = call(
                &state,
                Method::GET,
                "/api/v2/whoami",
                Some(&basic(user, pw)),
                None,
            )
            .await;
            assert_eq!(
                me["privileges"],
                json!({"observe": observe, "manage": manage}),
                "{user}"
            );
        }
        // Under none, nobody manages, even by name.
        let state = with_authz(web_state(false, None), None, &["root"]);
        let req = axum::http::Request::builder()
            .uri("/api/v2/whoami")
            .header("x-arneb-user", "root")
            .body(Body::empty())
            .unwrap();
        let resp = build_router(state).oneshot(req).await.unwrap();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let me: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(me["privileges"], json!({"observe": true, "manage": false}));
    }

    #[tokio::test]
    async fn cluster_needs_observe_when_the_section_exists() {
        let state = with_authz(
            web_state(true, None),
            Some(r#"{"system_information":[{"user":"ops","allow":["read"]}]}"#),
            &[],
        );
        let alice = basic("alice", "secret");
        let ops = basic("ops", "opspw");
        for path in [
            "/api/v2/cluster",
            "/api/v2/cluster/workers",
            "/api/v1/cluster",
            "/api/v1/cluster/workers",
        ] {
            let (status, body) = call(&state, Method::GET, path, Some(&alice), None).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
            assert_eq!(body["error"]["code"], "forbidden", "{path}");
            assert!(body["error"]["hint"]
                .as_str()
                .unwrap()
                .contains("allows read for user alice"));
            let (status, _) = call(&state, Method::GET, path, Some(&ops), None).await;
            assert_eq!(status, StatusCode::OK, "{path}");
        }
    }

    const KILL_RULES: &str = r#"{"queries":[
        {"user":"ops","queryOwner":"bob","allow":["kill"]},
        {"allow":["execute","view"]}]}"#;

    /// Password auth with [`KILL_RULES`] and `root` in `admin_users`.
    fn kill_state() -> WebState {
        with_authz(web_state(true, None), Some(KILL_RULES), &["root"])
    }

    fn creds(user: &str) -> String {
        let pw = match user {
            "alice" => "secret",
            "bob" => "hunter2",
            "root" => "rootpw",
            "ops" => "opspw",
            other => panic!("no password for {other}"),
        };
        basic(user, pw)
    }

    /// A running query of `owner`, with a canceller recording who killed it.
    fn running(
        state: &WebState,
        owner: &str,
    ) -> (
        arneb_common::identifiers::QueryId,
        Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let t = &state.query_tracker;
        let id = t.create_query_from("SELECT 1".into(), "pgwire", Some(owner.into()));
        t.advance_to(&id, arneb_scheduler::QueryState::Running)
            .unwrap();
        let killed_by = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&killed_by);
        t.set_canceller(
            &id,
            Box::new(move |by| seen.lock().unwrap().push(by.to_owned())),
        );
        (id, killed_by)
    }

    async fn cancel_as(state: &WebState, user: &str, path: &str) -> (StatusCode, Value) {
        call(state, Method::DELETE, path, Some(&creds(user)), None).await
    }

    /// A request under `[auth] type = "none"` claiming `user`.
    async fn call_as(
        state: &WebState,
        method: Method,
        path: &str,
        user: &str,
    ) -> (StatusCode, Value) {
        let req = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("x-arneb-user", user)
            .body(Body::empty())
            .unwrap();
        let resp = build_router(state.clone()).oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn audit_entries(state: &WebState) -> Vec<Value> {
        let page = state.audit.query(&arneb_state::AuditQuery {
            limit: 100,
            before_seq: None,
            user: None,
            action: None,
        });
        page.entries
            .into_iter()
            .map(|e| serde_json::to_value(e).unwrap())
            .collect()
    }

    fn query_state(
        state: &WebState,
        id: &arneb_common::identifiers::QueryId,
    ) -> arneb_scheduler::QueryState {
        state.query_tracker.get_query(id).unwrap().state
    }

    #[tokio::test]
    async fn owner_cancels_without_audit() {
        let state = kill_state();
        let (id, killed_by) = running(&state, "bob");
        let (status, body) = cancel_as(&state, "bob", &format!("/api/v2/queries/{id}")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dry_run"], false);
        assert_eq!(body["action"], "query.cancel");
        assert_eq!(body["target"], id.to_string());
        assert_eq!(body["changed"], true);
        assert_eq!(body["warning"], Value::Null);
        assert_eq!(
            body["result"],
            json!({"query_id": id.to_string(), "owner": "bob",
                   "state_before": "running", "state": "cancelled"})
        );
        assert_eq!(*killed_by.lock().unwrap(), ["bob"]);
        assert!(
            audit_entries(&state).is_empty(),
            "an owner's cancel is not audited"
        );
    }

    #[tokio::test]
    async fn kill_rule_and_manage_cancel_others_and_are_audited() {
        let state = kill_state();
        let (bobs, bob_killed_by) = running(&state, "bob");
        let (alices, _) = running(&state, "alice");
        let (other_bobs, _) = running(&state, "bob");
        let (status, body) = cancel_as(&state, "ops", &format!("/api/v2/queries/{bobs}")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(*bob_killed_by.lock().unwrap(), ["ops"]);
        let (status, body) = cancel_as(&state, "root", &format!("/api/v2/queries/{alices}")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) =
            cancel_as(&state, "alice", &format!("/api/v2/queries/{other_bobs}")).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "forbidden");
        assert_eq!(
            body["error"]["hint"],
            "cancelling a query of bob needs a queries rule that allows kill for \
             queryOwner bob, or the manage privilege"
        );
        assert_eq!(
            query_state(&state, &other_bobs),
            arneb_scheduler::QueryState::Running
        );

        let entries = audit_entries(&state);
        let seen: Vec<(&str, &str, &str)> = entries
            .iter()
            .map(|e| {
                (
                    e["user"].as_str().unwrap(),
                    e["action"].as_str().unwrap(),
                    e["target"].as_str().unwrap(),
                )
            })
            .collect();
        let (bobs, alices) = (bobs.to_string(), alices.to_string());
        assert_eq!(
            seen,
            [
                ("root", "query.cancel", alices.as_str()),
                ("ops", "query.cancel", bobs.as_str())
            ]
        );
        for e in &entries {
            assert_eq!(e["outcome"], "ok");
            assert_eq!(e["parameters"]["effect"], "applied");
        }
    }

    #[tokio::test]
    async fn dry_run_cancels_nothing() {
        let state = kill_state();
        let (id, killed_by) = running(&state, "alice");
        let (status, body) = cancel_as(
            &state,
            "root",
            &format!("/api/v2/queries/{id}?dry_run=true"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dry_run"], true);
        assert_eq!(body["changed"], true);
        let summary = body["summary"].as_str().unwrap();
        // `^cancels 1 query \(.+\) owned by alice`
        let rest = summary
            .strip_prefix("cancels 1 query (")
            .unwrap_or_default();
        assert!(
            rest.find(") owned by alice").is_some_and(|i| i > 0),
            "{summary}"
        );
        assert!(body.get("result").is_none(), "{body}");
        assert_eq!(
            query_state(&state, &id),
            arneb_scheduler::QueryState::Running
        );
        assert!(killed_by.lock().unwrap().is_empty());
        assert!(audit_entries(&state).is_empty());
        // dry_run=false is the real call.
        let (status, body) = cancel_as(
            &state,
            "root",
            &format!("/api/v2/queries/{id}?dry_run=false"),
        )
        .await;
        assert_eq!((status, &body["dry_run"]), (StatusCode::OK, &json!(false)));
        assert_eq!(
            query_state(&state, &id),
            arneb_scheduler::QueryState::Cancelled
        );
    }

    #[tokio::test]
    async fn forbidden_dry_run_is_refused_like_the_real_call() {
        let state = kill_state();
        let (bobs, killed_by) = running(&state, "bob");
        let path = format!("/api/v2/queries/{bobs}");
        let real = cancel_as(&state, "alice", &path).await;
        let dry = cancel_as(&state, "alice", &format!("{path}?dry_run=true")).await;
        assert_eq!(real.0, StatusCode::FORBIDDEN);
        assert_eq!(dry, real);
        assert_eq!(
            query_state(&state, &bobs),
            arneb_scheduler::QueryState::Running
        );
        assert!(killed_by.lock().unwrap().is_empty());
        assert!(audit_entries(&state).is_empty());

        // Under none: a non-owner is refused the same way, dry or not.
        let state = with_authz(web_state(false, None), None, &["root"]);
        let (bobs, killed_by) = running(&state, "bob");
        let path = format!("/api/v2/queries/{bobs}");
        let real = call_as(&state, Method::DELETE, &path, "root").await;
        let dry = call_as(
            &state,
            Method::DELETE,
            &format!("{path}?dry_run=true"),
            "root",
        )
        .await;
        assert_eq!(real.0, StatusCode::FORBIDDEN, "{}", real.1);
        assert_eq!(real.1["error"]["code"], "manage_requires_auth");
        assert_eq!(dry, real);
        assert!(killed_by.lock().unwrap().is_empty());
        // The owner may still cancel (and dry-run) their own.
        let (status, body) = call_as(
            &state,
            Method::DELETE,
            &format!("{path}?dry_run=true"),
            "bob",
        )
        .await;
        assert_eq!(
            (status, &body["dry_run"]),
            (StatusCode::OK, &json!(true)),
            "{body}"
        );
    }

    #[tokio::test]
    async fn cancelling_an_ended_query_is_a_noop() {
        let state = kill_state();
        let t = &state.query_tracker;
        let id = t.create_query_from("SELECT 1".into(), "pgwire", Some("alice".into()));
        t.finish_query(&id, 1).unwrap();
        let (status, body) = cancel_as(&state, "root", &format!("/api/v2/queries/{id}")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["changed"], false);
        let summary = body["summary"].as_str().unwrap();
        assert!(summary.contains("already finished"), "{summary}");
        assert_eq!(body["result"]["state_before"], "finished");
        assert_eq!(body["result"]["state"], "finished");
        assert_eq!(
            query_state(&state, &id),
            arneb_scheduler::QueryState::Finished
        );
        let entries = audit_entries(&state);
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0]["outcome"], "ok");
        assert_eq!(entries[0]["parameters"]["effect"], "already_ended");
        // The dry run says the same, unaudited.
        let (_, dry) = cancel_as(
            &state,
            "root",
            &format!("/api/v2/queries/{id}?dry_run=true"),
        )
        .await;
        assert_eq!(
            (&dry["changed"], &dry["summary"]),
            (&json!(false), &body["summary"])
        );
        assert_eq!(audit_entries(&state).len(), 1);

        let (status, body) = cancel_as(&state, "root", "/api/v2/queries/not-a-uuid").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
        let missing = format!("/api/v2/queries/{}", uuid::Uuid::new_v4());
        let (status, body) = cancel_as(&state, "root", &missing).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "not_found");
        assert_eq!(audit_entries(&state).len(), 1, "refusals are not audited");
    }

    #[tokio::test]
    async fn bad_dry_run_value_is_invalid_argument() {
        let state = kill_state();
        let (id, _) = running(&state, "alice");
        let (status, body) = cancel_as(
            &state,
            "alice",
            &format!("/api/v2/queries/{id}?dry_run=maybe"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert_eq!(
            query_state(&state, &id),
            arneb_scheduler::QueryState::Running
        );
    }

    #[tokio::test]
    async fn manager_may_cancel_a_query_hidden_from_them() {
        let state = with_authz(
            web_state(true, None),
            Some(r#"{"queries":[{"user":"alice|bob","allow":["execute"]}]}"#),
            &["root"],
        );
        let (id, _) = running(&state, "bob");
        let path = format!("/api/v2/queries/{id}");
        let (status, body) = cancel_as(&state, "alice", &path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "hidden: {body}");
        let (status, body) = cancel_as(&state, "root", &path).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["result"]["state"], "cancelled");
    }

    #[tokio::test]
    async fn v1_cancel_uses_the_same_rule() {
        let state = kill_state();
        let (bobs, killed_by) = running(&state, "bob");
        let path = format!("/api/v1/queries/{bobs}");
        let (status, body) = cancel_as(&state, "alice", &path).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "forbidden");
        let (status, _) = cancel_as(&state, "root", &path).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(*killed_by.lock().unwrap(), ["root"]);
        assert_eq!(
            query_state(&state, &bobs),
            arneb_scheduler::QueryState::Cancelled
        );
        let entries = audit_entries(&state);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["user"], "root");
        assert_eq!(entries[0]["action"], "query.cancel");
        let (status, _) = cancel_as(&state, "root", "/api/v1/queries/not-a-uuid").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let missing = format!("/api/v1/queries/{}", uuid::Uuid::new_v4());
        assert_eq!(
            cancel_as(&state, "root", &missing).await.0,
            StatusCode::NOT_FOUND
        );
    }

    /// Ruling R1: under `[auth] type = "none"`, `/api/v1` keeps its open
    /// cancel (any caller, any query), and now really stops the query;
    /// `/api/v2` follows the kill rule.
    #[tokio::test]
    async fn v1_cancel_under_none_stays_open() {
        let state = with_authz(web_state(false, None), None, &[]);
        let (bobs, killed_by) = running(&state, "bob");
        let (status, body) = call_as(
            &state,
            Method::DELETE,
            &format!("/api/v2/queries/{bobs}"),
            "alice",
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "manage_requires_auth");
        let (status, _) = call_as(
            &state,
            Method::DELETE,
            &format!("/api/v1/queries/{bobs}"),
            "alice",
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(*killed_by.lock().unwrap(), ["alice"]);
        assert_eq!(
            query_state(&state, &bobs),
            arneb_scheduler::QueryState::Cancelled
        );
        // One's own query too, and an ended one is still 204.
        let (alices, _) = running(&state, "alice");
        let path = format!("/api/v1/queries/{alices}");
        assert_eq!(
            call_as(&state, Method::DELETE, &path, "alice").await.0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            call_as(&state, Method::DELETE, &path, "alice").await.0,
            StatusCode::NO_CONTENT
        );
        let missing = format!("/api/v1/queries/{}", uuid::Uuid::new_v4());
        assert_eq!(
            call_as(&state, Method::DELETE, &missing, "alice").await.0,
            StatusCode::NOT_FOUND
        );
        assert!(
            audit_entries(&state).is_empty(),
            "nothing is audited under none"
        );
    }

    /// A failed append leaves the audit log unwritable: issue a token (an
    /// audited action) while appends fail, so later calls see it closed.
    async fn close_the_audit_log(state: &WebState) {
        state.audit.set_fail_appends(true);
        let (status, _) = call(
            state,
            Method::POST,
            "/api/v2/tokens",
            Some(&creds("bob")),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert!(!state.audit.is_writable());
    }

    #[tokio::test]
    async fn dry_run_warns_when_the_audit_log_is_unavailable() {
        let state = kill_state();
        let (id, killed_by) = running(&state, "alice");
        let path = format!("/api/v2/queries/{id}?dry_run=true");
        let (_, body) = cancel_as(&state, "root", &path).await;
        assert_eq!(body["warning"], Value::Null, "{body}");
        close_the_audit_log(&state).await;
        let (status, body) = cancel_as(&state, "root", &path).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["warning"],
            crate::web::manage::AUDIT_UNAVAILABLE_WARNING,
            "{body}"
        );
        assert_eq!(body["changed"], true);
        // An owner's cancel is not audited: no warning.
        let (_, body) = cancel_as(&state, "alice", &path).await;
        assert_eq!(body["warning"], Value::Null, "{body}");
        assert!(killed_by.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn audit_unavailable_kills_nothing() {
        let state = kill_state();
        let (id, killed_by) = running(&state, "alice");
        close_the_audit_log(&state).await;
        let (status, body) = cancel_as(&state, "root", &format!("/api/v2/queries/{id}")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert_eq!(body["error"]["code"], "audit_unavailable");
        assert!(
            killed_by.lock().unwrap().is_empty(),
            "the canceller was not called"
        );
        assert_eq!(
            query_state(&state, &id),
            arneb_scheduler::QueryState::Running
        );
        // v1 under password auth is refused the same way.
        let (status, _) = cancel_as(&state, "root", &format!("/api/v1/queries/{id}")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(killed_by.lock().unwrap().is_empty());
        assert_eq!(
            query_state(&state, &id),
            arneb_scheduler::QueryState::Running
        );
    }
}
