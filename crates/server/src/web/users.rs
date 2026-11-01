//! Users and their API tokens on `/api/v2` (runtime-config D4, D5): list,
//! create, re-password and delete users at runtime, and list, issue and
//! revoke another user's API tokens. Every route needs manage, also when a
//! manager names themselves (the self-service `/api/v2/tokens*` routes stay
//! as they are).
//!
//! - A change is persisted in the state store before the response, so it
//!   applies to pgwire, Trino and the Web API at once. After a password
//!   change or a delete, the user's cached Basic logins are dropped
//!   ([`arneb_protocol::BasicAuthenticator::forget_user`]) and their Web
//!   sessions end ([`super::SessionStore::end_user`]); pgwire connections
//!   already open stay open.
//! - Passwords are hashed (PBKDF2) on the blocking pool behind H1's
//!   issuance limiter, before the change lock is taken. They are never
//!   logged, audited or returned.
//! - Every change holds `WebState::config_changes` (runtime-config D10)
//!   until its audit entry is appended.

use std::ops::RangeInclusive;
use std::sync::Arc;

use arneb_protocol::ScramVerifier;
use arneb_state::{StateError, StateStore, UserInfo, MAX_TOKENS_PER_USER};
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Json;
use axum::routing::{delete, get};
use axum::{Extension, Router};
use serde::{Deserialize, Serialize};

use super::auth::Identity;
use super::error::{ApiError, ErrorCode};
use super::manage::{
    applied_outcome, config_change_lock, dry_run_outcome, run_manage, Applied, ChangeLock, DryRun,
    ManageAction, ManageOutcome, Preview,
};
use super::privilege::Layer;
use super::v2::{
    bad_body, bad_query, blocking_failed, issue_token_for, list_tokens_for, no_such_user,
    revoke_token_for, state_error, IssueTokenRequest, IssuedTokenResponse, TokenTarget, TokenView,
    ISSUE_PERMITS,
};
use super::WebState;

/// Users listed when the request names no `limit`.
const DEFAULT_USER_LIMIT: usize = 100;
/// The largest accepted `limit` of the user and user-token listings.
const MAX_USER_LIMIT: usize = 1000;
/// Tokens listed when the request names no `limit`.
const DEFAULT_TOKEN_LIMIT: usize = 100;
/// The accepted length of a new user name, in characters.
const NAME_CHARS: RangeInclusive<usize> = 1..=64;
/// The accepted length of a password, in characters.
const PASSWORD_CHARS: RangeInclusive<usize> = 8..=1024;

/// One user; never a verifier.
#[derive(Debug, Serialize)]
pub(super) struct UserView {
    name: String,
    /// Whether the config file manages (seeded) the user.
    from_config: bool,
    /// The user's live API tokens.
    tokens: usize,
}

impl From<UserInfo> for UserView {
    fn from(u: UserInfo) -> Self {
        Self {
            name: u.name,
            from_config: u.from_config,
            tokens: u.tokens,
        }
    }
}

/// A bounded page of users, in name order.
#[derive(Debug, Serialize)]
pub(super) struct UserList {
    users: Vec<UserView>,
    truncated: bool,
}

/// The result of a password change.
#[derive(Debug, Serialize)]
pub(super) struct PasswordChanged {
    user: UserView,
    revoked_tokens: usize,
    /// The user was seeded from the config file and no longer is managed
    /// by it.
    detached: bool,
    sessions_ended: usize,
}

/// The result of a delete.
#[derive(Debug, Serialize)]
pub(super) struct UserDeleted {
    name: String,
    revoked_tokens: usize,
    /// A tombstone keeps the next start from re-adding the config user.
    tombstoned: bool,
    sessions_ended: usize,
}

/// A user's tokens, bounded.
#[derive(Serialize)]
pub(super) struct UserTokens {
    user: String,
    tokens: Vec<TokenView>,
    truncated: bool,
}

/// The result of a per-user token revoke.
#[derive(Debug, Serialize)]
pub(super) struct RevokedToken {
    user: String,
    id: String,
}

/// The state store behind the user routes (manage already passed, so the
/// server runs password auth and has one).
fn store(state: &WebState) -> Result<Arc<StateStore>, ApiError> {
    state.state.clone().ok_or_else(|| {
        tracing::error!("password auth is configured without a state store");
        ApiError::new(ErrorCode::Internal, "the state store is unavailable")
    })
}

fn find_user(store: &StateStore, name: &str) -> Option<UserInfo> {
    store.list_users().into_iter().find(|u| u.name == name)
}

fn existing_user(store: &StateStore, name: &str) -> Result<UserInfo, ApiError> {
    find_user(store, name).ok_or_else(|| no_such_user(name))
}

fn check_limit(limit: Option<usize>, default: usize) -> Result<usize, ApiError> {
    let limit = limit.unwrap_or(default);
    if limit == 0 || limit > MAX_USER_LIMIT {
        return Err(ApiError::new(
            ErrorCode::InvalidArgument,
            format!("limit {limit} is not valid"),
        )
        .with_hint(format!("use a limit between 1 and {MAX_USER_LIMIT}")));
    }
    Ok(limit)
}

pub(super) fn validate_name(name: &str) -> Result<(), ApiError> {
    let ok_chars = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '@' | '-'));
    if NAME_CHARS.contains(&name.chars().count()) && ok_chars {
        return Ok(());
    }
    Err(
        ApiError::new(ErrorCode::InvalidArgument, "the user name is not valid").with_hint(format!(
            "a user name is {} to {} characters of A-Z, a-z, 0-9, '_', '.', '@' and '-'",
            NAME_CHARS.start(),
            NAME_CHARS.end()
        )),
    )
}

/// Never echoes the password.
fn validate_password(password: &str) -> Result<(), ApiError> {
    if PASSWORD_CHARS.contains(&password.chars().count()) {
        return Ok(());
    }
    Err(
        ApiError::new(ErrorCode::InvalidArgument, "the password is not valid").with_hint(format!(
            "use a password of {} to {} characters",
            PASSWORD_CHARS.start(),
            PASSWORD_CHARS.end()
        )),
    )
}

/// [`StateError`]s of the user operations as API errors.
fn user_error(e: StateError) -> ApiError {
    match e {
        StateError::UnknownUser(name) => no_such_user(&name),
        StateError::UserExists(name) => user_exists(&name),
        other => state_error(other),
    }
}

fn user_exists(name: &str) -> ApiError {
    ApiError::new(ErrorCode::Conflict, format!("user {name:?} already exists")).with_hint(format!(
        "pick another name, or set a new password with PATCH /api/v2/users/{name}"
    ))
}

/// A fresh SCRAM verifier for `password` (random 16-byte salt, the H1
/// default iteration count), derived on the blocking pool behind
/// [`ISSUE_PERMITS`].
async fn hash_password(password: String) -> Result<ScramVerifier, ApiError> {
    let permit = ISSUE_PERMITS
        .acquire()
        .await
        .map_err(|_| ApiError::new(ErrorCode::Internal, "password hashing is unavailable"))?;
    // The permit moves into the task, so it bounds PBKDF2 even if the
    // request is dropped.
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        ScramVerifier::generate(&password)
    })
    .await
    .map_err(blocking_failed)?
    .map_err(|_| {
        tracing::error!("the system random number generator failed");
        ApiError::new(ErrorCode::Internal, "the password could not be hashed")
    })
}

fn plural(n: usize, one: &str) -> String {
    if n == 1 {
        format!("{n} {one}")
    } else {
        format!("{n} {one}s")
    }
}

fn add_warning(warning: &mut Option<String>, note: String) {
    *warning = Some(match warning.take() {
        Some(w) => format!("{w}; {note}"),
        None => note,
    });
}

/// Every user, in name order, at most `limit` (default 100, at most 1000).
/// Needs manage. Shared with the MCP `list_users` tool.
pub(super) async fn list_users_for(
    state: &WebState,
    who: &Identity,
    limit: Option<usize>,
) -> Result<UserList, ApiError> {
    state.authorizer.check(who, Layer::Manage)?;
    let limit = check_limit(limit, DEFAULT_USER_LIMIT)?;
    let users = store(state)?.list_users();
    let truncated = users.len() > limit;
    Ok(UserList {
        users: users.into_iter().take(limit).map(UserView::from).collect(),
        truncated,
    })
}

/// Creates user `name` with `password` for `who`, who needs manage (also
/// for a dry run). Shared with the MCP `create_user` tool.
pub(super) async fn create_user_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
    name: String,
    password: String,
) -> Result<ManageOutcome<UserView>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    validate_name(&name)?;
    validate_password(&password)?;
    let store = store(state)?;
    if dry_run {
        let action = CreateUser {
            name,
            verifier: None,
            store,
            lock: None,
        };
        return run_manage(state, who, grant, true, action).await;
    }
    // Cheap pre-check, so a taken name never pays for PBKDF2; the store
    // checks again under the lock.
    if find_user(&store, &name).is_some() {
        return Err(user_exists(&name));
    }
    let verifier = hash_password(password).await?;
    let lock = config_change_lock(state, who).await?;
    let action = CreateUser {
        name,
        verifier: Some(verifier),
        store,
        lock: Some(lock),
    };
    run_manage(state, who, grant, false, action).await
}

/// Sets a new `password` for user `name` for `who`, who needs manage (also
/// for a dry run): revokes all of the user's tokens, detaches a config
/// user, drops their cached Basic logins and ends their Web sessions.
/// Shared with the MCP `set_user_password` tool.
pub(super) async fn set_password_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
    name: String,
    password: String,
) -> Result<ManageOutcome<PasswordChanged>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    validate_password(&password)?;
    let store = store(state)?;
    existing_user(&store, &name)?;
    let (verifier, lock) = if dry_run {
        (None, None)
    } else {
        let verifier = hash_password(password).await?;
        (Some(verifier), Some(config_change_lock(state, who).await?))
    };
    let action = SetPassword {
        name,
        verifier,
        store,
        state: state.clone(),
        lock,
    };
    run_manage(state, who, grant, dry_run, action).await
}

/// Deletes user `name` and their tokens for `who`, who needs manage (also
/// for a dry run); deleting yourself is allowed. Shared with the MCP
/// `delete_user` tool.
pub(super) async fn delete_user_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
    name: String,
) -> Result<ManageOutcome<UserDeleted>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    let store = store(state)?;
    existing_user(&store, &name)?;
    let lock = if dry_run {
        None
    } else {
        Some(config_change_lock(state, who).await?)
    };
    let action = DeleteUser {
        name,
        myself: who.user.clone(),
        store,
        state: state.clone(),
        lock,
    };
    run_manage(state, who, grant, dry_run, action).await
}

/// User `name`'s tokens, at most `limit` (default 100, at most 1000).
/// Needs manage. Shared with the MCP `list_user_tokens` tool.
pub(super) async fn list_user_tokens_for(
    state: &WebState,
    who: &Identity,
    name: String,
    limit: Option<usize>,
) -> Result<UserTokens, ApiError> {
    state.authorizer.check(who, Layer::Manage)?;
    let limit = check_limit(limit, DEFAULT_TOKEN_LIMIT)?;
    existing_user(store(state)?.as_ref(), &name)?;
    let mut tokens = list_tokens_for(state, &name)?.tokens;
    let truncated = tokens.len() > limit;
    tokens.truncate(limit);
    Ok(UserTokens {
        user: name,
        tokens,
        truncated,
    })
}

/// Issues an API token for user `name` for `who`, who needs manage (also
/// for a dry run). The secret is shown once, in the result. Audited as
/// `token.issue` with the user as the target. `headers` are the request's
/// (see [`issue_token_for`]). Shared with the MCP `issue_user_token` tool.
pub(super) async fn issue_user_token_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
    name: String,
    headers: &HeaderMap,
    req: IssueTokenRequest,
) -> Result<ManageOutcome<IssuedTokenResponse>, ApiError> {
    const ACTION: &str = "token.issue";
    state.authorizer.check(who, Layer::Manage)?;
    let store = store(state)?;
    let user = existing_user(&store, &name)?;
    let spec = req.clone().validate()?;
    if user.tokens >= MAX_TOKENS_PER_USER {
        return Err(ApiError::new(
            ErrorCode::InvalidArgument,
            format!("user {name:?} already has {MAX_TOKENS_PER_USER} live tokens"),
        )
        .with_hint(format!(
            "revoke unused tokens with DELETE /api/v2/users/{name}/tokens/{{id}}"
        )));
    }
    if dry_run {
        let label = spec
            .name
            .as_deref()
            .map(|n| format!(" named {n:?}"))
            .unwrap_or_default();
        let expiry = if spec.expires_at_ms.is_some() {
            "that expires"
        } else {
            "that never expires"
        };
        let preview = Preview {
            summary: format!(
                "issues an API token{label} for user {name:?} {expiry}; the token is \
                 shown once, in the result"
            ),
            changed: true,
            warning: None,
        };
        return Ok(dry_run_outcome(
            state, who, ACTION, name, preview, true, true,
        ));
    }
    let lock = config_change_lock(state, who).await?;
    let target = TokenTarget::User {
        name: name.clone(),
        _lock: lock,
    };
    let issued = issue_token_for(state, who, target, headers, req).await?;
    let applied = Applied {
        preview: Preview {
            summary: format!("issued API token {} for user {name:?}", issued.id()),
            changed: true,
            warning: None,
        },
        result: issued,
    };
    Ok(applied_outcome(state, ACTION, name, applied, true))
}

/// Revokes user `name`'s token `id` for `who`, who needs manage (also for a
/// dry run); a token of another user is `not_found`. Audited as
/// `token.revoke` with the user as the target. Shared with the MCP
/// `revoke_user_token` tool.
pub(super) async fn revoke_user_token_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
    name: String,
    id: String,
) -> Result<ManageOutcome<RevokedToken>, ApiError> {
    const ACTION: &str = "token.revoke";
    state.authorizer.check(who, Layer::Manage)?;
    let store = store(state)?;
    existing_user(&store, &name)?;
    if dry_run {
        if !arneb_protocol::is_token_id(&id) {
            return Err(
                ApiError::new(ErrorCode::InvalidArgument, "the token id is not valid").with_hint(
                    format!(
                "a token id is 16 lowercase hex characters, as GET /api/v2/users/{name}/tokens \
                 lists"
            ),
                ),
            );
        }
        if !store.list_tokens(&name).iter().any(|t| t.id == id) {
            return Err(ApiError::new(ErrorCode::NotFound, "no such token"));
        }
        let preview = Preview {
            summary: format!("revokes API token {id} of user {name:?}; it stops working at once"),
            changed: true,
            warning: None,
        };
        return Ok(dry_run_outcome(
            state, who, ACTION, name, preview, true, true,
        ));
    }
    let lock = config_change_lock(state, who).await?;
    let target = TokenTarget::User {
        name: name.clone(),
        _lock: lock,
    };
    revoke_token_for(state, who, target, id.clone()).await?;
    let applied = Applied {
        preview: Preview {
            summary: format!("revoked API token {id} of user {name:?}"),
            changed: true,
            warning: None,
        },
        result: RevokedToken {
            user: name.clone(),
            id,
        },
    };
    Ok(applied_outcome(state, ACTION, name, applied, true))
}

struct CreateUser {
    name: String,
    /// `None` on a dry run.
    verifier: Option<ScramVerifier>,
    store: Arc<StateStore>,
    lock: Option<ChangeLock>,
}

impl ManageAction for CreateUser {
    type Output = UserView;

    fn action(&self) -> &'static str {
        "user.create"
    }

    fn target(&self) -> String {
        self.name.clone()
    }

    /// Never the password.
    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({})
    }

    fn persisted(&self) -> bool {
        true
    }

    fn preview(&self, _state: &WebState) -> Result<Preview, ApiError> {
        if find_user(&self.store, &self.name).is_some() {
            return Err(user_exists(&self.name));
        }
        Ok(Preview {
            summary: format!(
                "creates user {:?} with a password login (no API tokens); it can sign in \
                 on pgwire, Trino and the Web API at once",
                self.name
            ),
            changed: true,
            warning: None,
        })
    }

    fn change_lock(&self) -> Option<ChangeLock> {
        self.lock.clone()
    }

    fn apply(self, state: &WebState) -> Result<Applied<UserView>, ApiError> {
        let preview = self.preview(state)?;
        let verifier = self
            .verifier
            .ok_or_else(|| ApiError::new(ErrorCode::Internal, "the password was not hashed"))?;
        self.store
            .create_user(&self.name, verifier)
            .map_err(user_error)?;
        tracing::info!(target: "arneb::config", user = %self.name, "user created at runtime");
        let user = existing_user(&self.store, &self.name)?;
        Ok(Applied {
            preview,
            result: user.into(),
        })
    }
}

/// The `warning` of a password change on a config-seeded user.
const DETACH_WARNING: &str = "the config file no longer manages this user: its \
     password_hash there stops applying, and every start logs a conflict WARN while the \
     name is still in the config file";

struct SetPassword {
    name: String,
    /// `None` on a dry run.
    verifier: Option<ScramVerifier>,
    store: Arc<StateStore>,
    state: WebState,
    lock: Option<ChangeLock>,
}

impl ManageAction for SetPassword {
    type Output = PasswordChanged;

    fn action(&self) -> &'static str {
        "user.set_password"
    }

    fn target(&self) -> String {
        self.name.clone()
    }

    /// Never the password.
    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({})
    }

    fn persisted(&self) -> bool {
        true
    }

    fn preview(&self, _state: &WebState) -> Result<Preview, ApiError> {
        let user = existing_user(&self.store, &self.name)?;
        Ok(Preview {
            summary: format!(
                "sets a new password for user {:?}, revokes their {}, and ends their Web \
                 sessions and cached Basic logins; open pgwire connections stay open",
                self.name,
                plural(user.tokens, "API token")
            ),
            changed: true,
            warning: user.from_config.then(|| DETACH_WARNING.to_owned()),
        })
    }

    fn change_lock(&self) -> Option<ChangeLock> {
        self.lock.clone()
    }

    /// Persists first, then drops the user's cached logins and sessions.
    fn apply(self, state: &WebState) -> Result<Applied<PasswordChanged>, ApiError> {
        let preview = self.preview(state)?;
        let verifier = self
            .verifier
            .ok_or_else(|| ApiError::new(ErrorCode::Internal, "the password was not hashed"))?;
        let change = self
            .store
            .set_user_password(&self.name, verifier)
            .map_err(user_error)?;
        if let Some(basic) = &self.state.basic {
            basic.forget_user(&self.name);
        }
        let sessions_ended = self.state.sessions.end_user(&self.name);
        tracing::info!(
            target: "arneb::config",
            user = %self.name,
            revoked_tokens = change.revoked_tokens,
            detached = change.detached,
            "user password changed at runtime"
        );
        let user = existing_user(&self.store, &self.name)?;
        Ok(Applied {
            preview,
            result: PasswordChanged {
                user: user.into(),
                revoked_tokens: change.revoked_tokens,
                detached: change.detached,
                sessions_ended,
            },
        })
    }
}

struct DeleteUser {
    name: String,
    /// The caller, to warn when they delete themselves.
    myself: String,
    store: Arc<StateStore>,
    state: WebState,
    lock: Option<ChangeLock>,
}

impl ManageAction for DeleteUser {
    type Output = UserDeleted;

    fn action(&self) -> &'static str {
        "user.delete"
    }

    fn target(&self) -> String {
        self.name.clone()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({})
    }

    fn persisted(&self) -> bool {
        true
    }

    fn preview(&self, _state: &WebState) -> Result<Preview, ApiError> {
        let user = existing_user(&self.store, &self.name)?;
        let mut warning = None;
        if user.config_seeded {
            add_warning(
                &mut warning,
                format!(
                    "user {:?} is in the config file: a tombstone keeps the next start from \
                     re-adding it until a re-seed (POST /api/v2/state/reseed)",
                    self.name
                ),
            );
        }
        if self.name == self.myself {
            add_warning(
                &mut warning,
                "you are deleting yourself: your password, API tokens and Web sessions stop \
                 working after this request"
                    .to_owned(),
            );
        }
        Ok(Preview {
            summary: format!(
                "deletes user {:?} and their {}, and ends their Web sessions and cached \
                 Basic logins; open pgwire connections stay open",
                self.name,
                plural(user.tokens, "API token")
            ),
            changed: true,
            warning,
        })
    }

    fn change_lock(&self) -> Option<ChangeLock> {
        self.lock.clone()
    }

    /// Persists first, then drops the user's cached logins and sessions.
    fn apply(self, state: &WebState) -> Result<Applied<UserDeleted>, ApiError> {
        let preview = self.preview(state)?;
        let gone = self.store.delete_user(&self.name).map_err(user_error)?;
        if let Some(basic) = &self.state.basic {
            basic.forget_user(&self.name);
        }
        let sessions_ended = self.state.sessions.end_user(&self.name);
        tracing::info!(
            target: "arneb::config",
            user = %self.name,
            revoked_tokens = gone.revoked_tokens,
            tombstoned = gone.tombstoned,
            "user deleted at runtime"
        );
        Ok(Applied {
            preview,
            result: UserDeleted {
                name: self.name,
                revoked_tokens: gone.revoked_tokens,
                tombstoned: gone.tombstoned,
                sessions_ended,
            },
        })
    }
}

/// `?limit=` of the listings.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListParams {
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateUserBody {
    name: String,
    password: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PasswordBody {
    password: String,
}

/// Parses a body that carries a password. The parser's message is dropped,
/// since it can quote the input.
fn parse_secret_body<T: serde::de::DeserializeOwned>(
    body: Result<Bytes, BytesRejection>,
    expected: &str,
) -> Result<T, ApiError> {
    let body = body.map_err(bad_body)?;
    serde_json::from_slice(&body).map_err(|_| {
        ApiError::new(ErrorCode::InvalidArgument, "invalid request body")
            .with_hint(format!("send the JSON object {expected}"))
    })
}

async fn list_users(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<ListParams>, QueryRejection>,
) -> Result<Json<UserList>, ApiError> {
    let Query(params) = params.map_err(bad_query)?;
    Ok(Json(list_users_for(&state, &who, params.limit).await?))
}

async fn create_user(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<DryRun>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ManageOutcome<UserView>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    state.authorizer.check(&who, Layer::Manage)?;
    let body: CreateUserBody =
        parse_secret_body(body, r#"{"name": "<user>", "password": "<password>"}"#)?;
    Ok(Json(
        create_user_for(&state, &who, dry_run, body.name, body.password).await?,
    ))
}

async fn set_password(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(name): Path<String>,
    params: Result<Query<DryRun>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ManageOutcome<PasswordChanged>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    state.authorizer.check(&who, Layer::Manage)?;
    let body: PasswordBody = parse_secret_body(body, r#"{"password": "<password>"}"#)?;
    Ok(Json(
        set_password_for(&state, &who, dry_run, name, body.password).await?,
    ))
}

async fn delete_user(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(name): Path<String>,
    params: Result<Query<DryRun>, QueryRejection>,
) -> Result<Json<ManageOutcome<UserDeleted>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    Ok(Json(delete_user_for(&state, &who, dry_run, name).await?))
}

async fn list_user_tokens(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(name): Path<String>,
    params: Result<Query<ListParams>, QueryRejection>,
) -> Result<Json<UserTokens>, ApiError> {
    let Query(params) = params.map_err(bad_query)?;
    Ok(Json(
        list_user_tokens_for(&state, &who, name, params.limit).await?,
    ))
}

async fn issue_user_token(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(name): Path<String>,
    params: Result<Query<DryRun>, QueryRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ManageOutcome<IssuedTokenResponse>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    state.authorizer.check(&who, Layer::Manage)?;
    let req: IssueTokenRequest = super::v2::parse_body(&body.map_err(bad_body)?, true)?;
    Ok(Json(
        issue_user_token_for(&state, &who, dry_run, name, &headers, req).await?,
    ))
}

async fn revoke_user_token(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path((name, id)): Path<(String, String)>,
    params: Result<Query<DryRun>, QueryRejection>,
) -> Result<Json<ManageOutcome<RevokedToken>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    Ok(Json(
        revoke_user_token_for(&state, &who, dry_run, name, id).await?,
    ))
}

/// The user routes, merged into `/api/v2`.
pub(super) fn routes() -> Router<WebState> {
    Router::new()
        .route("/users", get(list_users).post(create_user))
        .route("/users/{name}", delete(delete_user).patch(set_password))
        .route(
            "/users/{name}/tokens",
            get(list_user_tokens).post(issue_user_token),
        )
        .route("/users/{name}/tokens/{id}", delete(revoke_user_token))
}

#[cfg(test)]
mod tests {
    use axum::http::{Method, StatusCode};
    use serde_json::{json, Value};

    use super::*;
    use crate::web::v2::tests::{basic, call, web_state, with_authz};

    /// Password auth with `root` in admin_users; bob does not manage.
    fn node() -> WebState {
        with_authz(web_state(true, None), None, &["root"])
    }

    fn root() -> String {
        basic("root", "rootpw")
    }

    fn bob() -> String {
        basic("bob", "hunter2")
    }

    fn audit_json(state: &WebState) -> Vec<Value> {
        state
            .audit
            .query(&arneb_state::AuditQuery {
                limit: 100,
                before_seq: None,
                user: None,
                action: None,
            })
            .entries
            .into_iter()
            .map(|e| serde_json::to_value(&e).unwrap())
            .collect()
    }

    async fn whoami(state: &WebState, auth: &str) -> (StatusCode, Value) {
        call(state, Method::GET, "/api/v2/whoami", Some(auth), None).await
    }

    async fn create(state: &WebState, name: &str, password: &str) -> (StatusCode, Value) {
        call(
            state,
            Method::POST,
            "/api/v2/users",
            Some(&root()),
            Some(json!({"name": name, "password": password})),
        )
        .await
    }

    fn names(state: &WebState) -> Vec<String> {
        state
            .state
            .as_ref()
            .unwrap()
            .list_users()
            .into_iter()
            .map(|u| u.name)
            .collect()
    }

    #[tokio::test]
    async fn user_routes_require_manage() {
        let state = node();
        let id = "0123456789abcdef";
        let body = json!({"name": "dave", "password": "davepass1"});
        for user in ["alice", "bob"] {
            let routes: Vec<(Method, String, Option<Value>)> = vec![
                (Method::GET, "/api/v2/users".into(), None),
                (Method::POST, "/api/v2/users".into(), Some(body.clone())),
                (
                    Method::PATCH,
                    format!("/api/v2/users/{user}"),
                    Some(json!({"password": "newpass12"})),
                ),
                (Method::DELETE, format!("/api/v2/users/{user}"), None),
                (Method::GET, format!("/api/v2/users/{user}/tokens"), None),
                (Method::POST, format!("/api/v2/users/{user}/tokens"), None),
                (
                    Method::DELETE,
                    format!("/api/v2/users/{user}/tokens/{id}"),
                    None,
                ),
            ];
            for (method, path, body) in routes {
                let dry = (method != Method::GET).then(|| format!("{path}?dry_run=true"));
                for path in std::iter::once(path.clone()).chain(dry) {
                    let (status, resp) =
                        call(&state, method.clone(), &path, Some(&bob()), body.clone()).await;
                    assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {resp}");
                    assert_eq!(resp["error"]["code"], "forbidden", "{method} {path}");
                }
            }
        }
        assert!(!names(&state).contains(&"dave".to_owned()));
        assert!(names(&state).contains(&"alice".to_owned()));
        assert!(audit_json(&state).is_empty());
    }

    #[tokio::test]
    async fn user_routes_under_none_need_auth() {
        let state = with_authz(web_state(false, None), None, &["root"]);
        let (status, body) = create(&state, "dave", "davepass1").await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "manage_requires_auth");
    }

    #[tokio::test]
    async fn create_user_validates_name_and_password() {
        let state = node();
        let long = "a".repeat(65);
        for (name, password) in [
            ("", "davepass1"),
            (long.as_str(), "davepass1"),
            ("da/ve", "davepass1"),
            ("da ve", "davepass1"),
            ("dave", "7chars!"),
        ] {
            let (status, body) = create(&state, name, password).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{name:?}: {body}");
            assert_eq!(body["error"]["code"], "invalid_argument", "{name:?}");
        }
        // The limits themselves are fine.
        let max = "b".repeat(64);
        let (status, body) = create(&state, &max, "8chars!!").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = create(&state, "x.y_z-w@corp", &"p".repeat(1024)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = create(&state, "dave", &"p".repeat(1025)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        // A taken name is a conflict.
        let (status, body) = create(&state, "alice", "davepass1").await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["code"], "conflict");
        // A body that is not the expected object.
        let (status, body) = call(
            &state,
            Method::POST,
            "/api/v2/users",
            Some(&root()),
            Some(json!({"name": "dave"})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn create_user_dry_run_changes_nothing() {
        let state = node();
        let (status, body) = call(
            &state,
            Method::POST,
            "/api/v2/users?dry_run=true",
            Some(&root()),
            Some(json!({"name": "dave", "password": "davepass1"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dry_run"], true);
        assert_eq!(body["action"], "user.create");
        assert_eq!(body["target"], "dave");
        assert_eq!(body["changed"], true);
        assert!(body.get("result").is_none(), "{body}");
        assert!(!names(&state).contains(&"dave".to_owned()));
        assert!(audit_json(&state).is_empty());
        let (status, _) = whoami(&state, &basic("dave", "davepass1")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        // A dry run on a taken name is refused like the real call.
        let (status, body) = call(
            &state,
            Method::POST,
            "/api/v2/users?dry_run=true",
            Some(&root()),
            Some(json!({"name": "alice", "password": "davepass1"})),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
    }

    #[tokio::test]
    async fn created_user_signs_in_and_is_listed() {
        let state = node();
        let (status, body) = create(&state, "dave", "davepass1").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dry_run"], false);
        assert_eq!(
            body["result"],
            json!({"name": "dave", "from_config": false, "tokens": 0})
        );
        // In memory: the change is lost on restart.
        assert!(body["warning"].as_str().unwrap().contains("in memory"));
        let (status, me) = whoami(&state, &basic("dave", "davepass1")).await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["user"], "dave");
        let entries = audit_json(&state);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["action"], "user.create");
        assert_eq!(entries[0]["target"], "dave");
        assert_eq!(entries[0]["parameters"], json!({"effect": "applied"}));
    }

    #[tokio::test]
    async fn password_never_in_audit_or_response() {
        let state = node();
        let first = "Fir5t-Secret-pw";
        let second = "S3cond-Secret-pw";
        let mut bodies = Vec::new();
        for dry in [true, false] {
            let q = if dry { "?dry_run=true" } else { "" };
            let (_, b) = call(
                &state,
                Method::POST,
                &format!("/api/v2/users{q}"),
                Some(&root()),
                Some(json!({"name": "dave", "password": first})),
            )
            .await;
            bodies.push(b);
        }
        for dry in [true, false] {
            let q = if dry { "?dry_run=true" } else { "" };
            let (_, b) = call(
                &state,
                Method::PATCH,
                &format!("/api/v2/users/dave{q}"),
                Some(&root()),
                Some(json!({"password": second})),
            )
            .await;
            bodies.push(b);
        }
        // Errors too: a taken name, a bad body.
        let (_, b) = create(&state, "dave", first).await;
        bodies.push(b);
        let (_, b) = call(
            &state,
            Method::PATCH,
            "/api/v2/users/dave",
            Some(&root()),
            Some(json!({"password": second, "extra": second})),
        )
        .await;
        bodies.push(b);
        let audit = serde_json::to_string(&audit_json(&state)).unwrap();
        assert!(audit.contains("user.set_password"), "{audit}");
        for text in bodies.iter().map(Value::to_string).chain([audit]) {
            assert!(!text.contains(first) && !text.contains(second), "{text}");
            assert!(!text.contains("SCRAM-SHA-256"), "{text}");
        }
        let (status, _) = whoami(&state, &basic("dave", second)).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn password_change_drops_the_basic_cache() {
        let state = node();
        let old = basic("alice", "secret");
        // The first request caches alice's Basic login.
        let (status, _) = whoami(&state, &old).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = whoami(&state, &old).await;
        assert_eq!(status, StatusCode::OK);
        let token = state
            .state
            .as_ref()
            .unwrap()
            .issue_token("alice", None, None)
            .unwrap();

        let (status, dry) = call(
            &state,
            Method::PATCH,
            "/api/v2/users/alice?dry_run=true",
            Some(&root()),
            Some(json!({"password": "brand-new-pw"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{dry}");
        assert!(
            dry["summary"].as_str().unwrap().contains("1 API token,"),
            "{dry}"
        );
        let warning = dry["warning"].as_str().unwrap();
        assert!(
            warning.contains("config file no longer manages"),
            "{warning}"
        );
        let (status, _) = whoami(&state, &old).await;
        assert_eq!(status, StatusCode::OK, "a dry run changes nothing");

        let (status, body) = call(
            &state,
            Method::PATCH,
            "/api/v2/users/alice",
            Some(&root()),
            Some(json!({"password": "brand-new-pw"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["action"], "user.set_password");
        assert_eq!(body["result"]["revoked_tokens"], 1);
        assert_eq!(body["result"]["detached"], true);
        assert_eq!(body["result"]["user"]["from_config"], false);
        // Right after the PATCH: the cached old login is refused.
        let (status, _) = whoami(&state, &old).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = whoami(&state, &format!("Bearer {}", token.token)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "tokens are revoked");
        let (status, _) = whoami(&state, &basic("alice", "brand-new-pw")).await;
        assert_eq!(status, StatusCode::OK);
        let entries = audit_json(&state);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["target"], "alice");

        // The Trino listener shares this authenticator: its cache is the
        // one that was just dropped.
        let basic_auth = state.basic.as_ref().unwrap();
        assert_eq!(
            basic_auth.check("alice", "secret").await,
            arneb_protocol::BasicVerdict::Invalid
        );
    }

    #[tokio::test]
    async fn patch_and_delete_of_an_unknown_user_are_not_found() {
        let state = node();
        for q in ["", "?dry_run=true"] {
            let (status, body) = call(
                &state,
                Method::PATCH,
                &format!("/api/v2/users/nobody{q}"),
                Some(&root()),
                Some(json!({"password": "whatever1"})),
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
            assert_eq!(body["error"]["code"], "not_found");
            let (status, body) = call(
                &state,
                Method::DELETE,
                &format!("/api/v2/users/nobody{q}"),
                Some(&root()),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
            for path in [
                format!("/api/v2/users/nobody/tokens{q}"),
                format!("/api/v2/users/nobody/tokens/0123456789abcdef{q}"),
            ] {
                let method = if path.ends_with("tokens") || path.ends_with("tokens?dry_run=true") {
                    Method::POST
                } else {
                    Method::DELETE
                };
                let (status, body) = call(&state, method, &path, Some(&root()), None).await;
                assert_eq!(status, StatusCode::NOT_FOUND, "{path}: {body}");
            }
        }
        let (status, _) = call(
            &state,
            Method::GET,
            "/api/v2/users/nobody/tokens",
            Some(&root()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(audit_json(&state).is_empty());
    }

    #[tokio::test]
    async fn delete_user_ends_sessions() {
        let state = node();
        let (status, login) = call(
            &state,
            Method::POST,
            "/api/v2/sessions",
            None,
            Some(json!({"user": "alice", "password": "secret"})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{login}");
        let session = format!("Bearer {}", login["token"].as_str().unwrap());
        let (status, _) = whoami(&state, &session).await;
        assert_eq!(status, StatusCode::OK);

        let (status, dry) = call(
            &state,
            Method::DELETE,
            "/api/v2/users/alice?dry_run=true",
            Some(&root()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{dry}");
        assert!(
            dry["warning"].as_str().unwrap().contains("tombstone"),
            "{dry}"
        );
        let (status, _) = whoami(&state, &session).await;
        assert_eq!(status, StatusCode::OK, "a dry run changes nothing");

        let (status, body) = call(
            &state,
            Method::DELETE,
            "/api/v2/users/alice",
            Some(&root()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["action"], "user.delete");
        assert_eq!(body["result"]["tombstoned"], true);
        assert_eq!(body["result"]["sessions_ended"], 1);
        let (status, _) = whoami(&state, &session).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = whoami(&state, &basic("alice", "secret")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(!names(&state).contains(&"alice".to_owned()));
    }

    #[tokio::test]
    async fn deleting_yourself_warns_and_works() {
        let state = node();
        let (status, dry) = call(
            &state,
            Method::DELETE,
            "/api/v2/users/root?dry_run=true",
            Some(&root()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{dry}");
        assert!(
            dry["warning"]
                .as_str()
                .unwrap()
                .contains("deleting yourself"),
            "{dry}"
        );
        let (status, _) = call(
            &state,
            Method::DELETE,
            "/api/v2/users/root",
            Some(&root()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = whoami(&state, &root()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn manager_issues_and_revokes_token_for_other_user() {
        let state = node();
        let (status, body) = create(&state, "dave", "davepass1").await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, dry) = call(
            &state,
            Method::POST,
            "/api/v2/users/dave/tokens?dry_run=true",
            Some(&root()),
            Some(json!({"name": "ci"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{dry}");
        assert_eq!(dry["action"], "token.issue");
        assert_eq!(dry["target"], "dave");
        assert!(dry.get("result").is_none());
        assert!(state.state.as_ref().unwrap().list_tokens("dave").is_empty());

        let (status, body) = call(
            &state,
            Method::POST,
            "/api/v2/users/dave/tokens",
            Some(&root()),
            Some(json!({"name": "ci", "expires_in_days": 7})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["action"], "token.issue");
        assert_eq!(body["target"], "dave");
        let token = body["result"]["token"].as_str().unwrap().to_owned();
        let id = body["result"]["id"].as_str().unwrap().to_owned();
        assert!(token.starts_with(&format!("arneb_{id}_")));

        let entries = audit_json(&state);
        let issue = entries
            .iter()
            .find(|e| e["action"] == "token.issue")
            .unwrap();
        assert_eq!(issue["target"], "dave");
        assert_eq!(issue["user"], "root");
        assert_eq!(issue["parameters"]["id"], id);
        assert_eq!(issue["parameters"]["name"], "ci");
        let all = serde_json::to_string(&entries).unwrap();
        assert!(!all.contains(&token), "never the token in the audit log");

        let bearer = format!("Bearer {token}");
        let (status, me) = whoami(&state, &bearer).await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["user"], "dave");

        let (status, list) = call(
            &state,
            Method::GET,
            "/api/v2/users/dave/tokens",
            Some(&root()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list["user"], "dave");
        assert_eq!(list["truncated"], false);
        assert_eq!(list["tokens"][0]["id"], id);
        assert!(!list.to_string().contains(&token));
        let (_, users) = call(&state, Method::GET, "/api/v2/users", Some(&root()), None).await;
        let dave = users["users"]
            .as_array()
            .unwrap()
            .iter()
            .find(|u| u["name"] == "dave")
            .unwrap();
        assert_eq!(dave["tokens"], 1);

        // Another user's token through dave's path is not found.
        let alice = state
            .state
            .as_ref()
            .unwrap()
            .issue_token("alice", None, None)
            .unwrap();
        for q in ["", "?dry_run=true"] {
            let (status, body) = call(
                &state,
                Method::DELETE,
                &format!("/api/v2/users/dave/tokens/{}{q}", alice.info.id),
                Some(&root()),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
            assert_eq!(body["error"]["code"], "not_found");
        }
        let (status, _) = whoami(&state, &format!("Bearer {}", alice.token)).await;
        assert_eq!(status, StatusCode::OK, "alice's token survives");

        let (status, body) = call(
            &state,
            Method::DELETE,
            &format!("/api/v2/users/dave/tokens/{id}"),
            Some(&root()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["action"], "token.revoke");
        assert_eq!(body["result"], json!({"user": "dave", "id": id}));
        let (status, _) = whoami(&state, &bearer).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let revoke = audit_json(&state)
            .into_iter()
            .find(|e| e["action"] == "token.revoke" && e["outcome"] == "ok")
            .unwrap();
        assert_eq!(revoke["target"], "dave");
        assert_eq!(revoke["parameters"]["id"], id);

        // The self-service routes keep the token id as the audit target.
        let (status, own) = call(&state, Method::POST, "/api/v2/tokens", Some(&root()), None).await;
        assert_eq!(status, StatusCode::CREATED, "{own}");
        let own_issue = audit_json(&state)
            .into_iter()
            .find(|e| e["action"] == "token.issue" && e["user"] == "root" && e["target"] != "dave")
            .unwrap();
        assert_eq!(own_issue["target"], own["id"]);
    }

    #[tokio::test]
    async fn list_users_bounded() {
        let state = node();
        for limit in [0, 1001] {
            let (status, body) = call(
                &state,
                Method::GET,
                &format!("/api/v2/users?limit={limit}"),
                Some(&root()),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(body["error"]["code"], "invalid_argument");
            let (status, _) = call(
                &state,
                Method::GET,
                &format!("/api/v2/users/alice/tokens?limit={limit}"),
                Some(&root()),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
        let (status, body) = call(
            &state,
            Method::GET,
            "/api/v2/users?limit=2",
            Some(&root()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["truncated"], true);
        assert_eq!(
            body["users"],
            json!([
                {"name": "alice", "from_config": true, "tokens": 0},
                {"name": "bob", "from_config": true, "tokens": 0},
            ])
        );
        let (_, body) = call(&state, Method::GET, "/api/v2/users", Some(&root()), None).await;
        assert_eq!(body["truncated"], false);
        let listed: Vec<&str> = body["users"]
            .as_array()
            .unwrap()
            .iter()
            .map(|u| u["name"].as_str().unwrap())
            .collect();
        assert_eq!(listed, ["alice", "bob", "carol", "ops", "root"]);
    }

    /// A request dropped while its change runs still holds the config lock
    /// until the audit entry is appended.
    #[tokio::test]
    async fn dropped_request_holds_the_config_lock_until_audited() {
        let state = node();
        let (inside, release) =
            crate::web::manage::test_hooks::stop_after_apply(&state.config_changes);
        let call = tokio::spawn({
            let state = state.clone();
            async move {
                call(
                    &state,
                    Method::DELETE,
                    "/api/v2/users/bob",
                    Some(&root()),
                    None,
                )
                .await
            }
        });
        tokio::task::spawn_blocking(move || inside.recv().unwrap())
            .await
            .unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(state.config_changes.try_lock().is_err());
        assert!(audit_json(&state).is_empty());
        release.send(()).unwrap();
        let _next = state.config_changes.lock().await;
        assert_eq!(audit_json(&state).len(), 1);
    }
    #[tokio::test]
    async fn deleting_a_detached_config_user_warns_about_the_tombstone() {
        let state = node();
        let (status, _) = call(
            &state,
            Method::PATCH,
            "/api/v2/users/alice",
            Some(&root()),
            Some(json!({"password": "brand-new-pw"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, dry) = call(
            &state,
            Method::DELETE,
            "/api/v2/users/alice?dry_run=true",
            Some(&root()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{dry}");
        assert!(
            dry["warning"].as_str().unwrap().contains("tombstone"),
            "{dry}"
        );
        let (status, body) = call(
            &state,
            Method::DELETE,
            "/api/v2/users/alice",
            Some(&root()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["result"]["tombstoned"], true);
    }

    /// The per-user token routes build their envelope outside `run_manage`:
    /// a dry run still warns, and a real run is still refused, when the
    /// audit log is not writable.
    #[tokio::test]
    async fn user_token_routes_respect_an_unwritable_audit_log() {
        let state = node();
        let alice = state
            .state
            .as_ref()
            .unwrap()
            .issue_token("alice", None, None)
            .unwrap();
        // One failed append marks the log unwritable.
        state.audit.set_fail_appends(true);
        let (status, _) = create(&state, "dave", "davepass1").await;
        assert_eq!(status, StatusCode::OK, "applied; only its append failed");
        assert!(!state.audit.is_writable());
        let revoke = format!("/api/v2/users/alice/tokens/{}", alice.info.id);
        for (method, path) in [
            (Method::POST, "/api/v2/users/alice/tokens".to_owned()),
            (Method::DELETE, revoke.clone()),
        ] {
            let (status, dry) = call(
                &state,
                method.clone(),
                &format!("{path}?dry_run=true"),
                Some(&root()),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{path}: {dry}");
            let warning = dry["warning"].as_str().unwrap();
            assert!(
                warning.contains(crate::web::manage::AUDIT_UNAVAILABLE_WARNING),
                "{path}: {warning}"
            );
            let (status, body) = call(&state, method, &path, Some(&root()), None).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}: {body}");
            assert_eq!(body["error"]["code"], "audit_unavailable", "{path}");
        }
        let store = state.state.as_ref().unwrap();
        assert_eq!(
            store.list_tokens("alice").len(),
            1,
            "nothing issued or revoked"
        );
    }
}
