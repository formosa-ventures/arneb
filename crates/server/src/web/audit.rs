//! The audit log on the Web API (manage-api D6): the [`audited`] wrapper
//! every audited action runs through, and `GET /api/v2/audit`.
//!
//! Order and failure: the log must be writable (or reopen) before the
//! action runs, else `audit_unavailable`; then the action runs; then its
//! entry is appended. A failed append cannot undo the action, so the entry
//! goes to the ERROR log, the log is marked unwritable, and the action's
//! real result is returned; later audited actions are refused until the
//! log is writable again. So at most one entry is missing per action in
//! flight when the log fails, and each is logged in full at ERROR on
//! `arneb::audit`.

use arneb_state::{AuditEntry, AuditOutcome, AuditQuery, AuditRecord};
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::response::Json;
use axum::routing::get;
use axum::{Extension, Router};
use serde::{Deserialize, Serialize};

use super::auth::Identity;
use super::error::{ApiError, ErrorCode};
use super::privilege::Layer;
use super::v2::bad_query;
use super::WebState;

/// Entries returned when the request names no `limit`.
pub(super) const DEFAULT_AUDIT_LIMIT: usize = 50;
/// The largest accepted `limit`.
pub(super) const MAX_AUDIT_LIMIT: usize = 500;

/// What an audited action acted on, described once its result is known.
pub(super) struct AuditSubject {
    pub target: String,
    pub parameters: serde_json::Value,
}

fn audit_unavailable() -> ApiError {
    ApiError::new(
        ErrorCode::AuditUnavailable,
        "the audit log cannot be written, so audited actions are refused",
    )
    .with_hint("retry shortly; the server log names the audit log error")
}

/// Runs `run` as the audited `action` of `who`: refuses with
/// `audit_unavailable` before running it when the log is unwritable and
/// cannot be reopened; otherwise runs it and appends an entry described by
/// `subject` (ok or error outcome). Blocking (fsync): call it from
/// `spawn_blocking`.
pub(super) fn audited<T>(
    state: &WebState,
    who: &Identity,
    action: &'static str,
    run: impl FnOnce() -> Result<T, ApiError>,
    subject: impl FnOnce(Result<&T, &ApiError>) -> AuditSubject,
) -> Result<T, ApiError> {
    if let Err(e) = state.audit.ensure_writable() {
        tracing::error!(
            target: "arneb::audit",
            user = %who.user,
            action,
            error = %e,
            "refusing an audited action: the audit log is not writable"
        );
        return Err(audit_unavailable());
    }
    let out = run();
    let AuditSubject { target, parameters } = subject(out.as_ref());
    let record = AuditRecord {
        user: who.user.clone(),
        via: who.via.as_str().to_owned(),
        action: action.to_owned(),
        target,
        parameters,
        outcome: match &out {
            Ok(_) => AuditOutcome::Ok,
            Err(e) => AuditOutcome::Error(e.code.name()),
        },
    };
    if let Err(arneb_state::AuditAppendError { entry, source }) = state.audit.append(record) {
        tracing::error!(
            target: "arneb::audit",
            seq = entry.seq,
            time_ms = entry.time_ms,
            entry = %serde_json::to_string(&entry).unwrap_or_default(),
            error = %source,
            "the action was applied but its audit entry could not be written; \
             audited actions are refused until the audit log is writable"
        );
    }
    out
}

/// `GET /api/v2/audit` query parameters.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AuditParams {
    pub limit: Option<usize>,
    /// The previous page's `next_cursor`: entries with a lower `seq`.
    pub cursor: Option<u64>,
    pub user: Option<String>,
    pub action: Option<String>,
}

/// A page of the audit log, newest first.
#[derive(Debug, Serialize)]
pub(super) struct AuditResponse {
    entries: Vec<AuditEntry>,
    next_cursor: Option<u64>,
    truncated: bool,
    /// Whether the log is on disk (false: lost on restart).
    persistent: bool,
    /// Whether audited actions are accepted right now.
    writable: bool,
}

/// The audit log page for `who`, who needs observe. Shared with the MCP
/// `list_audit_log` tool.
pub(super) fn audit_page_for(
    state: &WebState,
    who: &Identity,
    params: AuditParams,
) -> Result<AuditResponse, ApiError> {
    state.authorizer.check(who, Layer::Observe)?;
    let limit = params.limit.unwrap_or(DEFAULT_AUDIT_LIMIT);
    if limit == 0 || limit > MAX_AUDIT_LIMIT {
        return Err(ApiError::new(
            ErrorCode::InvalidArgument,
            format!("limit {limit} is not valid"),
        )
        .with_hint(format!("use a limit between 1 and {MAX_AUDIT_LIMIT}")));
    }
    let page = state.audit.query(&AuditQuery {
        limit,
        before_seq: params.cursor,
        user: params.user,
        action: params.action,
    });
    Ok(AuditResponse {
        entries: page.entries,
        next_cursor: page.next_cursor,
        truncated: page.truncated,
        persistent: state.audit.is_persistent(),
        writable: state.audit.is_writable(),
    })
}

async fn audit(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<AuditParams>, QueryRejection>,
) -> Result<Json<AuditResponse>, ApiError> {
    let Query(params) = params.map_err(bad_query)?;
    Ok(Json(audit_page_for(&state, &who, params)?))
}

/// The audit routes, merged into `/api/v2`.
pub(super) fn routes() -> Router<WebState> {
    Router::new().route("/audit", get(audit))
}

#[cfg(test)]
mod tests {
    use axum::http::{Method, StatusCode};
    use serde_json::{json, Value};

    use crate::web::v2::tests::{basic, call, web_state, with_authz};
    use crate::web::WebState;

    fn state() -> WebState {
        with_authz(web_state(true, None), None, &["root"])
    }

    fn alice() -> String {
        basic("alice", "secret")
    }

    async fn issue(state: &WebState) -> (StatusCode, Value) {
        call(
            state,
            Method::POST,
            "/api/v2/tokens",
            Some(&alice()),
            Some(json!({"name": "ci"})),
        )
        .await
    }

    async fn audit_log(state: &WebState, query: &str) -> (StatusCode, Value) {
        call(
            state,
            Method::GET,
            &format!("/api/v2/audit{query}"),
            Some(&basic("root", "rootpw")),
            None,
        )
        .await
    }

    async fn token_count(state: &WebState) -> usize {
        let (status, body) = call(state, Method::GET, "/api/v2/tokens", Some(&alice()), None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["tokens"].as_array().unwrap().len()
    }

    #[tokio::test]
    async fn token_issue_and_revoke_are_audited() {
        let state = state();
        let (status, issued) = issue(&state).await;
        assert_eq!(status, StatusCode::CREATED, "{issued}");
        let token = issued["token"].as_str().unwrap().to_owned();
        let id = issued["id"].as_str().unwrap().to_owned();
        assert_eq!(id.len(), 16);
        let (status, _) = call(
            &state,
            Method::DELETE,
            &format!("/api/v2/tokens/{id}"),
            Some(&alice()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, body) = audit_log(&state, "").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let entries = body["entries"].as_array().unwrap();
        let actions: Vec<&str> = entries
            .iter()
            .map(|e| e["action"].as_str().unwrap())
            .collect();
        assert_eq!(actions, ["token.revoke", "token.issue"]);
        for e in entries {
            assert_eq!(e["user"], "alice");
            assert_eq!(e["via"], "basic");
            assert_eq!(e["target"], id.as_str());
            assert_eq!(e["outcome"], "ok");
        }
        assert_eq!(entries[1]["parameters"]["name"], "ci");
        assert!(entries[1]["parameters"]
            .as_object()
            .unwrap()
            .contains_key("expires_ms"));
        assert_eq!(entries[0]["parameters"], json!({}));
        assert_eq!(body["truncated"], false);
        assert_eq!(body["next_cursor"], Value::Null);
        assert_eq!(body["persistent"], false);
        assert_eq!(body["writable"], true);
        let text = body.to_string();
        assert!(!text.contains(&token), "a token leaked into the audit log");
        assert!(!text.contains("arneb_"), "{text}");

        // A failed revoke is audited with its error code.
        let (status, _) = call(
            &state,
            Method::DELETE,
            &format!("/api/v2/tokens/{id}"),
            Some(&alice()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (_, body) = audit_log(&state, "?action=token.revoke&limit=1").await;
        assert_eq!(body["entries"][0]["outcome"], json!({"error": "not_found"}));
        assert_eq!(body["truncated"], true);
    }

    #[tokio::test]
    async fn revoking_by_a_malformed_id_is_refused_unaudited() {
        let state = state();
        let (_, issued) = issue(&state).await;
        let token = issued["token"].as_str().unwrap().to_owned();
        for bad in [token.as_str(), "0123456789ABCDEF", "0123456789abcde"] {
            let (status, body) = call(
                &state,
                Method::DELETE,
                &format!("/api/v2/tokens/{bad}"),
                Some(&alice()),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(body["error"]["code"], "invalid_argument");
        }
        let (_, body) = audit_log(&state, "").await;
        assert_eq!(body["entries"].as_array().unwrap().len(), 1, "{body}");
        assert!(!body.to_string().contains(&token));
    }

    #[tokio::test]
    async fn audit_listing_is_bounded_and_needs_observe() {
        let state = state();
        let (status, body) = audit_log(&state, "?limit=501").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert!(
            body["error"]["hint"].as_str().unwrap().contains("500"),
            "{body}"
        );
        let (status, body) = audit_log(&state, "?limit=0").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        let state = with_authz(
            web_state(true, None),
            Some(r#"{"system_information":[{"user":"root","allow":["read"]}]}"#),
            &[],
        );
        let (status, body) = call(&state, Method::GET, "/api/v2/audit", Some(&alice()), None).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "forbidden");
        let (status, body) = audit_log(&state, "").await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    #[tokio::test]
    async fn failed_append_closes_manage_until_writable() {
        let state = state();
        // 1. Appends start failing.
        state.audit.set_fail_appends(true);
        // 2. The action is applied and answered with its real result.
        let (status, issued) = issue(&state).await;
        assert_eq!(status, StatusCode::CREATED, "{issued}");
        let token = issued["token"].as_str().unwrap().to_owned();
        let (status, me) = call(
            &state,
            Method::GET,
            "/api/v2/whoami",
            Some(&format!("Bearer {token}")),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["user"], "alice");
        assert!(!state.audit.is_writable());
        assert_eq!(state.audit.len(), 0, "the failed entry is not in the log");
        // 3. The next audited action is refused before it runs.
        let before = token_count(&state).await;
        let (status, body) = issue(&state).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert_eq!(body["error"]["code"], "audit_unavailable");
        assert_eq!(token_count(&state).await, before, "no token was created");
        let (_, page) = audit_log(&state, "").await;
        assert_eq!(page["writable"], false);
        // 4. The log recovers.
        state.audit.set_fail_appends(false);
        // 5. Audited actions work again, and are audited.
        let (status, issued) = issue(&state).await;
        assert_eq!(status, StatusCode::CREATED, "{issued}");
        let (_, page) = audit_log(&state, "").await;
        assert_eq!(page["writable"], true);
        let entries = page["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "{page}");
        assert_eq!(entries[0]["action"], "token.issue");
        assert_eq!(entries[0]["target"], issued["id"]);
        assert_eq!(token_count(&state).await, before + 1);
    }
}
