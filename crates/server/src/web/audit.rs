//! The audit log on the Web API (manage-api D6): the [`audited`] wrapper
//! every audited action runs through, and `GET /api/v2/audit`.
//!
//! Order and failure: the log must be writable (or reopen) and the
//! action's intent entry (outcome `pending`) must be synced before the
//! action runs, else `audit_unavailable` and nothing is applied; then the
//! action runs; then its completion entry is appended (a panicking action
//! is completed as `interrupted`: it may or may not have been applied). A failed completion cannot undo the
//! action, so it goes to the ERROR log, the log is marked unwritable and
//! keeps the completion, and the action's real result is returned; the next
//! audited action, the background retry ([`spawn_audit_retry`], every 5 s)
//! or the shutdown flush ([`flush_audit`]) reopens the log, which first
//! appends the kept completion; an audited action is refused while that
//! fails. So every applied action has at least its
//! intent on disk: a crash before its completion is written leaves it
//! `pending`, and the next start reports it at WARN on `arneb::audit` and
//! records it as `interrupted`.

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

/// How often [`spawn_audit_retry`] tries to reopen an unwritable log.
pub(crate) const AUDIT_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Tries to make an unwritable audit log writable again every `every`,
/// appending the completions whose write failed
/// ([`arneb_state::AuditLog::ensure_writable`]), so a known outcome reaches
/// the file (and `writable` turns true again) once the disk recovers, even
/// when no audited action comes in. A no-op tick while writable.
pub(crate) fn spawn_audit_retry(
    audit: std::sync::Arc<arneb_state::AuditLog>,
    every: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if audit.is_writable() {
                continue;
            }
            let log = std::sync::Arc::clone(&audit);
            // Blocking (reopen + fsync); failures were already logged at
            // ERROR when the log turned unwritable.
            if let Ok(Err(e)) = tokio::task::spawn_blocking(move || log.ensure_writable()).await {
                tracing::debug!(
                    target: "arneb::audit",
                    error = %e,
                    "the audit log is still not writable"
                );
            }
        }
    })
}

/// How long the shutdown flush waits for audited actions still running
/// (e.g. a cancel waiting on its kill, a PBKDF2 token issue).
pub(crate) const SHUTDOWN_ACTION_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// At graceful shutdown: waits (at most [`SHUTDOWN_ACTION_WAIT`]) for the
/// audited actions still running in `spawn_blocking` to complete, then
/// makes one last attempt to append the completions whose write failed, so
/// their known outcomes are not lost (a restart would record their intents
/// as `interrupted`). Logs at ERROR how many actions are still running, or
/// how many completions are lost, when that fails.
pub(crate) async fn flush_audit(audit: std::sync::Arc<arneb_state::AuditLog>) {
    flush_audit_within(audit, SHUTDOWN_ACTION_WAIT).await;
}

async fn flush_audit_within(
    audit: std::sync::Arc<arneb_state::AuditLog>,
    wait: std::time::Duration,
) {
    let deadline = tokio::time::Instant::now() + wait;
    while audit.actions_in_flight() > 0 {
        if tokio::time::Instant::now() >= deadline {
            tracing::error!(
                target: "arneb::audit",
                running = audit.actions_in_flight(),
                "shutting down with audited actions still running; \
                 the next start records them as interrupted"
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    if audit.is_writable() {
        return;
    }
    let log = std::sync::Arc::clone(&audit);
    let error = match tokio::task::spawn_blocking(move || log.ensure_writable()).await {
        Ok(Ok(())) => return,
        Ok(Err(e)) => e.to_string(),
        Err(e) => format!("the flush task failed: {e}"),
    };
    tracing::error!(
        target: "arneb::audit",
        error = %error,
        completions = audit.unwritten_completions(),
        "shutting down with audit completions that could not be written; \
         the next start records their actions as interrupted"
    );
}

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

/// Runs `run` as the audited `action` of `who`: records `intent` (what is
/// known before the action runs) as a `pending` entry first, refusing with
/// `audit_unavailable` before running anything when the log is unwritable
/// and cannot be reopened or the intent cannot be written; otherwise runs
/// it and appends the completion described by `subject` (ok or error
/// outcome). Blocking (fsync): call it from `spawn_blocking`.
pub(super) fn audited<T>(
    state: &WebState,
    who: &Identity,
    action: &'static str,
    intent: AuditSubject,
    run: impl FnOnce() -> Result<T, ApiError>,
    subject: impl FnOnce(Result<&T, &ApiError>) -> AuditSubject,
) -> Result<T, ApiError> {
    let record = |AuditSubject { target, parameters }, outcome| AuditRecord {
        user: who.user.clone(),
        via: who.via.as_str().to_owned(),
        action: action.to_owned(),
        target,
        parameters,
        outcome,
    };
    let refuse = |error: &dyn std::fmt::Display| {
        tracing::error!(
            target: "arneb::audit",
            user = %who.user,
            action,
            error = %error,
            "refusing an audited action: the audit log is not writable"
        );
        audit_unavailable()
    };
    // Counted until its completion is appended (or kept), so the shutdown
    // flush waits for it ([`flush_audit`]).
    let _running = state.audit.track_action();
    if let Err(e) = state.audit.ensure_writable() {
        return Err(refuse(&e));
    }
    let closing = AuditSubject {
        target: intent.target.clone(),
        parameters: intent.parameters.clone(),
    };
    let begun = match state.audit.begin(record(intent, AuditOutcome::Pending)) {
        Ok(entry) => entry,
        Err(e) => {
            // A write was attempted and may have reached the disk (a failed
            // fsync): close the intent, best effort, as refused, so a
            // restart does not report an action that never ran as
            // interrupted. (If this fails too, the log keeps the closing
            // entry and appends it when it reopens.) An intent refused
            // without a write (the log turned unwritable meanwhile) is not
            // in the file, so there is nothing to close.
            if matches!(e.source, arneb_state::StateError::Io { .. }) {
                let refused = AuditOutcome::Error(ErrorCode::AuditUnavailable.name());
                if state.audit.ensure_writable().is_ok() {
                    let _ = state.audit.complete(e.entry.seq, record(closing, refused));
                }
            }
            return Err(refuse(&e.source));
        }
    };
    // A panicking action is recorded as `interrupted` (it may or may not
    // have been applied: the panic may follow the side effect), then the
    // panic continues, so its intent does not stay pending.
    let out = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)) {
        Ok(out) => out,
        Err(panic) => {
            let failed = AuditOutcome::Interrupted;
            if let Err(e) = state.audit.complete(begun.seq, record(closing, failed)) {
                tracing::error!(
                    target: "arneb::audit",
                    intent_seq = begun.seq,
                    error = %e.source,
                    "an audited action panicked and its audit completion could not be written"
                );
            }
            std::panic::resume_unwind(panic);
        }
    };
    // An action that ended without an outcome (e.g. a kill interrupted
    // after it may have stopped the work) is `interrupted`, like a panic.
    let outcome = match &out {
        Ok(_) => AuditOutcome::Ok,
        Err(e) if e.interrupted => AuditOutcome::Interrupted,
        Err(e) => AuditOutcome::Error(e.code.name()),
    };
    let done = record(subject(out.as_ref()), outcome);
    if let Err(arneb_state::AuditAppendError { entry, source }) =
        state.audit.complete(begun.seq, done)
    {
        tracing::error!(
            target: "arneb::audit",
            seq = entry.seq,
            intent_seq = begun.seq,
            time_ms = entry.time_ms,
            entry = %serde_json::to_string(&entry).unwrap_or_default(),
            error = %source,
            "the action was applied but its audit completion could not be written \
             (it is appended once the log reopens); audited actions are refused \
             until the audit log is writable"
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
    /// `all` for a caller with manage; `own` for anyone else, who sees only
    /// the entries of their own actions.
    scope: &'static str,
}

/// The audit log page for `who`, who needs observe. A caller with manage
/// reads the whole log; anyone else reads only the entries of their own
/// actions (a `user` filter naming someone else matches nothing). Shared
/// with the MCP `list_audit_log` tool.
pub(super) fn audit_page_for(
    state: &WebState,
    who: &Identity,
    params: AuditParams,
) -> Result<AuditResponse, ApiError> {
    state.authorizer.check(who, Layer::Observe)?;
    let full = state.authorizer.allows(who, Layer::Manage);
    let limit = params.limit.unwrap_or(DEFAULT_AUDIT_LIMIT);
    if limit == 0 || limit > MAX_AUDIT_LIMIT {
        return Err(ApiError::new(
            ErrorCode::InvalidArgument,
            format!("limit {limit} is not valid"),
        )
        .with_hint(format!("use a limit between 1 and {MAX_AUDIT_LIMIT}")));
    }
    let user = match params.user {
        Some(u) if !full && u != who.user => None,
        Some(u) => Some(Some(u)),
        None if full => Some(None),
        None => Some(Some(who.user.clone())),
    };
    let page = match user {
        Some(user) => state.audit.query(&AuditQuery {
            limit,
            before_seq: params.cursor,
            user,
            action: params.action,
        }),
        // Another user's entries, for a caller without manage: none.
        None => arneb_state::AuditPage {
            entries: Vec::new(),
            next_cursor: None,
            truncated: false,
        },
    };
    Ok(AuditResponse {
        entries: page.entries,
        next_cursor: page.next_cursor,
        truncated: page.truncated,
        persistent: state.audit.is_persistent(),
        writable: state.audit.is_writable(),
        scope: if full { "all" } else { "own" },
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
    use super::{
        audited, flush_audit, flush_audit_within, spawn_audit_retry, ApiError, AuditOutcome,
        AuditQuery, AuditRecord, AuditSubject, Identity,
    };
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
    async fn only_managers_read_other_users_entries() {
        let state = state();
        let (status, _) = issue(&state).await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = call(
            &state,
            Method::POST,
            "/api/v2/tokens",
            Some(&basic("bob", "hunter2")),
            Some(json!({"name": "bobs"})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let users = |body: &Value| -> Vec<String> {
            body["entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["user"].as_str().unwrap().to_owned())
                .collect()
        };
        // root manages: the whole log.
        let (status, body) = audit_log(&state, "").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(users(&body), ["bob", "alice"]);
        assert_eq!(body["scope"], "all");
        // alice: only her own actions, whatever she filters by.
        for query in ["", "?user=alice"] {
            let (status, body) = call(
                &state,
                Method::GET,
                &format!("/api/v2/audit{query}"),
                Some(&alice()),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(users(&body), ["alice"], "{query}");
            assert_eq!(body["scope"], "own");
        }
        let (status, body) = call(
            &state,
            Method::GET,
            "/api/v2/audit?user=bob",
            Some(&alice()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(users(&body).is_empty(), "{body}");
        assert!(!body.to_string().contains("bobs"), "{body}");
    }

    /// The intent is written before the action runs: an unwritable log
    /// refuses the action before anything is applied.
    #[tokio::test]
    async fn failed_intent_refuses_before_applying() {
        let state = state();
        state.audit.set_fail_appends(true);
        let (status, body) = issue(&state).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert_eq!(body["error"]["code"], "audit_unavailable");
        assert_eq!(token_count(&state).await, 0, "no token was created");
        assert!(state.audit.is_empty());
        state.audit.set_fail_appends(false);
        let (status, issued) = issue(&state).await;
        assert_eq!(status, StatusCode::CREATED, "{issued}");
        let (_, page) = audit_log(&state, "").await;
        let entries = page["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "{page}");
        assert_eq!(entries[0]["outcome"], "ok");
        assert!(entries[0]["intent_seq"].is_u64(), "{page}");
    }

    /// An intent whose write failed is closed as refused when the log
    /// recovers at once, so it is never reported as interrupted.
    #[tokio::test]
    async fn a_failed_intent_is_closed_as_refused() {
        let state = state();
        state.audit.fail_next_append();
        let (status, body) = issue(&state).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert_eq!(token_count(&state).await, 0, "nothing was applied");
        let (_, page) = audit_log(&state, "").await;
        let entries = page["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "{page}");
        assert_eq!(entries[0]["action"], "token.issue");
        assert_eq!(entries[0]["outcome"], json!({"error": "audit_unavailable"}));
        assert!(entries[0]["intent_seq"].is_u64(), "{page}");
        assert_eq!(page["writable"], true);
    }

    #[tokio::test]
    async fn failed_append_closes_manage_until_writable() {
        let state = state();
        // 1. Completions start failing (the intent is still written).
        state.audit.set_fail_completions(true);
        // 2. The action is applied and answered with its real result.
        let (status, issued) = issue(&state).await;
        assert_eq!(status, StatusCode::CREATED, "{issued}");
        let first_id = issued["id"].clone();
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
        assert_eq!(state.audit.len(), 1, "only the intent is in the log");
        state.audit.set_fail_completions(false);
        state.audit.set_fail_appends(true);
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
        assert_eq!(entries.len(), 2, "{page}");
        assert_eq!(entries[0]["action"], "token.issue");
        assert_eq!(entries[0]["target"], issued["id"]);
        assert_eq!(entries[0]["outcome"], "ok");
        // The completion that failed was appended when the log reopened,
        // so the first issue is no longer pending.
        assert_eq!(entries[1]["outcome"], "ok");
        assert_eq!(entries[1]["action"], "token.issue");
        assert_eq!(entries[1]["target"], first_id);
        assert!(entries[1]["intent_seq"].is_u64(), "{page}");
        assert_eq!(token_count(&state).await, before + 1);
    }

    /// An intent refused without a write (the log turned unwritable just
    /// before it) is not in the file: no closing entry is written for it.
    #[tokio::test]
    async fn an_intent_refused_without_a_write_is_not_closed() {
        let state = state();
        state.audit.refuse_next_append();
        let (status, body) = issue(&state).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert_eq!(token_count(&state).await, 0, "nothing was applied");
        assert!(state.audit.is_empty(), "no dangling refused entry");
        // The next action reopens the log and is audited normally.
        let (status, issued) = issue(&state).await;
        assert_eq!(status, StatusCode::CREATED, "{issued}");
        let (_, page) = audit_log(&state, "").await;
        let entries = page["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "{page}");
        assert_eq!(entries[0]["outcome"], "ok");
    }

    /// The `token.issue` intent names the token id, so a crash between
    /// issuing and completing leaves an entry naming the persisted token.
    #[tokio::test]
    async fn the_token_issue_intent_names_the_token() {
        let state = state();
        state.audit.set_fail_completions(true);
        let (status, issued) = issue(&state).await;
        assert_eq!(status, StatusCode::CREATED, "{issued}");
        let (_, page) = audit_log(&state, "").await;
        let entries = page["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "{page}");
        assert_eq!(entries[0]["outcome"], "pending");
        assert_eq!(entries[0]["target"], issued["id"]);
    }

    /// A panicking action is completed as `interrupted` (the panic may come
    /// after the side effect), so its intent does not stay pending, and the
    /// panic continues.
    #[tokio::test]
    async fn a_panicking_action_is_completed_as_interrupted() {
        let state = state();
        let who = Identity {
            user: "root".into(),
            via: crate::web::auth::AuthVia::Basic,
            oauth_expires_ms: None,
        };
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            audited(
                &state,
                &who,
                "worker.cordon",
                AuditSubject {
                    target: "w1".into(),
                    parameters: json!({}),
                },
                || -> Result<(), ApiError> { panic!("boom") },
                |_| unreachable!(),
            )
        }));
        assert!(caught.is_err(), "the panic continues");
        let (_, page) = audit_log(&state, "").await;
        let entries = page["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "{page}");
        assert_eq!(entries[0]["action"], "worker.cordon");
        assert_eq!(entries[0]["target"], "w1");
        assert_eq!(entries[0]["outcome"], "interrupted");
        assert!(entries[0]["intent_seq"].is_u64(), "{page}");
    }

    /// A completion whose write failed reaches the log from the background
    /// retry once the disk recovers, with no further audited action, and
    /// the log turns writable again.
    #[tokio::test]
    async fn a_failed_completion_is_retried_in_the_background() {
        let state = state();
        state.audit.set_fail_completions(true);
        let (status, issued) = issue(&state).await;
        assert_eq!(status, StatusCode::CREATED, "{issued}");
        assert!(!state.audit.is_writable());
        assert_eq!(state.audit.unwritten_completions(), 1);
        state.audit.set_fail_completions(false);
        let retry = spawn_audit_retry(
            std::sync::Arc::clone(&state.audit),
            std::time::Duration::from_millis(10),
        );
        for _ in 0..200 {
            if state.audit.is_writable() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        retry.abort();
        assert!(
            state.audit.is_writable(),
            "the retry never reopened the log"
        );
        assert_eq!(state.audit.unwritten_completions(), 0);
        let (_, page) = audit_log(&state, "").await;
        assert_eq!(page["writable"], true, "{page}");
        let entries = page["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "{page}");
        assert_eq!(entries[0]["outcome"], "ok");
        assert_eq!(entries[0]["target"], issued["id"]);
    }

    /// The shutdown flush appends a kept completion, so the next start
    /// loads its real outcome instead of recording it as interrupted.
    #[tokio::test]
    async fn the_shutdown_flush_appends_a_kept_completion() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("audit.jsonl");
        let settings = arneb_state::AuditSettings {
            max_entries: 100,
            retention_days: 0,
        };
        let audit = std::sync::Arc::new(arneb_state::AuditLog::open(Some(&log), settings).unwrap());
        let record = |outcome| AuditRecord {
            user: "root".into(),
            via: "basic".into(),
            action: "worker.cordon".into(),
            target: "w1".into(),
            parameters: json!({}),
            outcome,
        };
        let intent = audit.begin(record(AuditOutcome::Pending)).unwrap();
        audit.set_fail_completions(true);
        assert!(audit
            .complete(intent.seq, record(AuditOutcome::Ok))
            .is_err());
        audit.set_fail_completions(false);
        flush_audit(std::sync::Arc::clone(&audit)).await;
        assert!(audit.is_writable());
        drop(audit);
        let reopened = arneb_state::AuditLog::open(Some(&log), settings).unwrap();
        let page = reopened.query(&AuditQuery {
            limit: 10,
            ..Default::default()
        });
        assert_eq!(page.entries.len(), 1, "{:?}", page.entries);
        assert_eq!(page.entries[0].record.outcome, AuditOutcome::Ok);
    }

    /// The shutdown flush waits for an audited action still running in
    /// `spawn_blocking` (a cancel waiting on its kill): when that action's
    /// completion then fails, the flush still appends it, instead of
    /// having returned before it finished.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_shutdown_flush_waits_for_running_audited_actions() {
        let state = state();
        let (inside_tx, inside_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let action = tokio::task::spawn_blocking({
            let state = state.clone();
            move || {
                let who = Identity {
                    user: "root".into(),
                    via: crate::web::auth::AuthVia::Basic,
                    oauth_expires_ms: None,
                };
                audited(
                    &state,
                    &who,
                    "query.cancel",
                    AuditSubject {
                        target: "q1".into(),
                        parameters: json!({}),
                    },
                    || -> Result<(), ApiError> {
                        inside_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Ok(())
                    },
                    |_| AuditSubject {
                        target: "q1".into(),
                        parameters: json!({}),
                    },
                )
            }
        });
        tokio::task::spawn_blocking(move || inside_rx.recv().unwrap())
            .await
            .unwrap();
        assert_eq!(state.audit.actions_in_flight(), 1);
        // Its completion will fail once; the log is writable until then.
        state.audit.fail_next_append();
        let flush = tokio::spawn(flush_audit(std::sync::Arc::clone(&state.audit)));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !flush.is_finished(),
            "the flush ran before the action ended"
        );
        release_tx.send(()).unwrap();
        action.await.unwrap().unwrap();
        flush.await.unwrap();
        assert_eq!(state.audit.actions_in_flight(), 0);
        assert!(state.audit.is_writable());
        assert_eq!(state.audit.unwritten_completions(), 0);
        let (_, page) = audit_log(&state, "").await;
        let entries = page["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "{page}");
        assert_eq!(entries[0]["outcome"], "ok");
    }

    /// The flush waits for running actions only up to its bound.
    #[tokio::test]
    async fn the_shutdown_flush_wait_is_bounded() {
        let state = state();
        let _running = state.audit.track_action();
        let flush = flush_audit_within(
            std::sync::Arc::clone(&state.audit),
            std::time::Duration::from_millis(50),
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), flush)
            .await
            .expect("the flush waited past its bound");
    }

    /// A failed `token.issue` completion names no token: the pre-generated
    /// id never became one. (The state file's directory turns read-only
    /// after the checks, so the issue itself fails.)
    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_token_issue_names_no_token() {
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.json");
        let state = with_authz(web_state(true, Some(&path)), None, &["root"]);
        let mode = |m| std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(m));
        mode(0o500).unwrap();
        let (status, body) = issue(&state).await;
        mode(0o700).unwrap();
        if status == StatusCode::CREATED {
            return; // running as root: permissions do not stop the write
        }
        assert_eq!(body["error"]["code"], "internal", "{body}");
        let (_, page) = audit_log(&state, "").await;
        let entries = page["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "{page}");
        assert_eq!(entries[0]["action"], "token.issue");
        assert!(entries[0]["outcome"]["error"].is_string(), "{page}");
        assert_eq!(entries[0]["target"], "", "{page}");
    }
}
